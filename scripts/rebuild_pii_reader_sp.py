"""Rebuild, from nothing, the service principal a PII-reader proof needs.

WHY THIS EXISTS AT ALL, and it is a defect this repository already wrote down. F4's
own evidence cites a two-principal transcript taken against a service principal that
has since been DELETED -- `SCIM ServicePrincipals` returns `totalResults: 0` -- so the
claim rested on an artefact nobody can re-read. A measurement whose apparatus no
longer exists is indistinguishable from one nobody took, which is this project's
recurring species: a guard whose output cannot tell "passed" from "never ran". The
answer is not a longer transcript; it is a script that rebuilds the apparatus, so the
next reviewer re-runs the proof instead of believing it.

THE CREATION PATH IS NOT THE DOCUMENTED ONE, and every step below was measured on this
workspace rather than read off a reference:

  * SCIM `POST .../ServicePrincipals` returns BOTH an `applicationId` (a UUID) and a
    numeric SCIM `id`. They are not interchangeable, and which one a call wants is not
    guessable: `service-principal-secrets-proxy create` takes the NUMERIC id and
    refuses the UUID outright -- `Error: Invalid service principal id
    'd0e35b43-...'`. `GRANT` takes the UUID, and refuses the numeric id.
  * The ACCOUNT-shaped secret paths all return `Not Found` from a workspace host.
    Account SCIM is likewise unreachable from here, which is the same fact that makes
    the mask predicate `is_member` rather than `is_account_group_member` (ADR 0008).
  * The ACCESS-CONTROL RULE-SET PROXY, however, IS served from the workspace host even
    though account SCIM is not -- `/api/2.0/preview/accounts/access-control/rule-sets`.
    A `run_as` job needs `roles/servicePrincipal.user` on the principal's rule set, and
    the update is a full replacement: read the current `etag` and rules first and send
    them back, or the PUT silently drops whatever else was there.
  * A usable token is then client credentials against `/oidc/v1/token` with
    `scope=all-apis`, using the applicationId as the username.

NO SECRET IS EVER WRITTEN TO A FILE BY THIS SCRIPT. It prints the client id and the
secret once, to stdout, for the operator to put in a git-ignored `.env`. A `--no-secret`
run rebuilds the principal and its role without minting one at all, which is what you
want when the principal exists and only the ACL was lost. Databricks does not serve a
secret twice; a lost one is replaced, not recovered.

WHAT IT DELIBERATELY DOES NOT DO: add the principal to `opl_pii_readers`. That group
is empty by decision -- membership is what lets a real identity read 55.8M real
personal names, and it is a decision with an owner. `--group` takes any group name so a
THROWAWAY proof group can be used instead, which is what F4 Task 5's proof did.

    uv run python scripts/rebuild_pii_reader_sp.py --display-name opl-pii-reader
    uv run python scripts/rebuild_pii_reader_sp.py --group _probe_pii_readers
"""
from __future__ import annotations

import argparse
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

PROFILE = "opl-free"
DEFAULT_DISPLAY_NAME = "opl-pii-reader"
RUN_AS_ROLE = "roles/servicePrincipal.user"


def find_or_create(w: WorkspaceClient, display_name: str) -> iam.ServicePrincipal:
    """The principal, created if this workspace has none by that name.

    Idempotent by DISPLAY NAME, which is the only handle an operator has before the
    ids exist. A second run therefore adopts the existing principal rather than
    creating a second one with the same name and a different applicationId -- two of
    those are indistinguishable in every UI and only one of them holds the grants."""
    for existing in w.service_principals.list(filter=f'displayName eq "{display_name}"'):
        print(f"rebuild_pii_reader_sp: found {display_name} "
              f"(applicationId {existing.application_id}, scim id {existing.id})")
        return existing
    created = w.service_principals.create(display_name=display_name, active=True)
    print(f"rebuild_pii_reader_sp: CREATED {display_name} "
          f"(applicationId {created.application_id}, scim id {created.id})")
    return created


def rule_set_name(w: WorkspaceClient, application_id: str) -> str:
    """The account-scoped name of one principal's rule set.

    The account id comes from the resolved SDK config rather than from a constant: it
    is not a secret, but it is workspace-specific, and a repository that hardcodes one
    is a repository that has to be edited to run anywhere else."""
    account_id = w.config.account_id
    if not account_id:
        raise SystemExit(
            "the account id could not be resolved from the CLI profile; pass "
            "--account-id, which the rule-set name needs"
        )
    return f"accounts/{account_id}/servicePrincipals/{application_id}/ruleSets/default"


def grant_run_as(w: WorkspaceClient, name: str, principal: str) -> None:
    """Give `principal` the right to run a job AS this service principal.

    A FULL REPLACEMENT, not an append: the API takes the whole rule list and the
    current `etag`, so the existing rules are read and sent back. Dropping them is how
    an operator loses `servicePrincipal.manager` on a principal they just created and
    can no longer administer."""
    current = w.account_access_control_proxy.get_rule_set(name=name, etag="")
    rules = list(current.grant_rules or [])
    if any(rule.role == RUN_AS_ROLE and principal in (rule.principals or []) for rule in rules):
        print(f"rebuild_pii_reader_sp: {principal} already holds {RUN_AS_ROLE}")
        return
    rules.append(iam.GrantRule(principals=[principal], role=RUN_AS_ROLE))
    w.account_access_control_proxy.update_rule_set(
        name=name,
        rule_set=iam.RuleSetUpdateRequest(name=name, etag=current.etag, grant_rules=rules),
    )
    print(f"rebuild_pii_reader_sp: granted {RUN_AS_ROLE} to {principal}")


def add_to_group(w: WorkspaceClient, group_name: str, scim_id: str) -> None:
    """Membership takes MINUTES to become visible to `is_member`, in BOTH directions.

    Measured on the opl-free warehouse with a fresh OAuth token on every read: an
    addition was invisible for 294 s and visible at 318 s; a removal was still visible
    at 211 s and gone at 234 s. So a proof that flips membership and reads back inside
    one session records a FALSE result -- use `GRANT`/`REVOKE`, which take effect on
    the next statement. See ADR 0008."""
    groups = list(w.groups.list(filter=f'displayName eq "{group_name}"'))
    if not groups:
        raise SystemExit(f"no workspace group named {group_name!r}; create it first")
    w.groups.patch(
        id=str(groups[0].id),
        operations=[iam.Patch(op=iam.PatchOp.ADD, path="members",
                              value=[{"value": scim_id}])],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )
    print(f"rebuild_pii_reader_sp: added scim id {scim_id} to {group_name} -- "
          "NOT visible to is_member() for several minutes")


def mint_secret(w: WorkspaceClient, scim_id: str, application_id: str) -> None:
    """Print one OAuth secret, once, and never write it anywhere.

    Takes the NUMERIC scim id: the proxy refuses the applicationId with `Invalid
    service principal id`, which is the single most surprising step of this path."""
    secret = w.service_principal_secrets_proxy.create(service_principal_id=scim_id)
    print("\n=== COPY THESE INTO A GIT-IGNORED .env, THEY ARE NOT SERVED AGAIN ===")
    print(f"OPL_PII_READER_CLIENT_ID={application_id}")
    print(f"OPL_PII_READER_CLIENT_SECRET={secret.secret}")
    print("=== end ===\n")


def _parse(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__ and __doc__.splitlines()[0])
    parser.add_argument("--display-name", default=DEFAULT_DISPLAY_NAME)
    parser.add_argument("--profile", default=PROFILE)
    parser.add_argument("--group", default=None,
                        help="workspace group to add it to; opl_pii_readers is "
                             "deliberately NOT the default")
    parser.add_argument("--run-as-principal", default=None,
                        help="user who may run a job as this SP, e.g. users/a@b.c")
    parser.add_argument("--no-secret", action="store_true",
                        help="rebuild the principal and its role without minting one")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse(argv)
    w = WorkspaceClient(profile=args.profile)
    principal = find_or_create(w, args.display_name)
    application_id, scim_id = str(principal.application_id), str(principal.id)
    if args.run_as_principal:
        grant_run_as(w, rule_set_name(w, application_id), args.run_as_principal)
    if args.group:
        add_to_group(w, args.group, scim_id)
    if not args.no_secret:
        mint_secret(w, scim_id, application_id)
    print(f"rebuild_pii_reader_sp: grant principal for SQL is {application_id} "
          "(the applicationId, NOT the scim id)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
