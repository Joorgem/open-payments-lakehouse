# `iac/` — Terraform, for identity, and for nothing else

**Nothing in this directory has ever been applied.** No `terraform apply` has been run
against any workspace from this repository, and neither has a `terraform plan` that
reached one. What has been run is written out below, with its exit code; what has not is
said plainly rather than implied by silence.

## What it declares

```bash
git ls-files iac        # the files, rather than the table below, which can go stale
```

| file | what is in it |
|---|---|
| `versions.tf` | the Terraform floor and the exact provider version, with an empty provider block |
| `variables.tf` | the group name, and the member list that is empty on purpose |
| `identity.tf` | `databricks_group`, `databricks_group_member`, and the `databricks_user` data source that turns a name into an id |
| `.terraform.lock.hcl` | the provider pin, with the platform hashes `terraform providers lock` wrote |

Derive the resource types rather than trusting this table:

```bash
grep -rhoE '^(resource|data) "[a-z_]+"' iac/*.tf | sort -u
```

`tests/test_iac_terraform.py` holds that set to an allowlist, so a third kind of object
arriving here is a decision somebody has to type out — the same shape, and the same
argument, as `_DECLARABLE` in `tests/test_bundle_resource_allowlist.py`.

## Why identity, and why only identity

A Databricks Asset Bundle cannot declare a group. The CLI's own schema is the authority
on which collections a bundle may declare, and asking it for an identity one returns the
empty list — printed, so it can be told apart from a command that failed:

```bash
databricks bundle schema | uv run python -c "import json,sys
n=json.load(sys.stdin)['\$defs']['github.com']['databricks']['cli']['bundle']['config.Resources']['oneOf'][0]['properties']
print([k for k in sorted(n) if 'group' in k or 'user' in k or 'principal' in k])"
```

So the group named by the socios mask predicate has always had to be created by hand.
`scripts/rebuild_pii_reader_sp.py`, the script that rebuilds the PII-reader apparatus,
refuses with *"no workspace group named `<name>`; create it first"* — the repository
knows the group must exist and had nothing that said so. That is the gap this directory
closes, and it is the whole of it.

Whatever the bundle *can* declare is declared in the bundle and not here. A second
Terraform over objects a bundle already deploys is not IaC, it is a second writer over
one set of objects — and the bundle's own engine is Terraform anyway: the CLI's
`engine.EngineType` enum is `["terraform", "direct"]`.

```bash
databricks bundle schema | uv run python -m json.tool | grep -A 5 'engine\.EngineType'
```

(The key is spelled `engine.EngineType`, dot included. A grep for `"EngineType"` returns
nothing and looks exactly like a key that is not there.)

### Unity Catalog grants are declined, and this is the measurement, not a memory

The provider's grant resources, derived from the pinned provider binary rather than from
documentation or recollection:

```bash
(cd iac && terraform providers schema -json > .terraform/schema.json)   # .terraform/ is git-ignored
uv run python -c "import json; s=json.load(open('iac/.terraform/schema.json'))['provider_schemas']['registry.terraform.io/databricks/databricks']; print(sorted(r for r in s['resource_schemas'] if 'grant' in r))"
```

Both are authoritative. At the pinned version's own documentation
(`docs/resources/grant.md` at tag `v1.130.0` of `databricks/terraform-provider-databricks`):

> This resource is _authoritative_ for grants on securables for a given _singular_
> principal. Configuring this resource for a securable will **OVERWRITE** any existing
> grants for the principal and changes made outside of Terraform will be reset.

and for the plural one:

> This resource is _authoritative_ for grants on securables. Configuring this resource
> for a securable will **OVERWRITE** any existing grants and changes made outside of
> Terraform will be reset.

[ADR 0018] Decision 6 keeps governance imperative — `apply_pii_governance` issues and
withdraws `SELECT` at run time — and the reason it gives against a declarative grant is
that an authoritative one revokes what this repository never issued, including the
platform's own `CREATE TABLE` on the schema every pipeline writes into.

**That reason does not reach the singular resource, and saying so is the honest half.**
`databricks_grant`'s blast radius is one principal, so it could not revoke a privilege
held by the platform. What keeps the refusal standing is a different fact, and it comes
from this repository rather than from the provider: over the securables
`apply_pii_governance` settles — the socios tables — it computes its plan from what
`SHOW GRANTS` reports and revokes `SELECT` from every principal not on a reviewed roster
that is empty today (`PII_READERS` in `src/opl/bronze/pii_governance.py`). A grant on one
of those tables declared here would be revoked by the next governance run and re-created
by the next apply: two writers, each authoritative over its own slice, with nothing
ordering them. Over a securable that task does not settle, that collision does not arise
— and no grant is declared here for one of those either, because [ADR 0018] Decision 6
put grants in the imperative task and this directory does not reopen it.

So: **no grant resource is declared here**, and the reversal condition is recorded rather
than left implicit — *if the provider ever ships a grant resource that is
non-authoritative, the first objection stops applying to it and the decision is retaken
against the second one.* At `1.130.0` it has not.

## What CI runs, and what it deliberately does not

`.github/workflows/ci.yml` runs, in the `terraform` job:

```bash
terraform fmt -check -recursive
terraform init -backend=false
terraform validate
```

**It does not run `terraform plan`, and that is the load-bearing part.** Measured in this
directory, with `DATABRICKS_CONFIG_FILE` pointed at a path that does not exist and
`DATABRICKS_HOST`/`DATABRICKS_TOKEN` unset:

| command | exit | what it printed |
|---|---|---|
| `terraform fmt -check -recursive` | 0 | — |
| `terraform init` | 0 | `Installed databricks/databricks v1.130.0` |
| `terraform validate` | 0 | `Success! The configuration is valid.` |
| `terraform plan`, committed defaults | **0** | `Plan: 1 to add, 0 to change, 0 to destroy.` |
| `terraform plan`, one member named | **1** | `Error: cannot read user: … cannot configure default credentials` |

Read the last two rows together. With the member list empty, nothing in the configuration
**reads** anything, so `plan` never authenticates and never looks — and `1 to add` is
therefore what it prints **whether or not the group is already there**. Nothing in that
run could tell the two apart, which is what makes the green worthless rather than merely
weak; and [ADR 0008] records this group as one that does already exist. Name one
member and the data source has to read, and the same command exits 1. A CI step running
`plan` would therefore either fail for want of a secret or pass without meaning, decided
by which of those two shapes the configuration happened to be in that week.

`tests/test_iac_terraform.py` locks the CI job against both halves: the job must run
`validate`, and no step in it may run `plan` or `apply`.

There is one more reason the CI job pins `terraform_version` to the version used here.
The provider falls back to `~/.databrickscfg` when nothing else configures it, so a check
that reads anything can pass on a developer box **because of a file nobody passed it** and
fail on a runner that has no such file. `fmt -check` and `validate` read nothing, so their
local behaviour and their CI behaviour are the same thing.

## The gated path: what a human would run, and what it needs

Not run here. Every command below is written from the provider's documentation and the
credential-free measurements above; none of it has been executed against a workspace.

**Credentials.** The provider's unified chain: a CLI profile
(`DATABRICKS_CONFIG_PROFILE=<profile>`, resolved from `~/.databrickscfg`) or a
`DATABRICKS_HOST`/`DATABRICKS_TOKEN` pair. No host, token, organization id or user name
belongs in a file in this repository — `.gitignore` covers `*.tfvars` and every state
file for that reason.

**Import comes first, and skipping it is the mistake this paragraph exists to prevent.**
[ADR 0008] records the group as created by hand and existing, and Terraform's state here
is empty, so a first `apply` would try to *create* it. Adopt it into state first:

```hcl
# a temporary block, deleted once the import has run
import {
  to = databricks_group.pii_readers
  id = "<group-id>"     # the SCIM id: databricks groups list, or the SCIM API
}
```

Then, in order, with the plan written to a file so that what is applied is what was read:

```bash
cd iac
terraform init
terraform plan -out=tfplan      # reads the workspace; needs credentials
terraform apply tfplan          # NEVER RUN FROM THIS REPOSITORY
```

**What `apply` would do that nothing here has watched:** adopt an existing group, and —
if `pii_reader_user_names` is not empty — add a member to the group whose membership is
what lets a real identity read real personal names. `lifecycle { prevent_destroy }` on
the group has likewise never refused anything, because nothing has been applied.

## Bumping the provider

```bash
cd iac
# edit versions.tf, then:
terraform init -upgrade
terraform providers lock -platform=linux_amd64 -platform=windows_amd64
```

The second command is not optional. A lock file written on one platform makes
`terraform init` fail on the other with a message about the lock rather than about this
configuration — a red build that says nothing about the change that caused it.

[ADR 0008]: ../docs/adr/0008-pii-masking-socios.md
[ADR 0018]: ../docs/adr/0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md
