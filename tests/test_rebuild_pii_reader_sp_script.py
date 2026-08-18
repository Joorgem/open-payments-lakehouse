# tests/test_rebuild_pii_reader_sp_script.py
"""Unit tests for scripts/rebuild_pii_reader_sp.py.

Hermetic: no network, no credentials. `FakeServicePrincipals` and `FakeRuleSets` stand
in for the two SDK surfaces this script drives.

WHAT IS WORTH PINNING HERE IS NOT THE HTTP. Every remote call in this script was
measured against the live workspace and the surprising parts are recorded in its
header -- the numeric-id-versus-UUID split, the rule-set proxy being served from a
workspace host. A unit test cannot re-derive any of that. What it CAN hold is the two
decisions a future edit would most plausibly undo, and both of them are safety
properties: that the script never writes a secret anywhere, and that it does not
default to putting a principal into the group that opens 55.8M real personal names.
"""
from __future__ import annotations

import ast
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from opl.bronze.pii_governance import PII_READER_GROUP

_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "rebuild_pii_reader_sp.py"
_spec = importlib.util.spec_from_file_location("rebuild_pii_reader_sp_cli", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

_APP = "d0e35b43-be45-4466-b4b7-6eec2d3a1fc8"
_SCIM = "78647837742784"


class FakeServicePrincipals:
    """`WorkspaceClient.service_principals`, with a `list` that filters on nothing --
    the test supplies what the filter would have found."""

    def __init__(self, existing: list[SimpleNamespace] | None = None) -> None:
        self.existing = list(existing or [])
        self.created: list[str] = []

    def list(self, filter: str = ""):  # noqa: A002 - the SDK's own parameter name
        return list(self.existing)

    def create(self, display_name: str, active: bool = True):
        self.created.append(display_name)
        return SimpleNamespace(application_id=_APP, id=_SCIM, display_name=display_name)


class FakeRuleSets:
    """`WorkspaceClient.account_access_control_proxy`, recording the whole rule list it
    is handed -- which is the property that matters, because the update is a FULL
    REPLACEMENT and an append-only reading of it silently drops the manager role."""

    def __init__(self, rules: list[object] | None = None) -> None:
        self.rules = list(rules or [])
        self.updated: list[list[object]] = []

    def get_rule_set(self, name: str, etag: str):
        return SimpleNamespace(etag="etag-1", grant_rules=list(self.rules))

    def update_rule_set(self, name: str, rule_set):
        self.updated.append(list(rule_set.grant_rules))
        return rule_set


def test_an_existing_principal_is_adopted_rather_than_duplicated():
    """Two principals with one display name are indistinguishable in every UI and only
    one of them holds the grants. Idempotence by display name is the only handle an
    operator has before the ids exist."""
    existing = SimpleNamespace(application_id=_APP, id=_SCIM, display_name="opl-pii-reader")
    api = FakeServicePrincipals([existing])
    found = cli.find_or_create(SimpleNamespace(service_principals=api), "opl-pii-reader")
    assert found is existing
    assert api.created == []


def test_a_missing_principal_is_created():
    api = FakeServicePrincipals([])
    created = cli.find_or_create(SimpleNamespace(service_principals=api), "opl-pii-reader")
    assert api.created == ["opl-pii-reader"]
    assert created.application_id == _APP and created.id == _SCIM


def test_the_run_as_grant_keeps_every_rule_that_was_already_there():
    """A FULL REPLACEMENT, so the existing rules must be read and sent back. Dropping
    them is how an operator loses `servicePrincipal.manager` on a principal they just
    created and can no longer administer -- a state no re-run of this script repairs,
    because the repair needs the role that was dropped."""
    manager = cli.iam.GrantRule(principals=["users/a@b.c"], role="roles/servicePrincipal.manager")
    rule_sets = FakeRuleSets([manager])
    cli.grant_run_as(SimpleNamespace(account_access_control_proxy=rule_sets),
                     "accounts/x/servicePrincipals/y/ruleSets/default", "users/a@b.c")
    assert len(rule_sets.updated) == 1
    roles = [rule.role for rule in rule_sets.updated[0]]
    assert "roles/servicePrincipal.manager" in roles
    assert cli.RUN_AS_ROLE in roles


def test_the_run_as_grant_is_a_no_op_when_the_role_is_already_held():
    """Measured against the live workspace: the second run prints and sends nothing."""
    held = cli.iam.GrantRule(principals=["users/a@b.c"], role=cli.RUN_AS_ROLE)
    rule_sets = FakeRuleSets([held])
    cli.grant_run_as(SimpleNamespace(account_access_control_proxy=rule_sets),
                     "accounts/x/servicePrincipals/y/ruleSets/default", "users/a@b.c")
    assert rule_sets.updated == []


def test_the_group_is_not_defaulted_to_the_one_that_opens_the_real_names():
    """THE SAFETY DECISION, and it is one argument away from being undone.

    `opl_pii_readers` is empty by decision: membership is what lets a real identity
    read 55.8M real personal names. A `--group` that defaulted to it would make
    "rebuild the principal" and "authorise it over production personal data" the same
    command."""
    parsed = cli._parse([])
    assert parsed.group is None
    assert PII_READER_GROUP not in (parsed.group or "")
    assert cli._parse(["--group", "_probe_pii_readers"]).group == "_probe_pii_readers"


def test_the_script_writes_no_file_at_all():
    """THE SECRET NEVER LANDS ON DISK, asserted over the source rather than over a run.

    A run-based test would only cover the paths it happened to take; the hazard is a
    future edit that "helpfully" saves the credential to `.env` for the operator --
    which is one line, reads as a kindness, and puts a live secret in a working tree
    that also contains a git repository."""
    tree = ast.parse(_SCRIPT_PATH.read_text(encoding="utf-8"))
    called = {
        node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", "")
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
    }
    forbidden = called & {"open", "write_text", "write_bytes", "writelines", "dump", "mkdir"}
    assert not forbidden, (
        f"scripts/rebuild_pii_reader_sp.py calls {sorted(forbidden)}. This script prints "
        "an OAuth secret; nothing it does may put one in a file."
    )


def test_the_secret_is_printed_and_not_returned():
    """A returned secret is a secret a caller can log, and this script is the kind of
    thing a future wrapper would call."""
    minted = SimpleNamespace(secret="s3cr3t-not-real")
    proxy = SimpleNamespace(create=lambda service_principal_id: minted)
    result = cli.mint_secret(SimpleNamespace(service_principal_secrets_proxy=proxy),
                             _SCIM, _APP)
    assert result is None


@pytest.mark.parametrize("argv", [[], ["--no-secret"]])
def test_the_parser_accepts_the_two_shapes_the_header_documents(argv):
    parsed = cli._parse(argv)
    assert parsed.display_name == cli.DEFAULT_DISPLAY_NAME
    assert parsed.profile == "opl-free"
    assert parsed.no_secret == ("--no-secret" in argv)
