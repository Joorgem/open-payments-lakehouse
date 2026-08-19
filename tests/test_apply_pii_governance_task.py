# tests/test_apply_pii_governance_task.py
"""Unit test for the `databricks/src/apply_pii_governance.py` job task.

What is under test is ORDER and TOTALITY, the same two things
`test_ensure_masked_table_task.py` asks of the task this one is the other half of.
Order, because a run that dies partway through must leave FEWER readers than it found:
every REVOKE is issued before any GRANT, across every table, and the inert tags go
last. Totality, because the tables it must reach include the one the mask task
deliberately refuses to name.

`_RecordingSession` answers `SHOW GRANTS` from its own arguments, INDEPENDENTLY of the
statements it has been handed -- the same independence `tests/test_backfill_masks.py`
argues for. A double that answered the probe out of the GRANTs it had just recorded
could not express the case the revoke half exists for: a principal that acquired
`SELECT` out of band, which no statement of this run's would mention.

AND IT ANSWERS WITH FOUR COLUMNS, because two of them decide whether a REVOKE would do
anything. `ALL PRIVILEGES` on the table and a `SELECT` inherited from the SCHEMA both
confer a read; `REVOKE SELECT ON TABLE` succeeds against either and removes neither
(measured 2026-08-18). A double that only ever handed back a direct table-level SELECT
could not have caught either hole, so `inherited` and `all_privileges` exist here to
make those two states expressible.

THE ROSTER IS SUBSTITUTED IN ONE TEST, and that is a repair rather than a convenience.
`PII_READERS` is empty by decision, so with it bound as `plan_grants`'s default every
fixture in this file produced an empty `plan.grant` -- and the ordering test below,
whose whole subject is that every REVOKE precedes every GRANT, could not fail: swapping
the two loops in `_apply_grants` left the file green.
"""
from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest

from opl.bronze import pii_governance
from opl.bronze.pii_governance import (
    CLASSIFIED_COLUMNS,
    GOVERNED_ROLES,
    UngovernedRead,
)
from opl.bronze.registry import table_spec
from opl.config import DEFAULT

_SRC = Path(__file__).resolve().parents[1] / "databricks" / "src"

# INVENTED: the shape the platform requires of a service principal's grant principal
# (an applicationId, i.e. a UUID) and the name of nothing in any workspace. The earlier
# fixture was the real applicationId of F4's deleted probe principal.
_SP = "5f2c8a10-0d4e-4b77-9a31-6c0e7d21b845"
_DECLARED = "zz-declared-reader@example.com"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Grants:
    """What `SHOW GRANTS ON TABLE` hands back, measured:
    `Principal | ActionType | ObjectType | ObjectKey`, one row per
    (principal, privilege, securable)."""

    def __init__(self, rows: list[dict[str, str]]) -> None:
        self._rows = rows

    def collect(self) -> list[dict[str, str]]:
        return list(self._rows)


class _RecordingSession:
    """Records every statement in order and answers the grants probe from its own state.

    Recording and not a no-op double: the assertions that matter here are about which
    statement came before which, and a `mock.Mock()` would auto-create whatever the
    task asked of it -- so a task that stopped issuing REVOKE entirely would still
    pass against one.

    Three rosters of holders, because a REVOKE means something different against each:
    `observed` holds a direct table-level SELECT (revocable), `all_privileges` holds
    `ALL PRIVILEGES` on the table, and `inherited` holds a SELECT granted on the SCHEMA.
    The last two survive `REVOKE SELECT ON TABLE`, measured."""

    def __init__(
        self,
        observed: tuple[str, ...] = (),
        inherited: tuple[str, ...] = (),
        all_privileges: tuple[str, ...] = (),
    ) -> None:
        self.statements: list[str] = []
        self._observed = observed
        self._inherited = inherited
        self._all_privileges = all_privileges

    def _rows(self, table: str) -> list[dict[str, str]]:
        schema = table.rsplit(".", 1)[0]
        return (
            [{"Principal": p, "ActionType": "SELECT", "ObjectType": "TABLE",
              "ObjectKey": table} for p in self._observed]
            + [{"Principal": p, "ActionType": "ALL PRIVILEGES", "ObjectType": "TABLE",
                "ObjectKey": table} for p in self._all_privileges]
            + [{"Principal": p, "ActionType": "SELECT", "ObjectType": "SCHEMA",
                "ObjectKey": schema} for p in self._inherited]
        )

    def sql(self, statement: str):
        self.statements.append(statement)
        if not statement.startswith("SHOW GRANTS"):
            return None
        return _Grants(self._rows(statement.rsplit(" ", 1)[-1]))


class _SessionFactory:
    def __init__(self, session: _RecordingSession) -> None:
        self.session = session
        self.built = 0

    @property
    def builder(self):
        return self

    def getOrCreate(self) -> _RecordingSession:
        self.built += 1
        return self.session


def _run(
    observed: tuple[str, ...] = (),
    argv: list[str] | None = None,
    inherited: tuple[str, ...] = (),
    all_privileges: tuple[str, ...] = (),
):
    task = _load("apply_pii_governance")
    session = _RecordingSession(observed, inherited, all_privileges)
    task.SparkSession = _SessionFactory(session)
    task.main([] if argv is None else argv)
    return session


def _socios_tables() -> list[str]:
    spec = table_spec("socios")
    return [DEFAULT.table(getattr(spec, role)) for role in GOVERNED_ROLES]


def test_every_revoke_precedes_every_grant_and_the_tags_come_last(monkeypatch):
    """THE ORDERING THIS TASK EXISTS FOR, asserted as positions in one list.

    `max_retries: 0` does not prevent a retry on INTERNAL_ERROR, so a run can die
    anywhere. The state that survives a partial run must be FEWER readers than it
    found, never more -- hence every REVOKE before any GRANT, across every table
    rather than table by table. Tags are inert metadata and go last: a tag that did
    not get applied costs a re-run; a grant that did costs a disclosure.

    THE ROSTER IS SUBSTITUTED, and until it was this test could not fail on the
    property in its name. `PII_READERS` is empty by decision, so `plan.grant` was empty
    in every fixture and there was never a GRANT for a REVOKE to precede: swapping the
    two loops in `_apply_grants` left the whole file green. With a declared reader here
    the same swap turns exactly this test red."""
    monkeypatch.setattr(pii_governance, "PII_READERS", (_DECLARED,))
    session = _run(observed=(_SP,))
    kinds = [statement.split(" ", 1)[0] for statement in session.statements]
    revokes = [i for i, k in enumerate(kinds) if k == "REVOKE"]
    grants = [i for i, k in enumerate(kinds) if k == "GRANT"]
    tags = [i for i, s in enumerate(session.statements) if "SET TAGS" in s]

    assert revokes, "the task issued no REVOKE against a catalog reporting a holder"
    assert grants, "the task issued no GRANT for a roster naming a principal"
    assert max(revokes) < min(grants), (
        "a GRANT was issued before a REVOKE finished: a run that dies in between "
        "leaves MORE readers than it found"
    )
    assert max(grants) < min(tags), "a tag is issued while a reader is still granted"
    assert all(kinds[i] == "SHOW" for i in range(min(revokes))), (
        "something other than the catalog reads runs before the first REVOKE"
    )


def test_it_reads_the_catalog_once_per_governed_table_before_changing_anything():
    """The plan is computed from what the CATALOG reports, not from what this run
    intends. A task that skipped the read could never revoke a grant issued out of
    band, which is the hole ADR 0008 records as its own weakest paragraph."""
    session = _run(observed=(_SP,))
    reads = [s for s in session.statements if s.startswith("SHOW GRANTS")]
    assert reads == [f"SHOW GRANTS ON TABLE {table}" for table in _socios_tables()]
    assert session.statements[: len(reads)] == reads


def test_it_governs_staging_which_the_mask_task_refuses_to_name():
    """THE ONE PLACE THE TWO TASKS DELIBERATELY DISAGREE, at the layer that issues the
    statements. `ensure_masked_table` never names staging, because a MASK there would
    make `promote_batch` read `***` and append it into bronze. A GRANT does not: it
    changes who may open the table and changes no value any reader gets -- and staging
    is where the exposure actually is, names in the clear, with nothing draining it."""
    session = _run(observed=(_SP,))
    staging = DEFAULT.table(table_spec("socios").staging)
    named = [s for s in session.statements if staging in s]
    assert any(s.startswith("REVOKE") for s in named), (
        f"nothing revokes on {staging}, the one socios table holding names IN THE CLEAR"
    )
    assert any("SET TAGS" in s for s in named)


def test_the_whole_statement_list_is_exactly_what_the_module_publishes():
    """A paste-lock over the run, in the same spirit as `create_dataops_views`'s. Until
    it existed, a second hand-written `spark.sql(...)` here would have issued a grant
    or a tag that no unit test in this repository knew about."""
    session = _run(observed=(_SP,))
    tables = _socios_tables()
    expected = [f"SHOW GRANTS ON TABLE {table}" for table in tables]
    expected += [f"REVOKE SELECT ON TABLE {table} FROM `{_SP}`" for table in tables]
    expected += [
        f"ALTER TABLE {table} ALTER COLUMN `{column}` SET TAGS ('{key}' = '')"
        for table in tables
        for column, keys in CLASSIFIED_COLUMNS["socios"].items()
        for key in keys
    ]
    assert session.statements == expected


def test_a_clean_catalog_and_an_empty_roster_issue_no_grant_and_no_revoke():
    """IDEMPOTENCE, stated as the second run of a successful first run. This is also
    the shape of the CURRENT workspace: no principal holds SELECT on any socios table,
    and the roster is empty by decision -- so the whole access half of this task is a
    read and nothing else, and the tags are re-applied identically."""
    session = _run(observed=())
    assert not [s for s in session.statements if s.split(" ", 1)[0] in ("GRANT", "REVOKE")]
    assert [s for s in session.statements if "SET TAGS" in s]


def test_a_read_it_cannot_withdraw_fails_the_run_after_everything_it_can_do():
    """THE MOST SEVERE HOLE THIS TASK HAD, at the layer that decides the run's verdict.

    `GRANT ALL PRIVILEGES` makes a principal a reader, `SHOW GRANTS` reports it as
    `ALL PRIVILEGES`, and `REVOKE SELECT ON TABLE` against it SUCCEEDS and removes
    nothing -- all measured. So the task must not pretend: it issues no REVOKE it
    cannot back, applies every statement it CAN, and then fails, naming the statement
    that closes the hole. A task that returned SUCCESS here would report a green tick
    over an undeclared reader of 55.8M personal names."""
    task = _load("apply_pii_governance")
    session = _RecordingSession(all_privileges=(_SP,))
    task.SparkSession = _SessionFactory(session)
    with pytest.raises(UngovernedRead, match="ALL PRIVILEGES"):
        task.main([])
    assert not [s for s in session.statements if s.startswith("REVOKE")], (
        "the task issued a REVOKE that was measured to change nothing"
    )
    assert [s for s in session.statements if "SET TAGS" in s], (
        "the loud failure cost the tagging, which was safe to do"
    )


def test_a_schema_inherited_select_is_reported_and_never_reported_as_revoked():
    """THE SILENT ONE: `GRANT SELECT ON SCHEMA` IS returned by `SHOW GRANTS ON TABLE`
    (as `ObjectType = SCHEMA`), and revoking it on the TABLE succeeds while leaving it
    in place -- so the old task printed `REVOKED SELECT on ... from ...` after a
    statement that removed nothing, forever, on every run. A claim with no evidence is
    worse here than silence, because it is what an incident responder reads."""
    task = _load("apply_pii_governance")
    session = _RecordingSession(inherited=(_SP,))
    task.SparkSession = _SessionFactory(session)
    with pytest.raises(UngovernedRead, match="SCHEMA"):
        task.main([])
    assert not [s for s in session.statements if s.startswith("REVOKE")]


def test_every_statement_names_the_catalog_and_schema_the_config_owns():
    """No bare table name reaches Unity Catalog. A `REVOKE` issued against the
    session's current catalog would silently change access on a DIFFERENT table."""
    session = _run(observed=(_SP,))
    qualified = f"{DEFAULT.catalog}.{DEFAULT.schema}."
    for statement in session.statements:
        assert qualified in statement, f"unqualified object in: {statement}"


def test_it_refuses_an_argument_rather_than_ignoring_it():
    """A task handed a parameter it does not read is a job YAML that believes it is
    configuring something. What this governs is total over the contracts that declare
    a mask, so there is no coordinate to hand it."""
    with pytest.raises(ValueError, match="takes no arguments"):
        _run(argv=["socios"])


def test_the_entry_point_never_raises_system_exit():
    """Serverless runs these under IPython, where an uncaught `SystemExit` reports a
    SUCCESSFUL run as FAILED."""
    source = (_SRC / "apply_pii_governance.py").read_text(encoding="utf-8")
    assert "SystemExit" not in source
    assert source.rstrip().endswith('if __name__ == "__main__":\n    main()')


def _docstrings(tree: ast.Module) -> set[int]:
    holders = (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, holders) and (doc := ast.get_docstring(node, clean=False)):
            for child in ast.walk(node):
                if isinstance(child, ast.Constant) and child.value == doc:
                    found.add(id(child))
                    break
    return found


def test_the_task_holds_no_ddl_of_its_own():
    """The SQL lives in `opl.bronze.pii_governance`, where it is unit-tested. SQL
    re-spelled here is SQL nothing checks -- and a `GRANT` spelled here would be a
    grant the roster does not govern and the revoke half cannot see.

    Over the STRING LITERALS the module evaluates, not over its text: naming the
    statements in prose is this repo's house style, and the sibling lock in
    `test_ensure_masked_table_task.py` strips docstrings for the same reason."""
    tree = ast.parse((_SRC / "apply_pii_governance.py").read_text(encoding="utf-8"))
    prose = _docstrings(tree)
    literals = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in prose
    ]
    for statement in ("GRANT ", "REVOKE ", "SET TAGS", "SHOW GRANTS"):
        offenders = [text for text in literals if statement in text]
        assert not offenders, (
            f"apply_pii_governance.py spells {statement} itself: {offenders}"
        )
