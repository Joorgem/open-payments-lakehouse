# tests/test_ensure_masked_table_task.py
"""Unit test for the `databricks/src/ensure_masked_table.py` job task.

What is under test is ORDER. The task exists because every other bronze table is
created by `promote_batch`'s `saveAsTable(...)` in append mode, which for a table
holding personal names would create it with the names already in it and let the
mask arrive afterwards. So the properties that matter are which statement is
issued before which, and they are asserted against a recorded statement list --
no Spark, no Unity Catalog. `SparkSession` is replaced rather than stubbed at the
`sql` boundary so that a task which built a real session would be visible here.

Loaded by path with the same importlib pattern as `tests/test_unzip_table_task.py`
-- the `databricks/src` scripts are job entry points, not part of the opl wheel.
"""
from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest

from opl.bronze.masking import MASK_FUNCTION, MASKED_COLUMNS
from opl.bronze.registry import UnknownTable, table_spec
from opl.config import DEFAULT

_SRC = Path(__file__).resolve().parents[1] / "databricks" / "src"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _RecordingSession:
    """Records every statement, in order, and returns nothing. A `mock.Mock()` would
    auto-create whatever the task asked of it, so a task that stopped issuing the
    CREATE TABLE would still pass against one."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, statement: str):
        self.statements.append(statement)
        return None


class _SessionFactory:
    """Stands in for the `SparkSession` class: `.builder.getOrCreate()` hands back the
    one recorder. Also records THAT a session was asked for, which is what the no-op
    test needs -- a task that builds a serverless session before deciding it has
    nothing to do costs a cluster start on every table that declares no mask."""

    def __init__(self, session: _RecordingSession) -> None:
        self.session = session
        self.built = 0

    @property
    def builder(self):
        return self

    def getOrCreate(self) -> _RecordingSession:
        self.built += 1
        return self.session


def _run(argv: list[str], monkeypatch) -> tuple[_SessionFactory, _RecordingSession]:
    task = _load("ensure_masked_table")
    session = _RecordingSession()
    factory = _SessionFactory(session)
    monkeypatch.setattr(task, "SparkSession", factory)
    task.main(argv)
    return factory, session


def test_every_table_is_created_before_any_mask_and_the_function_before_the_first_one(
    monkeypatch,
):
    """THE ORDERING THIS TASK EXISTS FOR, asserted as positions in one list.

    Three claims, and each one is a real failure if it moves. Every CREATE TABLE
    must come before every mask, or the mask has nothing to attach to and the
    writer creates the table instead -- with the names in it. The function must
    exist before the first `SET MASK`, or the ALTER fails on a missing routine. And
    every masked column of every covered table must be reached before the task
    returns, because the moment it returns the job goes on to unzip and ingest."""
    _, session = _run(["socios"], monkeypatch)

    kinds = [statement.split(" ", 3)[:3] for statement in session.statements]
    creates = [i for i, k in enumerate(kinds) if k == ["CREATE", "TABLE", "IF"]]
    function = next(i for i, k in enumerate(kinds) if k == ["CREATE", "OR", "REPLACE"])
    masks = [i for i, s in enumerate(session.statements) if "SET MASK" in s]

    assert creates == [0, 1], "something is issued before the tables are created"
    assert function < min(masks), "a mask is set before its function exists"
    assert len(masks) == len(_COVERED_ROLES) * len(MASKED_COLUMNS["socios"])
    assert len(session.statements) == len(creates) + 1 + len(masks), (
        f"the task issues a statement this lock does not know about: {session.statements}"
    )


# The tables the control covers, by BronzeTable field. Staging is deliberately not
# one of them -- see `test_staging_is_never_named_by_this_task`.
_COVERED_ROLES = ("bronze", "quarantine")


def test_it_masks_every_column_of_every_covered_table_and_no_other(monkeypatch):
    """Both name columns, on both covered tables. Masking one of the two columns is
    the defect this task's 'two columns, not one' argument is about and is one
    deleted line away; masking bronze alone is the gap two reviewers found in ADR
    0008, and quarantine is the table a human is EXPECTED to open during triage."""
    _, session = _run(["socios"], monkeypatch)
    spec = table_spec("socios")
    function = DEFAULT.table(MASK_FUNCTION)

    masked = [s for s in session.statements if "SET MASK" in s]
    assert masked == [
        f"ALTER TABLE {DEFAULT.table(getattr(spec, role))} "
        f"ALTER COLUMN `{column}` SET MASK {function}"
        for role in _COVERED_ROLES
        for column in MASKED_COLUMNS["socios"]
    ]


def test_staging_is_never_named_by_this_task(monkeypatch):
    """THE EXCLUSION, at the layer that issues the statements.

    `promote_batch` READS staging and appends what it read into bronze, and a UC
    column mask applies to that read for every principal the mask function does not
    admit -- the live run saw `***` returned to the table owner's own query. So a
    `SET MASK` on staging would put `***` into bronze permanently and would make the
    DQ gate evaluate `null_or_empty_nome_socio_razao_social` against a value that is
    neither null nor empty, silently ending the rejections. The 1,797 rows quarantined
    in the F1.4b run are the measure of what that rule catches.

    `opl.bronze.masking.masked_table_ddls` carries the argument and
    `test_the_control_covers_bronze_and_quarantine_and_never_staging` pins the DDL
    side; this pins the SQL that actually reaches the workspace, because the two
    could come apart through a table name spelled here rather than resolved there."""
    _, session = _run(["socios"], monkeypatch)
    staging = DEFAULT.table(table_spec("socios").staging)

    offenders = [s for s in session.statements if staging in s]
    assert not offenders, (
        f"this task named {staging}: {offenders}. Masking staging corrupts bronze on "
        "the next promote and disables the missing-name rule -- see ADR 0008. It "
        "becomes correct only once the job's run-as principal is a member of "
        "opl_pii_readers, which F4 created EMPTY."
    )


def test_every_statement_names_the_catalog_and_schema_the_config_owns(monkeypatch):
    """No bare table name reaches Unity Catalog. A statement issued against the
    session's current catalog would silently create or mask a DIFFERENT table
    depending on what the job's default happened to be."""
    _, session = _run(["socios"], monkeypatch)
    qualified = f"{DEFAULT.catalog}.{DEFAULT.schema}."
    for statement in session.statements:
        assert qualified in statement, f"unqualified object in: {statement}"


def test_a_table_that_declares_no_masked_column_is_a_no_op(monkeypatch, capsys):
    """So the same task can sit in any job's YAML without a per-table branch -- and
    so that adding it to the empresas job does not quietly create an EMPTY bronze
    table by hand for a contract whose schema nobody pinned.

    No session is built at all: the decision is made from the registry, before
    Spark, the way `table_spec` and `require_month` refuse."""
    factory, session = _run(["empresas"], monkeypatch)
    assert factory.built == 0, "a serverless session was started for a table with no mask"
    assert session.statements == []
    assert "no masked column" in capsys.readouterr().out


def test_an_unknown_table_is_refused_naming_the_real_ones(monkeypatch):
    """Before Spark, from the same registry as every other task -- this one runs
    FIRST in its job, so a typo here should not cost a session start."""
    task = _load("ensure_masked_table")
    monkeypatch.setattr(task, "SparkSession", _SessionFactory(_RecordingSession()))
    with pytest.raises(UnknownTable) as excinfo:
        task.main(["socio"])  # a real typo: singular
    assert "socios" in str(excinfo.value)


@pytest.mark.parametrize("argv", [[], [""]])
def test_no_table_at_all_is_refused_rather_than_defaulted(argv, monkeypatch):
    task = _load("ensure_masked_table")
    monkeypatch.setattr(task, "SparkSession", _SessionFactory(_RecordingSession()))
    with pytest.raises(UnknownTable):
        task.main(argv)


def test_the_entry_point_never_raises_system_exit():
    """Serverless runs these under IPython, where an uncaught `SystemExit` reports a
    SUCCESSFUL run as FAILED. Every other task under databricks/src calls a bare
    `main()`; this one is new, so the shape is pinned rather than assumed."""
    source = (_SRC / "ensure_masked_table.py").read_text(encoding="utf-8")
    assert "SystemExit" not in source
    assert source.rstrip().endswith('if __name__ == "__main__":\n    main()')


def _docstrings(tree: ast.Module) -> set[int]:
    """`id()` of every string constant that is a docstring rather than a value."""
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
    """The DDL lives in `opl.bronze.masking`, where it is unit-tested against the
    contract and against a real Delta append. DDL re-spelled here is DDL nothing
    checks -- the same reason `promote_batch` holds none.

    Over the STRING LITERALS the module evaluates, not over its text. Naming the
    statements in prose is this repo's house style and is how the idempotence
    argument is recorded; the sibling lock in test_task_wiring.py strips comment
    lines for the same reason, and a module docstring is the same kind of writing."""
    tree = ast.parse((_SRC / "ensure_masked_table.py").read_text(encoding="utf-8"))
    prose = _docstrings(tree)
    literals = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in prose
    ]
    for statement in ("ALTER TABLE", "CREATE TABLE", "CREATE OR REPLACE"):
        offenders = [text for text in literals if statement in text]
        assert not offenders, f"ensure_masked_table.py spells {statement} itself: {offenders}"
