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


def test_the_table_is_created_before_any_mask_and_the_function_before_the_first_one(
    monkeypatch,
):
    """THE ORDERING THIS TASK EXISTS FOR, asserted as positions in one list.

    Three claims, and each one is a real failure if it moves. The CREATE TABLE must
    come first, or the mask has nothing to attach to and the append creates the
    table instead -- with the names in it. The function must exist before the first
    `SET MASK`, or the ALTER fails on a missing routine. And every masked column
    must be covered before the task returns, because the moment it returns the job
    goes on to unzip and ingest."""
    _, session = _run(["socios"], monkeypatch)

    kinds = [statement.split(" ", 3)[:3] for statement in session.statements]
    create = next(i for i, k in enumerate(kinds) if k == ["CREATE", "TABLE", "IF"])
    function = next(i for i, k in enumerate(kinds) if k == ["CREATE", "OR", "REPLACE"])
    masks = [i for i, s in enumerate(session.statements) if "SET MASK" in s]

    assert create == 0, "something is issued before the table is created"
    assert function < min(masks), "a mask is set before its function exists"
    assert len(masks) == len(MASKED_COLUMNS["socios"])
    assert len(session.statements) == 2 + len(masks), (
        f"the task issues a statement this lock does not know about: {session.statements}"
    )


def test_it_masks_every_column_the_contract_declares_and_no_other(monkeypatch):
    """Both name columns. Masking one of the two is the defect this whole task's
    'two columns, not one' argument is about, and it is one deleted line away."""
    _, session = _run(["socios"], monkeypatch)
    table = DEFAULT.table(table_spec("socios").bronze)
    function = DEFAULT.table(MASK_FUNCTION)

    masked = [s for s in session.statements if "SET MASK" in s]
    assert masked == [
        f"ALTER TABLE {table} ALTER COLUMN `{column}` SET MASK {function}"
        for column in MASKED_COLUMNS["socios"]
    ]


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
