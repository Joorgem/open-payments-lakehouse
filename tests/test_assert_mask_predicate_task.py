# tests/test_assert_mask_predicate_task.py
"""Unit test for the `databricks/src/assert_mask_predicate.py` job task.

WHAT IS UNDER TEST IS THAT IT CAN FAIL, which is the whole reason the task exists. The
check it replaces -- "after the run, `SELECT nome_socio_razao_social FROM
bronze_cnpj_socios` reads `***`" -- is true under the repair, true without it, true when
the mask task returned early and true when it never ran, because `opl_pii_readers` is
empty by decision and both predicates are therefore false for every principal. So the
fixture that matters here is the body this workspace ACTUALLY serves, verbatim, and the
assertion that matters is that the task refuses it.

No Spark and no Unity Catalog: `SparkSession` is replaced rather than stubbed at the
`sql` boundary, so a task that built a real session would be visible here. Same
importlib-by-path pattern as the other `databricks/src` task tests -- these scripts are
job entry points, not part of the opl wheel.
"""
from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest

from opl.bronze.masking import MASK_FUNCTION, MASK_PREDICATE, StaleMaskPredicate
from opl.config import DEFAULT

_SRC = Path(__file__).resolve().parents[1] / "databricks" / "src"

# The body `workspace.information_schema.routines` returns for `mask_personal_name`
# today, character for character, `last_altered 2026-08-03T21:31:27.142Z` (re-measured
# 2026-08-19). Quoted rather than paraphrased: this string is the no-op the task has to
# tell from a repair.
_DEPLOYED_TODAY = (
    "CASE WHEN is_account_group_member('opl_pii_readers') THEN name ELSE '***' END"
)
_REPAIRED = f"CASE WHEN {MASK_PREDICATE} THEN name ELSE '***' END"
_ALTERED = "2026-08-03T21:31:27.142Z"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Routines:
    """What the routines read hands back: zero or one row, keyed by column name."""

    def __init__(self, rows: list[dict[str, str]]) -> None:
        self._rows = rows

    def collect(self) -> list[dict[str, str]]:
        return list(self._rows)


class _RecordingSession:
    """Records every statement and answers the routines read from its own state.

    Answers INDEPENDENTLY of what it was handed, for the reason the sibling governance
    double gives: a session that echoed the task's own intent could not express the case
    this task exists for, which is a catalog that disagrees with the deploy."""

    def __init__(self, definition: str | None) -> None:
        self.statements: list[str] = []
        self._definition = definition

    def sql(self, statement: str):
        self.statements.append(statement)
        if self._definition is None:
            return _Routines([])
        return _Routines(
            [{"routine_definition": self._definition, "last_altered": _ALTERED}]
        )


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


def _run(definition: str | None, argv: list[str] | None = None):
    task = _load("assert_mask_predicate")
    session = _RecordingSession(definition)
    factory = _SessionFactory(session)
    task.SparkSession = factory
    task.main([] if argv is None else argv)
    return factory, session


def test_the_body_this_workspace_serves_today_fails_the_run():
    """THE ASSERTION THE PUBLISHED CHECK COULD NOT MAKE.

    Under the deployed `is_account_group_member` body every reader sees `***`, exactly
    as they do under the repair -- so the `SELECT ... LIMIT 1` that was published as the
    safety gate returns the same value either way. This is what tells them apart, and it
    has to be red against the string the catalog returns right now, or the task is the
    same green tick in a different place."""
    task = _load("assert_mask_predicate")
    session = _RecordingSession(_DEPLOYED_TODAY)
    task.SparkSession = _SessionFactory(session)
    with pytest.raises(StaleMaskPredicate, match="does not carry"):
        task.main([])


def test_the_repaired_body_passes_and_costs_exactly_one_statement():
    """The other direction, and the cost. One read of `information_schema.routines`:
    no table is opened, no row of the 55.8M is fetched, and nothing is written -- which
    is what makes a task acceptable in a job whose other four already cost a start."""
    _, session = _run(_REPAIRED)
    assert len(session.statements) == 1, session.statements
    assert session.statements[0].startswith("SELECT routine_definition, last_altered")


def test_it_reads_the_function_the_config_and_the_masking_module_name():
    """No second spelling of `workspace.default.mask_personal_name`. A task that named
    its own would read back a function nothing masks with."""
    _, session = _run(_REPAIRED)
    function = DEFAULT.table(MASK_FUNCTION)
    catalog, schema, name = function.split(".")
    assert f"{catalog}.information_schema.routines" in session.statements[0]
    assert f"routine_schema = '{schema}'" in session.statements[0]
    assert f"routine_name = '{name}'" in session.statements[0]


def test_an_absent_routine_is_a_failure_and_not_an_opinionless_pass():
    """Zero rows means the mask's function is not in the catalog at all, in a job whose
    previous task exists to create it. A task that read "no row" as "nothing to say"
    would be green over four column masks that resolve to nothing."""
    task = _load("assert_mask_predicate")
    session = _RecordingSession(None)
    task.SparkSession = _SessionFactory(session)
    with pytest.raises(StaleMaskPredicate, match="not in information_schema.routines"):
        task.main([])


def test_it_refuses_an_argument_rather_than_ignoring_it():
    """A task handed a parameter it does not read is a job YAML that believes it is
    configuring something. There is exactly one mask function and the module names it."""
    with pytest.raises(ValueError, match="takes no arguments"):
        _run(_REPAIRED, argv=["socios"])


def test_it_starts_no_session_when_no_contract_declares_a_mask():
    """The same shape `ensure_masked_table` refuses to pay: a task that builds a
    serverless session before deciding it has nothing to do costs a start for nothing.
    Reached by emptying the mask registry, which is the only state in which it holds."""
    task = _load("assert_mask_predicate")
    task.MASKED_COLUMNS = {}
    factory = _SessionFactory(_RecordingSession(_REPAIRED))
    task.SparkSession = factory
    task.main([])
    assert factory.built == 0


def test_the_entry_point_never_raises_system_exit():
    """Serverless runs these under IPython, where an uncaught `SystemExit` reports a
    SUCCESSFUL run as FAILED."""
    source = (_SRC / "assert_mask_predicate.py").read_text(encoding="utf-8")
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


def test_the_task_holds_no_sql_and_no_predicate_of_its_own():
    """The statement lives in `opl.bronze.masking`, beside the DDL it reads back, where
    both are unit-tested. A predicate re-spelled here is a check that can agree with
    itself while disagreeing with the function this project deploys.

    WHAT IS BANNED IS A STATEMENT AND A PREDICATE, NOT THE NAME OF THE CATALOG THIS TASK
    READS. The first spelling of this lock refused `information_schema` anywhere, and it
    went red on the task's own failure message -- the one an operator reads when the
    function is missing, whose whole job is to say WHERE it was looked for. That is the
    same trap `test_no_module_that_runs_on_databricks_asks_the_engine_to_cache` records:
    a guard that cannot tell a statement from a mention of one punishes explaining it.
    Column names stay allowed for the reason the sibling governance lock allows
    `Principal` and `ActionType`: the task is what knows the shape of the result."""
    tree = ast.parse((_SRC / "assert_mask_predicate.py").read_text(encoding="utf-8"))
    prose = _docstrings(tree)
    literals = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in prose
    ]
    for fragment in ("SELECT ", "FROM ", "WHERE ", "is_member"):
        offenders = [text for text in literals if fragment in text]
        assert not offenders, (
            f"assert_mask_predicate.py spells {fragment} itself: {offenders}"
        )
