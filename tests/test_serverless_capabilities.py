"""What the DEPLOY TARGET refuses that local Spark allows -- swept over every module that
can run inside a job.

SPLIT OUT OF `tests/test_task_wiring.py`, which reached 831 of this project's 800-line
limit when F-API's fix pass gave the two PTAX entry points the coordinate locks the other
five already had. §4.9/§4.12 say whoever touches a file at the cap splits it first.

THE SEAM IS A DIFFERENT SUBJECT AND NOT A LINE COUNT, and that file's own docstring drew it
by putting this block under a section header of its own. Everything left there asks WHICH
TABLE a task touches: one spec resolved from argv, and every coordinate a field of it. This
asks nothing about tables at all -- it asks whether a module uses a capability serverless
does not have, over `src/opl/**` plus `databricks/src/*`, which is a set neither of that
file's other locks looks at. Two reasons to edit are two files: that half changes when an
ENTRY POINT changes, this one changes when the PLATFORM refuses something new.

IT IS ALSO ONE OF THE TWO PARAMETRISED SWEEPS EVERY NEW SOURCE FILE PAYS FOR (protocol
§4.10, the other being `test_revision_stamp.py`'s git-at-runtime ban), so the module it
lives in is the one a new file's two ids come from. Moving it changes the ids' module and
not their count.

Nothing here starts Spark: every assertion is over the AST of a source file."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

_CACHING_CALLS = frozenset({"persist", "cache", "unpersist"})


def _python_sources_that_run_on_databricks() -> list[Path]:
    """Every module that can execute inside a job: the wheel's `opl` package and the
    entry points beside it. Test code is excluded because it runs on local Spark, where
    caching is supported and is a legitimate thing for a fixture to do."""
    repo = Path(__file__).resolve().parents[1]
    return sorted((repo / "src" / "opl").rglob("*.py")) + sorted(
        (repo / "databricks" / "src").glob("*.py")
    )


@pytest.mark.parametrize(
    "path", _python_sources_that_run_on_databricks(), ids=lambda p: p.name
)
def test_no_module_that_runs_on_databricks_asks_the_engine_to_cache(path: Path):
    """`persist()`/`cache()` are a HARD REFUSAL on serverless, not a slow path.

    THIS IS A REGRESSION GUARD FOR A FAILURE THAT ONLY THE DEPLOY TARGET PRODUCES, which
    is why it is a source check rather than a behavioural one. Local Spark supports
    caching happily, so every test in this repository passes with a `persist()` in place
    and the refusal appears for the first time in the workspace:

        [NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless
        compute. SQLSTATE: 0A000

    Measured 2026-08-09 on run `604594149706864`, where `effectivity._append_and_count
    _closes` called `rows.persist()` and failed the task twice. Nothing was written --
    the call precedes both the count and the append -- but the phase lost the run, and a
    reviewer had independently recommended adding a SECOND `persist()` a few lines above
    it, which would have failed the same way.

    The remedy for a frame consumed twice on this platform is to materialise it or to
    restructure so it is read once. Asking the engine to hold it is not available.

    THE CHECK IS OVER THE AST AND NOT OVER THE TEXT, which is not fastidiousness: the
    first spelling of this guard matched substrings and went red on `effectivity.py`,
    whose docstring now EXPLAINS the refusal and therefore contains `rows.persist()` as
    prose. A guard that cannot tell a call from a mention of a call punishes documenting
    the very thing it is guarding."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    offenders = sorted({
        f"{node.func.attr}() at line {node.lineno}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _CACHING_CALLS
    })
    assert not offenders, (
        f"{path.name} calls {offenders} -- serverless refuses explicit caching outright "
        "(NOT_SUPPORTED_WITH_SERVERLESS), so this runs locally and fails in the workspace"
    )
