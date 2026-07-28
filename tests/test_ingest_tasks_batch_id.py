# tests/test_ingest_tasks_batch_id.py
"""Both ingest job tasks must refuse to run without a batch id.

They used to default it to the literal `"manual"`. That is worse than a crash:
the gate and the promote are scoped to `_batch_id == {{job.run_id}}`, so rows
tagged `"manual"` land in staging and are then never evaluated, never promoted
and never reported anywhere -- a silent hole, not a failure. Both job YAMLs do
pass `{{job.run_id}}`, so the default was only ever reachable by a manual or
misconfigured invocation, which is exactly the case that must be loud.

Loaded by path with the same importlib pattern as the other task tests -- the
`databricks/src` scripts are job entry points, not part of the opl wheel. No JVM
and no workspace: `main` refuses before it touches Spark, which is the point.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from opl.bronze.promote import PromoteRefused

_SRC = Path(__file__).resolve().parents[1] / "databricks" / "src"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("script", ["bronze_ingest", "bronze_estab_ingest"])
@pytest.mark.parametrize("argv", [[], [""], ["   "]])
def test_an_ingest_without_a_batch_id_is_refused(script, argv):
    module = _load(script)
    with pytest.raises(PromoteRefused) as excinfo:
        module.main(argv)
    # The operator has to learn WHICH id is missing and where it comes from.
    assert "batch" in str(excinfo.value).lower()


@pytest.mark.parametrize("script", ["bronze_ingest", "bronze_estab_ingest"])
def test_the_batch_id_goes_through_the_shared_guard(script):
    """Lock the property, not a spelling.

    Two weaker locks were tried and rejected: grepping for the old literal
    `"manual"` also matches the comment recording why it was removed, and
    grepping for an `else '...'` fallback also matches this module's own correct
    `args[0] if args else ""`, which passes the empty string INTO the guard on
    purpose. What actually must hold is that the id is validated before use.
    """
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    assert "require_batch_id(" in source, (
        f"{script}.py no longer routes its batch id through require_batch_id -- "
        "an unvalidated id lands rows that are never gated, promoted or reported, "
        "because both the gate and the promote are scoped to the run id"
    )
