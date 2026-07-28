# tests/test_fail_on_dq_task.py
"""Unit tests for the `databricks/src/fail_on_dq.py` job task, whose error
message is its entire deliverable: it is the first instruction a triager gets
(ADR 0006's workflow starts with "a human has read the quarantine"), and the task
is shared by two jobs that quarantine to different tables.

Loaded by path with the same importlib pattern as `tests/test_extract_cnpj_cli.py`
-- the `databricks/src` scripts are job entry points, not part of the opl wheel.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "fail_on_dq_task", _REPO / "databricks" / "src" / "fail_on_dq.py"
)
assert _spec is not None and _spec.loader is not None
task = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(task)


def test_it_names_the_estabelecimentos_quarantine_table():
    """The defect this locks down: the estab job's failures pointed operators at
    the lookup quarantine, a table holding 7,408 unrelated F1.2 rows."""
    with pytest.raises(RuntimeError) as excinfo:
        task.main(["bronze_cnpj_estab_quarantine"])

    assert "workspace.default.bronze_cnpj_estab_quarantine" in str(excinfo.value)
    assert "bronze_cnpj_lookup_quarantine" not in str(excinfo.value)


def test_the_lookup_message_is_byte_for_byte_what_f1_2_shipped():
    with pytest.raises(RuntimeError) as excinfo:
        task.main(["bronze_cnpj_lookup_quarantine"])

    assert str(excinfo.value) == (
        "DQ gate rejected rows - promotion blocked; see the quarantine table "
        "(workspace.default.bronze_cnpj_lookup_quarantine) for reject reasons."
    )


def test_an_already_qualified_table_name_is_not_qualified_twice():
    with pytest.raises(RuntimeError) as excinfo:
        task.main(["workspace.default.bronze_cnpj_estab_quarantine"])

    assert "(workspace.default.bronze_cnpj_estab_quarantine)" in str(excinfo.value)


def test_it_still_fails_the_run_when_no_table_is_passed():
    """Its one job is to fail the run: a missing parameter must not turn a
    blocked batch into a green one."""
    with pytest.raises(RuntimeError, match="no table name"):
        task.main([])


def test_each_bronze_job_passes_its_own_quarantine_table():
    """Locks the wiring, not just the script. These values must match the
    QUARANTINE constants in databricks/src/dq_gate.py and dq_gate_batch.py, which
    cannot be imported here (they pull in databricks.sdk.runtime, which needs a
    workspace to import at all)."""
    resources = _REPO / "databricks" / "resources"
    lookup = (resources / "bronze_job.yml").read_text(encoding="utf-8")
    estab = (resources / "bronze_estabelecimentos_job.yml").read_text(encoding="utf-8")

    assert 'parameters: ["bronze_cnpj_lookup_quarantine"]' in lookup
    assert 'parameters: ["bronze_cnpj_estab_quarantine"]' in estab
