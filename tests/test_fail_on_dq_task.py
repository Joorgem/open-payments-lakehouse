# tests/test_fail_on_dq_task.py
"""Unit tests for the `databricks/src/fail_on_dq.py` job task, whose error
message is its entire deliverable: it is the first instruction a triager gets
(ADR 0006's workflow starts with "a human has read the quarantine"), and the task
is shared by two jobs that quarantine to different tables.

Loaded by path with the same importlib pattern as `tests/test_extract_cnpj_cli.py`
-- the `databricks/src` scripts are job entry points, not part of the opl wheel.
"""
from __future__ import annotations

import ast
import importlib.util
import re
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
    the lookup quarantine -- a table `dq_gate.py` overwrites with the LOOKUP
    gate's rejects, so it holds nothing about the batch that was blocked."""
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


def _gate_quarantine(gate_module: str, root: Path = _REPO) -> str:
    """The table a gate task writes rejects to, read WITHOUT importing the module.

    dq_gate.py and dq_gate_batch.py cannot be imported outside a workspace --
    `from databricks.sdk.runtime import dbutils` raises at import time -- so
    parsing the source is the only way to assert against their constants locally.
    `ast` over the module body rather than a regex over the text, so a table name
    that merely appears in a comment or a nested scope cannot satisfy the lock.
    (The clean fix is to lift these constants into the opl wheel, where both the
    gates and this test could import them; see the report on this change.)"""
    source = (root / "databricks" / "src" / gate_module).read_text(encoding="utf-8")
    for node in ast.parse(source, filename=gate_module).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "QUARANTINE" for t in node.targets
        ):
            value = ast.literal_eval(node.value)
            assert isinstance(value, str), f"{gate_module}: QUARANTINE is not a string"
            return value
    raise AssertionError(f"{gate_module} defines no module-level QUARANTINE")


# The `parameters:` of the fail_on_dq task specifically -- both job YAMLs carry
# several `parameters:` keys, so matching the first one, or any one, would lock the
# wrong wiring. Comment lines are allowed between the two because that is exactly
# where both YAMLs document this pairing.
_FAIL_ON_DQ_PARAMETERS = re.compile(
    r"python_file:\s*\.\./src/fail_on_dq\.py[^\n]*\n(?:[^\n]*#[^\n]*\n)*\s*"
    r"parameters:\s*(\[[^\]]*\])"
)


def _fail_on_dq_parameters(job_yml: str, root: Path = _REPO) -> list[str]:
    text = (root / "databricks" / "resources" / job_yml).read_text(encoding="utf-8")
    found = _FAIL_ON_DQ_PARAMETERS.findall(text)
    assert len(found) == 1, (
        f"{job_yml}: expected exactly 1 fail_on_dq task carrying parameters, "
        f"found {len(found)} -- the wiring lock is not reading what it thinks"
    )
    return ast.literal_eval(found[0])


def _assert_job_points_at_its_own_gate(
    job_yml: str, gate_module: str, root: Path = _REPO
) -> str:
    written = _gate_quarantine(gate_module, root)
    assert _fail_on_dq_parameters(job_yml, root) == [written], (
        f"{job_yml} hands fail_on_dq a different table than {gate_module} writes"
    )
    return written


def test_each_bronze_job_passes_the_quarantine_its_own_gate_writes():
    """Locks the wiring, not just the script: the table each job hands fail_on_dq
    must be the table that job's gate actually writes. Only the YAML half of that
    pair used to be asserted, with the equality against the QUARANTINE constants
    left in a comment -- so the two halves could drift apart silently, which is
    the class of defect that produced the misdirected message in the first place."""
    lookup = _assert_job_points_at_its_own_gate("bronze_job.yml", "dq_gate.py")
    estab = _assert_job_points_at_its_own_gate(
        "bronze_estabelecimentos_job.yml", "dq_gate_batch.py"
    )
    # The entire reason the table is a task parameter: the two jobs differ.
    assert lookup != estab


def test_the_wiring_lock_catches_a_gate_whose_constant_drifts(tmp_path):
    """Proves the lock above can fail. A check that reads two files passes just as
    happily on a typo in its own extraction as on correct wiring, so: copy the
    estab pair, change the gate's constant, and require the assertion to fire."""
    src, resources = tmp_path / "databricks" / "src", tmp_path / "databricks" / "resources"
    src.mkdir(parents=True), resources.mkdir(parents=True)
    (resources / "bronze_estabelecimentos_job.yml").write_text(
        (_REPO / "databricks" / "resources" / "bronze_estabelecimentos_job.yml").read_text(
            encoding="utf-8"
        ),
        encoding="utf-8",
    )
    original = (_REPO / "databricks" / "src" / "dq_gate_batch.py").read_text(encoding="utf-8")
    drifted = original.replace(
        f'QUARANTINE = "{_gate_quarantine("dq_gate_batch.py")}"',
        'QUARANTINE = "bronze_cnpj_estab_quarantine_v2"',
    )
    assert drifted != original, "the mutation did not apply -- this test proves nothing"
    (src / "dq_gate_batch.py").write_text(drifted, encoding="utf-8")

    with pytest.raises(AssertionError, match="different table"):
        _assert_job_points_at_its_own_gate(
            "bronze_estabelecimentos_job.yml", "dq_gate_batch.py", root=tmp_path
        )
