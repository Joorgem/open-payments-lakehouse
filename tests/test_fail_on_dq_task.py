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

from opl.bronze import autoloader as _tables
from opl.bronze.autoloader import BRONZE_ESTAB_QUARANTINE

_TABLES_MODULE = _tables.__name__

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
    """The table a gate task writes rejects to — now a real reference, not a parse.

    This used to `ast.parse` the gate's source, because those modules cannot be
    imported outside a workspace (`from databricks.sdk.runtime import dbutils`
    raises at import) and the constants lived only inside them. The clean fix it
    pointed at has since been made: both quarantine names live in
    `opl.bronze.autoloader`, the gates import them, and so does this test — so the
    lock compares symbols instead of scraping text."""
    source = (root / "databricks" / "src" / gate_module).read_text(encoding="utf-8")
    imported = [
        alias.name
        for node in ast.parse(source, filename=gate_module).body
        if isinstance(node, ast.ImportFrom) and node.module == _TABLES_MODULE
        for alias in node.names
        if alias.asname == "QUARANTINE"
    ]
    assert len(imported) == 1, (
        f"{gate_module} does not import exactly one {_TABLES_MODULE} name as "
        f"QUARANTINE (found {imported}) -- either it went back to a local literal "
        "or it renamed something, and this lock would stop seeing what it writes"
    )
    # Resolve the imported NAME against the wheel, so the lock reads the gate's own
    # choice and then the real value. A hardcoded module→constant map would pass
    # even if a gate imported the other table's constant, which a probe confirmed.
    return getattr(_tables, imported[0])


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


def test_the_wiring_lock_catches_a_yaml_that_drifts_from_its_gate(tmp_path):
    """Proves the lock above can fail. A check that reads two files passes just as
    happily on a typo in its own extraction as on correct wiring, so: copy the
    estab pair, drift the YAML off the gate, and require the assertion to fire.

    Two other drift shapes are covered by `_gate_quarantine`'s own assertion rather
    than here — a gate importing the OTHER table's constant, and a gate going back
    to a local literal. Both were confirmed to fail the suite by mutation probe."""
    src, resources = tmp_path / "databricks" / "src", tmp_path / "databricks" / "resources"
    src.mkdir(parents=True), resources.mkdir(parents=True)
    (resources / "bronze_estabelecimentos_job.yml").write_text(
        (_REPO / "databricks" / "resources" / "bronze_estabelecimentos_job.yml").read_text(
            encoding="utf-8"
        ),
        encoding="utf-8",
    )
    # The real gate, unmutated: `_gate_quarantine` parses its import to learn which
    # table it writes, so a stub would trip that check instead of the one under test.
    (src / "dq_gate_batch.py").write_text(
        (_REPO / "databricks" / "src" / "dq_gate_batch.py").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    # Mutate the half that can still drift. The gate's table name is no longer a
    # literal in its source (both gates import it from the wheel), so the drift a
    # reader should fear now lives in the YAML: a job handing fail_on_dq a table
    # its own gate does not write.
    job = resources / "bronze_estabelecimentos_job.yml"
    original = job.read_text(encoding="utf-8")
    drifted = original.replace(
        f'parameters: ["{BRONZE_ESTAB_QUARANTINE}"]',
        'parameters: ["bronze_cnpj_estab_quarantine_v2"]',
    )
    assert drifted != original, "the mutation did not apply -- this test proves nothing"
    job.write_text(drifted, encoding="utf-8")

    with pytest.raises(AssertionError, match="different table"):
        _assert_job_points_at_its_own_gate(
            "bronze_estabelecimentos_job.yml", "dq_gate_batch.py", root=tmp_path
        )
