# tests/test_fail_on_dq_task.py
"""Unit tests for the `databricks/src/fail_on_dq.py` job task, whose error
message is its entire deliverable: it is the first instruction a triager gets
(ADR 0006's workflow starts with "a human has read the quarantine"), and the task
is shared by two jobs that quarantine to different tables.

Its parameter is now the TABLE, and the quarantine is derived from the registry.
It used to be the quarantine NAME, which is what let a job YAML point estab
triagers at the lookup quarantine -- a table that holds no Estabelecimentos row.

Loaded by path with the same importlib pattern as `tests/test_extract_cnpj_cli.py`
-- the `databricks/src` scripts are job entry points, not part of the opl wheel.
"""
from __future__ import annotations

import ast
import importlib.util
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from opl.bronze.registry import REGISTRY, table_spec
from opl.config import DEFAULT

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


task = _load("fail_on_dq")


def test_fail_on_dq_names_the_quarantine_of_the_table_it_is_given():
    """The defect this locks down: the estab job's failures pointed operators at
    the lookup quarantine -- a table the deleted `dq_gate.py` OVERWROTE with the
    LOOKUP gate's rejects, so it held nothing about the batch that was blocked."""
    with pytest.raises(RuntimeError) as excinfo:
        task.main(["estabelecimentos"])
    assert "bronze_cnpj_estab_quarantine" in str(excinfo.value)
    assert "bronze_cnpj_lookup_quarantine" not in str(excinfo.value)


def test_fail_on_dq_names_the_lookup_quarantine_for_the_lookup_table():
    with pytest.raises(RuntimeError) as excinfo:
        task.main(["lookup"])
    assert "bronze_cnpj_lookup_quarantine" in str(excinfo.value)


def test_the_lookup_message_is_byte_for_byte_what_f1_2_shipped():
    """The parameter changed from a quarantine name to a table name; the sentence
    a triager reads did not. Deriving the quarantine has to produce the SAME
    message F1.2 shipped, or this refactor moved the deliverable."""
    with pytest.raises(RuntimeError) as excinfo:
        task.main(["lookup"])

    assert str(excinfo.value) == (
        "DQ gate rejected rows - promotion blocked; see the quarantine table "
        "(workspace.default.bronze_cnpj_lookup_quarantine) for reject reasons."
    )


def test_fail_on_dq_still_fails_the_run_when_given_no_table():
    """Pointing nowhere is bad; swallowing a DQ block would be worse."""
    with pytest.raises(RuntimeError, match="no table name"):
        task.main([])


def test_fail_on_dq_with_an_unknown_table_still_fails_the_run():
    """An unknown table must not turn a DQ block into a green run. It reports the
    typo AND fails."""
    with pytest.raises(RuntimeError) as excinfo:
        task.main(["estabelecimento"])
    assert "estabelecimento" in str(excinfo.value)


def test_a_stale_quarantine_name_is_reported_as_unregistered_and_still_fails():
    """The old contract handed in by a not-yet-rewired job YAML.

    `UnknownTable` is a ValueError raised out of `table_spec`, and this task must
    swallow it into the message rather than let it replace the RuntimeError: a
    gate that blocked a batch has to end the run FAILED whatever it was handed.
    The message says the parameter is unregistered, so a triager is told the job
    is misconfigured instead of being sent confidently to a table that does not
    exist under that name."""
    with pytest.raises(RuntimeError) as excinfo:
        task.main(["bronze_cnpj_estab_quarantine"])

    message = str(excinfo.value)
    assert "promotion blocked" in message  # the DQ verdict survives, first
    assert "UNKNOWN" in message and "unregistered table" in message
    assert "estabelecimentos" in message  # table_spec names the valid ones


def _gate_quarantine(table: str) -> str:
    """The table the ONE gate actually writes rejects to, for `table`.

    Executed, not parsed. This used to `ast.parse` a per-table gate's imports,
    because there were two gates and each named its own quarantine constant. There
    is one gate now and it names no table at all, so the honest way to learn where
    it writes is to run its `main` with Spark stubbed and record the write --
    which is also a stronger claim than reading an import ever was."""
    gate = _load("dq_gate_batch")
    written: list[str] = []
    frame = SimpleNamespace()
    frame.write = frame
    frame.format = lambda fmt: frame
    frame.mode = lambda mode: frame
    frame.saveAsTable = written.append
    # The gate asks the batch frame which columns it has, so it can name the rules
    # that will not run against it. Empty is the honest answer for a double that
    # carries no schema, and it makes this test exercise the noisy path -- which is
    # fine here: what is locked below is WHICH TABLE the gate writes to, and a
    # notice on stdout does not move that.
    frame.columns = []
    gate.SparkSession = SimpleNamespace(builder=SimpleNamespace(getOrCreate=lambda: None))
    gate.batch_rows = lambda spark, tbl, batch_id: frame
    gate.evaluate = lambda df, rules=None: df
    gate.tally = lambda evaluated: (0, 0)
    gate.split = lambda df, rules=None: (frame, frame)
    gate.rows_of_batch = lambda spark, tbl, batch_id: 0
    gate._publish = lambda key, value: None

    gate.main([table, "1"])

    assert len(written) == 1, (
        f"the gate wrote {len(written)} table(s) for {table}, expected exactly 1 -- "
        "this lock is reading the wrong write, or none, so it would pass on wiring "
        "it never saw"
    )
    return written[0]


@pytest.mark.parametrize("table", sorted(REGISTRY))
def test_the_quarantine_fail_on_dq_names_is_the_one_that_table_s_gate_writes(table):
    """The pairing the misdirected message broke, now locked per registered table.

    This replaces a lock that compared a job YAML's parameter against a gate's
    imported constant. Both halves of that pair are gone: there is one gate, it
    imports no table name, and the YAML hands over a table rather than a
    quarantine. What has to hold instead is end to end -- the table where the gate
    put this batch's rejects is the table this message sends the triager to."""
    with pytest.raises(RuntimeError) as excinfo:
        task.main([table])

    assert f"({_gate_quarantine(table)})" in str(excinfo.value)


def test_the_two_registered_tables_still_quarantine_apart():
    """The reason the table is a parameter at all: if both jobs quarantined to one
    table the pairing test above would pass on a task that ignored its argument."""
    quarantines = {_gate_quarantine(table) for table in REGISTRY}
    assert len(quarantines) == len(REGISTRY)


# The `parameters:` of the fail_on_dq task specifically -- both job YAMLs carry
# several `parameters:` keys, so matching the first one, or any one, would lock the
# wrong wiring. Comment lines are allowed between the two because that is exactly
# where both YAMLs document this pairing.
_FAIL_ON_DQ_PARAMETERS = re.compile(
    r"python_file:\s*\.\./src/fail_on_dq\.py[^\n]*\n(?:[^\n]*#[^\n]*\n)*\s*"
    r"parameters:\s*(\[[^\]]*\])"
)

# Which job serves which registered table.
_JOB_TABLE = {
    "bronze_job.yml": "lookup",
    "bronze_estabelecimentos_job.yml": "estabelecimentos",
}


def _fail_on_dq_parameters(job_yml: str, root: Path = _REPO) -> list[str]:
    text = (root / "databricks" / "resources" / job_yml).read_text(encoding="utf-8")
    found = _FAIL_ON_DQ_PARAMETERS.findall(text)
    assert len(found) == 1, (
        f"{job_yml}: expected exactly 1 fail_on_dq task carrying parameters, "
        f"found {len(found)} -- the wiring lock is not reading what it thinks"
    )
    return ast.literal_eval(found[0])


def _names_of(table: str) -> set[str]:
    """Every spelling a job YAML may use to mean "this job's own pipeline".

    Three, transitionally. `table` is the contract this task now reads. The two
    quarantine spellings are the OLD contract, which the not-yet-rewired YAMLs
    still carry: rewiring them is Task 10's, and `fail_on_dq` answers them by
    failing the run and saying the parameter is unregistered.

    Accepting the old spellings is not the lock going soft. What this file's
    predecessor caught -- and what actually happened in production -- is a job
    naming the OTHER table's quarantine, and that stays caught in every spelling,
    because these sets are disjoint across tables (asserted below). When Task 10
    lands, drop the two quarantine entries."""
    spec = table_spec(table)
    return {table, spec.quarantine, DEFAULT.table(spec.quarantine)}


def _assert_job_points_at_its_own_pipeline(job_yml: str, root: Path = _REPO) -> None:
    table = _JOB_TABLE[job_yml]
    passed = _fail_on_dq_parameters(job_yml, root)
    assert len(passed) == 1, f"{job_yml} hands fail_on_dq {passed}, expected one name"
    assert passed[0] in _names_of(table), (
        f"{job_yml} hands fail_on_dq {passed[0]!r}, which does not name its own "
        f"table ({table}) -- this is the drift that sent estab triagers to a table "
        "full of unrelated F1.2 lookup rows"
    )


def test_the_accepted_names_of_the_two_tables_do_not_overlap():
    """The lock above is only worth anything if naming the wrong table is
    detectable. Disjoint sets are what make membership in one prove absence from
    the other, across both the old and the new spelling."""
    lookup, estab = _names_of("lookup"), _names_of("estabelecimentos")
    assert not lookup & estab


def test_each_bronze_job_hands_fail_on_dq_its_own_table():
    for job_yml in _JOB_TABLE:
        _assert_job_points_at_its_own_pipeline(job_yml)


def test_the_wiring_lock_catches_a_yaml_that_names_the_other_table(tmp_path):
    """Proves the lock above can fail. A check that reads a file passes just as
    happily on a typo in its own extraction as on correct wiring, so: copy the
    estab job, point its fail_on_dq at the LOOKUP quarantine -- the exact
    production defect -- and require the assertion to fire."""
    resources = tmp_path / "databricks" / "resources"
    resources.mkdir(parents=True)
    job = resources / "bronze_estabelecimentos_job.yml"
    original = (
        _REPO / "databricks" / "resources" / "bronze_estabelecimentos_job.yml"
    ).read_text(encoding="utf-8")
    drifted = re.sub(
        r'(python_file: \.\./src/fail_on_dq\.py(?:.|\n)*?parameters: )\[[^\]]*\]',
        r'\1["bronze_cnpj_lookup_quarantine"]',
        original,
    )
    assert drifted != original, "the mutation did not apply -- this test proves nothing"
    job.write_text(drifted, encoding="utf-8")

    with pytest.raises(AssertionError, match="does not name its own table"):
        _assert_job_points_at_its_own_pipeline(
            "bronze_estabelecimentos_job.yml", root=tmp_path
        )
