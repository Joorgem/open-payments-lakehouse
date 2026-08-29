"""`gate_run_absent`, the batch that ran clean, and the one run against the view F4 deploys.

TWO SHAPES REACH THE FOURTH WORD AND THE MEASURED CORPUS CANNOT CONTAIN EITHER. A workspace
with a `check_bad_rows` run for every job run -- which is what `docs/f6-run-evidence.md` 0.10
measured, 29 over 29 -- never produces this word at all, so every input here is CONSTRUCTED
and says so. `tests/triage_agent/test_history.py` drives the third shape, a batch id in no
row whatsoever, against that corpus.

  1. THE RETENTION SHAPE, WHICH IS THE ONE THE WORD WAS DESIGNED FOR. F4 measured a ~25-day
     floor on the system tables while a quarantine keeps its `_batch_id` for good. A job
     run's condition task is OLDER than its failure task, so a moving floor reaches the
     anchor FIRST: the `fail_on_dq` rows are still in the window, the telemetry can still
     name the job, and the row that would date the batch is gone. The reading says
     `gate_run_absent` with NULL counts -- AND ITS `job_id` IS NULL TOO, which is the cost
     worth asserting rather than discovering, since the surviving rows carry it.
  2. AN EMPTY TELEMETRY VIEW, which arrives here as the same word and is not what that test
     is about. That test is about the COLUMNS.

AND ONE READING HERE IS NOT AN ABSENCE AT ALL: the batch that ran CLEAN. `_CLEAN_RUN` has
been in this fixture from the start as the prior run the retention test's control counts,
and nothing ever asked the module ABOUT it -- so nothing stated what a gate run that found
NOTHING reads as. It is asked here, and the answer is a measurement: `no_prior_execution`
with both counts at 0, which is what separates a zero from the NULLs the two shapes above
get.

WHY THIS IS A SECOND SPARK FILE RATHER THAN MORE OF THE FIRST. The retention shape cannot go
in the measured corpus without moving that corpus's own `fail_on_dq` census -- 22 rows over
11 incidents -- which is one of the numbers 0.10 publishes and which a test there asserts.
A second file costs a second `CREATE DATABASE`; two extra `fail_on_dq` rows inside a
measured census cost the census. `test_history.py` was also within ~30 lines of the file cap
with these in it, which decided nothing on its own and is recorded because it is true.

THE SHIPPED-VIEW RUN, AND EXACTLY WHAT IT BUYS. `history.py` reads four columns --
`job_run_id`, `job_id`, `started_at`, `task_key` -- and until this file ran, the claim that
they exist in the deployed view was an argument: they are a subset of the six T1's feed
reads, so T1's own empty-view test covers them. The argument was sound. It was still an
argument, and this is the run. WHAT IT DOES NOT BUY: the system tables under it are EMPTY,
so it proves the four names RESOLVE against `task_telemetry_sql`'s body and nothing more --
no count is checked by it, `result_state` is deliberately not among the four (defect 3),
and the deployed view over real system tables is T8's workspace run and not this file's.

THE THREE SYSTEM-TABLE SHAPES BELOW ARE A SECOND COPY OF T1's, DELIBERATELY. Importing them
from `test_incidents.py` would make one Spark test file's fixture depend on another's
private names, and a rename there would fail here for a reason a reader could not see. They
are a column list for three platform tables, and if they drift from what those tables really
carry, the shipped view stops building over them -- loudly, in both files.

THE COLOUR IS NAMED AND NEVER THE TOTAL, which this repository has ruled twice.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from opl.dataops.telemetry import SystemTables, task_telemetry_sql
from opl.triage_agent.history import (
    GATE_RUN_ABSENT,
    HISTORY_TASK_KEY,
    INSUFFICIENT_HISTORY,
    NO_PRIOR_EXECUTION,
    history_sql,
    live_gate_spellings,
)
from opl.triage_agent.incidents import DQ_GATE_TASK_KEY

_SCHEMA = "opl_history_absence"

# INVENTED, all three: a job that ran twice with an anchor each time, and a third batch that
# has outlived its own. `_AGED_OUT` carries `fail_on_dq` rows and NO `check_bad_rows` row,
# which is the whole of the retention shape.
_CLEAN_RUN = "111000000000111"
_INCIDENT_RUN = "222000000000222"
_AGED_OUT = "333000000000333"

# INVENTED. The dev prefix is a placeholder and must stay one: the runtime `job_name` carries
# a real operator username CLAUDE.md forbids committing.
_JOB_ID = "900"
_JOB_NAME = "[dev fixture_operator] opl-bronze-cnpj-estabelecimentos"
_BASE = datetime(2026, 7, 24, 6, 0, 0)

_SOURCE_COLUMNS = (
    ("run_id", "STRING"), ("job_run_id", "STRING"), ("job_id", "STRING"),
    ("job_name", "STRING"), ("task_key", "STRING"), ("attempt", "INT"),
    ("started_at", "TIMESTAMP"), ("ended_at", "TIMESTAMP"), ("result_state", "STRING"),
)

# What one reading IS. Named here so the empty-source test can assert the statement
# PROJECTED something, which is what separates "no history" from "no query".
_READING_COLUMNS = (
    "batch_id", "job_id", "gate_started_at", "executions_requested",
    "prior_executions", "prior_incidents", "history",
)

_EMPTY_SYSTEM = SystemTables(
    timeline=f"spark_catalog.{_SCHEMA}.system_timeline",
    query_history=f"spark_catalog.{_SCHEMA}.system_history",
    jobs=f"spark_catalog.{_SCHEMA}.system_jobs",
)

_TIMELINE_COLUMNS = (
    ("run_id", "STRING"), ("job_id", "STRING"), ("job_run_id", "STRING"),
    ("task_key", "STRING"), ("period_start_time", "TIMESTAMP"),
    ("period_end_time", "TIMESTAMP"), ("result_state", "STRING"),
    ("setup_duration_seconds", "BIGINT"), ("execution_duration_seconds", "BIGINT"),
    ("cleanup_duration_seconds", "BIGINT"),
)
_JOBS_COLUMNS = (("job_id", "STRING"), ("name", "STRING"), ("change_time", "TIMESTAMP"))
_QUERY_HISTORY_COLUMNS = (
    ("query_source", "STRUCT<job_info: STRUCT<job_task_run_id: STRING>>"),
    ("end_time", "TIMESTAMP"), ("read_rows", "BIGINT"),
    ("written_rows", "BIGINT"), ("written_bytes", "BIGINT"),
)


def _empty_projection(columns: tuple[tuple[str, str], ...]) -> str:
    """A typed, row-less relation. `WHERE false` rather than an empty VALUES list, which
    Spark cannot type. T1's spelling, for T1's reason."""
    cast = ", ".join(f"CAST(NULL AS {kind}) AS {name}" for name, kind in columns)
    return f"SELECT {cast} WHERE false"


def _retained_rows() -> tuple[tuple, ...]:
    """Two complete job runs and one batch that has lost its anchor.

    The complete runs are the control this file cannot do without: a statement that returns
    NULL for every batch is also what a broken statement returns, so the same statement over
    the same view has to produce a NUMBER for a batch whose anchor is still there."""
    live, = live_gate_spellings()
    rows: list[tuple] = []
    for batch, day, gated in (
        (_CLEAN_RUN, 0, False), (_INCIDENT_RUN, 1, True), (_AGED_OUT, 2, True)
    ):
        at = _BASE + timedelta(days=day)
        if batch != _AGED_OUT:
            rows.append((batch, live, 1, at, "SUCCEEDED"))
            rows.append((batch, HISTORY_TASK_KEY, 1, at + timedelta(minutes=10), "SUCCEEDED"))
        if gated:
            rows.extend(
                (batch, DQ_GATE_TASK_KEY, attempt,
                 at + timedelta(minutes=20 * attempt), "FAILED")
                for attempt in (1, 2)
            )
    return tuple(rows)


def _retained_sql() -> str:
    values = ",\n    ".join(
        f"('{index:05d}', '{batch}', '{_JOB_ID}', '{_JOB_NAME}', '{task}', {attempt}, "
        f"TIMESTAMP'{at:%Y-%m-%d %H:%M:%S}', "
        f"TIMESTAMP'{at + timedelta(minutes=5):%Y-%m-%d %H:%M:%S}', '{state}')"
        for index, (batch, task, attempt, at, state) in enumerate(_retained_rows())
    )
    names = ", ".join(name for name, _ in _SOURCE_COLUMNS)
    return f"SELECT * FROM VALUES\n    {values}\n  AS t({names})"


def _table(name: str) -> str:
    return f"spark_catalog.{_SCHEMA}.{name}"


@pytest.fixture(scope="module")
def absence_probe(spark):
    """Two relations: the SHIPPED view over empty system tables, and the retention shape.

    NOT NAMED `probe`, for the reason `test_history.py`'s fixture records: this package
    already puts two meanings on that name and a third would be a reader's problem."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_SCHEMA}")
    for name, body in (
        ("system_timeline", _empty_projection(_TIMELINE_COLUMNS)),
        ("system_jobs", _empty_projection(_JOBS_COLUMNS)),
        ("system_history", _empty_projection(_QUERY_HISTORY_COLUMNS)),
        ("telemetry_empty", task_telemetry_sql(_EMPTY_SYSTEM)),
        ("telemetry_retained", _retained_sql()),
    ):
        spark.sql(f"CREATE OR REPLACE VIEW {_table(name)} AS {body}")
    yield spark
    spark.sql(f"DROP DATABASE IF EXISTS {_SCHEMA} CASCADE")


def _read(spark, view: str, batch: str):
    return spark.sql(history_sql(view=_table(view)), args={"batch_id": batch})


def test_the_statement_runs_over_the_shipped_telemetry_view_and_projects_its_columns(
    absence_probe,
):
    """THE ONE RUN AGAINST WHAT DEPLOYS, and three things stop it being a test of nothing.

    (1) The relation is built from `task_telemetry_sql` itself rather than by hand, so the
    four columns this module reads are resolved against the deployed view's own body -- an
    unresolved name raises rather than returning nothing. (2) The projected column list is
    asserted, so a statement that answered nothing because it selected nothing fails.
    (3) The same statement over the populated view returns a NUMBER, so what is empty here
    is the DATA.

    T1 closed the identical gap in
    `test_incidents.py::test_an_empty_telemetry_view_yields_no_incidents_and_does_not_raise`
    and this is that test's shape, with one difference that belongs to the module and not to
    the test: this statement is bound on `:batch_id` and answers about a batch a caller
    ASKED for, so an empty view returns ONE row saying so rather than no rows at all."""
    empty = _read(absence_probe, "telemetry_empty", _INCIDENT_RUN)
    populated = _read(absence_probe, "telemetry_retained", _INCIDENT_RUN)

    assert tuple(empty.columns) == _READING_COLUMNS
    assert tuple(populated.columns) == _READING_COLUMNS

    row = empty.collect()
    assert len(row) == 1, "the row exists because a caller asked, not because a gate ran"
    assert row[0]["history"] == GATE_RUN_ABSENT
    assert row[0]["batch_id"] == _INCIDENT_RUN
    assert row[0]["prior_executions"] is None and row[0]["job_id"] is None
    assert populated.collect()[0]["prior_executions"] == 1


def test_a_batch_whose_anchor_aged_out_reads_absent_and_loses_a_job_the_rows_still_name(
    absence_probe,
):
    """THE RETENTION SHAPE, and the reading pays for it in a column nobody would look at.

    The `fail_on_dq` rows survive, `check_bad_rows` is gone, and the counts are NULL rather
    than 0 -- 0 is what a measurement says and there is nothing here to measure. That much
    is the design. What is worth asserting beside it is that `job_id` goes NULL too: it
    comes off `own_gate`, which is the CTE that is empty, so this row cannot name a job that
    surviving rows in the same view still name. A consumer that wants it takes it from T1's
    record, which reads the same telemetry without needing an anchor.

    THE CONTROL IS THE SAME STATEMENT OVER THE SAME VIEW: the incident run, whose anchor is
    still there, reports one prior execution and names its job."""
    aged_out = _read(absence_probe, "telemetry_retained", _AGED_OUT).collect()[0]
    control = _read(absence_probe, "telemetry_retained", _INCIDENT_RUN).collect()[0]
    surviving = absence_probe.sql(
        f"SELECT task_key, job_id, job_name FROM {_table('telemetry_retained')} "
        f"WHERE job_run_id = '{_AGED_OUT}'"
    ).collect()

    assert aged_out["history"] == GATE_RUN_ABSENT
    assert aged_out["prior_executions"] is None and aged_out["prior_incidents"] is None
    assert aged_out["gate_started_at"] is None
    assert aged_out["job_id"] is None, "the anchor is the only place the job id comes from"

    assert {row["task_key"] for row in surviving} == {DQ_GATE_TASK_KEY}
    assert {row["job_id"] for row in surviving} == {_JOB_ID}
    assert {row["job_name"] for row in surviving} == {_JOB_NAME}

    assert control["history"] == INSUFFICIENT_HISTORY
    assert control["prior_executions"] == 1 and control["job_id"] == _JOB_ID


def test_a_batch_whose_gate_found_nothing_is_measured_at_zero_and_counted_as_a_prior_run(
    absence_probe,
):
    """THE BATCH THAT RAN CLEAN, asked about rather than only counted.

    Every other batch either Spark file asks about carries a `fail_on_dq` row or has no rows
    at all, so until this test nothing stated the module's answer for a gate run that found
    NOTHING. `_CLEAN_RUN` is its job's first gate run, so the answer is `no_prior_execution`
    -- a measurement of zero, with the anchor and the `job_id` both there, where `_AGED_OUT`
    carries NULL in all three.

    THE CLEAN READING IS ALSO WHAT PINS THE ANCHOR TO `check_bad_rows` here: anchored on
    `fail_on_dq` instead, the statement finds no row for a batch that ran clean and reports
    the FOURTH word over a run that plainly happened -- the mutation `test_history.py`'s tie
    test names, measured red on the first assertion below. The run after it has `fail_on_dq`
    rows of its own, so an anchor taken from that task still finds one there -- which is why
    the CLEAN batch is the reading that can tell the two anchors apart.

    AND THAT RUN COUNTS THIS ONE AS HISTORY WITHOUT COUNTING IT AS A FINDING: one prior
    execution, zero prior incidents."""
    clean = _read(absence_probe, "telemetry_retained", _CLEAN_RUN).collect()[0]
    after = _read(absence_probe, "telemetry_retained", _INCIDENT_RUN).collect()[0]

    assert clean["history"] == NO_PRIOR_EXECUTION
    assert clean["prior_executions"] == 0 and clean["prior_incidents"] == 0
    assert clean["job_id"] == _JOB_ID and clean["gate_started_at"] is not None

    assert after["history"] == INSUFFICIENT_HISTORY
    assert after["prior_executions"] == 1 and after["prior_incidents"] == 0
