"""The telemetry view, driven out of tables shaped like the platform's own.

WHY THE SHIPPED STRING IS EXECUTED AND NOT MATCHED. Every property this view has is a
property of an AGGREGATION -- `MAX` where `SUM` would double, a LEFT JOIN whose NULL means
something, a `GROUP BY` that must not fan out. A test that asserted the SQL text would pass
for a query that computes the opposite, and this repository has now shipped six guards
whose output could not distinguish "passed" from "never ran".

WHY `SystemTables` IS OVERRIDDEN HERE AND NOWHERE ELSE. `system.lakeflow` and
`system.query` exist only in a Unity Catalog metastore, so the alternative to pointing the
shipped SQL at local fixtures is a view whose behaviour is asserted by nobody until a
workspace run -- after a deploy, after a commit. The default is pinned by
`tests/dataops/test_views.py`, so the seam cannot leak into what deploys.

THE FIXTURE ROWS ARE COPIED FROM THE LIVE WORKSPACE, not invented. The three hour-sliced
periods carrying 0, 5633, 5633 are `sat_empresa_dados` run `99407495289863`; the task that
issues SQL reading exactly zero rows is `ensure_masked_table`, all four of whose runs do;
the task that issues none at all is `fail_on_dq`, all 22 of whose runs do; and the retry
pair is one of the 24 (job run, task key) pairs holding two `run_id`s despite
`max_retries: 0`.
"""
from __future__ import annotations

import pytest

from opl.dataops.telemetry import (
    MEASURED,
    NO_SQL_ATTRIBUTED,
    SystemTables,
    task_telemetry_sql,
)

_SCHEMA = "spark_catalog.opl_telemetry_probe"
_SYSTEM = SystemTables(
    timeline=f"{_SCHEMA}.job_task_run_timeline",
    query_history=f"{_SCHEMA}.history",
    jobs=f"{_SCHEMA}.jobs",
)

# (run_id, job_id, job_run_id, task_key, start, end, result_state, setup, exec, cleanup)
_TIMELINE = (
    # One 94-minute satellite load, sliced on the hour. `execution_duration_seconds`
    # REPEATS the run's own total on the later periods rather than slicing it, so the sum
    # of these three is 11266 for a task that ran 5633 seconds.
    ("r_sliced", "j1", "jr1", "sat_empresa_dados", "14:28:18", "15:00:00", None, 1, 0, 0),
    ("r_sliced", "j1", "jr1", "sat_empresa_dados", "15:00:00", "16:00:00", None, 1, 5633, 1),
    ("r_sliced", "j1", "jr1", "sat_empresa_dados", "16:00:00", "16:02:13", "SUCCEEDED", 1, 5633, 1),
    # Issued SQL and read nothing.
    ("r_zero", "j1", "jr2", "ensure_masked_table", "17:00:00", "17:00:30", "SUCCEEDED", 1, 29, 0),
    # Issued no SQL at all. Same job run as the one above, so the only thing separating
    # them in any report is what this view says about them.
    ("r_nosql", "j1", "jr2", "fail_on_dq", "17:00:30", "17:00:46", "SUCCEEDED", 1, 16, 0),
    # `max_retries: 0` and two attempts anyway.
    ("r_try1", "j1", "jr3", "fail_on_dq", "18:00:00", "18:00:16", "FAILED", 1, 16, 0),
    ("r_try2", "j1", "jr3", "fail_on_dq", "18:01:00", "18:01:18", "SUCCEEDED", 1, 18, 0),
    # A job that is no longer in `system.lakeflow.jobs`.
    ("r_orphan", "j_gone", "jr4", "smoke", "19:00:00", "19:00:20", "SUCCEEDED", 1, 20, 0),
)

# (job_id, name, change_time) -- `j1` renamed, which is what fans an SCD join out.
_JOBS = (
    ("j1", "opl-vault-empresa-OLD", "2026-08-01"),
    ("j1", "opl-vault-empresa", "2026-08-09"),
)

# (job_task_run_id or None, read_rows, written_rows, written_bytes)
_HISTORY = (
    ("r_sliced", 137691996, 69062849, 4194304),
    ("r_sliced", 0, 0, 0),
    ("r_zero", 0, 0, 0),
    ("r_zero", 0, 0, 0),
    ("r_try1", 12, 0, 0),
    # An interactive statement, attributed to no task run. It must not become a group.
    (None, 999999, 0, 0),
)


def _timeline_sql() -> str:
    rows = ",\n    ".join(
        f"('{run}', '{job}', '{job_run}', '{task}', "
        f"TIMESTAMP'2026-08-10 {start}', TIMESTAMP'2026-08-10 {end}', "
        f"{'NULL' if state is None else repr(state)}, {setup}L, {execution}L, {cleanup}L)"
        for run, job, job_run, task, start, end, state, setup, execution, cleanup in _TIMELINE
    )
    return (
        f"SELECT * FROM VALUES\n    {rows}\n  AS t(run_id, job_id, job_run_id, task_key, "
        "period_start_time, period_end_time, result_state, setup_duration_seconds, "
        "execution_duration_seconds, cleanup_duration_seconds)"
    )


def _jobs_sql() -> str:
    rows = ",\n    ".join(
        f"('{job}', '{name}', TIMESTAMP'{change} 00:00:00')" for job, name, change in _JOBS
    )
    return f"SELECT * FROM VALUES\n    {rows}\n  AS t(job_id, name, change_time)"


def _history_sql() -> str:
    rows = "\n  UNION ALL ".join(
        f"SELECT named_struct('job_info', named_struct('job_task_run_id', "
        f"{'CAST(NULL AS STRING)' if run is None else repr(run)})) AS query_source, "
        f"{read}L AS read_rows, {written}L AS written_rows, {written_bytes}L AS written_bytes"
        for run, read, written, written_bytes in _HISTORY
    )
    return rows


@pytest.fixture(scope="module")
def probe(spark):
    """The three platform tables, as views, in a schema this module owns and drops."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_SCHEMA.split('.')[-1]}")
    for name, body in (
        (_SYSTEM.timeline, _timeline_sql()),
        (_SYSTEM.jobs, _jobs_sql()),
        (_SYSTEM.query_history, _history_sql()),
    ):
        spark.sql(f"CREATE OR REPLACE VIEW {name} AS {body}")
    yield spark
    spark.sql(f"DROP DATABASE IF EXISTS {_SCHEMA.split('.')[-1]} CASCADE")


def _by_run(spark) -> dict:
    return {row["run_id"]: row for row in spark.sql(task_telemetry_sql(_SYSTEM)).collect()}


def test_the_grain_is_one_row_per_task_run_attempt(probe):
    """Eight timeline rows, six task runs, six rows out.

    The `GROUP BY` carries `job_id`, `job_run_id` and `task_key` beside `run_id`, so a
    platform that ever disagreed with itself about a task run's own attributes would show
    two rows here rather than one row silently labelled with the larger of two task keys."""
    rows = probe.sql(task_telemetry_sql(_SYSTEM)).collect()
    assert len(rows) == 6
    assert len({row["run_id"] for row in rows}) == 6


def test_a_task_that_issued_no_sql_is_not_the_same_row_as_one_that_read_no_rows(probe):
    """THE DISTINCTION THIS VIEW EXISTS FOR, and both halves are live in the workspace.

    `r_zero` issued two statements that read zero rows; `r_nosql` issued none and the
    platform knows nothing about it. Rendered as `read_rows = 0` they are the same claim --
    "this task read no rows" -- and only one of them is true. 130 of 273 task runs in this
    workspace are on the second side, including every run of `fail_on_dq`, this phase's
    headline DQ event; 15 are on the first."""
    rows = _by_run(probe)
    assert rows["r_zero"]["sql_telemetry"] == MEASURED
    assert rows["r_zero"]["statements"] == 2
    assert rows["r_zero"]["read_rows"] == 0

    assert rows["r_nosql"]["sql_telemetry"] == NO_SQL_ATTRIBUTED
    assert rows["r_nosql"]["statements"] is None
    assert rows["r_nosql"]["read_rows"] is None


def test_coalescing_the_statement_count_to_zero_erases_that_distinction(probe):
    """The mutation this view refuses, executed, so the column above is shown to do work.

    A dashboard that fills the NULLs -- which is the default behaviour of most tile
    renderers -- makes the two rows above identical in every metric column. This is that
    query, and the assertion is that it CANNOT tell them apart."""
    naive = task_telemetry_sql(_SYSTEM).replace(
        "s.statements, s.read_rows", "COALESCE(s.statements, 0) AS statements, "
        "COALESCE(s.read_rows, 0) AS read_rows"
    )
    rows = {row["run_id"]: row for row in probe.sql(naive).collect()}
    assert rows["r_nosql"]["read_rows"] == rows["r_zero"]["read_rows"] == 0


def test_the_duration_of_an_hour_sliced_run_is_its_own_total_and_not_the_sum(probe):
    """`MAX`, never `SUM`, and the gap is the whole of the most expensive task here.

    The platform repeats a task run's execution seconds on each period rather than slicing
    them, so summing the three fixture rows gives 11,266 for a task that ran 5,633. Wrong
    by exactly 2x, on the column anyone reaches for first, with nothing failing."""
    row = _by_run(probe)["r_sliced"]
    assert row["timeline_periods"] == 3
    assert row["execution_seconds"] == 5633
    assert sum(entry[8] for entry in _TIMELINE if entry[0] == "r_sliced") == 11266
    assert row["setup_seconds"] == 1
    assert row["result_state"] == "SUCCEEDED"


def test_the_state_is_the_one_the_run_ended_on_and_not_the_largest_it_ever_showed(probe):
    """`MAX_BY(result_state, period_end_time)`: every period but the last carries NULL."""
    assert _by_run(probe)["r_sliced"]["result_state"] == "SUCCEEDED"
    assert _by_run(probe)["r_try1"]["result_state"] == "FAILED"


def test_a_retry_is_a_second_attempt_and_says_so(probe):
    """`max_retries: 0` does not prevent a retry -- 24 measured pairs in this workspace.

    Both attempts really ran, so neither is dropped; what the view refuses is to let a sum
    grouped by task key double-count without anything saying it did."""
    rows = _by_run(probe)
    assert (rows["r_try1"]["attempt"], rows["r_try2"]["attempt"]) == (1, 2)
    assert rows["r_try1"]["job_run_id"] == rows["r_try2"]["job_run_id"]
    assert rows["r_zero"]["attempt"] == 1


def test_a_task_run_whose_job_has_aged_out_is_still_in_the_view(probe):
    """Three live task runs have no row in `system.lakeflow.jobs`.

    An inner join would delete them from the telemetry record -- silently, and exactly for
    the jobs nobody is watching any more."""
    row = _by_run(probe)["r_orphan"]
    assert row["job_name"] is None
    assert row["task_key"] == "smoke"


def test_a_renamed_job_contributes_one_row_carrying_its_current_name(probe):
    """`system.lakeflow.jobs` is SCD: a rename is a second row and would fan the join out."""
    assert _by_run(probe)["r_sliced"]["job_name"] == "opl-vault-empresa"


def test_a_statement_attributed_to_no_task_run_creates_no_row(probe):
    """The interactive query in the fixture reads 999,999 rows and belongs to no task.

    Without the `IS NOT NULL` filter it groups under a NULL key, which joins to nothing --
    harmless here and not harmless the day someone reads the aggregate directly."""
    rows = _by_run(probe)
    assert None not in rows
    assert all(row["read_rows"] != 999999 for row in rows.values())
