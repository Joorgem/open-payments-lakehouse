# src/opl/dataops/telemetry.py
"""One row per TASK-RUN ATTEMPT: what the platform already recorded, joined correctly.

WHAT THIS IS NOT: instrumentation. Every number below is already in `system.lakeflow` and
`system.query` before this file exists. What was missing is a correct join, three
aggregations that are not the obvious ones, and a column that says when a metric is
absent rather than zero. The view writes nothing and costs nothing until it is read; a
job task that stamped timings would cost 15-30 s on jobs whose smallest tasks finish in
16, and would measure the instrument.

FOUR CORRECTIONS, EACH MEASURED AGAINST THE LIVE WORKSPACE ON 2026-08-18, AND EACH ONE
IS A WRONG NUMBER THAT NOTHING WOULD HAVE REPORTED:

1. **The join key is `t.run_id`.** `q.query_source.job_info.job_task_run_id = t.job_run_id`
   returns ZERO rows and no error; `t.task_run_id` does not exist. On `t.run_id` the join
   matches 143 of 273 task runs.

2. **The timeline is sliced on the hour, so `run_id` is not unique in it.** 273 task runs
   occupy 283 rows; one three-period row set belongs to a single 94-minute satellite load.
   Every consumer must group.

3. **`execution_duration_seconds` IS NOT ADDITIVE ACROSS THOSE PERIODS -- it repeats.**
   `sat_empresa_dados` run `99407495289863` carries 0, 5633 and 5633 on its three rows;
   its wall clock is 5,635 s. `SUM` reports 11,266 -- a clean 2x overstatement of the most
   expensive task in this workspace, on the column anyone would reach for first. `MAX` is
   the run's own total. Same for `setup_` and `cleanup_`, which repeat their 1 on every
   period.

4. **The `query.history` side fans out ~5x**, so it is aggregated to task-run grain
   BEFORE the join. Read straight off the join, any per-task metric is multiplied by the
   statement count.

A NULL HERE MEANS "THE PLATFORM RECORDED NO STATEMENT", AND IT MUST NEVER RENDER AS 0.
130 of 273 task runs match nothing, and they are not idle: `assert_deployed_revision`
(41 runs), `check_bad_rows` (29), `fail_on_dq` (22), `unzip` (18) and `smoke` (14) issue
no SQL at all. `fail_on_dq` -- this phase's headline DQ event -- has zero rows in
`system.query.history`. A dashboard showing `read_rows = 0` against it says the gate read
no rows; the truth is that the platform knows nothing about the gate. This repository has
found six guards whose output could not distinguish "passed" from "never ran", and this is
that shape one level out.

AND THE DISTINCTION IS LIVE, NOT HYPOTHETICAL: **15 task runs issued SQL that read
exactly 0 rows** -- all four `ensure_masked_table` runs, three of 24 `dq_gate_batch`, three
of 27 `promote`, two of three `create_views`. `create_views` appears on BOTH sides, so
coalescing NULL to 0 would put two of its runs and one other in the same bucket while they
mean opposite things. `sql_telemetry` names which side a row is on, and `statements` is the
one column that decides: NULL there, and only there, means nothing was attributed.

`no_sql_attributed` IS DELIBERATELY NOT SPELLED `no_sql_issued`, and one measurement is
why. Of three `create_views` task runs -- a task whose entire body is
`CREATE OR REPLACE VIEW` -- one is attributed no statement at all. So an empty right-hand
side is a statement about `system.query.history`, not about the task, and naming it after
the task's behaviour would have been a claim the data does not support.

ONE ROW PER ATTEMPT, NOT PER TASK, AND `attempt` IS HOW A CONSUMER SEES IT. `max_retries:
0` does not prevent a retry: 24 (job run, task key) pairs in this workspace hold two
`run_id`s, `fail_on_dq` alone accounting for ten. A sum grouped by task key therefore
counts a retried task twice, and there is no way to make that not so -- both attempts
really ran. What this view refuses to do is hide it.

WHY THE GROUP BY CARRIES `job_id`, `job_run_id` AND `task_key` RATHER THAN `MAX()`-ING
THEM. They are attributes of a task run and cannot vary within one, so the two spellings
are equivalent today. They differ in how they fail: `MAX()` would silently pick one of two
disagreeing task keys and label the row with it, while grouping emits two rows and breaks
the run_id-is-unique property a reader can check. A loud wrong answer is the one this
project keeps choosing.

WHAT IS DELIBERATELY LEFT OUT. `system.query.history.from_result_cache` is a real column
and it is not here: it is a property of a STATEMENT, and summing it to task-run grain
would need a third state for the NULLs, which is the exact confusion the rest of this file
is about. `.plans/HANDOFF.md` records that this project has already published a cache flag
that was a structural absence printed in the shape of a measurement; a half-honest one
here would be the second.

AND THE NARROW WRITTEN RECORD IS REFUSED. F4's plan proposed a small Delta table for three
things the platform cannot know -- reject counts on an idempotent re-run, and the vault
loaders' `collapsed_duplicates` and `already_present`. Nothing writes it today, and the
four reasons it is not being wired here, in the order that decided it:

  1. **Two of the three are already recoverable without a writer.** Reject counts are in
     the QUARANTINE TABLE, and `dataops_reconciliation` reports them per (table, batch) at
     any later time whether or not the re-run appended -- so "the gate skipped the append
     and the system tables read zero" is answered by a view that already ships. `appended`
     is in Delta history: `hub_empresa` version 2 is a `WRITE` with
     `operationMetrics.numOutputRows = 0`, which is the idempotent second load, and
     `already_present` follows from the row count at that version. Recoverable only for
     `delta.logRetentionDuration`, which is a bound and not an absence.
  2. **The one that is genuinely unrecoverable is produced by five other modules.**
     `collapsed_duplicates` is real and non-zero -- 4,329 of 2026-07's 27,990,592 partner
     link rows -- and it is computed in `opl.vault.{partners,reference,effectivity}` and
     returned by five loader entry points that print it and drop it. Persisting it means
     adding a write side effect to the write path of every loader in the project, from the
     task whose stated remit is that it instruments nothing.
  3. **`max_retries: 0` does not prevent a retry, measured: 24 (job run, task key) pairs
     here ran two attempts.** A telemetry append is exactly the side effect that turns a
     harmless second attempt into duplicate rows. Making it idempotent needs the task run
     id as a key inside each of those five entry points, which is more platform coupling
     than the number is currently worth.
  4. **An empty table would be worse than none.** This repository has found six guards
     whose output could not distinguish "passed" from "never ran"; a telemetry table that
     no task writes is that shape with a schema.

WHAT REVERSES IT: `collapsed_duplicates` becoming load-bearing rather than defensive --
the moment anything BRANCHES on it instead of printing it, its history has to be queryable
and printing it in a run log stops being enough. The write belongs in that change, keyed on
the task run id, in the loaders that produce the number."""
from __future__ import annotations

from dataclasses import dataclass

# The view is prefixed `dataops_` for `opl.dataops.views`' reason: Free Edition ships one
# catalog and one schema, and the three registry collision guards range over registries no
# view can be in, so the prefix plus that module's lock is the whole of the protection.
TASK_TELEMETRY_VIEW = "dataops_task_telemetry"

# The two values of `sql_telemetry`, spelled once because a dashboard filters on them.
MEASURED = "measured"
NO_SQL_ATTRIBUTED = "no_sql_attributed"


@dataclass(frozen=True)
class SystemTables:
    """Where the platform's own tables live. Defaulted; overridden only by tests.

    IT IS NOT A CONFIGURATION KNOB and no task passes it. `system.lakeflow` and
    `system.query` are fixed names in every Unity Catalog metastore, so an override in a
    job would be a coordinate that can only be wrong. It exists because the alternative is
    a view whose SQL can never be executed anywhere but a workspace -- and this project has
    already shipped four guards that were green because nothing could run them. The tests
    point these three at local fixtures and drive the shipped string; `tests/dataops/
    test_telemetry.py::test_the_shipped_view_reads_the_platforms_own_tables` pins that the
    default is what deploys."""

    timeline: str = "system.lakeflow.job_task_run_timeline"
    query_history: str = "system.query.history"
    jobs: str = "system.lakeflow.jobs"


SYSTEM = SystemTables()

# The task-run id, as `query.history` spells it. The whole join is this one path, and it
# is the correction that mattered most: the other two spellings return zero rows or an
# UNRESOLVED_COLUMN, and only one of those two failures is loud.
_TASK_RUN_ID = "query_source.job_info.job_task_run_id"


def _task_runs_sql(system: SystemTables) -> str:
    """The timeline, folded from hour-sliced periods to one row per task-run attempt.

    `MAX_BY(result_state, period_end_time)` rather than `MAX(result_state)`: every period
    but the last carries NULL, and what is wanted is the state AT THE END, not the
    alphabetically largest state the run ever showed. The two agree on every row in this
    workspace and disagree the moment a run reports two terminal states.

    `MAX` on the three durations, never `SUM` -- see the header's correction 3."""
    return (
        "SELECT run_id, job_id, job_run_id, task_key,\n"
        "    MIN(period_start_time) AS started_at,\n"
        "    MAX(period_end_time) AS ended_at,\n"
        "    MAX_BY(result_state, period_end_time) AS result_state,\n"
        "    MAX(setup_duration_seconds) AS setup_seconds,\n"
        "    MAX(execution_duration_seconds) AS execution_seconds,\n"
        "    MAX(cleanup_duration_seconds) AS cleanup_seconds,\n"
        "    COUNT(*) AS timeline_periods\n"
        f"  FROM {system.timeline}\n"
        "  GROUP BY run_id, job_id, job_run_id, task_key"
    )


def _job_names_sql(system: SystemTables) -> str:
    """`job_id` to its current display name, deduped over an SCD table.

    `system.lakeflow.jobs` keeps a row per change, so a rename fans the join out. One job
    in this workspace already has two rows. `MAX_BY(name, change_time)` takes the latest.

    ROWS WITH A `delete_time` ARE KEPT. A deleted job's task runs still happened, and
    filtering them would drop task runs from a telemetry view -- silently, and exactly for
    the jobs nobody is watching any more."""
    return (
        f"SELECT job_id, MAX_BY(name, change_time) AS job_name\n  FROM {system.jobs}\n"
        "  GROUP BY job_id"
    )


def _statements_sql(system: SystemTables) -> str:
    """`query.history` summed to task-run grain, which is what stops the ~5x fan-out.

    `statements` IS THE DISCRIMINATOR, not the metrics. `SUM` skips NULLs, so a task run
    whose statements were all recorded without a `read_rows` would show `read_rows` NULL
    with `statements` set -- a different fact from "no statement was attributed", and the
    only column that separates them is the count."""
    return (
        f"SELECT {_TASK_RUN_ID} AS run_id,\n"
        "    COUNT(*) AS statements,\n"
        "    SUM(read_rows) AS read_rows,\n"
        "    SUM(written_rows) AS written_rows,\n"
        "    SUM(written_bytes) AS written_bytes\n"
        f"  FROM {system.query_history}\n"
        f"  WHERE {_TASK_RUN_ID} IS NOT NULL\n"
        f"  GROUP BY {_TASK_RUN_ID}"
    )


def task_telemetry_sql(system: SystemTables = SYSTEM) -> str:
    """The view body: one row per task-run attempt, with its statements where there are any.

    LEFT JOINs on both sides and for the same reason -- a task run whose job has aged out
    of `system.lakeflow.jobs` (three of them here) and a task run that issued no SQL (130)
    are both facts to report, and an inner join would delete them from the record."""
    return (
        f"WITH task_runs AS (\n  {_task_runs_sql(system)}\n),\n"
        f"job_names AS (\n  {_job_names_sql(system)}\n),\n"
        f"statements AS (\n  {_statements_sql(system)}\n)\n"
        "SELECT t.run_id, t.job_run_id, t.job_id, j.job_name, t.task_key,\n"
        "  ROW_NUMBER() OVER (\n"
        "    PARTITION BY t.job_run_id, t.task_key ORDER BY t.started_at\n"
        "  ) AS attempt,\n"
        "  t.started_at, t.ended_at, t.result_state,\n"
        "  t.setup_seconds, t.execution_seconds, t.cleanup_seconds, t.timeline_periods,\n"
        f"  CASE WHEN s.statements IS NULL THEN '{NO_SQL_ATTRIBUTED}'\n"
        f"    ELSE '{MEASURED}' END AS sql_telemetry,\n"
        "  s.statements, s.read_rows, s.written_rows, s.written_bytes\n"
        "FROM task_runs t\n"
        "  LEFT JOIN job_names j ON j.job_id = t.job_id\n"
        "  LEFT JOIN statements s ON s.run_id = t.run_id"
    )
