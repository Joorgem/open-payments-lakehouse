# src/opl/dataops/telemetry.py
"""One row per TASK-RUN ATTEMPT: what the platform already recorded, joined correctly.

WHAT THIS IS NOT: instrumentation. Every number below is already in `system.lakeflow` and
`system.query` before this file exists. What was missing is a correct join, three
aggregations that are not the obvious ones, and a column that says when a metric is
absent rather than zero. The view writes nothing and costs nothing until it is read; a
job task that stamped timings would cost 15-30 s on jobs whose smallest tasks finish in
16, and would measure the instrument.

EVERY SYSTEM-TABLE COUNT BELOW CARRIES A READING TIME, and that is not decoration. These
tables grow with every run, so a bare count in a docstring is false by the next job and
unfalsifiable afterwards -- which is how the first version of this header came to argue
its own naming from an instance that had evaporated by the time a reviewer looked (see
`no_sql_attributed`, below). Unless stated otherwise, every figure here was read at
**2026-08-18 21:53-21:58 UTC** against the `opl-free` workspace.

FOUR CORRECTIONS, EACH MEASURED AGAINST THE LIVE WORKSPACE, AND EACH ONE IS A WRONG
NUMBER THAT NOTHING WOULD HAVE REPORTED:

1. **The join key is `t.run_id`.** `q.query_source.job_info.job_task_run_id = t.job_run_id`
   returns ZERO rows and no error; `t.task_run_id` does not exist. On `t.run_id` the join
   matches 145 of 274 task runs.

2. **The timeline is sliced on the hour, so `run_id` is not unique in it.** 274 task runs
   occupy 284 rows; one three-period row set belongs to a single 94-minute satellite load.
   Every consumer must group.

3. **`execution_duration_seconds` IS NOT ADDITIVE ACROSS THOSE PERIODS -- it repeats.**
   `sat_empresa_dados` run `99407495289863` carries 0, 5633 and 5633 on its three rows;
   its wall clock is 5,635 s. `SUM` reports 11,266 -- a clean 2x overstatement of the most
   expensive task in this workspace, on the column anyone would reach for first. `MAX` is
   the run's own total. Same for `setup_` and `cleanup_`, which repeat their 1 on every
   period.

4. **The `query.history` side fans out 4.5x** -- 649 attributed statements over 145 task
   runs -- so it is aggregated to task-run grain BEFORE the join. Read straight off the
   join, any per-task metric is multiplied by the statement count.

A NULL HERE MEANS "THE PLATFORM RECORDED NO STATEMENT", AND IT MUST NEVER RENDER AS 0.
129 of 274 task runs match nothing, and they are not idle: `assert_deployed_revision`
(42 runs), `check_bad_rows` (29), `fail_on_dq` (22), `unzip` (18) and `smoke` (14) issue
no SQL at all. `fail_on_dq` -- this phase's headline DQ event -- has zero rows in
`system.query.history`. A dashboard showing `read_rows = 0` against it says the gate read
no rows; the truth is that the platform knows nothing about the gate. This repository has
found six guards whose output could not distinguish "passed" from "never ran", and this is
that shape one level out.

AND THE DISTINCTION IS LIVE, NOT HYPOTHETICAL: **16 task runs issued SQL that read
exactly 0 rows** -- all four `ensure_masked_table` runs, all three `create_views`, three of
27 `promote`, three of 24 `dq_gate_batch`, two of four `reclaim_landing` and the one
`backfill_quarantine`. Coalescing NULL to 0 would put those 16 in the same bucket as the
129 above while they mean opposite things. `sql_telemetry` names which side a row is on,
and `statements` is the one column that decides: NULL there, and only there, means nothing
was attributed.

`no_sql_attributed` IS DELIBERATELY NOT SPELLED `no_sql_issued`, AND THE REASON IS NOT THE
ONE THIS HEADER FIRST GAVE. It argued from an instance -- "of three `create_views` task
runs, one is attributed no statement at all" -- and that instance is gone: all three now
show `statements = 2, read_rows = 0`, with no fourth run in between. The vanished instance
IS the mechanism, and it is `system.query.history`'s INGESTION LAG:

  * Measured 2026-08-18 21:56:25Z: a statement of this session's own, completed 21:53:13Z,
    was ABSENT from `system.query.history` three minutes later, and the table's newest row
    sat 729 s behind `current_timestamp()`. The same lag read 599 s two minutes earlier and
    1,349 s in an independent reading the same evening. It is minutes to tens of minutes
    and it is not fixed.
  * So a task run that finished inside that window is attributed nothing YET, and the
    attribution appears later with nothing having run. A view that called that
    `no_sql_issued` would be publishing a claim about the task from a fact about the table.

WHICH IS WHY THE LABEL IS NOT BINARY. `sql_telemetry` has four values, and the two new ones
exist so that "the record does not reach this run" cannot be read as "this run issued no
SQL" -- the failure the whole column was added to prevent, one level further out:

  * `measured`             -- statements were attributed. The metrics mean what they say.
  * `not_yet_attributed`   -- the run ended AFTER the newest statement `query.history`
                              holds, so the record has not caught up to it. The label is
                              conservative in the right direction: `ended_at > watermark`
                              proves the record cannot yet cover the run, while
                              `ended_at <= watermark` only makes coverage likely, since
                              ingestion is not strictly ordered. IT IS LIVE AND IT IS
                              TRANSIENT, and a ZERO in it is not evidence -- see "A ZERO
                              IN `not_yet_attributed`", below.
  * `older_than_history`   -- the run ended BEFORE the oldest statement `query.history`
                              holds, so whatever it issued is outside the window the table
                              retains. The retention figures for the two system tables are
                              deliberately NOT asserted here as documented numbers: the
                              view measures the window it actually has (`MIN(end_time)`)
                              instead, which cannot go stale against a docs page. That this
                              arm is live rather than theoretical is measured -- 10 task
                              runs already sit before `query.history`'s oldest row, all of
                              them `smoke`, which issues no SQL anyway. The day the two
                              retentions diverge further, runs that DID issue SQL cross the
                              same line, and this is the label they get.
  * `no_sql_attributed`    -- the run sits INSIDE the window on both sides and the platform
                              still holds nothing for it. THIS IS A FACT ABOUT THE RECORD
                              AND NOT ABOUT THE TASK, which is not what this header used to
                              say -- see "WHAT `no_sql_attributed` DOES NOT MEAN", below.

WHAT `no_sql_attributed` DOES NOT MEAN, AND THIS FILE USED TO CLAIM IT DID. It called this
arm "the only one of the four that is evidence about the task". That is wrong, and the
refutation was already sitting two paragraphs above it: ingestion into
`system.query.history` IS NOT STRICTLY ORDERED. `w.newest_statement` proves the table holds
a statement NEWER than the run; it does not prove the table holds every OLDER one. A run
whose own statements are still in flight, passed by some unrelated statement that ingested
ahead of them, therefore falls out of `not_yet_attributed` and lands here.

AND THE READER IS OFTEN THE ONE WHO ADVANCES THE WATERMARK. The statements moving
`MAX(end_time)` are frequently an operator's own interactive queries, issued while nothing
else is running -- `_history_window_sql` takes the window over ALL statements deliberately,
and this is the cost of that choice. So READING THIS VIEW can promote a run out of
`not_yet_attributed` and into `no_sql_attributed` while the run's statements are still on
their way. A watermark is a LOWER BOUND ON INGESTION and never a completeness guarantee --
the same error the previous correction to this file found one level out, where
`now - MAX(watermark)` turned out not to be a lag.

A COMPLETENESS GUARANTEE WAS LOOKED FOR AND THERE IS NONE. Measured 2026-08-18:
`DESCRIBE TABLE system.query.history` returns 46 columns and not one of them bounds
ingestion. `update_time` is the closest candidate and it is not one -- it is the STATEMENT's
last progress update, a property of the query rather than of when its row landed. There is
no commit timestamp and no ingestion column. Neither is there a partition boundary to read:
`DESCRIBE DETAIL` fails outright with `[DELTA_UNSUPPORTED_FILE_SYSTEM] ...
uc-deltasharing://system.query.history/_delta_log`, so the file-level metadata such a bound
would live in is not reachable from this workspace at all. Until the platform publishes
one, an unmatched row is an observation about the ATTRIBUTION RECORD and nothing more.

SO WHAT THE ARM IS FOR, GIVEN IT IS NOT EVIDENCE. Its motivating case is real and unchanged:
`fail_on_dq` issues NO SQL, on all 22 of its runs, and a dashboard rendering `read_rows = 0`
against this project's headline DQ event asserts something the platform never said. This
arm is what stops that. What it cannot do is the converse -- it cannot separate "the record
attributes no statement to this run" from "this task issued none", and NOTHING IN THIS VIEW
CAN. A consumer needing the second reads the task's code, not this column.

THE SAME NON-ORDERING MAKES `measured` PROVISIONAL, which is one defect and not two.
`statements`, `read_rows`, `written_rows` and `written_bytes` sum whatever had been ingested
AT THE MOMENT THE VIEW WAS READ, so a statement landing later raises them with nothing
having run. Measured on this view's own rows: `create_views` carried `statements` NULL at
22:31:14Z and 4 once the record caught up. Every metric here is a FLOOR, not a total.

A ZERO IN `not_yet_attributed` IS NOT EVIDENCE THAT NO RUN WAS TOO RECENT. The arm's
reachability is a RACE BETWEEN TWO WATERMARKS, not a property of this workspace: it can
fire only while the timeline's `MAX(period_end_time)` is AHEAD of `query.history`'s
`MAX(end_time)`. No row here ends later than the first, so once the second overtakes it,
nothing can satisfy `ended_at > newest_statement` and the arm reads zero for a structural
reason rather than an observed one. BOTH ORDERINGS WERE MEASURED ON 2026-08-18, four
minutes apart, against the deployed view:

  * 22:31:14Z -- the arm HELD TWO ROWS: `create_views` and `measure_rule_overlap` of an
    `opl-dataops-views` run, ended 22:24:39Z and 22:25:22Z, `statements` NULL. The two
    watermarks, read 16 s later at 22:31:30Z: timeline 22:25:22Z, statement 21:57:57Z --
    timeline ahead by 1,645 s.
  * 22:34:47Z -- statement watermark 22:26:15Z, timeline watermark still 22:25:22Z. The
    order had REVERSED, and at 22:35:12Z the arm held ZERO: those same two runs now read
    `measured`, having acquired their statements with nothing having run. That transition
    is exactly what the label exists to describe, and it took under four minutes.

SO `now - MAX(watermark)` IS NOT A LAG, AND IT IS WHAT MADE THIS ARM LOOK DEAD. The figure
that suggested so -- a timeline "lagging" 7,455 s against `query.history`'s 1,780 s, four
times slower, therefore an arm that can never fire -- reproduces here (7,550 s at 22:29:13Z
and 1,883 s at 22:29:20Z, the same two rows simply aged) and is not measuring ingestion at
all. `now - MAX(period_end_time)` is time since
the newest EVENT the table holds: ingestion delay PLUS however long the source has been
idle. The timeline's source is idle for hours -- no job had run since 20:23:23Z, so almost
all of those 7,550 s were an empty workspace. `query.history`'s source is never idle here,
since every statement of every operator session feeds it including the ones taking these
measurements, so for that table alone the subtraction approximates a lag. Measured the
other way, on one event: a 22:25:22Z timeline row was ABSENT at 22:29:13Z and PRESENT at
22:31:14Z, so the timeline carried it within 231-352 s while `query.history` was still
2,013 s behind at 22:31:30Z. On that evidence the timeline is the FASTER table. Neither
reading is a bound; both tables ingest in bursts, and one sample of either is one sample.

WHAT NO ARM CAN SAY: THE RUN THIS VIEW CANNOT SEE AT ALL. Every label above is a property
of a ROW, and a task run the timeline has not ingested has no row to carry one. Measured,
not hypothetical: the `opl-dataops-views` run above started 22:23:50Z, and at 22:29:13Z the
timeline's newest event was still 20:23:23Z -- so this view could not have held those runs
then. It stood at the 274 rows this header's 21:53-21:58Z figures carry, and read 276 at
22:31:14Z and 277 at 22:34:47Z, with nothing new having run in between. A consumer reading
at 22:29 saw a complete-looking record missing its three most recent task runs, and no
value of `sql_telemetry` says so.

THE VIEW ALREADY CARRIES THAT BOUND, WHICH IS WHY NO COLUMN IS ADDED FOR IT. The edge is
the timeline's own watermark, and `MAX(ended_at)` OVER THIS VIEW IS THAT WATERMARK: the
view folds the timeline with `MAX(period_end_time)`, so the largest `ended_at` it can
report is the largest the table holds. Measured 22:34:47Z, the two read the same
2026-08-18T22:25:22.514Z. The rule is therefore evaluable from the output alone -- THIS
VIEW KNOWS NOTHING ABOUT A TASK RUN THAT ENDED AFTER ITS OWN `MAX(ended_at)` -- and
`ds_task_runs` orders by `started_at DESC`, so that value sits at the top of the page.

WHAT IS REFUSED HERE, AND WHY, SINCE ALL THREE ARE OBVIOUS ADDITIONS:

  1. A per-row `timeline_watermark`: 277 copies of a number recoverable from the rows
     beside it.
  2. A per-row pair of `query.history` watermarks. This one nearly went the other way,
     because a Unity Catalog view runs with its OWNER's privileges -- a principal holding
     `SELECT` on this view cannot generally read `system.query.history` itself, so those
     two timestamps are NOT recoverable outside the view the way `MAX(ended_at)` is. They
     are still not published: the CASE already spends both of them on every row, so a
     reader gains no branch it did not have.
  3. A `staleness_seconds` or `lag_seconds` on either table. REFUSED HARDEST, for the
     paragraph above: that subtraction cannot separate a slow ingest from a quiet
     workspace, and printed as a number it reads as the first. It is the number that
     produced a two-hour lag out of two hours of nobody running a job.

WHY THE WINDOW AND NOT A COLUMN. The two bounds are constant across every row, so
publishing them per row is a copy of the same two timestamps on every one; the label is the
part a consumer branches on. `ds_task_runs` is `ORDER BY started_at DESC LIMIT 200`, so the
rows a reader looks at FIRST are exactly the ones most exposed to the lag -- which is the
whole reason this cannot stay a footnote in a docstring.

ONE ROW PER ATTEMPT, NOT PER TASK, AND `attempt` IS HOW A CONSUMER SEES IT. `max_retries:
0` does not prevent a retry: 24 (job run, task key) pairs in this workspace hold two
`run_id`s, across 7 task keys, `fail_on_dq` alone accounting for 11. A sum grouped by task
key therefore counts a retried task twice, and there is no way to make that not so -- both
attempts really ran. What this view refuses to do is hide it.

WHY THE GROUP BY CARRIES `job_id`, `job_run_id` AND `task_key` RATHER THAN `MAX()`-ING
THEM. They are attributes of a task run and cannot vary within one, so the two spellings
are equivalent today. They differ in how they fail: `MAX()` would silently pick one of two
disagreeing task keys and label the row with it, while grouping emits two rows and breaks
the run_id-is-unique property a reader can check. A loud wrong answer is the one this
project keeps choosing.

THERE IS NO `workspace_id` PREDICATE AND NO TIME BOUND, AND BOTH ARE SAFE ONLY BECAUSE OF
A FACT ABOUT THIS WORKSPACE. `job_id`'s own column comment in `system.lakeflow.jobs` reads
"Only unique within a single workspace", and this view groups and joins on `job_id` and
`run_id` alone. Measured: `COUNT(DISTINCT workspace_id)` over the timeline is **1**, so
there is nothing to collide with. THE DAY A SECOND WORKSPACE'S ROWS APPEAR IN THIS
METASTORE -- a metastore shared across workspaces, or a federated `system` schema -- two
different jobs with the same `job_id` merge into one row set and the numbers stay
plausible. The fix is a `workspace_id` in every GROUP BY and in both join predicates; it
is not written today because a column with one value is a predicate nobody can test.
Likewise there is no `WHERE started_at > ...`: the view is total over what the platform
retains, which is the honest default for a record whose whole point is that its edges are
where the labels above come from.

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

# The four values of `sql_telemetry`. ALL FOUR ARE FACTS ABOUT THE ATTRIBUTION RECORD AND
# NONE IS A FACT ABOUT THE TASK.
# `MEASURED` says the metrics have something behind them; the other three say which way the
# record falls short. `NO_SQL_ATTRIBUTED` in particular is NOT evidence that a task issued
# no SQL -- see the header's "WHAT `no_sql_attributed` DOES NOT MEAN".
MEASURED = "measured"
NO_SQL_ATTRIBUTED = "no_sql_attributed"
NOT_YET_ATTRIBUTED = "not_yet_attributed"
OLDER_THAN_HISTORY = "older_than_history"

# ~~`SQL_TELEMETRY_VALUES`~~ was declared here and read by nothing -- not by `src/`, not by
# `databricks/`, not by a test -- with a comment claiming it was "spelled once because a
# dashboard filters on them". The dashboard has three table widgets, no filter widget and no
# WHERE clause. Removed by F4's closing code review: a constant nobody reads, justified by a
# consumer that does not exist, is two claims and neither was true.


@dataclass(frozen=True)
class SystemTables:
    """Where the platform's own tables live. Defaulted; overridden only by tests.

    IT IS NOT A CONFIGURATION KNOB and no task passes it. `system.lakeflow` and
    `system.query` are fixed names in every Unity Catalog metastore, so an override in a
    job would be a coordinate that can only be wrong. It exists because the alternative is
    a view whose SQL can never be executed anywhere but a workspace -- and this project has
    already shipped four guards that were green because nothing could run them. The tests
    point these three at local fixtures and drive the shipped string; `tests/dataops/
    test_views.py::test_the_telemetry_view_that_deploys_reads_the_platforms_own_tables`
    pins that the default is what deploys."""

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
    workspace -- measured, ZERO runs here report two distinct terminal states -- and they
    disagree the moment one does. The stakes are one-directional and that is why this is
    `MAX_BY`: alphabetically `CANCELED < FAILED < SUCCEEDED`, so `MAX` does not garble a
    state, it REPORTS A FAILED RUN AS SUCCEEDED and leaves every other number intact.
    `tests/dataops/test_telemetry.py::test_the_state_is_the_one_the_run_ended_on_and_not
    _the_largest_it_ever_showed` drives a constructed run that ends FAILED after a
    SUCCEEDED period, because no observed row can tell the two apart.

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

    `system.lakeflow.jobs` keeps a row per change, so a rename fans the join out. Measured
    2026-08-18 21:53Z: 25 rows over 22 `job_id`s, so THREE jobs here already carry more
    than one. `MAX_BY(name, change_time)` takes the latest.

    ROWS WITH A `delete_time` ARE KEPT. A deleted job's task runs still happened, and
    filtering them would drop task runs from a telemetry view -- silently, and exactly for
    the jobs nobody is watching any more."""
    return (
        f"SELECT job_id, MAX_BY(name, change_time) AS job_name\n  FROM {system.jobs}\n"
        "  GROUP BY job_id"
    )


def _statements_sql(system: SystemTables) -> str:
    """`query.history` summed to task-run grain, which is what stops the 4.5x fan-out.

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


def _history_window_sql(system: SystemTables) -> str:
    """The two edges of what `query.history` actually holds. One row, two timestamps.

    NOT FILTERED BY `job_task_run_id`. The window wanted is the table's ingestion and
    retention frontier, which interactive statements evidence just as well as job ones --
    and restricting it to job statements would move the frontier by however long this
    workspace happened to go without running a job, which is a property of an operator's
    week rather than of the table.

    WHAT IT COSTS: a second aggregate over `query.history` -- one column, no join, no
    grouping -- on a table this view already reads once. It is a view, so nothing pays it
    until somebody reads it, and the alternative was a label that is wrong for every task
    run finishing inside a lag measured in minutes to tens of minutes."""
    return (
        "SELECT MIN(end_time) AS oldest_statement, MAX(end_time) AS newest_statement\n"
        f"  FROM {system.query_history}"
    )


def _sql_telemetry_case() -> str:
    """The four-value label, first-match-wins, and the ORDER is the argument.

    `MEASURED` first: an attributed run needs no window reasoning. Then the two edges,
    BEFORE the bare `NO_SQL_ATTRIBUTED` else -- reversing that would let a run the record
    cannot reach be reported as a run the record reached and found empty, which is the
    exact substitution `sql_telemetry` exists to refuse.

    THE ELSE IS AN OBSERVATION ABOUT THE RECORD, NOT ABOUT THE TASK, and no arrangement of
    these arms can make it more than that. `w.newest_statement` bounds ingestion from BELOW
    -- a newer statement is present, so the table is at least that far along -- and the
    header records that no column, commit stamp or partition boundary in
    `system.query.history` bounds it from above. A run whose statements are still in flight
    is therefore in this ELSE, indistinguishable from one that issued none. The names carry
    that: `no_sql_ATTRIBUTED`, never `no_sql_issued`.

    THE `NOT_YET_ATTRIBUTED` ARM IS REACHABLE ONLY WHILE THE TIMELINE'S WATERMARK IS AHEAD
    OF `w.newest_statement`, because no `t.ended_at` exceeds the former. Both orderings
    were measured four minutes apart on 2026-08-18 and the arm went two rows to zero
    across them, so a zero here dates faster than a reader will assume -- see the header's
    "A ZERO IN `not_yet_attributed`". The arm is kept for the ordering it does not
    currently hold, which is the direction in which being wrong is safe.

    `newest_statement IS NULL` (an empty or unreadable `query.history`) folds into
    `NOT_YET_ATTRIBUTED` rather than into the ELSE. With no statement recorded anywhere,
    "nothing has been attributed yet" is literally true, and the ELSE would instead claim
    the record covers a run it demonstrably does not."""
    return (
        f"CASE WHEN s.statements IS NOT NULL THEN '{MEASURED}'\n"
        "    WHEN w.newest_statement IS NULL OR t.ended_at > w.newest_statement\n"
        f"      THEN '{NOT_YET_ATTRIBUTED}'\n"
        f"    WHEN t.ended_at < w.oldest_statement THEN '{OLDER_THAN_HISTORY}'\n"
        f"    ELSE '{NO_SQL_ATTRIBUTED}' END AS sql_telemetry"
    )


def task_telemetry_sql(system: SystemTables = SYSTEM) -> str:
    """The view body: one row per task-run attempt, with its statements where there are any.

    LEFT JOINs on both sides and for the same reason -- a task run whose job has aged out
    of `system.lakeflow.jobs` (three of them here) and a task run that issued no SQL (129)
    are both facts to report, and an inner join would delete them from the record.

    `CROSS JOIN history_window` is a one-row broadcast, not a fan-out: the CTE aggregates
    the whole of `query.history` to a single row, so every task run is paired with the same
    two timestamps and the row count is unchanged."""
    return (
        f"WITH task_runs AS (\n  {_task_runs_sql(system)}\n),\n"
        f"job_names AS (\n  {_job_names_sql(system)}\n),\n"
        f"statements AS (\n  {_statements_sql(system)}\n),\n"
        f"history_window AS (\n  {_history_window_sql(system)}\n)\n"
        "SELECT t.run_id, t.job_run_id, t.job_id, j.job_name, t.task_key,\n"
        "  ROW_NUMBER() OVER (\n"
        "    PARTITION BY t.job_run_id, t.task_key ORDER BY t.started_at\n"
        "  ) AS attempt,\n"
        "  t.started_at, t.ended_at, t.result_state,\n"
        "  t.setup_seconds, t.execution_seconds, t.cleanup_seconds, t.timeline_periods,\n"
        f"  {_sql_telemetry_case()},\n"
        "  s.statements, s.read_rows, s.written_rows, s.written_bytes\n"
        "FROM task_runs t\n"
        "  CROSS JOIN history_window w\n"
        "  LEFT JOIN job_names j ON j.job_id = t.job_id\n"
        "  LEFT JOIN statements s ON s.run_id = t.run_id"
    )
