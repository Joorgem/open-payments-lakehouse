"""The incident feed, driven out of a real two-attempt fixture rather than asserted.

THE TENSION THIS FILE EXISTS TO CLOSE. A feed that returns 11 and a feed that returns 22
both look like they worked: both are plausible counts, both render as a list of failures,
and the wrong one double-counts every incident in this corpus. A fixture with ONE attempt
per incident would report 11 whether or not the grouping exists -- which is this
repository's signature defect, a check whose output cannot distinguish "passed" from
"never ran", and which it has now found in seven guards. ADR 0018's standing instruction
is to ask what ELSE would produce the expected value; here the answer is "a query with no
GROUP BY at all", so the fixture carries the MEASURED two-attempt shape and this file also
runs the naive spelling and asserts it returns 22.

FIVE THINGS ARE PROVEN BY RUNNING SQL OVER TABLES, not by matching a string:

  1. 11 incidents over 22 gate rows, `attempts == 2` on each.
  2. The naive spelling returns 22 -- the trap is real, so the grouping guards something.
  3. A 1-attempt incident and a 3-attempt incident are each ONE record carrying their own
     count, so the fold is not secretly "divide by two".
  4. The wrong-key join (`CAST(_batch_id AS BIGINT)` against `run_id`) returns 0 and
     raises nothing, and the right key against the RAW attempts returns exactly twice the
     quarantine -- the two silent failures the feed exists to prevent.
  5. An empty telemetry view yields zero incidents and does not raise -- that test is run
     against the SHIPPED `task_telemetry_sql` over empty system tables, so it also proves
     the feed reads columns the deployed view actually produces. An empty answer that
     cannot be told from a broken query is worth nothing, so the same assertion names the
     output columns and the same SQL returns 11 rows two tests up.

WHY THE FIXTURE ALSO CARRIES ROWS THAT ARE NOT GATE RUNS. Without them, a feed that
forgot its `WHERE task_key = ...` would return exactly the same 11 records and every
assertion here would pass. One of those extra rows sits INSIDE one of the eleven job runs,
so the missing filter would change a NUMBER (`attempts` 2 -> 3) rather than only a row
count -- the direction a reader cannot spot.

THE DEV PREFIX IN THIS FILE IS A PLACEHOLDER AND MUST STAY ONE. The runtime `job_name`
carries `[dev <operator>] `, where the operator is a real username; CLAUDE.md forbids
committing one. `_DEV_PREFIX` below is invented, and the prefix test is parametrised over
several spellings precisely so that passing it cannot depend on any particular one.
"""
from __future__ import annotations

import pytest

from opl.bronze.promote import BATCH_COLUMN
from opl.dataops.telemetry import SystemTables, task_telemetry_sql
from opl.triage_agent.incidents import (
    DQ_GATE_TASK_KEY,
    TABLE_OF_JOB,
    incident_feed_sql,
    stripped_job_name_sql,
)

_SCHEMA = "opl_triage_probe"
_DAY = "2026-08-10"

# INVENTED. The deployed prefix is `[dev <operator>] `, and the operator is a Windows
# username this repository is not allowed to commit. Nothing about the feed depends on
# which name is inside the brackets -- the prefix test below is parametrised over several
# to keep it that way.
_DEV_PREFIX = "[dev fixture_operator] "

# THE MEASURED CORPUS: the eleven `job_run_id`s that ran `fail_on_dq` in this workspace and
# the job each belongs to, read 2026-08-24 from `dataops_task_telemetry`. Every one of them
# carries TWO attempt rows, both FAILED -- `max_retries: 0` does not prevent a retry -- so
# these eleven pairs are the 22 rows the naive spelling returns.
_MEASURED_INCIDENTS = (
    ("592660596679630", "opl-bronze-payments"),
    ("1121645114029617", "opl-bronze-cnpj-socios"),
    ("409962018634322", "opl-bronze-cnpj-socios"),
    ("128878829411613", "opl-bronze-cnpj-estabelecimentos"),
    ("321750543973966", "opl-bronze-cnpj-empresas"),
    ("371067950667703", "opl-bronze-cnpj-empresas"),
    ("184706631093131", "opl-bronze-cnpj-lookup"),
    ("241387611390862", "opl-bronze-cnpj-lookup"),
    ("996871467498110", "opl-bronze-cnpj-lookup"),
    ("187805471003061", "opl-bronze-cnpj-estabelecimentos"),
    ("315230730740144", "opl-bronze-cnpj-estabelecimentos"),
)

_PAYMENTS_RUN = _MEASURED_INCIDENTS[0][0]
_ESTAB_RUN = _MEASURED_INCIDENTS[3][0]

_JOB_IDS = {
    job: str(900 + index)
    for index, job in enumerate(sorted({job for _, job in _MEASURED_INCIDENTS}))
}

# The columns of `dataops_task_telemetry` this feed reads, plus `run_id` and `attempt`.
# A SUBSET of the view's eighteen: the rest are timings and statement metrics no triage
# record carries, and a fixture that reproduced them would be asserting the F4 view again.
# That the feed is compatible with the REAL view is proven separately, by
# `test_an_empty_telemetry_view_yields_no_incidents_and_does_not_raise`, which runs it over
# the shipped `task_telemetry_sql` rather than over this hand-built shape.
_SOURCE_COLUMNS = (
    ("run_id", "STRING"),
    ("job_run_id", "STRING"),
    ("job_id", "STRING"),
    ("job_name", "STRING"),
    ("task_key", "STRING"),
    ("attempt", "INT"),
    ("started_at", "TIMESTAMP"),
    ("ended_at", "TIMESTAMP"),
    ("result_state", "STRING"),
)

# What one incident record is. Named here so the empty-source test can assert the query
# PROJECTED something, which is what separates "no incidents" from "no query".
_FEED_COLUMNS = (
    "job_run_id",
    "batch_id",
    "source",
    "job_name",
    "job_id",
    "task_key",
    "attempts",
    "first_started_at",
    "last_ended_at",
    "result_states",
)


def _gate_rows() -> tuple[tuple, ...]:
    """Two FAILED attempts per measured incident: the 22 rows, generated from the 11."""
    rows = []
    for index, (job_run_id, job) in enumerate(_MEASURED_INCIDENTS):
        for attempt in (1, 2):
            rows.append(
                (
                    f"7{index:02d}{attempt}00000000",
                    job_run_id,
                    _JOB_IDS[job],
                    _DEV_PREFIX + job,
                    DQ_GATE_TASK_KEY,
                    attempt,
                    f"{9 + index:02d}:0{attempt}:00",
                    f"{9 + index:02d}:0{attempt}:30",
                    "FAILED",
                )
            )
    return tuple(rows)


# NOT GATE RUNS, and the feed must not see them. The first shares a `job_run_id` with a
# real incident, so a feed missing its `WHERE` reports that incident with three attempts;
# the second is a job run in which no gate ever fired, so the same defect adds a twelfth
# record. Two different failure shapes for one missing predicate.
_OTHER_TASK_ROWS = (
    (
        "800000000001", _PAYMENTS_RUN, _JOB_IDS["opl-bronze-payments"],
        _DEV_PREFIX + "opl-bronze-payments", "promote", 1,
        "09:00:00", "09:00:20", "SUCCEEDED",
    ),
    (
        "800000000002", "777000111222333", _JOB_IDS["opl-bronze-cnpj-estabelecimentos"],
        _DEV_PREFIX + "opl-bronze-cnpj-estabelecimentos", "ingest", 1,
        "20:00:00", "20:01:00", "SUCCEEDED",
    ),
)

_MEASURED_ROWS = _gate_rows() + _OTHER_TASK_ROWS

# SHAPES THE CORPUS DOES NOT CONTAIN, and a fixture that can only express what has already
# happened cannot fail on the mutation that has not. One attempt, three attempts, and a job
# whose name this project's declaration does not know -- the exact shape of a rename that
# reached the workspace and not the repository.
_UNEVEN_INCIDENTS = (
    ("111000000000001", "opl-bronze-ptax", 1),
    ("222000000000002", "opl-bronze-merchant", 3),
    ("333000000000003", "opl-bronze-cnpj-socios-v2", 2),
)


def _uneven_rows() -> tuple[tuple, ...]:
    rows = []
    for index, (job_run_id, job, attempts) in enumerate(_UNEVEN_INCIDENTS):
        for attempt in range(1, attempts + 1):
            rows.append(
                (
                    f"6{index:02d}{attempt}00000000",
                    job_run_id,
                    str(800 + index),
                    _DEV_PREFIX + job,
                    DQ_GATE_TASK_KEY,
                    attempt,
                    f"{9 + index:02d}:0{attempt}:00",
                    f"{9 + index:02d}:0{attempt}:30",
                    "FAILED",
                )
            )
    return tuple(rows)


# A quarantine, scaled down. The live one for `592660596679630` holds 2,000 rows and the
# raw-attempt join returns 4,000 of them; four and eight say the same thing in a fixture.
# The second batch is here so the join is shown to be selective rather than total.
_QUARANTINE_ROWS = tuple([_PAYMENTS_RUN] * 4 + [_ESTAB_RUN] * 2)

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
_HISTORY_COLUMNS = (
    ("query_source", "STRUCT<job_info: STRUCT<job_task_run_id: STRING>>"),
    ("end_time", "TIMESTAMP"), ("read_rows", "BIGINT"),
    ("written_rows", "BIGINT"), ("written_bytes", "BIGINT"),
)


def _empty_projection(columns: tuple[tuple[str, str], ...]) -> str:
    """A typed, row-less relation. `WHERE false` rather than an empty VALUES list, which
    Spark cannot type."""
    cast = ", ".join(f"CAST(NULL AS {kind}) AS {name}" for name, kind in columns)
    return f"SELECT {cast} WHERE false"


def _source_view_sql(rows: tuple[tuple, ...]) -> str:
    values = ",\n    ".join(
        f"('{run_id}', '{job_run}', '{job_id}', '{job_name}', '{task_key}', {attempt}, "
        f"TIMESTAMP'{_DAY} {start}', TIMESTAMP'{_DAY} {end}', '{state}')"
        for run_id, job_run, job_id, job_name, task_key, attempt, start, end, state in rows
    )
    names = ", ".join(name for name, _ in _SOURCE_COLUMNS)
    return f"SELECT * FROM VALUES\n    {values}\n  AS t({names})"


def _widened(view: str) -> str:
    """`telemetry_measured` with `job_run_id` as a BIGINT and every other column untouched.

    THE ONE INPUT THAT CAN TELL `CAST(job_run_id AS STRING)` FROM A NO-OP. The source view
    types that column STRING today, so the cast is unobservable over every other fixture in
    this file and deleting it leaves them all green. The projection is built from
    `_SOURCE_COLUMNS` rather than retyped, so a column added to the contract is carried
    here with no edit and this view cannot silently narrow the one it widens."""
    projected = ", ".join(
        f"CAST({name} AS BIGINT) AS {name}" if name == "job_run_id" else name
        for name, _ in _SOURCE_COLUMNS
    )
    return f"SELECT {projected} FROM {view}"


def _quarantine_sql() -> str:
    values = ", ".join(f"('{batch}')" for batch in _QUARANTINE_ROWS)
    return f"SELECT * FROM VALUES {values} AS t({BATCH_COLUMN})"


def _table(name: str) -> str:
    return f"spark_catalog.{_SCHEMA}.{name}"


@pytest.fixture(scope="module")
def probe(spark):
    """Three telemetry shapes and a quarantine, as views, in a schema this module drops.

    VIEWS AND NOT DELTA TABLES, for `tests/bronze/test_reconcile.py`'s reason: the feed
    reads a filter, a GROUP BY and four aggregates, which need rows and a schema and
    nothing else. `telemetry_empty` is the one built from the SHIPPED view definition
    rather than by hand, so the feed is executed against the real column contract at least
    once."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_SCHEMA}")
    for name, body in (
        ("system_timeline", _empty_projection(_TIMELINE_COLUMNS)),
        ("system_jobs", _empty_projection(_JOBS_COLUMNS)),
        ("system_history", _empty_projection(_HISTORY_COLUMNS)),
    ):
        spark.sql(f"CREATE OR REPLACE VIEW {_table(name)} AS {body}")
    for name, body in (
        ("telemetry_measured", _source_view_sql(_MEASURED_ROWS)),
        ("telemetry_uneven", _source_view_sql(_uneven_rows())),
        ("telemetry_empty", task_telemetry_sql(_EMPTY_SYSTEM)),
        ("quarantine", _quarantine_sql()),
        ("telemetry_bigint", _widened(_table("telemetry_measured"))),
    ):
        spark.sql(f"CREATE OR REPLACE VIEW {_table(name)} AS {body}")
    yield spark
    spark.sql(f"DROP DATABASE IF EXISTS {_SCHEMA} CASCADE")


def _feed(spark, name: str = "telemetry_measured") -> list:
    return spark.sql(incident_feed_sql(view=_table(name))).collect()


# ----------------------------------------------------------------------------------
# The grain, and the trap beside it.
# ----------------------------------------------------------------------------------


def test_the_feed_returns_one_record_per_incident_and_keeps_the_attempt_count(probe):
    """Eleven incidents over twenty-two gate rows, each reporting the two attempts it ran.

    `attempts` IS THE ASSERTION AND NOT A DECORATION. A feed that returned 11 records with
    the attempt count dropped would satisfy the row count and lose the fact a triager
    reads first -- that this job did not fail once, it failed, retried, and failed again
    against the same wall. `max_retries: 0` is set on every one of these tasks."""
    gate_rows = [row for row in _MEASURED_ROWS if row[4] == DQ_GATE_TASK_KEY]
    assert len(gate_rows) == 22 and len(_MEASURED_INCIDENTS) == 11

    records = _feed(probe)
    assert len(records) == 11
    assert {row["job_run_id"] for row in records} == {run for run, _ in _MEASURED_INCIDENTS}
    assert {row["attempts"] for row in records} == {2}
    assert all(row["result_states"] == ["FAILED", "FAILED"] for row in records)


def test_the_naive_spelling_returns_a_row_per_attempt_so_the_grouping_guards_something(
    probe,
):
    """THE TRAP, EXECUTED. Without this, the test above is a guard that decorates.

    A `SELECT` over the same filtered rows -- which is what anyone writes first, and which
    reads as an incident list -- returns 22 for the 11 incidents that exist. The two
    numbers are equally plausible in isolation, and this is the assertion that makes the
    difference between them a measured fact rather than an intention. Every count taken
    off the naive shape is exactly 2x in this corpus."""
    naive = (
        f"SELECT job_run_id, job_name, result_state FROM {_table('telemetry_measured')} "
        f"WHERE task_key = '{DQ_GATE_TASK_KEY}'"
    )
    rows = probe.sql(naive).collect()

    assert len(rows) == 22
    assert len({row["job_run_id"] for row in rows}) == 11
    assert len(_feed(probe)) == 11


def test_an_incident_with_one_attempt_and_one_with_three_are_each_one_record(probe):
    """So the fold is a GROUP BY and not "divide by two".

    Every incident in the live corpus ran exactly two attempts, so a query that halved a
    row count would agree with the correct one on every observed input. These three shapes
    do not exist in the workspace and are the only rows that can tell the two apart."""
    rows = _feed(probe, "telemetry_uneven")
    records = {row["job_run_id"]: row for row in rows}

    assert len(records) == len(rows) == 3, "keying on job_run_id must not COLLAPSE rows"
    assert records["111000000000001"]["attempts"] == 1
    assert records["222000000000002"]["attempts"] == 3
    assert records["333000000000003"]["attempts"] == 2
    assert records["222000000000002"]["result_states"] == ["FAILED"] * 3


def test_a_task_that_is_not_the_dq_gate_neither_adds_an_incident_nor_an_attempt(probe):
    """The `WHERE`, made load-bearing on a NUMBER and not only on a row count.

    One of the fixture's non-gate rows sits inside `592660596679630`, so a feed that
    dropped the filter would report that incident with three attempts -- a wrong number in
    a correct-looking record. The other is a job run with no gate at all, which would
    become a twelfth incident. Both are silent; neither is visible in a row count that a
    reader has no expected value for."""
    rows = _feed(probe)
    records = {row["job_run_id"]: row for row in rows}

    assert len(records) == len(rows), "keying on job_run_id must not COLLAPSE rows"
    assert records[_PAYMENTS_RUN]["attempts"] == 2
    assert records[_PAYMENTS_RUN]["result_states"] == ["FAILED", "FAILED"]
    assert "777000111222333" not in records
    assert all(row["task_key"] == DQ_GATE_TASK_KEY for row in records.values())


# ----------------------------------------------------------------------------------
# The batch key, and the two ways of getting it wrong that raise nothing.
# ----------------------------------------------------------------------------------


def test_the_quarantine_joins_the_feed_once_and_the_raw_attempts_twice(probe):
    """The fan-out this feed exists to prevent, measured on both sides of the fold.

    Live: a 2,000-row quarantine joined to the raw timeline on the RIGHT key returns
    4,000, because the two attempt rows fan every rejected row out twice. A sample, a
    reject-reason histogram or a blast-radius count taken off that join is exactly 2x --
    and 2x is not a number that looks wrong. Against the feed, one row per incident, the
    join returns the quarantine unchanged."""
    quarantine = _table("quarantine")
    feed = incident_feed_sql(view=_table("telemetry_measured"))
    folded = probe.sql(
        f"SELECT q.{BATCH_COLUMN} AS batch FROM ({feed}) i "
        f"JOIN {quarantine} q ON q.{BATCH_COLUMN} = i.batch_id"
    ).collect()
    raw = probe.sql(
        f"SELECT q.{BATCH_COLUMN} AS batch FROM {_table('telemetry_measured')} t "
        f"JOIN {quarantine} q ON q.{BATCH_COLUMN} = t.job_run_id "
        f"WHERE t.task_key = '{DQ_GATE_TASK_KEY}'"
    ).collect()

    assert len(_QUARANTINE_ROWS) == 6
    assert len(folded) == 6, "the feed must join a quarantine row exactly once"
    assert len(raw) == 12, "two attempts fan every quarantined row out twice"
    assert len([r for r in folded if r["batch"] == _PAYMENTS_RUN]) == 4
    assert len([r for r in raw if r["batch"] == _PAYMENTS_RUN]) == 8


def test_the_batch_key_is_a_string_and_the_bigint_cast_matches_nothing(probe):
    """THE WRONG KEY, PINNED BY A TEST RATHER THAN BY A COMMENT.

    `t.run_id = CAST(q._batch_id AS BIGINT)` is wrong twice -- `run_id` is the TASK-run id,
    not the job-run id, and the cast turns the mismatch into a type coercion instead of an
    error. It returns zero rows and raises nothing, so its output is indistinguishable
    from a quarantine that is genuinely empty.

    THE POSITIVE CONTROL IS IN THIS TEST AND NOT IN THE ONE ABOVE, which is where it used
    to be and where it protected nothing: an empty telemetry fixture, an empty quarantine
    or a broken view all make `wrong == []` pass on their own. `right` joins the SAME two
    tables on the SAME rows by the correct key and returns twelve, so the zero beside it is
    a fact about the KEY rather than about the data -- `tests/bronze/test_reconcile.py`'s
    standard, which is that the discriminating arm runs in the same test function."""
    wrong = probe.sql(
        f"SELECT 1 FROM {_table('telemetry_measured')} t "
        f"JOIN {_table('quarantine')} q ON t.run_id = CAST(q.{BATCH_COLUMN} AS BIGINT)"
    ).collect()
    right = probe.sql(
        f"SELECT 1 FROM {_table('telemetry_measured')} t "
        f"JOIN {_table('quarantine')} q ON t.job_run_id = q.{BATCH_COLUMN} "
        f"WHERE t.task_key = '{DQ_GATE_TASK_KEY}'"
    ).collect()

    assert len(right) == 12, "the control: the same two tables, the right key, non-empty"
    assert wrong == []

    feed = incident_feed_sql(view=_table("telemetry_measured"))
    batch = probe.sql(f"SELECT batch_id, typeof(batch_id) AS kind FROM ({feed})").collect()
    assert len(batch) == len(_MEASURED_INCIDENTS), "one row per incident, before collapsing"
    assert {row["kind"] for row in batch} == {"string"}
    assert {row["batch_id"] for row in batch} == {run for run, _ in _MEASURED_INCIDENTS}


def test_the_batch_id_is_still_a_string_when_the_platform_widens_the_run_id(probe):
    """THE CAST, OVER THE ONE SOURCE SHAPE THAT CAN OBSERVE IT.

    `CAST(job_run_id AS STRING)` is a no-op against every other fixture here, because the
    source view already types that column STRING -- so the sibling above asserts
    `typeof(batch_id) == 'string'` and would go on asserting it with the cast deleted. What
    the cast is FOR is the day the platform widens `job_run_id` to a BIGINT, and on that day
    the failure is a join that silently matches nothing against a quarantine whose
    `_batch_id` is a STRING: the same zero rows, raising nothing, that this file's other
    test measures for the wrong key.

    THE FIRST ASSERTION IS THE CONTROL. Without it a fixture that quietly stayed STRING
    would make this test a second copy of the sibling, passing for the wrong reason."""
    source_kind = probe.sql(
        f"SELECT typeof(job_run_id) AS kind FROM {_table('telemetry_bigint')} LIMIT 1"
    ).collect()[0]["kind"]
    assert source_kind == "bigint", "the control: the widened fixture really is widened"

    feed = incident_feed_sql(view=_table("telemetry_bigint"))
    rows = probe.sql(f"SELECT batch_id, typeof(batch_id) AS kind FROM ({feed})").collect()

    assert {row["kind"] for row in rows} == {"string"}
    assert {row["batch_id"] for row in rows} == {run for run, _ in _MEASURED_INCIDENTS}


# ----------------------------------------------------------------------------------
# The empty source, run against the view that actually deploys.
# ----------------------------------------------------------------------------------


def test_an_empty_telemetry_view_yields_no_incidents_and_does_not_raise(probe):
    """Zero incidents, and THREE things that stop this from being a test of nothing.

    An empty result is what a broken query, a mistyped filter and a genuinely quiet
    workspace all produce, so on its own this assertion is worth nothing. What separates
    them here: (1) the source is built from the SHIPPED `task_telemetry_sql`, not by hand,
    so the query having run at all proves the feed reads columns the deployed view really
    produces -- an unresolved column would raise rather than return nothing; (2) the
    projected column names are asserted, so a query that returned nothing because it
    selected nothing fails; (3) the same SQL returns eleven rows over the populated
    fixture, so emptiness here is a property of the DATA."""
    empty = probe.sql(incident_feed_sql(view=_table("telemetry_empty")))
    populated = probe.sql(incident_feed_sql(view=_table("telemetry_measured")))

    assert empty.columns == list(_FEED_COLUMNS)
    assert empty.collect() == []
    assert populated.columns == list(_FEED_COLUMNS)
    assert len(populated.collect()) == 11


# ----------------------------------------------------------------------------------
# The job name, the table, and the declaration behind them.
# ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "prefix",
    ["", "[dev fixture_operator] ", "[dev another_operator] ", "[dev ci-runner]  "],
    ids=["none", "one", "another", "extra-space"],
)
def test_the_bundle_dev_prefix_is_stripped_by_shape_and_no_operator_name_is_pinned(
    probe, prefix
):
    """`mode: development` names jobs `[dev <operator>] <name>`, and the operator is a
    username this repository may not commit.

    So the pattern matches a SHAPE -- any leading bracketed token -- and this test is
    parametrised over several so that passing it cannot depend on one spelling. The empty
    prefix is in the list because a target that adds none must also work: the expression
    has to be a strip, not a required unwrap."""
    stripped = probe.sql(
        f"SELECT {stripped_job_name_sql('name')} AS job_name FROM VALUES "
        f"('{prefix}opl-bronze-cnpj-socios') AS t(name)"
    ).collect()

    assert stripped[0]["job_name"] == "opl-bronze-cnpj-socios"


def test_the_source_column_is_the_registry_key_the_repromote_command_takes(probe):
    """Each incident resolves to the table whose quarantine holds its rejected rows.

    The value is a `REGISTRY` key, which is what `table_spec` resolves and what
    `repromote_batch_job.yml` takes -- and it is spelled `source` because that is already
    the name `dataops_reconciliation` and `dataops_freshness` publish it under, so a later
    join to either needs no translation."""
    rows = _feed(probe)
    records = {row["job_run_id"]: row for row in rows}
    expected = {run: TABLE_OF_JOB[job] for run, job in _MEASURED_INCIDENTS}

    assert len(records) == len(rows), "keying on job_run_id must not COLLAPSE rows"
    assert {run: row["source"] for run, row in records.items()} == expected
    assert records[_PAYMENTS_RUN]["job_name"] == "opl-bronze-payments"


def test_an_incident_on_a_job_the_declaration_does_not_know_is_reported_not_dropped(probe):
    """A stale declaration must be VISIBLE, and the visible form is a NULL table.

    `element_at` on a missing key returns NULL, and the record survives with its
    `job_run_id`, its attempts and its job name intact. Filtering it out instead would
    make a renamed job look like a job that stopped failing -- and `telemetry.py` already
    keeps task runs whose job has aged out of `system.lakeflow.jobs` for this reason.

    THE NULL IS DISCRIMINATED AGAINST A HIT IN THE SAME VIEW, because a NULL is also what a
    lookup that can never match returns -- and that mutation (`element_at(map(...),
    concat(job_name, '-zz'))`) left this test green when the only assertion about the
    lookup was the NULL. `known` resolves through the SAME `element_at` over the SAME rows,
    so the pair says "this key hits and that one does not" rather than "nothing hits"."""
    rows = _feed(probe, "telemetry_uneven")
    records = {row["job_run_id"]: row for row in rows}
    unknown, known = records["333000000000003"], records["111000000000001"]

    assert len(records) == len(rows), "keying on job_run_id must not COLLAPSE rows"
    assert known["source"] == TABLE_OF_JOB["opl-bronze-ptax"]
    assert unknown["source"] is None
    assert unknown["job_name"] == "opl-bronze-cnpj-socios-v2"
    assert unknown["attempts"] == 2
