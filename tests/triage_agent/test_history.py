"""The last-N comparison, driven out of the measured gate schedule rather than asserted.

THE TENSION THIS FILE EXISTS TO CLOSE. A history module that returns a number and one that
returns the RIGHT number both look like they worked: all three defects this module is built
against return a plausible integer, raise nothing and produce no NULL. So the fixture
reproduces the workspace's own gate schedule and every wrong answer is EXECUTED here beside
the right one -- the shape `test_incidents.py` uses when it asserts that the naive spelling
returns 22 beside the feed's 11.

THE FOUR BULLETS BELOW EACH NAME THE QUERY THEY ARE MEASURED AGAINST, and nothing here
generalises over what they MOVE. Two that did have stood in this place and both were false: the
first said each differed from the RIGHT query in exactly one respect (false for two of the
three, in the direction that misattributes the headline defect); the second said each link
moved one thing from the link before it, where the four constants in chain order move ONE
thing, then THREE, then TWO. No third umbrella replaces them -- a baseline named on each
bullet is what makes a difference in an answer attributable to something.

  * `_NAIVE_TIMESTAMP` is the controller's hand query as it was run (0.10) -- anchored on
    the incident's own `fail_on_dq` row, `<`, and NO identity predicate at all. THREE
    things separate it from the shipped query, and the +1 it reports on all eleven is the
    MISSING IDENTITY CLAUSE's: leave the anchor and the `<` exactly where they are, add
    `job_run_id <> :batch_id`, and every count comes back right. The bound is the third
    difference and on this fixture it is inert, a `fail_on_dq` anchor sitting minutes after
    the condition rows it is compared against -- which is NOT true of the shipped shape,
    where the same character decides a count.
  * `_HAND_QUERY_WITH_IDENTITY` is that same query with `job_run_id <> :batch_id` added and
    nothing else touched. Run at `check_bad_rows` it returns all eleven RIGHT counts, which
    is what separates the two causes; run at the live gate spelling, with the task key the
    only thing that has moved, it is defect 2 and its three zeroes are the KEY's.
  * `_NO_FOLD` counts the shipped prior-runs leg with the fold DROPPED, and moves nothing
    else.
  * `_STRICTLY_BEFORE` counts the SHIPPED prior-runs leg, folded, with `<=` narrowed to
    `<`, and moves nothing else. It is the one character that would turn the identity
    predicate into dead code, and the tie in the fixture is what makes the two bounds
    disagree on a number.

`_OFF_THE_TERMINAL_STATE` and `_KEYED_ON_THE_NAME` are not links in that chain: each is the
shipped shape reading one different column, and each is named beside the test that runs it.

WHAT IS MEASURED HERE AND WHAT IS INVENTED, LABELLED, BECAUSE THE FILE CONTAINS BOTH.

  MEASURED (`docs/f6-run-evidence.md` 0.10, controller-verified 2026-08-25): the eleven
  incident `job_run_id`s, their prior-execution and prior-incident counts, the gate-run
  count per job (8/6/5/4/3/2/1 = 29), the reconstructed POSITION of each incident in its
  job's sequence, that `check_bad_rows` runs exactly once per job run, that it is SUCCEEDED
  on all 29, that `fail_on_dq` carries two FAILED task runs per incident, and that the
  lookup's first five gate runs are under the retired `dq_gate` spelling.

  INVENTED, and each carries a comment saying what it is for: the eighteen non-incident
  `job_run_id`s (0.10 publishes counts, not ids), the `job_id`s, the calendar (only the
  ORDER of runs is measured -- in the workspace the lookup's five `dq_gate` runs share
  2026-07-24 and its `dq_gate_batch` run is 2026-07-31), the dev prefix, the DOUBLED
  `check_bad_rows` row, the ONE run whose `job_name` is NULL, and the TIE between two
  estabelecimentos condition tasks.

THE DOUBLED ROW IS THE ONE THING A FAITHFUL FIXTURE CANNOT SUPPLY. `check_bad_rows` runs
once per job run on all seven jobs -- 29 task runs over 29 job runs -- so a fixture that
reproduced this workspace exactly WOULD REPORT GREEN FOR A QUERY WITH NO FOLD IN IT. The
two-attempt fan-out is `fail_on_dq`'s alone. `_DOUBLED_GATE_RUN` is therefore constructed,
on a run that is PRIOR to three of the eleven incidents, and it is labelled the way
`conftest._MATRIX_BATCH` and `test_severity._DISAGREEMENT_VIEW` are.

AND THE OTHER FOLD IS VISIBLE IN THE CORPUS, which is worth separating from the sentence
above rather than filing under it: `prior_incidents` counts job runs that also fired the
gate, and `fail_on_dq` really does carry two rows per incident here, so THAT leg's DISTINCT
is exercised by the measured data alone.

WHAT THIS FILE DOES NOT PROVE, NAMED HERE RATHER THAN LEFT TO BE ASSUMED. Every row it
reads is hand-built, so nothing in it says the four columns the module reads --
`job_run_id`, `job_id`, `started_at`, `task_key`, and NOT `result_state`, which is defect
3 -- exist in the view F4 deploys. `tests/triage_agent/test_history_absence.py` runs this
module's statement over `task_telemetry_sql`'s own body and closes exactly that; the
argument that stood in its place, that those four are a subset of the six T1's feed reads
and are therefore covered by T1's test, was sound and was still an argument. That file also
drives the retention shape of `gate_run_absent`, which this corpus cannot carry without
moving its own `fail_on_dq` census.

THE DEV PREFIX HERE IS A PLACEHOLDER AND MUST STAY ONE: the runtime `job_name` carries
`[dev <operator>] `, where the operator is a real username CLAUDE.md forbids committing.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from opl.triage_agent.history import (
    GATE_RUN_ABSENT,
    GATE_SPELLINGS,
    HISTORY_COMPLETE,
    HISTORY_TASK_KEY,
    INSUFFICIENT_HISTORY,
    N_EXECUTIONS,
    NO_PRIOR_EXECUTION,
    history_sql,
    live_gate_spellings,
    retired_gate_spellings,
)
from opl.triage_agent.incidents import DQ_GATE_TASK_KEY

_SCHEMA = "opl_history_probe"

# INVENTED. The deployed prefix is `[dev <operator>] `; nothing in this module reads a job
# name at all, and the one test that does is about a name being ABSENT.
_DEV_PREFIX = "[dev fixture_operator] "

# INVENTED. Only the ORDER of a job's runs is measured, not their calendar.
_BASE = datetime(2026, 7, 24, 6, 0, 0)

# THE MEASURED SCHEDULE: each job's gate-run count, and WHICH position in that sequence each
# incident occupies. Both halves are from 0.10 -- the counts from the per-job census, the
# positions from the reconciliation of the per-incident prior counts against it -- and the
# fixture is built from the positions while the assertions are made against the counts,
# which is what keeps the test from proving only that arithmetic is arithmetic.
#
# 8 + 6 + 5 + 4 + 3 + 2 + 1 = 29, which is the census that reconciles with `check_bad_rows`.
_JOBS = (
    (
        "opl-bronze-cnpj-estabelecimentos",
        8,
        {3: "315230730740144", 4: "187805471003061", 8: "128878829411613"},
    ),
    (
        "opl-bronze-cnpj-lookup",
        6,
        {2: "996871467498110", 4: "241387611390862", 5: "184706631093131"},
    ),
    ("opl-bronze-payments", 5, {3: "592660596679630"}),
    ("opl-bronze-cnpj-socios", 4, {1: "1121645114029617", 4: "409962018634322"}),
    ("opl-bronze-merchant", 3, {}),
    ("opl-bronze-cnpj-empresas", 2, {1: "321750543973966", 2: "371067950667703"}),
    ("opl-bronze-ptax", 1, {}),
)

# 0.10's OWN TABLE, TYPED FROM THE DOCUMENT: (job_run_id, prior executions, prior incidents,
# same under the naive timestamp, same under the naive key). Not derived from `_JOBS` above
# -- the two are independent spellings of one measurement, and the test that holds them
# equal is what makes a disagreement between the document and this module visible.
_MEASURED = (
    ("128878829411613", 7, 2, 8, 7),
    ("184706631093131", 4, 2, 5, 0),
    ("187805471003061", 3, 1, 4, 3),
    ("241387611390862", 3, 1, 4, 0),
    ("409962018634322", 3, 1, 4, 3),
    ("315230730740144", 2, 0, 3, 2),
    ("592660596679630", 2, 0, 3, 2),
    ("371067950667703", 1, 1, 2, 1),
    ("996871467498110", 1, 0, 2, 0),
    ("1121645114029617", 0, 0, 1, 0),
    ("321750543973966", 0, 0, 1, 0),
)

_INCIDENTS = tuple(batch for batch, *_ in _MEASURED)
_LOOKUP_INCIDENTS = ("184706631093131", "241387611390862", "996871467498110")
_ESTAB_NEWEST = "128878829411613"

# INVENTED, both on `opl-bronze-cnpj-estabelecimentos`, both on runs that are PRIOR to all
# three of that job's incidents, and both chosen so the measured eleven counts do not move.
#
# `_DOUBLED_GATE_RUN` is the run given a SECOND `check_bad_rows` task run. No job run in
# this workspace has one, which is exactly why it has to be constructed: without it, a query
# that never folded to `job_run_id` would return every measured number correctly.
# `_AGED_OUT_NAME_RUN` is the run whose `job_name` is NULL on every row -- the shape
# `opl.dataops.telemetry` deliberately keeps when a job ages out of `system.lakeflow.jobs`,
# and the one input that separates keying the history on `job_id` from keying it on the name.
_DOUBLED_GATE_RUN = 2
_AGED_OUT_NAME_RUN = 1

# INVENTED, and the fixture's ONLY tie. Run 7 of estabelecimentos is retimed onto run 8's
# day, so the two runs' condition tasks carry the SAME `started_at`. NOTHING MEASURED SAYS
# WHETHER TWO GATE RUNS OF ONE JOB HAVE EVER SHARED AN INSTANT in this workspace -- 0.10
# records that the lookup's five `dq_gate` runs share a DAY and goes no finer -- so the tie
# is constructed rather than reproduced, and it is constructed because without it `<=` and
# `<` differ only in the text of the query. It moves no count under the shipped bound: run 7
# was prior to run 8 before the retiming and still is, and it is later than that job's other
# two incidents either way.
_TIED_PRIOR_RUN = 7
_TIED_TO_RUN = 8

# A batch id that is in no fixture row at all: a mistyped id, or a batch the timeline has
# finished forgetting. IT IS NOT THE SHAPE `gate_run_absent` WAS DESIGNED FOR, and this
# comment used to say it was. The designed-for shape is a batch whose `fail_on_dq` rows are
# still in the window while the `check_bad_rows` row that would anchor it has aged out --
# the order a moving retention floor reaches them in, the condition task being the older of
# the two -- and that shape is driven in `tests/triage_agent/test_history_absence.py`,
# because putting it in this corpus would move the measured `fail_on_dq` census.
_BATCH_WITH_NO_GATE_RUN = "404000000000404"

_HISTORY_COLUMNS = (
    "batch_id",
    "job_id",
    "gate_started_at",
    "executions_requested",
    "prior_executions",
    "prior_incidents",
    "history",
)

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

# THE WRONG ANSWERS, SPELLED AS QUERIES, PLUS THE ONE THAT IS NOT WRONG. `{view}` is the
# same relation the module is pointed at, so each runs over the same rows the right answer
# is taken from, and `{gate}` is the task key the caller supplies -- which is what lets
# `_HAND_QUERY_WITH_IDENTITY` run twice, once at each key, with nothing else moving.
_NAIVE_TIMESTAMP = """
WITH gate_runs AS (
  SELECT job_run_id, job_id, started_at FROM {view} WHERE task_key = '{gate}'
),
incident AS (
  SELECT job_id, MIN(started_at) AS started_at FROM {view}
  WHERE task_key = '{incident}' AND job_run_id = :batch_id GROUP BY job_id
)
SELECT COUNT(DISTINCT g.job_run_id) AS prior_executions
FROM gate_runs g JOIN incident i ON g.job_id = i.job_id
WHERE g.started_at < i.started_at
"""

_HAND_QUERY_WITH_IDENTITY = """
WITH gate_runs AS (
  SELECT job_run_id, job_id, started_at FROM {view} WHERE task_key = '{gate}'
),
incident AS (
  SELECT job_id, MIN(started_at) AS started_at FROM {view}
  WHERE task_key = '{incident}' AND job_run_id = :batch_id GROUP BY job_id
)
SELECT COUNT(DISTINCT g.job_run_id) AS prior_executions
FROM gate_runs g JOIN incident i ON g.job_id = i.job_id
WHERE g.started_at < i.started_at AND g.job_run_id <> :batch_id
"""

_NO_FOLD = """
WITH gate_runs AS (
  SELECT job_run_id, job_id, started_at FROM {view} WHERE task_key = '{gate}'
),
own_gate AS (
  SELECT job_id, MIN(started_at) AS gate_started_at FROM gate_runs
  WHERE job_run_id = :batch_id GROUP BY job_id
)
SELECT COUNT(*) AS prior_executions
FROM gate_runs g JOIN own_gate o ON g.job_id = o.job_id
WHERE g.job_run_id <> :batch_id AND g.started_at <= o.gate_started_at
"""

_STRICTLY_BEFORE = """
WITH gate_runs AS (
  SELECT job_run_id, job_id, started_at FROM {view} WHERE task_key = '{gate}'
),
own_gate AS (
  SELECT job_id, MIN(started_at) AS gate_started_at FROM gate_runs
  WHERE job_run_id = :batch_id GROUP BY job_id
)
SELECT COUNT(DISTINCT g.job_run_id) AS prior_executions
FROM gate_runs g JOIN own_gate o ON g.job_id = o.job_id
WHERE g.job_run_id <> :batch_id AND g.started_at < o.gate_started_at
"""

_OFF_THE_TERMINAL_STATE = """
WITH gate_runs AS (
  SELECT job_run_id, job_id, started_at, result_state FROM {view}
  WHERE task_key = '{gate}'
),
own_gate AS (
  SELECT job_id, MIN(started_at) AS gate_started_at FROM gate_runs
  WHERE job_run_id = :batch_id GROUP BY job_id
)
SELECT COUNT(DISTINCT g.job_run_id) AS prior_incidents
FROM gate_runs g JOIN own_gate o ON g.job_id = o.job_id
WHERE g.job_run_id <> :batch_id AND g.started_at <= o.gate_started_at
  AND g.result_state <> 'SUCCEEDED'
"""

_UNFOLDED_INCIDENTS = """
WITH gate_runs AS (
  SELECT job_run_id, job_id, started_at FROM {view} WHERE task_key = '{gate}'
),
own_gate AS (
  SELECT job_id, MIN(started_at) AS gate_started_at FROM gate_runs
  WHERE job_run_id = :batch_id GROUP BY job_id
),
prior_runs AS (
  SELECT DISTINCT g.job_run_id FROM gate_runs g JOIN own_gate o ON g.job_id = o.job_id
  WHERE g.job_run_id <> :batch_id AND g.started_at <= o.gate_started_at
),
gated AS (
  SELECT job_run_id FROM {view} WHERE task_key = '{incident}'
)
SELECT COUNT(*) AS prior_executions, COUNT(g.job_run_id) AS prior_incidents
FROM prior_runs p LEFT JOIN gated g ON g.job_run_id = p.job_run_id
"""

_KEYED_ON_THE_NAME = """
WITH gate_runs AS (
  SELECT job_run_id, job_name, started_at FROM {view} WHERE task_key = '{gate}'
),
own_gate AS (
  SELECT job_name, MIN(started_at) AS gate_started_at FROM gate_runs
  WHERE job_run_id = :batch_id GROUP BY job_name
)
SELECT COUNT(DISTINCT g.job_run_id) AS prior_executions
FROM gate_runs g JOIN own_gate o ON g.job_name = o.job_name
WHERE g.job_run_id <> :batch_id AND g.started_at <= o.gate_started_at
"""


def _job_run_id(job_index: int, position: int, incidents: dict[int, str]) -> str:
    """The measured id where 0.10 publishes one, an invented placeholder otherwise."""
    return incidents.get(position, f"9{job_index}{position:02d}00000000")


def _job_name(job: str, job_index: int, position: int) -> str:
    """The job's runtime name as a SQL literal, or a typed NULL for the aged-out run."""
    aged_out = job_index == 0 and position == _AGED_OUT_NAME_RUN
    return "CAST(NULL AS STRING)" if aged_out else f"'{_DEV_PREFIX}{job}'"


def _gate_spelling(job_index: int, position: int) -> str:
    """Which gate task ran. MEASURED: the lookup's first five are the retired spelling.

    Its sixth run -- and every run of every other job -- is under the live one. That is the
    whole of 0.8's hazard in this fixture: keyed on the live spelling alone, the lookup's
    only visible gate run postdates all three of its incidents."""
    retired, = retired_gate_spellings()
    live, = live_gate_spellings()
    return retired if job_index == 1 and position <= 5 else live


def _run_started_at(job_index: int, position: int) -> datetime:
    """When this job run's tasks start. INVENTED: only the ORDER of runs is measured.

    ONE RUN IS RETIMED, and it is the fixture's only tie -- see `_TIED_PRIOR_RUN`. Run 7 of
    estabelecimentos starts on run 8's day, so the two runs' `check_bad_rows` rows carry the
    same instant and `<=` and `<` disagree on a NUMBER rather than only in the text of the
    query. Every task of the run moves together, so nothing inside it is reordered."""
    if job_index == 0 and position == _TIED_PRIOR_RUN:
        position = _TIED_TO_RUN
    return _BASE + timedelta(days=position - 1, hours=job_index)


def _run_rows(job: str, job_index: int, position: int, incidents: dict[int, str]) -> list:
    """One job run's task rows: its gate, its condition task, and its failure attempts.

    THE ORDER INSIDE THE RUN IS THE MEASURED ONE AND IT IS WHAT DEFECT 1 RESTS ON:
    `check_bad_rows` starts BEFORE `fail_on_dq`, because the condition task is what decides
    whether the failure task runs at all. Anchor a history on the incident's own start and
    the incident's own gate run is inside the window."""
    job_run_id = _job_run_id(job_index, position, incidents)
    name = _job_name(job, job_index, position)
    at = _run_started_at(job_index, position)
    rows = [
        (job_run_id, str(900 + job_index), name, _gate_spelling(job_index, position),
         1, at, "SUCCEEDED"),
        (job_run_id, str(900 + job_index), name, HISTORY_TASK_KEY,
         1, at + timedelta(minutes=10), "SUCCEEDED"),
    ]
    if job_index == 0 and position == _DOUBLED_GATE_RUN:
        rows.append((job_run_id, str(900 + job_index), name, HISTORY_TASK_KEY,
                     2, at + timedelta(minutes=12), "SUCCEEDED"))
    if position in incidents:
        rows.extend(
            (job_run_id, str(900 + job_index), name, DQ_GATE_TASK_KEY, attempt,
             at + timedelta(minutes=20 * attempt), "FAILED")
            for attempt in (1, 2)
        )
    return rows


def _all_rows() -> tuple[tuple, ...]:
    """The whole fixture: 29 job runs over seven jobs, plus the three invented shapes."""
    rows = []
    for job_index, (job, runs, incidents) in enumerate(_JOBS):
        for position in range(1, runs + 1):
            rows.extend(_run_rows(job, job_index, position, incidents))
    return tuple((f"7{index:05d}", *row) for index, row in enumerate(rows))


def _values_sql(rows: tuple[tuple, ...]) -> str:
    values = ",\n    ".join(
        f"('{run_id}', '{job_run}', '{job_id}', {name}, '{task}', {attempt}, "
        f"TIMESTAMP'{start:%Y-%m-%d %H:%M:%S}', "
        f"TIMESTAMP'{start + timedelta(minutes=5):%Y-%m-%d %H:%M:%S}', '{state}')"
        for run_id, job_run, job_id, name, task, attempt, start, state in rows
    )
    names = ", ".join(name for name, _ in _SOURCE_COLUMNS)
    return f"SELECT * FROM VALUES\n    {values}\n  AS t({names})"


def _table(name: str) -> str:
    return f"spark_catalog.{_SCHEMA}.{name}"


@pytest.fixture(scope="module")
def gate_probe(spark):
    """The telemetry shape this module reads, as a view, in a schema this module drops.

    NOT NAMED `probe`, DELIBERATELY. `tests/triage_agent/conftest.py` defines a
    session-scoped `probe` (the quarantine corpus) and `test_incidents.py` defines its own
    module-scoped one that shadows it; a third would put three meanings on one name in one
    package. Fixture resolution would have worked -- a module-scoped definition shadows only
    inside its own module -- so this is about a reader, and the hazard is recorded here
    rather than relied on being noticed."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_SCHEMA}")
    spark.sql(f"CREATE OR REPLACE VIEW {_table('telemetry')} AS {_values_sql(_all_rows())}")
    yield spark
    spark.sql(f"DROP DATABASE IF EXISTS {_SCHEMA} CASCADE")


def _reading(spark, batch: str, **kwargs) -> dict:
    """One incident's reading, as a plain dict. Exactly one row, asserted every time."""
    rows = spark.sql(
        history_sql(view=_table("telemetry"), **kwargs), args={"batch_id": batch}
    ).collect()
    assert len(rows) == 1, f"{batch} produced {len(rows)} rows, and this is one per batch"
    return rows[0].asDict()


def _naive(spark, statement: str, batch: str, column: str, gate: str = HISTORY_TASK_KEY):
    """One of the wrong answers, run over the same rows as the right one."""
    sql = statement.format(view=_table("telemetry"), gate=gate, incident=DQ_GATE_TASK_KEY)
    return spark.sql(sql, args={"batch_id": batch}).collect()[0][column]


# ----------------------------------------------------------------------------------
# The fixture is the measurement, before anything is asserted against it.
# ----------------------------------------------------------------------------------


def test_the_fixture_reproduces_the_measured_gate_schedule(gate_probe):
    """0.10's census, over the rows every other test in this file reads.

    THE POSITIONS AND THE COUNTS ARE HELD AGAINST EACH OTHER HERE, which is the property
    0.10 claims for its own table -- every column derivable from the others, none typed
    twice. `_JOBS` carries the positions and `_MEASURED` carries the counts, and this is
    where a typo in either becomes visible instead of becoming an expected value.

    The last two assertions are the corpus facts defect 3 rests on: the condition task is
    SUCCEEDED on every run whether the gate fired or not, and `fail_on_dq` carries two rows
    per incident."""
    gate = f"SELECT * FROM {_table('telemetry')} WHERE task_key = '{HISTORY_TASK_KEY}'"
    runs = gate_probe.sql(gate).collect()
    per_job = {job: runs for job, runs, _ in _JOBS}
    positions = {batch: at for _, _, ids in _JOBS for at, batch in ids.items()}

    assert sum(per_job.values()) == 29
    assert len({row["job_run_id"] for row in runs}) == 29
    assert len(runs) == 30, "the 29 measured rows plus the one INVENTED doubled row"
    assert {batch: at - 1 for batch, at in positions.items()} == {
        batch: prior for batch, prior, *_ in _MEASURED
    }
    assert {row["result_state"] for row in runs} == {"SUCCEEDED"}

    failures = f"SELECT * FROM {_table('telemetry')} WHERE task_key = '{DQ_GATE_TASK_KEY}'"
    attempts = gate_probe.sql(failures).collect()
    assert len(attempts) == 22 and len({row["job_run_id"] for row in attempts}) == 11
    assert {row["result_state"] for row in attempts} == {"FAILED"}


def test_the_two_gate_spellings_sum_to_the_history_key(gate_probe):
    """`dq_gate (5) + dq_gate_batch (24) = 29 = check_bad_rows (29)`, EXECUTED.

    The identity is the whole argument for the key this module counts on, and it is quoted
    in the header of a module that would work perfectly well without it being true -- so it
    is run here. It reads `GATE_SPELLINGS` in full, so the RETIRED entry is not decoration.

    WHAT THIS TEST IS NOT IS THE LOCK ON THAT ENTRY, and saying it was is an error this
    docstring made about itself. Delete the `dq_gate` entry and `_gate_spelling`'s
    `retired, = retired_gate_spellings()` raises while the FIXTURE is being built: every
    Spark test in this file errors before this one compares anything, and two tests in
    `test_history_declaration.py` fail. The entry is held, harder than described and one
    file away -- and not by the arithmetic below."""
    counted = {}
    for spelling in sorted(GATE_SPELLINGS):
        sql = (
            f"SELECT COUNT(DISTINCT job_run_id) AS runs FROM {_table('telemetry')} "
            f"WHERE task_key = '{spelling}'"
        )
        counted[spelling] = gate_probe.sql(sql).collect()[0]["runs"]

    assert counted == {"dq_gate": 5, "dq_gate_batch": 24}
    assert sum(counted.values()) == 29


# ----------------------------------------------------------------------------------
# The eleven counts, and the three ways to get them wrong.
# ----------------------------------------------------------------------------------


def test_the_eleven_prior_counts_are_the_ones_the_workspace_was_measured_to_have(gate_probe):
    """PREDICTION 6, RUN. The module's answer against 0.10's, both numbers, all eleven.

    The controller's hand query and this module are two spellings of one question, and 0.10
    records that the controller's FIRST spelling was wrong by one on all eleven. A
    disagreement here is a finding either way and is not to be adjusted into agreement."""
    readings = {batch: _reading(gate_probe, batch) for batch in _INCIDENTS}
    produced = {
        batch: (row["prior_executions"], row["prior_incidents"])
        for batch, row in readings.items()
    }

    assert produced == {batch: (prior, gated) for batch, prior, gated, *_ in _MEASURED}
    assert [produced[batch][0] for batch, *_ in _MEASURED] == [7, 4, 3, 3, 3, 2, 2, 1, 1, 0, 0]


def test_the_naive_timestamp_adds_one_to_every_incident_and_deletes_both_zeroes(gate_probe):
    """DEFECT 1, EXECUTED: "gate runs that started before this incident started".

    It is the query anyone writes first, it is the one the controller ran first, and it
    counts the incident's OWN gate run -- because `check_bad_rows` starts before
    `fail_on_dq` inside the same job run. Wrong by one on all eleven, nothing raises, and
    the two incidents whose true history is 0 come back as 1: THE ONE STATE A TRIAGER MOST
    NEEDS, this table has never been gated before, is the state the defect deletes.

    The right answer is taken in the same test over the same rows, so the difference is
    about the QUERY rather than about the fixture -- and a THIRD run here says which part
    of the query. THE NAME IS THE CONTROLLER'S AND THE TIMESTAMP IS NOT THE CAUSE: add the
    identity clause alone, leaving this query's `fail_on_dq` anchor and its `<` exactly
    where they are, and all eleven counts come back right. So the defect is "no identity
    exclusion, on an anchor that postdates the row being excluded", its two halves are
    separable, and only one of them is doing the damage -- which is 0.10's own amendment
    executed here rather than quoted."""
    naive = {batch: _naive(gate_probe, _NAIVE_TIMESTAMP, batch, "prior_executions")
             for batch in _INCIDENTS}
    repaired = {batch: _naive(gate_probe, _HAND_QUERY_WITH_IDENTITY, batch,
                              "prior_executions") for batch in _INCIDENTS}
    right = {batch: _reading(gate_probe, batch)["prior_executions"] for batch in _INCIDENTS}

    assert naive == {batch: inflated for batch, _, _, inflated, _ in _MEASURED}
    assert all(naive[batch] == right[batch] + 1 for batch in _INCIDENTS)
    assert [naive[batch] for batch in ("1121645114029617", "321750543973966")] == [1, 1]
    assert [right[batch] for batch in ("1121645114029617", "321750543973966")] == [0, 0]
    assert repaired == right, "the identity clause alone repairs it -- the anchor does not"


def test_the_naive_key_erases_the_lookups_history_and_leaves_the_other_eight_intact(
    gate_probe,
):
    """DEFECT 2, EXECUTED: the same query keyed on the surviving gate spelling.

    THE SAME STRING THE TEST ABOVE RUNS, at a different task key and with nothing else
    moved, so the three zeroes below are the KEY's and cannot be read as the anchor's.

    It does not SHORTEN the lookup's history, it ERASES it -- 4, 3 and 1 become 0, 0 and 0,
    because the lookup's only `dq_gate_batch` run postdates all three of its incidents. The
    three it destroys are exactly the three whose quarantine evidence is already gone (0.5).

    THE OTHER EIGHT ARE ASSERTED UNCHANGED, and that half is why nobody notices: the defect
    is invisible on 8 of 11 incidents, on the six jobs that never ran under the old name."""
    live, = live_gate_spellings()
    naive = {batch: _naive(gate_probe, _HAND_QUERY_WITH_IDENTITY, batch,
                           "prior_executions", gate=live)
             for batch in _INCIDENTS}
    right = {batch: _reading(gate_probe, batch)["prior_executions"] for batch in _INCIDENTS}

    assert naive == {batch: keyed for batch, _, _, _, keyed in _MEASURED}
    assert [naive[batch] for batch in _LOOKUP_INCIDENTS] == [0, 0, 0]
    assert [right[batch] for batch in _LOOKUP_INCIDENTS] == [4, 3, 1]
    untouched = [batch for batch in _INCIDENTS if batch not in _LOOKUP_INCIDENTS]
    assert all(naive[batch] == right[batch] for batch in untouched) and len(untouched) == 8


def test_the_terminal_state_reports_the_number_a_clean_workspace_would_report(gate_probe):
    """DEFECT 3, EXECUTED: `prior_incidents` taken off `check_bad_rows.result_state`.

    A condition task SUCCEEDS whether its answer is true or false, so the column that looks
    most like the answer is SUCCEEDED on all 29 runs -- the same reading a workspace with
    zero incidents would produce. The only signal that a gate found rejected rows is the
    PRESENCE of a `fail_on_dq` task run, which is what this module counts.

    The control is in the same test: the right answer is non-zero for SIX of the eleven --
    0.10's prior-incident column is 2, 2, 1, 1, 1, 1 and five zeroes -- so the zeroes below
    are a fact about the COLUMN and not about a fixture with no incidents in it."""
    off_state = {batch: _naive(gate_probe, _OFF_THE_TERMINAL_STATE, batch, "prior_incidents")
                 for batch in _INCIDENTS}
    right = {batch: _reading(gate_probe, batch)["prior_incidents"] for batch in _INCIDENTS}

    assert set(off_state.values()) == {0}
    assert right == {batch: gated for batch, _, gated, *_ in _MEASURED}
    assert len([batch for batch in _INCIDENTS if right[batch] > 0]) == 6


def test_a_prior_run_tied_to_the_anchor_is_kept_by_the_inclusive_bound(gate_probe):
    """`<=` AGAINST `<`, EXECUTED -- the one character that makes a predicate dead code.

    Self-exclusion in this module is the identity predicate's job and nothing else's.
    Narrow the bound to `<` and the anchor -- `MIN(started_at)` over the incident's OWN
    condition rows -- excludes every one of those rows by arithmetic, so
    `job_run_id <> :batch_id` can no longer remove anything that is still there. It becomes
    unreachable, and self-exclusion comes to rest on a property of the anchor that stops
    holding the moment the anchor moves by one task. THAT IS AN ARGUMENT ABOUT THE QUERY'S
    TEXT, and until this fixture carried a TIE no prior condition row shared the anchor's
    instant, so the two bounds selected the same set on every one of the eleven and one
    character could be changed with nothing in the file going red.

    Run 7 of estabelecimentos starts at run 8's instant, so `<=` counts it and `<` drops
    it. The other ten incidents agree under both bounds, which is the half that says why
    the character was invisible.

    WHAT THIS DOES NOT HOLD, AND THE WORD THAT LEFT ITS NAME. The anchor moving by one
    task -- the hazard the first paragraph names -- is a THIRD way to exclude the own run:
    anchor `own_gate` on the incident's own `fail_on_dq` row, narrow the bound to `<`, keep
    the identity predicate, and every test in this file was green. So the tied run was not
    kept ONLY by the inclusive bound, the name lost that word, and the anchor assertion
    below is what refuses that mutation here; `test_history_absence.py` refuses it one file
    away, on two readings that turn on which task the anchor is taken from. Held by nothing
    is the unreachability argument, a property of the anchor and not of any row -- and
    deleting the identity predicate while the bound is `<` reddens this test THROUGH THE
    TIE, saying nothing about the OWN run."""
    tied_run = _job_run_id(0, _TIED_PRIOR_RUN, _JOBS[0][2])
    instants = gate_probe.sql(
        f"SELECT job_run_id, MIN(started_at) AS at FROM {_table('telemetry')} "
        f"WHERE task_key = '{HISTORY_TASK_KEY}' AND job_run_id IN "
        f"('{tied_run}', '{_ESTAB_NEWEST}') GROUP BY job_run_id"
    ).collect()
    strict = {batch: _naive(gate_probe, _STRICTLY_BEFORE, batch, "prior_executions")
              for batch in _INCIDENTS}
    right = {batch: _reading(gate_probe, batch)["prior_executions"] for batch in _INCIDENTS}
    anchor = _reading(gate_probe, _ESTAB_NEWEST)["gate_started_at"]

    assert len(instants) == 2 and tied_run != _ESTAB_NEWEST
    assert len({row["at"] for row in instants}) == 1, "the fixture lost its tie"
    assert anchor == instants[0]["at"], "the anchor is the incident's OWN condition row"
    assert right[_ESTAB_NEWEST] == 7 and strict[_ESTAB_NEWEST] == 6
    others = [batch for batch in _INCIDENTS if batch != _ESTAB_NEWEST]
    assert len(others) == 10 and all(strict[batch] == right[batch] for batch in others)


# ----------------------------------------------------------------------------------
# The two folds, one constructed and one the corpus supplies.
# ----------------------------------------------------------------------------------


def test_a_second_gate_row_on_a_prior_run_is_not_a_second_prior_execution(gate_probe):
    """THE FOLD THIS CORPUS CANNOT SEE, driven by the one INVENTED row in the fixture.

    `check_bad_rows` runs once per job run on all seven jobs here, so every measured number
    in this file is reproduced by a query with no DISTINCT in it. The doubled row sits on an
    estabelecimentos run that is prior to all THREE of that job's incidents, so an unfolded
    count moves 2, 3 and 7 to 3, 4 and 8 -- and the module's own answer must not move at
    all, which is the first assertion."""
    estab = ("315230730740144", "187805471003061", _ESTAB_NEWEST)
    right = [_reading(gate_probe, batch)["prior_executions"] for batch in estab]
    unfolded = [_naive(gate_probe, _NO_FOLD, batch, "prior_executions") for batch in estab]

    assert right == [2, 3, 7]
    assert unfolded == [3, 4, 8]
    elsewhere = "592660596679630"
    assert _naive(gate_probe, _NO_FOLD, elsewhere, "prior_executions") == 2, (
        "the doubled row is on ONE job, so the unfolded count must agree everywhere else"
    )


def test_the_retry_attempts_do_not_double_the_prior_incident_count(gate_probe):
    """THE FOLD THIS CORPUS DOES SUPPLY, which is the half worth separating from the above.

    `fail_on_dq` carries TWO task runs per incident here -- 22 rows over 11 job runs,
    `max_retries: 0` failing to prevent a retry -- so the `gated_runs` leg is exercised by
    the measured data alone. Without its DISTINCT, the LEFT JOIN fans out and BOTH numbers
    move: the newest estabelecimentos incident has two prior incidents among seven prior
    runs, and the unfolded spelling reports four among nine."""
    sql = _UNFOLDED_INCIDENTS.format(
        view=_table("telemetry"), gate=HISTORY_TASK_KEY, incident=DQ_GATE_TASK_KEY
    )
    fanned = gate_probe.sql(sql, args={"batch_id": _ESTAB_NEWEST}).collect()[0]
    reading = _reading(gate_probe, _ESTAB_NEWEST)

    assert reading["prior_incidents"] == 2 and reading["prior_executions"] == 7
    assert fanned["prior_incidents"] == 4, "the two prior incidents, each counted twice"
    assert fanned["prior_executions"] == 9, "and the fan-out reaches the other number too"


# ----------------------------------------------------------------------------------
# The reading: four words, and the number beside every one of them.
# ----------------------------------------------------------------------------------


def test_ten_of_eleven_are_short_of_n_and_two_of_those_have_no_prior_execution(gate_probe):
    """PREDICTION 7, RUN -- and the module splits the ten that prediction names in two.

    At N = 5 exactly one incident of the eleven has a full window. The other ten are short,
    and the two with NOTHING to compare against get their own word rather than being folded
    into the eight that have something: `no_prior_execution` is asserted here on REAL
    incidents of REAL tables (socios and empresas, each that job's first gate run ever), as
    is `insufficient_history` -- which the plan requires be exercised on a real table rather
    than a contrived one, and which ten of the eleven qualify for.

    THE COUNT IS ON EVERY ROW WHATEVER THE WORD IS, which is the property the whole module
    exists for: a reading is never the only thing a reader has."""
    readings = {batch: _reading(gate_probe, batch) for batch in _INCIDENTS}
    words = {batch: row["history"] for batch, row in readings.items()}

    assert len([b for b, word in words.items() if word != HISTORY_COMPLETE]) == 10
    assert words[_ESTAB_NEWEST] == HISTORY_COMPLETE
    assert [b for b, word in words.items() if word == NO_PRIOR_EXECUTION] == [
        "1121645114029617", "321750543973966"
    ]
    assert len([b for b, word in words.items() if word == INSUFFICIENT_HISTORY]) == 8
    assert words["184706631093131"] == INSUFFICIENT_HISTORY
    assert all(row["prior_executions"] is not None for row in readings.values())
    assert all(row["executions_requested"] == N_EXECUTIONS for row in readings.values())


def test_the_window_is_reached_at_n_and_not_only_above_it(gate_probe):
    """The boundary, which no incident in this corpus sits on at N = 5.

    The lookup incident with FOUR prior runs is complete at N = 4 and short at N = 5, so
    `>=` and `>` are told apart here rather than by reading the CASE. Nothing else in this
    file can do it: the corpus's counts skip five entirely."""
    batch = "184706631093131"
    assert _reading(gate_probe, batch, executions=4)["history"] == HISTORY_COMPLETE
    assert _reading(gate_probe, batch, executions=5)["history"] == INSUFFICIENT_HISTORY
    assert _reading(gate_probe, batch, executions=4)["executions_requested"] == 4
    assert _reading(gate_probe, batch, executions=4)["prior_executions"] == 4


def test_a_batch_with_no_gate_run_reads_absent_and_not_complete(gate_probe):
    """The fourth word, and the ladder arm whose absence is the worst failure in the file.

    A batch the telemetry has no `check_bad_rows` run for has no anchor, so there is nothing
    to count up to: the counts are NULL rather than 0, because 0 is what a measurement says
    and this is not one. And with the first ladder arm removed the row does not fall through
    to another absence word -- `NULL = 0` and `NULL < 5` are both NULL in SQL, so it reaches
    the ELSE and reports `history_complete`, the most reassuring word on the list, for the
    batch nothing could be read about.

    THE CONTROL IS IN THE SAME TEST: a real batch through the same statement returns a
    number and a different word, so the NULLs are about the BATCH and not about the query."""
    absent = _reading(gate_probe, _BATCH_WITH_NO_GATE_RUN)
    present = _reading(gate_probe, _ESTAB_NEWEST)

    assert absent["history"] == GATE_RUN_ABSENT
    assert absent["prior_executions"] is None and absent["prior_incidents"] is None
    assert absent["job_id"] is None and absent["gate_started_at"] is None
    assert absent["batch_id"] == _BATCH_WITH_NO_GATE_RUN, "the row is about what was asked"
    assert present["history"] == HISTORY_COMPLETE and present["prior_executions"] == 7


def test_the_statement_publishes_the_counts_side_by_side_and_never_a_ratio(gate_probe):
    """What one reading IS. The column list, asserted, over a batch and over an absent one.

    Two numbers and no third: a rate would divide by a denominator that is 0 for two of the
    eleven incidents, and T3 refused the same shape in the severity ladder. The empty-batch
    row is asserted to carry the SAME columns, so the absence path cannot quietly project
    something else."""
    frame = gate_probe.sql(
        history_sql(view=_table("telemetry")), args={"batch_id": _ESTAB_NEWEST}
    )
    absent = gate_probe.sql(
        history_sql(view=_table("telemetry")), args={"batch_id": _BATCH_WITH_NO_GATE_RUN}
    )

    assert tuple(frame.columns) == _HISTORY_COLUMNS
    assert tuple(absent.columns) == _HISTORY_COLUMNS
    assert len(frame.collect()) == 1 and len(absent.collect()) == 1


def test_a_prior_run_whose_job_name_aged_out_is_kept_by_the_id_and_lost_by_the_name(
    gate_probe,
):
    """DECISION 3, DRIVEN ON THE SHAPE THIS CORPUS DOES NOT CONTAIN.

    Measured 2026-08-25: seven job ids, one name each, and no `check_bad_rows` run with a
    NULL name -- so both columns work TODAY, which is a property of this corpus on this date
    and not a guarantee. `opl.dataops.telemetry` KEEPS task runs whose job has aged out of
    `system.lakeflow.jobs`, and those carry a live id and a NULL name; the fixture's one
    such run is prior to all three estabelecimentos incidents, and a name join drops it
    because NULL never equals NULL.

    The name-keyed answer is right for every OTHER job, which is the half that makes this
    silent: one aged-out job run shortens one job's history and nothing else moves."""
    estab = ("315230730740144", "187805471003061", _ESTAB_NEWEST)
    by_id = [_reading(gate_probe, batch)["prior_executions"] for batch in estab]
    by_name = [_naive(gate_probe, _KEYED_ON_THE_NAME, batch, "prior_executions")
               for batch in estab]

    assert by_id == [2, 3, 7]
    assert by_name == [1, 2, 6], "the aged-out run is invisible to a name join"
    elsewhere = "409962018634322"
    assert _naive(gate_probe, _KEYED_ON_THE_NAME, elsewhere, "prior_executions") == 3
    assert _reading(gate_probe, elsewhere)["prior_executions"] == 3
