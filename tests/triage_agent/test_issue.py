# tests/triage_agent/test_issue.py
"""THE ONLY THING HOLDING THE PAYLOAD'S FIELD NAMES TO THE SQL THAT PRODUCES THEM.

THIS FILE STARTS SPARK. The three other `test_issue*` files do not, and the seam was chosen
before any of them was written: `issue.py`'s field names are string literals that no import
connects to `severity_sql`'s `AS severity_rank`, because a SQL alias is not a Python object.
Only a run of the shipped statements over real tables can hold the two equal, and that is
what happens here. A rename in another module of this package fails THIS file, and the
failure it prevents is a published issue with a blank where the grade goes.

AND IT RENDERS BODIES FROM ROWS THE STATEMENTS ACTUALLY RETURNED, which is the other half:
the no-JVM files drive the renderer from constructed records, so nothing there can tell
whether a real result row even carries the fields those records are made of.

THE TAINT SWEEP IS THIS FILE'S SECOND SUBJECT AND IT IS ONE ARM OF A PAIR. Every value in
`conftest.py`'s corpus carries a sentinel, and two columns declared personal carry a token
of their own, so "did a value escape into the issue body" is a question about the OUTPUT
rather than about the code that built it. THE BODY IS THE FIFTH PUBLISHABLE ARTEFACT in this
package -- after the census, the row shapes, the reconciliation and the graded row -- and it
is the only one a stranger reads without running anything.

  * THIS ARM sees a leak in any spelling, including a `SELECT *`, and ONLY where the leaked
    text still carries a sentinel. It cannot see a value transformed away from one --
    `SUBSTR(nome_socio_razao_social, 1, 3)` drops the sentinel and publishes three
    characters of a real partner's name.
  * `test_issue_report.py` IS THE OTHER ARM. It counts declared-personal column NAMES in the
    rendered body, needs no session, and catches exactly that transform because the name is
    still spelled. It is blind to every leak that never spells the name -- a raw value
    pasted into a field -- which is this arm's whole coverage.

Each arm's only cover is the other's measured blind spot, which is the pairing T2 and T3
established and this file inherits rather than restates.

THE TELEMETRY FIXTURE HERE IS THE SMALLEST SHAPE THAT CARRIES THE THREE THINGS THE PAYLOAD
NEEDS FROM IT, and it is NOT a re-measurement of `docs/f6-run-evidence.md` 0.10. T4 owns the
prior-execution counts, over a fixture of 29 job runs it built for that purpose; what this
one has to produce is a non-NULL count, a zero and an absence, so that the payload's history
fields are exercised in all three states against the shipped query rather than against a
dict a test typed. Its job names carry an INVENTED development prefix, which is the point of
the last test in this file.
"""
from __future__ import annotations

import pytest

from opl.bronze.promote import BATCH_COLUMN
from opl.bronze.registry import table_spec
from opl.dataops.telemetry import TASK_TELEMETRY_VIEW
from opl.triage_agent.evidence import quarantine_census_sql
from opl.triage_agent.history import (
    GATE_RUN_ABSENT,
    HISTORY_TASK_KEY,
    INSUFFICIENT_HISTORY,
    NO_PRIOR_EXECUTION,
    history_sql,
)
from opl.triage_agent.incidents import DQ_GATE_TASK_KEY, incident_feed_sql
from opl.triage_agent.issue import (
    CENSUS_FACTS,
    HISTORY_FACTS,
    INCIDENT_FACTS,
    SEVERITY_FACTS,
    Provenance,
    triage_issue,
)
from opl.triage_agent.report import render_body, render_title
from opl.triage_agent.severity import severity_sql

from .conftest import (
    _CONFIG,
    _EMPRESAS_BATCHES,
    _LOOKUP_BATCHES,
    _MATRIX_BATCH,
    _PAYMENTS_BATCH,
    _PERSONAL_TOKENS,
    _SENTINEL,
    _TAINT_SWEEP,
    _run,
)

_SCHEMA = "opl_issue_probe"
_TELEMETRY = f"spark_catalog.{_SCHEMA}.{TASK_TELEMETRY_VIEW}"

# INVENTED, and it must not be a real one: `databricks.yml` deploys in development mode,
# which prefixes every job name with `[dev <operator>] `, and that token carries an
# operator's username. This fixture reproduces the SHAPE so that the strip can be observed;
# committing a real one is what CLAUDE.md forbids.
_DEV_PREFIX = "[dev fixture_operator] "

# The payments job's three gate runs and the empresas job's one. The two prior payments runs
# are the only quantity here, and they exist to make `prior_executions` non-NULL rather than
# to reproduce a measurement -- 0.10's eleven counts are T4's and are asserted there.
_PRIOR_PAYMENTS_RUNS = ("9000100000001", "9000100000002")
_EMPRESAS_BATCH = _EMPRESAS_BATCHES[0]

# A batch with no `check_bad_rows` row anywhere in this relation: the third history state,
# and the one whose counts must come back NULL rather than 0.
_BATCH_WITH_NO_GATE_RUN = _LOOKUP_BATCHES[0]

# The incident fact for the batch this file's telemetry deliberately does not hold.
# CONSTRUCTED, AND THE ABSENCE IS THE SUBJECT: T1's feed reads the same relation, so it
# returns no row for this batch either -- which is the state being driven. A quarantine
# keeps its `_batch_id` forever while the timeline ages out, so an incident with evidence and
# no gate run is a shape this workspace will reach on its own.
_CONSTRUCTED_INCIDENT = {
    "batch_id": _BATCH_WITH_NO_GATE_RUN, "source": "lookup", "job_name": None,
    "attempts": 2, "first_started_at": None, "result_states": ["FAILED", "FAILED"],
}

_PROVENANCE = Provenance(
    produced_by="pytest: the shipped statements over tests/triage_agent fixtures",
    statements=(("severity", "no statement id -- a local session"),),
    telemetry_view=_TELEMETRY,
)


def _task_row(job_run: str, job: str, job_id: str, task: str, attempt: int, minute: int) -> str:
    """One task run, as a VALUES leg. `started_at` orders runs and nothing else reads it."""
    return (
        f"('{job_run}', '{job_id}', '{_DEV_PREFIX}{job}', '{task}', {attempt}, "
        f"TIMESTAMP'2026-08-{10 + minute // 60:02d} {minute % 24:02d}:00:00', "
        f"TIMESTAMP'2026-08-{10 + minute // 60:02d} {minute % 24:02d}:30:00', 'FAILED')"
    )


def _gate_run(job_run: str, job: str, job_id: str, minute: int, *, fired: bool) -> list[str]:
    """One job run: its condition task, and the failure task only where the gate fired.

    THE CONDITION TASK IS WHAT HISTORY COUNTS AND THE FAILURE TASK IS WHAT MAKES AN INCIDENT
    -- `history.py`'s identity, reproduced here rather than restated, because a fixture that
    put both on every run could not tell `prior_executions` from `prior_incidents`."""
    rows = [_task_row(job_run, job, job_id, HISTORY_TASK_KEY, 1, minute)]
    if fired:
        rows += [
            _task_row(job_run, job, job_id, DQ_GATE_TASK_KEY, attempt, minute + attempt)
            for attempt in (1, 2)
        ]
    return rows


def _telemetry_sql() -> str:
    """Four job runs over two jobs, as the columns the two statements read."""
    rows = [
        *_gate_run(_PRIOR_PAYMENTS_RUNS[0], "opl-bronze-payments", "900", 1, fired=False),
        *_gate_run(_PRIOR_PAYMENTS_RUNS[1], "opl-bronze-payments", "900", 5, fired=False),
        *_gate_run(_PAYMENTS_BATCH, "opl-bronze-payments", "900", 9, fired=True),
        *_gate_run(_EMPRESAS_BATCH, "opl-bronze-cnpj-empresas", "901", 13, fired=True),
    ]
    columns = ("job_run_id", "job_id", "job_name", "task_key", "attempt", "started_at",
               "ended_at", "result_state")
    return (
        f"SELECT * FROM VALUES\n  {',\n  '.join(rows)}\n  AS t({', '.join(columns)})"
    )


@pytest.fixture(scope="module")
def gate_probe(probe):
    """The telemetry this file reads, beside `conftest.py`'s quarantine corpus.

    IT TAKES `probe` RATHER THAN `spark`, so one session holds both: the payload needs the
    quarantine tables AND the timeline at once, which is the whole point of a record that
    fuses four statements. NOT NAMED `probe`, for the reason `test_history.py` records --
    three meanings on one name in one package."""
    probe.sql(f"CREATE DATABASE IF NOT EXISTS {_SCHEMA}")
    probe.sql(f"CREATE OR REPLACE VIEW {_TELEMETRY} AS {_telemetry_sql()}")
    yield probe
    probe.sql(f"DROP DATABASE IF EXISTS {_SCHEMA} CASCADE")


def _census_rows(spark, source: str, batch: str) -> list:
    """The census for one incident, from the shipped statement."""
    return _run(spark, quarantine_census_sql(table_spec(source), _CONFIG), batch)


def _facts(spark, source: str, batch: str) -> dict:
    """The four results for one incident, as the assembler takes them: every one a real row.

    `asDict()` IS THE CALLER'S LINE AND IS DELIBERATELY NOT HIDDEN inside `triage_issue`: a
    record built straight from a `Row` would take its field names from whatever the query
    returned, which is the property this file exists to hold."""
    feed = [row.asDict() for row in _run(spark, incident_feed_sql(_TELEMETRY), batch)
            if row["batch_id"] == batch]
    return {
        "incident": feed[0] if feed else {},
        "severity": _run(spark, severity_sql(source, _CONFIG), batch)[0].asDict(),
        "census": [row.asDict() for row in _census_rows(spark, source, batch)],
        "history": _run(spark, history_sql(_TELEMETRY), batch)[0].asDict(),
    }


# ----------------------------------------------------------------------------------
# The contract: the names this record reads are the names the statements emit.
# ----------------------------------------------------------------------------------


def test_every_field_the_payload_reads_is_a_column_the_shipped_statements_emit(gate_probe):
    """THE ONE TEST THAT CONNECTS TWO SPELLINGS NOTHING ELSE CONNECTS.

    `issue.py` names its fields in four tuples; `incidents.py`, `evidence.py`, `severity.py`
    and `history.py` name the same columns as SQL aliases inside f-strings. No import runs
    between them. A rename on either side is invisible to `ruff`, to every no-JVM test in
    this package and to a reader, and its consequence is a published issue with a blank
    where a grade goes -- so it is asserted here, against result schemas, on live-shaped
    fixtures.

    The subset direction is the one that matters: a statement may emit MORE than the record
    reads (it does -- `severity_sql` publishes `job_id`, `attempts` and more), and a record
    that read a column no statement emits is the failure."""
    facts = _facts(gate_probe, "payments", _PAYMENTS_BATCH)
    declared = {"incident": INCIDENT_FACTS, "severity": SEVERITY_FACTS,
                "history": HISTORY_FACTS}

    for name, keys in declared.items():
        missing = sorted(set(keys) - set(facts[name]))
        assert not missing, f"the {name} statement emits no {missing}"

    for row in facts["census"]:
        assert not sorted(set(CENSUS_FACTS) - set(row))


def test_a_whole_issue_assembles_from_four_real_results_and_renders(gate_probe):
    """END TO END, and every number in the body traceable to a row this session produced.

    The three constructed-fact files can each be true of a renderer that no statement can
    feed. This is the arm where the census sum check, the identity check and the field
    reader all run against results rather than against dictionaries -- and the counts
    asserted below are the fixture's own, so a statement that returned a plausible number
    for the wrong batch is visible."""
    drafted = triage_issue(**_facts(gate_probe, "payments", _PAYMENTS_BATCH),
                           provenance=_PROVENANCE)
    body = render_body(drafted)

    assert drafted.rejected_rows == 2000
    assert drafted.unaccounted == 8000
    assert drafted.prior_executions == len(_PRIOR_PAYMENTS_RUNS)
    assert drafted.history == INSUFFICIENT_HISTORY
    assert "2,000" in body and "8,000" in body
    assert f"FOUND: **{len(_PRIOR_PAYMENTS_RUNS)}**" in body
    assert render_title(drafted).startswith("[triage] payments batch")


def test_the_three_history_states_reach_the_body_from_the_shipped_query(gate_probe):
    """A count, a zero and an absence, each read out of `history_sql` rather than typed.

    THE ABSENCE IS THE ONE THAT MATTERS: a batch with no `check_bad_rows` row has no anchor,
    so the counts come back NULL and the body must publish no number at all. `0` there
    asserts "this table was never gated before" -- which is what the OTHER arm of this test
    says truthfully about empresas, and the two must not render alike."""
    counted = triage_issue(**_facts(gate_probe, "payments", _PAYMENTS_BATCH),
                           provenance=_PROVENANCE)
    zero = triage_issue(**_facts(gate_probe, "empresas", _EMPRESAS_BATCH),
                        provenance=_PROVENANCE)
    facts = _facts(gate_probe, "lookup", _BATCH_WITH_NO_GATE_RUN)
    absent = triage_issue(
        **{**facts, "incident": _CONSTRUCTED_INCIDENT},
        provenance=_PROVENANCE,
    )

    assert (counted.history, zero.history, absent.history) == (
        INSUFFICIENT_HISTORY, NO_PRIOR_EXECUTION, GATE_RUN_ABSENT)
    assert absent.prior_executions is None and zero.prior_executions == 0
    assert "FOUND: **not measured**" in render_body(absent)
    assert "FOUND: **0**" in render_body(zero)


# ----------------------------------------------------------------------------------
# The taint sweep: the body is the fifth publishable artefact.
# ----------------------------------------------------------------------------------


def _body_from_real_evidence(spark, source: str, batch: str, *, job: str | None = None) -> str:
    """A body whose census and grade are real rows for `(source, batch)`.

    THE INCIDENT AND HISTORY FACTS ARE CONSTRUCTED HERE AND THAT IS THE HONEST SCOPE OF THIS
    SWEEP: those two statements read the TIMELINE, which holds no quarantine value, so
    neither is a path from a rejected row to a public artefact. The two that read the
    quarantine -- the census and the graded row -- are the real thing for all twelve swept
    batches, and they are what a leak would come through."""
    return render_body(triage_issue(
        incident={"batch_id": batch, "source": source, "job_name": job or f"job-{source}",
                  "attempts": 2, "first_started_at": None, "result_states": ["FAILED"]},
        severity=_run(spark, severity_sql(source, _CONFIG), batch)[0].asDict(),
        census=[row.asDict() for row in _census_rows(spark, source, batch)],
        history={"batch_id": batch, "executions_requested": 5, "prior_executions": None,
                 "prior_incidents": None, "history": GATE_RUN_ABSENT},
        provenance=_PROVENANCE,
    ))


def test_no_issue_body_carries_a_row_value_and_the_reader_is_proven_to_work(probe):
    """THE PROPERTY THIS REPOSITORY'S PUBLICNESS RESTS ON, at the last layer.

    Every fixture value carries `_SENTINEL`, so any field that reached a body from a
    quarantine row puts one in the markdown. The sweep is `_TAINT_SWEEP` and not
    `_INCIDENTS` for `conftest.py`'s reason: the corpus's socios rows hold `''` in the most
    sensitive column, because being empty is what got them rejected, so the eleven alone
    cannot ask whether a PERSONAL value escaped.

    BOTH CONTROLS ARE IN THIS TEST. A taint check whose reader is broken reports clean over
    everything, so the same reader is pointed at a body doctored with a planted value and
    required to find it; and the personal tokens are required to be in the fixture before
    their absence from the body means anything."""
    for source, batch in _TAINT_SWEEP:
        body = _body_from_real_evidence(probe, source, batch)
        assert _SENTINEL not in body, f"the body for {source}/{batch} carries a row value"
        for column, token in _PERSONAL_TOKENS.items():
            assert token not in body, f"{column}'s value reached the issue body"

    quarantine = _CONFIG.table(table_spec("socios").quarantine)
    planted = str(_run(probe, f"SELECT * FROM {quarantine} WHERE {BATCH_COLUMN} = :batch_id",
                       _MATRIX_BATCH))
    for column, token in _PERSONAL_TOKENS.items():
        assert token in planted, (
            f"the fixture holds no {column} token in this batch, so the absences above are "
            "absences from the INPUT and would be green with every redaction deleted"
        )

    doctored = _body_from_real_evidence(
        probe, "socios", _MATRIX_BATCH, job=f"job-{_PERSONAL_TOKENS['nome_do_representante']}"
    )
    assert _SENTINEL in doctored and _PERSONAL_TOKENS["nome_do_representante"] in doctored, (
        "the reader misses a planted value that DID reach a field, so its silence above "
        "says nothing"
    )


def test_the_body_carries_no_operator_identifier_because_the_feed_strips_it(gate_probe):
    """`databricks.yml` deploys in development mode, so every runtime job name is
    `[dev <operator>] <name>` and that token is a person's username. The feed strips it in
    SQL; this file's fixture puts an invented one back so the strip can be observed at the
    END of the chain rather than at the start.

    THE FIXTURE'S OWN PREFIX IS ASSERTED TO BE THERE FIRST, or this is an absence check over
    a string nothing planted -- which is the shape that left a personal-column assertion in
    this package green for two passes."""
    assert _DEV_PREFIX in _telemetry_sql(), "the fixture plants no prefix to strip"

    drafted = triage_issue(**_facts(gate_probe, "payments", _PAYMENTS_BATCH),
                           provenance=_PROVENANCE)
    body = render_body(drafted)

    assert drafted.job_name == "opl-bronze-payments"
    assert "fixture_operator" not in body
    assert "[dev" not in body
