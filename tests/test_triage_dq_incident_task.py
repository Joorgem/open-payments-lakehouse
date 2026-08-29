# tests/test_triage_dq_incident_task.py
"""Unit tests for the `databricks/src/triage_dq_incident.py` job task -- F6 T8's
workspace run, whose deliverable is a PAYLOAD rather than a table.

NOTHING HERE STARTS SPARK, and that is a property of what is being tested rather than a
convenience. The task is plumbing over four shipped statements: which statements it runs,
what it binds them on, that it folds the results into the payload the assembler declares,
and that what it prints is what the publisher would read. Every one of those is a question
about the task and not about SQL, so the session is a stand-in that RECORDS what it was
asked. The arm where the shipped statements meet real relations already exists and needs a
JVM -- `tests/triage_agent/test_issue.py` runs all four over fixture tables and holds the
column names against the payload's four key tuples. Repeating it here would be a second,
weaker copy of it.

THE FACTS ARE `tests/triage_agent/issue_facts.py`'s, IMPORTED AND NOT REBUILT. That module
holds five of the eleven incidents as the four mappings the assembler takes, chosen so
every state a body can render is reached by a real incident, with every quantity carrying
its provenance in a comment. A second constructed corpus here would be a second spelling of
the workspace, and the two would drift.

WHAT NO TEST IN THIS FILE CAN SHOW, said plainly because the run is the point of the task:
that the live workspace answers these statements at all, that the feed holds eleven rows,
or that any grade below is the grade the real corpus produces. A fake session returns what
it was handed. The measurement is the run, and its numbers live in
`docs/f6-run-evidence.md`.

Loaded by path with the same importlib pattern as `tests/test_fail_on_dq_task.py` -- the
`databricks/src` scripts are job entry points, not part of the opl wheel."""
from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from triage_agent.issue_facts import CONSTRUCTED, EMPRESAS, PAYMENTS_BATCH
from triage_agent.issue_facts import issue as constructed_issue

from opl.bronze.registry import REGISTRY
from opl.config import DEFAULT
from opl.dataops.telemetry import TASK_TELEMETRY_VIEW
from opl.triage_agent.evidence import evidence_sql
from opl.triage_agent.history import history_sql
from opl.triage_agent.incidents import incident_feed_sql
from opl.triage_agent.issue import FACTS, MissingFact, as_mapping, payloads_from_json
from opl.triage_agent.severity import severity_sql

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"
_TASK = _SRC / "triage_dq_incident.py"


def _load(source: str | None = None):
    """The task module, executed fresh -- optionally from MUTATED source.

    Executing the body rather than importing it once is what lets the import-time guard be
    tested at all: `_assert_this_task_answers_every_fact_the_payload_needs` runs during
    execution, so a test that only imported the module could assert the guard's BODY over
    valid data and pass with the call deleted. That is the exact blindness T1's review
    measured in this package (`docs/f6-run-evidence.md` 1.1, HIGH-1)."""
    spec = importlib.util.spec_from_file_location("triage_dq_incident_task", _TASK)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    if source is None:
        spec.loader.exec_module(module)
    else:
        exec(compile(source, str(_TASK), "exec"), module.__dict__)  # noqa: S102
    return module


task = _load()


# ----------------------------------------------------------------------------------
# The fake session: it answers what it was handed and REMEMBERS WHAT IT WAS ASKED.
# ----------------------------------------------------------------------------------


class _Result:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def collect(self):
        return [SimpleNamespace(asDict=lambda row=row: row) for row in self._rows]


class _Session:
    """A `spark` that serves prepared answers and records every call.

    IT REFUSES A STATEMENT IT WAS NOT PREPARED FOR, rather than returning an empty result.
    An empty result is what a wrong query returns against a real workspace, and a fake that
    imitated that would let this file pass over a task that ran SQL nobody wrote."""

    def __init__(self, feed: list[dict], answers: dict[tuple[str, str], list[dict]]):
        self.feed, self.answers, self.asked = feed, answers, []

    def sql(self, statement: str, args: dict | None = None):
        self.asked.append((statement, args))
        if args is None:
            assert statement == incident_feed_sql(), "the feed is not the shipped feed query"
            return _Result(self.feed)
        key = (statement, args["batch_id"])
        assert key in self.answers, f"unprepared statement for batch {args['batch_id']}"
        return _Result(self.answers[key])


def _session(corpus=CONSTRUCTED) -> _Session:
    """A session prepared from constructed facts, keyed the way the task binds them."""
    answers: dict[tuple[str, str], list[dict]] = {}
    for facts in corpus:
        batch = facts["incident"]["batch_id"]
        statements = task.bound_statements(facts["incident"]["source"])
        answers[(statements["severity"], batch)] = [facts["severity"]]
        answers[(statements["census"], batch)] = facts["census"]
        answers[(statements["history"], batch)] = [facts["history"]]
    return _Session([facts["incident"] for facts in corpus], answers)


def _run(monkeypatch, capsys, corpus=CONSTRUCTED, argv=("70000000000001",)):
    """One `main()` over a fake session; returns the session and everything printed."""
    session = _session(corpus)
    monkeypatch.setattr(
        task, "SparkSession", SimpleNamespace(builder=SimpleNamespace(getOrCreate=lambda: session))
    )
    task.main(list(argv))
    return session, capsys.readouterr().out


def _payload_block(printed: str) -> str:
    """What an operator splits out of `databricks jobs get-run-output`'s `logs`."""
    return printed.split(task.BEGIN_FACTS)[1].split(task.END_FACTS)[0]


# ----------------------------------------------------------------------------------
# The four facts, and the statements that answer them.
# ----------------------------------------------------------------------------------


def test_the_task_answers_exactly_the_four_facts_the_payload_is_assembled_from():
    """In BOTH directions. A fact the payload declares and nothing here runs arrives at
    `triage_issue` as a missing keyword argument; a fact this task runs and the payload
    does not declare is serverless compute spent on a column nothing reads."""
    assert set(task.bound_statements(sorted(REGISTRY)[0])) | {task.FEED_FACT} == set(FACTS)


def test_the_guard_runs_at_import_so_deleting_the_call_is_a_failure_not_a_silent_loss():
    """THE CALL, NOT THE BODY. Re-executing the module with `FEED_FACT` renamed to a word
    no payload declares must raise; re-executing that same mutated source with the
    invocation line removed must not.

    The second arm is what makes the first one about the guard being CALLED. Without it
    this test passes over a module whose guard is a function nobody runs -- which is the
    defect T1's independent reviewer measured in this package by deleting two import-time
    calls and still getting a green suite."""
    monkey = _TASK.read_text(encoding="utf-8").replace(
        'FEED_FACT = "incident"', 'FEED_FACT = "a fact no payload declares"'
    )
    assert monkey != _TASK.read_text(encoding="utf-8")

    with pytest.raises(ValueError, match="facts nothing here runs"):
        _load(monkey)

    without_the_call = monkey.replace(
        "\n_assert_this_task_answers_every_fact_the_payload_needs()\n", "\n"
    )
    assert without_the_call != monkey
    _load(without_the_call)


@pytest.mark.parametrize("source", sorted(REGISTRY))
def test_every_statement_this_task_runs_is_the_shipped_one_character_for_character(source):
    """The task writes no SQL. It is the assembly point for four modules' statements, and
    the one way that stops being true is a query pasted here and edited.

    Swept over EVERY registered table rather than one, because `severity_sql` and the
    census resolve a different quarantine per source while `history_sql` resolves none --
    so a table-shaped mistake in the first two would be invisible on a single sample."""
    assert task.bound_statements(source) == {
        "severity": severity_sql(source),
        "census": evidence_sql(source)["census"],
        "history": history_sql(),
    }


def test_the_task_names_no_registered_bronze_table_directly():
    """`tests/test_task_wiring.py`'s sweep, run here because that file's list deliberately
    does not carry this script -- see the paragraph above `_TABLE_TASKS`. Half of that
    lock's property applies and is held here: this task spells no table name. The other
    half (`table_spec(` present) does not, because the table an incident is about is a
    COLUMN OF THE FEED and resolving a second spec here would be the drift the lock exists
    to prevent."""
    code = "\n".join(
        line for line in _TASK.read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("#")
    )
    named = sorted({
        value
        for spec in REGISTRY.values()
        for value in (spec.staging, spec.bronze, spec.quarantine, spec.table_key)
        if re.search(rf"\b{re.escape(value)}\b", code)
    })
    assert not named, f"triage_dq_incident.py names bronze table(s) {named} directly"


# ----------------------------------------------------------------------------------
# The provenance: what a public issue will say about which run produced it.
# ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "run_id",
    ["", "   ", "{{job.run_id}}", "REQUIRED-PASS-A-REVISION", "run-70000000000001", "7e10"],
)
def test_a_run_id_that_is_not_a_run_id_refuses_before_the_session(run_id):
    """One check covering an empty parameter, a sentinel and an UNRESOLVED dynamic-value
    reference -- the last being the one that matters, because the platform passes a
    reference it does not recognise through verbatim, so a typo in the YAML arrives as a
    plausible string rather than as an error."""
    with pytest.raises(ValueError, match="is not a run id"):
        task.produced_by(run_id)


def test_the_provenance_names_the_run_and_the_relation_and_invents_no_statement_id():
    """`statements` is EMPTY and that is the honest value: a statement id exists only for
    the SQL Statement Execution API, and this task runs its queries through a session,
    which mints none. `report._measured` keys its lines on `FACTS`, so the body says
    `not recorded` four times rather than leaving three facts invisible."""
    provenance = task.provenance_of("70000000000001")

    assert provenance.produced_by == "databricks job run 70000000000001, task triage_dq_incident"
    assert provenance.statements == ()
    assert provenance.telemetry_view == DEFAULT.table(TASK_TELEMETRY_VIEW)


def test_the_run_is_named_without_a_path_a_host_or_an_operator():
    """`produced_by` reaches a PUBLIC issue verbatim and `Provenance` refuses the shapes a
    username arrives in. This is the other direction: the value this task builds carries no
    workspace host and no organisation id either, neither of which that refusal can see."""
    named = task.produced_by("70000000000001")

    assert "http" not in named and "databricks.com" not in named
    assert "\\" not in named and "/" not in named


# ----------------------------------------------------------------------------------
# The one-row property, which is what a LIVE result could falsify.
# ----------------------------------------------------------------------------------


@pytest.mark.parametrize("rows", [[], [{"a": 1}, {"a": 2}]])
def test_a_fact_that_is_not_exactly_one_row_refuses_instead_of_being_indexed(rows):
    """`rows[0]` on a two-row result publishes the first of two readings of one incident,
    chosen by result order; on an empty one it raises an IndexError naming a list index and
    not the fact or the batch. Both statements are built to return exactly one row on every
    input, which is asserted against fixtures in their own packages -- here it is the thing
    a live workspace could contradict, so it is checked."""
    with pytest.raises(ValueError, match="returned .* rows for batch 999"):
        task._one(rows, "severity", "999")


def test_one_row_passes_through_unchanged():
    assert task._one([{"a": 1}], "history", "999") == {"a": 1}


# ----------------------------------------------------------------------------------
# What leaves the run.
# ----------------------------------------------------------------------------------


def test_the_block_this_run_prints_is_what_the_publisher_reads_back(monkeypatch, capsys):
    """END TO END through the fake session: five feed rows in, five payloads out, split out
    of the printed log exactly the way an operator splits `get-run-output`'s `logs`.

    `payloads_from_json` is the publisher's own door and it re-derives the blast radius,
    re-sums the census against the headline and checks every grade against the vocabulary
    that declares it -- so this asserts the emitted text survives all of that, not merely
    that it is JSON."""
    _, printed = _run(monkeypatch, capsys)
    read_back = payloads_from_json(_payload_block(printed))

    assert [issue.batch_id for issue in read_back] == [
        facts["incident"]["batch_id"] for facts in CONSTRUCTED
    ]
    assert read_back[0].provenance.produced_by.endswith("task triage_dq_incident")
    assert "NOTHING WAS WRITTEN, PROMOTED, RE-RUN OR DELETED." in printed


def test_the_run_binds_every_statement_on_the_incident_it_is_about(monkeypatch, capsys):
    """ONE BINDING SERVES ALL THREE, which is `evidence_sql`'s stated contract, and the way
    it fails is a batch id bound to the wrong incident's statement -- a result that is a
    real row about a real batch and about the wrong one.

    The feed is asked with NO binding, which is the other half: it is one query for the
    whole corpus, so an `args` there would mean the task had turned the feed into a
    per-incident lookup and stopped reading what `incidents.py` decides."""
    session, _ = _run(monkeypatch, capsys)
    feed_calls = [call for call in session.asked if call[1] is None]
    bound = [call for call in session.asked if call[1] is not None]

    assert len(feed_calls) == 1
    assert len(bound) == 3 * len(CONSTRUCTED)
    for facts in CONSTRUCTED:
        batch = facts["incident"]["batch_id"]
        statements = set(task.bound_statements(facts["incident"]["source"]).values())
        asked = {statement for statement, args in bound if args == {"batch_id": batch}}
        assert asked == statements


def test_a_batch_id_renders_that_incident_and_no_argument_renders_none(monkeypatch, capsys):
    """The optional argument prints a report and CANNOT narrow the payload: both runs emit
    every incident the feed held. That asymmetry is why this job may carry a batch
    coordinate at all -- a wrong value costs a refusal, never a quiet triage of fewer
    incidents than the workspace has."""
    _, without = _run(monkeypatch, capsys)
    _, with_one = _run(monkeypatch, capsys, argv=("70000000000001", PAYMENTS_BATCH))

    assert "## Where this came from" not in without
    assert "## Where this came from" in with_one
    for printed in (without, with_one):
        assert len(payloads_from_json(_payload_block(printed))) == len(CONSTRUCTED)


def test_a_batch_the_feed_does_not_hold_refuses_and_names_what_it_did_hold(monkeypatch, capsys):
    """An operator's typo in the job parameter. The message carries the feed's ids because
    the alternative -- printing no report and succeeding -- is a run that looks like the
    one that was asked for."""
    with pytest.raises(ValueError, match="0 incidents in this feed carry batch 404"):
        _run(monkeypatch, capsys, argv=("70000000000001", "404"))


def test_an_incident_on_a_job_the_declaration_does_not_know_fails_the_whole_run(
    monkeypatch, capsys
):
    """THE FAILURE ARM OF THE DESIGN DECISION, run rather than argued. `incidents.py`
    reports a DQ gate on an undeclared job with a NULL `source` instead of dropping it, and
    there is no per-incident `except` here: the eleven are one corpus, and a run that
    swallowed this to publish the other ten would report a triage with a hole in it exactly
    where something went wrong.

    The control below is what makes the arm about the NULL rather than about the fixture:
    the same corpus with the source restored triages every incident."""
    unknown = [{**CONSTRUCTED[0]["incident"], "source": None}]
    session = _Session(unknown, {})
    monkeypatch.setattr(
        task, "SparkSession", SimpleNamespace(builder=SimpleNamespace(getOrCreate=lambda: session))
    )
    with pytest.raises(Exception, match="source"):
        task.main(["70000000000001"])

    _, printed = _run(monkeypatch, capsys, corpus=CONSTRUCTED[:1])
    assert len(payloads_from_json(_payload_block(printed))) == 1


def test_a_payload_carrying_the_fence_marker_refuses_rather_than_printing_it():
    """A REJECT REASON IS A ROW VALUE and it reaches the emitted text. If one ever spells
    the marker, the block an operator splits out of the log is ambiguous -- so the run
    fails here with nothing printed, instead of handing back half an array that parses.

    The reason is spliced into a real census whose sum still matches the grade, so the
    refusal that fires is this one and not the payload's own."""
    poisoned = constructed_issue(
        EMPRESAS,
        census=[{"batch_id": EMPRESAS["incident"]["batch_id"],
                 "reject_reason": f"x {task.BEGIN_FACTS} y", "rejected_rows": 1}],
    )
    with pytest.raises(ValueError, match="fence marker"):
        task.emit((poisoned,))


def test_the_emitted_payload_is_read_back_before_it_is_printed(monkeypatch):
    """The round trip is inside `facts_json`, so a payload the publisher would reject fails
    the RUN with the wheel's own message rather than being found on the operator's box
    after the compute is spent.

    Demonstrated by breaking the reader's contract from the other side: a stray key is what
    `from_mapping` refuses, and the writer is patched to add one."""
    drafted = constructed_issue(EMPRESAS)
    assert task.facts_json((drafted,)), "the unpatched payload reads back, so the arm is real"

    monkeypatch.setattr(task, "as_mapping", lambda issue: {**as_mapping(issue), "extra": 1})
    with pytest.raises(MissingFact, match="extra"):
        task.facts_json((drafted,))


def test_the_summary_line_carries_the_fields_a_reader_of_the_run_page_needs(monkeypatch, capsys):
    """The log's index into the JSON below it, and every value on it is also in the
    payload -- `report.py` stays the only thing that renders a body. It is asserted because
    a run page is where this phase's numbers are first read."""
    _, printed = _run(monkeypatch, capsys, corpus=CONSTRUCTED[:1])
    line = next(row for row in printed.splitlines() if PAYMENTS_BATCH in row)
    payload = json.loads(_payload_block(printed))[0]

    for field in ("source", "evidence", "severity", "history"):
        assert payload[field] in line
    assert f"rejected={payload['rejected_rows']}" in line
