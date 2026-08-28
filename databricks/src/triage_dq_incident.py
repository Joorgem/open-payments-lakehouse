# databricks/src/triage_dq_incident.py
"""Job task: triage every DQ incident this workspace holds, and CHANGE NOTHING.

WHAT IT IS. The shipped `opl.triage_agent` path, run on serverless against the real
corpus. It reads F4's task-telemetry view for the incident feed, then for each incident
runs the three statements the payload is assembled from -- the graded row, the quarantine
census and the comparison baseline -- and hands the four results to `triage_issue`. What
leaves the run is the issue as DATA: `as_mapping` for every incident, as one JSON array
fenced between two marker lines in this task's own stdout.

IT WRITES NOTHING AND THAT IS NOT A STYLE CHOICE. Plan section 1.3 and ADR 0018
Decision 3: the agent reads, ranks and drafts, and a person decides. The operational half
is that `max_retries: 0` does not prevent a retry -- this repository has measured it three
times, and the corpus this task reads is eleven incidents wearing twenty-two rows BECAUSE
OF IT. Anything that writes has to be idempotent or fail before its first write; a task
that writes nothing is neither, it is simply unable to double anything on a second attempt.
So a retry of this task re-reads the same relations and re-prints the same payload, and the
worst a second attempt can cost is compute.

WHERE THE PAYLOAD GOES, AND THE ALTERNATIVES ARE NAMED BECAUSE ONE OF THEM LOOKS OBVIOUS.
The publisher lives under `scripts/`, which `pyproject.toml` does not package, so nothing
running in this workspace can import it -- that is a credential boundary and the reason a
GitHub PAT never has to exist in a Databricks secret scope. The consequence is that a
payload produced here has to reach the operator's box by some route that is not an import,
and the route is THIS RUN'S OWN STDOUT, read back with `databricks jobs get-run-output
--run-id <task run id>`, which returns a `logs` string and a `logs_truncated` boolean.

  MEASURED BEFORE THIS TASK WAS WRITTEN, on job run 445103558019914 (`opl-smoke`, task run
  75012715445096, a serverless `spark_python_task` twenty-six days old): the API returned
  this project's own `opl_smoke OK | ...` line with `logs_truncated: false`. So the route is
  a measurement rather than an assumption, and `logs_truncated` means the retrieval says
  when it failed instead of handing back a short file that parses.

  REJECTED: A FILE IN A UC VOLUME. It works and it is durable, and it is a WRITE -- state
  in the workspace, on a path a second attempt overwrites, inside the one package whose
  header says it never writes. It also needs a volume path constant that nothing else in
  this project reads. The payload is small and the operator fetches it minutes later, so
  durability buys nothing here that the run id does not already buy.
  REJECTED: `dbutils.jobs.taskValues.set`. It is a channel to DOWNSTREAM TASKS -- which is
  what `dq_gate_batch` uses it for, publishing `bad_row_count` to a condition task -- and
  nothing in this repository reads one from outside a run.
  REJECTED: a notebook task with `dbutils.notebook.exit(json)`. That moves the entry point
  out of `databricks/src/*.py`, which is the set two of this repository's sweeps read --
  the serverless-capability AST guard and the git-at-runtime ban. An artefact that escapes
  the guards to make its output easier to fetch is the wrong trade in this repository.

  WHAT REVERSES IT: `logs_truncated: true` -- a corpus large enough that the log is cut --
  or the platform ceasing to capture stdout for a serverless `spark_python_task`. Then the
  volume write is the fallback and the destination becomes a parameter of this task.

ONE BAD INCIDENT FAILS THE WHOLE RUN, DELIBERATELY. Every refusal `opl.triage_agent` makes
exists to stop a wrong artefact being published, and the eleven incidents are ONE corpus:
a run that swallowed a refusal to publish the other ten would be reporting a triage of the
workspace with a hole in it exactly where something went wrong. There is no per-incident
`except` here for that reason. The cost is a lost run, which is compute; the alternative
cost is a payload whose absences are invisible.

`produced_by` NAMES THE RUN BY ITS ID AND NOT BY ITS URL. `Provenance` documents the field
as "a Databricks job run URL, or the command a human typed", and it reaches a PUBLIC issue
verbatim. A run page URL carries this workspace's host and organisation id into that issue
and is useless to a stranger, who cannot open it; the run id is what a later reader with
this repository actually follows (`databricks jobs get-run <id>`), and statement ids expire
in about five days while run ids do not. So the id is the smaller true thing.

`statements` IS EMPTY, AND THE BODY WILL SAY `not recorded` FOUR TIMES. That field is
(fact, statement id) pairs, and a statement id exists only for the SQL Statement Execution
API. This task runs its queries through a Spark session, which mints no such id, so there
is nothing to record and nothing is invented. `report._measured` keys its lines on `FACTS`
precisely so an unrecorded fact says so rather than going missing.

argv: [job_run_id, batch_id]

`job_run_id` is `{{job.run_id}}`, required, and refused unless it is all digits -- which
refuses an empty parameter, a job-parameter sentinel and an unresolved dynamic-value
reference in one check, because Databricks passes a reference it does not recognise through
literally. `batch_id` is optional: given, the run ALSO prints that one incident's rendered
title and body, which is the report plan section 2 asks this task to print; absent, the run
emits the payloads and no markdown. There is no arm that renders eleven bodies."""
import json
import sys

from pyspark.sql import SparkSession

from opl.bronze.registry import REGISTRY
from opl.config import DEFAULT
from opl.dataops.telemetry import TASK_TELEMETRY_VIEW
from opl.triage_agent.evidence import evidence_sql
from opl.triage_agent.history import history_sql
from opl.triage_agent.incidents import incident_feed_sql
from opl.triage_agent.issue import (
    FACTS,
    Provenance,
    TriageIssue,
    as_mapping,
    payloads_from_json,
    triage_issue,
)
from opl.triage_agent.report import render_body, render_title
from opl.triage_agent.severity import severity_sql

# The task key this file is wired to, spelled once and printed into `produced_by` so a
# reader of a public issue can find the task inside the run it names.
TASK_KEY = "triage_dq_incident"

# THE FENCE. The retrieval is a text split over `logs`, so the markers have to be lines
# that cannot occur inside JSON a quarantine's reject reason could produce -- and `emit`
# refuses to print a payload containing either of them rather than trusting that sentence.
# They are constants so the operator's split and this task's print are one spelling; a
# reader with the run id needs nothing else to find the payload.
BEGIN_FACTS = ">>> OPL TRIAGE FACTS JSON BEGIN <<<"
END_FACTS = ">>> OPL TRIAGE FACTS JSON END <<<"

# The one fact of the four that is NOT a per-incident statement: the feed answers it for
# every incident at once, in one query, which is what makes "one record per incident" a
# property of a single result rather than of a loop.
FEED_FACT = "incident"


def produced_by(run_id: str) -> str:
    """The run this payload came out of, named without naming a person or a host.

    `isdigit()` IS THREE REFUSALS IN ONE CHECK and that is why it is spelled this way: an
    empty parameter, a sentinel default and an unresolved run-id reference are all
    non-numeric, and the last one matters most because Databricks passes a dynamic-value
    reference it does not recognise through VERBATIM -- so a typo in the YAML arrives here
    as a plausible-looking string, not as an error.

    IT MUST ALSO SURVIVE `issue._assert_the_run_is_named_without_naming_a_person`, which
    refuses a filesystem path and the bundle's development prefix. A digit string and two
    English words carry neither, which is a second reason not to build a URL here."""
    if not run_id.isdigit():
        raise ValueError(
            f"the job run id {run_id!r} is not a run id: this task is handed the run's own "
            "id and a value that is not all digits is an empty parameter, a sentinel, or a "
            "dynamic-value reference the platform passed through unresolved. `produced_by` "
            "reaches a public issue, so it is not built from a guess"
        )
    return f"databricks job run {run_id}, task {TASK_KEY}"


def provenance_of(run_id: str) -> Provenance:
    """What produced this payload, with the two things this run can honestly say.

    `telemetry_view` IS THE ONE RELATION THE WHEEL CANNOT DERIVE -- it is a seam on
    `incident_feed_sql` and `history_sql` -- so this task names the relation it actually
    pointed them at, which is the deployed F4 view its own `OplConfig` locates. It is
    labelled THE CALLER'S WORD in the body, correctly: nothing downstream can check it."""
    return Provenance(
        produced_by=produced_by(run_id),
        telemetry_view=DEFAULT.table(TASK_TELEMETRY_VIEW),
    )


def bound_statements(source: str) -> dict[str, str]:
    """The three statements bound on one incident, keyed by the fact each one answers.

    THE KEYS ARE `issue.FACTS`' OWN WORDS MINUS THE FEED'S, which the import-time guard at
    the foot of this file holds, so "which four results make an issue" has one spelling in
    this project rather than two. Every value is a shipped function's output taken whole:
    this task writes no SQL, and a test asserts each of these three is character-identical
    to what the module that owns it returns for the same source.

    NO `view=` IS PASSED ANYWHERE. Those seams exist so a test can point a statement at a
    fixture; a run that overrode them would be triaging something other than the workspace,
    which is the one thing this task exists to do."""
    return {
        "severity": severity_sql(source),
        "census": evidence_sql(source)["census"],
        "history": history_sql(),
    }


def _one(rows: list[dict], fact: str, batch_id: str) -> dict:
    """Exactly one row, or refuse naming which fact and which incident.

    `evidence.py` and `history.py` each build their statement so that it returns exactly one
    row on every input -- an ungrouped aggregate under a `LEFT JOIN ... ON true` -- and
    `severity.py` joins two such sides `ON true` so a violation shows up as EXTRA rows. That
    property is asserted in those packages against fixtures; here it is the thing a live
    result could falsify, so it is checked rather than indexed. `rows[0]` on a two-row
    result would publish the first of two readings of one incident, chosen by result
    order."""
    if len(rows) != 1:
        raise ValueError(
            f"the {fact} statement returned {len(rows)} rows for batch {batch_id} and this "
            "package is built on it returning exactly one. Two rows is one incident wearing "
            "two readings and picking either would be result order deciding; zero is a "
            "statement that did not run the way its module says it cannot fail to"
        )
    return rows[0]


def facts_for(spark: SparkSession, record: dict) -> dict:
    """The four facts for one incident: the feed's row, plus the three it binds.

    `asDict()` IS THIS CALLER'S LINE AND IS NOT HIDDEN INSIDE `triage_issue`, which that
    function's docstring says outright: a record built straight from a `Row` would take its
    field names from whatever the query happened to return, and the whole point of the
    payload's four key tuples is that the names are declared.

    ONE BINDING SERVES ALL THREE, which is why this loop needs no per-statement argument
    table: every statement in this package takes exactly `args={"batch_id": ...}`."""
    batch_id, source = str(record["batch_id"]), record["source"]
    args = {"batch_id": batch_id}
    read = {
        fact: [row.asDict() for row in spark.sql(statement, args=args).collect()]
        for fact, statement in bound_statements(source).items()
    }
    return {
        FEED_FACT: record,
        "severity": _one(read["severity"], "severity", batch_id),
        "census": read["census"],
        "history": _one(read["history"], "history", batch_id),
    }


def drafted(spark: SparkSession, provenance: Provenance) -> tuple[TriageIssue, ...]:
    """Every incident this workspace holds, as payloads, in the feed's own order.

    THE FEED IS READ ONCE AND IS THE ONLY THING THAT DECIDES WHAT AN INCIDENT IS -- that is
    `incidents.py`'s stated role and the reason nothing here filters, sorts or de-duplicates
    what it returns. A run that triaged a subset chosen here would be answering a different
    question from the one the feed answers, with no column saying so."""
    feed = [row.asDict() for row in spark.sql(incident_feed_sql()).collect()]
    return tuple(
        triage_issue(**facts_for(spark, record), provenance=provenance) for record in feed
    )


def facts_json(issues: tuple[TriageIssue, ...]) -> str:
    """The payloads as one JSON array -- the exact characters the publisher reads.

    ROUND-TRIPPED THROUGH `payloads_from_json` BEFORE IT IS PRINTED, and that is the check
    worth having: the file door refuses four things the assembler does not, re-derives the
    blast radius and re-sums the census against the headline. Doing it here means a payload
    the publisher would reject fails THIS RUN, with the wheel's own message, instead of
    being found on the operator's box after the compute is spent."""
    text = json.dumps([as_mapping(issue) for issue in issues], indent=2, sort_keys=True)
    read_back = payloads_from_json(text)
    if [issue.batch_id for issue in read_back] != [issue.batch_id for issue in issues]:
        raise ValueError(
            "the payloads this run emitted do not read back as the payloads it drafted, so "
            "what the publisher would parse is not what was assembled"
        )
    return text


def emit(issues: tuple[TriageIssue, ...]) -> str:
    """The fenced block, or refuse rather than print a fence a reader cannot trust.

    A REJECT REASON IS A ROW VALUE and it reaches this text, so "the markers cannot occur
    inside the payload" is checked rather than asserted in a comment. If one ever does, the
    run fails here with the payload unprinted -- which loses a run and keeps an operator
    from splitting a truncated array out of a log and publishing it."""
    text = facts_json(issues)
    inside = [marker for marker in (BEGIN_FACTS, END_FACTS) if marker in text]
    if inside:
        raise ValueError(
            f"the payload contains the fence marker(s) {inside}, so the block this run "
            "prints could not be split back out of the log unambiguously"
        )
    return f"{BEGIN_FACTS}\n{text}\n{END_FACTS}"


def summary(issue: TriageIssue) -> str:
    """One incident on one line: the fields the run's open questions turn on.

    NOT A SECOND RENDERING OF THE ISSUE. It is the log's index into the JSON below it, so a
    person reading the run page can see the corpus without parsing anything; every value
    here is also in the payload, and `report.py` stays the only thing that renders a body."""
    return (
        f"  {issue.batch_id:>16}  {issue.source:<18} {issue.evidence:<34} "
        f"rejected={issue.rejected_rows:<6} {issue.severity:<20} "
        f"prior={issue.prior_executions} {issue.history}"
    )


def report_on(issues: tuple[TriageIssue, ...], batch_id: str) -> None:
    """Print one incident's title and body, or refuse naming what the feed did hold."""
    found = [issue for issue in issues if issue.batch_id == batch_id]
    if len(found) != 1:
        raise ValueError(
            f"{len(found)} incidents in this feed carry batch {batch_id}; the feed holds "
            f"{sorted(issue.batch_id for issue in issues)}"
        )
    print(f"\n{render_title(found[0])}\n")
    print(render_body(found[0]))


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # The run id is read and refused BEFORE the session, the way `dq_gate_batch` refuses its
    # two arguments: an operator should not wait for a serverless start to be told that the
    # run-id parameter arrived unresolved. An absent argument is passed through as "" rather
    # than indexed, so an argv shorter than the YAML declares refuses with the message that
    # names the parameter instead of a bare IndexError naming a list index.
    provenance = provenance_of(args[0] if args else "")
    batch_id = args[1].strip() if len(args) > 1 else ""
    spark = SparkSession.builder.getOrCreate()
    issues = drafted(spark, provenance)
    print(f"{TASK_KEY}: {len(issues)} incident(s) triaged from "
          f"{provenance.telemetry_view} through the shipped opl.triage_agent path. NOTHING "
          "WAS WRITTEN, PROMOTED, RE-RUN OR DELETED.")
    for issue in issues:
        print(summary(issue))
    if batch_id:
        report_on(issues, batch_id)
    print()
    print(emit(issues))


def _assert_this_task_answers_every_fact_the_payload_needs() -> None:
    """The three bound statements plus the feed ARE `issue.FACTS`, in both directions.

    A fact the payload declares and this task never runs would arrive at `triage_issue` as a
    missing keyword; a fact this task runs and the payload does not declare is a query
    costing serverless compute for a column nothing reads. Neither is visible to `ruff` or to
    a reader, because the two spellings are a tuple in one module and dict keys in another
    and no import runs between them. Refused at IMPORT rather than in `main`, on
    `incidents.py`'s pattern -- and the two spellings live on OPPOSITE SIDES OF THE DEPLOY
    here, which that module's guard does not have to contend with: `FACTS` is in the wheel
    and this file is a synced entry point, so a partial deploy can move one and not the
    other. A run whose task file and wheel disagree refuses at import rather than at the
    assembler.

    `bound_statements` is CALLED rather than having its keys retyped here, so the guard
    reads the real key set instead of a hand-written copy of it -- which would be the drift
    wearing the shape of a check. Its argument is taken off `REGISTRY` rather than spelled,
    for this repository's standing reason and for one of its own: the key set is the same
    for every source (all three statements are built on every input), so a literal table
    name here would be a coordinate that cannot be right or wrong, in a file whose whole
    subject is that a task should not spell one."""
    answered = set(bound_statements(sorted(REGISTRY)[0])) | {FEED_FACT}
    if answered != set(FACTS):
        raise ValueError(
            f"this task answers {sorted(answered)} and the issue payload is assembled from "
            f"{sorted(FACTS)}: facts nothing here runs {sorted(set(FACTS) - answered)}, "
            f"facts nothing reads {sorted(answered - set(FACTS))}"
        )


_assert_this_task_answers_every_fact_the_payload_needs()


if __name__ == "__main__":
    main()
