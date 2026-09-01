# src/opl/triage_agent/issue.py
"""The issue as DATA, assembled from the four facts. It renders nothing and posts nothing.

WHAT THIS IS. The four modules before this one each answer one question about one incident
and each answers it in SQL. This is the record that holds all four answers at once, so that
"what would we say about this incident" is a VALUE a test can diff, and not a string built
at the moment somebody presses send. `report.py` turns this record into markdown;
`scripts/open_triage_issue.py` is the only thing in this repository that can put that
markdown in front of a stranger, and it lives outside the wheel on purpose -- its own
header says why.

WHY A RECORD AND NOT A STRING, AND THE THIRD REASON IS THE ONE THAT DECIDED IT.
`opl.bronze.reconcile` prints the remedy and runs none of it -- ADR 0018 Decision 3 calls a
view that promoted rows "a gate bypass wearing a dashboard" -- and the same line one layer
up is that this package DRAFTS. Second, a body assembled by concatenation at the point of
posting is asserted by nobody: it cannot be diffed between two incidents, and "does this
issue say anything an incident-specific fact decided" stops being a question with an
answer. Third, and this is the one: the credential. `gh` on the operator's box already
carries `repo` scope (docs/f6-run-evidence.md 0.2); a Databricks task calling the GitHub
API needs a PAT in a secret scope -- a new credential, a new human gate, and a token with
repo write sitting beside 55.8M rows of personal data, for a POST a laptop can make.

THE FIELD NAMES ARE THE OTHER MODULES' SQL ALIASES, AND THAT IS A SECOND SPELLING WITH
EXACTLY ONE THING HOLDING IT TOGETHER. `severity_sql` writes `AS severity_rank` as a
literal inside an f-string; this file writes `"severity_rank"` in `SEVERITY_FACTS`. No
import connects them and none can, because a SQL alias is not a Python object. What holds
them equal is `tests/triage_agent/test_issue.py`, which runs the shipped statements over
real tables and requires every name below to be in the result schema -- so a rename in
`severity.py` fails that test rather than emptying a section of a published issue. THE
READER IS PART OF THE DEFENCE: every field is read through `_fact`, which RAISES on a
missing key, so a rename that reached the workspace and not this file refuses to build the
issue at all. A `.get(name)` here would publish a body with a blank where the grade goes,
which is worse than publishing nothing.

WHAT IT REFUSES TO ASSEMBLE, AND EVERY ONE IS REACHABLE BY AN ORDINARY CALLER. Four results
fetched by four statements are four separate round trips, and nothing in a result row says
which incident the row BESIDE it is about. THE SECOND AND THIRD ARE MADE AT BOTH DOORS --
`triage_issue` and `from_mapping` -- and the identity check is made at the first only,
because a file carries one record and has no second fact to disagree with:

  * `MismatchedFacts` -- the facts do not carry one `batch_id`, or the incident and the
    grade disagree about the `source`. A body assembled from payments' severity and
    empresas' history is a plausible, fluent, wrong artefact, and it is one mistyped
    variable away in any caller that loops over the feed.
  * `MismatchedFacts` again -- the census rows do not SUM to the grade's `rejected_rows`.
    That is the one cross-check two of the four facts allow, and what it catches is a
    census read from a different reading of the same quarantine: the breakdown a reader is
    shown would then not decompose the number the grade was computed from. Both results are
    internally consistent, so no other column can see it.
  * `MismatchedFacts` a third time -- a `job_name` still wearing the bundle's development
    prefix, which contains AN OPERATOR'S USERNAME. That one is not about coherence at all;
    it is the only field in this record that can carry a personal identifier into a public
    repository, and the caller that reaches it is the one that read the name from the raw
    timeline instead of from the feed that strips it.

THE FILE DOOR MAKES FOUR MORE THAT THE ASSEMBLER DOES NOT, AND THE ASYMMETRY IS THE POINT.
`severity`, `recommended_action` and `verdict` reach the body inside backticks `report.py`
writes by hand; `hold_note` is quoted whole; `attempts`, `severity_rank` and
`executions_requested` are printed with no formatter at all. Every one of those was called a
word this package chose -- true of the SQL door, where a CASE ladder over declared literals
is the only thing that can produce them, and false of a JSON file, which is what the
publisher reads. So the words are checked against the tuples that declare them, the rank is
re-derived from `SEVERITIES`, the note is re-derived from `HOLDS`, and the three numbers must
be numbers. A backtick in any of them turns the rest of a PUBLIC issue into live markdown.

THE PROVENANCE IS THE FOURTH REFUSAL AND IT IS THE ONE THIS RECORD USED TO MAKE LEAST OF.
`produced_by` renders verbatim into a public issue, so it is refused when it carries the
shape of a filesystem path or of the bundle's bracketed prefix -- both of which carry an
operator's username -- and a statement id must name one of `FACTS`, once, non-blank. The
relations an issue read are NOT carried as free text any more: two of the three are a
function of `source` and of `opl.bronze.reconcile`, so `report.py` derives them, and only
the telemetry view (already an argument to two of the statements) is a field here.

WHAT IT DOES NOT REFUSE, NAMED HERE BECAUSE THE FOUR ABOVE COULD BE READ AS COVER. Nothing
checks that the facts are FRESH, that the four statements ran against the same workspace,
or that the incident is still in the feed. A quarantine emptied between the census and the
grade is invisible here and always will be; what this record carries about that is the
provenance block, which says what produced it, and a reader who needs more asks that run.
Nothing checks that the run named there happened, or that a recorded statement id is the
statement that produced these numbers -- the body says so in the body, in that section.

`quarantine_table_rows` IS CARRIED AND IS NOT THIS INCIDENT'S SIZE. `evidence.py` renamed
that column for exactly this reason: it is a whole-table `COUNT(*)`, and on the socios pair
it reads 3,583 beside `rejected_rows` of 1,797 -- two incidents three weeks apart. It is
here because it is the number that separates the two removals from each other -- an empty
table against a populated one this batch is absent from -- and `report.py` is required by
test to render it under the TABLE's name and never as the incident's.

THE BLAST RADIUS IS RECOMPUTED ON THE WAY IN AND NOT TRUSTED FROM THE FILE. It is a pure
function of `source` over declarations the wheel carries, so `from_mapping` derives it
again and REFUSES a serialised payload whose radius disagrees. What that catches is a facts
file produced by one wheel and published by another, in the one direction that matters: the
manifest gained or lost an edge between the run and the post. It is not a version check and
does not pretend to be one -- two wheels with identical manifests and different everything
else compare equal here.
"""
from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, fields
from typing import Any

from opl.bronze.reconcile import VERDICTS
from opl.triage_agent.blast_radius import BlastRadius, blast_radius
from opl.triage_agent.evidence import NO_RECONCILIATION_ROW
from opl.triage_agent.severity import HOLDS, RECOMMENDED_ACTIONS, SEVERITIES

# The keys each fact must carry, spelled ONCE and used to READ the value as well as to
# check for it -- see the header. Every name here is a column another module's SQL emits
# under exactly this alias, and the payload field it lands in has the same name, so there
# is no translation table for either side to fall out of step with.
INCIDENT_FACTS = (
    "batch_id", "source", "job_name", "attempts", "first_started_at", "result_states",
)
SEVERITY_FACTS = (
    "batch_id", "source", "severity", "severity_rank", "recommended_action", "hold_note",
    "rejected_rows", "quarantine_table_rows", "evidence", "staged", "promoted",
    "quarantined", "unaccounted", "verdict", "remedy",
)
CENSUS_FACTS = ("batch_id", "reject_reason", "rejected_rows")
HISTORY_FACTS = (
    "batch_id", "executions_requested", "prior_executions", "prior_incidents", "history",
)

# The four facts, named once, in the order a reader meets them. Read by the assembler's
# refusals and by the contract test that runs the shipped statements.
FACTS = ("incident", "severity", "census", "history")

# What a body says where a run recorded no statement id and no relation. A WORD AND NOT AN
# OMITTED LINE, which is this package's rule three modules deep: `evidence.py` publishes
# `no_reconciliation_row` rather than NULL and `history.py` publishes `gate_run_absent`
# rather than 0, both because a missing section reads as a question that did not arise.
NOTHING_RECORDED = "not recorded"

# THE CLOSED VOCABULARIES, each read from the module that DECLARES it rather than listed
# here. `report.py` interpolates all three into the body inside backticks and the SQL door
# can only produce them from a CASE ladder -- but a FILE is not a trusted caller, and this
# was the door where "words this package chose" stopped being true. The other two vocabulary
# fields, `evidence` and `history`, need no entry: `report.py` renders them through
# `CENSUS_MEANING[...]` and `HISTORY_MEANING[...]`, which raise on a word neither table has.
_DECLARED_WORDS: dict[str, tuple[str, ...]] = {
    "severity": SEVERITIES,
    "recommended_action": RECOMMENDED_ACTIONS,
    "verdict": (*VERDICTS, NO_RECONCILIATION_ROW),
}

# The three fields a body prints WITHOUT a formatter between them and the markdown: the
# attempt count, the severity's rank and the comparison window. `_count` refuses a string
# (`f"{'x':,}"` raises) and every other number goes through it; these three are interpolated
# bare, so a file carrying `"2** @torvalds #1 **"` for `attempts` would publish it as live
# markdown. `triage_issue` coerces `attempts` already; this is the same coercion's refusal at
# the door that does not.
_WHOLE_NUMBERS = ("attempts", "severity_rank", "executions_requested")


class MissingFact(ValueError):
    """A result did not carry a column this record is built out of."""


class MismatchedFacts(ValueError):
    """The facts handed in are not all about one incident, or they disagree."""


# The shapes an operator identifier actually arrives in on this project's boxes: a
# backslash (every Windows path separator), the home directory three platforms spell
# differently, and -- added separately below -- a drive-letter path and the bundle's
# bracketed prefix. THIS IS NOT A SWEEP FOR A NAME. Nothing here knows one, and committing
# one so that a check could find it is exactly what CLAUDE.md forbids.
_OPERATOR_SHAPES = ("\\", "/home/", "/users/", "/root/")

# A drive letter is ONE letter, so the lookbehind is load-bearing rather than tidy: without
# it `[A-Za-z]:` matches the `s:` of `https://`, and the one value this field is documented
# to want -- a job run URL -- is refused by the check meant to protect it.
#
# THE TRAILING `\S` REPLACED `[\\/]`, WHICH LEFT A MEASURED HOLE: `C:Users/an_operator/x.py`
# is drive-RELATIVE, has no separator after the colon, and was accepted while carrying the
# same username the check exists to stop. `\S` closes that and still lets English through,
# because a one-letter label in prose (`step a: run it`) is followed by a space and a drive
# reference never is.
_DRIVE_PATH = re.compile(r"(?<![A-Za-z])[A-Za-z]:\S")

# The home directory's SHORTHAND, which the three spelled-out prefixes above do not cover:
# `~/repo/x.py` and `~an_operator/repo/x.py` were both accepted, and the second carries an
# operator name in the shell's own syntax. The separator is required so that `~3 minutes`
# is not a path.
_HOME_SHORTHAND = re.compile(r"~[A-Za-z0-9_.-]*[\\/]")


def _assert_no_operator_path_reaches_the_issue(field: str, value: str) -> None:
    """A `Provenance` string reaches a PUBLIC issue verbatim, so it may not carry a path.

    THREE FIELDS, ONE SHAPE LIST, AND THE FIELD NAMES ITSELF IN THE REFUSAL. `produced_by`,
    `telemetry_view` and every statement id are caller-supplied free text that `report.py`
    prints into the body, and no vocabulary constrains any of them. The documented value for
    the first was "a Databricks job run URL, or the command a human typed", and on this
    project's own box that command is `uv run python
    C:\\Users\\<operator>\\...\\open_triage_issue.py` -- a username, in the section a reader is
    told to trust most, in a repository about to be public. CLAUDE.md forbids committing that
    string. THE OTHER TWO WERE CHECKED BY NOTHING until the same Windows path was driven
    through `as_mapping` -> `from_mapping` -- the publisher's own entry, whose own docstring
    says a file is not a trusted caller -- and reached the rendered body. A second shape list
    for them would be the defect this package hunts, so there is one and it is this.

    WHAT IT REFUSES IS A SHAPE and the shapes are a path's and the bundle prefix's. WHAT IT
    DOES NOT REFUSE, and no check in this file can: a username spelled as a bare word, an
    email address, a hostname, or the workspace id a job-run URL carries. A POSIX relative
    path (`scripts/x.py`) is not refused either; a WINDOWS one (`.\\scripts\\x.py`) is, by
    the backslash shape, and the two were one line in this docstring until the complement was
    measured. `report.py` labels the run and the view THE CALLER'S WORD in the body rather
    than letting the heading imply they were checked."""
    lowered = value.lower()
    found = [shape for shape in _OPERATOR_SHAPES if shape in lowered]
    if "[" in value:
        found.append("[")
    if _DRIVE_PATH.search(value):
        found.append("<drive>:")
    if _HOME_SHORTHAND.search(value):
        found.append("~/")
    if found:
        raise MismatchedFacts(
            f"`{field}` carries {found}, which is the shape of a filesystem path or of "
            "the bundle's `[dev <operator>] ` prefix, and both carry an operator's username "
            "into a public issue. Name the run by its URL and the relation by its qualified "
            "name; neither is a path"
        )


def _assert_every_statement_names_one_fact(statements: tuple[tuple[str, str], ...]) -> None:
    """A statement id belongs to one of the four `FACTS`, once, and is not blank.

    THIS IS THE CONSTRAINT THAT STANDS IN FOR A DERIVATION THAT DOES NOT EXIST. A statement
    id exists only after execution, so nothing in this process can recompute one; what it
    can do is refuse a key this record has no fact for -- the same refusal `from_mapping`
    makes for a stray payload key -- so that `report.py` can key its lines on `FACTS` and
    print the word for a fact whose id was never recorded, instead of printing the caller's
    list and leaving the absent three invisible.

    WHAT IT DOES NOT CHECK, and nothing here can: that the id names a statement that ran,
    that it ran against this workspace, or that it produced the numbers in this body."""
    named = [fact for fact, _ in statements]
    stray = sorted(set(named) - set(FACTS))
    repeated = sorted({fact for fact in named if named.count(fact) > 1})
    blank = sorted(fact for fact, identifier in statements if not identifier.strip())
    if stray or repeated or blank:
        raise MismatchedFacts(
            "a statement line is one of the four facts, named once, carrying a non-blank "
            f"id. These are not: unknown facts {stray}, repeated facts {repeated}, blank "
            f"ids {blank}. The four facts are {list(FACTS)}"
        )


@dataclass(frozen=True, kw_only=True)
class Provenance:
    """WHAT PRODUCED THIS ISSUE, and its three fields ask for three different trusts.

    `produced_by` is the run: a Databricks job run URL, or the command a human typed. It is
    required because it is the one thing a reader of a public issue cannot recover from the
    body -- every other line can be re-derived by running the statements again, and this
    one says WHICH running of them is being read. IT IS THE CALLER'S WORD and is checked
    only for the shapes an operator identifier arrives in, which
    `_assert_no_operator_path_reaches_the_issue` names -- and which the other two fields
    are now checked against, on that same one list.

    `statements` is (fact, statement id) for whatever the run recorded. THE FACT MUST BE ONE
    OF `FACTS`, once, with a non-blank id. It may still be empty and it may be partial: both
    provenances this repository ships record ONE id for FOUR facts, and `report.py` keys its
    lines on `FACTS` so the three with no id say so rather than being absent from a list.

    `telemetry_view` IS A NAMED FIELD BECAUSE IT IS THE ONLY RELATION THIS WHEEL CANNOT
    DERIVE. It is already an argument to `incident_feed_sql` and `history_sql`. The other
    two relations an issue reads -- the quarantine and the reconciliation view -- are a
    function of `source` and of `opl.bronze.reconcile`, so `report.py` derives them at
    render time; carrying them here as free text would put a heading that promises
    measurement over three strings nothing holds, which is what this field replaced."""

    produced_by: str
    statements: tuple[tuple[str, str], ...] = ()
    telemetry_view: str | None = None

    def __post_init__(self) -> None:
        if not self.produced_by.strip():
            raise ValueError(
                "an issue payload with no `produced_by` cannot say what produced it, and a "
                "body whose numbers cannot be traced to a run is the artefact this phase "
                "exists to refuse"
            )
        _assert_every_statement_names_one_fact(self.statements)
        _assert_no_operator_path_reaches_the_issue("produced_by", self.produced_by)
        if self.telemetry_view is not None:
            _assert_no_operator_path_reaches_the_issue("telemetry_view", self.telemetry_view)
        for fact, identifier in self.statements:
            _assert_no_operator_path_reaches_the_issue(f"the {fact} statement id", identifier)


@dataclass(frozen=True, kw_only=True)
class RejectGroup:
    """One (reject reason, rows) pair out of the census. `reason` IS NULLABLE.

    A NULL reject reason is a real group in this project's own fixture and is not an
    absence of rejected rows -- `evidence._CENSUS_LADDER` keys the presence of rejected
    rows on the COUNT and not on the reason, for exactly that input."""

    reason: str | None
    rows: int


@dataclass(frozen=True, kw_only=True)
class TriageIssue:
    """Everything one issue says, as values. No markdown, no ordering, no sentence.

    THE INPUTS ARE HERE BESIDE THE GRADES, which is `severity.py`'s rule inherited rather
    than restated: `rejected_rows`, `evidence`, `verdict` and the four reconciliation
    counts are the facts the severity was computed from, and a grade whose inputs are not
    beside it cannot be checked against them by the person reading the issue.

    EVERY NULLABLE FIELD IS NULLABLE FOR A MEASURED REASON, and none of them is nullable
    because a value was inconvenient: the four counts and `remedy` are NULL for the five
    incidents `dataops_reconciliation` has no row for, `hold_note` for every batch but one,
    the two history counts for a batch whose gate run has aged out of the telemetry, and
    `job_name` for a run whose job `system.lakeflow.jobs` has forgotten."""

    batch_id: str
    source: str
    job_name: str | None
    attempts: int
    first_started_at: str | None
    result_states: tuple[str, ...]

    severity: str
    severity_rank: int
    recommended_action: str
    hold_note: str | None

    rejected_rows: int
    quarantine_table_rows: int
    evidence: str
    reject_groups: tuple[RejectGroup, ...]

    staged: int | None
    promoted: int | None
    quarantined: int | None
    unaccounted: int | None
    verdict: str
    remedy: str | None

    executions_requested: int
    prior_executions: int | None
    prior_incidents: int | None
    history: str

    radius: BlastRadius
    provenance: Provenance


def _fact(row: Mapping[str, Any], key: str, name: str) -> Any:
    """One value out of one result row, or REFUSE.

    The whole reason this is a function rather than a subscript: `row.get(key)` is one
    character shorter and publishes a blank where a grade goes."""
    if key not in row:
        raise MissingFact(
            f"the {name} fact has no {key!r} column (it carries {sorted(row)}). That name "
            "is the alias another module's SQL emits, and this record reads it by name -- "
            "so a rename there arrives here as a refusal to build the issue"
        )
    return row[key]


def _facts(row: Mapping[str, Any], keys: tuple[str, ...], name: str) -> dict[str, Any]:
    """Every declared key of one fact, read through `_fact`."""
    return {key: _fact(row, key, name) for key in keys}


def _assert_one_incident(rows: Mapping[str, Mapping[str, Any]]) -> None:
    """The facts are about ONE incident, or refuse to fuse them.

    Four statements are four round trips and a result row says nothing about the row beside
    it. THE `source` CHECK IS THE SECOND HALF AND NOT A RESTATEMENT: two incidents of one
    job share no batch id, so the first check cannot see a caller that read the batch from
    the feed and the grade from another table's cached result."""
    ids = {name: str(_fact(row, "batch_id", name)) for name, row in rows.items()}
    if len(set(ids.values())) > 1:
        raise MismatchedFacts(
            f"these facts are about more than one incident: {ids}. An issue assembled from "
            "them would be fluent, plausible and about nothing"
        )
    sources = {row["source"] for row in rows.values() if "source" in row}
    if len(sources) > 1:
        raise MismatchedFacts(
            f"the facts name more than one bronze table ({sorted(sources)}), so the grade "
            "and the incident are not about the same pipeline"
        )


def _assert_no_operator_identifier_reaches_the_payload(job_name: str | None) -> None:
    """A runtime job name still wearing its bundle prefix carries an OPERATOR'S USERNAME.

    The target this repository DEPLOYS is `mode: development`, which names every deployed
    job `[dev <operator>] <name>`, and CLAUDE.md forbids committing that string -- the
    run-evidence documents redact the same identifier elsewhere. `incidents.py` strips it in
    SQL, by a pattern that matches any leading bracketed token, and every payload built from
    that feed is clean before it arrives here.

    THIS IS A SECOND, COARSER CHECK AND NOT A SECOND SPELLING OF THAT PATTERN. It refuses a
    SHAPE -- a bracket in a job name -- where the feed removes a token, so the two fail
    differently and neither can be mistaken for the other's coverage. What it catches is the
    caller that read `job_name` from the raw timeline instead of from the feed, which is one
    join away in a workspace where both are available. What it does NOT catch is an operator
    identifier arriving in any other shape or any other field; nothing here can, because
    only the bundle knows what that prefix looks like.

    NAMED CONCRETELY, BECAUSE "ANY OTHER SHAPE" IS TOO VAGUE TO ACT ON: a target declaring
    `presets.name_prefix: dev_<operator>_` produces `dev_jorge_opl-bronze-payments`, which
    has no bracket and passes here. What keeps this check total over what the bundle can
    actually emit is not this function -- it is `test_issue_payload.py`, which reads
    `databricks.yml` and refuses a target that prefixes a job name any other way."""
    if job_name is not None and "[" in job_name:
        raise MismatchedFacts(
            f"the job name {job_name!r} still carries a bracketed prefix. This repository "
            "deploys in development mode, which puts `[dev <operator>] ` in front of every "
            "job name, and that is an operator's username -- `incident_feed_sql` strips it "
            "and this payload was not built from that column"
        )


def _assert_the_breakdown_sums_to_the_grade(
    groups: tuple[RejectGroup, ...], rejected_rows: int
) -> None:
    """The one cross-check two of the four facts allow, MADE AT BOTH DOORS.

    `severity.py` computes its `rejected_rows` by SUMming this very census inside SQL, so a
    breakdown that does not add up to the graded number is a census from another reading of
    the quarantine -- and both results are internally consistent, so no other column can see
    it.

    IT IS A FUNCTION AND NOT A BLOCK INSIDE `_reject_groups` BECAUSE OF WHAT IT COST AS ONE.
    `from_mapping` rebuilds the groups straight out of a file and never called that, so a
    payload whose breakdown summed to 1 against a headline of 2,000 published through the
    publisher's only door while the same disagreement through `triage_issue` raised."""
    total = sum(group.rows for group in groups)
    if total != rejected_rows:
        raise MismatchedFacts(
            f"the census rows sum to {total} rejected rows and the grade was computed from "
            f"{rejected_rows}. `severity_sql` sums this same census, so these two results "
            "are not from one reading of the quarantine"
        )


def _assert_every_grade_is_a_word_this_package_declares(values: Mapping[str, Any]) -> None:
    """The three closed vocabularies, and the rank DERIVED from one of them. FILE DOOR ONLY.

    `report.py` writes `` f"`{issue.verdict}`" `` by hand, and a backtick in the value closes
    that span: what follows is live markdown in a PUBLIC issue, where `@handle` notifies a
    real person. Its header called these "words this package chose", which is true of the SQL
    door -- each is produced by a CASE ladder over declared literals -- and was false here,
    where the value is whatever a JSON file says.

    THE RANK IS RE-DERIVED AND NOT MERELY TYPE-CHECKED, on `_radius_of`'s pattern:
    `SEVERITIES` is ordered worst-first, so the rank is a function of the word beside it and
    a payload where the two disagree is two readings of one ladder. That also closes the
    field for good -- an integer this body prints bare cannot be a crafted string.

    NOT MADE AT THE ASSEMBLER, and that is the same asymmetry `_assert_one_incident` has in
    the other direction: `severity_sql` cannot emit a word that is not in these tuples, so a
    check there would be a second spelling of the ladder rather than a refusal of anything
    reachable."""
    wrong = {
        name: values[name]
        for name, allowed in _DECLARED_WORDS.items()
        if values[name] not in allowed
    }
    if wrong:
        raise MismatchedFacts(
            f"the payload carries {wrong}, and these are not words this package declares. "
            f"The vocabularies are {({name: list(v) for name, v in _DECLARED_WORDS.items()})}"
        )
    rank = SEVERITIES.index(values["severity"]) + 1
    if values["severity_rank"] != rank:
        raise MismatchedFacts(
            f"the payload grades this incident {values['severity']!r} and ranks it "
            f"{values['severity_rank']!r}; `severity.SEVERITIES` is ordered worst-first and "
            f"puts that word at {rank}"
        )


def _assert_the_numbers_a_body_prints_bare_are_numbers(values: Mapping[str, Any]) -> None:
    """`attempts`, `severity_rank` and `executions_requested` reach the body unformatted.

    Every other number in a body goes through `report._count`, which refuses a string by
    raising out of `f"{value:,}"`. These three are interpolated bare -- two of them inside
    `**` -- so a file carrying a crafted string for one of them publishes it as markdown. A
    `bool` is refused with them: `True` is an `int` in Python and would render as `True`
    where a reader expects a count."""
    wrong = {
        name: values[name]
        for name in _WHOLE_NUMBERS
        if not isinstance(values[name], int) or isinstance(values[name], bool)
    }
    if wrong:
        raise MismatchedFacts(
            f"the payload carries {wrong} where this record holds whole numbers. "
            "`report.py` prints all three into the body with no formatter between them and "
            "the markdown, so a string here is published as written"
        )


def _assert_the_hold_note_is_one_this_repository_declared(
    batch_id: str, hold_note: str | None
) -> None:
    """A note is `HOLDS`' own `why` for THIS batch, or there is no note. RE-DERIVED.

    THE REASONING THIS REPLACES WAS UNSOUND AND SAID SO IN THREE HEADERS: `hold_note` was
    left out of every fence because it is "declared in this repository rather than read from
    a row". The chain is `HOLDS[batch].why` -> `sql_string_literal` into the graded CASE -> a
    RESULT ROW (it is in `SEVERITY_FACTS`) -> the payload JSON -> here, which performed no
    `HOLDS` lookup -> a blockquote in a public issue. It was read from a row, and this is the
    door the publisher uses.

    So the lookup the sentence assumed is now MADE, which is `_radius_of`'s shape: the wheel
    derives the note again and refuses a disagreement. That is what makes "declared in this
    repository" true of the file door as well, and it is why `report.py` may quote the note
    as prose rather than fencing it -- a blockquote cannot be a code span and a hold whose
    argument is rendered as code is one nobody reads.

    THE ABSENCE DIRECTION IS DELIBERATELY NOT REFUSED: a held batch carrying no note is the
    state `test_issue_report.py` builds to prove the recommendation flips when the
    declaration is removed, and a payload written before a hold was declared is not a forged
    one. What is refused is a note this repository never wrote."""
    declared = HOLDS[batch_id].why if batch_id in HOLDS else None
    if hold_note is not None and hold_note != declared:
        raise MismatchedFacts(
            f"the payload carries a hold note for batch {batch_id} that "
            f"{'differs from' if declared else 'this repository never declared'} "
            "`opl.triage_agent.severity.HOLDS`. That note is quoted whole into a public "
            "issue as the decision on record, so it is the repository's word or it is nothing"
        )


def _reject_groups(
    census: Sequence[Mapping[str, Any]], rejected_rows: int
) -> tuple[RejectGroup, ...]:
    """The census as records, read through `_fact` and summed against the grade."""
    read = [_facts(row, CENSUS_FACTS, "census") for row in census]
    groups = tuple(
        RejectGroup(reason=row["reject_reason"], rows=int(row["rejected_rows"]))
        for row in read
    )
    _assert_the_breakdown_sums_to_the_grade(groups, rejected_rows)
    return groups


def triage_issue(
    *,
    incident: Mapping[str, Any],
    severity: Mapping[str, Any],
    census: Sequence[Mapping[str, Any]],
    history: Mapping[str, Any],
    provenance: Provenance,
) -> TriageIssue:
    """The record, from the four results and a provenance. A PURE FUNCTION OF ITS INPUTS.

    It reads no table, opens no session and takes no default from the workspace, which is
    what makes a body diffable between two incidents and assertable about one. The rows are
    mappings: a caller holding pyspark `Row`s hands in `row.asDict()`, which is the one line
    this signature deliberately does not hide -- a record built straight from a `Row` would
    take its column names from whatever the query happened to return."""
    _assert_one_incident({
        "incident": incident, "severity": severity, "history": history,
        **({"census": census[0]} if census else {}),
    })
    graded = _facts(severity, SEVERITY_FACTS, "severity")
    seen = _facts(incident, INCIDENT_FACTS, "incident")
    counted = _facts(history, HISTORY_FACTS, "history")
    started = seen["first_started_at"]
    _assert_no_operator_identifier_reaches_the_payload(seen["job_name"])
    return TriageIssue(
        **{key: graded[key] for key in SEVERITY_FACTS},
        job_name=seen["job_name"],
        attempts=int(seen["attempts"]),
        first_started_at=None if started is None else str(started),
        result_states=tuple(seen["result_states"] or ()),
        reject_groups=_reject_groups(census, graded["rejected_rows"]),
        **{key: counted[key] for key in HISTORY_FACTS if key != "batch_id"},
        radius=blast_radius(graded["source"]),
        provenance=provenance,
    )


def as_mapping(issue: TriageIssue) -> dict[str, Any]:
    """The record as plain JSON-able data, radius and provenance included.

    THE RADIUS IS WRITTEN OUT AND IS NOT WHAT IS READ BACK -- `from_mapping` derives it
    again and refuses a disagreement. It is here so that a person reading the file sees the
    same answer the body will carry, without importing this package."""
    out: dict[str, Any] = {}
    for spec in fields(issue):
        value = getattr(issue, spec.name)
        if spec.name == "radius":
            out[spec.name] = {
                "source": value.source, "vault": list(value.vault), "gold": list(value.gold),
            }
        elif spec.name == "provenance":
            out[spec.name] = {
                "produced_by": value.produced_by,
                "statements": [list(pair) for pair in value.statements],
                "telemetry_view": value.telemetry_view,
            }
        elif spec.name == "reject_groups":
            out[spec.name] = [{"reason": g.reason, "rows": g.rows} for g in value]
        else:
            out[spec.name] = list(value) if isinstance(value, tuple) else value
    return out


def _radius_of(carried: Mapping[str, Any], source: str) -> BlastRadius:
    """The radius this wheel derives, checked against the one the file carries."""
    derived = blast_radius(source)
    written = BlastRadius(
        source=carried["source"], vault=tuple(carried["vault"]), gold=tuple(carried["gold"]),
    )
    if derived != written:
        raise MismatchedFacts(
            f"the payload carries a blast radius of {written} and this wheel derives "
            f"{derived}. The downstream manifest changed between the run that produced "
            "these facts and the attempt to publish them"
        )
    return derived


def _provenance_of(carried: Mapping[str, Any]) -> Provenance:
    """The provenance a file carries, read through the same door as every other field.

    A NESTED MAPPING IS STILL A FILE. `from_mapping` refuses a stray key at the top level,
    and this is the one field whose value is itself a record -- so an unknown key and a
    missing key are refused here too, rather than arriving as a `KeyError` at render time
    with nothing in the message naming the payload. `Provenance.__post_init__` then makes
    every refusal an in-process caller already meets, including the two this task added."""
    known = {spec.name for spec in fields(Provenance)}
    stray = sorted(set(carried) - known)
    if stray:
        raise MissingFact(
            f"the payload's provenance carries {stray}, which `Provenance` has no field "
            "for. A key this wheel does not know is a payload written by code this wheel is "
            "not"
        )
    return Provenance(
        produced_by=_fact(carried, "produced_by", "provenance"),
        statements=tuple(
            (str(fact), str(identifier))
            for fact, identifier in _fact(carried, "statements", "provenance")
        ),
        telemetry_view=_fact(carried, "telemetry_view", "provenance"),
    )


def _assert_what_only_a_file_can_get_wrong(values: Mapping[str, Any]) -> None:
    """The three refusals the assembler does not make, in the order they have to run.

    THE NUMBER CHECK RUNS FIRST AND THAT ORDER IS LOAD-BEARING. `severity_rank` is in both
    it and the grade check, and the grade check compares that field for EQUALITY against the
    rank `SEVERITIES` gives -- where `True == 1` is true in Python, so a `bool` would pass the
    rank arm and reach the body as the word `True` where a reader expects a number."""
    _assert_the_numbers_a_body_prints_bare_are_numbers(values)
    _assert_every_grade_is_a_word_this_package_declares(values)
    _assert_the_hold_note_is_one_this_repository_declared(
        str(values["batch_id"]), values["hold_note"]
    )


def from_mapping(carried: Mapping[str, Any]) -> TriageIssue:
    """A record read back from JSON. Every refusal the assembler makes still applies.

    A FILE IS NOT A TRUSTED CALLER: it was written by another process, possibly by another
    wheel, and the publisher that reads it can put what it holds in front of a stranger. So
    the tuples are rebuilt, the radius is re-derived and compared, THE BREAKDOWN IS SUMMED
    AGAINST THE HEADLINE, the provenance is rebuilt through its own refusals, an unknown key
    is refused rather than ignored, and a missing field raises rather than defaulting.

    AND THE WORDS ARE CHECKED AGAINST THE VOCABULARIES THAT DECLARE THEM. `severity`,
    `recommended_action` and `verdict` are interpolated into the body inside hand-written
    backticks, `hold_note` is quoted whole as a blockquote, and three numbers are printed
    bare -- seven fields a file decided and nothing here read. So the words must be declared,
    the rank must be the one `SEVERITIES` gives that word, the note must be `HOLDS`' own, and
    the three numbers must be numbers.

    THE SUM CHECK WAS THE ONE THAT WENT MISSING, and the sentence above said otherwise for
    three docstrings: a payload whose breakdown summed to 1 against a headline of 2,000 was
    ACCEPTED here and rendered both numbers. What is still NOT re-made from a file: nothing
    checks that the facts are fresh, that they came from one workspace, or that the run
    named in the provenance ever happened."""
    known = {spec.name for spec in fields(TriageIssue)}
    stray = sorted(set(carried) - known)
    if stray:
        raise MissingFact(
            f"the payload carries {stray}, which this record has no field for. A key this "
            "wheel does not know is a payload written by code this wheel is not"
        )
    values = {name: _fact(carried, name, "payload") for name in sorted(known)}
    _assert_no_operator_identifier_reaches_the_payload(values["job_name"])
    _assert_what_only_a_file_can_get_wrong(values)
    groups = tuple(
        RejectGroup(reason=g["reason"], rows=g["rows"]) for g in values["reject_groups"]
    )
    _assert_the_breakdown_sums_to_the_grade(groups, values["rejected_rows"])
    rebuilt = ("radius", "provenance", "reject_groups", "result_states")
    return TriageIssue(
        **{name: value for name, value in values.items() if name not in rebuilt},
        result_states=tuple(values["result_states"]),
        reject_groups=groups,
        radius=_radius_of(values["radius"], values["source"]),
        provenance=_provenance_of(values["provenance"]),
    )


def payloads_from_json(text: str) -> tuple[TriageIssue, ...]:
    """Every record in a facts file: one mapping, or a list of them.

    BOTH SHAPES BECAUSE BOTH ARE WHAT A PRODUCER WRITES -- a run that triaged one incident
    and a run that triaged the feed -- and the publisher's refusal to post more than one
    issue is about what it POSTS, not about what it may read."""
    loaded = json.loads(text)
    listed: Iterable[Any] = loaded if isinstance(loaded, list) else [loaded]
    return tuple(from_mapping(item) for item in listed)
