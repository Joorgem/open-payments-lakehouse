# src/opl/triage_agent/report.py
"""One `TriageIssue` as markdown. THE BODY IS A PURE FUNCTION OF THE RECORD.

WHAT THIS FILE IS FOR, AND WHAT IT IS WRITTEN AGAINST. An issue body that reads like
competent analysis for ANY incident is not triage -- it is the species this phase hunts,
wearing markdown, and it is the easiest thing in this package to write by accident. Prose
is the one output whose defects are invisible to the compiler, to `ruff` and to a reader in
a hurry, because a fluent paragraph about the wrong incident looks exactly like a fluent
paragraph about the right one.

SO EVERY SENTENCE HERE IS EITHER A CONSTANT OR A FUNCTION OF A FIELD, and the tests are
shaped to prove the second kind moves. `tests/triage_agent/test_issue_report.py` renders
`592660596679630` (payments, 2,000 rejected rows, this workspace's only stranding) against
`321750543973966` (empresas, 1 row) and asserts the DIFFERENCES -- a different severity, a
different recommended action, a different history reading, a different vault leg, each word
required to be in its own body and absent from the other. Two golden files that happen to
differ somewhere would pass while a template ignored every field it interpolated.

WHAT IS NOT COVERED BY THAT, AND IT IS THE LARGER HALF: the constant sentences. Nothing
proves that the fixed prose in this file is TRUE -- that `evidence_missing_batch_absent`
really is unexplained by the record, that a `fail_on_dq` run really does imply rows were in
quarantine at that instant. Those are read off `docs/f6-run-evidence.md` 0.5 and
`evidence.py`'s header, they are cited where they are stated, and a test cannot check them.
What the tests do cover is that the constants are not INTERCHANGEABLE: each census verdict
and each history reading gets its own sentence, no two are equal, and none of them may be
reached by an incident of the other kind.

THE FIVE INCIDENTS WITH NO QUARANTINE EVIDENCE ARE THE HARDEST THING THIS FILE RENDERS.
Zero rejected rows reads as "nothing to see" in every vocabulary anyone would reach for, and
it is the opposite: `fail_on_dq` runs only when the gate has ALREADY appended rejected rows
(`docs/f6-run-evidence.md` 0.5, off the bundle's wiring), so zero rows today means the
evidence was removed after the fact. The severity ladder already refuses to rank those
mildly; this file has to refuse to WORD them mildly, which is a separate failure with the
same shape. And the two removals get two sentences rather than one, because
`187805471003061` and `315230730740144` sit in a POPULATED quarantine and are explained by
nothing in the record -- one word would let them borrow the lookup trio's account, which is
the only account that exists. That split is T2's; this file carries it through to the prose
or it is undone at the last step.

NO SENTENCE HERE CLAIMS A COMPARISON. `history.py` counts prior executions and compares
nothing, and "compared against the last 5, nothing anomalous" is false for ten of this
workspace's eleven incidents. So the history section states the number FOUND beside the
number ASKED FOR, always both, and says in the same breath that no comparison was made. A
body carrying the reading word without the count would be that sentence with a synonym.

WHAT THE BODY MAY CARRY, AND THIS IS THE PRIVACY LINE. Everything here is batch-grain:
counts, verdicts, reject reasons, table names, declared notes. NO ROW VALUE REACHES IT, and
that is not a promise this file can keep on its own -- it is held by two arms with two
measured blind spots. `test_issue_report.py` counts declared-personal column NAMES in the
rendered body and is blind to a leak that never spells one (a value pasted in); `test_
issue.py` renders bodies from rows the shipped statements actually returned over a fixture
where every value carries a sentinel, and is blind to a leak that TRANSFORMS the value away
from that sentinel. Each arm's only cover is the other's blind spot, which is the pairing
T2 and T3 established for the three statements and the graded row, applied to the fourth
publishable artefact and to the fifth.

THERE IS A SENTENCE TABLE FOR THE CENSUS VERDICTS AND THE HISTORY READINGS AND THERE IS
NONE FOR THE GRADES, which is a decision rather than an omission. `severity.py` publishes
every input beside the grade and this file prints those inputs, so an English gloss of
`bulk_rejection` would be a second spelling of a ladder arm whose predicate is already in
the repository -- and the one grade that genuinely cannot be derived from a column ships
its own prose already, as the declared hold's `why`. The two tables that DO exist are for
words whose meaning is a fact about this workspace (what a missing quarantine implies, what
a missing gate run implies) rather than a restatement of a predicate. NOTHING ENFORCES THAT
LINE: a later gloss table for severities would import cleanly and no test would object.

MARKDOWN IS A THIRD THING A VALUE CAN DO, AND BACKTICKS ARE NOT ESCAPING. `f"`{value}`"`
closes on the first backtick inside the value, and on GitHub what follows is live markdown:
`@handle` notifies a real person and `#123` cross-links a real issue. `reject_reason` is a
quarantine ROW VALUE, so this is the same crafted input the name sweep already reasons
about, arriving as a notification rather than as a leak. `_code` fences by CommonMark's own
rule -- one backtick longer than the longest run inside, padded, folded to one line -- and
each of those three arms is deleted on its own by a test in
`tests/triage_agent/test_issue_markdown.py`. THE TITLE IS FENCED TOO AND THE FENCE IS
LOAD-BEARING THERE AS WELL: GitHub DOES render code spans in issue titles, measured on this
phase's own issue #29 -- see the docstring on `render_title`, which carries the reading.

EXACTLY ONE VALUE IS STILL INTERPOLATED AS PROSE AND IT IS NAMED HERE: `hold_note`, quoted
whole as a blockquote, because a decision rendered as code is one nobody reads. Every line
of it is quoted, so a blank line cannot end the quote and hand the paragraph below it to the
note's author -- and the claim that it is "declared in this repository" is now CHECKED, by
`issue._assert_the_hold_note_is_one_this_repository_declared`, which re-derives it from
`HOLDS` at the file door. That sentence used to be the whole defence and the chain it
described ran through a result row. THE CLOSED VOCABULARIES (`severity`, `recommended_
action`, `verdict`) are fenced here AND constrained there; `evidence` and `history` need
neither, because `CENSUS_MEANING[...]` and `HISTORY_MEANING[...]` raise on a word neither
table has. Nothing enforces the split for a NEW field: one interpolated raw would render and
no test in this package would object unless it is added to one.

WHERE THIS CAME FROM IS THE SECTION THAT ASKS FOR THE MOST TRUST, SO IT SAYS WHERE IT HAS
NONE. The heading used to promise that every number was measured by a nameable statement,
over two free tuples a caller filled in and no test held. Two of the three relations are now
DERIVED here -- the quarantine from `source`, the reconciliation view from
`opl.bronze.reconcile` -- and printed as DERIVED; the statement lines are keyed on
`issue.FACTS`, so a fact whose id was never recorded says `not recorded` instead of being
absent from a list. The run and the telemetry view stay the caller's word and the body calls
them that, because a heading that promises measurement over unlockable free text is worse
than no heading.

AND ONE OCCURRENCE OF A PERSONAL COLUMN NAME IS LEGAL HERE, WHICH IS WHY THAT ARM IS A
COUNT AND NOT AN ABSENCE. The socios reject reason IS
`null_or_empty_nome_socio_razao_social` -- the gate's own word for what it rejected, from
`opl.bronze.rules`, and the body must carry it or it stops saying why 3,583 rows were
rejected. The name arm therefore strips the DECLARED reject reasons and sweeps what is
left, so a reason nobody declared is not stripped and is swept like any other text.
"""
from __future__ import annotations

import re

from opl.bronze.reconcile import BATCH_GRAIN_VIEW
from opl.bronze.registry import table_spec
from opl.triage_agent.blast_radius import blast_radius_note
from opl.triage_agent.evidence import (
    CENSUS_VERDICTS,
    EVIDENCE_MISSING_BATCH_ABSENT,
    EVIDENCE_MISSING_QUARANTINE_EMPTY,
    NO_RECONCILIATION_ROW,
    ROWS_PRESENT,
)
from opl.triage_agent.history import (
    GATE_RUN_ABSENT,
    HISTORY_COMPLETE,
    HISTORY_READINGS,
    INSUFFICIENT_HISTORY,
    NO_PRIOR_EXECUTION,
)
from opl.triage_agent.issue import FACTS, NOTHING_RECORDED, TriageIssue
from opl.triage_agent.severity import SEVERITIES

# What each census verdict MEANS, in one sentence a stranger can read. DECLARED, total over
# `CENSUS_VERDICTS` in both directions at import, and no two of them are the same sentence.
#
# THE TWO ABSENCES ARE TWO SENTENCES AND THAT IS THE POINT (T2's split, `docs/f6-run-
# evidence.md` 0.5). The lookup's three firings are accounted for by F4 -- the table was
# recreated a week later -- and `187805471003061` and `315230730740144` are accounted for by
# nothing, because the estabelecimentos quarantine is NOT empty and still holds neither of
# them. One sentence for both would hand the unexplained pair the explained trio's account.
CENSUS_MEANING: dict[str, str] = {
    ROWS_PRESENT: (
        "the quarantine holds this batch's rejected rows, so the counts above were read "
        "from the rows themselves"
    ),
    EVIDENCE_MISSING_QUARANTINE_EMPTY: (
        "the quarantine table is EMPTY. `fail_on_dq` runs only when the gate has already "
        "appended rejected rows to it, so this is evidence removed after the fact -- not a "
        "batch that was rejected for nothing, which cannot happen (docs/f6-run-evidence.md "
        "0.5). Whether that removal is accounted for is a question for whoever recreated "
        "or truncated the table"
    ),
    EVIDENCE_MISSING_BATCH_ABSENT: (
        "the quarantine table holds OTHER batches' rows and none of this one's. The gate "
        "appended this batch's rejected rows before it published the count that failed the "
        "job, so they were removed after the fact -- and a recreated or truncated table "
        "cannot account for it, because the rows of other batches are still there "
        "(docs/f6-run-evidence.md 0.5)"
    ),
}

# What each history reading MEANS. DECLARED, total over `HISTORY_READINGS` in both
# directions at import.
#
# `gate_run_absent` IS NOT "NO HISTORY" AND THE SENTENCE HAS TO SAY SO: the counts beside it
# are NULL rather than 0, because nothing was measured -- T4's third absence word, for a
# batch whose own gate run has aged out of the telemetry while its quarantine keeps its
# `_batch_id` forever.
HISTORY_MEANING: dict[str, str] = {
    HISTORY_COMPLETE: (
        "at least as many prior gate executions exist as this comparison window asks for"
    ),
    INSUFFICIENT_HISTORY: (
        "fewer prior gate executions exist than this comparison window asks for"
    ),
    NO_PRIOR_EXECUTION: (
        "this job had never run its DQ gate before this incident, so there is nothing to "
        "compare against at all"
    ),
    GATE_RUN_ABSENT: (
        "this batch has NO gate run in the telemetry, so nothing could be counted. The two "
        "counts are absent rather than zero: `0` would assert that this table was never "
        "gated before, which is a measurement nobody made"
    ),
}

# The one line every body carries about what this agent is. `opl.bronze.reconcile` prints
# the remedy and runs none of it, and the same rule one layer up is why this file exists at
# all rather than a function that opens issues.
_FOOTER = (
    "This issue was drafted by `opl.triage_agent`, which reads, ranks and drafts. It "
    "promotes nothing, re-runs nothing, deletes nothing and writes to no table. Any "
    "command in this issue is printed and not run. A person decides."
)

_NO_COMPARISON = (
    "This is how much history EXISTS. No comparison was made against it -- not here and "
    "not anywhere in this agent."
)

_NO_MAGNITUDE = (
    "WHICH tables, never how much of them: nothing in this section is a count, a "
    "proportion or a score."
)


def _code(value: object) -> str:
    """One untrusted string as an inline code span it cannot break out of.

    A BACKTICK IN THE VALUE IS THE WHOLE POINT, and `f"`{value}`"` is not escaping: it
    closes on the first backtick inside, and everything after it is markdown. On GitHub that
    means `@handle` notifies a real person and `#123` cross-links a real issue, from a body
    this repository publishes. `reject_reason` is a quarantine ROW VALUE read out of
    `_dq_reject_reason`, so the crafted input `test_issue_report.py` already reasons about
    for the name sweep is the same input here.

    IT HAS THREE ARMS AND EVERY ONE OF THEM IS LOAD-BEARING, which is measured rather than
    asserted -- `tests/triage_agent/test_issue_markdown.py` deletes each one on its own and
    each deletion reddens one named test. Two of the three are LIVE BREAKOUTS:

      * THE FENCE IS ONE BACKTICK LONGER THAN THE LONGEST RUN INSIDE, CommonMark's own rule.
      * THE PAD is a space on each side when the value begins or ends with a backtick.
        Without it, `` `x `` fences to ```` ```x`` ````: the opener is a three-run, there is
        no three-run closer, no code span opens at all, and the value is live markdown.
      * THE FOLD turns `\\r` and `\\n` into spaces. Without it, a value carrying a BLANK LINE
        ends the paragraph the opening fence is in, so the span never closes and everything
        after it renders. A code span is one line by definition; this is the rendering choice
        that makes the fence mean anything.

    WHAT IT REMOVES, AND IT IS EXACTLY ONE THING: CommonMark strips a single leading and a
    single trailing space from a code span whose content is space-bounded, so a value that
    already began or ended with a space loses one at each such end when a reader looks at it.
    That is the renderer's rule and not this function's -- what is emitted carries every
    character -- and it is stated because the sentence here used to say "nothing is removed",
    which is true of 24 of the 25 crafted values this package's tests round-trip.

    WHAT IT DOES NOT DO: it neutralises nothing OUTSIDE a code span. A caller that
    interpolates a value into prose gets no protection from this, and the one caller that
    still does is `_hold` -- a blockquote cannot be a code span. `hold_note` is safe for a
    different reason, which `issue._assert_the_hold_note_is_one_this_repository_declared`
    now makes true rather than assumed."""
    text = str(value).replace("\r", " ").replace("\n", " ") or " "
    fence = "`" * (max((len(run) for run in re.findall("`+", text)), default=0) + 1)
    pad = " " if text.startswith("`") or text.endswith("`") else ""
    return f"{fence}{pad}{text}{pad}{fence}"


def _count(value: int | None) -> str:
    """A number for a reader, or the word for its absence. NEVER `0` FOR A MISSING VALUE.

    That substitution is the one this whole package is written against, and it is cheapest
    to make in a formatter: `f"{None or 0:,}"` reads as tidy defensive code and publishes a
    measurement nobody took."""
    return "not measured" if value is None else f"{value:,}"


def _rank(issue: TriageIssue) -> str:
    """The severity with its rank, WHICH END IS WORSE, and the ladder's own denominator.

    "rank 1 of 4" is a number with no direction on a page that never says which way the
    ladder runs, and the one thing a reader does with it is decide what to open first.
    `SEVERITIES` is ordered worst-first (`severity.py`'s own comment), so 1 is the worst.

    THE DENOMINATOR IS `len(SEVERITIES)` AND A TEST HOLDS IT THERE. Hardcoding it to `4`
    left the whole file green, which makes it exactly the kind of constant that survives a
    fifth grade being added and then misreports every issue this package drafts."""
    return (
        f"{_code(issue.severity)} (rank {issue.severity_rank} of {len(SEVERITIES)}, "
        "1 is the worst)"
    )


def render_title(issue: TriageIssue) -> str:
    """The issue title: the table, the batch, the grade and the recommendation.

    ALL FOUR ARE FIELDS. A title that named only the table would collide between two
    incidents of one job -- socios has two, three weeks apart -- and the two things a
    reader triages by are the grade and what it says to do.

    THE TITLE IS NOT INERT, AND THAT IS MEASURED. GitHub renders code spans in issue
    titles: the phase's own issue #29 serves `titleHTML` as `[triage] payments batch
    <code>592660596679630</code>: ...`, and the rendered page carries the same `<code>` in
    its `data-testid="issue-title"` element. So `_code` here is the body's defence applied
    to the one line a reader sees first, and not a decoration.

    THIS FILE ONCE CARRIED THE OPPOSITE, MEASURED, AND THE MEASUREMENT WAS THE DEFECT. It
    read `<title>` and `og:title` -- slots that are plain text by definition, so they carry
    literal backticks in a world where the title renders and in a world where it does not --
    and REST v3's lack of a `title_html`, which is a fact about REST v3. Neither can
    distinguish the two worlds, so neither was evidence. `docs/f6-run-evidence.md` 1.10
    carries how that reached three agents and how it left; it is not restated here.

    `batch_id` is fenced for the reason `_headline` fences it -- it is a value the TIMELINE
    returned rather than a word this repository chose -- and the other three are words the
    wheel constrains:
    `source` by `table_spec`, which raises `UnknownTable` at both doors, and the grade and
    the action by the vocabularies `issue.from_mapping` now checks a file against.

    THERE IS NO SHELL EXPOSURE HERE and no escaping for one. The title is a single argv
    element handed to `gh`, and `scripts/open_triage_issue.py` refuses `shell=True` -- a test
    reads the source for it. Whether `@` and `#` LINKIFY in a title is unproven in both
    directions; what is fixed is the formatting break, which is not."""
    return (
        f"[triage] {issue.source} batch {_code(issue.batch_id)}: "
        f"{issue.severity} / {issue.recommended_action}"
    )


def _headline(issue: TriageIssue) -> str:
    """What was gated, how the platform recorded it, and what this issue recommends.

    `job_name`, `batch_id` and `first_started_at` go through `_code` because all three are
    values the TIMELINE returned rather than words this repository chose. THE SEVERITY AND
    THE RECOMMENDED ACTION GO THROUGH IT TOO, and used to be interpolated inside hand-written
    backticks on the ground that they are closed vocabularies -- which was true of the SQL
    door and not of the file the publisher reads. They are constrained at that door now AND
    fenced here, because one check and one fence fail differently."""
    states = ", ".join(_code(state) for state in issue.result_states) or "none recorded"
    return "\n".join((
        f"**{issue.source}**, batch {_code(issue.batch_id)} -- severity {_rank(issue)}, "
        f"recommended action {_code(issue.recommended_action)}.",
        "",
        f"- job: {_code(issue.job_name or 'not recorded in the telemetry')}",
        f"- gate task attempts: **{issue.attempts}**, terminal states {states}",
        f"- first attempt started: {_code(issue.first_started_at or 'not recorded')}",
    ))


def _hold(issue: TriageIssue) -> str:
    """The declared hold, verbatim, or the word for what is in its place. THREE ARMS.

    THE NOTE IS NOT PARAPHRASED AND NOT SUMMARISED. It is `severity.HOLDS`' own `why`,
    carrying its own citation, and a hold nobody can trace to a decision is one the next
    operator deletes -- which is the argument `cadence.py` makes for the shape and T3
    inherited. The absence arm is here so that "no hold" is a stated fact rather than a
    missing section a reader has to notice.

    EVERY LINE IS QUOTED, NOT JUST THE FIRST, and it is `_remedy`'s defect in the other
    section rather than a new one. `f"> {note}"` quotes line one; a BLANK LINE in the note
    ends the blockquote, and the paragraph below -- the one that vouches for the note as a
    recorded decision -- then follows unquoted text a stranger cannot tell from this
    repository's own prose. A blank line becomes `>` rather than `> `, because a quoted line
    of trailing whitespace is a diff nobody wants.

    AN EMPTY NOTE IS NOT A MISSING ONE, which is the third arm and verbatim the fix
    `_remedy` got: `severity.py` refuses a blank `why` at import, so a blank one here came
    from somewhere else, and vouching for it as a DECLARED decision would be a claim about
    `HOLDS` made from a fact about one payload.

    THE NOTE IS THE ONE VALUE IN THIS FILE STILL INTERPOLATED AS PROSE, and the reason it
    may be is no longer an assumption: `issue._assert_the_hold_note_is_one_this_repository_
    declared` re-derives it from `HOLDS` at the file door."""
    if issue.hold_note is None:
        return (
            "## The decision on record\n\n"
            "None. No hold is declared for this batch, so the recommended action above is "
            "derived from the columns below and nothing else."
        )
    if not issue.hold_note.strip():
        return (
            "## The decision on record\n\n"
            "A hold is recorded for this batch and its note is BLANK, which is not the same "
            "as no hold: the recommended action above was decided by a declaration that says "
            "nothing about why. `opl.triage_agent.severity` refuses a blank reason at import, "
            "so this one did not come from there."
        )
    quoted = "\n".join(f"> {line}".rstrip() for line in issue.hold_note.split("\n"))
    return (
        "## The decision on record\n\n"
        f"{quoted}\n\n"
        "That note is a DECLARED hold (`opl.triage_agent.severity.HOLDS`) and not a "
        "derivation. It outranks every arm of the action ladder, which is why the "
        f"recommendation above is {_code(issue.recommended_action)} and not what this "
        "incident's counts alone would produce. Removing the declaration changes what this "
        "issue recommends."
    )


def _rejected(issue: TriageIssue) -> str:
    """What the gate rejected, by reason, and what the census verdict means.

    THE PER-REASON BREAKDOWN IS THE CENSUS AND NOT A RE-DERIVATION -- one line per row of
    `quarantine_census_sql`, in the order that statement returned them.

    A DECLARED REASON WITH ZERO ROWS IS PRINTED. The filter was `if group.rows`, which
    dropped it silently while the sum check stayed green, so a reason the gate declares and
    did not fire read exactly like a reason the gate does not have. THE ONE ROW STILL
    DROPPED is the census's own no-rows row -- a NULL reason at 0, which
    `quarantine_census_sql` emits by construction for a batch the quarantine does not hold
    -- because a breakdown line under the five incidents whose whole subject is that there
    is nothing to break down is noise, not evidence.

    THE TABLE SIZE IS PAST TENSE. It said "currently holds" in an artefact a person reads
    weeks later, and the only thing anchoring "currently" was the free text above it."""
    quarantine = table_spec(issue.source).quarantine
    lines = [
        "## What the gate rejected",
        "",
        f"- rejected rows in THIS batch: **{_count(issue.rejected_rows)}**",
    ]
    lines += [
        f"  - {_code(group.reason or 'no reject reason recorded')}: {_count(group.rows)}"
        for group in issue.reject_groups
        if group.rows or group.reason is not None
    ]
    lines += [
        f"- census verdict: `{issue.evidence}` -- {CENSUS_MEANING[issue.evidence]}.",
        f"- the quarantine table {_code(quarantine)} held "
        f"**{_count(issue.quarantine_table_rows)}** rows in total when this issue was "
        "drafted, across every batch it has ever held. That is the table's size and not "
        "this incident's.",
    ]
    return "\n".join(lines)


def _remedy(issue: TriageIssue) -> str:
    """The remedy column, as an indented code block, or the word for what is in its place.

    THE REMEDY IS `dataops_reconciliation`'s OWN COLUMN, passed through. It is a command
    this repository ships and a human runs; nothing in this package runs it, and the
    sentence beside it says so rather than leaving a reader to assume either way.

    EVERY LINE IS INDENTED, NOT JUST THE FIRST. A single interpolation after six spaces put
    line one inside the block and dropped the rest into the issue as markdown -- and it is a
    column value, so what those lines say is not this file's to assume.

    AN EMPTY REMEDY IS NOT A MISSING ONE. `if issue.remedy` answered "the ladder prints no
    remedy for this verdict" over a column that came back blank, which is a claim about the
    ladder made from a fact about one row."""
    if issue.remedy is None:
        return ("- no remedy is printed for this verdict: `dataops_reconciliation` prints "
                "one only where rows are stranded, and there is nothing here to repromote.")
    if not issue.remedy.strip():
        return ("- the remedy column is present and BLANK, which is not the same as a "
                "verdict that prints no remedy: the view returned an empty value where it "
                "prints a command.")
    block = "\n".join(f"      {line}" for line in issue.remedy.split("\n"))
    return (
        f"- remedy on record, PRINTED AND NOT RUN:\n\n{block}\n\n"
        "Whether that command should be run at all is what the recommended action "
        f"({_code(issue.recommended_action)}) answers. This agent does not run it."
    )


def _reconciliation(issue: TriageIssue) -> str:
    """The reconciliation verdict and its four counts.

    THE ABSENCE ARM SAYS WHAT IS ABSENT AND NOTHING ABOUT THE CORPUS. It read "Five of this
    workspace's eleven incidents are in this state" -- a measurement of eleven incidents,
    published on one, changing on the twelfth with nothing in this repository able to
    notice. What replaces it is the same fact about THIS batch.

    THE ABSENCE ARM'S FENCE IS RULED, NOT GUARDED, AND FOUR MORE IN THIS FILE ARE NEITHER.
    `_code(issue.verdict)` there was mutated back to a hand-written span and NO TEST WENT
    RED -- correctly, because that branch is entered only when the value EQUALS
    `NO_RECONCILIATION_ROW`, so no crafted verdict can reach it. THIS IS THE ONLY SUCH LINE
    WITH A RULE BEHIND IT, which is what the sentence here used to claim of the count: the
    same mutation on `job_name`, `produced_by`, the statement ids and `telemetry_view` was
    green too, and those four carry values no vocabulary constrains -- a telemetry row and
    three strings a caller typed -- so there the fence is the whole defence.
    `test_issue_markdown.py` drives a crafted value through all four now. This one stays for
    uniformity with the other arm and is still not counted as a defence."""
    if issue.verdict == NO_RECONCILIATION_ROW:
        return (
            "## Whether the batch reconciles\n\n"
            f"- verdict: {_code(issue.verdict)} -- `dataops_reconciliation` has no row for "
            "this "
            "batch, so the four counts are absent rather than zero. What is absent is the "
            "view's answer; this line says nothing about how many rows the batch had."
        )
    counts = " / ".join(
        f"{name} {_count(value)}"
        for name, value in (
            ("staged", issue.staged), ("promoted", issue.promoted),
            ("quarantined", issue.quarantined), ("unaccounted", issue.unaccounted),
        )
    )
    return (
        f"## Whether the batch reconciles\n\n- verdict: {_code(issue.verdict)}\n- {counts}\n"
        f"{_remedy(issue)}"
    )


def _history(issue: TriageIssue) -> str:
    """How much history exists, WITH THE NUMBER FOUND, and no claim about a comparison.

    THE COUNT IS ON THE ROW FOR THE REASON T4's HEADER GIVES: at N = 5, ten of this
    workspace's eleven incidents are short of the window and two have no prior execution at
    all, so a reading word without the number it was derived from is the phase's own
    species in one line."""
    return "\n".join((
        "## What history there is to compare against",
        "",
        f"- prior gate executions FOUND: **{_count(issue.prior_executions)}**, against the "
        f"**{issue.executions_requested}** this window asks for.",
        f"- of those, **{_count(issue.prior_incidents)}** also fired the DQ gate.",
        f"- reading: `{issue.history}` -- {HISTORY_MEANING[issue.history]}.",
        "",
        _NO_COMPARISON,
    ))


def _radius(issue: TriageIssue) -> str:
    """WHICH tables are downstream. Never how much of them.

    The sentence is `blast_radius_note`'s, which says the vault bypass OUT LOUD for the two
    bronze tables that reach gold without a vault table in between -- one of them
    `payments`, this workspace's largest incident. The lists are printed under it because a
    sentence naming five tables is harder to scan than five names.

    THE LISTS ARE SPELLED THE WAY THE SENTENCE SPELLS THEM. The bullets carried backticks
    and the sentence two lines above them did not, so `dim_date, fact_payment` and
    `` `dim_date`, `fact_payment` `` were the same two names in two typographies on one
    screen, which reads as two different kinds of thing. `blast_radius_note` cannot fence
    them -- its own test requires the sentence to START with the table name -- so the
    bullets give the backticks up instead. Nothing is lost: these are names this repository
    DECLARES, in the one section that carries no row value at all."""
    return "\n".join((
        "## What else is downstream",
        "",
        blast_radius_note(issue.source) + ".",
        "",
        f"- vault tables: {', '.join(issue.radius.vault) or 'none'}",
        f"- gold tables: {', '.join(issue.radius.gold) or 'none'}",
        "",
        _NO_MAGNITUDE,
    ))


def _measured(issue: TriageIssue) -> str:
    """One line per fact of `FACTS`, and the word for a fact whose run recorded no id.

    THE LINES ARE KEYED ON THE FOUR FACTS AND NOT ON WHAT THE CALLER HAPPENED TO RECORD.
    Both provenances this repository ships record ONE id for FOUR facts, and a list built
    from the caller's tuple printed that one line and nothing about the other three -- so a
    reader counted one statement and had no way to learn that three quarters of the numbers
    above it trace to nothing. `Provenance` refuses a key that is not one of these four, so
    this loop is total in both directions rather than by convention."""
    recorded = dict(issue.provenance.statements)
    return "\n".join(
        f"  - {fact}: {_code(recorded[fact]) if fact in recorded else NOTHING_RECORDED}"
        for fact in FACTS
    )


def _read_from(issue: TriageIssue) -> str:
    """The relations this issue's numbers came out of: two DERIVED, one the caller's word.

    THE FIRST TWO ARE NOT VALIDATED, THEY ARE RECOMPUTED HERE. The quarantine is
    `table_spec(source).quarantine`, the same expression `_rejected` already prints earlier in
    this file; the reconciliation view is `opl.bronze.reconcile`'s own constant. They were a
    free tuple a caller filled in, held by no test -- a heading promising measurement over
    three strings, which was demonstrated by hardcoding a wrong relation and watching every
    body name the payments quarantine with nothing objecting.

    AND THE SECOND IS DERIVED WITHOUT BEING GUARANTEED, WHICH THE LINE NOW SAYS. `severity_
    sql` takes a `view=` seam -- T2's, so a test can drive the three verdicts this workspace
    has never produced -- and a run that used it read a relation this record does not carry.
    The name here is the view this wheel DEPLOYS; that it is also the one the graded row read
    is true of every shipped caller and of nothing this file can check. Plumbing the value
    through would buy a fourth caller-supplied string; saying less is cheaper and true.

    THE TELEMETRY VIEW STAYS THE CALLER'S WORD AND IS LABELLED AS SUCH IN THE BODY. It is
    an argument to `incident_feed_sql` and to `history_sql` -- a seam those modules have on
    purpose -- so no derivation here can know which relation a run pointed them at.

    BOTH DERIVED NAMES ARE UNQUALIFIED, and the line says so: the catalog and schema come
    from the run's `OplConfig`, which this record does not carry and this file will not
    invent.

    THE UNSET TELEMETRY VIEW GETS ITS OWN WORDING RATHER THAN THE WORD `not recorded` WHERE
    A NAME GOES. Every sibling line in this section ends in a fenced relation name; that one
    ended in a bare phrase -- "the task telemetry view not recorded, which the incident feed
    and the history read" -- which scans as a dropped word rather than as an absence, on the
    page where a reader is deciding how much of this issue to believe."""
    view = issue.provenance.telemetry_view
    named = (
        f"the task telemetry view {_code(view)}, which the incident feed and the history "
        "read."
    ) if view else (
        "the task telemetry view that the incident feed and the history read was "
        f"{NOTHING_RECORDED} by this run, so this issue cannot name it."
    )
    return "\n".join((
        f"  - DERIVED by this wheel: the quarantine {_code(table_spec(issue.source).quarantine)}"
        ", which the census and the graded row read.",
        f"  - DERIVED by this wheel: the reconciliation view {_code(BATCH_GRAIN_VIEW)}, "
        "which the graded row reads unless a caller pointed `severity_sql` at another "
        "relation -- a seam this record does not carry.",
        f"  - THE CALLER'S WORD, checked by nothing: {named}",
        "  - The two derived names are UNQUALIFIED: the catalog and schema are the run's "
        "own configuration, which this issue does not carry.",
    ))


def _provenance(issue: TriageIssue) -> str:
    """What was MEASURED, by what, and what was DECLARED. The line a reader needs most.

    WHAT THIS SECTION ASSERTS IS NOW LESS THAN THE HEADING ONCE PROMISED, AND THAT IS THE
    FIX. It claimed every number here was either something a statement returned in a
    nameable run or something a human typed into this repository -- and the first half was
    held by nothing: `statements` and `read_from` were free tuples rebuilt verbatim from the
    file. Two of the three relations are now DERIVED at render time, the statement lines are
    keyed on `FACTS` so an unrecorded id says so, and the two things that remain the
    caller's unchecked word -- the run, and the telemetry view -- are LABELLED as the
    caller's word in the body instead of sitting under a heading that implies otherwise."""
    return "\n".join((
        "## Where this came from",
        "",
        f"- produced by, THE CALLER'S WORD: {_code(issue.provenance.produced_by)}. Nothing "
        "in this repository checks that this run happened, or that it is the run these "
        "numbers came from.",
        "- measured, by these statements:",
        _measured(issue),
        "- read from:",
        _read_from(issue),
        "- DECLARED, not measured: the job-to-table map "
        "(`opl.triage_agent.incidents.TABLE_OF_JOB`), the downstream manifest "
        "(`opl.triage_agent.blast_radius`), the comparison window "
        f"(N = {issue.executions_requested}), and any hold quoted above "
        "(`opl.triage_agent.severity.HOLDS`). All four are typed into this repository and "
        "none of them was read out of the workspace.",
    ))


_SECTIONS = (_headline, _hold, _rejected, _reconciliation, _history, _radius, _provenance)


def render_body(issue: TriageIssue) -> str:
    """The whole body. EVERY SECTION IS RENDERED FOR EVERY INCIDENT, or none is.

    No section is dropped when its fact is absent: each one has an arm for the absence and
    says which absence it is. A body that silently omitted the reconciliation for the five
    incidents that have no row would read, to anyone who had not seen a fuller one, like an
    incident where that question did not arise."""
    return "\n\n".join((*(section(issue) for section in _SECTIONS), _FOOTER))


def _assert_every_census_verdict_has_a_meaning() -> None:
    """Total over `evidence.CENSUS_VERDICTS`, both directions, refused at import.

    A verdict with no sentence would raise `KeyError` at RENDER time -- during a publish, on
    the incident that has it -- and the missing one would be an absence word, because those
    are the two a later change is most likely to add to. The second direction catches a
    sentence left behind by a rename, which would sit here looking like coverage."""
    missing = sorted(set(CENSUS_VERDICTS) - set(CENSUS_MEANING))
    stray = sorted(set(CENSUS_MEANING) - set(CENSUS_VERDICTS))
    if missing or stray:
        raise ValueError(
            f"the census verdicts and their meanings disagree: no sentence for {missing}, "
            f"sentences for {stray} which `evidence.py` does not emit"
        )


def _assert_every_history_reading_has_a_meaning() -> None:
    """Total over `history.HISTORY_READINGS`, both directions, refused at import.

    T4 split one absence into three words BECAUSE they mean different things; a rendering
    that lost one of the three at the last step would undo that split in the only place a
    stranger reads."""
    missing = sorted(set(HISTORY_READINGS) - set(HISTORY_MEANING))
    stray = sorted(set(HISTORY_MEANING) - set(HISTORY_READINGS))
    if missing or stray:
        raise ValueError(
            f"the history readings and their meanings disagree: no sentence for {missing}, "
            f"sentences for {stray} which `history.py` does not emit"
        )


def _assert_no_two_words_share_a_sentence() -> None:
    """No two declared meanings are the same string, which is what the split BUYS.

    Two words rendering identically is exactly the collapse T2's and T4's splits exist to
    prevent, arriving one layer later: the vocabulary would still carry the distinction and
    the artefact a person reads would not. It ranges over both tables at once because the
    hazard is not per-table -- a history sentence copied onto a census verdict is the same
    failure."""
    sentences = [*CENSUS_MEANING.values(), *HISTORY_MEANING.values()]
    if len(set(sentences)) != len(sentences):
        raise ValueError(
            "two declared words render the same sentence, so a reader of the body cannot "
            "tell them apart even though the vocabulary does"
        )


_assert_every_census_verdict_has_a_meaning()
_assert_every_history_reading_has_a_meaning()
_assert_no_two_words_share_a_sentence()
