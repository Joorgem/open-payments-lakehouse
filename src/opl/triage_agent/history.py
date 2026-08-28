# src/opl/triage_agent/history.py
"""How many prior gate executions this incident can actually be compared against.

WHAT THIS ANSWERS. The master spec asks the agent to compare an incident against the
LAST N EXECUTIONS. This is the module that says how many of those N exist: for one
`job_run_id`, how many gate runs of the same job came before it, how many of those also
fired the gate, and whether N was available at all. It reads, folds and reports. IT
GRADES NOTHING -- severity is `severity.py`'s and stays there -- and it writes nothing,
which is the package rule inherited rather than restated (ADR 0018 Decision 3).

AN AGENT THAT REPORTS "compared against the last 5, nothing anomalous" WITHOUT SAYING HOW
MANY IT FOUND IS REPORTING A COMPARISON IT DID NOT MAKE. At N = 5 that sentence is false
for TEN of the eleven incidents in this workspace, and for TWO of them there is no prior
execution at all (`docs/f6-run-evidence.md` 0.10). So the number actually found is on
every row whatever the reading says, and "fewer than N exist" gets its own word rather
than borrowing the vocabulary of a comparison that happened.

THE THREE WAYS TO GET THIS WRONG ARE ALL SILENT, ALL MEASURED, AND EACH RETURNS A
PLAUSIBLE INTEGER. None of them raises, none produces a NULL, and none is visible in the
output. They are what this file's shape is for, and `tests/triage_agent/test_history.py`
runs all three beside the right answer rather than describing them.

  1. THE NAIVE TIMESTAMP COMPARISON INFLATES EVERY INCIDENT BY EXACTLY ONE. `check_bad_
     rows` starts BEFORE `fail_on_dq` inside the same job run -- the condition task is
     what decides whether the failure task runs at all -- so "gate runs that started
     before this incident started" counts the incident's OWN gate run as prior history.
     It turns the two zeroes into ones, deleting the one state a triager most needs:
     THIS TABLE HAS NEVER BEEN GATED BEFORE. The defect was shipped in the controller's
     own first probe and was caught by arithmetic that could not close (8 prior runs on a
     job with 8 gate runs in total), not by review.
  2. THE RETIRED GATE SPELLING ERASES A HISTORY RATHER THAN SHORTENING IT. Keyed on
     `dq_gate_batch`, the lookup's three incidents return 0 prior executions where the
     truth is 4, 3 and 1 -- its only `dq_gate_batch` run postdates all three of them.
     Those are the same three incidents whose quarantine evidence is already gone (0.5),
     so the naive key truncates precisely the history that is left.
  3. `check_bad_rows.result_state` CANNOT TELL A FIRED GATE FROM A CLEAN ONE. It is
     SUCCEEDED on all 29 runs, because a condition task succeeds whether its answer is
     true or false. A count of "prior incidents" taken off the terminal state reports the
     number a workspace with ZERO incidents would report. THE ONLY SIGNAL THAT A GATE
     FOUND REJECTED ROWS IS THE PRESENCE OF A `fail_on_dq` TASK RUN, which is what
     `prior_incidents` is counted from.

THE KEY IS `check_bad_rows` AND THE ARGUMENT IS AN IDENTITY RATHER THAN A PREFERENCE.
Measured 2026-08-25 over `dataops_task_telemetry` (0.8, 0.10):

    dq_gate (5) + dq_gate_batch (24) = 29 = check_bad_rows (29) = ingest (29)

`check_bad_rows` is the only gate-adjacent task present in all seven jobs across the whole
window, and its job-run count equals the SUM of both gate spellings exactly. It is the
condition task that consumes `bad_row_count`, so it exists on every run where a gate
produced a verdict -- under either spelling. That is why the gate's own name is not the
key here even though it is the task a reader would name first.

WHAT THE `check_bad_rows` KEY DOES NOT BUY, SAID BESIDE IT: the identity above is a
measurement of THIS workspace on THIS date, not a property of Lakeflow. It holds because
every bronze job wires the condition task downstream of its gate, which the bundle lock
below holds true going forward -- and it says nothing about runs already recorded under
some other spelling, which no code in this wheel can know exist. That is the half of 0.8's
hazard this repository cannot close, and `incidents.py` states the same limit for its own
pinned key.

BOTH GATE SPELLINGS ARE DECLARED AS DATA, ON `opl.dataops.cadence`'s PATTERN, AND THE
DECLARATION IS ASYMMETRIC BECAUSE THE WORLD IS. `dq_gate_batch` is live and appears in all
seven bronze jobs; `dq_gate` is RETIRED and appears in no YAML at all -- its five runs live
only in the telemetry. So the lock cannot be "the declaration equals what the bundle says"
in both directions: it holds that every LIVE spelling is consumed by a `check_bad_rows`
condition task in the bundle and that every RETIRED one is consumed by none.
`tests/triage_agent/test_history_declaration.py` carries it, and that file's header rules
on what it does not catch.

THE ANCHOR IS THE INCIDENT'S OWN GATE RUN, AND SELF-EXCLUSION IS BY IDENTITY. A prior run
is a gate run of the same job that started NO LATER than this job run's own gate started,
whose `job_run_id` is not this one. Two predicates, and neither is decoration:

  * `started_at <= gate_started_at` IS WRITTEN WITH `<=` AND NOT `<`, deliberately. A
    strict inequality would exclude this incident's own gate run as a side effect of the
    anchor being its own start -- self-exclusion by arithmetic accident, which decision 4
    of this task refuses and which stops working the moment the anchor moves by one task.
    So `<=` includes the incident's own row and the identity predicate is the ONE thing
    that removes it: delete `job_run_id <> :batch_id` and every count in this workspace
    rises by exactly one, which is defect 1 above reproduced exactly.
  * THE TIE IS DECIDED IN BOTH DIRECTIONS AND ONLY ONE OF THEM FLATTERS THIS CHOICE. `<`
    drops a genuinely PRIOR run recorded at the anchor's own timestamp; `<=` admits a run
    of the same job that is genuinely LATER and recorded at that same timestamp. No
    spelling of the bound is right in both directions, and `<=` is chosen because it is the
    one that leaves the identity predicate reachable. `test_a_prior_run_tied_to_the_anchor_
    is_kept_by_the_inclusive_bound` holds the first direction on a constructed tie;
    THE SECOND DIRECTION IS HELD BY NOTHING, and nothing measured says whether two gate
    runs of one job have ever shared an instant here -- 0.10 records that the lookup's five
    retired-spelling runs share a DAY and goes no finer.
  * The time predicate is what makes it PRIOR rather than ALL: the estabelecimentos
    incident at run #3 of 8 has two prior runs, not seven.

FOLD TO `job_run_id` BEFORE COUNTING ANYTHING, TWICE, AND ONLY ONE OF THE TWO FOLDS IS
VISIBLE IN THIS WORKSPACE. `fail_on_dq` carries TWO task runs per job run here -- 22 rows
over 11 incidents, `max_retries: 0` failing to prevent a retry -- so a `gated_runs` leg
without its DISTINCT doubles `prior_incidents` AND fans the prior-run join out with it,
and the corpus shows that immediately. `check_bad_rows` runs exactly ONCE per job run on
all seven jobs (29 task runs over 29 job runs), so the fold on the history leg is invisible
here: A FIXTURE FAITHFUL TO THIS WORKSPACE REPORTS GREEN FOR A QUERY WITH NO FOLD IN IT.
That leg is proven by a constructed doubled row, labelled as constructed, in the test file.

FOUR WORDS, AND THE ABSENCE IS SPLIT IN TWO FOR `evidence.py`'s REASON. `insufficient_
history` and `no_prior_execution` are both "fewer than N", and collapsing them puts NEVER
GATED BEFORE inside a word that also means THREE OF FIVE. That is the same refusal T2 made
between `evidence_missing_quarantine_empty` and `evidence_missing_batch_absent`, and it is
made here for the same reason: the two states send a reader to different places, and the
one that reads worse is the one that would be hidden. `history_complete` is the only word
that is not a finding.

EVERY WORD IS ABOUT WHETHER A COMPARISON IS POSSIBLE, NEVER ABOUT WHAT ONE FOUND. Nothing
in this module compares anything, so `history_complete` means "the N you asked for exist",
never "N were compared and nothing was anomalous". A renderer that prints the second
sentence off this column is doing what the plan's 1.6 exists to refuse.

`gate_run_absent` IS THE FOURTH WORD AND ITS COUNTS ARE NULL RATHER THAN ZERO. If the
telemetry holds no `check_bad_rows` run for the batch, there is no anchor, and "how many
ran before this one" has no answer -- so it is not answered. Zero would be a claim, and it
would be the same claim `no_prior_execution` makes from a measurement. This is reachable
without any drift: F4 measured a ~25-day retention floor on the system tables while a
quarantine keeps its `_batch_id` forever, so a batch a triager can still see in a
quarantine can outlive the timeline rows that would date it.

AND THE LADDER'S FIRST ARM IS LOAD-BEARING IN A WAY THAT IS EASY TO MISS. With the arm
removed, an absent gate run does not fall through to another absence word: `NULL = 0` and
`NULL < 5` are both NULL, so it reaches the ELSE and reports `history_complete` -- SQL's
three-valued logic turning "I could not look" into the most reassuring word on the list.
`test_a_batch_with_no_gate_run_reads_absent_and_not_complete` fires it.

NO RATIO, NO PROPORTION. `prior_executions` and `prior_incidents` are published side by
side and never divided. The denominator is 0 for two of the eleven incidents, and this
phase has refused a proportion twice already -- T3 refused it in the severity ladder, and
0's own fixture warning records that the shared fixture does not reproduce the workspace's
proportions at all. A rate computed here would be a number about the fixture.

THE HISTORY IS KEYED ON `job_id` AND NOT ON `job_name`. Measured 2026-08-25: seven job
ids, one name each, and NO `check_bad_rows` run carries a NULL name -- so both columns
would work TODAY, and that is written down as a property of this corpus on this date
rather than as a guarantee. It is keyed on the id anyway because `opl.dataops.telemetry`
deliberately KEEPS task runs whose job has aged out of `system.lakeflow.jobs`, and those
rows carry a live id and a NULL name; a name join drops them silently, and dropping a
prior run is the defect this whole file is about. The test file drives that shape, which
this corpus does not contain.

FOUR COLUMNS ARE READ FROM THE VIEW, AND THEY ARE EXECUTED AGAINST THE SHIPPED ONE RATHER
THAN ARGUED FOR. `job_run_id`, `job_id`, `started_at` and `task_key` are a SUBSET of the six
T1's feed reads, and that subset relation was the whole of the argument until
`tests/triage_agent/test_history_absence.py` ran this statement over
`dataops.telemetry.task_telemetry_sql`'s own body. `result_state` is deliberately not among
the four, which is defect 3 above. WHAT THAT RUN DOES NOT BUY: the view it drives is built
on EMPTY system tables, so it proves the four names RESOLVE against what F4 deploys and
nothing else -- no count in this module is checked by it, and the deployed view over real
system tables is T8's run and not this file's.

WHAT IS PUBLISHED, AND WHY THE ANCHOR IS AMONG IT. `gate_started_at` is on the row because
every number here is relative to it: a reader who doubts a count can see what it was
counted up to, which is the property `severity.py` states for its own inputs. `source` is
NOT re-derived here -- it is T1's column, on T1's row, and this row joins to it on
`batch_id`; re-emitting it would be a second spelling of the job-name strip and of the
declaration behind it.
"""
from __future__ import annotations

from dataclasses import dataclass

from opl.bronze.reconcile import VERDICTS
from opl.config import DEFAULT, OplConfig
from opl.dataops.telemetry import TASK_TELEMETRY_VIEW
from opl.triage_agent.evidence import CENSUS_VERDICTS, NO_RECONCILIATION_ROW
from opl.triage_agent.incidents import DQ_GATE_TASK_KEY
from opl.triage_agent.severity import RECOMMENDED_ACTIONS, SEVERITIES

# The task whose run IS a gate execution, for counting purposes. NOT the gate task itself
# -- see the header's identity. One spelling, here, and the bundle lock in
# `tests/triage_agent/test_history_declaration.py` matches on this constant, so a rename
# that reached the bundle and not this file fails that test rather than shortening every
# history in the project.
HISTORY_TASK_KEY = "check_bad_rows"

# The two states a declared gate spelling can be in. `RETIRED` is not a synonym for
# "deleted": `dq_gate`'s five runs are still in the telemetry and still count as gate
# executions, which is the whole reason this declaration exists.
LIVE = "live"
RETIRED = "retired"


@dataclass(frozen=True, kw_only=True)
class GateSpelling:
    """One task name under which a DQ gate has run in this project. Frozen, keyword-only.

    `status` decides which direction the bundle lock holds for it -- a LIVE spelling must
    be consumed by a `check_bad_rows` condition task in the bundle, a RETIRED one by none.
    `why` is the record of what the name was and what happened to it, which is the fact a
    reader needs and the telemetry does not carry: no column anywhere marks a task key as
    superseded."""

    status: str
    why: str


# THE GATE SPELLINGS THIS PROJECT HAS RUN, DECLARED. Read by three consumers and they are
# named here because this repository has deleted a constant that had none: the import-time
# guards below, the bundle lock in `test_history_declaration.py`, and
# `test_the_two_gate_spellings_sum_to_the_history_key` in `test_history.py`, which is
# where the identity in the header is executed rather than quoted.
GATE_SPELLINGS: dict[str, GateSpelling] = {
    "dq_gate": GateSpelling(
        status=RETIRED,
        why=(
            "the lookup's whole-table gate, retired mid-project when the lookup inherited "
            "batch scoping -- `databricks/src/dq_gate_batch.py`'s own docstring records "
            "the migration. Five job runs, all 2026-07-24, all on opl-bronze-cnpj-lookup, "
            "and they are still in the telemetry under this name with nothing marking it "
            "superseded. Three of the eleven incidents sit inside those five runs, so a "
            "history that does not know this name reports 0 prior executions for all "
            "three where the truth is 4, 3 and 1 (docs/f6-run-evidence.md 0.8, 0.10)"
        ),
    ),
    "dq_gate_batch": GateSpelling(
        status=LIVE,
        why=(
            "the batch-scoped gate every bronze job runs today, 24 job runs across all "
            "seven jobs from 2026-07-27. It publishes `bad_row_count`, which is what the "
            "`check_bad_rows` condition task consumes -- the wiring that makes that task "
            "total over gate runs under either spelling"
        ),
    ),
}

# HOW MANY PRIOR EXECUTIONS THE SPEC ASKS FOR. Declared, so the number a reading is short
# OF is the same number in the query and in the word -- and published on every row, so a
# consumer never has to know it. Five because the spec says "the last N executions" and
# five is the figure the plan carries; it is not tuned, and moving it moves the reading
# rather than the counts, which is why `history_sql` takes it as a keyword.
#
# EXACTLY ONE of this workspace's eleven incidents has five prior gate runs.
N_EXECUTIONS = 5

HISTORY_COMPLETE = "history_complete"
INSUFFICIENT_HISTORY = "insufficient_history"
NO_PRIOR_EXECUTION = "no_prior_execution"
GATE_RUN_ABSENT = "gate_run_absent"

# First-match-wins, like `severity._SEVERITY_LADDER` and `reconcile._VERDICT_LADDER`. THE
# ARMS ARE NOT DISJOINT, AND THE ORDER DECIDES ONE PAIR OF THEM WHILE BEING VACUOUS FOR THE
# THIRD -- which is separated here because the two are easy to file under one sentence and
# only one of them is a claim about ORDER at all.
#
#   * ORDER, AND IT DECIDES THE ANSWER: `NO_PRIOR_EXECUTION` stands before
#     `INSUFFICIENT_HISTORY` because zero prior executions satisfies "fewer than N" as
#     well. Swap the two arms and a table that has never been gated reads
#     `insufficient_history` -- the collapse the header refuses, arriving through arm
#     order rather than through the vocabulary.
#   * PRESENCE, AND NOT POSITION: `GATE_RUN_ABSENT` reads `found`, which no other arm
#     touches, against counts that are NULL -- `NULL = 0` and `NULL < 5` match nothing --
#     so moving it last changes no answer. What is load-bearing is that the arm EXISTS:
#     delete it and the row falls to the ELSE and reads `history_complete`, which is the
#     header's paragraph on three-valued logic and is what
#     `test_a_batch_with_no_gate_run_reads_absent_and_not_complete` fires on.
#
# `HISTORY_COMPLETE` is the ELSE and is therefore not in the tuple. NO TEST PERMUTES THESE
# ARMS: the order claim above is read off SQL's first-match rule rather than executed, and
# what is executed is each arm's presence.
_READING_LADDER = (
    (GATE_RUN_ABSENT, "found IS NULL"),
    (NO_PRIOR_EXECUTION, "prior_executions = 0"),
    (INSUFFICIENT_HISTORY, "prior_executions < {executions}"),
)

# The closed vocabulary this module publishes, built FROM the ladder so the two cannot
# disagree -- `severity.py`'s shape and its reason.
HISTORY_READINGS = (*(name for name, _ in _READING_LADDER), HISTORY_COMPLETE)


def live_gate_spellings() -> tuple[str, ...]:
    """The gate task names the bundle is expected to declare today, sorted."""
    return tuple(sorted(n for n, gate in GATE_SPELLINGS.items() if gate.status == LIVE))


def retired_gate_spellings() -> tuple[str, ...]:
    """The gate task names that must appear in NO job YAML, sorted.

    Their runs are in the telemetry and not in the bundle, which is the asymmetry the lock
    has to hold: equality in both directions would fail on a name that is real history."""
    return tuple(sorted(n for n, gate in GATE_SPELLINGS.items() if gate.status == RETIRED))


def history_case_sql(executions: int = N_EXECUTIONS) -> str:
    """The reading ladder as one CASE expression. Spelled once, here.

    `executions` is interpolated into a predicate, so `_require_a_window` runs first --
    the same discipline `evidence._require_a_bound` states for its LIMIT."""
    _require_a_window(executions)
    arms = "\n    ".join(
        f"WHEN {predicate.format(executions=executions)} THEN '{name}'"
        for name, predicate in _READING_LADDER
    )
    return f"CASE\n    {arms}\n    ELSE '{HISTORY_COMPLETE}'\n  END"


def _prior_runs_sql() -> str:
    """The prior gate runs of this incident's job, folded to one row per job run.

    `SELECT DISTINCT` AND NOT A `COUNT(DISTINCT ...)` LATER: the fold happens before
    anything counts, so both numbers below are taken over the same folded set and the
    `gated_runs` join cannot fan one of them out behind the other's back.

    The two predicates are the header's, and both are load-bearing: the identity is the
    only thing excluding this incident's own gate run from its own history."""
    return (
        "  SELECT DISTINCT g.job_run_id\n"
        "  FROM gate_runs g JOIN own_gate o ON g.job_id = o.job_id\n"
        "  WHERE g.job_run_id <> :batch_id\n"
        "    AND g.started_at <= o.gate_started_at"
    )


def _counted_sql() -> str:
    """The two numbers, side by side, over the folded prior runs.

    `COUNT(*)` is the prior runs and `COUNT(gated.job_run_id)` is those of them that also
    fired the gate -- counted from the PRESENCE of a `fail_on_dq` row, never from a
    terminal state, which is defect 3 in the header. A LEFT JOIN rather than a semi-join
    because both numbers come off one scan, and its right side is already folded, so it
    cannot multiply the left."""
    return (
        "  SELECT COUNT(*) AS prior_executions,\n"
        "    COUNT(gated.job_run_id) AS prior_incidents\n"
        "  FROM prior_runs p LEFT JOIN gated_runs gated\n"
        "    ON gated.job_run_id = p.job_run_id"
    )


def _reading_sql(relation: str) -> str:
    """The CTE chain: the gate runs, this incident's own, the prior ones, and the counts.

    `asked LEFT JOIN own_gate ON true` IS `evidence.reconciliation_sql`'s SHAPE AND ITS
    REASON: the row exists because a caller asked about a batch, not because the telemetry
    had one, so a batch with no gate run comes back as one row saying so instead of as no
    rows at all. `found` is what the ladder reads to tell the two apart.

    `own_gate` GROUPS BY `job_id` RATHER THAN `MAX()`-ING IT, which is `incidents.py`'s
    choice for its reason: a `job_run_id` belongs to one job, so the two spellings agree on
    every input and differ in how they FAIL -- grouping returns two rows and breaks the
    one-row-per-incident property a reader can check."""
    return (
        "asked AS (\n  SELECT :batch_id AS batch_id\n),\n"
        "gate_runs AS (\n"
        "  SELECT job_run_id, job_id, started_at\n"
        f"  FROM {relation}\n"
        f"  WHERE task_key = '{HISTORY_TASK_KEY}'\n"
        "),\n"
        "own_gate AS (\n"
        "  SELECT job_id, MIN(started_at) AS gate_started_at, TRUE AS found\n"
        "  FROM gate_runs\n"
        "  WHERE job_run_id = :batch_id\n"
        "  GROUP BY job_id\n"
        "),\n"
        f"prior_runs AS (\n{_prior_runs_sql()}\n),\n"
        "gated_runs AS (\n"
        "  SELECT DISTINCT job_run_id\n"
        f"  FROM {relation}\n"
        f"  WHERE task_key = '{DQ_GATE_TASK_KEY}'\n"
        "),\n"
        f"counted AS (\n{_counted_sql()}\n)"
    )


def history_sql(
    view: str | None = None,
    config: OplConfig = DEFAULT,
    *,
    executions: int = N_EXECUTIONS,
) -> str:
    """One incident's comparison baseline. The query, spelled once, bound on `:batch_id`.

    ONE STATEMENT, ONE BINDING -- `args={"batch_id": ...}`, which is the signature every
    statement in this package already takes, so a caller holding a T1 record can run this
    beside `evidence_sql`'s three without re-labelling anything.

    `view=` IS T1's SEAM INHERITED AND NOT A NEW ONE. It defaults to the F4 view this
    project deploys, which `config` locates; it exists so the query is asserted by
    something before a workspace run rather than after one. It is NOT called `source`:
    that word is the registry-key column in both F4 views and in T1's feed.

    THE COUNTS ARE NULLED BY `found`, NOT BY A COALESCE. A batch with no `check_bad_rows`
    run has no anchor, so there is nothing to count up to and nothing is counted; zero
    would be the answer a measurement gives and this is not one."""
    relation = view or config.table(TASK_TELEMETRY_VIEW)
    return (
        f"WITH {_reading_sql(relation)},\n"
        "reading AS (\n"
        "  SELECT a.batch_id, o.job_id, o.gate_started_at, o.found,\n"
        "    CASE WHEN o.found THEN c.prior_executions END AS prior_executions,\n"
        "    CASE WHEN o.found THEN c.prior_incidents END AS prior_incidents\n"
        "  FROM asked a LEFT JOIN own_gate o ON true LEFT JOIN counted c ON true\n"
        ")\n"
        "SELECT batch_id, job_id, gate_started_at,\n"
        f"  {executions} AS executions_requested,\n"
        "  prior_executions, prior_incidents,\n"
        f"  {history_case_sql(executions)} AS history\n"
        "FROM reading"
    )


def _require_a_window(executions: int) -> None:
    """N has to be a positive whole number, and it is written straight into a predicate.

    `bool` is excluded explicitly because `True == 1` in Python and `isinstance(True, int)`
    is True, so `executions=True` would otherwise build a query comparing against 1 and
    call ten of eleven incidents complete. `evidence._require_a_bound` refuses the same
    shape on its LIMIT for the same reason."""
    if not isinstance(executions, int) or isinstance(executions, bool) or executions < 1:
        raise ValueError(
            f"the comparison window {executions!r} is not a positive integer. It is "
            "interpolated into the reading's predicate, and a window of zero or less "
            "would make every incident's history complete without comparing anything"
        )


def _assert_every_gate_spelling_declares_a_status_and_a_reason() -> None:
    """A declared spelling has to say which side of the lock it is on, and why it is here.

    An unknown status is worse than a missing entry: the lock ranges over LIVE and RETIRED,
    so a third word silently puts the spelling in NEITHER direction -- present in the
    declaration, checked by nothing. An empty `why` loses the only record that a task key
    was superseded, which is the fact no telemetry column carries."""
    for name, gate in sorted(GATE_SPELLINGS.items()):
        if gate.status not in (LIVE, RETIRED):
            raise ValueError(
                f"gate spelling {name!r} declares status {gate.status!r}, which is "
                f"neither {LIVE!r} nor {RETIRED!r}. The bundle lock holds one direction "
                "for each, so this entry would be checked in neither"
            )
        if not gate.why.strip():
            raise ValueError(
                f"gate spelling {name!r} declares no reason. `why` is the only record "
                "that this name is a gate at all -- no telemetry column marks a task key "
                "as superseded, which is how the last rename cost five runs"
            )


def _assert_a_live_gate_spelling_exists() -> None:
    """GUARD THE GUARD, and this is the direction the lock cannot see from inside itself.

    The bundle lock holds "every live spelling is consumed by a `check_bad_rows` condition
    task". Over an EMPTY set of live spellings that sentence is true and the lock reports
    green over nothing, which is this repository's most-hunted species -- so a declaration
    that retired every spelling it knows is refused here, at import, rather than passing a
    test that had nothing to range over."""
    if not live_gate_spellings():
        raise ValueError(
            "no gate spelling is declared live, so the bundle lock ranges over an empty "
            f"set and passes over any wiring at all. {sorted(GATE_SPELLINGS)} are "
            "declared and every one of them is retired"
        )


def _assert_the_task_keys_are_three_different_roles() -> None:
    """The gate, the condition task and the failure task are three roles, never two.

    The whole argument for `HISTORY_TASK_KEY` is that it is DOWNSTREAM of every gate
    spelling and UPSTREAM of the failure task -- 5 + 24 = 29 = 29. If a gate spelling were
    also the history key, the identity would compare a set with itself; if `fail_on_dq`
    were, `prior_incidents` would equal `prior_executions` on every input and the terminal
    state defect would be indistinguishable from the fix."""
    for role, key in sorted(
        {"the history key": HISTORY_TASK_KEY, "the incident key": DQ_GATE_TASK_KEY}.items()
    ):
        if key in GATE_SPELLINGS:
            raise ValueError(
                f"{key!r} is {role} AND a declared gate spelling. The history key is the "
                "condition task that CONSUMES a gate's verdict and the incident key is "
                "the task that runs on a failed one; a name serving two of those roles "
                "makes the count that proves the key safe a count against itself"
            )
    if HISTORY_TASK_KEY == DQ_GATE_TASK_KEY:
        raise ValueError(
            f"{HISTORY_TASK_KEY!r} is both the history key and the incident key, so every "
            "gate run would report as an incident and `prior_incidents` would equal "
            "`prior_executions` on every input"
        )


def _assert_no_reading_word_is_another_modules() -> None:
    """Every reading is one word, and none of them is a verdict, a census or a grade.

    TWO DIRECTIONS, AND THE SECOND IS WHY THIS IS NOT PARANOIA. Within this file, a
    duplicated word collapses two arms of the ladder into one that no output can tell
    apart. Across files, a reading that collided with `reconcile.py`'s verdict,
    `evidence.py`'s census word or `severity.py`'s grade would put ONE string on a row
    that already carries those columns, and a consumer formatting them could not tell
    which question it was reading the answer to. That is
    `severity._assert_no_grade_is_spelled_twice` applied to four more words, and it is
    stated there for this row's neighbours -- range included: the verdicts are
    `reconcile.VERDICTS`, so a fifth arm is checked here in the commit that adds it."""
    if len(set(HISTORY_READINGS)) != len(HISTORY_READINGS):
        raise ValueError(
            f"the history vocabulary spells a word twice ({list(HISTORY_READINGS)}), so "
            "two arms of the reading ladder are indistinguishable in every output"
        )
    borrowed = sorted(
        set(HISTORY_READINGS)
        & {
            *VERDICTS,
            NO_RECONCILIATION_ROW,
            *CENSUS_VERDICTS,
            *SEVERITIES,
            *RECOMMENDED_ACTIONS,
        }
    )
    if borrowed:
        raise ValueError(
            f"{borrowed} are history readings here AND words published on the same "
            "incident by `opl.bronze.reconcile`, `opl.triage_agent.evidence` or "
            "`opl.triage_agent.severity`, so one string would answer two questions"
        )


_assert_every_gate_spelling_declares_a_status_and_a_reason()
_assert_a_live_gate_spelling_exists()
_assert_the_task_keys_are_three_different_roles()
_assert_no_reading_word_is_another_modules()
