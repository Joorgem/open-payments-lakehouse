# src/opl/triage_agent/severity.py
"""How bad ONE incident is, and what a human should do about it. Two answers, never one.

WHAT THIS ANSWERS. `incidents.py` says WHICH job runs are being triaged and `evidence.py`
says what is in the workspace for one of them. This grades that evidence: a `severity`
with a rank, and a `recommended_action`, for a single incident named by T1's `source` and
`batch_id`. It reads; it never promotes, never repairs and never writes -- ADR 0018
Decision 3's rule inherited rather than rediscovered, one layer up.

SEVERITY AND RECOMMENDED ACTION ARE SEPARATE COLUMNS AND THAT IS THE WHOLE DESIGN. Fused,
they produce "2,000 rows, therefore HIGH, therefore repromote", which is exactly backwards
on this workspace's largest incident. Kept apart, each says one thing:

  * `severity`           -- how much is wrong with this batch, ordered, so eleven
                            incidents can be put in an order a triager works down.
  * `recommended_action` -- what a person should DO next. It is not derivable from the
                            severity and the corpus proves it in BOTH directions:
                            - SAME severity, DIFFERENT action: the three lookup incidents
                              and the two unexplained estabelecimentos ones are all
                              `evidence_removed`, and one set sends a reader to the
                              quarantine TABLE (it is empty for every batch) while the
                              other sends them to THIS BATCH (the table is populated and
                              only these rows are gone).
                            - DIFFERENT severity, SAME action: socios' 1,797 rejected rows
                              are `bulk_rejection` and empresas' 1 is `isolated_rejection`,
                              and both recommend reading the quarantined rows. Nothing is
                              lost in either; only the size differs.
                            - And the declared hold below changes the ACTION on
                              `592660596679630` while leaving its SEVERITY untouched. The
                              8,000 rows are still stranded; what the hold decides is that
                              nobody promotes them.

THERE IS NO `clean` SEVERITY AND THERE CANNOT BE ONE. Membership in T1's feed IS the
finding: `fail_on_dq` is reachable only through `check_bad_rows -> false`, so every
incident here had rejected rows at the instant the gate ran. The lowest severity this
module can emit is `isolated_rejection`, and the five incidents whose evidence has since
disappeared are two ranks ABOVE it. Absent evidence is not absent damage.

FOUR SEVERITIES, FIRST MATCH WINS, AND THE ORDER OF THE LADDER IS THE ARGUMENT.

  * `does_not_reconcile`  -- FIRST. `dataops_reconciliation` says this batch's three
                             counts do not add up: rows were staged and are in neither
                             bronze nor quarantine (`stranded_gated`,
                             `stranded_unexplained`), or bronze and quarantine together
                             hold more than staging ever did (`over_promoted`, where the
                             counts themselves cannot be trusted). This outranks any
                             number of cleanly rejected rows, because a rejected row is in
                             a table somebody can read and an unaccounted row is in none.
  * `evidence_removed`    -- the gate fired, so rows WERE rejected, and nothing in the
                             workspace can now say which. Five of eleven. `evidence.py`
                             argues the chain that makes this the only reading; the point
                             here is that it is neither clean nor lowest.
  * `bulk_rejection`      -- rows are in quarantine, the batch reconciles, and the
                             rejection is large enough to be a property of the batch's
                             population rather than a handful of rows.
  * `isolated_rejection`  -- the ELSE. Rows are in quarantine, the batch reconciles, and
                             the count is small.

THE VERDICT IS READ, NOT RE-DERIVED. The first arm keys on `dataops_reconciliation`'s
`verdict` column and NOT on `unaccounted > 0`, which would be a second spelling of
`reconcile._VERDICT_LADDER` -- the rule this repository polices hardest, and the one
`reconcile.py`'s own header states. It also gets `over_promoted` right for free:
that verdict makes `unaccounted` NEGATIVE, so an arithmetic re-derivation would have
graded a batch whose counts contradict themselves as the mildest thing this module emits.

THE ONE NUMBER IN THIS FILE, AND IT IS BORROWED RATHER THAN INVENTED. `bulk_rejection`
needs a line between "a handful of rows" and "a property of the population", and this
repository has argued in writing for exactly one reject-count line: ADR 0006's condition 2
for a per-reason DQ tolerance asks for monthly observations "with a reject count >= 10, so
the Poisson relative error falls under ~30%". Writing a second number here would be a
second spelling of "how many rejected rows is a lot", which is the defect this project has
paid for three times, so the number that already exists is the one used -- and it is held
equal to the ADR's own figure by `test_the_population_scale_threshold_is_the_number_ADR_
0006_argues_for`, which reads the figure out of the document rather than trusting this
paragraph.

AND THE POISSON ARGUMENT DOES **NOT** TRANSFER, WHICH THIS HEADER USED TO CLAIM IT DID.
ADR 0006's >= 10 bounds the relative error of a RATE ESTIMATE: a count sampled from one
month, used as the numerator of a per-reason reject rate. A quarantine census is not
sampled -- it is an exact count of the rows in a table -- and the refusal below removed
ratios from this module altogether, so there is no estimate here for a Poisson bound to be
about. What the borrow actually buys is a SCALE BOUNDARY WITH AN OWNER AND A DIFF: the one
place in this repository where somebody argued, in writing and with a mechanism, at
what size a reject count stops being a handful. That is a weaker claim than "the mechanism
transfers", and it is the true one.

**THIS IS THE PART OF THIS FILE TO ARGUE WITH**, and it is one constant with its reason
beside it, in the diff, rather than a number typed into a dashboard:
`opl.dataops.cadence`'s `_RFB_MONTHLY_DAYS` is the pattern -- that file splits its 45 into
31 days observed and 14 days judged, and names which half is which. This one is a borrow
end to end. WHAT NO TEST HERE CAN SAY is that 10 is the RIGHT boundary for a triage grade:
the lock holds the constant equal to a figure ADR 0006 derived for a different question,
and the corpus pins it only to the open interval (4, 1797].

WHAT SEVERITY DELIBERATELY DOES NOT READ, EACH FOR A MEASURED REASON:

  * `staged`, `promoted` and `quarantined` AS A RATIO. "What fraction of the batch was
    rejected" is the obvious severity metric and it is unavailable here: of the six
    reconciled corpus incidents, only `592660596679630`'s counts are the live ones. The
    test corpus (`tests/triage_agent/conftest.py`) sets socios to 1,800 staged against a
    live staging table of 55,830,826 rows, because those counts were chosen to make the
    batch reconcile rather than to reproduce a measurement. A ratio would therefore rank
    socios near 100% in the fixture and near 0% in the workspace -- a severity whose test
    asserts the opposite of what the deploy computes. `rejected_rows` is the corpus's own
    measured number and is what the ladder uses. The counts are still PUBLISHED beside the
    severity, because a classification whose inputs are not visible cannot be checked.
  * `attempts`. Every incident in this workspace carries exactly two, uniformly (T1's
    header measures it). A column that is the same on every input cannot discriminate, and
    ADR 0018's standing instruction is that a check whose output would be identical
    whatever happened is not a check.
  * THE REJECT REASON'S REPAIRABILITY CLASS. ADR 0018 Decision 5 does split this corpus
    three ways -- 4 rows whose bytes are irreparable, 3,585 that are a fact about the
    world, 2,000 whose repair is a contract v2 -- and a severity keyed on it would be a
    fourth declaration of a per-reason property in a project whose ADR 0006 refuses
    per-reason policy. It is left for the module that drafts the issue text, where a
    reason's class is prose rather than a grade.

SIX RECOMMENDED ACTIONS, FIRST MATCH WINS, AND EVERY ONE OF THEM IS SOMETHING A PERSON
DOES. Nothing here runs.

  * `hold_do_not_promote`             -- a hold is DECLARED for this batch. First, and it
                                         outranks everything, exactly as
                                         `paused_by_decision` does in `dataops_freshness`.
  * `promote_the_clean_rows`          -- the batch is stranded and no hold is declared.
                                         The command is the `remedy` column, which
                                         `dataops_reconciliation` already spells.
  * `investigate_the_counts`          -- `over_promoted`. There is no remedy for it and
                                         `reconcile.remedy_sql` emits NULL, correctly: a
                                         repromote is the last thing a batch that already
                                         holds too many rows needs.
  * `investigate_the_missing_batch`   -- the quarantine holds OTHER batches' rows and none
                                         of this one's. Something removed exactly these.
  * `investigate_the_quarantine_table`-- the quarantine holds no rows at all, so whatever
                                         removed this batch removed everything and the
                                         batch is not singled out. The two are different
                                         investigations and `evidence.py` argues at length
                                         why folding them lets the second borrow the
                                         first's explanation.
  * `review_the_quarantined_rows`     -- the ELSE. The gate did its job, nothing is lost,
                                         and a human reads `row_shapes` or `row_sample`.

NEITHER INVESTIGATION WORD ENCODES F4's ACCOUNT OF THE LOOKUP TABLE, and that is
deliberate. `evidence.py` records that which of the two absence words an incident gets is
a fact about the quarantine table TODAY and not a durable label -- the three lookup
incidents flip the moment any later run puts one reject in that table. So these actions
say WHERE TO LOOK, which follows from today's table state, and say nothing about whether
somebody has already explained it.

THE REMEDY IS PASSED THROUGH AND NEVER RE-SPELLED. `dataops_reconciliation.remedy` already
carries the exact `repromote_triaged_batch` invocation with the batch id in it, and
`reconcile.py`'s comment explains why `$(git rev-parse HEAD)` is left unexpanded in it.
This module emits that column unchanged. A held incident therefore publishes the remedy
BESIDE `hold_do_not_promote`, which is not a contradiction but the house standard stated
in F4's own words: "The command stays printed by the view, and nothing automated will ever
run it."

THE DECLARED HOLD, AND WHY IT IS A DECLARATION RATHER THAN A DERIVATION. The largest
incident in this workspace is `592660596679630` and its correct recommendation is DO NOT
PROMOTE. Nothing in the data says so -- every column this module reads says "stranded, and
here is the command that fixes it". The reason is a decision a person took and recorded,
so it ships the way `opl.dataops.cadence` ships a paused ingest: as data, in the
repository, carrying the citation, in the diff. ADR 0018 Decision 2 states the principle
for the freshness case -- "a metric that cannot tell 'deliberately not ingested' from
'ingest broken' has the defect it was built to avoid, moved one level down" -- and this is
the same sentence with two words changed: deliberately not PROMOTED against promotion
BROKEN.

AND THE HOLD IS NON-VACUOUS, WHICH IS ASSERTED RATHER THAN ASSUMED.
`tests/triage_agent/test_severity.py::test_removing_the_declared_hold_flips_the_recommenda
tion_on_that_batch` runs the same incident over the same data with
`HOLDS` emptied and requires the recommendation to become `promote_the_clean_rows`. A hold
that changed nothing when deleted would be decoration, and the recommendation would be
coming from somewhere this file does not name.

THE `why` CARRIES THE DECISIVE ARGUMENT AND NOT THE WEAK ONE. `docs/f4-run-evidence.md`
1.2 rejects the weak one by name: that the 2,000 rows are F1b's injected schema drift, so
promoting them spoils an experiment -- "a preference, not a mechanism, and this project
does not decide on preferences". The decisive argument is arithmetic and it is what the
note below spells out.

THE HOLD IS KEYED ON THE BATCH ID ALONE, AND `Hold.source` IS NOT A FILTER. Filtering the
map by the spec being triaged would make a hold whose declared table is wrong fail OPEN --
silently absent, and the recommendation flips back to "promote" in the one direction that
moves rows. Keyed on the batch id, a mis-declared table costs a wrong label on a row of a
quarantine that holds nothing for that batch anyway. `source` is checked at import against
`REGISTRY` and locked to the corpus by a test; it is what tells a reader which table the
decision is about.

THE CLASSIFICATION IS SPELLED ONCE, IN SQL, over the two statements `evidence_sql`
publishes. Three reasons, and the first is the house rule rather than a preference:

  1. `opl.bronze.reconcile` states it -- the verdict is spelled once, in SQL, because this
     repository has paid for two spellings of a sentinel, of a month and of a prefix.
     `reconcile`, `dq`, `freshness` and `evidence` all put their ladder in one CASE.
  2. The alternative is not "a Python ladder INSTEAD of SQL", it is "a Python ladder that
     runs after the SQL has already been executed and collected". The evidence T2
     publishes is SQL; a Python grader would need a driver that pulls those rows back
     before it can grade, which puts the classification a round trip away from the data
     and out of reach of anything that reads the workspace directly.
  3. Composition. T4 (last-N history), T5 (blast radius) and T6 (issue text) each add
     columns to an incident. A severity that is a column joins; a severity that is a
     function call has to be re-run by every consumer, and the day two consumers disagree
     about which one they ran is the day this becomes a second spelling anyway.

WHAT THAT COSTS AND IT IS PAID KNOWINGLY: eleven rows are not a scale that needs SQL, and
a Python ladder would be easier to unit-test. The tests here run the SQL against real
tables for `reconcile.py`'s reason -- an arm nothing can enter is a grade that will never
be wrong -- so the testability argument buys less than it looks like it does.

THE STATEMENT IS PER-INCIDENT AND TAKES T2's BINDING UNCHANGED: one `:batch_id`, used by
both wrapped statements, and `source` projected as a literal from the resolved spec. It
does NOT re-read T1's feed. The feed's contribution to this statement is the two values
that name the incident, which the caller already holds; joining the telemetry view again
to recover `attempts` would re-read a view for a column the ladder is documented above as
refusing to use.
"""
from __future__ import annotations

from dataclasses import dataclass

from opl.bronze.reconcile import (
    OVER_PROMOTED,
    RECONCILED,
    STRANDED_GATED,
    STRANDED_UNEXPLAINED,
)
from opl.bronze.registry import REGISTRY
from opl.config import DEFAULT, OplConfig
from opl.dataops.freshness import sql_string_literal
from opl.triage_agent.evidence import (
    EVIDENCE_MISSING_BATCH_ABSENT,
    EVIDENCE_MISSING_QUARANTINE_EMPTY,
    NO_RECONCILIATION_ROW,
    ROWS_PRESENT,
    evidence_sql,
)

DOES_NOT_RECONCILE = "does_not_reconcile"
EVIDENCE_REMOVED = "evidence_removed"
BULK_REJECTION = "bulk_rejection"
ISOLATED_REJECTION = "isolated_rejection"

HOLD_DO_NOT_PROMOTE = "hold_do_not_promote"
PROMOTE_THE_CLEAN_ROWS = "promote_the_clean_rows"
INVESTIGATE_THE_COUNTS = "investigate_the_counts"
INVESTIGATE_THE_MISSING_BATCH = "investigate_the_missing_batch"
INVESTIGATE_THE_QUARANTINE_TABLE = "investigate_the_quarantine_table"
REVIEW_THE_QUARANTINED_ROWS = "review_the_quarantined_rows"

# THE ONE NUMBER IN THIS FILE, and the header argues where it comes from and which half of
# its argument does NOT transfer. ADR 0006 condition 2: "at least six monthly observations
# per table with a reject count >= 10, so the Poisson relative error falls under ~30%".
# Below it a rejection is a handful of rows; at or above it, it is the size the only
# written argument in this repository about reject counts was drawn at. Spelled once and
# used by one arm, so there is no second threshold to tune and no per-table variant to
# drift, and a test holds it equal to the figure the cited document carries.
_POPULATION_SCALE_ROWS = 10

# The three `reconcile.py` verdicts that are NOT `reconciled`, and the word for a batch the
# view has no row for is deliberately absent: `no_reconciliation_row` is a fact about the
# VIEW and the five incidents that carry it are graded by the evidence arm below, which is
# a fact about the workspace.
_UNRECONCILED_VERDICTS = (STRANDED_GATED, STRANDED_UNEXPLAINED, OVER_PROMOTED)

# The two census words that mean the rejected rows are gone. `rows_present` is the only
# census verdict that is not a finding, so it is the only one absent from this tuple.
_EVIDENCE_ABSENT = (EVIDENCE_MISSING_QUARANTINE_EMPTY, EVIDENCE_MISSING_BATCH_ABSENT)


def _in_list(values: tuple[str, ...]) -> str:
    """A tuple of declared words as a SQL `IN` list. Every value here is a module
    constant, never a row value, so there is nothing to escape and nothing to bind."""
    return ", ".join(f"'{value}'" for value in values)


# First-match-wins, like `reconcile._VERDICT_LADDER`, `freshness._STATUS_LADDER` and
# `dq._reject_reason`. The arms are NOT disjoint -- `592660596679630` satisfies the third
# as well as the first -- and the order is what decides, which the header argues arm by
# arm. `ISOLATED_REJECTION` is the ELSE and is therefore not in the tuple.
_SEVERITY_LADDER = (
    (DOES_NOT_RECONCILE, f"verdict IN ({_in_list(_UNRECONCILED_VERDICTS)})"),
    (EVIDENCE_REMOVED, f"evidence IN ({_in_list(_EVIDENCE_ABSENT)})"),
    (BULK_REJECTION, f"rejected_rows >= {_POPULATION_SCALE_ROWS}"),
)

# MOST SEVERE FIRST, so the index in this tuple IS the rank and the two cannot disagree:
# `severity_rank_sql` builds a map from this very tuple rather than from a second list of
# numbers. 1 is the worst, so `ORDER BY severity_rank` puts the incident to work on first.
SEVERITIES = (*(name for name, _ in _SEVERITY_LADDER), ISOLATED_REJECTION)

# The hold arm is FIRST for `freshness._STATUS_LADDER`'s reason: a recorded decision
# outranks every derivation, or the derivation quietly overrules the person who took it.
_ACTION_LADDER = (
    (HOLD_DO_NOT_PROMOTE, "hold_note IS NOT NULL"),
    (
        PROMOTE_THE_CLEAN_ROWS,
        f"verdict IN ({_in_list((STRANDED_GATED, STRANDED_UNEXPLAINED))})",
    ),
    (INVESTIGATE_THE_COUNTS, f"verdict = '{OVER_PROMOTED}'"),
    (INVESTIGATE_THE_MISSING_BATCH, f"evidence = '{EVIDENCE_MISSING_BATCH_ABSENT}'"),
    (
        INVESTIGATE_THE_QUARANTINE_TABLE,
        f"evidence = '{EVIDENCE_MISSING_QUARANTINE_EMPTY}'",
    ),
)

RECOMMENDED_ACTIONS = (*(name for name, _ in _ACTION_LADDER), REVIEW_THE_QUARANTINED_ROWS)


@dataclass(frozen=True, kw_only=True)
class Hold:
    """One recorded decision NOT to act on a batch. Frozen, keyword-only.

    `source` is the registry key of the table the decision is about -- checked against
    `REGISTRY` at import, and NOT used to filter the lookup (the header says why the
    filtered spelling fails open). `why` is what the view prints beside the
    recommendation, and it has to carry the argument AND the citation: a hold nobody can
    trace to a decision is a hold the next operator re-litigates or removes."""

    source: str
    why: str


HOLDS: dict[str, Hold] = {
    # THE ENTRY THIS FILE WAS WRITTEN FOR, and the argument is arithmetic rather than
    # preference. `docs/f4-run-evidence.md` 1.2 records the decision, its owner and its
    # date, and rejects the tempting argument (the 2,000 rows are an experiment's other
    # half) as "a preference, not a mechanism". What is written here is the one that
    # survives: a promote makes a committed, documented number wrong by 8,000.
    "592660596679630": Hold(
        source="payments",
        why=(
            "do not promote -- recorded decision, not a fault: bronze_payments holds "
            "40,150 rows against fact_payment's 40,000, and that 150-row gap is "
            "documented at gold_load_fact.py:125-143 as the repeats deduplication "
            "removes. Promoting this batch's 8,000 clean rows makes bronze 48,150 "
            "against a fact still at 40,000, because the gold fact loader is append-only "
            "and refuses a target it did not write in the same run -- so the documented "
            "150 becomes an undocumented 8,150 and the rule that explains the gap stops "
            "explaining it (docs/f4-run-evidence.md 1.2)"
        ),
    ),
}


def _ladder_case_sql(ladder: tuple[tuple[str, str], ...], otherwise: str) -> str:
    """A first-match-wins ladder as one CASE expression. Both ladders use this.

    One builder for the two, so the severity ladder and the action ladder cannot drift
    into two shapes -- and so that adding an arm to either is one line of data."""
    arms = "\n    ".join(f"WHEN {predicate} THEN '{name}'" for name, predicate in ladder)
    return f"CASE\n    {arms}\n    ELSE '{otherwise}'\n  END"


def severity_case_sql() -> str:
    """The severity ladder as one CASE expression. Spelled once, here."""
    return _ladder_case_sql(_SEVERITY_LADDER, ISOLATED_REJECTION)


def recommended_action_case_sql() -> str:
    """The action ladder as one CASE expression. Spelled once, here."""
    return _ladder_case_sql(_ACTION_LADDER, REVIEW_THE_QUARANTINED_ROWS)


def severity_rank_sql(column: str = "severity") -> str:
    """The rank of an already-computed severity, as a map lookup over `SEVERITIES`.

    A LOOKUP ON THE WORD AND NOT A SECOND CASE LADDER. A parallel ladder emitting integers
    would be the same rule spelled twice, and the failure it invites is silent: the two
    agree on every input until somebody reorders one of them. Here the order of
    `SEVERITIES` is the only place a rank exists, and `SEVERITIES` is built from
    `_SEVERITY_LADDER` -- so the ladder's own order is the rank, and a reordered ladder
    reorders the ranks with it.

    `element_at` returns NULL for a word not in the map. That cannot happen for a severity
    this module emits -- `SEVERITIES` is the map AND the ELSE of the ladder is in it -- and
    it is worth being plain that if it ever did, the answer would be SILENT rather than
    loud: an unranked incident sorts wherever the consumer's `ORDER BY` puts NULLs, which
    on Spark is first ascending -- MEASURED on local pyspark 3.5.9, `ORDER BY r` over
    `(NULL, 1, 2)` returning `[NULL, 1, 2]` ascending and `[2, 1, NULL]` descending, and
    ASSERTED BY NO TEST HERE. Nothing here guards it either, because nothing here can
    reach it; the guard that keeps it unreachable is `_assert_no_grade_is_spelled_twice`,
    which refuses the one way `SEVERITIES` could lose a word it emits."""
    pairs = ", ".join(f"'{name}', {rank}" for rank, name in enumerate(SEVERITIES, start=1))
    return f"element_at(map({pairs}), {column})"


def hold_note_sql(batch_id: str = ":batch_id") -> str:
    """The declared holds as a SQL map lookup keyed by batch id. NULL for every other batch.

    A MAP LITERAL AND NOT A CASE LADDER, which is `incidents.table_of_job_sql`'s shape and
    its reason: the arms of a ladder are ordered and can overlap, and this is a lookup with
    one answer or none. `sorted` puts it in batch order so re-ordering the declaration is
    not a diff in generated SQL.

    THE NOTE IS ENGLISH PROSE, so it goes through
    `opl.dataops.freshness.sql_string_literal` rather than an f-string. That function
    exists because `''` is not an escape on Spark -- it ends the literal, the apostrophe is
    DELETED, the statement parses and nothing reports a problem -- and its own docstring
    says anyone building SQL out of English text should reach for it.
    `test_a_hold_note_carrying_an_apostrophe_and_a_backslash_survives_the_round_trip` runs
    the escape rather than reading it, because every failure mode of the wrong spelling is
    silent. An empty declaration emits a typed NULL rather than `element_at(map())`, which
    Spark cannot type.

    THE KEY GOES THROUGH THE SAME FUNCTION AS THE NOTE, and it is declared data either
    way: `HOLDS` is keyed by batch ids, which are digits. It is escaped anyway because
    this file argues about escaping for a living, and one raw interpolation beside an
    escaped one reads as a considered exemption rather than as the absence of a reason."""
    if not HOLDS:
        return "CAST(NULL AS STRING)"
    pairs = ", ".join(
        f"{sql_string_literal(batch)}, {sql_string_literal(hold.why)}"
        for batch, hold in sorted(HOLDS.items())
    )
    return f"element_at(map({pairs}), {batch_id})"


def _graded_sql(census: str, reconciliation: str) -> str:
    """T2's two publishable statements, folded to one row and joined. The plumbing.

    THE CENSUS IS SUMMED, NOT READ ROW BY ROW: `quarantine_census_sql` publishes one row
    per reject reason, and severity is a property of the INCIDENT. `evidence` and
    `quarantine_table_rows` are GROUPED rather than aggregated with `MIN` -- they are
    constant across a census by construction (a batch with rejected rows cannot also be
    reported missing), and grouping means that if they ever were not, this returns two
    rows and breaks the one-row-per-incident property a reader can check, instead of
    silently labelling the incident with whichever value sorted first. `incidents.py`
    makes the same choice for the same reason.

    `JOIN ... ON true` AND NOT ON THE KEY. Both sides project the same bound `:batch_id`,
    so a key join would be a value compared against itself; what actually has to hold is
    that each side returns exactly one row, and `ON true` renders a violation as extra
    rows rather than as zero. Both sides are built to return one row on every input --
    that is `evidence.py`'s central property and it is asserted there."""
    return (
        f"WITH census AS (\n{census}\n),\n"
        "counted AS (\n"
        "  SELECT source, batch_id, evidence, quarantine_table_rows,\n"
        "    SUM(rejected_rows) AS rejected_rows\n"
        "  FROM census\n"
        "  GROUP BY source, batch_id, evidence, quarantine_table_rows\n"
        "),\n"
        f"reconciliation AS (\n{reconciliation}\n),\n"
        "joined AS (\n"
        "  SELECT c.source, c.batch_id, c.rejected_rows, c.quarantine_table_rows,\n"
        "    c.evidence, r.staged, r.promoted, r.quarantined, r.unaccounted,\n"
        "    r.verdict, r.remedy,\n"
        f"    {hold_note_sql()} AS hold_note\n"
        "  FROM counted c JOIN reconciliation r ON true\n"
        ")\n"
        f"SELECT *, {severity_case_sql()} AS severity FROM joined"
    )


def severity_sql(
    source: str | None, config: OplConfig = DEFAULT, *, view: str | None = None
) -> str:
    """One incident's severity, rank and recommended action. The query, spelled once.

    `source` IS T1's COLUMN, taken rather than re-derived, and it is `evidence_sql` that
    resolves it -- so a NULL `source` (a DQ gate on a job `TABLE_OF_JOB` does not declare)
    raises `UnknownTable` here with that module's message instead of being graded as an
    incident with no evidence.

    `view=` IS T2's SEAM INHERITED, not a new one: it points `reconciliation_sql` at a
    relation a test controls, which is how the three verdicts this workspace has never
    produced are driven through the ladder at all.

    THE INPUTS ARE PUBLISHED BESIDE THE OUTPUTS -- `rejected_rows`, `evidence`, `verdict`
    and the four counts all reach the result row. A grade whose inputs are not on the row
    beside it cannot be checked against the facts it was computed from, which is the
    property `evidence.py` refuses to give up by grading nothing itself."""
    statements = evidence_sql(source, config, view=view)
    graded = _graded_sql(statements["census"], statements["reconciliation"])
    return (
        f"WITH graded AS (\n{graded}\n)\n"
        f"SELECT *, {severity_rank_sql()} AS severity_rank,\n"
        f"  {recommended_action_case_sql()} AS recommended_action\n"
        "FROM graded"
    )


def _assert_every_hold_names_a_registered_table_and_carries_a_reason() -> None:
    """A hold has to be about a table this project has, and has to say why.

    A hold naming an unregistered table is a decision about nothing: no incident can carry
    that `source`, so the entry sits in the map looking like a control and holds no batch.
    An empty `why` is worse than no hold at all -- the recommendation would say "do not
    promote" with nothing a reader can follow, which is the state `cadence.py`'s own guard
    refuses for the same reason: a status nobody can trace to a decision is a status
    somebody removes."""
    for batch, hold in sorted(HOLDS.items()):
        if hold.source not in REGISTRY:
            raise ValueError(
                f"batch {batch} declares a hold on {hold.source!r}, which is not a "
                f"registered bronze table ({sorted(REGISTRY)}). No incident can carry "
                "that source, so this hold can never fire and the batch it was written "
                "for would be recommended for promotion"
            )
        if not hold.why.strip():
            raise ValueError(
                f"batch {batch} declares a hold with no reason. `why` is what the "
                "recommendation prints beside `hold_do_not_promote`, and a refusal to "
                "act that cites nothing is one the next operator deletes"
            )


def _assert_no_grade_is_spelled_twice() -> None:
    """Every severity and every action is one word, and none of them is another module's.

    TWO DIRECTIONS, AND THE SECOND IS THE ONE THAT BITES. Within this file, a duplicated
    severity collapses two ranks into one silently, because `severity_rank_sql` builds its
    map from `SEVERITIES` and a repeated key simply wins. Across files, a severity that
    collided with a `reconcile.py` verdict or an `evidence.py` census word would put one
    string on a row that already carries both of those columns, and a consumer joining or
    formatting them could not tell which question it was reading the answer to. That is
    the same requirement `evidence._assert_the_absence_word_is_not_a_reconciliation_
    verdict` states for its own word, applied to eight more."""
    for kind, words in (("severity", SEVERITIES), ("recommended action", RECOMMENDED_ACTIONS)):
        if len(set(words)) != len(words):
            raise ValueError(
                f"the {kind} vocabulary spells a word twice ({list(words)}), so two arms "
                "of one ladder are indistinguishable in every output"
            )
    borrowed = sorted(
        set(SEVERITIES + RECOMMENDED_ACTIONS)
        & {
            RECONCILED,
            STRANDED_GATED,
            STRANDED_UNEXPLAINED,
            OVER_PROMOTED,
            NO_RECONCILIATION_ROW,
            ROWS_PRESENT,
            *_EVIDENCE_ABSENT,
        }
    )
    if borrowed:
        raise ValueError(
            f"{borrowed} are graded words here AND verdicts published on the same row by "
            "`opl.bronze.reconcile` or `opl.triage_agent.evidence`, so one string would "
            "answer two different questions on one incident"
        )


_assert_every_hold_names_a_registered_table_and_carries_a_reason()
_assert_no_grade_is_spelled_twice()
