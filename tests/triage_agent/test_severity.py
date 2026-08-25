"""The grade: a severity, its rank, and a recommended action, driven out of real tables.

SPLIT AT A SPARK SEAM WHEN THIS FILE REACHED 798 OF THE 800-LINE CAP, in a commit free of
behaviour: no assertion was edited, nothing renamed, and the two files hold the tests this
one held. What stays here is everything that takes `probe` and RUNS the statement over
tables. `test_severity_declaration.py` is the other half -- the two document locks, the
declaration's guards and the proof that they fire at import, and what the graded statement
may publish against what it must refuse to read. That half starts no JVM, and NOTHING
ENFORCES THAT: its own header says so rather than leaving it implied. `_HELD_BATCH` is the
only name the UNDIVIDED file defined that both halves read; it moved to `conftest.py` beside
the `_PAYMENTS_BATCH` it aliases, because a second definition here is exactly the drift the
comment beside it was written to prevent. Every other MODULE-LEVEL name live in both halves
is IMPORTED by both, not defined by either.

WHY EVERY TEST HERE RUNS THE SQL RATHER THAN MATCHING IT. `opl.bronze.reconcile` states the
reason and this file inherits it: an arm nothing can enter is a grade that will never be
wrong. All four severities and ALL SIX recommended actions are reached out of the `probe`
corpus or a relation handed through `view=`, so the ladders are exercised rather than read.

THAT SENTENCE READ "five of the six" FROM THE DAY IT WAS WRITTEN, AND IT WAS WRONG IN THE
ONE DIRECTION NOBODY AUDITS: it UNDERSTATED. A header that undersells its own coverage
costs a reader nothing and trips nothing, so it survived T3's review and this file's split
and was caught only when a correction pass was asked to CHECK it rather than to carry it.

~~AND NOTHING ASSERTS EITHER COUNT.~~ RETRACTED, AND IT WAS FALSE ELEVEN LINES ABOVE THE
ASSERTION THAT REFUTES IT. `test_the_rank_and_the_word_are_one_ladder_and_cannot_disagree`
grades its own five incidents and closes with `{severity for row} == set(_EXPECTED_RANKS)`,
so the FOUR is asserted; and because the line above it holds `tuple(_EXPECTED_RANKS) ==
SEVERITIES`, a FIFTH severity added to the module turns that test red too. **The severity
half is total.**

THE ACTION HALF IS NOT, AND THAT ASYMMETRY IS THE WHOLE OF WHAT IS UNGUARDED HERE. The
same test only checks `recommended_action in RECOMMENDED_ACTIONS`, which is membership and
not coverage. What holds the SIX up is that each of the six is pinned by an equality on a
`_graded(...)` row somewhere in this file, so an action that stopped being reachable turns
a NAMED test red -- demonstrated, not assumed: making arm 2 of `_ACTION_LADDER`
unreachable fails `test_an_over_promoted_batch_is_graded_high...` on
`'review_the_quarantined_rows' != 'investigate_the_counts'`. **But a SEVENTH action added
to `RECOMMENDED_ACTIONS` and reached by nothing would leave every test in this repository
green**, because no line anywhere compares the reached set against that tuple. That is the
gap, it is one-directional, and `docs/f6-run-evidence.md` section 3 carries it.

THE CORPUS ALONE CANNOT SEPARATE TWO VERY DIFFERENT CLASSIFIERS, AND THAT IS WHY THERE IS
A CONSTRUCTED CASE. `592660596679630` is both the incident with the most rejected rows
(2,000) and the only stranding in this workspace, so a grader that read `rejected_rows` and
a grader that read `dataops_reconciliation.verdict` put the eleven incidents in the SAME
order. That is ADR 0018's species -- a check whose output is identical under two different
implementations -- arriving in the file written to grade them. `_DISAGREEMENT_VIEW` below
is INVENTED, is labelled as such, and makes the two signals disagree: a stranding of 8
rejected rows beside a clean rejection of 1,797. Only one ordering survives it.

WHAT THE CORPUS DOES CLOSE ON ITS OWN, so the constructed relations are not carrying the
whole file: socios (1,797) and estabelecimentos (4) are both `rows_present` and both
`reconciled`, so their different severities can only come from the size arm.

AND WHAT IT DOES NOT CLOSE, WHICH THE SENTENCE THAT USED TO STAND HERE CLAIMED IT DID. The
five incidents whose evidence is gone carry ZERO rejected rows and outrank both socios
batches -- which is NOT beyond a row-count grader, only beyond a MONOTONIC one. Arm 2 is
`rejected_rows = 0` by construction (`quarantine_census_sql` emits `rows_present` iff a
GROUP BY group exists, iff the count is at least one), so `{0 -> 2, >= 10 -> 3, else 4}`
reads the row count and nothing else and reproduces the corpus's ordering exactly. The
not-a-row-counter property therefore rests on ARM 1 -- the verdict -- alone.

AND THE CORPUS HIDES A SECOND CONFOUND BEHIND THE FIRST, SO ARM 2 HAS ITS OWN PAIR.
`docs/f6-run-evidence.md` 0.5: the five incidents with no quarantine evidence are the SAME
five with no reconciliation row, so arm 2 spelled `verdict = 'no_reconciliation_row'` ranks
all eleven identically to the shipped `evidence IN (...)` -- and `_DISAGREEMENT_VIEW`
cannot separate them either, because both of ITS incidents are found in it. `_CONFOUND_VIEW`
is the second constructed relation and it splits the two spellings in both directions.
"""
from __future__ import annotations

from contextlib import contextmanager

from opl.bronze.reconcile import OVER_PROMOTED, RECONCILED, STRANDED_GATED
from opl.triage_agent import severity as severity_module
from opl.triage_agent.evidence import (
    EVIDENCE_MISSING_BATCH_ABSENT,
    EVIDENCE_MISSING_QUARANTINE_EMPTY,
    NO_RECONCILIATION_ROW,
    ROWS_PRESENT,
)
from opl.triage_agent.severity import (
    BULK_REJECTION,
    DOES_NOT_RECONCILE,
    EVIDENCE_REMOVED,
    HOLD_DO_NOT_PROMOTE,
    HOLDS,
    INVESTIGATE_THE_COUNTS,
    INVESTIGATE_THE_MISSING_BATCH,
    INVESTIGATE_THE_QUARANTINE_TABLE,
    ISOLATED_REJECTION,
    PROMOTE_THE_CLEAN_ROWS,
    RECOMMENDED_ACTIONS,
    REVIEW_THE_QUARANTINED_ROWS,
    SEVERITIES,
    Hold,
    severity_sql,
)

from .conftest import (
    _CONFIG,
    _EMPRESAS_BATCHES,
    _ESTAB_BATCH,
    _ESTAB_UNEXPLAINED,
    _HELD_BATCH,
    _LOOKUP_BATCHES,
    _NULL_REASON_BATCH,
    _PAYMENTS_BATCH,
    _SOCIOS_BATCHES,
    _TWO_REASON_BATCH,
    _run,
)

# THE LADDER'S ORDER, RESTATED AS LITERALS rather than read back out of `SEVERITIES`: a
# test deriving the expected rank from the very tuple the SQL's map was built from asserts
# only that a map lookup is a map lookup, and `{severity} == set(SEVERITIES)` is order-blind
# on top of that. A REORDERED `SEVERITIES` moves every rank and leaves both green.
_EXPECTED_RANKS = {
    DOES_NOT_RECONCILE: 1,
    EVIDENCE_REMOVED: 2,
    BULK_REJECTION: 3,
    ISOLATED_REJECTION: 4,
}

# An apostrophe AND a backslash, in a note nothing else in this file asserts a token of.
# Both characters are needed: the SQL-standard doubling deletes the apostrophe, and the
# unescaped backslashes it leaves behind are then read as escapes by Spark's lexer.
_ROUND_TRIP_NOTE = r"don't promote -- see C:\notes\why.md\nsecond line"

# INVENTED, and labelled the way `conftest._MATRIX_BATCH` is. It exists because the corpus
# cannot express the one input that separates a size grader from a verdict grader: every
# stranding in this workspace is also its largest rejection. Handed to `reconciliation_sql`
# through `view=`, it restates socios' real reconciliation unchanged and gives merchant's
# 8 quarantined rows a stranding they do not have.
_DISAGREEMENT_VIEW = "reconciliation_with_a_small_stranding"


def _disagreement_rows() -> str:
    """Two reconciliation rows: a SMALL stranding and a LARGE clean rejection.

    merchant's `_TWO_REASON_BATCH` really holds 8 quarantined rows and really reconciles
    (8 staged, 0 promoted, 8 quarantined); only the verdict and the staging count are
    invented here, so the rejected-row count either grader reads is the fixture's own.
    socios' row is its real one, restated so both incidents come out of ONE relation and
    "they were read from different views" is not an explanation for the difference."""
    return (
        "SELECT 'merchant' AS source, "
        f"'{_TWO_REASON_BATCH}' AS batch_id, 108 AS staged, 0 AS promoted, "
        f"8 AS quarantined, 100 AS unaccounted, '{STRANDED_GATED}' AS verdict, "
        "'databricks bundle run repromote_triaged_batch' AS remedy\n"
        "UNION ALL\n"
        f"SELECT 'socios', '{_SOCIOS_BATCHES[0]}', 1800, 3, 1797, 0, "
        f"'{RECONCILED}', CAST(NULL AS STRING)"
    )


# INVENTED, and labelled the way `_DISAGREEMENT_VIEW` is. It exists for the SECOND confound
# this file's header states, and it is ONE row that splits it in both directions.
_CONFOUND_VIEW = "reconciliation_that_names_only_the_lookup_batch"


def _confound_rows() -> str:
    """ONE reconciliation row, for a lookup incident, and it says `reconciled`.

    NOT AN ARBITRARY STATE. F4 records four gated batches later repromoted that now read
    `reconciled`; a lookup batch whose quarantine was recreated after its firing -- those
    incidents' actual history -- and whose staging rows were then repromoted is a workspace
    state this corpus happens not to contain, not one that cannot occur. The counts are the
    ones `reconciled` means. And socios is DELIBERATELY absent from the same relation, which
    is what gives it `no_reconciliation_row` while its 1,797 quarantined rows sit where they
    always were -- so one relation drives both directions and "they were read from different
    views" explains neither."""
    return (
        f"SELECT 'lookup' AS source, '{_LOOKUP_BATCHES[0]}' AS batch_id, "
        "5 AS staged, 5 AS promoted, 0 AS quarantined, 0 AS unaccounted, "
        f"'{RECONCILED}' AS verdict, CAST(NULL AS STRING) AS remedy"
    )


@contextmanager
def _temp_view(spark, name: str, body: str):
    """A temp view for one test's body, dropped even when its assertions fail: `probe` is
    session-scoped and outlives every test here, so a leaked relation would be visible to
    a `view=` seam that never asked for it."""
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {name} AS {body}")
    try:
        yield name
    finally:
        spark.catalog.dropTempView(name)


def _graded(spark, source: str, batch: str, *, view: str | None = None):
    """One incident's grade, run rather than asserted as text."""
    rows = _run(spark, severity_sql(source, _CONFIG, view=view), batch)
    assert len(rows) == 1, (
        f"{source}/{batch} produced {len(rows)} rows. One incident is one grade: the "
        "census is summed to the incident and the reconciliation returns a row present "
        "or absent, so anything else means one of those two properties has broken"
    )
    return rows[0]


def _grade(row) -> tuple[str, int, str]:
    return row["severity"], row["severity_rank"], row["recommended_action"]


# ----------------------------------------------------------------------------------
# 1. The declared hold, and the falsifier that makes it non-vacuous.
# ----------------------------------------------------------------------------------


def test_the_declared_hold_recommends_against_promoting_and_carries_its_citation(probe):
    """The phase's headline case, and the one a size grader gets exactly backwards.

    `592660596679630` is the largest incident in this workspace AND its recorded
    recommendation is do not promote. The severity says the batch is stranded -- which is
    true, and is what the data supports; the ACTION says the decision, and the note beside
    it carries the argument and the citation rather than only a verdict. The remedy stays
    printed, which is not a contradiction: `dataops_reconciliation` prints
    `repromote_triaged_batch` for every stranding and runs none of them, and F4's own
    sentence is "the command stays printed by the view, and nothing automated will ever
    run it"."""
    row = _graded(probe, "payments", _HELD_BATCH)

    assert _grade(row) == (DOES_NOT_RECONCILE, 1, HOLD_DO_NOT_PROMOTE)
    assert (row["verdict"], row["rejected_rows"]) == (STRANDED_GATED, 2000)
    assert "repromote_triaged_batch" in row["remedy"], "the remedy is printed, not run"

    note = row["hold_note"]
    assert note == HOLDS[_HELD_BATCH].why, (
        "the WHOLE declared note must reach the row, not a token of it. Every assertion "
        "below is apostrophe-free, so they all pass over a note the escaping mangled"
    )
    assert "docs/f4-run-evidence.md 1.2" in note
    assert "40,150" in note and "40,000" in note and "48,150" in note
    assert "gold_load_fact.py:125-143" in note
    assert "experiment" not in note, (
        "the note must carry the DECISIVE argument. f4-run-evidence.md 1.2 rejects the "
        "other one by name -- that promoting the rows spoils an experiment -- as 'a "
        "preference, not a mechanism, and this project does not decide on preferences'. "
        "This is a ONE-WORD ban and nothing more: the weak argument restated without that "
        "word walks straight through it, and no test here can see that"
    )


def test_removing_the_declared_hold_flips_the_recommendation_on_that_batch(probe, monkeypatch):
    """THE TEST THAT MAKES THE HOLD MEAN ANYTHING. Both arms, over the same data.

    A hold that changes nothing when it is deleted is decoration, and the recommendation
    would be coming from somewhere this repository does not name -- a batch id hard-coded
    in a ladder would pass the test above and fail this one. `cadence.py` pairs every
    refusal with a sibling that requires it to fire; this is that sibling for a
    declaration whose whole job is to CHANGE an answer.

    THE SEVERITY IS ASSERTED UNCHANGED ACROSS BOTH ARMS, and that is the separability this
    module exists for: the 8,000 rows are stranded whether or not anybody has decided what
    to do about them. The hold moves the action and nothing else."""
    held = _graded(probe, "payments", _HELD_BATCH)

    monkeypatch.setattr(severity_module, "HOLDS", {})
    released = _graded(probe, "payments", _HELD_BATCH)

    assert _grade(held) == (DOES_NOT_RECONCILE, 1, HOLD_DO_NOT_PROMOTE)
    assert _grade(released) == (DOES_NOT_RECONCILE, 1, PROMOTE_THE_CLEAN_ROWS), (
        "with the declaration removed the batch must earn what its stranded_gated verdict "
        "otherwise earns. If it still reads hold_do_not_promote, the recommendation is "
        "not coming from the declaration and the hold is decoration"
    )
    assert released["hold_note"] is None
    assert held["severity"] == released["severity"], "the hold is about the action, not the size"


def test_a_hold_note_carrying_an_apostrophe_and_a_backslash_survives_the_round_trip(
    probe, monkeypatch
):
    r"""THE ESCAPE, EXECUTED -- because every way of getting it wrong here is SILENT.

    `sql_string_literal` exists because `''` is not an escape on Spark: it ends the
    literal, the next begins, adjacent literals concatenate, and the apostrophe is DELETED
    with nothing raising. `CLAUDE.md` records the same for Databricks (`length('don''t')`
    is 4). Until this test every token asserted about the note was apostrophe-free, so
    `hold_note_sql` respelled with the SQL-standard doubling passed the whole file -- with
    the shipped note reaching the row as `...fact_payments 40,000...` and `...this batchs
    8,000...`.

    BOTH CHARACTERS, BECAUSE THE TWO SPELLINGS FAIL DIFFERENTLY, and both were run red.
    The doubling eats the apostrophe (`don't` returns as `dont`). Dropping only the
    backslash half leaves the apostrophe intact and returns this note as THREE lines:
    Spark's lexer reads `\n` as a newline and drops the backslash of `\w`, so
    `C:\notes\why.md\nsecond` comes back as `C:` / `oteswhy.md` / `second`. A note carrying
    only one of the two characters leaves the other failure invisible. The note is
    CONSTRUCTED and the declaration swapped for it, so the assertion is byte equality
    rather than a token search."""
    monkeypatch.setattr(
        severity_module, "HOLDS", {_HELD_BATCH: Hold(source="payments", why=_ROUND_TRIP_NOTE)}
    )
    row = _graded(probe, "payments", _HELD_BATCH)

    assert row["hold_note"] == _ROUND_TRIP_NOTE, (
        "the note must reach the row byte for byte. A shorter string means the apostrophe "
        "was eaten; a newline in it means a backslash was read as an escape"
    )
    assert _grade(row)[2] == HOLD_DO_NOT_PROMOTE, "and the hold must still fire"


# ----------------------------------------------------------------------------------
# 2. The corpus, graded. Eleven incidents, four severities, four actions.
# ----------------------------------------------------------------------------------


def test_severity_is_ordered_across_the_corpus_and_the_size_arm_is_load_bearing(probe):
    """2,000 / 1,797 / 4 / 1, and the ordering asserted rather than a bare inequality.

    THE PAIR THAT PROVES THE SIZE ARM IS DOING WORK is socios (1,797) against
    estabelecimentos (4): both are `rows_present`, both `reconciled`, and the only column
    that differs between them is the count. Raising `_POPULATION_SCALE_ROWS` above 1,797
    collapses them onto one severity and turns this red.

    4 AND 1 SHARE A SEVERITY AND THAT IS STATED RATHER THAN ENGINEERED AROUND. Separating
    them needs a threshold between 1 and 4, and nothing in this repository supports one --
    ADR 0006's is the only reject-count line this project has argued for, and it sits at
    10. A number invented to make a test show three values would be a SECOND spelling of
    "how many rejected rows is a lot", which is what this module's header refuses.
    `cadence.py` is not the precedent for refusing an invented number: it openly invents
    `_RFB_MONTHLY_DAYS = 45` and names which half of it is observed and which is judged.
    What it refuses is a number arriving without that split."""
    payments = _graded(probe, "payments", _PAYMENTS_BATCH)
    socios = _graded(probe, "socios", _SOCIOS_BATCHES[0])
    estab = _graded(probe, "estabelecimentos", _ESTAB_BATCH)
    empresas = _graded(probe, "empresas", _EMPRESAS_BATCHES[0])

    counts = tuple(row["rejected_rows"] for row in (payments, socios, estab, empresas))
    assert counts == (2000, 1797, 4, 1), "the corpus's own sizes, so the ranks below mean something"

    assert payments["severity"] == DOES_NOT_RECONCILE and payments["severity_rank"] == 1
    assert socios["severity"] == BULK_REJECTION and socios["severity_rank"] == 3
    assert estab["severity"] == ISOLATED_REJECTION and estab["severity_rank"] == 4
    assert empresas["severity"] == ISOLATED_REJECTION and empresas["severity_rank"] == 4
    assert payments["severity_rank"] < socios["severity_rank"] < estab["severity_rank"]
    assert estab["severity_rank"] == empresas["severity_rank"], (
        "4 rows and 1 row are the same triage response and are graded the same. This is "
        "an assertion about the design, not an accident: see the docstring"
    )


def test_the_second_socios_incident_is_graded_on_its_own_and_not_fused_with_the_first(probe):
    """1,786 is its own incident, three weeks from 1,797, and the grain is `(table, batch)`.

    `docs/f6-run-evidence.md` 0.3 flags this as the fusion hazard: the two socios firings
    share a reject reason, and 3,583 is the number a per-reason grader would report on
    both. The census column that carries 3,583 -- the whole-table count -- reaches this
    row and is deliberately NOT what the ladder reads."""
    second = _graded(probe, "socios", _SOCIOS_BATCHES[1])

    assert second["rejected_rows"] == 1786
    assert second["quarantine_table_rows"] == 3588, "both batches plus the invented matrix"
    assert _grade(second) == (BULK_REJECTION, 3, REVIEW_THE_QUARANTINED_ROWS)


def test_the_five_incidents_whose_evidence_is_gone_are_neither_clean_nor_the_lowest(probe):
    """Absent evidence is not absent damage, and this is the arm that says so.

    `fail_on_dq` is reachable only through `check_bad_rows -> false`, so the gate ran, rows
    WERE rejected, and zero rows in the quarantine today means the evidence was removed
    after the fact. There is no `clean` severity in this module for exactly that reason.

    THESE FIVE CARRY ZERO REJECTED ROWS AND OUTRANK BOTH SOCIOS BATCHES, WHICH IS LESS
    THAN THIS DOCSTRING USED TO CLAIM. It said no grader reading only `rejected_rows` could
    produce this ordering. False: arm 2 is `rejected_rows = 0` BY CONSTRUCTION -- the census
    emits `rows_present` iff a GROUP BY group exists, iff the count is at least one -- so
    `{0 -> 2, >= 10 -> 3, else 4}` reads nothing but the row count and produces exactly what
    is asserted below. The true claim is that no MONOTONIC row-count grader can.

    WHAT THAT COSTS: the not-a-row-counter property rests on ARM 1 alone, so the two
    constructed relations here -- `_DISAGREEMENT_VIEW` for arm 1, `_CONFOUND_VIEW` for arm
    2 -- are the tests carrying it, and this one is not."""
    graded = [
        _graded(probe, source, batch)
        for source, batch in (
            *(("lookup", batch) for batch in _LOOKUP_BATCHES),
            *(("estabelecimentos", batch) for batch in _ESTAB_UNEXPLAINED),
        )
    ]
    socios_rank = _graded(probe, "socios", _SOCIOS_BATCHES[0])["severity_rank"]

    assert len(graded) == 5
    for row in graded:
        assert row["rejected_rows"] == 0 and row["verdict"] == NO_RECONCILIATION_ROW
        assert row["evidence"] in (
            EVIDENCE_MISSING_QUARANTINE_EMPTY,
            EVIDENCE_MISSING_BATCH_ABSENT,
        )
        assert row["severity"] == EVIDENCE_REMOVED and row["severity_rank"] == 2
        assert row["severity"] != ISOLATED_REJECTION
        assert row["severity_rank"] < len(SEVERITIES), (
            "an incident whose evidence has disappeared must not be graded the mildest "
            "thing this module emits"
        )
        assert row["severity_rank"] < socios_rank


def test_an_over_promoted_batch_is_graded_high_and_is_never_recommended_for_a_repromote(probe):
    """The third verdict of `reconcile.py`'s ladder, reached out of the fixture as it stands.

    merchant's null-reason batch has 7 quarantined rows and no staging rows at all, so
    `batch_grain_sql` reads it `over_promoted` -- bronze and quarantine together hold more
    than staging ever did. `unaccounted` is NEGATIVE there, which is why the severity ladder
    keys on the VERDICT and not on `unaccounted > 0`: the arithmetic re-derivation would
    have graded a batch whose counts contradict themselves as the mildest thing here.

    And the action is not a repromote. `reconcile.remedy_sql` emits NULL for this verdict
    and this module does not invent one: a batch already holding too many rows is the last
    place to send a promote."""
    row = _graded(probe, "merchant", _NULL_REASON_BATCH)

    assert (row["verdict"], row["unaccounted"]) == (OVER_PROMOTED, -7)
    assert _grade(row) == (DOES_NOT_RECONCILE, 1, INVESTIGATE_THE_COUNTS)
    assert row["remedy"] is None and row["hold_note"] is None


# ----------------------------------------------------------------------------------
# 3. The two signals, made to disagree.
# ----------------------------------------------------------------------------------


def test_a_small_stranding_outranks_a_large_clean_rejection(probe):
    """SEVERITY IS NOT A PURE FUNCTION OF ROW COUNT, and the corpus alone cannot say so.

    Both incidents come out of ONE constructed relation, so the difference cannot be
    attributed to two different views. The rejected-row counts are the fixture's real ones
    -- 8 quarantined merchant rows against 1,797 quarantined socios rows -- and the
    ordering runs the other way: the 8-row stranding is rank 1 and the 1,797-row clean
    rejection is rank 3.

    A grader that read `rejected_rows` alone would invert this, and would agree with this
    one on all eleven live incidents, because the only stranding this workspace has is
    also its largest rejection."""
    with _temp_view(probe, _DISAGREEMENT_VIEW, _disagreement_rows()) as view:
        stranded = _graded(probe, "merchant", _TWO_REASON_BATCH, view=view)
        rejected = _graded(probe, "socios", _SOCIOS_BATCHES[0], view=view)

    assert (stranded["rejected_rows"], rejected["rejected_rows"]) == (8, 1797)
    assert _grade(stranded) == (DOES_NOT_RECONCILE, 1, PROMOTE_THE_CLEAN_ROWS)
    assert _grade(rejected) == (BULK_REJECTION, 3, REVIEW_THE_QUARANTINED_ROWS)
    assert stranded["severity_rank"] < rejected["severity_rank"], (
        "the smaller incident outranks the larger one because its rows are in no table. "
        "If this inverts, severity is being read off the row count"
    )
    assert stranded["rejected_rows"] < rejected["rejected_rows"]


def test_the_removal_WORD_and_not_the_absent_reconciliation_row_is_what_ranks_arm_two(probe):
    """SEVERITY ARM 2 KEYS ON `evidence`, AND NO LIVE INCIDENT CAN SHOW THAT.

    `docs/f6-run-evidence.md` 0.5: the five incidents whose quarantine evidence is gone are
    exactly the five `dataops_reconciliation` has no row for, so across all eleven
    `evidence IN (<the two removal words>)` and `verdict = 'no_reconciliation_row'` are one
    predicate -- and `_DISAGREEMENT_VIEW` cannot tell them apart either, both of its
    incidents being found in it. Mutating the shipped arm to the verdict spelling left this
    file green before this test and its sibling below.

    HERE THEY COME APART. The lookup batch IS in the constructed relation and reconciles, so
    the verdict spelling would not fire; its quarantine is still empty, so the evidence
    spelling does. The confound answers `isolated_rejection` at rank 4 -- the MILDEST grade
    this module emits, on an incident whose rejected rows have vanished."""
    with _temp_view(probe, _CONFOUND_VIEW, _confound_rows()) as view:
        row = _graded(probe, "lookup", _LOOKUP_BATCHES[0], view=view)

    assert (row["verdict"], row["rejected_rows"]) == (RECONCILED, 0)
    assert row["evidence"] == EVIDENCE_MISSING_QUARANTINE_EMPTY
    assert _grade(row) == (EVIDENCE_REMOVED, 2, INVESTIGATE_THE_QUARANTINE_TABLE), (
        "the batch reconciles and its evidence is gone, so the grade must come from the "
        "census. An arm that read the MISSING RECONCILIATION ROW instead finds this row, "
        "falls through the size arm at 0 rejected rows, and grades it isolated_rejection"
    )


def test_an_absent_reconciliation_row_does_not_by_itself_outrank_a_large_clean_rejection(probe):
    """The other direction of the same confound, out of the SAME one-row relation.

    socios' `1121645114029617` is not named by it, so its `verdict` is
    `no_reconciliation_row` while its 1,797 quarantined rows are where they have always
    been. Nothing is lost, so the size arm is the grade and the action is to read the rows.
    Under the confound's spelling this becomes `evidence_removed` at rank 2: an incident
    whose evidence is INTACT graded as one whose evidence has disappeared, on the strength
    of a missing view row.

    WHAT THIS DOES NOT SAY. A missing reconciliation row is not nothing -- for the five
    live incidents carrying it, it is why the four counts here are NULL, asserted below
    rather than assumed. What it is not is EVIDENCE of removal, which is the census's
    answer about the quarantine table and not the view's answer about itself."""
    with _temp_view(probe, _CONFOUND_VIEW, _confound_rows()) as view:
        row = _graded(probe, "socios", _SOCIOS_BATCHES[0], view=view)

    assert (row["verdict"], row["rejected_rows"]) == (NO_RECONCILIATION_ROW, 1797)
    assert row["evidence"] == ROWS_PRESENT
    assert (row["staged"], row["unaccounted"]) == (None, None), "no row found, so no counts"
    assert _grade(row) == (BULK_REJECTION, 3, REVIEW_THE_QUARANTINED_ROWS), (
        "1,797 rows are in the quarantine table and the ladder must say so. An arm that "
        "read the missing view row as removed evidence outranks a batch that lost nothing"
    )


# ----------------------------------------------------------------------------------
# 4. Severity and recommended action are separable, in both directions.
# ----------------------------------------------------------------------------------


def test_one_severity_carries_two_actions_and_one_action_carries_two_severities(probe):
    """The property this module exists for, asserted on the LIVE corpus in both directions.

    SAME SEVERITY, DIFFERENT ACTION: all five evidence-removed incidents are rank 2, and
    they split on WHERE a reader has to look. The three lookup incidents sit in a
    quarantine that is empty for every batch, so whatever removed them removed everything;
    the two estabelecimentos ones sit in a POPULATED quarantine and contributed nothing to
    it, so something removed exactly those rows. `evidence.py` argues at length why folding
    the two lets the second borrow the first's explanation, and the actions keep them apart.

    DIFFERENT SEVERITY, SAME ACTION: socios at rank 3 and empresas at rank 4 both send a
    reader to the quarantined rows. Nothing is lost in either; only the size differs.

    A design that derived the action from the severity could produce neither line."""
    lookup = _graded(probe, "lookup", _LOOKUP_BATCHES[0])
    unexplained = _graded(probe, "estabelecimentos", _ESTAB_UNEXPLAINED[0])
    socios = _graded(probe, "socios", _SOCIOS_BATCHES[0])
    empresas = _graded(probe, "empresas", _EMPRESAS_BATCHES[1])

    assert lookup["evidence"] == EVIDENCE_MISSING_QUARANTINE_EMPTY
    assert unexplained["evidence"] == EVIDENCE_MISSING_BATCH_ABSENT
    assert lookup["severity"] == unexplained["severity"] == EVIDENCE_REMOVED
    assert lookup["recommended_action"] == INVESTIGATE_THE_QUARANTINE_TABLE
    assert unexplained["recommended_action"] == INVESTIGATE_THE_MISSING_BATCH
    assert lookup["recommended_action"] != unexplained["recommended_action"]

    assert socios["severity_rank"] != empresas["severity_rank"]
    assert socios["recommended_action"] == empresas["recommended_action"]
    assert empresas["recommended_action"] == REVIEW_THE_QUARANTINED_ROWS


def test_the_rank_and_the_word_are_one_ladder_and_cannot_disagree(probe):
    """`severity_rank` is a lookup on the word, never a second ladder of integers.

    Two ladders agree on every input until somebody reorders one of them, and then they
    disagree silently. This walks one incident per severity and holds the emitted rank
    against `_EXPECTED_RANKS`, which is a LITERAL restatement of the order.

    THE LITERALS ARE THE POINT AND THE DERIVED FORM WAS NOT. This test used to compare the
    emitted rank against `enumerate(SEVERITIES)` and the emitted words against
    `set(SEVERITIES)`. Both sides came out of the tuple the SQL's map is built from, so
    they agreed by construction: a REORDERED `SEVERITIES` moves every rank the SQL emits
    and moves `rank_of` with it, and set equality cannot see order at all. The literal
    `tuple(_EXPECTED_RANKS) == SEVERITIES` is what fails there. The same four numbers are
    pinned a second time, against the corpus's own row counts, by
    `test_severity_is_ordered_across_the_corpus_and_the_size_arm_is_load_bearing`."""
    rows = [
        _graded(probe, source, batch)
        for source, batch in (
            ("payments", _PAYMENTS_BATCH),
            ("socios", _SOCIOS_BATCHES[0]),
            ("estabelecimentos", _ESTAB_BATCH),
            ("lookup", _LOOKUP_BATCHES[1]),
            ("merchant", _NULL_REASON_BATCH),
        )
    ]

    assert len(rows) == 5
    assert tuple(_EXPECTED_RANKS) == SEVERITIES, (
        "the module's ordering moved. `severity_rank_sql` builds its map from `SEVERITIES`, "
        "so this line is the only place the intended order is stated independently of it"
    )
    assert {row["severity"] for row in rows} == set(_EXPECTED_RANKS), "all four reached"
    for row in rows:
        assert row["severity_rank"] == _EXPECTED_RANKS[row["severity"]], row["severity"]
        assert row["recommended_action"] in RECOMMENDED_ACTIONS
