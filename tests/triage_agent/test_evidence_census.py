"""HOW MANY, AND WHAT VERDICT: the census by reason and the reconciliation beside it.

SPLIT FROM `test_evidence.py` AT A SUBJECT SEAM when that file reached the 800-line cap.
This half is the COUNTS -- what the census reports and what the reconciliation says about
it; `test_evidence_sample.py` is the STATES, what a sampled row looks like and what may
leave. The corpus both read is in `conftest.py`, which is also where the numbers below come
from and why they are unequal.

THE THINGS THIS FILE HAS TO PROVE, and each of them is a pair rather than a value. They are
numbered as they were when both halves were one file; number 3, the sample's vocabulary,
is `test_evidence_sample.py`'s subject and is not restated here:

  1. The census counts BY REASON. Proven twice -- against the corpus spread, and against a
     batch carrying TWO reject reasons, because no corpus batch has two and a census that
     returned the batch TOTAL under one reason's name would satisfy every corpus assertion
     in this file.
  2. Zero quarantined rows for a batch is `evidence_missing` AND NOT AN EMPTY RESULT SET.
     Both spellings of the removal are reached -- a quarantine that is empty outright, and
     one that is populated and lacks this batch -- because folding them into one word lets
     the unexplained case borrow the explained one's account. Each also has to return
     exactly ONE row: an empty result is the shape that cannot be told from a query that
     never ran, which is the whole species this phase is written against.
  4. The reconciliation verdict is attached where `dataops_reconciliation` has a row and
     reported AS ABSENT where it does not -- which is five of the eleven incidents, so the
     absence arm is the majority case and not an edge.
  5. THE TWO DISCRIMINATORS THE MODULE ARGUES HARDEST FOR HAVE A FAILING ARM HERE:
     `r.rejected_rows IS NOT NULL` over `r.reject_reason IS NOT NULL`, and `f.matched IS
     NULL` over `f.verdict IS NULL`. The corpus tells neither pair apart -- every group has
     a reason, no deployed verdict is NULL -- so both substitutions are green over
     everything above, which is a guard whose failure was never shown reachable. Two
     fixtures exist only to reach them: a rejected group whose reason is NULL, and a
     relation handed through `view=` whose verdict is NULL for a row that WAS found.
"""
from __future__ import annotations

from opl.bronze.reconcile import RECONCILED, STRANDED_GATED
from opl.bronze.registry import table_spec
from opl.triage_agent.evidence import (
    EVIDENCE_MISSING_BATCH_ABSENT,
    EVIDENCE_MISSING_QUARANTINE_EMPTY,
    NO_RECONCILIATION_ROW,
    ROWS_PRESENT,
    evidence_sql,
    reconciliation_sql,
)

from .conftest import (
    _CONFIG,
    _EMPRESAS_BATCHES,
    _ESTAB_BATCH,
    _ESTAB_UNEXPLAINED,
    _INCIDENTS,
    _LOOKUP_BATCHES,
    _MATRIX_BATCH,
    _NULL_REASON_BATCH,
    _PAYMENTS_BATCH,
    _SOCIOS_BATCHES,
    _SOCIOS_REASON,
    _TWO_REASON_BATCH,
    _census,
    _counts_by_reason,
    _run,
)

# Handed to `reconciliation_sql` through its `view=` seam: a row whose verdict is NULL,
# which `reconcile.verdict_case_sql` cannot produce and which is the only input separating
# `f.matched IS NULL` from `f.verdict IS NULL`.
_NULL_VERDICT_VIEW = "reconciliation_with_a_null_verdict"


# ----------------------------------------------------------------------------------
# 1. The census, by reason, at the corpus's own sizes.
# ----------------------------------------------------------------------------------


def test_the_census_counts_by_reject_reason_at_the_measured_sizes(probe):
    """The six incidents that carry rows, each reporting its own count and its own reason.

    THE SPREAD IS THE ASSERTION. 2,000 / 1,797 / 1,786 / 4 / 1 / 1 are three orders of
    magnitude apart, so a census that read the wrong batch, the whole table, or both socios
    batches together produces a number belonging to a different incident. Six equal
    fixtures would have let all three of those pass.

    `null_or_empty_nome_socio_razao_social`'s 3,583 is TWO incidents three weeks apart and
    is asserted as two: a severity computed per reason rather than per (table, batch) fuses
    them, which is what `docs/f6-run-evidence.md` 0.3 settles.

    AND THE WHOLE-TABLE COUNT SITS BESIDE THEM, WHERE THE TWO NUMBERS DIFFER, the only
    place the grain is provable: the table holds 3,588 and this incident is 1,797 of them.
    Live, that column reads 3,583 on the incident row for EACH socios batch -- the number
    0.3 flags as the fusion hazard."""
    assert _counts_by_reason(probe, "payments", _PAYMENTS_BATCH) == {"rescued_data_present": 2000}
    assert _counts_by_reason(probe, "socios", _SOCIOS_BATCHES[0]) == {_SOCIOS_REASON: 1797}
    assert _counts_by_reason(probe, "socios", _SOCIOS_BATCHES[1]) == {_SOCIOS_REASON: 1786}
    assert _counts_by_reason(probe, "estabelecimentos", _ESTAB_BATCH) == {
        "encoding_replacement_char": 4
    }
    for batch in _EMPRESAS_BATCHES:
        assert _counts_by_reason(probe, "empresas", batch) == {"null_or_empty_razao_social": 1}

    for batch, rejected in ((_SOCIOS_BATCHES[0], 1797), (_SOCIOS_BATCHES[1], 1786)):
        row = _census(probe, "socios", batch)[0]
        assert (row["rejected_rows"], row["quarantine_table_rows"]) == (rejected, 3588), (
            "two grains, asserted where they differ, or neither column is proven"
        )

    for source, batch in _INCIDENTS[:6]:
        assert {row["evidence"] for row in _census(probe, source, batch)} == {ROWS_PRESENT}


def test_a_batch_with_two_reject_reasons_is_two_rows_and_not_one_total(probe):
    """BY REASON, PROVEN AGAINST A BATCH THAT HAS TWO -- which no corpus batch does.

    Every assertion in the test above is satisfied by a census that ignored the reason
    entirely and reported the batch total under whichever reason it happened to see first,
    because each of those batches carries exactly one. This is the fixture that can tell
    those apart: 5 and 3, whose total 8 is not either of them.

    AND THE WHOLE-TABLE COUNT IS A THIRD NUMBER, 15, because this table also holds the
    seven null-reason rows the test below uses. An earlier docstring claimed that count was
    asserted here and it was not -- over a fixture where table and batch were both 8, so
    even the claimed assertion would have passed for either grain."""
    rows = _census(probe, "merchant", _TWO_REASON_BATCH)

    assert {row["reject_reason"]: row["rejected_rows"] for row in rows} == {
        "null_or_empty_cnpj": 5,
        "encoding_replacement_char": 3,
    }
    assert [row["rejected_rows"] for row in rows] == [5, 3], "ordered by size, largest first"
    assert {row["quarantine_table_rows"] for row in rows} == {15}, "the TABLE, not the batch"
    assert {row["evidence"] for row in rows} == {ROWS_PRESENT}


def test_a_rejected_group_whose_reason_is_null_is_still_rows_present(probe):
    """THE FAILING ARM OF `r.rejected_rows IS NOT NULL` AND NOT `r.reject_reason IS NOT
    NULL` -- the one real decision in the census ladder, which had no input until here.

    Substituting the reason test for the count test leaves every other assertion in this
    file green, because every corpus group carries a reason. These seven tell the two
    apart: genuinely rejected, in the table, `_dq_reject_reason` NULL. The count test calls
    them `rows_present` and keeps the 7; the reason test calls them
    `evidence_missing_batch_absent` -- a REMOVAL -- handing a later task an incident
    reported as deleted evidence over rows that are still there. The reason comes back NULL
    rather than coalesced: inventing a string would publish something nothing read."""
    rows = _census(probe, "merchant", _NULL_REASON_BATCH)

    assert len(rows) == 1
    assert rows[0]["reject_reason"] is None
    assert rows[0]["rejected_rows"] == 7, "the rows are there and the count survives"
    assert rows[0]["quarantine_table_rows"] == 15
    assert rows[0]["evidence"] == ROWS_PRESENT, (
        "a rejected group with no reason is rows_present; a ladder keyed on the reason "
        "would report these seven rows as evidence that was removed"
    )


# ----------------------------------------------------------------------------------
# 2. Zero rows, which is two different worlds and neither of them is empty output.
# ----------------------------------------------------------------------------------


def test_a_quarantine_that_is_empty_and_one_that_lacks_this_batch_are_different_findings(
    probe,
):
    """The state that is not `clean`, in both of the spellings this corpus contains.

    `fail_on_dq` is reachable only through `check_bad_rows -> false`, and `dq_gate_batch`
    appends the rejects BEFORE publishing the count it is judged on, so an incident with
    zero quarantined rows today is evidence that was removed -- never "the gate rejected
    nothing". The two spellings are kept apart because only one of them has an account:
    the lookup table was recreated a week after its firings, and the two estabelecimentos
    incidents sit in a table holding another batch's four rows with nothing in the record
    explaining them.

    ONE ROW EACH, AND THAT IS THE ASSERTION THE VERDICTS REST ON. An empty result set is
    the shape that cannot be told from a query that failed to run, and it is what the
    obvious spelling of this census returns for both of these."""
    for batch in _LOOKUP_BATCHES:
        rows = _census(probe, "lookup", batch)
        assert len(rows) == 1, "a removal must not be reported as an empty answer"
        assert rows[0]["evidence"] == EVIDENCE_MISSING_QUARANTINE_EMPTY
        assert rows[0]["quarantine_table_rows"] == 0

    for batch in _ESTAB_UNEXPLAINED:
        rows = _census(probe, "estabelecimentos", batch)
        assert len(rows) == 1
        assert rows[0]["evidence"] == EVIDENCE_MISSING_BATCH_ABSENT
        assert rows[0]["quarantine_table_rows"] == 4, "the table holds another batch's rows"

    empty, absent = (
        _census(probe, "lookup", _LOOKUP_BATCHES[0])[0],
        _census(probe, "estabelecimentos", _ESTAB_UNEXPLAINED[0])[0],
    )
    assert empty["evidence"] != absent["evidence"]
    assert {empty["rejected_rows"], absent["rejected_rows"]} == {0}
    assert empty["reject_reason"] is None and absent["reject_reason"] is None
    assert ROWS_PRESENT not in {empty["evidence"], absent["evidence"]}


def test_the_same_zero_is_reported_for_two_different_reasons_and_the_integer_cannot_say_so(
    probe,
):
    """WHY THE VERDICT COLUMN EXISTS, stated as the thing it would cost to drop it.

    The count is the identical integer under both removals and under a batch nobody ever
    gated. If the census published only that integer, the three would be one output --
    which is ADR 0018's species, and the reason it is hunted is that its output looks
    exactly like a pass."""
    zeros = [
        _census(probe, "lookup", _LOOKUP_BATCHES[0])[0],
        _census(probe, "estabelecimentos", _ESTAB_UNEXPLAINED[0])[0],
        # Not an incident at all: no gate ever fired on this batch of this table.
        _census(probe, "ptax", "a_batch_that_was_never_gated")[0],
    ]

    assert [row["rejected_rows"] for row in zeros] == [0, 0, 0]
    assert len({row["evidence"] for row in zeros}) == 2, (
        "the two removals must differ; the never-gated batch shares a table state with one "
        "of them and is separated by the incident feed, not by this census"
    )


# ----------------------------------------------------------------------------------
# 4. The reconciliation, whose ABSENCE is the majority case.
# ----------------------------------------------------------------------------------


def _reconciliation(spark, source: str, batch: str):
    rows = _run(spark, reconciliation_sql(table_spec(source), _CONFIG), batch)
    assert len(rows) == 1, "one incident, one reconciliation row, present or not"
    return rows[0]


def test_the_reconciliation_verdict_is_attached_where_the_view_has_a_row(probe):
    """Passed through, not re-derived -- including the four batches that read `reconciled`
    after the gate fired and they were repromoted.

    `592660596679630` is the one live stranding and it reproduces exactly: 10,000 staged,
    0 promoted, 2,000 quarantined, 8,000 unaccounted. Its quarantined count is the same
    2,000 the census reports, because both come from the same rows."""
    payments = _reconciliation(probe, "payments", _PAYMENTS_BATCH)
    assert payments["verdict"] == STRANDED_GATED
    assert (payments["staged"], payments["promoted"]) == (10000, 0)
    assert (payments["quarantined"], payments["unaccounted"]) == (2000, 8000)
    assert "repromote_triaged_batch" in payments["remedy"]

    socios = _reconciliation(probe, "socios", _SOCIOS_BATCHES[0])
    assert socios["verdict"] == RECONCILED
    assert (socios["staged"], socios["promoted"], socios["quarantined"]) == (1800, 3, 1797)
    assert socios["remedy"] is None


def test_the_absence_of_a_reconciliation_row_is_reported_as_absence(probe):
    """Five of eleven, so this is the majority rendering and not an edge.

    The five zero-quarantine incidents have no staging rows either, so the view that would
    count them cannot speak for them. That is a fact about the view's inputs, and it is
    said as one: the verdict is a word of its own -- not `reconciled`, which would claim
    the batch is finished, and not NULL, which the first consumer to format it reads as
    nothing wrong. The counts stay NULL because there genuinely is no count, which is F4's
    rendering of a missing metric rather than a zero."""
    missing = [
        _reconciliation(probe, source, batch)
        for source, batch in _INCIDENTS
        if batch in (*_LOOKUP_BATCHES, *_ESTAB_UNEXPLAINED)
    ]

    assert len(missing) == 5
    for row in missing:
        assert row["verdict"] == NO_RECONCILIATION_ROW
        assert row["verdict"] != RECONCILED and row["verdict"] is not None
        assert (row["staged"], row["promoted"], row["quarantined"]) == (None, None, None)
        assert row["unaccounted"] is None and row["remedy"] is None


def test_a_reconciliation_row_whose_verdict_is_null_is_still_a_row_that_was_found(probe):
    """THE FAILING ARM OF `f.matched IS NULL` AND NOT `f.verdict IS NULL`.

    `reconcile.verdict_case_sql` has an ELSE arm, so no deployed verdict is NULL and the
    substitution is green over every batch in this file -- the property
    `reconciliation_sql` refuses to depend on. Whether a row was FOUND is a fact about the
    join; reading it off another module's CASE ladder makes a correction over there a
    silent defect here, and `view=` is the seam that shows it now rather than on the day
    `reconcile.py` grows a NULL arm. The counts sit beside the verdict because under the
    substitution `staged` still reads 41 while the verdict reads `no_reconciliation_row` --
    one column saying the row does not exist and another saying how big it is."""
    probe.sql(
        f"CREATE OR REPLACE TEMP VIEW {_NULL_VERDICT_VIEW} AS SELECT 'socios' AS source, "
        f"'{_MATRIX_BATCH}' AS batch_id, 41 AS staged, 0 AS promoted, 5 AS quarantined, "
        "36 AS unaccounted, CAST(NULL AS STRING) AS verdict, CAST(NULL AS STRING) AS remedy"
    )
    statement = reconciliation_sql(table_spec("socios"), _CONFIG, view=_NULL_VERDICT_VIEW)
    try:
        rows = _run(probe, statement, _MATRIX_BATCH)

        assert len(rows) == 1
        assert (rows[0]["staged"], rows[0]["quarantined"]) == (41, 5), "the row was found"
        assert rows[0]["verdict"] is None, (
            "a found row's NULL verdict is passed through as NULL; calling it "
            f"{NO_RECONCILIATION_ROW!r} would say no row was found, and one was"
        )
    finally:
        probe.catalog.dropTempView(_NULL_VERDICT_VIEW)


def test_every_incident_in_the_corpus_gets_exactly_one_of_each_publishable_statement(probe):
    """Eleven incidents, three statements, one `args` binding -- run, not asserted as text.

    The count split is the corpus's own and is checked here so a fixture that quietly lost
    a batch would fail: six incidents have rows and reconcile, five have neither.

    A ZERO-ROW INCIDENT'S CENSUS IS EXACTLY ONE ROW, not "at least one". `>=` is weaker
    than the module's property -- `LEFT JOIN ... ON true` against an ungrouped `COUNT(*)`
    can yield no other number when the grouped side is empty -- so two removal rows for one
    batch is as wrong as none, and `>=` reports green over it."""
    with_rows, without = 0, 0
    for source, batch in _INCIDENTS:
        statements = evidence_sql(source, _CONFIG)
        census = _run(probe, statements["census"], batch)
        shapes = _run(probe, statements["row_shapes"], batch)
        recon = _run(probe, statements["reconciliation"], batch)

        assert len(census) >= 1 and len(recon) == 1
        if census[0]["evidence"] == ROWS_PRESENT:
            with_rows += 1
            assert shapes and recon[0]["verdict"] != NO_RECONCILIATION_ROW
        else:
            without += 1
            assert len(census) == 1, "a removal is ONE row: not an empty answer, not two"
            assert shapes == [] and recon[0]["verdict"] == NO_RECONCILIATION_ROW
    assert (with_rows, without) == (6, 5)
