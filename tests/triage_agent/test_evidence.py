"""The evidence for one incident, driven out of real tables at the measured corpus's sizes.

WHY THE FIXTURE CARRIES 2,000 / 1,797 / 1,786 / 4 / 1 / 1 AND NOT SIX EQUAL HANDFULS. The
counts are the corpus's own (`docs/f6-run-evidence.md` 0.3), and they are unequal on
purpose: a fixture that gave every incident three rows would let a census that read the
wrong batch, or the whole table, or half of one, return a plausible number for every
assertion in this file. With this spread, any of those returns a number that belongs to
another incident and is visible on sight. The three-order-of-magnitude gap between 2,000
and 1 is doing the work a hand-checked constant cannot.

THE FIVE THINGS THIS FILE HAS TO PROVE, and each of them is a pair rather than a value:

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
  3. A masked column reports `masked` and an unmasked EMPTY column reports `empty`, BOTH
     arms over the same five rows. One arm proves nothing: a sampler answering `masked`
     everywhere passes the first, one ignoring the declaration passes the second.
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

AND ONE MORE, WHICH MAKES THIS REPOSITORY'S PUBLICNESS SAFE RATHER THAN CAREFUL: every fixture
value carries a sentinel EXCEPT where a state word needs it not to, and each personal column a
token of its own, so "did a value escape" and "did a PERSONAL value escape" are both askable of
the OUTPUT -- of the batches `_TAINT_SWEEP` walks, which are not `_INCIDENTS`. Both arms keep
their control in the same test, because a taint check whose reader is broken -- or whose fixture
never planted what it looks for -- reports clean over everything.

THE RECONCILIATION IS BUILT BY THE SHIPPED `batch_grain_sql` OVER THESE SAME TABLES, not
hand-written to look like it. So the counts this file asserts about the reconciliation are
derived from the very quarantine rows the census counts, the column contract is the
deployed one, and payments reproduces the live stranding exactly: 10,000 staged, 0
promoted, 2,000 quarantined, 8,000 unaccounted.

VIEWS, NOT DELTA TABLES, for `tests/bronze/test_reconcile.py`'s reason: everything here is
a filter, a GROUP BY, a LIMIT and four aggregates, which need rows and a schema and nothing
else. The large legs are `range(n)` so 10,000 staged rows cost a scan and not a write.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from opl.bronze.autoloader import SOURCE_FILE_COLUMN
from opl.bronze.dq import REJECT_COLUMN, RESCUED_DATA_COLUMN
from opl.bronze.promote import BATCH_COLUMN
from opl.bronze.reconcile import (
    BATCH_GRAIN_VIEW,
    RECONCILED,
    STRANDED_GATED,
    batch_grain_sql,
    create_view_ddl,
)
from opl.bronze.registry import REGISTRY, table_spec
from opl.config import OplConfig
from opl.contracts.catalogue import columns_for
from opl.triage_agent.evidence import (
    EMPTY,
    EVIDENCE_MISSING_BATCH_ABSENT,
    EVIDENCE_MISSING_QUARANTINE_EMPTY,
    MASKED,
    NO_RECONCILIATION_ROW,
    NULL_VALUE,
    PRESENT,
    REPLACEMENT_CHAR,
    ROWS_PRESENT,
    VALUE_STATES,
    evidence_sql,
    quarantine_census_sql,
    reconciliation_sql,
    row_sample_sql,
    row_shapes_sql,
    value_state_sql,
)

_SCHEMA = "opl_evidence_probe"
_CONFIG = OplConfig(catalog="spark_catalog", schema=_SCHEMA)

# Every fixture value carries this EXCEPT where a state word needs it not to (`''`, NULL,
# `***`), so a column pinned to one of those -- or a leak that TRANSFORMS a value away from this
# string -- is invisible here; `_TAINT_SWEEP` and the contract file's name count are what cover
# those two. Chosen so nothing in this repository, in Spark's output or in a reject reason could
# produce it by accident.
_SENTINEL = "ZZSENTINELZZ"

# U+FFFD, spelled as its CODE POINT here rather than as either module's constant. The lock
# holding the two modules equal is in `test_evidence_contract.py`; what this file needs is
# a third, independent spelling, so that a fixture and the code under test cannot agree on
# the wrong character together.
_REPLACEMENT = chr(0xFFFD)

# The corpus, measured 2026-08-24. Six incidents carry quarantined rows and five carry none
# -- and the five are not one story: three are the lookup table recreated 2026-07-31 (F4's
# account) and TWO are estabelecimentos incidents that sit in a POPULATED quarantine and
# contributed nothing to it, which nothing in the record explains.
_PAYMENTS_BATCH = "592660596679630"
_SOCIOS_BATCHES = ("1121645114029617", "409962018634322")
_ESTAB_BATCH = "128878829411613"
_ESTAB_UNEXPLAINED = ("187805471003061", "315230730740144")
_EMPRESAS_BATCHES = ("321750543973966", "371067950667703")
_LOOKUP_BATCHES = ("184706631093131", "241387611390862", "996871467498110")

# INVENTED, and labelled so no later reader mistakes either for a measurement.
#
# `_MATRIX_BATCH` exists because the corpus cannot express what test 3 needs: its 3,583
# socios rows are uniform, and the masked/unmasked vocabulary has to be read off rows that
# differ. `_TWO_REASON_BATCH` exists because NO corpus batch carries two reject reasons, so
# without it a census that reported the batch total under one reason's name would pass
# every other assertion in this file.
# `_NULL_REASON_BATCH` exists because no corpus group has a NULL reject reason, the only
# input that tells the census ladder's discriminator from the one it was chosen over.
_MATRIX_BATCH = "invented_shapes_matrix"
_TWO_REASON_BATCH = "invented_two_reason_batch"
_NULL_REASON_BATCH = "invented_null_reason_batch"

# Handed to `reconciliation_sql` through its `view=` seam: a row whose verdict is NULL,
# which `reconcile.verdict_case_sql` cannot produce and which is the only input separating
# `f.matched IS NULL` from `f.verdict IS NULL`.
_NULL_VERDICT_VIEW = "reconciliation_with_a_null_verdict"

_SOCIOS_REASON = "null_or_empty_nome_socio_razao_social"
_MASKED_SOCIOS_COLUMN = "nome_socio_razao_social"
_UNMASKED_SOCIOS_COLUMN = "cnpj_basico"

# A TOKEN PER DECLARED-PERSONAL COLUMN, PLANTED IN THAT COLUMN AND NOWHERE ELSE. The
# sentinel alone cannot ask "did a PERSONAL value escape" -- every innocent column carries
# it too -- and a token shared with `cnpj_basico` answers about the wrong column. These two
# are unique to the two masked columns, which is what makes the taint test's personal arm
# able to fail at all: the version that shipped asserted `"MARIA" not in ...` over a batch
# where no column held it, and deleting `row_sample_sql`'s mask filter left it green.
_PERSONAL_TOKENS = {
    _MASKED_SOCIOS_COLUMN: f"{_SENTINEL}_MARIA",
    "nome_do_representante": f"{_SENTINEL}_JOAO",
}


@dataclass(frozen=True)
class _Group:
    """`rows` quarantined rows for one (batch, reject reason), as one leg of a UNION ALL.

    `values` overrides a column's expression; everything else gets a per-row sentinel, so
    the taint check has something to find in every column it is not told about.

    `reason` IS NULLABLE, and not for convenience: `None` writes a genuinely NULL
    `_dq_reject_reason` onto real rejected rows, the only input separating the census
    ladder's `r.rejected_rows IS NOT NULL` from the `r.reject_reason IS NOT NULL` it was
    chosen over."""

    batch: str
    reason: str | None
    rows: int
    values: dict[str, str] = field(default_factory=dict)


# THE FIVE ROWS THAT CARRY THE VOCABULARY, and the two columns are the whole point: they
# hold the same five SHAPES, one column declared personal and one not, so the two states
# cannot be produced by the data alone. Row 2's `***` is the sharpest: it is the exact
# string a UC mask emits, and the unmasked column must still call it `present` -- the word
# `masked` is not reachable by any value, because the masked column is never read.
#
# ROW 0's TOKEN IS THE ONE THING THAT DIFFERS BETWEEN THE TWO COLUMNS, on purpose: the
# states must be identical or the vocabulary comparison is not like for like, and the token
# must differ or nothing can ask whether a PERSONAL value escaped.
_MATRIX_STATES = (PRESENT, EMPTY, PRESENT, NULL_VALUE, REPLACEMENT_CHAR)


def _matrix_values(row_zero: str) -> tuple[str, ...]:
    """The five values producing `_MATRIX_STATES`, row 0's token from the caller."""
    mangled = f"'{_SENTINEL}_{_REPLACEMENT}_MANGLED'"
    return (f"'{row_zero}'", "''", "'***'", "CAST(NULL AS STRING)", mangled)


def _by_row_id(values: tuple[str, ...]) -> str:
    arms = " ".join(f"WHEN {index} THEN {value}" for index, value in enumerate(values))
    return f"CASE id {arms} END"


_MATRIX_OVERRIDES = {
    _MASKED_SOCIOS_COLUMN: _by_row_id(_matrix_values(_PERSONAL_TOKENS[_MASKED_SOCIOS_COLUMN])),
    _UNMASKED_SOCIOS_COLUMN: _by_row_id(_matrix_values(f"{_SENTINEL}_PUBLIC")),
    # The second declared-personal column, carrying its own token in every row. Its state
    # is `masked` whatever it holds, so the value is here only to be findable if it ever
    # reaches a statement that should not project it.
    "nome_do_representante": f"'{_PERSONAL_TOKENS['nome_do_representante']}'",
}

_QUARANTINE: dict[str, tuple[_Group, ...]] = {
    "payments": (
        _Group(
            _PAYMENTS_BATCH,
            "rescued_data_present",
            2000,
            {RESCUED_DATA_COLUMN: f"concat('{_SENTINEL}_rescued_', CAST(id AS STRING))"},
        ),
    ),
    "socios": (
        _Group(_SOCIOS_BATCHES[0], _SOCIOS_REASON, 1797, {_MASKED_SOCIOS_COLUMN: "''"}),
        _Group(_SOCIOS_BATCHES[1], _SOCIOS_REASON, 1786, {_MASKED_SOCIOS_COLUMN: "''"}),
        _Group(_MATRIX_BATCH, _SOCIOS_REASON, 5, _MATRIX_OVERRIDES),
    ),
    # The four rows whose lost byte ADR 0006 records, in the column that actually held it.
    # The two unexplained incidents are absent from this table ON PURPOSE: that is what
    # makes it populated and this batch missing, which is a different finding from empty.
    "estabelecimentos": (
        _Group(
            _ESTAB_BATCH,
            "encoding_replacement_char",
            4,
            {"correio_eletronico": f"'{_SENTINEL}{_REPLACEMENT}mail'"},
        ),
    ),
    "empresas": tuple(
        _Group(batch, "null_or_empty_razao_social", 1, {"razao_social": "''"})
        for batch in _EMPRESAS_BATCHES
    ),
    # 5 + 3 + 7 = 15, all three deliberately different: the whole-table count is the one
    # census column NOT at the incident's grain, so it must be asserted where it cannot be
    # confused with either reason's count.
    "merchant": (
        _Group(_TWO_REASON_BATCH, "null_or_empty_cnpj", 5),
        _Group(_TWO_REASON_BATCH, "encoding_replacement_char", 3),
        _Group(_NULL_REASON_BATCH, None, 7),
    ),
    # EMPTY OUTRIGHT, which is the lookup's real state: the table was recreated on
    # 2026-07-31, a week after its three firings, so nothing of them survives.
    "lookup": (),
    "ptax": (),
}

# staged and promoted per batch, chosen so every batch RECONCILES except payments, which
# reproduces the live stranding: 10,000 staged, 0 promoted, 2,000 quarantined.
_STAGED: dict[str, tuple[tuple[str, int], ...]] = {
    "payments": ((_PAYMENTS_BATCH, 10000),),
    "socios": ((_SOCIOS_BATCHES[0], 1800), (_SOCIOS_BATCHES[1], 1790), (_MATRIX_BATCH, 5)),
    "estabelecimentos": ((_ESTAB_BATCH, 104),),
    "empresas": tuple((batch, 3) for batch in _EMPRESAS_BATCHES),
    "merchant": ((_TWO_REASON_BATCH, 8),),
}
_PROMOTED: dict[str, tuple[tuple[str, int], ...]] = {
    "socios": ((_SOCIOS_BATCHES[0], 3), (_SOCIOS_BATCHES[1], 4)),
    "estabelecimentos": ((_ESTAB_BATCH, 100),),
    "empresas": tuple((batch, 2) for batch in _EMPRESAS_BATCHES),
}

# (registry key, batch) for all eleven, in the corpus's own order.
_INCIDENTS = (
    ("payments", _PAYMENTS_BATCH),
    ("socios", _SOCIOS_BATCHES[0]),
    ("socios", _SOCIOS_BATCHES[1]),
    ("estabelecimentos", _ESTAB_BATCH),
    ("empresas", _EMPRESAS_BATCHES[0]),
    ("empresas", _EMPRESAS_BATCHES[1]),
    *(("lookup", batch) for batch in _LOOKUP_BATCHES),
    *(("estabelecimentos", batch) for batch in _ESTAB_UNEXPLAINED),
)

# WHAT THE TAINT SWEEP WALKS, AND IT IS NOT `_INCIDENTS`. `_QUARANTINE["socios"]` sets the
# masked column to `''` for both corpus batches -- that is WHY they are a
# `null_or_empty_nome_socio_razao_social` incident -- so `nome_socio_razao_social` carries
# no sentinel anywhere in the eleven and projecting it in the clear swept clean: adding
# `nome_socio_razao_social AS leaked_name` to `row_shapes_sql` left this file green.
# `_MATRIX_BATCH` is the one place either personal column holds a findable value.
_TAINT_SWEEP = (*_INCIDENTS, ("socios", _MATRIX_BATCH))

_QUARANTINE_COLUMNS = (BATCH_COLUMN, REJECT_COLUMN, SOURCE_FILE_COLUMN, RESCUED_DATA_COLUMN)


def _quarantine_columns(source: str) -> tuple[str, ...]:
    return (*_QUARANTINE_COLUMNS, *columns_for(REGISTRY[source].contract))


def _group_sql(source: str, group: _Group) -> str:
    """One leg: `group.rows` rows of `range`, every column an expression.

    A `None` reason is a typed NULL and not the string `'None'`, which is what an f-string
    would have produced and would have made the group carry a reason after all."""
    reason = "CAST(NULL AS STRING)" if group.reason is None else f"'{group.reason}'"
    projected = [
        f"'{group.batch}' AS {BATCH_COLUMN}",
        f"{reason} AS {REJECT_COLUMN}",
        f"'{_SENTINEL}_file_{group.batch}.csv' AS {SOURCE_FILE_COLUMN}",
        f"{group.values.get(RESCUED_DATA_COLUMN, 'CAST(NULL AS STRING)')}"
        f" AS {RESCUED_DATA_COLUMN}",
    ]
    for column in columns_for(REGISTRY[source].contract):
        default = f"concat('{_SENTINEL}_{column}_', CAST(id AS STRING))"
        projected.append(f"{group.values.get(column, default)} AS `{column}`")
    return f"SELECT {', '.join(projected)} FROM range({group.rows})"


def _empty_sql(columns: tuple[str, ...]) -> str:
    """A typed, row-less relation. `WHERE false`, which Spark can type and an empty VALUES
    list cannot."""
    cast = ", ".join(f"CAST(NULL AS STRING) AS `{column}`" for column in columns)
    return f"SELECT {cast} WHERE false"


def _quarantine_sql(source: str) -> str:
    groups = _QUARANTINE.get(source, ())
    if not groups:
        return _empty_sql(_quarantine_columns(source))
    return "\nUNION ALL\n".join(_group_sql(source, group) for group in groups)


def _batches_sql(counts: tuple[tuple[str, int], ...]) -> str:
    """Staging or bronze: `batch_grain_sql` groups on `_batch_id` and reads nothing else."""
    if not counts:
        return _empty_sql((BATCH_COLUMN,))
    return "\nUNION ALL\n".join(
        f"SELECT '{batch}' AS {BATCH_COLUMN} FROM range({rows})" for batch, rows in counts
    )


@pytest.fixture(scope="module")
def probe(spark):
    """All 21 registry objects plus the reconciliation view, in a schema this module drops.

    The reconciliation is the SHIPPED `batch_grain_sql` over these tables rather than a
    hand-built lookalike, so what `reconciliation_sql` reads is the deployed column
    contract and the counts it reports are derived from the same rows the census counts."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_SCHEMA}")
    for source, spec in REGISTRY.items():
        for table, body in (
            (spec.staging, _batches_sql(_STAGED.get(source, ()))),
            (spec.bronze, _batches_sql(_PROMOTED.get(source, ()))),
            (spec.quarantine, _quarantine_sql(source)),
        ):
            spark.sql(f"CREATE OR REPLACE VIEW {_CONFIG.table(table)} AS {body}")
    spark.sql(create_view_ddl(BATCH_GRAIN_VIEW, batch_grain_sql(_CONFIG), _CONFIG))
    yield spark
    spark.sql(f"DROP DATABASE IF EXISTS {_SCHEMA} CASCADE")


def _run(spark, sql: str, batch: str) -> list:
    return spark.sql(sql, args={"batch_id": batch}).collect()


def _census(spark, source: str, batch: str) -> list:
    return _run(spark, quarantine_census_sql(table_spec(source), _CONFIG), batch)


def _counts_by_reason(spark, source: str, batch: str) -> dict[str | None, int]:
    return {row["reject_reason"]: row["rejected_rows"] for row in _census(spark, source, batch)}


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
# 3. The sample's vocabulary: masked is not empty, and neither can borrow the other.
# ----------------------------------------------------------------------------------


def _states(spark, source: str, batch: str) -> list[dict[str, str]]:
    rows = _run(spark, row_shapes_sql(table_spec(source), _CONFIG), batch)
    return [row["value_states"] for row in rows]


def test_a_masked_column_reports_masked_and_an_unmasked_empty_one_reports_empty(probe):
    """BOTH ARMS, over five rows holding the same five SHAPES in two columns.

    They hold the same states row for row and differ in one thing: row 0's token, which is
    unique to the personal column. That is what lets the taint test at the bottom of this
    file ask whether a PERSONAL value escaped rather than whether any value did.

    The rejection these socios rows carry IS that the name column is null or empty, and
    that same column is masked -- so a sampler that read it would show `***` and could not
    tell "masked from me" from "empty, which is why this row is here". `masked` is emitted
    without reading the column, so it says which of those it is: neither. The value is not
    evidence in this output, for any reader.

    Row by row, the unmasked column moves through `present`, `empty`, `present`, `null`,
    `replacement_char` while the masked one never moves at all. The `***` row is the sharp
    one: that is exactly what a UC mask emits, and the unmasked column still calls it
    `present` -- so no value can manufacture the word `masked`."""
    states = _states(probe, "socios", _MATRIX_BATCH)
    assert len(states) == 5

    unmasked = [row[_UNMASKED_SOCIOS_COLUMN] for row in states]
    masked = [row[_MASKED_SOCIOS_COLUMN] for row in states]

    assert unmasked == list(_MATRIX_STATES)
    assert masked == [MASKED] * 5
    assert "'***'" in _MATRIX_OVERRIDES[_UNMASKED_SOCIOS_COLUMN], "the sharp row, still planted"


def test_every_declared_personal_column_is_masked_and_no_other_column_is(probe):
    """The redaction is exactly the declaration, over a whole sampled row.

    Not only the two columns the test above looks at: the socios contract has eleven, two
    of them declared personal, and a redaction that covered one or spread to all of them
    would be a different control. `_rescued_data` is profiled too and is NOT redacted --
    its state is the evidence for `rescued_data_present`, the largest incident here."""
    row = _states(probe, "socios", _SOCIOS_BATCHES[0])[0]
    masked = {column for column, state in row.items() if state == MASKED}

    assert masked == {_MASKED_SOCIOS_COLUMN, "nome_do_representante"}
    assert set(row) == {*columns_for("socios"), RESCUED_DATA_COLUMN}
    assert row[RESCUED_DATA_COLUMN] == NULL_VALUE

    payments = _states(probe, "payments", _PAYMENTS_BATCH)[0]
    assert MASKED not in payments.values(), "no payments column is declared personal"
    assert payments[RESCUED_DATA_COLUMN] == PRESENT, "the reason this incident exists"


def test_the_empty_and_null_states_are_the_gates_own_predicate_and_not_a_second_spelling(
    probe,
):
    """`null_or_empty_*` is `isNull() | trim(...) == ''`, and this file must agree with it.

    Two spellings of one rule is what this repository keeps paying for, and here the drift
    would be invisible: a state expression testing `= ''` rather than `TRIM(...) = ''`
    reports a whitespace-only value as `present` while the gate that rejected the row calls
    it blank -- so the sample would contradict the reject reason beside it and neither
    would look wrong. The gate's own predicate is executed here and the two are compared
    row for row, rather than the SQL being read."""
    from opl.bronze.rule_predicates import _null_or_blank

    values = [("a",), ("",), ("   ",), (None,), (f"x{_REPLACEMENT}y",)]
    frame = probe.createDataFrame(values, "c STRING")
    frame.createOrReplaceTempView("gate_probe")
    blank = [row["blank"] for row in frame.withColumn("blank", _null_or_blank("c")()).collect()]

    states = [
        row["state"]
        for row in probe.sql(
            f"SELECT {value_state_sql('c', masked=False)} AS state FROM gate_probe"
        ).collect()
    ]

    try:
        assert states == [PRESENT, EMPTY, EMPTY, NULL_VALUE, REPLACEMENT_CHAR]
        assert blank == [False, True, True, True, False]
        assert [state in (EMPTY, NULL_VALUE) for state in states] == blank
    finally:
        # `probe` drops a DATABASE; a temp view is session-scoped and outlives that.
        probe.catalog.dropTempView("gate_probe")


def test_every_state_this_module_reports_is_one_of_the_closed_vocabulary(probe):
    """A word outside `VALUE_STATES` would be a value leaking through the state column.

    Swept over every incident that has rows rather than over one, because the state
    expression is generated per column and a contract with a column the generator handled
    differently would only show up on that contract.

    EQUALITY IN BOTH DIRECTIONS, WHICH IS THE POINT OF A CLOSED VOCABULARY. The first
    version required four of the five, leaving `replacement_char` unrequired in the test
    whose subject is that every arm is reached -- though the sweep reaches it, on
    `estabelecimentos.correio_eletronico`, ADR 0006's four lost-byte rows."""
    seen = {
        state
        for source, batch in _INCIDENTS[:6]
        for row in _states(probe, source, batch)
        for state in row.values()
    }

    assert seen, "the sweep found no sampled rows at all"
    assert seen == set(VALUE_STATES), (
        f"{sorted(seen - set(VALUE_STATES))} is not a state and "
        f"{sorted(set(VALUE_STATES) - seen)} was reached by nothing"
    )


def test_the_sample_is_bounded_even_where_the_incident_is_two_thousand_rows(probe):
    """A sample is bounded and a census is not, which is why they are two statements."""
    assert len(_states(probe, "payments", _PAYMENTS_BATCH)) == 20
    assert _counts_by_reason(probe, "payments", _PAYMENTS_BATCH) == {"rescued_data_present": 2000}
    narrower = row_shapes_sql(table_spec("payments"), _CONFIG, limit=3)
    assert len(_run(probe, narrower, _PAYMENTS_BATCH)) == 3


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


# ----------------------------------------------------------------------------------
# The publishable statements emit no row value, asserted against the OUTPUT.
# ----------------------------------------------------------------------------------


def _planted(spark, source: str, column: str, batch: str) -> str:
    """What the FIXTURE holds in one column of one batch, read straight off the table.

    The control for every "this token did not escape" assertion: an absence check over a
    string the fixture never planted is green for a reason nothing to do with the statement
    under test, which is what the shipped personal-column assertion was."""
    rows = spark.sql(
        f"SELECT `{column}` AS planted FROM {_CONFIG.table(table_spec(source).quarantine)} "
        f"WHERE {BATCH_COLUMN} = '{batch}'"
    ).collect()
    return str([row["planted"] for row in rows])


def test_no_publishable_statement_emits_a_row_value_and_the_sample_proves_the_reader_works(
    probe,
):
    """THE PROPERTY THIS REPOSITORY'S PUBLICNESS RESTS ON, checked where it can be checked.

    Every fixture value carries `_SENTINEL`, so any statement that projects a column value
    puts one in its output. The three statements `evidence_sql` returns must emit none.

    THE SWEEP IS `_TAINT_SWEEP` AND NOT `_INCIDENTS`, which is the difference between this
    check and the one that shipped: the corpus's socios rows hold `''` in
    `nome_socio_razao_social`, so the most sensitive column here carries no findable value
    anywhere in the eleven and projecting it in the clear swept clean.

    BOTH CONTROLS ARE IN THIS TEST. A taint check whose reader is broken reports clean over
    everything, so the same reader is pointed at `row_sample_sql` -- NOT publishable -- and
    required to find sentinels. And `assert "MARIA" not in leaked` ran against a batch
    where no column held that string, so deleting `row_sample_sql`'s mask filter outright
    left it green; the tokens are now read out of the fixture first and required to be
    there, making the absence below an absence from the OUTPUT and not from the input."""
    for source, batch in _TAINT_SWEEP:
        for name, sql in evidence_sql(source, _CONFIG).items():
            rendered = str(_run(probe, sql, batch))
            assert _SENTINEL not in rendered, f"{name} on {source}/{batch} emitted a value"

    leaked = str(_run(probe, row_sample_sql(table_spec("socios"), _CONFIG), _MATRIX_BATCH))
    assert _SENTINEL in leaked, "the sentinel reader found nothing where values ARE projected"

    for column, token in _PERSONAL_TOKENS.items():
        assert token in _planted(probe, "socios", column, _MATRIX_BATCH), (
            f"{token} is not in {column} of this batch, so the assertion below is an "
            "absence check over a string the fixture never planted"
        )
        assert token not in leaked, f"{column}'s value reached the NOT-publishable sample"
