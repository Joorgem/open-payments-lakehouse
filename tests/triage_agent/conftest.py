"""The corpus the evidence tests are driven out of: real tables at the measured sizes.

SPLIT OUT OF `test_evidence.py` WHEN IT REACHED THE FILE CAP, in a commit free of
behaviour. The bodies now sit in `test_evidence_census.py` (how many, and what verdict) and
`test_evidence_sample.py` (what a row looks like, and what may leave). Every name below is
read by BOTH halves -- which is the seam, and the reason they are here rather than in
either file.

`probe` IS SESSION-SCOPED AND WAS MODULE-SCOPED, WHICH IS THE ONE BEHAVIOURAL CHANGE THE
SPLIT FORCED. A module-scoped fixture in a conftest read by two modules builds its 21
views, and the reconciliation over them, TWICE. The scope the `spark` fixture it depends on
already uses is the one that costs once, and the `DROP DATABASE ... CASCADE` teardown is
unchanged: a session-scoped dependant is finalised BEFORE the session-scoped `spark` it was
built from, so the drop still runs against a live session.

WHY THE FIXTURE CARRIES 2,000 / 1,797 / 1,786 / 4 / 1 / 1 AND NOT SIX EQUAL HANDFULS. The
counts are the corpus's own (`docs/f6-run-evidence.md` 0.3), and they are unequal on
purpose: a fixture that gave every incident three rows would let a census that read the
wrong batch, or the whole table, or half of one, return a plausible number for every
assertion in either half. With this spread, any of those returns a number that belongs to
another incident and is visible on sight. The three-order-of-magnitude gap between 2,000
and 1 is doing the work a hand-checked constant cannot.

THE RECONCILIATION IS BUILT BY THE SHIPPED `batch_grain_sql` OVER THESE SAME TABLES, not
hand-written to look like it. So the counts these files assert about the reconciliation are
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
    batch_grain_sql,
    create_view_ddl,
)
from opl.bronze.registry import REGISTRY, table_spec
from opl.config import OplConfig
from opl.contracts.catalogue import columns_for
from opl.triage_agent.evidence import (
    EMPTY,
    NULL_VALUE,
    PRESENT,
    REPLACEMENT_CHAR,
    quarantine_census_sql,
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
# holding the two modules equal is in `test_evidence_contract.py`; what these files need is
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

# The batch the one declared hold is about. Named once here so the tests that assert the
# hold fires and the tests that assert it can be removed cannot drift onto two batches.
_HELD_BATCH = _PAYMENTS_BATCH

# INVENTED, and labelled so no later reader mistakes either for a measurement.
#
# `_MATRIX_BATCH` exists because the corpus cannot express what the mask/empty pair needs:
# its 3,583 socios rows are uniform, and the masked/unmasked vocabulary has to be read off
# rows that differ. `_TWO_REASON_BATCH` exists because NO corpus batch carries two reject
# reasons, so without it a census that reported the batch total under one reason's name
# would pass every other assertion in these files.
# `_NULL_REASON_BATCH` exists because no corpus group has a NULL reject reason, the only
# input that tells the census ladder's discriminator from the one it was chosen over.
_MATRIX_BATCH = "invented_shapes_matrix"
_TWO_REASON_BATCH = "invented_two_reason_batch"
_NULL_REASON_BATCH = "invented_null_reason_batch"

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
# `nome_socio_razao_social AS leaked_name` to `row_shapes_sql` left that file green.
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


@pytest.fixture(scope="session")
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
