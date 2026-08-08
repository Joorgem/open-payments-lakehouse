# src/opl/vault/reference.py
"""Load a DV2 reference table: one row per natural key, ever, insert-only -- and the
routing that keeps six reference types in one bronze table from merging on a shared
`codigo`.

THE TRAP THIS MODULE EXISTS FOR, MEASURED (`01f192c7-7c0b-169f-9a14-fae6761be7e9`,
`01f192c7-9820-18be-ba93-5167bf5e1ede`). `bronze_cnpj_lookup` holds six reference
types in one table -- CNAE, motivo, município, natureza jurídica, país, qualificação
de sócio -- distinguished only by `_source_file`, and `codigo` is unique WITHIN a
type and collides ACROSS types: `codigo='05'`, `'08'`, `'09'`, `'10'` each name a
motivo AND a qualificação, because MOTI and QUALS share a two-character width;
`municipio`/`natureza_juridica` share a four-character one the same way. Neither
`codigo` alone nor its width identifies the type. A loader that grouped this table on
`codigo` alone would silently merge two reference types into one row -- right row
count, right column names, one description replaced by the other's, nothing failing.

THE ROUTING IS `lookup_type_from_filename`, NOT A SECOND SPELLING OF IT. Bronze's own
ingest already computes a `lookup_type` column from the same suffix map
(`opl.bronze.autoloader.lookup_type_column`), but that column is not part of the
`lookup` contract (`opl.contracts.cnpj_schemas.TABLES['lookup']` is `['codigo',
'descricao']` only) and this loader does not read it -- reading it would make this
layer trust a SECOND, independently-coded computation of the same rule rather than
apply the one function this repository names as canonical. `_routed_files` derives
`lookup_type` fresh from `_source_file`, per DISTINCT filename in the window (six
today, cheap to collect however large the table grows), by calling
`lookup_type_from_filename` directly. A filename that function cannot classify
raises THERE, before any candidate is built and before any write -- so a lookup file
whose naming convention changed is a loud failure on the very next load, for
whichever reference table happens to load first, rather than a silently empty one
for just that type.

WHY THIS IS INSERT-ONLY AND NEVER UPDATES A DESCRIPTION, reusing `opl.vault.hubs`'
shape because it is right here, not because it is the only shape this package has.
`bronze_cnpj_lookup` holds ONE month, 2026-06: the 2026-07 lookup zips were never
published in that month's set, so there is no second observation to compare a
`descricao` against, no `applied_date` sequence to order, and no absence for the
observation ledger to report -- these tables have NO HISTORY TO RECONSTRUCT, and
nothing below claims otherwise. Building a satellite-shaped change detector over data
that cannot exercise a change would be exactly the untested path this phase's review
record already carries twice over (empresas' end-dating, the satellite's dedup rule
under `opl.vault.satellites`) -- a third would be added on purpose. So a reference
table loads like a hub: anti-joined on its natural key, appended once, never
revisited. IF THE RFB EVER REVISES A CODE'S DESCRIPTION IN A LATER SNAPSHOT, THIS
LOADER WILL NOT PICK IT UP -- the anti-join drops the candidate because its `codigo`
is already present, and the row already written keeps its first-seen `descricao`
forever. That is a stated limitation, not a silent one; the mechanism is proven by
a synthetic second month in `tests/vault/test_reference_vault.py`, since real bronze
has none to measure it against.

THE DEDUP TIE-BREAK, MEASURED TO NEVER FIRE TODAY. `codigo` is unique within a type
in every one of the six 2026-06 counts (CNAE 1,359 rows / 1,359 distinct `codigo`,
and the same identity for the other five). A duplicate `(codigo, lookup_type)` pair
is therefore UNMEASURED, not assumed absent, and the deterministic pick -- earliest
month, then lowest `record_source`, then lowest `descricao` -- is `F.min` over a
struct for the reason it is everywhere else in this package: two runs over the same
data must agree.

THIS PACKAGE HAS **TWO** TIE-BREAK DOCTRINES AND THIS TABLE IS ON THE FIRST ONE, which
is worth stating because an earlier version of this paragraph credited both to
"earliest wins, and that is the honest reading" and `opl.vault.effectivity` says the
opposite of one of them, four lines apart:

  - EARLIEST WINS, AND IT MEANS SOMETHING. `opl.vault.hubs.hub_candidates` (through
    `loading.earliest_record_source`), `opl.vault.effectivity` (the earliest delivered
    `data_entrada_sociedade`), and this module's leading `_snapshot_month`. The chosen
    value is the first observation, which is a claim a reader can check against the
    source.
  - DETERMINISTIC BUT ARBITRARY. `opl.vault.satellites.satellite_candidates` breaks a
    tie on the lowest `hash_diff`, and a digest is not earlier, truer or better than
    its twin -- it is only stable. `opl.vault.effectivity`'s docstring names that
    contrast explicitly ("unlike `opl.vault.satellites`' lowest-`hash_diff` tie-break
    it is not arbitrary"), so citing that module here as a fellow "earliest" was a
    direct contradiction of it.

The tail of this module's own struct is in the second category and says so: after
`_snapshot_month`, `record_source` and `descricao` are ordered lexicographically
because SOMETHING must be, not because a lower one is more true."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.bronze.autoloader import SOURCE_FILE_COLUMN
from opl.bronze.lookup_routing import lookup_type_from_filename
from opl.vault.columns import LOAD_DATE, RECORD_SOURCE
from opl.vault.hashing_spark import refuse_non_string_columns
from opl.vault.loading import (
    BRONZE_RECORD_SOURCE,
    SNAPSHOT_MONTH_COLUMN,
    read_snapshot_window,
    rows_in,
)
from opl.vault.registry import ReferenceTable

# Internal to the tie-break, selected through by field: a bare string at both ends
# is one typo from a column of NULLs.
_CHOSEN = "_chosen"


@dataclass(frozen=True)
class ReferenceLoadResult:
    """What one reference table load did. `appended` and `already_present` are
    derived from the target's own row count before and after the append, so
    `appended` is what LANDED rather than what was planned."""

    table: str
    appended: int
    # What the target already held before this load, per `HubLoadResult.
    # already_present`'s reason: not "how many of this window's codes were already
    # there" -- the target may hold codes from months outside the window, and
    # reporting a narrower number than the one that was measured would be a claim
    # the count cannot support.
    already_present: int
    # Rows sharing a (`codigo`, month) pair -- WITHIN one month, like
    # `opl.vault.partners._collapsed_duplicates` -- folded to one. NOT a count of
    # `codigo` recurring ACROSS months in a multi-month window: that recurrence is
    # the ordinary shape of a reference list the RFB republishes whole every time,
    # `reference_candidates` folds it the same way (earliest month wins) without it
    # being a data-quality defect, and counting it here would report thousands on
    # the first multi-month load for a table that folded nothing wrong. See
    # `_collapsed_duplicates` and the module docstring: zero in every measured
    # 2026-06 count, reported because the dedup argument rests on this number
    # staying small.
    collapsed_duplicates: int


def _routed_files(source: DataFrame, lookup_type: str) -> list[str]:
    """Every `_source_file` this window carries that routes to `lookup_type`.

    CALLS `lookup_type_from_filename` OVER EVERY DISTINCT FILENAME PRESENT, not only
    the ones a caller expects to match, so a filename the function cannot classify
    raises HERE -- before a candidate is built for ANY reference table -- rather than
    being silently excluded from every one of them.

    THE MISSING-COLUMN REFUSAL IS NAMED, like every sibling's. `reference_candidates`
    refuses `ref.natural_key` and `ref.payload` by name through
    `refuse_non_string_columns`, and then this function reached for `_source_file`
    raw -- so the ONE column this module routes on was the one whose absence arrived as
    a Spark `AnalysisException` about a column list, where every other missing column in
    this package gives a `ValueError` saying which table is wrong and why."""
    if SOURCE_FILE_COLUMN not in source.columns:
        raise ValueError(
            f"the reference source has no {SOURCE_FILE_COLUMN!r} column, so the six "
            "reference types in it cannot be told apart. `codigo` is unique WITHIN a "
            "type and collides ACROSS types, so a load without this column would not "
            "fail -- it would merge two reference types onto one row. Every bronze "
            f"table carries {SOURCE_FILE_COLUMN!r}; a source that does not is not a "
            "bronze table this loader can route"
        )
    files = [row[0] for row in source.select(SOURCE_FILE_COLUMN).distinct().collect()]
    return [f for f in files if lookup_type_from_filename(f) == lookup_type]


def _routed_to_type(source: DataFrame, lookup_type: str) -> DataFrame:
    """`source`, narrowed to the rows whose `_source_file` routes to `lookup_type`.

    `isin([])` ON AN EMPTY LIST IS AVOIDED RATHER THAN RELIED ON: this window simply
    has no row of this type (a narrow `months` window, most often), and the explicit
    `F.lit(False)` says so without depending on how Spark's own `isin` treats an
    empty argument."""
    files = _routed_files(source, lookup_type)
    if not files:
        return source.filter(F.lit(False))
    return source.filter(F.col(SOURCE_FILE_COLUMN).isin(files))


def reference_candidates(
    spark: SparkSession,
    ref: ReferenceTable,
    *,
    source_table: str,
    months: Sequence[str] | None,
) -> DataFrame:
    """One row per `codigo` in the window, routed to `ref.lookup_type` and
    deduplicated by the earliest (month, record_source, payload).

    ROUTED BEFORE THE GROUP-BY, NOT AFTER, which is the whole point of this module:
    grouping on `codigo` first and filtering by type second would already have
    merged motivo and qualificação into one group before the filter ever ran."""
    source = read_snapshot_window(spark, source_table, months)
    refuse_non_string_columns(source, (ref.natural_key, ref.payload))
    keyed = _routed_to_type(source, ref.lookup_type).select(
        F.col(ref.natural_key),
        F.col(ref.payload),
        F.col(SNAPSHOT_MONTH_COLUMN),
        F.col(BRONZE_RECORD_SOURCE),
    )
    return (
        keyed.groupBy(ref.natural_key)
        .agg(
            F.min(F.struct(SNAPSHOT_MONTH_COLUMN, BRONZE_RECORD_SOURCE, ref.payload))
            .alias(_CHOSEN)
        )
        .select(
            ref.natural_key,
            F.col(f"{_CHOSEN}.{ref.payload}").alias(ref.payload),
            F.col(f"{_CHOSEN}.{BRONZE_RECORD_SOURCE}").alias(RECORD_SOURCE),
        )
    )


def _collapsed_duplicates(
    spark: SparkSession, ref: ReferenceTable, source_table: str, months: Sequence[str] | None
) -> int:
    """Routed rows in the window, minus distinct (`codigo`, month) pairs among them --
    the same second-pass shape `opl.vault.partners._collapsed_duplicates` uses, and
    projecting the SAME two columns it does (`partners.py` selects `(link key,
    SNAPSHOT_MONTH_COLUMN)`), for the same reason: this counts a genuine data-quality
    duplicate -- two source rows for one `codigo` in ONE month -- not a `codigo`
    recurring in a LATER month, which `reference_candidates`'s own fold treats as the
    normal shape of a republished reference list rather than something to warn about.
    Projecting `ref.natural_key` alone here would count every such recurrence too, and
    on the first multi-month load would report thousands where nothing was actually
    wrong; `test_a_later_months_changed_description_is_not_reflected` pins the zero."""
    source = read_snapshot_window(spark, source_table, months)
    keyed = _routed_to_type(source, ref.lookup_type).select(
        ref.natural_key, SNAPSHOT_MONTH_COLUMN
    )
    return keyed.count() - keyed.distinct().count()


def load_reference_table(
    spark: SparkSession,
    ref: ReferenceTable,
    *,
    source_table: str,
    target_table: str,
    load_date: datetime,
    months: Sequence[str] | None = None,
) -> ReferenceLoadResult:
    """Append every `codigo` of `ref.lookup_type` that `target_table` does not
    already hold, stamped with `load_date`.

    ANTI-JOIN THEN APPEND, LIKE `load_hub`, AND FOR THE SAME THREE REASONS: not a
    MERGE (this table is small, but there is still no reason to pay a rewrite for a
    load that should be a no-op), not delete-then-append (two Delta commits, a
    failure window between them), an anti-join on the natural key is one commit and a
    free re-run. `load_date` has no default for `load_hub`'s reason: a loader that
    stamps its own clock cannot be asserted against.

    NOTED, NOT RESTRUCTURED: `_collapsed_duplicates` and `reference_candidates` each
    re-read and re-route the source, so one load costs two Spark reads and two
    driver-side collects -- twelve across all six tables. Nothing at 7,408 rows;
    sharing one pre-routed frame between the two is the cleanup once this table
    stops being tiny."""
    before = rows_in(spark, target_table)
    collapsed = _collapsed_duplicates(spark, ref, source_table, months)
    candidates = reference_candidates(spark, ref, source_table=source_table, months=months)
    if before:
        candidates = candidates.join(
            spark.read.table(target_table).select(ref.natural_key),
            on=ref.natural_key,
            how="left_anti",
        )
    (
        candidates.select(
            ref.natural_key,
            ref.payload,
            F.lit(load_date).alias(LOAD_DATE),
            F.col(RECORD_SOURCE),
        )
        .write.format("delta").mode("append").saveAsTable(target_table)
    )
    return ReferenceLoadResult(
        table=target_table,
        appended=rows_in(spark, target_table) - before,
        already_present=before,
        collapsed_duplicates=collapsed,
    )
