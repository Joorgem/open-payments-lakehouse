# src/opl/gold/dimensions.py
"""Build a Kimball SCD2 dimension out of a Data Vault satellite: one row per version,
with a half-open validity interval that is closed in the SAME window pass that orders
the versions.

NO MERGE, AND THE ALTERNATIVE IS NOT "A CHEAPER MERGE". The textbook SCD2 load writes
the new version and then UPDATEs the previous one's end date, which needs a MERGE or an
UPDATE -- and there is none in this repository: `grep -rin "MERGE INTO|\\.merge\\(|
DeltaTable"` over `src/`, `databricks/src/` and `tests/` returns zero, because every
loader here is `mode("append")`. Proving a new write pattern on Free Edition serverless
against 69.2M rows is not a cost this phase pays to save one window function the vault
already runs at that scale (`opl.vault.loading.changed_rows` partitions by key and
orders by `applied_date` with `F.lag`). So `valid_to` is `F.lead(applied_date)` over
that same window, computed before anything is written, and the close is a COLUMN rather
than a second statement. `F.lag` and `F.lead` share one window spec here, so Spark plans
them as a single window operator: one shuffle, both bounds.

WHAT THAT COSTS, STATED PLAINLY, BECAUSE IT IS THE REAL LIMIT OF THIS DESIGN. A version
chain computed in one pass is correct for the source AS IT IS. When the vault gains a
snapshot, the previously-open version's `valid_to` stops being the ceiling -- and an
append cannot revise a row it already wrote. Appending the corrected chain would put TWO
intervals on one surrogate key, so every as-of lookup for that company would return two
rows. `_refuse_a_target_the_source_has_outgrown` refuses that BEFORE the first write and
names the repair (drop the dimension and rebuild it); closing it properly is a MERGE, and
that is the seam a later phase closes rather than one this one pretends away.

`mode("overwrite")` WAS THE OBVIOUS ANSWER AND IT IS NOT AVAILABLE. A derived table
rebuilt wholesale is idempotent by construction and needs none of the above. It is
rejected on a measurement rather than on taste: the local Delta session this project's
tests run against cannot execute a replace at all -- `mode("overwrite").saveAsTable` and
`writeTo(...).createOrReplace()` both raise `AnalysisException: Table X does not support
truncate in batch mode` (probed on pyspark 3.5.9 + delta-spark 3.3.1, and recorded
independently in `tests/test_backfill_snapshot_columns_script.py`). A loader whose only
write path cannot run in CI is a loader whose behaviour is asserted nowhere, which is a
worse trade than the refusal above.

THE SATELLITE DOES NOT CARRY THE BUSINESS KEY, SO THIS IS A JOIN AND NOT A PROJECTION.
`sat_empresa_dados` holds `hub_empresa_hk` and its payload; `cnpj_basico` lives in
`hub_empresa`. That join -- 69.2M rows against 69.06M -- is the price of DV2's own
decomposition rather than a choice made here, and it is paid ONCE: the hub is read
projected down to (hash key, business key) so nothing else of its columns crosses the
shuffle, and so its own `load_date` and `record_source` cannot collide with the
satellite's.

THE SURROGATE KEY IS A HASH, AND THE TWO OBVIOUS GENERATORS ARE BOTH WRONG HERE.
`monotonically_increasing_id()` is unique within one write and depends on partition
assignment, so a rebuild re-keys the dimension -- and the fact stores `company_sk`, so
every fact row would then point at a row that no longer means what it did, with the
join still resolving. `row_number()` over an unpartitioned window is a single-partition
sort of 69.2M rows. A hash of (business key, `valid_from`) is deterministic, needs no
coordination, and is stable under a rebuild.

WHAT A HASH COSTS AND HOW IT IS PAID. `xxhash64` is 64 bits, so over 69,202,818 rows the
birthday probability of ANY collision is about 1.3e-4 -- small, not zero, and a collision
means two dimension versions sharing a key, which is the silent wrong answer a star
cannot recover from. So the load MEASURES it, on the written table, and refuses. That
measurement is one distinct-count over a column of longs, which is cheap beside the load
it verifies (`sat_empresa_dados` itself took 5,635 s to build), and unlike
`load_satellite`'s two optional diagnostics it is not a report an operator may skip: it
decides whether the table is usable. It runs AFTER the write and that is deliberate --
measuring before would mean deriving the whole frame a second time, since serverless
refuses `persist()` (master protocol section 4.3) -- so the refusal says the table on
disk must be dropped rather than implying the write did not happen.

IT IS NOT THE VAULT'S HASH STANDARD AND MUST NOT BE READ AS ONE.
`opl.vault.hashing_spark` owns the BUSINESS-KEY digest: sha256, length-prefixed
components, one spelling, and every join in the vault rests on it. This is a surrogate
key -- an opaque integer whose only contract is uniqueness within this table -- and
nothing joins to it that was not built from this table in the same breath. Spelling it
with the vault's standard would mean truncating a 256-bit hex digest into a long, which
is a second encoding of that standard with worse collision behaviour than the function
built for this. The INPUT is canonical either way: `load_hub` stores the padded business
key, so the value hashed here is the one the vault itself keyed on."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from opl.gold.columns import (
    GHOST_RECORD_SOURCE,
    GHOST_SURROGATE_KEY,
    IS_CURRENT,
    LOAD_DATE,
    RECORD_SOURCE,
    VALID_FROM,
    VALID_FROM_FLOOR,
    VALID_TO,
    VALID_TO_CEILING,
)
from opl.gold.registry import Scd2Dimension
from opl.vault.columns import APPLIED_DATE
from opl.vault.loading import rows_in
from opl.vault.registry import Hub, Satellite

__all__ = [
    "DimensionLoadResult",
    "dimension_rows",
    "instant_literal",
    "load_dimension",
]

_TIMESTAMP = "timestamp"

# How an `applied_date` is rendered to compare against the job's `months` window. One
# spelling, here, because the comparison is only meaningful if both sides agree.
_MONTH_FORMAT = "yyyy-MM"

# Internal to `_bounded`, and named because they are selected through by string: a bare
# literal at both ends is one typo away from a column of NULLs, which would floor every
# version rather than only the first.
_OBSERVED_AT = "_observed_at"
_PRECEDING = "_preceding_version"
_FOLLOWING = "_following_version"


@dataclass(frozen=True)
class DimensionLoadResult:
    """What one dimension build did, and the two numbers that make its row count
    checkable against the layer below."""

    table: str
    appended: int
    # What the target already held before this build, whole-table. A build that appends
    # 0 against a non-zero `already_present` is the idempotent re-run; the two fields
    # keep that distinguishable from a build that had nothing to write.
    already_present: int
    # `rows_in` over the SOURCE SATELLITE. Here so the star's headline claim -- one
    # dimension row per satellite version, plus one ghost -- is a comparison a reader
    # can make from the run log instead of from two documents. Free: Delta answers a
    # whole-table count from the transaction log's file statistics.
    source_versions: int
    # Distinct surrogate keys in the written table. Measured rather than assumed; see
    # the module docstring for why a 64-bit key needs measuring and why the measurement
    # happens here rather than before the write.
    distinct_keys: int


def instant_literal(value: datetime) -> Column:
    """`value` as a timestamp Column, built from its ISO text rather than from the
    `datetime` itself.

    `F.lit(datetime)` GOES THROUGH `time.mktime`, which is a C-runtime call and which
    raises `OverflowError: mktime argument out of range` on this project's Windows dev
    box for anything past year 3000. Casting the ISO string instead is parsed by Spark
    in the session timezone with no Python conversion involved, so the sentinel values
    in `opl.gold.columns` are chosen for what can be READ BACK rather than for what can
    be written."""
    return F.lit(value.isoformat(sep=" ")).cast(_TIMESTAMP)


def _refuse_a_mismatched_source(
    dimension: Scd2Dimension, satellite: Satellite, hub: Hub
) -> None:
    """The dimension, its satellite and that satellite's hub arrive as three arguments,
    so something has to check they belong together.

    They are three arguments for `opl.vault.satellites._refuse_a_mismatched_hub`'s
    reason: a loader that resolved its source through the module-level registry could
    not be driven with a throwaway spec, and the registry is exactly the thing a later
    task must be able to extend without this file changing. The cost is this check, and
    what it prevents is worth naming -- handed another satellite, this builds a
    perfectly well-formed dimension about a different table; handed another hub, the
    join on the hash key returns nothing and the dimension is empty but for its ghost,
    with the load reporting success."""
    if satellite.name != dimension.source_satellite:
        raise ValueError(
            f"dimension {dimension.name!r} declares source "
            f"{dimension.source_satellite!r} and was handed satellite "
            f"{satellite.name!r}. It would be built from another table's versions and "
            "would not fail doing it -- resolve the source with "
            "opl.gold.registry.table_spec and opl.vault.domains.table_spec rather than "
            "passing a satellite by hand"
        )
    if hub.name != satellite.parent:
        raise ValueError(
            f"satellite {satellite.name!r} hangs off hub {satellite.parent!r} and was "
            f"handed hub {hub.name!r}. The join is on the hash key, so it would match "
            "nothing: the dimension would hold its ghost and no versions, and the load "
            "would report success -- resolve the parent with "
            "opl.vault.domains.parent_hub"
        )


def _refuse_a_window_that_is_not_the_snapshots_the_source_holds(
    source: DataFrame, months: Sequence[str], dimension: Scd2Dimension
) -> None:
    """Refuse a `months` window that is not exactly the set of snapshots the satellite
    carries.

    THE WINDOW IS A DECLARATION HERE AND NOT A FILTER, which is the one honest job it
    can do for an SCD2 build. `valid_to` for a company's last version is the date of its
    NEXT version, so a build that read only some snapshots would close intervals against
    versions it could not see -- and report success. Narrowing is therefore not
    available, and a parameter that narrowed nothing would be decoration.

    What it buys instead is real: a build launched with the window the operator has in
    mind REFUSES if the vault has since gained a snapshot they did not know about,
    rather than quietly producing a dimension that covers more than they said. It is
    the only thing tying the launch's stated intent to what the derivation actually
    covers.

    One column scan and a shuffle over a handful of distinct values -- cheap beside the
    join and window that follow it, and it runs before any of them."""
    held = {
        row[0]
        for row in source.select(
            F.date_format(F.col(APPLIED_DATE), _MONTH_FORMAT)
        ).distinct().collect()
    }
    declared = set(months)
    if held != declared:
        raise ValueError(
            f"refusing to build {dimension.name!r}: the window names "
            f"{sorted(declared)} and the snapshots it actually holds are "
            f"{sorted(held)}. An SCD2 build cannot be narrowed -- a version's end date "
            "is the next version's start, so reading a subset closes intervals against "
            "versions it cannot see -- so this parameter states which snapshots the "
            "dimension covers and is checked rather than applied. Pass the window the "
            "vault actually holds, or load the missing snapshot first"
        )


def _bounded(satellite: Satellite, hub: Hub, source: DataFrame, keys: DataFrame) -> DataFrame:
    """The satellite's versions, joined to their business key, each carrying the
    `applied_date` of the version before and after it.

    ONE WINDOW SPEC FOR BOTH BOUNDS. `F.lag` and `F.lead` are taken over the identical
    partition-and-order, so Spark plans a single window operator: the versions are
    sorted once and both neighbours fall out of that one pass. Partitioned by the
    BUSINESS key rather than by the hash key because the two are 1:1 after the join and
    the business key is eight characters against the digest's sixty-four -- the same
    partition, a narrower shuffle.

    `applied_date` IS CAST TO AN INSTANT HERE, ONCE. The vault stores it as a DATE, the
    fact will ask as of a TIMESTAMP, and leaving the two to meet in a predicate would
    make every as-of join rest on an implicit widening nobody wrote down. Midnight of
    the snapshot's own date is the only reading available: the RFB tells us the day, not
    the hour."""
    business = hub.business_key_columns
    joined = source.join(keys, on=hub.hash_key, how="inner").select(
        *(F.col(column) for column in business),
        F.col(APPLIED_DATE).cast(_TIMESTAMP).alias(_OBSERVED_AT),
        *(F.col(column) for column in satellite.payload_columns),
        F.col(RECORD_SOURCE),
    )
    ordered = Window.partitionBy(*business).orderBy(_OBSERVED_AT)
    return joined.select(
        "*",
        F.lag(_OBSERVED_AT).over(ordered).alias(_PRECEDING),
        F.lead(_OBSERVED_AT).over(ordered).alias(_FOLLOWING),
    )


def _versioned(
    dimension: Scd2Dimension,
    satellite: Satellite,
    hub: Hub,
    bounded: DataFrame,
    load_date: datetime,
) -> DataFrame:
    """One dimension row per satellite version, in the dimension's declared column
    order.

    THE ORDER IS PINNED BY A TEST AND IS LOAD-BEARING, not tidy: a Delta append matches
    POSITIONALLY unless `mergeSchema` says otherwise, so two builds projecting the same
    columns in two orders would write each other's values without failing.

    THE SURROGATE KEY IS TAKEN OVER (business key..., `valid_from`) IN THAT ORDER, and
    the order is part of the key: permuting it re-keys the whole table. `valid_from` and
    not `applied_date`, so that the first version's key is derived from the value the
    row actually carries."""
    valid_from = F.when(
        F.col(_PRECEDING).isNull(), instant_literal(VALID_FROM_FLOOR)
    ).otherwise(F.col(_OBSERVED_AT))
    valid_to = F.coalesce(F.col(_FOLLOWING), instant_literal(VALID_TO_CEILING))
    business = [F.col(column) for column in hub.business_key_columns]
    return bounded.select(
        F.xxhash64(*business, valid_from).alias(dimension.surrogate_key),
        *business,
        *(F.col(column) for column in satellite.payload_columns),
        valid_from.alias(VALID_FROM),
        valid_to.alias(VALID_TO),
        (valid_to == instant_literal(VALID_TO_CEILING)).alias(IS_CURRENT),
        F.lit(load_date).alias(LOAD_DATE),
        F.col(RECORD_SOURCE),
    )


def _ghost_like(
    spark: SparkSession, versioned: DataFrame, dimension: Scd2Dimension, load_date: datetime
) -> DataFrame:
    """The one unknown-member row, shaped from `versioned`'s own schema.

    BUILT FROM THE SCHEMA RATHER THAN FROM A COLUMN LIST, so the two frames cannot
    disagree about order or type -- a ghost that declared its payload `string` would
    break the union the day a satellite carries something else, and one that listed its
    columns by hand would go stale the day a payload column is added.

    IT SPANS ALL TIME, which is a statement rather than a filler: what is unknown is
    unknown at every instant. It costs nothing, because the row is not reachable by any
    join -- it carries no business key -- and it keeps the ghost inside every invariant
    the versioned rows obey (no NULL bound, `is_current` iff the interval is open)
    instead of making it the exception every query has to remember."""
    fixed = {
        dimension.surrogate_key: F.lit(GHOST_SURROGATE_KEY),
        VALID_FROM: instant_literal(VALID_FROM_FLOOR),
        VALID_TO: instant_literal(VALID_TO_CEILING),
        IS_CURRENT: F.lit(True),
        LOAD_DATE: F.lit(load_date),
        RECORD_SOURCE: F.lit(GHOST_RECORD_SOURCE),
    }
    return spark.range(1).select(
        *(
            fixed.get(field.name, F.lit(None)).cast(field.dataType).alias(field.name)
            for field in versioned.schema.fields
        )
    )


def dimension_rows(
    spark: SparkSession,
    dimension: Scd2Dimension,
    *,
    satellite: Satellite,
    hub: Hub,
    source: DataFrame,
    keys: DataFrame,
    load_date: datetime,
) -> DataFrame:
    """Every row the dimension will hold: one per satellite version, plus the ghost.

    Public for `opl.vault.satellites.satellite_candidates`' reason -- the frame is worth
    having without the write, both for a test and for whatever builds the fact next."""
    versioned = _versioned(
        dimension, satellite, hub, _bounded(satellite, hub, source, keys), load_date
    )
    return versioned.unionByName(_ghost_like(spark, versioned, dimension, load_date))


def _refuse_a_target_the_source_has_outgrown(
    spark: SparkSession, dimension: Scd2Dimension, rows: DataFrame, target_table: str
) -> None:
    """Refuse when the derived chain is not the chain the target already holds.

    THE LIMIT OF AN APPEND-ONLY SCD2, MADE LOUD. The check is on (surrogate key,
    `valid_to`): the surrogate key does not move when a company gains a version -- it is
    hashed over `valid_from`, which is unchanged -- so what a new snapshot changes is
    exactly the previously-open row's END. A row of that pair missing from the target is
    therefore the signal that the source has moved on, and appending it would put two
    intervals on one surrogate key.

    BEFORE THE FIRST WRITE, per master protocol section 4.4, because `max_retries: 0`
    does not prevent a retry and this is the branch a retry lands in.

    WHAT IT DOES NOT COVER, stated rather than implied: a target holding rows the
    derivation no longer produces -- a company deleted from the vault by hand -- passes
    this check, because every derived row is still present. Nothing in this project
    deletes from a satellite, and the surrogate-key uniqueness measurement would not see
    it either; a MERGE-based rebuild is what closes it."""
    existing = spark.read.table(target_table).select(dimension.surrogate_key, VALID_TO)
    revised = (
        rows.select(dimension.surrogate_key, VALID_TO)
        .join(existing, on=[dimension.surrogate_key, VALID_TO], how="left_anti")
        .limit(1)
        .count()
    )
    if revised:
        raise ValueError(
            f"refusing to load {target_table}: it already holds a version chain that "
            f"this build does not reproduce, so {dimension.name!r} would gain a second "
            "interval on a surrogate key it already has and every as-of lookup for that "
            "company would return two rows. An append-only SCD2 cannot revise an "
            "interval it already closed -- which is what a new snapshot in the source "
            "does to the open version. Drop the table and rebuild it; nothing has been "
            "written by this run"
        )


def _distinct_surrogate_keys(
    spark: SparkSession, dimension: Scd2Dimension, target_table: str, rows: int
) -> int:
    """How many distinct surrogate keys the written table holds, refusing if that is not
    every row.

    ONE COLUMN, NOT A TUPLE, and that is deliberate rather than incidental: master
    protocol section 4.8 records that `COUNT(DISTINCT a, b, c)` drops NULL-bearing rows
    and cost this project 8,761 of them once. The surrogate key is never NULL -- a hash
    of anything is a value, and the ghost's is a literal -- so a single-column distinct
    count is total over the table, the ghost included. That totality is what lets ONE
    number cover both hazards: a collision between two versions, and the astronomically
    unlikely versioned row that hashed onto the ghost's reserved key."""
    distinct = spark.read.table(target_table).select(dimension.surrogate_key).distinct().count()
    if distinct != rows:
        raise ValueError(
            f"{target_table} holds {rows} rows and only {distinct} distinct "
            f"{dimension.surrogate_key} values. Two dimension versions share a surrogate "
            "key, so every fact joining on it would match both -- silently. The key is a "
            "64-bit hash and a collision has roughly a 1.3e-4 chance at this row count, "
            "so this is the outcome that was measured for rather than an impossible one. "
            "THE TABLE ON DISK IS ALREADY WRITTEN and must be dropped; the repair is a "
            "wider key, not a re-run, which would reproduce the same collision"
        )
    return distinct


def load_dimension(
    spark: SparkSession,
    dimension: Scd2Dimension,
    *,
    satellite: Satellite,
    hub: Hub,
    source_table: str,
    hub_table: str,
    target_table: str,
    load_date: datetime,
    months: Sequence[str],
) -> DimensionLoadResult:
    """Build `dimension` from `source_table`'s versions and `hub_table`'s business keys,
    and append it -- once, whole, with every interval already closed.

    `load_date` is an argument with no default, for `opl.vault.hubs.load_hub`'s reason:
    a loader that stamps its own clock cannot be asserted against, and in the data it
    would make the LDTS a record of when the pipeline ran rather than a value the job's
    own parameters pin.

    Idempotent: a re-run over an unchanged source writes nothing and reports 0 appended.
    A re-run over a source that GAINED a snapshot is refused before its first write --
    `_refuse_a_target_the_source_has_outgrown` is where that limit is argued."""
    _refuse_a_mismatched_source(dimension, satellite, hub)
    source = spark.read.table(source_table)
    _refuse_a_window_that_is_not_the_snapshots_the_source_holds(source, months, dimension)
    # PROJECTED DOWN TO (hash key, business key) BEFORE THE JOIN, so the hub's own
    # `load_date` and `record_source` never reach it -- they would collide by name with
    # the satellite's and make every column reference ambiguous.
    keys = spark.read.table(hub_table).select(hub.hash_key, *hub.business_key_columns)
    rows = dimension_rows(
        spark, dimension, satellite=satellite, hub=hub, source=source, keys=keys,
        load_date=load_date,
    )
    before = rows_in(spark, target_table)
    if before:
        _refuse_a_target_the_source_has_outgrown(spark, dimension, rows, target_table)
        after = before
    else:
        rows.write.format("delta").mode("append").saveAsTable(target_table)
        after = rows_in(spark, target_table)
    return DimensionLoadResult(
        table=target_table,
        appended=after - before,
        already_present=before,
        source_versions=rows_in(spark, source_table),
        distinct_keys=_distinct_surrogate_keys(spark, dimension, target_table, after),
    )
