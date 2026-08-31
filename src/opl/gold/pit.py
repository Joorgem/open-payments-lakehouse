# src/opl/gold/pit.py
"""Build a Data Vault point-in-time table: one row per (hub key, `as_of_date`), carrying
the `applied_date` of the version IN FORCE in each satellite at that instant.

WHAT IT DEFEATS, MEASURED ON THIS VAULT RATHER THAN ASSERTED FROM THE LITERATURE. Two
satellites of one hub change on their own dates, so the query everybody writes --
`hub JOIN sat_a USING (hk, applied_date) JOIN sat_b USING (hk, applied_date)` -- returns
only the keys that happened to move in BOTH on the same day. Task 0 ran both sides at
2026-07-11 (`docs/f3-run-evidence.md` §0.5, statements `01f196b3-55c2…` and
`01f196b3-b82f…`): the naive join answers **514,504**, the as-of answer is **72,318,968**.
1,141,850 establishments would be handed a NULL address that is sitting in the June row
and still in force; 499,630 the other way round. That is what "timeline collapse" costs
here, and it is why the pointers below are `max(applied_date <= as_of_date)` and never an
equality.

THIS TABLE JOINS TO NOTHING IN THIS STAR, AND THE EVIDENCE MUST SAY SO RATHER THAN DRAW
AN ARROW. `dim_company` is at empresa grain (eight-character `cnpj_basico`, which is what
`opl.contracts.payments` carries); `dim_merchant` at estabelecimento grain is deferred
for exactly that reason; `dim_geography` is skipped. So no fact and no dimension in this
lakehouse reaches `pit_estabelecimento` -- it is a MECHANISM DEMONSTRATION OUTSIDE THE
STAR, which is the same charge the phase plan levels at `dim_merchant` and which is
levelled here at this table too rather than only at that one. THE ONE CHANGE THAT WOULD
PULL IT IN: a `dim_merchant` at estabelecimento grain, which needs the payment
counterparties to carry a 14-DIGIT CNPJ. That is a change to `opl.generator`'s contract
(a `SCHEMA_VERSION` bump), not to this layer, and on the day it lands this table is the
navigation index every as-of merchant lookup goes through. One further consequence,
stated because it is genuinely unusual: nothing downstream breaks if this job is never
run. No other gold job in this repository can say that.

THE GRAIN IS (hub key, as_of_date) AND THE AS-OF SET IS MEASURED, NEVER DECLARED. It is
the distinct union of `applied_date` across every satellite -- {2026-06-13, 2026-07-11}
today -- because those are the only instants at which any answer in this vault changes,
so a denser calendar would be rows that repeat their neighbour and a sparser one would
skip a state the vault actually held. A KEY DOES NOT APPEAR AT AN AS-OF DATE BEFORE IT
WAS FIRST OBSERVED, and that falls out of the mechanism rather than being a second rule:
a row whose every pointer is still NULL after the fill is a row for which no satellite
had anything to say, and it is dropped.

NO GHOST ROW, AND THE ABSENCE IS A DECISION RATHER THAN AN OVERSIGHT. Every other gold
table in this repository carries one, so a reader who found none here would reasonably
suspect it was forgotten. A ghost exists so that a foreign key which resolved to nothing
still lands somewhere -- `COALESCE(<lookup>, GHOST_SURROGATE_KEY)` at fact-build time.
Nothing holds a foreign key to a PIT: it is read by JOINING to it on (hash key,
`as_of_date`) from the hub side, never by dereferencing a key stored elsewhere, so an
unknown member would be a row no query could arrive at. What a PIT owes instead is that a
key with nothing to say at an instant is ABSENT from it -- which is what the final filter
below does, and what the "not before it was first observed" rule is.

HOW IT IS COMPUTED, AND WHY IT IS A UNION AND NOT A JOIN. The grid (every hub key x every
as-of date, pointers NULL) and the observations (each satellite's own (hash key,
`applied_date`) pairs, tagged into that satellite's pointer column) are `unionByName`d
into ONE frame, collapsed by `groupBy(hash key, as_of_date)`, and then filled forward by
one window per pointer over `partitionBy(hash key).orderBy(as_of_date)` with a RANGE frame
of unbounded-preceding..current-row. `max` over that frame IS `max(applied_date <=
as_of_date)`, because a pointer column is non-NULL only where its value equals the row's
own `as_of_date`. RANGE and not ROWS: a range frame includes every PEER of the current
row, so the answer cannot depend on how ties are ordered.

NO `persist()`, NO `cache()` -- serverless refuses both (master protocol §4.3) and an AST
sweep catches them. WHERE THE EXPENSIVE FRAME IS CONSUMED ONCE: `pit_rows` derives the
~291M-row union exactly once and hands it to exactly one `groupBy`; both pointer windows
share ONE window spec, so Spark plans them as a single window operator (the trick
`opl.gold.dimensions._bounded` already uses for `lag`/`lead`). The satellites are read
twice in total and never more -- once for `as_of_dates`, over the single `applied_date`
column with a partial aggregate that emits at most one row per partition, and once inside
the union for (hash key, `applied_date`). The reconciliation counts read the WRITTEN
table, not the frame, for `opl.gold.dimensions._distinct_surrogate_keys`' reason:
measuring before the write would mean deriving the whole thing a second time.

EVERY JOIN AUDITED FOR NULL, BECAUSE A PIT IS AN OUTER-JOIN SHAPE BY NATURE AND THIS
REPOSITORY HAS ALREADY PAID FOR ONE. `docs/f2-wave-1-run-evidence.md` records a `LEFT ANTI
JOIN … USING` on a nullable partner key inventing **8,757 phantom departures**, because
NULL never matches under plain equality. There is exactly ONE join in this module -- `_new_rows`'
anti-join on (hash key, `as_of_date`) -- and both of its keys are provably non-NULL:
the hash key is `opl.vault.hashing_spark.hash_key_column`, whose `_encoded` returns a
non-NULL token on every branch INCLUDING the NULL one, so the digest is never NULL; and
`as_of_date` is refused if any satellite carries a NULL `applied_date`, by
`_refuse_an_unusable_as_of_set` below, before a single row is derived. Everything else
here is a `unionByName` or a `groupBy`, neither of which drops a NULL-bearing row.

APPEND-ONLY, AND A PIT IS MONOTONE UNDER A LATER SNAPSHOT WHERE AN SCD2 CHAIN IS NOT.
That difference is worth stating because the two loaders sit beside each other.
`opl.gold.dimensions` must REFUSE a source that gained a snapshot, since a version's
`valid_to` is the next version's `valid_from` and an append cannot revise a closed
interval. Here a later snapshot adds a whole new as-of layer and moves no pointer already
written: `max(applied <= d)` for a d that already exists cannot change when a date after d
arrives. What is NOT monotone is a snapshot loaded OUT OF ORDER -- backfilling 2026-07
into a table built over 2026-06 and 2026-08 would move pointers at 2026-08 -- so that
exact case is refused before the first write, in the shape `opl.vault.loading.changed_rows`
records for the same hazard ("the correction is to load in order, not to rewrite")."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from opl.gold.columns import LOAD_DATE, RECORD_SOURCE

# ONE TIMESTAMP PATH FOR THE WHOLE LAYER, and this module is the one that had a second.
# `opl.gold.conformed` imports it for the same reason and states the same argument:
# `F.lit(datetime)` converts through `time.mktime` -- the DRIVER's operating-system zone --
# where every other instant gold writes is parsed by Spark in the SESSION zone. This module
# writes exactly one TIMESTAMP, `load_date`, and it wrote it the other way.
from opl.gold.dimensions import instant_literal
from opl.gold.specs import PointInTimeTable
from opl.vault.columns import APPLIED_DATE
from opl.vault.loading import rows_in
from opl.vault.registry import Hub

__all__ = [
    "PIT_RECORD_SOURCE",
    "PitLoadResult",
    "as_of_dates",
    "load_pit",
    "pit_rows",
    "rows_per_as_of",
]

# The RSRC every PIT row carries, and it is `opl.gold.conformed`'s question with the same
# answer for a different reason. `dim_company` passes on the RFB's own `record_source`
# because the RFB delivered those values; a PIT row carries no delivered value at all --
# only a pointer this module computed -- so the source it names is this module.
#
# DECLARED HERE AND NOT IN `opl.gold.columns`, which is "the columns every DIMENSION
# carries". A point-in-time table is not a dimension: it has no surrogate key, no natural
# key and no attribute, and putting its RSRC in that file would widen a set whose whole
# job is to say which names a dimension's projection may not reuse.
PIT_RECORD_SOURCE = "opl.gold.pit"

_DATE = "date"


@dataclass(frozen=True)
class PitLoadResult:
    """What one PIT build did, and the numbers that make its row count checkable against
    the vault below it rather than against a document."""

    table: str
    appended: int
    # What the target already held before this build. A build that appends 0 against a
    # non-zero `already_present` is the idempotent re-run.
    already_present: int
    # The as-of set this build MEASURED from the satellites, published because it decides
    # the row count and because a reader should not have to infer it from the snapshots.
    as_of_dates: tuple[date, ...]
    # `rows_in` over the HUB. Free -- Delta answers a whole-table count from the
    # transaction log -- and it is the number the reconciliation below is against.
    hub_keys: int
    # Rows the WRITTEN table holds at each as-of date, in date order. This is the whole
    # reconciliation: the count at the last as-of date is the number of hub keys with any
    # descriptive history at all, so `hub_keys` minus it is the dangling-hub-key
    # population, and each earlier count is the keys first observed at or before that
    # date. One scan of one low-cardinality column, collapsed by a partial aggregate.
    rows_per_as_of: tuple[tuple[date, int], ...]


def _union(frames: Sequence[DataFrame]) -> DataFrame:
    """`frames` stacked BY NAME, which is the only safe stacking here: the pointer columns
    differ per satellite, so a positional `union` would put one satellite's dates in
    another's column with nothing failing."""
    return reduce(lambda left, right: left.unionByName(right), frames)


def _refuse_an_unusable_as_of_set(pit: PointInTimeTable, measured: Sequence[date | None]) -> None:
    """Refuse an empty as-of set, and refuse a NULL `applied_date` LOUDLY.

    THE NULL IS THE ONE THAT MATTERS AND IT IS THE 8,757-PHANTOM-DEPARTURE FAMILY. A NULL
    would arrive here as a member of the as-of set, become a NULL literal in the grid, and
    from there: every `as_of_date` comparison against it is NULL rather than false, so the
    window fills nothing; the anti-join on a re-run never matches it, so the whole NULL
    layer is appended again on every run; and the row count still looks like a multiple of
    the key count. Refused before a row is derived, because after the write there is
    nothing to do but drop the table."""
    if any(value is None for value in measured):
        raise ValueError(
            f"refusing to build {pit.name!r}: at least one of {list(pit.satellites)} "
            f"carries a NULL {APPLIED_DATE}. It would become a NULL as-of date, which no "
            "comparison matches and no anti-join removes -- the layer would be re-appended "
            "on every run and every pointer in it would stay NULL, with the build "
            "reporting success. The vault takes `applied_date` from bronze's "
            "`_snapshot_ref_date`, so this is a row that did not come through the ingest"
        )
    if not measured:
        raise ValueError(
            f"refusing to build {pit.name!r}: none of {list(pit.satellites)} holds a "
            "single row, so there is no as-of date to build at and the table would be "
            "empty with the build reporting success"
        )


def as_of_dates(
    spark: SparkSession, pit: PointInTimeTable, satellite_tables: Mapping[str, str]
) -> tuple[date, ...]:
    """Every instant this PIT holds a row at: the distinct union of `applied_date` across
    its satellites, in order.

    MEASURED AND NOT DECLARED, for `opl.gold.conformed.covered_span`'s reason -- a pair of
    date literals in a spec is a second spelling of a value the vault owns, and it goes
    stale the day a snapshot lands. Collected to the driver because the set is bounded by
    the number of snapshots ever loaded (two today), and because a driver-side list lets
    the grid be built by an `explode` over literals instead of by a join."""
    frames = [spark.read.table(table).select(APPLIED_DATE) for table in satellite_tables.values()]
    measured = [row[0] for row in _union(frames).distinct().collect()]
    _refuse_an_unusable_as_of_set(pit, measured)
    return tuple(sorted(measured))


def _grid(
    spark: SparkSession,
    pit: PointInTimeTable,
    hub: Hub,
    hub_table: str,
    dates: Sequence[date],
) -> DataFrame:
    """Every hub key at every as-of date, with no pointer -- the frame that turns a naive
    equi-join into an as-of answer.

    AN `explode` OVER LITERALS AND NOT A CROSS JOIN. The as-of set is on the driver, so the
    whole expansion is map-side: no shuffle, no broadcast, nothing to spill. `F.lit(date)`
    builds a DATE literal through `DateType.toInternal`, which is `(value - epoch).days` --
    no `time.mktime` and no session timezone, unlike the TIMESTAMP literals
    `opl.gold.dimensions.instant_literal` has to work around."""
    days = F.array(*(F.lit(day) for day in dates))
    return spark.read.table(hub_table).select(
        F.col(hub.hash_key),
        F.explode(days).alias(pit.as_of_column),
        *(F.lit(None).cast(_DATE).alias(pointer) for pointer in pit.pointer_columns),
    )


def _observations(
    spark: SparkSession,
    pit: PointInTimeTable,
    hub: Hub,
    satellite_tables: Mapping[str, str],
) -> DataFrame:
    """Each satellite's own (hash key, `applied_date`) pairs, tagged into that satellite's
    pointer column and NULL in every other -- the frame the grid is filled from.

    KEYED BY SATELLITE NAME AND NOT BY POSITION, which is what `satellite_tables` being a
    Mapping buys: handed a tuple, a caller who swapped two tables would produce pointer
    columns named after one satellite carrying the other's dates, and nothing about the
    result would look wrong."""
    frames = [
        spark.read.table(satellite_tables[satellite]).select(
            F.col(hub.hash_key),
            F.col(APPLIED_DATE).alias(pit.as_of_column),
            *(
                F.col(APPLIED_DATE).alias(pointer)
                if pointer == pit.pointer_column(satellite)
                else F.lit(None).cast(_DATE).alias(pointer)
                for pointer in pit.pointer_columns
            ),
        )
        for satellite in pit.satellites
    ]
    return _union(frames)


def pit_rows(
    spark: SparkSession,
    pit: PointInTimeTable,
    *,
    hub: Hub,
    hub_table: str,
    satellite_tables: Mapping[str, str],
    dates: Sequence[date],
    load_date: datetime,
) -> DataFrame:
    """Every row the PIT will hold, in the declared column order.

    THE ORDER IS PINNED AND IS LOAD-BEARING, for `opl.gold.dimensions._versioned`'s
    reason: a Delta append matches POSITIONALLY unless `mergeSchema` says otherwise.

    ONE `groupBy` OVER THE UNION AND ONE WINDOW OVER ITS RESULT, and both pointer fills
    take the IDENTICAL window spec so Spark plans a single window operator. The final
    filter is the "not before it was first observed" rule: a row whose every pointer is
    still NULL after the fill is a key no satellite had anything to say about at that
    instant, which for a key that exists is exactly an as-of date before its first
    observation, and for a hub key with no satellite row at all is every instant.

    `load_date` GOES THROUGH `instant_literal` LIKE EVERY OTHER INSTANT GOLD WRITES. It is
    the only TIMESTAMP in this projection and it was the one place in the layer that still
    used `F.lit(datetime)`; see the import above, and its test for the measurement.

    Public for `opl.gold.dimensions.dimension_rows`' reason -- the frame is worth having
    without the write."""
    pointers = pit.pointer_columns
    collapsed = _union(
        [
            _grid(spark, pit, hub, hub_table, dates),
            _observations(spark, pit, hub, satellite_tables),
        ]
    ).groupBy(hub.hash_key, pit.as_of_column).agg(
        *(F.max(pointer).alias(pointer) for pointer in pointers)
    )
    in_force = (
        Window.partitionBy(hub.hash_key)
        .orderBy(pit.as_of_column)
        .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return collapsed.select(
        F.col(hub.hash_key),
        F.col(pit.as_of_column),
        *(F.max(pointer).over(in_force).alias(pointer) for pointer in pointers),
        instant_literal(load_date).alias(LOAD_DATE),
        F.lit(PIT_RECORD_SOURCE).alias(RECORD_SOURCE),
    ).where(F.coalesce(*(F.col(pointer) for pointer in pointers)).isNotNull())


def _refuse_a_mismatched_source(
    pit: PointInTimeTable, hub: Hub, satellite_tables: Mapping[str, str]
) -> None:
    """The PIT, its hub and its satellites' tables arrive as three arguments, so something
    has to check they belong together.

    Three arguments for `opl.gold.dimensions._refuse_a_mismatched_source`'s reason: a
    loader that resolved its source through the module-level registry could not be driven
    with a throwaway spec. Handed another hub, the union is on a hash key of another name
    and the build either fails inside Spark's analysis or -- where two hubs spell it alike
    -- merges two key spaces. Handed a satellite the spec does not name, the pointer column
    for the one it DOES name is NULL on every row and the table is well-formed and empty
    of the thing it exists for."""
    if hub.name != pit.hub:
        raise ValueError(
            f"point-in-time table {pit.name!r} is built over {pit.hub!r} and was handed "
            f"hub {hub.name!r}. The keys and the pointers are unioned on the hash key, so "
            "this either fails in analysis or, where the two hubs spell their hash key "
            "alike, merges two key spaces without failing -- resolve the hub with "
            "opl.vault.domains.table_spec rather than passing one by hand"
        )
    if set(satellite_tables) != set(pit.satellites):
        raise ValueError(
            f"point-in-time table {pit.name!r} points at {sorted(pit.satellites)} and was "
            f"handed tables for {sorted(satellite_tables)}. The mapping is keyed BY "
            "SATELLITE NAME precisely so the tables cannot be swapped; a missing key "
            "leaves that satellite's pointer NULL on every row, which is a build that "
            "reports success and answers 'no address on record' for every establishment"
        )


def _held_as_of_dates(
    spark: SparkSession, pit: PointInTimeTable, target_table: str
) -> tuple[date, ...]:
    """The as-of dates the target already holds. One scan of one low-cardinality column,
    collapsed by a partial aggregate to at most one row per partition."""
    return tuple(
        sorted(
            row[0]
            for row in spark.read.table(target_table)
            .select(pit.as_of_column)
            .distinct()
            .collect()
        )
    )


def _refuse_an_out_of_order_backfill(
    pit: PointInTimeTable,
    target_table: str,
    dates: Sequence[date],
    held: Sequence[date],
) -> None:
    """Refuse a new as-of date that lands BEFORE one the target already holds.

    THE ONE CASE AN APPEND-ONLY PIT CANNOT DO, AND THE ANTI-JOIN CANNOT SEE IT. A pointer
    is `max(applied_date <= as_of_date)`, so a snapshot loaded after every snapshot this
    table was built over moves nothing already written -- which is why this loader needs no
    equivalent of `opl.gold.dimensions._refuse_a_target_the_source_has_outgrown`. A
    snapshot loaded BETWEEN two it holds is different: a key with a June row and an August
    row has an August pointer of June, and backfilling July REVISES it. The re-run's
    anti-join is on (hash key, `as_of_date`) and that pair is unchanged, so the revised row
    is dropped as already present and the stale pointer survives, silently. Refused before
    the first write; the repair is to drop the table and rebuild, or to load in order."""
    if not held:
        return
    backfilled = sorted(day for day in set(dates) - set(held) if day < max(held))
    if backfilled:
        raise ValueError(
            f"refusing to load {target_table}: the satellites have gained "
            f"{[day.isoformat() for day in backfilled]}, which is BEFORE "
            f"{max(held).isoformat()}, an as-of date this table already holds. A pointer "
            "is the latest applied_date at or before its as-of date, so an out-of-order "
            "snapshot REVISES pointers that are already written -- and the (hash key, "
            f"{pit.as_of_column}) pair does not move, so the re-run's anti-join would drop "
            "every revised row as already present and leave the stale pointer in place. "
            "Drop the table and rebuild it; nothing has been written by this run"
        )


def _new_rows(
    spark: SparkSession, pit: PointInTimeTable, hub: Hub, rows: DataFrame, target: str
) -> DataFrame:
    """`rows` minus what the target already holds, by (hash key, `as_of_date`).

    AN ANTI-JOIN AND NOT A REFUSAL, for `opl.gold.conformed._new_rows`' reason and with one
    addition: it also catches a key that arrived at an EXISTING as-of date, which happens
    when a partially-loaded snapshot is completed. NULL-SAFE BY CONSTRUCTION AND NOT BY
    OPERATOR -- see the module docstring; both keys are provably non-NULL, which is the
    only reason a plain equality anti-join is correct here.

    IT IS THE ONE EXPENSIVE STEP THAT ONLY A RE-RUN PAYS: a shuffle of the derived frame
    against a target of the same size. A first build skips it entirely."""
    existing = spark.read.table(target).select(hub.hash_key, pit.as_of_column)
    return rows.join(existing, on=[hub.hash_key, pit.as_of_column], how="left_anti")


def rows_per_as_of(
    spark: SparkSession, pit: PointInTimeTable, target_table: str
) -> tuple[tuple[date, int], ...]:
    """How many rows the written table holds at each as-of date, in date order.

    THE RECONCILIATION, AND IT IS CHEAP FOR THE REASON THE COLUMN IS LOW-CARDINALITY: a
    partial aggregate reduces each partition to one row per as-of date before the shuffle,
    so the cost is the scan of one column. Read from the TABLE and not from the frame --
    measuring the frame would derive the whole build a second time, and serverless refuses
    `persist()`."""
    counted = (
        spark.read.table(target_table).groupBy(pit.as_of_column).count().collect()
    )
    return tuple(sorted((row[0], row["count"]) for row in counted))


def _refuse_an_as_of_layer_wider_than_the_hub(
    pit: PointInTimeTable, target_table: str, hub_keys: int, per_as_of: Sequence
) -> None:
    """Refuse an as-of layer holding more rows than the hub holds keys.

    THE GRAIN, CHECKED AGAINST SOMETHING RATHER THAN ASSUMED. One row per (key, as-of) is
    guaranteed by the `groupBy` that produces it, exactly as `dim_company`'s surrogate key
    is "guaranteed" by a hash -- and that one is measured anyway. This costs nothing beyond
    the reconciliation already collected.

    WHAT IT DOES NOT COVER, STATED RATHER THAN IMPLIED: it is a bound, not an equality. A
    layer with one duplicated key AND one absent key passes it. What it does catch is the
    failure that actually scales -- a repeated union member, a satellite table handed
    twice, a re-run that appended a whole layer again -- each of which pushes a layer past
    the key count by a wide margin.

    AND ONE FAILURE THAT IS NOT ABOUT THIS BUILD AT ALL: a satellite carrying a hash key
    the HUB does not hold. Such a key reaches the union through the observations and never
    through the grid, so it appears at the instants it was observed and is missing from
    every later one -- a partial history nothing else here would notice. It is the mirror
    of the dangling satellite version `gold_load_dimension.py` reports, and it lands in
    this bound because the layers it lands in are the ones already full."""
    over = [(day, rows) for day, rows in per_as_of if rows > hub_keys]
    if over:
        raise ValueError(
            f"{target_table} holds {over} rows at as-of dates whose hub only has "
            f"{hub_keys} keys. The grain is one row per (hash key, {pit.as_of_column}), so "
            "a wider layer is a key present twice at one instant -- every as-of read for it "
            "returns two versions and every count over the table is inflated. THE TABLE ON "
            "DISK IS ALREADY WRITTEN and must be dropped before a repaired build"
        )


def load_pit(
    spark: SparkSession,
    pit: PointInTimeTable,
    *,
    hub: Hub,
    hub_table: str,
    satellite_tables: Mapping[str, str],
    target_table: str,
    load_date: datetime,
) -> PitLoadResult:
    """Build `pit` over `hub_table`'s keys and its satellites' version dates, and append
    it -- one row per key per as-of date, every pointer already resolved.

    `load_date` is an argument with no default, for `opl.vault.hubs.load_hub`'s reason: a
    loader that stamps its own clock cannot be asserted against.

    Idempotent by (hash key, `as_of_date`): a re-run over an unchanged vault appends
    nothing, and a vault that gained a LATER snapshot appends that layer alone. A vault
    that gained an EARLIER one is refused before the first write."""
    _refuse_a_mismatched_source(pit, hub, satellite_tables)
    dates = as_of_dates(spark, pit, satellite_tables)
    before = rows_in(spark, target_table)
    if before:
        _refuse_an_out_of_order_backfill(
            pit, target_table, dates, _held_as_of_dates(spark, pit, target_table)
        )
    rows = pit_rows(
        spark, pit, hub=hub, hub_table=hub_table, satellite_tables=satellite_tables,
        dates=dates, load_date=load_date,
    )
    if before:
        rows = _new_rows(spark, pit, hub, rows, target_table)
    rows.write.format("delta").mode("append").saveAsTable(target_table)
    per_as_of = rows_per_as_of(spark, pit, target_table)
    hub_keys = rows_in(spark, hub_table)
    _refuse_an_as_of_layer_wider_than_the_hub(pit, target_table, hub_keys, per_as_of)
    after = rows_in(spark, target_table)
    return PitLoadResult(
        table=target_table,
        appended=after - before,
        already_present=before,
        as_of_dates=dates,
        hub_keys=hub_keys,
        rows_per_as_of=per_as_of,
    )
