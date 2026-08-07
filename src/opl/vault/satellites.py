# src/opl/vault/satellites.py
"""Load a DV2 descriptive satellite: one row per hash key per `applied_date`, written
only when the payload CHANGED.

THE MECHANIC, AND THE PHASE'S PREMISE. `hash_diff` is the business-key hash standard
applied to the payload instead of to a business key. A candidate row is kept when its
`hash_diff` differs from the one that preceded it for the same hash key, in
`applied_date` order, and dropped when it does not -- so 69M companies with 105,820
changed razões sociais between two snapshots produce 105,820 second rows, not 69M.
Two rows for a company whose razão social moved and one for a company whose did not IS
the claim; `tests/vault/test_cnpj_vault.py` holds it over a fixture and the task report
carries the measurement against real bronze, because CI has no Databricks credential.

`applied_date` IS THE ORDERING AXIS, NOT `load_date`, and that is the departure this
phase exists to make. Classic DV2 orders a satellite by LDTS, which is correct only
when the load and the fact are the same event. Ours are not -- the RFB publishes a
snapshot dated 2026-06-13 and another dated 2026-07-11, and we load them whenever we
get to them, possibly both in one run. Ordering by `load_date` would make two
snapshots loaded in one job indistinguishable and their order arbitrary; ordering by
`applied_date` reconstructs the source's own history no matter when we ran.

WHAT THIS SATELLITE DOES NOT DO, and both are refusals rather than omissions:

  - **It writes no end-date and has no column to write one in.** A delta-driven
    satellite cannot tell "unchanged" from "not observed" -- both are the absence of a
    row -- so inferring a close from a missing row would end-date every key our own DQ
    gate happened to quarantine. That is ADR 0010's whole subject. The column list is
    pinned by test so the property belongs to the table rather than to the one row a
    test looked at.
  - **It does not act on a departure. It reports one.** The observation ledger is
    derived for the window and the count of `absent_after_observation` keys is returned
    in `SatelliteLoadResult`, where an operator sees it and no code branches on it. A
    caller who wants a departure signal maps that state onto one in their own code,
    where the choice is visible in review.

WHY CONSULTING THE LEDGER IS LOAD-BEARING HERE AND NOT DECORATIVE, stated precisely
because the tempting version of this wiring is not. Filtering candidates against the
ledger's observed states would be DEAD CODE: a candidate exists only because it has a
bronze row, and a bronze row makes the state `observed` or
`observed_with_rejected_siblings` by construction, so the filter could never remove
anything. A guard that cannot fire is worse than none, because the next reader
believes the hole is closed. What the ledger actually provides is two things that are
real: the departure count above, and `_window`'s refusal of a month with no row on
either side -- `months=['2026-09']` would otherwise select no bronze row, write
nothing, and report success.

THE DEDUPLICATION RULE IS STATED, because the source does not guarantee one row per
key per month. Measured at link grain on socios, 27,990,592 rows cover 27,986,263
distinct triples. Where two source rows share a hash key and an `applied_date`, the one
with the LOWEST `hash_diff` wins -- deterministic, so two runs over the same data agree,
and free, because `min` over a struct is a partial aggregate inside the grouping this
loader already needs. Identical duplicates collapse silently, which is the common case.
Whether empresas has any such duplicate at all is a question for the real-bronze
measurement in the task report, not for this comment."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from opl.vault.columns import APPLIED_DATE, HASH_DIFF, LOAD_DATE, RECORD_SOURCE
from opl.vault.hashing_spark import hash_key_column, refuse_non_string_columns
from opl.vault.loading import (
    BRONZE_RECORD_SOURCE,
    SNAPSHOT_REF_DATE_COLUMN,
    hash_key_expression,
    read_snapshot_window,
    rows_in,
)
from opl.vault.observation import (
    STATE_COLUMN,
    ObservationGrain,
    ObservationState,
    observation_ledger,
)
from opl.vault.registry import Hub, Satellite

# Internal to the change detection below. Named rather than inlined because both are
# selected through by string and a typo in one of the two spellings would silently
# make every row look new (`_PERSISTED` never true) or every row look old.
_PERSISTED = "_persisted"
_PREVIOUS = "_previous_hash_diff"
_CHOSEN = "_chosen"


@dataclass(frozen=True)
class SatelliteLoadResult:
    """What one satellite load did, plus the one number the ledger contributes."""

    table: str
    appended: int
    # Keys the observation ledger calls `absent_after_observation` over this window:
    # present in an earlier month, absent here. A CANDIDATE delete and never an
    # asserted one -- it is equally the shape of a missed file, a dropped partition, or
    # an entity that returns next month. Reported so an operator can see it; acted on
    # nowhere in this module.
    candidate_departures: int


def _refuse_a_mismatched_hub(satellite: Satellite, hub: Hub) -> None:
    """The satellite and the hub arrive as two arguments, so something has to check
    they belong together.

    They are two arguments on purpose: a loader that resolved the parent through the
    module-level registry could not be tested against a throwaway spec, and the
    registry is exactly the thing wave 2 must be able to extend without this file
    changing. The cost of that is this check, and it is worth stating what it prevents
    -- a satellite keyed on another hub's digest joins to nothing, silently, and
    reports success doing it."""
    if hub.name != satellite.parent:
        raise ValueError(
            f"satellite {satellite.name!r} declares parent {satellite.parent!r} and was "
            f"handed hub {hub.name!r}. Its hash key would be the wrong hub's digest, so "
            "the satellite would join to nothing without failing -- resolve the parent "
            "with opl.vault.domains.parent_hub rather than passing a hub by hand"
        )


def satellite_candidates(
    spark: SparkSession,
    satellite: Satellite,
    hub: Hub,
    *,
    source_table: str,
    months: Sequence[str] | None,
) -> DataFrame:
    """One row per (hash key, applied_date) in the window, carrying the payload, its
    `hash_diff` and the source row's `record_source`.

    `applied_date` COMES FROM `_snapshot_ref_date` AND NOT FROM `_snapshot_month`. The
    two are separate bronze columns on purpose (`opl.bronze.snapshot`): the month is the
    operational identity of the run, the ref date is the date the RFB itself declares
    in its filename, and it is not month-end -- 2026-06 carries the 13th and 2026-07
    the 11th. Deriving a date from the month would invent a day."""
    source = read_snapshot_window(spark, source_table, months)
    payload = tuple(satellite.payload_columns)
    refuse_non_string_columns(source, (*hub.business_key_columns, *payload))
    keyed = source.select(
        hash_key_expression(hub).alias(hub.hash_key),
        F.col(SNAPSHOT_REF_DATE_COLUMN).alias(APPLIED_DATE),
        hash_key_column([F.col(column) for column in payload]).alias(HASH_DIFF),
        *(F.col(column) for column in payload),
        F.col(BRONZE_RECORD_SOURCE).alias(RECORD_SOURCE),
    )
    return (
        keyed.groupBy(hub.hash_key, APPLIED_DATE)
        .agg(F.min(F.struct(HASH_DIFF, *payload, RECORD_SOURCE)).alias(_CHOSEN))
        .select(hub.hash_key, APPLIED_DATE, f"{_CHOSEN}.*")
    )


def _changed_rows(candidates: DataFrame, existing: DataFrame | None, key: str) -> DataFrame:
    """The candidates whose payload differs from the one that preceded it.

    THE PERSISTED ROWS SEED THE WINDOW, which is what makes an incremental load
    correct. June is already in the satellite when July's snapshot arrives, so July's
    comparison has to be against the row on disk rather than against another row in the
    same batch; a `lag` over the candidates alone would call July's payload new because
    nothing precedes it in that batch.

    A CANDIDATE WHOSE (key, applied_date) IS ALREADY PERSISTED IS DROPPED FIRST, before
    the union, and that ordering is load-bearing rather than tidy: leaving it in would
    put two rows at the same position in the window, and whichever of the identical
    pair `lag` happened to place second would be marked unchanged while the first was
    marked changed -- so a re-run would append a duplicate roughly half the time,
    depending on nothing anyone can see.

    ORDERED BY `applied_date`, so a snapshot loaded out of order still lands in its
    true position in the key's history. What that cannot repair is a row already
    written: backfilling June after July has been loaded leaves July's row in place
    even though it is now redundant. Redundant, not wrong -- it still says the payload
    was P on 2026-07-11 -- so the correction is to load in order, not to rewrite."""
    lineage = candidates.select(key, APPLIED_DATE, HASH_DIFF).withColumn(
        _PERSISTED, F.lit(False)
    )
    if existing is not None:
        lineage = lineage.unionByName(
            existing.select(key, APPLIED_DATE, HASH_DIFF).withColumn(_PERSISTED, F.lit(True))
        )
    ordered = Window.partitionBy(key).orderBy(APPLIED_DATE)
    changed = (
        lineage.withColumn(_PREVIOUS, F.lag(HASH_DIFF).over(ordered))
        .filter(F.col(_PREVIOUS).isNull() | (F.col(_PREVIOUS) != F.col(HASH_DIFF)))
        .filter(~F.col(_PERSISTED))
        .select(key, APPLIED_DATE)
    )
    return candidates.join(changed, on=[key, APPLIED_DATE], how="left_semi")


def _candidate_departures(
    spark: SparkSession, grain: ObservationGrain, months: Sequence[str] | None
) -> int:
    """How many (key, month) pairs the ledger calls `absent_after_observation`.

    Eager, and BEFORE anything is written, for both of the reasons this loader consults
    the ledger at all: the number belongs in the operator's log next to what was
    written, and deriving the ledger is what routes `months` through
    `observation._window`, whose refusal of a month with no row on either side is the
    guard a satellite has no other way to get."""
    ledger = observation_ledger(spark, grain, months=months)
    return ledger.filter(
        F.col(STATE_COLUMN) == F.lit(ObservationState.ABSENT_AFTER_OBSERVATION.value)
    ).count()


def _in_column_order(
    rows: DataFrame, satellite: Satellite, hub: Hub, load_date: datetime
) -> DataFrame:
    """The rows about to be written, in the satellite's declared column order.

    An explicit projection rather than whatever order the joins left behind, because
    the column ORDER is what a Delta append matches on when the table already exists --
    `mode("append")` is positional unless `mergeSchema` says otherwise, so two loads
    building the same columns in two orders would write the payload into each other's
    columns without failing. Metadata first, then payload, and
    `test_the_satellite_has_no_end_date_column_at_all` pins the whole list."""
    return rows.select(
        hub.hash_key,
        F.lit(load_date).alias(LOAD_DATE),
        F.col(APPLIED_DATE),
        F.col(RECORD_SOURCE),
        F.col(HASH_DIFF),
        *(F.col(column) for column in satellite.payload_columns),
    )


def load_satellite(
    spark: SparkSession,
    satellite: Satellite,
    *,
    hub: Hub,
    source_table: str,
    target_table: str,
    load_date: datetime,
    grain: ObservationGrain,
    months: Sequence[str] | None = None,
) -> SatelliteLoadResult:
    """Append a row for every (hash key, `applied_date`) whose payload changed.

    `load_date` is an argument with no default, for `load_hub`'s reason: a loader that
    stamps its own clock cannot be asserted against, and in the data it would make the
    LDTS a record of when the pipeline happened to run.

    Idempotent: a re-run finds every (key, applied_date) it would write already
    persisted, drops them before the window, and appends nothing. The write is a single
    Delta append, so there is no partial state between the refusals above and the
    committed rows."""
    _refuse_a_mismatched_hub(satellite, hub)
    candidates = satellite_candidates(
        spark, satellite, hub, source_table=source_table, months=months
    )
    departures = _candidate_departures(spark, grain, months)
    before = rows_in(spark, target_table)
    existing = None
    if before:
        existing = spark.read.table(target_table).select(hub.hash_key, APPLIED_DATE, HASH_DIFF)
        candidates = candidates.join(
            existing.select(hub.hash_key, APPLIED_DATE),
            on=[hub.hash_key, APPLIED_DATE],
            how="left_anti",
        )
    changed = _changed_rows(candidates, existing, hub.hash_key)
    (
        _in_column_order(changed, satellite, hub, load_date)
        .write.format("delta").mode("append").saveAsTable(target_table)
    )
    return SatelliteLoadResult(
        table=target_table,
        appended=rows_in(spark, target_table) - before,
        candidate_departures=departures,
    )
