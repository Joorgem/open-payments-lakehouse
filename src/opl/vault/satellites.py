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

THE DEDUPLICATION RULE IS STATED, AND ON EMPRESAS IT NEVER FIRES. The source does not
guarantee one row per key per month -- at link grain on socios, 27,990,592 rows cover
27,986,263 distinct triples. Where two source rows share a hash key and an
`applied_date`, the one with the LOWEST `hash_diff` wins: deterministic, so two runs
over the same data agree, and free, because `min` over a struct is a partial aggregate
inside the grouping this loader already needs. Identical duplicates collapse silently.
On empresas the question was measured after the Task 3 review and the answer is ZERO
duplicate `(cnpj_basico, _snapshot_month)` rows across both months
(`01f19274-c1e0-1f3a-998a-ee0234483f5c`), so the tie-break is unexercised there today.

WHAT THE RULE COSTS WHERE IT DOES FIRE, since "deterministic" is not "correct". Bronze
is append-only and a corrected batch can be promoted for the same month, so two rows
for one key-month with DIFFERENT payloads are reachable. This loader picks one of them
silently -- there is no refusal and no count -- and **a later re-load cannot correct
the choice**, because the anti-join drops a candidate on `(hash key, applied_date)`
alone and never looks at the payload. Repairing such a row means deleting it from the
satellite by hand. Task 5, whose link grain has 4,329 measured collisions, should treat
that as a decision to make rather than a rule to inherit."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.vault.columns import APPLIED_DATE, HASH_DIFF, LOAD_DATE, RECORD_SOURCE
from opl.vault.hashing_spark import hash_key_column, refuse_non_string_columns
from opl.vault.loading import (
    BRONZE_RECORD_SOURCE,
    SNAPSHOT_REF_DATE_COLUMN,
    changed_rows,
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

# Internal to `satellite_candidates`' tie-break, and named because it is selected
# through by field: a bare string at both ends is one typo from a column of NULLs.
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


def _grain_key_mismatch(hub: Hub, grain: ObservationGrain) -> str | None:
    """Why `grain`'s key columns are not `hub`'s, or None if they are.

    TWO DIFFERENT MISTAKES, TWO MESSAGES, which is the point of this function: one
    comparison told the reordered case that its ledger was "coarser or finer", which is
    FALSE and sends someone looking for a bug in their column list.

    ORDER IS PART OF THE MATCH, AND THAT IS A DECISION TAKEN HERE. `hub_estabelecimento`
    is the vault's first multi-column key, so "is (`cnpj_dv`, `cnpj_ordem`,
    `cnpj_basico`) the same grain as (`cnpj_basico`, `cnpj_ordem`, `cnpj_dv`)?" stops
    being theoretical. FOR THE LEDGER, YES -- and the argument has to concede that
    first, because the tempting justification for refusing is wrong: `groupBy` is
    order-insensitive, so a permuted grain returns the same states for the same keys and
    miscounts nothing. This branch refuses something that would have answered correctly.

    WHAT IT BUYS IS THAT THE TWO DECLARATIONS ARE ONE LIST, not merely one set. The
    hub's order IS load-bearing (`hash_key_expression` concatenates in it, so a permuted
    hub is a re-keyed hub), and a domain writes `key_columns=<hub>.business_key_columns`
    so that there is one order in the file rather than two. This check keeps that the
    only spelling that passes. Accept a permutation and anything that later pairs the
    two POSITIONALLY -- a join built by zipping them, a message printing one against the
    other -- pairs `cnpj_basico` with `cnpj_dv` with nothing failing. Set equality is
    the weaker claim and buys only the right to write the columns in an order no domain
    should want. The cost is a refusal of a correct configuration, so the message names
    the one-line fix."""
    declared, expected = tuple(grain.key_columns), hub.business_key_columns
    if set(declared) != set(expected):
        return (
            f"the observation grain is keyed on {declared} and hub {hub.name!r} on "
            f"{expected}. The ledger would count departures at a different grain than "
            "the satellite records change at -- coarser and it misses departures, "
            "finer and it invents them"
        )
    if declared != expected:
        return (
            f"the observation grain is keyed on {declared} and hub {hub.name!r} on "
            f"{expected} -- the same columns in a different order. The LEDGER would "
            "answer the same, because groupBy does not care; this is refused so that "
            "the two declarations stay one list rather than two sets. The hub's order "
            "IS load-bearing (the hash concatenates in it), and anything that later "
            "pairs the grain's columns with the hub's positionally would pair the "
            "wrong two. Build the grain with key_columns=<the hub spec>."
            "business_key_columns rather than restating the columns"
        )
    return None


def _refuse_a_mismatched_grain(
    hub: Hub, grain: ObservationGrain, source_table: str
) -> None:
    """The grain arrives as a third free argument and must describe the SAME rows the
    satellite is loading.

    `_refuse_a_mismatched_hub` exists because two independently-passed arguments can
    disagree; the grain has that hazard twice over, and worse, because it is the one
    argument whose mistakes are invisible in the output. It drives two things: the
    departure count, and `_window`'s refusal of a month with no row on either side --
    and `_window` reads `grain.bronze_table`, NOT `source_table`. A grain pointing at
    estabelecimentos would let `months=['2026-09']` pass or fail against the wrong
    table, and would report a departure count for a different key space, with the
    satellite's own rows perfectly correct beside it.

    TWO CHECKS, AND THE NAME IS DELIBERATELY NOT ONE OF THEM. The review suggested
    `grain.name == hub.name`, which `domains/cnpj.py` does satisfy. It is the weaker
    claim: a name is a label, so two grains can share one while reading different
    tables, and it is precisely the table and the key space that the two failures above
    are about. Checking what the ledger actually READS covers both, and covers them
    whether or not a future domain follows the naming convention.

    The key-space half is `_grain_key_mismatch`, which is where the order decision the
    first multi-column key forced is argued."""
    if grain.bronze_table != source_table:
        raise ValueError(
            f"the observation grain reads {grain.bronze_table!r} and the satellite is "
            f"being loaded from {source_table!r}. The ledger would describe a "
            "different table than the one written: its departure count would be about "
            "another key space, and its refusal of an unloaded month would be checked "
            "against another table's months. Pass the grain built for this source"
        )
    mismatch = _grain_key_mismatch(hub, grain)
    if mismatch is not None:
        raise ValueError(mismatch)


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
    _refuse_a_mismatched_grain(hub, grain, source_table)
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
    changed = changed_rows(candidates, existing, hub.hash_key)
    (
        _in_column_order(changed, satellite, hub, load_date)
        .write.format("delta").mode("append").saveAsTable(target_table)
    )
    return SatelliteLoadResult(
        table=target_table,
        appended=rows_in(spark, target_table) - before,
        candidate_departures=departures,
    )
