# src/opl/vault/effectivity.py
"""Load an effectivity satellite on a LINK: when the relationship was effective, with
the window's open taken from the source and only its close inferred by us.

WHICH "EFFECTIVITY SATELLITE" THIS IS, BECAUSE THE WORD NAMES TWO INCOMPATIBLE THINGS.
The research snapshot (`.plans/f2-research-snapshot-dv2.md` §3) records both:

  §3a AutomateDV -- a window over a link, closed by a DRIVING KEY mechanic: when a new
      driven key arrives for the same driving key, the old window is end-dated "so that
      we do not have 2 open Link records". The master spec points at this one.
  §3b DataVault4dbt -- `is_active` over a single tracked hash key, driven by the key's
      APPEARANCE and DISAPPEARANCE in a full extract.

THIS MODULE IMPLEMENTS §3b AND REJECTS §3a, and the rejection is measured rather than
stylistic. Of 16,644,534 companies with partners in 2026-07, 8,266,470 -- 49.7% -- have
MORE THAN ONE simultaneous partner; the mean is 1.681 and the maximum 2,573
(`01f19061-d161-17c6-971d-23106c8d8bcf`). "Two open link records" is not the anomaly
§3a end-dates on, it is the normal case for half the companies in the table, so a
replacement-driven close would end-date a partner every time the company gained
another. §3a is not incomplete here; on this relationship it is wrong. See ADR 0011.

THE CLOSE IS GATED ON THE OBSERVATION LEDGER, AT LINK GRAIN, AND ON ONE STATE. A
relationship that disappears from bronze has disappeared for one of two reasons and the
satellite must not treat them alike: the source stopped publishing it, or OUR DQ gate
quarantined the row. `CLOSING_STATE` is `absent_after_observation` and nothing else --
`rejected_by_our_gate` and `observed_with_rejected_siblings` close no window, and
`absent_before_first_observation` closes nothing because there was nothing open. The
state that authorised the close is written into `closed_by`, so the gate is a property
of the DATA and not only of this file.

HUB GRAIN WOULD BE THE WRONG LEDGER AND WOULD FAIL QUIETLY. A partner who loses one of
two partnerships is `absent_after_observation` at LINK grain and plainly `observed` at
hub grain, so a hub-grain ledger would report no departure and the window would stay
open forever. `_refuse_a_mismatched_grain` compares the grain's key columns against the
link's own identity columns, which is the strongest available statement of "the ledger
is keyed on the thing the satellite records".

THE OPEN IS DELIVERED AND THE CLOSE IS DERIVED, AND THE TABLE KEEPS THEM APART. The
entry column keeps the SOURCE'S OWN NAME and the source's own spelling --
`data_entrada_sociedade`, populated on 100% of 2026-07's rows with no `00000000`
sentinel (`01f19063-53c0-1f06-89f1-6aade0691af8`) -- while everything we inferred is
named in our vocabulary: `is_active`, `last_observed_on`, `closed_by`. AND THE DERIVED
CLOSE DOES NOT CLAIM A DATE THE SOURCE NEVER GAVE US: `last_observed_on` is the
`applied_date` of the last month we SAW the relationship, not the date it ended, which
nothing in this pipeline knows. The research's own position is that a derived delete is
a weaker claim than a delivered one; here it is weaker still, because part of the
absence signal is our own gate's, and the column names are the cheapest way to stop the
two being read alike.

THE DEDUP RULE, AND THIS IS THE HALF THAT COSTS SOMETHING. The source is not unique on
the link's business key -- 4,329 collisions in 2026-07, and 3,088 rows that are exact
duplicates even after adding `qualificacao_socio` and `data_entrada_sociedade`
(`01f19063-53c0-1f06-89f1-6aade0691af8`). The link can fold them for free because it
carries no payload; this table cannot, because it has to pick ONE
`data_entrada_sociedade`. THE EARLIEST WINS. That is deterministic, so two runs over the
same data agree; it is order-independent, so it does not depend on partition layout; and
unlike `opl.vault.satellites`' lowest-`hash_diff` tie-break it is not arbitrary -- the
open of a window is the earliest moment the source claims the relationship began.
`EffectivityLoadResult.collapsed_duplicates` counts what was folded, so the choice is
visible in the run log rather than only in this docstring.

TWO EDGES OF THAT RULE, BOTH SPELLED OUT BECAUSE `F.min` OVER A STRUCT IS QUIETER THAN
IT LOOKS. First, a genuine TIE on the entry date is still deterministic: `min` over
`struct(entry_column, record_source)` compares field by field, so equal entry dates are
broken lexicographically on `record_source` -- there is no state in which two runs over
the same data disagree. Second, a NULL entry date sorts FIRST in Spark's ascending
ordering, so a NULL would beat a delivered date on its twin. Unreachable on this source
-- `data_entrada_sociedade` is populated on 100% of 2026-07's rows -- and worth knowing
before a wave-2 feed reuses this loader with a nullable open."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from opl.vault.columns import (
    APPLIED_DATE,
    CLOSED_BY,
    IS_ACTIVE,
    LAST_OBSERVED_ON,
    LOAD_DATE,
    RECORD_SOURCE,
)
from opl.vault.hashing_spark import refuse_non_string_columns
from opl.vault.links import refuse_mismatched_hubs
from opl.vault.loading import (
    BRONZE_RECORD_SOURCE,
    SNAPSHOT_MONTH_COLUMN,
    SNAPSHOT_REF_DATE_COLUMN,
    changed_rows,
    link_hash_key_expression,
    read_snapshot_window,
    rows_in,
)
from opl.vault.observation import (
    MONTH_COLUMN,
    STATE_COLUMN,
    ObservationGrain,
    ObservationState,
    observation_ledger,
)
from opl.vault.registry import (
    EffectivitySatellite,
    Hub,
    Link,
    identifying_hubs,
    identity_columns_of,
)

# THE ONE STATE THIS VAULT CLOSES A WINDOW ON. Named here, once, and written into every
# closing row's `closed_by`, so widening the gate means changing a value that is
# visible in the data rather than a condition buried in a filter.
CLOSING_STATE = ObservationState.ABSENT_AFTER_OBSERVATION

# Internal to the dedup below, selected through by field.
_CHOSEN = "_chosen"
_ROWS = "_rows"


@dataclass(frozen=True)
class EffectivityLoadResult:
    """What one effectivity load did."""

    table: str
    appended: int
    already_present: int
    # How many of the appended rows CLOSE a window. The number an operator should read
    # beside `collapsed_duplicates`: it is the count of relationships this load decided
    # had departed, and every one of them is a derived claim.
    closed: int
    # Source rows folded into another row sharing its (link key, month). Here the fold
    # DISCARDS a delivered value -- one `data_entrada_sociedade` of several -- unlike
    # the link's, which discards nothing. See the module docstring for the rule.
    collapsed_duplicates: int


def _refuse_a_mismatched_grain(
    satellite: EffectivitySatellite, link: Link, hubs: Sequence[Hub],
    grain: ObservationGrain, source_table: str,
) -> None:
    """The satellite, its link, the link's hubs, the grain and the source must all
    describe one table.

    THE GRAIN IS THE ARGUMENT WHOSE MISTAKES ARE INVISIBLE IN THE OUTPUT, exactly as it
    is for `load_satellite`, and here it decides which windows close. A grain keyed one
    column coarser than the link -- the hub grain, say -- reports a departure only when
    a partner leaves EVERY company, so a relationship that really ended stays open with
    nothing failing. `identity_columns_of` derives the comparison from the link's own
    spec, so the two cannot drift."""
    if satellite.parent != link.name:
        raise ValueError(
            f"effectivity satellite {satellite.name!r} declares parent "
            f"{satellite.parent!r} and was handed link {link.name!r}. The satellite and "
            "the link are free arguments so both can be tested against throwaway specs, "
            "and nothing but this check stops them being mismatched -- the load would "
            "SUCCEED, keying every row on the wrong link's hash key, and produce a "
            "plausible, fully populated table about a different relationship. Resolve "
            "the parent with opl.vault.domains.parent_link rather than passing a link "
            "by hand"
        )
    refuse_mismatched_hubs(link, hubs)
    if grain.bronze_table != source_table:
        raise ValueError(
            f"the observation grain reads {grain.bronze_table!r} and effectivity "
            f"satellite {satellite.name!r} is being loaded from {source_table!r}. The "
            "ledger would decide which windows close from a different table's absences"
        )
    identity = identity_columns_of(link, hubs)
    if tuple(grain.key_columns) != identity:
        raise ValueError(
            f"the observation grain is keyed on {tuple(grain.key_columns)} and link "
            f"{link.name!r} on {identity}. The ledger must be keyed on the LINK's "
            "identity, in its hash order: coarser and a relationship that ended stays "
            "open because the key survives elsewhere, finer and it closes windows that "
            "never departed. Build the grain from the link spec"
        )


def _observed(
    spark: SparkSession, satellite: EffectivitySatellite, link: Link, hubs: Sequence[Hub],
    source_table: str, months: Sequence[str] | None,
) -> DataFrame:
    """One row per (link hash key, month) bronze shows, carrying the DELIVERED open.

    Deduplicated by the earliest `data_entrada_sociedade`, with `_ROWS` carrying how
    many source rows were folded into each -- see the module docstring for the rule and
    for why this table's fold is the one that costs something."""
    source = read_snapshot_window(spark, source_table, months)
    refuse_non_string_columns(
        source,
        [*identity_columns_of(link, hubs), satellite.entry_column],
    )
    keyed = source.select(
        link_hash_key_expression(link, identifying_hubs(link, hubs)).alias(link.hash_key),
        F.col(SNAPSHOT_MONTH_COLUMN),
        F.col(SNAPSHOT_REF_DATE_COLUMN).alias(APPLIED_DATE),
        F.col(satellite.entry_column),
        F.col(BRONZE_RECORD_SOURCE).alias(RECORD_SOURCE),
    )
    return (
        keyed.groupBy(link.hash_key, SNAPSHOT_MONTH_COLUMN, APPLIED_DATE)
        .agg(
            F.min(F.struct(satellite.entry_column, RECORD_SOURCE)).alias(_CHOSEN),
            F.count(F.lit(1)).alias(_ROWS),
        )
        .select(link.hash_key, SNAPSHOT_MONTH_COLUMN, APPLIED_DATE, f"{_CHOSEN}.*", _ROWS)
    )


def _reference_dates(spark: SparkSession, grain: ObservationGrain) -> DataFrame:
    """`_snapshot_month` -> the RFB's own ref date, from BOTH sides of the ledger.

    Quarantine as well as bronze, because the ledger reports on months where a key
    appears on either side and a closing row needs an `applied_date` for its month. A
    map built from bronze alone would drop a departure in a month whose bronze rows all
    failed the gate, which is a small population and exactly the wrong one to lose."""
    frames = [
        spark.read.table(table).select(MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN)
        for table in (grain.bronze_table, grain.quarantine_table)
    ]
    return (
        frames[0].unionByName(frames[1])
        .groupBy(MONTH_COLUMN)
        .agg(F.min(SNAPSHOT_REF_DATE_COLUMN).alias(APPLIED_DATE))
    )


def _departures(
    spark: SparkSession, link: Link, hubs: Sequence[Hub], grain: ObservationGrain,
    months: Sequence[str] | None,
) -> DataFrame:
    """The (link hash key, month, applied_date) triples whose window this load closes.

    THE GATE. `CLOSING_STATE` and nothing else, so a key our own DQ gate removed --
    `rejected_by_our_gate`, 1,781 of them at link grain in 2026-07 -- reaches this
    function and is filtered out, rather than never being asked about. The ledger is
    keyed on the link's RAW identity columns, so the same
    `link_hash_key_expression` that keyed bronze keys the ledger: one spelling, and the
    two sides cannot disagree about which relationship a departure belongs to.

    THE JOIN IS ON THE MONTH AND NEVER ON A BUSINESS KEY, which is what keeps the 4
    measured departures whose `cpf_cnpj_socio` is NULL from being lost or invented. The
    controller's own first verification of this load used `LEFT ANTI JOIN ... USING`,
    plain equality, and read every NULL-keyed foreign partner as departed -- 74,201
    instead of 65,444, i.e. 8,757 phantom departures. NULL is absorbed into the digest
    by `_encoded` before anything compares it, so by the time a key reaches this join
    it is a 64-character string."""
    ledger = observation_ledger(spark, grain, months=months)
    closing = ledger.filter(F.col(STATE_COLUMN) == F.lit(CLOSING_STATE.value))
    return (
        closing.select(
            link_hash_key_expression(link, identifying_hubs(link, hubs)).alias(link.hash_key),
            F.col(MONTH_COLUMN),
        )
        .join(_reference_dates(spark, grain), on=MONTH_COLUMN)
    )


def _statements(
    observed: DataFrame, departures: DataFrame, satellite: EffectivitySatellite, link: Link
) -> DataFrame:
    """Every claim this load can make about a relationship, one row per (key, month),
    with the closing rows carrying forward what the last observed row delivered.

    THE CARRY-FORWARD IS WHY THIS IS NOT TWO SELECTS. A closing row has no source row --
    that is what makes it a close -- so its `data_entrada_sociedade` and its
    `record_source` have to come from the last month we DID observe, and its
    `last_observed_on` is that month's `applied_date`. Taking them from the current row
    would leave the window's delivered open NULL on the very row that closes it, which
    is the one row where a reader most needs to see both ends."""
    closing = (
        departures
        .withColumn(satellite.entry_column, F.lit(None).cast("string"))
        .withColumn(RECORD_SOURCE, F.lit(None).cast("string"))
        .withColumn(_ROWS, F.lit(0))
        .withColumn(IS_ACTIVE, F.lit(False))
    )
    everything = observed.withColumn(IS_ACTIVE, F.lit(True)).unionByName(closing)
    history = (
        Window.partitionBy(link.hash_key)
        .orderBy(APPLIED_DATE)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    def when_active(column: str):
        return F.last(F.when(F.col(IS_ACTIVE), F.col(column)), ignorenulls=True).over(history)

    return (
        everything
        .withColumn(LAST_OBSERVED_ON, F.when(~F.col(IS_ACTIVE), when_active(APPLIED_DATE)))
        .withColumn(CLOSED_BY, F.when(~F.col(IS_ACTIVE), F.lit(CLOSING_STATE.value)))
        .withColumn(
            satellite.entry_column,
            F.when(F.col(IS_ACTIVE), F.col(satellite.entry_column))
            .otherwise(when_active(satellite.entry_column)),
        )
        .withColumn(
            RECORD_SOURCE,
            F.when(F.col(IS_ACTIVE), F.col(RECORD_SOURCE))
            .otherwise(when_active(RECORD_SOURCE)),
        )
    )


def _in_column_order(
    rows: DataFrame, satellite: EffectivitySatellite, link: Link, load_date: datetime
) -> DataFrame:
    """The rows about to be written, in the satellite's declared column order.

    An explicit projection rather than whatever the joins left behind, for
    `opl.vault.satellites._in_column_order`'s reason: `mode("append")` matches by
    POSITION on an existing table, so two loads building these columns in two orders
    would write `is_active` into `last_observed_on` without failing. The delivered open
    sits between our metadata and our two derived columns, which is the order the table
    reads in: what we did, what the source said, what we inferred."""
    return rows.select(
        link.hash_key,
        F.lit(load_date).alias(LOAD_DATE),
        F.col(APPLIED_DATE),
        F.col(RECORD_SOURCE),
        F.col(IS_ACTIVE),
        F.col(satellite.entry_column),
        F.col(LAST_OBSERVED_ON),
        F.col(CLOSED_BY),
    )


def load_effectivity_satellite(
    spark: SparkSession,
    satellite: EffectivitySatellite,
    *,
    link: Link,
    hubs: Sequence[Hub],
    source_table: str,
    target_table: str,
    load_date: datetime,
    grain: ObservationGrain,
    months: Sequence[str] | None = None,
) -> EffectivityLoadResult:
    """Append a row for every (link hash key, `applied_date`) at which `is_active`
    CHANGED: the relationship's first appearance, its disappearance, and its return.

    Delta-driven like a descriptive satellite and through the same `changed_rows`, so a
    relationship present in both months writes ONE row rather than two. Idempotent: a
    re-run finds every (key, applied_date) persisted, drops them before the window, and
    appends nothing."""
    _refuse_a_mismatched_grain(satellite, link, hubs, grain, source_table)
    observed = _observed(spark, satellite, link, hubs, source_table, months)
    collapsed = observed.select(F.coalesce(F.sum(F.col(_ROWS) - 1), F.lit(0))).first()[0]
    departures = _departures(spark, link, hubs, grain, months)
    candidates = _statements(observed, departures, satellite, link)
    before = rows_in(spark, target_table)
    existing = None
    if before:
        existing = spark.read.table(target_table).select(
            link.hash_key, APPLIED_DATE, IS_ACTIVE
        )
        candidates = candidates.join(
            existing.select(link.hash_key, APPLIED_DATE),
            on=[link.hash_key, APPLIED_DATE], how="left_anti",
        )
    changed = changed_rows(candidates, existing, link.hash_key, change_column=IS_ACTIVE)
    rows = _in_column_order(changed, satellite, link, load_date).persist()
    # COUNTED FROM THE FRAME THAT IS WRITTEN, NOT FROM `load_date` ON THE TARGET, and
    # the earlier spelling is the bug this replaces: filtering the target on
    # `load_date` made a re-run under the SAME stamp report `appended=0` beside the
    # PREVIOUS run's `closed`, two numbers on two bases in one result object. This is
    # a breakdown of `appended`, on `appended`'s own basis -- the write is a single
    # atomic Delta append of exactly these rows, so the only way the two can disagree
    # is a failed write, which raises.
    closed = rows.filter(~F.col(IS_ACTIVE)).count()
    rows.write.format("delta").mode("append").saveAsTable(target_table)
    rows.unpersist()
    return EffectivityLoadResult(
        table=target_table,
        appended=rows_in(spark, target_table) - before,
        already_present=before,
        closed=closed,
        collapsed_duplicates=int(collapsed),
    )
