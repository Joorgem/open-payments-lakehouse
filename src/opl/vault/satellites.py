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

**THAT MEASUREMENT IS ABOUT EMPRESAS AND THIS LOADER NO LONGER ONLY LOADS EMPRESAS.**
Task 4 pointed it at `sat_estabelecimento_dados` and `sat_estabelecimento_endereco`
over 72.3M rows and the equivalent question -- duplicate
`(cnpj_basico, cnpj_ordem, cnpj_dv, _snapshot_month)` rows -- was never asked. **The
estabelecimentos duplicate rate is UNMEASURED**, not measured at zero, and the empresas
statement id above must not be read as covering it. The query that would settle it is
one `GROUP BY` (see the F2 wave-1 fix report); until it is run, the number to look at is
`SatelliteLoadResult.collapsed_duplicates` **on a load that was asked to measure it** --
see `report_diagnostics` below. Task 5 asked this question of its own tables and answered
it there; it did not come back for this one.

THE TWO REPORTED COUNTS ARE OPTIONAL AND DEFAULT TO OFF, AND THAT IS A COST DECISION
WITH A NUMBER BEHIND IT. The vault's first real run loaded `sat_empresa_dados` in
**5,635 s** against `hub_empresa`'s **281 s** over the same 69,062,849 keys
(`docs/f2-wave-1-workspace-run-evidence.md` §1.6). `load_hub` makes ONE pass over the
source; this loader made FOUR -- `satellite_candidates`, then `_collapsed_duplicates`
(a second full scan), then the ledger's all-keys x all-months grid for
`_candidate_departures`, then the append -- and the middle two WRITE NOTHING. Both
answered 0. Estabelecimentos is 72.3M keys with two satellites. So `load_satellite`
takes `report_diagnostics`, default False, and a load that was not asked reports both
counts as `None`.

`None` IS NOT `0`, AND KEEPING THEM APART IS THE POINT RATHER THAN A DETAIL. Those two
zeros are published as evidence that the dedup tie-break and the departure path are
unexercised by real data. A flag that turned a real 0 into a silent 0 would make that
evidence unfalsifiable, because nothing in the result or the log would separate a
measurement from a skip -- so the fields are `int | None`, `SatelliteLoadResult` refuses
a half-measured pair, and `databricks/src/vault_load_satellite.py` prints two different
sentences. WHAT IS **NOT** OPTIONAL is deriving the ledger: that is what routes `months`
through `observation._window` and its refusal of a month with no row on either side,
which is the second of the two things this module says consulting the ledger really
buys. The derivation runs on every load; only the `count()` over it is skipped.

WHAT THE RULE COSTS WHERE IT DOES FIRE, since "deterministic" is not "correct". Bronze
is append-only and a corrected batch can be promoted for the same month, so two rows
for one key-month with DIFFERENT payloads are reachable. This loader picks one of them
silently -- there is no refusal -- and **a later re-load cannot correct the choice**,
because the anti-join drops a candidate on `(hash key, applied_date)` alone and never
looks at the payload. Repairing such a row means deleting it from the satellite by
hand. There IS now a count: `_collapsed_duplicates` reports the fold in every load's
result, which is what the three sibling loaders already did and what this one was
missing -- a silent fold whose choice cannot be revoked was the worst of the four to
leave uncounted. Task 5, whose link grain has 4,329 measured collisions, treated the
rule as a decision to make rather than one to inherit."""
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
    """What one satellite load did, plus the two numbers it was asked to measure."""

    table: str
    appended: int
    # What the target already held before this load, whole-table and not window-scoped,
    # for `HubLoadResult.already_present`'s reason: the satellite may hold rows from
    # months outside the window, and reporting a narrower number than the one that was
    # measured would be a claim the count cannot support. This was the only result
    # object of the six without it, while `load_satellite` computed the number anyway.
    already_present: int
    # Source rows folded into another row sharing its (hash key, `applied_date`), by
    # `satellite_candidates`' lowest-`hash_diff` tie-break. Reported for the reason
    # `partners`, `reference` and `effectivity` report theirs, and here the reason is
    # sharper than for any of them: THIS fold discards a payload, silently, and a later
    # re-load cannot correct the choice (the anti-join drops the candidate on (hash key,
    # `applied_date`) alone and never looks at what was kept). See the module docstring.
    #
    # `None` WHEN THE LOAD WAS NOT ASKED TO MEASURE IT -- the type is what keeps that
    # apart from a measured 0, and the two readings are not close: a 0 says this loader
    # discarded no payload, a `None` says nobody looked.
    collapsed_duplicates: int | None
    # Keys the observation ledger calls `absent_after_observation` over this window:
    # present in an earlier month, absent here. A CANDIDATE delete and never an
    # asserted one -- it is equally the shape of a missed file, a dropped partition, or
    # an entity that returns next month. Reported so an operator can see it; acted on
    # nowhere in this module. `None` under the same rule as above.
    candidate_departures: int | None

    def __post_init__(self) -> None:
        """ONE FLAG GOVERNS BOTH, so a half-measured pair is a state no load can produce
        and no reader can interpret -- `collapsed_duplicates=0` beside
        `candidate_departures=None` claims the load both did and did not do the extra
        work. Refused in the type rather than trusted to its one caller, because the
        whole value of `None` here is that it means exactly one thing."""
        if (self.collapsed_duplicates is None) != (self.candidate_departures is None):
            raise ValueError(
                f"a satellite load reported collapsed_duplicates="
                f"{self.collapsed_duplicates!r} beside candidate_departures="
                f"{self.candidate_departures!r}. One report_diagnostics flag decides both, "
                "so they are measured together or not at all: None means NOT MEASURED and "
                "0 means measured and found none, and a pair carrying one of each cannot "
                "be read as either"
            )


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


def _collapsed_duplicates(
    spark: SparkSession,
    satellite: Satellite,
    hub: Hub,
    source_table: str,
    months: Sequence[str] | None,
) -> int:
    """Source rows in the window, minus distinct (hash key, `applied_date`) pairs.

    A SECOND PASS, DELIBERATELY, in the shape `opl.vault.partners._collapsed_duplicates`
    and `opl.vault.reference._collapsed_duplicates` use -- and this loader is the one
    that most needed it. The other three folds either discard nothing (`partners`,
    whose link rows carry no payload) or discard one delivered value under a rule the
    module argues for (`effectivity`'s earliest entry date). This one picks a PAYLOAD
    silently, and the module docstring records that a re-load cannot correct the pick.
    A fold with that consequence and no count was the one number an operator had no way
    to get. `satellite` is taken for the same reason `hub` is: the pair is what the
    caller already validated, and reading the source through anything else would be a
    second spelling of the grain this counts against.

    THE HASH KEY IS RECOMPUTED RATHER THAN THE RAW COLUMNS COUNTED, which costs a second
    digest over the window and is not interchangeable with the cheap version:
    `zero_padded_column` maps `'1'` and `'01'` onto one padded key, so distinct raw
    values can share a hash key. Counting the raw columns would report fewer duplicates
    than the fold actually performs, which is the wrong direction for a number whose
    whole job is to make the fold visible."""
    source = read_snapshot_window(spark, source_table, months)
    keyed = source.select(
        hash_key_expression(hub).alias(hub.hash_key),
        F.col(SNAPSHOT_REF_DATE_COLUMN).alias(APPLIED_DATE),
    )
    return keyed.count() - keyed.distinct().count()


def _candidate_departures(ledger: DataFrame) -> int:
    """How many (key, month) pairs `ledger` calls `absent_after_observation`.

    IT TAKES THE LEDGER RATHER THAN DERIVING ONE, AND THAT SPLIT IS WHAT MADE THE COUNT
    SAFE TO SKIP. This function used to call `observation_ledger` itself, which bundled
    two unrelated things into one optional step: a number for the operator's log, and
    the ONLY route by which `months` reaches `observation._window` and its refusal of a
    month with no row on either side. Skipping the pair would have dropped a guard the
    module docstring calls one of the two real things the ledger buys. So the derivation
    moved out to `load_satellite`, which does it unconditionally, and what is left here
    is the part that is genuinely only a report.

    THE DERIVATION IS LAZY PAST THAT REFUSAL, which is why moving it out costs nothing on
    a load that reports no count: `observation_ledger` runs one eager job -- the distinct
    months `_window` collects -- and returns a plan. The `crossJoin` grid over the whole
    key space and the fold over it are built only when something asks for rows, and this
    `count()` is the only thing in this module that does.

    Eager, and BEFORE anything is written, so the number belongs in the operator's log
    next to what was written rather than to a later run."""
    return ledger.filter(
        F.col(STATE_COLUMN) == F.lit(ObservationState.ABSENT_AFTER_OBSERVATION.value)
    ).count()


def _diagnostics(
    spark: SparkSession,
    satellite: Satellite,
    hub: Hub,
    source_table: str,
    months: Sequence[str] | None,
    ledger: DataFrame,
    *,
    report: bool,
) -> tuple[int | None, int | None]:
    """The two reported counts, or `(None, None)` when this load was not asked for them.

    `None` AND NEVER `0`, which is the whole reason this returns an optional pair rather
    than defaulting to zeros. The first real run of this loader reported 0 collapsed
    duplicates and 0 candidate departures, and both zeros are PUBLISHED as evidence that
    two paths are unexercised by real data; a skip that reported 0 would make that
    evidence unfalsifiable, because no reader could separate a measurement from an
    omission. See the module docstring for what the skip is worth in seconds."""
    if not report:
        return None, None
    return (
        _collapsed_duplicates(spark, satellite, hub, source_table, months),
        _candidate_departures(ledger),
    )


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


def _append_changed(
    spark: SparkSession,
    candidates: DataFrame,
    satellite: Satellite,
    hub: Hub,
    target_table: str,
    load_date: datetime,
    before: int,
) -> None:
    """Append the candidates whose payload changed, in the satellite's column order.

    Split out of `load_satellite` when the diagnostics became optional, so that function
    stays inside this project's 50-line cap. It is a single Delta append of one frame, so
    the split adds no state between `load_satellite`'s refusals and the committed rows --
    `before` is passed in rather than re-read for the same reason it is read at all: the
    result object's `appended` is an after-minus-before over one measurement point."""
    existing = None
    if before:
        existing = spark.read.table(target_table).select(hub.hash_key, APPLIED_DATE, HASH_DIFF)
    # The anti-join that used to sit here is inside `changed_rows` now -- it was the one
    # step of that function's contract each caller had to remember, and it was the step
    # the docstring called load-bearing. See `loading._without_persisted`.
    changed = changed_rows(candidates, existing, hub.hash_key)
    (
        _in_column_order(changed, satellite, hub, load_date)
        .write.format("delta").mode("append").saveAsTable(target_table)
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
    report_diagnostics: bool = False,
) -> SatelliteLoadResult:
    """Append a row for every (hash key, `applied_date`) whose payload changed.

    `load_date` is an argument with no default, for `load_hub`'s reason: a loader that
    stamps its own clock cannot be asserted against, and in the data it would make the
    LDTS a record of when the pipeline happened to run.

    `report_diagnostics` DEFAULTS OFF, AND OFF REPORTS `None` RATHER THAN `0`. On, this
    load pays a second full scan of the source and materialises the ledger's key-space
    grid to fill `collapsed_duplicates` and `candidate_departures`; off it pays neither,
    and "not measured" is a thing no reader can confuse with "measured, found none". The
    first real run spent most of 5,635 s on the two and both answered 0; see _diagnostics.

    Idempotent: a re-run finds every (key, applied_date) it would write already
    persisted, drops them before the window, and appends nothing. The write is a single
    Delta append, so there is no partial state between the refusals and the committed
    rows."""
    _refuse_a_mismatched_hub(satellite, hub)
    _refuse_a_mismatched_grain(hub, grain, source_table)
    candidates = satellite_candidates(
        spark, satellite, hub, source_table=source_table, months=months
    )
    # DERIVED ON EVERY LOAD, INCLUDING ONE THAT REPORTS NOTHING FROM IT: this is the only
    # route by which `months` reaches `observation._window`'s refusal of a month with no
    # row on either side. Lazy past that refusal -- see `_candidate_departures`.
    ledger = observation_ledger(spark, grain, months=months)
    collapsed, departures = _diagnostics(
        spark, satellite, hub, source_table, months, ledger, report=report_diagnostics
    )
    before = rows_in(spark, target_table)
    _append_changed(spark, candidates, satellite, hub, target_table, load_date, before)
    return SatelliteLoadResult(
        table=target_table,
        appended=rows_in(spark, target_table) - before,
        already_present=before,
        collapsed_duplicates=collapsed,
        candidate_departures=departures,
    )
