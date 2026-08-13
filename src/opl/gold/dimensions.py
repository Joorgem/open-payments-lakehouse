# src/opl/gold/dimensions.py
"""Build a Kimball SCD2 dimension out of a Data Vault satellite: one row per version,
with a half-open validity interval that is closed in the SAME window pass that orders
the versions.

NO MERGE, AND THE ALTERNATIVE IS NOT "A CHEAPER MERGE". The textbook SCD2 load writes
the new version and then UPDATEs the previous one's end date, which needs a MERGE or an
UPDATE -- and there is none in this repository. Measured, and the spelling matters
because the first one written here did not do what it said: `git grep -lEi "MERGE
INTO|\\.merge\\(|DeltaTable" -- src databricks/src tests` matches this file and nothing
else, and it matches this file because of the sentence you are reading. (It was written
as `grep -rin`, where `|` is a LITERAL under basic regular expressions and the pattern
therefore matches nothing anywhere -- a claim that could not fail.) Every loader here is
`mode("append")`. Proving a new write pattern on Free Edition serverless
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
sort of 69.2M rows. A hash of (business key, `applied_date`) is deterministic, needs no
coordination, and is stable under a rebuild -- under a rebuild in ANOTHER TIMEZONE
included, which the first spelling of this key was not: it hashed `valid_from`, a
TIMESTAMP, and three session zones produced three different keys for one version.
`_versioned` argues that change where it is made.

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

from opl.config import SESSION_TIMEZONE, SESSION_TIMEZONE_CONFIG
from opl.gold.columns import (
    GHOST_RECORD_SOURCE,
    GHOST_ROWS,
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
    "COLLISION_CAUSES",
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
    `datetime` itself. EVERY instant this module writes goes through here.

    IT IS ABOUT THE ZONE AND NOT ABOUT THE RANGE, which corrects what this docstring
    used to say. `F.lit(datetime)` goes through `time.mktime`, and the range half of that
    is real but SYMMETRIC: measured on this Windows box, `F.lit` raises `OverflowError`
    outside 1970-01-01 .. 3000-12-31 -- at BOTH ends, `datetime(1900, 1, 1)` and
    `datetime(3001, 1, 1)` alike -- and collecting a cast ISO string raises `OSError
    [Errno 22]` for exactly the same values. The cast buys no range at all for a value
    Python has to read back; `opl.gold.columns` tabulates both directions.

    WHAT IT BUYS IS WHICH ZONE THE TEXT IS READ IN. `mktime` uses the DRIVER's
    operating-system zone; a cast string is parsed by Spark in the SESSION zone, which
    `opl.config.SESSION_TIMEZONE` pins to UTC in both gold entry points and in
    `opl.spark.local_session`. So `VALID_FROM_FLOOR` is the epoch itself on every
    machine. Through `F.lit` it would be midnight in the driver's own zone, which is a
    NEGATIVE epoch value east of Greenwich -- and `mktime` refuses one, so the floor
    would fail to be written at all on such a box rather than being written differently.

    THE ONE THING IT CANNOT FIX is the other direction: `collect()` converts back through
    `datetime.fromtimestamp`, which reads the operating system's zone whatever the
    session is set to. A test comparing a collected bound against a Python `datetime`
    is comparing across those two zones, not asserting anything about the dimension."""
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

    `applied_date` IS CAST TO AN INSTANT HERE, ONCE, AND IS ALSO CARRIED THROUGH AS THE
    DATE IT IS. The vault stores it as a DATE, the fact will ask as of a TIMESTAMP, and
    leaving the two to meet in a predicate would make every as-of join rest on an
    implicit widening nobody wrote down. Midnight of the snapshot's own date is the only
    reading available: the RFB tells us the day, not the hour. The uncast column survives
    beside it because `_versioned` hashes THAT -- a DATE has no timezone and the instant
    derived from it does; see the surrogate-key paragraph there.

    NO TIEBREAKER ON THE ORDER, AND TWO LAYERS BELOW THIS ONE ARE WHY. A tie needs one
    key with two rows on one `applied_date`, which `opl.vault.satellites` cannot produce
    (`keyed.groupBy(hub.hash_key, APPLIED_DATE)` is its grain) and `opl.vault.loading
    ._without_persisted` cannot re-add (it anti-joins on the same pair). Stated rather
    than assumed because `load_dimension` accepts an arbitrary `source_table`: handed a
    table that is not a satellite, a tie would make `lag`/`lead` pick a neighbour
    non-deterministically, and a third row on one date would put a ZERO-WIDTH interval in
    the chain -- one that no as-of query can ever return, since the predicate is
    half-open."""
    business = hub.business_key_columns
    joined = source.join(keys, on=hub.hash_key, how="inner").select(
        *(F.col(column) for column in business),
        F.col(APPLIED_DATE),
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

    THE SURROGATE KEY IS TAKEN OVER (business key..., `applied_date`) IN THAT ORDER, and
    the order is part of the key: permuting it re-keys the whole table.

    `applied_date` AND NOT `valid_from`, WHICH IS A CHANGE. `valid_from` is a TIMESTAMP,
    i.e. UTC micros, and the DATE it derives from resolves through
    `spark.sql.session.timeZone` -- so the key MOVED with a cluster setting: measured,
    one business key on one `applied_date` under three session zones produced three
    different `company_sk` values. The fact stores this key.
    `opl.config.SESSION_TIMEZONE` pins the zone; hashing the DATE, which has none, is
    what makes the key stand still when nobody is holding the setting.

    IT COSTS re-derivability from a dimension row, since `applied_date` is not a column
    of one, and BUYS two things: the key is now over the pair the VAULT keys a version on
    (`opl.vault.satellites`' grain is exactly (hash key, `applied_date`)), and it is
    stable under a backfill, where the old key was not -- an earlier snapshot landing
    makes a previously-first version stop being floored, moving its `valid_from` and so
    its key while the row was the same row."""
    valid_from = F.when(
        F.col(_PRECEDING).isNull(), instant_literal(VALID_FROM_FLOOR)
    ).otherwise(F.col(_OBSERVED_AT))
    valid_to = F.coalesce(F.col(_FOLLOWING), instant_literal(VALID_TO_CEILING))
    business = [F.col(column) for column in hub.business_key_columns]
    return bounded.select(
        F.xxhash64(*business, F.col(APPLIED_DATE)).alias(dimension.surrogate_key),
        *business,
        *(F.col(column) for column in satellite.payload_columns),
        valid_from.alias(VALID_FROM),
        valid_to.alias(VALID_TO),
        (valid_to == instant_literal(VALID_TO_CEILING)).alias(IS_CURRENT),
        instant_literal(load_date).alias(LOAD_DATE),
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
        LOAD_DATE: instant_literal(load_date),
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
    hashed over that version's own `applied_date`, which is unchanged -- so what a new
    snapshot changes is exactly the previously-open row's END. A row of that pair missing
    from the target is therefore the signal that the source has moved on, and appending
    it would put two intervals on one surrogate key.

    `valid_to` IS AN INSTANT AND SO THIS HALF OF THE PAIR STILL MOVES WITH THE SESSION
    ZONE, where the key no longer does. `opl.config.SESSION_TIMEZONE` is pinned in both
    gold entry points and in `opl.spark.local_session` precisely so that it cannot, and
    the refusal below names the zone among its causes rather than leaving an operator to
    read "a new snapshot in the source" and drop a 69.2M-row table for the wrong reason.

    BEFORE THE FIRST WRITE, per master protocol section 4.4, because `max_retries: 0`
    does not prevent a retry and this is the branch a retry lands in.

    WHAT IT DOES NOT COVER: a target holding rows the derivation no longer produces
    passes, every derived row still being present.
    `_refuse_a_count_that_is_not_every_version_plus_the_ghost` catches that as a count
    too high; what stays uncovered is a target short and long by the same number, which
    a MERGE-based rebuild is what closes."""
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
            "does to the open version, and it is the likely cause. The other one is a "
            f"session timezone that is not {SESSION_TIMEZONE}: `valid_to` is an INSTANT, "
            "so a rebuild under a moved zone reproduces every row with its bounds shifted "
            "and is refused here saying this. Check "
            f"`spark.conf.get({SESSION_TIMEZONE_CONFIG!r})` before dropping anything; if "
            "it is right, drop the table and rebuild it -- nothing has been written by "
            "this run"
        )


# THE THREE WAYS ONE SURROGATE KEY LANDS ON TWO ROWS, EACH WITH THE REPAIR IT NEEDS --
# and they do NOT share one. This refusal named the first alone and closed on "the repair
# is a wider key, not a re-run", which is right for that cause and wrong for the other
# two: both are defects in the SOURCE, where the key is fine and a re-run over a repaired
# vault is exactly what fixes them. Telling an operator to widen a key would leave the
# duplicate in place and re-key a 69.2M-row dimension for nothing.
#
# A TUPLE THE MESSAGE IS RENDERED FROM rather than three sentences written into it, so
# `tests/gold/test_dim_company.py` can drive one case per cause without restating the
# prose -- the shape `opl.contracts.payments` uses for its declared domains.
COLLISION_CAUSES: tuple[tuple[str, str], ...] = (
    (
        "a genuine xxhash64 collision between two versions -- 64 bits over 69.2M rows "
        "is roughly a 1.3e-4 birthday chance, which is the outcome this count was "
        "measured for rather than an impossible one",
        "a WIDER KEY. A re-run reproduces the same digest from the same inputs",
    ),
    (
        "two hub rows carrying one business key, so the join to the hub FANS OUT and "
        "emits one satellite version twice with identical inputs to the hash",
        "FIX THE HUB and re-run. The key is not the problem and widening it changes "
        "nothing",
    ),
    (
        "two satellite rows for one hash key on one `applied_date`, which the vault's "
        "own grain forbids -- the key IS (business key, `applied_date`), so two such "
        "rows hash to one value by construction",
        "FIX THE SATELLITE and re-run, for the cause above's reason",
    ),
)


def _surrogate_key_collision(
    *, target_table: str, surrogate_key: str, rows: int, distinct: int
) -> str:
    """The refusal text for a surrogate key that is not unique, in one place.

    Extracted like `opl.bronze.registry_collisions._delta_name_collision` and for its
    two reasons: enumerating three causes inline takes the guard past the fifty lines
    this project gives a function, and a refusal text is its own thing to grep for."""
    causes = "".join(
        f" ({number}) {cause}; the repair is {repair}."
        for number, (cause, repair) in enumerate(COLLISION_CAUSES, start=1)
    )
    return (
        f"{target_table} holds {rows} rows and only {distinct} distinct {surrogate_key} "
        "values. Two dimension rows share a surrogate key, so every fact joining on it "
        "would match both -- silently. THREE THINGS PRODUCE THIS AND THEY DO NOT SHARE A "
        f"REPAIR:{causes} THE TABLE ON DISK IS ALREADY WRITTEN and must be dropped "
        "whichever it was."
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
    unlikely versioned row that hashed onto the ghost's reserved key -- which is what
    RESERVES that key, since `xxhash64` returns the full signed 64-bit range and -1 is an
    ordinary value in it (`opl.gold.columns`)."""
    distinct = spark.read.table(target_table).select(dimension.surrogate_key).distinct().count()
    if distinct != rows:
        raise ValueError(
            _surrogate_key_collision(
                target_table=target_table,
                surrogate_key=dimension.surrogate_key,
                rows=rows,
                distinct=distinct,
            )
        )
    return distinct


def _refuse_a_count_that_is_not_every_version_plus_the_ghost(
    hub: Hub,
    *,
    target_table: str,
    source_table: str,
    hub_table: str,
    held: int,
    source_versions: int,
) -> None:
    """Refuse unless the target holds exactly one row per satellite version plus the
    ghost. THE PHASE'S HEADLINE NUMBER, ENFORCED INSTEAD OF OBSERVED.

    `_bounded` joins `how="inner"`, so a satellite version whose hash key matches no hub
    row -- what a hub loaded over a narrower window than its satellite produces, and what
    nothing in the vault errors on -- is DROPPED. Measured: with one hub key deleted the
    load succeeded, reporting `appended = 4` against `source_versions = 4`.
    `_distinct_surrogate_keys` cannot catch it: it compares distinct keys against the row
    count that was WRITTEN, never against the one that was expected.

    CHECKED ON THE COUNT HELD AND NOT ON THE COUNT APPENDED, because it is an invariant
    of every state this loader accepts -- fresh build, idempotent re-run, and a target
    this run did not write. The re-run is the state that was reporting a clean no-op over
    a permanently short dimension. It also catches a count that is too HIGH, which is the
    gap `_refuse_a_target_the_source_has_outgrown` names as its own.

    AFTER THE WRITE, like `_distinct_surrogate_keys` and for its reason: counting before
    means deriving the whole frame twice, and serverless has no `persist()`. So the
    message says the rows are on disk."""
    expected = source_versions + GHOST_ROWS
    if held == expected:
        return
    diagnosis = (
        "satellite versions whose hash key matched no row in the hub -- a dangling "
        "reference the vault does not error on and this build's inner join drops"
        if held < expected
        else "rows the derivation no longer produces -- a satellite row removed after "
        "this table was built, which no other check in this loader can see"
    )
    raise ValueError(
        f"refusing to accept {target_table}: it holds {held} rows and {source_table} "
        f"holds {source_versions} satellite versions, which with {GHOST_ROWS} ghost is "
        f"{expected}. The difference is {diagnosis}. This layer's whole claim is one "
        "dimension row per satellite version, so the table is a wrong answer that every "
        "later re-run would report as a clean no-op. List the offending rows with "
        f"spark.read.table({source_table!r}).join(spark.read.table({hub_table!r}).select("
        f"{hub.hash_key!r}), on={hub.hash_key!r}, how='left_anti'), load the hub over the "
        "window the satellite covers, then DROP THIS TABLE -- the short chain is already "
        "on disk -- and rebuild it"
    )


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

    `load_date` is an argument with no default, for `opl.vault.hubs.load_hub`'s reason: a
    loader that stamps its own clock cannot be asserted against. Idempotent: a re-run over
    an unchanged source writes nothing and reports 0 appended. A source that GAINED a
    snapshot is refused before the first write; a target that is not one row per version
    plus the ghost is refused in every state."""
    _refuse_a_mismatched_source(dimension, satellite, hub)
    source = spark.read.table(source_table)
    _refuse_a_window_that_is_not_the_snapshots_the_source_holds(source, months, dimension)
    # PROJECTED DOWN TO (hash key, business key) BEFORE THE JOIN: the hub's own
    # `load_date` and `record_source` would collide by name with the satellite's.
    keys = spark.read.table(hub_table).select(hub.hash_key, *hub.business_key_columns)
    rows = dimension_rows(
        spark, dimension, satellite=satellite, hub=hub, source=source,
        keys=keys, load_date=load_date,
    )
    before = rows_in(spark, target_table)
    if before:
        _refuse_a_target_the_source_has_outgrown(spark, dimension, rows, target_table)
        after = before
    else:
        rows.write.format("delta").mode("append").saveAsTable(target_table)
        after = rows_in(spark, target_table)
    source_versions = rows_in(spark, source_table)
    _refuse_a_count_that_is_not_every_version_plus_the_ghost(
        hub, target_table=target_table, source_table=source_table,
        hub_table=hub_table, held=after, source_versions=source_versions,
    )
    return DimensionLoadResult(
        table=target_table,
        appended=after - before,
        already_present=before,
        source_versions=source_versions,
        distinct_keys=_distinct_surrogate_keys(spark, dimension, target_table, after),
    )
