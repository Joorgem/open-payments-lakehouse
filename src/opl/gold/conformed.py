# src/opl/gold/conformed.py
"""Build and load the star's conformed dimensions -- `dim_date`, `dim_channel`,
`dim_currency` -- and MEASURE how much of each one the fact actually reaches.

THE NUMBER THIS MODULE EXISTS TO PRODUCE. Two of these three dimensions are constant
columns wearing a dimension's name, and saying so with the word "thin" is not saying it.
`CURRENCIES = ("BRL",)`, so `dim_currency` has ONE member and every fact row points at
it. Every payment measured at Task 0 sits on 2026-08-01 (`docs/f3-run-evidence.md` §0.5,
P1-P3) and the `between-snapshots` profile adds 2026-06-20, so `dim_date`'s FACT-SIDE
cardinality is 1 today and 2 once that profile lands -- against a span of about fifty
days. A dimension whose fact-side cardinality is 1 cannot be wrong, and no test over it
can fail. `fact_side_cardinality` therefore counts it from the fact on every load and
`ConformedLoadResult` carries it beside the member count, so the phase's evidence
publishes two numbers instead of an adjective. `dim_channel` is the one of the three
that is not constant: five declared rails, five reached.

THE MEMBERS ARE THE CONTRACT'S DECLARED DOMAIN AND NEVER `SELECT DISTINCT`. The tempting
build for an enumerated dimension is to read its members out of the fact. It is refused
because it makes the two numbers above the SAME number by construction: a dimension that
cannot contain an unobserved member can never report that a rail went unused, and its
"cardinality" measurement would be a restatement of its own row count. `dim_date` is the
mirror case and goes the other way -- its span IS derived from the data, because the
alternative is fifty date literals in a registry, and the literal that goes stale is the
one nobody re-reads. What keeps that honest is that the derivation reads the two tables
whose dates the star must cover, not the dimension's own future contents.

NO MERGE, NO OVERWRITE, ONE ANTI-JOIN -- and unlike `opl.gold.dimensions` this loader
needs no refusal to be idempotent. That module cannot revise an interval it has already
closed, so a source that gained a snapshot forces it to STOP; a conformed dimension has
no interval and no version chain, so a source that gained a day is simply a day to
append, and a member already present is a member the anti-join drops. Same append-only
write pattern, none of the fragility, and the difference is worth stating because the two
loaders sit beside each other and only one of them can be re-run against a grown source.

THE CALENDAR DAY IS READ OUT OF THE ISO TEXT, WHICH IS THE ONE REAL HAZARD IN THIS FILE.
`event_time` is a UTC instant stored as a STRING (`opl.contracts.payments`: all-string in
bronze, `...T00:00:00.000Z`), and `CAST(... AS TIMESTAMP)` resolves it in the SESSION
timezone. Under America/Sao_Paulo -- which local Spark inherited from the operating
system until `opl.config.SESSION_TIMEZONE` was pinned -- midnight UTC casts to 21:00 of
the PREVIOUS day: every payment's date moves, `dim_date`'s span moves with it, and the
star's answer becomes a function of a cluster setting. `day_of` takes the first ten
characters instead -- the day the producer stamped, zone-free -- and `tests/gold/
test_conformed.py` pins both halves of that, the right answer and the wrong one, SETTING
the wrong zone rather than inheriting it so that the pin cannot make the test vacuous.

THE PIN DOES NOT MAKE `day_of` REDUNDANT, which is the reading to head off. A pinned
setting is a setting: it is one `spark.conf.set` in a notebook away from being something
else, and it governs the whole session rather than this column. Reading the day out of
the text means `dim_date`'s members do not depend on it at all -- what still does is the
`load_date` stamped on every row, which is a plain instant and is why the entry point
sets the pin anyway.

THE SURROGATE KEYS ARE TWO MECHANISMS FOR TWO KINDS, DELIBERATELY. `dim_date` is keyed
`yyyyMMdd` as an int: a day's identity IS its calendar position, the key is readable in
the fact, it sorts, and -- the reason that matters -- `fact_payment` can DERIVE it from
`event_time` without joining anything. An enumerated member has no canonical integer, so
its key is `xxhash64` of the member itself: deterministic, computable on the fact side
without a join, stable when the declared tuple is reordered. Reordering matters here in a
way it does not elsewhere: `opl.contracts.payments` records that the generator picks a
payment method BY INDEX, so a key derived from position would silently re-key the
dimension the day somebody reorders the domain to re-draw a stream."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.gold.columns import (
    CONFORMED_RECORD_SOURCE,
    GHOST_RECORD_SOURCE,
    GHOST_ROWS,
    GHOST_SURROGATE_KEY,
    LOAD_DATE,
    RECORD_SOURCE,
)

# ONE TIMESTAMP PATH FOR THE WHOLE LAYER, which is the only reason a conformed loader
# imports anything of the SCD2 one. `F.lit(datetime)` converts through `time.mktime` --
# the DRIVER's operating-system zone -- where every other instant gold writes is parsed
# by Spark in the SESSION zone, so the two would stamp the same run's `load_date` hours
# apart on a box that is not UTC. `instant_literal` is where that argument lives.
from opl.gold.dimensions import instant_literal
from opl.gold.members import (
    calendar_members,
    calendar_schema,
    enumerated_members,
    enumerated_schema,
)
from opl.gold.specs import CalendarDimension, ConformedDimension, EnumeratedDimension
from opl.vault.columns import APPLIED_DATE
from opl.vault.loading import rows_in

__all__ = [
    "ConformedLoadResult",
    "conformed_rows",
    "covered_span",
    "day_of",
    "fact_side_cardinality",
    "fact_surrogate_key",
    "load_conformed_dimension",
]

# How many characters of an ISO-8601 instant spell its calendar day. See the module
# docstring for why this is a substring and not a cast.
_ISO_DAY_WIDTH = 10

_DATE_KEY_FORMAT = "yyyyMMdd"


@dataclass(frozen=True)
class ConformedLoadResult:
    """What one conformed build did, and the two numbers that decide whether the
    dimension is a dimension or a constant column with a surrogate key on it."""

    table: str
    appended: int
    # What the target already held before this build. A build that appends 0 against a
    # non-zero `already_present` is the idempotent re-run; the pair keeps that
    # distinguishable from a build that had nothing to write.
    already_present: int
    # Rows the dimension holds that are not the ghost -- the member count.
    members: int
    # DISTINCT members of this dimension that the FACT reaches. Measured on every load,
    # from the fact, because it is the only number that separates a conformed dimension
    # from a constant column: see the module docstring.
    fact_side_cardinality: int
    # Distinct surrogate keys in the written table, measured rather than assumed for
    # `opl.gold.dimensions._distinct_surrogate_keys`' reason -- two members sharing a key
    # is a silent double match, and the enumerated key is a 64-bit hash.
    distinct_keys: int


def day_of(column: str) -> Column:
    """The UTC calendar day an ISO-8601 instant names, read from its TEXT.

    NOT `CAST(... AS TIMESTAMP)`, and the module docstring argues why at length: the cast
    resolves the instant in the session timezone, so on America/Sao_Paulo every midnight-
    UTC payment lands on the previous day and `dim_date`'s span shifts with the cluster's
    configuration. Ten characters of ISO text are the day the producer stamped, and they
    mean the same thing on every machine.

    A value that is not an instant yields NULL here rather than an error, which is why
    `covered_span` counts the NULLs instead of trusting `min`/`max` -- those ignore them,
    so an unparseable `event_time` would shrink the span silently."""
    return F.to_date(F.substring(F.col(column), 1, _ISO_DAY_WIDTH))


def _fact_member(dimension: ConformedDimension) -> Column:
    """The value a fact row resolves to a member of `dimension` -- its natural key as the
    FACT spells it, which is not always as the dimension spells it."""
    if isinstance(dimension, CalendarDimension):
        return day_of(dimension.fact_column)
    return F.col(dimension.fact_column)


def fact_side_cardinality(fact: DataFrame, dimension: ConformedDimension) -> int:
    """How many DISTINCT members of `dimension` the rows of `fact` actually reach.

    COUNTED, NEVER DECLARED, and that is the whole contract of this function. It is what
    lets the phase's evidence say `dim_currency` has one member and the fact reaches one
    of them -- a fact about this data -- rather than describing the dimension as thin,
    which is an opinion about it.

    ONE COLUMN IN THE DISTINCT AND NOT A TUPLE, per master protocol §4.8: `COUNT(DISTINCT
    a, b, c)` drops NULL-bearing rows and cost this project 8,761 of them once. A single
    column's distinct count includes NULL as a value, so a fact row whose member cannot be
    read is counted here rather than quietly excluded."""
    return fact.select(_fact_member(dimension).alias("_member")).distinct().count()


def covered_span(
    fact: DataFrame, dimension: CalendarDimension, *, applied: DataFrame
) -> tuple[date, date]:
    """The first and last calendar day `dimension` must contain, measured from the two
    tables whose dates the star refers to.

    TWO SOURCES BECAUSE THE STAR HAS TWO KINDS OF DATE. The fact's `event_time` days are
    what `fact_payment` joins on; the vault's `applied_date`s are where `dim_company`'s
    version boundaries sit, and a calendar that cannot name the day a version opened is a
    calendar the star's own history falls outside of.

    ONE PASS EACH. The satellite side is a min/max over one date column of a 69.2M-row
    table -- a real scan, and cheap beside the join `dim_company` itself runs. The fact
    side counts unreadable days in the SAME aggregate, because `min`/`max` ignore NULLs:
    an `event_time` that is not an instant would otherwise shrink the span silently and
    send its own payments to the ghost."""
    day = day_of(dimension.fact_column)
    measured = fact.agg(
        F.min(day).alias("first"),
        F.max(day).alias("last"),
        F.count(F.when(day.isNull(), 1)).alias("unreadable"),
    ).collect()[0]
    _refuse_unreadable_event_times(dimension, measured["unreadable"])
    snapshots = applied.agg(
        F.min(APPLIED_DATE).alias("first"), F.max(APPLIED_DATE).alias("last")
    ).collect()[0]
    ends = [value for value in (measured, snapshots) if value["first"] is not None]
    if not ends:
        raise ValueError(
            f"refusing to build {dimension.name!r}: neither the fact nor "
            f"{dimension.applied_date_source!r} carries a single readable date, so the "
            "calendar has no span to cover and would hold nothing but its ghost"
        )
    return min(end["first"] for end in ends), max(end["last"] for end in ends)


def _refuse_unreadable_event_times(dimension: CalendarDimension, unreadable: int) -> None:
    """Refuse a fact carrying a `fact_column` value that is not an ISO-8601 instant."""
    if unreadable:
        raise ValueError(
            f"refusing to build {dimension.name!r}: {unreadable} fact rows carry a "
            f"{dimension.fact_column!r} from which no calendar day can be read. `min` and "
            "`max` IGNORE NULLs, so those rows would not widen the span and would not "
            "narrow it either -- the calendar would be built without them and every one "
            "of them would resolve to the unknown member, with the build reporting "
            "success. The payment contract makes every column required and non-empty, so "
            "this is a bronze row that did not come through the DQ gate"
        )


def _member_frame(
    spark: SparkSession, dimension: ConformedDimension, span: tuple[date, date] | None
) -> DataFrame:
    """The dimension's members as a frame: natural key first, attributes after.

    A `ValueError` AND NOT AN `assert` for the missing span, even though
    `load_conformed_dimension` cannot reach this branch without one: `conformed_rows` is
    public, `python -O` strips asserts, and a calendar built from `None` would raise a
    `TypeError` inside `calendar_members` naming an argument rather than a table."""
    if isinstance(dimension, CalendarDimension):
        if span is None:
            raise ValueError(
                f"{dimension.name!r} is a calendar and was given no span. Measure one "
                "with `covered_span`, which reads the fact's own days and the "
                f"`applied_date`s of {dimension.applied_date_source!r}"
            )
        return spark.createDataFrame(
            calendar_members(*span), calendar_schema(dimension.natural_key)
        )
    return spark.createDataFrame(
        enumerated_members(dimension.members), enumerated_schema(dimension.natural_key)
    )


def _member_key(dimension: ConformedDimension, member: Column) -> Column:
    """`member`'s surrogate key under `dimension`'s own mechanism, whichever column the
    member arrived in.

    TWO MECHANISMS FOR TWO KINDS -- see the module docstring. `yyyyMMdd` for a day,
    because a date HAS a canonical integer identity and the fact can derive the key
    without a join; `xxhash64` for a member that has none.

    THE COLUMN IS AN ARGUMENT PRECISELY SO THE DIMENSION AND THE FACT CANNOT DRIFT. The
    same member arrives here as `full_date` when the dimension is built and as
    `to_date(substring(event_time, 1, 10))` when the fact is, and as `channel_code` against
    `payment_method`. Two spellings of the mechanism would be two keys for one member --
    silently, since both are integers and the join simply returns nothing -- so there is
    one, and `tests/gold/test_fact_payment.py` pins that the two call sites agree on every
    declared member."""
    if isinstance(dimension, CalendarDimension):
        return F.date_format(member, _DATE_KEY_FORMAT).cast("int")
    return F.xxhash64(member)


def _surrogate_key(dimension: ConformedDimension) -> Column:
    """`dimension`'s surrogate key, over its own natural key column."""
    return _member_key(dimension, F.col(dimension.natural_key))


def fact_surrogate_key(dimension: ConformedDimension) -> Column:
    """The key a FACT row derives for `dimension`, from the fact's OWN column and with no
    join at all.

    NO JOIN, WHICH IS A PRE-DECISION OF THIS MODULE RATHER THAN A SHORTCUT TAKEN BY THE
    FACT. The module docstring above states it as the reason the two key mechanisms are
    what they are: a day's key IS its calendar position, so `fact_payment` can derive it
    from `event_time`; an enumerated member's key is `xxhash64` of the member itself,
    "computable on the fact side without a join". Resolving these three by joining would
    be three shuffles to look up a value the row already carries.

    WHAT IT COSTS, STATED BECAUSE IT IS REAL: a derived key cannot fall back on the ghost.
    An `event_time` outside the span `dim_date` was built over yields a key no member
    holds, where a LEFT JOIN would have yielded NULL and been coalesced. That is why
    `opl.gold.facts` measures referential integrity per conformed dimension AFTER the
    write instead -- the orphan is found by counting, not by joining, and the two
    enumerated dimensions cannot produce one at all while the generator picks from the
    domains the contract declares.

    AN ABSENT VALUE IS THE CASE THAT LAST CLAUSE DOES NOT COVER, AND IT PRODUCES A KEY
    RATHER THAN A NULL. `xxhash64(NULL)` is **42** -- measured, and 42 whatever the NULL's
    type, because Spark seeds the hash at 42 and a NULL field contributes nothing to it.
    So a payment whose `payment_method` or `currency` is absent derives key 42, which no
    declared member holds and which is not `GHOST_SURROGATE_KEY` either. It is COUNTED AS
    AN ORPHAN AND REPORTED, exactly like a day outside `dim_date`'s span, and that is the
    intended outcome: the row is written, the fact says one lookup does not resolve, and
    nothing is silently attributed to a member.

    IT IS DELIBERATELY NOT A REFUSAL, WHICH IS THE ASYMMETRY WORTH NAMING.
    `opl.gold.facts._refuse_payments_no_instant_can_be_read` STOPS the build over a NULL
    `event_time` or a NULL measure, because those are silent in a way this is not -- a
    NULL instant matches no half-open interval and ghosts BOTH role keys while the run
    reports a merely lower resolution rate, and a NULL measure lowers every SUM by an
    amount nobody can name. An absent enumerated value costs one row one dimension lookup
    and says so in a number. `tests/gold/test_conformed.py` pins that 42 is held by no
    declared member of either enumerated dimension, which is what makes "reported" and not
    "silently attributed" true."""
    return _member_key(dimension, _fact_member(dimension))


def _keyed(
    dimension: ConformedDimension, members: DataFrame, load_date: datetime
) -> DataFrame:
    """One dimension row per member, in the dimension's declared column order.

    THE ORDER IS PINNED AND IS LOAD-BEARING, for `opl.gold.dimensions._versioned`'s
    reason: a Delta append matches POSITIONALLY unless `mergeSchema` says otherwise, so
    two builds projecting the same columns in two orders would write each other's values
    with neither failing."""
    return members.select(
        _surrogate_key(dimension).alias(dimension.surrogate_key),
        "*",
        instant_literal(load_date).alias(LOAD_DATE),
        F.lit(CONFORMED_RECORD_SOURCE).alias(RECORD_SOURCE),
    )


def _ghost_like(
    spark: SparkSession, keyed: DataFrame, dimension: ConformedDimension, load_date: datetime
) -> DataFrame:
    """The one unknown-member row, shaped from `keyed`'s own schema.

    BUILT FROM THE SCHEMA RATHER THAN FROM A COLUMN LIST, exactly as
    `opl.gold.dimensions._ghost_like` is and for the same reason -- a ghost that listed
    its columns by hand goes stale the day the calendar gains an attribute. It is not
    SHARED with that function because the two fix different columns: that one pins three
    interval bounds a conformed dimension does not have, and a helper taking the fix map
    as an argument would be a third module for eight lines of mechanism.

    THE NATURAL KEY IS NULL, WHICH IS WHAT MAKES THE ROW UNREACHABLE BY A JOIN. A fact
    row arrives at it as `COALESCE(<lookup>, GHOST_SURROGATE_KEY)` at fact-build time and
    never by matching -- so a ghost carrying a real member would silently merge every
    unresolved payment onto that member, which is the failure `opl.gold.columns` records
    for `'00000000'`."""
    fixed = {
        dimension.surrogate_key: F.lit(GHOST_SURROGATE_KEY),
        LOAD_DATE: instant_literal(load_date),
        RECORD_SOURCE: F.lit(GHOST_RECORD_SOURCE),
    }
    return spark.range(1).select(
        *(
            fixed.get(field.name, F.lit(None)).cast(field.dataType).alias(field.name)
            for field in keyed.schema.fields
        )
    )


def conformed_rows(
    spark: SparkSession,
    dimension: ConformedDimension,
    *,
    load_date: datetime,
    span: tuple[date, date] | None = None,
) -> DataFrame:
    """Every row the dimension will hold: one per member, plus the ghost.

    Public for `opl.gold.dimensions.dimension_rows`' reason -- the frame is worth having
    without the write, both for a test and for whatever builds the fact next."""
    keyed = _keyed(dimension, _member_frame(spark, dimension, span), load_date)
    return keyed.unionByName(_ghost_like(spark, keyed, dimension, load_date))


def _refuse_a_mismatched_source(
    dimension: ConformedDimension, applied_date_table: str | None
) -> None:
    """Refuse a kind this loader cannot build, and a kind/argument pairing that would
    build the wrong thing quietly.

    THE MISSING SATELLITE IS THE ONE THAT MATTERS. Defaulted rather than refused, a
    `dim_date` built without it would take its span from the payment window alone -- a
    calendar that silently stops covering the days `dim_company`'s versions open on, with
    the build reporting success and every count looking right. The other direction is the
    copied-YAML shape: an argument left behind from the task above, silently ignored."""
    if not isinstance(dimension, EnumeratedDimension | CalendarDimension):
        raise ValueError(
            f"{getattr(dimension, 'name', dimension)!r} is not a conformed dimension. "
            "This loader builds a declared value domain or a calendar; an SCD2 dimension "
            "is a satellite's version chain and is built by opl.gold.dimensions, which "
            "resolves a parent hub and closes an interval this one has no concept of"
        )
    if isinstance(dimension, CalendarDimension) and applied_date_table is None:
        raise ValueError(
            f"{dimension.name!r} needs the satellite it declares "
            f"({dimension.applied_date_source!r}) to measure its span: its calendar must "
            "cover both RFB `applied_date`s, which is where dim_company's version "
            "boundaries sit. Without it the span would be the payment window alone and "
            "the build would report success"
        )
    if isinstance(dimension, EnumeratedDimension) and applied_date_table is not None:
        raise ValueError(
            f"{dimension.name!r} reads no satellite -- its members are a value domain "
            f"opl.contracts.payments declares -- and was handed {applied_date_table!r}. "
            "An ignored table argument is a paste from the calendar task above it"
        )


def _distinct_surrogate_keys(
    spark: SparkSession, dimension: ConformedDimension, target_table: str, rows: int
) -> int:
    """How many distinct surrogate keys the written table holds, refusing if that is not
    every row.

    ONE COLUMN, NOT A TUPLE, per `opl.gold.dimensions._distinct_surrogate_keys`. The
    enumerated key is a 64-bit hash over a handful of members, so a collision is not the
    hazard here that it is at 69.2M rows -- what this catches is a member declared twice
    reaching the table anyway, and the day whose `yyyyMMdd` no longer fits an int.

    THAT SECOND ONE IS MEASURED, AND THE YEAR IS NOT THE ONE THIS DOCSTRING FIRST NAMED.
    `_member_key` renders the day with `date_format` and parses it with `cast("int")`, so
    the ceiling is 2,147,483,647 READ AS A `yyyyMMdd`: 21480101 fits, 99990101 fits,
    2147481231 fits, and the first day that does not is 214749-01-01 -- which Spark
    renders `+2147490101`, having prefixed a `+` to every year of five digits or more
    since 10000. The cast returns NULL rather than raising. NULL is a VALUE to `distinct`,
    so ONE overflowed day still counts once and passes here; TWO collapse into one and
    this refuses. "Year 2148" was the int ceiling misread as a date key, and the
    correction is recorded rather than deleted because the MECHANISM is real and silent
    even though no span this star derives can reach the year. Pinned by
    `tests/gold/test_conformed.py::test_the_day_key_ceiling_is_the_int_read_as_a_date`."""
    distinct = spark.read.table(target_table).select(dimension.surrogate_key).distinct().count()
    if distinct != rows:
        raise ValueError(
            f"{target_table} holds {rows} rows and only {distinct} distinct "
            f"{dimension.surrogate_key} values. Two members share a surrogate key, so "
            "every fact row joining on it matches both -- silently. THE TABLE ON DISK IS "
            "ALREADY WRITTEN and must be dropped before a repaired build"
        )
    return distinct


def _new_rows(spark: SparkSession, dimension: ConformedDimension, rows: DataFrame, target: str):
    """`rows` minus what the target already holds, by surrogate key.

    AN ANTI-JOIN AND NOT A REFUSAL, which is where this loader parts company with
    `opl.gold.dimensions`. That one must STOP when its source has grown, because an
    append cannot revise an interval it already closed; nothing here has an interval, so
    a span that grew by a day is a day to append and a member already present is a row to
    drop. A member whose ATTRIBUTES changed would be kept as-is rather than revised --
    which cannot happen to a calendar day and cannot happen to a one-column enumerated
    member, and is the seam a MERGE would close if a conformed dimension ever gained a
    mutable attribute."""
    existing = spark.read.table(target).select(dimension.surrogate_key)
    return rows.join(existing, on=dimension.surrogate_key, how="left_anti")


def load_conformed_dimension(
    spark: SparkSession,
    dimension: ConformedDimension,
    *,
    fact_table: str,
    target_table: str,
    load_date: datetime,
    applied_date_table: str | None = None,
) -> ConformedLoadResult:
    """Build `dimension` and append it, then measure how much of it the fact reaches.

    `load_date` is an argument with no default, for `opl.vault.hubs.load_hub`'s reason: a
    loader that stamps its own clock cannot be asserted against, and in the data it makes
    the LDTS a record of when the pipeline ran rather than a value the job's parameters
    pin.

    Idempotent by surrogate key, with no refusal needed -- see `_new_rows`."""
    _refuse_a_mismatched_source(dimension, applied_date_table)
    fact = spark.read.table(fact_table)
    span = None
    if applied_date_table is not None:
        span = covered_span(fact, dimension, applied=spark.read.table(applied_date_table))
    rows = conformed_rows(spark, dimension, load_date=load_date, span=span)
    before = rows_in(spark, target_table)
    if before:
        rows = _new_rows(spark, dimension, rows, target_table)
    rows.write.format("delta").mode("append").saveAsTable(target_table)
    after = rows_in(spark, target_table)
    return ConformedLoadResult(
        table=target_table,
        appended=after - before,
        already_present=before,
        members=after - GHOST_ROWS,
        fact_side_cardinality=fact_side_cardinality(fact, dimension),
        distinct_keys=_distinct_surrogate_keys(spark, dimension, target_table, after),
    )
