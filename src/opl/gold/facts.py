# src/opl/gold/facts.py
"""Build `fact_payment`: one row per payment EVENT, a degenerate `transaction_id`, and a
foreign key per counterparty ROLE resolved AS OF that payment's own `event_time`.

THE PHASE'S HEADLINE, AND THE ONE THING IN IT THAT CAN FAIL SILENTLY IN BOTH DIRECTIONS.
An as-of join that always returns the same version is a join with extra columns -- Task 0
measured every payment in bronze on 2026-08-01, AFTER both RFB `applied_date`s, which
makes the lookup bit-identical to `WHERE valid_to = <sentinel>` and indistinguishable from
the naive answer. A join that returns two versions is worse: the row count doubles and
every measure doubles with it. Both are refused or measured below, and neither is
described in prose.

--- THE FOUR DECISIONS, EACH OF WHICH HAS A CHEAPER WRONG ANSWER --------------------

TWO ROLE-PLAYING FOREIGN KEYS INTO ONE DIMENSION, NEVER ONE. `payer_company_sk` and
`payee_company_sk` both resolve against `dim_company`, because "conformed" means one
dimension answers one question for every fact that asks it. The acceptance this task
inherited -- "every `fact_payment` row resolves to exactly one `dim_company` version" --
is ill-formed: a correct row resolves to TWO, one per role. Its satisfiable reading is a
fact that joins on the payer alone, and nothing about that fails. So resolution is
measured at (ROW, ROLE) grain: `2 x COUNT(*)` references, counted per role, with the
MULTI-MATCH checked separately by the row count -- a fan-out does not lower a resolved
count, it raises the row count, and only one of the two measurements can see it.

DEDUPLICATION IS BY `transaction_id` AND BY NOTHING ELSE. `opl.contracts.payments` grants
the distinction its whole docstring: a DUPLICATE is one payment delivered twice (the same
id), a LEGITIMATE REPEAT is a different payment that happens to look the same (a different
id, an identical business-attribute tuple). Both promoted streams carry
`_REPEAT_COUNT = 800` repeats on purpose, so a fact that deduplicated on the attributes --
or on a "natural key" built from (payer, payee, amount, currency, payment_method), which
is the obvious thing to reach for -- deletes 1,600 real payments and returns a plausible
18,400. `opl.gold.specs._assert_the_grain_is_the_events_own_identity` refuses that at
declaration; `_refuse_a_row_count_that_is_not_one_per_delivered_identity` refuses it on
the written table.

AND THE 150-DUPLICATE ACCEPTANCE IS A TAUTOLOGY, WHICH IS WHY IT IS NOT USED AS ONE.
"The build removed 150 duplicates" is `COUNT(*) - COUNT(DISTINCT <grain>)` BY DEFINITION
OF THE OPERATION: it re-measures bronze's own arithmetic and comes out right whichever
column the deduplication was taken over. What is published instead is `legitimate_repeats`
-- rows the fact holds beyond its distinct business tuples -- which is 1,600 when the
repeats survived and 0 when they did not.

HALF-OPEN INTERVALS, NEVER `BETWEEN`. `valid_from <= t < valid_to`. `BETWEEN` is inclusive
at both ends, so a payment landing exactly on a version boundary matches the version that
closed there AND the one that opened there -- the multi-match this fact's own acceptance
forbids, manufactured by the operator that reads most naturally. `opl.gold.columns` argues
it where the sentinels are declared.

--- THE ZONE, WHICH IS THE SHARPEST INSTANCE IN THIS REPOSITORY ---------------------

`event_time` IS ISO-8601 TEXT IN BRONZE AND `valid_from`/`valid_to` ARE TIMESTAMPS, so the
as-of comparison crosses two representations. It is made with `to_timestamp(text,
"yyyy-MM-dd'T'HH:mm:ss.SSSXXX")` -- an explicit-format parse in which the ZONE DESIGNATOR
IS MANDATORY -- and never with `CAST(... AS TIMESTAMP)`.

THE ARGUMENT IS NOT THE ONE THE HAZARD'S NAME SUGGESTS, AND SAYING SO PRECISELY IS THE
POINT. Measured on this box, pyspark 3.5.9, one string under three session zones:

    text                       to_timestamp(XXX)   CAST(... AS TIMESTAMP)   to_date(cast)
    2026-08-01T00:00:00.000Z   1785542400          1785542400               2026-08-01 (UTC)
    2026-08-01T00:00:00.000Z   1785542400          1785542400               2026-07-31 (BRT)
    2026-08-01T00:00:00.000Z   1785542400          1785542400               2026-08-01 (JST)
    2026-08-01T00:00:00.000    NULL                (resolves in the SESSION zone)

The cast is NOT wrong about the instant while the text carries `Z`: Spark honours the
offset and both spellings agree to the microsecond in every zone. What moves with the zone
is RENDERING an instant back to a DAY, which is `opl.gold.conformed.day_of`'s subject and
not this one. What the cast is wrong about is the CONTRACT: it ACCEPTS a value carrying no
offset and silently resolves it through `spark.sql.session.timeZone`, so the instant
becomes a function of a cluster setting with nothing in the data to show it. The explicit
pattern yields NULL for that value instead, and `_refuse_payments_no_instant_can_be_read`
turns the NULL into a refusal naming the rows. That is what the choice buys: not a
different number today, but the impossibility of a different number tomorrow.

THE DIMENSION SIDE NEEDS NO CONVERSION AT ALL, and that is the other half. `valid_from` and
`valid_to` are already instants, written by `opl.gold.dimensions` under the UTC pin
`opl.config.SESSION_TIMEZONE` sets, and stored as UTC micros -- so reading them back under
any session zone yields the same instant, and the comparison below is between two instants
rather than between an instant and a rendering. `tests/gold/test_fact_payment.py` builds
the whole fact under `America/Sao_Paulo` and asserts every resolved version and every
`event_date_key` is unchanged.

--- WHERE THE EXPENSIVE FRAME IS CONSUMED, ONCE PER ROLE AND NEVER MORE -------------

`dim_company` is 69,202,818 rows and it is the only expensive thing this loader touches.
No `persist()`, no `cache()` -- serverless refuses both (master protocol section 4.3) and
an AST sweep catches them -- so the reduction is structural instead:

  1. `_counterparties` reads the 20,150-row bronze table and produces the <=1,024 distinct
     CNPJs this fact can possibly reference.
  2. `_versions_the_fact_can_reach` scans `dim_company` ONCE per role, projected to four
     columns, and filters it by a BROADCAST semi-join against that key set before anything
     is shuffled. What survives is ~1,027 rows -- 1,024 companies, three of which carry two
     versions (`docs/f3-run-evidence.md` section 0.5, P6 falsified at 3).
  3. Each role join then broadcasts THAT, so the as-of comparison is a broadcast hash join
     on the business key with the interval as a residual predicate. There is no shuffle of
     the dimension anywhere in this loader.

The alternative -- joining 40,000 references straight into 69.2M rows -- cannot broadcast
either side (the build side of a LEFT OUTER join is the right side, whichever way the join
is spelled), so it is a sort-merge join shuffling 69.2M rows per role.

WHAT *IS* DERIVED TWICE, SAID PLAINLY: the deduplication. `_deduplicated` runs once into
the write and once for `retained_tuples`, which is 20,150 rows through one window each
time. That is the cheap frame; the dimension is the expensive one, and it is read once per
role because two roles need two independent answers.

--- WHAT IS MEASURED AND WHAT IS REFUSED --------------------------------------------

Refused BEFORE the first write: a payment whose `event_time` carries no readable instant or
whose measure is not a number (both would resolve to the ghost or to NULL with the build
reporting success), and a deduplication that lost a business tuple.

Refused AFTER it, in the shape `opl.gold.dimensions` uses and with the same closing
sentence: a row count that is not one row per delivered identity. That single number covers
BOTH remaining failures -- a fan-out raises it, a deduplication taken over the wrong
columns lowers it -- and neither is visible in a resolution rate.

REPORTED and never refused: rows resolving to the ghost, per role, and rows whose derived
conformed key matches no member of the dimension it names. Both are legitimate states of a
correct fact built at the wrong moment -- a conformed dimension built before a payment
batch landed simply lacks that batch's days, and the repair is to re-run the conformed
build, which APPENDS. `gold_load_fact.py` prints a different sentence for each.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from opl.contracts import payments
from opl.gold.columns import (
    GHOST_SURROGATE_KEY,
    LOAD_DATE,
    RECORD_SOURCE,
    VALID_FROM,
    VALID_TO,
)
from opl.gold.conformed import fact_surrogate_key

# ONE TIMESTAMP PATH FOR THE WHOLE LAYER, which is the only reason this module imports
# anything of the SCD2 loader -- `opl.gold.conformed` imports it for the same sentence.
# `F.lit(datetime)` converts through `time.mktime`, i.e. the DRIVER's operating-system
# zone, where every other instant gold writes is parsed by Spark in the SESSION zone.
from opl.gold.dimensions import instant_literal
from opl.gold.specs import ConformedDimension, PaymentFact, Scd2Dimension
from opl.vault.loading import BRONZE_RECORD_SOURCE, rows_in
from opl.vault.registry import Hub

__all__ = [
    "AMOUNT_TYPE",
    "ISO_INSTANT_FORMAT",
    "FactLoadResult",
    "event_instant",
    "fact_rows",
    "load_fact",
]

# THE ZONE DESIGNATOR IS PART OF THE PATTERN AND THAT IS THE WHOLE POINT -- see the module
# docstring. `XXX` accepts `+HH:MM` and the literal `Z`; a value carrying neither yields
# NULL here, where a plain cast would resolve it through the session timezone.
ISO_INSTANT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

# THE MEASURE'S TYPE. The scale is the CONTRACT's (`AMOUNT_SCALE`, two, because BRL has
# centavos) rather than a literal, so a currency with a different minor unit moves it in
# one place. The precision is 18: `MAX_AMOUNT_CENTS` is 5,000,000, i.e. seven digits before
# the point, and a DECIMAL leaves no room for the binary rounding a DOUBLE would introduce
# into a column whose whole contract is that it is an exact number of centavos.
AMOUNT_TYPE = f"decimal(18, {payments.AMOUNT_SCALE})"

# Internal to `_deduplicated`, and named because it is selected through by string.
_DELIVERY = "_delivery"

# Internal to the as-of join: the payment's instant, derived once and compared twice.
_EVENT_INSTANT = "_event_instant"


@dataclass(frozen=True)
class FactLoadResult:
    """What one fact build did, and the numbers that make its grain checkable against
    bronze rather than against a document."""

    table: str
    appended: int
    # What the target already held before this build. A build that appends 0 against a
    # non-zero `already_present` is the idempotent re-run.
    already_present: int
    # `rows_in` over BRONZE. Free -- Delta answers a whole-table count from the transaction
    # log -- and it is one half of the deduplication arithmetic.
    source_rows: int
    # Distinct `grain_key` values in bronze. THE ROW COUNT THIS FACT MUST HAVE, and
    # `source_rows` minus this is the redelivery count that the acceptance calls 150.
    source_identities: int
    # Distinct BUSINESS-ATTRIBUTE tuples in bronze, by `GROUP BY` and never by
    # `COUNT(DISTINCT a, b, c)` -- master protocol section 4.8, an 8,761-row defect in this
    # repository's own history.
    source_tuples: int
    # The same count over the DEDUPLICATED frame, which is what the fact's rows are a pure
    # projection of. Measured there rather than on the written table because the fact
    # carries surrogate keys where bronze carries CNPJs, and recovering them would mean two
    # more passes over a 69.2M-row dimension for a number that cannot move between the two.
    retained_tuples: int
    # Rows the fact holds beyond its distinct business tuples: the LEGITIMATE REPEATS, and
    # the number the phase publishes instead of the tautological duplicate count. ZERO here
    # is not success -- it means the fact deduplicated on the attributes and deleted them.
    legitimate_repeats: int
    # Rows resolving to the ghost, PER ROLE, over `len(roles) x rows` references. Predicted
    # 0 on this data and reported as an UNEXERCISED path rather than as a success: F1b
    # measured 1,024/1,024 counterparties resolving to `hub_empresa`, and `dim_company`
    # covers every hub key, so `COALESCE(<lookup>, GHOST)` cannot fire.
    unresolved: tuple[tuple[str, int], ...]
    # Rows whose DERIVED conformed key matches no member of the dimension it names, per
    # dimension. The price of deriving those three keys without a join, paid by counting.
    orphaned: tuple[tuple[str, int], ...]


def event_instant(column: str) -> Column:
    """The INSTANT an ISO-8601 payment timestamp names, parsed with its ZONE REQUIRED.

    NOT `CAST(... AS TIMESTAMP)`, and the module docstring tabulates why with the
    measurement: the cast agrees with this to the microsecond for text that carries `Z`,
    and ACCEPTS text that does not, resolving it through `spark.sql.session.timeZone`. This
    yields NULL for that value, which `_refuse_payments_no_instant_can_be_read` turns into
    a refusal naming the rows.

    NOT `opl.gold.conformed.day_of` EITHER, and the two must not be confused. That one
    reads a calendar DAY out of the first ten characters, because a day rendered from an
    instant moves with the session zone; this one reads the INSTANT, which does not move at
    all. The fact carries both -- `event_date_key` from the text, the as-of comparison from
    the instant -- and they answer different questions about the same column."""
    return F.to_timestamp(F.col(column), ISO_INSTANT_FORMAT)


def _refuse_a_mismatched_source(
    fact: PaymentFact,
    dimension: Scd2Dimension,
    hub: Hub,
    conformed: Sequence[ConformedDimension],
) -> None:
    """The fact, its dimension, that dimension's hub and the conformed dimensions arrive as
    four arguments, so something has to check they belong together.

    Four arguments for `opl.gold.dimensions._refuse_a_mismatched_source`'s reason: a loader
    that resolved its sources through the module-level registry could not be driven with a
    throwaway spec. What each mismatch would cost is worth naming. Another dimension builds
    a well-formed fact against another table's version chain. A hub with a COMPOSITE
    business key cannot be reached from one counterparty column at all -- the join would be
    on the first component and would match every company sharing it. And the conformed
    dimensions are checked IN ORDER, because they are projected in order and a Delta append
    matches POSITIONALLY: two of them transposed writes each key into the other's column,
    and all three are integers, so nothing fails."""
    if dimension.name != fact.company_dimension:
        raise ValueError(
            f"payment fact {fact.name!r} resolves against {fact.company_dimension!r} and "
            f"was handed dimension {dimension.name!r}. Both role keys would be as-of "
            "lookups into another table's version chain, and the build would not fail "
            "doing it -- resolve the dimension with opl.gold.registry.table_spec rather "
            "than passing one by hand"
        )
    if len(hub.business_key_columns) != 1:
        raise ValueError(
            f"payment fact {fact.name!r} joins one counterparty column to "
            f"{dimension.name!r}, whose business key is "
            f"{list(hub.business_key_columns)} -- {len(hub.business_key_columns)} columns. "
            "A composite key cannot be reached from one column: the join would match on "
            "the first component alone and resolve every company that shares it"
        )
    handed = tuple(item.name for item in conformed)
    if handed != fact.conformed:
        raise ValueError(
            f"payment fact {fact.name!r} reaches {list(fact.conformed)} IN THAT ORDER and "
            f"was handed {list(handed)}. The keys are projected in the declared order and "
            "a Delta append matches positionally, so a permutation writes each dimension's "
            "key into another's column -- and all of them are integers, so nothing fails"
        )


def _refuse_payments_no_instant_can_be_read(
    fact: PaymentFact, *, unreadable_instants: int, unreadable_amounts: int
) -> None:
    """Refuse a payment whose `event_time` carries no zone-qualified instant, or whose
    measure is not a number.

    BOTH ARE SILENT AND BOTH ARE THE SAME SHAPE. A NULL instant matches no half-open
    interval, so the row resolves to the ghost in BOTH roles and the build reports a
    resolution rate that is merely lower; a NULL measure sums to nothing and lowers every
    total by an amount nobody can name. The payment contract makes every column required
    and non-empty and the DQ gate rejects both spellings of absent, so either count above
    zero is a bronze row that did not come through the gate -- or an `event_time` written
    without its `Z`, which is the one this loader's parse exists to catch."""
    if unreadable_instants:
        raise ValueError(
            f"refusing to build {fact.name!r}: {unreadable_instants} bronze rows carry an "
            f"{payments.EVENT_TIME_COLUMN!r} from which no instant can be read. The format "
            f"is {ISO_INSTANT_FORMAT} and the ZONE DESIGNATOR IS REQUIRED -- a value "
            "without one would otherwise be resolved through spark.sql.session.timeZone, "
            "which makes the as-of answer a function of a cluster setting. Every such row "
            "matches no half-open interval, so it would resolve to the unknown member in "
            "BOTH roles with the build reporting success"
        )
    if unreadable_amounts:
        raise ValueError(
            f"refusing to build {fact.name!r}: {unreadable_amounts} bronze rows carry a "
            f"{fact.measure!r} that is not a {AMOUNT_TYPE}. It would be written NULL, and "
            "a SUM over the fact would come back smaller with nothing to show for it. The "
            "payment contract makes every column required and the DQ gate rejects blanks, "
            "so this is a row that did not come through the gate"
        )


def _measured_source(spark: SparkSession, fact: PaymentFact, source_table: str) -> DataFrame:
    """The bronze payments, with the two unreadable-value counts already refused.

    ONE AGGREGATE FOR BOTH COUNTS, over the 20,150-row source and before anything is
    derived -- so the refusal happens before the first write rather than after it, which is
    the opposite of where the post-write checks sit and is affordable for exactly the
    reason they are not: this table is small and the dimension is not."""
    source = spark.read.table(source_table)
    measured = source.agg(
        F.count(F.when(event_instant(payments.EVENT_TIME_COLUMN).isNull(), 1)).alias("t"),
        F.count(F.when(F.col(fact.measure).cast(AMOUNT_TYPE).isNull(), 1)).alias("m"),
    ).collect()[0]
    _refuse_payments_no_instant_can_be_read(
        fact, unreadable_instants=measured["t"], unreadable_amounts=measured["m"]
    )
    return source


def _bronze(
    spark: SparkSession, fact: PaymentFact, source_table: str
) -> tuple[DataFrame, int, int]:
    """The payments and their two tuple counts, with everything refusable already refused.

    EVERYTHING THAT CAN STOP THIS BUILD BEFORE IT WRITES A ROW HAPPENS HERE, in three
    passes over a small table: the unreadable instants and measures, the distinct business
    tuples bronze holds, and the same count over the deduplicated frame. Grouped into one
    function rather than left inline so `load_fact` reads as the four things it does --
    check, derive, append, reconcile -- rather than as a list of eleven statements."""
    source = _measured_source(spark, fact, source_table)
    source_tuples = _distinct_business_tuples(source)
    retained_tuples = _distinct_business_tuples(_deduplicated(source, fact))
    _refuse_a_deduplication_that_lost_a_business_tuple(
        fact, source_tuples=source_tuples, retained_tuples=retained_tuples
    )
    return source, source_tuples, retained_tuples


def _distinct_business_tuples(frame: DataFrame) -> int:
    """How many distinct BUSINESS-ATTRIBUTE tuples `frame` carries.

    A `GROUP BY` AND NEVER `COUNT(DISTINCT a, b, c)` -- master protocol section 4.8. The
    banned operator DROPS every row that is NULL in any argument, which is how 8,761 rows
    once went missing in this repository; `SELECT DISTINCT` treats NULL as a value, so this
    count is total over the frame whatever it holds. The distinction is inert on required
    columns and this is the count the whole repeat claim rests on, so it is spelled the
    safe way regardless.

    `BUSINESS_ATTRIBUTE_COLUMNS` AND NOT A LIST WRITTEN HERE. `opl.contracts.payments`
    refuses at import any member of that tuple that is not REQUIRED, which is what makes
    the NULL question inert; a copy of the list here would be a copy that outlives the
    guarantee."""
    return frame.select(*payments.BUSINESS_ATTRIBUTE_COLUMNS).distinct().count()


def _deduplicated(source: DataFrame, fact: PaymentFact) -> DataFrame:
    """One row per `grain_key`: the payment, not the delivery.

    BY THE GRAIN KEY AND BY NOTHING ELSE -- the module docstring argues the 1,600 payments
    the alternative deletes.

    `row_number()` AND NOT `dropDuplicates`, AND THE DIFFERENCE IS DETERMINISM. A
    redelivery is byte-identical to the delivery it repeats (`opl.generator.defects`: "the
    same bytes again"), so which copy survives cannot matter -- but that is a property of
    the data and not of the operator, and `dropDuplicates` picks by partition assignment,
    so a stream that ever delivered one id twice with DIFFERENT attributes would resolve
    differently on every run. Ordering over every contract column plus the bronze record
    source makes the choice reproducible, and the disagreement itself is then caught by
    `retained_tuples`: dropping one of two conflicting tuples lowers that count below
    bronze's, and the build refuses.

    ONE WINDOW OVER 20,150 ROWS, and it is derived twice -- see the module docstring."""
    ordered = Window.partitionBy(fact.grain_key).orderBy(
        *(F.col(column) for column in (*payments.COLUMNS, BRONZE_RECORD_SOURCE))
    )
    return (
        source.select(
            *(F.col(column) for column in payments.COLUMNS),
            F.col(BRONZE_RECORD_SOURCE),
            F.row_number().over(ordered).alias(_DELIVERY),
        )
        .where(F.col(_DELIVERY) == 1)
        .drop(_DELIVERY)
    )


def _counterparties(source: DataFrame, fact: PaymentFact, natural_key: str) -> DataFrame:
    """Every company this fact can possibly reference, under the DIMENSION's name for the
    column -- at most `POOL_SIZE` values out of a 69.06M-key hub.

    UNIONED FROM EVERY ROLE AND NOT FROM THE PAYER ALONE, which is the same mistake the
    fact itself must not make one layer down: a reduction taken over one role would filter
    the dimension to the companies that ever paid, and every company that only ever
    RECEIVED would then resolve to the ghost -- a fact that is well-formed, half-resolved,
    and reports the shortfall as a data quality finding about the CNPJ pool."""
    return reduce(
        lambda left, right: left.unionByName(right),
        [
            source.select(F.col(counterparty).alias(natural_key))
            for counterparty, _key in fact.roles
        ],
    ).distinct()


def _versions_the_fact_can_reach(
    dimension: Scd2Dimension, natural_key: str, versions: DataFrame, counterparties: DataFrame
) -> DataFrame:
    """`dimension`'s version chain, projected to four columns and reduced to the companies
    this fact references -- the ONE place the 69.2M-row table is touched.

    A BROADCAST SEMI-JOIN AND NOT A SHUFFLE. The counterparty set is at most 1,024 values,
    so the filter rides along with the scan and the dimension is never shuffled; what comes
    out is ~1,027 rows, which the role joins below can then broadcast in turn. The bound is
    a property of the data and not a hope: one row per (company, version), over the
    companies the payment stream draws from, over the snapshots the vault holds.

    THE GHOST FALLS OUT HERE AND WAS NEVER REACHABLE ANYWAY, and the two halves of that
    sentence are one property. It carries a NULL business key, so this semi-join drops it
    for the same reason the role joins below could never have matched it: NULL never
    matches under equality. That is precisely the behaviour that made a LEFT ANTI JOIN
    invent 8,757 phantom departures in this repository's own history -- and here it is the
    DESIRED one, because the role joins are LEFT rather than ANTI, so an unresolvable
    counterparty does not vanish. It arrives with a NULL key and is coalesced onto the
    ghost's reserved value, deliberately and countably."""
    return versions.select(
        F.col(natural_key),
        F.col(dimension.surrogate_key),
        F.col(VALID_FROM),
        F.col(VALID_TO),
    ).join(F.broadcast(counterparties), on=natural_key, how="left_semi")


def _resolved(
    frame: DataFrame,
    versions: DataFrame,
    dimension: Scd2Dimension,
    natural_key: str,
    role: tuple[str, str],
) -> DataFrame:
    """`frame` with one role's foreign key set to the dimension version IN FORCE at the
    payment's own instant, or NULL where nothing was.

    HALF-OPEN, `valid_from <= t < valid_to`, AND NEVER `BETWEEN`. Inclusive at both ends,
    a payment landing exactly on a version boundary matches the version that closed there
    and the version that opened there -- a fan-out that doubles that row and every measure
    on it, produced by the operator the phase plan actually prescribed.

    THE VERSION SIDE IS RENAMED BEFORE THE JOIN rather than aliased through a frame name.
    Both sides carry a business key and the fact carries two of them, so a condition
    written against ambiguous names would resolve to whichever Spark picked -- and with two
    roles joined in sequence, the second role's condition could silently read the first
    role's already-joined column."""
    counterparty, key = role
    matched = (f"_{key}_key", f"_{key}_from", f"_{key}_to")
    side = versions.select(
        F.col(natural_key).alias(matched[0]),
        F.col(dimension.surrogate_key).alias(key),
        F.col(VALID_FROM).alias(matched[1]),
        F.col(VALID_TO).alias(matched[2]),
    )
    return frame.join(
        F.broadcast(side),
        (F.col(counterparty) == F.col(matched[0]))
        & (F.col(_EVENT_INSTANT) >= F.col(matched[1]))
        & (F.col(_EVENT_INSTANT) < F.col(matched[2])),
        how="left",
    ).drop(*matched)


def fact_rows(
    fact: PaymentFact,
    *,
    dimension: Scd2Dimension,
    hub: Hub,
    conformed: Sequence[ConformedDimension],
    source: DataFrame,
    versions: DataFrame,
    load_date: datetime,
) -> DataFrame:
    """Every row the fact will hold, in the declared column order.

    THE ORDER IS PINNED BY A TEST AND IS LOAD-BEARING, for `opl.gold.dimensions
    ._versioned`'s reason: a Delta append matches POSITIONALLY unless `mergeSchema` says
    otherwise, and this table's first five columns are all integers.

    THE THREE CONFORMED KEYS ARE DERIVED AND NOT JOINED, which `opl.gold.conformed`
    pre-decided where the two key mechanisms are chosen: a day's key IS its calendar
    position and an enumerated member's key is a hash of the member, both computable from
    the fact's own columns. `fact_surrogate_key` is the one spelling of that, shared with
    the dimension build so the two cannot drift.

    Public for `opl.gold.dimensions.dimension_rows`' reason -- the frame is worth having
    without the write, both for a test and for a reader who wants to see the projection."""
    (natural_key,) = hub.business_key_columns
    deduplicated = _deduplicated(source, fact).withColumn(
        _EVENT_INSTANT, event_instant(payments.EVENT_TIME_COLUMN)
    )
    reachable = _versions_the_fact_can_reach(
        dimension, natural_key, versions, _counterparties(source, fact, natural_key)
    )
    resolved = reduce(
        lambda frame, role: _resolved(frame, reachable, dimension, natural_key, role),
        fact.roles,
        deduplicated,
    )
    return resolved.select(
        *(
            F.coalesce(F.col(key), F.lit(GHOST_SURROGATE_KEY)).alias(key)
            for key in fact.role_keys
        ),
        *(fact_surrogate_key(item).alias(item.fact_key) for item in conformed),
        F.col(fact.grain_key),
        F.col(fact.measure).cast(AMOUNT_TYPE).alias(fact.measure),
        F.col(payments.EVENT_TIME_COLUMN),
        instant_literal(load_date).alias(LOAD_DATE),
        F.col(BRONZE_RECORD_SOURCE).alias(RECORD_SOURCE),
    )


def _refuse_a_deduplication_that_lost_a_business_tuple(
    fact: PaymentFact, *, source_tuples: int, retained_tuples: int
) -> None:
    """Refuse a deduplication that changed WHAT THE PAYMENTS WERE.

    A redelivery is byte-identical to its original, so removing it removes no tuple: this
    number must be equal, exactly, and a shortfall is one `transaction_id` delivered twice
    with DIFFERENT business attributes -- which is not a redelivery at all but a corrupt
    stream, and which no resolution rate and no row count can see. It is checked before the
    first write because there is nothing to keep."""
    if retained_tuples != source_tuples:
        raise ValueError(
            f"refusing to build {fact.name!r}: bronze holds {source_tuples} distinct "
            f"business-attribute tuples and the deduplicated payments hold "
            f"{retained_tuples}. Deduplication is by {fact.grain_key!r} and a redelivery "
            "is byte-identical to its original, so this cannot change -- a shortfall means "
            "one identity was delivered twice carrying DIFFERENT attributes, and the copy "
            "that survived was chosen by a sort. Nothing has been written by this run"
        )


def _refuse_a_row_count_that_is_not_one_per_delivered_identity(
    fact: PaymentFact, *, target_table: str, source_table: str, held: int, identities: int
) -> None:
    """Refuse a fact that is not exactly one row per distinct `grain_key` in bronze. THE
    GRAIN, ENFORCED INSTEAD OF OBSERVED.

    ONE NUMBER COVERING THE TWO FAILURES THAT NOTHING ELSE HERE SEES, and they push it in
    opposite directions. TOO HIGH is a MULTI-MATCH: a payment resolving to two versions of
    one company, which `BETWEEN` produces at a version boundary and a broken version chain
    produces everywhere. A fan-out does NOT lower the resolution rate -- both matches
    resolve -- so the count is the only measurement that can see it. TOO LOW is a
    deduplication taken over the wrong columns: on today's bronze, over the business
    attributes, that is 18,400 rows against 20,000 identities and 1,600 real payments
    deleted.

    CHECKED ON THE COUNT HELD AND NOT ON THE COUNT APPENDED, for `opl.gold.dimensions
    ._refuse_a_count_that_is_not_every_version_plus_the_ghost`'s reason: it is an invariant
    of every state this loader accepts, the idempotent re-run included, and the re-run is
    the branch a retry lands in (`max_retries: 0` does not prevent one).

    AFTER THE WRITE, so the message says the rows are on disk."""
    if held == identities:
        return
    diagnosis = (
        "a payment resolving to MORE THAN ONE dimension version -- a multi-match, which "
        "a resolution rate cannot see because both matches resolve -- or, on a re-run, "
        "payments this table holds that bronze no longer delivers"
        if held > identities
        else "payments deleted by a deduplication taken over something other than "
        f"{fact.grain_key!r}. Over the business attributes it is the LEGITIMATE REPEATS "
        "that go, which are real payments the stream emits on purpose"
    )
    raise ValueError(
        f"refusing to accept {target_table}: it holds {held} rows and {source_table} "
        f"holds {identities} distinct {fact.grain_key} values. The grain of this fact is "
        f"ONE ROW PER PAYMENT EVENT, so the difference is {diagnosis}. THE TABLE ON DISK "
        "IS ALREADY WRITTEN and must be dropped before a repaired build"
    )


def _unresolved_per_role(written: DataFrame, fact: PaymentFact) -> tuple[tuple[str, int], ...]:
    """Rows resolving to the ghost, PER ROLE -- the (row, role) grain the acceptance is
    measured at.

    ONE AGGREGATE FOR EVERY ROLE, over the written table. Per role and never summed,
    because the failure this exists to catch is asymmetric: a fact that resolved the payer
    and not the payee reports half of `2 x rows` and reads as a 50% data-quality problem
    rather than as a missing foreign key."""
    counted = written.agg(
        *(
            F.count(F.when(F.col(key) == GHOST_SURROGATE_KEY, 1)).alias(key)
            for key in fact.role_keys
        )
    ).collect()[0]
    return tuple((key, counted[key]) for key in fact.role_keys)


def _orphaned_per_conformed_dimension(
    spark: SparkSession,
    written: DataFrame,
    conformed: Sequence[ConformedDimension],
    conformed_tables: Mapping[str, str],
) -> tuple[tuple[str, int], ...]:
    """Rows whose DERIVED conformed key matches no member of the dimension it names.

    THE PRICE OF NOT JOINING, PAID BY COUNTING. The three conformed keys are computed from
    the fact's own columns, so there is no lookup to coalesce onto a ghost: a payment whose
    day falls outside the span `dim_date` was built over gets a key no row holds. That is a
    legitimate state of a correct fact built at the wrong moment -- the repair is to re-run
    the conformed build, which APPENDS -- so it is reported and not refused.

    KEYED BY DIMENSION NAME, which is what stops two tables being swapped: the keys are
    named after the dimensions, so a positional pairing that transposed them would compare
    each fact key against another dimension's members and report every row as an orphan.

    A BROADCAST ANTI-JOIN PER DIMENSION, against 51, 5 and 1 members: three passes over the
    fact and no shuffle."""
    return tuple(
        (
            item.name,
            written.join(
                F.broadcast(
                    spark.read.table(conformed_tables[item.name]).select(
                        F.col(item.surrogate_key).alias(item.fact_key)
                    )
                ),
                on=item.fact_key,
                how="left_anti",
            ).count(),
        )
        for item in conformed
    )


def _new_rows(
    spark: SparkSession, fact: PaymentFact, rows: DataFrame, target: str
) -> DataFrame:
    """`rows` minus what the target already holds, by `grain_key`.

    AN ANTI-JOIN AND NOT A REFUSAL, which is `opl.gold.conformed._new_rows`' argument
    applied to a fact. `opl.gold.dimensions` must STOP when its source grows, because an
    append cannot revise an interval it already closed; nothing here has an interval. A
    payment batch that landed after the last build is a set of new `transaction_id` values
    to APPEND, and the surrogate keys of the rows already written cannot move -- they are
    hashes over a version's own `applied_date`, and the versions are the same versions.

    NULL-SAFE BY THE CONTRACT AND CAUGHT BY THE COUNT IF IT IS NOT. `transaction_id` is a
    required column that the DQ gate rejects blank in both spellings, so a plain equality
    anti-join is correct; and were one ever NULL it would fail to match here and be
    re-appended, which raises the row count above the identity count and refuses on the
    next run rather than accumulating quietly.

    IT IS THE ONE STEP ONLY A RE-RUN PAYS: a shuffle of the derived rows against a target
    of the same size. A first build skips it entirely."""
    existing = spark.read.table(target).select(fact.grain_key)
    return rows.join(existing, on=fact.grain_key, how="left_anti")


def _appended(
    spark: SparkSession, fact: PaymentFact, rows: DataFrame, target_table: str
) -> int:
    """Append `rows`, and return what the target held BEFORE.

    The count is taken first because `appended` must be what LANDED rather than what was
    planned -- `opl.vault.loading.rows_in`'s own reason -- and it is free, since Delta
    answers a whole-table count from the transaction log's file statistics."""
    before = rows_in(spark, target_table)
    if before:
        rows = _new_rows(spark, fact, rows, target_table)
    rows.write.format("delta").mode("append").saveAsTable(target_table)
    return before


def load_fact(
    spark: SparkSession,
    fact: PaymentFact,
    *,
    dimension: Scd2Dimension,
    hub: Hub,
    conformed: Sequence[ConformedDimension],
    source_table: str,
    dimension_table: str,
    conformed_tables: Mapping[str, str],
    target_table: str,
    load_date: datetime,
) -> FactLoadResult:
    """Build `fact_payment` from `source_table`'s payments and append it -- one row per
    payment event, both counterparties resolved as of that payment's own `event_time`.

    `load_date` is an argument with no default, for `opl.vault.hubs.load_hub`'s reason: a
    loader that stamps its own clock cannot be asserted against. Idempotent by `grain_key`:
    a re-run over an unchanged source appends nothing and reports 0, a source that GAINED a
    payment batch appends that batch alone, and the grain check holds in every one of those
    states."""
    _refuse_a_mismatched_source(fact, dimension, hub, conformed)
    source, source_tuples, retained_tuples = _bronze(spark, fact, source_table)
    rows = fact_rows(
        fact, dimension=dimension, hub=hub, conformed=conformed, source=source,
        versions=spark.read.table(dimension_table), load_date=load_date,
    )
    before = _appended(spark, fact, rows, target_table)
    held = rows_in(spark, target_table)
    identities = source.select(fact.grain_key).distinct().count()
    _refuse_a_row_count_that_is_not_one_per_delivered_identity(
        fact, target_table=target_table, source_table=source_table,
        held=held, identities=identities,
    )
    written = spark.read.table(target_table)
    return FactLoadResult(
        table=target_table,
        appended=held - before,
        already_present=before,
        source_rows=rows_in(spark, source_table),
        source_identities=identities,
        source_tuples=source_tuples,
        retained_tuples=retained_tuples,
        legitimate_repeats=held - retained_tuples,
        unresolved=_unresolved_per_role(written, fact),
        orphaned=_orphaned_per_conformed_dimension(
            spark, written, conformed, conformed_tables
        ),
    )
