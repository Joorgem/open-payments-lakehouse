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
id, an identical business-attribute tuple). All FOUR promoted streams carry
`_REPEAT_COUNT = 800` repeats on purpose, so a fact that deduplicated on the attributes --
or on a "natural key" built from (payer, payee, amount, currency, payment_method), which
is the obvious thing to reach for -- deletes 3,200 real payments and returns a plausible
36,800. `opl.gold.fact_spec._assert_the_grain_is_the_events_own_identity` refuses that at
declaration; `_refuse_a_row_count_that_is_not_one_per_delivered_identity` refuses it on
the written table.

AND THE 150-DUPLICATE ACCEPTANCE IS A TAUTOLOGY, WHICH IS WHY IT IS NOT USED AS ONE. "The
build removed 150 duplicates" is `COUNT(*) - COUNT(DISTINCT <grain>)` BY DEFINITION OF THE
OPERATION: it re-measures bronze's own arithmetic and comes out right whichever column the
deduplication was taken over. What is published instead is `legitimate_repeats` -- rows the
fact holds beyond its distinct business tuples -- 3,200 when the repeats survived, 0 if not.

--- THE FX COLUMNS, AND THE ONE JOIN IN THIS LOADER THAT CANNOT BROADCAST ON A BOUND ---

`fx_rate`, `fx_rate_date_key` and `amount_brl` are resolved by `opl.gold.fx`, which owns
every argument about them: the rate is the most recent quote whose PUBLICATION INSTANT
precedes the payment's own (a day-grain join would hand a Sunday payment Monday's rate), the
FX side is reduced to one row per `(currency, quote_date)` BEFORE the join because
`bronze_ptax` appends and a re-run would otherwise double every USD row, and two landed rows
that disagree on a rate are REFUSED rather than resolved. What belongs in THIS file is where
the refusal sits relative to the write: `refuse_payments_no_rate_can_be_resolved` runs over
the DERIVED frame before `_appended`, because a NULL rate is a NULL `amount_brl` and there
would be nothing to keep.

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
the whole fact under `America/Sao_Paulo` and asserts every resolved version and every date
key is unchanged.

AND THAT TEST CAUGHT THE FX COLUMN, WHICH IS WORTH RECORDING BECAUSE IT WAS THE FIRST DRAFT.
A reporting-currency payment's `fx_rate_date` was written `to_date(<event instant>)` -- a
RENDERING -- so under America/Sao_Paulo every midnight-UTC payment dated its identity
conversion to the previous day and `fx_rate_date_key` became a function of a cluster setting.
It is `day_of(event_time)` now, the ten characters `event_date_key` is read from. The hazard
is met by EVERY new column answering "which day" and by every instant this layer REPORTS
(`opl.gold.fx._as_instant`), and the only defence that has worked is a test that SETS the
wrong zone rather than inheriting it.

--- WHERE THE EXPENSIVE FRAME IS CONSUMED, ONCE PER ROLE AND NEVER MORE -------------

`dim_company` is 69,202,818 rows and it is the only expensive thing this loader touches.
No `persist()`, no `cache()` -- serverless refuses both (master protocol section 4.3) and
an AST sweep catches them -- so the reduction is structural instead:

  1. `_counterparties` reads the 40,150-row bronze table and produces the <=1,024 distinct
     CNPJs this fact can possibly reference.
  2. `_versions_the_fact_can_reach` scans `dim_company` ONCE per role, projected to four
     columns, and filters it by a BROADCAST semi-join against that key set before anything
     is shuffled. What survives is ~1,027 rows -- 1,024 companies, three of which carry two
     versions (`docs/f3-run-evidence.md` section 0.5, P6 falsified at 3).
  3. Each role join then broadcasts THAT, so the as-of comparison is a broadcast hash join
     on the business key with the interval as a residual predicate -- no shuffle of the
     dimension anywhere in this loader.

The alternative -- joining 80,000 references straight into 69.2M rows -- cannot broadcast
either side (the build side of a LEFT OUTER join is the right side, whichever way the join is
spelled), so it is a sort-merge join shuffling 69.2M rows per role.

THE FX SIDE IS THE SECOND BROADCAST AND ITS BOUND IS DIFFERENT IN KIND. `opl.gold.fx`
reduces `bronze_ptax` to one row per `(currency, quote_date)` -- 42 rows over this phase's
extraction window -- so what is broadcast is small because the REDUCE made it small, not
because the source was. That distinction is the whole of T4c and `opl.gold.fx`'s docstring
carries it.

WHAT *IS* DERIVED TWICE, SAID PLAINLY: the deduplication, and now the FX projection. The
first runs once into the write and once for `retained_tuples`; the second is re-derived by
`opl.gold.fx.coverage`, which takes the NULL rate count the pre-write refusal reads and the
two window numbers the run log reports in ONE aggregate before `_appended` consumes the same
frame. Both are 40,150 rows through one window and three broadcast joins. That is the cheap
frame; the 69.2M-row dimension is the expensive one, read once per ROLE because two roles
need two independent answers.

--- WHAT IS MEASURED AND WHAT IS REFUSED --------------------------------------------

Refused BEFORE the first write: a payment whose `event_time` carries no readable instant or
whose DELIVERED measure is not a number (both would resolve to the ghost or to NULL with the
build reporting success), a deduplication that lost a business tuple, a landed PTAX quote
date that does not reduce to one rate, and a payment for which no quote had been published
yet -- the DERIVED measure's own refusal, which the delivered measure's cannot cover because
that one reads bronze before anything is derived.

Refused AFTER it, in the shape `opl.gold.dimensions` uses and with the same closing
sentence: a row count that is not one row per delivered identity. That single number covers
BOTH remaining failures -- a fan-out raises it, a deduplication taken over the wrong
columns lowers it -- and neither is visible in a resolution rate.

REPORTED and never refused: rows resolving to the ghost, per role; rows whose derived
conformed key matches no member of the dimension it names; and the two numbers that say how
far the PTAX series reached past the payments -- conversions that took its last landed quote,
and the widest fallback taken. The first two are legitimate states of a correct fact built at
the wrong moment, whose repair is to re-run the conformed build, which APPENDS. The last two
are not refusable at all without the calendar T3 rejects (`opl.gold.fx.coverage`), and a
truncated extraction is invisible in every other number this loader prints.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
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
from opl.gold.conformed import day_of, fact_surrogate_key

# ONE TIMESTAMP PATH FOR THE WHOLE LAYER, which is the only reason this module imports
# anything of the SCD2 loader -- `opl.gold.conformed` imports it for the same sentence.
# `F.lit(datetime)` converts through `time.mktime`, i.e. the DRIVER's operating-system
# zone, where every other instant gold writes is parsed by Spark in the SESSION zone.
from opl.gold.dimensions import instant_literal

# THE REFUSALS AND THE TWO READ DECISIONS LIVE NEXT DOOR AND ARE CALLED FROM HERE, in
# `opl.bronze.registry`'s shape: the guards moved to their own module at the cap, and the
# three names this file RE-EXPORTS are re-exported so that no consumer's import line moved
# when they did. `opl.gold.fact_guards` states where each refusal sits relative to the write.
from opl.gold.fact_guards import (  # noqa: F401  (re-exported for consumers)
    _CURRENCY_COLUMN,
    AMOUNT_TYPE,
    ISO_INSTANT_FORMAT,
    _refuse_a_deduplication_that_lost_a_business_tuple,
    _refuse_a_derived_role_this_loader_cannot_produce,
    _refuse_a_fact_whose_measures_this_loader_cannot_derive,
    _refuse_a_mismatched_source,
    _refuse_a_row_count_that_is_not_one_per_delivered_identity,
    _refuse_payments_no_instant_can_be_read,
    _refuse_unresolved_rates,
    event_instant,
)
from opl.gold.fx import (
    AMOUNT_BRL,
    FX_RATE,
    FxCoverage,
    FxSeries,
    converted_amount,
    coverage,
    rate_intervals,
    with_resolved_rates,
)
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
_DELIVERY = "_delivery"

# Internal to the as-of join: the payment's instant, derived once and compared THREE times
# now -- once per counterparty role and once against the FX series' publication intervals.
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
    # Rows whose DERIVED conformed key matches no member of the dimension it names, PER FACT
    # KEY COLUMN and not per dimension -- `dim_date` answers two of them since F-API T4b, and
    # a total over the pair would say "dim_date: 12" without saying which date was outside
    # the calendar. The price of deriving four keys without a join, paid by counting.
    orphaned: tuple[tuple[str, int], ...]
    # How many `(currency, quote_date)` quotes the reduced PTAX series held, and the first and
    # last publication instant in it -- AWARE UTC, so the run log does not render them in
    # whatever zone the driver's operating system is on (`opl.gold.fx._as_instant`). REPORTED
    # and never refused: gaplessness in business days is not asserted (`opl.gold.fx` says
    # why), so these three numbers are what make a BOUNDED extraction visible in the run log
    # instead of hiding behind a rate that resolved.
    fx_quotes: int
    fx_first_published: datetime
    fx_last_published: datetime
    # AND THE OTHER SIDE OF THAT COMPARISON, because the three above describe the SERIES and
    # nothing here described the PAYMENT WINDOW they are checked against. Converted payments
    # that took the series' LAST landed quote -- nothing later was landed, so the rate may be
    # stale and the series cannot say -- and the widest fallback any conversion took, which is
    # what makes an INTERIOR hole visible when the high end reads zero. REPORTED for
    # `opl.gold.fx.coverage`'s reason: a bound on either is a calendar or a guess.
    fx_beyond_series: int
    fx_widest_fallback_days: int
    # Distinct `fx_rate` values the fact carries, from the WRITTEN table. MORE THAN ONE is the
    # point and ONE the failure (`<= 1` branches). This read "TWO IS THE POINT" -- a count of
    # QUOTES; the fact carries THREE: 1.00000 (the reporting currency), 5.14420, 5.13950.
    fx_rates_used: int


def _measured_source(spark: SparkSession, fact: PaymentFact, source_table: str) -> DataFrame:
    """The bronze payments, with the two unreadable-value counts already refused.

    ONE AGGREGATE FOR BOTH COUNTS, over the 40,150-row source and before anything is derived
    -- so the refusal happens before the first write rather than after it, which is the
    opposite of where the post-write checks sit and is affordable for exactly the reason they
    are not: this table is small and the dimension is not.

    `fact.measure` IS READ OFF BRONZE HERE, WHICH IS WHY IT MUST STAY THE DELIVERED COLUMN. A
    derived name in that field -- `amount_brl`, the obvious thing to reach for once the star
    has two currencies -- is not a guard's message but an `AnalysisException` on a column
    bronze does not have, raised after a session has started. `opl.gold.fact_spec` records
    that as the reason `amount_brl` is a `DerivedMeasure`, and this line is the reason."""
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

    BY THE GRAIN KEY AND BY NOTHING ELSE -- the module docstring argues the 3,200 payments
    the alternative deletes.

    `row_number()` AND NOT `dropDuplicates`, AND THE DIFFERENCE IS DETERMINISM. A redelivery
    is byte-identical to the delivery it repeats (`opl.generator.defects`: "the same bytes
    again"), so which copy survives cannot matter -- but that is a property of the data and
    not of the operator, and `dropDuplicates` picks by partition assignment, so a stream that
    ever delivered one id twice with DIFFERENT attributes would resolve differently on every
    run. Ordering over every contract column plus the bronze record source makes the choice
    reproducible, and the disagreement is then caught by `retained_tuples`: dropping one of
    two conflicting tuples lowers that count below bronze's, and the build refuses.

    ONE WINDOW OVER 40,150 ROWS, and it is derived twice -- see the module docstring."""
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
    sentence are one property. It carries a NULL business key, so this semi-join drops it for
    the same reason the role joins below could never have matched it: NULL never matches under
    equality. That is precisely the behaviour that made a LEFT ANTI JOIN invent 8,757 phantom
    departures in this repository's own history -- and here it is the DESIRED one, because the
    role joins are LEFT rather than ANTI, so an unresolvable counterparty does not vanish: it
    arrives with a NULL key and is coalesced onto the ghost's value, deliberately."""
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

    HALF-OPEN, `valid_from <= t < valid_to`, AND NEVER `BETWEEN`. Inclusive at both ends, a
    payment landing exactly on a version boundary matches the version that closed there and
    the version that opened there -- a fan-out that doubles that row and every measure on it,
    produced by the operator the phase plan actually prescribed.

    THE VERSION SIDE IS RENAMED BEFORE THE JOIN rather than aliased through a frame name. Both
    sides carry a business key and the fact carries two of them, so a condition written
    against ambiguous names would resolve to whichever Spark picked -- and with two roles
    joined in sequence, the second role's condition could read the first role's column."""
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


def _projected(
    converted: DataFrame,
    fact: PaymentFact,
    conformed: Sequence[ConformedDimension],
    load_date: datetime,
) -> DataFrame:
    """The fact's columns, in the declared order, out of the fully derived frame.

    THE ORDER IS PINNED BY A TEST AND IS LOAD-BEARING, for `opl.gold.dimensions
    ._versioned`'s reason: a Delta append matches POSITIONALLY unless `mergeSchema` says
    otherwise, and this table's first six columns are all integers.

    THE FOUR CONFORMED KEYS ARE DERIVED AND NOT JOINED, which `opl.gold.conformed`
    pre-decided where the two key mechanisms are chosen: a day's key IS its calendar
    position and an enumerated member's key is a hash of the member, both computable from
    the fact's own columns. `fact_surrogate_key` is the one spelling of that, shared with
    the dimension build so the two cannot drift. FOUR AND NOT THREE because `dim_date`
    answers two roles since F-API T4b, which is why the comprehension is over
    `item.fact_roles` and not over `conformed`.

    AND `fx_rate_date` IS READ HERE AND NEVER PROJECTED. The star carries `fx_rate_date_key`
    and no bare date column -- one of THREE deviations from master spec §4.3's column list,
    with `amount_original` satisfied by a mapping onto `amount` and `currency` carried only
    as `currency_key`. All three are recorded in ADR 0016; the third was found by reading the
    REBUILT TABLE's schema, not the code (`docs/f-api-run-evidence.md` §2.9)."""
    return converted.select(
        *(
            F.coalesce(F.col(key), F.lit(GHOST_SURROGATE_KEY)).alias(key)
            for key in fact.role_keys
        ),
        *(
            fact_surrogate_key(item, role).alias(role.key)
            for item in conformed
            for role in item.fact_roles
        ),
        F.col(fact.grain_key),
        F.col(fact.measure).cast(AMOUNT_TYPE).alias(fact.measure),
        F.col(FX_RATE),
        converted_amount(amount_column=fact.measure, amount_type=AMOUNT_TYPE).alias(AMOUNT_BRL),
        F.col(payments.EVENT_TIME_COLUMN),
        instant_literal(load_date).alias(LOAD_DATE),
        F.col(BRONZE_RECORD_SOURCE).alias(RECORD_SOURCE),
    )


# THE DAY IS `day_of` AND NEVER `to_date(<instant>)` IN BOTH CALLS BELOW, which is the same
# distinction `event_instant`'s docstring draws about the same column: the INSTANT does not move
# with the session zone and a DAY rendered from it does. It is the day a reporting-currency
# payment's identity conversion is dated, and it must be as stable as `event_date_key`. Here
# rather than in the docstring for the 50-line cap's reason, as `opl.gold.fx` does twice.
#
# AND THE COVERAGE IS MEASURED BEFORE THE PROJECTION, WHICH IS WHY THIS RETURNS A PAIR.
# `_projected` drops `fx_rate_date` (the star carries only its key) and never carried
# `currency` (it carries `currency_key`) -- and those are the two columns the widest fallback
# and the quote-consulting population are read from. Measuring off the written table would
# mean recovering both through `dim_currency` and a date-key parse, for numbers in hand here.


def fact_rows(
    fact: PaymentFact,
    *,
    dimension: Scd2Dimension,
    hub: Hub,
    conformed: Sequence[ConformedDimension],
    source: DataFrame,
    versions: DataFrame,
    series: FxSeries,
    load_date: datetime,
) -> tuple[DataFrame, FxCoverage]:
    """Every row the fact will hold -- deduplicate, resolve both roles as of the payment's own
    instant, resolve the rate as of it too, then project -- AND what those rows say about the
    series they resolved against (the comment block above says why that is one function).

    THE FX COLUMNS ARE JOINED AND *THEN* DERIVED, WHICH IS THE ONE ASYMMETRY HERE. The rate
    comes from a half-open interval join against the reduced series (`opl.gold.fx`), because
    which quote applies is a fact about two instants and nothing in the payment row can
    compute it; `amount_brl` is then pure arithmetic over two columns of the same row. The
    same `_EVENT_INSTANT` is the left side of all three joins, derived once.

    NOTHING HERE REFUSES -- `load_fact` reads `unresolved` and refuses with it -- so the frame
    is still worth having without the write, which is `dimension_rows`' reason for public."""
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
    converted = with_resolved_rates(
        resolved,
        series.intervals,
        instant=F.col(_EVENT_INSTANT),
        day=day_of(payments.EVENT_TIME_COLUMN),
        currency_column=_CURRENCY_COLUMN,
        reporting_currency=payments.REPORTING_CURRENCY,
    )
    measured = coverage(
        converted, instant=F.col(_EVENT_INSTANT), currency_column=_CURRENCY_COLUMN,
        day=day_of(payments.EVENT_TIME_COLUMN), last_published=series.last_published,
        reporting_currency=payments.REPORTING_CURRENCY,
    )
    return _projected(converted, fact, conformed, load_date), measured


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


def _conformed_members(
    spark: SparkSession,
    conformed: Sequence[ConformedDimension],
    conformed_tables: Mapping[str, str],
) -> tuple[tuple[str, DataFrame], ...]:
    """Each conformed dimension's ROLE paired with that dimension's surrogate keys, aliased
    to the column the FACT spells them in -- resolved BEFORE the fact is written.

    ONE ENTRY PER ROLE AND NOT PER DIMENSION SINCE F-API T4b, which is what makes the orphan
    count total over the projection: `dim_date` answers `event_date_key` and
    `fx_rate_date_key`, so its member frame is read once and aliased twice. The two frames are
    the same table read under two names -- three catalog lookups became four, and still no
    scan.

    THE READ IS THE PROBE, AND ITS POSITION IS THE WHOLE POINT OF THE FUNCTION.
    `spark.read.table` and the `select` under it are ANALYSED EAGERLY, so a conformed table
    that does not exist -- or that exists without its surrogate-key column -- raises here.
    Called from `load_fact` before the append, it makes `gold_fact_payment_job.yml`'s "a
    missing table fails the read" true of every input that file names. Resolved where the
    orphan count needed it, which is where it used to be, that sentence was true of
    `dim_company` alone: run before the conformed job, the fact was FULLY WRITTEN and the task
    then failed table-not-found, so an operator met a FAILED run against a correct table and
    the obvious repair -- drop it -- was the wrong one.

    MOVING IT COSTS NOTHING: four catalog lookups and no scan, and the frames stay lazy until
    the anti-join below consumes them after the write.

    KEYED BY DIMENSION NAME, which is what stops two tables being swapped: the tables
    arrive as a MAPPING and the lookup is by `item.name`, so a positional pairing that
    transposed two of them is not expressible here."""
    return tuple(
        (
            role.key,
            spark.read.table(conformed_tables[item.name]).select(
                F.col(item.surrogate_key).alias(role.key)
            ),
        )
        for item in conformed
        for role in item.fact_roles
    )


def _orphaned_per_fact_key(
    written: DataFrame, members: Sequence[tuple[str, DataFrame]]
) -> tuple[tuple[str, int], ...]:
    """Rows whose DERIVED conformed key matches no member of the dimension it names, PER FACT
    KEY COLUMN.

    THE PRICE OF NOT JOINING, PAID BY COUNTING. The four conformed keys are computed from
    the fact's own columns, so there is no lookup to coalesce onto a ghost: a payment whose
    day falls outside the span `dim_date` was built over gets a key no row holds. That is a
    legitimate state of a correct fact built at the wrong moment -- the repair is to re-run
    the conformed build, which APPENDS -- so it is reported and not refused.

    PER KEY AND NOT PER DIMENSION, WHICH IS THE RENAME F-API T4b FORCED AND NOT A COSMETIC
    ONE. `dim_date` answers two of the fact's columns, and the failure the derived role can
    have is asymmetric: a payment whose EVENT day is outside the calendar and a payment whose
    FX quote date is outside it are different defects with different repairs, and a total
    labelled `dim_date` would name neither. The keys are distinct across the whole fact --
    `opl.gold.registry_guards._assert_no_two_columns_of_one_fact_share_a_name` refuses
    otherwise -- so the label is unambiguous.

    KEYED BY THE FACT'S OWN COLUMN, and `_conformed_members` above is where that keying
    happens: a positional pairing that transposed two tables would compare each fact key
    against another dimension's members and report every row as an orphan.

    A BROADCAST ANTI-JOIN PER KEY, against 51, 51, 6 and 3 ROWS -- four passes over the fact
    and no shuffle, two of them over the same broadcast `dim_date`. Those are ROWS and not the
    50, 50, 5 and 2 MEMBERS `gold_conformed_dimensions_job.yml` predicts, because each whole
    table is broadcast and each carries its ghost. Nothing derived can equal
    `GHOST_SURROGATE_KEY`, so the ghost changes no count -- but mixing the two units is how a
    reader reconciles against the wrong side."""
    return tuple(
        (key, written.join(F.broadcast(frame), on=key, how="left_anti").count())
        for key, frame in members
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
    re-appended, raising the row count above the identity count and refusing on the next run.

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
    fx_source_table: str,
    target_table: str,
    load_date: datetime,
) -> FactLoadResult:
    """Build `fact_payment` from `source_table`'s payments and append it -- one row per
    payment event, both counterparties resolved as of that payment's own `event_time`.

    `load_date` is an argument with no default, for `opl.vault.hubs.load_hub`'s reason: a
    loader that stamps its own clock cannot be asserted against. Idempotent by `grain_key`: a
    re-run over an unchanged source appends nothing, a source that GAINED a payment batch
    appends that batch alone, and the grain check holds in every one of those states.

    `fx_source_table` IS `bronze_ptax`, AND `rate_intervals` REDUCES AND REFUSES IT BEFORE
    `fact_rows` BUILDS ANYTHING -- the fan-out an unreduced FX join causes is visible only to
    a check whose own message begins "THE TABLE ON DISK IS ALREADY WRITTEN"."""
    _refuse_a_mismatched_source(fact, dimension, hub, conformed)
    _refuse_a_fact_whose_measures_this_loader_cannot_derive(fact)
    _refuse_a_derived_role_this_loader_cannot_produce(fact, conformed)
    members = _conformed_members(spark, conformed, conformed_tables)
    series = rate_intervals(spark.read.table(fx_source_table))
    source, source_tuples, retained_tuples = _bronze(spark, fact, source_table)
    rows, measured = fact_rows(
        fact, dimension=dimension, hub=hub, conformed=conformed, source=source,
        versions=spark.read.table(dimension_table), series=series,
        load_date=load_date,
    )
    _refuse_unresolved_rates(fact, unresolved=measured.unresolved)
    before = _appended(spark, fact, rows, target_table)
    held = rows_in(spark, target_table)
    identities = source.select(fact.grain_key).distinct().count()
    _refuse_a_row_count_that_is_not_one_per_delivered_identity(
        fact, target_table=target_table, source_table=source_table,
        held=held, identities=identities,
    )
    return _reconciled(
        spark, fact, series, measured, members,
        source_table=source_table, target_table=target_table,
        before=before, held=held, identities=identities,
        source_tuples=source_tuples, retained_tuples=retained_tuples,
    )


def _reconciled(
    spark: SparkSession,
    fact: PaymentFact,
    series: FxSeries,
    measured: FxCoverage,
    members: Sequence[tuple[str, DataFrame]],
    *,
    source_table: str,
    target_table: str,
    before: int,
    held: int,
    identities: int,
    source_tuples: int,
    retained_tuples: int,
) -> FactLoadResult:
    """Everything the build REPORTS, measured off the written table after the grain has been
    enforced.

    SPLIT OUT OF `load_fact` WHEN THE FX NUMBERS TOOK IT PAST THE 50-LINE CAP, on the seam
    that module's docstring already names: `load_fact` reads as check, derive, append,
    reconcile, and this is the fourth. Nothing here can fail -- every refusal has already
    run -- which is exactly why it is separable."""
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
        orphaned=_orphaned_per_fact_key(written, members),
        fx_quotes=series.quotes,
        fx_first_published=series.first_published,
        fx_last_published=series.last_published,
        fx_beyond_series=measured.beyond_series,
        fx_widest_fallback_days=measured.widest_fallback_days,
        fx_rates_used=written.select(FX_RATE).distinct().count(),
    )
