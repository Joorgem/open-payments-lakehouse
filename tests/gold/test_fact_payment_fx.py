"""The FX columns: the rate a payment converted at, the quote date it came from, and the
amount in the reporting currency -- and the one property no calendar-day implementation can
produce.

WHY THIS IS A FILE OF ITS OWN. `test_fact_payment.py` is 747 lines of the project's 800-line
cap, and its whole fixture is five BRL payments over three instants -- the right fixture for an
as-of join into `dim_company` and the wrong one for FX. A BRL-only population converts at
exactly 1 on every row and `amount_brl` equals `amount` everywhere, so an assertion over
either column is a statement about the FIXTURE rather than about the conversion: it would hold
under an implementation that wrote `fx_rate = 1` unconditionally. That is verbatim the state
F-API's T1 exists to end, so a test of the FX layer has to reach a population with two
currencies in it.

--- THE TWO POPULATIONS THIS FILE MEASURES, AND WHY BOTH ARE NEEDED -------------------

THE FIFTH PROFILE'S REAL 10,000 ROWS, WITHOUT A VAULT. `test_the_resolution_reproduces_the
_published_fx_split` generates `cross-currency` from its own declaration and runs
`opl.gold.fx` over it -- no `dim_company`, no conformed tables, no 69.2M-row satellite. That
is what makes it affordable, and it is also what makes it the sharper test: the numbers it is
marked against (2,864 and 2,041) were PUBLISHED in `docs/f-api-run-evidence.md` §1.1 before
this code existed, derived from the window's arithmetic and the measured publication instant.
`tests/test_payment_profiles.py` already pins them against a Python-side comparison of the
same rule; what this file adds is that the SPARK implementation reproduces them, which is the
only version of the claim that is about the code that will run in the workspace.

THE WHOLE STAR, THROUGH THE REAL LOADER, on the small fixture. That is where the schema, the
types, the projection and the derived date key are asserted -- properties of a build rather
than of an arithmetic, and ones the 10,000-row frame is not built through a loader to have.

--- WHAT WOULD PASS WITHOUT THE RULE, WHICH IS THE ONLY REASON THIS FILE IS WORTH ITS RUNTIME

A CALENDAR-DAY JOIN PASSES EVERY OTHER TEST IN THIS REPOSITORY. Every payment in the four
earlier streams falls on a Saturday with no quote, so a day-grain implementation falls back
exactly as the instant rule does and agrees with it on all 30,000 rows. The fifth profile is
the one population where the two answers differ, and they differ WITHIN one calendar day:
5,836 events precede the 2026-06-22 bulletin and 4,164 follow it, so a day join gives all
10,000 the same rate and this file's last assertion is what refuses that.
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from opl.config import SESSION_TIMEZONE, SESSION_TIMEZONE_CONFIG
from opl.contracts import payments
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import delivered_records
from opl.generator.profiles import CROSS_CURRENCY, POOL_SIZE, PROFILES
from opl.gold.conformed import day_of
from opl.gold.fact_guards import AMOUNT_TYPE, event_instant
from opl.gold.fx import (
    _FX_FROM,
    _FX_QUOTE_DATE,
    _FX_TO,
    AMOUNT_BRL,
    FX_RATE,
    FX_RATE_DATE,
    FX_RATE_TYPE,
    converted_amount,
    rate_intervals,
    with_resolved_rates,
)
from opl.gold.registry import DIM_CURRENCY, FACT_PAYMENT

from .conftest import (
    AFTER,
    BEFORE,
    PAYMENTS_SCHEMA,
    build_fact,
    payment,
    ptax_table,
)

# THE TWO RATES THE FIFTH PROFILE RESOLVES TO, MEASURED AND NOT CHOSEN.
# `docs/f-api-run-evidence.md` §0.2 and §1.2 carry the live requests that produced them:
# 2026-06-19 venda 5.14420 published 13:03:25.555497, 2026-06-22 venda 5.13950 published
# 13:06:19.750415. Both stamps are read as BRT (T3), which is what puts the boundary at event
# index 5,836 rather than 3,676.
FRIDAY = date(2026, 6, 19)
MONDAY = date(2026, 6, 22)
FRIDAY_VENDA = Decimal("5.14420")
MONDAY_VENDA = Decimal("5.13950")
UNIT_RATE = Decimal("1.00000")

# 13:03:25.555497 READ AS BRT (T3) IS 16:03:25.555497Z, and this is the whole instant rather
# than its microseconds. `opl.gold.fx` reports aware UTC instants built from `unix_micros`, so
# this literal is comparable on any driver and under any session zone -- which is what makes
# the invariance assertion below writable at all.
FRIDAY_PUBLISHED = datetime(2026, 6, 19, 16, 3, 25, 555_497, tzinfo=UTC)

# The published FX-resolving populations -- `docs/f-api-run-evidence.md` §1.1. These are the
# USD rows either side of the bulletin, and NOT the 5,836 / 4,164 ROW split: a BRL row
# consults no quote at all, so it is in neither.
FELL_BACK = 2_864
SAME_DAY = 2_041
BRL_ROWS = 5_095
USD_ROWS = 4_905


def _cross_currency_rows() -> list[tuple]:
    """The fifth profile's 10,000 delivered records, in the fixture's bronze row shape.

    GENERATED FROM ITS OWN DECLARATION rather than written out, and against the same
    synthetic 1,024-key pool `scripts/probe_byte_identity.py` uses -- so the currencies and
    the instants are the generator's, not this file's, and a change to either would move the
    counts below rather than agreeing with them."""
    profile = PROFILES[CROSS_CURRENCY]
    pool = validated_pool(tuple(f"{n:08d}" for n in range(1, POOL_SIZE + 1)))
    return [
        payment(
            record[payments.IDENTITY_COLUMN],
            record[payments.EVENT_TIME_COLUMN],
            payer=record["payer_cnpj_basico"],
            payee=record["payee_cnpj_basico"],
            amount=record["amount"],
            method=record["payment_method"],
            currency=record["currency"],
        )
        for record in delivered_records(profile.stream_spec(pool), profile.defects)
    ]


@pytest.fixture(scope="module")
def resolved_cross_currency(spark, empresas_bronze):
    """The fifth profile's 10,000 rows with `fx_rate`, `fx_rate_date` and `amount_brl`
    resolved by the REAL functions the loader calls -- and nothing else.

    NO STAR AND NO VAULT, which is what makes a 10,000-row test affordable here. What is
    under test is the resolution, and the resolution reads exactly two of the payment's own
    columns: `currency`, and `event_time` through `event_instant`."""
    source = spark.createDataFrame(_cross_currency_rows(), PAYMENTS_SCHEMA)
    series = rate_intervals(spark.read.table(ptax_table(spark, empresas_bronze.db)))
    resolved = with_resolved_rates(
        source,
        series.intervals,
        instant=event_instant(payments.EVENT_TIME_COLUMN),
        day=day_of(payments.EVENT_TIME_COLUMN),
        currency_column=DIM_CURRENCY.fact_column,
        reporting_currency=payments.REPORTING_CURRENCY,
    )
    return resolved.withColumn(
        AMOUNT_BRL, converted_amount(amount_column="amount", amount_type=AMOUNT_TYPE)
    )


def test_the_resolution_reproduces_the_published_fx_split(resolved_cross_currency):
    """THE CLOSING TEST, marked against numbers published before this code existed.

    `docs/f-api-run-evidence.md` §1.1: 2,864 USD rows fall back to Friday 2026-06-19 at venda
    5.14420 and 2,041 resolve same-day to Monday 2026-06-22 at venda 5.13950. The two are
    counted SEPARATELY and both must be non-zero -- standing decision §4.6, and the assertion
    three earlier windows of this phase would each have failed: 2026-06-19 put every row on
    the same-day path, and 2026-06-21T14:00Z put every row on the fallback path because the
    Monday quote publishes twelve hours after that window closes.

    IT IS COUNTED OFF THE RESOLVED FRAME AND NOT OFF THE RECORDS. `tests/test_payment
    _profiles.py` already compares each record's instant against the publication instant in
    Python; this counts what `opl.gold.fx` actually wrote, which is the claim about the code
    that will run.

    THE COMPARISON IS TOTAL, WHICH IS WHAT REPLACED THREE ASSERTIONS THAT COULD NOT FAIL. It
    read `min(FELL_BACK, SAME_DAY) > 0` and `FELL_BACK + SAME_DAY == USD_ROWS` -- arithmetic on
    three module literals, true without a session. A whole-dict equality carries both claims
    against MEASURED counts: a path with no rows is a MISSING KEY, a third population is an
    EXTRA one, and a day-grain implementation collapses the first two into a single entry."""
    by_rate = {
        (row[FX_RATE], row[FX_RATE_DATE]): row["n"]
        for row in resolved_cross_currency.groupBy(FX_RATE, FX_RATE_DATE)
        .agg(F.count(F.lit(1)).alias("n"))
        .collect()
    }
    assert by_rate == {
        (FRIDAY_VENDA, FRIDAY): FELL_BACK,
        (MONDAY_VENDA, MONDAY): SAME_DAY,
        (UNIT_RATE, MONDAY): BRL_ROWS,
    }, "an empty path is an unexercised path and a fourth group is a population nobody named"
    converted = resolved_cross_currency.where(
        F.col("currency") != payments.REPORTING_CURRENCY
    ).count()
    assert by_rate[(FRIDAY_VENDA, FRIDAY)] + by_rate[(MONDAY_VENDA, MONDAY)] == converted
    assert converted == USD_ROWS
    assert sum(by_rate.values()) == 10_000


def test_two_payments_on_one_calendar_day_carry_two_different_rates(resolved_cross_currency):
    """THE PROPERTY NO CALENDAR-DAY IMPLEMENTATION CAN PRODUCE, and the reason T3's rule is an
    INSTANT comparison rather than a join on dates.

    Every payment in this stream falls on 2026-06-22 -- one calendar day, in both UTC and BRT
    -- and the rate they convert at is not one number: the 2026-06-22 bulletin publishes
    partway through the window, so an event before it may only use Friday's quote and one
    after it uses Monday's. A day-grain join gives all 10,000 rows the same rate and passes
    every other assertion in this repository.

    IT ALSO PROVES THE FALLBACK CROSSES A WHOLE WEEKEND on real rows: the pre-bulletin
    population reaches back past Sunday 2026-06-21 and Saturday 2026-06-20 to Friday
    2026-06-19, and past the unquoted part of Monday morning as well.

    AND THAT LAST CLAIM IS MEASURED OFF THE FRAME NOW. It was `FRIDAY <= MONDAY - 3 days`, two
    module literals and a subtraction -- true in any Python. The distance the rows ACTUALLY
    took is `datediff(day, fx_rate_date)`, which is `{0, 3}` here and is the same quantity
    `FactLoadResult.fx_widest_fallback_days` reports for a whole build."""
    usd = resolved_cross_currency.where(F.col("currency") == "USD")
    days = {row[0] for row in usd.select(day_of(payments.EVENT_TIME_COLUMN)).distinct().collect()}
    assert days == {MONDAY}, "the whole window sits inside one calendar day"

    rates = {row[0] for row in usd.select(FX_RATE).distinct().collect()}
    assert rates == {FRIDAY_VENDA, MONDAY_VENDA}, (
        "two rates on one calendar day is the whole point: a join on dates cannot produce it"
    )
    quote_dates = {row[0] for row in usd.select(FX_RATE_DATE).distinct().collect()}
    assert quote_dates == {FRIDAY, MONDAY}
    reached = {
        row[0]
        for row in usd.select(
            F.datediff(day_of(payments.EVENT_TIME_COLUMN), F.col(FX_RATE_DATE))
        ).distinct().collect()
    }
    assert reached == {0, 3}, (
        "the fallback reaches back across Saturday and Sunday to Friday's quote, and the "
        "same-day population reaches back nothing at all"
    )


def test_a_reporting_currency_row_converts_at_exactly_one_and_consults_no_quote(
    resolved_cross_currency,
):
    """`fx_rate = 1` EXACTLY, and "exactly" is the assertion. BRL is what this lakehouse
    reports in, so its rate is definitional rather than looked up: a BRL row that had
    somehow matched a PTAX interval would carry 5.something here, and `amount_brl` would be
    five times the amount with every count in the load still clean.

    AND ITS `fx_rate_date` IS THE PAYMENT'S OWN DAY rather than NULL -- the alternative reads
    as more honest and is worse in a star, because `date_format(NULL)` is NULL and every one
    of these rows would carry an unjoinable foreign key."""
    brl = resolved_cross_currency.where(F.col("currency") == payments.REPORTING_CURRENCY)
    measured = brl.select(
        F.count(F.lit(1)).alias("rows"),
        F.count(F.when(F.col(FX_RATE) == F.lit(1).cast(FX_RATE_TYPE), 1)).alias("unit"),
        F.count(F.when(F.col(AMOUNT_BRL) == F.col("amount").cast(AMOUNT_TYPE), 1)).alias("same"),
        F.count(
            F.when(F.col(FX_RATE_DATE) == day_of(payments.EVENT_TIME_COLUMN), 1)
        ).alias("own_day"),
        F.count_distinct(F.col(FX_RATE)).alias("rates"),
    ).collect()[0]
    assert measured["rows"] == BRL_ROWS
    assert measured["unit"] == BRL_ROWS
    assert measured["same"] == BRL_ROWS
    assert measured["own_day"] == BRL_ROWS
    assert measured["rates"] == 1


def test_the_rate_keeps_five_decimals_and_the_converted_amount_rounds_half_up(spark):
    """`fx_rate` IS NOT `AMOUNT_TYPE`, AND THE ARITHMETIC IS THE ARGUMENT. At
    `decimal(18, 2)` the rate 5.14420 becomes 5.14 and `amount_brl` is about 0.08% wrong on
    every USD row -- plausible in magnitude, in a column nobody re-derives.

    AND THE ROUNDING IS HALF-UP AT THE ROW, WHICH IS WHY `SUM(amount_brl)` IS NOT
    `SUM(amount) * rate` TO THE CENT. 1.23 x 1.50000 is 1.845 exactly: half-up gives 1.85 and
    half-even gives 1.84, so this pins WHICH rounding Spark's decimal cast performs rather
    than merely that it rounds. The evidence document says the same thing in words so a
    reader does not file the difference as a defect."""
    frame = spark.createDataFrame(
        [("1.23", "1.50000"), ("100.00", "5.14420")], "amount string, rate string"
    ).select(
        F.col("rate").cast(FX_RATE_TYPE).alias(FX_RATE),
        F.col("amount").alias("amount"),
    )
    converted = frame.withColumn(
        AMOUNT_BRL, converted_amount(amount_column="amount", amount_type=AMOUNT_TYPE)
    )
    assert dict(converted.dtypes)[FX_RATE] == FX_RATE_TYPE.replace(" ", "")
    assert dict(converted.dtypes)[AMOUNT_BRL] == AMOUNT_TYPE.replace(" ", "")
    values = {row["amount"]: row[AMOUNT_BRL] for row in converted.collect()}
    assert values["1.23"] == Decimal("1.85"), "half-even would give 1.84"
    assert values["100.00"] == Decimal("514.42")


# --- T4c: the reduce, and the refusal that is not a tie-break --------------------------


def _quote(quote_date: str, published: str, venda: str) -> tuple:
    """One landed `bronze_ptax` row, all-string as bronze holds it."""
    return (quote_date, "USD", published, "5.00000", venda)


def test_two_landed_rows_for_one_quote_date_that_disagree_are_refused(spark, empresas_bronze):
    """T4c's REFUSAL, and it is never a `max()`.

    `bronze_ptax` is written `mode("append")`, so a second extraction over the same window
    lands a second row per quote date -- an ordinary re-run, not an incident.
    `ptax_source.sole_quote` reduces ONE RESPONSE and cannot see across runs, so the reduce
    has to happen here. Two rows that DISAGREE on the rate are refused rather than resolved,
    because taking either silently picks the rate every payment on that date converts at.

    THE REFUSAL SHIPS UNEXERCISED AGAINST THE SOURCE and this is the only body that has ever
    taken it: Task 0 walked 903 rows over 3.6 years and found one duplicate pair, and the fix
    pass found a second -- both AGREE. It is a suite-only path by construction, and the
    evidence says so rather than reporting it as working."""
    disagreeing = ptax_table(
        spark,
        empresas_bronze.db,
        rows=(
            _quote("2026-06-19", "2026-06-19 13:03:25.555497", "5.14420"),
            _quote("2026-06-19", "2026-06-19 13:03:25.555524", "5.14430"),
        ),
    )
    with pytest.raises(ValueError, match="DIFFERENT cotacao_venda values"):
        rate_intervals(spark.read.table(disagreeing))


# HOW `_published_instant` REACHES 16:03:25.555497Z, AND THE RETRACTED CLAIM THIS FILE STILL
# GAVE AS THE REASON. It APPENDS THE OFFSET TO THE TEXT --
# `to_timestamp(concat(data_hora_cotacao, '-03:00'))` -- so the instant is a function of the
# value and not of `spark.sql.session.timeZone`.
#
# The docstring below said "`_published_instant` spells it `to_utc_timestamp` so the session
# zone cancels on both sides". That is the sentence commit `9d83efe` retracted in the source
# and did not follow here. `to_utc_timestamp` does NOT cancel: it renders its input in UTC
# rather than in the session zone, while `to_timestamp` over zoneless text parses IN the
# session zone -- so only one of the two ever varied, and the shipped instant moved to
# 19:03:25.555497Z under America/Sao_Paulo. There is no `to_utc_timestamp` anywhere in `src/`,
# `databricks/` or `scripts/` except the three sites in `fx.py` that retract it.
#
# THE ASSERTIONS WERE ALWAYS RIGHT AND ONLY THE REASON WAS FALSE, which is the shape this
# phase keeps catching: a test that pins the verdict does not pin the explanation beside it,
# and a stale claim survives longest in the file a reader consults about that very subject.
# The implementation they refuse is the other obvious alternative -- adding three hours to a
# parsed wall clock -- which agrees under UTC and is three hours out under America/Sao_Paulo.


def test_two_landed_rows_that_agree_reduce_to_the_earlier_publication_instant(
    spark, empresas_bronze
):
    """THE RE-RUN CASE, WHICH IS THE ONE THAT HAPPENS. Two identical extractions of one quote
    date agree by construction, so the reduce must yield ONE row -- otherwise the FX join
    fans out and every USD fact row is duplicated, caught only by a check whose own message
    begins "THE TABLE ON DISK IS ALREADY WRITTEN".

    THE EARLIER STAMP IS KEPT, AND `max()` WOULD CHANGE THE ANSWER. Both rows carry the same
    rate -- the branch above has already refused any group that does not -- so BCB published
    it at the earlier instant and the later row re-publishes a number already knowable. Under
    `max()` a payment falling between the two would be denied a rate it could already have
    used and would fall back to the previous business day, which is the model this phase's T3
    retracted, rebuilt inside the reduce.

    AND THE INSTANT IS ASSERTED IN FULL, UNDER TWO SESSION ZONES, WHICH IS T3's "pin it in a
    test". The zone FIX shipped and its INVARIANCE was asserted nowhere: the only non-UTC
    session test (`test_fact_payment.py`) has a BRL-only fixture, so the FX interval bounds are
    computed there and discarded, and this assertion read `.microsecond` -- blind to a
    whole-hour shift, which is the only thing a wrong zone does. 13:03:25.555497 read as BRT
    (T3's ruling) is 16:03:25.555497Z -- see the comment block above for how
    `_published_instant` gets there, and for the retracted claim this docstring used to give
    as the reason."""
    twice = ptax_table(
        spark,
        empresas_bronze.db,
        rows=(
            _quote("2026-06-19", "2026-06-19 13:03:25.555497", "5.14420"),
            _quote("2026-06-19", "2026-06-19 13:03:25.555524", "5.14420"),
        ),
    )
    series = rate_intervals(spark.read.table(twice))
    rows = series.intervals.collect()
    assert len(rows) == 1, "an unreduced series fans out every USD fact row"
    assert series.quotes == 1
    assert series.first_published == FRIDAY_PUBLISHED, "the EARLIER of the two, in full"
    assert series.last_published == FRIDAY_PUBLISHED
    pinned = spark.conf.get(SESSION_TIMEZONE_CONFIG)
    assert pinned == SESSION_TIMEZONE, "the suite's session is no longer the pinned one"
    try:
        spark.conf.set(SESSION_TIMEZONE_CONFIG, "America/Sao_Paulo")
        moved = rate_intervals(spark.read.table(twice))
    finally:
        spark.conf.set(SESSION_TIMEZONE_CONFIG, pinned)
    assert moved.first_published == FRIDAY_PUBLISHED, (
        "the publication instant moved with spark.sql.session.timeZone, so which quote is in "
        "force at a payment's instant is a function of a cluster setting"
    )


def test_an_empty_landed_series_is_refused_rather_than_converting_everything_at_one(
    spark, empresas_bronze
):
    """THE ABSENT SERIES, AND ITS OWN AGGREGATE IS WHY IT NEEDED ITS OWN BRANCH. Over zero rows
    `max` and `min` return NULL, so `rates` is NULL and `unreadable` is NULL and BOTH refusals
    below are skipped -- a fact then builds against no PTAX table at all: every BRL row converts
    at 1 by definition, every USD row is refused for carrying no rate (so a BRL-only lakehouse
    would not even notice), and the run log prints "converted at ONE rate over 0 quotes
    published None .. None". That is the pre-phase state reported as a clean conversion.

    A `quotes == 0` REFUSAL NEEDS NO BOUND, which is what separates it from the gaplessness
    assertion this layer declines: it is a statement about the extraction window that requires
    neither a holiday calendar nor a number drawn from one window."""
    with pytest.raises(ValueError, match="reduced to NO quotes at all"):
        rate_intervals(spark.read.table(ptax_table(spark, empresas_bronze.db, rows=())))


def test_a_rate_that_cannot_be_read_is_refused_and_the_distinct_count_cannot_see_it(
    spark, empresas_bronze
):
    """THE UNREADABLE-VALUE REFUSAL, WITH A WITNESS. It was recorded as "suite-only, and
    deliberately not removed" while nothing in the suite reached it -- and every other
    suite-only entry in that ledger means a fixture fires the branch.

    AND THE ROUTE TO IT IS THE ARGUMENT FOR COUNTING NULLS SEPARATELY. `cotacao_venda` of
    `"abc"` casts to NULL; `count_distinct` IGNORES NULLs, so `rates` is 0 rather than 1 and the
    disagreement branch tests truthiness before comparing -- a series of nothing but unreadable
    rates would sail past a `> 1` check. The count beside it is what fires, and without it the
    group would reduce to a NULL rate and convert every payment on that quote date to NULL.

    THE DQ GATE REFUSES THIS SHAPE ONE LAYER UP, so it can only be a bronze row that did not
    come through the gate -- which is the boundary the gate's own `null_or_empty_*` rules are
    justified by, and the reason it is not removed."""
    unreadable = ptax_table(
        spark,
        empresas_bronze.db,
        rows=(_quote("2026-06-19", "2026-06-19 13:03:25.555497", "abc"),),
    )
    with pytest.raises(ValueError, match="that is not a decimal"):
        rate_intervals(spark.read.table(unreadable))


def test_two_quote_dates_sharing_one_publication_instant_do_not_depend_on_the_row_order(
    spark, empresas_bronze
):
    """THE TIE-BREAK, WHOSE FIXTURE IS ITS ONLY WITNESS -- and this docstring used to claim two
    real ones that it does not have. `Window.partitionBy(currency).orderBy(publication)` had no
    second key. It said "two quote dates CAN share one publication instant -- 2001-12-21's
    duplicate pair carries identical stamps, and 1984-12-03/04/05 all published on 1984-12-05".
    **Neither is that phenomenon.** 2001-12-21's two rows are ONE quote date, so `_reduced`
    collapses them before this window exists; the 1984 three publish at 11:31, 12:40 and 18:50
    -- one publication DATE, three instants. Measured over 1984-11-28 .. 2026-08-13: 10,447
    rows over **10,446 distinct publication stamps**, so exactly one stamp repeats and both its
    rows are one quote date. **No two distinct quote dates share a publication instant anywhere
    in 42 years of series**, which is what that arithmetic settles rather than samples.

    IT IS STILL DRIVEN, AND THE ROWS BELOW ARE WHY IT MAY BE. `bronze_ptax` appends, so two
    quote dates under one stamp is reachable from a hand-repaired file or a revised window even
    though BCB has never published one. One of the tied rows gets the empty interval `[t, t)`
    and the other the range to the ceiling, and WHICH is undetermined: no fan-out is possible
    either way, so the row count and every count taken off it stay right while the surviving
    rate is arbitrary. `docs/f-api-run-evidence.md` §3 carries it as unexercised.

    ASSERTED AS ORDER-INDEPENDENCE RATHER THAN AS A CHOSEN WINNER, because that is the property:
    the same two landed rows in the other order must produce the same intervals. The later quote
    date is the one that carries the range, which is the answer a reader would predict from the
    series -- a quote published at the same instant as an earlier one supersedes it."""
    stamp = "2026-06-19 13:03:25.555497"
    pair = (_quote("2026-06-18", stamp, "5.14400"), _quote("2026-06-19", stamp, "5.14420"))
    bounds = [
        {
            row[_FX_QUOTE_DATE]: (row[_FX_FROM], row[_FX_TO])
            for row in rate_intervals(
                spark.read.table(ptax_table(spark, empresas_bronze.db, rows=rows))
            ).intervals.collect()
        }
        for rows in (pair, tuple(reversed(pair)))
    ]
    assert bounds[0] == bounds[1], "which rate survives depends on the landed row order"
    superseded, in_force = bounds[0][date(2026, 6, 18)], bounds[0][date(2026, 6, 19)]
    assert superseded[0] == superseded[1], "the superseded quote's interval is empty"
    assert in_force[1] > in_force[0], "the later quote date carries the open range"


def test_a_payment_below_the_series_first_publication_is_refused_and_not_nulled(
    spark, empresas_bronze, dim_loaded, conformed_tables, fact_target
):
    """T3 CLAUSE 3, and the DERIVED measure's own pre-write refusal.

    A NULL rate gives a NULL `amount_brl` and lowers every total by an amount nobody can
    name, which is exactly the argument `_refuse_payments_no_instant_can_be_read` makes about
    the delivered measure -- and that guard cannot cover this one, because it reads bronze
    before the FX join exists. So the refusal is its own, over the derived frame, before the
    append.

    IT SHIPS UNEXERCISED AGAINST THE REAL DATA. Nothing in this phase's payment range sits
    below 2026-06-03, so the only population that reaches it is this fixture: a USD payment
    on 2026-06-20 against a series whose first quote publishes on 2026-07-31."""
    late_series = ptax_table(
        spark,
        empresas_bronze.db,
        rows=(_quote("2026-07-31", "2026-07-31 13:10:31.061071", "5.07730"),),
    )
    early_usd = f"{empresas_bronze.db}.bronze_payments_early_usd"
    (
        spark.createDataFrame(
            [payment("tx-usd-early", BEFORE, currency="USD"), payment("tx-brl", AFTER)],
            PAYMENTS_SCHEMA,
        )
        .write.format("delta").mode("append").saveAsTable(early_usd)
    )
    with pytest.raises(ValueError, match="carry no fx_rate"):
        build_fact(
            spark, dim_loaded=dim_loaded, fact_source=early_usd,
            conformed_tables=conformed_tables, fx_source=late_series, target=fact_target,
        )
    assert not spark.catalog.tableExists(fact_target), (
        "the refusal is before the first write, so there is nothing to drop"
    )


# THE TRUNCATED EXTRACTION, WHICH IS THE ONE FAILURE EVERY OTHER NUMBER IN THE BUILD BELOW
# CALLS CLEAN. `_FX_TO` coalesces the last quote's bound to `VALID_TO_CEILING`, so a payment
# after the last landed publication does not fail to resolve -- it matches THAT quote and
# converts at it. Here the series holds one quote, 2026-06-19, and the USD payment is on
# 2026-08-01: it converts at a rate FORTY-THREE DAYS old, the grain is enforced, no key is
# orphaned, no reference is unresolved, and `fx_rates_used` is the 2 a mixed star should have.
# Nothing in that list moves.
#
# SO THE RUN'S OWN NUMBERS ARE WHAT MAKE IT VISIBLE, and they are the two the fix pass added
# because the publication span alone was one side of a comparison. `fx_beyond_series` counts
# the conversions that took the last landed quote and `fx_widest_fallback_days` says how far
# back any conversion reached -- 1 and 43 here, 0 and 3 on the real rebuild. Both are REPORTED
# and neither is refused: a payment after the most recent bulletin is the normal case (20,000
# fact rows fall on Saturday 2026-08-01), and telling that from a window that stopped early
# needs the holiday calendar T3 refuses on the record.
#
# IT WOULD ALSO HAVE PASSED AGAINST THE PREVIOUS IMPLEMENTATION -- the fact builds, correctly,
# from a truncated series -- which is exactly why the assertions are on the REPORT and not on
# a refusal. Against that implementation `FactLoadResult` carried neither field.
#
# THE PROSE IS A COMMENT BLOCK AND NOT A DOCSTRING because the function reached 52 of the
# project's 50-line cap with it inside, which is the remedy `opl.gold.fx` applies twice.


def test_a_series_that_stops_short_of_the_payments_is_REPORTED_by_the_run_it_cannot_refuse(
    spark, empresas_bronze, dim_loaded, conformed_tables, fact_target
):
    """A one-quote series against payments 43 days later: everything reads clean except the
    two coverage numbers. See the comment block above."""
    short_series = ptax_table(
        spark,
        empresas_bronze.db,
        rows=(_quote("2026-06-19", "2026-06-19 13:03:25.555497", "5.14420"),),
    )
    payments_table = f"{empresas_bronze.db}.bronze_payments_past_the_series"
    (
        spark.createDataFrame(
            [payment("tx-usd-late", AFTER, currency="USD"), payment("tx-brl-late", AFTER)],
            PAYMENTS_SCHEMA,
        )
        .write.format("delta").mode("append").saveAsTable(payments_table)
    )
    result = build_fact(
        spark, dim_loaded=dim_loaded, fact_source=payments_table,
        conformed_tables=conformed_tables, fx_source=short_series, target=fact_target,
    )
    assert result.appended == 2 and result.source_identities == 2
    assert dict(result.unresolved) == {"payer_company_sk": 0, "payee_company_sk": 0}
    assert set(dict(result.orphaned).values()) == {0}, "every other count reads clean"
    assert result.fx_rates_used == 2
    assert result.fx_quotes == 1
    assert result.fx_last_published == FRIDAY_PUBLISHED
    assert result.fx_beyond_series == 1, (
        "the USD payment is later than every publication in the series, so its rate is "
        "whatever the extraction stopped at and no other number in this build says so"
    )
    assert result.fx_widest_fallback_days == 43
    converted = spark.read.table(fact_target).where(F.col(FX_RATE) != UNIT_RATE).collect()
    assert [row[FX_RATE] for row in converted] == [FRIDAY_VENDA]


def test_the_built_fact_carries_the_declared_fx_types_and_a_joinable_quote_date_key(
    spark, fact_loaded
):
    """THE WHOLE STAR, THROUGH THE REAL LOADER -- what the 10,000-row frame above is not
    built to say.

    The fixture population is BRL-only, so every row converts at exactly 1 and
    `amount_brl == amount`: that is a property of the FIXTURE and not evidence about the
    conversion, which is why the counts live in the generated-stream tests above. What this
    asserts is the SHAPE the workspace will get -- the two decimal types, and a
    `fx_rate_date_key` that equals `event_date_key` on a row whose rate is definitional."""
    result, table = fact_loaded
    frame = spark.read.table(table)
    assert dict(frame.dtypes)[FX_RATE] == FX_RATE_TYPE.replace(" ", "")
    assert dict(frame.dtypes)[AMOUNT_BRL] == AMOUNT_TYPE.replace(" ", "")
    assert FX_RATE_DATE not in frame.columns, (
        "the star carries fx_rate_date_key and no bare date column -- one of three "
        "deviations from master spec 4.3, all recorded in docs/adr/0016-fx-resolved-by-"
        "publication-instant-not-a-holiday-calendar.md"
    )
    rows = frame.select("event_date_key", "fx_rate_date_key", FX_RATE, AMOUNT_BRL, "amount")
    for row in rows.collect():
        assert row[FX_RATE] == Decimal("1.00000")
        assert row[AMOUNT_BRL] == row["amount"]
        assert row["fx_rate_date_key"] == row["event_date_key"]
    assert result.fx_rates_used == 1, (
        "a BRL-only fixture reaches ONE rate, which is the state the fifth profile ends and "
        "which the run log reports as such rather than as a clean conversion"
    )
    assert result.fx_quotes == 3
    assert FACT_PAYMENT.additive_measure == AMOUNT_BRL
