"""The FX columns: the rate a payment converted at, the quote date it came from, and the
amount in the reporting currency -- and the one property no calendar-day implementation can
produce.

WHY THIS IS A FILE OF ITS OWN. `test_fact_payment.py` is 750 lines and its whole fixture is
five BRL payments over three instants, which is the right fixture for an as-of join into
`dim_company` and the wrong one for FX: a BRL-only population converts at exactly 1 on every
row, `amount_brl` equals `amount` everywhere, and no assertion over either column can fail.
That is verbatim the state F-API's T1 exists to end, so a test of the FX layer has to reach a
population with two currencies in it.

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

from datetime import date, timedelta
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from opl.contracts import payments
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import delivered_records
from opl.generator.profiles import CROSS_CURRENCY, POOL_SIZE, PROFILES
from opl.gold.conformed import day_of
from opl.gold.fact_guards import AMOUNT_TYPE, event_instant
from opl.gold.fx import (
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
    that will run."""
    by_rate = {
        (row[FX_RATE], row[FX_RATE_DATE]): row["n"]
        for row in resolved_cross_currency.groupBy(FX_RATE, FX_RATE_DATE)
        .agg(F.count(F.lit(1)).alias("n"))
        .collect()
    }
    assert by_rate[(FRIDAY_VENDA, FRIDAY)] == FELL_BACK
    assert by_rate[(MONDAY_VENDA, MONDAY)] == SAME_DAY
    assert min(FELL_BACK, SAME_DAY) > 0, "an empty path is an unexercised path"
    assert FELL_BACK + SAME_DAY == USD_ROWS
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
    2026-06-19, and past the unquoted part of Monday morning as well."""
    usd = resolved_cross_currency.where(F.col("currency") == "USD")
    days = {row[0] for row in usd.select(day_of(payments.EVENT_TIME_COLUMN)).distinct().collect()}
    assert days == {MONDAY}, "the whole window sits inside one calendar day"

    rates = {row[0] for row in usd.select(FX_RATE).distinct().collect()}
    assert rates == {FRIDAY_VENDA, MONDAY_VENDA}, (
        "two rates on one calendar day is the whole point: a join on dates cannot produce it"
    )
    quote_dates = {row[0] for row in usd.select(FX_RATE_DATE).distinct().collect()}
    assert quote_dates == {FRIDAY, MONDAY}
    assert FRIDAY <= MONDAY - timedelta(days=3), (
        "the fallback reaches back across Saturday and Sunday"
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
    retracted, rebuilt inside the reduce."""
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
    # 13:03:25.555497 BRT is 16:03:25.555497Z. The EARLIER of the two, to the microsecond.
    assert series.first_published.microsecond == 555_497


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
        "the star carries fx_rate_date_key and no bare date column -- the second deviation "
        "from master spec 4.3, recorded in the T3 ADR"
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
