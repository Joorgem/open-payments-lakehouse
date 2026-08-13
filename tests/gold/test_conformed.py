"""The three conformed dimensions -- `dim_date`, `dim_channel`, `dim_currency` -- and
the measurement that stops the evidence describing them with an adjective.

WHAT THIS FILE IS ACTUALLY FOR. Two of the three are constant columns wearing a
dimension's name: `dim_currency` has exactly ONE member and every payment carries it,
and `dim_date` spans about fifty days that the whole payment population reaches on ONE
of. A dimension whose fact-side cardinality is 1 cannot be wrong and no test over it can
fail -- so the tests below are not attempts to make it look tested. They pin the two
things that are genuinely checkable: that the member set is what the payment CONTRACT
declares (never what the data happens to contain, which would make the two numbers equal
by construction), and that `fact_side_cardinality` is COUNTED from a fact frame rather
than declared anywhere.

THE FACT FRAME IS BUILT FROM `opl.generator.profiles.PROFILES` AND NOT FROM A LITERAL.
Every profile publishes `window_start` and `last_event_time` before any stream is
generated, so the days a payment can carry are derivable without generating 10,000
events per profile (~10 s each on this box). Reading them here is also what makes the
coverage test below hold when the `between-snapshots` window moves: it is the
declaration that moves, and this file follows it."""
from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from opl.config import SESSION_TIMEZONE, SESSION_TIMEZONE_CONFIG
from opl.contracts import payments
from opl.generator.profiles import PROFILES
from opl.gold.columns import CONFORMED_RECORD_SOURCE, GHOST_RECORD_SOURCE, GHOST_SURROGATE_KEY
from opl.gold.conformed import (
    covered_span,
    day_of,
    fact_side_cardinality,
    load_conformed_dimension,
)
from opl.gold.members import CALENDAR_ATTRIBUTES, calendar_members
from opl.gold.registry import DIM_CHANNEL, DIM_COMPANY, DIM_CURRENCY, DIM_DATE

from .conftest import BUILT_AT, JUL, JUN, REF_DATES

# The contract's own column list, as a bronze-payments-shaped all-string DDL. Bronze
# stores every contract column as a STRING (`opl.contracts.payments`), so a fixture that
# typed `event_time` as a timestamp would test a table this lakehouse does not have --
# and would hide the one hazard `day_of` exists for.
PAYMENTS_SCHEMA = ", ".join(f"{column} string" for column in payments.COLUMNS)

# What a payment carries in every column this layer does not read. The two it does read
# are supplied per row.
_FILLER = ("10000001", "10000002", "100,00")


def _payment(event_time: str, *, method: str = "PIX", currency: str = "BRL") -> tuple:
    return (f"tx-{event_time}-{method}", event_time, event_time, *_FILLER, currency, method)


def reachable_instants(profiles=PROFILES) -> tuple[str, ...]:
    """The first and last `event_time` every declared profile can emit.

    THE EXTREMES AND NOT THE MIDDLE, which is exactly enough: `event_time` advances by
    whole intervals from `window_start` (`opl.generator.stream._event_at`), so every
    instant a stream carries lies between the two, and the days between them are days
    `dim_date` must contain whether or not a payment lands on each."""
    return tuple(
        instant
        for profile in profiles.values()
        for instant in (profile.window_start, profile.last_event_time)
    )


def reachable_days(profiles=PROFILES) -> set[date]:
    """The UTC calendar days those instants fall on.

    READ OUT OF THE ISO TEXT, in the same way `opl.gold.conformed.day_of` reads them --
    see its docstring for why a cast through a timestamp would move every one of these
    by three hours on this project's session timezone."""
    return {date.fromisoformat(instant[:10]) for instant in reachable_instants(profiles)}


@pytest.fixture(scope="module")
def payments_bronze(spark, empresas_bronze):
    """A bronze-payments-shaped table holding the extremes of every declared profile."""
    table = f"{empresas_bronze.db}.bronze_payments_fixture"
    rows = [
        _payment(instant, method=method, currency=currency)
        for instant in reachable_instants()
        for method, currency in [(payments.PAYMENT_METHODS[0], payments.CURRENCIES[0])]
    ]
    # One row per declared payment method, so `dim_channel`'s fact-side cardinality is a
    # measurement with something to measure rather than a restatement of the fixture.
    rows += [
        _payment(PROFILES["clean"].window_start, method=method)
        for method in payments.PAYMENT_METHODS
    ]
    (
        spark.createDataFrame(rows, PAYMENTS_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(table)
    )
    return table


def build(spark, dimension, *, fact_table, target, vault=None, load_date=BUILT_AT):
    applied = {"applied_date_table": vault} if vault is not None else {}
    return load_conformed_dimension(
        spark,
        dimension,
        fact_table=fact_table,
        target_table=target,
        load_date=load_date,
        **applied,
    )


# --- the pure half: the calendar's own arithmetic ------------------------------------


def test_the_calendar_holds_every_day_between_its_bounds_inclusive_and_no_other():
    first, last = date(2026, 6, 13), date(2026, 8, 1)
    members = calendar_members(first, last)
    assert len(members) == 50, "18 days of June + 31 of July + 1 of August"
    days = [row[0] for row in members]
    assert days[0] == first and days[-1] == last
    assert days == sorted(set(days)), "a calendar with a gap or a repeat is not a calendar"
    assert (days[-1] - days[0]).days + 1 == len(days)


def test_the_calendar_names_its_days_without_asking_the_operating_system_for_a_locale():
    """`calendar.month_name` and `%A` are LOCALE-DEPENDENT: the same build run on a
    Portuguese-configured box would write `agosto` into a column another run wrote
    `August` into, and nothing would fail. The names are declared tuples for that
    reason, and ISO weekday numbering (Monday = 1) is stated rather than inherited --
    Spark's own `dayofweek` is Sunday = 1, so the two must not be confused."""
    (row,) = calendar_members(date(2026, 8, 1), date(2026, 8, 1))
    attributes = dict(zip(CALENDAR_ATTRIBUTES, row[1:], strict=True))
    assert attributes == {
        "year": 2026,
        "quarter": 3,
        "month": 8,
        "month_name": "August",
        "day_of_month": 1,
        "day_of_week": 6,
        "day_name": "Saturday",
        "is_weekend": True,
    }


def test_a_calendar_whose_last_day_precedes_its_first_is_refused():
    with pytest.raises(ValueError, match="ends before it starts"):
        calendar_members(date(2026, 8, 1), date(2026, 6, 13))


# --- T-D: the span covers every reachable payment day and both applied dates ----------


def test_dim_date_covers_every_reachable_payment_day_and_both_rfb_applied_dates(
    spark, vault_loaded, payments_bronze, gold_target
):
    """T-D's closing test. Every distinct `event_time` day the payment contract's
    declared windows can reach resolves to exactly one `dim_date` row, and so does each
    RFB `applied_date` -- which is why the span is measured from the satellite rather
    than declared as two literals in `opl.gold.registry`.

    EXACTLY ONE ROW AND NOT AT LEAST ONE. A date dimension with two rows for one day
    doubles every fact row that joins it, and the failure looks like a data problem
    rather than a dimension problem."""
    build(spark, DIM_DATE, fact_table=payments_bronze, target=gold_target.dim,
          vault=vault_loaded.sat)
    held = {
        row[0]
        for row in spark.read.table(gold_target.dim).select(DIM_DATE.natural_key).collect()
    }
    must_cover = reachable_days() | {REF_DATES[JUN], REF_DATES[JUL]}
    assert must_cover <= held, f"days the star refers to and dim_date lacks: {must_cover - held}"
    counted = (
        spark.read.table(gold_target.dim)
        .groupBy(DIM_DATE.natural_key).count()
        .where("count > 1").count()
    )
    assert counted == 0, "a day held twice doubles every fact row that joins it"


def test_the_span_is_measured_from_the_fact_and_the_vault_and_not_declared(
    spark, vault_loaded, payments_bronze
):
    """The span's two ends come from two different tables, and the test says which.

    The low end is the earliest `applied_date` the satellite holds -- 2026-05-09 in this
    fixture, a month the RFB publishes and this project never loaded -- and the high end
    is the latest payment day. Neither is a literal anywhere in `src/opl/gold`."""
    fact = spark.read.table(payments_bronze)
    applied = spark.read.table(vault_loaded.sat)
    first, last = covered_span(fact, DIM_DATE, applied=applied)
    assert first == min(REF_DATES.values()), "the low end is the vault's earliest snapshot"
    assert last == max(reachable_days()), "the high end is the latest payment day"


def test_a_payment_whose_event_time_is_not_an_instant_is_refused_rather_than_dropped(
    spark, empresas_bronze, vault_loaded, gold_target
):
    """`min`/`max` IGNORE NULLs, so an unparseable `event_time` would silently shrink the
    span instead of widening it -- and the payments it belongs to would then resolve to
    the ghost while the build reported success."""
    table = f"{empresas_bronze.db}.bronze_payments_broken_{gold_target.dim.split('_')[-1]}"
    (
        spark.createDataFrame([_payment("not-a-timestamp")], PAYMENTS_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(table)
    )
    with pytest.raises(ValueError, match="no calendar day can be read"):
        build(spark, DIM_DATE, fact_table=table, target=gold_target.dim,
              vault=vault_loaded.sat)


def test_the_calendar_day_is_read_from_the_iso_text_and_not_through_the_session_zone(
    spark
):
    """THE HAZARD THIS PROJECT WOULD OTHERWISE HAVE SHIPPED. `event_time` is a UTC
    instant written as text (`...T00:00:00.000Z`), and `CAST(... AS TIMESTAMP)` resolves
    it in the SESSION timezone -- so under America/Sao_Paulo midnight UTC becomes 21:00
    of the PREVIOUS day and every payment's date moves. The star's answer would then
    depend on a cluster setting, which is not a property a calendar may have.

    THE WRONG ZONE IS SET HERE RATHER THAN INHERITED, which is the correction the pin
    forced. This assertion used to rest on the suite's session having happened to inherit
    America/Sao_Paulo from the operating system: with `opl.config.SESSION_TIMEZONE`
    pinned to UTC the cast agrees with the text, and the test would have gone green while
    saying nothing -- exactly the outcome its own failure message predicted. Setting the
    zone makes both halves hold on any box and under any pin: the text-read day never
    moves, and the cast moves it whenever the session is not UTC."""
    frame = spark.createDataFrame(
        [_payment("2026-08-01T00:00:00.000Z")], PAYMENTS_SCHEMA
    )
    read = frame.select(day_of(payments.EVENT_TIME_COLUMN).alias("d")).collect()[0][0]
    assert read == date(2026, 8, 1)
    pinned = spark.conf.get(SESSION_TIMEZONE_CONFIG)
    assert pinned == SESSION_TIMEZONE, "the suite's session is no longer the pinned one"
    try:
        spark.conf.set(SESSION_TIMEZONE_CONFIG, "America/Sao_Paulo")
        under_the_zone = frame.select(
            day_of(payments.EVENT_TIME_COLUMN).alias("read"),
            F.to_date(F.col(payments.EVENT_TIME_COLUMN).cast("timestamp")).alias("cast"),
        ).collect()[0]
    finally:
        spark.conf.set(SESSION_TIMEZONE_CONFIG, pinned)
    assert under_the_zone["read"] == date(2026, 8, 1), (
        "day_of moved the day with the session zone, which is the whole thing it exists "
        "not to do"
    )
    assert under_the_zone["cast"] == date(2026, 7, 31), (
        "the cast no longer moves the day under America/Sao_Paulo -- if that is true, "
        "the reason day_of reads the text has changed and both this assertion and the "
        "docstring in opl.gold.conformed need revisiting"
    )


# --- T-A: fact-side cardinality is counted, never declared ---------------------------


def test_the_fact_side_cardinality_is_counted_from_the_fact_it_is_handed(
    spark, payments_bronze
):
    """T-A's closing test. The same dimension, two different fact frames, two different
    numbers -- which no declared constant can produce.

    WHY THIS IS THE MEASUREMENT THAT MATTERS. `dim_date` spans about fifty days and the
    entire payment population reaches ONE of them (`docs/f3-run-evidence.md` §0.5: every
    `event_time` on 2026-08-01), rising to two when the `between-snapshots` profile
    lands. Task 5 has to publish those numbers rather than the word "thin", and a
    cardinality that was declared anywhere would publish the declaration."""
    fact = spark.read.table(payments_bronze)
    f1b_only = {name: PROFILES[name] for name in ("clean", "promotable", "drifting")}
    before = fact.where(
        F.col(payments.EVENT_TIME_COLUMN).isin(list(reachable_instants(f1b_only)))
    )
    assert fact_side_cardinality(before, DIM_DATE) == len(reachable_days(f1b_only)) == 1
    assert fact_side_cardinality(fact, DIM_DATE) == len(reachable_days()) == 2
    assert fact_side_cardinality(fact, DIM_CHANNEL) == len(payments.PAYMENT_METHODS)
    assert fact_side_cardinality(fact, DIM_CURRENCY) == len(payments.CURRENCIES) == 1


def test_the_member_count_and_the_fact_side_cardinality_are_independent_numbers(
    spark, payments_bronze, gold_target
):
    """The reason the members are the CONTRACT's domain and never the observed values.

    Built from `SELECT DISTINCT payment_method`, a channel dimension could not have an
    unobserved member: its member count and its fact-side cardinality would be one
    number twice, and the star would lose the ability to say a rail went unused. Here
    they are two numbers, and this fixture makes them differ."""
    one_rail = spark.read.table(payments_bronze).where(
        F.col("payment_method") == payments.PAYMENT_METHODS[0]
    )
    result = build(spark, DIM_CHANNEL, fact_table=payments_bronze, target=gold_target.dim)
    assert result.members == len(payments.PAYMENT_METHODS) == 5
    assert result.fact_side_cardinality == 5
    assert fact_side_cardinality(one_rail, DIM_CHANNEL) == 1


# --- the loads themselves ------------------------------------------------------------


@pytest.mark.parametrize(
    ("dimension", "expected"),
    [(DIM_CHANNEL, len(payments.PAYMENT_METHODS)), (DIM_CURRENCY, len(payments.CURRENCIES))],
    ids=lambda value: getattr(value, "name", value),
)
def test_an_enumerated_dimension_holds_its_contract_domain_and_one_ghost(
    spark, payments_bronze, gold_target, dimension, expected
):
    result = build(spark, dimension, fact_table=payments_bronze, target=gold_target.dim)
    assert (result.members, result.appended) == (expected, expected + 1)
    written = spark.read.table(gold_target.dim)
    assert result.distinct_keys == result.appended, "two members on one key match both"
    held = {
        row[0]
        for row in written.where(F.col(dimension.natural_key).isNotNull())
        .select(dimension.natural_key).collect()
    }
    assert held == set(dimension.members)
    sources = {row[0] for row in written.select("record_source").distinct().collect()}
    assert sources == {CONFORMED_RECORD_SOURCE, GHOST_RECORD_SOURCE}


@pytest.mark.parametrize("dimension", [DIM_DATE, DIM_CHANNEL, DIM_CURRENCY], ids=lambda d: d.name)
def test_every_conformed_dimension_carries_one_ghost_no_fact_row_can_join_to(
    spark, vault_loaded, payments_bronze, gold_target, dimension
):
    """T-F. Each of the three gets a ghost, each ghost is keyed on `GHOST_SURROGATE_KEY`
    and carries a NULL natural key, and NONE of them is reachable by a join -- a fact row
    reaches it as `COALESCE(<lookup>, GHOST_SURROGATE_KEY)` at fact-build time and never
    by matching on the key it does not have.

    AND ALL THREE ARE PREDICTED TO REPORT ZERO ROWS IN TASK 4, which is the honest thing
    to say rather than counting the row as an exercised path (master protocol §A6). Every
    contract column these dimensions draw from is REQUIRED and drawn from the declared
    domain by the generator, and `dim_date`'s span is derived from the fact itself, so no
    payment in `bronze_payments` can fail to resolve. `dim_date`'s ghost is the only one
    reachable even in principle: a batch landing outside the span the dimension was built
    over would reach it. The other two are structurally unreachable while `CURRENCIES` and
    `PAYMENT_METHODS` are the domains the generator picks from."""
    vault = vault_loaded.sat if dimension is DIM_DATE else None
    build(spark, dimension, fact_table=payments_bronze, target=gold_target.dim, vault=vault)
    ghosts = spark.read.table(gold_target.dim).where(
        F.col(dimension.surrogate_key) == GHOST_SURROGATE_KEY
    ).collect()
    assert len(ghosts) == 1
    assert ghosts[0][dimension.natural_key] is None, (
        "a ghost carrying a natural key is a ghost a fact row can join to, which merges "
        "every unresolved row onto whatever that key means"
    )
    assert ghosts[0]["record_source"] == GHOST_RECORD_SOURCE


@pytest.mark.parametrize("dimension", [DIM_DATE, DIM_CHANNEL], ids=lambda d: d.name)
def test_a_second_build_over_an_unchanged_source_appends_nothing(
    spark, vault_loaded, payments_bronze, gold_target, dimension
):
    """`max_retries: 0` does not prevent a retry (master protocol §4.4), so the branch a
    retry lands in is a branch that has to stay green. Unlike the SCD2 loader this one
    needs no refusal: a conformed dimension has no interval to revise, so a source that
    GAINED a day appends that day and re-keys nothing."""
    vault = vault_loaded.sat if dimension is DIM_DATE else None
    first = build(spark, dimension, fact_table=payments_bronze, target=gold_target.dim,
                  vault=vault)
    again = build(spark, dimension, fact_table=payments_bronze, target=gold_target.dim,
                  vault=vault, load_date=datetime(2027, 9, 9, 9, 9, 9))
    assert first.appended > 0
    assert (again.appended, again.already_present) == (0, first.appended)
    assert again.members == first.members


def test_a_calendar_dimension_built_with_no_applied_date_table_is_refused(
    spark, payments_bronze, gold_target
):
    """The pairing, refused rather than defaulted. Without the satellite the span would
    be the payment window alone -- a `dim_date` that silently stops covering the days
    `dim_company`'s versions open on, with the build reporting success."""
    with pytest.raises(ValueError, match="needs the satellite"):
        build(spark, DIM_DATE, fact_table=payments_bronze, target=gold_target.dim)


def test_an_enumerated_dimension_handed_an_applied_date_table_is_refused(
    spark, vault_loaded, payments_bronze, gold_target
):
    """The other direction, and it is the copied-YAML shape: a table argument left behind
    from the task above. An ignored argument is how a build that reads the wrong source
    reports success."""
    with pytest.raises(ValueError, match="reads no satellite"):
        build(spark, DIM_CHANNEL, fact_table=payments_bronze, target=gold_target.dim,
              vault=vault_loaded.sat)


def test_the_conformed_loader_refuses_the_scd2_dimension_it_cannot_build(
    spark, payments_bronze, gold_target
):
    """`dim_company` is a real registered gold table and the single most plausible string
    to end up in this task's parameter. It has no `fact_column` and no members; handed
    here it must be refused by kind, before a frame is derived."""
    with pytest.raises(ValueError, match="is not a conformed dimension"):
        build(spark, DIM_COMPANY, fact_table=payments_bronze, target=gold_target.dim)
