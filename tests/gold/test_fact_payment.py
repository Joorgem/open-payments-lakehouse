"""`fact_payment` -- the six tensions this task was dispatched against, each closed by a
test that can fail.

EVERY NUMBER HERE IS A FIXTURE'S AND NOT THE WORKSPACE'S, and the distinction is stated
once, at the top, rather than left for a reader to infer. The fixture is a five-company
vault built by the REAL vault loaders and seven bronze payments over four business
attribute tuples, all of it declared in `tests/gold/conftest.py`; the workspace claims --
40,150 rows, 40,000 identities, 36,800 tuples, 3,200 legitimate repeats -- are PREDICTIONS
published in `databricks/resources/gold_fact_payment_job.yml`, and the run of 2026-08-14
confirmed all four (`docs/f-api-run-evidence.md` §2.4, §2.8). What the fixture proves is
that the MECHANISM does the thing; what the run proves is what the mechanism answers on
real data. Neither is offered as the other.

THOSE FOUR NUMBERS READ 20,150 / 20,000 / 18,400 / 1,600 HERE UNTIL THE CONSOLIDATION PASS
-- TWO PHASES STALE, AND ATTRIBUTED TO A YAML THAT NO LONGER SAID THEM. They were the
two-promoted-stream state; F3 landed a third stream (30,150 / 30,000 / 27,600 / 2,400) and
F-API a fourth. `gold_fact_payment_job.yml` retired both earlier states explicitly -- "a
third pair of columns would be two stale states above one live one" -- and
`fact_guards.py`'s own refusal message was moved to 36,800 / 40,000 / 3,200 in the same
phase. This file was the half that was not, and it cited the YAML for figures the YAML had
deleted. The workspace numbers are quoted here because the tests below drive the guards at
them; a fixture number and a workspace number are never mixed in one assertion.

THE STAR'S FIXTURES ARE IN THE CONFTEST, WHICH IS A SPLIT AND NOT A TIDY-UP. With them
inline this file stood at 894 lines against the project's 800-line cap, and the master
protocol's remedy for that is a split rather than a condensation -- "a previous
condensation to hit exactly 800 lost two real arguments while claiming it had not". The
conftest argues the payment population where it is declared; every test below reads
`fact_source`, `conformed_tables` and `fact_loaded` from there.

THE STRADDLE IS THE POINT OF THE FIXTURE. `conftest.C_THREE_VERSIONS` carries MAY, JUNE
and JULY versions of `capital_social` -- 1000,00 -> 5000,00 -> 370000,00, the last of which
is `47070968`'s real July value -- so two payments on either side of the July snapshot
resolve to two DIFFERENT `dim_company` versions carrying two different capital figures.
An as-of join that cannot change its answer is a join with extra columns, and Task 0
measured that every payment currently in bronze sits AFTER both snapshots, which makes the
workspace's own answer degenerate until the `between-snapshots` stream lands.

RAZAO SOCIAL IS NOT QUOTED ANYWHERE IN THIS FILE, and that is a rule about the real data
rather than about the fixture: two of the three pool companies that carry two versions are
`natureza_juridica` 2135, empresario individual, where the razao social IS a private
individual's name. `capital_social` on a sociedade empresaria limitada is the quotable
attribute, and it is the one the fixture moves."""
from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from opl.config import SESSION_TIMEZONE, SESSION_TIMEZONE_CONFIG
from opl.contracts import payments
from opl.gold.columns import GHOST_SURROGATE_KEY, RECORD_SOURCE, VALID_FROM, VALID_TO
from opl.gold.conformed import fact_surrogate_key, load_conformed_dimension
from opl.gold.facts import (
    AMOUNT_TYPE,
    _refuse_a_row_count_that_is_not_one_per_delivered_identity,
    event_instant,
    fact_rows,
)
from opl.gold.fx import AMOUNT_BRL, FX_RATE, rate_intervals
from opl.gold.registry import (
    DIM_CHANNEL,
    DIM_COMPANY,
    DIM_CURRENCY,
    DIM_DATE,
    FACT_PAYMENT,
)
from opl.gold.specs import contract_role, fact_keys
from opl.vault.registry import BusinessKeyColumn

from .conftest import (
    AFTER,
    BEFORE,
    BUILT_AT,
    C_THREE_VERSIONS,
    CAPITAL_IN_JULY,
    CAPITAL_IN_JUNE,
    DELIVERED_ROWS,
    DISTINCT_IDENTITIES,
    DISTINCT_TUPLES,
    HUB,
    NATURAL_KEY,
    PAYMENTS_SCHEMA,
    RECORD_SOURCE_VALUE,
    SAT,
    build_fact,
    payment,
    recovered,
    row_of,
    version_of,
)

# --- T-A: two role-playing keys into one dimension, measured at (row, role) grain ------


def test_every_payment_resolves_both_roles_and_matches_exactly_one_version(
    spark, dim_loaded, fact_loaded
):
    """T-A's CLOSING TEST, at the grain the acceptance is actually about.

    The acceptance this task inherited says "every `fact_payment` row resolves to exactly
    one `dim_company` version". That is ill-formed: a correct row resolves to TWO, one per
    role. So the measurement is `2 x COUNT(*)` references, zero unresolved, zero
    multi-matched, REPORTED PER ROLE.

    AND THE MULTI-MATCH CHECK IS SEPARATE FROM THE RESOLVED COUNT, which is the half that
    is easy to get wrong: a fan-out does not lower a resolution rate -- both matches
    resolve -- it raises the ROW COUNT. So the two are asserted independently, one from the
    ghost counts and one from the grain."""
    result, table = fact_loaded
    references = len(FACT_PAYMENT.roles) * (result.appended + result.already_present)
    assert references == 2 * DISTINCT_IDENTITIES
    assert result.unresolved == (("payer_company_sk", 0), ("payee_company_sk", 0))
    assert result.source_identities == DISTINCT_IDENTITIES
    assert result.appended == DISTINCT_IDENTITIES, (
        "the fact holds more or fewer rows than bronze has distinct transaction_id -- a "
        "fan-out or an over-eager dedup, neither of which a resolution rate can see"
    )
    both = spark.read.table(table).where(
        (F.col("payer_company_sk") != GHOST_SURROGATE_KEY)
        & (F.col("payee_company_sk") != GHOST_SURROGATE_KEY)
    )
    assert both.count() == DISTINCT_IDENTITIES
    for key in FACT_PAYMENT.role_keys:
        resolved = {row[0] for row in spark.read.table(table).select(key).collect()}
        assert all(version_of(spark, dim_loaded.table, sk) for sk in resolved), (
            f"a {key} names no dim_company row at all"
        )


def test_the_two_roles_are_resolved_independently_and_never_borrow_each_other(
    spark, dim_loaded, fact_loaded
):
    """THE FAILURE A SINGLE-ROLE FACT PRODUCES, made visible. `tx-d` reverses the pair:
    the three-version company is the PAYEE and the one-version company is the payer. If
    the two role keys were derived from one join, or if one role were resolved and the
    other copied, `tx-d`'s payee key would be the payer's."""
    _result, table = fact_loaded
    (reversed_pair,) = row_of(spark, table, "tx-d")
    (straight,) = row_of(spark, table, "tx-b")
    assert reversed_pair["payer_company_sk"] != reversed_pair["payee_company_sk"]
    assert reversed_pair["payee_company_sk"] == straight["payer_company_sk"], (
        "the three-version company resolves to one version at one instant whichever role "
        "it plays -- these two payments are both AFTER the July snapshot"
    )
    assert reversed_pair["payer_company_sk"] == straight["payee_company_sk"]


# --- T-B: the as-of join has to CHANGE ITS ANSWER -------------------------------------


def test_two_payments_straddling_a_snapshot_resolve_to_two_different_company_versions(
    spark, dim_loaded, fact_loaded
):
    """T-B's CLOSING TEST, ON A LOCAL FIXTURE -- said plainly, because the workspace proof
    comes later from the run and this is not it.

    ONE COMPANY, TWO PAYMENTS, ONE SNAPSHOT BETWEEN THEM. `tx-a` happens on 2026-06-20 and
    `tx-b` on 2026-08-01, either side of the July snapshot, and they are the same payer
    paying the same payee the same amount by the same rail -- a LEGITIMATE REPEAT, so the
    ONLY thing that differs is when. They must resolve to two different `company_sk`
    values carrying two different `capital_social` values.

    THE ATTRIBUTE IS `capital_social` AND NEVER THE RAZAO SOCIAL. Two of the three real
    pool companies that move are empresario individual, where that field is a private
    individual's name; the fixture moves capital instead, and its July figure (370000,00)
    is `47070968`'s own.

    THE PAYEE IS THE CONTROL. It carries ONE version, so its key must be IDENTICAL across
    the two payments -- which is what stops this test passing because the loader keyed on
    the instant rather than resolving with it."""
    _result, table = fact_loaded
    (before,) = row_of(spark, table, "tx-a")
    (after,) = row_of(spark, table, "tx-b")

    assert before["payer_company_sk"] != after["payer_company_sk"], (
        "the as-of join returned one version for two payments on either side of a "
        "snapshot -- which is a join with extra columns, not an as-of join"
    )
    (june,) = version_of(spark, dim_loaded.table, before["payer_company_sk"])
    (july,) = version_of(spark, dim_loaded.table, after["payer_company_sk"])
    assert (june["capital_social"], july["capital_social"]) == (
        CAPITAL_IN_JUNE, CAPITAL_IN_JULY,
    )
    assert june[NATURAL_KEY] == july[NATURAL_KEY] == C_THREE_VERSIONS

    assert before["payee_company_sk"] == after["payee_company_sk"], (
        "the single-version payee moved between two instants, so the key is a function of "
        "the payment rather than of the version it resolved"
    )


def test_the_resolved_version_is_the_one_in_force_and_not_merely_the_latest(
    spark, dim_loaded, fact_loaded
):
    """The failure Task 0 measured the workspace into: every payment sitting after both
    snapshots makes an as-of join bit-identical to `WHERE valid_to = <sentinel>`, and
    nothing distinguishes its answer from the naive one. Here the June payment must resolve
    to a version that is NOT current, and its interval must contain the payment's own
    instant."""
    _result, table = fact_loaded
    (before,) = row_of(spark, table, "tx-a")
    (june,) = version_of(spark, dim_loaded.table, before["payer_company_sk"])
    assert june["is_current"] is False, (
        "the June payment resolved to the CURRENT version, so the as-of predicate is doing "
        "nothing that `WHERE is_current` would not"
    )
    (instant,) = (
        spark.read.table(table)
        .where(F.col(FACT_PAYMENT.grain_key) == "tx-a")
        .select(event_instant(payments.EVENT_TIME_COLUMN).alias("t"))
        .collect()
    )
    assert june[VALID_FROM] <= instant["t"] < june[VALID_TO]


# --- T-F: half-open, never BETWEEN ----------------------------------------------------


def test_a_payment_landing_exactly_on_a_boundary_matches_the_opening_version_only(
    spark, dim_loaded, fact_loaded
):
    """T-F's CLOSING TEST. `tx-c` happens at exactly the July snapshot's instant. Under
    `BETWEEN`, which is inclusive at both ends, it would match the version that CLOSED
    there and the version that OPENED there -- the multi-match this star's own acceptance
    forbids, manufactured by the operator that reads most naturally.

    THE ROW COUNT IS THE ASSERTION AND THE VERSION IS THE CONFIRMATION. A fan-out shows up
    as a second row for `tx-c`, which is why the count is checked first: `valid_from <= t`
    picking the wrong side would still return exactly one row, and only the capital figure
    tells the two apart."""
    _result, table = fact_loaded
    boundary = row_of(spark, table, "tx-c")
    assert len(boundary) == 1, (
        "a payment on a version boundary matched two versions -- BETWEEN, or an interval "
        "that is closed at both ends"
    )
    (july,) = version_of(spark, dim_loaded.table, boundary[0]["payer_company_sk"])
    assert july["capital_social"] == CAPITAL_IN_JULY, (
        "the boundary instant resolved to the version that CLOSED there rather than the "
        "one that opened there -- the interval is `valid_from <= t < valid_to`"
    )
    (instant,) = (
        spark.read.table(table)
        .where(F.col(FACT_PAYMENT.grain_key) == "tx-c")
        .select(event_instant(payments.EVENT_TIME_COLUMN).alias("t"))
        .collect()
    )
    assert july[VALID_FROM] == instant["t"], (
        "the payment's instant is not the version's own opening instant, so this test is "
        "not standing on the boundary at all and proves nothing about BETWEEN. Both sides "
        "are collected through the same conversion, so the comparison is about the DATA"
    )


# --- T-C: the zone, and the two representations the as-of join crosses ----------------


def test_the_fact_is_unchanged_when_it_is_built_under_a_non_utc_session_zone(
    spark, dim_loaded, fact_source, conformed_tables, fact_target, fact_loaded
):
    """T-C's CLOSING TEST. A `TIMESTAMP` is UTC micros and every conversion into or out of
    one resolves through `spark.sql.session.timeZone`; the as-of comparison crosses ISO
    TEXT in bronze and TIMESTAMPS in `dim_company`, which is the sharpest instance of that
    hazard in this repository.

    THE WRONG ZONE IS SET HERE RATHER THAN INHERITED, which is the correction the UTC pin
    forced on `tests/gold/test_conformed.py`: with the session pinned, a test that merely
    hoped to be on a non-UTC box would go green while saying nothing.

    WHAT MUST NOT MOVE: both role keys, and `event_date_key`. The first is an instant
    comparison -- `to_timestamp(..., XXX)` reads the offset out of the text, so the parse
    is zone-free, and `valid_from`/`valid_to` were WRITTEN as instants. The second is read
    from the first ten characters of the text, never from a cast, which is what
    `opl.gold.conformed.day_of` exists for.

    WHAT DOES MOVE, AND IS THEREFORE NOT ASSERTED: `load_date`. It is a plain instant built
    from ISO text with no offset, so Spark parses it in the session zone -- which is
    exactly why every gold entry point pins the zone before it reads a table."""
    _result, utc_table = fact_loaded
    pinned = spark.conf.get(SESSION_TIMEZONE_CONFIG)
    assert pinned == SESSION_TIMEZONE, "the suite's session is no longer the pinned one"
    try:
        spark.conf.set(SESSION_TIMEZONE_CONFIG, "America/Sao_Paulo")
        build_fact(
            spark, dim_loaded=dim_loaded, fact_source=fact_source,
            conformed_tables=conformed_tables, target=fact_target,
        )
        moved = _keyed_rows(spark, fact_target)
    finally:
        spark.conf.set(SESSION_TIMEZONE_CONFIG, pinned)
    assert moved == _keyed_rows(spark, utc_table), (
        "the fact built under America/Sao_Paulo disagrees with the one built under UTC, so "
        "the star's answer is a function of a cluster setting"
    )


def _keyed_rows(spark, table) -> dict:
    """Every fact row's keys, by payment -- the columns a session zone must not move."""
    columns = (*FACT_PAYMENT.role_keys, *fact_keys(DIM_DATE), *fact_keys(DIM_CHANNEL))
    return {
        row[FACT_PAYMENT.grain_key]: tuple(row[column] for column in columns)
        for row in spark.read.table(table).collect()
    }


def test_a_payment_whose_event_time_carries_no_zone_designator_is_refused(
    spark, empresas_bronze, dim_loaded, conformed_tables, fact_target
):
    """THE CONTRACT THE CAST DOES NOT ENFORCE, and the reason the parse is explicit.
    `CAST('2026-08-01T00:00:00.000' AS TIMESTAMP)` succeeds and resolves the instant
    through `spark.sql.session.timeZone`, so the as-of answer becomes a function of a
    cluster setting with nothing in the data to show it. `to_timestamp(..., XXX)` yields
    NULL, which matches no half-open interval -- every such row would resolve to the ghost
    in BOTH roles with the build reporting a merely lower resolution rate."""
    table = f"{empresas_bronze.db}.bronze_payments_zoneless"
    (
        spark.createDataFrame(
            [payment("tx-zoneless", "2026-08-01T00:00:00.000")], PAYMENTS_SCHEMA
        )
        .write.format("delta").mode("append").saveAsTable(table)
    )
    with pytest.raises(ValueError, match="no instant can be read"):
        build_fact(
            spark, dim_loaded=dim_loaded, fact_source=table,
            conformed_tables=conformed_tables, target=fact_target,
        )


def test_a_payment_whose_amount_is_not_a_number_is_refused(
    spark, empresas_bronze, dim_loaded, conformed_tables, fact_target
):
    """The measure's half of the same guard. A NULL measure sums to nothing and lowers
    every total by an amount nobody can name -- and the payment contract makes the column
    required, so this is a bronze row that did not come through the DQ gate."""
    table = f"{empresas_bronze.db}.bronze_payments_bad_amount"
    (
        spark.createDataFrame([payment("tx-bad", AFTER, amount="R$ 100")], PAYMENTS_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(table)
    )
    with pytest.raises(ValueError, match="that is not a decimal"):
        build_fact(
            spark, dim_loaded=dim_loaded, fact_source=table,
            conformed_tables=conformed_tables, target=fact_target,
        )


def test_a_conformed_table_that_does_not_exist_stops_the_build_before_it_writes(
    spark, dim_loaded, fact_source, conformed_tables, fact_target
):
    """THE OPERATIONAL ORDER THIS JOB CANNOT DECLARE, caught in the loader instead.
    `gold_fact_payment_job.yml` states the order `dim_company -> conformed -> fact`
    because Databricks Asset Bundles have no cross-JOB dependency, and it says a missing
    table fails the read. That sentence was true of `dim_company` -- read inside
    `fact_rows` -- and FALSE of the three conformed tables, which were read only by the
    orphan count. The orphan count runs AFTER the append, so this job launched before the
    conformed one wrote `fact_payment` in full and THEN failed table-not-found: an
    operator saw a FAILED task against a correct table, and the repair that shape suggests
    -- drop it -- was the wrong one.

    THE ASSERTION THAT MATTERS IS THE SECOND. That the build raises proves little; a build
    that raised at the orphan count would raise too. That the TARGET DOES NOT EXIST is the
    property, and it is the one that was false. `dim_currency` is used rather than
    `dim_date` deliberately: it is the LAST of the three, so a resolution that ran per
    dimension somewhere after the write would still let the first two through.

    NOTHING IS CREATED TO MAKE THIS FAIL -- the name is simply a table that was never
    built, which is exactly the state of the star before the conformed job's first run."""
    never_built = f"{fact_source}_no_such_conformed_table"
    assert not spark.catalog.tableExists(never_built), "the fixture name is not free"
    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        build_fact(
            spark, dim_loaded=dim_loaded, fact_source=fact_source,
            conformed_tables={**conformed_tables, DIM_CURRENCY.name: never_built},
            target=fact_target,
        )
    assert not spark.catalog.tableExists(fact_target), (
        "the fact was written and the task then failed on a table it had not read yet -- "
        "which is a FAILED run against a correct table, and the defect this test closes"
    )


# --- T-D: the 3,200 legitimate repeats ------------------------------------------------


def test_the_fact_keeps_every_business_tuple_bronze_holds_and_more_rows_than_tuples(
    spark, dim_loaded, fact_source, conformed_tables, fact_loaded
):
    """T-D's CLOSING TEST, AND IT IS THE LOAD-BEARING ONE. Two assertions, and neither
    alone is enough.

    FIRST: the distinct business-attribute-tuple count is IDENTICAL in bronze and in the
    fact. Deduplication is by `transaction_id`, and a redelivery is byte-identical to its
    original, so removing one removes no tuple.

    SECOND, AND IT IS THE ONE THAT CATCHES THE DEFECT: `COUNT(*) - distinct tuples` in the
    fact is STRICTLY GREATER THAN ZERO. A fact deduplicated on the business attributes --
    or on a "natural key" built from (payer, payee, amount, currency, payment_method),
    which is the obvious thing to reach for -- SATISFIES THE FIRST ASSERTION PERFECTLY:
    36,800 rows over 36,800 tuples on the workspace's data. Zero here is the 3,200 real
    payments it deleted.

    THE 150-DUPLICATE ACCEPTANCE IS NOT USED, because it cannot fail. `COUNT(*) -
    COUNT(DISTINCT transaction_id)` re-measures bronze's own arithmetic and comes out right
    whichever column the dedup was taken over; it is asserted here as a property of BRONZE
    and never of the fact.

    THE TUPLES ARE RECOVERED THROUGH THE DIMENSIONS, which is the star doing its job: the
    fact carries `payer_company_sk` where bronze carries `payer_cnpj_basico`, and
    `channel_key` where bronze carries `payment_method`."""
    result, table = fact_loaded
    bronze = spark.read.table(fact_source)
    bronze_tuples = bronze.select(*payments.BUSINESS_ATTRIBUTE_COLUMNS).distinct().count()
    assert (bronze.count(), bronze_tuples) == (DELIVERED_ROWS, DISTINCT_TUPLES)
    assert bronze.count() - result.source_identities == DELIVERED_ROWS - DISTINCT_IDENTITIES

    star = recovered(spark, table, dim_loaded.table, conformed_tables)
    fact_tuples = star.select(*payments.BUSINESS_ATTRIBUTE_COLUMNS).distinct().count()
    assert fact_tuples == bronze_tuples, (
        "the fact holds fewer business tuples than bronze -- something the deduplication "
        "was not supposed to touch has gone"
    )
    held = result.appended + result.already_present
    assert held - fact_tuples > 0, (
        "the fact holds exactly one row per business tuple, which means it deduplicated on "
        "the ATTRIBUTES and deleted every legitimate repeat -- 3,200 real payments on the "
        "workspace's own data"
    )
    assert (held - fact_tuples, result.legitimate_repeats) == (
        DISTINCT_IDENTITIES - DISTINCT_TUPLES, DISTINCT_IDENTITIES - DISTINCT_TUPLES,
    )


@pytest.mark.parametrize(
    ("held", "identities", "expected"),
    [
        (36_800, 40_000, "LEGITIMATE REPEATS"),
        (80_000, 40_000, "MORE THAN ONE dimension version"),
    ],
    ids=["deduplicated on the attributes", "multi-matched"],
)
def test_a_row_count_that_is_not_one_per_delivered_identity_is_refused(
    held, identities, expected
):
    """THE TWO FAILURES ONE NUMBER COVERS, driven at the workspace's own counts rather than
    at the fixture's -- so this test also pins what the job YAML promises.

    THE PAIRS WERE 18,400 / 20,000 AND 40,000 / 20,000, WHICH THE YAML STOPPED PROMISING TWO
    PHASES AGO, so a docstring saying "this pins what the YAML promises" was pinning what it
    used to. They are the four-stream figures now: 36,800 tuples against 40,000 identities
    for the deduplication, and 80,000 -- the (row, role) reference count -- against 40,000
    for a two-way fan-out.

    They push the count in opposite directions and neither is visible in a resolution rate:
    a fan-out RAISES it (both matches resolve, so the rate is still 100%), and a
    deduplication over the business attributes LOWERS it to the distinct-tuple count, which
    is a number that looks entirely plausible beside a duplicate count that still reads
    150."""
    with pytest.raises(ValueError, match=expected):
        _refuse_a_row_count_that_is_not_one_per_delivered_identity(
            FACT_PAYMENT,
            target_table="workspace.default.fact_payment",
            source_table="workspace.default.bronze_payments",
            held=held,
            identities=identities,
        )


def test_a_redelivery_carrying_different_attributes_is_refused_before_the_write(
    spark, empresas_bronze, dim_loaded, conformed_tables, fact_target
):
    """The one thing the row count cannot see, refused by the tuple count instead. Two rows
    under ONE `transaction_id` carrying DIFFERENT business attributes is not a redelivery at
    all -- it is a corrupt stream -- and the deduplication would keep whichever one a sort
    put first, at the right row count and with a resolution rate of 100%."""
    table = f"{empresas_bronze.db}.bronze_payments_conflicting"
    (
        spark.createDataFrame(
            [payment("tx-x", AFTER, amount="100.00"), payment("tx-x", AFTER, amount="900.00")],
            PAYMENTS_SCHEMA,
        )
        .write.format("delta").mode("append").saveAsTable(table)
    )
    with pytest.raises(ValueError, match="distinct business-attribute tuples"):
        build_fact(
            spark, dim_loaded=dim_loaded, fact_source=table,
            conformed_tables=conformed_tables, target=fact_target,
        )
    assert not spark.catalog.tableExists(fact_target), "the refusal wrote rows first"


# --- T-E: the ghost, predicted at zero and reported as unexercised --------------------


def test_no_payment_in_this_star_reaches_the_ghost(spark, fact_loaded):
    """T-E, and the honest half of it. Every counterparty is drawn from the hub's own key
    space, `dim_company` covers every hub key, and both interval ends are floored -- so
    `COALESCE(<as-of lookup>, GHOST)` CANNOT FIRE on this data. Zero here is a PREDICTION
    that held, not a path that was exercised, and `gold_load_fact.py` prints it as such."""
    result, table = fact_loaded
    assert result.unresolved == (("payer_company_sk", 0), ("payee_company_sk", 0))
    ghosts = spark.read.table(table).where(
        (F.col("payer_company_sk") == GHOST_SURROGATE_KEY)
        | (F.col("payee_company_sk") == GHOST_SURROGATE_KEY)
    )
    assert ghosts.count() == 0


def test_a_counterparty_no_dimension_version_covers_lands_on_the_ghost_and_is_counted(
    spark, empresas_bronze, dim_loaded, conformed_tables, fact_target
):
    """THE MECHANISM, EXERCISED IN A FIXTURE BECAUSE THE REAL DATA CANNOT EXERCISE IT --
    and this is not a manufactured payment added to the workspace's evidence to make its
    number non-zero. It is the only way `COALESCE(..., GHOST)` is reached at all: without
    it the coalesce is untested code that the phase's own prediction says will never run.

    THE ROW SURVIVES, which is the property being pinned. NULL never matches under
    equality, and this repository has already paid 8,757 phantom departures for that fact
    in a LEFT ANTI join -- here the join is LEFT, so the unresolved counterparty does not
    vanish from the fact, it arrives on the ghost and is counted."""
    table = f"{empresas_bronze.db}.bronze_payments_unknown"
    (
        spark.createDataFrame(
            [payment("tx-unknown", AFTER, payer="99999999")], PAYMENTS_SCHEMA
        )
        .write.format("delta").mode("append").saveAsTable(table)
    )
    result = build_fact(
        spark, dim_loaded=dim_loaded, fact_source=table,
        conformed_tables=conformed_tables, target=fact_target,
    )
    assert result.appended == 1, "the unresolved payment was DROPPED rather than ghosted"
    assert result.unresolved == (("payer_company_sk", 1), ("payee_company_sk", 0))
    (row,) = spark.read.table(fact_target).collect()
    assert row["payer_company_sk"] == GHOST_SURROGATE_KEY
    assert row["payee_company_sk"] != GHOST_SURROGATE_KEY


# --- the conformed keys, derived rather than joined -----------------------------------


def test_the_key_a_fact_derives_is_the_key_the_dimension_holds(
    spark, conformed_tables, fact_loaded
):
    """THE PRICE OF DERIVING THREE FOREIGN KEYS WITHOUT A JOIN, paid by a test.
    `opl.gold.conformed` computes the dimension's key over its own natural key column and
    the fact's key over the payment's column, through ONE function -- so this asserts the
    two call sites agree on every declared member, which is the only thing that makes the
    derivation legitimate. Two spellings would be two keys for one member, silently, since
    both are integers and the join simply returns nothing."""
    _result, table = fact_loaded
    for dimension in (DIM_CHANNEL, DIM_CURRENCY):
        members = spark.read.table(conformed_tables[dimension.name]).where(
            F.col(dimension.natural_key).isNotNull()
        )
        declared = {
            row[0]: row[1]
            for row in members.select(
                F.col(dimension.natural_key), F.col(dimension.surrogate_key)
            ).collect()
        }
        derived = {
            row[0]: row[1]
            for row in spark.createDataFrame(
                [(member,) for member in dimension.members],
                f"{dimension.fact_column} string",
            )
            .select(
                F.col(dimension.fact_column),
                fact_surrogate_key(dimension, contract_role(dimension)).alias("k"),
            )
            .collect()
        }
        assert derived == declared


def test_every_derived_conformed_key_names_a_member_that_exists(fact_loaded):
    """REFERENTIAL INTEGRITY OVER THE THREE KEYS THAT HAVE NO GHOST TO FALL BACK ON. They
    are computed from the payment's own columns rather than looked up, so an orphan does
    not resolve to the unknown member -- it joins to nothing and the payment disappears
    from every report grouped by that dimension, with no count anywhere going wrong."""
    result, _table = fact_loaded
    assert result.orphaned == (
        ("event_date_key", 0), ("fx_rate_date_key", 0), ("channel_key", 0),
        ("currency_key", 0),
    )


def test_a_conformed_dimension_older_than_the_payments_is_reported_and_not_refused(
    spark, empresas_bronze, vault_loaded, dim_loaded, fact_source, conformed_tables,
    fact_target,
):
    """THE STATE THAT IS LEGITIMATE AND MUST STILL BE VISIBLE. A `dim_date` built before a
    payment batch landed simply lacks that batch's days; the repair is to re-run the
    conformed build, which APPENDS, so refusing would make the operator drop a fact that is
    not wrong. It is reported instead, and `gold_load_fact.py` prints a different sentence
    for it.

    THE NARROW CALENDAR IS BUILT OVER A NARROWER FACT, which is how this happens in
    practice rather than by hand: the same loader, the same satellite, one payment day."""
    narrow_source = f"{empresas_bronze.db}.bronze_payments_one_day"
    (
        spark.createDataFrame([payment("tx-only", BEFORE)], PAYMENTS_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(narrow_source)
    )
    narrow_date = f"{empresas_bronze.db}.dim_date_narrow"
    load_conformed_dimension(
        spark, DIM_DATE, fact_table=narrow_source, target_table=narrow_date,
        load_date=BUILT_AT, applied_date_table=vault_loaded.sat,
    )
    result = build_fact(
        spark, dim_loaded=dim_loaded, fact_source=fact_source,
        conformed_tables={**conformed_tables, DIM_DATE.name: narrow_date},
        target=fact_target,
    )
    orphans = dict(result.orphaned)
    assert orphans["event_date_key"] > 0, (
        "the narrow calendar stops at 2026-07-11 and the fact carries payments on "
        "2026-08-01, so those rows key into nothing"
    )
    assert orphans["channel_key"] == orphans["currency_key"] == 0


# --- the projection, the write, and the re-run ----------------------------------------


def test_the_projected_columns_are_the_declared_ones_in_the_declared_order(
    spark, dim_loaded, fact_source, fx_source, fact_loaded
):
    """THE ORDER IS LOAD-BEARING AND NOT TIDY: a Delta append matches POSITIONALLY unless
    `mergeSchema` says otherwise, and this table's first five columns are all integers, so
    a permutation writes each key into another's column with nothing failing.

    ASSERTED OVER `fact_rows` AS WELL AS THE TABLE, because the frame is public and a later
    caller projecting it in another order is exactly what this pins against."""
    _result, table = fact_loaded
    expected = [
        "payer_company_sk", "payee_company_sk", "event_date_key", "fx_rate_date_key",
        "channel_key", "currency_key", FACT_PAYMENT.grain_key, FACT_PAYMENT.measure,
        FX_RATE, AMOUNT_BRL, payments.EVENT_TIME_COLUMN, "load_date", RECORD_SOURCE,
    ]
    assert spark.read.table(table).columns == expected
    frame, _coverage = fact_rows(
        FACT_PAYMENT,
        dimension=DIM_COMPANY,
        hub=HUB,
        conformed=(DIM_DATE, DIM_CHANNEL, DIM_CURRENCY),
        source=spark.read.table(fact_source),
        versions=spark.read.table(dim_loaded.table),
        series=rate_intervals(spark.read.table(fx_source)),
        load_date=BUILT_AT,
    )
    assert frame.columns == expected
    assert dict(frame.dtypes)[FACT_PAYMENT.measure] == AMOUNT_TYPE.replace(" ", "")


def test_the_fact_carries_the_producers_own_record_source_and_not_this_layers(
    spark, fact_loaded
):
    """`dim_company` passes on the RFB's `record_source` because the RFB delivered those
    values; a PIT row names `opl.gold.pit` because it carries only a pointer this layer
    computed. A fact row's measure, its grain and its instant were all DELIVERED by the
    payment producer, so it passes bronze's `_record_source` on for `dim_company`'s
    reason."""
    _result, table = fact_loaded
    sources = {
        row[0] for row in spark.read.table(table).select(RECORD_SOURCE).distinct().collect()
    }
    assert sources == {RECORD_SOURCE_VALUE}


def test_a_second_build_over_an_unchanged_source_appends_nothing(
    spark, dim_loaded, fact_source, conformed_tables, fact_target
):
    """`max_retries: 0` does not prevent a retry (master protocol section 4.4), so the
    branch a retry lands in has to stay green -- and the grain check has to hold there,
    because it is checked on the count HELD rather than on the count appended."""
    first = build_fact(
        spark, dim_loaded=dim_loaded, fact_source=fact_source,
        conformed_tables=conformed_tables, target=fact_target,
    )
    again = build_fact(
        spark, dim_loaded=dim_loaded, fact_source=fact_source,
        conformed_tables=conformed_tables, target=fact_target,
        load_date=datetime(2027, 9, 9, 9, 9, 9),
    )
    assert (first.appended, first.already_present) == (DISTINCT_IDENTITIES, 0)
    assert (again.appended, again.already_present) == (0, DISTINCT_IDENTITIES)


def test_a_payment_batch_that_lands_later_is_appended_and_nothing_is_re_keyed(
    spark, empresas_bronze, dim_loaded, conformed_tables, fact_target
):
    """UNLIKE THE SCD2 LOADER, THIS ONE NEED NOT STOP WHEN ITS SOURCE GROWS. That module
    must refuse, because an append cannot revise an interval it has already closed; nothing
    here has an interval, so a payment batch that landed since the last build is a set of
    new `transaction_id` values to append -- and the keys already written cannot move,
    because they are `dim_company`'s and the versions are the same versions."""
    growing = f"{empresas_bronze.db}.bronze_payments_growing"
    frame = spark.createDataFrame([payment("tx-first", BEFORE)], PAYMENTS_SCHEMA)
    frame.write.format("delta").mode("append").saveAsTable(growing)
    first = build_fact(
        spark, dim_loaded=dim_loaded, fact_source=growing,
        conformed_tables=conformed_tables, target=fact_target,
    )
    before = {row[FACT_PAYMENT.grain_key]: row["payer_company_sk"]
              for row in spark.read.table(fact_target).collect()}
    (
        spark.createDataFrame([payment("tx-second", AFTER)], PAYMENTS_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(growing)
    )
    second = build_fact(
        spark, dim_loaded=dim_loaded, fact_source=growing,
        conformed_tables=conformed_tables, target=fact_target,
    )
    assert (first.appended, second.appended) == (1, 1)
    after = {row[FACT_PAYMENT.grain_key]: row["payer_company_sk"]
             for row in spark.read.table(fact_target).collect()}
    assert after["tx-first"] == before["tx-first"], "an existing payment was re-keyed"
    assert after["tx-second"] != after["tx-first"], (
        "the two payments straddle the July snapshot and must resolve to two versions"
    )


# --- the pairings the loader refuses before it derives anything -----------------------


def test_the_loader_refuses_a_dimension_a_hub_or_a_conformed_order_it_was_not_declared(
    spark, dim_loaded, fact_source, conformed_tables, fact_target
):
    """Four arguments have to be checked against one another, and each mismatch is silent
    in its own way: another dimension builds a well-formed fact against another table's
    version chain, a composite-key hub matches every company sharing its first component,
    and two conformed dimensions transposed write each key into the other's column -- all
    of them integers, so nothing fails."""
    import dataclasses

    with pytest.raises(ValueError, match="was handed dimension"):
        build_fact(
            spark, dim_loaded=dim_loaded, fact_source=fact_source,
            conformed_tables=conformed_tables, target=fact_target,
            dimension=dataclasses.replace(DIM_COMPANY, name="dim_other"),
        )
    with pytest.raises(ValueError, match="A composite key cannot be reached"):
        build_fact(
            spark, dim_loaded=dim_loaded, fact_source=fact_source,
            conformed_tables=conformed_tables, target=fact_target,
            # `business_key_columns` is a PROPERTY over `business_keys`, so the composite
            # hub is built by widening the declaration rather than the derived tuple.
            hub=dataclasses.replace(
                HUB,
                business_keys=(
                    *HUB.business_keys, BusinessKeyColumn(name=SAT.payload_columns[0]),
                ),
            ),
        )
    with pytest.raises(ValueError, match="IN THAT ORDER"):
        build_fact(
            spark, dim_loaded=dim_loaded, fact_source=fact_source,
            conformed_tables=conformed_tables, target=fact_target,
            conformed=(DIM_CHANNEL, DIM_DATE, DIM_CURRENCY),
        )
