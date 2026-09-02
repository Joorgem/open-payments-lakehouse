"""`dim_company`: a Kimball SCD2 dimension over `sat_empresa_dados`, and the four
tensions its shape has to close.

THIS FILE IS HALF OF THE EVIDENCE, DELIBERATELY, in the shape
`tests/vault/test_cnpj_vault.py` already states for the layer below. The phase's claim
is about 69,202,817 satellite versions over 69,062,849 companies, and CI cannot see
them -- it runs local Spark with no Databricks credential. So the proof is two
artefacts: this fixture, which guards the MECHANIC on every push, and a measurement
against the real vault recorded in `docs/f3-run-evidence.md`. Neither stands in for the
other, and nothing here may be read as evidence about the RFB's data.

THE FOUR TENSIONS AND WHERE EACH IS CLOSED:

  - the version chain is closed WITHOUT a MERGE -- `F.lead` in the same window pass that
    orders the versions, one write, no second pass to end-date anything. Closed by the
    row count (`test_every_satellite_version_becomes_one_dimension_row_plus_one_ghost`)
    and by `test_no_row_leaves_its_interval_open_ended`.
  - the interval is HALF-OPEN and `BETWEEN` is forbidden. Closed by
    `test_each_versions_valid_to_is_exactly_the_next_versions_valid_from` and by
    `test_an_event_at_a_version_boundary_resolves_to_exactly_one_version`, which is the
    multi-match `BETWEEN` would manufacture.
  - BOTH ends are floored, not just the top. Closed by
    `test_the_first_version_of_every_company_starts_at_the_floor`.
  - the ghost is UNREACHABLE by any join. Closed by
    `test_the_ghost_is_unreachable_by_the_as_of_join_the_fact_will_run` and by
    `test_the_ghost_is_not_keyed_on_the_hubs_real_lowest_key`.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F

from opl.config import SESSION_TIMEZONE, SESSION_TIMEZONE_CONFIG
from opl.gold.columns import (
    GHOST_RECORD_SOURCE,
    GHOST_SURROGATE_KEY,
    IS_CURRENT,
    LOAD_DATE,
    RECORD_SOURCE,
    VALID_FROM,
    VALID_FROM_FLOOR,
    VALID_TO,
    VALID_TO_CEILING,
)
from opl.gold.dimensions import (
    COLLISION_CAUSES,
    _surrogate_key_collision,
    instant_literal,
)
from opl.vault import domains

from .conftest import (
    BUILT_AT,
    C_JULY_ONLY,
    C_THREE_VERSIONS,
    C_ZERO,
    DIM,
    HUB,
    JUL,
    JUN,
    MAY,
    REBUILT_AT,
    REF_DATES,
    SAT,
    SOURCE_VERSIONS,
    VERSIONS_OF,
    WINDOW,
    as_collected,
    build_dimension,
    load_vault,
)

KEY = HUB.business_key_columns[0]

# The two RFB ref dates as INSTANTS, which is what a version boundary is once
# `applied_date` (a DATE) becomes `valid_from` (a TIMESTAMP): the start of that day.
JUN_BOUNDARY = datetime.combine(REF_DATES[JUN], datetime.min.time())
JUL_BOUNDARY = datetime.combine(REF_DATES[JUL], datetime.min.time())


def _rows(spark, table):
    """Every dimension row, as dicts, ordered so a chain reads top to bottom."""
    frame = spark.read.table(table).orderBy(KEY, VALID_FROM)
    return [row.asDict() for row in frame.collect()]


def _chains(spark, table) -> dict[str, list[dict]]:
    """The versioned rows grouped by business key, each list in `valid_from` order.
    The ghost is excluded by its NULL key, which is the property that makes it
    unreachable in the first place."""
    chains: dict[str, list[dict]] = {}
    for row in _rows(spark, table):
        if row[KEY] is None:
            continue
        chains.setdefault(row[KEY], []).append(row)
    return chains


def _ghost(spark, table) -> dict:
    ghosts = [row for row in _rows(spark, table) if row[KEY] is None]
    assert len(ghosts) == 1, f"expected exactly one ghost row, found {len(ghosts)}"
    return ghosts[0]


# --- T-A: the chain closes without a MERGE ------------------------------------------


def test_every_satellite_version_becomes_one_dimension_row_plus_one_ghost(
    spark, dim_loaded
):
    """THE COUNT THAT SAYS WHICH NUMBER IS BEING COUNTED, which the phase's published
    prediction demands: `dim_company` holds one row per SATELLITE VERSION -- not one per
    company -- plus exactly one ghost. On the real vault that is 69,202,817 + 1;
    here it is this fixture's own version total + 1, derived from `VERSIONS_OF` rather
    than written as a literal so the fixture and the expectation cannot drift.

    It is the closing test for the no-MERGE decision because it is what an end-dating
    second pass would break: a MERGE that closed versions in place would leave the row
    count equal to the number of COMPANIES for any run that half-succeeded, and an
    append-plus-close would double it."""
    assert spark.read.table(dim_loaded.table).count() == SOURCE_VERSIONS + 1
    assert dim_loaded.result.source_versions == SOURCE_VERSIONS
    assert dim_loaded.result.appended == SOURCE_VERSIONS + 1
    assert {key: len(chain) for key, chain in _chains(spark, dim_loaded.table).items()} == (
        VERSIONS_OF
    )


def test_no_row_leaves_its_interval_open_ended(spark, dim_loaded):
    """NOT ONE `valid_to` IS NULL, the ghost included.

    A NULL open end is the commoner SCD2 spelling and it is refused twice over: it makes
    every as-of predicate carry an `OR valid_to IS NULL` that a reader forgets exactly
    once, and NULL compares FALSE in a join -- so the omission loses rows silently
    instead of failing."""
    frame = spark.read.table(dim_loaded.table)
    assert frame.filter(F.col(VALID_TO).isNull()).count() == 0
    assert frame.filter(F.col(VALID_FROM).isNull()).count() == 0


# --- T-B: half-open intervals, and no `BETWEEN` -------------------------------------


def test_each_versions_valid_to_is_exactly_the_next_versions_valid_from(spark, dim_loaded):
    """NO GAP AND NO OVERLAP, asserted as EQUALITY rather than as an ordering.

    `valid_to <= next valid_from` would admit a gap and `>=` would admit an overlap;
    both are the shapes that appear when someone writes `valid_to = next_start - 1 day`
    to make `BETWEEN` behave. There is no "one before" at any precision that does not
    leave a hole exactly that wide, which is why the interval is half-open and the two
    values are the SAME instant.

    `C_THREE_VERSIONS` is what makes this test more than a tautology: with two versions
    every chain has one adjacent pair whose ends are the floor and the ceiling, so the
    MIDDLE version -- both of whose bounds are real dates -- would never be built."""
    chains = _chains(spark, dim_loaded.table)
    assert len(chains[C_THREE_VERSIONS]) == 3, "the middle-version case is not in the fixture"
    for key, chain in chains.items():
        for earlier, later in zip(chain, chain[1:], strict=False):
            assert earlier[VALID_TO] == later[VALID_FROM], (
                f"{key}: version ending {earlier[VALID_TO]} is followed by one starting "
                f"{later[VALID_FROM]} -- a gap or an overlap, and an as-of lookup in it "
                "returns nothing or two rows"
            )


@pytest.mark.parametrize(
    "instant",
    [
        VALID_FROM_FLOOR,
        JUN_BOUNDARY - timedelta(microseconds=1),
        JUN_BOUNDARY,
        JUN_BOUNDARY + timedelta(microseconds=1),
        JUL_BOUNDARY,
        datetime(2026, 8, 1, 13, 53, 15),
    ],
)
def test_an_event_at_a_version_boundary_resolves_to_exactly_one_version(
    spark, dim_loaded, instant
):
    """EXACTLY ONE, AT EVERY INSTANT, INCLUDING THE TWO BOUNDARIES THEMSELVES.

    This is the test the phase plan's `BETWEEN` could not pass: inclusive at both ends,
    an event at exactly 2026-06-13T00:00:00 matches the version that ended there AND the
    one that started there, and the multi-match the star's own acceptance forbids is
    manufactured by the operator the plan prescribed. The microsecond either side of the
    boundary is here so the test cannot pass by the boundary happening to fall in a gap.

    2026-08-01T13:53:15 is not arbitrary: it is `max(event_time)` over `bronze_payments`
    as measured in `docs/f3-run-evidence.md` (P2), so the instant the fact will actually
    ask about is one of the six.

    ASKED THROUGH `instant_literal` AND NOT `F.lit`, which is the difference between
    asserting about this dimension and asserting about two timezones. `F.lit(datetime)`
    resolves in the DRIVER's operating-system zone while every bound in the table was
    written in the SESSION's -- so on a box that is not on the session zone, the
    microsecond either side of a boundary lands hours away from it and the test measures
    the offset instead of the interval."""
    asked = instant_literal(instant)
    resolved = (
        spark.read.table(dim_loaded.table)
        .filter(F.col(KEY) == C_THREE_VERSIONS)
        .filter((F.col(VALID_FROM) <= asked) & (asked < F.col(VALID_TO)))
    )
    assert resolved.count() == 1, (
        f"{C_THREE_VERSIONS} at {instant} resolves to {resolved.count()} versions"
    )


def test_the_as_of_lookup_changes_its_answer_across_a_boundary(spark, dim_loaded):
    """The mechanic the whole phase exists to demonstrate, at fixture scale: one
    company, two instants either side of one `applied_date`, two DIFFERENT attribute
    values. The real version of this is Task 4's, against `47070968`, whose
    `capital_social` moves 50000,00 -> 370000,00 between the two RFB snapshots."""

    def capital_at(instant: datetime) -> str:
        asked = instant_literal(instant)
        rows = (
            spark.read.table(dim_loaded.table)
            .filter(F.col(KEY) == C_THREE_VERSIONS)
            .filter((F.col(VALID_FROM) <= asked) & (asked < F.col(VALID_TO)))
            .collect()
        )
        assert len(rows) == 1
        return rows[0]["capital_social"]

    assert capital_at(JUL_BOUNDARY - timedelta(microseconds=1)) == "5000,00"
    assert capital_at(JUL_BOUNDARY) == "370000,00"


# --- T-C: both sentinels, not one ---------------------------------------------------


def test_the_first_version_of_every_company_starts_at_the_floor(spark, dim_loaded):
    """THE LOW SENTINEL, WHICH THE PHASE PLAN DID NOT ASK FOR.

    Flooring only the open end leaves a payment dated before the earliest snapshot
    resolving to NO version of a perfectly well-known company -- the star answering
    "unknown" about a row it plainly describes. `C_JULY_ONLY` is in the assertion on
    purpose: the floor is UNCONDITIONAL, so a company first observed in July is floored
    too, and `opl.gold.columns` argues both what that does not claim and why the
    tempting conditional version was rejected (it makes `valid_from`, and therefore
    every surrogate key derived from it, move under a backfill of an earlier snapshot).

    THE LAST LINE USED TO RESTATE THE LOOP. `chains[C_JULY_ONLY][0][VALID_FROM] ==
    VALID_FROM_FLOOR` is entailed by the loop above it and could not fail on its own, so
    it locked nothing. What is NOT entailed -- and what the loop's claim is only
    interesting given -- is that this company has ONE version and it is July's: a floor
    applied only to the earliest snapshot would have started it at 2026-07-11, and a
    fixture that quietly gained a June row for it would take that case away with every
    assertion here still green. That premise is what the last line asserts now, in the
    same shape `test_each_versions_valid_to_is_exactly_the_next_versions_valid_from`
    locks its own."""
    floor = as_collected(spark, VALID_FROM_FLOOR)
    chains = _chains(spark, dim_loaded.table)
    assert set(chains) == set(VERSIONS_OF)
    for key, chain in chains.items():
        assert chain[0][VALID_FROM] == floor, (
            f"{key}'s first version starts at {chain[0][VALID_FROM]}, not the floor"
        )
    assert len(chains[C_JULY_ONLY]) == 1, "the July-only case is not in the fixture"


def test_the_open_version_of_every_company_ends_at_the_ceiling(spark, dim_loaded):
    ceiling = as_collected(spark, VALID_TO_CEILING)
    chains = _chains(spark, dim_loaded.table)
    for key, chain in chains.items():
        assert chain[-1][VALID_TO] == ceiling, (
            f"{key}'s open version ends at {chain[-1][VALID_TO]}, not the ceiling"
        )


def test_is_current_is_exactly_the_rows_whose_interval_is_still_open(spark, dim_loaded):
    """`is_current` is DERIVED and therefore able to disagree with what it is derived
    from, which is the whole reason a denormalisation gets a test rather than a comment.

    IT IS A TAUTOLOGY OVER THE VERSIONED ROWS AND IT IS KEPT ANYWAY, because it is not
    one over the GHOST -- which is where it earns its place and which nothing else here
    checks. `_versioned` computes `is_current` AS `valid_to == VALID_TO_CEILING`, so for
    a version this compares an expression against itself. `_ghost_like` does not: it puts
    `F.lit(True)` and the ceiling into the schema as two INDEPENDENT entries of one fix
    map, so a ghost whose interval was closed, or whose flag was dropped to False, is a
    row this assertion catches and no derivation prevents. Stated rather than left for
    the next reader to rediscover and delete.

    The count beneath it is load-bearing for both kinds: one open version per company
    plus the ghost is a claim about the CHAINS, not about the derivation."""
    frame = spark.read.table(dim_loaded.table)
    disagreeing = frame.filter(
        F.col(IS_CURRENT) != (F.col(VALID_TO) == instant_literal(VALID_TO_CEILING))
    )
    assert disagreeing.count() == 0
    ghost = _ghost(spark, dim_loaded.table)
    assert ghost[IS_CURRENT] is True and ghost[VALID_TO] == as_collected(
        spark, VALID_TO_CEILING
    ), "the ghost is the one row whose flag and whose bound are two separate literals"
    # One open version per company, and one for the ghost.
    assert frame.filter(F.col(IS_CURRENT)).count() == len(VERSIONS_OF) + 1


# --- T-D: the ghost is unreachable --------------------------------------------------


def test_the_ghost_is_unreachable_by_the_as_of_join_the_fact_will_run(spark, dim_loaded):
    """THE CLOSING TEST FOR THE GHOST, DRIVEN THROUGH THE JOIN TASK 4 ACTUALLY RUNS:
    payment rows carrying a `cnpj_basico` and an instant, matched on the key AND on the
    half-open interval.

    IT USED TO END ON A STATEMENT THAT COULD NOT FAIL. The old closing assertion inner-
    joined the NULL-keyed row against the hub and required 0 rows back -- which is
    `NULL <> NULL`, true of any frame with a NULL key against any other frame whatsoever,
    and true before the ghost existed. It restated SQL, not this dimension.

    WHAT IS ASSERTED INSTEAD, and both halves are properties of the ghost. A payment
    naming a company the hub holds resolves to exactly one version, and that version is
    never the ghost -- so the ghost cannot steal a row that had a real answer. A payment
    naming a company the hub does NOT hold resolves to NOTHING: it comes back from the
    left join with a NULL key rather than with -1, which is what makes the fact's
    `COALESCE(<lookup>, GHOST_SURROGATE_KEY)` a decision written in one place instead of
    a join that quietly succeeds against a row carrying a business key."""
    ghost = _ghost(spark, dim_loaded.table)
    assert ghost[KEY] is None and ghost[DIM.surrogate_key] == GHOST_SURROGATE_KEY
    unknown = "99999999"
    hub_keys = {row[KEY] for row in spark.read.table(dim_loaded.names.hub).collect()}
    assert unknown not in hub_keys, "the unresolvable payment names a company that exists"
    payments = spark.createDataFrame(
        [(C_THREE_VERSIONS,), (C_ZERO,), (unknown,)], f"{KEY} string"
    ).withColumn("event_time", instant_literal(datetime(2026, 8, 1, 13, 53, 15)))
    dimension = spark.read.table(dim_loaded.table)
    resolved = payments.alias("f").join(
        dimension.alias("d"),
        (F.col(f"f.{KEY}") == F.col(f"d.{KEY}"))
        & (F.col(f"d.{VALID_FROM}") <= F.col("f.event_time"))
        & (F.col("f.event_time") < F.col(f"d.{VALID_TO}")),
        how="left",
    ).select(F.col(f"f.{KEY}").alias("asked"), F.col(f"d.{DIM.surrogate_key}").alias("sk"))
    answers = {row["asked"]: row["sk"] for row in resolved.collect()}
    assert resolved.count() == 3, "the as-of predicate matched one payment twice"
    assert answers[unknown] is None, (
        "an unresolvable payment reached a dimension row, so the ghost is joinable"
    )
    assert answers[C_THREE_VERSIONS] is not None and answers[C_ZERO] is not None
    assert GHOST_SURROGATE_KEY not in answers.values()


def test_the_ghost_is_not_keyed_on_the_hubs_real_lowest_key(spark, dim_loaded):
    """`00000000` IS A REAL COMPANY. `docs/f1b-run-evidence.md` section 2.4 records it as
    `hub_empresa`'s lowest key on the live data, so a ghost keyed there would silently
    merge every unresolved payment onto a company that exists -- with the join working,
    the row counts right and nothing to see in any log. The fixture carries the key, so
    this asserts against a hub that actually holds it rather than against an idea."""
    hub_keys = {row[KEY] for row in spark.read.table(dim_loaded.names.hub).collect()}
    assert C_ZERO in hub_keys, "the fixture no longer carries the hub's real lowest key"
    ghosts = spark.read.table(dim_loaded.table).filter(
        F.col(DIM.surrogate_key) == GHOST_SURROGATE_KEY
    )
    assert ghosts.filter(F.col(KEY) == C_ZERO).count() == 0
    assert _chains(spark, dim_loaded.table)[C_ZERO][0]["razao_social"] == "ZERO KEY LTDA"


def test_the_ghost_carries_no_delivered_attribute_and_names_this_loader(spark, dim_loaded):
    """NULL PAYLOAD RATHER THAN "(unknown)", which is the Kimball convention and is
    refused here: the four payload columns carry values the RFB delivered, and writing
    a string we invented into one of them puts a derived claim where a delivered fact
    belongs. The row is identified instead by the two columns that are OURS -- the
    surrogate key and `record_source` -- which is the same line `opl.vault.columns`
    draws between `data_entrada_sociedade` and `last_observed_on`."""
    ghost = _ghost(spark, dim_loaded.table)
    assert all(ghost[column] is None for column in SAT.payload_columns)
    assert ghost[RECORD_SOURCE] == GHOST_RECORD_SOURCE
    assert ghost[VALID_FROM] == as_collected(spark, VALID_FROM_FLOOR)
    assert ghost[VALID_TO] == as_collected(spark, VALID_TO_CEILING)
    assert ghost[IS_CURRENT] is True


# --- the surrogate key --------------------------------------------------------------


def test_the_surrogate_key_is_unique_over_every_row_including_the_ghost(spark, dim_loaded):
    """A 64-bit hash over 69.2M rows has a birthday collision probability of about
    1.3e-4, which is small and not zero -- and a collision means two dimension versions
    share a key, which is the silent wrong answer a star cannot recover from. The loader
    measures this on the WRITTEN table and refuses; this asserts the measurement is
    total, the ghost included, so a versioned row that hashed to -1 would be caught by
    the same number."""
    frame = spark.read.table(dim_loaded.table)
    assert frame.select(DIM.surrogate_key).distinct().count() == frame.count()
    assert dim_loaded.result.distinct_keys == SOURCE_VERSIONS + 1


def _keys(spark, table) -> set[int]:
    return {row[0] for row in spark.read.table(table).select(DIM.surrogate_key).collect()}


@pytest.mark.parametrize("zone", ["America/Sao_Paulo", "Asia/Tokyo"])
def test_the_surrogate_key_is_the_same_under_every_session_timezone(
    spark, dim_loaded, empresas_bronze, zone
):
    """THE KEY IS A FUNCTION OF THE DATA AND NOT OF A CLUSTER SETTING, which the pin in
    `opl.config.SESSION_TIMEZONE` makes true in practice and this makes true regardless.

    MEASURED BEFORE IT WAS FIXED: the same business key on the same `applied_date` under
    three session zones produced THREE different `company_sk` values, because the key
    hashed `valid_from` -- a TIMESTAMP, i.e. UTC micros, and `DATE -> TIMESTAMP` resolves
    through `spark.sql.session.timeZone`. The fact stores `company_sk`, so that made
    every fact row's target a property of where the dimension happened to be built; and a
    rebuild under a moved zone makes `_refuse_a_target_the_source_has_outgrown` fire
    blaming "a new snapshot in the source", which tells an operator to drop a 69.2M-row
    table for a reason that is not the reason.

    The key hashes `applied_date` instead -- a DATE, which has no zone at all. Pinning
    the session alone would have been the weaker claim: it makes the key stand still
    while nobody moves the setting.

    THE INTERVAL BOUNDS STILL MOVE, and the second assertion REQUIRES them to. Without
    it this test passes just as happily against a key that never stopped moving, because
    a zone that changed nothing proves nothing about a key that ignores it."""
    names = SimpleNamespace(
        hub=dim_loaded.names.hub,
        sat=dim_loaded.names.sat,
        dim=f"{empresas_bronze.db}.dim_tz_{zone.rsplit('/', 1)[1].lower()}",
    )
    pinned = spark.conf.get(SESSION_TIMEZONE_CONFIG)
    assert pinned == SESSION_TIMEZONE, (
        f"the suite's session runs in {pinned!r}, so `dim_loaded` is not the UTC "
        "baseline this test compares against -- opl.spark.local_session no longer pins it"
    )
    try:
        spark.conf.set(SESSION_TIMEZONE_CONFIG, zone)
        build_dimension(spark, names)
    finally:
        spark.conf.set(SESSION_TIMEZONE_CONFIG, pinned)

    def bounds(table) -> set[int]:
        return {
            row[0]
            for row in spark.read.table(table)
            .select(F.unix_micros(F.col(VALID_FROM))).distinct().collect()
        }

    assert _keys(spark, names.dim) == _keys(spark, dim_loaded.table)
    assert bounds(names.dim) != bounds(dim_loaded.table), (
        f"{zone} moved no interval bound at all, so this test would pass unchanged "
        "against a surrogate key that still hashed one"
    )


def _collision_message() -> str:
    return _surrogate_key_collision(
        target_table="workspace.default.dim_company",
        surrogate_key=DIM.surrogate_key,
        rows=69_202_818,
        distinct=69_202_817,
    )


@pytest.mark.parametrize(
    ("cause", "repair"), COLLISION_CAUSES, ids=[f"cause-{n}" for n in (1, 2, 3)]
)
def test_the_collision_refusal_names_every_cause_with_the_repair_that_one_needs(
    cause, repair
):
    """ONE REFUSAL, THREE CAUSES, AND THEY DO NOT SHARE A REPAIR.

    The message used to name a hash collision alone and to close on "the repair is a
    wider key, not a re-run, which would reproduce the same collision". That is right for
    exactly one of the three states that reach it. A duplicate (hash key, business key)
    in the hub makes the join fan out and emit one version twice, and two satellite rows
    on one `applied_date` hash to one key because the key IS (business key,
    `applied_date`) -- both are source defects, both are repaired by fixing the source
    and re-running, and both were being told to widen a key that is not the problem.

    DRIVEN OFF `COLLISION_CAUSES` rather than off three quoted strings, so the causes
    cannot drift out of the message they are rendered into."""
    message = _collision_message()
    assert cause in message and repair in message


def test_the_collision_refusal_no_longer_prescribes_one_repair_for_all_of_them():
    """The regression, in the words that were wrong. Kept as its own assertion because
    the parametrised test above would pass with the old sentence still appended."""
    message = _collision_message()
    assert len(COLLISION_CAUSES) == 3
    assert "the repair is a wider key, not a re-run" not in message
    assert message.index(COLLISION_CAUSES[0][0]) < message.index(COLLISION_CAUSES[2][0])
    assert "ALREADY WRITTEN" in message, "the table on disk is still the operator's problem"


def test_two_builds_of_one_source_produce_the_same_surrogate_keys(
    spark, vault_loaded, empresas_bronze
):
    """DETERMINISM, and it is not a nicety: the fact stores `company_sk`, so a rebuild
    that re-keyed the dimension would leave every fact row pointing at a row that no
    longer means what it did -- silently, because the keys would still resolve.

    `monotonically_increasing_id()` is the obvious generator and this is the property it
    does not have; the key is a hash of (business key, `valid_from`) for exactly this
    reason. The second build is stamped with a DIFFERENT `load_date`, so the test also
    says the key does not depend on when we ran."""
    first = SimpleNamespace(
        hub=vault_loaded.hub, sat=vault_loaded.sat, dim=f"{empresas_bronze.db}.dim_sk_a"
    )
    second = SimpleNamespace(
        hub=vault_loaded.hub, sat=vault_loaded.sat, dim=f"{empresas_bronze.db}.dim_sk_b"
    )
    build_dimension(spark, first)
    build_dimension(spark, second, load_date=REBUILT_AT)

    def keys(table):
        return {row[DIM.surrogate_key] for row in spark.read.table(table).collect()}

    assert keys(first.dim) == keys(second.dim)
    assert len(keys(first.dim)) == SOURCE_VERSIONS + 1


# --- loading twice ------------------------------------------------------------------


def test_a_second_load_of_an_unchanged_source_appends_nothing(spark, gold_target,
                                                              empresas_bronze):
    """IDEMPOTENT, which `max_retries: 0` makes mandatory rather than pleasant: a retry
    on INTERNAL_ERROR is a second run of a task that may already have written, and an
    unguarded append would double a 69.2M-row table with nothing failing."""
    load_vault(spark, empresas_bronze, gold_target)
    first = build_dimension(spark, gold_target)
    second = build_dimension(spark, gold_target, load_date=REBUILT_AT)
    assert first.appended == SOURCE_VERSIONS + 1
    assert (second.appended, second.already_present) == (0, SOURCE_VERSIONS + 1)
    assert spark.read.table(gold_target.dim).count() == SOURCE_VERSIONS + 1


def test_a_hub_that_does_not_cover_its_satellite_is_refused_and_not_reported_as_a_no_op(
    spark, gold_target, empresas_bronze
):
    """THE NUMBER THIS LAYER'S HEADLINE CLAIM RESTS ON, ENFORCED RATHER THAN PRINTED.

    `_bounded` joins the satellite to the hub with `how="inner"`, so a satellite version
    whose hash key matches no hub row is DROPPED -- which is exactly what a hub loaded
    over a narrower window than its satellite produces, and which nothing in the vault
    errors on. Measured before this guard existed: deleting one hub key and loading again
    SUCCEEDED, reporting `appended = 4` against `source_versions = 4`, with one version
    silently gone. `_distinct_surrogate_keys` cannot see it -- it compares distinct keys
    against the WRITTEN row count, never against the expected one -- so 69,202,818 was a
    number the run log observed rather than a number anything enforced.

    THE RE-RUN IS ASSERTED TOO, AND IT IS THE WORSE HALF. A second build over the same
    short hub reproduces the same short chain, so `_refuse_a_target_the_source_has_
    outgrown` finds nothing to revise and `appended` is 0 against a non-zero
    `already_present` -- the idempotent-re-run branch, which printed a clean "the re-run
    is a no-op" line over a permanently short dimension. `max_retries: 0` does not
    prevent a retry, so that branch is reached by accident and not only by choice."""
    load_vault(spark, empresas_bronze, gold_target)
    short_hub = f"{gold_target.hub}_short"
    (
        spark.read.table(gold_target.hub)
        .where(F.col(KEY) != C_JULY_ONLY)
        .write.format("delta").mode("append").saveAsTable(short_hub)
    )
    with pytest.raises(ValueError, match="left_anti"):
        build_dimension(spark, gold_target, hub_table=short_hub)
    written = SOURCE_VERSIONS - VERSIONS_OF[C_JULY_ONLY] + 1
    assert spark.read.table(gold_target.dim).count() == written, (
        "the refusal is AFTER the write, so the short chain is on disk and the message "
        "has to say so"
    )
    with pytest.raises(ValueError, match="left_anti"):
        build_dimension(spark, gold_target, hub_table=short_hub, load_date=REBUILT_AT)
    assert spark.read.table(gold_target.dim).count() == written


def test_a_source_that_gained_a_version_is_refused_before_anything_is_written(
    spark, gold_target, empresas_bronze
):
    """THE HONEST LIMIT OF AN APPEND-ONLY SCD2, refused rather than absorbed.

    When the vault gains a snapshot, the previously-open version's `valid_to` stops
    being the ceiling -- and an append cannot revise a row it already wrote. Appending
    the corrected chain would put TWO intervals on one `company_sk`, so every as-of
    lookup for that company would return two rows. The loader refuses BEFORE its first
    write (master protocol section 4.4) and the message names the repair.

    Built by loading the vault over two months, building the dimension, then loading the
    satellite's third month -- which is the real sequence, not a simulated one."""
    load_vault(spark, empresas_bronze, gold_target, months=(MAY, JUN))
    build_dimension(spark, gold_target, months=(MAY, JUN))
    before = spark.read.table(gold_target.dim).count()
    load_vault(spark, empresas_bronze, gold_target, months=WINDOW)
    with pytest.raises(ValueError, match="cannot revise an interval it already closed"):
        build_dimension(spark, gold_target, months=WINDOW)
    assert spark.read.table(gold_target.dim).count() == before


# --- the window parameter, which is a DECLARATION and not a filter -------------------


def test_a_window_that_is_not_the_snapshots_the_source_holds_is_refused(spark, dim_loaded):
    """THE ONE THING THE `months` JOB PARAMETER CAN HONESTLY DO HERE.

    An SCD2 build cannot be narrowed to a window: `valid_to` for a company's last
    version is the date of its NEXT version, so a load that read only some snapshots
    would close intervals against versions it could not see and report success. So the
    parameter is not a filter -- it is the operator's statement of which snapshots this
    dimension covers, checked against the ones the source actually holds. Its value is
    that a build launched after an unnoticed third snapshot landed REFUSES instead of
    quietly covering it."""
    with pytest.raises(ValueError, match="the snapshots it actually holds"):
        build_dimension(spark, dim_loaded.names, months=(JUN, JUL))
    with pytest.raises(ValueError, match="the snapshots it actually holds"):
        build_dimension(spark, dim_loaded.names, months=(*WINDOW, "2026-08"))


# --- the specs the loader is handed --------------------------------------------------


def test_a_satellite_that_is_not_the_dimensions_declared_source_is_refused(spark, dim_loaded):
    """The loader takes the dimension AND its satellite as two arguments -- so it can be
    driven with a throwaway spec, which is `opl.vault.satellites._resolved_parent`'s
    reason -- and something therefore has to check they belong together. Handed another
    satellite it would build a plausible dimension about a different table."""
    with pytest.raises(ValueError, match="declares source"):
        build_dimension(
            spark, dim_loaded.names, satellite=domains.table_spec("sat_estabelecimento_dados")
        )


def test_a_hub_that_is_not_the_satellites_parent_is_refused(spark, dim_loaded):
    """The business key would be another hub's, so the join would return nothing and the
    dimension would be empty but for its ghost -- with the load reporting success."""
    with pytest.raises(ValueError, match="hangs off hub"):
        build_dimension(
            spark, dim_loaded.names, hub=domains.table_spec("hub_estabelecimento")
        )


# --- the shape of the table ----------------------------------------------------------


def test_the_columns_are_the_surrogate_the_hubs_key_and_the_satellites_payload(
    spark, dim_loaded
):
    """THE COLUMN LIST, PINNED, and derived from the two vault specs rather than
    restated -- so a satellite that gains a payload column gains it here, and a
    dimension that quietly dropped one goes red.

    The ORDER is pinned too, for `opl.vault.satellites._in_column_order`'s reason: a
    Delta append is POSITIONAL unless `mergeSchema` says otherwise, so two builds
    projecting the same columns in two orders would write each other's values without
    failing."""
    frame = spark.read.table(dim_loaded.table)
    assert frame.columns == [
        DIM.surrogate_key,
        *HUB.business_key_columns,
        *SAT.payload_columns,
        VALID_FROM,
        VALID_TO,
        IS_CURRENT,
        LOAD_DATE,
        RECORD_SOURCE,
    ]
    types = dict(frame.dtypes)
    assert types[DIM.surrogate_key] == "bigint"
    assert types[VALID_FROM] == types[VALID_TO] == "timestamp"
    assert types[IS_CURRENT] == "boolean"


def test_the_load_stamps_the_build_time_it_was_given_and_not_a_clock(spark, dim_loaded):
    """`load_date` is an argument with no default, for `opl.vault.hubs`' reason: a
    loader that stamps its own clock can only be checked against another clock reading,
    and in the data it makes the LDTS a record of when the pipeline happened to run
    rather than a value the job's own parameters pin.

    Compared through `as_collected` for the reason every instant here is: `load_date`
    goes through `instant_literal` like every other timestamp this loader writes, so it
    is parsed in the SESSION zone while `collect()` converts it back through the
    operating system's."""
    stamps = {row[LOAD_DATE] for row in _rows(spark, dim_loaded.table)}
    assert stamps == {as_collected(spark, BUILT_AT)}
