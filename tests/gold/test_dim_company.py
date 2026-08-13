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
    `test_the_ghost_matches_no_business_key_the_hub_holds` and by
    `test_the_ghost_is_not_keyed_on_the_hubs_real_lowest_key`.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F

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
    ask about is one of the six."""
    resolved = (
        spark.read.table(dim_loaded.table)
        .filter(F.col(KEY) == C_THREE_VERSIONS)
        .filter((F.col(VALID_FROM) <= F.lit(instant)) & (F.lit(instant) < F.col(VALID_TO)))
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
        rows = (
            spark.read.table(dim_loaded.table)
            .filter(F.col(KEY) == C_THREE_VERSIONS)
            .filter((F.col(VALID_FROM) <= F.lit(instant)) & (F.lit(instant) < F.col(VALID_TO)))
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
    every surrogate key derived from it, move under a backfill of an earlier snapshot)."""
    chains = _chains(spark, dim_loaded.table)
    assert set(chains) == set(VERSIONS_OF)
    for key, chain in chains.items():
        assert chain[0][VALID_FROM] == VALID_FROM_FLOOR, (
            f"{key}'s first version starts at {chain[0][VALID_FROM]}, not the floor"
        )
    assert chains[C_JULY_ONLY][0][VALID_FROM] == VALID_FROM_FLOOR


def test_the_open_version_of_every_company_ends_at_the_ceiling(spark, dim_loaded):
    chains = _chains(spark, dim_loaded.table)
    for key, chain in chains.items():
        assert chain[-1][VALID_TO] == VALID_TO_CEILING, (
            f"{key}'s open version ends at {chain[-1][VALID_TO]}, not the ceiling"
        )


def test_is_current_is_exactly_the_rows_whose_interval_is_still_open(spark, dim_loaded):
    """`is_current` is DERIVED and therefore able to disagree with what it is derived
    from, which is the whole reason a denormalisation gets a test rather than a
    comment."""
    frame = spark.read.table(dim_loaded.table)
    disagreeing = frame.filter(
        F.col(IS_CURRENT) != (F.col(VALID_TO) == F.lit(VALID_TO_CEILING))
    )
    assert disagreeing.count() == 0
    # One open version per company, and one for the ghost.
    assert frame.filter(F.col(IS_CURRENT)).count() == len(VERSIONS_OF) + 1


# --- T-D: the ghost is unreachable --------------------------------------------------


def test_the_ghost_matches_no_business_key_the_hub_holds(spark, dim_loaded):
    """THE CLOSING TEST FOR THE GHOST, and it is a statement about the JOIN rather than
    about the row: the ghost carries no business key at all, so no as-of lookup keyed on
    `cnpj_basico` can reach it. A fact row that resolves to nothing reaches it as
    `COALESCE(<lookup>, GHOST_SURROGATE_KEY)` at BUILD time, which is a decision the
    fact makes visibly rather than a join that quietly succeeds."""
    ghost = _ghost(spark, dim_loaded.table)
    assert ghost[KEY] is None
    assert ghost[DIM.surrogate_key] == GHOST_SURROGATE_KEY
    hub_keys = {row[KEY] for row in spark.read.table(dim_loaded.names.hub).collect()}
    assert ghost[KEY] not in hub_keys
    assert (
        spark.read.table(dim_loaded.table)
        .filter(F.col(KEY).isNull())
        .join(spark.read.table(dim_loaded.names.hub), on=KEY, how="inner")
        .count()
        == 0
    )


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
    assert ghost[VALID_FROM] == VALID_FROM_FLOOR and ghost[VALID_TO] == VALID_TO_CEILING
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
    driven with a throwaway spec, which is `opl.vault.satellites._refuse_a_mismatched_hub`'s
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
    rather than a value the job's own parameters pin."""
    stamps = {row[LOAD_DATE] for row in _rows(spark, dim_loaded.table)}
    assert stamps == {BUILT_AT}
