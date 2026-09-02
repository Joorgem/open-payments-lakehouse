"""`sat_link_payment` against real Spark: the vault's first DESCRIPTIVE satellite on a
LINK, and its first `applied_date` that does not come from `_snapshot_ref_date`.

WHAT IS BEING SHOWN, AND WHAT IS NOT. Every number below is over the synthesised bronze
fixture `conftest` builds for `test_payments_vault.py` -- the SAME five rows, read a
second time as the case for the satellite rather than for the link. Nothing here is a
claim about a Databricks run: F2 wave 2 cannot launch one (the workspace 403s on any NEW
job), so this file is the whole of the evidence that the mechanism fires, and it says so
rather than borrowing a run's authority.

THE THREE THINGS THIS TABLE IS THE FIRST OF, one section per class:

  1. A `Satellite` WHOSE PARENT IS A `Link`. `load_satellite` took `hub: Hub` and the
     registry refused the shape in those exact terms -- "the guard and that signature
     have to change together". Both moved in one task. What must NOT have moved is
     everything else the guard refused, which `tests/vault/test_registry.py` drives.
  2. AN `applied_date` READ FROM A CONTRACT COLUMN. Every satellite before it built the
     column from `_snapshot_ref_date`, and **`bronze_payments` does not have that
     column** -- `add_common_audit_columns` omits it for a generated source, deliberately.
     So the source is DECLARED, and the fixture omits the column too: a fixture carrying
     it would have let the old unconditional projection pass here and fail in production.
  3. A SATELLITE WITH NO OBSERVATION LEDGER. A payment is an event, so every key of every
     earlier month is `absent_after_observation` in this one by construction and a
     departure count would be a candidate delete per payment. What the table does NOT
     lose is the window guard, which is the second of the two things the ledger really
     buys; it reaches it over ONE table instead of two.

THE FOLD IS LIVE HERE AND IT IS MEASURED AT ZERO ON EMPRESAS. `t-0001` is delivered in
June and redelivered byte-identical in July -- "the SAME payment seen twice", in the
contract's words -- and both deliveries carry the same `transaction_id` and the same
`event_time`, so both land on ONE (link hash key, applied_date) and the satellite folds
them. That is one row of `collapsed_duplicates` on a five-row fixture, where the vault's
first real run reported 0 over 69M empresas rows.

WHAT THIS LOADER DOES NOT CHECK, said plainly rather than left to be discovered: it does
not require the LINK to have been loaded. Neither does `load_satellite` require its hub
to exist, and for the same reason -- the hash key is COMPUTED from the source, so the
digests agree with `load_link`'s by construction rather than by ordering. What ordering
buys is that the rows point at link rows that are there, and the guard for that is
`links.refuse_unloaded_hubs`, which lives in the LINK loader. A satellite written before
its link is a dangling reference on an insert-only pair, exactly as it is for a hub, and
it is a gap this task did not close because closing it for a link alone would be half a
decision."""
from __future__ import annotations

import re
from datetime import date
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pyspark.sql import functions as F

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.bronze.snapshot import SNAPSHOT_REF_DATE_COLUMN
from opl.bronze.snapshot_axis import (
    INSTANT_PATTERN,
    INSTANT_WIDTH,
    MONTHLY_SNAPSHOT,
    _is_instant,
)
from opl.contracts import payments as payments_contract
from opl.contracts.catalogue import columns_for
from opl.dataops.cadence import declares_source_date
from opl.generator.instants import to_text
from opl.gold.conformed import day_of
from opl.vault import domains
from opl.vault.columns import APPLIED_DATE, HASH_DIFF, LOAD_DATE, RECORD_SOURCE
from opl.vault.domains.cnpj import EMPRESA_GRAIN, HUB_EMPRESA
from opl.vault.hubs import load_hub
from opl.vault.links import load_link
from opl.vault.loading import applied_date_expression
from opl.vault.observation import ObservationGrain
from opl.vault.satellites import SatelliteLoadResult, load_satellite
from opl.vault.specs import (
    READS_DATE,
    READS_ISO_TEXT,
    SNAPSHOT_REF_DATE,
    AppliedDateSource,
    Satellite,
)

from .conftest import (
    JUL,
    JUL_EVENT,
    JUN,
    JUN_EVENT,
    LOADED_AT,
    PAYMENTS_AUDIT_DDL,
    PAYMENTS_SPEC,
    derived_table,
)

SAT = domains.table_spec("sat_link_payment")
LINK = domains.table_spec("link_payment")
LINK_HUBS = domains.linked_hubs(LINK)

# The two days the fixture's five payments happened on. `t-0001` appears TWICE in
# bronze -- June and July -- with ONE `event_time`, which is what makes it one
# satellite row and one collapsed duplicate: three payments on 2026-06-01 and one on
# 2026-07-02.
JUN_DAY, JUL_DAY = date(2026, 6, 1), date(2026, 7, 2)


@pytest.fixture
def sat_target(payments_source):
    """Fresh table names per test, for the tests that WRITE -- sharing one would make
    idempotence pass for the wrong reason."""
    db, suffix = payments_source.db, uuid4().hex[:8]
    return SimpleNamespace(db=db, sat=f"{db}.sat_{suffix}")


def load_payment_satellite(spark, source, target, *, months=None, diagnostics=False):
    """One `sat_link_payment` load over `months`, through the real loader.

    THE AXIS IS PASSED AND THE GRAIN IS NOT, which is this table's whole shape: a
    TRANSACTIONAL satellite has no ledger, so nothing else in the call spells the column
    the window reads and the parameter is the only spelling rather than a second one.
    `MONTHLY_SNAPSHOT` is what the payments `BronzeTable` declares -- read off the
    registry in the assertion below rather than trusted here."""
    return load_satellite(
        spark, SAT, link=LINK, hubs=LINK_HUBS,
        source_table=source.bronze, target_table=target.sat,
        load_date=LOADED_AT, axis=MONTHLY_SNAPSHOT, months=months,
        report_diagnostics=diagnostics,
    )


def load_hub_and_link(spark, source, target):
    """`hub_empresa` then `link_payment`, which is the order `load_link`'s own preflight
    requires -- its references are COMPUTED rather than joined, so a link written before
    its hub dangles on an insert-only table."""
    load_hub(
        spark, HUB_EMPRESA, source_table=source.empresas, target_table=target.hub,
        load_date=LOADED_AT,
    )
    load_link(
        spark, LINK, hubs=LINK_HUBS, hub_tables={HUB_EMPRESA.name: target.hub},
        source_table=source.bronze, target_table=target.link, load_date=LOADED_AT,
    )


@pytest.fixture(scope="module")
def satellite_loaded(spark, payments_source):
    """One load over both months, shared by every read-only assertion."""
    target = SimpleNamespace(db=payments_source.db, sat=f"{payments_source.db}.sat_shared")
    result = load_payment_satellite(spark, payments_source, target)
    return SimpleNamespace(sat=target.sat, result=result)


# --------------------------------------------------------------------------- #
# The spec. No Spark: a declaration this file can read in milliseconds.
# --------------------------------------------------------------------------- #


def test_the_satellite_carries_the_measures_and_derives_them_from_the_contract():
    """`amount`, `currency`, `payment_method` -- and NOT the two counterparties.

    THE SUBTRACTION IS ASSERTED AS A SUBTRACTION, not as a literal tuple, which is the
    whole reason the domain writes it as one. `BUSINESS_ATTRIBUTE_COLUMNS` is the
    contract's own "what the payment WAS, as opposed to which delivery of it this row
    is"; the two counterparties are already in the link as roled hub references, so
    carrying them again would put one fact in two tables at two grains. A hand-written
    payload would keep passing on the day a sixth business attribute is declared --
    exactly the drift `opl.contracts.payments._assert_the_columns_partition_cleanly` refuses
    at import, by holding the counterparties a SUBSET of the attributes.

    ORDER IS THE CONTRACT'S AND IT IS LOAD-BEARING: `_in_column_order` writes the payload
    in declaration order and a Delta `mode("append")` matches by POSITION, so two loads
    building these columns in two orders would write `currency` into `amount`."""
    assert SAT.payload_columns == ("amount", "currency", "payment_method")
    assert SAT.payload_columns == tuple(
        column
        for column in payments_contract.BUSINESS_ATTRIBUTE_COLUMNS
        if column not in payments_contract.COUNTERPARTY_COLUMNS
    )
    assert set(SAT.payload_columns).isdisjoint(payments_contract.COUNTERPARTY_COLUMNS)
    assert set(SAT.payload_columns) <= set(payments_contract.BUSINESS_ATTRIBUTE_COLUMNS)


def test_bronze_payments_has_no_snapshot_ref_date_which_is_why_the_source_is_declared():
    """THE MEASUREMENT THIS WHOLE TASK TURNED ON, driven rather than quoted.

    Every satellite before this one built `applied_date` from `_snapshot_ref_date`
    unconditionally, and `bronze_payments` does not have that column: it is stamped by
    `add_audit_columns` for a FILE-FED source and omitted by `add_common_audit_columns`
    for a generated one, deliberately, because stamping an all-NULL column would have
    forced the payments DQ set to drop `unprovable_snapshot_ref_date` -- a control left
    out so the value it refuses can be written.

    ASKED THROUGH `opl.dataops.cadence.declares_source_date`, which is this repository's
    one spelling of the question and reads it off `landing` and `snapshot_axis` -- the
    two fields that pick between the three audit stamps. A test that listed the columns
    itself would be a fourth place the answer lives. The fixture is asserted too, because
    a fixture carrying the column would make every `applied_date` assertion below
    VACUOUS: the pre-F2-wave-2 projection would have found it here."""
    assert not declares_source_date(PAYMENTS_SPEC)
    assert declares_source_date(bronze_table_spec("empresas"))
    assert SNAPSHOT_REF_DATE_COLUMN not in columns_for(payments_contract.CONTRACT)
    assert SNAPSHOT_REF_DATE_COLUMN not in PAYMENTS_AUDIT_DDL


def test_the_satellite_reads_its_applied_date_from_the_payments_own_event_time():
    """The declaration, off the contract rather than as a literal, and the reader it
    carries beside the column name.

    THE READER IS HALF THE DECLARATION. `_snapshot_ref_date` is already a `date` and
    `event_time` is ISO-8601 TEXT, so a column name alone cannot say how to get a day out
    of it -- which is `opl.bronze.snapshot_axis.SnapshotAxis`' shape ("a column plus the
    rule for what its values look like") applied to a different question about the same
    row."""
    assert SAT.applied_date_from.column == payments_contract.EVENT_TIME_COLUMN
    assert SAT.applied_date_from.reads == READS_ISO_TEXT
    assert SAT.transactional is True
    assert PAYMENTS_SPEC.snapshot_axis == MONTHLY_SNAPSHOT


def test_ref_date_from_instant_cannot_read_event_time_so_golds_own_rule_is_used():
    """THE FUNCTION THAT LOOKS LIKE THE ANSWER AND IS NOT, measured rather than assumed.

    `opl.bronze.snapshot.ref_date_from_instant` does instant -> date and was the obvious
    thing to reuse. It pins `opl.bronze.snapshot_axis.INSTANT_PATTERN` -- twenty-seven
    characters, SIX fractional digits -- because that is what a Postgres extraction
    renders. The payment stream's `event_time` is what `opl.generator.instants.to_text`
    emits: twenty-four characters, THREE fractional digits. It fails the width check AND
    the pattern, so that function would have returned NULL for every payment row and
    `changed_rows` would have ordered the whole version chain on an all-NULL column.

    NOTHING WOULD HAVE FAILED, WHICH IS WHY THIS IS A TEST AND NOT A COMMENT. And the
    fixture would not have caught it either: T1's `_PAYMENT_DEFAULTS` wrote a
    twenty-seven-character `event_time`, a rendering this producer never emits and which
    that function accepts. The last assertion is what stops that returning."""
    sample = to_text(1780315200000)

    assert len(sample) == 24
    assert len(sample) != INSTANT_WIDTH
    assert not _is_instant(sample)
    assert re.match(INSTANT_PATTERN, sample) is None
    assert JUN_EVENT == sample


def test_the_default_applied_date_source_is_bronzes_column_and_the_rfb_satellites_use_it():
    """THE PROPERTY THAT MAKES THE FIELD FREE TO ADD, asserted rather than argued: the
    four satellites that shipped before it read exactly what they read before.

    `AppliedDateSource`'s column is a SECOND SPELLING of
    `opl.bronze.snapshot.SNAPSHOT_REF_DATE_COLUMN` -- `opl.vault.specs` may not import
    that module, which is Spark `Column` expressions and nothing else, while this vault's
    spec module must import where pyspark is not installed. The duplicate is turned into
    a cross-check here, which is `opl.vault.loading.BRONZE_RECORD_SOURCE`'s own idiom."""
    assert SNAPSHOT_REF_DATE.column == SNAPSHOT_REF_DATE_COLUMN
    assert SNAPSHOT_REF_DATE.reads == READS_DATE

    others = [
        spec for spec in domains.REGISTRY.values()
        if isinstance(spec, Satellite) and spec.name != SAT.name
    ]
    assert len(others) == 4
    assert all(spec.applied_date_from == SNAPSHOT_REF_DATE for spec in others)
    assert not any(spec.transactional for spec in others)


def test_the_satellite_resolves_to_its_link_and_parent_hub_still_refuses_it():
    """`parent_of` is the resolution `load_satellite` needs; `parent_hub` is the narrower
    one and must keep refusing.

    THE SECOND HALF IS WHAT KEEPS THE WIDENING FROM REACHING GOLD.
    `opl.gold.registry_guards` calls `parent_hub` for every SCD2 dimension's source
    satellite and reads `business_key_columns` off the answer; a `parent_hub` widened to
    return `Hub | Link` would have handed it a `Link` and produced an `AttributeError`
    several frames from the declaration that caused it. The refusal's own MESSAGE is
    asserted, not just its type, because "'link_payment' is not a hub" -- what it said
    before this task -- names neither the consequence nor the alternative."""
    assert domains.parent_of(SAT) is LINK
    with pytest.raises(ValueError, match="satellite on a LINK"):
        domains.parent_hub(SAT)


# --------------------------------------------------------------------------- #
# The load, against real Spark
# --------------------------------------------------------------------------- #


def test_the_satellite_writes_the_measures_after_the_links_hash_key_and_the_metadata(
    spark, satellite_loaded
):
    """THE PROJECTION, and the column ORDER is the assertion rather than the set.

    `mode("append")` matches by POSITION on a table that already exists, so two loads
    building these columns in two orders would write `currency` into `amount` without
    failing. The hash key is the LINK's, which is the whole of what changed here: the
    same `_in_column_order` that writes `hub_empresa_hk` for `sat_empresa_dados` writes
    `link_payment_hk` for this one, and nothing else about the projection differs.

    AND THERE IS NO END-DATE COLUMN, which is the property the descriptive satellite
    holds whatever its parent: a delta-driven satellite cannot tell "unchanged" from "not
    observed", so it has no column in which to claim a close."""
    written = spark.read.table(satellite_loaded.sat).columns

    assert written == [
        LINK.hash_key,
        LOAD_DATE,
        APPLIED_DATE,
        RECORD_SOURCE,
        HASH_DIFF,
        "amount",
        "currency",
        "payment_method",
    ]
    assert "valid_to" not in written
    assert "end_date" not in written


def test_every_satellite_row_is_keyed_on_a_link_row_that_exists(
    spark, payments_source, payments_target, sat_target
):
    """THE JOIN-SAFETY PROPERTY, AND THE ONE A SECOND SPELLING OF THE DIGEST WOULD LOSE
    WITHOUT FAILING.

    The satellite computes its hash key with `link_hash_key_expression` over the same
    identifying ends and the same dependent-child key the LINK loader used, so the two
    agree by construction rather than by ordering. A second spelling anywhere on that
    path gives a satellite whose every join to its link returns nothing -- no error, no
    row-count anomaly on either side, and every query simply reporting that the payment
    has no measures.

    ASSERTED BY SET EQUALITY IN BOTH DIRECTIONS rather than by a join count, because a
    join of the wrong digests to an empty result and a join nobody performed report the
    same number: zero unmatched."""
    load_hub_and_link(spark, payments_source, payments_target)
    load_payment_satellite(spark, payments_source, sat_target)

    link_keys = {
        row[LINK.hash_key]
        for row in spark.read.table(payments_target.link).collect()
    }
    sat_keys = {
        row[LINK.hash_key] for row in spark.read.table(sat_target.sat).collect()
    }

    assert len(link_keys) == 4
    assert sat_keys == link_keys


def test_the_applied_date_is_the_payments_own_event_day_and_is_a_date(
    spark, satellite_loaded
):
    """THE COLUMN THIS TASK EXISTS TO GET RIGHT, in value AND in type.

    IN VALUE: the day is the payment's own `event_time`, not the month it was ingested
    in. `t-0001` is delivered in June and redelivered in July, and its `applied_date` is
    2026-06-01 in both -- the day the payment HAPPENED. An `applied_date` taken from
    `_snapshot_month` would have made the redelivery a second, later version of a payment
    that never changed.

    IN TYPE: a `date`, like every other satellite's. `READS_DATE` over this string column
    would have passed every other check and left `applied_date` a STRING -- the ordering
    axis of the version chain, ordered lexicographically over a rendering, and a column
    whose type disagrees with the four satellites already on disk."""
    frame = spark.read.table(satellite_loaded.sat)
    days = sorted(row[APPLIED_DATE] for row in frame.collect())

    assert len(days) == 4
    assert days == [JUN_DAY, JUN_DAY, JUN_DAY, JUL_DAY]
    assert dict(frame.dtypes)[APPLIED_DATE] == "date"


def test_a_redelivered_payment_is_one_satellite_row_and_is_counted_as_a_fold(
    spark, payments_source, sat_target
):
    """THE FOLD, WHICH IS LIVE HERE AND MEASURED AT ZERO ON EMPRESAS.

    `t-0001` is in June's bronze and again in July's, byte-identical -- "the SAME payment
    seen twice" in the contract's words. Both deliveries carry one `transaction_id` and
    one `event_time`, so both land on ONE (link hash key, `applied_date`) and
    `satellite_candidates`' `min`-over-a-struct picks one. FIVE source rows, FOUR
    candidates, ONE collapsed duplicate.

    THE COUNT IS WHY `collapsed_duplicates` SURVIVED THE LEDGER BECOMING OPTIONAL. The
    module docstring prices this fold at "a payload discarded silently, and a later
    re-load cannot correct the choice"; on this table it is not hypothetical, and a
    transactional satellite that reported no fold count would have hidden the one number
    an operator has no other way to get."""
    result = load_payment_satellite(spark, payments_source, sat_target, diagnostics=True)

    assert result.appended == 4
    assert spark.read.table(payments_source.bronze).count() == 5
    assert result.collapsed_duplicates == 1
    ids = {
        row[LINK.hash_key] for row in spark.read.table(sat_target.sat).collect()
    }
    assert len(ids) == 4


def test_the_load_reports_no_ledger_and_that_is_not_the_same_as_not_looking(
    spark, payments_source, sat_target
):
    """`None` FOR THE DEPARTURE COUNT, ON A RUN THAT WAS ASKED FOR DIAGNOSTICS -- which is
    a state `SatelliteLoadResult` could not express before this task and which it now
    carries as its own field.

    `ledger_derived=False` says there was nothing to look at; `collapsed_duplicates=None`
    would say nobody looked. Reading one off the other is exactly the confusion the pair
    rule exists to prevent, which is why the flag is a field rather than an inference.
    The `__post_init__` arm below is the other direction: a ledgerless load may not name a
    departure count at all, because there is no ledger it could have come from."""
    result = load_payment_satellite(spark, payments_source, sat_target, diagnostics=True)

    assert result.ledger_derived is False
    assert result.candidate_departures is None
    assert result.collapsed_duplicates == 1

    with pytest.raises(ValueError, match="derived none"):
        SatelliteLoadResult(
            table="sat_link_payment", appended=1, already_present=0,
            collapsed_duplicates=1, candidate_departures=0, ledger_derived=False,
        )


def test_a_reload_appends_nothing_and_a_window_narrows_to_that_months_payments(
    spark, payments_source, sat_target
):
    """IDEMPOTENCE AND THE WINDOW, in one test because the second is what makes the first
    non-trivial.

    JUNE holds three payments; JULY holds two, one of which is `t-0001` redelivered. A
    July load after a June one appends ONE row, not two -- the redelivery's (hash key,
    `applied_date`) is already persisted and `changed_rows` drops it before the window,
    which is `loading._without_persisted`'s whole subject. Then a full re-run appends
    nothing at all.

    `already_present` IS READ OFF THE TARGET'S ROW COUNT BEFORE THE WRITE, so it is what
    LANDED rather than what was planned."""
    june = load_payment_satellite(spark, payments_source, sat_target, months=[JUN])
    assert (june.appended, june.already_present) == (3, 0)

    july = load_payment_satellite(spark, payments_source, sat_target, months=[JUL])
    assert (july.appended, july.already_present) == (1, 3)

    again = load_payment_satellite(spark, payments_source, sat_target)
    assert (again.appended, again.already_present) == (0, 4)


def test_a_month_the_source_never_loaded_is_refused_before_anything_is_written(
    spark, payments_source, sat_target
):
    """THE GUARD A TRANSACTIONAL SATELLITE WOULD OTHERWISE HAVE LOST WITH ITS LEDGER.

    `observation._window` refuses a month with no row on either side, and
    `opl.vault.satellites`' own docstring calls that one of the two real things
    consulting the ledger buys. This table derives no ledger -- so without a replacement
    the guard would simply be gone for it, and `months=['2026-09']` would select no row,
    write nothing, and report success. That is the failure this layer is least able to
    notice: a vault table that gained no rows looks exactly like one that had nothing to
    gain.

    THE RULE IS THE LEDGER'S OWN, SPELLED ONCE. `opl.vault.months.refuse_unloaded_months`
    is called by both, over two tables there and one here; what differs is the
    CONSEQUENCE, which is a parameter for `validated_months`' reason. The assertion is on
    the month AND on the consequence text, because a refusal that fired for the right
    input with the ledger's message would mean the two had been fused rather than
    shared."""
    with pytest.raises(ValueError, match="2026-09.*write nothing and report"):
        load_payment_satellite(
            spark, payments_source, sat_target, months=[JUN, JUL, "2026-09"]
        )

    assert not spark.catalog.tableExists(sat_target.sat)


# --------------------------------------------------------------------------- #
# The refusals: every argument this loader takes free, and both directions of each.
# --------------------------------------------------------------------------- #


def test_a_grain_handed_to_a_transactional_satellite_is_refused(
    spark, payments_source, sat_target
):
    """THE FIRST DIRECTION OF THE PAIRING, and the one that keeps the ledger from being
    reattached to a table it means nothing for.

    A payment is an EVENT: every key of every earlier month is absent from this one by
    construction, so the ledger would report a candidate delete per payment. Measured on
    this fixture, a link-grain ledger over both months calls two of four keys
    `absent_after_observation` for a stream in which nothing departed."""
    grain = ObservationGrain(
        name="link_payment", bronze_table=payments_source.bronze,
        quarantine_table=payments_source.bronze,
        key_columns=domains.link_identity_columns(LINK),
    )

    with pytest.raises(ValueError, match="candidate delete per event"):
        load_satellite(
            spark, SAT, link=LINK, hubs=LINK_HUBS,
            source_table=payments_source.bronze, target_table=sat_target.sat,
            load_date=LOADED_AT, grain=grain,
        )

    assert not spark.catalog.tableExists(sat_target.sat)


def test_a_state_satellite_handed_no_grain_is_refused_which_is_the_other_direction(
    spark, payments_source, sat_target
):
    """THE ARM THAT KEEPS `transactional` FROM BEING A WAY TO SWITCH A LEDGER OFF.

    Widening a refusal is how a guard stops being able to fail, and making an argument
    optional is a widening. So the loader refuses a satellite that does NOT declare
    itself transactional and arrives without a grain, as loudly as it refuses the
    opposite -- and the message names what the ledger was providing rather than saying
    the argument is missing.

    DRIVEN ON A THROWAWAY SPEC, because `build_registry` refuses this pairing at import
    and no registered table can reach it -- which is the point of the loader taking its
    parent as a free argument."""
    state = Satellite(
        name="sat_link_payment", parent=LINK.name, payload_columns=("amount",),
        applied_date_from=SAT.applied_date_from,
    )

    with pytest.raises(ValueError, match="observation._window"):
        load_satellite(
            spark, state, link=LINK, hubs=LINK_HUBS,
            source_table=payments_source.bronze, target_table=sat_target.sat,
            load_date=LOADED_AT, axis=MONTHLY_SNAPSHOT,
        )

    assert not spark.catalog.tableExists(sat_target.sat)


def test_a_state_satellite_on_a_link_with_a_grain_is_refused_naming_the_missing_check(
    spark, payments_source, sat_target
):
    """THE THIRD ARM OF THE SAME DECISION, and the only one reachable with a grain in hand.

    A satellite that is not transactional and hangs off a LINK would need its ledger keyed
    on the LINK's identity columns and read through the link's own prefixes.
    `opl.vault.effectivity._refuse_a_mismatched_link_grain` is the comparison that checks
    that, `load_satellite` does not call it, and without this arm the load would reach
    `_grain_key_mismatch` -- which reads `business_key_columns` off its parent and gets an
    `AttributeError` on a `Link`, several frames from the declaration that caused it.

    `build_registry` REFUSES THE PAIRING AT IMPORT so no registered table can reach here;
    this drives it through the free argument that exists so a throwaway spec can."""
    state = Satellite(
        name="sat_link_payment", parent=LINK.name, payload_columns=("amount",),
        applied_date_from=SAT.applied_date_from,
    )
    grain = ObservationGrain(
        name="link_payment", bronze_table=payments_source.bronze,
        quarantine_table=payments_source.bronze,
        key_columns=domains.link_identity_columns(LINK),
    )

    with pytest.raises(ValueError, match="_refuse_a_mismatched_link_grain"):
        load_satellite(
            spark, state, link=LINK, hubs=LINK_HUBS,
            source_table=payments_source.bronze, target_table=sat_target.sat,
            load_date=LOADED_AT, grain=grain,
        )


def test_an_event_satellite_on_a_hub_is_refused_at_the_loader_and_not_only_at_import(
    spark, payments_source, sat_target
):
    """THE MIRROR OF THE TEST ABOVE, MISSING UNTIL THIS PHASE'S CORRECTION ROUND -- which
    made the claim beside it false in the half that mattered.

    `opl.vault.specs.Satellite` said `transactional` "cannot become a switch" because a hub
    parent refuses `transactional=True` and a link parent refuses its absence. True of
    `build_registry`, FALSE of the loader: `snapshot_axis_for` branched on the flag FIRST
    and delegated to `_transactional_axis`, which does not take `parent` at all -- so this
    exact spec LOADED, with `ledger_derived=False`, no `candidate_departures`, no
    `_refuse_a_mismatched_grain`, and nothing raised. The review demonstrated it by
    construction on this fixture and counted 3 rows written; this is that construction, and
    deleting the guard makes this fail with `DID NOT RAISE` rather than with a wrong
    message, which is the only shape that proves the branch is what refuses.

    ON A THROWAWAY SPEC, for the reason the link-parent test above gives: `build_registry`
    refuses the pairing at import and `_resolved_parent` pins the parent by NAME, so no
    registered table can reach it. The parent is a free argument precisely so a throwaway
    one can, and the refusal names the registry guard that owns the argument."""
    events = Satellite(
        name="sat_empresa_events", parent=HUB_EMPRESA.name,
        payload_columns=("cnpj_basico",), transactional=True,
    )

    with pytest.raises(ValueError, match="_refuse_a_transactionality_the_parent"):
        load_satellite(
            spark, events, hub=HUB_EMPRESA,
            source_table=payments_source.empresas, target_table=sat_target.sat,
            load_date=LOADED_AT, axis=MONTHLY_SNAPSHOT,
        )

    assert not spark.catalog.tableExists(sat_target.sat)


def test_a_transactional_satellite_with_neither_grain_nor_axis_is_refused(
    spark, payments_source, sat_target
):
    """The window still has to be applied to a COLUMN, and with no grain to read it off
    there is nothing else in the call that names one.

    `load_satellite`'s comment block used to say an `axis=` argument would be "a second
    spelling of one decision"; that is true only while a grain is required. With none,
    the parameter is the ONLY spelling, and its absence has to be refused rather than
    defaulted -- a default would be `MONTHLY_SNAPSHOT`, which is right for this source
    and silently wrong for the first one observed at instants."""
    with pytest.raises(ValueError, match="neither a grain nor an axis"):
        load_satellite(
            spark, SAT, link=LINK, hubs=LINK_HUBS,
            source_table=payments_source.bronze, target_table=sat_target.sat,
            load_date=LOADED_AT,
        )

    assert not spark.catalog.tableExists(sat_target.sat)


def test_a_grain_and_an_axis_together_are_refused_for_the_original_reason(
    spark, payments_source, sat_target
):
    """The half of the old argument that survives: where a grain IS required it already
    carries the source's axis and has been pinned to this source table, so a second one
    is a second spelling whose disagreement lands as a window that silently selected
    nothing.

    Driven on a HUB satellite, because that is the shape a grain belongs to."""
    sat = domains.table_spec("sat_empresa_dados")

    with pytest.raises(ValueError, match="second spelling of"):
        load_satellite(
            spark, sat, hub=HUB_EMPRESA,
            source_table=payments_source.bronze, target_table=sat_target.sat,
            load_date=LOADED_AT, grain=EMPRESA_GRAIN, axis=MONTHLY_SNAPSHOT,
        )

    assert not spark.catalog.tableExists(sat_target.sat)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    (
        ({}, "Exactly one is the parent"),
        ({"hub": HUB_EMPRESA, "link": LINK}, "Exactly one is the parent"),
        ({"hub": HUB_EMPRESA, "hubs": LINK_HUBS}, "a hub has no ends"),
        ({"link": LINK}, "joins .* and was handed"),
    ),
)
def test_the_parent_pair_is_refused_every_way_it_can_be_wrong(
    spark, payments_source, sat_target, kwargs, message
):
    """FOUR SHAPES, AND EACH ONE IS A CALLER WHO RESOLVED SOMETHING AND PASSED THE WRONG
    HALF OF IT.

    Neither/both: a satellite keys on ONE table's hash key and neither spelling says
    which. A hub with `hubs`: `hubs` is a LINK's ends' hubs and a hub has no ends, so the
    pair means a link was resolved somewhere. A link with no `hubs`: `refuse_mismatched
    _hubs` catches it, which is the same refusal `load_link` uses and is reused rather
    than restated -- the link's own digest concatenates its hubs' business keys IN ORDER,
    so a wrong or reordered list re-keys the satellite while every column stays correct.

    THIS IS THE COST OF FREE ARGUMENTS AND IT IS PAID DELIBERATELY: a loader that
    resolved its parent through the module-level registry could not be tested against a
    throwaway spec, and the registry is the thing a new domain must extend without this
    file changing."""
    with pytest.raises(ValueError, match=message):
        load_satellite(
            spark, SAT, source_table=payments_source.bronze,
            target_table=sat_target.sat, load_date=LOADED_AT,
            axis=MONTHLY_SNAPSHOT, **kwargs,
        )

    assert not spark.catalog.tableExists(sat_target.sat)


def test_a_satellite_handed_a_parent_it_does_not_declare_is_refused(
    spark, payments_source, sat_target
):
    """The satellite and its parent are two arguments, so something has to check they
    belong together. Keyed on the wrong table's digest, the satellite joins to nothing --
    silently, and reports success doing it."""
    stranger = domains.table_spec("link_company_partner")

    with pytest.raises(ValueError, match="was handed 'link_company_partner'"):
        load_satellite(
            spark, SAT, link=stranger, hubs=domains.linked_hubs(stranger),
            source_table=payments_source.bronze, target_table=sat_target.sat,
            load_date=LOADED_AT, axis=MONTHLY_SNAPSHOT,
        )

    assert not spark.catalog.tableExists(sat_target.sat)


def test_a_source_without_the_declared_applied_date_column_is_refused_by_name(
    spark, payments_source, sat_target
):
    """THE PAIRING THIS TASK MADE POSSIBLE TO GET WRONG, refused where it lands.

    The applied-date column is named by STRING on the satellite and read off the source,
    and it is NOT covered by `refuse_non_string_columns`: `_snapshot_ref_date` is a
    `date`, so being refused as a non-string would be exactly wrong for the default. A
    source that does not carry the declared column is otherwise an `AnalysisException`
    several operators into a plan, naming a column and not the pairing.

    IT IS THE REAL PAIRING TOO, not a hypothetical: `sat_empresa_dados` pointed at
    `bronze_payments` -- one copied job task -- reads `_snapshot_ref_date`, which that
    table does not have. The message says which table has which columns."""
    without = derived_table(
        spark, payments_source.db, "no_event_time",
        spark.read.table(payments_source.bronze).drop(
            payments_contract.EVENT_TIME_COLUMN
        ),
    )

    with pytest.raises(ValueError, match="which .* does not carry"):
        load_satellite(
            spark, SAT, link=LINK, hubs=LINK_HUBS,
            source_table=without, target_table=sat_target.sat,
            load_date=LOADED_AT, axis=MONTHLY_SNAPSHOT,
        )

    assert not spark.catalog.tableExists(sat_target.sat)


@pytest.mark.parametrize("reads", (READS_DATE, READS_ISO_TEXT))
def test_a_reader_the_columns_representation_cannot_support_is_refused(
    spark, payments_source, sat_target, reads
):
    """THE HALF THAT FAILS SILENTLY, and it is `opl.gold.spec_fields`'
    `_assert_the_reader_matches_the_representation` asked one layer down.

    NEITHER DIRECTION RAISES IN SPARK. `substring` over a `date` casts it to text first
    and returns the right ten characters; `F.col` over an ISO string passes the string
    through. What the second produces is a satellite whose `applied_date` is a STRING
    while every other satellite's is a `date` -- and `applied_date` is this loader's
    ORDERING axis, so the version chain would be ordered lexicographically over a
    rendering, and a Delta append onto an existing table matches by POSITION.

    BOTH DIRECTIONS ARE DRIVEN, over the same two columns swapped: `READS_DATE` on
    `event_time` (a string), and `READS_ISO_TEXT` on a `date`. One arm alone would leave
    the other's branch with no producer."""
    column, source = (
        (payments_contract.EVENT_TIME_COLUMN, payments_source.bronze)
        if reads == READS_DATE
        else (
            "event_day",
            derived_table(
                spark, payments_source.db, f"dated_{uuid4().hex[:6]}",
                spark.read.table(payments_source.bronze).withColumn(
                    "event_day",
                    F.to_date(F.substring(payments_contract.EVENT_TIME_COLUMN, 1, 10)),
                ),
            ),
        )
    )
    mismatched = Satellite(
        name=SAT.name, parent=LINK.name, payload_columns=SAT.payload_columns,
        applied_date_from=AppliedDateSource(column=column, reads=reads),
        transactional=True,
    )

    with pytest.raises(ValueError, match="Neither direction raises in Spark"):
        load_satellite(
            spark, mismatched, link=LINK, hubs=LINK_HUBS,
            source_table=source, target_table=sat_target.sat,
            load_date=LOADED_AT, axis=MONTHLY_SNAPSHOT,
        )

    assert not spark.catalog.tableExists(sat_target.sat)


def test_the_vaults_iso_day_reading_is_the_one_the_gold_layer_already_uses(spark):
    """A CROSS-CHECK ON THE ANSWER, WHICH IS THE ONLY KIND AVAILABLE HERE.

    `opl.gold.conformed.day_of` reads the same column on the same rows for
    `fact_payment`'s date key, and argues at length why it takes ten characters of text
    rather than a CAST: a cast resolves the instant in the SESSION timezone, so under
    America/Sao_Paulo a midnight-UTC payment lands on the previous day. For a satellite
    that is worse than for a fact, because `applied_date` is the ORDERING axis of the
    version chain -- a cluster setting would decide which of two payloads is the later.

    THE TWO ARE NOT ONE IMPORT, AND THAT IS A LAYER DECISION. `opl.gold.registry_guards`
    imports `opl.vault.domains`, so the edge runs gold -> vault and importing back would
    be a cycle. What keeps them honest is this assertion -- the same ANSWER on the same
    input, NULL included -- rather than a shared constant, which would have been satisfied
    by two functions that agreed on a name and not on a value."""
    frame = spark.createDataFrame(
        [(JUN_EVENT,), (JUL_EVENT,), ("not-an-instant",)],
        f"{payments_contract.EVENT_TIME_COLUMN} string",
    )

    both = frame.select(
        applied_date_expression(SAT.applied_date_from).alias("vault"),
        day_of(payments_contract.EVENT_TIME_COLUMN).alias("gold"),
    ).collect()

    assert [row["vault"] for row in both] == [row["gold"] for row in both]
    assert [row["vault"] for row in both] == [JUN_DAY, JUL_DAY, None]
