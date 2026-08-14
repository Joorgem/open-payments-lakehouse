# tests/test_payment_profiles.py
"""THE DECLARED STREAMS, and every number that is arithmetic on a declaration.

SPLIT OUT OF `tests/test_payment_emit.py` BY F-API TASK 3, AT 889 OF 800 LINES. Protocol
§4.9 and §4.12 say whoever touches a file at the cap splits it first, and this phase added
the fifth profile plus the placement guard's probes to a file that was already 657. The
seam is the one that file's own docstring drew: it is about **what a WRITER puts in a
file** -- the binary write, the newline trap, the read-back, the idempotent refusal -- and
that claim is about bytes whatever produced them. Everything here is about the DECLARATIONS
those bytes come from, and it changes when a profile is added rather than when the emitter
is touched. The two job entry points stayed with the emitter, because what they assert is
that a refusal happens before a session is built.

WHAT MAKES THIS FILE WORTH ITS OWN NAME. Every number below is computable from
`opl.generator.profiles` alone -- no session, no CNPJ pool, no workspace -- which is what
lets the phase publish row counts, defect counts, currency splits and FX resolution splits
BEFORE the run that lands them (master protocol §4.5). A test that needed a run to state
its expectation would not be pinning a prediction; it would be recording an outcome.

NOTHING HERE STARTS SPARK. The generator is pure Python, and the only cost in the file is
the one test that generates a full 10,000-event stream to count its currencies.
"""
from __future__ import annotations

import dataclasses
import inspect
from pathlib import Path

import pytest

from opl.bronze.generated_landing import STREAM_FILE_SUFFIX, filename_for, serialised_bytes
from opl.contracts import payments
from opl.contracts.payments import DRIFT_COLUMN
from opl.generator import profiles as profiles_module
from opl.generator import stream as stream_module
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import (
    LATENESS_WINDOW_MS,
    NO_DEFECTS,
    DefectSpec,
    _require_defects_fit,
    delivered_records,
    drift_positions,
    duplicate_positions,
    late_positions,
)
from opl.generator.instants import from_text, to_text
from opl.generator.profiles import (
    BETWEEN_SNAPSHOTS,
    CROSS_CURRENCY,
    POOL_SEED,
    POOL_SIZE,
    PROFILES,
    SENTINEL_PROFILE,
    StreamProfile,
    profile_for,
)
from opl.generator.stream import StreamSpec

# A pool small enough that the declaration tests are instant. Twenty keys, canonical.
# The profiles' real pool is 1,024 keys out of `hub_empresa` and needs a Spark session;
# nothing asserted here depends on WHICH companies are in it, and the one test that does
# depend on its SIZE builds its own.
_POOL = validated_pool([f"{n:08d}" for n in range(1, 21)])


# --- THE DECLARED PROFILES ---------------------------------------------------------


# The three streams F1b landed, ingested and published evidence about. Named as a
# group because F3 added a fourth profile whose window is deliberately elsewhere, and
# every claim below that is about the SHARED WINDOW is about these three only -- while
# every claim about the shared SHAPE is still about all four.
_F1B_PROFILES = ("clean", "promotable", "drifting")


def test_the_profiles_are_the_ones_declared_today():
    """Golden pin. Every number here is an input to a stream that will be landed in a
    Volume and ingested into bronze, so a change to any of them re-generates a file
    the workspace already holds -- which `emit_stream_file` then refuses. Pinned so
    that is a deliberate act."""
    assert sorted(PROFILES) == [
        "between-snapshots", "clean", "cross-currency", "drifting", "promotable",
    ]
    assert (POOL_SIZE, POOL_SEED) == (1024, 20260812)
    for profile in PROFILES.values():
        assert (profile.event_count, profile.repeat_count) == (10_000, 800)
        assert (profile.event_interval_ms, profile.emission_lag_ms) == (5_000, 1_500)
    for name in _F1B_PROFILES:
        assert PROFILES[name].window_start == "2026-08-01T00:00:00.000Z"

    clean, promotable, drifting = (PROFILES[n] for n in _F1B_PROFILES)
    assert clean.defects == NO_DEFECTS
    assert promotable.defects == DefectSpec(
        duplicate_count=150, late_count=100, late_by_ms=LATENESS_WINDOW_MS
    )
    assert drifting.defects == DefectSpec(drift_from_index=8_000)


def test_the_predicted_row_counts_are_arithmetic_on_the_declaration():
    """PUBLISHABLE BEFORE THE RUN, which is the standing rule about predictions.

    A duplicate adds a row; a legitimate repeat does not (it is one of the
    `event_count`, carrying an earlier event's attributes under its own id); lateness
    reorders and drift widens, and neither adds one. So these four numbers are known
    without a session, a pool or a workspace."""
    assert PROFILES["clean"].delivered_row_count == 10_000
    assert PROFILES["promotable"].delivered_row_count == 10_150
    assert PROFILES["drifting"].delivered_row_count == 10_000
    assert PROFILES["drifting"].drifted_row_count == 2_000
    assert PROFILES["clean"].drifted_row_count == 0
    assert PROFILES["promotable"].drifted_row_count == 0


def test_every_declared_profile_asks_for_defects_its_stream_can_carry():
    """THE TOTAL CHECK THE IMPORT GUARD DELIBERATELY DOES NOT RUN.

    `profiles._assert_every_profile_describes_a_stream_that_can_exist` covers the two
    cross-checks that are free (`_require_defects_fit`) and leaves this one here,
    because each of these three calls digests EVERY candidate position -- nine sorts
    of 10,000 SHA-256 draws, which would be paid on every import of the generator by
    every test that touches it. An expensive total check belongs in a test; the cheap
    half belongs at import.

    Against the SMALLEST legal pool, because none of these bounds involves the pool:
    they compare a defect count against the positions eligible for it, which is a
    property of `event_count` alone."""
    probe = validated_pool(("00000001", "00000002"))
    for profile in PROFILES.values():
        spec = profile.stream_spec(probe)
        assert len(duplicate_positions(spec, profile.defects)) == (
            profile.defects.duplicate_count
        )
        assert len(late_positions(spec, profile.defects)) == profile.defects.late_count
        assert len(drift_positions(spec, profile.defects)) == profile.drifted_row_count


def test_the_declared_profiles_derive_disjoint_identities():
    """WHY EACH PROFILE HAS ITS OWN SEED AND STREAM ID.

    All of them promote into one bronze table, so a shared identity space would make
    `COUNT(*) - COUNT(DISTINCT transaction_id)` over that table count collisions
    BETWEEN streams as if they were injected redeliveries -- the one number F1b's
    duplicate claim is stated in. This quantifies over `PROFILES` rather than over
    three names, so the fourth profile is covered by the same assertion instead of by
    a second one somebody has to remember to write.

    Measured over 24 indices rather than 10,000, and that is a proxy stated as one:
    `stream._transaction_id` is a function of (seed, stream_id, index), so
    disjointness at any index range is evidence about the derivation rather than
    about a sample. Generating three full streams here would cost half a minute for
    the same argument."""
    identities = []
    for profile in PROFILES.values():
        spec = StreamSpec(
            seed=profile.seed,
            stream_id=profile.stream_id,
            event_count=24,
            repeat_count=0,
            window_start=profile.window_start,
            event_interval_ms=profile.event_interval_ms,
            emission_lag_ms=profile.emission_lag_ms,
            cnpj_pool=_POOL,
        )
        identities.append({r["transaction_id"] for r in delivered_records(spec, NO_DEFECTS)})
    assert len(set.union(*identities)) == sum(len(s) for s in identities)


@pytest.mark.parametrize("name", ["", "   ", SENTINEL_PROFILE])
def test_an_absent_profile_is_refused_as_an_absence(name):
    """The sentinel is what the job YAML defaults to, so reaching it means the run was
    launched without `--params profile=...`. Refused as an ABSENCE rather than as an
    unknown name, the way `require_month` treats `SENTINEL_MONTH`: the message names
    the missing parameter instead of explaining what a profile is."""
    with pytest.raises(ValueError, match="no profile was given"):
        profile_for(name)


def test_an_unknown_profile_is_refused_naming_the_declared_ones():
    with pytest.raises(ValueError, match="unknown stream profile") as excinfo:
        profile_for("promotible")  # a real typo
    assert "promotable" in str(excinfo.value)


def test_the_drifting_profile_actually_drifts_and_the_others_do_not():
    """The claim the whole `drifting` profile exists to make, checked on a proxy
    stream so it costs nothing: the drift column appears from the declared index
    onward and NOWHERE before it.

    Not a restatement of `drift_positions`: this reads the rendered records, which is
    what Auto Loader will meet, and asserts the pre-drift population is entirely
    without the key -- the population a `COUNT(DISTINCT ..., payment_channel)` would
    silently drop."""
    spec = StreamSpec(
        seed=PROFILES["drifting"].seed,
        stream_id=PROFILES["drifting"].stream_id,
        event_count=100,
        repeat_count=0,
        window_start=PROFILES["drifting"].window_start,
        event_interval_ms=PROFILES["drifting"].event_interval_ms,
        emission_lag_ms=PROFILES["drifting"].emission_lag_ms,
        cnpj_pool=_POOL,
    )
    records = delivered_records(spec, DefectSpec(drift_from_index=80))
    carriers = [i for i, record in enumerate(records) if DRIFT_COLUMN in record]
    assert carriers == list(range(80, 100))
    assert all(DRIFT_COLUMN not in record for record in records[:80])


# --- THE FOURTH PROFILE: THE "BEFORE" SIDE OF F3's AS-OF JOIN ----------------------
#
# The two `applied_date`s the CNPJ vault holds, spelled as LITERALS here rather than
# imported from `opl.generator.profiles`. That module's import guard compares the
# declared window against its own copy of these two dates; a test that imported the
# same constant would agree with it forever -- including on the day someone widened
# the constant to make a moved window legal. Measured in F2 wave 1 and restated in
# `docs/f3-run-evidence.md` section 0.5.
_EARLIER_APPLIED_DATE = "2026-06-13T00:00:00.000Z"
_LATER_APPLIED_DATE = "2026-07-11T00:00:00.000Z"


def test_the_fourth_profiles_whole_window_lies_strictly_inside_the_vault_interval():
    """THE CLAIM THE FOURTH PROFILE EXISTS TO MAKE, AND THE ONLY ONE ONLY IT CAN.

    F3 Task 0 measured that all 20,150 payments in bronze sit on 2026-08-01, after
    both `applied_date`s, so every as-of lookup resolves to the LAST version and the
    join is bit-identical to `WHERE valid_to = <sentinel>`. The existing three streams
    are the "after" side; this profile is the "before" side, and it is only that if
    its whole event window falls between the two dates.

    COMPUTED FROM THE DECLARATION ALONE. `window_start` is the first `event_time` --
    `stream._event_at` adds whole intervals to it -- and `last_event_time` is that
    plus `(event_count - 1) * event_interval_ms`. No pool, no session, no generated
    byte, so this is a prediction in the sense section 4.5 of the master protocol
    means.

    ASSERTED RATHER THAN REASONED ABOUT, because the reasoning is only true at today's
    numbers: 10,000 events 5,000 ms apart span 13 h 53 m 15 s against an interval 28
    days wide, which is enormous margin right up until `_EVENT_COUNT` or
    `_EVENT_INTERVAL_MS` moves. The day it stops fitting is the day the profile
    silently becomes a fourth copy of the other three."""
    profile = PROFILES[BETWEEN_SNAPSHOTS]
    first, last = from_text(profile.window_start), from_text(profile.last_event_time)

    assert from_text(_EARLIER_APPLIED_DATE) < first
    assert last < from_text(_LATER_APPLIED_DATE)
    assert (profile.window_start, profile.last_event_time) == (
        "2026-06-20T00:00:00.000Z", "2026-06-20T13:53:15.000Z"
    )
    assert last - first == (profile.event_count - 1) * profile.event_interval_ms
    assert last - first == 49_995_000  # 13 h 53 m 15 s, the F1b span unchanged


def test_every_profile_sits_where_it_says_it_sits_and_says_where_it_sits():
    """THE PLACEMENT GUARD READS A DECLARATION AND ITERATES `PROFILES`, WHICH IS F-API's
    CHANGE TO IT.

    F3's version read `PROFILES[BETWEEN_SNAPSHOTS]` -- one hardcoded key -- so a fifth
    profile inherited none of it and could have sat anywhere without a word going red. The
    obvious generalisation is WRONG and this test pins that too: a blanket "every profile
    sits between the two dates" refuses the F1b three, which legitimately sit AFTER both
    and are the "after" side the as-of join needs. There is no single correct placement, so
    it became part of the declaration.

    THE FIRST ASSERTION IS WHAT MAKES THE GUARD TOTAL: every declared profile carries a
    placement, and every one of the three placements is a word the guard dispatches on."""
    for name, profile in PROFILES.items():
        assert profile.placement in profiles_module.PLACEMENTS, name
        assert profiles_module.observed_placement(profile) == profile.placement, name
    declared = {profile.placement for profile in PROFILES.values()}
    assert declared == {profiles_module.PLACEMENT_AFTER, profiles_module.PLACEMENT_BETWEEN}
    profiles_module._refuse_a_window_that_contradicts_its_declared_placement()


@pytest.mark.parametrize(
    ("window_start", "declared"),
    [
        # The single most likely wrong edit: `between-snapshots` moved back onto F1b's
        # window, which is what `_profile`'s default gives anyone who deletes the keyword.
        ("2026-08-01T00:00:00.000Z", "between"),
        # Below the earlier applied_date, where the as-of join resolves through
        # `VALID_FROM_FLOOR` rather than through the version chain.
        ("2026-06-01T00:00:00.000Z", "between"),
        # A STRADDLE: the window opens before the later date and closes after it. It is
        # neither "between" nor "after", and `observed_placement` returns None for it --
        # so the comparison catches it without a fourth branch.
        ("2026-07-10T23:00:00.000Z", "between"),
        # The mirror of the first case, and the one a blanket "every profile is between"
        # would have made unrefusable: an F1b-shaped window declared as anything else.
        ("2026-08-01T00:00:00.000Z", "before"),
    ],
)
def test_a_window_that_contradicts_its_declared_placement_is_refused_at_import(
    monkeypatch, window_start, declared
):
    """THE PROBE THAT MAKES THE GUARD WORTH HAVING, over four doctored declarations.

    Without it the refusal is a branch nothing has ever taken, which is how a refusal path
    stays green and wrong. Each case is a different way for the DECLARATION and the
    ARITHMETIC to disagree, and the guard's message names both."""
    moved = dataclasses.replace(
        PROFILES[BETWEEN_SNAPSHOTS], window_start=window_start, placement=declared
    )
    monkeypatch.setitem(profiles_module.PROFILES, BETWEEN_SNAPSHOTS, moved)
    with pytest.raises(ValueError, match="declares placement"):
        profiles_module._refuse_a_window_that_contradicts_its_declared_placement()


def test_the_before_placement_is_a_real_branch_and_not_a_spelling():
    """`before` HAS NO DECLARED PROFILE, so without this its branch is unreachable code.

    Master protocol §4.6: a path that ran zero rows through it is not a path that works.
    This is the honest half of that -- the branch is exercised HERE and by no declaration,
    and the phase's evidence says so rather than reporting it as covered. A window entirely
    below the earlier `applied_date` is `before`; the same window declared `between` is
    refused; and `between-snapshots`' real window is not `before`, which is what stops this
    from passing under an `observed_placement` that returned `before` for everything."""
    early = dataclasses.replace(
        PROFILES[BETWEEN_SNAPSHOTS],
        window_start="2026-06-01T00:00:00.000Z",
        placement=profiles_module.PLACEMENT_BEFORE,
    )
    assert profiles_module.observed_placement(early) == profiles_module.PLACEMENT_BEFORE
    assert early.last_event_time == "2026-06-01T13:53:15.000Z"
    assert from_text(early.last_event_time) < from_text(_EARLIER_APPLIED_DATE)
    assert profiles_module.observed_placement(PROFILES[BETWEEN_SNAPSHOTS]) != (
        profiles_module.PLACEMENT_BEFORE
    )


@pytest.mark.parametrize("placement", ["sideways", "BETWEEN", "", None])
def test_a_placement_that_is_not_one_of_the_three_is_refused_at_construction(placement):
    """AT CONSTRUCTION AND NOT ONLY AT IMPORT, because the two mistakes are different.

    A typo in the WORD is not a window in the wrong place: the guard would compare it
    against the arithmetic and refuse with a message about applied_dates, sending a reader
    to look at a window that is exactly where it should be. `BETWEEN` is in the list on
    purpose -- these are compared exactly, unlike the derivation purposes, which
    `hash_key` case-folds."""
    with pytest.raises(ValueError, match="not one of"):
        dataclasses.replace(PROFILES[BETWEEN_SNAPSHOTS], placement=placement)


def test_the_fourth_profiles_predicted_counts_are_arithmetic_on_the_declaration():
    """IT CARRIES NO DEFECT, AND THAT IS A DECLARATION RATHER THAN AN OMISSION.

    The profile exists to place payments in time. A fourth defect class would add a
    number to F1b's published totals -- 150 redeliveries, 100 late arrivals, 2,000
    drifted rows, all stated against `bronze_payments` as a whole -- and every one of
    those sentences would then be describing a table it no longer describes. The three
    sums below are the assertion that none of them moved.

    `delivered_row_count` equals `event_count` exactly because a duplicate is the only
    thing that adds a row; `drifted_row_count` is zero because `drift_from_index` is
    None. Both are arithmetic on the declaration, which is what lets Task 4 publish
    them before the run."""
    profile = PROFILES[BETWEEN_SNAPSHOTS]
    assert profile.defects == NO_DEFECTS and profile.defects.is_clean
    assert profile.delivered_row_count == profile.event_count == 10_000
    assert profile.drifted_row_count == 0

    assert sum(p.defects.duplicate_count for p in PROFILES.values()) == 150
    assert sum(p.defects.late_count for p in PROFILES.values()) == 100
    assert sum(p.drifted_row_count for p in PROFILES.values()) == 2_000
    # 50,150 over FIVE declared profiles, and the number `bronze_payments` reaches is
    # 40,150 -- `drifting` stops at `fail_on_dq` and never promotes. Both numbers are in
    # `docs/f-api-run-evidence.md` §1.1 and they are not the same claim.
    assert sum(p.delivered_row_count for p in PROFILES.values()) == 50_150


def test_the_fourth_profile_lands_under_its_own_name_and_its_own_identities():
    """THE STREAM ID IS THE LANDING FILENAME AND HALF OF EVERY DERIVATION PURPOSE.

    A duplicated one would write two streams to one path -- the second run refused,
    loudly, but only after the first had landed and been ingested -- and would fold two
    profiles into one identity space, which is what
    `test_the_declared_profiles_derive_disjoint_identities` measures on the other side.
    The key/name agreement is the third: `profile_for("between-snapshots")` returning
    something that logs itself as "clean" is misdirection at the exact moment an
    operator is reading a log to find out what landed."""
    filenames = [filename_for(p.stream_spec(_POOL)) for p in PROFILES.values()]
    assert filename_for(PROFILES[BETWEEN_SNAPSHOTS].stream_spec(_POOL)) == (
        f"F3-BETWEEN-SNAPSHOTS{STREAM_FILE_SUFFIX}"
    )
    assert len(set(filenames)) == len(PROFILES) == 5
    assert all(profile.name == key for key, profile in PROFILES.items())
    assert profile_for(BETWEEN_SNAPSHOTS) is PROFILES[BETWEEN_SNAPSHOTS]
    profiles_module._assert_the_profiles_are_declared_consistently()


def test_the_three_f1b_streams_are_the_same_input_to_the_generator_as_before():
    """BYTE-IDENTITY, PINNED WHERE IT IS CHEAP TO PIN.

    F1b's published evidence -- 20,150 rows, 150 duplicates, 100 late arrivals, 2,000
    drifted rows, 1,024/1,024 CNPJ resolution -- rests on three files that must not
    move. `delivered_records` is a pure function of `(StreamSpec, DefectSpec)`, so
    pinning that PAIR is the whole input to the derivation and therefore the whole
    input to the bytes; generating the three 10,000-event streams to digest them would
    cost half a minute for a strictly weaker statement (it would also depend on the
    real pool, which needs a 69M-row table).

    Written as literal constructions rather than as a loop over `PROFILES`, because a
    pin that reads its expectation from the thing it is pinning is not a pin. The
    fourth profile is deliberately absent: adding it must not touch these three."""
    probe = validated_pool(("00000001", "00000002"))
    shape = {
        "event_count": 10_000, "repeat_count": 800,
        "window_start": "2026-08-01T00:00:00.000Z",
        "event_interval_ms": 5_000, "emission_lag_ms": 1_500, "cnpj_pool": probe,
        # SPELLED OUT RATHER THAN LEFT TO `StreamSpec`'s DEFAULT, which is the whole point
        # of a pin: relying on the default would make this test agree with a change to the
        # default, and the default is exactly what F-API's currency widening rests on.
        "currencies": ("BRL",),
    }
    expected = {
        "clean": (StreamSpec(seed=20260813, stream_id="F1B-CLEAN-2026-08", **shape),
                  NO_DEFECTS),
        "promotable": (StreamSpec(seed=20260814, stream_id="F1B-PROMOTABLE-2026-08", **shape),
                       DefectSpec(duplicate_count=150, late_count=100,
                                  late_by_ms=LATENESS_WINDOW_MS)),
        "drifting": (StreamSpec(seed=20260815, stream_id="F1B-DRIFTING-2026-08", **shape),
                     DefectSpec(drift_from_index=8_000)),
    }
    for name, (spec, defects) in expected.items():
        assert PROFILES[name].stream_spec(probe) == spec
        assert PROFILES[name].defects == defects


# --- THE FIFTH PROFILE: TWO CURRENCIES, AND TWO RATES ON ONE CALENDAR DAY -----------
#
# The 2026-06-22 PTAX quote's publication instant, spelled as a LITERAL here rather than
# imported from anywhere. It is a fact about BCB's series, captured live in
# `tests/test_ptax_source.py`'s fixture bodies (`dataHoraCotacao 2026-06-22
# 13:06:19.750415`) and read as BRT, which the phase's T3 rules and pins. Restating it here
# is deliberate: this file must not agree with a constant somebody widened.
_PUBLISHED_AT_BRT = "2026-06-22T16:06:19.750Z"
_PUBLICATION_REMAINDER_US = 415


def test_the_fifth_profiles_window_straddles_the_bulletin_it_was_placed_for():
    """THE ARITHMETIC THE WINDOW WAS CHOSEN FOR, from the declaration alone.

    THREE EARLIER WINDOWS WERE PUBLISHED AND FALSIFIED before this one, and each failed a
    different half of the same requirement. 2026-06-19 put every row after that day's
    bulletin, so the FALLBACK went unexercised. 2026-06-21T14:00Z closed twelve hours
    before the 06-22 bulletin, so the DIRECT lookup went unexercised -- killed by the
    phase's own T3 ruling twenty-four lines below it. Sunday-into-Monday collapsed onto one
    BRT day, making the verdict a function of a timezone convention.

    WHAT THIS ONE HAS TO SATISFY, asserted rather than reasoned about:

      1. The publication instant falls strictly INSIDE the window, so both populations are
         non-empty. It is 29,179,750.415 ms after the window opens.
      2. NO EVENT LANDS ON THE BOUNDARY. Events are on whole 5,000 ms steps and the
         publication carries a 415 us remainder, so `<=` and `<` cannot disagree about a
         row -- which is the one way a boundary bug would be invisible.
      3. THE WHOLE WINDOW SITS INSIDE ONE CALENDAR DAY IN BOTH ZONES, so this profile adds
         exactly one `event_date_key` and its calendar day does not depend on a convention.
      4. It is strictly inside the vault's applied_date interval, which is the placement
         guard's business and is asserted with the others above."""
    profile = PROFILES[CROSS_CURRENCY]
    first, last = from_text(profile.window_start), from_text(profile.last_event_time)
    published = from_text(_PUBLISHED_AT_BRT)

    assert (profile.window_start, profile.last_event_time) == (
        "2026-06-22T08:00:00.000Z", "2026-06-22T21:53:15.000Z"
    )
    assert first < published < last, "both populations must be non-empty"
    assert published - first == 29_179_750
    offset_us = (published - first) * 1_000 + _PUBLICATION_REMAINDER_US
    assert offset_us % (profile.event_interval_ms * 1_000) != 0, "no event on the boundary"
    assert -(-offset_us // (profile.event_interval_ms * 1_000)) == 5_836

    # One calendar day in UTC, and in BRT (UTC-3) too: 08:00..21:53:15 Z is
    # 05:00..18:53:15 in Brasilia, so neither end crosses midnight in either zone.
    assert profile.window_start[:10] == profile.last_event_time[:10] == "2026-06-22"
    brt = (from_text(profile.window_start) - 3 * 3_600_000, last - 3 * 3_600_000)
    assert {to_text(instant)[:10] for instant in brt} == {"2026-06-22"}
    assert last - first == 49_995_000, "13 h 53 m 15 s, the F1b span unchanged"


# WHAT THE TWO CLOSING TESTS BELOW SHARE, AND WHY THE PROSE IS OUT HERE.
#
# Both are marked against `docs/f-api-run-evidence.md` §1.1, where every number was
# published BEFORE this profile was declared, computed on a tree that does not contain it.
# They were one 65-line function until the 50-line limit was measured rather than assumed;
# they are two now because they close two different tensions -- T1 asks whether a payment's
# currency varies WITHIN a stream, T2 whether two payments on one calendar day get two
# rates -- and a single failure should name which one moved.
#
# THE SPLIT IS NOT A PARITY COUNT OVER 10,000 INDICES, and getting that wrong is how a
# plausible prediction misses. A legitimate repeat does not draw its own currency:
# `stream.generate` copies an earlier BASE event's whole attribute tuple, currency included.
# So the draw is over the 9,200 base positions and the other 800 INHERIT, and the two halves
# are counted separately because a fix to one that broke the other would leave the total
# right. No version of the phase plan said so.
#
# THE POOL IS SYNTHETIC AND THAT IS SOUND, with one stated assumption. Neither the repeat
# positions nor the repeat sources depend on the pool, and the currency draw depends on it
# only through `_distinct_attributes`' collision retry, which increments a SALT. At 1,024
# companies the attribute space is ~5.2e13 and 9,200 draws collide with probability ~8e-7.
# A collision would move every count below, which is what makes these assertions rather
# than restatements.
#
# MODULE-SCOPED, because generating 10,000 events is ~10 s and both tests read the same
# stream. The fixture returns records rather than a spec so that what is counted is the
# RENDERED contract column an Auto Loader would meet, not a typed field one layer up.


@pytest.fixture(scope="module")
def cross_currency_records() -> tuple[dict[str, str], ...]:
    profile = PROFILES[CROSS_CURRENCY]
    pool = validated_pool(tuple(f"{n:08d}" for n in range(1, POOL_SIZE + 1)))
    return delivered_records(profile.stream_spec(pool), profile.defects)


def test_the_fifth_profiles_currency_split_is_the_published_number(cross_currency_records):
    """T1's CLOSING TEST: a payment's currency varies WITHIN one stream.

    THE BYTE COUNT IS PINNED HERE TOO, and it is pool-independent: every `cnpj_basico` is
    eight characters and both currency codes are three, so 2,926,588 is the number the
    workspace run must land whatever pool it draws. It was predicted in §1.1 by emitting
    this same (seed, stream id, window) from code that only knew `("BRL",)`."""
    records = cross_currency_records
    profile = PROFILES[CROSS_CURRENCY]
    drawn = [record["currency"] for record in records]

    assert len(records) == profile.delivered_row_count == 10_000
    assert drawn.count("BRL") == 5_095
    assert drawn.count("USD") == 4_905
    assert set(drawn) == {"BRL", "USD"} == set(profile.currencies)
    assert len(serialised_bytes(records)) == 2_926_588

    # A base event's attribute tuple is unique by construction, so the 800 rows whose tuple
    # is shared with an earlier row ARE the repeats -- read off the records rather than
    # recomputed from `_repeat_positions`, which would be a second spelling of the
    # generator's own selection.
    seen: set[tuple[str, ...]] = set()
    base: list[str] = []
    inherited: list[str] = []
    for record in records:
        tuple_of = tuple(record[column] for column in payments.BUSINESS_ATTRIBUTE_COLUMNS)
        (inherited if tuple_of in seen else base).append(record["currency"])
        seen.add(tuple_of)
    assert (len(base), len(inherited)) == (9_200, 800)
    assert (base.count("USD"), inherited.count("USD")) == (4_525, 380)
    assert (base.count("BRL"), inherited.count("BRL")) == (4_675, 420)


def test_the_fifth_profiles_two_resolution_paths_are_the_published_numbers(
    cross_currency_records,
):
    """T2's CLOSING TEST: two counts, both non-zero, published as numbers.

    A USD row whose own instant precedes the bulletin falls back to Friday 2026-06-19
    (venda 5.14420); one after it resolves same-day to 2026-06-22 (venda 5.13950). BRL rows
    consult no quote at all -- `fx_rate` is 1.0 by definition -- so they are counted apart,
    which is the refinement the phase plan's own 5,836 / 4,164 ROW split does not carry: the
    populations that resolve a rate are 2,864 and 2,041.

    `min(...) > 0` is the assertion standing decision §4.6 asks for. A path with zero rows
    through it is not a path that works, and either count reaching zero is how this phase's
    three earlier windows failed."""
    records = cross_currency_records
    published = from_text(_PUBLISHED_AT_BRT)
    before = [r for r in records if from_text(r[payments.EVENT_TIME_COLUMN]) < published]
    after = [r for r in records if from_text(r[payments.EVENT_TIME_COLUMN]) > published]

    assert (len(before), len(after)) == (5_836, 4_164)
    assert len(before) + len(after) == len(records), "no row sits on the boundary"
    fell_back = [r for r in before if r["currency"] == "USD"]
    same_day = [r for r in after if r["currency"] == "USD"]
    assert len(fell_back) == 2_864
    assert len(same_day) == 2_041
    assert len(fell_back) + len(same_day) == 4_905
    assert min(len(fell_back), len(same_day)) > 0, "an empty path is an unexercised path"


def test_every_profile_hands_its_own_currency_tuple_to_the_spec_that_draws():
    """THE ONE LINE THAT WOULD FAIL SILENTLY IF IT WERE DELETED.

    `StreamSpec.currencies` has a DEFAULT -- deliberately, because that default is what
    keeps four landed streams byte-identical -- so a `stream_spec()` that forgot to pass
    the profile's own tuple would construct without complaint and generate a BRL-only
    stream out of a profile declaring a mix. Every count would be green, every guard
    would pass, and the FX layer would be back to a column that cannot be wrong, which is
    the failure T1 exists to remove. Nothing else in this repository can see that.

    THE SECOND ASSERTION IS THE GUARD T1 ASKED FOR, reached where it actually lives. No
    loop over `PROFILES` spells the subset rule: `profiles._assert_every_profile_describes
    _a_stream_that_can_exist` builds every profile's `StreamSpec` at import, and
    `StreamSpec.__post_init__` calls `stream._require_currencies`. So this reproduces the
    rule's verdict on the live declarations rather than adding a second copy of it."""
    probe = validated_pool(("00000001", "00000002"))
    for name, profile in PROFILES.items():
        assert profile.stream_spec(probe).currencies == profile.currencies, name
        stream_module._require_currencies(profile.currencies)
        ranks = [payments.CURRENCIES.index(code) for code in profile.currencies]
        assert ranks == sorted(set(ranks)), f"{name} is not a subsequence of the domain"


def test_the_factory_varies_exactly_the_fields_its_own_prose_names():
    """THE PROSE AND THE CODE, CHECKED AGAINST EACH OTHER RATHER THAN BOTH READ.

    `_profile` used to hardcode `window_start` while its docstring said the streams
    "differ ONLY in their id, their seed and their defects". Both halves were true, and
    the fourth profile makes the second one false -- so the parameter and the sentence
    had to move together. This repository has a documented case of exactly that pair
    drifting apart with the suite green: `drifted_row_count`'s docstring promised
    redeliveries its arithmetic did not count, and the test that guarded it was
    index-based in the same way (found by review on PR #14).

    THE ASSERTION IS STRUCTURAL, NOT TEXTUAL. The factory's parameters are the fields
    that MAY differ; every `StreamProfile` field that is not a parameter therefore
    cannot differ, and the third block proves that is what the declarations actually
    do. The prose check is last and is the weakest of the three: it only catches a
    sentence that stopped naming a field, which is the half a structural check cannot
    see.

    IT READS THE MODULE'S COMMENT BLOCK AND NOT `__doc__`, WHICH IS F-API's CHANGE HERE.
    The invariant outgrew the 50-line function limit once the two new parameters were
    documented, so it moved above the factory as a comment -- the shape
    `opl.bronze.generated_landing` and `opl.bronze.rules` already use. A docstring-only
    check would have gone GREEN on that move while checking nothing, which is precisely
    the failure this test exists to catch, so it follows the prose to where the prose is."""
    varying = set(inspect.signature(profiles_module._profile).parameters)
    assert varying == {
        "name", "stream_id", "seed", "defects", "window_start", "currencies", "placement",
    }

    shared = {field.name for field in dataclasses.fields(StreamProfile)} - varying
    assert shared == {"event_count", "repeat_count", "event_interval_ms", "emission_lag_ms"}
    for field in shared:
        held = {getattr(profile, field) for profile in PROFILES.values()}
        assert len(held) == 1, f"{field} is no factory parameter and yet differs: {held}"

    source = Path(profiles_module.__file__).read_text(encoding="utf-8")
    _, _, after = source.partition("WHAT THE FACTORY BELOW VARIES")
    prose, _, _ = after.partition("def _profile(")
    assert prose, "the factory's comment block is gone, and this check reads it"
    for field in varying | shared:
        assert field in prose, f"the factory's own prose no longer names {field}"
    assert (profiles_module._profile.__doc__ or "").count("comment block above") == 1, (
        "the docstring must point at the block, or a reader lands on a bare signature"
    )


def test_a_straddling_window_would_have_cost_the_shared_shape_and_is_not_refused():
    """WHY THE WINDOW SITS BETWEEN THE TWO DATES RATHER THAN ACROSS THEM.

    THE REASON IS NOT THE ONE IT LOOKS LIKE, AND THE ARITHMETIC IS HERE SO NOBODY HAS
    TO TAKE IT ON TRUST. A stream spanning the 28 days would need an interval of
    241,944 ms, and `_require_defects_fit` would NOT refuse lateness on it: that guard
    asks only that `late_by_ms` EXCEED the interval, and one lateness window is
    3,600,000 ms -- fourteen times larger. A straddle could carry all three defect
    classes.

    WHAT IT WOULD ACTUALLY COST IS THE SHARED SHAPE. 241,944 ms is 48x the interval the
    other three profiles run at, so the fourth stream would no longer be the same
    delivery moved in time; any difference an as-of join showed between it and them
    would have two candidate causes, which is the same argument
    `opl.generator.defects` makes for switching one defect class at a time. F3 Task 0
    pre-decided the between-window on that basis (`docs/f3-run-evidence.md` 0.3), and
    the existing streams already supply the "after" side, so nothing needs a straddle.

    ASSERTED IN BOTH DIRECTIONS: that the straddle is legal (so the false reason cannot
    survive here as folklore) and that the declared profile did not take it."""
    span = from_text(_LATER_APPLIED_DATE) - from_text(_EARLIER_APPLIED_DATE)
    profile = PROFILES[BETWEEN_SNAPSHOTS]
    straddling_interval = span // (profile.event_count - 1)
    assert (span, straddling_interval) == (28 * 24 * 3_600_000, 241_944)

    straddle = dataclasses.replace(
        profile.stream_spec(_POOL),
        window_start=_EARLIER_APPLIED_DATE,
        event_interval_ms=straddling_interval,
    )
    _require_defects_fit(straddle, DefectSpec(late_count=1, late_by_ms=LATENESS_WINDOW_MS))
    assert LATENESS_WINDOW_MS > straddling_interval

    assert profile.event_interval_ms == PROFILES["clean"].event_interval_ms == 5_000
    assert straddling_interval == 48 * profile.event_interval_ms + 1_944
