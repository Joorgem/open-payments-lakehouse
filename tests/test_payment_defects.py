# tests/test_payment_defects.py
"""The three defect classes F1b carries on purpose, each measured while the others are off.

WHAT MAKES THIS FILE THE DELIVERABLE RATHER THAN `opl.generator.defects`. A generator that
"contains duplicates, late arrivals and drift" is a sentence; a generator whose duplicate
count is recoverable by subtraction, whose late arrivals are the set a query independently
measures, and whose drift column is absent from every row before a named position is a
fixture other phases can build evidence on. Each section below states the count it closes
and then closes it with an arithmetic identity, not with an inequality.

THE ISOLATION PROPERTY IS ASSERTED, NOT ASSUMED. Every class has a test that turns it on
alone and shows the OTHER TWO measure zero -- because a defect fixture whose classes
contaminate each other cannot support any claim about one of them, and the two obvious
contamination routes are real: a redelivery placed at the wrong position in the file reads
as a late arrival under a row-order reading of the definition, and a drift boundary keyed
on the delivery position moves when lateness reorders the delivery.

THE MEASUREMENT AND THE INJECTION ARE COMPUTED BY CODE THAT CANNOT SEE EACH OTHER.
`opl.generator.measures` imports no part of `opl.generator.defects`; it reads the rendered
records and nothing else. So "the injected late set equals the measured late set" is a
comparison between two implementations of one definition rather than a restatement.
"""
from __future__ import annotations

import hashlib

import pytest

from opl.contracts.payments import COLUMNS, DRIFT_COLUMN, DRIFT_VALUES
from opl.generator import defects as defects_module
from opl.generator import measures
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import (
    LATENESS_WINDOW_MS,
    NO_DEFECTS,
    DefectSpec,
    delivered_records,
    drift_positions,
    duplicate_positions,
    late_positions,
)
from opl.generator.events import business_attributes_of, to_jsonl
from opl.generator.instants import MILLISECONDS_PER_HOUR, from_text
from opl.generator.stream import StreamSpec, records_for

# THE PINNED STREAM FOR THIS FILE, and it is deliberately NOT the one
# `tests/test_payment_generator.py` pins. That stream is 24 events over six seconds: a
# one-hour delay applied to any of them lands past the whole stream, so every late arrival
# would sit in the tail and "crossing a window boundary" would be unfalsifiable. This one
# runs 180 events one minute apart -- three hours, three whole windows -- so a record
# delayed by one window is delivered BETWEEN punctual records rather than after all of
# them, which is the case a real late arrival is.
_POOL = validated_pool([f"{n:08d}" for n in range(1, 21)])
_SPEC = StreamSpec(
    seed=20260812,
    stream_id="F1B-DEFECTS",
    event_count=180,
    repeat_count=12,
    window_start="2026-06-01T00:00:00.000Z",
    event_interval_ms=60_000,
    emission_lag_ms=1_500,
    cnpj_pool=_POOL,
)

# ONE SWITCH EACH, and one with all three on. The counts are chosen so that no duplicated
# position is also a late one (asserted below), which keeps every count in the isolated
# sections exact; `_OVERLAPPING` exercises the other branch on purpose.
_DUPLICATES = DefectSpec(duplicate_count=7)
_LATE = DefectSpec(late_count=5, late_by_ms=LATENESS_WINDOW_MS)
_DRIFT = DefectSpec(drift_from_index=120)
_ALL = DefectSpec(
    duplicate_count=7, late_count=5, late_by_ms=LATENESS_WINDOW_MS, drift_from_index=120
)
_OVERLAPPING = DefectSpec(duplicate_count=10, late_count=5, late_by_ms=LATENESS_WINDOW_MS)

_SWITCHED = {
    "clean": NO_DEFECTS,
    "duplicates": _DUPLICATES,
    "late": _LATE,
    "drift": _DRIFT,
    "all": _ALL,
}


def _delivered(defects: DefectSpec) -> tuple[dict[str, str], ...]:
    return delivered_records(_SPEC, defects)


# --- the defect layer is additive --------------------------------------------------------


def test_no_defects_delivers_the_clean_stream_and_not_a_copy_of_it():
    """THE PROPERTY THAT KEEPS THE CLEAN GOLDEN PIN MEANINGFUL.

    `tests/test_payment_generator.py` pins the clean stream's bytes. If this module were a
    layer the clean stream passed THROUGH -- re-rendering, re-ordering, re-keying -- that
    pin would still be green while describing a stream nothing produces any more. Asserted
    over the records AND over the serialised text, because equal dicts can still serialise
    differently if key order moved."""
    assert _delivered(NO_DEFECTS) == records_for(_SPEC)
    assert to_jsonl(_delivered(NO_DEFECTS)) == to_jsonl(records_for(_SPEC))
    assert NO_DEFECTS.is_clean


@pytest.mark.parametrize("name", sorted(_SWITCHED))
def test_the_same_seed_reproduces_byte_identically_with_defects_enabled(name):
    """Determinism is the property everything else rests on, and it has to survive the
    defects rather than only precede them.

    Each stream is generated twice from independently constructed defect specs, so the
    comparison cannot be satisfied by a cached object -- the same shape the clean
    byte-identity test uses."""
    defects = _SWITCHED[name]
    twin = DefectSpec(
        duplicate_count=defects.duplicate_count,
        late_count=defects.late_count,
        late_by_ms=defects.late_by_ms,
        drift_from_index=defects.drift_from_index,
    )
    first = to_jsonl(_delivered(defects)).encode("utf-8")
    assert first == to_jsonl(_delivered(twin)).encode("utf-8")
    assert b"\r" not in first, "an os.linesep join would differ between Windows and DBR"


@pytest.mark.parametrize(
    ("name", "rows", "size", "sha"),
    [
        ("clean", 180, 52_625, "ef8be4918abe43c65f377e349518e4f14f2c952fee536ef8fa183e85ce3f56b0"),
        ("duplicates", 187, 54_659, "67d4d51916581e8ca2c81692321ae020e05d8c4ae42905f590a698bfa361"
         "2dae"),
        ("late", 180, 52_625, "3aa789bb1481758d26ad94cc65fffbb9572c017aed55083bf3996493df5b5e6f"),
        ("drift", 180, 54_501, "b6c26a43cdb8893d906720787f8ff1b236b9f78a8dc7544b6f0b8e6bfdb59b58"),
        ("all", 187, 56_562, "e1d75ad4c08afd7ffbdf0b05ba60b8c939034c74fef26f3b97ee7ea36a625485"),
    ],
)
def test_the_pinned_defective_streams_are_these_and_no_others(name, rows, size, sha):
    """THE GOLDEN PINS, one per switch, in the shape the clean stream's pin takes.

    The row count and the byte count are pinned beside the digest because a bare digest
    mismatch names nothing. `clean` and `late` hold the same 180 rows in the same 52,625
    bytes and differ only in their digest -- lateness reorders rows and rewrites five
    fixed-width timestamps, and neither operation can change a total -- while `drift`
    differs from `clean` by exactly the bytes of sixty appended keys. Which of the three
    numbers moves is itself the diagnosis."""
    text = to_jsonl(_delivered(_SWITCHED[name]))
    assert len(text.splitlines()) == rows
    assert len(text.encode("utf-8")) == size
    assert hashlib.sha256(text.encode("utf-8")).hexdigest() == sha


@pytest.mark.parametrize("name", sorted(_SWITCHED))
def test_no_contract_column_is_ever_blank_however_the_defects_are_switched(name):
    """The typed/string boundary, re-asserted over the defective streams.

    It is not inherited: the drift injection adds a key that no renderer produced and a
    redelivery copies a dict, so both bypass `record_of`'s own guarantees. If a defect
    could emit `""` or a non-`str`, "a NULL in bronze means the JSON did not carry the
    field" would stop being true and the drift class would stop being measurable."""
    for record in _delivered(_SWITCHED[name]):
        assert tuple(record)[: len(COLUMNS)] == COLUMNS, "the declared columns come first"
        for column, value in record.items():
            assert type(value) is str, f"{column} left the generator as {type(value)}"
            assert value != "" and value.strip() == value


# --- T2: a duplicate is not a repeat -------------------------------------------------------


def test_the_duplicate_count_is_recoverable_by_subtraction():
    """THE TEST THAT CLOSES T2, in the shape Task 4's query takes.

    `COUNT(*) - COUNT(DISTINCT transaction_id)` is the injected count and nothing else.
    Pinned as exact numbers rather than as a difference, so a change that moved both terms
    together could not pass."""
    records = _delivered(_DUPLICATES)
    assert len(records) == 187
    assert measures.distinct_identities(records) == 180 == _SPEC.event_count
    assert measures.duplicate_row_count(records) == 7 == _DUPLICATES.duplicate_count


def test_the_two_distinct_counts_disagree_and_the_gap_is_the_repeats():
    """If these agreed, the fixture would be measuring nothing.

    180 identities over 168 distinct attribute tuples: the twelve legitimate repeats. The
    seven redeliveries move NEITHER number -- a redelivery adds a row that is already
    counted in both -- which is what makes the two counts able to disagree about different
    things."""
    records = _delivered(_DUPLICATES)
    assert measures.distinct_identities(records) == 180
    assert measures.distinct_business_attributes(records) == 168 == _SPEC.base_count
    assert measures.distinct_identities(records) != measures.distinct_business_attributes(records)
    assert _SPEC.event_count - _SPEC.repeat_count == 168


def test_a_legitimate_repeat_never_appears_in_the_duplicate_count():
    """The clean stream carries twelve legitimate repeats and zero duplicates. That the
    duplicate figure reads 0 there and 7 with the switch on is what says the figure counts
    redeliveries rather than similar-looking rows."""
    assert measures.duplicate_row_count(_delivered(NO_DEFECTS)) == 0
    assert measures.distinct_business_attributes(_delivered(NO_DEFECTS)) == 168
    assert measures.redelivered_identities(_delivered(NO_DEFECTS)) == ()
    assert len(measures.redelivered_identities(_delivered(_DUPLICATES))) == 7


def test_a_redelivery_is_byte_identical_to_the_row_it_repeats():
    """A duplicate is the same EVENT delivered twice: same identity, same timestamps, same
    attributes, same bytes. A redelivery differing in any column would be a legitimate
    repeat wearing a shared id, which is a third thing this contract does not model."""
    records = _delivered(_DUPLICATES)
    repeated = set(measures.redelivered_identities(records))
    for identity in repeated:
        deliveries = [r for r in records if r["transaction_id"] == identity]
        assert len(deliveries) == 2
        assert deliveries[0] == deliveries[1]
    lines = to_jsonl(records).splitlines()
    assert len(lines) - len(set(lines)) == 7, "seven lines are byte-identical to another"


def test_duplicates_alone_introduce_no_late_arrival_and_no_drift():
    """ISOLATION, and the first half is the one that could plausibly fail.

    A redelivery carries its original's `emitted_at`, and it is delivered immediately after
    it, so neither the ordering reading of lateness nor a row-order reading of the same
    sentence can see it. That is a property of where the redelivery is PUT, not of what it
    contains, which is why it is asserted rather than argued."""
    records = _delivered(_DUPLICATES)
    assert measures.late_arrivals(records) == ()
    assert measures.drifted_rows(records) == ()
    assert measures.undeclared_keys(records) == ()


# --- T1: late arrivals ----------------------------------------------------------------------


def test_the_measured_late_arrivals_are_the_injected_ones():
    """The injection and the measurement are computed by modules that cannot see each
    other, so this is a comparison rather than a restatement. Five, and their positions
    named: a count alone would pass on five wrong rows."""
    records = _delivered(_LATE)
    assert measures.late_arrivals(records) == (110, 120, 166, 178, 179)
    assert len(measures.late_arrivals(records)) == _LATE.late_count == 5
    assert sorted(late_positions(_SPEC, _LATE)) == [53, 62, 109, 140, 156]


def test_the_measurement_that_reads_zero_on_the_clean_stream_reads_five_here():
    """WHAT MAKES `test_the_clean_stream_contains_no_late_arrival` A MEASUREMENT.

    Task 1's author flagged that his no-lateness test was true BY CONSTRUCTION --
    `emitted_at = event_time + constant` cannot express lateness, so the assertion could
    not have failed. The same function is run here over a stream that does contain late
    arrivals, and it returns them. One function, two answers, and the clean pin is
    therefore a fact about the stream rather than about the arithmetic."""
    assert measures.late_arrivals(_delivered(NO_DEFECTS)) == ()
    assert measures.late_arrivals(records_for(_SPEC)) == ()
    assert len(measures.late_arrivals(_delivered(_LATE))) == 5


def test_a_late_record_is_emitted_after_records_whose_event_time_is_newer():
    """The definition, spelled out row by row rather than delegated to the measure.

    For each late row there is at least one row emitted STRICTLY earlier whose payment
    happened LATER -- which is the whole of what "late arrival" means, and the reason no
    clock is involved: both halves are columns."""
    records = _delivered(_LATE)
    for position in measures.late_arrivals(records):
        late = records[position]
        earlier = [
            r
            for r in records
            if from_text(r["emitted_at"]) < from_text(late["emitted_at"])
            and from_text(r["event_time"]) > from_text(late["event_time"])
        ]
        assert earlier, f"row {position} was declared late and nothing newer preceded it"


def test_every_late_arrival_crosses_at_least_one_window_boundary():
    """The consequence lateness HAS, as opposed to the relation lateness IS. A windowed
    aggregation over `event_time`, keyed off `emitted_at`, had already closed and emitted
    that window before the row turned up. Checked against the punctual rows too, so this
    cannot pass on a stream where everything crosses."""
    records = _delivered(_LATE)
    late = set(measures.late_arrivals(records))
    for position, record in enumerate(records):
        crossed = measures.crosses_a_window(record, window_ms=MILLISECONDS_PER_HOUR)
        if position in late:
            assert crossed, f"late row {position} stayed inside its own window"
        else:
            assert not crossed, f"punctual row {position} crossed a boundary"


def test_lateness_moves_the_emission_and_never_the_event_time():
    """The payment did not move; the delivery did. Adding the delay to both would produce
    a stream with a different window rather than a stream with a late arrival in it -- and
    it would reproduce byte-identically while doing so."""
    clean = _delivered(NO_DEFECTS)
    late = _delivered(_LATE)
    by_identity = {r["transaction_id"]: r for r in late}
    for record in clean:
        moved = by_identity[record["transaction_id"]]
        assert moved["event_time"] == record["event_time"]
        delay = from_text(moved["emitted_at"]) - from_text(record["emitted_at"])
        assert delay in (0, _LATE.late_by_ms)
    assert measures.distinct_identities(late) == 180 and len(late) == 180


def test_a_row_sharing_an_emitted_at_with_a_newer_event_is_still_late():
    """THE STRICTLY-EARLIER CLAUSE, exercised rather than reviewed.

    A delay of exactly one window over a one-minute interval lands each late record on the
    same `emitted_at` as a punctual one sixty places along. Rows released together cannot
    make each other late, so the punctual twin must NOT count -- and the late row must
    still count, on the strength of the fifty-nine rows emitted before it. Both halves are
    asserted, because a measure that dropped the strictness would flip exactly one of
    them."""
    records = _delivered(_LATE)
    late = set(measures.late_arrivals(records))
    shared = [
        position
        for position in late
        if sum(1 for r in records if r["emitted_at"] == records[position]["emitted_at"]) > 1
    ]
    assert shared, "the fixture no longer produces a tie; the clause is untested"
    for position in shared:
        twins = [
            at
            for at, r in enumerate(records)
            if r["emitted_at"] == records[position]["emitted_at"] and at != position
        ]
        assert twins and all(at not in late for at in twins)


def test_late_arrivals_alone_introduce_no_duplicate_and_no_drift():
    records = _delivered(_LATE)
    assert measures.duplicate_row_count(records) == 0
    assert measures.redelivered_identities(records) == ()
    assert measures.drifted_rows(records) == ()


# --- schema drift ------------------------------------------------------------------------------


def test_the_drift_column_appears_mid_stream_and_never_before():
    """A NEW OPTIONAL COLUMN APPEARING MID-STREAM, which is the class the ruling chose:
    the one that tests the contract layer and the DQ gate rather than a distribution the
    vault does not model.

    Sixty rows carry it, one hundred and twenty do not, and the boundary is where the spec
    put it. The contiguity matters -- a scattered subset would be a field that flickers,
    which no schema reader meets as drift."""
    records = _delivered(_DRIFT)
    assert measures.drifted_rows(records) == tuple(range(120, 180))
    assert len(measures.drifted_rows(records)) == 60
    assert sorted(drift_positions(_SPEC, _DRIFT)) == list(range(120, 180))
    for position, record in enumerate(records):
        assert (DRIFT_COLUMN in record) == (position >= 120)


def test_the_drift_column_is_the_only_key_the_contract_does_not_declare():
    """What a reader holding the v1 schema would push into `_rescued_data`, and therefore
    what `opl.bronze.dq.evaluate`'s `rescued_data_present` rule fires on. An undeclared key
    that is NOT the declared drift column would be a generator bug wearing a defect's
    clothes."""
    assert measures.undeclared_keys(_delivered(_DRIFT)) == (DRIFT_COLUMN,)
    assert measures.undeclared_keys(_delivered(NO_DEFECTS)) == ()
    assert DRIFT_COLUMN not in COLUMNS


def test_the_drift_column_is_appended_after_the_declared_ones():
    """A producer that started sending a field appends it; it does not reshuffle the object
    it has been sending for months. Key order is the emitted byte order, so this is pinned
    rather than left to `dict` insertion happening to behave."""
    for record in _delivered(_DRIFT)[120:]:
        assert tuple(record) == (*COLUMNS, DRIFT_COLUMN)
        assert record[DRIFT_COLUMN] in DRIFT_VALUES


def test_every_drifted_value_is_upper_case_ascii_from_the_declared_domain():
    """The domain is picked BY INDEX, so it is part of the stream's bytes. An empty value
    would be a second spelling of "absent" and would give the column's NULL two meanings,
    which is the one thing the drift class cannot afford."""
    values = {record[DRIFT_COLUMN] for record in _delivered(_DRIFT)[120:]}
    assert values <= set(DRIFT_VALUES) and values
    for value in DRIFT_VALUES:
        assert value and value.isascii() and value == value.upper() and " " not in value


def test_drift_alone_introduces_no_duplicate_and_no_late_arrival():
    records = _delivered(_DRIFT)
    assert measures.duplicate_row_count(records) == 0
    assert measures.late_arrivals(records) == ()


def test_a_redelivery_of_a_pre_drift_record_still_lacks_the_column():
    """BYTE-IDENTITY WINS OVER THE DRIFT BOUNDARY, and this is where the two rules could
    have been made to disagree.

    A processor replaying an old message replays the message it has, not the message it
    would send today. Six of the seven redeliveries repeat pre-drift records and carry no
    column; the seventh repeats a drifted one and carries it -- which is why the all-on
    stream has 61 drifted rows against the drift-only stream's 60."""
    records = _delivered(_ALL)
    assert len(measures.drifted_rows(records)) == 61
    for identity in measures.redelivered_identities(records):
        deliveries = [r for r in records if r["transaction_id"] == identity]
        assert len(deliveries) == 2 and deliveries[0] == deliveries[1]
    assert measures.undeclared_keys(records) == (DRIFT_COLUMN,)


# --- the classes are independently switchable ---------------------------------------------------


@pytest.mark.parametrize(
    ("isolated", "chooser"),
    [(_DUPLICATES, duplicate_positions), (_LATE, late_positions), (_DRIFT, drift_positions)],
)
def test_turning_the_other_classes_on_does_not_move_a_class_positions(isolated, chooser):
    """INDEPENDENTLY SWITCHABLE MEANS THE POSITIONS DO NOT MOVE -- stronger than three
    booleans, and the property a test isolating one class actually needs.

    Each class draws under its own derivation purpose, indexed by the CLEAN stream's
    emission index. A selection consuming a shared sequence, or keyed off the delivery
    position after an earlier class reordered it, would make every isolated measurement
    true only of the combination it was measured in."""
    assert chooser(_SPEC, isolated) == chooser(_SPEC, _ALL)


def test_the_isolated_fixtures_do_not_overlap_and_the_overlapping_one_composes():
    """WHICH BRANCH THE ISOLATED SECTIONS EXERCISE, stated rather than left to luck.

    A redelivery of a LATE record is itself late -- it carries that record's two timestamps
    verbatim -- so the measured late count is `late_count + |duplicated & late|`. The
    fixtures above avoid that overlap, which is why their counts are exact; `_OVERLAPPING`
    forces it so the composition rule is pinned rather than merely absent."""
    assert not duplicate_positions(_SPEC, _ALL) & late_positions(_SPEC, _ALL)
    assert len(measures.late_arrivals(_delivered(_ALL))) == 5
    overlap = duplicate_positions(_SPEC, _OVERLAPPING) & late_positions(_SPEC, _OVERLAPPING)
    assert sorted(overlap) == [53]
    records = _delivered(_OVERLAPPING)
    assert len(measures.late_arrivals(records)) == 5 + len(overlap) == 6
    assert measures.duplicate_row_count(records) == 10


def test_every_switch_is_off_by_default_and_says_so():
    """`DefectSpec()` is the clean stream, so a caller enabling one class states exactly
    one thing and a reader of a spec literal can see what it does not do."""
    assert DefectSpec() == NO_DEFECTS
    assert NO_DEFECTS.is_clean
    for defects in (_DUPLICATES, _LATE, _DRIFT, _ALL):
        assert not defects.is_clean


# --- what the defect spec refuses -----------------------------------------------------------


def test_a_delay_shorter_than_one_window_is_refused():
    """A late arrival must cross at least one window boundary, or the aggregation it is
    meant to arrive behind never closed and the class exhibits nothing."""
    with pytest.raises(ValueError, match="below one window"):
        DefectSpec(late_count=1, late_by_ms=LATENESS_WINDOW_MS - 1)


def test_a_delay_with_nothing_to_delay_is_refused():
    """Two specs differing only in a field neither stream reads are two names for one
    stream, and one of them would reproduce a digest pinned under the other."""
    with pytest.raises(ValueError, match="delays nothing"):
        DefectSpec(late_by_ms=LATENESS_WINDOW_MS)


def test_a_delay_that_cannot_reorder_anything_is_refused():
    """Lateness is a REORDERING, not a delay. Over a stream whose events are two hours
    apart, a one-hour delay leaves every record emitted before the next one happened --
    a stream that declares late arrivals and holds none, which is the exact shape of a
    fixture that passes its own test by comparing two empty sets."""
    coarse = StreamSpec(
        seed=_SPEC.seed,
        stream_id=_SPEC.stream_id,
        event_count=8,
        repeat_count=0,
        window_start=_SPEC.window_start,
        event_interval_ms=2 * MILLISECONDS_PER_HOUR,
        emission_lag_ms=_SPEC.emission_lag_ms,
        cnpj_pool=_POOL,
    )
    with pytest.raises(ValueError, match="does not exceed event_interval_ms"):
        delivered_records(coarse, DefectSpec(late_count=1, late_by_ms=LATENESS_WINDOW_MS))


def test_a_lateness_that_measures_as_something_else_is_refused():
    """THE PROBE THAT CLOSES THE MEASURABILITY GUARD, and it took construction to reach.

    Delay 60 of 61 records by exactly 60 intervals and the injected set stops being the
    measured one: record 0 lands on the SAME `emitted_at` as the single punctual record
    60, and rows released together cannot make each other late -- so 0 measures punctual
    while 1..59 measure late. The guard catches the disagreement rather than delivering a
    stream whose spec claims sixty late arrivals and whose data holds fifty-nine.

    Without this test the guard would be a branch nothing had ever taken, which is how a
    refusal path stays green and wrong."""
    dense = StreamSpec(
        seed=_SPEC.seed,
        stream_id=_SPEC.stream_id,
        event_count=61,
        repeat_count=0,
        window_start=_SPEC.window_start,
        event_interval_ms=_SPEC.event_interval_ms,
        emission_lag_ms=_SPEC.emission_lag_ms,
        cnpj_pool=_POOL,
    )
    with pytest.raises(ValueError, match="lateness was injected at emission indices"):
        delivered_records(dense, DefectSpec(late_count=60, late_by_ms=LATENESS_WINDOW_MS))


def test_a_drift_that_starts_at_the_first_record_is_refused():
    """A column present from position 0 is a column the stream simply has: no pre-drift
    population to be NULL in it, nothing for a v1 reader to rescue, nothing for a distinct
    count to drop."""
    with pytest.raises(ValueError, match="at least 1"):
        DefectSpec(drift_from_index=0)


def test_a_drift_that_leaves_no_drifted_record_is_refused():
    """The mirror image, and it needs the stream's length -- which is why it is checked
    where the two specs meet rather than in `DefectSpec.__post_init__`."""
    with pytest.raises(ValueError, match="leaves no drifted record"):
        delivered_records(_SPEC, DefectSpec(drift_from_index=_SPEC.event_count))


@pytest.mark.parametrize(
    ("defects", "match"),
    [
        (DefectSpec(duplicate_count=181), "positions were asked for"),
        (DefectSpec(late_count=180, late_by_ms=LATENESS_WINDOW_MS), "positions were asked for"),
    ],
)
def test_more_positions_than_are_eligible_is_refused(defects, match):
    """Injecting fewer than asked would make every count downstream measured rather than
    predicted. `late_count=180` is the interesting one: the LAST record is not eligible,
    because nothing has a newer `event_time` than it and delaying it adds a field to a spec
    and nothing to the stream."""
    with pytest.raises(ValueError, match=match):
        delivered_records(_SPEC, defects)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("duplicate_count", -1),
        ("late_count", -1),
        ("late_by_ms", -1),
        ("duplicate_count", True),
        ("drift_from_index", 1.5),
    ],
)
def test_a_degenerate_defect_field_is_refused(field, value):
    """`True` is refused explicitly because `bool` IS an `int` in Python: a flag passed to
    the wrong parameter would otherwise inject exactly one duplicate and read, at the call
    site, as switching something on."""
    with pytest.raises((TypeError, ValueError)):
        DefectSpec(**{field: value})


def test_a_defect_purpose_colliding_with_a_stream_purpose_is_refused(monkeypatch):
    """ACROSS BOTH MODULES, which is the half a per-module check misses: the purpose is
    hashed, not namespaced, so `opl.generator.stream`'s own guard passing says nothing.
    A collision would tie which rows duplicate to what those rows cost -- perfectly
    correlated, perfectly reproducible, chosen by nobody."""
    monkeypatch.setattr(defects_module, "DEFECT_PURPOSES", ("amount",))
    with pytest.raises(ValueError, match="collides with another purpose"):
        defects_module._assert_defect_purposes_are_distinct()


def test_the_live_defect_purposes_pass_their_own_guard():
    """Guard the guard: the refusal above is monkeypatched, so this is what says the three
    real defect purposes collide with neither each other nor the eight stream purposes."""
    defects_module._assert_defect_purposes_are_distinct()


def test_the_delivered_records_still_answer_the_repeat_question_they_did_when_clean():
    """A last cross-check that the defect layer did not quietly change what a business
    attribute IS: the repeat groups of the all-on stream are the clean stream's groups,
    plus the redeliveries, and no group gained a member by drifting."""
    clean_groups: dict[tuple[str, ...], set[str]] = {}
    for record in _delivered(NO_DEFECTS):
        clean_groups.setdefault(business_attributes_of(record), set()).add(record["transaction_id"])
    all_groups: dict[tuple[str, ...], set[str]] = {}
    for record in _delivered(_ALL):
        all_groups.setdefault(business_attributes_of(record), set()).add(record["transaction_id"])
    assert clean_groups == all_groups
    assert any(len(identities) > 1 for identities in clean_groups.values())
