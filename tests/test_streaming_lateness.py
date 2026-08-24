# tests/test_streaming_lateness.py
"""The boundary ARITHMETIC: the two watermark delays, derived with no session and no broker.

THE ARITHMETIC IS THE POINT OF THIS FILE. The two watermark delays the integration run
uses are computed from `promotable`'s declaration -- 10,000 events 5,000 ms apart, 100 of
them delayed by one `LATENESS_WINDOW_MS`, 150 redelivered -- and that computation needs no
session, no broker and no Spark. `delivered_records` is a pure function of the profile, so
the number the phase publishes BEFORE its run is guarded on every commit here and the run
is left to confirm it rather than to produce it. Measured: importing
`opl.streaming.lateness` leaves `pyspark` out of `sys.modules` entirely.

WHAT IS NEXT DOOR AND WHY. `tests/test_streaming_watermarked_dedup.py` covers the module
that RUNS things -- the operator chain, the two arms, and the refusals that compare what
they landed against the prediction this file derives. The seam is the same one the two
source modules are split on, and the reason is the same: a prediction that could only be
computed by starting the run it predicts is not a prediction.

IT ALSO GUARDS A CORRECTION, which is what
`test_the_correction_replaced_the_margin_function_and_not_one_constant` is for, and the
correction is larger than a changed constant. The first prediction was derived from the
CORPUS measure -- the narrowest frontier lag minus one batch of slack, halved -- and that
arithmetic had no per-batch watermark in it at all; it published 167,500 ms, predicted that
all 100 late rows would drop, and the run dropped 97. `watermark_margins` replaced that
function. `LATE_EVENT_WATERMARK_LAG_BATCHES` is a term inside the replacement, and setting
it to 1 gives a third number (325,000 ms) that was never published. The test runs the two
separately for that reason, and the corrected model refuses outright the rate limit that
produced the wrong run.

WHAT IT DOES NOT GUARD IS THE CONSTANT ITSELF. Every number in this file is a property of
the MODEL; the shipped configuration drops 100 under a lag of 1 and of 2 alike, so nothing
here can separate them. The run that can is
`tests/integration/test_late_arrival_boundary.py`, at a rate limit where the three
candidate lags predict three different landed counts.

AND IT GUARDS TWO NUMBERS THE RUN CANNOT SEE. That run's set equality discriminates the
LAG and little else -- three other changes to the same model leave its 97 identities
bit-identical, which `tests/integration/test_late_arrival_boundary.py` names in
`_identities_the_model_drops`. So `_batch_frontiers`'s floor marker is pinned HERE as a
value, -1 against the 0 its one alternative gives, and the delay-equals-margin case that
would separate a `<=` predicate from a `<` one is measured here as absent from the corpus.
Neither is visible in a landed count.

THE CORPUS IS BUILT ONCE AT MODULE LEVEL and costs about a second and a half -- 10,000
events derived twice, and `opl.generator.profiles` explains why. That is the price of
asserting over the real declared corpus rather than over a miniature that would need
numbers of its own.
"""
from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from opl.contracts.payments import (
    BUSINESS_ATTRIBUTE_COLUMNS,
    EVENT_TIME_COLUMN,
    IDENTITY_COLUMN,
)
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import (
    LATENESS_WINDOW_MS,
    DefectSpec,
    delivered_records,
    late_positions,
)
from opl.generator.instants import from_text
from opl.generator.measures import late_arrivals
from opl.generator.profiles import POOL_SIZE, PROFILES
from opl.generator.stream import StreamSpec
from opl.streaming import lateness as lt
from opl.streaming.lateness import (
    DROPPING,
    KEEPING,
    LATE_EVENT_WATERMARK_LAG_BATCHES,
    WatermarkMargins,
    boundary_for,
    frontier_lags_ms,
    watermark_margins,
)

_PROFILE = PROFILES["promotable"]
_DEFECTS = _PROFILE.defects
_POOL = validated_pool([f"{n:08d}" for n in range(1, POOL_SIZE + 1)])
_SPEC = _PROFILE.stream_spec(_POOL)
_RECORDS = delivered_records(_SPEC, _DEFECTS)

# The delivered event times, parsed once. Two tests below need the batch arithmetic Spark
# does over them -- which batch a row lands in, and whether the last batch carries a new
# maximum -- and `from_text` is the same parse `watermark_margins` runs.
_EVENTS = [from_text(record[EVENT_TIME_COLUMN]) for record in _RECORDS]

# The rate limit the integration run reads at, and the one every number below is stated
# against. IT IS CHOSEN, NOT SOLVED FOR: nothing in `src/` computes a rate limit and
# `boundary_for` accepts 191 of the values between 1 and 260, so 133 is picked out of a band
# -- `tests/integration/test_late_arrival_boundary.py` names what binds the band's two ends.
# Copied here rather than imported, because `tests/` is not a package. WHAT WOULD CATCH THE
# TWO COPIES DIVERGING IS THE PINNED LITERALS AND NOT `boundary_for`, which accepts 137 at
# 327,500 ms and 147 at 315,000 just as happily as 133 at 262,500: this file pins that
# 262,500 and 77 batches, and `tests/integration/test_late_arrival_boundary.py` pins the
# same 77 against the limit it runs at, so one copy moving fails against the other's number.
_PER_TRIGGER = 133

# The rate limit the FIRST, WRONG prediction was made at. A named constant because three
# assertions below are about it: the superseded arithmetic took it, the corrected model
# refuses it, and the one-constant variant of the corrected model accepts it at a third
# delay that was never published.
_WRONG_PER_TRIGGER = 200

# `spark.sql.streaming.numRecentProgressUpdates`, whose default
# `opl.streaming.ingest._progress_of` reads and refuses AT. Spelled here because that
# function takes it from a live session's conf and there is no constant to import.
_RING_CAP = 100

# Predicted from the declaration: a record delayed by one window at a 5,000 ms interval
# lands exactly on the emission instant of the event 720 later and is sorted ahead of it,
# so the newest event already delivered is 719 events after its own.
_WIDEST_LAG = LATENESS_WINDOW_MS - _SPEC.event_interval_ms

# The three candidate lags `tests/integration/test_late_arrival_boundary.py` puts under a
# run. Named here because the delay-equals-margin case is claimed of ALL THREE, and a test
# that measured only the shipped one would be checking a third of its own sentence.
_CANDIDATE_LAGS = (1, 2, 3)

# A SECOND CORPUS, AND THE ONLY THING IT HAS THAT `promotable` DOES NOT: a late record that
# is ALSO redelivered. `promotable`'s late and duplicate sets are disjoint, so the
# cancellation the prediction rests on is unexercised there -- 400 events with 40 of each
# on one seed put 4 identities in both sets, and the whole corpus derives in about a tenth
# of a second. 20 records a trigger is one of the 39 limits `boundary_for` accepts over its
# 440 delivered records, the corpus being small enough that the sweep is the whole range.
_OVERLAP_DEFECTS = DefectSpec(duplicate_count=40, late_count=40, late_by_ms=LATENESS_WINDOW_MS)
_OVERLAP_SPEC = StreamSpec(
    seed=1,
    stream_id="OVERLAP",
    event_count=400,
    repeat_count=0,
    window_start=_SPEC.window_start,
    event_interval_ms=_SPEC.event_interval_ms,
    emission_lag_ms=_SPEC.emission_lag_ms,
    cnpj_pool=_POOL,
)
_OVERLAP_RECORDS = delivered_records(_OVERLAP_SPEC, _OVERLAP_DEFECTS)
_OVERLAP_PER_TRIGGER = 20
_OVERLAP_LATE_IDENTITIES = frozenset(
    _OVERLAP_RECORDS[position][IDENTITY_COLUMN] for position in late_arrivals(_OVERLAP_RECORDS)
)


def _margins(*, late=(1,), punctual=(-1,)) -> WatermarkMargins:
    return WatermarkMargins(
        late_rows=len(late),
        late_identities=len(late),
        narrowest_late_ms=min(late),
        widest_late_ms=max(late),
        widest_punctual_ms=max(punctual),
    )


def _identities_the_model_removes(records, *, per_trigger: int, delay_ms: int) -> set[str]:
    """Which identities a delay of `delay_ms` removes ENTIRELY, under the shipped model.

    `lt._batch_frontiers` and not a second spelling of it, so what is simulated is the
    module under test. AN IDENTITY SURVIVES WHILE ANY ONE OF ITS COPIES DOES: a redelivery
    sits at a different delivered position, so it meets a different batch's watermark."""
    events = [from_text(record[EVENT_TIME_COLUMN]) for record in records]
    frontiers = lt._batch_frontiers(events, per_trigger)
    surviving = {
        record[IDENTITY_COLUMN]
        for position, record in enumerate(records)
        if delay_ms > (
            lt._UNREACHABLE
            if frontiers[position // per_trigger] is None
            else frontiers[position // per_trigger] - events[position]
        )
    }
    return {record[IDENTITY_COLUMN] for record in records} - surviving


def _positions_whose_margin_equals_a_delay(*, per_trigger: int, lag: int, delays) -> list[int]:
    """Delivered positions sitting EXACTLY ON one of `delays`, under a lag of `lag` batches.

    The one case `event_time <= watermark` and `event_time < watermark` answer differently.
    `lt._batch_frontiers` is read at a lag this function supplies, so the claim can be
    stated over every candidate lag rather than only over the shipped one."""
    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(lt, "LATE_EVENT_WATERMARK_LAG_BATCHES", lag)
        frontiers = lt._batch_frontiers(_EVENTS, per_trigger)
    return [
        position
        for position, event in enumerate(_EVENTS)
        if frontiers[position // per_trigger] is not None
        and frontiers[position // per_trigger] - event in delays
    ]


def test_the_corpus_this_file_reasons_over_is_the_declared_one():
    """THE FLOOR UNDER EVERY NUMBER BELOW. A `_POOL` or a profile that stopped producing
    `promotable`'s shape would otherwise send every margin quietly somewhere else."""
    identities = [record[IDENTITY_COLUMN] for record in _RECORDS]
    attributes = {tuple(r[c] for c in BUSINESS_ATTRIBUTE_COLUMNS) for r in _RECORDS}
    assert len(_RECORDS) == _PROFILE.delivered_row_count == 10_150
    assert len(set(identities)) == _PROFILE.event_count == 10_000
    assert len(identities) - len(set(identities)) == _DEFECTS.duplicate_count == 150
    assert len(attributes) == _PROFILE.event_count - _PROFILE.repeat_count == 9_200
    assert _DEFECTS.late_count == 100
    assert _DEFECTS.late_by_ms == LATENESS_WINDOW_MS == 3_600_000


def test_the_widest_frontier_lag_is_the_window_minus_one_interval():
    """WHAT MAKES THE CORPUS A LATE-ARRIVAL CORPUS, and it is a fact about the corpus alone.

    The delay is exactly one `LATENESS_WINDOW_MS` and the interval is 5,000 ms, so a
    delayed record's emission instant COINCIDES with that of the event 720 places later --
    and `defects._delivery_order` breaks that tie by emission index, which the earlier
    (late) record wins. The newest `event_time` already delivered is therefore index+719's
    rather than index+720's, and the whole measure turns on that one place."""
    lags = frontier_lags_ms(_SPEC, _DEFECTS)
    assert len(lags) == _DEFECTS.late_count == 100
    assert lags == tuple(sorted(lags))
    assert lags[-1] == _WIDEST_LAG == 3_595_000
    assert lags.count(_WIDEST_LAG) == 92, (
        "92 of the 100 sit at the full lag and 8 do not -- if every one of them did, the "
        "end-of-stream case the narrowest lag comes from would be untested by this corpus"
    )
    assert lags.count(_WIDEST_LAG - _SPEC.event_interval_ms) == 1, (
        "and only SEVEN of those 8 are the end-of-stream case. The eighth is emission "
        "index 4927, whose frontier candidate is itself late -- so `_frontier_index` "
        "walks back one place, which is that branch's only firing over this corpus"
    )


def test_the_narrowest_lag_belongs_to_the_last_late_record_in_the_stream():
    """AND IT IS NOT THE FULL LAG. A record delayed past the END of the stream has nothing
    720 places later to be behind: its frontier is the stream's final event. So the
    narrowest lag is the LAST selected index's distance from `event_count - 1`, and a delay
    derived from the full lag alone would be far too wide to drop it."""
    lags = frontier_lags_ms(_SPEC, _DEFECTS)
    last = max(late_positions(_SPEC, _DEFECTS))
    assert lags[0] == (_SPEC.event_count - 1 - last) * _SPEC.event_interval_ms
    assert lags[0] == 1_330_000 < _WIDEST_LAG
    assert last == 9_733


def test_the_margins_are_the_lags_a_batched_reader_actually_sees():
    """THE MEASURE THE DELAYS ARE DERIVED FROM, and it is smaller than the lag above.

    A margin is how far a row sits below the watermark ITS OWN BATCH is filtered against,
    so it pays for both the micro-batch boundary and the extra batch of lag. Every injected
    record still has a positive one at this rate limit -- which is what lets a single delay
    drop all 100 -- and no punctual row does."""
    margins = watermark_margins(_RECORDS, max_offsets_per_trigger=_PER_TRIGGER)
    assert (margins.late_rows, margins.late_identities) == (100, _DEFECTS.late_count)
    assert margins.narrowest_late_ms == 525_000 > 0
    assert margins.widest_late_ms == 2_945_000
    assert margins.narrowest_late_ms < frontier_lags_ms(_SPEC, _DEFECTS)[0], (
        "a margin that were not smaller than the corpus's own lag would mean the batching "
        "cost nothing, and the correction this module records would be about nothing"
    )
    assert margins.widest_punctual_ms == lt._UNREACHABLE == -1, (
        "no punctual row may be reachable by any legal delay, or a boundary narrow enough "
        "to take every late record would take ordinary rows with it -- and the VALUE is "
        "pinned rather than the inequality, for the reason the next test measures. The "
        "literal is here as well as there because comparing only against the sentinel "
        "would be satisfied by any value the sentinel was changed to"
    )


def test_the_first_batches_are_behind_a_floor_and_not_behind_the_first_record():
    """`_batch_frontiers`'s FLOOR MARKER, PUT UNDER THE ONE NUMBER THAT TELLS IT APART.

    The first `LATE_EVENT_WATERMARK_LAG_BATCHES` batches are filtered against a watermark
    that has not left its floor, so `_batch_frontiers` MARKS them rather than computing a
    frontier for them and every row in them takes `_UNREACHABLE`. The one other thing the
    function could compute there is the running maximum's first value -- the first
    delivered record's own event time -- and that alternative is invisible to the rest of
    this phase: it refuses the same rate limits, leaves the falsifier run's 97 identities
    bit-identical, and passes a `widest_punctual_ms < 1` assertion, because what it puts
    there is 0.

    SO THE TWO NUMBERS ARE 0 AND -1 and this is where they are separated. The alternative
    is COMPUTED below rather than described, so what is ruled out is an arithmetic and not
    a sentence about one."""
    margins = watermark_margins(_RECORDS, max_offsets_per_trigger=_PER_TRIGGER)
    lag = LATE_EVENT_WATERMARK_LAG_BATCHES
    frontiers = lt._batch_frontiers(_EVENTS, _PER_TRIGGER)

    assert frontiers[:lag] == [None] * lag
    assert frontiers[lag] == max(_EVENTS[:_PER_TRIGGER]) != _EVENTS[0]
    assert margins.widest_punctual_ms == lt._UNREACHABLE == -1

    marked = lag * _PER_TRIGGER
    late = set(late_arrivals(_RECORDS))
    alternative = max(
        (_EVENTS[0] if position < marked else frontiers[position // _PER_TRIGGER]) - event
        for position, event in enumerate(_EVENTS)
        if position not in late
    )
    assert alternative == 0 != margins.widest_punctual_ms, (
        "the alternative reading of those batches, run rather than argued: it lands the "
        "FIRST delivered row on a margin of exactly 0, which is above no legal delay and "
        "below none either -- and 'widest punctual < 1' cannot see the difference"
    )
    assert min(_EVENTS[0] - event for event in _EVENTS[:marked]) == -1_320_000, (
        "and the other 265 rows in those batches spread below it, which is the shape "
        "`_batch_frontiers`'s docstring names"
    )


def test_the_correction_replaced_the_margin_function_and_not_one_constant(monkeypatch):
    """THE CORRECTION, RUN RATHER THAN DESCRIBED -- AND IT IS NOT THIS MODULE WITH A 1 IN IT.

    THE SUPERSEDED DERIVATION IS REBUILT FROM `frontier_lags_ms`, which is the measure it
    actually used: the narrowest frontier lag is 1,330,000 ms, a batch of 200 records at a
    5,000 ms interval reaches 995,000 ms back, and halving what is left gives exactly the
    167,500 ms that was published. That arithmetic contains no per-batch watermark at all,
    which is why no value of a lag constant could have saved it. It predicted that all 100
    late rows would drop; the run dropped 97.

    `watermark_margins` REPLACED it rather than parameterising it, so the two are asserted
    separately. Setting `LATE_EVENT_WATERMARK_LAG_BATCHES` to 1 -- the whole of what "the
    one-batch model" could mean inside today's code -- accepts that same rate limit at
    325,000 ms, a THIRD number never published and never run. A test that monkeypatched the
    constant and called the result "the superseded model" would be comparing today's model
    against a hybrid that never existed.

    What the corrected model does to that rate limit is refuse it outright, which is the
    half of this that stops the mistake being repeated by choosing 200 again."""
    assert LATE_EVENT_WATERMARK_LAG_BATCHES == 2

    narrowest_lag = frontier_lags_ms(_SPEC, _DEFECTS)[0]
    one_batch_of_slack = (_WRONG_PER_TRIGGER - 1) * _SPEC.event_interval_ms
    assert (narrowest_lag, one_batch_of_slack) == (1_330_000, 995_000)
    assert (narrowest_lag - one_batch_of_slack) // 2 == 167_500, (
        "the superseded derivation, rebuilt: this is the number that was published"
    )

    corrected = watermark_margins(_RECORDS, max_offsets_per_trigger=_WRONG_PER_TRIGGER)
    assert corrected.narrowest_late_ms == -350_000
    with pytest.raises(ValueError, match="behind no watermark this read produces"):
        boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=_WRONG_PER_TRIGGER)

    monkeypatch.setattr(lt, "LATE_EVENT_WATERMARK_LAG_BATCHES", 1)
    one_constant = watermark_margins(_RECORDS, max_offsets_per_trigger=_WRONG_PER_TRIGGER)
    assert one_constant.narrowest_late_ms == 650_000
    accepted = boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=_WRONG_PER_TRIGGER)
    assert accepted.dropping_delay_ms == 325_000 != 167_500, (
        "changing the constant does not reproduce the superseded derivation -- the whole "
        "margin function was replaced, and this is the number that change would give"
    )


def test_the_boundary_sits_inside_the_margins_that_placed_it():
    """THE TWO DELAYS THE RUN USES, and the inequalities that make them a pair.

    At or below the narrowest late margin, so EVERY injected record is behind the dropping
    arm's watermark; strictly above the widest, so NONE is behind the keeping arm's. Stated
    as the comparisons first and then pinned as literals, because a changed derivation that
    still satisfied the inequalities would be a different experiment reported under this
    one's numbers."""
    boundary = boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=_PER_TRIGGER)
    margins = boundary.margins

    assert boundary.dropping_delay_ms <= margins.narrowest_late_ms
    assert boundary.dropping_delay_ms > margins.widest_punctual_ms
    assert boundary.keeping_delay_ms > margins.widest_late_ms

    assert (boundary.dropping_delay_ms, boundary.keeping_delay_ms) == (262_500, 3_600_000)
    assert boundary.keeping_delay_ms == _DEFECTS.late_by_ms == LATENESS_WINDOW_MS
    assert boundary.max_offsets_per_trigger == _PER_TRIGGER
    assert boundary.late_count == _DEFECTS.late_count == 100


def test_the_batch_split_the_margins_were_derived_under_is_stated_as_a_number():
    """`batches_over` IS THE MARGINS' OWN PRECONDITION, MADE COMPARABLE TO A RUN.

    `watermark_margins` puts delivered position `p` in batch `p // per_trigger`, so every
    frontier it computes is a statement about a read that took exactly that many offsets a
    batch. Nothing said so out loud until this method existed, and the rate limit is passed
    TWICE -- once here and once to `ingest.payment_stream` -- so the two copies could differ
    with nothing to notice.

    BOTH CASES THE PHASE ACTUALLY RUNS, AND THEY DIVIDE DIFFERENTLY. 10,150 at 133 is 77
    batches with a partial last one, which is what the integration file
    `tests/integration/test_late_arrival_boundary.py` reads off Spark's own progress, and it
    is the case a floor written where a ceiling belongs gets wrong by exactly one. 10,150 at
    175 is 58 EXACTLY, the falsifier arm's, and a floor and a ceiling agree there -- so one
    case alone would leave half the arithmetic untested."""
    boundary = boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=_PER_TRIGGER)
    assert boundary.batches_over(len(_RECORDS)) == 77
    assert 76 * _PER_TRIGGER < len(_RECORDS) < 77 * _PER_TRIGGER, (
        "the last batch is PARTIAL here, which is what makes 77 discriminate: a floor "
        "would say 76 and a run of 76 batches would never reach the corpus"
    )

    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(lt, "LATE_EVENT_WATERMARK_LAG_BATCHES", 1)
        falsifier = boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=175)
    assert falsifier.batches_over(len(_RECORDS)) == 58
    assert 58 * 175 == len(_RECORDS), "and this one divides exactly"


def test_the_predicted_difference_is_the_injected_late_count_whatever_the_overlap():
    """WHY THE PREDICTION IS 100 AND NOT "100 PLUS THE REDELIVERED ONES".

    If a late record is also selected for redelivery, both its copies are dropped by the
    watermark and the dedup then has one fewer redelivery left to collapse. The two cancel
    exactly, so the difference between the LANDED counts is the injected late count.

    WHAT SETTLES THE FIRST HALF IS NOT THAT THE COPIES SHARE TIMESTAMPS, which an earlier
    version of this docstring said. They sit at adjacent DELIVERED positions and can
    straddle a batch boundary, so they are filtered against two different watermarks. What
    covers them is that acceptance is stated over late ROWS: `narrowest_late_ms` is a
    minimum over every late row, both copies included, and the dropping delay is at or
    below it -- and `_batch_frontiers`'s prefix is a running maximum, so the later copy is
    never behind a narrower frontier than the earlier one.

    OVER `promotable` THE OVERLAP IS ZERO, so it is the second corpus that exercises the
    cancellation rather than restating it: 4 of its 40 late records are redelivered, which
    makes 44 late ROWS against 40 late IDENTITIES -- and the model removes the 40."""
    boundary = boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=_PER_TRIGGER)
    assert boundary.dropped_rows == _DEFECTS.late_count == 100
    assert boundary.margins.late_rows == boundary.margins.late_identities == 100

    overlapping = boundary_for(
        _OVERLAP_RECORDS, _OVERLAP_DEFECTS, max_offsets_per_trigger=_OVERLAP_PER_TRIGGER
    )
    margins = overlapping.margins
    assert (margins.late_rows, margins.late_identities) == (44, 40), (
        "4 of the 40 late records are redelivered and BOTH copies of each measure as late "
        "-- which is what puts them under the minimum the dropping delay is taken from"
    )
    removed = _identities_the_model_removes(
        _OVERLAP_RECORDS,
        per_trigger=_OVERLAP_PER_TRIGGER,
        delay_ms=overlapping.dropping_delay_ms,
    )
    assert removed == _OVERLAP_LATE_IDENTITIES
    assert len(removed) == overlapping.dropped_rows == _OVERLAP_DEFECTS.late_count == 40, (
        "44 rows go and 40 identities with them, so the difference between the landed "
        "counts is the injected late count and not the late ROW count -- which is the "
        "half `promotable` cannot show, having no overlap in it"
    )


def test_the_boundary_names_its_two_arms_and_no_third():
    boundary = boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=_PER_TRIGGER)
    assert boundary.delay_of(DROPPING) == boundary.dropping_delay_ms
    assert boundary.delay_of(KEEPING) == boundary.keeping_delay_ms
    with pytest.raises(ValueError, match="is not one of"):
        boundary.delay_of("wide-ish")


def test_a_read_that_cannot_advance_a_watermark_is_refused_before_a_delay_exists():
    """TRAP 2. Spark filters a batch's late events against a watermark computed from
    EARLIER batches, so a single-batch run starts at the floor and drops nothing -- and the
    dropping arm would land exactly what the keeping arm lands, which reads as a pass.

    MEASURED, not argued: the same corpus at the same delay with no rate limit lands 10,000
    rows -- every redelivery collapsed and every late arrival kept.

    `boundary_for` refuses to compute delays for such a read at all, so the trap cannot be
    fallen into by leaving an argument out."""
    for unrated in (None, 0, -1):
        with pytest.raises(ValueError, match="Spark filters a batch's late events"):
            boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=unrated)


def test_the_rate_limits_this_corpus_cannot_carry_are_a_sawtooth_and_not_a_ceiling():
    """A batch boundary pulls the late-event watermark back, so a limit that puts the LAST
    late record early enough in its own micro-batch leaves that record behind no watermark
    the read produces -- and there is then no delay that drops EVERY one of them. A partial
    drop is a difference the declaration cannot predict, so it is refused rather than
    measured.

    WHICH LIMITS THOSE ARE IS NOT A CEILING, and an earlier version of this docstring said
    it was. Where the last late record falls inside its batch does not vary monotonically
    with the limit, so acceptance ALTERNATES above a point: 143 and 145 are refused while
    144, 146 and even 260 are accepted. Measured over every limit from 1 to 260: 191
    accepted, 69 refused, every limit from 1 to 142 accepted, and the first refusal at 143.

    SEVEN LIMITS RATHER THAN THE SWEEP, because `boundary_for` re-derives the margins over
    all 10,150 delivered records and costs about a second a call -- the full sweep is four
    and a half minutes. These seven are what separates a sawtooth from a ceiling: two
    refusals INTERLEAVED with acceptances (143 against 144, 145 against 146), the limit an
    earlier version of this file called the last one this corpus carries (150), a refusal
    above it (175), and the far end of the range still accepted (260)."""
    delays = {}
    for accepted in (144, 146, 150, 260):
        boundary = boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=accepted)
        assert boundary.dropped_rows == _DEFECTS.late_count
        delays[accepted] = boundary.dropping_delay_ms
    assert delays == {144: 167_500, 146: 147_500, 150: 80_000, 260: 30_000}, (
        "what an accepted limit buys is a DELAY that clears the margins, and the limit "
        "moves it -- 103 distinct dropping delays over the 191 accepted. The equality "
        "above finds nothing: `dropped_rows` returns `defects.late_count` and reads "
        "neither the limit nor the margins, so it is the declaration read back"
    )
    for refused in (143, 145, 175):
        with pytest.raises(ValueError, match="Lower max_offsets_per_trigger"):
            boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=refused)


def test_the_finest_accepted_rate_limits_are_not_ones_a_run_could_use():
    """AN ACCEPTED LIMIT IS NOT A RUNNABLE ONE, and nothing at the call site says so.

    `boundary_for` reads the corpus and knows nothing about a query.
    `opl.streaming.ingest._progress_of` refuses a run whose progress updates REACH
    `numRecentProgressUpdates`, because the buffer is a ring and any total taken over a full
    one is an undercount -- and it counts EVERY progress, not only the consuming ones.

    THE EXTRA PROGRESS HAS A PRECONDITION, AND IT IS ASSERTED BELOW RATHER THAN ASSUMED. A
    stateful `availableNow` query runs one further batch that consumes nothing when its LAST
    CONSUMING BATCH ADVANCES THE WATERMARK -- the state eviction -- so `batches + 1` is the
    count that meets the cap only where that holds, and it does not hold everywhere: this
    corpus's newest event time sits at delivered position 10,142 of 10,150, so a limit fine
    enough to leave it out of the last batch -- 5, say -- evicts nothing extra. The eviction
    batch itself was measured at 175 records a trigger, in the F5 falsifier arm's
    checkpoint: `commits` 0..58, `offsets/57` and `offsets/58` both ending at `{"0":10150}`.

    That puts the floor at 104 records a trigger over this corpus: 103 is 99 consuming
    batches, one eviction batch and so 100 progress updates, which is the cap exactly. Every
    limit from 1 to 103 is accepted by `boundary_for` and refused by the run -- 103 of the
    191 it accepts between 1 and 260. The shipped 133 is clear of it at 77 batches.

    THE REFUSAL ITSELF IS NOT RE-TESTED HERE. `tests/test_streaming_delta_ingest.py` runs
    both its arms over a fake session and pins the 100 against a real one; what this test
    adds is that the two facts MEET inside the region `boundary_for` calls acceptable, which
    neither file could see on its own."""
    for runnable_or_not in (103, 104):
        assert boundary_for(
            _RECORDS, _DEFECTS, max_offsets_per_trigger=runnable_or_not
        ).dropping_delay_ms > 0

    batches = {limit: -(-len(_RECORDS) // limit) for limit in (103, 104, _PER_TRIGGER)}
    assert batches == {103: 99, 104: 98, _PER_TRIGGER: 77}
    assert batches[103] + 1 >= _RING_CAP, "accepted above, and refused by the run"
    assert batches[104] + 1 < _RING_CAP, "the first limit that is both"
    assert batches[_PER_TRIGGER] + 1 < _RING_CAP

    advances = {}
    for limit in (5, 103, 104, _PER_TRIGGER):
        start = (-(-len(_RECORDS) // limit) - 1) * limit
        advances[limit] = max(_EVENTS[start:]) > max(_EVENTS[:start])
    assert advances == {5: False, 103: True, 104: True, _PER_TRIGGER: True}, (
        "the `+ 1` above is the state-eviction batch, and it follows only where the last "
        "consuming batch moves the watermark. At the three limits the floor is stated "
        "over it does; at 5 records a trigger it does not, so the floor is arithmetic on "
        "this corpus rather than a constant of `availableNow`"
    )


def test_a_corpus_with_no_late_arrivals_is_refused_rather_than_measured():
    """Both arms would land identical rows and their difference -- the whole measurement --
    would be 0 and true, which is what a run that never happened also reports.

    THE MESSAGE NAMES WHAT THE CALLER COUNTED, which it did not until `counting` was a
    parameter: `frontier_lags_ms` counts EMISSION INDICES out of the spec and `boundary_for`
    counts DELIVERED IDENTITIES that measure as late, and the single hardcoded noun
    described the first while being raised by the second."""
    with pytest.raises(ValueError, match="declares 0 late arrivals and 0 emission indices"):
        frontier_lags_ms(_SPEC, DefectSpec(duplicate_count=150))
    with pytest.raises(
        ValueError, match="declares 0 late arrivals and 100 delivered identities"
    ):
        boundary_for(
            _RECORDS, DefectSpec(duplicate_count=150), max_offsets_per_trigger=_PER_TRIGGER
        )


@pytest.mark.parametrize("interval", [LATENESS_WINDOW_MS, LATENESS_WINDOW_MS * 2])
def test_a_spec_whose_interval_swallows_the_delay_is_refused(interval):
    """`_refuse_a_lag_that_is_not_a_lag`'s FAILURE ARM, and it is reached through a spec
    both `StreamSpec` and `DefectSpec` accept rather than through a corrupt tuple.

    `frontier_lags_ms` reaches `ceil(late_by_ms / event_interval_ms) - 1` events past a late
    record to find its frontier. At an interval as wide as the delay that reach is ZERO, so
    the walk starts on the record's OWN index, finds it late, and steps BACKWARD -- and the
    lag comes back negative rather than merely small. Nothing else refuses this: the delay
    is a full `LATENESS_WINDOW_MS`, which is the smallest `DefectSpec` permits, and an
    interval is legal at any positive value.

    A negative lag admitted here would set the dropping delay below zero, and an arm with a
    negative watermark delay is an arm with no boundary in it."""
    flat = StreamSpec(
        seed=1,
        stream_id="FLAT",
        event_count=10,
        repeat_count=0,
        window_start=_SPEC.window_start,
        event_interval_ms=interval,
        emission_lag_ms=0,
        cnpj_pool=_POOL,
    )
    with pytest.raises(ValueError, match=r"the narrowest frontier lag is -"):
        frontier_lags_ms(flat, DefectSpec(late_count=3, late_by_ms=LATENESS_WINDOW_MS))


def test_a_dropping_delay_that_could_take_a_punctual_row_is_refused():
    """THE OTHER HALF OF THE BOUNDARY, AND IT IS NOT HYPOTHETICAL. A redelivery carries its
    original's `event_time` verbatim, so it can sit exactly ON the frontier -- under the
    superseded one-batch model this corpus has punctual rows at a margin of 0, which a delay
    of 0 would take. "100 rows were dropped" would then be true of a boundary that dropped
    100 of the wrong ones.

    Called directly, because `boundary_for` halves a positive narrowest margin and this
    corpus never brings the two within reach of each other -- the arm that cannot be reached
    through the front door is reached here, as `tests/test_streaming_delta_ingest.py` does
    for its own unreachable refusal."""
    refuse = lt._refuse_a_dropping_delay_that_reaches_a_punctual_row
    refuse(1, _margins(late=(10,), punctual=(0,)))
    with pytest.raises(ValueError, match="not above the widest punctual"):
        refuse(0, _margins(late=(10,), punctual=(0,)))
    with pytest.raises(ValueError, match="not above the widest punctual"):
        refuse(5_000, _margins(late=(10_000,), punctual=(5_000,)))


def test_a_keeping_delay_equal_to_the_widest_margin_is_refused():
    """AN EQUAL DELAY IS THE CASE THIS PHASE CANNOT CALL, WHICH IS WHY IT IS REFUSED. It
    puts the watermark exactly ON the record's own event time, the one place where
    `event_time <= watermark` and `event_time < watermark` disagree.

    AN EARLIER VERSION OF THIS DOCSTRING SAID THE STRICTNESS WAS SPARK'S RATHER THAN THIS
    MODULE'S TASTE. That was a reading of Spark and not a run of it, and no run in F5 tests
    it: the assertion below measures that NO delivered record's margin equals either arm's
    delay -- at the shipped 133 and at the falsifier's 175, where the two arms and the third
    ran, and under each of the three candidate lags, which is the SIX combinations the
    module's own sentence is stated over -- so every set this phase compared would have come
    out the same under `<`. The guard refuses the ambiguous case instead of resolving it.

    CALLED DIRECTLY, AND FOR THE SAME REASON ITS SIBLING ABOVE IS. The keeping delay IS
    `defects.late_by_ms`, and over `promotable` it clears the widest late margin by at least
    one event interval at every limit from 1 to 260 that `boundary_for` accepts. 5,000 ms is
    the smallest of those 191 gaps and belongs to a limit of 1 ALONE; it does not then widen
    with the limit, since 25 leaves 125,000 ms and 26 leaves 120,000. So this arm is not
    reachable through the front door over THIS corpus, which is a fact about the corpus and
    not a reason to stop asserting it."""
    refuse = lt._refuse_a_keeping_delay_that_does_not_clear_the_widest_margin
    refuse(_WIDEST_LAG + 1, _margins(late=(_WIDEST_LAG,)))
    with pytest.raises(ValueError, match="which way the comparison falls"):
        refuse(_WIDEST_LAG, _margins(late=(_WIDEST_LAG,)))

    boundary = boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=_PER_TRIGGER)
    delays = (boundary.dropping_delay_ms, boundary.keeping_delay_ms)
    assert delays == (262_500, 3_600_000)
    equal = {
        (per_trigger, lag): _positions_whose_margin_equals_a_delay(
            per_trigger=per_trigger, lag=lag, delays=delays
        )
        for per_trigger in (_PER_TRIGGER, 175)  # 175: the falsifier arm, run at 262,500 ms
        for lag in _CANDIDATE_LAGS
    }
    assert len(equal) == 6 and not any(equal.values()), (
        "a record whose margin equalled a delay would be dropped under `<=` and kept "
        "under `<`. This corpus has none at either rate limit under any of the three "
        f"candidate lags, so the strictness above is refused rather than measured: {equal}"
    )


def test_the_boundary_is_a_frozen_record_of_its_own_derivation():
    """`LatenessBoundary` carries the margins beside the delays so a reader can check the
    inequalities without re-deriving anything -- and it is frozen, because a delay adjusted
    after its prediction was published is the one edit this phase's method is against.

    THE EXCEPTION TYPE IS THE ASSERTION, not a word in the message: `FrozenInstanceError`
    reads "cannot assign to field ..." and never says "frozen", so a `match="frozen"` here
    failed against a dataclass that was correctly frozen -- a test that would have been
    "fixed" by unfreezing the very thing it guards."""
    boundary = boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=_PER_TRIGGER)
    with pytest.raises(FrozenInstanceError, match="cannot assign to field"):
        boundary.dropping_delay_ms = 1
    with pytest.raises(FrozenInstanceError, match="cannot assign to field"):
        boundary.margins.narrowest_late_ms = 1
