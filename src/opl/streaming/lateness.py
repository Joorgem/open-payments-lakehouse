# src/opl/streaming/lateness.py
"""T4's ARITHMETIC: the two watermark delays one corpus is read at, and nothing that runs.

`opl.streaming.watermarked_dedup` is the operator chain and the run. This is the half that
happens BEFORE either: `boundary_for` reads the delivered records, a `DefectSpec` and a rate
limit, starts no session, opens no socket and reads no clock, and returns the pair of delays
the run is then left to confirm. That is the whole reason the two are separate files -- a
prediction that could only be computed by starting the thing it predicts is not a
prediction, and keeping the derivation importable without pyspark is what makes the number
publishable before the run.

The seam is not the one the chain itself must not be split on. `withWatermark` and
`dropDuplicatesWithinWatermark` are one operator chain and they stay together next door;
what moves here is the arithmetic that places one of their arguments.

--- THE WATERMARK SPARK FILTERS LATE ROWS AGAINST IS TWO BATCHES OLD ---------------------

THIS SECTION EXISTS BECAUSE THE PREDICTION IT CORRECTS WAS PUBLISHED WRONG FIRST. A model
paying for ONE batch of slack -- in effect "the watermark applied to batch N is computed
from batches 0..N-1", though the arithmetic it was actually written in is the next
section's and not this module's -- predicted 100 dropped rows and the run dropped 97. It
is not a rounding error and it was not adjusted away; it is a mechanism, and it was measured
rather than recalled:

  * Spark's own `StreamingQueryProgress.eventTime.watermark` for batch N equals
    `max(event_time over batches 0..N-1) - delay` exactly -- that model's own value.
  * The three rows that survived (file positions 10147, 10148, 10149) carry event times
    strictly BETWEEN the value reported for batch N-1 and the value reported for batch N.
    So the value they were filtered against was the one reported for the PREVIOUS batch.
  * Re-simulating the whole corpus in pure Python at a lag of 1, 2 and 3 batches
    reproduces Spark's outcome at 2 and at neither of the others -- 97 rows and those same
    three positions, not merely the same count.

WHAT THOSE THREE ESTABLISH IS THE EFFECT, AND THE EFFECT IS ALL THAT IS CLAIMED: a
batch's late events meet the value reported for the PREVIOUS batch, which is the second
batch of lag `LATE_EVENT_WATERMARK_LAG_BATCHES` names and `watermark_margins` pays. WHY
Spark carries two watermarks is a reading of its source rather than a run of it, and an
earlier version of this paragraph made that reading flatly -- ten lines above a constant
whose own comment insists it is a measurement.

THE CORRECTION WAS NOT A CHANGED CONSTANT, and calling it one misreports what moved. The
published 167,500 ms came from a DIFFERENT FUNCTION -- the narrowest frontier lag minus
one batch of slack, halved: `(1,330,000 - 995,000) // 2` at 200 records a trigger -- which
had no per-batch watermark in it at all, and so had nowhere for a lag of any size to go.
`watermark_margins` REPLACED that arithmetic; the constant is a term inside the
replacement, not a knob on the original. Setting it to 1 today gives 325,000 ms at that
same rate limit -- a third number, never published and never run.
`tests/test_streaming_lateness.py` re-runs the superseded derivation and the one-constant
variant SEPARATELY, because a test that monkeypatched the constant and called the result
"the superseded model" would be describing a hybrid that never existed.

AND THE CONSTANT IS NOW UNDER A RUN RATHER THAN UNDER THIS PARAGRAPH. The shipped
configuration cannot test it: at 133 records a trigger and a delay of 262,500 ms a lag of 1
and a lag of 2 remove the SAME 100 identities, so the headline pair would read identically
with either. 175 records a trigger separates them -- `boundary_for` accepts it at a lag of 1
and refuses it at 2 -- and at the delay the one-batch model derives there, the three
candidate lags name three DIFFERENT sets: 100 identities at a lag of 1, 97 at 2, 95 at 3.
MEASURED, over Redpanda, one arm: Spark removed the 97 the two-batch model names, element
for element and not merely in count. `tests/integration/test_late_arrival_boundary.py`
carries that arm.

--- THE TWO DELAYS ARE DERIVED, ABOVE A RATE LIMIT THAT IS CHOSEN -----------------------

Both are arithmetic over the DELIVERED CORPUS AND THE RATE LIMIT THE READ WILL USE. The
corpus half is a pure function of the profile -- `delivered_records` starts no session,
opens no socket and reads no clock -- so both delays exist before anything runs. The rate
limit half is not derived by anything here: it is an ARGUMENT and a choice, and
`boundary_for` is where what that choice does and does not buy is written down. There are
two corpus measures below and they answer different questions.

`frontier_lags_ms` -- WHY THE CORPUS CAN EXPRESS LATENESS AT ALL, independent of any
reader. IT IS NOT ON THE DELAY PATH: nothing else in `src/` calls it, `boundary_for` reads
`watermark_margins` and only that, and this measure ships as a SECOND INSTRUMENT reachable
from the tests that state what the corpus is. It is kept rather than deleted because the
section above is about the two being confused, and a reader who can see only the one the
delays came from cannot check that it was the right one. A late record's `emitted_at` was
pushed forward by `defects.late_by_ms` while its `event_time` STAYED PUT (the payment did
not move, the delivery did), so at the instant it arrives the newest `event_time` already
delivered belongs to a record further down the stream. Over `promotable` that distance is
3,595,000 ms for 92 of the 100 -- one window minus one interval, because a delay of exactly
one window lands the record on the emission instant of the event 720 later and the delivery
sort's tie-break by emission index puts it ahead of that record, leaving index+719 as the
frontier. SEVEN of the other 8 are nearer the end of the stream, where there is no event
720 later and the frontier is the stream's last event; the narrowest is 1,330,000 ms. The
eighth is emission index 4927, mid-stream and with an event 720 later: its frontier
candidate 5646 is ITSELF late, so `_frontier_index` walks back to 5645 and the lag comes
out 3,590,000 ms. That walk-back is the second mechanism, and its only firing here.

`watermark_margins` -- WHAT A PARTICULAR READER'S DELAY MUST BE NARROWER THAN. The same
distance, measured against the watermark Spark actually filters against, which is behind
the frontier by both the micro-batch boundary and the extra batch above. THE DELAYS ARE
DERIVED FROM THIS ONE, and the section above is why: the first measure alone is a
statement about the corpus and was mistaken for a statement about the run.

    DROPPING DELAY = half the NARROWEST late margin. Every injected record is then behind
    the watermark of the batch it is read in, and -- separately checked -- no PUNCTUAL row
    is, which the widest punctual margin makes a comparison rather than a hope.

    KEEPING DELAY = `defects.late_by_ms`. It must EXCEED the widest late margin rather than
    equal it: a delay equal to the margin puts the watermark exactly ON the record's own
    event time, which is the one case a `<=` predicate and a `<` one answer differently.
    NO RUN IN THIS PHASE REACHES IT -- measured below -- so the guard refuses the case
    instead of choosing an answer for it.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from opl.contracts.payments import EVENT_TIME_COLUMN, IDENTITY_COLUMN
from opl.generator.defects import DefectSpec, late_positions
from opl.generator.instants import from_text
from opl.generator.measures import late_arrivals
from opl.generator.stream import StreamSpec

# HOW FAR BEHIND THE REPORTED WATERMARK SPARK'S LATE-EVENT FILTER RUNS, in micro-batches.
# 2, and it is a measurement rather than a reading of the source -- the module docstring's
# first section gives the three observations that fix it and the one they rule out.
LATE_EVENT_WATERMARK_LAG_BATCHES = 2

# The two arms, named. They are directory names under the run root as well as labels, so a
# typo produces a third arm reading an empty checkpoint rather than a KeyError.
DROPPING = "dropping"
KEEPING = "keeping"
ARMS = (DROPPING, KEEPING)

_Records = Sequence[Mapping[str, str]]


def _frontier_index(index: int, *, late: frozenset[int], steps: int, event_count: int) -> int:
    """The newest PUNCTUAL emission index delivered before the late record at `index`.

    `steps - 1` is the reach: a record delayed by `late_by_ms` is delivered after every
    punctual index whose own emission instant is strictly earlier, and on an exact tie the
    delivery sort breaks by emission index, which the late record wins. Capped at the last
    index, because nothing follows the end of the stream. The walk back over late indices
    is not a refinement -- a late index is delivered later still, so it can never be the
    frontier of a record delivered before it -- and it is LIVE rather than defensive: over
    `promotable` it fires exactly once, at emission index 4927, whose candidate 5646 is
    itself late."""
    frontier = min(event_count - 1, index + steps - 1)
    while frontier in late:
        frontier -= 1
    return frontier


def frontier_lags_ms(spec: StreamSpec, defects: DefectSpec) -> tuple[int, ...]:
    """How far behind the punctual frontier each injected late record lands, ascending.

    WHAT MAKES THE CORPUS A LATE-ARRIVAL CORPUS, and it is a statement about the corpus
    ALONE -- no reader, no batch boundary, no watermark. `watermark_margins` is the one a
    delay is derived from; the module docstring's first section records what happened the
    time these two were confused.

    NOTHING ELSE IN `src/` CALLS EITHER OF THESE TWO. `_frontier_index` has exactly one
    caller and it is the loop below; `frontier_lags_ms` has none outside the tests. They
    are the second instrument, not a step in the delay path, and the module docstring's
    second section says why they ship anyway. An earlier version of this paragraph said
    the pair had NO caller in `src/`, which was false of the `_frontier_index(...)` call
    inside the comprehension below."""
    late = late_positions(spec, defects)
    _refuse_a_corpus_with_nothing_to_drop(
        defects, len(late), counting="emission indices were selected"
    )
    steps = -(-defects.late_by_ms // spec.event_interval_ms)
    lags = tuple(
        sorted(
            (_frontier_index(i, late=late, steps=steps, event_count=spec.event_count) - i)
            * spec.event_interval_ms
            for i in sorted(late)
        )
    )
    _refuse_a_lag_that_is_not_a_lag(lags)
    return lags


def _refuse_a_corpus_with_nothing_to_drop(
    defects: DefectSpec, measured: int, *, counting: str
) -> None:
    """A profile with no late arrivals makes both arms land the same rows, the difference
    zero, and "the boundary held" true of an experiment that had no boundary in it.

    `counting` NAMES WHAT THE CALLER MEASURED, because the two callers do not measure the
    same thing: `frontier_lags_ms` counts EMISSION INDICES selected out of the spec, and
    `boundary_for` counts DELIVERED IDENTITIES that measure as late. Both must equal the
    declared count, and a single hardcoded noun in the message described one caller while
    being raised by the other."""
    if defects.late_count > 0 and measured == defects.late_count:
        return
    raise ValueError(
        f"the defect spec declares {defects.late_count} late arrivals and {measured} "
        f"{counting}. A corpus with nothing late in it makes both watermark arms land "
        "identical rows, and their difference -- which is the whole measurement -- would "
        "be 0 and true."
    )


def _refuse_a_lag_that_is_not_a_lag(lags: tuple[int, ...]) -> None:
    """A non-positive lag means the record is behind nothing and no watermark can drop it.
    `opl.generator.defects` already refuses such a delivery; this is the same statement on
    the consuming side, where a silent zero would set the dropping delay to zero -- and a
    delay of zero drops a redelivery that shares the frontier's own event time.

    REACHED BY A LEGAL SPEC RATHER THAN ONLY BY A CORRUPT ONE. A `StreamSpec` whose
    `event_interval_ms` is at least `defects.late_by_ms` puts `steps` at 1, so the frontier
    walk starts on the late record's OWN index, finds it late and steps backward -- and the
    lag comes back negative. Both halves of that spec are values `StreamSpec` and
    `DefectSpec` accept, and `tests/test_streaming_lateness.py` builds one.

    `lags[0]` UNGUARDED: `frontier_lags_ms` runs `_refuse_a_corpus_with_nothing_to_drop`
    first, which passes only for a positive `late_count` matched by as many selected
    indices, so the tuple reaching here is never empty. A branch for the empty case would
    be one nothing could reach."""
    if lags[0] > 0:
        return
    raise ValueError(
        f"the narrowest frontier lag is {lags[0]}. A record delayed past nothing newer "
        "than itself is not late, so no watermark can drop it and the dropping arm would "
        "be an arm with no boundary in it."
    )


def _refuse_a_read_that_cannot_advance_a_watermark(max_offsets_per_trigger: int | None) -> None:
    """TRAP 2, IN SHIPPED CODE. See `opl.streaming.watermarked_dedup`'s docstring, second
    numbered paragraph."""
    if isinstance(max_offsets_per_trigger, int) and max_offsets_per_trigger > 0:
        return
    raise ValueError(
        f"max_offsets_per_trigger={max_offsets_per_trigger!r}. Spark filters a batch's late "
        "events against a watermark computed from EARLIER batches, so a run of a single "
        "batch starts at the watermark's floor and can drop nothing -- and every count "
        "taken over it would report a clean stream over a run that had no boundary in it."
    )


# The margin of a row no watermark can reach -- one read in the first batches, before the
# watermark has left its floor. A sentinel rather than `None` so every comparison below
# stays arithmetic, and -1 rather than a large negative so that it is BELOW every legal
# delay (which is at least 1) while never being mistaken for one.
#
# BELOW ZERO IS THE LOAD-BEARING PART, AND 0 IS WHAT THE ALTERNATIVE GIVES. Measured over
# `promotable` at 133 and at 175 records a trigger: this sentinel makes `widest_punctual_ms`
# -1, and measuring those batches against the first delivered record's own event time makes
# it 0 -- an assertion reading it as "< 1" is satisfied by both, so
# `tests/test_streaming_lateness.py` pins the VALUE.
_UNREACHABLE = -1


@dataclass(frozen=True, kw_only=True)
class WatermarkMargins:
    """How far each delivered row sits below the watermark its own batch is filtered
    against, at a delay of zero. THE MODEL DROPS A ROW WHEN THE DELAY IS <= ITS MARGIN, and
    the `<=` is the one part of it this phase never reached: measured over `promotable`, no
    delivered record's margin equals either arm's delay at 133 or at 175 records a trigger,
    under a lag of 1, 2 or 3 alike -- so a model written with `<` would have named the same
    sets, including the falsifier run's 97.

    THE PUNCTUAL SIDE IS CARRIED BECAUSE IT IS THE OTHER HALF OF THE CLAIM. A dropping
    delay narrow enough to take every late record could also be narrow enough to take a
    redelivery that shares the frontier's own event time, and "100 rows were dropped"
    would then be true of a boundary that took 100 of the wrong ones."""

    late_rows: int
    late_identities: int
    narrowest_late_ms: int
    widest_late_ms: int
    widest_punctual_ms: int


def _batch_frontiers(events: Sequence[int], per_trigger: int) -> list[int | None]:
    """The event time each batch's late-event watermark is computed from, per batch.

    `None` for the first `LATE_EVENT_WATERMARK_LAG_BATCHES`, which are filtered against a
    watermark still at its floor -- so nothing in them can be dropped at any delay, and a
    late record read there is a hole the margins below make visible as a non-positive
    number rather than as a difference that is quietly short.

    THE MARKER IS FALSIFIABLE, AND ITS ALTERNATIVE IS RUN RATHER THAN ARGUED. Computing
    `prefix[0]` for those batches instead -- the first delivered record's own event time --
    says they sit behind a watermark rather than behind a floor, and over `promotable` at
    133 it moves their 266 margins onto a spread from -1,320,000 to 0. `_UNREACHABLE`'s
    comment carries the number that separates the two, and
    `tests/test_streaming_lateness.py` pins it."""
    running, prefix = -1, []
    for event in events:
        running = max(running, event)
        prefix.append(running)
    lag = LATE_EVENT_WATERMARK_LAG_BATCHES
    return [
        None if batch < lag else prefix[min((batch - lag + 1) * per_trigger, len(events)) - 1]
        for batch in range(-(-len(events) // per_trigger))
    ]


def watermark_margins(records: _Records, *, max_offsets_per_trigger: int) -> WatermarkMargins:
    """The margins over `records`, read at `max_offsets_per_trigger` records a batch.

    Late rows are found by `opl.generator.measures.late_arrivals` -- the same function
    `defects._require_the_injected_lateness_is_measurable` verifies the injection against,
    rather than a second spelling of the definition living here."""
    _refuse_a_read_that_cannot_advance_a_watermark(max_offsets_per_trigger)
    events = [from_text(record[EVENT_TIME_COLUMN]) for record in records]
    late = set(late_arrivals(records))
    frontiers = _batch_frontiers(events, max_offsets_per_trigger)
    late_margins: list[int] = []
    punctual_margins: list[int] = []
    for position, event in enumerate(events):
        frontier = frontiers[position // max_offsets_per_trigger]
        margin = _UNREACHABLE if frontier is None else frontier - event
        (late_margins if position in late else punctual_margins).append(margin)
    return WatermarkMargins(
        late_rows=len(late),
        late_identities=len({records[position][IDENTITY_COLUMN] for position in late}),
        narrowest_late_ms=min(late_margins, default=_UNREACHABLE),
        widest_late_ms=max(late_margins, default=_UNREACHABLE),
        widest_punctual_ms=max(punctual_margins, default=_UNREACHABLE),
    )


@dataclass(frozen=True, kw_only=True)
class LatenessBoundary:
    """The two watermark delays one corpus is read at, and the arithmetic that placed them.

    EVERY FIELD IS COMPUTED WITHOUT A SESSION OR A BROKER, which is what lets a run publish
    its prediction before it starts. The margins are carried beside the delays rather than
    discarded because they are what makes the delays reviewable: a reader can check that
    one sits at or below the narrowest late margin and the other strictly above the widest,
    without re-deriving either."""

    late_count: int
    max_offsets_per_trigger: int
    margins: WatermarkMargins
    dropping_delay_ms: int
    keeping_delay_ms: int

    @property
    def dropped_rows(self) -> int:
        """How many rows fewer the dropping arm lands. THE PREDICTION, AND AN IDENTITY: it
        reads the declaration and neither the rate limit nor the margins, so asserting it
        against `defects.late_count` finds nothing. What makes it a prediction is meeting a
        run's difference between two landed counts, and
        `watermarked_dedup._refuse_a_drop_the_boundary_did_not_predict` is what compares
        them -- in `prove_boundary` on every run, and over a hand-built pair in
        `tests/test_streaming_watermarked_dedup.py`.

        `late_count` and not `margins.late_rows`, and the two differ when a late record was
        also redelivered: BOTH its copies are dropped by the watermark, and the dedup then
        has one fewer redelivery to collapse. The two cancel exactly, so the difference
        between the LANDED counts is the injected late count whatever the overlap is."""
        return self.late_count

    def delay_of(self, arm: str) -> int:
        if arm == DROPPING:
            return self.dropping_delay_ms
        if arm == KEEPING:
            return self.keeping_delay_ms
        raise ValueError(f"{arm!r} is not one of {ARMS}. The boundary has two arms.")

    def batches_over(self, input_rows: int) -> int:
        """How many CONSUMING micro-batches a read of `input_rows` records at this
        boundary's rate limit splits into.

        THE PREDICTION THE MARGINS WERE DERIVED UNDER, made comparable to a run.
        `watermark_margins` assigns delivered position `p` to batch `p // per_trigger` and
        computes that batch's frontier from the prefix ending there, so the whole delay
        derivation is a statement about a read that took EXACTLY this many offsets per
        batch, in delivered order. What compares that against a run is
        `watermarked_dedup`'s `_refuse_a_split_the_boundary_was_not_derived_for`, over
        `LandedArm.batch_ids`."""
        return -(-input_rows // self.max_offsets_per_trigger)


def _refuse_margins_no_delay_can_separate(margins: WatermarkMargins, per_trigger: int) -> None:
    """A rate limit under which some injected record cannot be dropped at all -- because it
    is read in the first batches, or because the batch boundary has pulled the watermark
    back past its own event time -- leaves no delay that drops EVERY one of them. The arms
    would then differ by SOME number, and a difference the declaration cannot predict is a
    partial drop nobody chose.

    THE ACCEPTED REGION IS NOT AN INTERVAL, which is why the message below says "lower"
    rather than "lower by one". Whether a limit is accepted turns on where the LAST late
    record falls inside its own micro-batch, and that does not vary monotonically with the
    limit: measured over `promotable` at every limit from 1 to 260, 191 are accepted and 69
    refused -- 143 and 145 among the refusals, 144, 146 and even 260 among the acceptances.
    What makes "lower" sound advice is the other half of that measurement: every limit from
    1 to 142 is accepted, and the first refusal is 143."""
    if margins.narrowest_late_ms >= 2:
        return
    raise ValueError(
        f"at {per_trigger} records a trigger the narrowest late margin is "
        f"{margins.narrowest_late_ms} ms, so at least one injected record is behind no "
        "watermark this read produces. Lower max_offsets_per_trigger -- a smaller batch "
        "keeps the late-event watermark closer to the frontier -- or read a corpus whose "
        "late arrivals sit further behind it."
    )


def _refuse_a_dropping_delay_that_reaches_a_punctual_row(
    dropping_delay_ms: int, margins: WatermarkMargins
) -> None:
    """THE OTHER HALF OF THE BOUNDARY. A delay at or below a punctual row's own margin drops
    that row too -- a redelivery sharing the frontier's event time is the case that is not
    hypothetical -- and the arms would then differ by the late records PLUS collateral."""
    if dropping_delay_ms > margins.widest_punctual_ms:
        return
    raise ValueError(
        f"a dropping delay of {dropping_delay_ms} ms is not above the widest punctual "
        f"margin ({margins.widest_punctual_ms} ms), so rows that are not late would be "
        "dropped with the ones that are, and the difference between the arms would no "
        "longer be a count of late arrivals."
    )


def _refuse_a_keeping_delay_that_does_not_clear_the_widest_margin(
    keeping_delay_ms: int, margins: WatermarkMargins
) -> None:
    """STRICTLY GREATER, AND THE STRICTNESS IS UNDER NO RUN IN THIS PHASE. A delay equal to
    the margin puts the watermark exactly ON the record's own event time, which is the one
    place `event_time <= watermark` and `event_time < watermark` disagree. Measured over
    `promotable`: no delivered record's margin equals either arm's delay -- at 133 or at
    175 records a trigger, under a lag of 1, 2 or 3 alike -- so nothing F5 ran can say
    which way that comparison falls. The guard REFUSES the case rather than resolving it,
    which is why this docstring no longer attributes the `<=` to Spark: that attribution
    was a reading, and the runs beside it were not a test of it.

    NOT REACHABLE THROUGH `boundary_for` OVER THIS CORPUS, which is the same thing
    `_refuse_a_dropping_delay_that_reaches_a_punctual_row`'s own test says about it. The
    keeping delay IS `defects.late_by_ms`, and measured over `promotable` at every limit
    from 1 to 260 that `boundary_for` accepts, it clears the widest late margin by at least
    one event interval. 5,000 ms is the smallest of those 191 gaps and it belongs to a
    limit of 1 ALONE; the gap does not then rise with the limit -- 25 leaves 125,000 ms and
    26 leaves 120,000, one of 32 adjacent accepted pairs where it narrows.
    The arm is therefore reached by calling this function directly, which
    `tests/test_streaming_lateness.py` does."""
    if keeping_delay_ms > margins.widest_late_ms:
        return
    raise ValueError(
        f"the keeping delay is {keeping_delay_ms} ms and the widest late margin is "
        f"{margins.widest_late_ms} ms. An equal delay puts the watermark exactly ON the "
        "record's own event time, where dropping and keeping differ by which way the "
        "comparison falls -- and the arm that exists to show the boundary can be avoided "
        "would be resting on it."
    )


def boundary_for(
    records: _Records, defects: DefectSpec, *, max_offsets_per_trigger: int
) -> LatenessBoundary:
    """The two delays this corpus is read at, derived from the corpus and a rate limit.

    THE DELAYS ARE DERIVED; THE RATE LIMIT IS AN ARGUMENT, AND A CHOICE. Nothing in this
    package computes a `max_offsets_per_trigger`. What this function does is refuse the
    ones this corpus cannot carry. Measured over `promotable`: 191 of the limits between 1
    and 260 are accepted, each with a delay pair clearing every inequality this module
    enforces, and the limit MOVES that pair -- 103 distinct dropping delays across the 191,
    from 665,000 ms at a limit of 1 to 30,000 ms at 260.

    WHAT THE LIMIT CANNOT MOVE IS AN IDENTITY RATHER THAN A FINDING.
    `LatenessBoundary.dropped_rows` returns `defects.late_count` and reads neither the limit
    nor the margins, so "every accepted limit predicts the same 100" is true before a limit
    is tried. The non-vacuous half is the one above -- that all 191 are ACCEPTED -- and the
    prediction meets a measurement only in the run, against two landed counts.

    AN ACCEPTED LIMIT IS NOT THE SAME AS A RUNNABLE ONE, and this is where a caller meets
    the gap, because there is nothing at the call site that would. This function reads the
    corpus and knows nothing about a query; `opl.streaming.ingest._progress_of` separately
    refuses a run whose progress updates reach `spark.sql.streaming.numRecentProgressUpdates`
    (100 by default), counting every progress and not only the consuming ones. A STATEFUL
    `availableNow` query runs one further batch that consumes nothing WHEN ITS LAST
    CONSUMING BATCH ADVANCES THE WATERMARK -- the state eviction -- which is measured, with
    that precondition and with the corpus arithmetic that decides it, in
    `tests/integration/test_late_arrival_boundary.py`'s header.

    SO THE FLOOR IS 104 RECORDS A TRIGGER over 10,150 delivered records: 103 is 99
    consuming batches, one eviction batch, and so 100 progress updates against a cap of
    100. 103 of the 191 limits accepted above are below that floor and will fail in the
    run -- and the floor is THIS CHAIN'S rather than the sink's, because a stateless query
    through the same cap has no eviction batch and nothing here has measured one."""
    margins = watermark_margins(records, max_offsets_per_trigger=max_offsets_per_trigger)
    _refuse_a_corpus_with_nothing_to_drop(
        defects, margins.late_identities, counting="delivered identities measure as late"
    )
    _refuse_margins_no_delay_can_separate(margins, max_offsets_per_trigger)
    dropping = margins.narrowest_late_ms // 2
    _refuse_a_dropping_delay_that_reaches_a_punctual_row(dropping, margins)
    _refuse_a_keeping_delay_that_does_not_clear_the_widest_margin(defects.late_by_ms, margins)
    return LatenessBoundary(
        late_count=defects.late_count,
        max_offsets_per_trigger=max_offsets_per_trigger,
        margins=margins,
        dropping_delay_ms=dropping,
        keeping_delay_ms=defects.late_by_ms,
    )
