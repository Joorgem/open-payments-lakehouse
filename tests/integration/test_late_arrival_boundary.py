# tests/integration/test_late_arrival_boundary.py
"""T4 + T5 -- THE LATE-ARRIVAL BOUNDARY AND THE DEDUP, over `promotable` through Redpanda.

TWO RUNS, ONE CORPUS, TWO WATERMARKS. The same 10,150 Kafka records, read at the same rate
limit, through the same `withWatermark -> dropDuplicatesWithinWatermark` chain, differing
in ONE THING: the watermark delay. Both delays are arithmetic over the delivered corpus and
`opl.streaming.lateness.boundary_for` computes them before anything runs.

    DROPPING     262,500 ms      9,900 rows land
    KEEPING    3,600,000 ms     10,000 rows land
                                ------
                                   100  <- the 100 late arrivals the profile declares

THE DIFFERENCE IS THE PRODUCT. Either arm alone is a demonstration: a run landing 10,000
rows at a wide watermark has shown only that a watermark can be set wide enough to drop
nothing, which is true of every watermark that is never tested; a run landing 9,900 at a
narrow one has shown only that 100 rows went missing, which is true of a broken parse. The
pair, over one corpus, at two delays fixed before the run, is the measurement -- and
`prove_boundary` is where it cannot be quietly lost: `_refuse_a_pair_that_dropped_nothing`
refuses a pair that found no boundary at all, and
`_refuse_a_drop_the_boundary_did_not_predict` refuses one whose difference is not the 100
the declaration names. THE SECOND IS NEW AND THE FIRST NEVER COVERED IT: a pair differing
by 97 -- the number this phase's published prediction actually met, and the number the
third arm below still reproduces at 175 records a trigger -- cleared every other refusal in
that module and came back reading as a clean measurement.

WHY 133 RECORDS A TRIGGER, AND THAT IT IS A CHOICE. Nothing in `src/` computes a rate
limit. 133 is picked out of a band, and it is not even the best pick inside it: 137 leaves
655,000 ms of margin over 75 batches, and 147 leaves 630,000 over 70, against 133's 525,000
over 77. What binds the band is a refusal at each end.

  BELOW 104   the run itself refuses. `opl.streaming.ingest._progress_of` reads a ring
              buffer capped at `numRecentProgressUpdates` (100), refuses any total taken
              over a full one, and counts EVERY progress rather than only the consuming
              ones. A stateful `availableNow` query runs one further batch that consumes
              nothing when its last consuming batch ADVANCES THE WATERMARK -- the state
              eviction -- so 103 records a trigger is 99 consuming batches, one eviction
              batch, 100 updates and a RuntimeError. 103 of the 191 limits `boundary_for`
              accepts are in that band.
  ABOVE 142   `boundary_for` refuses -- intermittently. Spark filters a batch's late events
              against a watermark computed two batches earlier, so a limit that puts the
              LAST late record early in its own micro-batch pulls that watermark back behind
              the record's own event time and no delay can then drop all 100. Where the
              record falls does not vary monotonically with the limit, so acceptance is a
              SAWTOOTH and not a ceiling: 143 and 145 are refused, 144, 146 and 260 are
              accepted. Measured over 1..260: 191 accepted, 69 refused, and every limit from
              1 to 142 accepted.

THE EVICTION BATCH IS CONDITIONAL AND THE MEASUREMENT IS THIS FILE'S OWN. It follows only
where the last consuming batch moves the watermark, and over `promotable` whether it does is
arithmetic rather than a constant: the newest event time sits at delivered position 10,142
of 10,150 -- the late arrivals are in the tail behind it -- so the last batch carries it at
103, 104, 133, 175 and 260 alike, while at a limit of 5 it does not and no eviction batch
would follow. What measures the eviction batch itself is the falsifier arm below: its
checkpoint records `commits` 0..58 with `offsets/57` and `offsets/58` both ending at
`{"0":10150}` -- 58 consuming batches and one that consumed none, against a
`_FALSIFIER_BATCHES` of 58.

AND IT IS A PROPERTY OF A STATEFUL QUERY. Every arm here runs through
`dropDuplicatesWithinWatermark`, which is what has state to evict. `write_payment_stream`
carries stateless queries through the same cap -- the one in
`tests/integration/test_payment_stream_ingest.py` -- and nothing in this phase has measured
whether such a query reports a trailing progress at all. The floor above is this chain's.

AND WHAT THE CHOICE CANNOT MOVE IS AN IDENTITY, NOT A FINDING.
`LatenessBoundary.dropped_rows` returns `defects.late_count` and reads neither the limit nor
the margins, so "every accepted limit predicts 100" is true before a limit is tried and is
no evidence about the choice. What the sweep does say is that all 191 are ACCEPTED -- each
with a delay pair clearing every inequality `boundary_for` enforces -- and that the limit
moves that pair: 262,500 ms at 133 against 665,000 at a limit of 1 and 30,000 at 260. The
prediction meets a measurement HERE instead, against the difference between two landed
counts. `opl.streaming.lateness`'s module docstring records the published prediction
that got the MODEL wrong -- 100 predicted, 97 dropped -- and
`tests/test_streaming_lateness.py` re-runs the superseded arithmetic against it.

WHAT THIS FILE DOES NOT CLAIM. It is not a byte-identity test: nothing here rebuilds F1b's
pinned file, and the pool below is a synthetic 1,024-key stand-in for the one
`StreamProfile.pool_query` extracts from `hub_empresa`, because which counterparties a
payment names changes no count on this page.
`tests/integration/test_payment_stream_ingest.py` is where the bytes are pinned. What this
file needs from the corpus is its SHAPE, and the first test asserts every number of that
shape against the profile's own arithmetic before a stream is started.
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest

from opl.contracts.payments import (
    BUSINESS_ATTRIBUTE_COLUMNS,
    EVENT_TIME_COLUMN,
    IDENTITY_COLUMN,
)
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import delivered_records
from opl.generator.instants import from_text
from opl.generator.measures import late_arrivals
from opl.generator.profiles import POOL_SIZE, PROFILES
from opl.streaming import lateness as lt
from opl.streaming.ingest import payment_stream
from opl.streaming.lateness import (
    DROPPING,
    KEEPING,
    LATE_EVENT_WATERMARK_LAG_BATCHES,
    LatenessBoundary,
    boundary_for,
)
from opl.streaming.producer import BrokerConfig, publish_records
from opl.streaming.watermarked_dedup import (
    BoundaryEvidence,
    LandedArm,
    dedup_shape,
    prove_boundary,
    run_arm,
)

pytestmark = [pytest.mark.integration, pytest.mark.redpanda]

_PROFILE = PROFILES["promotable"]
_DEFECTS = _PROFILE.defects
_PER_TRIGGER = 133

# EVERY NUMBER BELOW IS ARITHMETIC ON THE DECLARATION, and none of them needs a run.
_DELIVERED = _PROFILE.delivered_row_count  # 10,000 events + 150 redeliveries
_IDENTITIES = _PROFILE.event_count  # one per event; a redelivery adds none
_REDELIVERIES = _DEFECTS.duplicate_count
_LATE = _DEFECTS.late_count
_ATTRIBUTES = _PROFILE.event_count - _PROFILE.repeat_count  # the base events
_REPEATS = _PROFILE.repeat_count  # events carrying an earlier one's attributes
_KEEPING_ROWS = _IDENTITIES  # every id once: the redeliveries collapsed
_DROPPING_ROWS = _IDENTITIES - _LATE  # ...and the late arrivals gone
_BATCHES = -(-_DELIVERED // _PER_TRIGGER)

# THE RATE LIMIT THAT SEPARATES THE MODELS, and the reason there is a third arm below.
# `boundary_for` ACCEPTS 175 at a lag of one batch and REFUSES it at two, so the shipped
# constant -- which the 133 arms cannot test, because they drop 100 under either -- becomes
# a claim one run can falsify.
_FALSIFIER_PER_TRIGGER = 175
_FALSIFIER_BATCHES = -(-_DELIVERED // _FALSIFIER_PER_TRIGGER)
_CANDIDATE_LAGS = (1, 2, 3)


@dataclass(frozen=True, kw_only=True)
class _Corpus:
    """The delivered records and the ONE topic holding them, read by every arm below."""

    records: tuple[dict[str, str], ...]
    topic: str
    published: int
    identities: tuple[str, ...]
    attributes: frozenset[tuple[str, ...]]
    late_identities: frozenset[str]


@dataclass(frozen=True, kw_only=True)
class _Proof:
    """The corpus, what the boundary predicted, and what the two arms landed."""

    boundary: LatenessBoundary
    published: int
    identities: tuple[str, ...]
    attributes: frozenset[tuple[str, ...]]
    late_identities: frozenset[str]
    evidence: BoundaryEvidence


@dataclass(frozen=True, kw_only=True)
class _Falsifier:
    """One arm at `_FALSIFIER_PER_TRIGGER`, and the superseded delay it was run at."""

    superseded: LatenessBoundary
    arm: LandedArm


@pytest.fixture(scope="module")
def corpus(module_kafka_topic, bootstrap) -> _Corpus:
    """Derive `promotable` and publish it ONCE. NO SPARK AND NO ARMS IN HERE.

    ONE TOPIC, because every arm below must be a reading of the SAME records: a topic per
    fixture would give each its own offsets and every difference between their landed counts
    would be a difference in the read rather than in the watermark. One partition, so the
    batch split is offset arithmetic rather than a partition assignment. Separate from
    `proof` so that the third arm can be run and read without paying for the pair."""
    pool = validated_pool([f"{n:08d}" for n in range(1, POOL_SIZE + 1)])
    spec = _PROFILE.stream_spec(pool)
    records = delivered_records(spec, _DEFECTS)
    topic = module_kafka_topic(1)
    published = publish_records(records, topic=topic, broker=BrokerConfig(bootstrap=bootstrap))
    assert published.message_count == _DELIVERED, (
        "the broker did not acknowledge the whole corpus, so every count below would be "
        "taken over a topic holding something other than what the profile declares"
    )
    return _Corpus(
        records=records,
        topic=topic,
        published=published.message_count,
        identities=tuple(record[IDENTITY_COLUMN] for record in records),
        attributes=frozenset(
            tuple(record[column] for column in BUSINESS_ATTRIBUTE_COLUMNS)
            for record in records
        ),
        late_identities=frozenset(
            records[position][IDENTITY_COLUMN] for position in late_arrivals(records)
        ),
    )


@pytest.fixture(scope="module")
def proof(corpus, kafka_spark, bootstrap, tmp_path_factory) -> _Proof:
    """Read the published corpus at both delays. THE EXPERIMENT.

    `minimum_rows=_DELIVERED` floors each arm on Spark's own progress, so an arm that
    consumed a short prefix is refused rather than reported as a small drop."""
    boundary = boundary_for(corpus.records, _DEFECTS, max_offsets_per_trigger=_PER_TRIGGER)
    frame = payment_stream(
        kafka_spark,
        topic=corpus.topic,
        bootstrap=bootstrap,
        max_offsets_per_trigger=_PER_TRIGGER,
    )
    return _Proof(
        boundary=boundary,
        published=corpus.published,
        identities=corpus.identities,
        attributes=corpus.attributes,
        late_identities=corpus.late_identities,
        evidence=prove_boundary(
            frame,
            boundary=boundary,
            root=tmp_path_factory.mktemp("late-arrival").as_posix(),
            topic=corpus.topic,
            minimum_rows=_DELIVERED,
        ),
    )


@pytest.fixture(scope="module")
def falsifier(corpus, kafka_spark, bootstrap, tmp_path_factory) -> _Falsifier:
    """ONE ARM, at the rate limit the superseded model accepts and the shipped one refuses.

    The delay is derived by the SUPERSEDED model -- `boundary_for` with the lag constant
    monkeypatched to 1 -- because the shipped model refuses to produce a delay here at all.
    That is the point: the run is the superseded model given its own best configuration, at
    a limit where its prediction and the shipped model's differ."""
    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(lt, "LATE_EVENT_WATERMARK_LAG_BATCHES", 1)
        superseded = boundary_for(
            corpus.records, _DEFECTS, max_offsets_per_trigger=_FALSIFIER_PER_TRIGGER
        )
    frame = payment_stream(
        kafka_spark,
        topic=corpus.topic,
        bootstrap=bootstrap,
        max_offsets_per_trigger=_FALSIFIER_PER_TRIGGER,
    )
    return _Falsifier(
        superseded=superseded,
        arm=run_arm(
            frame,
            arm="one-batch-model",
            delay_ms=superseded.dropping_delay_ms,
            root=tmp_path_factory.mktemp("lag-falsifier").as_posix(),
            topic=corpus.topic,
            minimum_rows=_DELIVERED,
        ),
    )


def _margin_of(frontier: int | None, event: int) -> int:
    """`watermark_margins`'s arithmetic, one row at a time: how far this row sits below the
    watermark its own batch is filtered against. `lt._UNREACHABLE` for a batch read before
    the watermark left its floor, which no legal delay is at or below."""
    return lt._UNREACHABLE if frontier is None else frontier - event


def _identities_the_model_drops(records, *, lag: int, per_trigger: int, delay_ms: int):
    """Which identities a late-event watermark `lag` batches back would remove entirely.

    `lt._batch_frontiers` is the SHIPPED model read at a lag this function supplies, so the
    three predictions below are COMPUTED FROM THE MODULE UNDER TEST rather than pinned as
    literals.

    WHAT THE SET EQUALITY BELOW DISCRIMINATES IS THE LAG, AND NOT THE MODEL AT LARGE -- an
    earlier version of this docstring claimed the second, that "a change to the model
    changes what the run is compared against". Measured over `promotable` at 175 records a
    trigger and the delay this file runs: three independent changes to the model leave the
    predicted set BIT-IDENTICAL to the shipped one, all of them still naming the same 97
    identities -- the frontier's prefix index moved back one record, the survival predicate
    `delay > margin` relaxed to `>=`, and `_batch_frontiers`'s floor marker replaced by the
    first record's own event time. What DOES move the set is the lag (100 at a lag of 1, 97
    at 2, 95 at 3) and the rate limit (99 at 174, 98 at 176). The index and the floor marker
    are caught in `tests/test_streaming_lateness.py`; the `>=` is caught nowhere,
    because no record over this corpus sits exactly at either arm's delay -- so `<=` and `<`
    name the same sets here and nothing in F5 separates them.

    An identity is removed only when every copy of it is. A redelivery carries its
    original's event time but sits at a different delivered position, so it is filtered
    against a different batch's watermark, and one copy surviving lands the identity."""
    events = [from_text(record[EVENT_TIME_COLUMN]) for record in records]
    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(lt, "LATE_EVENT_WATERMARK_LAG_BATCHES", lag)
        frontiers = lt._batch_frontiers(events, per_trigger)
    surviving = {
        record[IDENTITY_COLUMN]
        for position, record in enumerate(records)
        if delay_ms > _margin_of(frontiers[position // per_trigger], events[position])
    }
    return {record[IDENTITY_COLUMN] for record in records} - surviving


def _landed(spark, path: str):
    return spark.read.format("delta").load(path)


def test_the_corpus_is_the_one_the_profile_declares(proof):
    """THE FLOOR UNDER EVERYTHING ELSE, measured before Spark is reached.

    Four counts the profile predicts and one measure that reads them back: 10,150 rows
    carrying 10,000 identities (so 150 redeliveries) over 9,200 attribute tuples (so 800
    legitimate repeats), with 100 distinct identities measuring as late arrivals.

    THE LATE IDENTITIES ARE READ WITH `measures.late_arrivals`, the same function
    `defects._require_the_injected_lateness_is_measurable` verifies the injection against --
    not a second spelling of the definition living here, which would agree with itself
    forever including on the day it stopped agreeing with the injection."""
    assert proof.published == len(proof.identities) == _DELIVERED == 10_150
    assert len(set(proof.identities)) == _IDENTITIES == 10_000
    assert len(proof.identities) - len(set(proof.identities)) == _REDELIVERIES == 150
    assert len(proof.attributes) == _ATTRIBUTES == 9_200
    assert _IDENTITIES - _ATTRIBUTES == _REPEATS == 800 > 0, (
        "a corpus whose identities and attribute tuples agree in count has no legitimate "
        "repeat in it, and the dedup below would never be asked a hard question"
    )
    assert len(proof.late_identities) == _LATE == 100


def test_the_two_watermarks_differ_by_exactly_the_injected_late_arrivals(proof):
    """THE HEADLINE. Two delays, one corpus, and the difference between what they land.

    THE FLOORS COME FIRST AND THEY ARE NOT DECORATION. Both arms are asserted to have
    consumed all 10,150 records across the same 77 batches -- read off Spark's own progress,
    which is the SOURCE side and independent of the sink -- so the difference below cannot
    be a short read wearing a boundary's clothes. Only then are the landed counts compared,
    and each is stated as the profile's arithmetic rather than as a literal read back from
    the run.

    NEITHER ARM ALONE SAYS ANYTHING. 10,000 rows at the wide delay is what a pipeline that
    never dropped a row reports, and 9,900 at the narrow one is what a pipeline that lost
    100 rows to anything at all reports."""
    dropping, keeping = proof.evidence.dropping, proof.evidence.keeping
    for arm in (dropping, keeping):
        assert arm.input_rows == _DELIVERED == proof.published
        assert len(arm.batch_ids) == _BATCHES == 77
    assert dropping.batch_ids == keeping.batch_ids

    assert (dropping.delay_ms, keeping.delay_ms) == (262_500, 3_600_000)
    assert dropping.delay_ms == proof.boundary.dropping_delay_ms
    assert keeping.delay_ms == proof.boundary.keeping_delay_ms

    assert keeping.landed_rows == _KEEPING_ROWS == 10_000
    assert dropping.landed_rows == _DROPPING_ROWS == 9_900
    assert proof.evidence.dropped_rows == proof.boundary.dropped_rows == _LATE == 100


def test_the_rows_the_narrow_watermark_dropped_are_the_late_ones(proof, kafka_spark):
    """A DROP OF THE RIGHT SIZE IN THE WRONG PLACE PASSES EVERY COUNT ABOVE THIS.

    100 rows fewer is 100 rows fewer whatever they were: a boundary that took the first 100
    records, or 100 scattered by a broken parse, satisfies the difference exactly. So the
    two arms' identity SETS are compared, and the 100 the narrow arm is missing must be
    precisely the identities `measures.late_arrivals` finds in the corpus.

    Each set is also asserted to be exactly as large as its table, which is the statement
    that neither arm landed any identity twice -- the dedup claim, made here on both arms
    rather than only on the one T5 reads."""
    identities = {}
    for arm in (DROPPING, KEEPING):
        landed = _landed(kafka_spark, getattr(proof.evidence, arm).path)
        rows = landed.select(IDENTITY_COLUMN).collect()
        identities[arm] = {row[IDENTITY_COLUMN] for row in rows}
        assert len(identities[arm]) == len(rows) == getattr(proof.evidence, arm).landed_rows

    assert identities[KEEPING] == set(proof.identities)
    assert identities[KEEPING] - identities[DROPPING] == set(proof.late_identities)
    assert identities[DROPPING] < identities[KEEPING]
    assert len(identities[KEEPING] - identities[DROPPING]) == _LATE == 100


def test_the_dedup_collapses_the_redeliveries_and_leaves_the_repeats_standing(
    proof, kafka_spark
):
    """T5, IN BOTH DIRECTIONS, because only one of them is a hard question.

    "150 fewer rows landed" is satisfied by a dedup keyed on the business attributes, which
    takes the legitimate repeats along with the redeliveries. MEASURED at this file's own
    rate limit and keeping delay: that key lands 9,624 rows -- 526 collapsed rather than 150
    -- and leaves 424 of the 800 repeats standing. So the surviving repeats are counted as a
    number of their own and asserted to be 800, which is the assertion that key fails; the
    row count alone does not flag it, because 9,624 is not a figure anything here predicts.

    A LEGITIMATE REPEAT IS ORDINARY BUSINESS: the same payer, payee, amount, currency and
    method as an earlier payment, carrying its OWN `transaction_id` -- a customer paying one
    supplier the same amount twice. A REDELIVERY is one `transaction_id` sent twice, byte
    for byte, and it is a property of the DATA rather than of the processing: F1b injects it
    on purpose and F1b already measures it. Exactly-once is the other question, counted over
    the Kafka coordinate pair by `opl.streaming.exactly_once`, which is blind to
    `transaction_id` for precisely this reason.

    READ OFF THE KEEPING ARM, where nothing was dropped: the narrow arm's 100 missing
    records take an unpredictable number of attribute tuples with them, so its surviving
    repeats are not a number the declaration can state -- and a number this file cannot
    predict is a number it will not assert."""
    shape = dedup_shape(_landed(kafka_spark, proof.evidence.keeping.path))
    assert shape.row_count == _KEEPING_ROWS == 10_000
    assert shape.distinct_identities == shape.row_count == _IDENTITIES
    assert proof.published - shape.row_count == _REDELIVERIES == 150
    assert shape.collapsed_rows == 0, (
        "every landed identity is unique, so the 150 above were collapsed by the dedup "
        "rather than merely absent from a table that also holds duplicates"
    )
    assert shape.distinct_business_attributes == _ATTRIBUTES == 9_200
    assert shape.surviving_repeats == _REPEATS == 800 > 0


def test_the_boundary_was_derived_from_the_declaration_and_not_from_the_run(proof):
    """THE PREDICTION, PINNED BESIDE THE RUN THAT CONFIRMED IT.

    `boundary_for` reads the delivered corpus, a `DefectSpec` and a rate limit. It starts no
    session, opens no socket and reads no clock -- `delivered_records` is a pure function of
    the profile -- so both delays existed before the corpus was published, and this test
    re-derives them and finds the same two numbers the arms ran at.

    The margins are checked too, because the delays are only meaningful relative to them:
    one at or below the narrowest margin an injected record has, one strictly above the
    widest, and both clear of every punctual row."""
    margins = proof.boundary.margins
    assert proof.boundary.late_count == _LATE
    assert proof.boundary.max_offsets_per_trigger == _PER_TRIGGER
    assert (margins.late_rows, margins.late_identities) == (_LATE, _LATE)
    assert margins.narrowest_late_ms == 525_000
    assert margins.widest_late_ms == 2_945_000

    assert proof.boundary.dropping_delay_ms <= margins.narrowest_late_ms
    assert proof.boundary.dropping_delay_ms > margins.widest_punctual_ms
    assert proof.boundary.keeping_delay_ms > margins.widest_late_ms
    assert proof.boundary.keeping_delay_ms == _DEFECTS.late_by_ms


def test_the_shipped_arms_cannot_tell_a_one_batch_lag_from_a_two_batch_one(proof, corpus):
    """WHY THERE IS A THIRD ARM, STATED BEFORE IT RUNS.

    `LATE_EVENT_WATERMARK_LAG_BATCHES` is 2, and the headline above is silent about it: at
    133 records a trigger and a delay of 262,500 ms, a one-batch model and a two-batch model
    remove the SAME 100 identities. Every assertion in this file up to here would read
    exactly as it does with the constant set to 1, so the run confirms the difference
    between two delays and says nothing at all about the lag that placed one of them.

    A THREE-BATCH LAG IS THE ONE CANDIDATE THIS RATE LIMIT DOES RULE OUT: it predicts 97
    identities removed where the arms removed 100. So the headline is not silent about every
    candidate -- only about the two that are actually in question. The test below moves to a
    rate limit where all three disagree."""
    at_the_shipped_limit = {
        lag: _identities_the_model_drops(
            corpus.records,
            lag=lag,
            per_trigger=_PER_TRIGGER,
            delay_ms=proof.boundary.dropping_delay_ms,
        )
        for lag in _CANDIDATE_LAGS
    }
    assert at_the_shipped_limit[1] == at_the_shipped_limit[2] == proof.late_identities
    assert len(at_the_shipped_limit[3]) == 97 < _LATE
    assert len({frozenset(dropped) for dropped in at_the_shipped_limit.values()}) == 2, (
        "two of the three candidate lags are indistinguishable at this rate limit, which "
        "is the whole reason the arm below reads the corpus a third time"
    )


def test_a_run_at_175_picks_the_two_batch_lag_out_of_three(falsifier, corpus, kafka_spark):
    """THE CONSTANT, PUT UNDER A RUN INSTEAD OF UNDER A PARAGRAPH.

    175 records a trigger is a limit the SUPERSEDED one-batch model accepts and the shipped
    two-batch model refuses outright. So the arm is run at the delay the superseded model
    derives for it -- 262,500 ms, its own best answer at its own best configuration -- and
    the three candidate lags then predict three DIFFERENT sets of removed identities: 100
    for a one-batch lag, 97 for two, 95 for three. The predictions are computed from
    `lt._batch_frontiers`, the shipped model, rather than typed in.

    ONE RUN CHOOSES AMONG THEM, AND AMONG THEM IS THE WHOLE OF WHAT IT CHOOSES. What Spark
    removes is compared against the whole set and not against its size, because three models
    that happened to agree on a count while disagreeing on rows would otherwise pass. The
    rows removed are also asserted to be a PROPER SUBSET of the corpus's late arrivals: a
    lag that dropped 97 punctual rows would match the count exactly. What this comparison
    does NOT discriminate is three other changes to the same model, which
    `_identities_the_model_drops` names and measures."""
    predictions = {
        lag: _identities_the_model_drops(
            corpus.records,
            lag=lag,
            per_trigger=_FALSIFIER_PER_TRIGGER,
            delay_ms=falsifier.superseded.dropping_delay_ms,
        )
        for lag in _CANDIDATE_LAGS
    }
    assert [len(predictions[lag]) for lag in _CANDIDATE_LAGS] == [100, 97, 95]
    assert len({frozenset(dropped) for dropped in predictions.values()}) == 3

    arm = falsifier.arm
    assert (arm.input_rows, len(arm.batch_ids)) == (_DELIVERED, _FALSIFIER_BATCHES)
    assert (_FALSIFIER_BATCHES, arm.delay_ms) == (58, 262_500)

    landed = _landed(kafka_spark, arm.path).select(IDENTITY_COLUMN).collect()
    removed = set(corpus.identities) - {row[IDENTITY_COLUMN] for row in landed}
    assert removed == predictions[LATE_EVENT_WATERMARK_LAG_BATCHES]
    assert removed != predictions[1], "the superseded model predicted all 100 here"
    assert removed < corpus.late_identities
    assert len(removed) == 97 < _LATE == 100
