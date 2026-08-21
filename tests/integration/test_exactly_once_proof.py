# tests/integration/test_exactly_once_proof.py
"""T3 -- THE EXACTLY-ONCE PROOF, AND ITS NEGATIVE CONTROL. The phase's headline.

TWO ARMS, ONE FAULT, THE SAME OFFSETS. Both write through `foreachBatch`, both read one
topic, both are killed on the same batch id after that batch's data write has committed
and before its offsets have. The arms differ in exactly one thing -- whether the append
carries Delta's `txnAppId`/`txnVersion` -- so any difference in what they land is a
difference that key made.

    NAIVE     39 rows, 29 deliveries, 10 DUPLICATES   <- the negative control
    GUARDED   29 rows, 29 deliveries,  0 duplicates

THE NEGATIVE CONTROL IS THE DELIVERABLE, AND THE ACCEPTANCE CUTS BOTH WAYS. If the NAIVE
arm does not duplicate, nothing here has proven exactly-once -- what it has proven is that
the fault never reached the window in which a replay can double-write. The guarded arm's
zero, on its own, is indistinguishable from a test that never ran: it is what an empty
table, a checkpoint that had already consumed the topic, and a restart that read nothing
all report. That is why every number below is stated against a floor, and why the floors
that matter live in `opl.streaming.exactly_once` as REFUSALS rather than here as
assertions.

WHAT A DUPLICATE IS COUNTED OVER: `(kafka_partition, kafka_offset)`, the delivery. NOT
`transaction_id` -- this corpus carries 5 deliberate redeliveries, and
`test_a_redelivery_is_not_an_exactly_once_failure` below shows that the identity column
cannot tell the two arms apart at all while the coordinate pair separates them cleanly.

PREDICTED BEFORE THE RUN, from the declarations and not from an execution: 29 records at
10 per trigger is 3 batches of 10/10/9; the fault on batch 1 leaves 20 rows landed and 2
batches invoked; the restart replays batch 1 (10 rows, the same offsets) and finishes with
batch 2 (9). NAIVE therefore lands 29 + 10 and GUARDED lands 29.

THIS FILE'S `_SPEC` IS NOT ANCHORED TO F1b'S DIGEST, AND DOES NOT NEED TO BE -- said here
because the file beside it makes the opposite promise about the same nine lines.
`tests/integration/test_payment_stream_ingest.py` carries this declaration next to the
literal `b45f1dc7...` and rebuilds the landed bytes into it, so BYTE identity is that
file's claim and a drift in `_SPEC` goes red there. Nothing here rebuilds a file: what this
copy has to be right about is the corpus's SHAPE, and the shape is pinned indirectly by two
floors this file already asserts -- `published.message_count == 29` in the fixture and
`len(set(identities)) == 24` in `test_a_redelivery_is_not_an_exactly_once_failure`. What
that does NOT cover, and is not meant to: a `_SPEC` that still produced 24 clean records
plus 5 redeliveries while changing their CONTENTS would be invisible here and red there.
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest
from pyspark.sql import functions as F

from opl.contracts.payments import IDENTITY_COLUMN
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import DefectSpec, delivered_records
from opl.generator.stream import StreamSpec
from opl.streaming.exactly_once import (
    ARMS,
    GUARDED,
    NAIVE,
    ArmEvidence,
    BatchFault,
    prove_arm,
)
from opl.streaming.ingest import (
    OFFSET_COLUMN,
    PARTITION_COLUMN,
    PROCESSING_IDENTITY,
    payment_stream,
)
from opl.streaming.producer import BrokerConfig, publish_records

pytestmark = [pytest.mark.integration, pytest.mark.redpanda]

_POOL = validated_pool([f"{n:08d}" for n in range(1, 21)])
_SPEC = StreamSpec(
    seed=20260809,
    stream_id="F1B-CLEAN",
    event_count=24,
    repeat_count=4,
    window_start="2026-06-01T00:00:00.000Z",
    event_interval_ms=250,
    emission_lag_ms=1_500,
    cnpj_pool=_POOL,
)
_CLEAN_ROWS = 24
# THE REDELIVERIES ARE IN THE CORPUS ON PURPOSE. They are a DATA property F1b already
# measures, and carrying them here is what lets this file show that the exactly-once
# measure is blind to them and that `transaction_id` is blind to the arms.
_REDELIVERIES = 5
_DELIVERED = _CLEAN_ROWS + _REDELIVERIES

# The rate limit, the batch split it produces, and the batch the fault kills. The MIDDLE
# batch: batch 0 would leave nothing already committed for a replay to duplicate against,
# and the last would leave the restart with nothing to do but the replay -- so a restart
# that did nothing at all would look the same as one that replayed.
_PER_TRIGGER = 10
_BATCHES = (0, 1, 2)
_FAULT_BATCH = 1
_FAULTED_BATCH_ROWS = _PER_TRIGGER
_LANDED_BEFORE_THE_RESTART = _PER_TRIGGER * (_FAULT_BATCH + 1)

# Predicted from the arithmetic above, before the run.
_NAIVE_ROWS = _DELIVERED + _FAULTED_BATCH_ROWS
_GUARDED_ROWS = _DELIVERED


# The alias the landings-per-coordinate count lands under. Named because `count` is also a
# DataFrame method and a `filter("count > 1")` over it reads as either.
_LANDINGS = "landings"


def _landed_more_than_once(spark, path: str) -> tuple[tuple[tuple[int, int], int], ...]:
    """Every delivery the table at `path` holds MORE THAN ONCE, with how many times.

    `groupBy(PROCESSING_IDENTITY)` -- the same pair `processing_duplicates` takes its
    arithmetic over, read as a set of COORDINATES rather than as a total. A total answers
    "how much" and this answers "which", and the phase's headline needs the second: the
    rows the naive arm holds twice have to be the rows the fault killed, not merely as
    many of them."""
    landed = spark.read.format("delta").load(path)
    rows = (
        landed.groupBy(*PROCESSING_IDENTITY)
        .agg(F.count(F.lit(1)).alias(_LANDINGS))
        .filter(F.col(_LANDINGS) > 1)
        .orderBy(*PROCESSING_IDENTITY)
        .collect()
    )
    return tuple(
        ((int(row[PARTITION_COLUMN]), int(row[OFFSET_COLUMN])), int(row[_LANDINGS]))
        for row in rows
    )


@dataclass(frozen=True, kw_only=True)
class _Proof:
    topic: str
    published: int
    identities: tuple[str, ...]
    arms: dict[str, ArmEvidence]


@pytest.fixture(scope="module")
def proof(kafka_spark, module_kafka_topic, bootstrap, tmp_path_factory) -> _Proof:
    """Publish the corpus ONCE and run both arms against it. THE EXPERIMENT.

    Module-scoped because the arms must be two readings of one topic: a fresh topic per
    test would give each arm its own offsets, and "the same offsets" is half of what the
    two arms are being compared over. One partition, so the batch split is offset
    arithmetic and not a partition assignment."""
    topic = module_kafka_topic(1)
    records = delivered_records(_SPEC, DefectSpec(duplicate_count=_REDELIVERIES))
    published = publish_records(records, topic=topic, broker=BrokerConfig(bootstrap=bootstrap))
    assert published.message_count == _DELIVERED, (
        "the broker did not acknowledge the whole corpus; every count below would be taken "
        "over a topic holding something other than what was published"
    )
    frame = payment_stream(
        kafka_spark, topic=topic, bootstrap=bootstrap, max_offsets_per_trigger=_PER_TRIGGER
    )
    root = tmp_path_factory.mktemp("exactly-once")
    arms = {
        arm: prove_arm(
            frame,
            arm=arm,
            path=(root / arm / "sink").as_posix(),
            checkpoint=(root / arm / "ckpt").as_posix(),
            app_id=f"opl-f5-{arm}",
            fault=BatchFault(batch_id=_FAULT_BATCH),
            minimum_rows=_LANDED_BEFORE_THE_RESTART,
        )
        for arm in ARMS
    }
    return _Proof(
        topic=topic,
        published=published.message_count,
        identities=tuple(record[IDENTITY_COLUMN] for record in records),
        arms=arms,
    )


def test_the_fault_struck_the_same_batch_in_both_arms(proof):
    """THE FAULT REACHED THE WINDOW, in both arms, over the same records.

    Without this the two later numbers are a comparison between one arm that was faulted
    and one that was not. Floors: each arm's first run invoked exactly batches (0, 1) and
    processed 20 rows -- so the run did work before it died -- and the killed batch is the
    same 10 offsets in both arms, which is the "one fault, the same offsets" claim itself
    rather than a restatement of the rate limit."""
    killed = {}
    for arm in ARMS:
        run = proof.arms[arm].faulted
        assert run.struck, f"the {arm} arm completed without the injected fault firing"
        assert run.batch_ids() == _BATCHES[: _FAULT_BATCH + 1]
        assert run.row_count() == _LANDED_BEFORE_THE_RESTART > 0
        assert run.killed_batch.batch_id == _FAULT_BATCH
        assert run.killed_batch.row_count == _FAULTED_BATCH_ROWS
        killed[arm] = run.killed_batch.coordinates
    assert killed[NAIVE] == killed[GUARDED]


def test_both_arms_replayed_the_faulted_batch_over_the_same_offsets(proof):
    """THE RESTART REALLY REPLAYED, and it replayed the SAME batch.

    A restart that read nothing produces no invocation at all, and one that resumed past
    the uncommitted batch produces different offsets -- neither is visible in a row count,
    and in the guarded arm neither is visible in the final total either, because a skipped
    replay and an absent replay land the identical 29 rows. The ledger is where they are
    distinguishable, so that is where this looks. Floors: the replayed batch carries 10
    rows (not zero), and the restart also ran batch 2, so it did more than repeat."""
    replayed = {}
    for arm in ARMS:
        evidence = proof.arms[arm]
        assert evidence.replayed.batch_ids() == (_FAULT_BATCH, _BATCHES[-1])
        assert evidence.replayed_batch.row_count == _FAULTED_BATCH_ROWS > 0
        assert evidence.replayed_batch.coordinates == evidence.faulted.killed_batch.coordinates
        replayed[arm] = evidence.replayed_batch.coordinates
    assert replayed[NAIVE] == replayed[GUARDED]


def test_the_naive_arm_writes_the_replayed_batch_twice(proof, kafka_spark):
    """THE NEGATIVE CONTROL, AND IT IS THE DELIVERABLE.

    `foreachBatch` + a plain append has no idempotency key, so the replayed batch lands a
    second time: 39 rows over 29 deliveries.

    THE SIZE OF THE DUPLICATION IS NOT ITS IDENTITY, and the counts alone cannot tell them
    apart here. `39 - 29 == 10` and `replayed_batch.row_count == 10` are both 10, but they
    are 10 for unrelated reasons -- the first is what the sink holds twice, the second is
    `maxOffsetsPerTrigger` -- and EVERY non-final batch in this run carries 10 rows, so
    that equality would hold for any 10-row batch landing twice, including one that is not
    the batch the fault killed. So the duplicated COORDINATES are read off the landed table
    and compared against the killed batch's own: the same pairs, each landed exactly twice,
    and nothing else in the table landed more than once. A duplication of the right SIZE in
    the wrong PLACE -- any other 10-row batch landing twice, or ten unrelated deliveries
    landing twice each -- satisfies every count above this and fails here.

    WHAT IT IS AND IS NOT SENSITIVE TO, both MUTATED AND RUN rather than reasoned about,
    because "this assertion is not inert" is the claim this phase keeps having to withdraw.
    The two sides come from INDEPENDENT instruments -- `killed` is the driver's own ledger
    of the batch the fault struck, `repeated` is a `groupBy` over the landed Delta table --
    and it is a DISAGREEMENT BETWEEN THE TWO that fails here.

      * SENSITIVE, and this is the mutation that shows it. Patch `_batch_body` so the naive
        RESTART skips the re-append of the batch it is replaying and appends a copy of batch
        0's ten rows instead: a duplication of the right size in the wrong place. Run: 39
        rows, 29 deliveries, 10 duplicates, an unchanged ledger, the two ledger tests above
        green and the guarded arm green -- and the ONLY red is the coordinate comparison
        below, `{(0,0)..(0,9)}` against a killed batch of `{(0,10)..(0,19)}`.
      * NOT SENSITIVE to WHICH batch was faulted. `_FAULT_BATCH = 0` moves the ledger and
        the landed duplication together, and THIS TEST STAYS GREEN under it; the file does
        not, but it goes red one test up, on `(0, 1, 2) != (0, 2)`, because that expectation
        is written for the middle batch. So a mutation that moves the fault demonstrates
        nothing about the pair below, in either direction."""
    duplicates = proof.arms[NAIVE].duplicates
    assert duplicates.duplicate_rows > 0, (
        "the NAIVE arm did not duplicate. This does not mean the pipeline is exactly-once: "
        "it means the fault never reached the window between the data write and the offset "
        "commit, and the guarded arm's zero is therefore evidence of nothing."
    )
    assert duplicates.row_count == _NAIVE_ROWS
    assert duplicates.distinct_records == _DELIVERED == proof.published
    assert duplicates.duplicate_rows == proof.arms[NAIVE].replayed_batch.row_count

    killed = proof.arms[NAIVE].faulted.killed_batch.coordinates
    repeated = _landed_more_than_once(kafka_spark, proof.arms[NAIVE].path)
    assert {coordinates for coordinates, _ in repeated} == set(killed)
    assert [landings for _, landings in repeated] == [2] * len(killed)


def test_the_guarded_arm_writes_it_once(proof, kafka_spark):
    """THE CLAIM. `txnAppId`/`txnVersion` makes the append idempotent, so the replay of
    batch 1 is a no-op and the table holds the corpus exactly once.

    The floor is `distinct_records == published`: "no duplicates" over a table missing half
    the stream is true, and so is "no duplicates" over an empty one. This says the whole
    corpus is present AND that no delivery is in it twice, which are two claims and need
    the two numbers.

    THE COORDINATE SET IS READ WITH THE SAME INSTRUMENT THE NAIVE ARM'S IS, on purpose: the
    two arms differ in one option, so the measure taken over them must not differ at all,
    and an empty result HERE against a set of exactly the killed offsets THERE is the
    comparison the phase is actually making."""
    duplicates = proof.arms[GUARDED].duplicates
    assert duplicates.row_count == _GUARDED_ROWS
    assert duplicates.distinct_records == _DELIVERED == proof.published
    assert duplicates.duplicate_rows == 0
    assert _landed_more_than_once(kafka_spark, proof.arms[GUARDED].path) == ()


def test_a_redelivery_is_not_an_exactly_once_failure(proof, kafka_spark):
    """THE DISTINCTION F5 MUST NOT BLUR, shown as two measures that disagree on purpose.

    Read off the LANDED TABLES, not off the published records -- the published side is the
    input, and a claim about what the pipeline did that is measured on its input is the
    species this phase hunts. Both tables hold 24 distinct `transaction_id` values because
    the corpus carries 5 deliberate redeliveries, so the identity column reports the SAME
    count for the arm that is correct and the arm that is not: it cannot tell them apart.
    The delivery coordinate can, and does.

    The null floor is not decoration: `select(id).distinct().count()` counts NULL as a
    value, so a table whose identities failed to parse would report 25 distinct and read
    as a near-miss rather than as a broken parse."""
    assert len(set(proof.identities)) == _CLEAN_ROWS
    assert len(proof.identities) - len(set(proof.identities)) == _REDELIVERIES

    by_identity = {}
    for arm in ARMS:
        landed = kafka_spark.read.format("delta").load(proof.arms[arm].path)
        assert landed.count() == proof.arms[arm].duplicates.row_count > 0
        assert landed.filter(landed[IDENTITY_COLUMN].isNull()).count() == 0
        by_identity[arm] = landed.select(IDENTITY_COLUMN).distinct().count()

    assert by_identity[NAIVE] == by_identity[GUARDED] == _CLEAN_ROWS
    assert proof.arms[NAIVE].duplicates.duplicate_rows == _FAULTED_BATCH_ROWS
    assert proof.arms[GUARDED].duplicates.duplicate_rows == 0
