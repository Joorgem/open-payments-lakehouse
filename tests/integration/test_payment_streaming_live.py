# tests/integration/test_payment_streaming_live.py
"""The same equivalence as `tests/test_payment_streaming.py`, through a REAL client and a
REAL broker -- and the two claims a recording double structurally cannot make.

WHAT THIS FILE ADDS, AND IT IS NOT "the same tests but slower". The unit file digests the
bytes handed to `produce()`. Everything after that call is librdkafka and Redpanda:
serialisation to the wire protocol, the partitioner, batching, compression defaults, and
the round trip back through a consumer. Two of this phase's claims live entirely in that
gap and are unfalsifiable without a broker:

  1. THE BYTES SURVIVE THE ROUND TRIP. A value is produced and consumed back byte for byte,
     so `b"".join` over a single-partition topic reproduces F1b's pinned file exactly -- the
     golden digest, reached through Kafka rather than through a Volume.
  2. THE PARTITIONER PUTS A REDELIVERY WHERE ITS ORIGINAL WENT. The double assigns
     partitions by a hash of its own; only the real client runs `murmur2(key) % partitions`.
     T5's dedup claim rests on that property, and this is the only place it is measured.

DESELECTED BY DEFAULT, AND THAT IS THE POINT OF THE MARKERS. `integration` keeps it out of
every default invocation; `redpanda` is what lets T6's CI job select exactly this file with
a Redpanda service container, the way the `postgres` job already selects its three. `-m
integration` alone would drag in two live-WebDAV modules and go red on them.

EVERY TEST HERE CREATES ITS OWN TOPIC AND DELETES IT. A topic that survived would carry the
previous run's records, and the next run's "the multiset matches" would then be comparing
against a corpus published twice -- the exact shape of the checkpoint hazard section 5.2 of
the phase plan names. A fresh topic per test is what makes the consumed count a measurement.
"""
from __future__ import annotations

import hashlib
import time
import uuid
from collections import Counter
from collections.abc import Iterator

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from opl.bronze.generated_landing import serialised_bytes
from opl.contracts.payments import IDENTITY_COLUMN
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import DefectSpec, delivered_records
from opl.generator.stream import StreamSpec, records_for
from opl.streaming.messages import message_key, message_values
from opl.streaming.producer import BrokerConfig, publish_records

pytestmark = [pytest.mark.integration, pytest.mark.redpanda]

# The compose broker. `docker-compose.yml` advertises `PLAINTEXT://localhost:9092` and
# `tests/integration/test_redpanda.py` already hardcodes it; this is the second copy and
# it is a coordinate rather than a credential (see `opl.streaming.producer`'s docstring).
BOOTSTRAP = "localhost:9092"

# The pinned spec and digest, copied from `tests/test_payment_generator.py` for the reason
# `tests/test_payment_streaming.py` states: the FILE side is asserted against the literal
# first, so a drifted copy goes red rather than quietly describing another stream.
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
_PINNED_SHA = "b45f1dc7d507da08bcc3a5d2586317ca589aeda29187f68f5039ff4ba7ad9ac9"
_PINNED_ROWS = 24
_DUPLICATES = 5

_CONSUME_TIMEOUT_SECONDS = 60.0


@pytest.fixture
def fresh_topic() -> Iterator[object]:
    """`fresh_topic(partitions)` -> a topic name nothing has published to, deleted after.

    CREATED EXPLICITLY RATHER THAN BY AUTO-CREATION, which would give whatever default
    partition count the broker is configured with -- and the partition count is the
    variable two of these tests are stated over."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    created: list[str] = []

    def _make(partitions: int) -> str:
        name = f"opl-payments-t1-{uuid.uuid4().hex[:8]}"
        futures = admin.create_topics(
            [NewTopic(name, num_partitions=partitions, replication_factor=1)]
        )
        futures[name].result(timeout=30)
        created.append(name)
        return name

    yield _make
    for name in created:
        admin.delete_topics([name])


def _refuse_a_topic_that_does_not_hold_exactly(topic: str, expected: int) -> None:
    """Refuse a topic whose watermarks say it holds a number of records other than `expected`.

    THE CEILING, WHICH `_poll_until`'s FLOOR IS NOT. A poll loop that stops at `expected`
    returns `expected` records from a topic holding twice that, and every assertion
    downstream then holds over the FIRST 24 of 48. That is committed rather than argued:
    `test_a_topic_holding_the_corpus_twice_is_refused` publishes this corpus twice to one
    fresh single-partition topic, shows the floor-only read passing all three assertions
    `test_a_single_partition_topic_returns_the_pinned_file_in_offset_order` makes, and then
    shows this function refusing that topic, naming 48 against 24.

    THE WATERMARKS ARE AN EXACT ANSWER RATHER THAN A LONGER POLL. `high - low` per partition,
    summed over the partitions the topic declares, is what the broker holds right now -- a
    second poll loop with a grace period could only ever say "nothing more arrived within N
    seconds". Exact in both directions, hence the name: a topic holding FEWER than `expected`
    is also refused here, before the poll loop can spend its timeout discovering the same
    thing more slowly.

    BUT `high - low` IS AN OFFSET RANGE AND NOT A RECORD COUNT, and it is one here only
    because of two conditions. The producer is NON-TRANSACTIONAL (`BrokerConfig.client_config`
    sets `enable.idempotence` and `acks` and no `transactional.id`): 24 records published
    inside a transaction to a fresh single-partition topic on this broker measured `low=0
    high=26`, the commit markers taking offsets no consumer returns. And a topic created the
    way `fresh_topic` creates them measured `cleanup.policy=delete`, the broker default;
    compaction is the other way a range and a count come apart -- named here, not measured."""
    consumer = Consumer(
        {"bootstrap.servers": BOOTSTRAP, "group.id": f"opl-t1-watermark-{uuid.uuid4().hex[:8]}"}
    )
    try:
        partitions = consumer.list_topics(topic, timeout=30).topics[topic].partitions
        held = sum(
            high - low
            for low, high in (
                consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=30, cached=False
                )
                for partition in partitions
            )
        )
    finally:
        consumer.close()
    if held != expected:
        raise AssertionError(
            f"{topic!r} holds {held} records across {len(partitions)} partition(s) and "
            f"{expected} were published to it. Over {expected}, a consumer that stops at "
            f"{expected} returns a PREFIX of a larger topic -- and the offsets, the rebuilt "
            "digest and the multiset below all pass over a prefix. Under it, the corpus the "
            "producer said it delivered is not the corpus the broker is holding."
        )


def _poll_until(topic: str, expected: int) -> list[tuple[int, int, bytes, bytes]]:
    """(partition, offset, key, value) for `expected` records from `topic`, or refuse short.

    THE FLOOR AND ONLY THE FLOOR, enforced here rather than left to each caller: a poll loop
    that timed out early would hand back a short list, and every multiset comparison
    downstream would then be measuring a subset against itself. `earliest` and a group id
    nobody has used before, so a committed offset from an earlier run cannot make this
    return nothing and call it agreement.

    SPLIT OUT FROM `_consume` RATHER THAN INLINED IN IT, because the ceiling is a claim about
    a poll loop that has no ceiling, and `test_a_topic_holding_the_corpus_twice_is_refused`
    has to be able to call one without the other to show it. Every other test calls
    `_consume`, which is this plus the ceiling."""
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"opl-t1-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    collected: list[tuple[int, int, bytes, bytes]] = []
    deadline = time.monotonic() + _CONSUME_TIMEOUT_SECONDS
    while len(collected) < expected and time.monotonic() < deadline:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error() is not None:
            consumer.close()
            raise RuntimeError(f"consuming {topic!r} failed: {message.error()}")
        collected.append(
            (message.partition(), message.offset(), message.key(), message.value())
        )
    consumer.close()
    if len(collected) != expected:
        raise AssertionError(
            f"{len(collected)} of {expected} records came back from {topic!r} within "
            f"{_CONSUME_TIMEOUT_SECONDS}s. A short read makes every comparison below a "
            "comparison over a subset, which passes for reasons that are not the claim."
        )
    return collected


def _consume(topic: str, expected: int) -> list[tuple[int, int, bytes, bytes]]:
    """`_poll_until`'s floor with the ceiling above it: exactly `expected`, or refuse.

    THE CEILING RUNS FIRST because it is a question for the broker and not for the poll
    loop, and because it answers in one round trip what the loop could only fail to answer
    after `_CONSUME_TIMEOUT_SECONDS`."""
    _refuse_a_topic_that_does_not_hold_exactly(topic, expected)
    return _poll_until(topic, expected)


def test_a_single_partition_topic_returns_the_pinned_file_in_offset_order(fresh_topic):
    """THE GOLDEN DIGEST, REACHED THROUGH KAFKA. One partition, so offset order IS file
    order and the concatenation of the consumed values is the landed file byte for byte.

    The offsets are pinned as `range(24)` beside it, which says the broker assigned them
    contiguously from zero and NOT that nothing else is on the topic: `range(24)` is exactly
    what the first 24 of 48 records look like too. The ceiling in `_consume` is what makes
    the second claim, by asking the broker its watermarks rather than the consumer its
    prefix."""
    topic = fresh_topic(1)
    records = records_for(_SPEC)
    published = publish_records(records, topic=topic, broker=BrokerConfig(bootstrap=BOOTSTRAP))
    assert (published.message_count, published.sha256) == (_PINNED_ROWS, _PINNED_SHA)
    assert published.partition_counts == ((0, _PINNED_ROWS),)

    consumed = sorted(_consume(topic, _PINNED_ROWS))
    assert [offset for _, offset, _, _ in consumed] == list(range(_PINNED_ROWS))
    rebuilt = b"".join(value for _, _, _, value in consumed)
    assert rebuilt == serialised_bytes(records)
    assert hashlib.sha256(rebuilt).hexdigest() == _PINNED_SHA


def test_a_topic_holding_the_corpus_twice_is_refused(fresh_topic):
    """THE CEILING SHOWN FIRING, over a prefix shown passing without it.

    The hazard `_refuse_a_topic_that_does_not_hold_exactly` exists for is not that a doubled
    topic turns the test above red. It is that a doubled topic keeps it GREEN: the first 24
    of 48 ARE the pinned corpus, at offsets 0..23, digesting to `b45f1dc7...`. So this makes
    that half first -- the same three assertions, over a floor-only read of a topic holding
    48 -- and only then asserts the refusal. A refusal test without it would be green over a
    topic the ceiling refused for some other reason, which is the shape this file's own
    prose refuses elsewhere.

    THE FLOOR IS THE TWO ACKNOWLEDGED PUBLISHES, stated before the read: 24 each, so 48 is a
    number this test put on the topic rather than one it read back and trusted."""
    topic = fresh_topic(1)
    records = records_for(_SPEC)
    for _ in range(2):
        published = publish_records(
            records, topic=topic, broker=BrokerConfig(bootstrap=BOOTSTRAP)
        )
        assert published.message_count == _PINNED_ROWS

    prefix = sorted(_poll_until(topic, _PINNED_ROWS))
    assert [offset for _, offset, _, _ in prefix] == list(range(_PINNED_ROWS))
    rebuilt = b"".join(value for _, _, _, value in prefix)
    assert rebuilt == serialised_bytes(records)
    assert hashlib.sha256(rebuilt).hexdigest() == _PINNED_SHA

    with pytest.raises(AssertionError, match=r"holds 48 records across 1 partition\(s\)"):
        _consume(topic, _PINNED_ROWS)


def test_a_three_partition_topic_returns_the_same_multiset(fresh_topic):
    """THE FORM THAT SURVIVES PARTITIONING, measured where partitioning actually happens.

    Two floors, because the multiset claim is trivially true over one partition and over an
    empty read: 24 records back, spread over more than one partition. The producer's own
    `partition_counts` is asserted to agree with what the consumer saw, which is the one
    place this repository can check that the evidence line a run prints describes the
    records a reader would find."""
    topic = fresh_topic(3)
    records = records_for(_SPEC)
    published = publish_records(records, topic=topic, broker=BrokerConfig(bootstrap=BOOTSTRAP))
    consumed = _consume(topic, _PINNED_ROWS)

    assert Counter(value for _, _, _, value in consumed) == Counter(message_values(records))
    seen = Counter(partition for partition, _, _, _ in consumed)
    assert len(seen) > 1, "one partition makes the multiset claim true for the wrong reason"
    assert tuple(sorted(seen.items())) == published.partition_counts


def test_a_redelivery_lands_in_its_originals_partition(fresh_topic):
    """THE PARTITIONER'S OWN PROPERTY, and the double structurally cannot prove it.

    `murmur2(key) % partitions` is librdkafka's, so only a real client decides where a key
    goes. A redelivery carries its original's `transaction_id`, hence its key, hence its
    partition -- which is what lets T5 state "the pipeline collapses exactly these 5" without
    the answer depending on an interleaving. Three floors: 29 records, exactly 5 keys seen
    twice, and more than one partition in use."""
    topic = fresh_topic(3)
    records = delivered_records(_SPEC, DefectSpec(duplicate_count=_DUPLICATES))
    publish_records(records, topic=topic, broker=BrokerConfig(bootstrap=BOOTSTRAP))
    consumed = _consume(topic, _PINNED_ROWS + _DUPLICATES)

    partitions: dict[bytes, set[int]] = {}
    for partition, _, key, _ in consumed:
        partitions.setdefault(key, set()).add(partition)
    keys = Counter(key for _, _, key, _ in consumed)

    assert len(keys) == _PINNED_ROWS
    assert sum(1 for count in keys.values() if count == 2) == _DUPLICATES
    assert len({partition for partition, _, _, _ in consumed}) > 1
    assert all(len(seen) == 1 for seen in partitions.values())
    assert set(keys) == {message_key(record) for record in records}, (
        f"every consumed key is the {IDENTITY_COLUMN} of a record that was published, and "
        "every one of those is a key that came back"
    )
