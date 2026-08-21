# tests/integration/test_payment_stream_ingest.py
"""T2: the Redpanda topic read into a local Delta table, checkpointed, `availableNow`.

THE ANCHOR IS THE SAME 64-CHARACTER LITERAL THE PRODUCE SIDE USES.
`tests/test_payment_generator.py::test_the_pinned_stream_is_this_one_and_no_other` pins
sha256 `b45f1dc7...` over the serialised corpus; `tests/test_payment_streaming.py` reaches
it through the producer's frames, `test_payment_streaming_live.py` reaches it through a
real consumer, and this file reaches it through SPARK -- read from Kafka, parsed, landed in
Delta, and rebuilt out of the landed rows. Four routes, one constant, none of them
comparing the code under test against itself.

THE COPY OF `_SPEC` CANNOT ROT, for the reason `tests/test_payment_streaming.py` gives: the
FILE side is asserted against the literal first, so a `_SPEC` that stopped describing F1b's
pinned stream goes red here before anything about Spark is reached.

EVERY ASSERTION CARRIES A NON-ZERO FLOOR ON ROWS ACTUALLY PROCESSED. An empty Delta table
satisfies "no duplicates", "every row parses" and "the multisets agree" simultaneously and
for free -- so each test states the count it is comparing over, and each count is one the
declarations predict rather than one this file reads back and trusts.
"""
from __future__ import annotations

import hashlib

import pytest

from opl.bronze.generated_landing import serialised_bytes
from opl.contracts.payments import BUSINESS_ATTRIBUTE_COLUMNS, IDENTITY_COLUMN
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import DefectSpec, delivered_records
from opl.generator.stream import StreamSpec, records_for
from opl.streaming.ingest import (
    EARLIEST,
    LATEST,
    OFFSET_COLUMN,
    PARTITION_COLUMN,
    VALUE_COLUMN,
    payment_stream,
    write_payment_stream,
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
_PINNED_SHA = "b45f1dc7d507da08bcc3a5d2586317ca589aeda29187f68f5039ff4ba7ad9ac9"
_PINNED_ROWS = 24
_DUPLICATES = 5


def _publish(topic: str, records, bootstrap: str) -> int:
    published = publish_records(
        records, topic=topic, broker=BrokerConfig(bootstrap=bootstrap)
    )
    assert published.message_count == len(records), (
        "the broker did not acknowledge the whole corpus, so nothing read back from this "
        "topic describes the stream the generator produced"
    )
    return published.message_count


def _landed(spark, path: str):
    return spark.read.format("delta").load(path)


def test_the_topic_lands_in_delta_as_the_pinned_file(kafka_spark, kafka_topic, bootstrap, tmp_path):
    """THE GOLDEN DIGEST, REACHED THROUGH STRUCTURED STREAMING.

    One partition, so offset order IS file order and the concatenation of the landed value
    bytes is F1b's file byte for byte. Three floors before the digest: the broker
    acknowledged 24, Spark's own progress says it consumed 24, and the Delta table holds
    24 -- three independently produced numbers, none of them read off another."""
    topic = kafka_topic(1)
    records = records_for(_SPEC)
    assert _publish(topic, records, bootstrap) == _PINNED_ROWS

    frame = payment_stream(kafka_spark, topic=topic, bootstrap=bootstrap)
    ingested = write_payment_stream(
        frame,
        path=(tmp_path / "sink").as_posix(),
        checkpoint=(tmp_path / "ckpt").as_posix(),
        topic=topic,
    )
    assert (ingested.batch_ids, ingested.input_rows) == ((0,), _PINNED_ROWS)

    landed = _landed(kafka_spark, (tmp_path / "sink").as_posix())
    assert landed.count() == _PINNED_ROWS
    rows = sorted(landed.collect(), key=lambda row: row[OFFSET_COLUMN])
    assert [row[OFFSET_COLUMN] for row in rows] == list(range(_PINNED_ROWS))
    assert {row[PARTITION_COLUMN] for row in rows} == {0}

    rebuilt = b"".join(bytes(row[VALUE_COLUMN]) for row in rows)
    assert rebuilt == serialised_bytes(records)
    assert hashlib.sha256(rebuilt).hexdigest() == _PINNED_SHA


def test_every_landed_row_carries_the_contract_columns_the_json_held(
    kafka_spark, kafka_topic, bootstrap, tmp_path
):
    """THE PARSE, over a floor that makes "no nulls" mean something.

    `from_json` returns a struct of NULLs for a value it cannot parse, so "no row has a
    null identity" is a real claim only once the row count is pinned: 24 rows and 24
    non-null identities.

    AND THE BUSINESS COLUMNS ARE CHECKED SEPARATELY, because the identity alone cannot say
    the parse reached past the first field. `_SPEC` declares 4 LEGITIMATE REPEATS -- a
    second payment with the same business attributes and its OWN `transaction_id`
    (`opl.generator.defects`' header draws that distinction) -- so 24 distinct identities
    must sit over exactly 20 distinct attribute tuples. A projection that produced the
    right shape and the wrong values gets both numbers wrong; one that parsed only the
    identity gets the second one wrong on its own."""
    topic = kafka_topic(1)
    records = records_for(_SPEC)
    assert _publish(topic, records, bootstrap) == _PINNED_ROWS

    frame = payment_stream(kafka_spark, topic=topic, bootstrap=bootstrap)
    write_payment_stream(
        frame,
        path=(tmp_path / "sink").as_posix(),
        checkpoint=(tmp_path / "ckpt").as_posix(),
        topic=topic,
    )
    landed = _landed(kafka_spark, (tmp_path / "sink").as_posix())
    assert landed.count() == _PINNED_ROWS
    assert landed.filter(landed[IDENTITY_COLUMN].isNotNull()).count() == _PINNED_ROWS
    rows = landed.collect()
    identities = {row[IDENTITY_COLUMN] for row in rows}
    assert len(identities) == _PINNED_ROWS
    assert identities == {record[IDENTITY_COLUMN] for record in records}

    attributes = {tuple(row[column] for column in BUSINESS_ATTRIBUTE_COLUMNS) for row in rows}
    assert len(attributes) == _SPEC.base_count == _PINNED_ROWS - _SPEC.repeat_count
    assert attributes == {
        tuple(record[column] for column in BUSINESS_ATTRIBUTE_COLUMNS) for record in records
    }


def test_a_rate_limited_run_splits_into_the_batches_the_arithmetic_predicts(
    kafka_spark, kafka_topic, bootstrap, tmp_path
):
    """THE PROPERTY T3'S FAULT RESTS ON, asserted where it can be seen on its own.

    `maxOffsetsPerTrigger` is the only knob in this phase that produces more than one
    micro-batch without introducing a clock, and T3 injects its fault on a chosen batch
    id -- which is a choice only because the split is a fixed function of the corpus. 29
    records at 10 per trigger is 3 batches, ids 0..2, and the last one holds 9. Predicted
    from the declaration, then measured."""
    topic = kafka_topic(1)
    records = delivered_records(_SPEC, DefectSpec(duplicate_count=_DUPLICATES))
    delivered = _PINNED_ROWS + _DUPLICATES
    assert _publish(topic, records, bootstrap) == delivered

    frame = payment_stream(
        kafka_spark, topic=topic, bootstrap=bootstrap, max_offsets_per_trigger=10
    )
    ingested = write_payment_stream(
        frame,
        path=(tmp_path / "sink").as_posix(),
        checkpoint=(tmp_path / "ckpt").as_posix(),
        topic=topic,
    )
    assert ingested.batch_ids == (0, 1, 2)
    assert ingested.input_rows == delivered
    assert _landed(kafka_spark, (tmp_path / "sink").as_posix()).count() == delivered


def test_a_second_run_over_the_same_checkpoint_is_refused_rather_than_counted_as_zero(
    kafka_spark, kafka_topic, bootstrap, tmp_path
):
    """§5.1's HAZARD, TURNED INTO A REFUSAL.

    The first run consumes the topic; the second finds nothing left and would otherwise
    return an `IngestedStream` of zero rows over a table that is already correct -- from
    which "the ingest is idempotent" reads as a pass. The floor lives in
    `write_payment_stream`, so the second run raises instead, and the table is asserted
    UNCHANGED afterwards so the refusal is not mistaken for a run that damaged something."""
    topic = kafka_topic(1)
    records = records_for(_SPEC)
    assert _publish(topic, records, bootstrap) == _PINNED_ROWS
    sink, checkpoint = (tmp_path / "sink").as_posix(), (tmp_path / "ckpt").as_posix()

    frame = payment_stream(kafka_spark, topic=topic, bootstrap=bootstrap)
    assert write_payment_stream(
        frame, path=sink, checkpoint=checkpoint, topic=topic
    ).input_rows == _PINNED_ROWS

    with pytest.raises(RuntimeError, match="consumed 0 records"):
        write_payment_stream(frame, path=sink, checkpoint=checkpoint, topic=topic)
    assert _landed(kafka_spark, sink).count() == _PINNED_ROWS


def test_the_read_refuses_to_start_where_the_corpus_is_already_behind_it(
    kafka_spark, kafka_topic, bootstrap
):
    """`startingOffsets: latest` over an already-published topic reads nothing, and every
    dedup claim is then true and worthless. Refused in `payment_stream` before a query
    exists -- and the `earliest` spelling is asserted to still build one, so the refusal is
    shown to be about the VALUE rather than about the option being unreachable."""
    topic = kafka_topic(1)
    with pytest.raises(ValueError, match=f"refusing startingOffsets={LATEST!r}"):
        payment_stream(
            kafka_spark, topic=topic, bootstrap=bootstrap, starting_offsets=LATEST
        )
    frame = payment_stream(
        kafka_spark, topic=topic, bootstrap=bootstrap, starting_offsets=EARLIEST
    )
    assert frame.isStreaming
