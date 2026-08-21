# src/opl/streaming/ingest.py
"""The consuming half of F5: a Structured Streaming read of the Redpanda topic into a
LOCAL DELTA TABLE, checkpointed, `trigger(availableNow=True)`.

--- WHAT A ROW IS HERE, AND WHY IT CARRIES ITS KAFKA COORDINATES --------------------------

Every landed row keeps three things the message came with -- its partition, its offset and
its RAW VALUE BYTES -- beside the contract columns parsed out of it. Each is load-bearing
and none is decoration.

  * `kafka_partition` + `kafka_offset` are the PROCESSING IDENTITY: within a topic, the
    pair names one delivery of one record and nothing else. It is what T3's exactly-once
    proof counts, and it is deliberately NOT `transaction_id`. This corpus contains 150
    deliberate REDELIVERIES (`opl.generator.defects`) -- one `transaction_id` published
    twice, on purpose, as a DATA property F1b already measures. A proof of exactly-once
    PROCESSING that counted transaction ids would report those as its own failure and
    would be measuring the generator. The two questions are separate and this column pair
    is what separates them.
  * `kafka_value` is the record, and the parsed columns are a convenience over it. The
    read schema (`opl.bronze.schema.struct_for`) deliberately excludes the drift column,
    so `from_json` DROPS a drifted record's extra key -- that absence is what makes drift
    measurable downstream, and it would make the parsed columns a lossy account of the
    stream if the bytes were not kept. With them kept, the landed table can still be
    rebuilt into F1b's pinned file byte for byte, which is what
    `tests/integration/test_payment_stream_ingest.py` asserts against `b45f1dc7...`.

--- NO WALL CLOCK, ANYWHERE ---------------------------------------------------------------

Nothing here calls `current_timestamp()` and nothing adds an ingestion instant. The
generator's `event_time` and `emitted_at` are derived from a seed, and a column reading the
clock of the machine that ran the test would be the one value in this pipeline that no
prediction could be published for. `opl.bronze.autoloader.add_audit_columns` is where a
bronze ingest stamps arrival; this is not that ingest, and borrowing it would put a
non-reproducible column into the table the exactly-once counts are taken over.

--- `startingOffsets: latest` IS REFUSED IN SHIPPED CODE ----------------------------------

The phase plan's §5.1 hazard, in one option: over a topic that has already been published
to, `latest` yields ZERO rows, and "no duplicates appeared" is then true, cheap and
worthless. So it is not available here at all -- the refusal is in the shipped function
rather than in an assertion, for `opl.streaming.messages._refuse_an_empty_corpus`'s reason:
a floor that lives only in a test protects only the tests that remember it.

The same argument produces `minimum_rows` on `write_payment_stream`. A run that processed
nothing is refused by DEFAULT, which is what turns "the checkpoint had already consumed
everything" from a silent zero into a named failure.

AND THE DEFAULT IS A FLOOR AGAINST ZERO, NOT AGAINST A SHORT READ -- stated here because
"a non-zero floor" reads like the stronger promise and is not it. `minimum_rows=1` accepts
a run that consumed 1 of 29 records; the only thing it refuses is 0. A SHORT read is caught
one level up, by callers that state the exact count they expect. Every call of this function
today is in `tests/integration/test_payment_stream_ingest.py` and all five take the DEFAULT:
three then assert the `input_rows` they predicted (24, 29 and 24), a fourth discards the
return and pins the LANDED row count at 24 instead, and the fifth is the second run over a
drained checkpoint, which is the refusal below firing. So: the default is the floor that
lives in SHIPPED code and catches the drained checkpoint; the exact count is the assertion
that lives in the caller and catches everything between 1 and the truth.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.bronze.schema import struct_for
from opl.contracts.payments import COLUMNS, CONTRACT

# The processing identity: which DELIVERY of a record this row is. See the module
# docstring for why it is not `transaction_id`.
PARTITION_COLUMN = "kafka_partition"
OFFSET_COLUMN = "kafka_offset"
PROCESSING_IDENTITY = (PARTITION_COLUMN, OFFSET_COLUMN)

# The record's bytes, kept verbatim beside the parsed columns.
VALUE_COLUMN = "kafka_value"

KAFKA_COLUMNS = (*PROCESSING_IDENTITY, VALUE_COLUMN)

# The only offset spec this module accepts, and the one it refuses by name. A JSON
# per-partition spec is accepted too (it is explicit about where it starts); `latest` is
# not, for the reason the module docstring gives.
EARLIEST = "earliest"
LATEST = "latest"


def _assert_the_kafka_columns_do_not_shadow_the_contract() -> None:
    """Refuse AT IMPORT if a Kafka column name collides with a contract column.

    A collision would not raise anywhere: `select` would simply keep one of the two, and
    the landed table would carry a payment field under a name this module's counts read as
    an offset. `opl.contracts.payments` runs its own import-time assertions for the same
    class of edit, and this is the one that belongs on this side of the seam."""
    collisions = sorted(set(KAFKA_COLUMNS) & set(COLUMNS))
    if collisions:
        raise AssertionError(
            f"{collisions} is both a Kafka metadata column and a payments contract column. "
            "One of the two would be silently dropped by the projection below, and the "
            "exactly-once counts would then be taken over a payment field."
        )


_assert_the_kafka_columns_do_not_shadow_the_contract()


@dataclass(frozen=True, kw_only=True)
class IngestedStream:
    """What one `availableNow` run of a stream processed, taken from SPARK'S OWN PROGRESS
    and not from the sink.

    THE POINT OF READING IT HERE is that it is an INDEPENDENT number. A test that counted
    the rows in the Delta table and compared them against the rows in the Delta table would
    report agreement under every outcome. `input_rows` comes from the source side of the
    query -- how many Kafka records the micro-batches consumed -- so `sink.count() ==
    ingested.input_rows` is two measurements meeting, which is the form `PublishedStream`'s
    acknowledged count already uses on the produce side.

    `batch_ids` is the second, and it is what makes the rate-limited split OBSERVABLE:
    T3's fault is injected on a chosen batch id, and a batch id can only be chosen because
    a run states which ones it had."""

    batch_ids: tuple[int, ...]
    input_rows: int


def _refuse_offsets_that_can_read_nothing(starting_offsets: str) -> None:
    """Refuse `latest`. See the module docstring's third section."""
    if starting_offsets != LATEST:
        return
    raise ValueError(
        f"refusing startingOffsets={LATEST!r}. The corpus is published to the topic BEFORE "
        "the stream starts, so `latest` reads zero records -- and a dedup assertion, a "
        "multiset comparison and a duplicate count are all TRUE over zero rows. Pass "
        f"{EARLIEST!r}, or an explicit per-partition JSON offset spec if a run really is "
        "meant to start somewhere else."
    )


def payment_stream(
    spark: SparkSession,
    *,
    topic: str,
    bootstrap: str,
    starting_offsets: str = EARLIEST,
    max_offsets_per_trigger: int | None = None,
) -> DataFrame:
    """The streaming read of `topic`: Kafka coordinates, raw value, and parsed contract
    columns.

    `max_offsets_per_trigger` is what splits ONE `availableNow` run into several
    micro-batches -- deterministically, because the offsets are fixed before the run
    starts. T3 needs more than one batch to have a batch to fault, and this is the only
    knob that produces them without introducing a clock."""
    _refuse_offsets_that_can_read_nothing(starting_offsets)
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        # DECLARED rather than inherited. It is already the connector's default; stating it
        # says that a topic whose records aged out from under a checkpoint must FAIL the
        # query, not silently resume further along -- which is §5.1's hazard reached
        # through the broker's retention policy instead of through a test's options.
        .option("failOnDataLoss", "true")
    )
    if max_offsets_per_trigger is not None:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    return _projected(reader.load())


def _projected(frame: DataFrame) -> DataFrame:
    """Kafka's envelope reduced to what this lakehouse keeps, plus the parsed record."""
    parsed = F.from_json(F.col("value").cast("string"), struct_for(CONTRACT))
    return frame.select(
        F.col("partition").alias(PARTITION_COLUMN),
        F.col("offset").alias(OFFSET_COLUMN),
        F.col("value").alias(VALUE_COLUMN),
        *[parsed.getField(column).alias(column) for column in COLUMNS],
    )


def _progress_of(query, spark: SparkSession) -> tuple[tuple[int, ...], int]:
    """The batch ids that consumed something, and the total they consumed.

    THE TRUNCATION IS REFUSED RATHER THAN SUMMED OVER. `recentProgress` is a RING BUFFER
    capped by `spark.sql.streaming.numRecentProgressUpdates` (100 by default), so a query
    with more batches than that silently reports the sum of its LAST hundred -- a number
    smaller than the truth, in the same shape as every other short read this phase
    refuses. Nothing here needs a long run; a run that becomes one gets a message instead
    of an undercount."""
    progresses = query.recentProgress
    cap = int(spark.conf.get("spark.sql.streaming.numRecentProgressUpdates", "100"))
    if len(progresses) >= cap:
        raise RuntimeError(
            f"the query reported {len(progresses)} progress updates against a "
            f"`numRecentProgressUpdates` cap of {cap}. The buffer is a ring, so the "
            "earliest batches have been dropped and any total taken over it is an "
            "undercount. Raise the cap or shorten the run."
        )
    consuming = [p for p in progresses if p["numInputRows"] > 0]
    return (
        tuple(int(p["batchId"]) for p in consuming),
        sum(int(p["numInputRows"]) for p in consuming),
    )


def _refuse_a_run_that_processed_nothing(input_rows: int, minimum_rows: int, topic: str) -> None:
    """The floor, in shipped code. At the DEFAULT `minimum_rows` this refuses zero and
    nothing else -- see the module docstring's last section for what catches a short read
    instead, and `tests/test_streaming_delta_ingest.py` for the arms of both."""
    if input_rows >= minimum_rows:
        return
    raise RuntimeError(
        f"the stream over {topic!r} consumed {input_rows} records and at least "
        f"{minimum_rows} were required. A checkpoint that had already consumed the topic, "
        "a topic that was recreated, or a spec that read from the wrong end all produce "
        "this -- and every count taken over the empty table downstream would have passed."
    )


def write_payment_stream(
    frame: DataFrame,
    *,
    path: str,
    checkpoint: str,
    topic: str,
    minimum_rows: int = 1,
) -> IngestedStream:
    """Run `frame` to completion into the Delta table at `path`. THE T2 SINK.

    `format("delta")` DELIBERATELY, AND THAT IS WHY IT PROVES NOTHING ABOUT EXACTLY-ONCE.
    This sink is exactly-once BY CONSTRUCTION -- it commits the batch id into the Delta log
    and ignores a replay of it -- so an experiment run against this function reports
    success whatever happens, including when nothing ran. It is the right sink for an
    ingest and the wrong one for a proof; the proof lives in `opl.streaming.exactly_once`,
    where the batch is written by user code and a replay can double-write.

    `availableNow` rather than `once` or a continuous trigger: it drains everything the
    broker holds and STOPS, which is what makes a run a measurement rather than a
    subscription -- and, unlike `once`, it still honours `maxOffsetsPerTrigger`, so the
    multi-batch split T3 depends on survives."""
    query = (
        frame.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start(path)
    )
    query.awaitTermination()
    batch_ids, input_rows = _progress_of(query, frame.sparkSession)
    _refuse_a_run_that_processed_nothing(input_rows, minimum_rows, topic)
    return IngestedStream(batch_ids=batch_ids, input_rows=input_rows)
