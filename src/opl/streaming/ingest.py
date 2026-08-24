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

--- TWO DOORS T8 OPENED, AND NEITHER IS A SECOND SPELLING OF THIS MODULE ------------------

The managed-broker job reads the SAME topic layout through THIS function, which is the
whole point of it existing -- so what it needed was two parameters, not a second ingest.

  * `broker_options` on the read. The managed cluster is SASL_SSL, and the JVM client's
    spelling of that credential is `opl.streaming.managed_broker`'s subject rather than
    this one's. What is this module's business is that a caller must not be able to hand
    in an option that re-decides something decided HERE:
    `_refuse_options_that_reopen_a_decision` refuses `startingOffsets` above all, because a
    caller-supplied `latest` would walk straight around the refusal three paragraphs up
    -- through the parameter added to support it.
  * `table` on the write, as an alternative to `path`. On the deploy target the two are
    not interchangeable: Unity Catalog does not let a Delta table be created inside a
    Volume, so a serverless run has a NAME to write to and no writable path, while every
    local test has a tmp dir and no catalog. Exactly one of the two, refused otherwise --
    passing both leaves the destination ambiguous, and passing neither is no destination
    at all.

--- AND A THIRD THING T8 FOUND: THIS MODULE'S OWN INSTRUMENT IS NOT ALWAYS READABLE -------

`_progress_of` sized its truncation refusal off a Spark config, and SERVERLESS REFUSES TO
READ THAT CONFIG AT ALL. Measured, after the run had already landed 10,151 rows:
`[CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION]` out of
`SparkConnectConfig$.assertConfigAllowedForRead`. Substituting the value it would have
returned would turn the one guard here that cannot fire in a shipped run into a guess --
a check whose output cannot tell "verified" from "could not look" -- so `RingBufferReading`
carries WHICH argument ruled truncation out, or that none did, and a run prints it beside
its count.
"""
from __future__ import annotations

from collections.abc import Mapping
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

# The Spark config that sizes `recentProgress`'s ring buffer, spelled once. READ rather
# than assumed, and on a serverless session not readable at all -- see
# `_cap_or_the_reason_it_could_not_be_read`.
PROGRESS_CAP_CONFIG = "spark.sql.streaming.numRecentProgressUpdates"

# The batch id a streaming query's FIRST batch carries. A query whose checkpoint is fresh
# (or whose checkpoint committed nothing) starts here; one that resumes a checkpoint
# continues from the id after the last committed batch. The ring evicts OLDEST-FIRST, so a
# buffer still holding this id has evicted nothing -- there is nothing before it to evict.
# That is `RingBufferReading`'s second argument, and the only one left where the cap is
# unreadable.
FIRST_BATCH_ID = 0

KAFKA_COLUMNS = (*PROCESSING_IDENTITY, VALUE_COLUMN)

# The only offset spec this module accepts, and the one it refuses by name. A JSON
# per-partition spec is accepted too (it is explicit about where it starts); `latest` is
# not, for the reason the module docstring gives.
EARLIEST = "earliest"
LATEST = "latest"

# Every reader option `payment_stream` decides for itself. A caller-supplied option under
# one of these names is refused rather than merged, and `startingOffsets` is why the set
# exists at all: silently letting a caller re-spell it would reopen the `latest` hole this
# module refuses in shipped code, through the door that was opened to carry SASL.
DECIDED_READER_OPTIONS = frozenset(
    {
        "kafka.bootstrap.servers",
        "subscribe",
        "startingOffsets",
        "failOnDataLoss",
        "maxOffsetsPerTrigger",
    }
)

# The same names folded for comparison. See `_refuse_options_that_reopen_a_decision` for
# why the refusal cannot be an exact-spelling one.
_DECIDED_FOLDED = frozenset(name.lower() for name in DECIDED_READER_OPTIONS)


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
class RingBufferReading:
    """WHETHER `input_rows` IS THE RUN'S WHOLE TOTAL OR ONLY A LOWER BOUND -- and by which
    of two arguments, because a run that cannot tell those apart must print neither.

    `recentProgress` IS A RING BUFFER: capped by `PROGRESS_CAP_CONFIG` (100 on every session
    this project has measured), evicting its OLDEST entry on overflow. A total summed over
    an overflowed buffer is an undercount, in the same shape as every other short read this
    phase refuses.

    THE OBVIOUS CHECK -- retained updates against the cap -- NEEDS THE CAP, AND SERVERLESS
    WILL NOT HAND IT OVER. Measured on T8's job run `570309961086740` (task run
    `533379837633364`), AFTER it had already landed 10,151 rows into
    `workspace.default.streaming_payments_managed_broker`:

        [CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION] Configuration
        spark.sql.streaming.numRecentProgressUpdates is not available.  SQLSTATE: 42K0I
          at com.databricks.sql.connect.SparkConnectConfig$.assertConfigAllowedForRead(:487)
          at ...SparkConnectConfigHandler.handleGetWithDefault(SparkConnectConfigHandler:366)

    AND A DEFAULT DOES NOT RESCUE IT -- that second frame is the proof, not an opinion. The
    call that raised WAS `spark.conf.get(key, "100")`; PySpark's Connect client routes a
    call carrying a default to `ConfigRequest.GetWithDefault` (`RuntimeConf.get`, in
    `pyspark/sql/connect/conf.py`), and the refusal is raised inside the handler for exactly
    that request. The
    default is applied by the SERVER, after a read it will not perform.

    AND `100` IS NOT PUT IN ITS PLACE. A hardcoded cap would report "the ring did not
    truncate" on a session that never said how big the ring was -- output identical to a
    real verification, which is the one property this module refuses everywhere else.

    SO THERE ARE TWO ARGUMENTS, AND A READING NAMES THE ONE IT USED:

      * THE CAP, where the session hands it over: fewer retained updates than the cap and
        nothing was evicted. Every local run takes this branch and it is unchanged.
      * THE FIRST BATCH ID, where it does not: the ring evicts oldest-first, so a buffer
        whose oldest retained update is `FIRST_BATCH_ID` has evicted nothing WHATEVER its
        size. That is a measurement over the same progress list the total is summed from,
        not a fallback -- and it covers the run T8 actually makes, a fresh checkpoint over
        a topic, where batch 0 is the first batch there is.

    WHEN NEITHER APPLIES -- an unreadable cap over a RESUMED checkpoint, whose oldest
    retained id is some later number -- `truncation_ruled_out` is False and `describe()`
    calls the count a LOWER BOUND. Nothing here refuses that run: an undercount is what
    `_refuse_a_run_that_processed_nothing`'s floor is for, and that floor is a number the
    CALLER declares (T8 declared 10,151 at launch) rather than one this module could
    invent."""

    updates: int
    earliest_batch_id: int | None
    cap: int | None
    unreadable_because: str | None

    @property
    def truncation_ruled_out(self) -> bool:
        """True only where one of the two arguments actually ran.

        Both terms of the cap arm are checked here rather than inferred from `_progress_of`
        having raised: a property reading "the cap is known" alone would answer True on a
        reading built anywhere else, and this value is what a caller prints."""
        if self.cap is not None:
            return self.updates < self.cap
        return self.earliest_batch_id == FIRST_BATCH_ID

    def describe(self) -> str:
        """The reading as one sentence, for a run's own output.

        DERIVED FROM THE SAME FIELDS the decision is, so a run cannot print one state while
        the other happened -- which is the whole requirement here: "the ring did not
        truncate" and "truncation is unruled-out" must never be the same sentence."""
        held = f"progress ring: {self.updates} updates"
        if self.cap is not None:
            return (
                f"{held} against a cap of {self.cap} READ from this session -- under it, so "
                "nothing was evicted and the count above is the run's whole total."
            )
        unread = f"{PROGRESS_CAP_CONFIG} could not be read here ({self.unreadable_because})"
        if self.earliest_batch_id == FIRST_BATCH_ID:
            return (
                f"{held}; {unread}, so no cap was compared against -- but the oldest "
                f"retained update is batch {FIRST_BATCH_ID}, the first this query ran, and "
                "the ring evicts oldest-first, so nothing was evicted and the count above "
                "is the run's whole total."
            )
        oldest = (
            f"the oldest retained update is batch {self.earliest_batch_id}"
            if self.earliest_batch_id is not None
            else "the ring is empty"
        )
        return (
            f"{held}; {unread}, and {oldest} rather than batch {FIRST_BATCH_ID} -- so an "
            "earlier update may have been evicted, TRUNCATION IS UNRULED-OUT, and the count "
            "above is a LOWER BOUND rather than the run's total."
        )


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
    a run states which ones it had.

    `ring` is the third and it qualifies the other two: whether `input_rows` is the run's
    total or a floor under it, and by which argument -- see `RingBufferReading`. It carries
    NO DEFAULT on purpose. There is no honest value for "nobody looked", and a default here
    would be exactly that value, printed in the shape of a measurement."""

    batch_ids: tuple[int, ...]
    input_rows: int
    ring: RingBufferReading


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


def _refuse_options_that_reopen_a_decision(options: Mapping[str, str]) -> None:
    """Refuse a caller option that re-spells one this function decides. See
    `DECIDED_READER_OPTIONS` for why `startingOffsets` is the one that matters.

    CASE-INSENSITIVELY, BECAUSE THE READER IT PROTECTS IS. Measured on a `readStream` in
    this project's own session: `.option("header", "true").option("HEADER", "false")`
    returns the header row AS DATA, and the reverse order does not -- so the streaming
    reader folds an option's name and the LAST spelling wins. The caller's options are
    applied after this function's own (see `payment_stream`), which makes the caller's
    spelling the later one. An exact-match refusal would therefore have refused
    `startingOffsets` and accepted `STARTINGOFFSETS`, which reaches the same source
    option, overrides the same decision, and is the same hole."""
    collisions = sorted(name for name in options if name.lower() in _DECIDED_FOLDED)
    if not collisions:
        return
    raise ValueError(
        f"broker_options carries {collisions}, which this function decides for itself. "
        "This door exists to carry a broker's SECURITY settings; an option that re-spells "
        f"{sorted(DECIDED_READER_OPTIONS)} would move a decision out of shipped code -- "
        f"and for startingOffsets that decision is the refusal of {LATEST!r}, which reads "
        "zero records over an already-published topic and makes every count downstream "
        "true and worthless."
    )


def payment_stream(
    spark: SparkSession,
    *,
    topic: str,
    bootstrap: str,
    starting_offsets: str = EARLIEST,
    max_offsets_per_trigger: int | None = None,
    broker_options: Mapping[str, str] | None = None,
) -> DataFrame:
    """The streaming read of `topic`: Kafka coordinates, raw value, and parsed contract
    columns.

    `max_offsets_per_trigger` is what splits ONE `availableNow` run into several
    micro-batches -- deterministically, because the offsets are fixed before the run
    starts. T3 needs more than one batch to have a batch to fault, and this is the only
    knob that produces them without introducing a clock.

    `broker_options` is how a SASL_SSL broker's `kafka.*` settings reach the reader without
    this module learning a second protocol -- `opl.streaming.managed_broker` builds them.
    An empty mapping is the local PLAINTEXT container and is the default."""
    _refuse_offsets_that_can_read_nothing(starting_offsets)
    options = dict(broker_options or {})
    _refuse_options_that_reopen_a_decision(options)
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
    # One `.option` per entry rather than `.options(**broker_options)`: these keys carry
    # dots, and `**` unpacking of non-identifier keys is a CPython permission rather than a
    # documented part of PySpark's signature. The loop needs no such permission, and it
    # also keeps `DECIDED_READER_OPTIONS`' derivation above honest -- the literals a
    # reviewer can see are the ones this function decides.
    for key, value in options.items():
        reader = reader.option(key, value)
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


def _first_line(exc: BaseException, limit: int = 200) -> str:
    """An exception reduced to something a run's output line can carry.

    A Spark Connect error arrives with the JVM stack trace attached -- T8's was ~90 frames
    -- and a reading whose reason is a stack trace is a reading nobody reads. The first
    line is the SQLSTATE-carrying message, which is the part that says why."""
    lines = str(exc).splitlines() or [""]
    text = " ".join(lines[0].split())
    return text if len(text) <= limit else f"{text[: limit - 3]}..."


def _cap_or_the_reason_it_could_not_be_read(
    spark: SparkSession,
) -> tuple[int | None, str | None]:
    """The ring buffer's cap, or the refusal that came back instead of it. Never a
    stand-in for it: exactly one of the two is not None.

    ASKED WITHOUT A DEFAULT, which is a change of question rather than of style. Measured
    on a real session (`tests/test_streaming_delta_ingest.py`): a key Spark does not know
    returns the DEFAULT when one is passed and raises `SQL_CONF_NOT_FOUND` when one is
    not -- so the defaulted call cannot tell "this session says 100" from "this session
    has never heard of that key". On serverless the default buys nothing at all; see
    `RingBufferReading` for the frame that proves it.

    THE REFUSAL IS CARRIED, NOT SWALLOWED. Its message is what `describe()` prints, and it
    is the only thing that lets a reader tell an unruled-out run from a checked one."""
    try:
        return int(spark.conf.get(PROGRESS_CAP_CONFIG)), None
    except Exception as refusal:  # noqa: BLE001 -- reported by `describe()`, not discarded
        return None, f"{type(refusal).__name__}: {_first_line(refusal)}"


def _progress_of(query, spark: SparkSession) -> IngestedStream:
    """What one run processed: the batch ids that consumed something, the total they
    consumed, and WHETHER THAT TOTAL IS THE WHOLE OF IT.

    THE TRUNCATION IS REFUSED RATHER THAN SUMMED OVER, WHERE THE CAP CAN BE READ.
    `recentProgress` is a RING BUFFER capped by `PROGRESS_CAP_CONFIG` (100 by default), so
    a query with more batches than that silently reports the sum of its LAST hundred -- a
    number smaller than the truth, in the same shape as every other short read this phase
    refuses. Nothing here needs a long run; a run that becomes one gets a message instead
    of an undercount.

    THAT REFUSAL IS AT `>= cap` AND STAYS THERE, including in the one case `FIRST_BATCH_ID`
    would clear (a full ring that has not evicted anything yet). It has never fired in a
    shipped run -- the runs here measure 1 and 3 consuming batches against a cap of 100 --
    and loosening a shipped refusal that nothing forced is not this fix's business.

    WHERE THE CAP CANNOT BE READ there is nothing to compare, and no comparison is
    invented. The reading says which argument it had; `RingBufferReading` is the argument."""
    progresses = query.recentProgress
    cap, unreadable_because = _cap_or_the_reason_it_could_not_be_read(spark)
    if cap is not None and len(progresses) >= cap:
        raise RuntimeError(
            f"the query reported {len(progresses)} progress updates against a "
            f"`numRecentProgressUpdates` cap of {cap}. The buffer is a ring, so the "
            "earliest batches have been dropped and any total taken over it is an "
            "undercount. Raise the cap or shorten the run."
        )
    consuming = [p for p in progresses if p["numInputRows"] > 0]
    return IngestedStream(
        batch_ids=tuple(int(p["batchId"]) for p in consuming),
        input_rows=sum(int(p["numInputRows"]) for p in consuming),
        ring=RingBufferReading(
            updates=len(progresses),
            earliest_batch_id=int(progresses[0]["batchId"]) if progresses else None,
            cap=cap,
            unreadable_because=unreadable_because,
        ),
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


def _start_at_the_one_destination(writer, path: str | None, table: str | None):
    """Start the query at exactly one of a path or a catalog name, or refuse.

    THE TWO ARE NOT INTERCHANGEABLE ON THE DEPLOY TARGET, which is why this is a refusal
    and not a default. Unity Catalog does not let a Delta table be created inside a Volume,
    so a serverless run has a NAME and no writable path; a local test has a tmp dir and no
    catalog. Both arguments would leave it ambiguous which one the run's counts describe,
    and passing neither is no destination at all."""
    if (path is None) == (table is None):
        raise ValueError(
            f"pass exactly one of `path` ({path!r}) or `table` ({table!r}). A path is a "
            "Delta location -- what every local run of this uses -- and a table is a Unity "
            "Catalog name, which is what a serverless run has, because UC refuses a Delta "
            "table inside a Volume. Two destinations, or none, is a run whose row counts "
            "describe nothing in particular."
        )
    return writer.start(path) if table is None else writer.toTable(table)


def write_payment_stream(
    frame: DataFrame,
    *,
    path: str | None = None,
    table: str | None = None,
    checkpoint: str,
    topic: str,
    minimum_rows: int = 1,
) -> IngestedStream:
    """Run `frame` to completion into the Delta table at `path` or named `table`. THE T2
    SINK.

    `format("delta")` DELIBERATELY, AND THAT IS WHY IT PROVES NOTHING ABOUT EXACTLY-ONCE.
    This sink is exactly-once BY CONSTRUCTION -- it commits the batch id into the Delta log
    and ignores a replay of it -- so an experiment run against this function reports
    success whatever happens, including when nothing ran. It is the right sink for an
    ingest and the wrong one for a proof; the proof lives in `opl.streaming.exactly_once`,
    where the batch is written by user code and a replay can double-write.

    `availableNow` rather than `once` or a continuous trigger: it drains everything the
    broker holds and STOPS, which is what makes a run a measurement rather than a
    subscription -- and, unlike `once`, it still honours `maxOffsetsPerTrigger`, so the
    multi-batch split T3 depends on survives.

    EXACTLY ONE OF `path` AND `table` -- see `_start_at_the_one_destination`. Everything
    else about the query is identical between them, deliberately: the checkpoint, the
    trigger, the progress reading and the floor are what make a run a measurement, and none
    of them is a function of where the rows land."""
    query = _start_at_the_one_destination(
        frame.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True),
        path,
        table,
    )
    query.awaitTermination()
    ingested = _progress_of(query, frame.sparkSession)
    _refuse_a_run_that_processed_nothing(ingested.input_rows, minimum_rows, topic)
    return ingested
