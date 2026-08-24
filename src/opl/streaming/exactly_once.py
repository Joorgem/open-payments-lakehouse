# src/opl/streaming/exactly_once.py
"""THE EXACTLY-ONCE PROOF, AND ITS NEGATIVE CONTROL. Two arms, one fault, the same offsets.

--- WHY THE PROOF IS NOT RUN AGAINST `writeStream.format("delta")` ------------------------

That sink -- `opl.streaming.ingest.write_payment_stream` -- is exactly-once BY
CONSTRUCTION. It records the batch id in the Delta log itself and ignores a replay of it,
so an experiment on it reports success under every outcome including the ones where the
fault never landed, the stream read nothing, or the restart did not happen. That is F4's
species wearing a streaming costume: a check whose output cannot distinguish "passed" from
"never ran".

THE HAZARD LIVES IN `foreachBatch`, where the batch is written by USER CODE and Spark's own
idempotency does not reach. So both arms here write through `foreachBatch` and differ in
EXACTLY ONE THING -- whether the append carries Delta's `txnAppId` / `txnVersion`:

    NAIVE    plain append, no idempotency key      duplicates MUST appear
    GUARDED  append with txnAppId / txnVersion     no duplicates

If NAIVE does not duplicate, exactly-once has NOT been demonstrated -- what has been
demonstrated is that the fault never reached the window. `_refuse_a_fault_that_never_struck`,
`_refuse_a_run_that_struck_without_an_armed_fault` and
`_refuse_a_replay_that_did_not_repeat_the_faulted_batch` below are that acceptance written
into the shipped code, so the negative control cannot be quietly lost: the first says the
fault fired on the run that armed it, the second says the RESTART did not end on the marker
too (any OTHER failure is already re-raised by `_refuse_a_failure_that_is_not_the_injected_one`,
so between them a restart cannot die quietly), and the third says the restart re-ran the
killed batch over the killed batch's own offsets.

--- THE FAULT, AND WHY IT IS DETERMINISTIC RATHER THAN A RACE -----------------------------

`BatchFault(batch_id=N)` raises when the `foreachBatch` body has just returned from its
DATA WRITE for batch N. That is the only window in which a replay can double-write: the
rows are in the Delta table and the streaming checkpoint has not committed the batch, so
Spark's offset log holds batch N with no matching commit and a restart re-runs it over the
SAME offsets.

Nothing about that mechanism reads a clock, a thread, a signal or a process state. It is an
integer comparison inside the driver's own callback, on a batch id that
`maxOffsetsPerTrigger` makes a fixed function of the topic's contents:
`opl.streaming.ingest.payment_stream(..., max_offsets_per_trigger=N)` over a topic holding
M records splits into ceil(M / N) batches with fixed boundaries, because `availableNow`
resolves the end offset before the first batch runs. The alternative shapes -- killing the
JVM, or racing a `stop()` against a commit -- would put the outcome at the mercy of
whichever ran first, and a proof whose fault lands "usually" is not a proof.

THE ARM IS FAULTED ON THE MIDDLE BATCH, deliberately. Batch 0 would leave nothing already
committed to duplicate against, and the LAST batch would leave the restart with nothing to
do except the replay -- so a restart that did nothing at all would be indistinguishable
from a restart that replayed. Faulting in the middle means the restart must both repeat the
faulted batch and finish the stream, and the two are separately visible in the ledger.

--- WHAT A DUPLICATE IS COUNTED OVER, AND WHY IT IS NOT `transaction_id` -------------------

`(kafka_partition, kafka_offset)` -- `opl.streaming.ingest.PROCESSING_IDENTITY`. Within a
topic that pair names ONE delivery of one record, so a pair appearing twice in the sink is
the pipeline having processed one message twice, which is exactly what exactly-once forbids.

`transaction_id` would measure something else and would report this corpus as broken in
both arms. F1b injects REDELIVERIES on purpose -- the producer sending one transaction
twice, byte for byte -- and F5's plan is explicit that a redelivery is a DATA property and
not an exactly-once failure. The guarded arm's table is therefore expected to hold
duplicate transaction ids and ZERO duplicate coordinates at the same time, and
`tests/integration/test_exactly_once_proof.py` asserts precisely that pair.

AND THE COUNT IS `select(...).distinct().count()`, NOT `countDistinct(...)`. This
repository lost 8,761 rows to that difference once: SQL's `COUNT(DISTINCT ...)` drops rows
where the expression is NULL, so a coordinate that failed to land would SHRINK the distinct
count, INFLATE `row_count - distinct` and report duplicates that are really missing
metadata -- or, in the other direction, hide real ones. `.distinct()` over a projection
counts NULL as a value, and `_refuse_a_null_processing_identity` refuses the table outright
before either arithmetic runs.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from opl.streaming.ingest import PROCESSING_IDENTITY

NAIVE = "naive"
GUARDED = "guarded"
ARMS = (NAIVE, GUARDED)

# The two Delta writer options that make a `foreachBatch` append idempotent. Named because
# they are the ONLY difference between the two arms, and a proof whose single variable is
# spelled as two string literals at the use site is a proof whose variable can be typo'd
# into a third arm that resembles neither.
TXN_APP_ID_OPTION = "txnAppId"
TXN_VERSION_OPTION = "txnVersion"

# WHAT MAKES AN INJECTED FAILURE DISTINGUISHABLE FROM A REAL ONE. The exception crosses the
# py4j boundary and comes back wrapped in a `StreamingQueryException` whose type is gone, so
# the marker travels in the MESSAGE. Without it, a broker that went away mid-run, a disk that
# filled, and the fault this module injects all present as "the query failed" -- §0.3's
# species, one string across the worlds it was meant to separate.
FAULT_MARKER = "opl-injected-batch-fault"


class InjectedBatchFault(RuntimeError):
    """The fault this module injects on purpose. Never raised by a real failure."""


@dataclass(frozen=True, kw_only=True)
class BatchFault:
    """Raise after batch `batch_id` has written its data and before its offsets commit."""

    batch_id: int

    def strike(self, batch_id: int) -> None:
        if batch_id != self.batch_id:
            return
        raise InjectedBatchFault(
            f"{FAULT_MARKER}: batch {batch_id} wrote its rows and is now failing before its "
            "offsets commit. This is the only window in which a replay can double-write."
        )


@dataclass(frozen=True, kw_only=True)
class BatchInvocation:
    """One call of the `foreachBatch` body: which batch, how many rows, WHICH OFFSETS.

    `coordinates` is what makes "the restart replayed the same offsets" a MEASUREMENT
    rather than a claim. A restart that read nothing produces no invocation at all; one
    that resumed further along produces an invocation whose coordinates differ. Both are
    visible here and neither is visible in a row count."""

    batch_id: int
    coordinates: tuple[tuple[int, int], ...]

    @property
    def row_count(self) -> int:
        return len(self.coordinates)


class BatchLedger:
    """What the `foreachBatch` body was handed, in order. Mutable, like
    `producer._DeliveryLedger`, and for the same reason: Spark reports batches by calling
    back, so accumulating is what the contract asks for."""

    def __init__(self) -> None:
        self.invocations: list[BatchInvocation] = []

    def record(self, batch_id: int, coordinates: tuple[tuple[int, int], ...]) -> None:
        self.invocations.append(
            BatchInvocation(batch_id=batch_id, coordinates=coordinates)
        )

    def batch_ids(self) -> tuple[int, ...]:
        return tuple(invocation.batch_id for invocation in self.invocations)

    def row_count(self) -> int:
        return sum(invocation.row_count for invocation in self.invocations)

    def of_batch(self, batch_id: int) -> tuple[BatchInvocation, ...]:
        return tuple(i for i in self.invocations if i.batch_id == batch_id)


@dataclass(frozen=True, kw_only=True)
class ArmRun:
    """One `availableNow` run of one arm: what the body saw, and whether the fault fired."""

    arm: str
    invocations: tuple[BatchInvocation, ...]
    struck: bool

    def of_batch(self, batch_id: int) -> tuple[BatchInvocation, ...]:
        return tuple(i for i in self.invocations if i.batch_id == batch_id)

    def batch_ids(self) -> tuple[int, ...]:
        return tuple(invocation.batch_id for invocation in self.invocations)

    def row_count(self) -> int:
        return sum(invocation.row_count for invocation in self.invocations)

    @property
    def killed_batch(self) -> BatchInvocation:
        """The invocation the fault struck: the body raises immediately after recording
        and writing it, so it is the LAST one this run recorded -- and asking for it on a
        run that did not fail is a mistake this refuses rather than answers."""
        if not self.struck:
            raise RuntimeError(
                f"the {self.arm} arm's run completed without the injected fault, so no "
                "batch was killed and there is no replay to describe."
            )
        return self.invocations[-1]


@dataclass(frozen=True, kw_only=True)
class ProcessingDuplicates:
    """How many rows the sink holds, how many DELIVERIES they are, and the difference.

    All three, not just the difference: `duplicate_rows == 0` over an empty table is true,
    and the phase's §5.1 hazard is exactly that sentence being reported as a pass."""

    row_count: int
    distinct_records: int
    duplicate_rows: int


def _coordinates_of(batch_df: DataFrame) -> tuple[tuple[int, int], ...]:
    """The (partition, offset) pairs in one micro-batch, sorted.

    COLLECTED, which reads the batch a second time -- the batch is re-read from Kafka over
    fixed offsets, so it is deterministic, and at this phase's record counts it is cheap.
    It is also why this runner is separate from `ingest.write_payment_stream`, which does
    not collect: the ledger is the PROOF's instrument and has no business on an ingest
    path."""
    rows = batch_df.select(*PROCESSING_IDENTITY).collect()
    return tuple(sorted((int(row[0]), int(row[1])) for row in rows))


def _append(batch_df: DataFrame, *, path: str, app_id: str | None, batch_id: int) -> None:
    """The sink write, and the single `if` that separates the two arms.

    With `app_id`, Delta records `(app_id, batch_id)` in the transaction log and treats a
    later append carrying a version it has already seen as a NO-OP. Without it, an append
    is an append and a replayed batch lands twice."""
    writer = batch_df.write.format("delta").mode("append")
    if app_id is not None:
        writer = writer.option(TXN_APP_ID_OPTION, app_id).option(TXN_VERSION_OPTION, batch_id)
    writer.save(path)


def _batch_body(
    *, path: str, ledger: BatchLedger, app_id: str | None, fault: BatchFault | None
) -> Callable[[DataFrame, int], None]:
    """The `foreachBatch` callable. Order is the whole point: LEDGER, WRITE, THEN FAULT."""

    def _run(batch_df: DataFrame, batch_id: int) -> None:
        ledger.record(batch_id, _coordinates_of(batch_df))
        _append(batch_df, path=path, app_id=app_id, batch_id=batch_id)
        if fault is not None:
            fault.strike(batch_id)

    return _run


def _refuse_an_unknown_arm(arm: str) -> None:
    if arm in ARMS:
        return
    raise ValueError(f"{arm!r} is not one of {ARMS}. The proof has two arms and no default.")


def _refuse_a_failure_that_is_not_the_injected_one(failure: BaseException, arm: str) -> None:
    """Re-raise anything that is not this module's own fault.

    A broker that went away, a full disk and a Delta conflict all terminate a query, and a
    proof that swallowed them would report the arm's numbers over a run that failed for a
    reason nobody looked at."""
    if FAULT_MARKER in str(failure):
        return
    raise RuntimeError(
        f"the {arm} arm's query failed with something other than the injected fault "
        f"({FAULT_MARKER!r} does not appear in the message). The run is not evidence of "
        "anything about exactly-once."
    ) from failure


def _refuse_a_fault_that_never_struck(fault: BatchFault | None, struck: bool, arm: str) -> None:
    """THE ACCEPTANCE, CUTTING THE OTHER WAY. An armed fault that did not fire means the
    batch id was never reached -- too few batches, or a rate limit that produced one -- and
    every downstream count would then describe a run with no fault in it."""
    if fault is None or struck:
        return
    raise RuntimeError(
        f"the {arm} arm armed a fault on batch {fault.batch_id} and the run completed "
        "without it firing. The stream did not produce that batch, so no replay was staged "
        "and neither arm's count says anything about exactly-once."
    )


def _refuse_a_run_that_struck_without_an_armed_fault(
    fault: BatchFault | None, struck: bool, arm: str
) -> None:
    """THE SAME ACCEPTANCE, ON THE OTHER RUN. `_await` reports `struck` for ANY failure
    carrying `FAULT_MARKER`, and `_refuse_a_fault_that_never_struck` returns immediately
    when no fault was armed -- so a run with `fault=None` that DIED on a marker-bearing
    failure would be recorded as a clean completion.

    `prove_arm`'s RESTART is the run whose shape this describes: it is the one called with
    `fault=None`, it re-runs the batch the first run was killed inside, and a restart that
    died in it is not a replay. Its ledger would then be a prefix of what it was handed,
    and `_refuse_a_replay_that_did_not_repeat_the_faulted_batch` -- which only asks that the
    killed batch appear once with the right offsets -- can be satisfied by that prefix.

    AND IT IS INERT INSURANCE RATHER THAN A LIVE ACCEPTANCE, said here because the paragraph
    above otherwise reads as a thing that happens. `_batch_body` calls `fault.strike` only
    under `if fault is not None`, and `BatchFault.strike` is the only code in this repository
    that raises `FAULT_MARKER` from INSIDE the `foreachBatch` body -- the two refusals that
    also spell the marker raise their messages at the driver, after `awaitTermination` has
    already answered, so neither can come back as a query failure. On a `fault=None` run
    `struck` therefore becomes True only if some unrelated failure's message happens to
    contain that literal, and nothing emits it. What stops a dead restart today is
    `_refuse_a_failure_that_is_not_the_injected_one` re-raising every other failure; this
    refusal is what keeps the pair complete if a second emitter of the marker is ever
    added."""
    if fault is not None or not struck:
        return
    raise RuntimeError(
        f"the {arm} arm's run armed NO fault and still ended on {FAULT_MARKER!r}. The run "
        "did not complete, so the batches it recorded are a PREFIX of what it was handed, "
        "and a replay read off a run that died says nothing about exactly-once."
    )


def _refuse_a_run_that_processed_nothing(ledger: BatchLedger, minimum_rows: int, arm: str) -> None:
    """The non-zero floor, in shipped code. §5.1's hazard, refused rather than asserted."""
    if ledger.row_count() >= minimum_rows:
        return
    raise RuntimeError(
        f"the {arm} arm's run processed {ledger.row_count()} rows across batches "
        f"{ledger.batch_ids()} and at least {minimum_rows} were required. A checkpoint that "
        "had already consumed the topic produces exactly this, and every duplicate count "
        "taken afterwards would have been 0 and meaningless."
    )


def run_arm(
    frame: DataFrame,
    *,
    arm: str,
    path: str,
    checkpoint: str,
    app_id: str,
    fault: BatchFault | None = None,
    minimum_rows: int = 1,
) -> ArmRun:
    """Run `frame` into `path` through `foreachBatch`, optionally failing on one batch.

    `app_id` is REQUIRED for both arms and reaches the writer only for GUARDED, so the
    single variable under test is visible here as one conditional rather than as two
    call sites that happen to differ."""
    _refuse_an_unknown_arm(arm)
    ledger = BatchLedger()
    query = (
        frame.writeStream.foreachBatch(
            _batch_body(
                path=path,
                ledger=ledger,
                app_id=app_id if arm == GUARDED else None,
                fault=fault,
            )
        )
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    struck = _await(query, arm)
    _refuse_a_fault_that_never_struck(fault, struck, arm)
    _refuse_a_run_that_struck_without_an_armed_fault(fault, struck, arm)
    _refuse_a_run_that_processed_nothing(ledger, minimum_rows, arm)
    return ArmRun(arm=arm, invocations=tuple(ledger.invocations), struck=struck)


def _await(query, arm: str) -> bool:
    """Wait for the run to end. True if it ended on THIS module's injected fault.

    `Exception` and not `BaseException`: the injected fault comes back wrapped in a
    `StreamingQueryException`, and widening far enough to catch it would also swallow a
    KeyboardInterrupt -- turning a run the operator stopped into a run this module reports
    as faulted on purpose."""
    try:
        query.awaitTermination()
    except Exception as failure:  # noqa: BLE001 -- narrowed by the refusal it delegates to
        _refuse_a_failure_that_is_not_the_injected_one(failure, arm)
        return True
    return False


def _refuse_a_null_processing_identity(frame: DataFrame) -> None:
    """Refuse a sink whose Kafka coordinates did not all land. See the module docstring's
    last section for the 8,761 rows this exists to not lose again."""
    missing = frame.filter(
        F.col(PROCESSING_IDENTITY[0]).isNull() | F.col(PROCESSING_IDENTITY[1]).isNull()
    ).count()
    if missing == 0:
        return
    raise RuntimeError(
        f"{missing} landed rows carry a NULL {PROCESSING_IDENTITY}. That pair is the "
        "processing identity every count below is taken over, so the duplicate arithmetic "
        "would be measuring missing metadata rather than repeated processing."
    )


def processing_duplicates(frame: DataFrame) -> ProcessingDuplicates:
    """How many rows in `frame` are a SECOND processing of a delivery already in it."""
    _refuse_a_null_processing_identity(frame)
    row_count = frame.count()
    if row_count == 0:
        raise RuntimeError(
            "refusing to count duplicates in an empty table: the answer is 0, it is true, "
            "and it is what a run that never happened also reports."
        )
    distinct_records = frame.select(*PROCESSING_IDENTITY).distinct().count()
    return ProcessingDuplicates(
        row_count=row_count,
        distinct_records=distinct_records,
        duplicate_rows=row_count - distinct_records,
    )


@dataclass(frozen=True, kw_only=True)
class ArmEvidence:
    """One arm, both runs, and the count over what it landed. THE PRODUCT."""

    arm: str
    path: str
    faulted: ArmRun
    replayed: ArmRun
    duplicates: ProcessingDuplicates

    @property
    def replayed_batch(self) -> BatchInvocation:
        """The restart's invocation of the batch the fault killed. Exists because
        `prove_arm` refused to return without it."""
        return self.replayed.of_batch(self.faulted.killed_batch.batch_id)[0]


def _refuse_a_replay_that_did_not_repeat_the_faulted_batch(
    faulted: ArmRun, replayed: ArmRun, arm: str
) -> None:
    """THE "IT REPLAYED" CLAIM, MEASURED. The restart must have been handed the faulted
    batch id again, carrying the SAME offsets. A restart that read nothing, or resumed
    past the uncommitted batch, fails here -- and in the guarded arm neither would be
    visible in the row count, because both produce the same final total as a replay that
    was correctly skipped."""
    killed = faulted.killed_batch
    repeats = replayed.of_batch(killed.batch_id)
    if len(repeats) != 1:
        raise RuntimeError(
            f"the {arm} arm's restart ran batches {replayed.batch_ids()} and batch "
            f"{killed.batch_id} -- the one the fault killed -- appears {len(repeats)} "
            "times in them. Without exactly one replay of it, nothing about the landed "
            "rows is a statement about a replay."
        )
    if repeats[0].coordinates != killed.coordinates:
        raise RuntimeError(
            f"the {arm} arm replayed batch {killed.batch_id} over "
            f"{_span(repeats[0].coordinates)} and the run the fault killed had "
            f"{_span(killed.coordinates)}. A replay over different offsets is a different "
            "batch -- a resume past the uncommitted offsets looks exactly like this -- and "
            "the two arms would no longer be one experiment."
        )


def _span(coordinates: tuple[tuple[int, int], ...]) -> str:
    """`n records, (partition, offset) first..last`. Both ends, because a replay that
    resumed further along can carry the SAME COUNT as the batch it was meant to repeat."""
    return f"{len(coordinates)} records {coordinates[0]}..{coordinates[-1]}"


def prove_arm(
    frame: DataFrame,
    *,
    arm: str,
    path: str,
    checkpoint: str,
    app_id: str,
    fault: BatchFault,
    minimum_rows: int = 1,
) -> ArmEvidence:
    """Run one arm twice over one checkpoint -- fault, then restart -- and count.

    The two runs share the checkpoint and therefore the offsets; the fault is armed on the
    first and absent from the second, which is what makes the restart a replay rather than
    a repeat of the failure.

    `minimum_rows` GOVERNS THE FAULTED RUN ONLY, and the restart is floored at 1 rather
    than at a number, because the restart's real floor is the refusal below it: an
    invocation of the killed batch carrying the killed batch's own offsets is a stronger
    statement than any row count, and a restart that read nothing cannot satisfy it.

    THAT THE RESTART COMPLETED is `run_arm`'s business and not this function's -- nothing
    HERE would notice one that died inside the batch it was replaying, because the
    one-invocation prefix such a run leaves still satisfies the refusal below it. The module
    docstring names the two halves that do cover it, and
    `tests/test_streaming_exactly_once.py::test_a_restart_that_died_is_not_recorded_as_a_clean_replay`
    runs that case rather than asserting it in prose."""
    faulted = run_arm(
        frame,
        arm=arm,
        path=path,
        checkpoint=checkpoint,
        app_id=app_id,
        fault=fault,
        minimum_rows=minimum_rows,
    )
    replayed = run_arm(
        frame, arm=arm, path=path, checkpoint=checkpoint, app_id=app_id, minimum_rows=1
    )
    _refuse_a_replay_that_did_not_repeat_the_faulted_batch(faulted, replayed, arm)
    landed = frame.sparkSession.read.format("delta").load(path)
    return ArmEvidence(
        arm=arm,
        path=path,
        faulted=faulted,
        replayed=replayed,
        duplicates=processing_duplicates(landed),
    )
