# tests/test_streaming_exactly_once.py
"""The half of T3 that needs no broker: the fault's determinism, the floors, and the
duplicate measure itself.

WHY THE MEASURE IS TESTED HERE AND NOT ONLY THROUGH THE PROOF. `processing_duplicates` is
the instrument the phase's headline number is read off. Exercised only through
`tests/integration/test_exactly_once_proof.py`, it would be checked exclusively against
tables it produced itself -- and a measure that reports 0 for every input passes the
guarded arm, while one that reports `row_count` for every input passes the naive arm. Here
it is handed tables whose duplicate counts are stated by construction, including the two
tables that must be REFUSED rather than counted: an empty one, and one whose coordinates
did not land.

The full experiment -- both arms, the fault, the replay -- needs a broker and Spark's
streaming machinery and lives in the integration file.
"""
from __future__ import annotations

import pytest

from opl.streaming.exactly_once import (
    FAULT_MARKER,
    GUARDED,
    NAIVE,
    ArmRun,
    BatchFault,
    BatchInvocation,
    BatchLedger,
    InjectedBatchFault,
    _refuse_a_failure_that_is_not_the_injected_one,
    _refuse_a_fault_that_never_struck,
    _refuse_a_replay_that_did_not_repeat_the_faulted_batch,
    _refuse_a_run_that_processed_nothing,
    _refuse_a_run_that_struck_without_an_armed_fault,
    processing_duplicates,
    run_arm,
)
from opl.streaming.ingest import PROCESSING_IDENTITY

_COORDINATES = tuple((0, offset) for offset in range(10))


def _invocation(batch_id: int, coordinates=_COORDINATES) -> BatchInvocation:
    return BatchInvocation(batch_id=batch_id, coordinates=coordinates)


def _run(arm: str, *invocations: BatchInvocation, struck: bool) -> ArmRun:
    return ArmRun(arm=arm, invocations=invocations, struck=struck)


def _frame(spark, coordinates):
    """A stand-in sink table: the two coordinate columns and nothing else, since that pair
    is the whole of what the measure reads."""
    schema = f"{PROCESSING_IDENTITY[0]} int, {PROCESSING_IDENTITY[1]} long"
    return spark.createDataFrame(list(coordinates), schema)


def test_a_fault_strikes_its_own_batch_and_no_other():
    """DETERMINISTIC BY CONSTRUCTION: an integer comparison, no clock and no race. The
    marker in the message is what later tells this failure apart from a broker that went
    away mid-run."""
    fault = BatchFault(batch_id=1)
    assert fault.strike(0) is None
    assert fault.strike(2) is None
    with pytest.raises(InjectedBatchFault, match=FAULT_MARKER):
        fault.strike(1)


def test_a_failure_without_the_marker_is_re_raised_rather_than_read_as_the_fault():
    """§0.3's species, refused. Without the marker, a broker that went away, a full disk
    and the injected fault all present as "the query failed" -- and an arm's numbers taken
    over the first two would be evidence of nothing."""
    _refuse_a_failure_that_is_not_the_injected_one(RuntimeError(f"x {FAULT_MARKER} y"), NAIVE)
    with pytest.raises(RuntimeError, match="something other than the injected fault"):
        _refuse_a_failure_that_is_not_the_injected_one(
            RuntimeError("Failed to construct kafka consumer"), NAIVE
        )


def test_an_armed_fault_that_never_fired_is_refused():
    """THE ACCEPTANCE CUTTING THE OTHER WAY. A run that completed with its fault unfired
    means the batch id was never reached -- one batch instead of three, say -- and both
    arms would then land the same rows and 'no duplicates' would be true of a run with no
    fault in it."""
    _refuse_a_fault_that_never_struck(None, False, NAIVE)
    _refuse_a_fault_that_never_struck(BatchFault(batch_id=1), True, NAIVE)
    with pytest.raises(RuntimeError, match="completed without it firing"):
        _refuse_a_fault_that_never_struck(BatchFault(batch_id=1), False, NAIVE)


def test_a_restart_that_died_is_not_recorded_as_a_clean_replay():
    """THE ACCEPTANCE'S OTHER HALF, AND THE ONE `prove_arm`'s RESTART NEEDS.

    `_refuse_a_fault_that_never_struck` returns immediately when no fault was armed, so it
    is blind to the opposite outcome: a run that armed NOTHING and still ended on
    `FAULT_MARKER`. That is precisely the restart's shape -- `prove_arm` calls it with
    `fault=None`, over the checkpoint whose uncommitted batch it is re-running -- and a
    restart that DIED inside that batch would otherwise be reported as a clean replay,
    because its ledger still holds one invocation of the killed batch over the killed
    batch's own offsets and `_refuse_a_replay_that_did_not_repeat_the_faulted_batch` asks
    for nothing more.

    Three arms, so the refusal is shown to be about the PAIR: no fault and no strike is the
    ordinary restart, an armed fault that struck is the ordinary faulted run, and only the
    combination that cannot happen on purpose is refused."""
    _refuse_a_run_that_struck_without_an_armed_fault(None, False, NAIVE)
    _refuse_a_run_that_struck_without_an_armed_fault(BatchFault(batch_id=1), True, NAIVE)
    with pytest.raises(RuntimeError, match="armed NO fault and still ended on"):
        _refuse_a_run_that_struck_without_an_armed_fault(None, True, NAIVE)

    # "nothing else would notice", RUN rather than reasoned: the ledger a restart that died
    # inside the replayed batch leaves is a one-invocation PREFIX, and the replay refusal
    # accepts it -- it asks only that the killed batch appear once with the right offsets.
    faulted = _run(GUARDED, _invocation(0), _invocation(1), struck=True)
    _refuse_a_replay_that_did_not_repeat_the_faulted_batch(
        faulted, _run(GUARDED, _invocation(1), struck=True), GUARDED
    )


def test_a_run_that_processed_nothing_is_refused():
    """§5.1's floor, in shipped code. A checkpoint that had already consumed the topic
    produces a run of zero rows, over which every duplicate count is 0 and true."""
    ledger = BatchLedger()
    with pytest.raises(RuntimeError, match="processed 0 rows"):
        _refuse_a_run_that_processed_nothing(ledger, 1, NAIVE)
    ledger.record(0, _COORDINATES)
    _refuse_a_run_that_processed_nothing(ledger, 10, NAIVE)
    with pytest.raises(RuntimeError, match="processed 10 rows"):
        _refuse_a_run_that_processed_nothing(ledger, 11, NAIVE)


def test_a_restart_that_did_not_repeat_the_faulted_batch_is_refused():
    """THE "IT REPLAYED" CLAIM, WHICH NO ROW COUNT CAN MAKE.

    In the guarded arm a replay that was correctly skipped and a replay that never happened
    land the identical rows, so the final total cannot separate them. Three cases: the
    restart that never saw the batch again, the restart that saw it over DIFFERENT offsets
    (a resume past the uncommitted batch), and the one that did replay it."""
    faulted = _run(GUARDED, _invocation(0), _invocation(1), struck=True)
    _refuse_a_replay_that_did_not_repeat_the_faulted_batch(
        faulted, _run(GUARDED, _invocation(1), _invocation(2), struck=False), GUARDED
    )
    with pytest.raises(RuntimeError, match="appears 0 times"):
        _refuse_a_replay_that_did_not_repeat_the_faulted_batch(
            faulted, _run(GUARDED, _invocation(2), struck=False), GUARDED
        )
    moved = _invocation(1, tuple((0, offset) for offset in range(10, 20)))
    with pytest.raises(RuntimeError, match="A replay over different offsets"):
        _refuse_a_replay_that_did_not_repeat_the_faulted_batch(
            faulted, _run(GUARDED, moved, struck=False), GUARDED
        )


def test_a_completed_run_has_no_killed_batch_to_describe():
    """`killed_batch` is "the last invocation" only because the body raises immediately
    after recording one. On a run that did not fail, that reasoning is absent and the last
    invocation is just the last batch -- so it refuses rather than answering plausibly."""
    completed = _run(NAIVE, _invocation(0), _invocation(1), struck=False)
    with pytest.raises(RuntimeError, match="no batch was killed"):
        _ = completed.killed_batch
    assert _run(NAIVE, _invocation(0), struck=True).killed_batch.batch_id == 0


def test_an_unknown_arm_is_refused_before_a_query_is_started():
    """Two arms and no default. `None` as the frame is what says the refusal happens before
    the writer is built: a later refusal would raise AttributeError here instead."""
    with pytest.raises(ValueError, match="is not one of"):
        run_arm(None, arm="guarded-ish", path="p", checkpoint="c", app_id="a")


def test_the_ledger_reports_the_batches_and_the_rows_it_was_handed():
    ledger = BatchLedger()
    ledger.record(0, _COORDINATES)
    ledger.record(1, _COORDINATES[:4])
    assert ledger.batch_ids() == (0, 1)
    assert ledger.row_count() == 14
    assert ledger.of_batch(1)[0].coordinates == _COORDINATES[:4]
    assert ledger.of_batch(7) == ()


def test_a_repeated_delivery_coordinate_is_counted_as_a_duplicate(spark):
    """THE MEASURE, over a table whose answer is stated by construction rather than read
    back from a run that produced it."""
    once = processing_duplicates(_frame(spark, [(0, 1), (0, 2), (1, 1)]))
    assert (once.row_count, once.distinct_records, once.duplicate_rows) == (3, 3, 0)

    twice = processing_duplicates(_frame(spark, [(0, 1), (0, 2), (0, 1), (0, 2)]))
    assert (twice.row_count, twice.distinct_records, twice.duplicate_rows) == (4, 2, 2)


def test_the_same_offset_in_a_different_partition_is_not_a_duplicate(spark):
    """The PAIR is the delivery. Offsets restart per partition, so a measure over the
    offset alone would report a three-partition topic as duplicated end to end."""
    counted = processing_duplicates(_frame(spark, [(0, 0), (1, 0), (2, 0)]))
    assert (counted.row_count, counted.distinct_records, counted.duplicate_rows) == (3, 3, 0)


def test_an_empty_table_is_refused_rather_than_reported_as_duplicate_free(spark):
    """§5.1 in one line: the answer is 0, it is true, and it is what a run that never
    happened also reports."""
    with pytest.raises(RuntimeError, match="empty table"):
        processing_duplicates(_frame(spark, []))


def test_a_null_coordinate_is_refused_rather_than_counted(spark):
    """THE 8,761 ROWS, NOT LOST AGAIN. A coordinate that failed to land makes the
    arithmetic a measurement of missing metadata: here the two NULL-offset rows would
    collapse into one distinct value and report a duplicate that is really a broken
    projection."""
    with pytest.raises(RuntimeError, match=r"landed rows carry a NULL"):
        processing_duplicates(_frame(spark, [(0, 1), (0, None), (0, None)]))
