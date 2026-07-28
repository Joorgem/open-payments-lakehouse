# tests/test_dq_gate_batch_task.py
"""Unit tests for the `databricks/src/dq_gate_batch.py` job task: that it appends
this batch's rejects to quarantine exactly once, that it publishes
`bad_row_count` whether or not it appended, and that it does not scan the batch
three times to find out.

Loaded by path with the same importlib pattern as `tests/test_promote_batch_task.py`
-- the `databricks/src` scripts are job entry points, not part of the opl wheel.
No JVM and no workspace: the Spark session is a recorder and the DQ transforms are
stubbed, because what is under test here is the task's own decision, not Spark.
The idempotence PRIMITIVE it decides with (`rows_of_batch`) is tested against a
real Delta log in `tests/bronze/test_promote.py`.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from opl.bronze.promote import PromoteRefused

_SCRIPT = Path(__file__).resolve().parents[1] / "databricks" / "src" / "dq_gate_batch.py"
_spec = importlib.util.spec_from_file_location("dq_gate_batch_task", _SCRIPT)
assert _spec is not None and _spec.loader is not None
task = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(task)

_STAGING = "workspace.default.bronze_cnpj_estab_staging"
_QUARANTINE = "workspace.default.bronze_cnpj_estab_quarantine"


class FakeFrame:
    """Records the write it is given and refuses to be counted: a `.count()` on a
    frame the task already tallied is another scan of a batch of up to ~29M rows,
    which is the cost this task used to pay three times over."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.saved: list[str] = []
        self.write_options: list[tuple[str, str]] = []

    @property
    def write(self):
        return self

    def format(self, fmt: str):
        self.write_options.append(("format", fmt))
        return self

    def mode(self, mode: str):
        self.write_options.append(("mode", mode))
        return self

    def saveAsTable(self, table: str) -> None:  # noqa: N802 - Spark's spelling
        self.saved.append(table)

    def count(self) -> int:
        raise AssertionError(f"{self.name}.count() is a second scan of the batch")


def _wire(monkeypatch, *, already: int, good_count: int = 6, bad_count: int = 4):
    """Stub everything between the task and Spark, recording what it asked for."""
    batch = FakeFrame("batch")
    bad = FakeFrame("bad")
    seen = SimpleNamespace(
        batch=batch, bad=bad, read_tables=[], presence_tables=[],
        tally_calls=0, published=[],
    )

    def fake_batch_rows(spark, table, batch_id):
        seen.read_tables.append((table, batch_id))
        return batch

    def fake_tally(evaluated):
        seen.tally_calls += 1
        return good_count, bad_count

    def fake_rows_of_batch(spark, table, batch_id):
        seen.presence_tables.append((table, batch_id))
        return already

    monkeypatch.setattr(task, "SparkSession",
                        SimpleNamespace(builder=SimpleNamespace(getOrCreate=lambda: None)))
    monkeypatch.setattr(task, "batch_rows", fake_batch_rows)
    monkeypatch.setattr(task, "evaluate", lambda df, rules=None: df)
    monkeypatch.setattr(task, "tally", fake_tally)
    monkeypatch.setattr(task, "split", lambda df, rules=None: (FakeFrame("good"), bad))
    monkeypatch.setattr(task, "rows_of_batch", fake_rows_of_batch)
    monkeypatch.setattr(task, "_publish",
                        lambda key, value: seen.published.append((key, value)))
    return seen


def test_it_appends_this_batch_s_rejects_and_publishes_the_count(monkeypatch, capsys):
    seen = _wire(monkeypatch, already=0)

    task.main(["999"])

    assert seen.read_tables == [(_STAGING, "999")]
    assert seen.bad.saved == [_QUARANTINE]
    assert seen.bad.write_options == [("format", "delta"), ("mode", "append")]
    assert seen.published == [("bad_row_count", 4)]
    # The line the F1.3 evidence doc quotes, unchanged.
    assert "dq_gate_batch: batch=999 good=6 bad=4" in capsys.readouterr().out


def test_a_repair_run_does_not_append_the_same_rejects_twice(monkeypatch, capsys):
    """A Repair re-executes this task under the SAME run id -- the mechanism the
    promote was made idempotent for -- so the bare append put the identical reject
    rows in quarantine a second time. A triager then sees 2 rows for 1 damaged
    record and cannot tell duplication from two real defects, and ADR 0006 makes
    this table the measured history a rate-based gate will be built on."""
    seen = _wire(monkeypatch, already=4, bad_count=4)

    task.main(["999"])

    assert seen.bad.saved == []
    assert seen.presence_tables == [(_QUARANTINE, "999")]
    # The condition task downstream reads this value on every run, including this
    # one: skipping the append must not skip publishing.
    assert seen.published == [("bad_row_count", 4)]
    out = capsys.readouterr().out
    assert "append skipped" in out
    assert "dq_gate_batch: batch=999 good=6 bad=4" in out


def test_a_quarantine_holding_a_different_count_is_refused(monkeypatch):
    """Neither absent nor exactly this batch's rejects: 8 rows for a batch with 4
    means the pre-fix double append already happened, or the rules changed. Fail
    closed and name both counts -- silently continuing corrupts the metric, and
    guessing which rows to add cannot work (quarantine rows have no identity)."""
    seen = _wire(monkeypatch, already=8, bad_count=4)

    with pytest.raises(RuntimeError) as excinfo:
        task.main(["999"])

    message = str(excinfo.value)
    assert "8" in message and "4" in message
    assert f"DELETE FROM {_QUARANTINE}" in message
    assert seen.bad.saved == []
    assert seen.published == []  # nothing may be promoted off a corrupted count


def test_a_clean_batch_still_appends_so_the_quarantine_table_exists(monkeypatch):
    """0 rejects and 0 rows already there: the append of an empty frame is what
    creates the table triagers and the future rate gate query."""
    seen = _wire(monkeypatch, already=0, bad_count=0)

    task.main(["999"])

    assert seen.bad.saved == [_QUARANTINE]
    assert seen.published == [("bad_row_count", 0)]


def test_no_batch_id_at_all_is_refused_with_an_operator_message(monkeypatch):
    """`args[0]` on an empty argv is a bare IndexError -- a traceback that names a
    list index, not the missing job parameter. The same accident on the promote task
    (a `spark_python_task` run or a repair with no parameters) is answered by
    `require_batch_id`, which says what to pass and how to find it, so this task
    reuses that guard rather than growing a second spelling of the same refusal
    (`opl.bronze.promote` already owns the batch-scoped primitives this task shares).

    It refuses before the batch is read and before anything is written -- no
    quarantine append, and no `bad_row_count` published, since the condition task
    downstream branches on that value and must not see one from a run that never
    identified a batch."""
    seen = _wire(monkeypatch, already=0)

    with pytest.raises(PromoteRefused, match="names no batch") as excinfo:
        task.main([])

    # It says which task stopped: a shared message that still read "refusing to
    # promote" would send the operator to the wrong end of the flow.
    assert "refusing to gate" in str(excinfo.value)
    assert seen.read_tables == []
    assert seen.bad.saved == []
    assert seen.published == []


def test_it_counts_both_sides_of_the_batch_in_one_pass(monkeypatch):
    """It used to compute the split three times over a batch of up to ~29M rows:
    `bad.write`, `bad.count()`, `good.count()`. There are two actions here -- count
    both sides, write the rejects -- so there must be two, and `FakeFrame.count`
    fails the test if either frame is counted separately."""
    seen = _wire(monkeypatch, already=0)

    task.main(["999"])

    assert seen.tally_calls == 1
