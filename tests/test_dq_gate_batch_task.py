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
from opl.bronze.registry import UnknownTable
from opl.contracts.cnpj_schemas import TABLES

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

    def __init__(self, name: str, columns: list[str] | None = None) -> None:
        self.name = name
        self.saved: list[str] = []
        self.write_options: list[tuple[str, str]] = []
        # The task asks the batch frame which columns it has, to report the rules
        # that will not run against it. Schema only -- no scan, hence no conflict
        # with this double's refusal to be counted.
        self.columns = [] if columns is None else columns

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


def _wire(monkeypatch, *, already: int, good_count: int = 6, bad_count: int = 4,
          batch_columns: list[str] | None = None):
    """Stub everything between the task and Spark, recording what it asked for."""
    batch = FakeFrame("batch", batch_columns)
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

    task.main(["estabelecimentos", "999"])

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

    task.main(["estabelecimentos", "999"])

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
        task.main(["estabelecimentos", "999"])

    message = str(excinfo.value)
    assert "8" in message and "4" in message
    assert f"DELETE FROM {_QUARANTINE}" in message
    assert seen.bad.saved == []
    assert seen.published == []  # nothing may be promoted off a corrupted count


def test_a_clean_batch_still_appends_so_the_quarantine_table_exists(monkeypatch):
    """0 rejects and 0 rows already there: the append of an empty frame is what
    creates the table triagers and the future rate gate query."""
    seen = _wire(monkeypatch, already=0, bad_count=0)

    task.main(["estabelecimentos", "999"])

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
    identified a batch.

    The table is valid in this argv, so the batch-id refusal is the only one that
    can fire -- the table is resolved first."""
    seen = _wire(monkeypatch, already=0)

    with pytest.raises(PromoteRefused, match="names no batch") as excinfo:
        task.main(["estabelecimentos"])

    # It says which task stopped: a shared message that still read "refusing to
    # promote" would send the operator to the wrong end of the flow.
    assert "refusing to gate" in str(excinfo.value)
    assert seen.read_tables == []
    assert seen.bad.saved == []
    assert seen.published == []


def test_an_unknown_table_is_refused_before_spark_and_before_the_batch_id(monkeypatch):
    """A mistyped table is refused by the registry, naming the registered ones.

    Before the batch id, deliberately: this argv carries a valid batch id, so the
    only refusal that can fire is the table one. And before Spark -- nothing about
    it needs a session, and an operator reading a Databricks run log should not
    have waited for one to be told what to type instead."""
    seen = _wire(monkeypatch, already=0)

    with pytest.raises(UnknownTable) as excinfo:
        task.main(["estabelecimento", "999"])  # a real typo: singular

    assert "estabelecimentos" in str(excinfo.value)
    assert seen.read_tables == []
    assert seen.bad.saved == []
    assert seen.published == []


def test_the_gate_scopes_the_lookup_to_one_batch_too(monkeypatch):
    """The lookup's own gate is gone, so it now runs through this one.

    This is carry-forward #7 paid as a consequence: the deleted `dq_gate.py`
    evaluated the WHOLE lookup staging table and OVERWROTE the quarantine, so one
    historical bad row wedged every later clean batch and the quarantine held only
    the most recent run's rejects. Same task, different table parameter -- and the
    coordinates it resolves are the lookup's, not estab's."""
    seen = _wire(monkeypatch, already=0, bad_count=2)

    task.main(["lookup", "777"])

    assert seen.read_tables == [("workspace.default.bronze_cnpj_lookup_staging", "777")]
    assert seen.bad.saved == ["workspace.default.bronze_cnpj_lookup_quarantine"]
    # Append, not overwrite: the whole point of inheriting this gate.
    assert seen.bad.write_options == [("format", "delta"), ("mode", "append")]
    assert seen.published == [("bad_row_count", 2)]


def test_it_counts_both_sides_of_the_batch_in_one_pass(monkeypatch):
    """It used to compute the split three times over a batch of up to ~29M rows:
    `bad.write`, `bad.count()`, `good.count()`. There are two actions here -- count
    both sides, write the rejects -- so there must be two, and `FakeFrame.count`
    fails the test if either frame is counted separately."""
    seen = _wire(monkeypatch, already=0)

    task.main(["estabelecimentos", "999"])

    assert seen.tally_calls == 1


# --- the skipped rule must be audible in the run log -------------------------

# The pre-F1.4a `bronze_cnpj_estab_staging` shape, 35 columns for a reason worth
# writing down: the 30 contract columns plus the 5 audit columns
# `add_audit_columns` wrote BEFORE F1.4a added the two snapshot ones. Bronze is
# 37. F1.4b PR B migrated staging to 37 on 2026-08-03, so this is no longer the
# live shape -- it is the shape any batch staged before a derivation exists has,
# and the next derivation added to a contract makes one again.
_PRE_F14A_STAGING_COLUMNS = [
    *TABLES["estabelecimentos"],
    "_ingested_at", "_record_source", "_batch_id", "_source_file", "_rescued_data",
]


def test_a_rule_skipped_for_a_missing_column_is_named_in_the_run_log(monkeypatch, capsys):
    """The gate's half of the fix for the rebuild+repromote silent path.

    `REQUIRES_COLUMN` makes the skip CORRECT -- a frame written before the
    derivation existed is not a defective frame -- but until now it was also
    inaudible, and the two together are how a control disappears instead of
    failing. A gate that quietly stops applying one of its rules and reports the
    batch clean is indistinguishable, in the run log, from a gate that applied it
    and found nothing.

    Deliberately NOT an error: the pre-F1.4a staging table is a legitimate input
    and raising here would make the documented rebuild procedure unrunnable. The
    line is the fix; the skip is not the bug."""
    assert len(_PRE_F14A_STAGING_COLUMNS) == 35, "the pre-F1.4a shape, measured live"
    seen = _wire(monkeypatch, already=0, batch_columns=_PRE_F14A_STAGING_COLUMNS)

    task.main(["estabelecimentos", "999"])

    out = capsys.readouterr().out
    assert "unprovable_snapshot_ref_date" in out
    assert "_snapshot_ref_date" in out
    assert _STAGING in out
    assert "dq_gate_batch:" in out
    # The verdict lines still happen -- the notice is additional, not a diversion.
    assert "dq_gate_batch: batch=999 good=6 bad=4" in out
    assert seen.published == [("bad_row_count", 4)]


def test_a_frame_carrying_every_column_prints_no_notice(monkeypatch, capsys):
    """Silence on the healthy path. A warning on every run is one nobody reads,
    which would leave the rebuild+repromote case exactly as invisible as before."""
    _wire(monkeypatch, already=0,
          batch_columns=[*_PRE_F14A_STAGING_COLUMNS,
                         "_snapshot_month", "_snapshot_ref_date"])

    task.main(["estabelecimentos", "999"])

    out = capsys.readouterr().out
    assert "unprovable_snapshot_ref_date" not in out
    assert "NOT CHECKED" not in out
