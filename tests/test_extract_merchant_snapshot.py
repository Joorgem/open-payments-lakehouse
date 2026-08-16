# tests/test_extract_merchant_snapshot.py
"""The three refusals that live in the extractor SCRIPT rather than in the pure layer.

`opl.extraction.postgres_source` owns every statement and every validation of an answer,
and `tests/test_postgres_source.py` proves it with no database. What is left here is what
the script owns: the wait for the mutation's readiness file, and the check that the
snapshot it then takes did not land inside the race that hand-off exists to close.

WHY THAT IS NOT IN THE PURE MODULE. Waiting is a clock and a filesystem, and
`postgres_source` may touch neither -- the AST sweep in the sibling file bans `os`, `open`
and `pathlib` outright. The PARSE is pure and lives there (`readiness_stamps`); the WAIT
and the REFUSAL are here, where the I/O already is.

HERMETIC: a `tmp_path` and a fake connection. Nothing here starts a container.

ITS OWN FILE, on `tests/integration/test_seed_merchant_db.py`'s precedent for reaching into
`scripts/` -- and it is NOT under `tests/integration/`, because none of it needs the stack.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from opl.extraction.postgres_source import MerchantSnapshot, MerchantSnapshotRefused

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import extract_merchant_snapshot as extractor  # noqa: E402

_T1 = "2026-08-16T22:10:46.710500+00:00"
_T2 = "2026-08-16T22:10:46.779989+00:00"


def _observed(watermark: str | None = "2026-08-16 22:10:46.779989+00") -> MerchantSnapshot:
    return MerchantSnapshot(
        instant="2026-08-16T22:10:47.000123Z",
        pg_snapshot="918:918:",
        wal_lsn="0/1A2B3C8",
        rows=(),
        watermark=watermark,
        incremental_keys=(),
        asked_since=None,
    )


class _Comparing:
    """A connection that answers only the one comparison the check below makes."""

    def __init__(self, verdict: bool):
        self.verdict = verdict
        self.asked: list[tuple] = []

    def execute(self, statement, params=None):
        self.asked.append((statement, params))
        return _Row(self.verdict)


class _Row:
    def __init__(self, value):
        self.value = value

    def fetchone(self):
        return (self.value,)

    def fetchall(self):
        return [(self.value,)]


def test_a_readiness_file_that_never_arrives_is_REFUSED_and_not_waited_out(tmp_path):
    """A TIMEOUT IS NOT READINESS, and this is the one failure in the phase that leaves
    every published count correct while silently removing the number nothing authored.

    Task 3 measured it: a snapshot taken before the mutation's t2 records a watermark below
    t1, so the held-open transaction's rows become perfectly visible to
    `WHERE updated_at > watermark`. The row counts, the class counts and the diff would all
    still be right -- only the headline would be gone."""
    with pytest.raises(MerchantSnapshotRefused, match="did not carry both mutation stamps"):
        extractor.wait_for_readiness(tmp_path / "never", timeout=0.2)


def test_a_half_written_readiness_file_is_waited_THROUGH_and_not_read_as_a_signal(tmp_path):
    """The file is written by another process, so a poller can observe it between create
    and close -- half of it is "not yet".

    The wait is proved to have HAPPENED rather than been skipped: the file is truncated
    when the wait starts and is completed by the poll callback, so a reader that accepted
    the partial content would return the wrong stamps rather than time out."""
    ready = tmp_path / "ready.txt"
    ready.write_text("t1=", encoding="utf-8")
    original = extractor.time.sleep

    def complete(_seconds: float) -> None:
        ready.write_text(f"t1={_T1}\nt2={_T2}\n", encoding="utf-8")
        extractor.time.sleep = original

    extractor.time.sleep = complete
    try:
        assert extractor.wait_for_readiness(ready, timeout=5.0) == {"t1": _T1, "t2": _T2}
    finally:
        extractor.time.sleep = original


def test_a_watermark_below_the_mutations_t2_is_refused():
    """THE HAND-OFF CHECKED RATHER THAN TRUSTED. Waiting for the file says the mutation had
    passed its own `t2 > t1` refusal; this says the snapshot that followed actually sees
    t2's write. They are different claims, and only the second one is about this run."""
    conn = _Comparing(verdict=False)

    with pytest.raises(MerchantSnapshotRefused, match="BEFORE the mutation's t2"):
        extractor._refuse_a_watermark_before_t2(conn, _observed(), {"t1": _T1, "t2": _T2})

    assert conn.asked and "::timestamptz >=" in conn.asked[0][0], (
        "the comparison must be made by POSTGRES: `max(updated_at)::text` and "
        "`datetime.isoformat()` are two spellings of one instant"
    )


def test_a_watermark_at_or_after_t2_is_accepted():
    """Guard the guard: the refusal above would also 'pass' if this check rejected
    everything."""
    extractor._refuse_a_watermark_before_t2(
        _Comparing(verdict=True), _observed(), {"t1": _T1, "t2": _T2}
    )


def test_an_empty_table_is_refused_rather_than_compared():
    """A NULL watermark is an empty table, and for THIS source an empty snapshot is exactly
    what a table whose every row was deleted looks like -- the vault would end-date every
    key it holds. It is refused here rather than handed to a comparison that cannot read
    it."""
    with pytest.raises(MerchantSnapshotRefused, match="watermark is NULL"):
        extractor._refuse_a_watermark_before_t2(
            _Comparing(verdict=True), _observed(watermark=None), {"t1": _T1, "t2": _T2}
        )
