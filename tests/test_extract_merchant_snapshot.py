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

import json
import sys
from pathlib import Path

import pytest

from opl.contracts.merchant import SOURCE_COLUMNS
from opl.extraction.postgres_source import (
    RENDERING_GUCS,
    MerchantSnapshot,
    MerchantSnapshotRefused,
)

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import extract_merchant_snapshot as extractor  # noqa: E402

_T1 = "2026-08-16T22:10:46.710500+00:00"
_T2 = "2026-08-16T22:10:46.779989+00:00"
_INSTANT = "2026-08-16T22:10:47.000123Z"
_WATERMARK = "2026-08-16 22:10:46.779989+00"
_MERCHANT_ID = "00057343-0001-4a2e-8f00-000000000001"

# One row as POSTGRES renders it under the pinned GUCs: eleven strings, in DDL order, with
# `trade_name` NULL. Shaped like the seeder's rather than invented, so the leading zero in
# the CNPJ root and the trailing zero of the `numeric(14,2)` both survive to the bytes.
_ROW = (
    _MERCHANT_ID,
    "00057343000129",
    "COMÉRCIO ATLÂNTICO LTDA",
    None,
    "active",
    "5411",
    "001-0042-00001234",
    "low",
    "12345.60",
    "2019-03-14",
    "2026-07-15 08:31:02.400500+00",
)


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


class _Cursor:
    def __init__(self, rows: list[tuple]):
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def fetchone(self) -> tuple | None:
        return self._rows[0] if self._rows else None


class _Session:
    """The WHOLE session `main()` drives, scripted and recorded: the pin, its read-back,
    the six statements of one snapshot, and the two `::timestamptz >=` comparisons.

    Matched by substring in the order below, which is load-bearing on one pair: the
    incremental statement ends in `COLLATE "C"` too, so `WHERE updated_at >` has to be
    tried before the row read or the incremental query would be answered with the rows."""

    _ANSWERS = (
        ("clock_timestamp", [(_INSTANT, "918:918:", "0/1A2B3C8")]),
        ("pg_attribute", [(name,) for name in SOURCE_COLUMNS]),
        ("count(*)", [(1,)]),
        ("max(updated_at)", [(_WATERMARK,)]),
        ("WHERE updated_at >", [(_MERCHANT_ID,)]),
        ("COLLATE", [_ROW]),
        ("::timestamptz >=", [(True,)]),
        ("server_encoding", [("UTF8",)]),
    )

    def __init__(self):
        self.statements: list[str] = []
        self.params: list[object] = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> bool:
        return False

    def execute(self, statement: str, params=None) -> _Cursor:
        self.statements.append(statement)
        self.params.append(params)
        if "current_setting" in statement:
            return _Cursor([(RENDERING_GUCS[params[0]],)])
        for marker, rows in self._ANSWERS:
            if marker in statement:
                return _Cursor(rows)
        return _Cursor([])


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


def test_an_incremental_run_without_the_hand_off_is_REFUSED_by_main(tmp_path):
    """`--since` AND `--wait-for` WERE INDEPENDENT FLAGS, AND THEY WERE NEVER INDEPENDENT.

    `--since` runs the incremental query whose complement this phase publishes;
    `_refuse_a_watermark_before_t2` ran only when `--wait-for` was ALSO passed. So the
    combination below took the complement with no protection against the race at all, and
    the refusal never fired because there were no stamps to fire it. The failure is silent
    in the way this phase's other two were: the miss falls from 48 to 40 while the row
    count, the byte count, the sha256 and the watermark all still print correct.

    Asserted through `main()` rather than through the helper, because the defect was in the
    WIRING and a helper-level test is exactly what could not have caught it."""
    with pytest.raises(MerchantSnapshotRefused, match="was given without --wait-for"):
        extractor.main(
            ["--month", "2026-08", "--since", _T2, "--no-upload", "--out", str(tmp_path)]
        )


def test_the_since_value_itself_is_compared_against_t2_and_not_only_the_watermark():
    """THE OPERAND THAT CARRIES THE RACE IS THE ARGUMENT, not this run's own watermark.

    `--since` is snapshot 1's watermark, produced by an EARLIER process. If that read
    landed between t1 and t2 it recorded a stamp below t1, and `WHERE updated_at > :since`
    then RETURNS the eight held-open rows rather than missing them. Snapshot 2's own
    watermark is t2 in that run and in a correct one alike, so the pre-existing check
    cannot see this: it is a second claim about a second value."""
    conn = _Comparing(verdict=False)

    with pytest.raises(MerchantSnapshotRefused, match="BEFORE the mutation's t2"):
        extractor._refuse_a_since_before_t2(conn, _T1, {"t1": _T1, "t2": _T2})

    assert conn.asked and conn.asked[0][1] == (_T1, _T2), (
        "the comparison must be made by POSTGRES over the --since value and t2, in that "
        "order -- not over this run's own watermark"
    )


def test_an_incremental_run_WITH_the_hand_off_still_runs_end_to_end(tmp_path, monkeypatch):
    """THE OTHER HALF OF THE REFUSAL, and without it the test above has closed nothing.

    A refusal that also refused the CORRECT invocation would pass the assertion above and
    break the run this phase exists to make. So the same `main()` is driven through the
    whole session against a scripted connection: the readiness file is there, both stamps
    are read back, the pin is read back, the six statements are issued, the file is written
    and verified, and the incremental query's answer reaches the manifest."""
    ready = tmp_path / "ready.txt"
    ready.write_text(f"t1={_T1}\nt2={_T2}\n", encoding="utf-8")
    conn = _Session()
    monkeypatch.setattr(extractor.psycopg, "connect", lambda *a, **k: conn)

    assert (
        extractor.main(
            [
                "--month", "2026-08",
                "--since", _WATERMARK,
                "--wait-for", str(ready),
                "--no-upload",
                "--out", str(tmp_path),
            ]
        )
        == 0
    )

    landed = tmp_path / extractor.filename_for(_INSTANT)
    manifest = json.loads((tmp_path / f"{landed.name}.manifest.json").read_text("utf-8"))
    assert manifest["incremental_keys"] == [_MERCHANT_ID]
    assert manifest["watermark"] == _WATERMARK
    assert manifest["byte_count"] == landed.stat().st_size
    assert any("::timestamptz >=" in statement for statement in conn.statements)


def test_an_empty_table_is_refused_rather_than_compared():
    """A NULL watermark is an empty table, and for THIS source an empty snapshot is exactly
    what a table whose every row was deleted looks like -- the vault would end-date every
    key it holds. It is refused here rather than handed to a comparison that cannot read
    it."""
    with pytest.raises(MerchantSnapshotRefused, match="watermark is NULL"):
        extractor._refuse_a_watermark_before_t2(
            _Comparing(verdict=True), _observed(watermark=None), {"t1": _T1, "t2": _T2}
        )


# --------------------------------------------------------------------------------
# The DSN this script prints on its own header line
# --------------------------------------------------------------------------------


def test_a_QUOTED_password_is_refused_rather_than_printed_half_redacted():
    """THE HALF THE WHITESPACE SPLIT COULD NOT SEE.

    `redacted_dsn` -- which `main` calls to print its header, so its output goes to stdout
    and from there into an evidence document -- blanked every token starting with
    `password=`. libpq's keyword/value form lets a value be SINGLE-QUOTED and therefore
    contain whitespace, and `str.split()` does not know that: `password='a b'` becomes
    `["password='a", "b'"]`, only the first token is blanked, and `b'` is printed in clear.

    Half a password in an evidence document is a leaked password. The function's own
    contract is that it never returns a string it cannot promise it cleaned, so this is
    refused the way the URI form already was, rather than redacted better -- writing a
    libpq quoting parser here would be a second thing to get wrong, in a probe helper.

    A BACKSLASH IS REFUSED FOR THE SAME REASON and asserted separately: it is how a quote
    is escaped inside a quoted value, so it says the same thing about token boundaries
    while containing no quote for the first check to catch.

    The last assertion is the control. Without it this test passes over a `redacted_dsn`
    that refused every DSN it was ever given, including the compose default the probes
    actually run on -- a refusal that redacts everything is not a fix, it is a broken
    header line."""
    quoted = extractor.redacted_dsn("host=localhost password='a b' dbname=opl")
    assert "not rendered" in quoted, "a quoted DSN was rendered rather than refused"
    assert "a b" not in quoted and "b'" not in quoted, (
        f"part of a quoted password survived redaction: {quoted!r}"
    )

    escaped = extractor.redacted_dsn("host=localhost password=a\\ b dbname=opl")
    assert "not rendered" in escaped, "a backslash-escaped DSN was rendered rather than refused"

    plain = extractor.redacted_dsn("host=localhost port=5433 dbname=opl user=opl password=s3cret")
    assert plain == "host=localhost port=5433 dbname=opl user=opl password=***", (
        "the control: an ordinary unquoted DSN must still RENDER with its password blanked, "
        "or the refusal above is just a function that refuses everything"
    )
