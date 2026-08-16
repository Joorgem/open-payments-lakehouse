"""Read ONE transactionally-consistent snapshot of the merchant registry and land it.

THE ANALOGUE OF `unzip_table`, `generate_payments` AND `fetch_ptax`, FOR A SOURCE NOTHING
INSIDE THE WORKSPACE CAN REACH. Every bronze table's landing dir is filled by something
before its ingest runs. This one is filled from HERE, on the extraction host, and that is
plan T1: the database is a container bound to `localhost:5433`, and egress OUT of
Databricks creates no route INTO a laptop behind a home NAT. Those are opposite directions,
and ADR 0002 conflated them -- F-API measured that serverless CAN reach the public
internet, which changes nothing about reaching this container. The conclusion is ARGUED, not
measured: there is no public address to point a probe at, so no measurement is offered and
none is implied.

WHAT THIS FILE OWNS, AND IT IS EXACTLY THREE THINGS. `opl.extraction.postgres_source`
builds every statement, validates every answer and executes no I/O of its own; this file
owns the CONNECTION, the local file, and the upload. That is the same seam
`databricks/src/fetch_ptax.py` sits on -- "the transport, and it is the only thing this file
adds to the extraction layer".

THE DSN COMES FROM THE ENVIRONMENT AND IS NEVER A LITERAL HERE.
`probe_postgres_session.dsn()` reads `OPL_POSTGRES_DSN` with the compose default as its
fallback, and `redacted_dsn()` blanks every `password=` token; both are reused rather than
re-spelled. Nothing in this file may print an unredacted DSN.

THE GUCs ARE PINNED AS THE SESSION'S FIRST ACT, BEFORE `BEGIN` -- measured, and the
measurement is in `postgres_source`'s docstring: `SELECT set_config(...)` acquires the
`REPEATABLE READ` snapshot itself, so a pin inside the transaction freezes the snapshot
before the stamp is taken.

--- READINESS IS WAITED FOR, AND ABSENCE IS NOT READINESS -----------------------------

`scripts/seed_merchant_db.py mutate --ready-on PATH` writes a file once its own `t2 > t1`
refusal has passed, carrying both stamps. Snapshot 1 must be taken AFTER that file appears
and its own watermark must be at or after `t2`. Task 3 measured what happens otherwise:
starting when an `idle in transaction` session appears catches the mutation BETWEEN t1 and
t2, the watermark lands below t1, and the out-of-order rows become perfectly visible to
`WHERE updated_at > watermark` -- the phase's one non-authored number disappears while every
other count stays correct.

So this waits, it REFUSES a timeout rather than proceeding, and it re-checks its own
watermark against `t2` afterwards. A partial read of the readiness file is expected (it is
written with a plain `write_text`, so a poller can observe it mid-write) and is treated as
"not yet", never as a signal.

--- THE FILE IS WRITTEN LOCALLY, VERIFIED AS BYTES, AND THEN PUT ----------------------

`opl.bronze.generated_landing.emit_records_file` CANNOT be reused, AND THE REASON IS
SHARPER THAN THE ONE USUALLY GIVEN. The usual one is that it `os.replace`s a staged file
into the landing dir, which is atomic only within one UC Volume's single FUSE mount -- true
of what that function DOES, and not what stops this caller: `os.replace` is perfectly happy
between two ordinary directories on one local drive.

What stops it is that `directory` is a VOLUME PATH and this host has no Volume mounted at
all. `emit_records_file(directory="/Volumes/workspace/default/landing/postgres/...")` would
`os.makedirs` that path on the extraction host -- on Windows, `C:\\Volumes\\workspace\\...` --
write a perfectly good file into it, verify it, and report success. Nothing raises. The
Volume stays empty, the ingest drains an empty directory, and for THIS source an empty
snapshot is exactly what a table whose every row was deleted looks like. So the file is
written locally and PUT through the control plane, which is the only route a host has.

WHAT THAT FUNCTION CARRIES THAT THIS NEEDS IS TWO REFUSALS, and both are in
`postgres_source`: the `\\r` refusal on the serialised bytes, and the read-back comparison
below. The write is BINARY and the encode is EXPLICIT UTF-8, for that module's reasons:
Python's text mode would ship `\\r\\n` on Windows where the read side declares `\\n`, and
`locale.getpreferredencoding()` is cp1252 on this dev box.

ITS THIRD PROPERTY -- `_refuse_a_different_file`, which compares against a file ALREADY IN
THE VOLUME -- is not reproduced, and that is a decision rather than a gap. It exists because
a PTAX window or a payment stream can be re-derived with DIFFERENT bytes under the SAME
name (BCB revises a quote; a seed changes). Here the name is a function of the snapshot
INSTANT, and an instant belongs to exactly one transaction: re-landing one snapshot is the
same bytes under the same name, and a second snapshot is a different name. The collision
that refusal exists for cannot be constructed.

THE FILENAME IS THE INSTANT AND CARRIES NO RUN ID. Auto Loader tracks files by PATH, so a
run-scoped name would make every re-run a second ingest of the same rows under a fresh
`_batch_id`, which the promote's idempotence is keyed on and cannot see. Two snapshots are
two instants and therefore two files; re-landing one is the same path, and `upload_to_volume`
overwrites it with identical bytes.

usage: extract_merchant_snapshot.py --month YYYY-MM [--wait-for PATH] [--since WATERMARK]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import psycopg
from probe_postgres_session import dsn, redacted_dsn

from opl.bronze.registry import landing_dir, table_spec
from opl.config import DEFAULT, require_month
from opl.extraction.landing import upload_client, upload_to_volume
from opl.extraction.postgres_source import (
    MerchantSnapshot,
    MerchantSnapshotRefused,
    filename_for,
    pin_rendering_gucs,
    readiness_stamps,
    records_of,
    refuse_bytes_that_are_not_the_payload,
    serialised_bytes,
    snapshot,
    watermark_is_at_or_after,
)

# The registry key this script extracts for. A literal, because a registry key is that
# dict's own namespace -- and it is resolved through `table_spec` immediately, so a rename
# is a refusal naming the registered tables rather than a directory that does not exist.
TABLE = "merchant"

# How long to wait for the readiness file before REFUSING. Generous, because the thing on
# the other side is a human running `mutate` in another terminal; finite, because a wait
# with no bound is a run that never fails and never finishes. A timeout is NOT readiness.
READY_TIMEOUT_SECONDS = 600.0
READY_POLL_SECONDS = 0.25


def wait_for_readiness(path: Path, timeout: float = READY_TIMEOUT_SECONDS) -> dict[str, str]:
    """Block until `path` yields both mutation stamps, or REFUSE.

    A partial file is "not yet" and never a signal -- see the module docstring. The refusal
    on timeout is the point of this function: proceeding without the hand-off is the one
    failure in this phase that leaves every published count correct and silently removes the
    number nothing authored."""
    deadline = time.monotonic() + timeout
    print(f"  waiting for the mutation's readiness file at {path}", flush=True)
    while time.monotonic() < deadline:
        if path.exists():
            stamps = readiness_stamps(path.read_text(encoding="utf-8"))
            if stamps is not None:
                print(f"  ready: t1={stamps['t1']} t2={stamps['t2']}", flush=True)
                return stamps
        time.sleep(READY_POLL_SECONDS)
    raise MerchantSnapshotRefused(
        f"{path} did not carry both mutation stamps within {timeout}s. This REFUSES rather "
        "than proceeding: a snapshot taken before the mutation's t2 records a watermark "
        "below t1, which makes the out-of-order rows visible to an incremental extract and "
        "deletes the one number in this phase's headline that nothing authored -- while "
        "every other count stays exactly right. Run `seed_merchant_db.py mutate "
        "--ready-on <this path> --release-on <a path this run touches>` first."
    )


def _refuse_a_watermark_before_t2(
    conn: psycopg.Connection, observed: MerchantSnapshot, stamps: dict[str, str]
) -> None:
    """Refuse a snapshot taken between the mutation's t1 and t2.

    THE HAND-OFF CHECKED RATHER THAN TRUSTED. The readiness file carries both stamps so
    that this comparison can be made, and it is made by POSTGRES -- `max(updated_at)::text`
    and `datetime.isoformat()` are two different spellings of one instant, and comparing
    them as Python strings is wrong in a way that would pass on most values."""
    if observed.watermark is None:
        raise MerchantSnapshotRefused(
            "the snapshot's watermark is NULL, so the table is empty -- run `seed` first"
        )
    if not watermark_is_at_or_after(conn, observed.watermark, stamps["t2"]):
        raise MerchantSnapshotRefused(
            f"this snapshot's watermark is {observed.watermark}, BEFORE the mutation's "
            f"t2={stamps['t2']}. The read landed between t1 and t2, so the held-open "
            "transaction's rows are still reachable by `WHERE updated_at > watermark` and "
            "the out-of-order miss this phase measures does not exist in this run."
        )


def write_verified(payload: bytes, path: Path) -> str:
    """Write `payload` in BINARY, read it back AS BYTES, and return its sha256.

    The read-back is the only comparison that can see the defect the binary write exists to
    prevent: a text-mode read normalises `\\r\\n` back to `\\n` and reports success over a
    file that is byte-different."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as handle:  # binary: no newline translation can exist
        handle.write(payload)
    refuse_bytes_that_are_not_the_payload(path.read_bytes(), payload, str(path))
    return hashlib.sha256(payload).hexdigest()


def manifest_of(observed: MerchantSnapshot, landed: dict[str, Any]) -> dict[str, Any]:
    """What this run observed, as data a later task can read rather than re-derive.

    THE INCREMENTAL KEY SET IS IN HERE AND NOT IN THE LANDED FILE, deliberately. Bronze is
    raw as-is: a column saying "an incremental extract would have caught this row" is a
    derivation, and deriving in the landing zone is exactly what plan T9 refuses. The
    complement that becomes the phase's headline is computed from this manifest and the
    lakehouse's own diff, by whoever publishes it."""
    return {
        "instant": observed.instant,
        "pg_snapshot": observed.pg_snapshot,
        "wal_lsn": observed.wal_lsn,
        "row_count": len(observed.rows),
        "watermark": observed.watermark,
        "asked_since": observed.asked_since,
        "incremental_row_count": len(observed.incremental_keys),
        "incremental_keys": list(observed.incremental_keys),
        **landed,
    }


def _report(observed: MerchantSnapshot, manifest: dict[str, Any]) -> None:
    """The run log, written to be quotable as evidence.

    The instant, the visibility set and the WAL position answer "which snapshot is this"
    exactly, where a wall clock alone can step backwards under NTP. The digest and the byte
    count are the two numbers a landed-file claim is stated in."""
    print(f"\nextract_merchant_snapshot: instant={observed.instant}")
    print(f"  pg_snapshot={observed.pg_snapshot}  wal_lsn={observed.wal_lsn}")
    print(f"  rows={manifest['row_count']}  watermark={observed.watermark}")
    if observed.asked_since is not None:
        print(
            f"  incremental (updated_at > {observed.asked_since}): "
            f"{manifest['incremental_row_count']} row(s)"
        )
    print(f"  bytes={manifest['byte_count']} sha256={manifest['sha256']}")
    print(f"  local={manifest['local_path']}")
    print(f"  landed={manifest.get('volume_path') or '(not uploaded)'}")


def _parse(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--month", required=True, help="the landing month, YYYY-MM")
    parser.add_argument("--schema", default="public")
    parser.add_argument(
        "--wait-for", type=Path, help="the mutation's --ready-on file; waited for, then checked"
    )
    parser.add_argument(
        "--since", help="the previous snapshot's watermark; runs the incremental query too"
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(tempfile.gettempdir()) / "opl-merchant-snapshots",
        help="local directory the verified file is written to before it is PUT",
    )
    parser.add_argument("--no-upload", action="store_true", help="write and verify only")
    return parser.parse_args(argv)


def _land(observed: MerchantSnapshot, args: argparse.Namespace, month: str) -> dict[str, Any]:
    """Serialise, write, verify, and (unless refused) PUT. Returns the landed facts."""
    payload = serialised_bytes(records_of(observed))
    local = Path(args.out) / filename_for(observed.instant)
    landed: dict[str, Any] = {
        "byte_count": len(payload),
        "sha256": write_verified(payload, local),
        "local_path": str(local),
        "volume_path": None,
    }
    if not args.no_upload:
        # THE ONE MAPPING, ASKED RATHER THAN RE-SPELLED. `landing_dir` takes the whole spec,
        # so the directory this host WRITES cannot drift from the one the ingest task READS,
        # and a mode no root serves is refused there rather than defaulting into a directory
        # holding another source's files -- which cloudFiles walks RECURSIVELY.
        target = landing_dir(DEFAULT, table_spec(TABLE), month)
        landed["volume_path"] = upload_to_volume(upload_client(), local, target)
    return landed


def main(argv: list[str] | None = None) -> int:
    args = _parse(argv)
    # NO DEFAULT, for `fetch_ptax`'s reason: this local picks the directory the file is PUT
    # into, and the ingest task that follows resolves its own source dir from the same job
    # parameter. A substituted month would write into one month's landing dir and read
    # another's -- a job that succeeds having ingested nothing.
    month = require_month(args.month, action="extract")
    print(f"  dsn: {redacted_dsn()}")
    stamps = wait_for_readiness(args.wait_for) if args.wait_for else None
    with psycopg.connect(dsn(), autocommit=True) as conn:
        # THE SESSION'S FIRST ACT, and outside the transaction -- see postgres_source.
        pin_rendering_gucs(conn)
        observed = snapshot(conn, schema=args.schema, since=args.since)
        if stamps is not None:
            _refuse_a_watermark_before_t2(conn, observed, stamps)
    # THE TRANSACTION IS OVER BEFORE ANYTHING IS UPLOADED. A `READ ONLY` transaction that
    # has merely read a table blocks every `ALTER TABLE` on it for as long as it lives.
    manifest = manifest_of(observed, _land(observed, args, month))
    Path(f"{manifest['local_path']}.manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    _report(observed, manifest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
