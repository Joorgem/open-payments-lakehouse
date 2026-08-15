"""F-DB Task 0 probe: every Postgres measurement the phase is built on, re-runnable.

WHY THIS FILE EXISTS. The F-DB plan states answers to questions nobody had run --
"restart survives, only `down`+`up` destroys", "a single SELECT under READ COMMITTED
cannot smear", "the stamp gap is 2.52 s", "`opl` is a SUPERUSER". Revision 1 of that
same plan pre-answered the persistence matrix and was WRONG, in the section whose
subject is refusing inherited errors. So every number this phase leans on is produced
here, beside the statements that produced it, and a reader who disbelieves one runs
the script rather than the document.

MEASURES ONLY. No contract, no registry entry, no extractor, no seeder. The tables
below live in their own schema (`probe_f_db`) and the schema is dropped on the way
out, in a `finally`, so a re-run meets the same database it met the first time.

RUN IT WITH `uv run python scripts/probe_postgres.py`. This module is the entry point
and carries the snapshot-and-visibility measurements (T2, T3). Two siblings carry the
rest and each runs standalone as well:
  * `probe_postgres_container.py` -- the persistence matrix (T6), opt-in below with
    `--persistence`, because measuring "empty on a fresh volume" needs `down -v`.
  * `probe_postgres_rendering.py` -- the GUC matrix and the `::text` divergences (T4).
`probe_postgres_session.py` holds the connection helpers all three share.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections.abc import Sequence
from typing import Any

import psycopg
from probe_postgres_container import measure_1_persistence
from probe_postgres_rendering import measure_all as measure_rendering
from probe_postgres_session import (
    DSN_ENV_VAR,
    LIBPQ_RENDERING_ENV,
    PROBE_SCHEMA,
    clean_env,
    commit_after,
    connect,
    heading,
    one,
    redacted_dsn,
    row,
    rows,
    setup_schema,
    teardown_schema,
)

# --------------------------------------------------------------------------------
# 0. The container, the driver, the role, the encoding, the collation
# --------------------------------------------------------------------------------


def measure_0_environment() -> None:
    heading("0. SERVER, DRIVER, ROLE, ENCODING, COLLATION")
    print(f"  psycopg version:          {psycopg.__version__}")
    source = f"${DSN_ENV_VAR}" if DSN_ENV_VAR in os.environ else "the compose default"
    print(f"  DSN source:               {source}")
    for name in LIBPQ_RENDERING_ENV:
        print(f"  ambient {name:<16} {os.environ.get(name, '(unset)')}")
    with clean_env(), connect(search_path=False) as conn:
        print(f"  version():                {one(conn, 'SELECT version()')}")
        print(f"  server_version_num:       {one(conn, 'SHOW server_version_num')}")
        user, superuser = row(
            conn, "SELECT current_user, usesuper FROM pg_user WHERE usename = current_user"
        )
        print(f"  current_user:             {user}")
        print(f"  IS A SUPERUSER:           {superuser}   <- T3's reason for READ ONLY")
        enc, collate, ctype, provider = row(
            conn,
            "SELECT pg_encoding_to_char(encoding), datcollate, datctype, datlocprovider "
            "FROM pg_database WHERE datname = current_database()",
        )
        print(f"  server_encoding:          {enc}  (asserted, not assumed -- T4)")
        print(f"  datcollate / datctype:    {collate} / {ctype}")
        print(f"  datlocprovider:           {provider}  ('c' = libc; ordering moved at glibc 2.28)")
        sample = "(VALUES ('a'),('A'),('B'),('Z'),('ä'),('a b'),('ab'),('_z')) AS t(s)"
        default_order = [r[0] for r in rows(conn, f"SELECT s FROM {sample} ORDER BY s")]
        c_order = [r[0] for r in rows(conn, f'SELECT s FROM {sample} ORDER BY s COLLATE "C"')]
        # ascii() rather than the strings themselves: a Windows console is cp1252 and
        # would render the non-ASCII sample as a replacement character.
        print(f"  ORDER BY s:               {ascii(default_order)}")
        print(f'  ORDER BY s COLLATE "C":   {ascii(c_order)}')
        print(f"  the two orders agree:     {default_order == c_order}   <- T11")
        user_tables = one(
            conn,
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')",
        )
        print(f"  user tables in `opl` now: {user_tables}")


# --------------------------------------------------------------------------------
# 2 + 3 + 4. Isolation: what smears, what does not, and why a transaction is needed
# --------------------------------------------------------------------------------

SMEAR_ROWS = 10


def reset_smear(conn: psycopg.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS smear")
    conn.execute("CREATE TABLE smear (id int PRIMARY KEY, val text NOT NULL)")
    conn.execute(
        "INSERT INTO smear SELECT g, 'old' FROM generate_series(1, %s) AS g",
        (SMEAR_ROWS,),
    )


def measure_2_single_statement_is_atomic() -> None:
    heading("2. READ COMMITTED, ONE STATEMENT -- the failure mode revision 1 claimed")
    with clean_env(), connect() as reader:
        reset_smear(reader)
        print(f"  isolation: {one(reader, 'SHOW transaction_isolation')} (session default)")
        writer = commit_after(
            2.0,
            [
                "UPDATE smear SET val = 'new'",
                "DELETE FROM smear WHERE id = 10",
                "INSERT INTO smear VALUES (11, 'inserted-mid-scan')",
            ],
        )
        started = time.monotonic()
        scanned = rows(reader, "SELECT id, val, pg_sleep(0.4) FROM smear ORDER BY id")
        elapsed = time.monotonic() - started
        writer.join()
        values = {r[0]: r[1] for r in scanned}
        print(f"  scan took {elapsed:.1f}s, writer committed at t=2.0s")
        print(f"  rows returned:            {sorted(values.items())}")
        print(f"  every row 'old':          {set(values.values()) == {'old'}}")
        print(f"  deleted row 10 returned:  {10 in values}")
        print(f"  inserted row 11 returned: {11 in values}")
        print("  => ONE STATEMENT IS ONE SNAPSHOT. A single-statement test CANNOT FAIL,")
        print("     so it certifies nothing (plan sec. 0.3).")
        after = rows(reader, "SELECT id, val FROM smear ORDER BY id")
        print(f"  the table afterwards:     {after}")


def batched_keyset(conn: psycopg.Connection, batch: int) -> list[tuple]:
    """The read every engineer writes, one batch per statement."""
    collected: list[tuple] = []
    last = 0
    while True:
        page = rows(
            conn,
            "SELECT id, val FROM smear WHERE id > %s ORDER BY id LIMIT %s",
            (last, batch),
        )
        if not page:
            return collected
        collected.extend(page)
        last = page[-1][0]


def measure_3_batched_read_smears() -> None:
    heading("3. READ COMMITTED, A BATCHED KEYSET READ -- the failure mode that IS real")
    with clean_env(), connect() as reader:
        reset_smear(reader)
        collected: list[tuple] = []
        last = 0
        batch = 3
        while True:
            page = rows(
                reader,
                "SELECT id, val FROM smear WHERE id > %s ORDER BY id LIMIT %s",
                (last, batch),
            )
            if not page:
                break
            collected.extend(page)
            last = page[-1][0]
            if last == 3:  # a writer commits between batch 1 and batch 2
                with clean_env(), connect() as writer:
                    writer.execute("UPDATE smear SET val = 'new'")
                    writer.execute("INSERT INTO smear VALUES (0, 'inserted-behind-the-cursor')")
                    writer.execute("DELETE FROM smear WHERE id = 7")
        print(f"  rows the batched read returned: {collected}")
        print(f"  distinct values in ONE read:    {sorted({r[1] for r in collected})}")
        seen = [r[0] for r in collected]
        print(f"  row 7 (deleted mid-read):       {'returned' if 7 in seen else 'MISSING'}")
        print(f"  row 0 (inserted behind it):     {'returned' if 0 in seen else 'MISSING'}")
        final = rows(reader, "SELECT id, val FROM smear ORDER BY id")
        print(f"  the table at the end:           {final}")
        print(f"  read {len(collected)} rows; the table holds {len(final)}. NEITHER is an instant.")
        print("  REFUSED, with the mechanism: an immutable-PK keyset cannot DUPLICATE a row.")
        print("  `id >` is strictly increasing, so a row is returned at most once. Duplication")
        print("  needs a mutable sort key or OFFSET paging -- measured next.")


def measure_3b_offset_paging_duplicates() -> None:
    heading("3b. WHERE DUPLICATION ACTUALLY COMES FROM: OFFSET paging")
    with clean_env(), connect() as reader:
        reset_smear(reader)
        collected: list[tuple] = []
        offset = 0
        batch = 3
        while True:
            page = rows(
                reader, "SELECT id, val FROM smear ORDER BY id OFFSET %s LIMIT %s", (offset, batch)
            )
            if not page:
                break
            collected.extend(page)
            offset += batch
            if offset == batch:
                with clean_env(), connect() as writer:
                    writer.execute("INSERT INTO smear VALUES (0, 'inserted-before-the-window')")
        ids = [r[0] for r in collected]
        duplicated = sorted({i for i in ids if ids.count(i) > 1})
        print(f"  ids returned: {ids}")
        print(f"  DUPLICATED:   {duplicated}")
        print("  One INSERT at a lower id shifts every later window by one row.")


def measure_4_repeatable_read_across_statements() -> None:
    heading("4. THE SAME TWO STATEMENTS, READ COMMITTED vs REPEATABLE READ")
    with clean_env(), connect() as reader:
        reset_smear(reader)
        reader.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        first = sorted({r[1] for r in rows(reader, "SELECT id, val FROM smear ORDER BY id")})
        with clean_env(), connect() as writer:
            writer.execute("UPDATE smear SET val = 'new'")
        second = sorted({r[1] for r in rows(reader, "SELECT id, val FROM smear ORDER BY id")})
        reader.execute("COMMIT")
        print(
            f"  READ COMMITTED   stmt1={first}  stmt2={second}  -> TWO snapshots in one transaction"
        )

        reset_smear(reader)
        reader.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        first = sorted({r[1] for r in rows(reader, "SELECT id, val FROM smear ORDER BY id")})
        with clean_env(), connect() as writer:
            writer.execute("UPDATE smear SET val = 'new'")
        second = sorted({r[1] for r in rows(reader, "SELECT id, val FROM smear ORDER BY id")})
        batched = sorted({r[1] for r in batched_keyset(reader, 3)})
        reader.execute("COMMIT")
        print(
            f"  REPEATABLE READ  stmt1={first}  stmt2={second}  batched={batched}  -> ONE snapshot"
        )
        print("  This is the test that closes T3: the SAME batched read, unchanged, under the")
        print("  transaction. Under READ COMMITTED it smeared (sec. 3); here it does not.")

        reader.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        try:
            reader.execute("UPDATE smear SET val = 'the extractor writes'")
            print("  READ ONLY did NOT refuse a write -- FALSIFIED")
        except psycopg.errors.ReadOnlySqlTransaction as exc:
            print(f"  READ ONLY refuses a write: {str(exc).strip().splitlines()[0]}")
        reader.execute("ROLLBACK")


# --------------------------------------------------------------------------------
# 5. The stamp gap
# --------------------------------------------------------------------------------


def measure_5_stamp_gap() -> None:
    heading("5. THE STAMP GAP -- `transaction_timestamp()` is fixed at BEGIN, the snapshot is not")
    with clean_env(), connect() as setup:
        setup.execute("DROP TABLE IF EXISTS gap")
        setup.execute(
            "CREATE TABLE gap (id serial PRIMARY KEY, note text NOT NULL, "
            "committed_at timestamptz NOT NULL)"
        )

    stamp_sql = (
        "SELECT transaction_timestamp(), clock_timestamp(), pg_current_snapshot()::text, "
        "pg_current_wal_lsn()::text"
    )
    for stamp_first in (False, True):
        label = (
            "STAMP AS THE FIRST STATEMENT (T3's ruling)" if stamp_first else "STAMP AFTER A DELAY"
        )
        print(f"  --- {label} ---")
        with clean_env(), connect() as reader:
            reader.execute("TRUNCATE gap RESTART IDENTITY")
            reader.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            stamp = row(reader, stamp_sql) if stamp_first else None
            writer = commit_after(
                1.2,
                [
                    "INSERT INTO gap (note, committed_at) "
                    "VALUES ('committed after BEGIN', clock_timestamp())"
                ],
            )
            time.sleep(2.5)
            writer.join()
            if stamp is None:
                stamp = row(reader, stamp_sql)
            visible = one(reader, "SELECT count(*) FROM gap")
            reader.execute("COMMIT")
        _report_stamp_gap(stamp, visible, stamp_first=stamp_first)


def _report_stamp_gap(stamp: tuple[Any, ...], visible: int, *, stamp_first: bool) -> None:
    """Print one arm of the gap measurement, and the row that falls inside it."""
    txn_ts, clock_ts, snapshot, lsn = stamp
    print(f"  transaction_timestamp() (fixed at BEGIN):  {txn_ts.isoformat()}")
    print(f"  clock_timestamp() at the snapshot stmt:    {clock_ts.isoformat()}")
    gap = (clock_ts - txn_ts).total_seconds()
    print(f"  GAP BEGIN -> SNAPSHOT:                     {gap:.3f}s")
    print(f"  pg_current_snapshot():                     {snapshot}")
    print(f"  pg_current_wal_lsn():                      {lsn}")
    print(f"  rows committed inside the gap that ARE in the snapshot: {visible}")
    if not (visible and not stamp_first):
        return
    with clean_env(), connect() as check:
        committed_at = one(check, "SELECT committed_at FROM gap ORDER BY id LIMIT 1")
    print(f"  the row's own commit clock: {committed_at.isoformat()}")
    print(f"  committed AFTER the stamp:  {committed_at > txn_ts}")
    print("  => the stamp claims to predate a row it can see. That is the smear the")
    print("     ruling exists to prevent, and it is in the PROVENANCE, not the data.")


# --------------------------------------------------------------------------------
# 6. The out-of-order commit -- the row no watermark run will ever return
# --------------------------------------------------------------------------------


def build_watermark_table(conn: psycopg.Connection) -> None:
    """A merchant-shaped table with the CORRECT fix in place: a BEFORE UPDATE trigger.

    A `DEFAULT now()` does not fire on UPDATE (§9), so a trigger is what an engineer
    who has already met that trap installs. The point of this section is that the
    trigger does not save them either.
    """
    conn.execute("DROP TABLE IF EXISTS wm")
    conn.execute(
        "CREATE TABLE wm (id int PRIMARY KEY, val text NOT NULL, "
        "updated_at timestamptz NOT NULL DEFAULT now())"
    )
    conn.execute(
        "CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $$ "
        "BEGIN NEW.updated_at := now(); RETURN NEW; END; $$ LANGUAGE plpgsql"
    )
    conn.execute(
        "CREATE TRIGGER wm_touch BEFORE UPDATE ON wm "
        "FOR EACH ROW EXECUTE FUNCTION touch_updated_at()"
    )
    conn.execute("INSERT INTO wm SELECT g, 'v0' FROM generate_series(1, 5) AS g")


def _snapshot_and_watermark(conn: Any, label: str) -> tuple[list[Any], Any]:
    """One `REPEATABLE READ READ ONLY` transaction reading the table AND its watermark.

    Both come out of ONE transaction on purpose: T2 rules that the watermark query runs
    inside the same transaction that reads its snapshot, so the two are one observation of
    one MVCC state and the miss set is a genuine complement rather than a definitional one.
    """
    conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
    snapshot = rows(conn, "SELECT id, val FROM wm ORDER BY id")
    watermark = one(conn, "SELECT max(updated_at) FROM wm")
    conn.execute("COMMIT")
    print(f"  {label}:               {snapshot}")
    return snapshot, watermark


def _watermark_run(conn: Any, watermark: Any) -> list[Any]:
    """What an incremental extract would return for this watermark, right now."""
    return rows(conn, "SELECT id, val, updated_at FROM wm WHERE updated_at > %s", (watermark,))


def measure_6_out_of_order_commit() -> None:
    heading("6. THE OUT-OF-ORDER COMMIT -- the row no watermark run will ever return")
    with clean_env(), connect() as setup:
        build_watermark_table(setup)
        print("  table has a BEFORE UPDATE trigger setting updated_at := now() -- the CORRECT fix")

    slow = connect()
    fast = connect()
    extractor = connect()
    try:
        slow.execute("BEGIN")
        slow.execute("UPDATE wm SET val = 'slow-writer' WHERE id = 1")
        t1 = one(slow, "SELECT updated_at FROM wm WHERE id = 1")
        print(f"  slow txn stamped id=1 at t1 = {t1.isoformat()} and STAYS OPEN")

        time.sleep(0.5)
        fast.execute("BEGIN")
        fast.execute("UPDATE wm SET val = 'fast-writer' WHERE id = 2")
        t2 = one(fast, "SELECT updated_at FROM wm WHERE id = 2")
        fast.execute("COMMIT")
        print(f"  fast txn stamped id=2 at t2 = {t2.isoformat()} and COMMITTED")
        print(f"  t2 > t1: {t2 > t1}   (the stamps order by transaction START)")

        snapshot_1, watermark = _snapshot_and_watermark(extractor, "snapshot 1")
        print(f"  watermark recorded:       {watermark.isoformat()}  (== t2: {watermark == t2})")

        slow.execute("COMMIT")
        print("  slow txn COMMITS -- after the extract, carrying a stamp older than the watermark")

        missed = _watermark_run(extractor, watermark)
        print(f"  WHERE updated_at > watermark: {missed}")
        time.sleep(1.0)
        again = _watermark_run(extractor, watermark)
        print(f"  the same query one second later: {again}   <- and forever")

        snapshot_2, _ = _snapshot_and_watermark(extractor, "snapshot 2")
        changed = [a[0] for a, b in zip(snapshot_1, snapshot_2, strict=True) if a != b]
        print(f"  ids the SNAPSHOT DIFF catches: {changed}")
        print(f"  ids the WATERMARK catches:     {[r[0] for r in again]}")
        print("  `updated_at` orders by transaction START; visibility orders by COMMIT.")
    finally:
        for conn in (slow, fast, extractor):
            conn.close()


# --------------------------------------------------------------------------------
# 10. Three more claims T3's table makes, checked rather than quoted
# --------------------------------------------------------------------------------


VOLATILE_WATCHER = (
    "CREATE OR REPLACE FUNCTION volatile_watch() RETURNS text AS $$ "
    "DECLARE before bigint; after bigint; "
    "BEGIN "
    "  SELECT count(*) INTO before FROM smear WHERE val = 'new'; "
    "  PERFORM pg_sleep(2); "
    "  SELECT count(*) INTO after FROM smear WHERE val = 'new'; "
    "  RETURN before || ' -> ' || after; "
    "END; $$ LANGUAGE plpgsql VOLATILE"
)


def volatile_function_inside_one_statement(isolation: str) -> str:
    """Two reads INSIDE one outer statement, with a writer committing between them.

    This is the only test that can answer T3's claim. Calling the function twice
    would prove nothing -- two statements are two snapshots under READ COMMITTED
    anyway (sec. 4). The question is whether a VOLATILE function escapes the snapshot
    of the statement that CALLS it, so both reads have to be inside one call.
    """
    with clean_env(), connect() as reader:
        reset_smear(reader)
        reader.execute(VOLATILE_WATCHER)
        reader.execute(f"BEGIN ISOLATION LEVEL {isolation}")
        writer = commit_after(1.0, ["UPDATE smear SET val = 'new'"])
        observed = one(reader, "SELECT volatile_watch()")
        writer.join()
        reader.execute("COMMIT")
        return observed


def measure_10_further_t3_claims() -> None:
    heading("10. THREE FURTHER T3 CLAIMS, CHECKED")
    read_committed = volatile_function_inside_one_statement("READ COMMITTED")
    repeatable_read = volatile_function_inside_one_statement("REPEATABLE READ READ ONLY")
    print(f"  VOLATILE function, two reads in ONE statement, READ COMMITTED:  {read_committed}")
    print(f"  the same function under REPEATABLE READ READ ONLY:              {repeatable_read}")
    print("  Under READ COMMITTED it escapes the statement snapshot (the control proves")
    print("  the mechanism exists at all). Under REPEATABLE READ it does NOT.")

    with clean_env(), connect() as reader:
        reset_smear(reader)
        reader.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        reader.execute(
            "DECLARE probe_cursor NO SCROLL CURSOR FOR SELECT id, val FROM smear ORDER BY id"
        )
        head = rows(reader, "FETCH 3 FROM probe_cursor")
        with clean_env(), connect() as writer:
            writer.execute("UPDATE smear SET val = 'new'")
        tail = rows(reader, "FETCH ALL FROM probe_cursor")
        reader.execute("CLOSE probe_cursor")
        reader.execute("COMMIT")
        values = sorted({r[1] for r in head + tail})
        print(
            f"  server-side cursor under READ COMMITTED: first 3 {head[:1]}..., rest {tail[:1]}..."
        )
        held = values == ["old"]
        print(f"  distinct values across the whole cursor: {values}  -> holds its snapshot: {held}")

        reader.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        unassigned = one(reader, "SELECT pg_current_xact_id_if_assigned()::text")
        try:
            xid = one(reader, "SELECT pg_current_xact_id()::text")
            assigned = one(reader, "SELECT pg_current_xact_id_if_assigned()::text")
            reader.execute("COMMIT")
            print(
                f"  inside a READ ONLY transaction: if_assigned={unassigned}, "
                f"pg_current_xact_id()={xid}, if_assigned after={assigned}"
            )
            print("  => it assigns a real XID from inside a read. Refused as a stamp (T3).")
        except psycopg.errors.ReadOnlySqlTransaction as exc:
            reader.execute("ROLLBACK")
            print(f"  pg_current_xact_id() REFUSED in a READ ONLY txn: {str(exc).splitlines()[0]}")


COLUMN_LIST_SQL = (
    "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) "
    "FROM information_schema.columns WHERE table_schema = %s AND table_name = 'smear'"
)


def measure_11_catalog_read_inside_the_transaction() -> None:
    """T3's ruling lists "the column-list catalog read" among the statements the
    REPEATABLE READ transaction protects. That is a claim about whether a catalog
    read is snapshot-stable, and it decides whether the landed column list belongs
    to the same observation as the landed rows. Nobody had run it.

    The ALTER happens BEFORE the reader touches the table, and that ordering is
    forced: see 11b, where the reverse order deadlocks the probe.
    """
    heading("11. IS THE COLUMN-LIST CATALOG READ PART OF THE SAME SNAPSHOT?")
    with clean_env(), connect() as reader:
        reset_smear(reader)
        reader.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        before = one(reader, COLUMN_LIST_SQL, (PROBE_SCHEMA,))
        with clean_env(), connect() as writer:
            writer.execute("ALTER TABLE smear ADD COLUMN added_mid_transaction text")
            writer.execute("INSERT INTO smear VALUES (99, 'added-mid-transaction', 'x')")
        after = one(reader, COLUMN_LIST_SQL, (PROBE_SCHEMA,))
        visible = one(reader, "SELECT count(*) FROM smear")
        reader.execute("COMMIT")
        live = one(reader, "SELECT count(*) FROM smear")
    print(f"  column list at the snapshot statement:       {before}")
    print(f"  column list after another session ALTERs it: {after}")
    print(f"  the catalog read is SNAPSHOT-STABLE:         {before == after}")
    print(f"  rows the same transaction sees: {visible}, rows actually in the table: {live}")
    print("  The DATA is snapshot-stable either way. Whether the CATALOG is decides whether")
    print("  a landed column list describes the rows landed beside it.")


def measure_11b_the_reader_blocks_ddl() -> None:
    """The hazard this probe hit by accident, so it is measured on purpose.

    A `REPEATABLE READ READ ONLY` transaction that has read a table holds ACCESS
    SHARE on it until it commits, and ACCESS EXCLUSIVE -- every form of `ALTER
    TABLE`, and `VACUUM FULL` -- waits behind it. The first version of section 11
    ordered the two the other way round and hung the probe with the reader `idle in
    transaction` and the writer waiting on `Lock`. That is T3's "COMMIT before the
    upload" ruling arriving from a second direction: the cost of an open read
    transaction is not only the xmin horizon, it is every DDL on the tables it read.
    """
    heading("11b. A READ ONLY TRANSACTION BLOCKS DDL ON WHAT IT READ")
    with clean_env(), connect() as reader:
        reset_smear(reader)
        reader.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        reader.execute("SELECT count(*) FROM smear")
        with clean_env(), connect() as writer:
            writer.execute("SET lock_timeout = '1000ms'")
            started = time.monotonic()
            try:
                writer.execute("ALTER TABLE smear ADD COLUMN blocked_column text")
                print("  the ALTER succeeded -- the reader did NOT block it")
            except psycopg.errors.LockNotAvailable as exc:
                waited = time.monotonic() - started
                print(f"  ALTER TABLE waited {waited:.1f}s and then: {str(exc).splitlines()[0]}")
        reader.execute("COMMIT")
        with clean_env(), connect() as writer:
            writer.execute("SET lock_timeout = '1000ms'")
            writer.execute("ALTER TABLE smear ADD COLUMN blocked_column text")
            print("  the same ALTER, once the reader COMMITTED: succeeded")


# --------------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="F-DB Task 0 Postgres probe. See the module docstring."
    )
    parser.add_argument(
        "--persistence",
        action="store_true",
        help="also run the container persistence matrix. DESTRUCTIVE: it runs `down -v`.",
    )
    args = parser.parse_args(argv)

    print("F-DB Task 0 probe -- local throwaway Postgres container, no secret involved.")
    print(f"dsn: {redacted_dsn()}")
    measure_0_environment()
    if args.persistence:
        measure_1_persistence()
    else:
        print("\n(section 1, the persistence matrix, is skipped: it destroys the database.")
        print(" Re-run with --persistence to measure it.)")
    setup_schema()
    try:
        measure_2_single_statement_is_atomic()
        measure_3_batched_read_smears()
        measure_3b_offset_paging_duplicates()
        measure_4_repeatable_read_across_statements()
        measure_5_stamp_gap()
        measure_6_out_of_order_commit()
        measure_rendering()
        measure_10_further_t3_claims()
        measure_11_catalog_read_inside_the_transaction()
        measure_11b_the_reader_blocks_ddl()
    finally:
        teardown_schema()
    return 0


if __name__ == "__main__":
    sys.exit(main())
