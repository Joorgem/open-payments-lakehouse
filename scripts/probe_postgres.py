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

THE DSN COMES FROM THE ENVIRONMENT, with the docker-compose defaults as the fallback
(plan §7). Nothing here reads a secret and nothing here should ever grow one -- the
credentials in the fallback are the throwaway container's, already committed in
`docker-compose.yml` and `tests/integration/test_postgres.py`.

THE PERSISTENCE MATRIX IS OPT-IN (`--persistence`) BECAUSE IT DESTROYS THE DATABASE.
It runs `docker compose down -v`, which is the only honest way to measure "empty on a
fresh volume". Once Task 3 seeds `merchant`, an unguarded default would delete it.

HOSTILE ENVIRONMENTS ARE SET, NOT DESCRIBED. §7 connects with `PGTZ`, `PGDATESTYLE`
and `PGOPTIONS` actually exported into the process environment, because that is the
delivery mechanism the ruling is about: libpq reads them at connect time and no code
change is required to be wrong.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]

DSN_ENV_VAR = "OPL_POSTGRES_DSN"
# The compose defaults. `docker-compose.yml:5-8` and `tests/integration/test_postgres.py:9`
# already carry these for a throwaway local container; this is the third and last copy.
DEFAULT_DSN = "host=localhost port=5433 dbname=opl user=opl password=opl"

PROBE_SCHEMA = "probe_f_db"

# The environment variables libpq turns into startup options with no code change.
LIBPQ_RENDERING_ENV = ("PGTZ", "PGDATESTYLE", "PGCLIENTENCODING", "PGOPTIONS")

# T4's pin set, verbatim from the ruling. `search_path` is the one deviation and it is
# printed as such: the probe's own tables have to be reachable, so the probe schema
# stands in for `public`.
PINS: dict[str, str] = {
    "TimeZone": "UTC",
    "DateStyle": "ISO, MDY",
    "IntervalStyle": "iso_8601",
    "bytea_output": "hex",
    "extra_float_digits": "3",
    "client_encoding": "UTF8",
    "search_path": f"pg_catalog, {PROBE_SCHEMA}",
}

HOSTILE_ENV = {
    "PGTZ": "America/Sao_Paulo",
    "PGDATESTYLE": "SQL, DMY",
    "PGOPTIONS": "-c extra_float_digits=-3",
}


def dsn() -> str:
    """The DSN, from the environment, with the compose default as the fallback."""
    return os.environ.get(DSN_ENV_VAR, DEFAULT_DSN)


def heading(text: str) -> None:
    print(f"\n=== {text} ===")


@contextmanager
def libpq_env(**overrides: str | None) -> Iterator[None]:
    """Set (or, with None, remove) libpq environment variables for the block.

    Used both to build a hostile environment and to guarantee a CLEAN one: if the
    operator already exports `PGTZ`, an unguarded baseline would silently measure
    their shell instead of the server's defaults.
    """
    saved = {name: os.environ.get(name) for name in overrides}
    try:
        for name, value in overrides.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        yield
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@contextmanager
def clean_env() -> Iterator[None]:
    """No rendering GUC arrives from the process environment."""
    with libpq_env(**dict.fromkeys(LIBPQ_RENDERING_ENV, None)):
        yield


def connect(*, search_path: bool = True) -> psycopg.Connection:
    """A connection in autocommit mode.

    Autocommit, because every transaction below is opened with the exact `BEGIN` the
    plan pins -- `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` -- rather than with
    a driver abstraction over it. What is measured is the SQL, not psycopg's opinion.
    """
    conn = psycopg.connect(dsn(), autocommit=True)
    if search_path:
        conn.execute(f"SET search_path TO {PROBE_SCHEMA}, pg_catalog")
    return conn


def one(conn: psycopg.Connection, sql: str, params: Sequence[Any] | None = None) -> Any:
    """The first column of the first row."""
    return conn.execute(sql, params).fetchone()[0]


def row(conn: psycopg.Connection, sql: str, params: Sequence[Any] | None = None) -> tuple:
    return conn.execute(sql, params).fetchone()


def rows(conn: psycopg.Connection, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
    return conn.execute(sql, params).fetchall()


def commit_after(delay: float, statements: Sequence[str]) -> threading.Thread:
    """Run `statements` on their own connection after `delay` seconds, and commit.

    A separate connection, because a second session is the whole point: every hazard
    below is about what one session sees while another one writes.
    """

    def run() -> None:
        time.sleep(delay)
        with clean_env(), connect() as writer:
            for statement in statements:
                writer.execute(statement)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


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
# 1. The container persistence matrix (opt-in: it destroys the database)
# --------------------------------------------------------------------------------


def compose(*args: str) -> str:
    """Run `docker compose ...` at the repo root and return its combined output."""
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return (result.stdout + result.stderr).strip()


MOUNT_FORMAT = (
    '{{range .Mounts}}{{if eq .Destination "/var/lib/postgresql/data"}}{{.Name}}{{end}}{{end}}'
)


def postgres_volume() -> str:
    """The name of the volume backing PGDATA right now, or '' if the container is gone."""
    result = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            MOUNT_FORMAT,
            "open-payments-lakehouse-postgres-1",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


def dangling_volume_count() -> int:
    result = subprocess.run(
        ["docker", "volume", "ls", "-qf", "dangling=true"],
        capture_output=True,
        text=True,
        check=False,
    )
    return len([line for line in result.stdout.splitlines() if line.strip()])


def wait_until_reachable(timeout: float = 90.0) -> float:
    """Seconds until a connection succeeds. Raises if it never does."""
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        try:
            with clean_env(), connect(search_path=False) as conn:
                one(conn, "SELECT 1")
            return time.monotonic() - started
        except psycopg.OperationalError:
            time.sleep(1.0)
    raise RuntimeError("Postgres never became reachable")


def persistence_marker_written() -> str:
    """Write the marker table and return the marker."""
    with clean_env(), connect(search_path=False) as conn:
        conn.execute("DROP TABLE IF EXISTS public.probe_persistence")
        conn.execute("CREATE TABLE public.probe_persistence (marker text NOT NULL)")
        conn.execute(
            "INSERT INTO public.probe_persistence VALUES (%s)", (f"written-at-{time.time():.3f}",)
        )
        return one(conn, "SELECT marker FROM public.probe_persistence")


def persistence_marker_read() -> str | None:
    """The marker, or None if the table does not exist."""
    with clean_env(), connect(search_path=False) as conn:
        exists = one(conn, "SELECT to_regclass('public.probe_persistence') IS NOT NULL")
        return one(conn, "SELECT marker FROM public.probe_persistence") if exists else None


def report_survival(label: str, expected: str) -> None:
    seconds = wait_until_reachable()
    found = persistence_marker_read()
    verdict = "SURVIVES" if found == expected else "DESTROYED"
    volume = postgres_volume() or "(none)"
    print(f"  {label:<28} {verdict:<10} (up after {seconds:.1f}s, volume {volume})")
    if found is not None and found != expected:
        print(f"    marker changed: {expected!r} -> {found!r}")


def measure_1_persistence() -> None:
    heading("1. THE CONTAINER PERSISTENCE MATRIX (destructive -- runs `down -v`)")
    print(f"  dangling volumes before:  {dangling_volume_count()}")
    print("  docker compose down -v ...")
    compose("down", "-v")
    print("  docker compose up -d ...")
    compose("up", "-d")
    wait_until_reachable()
    print(f"  volume after fresh up:    {postgres_volume()}")
    with clean_env(), connect(search_path=False) as conn:
        fresh_tables = rows(
            conn,
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')",
        )
    print(f"  `opl` on a fresh volume:  {len(fresh_tables)} user table(s) {fresh_tables}")

    marker = persistence_marker_written()
    print(f"  probe marker written:     {marker!r}")

    compose("restart")
    report_survival("docker compose restart:", marker)

    compose("stop")
    compose("start")
    report_survival("stop + start:", marker)

    before = postgres_volume()
    compose("down")
    compose("up", "-d")
    report_survival("down + up -d:", marker)
    print(f"    PGDATA volume before down: {before}")
    print(f"    PGDATA volume after up:    {postgres_volume()}")
    print(f"    dangling volumes now:      {dangling_volume_count()}  (T6: an orphaned PGDATA)")

    with clean_env(), connect(search_path=False) as conn:
        conn.execute("DROP TABLE IF EXISTS public.probe_persistence")


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
        txn_ts, clock_ts, snapshot, lsn = stamp
        print(f"  transaction_timestamp() (fixed at BEGIN):  {txn_ts.isoformat()}")
        print(f"  clock_timestamp() at the snapshot stmt:    {clock_ts.isoformat()}")
        gap_seconds = (clock_ts - txn_ts).total_seconds()
        print(f"  GAP BEGIN -> SNAPSHOT:                     {gap_seconds:.3f}s")
        print(f"  pg_current_snapshot():                     {snapshot}")
        print(f"  pg_current_wal_lsn():                      {lsn}")
        print(f"  rows committed inside the gap that ARE in the snapshot: {visible}")
        if visible and not stamp_first:
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

        extractor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        snapshot_1 = rows(extractor, "SELECT id, val FROM wm ORDER BY id")
        watermark = one(extractor, "SELECT max(updated_at) FROM wm")
        extractor.execute("COMMIT")
        print(f"  snapshot 1:               {snapshot_1}")
        print(f"  watermark recorded:       {watermark.isoformat()}  (== t2: {watermark == t2})")

        slow.execute("COMMIT")
        print("  slow txn COMMITS -- after the extract, carrying a stamp older than the watermark")

        missed = rows(
            extractor, "SELECT id, val, updated_at FROM wm WHERE updated_at > %s", (watermark,)
        )
        print(f"  WHERE updated_at > watermark: {missed}")
        time.sleep(1.0)
        again = rows(
            extractor, "SELECT id, val, updated_at FROM wm WHERE updated_at > %s", (watermark,)
        )
        print(f"  the same query one second later: {again}   <- and forever")

        extractor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        snapshot_2 = rows(extractor, "SELECT id, val FROM wm ORDER BY id")
        extractor.execute("COMMIT")
        print(f"  snapshot 2:               {snapshot_2}")
        changed = [a[0] for a, b in zip(snapshot_1, snapshot_2, strict=True) if a != b]
        print(f"  ids the SNAPSHOT DIFF catches: {changed}")
        print(f"  ids the WATERMARK catches:     {[r[0] for r in again]}")
        print("  `updated_at` orders by transaction START; visibility orders by COMMIT.")
    finally:
        for conn in (slow, fast, extractor):
            conn.close()


# --------------------------------------------------------------------------------
# 7. The GUC matrix
# --------------------------------------------------------------------------------

RENDER_SQL = (
    "SELECT ts::text, num::text, f::text, b::text, "
    "current_setting('TimeZone'), current_setting('DateStyle'), "
    "current_setting('extra_float_digits') FROM guc"
)


def build_guc_table(conn: psycopg.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS guc")
    conn.execute(
        "CREATE TABLE guc (ts timestamptz NOT NULL, num numeric(14,2) NOT NULL, "
        "trailing_zeros numeric NOT NULL, f float8 NOT NULL, b boolean NOT NULL, "
        "c char(5) NOT NULL)"
    )
    # `0.1 + 0.2` in SQL is NUMERIC arithmetic and lands on exactly 0.3 -- the float8
    # hazard needs the casts, or the measurement quietly stops being about float8.
    conn.execute(
        "INSERT INTO guc VALUES "
        "(timestamptz '2026-08-03 17:23:01.123456+00', 1234.50, 10.500, "
        "0.1::float8 + 0.2::float8, true, 'ab')"
    )


def render_under(label: str, **env: str | None) -> tuple[str, ...]:
    with libpq_env(**dict.fromkeys(LIBPQ_RENDERING_ENV, None)), libpq_env(**env):
        with connect() as conn:
            rendered = row(conn, RENDER_SQL)
    ts, num, f, b, tz, ds, efd = rendered
    print(f"  {label}")
    print(
        f"      GUCs seen by the session: TimeZone={tz}  DateStyle={ds}  extra_float_digits={efd}"
    )
    print(f"      timestamptz::text = {ts}")
    print(f"      numeric::text     = {num}      float8::text = {f}      boolean::text = {b}")
    return rendered


def measure_7_guc_matrix() -> None:
    heading("7. THE GUC MATRIX -- `col::text` is session-dependent, set from the SHELL")
    with clean_env(), connect() as setup:
        build_guc_table(setup)
    print(
        "  one row: ts=2026-08-03 17:23:01.123456+00, numeric(14,2)=1234.50, "
        "float8=0.1::float8+0.2::float8, bool=true"
    )
    baseline = render_under("baseline (no PG* variables exported)")
    render_under("PGTZ=America/Sao_Paulo", PGTZ="America/Sao_Paulo")
    dmy = render_under("PGDATESTYLE='SQL, DMY'", PGDATESTYLE="SQL, DMY")
    render_under("PGOPTIONS='-c extra_float_digits=0'", PGOPTIONS="-c extra_float_digits=0")
    hostile = render_under("ALL THREE HOSTILE AT ONCE", **HOSTILE_ENV)
    print(f"  baseline and hostile render the SAME bytes: {baseline[:4] == hostile[:4]}")

    heading("7b. THE MISPARSE IS SILENT AND ASYMMETRIC")
    written = dmy[0]
    with clean_env(), connect() as reader:
        reparsed = one(reader, "SELECT (%s::timestamptz)::date::text", (written,))
        original = one(reader, "SELECT (ts)::date::text FROM guc")
    print(f"  a writer at DateStyle='SQL, DMY' renders: {written}")
    print(f"  a reader at DateStyle='ISO, MDY' parses:  {reparsed}")
    print(f"  the value actually stored is:             {original}")
    print(f"  ROUND-TRIP: {'ok' if reparsed == original else 'BROKEN, and nothing raised'}")

    heading("7c. A `SET` + READ-BACK PIN DEFEATS PGOPTIONS")
    with libpq_env(**dict.fromkeys(LIBPQ_RENDERING_ENV, None)), libpq_env(**HOSTILE_ENV):
        with connect() as conn:
            before = {name: one(conn, f"SHOW {name}") for name in PINS}
            # set_config(), not `SET`: `SET` is a utility statement and takes no
            # parameter, so a literal-interpolated pin would be the one place in the
            # extractor where a GUC value is concatenated into SQL.
            for name, value in PINS.items():
                conn.execute("SELECT set_config(%s, %s, false)", (name, value))
            after = {name: one(conn, f"SHOW {name}") for name in PINS}
            pinned = row(conn, RENDER_SQL)
    for name, wanted in PINS.items():
        got = after[name]
        print(f"  {name:<18} startup={before[name]!r:<28} pinned={got!r:<20} ok={got == wanted}")
    print(f"  rendered under the pin, inside the hostile environment: {pinned[:4]}")
    print(f"  identical to the clean baseline: {pinned[:4] == baseline[:4]}")
    print("  NOTE: `search_path` deviates from T4's 'pg_catalog, public' only because the")
    print("  probe's tables live in their own schema. Every other pin is the ruling verbatim.")


# --------------------------------------------------------------------------------
# 8. `::text` is the CAST, not the type's output function
# --------------------------------------------------------------------------------


def copy_out(conn: psycopg.Connection, sql: str) -> str:
    """`COPY ... TO STDOUT` -- the type's OUTPUT FUNCTION, which is what a dump writes."""
    chunks: list[bytes] = []
    with conn.cursor().copy(sql) as copy:
        for chunk in copy:
            chunks.append(bytes(chunk))
    return b"".join(chunks).decode("utf-8").rstrip("\n")


def measure_8_cast_versus_output_function() -> None:
    heading("8. `col::text` IS THE CAST, NOT THE OUTPUT FUNCTION")
    with clean_env(), connect() as conn:
        cast_b, cast_c, cast_ts = row(conn, "SELECT b::text, c::text, ts::text FROM guc")
        copied = copy_out(conn, "COPY (SELECT b, c, ts FROM guc) TO STDOUT")
        out_b, out_c, out_ts = copied.split("\t")
        typoutput = rows(
            conn,
            "SELECT t.typname, p.proname FROM pg_type t JOIN pg_proc p ON p.oid = t.typoutput "
            "WHERE t.typname IN ('bool', 'bpchar')",
        )
    print(f"  boolean   cast={cast_b!r:<10} output function={out_b!r}")
    print(f"  char(5)   cast={cast_c!r:<10} output function={out_c!r}   <- the CAST STRIPS PADDING")
    print(f"  timestamptz cast={cast_ts!r}  output function={out_ts!r}  agree={cast_ts == out_ts}")
    print(f"  the output functions themselves: {typoutput}")
    print("  => 'canonical representation' is false for bool and char(n). char(n) is excluded")
    print("     from the seeded schema and the boolean spelling is pinned in the contract (T4).")


# --------------------------------------------------------------------------------
# 9. The watermark's other two blind spots, and the four time functions
# --------------------------------------------------------------------------------


def measure_9_watermark_blind_spots() -> None:
    heading("9. `DEFAULT now()` ON UPDATE, A HARD DELETE, AND THE FOUR TIME FUNCTIONS")
    with clean_env(), connect() as conn:
        conn.execute("DROP TABLE IF EXISTS blind")
        conn.execute(
            "CREATE TABLE blind (id int PRIMARY KEY, val text NOT NULL, "
            "updated_at timestamptz NOT NULL DEFAULT now())"
        )
        conn.execute("INSERT INTO blind SELECT g, 'v0' FROM generate_series(1, 5) AS g")
        watermark = one(conn, "SELECT max(updated_at) FROM blind")
        before = one(conn, "SELECT updated_at FROM blind WHERE id = 1")
        time.sleep(1.0)
        conn.execute("UPDATE blind SET val = 'v1' WHERE id = 1")
        after = one(conn, "SELECT updated_at FROM blind WHERE id = 1")
        print(f"  updated_at before UPDATE: {before.isoformat()}")
        print(f"  updated_at after  UPDATE: {after.isoformat()}")
        print(
            f"  the DEFAULT fired on UPDATE: {after != before}   <- a DEFAULT is INSERT-time only"
        )
        moved = rows(conn, "SELECT id FROM blind WHERE updated_at > %s", (watermark,))
        print(f"  rows a watermark run sees after that UPDATE: {moved}")

        count_before = one(conn, "SELECT count(*) FROM blind")
        conn.execute("DELETE FROM blind WHERE id = 5")
        count_after = one(conn, "SELECT count(*) FROM blind")
        after_delete = rows(conn, "SELECT id FROM blind WHERE updated_at > %s", (watermark,))
        print(f"  rows: {count_before} -> {count_after} after a hard DELETE")
        print(
            f"  a watermark run after the DELETE sees: {after_delete}  <- a DELETE leaves NOTHING"
        )

        conn.execute("BEGIN")
        first = row(
            conn,
            "SELECT now(), transaction_timestamp(), statement_timestamp(), clock_timestamp()",
        )
        time.sleep(0.5)
        second = row(
            conn,
            "SELECT now(), transaction_timestamp(), statement_timestamp(), clock_timestamp()",
        )
        conn.execute("COMMIT")
        names = ("now()", "transaction_timestamp()", "statement_timestamp()", "clock_timestamp()")
        for name, a, b in zip(names, first, second, strict=True):
            print(f"  {name:<26} stmt1={a.isoformat()}  moved by stmt2: {b != a}")
        print(
            f"  now() IS transaction_timestamp(): {first[0] == first[1] and second[0] == second[1]}"
        )

    heading("9b. NUMERIC THROUGH `::text`")
    with clean_env(), connect() as conn:
        for expression in (
            "10.500::numeric",
            "(SELECT trailing_zeros FROM guc)",
            "(SELECT num FROM guc)",
            "(1.0 * 1.0)::numeric",
            "'1e3'::numeric",
            "1234.5::numeric(14,2)",
        ):
            print(f"  {expression:<28} ::text -> {one(conn, f'SELECT ({expression})::text')!r}")


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


# --------------------------------------------------------------------------------


def setup_schema() -> None:
    with clean_env(), connect(search_path=False) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {PROBE_SCHEMA} CASCADE")
        conn.execute(f"CREATE SCHEMA {PROBE_SCHEMA}")


def teardown_schema() -> None:
    heading("CLEANUP -- the probe leaves nothing behind")
    with clean_env(), connect(search_path=False) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {PROBE_SCHEMA} CASCADE")
        conn.execute("DROP TABLE IF EXISTS public.probe_persistence")
        left = rows(
            conn,
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')",
        )
        schemas = rows(conn, "SELECT nspname FROM pg_namespace WHERE nspname = %s", (PROBE_SCHEMA,))
    print(f"  probe schema still present: {bool(schemas)}")
    print(f"  user tables left in `opl`:  {len(left)} {left}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="F-DB Task 0 Postgres probe. See the module docstring."
    )
    parser.add_argument(
        "--persistence",
        action="store_true",
        help="run the container persistence matrix. DESTRUCTIVE: it runs `docker compose down -v`.",
    )
    args = parser.parse_args(argv)

    print("F-DB Task 0 probe -- local throwaway Postgres container, no secret involved.")
    print(f"dsn: {dsn().replace('password=opl', 'password=***')}")
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
        measure_7_guc_matrix()
        measure_8_cast_versus_output_function()
        measure_9_watermark_blind_spots()
        measure_10_further_t3_claims()
    finally:
        teardown_schema()
    return 0


if __name__ == "__main__":
    sys.exit(main())
