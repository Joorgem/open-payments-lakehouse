"""Seed the merchant registry, and apply the mutation that sits between the snapshots.

WHAT THIS OWNS. Every number in F-DB's headline is produced here: the 1,088 merchants
snapshot 1 sees, the 32/48/24/16 that change between the snapshots, and the 8 rows whose
transaction is held open across snapshot 1's read. `merchant_population.py` owns the
arithmetic and imports no psycopg; this module owns the SQL, the session and the clock.

THE TAUTOLOGY, DECLARED HERE AND NOT ONLY IN THE PLAN. Five of the six change classes are
CHOSEN. The deletes are chosen, the silent updates are chosen, and so is the number 8.
What is not chosen is that a row stamped before the watermark and committed after it is
unreachable by `WHERE updated_at > watermark` forever -- `updated_at` orders by
transaction START, visibility orders by transaction COMMIT, and no care on the source side
closes that gap. This module proves it the hard way: the table carries the BEFORE UPDATE
trigger that IS the correct fix for the other two watermark classes, and the out-of-order
row escapes anyway.

THREE SQL STATEMENTS FOR SIX CLASSES. `_apply` dispatches on the presence verb -- one
INSERT, one UPDATE, one DELETE -- and every other difference between the classes is a
boolean read off `ChangeClass`: whether the trigger is armed for the write, whether the
transaction is held open across snapshot 1's read, whether it commits before that read,
whether the payload changes. `_phases` derives the run order from those booleans rather
than from a list of names, so there is no branch per class anywhere and adding a seventh
would add no code.

IDEMPOTENT AGAINST A POPULATED DATABASE. `postgres:16` declares an anonymous volume;
measured (evidence §0.4), `restart` and `stop`+`start` preserve it and only `down`+`up`
destroys it, so the common path meets yesterday's data. `seed` is DROP -> CREATE -> INSERT
inside ONE transaction, and re-running it reproduces the same rows byte for byte -- which
is not asserted, it is MEASURED: `seed` prints a sha256 over the whole table rendered under
the pinned GUCs, and a re-run printing the same digest is the verification.

THE GUCs ARE PINNED AND READ BACK (T4). libpq turns `PGTZ`, `PGDATESTYLE`,
`PGCLIENTENCODING` and `PGOPTIONS` into startup options with no code change, and a `SET`
that is not read back is not a pin. Every value written by this module is handed to
Postgres as TEXT with an explicit cast, so the pin covers the write path and not only the
read path: a writer at `DateStyle='SQL, DMY'` and a reader at `'ISO, MDY'` disagree about
`03/08/2026` silently, and this is the only place in the phase that writes.

THE DSN COMES FROM THE ENVIRONMENT. `probe_postgres_session.dsn()` already reads
`OPL_POSTGRES_DSN` with the compose default as its fallback, and its `redacted_dsn()`
blanks every `password=` token rather than one literal. Reused rather than re-spelled.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
from merchant_population import (
    CHANGE_CLASSES,
    SEED_UPDATED_CEILING,
    SNAPSHOT_1_ROWS,
    ChangeClass,
    Merchant,
    Plan,
    Presence,
    build_plan,
    mutated,
)
from probe_postgres_session import dsn, redacted_dsn
from psycopg import sql

TABLE = "merchant"
TRIGGER_FUNCTION = "merchant_touch_updated_at"
DEFAULT_SCHEMA = "public"

# T4's pin set, verbatim. `search_path` is a pin and not a dependency: every statement
# below names its schema, so a hostile `search_path` cannot redirect one.
PINS: dict[str, str] = {
    "TimeZone": "UTC",
    "DateStyle": "ISO, MDY",
    "IntervalStyle": "iso_8601",
    "bytea_output": "hex",
    "extra_float_digits": "3",
    "client_encoding": "UTF8",
    "search_path": "pg_catalog, public",
}

# Asserted, never assumed. A `LATIN1` database was created on this very container during
# Task 0, and `SQL_ASCII` accepts arbitrary bytes as `text` -- "a Postgres database is
# UTF-8" is false as a general statement and true of this one only if it is checked.
REQUIRED_SERVER_ENCODING = "UTF8"

# The payload columns, in plan §4 order. `updated_at` is deliberately absent: the trigger
# owns it, so no UPDATE this module issues ever names it.
PAYLOAD_COLUMNS = (
    "cnpj",
    "legal_name",
    "trade_name",
    "status",
    "mcc",
    "settlement_account",
    "risk_tier",
    "credit_limit",
    "onboarded_on",
)

_DDL = """
CREATE SCHEMA IF NOT EXISTS {schema};
DROP TABLE IF EXISTS {table} CASCADE;
CREATE TABLE {table} (
    merchant_id        uuid          PRIMARY KEY,
    cnpj               text          NOT NULL,
    legal_name         text          NOT NULL,
    trade_name         text,
    status             text          NOT NULL,
    mcc                text          NOT NULL,
    settlement_account text          NOT NULL,
    risk_tier          text          NOT NULL,
    credit_limit       numeric(14,2) NOT NULL,
    onboarded_on       date          NOT NULL,
    updated_at         timestamptz   NOT NULL DEFAULT now()
);
CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at := now(); RETURN NEW; END;
$$;
CREATE TRIGGER {trigger} BEFORE UPDATE ON {table}
    FOR EACH ROW EXECUTE FUNCTION {function}();
"""

_INSERT = """
INSERT INTO {table} (merchant_id, cnpj, legal_name, trade_name, status, mcc,
                     settlement_account, risk_tier, credit_limit, onboarded_on, updated_at)
VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s::numeric(14,2), %s::date,
        coalesce(%s::timestamptz, now()))
"""

_UPDATE = """
UPDATE {table} SET cnpj = %s, legal_name = %s, trade_name = %s, status = %s, mcc = %s,
       settlement_account = %s, risk_tier = %s, credit_limit = %s::numeric(14,2),
       onboarded_on = %s::date
WHERE merchant_id = %s::uuid
"""

_DELETE = "DELETE FROM {table} WHERE merchant_id = %s::uuid"

# A sha256 over the whole table, ordered by `merchant_id::text COLLATE "C"` (T11: never by
# a human-readable name, and never by a collation the container and CI could disagree
# about). Rendered by the DATABASE under the pinned GUCs, so this digest is exactly the
# bytes an extractor would land.
_DIGEST = """
SELECT encode(sha256(convert_to(coalesce(
    string_agg({expression}::text, E'\\n' ORDER BY merchant_id::text COLLATE "C"), ''), 'UTF8')),
    'hex') FROM {table}
"""


class Refusal(RuntimeError):
    """A precondition this module will not proceed without."""


def _refuse_unless(condition: bool, message: str) -> None:
    if not condition:
        raise Refusal(message)


# --------------------------------------------------------------------------------
# The session
# --------------------------------------------------------------------------------


def pin_rendering_gucs(conn: psycopg.Connection) -> dict[str, str]:
    """Pin T4's seven GUCs and READ THEM BACK, refusing on any disagreement.

    `set_config()` rather than `SET`, because `SET` is a utility statement that takes no
    parameter -- a literal-interpolated pin would be the one place here where a GUC value
    is concatenated into SQL.
    """
    for name, value in PINS.items():
        conn.execute("SELECT set_config(%s, %s, false)", (name, value))
    read_back = {name: _one(conn, sql.SQL("SHOW {}").format(sql.Identifier(name))) for name in PINS}
    wrong = {name: got for name, got in read_back.items() if got != PINS[name]}
    _refuse_unless(not wrong, f"the rendering GUCs did not read back as pinned: {wrong}")
    encoding = _one(conn, "SHOW server_encoding")
    _refuse_unless(
        encoding == REQUIRED_SERVER_ENCODING,
        f"server_encoding is {encoding!r}, not {REQUIRED_SERVER_ENCODING!r}",
    )
    return read_back


def connect() -> psycopg.Connection:
    """An autocommit connection with the rendering GUCs pinned as the session's first act.

    Autocommit, because every transaction below is opened with the exact SQL it needs
    rather than with psycopg's opinion of it -- the same reason the Task 0 probes give.
    """
    conn = psycopg.connect(dsn(), autocommit=True)
    pin_rendering_gucs(conn)
    return conn


def _one(conn: psycopg.Connection, statement: Any, params: Sequence[Any] | None = None) -> Any:
    return conn.execute(statement, params).fetchone()[0]


def _table(schema: str) -> sql.Composed:
    return sql.SQL("{}").format(sql.Identifier(schema, TABLE))


def _statement(template: str, _schema: str, **extra: sql.Composable) -> sql.Composed:
    """`template` with `{table}` bound to `_schema.merchant`, plus any extra placeholders.

    The leading underscore is not decoration: `_DDL` also needs a bare `{schema}`
    placeholder, and a plain `schema` parameter would collide with it.
    """
    return sql.SQL(template).format(table=_table(_schema), **extra)


@contextmanager
def _trigger_armed(conn: psycopg.Connection, *, armed: bool):
    """Arm or disarm the BEFORE UPDATE trigger for the block.

    `session_replication_role = 'replica'` is what a logical-replication apply worker runs
    under, so the disarmed path is a real operational write path rather than a simulation
    -- and it is session-scoped, unlike `ALTER TABLE ... DISABLE TRIGGER`, which would take
    an ACCESS EXCLUSIVE lock on the table the extractor is about to read.
    """
    role = "origin" if armed else "replica"
    conn.execute("SELECT set_config('session_replication_role', %s, false)", (role,))
    try:
        yield
    finally:
        conn.execute("SELECT set_config('session_replication_role', 'origin', false)")


# --------------------------------------------------------------------------------
# Rendering a merchant as parameters -- text, always, with the cast in the SQL
# --------------------------------------------------------------------------------


def _payload_params(row: Merchant) -> list[str | None]:
    """The nine payload columns as text. `None` stays `None`; `''` stays `''`."""
    return [
        row.cnpj,
        row.legal_name,
        row.trade_name,
        row.status,
        row.mcc,
        row.settlement_account,
        row.risk_tier,
        str(row.credit_limit),
        row.onboarded_on.isoformat(),
    ]


def _insert_params(row: Merchant, *, stamp_now: bool) -> list[str | None]:
    """Insert parameters; `updated_at` is NULL when the server clock should stamp it.

    `coalesce(%s::timestamptz, now())` in the statement means the seeded rows carry their
    committed literal and the between-snapshot inserts carry a real commit-time stamp,
    from ONE statement rather than two.
    """
    stamp = None if stamp_now else row.updated_at.isoformat()
    return [row.merchant_id, *_payload_params(row), stamp]


def _update_params(row: Merchant) -> list[str | None]:
    return [*_payload_params(row), row.merchant_id]


def _execute_many(conn: psycopg.Connection, statement: sql.Composed, params: list[list]) -> int:
    with conn.cursor() as cur:
        cur.executemany(statement, params)
        return cur.rowcount


def _updated_at(conn: psycopg.Connection, schema: str, ids: Sequence[str]) -> dict[str, datetime]:
    statement = _statement(
        "SELECT merchant_id::text, updated_at FROM {table} WHERE merchant_id = ANY(%s::uuid[])",
        schema,
    )
    return dict(conn.execute(statement, (list(ids),)).fetchall())


# --------------------------------------------------------------------------------
# One class, applied. Three statements cover all six.
# --------------------------------------------------------------------------------


def _apply_update(
    conn: psycopg.Connection, klass: ChangeClass, rows: Sequence[Merchant], schema: str
) -> dict[str, int]:
    """The one UPDATE. The class contributes two booleans and no code path.

    The before/after read is a CONTROL IN BOTH DIRECTIONS: the armed classes must move
    `updated_at` on every row and the disarmed one must move it on none. A silent-update
    class that quietly started stamping would otherwise vanish from the headline with no
    test going red, and the assertion is one expression because the expectation is the
    boolean itself.
    """
    ids = [row.merchant_id for row in rows]
    before = _updated_at(conn, schema, ids)
    payload = [mutated(row) if klass.payload_changed else row for row in rows]
    with _trigger_armed(conn, armed=klass.moves_updated_at):
        params = [_update_params(row) for row in payload]
        affected = _execute_many(conn, _statement(_UPDATE, schema), params)
    after = _updated_at(conn, schema, ids)
    moved = sum(1 for key in ids if after[key] != before[key])
    expected = len(ids) if klass.moves_updated_at else 0
    _refuse_unless(
        moved == expected,
        f"{klass.name}: {moved} of {len(ids)} rows moved updated_at, expected {expected}. "
        "The trigger is the mechanism the whole watermark argument rests on.",
    )
    return {"rows": affected, "moved_updated_at": moved}


def _apply(conn: psycopg.Connection, klass: ChangeClass, plan: Plan, schema: str) -> dict[str, int]:
    """`klass`, applied. One statement per presence verb, for all six classes."""
    rows = plan.rows_for(klass.name)
    _refuse_unless(
        len(rows) == klass.count,
        f"{klass.name} holds {len(rows)} rows, not the published {klass.count}",
    )
    if klass.presence is Presence.INSERT:
        params = [_insert_params(row, stamp_now=klass.moves_updated_at) for row in rows]
        return {"rows": _execute_many(conn, _statement(_INSERT, schema), params)}
    if klass.presence is Presence.DELETE:
        params = [[row.merchant_id] for row in rows]
        return {"rows": _execute_many(conn, _statement(_DELETE, schema), params)}
    return _apply_update(conn, klass, rows, schema)


# --------------------------------------------------------------------------------
# Seed
# --------------------------------------------------------------------------------


def table_digest(conn: psycopg.Connection, schema: str, *, payload_only: bool = False) -> str:
    """sha256 over the table as the database renders it under the pinned GUCs."""
    columns = sql.SQL(", ").join(sql.Identifier(name) for name in ("merchant_id", *PAYLOAD_COLUMNS))
    expression = (
        sql.SQL("ROW({})").format(columns) if payload_only else sql.Identifier(TABLE)
    )
    return _one(conn, _statement(_DIGEST, schema, expression=expression))


def seed(conn: psycopg.Connection, plan: Plan, schema: str) -> dict[str, Any]:
    """DROP -> CREATE -> INSERT, in one transaction, against a possibly populated database."""
    conn.execute("BEGIN")
    try:
        conn.execute(
            _statement(
                _DDL,
                schema,
                schema=sql.Identifier(schema),
                function=sql.Identifier(schema, TRIGGER_FUNCTION),
                trigger=sql.Identifier(TRIGGER_FUNCTION),
            )
        )
        inserted = _execute_many(
            conn,
            _statement(_INSERT, schema),
            [_insert_params(row, stamp_now=False) for row in plan.seed],
        )
        _refuse_unless(
            inserted == SNAPSHOT_1_ROWS,
            f"seeded {inserted} rows, not the published {SNAPSHOT_1_ROWS}",
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return {"rows": inserted, **census(conn, schema)}


def census(conn: psycopg.Connection, schema: str) -> dict[str, Any]:
    """What the table holds now, and the two digests a re-run is compared against."""
    counts = conn.execute(
        _statement(
            "SELECT count(*), count(DISTINCT left(cnpj, 8)), count(*) FILTER "
            "(WHERE trade_name IS NULL), count(*) FILTER (WHERE trade_name = ''), "
            "max(updated_at) FROM {table}",
            schema,
        )
    ).fetchone()
    return {
        "merchants": counts[0],
        "distinct_cnpj_roots": counts[1],
        "trade_name_null": counts[2],
        "trade_name_empty": counts[3],
        "max_updated_at": counts[4],
        "digest": table_digest(conn, schema),
        "payload_digest": table_digest(conn, schema, payload_only=True),
    }


# --------------------------------------------------------------------------------
# Mutate -- including the transaction held open across snapshot 1's read
# --------------------------------------------------------------------------------


def _refuse_unless_seeded(conn: psycopg.Connection, schema: str) -> None:
    """`mutate` is not idempotent and says so, rather than double-mutating in silence."""
    rows = _one(conn, _statement("SELECT count(*) FROM {table}", schema))
    _refuse_unless(
        rows == SNAPSHOT_1_ROWS,
        f"the table holds {rows} rows, not the seeded {SNAPSHOT_1_ROWS}. Run `seed` first.",
    )
    latest = _one(conn, _statement("SELECT max(updated_at) FROM {table}", schema))
    _refuse_unless(
        latest <= SEED_UPDATED_CEILING,
        f"max(updated_at) is {latest}, after the seed window closes at {SEED_UPDATED_CEILING}. "
        "Something has already written to this table; re-run `seed`.",
    )


def _batch(
    conn: psycopg.Connection, classes: Sequence[ChangeClass], plan: Plan, schema: str,
    applied: dict[str, dict],
) -> datetime | None:
    """Apply `classes` on `conn` and return the latest stamp they left, if any."""
    ids: list[str] = []
    for klass in classes:
        applied[klass.name] = _apply(conn, klass, plan, schema)
        ids += [row.merchant_id for row in plan.rows_for(klass.name)]
    stamps = _updated_at(conn, schema, ids).values()
    return max(stamps) if stamps else None


def _phases(plan: Plan) -> tuple[list[ChangeClass], list[ChangeClass], list[ChangeClass]]:
    """The three phases, DERIVED from the class booleans rather than from a list of names.

    Held open, committed before snapshot 1, everything else. A class is placed by what it
    declares, so the ordering cannot drift out of step with the published table -- and the
    two refusals below are the ones that would make the headline a fabrication if either
    phase were empty.
    """
    held = [klass for klass in CHANGE_CLASSES if klass.held_open]
    early = [klass for klass in CHANGE_CLASSES if klass.before_snapshot_1]
    rest = [klass for klass in CHANGE_CLASSES if not (klass.held_open or klass.before_snapshot_1)]
    _refuse_unless(bool(held), "no class is held open, so there is no out-of-order commit")
    _refuse_unless(bool(early), "no class commits before snapshot 1, so the watermark stays t1")
    _refuse_unless(bool(plan.seed), "the plan carries no rows")
    return held, early, rest


def mutate(conn: psycopg.Connection, plan: Plan, schema: str, release: Callable[[], None]) -> dict:
    """The whole between-snapshots mutation, with `release()` standing in for snapshot 1.

    ORDER IS THE MECHANISM. The slow transaction stamps t1 and stays open; a touch that
    fires the trigger and changes nothing else commits at t2 > t1, so the watermark
    snapshot 1 records is t2 rather than t1; `release()` is where snapshot 1 is read; and
    only then does the slow transaction commit. `WHERE updated_at > t2` cannot return those
    rows -- not now, not ever.
    """
    _refuse_unless_seeded(conn, schema)
    held, early, rest = _phases(plan)
    applied: dict[str, dict] = {}
    slow = connect()
    try:
        slow.execute("BEGIN")
        t1 = _batch(slow, held, plan, schema, applied)
        t2 = _batch(conn, early, plan, schema, applied)
        _refuse_unless(
            t2 > t1,
            f"the watermark-advancing write stamped {t2}, not after the held-open {t1}. "
            "Without t2 > t1 the out-of-order miss would be a fabrication.",
        )
        release()
        slow.execute("COMMIT")
    finally:
        slow.close()
    _batch(conn, rest, plan, schema, applied)
    return {"t1": t1, "t2": t2, "classes": applied, **census(conn, schema)}


# --------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------


def _release_on_file(path: Path, poll_seconds: float = 0.25) -> Callable[[], None]:
    def wait() -> None:
        print(f"  holding the out-of-order transaction open; waiting for {path}", flush=True)
        while not path.exists():
            time.sleep(poll_seconds)

    return wait


def _release_after(seconds: float) -> Callable[[], None]:
    def wait() -> None:
        print(f"  holding the out-of-order transaction open for {seconds}s", flush=True)
        time.sleep(seconds)

    return wait


def _report(title: str, values: dict[str, Any]) -> None:
    print(f"\n=== {title} ===")
    for key, value in values.items():
        if isinstance(value, dict):
            print(f"  {key}:")
            for inner, count in value.items():
                print(f"      {inner:<32} {count}")
        else:
            print(f"  {key:<24} {value}")


def _predictions() -> None:
    print("\n=== the published population, before the run ===")
    for klass in CHANGE_CLASSES:
        print(
            f"  {klass.name:<30} {klass.count:>5}  {klass.presence.value:<7}"
            f" moves_updated_at={klass.moves_updated_at!s:<6}"
            f" held_open={klass.held_open!s:<6}"
            f" before_snapshot_1={klass.before_snapshot_1!s:<6}"
            f" payload_changed={klass.payload_changed}"
        )


def _parse(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("seed", "mutate", "report"))
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    release = parser.add_mutually_exclusive_group()
    release.add_argument("--release-on", type=Path, help="wait for this file, then commit")
    release.add_argument("--release-after", type=float, help="wait this many seconds, then commit")
    return parser.parse_args(argv)


def _release_from(args: argparse.Namespace) -> Callable[[], None]:
    if args.release_on is not None:
        return _release_on_file(args.release_on)
    if args.release_after is not None:
        return _release_after(args.release_after)
    raise Refusal(
        "`mutate` needs --release-on PATH or --release-after SECONDS. Without one the "
        "held-open transaction commits before snapshot 1 is read and the out-of-order "
        "class silently stops existing -- which is the one number this phase does not author."
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse(argv)
    print(f"  dsn: {redacted_dsn()}")
    plan = build_plan()
    with connect() as conn:
        _report("the pinned rendering GUCs, read back", pin_rendering_gucs(conn))
        if args.command == "report":
            _report("census", census(conn, args.schema))
            return 0
        if args.command == "seed":
            _predictions()
            _report("seeded", seed(conn, plan, args.schema))
            return 0
        _report("mutated", mutate(conn, plan, args.schema, _release_from(args)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
