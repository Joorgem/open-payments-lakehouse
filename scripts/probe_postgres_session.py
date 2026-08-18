"""Shared session control for the F-DB Task 0 probes.

WHY THIS IS ITS OWN MODULE. Three of Task 0's measurements are about what a SESSION
is -- which environment variables reached libpq, which GUCs the server ended up with,
which transaction a statement ran in -- and getting that wrong silently invalidates
every number downstream. A baseline measured in a shell that already exports `PGTZ`
is not a baseline. So the connection helpers live in one place, `clean_env()` is the
default rather than an option, and every probe imports them rather than re-deriving
them.

`connect()` is AUTOCOMMIT. Every transaction in these probes is opened with the exact
`BEGIN` the plan pins -- `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` -- rather
than with a driver abstraction over it. What is measured is the SQL, not psycopg's
opinion of it.

THE DSN COMES FROM THE ENVIRONMENT, with the docker-compose defaults as the fallback
(plan sec. 7). Nothing here reads a secret and nothing here should ever grow one: the
credentials in the fallback are the throwaway container's, already committed in
`docker-compose.yml` and `tests/integration/test_postgres.py`.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import psycopg

DSN_ENV_VAR = "OPL_POSTGRES_DSN"
# The compose defaults. `docker-compose.yml:5-8` and `tests/integration/test_postgres.py:9`
# already carry these for a throwaway local container; this is the third and last copy.
DEFAULT_DSN = "host=localhost port=5433 dbname=opl user=opl password=opl"

PROBE_SCHEMA = "probe_f_db"

# The environment variables libpq turns into startup options with no code change.
LIBPQ_RENDERING_ENV = ("PGTZ", "PGDATESTYLE", "PGCLIENTENCODING", "PGOPTIONS")

# libpq's keyword/value form lets a VALUE be single-quoted, and lets a backslash escape a
# quote or a backslash inside one. Either means whitespace is no longer a token boundary,
# so `str.split()` is parsing something other than the DSN it was handed. `redacted_dsn`
# refuses both rather than guessing where a value ends.
_QUOTING_CHARACTERS = ("'", "\\")


def dsn() -> str:
    """The DSN, from the environment, with the compose default as the fallback."""
    return os.environ.get(DSN_ENV_VAR, DEFAULT_DSN)


def redacted_dsn(value: str | None = None) -> str:
    """`value` with every `password=` token blanked, whatever the password is.

    NOT a replace of the compose literal. This printed
    `dsn().replace("password=opl", "password=***")` until the Task 0 review, which is a
    redaction keyed to ONE password while `dsn()` reads `OPL_POSTGRES_DSN` by design -- so
    any DSN whose password was not `opl` printed it in clear, onto stdout and from there
    into whatever evidence document the output was pasted into. Master protocol §4.9 forbids
    a secret in a source file; a redaction that only works on the throwaway one is the same
    hole with a lid on it.

    Splitting on whitespace is enough for UNQUOTED libpq keyword/value form, which is what
    `DEFAULT_DSN` is and what the compose stack hands out. Two shapes are not that, and
    both are REFUSED rather than passed through half-redacted -- returning a string this
    function cannot promise it cleaned is the failure it exists to prevent.

      * A URI DSN (`postgresql://user:pw@host/db`), whose password is not in a token at all.
      * A QUOTED value, which libpq allows and which may contain whitespace. This one had
        teeth: `password='a b' host=localhost` splits into `["password='a", "b'", ...]`,
        only the first token starts with `password=`, and `b'` -- half the password --
        went to stdout in clear, which is exactly the leak the Task 0 review closed for
        the other half. A backslash is refused alongside the quote because it is how a
        quote is escaped inside one, so its presence means the same thing: the token
        boundaries are not where whitespace is.
    """
    raw = dsn() if value is None else value
    if "://" in raw:
        return f"<URI DSN, not rendered: {DSN_ENV_VAR} is set to a URI>"
    if any(character in raw for character in _QUOTING_CHARACTERS):
        return f"<quoted DSN, not rendered: {DSN_ENV_VAR} quotes or escapes a value>"
    return " ".join(
        "password=***" if token.lower().startswith("password=") else token
        for token in raw.split()
    )


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
