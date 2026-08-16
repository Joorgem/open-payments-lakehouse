# src/opl/extraction/postgres_source.py
"""Postgres source knowledge: the one transaction this snapshot may be read in, and the
validation of what it answers. Builds the statements and reads the results; the
CONNECTION is the caller's.

NO psycopg, NO pyspark, NO HTTP CLIENT, NO `os`, NO `open`. This module executes no I/O of
its own: every statement goes through a connection object handed in, exactly as
`opl.extraction.ptax_source` takes a `fetch` callable, so every test of it is hermetic and
needs no database. `tests/test_postgres_source.py` holds that down over the AST, as the
twin of the sweep `tests/test_ptax_source.py` runs over the PTAX layer.

THIS SOURCE NEEDS LESS THAN PTAX DOES, AND THE DIFFERENCE IS DELIBERATE. That module is
585 lines because the BCB series forced them: a retry ladder, a publication-instant parser
with five observed fractional widths, a spread bound derived from 42 years of rows, a
range-attribution refusal. NONE of those failure modes exists here. There is no network, so
there is nothing to retry and no status code to misread. There is no publication instant --
the only instant is one this module's own transaction stamps. There is no range to
attribute, because a snapshot is the whole table at one instant by construction. Every
guard below is re-derived from a failure this source can actually produce, and a guard
pasted from PTAX would be a control that cannot fire.

--- WHAT THE TRANSACTION IS FOR, AND WHY IT IS NOT WHAT REVISION 1 SAID ----------------

A single large `SELECT` under `READ COMMITTED` does NOT smear: `READ COMMITTED` takes a new
snapshot per STATEMENT, and one `SELECT` is one statement (measured -- a 4.0 s scan with a
writer committing `UPDATE`+`DELETE`+`INSERT` at t=2.0 s returned `old` for all ten rows).
The justification is that AN EXTRACTION IS MORE THAN ONE STATEMENT. This one issues six
inside its transaction: the stamp, the column-list catalog read, the table read, the
watermark, the reconciliation count, and -- on the second snapshot -- the incremental query
whose complement is the phase's headline. Two statements in one `READ COMMITTED`
transaction are TWO snapshots, measured: statement 1 all `old`, statement 2 all `new`.

`READ ONLY` IS NOT DECORATION. Measured: the `opl` role is a SUPERUSER, so nothing else in
this system prevents the extractor from writing to the database it exists to observe.

`SERIALIZABLE` IS REFUSED WITH ITS MECHANISM. For a pure reader, snapshot isolation already
IS a consistent snapshot; SSI exists to prevent write skew between WRITING transactions. It
would add `40001` retry semantics for zero correctness gain.

--- THE STAMP IS THE TRANSACTION'S FIRST STATEMENT, AND THE GUC PIN IS NOT IN THE ---
--- TRANSACTION AT ALL ------------------------------------------------------------------

The `REPEATABLE READ` snapshot is acquired at the first statement that needs one, NOT at
`BEGIN`, while `transaction_timestamp()` (and its alias `now()`) is fixed AT `BEGIN`.
Measured: a 2.503 s gap, in which a row committed after `BEGIN` IS in the snapshot while
the stamp claims to predate it. So the stamp is `clock_timestamp()`, and it is issued as the
transaction's first statement -- one statement that acquires the snapshot AND labels it,
with a measured gap of 0.001 s and 0 rows inside it.

THE PLAN LISTED THE GUC PIN AS ONE OF THE STATEMENTS INSIDE THE TRANSACTION, AND IT CANNOT
BE. `SELECT set_config(...)` is a query, so under `REPEATABLE READ` it acquires the
transaction snapshot itself -- measured, on this container, with the control that makes the
reading mean something:

    pin INSIDE  the transaction, writer commits after it -> reader sees 3 of 13 rows
    pin OUTSIDE the transaction (the session's first act) -> reader sees 13 of 13 rows

A pin inside therefore freezes the snapshot BEFORE the stamp is taken, which is the 2.503 s
gap this ruling exists to close, mirrored: rows committed between the pin and the stamp are
absent from the snapshot while the stamp claims to postdate them. The pin is the SESSION's
first act, before `BEGIN`; the stamp is the TRANSACTION's first statement. Both halves are
`SET`-style and session-scoped, so nothing is lost by putting them on that side of the
boundary.

AND A `SET` THAT IS NOT READ BACK IS NOT A PIN (plan T4). libpq turns `PGTZ`,
`PGDATESTYLE`, `PGCLIENTENCODING` and `PGOPTIONS` into startup options with NO CODE CHANGE
-- measured, a writer at `DateStyle='SQL, DMY'` renders `03/08/2026` and a reader at
`'ISO, MDY'` parses it as 2026-03-08, silently -- so every pinned value is read back and a
disagreement is a refusal.

--- THE TRANSACTION IS COMMITTED BEFORE THE UPLOAD, AND THAT IS THIS MODULE'S SHAPE ----

`snapshot()` returns rows already in memory and its transaction is over. Three measured
reasons, and the third is the sharp one: an open read pins the global xmin horizon; it
exposes the extractor to `idle_in_transaction_session_timeout`, which is 0 here and is not 0
on any managed Postgres; and a `READ ONLY` transaction that has merely READ a table BLOCKS
EVERY `ALTER TABLE` on it for as long as it lives (measured by the probe that deadlocked
itself finding it). An extractor holding its snapshot across a multi-minute upload is a
reader that stops the operational database from being migrated.

--- WHY THE LANDING WRITER IS NOT `emit_records_file` -----------------------------------

That function `os.replace`s a staged file into the landing directory, which is atomic only
within one UC Volume's single FUSE mount -- a DATABRICKS-SIDE rename. This extractor runs on
the extraction HOST (plan T1) and PUTs its file through the Files API, so there is no
Volume-side staging step for it to perform and no rename for it to make atomic. What it does
need is the two REFUSALS that function carries, and those are re-derived here rather than
imported: `serialised_bytes` refuses a carriage return in the payload, and
`refuse_bytes_that_are_not_the_payload` is the read-back check a caller makes against the
file it just wrote. Re-derived rather than lifted because the third property --
`_refuse_a_different_file`, which compares against a file ALREADY IN THE VOLUME -- is one
this module cannot express: it has no filesystem.
"""
from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from opl.contracts.merchant import (
    COLUMNS,
    NULLABLE_COLUMNS,
    OBSERVATION_COLUMNS,
    PG_SNAPSHOT_COLUMN,
    PG_WAL_LSN_COLUMN,
    SNAPSHOT_AT_COLUMN,
    SOURCE_COLUMNS,
    UPDATED_AT_COLUMN,
)

# The operational table this reads. Its name is NOT `merchant.CONTRACT`, though the two
# strings are equal: a contract key and a Postgres relation are different namespaces, and
# resolving one through the other is the coincidence `registry.spec_for_contract` exists
# because of. `scripts/seed_merchant_db` imports this name rather than declaring a second.
SOURCE_TABLE = "merchant"

# T4's pin set, and the ONE declaration of it -- `scripts/seed_merchant_db` imports this
# dict rather than carrying its own. THE DECLARATION IS WHAT MUST NOT DRIFT: a writer and a
# reader disagreeing about `DateStyle` is a silent misparse, while two modules each reading
# their pins back is merely two implementations of a check that cannot pass wrongly. The
# seeder keeps its own executor because it may import psycopg and this module may not.
#
# `search_path` is a pin and not a dependency: every statement here names its schema.
RENDERING_GUCS: dict[str, str] = {
    "TimeZone": "UTC",
    "DateStyle": "ISO, MDY",
    "IntervalStyle": "iso_8601",
    "bytea_output": "hex",
    # 3 buys NOTHING over the server default of 1 -- since PG12 both use the shortest-exact
    # algorithm and both round-trip -- and EVERYTHING over an environment that sets 0 or -3,
    # both of which render `0.1::float8 + 0.2::float8` as `0.3`, which does not round-trip.
    # The pin is worth taking and the reason is the environment, not the default.
    "extra_float_digits": "3",
    "client_encoding": "UTF8",
    "search_path": "pg_catalog, public",
}

# ASSERTED, NEVER ASSUMED. "A Postgres database is UTF-8" is false as a general statement:
# a `LATIN1` database was created on this very container during Task 0, and `SQL_ASCII`
# accepts arbitrary bytes as `text`. The conclusion survives for this container only if it
# is checked, on the same discipline as the CNPJ dialect being asserted against the imported
# value rather than against a restated encoding name.
REQUIRED_SERVER_ENCODING = "UTF8"

# A bare identifier, which is what a schema name is allowed to be here. The schema is the
# ONE coordinate that reaches this module from a command line, and the table read below
# interpolates it -- a parameter cannot stand where a relation name goes. So it is refused
# unless it is an unquoted identifier, which is the same treatment
# `registry._assert_subdirs_are_single_path_components` gives a landing subdir: it names one
# schema, it is not a path and it is not an expression.
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")

# One record per line, UTF-8, `\n`-TERMINATED -- the last line carries one too, matching
# `opl.generator.events.to_jsonl`, whose docstring gives the reason: concatenating two
# chunks then produces a valid file rather than one fused line.
LINE_SEPARATOR = "\n"
STREAM_FILE_SUFFIX = ".jsonl"


class MerchantSnapshotRefused(Exception):
    """A snapshot could not be trusted to mean what the caller will do with it.

    Every refusal below names what it found, because a merchant snapshot that is wrong is
    a vault whose end-dating fires on rows that never departed -- or, worse, does not fire
    on rows that did, which is the failure with nothing to see."""


class Cursor(Protocol):
    """What `Connection.execute` gives back. psycopg's cursor satisfies it, and so does a
    list-backed fake -- which is the point."""

    def fetchall(self) -> list[tuple[Any, ...]]: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...


class Connection(Protocol):
    """The injected transport. DUCK-TYPED ON PURPOSE, and this is the whole reason this
    module is testable with no database: `psycopg.Connection` satisfies it without knowing
    that it does, and nothing here imports psycopg to say so."""

    def execute(self, statement: str, params: Sequence[Any] | None = ...) -> Cursor: ...


# --- the statements, each one named so the run log and a test quote the same string ----

BEGIN_SNAPSHOT = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
COMMIT = "COMMIT"
ROLLBACK = "ROLLBACK"

# THE FIRST STATEMENT, and it does two jobs in one: reading `clock_timestamp()` acquires the
# transaction's snapshot, and the value it returns labels it. `AT TIME ZONE 'UTC'` is
# explicit rather than left to the pinned `TimeZone`, so the rendering is right even if the
# pin were somehow bypassed -- and `to_char` with `.US` keeps the trailing fractional zeros
# `::text` trims, which is what makes the axis sort chronologically as a string.
STAMP = (
    "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', "
    "'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'), "
    "pg_current_snapshot()::text, "
    "pg_current_wal_lsn()::text"
)

# THE COLUMN LIST, READ FROM THE CATALOG rather than assumed. An operational database is
# migrated; a column added, renamed or reordered upstream would otherwise arrive as a NULL
# column, a rescued field, or -- for a REORDER -- as a set of values silently transposed
# between two `text` columns of the same width, which no downstream count can see. The
# relation arrives as a PARAMETER cast to `regclass`, so nothing is interpolated.
COLUMN_LIST = (
    "SELECT attname FROM pg_attribute "
    "WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped "
    "ORDER BY attnum"
)

# THE WATERMARK QUERY, and it is the REAL one an engineer would write. It runs INSIDE the
# same transaction that reads the snapshot, so its answer and the snapshot are one
# observation of one MVCC state -- run against the landed file instead, deletes are missing
# by construction and the number is theatre; run against the live database, it is a third
# read at a third instant.
WATERMARK = "SELECT max({column})::text FROM {table}"

# WHAT AN INCREMENTAL EXTRACT WOULD HAVE RETURNED, given the previous snapshot's watermark.
#
# STRICTLY `>`, AND THE BOUNDARY IS POPULATED SO THIS IS NOT A STYLE CHOICE. Measured on the
# seeded population: all eight `watermark_advance` rows carry ONE identical `updated_at`,
# exactly equal to snapshot 1's watermark. Under `>=` those eight are returned again, the
# incremental set stops being a SUBSET of what the snapshot diff caught, and the comparison
# the phase publishes stops being a complement. `>` is also what every real incremental
# extract writes, for the ordinary reason: `>=` re-reads the boundary row on every run.
INCREMENTAL = (
    "SELECT {key}::text FROM {table} WHERE {column} > %s::timestamptz "
    'ORDER BY {key}::text COLLATE "C"'
)

# THE RECONCILIATION COUNT, inside the transaction, against the same snapshot the rows came
# from. It cannot disagree with `len(rows)` unless the read was TRUNCATED -- a cursor
# abandoned, a `fetchall` that returned a page -- which is exactly the failure that produces
# a short snapshot every downstream count reports as a departure.
ROW_COUNT = "SELECT count(*) FROM {table}"


def qualified(schema: str) -> str:
    """`schema.merchant`, or refuse a schema that is not a bare identifier.

    THE ONLY VALUE THIS MODULE INTERPOLATES INTO SQL, so it is the only one that needs
    this. A relation name cannot be a bound parameter, and a schema arriving from a command
    line is operator input at a boundary -- refused where it enters, not trusted because
    the caller is ours today."""
    if not _IDENTIFIER.match(schema or ""):
        raise MerchantSnapshotRefused(
            f"schema {schema!r} is not a bare lower-case identifier. It names ONE schema "
            "and is interpolated into the relation this reads, so it is not a path, not a "
            "quoted name and not an expression."
        )
    return f"{schema}.{SOURCE_TABLE}"


@dataclass(frozen=True)
class MerchantSnapshot:
    """One transactionally-consistent observation of the merchant registry.

    `instant`, `pg_snapshot` and `wal_lsn` are the three stamps the transaction took before
    it read anything, and they are what every landed row carries. `watermark` is what an
    incremental extract would have recorded HERE; `incremental_keys` is what one would have
    RETURNED, given the previous snapshot's watermark, and is empty when none was supplied
    rather than absent -- an absence and an empty answer are different facts about a
    watermark and this type must not conflate them."""

    instant: str
    pg_snapshot: str
    wal_lsn: str
    rows: tuple[tuple[str | None, ...], ...]
    watermark: str | None
    incremental_keys: tuple[str, ...]
    asked_since: str | None


def pin_rendering_gucs(conn: Connection) -> dict[str, str]:
    """Pin the rendering GUCs and READ THEM BACK, refusing on any disagreement.

    `set_config()` and `current_setting()` rather than `SET` and `SHOW`, because both are
    ordinary functions that take their argument as a BOUND PARAMETER -- where `SET x = y`
    and `SHOW x` are utility statements whose name has to be interpolated. That removes the
    one place a GUC name would otherwise be concatenated into SQL, and it is why this
    module needs no identifier-quoting helper and therefore no driver.

    THE SESSION'S FIRST ACT, and never inside the read transaction -- see the module
    docstring for the measurement that decides it."""
    for name, value in RENDERING_GUCS.items():
        conn.execute("SELECT set_config(%s, %s, false)", (name, value))
    read_back = {
        name: _one(conn, "SELECT current_setting(%s)", (name,)) for name in RENDERING_GUCS
    }
    wrong = {name: got for name, got in read_back.items() if got != RENDERING_GUCS[name]}
    if wrong:
        raise MerchantSnapshotRefused(
            f"the rendering GUCs did not read back as pinned: {wrong}. libpq applies "
            "PGTZ, PGDATESTYLE, PGCLIENTENCODING and PGOPTIONS as startup options with no "
            "code change, so a SET that is not read back is not a pin -- and a writer and "
            "a reader disagreeing about DateStyle misparse 03/08/2026 in silence."
        )
    encoding = _one(conn, "SHOW server_encoding")
    if encoding != REQUIRED_SERVER_ENCODING:
        raise MerchantSnapshotRefused(
            f"server_encoding is {encoding!r}, not {REQUIRED_SERVER_ENCODING!r}. A LATIN1 "
            "database was created on this very container during Task 0 and SQL_ASCII "
            "accepts arbitrary bytes as text, so this is asserted rather than assumed."
        )
    return read_back


def _one(conn: Connection, statement: str, params: Sequence[Any] | None = None) -> Any:
    """The single scalar `statement` answers, or refuse an empty result.

    A statement that returns no row would otherwise raise `TypeError: 'NoneType' object is
    not subscriptable` several frames from the query that produced it."""
    row = conn.execute(statement, params).fetchone()
    if row is None:
        raise MerchantSnapshotRefused(
            f"{statement!r} returned no row where exactly one scalar was expected"
        )
    return row[0]


def _refuse_a_schema_that_is_not_the_contract(found: Sequence[str], relation: str) -> None:
    """Refuse a source table whose columns are not the ones the contract declares.

    ORDER IS PART OF IT, and that is the half a `set` comparison would lose. Every value
    below is rendered `::text`, so two `text` columns SWAPPED by a migration produce a
    landed file of the right shape, the right width and the right row count, with two
    columns' values transposed -- and `legal_name`/`trade_name`, or `status`/`risk_tier`,
    are pairs no downstream check distinguishes. The read below selects columns BY NAME, so
    a reorder alone cannot transpose anything; what this refuses is the state in which the
    table and the contract have stopped describing the same thing, before a file is landed
    under a name the emitter will refuse to correct."""
    if tuple(found) != tuple(SOURCE_COLUMNS):
        raise MerchantSnapshotRefused(
            f"{relation} declares columns {tuple(found)}, and the contract declares "
            f"{tuple(SOURCE_COLUMNS)}. An operational database is migrated: a column added, "
            "renamed or reordered upstream is a change this lakehouse adopts DELIBERATELY, "
            "with a contract version bump, rather than one it lands as a NULL column, a "
            "rescued field, or two text columns silently transposed."
        )


def _refuse_a_truncated_read(rows: Sequence[Any], counted: int, relation: str) -> None:
    """Refuse a row set that is not the whole table the same snapshot counted.

    THE ONE THING A SHORT SNAPSHOT LOOKS LIKE DOWNSTREAM IS A DEPARTURE, which is this
    phase's headline. A `fetchall` that returned a page, an abandoned cursor, or a client
    that ran out of memory mid-read all produce a file that is a perfectly valid snapshot of
    a smaller table -- and the vault would end-date every missing key with an
    `absent_after_observation` closing row. The count is taken INSIDE the same transaction,
    so it is the same MVCC state and cannot disagree for any legitimate reason."""
    if len(rows) != counted:
        raise MerchantSnapshotRefused(
            f"{relation} answered {len(rows)} rows and the same snapshot counts {counted}. "
            "A short read is indistinguishable downstream from a hard DELETE of every row "
            "it is missing, and the vault would end-date all of them."
        )


def snapshot(
    conn: Connection, *, schema: str = "public", since: str | None = None
) -> MerchantSnapshot:
    """ONE `REPEATABLE READ READ ONLY` transaction, stamped by its own first statement.

    The caller owns the connection and must have pinned the rendering GUCs on it already
    -- see the module docstring for why that cannot happen in here. `since` is the previous
    snapshot's watermark; supplied, the incremental query an engineer would have written
    runs inside this same transaction, so its answer and this snapshot are one observation.

    COMMITS BEFORE RETURNING, always, including on the failure path: a `READ ONLY`
    transaction that has merely read a table blocks every `ALTER TABLE` on it until it
    ends."""
    relation = qualified(schema)
    conn.execute(BEGIN_SNAPSHOT)
    try:
        instant, pg_snapshot, wal_lsn = _stamp(conn)
        _refuse_a_schema_that_is_not_the_contract(_columns(conn, relation), relation)
        rows = _rows(conn, relation)
        _refuse_a_truncated_read(rows, _one(conn, ROW_COUNT.format(table=relation)), relation)
        watermark = _one(
            conn, WATERMARK.format(column=UPDATED_AT_COLUMN, table=relation)
        )
        keys = _incremental(conn, relation, since)
    except BaseException:
        conn.execute(ROLLBACK)
        raise
    conn.execute(COMMIT)
    return MerchantSnapshot(
        instant=instant,
        pg_snapshot=pg_snapshot,
        wal_lsn=wal_lsn,
        rows=rows,
        watermark=watermark,
        incremental_keys=keys,
        asked_since=since,
    )


def _stamp(conn: Connection) -> tuple[str, str, str]:
    """The three stamps, from ONE statement that is also what acquires the snapshot."""
    row = conn.execute(STAMP).fetchone()
    if row is None or len(row) != 3 or any(not isinstance(value, str) for value in row):
        raise MerchantSnapshotRefused(
            f"the snapshot stamp answered {row!r}, and it must be three strings: the "
            "instant, the visibility set and the WAL position. This statement is what "
            "acquires the REPEATABLE READ snapshot, so an unreadable answer means nothing "
            "after it is reading the state it claims to."
        )
    return row[0], row[1], row[2]


def _columns(conn: Connection, relation: str) -> tuple[str, ...]:
    return tuple(row[0] for row in conn.execute(COLUMN_LIST, (relation,)).fetchall())


def _rows(conn: Connection, relation: str) -> tuple[tuple[str | None, ...], ...]:
    """Every row, rendered to text BY POSTGRES, in a byte-deterministic order.

    `ORDER BY merchant_id::text COLLATE "C"` (plan T11), and the collation is the load-
    bearing half. Default collation is NOT byte order -- measured, `en_US.utf8` orders
    `['a','A','ä','a b','ab',...]` where `C` orders `['A','B','Z','_z','a',...]` -- and this
    database's `datlocprovider` is libc, whose collation changed at glibc 2.28. The landing
    path refuses a file whose bytes differ from the ones it derived, so a container and a CI
    runner on different glibc would produce that refusal as a mysterious diff rather than as
    a finding. The `::text` is not optional either: `COLLATE` applies to collatable types
    and `merchant_id` is a `uuid`. It is also the exact expression
    `scripts/seed_merchant_db._DIGEST` orders by, so the seeder's published digest and this
    file's row order are the same ordering rather than two that agree today."""
    key = SOURCE_COLUMNS[0]
    projection = ", ".join(f"{column}::text" for column in SOURCE_COLUMNS)
    statement = (
        f"SELECT {projection} FROM {relation} "
        f'ORDER BY {key}::text COLLATE "C"'
    )
    return tuple(tuple(row) for row in conn.execute(statement).fetchall())


def _incremental(conn: Connection, relation: str, since: str | None) -> tuple[str, ...]:
    """What `WHERE updated_at > :since` returns, inside this same transaction."""
    if since is None:
        return ()
    statement = INCREMENTAL.format(
        key=SOURCE_COLUMNS[0], table=relation, column=UPDATED_AT_COLUMN
    )
    return tuple(row[0] for row in conn.execute(statement, (since,)).fetchall())


def _refuse_a_value_postgres_did_not_render(
    column: str, value: object, key: str | None
) -> None:
    """Refuse a value that is not text, and a NULL in a column the contract requires.

    EVERY VALUE IS `::text` IN THE PROJECTION, so a non-string here means the cast was
    dropped from one column of the SELECT -- after which Python's `str()` would be the
    thing rendering it, which is precisely what plan T4 forbids: `json.loads` -> `float` ->
    `str` dropped the trailing zero of `5.07730` one phase ago, and a `Decimal` or a
    `datetime` arriving here would be re-rendered by whatever `json.dumps` decides.

    THE NULL HALF IS NOT THE DQ GATE REPEATED. The gate runs on LANDED rows and describes
    them; this runs on the read and describes the READ. A required column arriving NULL
    from a `NOT NULL` column means the projection and the table have come apart, and
    landing the file first would put the diagnosis a job run away from the SELECT."""
    if value is None and column not in NULLABLE_COLUMNS:
        raise MerchantSnapshotRefused(
            f"{column} is NULL for {key!r}, and the source declares it NOT NULL. The "
            "contract's required set is that DDL read across, so a NULL here means the "
            "projection and the table have come apart."
        )
    if value is not None and not isinstance(value, str):
        raise MerchantSnapshotRefused(
            f"{column} arrived as {type(value).__name__} ({value!r}) for {key!r}, not as "
            "text. Every column is cast `::text` in the projection so that POSTGRES does "
            "the rendering under the pinned GUCs; a value that reaches Python un-rendered "
            "is one Python will render instead, which is how a trailing zero is lost."
        )


def _record(row: Sequence[object], observed: Mapping[str, str]) -> dict[str, str | None]:
    """One landed record: the row's own columns, then the observation's three stamps.

    Built by iterating `COLUMNS`, so the CONTRACT's declared order decides the emitted
    line's key order -- `json.dumps` writes keys in insertion order, and that order is what
    the byte-identity refusal is taken over."""
    if len(row) != len(SOURCE_COLUMNS):
        raise MerchantSnapshotRefused(
            f"a row carries {len(row)} values and the projection selects "
            f"{len(SOURCE_COLUMNS)}: {row!r}"
        )
    values: dict[str, str | None] = dict(zip(SOURCE_COLUMNS, row, strict=True))
    for column, value in values.items():
        _refuse_a_value_postgres_did_not_render(column, value, values[SOURCE_COLUMNS[0]])
    values.update(observed)
    return {column: values[column] for column in COLUMNS}


def records_of(observation: MerchantSnapshot) -> tuple[dict[str, str | None], ...]:
    """`observation`'s rows as the contract's fourteen columns, in `COLUMNS` order.

    THE THREE STAMPS ARE CARRIED, NOT RE-DERIVED, and that is ADR 0016's finding met for
    the third time: the RFB's applied date comes from a filename, PTAX's quote date is
    carried from the REQUEST because the response does not contain it, and this instant is
    carried from the transaction that read the rows. None of the three is in the payload.
    Every one of them is a string POSTGRES rendered, so nothing here re-renders anything."""
    observed = {
        SNAPSHOT_AT_COLUMN: observation.instant,
        PG_SNAPSHOT_COLUMN: observation.pg_snapshot,
        PG_WAL_LSN_COLUMN: observation.wal_lsn,
    }
    if tuple(observed) != tuple(OBSERVATION_COLUMNS):
        raise MerchantSnapshotRefused(
            f"this layer stamps {tuple(observed)} and the contract declares the observation "
            f"columns as {tuple(OBSERVATION_COLUMNS)}. The dict above is the ONE place a "
            "stamp is attached to a row, so a column the contract added and this forgot "
            "would land as a missing key -- which `struct_for` reads as NULL rather than as "
            "an error."
        )
    if not all(isinstance(value, str) and value for value in observed.values()):
        raise MerchantSnapshotRefused(
            f"the observation's own stamps are not three non-empty strings: {observed}"
        )
    return tuple(_record(row, observed) for row in observation.rows)


def serialised_bytes(records: Sequence[Mapping[str, str | None]]) -> bytes:
    """`records` as the exact bytes that belong on disk. THE crossing into bytes.

    A `\\n` JOIN AND AN EXPLICIT UTF-8 ENCODE, matching `opl.generator.events.to_jsonl` and
    `opl.bronze.generated_landing.serialised_bytes` in both, because
    `opl.bronze.reader.jsonl_read_options` declares `encoding=UTF-8` on the READ side and a
    writer whose encoding came from the machine's locale would make that a coincidence --
    cp1252 on this dev box.

    THE `\\r` REFUSAL IS A GUARD ON THE OUTPUT, not a restatement of the join, and it is
    re-derived here rather than imported because this source can produce one where the
    generator cannot: `legal_name` and `trade_name` are free text in an operational
    database, and `json.dumps` escapes a carriage return inside a VALUE as the two ASCII
    characters `\\` and `r` -- so a CR in these bytes can only have come from the SEPARATOR,
    i.e. from an `os.linesep` join or a text-mode write, which would make byte-identity true
    only within one operating system."""
    payload = "".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")) + LINE_SEPARATOR
        for record in records
    ).encode("utf-8")
    if b"\r" in payload:
        raise ValueError(
            "the serialised snapshot contains a carriage return, so its bytes are not the "
            "bytes this run derived. json.dumps escapes a CR inside a value as the two "
            "characters '\\' and 'r', so one in the payload came from the SEPARATOR -- an "
            "os.linesep join or a text-mode write, which produces '\\r\\n' on Windows and "
            "'\\n' on Databricks and makes byte-identity true only within one OS."
        )
    return payload


def refuse_bytes_that_are_not_the_payload(written: bytes, payload: bytes, where: str) -> None:
    """Refuse a file whose bytes are not the ones that were serialised.

    THE READ-BACK IS THE POINT AND IT IS NOT PARANOIA. A text-mode read of the same file
    would normalise `\\r\\n` back to `\\n` and report success over a file that is
    byte-different -- which is how a "restore the original" step in this repository passed a
    content comparison while leaving the file changed. Reading BYTES is the only comparison
    that can see the defect a binary write exists to prevent.

    A PURE FUNCTION TAKING BOTH SIDES, because this module opens no file. The caller reads
    the file back and hands the result here, so the REFUSAL has one spelling even though the
    I/O is somewhere else."""
    if written != payload:
        raise ValueError(
            f"{where} holds {len(written)} bytes and {len(payload)} were serialised. The "
            "file on disk is not the artefact this run derived -- the usual cause is a "
            "text-mode write translating '\\n' into '\\r\\n'."
        )


def filename_for(instant: str) -> str:
    """The landing filename for the snapshot taken at `instant`.

    THE INSTANT, AND NOTHING ELSE. Auto Loader tracks files by PATH, so a name carrying a
    run id, a month or a host would make every re-run a NEW file holding the same rows --
    ingested again under a fresh `_batch_id`, which the promote's idempotence is keyed on
    and therefore cannot see. The instant IS this snapshot's identity: two snapshots are
    two instants and therefore two files, and re-landing one is the same path.

    THE SEPARATORS ARE STRIPPED, AND THAT IS THE ONE THING THIS FUNCTION DOES BEYOND
    COPYING. `:` cannot appear in a filename on Windows, and this file is written and
    verified on the extraction HOST before it is PUT -- so the axis's own rendering cannot
    be the name. Stripping `-`, `:` and `.` is injective over the fixed-width rendering
    (every one of them sits at a fixed offset), so two instants still give two names and the
    instant is still recoverable by eye.

    THE VALUE IS NOT RE-VALIDATED HERE. It is whatever `to_char` returned inside the
    transaction, and the shape is asserted where it can be asserted against something --
    the DQ gate's `bad_snapshot_at_shape` and the registry's `_snapshot_at_instant_shape`
    CHECK, both of which compare it against the declared axis."""
    compact = instant.replace("-", "").replace(":", "").replace(".", "")
    return f"{SOURCE_TABLE}-{compact}{STREAM_FILE_SUFFIX}"


# --- the hand-off from the mutation, which is NOT inferred -----------------------------
#
# Module level for the reason `opl.bronze.rules` states above `rules_for`: this is the
# reasoning, and inside the parser's docstring it puts the function past the 50-line limit.
#
# READINESS IS DECLARED BY THE WRITER, AND ABSENCE IS NOT READINESS. Task 3 measured what
# happens when the extractor infers it instead: starting snapshot 1 the moment an
# `idle in transaction` session appears catches `mutate` BETWEEN t1 and t2 -- the watermark
# then lands BELOW t1, the out-of-order rows become perfectly visible to
# `WHERE updated_at > watermark`, and the one number in this phase's headline that nothing
# authored silently disappears WHILE EVERY OTHER COUNT STAYS CORRECT. That is the exact
# failure shape this repository refuses: a control that vanishes rather than fails.
#
# So `seed_merchant_db.py mutate --ready-on PATH` writes a file once its own `t2 > t1`
# refusal has passed, carrying both stamps, and the extractor waits for THAT.
#
# THE PARSE IS TOLERANT AND THE VERDICT IS NOT. The file is written by a plain
# `write_text`, so a reader polling `exists()` can observe it between create and close and
# read a truncated `t1=`. Half a file is not a signal: this returns `None` for anything that
# does not yield BOTH stamps, and the caller keeps waiting. What it must never do is treat a
# timeout as readiness -- see `scripts/extract_merchant_snapshot.py`, which refuses.
_READY_LINE = re.compile(r"^(t1|t2)=(.+)$")


def readiness_stamps(text: str) -> dict[str, str] | None:
    """`{'t1': ..., 't2': ...}` from a readiness file's text, or None if it is not whole.

    See the comment block above for why a partial read is expected and why `None` means
    "keep waiting" rather than "proceed"."""
    stamps = {
        match.group(1): match.group(2).strip()
        for match in map(_READY_LINE.match, text.splitlines())
        if match and match.group(2).strip()
    }
    return stamps if {"t1", "t2"} <= set(stamps) else None
