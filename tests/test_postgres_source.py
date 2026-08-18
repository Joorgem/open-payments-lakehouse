# tests/test_postgres_source.py
"""`opl.extraction.postgres_source` -- the transaction, the refusals, and the bytes.

HERMETIC, AND THAT IS THE MODULE'S DESIGN RATHER THAN THIS FILE'S CONVENIENCE. The
connection is INJECTED, exactly as `ptax_source` takes a `fetch` callable, so every
statement below runs against a fake that records what it was asked and answers from a
script. Nothing here connects to Postgres, starts a container or reads a clock. The live
container is exercised in `tests/integration/`, under the `postgres` marker.

THE AST SWEEP AT THE FOOT OF THIS FILE IS THE TWIN OF `test_ptax_source.py`'s, and it is
what makes the sentence above structural rather than a habit: that module cannot acquire an
HTTP client, and this one cannot acquire a driver.

WHAT IS DELIBERATELY NOT TESTED HERE: that a snapshot of a real table has 1,088 rows, that
the GUC pin defeats a hostile `PGOPTIONS`, or that `REPEATABLE READ` holds across
statements. Those are facts about Postgres and about a seeded database, and a fake
connection asserting them would be a test of its own script."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from opl.contracts.merchant import COLUMNS, SOURCE_COLUMNS
from opl.extraction import postgres_source
from opl.extraction.postgres_source import (
    BEGIN_SNAPSHOT,
    COMMIT,
    RENDERING_GUCS,
    ROLLBACK,
    MerchantSnapshot,
    MerchantSnapshotRefused,
    filename_for,
    pin_rendering_gucs,
    qualified,
    readiness_stamps,
    records_of,
    refuse_bytes_that_are_not_the_payload,
    serialised_bytes,
    snapshot,
    watermark_is_at_or_after,
)

# One row of the merchant table as POSTGRES renders it under the pinned GUCs: eleven
# strings, in DDL order, with `trade_name` NULL. The values are shaped like the seeder's --
# a 14-digit CNPJ with a leading zero in its root, a `numeric(14,2)` with its scale, an ISO
# date and an ISO-with-offset instant -- because a fixture of invented shapes would not show
# that the leading zero and the trailing zero both survive to the bytes.
_ROW = (
    "00057343-0001-4a2e-8f00-000000000001",
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

_INSTANT = "2026-08-16T22:10:47.000123Z"
_PG_SNAPSHOT = "918:918:"
_WAL_LSN = "0/1A2B3C8"
_WATERMARK = "2026-08-16 22:10:46.779989+00"


class _Cursor:
    def __init__(self, rows: list[tuple]):
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def fetchone(self) -> tuple | None:
        return self._rows[0] if self._rows else None


class _Connection:
    """A connection that RECORDS what it was asked and answers from a script.

    Matching is by substring on the statement, which is exactly how a reader of this file
    identifies them too -- and it means a test asserting "the transaction issued the stamp
    first" is reading the recorded order rather than trusting the implementation."""

    def __init__(self, answers: list[tuple[str, list[tuple]]]):
        self.answers = answers
        self.statements: list[str] = []
        self.params: list[object] = []

    def execute(self, statement: str, params=None) -> _Cursor:
        self.statements.append(statement)
        self.params.append(params)
        for marker, rows in self.answers:
            if marker in statement:
                return _Cursor(rows)
        return _Cursor([])


def _connection(
    *,
    rows: list[tuple] | None = None,
    columns: tuple[str, ...] = SOURCE_COLUMNS,
    counted: int | None = None,
    watermark: str | None = _WATERMARK,
    incremental: list[str] | None = None,
) -> _Connection:
    """A fake answering the six statements one snapshot issues, in any order it asks."""
    rows = [_ROW] if rows is None else rows
    return _Connection(
        [
            ("clock_timestamp", [(_INSTANT, _PG_SNAPSHOT, _WAL_LSN)]),
            ("pg_attribute", [(name,) for name in columns]),
            ("count(*)", [(len(rows) if counted is None else counted,)]),
            ("max(updated_at)", [(watermark,)]),
            ("WHERE updated_at >", [(key,) for key in (incremental or [])]),
            ("COLLATE", rows),
        ]
    )


class _PinConnection(_Connection):
    """A connection whose `current_setting(%s)` answers PER GUC, from its parameter.

    A `_Connection` cannot do this: it matches on the statement text, and every read-back
    is the SAME statement with a different bound parameter -- which is the property being
    asserted. `hostile` names the GUCs that answer something other than what was asked
    for, so a test says which pin it is attacking rather than which position in a list."""

    def __init__(self, *, hostile: dict[str, str] | None = None, encoding: str = "UTF8"):
        super().__init__([])
        self.hostile = hostile or {}
        self.encoding = encoding

    def execute(self, statement: str, params=None) -> _Cursor:
        self.statements.append(statement)
        self.params.append(params)
        if "current_setting" in statement:
            name = params[0]
            return _Cursor([(self.hostile.get(name, RENDERING_GUCS[name]),)])
        if "server_encoding" in statement:
            return _Cursor([(self.encoding,)])
        return _Cursor([])


def _issued(conn: _Connection, marker: str) -> str:
    return next(statement for statement in conn.statements if marker in statement)


# --- the transaction ------------------------------------------------------------------


def test_the_stamp_is_the_transactions_first_statement():
    """THE MEASUREMENT THIS ENCODES: the REPEATABLE READ snapshot is acquired at the first
    statement that needs one, NOT at `BEGIN`, while `transaction_timestamp()` is fixed at
    `BEGIN` -- a 2.503 s gap, with a row committed inside it that IS in the snapshot while
    the stamp claims to predate it.

    So the order below is the correctness property and not an implementation detail: the
    statement that stamps the snapshot must also be the statement that acquires it."""
    conn = _connection()
    snapshot(conn)

    assert conn.statements[0] == BEGIN_SNAPSHOT
    assert "clock_timestamp" in conn.statements[1]
    assert conn.statements[-1] == COMMIT


def test_the_transaction_is_read_only_and_repeatable_read():
    """`READ ONLY` is not decoration: the `opl` role is a SUPERUSER, so nothing else stops
    the extractor writing to the database it exists to observe."""
    assert BEGIN_SNAPSHOT == "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"


def test_the_stamp_is_clock_timestamp_and_never_now_or_transaction_timestamp():
    """`now()` and `transaction_timestamp()` are ALIASES, both fixed at `BEGIN` -- which is
    exactly the value the measured gap makes wrong. Task 0's four-function question has two
    correct answers that are the same function, and neither of them is this one."""
    stamp = postgres_source.STAMP
    assert "clock_timestamp()" in stamp
    assert "now()" not in stamp and "transaction_timestamp()" not in stamp


def test_the_stamp_renders_the_axis_in_sql_and_never_by_cast():
    """`::text` on a timestamptz yields `2026-08-16 22:10:47.000123+00` -- a space instead
    of `T`, `+00` instead of `Z`, and TRIMMED trailing fractional zeros. The axis is
    compared lexicographically as a chronology, so the fixed width is the sort property."""
    stamp = postgres_source.STAMP
    assert 'to_char(clock_timestamp() AT TIME ZONE \'UTC\'' in stamp
    assert '.US"Z"' in stamp


def test_txid_current_is_never_asked_for():
    """REFUSED with its mechanism: measured, it assigns an XID even inside a `READ ONLY`
    transaction and advances the cluster counter -- a WAL write from a transaction whose
    whole point is to be a read. `pg_current_snapshot()` and `pg_current_wal_lsn()` carry
    the same provenance without one."""
    from opl.contracts import merchant

    contract = Path(merchant.__file__).read_text(encoding="utf-8")
    assert "txid_current" in contract, "the refusal is recorded where the columns are"
    assert "txid_current" not in postgres_source.STAMP


def test_the_rows_are_ordered_by_an_explicit_C_collation():
    """T11. Default collation is NOT byte order -- `en_US.utf8` orders
    `['a','A','ä','a b','ab',...]` where `C` orders `['A','B','Z','_z','a',...]` -- and
    libc collation changed at glibc 2.28, so a container and a CI runner can disagree. The
    landing refuses a file whose bytes differ from the ones it derived, which would surface
    that as a mysterious diff rather than as a finding.

    `::text` before the COLLATE is load-bearing too: `COLLATE` applies to collatable types
    and `merchant_id` is a `uuid`."""
    conn = _connection()
    snapshot(conn)

    assert 'ORDER BY merchant_id::text COLLATE "C"' in _issued(conn, "COLLATE")


def test_every_selected_column_is_cast_to_text_by_postgres():
    """T4: the rendering is the DATABASE's, under pinned GUCs, never Python's."""
    conn = _connection()
    snapshot(conn)
    projection = _issued(conn, "COLLATE")

    for column in SOURCE_COLUMNS:
        assert f"{column}::text" in projection


def test_the_incremental_query_is_strictly_greater_than():
    """THE BOUNDARY IS POPULATED, so this is not a style choice: all eight
    `watermark_advance` rows carry ONE identical `updated_at`, exactly equal to snapshot
    1's watermark. Under `>=` those eight are returned again, the incremental set stops
    being a SUBSET of what the snapshot diff caught, and the comparison the phase publishes
    stops being a complement."""
    conn = _connection(incremental=["a"])
    snapshot(conn, since=_WATERMARK)
    issued = _issued(conn, "WHERE updated_at >")

    assert "WHERE updated_at > %s::timestamptz" in issued
    assert ">=" not in issued


def test_no_incremental_query_runs_when_no_watermark_was_supplied():
    """An absence and an empty answer are different facts about a watermark."""
    conn = _connection()
    observed = snapshot(conn)

    assert observed.incremental_keys == ()
    assert observed.asked_since is None
    assert not any("WHERE updated_at >" in statement for statement in conn.statements)


def test_the_incremental_query_runs_inside_the_same_transaction():
    """T2's condition on the headline: the miss set is a genuine complement only if the
    watermark query and the snapshot are ONE observation of one MVCC state."""
    conn = _connection(incremental=["a", "b"])
    observed = snapshot(conn, since=_WATERMARK)

    positions = [i for i, s in enumerate(conn.statements) if "WHERE updated_at >" in s]
    assert positions and 0 < positions[0] < conn.statements.index(COMMIT)
    assert observed.incremental_keys == ("a", "b")
    assert observed.asked_since == _WATERMARK


# --- the refusals ---------------------------------------------------------------------


def test_a_source_table_whose_columns_are_not_the_contract_is_refused():
    """An operational database is MIGRATED. A column added, renamed or reordered upstream
    is a change this lakehouse adopts deliberately, with a contract version bump."""
    conn = _connection(columns=(*SOURCE_COLUMNS, "loyalty_tier"))

    with pytest.raises(MerchantSnapshotRefused, match="declares columns"):
        snapshot(conn)


def test_a_reordered_source_table_is_refused_even_though_the_names_all_match():
    """ORDER IS PART OF IT, and a `set` comparison would lose exactly this. Every value is
    rendered `::text`, so two `text` columns swapped by a migration produce a file of the
    right shape, width and row count with two columns transposed -- and `legal_name` /
    `trade_name` is a pair nothing downstream distinguishes."""
    swapped = list(SOURCE_COLUMNS)
    swapped[2], swapped[3] = swapped[3], swapped[2]
    conn = _connection(columns=tuple(swapped))

    with pytest.raises(MerchantSnapshotRefused, match="declares columns"):
        snapshot(conn)


def test_a_read_shorter_than_the_snapshots_own_count_is_refused():
    """THE ONE THING A SHORT SNAPSHOT LOOKS LIKE DOWNSTREAM IS A DEPARTURE, which is this
    phase's headline. The count is taken inside the same transaction, so it cannot disagree
    for any legitimate reason."""
    conn = _connection(counted=2)

    with pytest.raises(MerchantSnapshotRefused, match="answered 1 rows"):
        snapshot(conn)


def test_a_refusal_rolls_back_rather_than_leaving_the_transaction_open():
    """A `READ ONLY` transaction that has merely READ a table blocks every `ALTER TABLE` on
    it until it ends -- measured by the Task 0 probe that deadlocked itself finding it. A
    failure path that left one open would stop the operational database being migrated."""
    conn = _connection(counted=99)

    with pytest.raises(MerchantSnapshotRefused):
        snapshot(conn)

    assert conn.statements[-1] == ROLLBACK
    assert COMMIT not in conn.statements


def test_a_stamp_that_is_not_three_strings_is_refused():
    conn = _connection()
    conn.answers[0] = ("clock_timestamp", [(_INSTANT, None, _WAL_LSN)])

    with pytest.raises(MerchantSnapshotRefused, match="three strings"):
        snapshot(conn)


def test_a_schema_that_is_not_a_bare_identifier_is_refused():
    """The ONLY value this module interpolates into SQL, so it is the only one that needs
    this. A relation name cannot be a bound parameter."""
    for hostile in ("public; DROP TABLE merchant", "PUBLIC", "a.b", "", "1x"):
        with pytest.raises(MerchantSnapshotRefused, match="bare lower-case identifier"):
            qualified(hostile)
    assert qualified("public") == "public.merchant"


def test_a_guc_that_does_not_read_back_is_refused():
    """A `SET` THAT IS NOT READ BACK IS NOT A PIN. libpq applies `PGTZ`, `PGDATESTYLE`,
    `PGCLIENTENCODING` and `PGOPTIONS` as startup options with no code change, so the pin
    can be overridden by one shell variable -- measured, a writer at `SQL, DMY` renders
    `03/08/2026` and a reader at `ISO, MDY` parses 2026-03-08."""
    conn = _PinConnection(hostile={"DateStyle": "SQL, DMY"})

    with pytest.raises(MerchantSnapshotRefused, match="did not read back as pinned"):
        pin_rendering_gucs(conn)


def test_a_server_that_is_not_utf8_is_refused():
    """ASSERTED, NEVER ASSUMED: a `LATIN1` database was created on this very container
    during Task 0, and `SQL_ASCII` accepts arbitrary bytes as `text`."""
    conn = _PinConnection(encoding="LATIN1")

    with pytest.raises(MerchantSnapshotRefused, match="server_encoding is 'LATIN1'"):
        pin_rendering_gucs(conn)


def test_the_pin_names_a_guc_only_in_a_bound_parameter():
    """`set_config()` and `current_setting()` are functions taking their argument as a
    PARAMETER, where `SET x = y` and `SHOW x` are utility statements whose name has to be
    interpolated. That is why this module needs no identifier quoting and therefore no
    driver."""
    conn = _PinConnection()

    assert pin_rendering_gucs(conn) == RENDERING_GUCS
    assert not any(
        name in statement for name in RENDERING_GUCS for statement in conn.statements
    ), "a GUC name reached the SQL text; it must only ever be a bound parameter"


def test_the_watermark_comparison_is_made_by_postgres():
    """`max(updated_at)::text` and `datetime.isoformat()` are two spellings of one instant
    -- a space against a `T`, `+00` against `+00:00` -- so comparing them as Python strings
    is wrong in a way that would pass on most values."""
    conn = _Connection([("::timestamptz >=", [(True,)])])

    assert watermark_is_at_or_after(conn, _WATERMARK, "2026-08-16T22:10:46.779989+00:00")
    assert conn.params[0] == (_WATERMARK, "2026-08-16T22:10:46.779989+00:00")


# --- the records ----------------------------------------------------------------------


def test_the_record_carries_the_contracts_fourteen_columns_in_order():
    """`json.dumps` writes keys in insertion order, and that order decides the bytes the
    landing path refuses a difference in."""
    observed = snapshot(_connection())
    (record,) = records_of(observed)

    assert tuple(record) == COLUMNS


def test_the_three_stamps_are_carried_onto_every_row():
    """ADR 0017's Pattern 1 -- ADR 0016's finding, met for the third time: the RFB's
    applied date comes from a filename, PTAX's quote date from the REQUEST, and this
    instant from the transaction that read the rows. None of the three is in the payload.

    0016 is where the SECOND of the three was found; 0017 is where three coincidences
    become a stated property of snapshot ingestion."""
    observed = snapshot(_connection(rows=[_ROW, _ROW]))
    records = records_of(observed)

    assert len(records) == 2
    for record in records:
        assert record["_snapshot_at"] == _INSTANT
        assert record["_pg_snapshot"] == _PG_SNAPSHOT
        assert record["_pg_wal_lsn"] == _WAL_LSN


def test_a_null_trade_name_stays_NULL_and_an_empty_one_stays_EMPTY():
    """THE ONE THING THAT COLUMN IS NULLABLE FOR. The source emits NULL, `''` and a name on
    purpose, and a landing path that conflated the first two could not be told from one
    that kept them apart by any row count."""
    empty = (*_ROW[:3], "", *_ROW[4:])
    observed = snapshot(_connection(rows=[_ROW, empty]))
    null_record, empty_record = records_of(observed)

    assert null_record["trade_name"] is None
    assert empty_record["trade_name"] == ""


def test_a_null_in_a_required_column_is_refused_at_the_read():
    """NOT THE DQ GATE REPEATED. The gate runs on LANDED rows and describes them; this runs
    on the read and describes the READ, so the diagnosis is at the SELECT rather than a job
    run away from it."""
    broken = (_ROW[0], None, *_ROW[2:])
    observed = snapshot(_connection(rows=[broken]))

    with pytest.raises(MerchantSnapshotRefused, match="cnpj is NULL"):
        records_of(observed)


def test_a_value_postgres_did_not_render_is_refused():
    """Every column is `::text` in the projection precisely so POSTGRES does the rendering
    under the pinned GUCs. A `Decimal` reaching Python is one `json.dumps` would render
    instead -- which is how `5.07730` became `5.0773` one phase ago."""
    unrendered = (*_ROW[:8], 12345.60, *_ROW[9:])
    observed = snapshot(_connection(rows=[unrendered]))

    with pytest.raises(MerchantSnapshotRefused, match="arrived as float"):
        records_of(observed)


def test_a_row_of_the_wrong_width_is_refused():
    observed = snapshot(_connection(rows=[_ROW[:5]], counted=1))

    with pytest.raises(MerchantSnapshotRefused, match="carries 5 values"):
        records_of(observed)


def test_the_observation_columns_are_asked_of_the_contract_and_not_restated():
    """Guard the stamp. The dict that attaches the three stamps is the ONE place a stamp
    reaches a row, so a column the contract added and this layer forgot would land as a
    MISSING KEY -- which `struct_for` reads as NULL rather than as an error."""
    observed = MerchantSnapshot(
        instant=_INSTANT,
        pg_snapshot=_PG_SNAPSHOT,
        wal_lsn="",
        rows=(_ROW,),
        watermark=_WATERMARK,
        incremental_keys=(),
        asked_since=None,
    )
    with pytest.raises(MerchantSnapshotRefused, match="three non-empty strings"):
        records_of(observed)


# --- the bytes ------------------------------------------------------------------------


def test_the_serialised_bytes_are_exactly_these():
    """A GOLDEN COPY WRITTEN OUT RATHER THAN A DIGEST, because the thing under test is what
    the bytes ARE -- a digest would pin them without a reader being able to see that
    `trade_name` is `null` and not `""`, that the CNPJ keeps its leading zero, that
    `12345.60` keeps its trailing one, and that the accented name is UTF-8 rather than a
    `\\uXXXX` escape."""
    observed = snapshot(_connection())

    assert serialised_bytes(records_of(observed)) == (
        b'{"merchant_id":"00057343-0001-4a2e-8f00-000000000001",'
        b'"cnpj":"00057343000129",'
        b'"legal_name":"COM\xc3\x89RCIO ATL\xc3\x82NTICO LTDA",'
        b'"trade_name":null,'
        b'"status":"active",'
        b'"mcc":"5411",'
        b'"settlement_account":"001-0042-00001234",'
        b'"risk_tier":"low",'
        b'"credit_limit":"12345.60",'
        b'"onboarded_on":"2019-03-14",'
        b'"updated_at":"2026-07-15 08:31:02.400500+00",'
        b'"_snapshot_at":"2026-08-16T22:10:47.000123Z",'
        b'"_pg_snapshot":"918:918:",'
        b'"_pg_wal_lsn":"0/1A2B3C8"}\n'
    )


def test_a_carriage_return_in_the_payload_is_refused():
    """`json.dumps` escapes a CR inside a VALUE as the two characters `\\` and `r`, so one
    in the payload can only have come from the SEPARATOR -- an `os.linesep` join or a
    text-mode write, which produces `\\r\\n` on Windows and `\\n` on Databricks and makes
    byte-identity true only within one operating system.

    Which is why this test has to reach past `json.dumps` to fire at all: a `\\r` inside a
    value is escaped and lands clean, and that is the correct behaviour."""
    assert b"\\r" in serialised_bytes([{"a": "x\ry"}])
    assert b"\r" not in serialised_bytes([{"a": "x\ry"}])

    # So the only way to a real CR is the separator, which is what the guard is FOR.
    original = postgres_source.LINE_SEPARATOR
    postgres_source.LINE_SEPARATOR = "\r\n"
    try:
        with pytest.raises(ValueError, match="carriage return"):
            serialised_bytes([{"a": "x"}])
    finally:
        postgres_source.LINE_SEPARATOR = original


def test_the_read_back_refusal_compares_bytes_and_says_both_lengths():
    """A text-mode read would normalise `\\r\\n` back to `\\n` and report success over a
    file that is byte-different -- which is how a "restore the original" step in this
    repository passed a content comparison while leaving the file changed."""
    refuse_bytes_that_are_not_the_payload(b"abc", b"abc", "x")

    with pytest.raises(ValueError, match="holds 4 bytes and 3 were serialised"):
        refuse_bytes_that_are_not_the_payload(b"a\r\nc", b"a\nc", "x")


def test_the_filename_is_the_instant_and_carries_no_run_id_and_no_colon():
    """Auto Loader tracks files by PATH, so a run-scoped name makes every re-run a second
    ingest under a fresh `_batch_id` the promote's idempotence cannot see. The separators
    are stripped because `:` cannot appear in a filename on Windows, and this file is
    written and verified on the extraction host before it is PUT."""
    name = filename_for(_INSTANT)

    assert name == "merchant-20260816T221047000123Z.jsonl"
    assert ":" not in name
    assert filename_for(_INSTANT) == name
    assert filename_for("2026-08-17T22:10:47.000123Z") != name


# --- the hand-off ---------------------------------------------------------------------


def test_a_whole_readiness_file_yields_both_stamps():
    assert readiness_stamps("t1=2026-08-16T22:10:46.710500+00:00\nt2=...779989+00:00\n") == {
        "t1": "2026-08-16T22:10:46.710500+00:00",
        "t2": "...779989+00:00",
    }


@pytest.mark.parametrize(
    "partial", ["", "t1=", "t1=2026-08-16T22:10:46.710500+00:00\n", "t1=x\nt2=\n", "junk"]
)
def test_a_partial_readiness_file_is_NOT_a_signal(partial):
    """THE FILE IS WRITTEN WITH A PLAIN `write_text`, so a poller can observe it between
    create and close. Half a file is "not yet" -- and the caller must never treat a timeout
    as readiness either, because a snapshot taken before t2 records a watermark below t1
    and deletes the one number in this phase's headline that nothing authored, while every
    other count stays exactly right."""
    assert readiness_stamps(partial) is None


# --- what this module may not import ---------------------------------------------------


def test_nothing_here_can_reach_a_database_because_nothing_here_holds_a_connection():
    """THE TWIN OF `test_ptax_source.py`'s SWEEP, asserted over the AST rather than the
    text: this module's docstring SAYS the words `psycopg` and `pyspark`, and a substring
    check would punish it for explaining the very thing it is guarding.

    The connection is injected, so this module imports no driver and has nowhere to open
    one. The same walk covers the pyspark ban -- this layer runs on the extraction host,
    where pyspark is an optional extra that is usually absent -- and the `os`/`open` ban,
    which is what makes "executes no I/O" a property rather than a claim: the landing
    writer is the caller's, and the two refusals this module owns are pure functions taking
    both sides."""
    tree = ast.parse(Path(postgres_source.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])

    assert imported.isdisjoint(
        {"psycopg", "psycopg2", "sqlalchemy", "pyspark", "requests", "os", "pathlib"}
    ), (
        f"postgres_source imports {sorted(imported)}; it builds statements and reads "
        "results, and executing them -- and writing anything to disk -- is the caller's"
    )
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "open" not in called


def test_the_pin_set_has_exactly_one_declaration_in_this_repository():
    """`scripts/seed_merchant_db` imports this dict rather than carrying its own, and that
    matters in one direction: the seeder is the only thing in this phase that WRITES, so a
    writer and a reader disagreeing about `DateStyle` is a silent misparse (`03/08/2026`
    written, 2026-03-08 read) rather than a failure either side would notice.

    The Task 0 probe keeps its own set deliberately -- it pins `search_path` to its own
    schema and prints the deviation -- so this asserts the two PRODUCTION spellings, which
    is what the sentence above is about."""
    seeder = Path(postgres_source.__file__).parents[3] / "scripts" / "seed_merchant_db.py"
    source = seeder.read_text(encoding="utf-8")

    assert "RENDERING_GUCS as PINS" in source
    assert "extra_float_digits" not in source, (
        "seed_merchant_db declares a GUC value of its own; the pin set has one declaration"
    )
    assert set(RENDERING_GUCS) == {
        "TimeZone",
        "DateStyle",
        "IntervalStyle",
        "bytea_output",
        "extra_float_digits",
        "client_encoding",
        "search_path",
    }
