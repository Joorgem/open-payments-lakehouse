"""Task 0's T4 measurements: Postgres renders its own values, but not canonically.

`hashing_spark.refuse_non_string_columns` raises for any non-STRING column reaching a
digest, so every column is rendered to text somewhere, and F-API's lesson says that
"somewhere" must be the database. The instinct is right and the conclusion did not
follow: `col::text` is SESSION-DEPENDENT, and libpq reads `PGTZ`, `PGDATESTYLE`,
`PGCLIENTENCODING` and `PGOPTIONS` from the process environment and applies them as
startup options. One shell variable, no code change, and a writer and a reader disagree
about what a date means.

SO THE HOSTILE ENVIRONMENTS HERE ARE SET, NOT DESCRIBED. Each rendering below is
produced by a connection made with those variables actually exported, which is the
delivery mechanism the ruling is about.

AND `col::text` IS THE CAST, NOT THE TYPE'S OUTPUT FUNCTION. The two disagree for
`boolean` and `char(n)`, and `COPY ... TO STDOUT` is what reaches the output function,
so both spellings are printed side by side rather than assumed equal.
"""

from __future__ import annotations

import sys
import time

import psycopg
from probe_postgres_session import (
    LIBPQ_RENDERING_ENV,
    PROBE_SCHEMA,
    clean_env,
    connect,
    heading,
    libpq_env,
    one,
    row,
    rows,
    setup_schema,
    teardown_schema,
)

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


def measure_all() -> None:
    """Sections 7, 7b, 7c, 8, 9 and 9b, in order. Called by the main probe."""
    measure_7_guc_matrix()
    measure_8_cast_versus_output_function()
    measure_9_watermark_blind_spots()


def main() -> int:
    setup_schema()
    try:
        measure_all()
    finally:
        teardown_schema()
    return 0


if __name__ == "__main__":
    sys.exit(main())
