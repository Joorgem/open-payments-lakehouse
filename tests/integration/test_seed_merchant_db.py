# tests/integration/test_seed_merchant_db.py
"""The seeder against the real container: idempotence, the GUC pin, and the miss set.

MARKED `postgres` AS WELL AS `integration`, and the two are not the same claim.
`-m integration` also selects Redpanda and two live-WebDAV modules, so a CI job with one
Postgres service container turns red on five of six tests (plan T2b). `postgres` selects
exactly what one container can satisfy.

IT WORKS IN ITS OWN SCHEMA. `public.merchant` is the phase's artefact and Task 6 runs a
two-day snapshot across it; a test that re-seeds it mid-phase would destroy an extraction's
source between its two observations. So the schema is a parameter, this file uses
`f_db_task3_test`, and it drops it afterwards.

THE OUT-OF-ORDER TEST PLAYS THE EXTRACTOR, because Task 4 has not been written. It takes
snapshot 1 and its watermark inside ONE `REPEATABLE READ READ ONLY` transaction -- T2's
ruling, so that the miss set is a genuine complement of one MVCC state rather than a
definitional one -- and it runs the incremental query inside snapshot 2's transaction for
the same reason.
"""

import sys
from datetime import timedelta
from pathlib import Path

import psycopg
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import merchant_population as population  # noqa: E402
import seed_merchant_db as seeder  # noqa: E402
from probe_postgres_session import libpq_env  # noqa: E402

SCHEMA = "f_db_task3_test"

PAYLOAD_SQL = (
    "SELECT merchant_id::text, ROW(cnpj, legal_name, trade_name, status, mcc, "
    "settlement_account, risk_tier, credit_limit, onboarded_on)::text "
    f'FROM {SCHEMA}.merchant ORDER BY merchant_id::text COLLATE "C"'
)


@pytest.fixture
def conn():
    connection = seeder.connect()
    try:
        yield connection
    finally:
        connection.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        connection.close()


@pytest.fixture
def plan():
    return population.build_plan()


def _snapshot(connection: psycopg.Connection, watermark=None):
    """One `REPEATABLE READ READ ONLY` transaction: the rows, the watermark, the increment.

    All three come out of one transaction because T2 rules they must: run the incremental
    query against the landed snapshot instead and the deletes are missing by construction,
    run it against the live database and it is a third read at a third instant.
    """
    connection.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
    rows = dict(connection.execute(PAYLOAD_SQL).fetchall())
    stamp = connection.execute(f"SELECT max(updated_at) FROM {SCHEMA}.merchant").fetchone()[0]
    incremental = set()
    if watermark is not None:
        incremental = {
            key
            for (key,) in connection.execute(
                f"SELECT merchant_id::text FROM {SCHEMA}.merchant WHERE updated_at > %s",
                (watermark,),
            ).fetchall()
        }
    connection.execute("COMMIT")
    return rows, stamp, incremental


# --------------------------------------------------------------------------------
# Idempotence, against a POPULATED database
# --------------------------------------------------------------------------------


def test_seeding_twice_reproduces_the_table_byte_for_byte(conn, plan):
    """The common path meets yesterday's data: `restart` and `stop`+`start` keep the volume.

    A seeder that assumes an empty database fails on a `CREATE TABLE`, or hits the primary
    key, or -- worst -- double-seeds and the authored class counts silently stop matching.
    """
    first = seeder.seed(conn, plan, SCHEMA)
    second = seeder.seed(conn, plan, SCHEMA)
    assert first["digest"] == second["digest"]
    assert (first["merchants"], first["distinct_cnpj_roots"]) == (1088, 1024)
    assert second["merchants"] == population.SNAPSHOT_1_ROWS


def test_seeding_over_a_mutated_table_returns_it_to_the_seeded_state(conn, plan):
    """Idempotence that a mutation cannot defeat, which is the case that actually arises."""
    seeded = seeder.seed(conn, plan, SCHEMA)
    seeder.mutate(conn, plan, SCHEMA, release=lambda: None)
    assert seeder.census(conn, SCHEMA)["digest"] != seeded["digest"]
    assert seeder.seed(conn, plan, SCHEMA)["digest"] == seeded["digest"]


def test_mutate_refuses_a_table_that_is_not_in_the_seeded_state(conn, plan):
    """`mutate` is NOT idempotent and says so rather than double-mutating in silence."""
    seeder.seed(conn, plan, SCHEMA)
    seeder.mutate(conn, plan, SCHEMA, release=lambda: None)
    with pytest.raises(seeder.Refusal, match="rows, not the seeded|after the seed window"):
        seeder.mutate(conn, plan, SCHEMA, release=lambda: None)


# --------------------------------------------------------------------------------
# T4 -- the pin, read back, inside a hostile environment
# --------------------------------------------------------------------------------


def test_the_rendering_gucs_read_back_as_pinned(conn):
    assert seeder.pin_rendering_gucs(conn) == seeder.PINS


def test_the_pin_defeats_pgoptions_and_renders_identically(conn, plan):
    """A `SET` that is not read back is not a pin: `PGOPTIONS` is applied at startup.

    Measured in Task 0: a writer at `DateStyle='SQL, DMY'` renders `03/08/2026` and an
    `ISO, MDY` reader parses `2026-03-08`, silently. The hostile environment here attacks
    `TimeZone`, `DateStyle` and `extra_float_digits`; the other four are already correct at
    startup and are NOT evidence that the pin works, which is why the digest comparison
    below -- not the read-back -- is the assertion that matters.
    """
    clean = seeder.seed(conn, plan, SCHEMA)["digest"]
    hostile_env = {
        "PGTZ": "America/Sao_Paulo",
        "PGDATESTYLE": "SQL, DMY",
        "PGOPTIONS": "-c extra_float_digits=-3",
    }
    with libpq_env(**hostile_env):
        unpinned = psycopg.connect(seeder.dsn(), autocommit=True)
        try:
            assert unpinned.execute("SHOW DateStyle").fetchone()[0] == "SQL, DMY"
            hostile = seeder.connect()
            try:
                assert seeder.pin_rendering_gucs(hostile) == seeder.PINS
                assert seeder.table_digest(hostile, SCHEMA) == clean
            finally:
                hostile.close()
        finally:
            unpinned.close()


def test_a_guc_that_does_not_read_back_stops_the_run(conn):
    """The refusal is the mechanism; a pin that only warns is a pin nobody notices.

    `DateStyle` is the case that proves the read-back is real rather than decorative:
    `set_config('DateStyle', 'ISO')` SUCCEEDS and then `SHOW DateStyle` answers
    `ISO, MDY`. A pin that only checked for an error would have accepted it.
    """
    with pytest.MonkeyPatch.context() as patch:
        patch.setitem(seeder.PINS, "DateStyle", "ISO")
        with pytest.raises(seeder.Refusal, match="did not read back as pinned"):
            seeder.pin_rendering_gucs(conn)


def test_the_server_encoding_is_asserted_and_not_assumed(conn):
    """T4: a LATIN1 database was created on this very container during Task 0."""
    assert conn.execute("SHOW server_encoding").fetchone()[0] == "UTF8"
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(seeder, "REQUIRED_SERVER_ENCODING", "LATIN1")
        with pytest.raises(seeder.Refusal, match="server_encoding"):
            seeder.pin_rendering_gucs(conn)


def test_the_dsn_is_never_printed_with_its_password(conn):
    """`redacted_dsn` blanks every `password=` token, not one committed literal."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("OPL_POSTGRES_DSN", "host=h port=5433 dbname=d user=u password=hunter2")
        assert "hunter2" not in seeder.redacted_dsn()


# --------------------------------------------------------------------------------
# The trigger, in both directions
# --------------------------------------------------------------------------------


def test_the_trigger_moves_updated_at_for_every_armed_class_and_none_of_the_disarmed(conn, plan):
    """The control in BOTH directions, which is what makes either number mean anything.

    A `DEFAULT now()` is an INSERT-time default and does not fire on UPDATE (measured), so
    the armed classes need the trigger; the silent class is the same statement with the
    trigger disarmed. A silent class that quietly started stamping would otherwise vanish
    from the headline with no test going red.
    """
    seeder.seed(conn, plan, SCHEMA)
    applied = seeder.mutate(conn, plan, SCHEMA, release=lambda: None)["classes"]
    assert applied["update_moving_updated_at"] == {"rows": 48, "moved_updated_at": 48}
    assert applied["update_not_moving_updated_at"] == {"rows": 24, "moved_updated_at": 0}
    assert applied["out_of_order_commit"] == {"rows": 8, "moved_updated_at": 8}
    assert applied["watermark_advance"] == {"rows": 8, "moved_updated_at": 8}
    assert (applied["insert"]["rows"], applied["hard_delete"]["rows"]) == (32, 16)


# --------------------------------------------------------------------------------
# The headline: the row no watermark run will ever return
# --------------------------------------------------------------------------------


def test_the_snapshot_diff_catches_48_rows_the_watermark_never_will(conn, plan):
    """The miss set, measured as a complement rather than typed into a document.

    The extractor role is played here: snapshot 1 and its watermark inside one
    `REPEATABLE READ READ ONLY` transaction, then the mutation's held-open transaction
    commits, then snapshot 2 and the incremental query inside a second one. The 8
    out-of-order rows are stamped BEFORE the watermark and committed AFTER it, so
    `WHERE updated_at > watermark` cannot return them -- not now, not ever.
    """
    seeder.seed(conn, plan, SCHEMA)
    observer = seeder.connect()
    captured: dict = {}
    try:
        seeder.mutate(
            conn, plan, SCHEMA,
            release=lambda: captured.update(
                zip(("rows", "watermark"), _snapshot(observer)[:2], strict=False)
            ),
        )
        snapshot_2, _, incremental = _snapshot(observer, watermark=captured["watermark"])
    finally:
        observer.close()

    snapshot_1 = captured["rows"]
    assert len(snapshot_1) == 1088 and len(snapshot_2) == 1104
    gone = set(snapshot_1) - set(snapshot_2)
    arrived = set(snapshot_2) - set(snapshot_1)
    changed = {key for key in set(snapshot_1) & set(snapshot_2)
               if snapshot_1[key] != snapshot_2[key]}
    diff_caught = gone | arrived | changed
    assert (len(gone), len(arrived), len(changed)) == (16, 32, 80)
    assert len(incremental) == 80
    assert incremental < diff_caught
    missed = diff_caught - incremental
    assert len(missed) == population.WATERMARK_MISS == 48
    expected = {
        row.merchant_id
        for name in ("hard_delete", "update_not_moving_updated_at", "out_of_order_commit")
        for row in plan.rows_for(name)
    }
    assert missed == expected


def test_the_out_of_order_rows_stay_missed_after_the_slow_transaction_commits(conn, plan):
    """`[]` now, and `[]` forever -- the property that makes this class not a race.

    Run the same watermark query again once everything has settled. It still cannot see the
    eight rows, because `updated_at` orders by transaction START and visibility orders by
    transaction COMMIT, and nothing later reconciles the two.
    """
    seeder.seed(conn, plan, SCHEMA)
    observer = seeder.connect()
    captured: dict = {}
    try:
        seeder.mutate(
            conn, plan, SCHEMA,
            release=lambda: captured.update(watermark=_snapshot(observer)[1]),
        )
        held = {row.merchant_id for row in plan.rows_for("out_of_order_commit")}
        after = conn.execute(
            f"SELECT merchant_id::text FROM {SCHEMA}.merchant WHERE updated_at > %s",
            (captured["watermark"],),
        ).fetchall()
    finally:
        observer.close()
    assert not held & {key for (key,) in after}
    landed = dict(
        conn.execute(
            f"SELECT merchant_id::text, status FROM {SCHEMA}.merchant "
            "WHERE merchant_id = ANY(%s::uuid[])",
            (sorted(held),),
        ).fetchall()
    )
    # The rows DID commit and their new payload IS in the table -- which is what makes the
    # empty watermark result above a finding rather than a query that never returns anything.
    assert landed == {
        row.merchant_id: population.mutated(row).status
        for row in plan.rows_for("out_of_order_commit")
    }


# --------------------------------------------------------------------------------
# The seeded schema itself
# --------------------------------------------------------------------------------


def test_the_seeded_columns_are_the_types_plan_4_fixed(conn, plan):
    """Excluded on purpose: char(n), float8, jsonb, json, arrays, interval and money."""
    seeder.seed(conn, plan, SCHEMA)
    types = dict(
        conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = 'merchant'",
            (SCHEMA,),
        ).fetchall()
    )
    assert types == {
        "merchant_id": "uuid",
        "cnpj": "text",
        "legal_name": "text",
        "trade_name": "text",
        "status": "text",
        "mcc": "text",
        "settlement_account": "text",
        "risk_tier": "text",
        "credit_limit": "numeric",
        "onboarded_on": "date",
        "updated_at": "timestamp with time zone",
    }
    scale = conn.execute(
        "SELECT numeric_precision, numeric_scale FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = 'merchant' AND column_name = 'credit_limit'",
        (SCHEMA,),
    ).fetchone()
    assert scale == (14, 2)


def test_onboarded_on_is_not_nullable(conn, plan):
    """T5: `effectivity.py:64-69` -- a NULL entry date sorts FIRST and beats a real one."""
    seeder.seed(conn, plan, SCHEMA)
    nullable = conn.execute(
        "SELECT column_name, is_nullable FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = 'merchant'",
        (SCHEMA,),
    ).fetchall()
    assert dict(nullable)["onboarded_on"] == "NO"
    assert [name for name, flag in nullable if flag == "YES"] == ["trade_name"]


def test_the_landed_numeric_keeps_its_declared_scale_and_the_cnpj_keeps_its_zero(conn, plan):
    """The two renderings F-API's `5.07730` defect and the 142 leading zeros are about."""
    seeder.seed(conn, plan, SCHEMA)
    rendered = conn.execute(
        f"SELECT count(*) FILTER (WHERE credit_limit::text ~ '[.][0-9]{{2}}$'), "
        f"count(*) FILTER (WHERE left(cnpj, 1) = '0'), count(*) FROM {SCHEMA}.merchant"
    ).fetchone()
    assert rendered == (1088, 151, 1088)


# --------------------------------------------------------------------------------
# The hand-off to Task 4/6, which is a race until something signals readiness
# --------------------------------------------------------------------------------


def test_the_readiness_file_appears_only_once_the_watermark_is_above_t1(conn, plan, tmp_path):
    """MEASURED AS A RACE, not anticipated as one.

    An extractor that starts snapshot 1 the moment it sees an `idle in transaction`
    session catches `mutate` BETWEEN t1 and t2 -- verified against this container, the
    watermark came back as the seed's own maximum. A snapshot 1 taken there records a
    watermark BELOW t1, `WHERE updated_at > watermark` then returns the out-of-order rows
    perfectly well, and the phase's one non-authored number disappears while every other
    count stays correct.

    So `mutate` writes the readiness file itself, after the `t2 > t1` refusal has passed,
    and carries both stamps so a caller can check its own watermark against t2.
    """
    seeder.seed(conn, plan, SCHEMA)
    ready = tmp_path / "ready"
    seen: dict = {}

    def release():
        seen["exists"] = ready.exists()
        seen["text"] = ready.read_text(encoding="utf-8")
        seen["watermark"] = _snapshot(conn)[1]

    measured = seeder.mutate(conn, plan, SCHEMA, release, ready=ready)
    assert seen["exists"]
    assert seen["text"] == f"t1={measured['t1'].isoformat()}\nt2={measured['t2'].isoformat()}\n"
    assert seen["watermark"] == measured["t2"]
    assert measured["t2"] > measured["t1"]


def test_mutate_writes_no_readiness_file_when_it_is_not_asked_to(conn, plan, tmp_path):
    """The signal is opt-in, and its absence must not be mistaken for readiness."""
    seeder.seed(conn, plan, SCHEMA)
    seeder.mutate(conn, plan, SCHEMA, release=lambda: None)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.postgres
def test_a_seed_window_the_server_says_is_in_the_future_is_refused(conn):
    """A future-dated seed passes every other refusal and silently inflates the headline.

    THE FAILURE THIS PINS WAS MEASURED, NOT IMAGINED. `_refuse_unless_seeded` compared
    `max(updated_at)` against `SEED_UPDATED_CEILING` -- one authored constant against
    another -- so a seed window in the FUTURE passed it. The Task 3 review drove that case
    end to end: every class count still reported exactly right, the `t2 > t1` refusal still
    passed, the readiness file was still written, and the published watermark miss silently
    became 128 instead of 48. Same shape as the readiness race, and in the direction that
    FLATTERS the number, which is the worse one -- nothing looks wrong.

    THE TRIGGER IS DISARMED TO SET THIS UP, AND THAT IS NOT INCIDENTAL. The seeded table
    carries a `BEFORE UPDATE` trigger, so a plain `UPDATE ... + interval '1 year'` is
    overwritten with `now()` and the fixture never reaches the state under test. The
    controller's first attempt at this test was defeated exactly that way, and reported a
    guard as firing when what had fired was the older literal check.
    """
    seeder.seed(conn, population.build_plan(), SCHEMA)
    ceiling = seeder.SEED_UPDATED_CEILING
    conn.execute("SET session_replication_role = 'replica'")
    try:
        conn.execute(f"UPDATE {SCHEMA}.merchant SET updated_at = updated_at + interval '1 year'")
        seeder.SEED_UPDATED_CEILING = ceiling + timedelta(days=365)
        with pytest.raises(seeder.Refusal, match="has not happened yet"):
            seeder._refuse_unless_seeded(conn, SCHEMA)
    finally:
        seeder.SEED_UPDATED_CEILING = ceiling
        conn.execute("SET session_replication_role = 'origin'")
