# tests/integration/test_postgres_source_live.py
"""`opl.extraction.postgres_source`'s GUC pin against the real container, under a HOSTILE
environment, with the expected renderings as COMMITTED LITERALS.

WHY THIS FILE EXISTS WHEN `tests/test_postgres_source.py` ALREADY TESTS THE PIN. It does
not, and cannot. That file is hermetic by design -- the module executes no I/O and every
statement runs against a fake -- and its `_PinConnection` answers `current_setting(%s)` from
a dict THE TEST supplies, independent of whether the code under test ever executed
`set_config`. So it distinguishes a correct read-back check from a broken one, which is what
it is for, and it cannot distinguish "the SET ran and defeated a hostile environment" from
"the SET never ran and the container's defaults happen to disagree with the pin". Nothing
committed made that distinction until this file.

PLAN T4 DEMANDS EXACTLY THIS SHAPE AND REJECTS THE OBVIOUS ALTERNATIVE BY NAME. Revision 1's
test compared the extractor against `psql` -- "two clients inheriting the same unpinned
defaults, so it passes under every GUC setting and can never catch this". The literals below
are committed, so the comparison is against a value this repository decided on rather than
against a second client that would move with the environment.

THE CONTROL IS THE HALF THAT MAKES THE ASSERTION MEAN SOMETHING, and it is the first test.
If the hostile environment did not reach the server, the pinned run would pass over a
connection that was never attacked. So an UNPINNED connection under the same environment is
asserted to render all five values DIFFERENTLY, and to differ in the specific way T4
measured: `SQL, DMY` swaps day and month, `PGTZ` moves the wall clock three hours,
`extra_float_digits=-3` renders `0.3` for a value that does not round-trip.

THE DELIVERY MECHANISM IS WHY THIS IS BLOCKING RATHER THAN FUSSY. libpq turns `PGTZ`,
`PGDATESTYLE`, `PGCLIENTENCODING` and `PGOPTIONS` into startup options with NO CODE CHANGE
-- one shell variable on a host or in a CI job.

MARKED `postgres` AS WELL AS `integration`: this needs one container and nothing else, and
`-m integration` also selects Redpanda and two live-WebDAV modules (plan T2b). It reads and
never writes -- no schema, no seed, no `DROP` -- so it is safe to run against `public.merchant`
mid-phase, which a test that seeded would not be.
"""
from __future__ import annotations

import sys
from pathlib import Path

import psycopg
import pytest

from opl.extraction.postgres_source import RENDERING_GUCS, pin_rendering_gucs

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from probe_postgres_session import clean_env, dsn, libpq_env  # noqa: E402

# ONE STATEMENT RENDERING ONE VALUE PER PINNED GUC THAT HAS A VISIBLE RENDERING. `TimeZone`
# and `DateStyle` are both read off the `timestamptz`; `extra_float_digits` off the `float8`;
# `DateStyle` again off the bare `date`; `IntervalStyle` off the interval; `bytea_output` off
# the bytea. `client_encoding` and `search_path` render nothing and are covered by the
# read-back alone -- said here rather than left as five checks for seven pins.
#
# THE `::float8` CASTS ARE LOAD-BearING. `0.1 + 0.2` in SQL is NUMERIC arithmetic and lands
# on exactly `0.3` under every setting, so the same expression without them measures
# nothing -- which Task 0's first version of this table did.
_RENDER = (
    "SELECT '2026-08-15 17:23:01.123456+00'::timestamptz::text, "
    "(0.1::float8 + 0.2::float8)::text, "
    "'2026-08-15'::date::text, "
    "'5 days 3 hours'::interval::text, "
    "decode('4f504c', 'hex')::text"
)

# WHAT THE PIN MUST PRODUCE, committed rather than re-derived from a second connection.
_PINNED_RENDERING = (
    "2026-08-15 17:23:01.123456+00",
    "0.30000000000000004",
    "2026-08-15",
    "P5DT3H",
    "\\x4f504c",
)

# WHAT THE SAME FIVE EXPRESSIONS RENDER AS WHEN THE ENVIRONMENT WINS, measured on this
# container. Every one differs from its pinned counterpart, which is what makes the hostile
# environment a real attack rather than a decorative one.
_UNPINNED_RENDERING = (
    "15/08/2026 14:23:01.123456 -03",
    "0.3",
    "15/08/2026",
    "5 days 03:00:00",
    "OPL",
)

_HOSTILE = {
    "PGTZ": "America/Sao_Paulo",
    "PGDATESTYLE": "SQL, DMY",
    "PGOPTIONS": "-c extra_float_digits=-3 -c IntervalStyle=postgres -c bytea_output=escape",
}


def _rendered(conn: psycopg.Connection) -> tuple[str, ...]:
    return tuple(conn.execute(_RENDER).fetchone())


def test_the_hostile_environment_really_reaches_the_server():
    """THE CONTROL, and without it the next test proves nothing.

    A pinned connection whose environment never arrived would pass every assertion below
    while measuring a session nobody attacked. This asserts the attack landed, and it
    asserts it through the RENDERING rather than only through `SHOW`: the five values come
    back in the environment's spelling, not the pin's.

    `clean_env()` wraps it because the operator's own shell may already export `PGTZ`, in
    which case an unguarded baseline measures their machine instead of this fixture."""
    with clean_env(), libpq_env(**_HOSTILE):
        with psycopg.connect(dsn(), autocommit=True) as conn:
            assert conn.execute("SHOW DateStyle").fetchone()[0] == "SQL, DMY"
            assert _rendered(conn) == _UNPINNED_RENDERING

    assert _UNPINNED_RENDERING != _PINNED_RENDERING, "the fixture stopped being an attack"
    assert not set(_UNPINNED_RENDERING) & set(_PINNED_RENDERING), (
        "every one of the five must differ, or the value that does not differ is measuring "
        "the container's default rather than the pin"
    )


def test_the_pin_defeats_the_hostile_environment_and_renders_the_committed_literals():
    """T4's closing test: the SET ran, it read back, and the bytes are the ones committed.

    TWO ASSERTIONS AND NOT ONE, because they fail for different reasons. The read-back is
    what `pin_rendering_gucs` itself refuses on -- a `SET` that is not read back is not a
    pin -- and it would still pass over a pin that set the seven GUCs to values that
    rendered something other than what this repository expects. The rendering comparison is
    the one that says the landed bytes are right."""
    with clean_env(), libpq_env(**_HOSTILE):
        with psycopg.connect(dsn(), autocommit=True) as conn:
            assert pin_rendering_gucs(conn) == RENDERING_GUCS
            assert _rendered(conn) == _PINNED_RENDERING


def test_the_pin_is_not_merely_agreeing_with_the_containers_defaults():
    """`IntervalStyle` IS THE PROOF, AND IT IS ALSO A CORRECTION TO T4's OWN WORDING.

    T4 records that under the pin "the rendering is byte-identical to the clean baseline".
    That is true of six of the seven and FALSE of `IntervalStyle`: the server default is
    `postgres`, which renders `5 days 03:00:00`, and the pin is `iso_8601`, which renders
    `P5DT3H`. The pin is not agreement with the defaults -- it is a decision that overrides
    one of them.

    Which makes it this file's sharpest control. Every other assertion here would survive an
    implementation whose `set_config` calls did nothing, on a container whose defaults
    happened to match; this one would not, on any container, because the pinned value is
    one no Postgres ships with."""
    with clean_env():
        with psycopg.connect(dsn(), autocommit=True) as conn:
            baseline = _rendered(conn)
            assert pin_rendering_gucs(conn) == RENDERING_GUCS
            assert _rendered(conn) == _PINNED_RENDERING

    assert baseline[3] != _PINNED_RENDERING[3], (
        "the container's default IntervalStyle now renders what the pin does. That removes "
        "this file's one assertion that cannot be satisfied by a no-op pin -- re-read T4's "
        "'byte-identical to the clean baseline' before deleting anything here"
    )
