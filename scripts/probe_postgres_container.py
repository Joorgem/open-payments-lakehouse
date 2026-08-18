"""Task 0's container persistence matrix: what a `docker compose` verb costs.

WHY THIS IS MEASURED AT ALL. The plan states the answer -- restart survives, only
`down`+`up` destroys -- and revision 1 of that same plan stated the OPPOSITE and was
wrong, inside the section whose subject is refusing inherited errors. T6's idempotent
seeder rests on the answer, so it is run rather than quoted.

IT IS DESTRUCTIVE, WHICH IS WHY IT IS NOT IN THE DEFAULT PROBE RUN. "empty on a fresh
volume" cannot be measured without `docker compose down -v`, and once Task 3 seeds
`merchant` an unguarded default would delete it.

EVERY COMMAND IS SCOPED TO THE `postgres` SERVICE. Measured: an unscoped
`docker compose up -d` starts redpanda as well, so the first run of this matrix created
a container that was not there before it and orphaned a second anonymous volume -- the
dangling count moved by two where the measurement is about one PGDATA.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import psycopg
from probe_postgres_session import clean_env, connect, heading, one, rows

REPO_ROOT = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------------
# 1. The container persistence matrix (opt-in: it destroys the database)
# --------------------------------------------------------------------------------


class ComposeFailed(RuntimeError):
    """A `docker compose` verb did not run, so nothing below it measures anything."""


def compose(*args: str) -> str:
    """Run `docker compose ...` at the repo root and return its combined output.

    RAISES ON A NON-ZERO EXIT, and that is the difference between a matrix and a table of
    numbers. This ran with `check=False` and every call site discarded the result, so a
    failed `docker compose restart` left the container up FROM BEFORE -- `report_survival`
    then read an unchanged marker and printed `SURVIVES` for a verb that never ran. A
    measurement describing nothing, in the exact shape it is printed in when it is real,
    and pasted into an evidence document as such. This whole probe exists because revision
    1 of the plan asserted this matrix and got it backwards; a silent green is how that
    happens twice.

    `check=True` would raise too, but `CalledProcessError` carries the exit code and not
    the output -- and with `capture_output=True` the reason docker refused is only in the
    output. So the check is written out, and stdout+stderr goes in the message."""
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    output = (result.stdout + result.stderr).strip()
    if result.returncode != 0:
        raise ComposeFailed(
            f"`docker compose {' '.join(args)}` exited {result.returncode}. Every "
            "measurement after this verb would describe the state the PREVIOUS verb left, "
            f"so the probe stops here rather than reporting it. Output:\n{output}"
        )
    return output


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
    print("  every command below is SCOPED to the `postgres` service. An unscoped")
    print("  `docker compose up -d` starts redpanda too, which is a state change on the")
    print("  operator's box that this measurement has no business making.")
    print(f"  dangling volumes before:  {dangling_volume_count()}")
    print("  docker compose down -v postgres ...")
    compose("down", "-v", "postgres")
    print("  docker compose up -d postgres ...")
    compose("up", "-d", "postgres")
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

    compose("restart", "postgres")
    report_survival("restart:", marker)

    compose("stop", "postgres")
    compose("start", "postgres")
    report_survival("stop + start:", marker)

    before = postgres_volume()
    compose("down", "postgres")
    compose("up", "-d", "postgres")
    report_survival("down (no -v) + up -d:", marker)
    print(f"    PGDATA volume before down: {before}")
    print(f"    PGDATA volume after up:    {postgres_volume()}")
    print(f"    dangling volumes now:      {dangling_volume_count()}  (T6: an orphaned PGDATA)")

    with clean_env(), connect(search_path=False) as conn:
        conn.execute("DROP TABLE IF EXISTS public.probe_persistence")


def main() -> int:
    measure_1_persistence()
    return 0


if __name__ == "__main__":
    sys.exit(main())
