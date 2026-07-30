# databricks/src/fail_on_dq.py
"""Job task (runs only when the gate fails): make the run end FAILED so a bad
batch visibly blocks promotion -- the compelling 'gate blocked' evidence.

The TABLE is the parameter, and the quarantine is derived from the registry. It
used to be the quarantine NAME, hardcoded per job YAML, and that hardcoding sent
two real Estabelecimentos runs to the lookup quarantine -- a table that never
contains an Estabelecimentos row. This message is the first instruction a triager
gets (ADR 0006's workflow starts with "a human has read the quarantine"), so
pointing it at the wrong table wastes the triage it exists to start.

Raises (instead of sys.exit) so the failure carries the reason into the run
output as a normal task error, not an opaque INTERNAL_ERROR.

argv: [table]"""
import sys

from opl.bronze.registry import UnknownTable, table_spec
from opl.config import DEFAULT


def _quarantine_of(args: list[str]) -> str:
    """Where to send the triager, or the most honest thing we can say instead.

    Every path through this returns a string; none of them raises. A missing or
    mistyped table must not stop this task from failing the run: pointing nowhere
    is bad, swallowing a DQ block would be worse. So the two bad cases are
    reported INSIDE the failure message rather than replacing it."""
    if not args or not args[0].strip():
        return "this job's quarantine table (fail_on_dq was given no table name)"
    try:
        return DEFAULT.table(table_spec(args[0]).quarantine)
    except UnknownTable as exc:
        return f"UNKNOWN -- fail_on_dq was given an unregistered table: {exc}"


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    raise RuntimeError(
        f"DQ gate rejected rows - promotion blocked; see the quarantine table "
        f"({_quarantine_of(args)}) for reject reasons."
    )


if __name__ == "__main__":
    main()
