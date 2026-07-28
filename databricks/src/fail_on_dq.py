# databricks/src/fail_on_dq.py
"""Job task (runs only when the gate fails): make the run end FAILED so a bad
batch visibly blocks promotion — the compelling 'gate blocked' evidence.

The quarantine table is a TASK PARAMETER, not a constant: this task is shared by
the lookup job and the Estabelecimentos job, which quarantine to different tables
(`bronze_cnpj_lookup_quarantine` vs `bronze_cnpj_estab_quarantine`). This message
is the first instruction a triager gets — ADR 0006's workflow starts with "a
human has read the quarantine" — and while it was hardcoded to the lookup table
it sent two real Estabelecimentos runs to the wrong table entirely: the lookup
quarantine, which `dq_gate.py` OVERWRITES with the lookup gate's own rejects on
every lookup run and which therefore never contains an Estabelecimentos row. A
triager following that message finds no trace of the batch that was blocked.

Raises (instead of sys.exit) so the failure carries the reason into the run
output as a normal task error, not an opaque INTERNAL_ERROR."""
import sys

from opl.config import DEFAULT


def _qualify(table: str) -> str:
    # Accept a bare table name (qualified here, so catalog/schema stay in
    # opl.config alone) or an already-qualified one.
    return table if "." in table else DEFAULT.table(table)


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # A missing parameter must not stop this task from failing the run: pointing
    # nowhere is bad, swallowing a DQ block would be worse.
    where = (
        _qualify(args[0]) if args
        else "this job's quarantine table (fail_on_dq was given no table name)"
    )
    raise RuntimeError(
        f"DQ gate rejected rows - promotion blocked; see the quarantine table "
        f"({where}) for reject reasons."
    )


if __name__ == "__main__":
    main()
