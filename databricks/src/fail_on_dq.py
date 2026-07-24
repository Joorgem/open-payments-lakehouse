# databricks/src/fail_on_dq.py
"""Job task (runs only when the gate fails): make the run end FAILED so a bad
batch visibly blocks promotion — the compelling 'gate blocked' evidence.

Raises (instead of sys.exit) so the failure carries the reason into the run
output as a normal task error, not an opaque INTERNAL_ERROR."""


def main() -> None:
    raise RuntimeError(
        "DQ gate rejected rows - promotion blocked; see the quarantine table "
        "(workspace.default.bronze_cnpj_lookup_quarantine) for reject reasons."
    )


if __name__ == "__main__":
    main()
