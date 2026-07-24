# databricks/src/fail_on_dq.py
"""Job task (runs only when the gate fails): make the run end FAILED so a bad
batch visibly blocks promotion — the compelling 'gate blocked' evidence."""
import sys


def main() -> None:
    print("fail_on_dq: DQ gate rejected rows — promotion blocked, failing the run.")
    sys.exit(1)


if __name__ == "__main__":
    main()
