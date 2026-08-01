# databricks/src/ensure_masked_table.py
"""Job task: create this contract's BRONZE and QUARANTINE tables EMPTY and apply
their column masks, before anything is ingested.

Runs FIRST in its job, ahead of unzip. Everywhere else in this repo a bronze table
is created by `promote_batch`'s `saveAsTable(...)` in append mode and a quarantine
table by the gate's, which for a contract holding personal names creates them with
the names already in them and lets the mask arrive afterwards. This task is the
ordering that makes "the control was applied when the data landed" true.

BRONZE AND QUARANTINE, NOT STAGING. The third table is a deliberate exclusion with
a mechanism behind it, not the part nobody got to: `promote_batch` READS staging and
writes what it read into bronze, and a UC mask applies to that read, so masking
staging would put `***` into bronze permanently and would make the DQ gate stop
rejecting rows with a missing name. `opl.bronze.masking.masked_table_ddls` carries
the argument and ADR 0008 records what covers staging instead.

IDEMPOTENT, every statement, which matters because `max_retries: 0` does not
prevent a retry on INTERNAL_ERROR. `CREATE TABLE IF NOT EXISTS` is a no-op over a
populated table and is deliberately not `CREATE OR REPLACE TABLE`, which would drop
its rows -- both socios tables are populated today, so that is not hypothetical;
`CREATE OR REPLACE FUNCTION` is the documented way to modify a function a live mask
references; and re-applying `SET MASK` to an already-masked column was PROBED
against the live workspace and succeeds. `opl.bronze.masking.set_mask_ddl` records
why that is still preferred to a `DROP MASK` first, which would unmask a populated
table on every monthly re-run.

WHY THESE TABLES CARRY NO CHECK CONSTRAINT. UC refuses a CHECK on a masked table,
so the socios registry entry declares NOT NULLs only. That is enforced at import by
`registry._assert_no_masked_contract_declares_a_check_constraint`, not here --
`promote_batch` is the task that issues constraint DDL, and it would do so after
its append had already committed. Only bronze ever receives that DDL; the quarantine
table has never carried a constraint, which is why masking it costs nothing.

A no-op for a table with no masked columns, so the same task can sit in any job's
YAML without a per-table branch -- and, more to the point, so that adding it to
another job does not hand-create empty tables for a contract whose schema nobody
has checked against the stream.

argv: [table]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.masking import (
    MASK_FUNCTION,
    MASKED_COLUMNS,
    mask_function_ddl,
    masked_table_ddls,
    set_mask_ddl,
)
from opl.bronze.registry import table_spec
from opl.config import DEFAULT


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # The table is resolved BEFORE the session, like every other task: this one runs
    # first in its job, so a mistyped table name should not cost a serverless start
    # to be told about. An absent argument is passed through as "" so that
    # `table_spec` refuses it naming the registered tables.
    spec = table_spec(args[0] if args else "")
    columns = MASKED_COLUMNS.get(spec.contract, ())
    if not columns:
        print(f"ensure_masked_table: {spec.name} declares no masked column -- nothing "
              "to create or mask here; its bronze and quarantine tables are created "
              "by their own writers as before")
        return
    spark = SparkSession.builder.getOrCreate()
    function = DEFAULT.table(MASK_FUNCTION)
    # WHICH tables is `masking`'s decision, not this task's -- the exclusion of
    # staging is load-bearing and belongs next to the DDL it is an argument about.
    tables = masked_table_ddls(
        bronze=DEFAULT.table(spec.bronze),
        quarantine=DEFAULT.table(spec.quarantine),
        contract=spec.contract,
    )
    # ORDER IS THE POINT OF THIS FUNCTION. Every table before any mask, or a writer
    # creates it instead and the names land first; the function before the first
    # SET MASK, or the ALTER fails on a missing routine.
    for table, ddl in tables:
        spark.sql(ddl)
        print(f"ensure_masked_table: {table} exists and is EMPTY or already populated "
              "-- created here rather than by its writer, so the masks below precede "
              "any row it has not got yet")
    spark.sql(mask_function_ddl(function))
    for table, _ in tables:
        for column in columns:
            spark.sql(set_mask_ddl(table, column, function))
            print(f"ensure_masked_table: {table}.{column} masked by {function}")


if __name__ == "__main__":
    main()
