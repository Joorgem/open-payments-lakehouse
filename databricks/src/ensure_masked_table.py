# databricks/src/ensure_masked_table.py
"""Job task: create this table's bronze table EMPTY and apply its column masks,
before anything is ingested.

Runs FIRST in its job, ahead of unzip. Everywhere else in this repo a bronze table
is created by `promote_batch`'s `saveAsTable(...)` in append mode, which for a
table holding personal names creates it with the names already in it and lets the
mask arrive afterwards. This task is the ordering that makes "the control was
applied when the data landed" true.

IDEMPOTENT, all three statements, which matters because `max_retries: 0` does not
prevent a retry on INTERNAL_ERROR. `CREATE TABLE IF NOT EXISTS` is a no-op over a
populated table and is deliberately not `CREATE OR REPLACE TABLE`, which would drop
its rows; `CREATE OR REPLACE FUNCTION` is the documented way to modify a function a
live mask references; and re-applying `SET MASK` to an already-masked column was
PROBED against the live workspace and succeeds. `opl.bronze.masking.set_mask_ddl`
records why that is still preferred to a `DROP MASK` first, which would unmask a
populated table on every monthly re-run.

WHY THIS TABLE CARRIES NO CHECK CONSTRAINT. UC refuses a CHECK on a masked table,
so the socios registry entry declares NOT NULLs only. That is enforced at import by
`registry._assert_no_masked_contract_declares_a_check_constraint`, not here --
`promote_batch` is the task that issues constraint DDL, and it would do so after
its append had already committed.

A no-op for a table with no masked columns, so the same task can sit in any job's
YAML without a per-table branch -- and, more to the point, so that adding it to
another job does not hand-create an empty bronze table for a contract whose
schema nobody has checked against the stream.

argv: [table]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.masking import (
    MASK_FUNCTION,
    MASKED_COLUMNS,
    create_table_ddl,
    mask_function_ddl,
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
              "to create or mask here; its bronze table is created by the promote's "
              "append as before")
        return
    spark = SparkSession.builder.getOrCreate()
    table = DEFAULT.table(spec.bronze)
    function = DEFAULT.table(MASK_FUNCTION)
    # ORDER IS THE POINT OF THIS FUNCTION. The table before the masks, or the append
    # creates it instead and the names land first; the function before the first
    # SET MASK, or the ALTER fails on a missing routine.
    spark.sql(create_table_ddl(table, spec.contract))
    print(f"ensure_masked_table: {table} exists and is EMPTY or already populated -- "
          "created here rather than by the first append, so the masks below precede "
          "any row")
    spark.sql(mask_function_ddl(function))
    for column in columns:
        spark.sql(set_mask_ddl(table, column, function))
        print(f"ensure_masked_table: {table}.{column} masked by {function}")


if __name__ == "__main__":
    main()
