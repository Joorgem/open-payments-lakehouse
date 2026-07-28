# databricks/src/promote_batch.py
"""Job task (gate passed): APPEND this batch's good rows to the bronze table,
once, then (re-)assert the declarative Delta constraints.

The append is idempotent for a given `_batch_id` and the operator guards that
refuse a batch id naming nothing live in `opl.bronze.promote`, which documents
why that shape was chosen and is unit-tested against a real Delta log. This file
is the job's entry point: it owns the table coordinates, the rule set and the
constraint DDL."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.autoloader import BRONZE_ESTAB_STAGING
from opl.bronze.promote import promote_batch
from opl.bronze.rules import rules_for
from opl.config import DEFAULT

BRONZE = "bronze_cnpj_estabelecimentos"


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    spark = SparkSession.builder.getOrCreate()
    tbl = DEFAULT.table(BRONZE)
    # An absent argument is passed through as "" rather than raising IndexError:
    # promote_batch owns refusing it, with a message aimed at the operator.
    result = promote_batch(
        spark,
        args[0] if args else "",
        staging_table=DEFAULT.table(BRONZE_ESTAB_STAGING),
        bronze_table=tbl,
        rules=rules_for("estabelecimentos"),
    )
    if result.already_promoted:
        print(f"promote_batch: batch {result.batch_id} is ALREADY in {tbl} -- append "
              "skipped (idempotent re-run of a promote that had committed)")
    else:
        print(f"promote_batch: appended {result.appended_rows} rows "
              f"(batch {result.batch_id}) to {tbl}")
    # The rejects of this batch were left in quarantine. For the operator job
    # that number is the point: a human read them and accepted them.
    print(f"promote_batch: {result.rejected_rows} rejected row(s) of batch "
          f"{result.batch_id} stay in quarantine, out of {tbl}")
    # Runs on BOTH paths and after the append, deliberately: this DDL is what
    # fails in the repair-run scenario the idempotent append exists for, so a
    # re-run must still reach it. A refused promote raises above and never gets
    # here -- it must not re-validate a CHECK over the whole table.
    _assert_constraints(spark, tbl)


def _assert_constraints(spark: SparkSession, tbl: str) -> None:
    spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN cnpj_basico SET NOT NULL")
    spark.sql(f"ALTER TABLE {tbl} DROP CONSTRAINT IF EXISTS cnpj_basico_len8")
    spark.sql(
        f"ALTER TABLE {tbl} ADD CONSTRAINT cnpj_basico_len8 CHECK (length(trim(cnpj_basico)) = 8)"
    )


if __name__ == "__main__":
    main()
