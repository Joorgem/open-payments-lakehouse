# databricks/src/promote_batch.py
"""Job task (gate passed): APPEND this batch's good rows to the bronze table.
Each staging row belongs to exactly one _batch_id, so incremental appends
never duplicate across runs. Constraints enforced once via idempotent DDL."""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from opl.bronze.autoloader import BRONZE_ESTAB_STAGING
from opl.bronze.dq import split
from opl.bronze.rules import rules_for
from opl.config import DEFAULT

BRONZE = "bronze_cnpj_estabelecimentos"


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    batch_id = sys.argv[1]
    batch = (spark.read.table(DEFAULT.table(BRONZE_ESTAB_STAGING))
             .filter(F.col("_batch_id") == batch_id))
    good, _ = split(batch, rules=rules_for("estabelecimentos"))
    tbl = DEFAULT.table(BRONZE)
    good.write.format("delta").mode("append").saveAsTable(tbl)
    spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN cnpj_basico SET NOT NULL")
    spark.sql(f"ALTER TABLE {tbl} DROP CONSTRAINT IF EXISTS cnpj_basico_len8")
    spark.sql(
        f"ALTER TABLE {tbl} ADD CONSTRAINT cnpj_basico_len8 CHECK (length(trim(cnpj_basico)) = 8)"
    )
    print(f"promote_batch: appended {good.count()} rows (batch {batch_id}) to {tbl}")


if __name__ == "__main__":
    main()
