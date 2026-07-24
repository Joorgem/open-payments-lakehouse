# databricks/src/promote.py
"""Job task (runs only when the gate passes): promote clean rows into the bronze
table and enforce declarative Delta constraints (NOT NULL + CHECK) as a
belt-and-suspenders backstop — they should never fire because dq_gate already
filtered."""
from pyspark.sql import SparkSession

from opl.bronze.autoloader import BRONZE_STAGING
from opl.bronze.dq import split
from opl.config import DEFAULT

BRONZE = "bronze_cnpj_lookup"


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    good, _ = split(spark.read.table(DEFAULT.table(BRONZE_STAGING)))
    tbl = DEFAULT.table(BRONZE)
    good.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl)
    spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN codigo SET NOT NULL")
    spark.sql(f"ALTER TABLE {tbl} DROP CONSTRAINT IF EXISTS codigo_not_blank")
    spark.sql(f"ALTER TABLE {tbl} ADD CONSTRAINT codigo_not_blank CHECK (length(trim(codigo)) > 0)")
    print(f"promote: wrote {good.count()} rows to {tbl} with constraints")


if __name__ == "__main__":
    main()
