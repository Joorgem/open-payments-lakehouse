# databricks/src/dq_gate_batch.py
"""Job task: BATCH-SCOPED gate -- evaluates only rows ingested by THIS run
(_batch_id == run id), appends rejects to quarantine, publishes bad_row_count.
A historical bad batch no longer wedges future clean batches (F1.2 lesson)."""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from databricks.sdk.runtime import dbutils
from opl.bronze.autoloader import BRONZE_ESTAB_STAGING
from opl.bronze.dq import split
from opl.bronze.rules import rules_for
from opl.config import DEFAULT

QUARANTINE = "bronze_cnpj_estab_quarantine"


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    batch_id = sys.argv[1]
    batch = (spark.read.table(DEFAULT.table(BRONZE_ESTAB_STAGING))
             .filter(F.col("_batch_id") == batch_id))
    good, bad = split(batch, rules=rules_for("estabelecimentos"))
    bad.write.format("delta").mode("append").saveAsTable(DEFAULT.table(QUARANTINE))
    bad_count = bad.count()
    print(f"dq_gate_batch: batch={batch_id} good={good.count()} bad={bad_count}")
    dbutils.jobs.taskValues.set(key="bad_row_count", value=bad_count)


if __name__ == "__main__":
    main()
