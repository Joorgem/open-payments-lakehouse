# databricks/src/dq_gate.py
"""Job task: split staging rows into promotable vs quarantine, persist the
quarantine, and publish bad_row_count as a task value for the condition task.
Does NOT raise — the condition task owns the branch; fail_on_dq owns the hard stop."""
from pyspark.sql import SparkSession

from databricks.sdk.runtime import dbutils
from opl.bronze.autoloader import BRONZE_STAGING
from opl.bronze.dq import split
from opl.config import DEFAULT

QUARANTINE = "bronze_cnpj_lookup_quarantine"


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    staging = spark.read.table(DEFAULT.table(BRONZE_STAGING))
    good, bad = split(staging)
    bad.write.format("delta").mode("overwrite").saveAsTable(DEFAULT.table(QUARANTINE))
    bad_count = bad.count()
    print(f"dq_gate: good={good.count()} bad={bad_count}")
    dbutils.jobs.taskValues.set(key="bad_row_count", value=bad_count)


if __name__ == "__main__":
    main()
