# databricks/src/bronze_ingest.py
"""Job task: Auto Loader reads the landed lookup CSVs and appends to the bronze
staging Delta table. Trigger.AvailableNow (the only serverless-supported trigger):
process all currently-available files, then stop."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.autoloader import (
    BRONZE_STAGING,
    add_audit_columns,
    bronze_lookup_stream,
    checkpoint_location,
)
from opl.config import DEFAULT


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    batch_id = sys.argv[1] if len(sys.argv) > 1 else "manual"
    df = bronze_lookup_stream(spark, DEFAULT)
    audited = add_audit_columns(df, batch_id=batch_id)
    query = (
        audited.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_location(DEFAULT))
        .trigger(availableNow=True)
        .toTable(DEFAULT.table(BRONZE_STAGING))
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
