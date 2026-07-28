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
from opl.bronze.promote import require_batch_id
from opl.config import DEFAULT


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # Refuse before Spark -- see the note in bronze_estab_ingest.py: a defaulted
    # batch id lands rows the gate is scoped away from, so they are never
    # evaluated, promoted or reported.
    batch_id = require_batch_id(args[0] if args else "", action="ingest")
    spark = SparkSession.builder.getOrCreate()
    df = bronze_lookup_stream(spark, DEFAULT)
    # TODO(F1.3+): parameterize the snapshot month (job parameter) instead of
    # pinning to opl.config's default; promote also needs a month/snapshot key
    # before a second month can land without duplicating lookup rows.
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
