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
    # The month is passed EXPLICITLY, and it is the same one the stream just read
    # from (`bronze_lookup_stream` with no month falls back to `DEFAULT.month`).
    # Reading it off the config here rather than defaulting it inside
    # `add_audit_columns` is the point: the pin is now visible at the call site
    # that owns it, so Task 6 turning it into a job parameter is a one-line edit
    # in one file instead of a silent behaviour change everywhere.
    # TODO(F1.4 Task 6): take the month from a job parameter, here and in the
    # stream together, so the two cannot drift apart. Promote still needs to key
    # on the snapshot before a second month can land without duplicating lookups.
    audited = add_audit_columns(df, batch_id=batch_id, snapshot_month=DEFAULT.month)
    query = (
        audited.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_location(DEFAULT))
        .trigger(availableNow=True)
        .toTable(DEFAULT.table(BRONZE_STAGING))
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
