# databricks/src/bronze_lookup_ingest.py
"""Job task: Auto Loader ingests the six landed lookup CSVs into lookup staging.

Its own entry point rather than a branch inside bronze_ingest.py: the lookup is
genuinely different, not a parameter of the others. Six differently-named single
files route into ONE table by filename suffix, the files are 1-120 KB, and they
land through the local-unzip path because unzipping a 22 KB file on serverless
Spark is absurd. Forcing that into the shared shape would be the premature
abstraction the registry deliberately avoids.

argv: [batch_id, month]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.autoloader import (
    add_audit_columns,
    bronze_lookup_stream,
    checkpoint_location,
)
from opl.bronze.promote import require_batch_id
from opl.bronze.registry import table_spec
from opl.config import DEFAULT


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # Refuse before Spark: a defaulted batch id lands rows the gate is scoped away
    # from, so they are never evaluated, promoted or reported -- a silent hole,
    # not a failure.
    batch_id = require_batch_id(args[0] if args else "", action="ingest")
    month = args[1] if len(args) > 1 else DEFAULT.month
    spec = table_spec("lookup")
    spark = SparkSession.builder.getOrCreate()
    # The month goes to the stream AND to the audit columns, from one local: this
    # closes the F1.2 seam where the stream fell back to the config's pinned month
    # while the rows were stamped with it separately, so the two could drift.
    df = bronze_lookup_stream(spark, DEFAULT, month)
    audited = add_audit_columns(df, batch_id=batch_id, snapshot_month=month)
    query = (
        audited.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_location(DEFAULT, spec.table_key))
        .trigger(availableNow=True)
        .toTable(DEFAULT.table(spec.staging))
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
