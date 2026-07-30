# databricks/src/bronze_estab_ingest.py
"""Job task: Auto Loader ingests newly-landed Estabelecimentos CSVs
(per-table subdir) into the estab staging table. AvailableNow: each run
drains only files the checkpoint has not seen -- staged multi-part ingestion."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.autoloader import (
    BRONZE_ESTAB_STAGING,
    add_audit_columns,
    bronze_stream,
    checkpoint_location,
)
from opl.bronze.promote import require_batch_id
from opl.config import DEFAULT


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # Refuse before Spark: this used to default to the literal "manual", which is
    # worse than crashing. The gate and the promote both filter
    # _batch_id == {{job.run_id}}, so rows tagged "manual" reach staging and are
    # then never evaluated, never promoted and never reported -- a silent hole.
    batch_id = require_batch_id(args[0] if args else "", action="ingest")
    spark = SparkSession.builder.getOrCreate()
    month = args[1] if len(args) > 1 else DEFAULT.month
    df = bronze_stream(spark, DEFAULT, "estabelecimentos",
                       DEFAULT.landing_table("estabelecimentos", month),
                       "bronze_cnpj_estab")
    # The SAME `month` the stream read from -- one local, fed to both, so the
    # snapshot the rows are stamped with cannot drift from the folder they came
    # out of.
    audited = add_audit_columns(df, batch_id=batch_id, snapshot_month=month)
    query = (
        audited.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_location(DEFAULT, "bronze_cnpj_estab"))
        .trigger(availableNow=True)
        .toTable(DEFAULT.table(BRONZE_ESTAB_STAGING))
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
