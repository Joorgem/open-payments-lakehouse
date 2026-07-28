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
from opl.config import DEFAULT


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    batch_id = sys.argv[1] if len(sys.argv) > 1 else "manual"
    month = sys.argv[2] if len(sys.argv) > 2 else DEFAULT.month
    df = bronze_stream(spark, DEFAULT, "estabelecimentos",
                       DEFAULT.landing_table("estabelecimentos", month),
                       "bronze_cnpj_estab")
    audited = add_audit_columns(df, batch_id=batch_id)
    query = (
        audited.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_location(DEFAULT, "bronze_cnpj_estab"))
        .trigger(availableNow=True)
        .toTable(DEFAULT.table(BRONZE_ESTAB_STAGING))
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
