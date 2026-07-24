"""Auto Loader (cloudFiles) bronze ingest for the CNPJ lookup files. The
streaming read runs only on Databricks serverless (Trigger.AvailableNow -- the
only supported trigger there); the audit-column and path helpers are pure and
unit-tested locally."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.bronze.lookup_routing import LOOKUP_SUFFIX
from opl.bronze.reader import csv_read_options
from opl.bronze.schema import struct_for
from opl.config import OplConfig

RECORD_SOURCE = "rfb_cnpj_webdav"
BRONZE_STAGING = "bronze_cnpj_lookup_staging"
BRONZE_ESTAB_STAGING = "bronze_cnpj_estab_staging"


def schema_location(cfg: OplConfig, table_key: str = "bronze_cnpj_lookup") -> str:
    return f"{cfg.volume_root}/_schemas/{table_key}"


def checkpoint_location(cfg: OplConfig, table_key: str = "bronze_cnpj_lookup") -> str:
    return f"{cfg.volume_root}/_checkpoints/{table_key}"


def add_audit_columns(
    df: DataFrame, batch_id: str, record_source: str = RECORD_SOURCE
) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_source", F.lit(record_source))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def lookup_type_column(file_path_col: Column) -> Column:
    # Build a nested CASE from the suffix map, matching the inner-file suffix in the path.
    col = F.lit(None)
    for suffix, lookup_type in LOOKUP_SUFFIX.items():
        col = F.when(file_path_col.endswith(f".{suffix}CSV"), F.lit(lookup_type)).otherwise(col)
    return col


def bronze_stream(
    spark: SparkSession,
    cfg: OplConfig,
    table: str,
    source_dir: str,
    table_key: str,
) -> DataFrame:
    """Generalized cloudFiles bronze read for any contract table. Reads
    ``source_dir`` with the ``struct_for(table)`` schema and the shared CSV
    options, adds ``_source_file``. Lookup-specific columns are added by the
    caller (see ``bronze_lookup_stream``)."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location(cfg, table_key))
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(struct_for(table))
    )
    for k, v in csv_read_options().items():
        reader = reader.option(k, v)
    df = reader.load(source_dir)
    return df.withColumn("_source_file", F.col("_metadata.file_path"))


def bronze_lookup_stream(
    spark: SparkSession, cfg: OplConfig, month: str | None = None
) -> DataFrame:
    df = bronze_stream(
        spark, cfg, "lookup", cfg.landing_cnpj_month(month), "bronze_cnpj_lookup"
    )
    return df.withColumn(
        "lookup_type", lookup_type_column(F.col("_metadata.file_path"))
    )
