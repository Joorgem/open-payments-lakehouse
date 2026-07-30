"""Auto Loader (cloudFiles) bronze ingest for the CNPJ lookup files. The
streaming read runs only on Databricks serverless (Trigger.AvailableNow -- the
only supported trigger there); the audit-column and path helpers are pure and
unit-tested locally."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.bronze.lookup_routing import LOOKUP_SUFFIX
from opl.bronze.reader import csv_read_options
from opl.bronze.registry import table_spec
from opl.bronze.schema import struct_for
from opl.bronze.snapshot import (
    SNAPSHOT_MONTH_COLUMN,
    SNAPSHOT_REF_DATE_COLUMN,
    ref_date_column,
)
from opl.config import OplConfig

RECORD_SOURCE = "rfb_cnpj_webdav"
# The one spelling of the column that records WHICH LANDED FILE a row came out of.
# It lives here because this is where the column is created, and it is a constant
# rather than a literal because `opl.bronze.retention` reads it back to decide which
# files may be deleted from the Volume: two spellings of it would be a rename in one
# place that makes the retention query return nothing and silently reclaim nothing.
SOURCE_FILE_COLUMN = "_source_file"
# NO table-name constants here. BRONZE_STAGING / BRONZE_ESTAB_STAGING /
# BRONZE_QUARANTINE / BRONZE_ESTAB_QUARANTINE lived here until F1.4 Task 8 and
# were a SECOND spelling of names `opl.bronze.registry` already owns -- one import
# away from re-creating the drift the registry exists to prevent, which is the
# drift that once sent an estab triager to a table holding no trace of the batch
# that had been blocked. Every staging/bronze/quarantine name now comes from
# `table_spec(...)`. `RECORD_SOURCE` stays: it names where the BYTES came from
# (the RFB WebDAV), which is a property of this ingest, not of a table.


def schema_location(cfg: OplConfig, table_key: str = "bronze_cnpj_lookup") -> str:
    return f"{cfg.volume_root}/_schemas/{table_key}"


def checkpoint_location(cfg: OplConfig, table_key: str = "bronze_cnpj_lookup") -> str:
    return f"{cfg.volume_root}/_checkpoints/{table_key}"


def add_audit_columns(
    df: DataFrame,
    batch_id: str,
    snapshot_month: str,
    record_source: str = RECORD_SOURCE,
) -> DataFrame:
    """Stamp the ingestion audit columns onto a bronze stream.

    `snapshot_month` is REQUIRED and has no default. A default would be one of
    two things, both bad: `opl.config`'s pinned month, which is how F1.2's ingest
    entry point silently tied every row to 2026-06, or the current month, which
    invents a fact. The F1.2 evidence doc recorded the seam this closes --
    "ingesting a second month requires parameterizing the month and adding a
    snapshot key".

    Expects `_source_file` on `df`; every bronze stream adds it (see
    `bronze_stream`), and the reference date is derived from it."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_source", F.lit(record_source))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn(SNAPSHOT_MONTH_COLUMN, F.lit(snapshot_month))
        .withColumn(
            SNAPSHOT_REF_DATE_COLUMN,
            ref_date_column(F.col(SOURCE_FILE_COLUMN), snapshot_month),
        )
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
    caller (see ``bronze_lookup_stream``).

    NO GLOB, and no parameter for one. cloudFiles walks ``source_dir``
    RECURSIVELY -- an F1.3 probe.txt planted in the ``zips/estabelecimentos/``
    subdir was ingested by the lookup stream reading the month root (staging
    7408 -> 7409) -- so a stream pointed at a shared root needed a
    ``pathGlobFilter`` to stay out of its neighbours' files. Every stream now
    reads its OWN per-table subdir, which removes the shared root and with it the
    need. That is deliberately structural: a glob is a discovery RULE, so a source
    filename drifting out of its pattern would silently under-ingest, with nothing
    downstream able to tell an empty batch from a missed one."""
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
    return df.withColumn(SOURCE_FILE_COLUMN, F.col("_metadata.file_path"))


def bronze_lookup_stream(
    spark: SparkSession, cfg: OplConfig, month: str | None = None
) -> DataFrame:
    spec = table_spec("lookup")
    df = bronze_stream(
        spark,
        cfg,
        spec.contract,
        # Its OWN subdir, not the month root the six lookups used to sit loose in.
        # `spec.subdir` and not the literal: the directory name is the registry's
        # to own -- that is why `subdir` is a field of its own and not derived
        # from the table key.
        cfg.landing_table(spec.subdir, month),
        spec.table_key,
    )
    return df.withColumn(
        "lookup_type", lookup_type_column(F.col("_metadata.file_path"))
    )
