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
from opl.bronze.snapshot import (
    SNAPSHOT_MONTH_COLUMN,
    SNAPSHOT_REF_DATE_COLUMN,
    ref_date_column,
)
from opl.config import OplConfig

RECORD_SOURCE = "rfb_cnpj_webdav"
BRONZE_STAGING = "bronze_cnpj_lookup_staging"
BRONZE_ESTAB_STAGING = "bronze_cnpj_estab_staging"
# The quarantine each gate writes. They live here, not in the job scripts, so the
# gate that writes one, the promote that points an operator at it, the fail_on_dq
# message that names it and the test that locks the wiring all read ONE spelling.
# Before this, promote_batch could not name the table in its recovery hint (it had
# no access to the constant) and the wiring test had to parse the script's source.
BRONZE_QUARANTINE = "bronze_cnpj_lookup_quarantine"
BRONZE_ESTAB_QUARANTINE = "bronze_cnpj_estab_quarantine"


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
            ref_date_column(F.col("_source_file"), snapshot_month),
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
    path_glob_filter: str | None = None,
) -> DataFrame:
    """Generalized cloudFiles bronze read for any contract table. Reads
    ``source_dir`` with the ``struct_for(table)`` schema and the shared CSV
    options, adds ``_source_file``. Lookup-specific columns are added by the
    caller (see ``bronze_lookup_stream``).

    ``path_glob_filter`` restricts ingestion to files whose *basename* matches
    the glob (Auto Loader's ``pathGlobFilter``). It is set only by callers whose
    ``source_dir`` is a shared root that Auto Loader walks recursively (the
    lookup stream reads the whole month root); callers that point at a dedicated
    per-table subdir leave it ``None`` so no legitimate file is filtered out."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location(cfg, table_key))
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(struct_for(table))
    )
    if path_glob_filter is not None:
        # F1.3 Task 6 probe (empirical): a probe.txt planted in the
        # cnpj/<month>/zips/estabelecimentos/ subdir WAS ingested by the lookup
        # stream (staging 7408 -> 7409) -- cloudFiles discovers the month root
        # recursively. pathGlobFilter="*CSV" excludes non-CSV subdir files (incl.
        # the .ESTABELE giant extracts) while still matching the lookup *…CSV
        # files. Only the lookup stream passes it; the estab ingest reads its own
        # subdir and must NOT filter (its files end in .ESTABELE, not *CSV).
        reader = reader.option("pathGlobFilter", path_glob_filter)
    for k, v in csv_read_options().items():
        reader = reader.option(k, v)
    df = reader.load(source_dir)
    return df.withColumn("_source_file", F.col("_metadata.file_path"))


def bronze_lookup_stream(
    spark: SparkSession, cfg: OplConfig, month: str | None = None
) -> DataFrame:
    df = bronze_stream(
        spark, cfg, "lookup", cfg.landing_cnpj_month(month), "bronze_cnpj_lookup",
        path_glob_filter="*CSV",
    )
    return df.withColumn(
        "lookup_type", lookup_type_column(F.col("_metadata.file_path"))
    )
