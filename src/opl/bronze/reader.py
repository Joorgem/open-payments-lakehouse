"""CSV read options shared by the local batch reader and the Databricks Auto
Loader stream, so both parse the RFB cp1252 files byte-identically. Auto Loader
(cloudFiles) is Databricks-only; the batch reader here is the local-testable twin."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from opl.bronze.schema import struct_for
from opl.contracts.cnpj_schemas import CSV_DIALECT


def csv_read_options() -> dict[str, str]:
    return {
        "header": "false",
        "sep": CSV_DIALECT["sep"],           # ";"
        "quote": CSV_DIALECT["quotechar"],   # '"'
        "encoding": CSV_DIALECT["encoding"],  # "cp1252" (Java alias of windows-1252)
        "mode": "PERMISSIVE",
        # RFB ships records with literal newlines inside quoted fields -- valid
        # CSV per RFC 4180, measured at 1 record in Estabelecimentos6 and 3 in
        # Estabelecimentos8 (of 4,753,435 each). Spark's default multiLine=false
        # splits each one into a NULL-tailed "parent" (which satisfies every DQ
        # rule and gets promoted) plus a garbage "fragment"; the row count still
        # matches the source, so no count check can catch it.
        #
        # Cost, stated because it is real: with multiLine Spark cannot split one
        # file across tasks, so the unit of parallelism becomes the file, not the
        # 128 MB block. Accepted for this workload -- Estabelecimentos ships as
        # ten parts in total (nine of ~320-370 MB plus part 0 at 2,128,818,559 B),
        # enough files to keep the cluster busy, and the lookup files are
        # single-part and small enough that a file was already one task. Part 0 is
        # the one-huge-file case: ~14 GB of CSV in a single task ingested
        # 29,093,533 rows in about nine minutes, so the ceiling is livable at this
        # scale. One run, nothing isolated -- a data point, not a benchmark; the
        # trade is correctness for a known parallelism ceiling (ADR 0005).
        "multiLine": "true",
    }


def read_csv_batch(spark: SparkSession, path: str, table: str) -> DataFrame:
    reader = spark.read.schema(struct_for(table))
    for k, v in csv_read_options().items():
        reader = reader.option(k, v)
    return reader.csv(path)
