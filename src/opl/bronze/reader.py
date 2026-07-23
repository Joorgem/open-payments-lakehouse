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
    }


def read_csv_batch(spark: SparkSession, path: str, table: str) -> DataFrame:
    reader = spark.read.schema(struct_for(table))
    for k, v in csv_read_options().items():
        reader = reader.option(k, v)
    return reader.csv(path)
