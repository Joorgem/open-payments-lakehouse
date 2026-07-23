"""Spark StructType built from the versioned CNPJ column contract. Bronze is
all-string on purpose: preserves leading zeros, future alphanumeric CNPJ keys,
decimal-comma amounts and AAAAMMDD dates verbatim; silver casts."""
from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

from opl.contracts.cnpj_schemas import columns_for


def struct_for(table: str) -> StructType:
    return StructType([StructField(c, StringType(), True) for c in columns_for(table)])
