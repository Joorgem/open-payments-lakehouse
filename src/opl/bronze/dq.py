# src/opl/bronze/dq.py
"""Bronze-boundary data-quality gate transforms. evaluate() tags a reject
reason (null = valid); split() partitions into promotable rows and quarantine.
Defensive about _rescued_data so the same rules run on a local batch DataFrame
and on the Databricks Auto Loader stream."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

REJECT_COLUMN = "_dq_reject_reason"
_REPLACEMENT_CHAR = "�"


def _reject_reason(df: DataFrame) -> Column:
    rescued = F.col("_rescued_data") if "_rescued_data" in df.columns else F.lit(None)
    return (
        F.when(rescued.isNotNull(), F.lit("rescued_data_present"))
        .when(F.col("codigo").isNull() | (F.trim(F.col("codigo")) == ""),
              F.lit("null_or_empty_codigo"))
        .when(F.col("descricao").contains(_REPLACEMENT_CHAR),
              F.lit("encoding_replacement_char"))
        .otherwise(F.lit(None))
    )


def evaluate(df: DataFrame) -> DataFrame:
    return df.withColumn(REJECT_COLUMN, _reject_reason(df))


def split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    evaluated = evaluate(df)
    good = evaluated.filter(F.col(REJECT_COLUMN).isNull()).drop(REJECT_COLUMN)
    bad = evaluated.filter(F.col(REJECT_COLUMN).isNotNull())
    return good, bad
