# src/opl/bronze/dq.py
"""Bronze-boundary data-quality gate transforms. evaluate() tags a reject
reason (null = valid); split() partitions into promotable rows and quarantine.
Defensive about _rescued_data so the same rules run on a local batch DataFrame
and on the Databricks Auto Loader stream.

Rejection rules are per-table (see opl.bronze.rules); the universal
rescued_data_present check is highest precedence and applied here, above any
per-table rule. evaluate()/split() default to the "lookup" rule set, keeping
the F1.2 lookup behavior byte-for-byte."""
from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from opl.bronze.rules import rules_for

REJECT_COLUMN = "_dq_reject_reason"

Rules = list[tuple[str, Callable[[], Column]]]


def _reject_reason(df: DataFrame, rules: Rules) -> Column:
    rescued = F.col("_rescued_data") if "_rescued_data" in df.columns else F.lit(None)
    chain = F.when(rescued.isNotNull(), F.lit("rescued_data_present"))
    for reason, predicate in rules:
        chain = chain.when(predicate(), F.lit(reason))
    return chain.otherwise(F.lit(None))


def evaluate(df: DataFrame, rules: Rules | None = None) -> DataFrame:
    if rules is None:
        rules = rules_for("lookup")
    return df.withColumn(REJECT_COLUMN, _reject_reason(df, rules))


def split(df: DataFrame, rules: Rules | None = None) -> tuple[DataFrame, DataFrame]:
    evaluated = evaluate(df, rules)
    good = evaluated.filter(F.col(REJECT_COLUMN).isNull()).drop(REJECT_COLUMN)
    bad = evaluated.filter(F.col(REJECT_COLUMN).isNotNull())
    return good, bad
