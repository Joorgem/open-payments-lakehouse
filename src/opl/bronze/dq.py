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

from opl.bronze.rules import REQUIRES_COLUMN, rules_for

REJECT_COLUMN = "_dq_reject_reason"

Rules = list[tuple[str, Callable[[], Column]]]


def _reject_reason(df: DataFrame, rules: Rules) -> Column:
    """The first matching rule's reason, or NULL. Order is the rule set's.

    A rule may declare that it reads a column outside the contract
    (`rules.REQUIRES_COLUMN`); when the frame does not carry that column the rule
    is SKIPPED. Same shape the `_rescued_data` line above has used since F1.2 --
    one mechanism for "this frame is a different shape", not two.

    THE SKIP IS DECLARED, NEVER DISCOVERED. Wrapping `predicate()` in
    `except AnalysisException` would read as the same thing and is not: it would
    equally skip a rule whose column name is a TYPO, turning a broken rule into
    one that never fires and reporting the batch clean. Looking the declared name
    up means an undeclared missing column still raises, loudly, which is what a
    frame missing a CONTRACT column must do (see the rule set's own test).

    Safe only because a column's ABSENCE is a different fact from its being NULL:
    absent means "this frame predates the derivation" -- a bare contract frame in
    a unit test, or a staging table written before the column existed -- while
    NULL means "the derivation ran and could not prove a value". Only the second
    is a reject, and the rule that reads it never sees the first."""
    rescued = F.col("_rescued_data") if "_rescued_data" in df.columns else F.lit(None)
    chain = F.when(rescued.isNotNull(), F.lit("rescued_data_present"))
    for reason, predicate in rules:
        needs = REQUIRES_COLUMN.get(reason)
        if needs is not None and needs not in df.columns:
            continue
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
