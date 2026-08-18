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

from collections.abc import Callable, Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from opl.bronze.rules import REQUIRES_COLUMN, rules_for

REJECT_COLUMN = "_dq_reject_reason"

# The column Auto Loader puts everything the read schema could not absorb into, and
# the highest-precedence verdict this gate reaches. ONE SPELLING, and it lives here
# rather than in `opl.bronze.autoloader` where the column is CREATED, for the reason
# `promote.BATCH_COLUMN` lives with its readers: the two spellings fail in opposite
# directions. Renaming the constant raises where it is imported; renaming the literal
# on the WRITE side leaves `_reject_reason`'s `in df.columns` test quietly false, and
# a rule that is skipped because its column is "absent" reports every drifted row as
# clean. F1b's whole drift proof is that this fires.
RESCUED_DATA_COLUMN = "_rescued_data"

# The verdict that column produces, and the one reason string in this gate that is NOT a
# rule -- it is applied here, above every per-table rule, and appears in no `rules_for`
# set. Named rather than left as the literal it was inside `_reject_reason` because
# `opl.bronze.rule_overlap` reports it as a category of its own and MUST NOT spell it a
# second time: the two spellings would drift in the direction that does not announce
# itself, leaving a report that names a reason no quarantine row carries.
RESCUED_REASON = "rescued_data_present"

Rules = list[tuple[str, Callable[[], Column]]]


def rescued_condition(df: DataFrame) -> Column:
    """Whether Auto Loader rescued anything for this row -- the gate's top verdict.

    Extracted from `_reject_reason` unchanged, expression for expression, so that
    `opl.bronze.rule_overlap` can count the same fact the chain branches on rather than
    rebuild it. A frame with no `_rescued_data` column is the local-batch shape and is
    not drift: `F.lit(None).isNotNull()` is false for every row, which is the same "this
    frame is a different shape" handling the per-table rules get from
    `skipped_rules`."""
    column = (
        F.col(RESCUED_DATA_COLUMN)
        if RESCUED_DATA_COLUMN in df.columns
        else F.lit(None)
    )
    return column.isNotNull()


def skipped_rules(
    columns: Iterable[str], rules: Rules
) -> tuple[tuple[str, str], ...]:
    """(reason, wanted column) for every rule `_reject_reason` will NOT run.

    THE ONE PLACE THE SKIP CONDITION IS SPELLED. `_reject_reason` asks this rather
    than re-deciding, so a run log claiming a rule was skipped and a gate that
    actually ran it cannot come apart -- which is the only way a notice about a
    missing control is worth reading.

    Takes a column list rather than a DataFrame: a caller can ask before it has
    built or scanned anything (`spark.read.table(t).columns` is a catalog lookup),
    and the pure-Python tests need no session to pin the answer."""
    present = frozenset(columns)
    return tuple(
        (reason, REQUIRES_COLUMN[reason])
        for reason, _ in rules
        if reason in REQUIRES_COLUMN and REQUIRES_COLUMN[reason] not in present
    )


def skip_notice(
    columns: Iterable[str], rules: Rules, *, task: str, source: str
) -> str | None:
    """The run-log line for a skipped rule, or None when every rule ran.

    WHY THIS EXISTS, and why it is a line rather than a raise. Skipping is
    CORRECT: a frame written before a derivation existed is not a defective
    frame, and the documented rebuild procedure produces exactly one -- it drops
    bronze while LEAVING staging (see `promote.plan_promotion`). The LIVE instance
    this was written for is closed: estab staging was the 35-column pre-F1.4a
    shape until F1.4b PR B migrated it to 37 on 2026-08-03, so no batch sitting in
    it today is narrow. The mechanism stays because the SHAPE recurs -- the next
    derivation added to a contract re-opens it for every batch staged before that
    derivation existed. What was wrong is that the skip was INAUDIBLE.
    Repromoting such a batch skips
    `unprovable_snapshot_ref_date`, every row reads clean, and the rows land in
    37-column bronze where Delta fills the absent column with NULL -- the exact
    value that rule exists to refuse. The control did not fail; it disappeared.

    So: not an error (raising would make the rebuild procedure unrunnable), and
    not silence. Returned rather than printed, because the emitter must be the
    JOB TASK -- `evaluate` runs inside unit tests on bare frames constantly and
    must not become noisy.

    None, not an empty string, when nothing is skipped: a healthy run prints no
    line at all. A warning on every run is one an operator learns to scroll past,
    which would leave the real one as invisible as the silence it replaces."""
    skipped = skipped_rules(columns, rules)
    if not skipped:
        return None
    wanted = "; ".join(f"{reason} (wants column {column})" for reason, column in skipped)
    return (
        f"{task}: {len(skipped)} DQ rule(s) did NOT run against {source} -- "
        f"{wanted}. Those rows are NOT CHECKED by it. This is expected for a table "
        "written before the column existed and is not a failure -- but if the "
        "table being appended to HAS that column, Delta fills it with NULL on "
        "append, which is the value the rule exists to refuse. Re-ingest the batch "
        f"instead of repromoting it if the {source} rows predate the column."
    )


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
    is a reject, and the rule that reads it never sees the first.

    A skip is SILENT HERE and reported by the job task (`skip_notice`): this
    function runs in unit tests on bare frames constantly, so a print here would
    be noise everywhere and a warning nowhere."""
    chain = F.when(rescued_condition(df), F.lit(RESCUED_REASON))
    # Asked, not re-decided, so the notice a task prints and the chain built here
    # can never describe different gates.
    skipped = {reason for reason, _ in skipped_rules(df.columns, rules)}
    for reason, predicate in rules:
        if reason in skipped:
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
