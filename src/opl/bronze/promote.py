# src/opl/bronze/promote.py
"""Promote one ingested batch's good rows into a bronze Delta table, exactly once.

This lives in the library rather than in `databricks/src/promote_batch.py` so the
two properties that matter can be tested against a real Delta log locally: that a
second promote of the same `_batch_id` does not duplicate it, and that a
`batch_id` naming no batch is refused before anything is written."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.bronze.dq import REJECT_COLUMN, evaluate

Rules = list[tuple[str, Callable[[], Column]]]

BATCH_COLUMN = "_batch_id"

# The value the repromote operator job carries as its `batch_id` default. It is a
# sentinel, not an id: reaching a promote with it means the job was run without
# `--params`. Kept here, next to the check that refuses it, because the YAML
# default is a placeholder and cannot validate anything.
SENTINEL_BATCH_ID = "REQUIRED-PASS-A-BATCH-ID"


class PromoteRefused(RuntimeError):
    """Nothing was promoted and nothing was written.

    Raised before the append, and therefore before the caller's constraint DDL,
    so a refused promote leaves the bronze table and its constraints exactly as
    they were."""


@dataclass(frozen=True)
class PromoteResult:
    batch_id: str
    appended_rows: int
    rejected_rows: int
    already_promoted: bool


def require_batch_id(batch_id: str | None) -> str:
    """Return the batch id to promote, or refuse if none was really given.

    A job-parameter default is not validation: the sentinel below simply matched
    no staging rows, so the promote appended nothing and exited 0 -- a forgotten
    `--params` reported SUCCESS to an operator recovering a stranded batch."""
    candidate = (batch_id or "").strip()
    if not candidate or candidate == SENTINEL_BATCH_ID:
        raise PromoteRefused(
            f"refusing to promote: batch_id={batch_id!r} names no batch. Pass the "
            "_batch_id of the batch to promote -- it is the run id of the run that "
            "ingested it, printed by that run's dq_gate_batch task as 'batch=<id>' "
            "(or list them with: SELECT _batch_id, count(*) FROM "
            "<staging table> GROUP BY 1). Example: databricks bundle run "
            "repromote_triaged_batch -t free --params batch_id=315230730740144"
        )
    return candidate


def promote_batch(
    spark: SparkSession,
    batch_id: str | None,
    *,
    staging_table: str,
    bronze_table: str,
    rules: Rules,
) -> PromoteResult:
    """Append the rows of `batch_id` that pass `rules` to `bronze_table`, once.

    IDEMPOTENCE SHAPE -- skip the append if the batch is already there, rather
    than delete-then-append or MERGE:
    - not delete-then-append: DELETE and APPEND are two separate Delta commits,
      so that shape's own failure window leaves bronze *missing* a batch it
      already held, and it rewrites files for ~9.5M rows to reach a state that
      is already correct.
    - not MERGE: bronze is append-only raw data with no verified unique key (RFB
      guarantees none, and duplicate keys in the source make MERGE fail
      outright), so there is nothing per-row to match on.
    - a skip rather than a hard refusal, because the failure this exists for is a
      REPAIR RUN: the append commits, the constraint DDL after it fails or times
      out, the task is marked FAILED with the rows already in bronze, and the
      operator hits Repair -- which re-runs this task with the same
      {{job.run_id}}. Refusing would make that repair run unrepairable; skipping
      the append lets the caller go on to the DDL, which is the part that failed.

    NOT concurrency-safe: two promotes of the same batch running at once can both
    see it absent and both append. The ingestion flow runs promote once per run
    and the operator job is a deliberate human action, so serializing is the
    operator's job here (same assumption as `landing.upload_to_volume`'s
    exclusive ownership of its target).

    Raises `PromoteRefused` (before touching `bronze_table`) if `batch_id` names
    no batch, matches no staging row, or has no promotable row at all."""
    batch_id = require_batch_id(batch_id)
    staged = evaluate(
        spark.read.table(staging_table).filter(F.col(BATCH_COLUMN) == batch_id),
        rules=rules,
    )
    promotable, rejected = _tally(staged)
    if promotable + rejected == 0:
        raise PromoteRefused(
            f"refusing to promote: batch_id={batch_id!r} matches no row in "
            f"{staging_table}. Check the id against that table's _batch_id values "
            "(SELECT _batch_id, count(*) FROM "
            f"{staging_table} GROUP BY 1) -- promoting nothing is not success, so "
            "this fails instead of reporting a batch it never found."
        )
    if promotable == 0:
        raise PromoteRefused(
            f"refusing to promote: all {rejected} row(s) of batch {batch_id} are "
            "rejected by the DQ rules, so there is nothing to promote. The whole "
            "batch is in the quarantine table; re-ingest it rather than re-promoting."
        )
    already_landed = _rows_already_in_bronze(spark, bronze_table, batch_id)
    if already_landed:
        return PromoteResult(batch_id, 0, rejected, already_promoted=True)
    good = staged.filter(F.col(REJECT_COLUMN).isNull()).drop(REJECT_COLUMN)
    good.write.format("delta").mode("append").saveAsTable(bronze_table)
    return PromoteResult(batch_id, promotable, rejected, already_promoted=False)


def _tally(evaluated: DataFrame) -> tuple[int, int]:
    """(promotable, rejected) row counts in ONE pass over the batch.

    Same predicate `dq.split` uses, taken from the same `evaluate` call, because
    calling `split` and counting both sides would scan the batch twice. The
    reject count is not optional output: the operator job promotes a batch whose
    rejects a human has agreed to accept, so that number belongs in the log."""
    counts = {
        row["promotable"]: row["n"]
        for row in (
            evaluated.groupBy(F.col(REJECT_COLUMN).isNull().alias("promotable"))
            .agg(F.count(F.lit(1)).alias("n"))
            .collect()
        )
    }
    return counts.get(True, 0), counts.get(False, 0)


def _rows_already_in_bronze(spark: SparkSession, bronze_table: str, batch_id: str) -> int:
    """Rows of `batch_id` that `bronze_table` already holds.

    WHY `_batch_id` is a sound idempotence key: a Delta append is a single atomic
    commit, so a failed promote leaves either all of a batch's rows or none of
    them -- never a partial batch this count would misread. And `_batch_id` is
    the ingesting run's id, so "bronze already has rows for it" can only mean
    "this same batch was already appended".

    COST: one narrow scan of `_batch_id` over the whole bronze table (71.9M rows
    at the end of F1.3) on every promote, since `_batch_id` is past the 32
    columns Delta keeps min/max stats for and so cannot be file-skipped. Cheap
    next to writing the batch, and it is the price of not double-counting one."""
    if not spark.catalog.tableExists(bronze_table):
        return 0
    landed = spark.read.table(bronze_table)
    if BATCH_COLUMN not in landed.columns:
        raise PromoteRefused(
            f"refusing to promote: {bronze_table} has no {BATCH_COLUMN} column, so an "
            "already-promoted batch cannot be recognised and appending could "
            "duplicate it."
        )
    return landed.filter(F.col(BATCH_COLUMN) == batch_id).count()
