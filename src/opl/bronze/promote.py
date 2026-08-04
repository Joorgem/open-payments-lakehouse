# src/opl/bronze/promote.py
"""Promote one ingested batch's good rows into a bronze Delta table, exactly once.

This lives in the library rather than in `databricks/src/promote_batch.py` so the
properties that matter can be tested against a real Delta log locally: that a
second promote of the same `_batch_id` does not duplicate it, that a `batch_id`
naming no batch is refused before anything is written, and that a batch bronze
holds only PART of is refused rather than reported promoted.

It also owns the batch-scoped primitives the DQ gate task reuses -- `batch_rows`
(the one spelling of "the rows of this batch"), `tally` (both side counts in one
pass) and `rows_of_batch` (what a target table already holds for a batch) --
because the gate's quarantine append needs the same append-once property as this
promote, and that property should have one tested implementation."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

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


class PromoteOutcome(Enum):
    """What a promote did. Not a boolean, deliberately: the four outcomes carry
    different obligations for the caller (only APPENDED wrote anything; only
    NOTHING_INGESTED must skip the constraint DDL) and different log lines."""

    APPENDED = "appended"
    ALREADY_PROMOTED = "already_promoted"
    # Bronze holds the batch and staging no longer does, so the promotable count
    # could not be re-derived: "already promoted" here is inferred from bronze
    # alone and the log must not claim a verified match. The REJECT count is not
    # re-derivable either -- same missing source -- so this is the one outcome whose
    # `rejected_rows` is None rather than a number.
    ALREADY_PROMOTED_STAGING_GONE = "already_promoted_staging_gone"
    NOTHING_INGESTED = "nothing_ingested"


@dataclass(frozen=True)
class PromoteResult:
    batch_id: str
    outcome: PromoteOutcome
    appended_rows: int
    # None means NOT KNOWABLE, which is a different thing from 0 and only one
    # outcome produces it: ALREADY_PROMOTED_STAGING_GONE, where the count's only
    # source (staging) no longer holds the batch. It used to be reported as 0 -- a
    # placeholder that read as a derived count, and `promote_batch.py` printed it to
    # the operator as "0 rejected row(s) ... stay in quarantine", a claim about the
    # quarantine table that nothing had checked. Optional rather than a sentinel
    # int: a magic -1 is still an int, so it survives arithmetic and comparisons
    # silently, whereas None makes every consumer face the case (and a type checker
    # point at the ones that do not).
    rejected_rows: int | None
    # What bronze already held for this batch before the call. Kept because the
    # skip-path log has to print the number it decided on, not just the verdict.
    bronze_rows: int


def require_batch_id(batch_id: str | None, *, action: str = "promote") -> str:
    """Return the batch id to work on, or refuse if none was really given.

    A job-parameter default is not validation: the sentinel below simply matched
    no staging rows, so the promote appended nothing and exited 0 -- a forgotten
    `--params` reported SUCCESS to an operator recovering a stranded batch.

    This is the ONLY guard that runs before bronze is consulted, and it can:
    a blank or the sentinel is not an id at all, so no batch was ever ingested
    under it and it cannot name an already-promoted batch. Every other refusal
    has to wait until both counts are known -- see `plan_promotion`.

    SHARED WITH THE DQ GATE TASK, which faces the identical accident (a task run
    with no parameters) and used to answer it with a bare `IndexError` naming a
    list index. `action` is what that costs: the message has to say which task is
    refusing, since "refusing to promote" from the gate would misreport where the
    run stopped. Everything after it is the same in both cases -- the id is the same
    id, found the same way -- and one guard is why it cannot drift."""
    candidate = (batch_id or "").strip()
    if not candidate or candidate == SENTINEL_BATCH_ID:
        raise PromoteRefused(
            f"refusing to {action}: batch_id={batch_id!r} names no batch. Pass the "
            f"_batch_id of the batch to {action} -- it is the run id of the run that "
            "ingested it, printed by that run's dq_gate_batch task as 'batch=<id>' "
            "(or list them with: SELECT _batch_id, count(*) FROM "
            "<staging table> GROUP BY 1). In the ingestion flow every task takes it "
            "as {{job.run_id}}, right after the table; an operator recovering a batch "
            "passes it by hand, WITH the table -- a batch id belongs to exactly one "
            "table's ingest, and the operator job's table default is a real table, so "
            "omitting it recovers against the wrong one: databricks bundle run "
            "repromote_triaged_batch -t free "
            "--params table=estabelecimentos,batch_id=315230730740144"
        )
    return candidate


def plan_promotion(
    batch_id: str,
    *,
    bronze_rows: int,
    staged_promotable: int,
    staged_rejected: int,
    in_flow: bool,
    staging_table: str,
    bronze_table: str,
) -> PromoteResult:
    """Decide what to do with this batch from the two counts alone, or refuse.

    Pure, so every reachable (bronze, staging) state is covered by a test that
    needs no Spark session. `promote_batch` gathers the counts, calls this, and
    performs the single write that APPENDED asks for -- so the returned value is
    both the plan and, once carried out, the result.

    WHAT "ALREADY PROMOTED" MEANS: bronze holds exactly as many rows for this
    batch as the batch has promotable rows. It used to mean "bronze holds ANY row
    for this batch", which read a count as a boolean: bronze holding a strict
    subset reported the batch fully present, the missing rows were never appended
    and the task exited 0. A subset is reachable -- the documented rebuild
    procedure drops bronze while leaving staging, so a repromote of a pre-rebuild
    `_batch_id` is exactly the case where the two counts can legitimately differ.

    ON A MISMATCH IT REFUSES, with both numbers, rather than appending the
    difference: bronze is append-only raw data with no verified unique key (RFB
    guarantees none), so there is no per-row identity to diff on -- "append the
    missing 1 of 2 rows" cannot be distinguished from "append both again". A
    mismatch is not one state either (a partial rebuild, a batch appended twice
    by an older build, or a DQ rule tightened since the promote all land here),
    and they need different repairs. Printing both counts and stopping is the
    only honest move; the message says which reconciliation to make.

    ORDER: bronze first. The staging-based refusals used to run before the
    idempotence check, so a batch whose staging rows were gone (truncated, or the
    rebuild that drops staging) could never reach the skip: the promote task was
    permanently un-repairable and the error blamed a missing batch id."""
    staged_total = staged_promotable + staged_rejected
    if bronze_rows > 0 and bronze_rows == staged_promotable:
        return PromoteResult(
            batch_id, PromoteOutcome.ALREADY_PROMOTED, 0, staged_rejected, bronze_rows
        )
    if bronze_rows > 0 and staged_total == 0:
        # rejected_rows=None, not 0: `staged_rejected` is 0 here because staging holds
        # NO row of this batch, which says nothing about how many of its rows are in
        # quarantine. Whatever the caller reports must not pass that off as a count.
        return PromoteResult(
            batch_id, PromoteOutcome.ALREADY_PROMOTED_STAGING_GONE, 0, None, bronze_rows
        )
    if bronze_rows > 0:
        raise PromoteRefused(
            f"refusing to promote: {bronze_table} already holds {bronze_rows} row(s) of "
            f"batch {batch_id}, but that batch has {staged_promotable} promotable row(s) "
            f"in {staging_table} -- it is neither absent nor fully present, so this "
            "promote can neither append it nor call it already promoted. Nothing was "
            "written: bronze has no per-row identity, so the difference cannot be "
            "diffed and appending it would duplicate whatever part is already there. "
            f"Reconcile by hand -- if the {bronze_rows} row(s) are a partial or "
            f"duplicated promote, DELETE FROM {bronze_table} WHERE {BATCH_COLUMN} = "
            f"'{batch_id}' and re-run this promote; if a DQ rule changed since the "
            "batch was promoted, the counts are expected to differ and nothing needs "
            "promoting."
        )
    if staged_total == 0:
        if in_flow:
            # The ingestion flow's own run ingested nothing (Auto Loader found no
            # new file), so its batch is legitimately empty. Refusing here turned
            # the pipeline's only legitimate no-op path red, and blamed a bad
            # batch id for it.
            return PromoteResult(batch_id, PromoteOutcome.NOTHING_INGESTED, 0, 0, 0)
        raise PromoteRefused(
            f"refusing to promote: batch_id={batch_id!r} matches no row in "
            f"{staging_table} and no row in {bronze_table}, so there is nothing to "
            "promote and no promoted batch to recognise. Check the id against that "
            f"table's _batch_id values (SELECT _batch_id, count(*) FROM {staging_table} "
            "GROUP BY 1) -- promoting nothing is not success, so this fails instead of "
            "reporting a batch it never found. (A run of the ingestion flow whose own "
            "batch is empty because no new file arrived is the one case where this is "
            "not an error, and it says so by passing the in-flow flag.)"
        )
    if staged_promotable == 0:
        raise PromoteRefused(
            f"refusing to promote: all {staged_rejected} row(s) of batch {batch_id} are "
            "rejected by the DQ rules, so there is nothing to promote. The whole "
            "batch is in the quarantine table; re-ingest it rather than re-promoting."
        )
    return PromoteResult(
        batch_id, PromoteOutcome.APPENDED, staged_promotable, staged_rejected, 0
    )


def promote_batch(
    spark: SparkSession,
    batch_id: str | None,
    *,
    staging_table: str,
    bronze_table: str,
    rules: Rules,
    in_flow: bool = False,
) -> PromoteResult:
    """Append the rows of `batch_id` that pass `rules` to `bronze_table`, once.

    `in_flow` says the caller is the ingestion flow itself and `batch_id` is its
    OWN run id, which is what makes an empty batch a success there. It defaults
    to False -- fail-closed -- so an operator run, or a caller that forgot to say
    so, still gets a refusal instead of a green run that promoted nothing.

    IDEMPOTENCE SHAPE -- skip the append when bronze already holds the batch,
    rather than delete-then-append or MERGE:
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

    `plan_promotion` owns which states are refused and why; every refusal happens
    before `bronze_table` is touched, and therefore before the caller's DDL."""
    batch_id = require_batch_id(batch_id)
    # Bronze BEFORE staging: an already-promoted batch has to be recognised even
    # when staging no longer holds it.
    bronze_rows = rows_of_batch(spark, bronze_table, batch_id)
    staged = evaluate(batch_rows(spark, staging_table, batch_id), rules=rules)
    promotable, rejected = tally(staged)
    plan = plan_promotion(
        batch_id,
        bronze_rows=bronze_rows,
        staged_promotable=promotable,
        staged_rejected=rejected,
        in_flow=in_flow,
        staging_table=staging_table,
        bronze_table=bronze_table,
    )
    if plan.outcome is not PromoteOutcome.APPENDED:
        return plan
    good = staged.filter(F.col(REJECT_COLUMN).isNull()).drop(REJECT_COLUMN)
    good.write.format("delta").mode("append").saveAsTable(bronze_table)
    return plan


def batch_rows(spark: SparkSession, table: str, batch_id: str) -> DataFrame:
    """The rows `table` holds for `batch_id`.

    One spelling of the batch filter, shared with the DQ gate task: the gate and
    the promote must scope to the same rows, and two copies of
    `col("_batch_id") == batch_id` are two places for that to drift."""
    return spark.read.table(table).filter(F.col(BATCH_COLUMN) == batch_id)


def tally(evaluated: DataFrame) -> tuple[int, int]:
    """(promotable, rejected) row counts in ONE pass over the batch.

    Same predicate `dq.split` uses, taken from the same `evaluate` call, because
    counting both sides of `split` separately scans the batch twice. The reject
    count is not optional output: the operator job promotes a batch whose rejects
    a human has agreed to accept, so that number belongs in the log."""
    counts = {
        row["promotable"]: row["n"]
        for row in (
            evaluated.groupBy(F.col(REJECT_COLUMN).isNull().alias("promotable"))
            .agg(F.count(F.lit(1)).alias("n"))
            .collect()
        )
    }
    return counts.get(True, 0), counts.get(False, 0)


def rows_of_batch(spark: SparkSession, table: str, batch_id: str) -> int:
    """How many rows `table` already holds for `batch_id`. 0 if it does not exist.

    WHY `_batch_id` is the idempotence key: it is the ingesting run's id, so rows
    carrying it in a target table can only have been put there by an append of
    that same batch. What the count canNOT tell you is that the append was
    complete -- a Delta append is one atomic commit, so no crash leaves a partial
    batch, but a later DELETE or a rebuild that drops the target can, and the
    documented rebuild procedure does exactly that. Hence the callers compare
    this count with the number of rows the batch should contribute instead of
    treating any nonzero value as "already done".

    COST: one narrow scan of `_batch_id` over the target table per call -- 144.2M
    rows across two months as of 2026-08. Cheap next to writing the batch, and it
    is the price of not double-counting one.

    This used to say `_batch_id` "cannot be file-skipped" because it sits past the
    32 columns Delta indexes by default. True when written, false now. It is at
    ordinal 34 of 37 in estabelecimentos, so it did start outside that prefix --
    but on 2026-08-02 Databricks' Predictive Optimization ran a
    DATA_SKIPPING_COLUMN_SELECTION unprompted, added `_batch_id` to
    `delta.workloadBasedColumns.deltaFileStatistics` and backfilled stats onto the
    existing files (Delta versions 12 and 13). Measured after the 2026-07 promote:
    the batch filter reads 29 of 59 files and 7.6MB where the same rows selected
    via an unindexed column read all 59 and 15.5MB. In empresas (14 columns) and
    socios (18 columns) `_batch_id` is at ordinal 11 and 15, inside the default
    32, so it was always skippable there and the old claim never applied.

    Depend on this for cost only, never for correctness. Nothing in this repo sets
    it, the same operation reports a `removed_data_skipping_columns` counterpart,
    and a platform that added the column unprompted can drop it the same way."""
    if not spark.catalog.tableExists(table):
        return 0
    landed = spark.read.table(table)
    if BATCH_COLUMN not in landed.columns:
        raise PromoteRefused(
            f"refusing to write: {table} has no {BATCH_COLUMN} column, so an "
            "already-appended batch cannot be recognised and appending could "
            "duplicate it."
        )
    return landed.filter(F.col(BATCH_COLUMN) == batch_id).count()
