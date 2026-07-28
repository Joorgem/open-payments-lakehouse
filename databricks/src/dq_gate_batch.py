# databricks/src/dq_gate_batch.py
"""Job task: BATCH-SCOPED gate -- evaluates only rows ingested by THIS run
(_batch_id == run id), appends rejects to quarantine, publishes bad_row_count.
A historical bad batch no longer wedges future clean batches (F1.2 lesson).

Does not raise on rejected rows: the condition task owns that branch and
fail_on_dq owns the hard stop. It DOES raise when the quarantine table already
holds a different number of rows for this batch than the batch has rejects, which
is a corrupted-state refusal rather than a DQ verdict -- see `main`."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.autoloader import BRONZE_ESTAB_STAGING
from opl.bronze.dq import evaluate, split
from opl.bronze.promote import batch_rows, rows_of_batch, tally
from opl.bronze.rules import rules_for
from opl.config import DEFAULT

QUARANTINE = "bronze_cnpj_estab_quarantine"


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    spark = SparkSession.builder.getOrCreate()
    batch_id = args[0]
    quarantine = DEFAULT.table(QUARANTINE)
    rules = rules_for("estabelecimentos")
    batch = batch_rows(spark, DEFAULT.table(BRONZE_ESTAB_STAGING), batch_id)
    # ONE pass for both counts. This task used to compute the split three times
    # over a batch of up to ~29M rows -- `bad.write`, `bad.count()`, `good.count()`
    # -- for the two things it actually does: count both sides and write the
    # rejects. `split` below re-derives the reject column, which is a second plan,
    # not a second scan of an already-counted frame.
    good_count, bad_count = tally(evaluate(batch, rules))
    # IDEMPOTENCE, for the same reason the promote has it: max_retries is 0, so no
    # automatic retry can re-run this task, but an explicit Repair (or a re-run of
    # this task alone) re-executes it under the SAME run id -- and the append was
    # bare, so it put the identical reject rows in quarantine a second time. A
    # triager then sees 2 rows for 1 damaged record and cannot tell duplication
    # from two real defects, and ADR 0006 designates this table as the measured
    # history a rate-based gate will be built on, so the duplicates feed a metric.
    #
    # Keyed on the row count for this `_batch_id`, not on "are there any rows",
    # for the reason `opl.bronze.promote.plan_promotion` documents at length: a
    # count read as a boolean cannot see a partial batch. Not Delta's
    # `txnAppId`/`txnVersion` idempotent-write options either: those skip any
    # write whose version is <= the last one recorded for the app id, which would
    # silently drop the rejects of any batch processed out of order.
    already = rows_of_batch(spark, quarantine, batch_id)
    if already == 0:
        _, bad = split(batch, rules)
        bad.write.format("delta").mode("append").saveAsTable(quarantine)
        print(f"dq_gate_batch: appended {bad_count} reject row(s) of batch {batch_id} "
              f"to {quarantine}")
    elif already == bad_count:
        print(f"dq_gate_batch: {quarantine} already holds all {bad_count} reject row(s) "
              f"of batch {batch_id} -- append skipped (idempotent re-run)")
    else:
        raise RuntimeError(
            f"dq_gate_batch: {quarantine} already holds {already} row(s) for batch "
            f"{batch_id}, but this batch has {bad_count} rejected row(s) -- it is "
            "neither absent nor exactly this batch's rejects, so appending would "
            "duplicate part of it and skipping would hide the difference. This is the "
            "quarantine ADR 0006 makes the measured reject history, so a partial or "
            "duplicated batch in it corrupts the rate a later gate is set against. "
            f"Reconcile it by hand (DELETE FROM {quarantine} WHERE _batch_id = "
            f"'{batch_id}') and repair this task; nothing is promoted until it "
            "succeeds."
        )
    print(f"dq_gate_batch: batch={batch_id} good={good_count} bad={bad_count}")
    _publish("bad_row_count", bad_count)


def _publish(key: str, value: int) -> None:
    """Publish a task value for the condition task downstream.

    `dbutils` is imported HERE rather than at module scope because
    `databricks.sdk.runtime` builds a workspace client on import and raises
    without workspace credentials -- which made this whole task unimportable, and
    so untestable, outside Databricks."""
    from databricks.sdk.runtime import dbutils
    dbutils.jobs.taskValues.set(key=key, value=value)


if __name__ == "__main__":
    main()
