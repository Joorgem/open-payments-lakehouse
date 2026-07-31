# databricks/src/dq_gate_batch.py
"""Job task: BATCH-SCOPED gate -- evaluates only rows ingested by THIS run
(_batch_id == run id), appends rejects to quarantine, publishes bad_row_count.
A historical bad batch no longer wedges future clean batches (F1.2 lesson).

Does not raise on rejected rows: the condition task owns that branch and
fail_on_dq owns the hard stop. It DOES raise on the two states that are not DQ
verdicts at all: no batch id given (the shared operator guard in
`opl.bronze.promote`), and a quarantine table already holding a different number
of rows for this batch than the batch has rejects -- see `main`.

Parameterised by table: this is the ONLY gate now. The lookup's whole-table gate
(dq_gate.py) is gone, so the lookup inherits batch scoping -- evaluating one
`_batch_id` instead of the whole staging table, which stops a single historical
bad row from wedging every later clean batch. Its quarantine accumulates now
instead of being overwritten, which is what ADR 0006 needs it to be.

argv: [table, batch_id]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.dq import evaluate, split
from opl.bronze.promote import batch_rows, require_batch_id, rows_of_batch, tally
from opl.bronze.registry import table_spec
from opl.bronze.rules import rules_for
from opl.config import DEFAULT


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # Table first, then the batch id -- both refused before Spark, for the reason
    # the batch-id guard documents: an operator should not wait for a serverless
    # session to be told which argument is wrong. An absent argument is passed
    # through as "" rather than indexed, the same way promote_batch.py does it,
    # because `args[0]` on an empty argv answered a task run without parameters
    # with a bare `IndexError: list index out of range`, which names a list index
    # and not the missing job parameter.
    #
    # Staging, quarantine and rule set all come off the ONE resolved spec, so the
    # table this reads a batch from and the table it writes that batch's rejects
    # to cannot drift apart -- the drift that "sent estab triagers to a table full
    # of unrelated F1.2 lookup rows".
    spec = table_spec(args[0] if args else "")
    batch_id = require_batch_id(args[1] if len(args) > 1 else "", action="gate")
    spark = SparkSession.builder.getOrCreate()
    quarantine = DEFAULT.table(spec.quarantine)
    rules = rules_for(spec.contract)
    batch = batch_rows(spark, DEFAULT.table(spec.staging), batch_id)
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
