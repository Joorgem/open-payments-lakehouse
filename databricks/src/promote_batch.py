# databricks/src/promote_batch.py
"""Job task (gate passed): APPEND this batch's good rows to the bronze table,
once, then (re-)assert the declarative Delta constraints.

The append is idempotent for a given `_batch_id` and the operator guards that
refuse a batch id naming nothing live in `opl.bronze.promote`, which documents
why that shape was chosen and is unit-tested against a real Delta log. This file
is the job's entry point: it owns the argv contract below and resolves everything
table-specific -- coordinates, rule set, constraint DDL -- from the registry.

Parameterised by table: this is the ONLY promote now. The lookup's promote
(promote.py) overwrote its bronze table from the WHOLE staging table; folding it
in here moves it to a scoped append of one `_batch_id`. That is intended -- an
overwrite from whole staging writes 2x the rows the moment a second batch exists
-- but it means the lookup's bronze table has to start clean, which a controlled
reload handles separately.

argv: [table, batch_id, --in-flow?]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.dq import skip_notice
from opl.bronze.promote import (
    BATCH_COLUMN,
    PromoteOutcome,
    PromoteResult,
    promote_batch,
)
from opl.bronze.registry import BronzeTable, table_spec
from opl.bronze.rules import rules_for
from opl.config import DEFAULT

# Second parameter, passed by the INGESTION FLOW's promote task only: it says the
# batch_id is `{{job.run_id}}`, the id of the run executing this very task, so an
# empty batch means "Auto Loader found no new file" and is a success. The
# operator job (repromote_triaged_batch) passes a human-supplied id naming an
# EARLIER run, which is indistinguishable from a typo -- same shape, same digits
# -- so the caller has to declare which it is, and an absent flag means the
# strict reading. Not inferred from the id's value: a mis-typed run id would then
# silently promote nothing and exit 0, the F1.3 defect this refuses to reopen.
IN_FLOW_FLAG = "--in-flow"


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    positional = [arg for arg in args if arg != IN_FLOW_FLAG]
    # The table is resolved BEFORE the session: a mistyped one is refused by
    # `table_spec` naming the registered tables, and nothing about that refusal
    # needs Spark. An absent batch id is passed through as "" rather than raising
    # IndexError: promote_batch owns refusing it, with a message aimed at the
    # operator.
    spec = table_spec(positional[0] if positional else "")
    spark = SparkSession.builder.getOrCreate()
    tbl = DEFAULT.table(spec.bronze)
    staging = DEFAULT.table(spec.staging)
    rules = rules_for(spec.contract)
    _announce_skipped_rules(spark, staging, rules)
    result = promote_batch(
        spark,
        positional[1] if len(positional) > 1 else "",
        staging_table=staging,
        bronze_table=tbl,
        rules=rules,
        in_flow=IN_FLOW_FLAG in args,
    )
    if result.outcome is PromoteOutcome.NOTHING_INGESTED:
        # The flow's only legitimate no-op. It returns before the DDL: that
        # statement re-validates a CHECK over the whole 71.9M-row table, and a run
        # that wrote nothing cannot have invalidated it.
        print(f"promote_batch: batch {result.batch_id} ingested no rows -- nothing to "
              f"promote into {tbl} (no new file arrived); constraints left as they are")
        return
    # The quarantine is qualified HERE, off the same `spec`, rather than inside the
    # reporting helper: every table this task names stays in one function, where a
    # coordinate borrowed from another table's spec would be visible in one screen.
    _report(result, tbl, DEFAULT.table(spec.quarantine))
    # Runs on the promote paths and after the append, deliberately: this DDL is
    # what fails in the repair-run scenario the idempotent append exists for, so a
    # re-run must still reach it. A refused promote raises above and never gets
    # here -- it must not re-validate a CHECK over the whole table.
    _assert_constraints(spark, spec, tbl)


def _announce_skipped_rules(spark: SparkSession, staging: str, rules) -> None:
    """Say which rules will NOT run against this batch, BEFORE the append. Nothing
    at all when they all run.

    This is the task where that silence costs something. The documented rebuild
    procedure drops bronze and LEAVES staging (`promote.plan_promotion` says so),
    and the live estab staging table is still the 35-column pre-F1.4a shape -- it
    carries neither snapshot column, because no estabelecimentos ingest has run
    since. Repromoting such a batch therefore skips `unprovable_snapshot_ref_date`
    correctly, every row reads clean, and the rows are appended into 37-column
    bronze where Delta fills the absent column with NULL. That NULL is the value
    the rule exists to refuse, arriving precisely because the rule was not there
    to refuse it. Nothing fails; the control is simply absent.

    Not an error: raising would make the documented rebuild unrunnable, and the
    skip is right. The fix is that the run log says it happened.

    `.columns` is a catalog lookup on the schema, not a scan, and it asks the SAME
    staging table the promote reads its rows from -- a notice about some other
    coordinate would be worse than no notice."""
    notice = skip_notice(spark.read.table(staging).columns, rules,
                         task="promote_batch", source=staging)
    if notice is not None:
        print(notice)


def _report(result: PromoteResult, tbl: str, quarantine: str) -> None:
    """What the run log says happened. Extracted from `main` to keep it readable;
    every branch here is a print, and the decisions were all made above."""
    if result.outcome is PromoteOutcome.ALREADY_PROMOTED:
        print(f"promote_batch: batch {result.batch_id} is ALREADY in {tbl} with all "
              f"{result.bronze_rows} of its promotable rows -- append skipped "
              "(idempotent re-run of a promote that had committed)")
    elif result.outcome is PromoteOutcome.ALREADY_PROMOTED_STAGING_GONE:
        print(f"promote_batch: batch {result.batch_id} is ALREADY in {tbl} "
              f"({result.bronze_rows} rows) and staging no longer holds it, so the row "
              "count could not be re-checked -- append skipped")
    else:
        print(f"promote_batch: appended {result.appended_rows} rows "
              f"(batch {result.batch_id}) to {tbl}")
    # The rejects of this batch were left in quarantine. For the operator job that
    # number is the point: a human read them and accepted them. It is only a number
    # when the promote could derive it -- `rejected_rows is None` says the batch's
    # rejects were counted from staging and staging no longer holds the batch, so this
    # task has no count. It used to print that case as "0 rejected row(s) ... stay in
    # quarantine", which is a claim about the quarantine table nothing verified, and
    # exactly what the ALREADY_PROMOTED_STAGING_GONE branch above refuses to do about
    # the promotable count.
    if result.rejected_rows is None:
        print(f"promote_batch: how many rows of batch {result.batch_id} are in "
              "quarantine is NOT knowable from here -- that count comes from the "
              "staging table, which no longer holds this batch. Read it from the "
              f"quarantine table itself: SELECT count(*) FROM {quarantine} WHERE "
              f"{BATCH_COLUMN} = '{result.batch_id}'; this task promoted nothing "
              "out of it either way")
    else:
        print(f"promote_batch: {result.rejected_rows} rejected row(s) of batch "
              f"{result.batch_id} stay in quarantine, out of {tbl}")


def _assert_constraints(spark: SparkSession, spec: BronzeTable, tbl: str) -> None:
    """Re-assert the table's declarative Delta constraints.

    The DDL lives in the registry rather than here so that adding a table cannot
    silently inherit another's constraints -- `cnpj_basico` happens to exist in
    three of the four CNPJ contracts, which is exactly the kind of coincidence
    that makes a copied constraint look correct."""
    for statement in spec.constraints:
        spark.sql(statement.format(table=tbl))


if __name__ == "__main__":
    main()
