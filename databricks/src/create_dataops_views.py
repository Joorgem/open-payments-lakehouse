# databricks/src/create_dataops_views.py
"""Job task: (re)create the DataOps views. Derives; writes no row anywhere.

WHY A JOB AND NOT A HAND-RUN STATEMENT. There is no `tables` resource and no `View`
resource in a Databricks Asset Bundle -- measured against `databricks bundle schema`
on CLI 1.8.0, whose complete resource list carries Schema and Volume and neither of
those two. So a view is either created by a job or it is "documented manual state",
which is the failure mode F4's governance section names and refuses. This task is the
same answer T4 gives for grants, one artefact earlier.

WHY IT IS GUARDED. It writes nothing, so the usual argument -- a stale wheel appends
wrong rows -- does not apply, and the real one is sharper: this task's whole output IS
the deployed wheel's code. `opl.bronze.reconcile` derives the SQL from `REGISTRY`, and
`CREATE OR REPLACE VIEW` overwrites whatever definition was there. A wheel from another
revision therefore installs a reconciliation that is individually well-formed, that
reports verdicts nobody reviewed, and that reports them under the name the correct one
would have had -- silently, over a dashboard an operator then believes. Nothing fails.

IDEMPOTENT, and the idempotence is `CREATE OR REPLACE`, not `IF NOT EXISTS`. The
distinction is argued in `reconcile.create_view_ddl`: `IF NOT EXISTS` is right for a
table, whose rows must not be dropped, and wrong for a view, where it would leave an
older definition standing while the run that was meant to replace it reports SUCCESS.
That matters here because `max_retries: 0` does not prevent a retry -- measured, 20
task-pairs across 6 task keys ran attempt 0 and attempt 1 -- so every statement this
task issues must be safe to run twice.

WHAT IT DELIBERATELY DOES NOT DO: promote anything. `dataops_reconciliation` prints the
`repromote_triaged_batch` command for every stranded batch and runs none of them.
Whether a stranded batch should be promoted is a decision with an owner and a
consequence for the observation ledger; a task that acted on its own verdict would be
a gate bypass wearing a dashboard.

argv: [] -- this task takes none. The views it creates are total over `REGISTRY`, so
there is no table to hand it and no coordinate for a parameter to get wrong."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.reconcile import view_ddls
from opl.config import DEFAULT


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    if args:
        raise ValueError(
            f"create_dataops_views takes no arguments and was handed {args!r}. A task "
            "given a parameter it does not read is a job YAML that believes it is "
            "configuring something -- see the header's argv line."
        )
    spark = SparkSession.builder.getOrCreate()
    ddls = view_ddls(DEFAULT)
    for ddl in ddls:
        # Printed before it is issued, so a failure names the statement that failed
        # rather than leaving which of the two to be inferred from the traceback.
        print(f"create_dataops_views: issuing\n{ddl}")
        spark.sql(ddl)
    print(f"create_dataops_views: {len(ddls)} views in {DEFAULT.catalog}.{DEFAULT.schema}")


if __name__ == "__main__":
    main()
