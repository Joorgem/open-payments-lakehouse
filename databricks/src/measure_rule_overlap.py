# databricks/src/measure_rule_overlap.py
"""Job task: how many staged rows match EACH DQ rule, and how many match two or more.

WHAT IT IS FOR. ADR 0006 rejected a per-reason reject tolerance and listed three
conditions that would reverse that. The first is structural: "per-reason counts derived
from **all** matching rules, not from the first ... it must ship BEFORE any per-reason
tolerance, never with it." This task is that count and only that count. IT CHANGES NO
GATE AND ADDS NO TOLERANCE -- it reads staging and prints numbers. See
`opl.bronze.rule_overlap` for the arithmetic and for which of the ADR's three conditions
this closes (one) and which no code can close (condition 2, a non-degenerate numerator).

WHY IT PRINTS AND WRITES NOTHING, which is a decision and not an omission:

  * Its product is a number in an evidence document, taken once per rule set. It is not
    an input to any control -- nothing branches on it, no view reads it, no gate consults
    it -- so a table would be a Delta object that exists to hold a value nobody queries,
    with a schema that changes shape every time a contract gains a rule (the columns ARE
    the reason strings).
  * This phase's Task 1 took the same fork for the reconciliation and derived rather than
    wrote, and the argument is stronger here: a reconciliation view is re-read by an
    operator on a dashboard, and this is a one-off answer to a closed question.
  * A written table would also need a name, and a name in this catalog that no registry
    owns is policed by none of the three registry-collision guards -- the gap
    `opl.bronze.reconcile` names when it explains its own `dataops_` prefix.
  * RE-DERIVABILITY DOES NOT DEPEND ON THE TABLE. This task takes no parameters and is
    total over `REGISTRY`, so re-deriving the numbers is running it again; the run log is
    the record, and the answer is a function of (staging contents, deployed rule set)
    exactly as ADR 0006 warns every reject figure in this repository is.

THE OUTPUT IS ONE SHAPE FOR EVERY NUMBER: a tab-separated `table batch key value`, so a
run log can be pasted into a document or grepped without a second format to parse. EVERY
reason is printed, INCLUDING the zeros. A reason omitted because it counted zero is
indistinguishable, in a log, from a reason nobody measured -- and "the overlap is zero"
is the claim this whole task exists to make checkable.

WHY IT IS GUARDED. It runs in `dataops_views_job.yml`, behind that job's existing
revision guard, and inherits its argument: the numbers this prints ARE the deployed
wheel's rule set applied to staging. A wheel from another revision prints a
well-formed-looking table of counts for rules nobody deployed, and ADR 0006's own warning
is that "every rate on this page is a rate of the rule set deployed when it was
measured". A measurement that cannot say which rule set produced it is not a measurement.

argv: [] -- this task takes none. It is total over `REGISTRY` at every grain it reports,
so there is no table and no batch to hand it and no coordinate for a parameter to get
wrong."""
import sys
from collections.abc import Sequence
from typing import Any

from pyspark.sql import SparkSession

from opl.bronze.dq import skip_notice
from opl.bronze.promote import BATCH_COLUMN
from opl.bronze.registry import REGISTRY, BronzeTable
from opl.bronze.rule_overlap import ROW_COUNT, overlap_frame
from opl.bronze.rules import rules_for
from opl.config import DEFAULT

TASK = "measure_rule_overlap"

# The batch coordinate for a line that belongs to a TABLE rather than to one of its
# batches. Every ingest job passes `{{job.run_id}}` as the batch id, so a live one is a
# run id's digits and this is not a value a reader will mistake for one -- though nothing
# ENFORCES that shape (`require_batch_id` refuses only a blank and the sentinel), which is
# why this is a legible placeholder and not a claim about uniqueness.
NO_BATCH = "-"


def _emit(table: str, batch: str, key: str, value: object) -> None:
    """One number, one line, one shape. Tab-separated so a log is a table."""
    print(f"{TASK}\t{table}\t{batch}\t{key}\t{value}")


def _emit_rows(table: str, rows: Sequence[Any], keys: Sequence[str]) -> int:
    """Every line for one table, from the collected aggregate. Returns the rows read.

    SEPARATE FROM `_measure` SO IT CAN BE DRIVEN WITHOUT SPARK: it needs only indexing by
    column name, which a `Row` and a dict both do, and the empty case below is otherwise
    reachable in a test only by creating an empty staging table.

    AN EMPTY STAGING TABLE GROUPS INTO NO ROWS. Left alone, the loop prints nothing at
    all for it, and the table is missing from the log for exactly the reason the header
    refuses to omit a zero-count reason: absence and "nobody measured it" are the same
    line to a reader. So the table gets one line saying it read no rows -- the same
    `table batch key value` shape as every other, with a batch coordinate that is not a
    batch, because there is no batch to name."""
    if not rows:
        _emit(table, NO_BATCH, ROW_COUNT, 0)
    read = 0
    for row in rows:
        for key in keys:
            _emit(table, row[BATCH_COLUMN], key, row[key])
        read += row[ROW_COUNT]
    return read


def _measure(spark: SparkSession, spec: BronzeTable) -> int:
    """Print every number for one table, at batch grain. Returns the rows it read.

    A MISSING STAGING TABLE IS LEFT TO RAISE, unlike `reconcile._counts_sql`'s `skip`.
    That mechanism exists so a RECLAIM can decide from whatever exists; here the absent
    table is the answer -- a measurement that silently omitted a contract would publish
    "zero overlap everywhere" over a set that was never six tables wide."""
    rules = rules_for(spec.contract)
    staging = DEFAULT.table(spec.staging)
    df = spark.read.table(staging)
    notice = skip_notice(df.columns, rules, task=TASK, source=staging)
    if notice is not None:
        print(notice)
    frame = overlap_frame(df, rules, group_by=(BATCH_COLUMN,))
    # The projection's own order -- the headline numbers, then one column per rule that
    # ran -- rather than a second list built here. A report ordered by a list this file
    # maintains would drift from the aggregate it describes.
    keys = [column for column in frame.columns if column != BATCH_COLUMN]
    return _emit_rows(spec.name, frame.collect(), keys)


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    if args:
        raise ValueError(
            f"{TASK} takes no arguments and was handed {args!r}. A task given a "
            "parameter it does not read is a job YAML that believes it is configuring "
            "something -- see the header's argv line."
        )
    spark = SparkSession.builder.getOrCreate()
    # ONE ELEMENT PER TABLE ACTUALLY MEASURED, and the summary counts THAT rather than
    # `len(REGISTRY)`. The two are equal only while this loop is total over the registry,
    # which is precisely the property the line is quoted as evidence for: the shipped
    # version printed the registry's length, so a sweep narrowed to three tables printed
    # "7 tables" with every per-rule number below it still correct, and the published
    # "fifteen pairs, seven contracts" would have been false with nothing to contradict
    # it. A summary that can only report what it saw cannot say that.
    measured = [_measure(spark, spec) for spec in REGISTRY.values()]
    print(f"{TASK}: {len(measured)} tables, {sum(measured)} staged rows read")


if __name__ == "__main__":
    main()
