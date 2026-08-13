# databricks/src/gold_load_dimension.py
"""Job task: build one Kimball SCD2 dimension from the Data Vault satellite it declares.

ONE PARAMETER NAMES A TABLE, AND THAT IS THE WHOLE POINT OF THE GOLD REGISTRY. Every
vault task is handed TWO registry keys -- a vault table and a bronze source -- because a
hub can have two feeds, and `tests/test_vault_job_wiring.py` exists largely because a
copied YAML can leave either behind with both wrong values naming tables that EXIST. A
dimension has no such freedom: `opl.gold.registry.Scd2Dimension` declares the satellite
it derives from, so the source is resolved here rather than passed, and the pairing
cannot be got wrong by a paste. The satellite's parent hub is resolved the same way,
which is the fix `opl.vault.satellites._refuse_a_mismatched_hub`'s own message names.

THREE TABLES ARE QUALIFIED AND ALL THREE COME FROM `opl.config.DEFAULT.table` -- the
dimension, the satellite it reads and that satellite's hub. A gold build touches more
names than any other task in this repository, which is exactly why none of them is
spelled here: `opl.gold.registry` and `opl.vault.registry` own the unqualified names and
`opl.config` owns the catalog and the schema, and on Free Edition's single catalog a
hardcoded `workspace.default.` would be invisible until the day there is a second one.

`months` IS A DECLARATION AND NOT A FILTER, WHICH IS UNIQUE TO THIS LAYER AMONG THE
JOBS IN THIS REPOSITORY. A bronze ingest STAMPS its month onto every row it reads; a
vault load READS `_snapshot_month` and narrows to it. An SCD2 build can do neither: a
version's end date is the NEXT version's start, so a build that read a subset of the
snapshots would close intervals against versions it could not see and report success. So
the window states which snapshots this dimension covers and `opl.gold.dimensions` checks
it against the ones the satellite actually holds -- which makes a build launched after an
unnoticed snapshot landed REFUSE rather than quietly cover it. Its default is
`opl.config.SENTINEL_MONTH`, a value `required_months` rejects, so a run launched without
`--params` stops instead of declaring a window nobody chose.

    databricks bundle run opl_gold_dim_company -t free \\
      --params months=2026-06+2026-07,revision=$(git rev-parse HEAD)

THE JOB PARAMETERS ARE PARSED BY `opl.vault.job_params`, WHICH IS NAMED FOR ONE LAYER AND
IS ABOUT ANOTHER THING. `MONTH_SEPARATOR` being `+` rather than a comma is a property of
`databricks bundle run --params`, the sentinel is shared with every job in this
repository, and an unresolved `{{job.start_time.iso_datetime}}` arriving verbatim needs
the same diagnosis whichever task it reaches. Forking any of that would give an operator
two launch grammars for one CLI. The module's name is narrower than its subject; a second
copy of its rules would be worse than the name.

argv: [table, months, load_date]"""
import sys

from pyspark.sql import SparkSession

from opl.config import DEFAULT, SESSION_TIMEZONE, SESSION_TIMEZONE_CONFIG

# THE STAR'S HEADLINE ARITHMETIC IS ONE ROW PER SATELLITE VERSION PLUS THE UNKNOWN
# MEMBER, and `GHOST_ROWS` is imported rather than restated here because `load_dimension`
# now REFUSES a target that is not exactly that. A `+ 1` of this task's own would be a
# second spelling of the number the loader enforces, and the line below would be able to
# reconcile against a total the load had already rejected.
from opl.gold.columns import GHOST_ROWS
from opl.gold.dimensions import DimensionLoadResult, load_dimension
from opl.gold.registry import table_spec as gold_table_spec
from opl.gold.specs import Scd2Dimension
from opl.vault import domains
from opl.vault.job_params import required_load_date, required_months


def _refuse_a_dimension_this_task_cannot_build(spec) -> Scd2Dimension:
    """Refuse a conformed dimension handed where an SCD2 one belongs, BEFORE the session.

    ADDED WHEN THE SECOND KIND ARRIVED (F3 Task 3), because until then the gold registry
    held one kind and this check would have refused nothing. `dim_date`, `dim_channel`
    and `dim_currency` are registered gold tables spelled correctly, they sit in a job
    beside this one, and `table` is the only parameter either task takes -- so a copied
    YAML puts one here. Without this the run fails on an `AttributeError` several lines
    into `main`, after a serverless session has started; `opl.gold.specs` gives the two
    kinds different field names for their satellites so that it can only ever be an
    AttributeError and never a plausible dimension built from the wrong spec."""
    if isinstance(spec, Scd2Dimension):
        return spec
    raise ValueError(
        f"gold table {spec.name!r} is not an SCD2 dimension: this task builds a "
        "satellite's version chain with a half-open interval on it. A conformed "
        "dimension is a declared value domain or a calendar and is built by "
        "gold_load_conformed_dimension.py, which takes no months window"
    )


def _reconciliation_note(result: DimensionLoadResult) -> str:
    """Whether the rows the target HOLDS are the source's versions plus the ghost, said
    in words.

    IT RECONCILES THE TOTAL AND NOT THE APPEND, which is the correction this function is.
    It used to test `appended` against the expectation and to take the idempotent re-run
    as a branch that needed no arithmetic at all -- so a re-run over a permanently short
    dimension printed a clean "the re-run is a no-op" line, `already_present` never being
    compared to anything. The shortfall it was written to make visible was invisible on
    exactly the branch a retry lands in, and `max_retries: 0` does not prevent a retry.

    THE THIRD BRANCH IS NOT REACHABLE THROUGH `load_dimension` ANY MORE, and it is kept
    deliberately. `_refuse_a_count_that_is_not_every_version_plus_the_ghost` raises on
    that state before a result is ever built, which is where enforcement belongs -- a
    printed sentence is not a guard. This is a total taken from a frozen dataclass any
    caller can construct, and the sentence an operator would meet if that refusal were
    ever relaxed; it costs three lines and it is the one branch whose absence would be
    invisible."""
    expected = result.source_versions + GHOST_ROWS
    held = result.already_present + result.appended
    if held != expected:
        return (
            f"which does NOT reconcile: the table holds {held} rows against "
            f"{result.source_versions} satellite versions + {GHOST_ROWS} ghost = "
            f"{expected}. The difference is satellite versions whose hash key matched no "
            "row in the hub -- a dangling reference, which nothing in the vault errors "
            "on and which this build drops rather than inventing a key for"
        )
    if result.appended == 0 and result.already_present:
        return (
            f"nothing was appended: the target already held {held} rows, which IS "
            f"{result.source_versions} satellite versions + {GHOST_ROWS} ghost, and this "
            "build reproduces them exactly -- so the re-run is a no-op"
        )
    return (
        f"which reconciles: {result.source_versions} satellite versions + "
        f"{GHOST_ROWS} ghost"
    )


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # The gold table first and BEFORE the window, so a mistyped name is refused by a
    # registry naming the valid alternatives rather than by a message about months.
    spec = _refuse_a_dimension_this_task_cannot_build(gold_table_spec(args[0] if args else ""))
    months = required_months(args[1] if len(args) > 1 else "", action=f"build {spec.name}")
    load_date = required_load_date(args[2] if len(args) > 2 else "")
    satellite = domains.table_spec(spec.source_satellite)
    hub = domains.parent_hub(satellite)
    spark = SparkSession.builder.getOrCreate()
    # BEFORE ANY TABLE IS READ. This build turns a DATE into an instant, and that
    # conversion resolves through the session zone -- so the setting decides where every
    # version boundary lands relative to the UTC `event_time` the fact asks with.
    spark.conf.set(SESSION_TIMEZONE_CONFIG, SESSION_TIMEZONE)
    result = load_dimension(
        spark,
        spec,
        satellite=satellite,
        hub=hub,
        source_table=DEFAULT.table(satellite.name),
        hub_table=DEFAULT.table(hub.name),
        target_table=DEFAULT.table(spec.name),
        load_date=load_date,
        months=list(months),
    )
    print(
        f"gold_load_dimension: {result.table} +{result.appended} rows from "
        f"{satellite.name} keyed on {hub.name} over {list(months)}, "
        f"{_reconciliation_note(result)}; {result.distinct_keys} distinct "
        f"{spec.surrogate_key} values, which is every row (a collision refuses)"
    )


if __name__ == "__main__":
    main()
