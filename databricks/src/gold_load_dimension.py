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

from opl.config import DEFAULT
from opl.gold.dimensions import DimensionLoadResult, load_dimension
from opl.gold.registry import table_spec as gold_table_spec
from opl.vault import domains
from opl.vault.job_params import required_load_date, required_months

# What the star's headline claim is: one dimension row per satellite version, plus the
# unknown member. Spelled once, here, because the line below is the only place a reader
# of the run log can check it without opening two documents.
GHOST_ROWS = 1


def _reconciliation_note(result: DimensionLoadResult) -> str:
    """Whether the rows written are the source's versions plus the ghost, said in words.

    THE TWO STATES MUST NOT READ ALIKE, which is why this is a function and not an
    f-string. A shortfall means satellite versions whose hash key matched no hub row --
    a dangling reference the vault does not error on and this build cannot repair -- and
    a run that printed only its own row count would leave that invisible. The idempotent
    re-run is a third state and says so rather than reporting a shortfall of everything."""
    expected = result.source_versions + GHOST_ROWS
    if result.appended == 0 and result.already_present:
        return (
            f"nothing was appended: the target already held {result.already_present} "
            f"rows and this build reproduces them exactly, so the re-run is a no-op"
        )
    if result.appended == expected:
        return (
            f"which reconciles: {result.source_versions} satellite versions + "
            f"{GHOST_ROWS} ghost"
        )
    return (
        f"which does NOT reconcile against {result.source_versions} satellite versions "
        f"+ {GHOST_ROWS} ghost = {expected}. The difference is satellite versions whose "
        "hash key matched no row in the hub -- a dangling reference, which nothing in "
        "the vault errors on and which this build drops rather than inventing a key for"
    )


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # The gold table first and BEFORE the window, so a mistyped name is refused by a
    # registry naming the valid alternatives rather than by a message about months.
    spec = gold_table_spec(args[0] if args else "")
    months = required_months(args[1] if len(args) > 1 else "", action=f"build {spec.name}")
    load_date = required_load_date(args[2] if len(args) > 2 else "")
    satellite = domains.table_spec(spec.source_satellite)
    hub = domains.parent_hub(satellite)
    spark = SparkSession.builder.getOrCreate()
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
