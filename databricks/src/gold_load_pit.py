# databricks/src/gold_load_pit.py
"""Job task: build one Data Vault point-in-time table over the hub and satellites it
declares.

ONE PARAMETER NAMES A TABLE, exactly as both gold tasks beside it do. `opl.gold.specs
.PointInTimeTable` declares the hub, the satellites and the as-of column name, so there
is no source parameter for a copied YAML to leave behind naming a table that exists --
which is the mistake the four vault jobs' two-key tasks can make and no gold task can.

THE KIND IS REFUSED BEFORE `getOrCreate()`, AND THAT LOCK IS THE REASON THIS FUNCTION
EXISTS. Three gold entry points now take a registered gold table as their first argument
and every registered name is spelled correctly, so the paste that reaches this task is a
name from one of the other two jobs. Handed `dim_company`, `load_pit` would fail on
`spec.hub` several lines into a serverless session that has already started costing
money; handed here it is refused by kind, on the driver, before Spark exists.
`tests/test_gold_job_wiring.py::test_each_gold_entry_point_refuses_the_kind_the_other_one
_builds` drives all six pairings.

IT TAKES NO `months`, AND THAT IS A DECISION RATHER THAN AN OMISSION. The SCD2 build
DECLARES a window so it can refuse when the vault has gained a snapshot the launch did
not name -- it must, because a version's end date is the next version's start and an
append cannot revise a closed interval. A PIT pointer is `max(applied_date <=
as_of_date)`, which a LATER snapshot cannot move, so a snapshot this table did not know
about is a layer to APPEND and nothing already written changes. The one case that IS a
revision -- a snapshot backfilled BETWEEN two the table holds -- is refused by
`opl.gold.pit` from the dates themselves, which a `months` parameter could not improve
on: the parameter would state an intention, where the refusal reads the table.

THREE NAMES ARE QUALIFIED AND ALL THREE COME FROM `opl.config.DEFAULT.table` -- the PIT,
its hub, and its satellites (one call, over the declared list). On Free Edition's single
catalog a hardcoded `workspace.default.` would be invisible until the day there is a
second one.

WHAT THE RUN LOG PUBLISHES AND WHY IT IS TWO SENTENCES. The reconciliation is the last
as-of layer against the hub's key count: they are equal exactly when every hub key has
some descriptive history. A shortfall is a hub key no satellite ever wrote a row for --
which this build cannot repair and must not hide, and which is REPORTED rather than
refused because a hub with a second feed (`hub_empresa` is loaded from estabelecimentos
as well as from empresas) can legitimately hold keys one satellite never sees.

    databricks bundle run opl_gold_pit_estabelecimento -t free \\
      --params revision=$(git rev-parse HEAD)

argv: [table, load_date]"""
import sys

from pyspark.sql import SparkSession

from opl.config import DEFAULT, SESSION_TIMEZONE, SESSION_TIMEZONE_CONFIG
from opl.gold.pit import PitLoadResult, load_pit
from opl.gold.registry import table_spec as gold_table_spec
from opl.gold.specs import PointInTimeTable
from opl.vault import domains
from opl.vault.job_params import required_load_date


def _refuse_a_table_this_task_cannot_build(spec) -> PointInTimeTable:
    """Refuse a dimension handed where a point-in-time table belongs, BEFORE the session.

    `dim_company`, `dim_date`, `dim_channel` and `dim_currency` are registered gold tables
    spelled correctly, they sit in two jobs beside this one, and `table` is the only
    parameter any of the three tasks takes -- so a copied YAML puts one here. Without this
    the run dies on an `AttributeError` inside `load_pit` after a serverless session has
    started."""
    if isinstance(spec, PointInTimeTable):
        return spec
    raise ValueError(
        f"gold table {spec.name!r} is not a point-in-time table: this task builds the "
        "as-of index over a hub and two or more of its satellites. A dimension is built "
        "by gold_load_dimension.py (SCD2, which takes a months window) or by "
        "gold_load_conformed_dimension.py (a declared value domain, or the calendar)"
    )


def _reconciliation_note(result: PitLoadResult) -> str:
    """Whether the last as-of layer is the hub's whole key set, said in words.

    THE TWO STATES MUST NOT READ ALIKE, which is why this is a function and not an
    f-string -- `gold_load_dimension.py`'s reason. A shortfall is hub keys for which no
    satellite ever wrote a row: they are absent from every layer of this table, and a run
    that printed only its own row count would leave that invisible. The idempotent re-run
    is a third state and says so rather than reporting a shortfall of everything."""
    if result.appended == 0 and result.already_present:
        return (
            f"nothing was appended: the target already held {result.already_present} rows "
            "and this build reproduces them exactly, so the re-run is a no-op"
        )
    _last, covered = result.rows_per_as_of[-1]
    if covered == result.hub_keys:
        return (
            f"which reconciles: the last as-of layer holds {covered} rows and "
            f"{result.hub_keys} keys are in the hub, so every key has descriptive history"
        )
    return (
        f"which does NOT reconcile: the last as-of layer holds {covered} rows against "
        f"{result.hub_keys} hub keys, so {result.hub_keys - covered} keys have no row in "
        "ANY satellite and are absent from every layer of this table. That is a hub key "
        "with no descriptive history -- legitimate for a hub with a second feed, and a "
        "short satellite load otherwise; this build reports it and repairs neither"
    )


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # The gold table first and BEFORE the timestamp, so a mistyped name is refused by a
    # registry naming the valid alternatives rather than by a message about a date.
    spec = _refuse_a_table_this_task_cannot_build(gold_table_spec(args[0] if args else ""))
    load_date = required_load_date(args[1] if len(args) > 1 else "")
    hub = domains.table_spec(spec.hub)
    spark = SparkSession.builder.getOrCreate()
    # PINNED BEFORE ANY DERIVATION, for the reason the other two gold tasks pin it: a
    # TIMESTAMP is UTC micros and every conversion into or out of one resolves through the
    # session zone, which is UTC on serverless and the OS zone locally. This table's
    # `as_of_date` and its pointers are DATEs and so are already immune -- but `load_date`
    # is not.
    #
    # THIS COMMENT USED TO END "a task that left the zone unpinned would be the one gold
    # task whose behaviour depended on where it ran", AND THAT WAS BACKWARDS. `opl.gold.pit`
    # built its `load_date` with `F.lit(datetime)`, which converts through `time.mktime` --
    # the DRIVER's OPERATING-SYSTEM zone -- so this line changed nothing this task wrote and
    # the LDTS depended on where it ran WITH the pin. It was the one gold task of which that
    # was true, which is the opposite of what the sentence claimed. The fix is in the loader
    # (`instant_literal`, the layer's one instant path) and not here; the pin is still
    # required, because it is now the thing that decides the value.
    spark.conf.set(SESSION_TIMEZONE_CONFIG, SESSION_TIMEZONE)
    result = load_pit(
        spark,
        spec,
        hub=hub,
        hub_table=DEFAULT.table(hub.name),
        # KEYED BY SATELLITE NAME, which is what stops two tables being swapped: the
        # pointer columns are named after the satellites, so a positional pairing that
        # transposed them would write each satellite's dates into the other's column with
        # nothing about the result looking wrong.
        satellite_tables={name: DEFAULT.table(name) for name in spec.satellites},
        target_table=DEFAULT.table(spec.name),
        load_date=load_date,
    )
    layers = ", ".join(
        f"{day.isoformat()}: {rows}" for day, rows in result.rows_per_as_of
    )
    print(
        f"gold_load_pit: {result.table} +{result.appended} rows over "
        f"{[day.isoformat() for day in result.as_of_dates]} from {list(spec.satellites)} "
        f"keyed on {hub.name} ({layers}), {_reconciliation_note(result)}"
    )


if __name__ == "__main__":
    main()
