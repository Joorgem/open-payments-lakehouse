# databricks/src/gold_load_conformed_dimension.py
"""Job task: build one conformed dimension of the star -- a declared value domain, or
the calendar -- and report how much of it the fact actually reaches.

ONE PARAMETER NAMES A TABLE, exactly as `gold_load_dimension.py`'s does and for the same
reason: `opl.gold.specs` declares everything else a conformed dimension needs -- its
members, the payment column the fact draws it from, and (for `dim_date`) the satellite
whose `applied_date`s are one end of its span -- so there is no source parameter for a
copied YAML to leave behind naming a table that exists.

IT TAKES NO `months`, AND THAT IS A DECISION RATHER THAN AN OMISSION. Every other job in
this repository takes a month window: a bronze ingest STAMPS it, a vault load NARROWS to
it, and `gold_load_dimension.py` DECLARES it so that an SCD2 build refuses when the vault
has gained a snapshot the launch did not name -- which it must, because a version's end
date is the next version's start and an append cannot revise one it already closed. None
of that applies here. `dim_channel` and `dim_currency` have no snapshot input at all, and
`dim_date` welcomes a snapshot it did not know about: a wider span is days to APPEND, and
no surrogate key moves, because a day's key is its own calendar position. A parameter
this task ignored would be decoration in the one repository whose whole wiring test suite
exists to catch a parameter that means nothing.

THREE TABLES ARE QUALIFIED AND ALL THREE COME FROM `opl.config.DEFAULT.table` -- the
dimension, the FACT SOURCE whose members it measures against, and (for a calendar) the
satellite it spans. `opl.gold.registry`, `opl.bronze.registry` and `opl.vault.domains`
own the unqualified names and `opl.config` owns the catalog and the schema; on Free
Edition's single catalog a hardcoded `workspace.default.` would be invisible until the
day there is a second one.

THE FACT SOURCE IS `bronze_payments` AND NOT `fact_payment`, WHICH DOES NOT EXIST YET.
The number this task publishes -- how many members of the dimension the fact reaches --
is a property of the PAYMENT POPULATION, and `fact_payment` (F3 Task 4) is one row per
payment drawn from exactly this table. Measuring here means the dimensions can be built
and their cardinality published before the fact exists, which is the order the phase
runs in; when the fact lands, the same function measures the same thing over it.

    databricks bundle run opl_gold_conformed_dimensions -t free \\
      --params revision=$(git rev-parse HEAD)

argv: [table, load_date]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT, SESSION_TIMEZONE, SESSION_TIMEZONE_CONFIG
from opl.contracts import payments

# ONE SPELLING OF THE GHOST COUNT, AND IT IS `opl.gold.columns`' -- the same import the
# sibling `gold_load_dimension.py` makes, for the reason stated there: the run log's
# arithmetic must reconcile against the number the LOADER enforces rather than against a
# second copy of it. This task read it from `opl.gold.conformed`, which re-exports it only
# INCIDENTALLY: `GHOST_ROWS` is absent from that module's `__all__`, so the import rested
# on an implementation detail the module does not advertise, and the day `conformed` stops
# needing the constant this task breaks for a reason that has nothing to do with it. One
# object either way today; one declared path to it now.
from opl.gold.columns import GHOST_ROWS
from opl.gold.conformed import ConformedLoadResult, load_conformed_dimension
from opl.gold.registry import table_spec as gold_table_spec
from opl.gold.specs import CalendarDimension, ConformedDimension
from opl.vault import domains
from opl.vault.job_params import required_load_date


def _refuse_a_dimension_this_task_cannot_build(spec) -> ConformedDimension:
    """Refuse an SCD2 dimension handed where a conformed one belongs, BEFORE the session.

    THE PASTE THIS TASK ACTUALLY PRODUCES. `dim_company` is a registered gold table, it
    is spelled correctly, and it sits in the job beside this one -- so it is the single
    most plausible string to end up in this parameter. `opl.gold.conformed` refuses it
    too, but only after `SparkSession.builder.getOrCreate()` has started a serverless
    session; an operator should be told here instead."""
    if isinstance(spec, ConformedDimension):
        return spec
    raise ValueError(
        f"gold table {spec.name!r} is not a conformed dimension: this task builds a "
        "declared value domain or the calendar. An SCD2 dimension is a satellite's "
        "version chain with an interval on it and is built by gold_load_dimension.py, "
        "which takes the months window this task deliberately does not"
    )


def _reach_note(spec: ConformedDimension, result: ConformedLoadResult) -> str:
    """How much of the dimension the fact reaches, said in words as well as in numbers.

    THE TWO STATES MUST NOT READ ALIKE, which is why this is a function and not an
    f-string. A dimension every fact row resolves to ONE member of is a constant column
    wearing a dimension's name, and a run log that printed the member count alone would let
    the phase's evidence describe it as thin instead of measuring it. Master protocol §4.6: a
    path that ran zero rows through it is not a path that works, and the same applies to a
    dimension whose fact-side cardinality is one.

    AND NO DIMENSION IN THIS STAR IS IN THAT STATE ANY MORE, which this docstring used to say
    the opposite of ("true today of `dim_currency` and of `dim_date`"). F-API widened the
    currency domain to `("BRL", "USD")` and landed a profile drawing from both, so
    `dim_currency` reaches 2 and `dim_date` reaches 3. The branch below therefore ships
    UNEXERCISED against the live star -- which is the outcome it was written to produce, and
    is not the same statement as it being untested."""
    reached = result.fact_side_cardinality
    if reached <= 1:
        return (
            f"the fact reaches {reached} of them, so {spec.name!r} is a CONSTANT COLUMN "
            "on this data -- every fact row resolves to one member, no test over it can "
            "fail, and the evidence must publish this number rather than call it thin"
        )
    return f"the fact reaches {reached} of them"


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # The gold table first and BEFORE the timestamp, so a mistyped name is refused by a
    # registry naming the valid alternatives rather than by a message about a date.
    spec = _refuse_a_dimension_this_task_cannot_build(gold_table_spec(args[0] if args else ""))
    load_date = required_load_date(args[1] if len(args) > 1 else "")
    applied = (
        DEFAULT.table(domains.table_spec(spec.applied_date_source).name)
        if isinstance(spec, CalendarDimension)
        else None
    )
    spark = SparkSession.builder.getOrCreate()
    # THE SAME PIN ITS SIBLING SETS, AND NOT BECAUSE THE TWO TASKS SHOULD LOOK ALIKE.
    # `day_of` already reads the calendar day out of the ISO TEXT, so this dimension's
    # members do not move with the zone -- what does move is the `load_date` stamped on
    # every row, which is a plain instant. One run, one LDTS, whichever task wrote it.
    spark.conf.set(SESSION_TIMEZONE_CONFIG, SESSION_TIMEZONE)
    result = load_conformed_dimension(
        spark,
        spec,
        fact_table=DEFAULT.table(bronze_table_spec(payments.CONTRACT).bronze),
        target_table=DEFAULT.table(spec.name),
        load_date=load_date,
        applied_date_table=applied,
    )
    print(
        f"gold_load_conformed_dimension: {result.table} +{result.appended} rows "
        f"({result.members} members + {GHOST_ROWS} ghost, {result.distinct_keys} distinct "
        f"{spec.surrogate_key} values, which is every row); drawn from "
        f"{spec.fact_column!r}, and {_reach_note(spec, result)}"
    )


if __name__ == "__main__":
    main()
