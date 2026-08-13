# databricks/src/gold_load_fact.py
"""Job task: build the star's fact table -- one row per payment event, both counterparties
resolved as of the payment's own `event_time`.

ONE PARAMETER NAMES A TABLE, exactly as the three gold tasks beside it do.
`opl.gold.specs.PaymentFact` declares the dimension its roles resolve against, the
conformed dimensions it reaches and the order it projects their keys in, so there is no
source parameter for a copied YAML to leave behind naming a table that exists -- which is
the mistake the four vault jobs' two-key tasks can make and no gold task can.

THE KIND IS REFUSED BEFORE `getOrCreate()`, AND THIS IS NOW THE FOURTH GOLD ENTRY POINT.
Every registered gold name is spelled correctly and sits in a job beside this one, so the
paste that reaches this task is a name from one of the other three; handed `dim_company`,
`load_fact` would fail on `spec.roles` several lines into a serverless session that has
already started costing money. `tests/test_gold_job_wiring.py::test_each_gold_entry_point
_refuses_the_kind_the_other_one_builds` drives all twelve pairings, and it is twelve rather
than six because the refusals are INCLUSIONS -- a kind added later acquires the other
tasks' refusals for free only while they are written that way.

IT TAKES NO `months`, FOR THE THIRD DISTINCT REASON IN THIS DIRECTORY. The SCD2 build
DECLARES a window so it can refuse a snapshot the launch did not name; the PIT takes none
because a later snapshot cannot move a pointer already written; and this task takes none
because a month window is a property of the RFB's SNAPSHOTS and a fact is grained on
PAYMENTS. A payment batch that landed since the last build is a set of new
`transaction_id` values to append, and `opl.gold.facts._new_rows` reads the target rather
than a parameter to find them -- a parameter would state an intention where the anti-join
reads the table.

FOUR NAMES ARE QUALIFIED AND ALL FOUR COME FROM `opl.config.DEFAULT.table` -- the fact,
the bronze payments it reads, `dim_company`, and the conformed dimensions (one call, in a
comprehension over the declared list, which is why the number is four and not six). On
Free Edition's single catalog a hardcoded `workspace.default.` would be invisible until
the day there is a second one.

WHAT THE RUN LOG PUBLISHES, AND WHY IT IS FOUR SENTENCES RATHER THAN A ROW COUNT. The row
count is enforced by the loader and printing it proves nothing; what an operator needs is
the three numbers that separate a working star from a plausible one -- resolution at (row,
role) grain, the legitimate repeats that survived deduplication, and the referential
integrity of the three DERIVED conformed keys. Each has a state that must not read like
success: zero unresolved is an UNEXERCISED path and not a triumph (master protocol section
4.6), zero legitimate repeats means the fact ate 1,600 real payments, and a non-zero orphan
count means a conformed dimension is older than the payments.

    databricks bundle run opl_gold_fact_payment -t free \\
      --params revision=$(git rev-parse HEAD)

argv: [table, load_date]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT, SESSION_TIMEZONE, SESSION_TIMEZONE_CONFIG
from opl.contracts import payments
from opl.gold.facts import FactLoadResult, load_fact
from opl.gold.registry import table_spec as gold_table_spec
from opl.gold.specs import PaymentFact
from opl.vault import domains
from opl.vault.job_params import required_load_date


def _refuse_a_table_this_task_cannot_build(spec) -> PaymentFact:
    """Refuse a dimension or a PIT handed where the fact belongs, BEFORE the session.

    `dim_company`, `dim_date`, `dim_channel`, `dim_currency` and `pit_estabelecimento` are
    registered gold tables spelled correctly, they sit in three jobs beside this one, and
    `table` is the only parameter three of the four tasks take -- so a copied YAML puts one
    here. Without this the run dies on an `AttributeError` inside `load_fact` after a
    serverless session has started."""
    if isinstance(spec, PaymentFact):
        return spec
    raise ValueError(
        f"gold table {spec.name!r} is not the payment fact: this task builds one row per "
        "payment event with a foreign key per counterparty role, resolved as of the "
        "payment's own event_time. A dimension is built by gold_load_dimension.py (SCD2, "
        "which takes a months window) or by gold_load_conformed_dimension.py (a declared "
        "value domain, or the calendar); the as-of index over a hub's satellites is built "
        "by gold_load_pit.py"
    )


def _resolution_note(result: FactLoadResult, roles: int) -> str:
    """Resolution at (ROW, ROLE) grain, said so that zero does not read as a triumph.

    THE GRAIN IS THE WHOLE SENTENCE. The acceptance this task inherited asks that every
    row resolve to exactly ONE dimension version; a correct row resolves to TWO, one per
    role, so the denominator is `roles x rows` and the numerator is reported per role. A
    total would hide the asymmetric failure -- a fact that resolved payers and not payees
    reads as a 50% data-quality problem rather than as a missing foreign key.

    ZERO IS AN UNEXERCISED PATH AND SAYS SO (master protocol section 4.6). F1b measured
    1,024/1,024 counterparties resolving to `hub_empresa` and `dim_company` covers every
    hub key, so `COALESCE(<as-of lookup>, GHOST)` cannot fire on this data. That is a
    prediction, not an achievement, and a run log that printed "0 unresolved" beside a
    resolution rate would let the evidence report a path nothing ran through as working."""
    held = result.already_present + result.appended
    unresolved = sum(count for _key, count in result.unresolved)
    per_role = ", ".join(f"{key}: {count}" for key, count in result.unresolved)
    if unresolved == 0:
        return (
            f"{roles * held} (row, role) references over {held} rows, all resolved and "
            f"NONE on the ghost ({per_role}) -- so the unknown-member path is UNEXERCISED "
            "rather than proven: every counterparty is drawn from hub_empresa's own key "
            "space, so nothing in this data can fail to resolve"
        )
    return (
        f"{roles * held} (row, role) references over {held} rows, of which {unresolved} "
        f"resolved to the ghost ({per_role}) -- a counterparty with no dim_company version "
        "in force at its payment's instant"
    )


def _repeat_note(result: FactLoadResult) -> str:
    """Whether the LEGITIMATE REPEATS survived deduplication, said in words.

    THE NUMBER THAT IS NOT A TAUTOLOGY. "The build removed 150 duplicates" is
    `COUNT(*) - COUNT(DISTINCT transaction_id)` by definition of the operation and comes
    out right whichever column the deduplication was taken over. This one cannot: a
    legitimate repeat is a DIFFERENT `transaction_id` under an IDENTICAL business-attribute
    tuple, so a fact deduplicated on the attributes reports ZERO here while every other
    number still looks plausible."""
    if result.legitimate_repeats == 0:
        return (
            f"and 0 legitimate repeats survived against {result.retained_tuples} distinct "
            "business tuples, which is NOT a clean stream -- both promoted profiles emit "
            "800 repeats each on purpose, so zero here means the deduplication was taken "
            "over the business attributes and deleted real payments"
        )
    return (
        f"and {result.legitimate_repeats} legitimate repeats survived it "
        f"({result.retained_tuples} distinct business tuples), which is the count a "
        "deduplication over the business attributes would have deleted"
    )


def _integrity_note(result: FactLoadResult) -> str:
    """Whether every DERIVED conformed key names a member that exists.

    THE TWO STATES MUST NOT READ ALIKE, which is why this is a function -- the reason
    `gold_load_dimension.py` gives for its own note. The three conformed keys are computed
    from the fact's own columns with no join, so there is no ghost to fall back on: an
    orphan is a payment whose day, rail or currency the dimension does not hold. It is
    REPORTED and not refused because the likeliest cause is order -- a conformed dimension
    built before this batch of payments landed -- and the repair is to re-run that build,
    which appends."""
    orphans = [(name, count) for name, count in result.orphaned if count]
    if not orphans:
        return "every derived conformed key names a member that exists"
    listed = ", ".join(f"{name}: {count} rows" for name, count in orphans)
    return (
        f"but {listed} carry a derived key NO MEMBER of that dimension holds. These keys "
        "are computed from the payment's own columns rather than looked up, so an orphan "
        "does not fall back on the unknown member -- it joins to nothing and disappears "
        "from every report grouped by that dimension. Re-run the conformed build, which "
        "APPENDS the missing members; this fact needs no repair"
    )


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # The gold table first and BEFORE the timestamp, so a mistyped name is refused by a
    # registry naming the valid alternatives rather than by a message about a date.
    spec = _refuse_a_table_this_task_cannot_build(gold_table_spec(args[0] if args else ""))
    load_date = required_load_date(args[1] if len(args) > 1 else "")
    dimension = gold_table_spec(spec.company_dimension)
    hub = domains.parent_hub(domains.table_spec(dimension.source_satellite))
    conformed = tuple(gold_table_spec(name) for name in spec.conformed)
    spark = SparkSession.builder.getOrCreate()
    # BEFORE ANY TABLE IS READ, and this task is the one with the most to lose by it. The
    # as-of comparison is between an instant parsed out of `event_time` -- zone-explicit,
    # so it does not move -- and `dim_company`'s interval bounds, which were WRITTEN under
    # this pin. `load_date` is a plain instant and moves with the zone like every other.
    spark.conf.set(SESSION_TIMEZONE_CONFIG, SESSION_TIMEZONE)
    result = load_fact(
        spark,
        spec,
        dimension=dimension,
        hub=hub,
        conformed=conformed,
        source_table=DEFAULT.table(bronze_table_spec(payments.CONTRACT).bronze),
        dimension_table=DEFAULT.table(dimension.name),
        # KEYED BY DIMENSION NAME, which is what stops two tables being swapped: the fact's
        # keys are named after the dimensions, so a positional pairing that transposed them
        # would check each key against another dimension's members and report every row as
        # an orphan.
        conformed_tables={name: DEFAULT.table(name) for name in spec.conformed},
        target_table=DEFAULT.table(spec.name),
        load_date=load_date,
    )
    print(
        f"gold_load_fact: {result.table} +{result.appended} rows from "
        f"{result.source_rows} bronze payments over {result.source_identities} distinct "
        f"{spec.grain_key} keyed on {dimension.name} and "
        f"{[item.name for item in conformed]}; {_resolution_note(result, len(spec.roles))}; "
        f"{_repeat_note(result)}; {_integrity_note(result)}"
    )


if __name__ == "__main__":
    main()
