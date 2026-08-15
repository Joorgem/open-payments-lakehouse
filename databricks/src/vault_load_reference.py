# databricks/src/vault_load_reference.py
"""Job task: load one DV2 reference table from the bronze lookup, over one month window.

SIX TASKS OVER ONE SOURCE, AND THE ROUTING IS THE POINT. `bronze_cnpj_lookup` holds all
six reference types in ONE table, distinguished only by `_source_file`, and `codigo` is
unique WITHIN a type and collides ACROSS them -- `'05'`, `'08'`, `'09'`, `'10'` each
name a motivo AND a qualificação, and `'1200'` is both a município and a natureza
jurídica. `opl.vault.reference` routes on the filename per table, so the job runs this
task once per reference table with the same source, and each one narrows to its own
`lookup_type`. A task handed the wrong `ref_*` name loads the wrong six-hundred-odd
rows into the wrong table and reports success, which is why the vault table is this
task's FIRST argument and is checked against the registry before anything else.

THESE TABLES CARRY NO HISTORY, AND THE WINDOW IS STILL REQUIRED. `bronze_cnpj_lookup`
is 2026-06 only -- the 2026-07 lookup zips were never published in that month's set --
so there is no second observation to compare a `descricao` against and no absence for
the ledger to report. `opl.vault.reference` says so and this task does not claim
otherwise. The window is still required for `opl.vault.job_params`' reason, which is
about a forgotten `--params` rather than about this table's history: the day a second
month lands, an unparameterised task would silently widen.

argv: [table, source, months, load_date]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.bronze.snapshot_axis import MONTHLY_SNAPSHOT
from opl.config import DEFAULT
from opl.vault.job_params import required_load_date, required_months, required_spec
from opl.vault.reference import load_reference_table
from opl.vault.registry import ReferenceTable


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    spec = required_spec(
        args[0] if args else "", ReferenceTable, loader="vault_load_reference"
    )
    source = bronze_table_spec(args[1] if len(args) > 1 else "")
    # THIS LOADER IS MONTHLY, AND A NON-MONTHLY SOURCE IS REFUSED RATHER THAN DEFAULTED.
    # `load_reference_table` takes no `axis`: it folds on `_snapshot_month`
    # throughout. Nothing in the
    # deployed wiring pairs it with a non-monthly source, but nothing in THIS file stopped
    # one either -- `bronze_table_spec` accepts any registered table. Every bronze row
    # carries `_snapshot_month` whatever its axis (`autoloader.add_common_audit_columns`),
    # so the failure would not raise: the window would validate, the fold would silently
    # use the wrong axis, and the load would report success. Refused by default here rather
    # than admitted by an `else`, which is `fetch_ptax._refuse_a_table_this_does_not_fetch`'s
    # discipline applied to the axis instead of the landing mode.
    if source.snapshot_axis != MONTHLY_SNAPSHOT:
        raise ValueError(
            f"refusing to load {spec.name}: {source.name} declares the "
            f"{source.snapshot_axis.name} snapshot axis, and `load_reference_table` folds on "
            f"{MONTHLY_SNAPSHOT.column} throughout. A loader that cannot honour an axis "
            f"must refuse the source rather than quietly read the wrong column"
        )
    months = required_months(args[2] if len(args) > 2 else "", action=f"load {spec.name}")
    load_date = required_load_date(args[3] if len(args) > 3 else "")
    spark = SparkSession.builder.getOrCreate()
    result = load_reference_table(
        spark,
        spec,
        source_table=DEFAULT.table(source.bronze),
        target_table=DEFAULT.table(spec.name),
        load_date=load_date,
        months=list(months),
    )
    # The `lookup_type` is in the line because it is the coordinate that decides which
    # of the six types' rows this table got, and it is the one thing a reader cannot
    # infer from the table name without opening the domain module.
    print(
        f"vault_load_reference: {result.table} +{result.appended} rows from "
        f"{DEFAULT.table(source.bronze)} over {list(months)}, routed on lookup_type "
        f"{spec.lookup_type!r}; the target already held {result.already_present} rows "
        f"(whole table, not this window); {result.collapsed_duplicates} source rows "
        "shared a (codigo, month) with another and were folded into it"
    )


if __name__ == "__main__":
    main()
