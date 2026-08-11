# databricks/src/vault_load_partner_link.py
"""Job task: load `link_company_partner` -- the one link whose partner end is DERIVED
from a source column rather than read from a column named after the hub.

A SECOND ENTRY POINT BECAUSE THERE IS A SECOND LOADER, AND THE LOADER IS SECOND FOR A
REASON THIS TASK CANNOT PAPER OVER. `opl.vault.links.load_link` writes one hash-key
reference per hub, computed from the columns that hub's business key is NAMED after.
The partner company's `cnpj_basico` is not a column of socios at all -- it is the first
eight characters of `cpf_cnpj_socio`, and only when `identificador_socio` says the
partner is a company. `load_link` therefore REFUSES this link outright, and the refusal
is what stops the plausible failure: both ends hashed from `cnpj_basico`, every
relationship reading as a company partnered with itself, right row count, working
joins, nonsense. Pointing `vault_load_link.py` at `link_company_partner` in a copied job
YAML hits that refusal rather than that table.

STILL PARAMETERISED BY (VAULT TABLE, BRONZE SOURCE), like every other vault task here,
even though `opl.vault.partners` is the one loader in the package that knows a source's
own shape. The alternative -- literals in this file -- would put a bronze table name in
`databricks/src`, which is the thing `tests/test_task_wiring.py` refuses for every
bronze task and for the documented reason: a coordinate spelled twice is a coordinate
that can drift. What keeps this task honest is the loader's own
`_refuse_a_link_this_derivation_does_not_fit`, which checks that the link's ends, its
dependent-child keys and both hubs' widths are the ones this derivation reads.

argv: [table, source, months, load_date]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT
from opl.vault import domains
from opl.vault.job_params import required_load_date, required_months, required_spec
from opl.vault.partners import load_partner_link
from opl.vault.registry import Link


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    spec = required_spec(args[0] if args else "", Link, loader="vault_load_partner_link")
    source = bronze_table_spec(args[1] if len(args) > 1 else "")
    months = required_months(args[2] if len(args) > 2 else "", action=f"load {spec.name}")
    load_date = required_load_date(args[3] if len(args) > 3 else "")
    spark = SparkSession.builder.getOrCreate()
    hubs = domains.linked_hubs(spec)
    result = load_partner_link(
        spark,
        spec,
        hubs=hubs,
        # THE ARGUMENT THIS JOB EXISTS TO GET RIGHT. `vault_partner_job.yml` does not
        # load `hub_empresa`, which BOTH ends of this link reference -- that is the
        # empresa job's task, and no `depends_on` reaches across two jobs. Handing the
        # loader where the hub lives is what lets it refuse the wrong-order run instead
        # of appending 28M references to rows that are not there.
        hub_tables={hub.name: DEFAULT.table(hub.name) for hub in hubs},
        source_table=DEFAULT.table(source.bronze),
        target_table=DEFAULT.table(spec.name),
        load_date=load_date,
        months=list(months),
    )
    # `collapsed_duplicates` is printed rather than left in the result object because
    # the whole argument for folding a link's duplicates rests on them being few and
    # payload-free -- 4,329 of 2026-07's 27,990,592 rows, measured -- so the load that
    # folds them should be the load that says how many.
    print(
        f"vault_load_partner_link: {result.table} +{result.appended} rows from "
        f"{DEFAULT.table(source.bronze)} over {list(months)}; the target already held "
        f"{result.already_present} rows (whole table, not this window); "
        f"{result.collapsed_duplicates} source rows shared a (link key, month) with "
        "another and were folded into it"
    )


if __name__ == "__main__":
    main()
