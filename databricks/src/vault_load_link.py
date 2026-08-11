# databricks/src/vault_load_link.py
"""Job task: load one DV2 link whose ends are all identifying and read from the source's
own columns, from one bronze table, over one month window.

THE HUBS ARE RESOLVED FROM THE REGISTRY, IN THE LINK'S DECLARATION ORDER, and the order
is the answer rather than a detail of it: `opl.vault.links.refuse_mismatched_hubs` says
that a right pair in the WRONG order gives a link whose two reference columns are
correct and whose own hash key is a digest over the business keys concatenated
backwards -- every row present, every join working, and the identity column disagreeing
with the one a re-load computes. `domains.linked_hubs` is the resolver that message
names, and it returns one entry per END, so a self-referencing link yields the same hub
twice.

THIS IS NOT THE PARTNER LINK'S ENTRY POINT. `opl.vault.links.load_link` refuses any
link carrying a dependent-child key or a non-identifying end, because it computes every
end's reference from the columns that hub is NAMED after -- so both ends of
`link_company_partner` would be hashed from `cnpj_basico` and every relationship would
read as a company partnered with itself, with the right row count and working joins.
`vault_load_partner_link.py` is the entry point that knows the derivation. Two entry
points because there are two loaders, and the loaders are two for the reason
`opl.vault.partners`' docstring gives.

argv: [table, source, months, load_date]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT
from opl.vault import domains
from opl.vault.job_params import required_load_date, required_months, required_spec
from opl.vault.links import load_link
from opl.vault.registry import Link


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    spec = required_spec(args[0] if args else "", Link, loader="vault_load_link")
    source = bronze_table_spec(args[1] if len(args) > 1 else "")
    months = required_months(args[2] if len(args) > 2 else "", action=f"load {spec.name}")
    load_date = required_load_date(args[3] if len(args) > 3 else "")
    spark = SparkSession.builder.getOrCreate()
    hubs = domains.linked_hubs(spec)
    result = load_link(
        spark,
        spec,
        hubs=hubs,
        # WHERE THE LOADER LOOKS FOR EACH HUB, and the qualification is this file's to
        # supply for `opl.vault.registry`'s reason: a spec carries an unqualified name
        # and `opl.config` is consulted by whatever calls a loader. Keyed by hub name, so
        # a self-referencing link contributes one entry rather than two that could differ.
        hub_tables={hub.name: DEFAULT.table(hub.name) for hub in hubs},
        source_table=DEFAULT.table(source.bronze),
        target_table=DEFAULT.table(spec.name),
        load_date=load_date,
        months=list(months),
    )
    print(
        f"vault_load_link: {result.table} +{result.appended} rows from "
        f"{DEFAULT.table(source.bronze)} over {list(months)}, joining "
        f"{list(spec.hub_names)}; the target already held {result.already_present} rows "
        "(whole table, not this window)"
    )


if __name__ == "__main__":
    main()
