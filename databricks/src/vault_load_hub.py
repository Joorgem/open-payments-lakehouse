# databricks/src/vault_load_hub.py
"""Job task: load one DV2 hub from one bronze table, over one month window.

PARAMETERISED BY (VAULT TABLE, BRONZE SOURCE) RATHER THAN ONE FILE PER HUB, and the
second half of that pair is not decoration: `hub_empresa` has TWO feeds. Empresas is
the obvious one; estabelecimentos carries `cnpj_basico` as well, so
`load_hub(HUB_EMPRESA, source_table=ESTABELECIMENTOS_SOURCE)` is a legitimate load and
the hub's anti-join makes running both free (`opl/vault/domains/cnpj.py:245-250`). A
hub therefore does not determine its own source, and a task that resolved one from the
other would make the second feed unexpressible.

THE SOURCE IS A BRONZE REGISTRY KEY, NEVER A TABLE NAME. `opl.bronze.registry` keeps
each table's staging/bronze/quarantine triple in one literal precisely so a second
spelling cannot drift, and `opl.config.DEFAULT.table` is the one place a catalog and a
schema are spelled. A job YAML naming `workspace.default.bronze_cnpj_empresas` would be
both at once.

argv: [table, source, months, load_date]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT
from opl.vault.hubs import load_hub
from opl.vault.job_params import required_load_date, required_months, required_spec
from opl.vault.registry import Hub


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # The vault table first and the bronze source second, both resolved BEFORE the
    # window and the timestamp: a mistyped name on either side is refused by a registry
    # naming the valid alternatives, and neither refusal needs Spark.
    spec = required_spec(args[0] if args else "", Hub, loader="vault_load_hub")
    source = bronze_table_spec(args[1] if len(args) > 1 else "")
    months = required_months(args[2] if len(args) > 2 else "", action=f"load {spec.name}")
    load_date = required_load_date(args[3] if len(args) > 3 else "")
    spark = SparkSession.builder.getOrCreate()
    result = load_hub(
        spark,
        spec,
        source_table=DEFAULT.table(source.bronze),
        target_table=DEFAULT.table(spec.name),
        load_date=load_date,
        months=list(months),
    )
    # THE SUCCESS LINE MATTERS AS MUCH AS THE REFUSAL, per `assert_deployed_revision`:
    # a task log that does not name what ran is what made PR A's stale-bundle incident
    # a human's job to find. `already_present` is whole-table and not window-scoped
    # (`HubLoadResult`), and this line says so rather than letting a reader take it for
    # "how many of this window's keys were already here" -- which is the number the
    # second `hub_empresa` feed is expected to report as 69,062,849 against 0 appended.
    print(
        f"vault_load_hub: {result.table} +{result.appended} rows from "
        f"{DEFAULT.table(source.bronze)} over {list(months)}; the target already held "
        f"{result.already_present} rows (whole table, not this window)"
    )


if __name__ == "__main__":
    main()
