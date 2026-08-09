# databricks/src/vault_load_satellite.py
"""Job task: load one DV2 descriptive satellite from one bronze table, over one month
window.

THE PARENT HUB IS RESOLVED FROM THE REGISTRY AND NOT FROM A PARAMETER.
`opl.vault.satellites._refuse_a_mismatched_hub` exists because the loader takes the
satellite and the hub as two free arguments -- so that it can be tested against a
throwaway spec -- and its message names the fix: "resolve the parent with
opl.vault.domains.parent_hub rather than passing a hub by hand". This task does that.
A hub as a second job parameter would be a second chance for a copied YAML to pair a
satellite with another hub's digest, which joins to nothing and reports success.

THE OBSERVATION GRAIN IS BUILT HERE, FROM THE TWO SPECS ALREADY RESOLVED, AND THAT IS
A DERIVATION RATHER THAN A SECOND DECISION. `domains/cnpj.py` declares
`EMPRESA_GRAIN` and `ESTABELECIMENTO_GRAIN` with exactly these four values -- the hub's
name, the bronze table, its quarantine, and `<hub>.business_key_columns` -- and there
is no way for a job to reach a domain's module-level constant by name without a mapping
that would itself be the second spelling. So the same constructor is called over the
same two registry entries, and `tests/test_vault_job_wiring.py::test_the_grain_this_task
_builds_is_the_grain_the_domain_declares` asserts the two are equal for every satellite
this vault has. The loader's own `_refuse_a_mismatched_grain` covers the runtime half:
the grain must read the table being loaded and be keyed on the hub's own columns, in
its order.

WHAT A ONE-MONTH WINDOW MEANS HERE, because the effectivity satellite refuses one and
this one does not. `SatelliteLoadResult.candidate_departures` comes from the ledger, and
a ledger over a single month can report NO absence at all -- its key universe is
derived from that same month's presence, so every key in it is present in it. The
satellite's own ROWS are still exactly right for a one-month window, which is why this
is not a refusal; what would be wrong is reading `candidate_departures = 0` as "nothing
departed". The line printed below states which of the two it is rather than leaving the
zero to be interpreted.

argv: [table, source, months, load_date]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.registry import BronzeTable
from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT
from opl.vault import domains
from opl.vault.job_params import required_load_date, required_months, required_spec
from opl.vault.observation import ObservationGrain
from opl.vault.registry import Hub, Satellite
from opl.vault.satellites import load_satellite

# Below this many months in the window, the ledger cannot report an absence at all --
# see the module docstring, and `vault_load_effectivity.py`, which refuses rather than
# annotates because absence is that table's whole output.
MONTHS_AN_ABSENCE_NEEDS = 2


def grain_for(hub: Hub, source: BronzeTable) -> ObservationGrain:
    """The observation grain for `hub` over `source`'s bronze/quarantine pair.

    Keyed on the RAW business-key columns in the HUB'S ORDER, which is what
    `opl.vault.satellites._grain_key_mismatch` requires: the two declarations must be
    one list rather than two sets, because the hub's order is load-bearing (the hash
    concatenates in it) and anything later pairing the grain's columns with the hub's
    positionally would pair the wrong two."""
    return ObservationGrain.in_default_schema(
        name=hub.name,
        bronze=source.bronze,
        quarantine=source.quarantine,
        key_columns=hub.business_key_columns,
    )


def _departure_note(months: tuple[str, ...], departures: int) -> str:
    """What the departure count can and cannot support for this window."""
    if len(months) < MONTHS_AN_ABSENCE_NEEDS:
        return (
            f"{departures} candidate departures, which is ZERO BY CONSTRUCTION over a "
            "one-month window -- the ledger's key universe is that month's own keys, so "
            "no key in it can be absent from it. Not evidence that nothing departed"
        )
    return f"{departures} candidate departures (absent_after_observation, never asserted)"


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    spec = required_spec(args[0] if args else "", Satellite, loader="vault_load_satellite")
    source = bronze_table_spec(args[1] if len(args) > 1 else "")
    months = required_months(args[2] if len(args) > 2 else "", action=f"load {spec.name}")
    load_date = required_load_date(args[3] if len(args) > 3 else "")
    hub = domains.parent_hub(spec)
    spark = SparkSession.builder.getOrCreate()
    result = load_satellite(
        spark,
        spec,
        hub=hub,
        source_table=DEFAULT.table(source.bronze),
        target_table=DEFAULT.table(spec.name),
        load_date=load_date,
        grain=grain_for(hub, source),
        months=list(months),
    )
    departures = _departure_note(months, result.candidate_departures)
    print(
        f"vault_load_satellite: {result.table} +{result.appended} rows from "
        f"{DEFAULT.table(source.bronze)} over {list(months)}, keyed on {hub.name}; the "
        f"target already held {result.already_present} rows (whole table, not this "
        f"window); {result.collapsed_duplicates} source rows were folded into a row "
        f"sharing their (hash key, applied_date); {departures}"
    )


if __name__ == "__main__":
    main()
