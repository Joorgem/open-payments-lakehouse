# databricks/src/vault_load_satellite.py
"""Job task: load one DV2 descriptive satellite from one bronze table, over one month
window.

THE PARENT IS RESOLVED FROM THE REGISTRY AND NOT FROM A PARAMETER, AND SINCE F2 WAVE 2
IT MAY BE A HUB OR A LINK. `opl.vault.satellites._resolved_parent` exists because the
loader takes the satellite and its parent as free arguments -- so that it can be tested
against a throwaway spec -- and its message names the fix: "resolve the parent with
opl.vault.domains.parent_of rather than passing one by hand". This task does that. A
parent as a second job parameter would be a second chance for a copied YAML to pair a
satellite with another table's digest, which joins to nothing and reports success.

THE KIND OF THE PARENT, NOT THE KIND OF THE SATELLITE, IS WHAT PICKS THIS TASK'S
ARGUMENTS -- and that distinction had no consequence until `sat_link_payment`. Every
`Satellite` this repository had hung off a HUB and every `EffectivitySatellite` off a
LINK, so routing by kind and routing by parent returned the same answer for all six
tables, and neither this file nor `vault_load_effectivity.py` ever had to say which of
the two it meant. `sat_link_payment` is a `Satellite` whose parent is a LINK: the first
table where the two rules disagree, and this file was written on the side that is wrong
for it, because `domains.parent_hub` refuses a link-parented satellite BY NAME.
`parent_arguments` below is the one place the resolved parent's kind is read.

WHY THIS SCRIPT GREW A SECOND PARENT KIND RATHER THAN A THIRD SCRIPT BEING WRITTEN.
`opl.vault.satellites.load_satellite` already accepts both parents, so a third entry
point would duplicate the parameter parsing, the source resolution and the window
validation in order to vary two arguments. `vault_load_effectivity.py` is a separate
script for a reason that does not transfer: it calls a DIFFERENT function
(`load_effectivity_satellite`), whose output is derived from ABSENCE and which closes
windows. `sat_link_payment` calls the same `load_satellite` as the other four
satellites. A THIRD ARGUMENT WAS WITHDRAWN RATHER THAN KEPT, and it is named here so
nobody restores it from memory: an earlier draft cited `opl.vault.registry` as ruling
that a new KIND earns its own module while a widened capability edits the mechanism,
and the citation did not survive being read against that module. The two reasons above
carry the decision on their own; none of it rests on the withdrawn one.

THE OBSERVATION GRAIN IS BUILT HERE, FROM THE TWO SPECS ALREADY RESOLVED, AND THAT IS
A DERIVATION RATHER THAN A SECOND DECISION. `domains/cnpj.py` declares
`EMPRESA_GRAIN` and `ESTABELECIMENTO_GRAIN` with exactly these four values -- the hub's
name, the bronze table, its quarantine, and `<hub>.business_key_columns` -- and there
is no way for a job to reach a domain's module-level constant by name without a mapping
that would itself be the second spelling. So the same constructor is called over the
same two registry entries, and `tests/test_vault_job_wiring.py::test_the_grain_this_task
_builds_is_the_grain_the_domain_declares` asserts the two are equal for every satellite
a job YAML loads through this script -- which is every HUB-parented one, since a
transactional satellite takes no grain and that sweep skips it on `spec.transactional`
rather than by name.
`opl.vault.satellite_grain._refuse_a_mismatched_grain` covers the runtime half:
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

AND A TRANSACTIONAL SATELLITE HAS NO LEDGER AT ALL, so there is no departure count to
qualify: `SatelliteLoadResult.ledger_derived` is False, `candidate_departures` is `None`
at every setting of the flag, and `_departure_note` says so instead of rendering that
`None` into a sentence about a window this load never opened.

`report_diagnostics` IS A JOB PARAMETER AND IT DEFAULTS TO `false`, WHICH IS THE ONLY
DEFAULT IN THIS FILE THAT IS NOT A REFUSAL. `months` defaults to a sentinel the task
rejects because no window is a safe one; this flag's default is safe by construction --
off, the loader measures neither `collapsed_duplicates` nor `candidate_departures`,
reports both as `None`, and skips two full passes over the source that the first real
run spent most of 5,635 s on. ON A TRANSACTIONAL SATELLITE THE FLAG BUYS ONE PASS AND
NOT TWO, because there is no ledger for the second one to read. See
`opl.vault.satellites`.

SO THIS TASK PRINTS THREE DIFFERENT SENTENCES AND NOT TWO, and that is load-bearing
rather than cosmetic. `0 candidate departures` is a MEASUREMENT, published as evidence
that a path is unexercised; a run that skipped the measurement must print nothing a
reader could count as that zero. AND THE SKIP ITSELF HAS TWO SHAPES. On a ledger-bearing
satellite both counts were skipped and both are had by re-running with the flag on; on a
TRANSACTIONAL one only the fold count was skipped, and the departure count does not
exist at any setting -- so the ledger-bearing skip's own instruction, printed on a
transactional load, would promise an operator two counts, deliver one, and answer the
other with "NO departure count, because this satellite is TRANSACTIONAL". That is the
sentence this file exists to keep out of a task log, so the default arm branches too.
`_diagnostics_note` is where the three are kept apart.

argv: [table, source, months, load_date, report_diagnostics]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.registry import BronzeTable
from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT
from opl.vault import domains
from opl.vault.job_params import (
    optional_flag,
    required_load_date,
    required_months,
    required_spec,
)
from opl.vault.observation import ObservationGrain
from opl.vault.registry import Hub, Link, Satellite
from opl.vault.satellites import SatelliteLoadResult, load_satellite

# Below this many months in the window, the ledger cannot report an absence at all --
# see the module docstring, and `vault_load_effectivity.py`, which refuses rather than
# annotates because absence is that table's whole output.
MONTHS_AN_ABSENCE_NEEDS = 2

# THE JOB PARAMETER'S NAME, SPELLED ONCE. The YAMLs declare it, the launch command sets
# it, the refusal message names it and `tests/test_vault_job_wiring.py` reads it from
# here rather than restating it -- so a rename is one edit and a YAML left behind is a
# red test instead of a flag that is passed to nothing.
DIAGNOSTICS_PARAMETER = "report_diagnostics"


def grain_for(hub: Hub, source: BronzeTable) -> ObservationGrain:
    """The observation grain for `hub` over `source`'s bronze/quarantine pair.

    A HUB PARENT AND NOTHING ELSE. A satellite on a LINK is transactional, has no window
    to close, takes no grain at all and reads its window off `axis=` instead -- see
    `parent_arguments` below, which is where the two are kept apart.

    Keyed on the RAW business-key columns in the HUB'S ORDER, which is what
    `opl.vault.satellite_grain._grain_key_mismatch` requires: the two declarations must be
    one list rather than two sets, because the hub's order is load-bearing (the hash
    concatenates in it) and anything later pairing the grain's columns with the hub's
    positionally would pair the wrong two."""
    return ObservationGrain.in_default_schema(
        name=hub.name,
        bronze=source.bronze,
        quarantine=source.quarantine,
        key_columns=hub.business_key_columns,
        # THE AXIS IS THE SOURCE'S OWN DECLARATION, CARRIED RATHER THAN CHOSEN. This is
        # the derivation T7 put the field on `BronzeTable` for: the ledger reads
        # (business key, snapshot axis), the axis is a property of what was observed and
        # not of the grain a caller happens to want, and every grain this repository has
        # is built right here from a `(spec, BronzeTable)` pair. Declaring it on the
        # grain instead would mean deciding it at each of these two call sites, where a
        # source observed twice in one month could be paired with a monthly axis and
        # produce a ledger that folds both observations into one and reports the
        # departure as `observed`.
        snapshot_axis=source.snapshot_axis,
    )


def parent_arguments(parent: Hub | Link, source: BronzeTable) -> dict[str, object]:
    """The `load_satellite` arguments that depend on the PARENT'S KIND, and nothing else.

    ONE PLACE WHERE THE KIND IS READ, so that a satellite on a hub and a satellite on a
    link differ by these two branches rather than by two call sites that have to be kept
    in step. It is PUBLIC because `tests/test_vault_entry_points.py` DRIVES it: the lock
    that says every registered satellite reaches a loader accepting its parent's kind has
    to ask this script what it would pass, and a test that restated the pairing would
    stay green over a script that stopped agreeing with it.

    A HUB PARENT TAKES A GRAIN AND A LINK PARENT TAKES AN AXIS, and that pairing is
    `opl.vault.satellite_grain.snapshot_axis_for`'s rather than this file's invention. A
    transactional satellite has no window to close, is REFUSED if handed a grain, and has
    nothing else to read its axis off; a hub-parented one is REFUSED if handed an axis,
    because the grain already carries the source's own and a second spelling of one
    decision lands as a window that silently selected nothing. Passing both, or the wrong
    one, is refused there by name -- so the cost of getting this branch wrong is a
    `ValueError` before any row is written rather than a load that reports success.

    `hubs=` TRAVELS WITH `link=` AND NEVER WITH `hub=`. `link_hash_key_expression` needs
    each identifying end's hub to know its business-key order and widths, so a `Link`
    alone cannot be keyed on; `load_satellite` refuses `hub=` beside a non-empty `hubs=`
    for the mirror reason -- that pair means a caller resolved a link and passed the wrong
    half of it."""
    if isinstance(parent, Hub):
        return {"hub": parent, "grain": grain_for(parent, source)}
    return {
        "link": parent,
        "hubs": domains.linked_hubs(parent),
        "axis": source.snapshot_axis,
    }


def _departure_note(months: tuple[str, ...], result: SatelliteLoadResult) -> str:
    """What the departure count can and cannot support for this load.

    THREE STATES SINCE F2 WAVE 2 AND NOT TWO, and the third is the one that would
    otherwise print a false sentence. A TRANSACTIONAL satellite derives no observation
    ledger, so `candidate_departures` is `None` whatever the flag says -- and without the
    first arm the other two would render that `None` into a sentence about a ledger this
    load never built.

    WHICH false sentence depends on the window, so the counterfactual is stated with its
    conditions rather than quoted as if there were one. Over a single month the second
    arm gives "None candidate departures, which is ZERO BY CONSTRUCTION over a one-month
    window"; over two or more the third gives "None candidate departures
    (absent_after_observation, never asserted)". Both are false the same way, and only
    the first is additionally about a window this load never opened."""
    if not result.ledger_derived:
        return (
            "NO departure count, because this satellite is TRANSACTIONAL and derives no "
            "observation ledger at all -- an event does not depart, so there is no key "
            "that could reach absent_after_observation and no window to close. Not a "
            "zero, and not a skipped measurement either"
        )
    if len(months) < MONTHS_AN_ABSENCE_NEEDS:
        return (
            f"{result.candidate_departures} candidate departures, which is ZERO BY "
            "CONSTRUCTION over a one-month window -- the ledger's key universe is that "
            "month's own keys, so no key in it can be absent from it. Not evidence that "
            "nothing departed"
        )
    return (
        f"{result.candidate_departures} candidate departures "
        "(absent_after_observation, never asserted)"
    )


# THE SKIP HAS TWO SHAPES, WHICH IS WHERE F2 WAVE 2'S FIRST FIX LANDED ONE ARM SHORT.
# Module level for `grain_for`'s reason and this repository's standing one (see
# `opl.vault.satellite_grain.snapshot_axis_for`): inside the docstring it puts the
# function past the `< 50 lines INCLUDING comments` cap.
#
# `report_diagnostics` defaults to false, so the FIRST line an operator ever sees for
# `sat_link_payment` is the skip -- and the ledger-bearing skip says two counts were not
# measured, that EACH costs a pass, and to re-run with the flag on to measure THEM. For a
# transactional satellite one of the two does not exist at any setting, only one pass is
# at stake, and an operator who follows that instruction spends the pass and is then told
# "NO departure count, because this satellite is TRANSACTIONAL". The MEASURED arm had
# already been corrected for that; the DEFAULT arm is the one an unflagged run actually
# prints, and it is corrected here.
#
# `_departure_note` IS CALLED FROM BOTH ARMS RATHER THAN RESTATED IN THIS ONE, so the
# three departure states have exactly one spelling and a later change to them cannot
# leave this branch behind.
def _diagnostics_note(months: tuple[str, ...], result: SatelliteLoadResult) -> str:
    """The two optional counts, or the fact that this run did not measure them.

    THE TWO STATES MUST NOT READ ALIKE, which is the whole reason this is a function and
    not an f-string. `0 source rows were folded` is a measurement, and it is published as
    evidence that the dedup tie-break is unexercised by real data; a load that skipped
    the measurement and printed a zero would put an unfalsifiable claim in a task log,
    where nobody re-derives anything. So the skip says what it skipped and why, and never
    prints a number. It also has TWO shapes, argued in the block above this function."""
    if result.collapsed_duplicates is None and not result.ledger_derived:
        return (
            f"THE FOLD COUNT WAS NOT MEASURED ({DIAGNOSTICS_PARAMETER}=false, the "
            "default), so this run reports none -- which is not it being zero. It is the "
            "ONLY count this flag buys on a transactional satellite, and it costs one "
            f"full extra pass over the source; re-run with --params ...,"
            f"{DIAGNOSTICS_PARAMETER}=true to measure it. "
            f"{_departure_note(months, result)}"
        )
    if result.collapsed_duplicates is None:
        return (
            f"NEITHER DIAGNOSTIC WAS MEASURED ({DIAGNOSTICS_PARAMETER}=false, the "
            "default), so this run reports no fold count and no departure count -- which "
            "is not either of them being zero. Each costs a full extra pass over the "
            f"source. Re-run with --params ...,{DIAGNOSTICS_PARAMETER}=true to measure "
            "them"
        )
    return (
        f"{result.collapsed_duplicates} source rows were folded into a row sharing "
        f"their (hash key, applied_date); "
        f"{_departure_note(months, result)}"
    )


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    spec = required_spec(args[0] if args else "", Satellite, loader="vault_load_satellite")
    source = bronze_table_spec(args[1] if len(args) > 1 else "")
    months = required_months(
        args[2] if len(args) > 2 else "",
        action=f"load {spec.name}",
        axis=source.snapshot_axis,
    )
    load_date = required_load_date(args[3] if len(args) > 3 else "")
    diagnostics = optional_flag(
        args[4] if len(args) > 4 else "", parameter=DIAGNOSTICS_PARAMETER
    )
    # RESOLVED AND PAIRED BEFORE THE SESSION, like every other argument refusal under
    # `databricks/src`: a satellite whose parent this loader cannot key on is a mistake in
    # a job YAML, and diagnosing it must not cost a serverless start.
    parent = domains.parent_of(spec)
    arguments = parent_arguments(parent, source)
    spark = SparkSession.builder.getOrCreate()
    result = load_satellite(
        spark,
        spec,
        **arguments,
        source_table=DEFAULT.table(source.bronze),
        target_table=DEFAULT.table(spec.name),
        load_date=load_date,
        months=list(months),
        report_diagnostics=diagnostics,
    )
    print(
        f"vault_load_satellite: {result.table} +{result.appended} rows from "
        f"{DEFAULT.table(source.bronze)} over {list(months)}, keyed on {parent.name}; the "
        f"target already held {result.already_present} rows (whole table, not this "
        f"window); {_diagnostics_note(months, result)}"
    )


if __name__ == "__main__":
    main()
