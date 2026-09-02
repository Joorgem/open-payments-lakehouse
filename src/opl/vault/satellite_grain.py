# src/opl/vault/satellite_grain.py
"""WHETHER A DESCRIPTIVE SATELLITE IS GATED ON AN OBSERVATION LEDGER, which axis its
window reads, and whether the grain it was handed describes this load.

WHY ITS OWN MODULE. `opl.vault.satellites` held all of this inline through F-DB and it
was the right place while the answer was "always, and off the grain". F2 wave 2 made the
grain OPTIONAL -- `opl.vault.specs.Satellite.transactional` declares a satellite over an
EVENT stream, which has no window to close -- and the cost of that optionality is
`snapshot_axis_for`'s six refusals plus a replacement for the one guard a ledgerless
load would otherwise lose.

MEASURED AND COUNTERFACTUAL, KEPT APART, WHICH THIS PARAGRAPH GOT WRONG ONCE. It read
"took `satellites.py` from 548 to 886": the 548 is real -- `git show
cae3eff:src/opl/vault/satellites.py | wc -l` -- and the 886 is a length no file ever had,
because the unsplit version was never written and so nobody can check it. What is
checkable is the baseline and the cap: 548 lines against a strictly-under-800 cap, with
this module's subject growing by more than the 252 that were left. Master protocol
section 4.12 is that whoever touches a file at the cap splits it FIRST, and
`opl.vault.specs` and `opl.vault.registry_satellites` record the same crossing handled
the same way.

THE SEAM IS A SUBJECT AND NOT AN ARITHMETIC CUT, which is what makes this a boundary.
Everything here answers ONE question -- "which ledger, if any, gates this load, and on
which column does its window read" -- and every function here takes a grain, an axis or
both. Nothing here builds a candidate row, hashes anything, or writes. `satellites.py`
keeps the loader: the result object, the parent resolution, the candidates, the
diagnostics and the append.

THE SIX FUNCTIONS SPLIT THREE WAYS AND THE SPLIT IS THE MODULE'S SHAPE. Two of them are
PUBLIC because `load_satellite` calls them directly -- `snapshot_axis_for`, which decides
and refuses, and `refuse_a_window_the_source_never_loaded`, which is the ledgerless
load's replacement for `observation._window`'s refusal. Three are the HUB-grain
comparison and are reachable only through the first, which is what keeps
`_refuse_a_prefixed_hub_grain` firing on every hub without needing a parent-kind branch
of its own: a link-parented satellite is refused before it gets here. The sixth is
`_transactional_axis`, the ledgerless half of the decision, and it was left out of this
count when the module was written -- the correction round that found it is the same one
that found the refusal missing from `snapshot_axis_for`, which is what a count in prose
is worth when nothing measures it.

NOTHING IN THIS MODULE MOVED IN CONTENT. `_grain_key_mismatch`, `_refuse_a_mismatched_
grain` and `_refuse_a_prefixed_hub_grain` are F2 wave 1's and F-DB's, verbatim except for
one paragraph added to the last of them saying why it does not need a link branch."""
from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import SparkSession

from opl.vault.loading import SnapshotAxis
from opl.vault.months import refuse_unloaded_months
from opl.vault.observation import ObservationGrain
from opl.vault.registry import Hub, Link, Satellite


def _grain_key_mismatch(hub: Hub, grain: ObservationGrain) -> str | None:
    """Why `grain`'s key columns are not `hub`'s, or None if they are.

    TWO DIFFERENT MISTAKES, TWO MESSAGES, which is the point of this function: one
    comparison told the reordered case that its ledger was "coarser or finer", which is
    FALSE and sends someone looking for a bug in their column list.

    ORDER IS PART OF THE MATCH, AND THAT IS A DECISION TAKEN HERE. `hub_estabelecimento`
    is the vault's first multi-column key, so "is (`cnpj_dv`, `cnpj_ordem`,
    `cnpj_basico`) the same grain as (`cnpj_basico`, `cnpj_ordem`, `cnpj_dv`)?" stops
    being theoretical. FOR THE LEDGER, YES -- and the argument has to concede that
    first, because the tempting justification for refusing is wrong: `groupBy` is
    order-insensitive, so a permuted grain returns the same states for the same keys and
    miscounts nothing. This branch refuses something that would have answered correctly.

    WHAT IT BUYS IS THAT THE TWO DECLARATIONS ARE ONE LIST, not merely one set. The
    hub's order IS load-bearing (`hash_key_expression` concatenates in it, so a permuted
    hub is a re-keyed hub), and a domain writes `key_columns=<hub>.business_key_columns`
    so that there is one order in the file rather than two. This check keeps that the
    only spelling that passes. Accept a permutation and anything that later pairs the
    two POSITIONALLY -- a join built by zipping them, a message printing one against the
    other -- pairs `cnpj_basico` with `cnpj_dv` with nothing failing. Set equality is
    the weaker claim and buys only the right to write the columns in an order no domain
    should want. The cost is a refusal of a correct configuration, so the message names
    the one-line fix."""
    declared, expected = tuple(grain.key_columns), hub.business_key_columns
    if set(declared) != set(expected):
        return (
            f"the observation grain is keyed on {declared} and hub {hub.name!r} on "
            f"{expected}. The ledger would count departures at a different grain than "
            "the satellite records change at -- coarser and it misses departures, "
            "finer and it invents them"
        )
    if declared != expected:
        return (
            f"the observation grain is keyed on {declared} and hub {hub.name!r} on "
            f"{expected} -- the same columns in a different order. The LEDGER would "
            "answer the same, because groupBy does not care; this is refused so that "
            "the two declarations stay one list rather than two sets. The hub's order "
            "IS load-bearing (the hash concatenates in it), and anything that later "
            "pairs the grain's columns with the hub's positionally would pair the "
            "wrong two. Build the grain with key_columns=<the hub spec>."
            "business_key_columns rather than restating the columns"
        )
    return None


def _transactional_axis(
    satellite: Satellite, grain: ObservationGrain | None, axis: SnapshotAxis | None
) -> SnapshotAxis:
    """The axis a TRANSACTIONAL satellite's window reads, or refuse.

    A transactional satellite records events, so there is no window to close and no key
    that can meaningfully be `absent_after_observation` -- `opl.vault.specs.Satellite`
    carries the measurement. It therefore takes NO grain, and being handed one is refused
    rather than ignored: an ignored grain is an argument a caller believes took effect.

    AND THE AXIS BECOMES A PARAMETER ONLY HERE, WHERE THE GRAIN IS NOT ONE.
    `load_satellite`'s own comment block says an `axis=` argument would be "a second
    spelling of one decision, whose disagreement would land as a window that silently
    selected nothing" -- true while a grain was always there to read it off. With no grain
    nothing else spells it, so this is the ONLY spelling rather than a second, and its
    absence is refused rather than defaulted: the default would be `MONTHLY_SNAPSHOT`,
    right for every source registered today and silently wrong for the first one observed
    at instants."""
    if grain is not None:
        raise ValueError(
            f"satellite {satellite.name!r} declares transactional=True and was handed "
            f"observation grain {grain.name!r}. A transactional satellite records "
            "EVENTS: every key of every earlier month is absent from this one by "
            "construction, so the ledger would report a candidate delete per event. "
            "Pass axis=<the BronzeTable's snapshot_axis> instead"
        )
    if axis is None:
        raise ValueError(
            f"satellite {satellite.name!r} declares transactional=True and was handed "
            "neither a grain nor an axis. The window still has to be applied to a "
            "column, and with no grain to read it off there is nothing else that "
            "names one -- pass axis=<the BronzeTable's snapshot_axis>"
        )
    return axis


# WHY `snapshot_axis_for`'S ARGUMENT IS HERE. Inside the docstring it puts the function
# past the `< 50 lines INCLUDING comments` cap (master protocol section 4.9);
# `opl.bronze.rules`'s "WHY THE RULE SETS BELOW ARE ORDERED THE WAY THEY ARE" block and
# `opl.vault.satellites`'s "WHY `load_satellite`'S ARGUMENT PROSE IS HERE" block are the
# two precedents for moving
# prose out rather than cutting it. Nothing below is dropped.
#
# THE GRAIN IS OPTIONAL SINCE F2 WAVE 2 AND THE OPTIONALITY IS DECLARED, NOT INFERRED --
# `_transactional_axis` above is the half that takes none. It is refused in BOTH
# directions, which is what keeps this from being a way to switch a ledger off.
#
# BOTH PARENT-KIND PAIRINGS ARE REFUSED HERE, AND ONLY ONE OF THEM WAS WRITTEN THE FIRST
# TIME -- which is the defect this pair of branches exists to record as well as to
# prevent. The link half was here from the start: `build_registry` refuses a
# non-transactional satellite on a LINK at import, but the parent arrives as a free
# argument precisely so a throwaway spec can reach the loader, and the alternative to
# that branch is `_grain_key_mismatch` reading `business_key_columns` off a `Link`, an
# `AttributeError` several frames from the declaration that caused it.
#
# THE HUB HALF WAS MISSING AND THE FLAG'S OWN CLAIM DEPENDED ON IT. `opl.vault.registry_
# satellites._refuse_a_transactionality_the_parent_does_not_support` refuses BOTH
# pairings, and this module's claim was that "the flag cannot become a switch". That was
# true of the registry and false of the loader: `transactional=True` with a HUB parent
# branched into `_transactional_axis`, which does not take `parent` at all, so the ledger,
# `candidate_departures` and `_refuse_a_mismatched_grain` all vanished and the load
# reported success. The REVIEW demonstrated it by construction on this project's own
# fixture, counting 3 rows written with `ledger_derived=False`; what the correction
# measured for itself is the same thing from the other side -- delete this branch and
# `test_an_event_satellite_on_a_hub_is_refused_at_the_loader_and_not_only_at_import`
# fails with `DID NOT RAISE`, meaning the load ran to completion.
#
# NO PRODUCTION PATH REACHES EITHER BRANCH TODAY, AND THAT IS STATED RATHER THAN IMPLIED.
# `satellites._resolved_parent` pins the parent by NAME against the satellite's own
# declaration, and `build_registry` refuses both pairings at import, so no registered spec
# can arrive here mispaired. The reason to guard anyway is the reason `load_satellite`
# takes free arguments in the first place: it is a public function whose parent, grain and
# axis are all caller-supplied, and the guard beside this one exists on exactly that
# argument. A refusal that only a throwaway spec can trip is still the refusal that says
# what the loader will not do.
def snapshot_axis_for(
    satellite: Satellite,
    parent: Hub | Link,
    grain: ObservationGrain | None,
    axis: SnapshotAxis | None,
    source_table: str,
) -> SnapshotAxis:
    """The axis this load reads its window on, having refused every disagreement between
    `Satellite.transactional`, the parent's kind, the grain and the axis. Six refusals,
    argued in the comment block above this function."""
    if satellite.transactional and isinstance(parent, Hub):
        raise ValueError(
            f"satellite {satellite.name!r} declares transactional=True and hangs off "
            f"hub {parent.name!r}, so this load would derive no ledger at all -- no "
            "candidate_departures, and no refusal of a window month nothing loaded. "
            "build_registry refuses the pairing at import, so a REGISTERED spec cannot "
            "reach here and only a throwaway one can; argue it in registry_satellites."
            "_refuse_a_transactionality_the_parent_does_not_support if a hub over an "
            "event stream ever arrives"
        )
    if satellite.transactional:
        return _transactional_axis(satellite, grain, axis)
    if grain is None:
        raise ValueError(
            f"satellite {satellite.name!r} does not declare transactional=True and was "
            "handed no observation grain. The ledger is what routes `months` through "
            "observation._window's refusal of a month with no row on either side, and "
            "what supplies candidate_departures; a state satellite loaded without it "
            "would report success over a window that selected nothing"
        )
    if axis is not None:
        raise ValueError(
            f"satellite {satellite.name!r} was handed both an observation grain and "
            f"axis={axis.name!r}. The grain already carries the source's axis and has "
            "been pinned to this source table, so a second one is a second spelling of "
            "one decision -- and the disagreement lands as a window that silently "
            "selected nothing"
        )
    if not isinstance(parent, Hub):
        raise ValueError(
            f"satellite {satellite.name!r} hangs off link {parent.name!r} and is not "
            "transactional, so its ledger would have to be keyed on the LINK's identity "
            "columns and read through the link's own prefixes. The comparison that checks "
            "that is opl.vault.effectivity._refuse_a_mismatched_link_grain and this "
            "loader does not call it; build_registry refuses the pairing at import, so a "
            "registered spec cannot reach here"
        )
    _refuse_a_mismatched_grain(parent, grain, source_table)
    return grain.snapshot_axis


def refuse_a_window_the_source_never_loaded(
    spark: SparkSession, source_table: str, months: Sequence[str] | None, axis: SnapshotAxis
) -> None:
    """The guard a transactional satellite would otherwise lose with its ledger.

    `observation._window` refuses a month with no row in bronze OR quarantine, and
    `load_satellite`'s docstring calls that one of the two real things consulting the
    ledger buys. A transactional satellite derives no ledger, so without this the guard
    would simply be gone for it: `months=['2026-09']` would select no row, write nothing,
    and report success -- which is the failure `opl.vault.loading._validated_months` says
    this layer is least able to notice, because "a vault table that gained no rows looks
    exactly like a vault table that had nothing to gain".

    ONE TABLE AND NOT TWO, WHICH IS THE ONLY DIFFERENCE FROM THE LEDGER'S VERSION. There
    is no quarantine side here: the ledger consults one because being REJECTED is evidence
    the source published a key, and this loader is not asking about keys at all -- it is
    asking whether the window it was handed selects anything to load. The RULE is shared
    (`opl.vault.months.refuse_unloaded_months`) so the two cannot drift; the CONSEQUENCE
    differs, which is why that function takes one.

    EAGER, AND ONLY FOR A CALLER WHO NAMES MONTHS -- `_window`'s cost model exactly. With
    `months=None` the window IS whatever the source holds, so it cannot name a month the
    source has not seen and nothing is collected."""
    if months is None:
        return
    loaded = {
        row[axis.column]
        for row in spark.read.table(source_table).select(axis.column).distinct().collect()
    }
    refuse_unloaded_months(
        months,
        loaded=loaded,
        tables=(source_table,),
        consequence=(
            "the window would select no row at all, so this load would write nothing and "
            "report success -- a vault table that gained no rows looks exactly like a "
            "vault table that had nothing to gain. Name a month this source carries"
        ),
    )


def _refuse_a_mismatched_grain(
    hub: Hub, grain: ObservationGrain, source_table: str
) -> None:
    """The grain arrives as a third free argument and must describe the SAME rows the
    satellite is loading.

    `satellites._resolved_parent` exists because two independently-passed arguments can
    disagree; the grain has that hazard twice over, and worse, because it is the one
    argument whose mistakes are invisible in the output. It drives two things: the
    departure count, and `_window`'s refusal of a month with no row on either side --
    and `_window` reads `grain.bronze_table`, NOT `source_table`. A grain pointing at
    estabelecimentos would let `months=['2026-09']` pass or fail against the wrong
    table, and would report a departure count for a different key space, with the
    satellite's own rows perfectly correct beside it.

    TWO CHECKS, AND THE NAME IS DELIBERATELY NOT ONE OF THEM. The review suggested
    `grain.name == hub.name`, which `domains/cnpj.py` does satisfy. It is the weaker
    claim: a name is a label, so two grains can share one while reading different
    tables, and it is precisely the table and the key space that the two failures above
    are about. Checking what the ledger actually READS covers both, and covers them
    whether or not a future domain follows the naming convention.

    The key-space half is `_grain_key_mismatch`, which is where the order decision the
    first multi-column key forced is argued, and the third check is
    `_refuse_a_prefixed_hub_grain`."""
    if grain.bronze_table != source_table:
        raise ValueError(
            f"the observation grain reads {grain.bronze_table!r} and the satellite is "
            f"being loaded from {source_table!r}. The ledger would describe a "
            "different table than the one written: its departure count would be about "
            "another key space, and its refusal of an unloaded month would be checked "
            "against another table's months. Pass the grain built for this source"
        )
    _refuse_a_prefixed_hub_grain(hub, grain)
    mismatch = _grain_key_mismatch(hub, grain)
    if mismatch is not None:
        raise ValueError(mismatch)


def _refuse_a_prefixed_hub_grain(hub: Hub, grain: ObservationGrain) -> None:
    """A hub grain may not be read through a `KeyPrefix`.

    A FLAT REFUSAL RATHER THAN A COMPARISON, WHICH IS WHY IT IS NOT PART OF
    `_grain_key_mismatch`. F-DB Task 5's correction pass gave `ObservationGrain` a
    derivation -- the thing that makes `link_merchant_empresa`'s ledger key on the eight
    characters its digest is over rather than on the fourteen bronze holds. A HUB has no
    such thing: its business key is read from the columns it is NAMED after
    (`loading._padded_components` is the whole of it), so there is no declaration to
    compare a prefix against.

    AND THE MISTAKE POINTS THE OPPOSITE WAY FROM THE LINK'S. A missing prefix made that
    ledger FINER than the link; a prefix here makes this one COARSER than the hub -- one
    ledger key spanning several hub keys -- so a departure is reported only when the last
    of them leaves, and the count is small, plausible and about a key space this
    satellite wrote no row for.

    IT STILL FIRES ON EVERY HUB PARENT AFTER F2 WAVE 2, AND IT IS NOT EXEMPTED FOR A LINK
    ONE -- which is the distinction that keeps this from being a refusal that quietly
    stopped applying. A LINK parent legitimately has prefixes (`link_payment`'s two ends
    are both derived at width 8), so a rule reading "no grain here may carry one" would
    have had to grow a parent-kind branch, and a branch is where a refusal goes to stop
    firing. Instead a link-parented satellite never reaches a grain at all:
    `snapshot_axis_for` refuses it above, by name, before this is called. The function's
    subject is unchanged -- it is called only with a `Hub` and refuses every prefix."""
    if not grain.key_prefixes:
        return
    raise ValueError(
        f"the observation grain declares key prefixes {tuple(grain.key_prefixes)} and "
        f"hub {hub.name!r} reads its business key from the columns it is named after. A "
        "prefix would key the ledger on a truncation of a hub key -- COARSER than the "
        "hub, so several hub keys share one ledger key and a departure is reported only "
        "when the last of them leaves. Prefixes belong to a LINK end that declares one; "
        "pass the grain built for this hub"
    )
