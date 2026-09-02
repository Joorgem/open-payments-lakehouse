# tests/vault_job_demands.py
"""What a VAULT TABLE demands of a bronze source, and what a bronze source CARRIES. No
test lives here, and that absence is the point -- pytest collects nothing from a module
that matches no `python_files` pattern, so this file adds no collection-order dependency
to the suite.

THE THIRD FILE OF A SPLIT, AND IT EXISTS FOR THE REASON `tests/job_yaml.py` AND
`tests/task_ast.py` EXIST. `tests/test_vault_job_wiring.py` reached **799** lines against
a strictly-under-800 cap (`wc -l`, on the tree this split was made from), and master
protocol section 4.12 is that whoever touches a file at the cap splits it FIRST. F2 wave
2's correction round had to add one mutation probe to that file and had nowhere to put
it. `src/opl/vault/registry_satellites.py` and `src/opl/vault/satellite_grain.py` are the
same crossing handled the same way in the same task.

THE SEAM IS A SUBJECT AND NOT AN ARITHMETIC CUT, which is what makes this a boundary. Two
questions are asked when a job YAML pairs a vault table with a bronze table: WHAT WILL THE
LOADER DEMAND BY NAME, and WHAT DOES THAT SOURCE ACTUALLY HAVE. Both are answered entirely
out of the two registries -- `opl.vault.domains` and `opl.bronze.registry` -- and neither
reads a line of YAML. Everything that stayed in `test_vault_job_wiring.py` reads the
YAMLs: which jobs exist, which task runs which script, what a task is handed, and in what
order the tasks run. A helper here that opened a YAML would be on the wrong side of it.

NOT IMPORTED FROM THE TEST MODULE, WHICH WAS THE ALTERNATIVE. A second test file
importing `test_vault_job_wiring`'s privates would give this suite a collection-order
dependency it does not otherwise have, and `tests/job_yaml.py`'s docstring already
refused exactly that at this directory's level. (`tests/vault/` does import across test
modules, but those are a package with a shared fixture registry; this directory is not.)

WHAT IS HERE IS ONLY WHAT THE LOCKS ASK. The YAML readers are NOT here -- only the
wiring file resolves a job -- and neither is `_mutated`, which belongs to the probes that
drive the locks. A declaration in a file that does not use it is a declaration nobody
maintains."""
from __future__ import annotations

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.bronze.snapshot import SNAPSHOT_REF_DATE_COLUMN
from opl.contracts.catalogue import columns_for
from opl.dataops.cadence import declares_source_date
from opl.vault import domains
from opl.vault.links import non_identifying_ends
from opl.vault.links import source_columns as link_source_columns
from opl.vault.registry import (
    EffectivitySatellite,
    Hub,
    Link,
    ReferenceTable,
    Satellite,
    VaultTable,
)

# `columns_for` COMES FROM `opl.contracts.catalogue` AND NOT FROM `cnpj_schemas`, SINCE
# F-DB. The pairing lock reads the contract of whatever bronze table a task names, and
# three of those contracts are not the Receita's file layouts at all --
# `cnpj_schemas.columns_for` raises KeyError for `merchant`. The catalogue is the join
# over every source, which is the question that lock is actually asking.

# The one loader per kind, EXCEPT for links -- see `entry_point_for`, where the split
# between the two link loaders is derived rather than listed.
_ENTRY_POINT_OF_KIND: dict[type, str] = {
    Hub: "vault_load_hub",
    Satellite: "vault_load_satellite",
    EffectivitySatellite: "vault_load_effectivity",
    ReferenceTable: "vault_load_reference",
}


def _is_a_derived_link(link: Link) -> bool:
    """Does this link have an end `load_link` cannot compute?

    THE SAME CONDITION `opl.vault.links._refuse_a_link_this_loader_cannot_write` TESTS,
    and since F-DB it is the same FUNCTION rather than a restatement of it:
    `non_identifying_ends` is exported from that module for exactly this caller. That
    refusal is the reason there are two link loaders at all -- `load_link` computes every
    end's reference from the columns that hub is NAMED after, so an undeclared derived
    end would hash both ends of `link_company_partner` from `cnpj_basico` and every
    relationship would read as a company partnered with itself, with the right row count
    and working joins.

    THE CONDITION MOVED AND THE ROUTING DID NOT, WHICH IS WHAT THIS RE-DERIVATION IS FOR.
    It read `any(not end.identifying ...)`, and that flag was a PROXY for "derived".
    `link_merchant_empresa`'s empresa end is derived AND identifying, so under the old
    spelling this lock would have routed it to `vault_load_partner_link.py` -- a loader
    that would refuse it -- while under the flag's stated MEANING it belongs on
    `vault_load_link.py`, which can now write it. Asking the loader's own function means
    the two cannot disagree about which entry point a link needs.

    THE DEPENDENT-CHILD-KEY ARM CAME OFF IN F2 WAVE 2, IN THE SAME COMMIT AS THE LOADER'S.
    `load_link` writes dependent-child keys now -- `link_candidates` projects them and
    `links.link_columns` names them -- so a link carrying one is no longer a link this
    entry point cannot run. Left in, this lock would have routed `link_payment` to
    `vault_load_partner_link.py`, a loader that refuses any link whose dependent-child
    keys are not socios' own two, and the whole table would have been unrunnable with the
    routing looking deliberate. `link_company_partner` still routes there, on the arm that
    survives: its partner end declares no `key_from`."""
    return bool(non_identifying_ends(link))


def entry_point_for(spec: VaultTable) -> str:
    """The one `databricks/src` script that can load `spec`."""
    if isinstance(spec, Link):
        return "vault_load_partner_link" if _is_a_derived_link(spec) else "vault_load_link"
    entry_point = _ENTRY_POINT_OF_KIND.get(type(spec))
    assert entry_point is not None, (
        f"vault table {spec.name!r} is a {type(spec).__name__}, a kind no entry point "
        "under databricks/src loads. A new table kind needs one, or it is a registered "
        "table no job can write"
    )
    return entry_point


def required_source_columns(spec: VaultTable) -> tuple[str, ...]:
    """The source columns the loader for `spec` will demand by name.

    MIRRORS EACH LOADER'S OWN `refuse_non_string_columns` CALL, which is the list that
    decides whether a (vault table, bronze source) pairing can work at all. Asserting it
    here is what turns "that pairing fails in Spark, eventually, if we are lucky" into
    "that pairing is refused before the bundle is deployed"."""
    if isinstance(spec, Hub):
        return spec.business_key_columns
    if isinstance(spec, Satellite):
        return (*_satellite_key_columns(spec), *spec.payload_columns,
                spec.applied_date_from.column)
    if isinstance(spec, Link):
        hubs = domains.linked_hubs(spec)
        if _is_a_derived_link(spec):
            # `partner_link_candidates` refuses the COMPANY end's key and the two
            # dependent-child keys; the partner end is derived from one of the latter.
            return (*hubs[0].business_key_columns, *spec.dependent_child_key_columns)
        # `link_candidates` asks the END where its hub's key lives -- the hub's own
        # column names, or the columns a `LinkEnd.key_from` declares. Restated as
        # `hub.business_key_columns` this lock would demand `cnpj_basico` from
        # `bronze_merchant`, which has no such column, and refuse a pairing that works.
        #
        # AND THE DEPENDENT-CHILD KEYS, SINCE F2 WAVE 2, for the reason `links.source_
        # columns` gives: they are hashed into the link's digest and written into the
        # table, so a source that does not carry one is a source this loader cannot read.
        # `link_payment` is the first link on this branch of the routing to have any.
        return tuple(
            name
            for end, hub in zip(spec.ends, hubs, strict=True)
            for name in end.source_columns(hub)
        ) + spec.dependent_child_key_columns
    if isinstance(spec, EffectivitySatellite):
        link = domains.parent_link(spec)
        return (*domains.link_identity_columns(link), spec.entry_column)
    assert isinstance(spec, ReferenceTable), f"no column list is known for {spec.name!r}"
    return (spec.natural_key, spec.payload)


# --- THE APPLIED-DATE HALF, AND WHAT DRIVES IT -----------------------------------------
#
# Module level for the standing reason `opl.bronze.rules` gives above `rules_for` ("WHY
# THE RULE SETS BELOW ARE ORDERED THE WAY THEY ARE"): inside the docstrings below
# this puts both functions past the 50-line cap, and it is one argument about two of them.
#
# `required_source_columns` DEMANDS `spec.applied_date_from.column` AND
# `columns_the_source_carries` ADDS `_snapshot_ref_date`, AND THE PAIR IS DRIVEN BY
# `test_the_source_lock_catches_a_default_applied_date_on_a_source_that_stamps_none`.
# That test is the reason both halves are here rather than one of them: F2 wave 2's review
# measured the two additions and found they CANCELLED. Every (task, source) pairing the
# YAMLs carry satisfies the demand, so removing it killed zero tests; and the audit half
# only ever fires BECAUSE the demand exists, so removing both returned the file to its
# behaviour before either. Two edits that exactly undo each other on every real pairing
# are two edits no test can see, which is the shape ADR 0018 is about: when a check
# reports the expected value, ask what else would produce that value.
#
# WHAT THE PAIR IS FOR IS THE PAIRING NO YAML CARRIES YET. A satellite that declares the
# DEFAULT applied-date source reads `_snapshot_ref_date`, an AUDIT column on no contract;
# `bronze_payments` does not have it, deliberately
# (`opl.bronze.autoloader.add_common_audit_columns` omits it for a GENERATED source, and
# `opl.bronze.rules`' payments set drops `unprovable_snapshot_ref_date` because THE COLUMN
# DOES NOT EXIST). Against the contract alone every satellite that reads
# `_snapshot_ref_date` -- and they are not all RFB, since `sat_merchant_dados` reads it off
# a POSTGRES source -- reads as a pairing that cannot work; without the demand at all, one
# of them repointed at `bronze_payments` reads as one that CAN, and it cannot. That is the
# copy-paste this file exists to refuse, one source further along than the original probe.


def _satellite_key_columns(spec: Satellite) -> tuple[str, ...]:
    """The source columns a satellite's own hash key is taken over.

    A SATELLITE'S PARENT MAY BE A LINK SINCE F2 WAVE 2, so this can no longer read
    `parent_hub`: that resolver refuses a link parent by design. The link branch is
    `links.source_columns`, the list `link_candidates` itself refuses on.

    THE LINK BRANCH IS UNEXERCISED TODAY AND THIS DOCSTRING USED TO CLAIM OTHERWISE. It
    said `sat_link_payment` "would have turned this lock into an unhandled `ValueError`".
    Measured false: reverting this function to `parent_hub` leaves the file green apart
    from the pre-existing red, because the branch is reached only through a YAML TASK and
    no YAML task names `sat_link_payment` -- T3 owns that task and is blocked on
    `databricks/`. So this is FORWARD WORK, written in the same commit as the registry
    entry that will need it, and it goes live on the day that task is declared. The row
    in `docs/unexercised-ledger.md` is WRITTEN -- `jobdem:186`, section 3.2 -- so this
    sentence is the claim and that row is the record."""
    parent = domains.parent_of(spec)
    if isinstance(parent, Link):
        return tuple(link_source_columns(parent, domains.linked_hubs(parent)))
    return parent.business_key_columns


def parents_in(spec: VaultTable) -> tuple[str, ...]:
    """The vault tables `spec`'s rows reference, which must be loaded before it.

    THE SATELLITE ARM READS `spec.parent` AND NOT `parent_hub(spec).name` SINCE F2 WAVE 2,
    and the change is UNEXERCISED for `_satellite_key_columns`' reason: the ordering lock
    walks the tasks a YAML declares, and no YAML declares one for `sat_link_payment`. The
    two spellings agree on every satellite a job loads today -- a hub parent's name IS
    `spec.parent` -- so nothing distinguishes them until the link-parented satellite gets
    a task. Kept because `parent_hub` would RAISE on that table rather than answer, and
    changing it then would be an edit in the middle of a red run."""
    if isinstance(spec, Satellite):
        return (spec.parent,)
    if isinstance(spec, Link):
        return tuple(dict.fromkeys(spec.hub_names))
    if isinstance(spec, EffectivitySatellite):
        return (spec.parent,)
    return ()


def columns_the_source_carries(source: str) -> set[str]:
    """Every column a bronze table really has that a vault loader may name: its contract's,
    plus `_snapshot_ref_date` where the ingest stamps one.

    THE AUDIT HALF IS NOT DECORATION, AND WHAT DRIVES IT IS ARGUED IN THE COMMENT BLOCK
    ABOVE -- together with the demand in `required_source_columns` that it answers, because
    the two were added together and were measured cancelling each other out.

    `declares_source_date` IS ASKED RATHER THAN RE-DERIVED. `opl.dataops.cadence` owns the
    question and reads it off `landing` and `snapshot_axis`, the two fields that pick
    between the three audit stamps. A second predicate here would drift, because those
    stamps live in `opl.bronze.autoloader` and nothing in this file watches them."""
    spec = bronze_table_spec(source)
    carried = set(columns_for(spec.contract))
    if declares_source_date(spec):
        carried.add(SNAPSHOT_REF_DATE_COLUMN)
    return carried
