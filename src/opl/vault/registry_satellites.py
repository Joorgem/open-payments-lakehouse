# src/opl/vault/registry_satellites.py
"""The whole-set guards for the SATELLITE kind: what its parent may be, what its
declared columns may not collide with, and which transactionality its parent supports.

WHY ITS OWN MODULE, AND THE RULE THAT PUT IT HERE. `opl.vault.registry` holds the
mechanism -- discovery, the whole-set guards, resolution -- and held these three inline
until F2 wave 2. Widening the parent from HUB to `Hub | Link` is the largest change any
guard in this registry has had -- the deferral lifted, the two pairings that are still
refused, and the collision surface a link parent does and does not add.

THE BASELINE IS MEASURED AND THE COUNTERFACTUAL IS DROPPED, WHICH THIS PARAGRAPH GOT
WRONG ONCE. It said the change "took that file from 710 to 839": `git show
cae3eff:src/opl/vault/registry.py | wc -l` is **710**, and the 839 is a length no file
ever had, because the unsplit version was never written. What is checkable is the
baseline against the cap: 710 lines with a strictly-under-800 cap leaves 90, and this
module -- which is the guard and its argument and nothing else -- is longer than that.
Master protocol section 4.12 is that whoever touches a file at the cap splits it FIRST,
and `opl.vault.specs` records the same crossing being handled the same way in Task 6.

THE SEAM IS ONE KIND'S GUARDS AND NOT AN ARITHMETIC CUT, which is the property that
makes this a boundary rather than a shelf. `opl.bronze.registry_collisions` and
`opl.gold.registry_guards` are the two precedents in this repository, and both are
"the whole-set questions about one subject, called from the registry's own
`build_registry` at its own import". Everything here is about a `Satellite`; nothing
here resolves a link's hubs, validates a domain, or discovers a module.

`build_registry`'S CALL ORDER STAYS IN `registry.py`, DELIBERATELY. That order is
load-bearing -- `_assert_no_gated_link_takes_its_identity_over_one_column_twice` runs
after the guard that refuses a non-link parent so the worse message never wins -- and
its own docstring says it has to be reviewable in one place. What moved is the bodies,
not the sequence.

TWO OF THE THREE FUNCTIONS ARE PRIVATE AND THE THIRD IS THE ONLY ONE CALLED FROM
OUTSIDE, which is why there is no `__all__`: `registry.py` imports
`assert_every_satellite_hangs_off_a_hub_or_a_link` and nothing else, and the two
helpers are reachable only through it."""
from __future__ import annotations

from collections.abc import Mapping

from opl.vault.specs import Hub, Link, Satellite, VaultTable

# --- WHAT A SATELLITE'S PARENT MAY BE, AND THE TWO PAIRINGS THAT ARE STILL REFUSED -----
#
# Module level for this file's standing reason (`opl.bronze.rules`'s "WHY THE RULE SETS
# BELOW ARE ORDERED THE WAY THEY ARE" block): this is the
# reasoning, and inside the docstrings below it puts the functions past the 50-line cap.
#
# THE PARENT MAY NOW BE A LINK, WHICH IS THE HALF OF THIS GUARD F2 WAVE 2 CHANGED. The
# refusal used to say a link-parented satellite "would be a registered table nothing in
# this package can write. The guard and that signature have to change together" -- and
# they did, in one task: `load_satellite` takes `link=` and `hubs=` beside `hub=`, and
# `sat_link_payment` is the table that consumed it. `opl.vault.specs.Satellite` carries
# the argument for why that is the SAME kind and not a fifth one.
#
# WHAT IS UNCHANGED IS EVERYTHING ELSE THE GUARD REFUSED. A missing parent, and a parent
# that is a SATELLITE, an EFFECTIVITY SATELLITE or a REFERENCE TABLE, are all still
# refused -- and widening a refusal is exactly how a guard stops being able to fail, so
# the `isinstance` is against the two kinds that have a hash key rather than against the
# ones it used to name. A satellite parented on another satellite would key on a column
# its parent does not have; a reference table has no hash key at all
# (`ReferenceTable` states that as its own decision), so there is nothing to key on.
#
# THE COLLISION SURFACE IS THE PARENT'S HASH KEY AND NOTHING ELSE, AND THE LINK PARENT
# ADDS NO SECOND ONE -- which is worth stating because the effectivity guard beside this
# one DOES also refuse its link's dependent-child keys. `opl.vault.satellites._in_column_
# order` writes exactly six kinds of column: the parent's hash key, `load_date`,
# `applied_date`, `record_source`, `hash_diff` and the payload. The link's REFERENCE
# COLUMNS (`payer_hub_empresa_hk`) and its DEPENDENT-CHILD KEYS (`transaction_id`) are
# columns of the LINK's own row and are never projected here, so a payload column of one
# of those names is a second reading of a source column, not a value lost to a write.
# `applied_date` and the other three metadata names are refused earlier and per-table, by
# `_validated_columns` and by `AppliedDateSource.__post_init__`.
#
# THE APPLIED-DATE SOURCE IS CHECKED BESIDE THE PAYLOAD because it is read out of the
# source by name exactly as a payload column is, and the write puts the parent's digest
# into the parent's hash key: a satellite declaring its `applied_date` came from
# `hub_empresa_hk` would be read from a column bronze does not have, or -- on a source
# that happened to carry one -- read and then overwritten.


def _refuse_a_satellite_column_the_write_would_take(
    satellite: Satellite, parent: Hub | Link
) -> None:
    """The payload and the applied-date source must not name the parent's hash key.

    See the comment block above for what else the write takes and why a link parent adds
    nothing to this list. Only visible once the parent is resolved, which is why this is
    a whole-set guard rather than a `__post_init__` one."""
    declared = {name: "a payload column" for name in satellite.payload_columns}
    declared.setdefault(satellite.applied_date_from.column, "its applied-date source")
    role = declared.get(parent.hash_key)
    if role is None:
        return
    kind = "hub" if isinstance(parent, Hub) else "link"
    raise ValueError(
        f"satellite {satellite.name!r} names {parent.hash_key!r} as {role}, and that is "
        f"its parent {kind} {parent.name!r}'s hash key. The loader writes the digest "
        "into that column, so the declared value would be replaced by it without "
        "anything failing"
    )


def _refuse_a_transactionality_the_parent_does_not_support(
    satellite: Satellite, parent: Hub | Link
) -> None:
    """`Satellite.transactional` decides whether an observation ledger gates this table,
    and the two pairings below are refused so it cannot become a switch.

    NEITHER IS A DEFINITION AND BOTH NAME WHAT WOULD HAVE TO CHANGE, which is the idiom
    the link-parent refusal itself used until this task lifted it. A STATE satellite on a
    link is legal DV2 -- socios' `qualificacao_socio` is the first candidate this registry
    names -- and it is refused today only because `load_satellite` has no link-grain
    comparison: `opl.vault.effectivity._refuse_a_mismatched_link_grain` is the one that
    exists, it is written for the effectivity satellite, and routing it here belongs to
    the task that has a table to point at it. An EVENT satellite on a hub is refused from
    the other side: every hub in this vault is keyed on a business object observed in
    snapshots, so its keys really can depart, and declaring one transactional would remove
    both the departure count AND `_window`'s refusal of an unloaded month from a table
    where both mean something -- silently, since neither shows up in the rows written."""
    if satellite.transactional and isinstance(parent, Hub):
        raise ValueError(
            f"satellite {satellite.name!r} declares transactional=True and hangs off hub "
            f"{parent.name!r}. A hub row asserts that a business key EXISTS, so its keys "
            "can be absent from a later snapshot and that absence is the one thing the "
            "observation ledger reports; transactional=True would drop the ledger, and "
            "with it the refusal of a window month nothing ever loaded. If a hub over an "
            "event stream ever arrives, argue it here rather than declaring past this"
        )
    if not satellite.transactional and isinstance(parent, Link):
        raise ValueError(
            f"satellite {satellite.name!r} hangs off link {parent.name!r} and does not "
            "declare transactional=True, so load_satellite would require an "
            "ObservationGrain at the LINK's identity -- and the comparison that checks "
            "one is opl.vault.effectivity._refuse_a_mismatched_link_grain, which "
            "load_satellite does not call. A STATE satellite on a link is legal DV2 and "
            "this is a deferral, not a rule: the task that declares one routes that "
            "comparison into load_satellite in the same edit"
        )


def assert_every_satellite_hangs_off_a_hub_or_a_link(
    tables: Mapping[str, VaultTable]
) -> None:
    """Refuse a satellite whose parent is missing, is neither a hub nor a link, whose
    payload or applied-date source collides with the parent's hash key, or whose
    transactionality its parent cannot support.

    All four need the other tables, which is why they are here and not in
    `Satellite.__post_init__`. The collision is the quiet one: the hash key is written by
    the loader, so a column of the same name loses its source value to the digest and
    keeps looking like a column. See the comment block above."""
    for table in tables.values():
        if not isinstance(table, Satellite):
            continue
        parent = tables.get(table.parent)
        if parent is None:
            raise ValueError(
                f"satellite {table.name!r} names parent {table.parent!r}, which no "
                f"domain registers. Registered: {', '.join(sorted(tables))}"
            )
        if not isinstance(parent, Hub | Link):
            raise ValueError(
                f"satellite {table.name!r} names parent {table.parent!r}, which is "
                "not a hub and not a link. A satellite keys on its parent's HASH KEY, "
                "and those are the only two kinds that have one: a satellite parented "
                "on another satellite would key on a column its parent does not have, "
                "and a reference table has no hash key at all. Resolve the parent with "
                "opl.vault.domains.parent_of"
            )
        _refuse_a_satellite_column_the_write_would_take(table, parent)
        _refuse_a_transactionality_the_parent_does_not_support(table, parent)
