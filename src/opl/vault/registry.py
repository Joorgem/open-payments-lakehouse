# src/opl/vault/registry.py
"""The vault registry MECHANISM. It holds no table of its own, and that is the point.

WHY PER-DOMAIN FROM THE OUTSET. The plan's scope boundary stakes DV2's extensibility
claim on wave 2 adding `hub_account`, `hub_customer` and `link_payment` with a git
diff of "+N files, 0 modified". A single registry carrying the table list would have
to be edited to register them, and the demonstration would be false on the one file
that matters -- and it cannot be repaired later, because the git history IS the
evidence. So the tables live in `opl/vault/domains/<domain>.py` and this module is
only the shape they must have, the guards they must pass, and the way they are found.

EXACTLY WHAT THE CLAIM COVERS TODAY, because it is narrower than "any domain" and
overstating it would be the same defect in prose that it is in code. A domain built
from HUBS, SATELLITES AND LINKS is "+1 file, 0 modified": `VaultTable` carries those
kinds and `VaultDomain.__post_init__` refuses anything else, so they need nothing added
here. That is wave 2's whole list -- its `hub_account` and `hub_customer` are hubs with
satellites and its `link_payment` is a link -- so the claim the plan stakes is covered
kind for kind rather than by analogy. Note that `link_payment`'s `transaction_id` is a
DEPENDENT-CHILD KEY, which `Link` now carries, so wave 2 does not need this file for
that either. A domain introducing a NEW table kind still does not clear the bar: the
kind and its own `__post_init__` land in `opl.vault.specs`, and its whole-set guard (if
it needs one) and its word in the `VaultTable` union land here, exactly as `Link` did in
Task 4 and `EffectivitySatellite` in Task 5, which is an edit inside WAVE 1 and is what
the plan always said would happen. `test_a_new_domain_of_hubs_satellites_and_links_is_
discovered_without_editing_any_file` builds a throwaway domain carrying wave 2's three
tables by name and registers it.

HOW A DOMAIN IS FOUND, AND THE THREE TIDIER ALTERNATIVES THAT ALL FAIL THE CLAIM.
`discover_domains` scans the `opl.vault.domains` package DIRECTORY and imports every
module in it, reading a module-level `DOMAIN` from each. Wave 2 adds one file and
nothing else changes. The alternatives:

  - a list of module names in `domains/__init__.py` -- wave 2 edits `__init__.py`;
  - an `import opl.vault.domains.payments` at the foot of this module -- wave 2 edits
    this file, which is the specific thing per-domain was for;
  - a `[project.entry-points]` table -- wave 2 edits `pyproject.toml`.

All three are "0 modified" only if you do not count the file doing the counting.

REGISTRATION IS A VALUE, NOT AN IMPORT SIDE EFFECT. A domain module exposes
`DOMAIN = VaultDomain(...)`; nothing mutates a global on import. Two things follow,
and both are why it is worth the extra line in each domain file. First, the
whole-set guards below run over EVERY domain at once, so a satellite may name a hub
another module declares without anyone depending on the order the filesystem yielded
files in -- an incremental `register()` would make that ordering load-bearing and
invisible. Second, a test can build a throwaway domain package and register it without
touching the real registry, which is how the "+N files, 0 modified" property is
asserted rather than asserted about (`tests/vault/test_registry.py`).

THE GUARDS RUN WHERE THE MISTAKE IS, in the house style of `opl.bronze.registry`:
everything checkable about one table is refused in its `__post_init__` (in
`opl.vault.specs`), before Spark and before any registry exists; everything that
needs to see the other tables is refused in `build_registry`, which
`domains/__init__.py` calls at import so a malformed registry breaks the import of
every module that reads it rather than the one job that touches that table.

WHAT TASK 5 ADDED, AND WHY EACH OF THE THREE WAS WORTH A NEW CONCEPT RATHER THAN A
WORKAROUND. All three were predicted here by name in Task 4 and left out until the data
said what shape they had.

  - `LinkEnd`, carrying a ROLE. `link_company_partner` references `hub_empresa` at both
    ends -- a company and a partner that is itself a company -- and Task 4's `Link`
    refused a repeated hub precisely because it had no role name and both references
    would have gone into one column. The role prefixes the reference column.
  - DEPENDENT-CHILD KEYS on a link. The measured sócio grain is (`cnpj_basico`,
    `identificador_socio`, `cpf_cnpj_socio`), whose last two components belong to NO
    hub: the RFB masks a partner's CPF to six middle digits, so its key space is 10^6
    and 99.99% occupied and a hub on it merges unrelated people onto every key by
    construction. They are key components stored on the link, which is the idiom the
    master spec itself chooses for `transaction_id` on `link_payment`.
  - `EffectivitySatellite`, a satellite ON a link. It is a fourth kind and not a
    `Satellite` with a link parent, and Task 5 gave TWO reasons -- no payload and no
    `hash_diff`, and `load_satellite` takes a `Hub`. **Only the first survives F2 wave
    2**, which lifted the second; `opl.vault.specs.EffectivitySatellite` now carries the
    corrected paragraph, and this bullet said "that refusal therefore stands unchanged"
    until the wave-2 task that changed it.

WHAT F2 WAVE 2 ADDED, AND IT IS THE DEFERRAL THIS BLOCK USED TO RECORD. A DESCRIPTIVE
satellite on a LINK -- `sat_link_payment`, carrying the payment's own measures. The
refusal that stood here named its own condition ("the guard and that signature have to
change together") and both halves moved in one task: `opl.vault.registry_satellites`
admits a `Hub | Link` parent and `load_satellite` takes `link=`/`hubs=` beside `hub=`.
Two pairings are still refused there, each naming what would have to change: an EVENT
satellite on a hub, and a STATE satellite on a link -- socios' `qualificacao_socio` and
`faixa_etaria` are the first candidates for the latter, and are still declared as
unmodelled in `domains/cnpj.py` rather than left as a gap.

WHAT IS STILL DELIBERATELY NOT HERE. No per-end KEY COLUMN MAP: a `LinkEnd` reads its
hub's business key from the columns named after it, and `link_company_partner`'s partner
end -- whose `cnpj_basico` socios carries only as the first eight characters of
`cpf_cnpj_socio` -- is derived by `opl.vault.partners`, which is the one loader in this
package that is domain-specific and says so.

No table QUALIFICATION either -- a spec carries an unqualified `name`, and the loaders
take the qualified table as an argument, so `opl.config` is consulted in the domain
module and by whatever calls a loader, and nowhere in this layer. (That "whatever" is
NOT a job task on this branch: nothing in `databricks/` runs any of this yet, which is
`docs/f2-wave-1-run-evidence.md` §20's third item. Two refusal messages in this file
told an operator to check "the `table` parameter of the job that failed"; there is no
such job to check, so they name the domain module instead.)

THE FIVE KIND SPECS MOVED TO `opl.vault.specs` IN TASK 6'S FIX ROUND, and this module
imports every one of them back -- `from opl.vault.registry import Hub` and the like
keep working unchanged for every caller in this repository, because the names below
ARE those imports, not a restatement of them. Why the move, and the rule for the next
kind, are argued in full in `opl.vault.specs`'s own module docstring rather than here:
in short, this file reached 799 of its 800-line cap adding `ReferenceTable`'s own module
in Task 6, one line of headroom was not enough room for review's own corrections to
land, and the fix is to keep every kind's SHAPE in one module built to hold several of
them, while this file keeps the MECHANISM -- discovery, the whole-set guards, and
resolution -- which is what it was always for."""
from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

from opl.vault.registry_satellites import (
    assert_every_satellite_hangs_off_a_hub_or_a_link,
)
from opl.vault.specs import (
    AppliedDateSource,
    BusinessKeyColumn,
    EffectivitySatellite,
    Hub,
    KeyPrefix,
    Link,
    LinkEnd,
    ReferenceTable,
    Satellite,
    VaultTable,
)

# Re-exported so `from opl.vault.registry import BusinessKeyColumn` (and every other
# kind name) keeps resolving for callers written before the Task 6 fix-round split --
# `opl.vault.specs` is where they are DEFINED, this module is where every caller in
# this repository already imports them FROM. `ruff` would flag these as unused
# without `__all__` naming them; the list is the re-export contract, spelled once.
__all__ = [
    "AppliedDateSource",
    "BusinessKeyColumn",
    "EffectivitySatellite",
    "Hub",
    "KeyPrefix",
    "Link",
    "LinkEnd",
    "ReferenceTable",
    "Satellite",
    "UnknownVaultTable",
    "VaultDomain",
    "VaultTable",
    "build_registry",
    "discover_domains",
    "identifying_hubs",
    "identity_columns_of",
    "identity_derivations_of",
    "link_identity_columns",
    "linked_hubs",
    "parent_hub",
    "parent_link",
    "parent_of",
    "table_spec",
]

# The name a domain module must bind at module level. One attribute, spelled once,
# because `discover_domains` reads it by name and a module that spells it differently
# would be found and silently contribute nothing.
DOMAIN_ATTRIBUTE = "DOMAIN"


class UnknownVaultTable(ValueError):
    """A vault table name that is not registered.

    A `ValueError` and not a `KeyError`, for the two reasons
    `opl.bronze.registry.UnknownTable` records: `KeyError.__str__` re-`repr`s its
    argument, so prose written for an operator's run log arrives quoted and escaped;
    and an `except KeyError` several frames up in a job entry point would swallow a
    mistyped table name and replace this message with a generic one."""


@dataclass(frozen=True, kw_only=True)
class VaultDomain:
    """One domain's vault tables, bound to `DOMAIN` in its own module.

    The registration entry point: a domain declares this value and `discover_domains`
    reads it. Nothing else in this package is a registration API, so there is no way
    to add a table that skips `build_registry`'s guards."""

    name: str
    tables: Sequence[VaultTable]

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("a vault domain needs a name")
        tables = tuple(self.tables)
        if not tables:
            raise ValueError(
                f"domain {self.name!r} declares no tables. A module in the domains "
                "package that registers nothing is a module left half-written, not a "
                "domain -- and it would be discovered and contribute silently"
            )
        # `isinstance` READS `VaultTable` rather than restating the union, so adding a
        # kind to it extends this refusal in the same edit. A restated list is how a
        # new kind gets registered by one line and refused by another.
        if any(not isinstance(table, VaultTable) for table in tables):
            raise TypeError(f"domain {self.name!r} declares something that is not a vault table")
        object.__setattr__(self, "tables", tables)


def _assert_domain_names_are_unique(domains: Sequence[VaultDomain]) -> None:
    seen: set[str] = set()
    for domain in domains:
        if domain.name in seen:
            raise ValueError(
                f"two domain modules both call themselves {domain.name!r}. The name is "
                "how a table is attributed to the team that owns it, and two claimants "
                "make that attribution arbitrary"
            )
        seen.add(domain.name)


def _collected_tables(domains: Sequence[VaultDomain]) -> dict[str, VaultTable]:
    """Every domain's tables in one mapping, refusing a name two of them claim.

    The collision that exists BECAUSE the registry is per-domain, and the reason this
    runs over the whole set: each domain file is individually valid and neither
    author sees the other's. Two specs on one Delta name means one of them silently
    loads into the other's table."""
    tables: dict[str, VaultTable] = {}
    owner: dict[str, str] = {}
    for domain in domains:
        for table in domain.tables:
            if table.name in tables:
                raise ValueError(
                    f"domains {owner[table.name]!r} and {domain.name!r} both declare a "
                    f"vault table called {table.name!r}. One of them would load into "
                    "the other's Delta table, with both runs reporting success"
                )
            tables[table.name] = table
            owner[table.name] = domain.name
    return tables


def _link_hubs(tables: Mapping[str, VaultTable], link: Link) -> tuple[Hub, ...]:
    """`link`'s hubs, resolved against `tables`, or refuse naming the one that failed.

    ONE ENTRY PER END, IN DECLARATION ORDER, INCLUDING A HUB THAT APPEARS TWICE. A
    self-referencing link resolves to the same `Hub` object at both ends and that is
    the answer, not a duplicate to collapse: the loader writes one reference column per
    END, and de-duplicating here would drop one of them.

    Shared by the whole-set guard and by `linked_hubs` so the refusal and the lookup
    are the same code: a resolver that repeated the guard's conditions in a weaker
    form is how a registry that passed its guards still returns something wrong."""
    resolved: list[Hub] = []
    for name in link.hub_names:
        hub = tables.get(name)
        if hub is None:
            raise ValueError(
                f"link {link.name!r} names hub {name!r}, which no domain registers. "
                f"Registered: {', '.join(sorted(tables))}"
            )
        if not isinstance(hub, Hub):
            raise ValueError(
                f"link {link.name!r} names {name!r} as one of its hubs, and it is not "
                "a hub. A link's hash key is taken over its hubs' BUSINESS KEYS, and "
                "nothing else in this registry has one -- the lookup would succeed and "
                "the failure would arrive layers away as a missing column"
            )
        resolved.append(hub)
    return tuple(resolved)


def _assert_every_link_joins_registered_hubs(tables: Mapping[str, VaultTable]) -> None:
    """Refuse a link whose hubs are missing or are not hubs, or whose hash-key column
    names collide with theirs.

    The sibling of `registry_satellites.assert_every_satellite_hangs_off_a_hub_or_a_link`,
    and here for its reason:
    every check needs the other tables. The COLLISION half is the quiet one. The loader
    writes one column per participating hub plus the link's own digest, so two of those
    names being equal means one projection writes two values into one column -- the
    link keeps its right row count and one of its ends is silently the wrong digest,
    joining to nothing. THE SELF-REFERENCING CASE IS EXACTLY THIS COLLISION with the
    two ends on one hub, which is what a `LinkEnd` role resolves and what an unroled
    repeat still runs into -- the message names the fix."""
    for table in tables.values():
        if not isinstance(table, Link):
            continue
        hubs = _link_hubs(tables, table)
        written: dict[str, str] = {table.hash_key: f"link {table.name!r}'s own hash key"}
        for key in table.dependent_child_keys:
            written[key.name] = f"link {table.name!r}'s dependent-child key"
        for end, hub in zip(table.ends, hubs, strict=True):
            reference = end.reference_column(hub)
            if reference in written:
                raise ValueError(
                    f"link {table.name!r} would write {reference!r} twice: it is the "
                    f"reference for hub {hub.name!r} under role {end.role!r} and also "
                    f"{written[reference]}. One column, two values -- the row count "
                    "stays right and one end of the relationship points at the wrong "
                    "digest. Two ends on the SAME hub need distinct roles, which is "
                    "what LinkEnd.role is for"
                )
            written[reference] = f"hub {hub.name!r}'s reference under role {end.role!r}"


def _refuse_a_derivation_that_does_not_fit(link: Link, end: LinkEnd, hub: Hub) -> None:
    """One declared end's `key_from` must describe the hub it claims to key.

    TWO CHECKS, AND BOTH FAIL SILENTLY WITHOUT THIS. A declaration with the wrong NUMBER
    of entries is matched positionally by `opl.vault.loading._padded`, which refuses a
    length mismatch -- but inside Spark, several tasks into a job, naming a component
    count rather than the link. A declaration with the wrong WIDTH does not fail at all:
    it keys on a different-length root, produces a digest `load_hub` never wrote, and the
    link joins to nothing while reporting the right row count.

    THE HUB'S WIDTH MUST BE DECLARED, which is the third refusal and the least obvious.
    `BusinessKeyColumn(width=None)` means "take the value as it is", so there is no width
    for a prefix to agree WITH -- and a prefix taken against it would be an independent
    claim about a canonical form the hub deliberately declines to make."""
    if len(end.key_from or ()) != len(hub.business_keys):
        raise ValueError(
            f"link {link.name!r} declares a key_from of {len(end.key_from or ())} "
            f"component(s) for hub {hub.name!r}, which is keyed on "
            f"{hub.business_key_columns}. They are matched POSITIONALLY, so a shorter or "
            "longer declaration derives and hashes the wrong column"
        )
    for prefix, key in zip(end.key_from or (), hub.business_keys, strict=True):
        if key.width == prefix.width:
            continue
        raise ValueError(
            f"link {link.name!r} derives hub {hub.name!r}'s {key.name!r} as the first "
            f"{prefix.width} characters of {prefix.column!r}, and that hub declares width "
            f"{key.width!r}. The prefix must be the hub's OWN declared width: a shorter or "
            "longer root is a different key space, so every reference would be a digest "
            f"load_hub never wrote and {link.name!r} would join to nothing without failing"
        )


def _assert_every_declared_key_derivation_fits_its_hub(
    tables: Mapping[str, VaultTable]
) -> None:
    """Refuse a `LinkEnd.key_from` that does not describe the hub it is declared on.

    HERE AND NOT IN `LinkEnd.__post_init__` for this file's standing reason: the end
    names its hub by STRING, so the width it must agree with is on a table only the whole
    set can resolve. `_link_hubs` is reused rather than re-resolved, so a link whose hub
    is missing is refused by the guard above with that message rather than by this one
    with a worse one."""
    for table in tables.values():
        if not isinstance(table, Link):
            continue
        hubs = _link_hubs(tables, table)
        for end, hub in zip(table.ends, hubs, strict=True):
            if end.key_from is not None:
                _refuse_a_derivation_that_does_not_fit(table, end, hub)


# --- THE IDENTITY COLUMNS OF A GATED LINK, AND WHY THE GUARD IS SCOPED ----------------
#
# Module level for the reason `opl.bronze.snapshot` states above `ref_date_from_instant`:
# this is the reasoning, and inside the docstring it puts the function past the project's
# 50-line cap.
#
# `identity_columns_of` concatenates each identifying end's source columns and then the
# dependent-child keys, with no dedup, because none of those lists knows about the others.
# Two identifying ends reading ONE source column therefore yield a tuple with a repeat.
#
# WHAT THE REPEAT COSTS, AND ONLY WHERE. That tuple is the OBSERVATION LEDGER'S KEY:
# `effectivity._refuse_a_mismatched_link_grain` requires the gating grain's key columns to
# be exactly this list, and `ObservationGrain.__post_init__` refuses a repeated key column
# outright -- so the pair is unsatisfiable and the satellite can never be loaded. Without
# this guard that is discovered at GRAIN-CONSTRUCTION time, deep inside a job, in a message
# naming the grain rather than the link that made it impossible.
#
# SCOPED TO EFFECTIVITY PARENTS, WHICH IS THE CORRECTION AND NOT A WEAKENING. The obvious
# guard -- no link may repeat an identity column, which is what the review asked for -- is
# WRONG, and measured wrong against this very registry: `link_empresa_estabelecimento` is
# HIERARCHICAL, `hub_estabelecimento`'s business key CONTAINS `hub_empresa`'s, and its
# identity is legitimately `('cnpj_basico', 'cnpj_basico', 'cnpj_ordem', 'cnpj_dv')`.
# Hashing the parent's key and then the child's compound key that contains it is what a
# hierarchy IS; the link is correct, and the blanket form would have refused a shipped
# table to close a hypothetical. It has no effectivity satellite, so its repeat costs
# nothing. `test_the_same_repeat_is_ALLOWED_on_a_link_with_no_effectivity_satellite` pins
# the permission, so the tightening cannot happen quietly later.
#
# HERE FOR THIS FILE'S STANDING REASON, the one `identifying_hubs`' docstring already names
# for a different function: an end names its hub by STRING and a satellite names its link
# by STRING, so this is only knowable once the whole set resolves. Import time is where a
# registry defect belongs -- every registry in this repository is built at import.
#
# NOT LIVE TODAY: `link_company_partner` and `link_merchant_empresa` are the two links with
# effectivity satellites and both have distinct identity columns. Wave 2's `link_payment`
# is the next two-identifying-end shape, which is why the guard lands before that link does
# rather than after.


def _refuse_a_repeated_identity_column(
    satellite: EffectivitySatellite, link: Link, columns: Sequence[str]
) -> None:
    """The identity columns of a link an EFFECTIVITY SATELLITE hangs off must be distinct.

    See the comment block above for what the repeat costs, and for why a link with no
    effectivity satellite is allowed to have one."""
    seen: dict[str, int] = {}
    for position, column in enumerate(columns):
        if column in seen:
            raise ValueError(
                f"link {link.name!r} takes its identity over {column!r} twice (hash "
                f"positions {seen[column]} and {position} of {tuple(columns)}), and "
                f"effectivity satellite {satellite.name!r} hangs off it. Those columns "
                "are the observation ledger's key, and ObservationGrain refuses a "
                "repeated key column -- so the grain that satellite requires cannot be "
                "built at all and the load would fail inside the job. Give the two ends "
                "distinct source columns (a LinkEnd.key_from names the column it reads), "
                "or drop the end that is not actually identifying. A link with no "
                "effectivity satellite may repeat: link_empresa_estabelecimento is "
                "hierarchical and does"
            )
        seen[column] = position


def _assert_no_gated_link_takes_its_identity_over_one_column_twice(
    tables: Mapping[str, VaultTable]
) -> None:
    """Refuse an unsatisfiable link/ledger grain at import rather than inside a job.

    Runs AFTER `_assert_every_effectivity_satellite_hangs_off_a_link`, so a parent that is
    missing or is not a link is already refused there with that message rather than here
    with a worse one."""
    for table in tables.values():
        if not isinstance(table, EffectivitySatellite):
            continue
        link = tables[table.parent]
        if not isinstance(link, Link):
            # Unreachable: the guard above raised on exactly this. Written as a narrow
            # rather than an `assert`, which `-O` strips.
            continue
        _refuse_a_repeated_identity_column(
            table, link, identity_columns_of(link, _link_hubs(tables, link))
        )


def _assert_every_effectivity_satellite_hangs_off_a_link(
    tables: Mapping[str, VaultTable]
) -> None:
    """Refuse an effectivity satellite whose parent is missing, is not a link, or whose
    entry column collides with a column the link already writes.

    THE COLLISION IS THE QUIET ONE, as it is for every other spec in this file. The
    loader writes the link's hash key, the DV2 metadata and the three effectivity
    columns; an entry column named like any of them keeps its name, loses its value,
    and leaves the delivered window open replaced by something we derived."""
    for table in tables.values():
        if not isinstance(table, EffectivitySatellite):
            continue
        parent = tables.get(table.parent)
        if parent is None:
            raise ValueError(
                f"effectivity satellite {table.name!r} names parent {table.parent!r}, "
                f"which no domain registers. Registered: {', '.join(sorted(tables))}"
            )
        if not isinstance(parent, Link):
            raise ValueError(
                f"effectivity satellite {table.name!r} names parent {table.parent!r}, "
                "which is not a link. An effectivity satellite records when a "
                "RELATIONSHIP held, so it hangs off the table that asserts the "
                "relationship; `load_effectivity_satellite` takes a Link and keys on "
                "its hash key, which a hub does not have"
            )
        written = {parent.hash_key: "the parent link's hash key"}
        written.update(
            {name: "a dependent-child key of the parent link"
             for name in parent.dependent_child_key_columns}
        )
        if table.entry_column in written:
            raise ValueError(
                f"effectivity satellite {table.name!r} names {table.entry_column!r} as "
                f"its entry column, and that is {written[table.entry_column]}. The "
                "window's open would be written into a key column, so the row would "
                "keep its shape and lose both values"
            )


def build_registry(domains: Iterable[VaultDomain]) -> Mapping[str, VaultTable]:
    """Every registered vault table by name, or refuse -- the whole-set guards.

    Returns a read-only mapping: the registry is data, and a caller who could
    `registry[...] = ...` could add a table that never passed a guard."""
    collected = list(domains)
    _assert_domain_names_are_unique(collected)
    tables = _collected_tables(collected)
    assert_every_satellite_hangs_off_a_hub_or_a_link(tables)
    _assert_every_link_joins_registered_hubs(tables)
    _assert_every_declared_key_derivation_fits_its_hub(tables)
    _assert_every_effectivity_satellite_hangs_off_a_link(tables)
    _assert_no_gated_link_takes_its_identity_over_one_column_twice(tables)
    return MappingProxyType(tables)


def parent_of(registry: Mapping[str, VaultTable], satellite: Satellite) -> Hub | Link:
    """The hub OR LINK a satellite hangs off.

    THE RESOLUTION `load_satellite` NEEDS SINCE F2 WAVE 2, and `parent_hub` below is now
    the NARROWER one rather than the only one. `build_registry` has already refused every
    way this could be wrong, so on a registry it produced the lookup cannot fail."""
    parent = registry[satellite.parent]
    if not isinstance(parent, Hub | Link):
        raise ValueError(
            f"{satellite.parent!r} is neither a hub nor a link, so satellite "
            f"{satellite.name!r} has no hash key to key on. build_registry refuses this, "
            "so a registry it produced cannot reach here"
        )
    return parent


def parent_hub(registry: Mapping[str, VaultTable], satellite: Satellite) -> Hub:
    """The HUB a satellite hangs off, refusing a satellite whose parent is a link.

    THE DOCSTRING HERE SAID "`build_registry` HAS ALREADY REFUSED EVERY WAY THIS COULD BE
    WRONG" AND F2 WAVE 2 MADE THAT FALSE. A link-parented satellite is now registrable,
    so this refusal is REACHABLE on a perfectly valid registry -- and it is reachable from
    outside this package: `opl.gold.registry_guards` calls it for every SCD2 dimension's
    source satellite, and `databricks/src/vault_load_satellite.py` calls it for whatever
    table its task names. Both would have got `'link_payment' is not a hub`, which names
    no consequence and no alternative. The refusal is kept rather than widened, because
    both of those callers really do need a HUB: an SCD2 dimension is a satellite's version
    chain hung on a hub's business key, and a PIT's spine is a hub's key set.

    `parent_of` is the resolution for a caller that can take either."""
    parent = registry[satellite.parent]
    if not isinstance(parent, Hub):
        raise ValueError(
            f"satellite {satellite.name!r} hangs off {satellite.parent!r}, which is not a "
            "hub -- this is a satellite on a LINK, which DV2 allows and this vault now "
            "registers. A caller that needs the hub's business key (an SCD2 dimension, a "
            "PIT spine) has no answer here; one that only needs the parent's hash key "
            "should resolve it with opl.vault.domains.parent_of"
        )
    return parent


def linked_hubs(registry: Mapping[str, VaultTable], link: Link) -> tuple[Hub, ...]:
    """The hubs a link joins, IN THE LINK'S DECLARATION ORDER.

    The order is the answer, not a detail of it: the link's hash key concatenates the
    hubs' business keys in this sequence, so a resolver returning them sorted or in
    registry order would silently re-key every link whose hubs are not alphabetical.

    `build_registry` has already refused every way this could be wrong, so on a
    registry it produced the resolution cannot fail."""
    return _link_hubs(registry, link)


def parent_link(registry: Mapping[str, VaultTable], satellite: EffectivitySatellite) -> Link:
    """The link an effectivity satellite hangs off. `build_registry` has already refused
    every way this could be wrong, so the lookup here cannot fail on a registry it
    produced."""
    parent = registry[satellite.parent]
    if not isinstance(parent, Link):
        raise ValueError(f"{satellite.parent!r} is not a link")
    return parent


def identifying_hubs(link: Link, hubs: Sequence[Hub]) -> tuple[Hub, ...]:
    """The hubs of `link`'s IDENTIFYING ends, in declaration order.

    ONE SPELLING OF THE FILTER, because two of them diverged. The first cut of
    `opl.vault.effectivity` keyed on `hubs[0]` while its own grain guard compared
    against `identity_columns_of`, which filters properly -- so a link with TWO
    identifying ends (wave 2's `link_payment` is exactly that shape) would pass the
    guard and then key the satellite on a digest that is not the link's hash key. Every
    join from satellite to link would return nothing, silently. `hubs` is EVERY end's
    hub, in the link's declaration order -- the list `linked_hubs` returns and the
    loaders take -- so the filtering happens here rather than at each call site."""
    return tuple(hub for end, hub in zip(link.ends, hubs, strict=True) if end.identifying)


def identity_columns_of(link: Link, hubs: Sequence[Hub]) -> tuple[str, ...]:
    """The SOURCE columns a link's own hash key is taken over, in hash order: each
    identifying end's hub business key, then the dependent-child keys.

    THIS IS THE LINK'S GRAIN, and it is what an observation ledger gating an
    effectivity satellite on this link has to be keyed on. Derived from the spec rather
    than restated wherever it is needed, so the two cannot drift: a ledger one column
    coarser than the link reports one departure for several relationships and the
    satellite closes windows that never departed.

    `hubs` is EVERY end's hub, in the link's declaration order -- the same list
    `linked_hubs` returns and the loaders take -- so the non-identifying ends are
    dropped here rather than by each caller.

    IT ASKS THE END, NOT THE HUB, SINCE F-DB, and that is what makes the grain right for
    a DERIVED end. The columns this returns are read off BRONZE -- the observation
    ledger's `_side` projects the source to exactly these names -- so for
    `link_merchant_empresa` the answer is `merchant_id` and `cnpj`, the columns the
    source has, and not `merchant_id` and `cnpj_basico`, which would be a ledger keyed on
    a column `bronze_merchant` does not carry. `LinkEnd.source_columns` answers `None`
    and a declaration in one place, so the two cannot drift; for every end declared
    before F-DB it returns the hub's own column names and this function is unchanged.

    NAMES ARE HALF THE GRAIN AND `identity_derivations_of` IS THE OTHER HALF -- read them
    together or the ledger is FINER than the link. `cnpj` is fourteen characters and the
    link hashes eight of them, so one link hash key has many `cnpj` values and a ledger
    keyed on the column alone reports a departure for a merchant that merely changed
    branch. Nothing in a column NAME can say that; see `identity_derivations_of`, which
    the observation grain carries beside these names and this function's own docstring
    once implied did not exist."""
    identifying = [
        (end, hub) for end, hub in zip(link.ends, hubs, strict=True) if end.identifying
    ]
    return tuple(
        [name for end, hub in identifying for name in end.source_columns(hub)]
        + list(link.dependent_child_key_columns)
    )


def link_identity_columns(registry: Mapping[str, VaultTable], link: Link) -> tuple[str, ...]:
    """`identity_columns_of` with the link's hubs resolved against the registry."""
    return identity_columns_of(link, _link_hubs(registry, link))


def identity_derivations_of(link: Link) -> tuple[KeyPrefix, ...]:
    """The derivations `identity_columns_of`'s column names are read THROUGH, in the same
    hash order: one `KeyPrefix` per identifying end that declares one, and nothing for an
    end that reads its hub's key by name.

    THE HALF OF THE GRAIN A COLUMN NAME CANNOT CARRY, and leaving it out is the defect
    this function exists to close. `link_merchant_empresa` keys on `substring(cnpj, 1, 8)`
    while its identity COLUMN is `cnpj`; `cnpj -> cnpj[:8]` is many-to-one, so an
    observation ledger keyed on the name alone is strictly FINER than the link -- and
    `effectivity._grain_key_mismatch`'s own docstring says what finer costs: "it closes
    windows that never departed". Measured: one merchant keeping its root and changing its
    full `cnpj` produced an active row AND a closing row on the same `applied_date` for
    the same hash key, with `appended=3, closed=1` in the run log.

    EMPTY FOR EVERY LINK WRITTEN BEFORE F-DB, which is what makes it free to carry: no end
    of either CNPJ link declares a `key_from` on the identifying side, so both grains stay
    byte-identical and `ObservationGrain` defaults to no derivation at all.

    DEPENDENT-CHILD KEYS CONTRIBUTE NOTHING and that is not an omission: they are read
    from the source under their own names, exactly as `identity_columns_of` appends them.

    NOT ZIPPED AGAINST THE NAMES POSITIONALLY. A `key_from` declares one prefix per
    business-key COMPONENT, so an end over a two-component hub contributes two names and
    two prefixes; the pairing that matters is by column, and `ObservationGrain` makes it
    by looking each prefix's column up in its key columns rather than by position.

    NO `hubs` AND NO REGISTRY, WHICH IS WHY THERE IS NO `link_identity_derivations` BESIDE
    `link_identity_columns`. A derivation is declared entirely on the END -- the hub is
    consulted only for the names an UNDECLARED end reads, which is the case that
    contributes nothing here -- so asking for hubs would be asking a caller for an
    argument this cannot check and does not use. Both grain-building sites already hold
    the `Link`."""
    return tuple(
        prefix for end in link.identifying_ends for prefix in (end.key_from or ())
    )


def table_spec(registry: Mapping[str, VaultTable], name: str) -> VaultTable:
    """The registered spec for `name`, or refuse naming the alternatives.

    Refuses BEFORE Spark, like `opl.bronze.registry.table_spec`: an operator who
    mistyped a table should not wait for a session to be told so."""
    try:
        return registry[name]
    except KeyError:
        raise UnknownVaultTable(
            f"unknown vault table {name!r} -- registered tables are: "
            f"{', '.join(sorted(registry))}. A table is registered by a module of "
            "opl.vault.domains binding DOMAIN; if this name is one you expect, check "
            "that its domain module declares it rather than that the caller mistyped it"
        ) from None


def discover_domains(search_paths: Sequence[str], package: str) -> tuple[VaultDomain, ...]:
    """Every `VaultDomain` declared by a module of `package`, in module-name order.

    ORDER IS SORTED AND THE SORT IS NOT LOAD-BEARING -- it is here so that a refusal
    message names the same domain on every machine. Nothing about the result depends
    on it, because `build_registry` sees every domain at once.

    UNDERSCORE-PREFIXED MODULES ARE SKIPPED, so the package can hold a shared helper
    without it having to pretend to be a domain.

    A MODULE WITHOUT `DOMAIN` IS REFUSED rather than skipped, and that is the whole
    difference between discovery and guesswork: a domain file whose constant is
    misspelled would otherwise be found, contribute nothing, and leave its tables
    quietly unregistered -- which surfaces later as `UnknownVaultTable` from whatever
    asked for the table, pointing at the caller rather than at the typo.

    All four behaviours above are pinned by `tests/vault/test_registry.py`: the two
    refusals in this loop, the empty-package refusal below, and the underscore skip."""
    found: list[VaultDomain] = []
    names = sorted(
        info.name
        for info in pkgutil.iter_modules(list(search_paths))
        if not info.name.startswith("_")
    )
    for name in names:
        module = importlib.import_module(f"{package}.{name}")
        domain = getattr(module, DOMAIN_ATTRIBUTE, None)
        if domain is None:
            raise ValueError(
                f"{package}.{name} is in the domains package and binds no "
                f"{DOMAIN_ATTRIBUTE}. Every module there is a vault domain and must "
                f"expose one, or its tables are silently unregistered; prefix the "
                "module with an underscore if it is a helper rather than a domain"
            )
        if not isinstance(domain, VaultDomain):
            raise TypeError(f"{package}.{name}.{DOMAIN_ATTRIBUTE} is not a VaultDomain")
        found.append(domain)
    if not found:
        raise ValueError(
            f"{package} contains no domain module. The registry would be empty and "
            "every lookup of any vault table would then refuse the name it was given"
        )
    return tuple(found)
