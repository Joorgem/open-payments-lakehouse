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
    `Satellite` with a link parent, for the reason
    `_assert_every_satellite_hangs_off_a_hub` still gives: a `Satellite` is delta-driven
    on a `hash_diff` over a payload and `load_satellite` takes a `Hub`. This table has
    neither. That refusal therefore stands unchanged and now names the alternative.

WHAT IS STILL DELIBERATELY NOT HERE. No DESCRIPTIVE satellite on a link, which is a
different table from the effectivity one and would need `load_satellite` to take a link;
socios' `qualificacao_socio` and `faixa_etaria` are its first candidates and are
declared as unmodelled in `domains/cnpj.py` rather than left as a gap. No per-end KEY
COLUMN MAP either: a `LinkEnd` reads its hub's business key from the columns named after
it, and `link_company_partner`'s partner end -- whose `cnpj_basico` socios carries only
as the first eight characters of `cpf_cnpj_socio` -- is derived by
`opl.vault.partners`, which is the one loader in this package that is domain-specific
and says so.

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

from opl.vault.specs import (
    BusinessKeyColumn,
    EffectivitySatellite,
    Hub,
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
    "BusinessKeyColumn",
    "EffectivitySatellite",
    "Hub",
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
    "link_identity_columns",
    "linked_hubs",
    "parent_hub",
    "parent_link",
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


def _assert_every_satellite_hangs_off_a_hub(tables: Mapping[str, VaultTable]) -> None:
    """Refuse a satellite whose parent is missing, or is not a hub, or whose payload
    collides with the hub's hash key.

    All three need the other tables, which is why they are here and not in
    `Satellite.__post_init__`. The payload collision is the quiet one: the hash key
    is written by the loader, so a payload column of the same name loses its source
    value to the digest and keeps looking like a column."""
    for table in tables.values():
        if not isinstance(table, Satellite):
            continue
        parent = tables.get(table.parent)
        if parent is None:
            raise ValueError(
                f"satellite {table.name!r} names parent {table.parent!r}, which no "
                f"domain registers. Registered: {', '.join(sorted(tables))}"
            )
        if not isinstance(parent, Hub):
            raise ValueError(
                f"satellite {table.name!r} names parent {table.parent!r}, which is "
                "not a hub. A satellite in THIS vault hangs off a hub: `parent_hub` "
                "returns a Hub and `load_satellite` takes one, so a satellite "
                "parented on another satellite would key on a column its parent does "
                "not have, and one parented on a LINK -- which DV2 does allow -- would "
                "be a registered table nothing in this package can write. The guard "
                "and that signature have to change together"
            )
        if parent.hash_key in table.payload_columns:
            raise ValueError(
                f"satellite {table.name!r} names {parent.hash_key!r} as a payload "
                f"column, and that is its parent hub {parent.name!r}'s hash key. The "
                "loader writes the digest into that column, so the payload value "
                "would be replaced by it without anything failing"
            )


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

    The sibling of `_assert_every_satellite_hangs_off_a_hub`, and here for its reason:
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
    _assert_every_satellite_hangs_off_a_hub(tables)
    _assert_every_link_joins_registered_hubs(tables)
    _assert_every_effectivity_satellite_hangs_off_a_link(tables)
    return MappingProxyType(tables)


def parent_hub(registry: Mapping[str, VaultTable], satellite: Satellite) -> Hub:
    """The hub a satellite hangs off. `build_registry` has already refused every way
    this could be wrong, so the lookup here cannot fail on a registry it produced."""
    parent = registry[satellite.parent]
    if not isinstance(parent, Hub):
        raise ValueError(f"{satellite.parent!r} is not a hub")
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
    dropped here rather than by each caller."""
    return tuple(
        [name for hub in identifying_hubs(link, hubs) for name in hub.business_key_columns]
        + list(link.dependent_child_key_columns)
    )


def link_identity_columns(registry: Mapping[str, VaultTable], link: Link) -> tuple[str, ...]:
    """`identity_columns_of` with the link's hubs resolved against the registry."""
    return identity_columns_of(link, _link_hubs(registry, link))


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
