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
from HUBS, SATELLITES AND LINKS is "+1 file, 0 modified": `VaultTable =
Hub | Satellite | Link` and `VaultDomain.__post_init__` refuses anything else, so
those three kinds need nothing added here. That is wave 2's whole list -- its
`hub_account` and `hub_customer` are hubs with satellites and its `link_payment` is a
link -- so the claim the plan stakes is now covered kind for kind rather than by
analogy. A domain introducing a FOURTH table kind still does not clear that bar: the
kind and its guards land in this file, exactly as `Link` did in Task 4, which is an
edit inside WAVE 1 and is what the plan always said would happen. `test_a_new_domain_
of_hubs_satellites_and_links_is_discovered_without_editing_any_file` builds a
throwaway domain carrying wave 2's three tables by name and registers it.

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
everything checkable about one table is refused in its `__post_init__`, before Spark
and before any registry exists; everything that needs to see the other tables is
refused in `build_registry`, which `domains/__init__.py` calls at import so a
malformed registry breaks the import of every module that reads it rather than the
one job that touches that table.

WHAT IS DELIBERATELY NOT HERE, NOW THAT `Link` IS. No SATELLITE ON A LINK. DV2 has
them, and Task 5's effectivity satellite is the first candidate -- but `Satellite`
resolves its hash key through `parent_hub`, which returns a `Hub`, and
`opl.vault.satellites.load_satellite` takes a `Hub`. Registering one here without the
loader to match would register a table nothing in this package can write, so
`_assert_every_satellite_hangs_off_a_hub` refuses it and its message says the two have
to move together.

No DEPENDENT-CHILD KEY on a link either, and that is the same refusal to guess that
kept `Link` out of Task 3. The measured sócio grain is (`cnpj_basico`,
`identificador_socio`, `cpf_cnpj_socio`), whose last two components belong to NO hub --
a hub on the masked CPF would merge ~27 people per key, which is why the plan removed
`hub_socio`. So `link_company_partner` needs a key component that is not a hub
reference, and Task 5 is where that shape is known. `Link` below joins hubs and nothing
else; a link that needs more says so by not fitting.

No table QUALIFICATION either -- a spec carries an unqualified `name`, and the loaders
take the qualified table as an argument, so `opl.config` is consulted in the domain
module and in the job task and nowhere in this layer."""
from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

from opl.vault.columns import METADATA_COLUMNS

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
class BusinessKeyColumn:
    """One column of a business key, and the fixed width it is padded to.

    `width=None` MEANS "TAKE THE VALUE AS IT IS", not "width unknown". Zero-padding is
    a claim about a column's canonical form -- `cnpj_basico` is eight characters, so a
    seven-character value read from a source that dropped a leading zero is the SAME
    key -- and that claim is false for a name or a free-text identifier, where padding
    would invent characters. Only a caller who knows the width may assert one.

    `kw_only`, like `opl.bronze.registry.BronzeTable`: `name` and `width` are adjacent
    and a positional construction that swapped them would be a type error today and a
    silent mis-padding the day a width becomes a string."""

    name: str
    width: int | None = None

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("a business-key column needs a name")
        if self.width is not None and self.width <= 0:
            raise ValueError(
                f"business-key column {self.name!r} declares width {self.width!r}. It "
                "must be a positive integer: a width of zero pads every value to the "
                "empty string, which collapses the whole hub onto one hash key"
            )


def _validated_columns(columns: Sequence[str], *, owner: str, role: str) -> tuple[str, ...]:
    """`columns` as a frozen tuple, or refuse -- the three mistakes a column list makes.

    Shared by the hub's business key and the satellite's payload because the three are
    the same mistakes in both places, and a second copy would be a second thing to keep
    in step with `METADATA_COLUMNS`."""
    if isinstance(columns, str):
        raise TypeError(
            f"{owner} received a bare str {columns!r} as its {role} -- a str is a "
            "Sequence[str] structurally, so no type checker catches this and it "
            f"iterates to one column per CHARACTER; pass a tuple, e.g. ({columns!r},)"
        )
    frozen = tuple(columns)
    if not frozen:
        raise ValueError(f"{owner} names no {role} column -- it needs at least one")
    if len(set(frozen)) != len(frozen):
        raise ValueError(f"{owner} names a {role} column more than once ({frozen})")
    reserved = sorted(set(frozen) & METADATA_COLUMNS)
    if reserved:
        raise ValueError(
            f"{owner} names {reserved} as a {role} column, and the loaders write "
            f"those themselves ({', '.join(sorted(METADATA_COLUMNS))}). The collision "
            "does not crash: the metadata value wins on the write, so the source's "
            "own value disappears and the column is still there, full of plausible "
            "numbers. Rename the column in the spec"
        )
    return frozen


@dataclass(frozen=True, kw_only=True)
class Hub:
    """A DV2 hub: a business key, its hash key, and nothing else.

    Per master spec section 4.2 the loaded table also carries `load_date` (LDTS) and
    `record_source` (RSRC); those are not fields here because they are not
    per-table decisions -- every hub carries them, and `opl.vault.columns` names them
    once."""

    name: str
    hash_key: str
    business_keys: Sequence[BusinessKeyColumn]

    def __post_init__(self) -> None:
        if not self.name or not self.hash_key:
            raise ValueError(f"a hub needs a name and a hash-key column ({self.name!r})")
        keys = tuple(self.business_keys)
        if any(not isinstance(key, BusinessKeyColumn) for key in keys):
            raise TypeError(
                f"hub {self.name!r} must declare its business key as "
                "BusinessKeyColumn values -- a bare column name cannot carry the "
                "zero-pad width, and a hub whose key is not padded to its canonical "
                "width matches nothing"
            )
        names = _validated_columns(
            [key.name for key in keys], owner=f"hub {self.name!r}", role="business-key"
        )
        if self.hash_key in names:
            raise ValueError(
                f"hub {self.name!r} names {self.hash_key!r} as both its hash key and a "
                "business-key column. The write would put the digest where the "
                "business key belongs: right row count, right column names, and the "
                "key it was derived from gone"
            )
        object.__setattr__(self, "business_keys", keys)

    @property
    def business_key_columns(self) -> tuple[str, ...]:
        """Just the column names, in declaration order -- the order the hash is taken
        in, so it is not incidental."""
        return tuple(key.name for key in self.business_keys)


@dataclass(frozen=True, kw_only=True)
class Satellite:
    """A DV2 satellite: a parent hub, and the payload whose change it records.

    NO HASH-KEY FIELD, DELIBERATELY. A satellite's hash key IS its parent's, and a
    satellite free to spell it independently is a satellite a typo can point at
    nothing -- silently, as an empty join rather than an error. `parent_hub` resolves
    it, and `build_registry` refuses a parent that is not a registered hub, so the two
    cannot disagree."""

    name: str
    parent: str
    payload_columns: Sequence[str]

    def __post_init__(self) -> None:
        if not self.name or not self.parent:
            raise ValueError(f"a satellite needs a name and a parent ({self.name!r})")
        object.__setattr__(
            self,
            "payload_columns",
            _validated_columns(
                self.payload_columns, owner=f"satellite {self.name!r}", role="payload"
            ),
        )


@dataclass(frozen=True, kw_only=True)
class Link:
    """A DV2 link: the hubs whose relationship it records, BY NAME, and its own hash
    key.

    HUBS BY NAME AND NOT BY VALUE, which is `Satellite.parent`'s decision for
    `Satellite.parent`'s reason: a spec holding `Hub` objects could only name hubs its
    own module had already constructed, and the whole point of the per-domain shape is
    that `build_registry` sees every domain at once. `linked_hubs` resolves them and
    the whole-set guard below refuses a name no domain declares, so the spec and the
    hubs cannot disagree.

    ORDER IS THE LINK'S IDENTITY, not a listing convention. The link's hash key is the
    business-key standard applied to the participating hubs' business keys CONCATENATED
    IN THIS ORDER (`opl.vault.loading.link_hash_key_expression`), so swapping two hubs
    re-keys the whole table. `_refuse_mismatched_hubs` in the loader is what stops a
    caller supplying them in another order.

    NO PAYLOAD AND NO `applied_date`. A link row asserts "this relationship exists",
    the same kind of statement a hub row makes about a key -- descriptive facts about
    the relationship, and the window in which it held, belong to a satellite on the
    link, which this vault does not have yet (see the module docstring)."""

    name: str
    hash_key: str
    hubs: Sequence[str]

    def __post_init__(self) -> None:
        if not self.name or not self.hash_key:
            raise ValueError(f"a link needs a name and a hash-key column ({self.name!r})")
        if isinstance(self.hubs, str):
            raise TypeError(
                f"link {self.name!r} received a bare str {self.hubs!r} as its hubs -- a "
                "str is a Sequence[str] structurally, so no type checker catches this "
                "and it iterates to one hub name per CHARACTER; pass a tuple, e.g. "
                f"({self.hubs!r},)"
            )
        hubs = tuple(self.hubs)
        if len(hubs) < 2:
            raise ValueError(
                f"link {self.name!r} names {hubs} -- a link records a RELATIONSHIP and "
                "needs at least two hubs. With one it is that hub's own business key "
                "hashed a second time under another name: two tables that look "
                "independent and are the same key space"
            )
        if len(set(hubs)) != len(hubs):
            raise ValueError(
                f"link {self.name!r} names the same hub twice ({hubs}). A same-hub "
                "hierarchical link is real DV2 and this spec cannot express one: it "
                "carries no ROLE name, so both references would be written into a "
                "single column named after that hub's hash key and one end of the "
                "relationship would silently be gone. Add roles when the first link "
                "that needs them arrives"
            )
        object.__setattr__(self, "hubs", hubs)


VaultTable = Hub | Satellite | Link


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

    Shared by the whole-set guard and by `linked_hubs` so the refusal and the lookup
    are the same code: a resolver that repeated the guard's conditions in a weaker
    form is how a registry that passed its guards still returns something wrong."""
    resolved: list[Hub] = []
    for name in link.hubs:
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
    joining to nothing."""
    for table in tables.values():
        if not isinstance(table, Link):
            continue
        hubs = _link_hubs(tables, table)
        written: dict[str, str] = {table.hash_key: f"link {table.name!r}'s own hash key"}
        for hub in hubs:
            if hub.hash_key in written:
                raise ValueError(
                    f"link {table.name!r} would write {hub.hash_key!r} twice: it is "
                    f"hub {hub.name!r}'s hash key and also {written[hub.hash_key]}. "
                    "One column, two values -- the row count stays right and one end "
                    "of the relationship points at the wrong digest"
                )
            written[hub.hash_key] = f"hub {hub.name!r}'s hash key"


def build_registry(domains: Iterable[VaultDomain]) -> Mapping[str, VaultTable]:
    """Every registered vault table by name, or refuse -- the whole-set guards.

    Returns a read-only mapping: the registry is data, and a caller who could
    `registry[...] = ...` could add a table that never passed a guard."""
    collected = list(domains)
    _assert_domain_names_are_unique(collected)
    tables = _collected_tables(collected)
    _assert_every_satellite_hangs_off_a_hub(tables)
    _assert_every_link_joins_registered_hubs(tables)
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


def table_spec(registry: Mapping[str, VaultTable], name: str) -> VaultTable:
    """The registered spec for `name`, or refuse naming the alternatives.

    Refuses BEFORE Spark, like `opl.bronze.registry.table_spec`: an operator who
    mistyped a table should not wait for a session to be told so."""
    try:
        return registry[name]
    except KeyError:
        raise UnknownVaultTable(
            f"unknown vault table {name!r} -- registered tables are: "
            f"{', '.join(sorted(registry))}. Every vault job task takes the table "
            "name as a parameter; check the `table` parameter of the job that failed"
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
    quietly unregistered -- which surfaces later as `UnknownVaultTable` from a job,
    pointing at the job rather than at the typo."""
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
            "every vault job would refuse its own table name"
        )
    return tuple(found)
