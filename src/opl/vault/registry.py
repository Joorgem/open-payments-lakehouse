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
kind and its guards land in this file, exactly as `Link` did in Task 4 and
`EffectivitySatellite` in Task 5, which is an edit inside WAVE 1 and is what the plan
always said would happen. `test_a_new_domain_of_hubs_satellites_and_links_is_
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
everything checkable about one table is refused in its `__post_init__`, before Spark
and before any registry exists; everything that needs to see the other tables is
refused in `build_registry`, which `domains/__init__.py` calls at import so a
malformed registry breaks the import of every module that reads it rather than the
one job that touches that table.

WHAT TASK 5 ADDED, AND WHY EACH OF THE THREE WAS WORTH A NEW CONCEPT RATHER THAN A
WORKAROUND. All three were predicted here by name in Task 4 and left out until the data
said what shape they had.

  - `LinkEnd`, carrying a ROLE. `link_company_partner` references `hub_empresa` at both
    ends -- a company and a partner that is itself a company -- and Task 4's `Link`
    refused a repeated hub precisely because it had no role name and both references
    would have gone into one column. The role prefixes the reference column.
  - DEPENDENT-CHILD KEYS on a link. The measured sócio grain is (`cnpj_basico`,
    `identificador_socio`, `cpf_cnpj_socio`), whose last two components belong to NO
    hub: the RFB masks a partner's CPF to six middle digits, so a hub on it would merge
    ~27 unrelated people per key. They are key components stored on the link, which is
    the idiom the master spec itself chooses for `transaction_id` on `link_payment`.
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
module and in the job task and nowhere in this layer."""
from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

from opl.vault.columns import EFFECTIVITY_COLUMNS, METADATA_COLUMNS

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
class LinkEnd:
    """One end of a link: the hub it references, the ROLE it plays in the relationship,
    and whether that reference is part of the link's identity.

    THE ROLE IS WHAT MAKES A SELF-REFERENCING LINK EXPRESSIBLE, and Task 4 predicted
    needing it: `link_company_partner` references `hub_empresa` at both ends -- a
    company and a partner that is itself a company -- and without a role both would be
    written into one column named after that hub's hash key, so one end of the
    relationship would silently be gone. `reference_column` prefixes the role, so the
    two ends are `company_hub_empresa_hk` and `partner_hub_empresa_hk`. A role of
    `None` keeps the hub's own hash-key name, which is what every single-role link
    wants and what `link_empresa_estabelecimento` has always had.

    `identifying=False` MARKS A REFERENCE THE LINK RESOLVES RATHER THAN ONE IT IS
    IDENTIFIED BY, and this is a real distinction rather than a flag. The partner
    company's `cnpj_basico` is the first eight characters of `cpf_cnpj_socio`, which is
    already a dependent-child key of the link -- so the reference is a FUNCTION of the
    identity, not a part of it. Hashing it as well would make the link's own key depend
    on a value we derived where every other component is one the source delivered, and
    would change that key the day the derivation changed."""

    hub: str
    role: str | None = None
    identifying: bool = True

    def __post_init__(self) -> None:
        if not self.hub or not self.hub.strip():
            raise ValueError("a link end needs a hub name")
        if self.role is not None and not self.role.strip():
            raise ValueError(
                f"the link end on hub {self.hub!r} declares an empty role. A role names "
                "the part this hub plays and prefixes its reference column; pass None "
                "for an end that has no role rather than a blank one"
            )

    def reference_column(self, hub: Hub) -> str:
        """The column this end's hash-key reference is written into."""
        return hub.hash_key if self.role is None else f"{self.role}_{hub.hash_key}"


@dataclass(frozen=True, kw_only=True)
class Link:
    """A DV2 link: the hubs whose relationship it records, BY NAME, its dependent-child
    keys, and its own hash key.

    HUBS BY NAME AND NOT BY VALUE, which is `Satellite.parent`'s decision for
    `Satellite.parent`'s reason: a spec holding `Hub` objects could only name hubs its
    own module had already constructed, and the whole point of the per-domain shape is
    that `build_registry` sees every domain at once. `linked_hubs` resolves them and
    the whole-set guard below refuses a name no domain declares, so the spec and the
    hubs cannot disagree. An entry may be a bare hub name or a `LinkEnd`; the bare name
    is normalised to `LinkEnd(hub=name)`, so the simple case stays one word.

    ORDER IS THE LINK'S IDENTITY, not a listing convention. The link's hash key is the
    business-key standard applied to the identifying ends' business keys CONCATENATED
    IN THIS ORDER, then the dependent-child keys
    (`opl.vault.loading.link_hash_key_expression`), so swapping two ends re-keys the
    whole table. `_refuse_mismatched_hubs` in the loader is what stops a caller
    supplying them in another order.

    DEPENDENT-CHILD KEYS ARE KEY COMPONENTS THAT BELONG TO NO HUB, and Task 5 is where
    the shape was known well enough to add them. The measured sócio grain is
    (`cnpj_basico`, `identificador_socio`, `cpf_cnpj_socio`), whose last two components
    identify no business object this vault has a hub for: the RFB masks a partner's CPF
    to six middle digits, so its key space is 10^6 and 99.99% occupied and a hub on it
    would merge ~27 unrelated people per key. They are stored on the link and hashed
    into its key, which is the idiom the master spec itself chooses for `transaction_id`
    on `link_payment`. See ADR 0011.

    NO PAYLOAD AND NO `applied_date`. A link row asserts "this relationship exists",
    the same kind of statement a hub row makes about a key -- descriptive facts about
    the relationship, and the window in which it held, belong to a satellite on the
    link, which is now `EffectivitySatellite` below."""

    name: str
    hash_key: str
    hubs: Sequence[str | LinkEnd]
    dependent_child_keys: Sequence[BusinessKeyColumn] = ()

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
        ends = tuple(
            end if isinstance(end, LinkEnd) else LinkEnd(hub=end) for end in self.hubs
        )
        self._refuse_too_few_identity_components(ends)
        if len(self.dependent_child_keys) and any(
            not isinstance(key, BusinessKeyColumn) for key in self.dependent_child_keys
        ):
            raise TypeError(
                f"link {self.name!r} must declare its dependent-child keys as "
                "BusinessKeyColumn values -- a bare column name cannot carry the "
                "zero-pad width, and an unpadded key component matches nothing"
            )
        if self.dependent_child_keys:
            _validated_columns(
                [key.name for key in self.dependent_child_keys],
                owner=f"link {self.name!r}",
                role="dependent-child key",
            )
        object.__setattr__(self, "hubs", ends)
        object.__setattr__(self, "dependent_child_keys", tuple(self.dependent_child_keys))

    def _refuse_too_few_identity_components(self, ends: tuple[LinkEnd, ...]) -> None:
        """A link records a RELATIONSHIP, so it needs at least two things to relate.

        THE OLD RULE WAS "AT LEAST TWO HUBS" AND IT WAS TOO NARROW, not merely stricter.
        Its argument was that a one-hub link is that hub's own business key hashed a
        second time under another name -- true when a link had nothing but hubs, and
        false the moment dependent-child keys exist: `link_company_partner` has one hub
        and two dependent-child keys, and its key space is the partnership, not the
        company. What still has to hold is that SOMETHING is being related, and that at
        least one hub anchors it -- a link of dependent-child keys alone would be a hub
        wearing a link's name."""
        identifying = [end for end in ends if end.identifying]
        if not identifying:
            raise ValueError(
                f"link {self.name!r} has no identifying end. Every reference it carries "
                "would be one it resolves rather than one it is keyed on, so its hash "
                "key would be taken over the dependent-child keys alone and the link "
                "would not be anchored to any hub"
            )
        components = len(identifying) + len(self.dependent_child_keys)
        if components < 2:
            raise ValueError(
                f"link {self.name!r} is keyed on {components} component -- a link "
                "records a RELATIONSHIP and needs at least two. With one it is that "
                "hub's own business key hashed a second time under another name: two "
                "tables that look independent and are the same key space"
            )

    @property
    def ends(self) -> tuple[LinkEnd, ...]:
        """Every end, in declaration order, normalised to `LinkEnd`."""
        return tuple(self.hubs)  # type: ignore[arg-type]

    @property
    def identifying_ends(self) -> tuple[LinkEnd, ...]:
        """The ends whose hub business key is part of the link's own hash key."""
        return tuple(end for end in self.ends if end.identifying)

    @property
    def hub_names(self) -> tuple[str, ...]:
        """Every end's hub name, in declaration order. A hub may appear twice."""
        return tuple(end.hub for end in self.ends)

    @property
    def dependent_child_key_columns(self) -> tuple[str, ...]:
        """Just the dependent-child key column names, in declaration order -- the order
        they are hashed in, so it is not incidental."""
        return tuple(key.name for key in self.dependent_child_keys)


@dataclass(frozen=True, kw_only=True)
class EffectivitySatellite:
    """A satellite on a LINK, recording when the relationship it hangs off was
    effective: one row per link hash key per change of `is_active`.

    A FOURTH TABLE KIND RATHER THAN A `Satellite` WITH A LINK PARENT, and the two
    reasons are the same ones `_assert_every_satellite_hangs_off_a_hub` gives for
    refusing that shape. A `Satellite` is delta-driven on a `hash_diff` over a payload
    and `load_satellite` takes a `Hub`; this table has no payload, no `hash_diff`, and
    is driven by the observation ledger instead. Registering it as a `Satellite` would
    make it a table `load_satellite` would key on a column its parent does not have.

    `entry_column` IS THE WINDOW'S OPEN AND IT KEEPS THE SOURCE'S OWN NAME, which is
    the one piece of epistemics this spec carries. The open is DELIVERED --
    `data_entrada_sociedade` is populated on 100% of 2026-07's rows with no `00000000`
    sentinel -- and the close is DERIVED by us from an absence. Carrying the delivered
    value under the column name the RFB gave it, beside `last_observed_on` and
    `closed_by` which are ours and are named in our vocabulary, is what stops a reader
    taking the two for claims of the same strength. See ADR 0011."""

    name: str
    parent: str
    entry_column: str

    def __post_init__(self) -> None:
        if not self.name or not self.parent or not self.entry_column:
            raise ValueError(
                f"an effectivity satellite needs a name, a parent link and an entry "
                f"column ({self.name!r})"
            )
        reserved = METADATA_COLUMNS | EFFECTIVITY_COLUMNS
        if self.entry_column in reserved:
            raise ValueError(
                f"effectivity satellite {self.name!r} names {self.entry_column!r} as "
                f"its entry column, and the loader writes that itself "
                f"({', '.join(sorted(reserved))}). The source's delivered window open "
                "would be replaced by our own value without anything failing"
            )


VaultTable = Hub | Satellite | Link | EffectivitySatellite


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
    identifying = [
        hub for end, hub in zip(link.ends, hubs, strict=True) if end.identifying
    ]
    return tuple(
        [name for hub in identifying for name in hub.business_key_columns]
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
