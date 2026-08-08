# src/opl/vault/specs.py
"""The five DV2 table-kind specs this vault knows: `Hub`, `Satellite`, `Link` (with
`LinkEnd` and `BusinessKeyColumn` as its component types), `EffectivitySatellite`,
`ReferenceTable`. Pure shape and per-table validation; the guards that need to see
the WHOLE registered set, `VaultDomain`, and everything that discovers or resolves
a domain live in `opl.vault.registry`, which imports every name below.

THE STRUCTURAL DECISION THIS MODULE IS, MADE IN TASK 6'S FIX ROUND RATHER THAN
DEFERRED. `registry.py` carried all five kinds inline through Task 5 and stood at
789 of the 800-line cap once Task 5 landed -- flagged there as "the next kind does
not fit." Task 6 confirmed it: `ReferenceTable`, at the argued-docstring standard
this repository holds every kind to, is 99 lines on its own, and the file had 11 to
give. `ReferenceTable` shipped in its own `registry_reference.py`, which closed
Task 6 but left `registry.py` at 799/800 -- one line of headroom, and review found
that too little: an ordinary docstring correction arising from Task 6's own review
had nowhere to land. Three shapes were weighed to fix that properly rather than push
the same problem one file further down the corridor:

  1. A `registry/` PACKAGE, one file per kind (`registry/hub.py`, `.../link.py`, ...)
     plus a `guards.py` and an `__init__.py` re-exporting everything. Rejected: the
     whole-set guards do not decompose by kind -- `_assert_every_link_joins_
     registered_hubs` reasons about `Link` AND `Hub` together, `_assert_every_
     effectivity_satellite_hangs_off_a_link` about `EffectivitySatellite` AND `Link`
     -- so a guards module would still import every kind, and seven files replace two
     for a repository with five kinds today and no concrete plan for a sixth. This is
     the shape to grow into if that count keeps climbing, not the one to pre-build.
  2. KEEP THE CURRENT SHAPE (`registry.py` + one `registry_<kind>.py` per new kind),
     with a stated rule for kind six. Rejected because it does not fix what review
     actually found broken: the growth pressure on `registry.py` is not only "one new
     kind" -- most of Task 6's own fix-round corrections are PROSE changes to
     `registry.py`'s existing paragraphs, and a file at 799/800 cannot absorb those
     regardless of where the next kind's class lives. Scattering five kind modules
     across `registry_hub.py`, `registry_satellite.py`, ... also makes the one
     comparison a reader of this vault's shapes most wants -- "how do these five
     kinds differ" -- cost five file opens instead of one scroll.
  3. ONE MODULE FOR EVERY KIND'S SHAPE, GUARDS STAYING IN `registry.py`. CHOSEN. It
     is a real boundary, not an arithmetic one: everything here is checkable about
     ONE table in isolation, before Spark and before any registry exists (the
     `__post_init__` on every class below); everything that needs the other tables
     stays in `registry.py`, which is `opl.bronze.registry`'s own split between a
     spec and its cross-referential guards. `registry.py` sheds the ~320 lines these
     five classes cost and keeps its own prose room to breathe -- corrected,
     extended, or argued with -- for a long time before this module's own headroom
     (five kinds at ~60-120 lines each leaves room for several more before the same
     pressure repeats) becomes the next thing to watch.

THE STATED RULE FOR KIND SIX, so the next task does not have to make this decision
again: a new table kind's dataclass and its `__post_init__` land HERE, in this
module, appended after `ReferenceTable`; only its WHOLE-SET guard (if it needs one --
`ReferenceTable` shows a kind need not) and its word in the `VaultTable` union land in
`registry.py`. If a kind ever needs its own whole-set guard AND this module is
itself near the cap when it arrives, split that one kind out the way `ReferenceTable`
briefly was -- but that is a decision for the kind that actually forces it, argued
against this module's line count at the time, not assumed now."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

from opl.vault.columns import EFFECTIVITY_COLUMNS, METADATA_COLUMNS


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
        """Every end, in declaration order, normalised to `LinkEnd`.

        `hubs` is DECLARED as what a caller may write and HOLDS what `__post_init__`
        normalised it to; the cast says so once, here, rather than leaving every reader
        of `link.hubs` to work out which of the two they have."""
        return cast("tuple[LinkEnd, ...]", self.hubs)

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


@dataclass(frozen=True, kw_only=True)
class ReferenceTable:
    """A DV2 reference table: a natural key, a payload column, and the bronze
    `lookup_type` that routes rows to it -- no hub, no hash key, no whole-set guard.
    Task 6's kind; see this module's docstring for where it landed and why.

    WHAT A REFERENCE TABLE ROW ASSERTS, for the reason `opl.vault.hubs` states it for a
    hub: "this code means this description", nothing about when or whether it changed.
    CNAE, município, natureza jurídica, motivo, qualificação de sócio and país (see
    `opl/vault/domains/cnpj.py` for why the sixth is modelled alongside the brief's
    five) are RFB code lists -- `codigo` -> `descricao` -- closed and versioned by the
    RFB itself, not evolved by anything this vault observes.

    NO HUB, BECAUSE THE NATURAL KEY IS ALREADY THE IDENTITY. A hub exists to give a
    business key a STABLE SURROGATE that a link or a satellite can reference without
    repeating a wide or composite key. `codigo` is neither: one short column, already
    unique within its own type (Task 6's whole trap is that it is NOT unique across
    types -- see `opl.vault.reference`), and nothing in this vault joins to it through a
    digest. Hashing it would add a column carrying no information the natural key does
    not, for a join nothing here performs.

    NO HASH KEY, FOR THE SAME REASON AND STATED AS ITS OWN DECISION, because `Hub`,
    `Link` and `EffectivitySatellite` all make one a FIELD: a hash key exists to give a
    business key a fixed-width, collision-resistant join column, and a reference
    table's own natural key already IS that column for anything that will join to it.
    Adding one here would be a second, unused spelling of `codigo`.

    NO WHOLE-SET GUARD IN `build_registry`, AND THAT IS A FINDING RATHER THAN AN
    OMISSION. Every guard in `registry.py` exists because a satellite, a link or an
    effectivity satellite names ANOTHER TABLE BY STRING, and the whole set has to be
    seen at once to catch a name nobody registers. `ReferenceTable` names no other
    table -- no `parent`, no `hubs`, nothing to resolve -- so there is nothing for a
    whole-set guard to check that `__post_init__` below has not already refused.
    `VaultDomain.__post_init__` and the three `isinstance` guards in `registry.py` all
    read `VaultTable` rather than restating the union they refuse or admit, so this
    kind needed no new guard function there at all -- only the word in the union.

    `natural_key` AND `payload` ARE `str`, NOT `Sequence[str]`, AND THAT IS NOT A
    SHORTCUT. `opl.contracts.cnpj_schemas.TABLES['lookup']` is `['codigo', 'descricao']`
    -- two columns, fixed, because that is the whole shape of an RFB lookup CSV row --
    so a `Sequence[str]` here would model a generality this contract cannot produce, the
    same argument `opl.vault.hashing`'s empty-`components` refusal makes about a
    zero-length business key: once a caller has consumed the answer, the wrong shape
    looks like a modelling decision instead of a bug. Widening it is a deliberate edit
    the day a reference source with a wider row arrives, not a defensive default now.

    `lookup_type` NAMES WHICH SLICE OF `bronze_cnpj_lookup` THIS TABLE READS, and it is
    validated as a non-empty string here and nowhere stronger: this module does not
    import `opl.bronze.lookup_routing`, so the registry MECHANISM stays bronze-agnostic
    the way `opl.vault.registry` already is. `opl/vault/domains/cnpj.py` sets it FROM
    `opl.bronze.lookup_routing.LOOKUP_SUFFIX` rather than retyping the six strings, and
    `opl.vault.reference` is where the type actually routes rows -- by calling
    `lookup_type_from_filename`, not a second spelling of it -- and where the
    motivo/qualificação collision this table exists to prevent is actually closed."""

    name: str
    lookup_type: str
    natural_key: str
    payload: str

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("a reference table needs a name")
        if not self.lookup_type or not self.lookup_type.strip():
            raise ValueError(f"reference table {self.name!r} needs a lookup_type")
        for role, column in (("natural key", self.natural_key), ("payload", self.payload)):
            if not isinstance(column, str) or not column.strip():
                raise ValueError(
                    f"reference table {self.name!r} needs a {role} column name, got "
                    f"{column!r}"
                )
        if self.natural_key == self.payload:
            raise ValueError(
                f"reference table {self.name!r} names {self.natural_key!r} as both "
                "its natural key and its payload column -- the write would put the "
                "description where the key belongs, or the key where the "
                "description belongs, depending which is projected last"
            )
        reserved = METADATA_COLUMNS & {self.natural_key, self.payload}
        if reserved:
            raise ValueError(
                f"reference table {self.name!r} names {sorted(reserved)} as its "
                "natural key or payload, and the loader writes those itself "
                f"({', '.join(sorted(METADATA_COLUMNS))}). The source's own value "
                "would be silently overwritten by the metadata on the write"
            )


# THE UNION EVERY WHOLE-SET GUARD AND `VaultDomain.__post_init__` READS RATHER THAN
# RESTATES, so a kind added above extends every refusal that matters in this one
# edit. `registry.py` imports this name and nothing constructs it independently.
VaultTable = Hub | Satellite | Link | EffectivitySatellite | ReferenceTable
