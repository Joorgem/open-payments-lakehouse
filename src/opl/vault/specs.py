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


# HOW A SATELLITE'S `applied_date` IS READ OUT OF THE COLUMN IT COMES FROM. Two readings,
# a closed set, and the vocabulary is `opl.gold.spec_fields.ROLE_READERS`' -- which asks
# the identical question one layer up ("is this column a DATE this pipeline derived, or
# ISO text the source delivered?") and answers it with `READS_DATE` / `READS_ISO_TEXT`.
#
# MIRRORED AND NOT IMPORTED, WHICH IS A LAYER DECISION RATHER THAN A DUPLICATE OVERLOOKED.
# The vault must not import gold: `opl.gold.registry_guards` imports `opl.vault.domains`,
# so the edge already runs the other way and importing back would be a cycle. What keeps
# the two honest is not a shared string but a shared ANSWER --
# `tests/vault/test_payments_satellite.py` asserts that this module's ISO reading and
# `opl.gold.conformed.day_of` return the same day for the same value, which is the
# property that actually matters and which a shared constant would not have given.
READS_DATE = "date"
READS_ISO_TEXT = "iso-instant-text"
APPLIED_DATE_READERS = (READS_DATE, READS_ISO_TEXT)


@dataclass(frozen=True, kw_only=True)
class AppliedDateSource:
    """WHERE A SATELLITE'S `applied_date` COMES FROM: a source column, plus the rule for
    reading a calendar day out of it.

    WHY THIS IS A DECLARATION AND NOT THE CONSTANT IT WAS. `satellite_candidates` built
    `applied_date` from `opl.bronze.snapshot.SNAPSHOT_REF_DATE_COLUMN` unconditionally,
    and that column IS NOT ON EVERY BRONZE TABLE. `add_common_audit_columns` omits it for
    a GENERATED or API-FED source, deliberately and with the reason written in four
    places (`opl.bronze.autoloader`, `opl.bronze.snapshot`'s third-derivation block,
    `opl.bronze.rules`' payments set, `opl.dataops.cadence`): the ref date is "the date
    the source declares in its own filename", a generated stream declares none, and
    stamping an all-NULL column would have forced the payments rule set to drop
    `unprovable_snapshot_ref_date` -- a control omitted so the value it refuses can be
    written. So a satellite over `bronze_payments` cannot get its `applied_date` the way
    every satellite before it did, and the repair is a declaration rather than a second
    loader or a bronze column nothing can prove.

    A COLUMN PLUS A RULE, WHICH IS `opl.bronze.snapshot_axis.SnapshotAxis`'S SHAPE AND
    DELIBERATELY NOT `SnapshotAxis` ITSELF. That type is one source's answer to "when did
    we OBSERVE this row" -- it names `_snapshot_month`, its `accepts` predicate validates
    a WINDOW value before Spark starts, and `ObservationGrain` and `read_snapshot_window`
    both key off it. This names something else about the same row: when the FACT was
    true. The two are different columns on the same table for payments (`_snapshot_month`
    and `event_time`) and for the RFB (`_snapshot_month` and `_snapshot_ref_date`), and
    `opl.bronze.snapshot`'s own docstring is about keeping exactly that pair apart.
    Reusing the type would let a caller pass an axis where a fact date belongs, and the
    two would agree on every source that has only one of them.

    `reads` AND NOT A CALLABLE, for `KeyPrefix`'s reason restated: this module imports no
    pyspark and must not start, so the value has to be something the registry can REASON
    about rather than a Column-building lambda. `opl.vault.loading.applied_date_expression`
    is the one place it becomes an expression, and it refuses a reader it has no branch
    for rather than falling through to a default.

    THE METADATA COLLISION IS REFUSED HERE, at construction, before any registry exists:
    the loader writes `applied_date` itself, so a source column of that name would be read
    and then overwritten by the value read from it -- which happens to be harmless and
    reads as a declaration that took effect. The other three are refused for
    `_validated_columns`' reason: the metadata value wins on the write."""

    column: str
    reads: str = READS_DATE

    def __post_init__(self) -> None:
        if not self.column or not self.column.strip():
            raise ValueError("an applied-date source needs a column name")
        if self.reads not in APPLIED_DATE_READERS:
            raise ValueError(
                f"applied-date source on column {self.column!r} declares reads="
                f"{self.reads!r}, which is not one of {APPLIED_DATE_READERS}. A reader "
                "outside the closed set has no expression behind it, so the column would "
                "be read by whatever branch happened to be last"
            )
        if self.column in METADATA_COLUMNS:
            raise ValueError(
                f"applied-date source names {self.column!r}, and the loaders write that "
                f"themselves ({', '.join(sorted(METADATA_COLUMNS))}). The source's own "
                "value would be replaced by the metadata on the write, leaving a column "
                "full of plausible dates that came from us rather than from the source"
            )


# THE DEFAULT, AND WHY EVERY SATELLITE WRITTEN BEFORE F2 WAVE 2 IS BYTE-UNCHANGED BY THE
# FIELD BELOW EXISTING. The four shipped satellites read `_snapshot_ref_date`, which is
# already a `date`, so `READS_DATE` returns `F.col(...)` -- exactly the expression
# `satellite_candidates` composed as a literal before the declaration existed.
#
# THE COLUMN NAME IS A SECOND SPELLING OF `opl.bronze.snapshot.SNAPSHOT_REF_DATE_COLUMN`,
# AND IT IS CROSS-CHECKED RATHER THAN TRUSTED -- `opl.vault.loading.BRONZE_RECORD_SOURCE`'s
# idiom, taken for that constant's reason and for one more. This module must import where
# pyspark is not installed (`opl.vault.columns` states the property and `KeyPrefix` states
# the rule), and `opl.bronze.snapshot` is Spark `Column` expressions and nothing else, so
# there is no importable constant here. `tests/vault/test_payments_satellite.py` asserts the
# two strings are equal, which turns the duplicate into a cross-check.
SNAPSHOT_REF_DATE = AppliedDateSource(column="_snapshot_ref_date", reads=READS_DATE)


@dataclass(frozen=True, kw_only=True)
class Satellite:
    """A DV2 satellite: a parent hub OR LINK, the payload whose change it records, where
    its `applied_date` is read from, and whether an observation ledger gates it.

    NO HASH-KEY FIELD, DELIBERATELY. A satellite's hash key IS its parent's, and a
    satellite free to spell it independently is a satellite a typo can point at
    nothing -- silently, as an empty join rather than an error. `parent_of` resolves
    it, and `build_registry` refuses a parent that is not a registered hub or link, so
    the two cannot disagree.

    THE PARENT MAY BE A LINK SINCE F2 WAVE 2, WHICH IS THE SAME KIND AND NOT A FIFTH ONE.
    `registry.py`'s stated criterion for `EffectivitySatellite` being a fourth kind is
    that "a `Satellite` is delta-driven on a `hash_diff` over a payload and
    `load_satellite` takes a `Hub`. This table has neither." A DESCRIPTIVE satellite on a
    link has BOTH -- `sat_link_payment` carries `amount`, `currency`, `payment_method` and
    a `hash_diff` over them, and goes through the SAME `changed_rows` `sat_empresa_dados`
    does -- so the only half that was ever true of it was the signature, which is the half
    the refusal itself named as the thing to change. A fifth kind would have been this
    dataclass with one annotation altered, and `opl.vault.specs`' rule for a genuinely new
    kind (its own `__post_init__` here, its word in the `VaultTable` union there) does not
    describe that.

    THE CHANGE DETECTOR IS THE SAME CODE AND IS INERT ON THIS TABLE, WHICH IS SAID HERE SO
    THE SENTENCE ABOVE IS NOT READ AS MORE THAN IT CLAIMS. `changed_rows` compares each
    row's `hash_diff` against `lag(hash_diff)` partitioned by the PARENT's hash key and
    ordered by `applied_date`. A link hash key that carries a dependent-child key is
    unique per event: `link_payment` hashes `transaction_id` into its digest, so every
    payment is its own partition with exactly one `applied_date`, `lag` is always NULL,
    and no candidate has ever been dropped as unchanged. What makes a re-load append
    nothing is `opl.vault.loading._without_persisted`'s (key, `applied_date`) anti-join
    ALONE. So the delta is true in form and unexercised in substance, and it stays in the
    shared path rather than being switched off: the first STATE satellite on a link --
    socios' `qualificacao_socio`, named below -- would exercise it on the day it lands,
    and a table-kind branch around it would be the thing that then had to be undone.

    `transactional` IS THE DECLARATION THAT THERE IS NO WINDOW TO CLOSE, and it decides
    one thing: whether `load_satellite` requires an `ObservationGrain`. It is DECLARED
    rather than inferred from the parent's kind, because both combinations are real DV2 --
    an EVENT satellite on a link (this one) and a STATE satellite on a link (socios'
    `qualificacao_socio`, which `registry.py` names as the first candidate) -- and a rule
    reading "a satellite on a link needs no ledger" would silently strip the ledger from
    the second the day it lands.

    IT IS NOT A PROXY AND IT IS NOT A SWITCH FOR TURNING A LEDGER OFF. The ledger's five
    states are derived over a key universe CROSSED WITH the months of the window, and
    `absent_after_observation` means "we saw this key earlier and did not see it here". At
    an EVENT grain that is the definition of every event in every later month: a payment
    made in June is absent from July because it happened once, not because anything
    departed. So a ledger here would report a candidate delete per payment of every
    earlier month -- measured on this task's own fixture at 2 of 4 keys for a stream in
    which nothing departed -- and print it into a task log as "candidate departures". A
    diagnostic whose only possible reading is false is the dual of the guard that cannot
    fire, and `opl.vault.observation`'s own rule is that the wrong thing must require
    typing. `build_registry` refuses the two pairings that would make this a switch: a
    transactional satellite on a HUB, and a non-transactional one on a LINK -- AND SO DOES
    `opl.vault.satellite_grain.snapshot_axis_for`, which is not a restatement. The registry
    guard covers every REGISTERED spec; the loader takes its parent as a free argument so
    that a throwaway one can reach it, and for a while it refused only one of the two
    there. A registry-only refusal is a refusal of the declarations, not of the loads."""

    name: str
    parent: str
    payload_columns: Sequence[str]
    applied_date_from: AppliedDateSource = SNAPSHOT_REF_DATE
    transactional: bool = False

    def __post_init__(self) -> None:
        if not self.name or not self.parent:
            raise ValueError(f"a satellite needs a name and a parent ({self.name!r})")
        if not isinstance(self.applied_date_from, AppliedDateSource):
            raise TypeError(
                f"satellite {self.name!r} declares applied_date_from="
                f"{self.applied_date_from!r}, which is not an AppliedDateSource -- a bare "
                "column name cannot carry the rule for reading a DAY out of it, and the "
                "two shapes this vault has (a `date` column and a 24-character ISO "
                "instant string) need different expressions"
            )
        object.__setattr__(
            self,
            "payload_columns",
            _validated_columns(
                self.payload_columns, owner=f"satellite {self.name!r}", role="payload"
            ),
        )


@dataclass(frozen=True, kw_only=True)
class KeyPrefix:
    """One component of a link end's business key, DERIVED as the first `width`
    characters of a named source column.

    WHY THIS EXISTS AT ALL, and it is F-DB's whole link. `link_candidates` reads every
    hub's business key from the columns the hub is NAMED after -- its docstring says so
    in capitals -- and `bronze_merchant` carries `cnpj`, fourteen characters, where
    `hub_empresa` keys on `cnpj_basico` at width 8. The empresa end is therefore DERIVED,
    exactly like `link_company_partner`'s partner end, and the plan's original ruling that
    `load_link` could write it was false. What changes here is that the derivation is
    DECLARED on the end instead of hard-coded in a second loader.

    A PREFIX AND NOT AN EXPRESSION, DELIBERATELY. A general escape hatch -- a lambda, a
    SQL string, a mini-language for slicing -- is refused for the reason
    `opl.bronze.registry`'s landing mode is a guarded declaration rather than a free
    string: a value the registry cannot REASON about cannot be checked against the hub it
    claims to key, and the guard in `build_registry` is the whole point of declaring it.
    A prefix covers both derivations this repository has (`merchant.cnpj` -> 8,
    `socios.cpf_cnpj_socio` -> 8), and the day a third shape arrives it is a deliberate
    edit here with its own guard rather than a hole that was already open.

    DATA, NOT A `Column`. This module imports no pyspark and must not start: the bronze
    registry -- which reaches these specs through nothing today, but which shares the
    constraint -- and the extraction scripts run on hosts where pyspark is an optional
    extra that is usually absent. `opl.vault.loading` is where this turns into an
    expression, through `hash_key_over`, which is the seam that already existed.

    `width` IS CROSS-CHECKED AGAINST THE HUB'S OWN, in `build_registry`, and a prefix that
    disagrees is refused naming the hub: it would key on a different-length root, produce
    a digest `load_hub` never wrote, and join to nothing without failing."""

    column: str
    width: int

    def __post_init__(self) -> None:
        if not self.column or not self.column.strip():
            raise ValueError("a key prefix needs a source column name")
        if self.width <= 0:
            raise ValueError(
                f"key prefix on column {self.column!r} declares width {self.width!r}. It "
                "must be a positive integer: a prefix of zero characters is the empty "
                "string for every row, which collapses the whole end onto one hash key"
            )


def _validated_key_from(
    key_from: Sequence[KeyPrefix] | None, hub: str
) -> tuple[KeyPrefix, ...] | None:
    """`key_from` as a frozen tuple, or None -- the three ways a declaration is malformed.

    `None` AND `()` ARE KEPT APART. `None` means "this end's key is read from the columns
    the hub is named after", which is every link this vault had before F-DB; an empty
    tuple is a declaration that declares nothing, and reading it as `None` would turn a
    half-written spec into the default silently."""
    if key_from is None:
        return None
    if isinstance(key_from, KeyPrefix):
        raise TypeError(
            f"the link end on hub {hub!r} received a bare KeyPrefix as its key_from; "
            "it is matched POSITIONALLY against the hub's business-key components, so "
            f"pass a tuple, e.g. ({key_from!r},)"
        )
    frozen = tuple(key_from)
    if not frozen:
        raise ValueError(
            f"the link end on hub {hub!r} declares an EMPTY key_from. Pass None for an "
            "end whose business key is read from the columns the hub is named after; an "
            "empty declaration is a half-written derivation that would read as that "
            "default"
        )
    if any(not isinstance(prefix, KeyPrefix) for prefix in frozen):
        raise TypeError(
            f"the link end on hub {hub!r} must declare its key_from as KeyPrefix values "
            "-- a bare column name cannot carry the prefix width, and the width is what "
            "build_registry checks against the hub's own"
        )
    return frozen


@dataclass(frozen=True, kw_only=True)
class LinkEnd:
    """One end of a link: the hub it references, the ROLE it plays in the relationship,
    whether that reference is part of the link's identity, and -- since F-DB -- how its
    business key is READ from the source when the source does not name it.

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
    would change that key the day the derivation changed.

    `identifying=False` IS NOT THE SAME QUESTION AS `key_from`, AND CONFLATING THEM IS
    THE DEFECT F-DB CORRECTED. Until then `identifying=False` was ALSO used as the proxy
    for "this end's key is derived", because the only derived end this vault had happened
    to be non-identifying. They are orthogonal: `identifying` asks whether the reference
    is part of the link's IDENTITY, and `key_from` asks where the reference's business
    key is READ FROM. `link_merchant_empresa`'s empresa end is derived AND identifying --
    `cnpj` enters that link's digest only through this end, and it must, or a merchant
    re-pointed to another company keeps its link hash key, the old relationship never
    becomes `absent_after_observation`, and no window is ever closed.

    `key_from` IS `None` FOR EVERY LINK WRITTEN BEFORE F-DB, which is what makes this
    field free to add: `None` means "read the hub's business key from the columns the hub
    is named after", which is exactly what `link_candidates` and `link_hash_key_expression`
    already did for every end, so both existing links keep their exact digests."""

    hub: str
    role: str | None = None
    identifying: bool = True
    key_from: Sequence[KeyPrefix] | None = None

    def __post_init__(self) -> None:
        if not self.hub or not self.hub.strip():
            raise ValueError("a link end needs a hub name")
        if self.role is not None and not self.role.strip():
            raise ValueError(
                f"the link end on hub {self.hub!r} declares an empty role. A role names "
                "the part this hub plays and prefixes its reference column; pass None "
                "for an end that has no role rather than a blank one"
            )
        object.__setattr__(
            self, "key_from", _validated_key_from(self.key_from, self.hub)
        )

    def reference_column(self, hub: Hub) -> str:
        """The column this end's hash-key reference is written into."""
        return hub.hash_key if self.role is None else f"{self.role}_{hub.hash_key}"

    def source_columns(self, hub: Hub) -> tuple[str, ...]:
        """The SOURCE columns this end's hub reference is read from, in hash order.

        THE ONE SPELLING OF "WHICH COLUMNS DOES THIS END NEED", read by four consumers
        that would otherwise each answer it: `links.link_candidates`' refusal of
        non-string columns, `loading`'s expression builders, `registry.identity_columns_of`
        (which is the observation grain an effectivity satellite on the link must be keyed
        on), and the job-wiring lock that checks a (vault table, bronze source) pairing
        before a deploy. A ledger keyed on a column bronze does not carry raises inside a
        vault job several tasks past the registry; a ledger keyed on the wrong one returns
        a full set of plausible states.

        NOT A `Column` AND NOT A WIDTH -- names only. What the prefix does to the value
        belongs to `opl.vault.loading`, which is the module that may import pyspark."""
        if self.key_from is None:
            return hub.business_key_columns
        return tuple(prefix.column for prefix in self.key_from)


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
    merges unrelated people onto every key by construction. They are stored on the
    link and hashed
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

    A FOURTH TABLE KIND RATHER THAN A `Satellite` WITH A LINK PARENT, AND ONLY ONE OF THE
    TWO ORIGINAL REASONS SURVIVES F2 WAVE 2 -- which is why this paragraph is written out
    rather than pointing at a guard. It read "the same reasons
    `_assert_every_satellite_hangs_off_a_hub` gives", and that guard now ADMITS a link
    parent: `load_satellite` takes one, and `sat_link_payment` is a `Satellite` on
    `link_payment`. So "the signature takes a `Hub`" is gone.

    WHAT IS UNCHANGED IS THE REASON THAT WAS ALWAYS THE REAL ONE: this table has NO
    PAYLOAD and NO `hash_diff`. A `Satellite` is delta-driven on the hash of its payload
    columns and `Satellite.__post_init__` refuses an empty payload outright; this table
    watches `is_active`, which nothing delivers and this vault derives from the
    observation ledger. Registering it as a `Satellite` would mean declaring a payload it
    does not have so that a change detector could be taken over it.

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
