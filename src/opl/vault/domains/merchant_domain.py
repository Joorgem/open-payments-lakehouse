# src/opl/vault/domains/merchant_domain.py
"""The merchant domain's vault tables and grains: `hub_merchant`, `sat_merchant_dados`,
`link_merchant_empresa` and `sat_eff_merchant_empresa` -- the second domain this vault
has, the first link across two sources, and the first table in this lakehouse whose
effectivity satellite can ever close a window.

`merchant_domain.py` AND NOT `merchant.py`, WHICH IS A NAMING DECISION WITH A MEASURED
REASON. `tests/test_revision_stamp.py` and `tests/test_serverless_capabilities.py` sweep
every file under `src/opl/**` with `ids=lambda p: p.name`, and `opl/contracts/merchant.py`
already exists -- so a second `merchant.py` collides into `merchant.py0`/`merchant.py1`,
the first basename collision in either sweep. `discover_domains` reads the DIRECTORY and
`DOMAIN` binds at module level, so the module's own name carries no meaning to the
registry and the suffix costs nothing.

THIS FILE IS DATA, like `cnpj.py` beside it. Every guard that could refuse what is below
lives in `opl.vault.registry`, every mechanism that reads it lives in the loaders, and
"+1 file, 0 modified" is the property `opl/vault/domains/__init__.py` exists to hold. It
is NOT held for this domain and the reason is in `links.py` and `specs.py` rather than
here: a link whose ends live in two sources needed `LinkEnd.key_from`, and a new KIND of
declaration was always an edit inside wave 1 (`registry.py`'s own docstring says so, kind
for kind). What survives untouched is `domains/__init__.py`, `hubs.py`, `satellites.py`,
`observation.py` and `effectivity.py` -- the discovery, the four loaders' logic, and the
ledger.

--- `hub_merchant` IS KEYED ON `merchant_id`, AND THE CNPJ IS REACHED BY A LINK --------

REVISION 1 OF THE PLAN KEYED THIS HUB ON THE CNPJ, AND THAT IS A DUPLICATE HUB.
`opl.vault.loading.hash_key_expression` hashes the padded key components and NOTHING
else -- the hub's name is not in the digest -- so `hub_merchant` on `cnpj_basico` would
produce, for `12345678`, the BYTE-IDENTICAL digest `hub_empresa` already holds over
69,062,849 keys. Two tables, one key space, one digest space, and no guard anywhere
refuses it. ADR 0011 rejected exactly this shape once already, for a PJ-only `hub_socio`:
"a duplicate hub... two tables that look independent and are one."

AND KEYING ON THE SURROGATE IS WHAT THE SOURCE IS, not a dodge. An operational merchant
registry keys on its own identifier and carries the CNPJ as an ATTRIBUTE -- which is what
makes the integration claim a LINK rather than a collision: Files (the Receita's CNPJ) and
Databases (Postgres) meeting on one hub, which is the central DV2 value proposition and
the one thing this project's vault had never demonstrated.

NO WIDTH ON `merchant_id`, WHICH IS `BusinessKeyColumn`'s "take the value as it is". It is
a Postgres `uuid` rendered by `::text` -- thirty-six characters, hyphenated, lower case --
and zero-padding it would invent characters where padding `cnpj_basico` to eight recovers
a leading zero a source may have dropped. A width is a claim about a canonical form, and
the canonical form of a UUID is the value itself.

--- `link_merchant_empresa`: THE DERIVATION IS DECLARED ON THE END ---------------------

`bronze_merchant` CARRIES `cnpj`, FOURTEEN CHARACTERS, AND NO `cnpj_basico` AT ALL.
`hub_empresa` keys on `cnpj_basico` at width 8, and `link_candidates` reads every hub's
business key from the columns that hub is NAMED after -- so this end is DERIVED, exactly
like `link_company_partner`'s partner end. It does not degrade quietly either:
`zero_padded_column` fails the query on an overlong value rather than truncating a
fourteen-character CNPJ onto another company's true eight-character key.

SO THE PREFIX IS DECLARED HERE AND CHECKED IN `build_registry` -- one entry per
business-key component, matched positionally, at a width that must EQUAL the hub's own.
A prefix that disagreed would key on a different-length root and join to nothing without
failing. See `opl.vault.specs.KeyPrefix` for why a prefix and not an expression.

BOTH ENDS ARE IDENTIFYING, AND THAT IS THE DIFFERENCE FROM THE PARTNER LINK.
`link_hash_key_expression` warns that hashing a derived value "would make the link's
identity depend on a value we derived" -- that warning is about REDUNDANCY, not about
derivation: socios' partner root is already in the digest through `cpf_cnpj_socio`, so
hashing it again adds nothing. Here `cnpj` enters the digest ONLY through this end, and it
MUST: with `merchant_id` alone as the identity, a merchant re-pointed to another company
keeps its link hash key, the old relationship never becomes `absent_after_observation`,
and NO CLOSING ROW IS WRITTEN. The link's grain is (`merchant_id`, `cnpj_basico`), read
off bronze as (`merchant_id`, `cnpj`).

--- WHAT THE SATELLITE WATCHES, AND THE TWO COLUMNS IT DELIBERATELY DOES NOT -----------

`sat_merchant_dados` CARRIES SEVEN OF THE ELEVEN POSTGRES COLUMNS. The four it does not
are each somewhere else, and `UNMODELLED_MERCHANT_COLUMNS` below is the one that is
nowhere -- declared for `UNMODELLED_ESTABELECIMENTO_COLUMNS`' reason, so a column absent
from every tuple is visibly a decision rather than an oversight.

`updated_at` IS NOT IN THE PAYLOAD, AND THAT IS THIS PHASE'S OWN DEFECT REFUSED IN THE
ONE PLACE IT WOULD LAND. It is a timestamp on a row and it looks like a descriptive fact;
a satellite payload containing it would move `hash_diff` on EVERY row the source's
`BEFORE UPDATE` trigger touched, whether or not any value a reader cares about changed --
collapsing T2's two axes into one. The phase measures 48 updates that move it against 24
that do not, over one derivation; a payload carrying the column would make those two
classes indistinguishable at the satellite, with every count still green. The same
argument `opl.contracts.merchant` makes about `_snapshot_at` versus `updated_at`, one
layer down.

`cnpj` IS NOT IN THE PAYLOAD EITHER, and for the opposite reason: it is not unmodelled at
all, it is the link's identity. A merchant re-pointed to another company is a NEW link key
and a closed window on the old one -- which is the fact the phase exists to produce -- and
carrying it in the satellite as well would record the same change twice, in a table whose
`hash_diff` is supposed to answer "did the description move".

`credit_limit` IS IN THE PAYLOAD AND IS A `numeric(14,2)` RENDERED BY POSTGRES. The scale
is pinned by the DDL rather than by arithmetic, so the digest is over the declared scale;
`rules.unparseable_credit_limit` is what stops a value that casts to NULL making two
different payloads digest the same.

--- THE TWO GRAINS, AND WHY ONE OF THEM IS AT LINK GRAIN ------------------------------

`opl.vault.effectivity` states it: a key that loses one of two relationships is
`absent_after_observation` at LINK grain and plainly `observed` at hub grain, so a
hub-grain ledger reports no departure and the window stays open forever. Both grains are
declared below because both are read -- the descriptive satellite gates its (optional)
departure diagnostic on the hub grain, and the effectivity satellite closes windows on the
link grain.

THE AXIS IS `_snapshot_at` ON BOTH, CARRIED OFF THE `BronzeTable` RATHER THAN CHOSEN
HERE. `bronze_merchant` is the only registered source observed TWICE INSIDE ONE CALENDAR
MONTH, which is what T7 put `snapshot_axis` on `BronzeTable` for: on the monthly default
both observations fold into one ledger row, the sixteen deleted merchants read as
`observed`, and the end-dating path has no producer -- no error and no red test."""
from __future__ import annotations

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT
from opl.contracts import merchant as merchant_contract
from opl.vault.domains.cnpj import CNPJ_BASICO_WIDTH, HUB_EMPRESA
from opl.vault.observation import ObservationGrain
from opl.vault.registry import (
    BusinessKeyColumn,
    EffectivitySatellite,
    Hub,
    KeyPrefix,
    Link,
    LinkEnd,
    Satellite,
    VaultDomain,
    identity_columns_of,
)

_MERCHANT = bronze_table_spec("merchant")

HUB_MERCHANT = Hub(
    name="hub_merchant",
    hash_key="hub_merchant_hk",
    # NO WIDTH. See the module docstring: a `uuid` rendered by `::text` is already its
    # own canonical form, and a pad would invent characters rather than recover a lost
    # leading zero. The column name is read from the contract rather than retyped --
    # `opl.contracts.merchant` is where the source's own spelling lives.
    business_keys=(BusinessKeyColumn(name=merchant_contract.MERCHANT_ID_COLUMN),),
)

SAT_MERCHANT_DADOS = Satellite(
    name="sat_merchant_dados",
    parent=HUB_MERCHANT.name,
    # SEVEN OF ELEVEN, in the contract's own order. `merchant_id` is the key, `cnpj` is
    # the link's identity, `onboarded_on` is the effectivity window's open and
    # `updated_at` is unmodelled below -- see the module docstring for why the last of
    # those is a refusal rather than an omission.
    payload_columns=(
        merchant_contract.LEGAL_NAME_COLUMN,
        merchant_contract.TRADE_NAME_COLUMN,
        merchant_contract.STATUS_COLUMN,
        merchant_contract.MCC_COLUMN,
        merchant_contract.SETTLEMENT_ACCOUNT_COLUMN,
        merchant_contract.RISK_TIER_COLUMN,
        merchant_contract.CREDIT_LIMIT_COLUMN,
    ),
)

LINK_MERCHANT_EMPRESA = Link(
    name="link_merchant_empresa",
    hash_key="link_merchant_empresa_hk",
    # THE ORDER IS THE LINK'S IDENTITY, and it is merchant-then-company because that is
    # the direction the relationship reads: this source's own entity, and the company it
    # points at. Swapping the two re-keys the whole table while every reference column
    # stays correct (`links.refuse_mismatched_hubs`).
    #
    # THE SECOND END IS DERIVED AND IDENTIFYING, which is the pairing no link in this
    # vault had before. Derived because `bronze_merchant` has no `cnpj_basico`;
    # identifying because `cnpj` reaches the digest through this end and nowhere else,
    # and a link whose identity omitted it would keep its hash key when a merchant is
    # re-pointed -- so the old relationship would never depart and no window would close.
    hubs=(
        LinkEnd(hub=HUB_MERCHANT.name),
        LinkEnd(
            hub=HUB_EMPRESA.name,
            key_from=(
                KeyPrefix(
                    column=merchant_contract.CNPJ_COLUMN, width=CNPJ_BASICO_WIDTH
                ),
            ),
        ),
    ),
    # NO DEPENDENT-CHILD KEYS. Both components of this relationship belong to a hub --
    # unlike socios, where the RFB's masked CPF has a 10^6 key space that is 99.99%
    # occupied -- so there is nothing here that identifies a business object this vault
    # has no hub for. It is also what lets `load_link` write this link at all.
)

SAT_EFF_MERCHANT_EMPRESA = EffectivitySatellite(
    name="sat_eff_merchant_empresa",
    parent=LINK_MERCHANT_EMPRESA.name,
    # THE WINDOW'S OPEN, DELIVERED, UNDER THE SOURCE'S OWN NAME -- the same epistemics
    # `sat_eff_company_partner` states: the open is the source's fact and the close is
    # ours, named in our vocabulary (`last_observed_on`, `closed_by`) so the two are not
    # read as claims of the same strength.
    #
    # `NOT NULL` IN THE DDL, AND ASSERTED AT THE GATE, WHICH IS NOT DECORATION.
    # `opl.vault.effectivity` records that a NULL entry date SORTS FIRST in Spark's
    # ascending ordering and therefore BEATS a delivered date on its twin in the dedup.
    # That edge was unreachable on socios by measurement; here it is unreachable by
    # schema, and `rules.bad_onboarded_on_shape` closes the "parseable" half.
    entry_column=merchant_contract.ONBOARDED_ON_COLUMN,
)

# Every Postgres column that is neither a key, nor a link identity component, nor a
# satellite payload, nor the effectivity window's open. Declared rather than left as a
# gap, for `UNMODELLED_ESTABELECIMENTO_COLUMNS`' reason.
UNMODELLED_MERCHANT_COLUMNS = (
    # THE WATERMARK COLUMN, LANDED AND NOT MODELLED, and the omission is the phase's
    # headline stated in a tuple. It is what an incremental extract WOULD have keyed on,
    # so bronze carries it -- the miss is measured against a real column, not a
    # hypothetical one -- and no vault table watches it, because it answers "when did
    # this row's transaction START" where every fact this vault records is about what a
    # snapshot SAW. See the module docstring for what putting it in the payload costs.
    merchant_contract.UPDATED_AT_COLUMN,
)

# Where the rows come from. Qualified here, once, so neither loader nor job task spells a
# catalog or a schema.
MERCHANT_SOURCE = DEFAULT.table(_MERCHANT.bronze)

# The observation ledger at HUB grain, keyed on the raw `merchant_id` as bronze holds it
# -- the ledger answers "what did we SEE". Read by `sat_merchant_dados`' departure
# diagnostic, which is a number in a log rather than a row in a table.
MERCHANT_GRAIN = ObservationGrain.in_default_schema(
    name=HUB_MERCHANT.name,
    bronze=_MERCHANT.bronze,
    quarantine=_MERCHANT.quarantine,
    key_columns=HUB_MERCHANT.business_key_columns,
    snapshot_axis=_MERCHANT.snapshot_axis,
)

# THE OBSERVATION LEDGER AT LINK GRAIN, which is the grain the effectivity satellite gates
# its closes on and NOT the hub grain -- `COMPANY_PARTNER_GRAIN` states the argument and
# it is the same one here.
#
# DERIVED THROUGH `identity_columns_of` RATHER THAN RESTATED, which `cnpj.py` could not do
# (its link's grain is a hub key plus dependent-child keys, spelled from two specs) and
# this one can: the function takes the link and its hubs explicitly, needs no built
# registry, and is the SAME call `opl.vault.effectivity._refuse_a_mismatched_link_grain`
# compares against. So the ledger's key columns and the link's identity cannot drift, and
# in particular the grain is keyed on `cnpj` -- the column bronze HAS -- rather than on
# `cnpj_basico`, which it does not.
MERCHANT_EMPRESA_GRAIN = ObservationGrain.in_default_schema(
    name=LINK_MERCHANT_EMPRESA.name,
    bronze=_MERCHANT.bronze,
    quarantine=_MERCHANT.quarantine,
    key_columns=identity_columns_of(
        LINK_MERCHANT_EMPRESA, (HUB_MERCHANT, HUB_EMPRESA)
    ),
    snapshot_axis=_MERCHANT.snapshot_axis,
)

DOMAIN = VaultDomain(
    name="merchant",
    tables=(
        HUB_MERCHANT,
        SAT_MERCHANT_DADOS,
        LINK_MERCHANT_EMPRESA,
        SAT_EFF_MERCHANT_EMPRESA,
    ),
)
