# src/opl/vault/domains/payments_domain.py
"""The payments domain's vault tables: `link_payment`, a TRANSACTIONAL link between the
two company roles of one payment, carrying `transaction_id` as a dependent-child key; and
`sat_link_payment`, the DESCRIPTIVE satellite on that link carrying the payment's own
measures.

THE README'S OWN *HONEST LIMITS* SAYS "the payment fact does not go through the vault --
there is no payment hub". This module is the half of that sentence F2 wave 2 makes
false, and the half it leaves standing is stated below rather than left to be discovered.

`payments_domain.py` AND NOT `payments.py`, WHICH IS `merchant_domain.py`'S NAMING
DECISION AND ITS MEASURED REASON. `tests/test_revision_stamp.py` and
`tests/test_serverless_capabilities.py` sweep every file under `src/opl/**` with
`ids=lambda p: p.name`, and `opl/contracts/payments.py` already exists -- so a second
`payments.py` collides into `payments.py0`/`payments.py1`. `discover_domains` reads the
DIRECTORY and `DOMAIN` binds at module level, so the module's own name carries no
meaning to the registry and the suffix costs nothing.

THIS FILE IS DATA, like `cnpj.py` and `merchant_domain.py` beside it. Every guard that
could refuse what is below lives in `opl.vault.registry` and its satellite half, every
mechanism that reads it lives in the loaders, and "+1 file, 0 modified" is the property
`domains/__init__.py` exists to hold.

**AND THIS PARAGRAPH CLAIMED THE PROPERTY HELD FOR THIS DOMAIN, WHICH WAS TRUE OF T1 AND
IS FALSE OF T2.** It listed `registry.py`, `specs.py`, `hubs.py`, `satellites.py`,
`observation.py`, `effectivity.py` and `domains/__init__.py` as untouched; the satellite
below modified SIX of those seven -- `hubs.py` ALONE survives, `effectivity.py` having
gone with the same commit's rename sweep -- and added `registry_satellites.py` and
`satellite_grain.py`. The honest split, the one T1's report published for the link:

  - THE `Link` NEEDED NOTHING NEW. Its kinds, guards and loader all existed; `links.py`
    changed only to consume a deferral ADR 0011 had already recorded by name.
  - THE `Satellite` ON A LINK NEEDED A KIND'S SIGNATURE TO CHANGE, which is precisely the
    case `opl.vault.registry`'s docstring has always said does NOT clear the "+1 file"
    bar: a domain introducing a new table kind, or needing an existing kind widened,
    edits the mechanism, and the guard that refused this one said so in its own message.
  - WHAT THE CLAIM STILL COVERS is a domain built from the kinds as they now stand. That
    is what `test_a_new_domain_of_hubs_satellites_and_links_is_discovered_without_editing
    _any_file` exercises, and it is unchanged.

--- WHY THERE IS NO `hub_account` AND NO `hub_customer` -------------------------------

`opl.vault.registry`'s own docstring STAKED DV2's extensibility claim on wave 2 adding
them, and `opl.contracts.payments` SAID the counterparties were "where they become
keys". BOTH STATEMENTS ARE REFUTED HERE RATHER THAN DEFERRED, and the refutation is the
phase's most interesting result. (Both sentences have since been corrected where they
stood, and the past tense here is the correction's other half: T4's sweep found this
paragraph describing two files in the present tense that its own edits had just changed
-- a rename falsifying a third file's description of it, which is ADR 0022 Decision 6
happening to the change that states the rule.)

  - `payer_cnpj_basico` and `payee_cnpj_basico` ARE `cnpj_basico`. `hub_empresa` is
    already keyed on it, and `loading.hash_key_expression` hashes the padded key
    components and NOTHING else -- the hub's name is not in the digest -- so a
    `hub_account` on that column would produce the BYTE-IDENTICAL digest `hub_empresa`
    already holds. Two tables, one key space, and no guard anywhere refuses it. ADR 0011
    rejected exactly this shape for `hub_socio` and `merchant_domain.py` rejected it
    again for a CNPJ-keyed `hub_merchant`: "a duplicate hub... two tables that look
    independent and are one."
  - THERE IS NO ACCOUNT AND NO CUSTOMER IN THE STREAM. `opl.contracts.payments.COLUMNS`
    carries a transaction id, two timestamps, two counterparty roots, an amount, a
    currency and a method. Inventing an identifier means editing the generator and
    `SCHEMA_VERSION`, which is F1b/F5's byte-identity surface -- a scope change.
  - A ROLE IS NOT A HUB. "Payer" and "payee" are what one company IS in one
    relationship, which is `LinkEnd.role`'s whole subject, and the roles below are how
    the same hub appears twice without either reference being lost.

--- `link_payment`: A TRANSACTIONAL LINK, AND WHAT THAT MEANS HERE --------------------

A payment is not a state two companies are in; it is an EVENT between them. So the link
is keyed on the pair AND on the event's own identifier, which belongs to no hub -- the
idiom the master spec chooses for `transaction_id` and the one `link_company_partner`
already uses for the sócio grain. `opl.contracts.payments` is emphatic about what that
identifier is: the PROCESSOR's id for one payment, never a hash of the business
attributes, so two payments between one pair on one day are two link rows rather than a
collision.

BOTH ENDS ARE DERIVED AND IDENTIFYING, WHICH IS `link_merchant_empresa`'S PAIRING USED
TWICE ON ONE HUB. Derived because bronze payments carries no `cnpj_basico` under that
name, so `link_candidates`' default -- read the hub's business key from the columns the
hub is NAMED after -- finds nothing and `LinkEnd.key_from` declares where it really
lives. Identifying because a payment between A and B is not a payment between A and C:
an end left non-identifying drops its counterparty out of the digest, and every payment
A ever made would share a link key with every payment made TO A.

THE PREFIX WIDTH IS `hub_empresa`'S OWN, CROSS-CHECKED AT IMPORT by
`registry._refuse_a_derivation_that_does_not_fit`. The counterparties are ALREADY eight
characters (`opl.contracts.payments`: "THE COUNTERPARTIES ARE `cnpj_basico` (THE
8-CHARACTER ROOT)"), so the prefix is a no-op on well-formed data -- and that is the
point rather than an argument against declaring it. A `KeyPrefix` is what the registry
can REASON about: the width is checked against the hub's, and `zero_padded_column` fails
the query on an overlong value instead of truncating a longer number onto another
company's true key.

ORDER IS PAYER THEN PAYEE, and it is the link's identity rather than a listing
convention. `link_hash_key_expression` flattens the components with no boundary marker,
so swapping the two ends re-keys the whole table while both reference columns stay
correct and every join keeps working -- `links.refuse_mismatched_hubs` is what stops a
caller supplying them the other way round. Payer-then-payee is the direction the
relationship reads: money leaves the first and arrives at the second.

--- `sat_link_payment`: THE MEASURES, ON THE LINK ------------------------------------

`amount`, `currency` and `payment_method` are descriptive facts about the payment and
belong to a satellite ON the link. That was refused when `link_payment` landed -- "one
parented on a LINK -- which DV2 does allow -- would be a registered table nothing in this
package can write. The guard and that signature have to change together" -- and both
halves moved in the same task: `opl.vault.registry_satellites` admits a `Hub | Link`
parent and `load_satellite` takes `link=`/`hubs=`. It is the SAME kind and not a fifth
one; `opl.vault.specs.Satellite` carries that argument.

THE PAYLOAD IS DERIVED FROM THE CONTRACT AND NOT TYPED OUT. It is
`BUSINESS_ATTRIBUTE_COLUMNS` minus `COUNTERPARTY_COLUMNS` -- "what the payment WAS, as
opposed to which delivery of it this row is", minus the two that are already IN the link
as hub references. A hand-written tuple would be a fourth spelling of that list and would
keep passing on the day a sixth business attribute is added; `opl.contracts.payments`'s own
`_assert_the_columns_partition_cleanly` refuses at import a `COUNTERPARTY_COLUMNS`
that is not a subset of `BUSINESS_ATTRIBUTE_COLUMNS`, which is what makes the
subtraction total rather than merely plausible.

`applied_date` COMES FROM `event_time`, WHICH IS THE FINDING THIS TABLE PAID FOR. Every
satellite before it read `_snapshot_ref_date`, and **`bronze_payments` does not have that
column** -- `add_common_audit_columns` omits it for a generated source, deliberately, with
the reason written in four places, and stamping an all-NULL one would have forced the
payments DQ set to drop `unprovable_snapshot_ref_date`. So the applied date is DECLARED
(`opl.vault.specs.AppliedDateSource`) and read from the payment's own event day.

AND IT IS READ AS TEN CHARACTERS OF ISO TEXT, NOT THROUGH `ref_date_from_instant`. That
function pins the 27-character microsecond rendering `opl.bronze.snapshot_axis` declares;
`event_time` is 24 characters with THREE fractional digits
(`opl.generator.instants.to_text`), so it fails both the width check and the pattern --
measured, and it returns NULL for every payment row. The derivation taken is the one the
gold layer already uses on this exact column (`opl.gold.conformed.day_of`,
`to_date(substring(event_time, 1, 10))`), and never a CAST: a cast resolves the instant in
the SESSION timezone, and `applied_date` is the satellite's ORDERING axis.

--- WHAT THIS MODULE DELIBERATELY DOES NOT DECLARE ------------------------------------

NO EFFECTIVITY SATELLITE, and this one is a refusal rather than a deferral. An
effectivity satellite records the window in which a RELATIONSHIP held and closes it when
the relationship stops being observed; a payment is an event that happened once, so
there is no window to close and a departure at this grain would mean the processor
un-delivered a payment. `sat_eff_*`'s whole mechanism is disappearance-driven (ADR 0011),
and a table whose closing path can never have a producer is a table with an untestable
half.

NO OBSERVATION GRAIN, WHICH FOLLOWS FROM THE SAME SENTENCE AND IS NOW DECLARED RATHER
THAN MERELY ABSENT. The paragraph above read "the ledger exists to gate those closes",
which was true and incomplete: `load_satellite` also required a grain, so the satellite
had to say something. It says `transactional=True`. That is not a way of switching the
ledger off -- `opl.vault.registry_satellites` refuses the flag on a hub parent, and
refuses its absence on a link one -- it is the same fact stated where a loader can read
it. A ledger at this grain would report every payment of every earlier month as
`absent_after_observation`, i.e. as a candidate delete, which is exactly the sentence
above turned into a number. What the satellite does NOT lose is the window guard:
`satellite_grain.refuse_a_window_the_source_never_loaded` refuses a `months` value
`bronze_payments` never carried, over one table where the ledger uses two.

AND THE README'S LIMIT NARROWS RATHER THAN CLOSING. `fact_payment` still reads
`bronze_payments` directly (`gold_load_fact.py`), over 40,000 rows carrying F-API's FX
columns; re-keying it against this link is a gold refactor with its own risk and its own
decision. So the honest sentence after this file lands is "the fact does not yet read
from the vault", not "payments are in the vault".
"""
from __future__ import annotations

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT
from opl.contracts import payments as payments_contract
from opl.vault.domains.cnpj import CNPJ_BASICO_WIDTH, HUB_EMPRESA
from opl.vault.registry import (
    BusinessKeyColumn,
    KeyPrefix,
    Link,
    LinkEnd,
    Satellite,
    VaultDomain,
)
from opl.vault.specs import READS_ISO_TEXT, AppliedDateSource

_PAYMENTS = bronze_table_spec("payments")


def _counterparty_end(role: str, column: str) -> LinkEnd:
    """One end of the payment: `hub_empresa` under `role`, keyed on the first
    `CNPJ_BASICO_WIDTH` characters of `column`.

    A FUNCTION RATHER THAN TWO SPELLINGS OF ONE SHAPE, because the two ends differ in
    exactly two strings and everything else about them -- the hub, the width, the
    `identifying` flag, the fact that `key_from` is a TUPLE matched positionally against
    the hub's business-key components -- is one decision taken once. Two hand-written
    ends is how one of them silently acquires `identifying=False`, at which point that
    counterparty leaves the link's digest and every payment to it shares a key.

    NOT A LOOP OVER `COUNTERPARTY_COLUMNS` EITHER: the roles are this vault's own
    vocabulary and the contract does not carry them, so the pairing of role to column is
    a statement this module makes and must be readable as one."""
    return LinkEnd(
        hub=HUB_EMPRESA.name,
        role=role,
        # DERIVED **AND** IDENTIFYING. See the module docstring: derived because bronze
        # payments has no `cnpj_basico`, identifying because a payment between A and B is
        # not a payment between A and C.
        identifying=True,
        key_from=(KeyPrefix(column=column, width=CNPJ_BASICO_WIDTH),),
    )


PAYER_COLUMN, PAYEE_COLUMN = payments_contract.COUNTERPARTY_COLUMNS

# The two roles, in the order money moves. THE VAULT'S OWN WORDS, not the contract's: the
# contract names the COLUMNS (`payer_cnpj_basico`, `payee_cnpj_basico`) and this names the
# part each company plays, which is what `LinkEnd.reference_column` prefixes onto
# `hub_empresa_hk` to give `payer_hub_empresa_hk` and `payee_hub_empresa_hk`.
PAYER_ROLE = "payer"
PAYEE_ROLE = "payee"

LINK_PAYMENT = Link(
    name="link_payment",
    hash_key="link_payment_hk",
    # ORDER IS THE LINK'S IDENTITY, payer then payee. See the module docstring.
    hubs=(
        _counterparty_end(PAYER_ROLE, PAYER_COLUMN),
        _counterparty_end(PAYEE_ROLE, PAYEE_COLUMN),
    ),
    # THE EVENT'S OWN IDENTIFIER, WHICH BELONGS TO NO HUB. `opl.contracts.payments` spells
    # out what it is and what it is not, column by column: it identifies the EVENT, it is
    # never a hash of the business attributes, and it is not an account, a customer or a
    # counterparty. So it is a key component the relationship carries -- the idiom the
    # master spec chooses for this exact table and the one `link_company_partner` already
    # uses for the sócio grain (ADR 0011).
    #
    # NO WIDTH, which is `BusinessKeyColumn`'s "take the value as it is". A width is a
    # claim about a canonical form; the id is the processor's opaque string (a sha256 hex
    # digest today, and `opl.bronze.registry` deliberately declines to pin 64 there
    # either), so padding it would invent characters rather than recover a dropped zero.
    dependent_child_keys=(
        BusinessKeyColumn(name=payments_contract.IDENTITY_COLUMN),
    ),
)

# WHAT THE PAYMENT WAS, MINUS WHAT THE LINK ALREADY HOLDS. Derived from the contract and
# never typed out: `BUSINESS_ATTRIBUTE_COLUMNS` is the contract's own "what the payment
# WAS, as opposed to which delivery of it this row is", and the two counterparties are in
# the link as `payer_hub_empresa_hk` / `payee_hub_empresa_hk` -- carrying them again here
# would put the same fact in two tables at two grains, and the satellite's `hash_diff`
# would then move for a change the LINK's own key already re-keys on.
#
# ORDER IS THE CONTRACT'S, which is what makes this a subtraction rather than a rewrite:
# the tuple comprehension preserves `BUSINESS_ATTRIBUTE_COLUMNS`' order, and column ORDER
# is what `satellites._in_column_order` writes and what a Delta `mode("append")` matches
# on positionally.
#
# THE SUBTRACTION IS TOTAL BECAUSE THE CONTRACT MAKES IT SO. `opl.contracts.payments`
# asserts at import that `COUNTERPARTY_COLUMNS` is a SUBSET of
# `BUSINESS_ATTRIBUTE_COLUMNS`, so nothing can be subtracted that was not there and a
# sixth business attribute lands in this payload on the day it is declared rather than on
# the day somebody remembers to widen a literal.
PAYMENT_MEASURES = tuple(
    column
    for column in payments_contract.BUSINESS_ATTRIBUTE_COLUMNS
    if column not in payments_contract.COUNTERPARTY_COLUMNS
)

SAT_LINK_PAYMENT = Satellite(
    name="sat_link_payment",
    parent=LINK_PAYMENT.name,
    payload_columns=PAYMENT_MEASURES,
    # THE PAYMENT'S OWN EVENT DAY, because `bronze_payments` carries no
    # `_snapshot_ref_date` -- see the module docstring for the four sites that say so and
    # for why `ref_date_from_instant` cannot read this column.
    applied_date_from=AppliedDateSource(
        column=payments_contract.EVENT_TIME_COLUMN, reads=READS_ISO_TEXT
    ),
    # AN EVENT, SO NO WINDOW TO CLOSE AND NO LEDGER TO GATE ONE ON. See the module
    # docstring; `opl.vault.registry_satellites` refuses this flag on a hub parent and
    # refuses its absence on a link one, so it cannot be used to switch a ledger off.
    transactional=True,
)

# Where the rows come from. Qualified here, once, so neither loader nor job task spells a
# catalog or a schema -- `EMPRESAS_SOURCE` and `MERCHANT_SOURCE`'s decision. ONE source
# for both tables: the link and its satellite read the same bronze rows, which is what
# makes the satellite's hash key the link's own digest by construction.
PAYMENTS_SOURCE = DEFAULT.table(_PAYMENTS.bronze)

DOMAIN = VaultDomain(
    name="payments",
    tables=(LINK_PAYMENT, SAT_LINK_PAYMENT),
)
