# src/opl/vault/domains/payments_domain.py
"""The payments domain's vault table: `link_payment`, a TRANSACTIONAL link between the
two company roles of one payment, carrying `transaction_id` as a dependent-child key.

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
could refuse what is below lives in `opl.vault.registry`, every mechanism that reads it
lives in the loaders, and "+1 file, 0 modified" is the property `domains/__init__.py`
exists to hold. It IS held for this domain in the vault: `registry.py`, `specs.py`,
`hubs.py`, `satellites.py`, `observation.py`, `effectivity.py` and `domains/__init__.py`
are untouched by it. `links.py` is not -- see the last section below, which is about a
deferral this table is the named consumer of rather than about a new KIND of declaration.

--- WHY THERE IS NO `hub_account` AND NO `hub_customer` -------------------------------

`opl.vault.registry`'s own docstring stakes DV2's extensibility claim on wave 2 adding
them, and `opl.contracts.payments` says the counterparties are "where they become keys".
BOTH STATEMENTS ARE REFUTED HERE RATHER THAN DEFERRED, and the refutation is the phase's
most interesting result:

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

--- WHAT THIS MODULE DELIBERATELY DOES NOT DECLARE ------------------------------------

NO DESCRIPTIVE SATELLITE. `amount`, `currency` and `payment_method` are descriptive facts
about the relationship and belong to a satellite ON the link -- which
`registry._assert_every_satellite_hangs_off_a_hub` refuses today, knowingly: "one
parented on a LINK -- which DV2 does allow -- would be a registered table nothing in this
package can write. The guard and that signature have to change together." That is one
decision with two halves and belongs to the task that owns both, not half-made here.

NO EFFECTIVITY SATELLITE, and this one is a refusal rather than a deferral. An
effectivity satellite records the window in which a RELATIONSHIP held and closes it when
the relationship stops being observed; a payment is an event that happened once, so
there is no window to close and a departure at this grain would mean the processor
un-delivered a payment. `sat_eff_*`'s whole mechanism is disappearance-driven (ADR 0011),
and a table whose closing path can never have a producer is a table with an untestable
half.

NO OBSERVATION GRAIN, which follows: the ledger exists to gate those closes.

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
    VaultDomain,
)

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

# Where the rows come from. Qualified here, once, so neither loader nor job task spells a
# catalog or a schema -- `EMPRESAS_SOURCE` and `MERCHANT_SOURCE`'s decision.
PAYMENTS_SOURCE = DEFAULT.table(_PAYMENTS.bronze)

DOMAIN = VaultDomain(
    name="payments",
    tables=(LINK_PAYMENT,),
)
