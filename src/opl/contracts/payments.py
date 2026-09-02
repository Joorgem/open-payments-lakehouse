# src/opl/contracts/payments.py
"""Versioned schema contract for the synthetic payment stream (F1b), in the shape
`opl.contracts.cnpj_schemas` takes for the Receita Federal files: the columns are
DATA here, and every mechanism that reads them lives elsewhere.

THE GRAIN IS ONE ROW PER PAYMENT EVENT. Not per settlement, not per day, not per
counterparty pair: one row is one payment, delivered once. Everything below follows
from that sentence, and the two hardest decisions in this phase are both restatements
of it.

WHAT `transaction_id` IDENTIFIES: the EVENT. It is the payment processor's own
identifier for one payment, assigned once when the payment happens and repeated
verbatim if the processor delivers that payment again. Two rows carrying the same
`transaction_id` are the SAME payment seen twice.

WHAT `transaction_id` DOES NOT IDENTIFY, stated column by column because each of these
was a candidate and each is wrong:

  - It is not a hash of the business attributes, and MUST NEVER BECOME ONE. If it
    were, a customer paying the same merchant the same amount twice would collide onto
    one id, and the stream would lose the ability to say whether two identical-looking
    rows are one event delivered twice or two events that happened. Every dedup claim
    downstream rests on that distinction; a derived id makes the claim unfalsifiable.
    This repository already carries a defect of exactly that shape -- a
    `COUNT(DISTINCT a,b,c)` that silently dropped 8,761 NULL-bearing rows -- so the
    failure mode is not hypothetical here.
  - It is not an account, a customer, or a counterparty. Those are
    `payer_cnpj_basico` / `payee_cnpj_basico`, and F2 wave 2 made them keys as two
    ROLES on ONE hub -- `payer_hub_empresa_hk` and `payee_hub_empresa_hk`, both
    references to `hub_empresa` on `link_payment`. THIS BULLET USED TO NAME
    `hub_account` / `hub_customer` AS WHERE THEY BECOME KEYS AND THAT IS REFUTED, not
    merely unbuilt: either hub would hash the same 8-character root `hub_empresa`
    already holds, and `loading.hash_key_expression` puts no table name in the digest,
    so the two tables would carry byte-identical keys and nothing would refuse it
    (ADR 0022 Decision 2). A role is not an entity; it is what one company IS in one
    relationship, which is the link's subject rather than a hub's.
  - It is not a settlement or a batch. A settlement groups many payments; nothing in
    this contract expresses that grouping, deliberately, because the vault does not
    model it yet and a column nothing reads is a column that rots.
  - It is not a file, a line number, or a position in the stream. Those are properties
    of a DELIVERY, and a delivery is exactly what may repeat.
  - It is not ordered and carries no time. `event_time` is when the payment happened;
    an id that also encoded time would give two spellings of one fact.

SO A LEGITIMATE REPEAT AND A DUPLICATE ARE DIFFERENT ROWS, AND THE DATA SAYS WHICH.
A legitimate repeat is a DIFFERENT `transaction_id` carrying identical
`BUSINESS_ATTRIBUTE_COLUMNS` -- normal business, and it must survive every dedup. A
duplicate is a byte-identical redelivery of an existing `transaction_id`, injected by
`opl.generator.defects`. The clean stream already emits legitimate repeats, on purpose,
so that when duplicates arrive there is something to confuse them with: a fixture
in which `COUNT(DISTINCT transaction_id)` and `COUNT(DISTINCT <business attributes>)`
agree is a fixture that tests nothing.

TWO TIMESTAMPS, AND NEITHER IS A WALL CLOCK. `event_time` is when the payment
happened; `emitted_at` is when the generator released the record. Lateness is the
ORDERING PROPERTY BETWEEN THEM -- a record is late when it is emitted after records
whose `event_time` is newer than its own -- so it can be stated, generated and
asserted without any process ever reading the clock. Both are derived from the
stream's own declared window; see `opl.generator.stream`. In the clean stream the two
orders agree, which is precisely the property the late arrivals in
`opl.generator.defects` violate, and `opl.generator.measures.late_arrivals` is the one
place the violation is counted.

ALL-STRING IN BRONZE, MATCHING THE CNPJ DECISION (`opl.bronze.schema`), AND THE
GENERATOR EMITS STRINGS TO MATCH. This is the one place a generator that knows types
and a bronze that stores strings could disagree about what NULL means, so the
disagreement is removed rather than managed:

  1. `opl.generator.events.PaymentEvent` is the only typed representation. It holds
     `int` milliseconds and `int` cents; it never holds a `float`, a `datetime`, or a
     `Decimal`.
  2. `opl.generator.events.record_of` is the ONLY crossing, and it is typed
     `dict[str, str]`. No `int`, `float`, `bool`, `datetime` or `None` can leave the
     generator inside a contract column.
  3. Therefore the emitted JSON carries no `null`, no `0`, no `false` and no `1.0` in
     any contract column, and bronze's STRING columns receive exactly the characters
     the generator wrote.
  4. The empty string is NOT a spelling of NULL and never appears: every column below
     is required in v1, `""` is refused at the generator's own boundary, and the DQ
     gate's `null_or_empty_*` rules (Task 3) reject both spellings under one reason.
     That is the same three-way split `opl.vault.hashing` makes between NULL, empty
     and whitespace -- absent, blank and whitespace-only are three states, not one.

The consequence worth stating plainly: `NULL` in the payments bronze table can only
mean "the JSON did not carry this field at all". Nothing else can produce it. That is
what makes the schema drift measurable -- a NEW column appearing mid-stream makes
every earlier row NULL in it, and no other mechanism competes for that meaning.

AND IT IS ALSO WHAT ARMS A DEFECT THIS REPOSITORY HAS ALREADY MET. While no contract
column is ever NULL, `COUNT(DISTINCT a, b, c)`'s row-dropping behaviour is INERT here;
the drift column is the first NULL this stream can hold, and it arms it. See the
`DRIFT_COLUMN` block below for the guard that keeps it out of every tuple a distinct
count is taken over, and `opl.generator.measures` for the two differently-named counts
that make the difference impossible to take by accident.

JSON LINES, NOT CSV, AND THE CHOICE IS ABOUT TASK 2. The CNPJ files are headerless
semicolon CSV because the RFB ships them that way. A payment stream is ours to shape,
and the drift class this phase must exhibit is a NEW OPTIONAL COLUMN APPEARING
MID-STREAM. In CSV that is a column-count change, which a positional reader either
refuses outright or silently misaligns; in JSON it is a key that some records carry
and others do not, which is what a real event stream actually does and what Auto
Loader's schema evolution and `_rescued_data` are built to meet. One record per line,
UTF-8, `\n` -- never `os.linesep`, or the same seed would produce different bytes on
Windows and on Databricks.

BOTH COUNTERPARTIES ARE LEGAL ENTITIES (PJ), AND THAT IS A DELIBERATE NARROWING. A
natural-person payer would need a CPF, and there is no CPF registry in this lakehouse
to resolve one against -- the RFB masks partner CPFs to six middle digits, over a 10^6
space that is 99.99% saturated, which is why there is no `hub_socio` (ADR 0011). A PF
side would therefore be an unresolvable key: exactly the "random digits joining to
nothing" that makes an integration decorative. Both sides are `cnpj_basico`, drawn
from the real `hub_empresa` key space, so 100% of them resolve by construction and a
rate below 100% is a finding rather than a tolerance.

THE COUNTERPARTIES ARE `cnpj_basico` (THE 8-CHARACTER ROOT), NOT THE 14-CHARACTER CNPJ
COMPLETO. `hub_empresa`'s business key is `cnpj_basico` and that is the hub the
integration claim is measured against. A fourteen-character CNPJ would need
`cnpj_ordem` and `cnpj_dv` invented alongside it, and an invented establishment whose
ROOT resolves while the establishment itself does not exist is a WEAKER claim wearing
a longer number -- it would resolve against `hub_empresa` and fail against
`hub_estabelecimento`. Eight characters that resolve exactly beats fourteen that
resolve partly. The width itself is owned by `opl.vault.domains.cnpj`, not respelled
here, and is enforced where the pool is built (`opl.generator.cnpj_pool`).

NEVER NUMERIC, for `cnpj_schemas`' reason: alphanumeric CNPJs take effect 2026-07-31,
and an `int()` round trip loses a leading zero the moment one arrives.
"""
from __future__ import annotations

# The contract's version, and it is STILL 1 with F1b Task 2 landed -- which contradicts
# what this comment said when Task 0 wrote it ("BUMPED, never edited in place: Task 2's
# schema drift adds an optional column mid-stream"). The contradiction is a finding, not
# an oversight, and the reason is one sentence: A DRIFT COLUMN THIS CONTRACT DECLARES IS
# NOT DRIFT. See the `DRIFT_COLUMN` block below for the three mechanisms that make a
# declared one invisible -- the serialiser would emit it on every record, the Auto Loader
# read schema would absorb it, and a distinct count would silently drop the pre-drift
# population.
#
# v1 is therefore what the generator writes, what a read schema is built from, and what
# the DQ gate validates. A drifted row is a row written under a LATER version this
# lakehouse has not adopted, which is precisely what schema drift IS. The version bumps
# when the column is ADOPTED -- when the gate is meant to accept it rather than report it
# -- and that is a decision with a diff, not a side effect of generating a defect.
SCHEMA_VERSION = 1

# The contract key, in the sense `opl.bronze.registry.BronzeTable.contract` means it.
CONTRACT = "payments"

# --- the columns ---------------------------------------------------------------------

IDENTITY_COLUMN = "transaction_id"
EVENT_TIME_COLUMN = "event_time"
EMITTED_AT_COLUMN = "emitted_at"

# WHAT THE PAYMENT WAS, as opposed to which delivery of it this row is. The T2 test
# reads this tuple: `DISTINCT` over these columns must give a DIFFERENT answer from
# `DISTINCT` over `transaction_id`, or the stream contains no legitimate repeats and
# proves nothing about dedup.
#
# THE TIMESTAMPS ARE DELIBERATELY NOT IN HERE, and that is the whole point of the
# tuple. Including `event_time` would make every row's attribute tuple unique by
# construction -- two payments cannot happen at the same instant in a generator that
# advances time per event -- so the two DISTINCT counts would agree and the test would
# be vacuous while looking thorough.
BUSINESS_ATTRIBUTE_COLUMNS = (
    "payer_cnpj_basico",
    "payee_cnpj_basico",
    "amount",
    "currency",
    "payment_method",
)

# The two columns that must resolve against `hub_empresa`. Named as their own tuple
# because Task 4's resolution measurement is over exactly these and nothing else.
COUNTERPARTY_COLUMNS = ("payer_cnpj_basico", "payee_cnpj_basico")

# ORDER IS AUTHORITATIVE, though not for the reason the CNPJ contract's order is.
# Those files are headerless, so their order decides what a column MEANS. These are
# JSON objects, where the key decides that -- but the order still decides the BYTES,
# and byte-identity from a seed is the property T1 rests on. A tuple, not a list:
# `opl.bronze.rules._encoding_check` records what handing out a mutable column list
# costs, and this one is read by a serialiser whose output is asserted byte-for-byte.
COLUMNS = (
    IDENTITY_COLUMN,
    EVENT_TIME_COLUMN,
    EMITTED_AT_COLUMN,
    *BUSINESS_ATTRIBUTE_COLUMNS,
)

# v1 has no optional column, and Task 2 did NOT add one -- see `SCHEMA_VERSION` above for
# why the drift column is undeclared rather than optional. Task 0 predicted these two
# tuples would stop being equal here; they have not, and the prediction is corrected
# rather than satisfied by a column that would destroy the defect it was added to carry.
#
# Still declared as its own name rather than as "= COLUMNS" at the use site, because the
# two mean different things even while they hold the same values: `COLUMNS` is what every
# record CARRIES (the serialiser walks it and its ORDER decides the emitted bytes), and
# `REQUIRED_COLUMNS` is what every record must carry NON-EMPTY (the gate reads it, and
# its order means nothing). The guard below depends on neither being an alias for the
# other, because it is the required set -- not the carried set -- that decides whether a
# distinct count can drop a row.
REQUIRED_COLUMNS = COLUMNS

# --- the value domains -----------------------------------------------------------

# THE VALUE DOMAIN, AND IT IS NOT THE TUPLE THE GENERATOR DRAWS FROM. Those were one
# object until F-API, and separating them is the whole of T1: it is what lets
# `dim_currency` gain a member without re-drawing a single row of a stream that has
# already landed.
#
# WHY IT GAINED A SECOND MEMBER. `opl.gold.registry.DIM_CURRENCY` declares
# `members=payments.CURRENCIES`, so this tuple IS the dimension's member set -- and with
# one member `dim_currency` is a constant column wearing a dimension's name, `fx_rate` is
# 1.0 on every row, and `amount_brl` equals `amount` everywhere. An FX layer built on that
# cannot be wrong, which is the same thing as saying nothing about it can be tested. This
# is the only place the dimension gains a member, so the addition happens here.
#
# WHY APPENDING HERE DOES NOT MOVE ONE BYTE OF THE FOUR STREAMS ALREADY LANDED, as a
# mechanism rather than a hope. The generator does NOT read this tuple: it draws from
# `StreamSpec.currencies`, a per-profile tuple defaulted to `stream.DEFAULT_CURRENCIES`
# (`(REPORTING_CURRENCY,)`). `derivation.pick` is `items[draw_below(..., bound=len(items))]`
# and `draw_below` reduces modulo the bound, so with `bound = 1` the index is 0 for every
# seed, every purpose and every position -- a one-tuple draws its single member and always
# will. The DOMAIN's length never enters a draw at all. Appending to what the generator
# drew from would have re-drawn roughly half the rows of every stream ever generated.
#
# ORDER IS AUTHORITATIVE HERE FOR A SECOND REASON, on top of the one `PAYMENT_METHODS`
# gives. A profile's `currencies` must be a subsequence of this tuple IN THIS ORDER
# (`stream._require_currencies`), so this declaration is the single authority on what a
# drawn index means. Reordering it would force the mixed-currency profile to be re-declared
# in the new order, which would re-draw its stream -- so the order is refused from moving
# rather than left to a comment.
#
# A TUPLE PER PROFILE AND NEVER A SCALAR, which is the decision that matters. A currency
# constant per stream is functionally determined by the delivery -- perfectly correlated
# with `_batch_id` -- so `dim_currency` would reach cardinality 2 because two STREAMS
# exist, not because payments vary, and any `GROUP BY currency` would be a `GROUP BY batch`
# under another name. `BUSINESS_ATTRIBUTE_COLUMNS` below is "what the payment WAS, as
# opposed to which delivery of it this row is"; a per-delivery currency violates that
# sentence while passing every guard in this repository.
CURRENCIES = ("BRL", "USD")

# The currency this lakehouse reports in, and the one every stream declared before F-API
# draws from exclusively. It is `CURRENCIES[0]`, asserted below rather than assumed: the
# gold layer's `amount_brl` is denominated in it, `fx_rate` is 1.0 for it BY DEFINITION
# rather than by a lookup, and `stream.DEFAULT_CURRENCIES` is `(REPORTING_CURRENCY,)`.
REPORTING_CURRENCY = "BRL"

# The real Brazilian rails, in the RFB's own alphabet: no accents, upper case, no
# spaces. PIX is the instant-payment system, TED the same-day wire, BOLETO the printed
# bank slip. Ordered as declared and never sorted at use: the generator picks by index
# into this tuple, so reordering it re-draws every payment method in every stream.
PAYMENT_METHODS = (
    "PIX",
    "TED",
    "BOLETO",
    "CARTAO_CREDITO",
    "CARTAO_DEBITO",
)

# The amount's scale, in the currency's minor unit. TWO, because BRL has centavos, and
# because a fixed scale is what lets the amount be rendered from an INTEGER number of
# cents with no float anywhere in the path. A float would break byte-identity from a
# seed (repr rounding) long before it broke arithmetic.
#
# ONE SCALE FOR THE WHOLE DOMAIN, AND THE SECOND CURRENCY DID NOT MOVE IT -- which is
# luck rather than design, and is said here so nobody reads the sentence above as still
# describing a one-currency world. USD's minor unit is also 1/100, so `format_amount`
# renders both correctly from the same constant. A currency whose minor unit is not
# hundredths (JPY has none, KWD has thousandths) would make this a PER-CURRENCY fact, and
# adding one is therefore a contract change here rather than a value added to `CURRENCIES`.
AMOUNT_SCALE = 2

# --- schema drift: the column v1 deliberately does NOT declare -------------------------
#
# F1b Task 2's drift class makes a NEW OPTIONAL COLUMN APPEAR MID-STREAM. Its name lives
# here because a string shared by a generator, a DQ gate and an evidence query must have
# exactly ONE spelling -- this repository has already paid for a quarantine name spelled
# twice, which "sent estab triagers to a table full of unrelated F1.2 lookup rows".
#
# WHAT IT IS NOT is the load-bearing half: it appears in neither `COLUMNS`, nor
# `REQUIRED_COLUMNS`, nor `BUSINESS_ATTRIBUTE_COLUMNS`, and
# `_assert_no_drifting_column_is_declared_by_v1` refuses AT IMPORT any edit that puts it
# in one of them. Three separate things break if it is declared, and every one of them is
# SILENT -- each leaves a green suite behind:
#
#   1. `opl.generator.events.record_of` walks `COLUMNS`, so a drift column in there is
#      emitted on EVERY record. The stream would then carry no drift at all, and every
#      test of the drift class would still pass, because what they assert is that the
#      column is present.
#   2. Whatever builds the Auto Loader read schema builds it from this contract. A drift
#      column in the read schema is a declared STRING column that is merely NULL before
#      the drift point: nothing lands in `_rescued_data`, so `opl.bronze.dq.evaluate`'s
#      `rescued_data_present` -- the highest-precedence rule in the gate -- never fires,
#      and the drift is ABSORBED. "Caught by the gate rather than absorbed" is the phase
#      requirement; declaring the column is how it silently stops being met.
#   3. A drift column inside `BUSINESS_ATTRIBUTE_COLUMNS` reproduces this repository's own
#      documented defect on this phase's own evidence. `COUNT(DISTINCT a, b, c)` DROPS
#      every row that is NULL in any of a, b, c -- that is how 8,761 rows once went
#      missing -- and every record before the drift point is NULL in this column by
#      construction. The count would exclude the entire pre-drift population and report a
#      smaller, confident number. In the clean stream that defect is INERT, because no
#      contract column is ever NULL; the moment this column exists it is live.
#
# THE GENERAL RULE THE GUARD ACTUALLY ENFORCES, rather than a special case for this one
# name: A COLUMN THAT SOME ROWS DO NOT CARRY MAY NEVER BE A BUSINESS ATTRIBUTE. The
# attribute tuple is drawn from REQUIRED columns only, so a distinct count over it cannot
# drop a row. That statement survives a second drift column being added; "keep
# payment_channel out of the tuple" would not.
#
# AND THE NAME IS A TEMPTING ONE ON PURPOSE. A channel is exactly the kind of field
# somebody would argue belongs in the attribute tuple, and picking an obviously-not-an-
# attribute name (a producer version, a delivery counter) would have made the guard look
# unnecessary while leaving it untested against the case that will actually happen.
DRIFT_COLUMN = "payment_channel"

# Every column the drift class may inject. A tuple of one today, and a tuple rather than a
# bare name so that a second drift column is covered by the guard below without anyone
# having to remember to widen it.
DRIFT_COLUMNS = (DRIFT_COLUMN,)

# The drift column's value domain, in the same alphabet as `PAYMENT_METHODS`: upper-case
# ASCII, no accents, no spaces. Picked BY INDEX like every other domain here, so
# reordering this tuple re-draws the channel of every drifted record in every stream.
# NEVER EMPTY, and no member is: an empty string would be a second spelling of "absent",
# and the whole value of this column is that its NULL means exactly one thing -- the
# record was written before the producer started sending it.
DRIFT_VALUES = ("MOBILE", "INTERNET_BANKING", "TERMINAL")

# --- bronze naming -------------------------------------------------------------------
#
# THE STAGING/BRONZE/QUARANTINE TRIPLE, IN ONE LITERAL BLOCK, which is the property
# `opl.bronze.registry` exists to hold: the documented defect is a quarantine name
# hardcoded somewhere else that "sent estab triagers to a table full of unrelated F1.2
# lookup rows".
#
# DELIBERATELY NOT YET INSERTED INTO `opl.bronze.REGISTRY`, and this is a scope
# decision with four named causes rather than an omission. A key in that dict is bound
# by four invariants, and every one of them is satisfied by an artefact F1b TASK 3
# owns:
#
#   1. `tests/test_job_yaml_wiring.py::test_every_registered_table_has_an_ingestion_job`
#      asserts `set(_JOB_OF) == set(REGISTRY)`, so a registered table with no job YAML
#      under `databricks/resources/` turns the suite red.
#   2. `tests/bronze/test_rules.py::test_every_registered_table_has_a_rule_set`
#      requires `opl.bronze.rules.rules_for(contract)` to yield a rule set, which is
#      the DQ gate's verdict on this source -- Task 3's gate, and pyspark-bound.
#   3. `registry._assert_contracts_exist` refuses, AT IMPORT, a contract that is not in
#      `cnpj_schemas.TABLES`. Registering payments means giving the registry a contract
#      catalogue spanning both sources; done here it would be a widening no consumer
#      uses yet.
#   4. `registry._assert_prefixes_match_their_file_groups` refuses, AT IMPORT, a
#      contract that no `FILE_GROUPS` entry feeds -- and a GENERATED source will never
#      have one, because nothing downloads it. That guard needs a landing mode it does
#      not have ("written directly by the generator", beside `zips` and `local`).
#
# So the names live here, asserted against the live registry for collisions by
# `tests/test_payment_contract.py`, and Task 3 lifts them into a `BronzeTable(...)`
# without inventing a single new string. Landing under `cnpj/<month>/` would be wrong
# and is the one thing this block cannot yet say: `OplConfig.landing_table` is rooted
# at `landing_cnpj_root`, and payments are a SECOND SOURCE, not a CNPJ table. Where the
# subdir below hangs is Task 3's call, together with the job that reads it.
BRONZE_TABLE_KEY = "bronze_payments"
BRONZE_STAGING_TABLE = "bronze_payments_staging"
BRONZE_TABLE = "bronze_payments"
BRONZE_QUARANTINE_TABLE = "bronze_payments_quarantine"
LANDING_SUBDIR = "payments"


def _assert_the_columns_partition_cleanly() -> None:
    """Fail at import if `COLUMNS` is not exactly identity + the two timestamps + the
    business attributes, each appearing once.

    AT IMPORT, beside the declaration, for `opl.bronze.registry`'s reason: the tuples
    above are read by two different consumers with two different needs -- the
    serialiser walks `COLUMNS`, the T2 test walks `BUSINESS_ATTRIBUTE_COLUMNS` -- and a
    column added to one and not the other does not fail. It produces a stream whose
    dedup test silently stops covering a column, or a serialiser that drops one.

    A plain ValueError: nothing here is an unknown table, and no operator supplied it.
    This is a source edit that broke a declaration."""
    named = (IDENTITY_COLUMN, EVENT_TIME_COLUMN, EMITTED_AT_COLUMN)
    expected = (*named, *BUSINESS_ATTRIBUTE_COLUMNS)
    if COLUMNS != expected:
        raise ValueError(
            f"COLUMNS is {COLUMNS}, but identity + timestamps + business attributes "
            f"spell {expected}. The serialiser walks COLUMNS and the duplicate/repeat "
            "test walks BUSINESS_ATTRIBUTE_COLUMNS; a column in one and not the other "
            "is dropped from the stream or dropped from the test, silently."
        )
    if len(set(COLUMNS)) != len(COLUMNS):
        raise ValueError(
            f"COLUMNS repeats a name: {COLUMNS}. A repeated key in a JSON object is "
            "last-one-wins, so the duplicate would not even be visible in the output."
        )
    if not set(COUNTERPARTY_COLUMNS) <= set(BUSINESS_ATTRIBUTE_COLUMNS):
        raise ValueError(
            f"COUNTERPARTY_COLUMNS {COUNTERPARTY_COLUMNS} is not a subset of "
            f"BUSINESS_ATTRIBUTE_COLUMNS {BUSINESS_ATTRIBUTE_COLUMNS} -- Task 4 "
            "measures CNPJ resolution over the counterparties, and a counterparty "
            "that is not a business attribute would be invisible to the repeat test."
        )


def _assert_no_drifting_column_is_declared_by_v1() -> None:
    """Fail at import if a drift column has been added to anything v1 declares.

    Three checks for three distinct silent failures -- see the `DRIFT_COLUMN` block
    above. The third is the one that reproduces this repository's own NULL-dropping
    `COUNT(DISTINCT ...)` defect, and the fourth check generalises it: it is
    NULLABILITY, not this particular name, that makes a column unusable in a distinct
    count, so a second optional column added later is refused without an edit here."""
    for name, declared in (
        ("COLUMNS", COLUMNS),
        ("REQUIRED_COLUMNS", REQUIRED_COLUMNS),
        ("BUSINESS_ATTRIBUTE_COLUMNS", BUSINESS_ATTRIBUTE_COLUMNS),
    ):
        intruders = [column for column in DRIFT_COLUMNS if column in declared]
        if intruders:
            raise ValueError(
                f"{intruders} are drift columns and {name} declares them. A drift column "
                "this contract declares is not drift: the serialiser would emit it on "
                "every record, an Auto Loader read schema built from the contract would "
                "absorb it instead of rescuing it, and a DISTINCT over the business "
                "attributes would silently drop every row written before the drift "
                "point -- this repository's own 8,761-row defect, on its own evidence."
            )
    nullable = [column for column in BUSINESS_ATTRIBUTE_COLUMNS if column not in REQUIRED_COLUMNS]
    if nullable:
        raise ValueError(
            f"{nullable} are business attributes that are not required. A column some "
            "rows do not carry cannot take part in a distinct count: SQL drops the whole "
            "ROW rather than the missing value, so the answer comes back smaller and "
            "reads as a cleaner stream than the one that exists."
        )


def _assert_the_currency_domain_leads_with_the_reporting_currency() -> None:
    """Fail at import if `CURRENCIES` no longer declares a usable value domain.

    THREE CHECKS FOR THREE THINGS THAT MOVE LANDED BYTES OR A DIMENSION'S MEMBERS, and
    none of them fails at run time.

    A DUPLICATED MEMBER is silent twice over. `dim_currency` would append two rows on one
    natural key, and any profile drawing from a tuple containing the repeat would give
    that currency twice its declared share while the declaration still read as one
    member -- so a published mix would be wrong with every count clean.

    A REORDERED DOMAIN re-draws a stream. `derivation.pick` is positional and
    `stream._require_currencies` requires a profile's tuple to be a subsequence of this
    one IN ORDER, so moving `REPORTING_CURRENCY` off the front would force the
    mixed-currency profile to be re-declared as `("USD", "BRL")` -- at which point index
    0 means USD and every currency in that stream flips. Refusing the reorder is what
    makes the byte-identity argument survive an edit here.

    A REPORTING CURRENCY THAT IS NOT A MEMBER is the third: `stream.DEFAULT_CURRENCIES`
    is `(REPORTING_CURRENCY,)`, so it would be a one-tuple drawing a value
    `dim_currency` has no member for, and every fact row of every existing stream would
    resolve to the ghost."""
    if len(set(CURRENCIES)) != len(CURRENCIES):
        raise ValueError(
            f"CURRENCIES repeats a member: {CURRENCIES}. `dim_currency` reads this tuple "
            "as its member set, so a repeat appends two rows on one natural key -- and "
            "the generator picks BY INDEX, so a profile drawing from a tuple carrying the "
            "repeat gives that currency twice its declared share, silently."
        )
    if not CURRENCIES or CURRENCIES[0] != REPORTING_CURRENCY:
        raise ValueError(
            f"CURRENCIES is {CURRENCIES} and REPORTING_CURRENCY is {REPORTING_CURRENCY!r}, "
            "which must be its FIRST member. `pick` is positional and a profile's "
            "currencies must be a subsequence of the domain in the domain's own order, so "
            "moving the reporting currency off the front re-declares the mixed-currency "
            "profile in a new order and re-draws every row of its stream."
        )


_assert_the_columns_partition_cleanly()
_assert_no_drifting_column_is_declared_by_v1()
_assert_the_currency_domain_leads_with_the_reporting_currency()
