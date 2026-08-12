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
    `payer_cnpj_basico` / `payee_cnpj_basico`, and F2 wave 2's `hub_account` /
    `hub_customer` are where they become keys.
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
duplicate (F1b Task 2, not built yet) is a byte-identical redelivery of an existing
`transaction_id`. The clean stream already emits legitimate repeats, on purpose, so
that when duplicates arrive there is something for them to be confused with: a fixture
in which `COUNT(DISTINCT transaction_id)` and `COUNT(DISTINCT <business attributes>)`
agree is a fixture that tests nothing.

TWO TIMESTAMPS, AND NEITHER IS A WALL CLOCK. `event_time` is when the payment
happened; `emitted_at` is when the generator released the record. Lateness is the
ORDERING PROPERTY BETWEEN THEM -- a record is late when it is emitted after records
whose `event_time` is newer than its own -- so it can be stated, generated and
asserted without any process ever reading the clock. Both are derived from the
stream's own declared window; see `opl.generator.stream`. In the clean stream the two
orders agree, which is precisely the property Task 2's late arrivals violate.

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
what makes Task 2's schema drift measurable -- a NEW column appearing mid-stream makes
every earlier row NULL in it, and no other mechanism competes for that meaning.

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

# The contract's version. BUMPED, never edited in place: Task 2's schema drift adds an
# optional column mid-stream, and a reader has to be able to say which version a row
# was written under. v1 is the clean stream -- every column below required, no optional
# columns at all -- which is what makes "a NULL means the field was absent" true.
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

# v1 has no optional column. Declared as its own name rather than as "= COLUMNS" at the
# use site, because Task 2's drift adds a column that is NOT required, and the two
# tuples stop being equal at that moment -- a reader who learned "required means all of
# them" from an alias would carry that into the version where it is false.
REQUIRED_COLUMNS = COLUMNS

# --- the value domains -----------------------------------------------------------

# One currency, and it is a column rather than an assumption. BRL is the only money
# this stream carries today; the column exists so that the amount's two decimal places
# are a stated consequence of BRL's minor unit rather than a convention nobody wrote
# down, and so that a second currency is a value change instead of a schema change.
CURRENCIES = ("BRL",)

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
AMOUNT_SCALE = 2

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


_assert_the_columns_partition_cleanly()
