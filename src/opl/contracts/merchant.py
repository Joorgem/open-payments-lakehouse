# src/opl/contracts/merchant.py
"""Versioned schema contract for the operational merchant registry held in Postgres
(F-DB), in the shape `opl.contracts.ptax` and `opl.contracts.payments` take: the columns
are DATA here, and every mechanism that reads them lives elsewhere.

IT IMPORTS NOTHING, and that is a requirement rather than a tidiness note.
`opl.contracts.catalogue` joins this module into the mapping `opl.bronze.registry` reads,
and the registry is imported by the extraction scripts, which run on a host where pyspark
is an optional extra that is usually absent. An import here is an import there -- and this
source's extractor is the FIRST that runs on a host with no Databricks runtime at all
(plan T1), so the constraint is load-bearing here rather than inherited.

--- THE COLUMN-PROVENANCE SPLIT, AND ITS RULE IS THE LEADING UNDERSCORE --------------

Fourteen columns, in two groups, and WHICH GROUP A COLUMN IS IN IS LEGIBLE FROM ITS NAME:

  * `SOURCE_COLUMNS` are the eleven columns the `merchant` table in Postgres declares
    (plan §4), under the names Postgres declares them with and rendered by Postgres.
  * `OBSERVATION_COLUMNS` are stamped by US, from the transaction that observed them, and
    every one of them begins with an UNDERSCORE -- `_snapshot_at`, `_pg_snapshot`,
    `_pg_wal_lsn`.

THE UNDERSCORE IS THE DOCUMENTATION, and it is the convention bronze already uses for a
column this lakehouse put there (`_ingested_at`, `_batch_id`, `_record_source`). A reader
who knows only that rule can answer "where did this value come from?" for every column of
this table without opening another file, and a future column named on the wrong side of
the rule is visibly wrong rather than merely undocumented.

AND THE SPLIT IS ENFORCED, NOT DESCRIBED. `_assert_the_provenance_split_is_a_partition`
refuses AT IMPORT any overlap between the two tuples, any column of `COLUMNS` in neither,
and any column whose NAME disagrees with the group it was put in.

WHY THAT LAST HALF EXISTS, AND IT IS THIS PHASE'S HEADLINE RATHER THAN A NAMING RULE. The
tempting edit here is `_snapshot_at` becoming a projection of `updated_at` -- both are
timestamps, both are about "when", and on a quiet table they move together. They are
different facts and the difference IS the phase:

  * `updated_at` is stamped by the writing transaction and orders by transaction START.
  * `_snapshot_at` is stamped by the READING transaction and the rows it labels are the
    rows that were VISIBLE to it, which orders by transaction COMMIT.

Measured (evidence §0.4): a row stamped t1, committed after a faster transaction stamped
t2 > t1 and committed, is never returned by `WHERE updated_at > t2` -- not then, not ever.
That row is the one non-authored number in this phase's headline. Collapse the two columns
and the vault's observation ledger is keyed on when a row last CHANGED rather than on when
this lakehouse LOOKED, every count stays green, and the miss the phase exists to measure
becomes unmeasurable.

`_snapshot_at` IS A CONTRACT COLUMN AND NOT AN AUDIT COLUMN, which is plan T8 and is the
first time this repository has needed the distinction. `opl.bronze.autoloader` stamps four
audit columns from the RUN -- when it ingested, who produced the bytes, which batch, which
month the operator asked for. None of those can answer "which of the two snapshots is this
row from", because both snapshots are ingested by the same job in the same month. The
instant has to travel IN THE FILE, from the transaction that read it, so it is declared
here beside the columns it labels. `_snapshot_ref_date` is then DERIVED from it
(`opl.bronze.snapshot.ref_date_from_instant`), which is the third audit-column path and the
subject of T8: three sources, three times, the key you must diff on is not in the payload.

ITS SPELLING IS ALSO `opl.bronze.snapshot_axis.INSTANT_SNAPSHOT.column`, and the two are
CROSS-CHECKED rather than trusted -- `registry._assert_a_non_monthly_axis_is_a_contract_
column` refuses at import a source whose axis names a column its contract does not carry.
This module cannot import that one (it imports nothing), so the duplicate is made a
cross-check in the one module that already sees both, exactly as `BronzeTable.prefix` is a
cross-check on `FILE_GROUPS`.

--- ALL-STRING IN BRONZE, AND POSTGRES DOES THE RENDERING ----------------------------

Matching every other contract here (`opl.bronze.schema`). The rendering is the DATABASE's
(`col::text` under the seven GUCs plan T4 pins and reads back), never Python's: F-API
measured `json.loads` -> `float` -> `str` dropping the trailing zero of `5.07730`, and the
same class here would silently restate `credit_limit` at a different scale.

WHICH IS WHY `trade_name` IS THE ONLY NULLABLE COLUMN, AND WHY THAT MATTERS ENOUGH TO
DECLARE. Postgres renders a NULL as SQL NULL and an empty string as `''`, and the landed
JSON keeps them apart (`null` against `""`). The seeder produces BOTH, deliberately
(`scripts/merchant_population._trade_name`): a column that is only ever one of them cannot
demonstrate that the landing path keeps them distinct. Every other column is `NOT NULL` in
the DDL, so `REQUIRED_COLUMNS` is not a judgement call -- it is the DDL, read across.

NO DRIFT COLUMN, and the absence is the payments contract's decision taken for a different
source. `opl.contracts.payments` declares one because F1b's phase requirement is to EXHIBIT
schema drift and the payment stream is ours to shape. This source is an operational
database, and the drift class it exists to demonstrate is a ROW's presence changing, not a
column's. A column Postgres does not have would be a fabricated field attributed to the
source in `_record_source`.

NO CREDENTIAL, EVER. The DSN lives in the environment (`OPL_POSTGRES_DSN`), is read by the
script that owns the connection, and never reaches this module or any other module in the
wheel -- there is nothing here for a secret to attach to and there must never be.
"""
from __future__ import annotations

# The contract's version. It bumps when a column is ADOPTED, which for this source means
# the operational schema grew a column and this lakehouse decided to declare it -- a change
# with a diff, not a side effect of a migration upstream. An UNDECLARED new Postgres column
# is refused by the extraction's own catalog read, before anything is landed.
SCHEMA_VERSION = 1

# The contract key, in the sense `opl.bronze.registry.BronzeTable.contract` means it.
CONTRACT = "merchant"

# --- the columns Postgres declares -----------------------------------------------------
#
# Plan §4's DDL, read across, in its own order. The types excluded from that DDL each have
# a measured reason (`char(n)`, `float8`, `jsonb`, `json`, arrays, `interval`, `money`) and
# the reasons live in the plan; what survives into this module is that every column below
# renders to text under the pinned GUCs and reads back through the type's own input
# function.

# The registry's OWN surrogate key, and plan T5's whole argument. `hub_merchant` is keyed
# on THIS, not on the CNPJ: `loading.hash_key_expression` hashes the padded key components
# and nothing else, so a hub keyed on `cnpj_basico` would produce the byte-identical digest
# `hub_empresa` already holds over 69,062,849 keys -- two tables, one key space, and no
# guard anywhere refuses it (ADR 0011 rejected exactly that shape once already).
MERCHANT_ID_COLUMN = "merchant_id"
# Fourteen digits, as TEXT and never as a number. 142 of the 1,024 pinned counterparty
# roots carry a LEADING ZERO (evidence §0.3), so any cast to numeric destroys 13.9% of this
# pool silently. Its first eight characters are the root that reaches `hub_empresa` through
# `link_merchant_empresa` -- an ATTRIBUTE of the merchant, which is what makes the
# integration claim a link rather than a collision.
CNPJ_COLUMN = "cnpj"
LEGAL_NAME_COLUMN = "legal_name"
# THE ONE NULLABLE COLUMN. See the module docstring: NULL and '' are different values here
# and the source produces both.
TRADE_NAME_COLUMN = "trade_name"
# NO VALUE DOMAIN IS DECLARED for `status`, `mcc` or `risk_tier`, and no CHECK constraint is
# registered for any of them -- the reason the payments entry gives about `currency`. These
# are value domains that GAIN members, and a CHECK would turn an INSERT upstream into a
# migration on a live bronze table.
STATUS_COLUMN = "status"
MCC_COLUMN = "mcc"
SETTLEMENT_ACCOUNT_COLUMN = "settlement_account"
RISK_TIER_COLUMN = "risk_tier"
# `numeric(14,2)`, with its SCALE pinned by the DDL rather than by arithmetic: measured,
# `1.0 * 1.0` renders `'1.00'` and `'1e3'` renders `'1000'`, so the scale a digest is taken
# over must be the DECLARED one. Trailing zeros survive `::text` (`10.500` -> `'10.500'`),
# so the F-API `5.07730` defect does not recur through this path.
CREDIT_LIMIT_COLUMN = "credit_limit"
# THE EFFECTIVITY ENTRY COLUMN (T5), and `NOT NULL` in the DDL because
# `opl.vault.effectivity` warns that a NULL entry date SORTS FIRST in Spark and beats a
# delivered one. The gate asserts it non-blank so a nullable open can never reach the
# loader.
ONBOARDED_ON_COLUMN = "onboarded_on"
# THE WATERMARK COLUMN, and it is landed because the phase's headline is about what it
# CANNOT say. It is maintained by a `BEFORE UPDATE` trigger -- a bare `DEFAULT now()` does
# not fire on UPDATE, measured -- and even correctly maintained it orders by transaction
# START while visibility orders by transaction COMMIT.
UPDATED_AT_COLUMN = "updated_at"

SOURCE_COLUMNS = (
    MERCHANT_ID_COLUMN,
    CNPJ_COLUMN,
    LEGAL_NAME_COLUMN,
    TRADE_NAME_COLUMN,
    STATUS_COLUMN,
    MCC_COLUMN,
    SETTLEMENT_ACCOUNT_COLUMN,
    RISK_TIER_COLUMN,
    CREDIT_LIMIT_COLUMN,
    ONBOARDED_ON_COLUMN,
    UPDATED_AT_COLUMN,
)

# --- the columns the OBSERVING TRANSACTION stamps ---------------------------------------

# The snapshot axis (T7/T8). `to_char(clock_timestamp() AT TIME ZONE 'UTC',
# 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`, generated in SQL as the transaction's FIRST statement
# -- which is what acquires the REPEATABLE READ snapshot and stamps it in one go. Not
# `::text`, which yields a space instead of `T` and `+00` instead of `Z` and TRIMS trailing
# fractional zeros, and not Python, which T4 forbids.
SNAPSHOT_AT_COLUMN = "_snapshot_at"
# THE EXACT VISIBILITY SET, and the reason it is landed beside the wall clock: it answers
# "which two snapshots did the diff compare" exactly, where a wall clock can step backwards
# under NTP. `pg_current_snapshot()::text` is `xmin:xmax:xip_list`.
PG_SNAPSHOT_COLUMN = "_pg_snapshot"
# MONOTONIC, where the wall clock is not. `pg_current_wal_lsn()::text`. `txid_current()` is
# REFUSED as a third: measured, it assigns an XID even inside a `READ ONLY` transaction and
# advances the cluster counter -- a WAL write from a transaction whose whole point is to be
# a read.
PG_WAL_LSN_COLUMN = "_pg_wal_lsn"

OBSERVATION_COLUMNS = (
    SNAPSHOT_AT_COLUMN,
    PG_SNAPSHOT_COLUMN,
    PG_WAL_LSN_COLUMN,
)

# The prefix that says a column was stamped by this lakehouse rather than read from
# Postgres. Named rather than written as a literal in the guard, because the guard tests it
# in both directions and two spellings of one rule is how a partition stops being one.
STAMPED_PREFIX = "_"

# ORDER IS AUTHORITATIVE, for `opl.contracts.ptax.COLUMNS`' reason: these are JSON objects,
# where the key decides what a value MEANS, but the order still decides the BYTES, and the
# landing writer refuses a file whose bytes differ from the ones it derived.
#
# SPELLED OUT RATHER THAN `(*SOURCE_COLUMNS, *OBSERVATION_COLUMNS)`, and that is what makes
# the partition guard below a check instead of a tautology. Derived, "a column in neither
# group" would be unreachable by construction and the guard would be decorative -- which
# this repository treats as worse than no guard, because the next reader believes the hole
# is closed.
COLUMNS = (
    MERCHANT_ID_COLUMN,
    CNPJ_COLUMN,
    LEGAL_NAME_COLUMN,
    TRADE_NAME_COLUMN,
    STATUS_COLUMN,
    MCC_COLUMN,
    SETTLEMENT_ACCOUNT_COLUMN,
    RISK_TIER_COLUMN,
    CREDIT_LIMIT_COLUMN,
    ONBOARDED_ON_COLUMN,
    UPDATED_AT_COLUMN,
    SNAPSHOT_AT_COLUMN,
    PG_SNAPSHOT_COLUMN,
    PG_WAL_LSN_COLUMN,
)

# THE DDL'S OWN NULLABILITY, READ ACROSS, and not a judgement call about live data. Every
# CNPJ contract's required set is a subset CHOSEN by measuring which RFB columns are never
# legitimately blank, because the RFB is a source this project does not control. This
# source's nullability is declared in `scripts/seed_merchant_db._DDL` -- one `NOT NULL` per
# column, one exception -- so the gate asserts the schema rather than a sample of it.
NULLABLE_COLUMNS = (TRADE_NAME_COLUMN,)

# Spelled out rather than derived from `COLUMNS - NULLABLE_COLUMNS`, for the reason
# `COLUMNS` is spelled out: the guard below compares the three declarations against each
# other, and a derived one cannot disagree.
REQUIRED_COLUMNS = (
    MERCHANT_ID_COLUMN,
    CNPJ_COLUMN,
    LEGAL_NAME_COLUMN,
    STATUS_COLUMN,
    MCC_COLUMN,
    SETTLEMENT_ACCOUNT_COLUMN,
    RISK_TIER_COLUMN,
    CREDIT_LIMIT_COLUMN,
    ONBOARDED_ON_COLUMN,
    UPDATED_AT_COLUMN,
    SNAPSHOT_AT_COLUMN,
    PG_SNAPSHOT_COLUMN,
    PG_WAL_LSN_COLUMN,
)

# Fourteen characters of digits: eight of root, four of establishment ordinal, two check
# digits. A LENGTH-AND-DIGITS fact rather than a numeric one -- see `CNPJ_COLUMN`.
CNPJ_WIDTH = 14
# The root that reaches `hub_empresa`, declared here because this contract is where the
# CNPJ's shape is documented and it can import nothing to borrow the number from.
#
# THE SENTENCE THAT USED TO BE HERE NAMED TWO CONSUMERS THAT DO NOT READ THIS DECLARATION,
# and the whole-branch code review is what caught it. It claimed "the DQ rule and the
# vault's link both need it and neither may re-derive it". Neither reads it: the DQ rule
# keeps its own literal in `opl.bronze.rule_predicates` -- and merchant has no root-width
# rule at all, it checks the FOURTEEN-character `cnpj` -- while `link_merchant_empresa`
# imports the constant from `opl.vault.domains.cnpj`.
#
# SO THIS IS THE FIFTH DECLARATION OF `8` IN THIS REPOSITORY, and the four others are
# `generator.cnpj_pool`, `vault.domains.cnpj`, `vault.partners` and
# `bronze.rule_predicates`. They are NOT collapsed into one: this module imports nothing by
# a constraint its own docstring argues, and two of the others are loaders proven over
# 33.13 GB. `tests/test_cnpj_pool.py` compares all five against each other instead --
# which is the check that was missing, since this constant's only reader asserted the
# module's own literal against itself and would have stayed green through any divergence.
CNPJ_BASICO_WIDTH = 8

# --- bronze naming ---------------------------------------------------------------------
#
# The staging/bronze/quarantine triple in ONE literal block, which is the property
# `opl.bronze.registry` exists to hold: the documented defect is a quarantine name spelled
# somewhere else that "sent estab triagers to a table full of unrelated F1.2 lookup rows".
BRONZE_TABLE_KEY = "bronze_merchant"
BRONZE_STAGING_TABLE = "bronze_merchant_staging"
BRONZE_TABLE = "bronze_merchant"
BRONZE_QUARANTINE_TABLE = "bronze_merchant_quarantine"
# The landing subdir, under the POSTGRES root -- a fifth landing mode and a fourth root, for
# the reason `opl.config` gives about the third: the discriminator is HOW THE BYTES ARRIVE.
LANDING_SUBDIR = "merchant"


def _assert_every_column_is_declared_once() -> None:
    """Fail at import if `COLUMNS` repeats a name or carries none.

    A repeated key in a JSON object is LAST-ONE-WINS, so a duplicate would not even be
    visible in the emitted record -- and it would give `struct_for` two fields of one name,
    which Spark accepts and then resolves arbitrarily.

    Emptiness is refused separately because an empty contract is the one shape that passes
    every other check here and produces a read schema with no columns.

    A plain ValueError: nothing here is an unknown table, and no operator supplied it."""
    if not COLUMNS:
        raise ValueError(
            "COLUMNS is empty, so struct_for(CONTRACT) would build a read schema with no "
            "fields and every value in the landed record would arrive as rescued data"
        )
    if len(set(COLUMNS)) != len(COLUMNS):
        raise ValueError(
            f"COLUMNS repeats a name: {COLUMNS}. A repeated key in a JSON object is "
            "last-one-wins, so the duplicate would not even be visible in the output."
        )


def _assert_the_provenance_split_is_a_partition() -> None:
    """Fail at import if a column is in BOTH provenance groups, or in NEITHER.

    See the module docstring for the edit this refuses -- `_snapshot_at` becoming a
    projection of `updated_at` -- and why it would take this phase's one non-authored
    number with it while every row count stayed green."""
    both = sorted(set(SOURCE_COLUMNS) & set(OBSERVATION_COLUMNS))
    if both:
        raise ValueError(
            f"{both} are declared as BOTH read from Postgres and stamped by the observing "
            "transaction. `updated_at` orders by transaction START and the snapshot "
            "instant orders by transaction COMMIT: a row stamped before a watermark and "
            "committed after it is unreachable by `WHERE updated_at > watermark` forever, "
            "which is the one number in this phase's headline nothing authored. One "
            "column doing both jobs deletes it with nothing to see."
        )
    unsplit = sorted(set(COLUMNS) ^ (set(SOURCE_COLUMNS) | set(OBSERVATION_COLUMNS)))
    if unsplit:
        raise ValueError(
            f"{unsplit} appear in COLUMNS without a provenance group, or in a group "
            f"without being a column. COLUMNS is {COLUMNS}, SOURCE_COLUMNS is "
            f"{SOURCE_COLUMNS} and OBSERVATION_COLUMNS is {OBSERVATION_COLUMNS}. This "
            "contract documents itself by the leading underscore -- one means this "
            "lakehouse stamped the value, none means Postgres declared the column -- and "
            "a column on neither side makes that rule unreadable rather than merely "
            "undocumented."
        )


def _assert_the_names_agree_with_the_groups() -> None:
    """Fail at import if a column's NAME disagrees with the group it was put in.

    THE HALF THAT MAKES THE RULE READABLE RATHER THAN MERELY TRUE. The partition above is
    satisfied by any assignment at all; this is what says the assignment is the one the
    module docstring describes, so a reader can trust the underscore instead of checking
    the tuples. A Postgres column that really began with an underscore would be refused
    here -- correctly: it could not be told from a column this lakehouse stamped, and the
    landed table is where that question has to be answerable."""
    stamped_but_named_like_a_source_column = sorted(
        column for column in OBSERVATION_COLUMNS if not column.startswith(STAMPED_PREFIX)
    )
    if stamped_but_named_like_a_source_column:
        raise ValueError(
            f"{stamped_but_named_like_a_source_column} are stamped by the observing "
            f"transaction and are not named with the {STAMPED_PREFIX!r} prefix that says "
            "so. Every other column of a bronze table this lakehouse put there carries it "
            "(_ingested_at, _batch_id, _record_source), and a stamped value wearing a "
            "source column's name is a claim that Postgres declared it."
        )
    read_but_named_like_a_stamp = sorted(
        column for column in SOURCE_COLUMNS if column.startswith(STAMPED_PREFIX)
    )
    if read_but_named_like_a_stamp:
        raise ValueError(
            f"{read_but_named_like_a_stamp} are read from Postgres and are named with the "
            f"{STAMPED_PREFIX!r} prefix this contract reserves for values it stamps "
            "itself. The landed table has to be able to answer where a value came from, "
            "and a source column wearing a stamp's name makes that unanswerable."
        )


def _assert_required_is_the_ddl_read_across() -> None:
    """Fail at import if required/nullable do not partition `COLUMNS`.

    The DDL declares one `NOT NULL` per column and one exception, so these three tuples
    are one fact written three ways and the guard is what stops them becoming three facts.
    A column in neither is one the gate never looks at; a column in both is a rule that
    rejects a value the source is entitled to send, on an ALL-OR-NOTHING gate.

    THE NULLABLE HALF IS THE ONE THAT COSTS SOMETHING IF IT IS WRONG. `_null_or_blank`
    treats `''` as blank, so putting `trade_name` in `REQUIRED_COLUMNS` would reject every
    row whose trade name the source deliberately left empty -- and the whole reason that
    column is nullable is to demonstrate that the landing path keeps `NULL` and `''`
    apart."""
    unsplit = sorted(set(COLUMNS) ^ (set(REQUIRED_COLUMNS) | set(NULLABLE_COLUMNS)))
    overlap = sorted(set(REQUIRED_COLUMNS) & set(NULLABLE_COLUMNS))
    if overlap or unsplit:
        raise ValueError(
            f"required/nullable do not partition COLUMNS: {overlap} are in both and "
            f"{unsplit} are in neither, or are declared without being columns. "
            f"REQUIRED_COLUMNS is {REQUIRED_COLUMNS} and NULLABLE_COLUMNS is "
            f"{NULLABLE_COLUMNS}. The source's DDL is one NOT NULL per column with one "
            "exception; these tuples are that fact read across, and a column in neither "
            "is one the DQ gate never asks about."
        )


_assert_every_column_is_declared_once()
_assert_the_provenance_split_is_a_partition()
_assert_the_names_agree_with_the_groups()
_assert_required_is_the_ddl_read_across()
