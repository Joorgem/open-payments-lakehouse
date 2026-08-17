# src/opl/bronze/rules.py
"""Per-table bronze DQ rule sets: ordered (reason, predicate) pairs,
first-match-wins. The universal _rescued_data check lives in dq.evaluate,
above any per-table rule.

Predicates are zero-arg factories (Callable[[], Column]) rather than eager
Column objects: PySpark cannot build a Column without an active SparkContext,
and rules_for is inspected (names, unknown-table KeyError) in pure-Python
tests that hold no session. The factory defers Column construction to
evaluate() time, where a DataFrame — hence a live session — always exists.

WHAT EACH PREDICATE TESTS FOR NOW LIVES IN `opl.bronze.rule_predicates`, and
this file is the half that decides WHICH rules a contract runs and IN WHAT
ORDER. Split at 791 of this project's 800-line file cap, with no behaviour in
the split. What stayed is what a reader consults this file for: the
per-contract `REQUIRED_FIELDS` declaration and the rule-set summary above
`rules_for`, which `tests/bronze/test_rule_set_prose.py` reads out of THIS
file's comments and holds against the sets below them -- so moving that block
would have moved a guard away from its subject."""
from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column

from opl.bronze.rule_predicates import (
    _CREDIT_LIMIT_TYPE,
    _bad_cnpj,
    _bad_iso_date,
    _bad_snapshot_instant,
    _basico_length,
    _case_divergence_check,
    _cnpj_basico_length,
    _encoding_check,
    _null_or_blank,
    _unparseable_decimal,
    _unparseable_publication_instant,
    _unparseable_rate,
    _unprovable_ref_date,
)
from opl.bronze.snapshot import SNAPSHOT_REF_DATE_COLUMN
from opl.contracts.merchant import (
    CNPJ_COLUMN,
    CREDIT_LIMIT_COLUMN,
    ONBOARDED_ON_COLUMN,
    SNAPSHOT_AT_COLUMN,
)
from opl.contracts.merchant import CONTRACT as MERCHANT_CONTRACT
from opl.contracts.merchant import REQUIRED_COLUMNS as MERCHANT_REQUIRED_COLUMNS
from opl.contracts.payments import CONTRACT as PAYMENTS_CONTRACT
from opl.contracts.payments import COUNTERPARTY_COLUMNS, REQUIRED_COLUMNS
from opl.contracts.ptax import CONTRACT as PTAX_CONTRACT
from opl.contracts.ptax import PUBLISHED_AT_COLUMN, QUOTE_DATE_COLUMN, RATE_COLUMNS
from opl.contracts.ptax import REQUIRED_COLUMNS as PTAX_REQUIRED_COLUMNS

# ONE spelling, because this string is the only thing tying the rule below to its
# declaration in REQUIRES_COLUMN. Two literals would drift in the direction that
# does not announce itself: the declaration would stop matching, the rule would
# stop being skippable, and the symptom is an UNRESOLVED_COLUMN raised inside the
# DQ gate task -- after ingest has already written staging -- rather than anything
# visible here. Every other reason string in this module is a literal because it
# is written in exactly one place; this one is not.
_UNPROVABLE_REF_DATE = "unprovable_snapshot_ref_date"

# A rule that reads a column the contract does not declare. The frame decides:
# the snapshot columns exist on the INGESTED frame and not on a bare contract
# frame, and both are legitimate inputs to `evaluate`. Declared per reason rather
# than discovered by catching AnalysisException -- a blanket except would also
# swallow a rule with a typo'd column name, which is the "silently produce a
# wrong answer" shape this gate exists to refuse.
#
# ONLY METADATA COLUMNS BELONG HERE, and that is a hard line rather than a habit:
# a CONTRACT column declared skippable would turn a broken ingest into a clean
# run over data the gate never looked at, which is exactly the leniency
# `test_a_rule_set_refuses_a_frame_that_is_missing_a_contract_column` was written
# to forbid. Guarded by a test, not left to this comment.
REQUIRES_COLUMN: dict[str, str] = {
    _UNPROVABLE_REF_DATE: SNAPSHOT_REF_DATE_COLUMN,
}

# Columns that are never legitimately blank, per contract. DECLARED, not derived
# from position: `empresas.ente_federativo_responsavel` is the LAST column and is
# empty for every private company, so "the tail is NULL" is not a defect signal.
# What IS a defect signal is a column the RFB always fills coming back empty --
# that only happens when the record was truncated, which is how both F1.3 parse
# defects passed the gate.
#
# `estabelecimentos.municipio` is the one that closes carry-forward #4 for the
# table the defects actually happened in. The three keys already checked
# (cnpj_basico/ordem/dv) sit at contract indices 0-2, so a truncated record keeps
# all three and passes -- the check has to reach PAST the truncation point to see
# it. `municipio` is index 20 of 30 and is MEASURED clean: over the 71,874,448
# rows live when this was written (2026-06), blanks in municipio,
# situacao_cadastral, uf and data_inicio_atividade are 0 -- and the 2026-07 ingest
# re-confirmed it on a further 72,318,968 staged rows whose only rejects were 4
# `encoding_replacement_char`. Declaring it required therefore rejects nothing
# that exists today, which is why it can be added to a live table's rule set at
# all. That is a claim about a (month, rule set) pair, and it is re-earned every
# month, not inherited.
#
# What is deliberately ABSENT is as much a decision as what is present, because
# the gate is all-or-nothing (any reject fails the run) and every entry here is a
# new way for a future batch to turn a run red:
#   - `socios.cpf_cnpj_socio` is blank for foreign shareholders -- the population
#     `nome_cidade_exterior`/`pais` exist to describe -- so requiring it would
#     reject a legitimate class wholesale.
#   - `empresas.capital_social` and `porte_empresa` are unmeasured on the live
#     table. They are plausibly always filled; "plausibly" is not the standard a
#     hard gate is held to, and either can be added once counted.
# A tuple per contract, not a list: these are read by `_required_rules` on every
# call and nothing may append to a shared default.
#
# PAYMENTS IS THE ONE ENTRY THAT IS NOT A JUDGEMENT CALL, and it is derived rather
# than listed for that reason. Every other contract's tuple is a subset chosen by
# measurement -- which RFB columns are never legitimately blank on live data -- because
# the RFB is a source this project does not control. The payment stream is generated
# here, `opl.contracts.payments` declares EVERY column required in v1, and
# `record_of`'s `dict[str, str]` return type plus `format_amount`'s refusal of
# non-positive input make "no contract column is ever blank" a property of
# construction. So the gate asserts the contract's own claim in full: anything less
# would be a gate that is looser than the generator, and the difference is exactly the
# set of columns a bug between the two could empty without anyone noticing.
#
# DERIVED FROM `REQUIRED_COLUMNS`, NOT PASTED, so a v2 that adds a column adds its
# rule. That is safe here and would NOT be safe for an RFB contract: adding a column
# to a CNPJ tuple is a new way for a live table's ingest to go red and has to be
# earned by a count. Here the rule set and the contract have one author.
REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "lookup": ("codigo", "descricao"),
    "estabelecimentos": ("cnpj_basico", "cnpj_ordem", "cnpj_dv", "municipio"),
    "empresas": ("cnpj_basico", "razao_social", "natureza_juridica"),
    "socios": ("cnpj_basico", "identificador_socio", "nome_socio_razao_social",
               "qualificacao_socio"),
    PAYMENTS_CONTRACT: tuple(REQUIRED_COLUMNS),
    # PTAX IS DERIVED FOR PAYMENTS' REASON, WITH A DIFFERENT AUTHOR ON THE OTHER SIDE.
    # The contract declares every column required, and `opl.extraction.ptax_source`
    # already refuses a response row missing any of the API's three fields and carries
    # the request's own two onto every quote -- so "no column is ever blank" is a
    # property of the layer above rather than a hope about BCB. Anything blank here means
    # something between that validation and bronze emptied a column, which is exactly the
    # set of columns a bug in the landing writer could empty without anyone noticing.
    PTAX_CONTRACT: tuple(PTAX_REQUIRED_COLUMNS),
    # MERCHANT IS DERIVED AND IT IS THE ONE ENTRY THAT IS A SCHEMA RATHER THAN A SAMPLE.
    # Every CNPJ tuple above is a subset chosen by MEASURING which columns are never blank
    # on live RFB data, because that source is one this project does not control and every
    # entry is a new way for a live table's ingest to go red. This source's nullability is
    # DECLARED, in `scripts/seed_merchant_db._DDL`: one `NOT NULL` per column with exactly
    # one exception, and `opl.contracts.merchant` carries that DDL read across. So the gate
    # asserts the schema itself, and the one nullable column -- `trade_name`, which the
    # source emits as NULL, as `''` and as a name on purpose -- is absent by construction
    # rather than by a decision taken here.
    MERCHANT_CONTRACT: tuple(MERCHANT_REQUIRED_COLUMNS),
}


def _required_rules(contract: str) -> list[tuple[str, Callable[[], Column]]]:
    """ONE RULE PER REQUIRED COLUMN, so the reject reason names WHICH column was
    empty rather than only that one of them was.

    Generated rather than hand-listed, and named to the existing convention on
    purpose: `null_or_empty_codigo`, `null_or_empty_cnpj_ordem` and
    `null_or_empty_cnpj_dv` come out byte-identical to the strings the lookup and
    estabelecimentos sets already produce. That matters because those strings are
    DATA -- they sit in live quarantine tables, and a triager filtering on them
    should not have to know which release wrote the row. Collapsing them into one
    `missing_required_field` would have split that vocabulary and thrown away
    which column failed.

    Raises KeyError for a contract with no declared required set, which is the
    same refusal `rules_for` makes and is caught by the same guard
    (`test_every_registered_table_has_a_rule_set`)."""
    return [
        (f"null_or_empty_{column}", _null_or_blank(column))
        for column in REQUIRED_FIELDS[contract]
    ]


# --- WHY THE RULE SETS BELOW ARE ORDERED THE WAY THEY ARE --------------------
#
# Module level rather than inside `rules_for`'s docstring, for a boring reason
# with a real consequence: this prose grew past the point where the function
# reading it stayed under the project's 50-line limit. It is the reasoning, not
# the function, so it lives here and the function stays a dict and a return.
#
# ORDER IS THE CONTRACT, not a detail, because only the FIRST matching rule's
# reason is recorded. Required-field rules run first as a group, then the shape
# check, then the encoding check: a row missing a key field is described by what
# is MISSING rather than by the shape of what is left, and a row whose bytes are
# damaged is only reported as such once nothing simpler explains it. Pinned per
# contract in tests/bronze/test_rules.py.
#
# One consequence of grouping, called out because it changed a live table's
# output: estabelecimentos used to run `bad_cnpj_basico_length` BETWEEN
# `null_or_empty_cnpj_basico` and `null_or_empty_cnpj_ordem`. A row that is both
# short in cnpj_basico and blank in a later required column now reports the blank
# column. Both reasons are true of that row and it is rejected either way -- a
# reporting change, not a gate change.
#
# `unprovable_snapshot_ref_date` is LAST, below even the encoding check, and for
# a reason the position alone does not carry: it is the only rule here that
# describes the FILE rather than the row, so when it fires it fires for every row
# of that file at once. Ranked any higher it would become the reason printed
# across a whole quarantine, burying the per-row defects -- a truncated record, a
# lost byte -- that a triager can act on. A row is judged by what is wrong with
# IT, and only then by where it came from.
#
# NOT ON LOOKUP, which is a scope line rather than a claim that lookup cannot
# drift: its rows carry `_snapshot_ref_date` too, so a lookup filename changing
# shape still goes unremarked. KNOWN GAP, and the reason it is a gap rather than
# a decision is worth being exact about -- the measurement that would justify
# closing it already exists (7,408 rows of bronze_cnpj_lookup, null_ref_date=0,
# same evidence doc). What holds it open is only that the lookup set is pinned
# byte-for-byte to what F1.2 shipped, and F1.4b's scope is the three tables it
# introduces. Adding a new way for the one table already in production to go red
# belongs in a change that says so, not as a rider on this one.
#
# --- AND NOT ON PAYMENTS, WHICH IS A DIFFERENT ANSWER TO THE SAME QUESTION -------
#
# The payments set is the only one below that omits `unprovable_snapshot_ref_date`
# because THE COLUMN DOES NOT EXIST on those rows, not because the check was
# declined. `_snapshot_ref_date` is "the date the source declares in its own
# filename" (`opl.bronze.snapshot`), which is a fact about the RFB's mainframe naming
# convention; a stream this lakehouse generates declares no such thing, so
# `add_common_audit_columns` does not stamp it. Had it been stamped anyway the column
# would be NULL on every payment row, this rule would reject every one of them, and
# the only way to load the table would have been to leave the rule out -- a control
# omitted so that the value it refuses can be written, which is the shape this
# repository calls a control that disappeared rather than failed.
#
# WHAT THE PAYMENTS SET DOES CARRY, and why each earns a place on an ALL-OR-NOTHING
# gate where every rule is a new way for a run to go red:
#
#   - Eight `null_or_empty_*`, one per contract column, DERIVED from
#     `payments.REQUIRED_COLUMNS` (see REQUIRED_FIELDS above). The contract declares
#     every v1 column required and the generator makes that true by construction, so
#     a blank here means something between the generator and bronze emptied a column.
#   - `bad_payer_cnpj_basico_length` and `bad_payee_cnpj_basico_length`. These are the
#     integration claim, checked at the gate rather than after the fact: the whole
#     premise is that payments join to real companies by business key, and the
#     failure that would break it silently is a numeric round trip eating a leading
#     zero -- `00000004` becoming `4`. Task 4 measures 100% resolution against
#     `hub_empresa` AFTER the promote; this refuses the rows BEFORE it, so bronze
#     cannot come to hold keys that resolve to nothing. They reject nothing the
#     generator can emit: `cnpj_pool.validated_pool` already refuses a key of any
#     other width where the pool is built.
#   - `encoding_replacement_char`, folded over all eight columns, which is the live
#     control on this phase's central risk -- see `_encoding_check`.
#
# WHAT IT DELIBERATELY DOES NOT CARRY: any value-domain rule (`currency IN
# CURRENCIES`, `payment_method IN PAYMENT_METHODS`, an amount-format regex). The
# drift class F1b generates is SCHEMA drift, caught by `rescued_data_present` above
# every rule here; value drift was explicitly deferred by Task 2 ("value drift can
# follow if it earns its place"). A gate rule for a defect class nothing generates is
# a control no test exercises, and this gate is all-or-nothing.
#
# THE ORDER IS THE SAME ARGUMENT THE CNPJ SETS MAKE: what is MISSING before what is
# the wrong SHAPE before what is damaged in its BYTES. A row missing its payer is
# described by the missing payer rather than by that column's length.
#
# --- AND THE PTAX SET, WHOSE ENTRIES ARE MOSTLY NEAR-TAUTOLOGIES, SAID SO ---------
#
# Every rule below is a statement about a record this repository BUILDS, from a response
# `opl.extraction.ptax_source` has already validated. So most of them cannot fire against
# any body BCB has ever returned, and that is reported here rather than dressed up:
#
#   - Five `null_or_empty_*`, one per contract column, derived from the contract. The
#     extraction layer refuses a row missing any API field and carries the request's two
#     values onto every quote, so a blank means the landing writer emptied a column.
#   - `bad_quote_date_shape`. NOT a near-tautology, and the one rule here that is aimed at
#     a mistake somebody will actually make: the API is asked in `MM-DD-YYYY`, so the
#     request's own spelling is the wrong value most likely to be stamped, and it joins to
#     nothing in gold with every count green.
#   - `unparseable_cotacao_compra` / `unparseable_cotacao_venda`. Near-tautological -- the
#     writer stamps `str(Decimal(...))` -- and kept because an unreadable venda converts
#     every payment on that date at nothing.
#   - `unparseable_data_hora_cotacao`. The most load-bearing of the three, because it is
#     T3's COMPARATOR: a stamp the join cannot read does not fail, it resolves the payment
#     to an older quote. AND IT IS NOT A NEAR-TAUTOLOGY ANY MORE, which is F-API's fix
#     pass rather than a re-description. It was `to_timestamp(...).isNull()`, and Spark's
#     single-argument parser reads a BARE TIME as today's date -- so the rule accepted a
#     landed value that renders differently on two days and sorts AFTER every payment in
#     this phase's window instead of before it. The rule now refuses any stamp whose
#     instant its own text does not determine. See the function.
#   - `encoding_replacement_char`, folded over all five columns and SHADOWED ON FOUR OF
#     THEM. It is live for the reason it is live on payments -- the serialiser returns TEXT
#     and a writer that did not encode UTF-8 explicitly hands Java bytes it substitutes
#     U+FFFD for, silently -- but first-match-wins puts an earlier CONTENT rule on every
#     column except `currency`: a U+FFFD in `quote_date` breaks `bad_quote_date_shape`'s
#     regex, one in either rate makes the decimal cast NULL, and one in
#     `data_hora_cotacao` breaks `_INSTANT_SHAPE`. So the row is always REJECTED and
#     nothing gets through, and this name can only ever be the REPORTED reason for
#     `currency` -- which matters because these strings are data an operator filters a
#     quarantine on. It stays folded over all five anyway: the fold is derived from the
#     contract, so a v2 column arrives covered, and `currency` is the one column no other
#     rule inspects. Measured in
#     `test_a_replacement_character_is_caught_but_only_currency_REPORTS_it`.
#
# WHAT IT DELIBERATELY DOES NOT CARRY, and the second one is a ruling rather than a
# deferral:
#
#   - No value-domain rule (`currency IN (...)`, a rate range, a quote_date window). The
#     currency is decided by which endpoint was called, not by the body, and a plausible
#     rate range is a number nobody in this repository is entitled to assert about BCB.
#   - NO GAPLESSNESS RULE, and it cannot be one. T3 requires the landed series to be
#     contiguous in business days over the whole span the fact reaches, because "most
#     recent landed quote" otherwise returns a STALE rate successfully. That is a
#     statement about a day that is ABSENT -- and this gate TAGS ROWS. There is no row for
#     a missing day, so no rule here can ever see one. It belongs to whatever asserts the
#     series after it is landed, and putting a decorative version of it here would be a
#     control that reports green over exactly the case it names.
#   - No `unprovable_snapshot_ref_date`, for payments' reason: the column does not exist
#     on these rows. `_snapshot_ref_date` is the date a source declares in its own
#     FILENAME, which is a fact about the RFB's mainframe naming convention; BCB declares
#     no such thing, so `add_common_audit_columns` does not stamp it.
#
# --- AND THE MERCHANT SET, WHICH IS THE FIRST TO CARRY `unprovable_snapshot_ref_date` ---
# --- WITHOUT A FILENAME BEHIND IT --------------------------------------------------------
#
# The two sets above omit that rule because the COLUMN is absent from their rows. This
# source can neither omit it nor derive it the RFB's way, and plan T8 is why: it is the
# first non-file-fed source ever loaded into a VAULT SATELLITE, and `opl.vault.satellites`
# reads `_snapshot_ref_date` unconditionally to build `applied_date`. So a third
# audit-column path stamps it from the snapshot instant the EXTRACTOR carried
# (`opl.bronze.snapshot.ref_date_from_instant`), and this rule keeps its job: an instant the
# derivation cannot read yields NULL and the row is rejected in the gate rather than
# reaching a satellite with no applied date.
#
# WHAT THIS SET CARRIES, and why each earns a place on an ALL-OR-NOTHING gate:
#
#   - Thirteen `null_or_empty_*`, DERIVED from `merchant.REQUIRED_COLUMNS`. The only
#     contract here whose required set is a SCHEMA rather than a sample: the source's DDL is
#     one `NOT NULL` per column with exactly one exception.
#   - NOTHING ABOUT `trade_name`, and the absence is the decision. It is that one exception,
#     and the source emits NULL, `''` AND a name for it on purpose -- a column that is only
#     ever one of them cannot demonstrate that the landing path keeps NULL and `''` apart.
#     `_null_or_blank` treats `''` as blank, so a rule here would reject rows the source is
#     entitled to send.
#   - `bad_cnpj_shape`. THE INTEGRATION CLAIM, checked at the gate rather than after the
#     fact, and the twin of payments' `bad_payer_cnpj_basico_length`: merchants join to real
#     companies by business key, and what breaks that silently is a numeric round trip
#     eating a leading zero. 142 of the 1,024 pinned roots have one.
#   - `bad_onboarded_on_shape`, reusing PTAX's ISO-date predicate. Not cosmetic: this is the
#     effectivity satellite's ENTRY column, and `opl.vault.effectivity` records that a NULL
#     entry date SORTS FIRST in Spark and beats a delivered one -- so an unparseable entry
#     date does not fail, it wins a window it should have lost.
#   - `bad_snapshot_at_shape`. The AXIS, and the least tautological rule in this file: the
#     ledger's before/after split is a string comparison on this column, so a wrong shape
#     sorts wrongly rather than raising. See the function for why the width is checked
#     beside the anchored pattern.
#   - `unparseable_credit_limit`. Near-tautological -- Postgres renders `numeric(14,2)`
#     under pinned GUCs -- and kept because this column is in the satellite's `hash_diff`,
#     so a value that casts to NULL makes two different payloads digest the same.
#   - `encoding_replacement_char`, folded over all fourteen columns. LIVE HERE FOR A REASON
#     THE OTHER TWO JSON SOURCES DO NOT HAVE: `legal_name` and `trade_name` are Portuguese
#     and accented, so a decode that went wrong has somewhere to show up.
#   - `unhashable_case_divergence`, folded over all fourteen columns, and it is a DIFFERENT
#     rule from the one above rather than a restatement of it. `encoding_replacement_char`
#     finds U+FFFD -- mojibake, the evidence that a byte was LOST. This finds the forty
#     characters JDK 17 (Unicode 13.0) and CPython 3.12 (Unicode 15.0) UPPER-CASE
#     DIFFERENTLY: valid, correctly decoded, and arriving exactly as sent. Plan T10 rules
#     that this constraint is a BRONZE DQ RULE and not a seeder assertion, in those words,
#     because a bound on `merchant_population.py` "protects the seed and nothing else -- not
#     the mutation script, not a manual `psql`, not a re-seed". The CNPJ contracts get the
#     same guard free at the boundary, their dialect being cp1252, in which none of the
#     forty is encodable; a UTF-8 Postgres source has no such property. What it prevents is
#     the failure with nothing to see: the row reaches the satellite's `hash_diff`, the
#     Python and Spark digests disagree on real data, and NO TEST GOES RED, because the
#     loaders only ever use the Spark spelling. SHADOWED on every column an earlier rule
#     inspects -- one of the forty in `cnpj` breaks `bad_cnpj_shape`'s digit test -- so the
#     columns it can be the REPORTED reason for are `legal_name` and `trade_name`, which are
#     the columns T10 says a UTF-8 source reaches them through. `opl.unicode_case` pins the
#     set as data and `tests/vault/test_hashing_spark.py` holds it as an EQUALITY against a
#     sweep of every cased character, so a JDK bump in either direction turns the suite red
#     rather than re-keying the vault quietly.
#   - `unprovable_snapshot_ref_date`, LAST, for the reason the CNPJ sets put it last: it is
#     the only rule here that describes the FILE rather than the row.
#
# WHAT IT DELIBERATELY DOES NOT CARRY:
#
#   - No value-domain rule on `status`, `mcc` or `risk_tier`: value domains GAIN members.
#   - NO RULE ABOUT `updated_at` BEYOND ITS PRESENCE, and this is a ruling rather than a
#     deferral. The temptation is a rule asserting it does not exceed `_snapshot_at` --
#     FALSE by construction, and the phase's entire subject: a transaction stamps
#     `updated_at` at its START and becomes visible at its COMMIT, so a row can legitimately
#     carry a stamp on either side of the instant that observed it. That rule would reject
#     exactly the rows the headline is measured over.
#   - NO ROW-COUNT OR COMPLETENESS RULE. "The snapshot holds every row the table had" is a
#     statement about a row that is ABSENT, and this gate TAGS ROWS. It is asserted where it
#     can be -- inside the extraction's own transaction, against `count(*)`.

# --- TWO SETS ARE BUILT BY A FUNCTION AND FIVE ARE INLINE, WHICH IS THE 50-LINE CAP ----
#
# `rules_for` stood at 49 lines of this project's 50-line function limit with six entries,
# so the SEVENTH could not be inline whatever it contained. The remedy is the one this
# module already applies to its prose (see the block above `rules_for`): move the volume
# out, keep the dispatch a dict and a return. The two sets extracted are the two newest,
# because extracting an older one would churn a literal that three test files pin by
# position; the next source extracts the next set.
#
# THE ORDER INSIDE EACH IS STILL THE CONTRACT and is still pinned per contract in
# `tests/bronze/test_ptax_rules.py` and `tests/bronze/test_merchant_rules.py`. Nothing here
# is a lookup by name -- each function returns one literal list -- so this is a seam, not a
# registry.


def _ptax_rules() -> list[tuple[str, Callable[[], Column]]]:
    """The PTAX set. See the comment block above `rules_for` for its ordering argument."""
    return [
        *_required_rules(PTAX_CONTRACT),
        (f"bad_{QUOTE_DATE_COLUMN}_shape", _bad_iso_date(QUOTE_DATE_COLUMN)),
        *(
            (f"unparseable_{column}", _unparseable_rate(column))
            for column in RATE_COLUMNS
        ),
        (f"unparseable_{PUBLISHED_AT_COLUMN}", _unparseable_publication_instant),
        ("encoding_replacement_char", _encoding_check(PTAX_CONTRACT)),
    ]


# `bad` + `_snapshot_at` + `_shape` reads as `bad_snapshot_at_shape`, which is the name a
# triager filters a quarantine on. Built from the column constant rather than typed, so a
# rename of the axis column reaches the reason string -- the underscore the column already
# carries is the separator.
def _merchant_rules() -> list[tuple[str, Callable[[], Column]]]:
    """The merchant set. See the comment block above `rules_for` for its ordering."""
    return [
        *_required_rules(MERCHANT_CONTRACT),
        (f"bad_{CNPJ_COLUMN}_shape", _bad_cnpj(CNPJ_COLUMN)),
        (f"bad_{ONBOARDED_ON_COLUMN}_shape", _bad_iso_date(ONBOARDED_ON_COLUMN)),
        (f"bad{SNAPSHOT_AT_COLUMN}_shape", _bad_snapshot_instant(SNAPSHOT_AT_COLUMN)),
        (
            f"unparseable_{CREDIT_LIMIT_COLUMN}",
            _unparseable_decimal(CREDIT_LIMIT_COLUMN, _CREDIT_LIMIT_TYPE),
        ),
        ("encoding_replacement_char", _encoding_check(MERCHANT_CONTRACT)),
        ("unhashable_case_divergence", _case_divergence_check(MERCHANT_CONTRACT)),
        (_UNPROVABLE_REF_DATE, _unprovable_ref_date),
    ]


def rules_for(table: str) -> list[tuple[str, Callable[[], Column]]]:
    """The ordered rule set for `table`, first-match-wins. KeyError if unknown.

    Order is part of the contract -- see the comment block above this function
    for what each set's ordering buys and why `unprovable_snapshot_ref_date` is
    last. Pinned per contract in tests/bronze/test_rules.py."""
    tables = {
        "lookup": [
            *_required_rules("lookup"),
            ("encoding_replacement_char", _encoding_check("lookup")),
        ],
        "estabelecimentos": [
            *_required_rules("estabelecimentos"),
            ("bad_cnpj_basico_length", _cnpj_basico_length),
            ("encoding_replacement_char", _encoding_check("estabelecimentos")),
            (_UNPROVABLE_REF_DATE, _unprovable_ref_date),
        ],
        "empresas": [
            *_required_rules("empresas"),
            ("bad_cnpj_basico_length", _cnpj_basico_length),
            ("encoding_replacement_char", _encoding_check("empresas")),
            (_UNPROVABLE_REF_DATE, _unprovable_ref_date),
        ],
        "socios": [
            *_required_rules("socios"),
            ("bad_cnpj_basico_length", _cnpj_basico_length),
            ("encoding_replacement_char", _encoding_check("socios")),
            (_UNPROVABLE_REF_DATE, _unprovable_ref_date),
        ],
        PAYMENTS_CONTRACT: [
            *_required_rules(PAYMENTS_CONTRACT),
            *(
                (f"bad_{column}_length", _basico_length(column))
                for column in COUNTERPARTY_COLUMNS
            ),
            ("encoding_replacement_char", _encoding_check(PAYMENTS_CONTRACT)),
        ],
        PTAX_CONTRACT: _ptax_rules(),
        MERCHANT_CONTRACT: _merchant_rules(),
    }
    return list(tables[table])
