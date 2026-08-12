# src/opl/bronze/rules.py
"""Per-table bronze DQ rule sets: ordered (reason, predicate) pairs,
first-match-wins. The universal _rescued_data check lives in dq.evaluate,
above any per-table rule.

Predicates are zero-arg factories (Callable[[], Column]) rather than eager
Column objects: PySpark cannot build a Column without an active SparkContext,
and rules_for is inspected (names, unknown-table KeyError) in pure-Python
tests that hold no session. The factory defers Column construction to
evaluate() time, where a DataFrame — hence a live session — always exists."""
from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column
from pyspark.sql import functions as F

from opl.bronze.snapshot import SNAPSHOT_REF_DATE_COLUMN
from opl.contracts.catalogue import CONTRACT_COLUMNS
from opl.contracts.payments import CONTRACT as PAYMENTS_CONTRACT
from opl.contracts.payments import COUNTERPARTY_COLUMNS, REQUIRED_COLUMNS

_REPLACEMENT_CHAR = "�"

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
}


def _null_or_blank(col: str) -> Callable[[], Column]:
    return lambda: F.col(col).isNull() | (F.trim(F.col(col)) == "")


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


def _encoding_check(contract: str) -> Callable[[], Column]:
    """U+FFFD in ANY column of the contract, not two hand-picked ones.

    Bronze is all-string, so every column of every contract is a string column
    and the check is total by construction. Derived from TABLES rather than
    listed, so a contract gaining a column gains the check with it -- a list
    would go stale exactly where a new column is most likely to be mojibake.

    Carry-forward #5, and the reason it is not cosmetic: one record in
    `Estabelecimentos8` carries a byte (0x8f) that windows-1252 cannot decode at
    all. Python raises on it; Java's decoder substitutes U+FFFD SILENTLY, which
    makes that character the only in-band evidence a byte was lost (ADR 0006).
    WHICH COLUMN HOLDS IT IS `correio_eletronico`, and that answer arrived only
    once the check was total: the 2026-07 estabelecimentos ingest rejected four
    rows for `encoding_replacement_char`, all four in that column (observed
    2026-08-03). `nome_fantasia` and `logradouro` -- the hand-picked pair the check
    covered before -- are neither of them it. So the old rule was not merely a coin
    flip on the record it was written for; it would have missed it, and did: the
    same four records sit un-flagged in 2026-06's bronze, promoted by a run whose
    gate measured zero. See ADR 0006 and `docs/f1.4b-pr-b-run-evidence.md` §20.3.

    The chain starts at `F.lit(False)` rather than at the first column's
    `contains`, so the fold is total over a contract of any length instead of
    raising IndexError on an empty one. The three-valued semantics are identical
    either way: `False | NULL` is NULL, `NULL | True` is True, so a row is flagged
    if ANY column holds the character and is left alone when none does, whatever
    mix of NULLs it carries.

    `tuple(...)` snapshots the contract's column list -- `CONTRACT_COLUMNS` hands
    out tuples, so this is belt-and-braces today; it stays because the property is
    about this closure outliving the call, not about the catalogue's current type.

    IT IS A LIVE CONTROL ON THE PAYMENT STREAM, NOT INHERITED BOILERPLATE, which
    needs saying because the obvious reading is that a generated UTF-8 JSON stream
    cannot contain mojibake. It can, by exactly the route F1b's central risk runs
    along: `opl.generator.events.to_jsonl` returns TEXT, and a writer that did not
    encode UTF-8 explicitly -- or a reader that did not decode as UTF-8 -- hands Java
    bytes it cannot map, and Java's decoder substitutes U+FFFD SILENTLY where Python
    raises. That character is then the only in-band evidence the bytes on disk are
    not the bytes the golden digest was taken over."""
    columns = tuple(CONTRACT_COLUMNS[contract])

    def predicate() -> Column:
        chain = F.lit(False)
        for column in columns:
            chain = chain | F.col(column).contains(_REPLACEMENT_CHAR)
        return chain

    return predicate


# 8 characters, which is `cnpj_basico`'s width in every contract that carries a
# company root -- the three RFB ones under that name, and the payment stream's two
# counterparty columns under their own. Named once here because the rules below build
# on it under two different column names; it is the same fact about the same key
# space, and `opl.generator.cnpj_pool.CNPJ_BASICO_WIDTH` refuses a pool entry of any
# other width at the boundary where payments' keys are drawn from `hub_empresa`.
_CNPJ_BASICO_WIDTH = 8


def _basico_length(column: str) -> Callable[[], Column]:
    """`column` is not exactly 8 characters after trimming.

    A LENGTH check and not a numeric one: alphanumeric CNPJs take effect 2026-07-31,
    and an `int()` round trip loses a leading zero the moment one arrives
    (`cnpj_schemas`). That is not an abstract worry for the payment stream -- it is
    the precise failure F1b Task 4's 100%-resolution measurement exists to catch, and
    this rule catches the same thing one step earlier, in the gate, BEFORE the rows
    reach bronze and before anyone joins them to `hub_empresa`.

    Parameterised by column since F1b Task 3, where the same shape has to be asserted
    about `payer_cnpj_basico` and `payee_cnpj_basico`. The reason string is built from
    the column name at the call site, so `bad_cnpj_basico_length` -- which already
    exists as DATA in two live quarantine tables -- comes out byte-identical for the
    three RFB contracts."""
    return lambda: F.length(F.trim(F.col(column))) != _CNPJ_BASICO_WIDTH


def _cnpj_basico_length() -> Column:
    """The RFB contracts' `cnpj_basico` width rule, under its historical name.

    Kept as a zero-argument function rather than replaced by `_basico_length(
    "cnpj_basico")` at the three call sites: `test_every_predicate_is_a_zero_arg_
    factory_and_not_a_column` inspects the signature of every predicate, and this is
    the module's one plain-function example of the three shapes it deliberately
    carries (a lambda, a closure, a module function)."""
    return _basico_length("cnpj_basico")()


def _unprovable_ref_date() -> Column:
    """The reference date the RFB declares in its own filename, absent.

    `snapshot.ref_date_column` yields NULL whenever it cannot PROVE a date --
    no `.D<y><mm><dd>.` token in the filename, two of them, or a token whose
    month/year digit disagrees with the job's month parameter. That refusal was
    only half a control: nothing read the NULLs, so a month shipping a different
    filename shape produced an all-NULL column and a green run. This is the half
    that speaks, and it is the debt `snapshot.py`'s docstring booked to F1.4b.

    SAFE ON A LIVE TABLE BECAUSE IT IS MEASURED, the same precondition
    `municipio` had to meet: over the 71,874,448 rows of
    workspace.default.bronze_cnpj_estabelecimentos live when this was written, the
    NULL count for this column is 0, verified by a SQL query independent of the
    backfill script's own log (docs/f1.4a-migration-evidence.md) -- and the 2026-07
    ingest re-confirmed it on a further 72,318,968 staged rows whose only rejects
    were 4 `encoding_replacement_char`. So this rejects nothing that exists today.
    The gate is all-or-nothing -- any reject fails the run -- so that number is a
    precondition and not a footnote, and it is re-earned each month rather than
    inherited: it is a claim about a (month, rule set) pair.

    A row-level rule for a FILE-level fact, deliberately. The gate has no other
    vocabulary -- it tags rows -- and the shape that follows from that is the
    right one anyway: when a filename format changes, every row of that file
    carries the reason, the gate is all-or-nothing, and the run goes red with the
    reason naming the actual cause. A batch mixing one unparseable file with
    several good ones quarantines only the rows from the bad file, which is the
    behaviour a per-file count could not express.

    WHAT THIS DOES NOT CATCH, so it is not mistaken for more than it is: a token
    that parses but is WRONG (the RFB restating June's date on a July file) is a
    date this rule accepts. `_snapshot_month` sits beside it carrying the job's
    month, so the disagreement is visible in the row -- see snapshot.py on why
    both columns exist. Catching that would be a cross-column check, not this."""
    return F.col(SNAPSHOT_REF_DATE_COLUMN).isNull()


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
    }
    return list(tables[table])
