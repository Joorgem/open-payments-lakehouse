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
from opl.contracts.cnpj_schemas import TABLES

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
REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "lookup": ("codigo", "descricao"),
    "estabelecimentos": ("cnpj_basico", "cnpj_ordem", "cnpj_dv", "municipio"),
    "empresas": ("cnpj_basico", "razao_social", "natureza_juridica"),
    "socios": ("cnpj_basico", "identificador_socio", "nome_socio_razao_social",
               "qualificacao_socio"),
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

    `tuple(...)` snapshots the contract's column list. `TABLES` hands out its
    mutable list, and this closure outlives the call -- a caller that mutated
    that list would silently change which columns a rule set already handed to a
    running job is checking."""
    columns = tuple(TABLES[contract])

    def predicate() -> Column:
        chain = F.lit(False)
        for column in columns:
            chain = chain | F.col(column).contains(_REPLACEMENT_CHAR)
        return chain

    return predicate


def _cnpj_basico_length() -> Column:
    """8 characters after trimming. Alphanumeric since 2026-07-31, hence a LENGTH
    check and not a numeric one (cnpj_schemas)."""
    return F.length(F.trim(F.col("cnpj_basico"))) != 8


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
    }
    return list(tables[table])
