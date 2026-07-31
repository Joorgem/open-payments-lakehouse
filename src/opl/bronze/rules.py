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

from opl.contracts.cnpj_schemas import TABLES

_REPLACEMENT_CHAR = "�"

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
# it. `municipio` is index 20 of 30 and is MEASURED clean: over all 71,874,448
# live rows, blanks in municipio, situacao_cadastral, uf and data_inicio_atividade
# are 0. Declaring it required therefore rejects nothing that exists today, which
# is why it can be added to a live table's rule set at all.
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
    WHICH COLUMN HOLDS IT IS NOT KNOWN -- so a check over 2 of 30 columns was a
    coin flip on the one record it was written for.

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


def rules_for(table: str) -> list[tuple[str, Callable[[], Column]]]:
    """The ordered rule set for `table`, first-match-wins. KeyError if unknown.

    ORDER IS THE CONTRACT, not a detail, because only the FIRST matching rule's
    reason is recorded. Required-field rules run first as a group, then the
    shape check, then the encoding check: a row missing a key field is described
    by what is MISSING rather than by the shape of what is left, and a row whose
    bytes are damaged is only reported as such once nothing simpler explains it.
    Pinned per contract in tests/bronze/test_rules.py.

    One consequence of grouping, called out because it changed a live table's
    output: estabelecimentos used to run `bad_cnpj_basico_length` BETWEEN
    `null_or_empty_cnpj_basico` and `null_or_empty_cnpj_ordem`. A row that is both
    short in cnpj_basico and blank in a later required column now reports the
    blank column. Both reasons are true of that row and it is rejected either
    way -- a reporting change, not a gate change."""
    tables = {
        "lookup": [
            *_required_rules("lookup"),
            ("encoding_replacement_char", _encoding_check("lookup")),
        ],
        "estabelecimentos": [
            *_required_rules("estabelecimentos"),
            ("bad_cnpj_basico_length", _cnpj_basico_length),
            ("encoding_replacement_char", _encoding_check("estabelecimentos")),
        ],
        "empresas": [
            *_required_rules("empresas"),
            ("bad_cnpj_basico_length", _cnpj_basico_length),
            ("encoding_replacement_char", _encoding_check("empresas")),
        ],
        "socios": [
            *_required_rules("socios"),
            ("bad_cnpj_basico_length", _cnpj_basico_length),
            ("encoding_replacement_char", _encoding_check("socios")),
        ],
    }
    return list(tables[table])
