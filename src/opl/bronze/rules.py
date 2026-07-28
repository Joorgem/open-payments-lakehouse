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

_REPLACEMENT_CHAR = "�"


def _null_or_blank(col: str) -> Callable[[], Column]:
    return lambda: F.col(col).isNull() | (F.trim(F.col(col)) == "")


def rules_for(table: str) -> list[tuple[str, Callable[[], Column]]]:
    tables = {
        "lookup": [
            ("null_or_empty_codigo", _null_or_blank("codigo")),
            ("null_or_empty_descricao", _null_or_blank("descricao")),
            ("encoding_replacement_char",
             lambda: F.col("descricao").contains(_REPLACEMENT_CHAR)),
        ],
        "estabelecimentos": [
            ("null_or_empty_cnpj_basico", _null_or_blank("cnpj_basico")),
            ("bad_cnpj_basico_length",
             lambda: F.length(F.trim(F.col("cnpj_basico"))) != 8),
            ("null_or_empty_cnpj_ordem", _null_or_blank("cnpj_ordem")),
            ("null_or_empty_cnpj_dv", _null_or_blank("cnpj_dv")),
            ("encoding_replacement_char",
             lambda: F.col("nome_fantasia").contains(_REPLACEMENT_CHAR)
             | F.col("logradouro").contains(_REPLACEMENT_CHAR)),
        ],
    }
    return list(tables[table])
