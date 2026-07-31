# tests/bronze/test_rules.py
import pytest
from pyspark.sql.types import StringType, StructField, StructType

from opl.bronze.dq import REJECT_COLUMN, evaluate, split
from opl.bronze.rules import rules_for


def test_unknown_table_raises():
    with pytest.raises(KeyError):
        rules_for("nope")


def test_lookup_rules_names_and_order():
    names = [name for name, _ in rules_for("lookup")]
    assert names == ["null_or_empty_codigo", "null_or_empty_descricao",
                     "encoding_replacement_char"]


def test_estabelecimentos_rules_evaluate(spark):
    cols = ["cnpj_basico", "cnpj_ordem", "cnpj_dv", "nome_fantasia", "logradouro"]
    df = spark.createDataFrame(
        [
            ("12345678", "0001", "95", "PADARIA AÇAÍ", "RUA A"),   # valid
            (None,        "0001", "95", "X", "Y"),                   # null cnpj_basico
            ("1234567",   "0001", "95", "X", "Y"),                   # 7 chars, bad length
            ("12345678",  None,   "95", "X", "Y"),                   # null ordem
            ("12345678",  "0001", None, "X", "Y"),                   # null dv
            ("87654321",  "0001", "95", "MOJI�BAKE", "Y"),           # replacement char
        ],
        cols,
    )
    out = {(r.cnpj_basico, r.cnpj_ordem, r.cnpj_dv): r[REJECT_COLUMN]
           for r in evaluate(df, rules=rules_for("estabelecimentos")).collect()}
    assert out[("12345678", "0001", "95")] is None
    assert out[(None, "0001", "95")] == "null_or_empty_cnpj_basico"
    assert out[("1234567", "0001", "95")] == "bad_cnpj_basico_length"
    assert out[("12345678", None, "95")] == "null_or_empty_cnpj_ordem"
    assert out[("12345678", "0001", None)] == "null_or_empty_cnpj_dv"
    # The replacement char is the ONLY in-band evidence that a byte was lost:
    # Java's cp1252 decoder substitutes U+FFFD silently where Python raises
    # (ADR 0006). Without this assertion the rule could be deleted from the
    # estabelecimentos set with the suite still green.
    assert out[("87654321", "0001", "95")] == "encoding_replacement_char"


def test_rescued_data_outranks_the_estabelecimentos_rules(spark):
    """The universal rescued_data_present check sits above every per-table rule
    (dq._reject_reason), so a rescued row must report that and not the per-table
    reason it also violates -- rescued data means the parse itself is suspect, so
    a narrower reason would misdirect the triage."""
    # Explicit schema: _rescued_data would be all-null in the clean row and
    # type inference cannot determine an all-null column's type.
    schema = StructType([StructField(c, StringType()) for c in (
        "cnpj_basico", "cnpj_ordem", "cnpj_dv", "nome_fantasia", "logradouro",
        "_rescued_data",
    )])
    df = spark.createDataFrame(
        [
            # Violates bad_cnpj_basico_length AND carries rescued data.
            ("1234567", "0001", "95", "X", "Y", '{"_c30":"31 extra"}'),
            ("12345678", "0001", "95", "X", "Y", None),
        ],
        schema,
    )
    out = {r.cnpj_basico: r[REJECT_COLUMN]
           for r in evaluate(df, rules=rules_for("estabelecimentos")).collect()}
    assert out["1234567"] == "rescued_data_present"
    assert out["12345678"] is None


def test_lookup_default_golden_unchanged(spark):
    """evaluate() with NO rules arg must behave exactly as F1.2 shipped."""
    # explicit schema: _rescued_data is all-null, so type inference cannot
    # determine it (PySparkValueError CANNOT_DETERMINE_TYPE) without this.
    schema = StructType([
        StructField("codigo", StringType()),
        StructField("descricao", StringType()),
        StructField("_rescued_data", StringType()),
    ])
    df = spark.createDataFrame(
        [("01", "AÇÃO", None), (None, "x", None), ("02", None, None)],
        schema,
    )
    out = {r.codigo: r[REJECT_COLUMN] for r in evaluate(df).collect()}
    assert out["01"] is None
    assert out[None] == "null_or_empty_codigo"
    assert out["02"] == "null_or_empty_descricao"
    good, bad = split(df)
    assert good.count() == 1 and bad.count() == 2
