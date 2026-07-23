# tests/bronze/test_dq.py
from opl.bronze.dq import REJECT_COLUMN, evaluate, split
from opl.spark import local_session


def _df(spark):
    # rows: (codigo, descricao, _rescued_data) — mimics the stream shape
    return spark.createDataFrame(
        [
            ("01", "AÇÃO", None),            # valid
            (None, "no code", None),         # invalid: null codigo
            ("  ", "blank code", None),      # invalid: empty codigo
            ("02", "mojib�de", None),   # invalid: replacement char (encoding fail)
            ("03", "extra cols", "{\"_c2\":\"x\"}"),  # invalid: rescued data present
        ],
        ["codigo", "descricao", "_rescued_data"],
    )


def test_evaluate_tags_reasons():
    spark = local_session("test-dq-eval")
    try:
        out = {r.codigo: r[REJECT_COLUMN] for r in evaluate(_df(spark)).collect()}
        assert out["01"] is None
        assert out[None] == "null_or_empty_codigo"
        assert out["  "] == "null_or_empty_codigo"
        assert out["02"] == "encoding_replacement_char"
        assert out["03"] == "rescued_data_present"
    finally:
        spark.stop()


def test_split_partitions_good_and_bad():
    spark = local_session("test-dq-split")
    try:
        good, bad = split(_df(spark))
        assert good.count() == 1
        assert bad.count() == 4
        assert REJECT_COLUMN not in good.columns   # dropped from good
        assert REJECT_COLUMN in bad.columns
    finally:
        spark.stop()


def test_evaluate_tolerates_missing_rescued_column():
    spark = local_session("test-dq-norescue")
    try:
        df = spark.createDataFrame([("01", "ok")], ["codigo", "descricao"])
        # no _rescued_data column present (local batch shape) -> must not crash
        out = evaluate(df).collect()
        assert out[0][REJECT_COLUMN] is None
    finally:
        spark.stop()
