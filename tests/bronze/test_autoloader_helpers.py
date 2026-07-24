from pyspark.sql import functions as F

from opl.bronze.autoloader import (
    RECORD_SOURCE,
    add_audit_columns,
    checkpoint_location,
    lookup_type_column,
    schema_location,
)
from opl.config import DEFAULT
from opl.spark import local_session


def test_audit_columns_added_with_constant_values():
    spark = local_session("test-audit")
    try:
        df = spark.createDataFrame([("01", "AÇÃO")], ["codigo", "descricao"])
        out = add_audit_columns(df, batch_id="run-123")
        assert {"_ingested_at", "_record_source", "_batch_id"} <= set(out.columns)
        row = out.collect()[0]
        assert row["_batch_id"] == "run-123"
        assert row["_record_source"] == RECORD_SOURCE
        assert row["_ingested_at"] is not None
    finally:
        spark.stop()


def test_state_locations_are_separate_and_not_under_table_dir():
    sl, cl = schema_location(DEFAULT), checkpoint_location(DEFAULT)
    assert sl != cl
    assert sl.startswith(DEFAULT.volume_root)
    assert cl.startswith(DEFAULT.volume_root)
    assert "_schemas" in sl and "_checkpoints" in cl


def test_lookup_type_column_maps_paths():
    spark = local_session("test-lookup-col")
    try:
        df = spark.createDataFrame(
            [
                ("/Volumes/workspace/default/landing/cnpj/2026-06/F.K03200$Z.D60613.CNAECSV",),
                ("/Volumes/workspace/default/landing/cnpj/2026-06/F.K03200$Z.D60613.QUALSCSV",),
                ("/some/other/file.txt",),
                ("/Volumes/x/F.K03200$Z.D60613.CNAECSV.bak",),
            ],
            ["path"],
        )
        out = {
            r.path.rsplit("/", 1)[-1]: r.lt
            for r in df.withColumn("lt", lookup_type_column(F.col("path"))).collect()
        }
        assert out["F.K03200$Z.D60613.CNAECSV"] == "cnae"
        assert out["F.K03200$Z.D60613.QUALSCSV"] == "qualificacao"
        assert out["file.txt"] is None
        assert out["F.K03200$Z.D60613.CNAECSV.bak"] is None
    finally:
        spark.stop()
