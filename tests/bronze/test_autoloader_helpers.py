from types import SimpleNamespace

from pyspark.sql import functions as F

import opl.bronze.autoloader as al
from opl.bronze.autoloader import (
    BRONZE_ESTAB_STAGING,
    RECORD_SOURCE,
    add_audit_columns,
    bronze_lookup_stream,
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


def test_estab_staging_constant():
    assert BRONZE_ESTAB_STAGING == "bronze_cnpj_estab_staging"


def test_state_locations_default_are_f12_golden():
    # F1.2 shipped exactly these paths; the refactor must not move them.
    assert schema_location(DEFAULT) == \
        "/Volumes/workspace/default/landing/_schemas/bronze_cnpj_lookup"
    assert checkpoint_location(DEFAULT) == \
        "/Volumes/workspace/default/landing/_checkpoints/bronze_cnpj_lookup"


def test_state_locations_estab_are_siblings():
    sl = schema_location(DEFAULT, "bronze_cnpj_estab")
    cl = checkpoint_location(DEFAULT, "bronze_cnpj_estab")
    assert sl.endswith("/_schemas/bronze_cnpj_estab")
    assert cl.endswith("/_checkpoints/bronze_cnpj_estab")
    assert sl != schema_location(DEFAULT) and cl != checkpoint_location(DEFAULT)


def test_lookup_stream_requests_csv_path_glob(monkeypatch):
    # F1.3 Task 6 subdir-isolation regression guard: the lookup stream reads the
    # cnpj/<month> root, which Auto Loader walks recursively. It MUST forward
    # pathGlobFilter="*CSV" to bronze_stream so non-CSV files planted in sibling
    # subdirs (empirically a probe.txt in zips/estabelecimentos/ was ingested,
    # staging 7408->7409, before this) are excluded. Spark-free: bronze_stream
    # and the lookup_type column builder are stubbed.
    captured: dict[str, object] = {}

    class _FakeDF:
        def withColumn(self, *_a, **_k):
            return self

    def _fake_bronze_stream(spark, cfg, table, source_dir, table_key, path_glob_filter=None):
        captured.update(table=table, path_glob_filter=path_glob_filter)
        return _FakeDF()

    monkeypatch.setattr(al, "bronze_stream", _fake_bronze_stream)
    monkeypatch.setattr(al, "lookup_type_column", lambda _col: None)
    monkeypatch.setattr(al.F, "col", lambda _name: None)

    bronze_lookup_stream(spark=None, cfg=DEFAULT)
    assert captured["table"] == "lookup"
    assert captured["path_glob_filter"] == "*CSV"


def test_bronze_stream_forwards_multiline_to_the_cloudfiles_read(monkeypatch):
    # F1.3 run-1 incident guard: the streaming path must carry multiLine=true,
    # otherwise a quoted field containing a literal newline (valid CSV per RFC
    # 4180; 1 such record in Estabelecimentos6, 3 in Estabelecimentos8) is split
    # into a NULL-tailed parent row that passes DQ plus a garbage fragment.
    # tests/bronze/test_reader_multiline.py proves the parse on real bytes; this
    # asserts the Auto Loader reader actually receives the option. Spark-free:
    # readStream is a recording double.
    opts: dict[str, object] = {}

    class _FakeDF:
        def withColumn(self, *_a, **_k):
            return self

    class _RecordingReader:
        def option(self, key, value):
            opts[key] = value
            return self

        def schema(self, _struct):
            return self

        def load(self, path):
            opts["__load_path"] = path
            return _FakeDF()

    class _FakeReadStream:
        def format(self, fmt):
            opts["__format"] = fmt
            return _RecordingReader()

    monkeypatch.setattr(al.F, "col", lambda _name: None)
    spark = SimpleNamespace(readStream=_FakeReadStream())

    for table in ("estabelecimentos", "lookup"):
        opts.clear()
        al.bronze_stream(spark, DEFAULT, table, "/some/dir", f"bronze_{table}")
        assert opts["__format"] == "cloudFiles"
        assert opts["multiLine"] == "true", f"{table}: {opts}"


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
