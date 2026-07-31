import datetime as dt
import inspect
from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F

import opl.bronze.autoloader as al
from opl.bronze.autoloader import (
    RECORD_SOURCE,
    add_audit_columns,
    bronze_lookup_stream,
    bronze_stream,
    checkpoint_location,
    lookup_type_column,
    schema_location,
)
from opl.bronze.registry import table_spec
from opl.bronze.snapshot import SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN
from opl.config import DEFAULT

_SOURCE_FILE = "/Volumes/workspace/default/landing/cnpj/2026-06/lookups/F.K03200$Z.D60613.CNAECSV"


def test_audit_columns_added_with_constant_values(spark):
    df = spark.createDataFrame(
        [("01", "AÇÃO", _SOURCE_FILE)], ["codigo", "descricao", "_source_file"]
    )
    out = add_audit_columns(df, batch_id="run-123", snapshot_month="2026-06")
    assert {
        "_ingested_at",
        "_record_source",
        "_batch_id",
        SNAPSHOT_MONTH_COLUMN,
        SNAPSHOT_REF_DATE_COLUMN,
    } <= set(out.columns)
    row = out.collect()[0]
    assert row["_batch_id"] == "run-123"
    assert row["_record_source"] == RECORD_SOURCE
    assert row["_ingested_at"] is not None
    # The operational identity is the parameter; the business fact is the
    # date the RFB declares, which is NOT month-end. Both, side by side.
    assert row[SNAPSHOT_MONTH_COLUMN] == "2026-06"
    assert row[SNAPSHOT_REF_DATE_COLUMN] == dt.date(2026, 6, 13)


def test_the_snapshot_month_has_no_default():
    """A defaulted month is the F1.2 defect itself, so the absence of the default
    is the thing worth locking.

    Either candidate default is wrong: `opl.config`'s pinned month is exactly how
    every F1.2 row was silently tied to 2026-06, and the current month invents a
    fact about data the RFB published whenever it published it. Spark-free -- the
    TypeError comes from the signature, before the body runs."""
    with pytest.raises(TypeError, match="snapshot_month"):
        add_audit_columns(object(), batch_id="run-123")


def test_state_locations_are_separate_and_not_under_table_dir():
    sl, cl = schema_location(DEFAULT), checkpoint_location(DEFAULT)
    assert sl != cl
    assert sl.startswith(DEFAULT.volume_root)
    assert cl.startswith(DEFAULT.volume_root)
    assert "_schemas" in sl and "_checkpoints" in cl


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


def test_bronze_stream_no_longer_accepts_a_path_glob_filter():
    """Removed, not merely unused: a dead parameter invites its return, and the
    hazard it patched is now structural -- every stream reads its own subdir, so
    there is nothing for a glob to exclude.

    A glob is a DISCOVERY rule, which is why it was rejected for the estab stream
    in F1.3: a naming drift would silently under-ingest with nothing downstream
    able to see it."""
    assert "path_glob_filter" not in inspect.signature(bronze_stream).parameters


def test_the_lookup_stream_reads_its_own_subdirectory_not_the_month_root(monkeypatch):
    """The F1.4b blocker, fixed structurally. The month root does not isolate
    `*CSV`, so landing Empresas (`.EMPRECSV`) or Socios (`.SOCIOCSV`) there would
    have contaminated the lookup table -- and cloudFiles walks a source dir
    RECURSIVELY (empirically: an F1.3 probe.txt planted in
    `cnpj/<month>/zips/estabelecimentos/` was ingested by this stream, staging
    7408->7409), so the month root reaches every sibling too.

    Asserted on the stream's own source_dir, not on `landing_table` alone: what
    regressed here would be this call reverting to `landing_cnpj_month(...)`, and
    a config-level assertion cannot see that. Spark-free: `bronze_stream` and the
    lookup_type column builder are stubbed."""
    captured: dict[str, object] = {}

    class _FakeDF:
        def withColumn(self, *_a, **_k):
            return self

    def _fake_bronze_stream(spark, cfg, table, source_dir, table_key):
        captured.update(table=table, source_dir=source_dir, table_key=table_key)
        return _FakeDF()

    monkeypatch.setattr(al, "bronze_stream", _fake_bronze_stream)
    monkeypatch.setattr(al, "lookup_type_column", lambda _col: None)
    monkeypatch.setattr(al.F, "col", lambda _name: None)

    bronze_lookup_stream(spark=None, cfg=DEFAULT, month="2026-06")

    spec = table_spec("lookup")
    assert captured["table"] == spec.contract
    assert captured["table_key"] == spec.table_key
    # `spec.subdir`, never the literal: the directory name is the registry's to
    # own, which is why `subdir` is a field of its own rather than the table key.
    assert captured["source_dir"] == DEFAULT.landing_table(spec.subdir, "2026-06")
    assert captured["source_dir"] != DEFAULT.landing_cnpj_month("2026-06")


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


def test_lookup_type_column_maps_paths(spark):
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
