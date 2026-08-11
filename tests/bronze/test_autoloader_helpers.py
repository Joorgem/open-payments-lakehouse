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
from opl.bronze.promote import BATCH_COLUMN
from opl.bronze.registry import REGISTRY, table_spec
from opl.bronze.snapshot import SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN
from opl.config import DEFAULT, is_month

_SOURCE_FILE = "/Volumes/workspace/default/landing/cnpj/2026-06/lookups/F.K03200$Z.D60613.CNAECSV"


def test_audit_columns_added_with_constant_values(spark):
    df = spark.createDataFrame(
        [("01", "AÇÃO", _SOURCE_FILE)], ["codigo", "descricao", "_source_file"]
    )
    out = add_audit_columns(df, batch_id="run-123", snapshot_month="2026-06")
    # BATCH_COLUMN, not the literal: this ingest WRITES the column that
    # `promote.rows_of_batch` and `retention.files_of_batch` filter on, and it wrote
    # it as a bare "_batch_id" until the F1.4a review. Asserted through the constant
    # so a rename fails here instead of turning the promote into a silent no-op
    # (0 rows of its own batch) and the reclaim into a delete of nothing.
    assert {
        "_ingested_at",
        "_record_source",
        BATCH_COLUMN,
        SNAPSHOT_MONTH_COLUMN,
        SNAPSHOT_REF_DATE_COLUMN,
    } <= set(out.columns)
    row = out.collect()[0]
    assert row[BATCH_COLUMN] == "run-123"
    assert row["_record_source"] == RECORD_SOURCE
    assert row["_ingested_at"] is not None
    # The operational identity is the parameter; the business fact is the
    # date the RFB declares, which is NOT month-end. Both, side by side.
    assert row[SNAPSHOT_MONTH_COLUMN] == "2026-06"
    assert row[SNAPSHOT_REF_DATE_COLUMN] == dt.date(2026, 6, 13)


def test_the_batch_column_written_here_follows_the_constant_the_readers_filter_on(
        spark, monkeypatch):
    """That the WRITE goes through the constant, not merely that the name matches.

    The test above cannot tell a constant from a literal that happens to spell the
    same thing, and that is exactly what this function held until the F1.4a review:
    `"_batch_id"` inline, two lines above `SOURCE_FILE_COLUMN` used as a constant
    BECAUSE "two spellings would silently reclaim nothing". Rebinding the constant is
    what separates them -- with a literal the produced frame keeps `_batch_id` and
    never grows the rebound name.

    Why it matters which one it is: `promote.rows_of_batch` and
    `retention.files_of_batch` FILTER on this constant. Rename it and six imports
    raise; rename the literal and the promote counts 0 rows of its own batch and
    reports success having appended nothing."""
    monkeypatch.setattr(al, "BATCH_COLUMN", "_batch_id_probe")
    df = spark.createDataFrame(
        [("01", "AÇÃO", _SOURCE_FILE)], ["codigo", "descricao", "_source_file"]
    )
    out = add_audit_columns(df, batch_id="run-123", snapshot_month="2026-06")
    assert "_batch_id_probe" in out.columns
    assert BATCH_COLUMN not in out.columns


def test_the_snapshot_month_has_no_default():
    """A defaulted month is the F1.2 defect itself, so the absence of the default
    is the thing worth locking.

    Either candidate default is wrong: `opl.config`'s pinned month is exactly how
    every F1.2 row was silently tied to 2026-06, and the current month invents a
    fact about data the RFB published whenever it published it. Spark-free -- the
    TypeError comes from the signature, before the body runs."""
    with pytest.raises(TypeError, match="snapshot_month"):
        add_audit_columns(object(), batch_id="run-123")


_LOOKUP_KEY = table_spec("lookup").table_key


def test_state_locations_are_separate_and_not_under_table_dir():
    sl = schema_location(DEFAULT, _LOOKUP_KEY, month="2026-06")
    cl = checkpoint_location(DEFAULT, _LOOKUP_KEY, month="2026-06")
    assert sl != cl
    assert sl.startswith(DEFAULT.volume_root)
    assert cl.startswith(DEFAULT.volume_root)
    assert "_schemas" in sl and "_checkpoints" in cl


def test_state_locations_are_month_scoped_between_the_kind_and_the_table():
    """`_checkpoints/<month>/<table_key>`, mirroring the landing layout
    (`cnpj/<month>/<table>`) so an operator listing this Volume maps each state dir
    1:1 onto the landing dir it drained.

    THE COMPONENT ORDER IS THE ASSERTION, not the presence of the month.
    `<month>/<table_key>` makes every month's state a SIBLING of the pre-Step-0
    `_checkpoints/<table_key>` directory; `<table_key>/<month>` would have put new
    Auto Loader state INSIDE a checkpoint directory a 2026-06 query still owns.

    The table key is SPELLED at the call site, not defaulted into the expectation.
    These two assertions used to read `schema_location(DEFAULT, month=...)` against
    an expected string ending `bronze_cnpj_lookup` -- a golden whose subject the
    test never named."""
    assert schema_location(DEFAULT, "bronze_cnpj_lookup", month="2026-07") == (
        "/Volumes/workspace/default/landing/_schemas/2026-07/bronze_cnpj_lookup"
    )
    assert checkpoint_location(DEFAULT, "bronze_cnpj_lookup", month="2026-07") == (
        "/Volumes/workspace/default/landing/_checkpoints/2026-07/bronze_cnpj_lookup"
    )


def test_neither_state_location_defaults_the_month():
    """Replaces `test_state_locations_default_are_f12_golden`, which read "F1.2
    shipped exactly these paths; the refactor must not move them" -- and Step 0
    moved them on purpose, so that lock had to go rather than be edited.

    REQUIRED, for `add_audit_columns.snapshot_month`'s reason and a sharper one.
    `opl.config`'s pinned month is how F1.2 silently tied every row to 2026-06;
    supplied HERE it would resolve the 2026-06 checkpoint while `load()` reads the
    2026-07 landing dir -- restarting a live query against a different source
    directory, which is the exact hazard month-scoping exists to remove, restored
    invisibly by the one value the parameter must refuse. `bronze_ingest.py:76`
    refuses a missing month rather than defaulting it for the same reason.

    KEYWORD-ONLY because `table_key` and `month` are adjacent and both `str`: a
    positional swap type-checks and yields `_checkpoints/<table_key>/<month>` --
    state nested inside the one directory this layout must stay out of. That is
    the argument `BronzeTable` is declared `kw_only=True` for.

    Spark-free: the TypeError comes from the signature, before the body runs."""
    with pytest.raises(TypeError, match="month"):
        schema_location(DEFAULT, _LOOKUP_KEY)
    with pytest.raises(TypeError, match="month"):
        checkpoint_location(DEFAULT, _LOOKUP_KEY)


def test_neither_state_location_defaults_the_table_key():
    """`table_key` had a `= "bronze_cnpj_lookup"` default until this round, and it
    was the SAME collision `registry._assert_no_two_tables_share_a_checkpoint_
    namespace` refuses, reached from the one direction that guard cannot see: it
    compares the keys tables DECLARE and cannot see a call site that omits the
    argument. `checkpoint_location(cfg, month=m)` type-checked and returned the
    lookup's namespace, so an estab stream wired that way would start up believing
    the lookup's files were its own and already ingested, write nothing, and report
    SUCCESS -- that guard's own raise message, verbatim, with the guard green.

    Making `month` keyword-only had made that call SHORTER than the correct one,
    which is the shape this same change removed from `bronze_lookup_stream`.

    Spark-free, and it is `table_key` that is named in the error rather than the
    month: Python reports the missing positional first, which is what makes this a
    different assertion from the one above rather than a duplicate of it."""
    with pytest.raises(TypeError, match="table_key"):
        schema_location(DEFAULT, month="2026-07")
    with pytest.raises(TypeError, match="table_key"):
        checkpoint_location(DEFAULT, month="2026-07")


def test_no_table_shares_or_nests_state_across_months_or_with_the_orphans():
    """Distinctness AND non-nesting, over every registered table and both kinds.

    Distinctness alone is not the property. `_checkpoints/2026-06/x` and
    `_checkpoints/2026-06/x/2026-07` are distinct too, and the second is Auto
    Loader state written inside a directory whose RocksDB store and offset log
    another query owns -- so both prefix directions are refused as well. STRING
    prefix, not path prefix: it is the stronger claim, and refuting it refutes both.

    The pre-Step-0 `_checkpoints/<table_key>` paths are in the comparison because
    they still EXIST in the Volume, holding 2026-06's state, and are deliberately
    not migrated. Orphaned state is safe; state nested underneath it is not.

    Derived from REGISTRY rather than listing today's four tables, so a table added
    later cannot slip past this.

    WHY THE MONTHS BELOW ARE A SAMPLE AND THE FIRST ASSERTION IS NOT. Nesting needs
    a `<month>` component that equals a `<table_key>` component. The month side is
    total already -- `opl.config._MONTH` admits only `YYYY-MM` -- so what the
    property actually rests on is that no `table_key` is month-shaped, and THAT is
    asserted over the whole registry rather than sampled. Without it the three
    literal months below would be checking a naming convention.

    IT NOW HAS A GUARD BEHIND IT, which it did not when it was written:
    `registry_collisions._assert_no_table_key_is_month_shaped` refuses a month-shaped
    `table_key` at IMPORT. It was declined at the time because `registry.py` stood at
    798 lines of an 800 cap and this repo requires the *why* beside a guard; F2 Task 0
    made the room by extracting the collision guards, and the guard landed beside the
    checkpoint-namespace one rather than inside it, for the reason the subdir trio is
    three functions.

    THIS ASSERTION STAYS ANYWAY, and not as a duplicate. What the guard refuses is a
    registry; what this states is the PREMISE the sampled comparison below rests on --
    the months are a sample, the shape claim is not, and a test whose sample is only
    valid under a stated premise has to state it where it is used."""
    for spec in REGISTRY.values():
        assert not is_month(spec.table_key), (
            f"{spec.name}.table_key={spec.table_key!r} is month-shaped, so "
            f"_checkpoints/{spec.table_key}/... is reachable as BOTH a month dir and a "
            "table dir and the sampled comparison below no longer covers the property"
        )
    for spec in REGISTRY.values():
        for locate, kind in ((schema_location, "_schemas"), (checkpoint_location, "_checkpoints")):
            orphan = f"{DEFAULT.volume_root}/{kind}/{spec.table_key}"
            paths = [orphan] + [
                locate(DEFAULT, spec.table_key, month=m)
                for m in ("2026-06", "2026-07", "2027-01")
            ]
            for i, one in enumerate(paths):
                for other in paths[i + 1:]:
                    assert one != other, f"{spec.name}/{kind}: {one} is reused"
                    assert not one.startswith(f"{other}/"), f"{spec.name}/{kind}: {one} nests"
                    assert not other.startswith(f"{one}/"), f"{spec.name}/{kind}: {other} nests"


def test_state_locations_estab_are_siblings():
    sl = schema_location(DEFAULT, "bronze_cnpj_estab", month="2026-06")
    cl = checkpoint_location(DEFAULT, "bronze_cnpj_estab", month="2026-06")
    assert sl.endswith("/_schemas/2026-06/bronze_cnpj_estab")
    assert cl.endswith("/_checkpoints/2026-06/bronze_cnpj_estab")
    assert sl != schema_location(DEFAULT, _LOOKUP_KEY, month="2026-06")
    assert cl != checkpoint_location(DEFAULT, _LOOKUP_KEY, month="2026-06")


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

    def _fake_bronze_stream(spark, cfg, table, source_dir, table_key, *, month):
        captured.update(
            table=table, source_dir=source_dir, table_key=table_key, month=month
        )
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
    # The month reaches the STREAM as well as the source dir, from the one argument
    # this function was given: it is what resolves the checkpoint that records these
    # files as read, and a second lookup of it could name another month's state.
    assert captured["month"] == "2026-06"


def test_the_lookup_streams_month_has_no_default():
    """It had one -- `month: str | None = None` -- and that default was a
    `DEFAULT.month` substitution wearing a different coat: `landing_cnpj_month`
    falls back to the pinned month for `None`, so the stream read 2026-06 without
    anyone passing it. Harmless-looking while the checkpoint was month-blind;
    now it would also resolve 2026-06's checkpoint, so it is refused."""
    with pytest.raises(TypeError, match="month"):
        bronze_lookup_stream(spark=None, cfg=DEFAULT)


def _recording_spark(opts: dict[str, object]) -> SimpleNamespace:
    """A `readStream` double that records every option and the loaded path.

    Shared by the two tests below because they assert about the same call from
    opposite ends -- which options the reader receives, and which state location
    one of those options resolves to."""

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

    return SimpleNamespace(readStream=_FakeReadStream())


def test_bronze_stream_forwards_multiline_to_the_cloudfiles_read(monkeypatch):
    # F1.3 run-1 incident guard: the streaming path must carry multiLine=true,
    # otherwise a quoted field containing a literal newline (valid CSV per RFC
    # 4180; 1 such record in Estabelecimentos6, 3 in Estabelecimentos8) is split
    # into a NULL-tailed parent row that passes DQ plus a garbage fragment.
    # tests/bronze/test_reader_multiline.py proves the parse on real bytes; this
    # asserts the Auto Loader reader actually receives the option. Spark-free:
    # readStream is a recording double.
    opts: dict[str, object] = {}
    monkeypatch.setattr(al.F, "col", lambda _name: None)
    spark = _recording_spark(opts)

    for table in ("estabelecimentos", "lookup"):
        opts.clear()
        al.bronze_stream(
            spark,
            DEFAULT,
            table,
            DEFAULT.landing_table(table, "2026-06"),
            f"bronze_{table}",
            month="2026-06",
        )
        assert opts["__format"] == "cloudFiles"
        assert opts["multiLine"] == "true", f"{table}: {opts}"


def test_bronze_stream_points_the_schema_location_at_the_month_it_loads(monkeypatch):
    """The whole point of threading the month this far in.

    `cloudFiles.schemaLocation` is the inferred-schema half of the same state the
    checkpoint holds -- Databricks' own guidance is that more than one source
    location loaded into one target table "requires a separate streaming
    checkpoint" -- so it has to move with the source directory. Asserted against
    `schema_location(...)` rather than a literal, so this cannot pass on a path
    the rest of the flow does not build."""
    opts: dict[str, object] = {}
    monkeypatch.setattr(al.F, "col", lambda _name: None)
    for month in ("2026-06", "2026-07"):
        opts.clear()
        source_dir = DEFAULT.landing_table("empresas", month)
        al.bronze_stream(
            _recording_spark(opts),
            DEFAULT,
            "empresas",
            source_dir,
            "bronze_cnpj_empresas",
            month=month,
        )
        assert opts["__load_path"] == source_dir
        assert opts["cloudFiles.schemaLocation"] == schema_location(
            DEFAULT, "bronze_cnpj_empresas", month=month
        )
        assert month in str(opts["cloudFiles.schemaLocation"])


@pytest.mark.parametrize(
    "source_dir",
    [
        # Another month's landing dir: the drift this refuses, arriving from the
        # source side rather than the checkpoint side.
        DEFAULT.landing_table("empresas", "2026-06"),
        # The month ROOT -- the F1.4b blocker. It holds every other table's files
        # and cloudFiles walks a source dir recursively.
        DEFAULT.landing_cnpj_month("2026-07"),
        # A path that escapes the month dir a prefix test would have admitted.
        f"{DEFAULT.landing_cnpj_month('2026-07')}/../2026-06/empresas",
        "/some/dir",
    ],
)
def test_bronze_stream_refuses_a_source_dir_that_is_not_the_given_months(source_dir):
    """The month arrives here TWICE -- inside `source_dir` and as `month` -- and
    the two are the same fact: which files are read, and which checkpoint records
    them as read. A disagreement drains one month's directory under another
    month's checkpoint, which is precisely what Step 0 exists to prevent, reached
    from the other direction. So it is refused rather than merely avoided by the
    entry points' one-local discipline.

    No Spark: the refusal precedes the reader."""
    with pytest.raises(ValueError, match="not a landing subdir"):
        al.bronze_stream(
            None, DEFAULT, "empresas", source_dir, "bronze_cnpj_empresas", month="2026-07"
        )


@pytest.mark.parametrize("month", ["", None, "   ", "2026-13", "2026-06/zips"])
def test_no_state_path_and_no_source_check_accepts_a_month_require_month_would_refuse(
    month,
):
    """The hole keyword-only-and-no-default cannot close: SUPPLIED, but not a month.

    THE EMPTY STRING IS THE ONE THAT MATTERED, and it defeated the source/month guard
    by being invisible to it. That guard rebuilds through `cfg.landing_table(subdir,
    month)`, and `landing_cnpj_month` is `f"{...}/{month or self.month}"` -- so `""` and
    `None` resolve to the config's PINNED month inside the rebuild, and the equality
    PASSES for a `source_dir` of the pinned month. The same `""` handed to
    `checkpoint_location` gave `.../_checkpoints//<table_key>`, which on a Volumes path
    collapses onto `_checkpoints/<table_key>` -- the pre-Step-0 directory this layout
    deliberately orphans and never migrates. A run wired that way advanced 2026-06's
    abandoned state for a read of another month, which is exactly the pair the guard's
    docstring says it refuses rather than trusts.

    `2026-13` and `2026-06/zips` are here because the state paths are the OTHER place
    the month is interpolated raw, and a rule with two homes is how `2026-13` came to be
    refused at two of four entry points. One predicate (`require_month`) now answers for
    the entry points and for every path they build.

    Defence-in-depth and said so: `bronze_ingest.py` and `bronze_lookup_ingest.py` both
    bind `require_month`, and `tests/test_task_wiring.py` locks that they keep doing so.
    This is what stops a FIFTH caller from being the one that finds out.

    No Spark anywhere: every refusal is a string check in the signature's own frame."""
    for locate in (schema_location, checkpoint_location):
        with pytest.raises(ValueError, match="month"):
            locate(DEFAULT, "bronze_cnpj_empresas", month=month)
    with pytest.raises(ValueError, match="month"):
        al._assert_source_dir_is_this_months(
            DEFAULT, DEFAULT.landing_table("empresas", DEFAULT.month), month
        )


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
