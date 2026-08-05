"""Auto Loader (cloudFiles) bronze ingest for the CNPJ lookup files. The
streaming read runs only on Databricks serverless (Trigger.AvailableNow -- the
only supported trigger there); the audit-column and path helpers are pure and
unit-tested locally."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.bronze.lookup_routing import LOOKUP_SUFFIX
from opl.bronze.promote import BATCH_COLUMN
from opl.bronze.reader import csv_read_options
from opl.bronze.registry import table_spec
from opl.bronze.schema import struct_for
from opl.bronze.snapshot import (
    SNAPSHOT_MONTH_COLUMN,
    SNAPSHOT_REF_DATE_COLUMN,
    ref_date_column,
)
from opl.config import OplConfig, require_month

RECORD_SOURCE = "rfb_cnpj_webdav"
# The one spelling of the column that records WHICH LANDED FILE a row came out of.
# It lives here because this is where the column is created, and it is a constant
# rather than a literal because `opl.bronze.retention` reads it back to decide which
# files may be deleted from the Volume: two spellings of it would be a rename in one
# place that makes the retention query return nothing and silently reclaim nothing.
SOURCE_FILE_COLUMN = "_source_file"
# `_batch_id`'s one spelling is `promote.BATCH_COLUMN`, imported above rather than
# restated here: it is created in `add_audit_columns` like the column above, but its
# READERS -- `promote.rows_of_batch`, `promote.batch_rows`, `retention.files_of_batch`
# and the three job tasks -- are all on the promote side, and that is where the
# constant already lived when this module still wrote the name as a bare literal.

# NO table-name constants here. BRONZE_STAGING / BRONZE_ESTAB_STAGING /
# BRONZE_QUARANTINE / BRONZE_ESTAB_QUARANTINE lived here until F1.4 Task 8 and
# were a SECOND spelling of names `opl.bronze.registry` already owns -- one import
# away from re-creating the drift the registry exists to prevent, which is the
# drift that once sent an estab triager to a table holding no trace of the batch
# that had been blocked. Every staging/bronze/quarantine name now comes from
# `table_spec(...)`. `RECORD_SOURCE` stays: it names where the BYTES came from
# (the RFB WebDAV), which is a property of this ingest, not of a table.


# BOTH STATE LOCATIONS ARE MONTH-SCOPED, and until F1.4b PR B neither was. They
# were keyed on `table_key` alone while `load()`'s source path is
# `landing_table(subdir, month)`, so the first run for a second month would have
# restarted a live query against a DIFFERENT SOURCE DIRECTORY. Spark's "Recovery
# Semantics after Changes in a Streaming Query" lists a change to the subscribed
# files as "generally not allowed as the results are unpredictable" and defines
# that as "likely to fail with unpredictable errors"; Databricks states the
# positive rule for Auto Loader directly -- more than one source location loaded
# into one target table "requires a separate streaming checkpoint". Every ingest
# through 2026-06 ran under the unscoped layout, so this was never exercised: the
# empresas and socios streams were first runs.
#
# WHAT THE RISK IS NOT: an under-ingest reported as SUCCESS. Auto Loader's
# exactly-once tracking is keyed on the full file path, and a second month's paths
# are all new, so a query that DOES start ingests them correctly. The failure mode
# is at startup. `Trigger.AvailableNow` changes none of this.
# `spark.databricks.cloudFiles.checkSourceChanged` is an active guard whose trip is
# a loud [STREAM_FAILED] StreamingQueryException, but the documented case is a
# cross-BUCKET mismatch under file-notification mode; whether it fires for a
# same-Volume, different-subdirectory change under the directory-listing default
# this job uses is NOT stated by the docs and has not been measured here. Which is
# why the layout is fixed rather than the guard relied on.
#
# `<month>/<table_key>` AND NOT `<table_key>/<month>`. The month goes above the
# table so each month's state is a SIBLING of the pre-Step-0
# `_checkpoints/<table_key>` directory; nesting it underneath would put a new
# RocksDB store and offset log INSIDE a checkpoint directory 2026-06's query still
# owns. It also mirrors the landing layout (`cnpj/<month>/<table>`), the same
# reason `landing_tmp` mirrors it: an operator listing this Volume maps each state
# dir 1:1 onto the landing dir it drained.
#
# NEITHER COMPONENT HAS A DEFAULT, and both used to be defaultable in their own way.
#
# `month` is REQUIRED for `add_audit_columns`'s reason, with a sharper edge:
# `opl.config`'s pinned month is how F1.2 silently tied every row to 2026-06, and
# supplied here it resolves 2026-06's checkpoint while the read is of the 2026-07
# landing dir -- reinstating the exact hazard above, invisibly, with the one value
# that must be refused. It is KEYWORD-ONLY because `table_key` and `month` are
# adjacent and both `str`: a positional swap type-checks and produces
# `_checkpoints/<table_key>/<month>`, which is the nesting forbidden above. That is
# the argument `BronzeTable` is declared `kw_only=True` for.
#
# `table_key` LOST ITS `= "bronze_cnpj_lookup"` DEFAULT, and the reason is the same
# collision `registry_collisions._assert_no_two_tables_share_a_checkpoint_namespace`
# refuses --
# reached from the one direction that guard cannot see. It compares the `table_key`s
# tables DECLARE; it cannot see a CALL SITE that omits the argument.
# `checkpoint_location(cfg, month=m)` type-checked and silently returned the LOOKUP's
# namespace, so an estab stream wired that way would start up believing the lookup's
# files were its own and already ingested, write nothing, and report SUCCESS -- the
# quietest of the four collisions, verbatim from that guard's own raise message.
# Making `month` keyword-only made that call SHORTER to write than the correct one,
# which is precisely the shape this same change removed from `bronze_lookup_stream`
# (its `month=None` was a `DEFAULT.month` substitution one call further down). All
# three production call sites already passed `spec.table_key`, so nothing at runtime
# changes; what changes is that the dangerous call no longer exists.


# BOTH STATE PATHS ASK `require_month` FIRST, and it is the same argument the guard
# below now carries: a state path must never be BUILDABLE from a value `require_month`
# would have rejected. Keyword-only and no-default stops the month being FORGOTTEN;
# neither stops it being SUPPLIED EMPTY. `checkpoint_location(cfg, key, month="")`
# type-checks, satisfies both, and yields `.../_checkpoints//<table_key>` -- which on a
# Volumes path collapses onto `_checkpoints/<table_key>`, the pre-Step-0 directory this
# layout deliberately ORPHANS. The ingest would then advance 2026-06's abandoned state
# for a 2026-07 read: the precise pairing month-scoping exists to make impossible.
#
# `require_month` and not a local `is_month` test, because the rule then has ONE
# spelling for the entry points and for the paths they build. A second copy here is how
# `2026-13` came to be refused at two of four entry points (see `opl.config._MONTH`).
# Free of Spark, like everything else in this module's path half.


def schema_location(cfg: OplConfig, table_key: str, *, month: str) -> str:
    return (
        f"{cfg.volume_root}/_schemas/"
        f"{require_month(month, action='locate the inferred-schema store')}/{table_key}"
    )


def checkpoint_location(cfg: OplConfig, table_key: str, *, month: str) -> str:
    """Where this table's ingest of THIS MONTH records which files it has read.

    THE 2026-06 STATE AT THE OLD PATHS IS ORPHANED, deliberately and permanently.
    Streaming state is never migrated: `_checkpoints/<table_key>` and
    `_schemas/<table_key>` are left exactly where they are, and nothing reads them.

    WHICH MAKES RE-RUNNING AN ALREADY-INGESTED MONTH A DUPLICATE-APPEND, and that
    is the one consequence an operator has to know. A 2026-06 ingest launched
    through the job flow now finds an EMPTY month-scoped checkpoint, treats every
    2026-06 file as new, and appends that month's rows into staging a second time
    under a fresh `_batch_id` -- and the promote's idempotence is keyed on
    `_batch_id`, so it cannot see the duplication and will carry it into bronze.
    Nothing fails; the row counts double. Recorded for operators in
    `docs/f1.4b-pr-b-run-evidence.md`, and it is why re-running a month that has
    already been promoted is a manual, deliberate act rather than a retry.

    REFUSES A MONTH THAT IS NOT ONE before it builds anything -- see the comment above
    this pair for why an empty string is the case that mattered."""
    return (
        f"{cfg.volume_root}/_checkpoints/"
        f"{require_month(month, action='locate the checkpoint')}/{table_key}"
    )


def _assert_source_dir_is_this_months(cfg: OplConfig, source_dir: str, month: str) -> None:
    """Refuse a `source_dir` that is not one table's landing subdir for `month`.

    THE MONTH ARRIVES TWICE -- inside `source_dir`, and as `month` -- and the two
    are the same fact: which files are read, and which checkpoint records them as
    read. A disagreement drains one month's directory under another month's
    checkpoint, which is the hazard the state layout above exists to remove,
    reached from the source side instead. `bronze_ingest.py` threads ONE local into
    both for that reason; this refuses the caller that does not.

    REBUILT THROUGH `cfg.landing_table` rather than compared by prefix, so the
    layout is asked rather than re-spelled here -- and equality refuses two things a
    prefix test would admit: the month ROOT (the F1.4b blocker, since it holds every
    other table's files and cloudFiles walks a source dir recursively) and any `..`
    that climbs back out of the month.

    IT DEPENDS ON `registry._assert_subdirs_are_single_path_components`, and says so
    rather than being safe by luck: taking the last component and rebuilding is total
    only because a registered `subdir` is ONE directory name. Were that check
    relaxed to allow `a/b`, this comparison would refuse every legitimate call from
    such a table -- loudly, at the top of the ingest, which is the right direction to
    fail in, but it would be THIS function's assumption that broke, not that one's.

    IT WAS BLIND TO THE ONE VALUE IT WAS WRITTEN TO REFUSE, and that is what the first
    line fixes. The equality rebuilds through `cfg.landing_table(subdir, month)`, and
    `opl.config.landing_cnpj_month` is `f"{...}/{month or self.month}"` -- so `month=""`
    or `None` resolves to the config's PINNED month INSIDE the rebuild. For a
    `source_dir` of the pinned month the comparison then PASSED, while the same empty
    string handed to `checkpoint_location` gave `.../_checkpoints//<table_key>`. That is
    the substituted-pinned-month pair this guard exists for, arriving in the one form
    the rebuild could not see it in. Not reachable from either entry point today -- both
    bind `require_month`, and `test_task_wiring.py` locks that -- so this is
    defence-in-depth; but the docstring above says this function refuses the pair rather
    than trusting it, and until now that was not true of every spelling of it."""
    require_month(month, action="read")
    subdir = source_dir.rsplit("/", 1)[-1]
    if source_dir != cfg.landing_table(subdir, month):
        raise ValueError(
            f"refusing to read: source_dir={source_dir!r} is not a landing subdir of "
            f"month={month!r} -- expected {cfg.landing_cnpj_month(month)}/<subdir>. The "
            "month is half of BOTH the files read and the checkpoint that records them "
            "as read, so a disagreement here restarts a stream against a source it did "
            "not checkpoint, which Spark's recovery semantics call not allowed and "
            "likely to fail unpredictably. Pass the same month local that built "
            "source_dir."
        )


def add_audit_columns(
    df: DataFrame,
    batch_id: str,
    snapshot_month: str,
    record_source: str = RECORD_SOURCE,
) -> DataFrame:
    """Stamp the ingestion audit columns onto a bronze stream.

    `snapshot_month` is REQUIRED and has no default. A default would be one of
    two things, both bad: `opl.config`'s pinned month, which is how F1.2's ingest
    entry point silently tied every row to 2026-06, or the current month, which
    invents a fact. The F1.2 evidence doc recorded the seam this closes --
    "ingesting a second month requires parameterizing the month and adding a
    snapshot key".

    Expects `_source_file` on `df`; every bronze stream adds it (see
    `bronze_stream`), and the reference date is derived from it."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_source", F.lit(record_source))
        # BATCH_COLUMN, not the literal it was until the F1.4a review. Every reader
        # of this column FILTERS on that constant, so the two spellings fail in
        # opposite directions: renaming the constant raises in six places, while
        # renaming this literal leaves `rows_of_batch` counting 0 rows of its own
        # batch -- a promote that reports success having appended nothing -- and
        # `files_of_batch` proving nothing, so a reclaim silently deletes no bytes.
        # That is the same argument SOURCE_FILE_COLUMN carries two lines below
        # ("two spellings would silently reclaim nothing"), in the same function and
        # for the same reason; this column was the one left out of it.
        .withColumn(BATCH_COLUMN, F.lit(batch_id))
        .withColumn(SNAPSHOT_MONTH_COLUMN, F.lit(snapshot_month))
        .withColumn(
            SNAPSHOT_REF_DATE_COLUMN,
            ref_date_column(F.col(SOURCE_FILE_COLUMN), snapshot_month),
        )
    )


def lookup_type_column(file_path_col: Column) -> Column:
    # Build a nested CASE from the suffix map, matching the inner-file suffix in the path.
    col = F.lit(None)
    for suffix, lookup_type in LOOKUP_SUFFIX.items():
        col = F.when(file_path_col.endswith(f".{suffix}CSV"), F.lit(lookup_type)).otherwise(col)
    return col


def bronze_stream(
    spark: SparkSession,
    cfg: OplConfig,
    table: str,
    source_dir: str,
    table_key: str,
    *,
    month: str,
) -> DataFrame:
    """Generalized cloudFiles bronze read for any contract table. Reads
    ``source_dir`` with the ``struct_for(table)`` schema and the shared CSV
    options, adds ``_source_file``. Lookup-specific columns are added by the
    caller (see ``bronze_lookup_stream``).

    ``month`` is REQUIRED and keyword-only, and it must be the month
    ``source_dir`` is under -- see ``_assert_source_dir_is_this_months``, which
    refuses the pair rather than trusting it. It scopes the inferred-schema half of
    this stream's Auto Loader state; the checkpoint half is the caller's
    ``writeStream``, so the two are set from the same local in one place.

    NO GLOB, and no parameter for one. cloudFiles walks ``source_dir``
    RECURSIVELY -- an F1.3 probe.txt planted in the ``zips/estabelecimentos/``
    subdir was ingested by the lookup stream reading the month root (staging
    7408 -> 7409) -- so a stream pointed at a shared root needed a
    ``pathGlobFilter`` to stay out of its neighbours' files. Every stream now
    reads its OWN per-table subdir, which removes the shared root and with it the
    need. That is deliberately structural: a glob is a discovery RULE, so a source
    filename drifting out of its pattern would silently under-ingest, with nothing
    downstream able to tell an empty batch from a missed one."""
    _assert_source_dir_is_this_months(cfg, source_dir, month)
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location(cfg, table_key, month=month))
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(struct_for(table))
    )
    for k, v in csv_read_options().items():
        reader = reader.option(k, v)
    df = reader.load(source_dir)
    return df.withColumn(SOURCE_FILE_COLUMN, F.col("_metadata.file_path"))


def bronze_lookup_stream(spark: SparkSession, cfg: OplConfig, month: str) -> DataFrame:
    # `month` HAS NO DEFAULT, and `= None` was one: `landing_cnpj_month` falls back
    # to `cfg.month` for None, so the stream read the pinned 2026-06 with nobody
    # having passed it -- `DEFAULT.month` substituted one call further down, which is
    # the shape `require_month` exists to refuse. It now also selects the Auto Loader
    # state, so None would resolve 2026-06's checkpoint too.
    spec = table_spec("lookup")
    df = bronze_stream(
        spark,
        cfg,
        spec.contract,
        # Its OWN subdir, not the month root the six lookups used to sit loose in.
        # `spec.subdir` and not the literal: the directory name is the registry's
        # to own -- that is why `subdir` is a field of its own and not derived
        # from the table key.
        cfg.landing_table(spec.subdir, month),
        spec.table_key,
        # The SAME argument that built the source dir above, not a second lookup of
        # it: it decides which checkpoint records those files as read.
        month=month,
    )
    return df.withColumn(
        "lookup_type", lookup_type_column(F.col("_metadata.file_path"))
    )
