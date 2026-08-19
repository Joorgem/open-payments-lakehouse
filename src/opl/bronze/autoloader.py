"""Auto Loader (cloudFiles) bronze ingest for the CNPJ lookup files. The
streaming read runs only on Databricks serverless (Trigger.AvailableNow -- the
only supported trigger there); the audit-column and path helpers are pure and
unit-tested locally."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.bronze.dq import RESCUED_DATA_COLUMN
from opl.bronze.lookup_routing import LOOKUP_SUFFIX
from opl.bronze.promote import BATCH_COLUMN
from opl.bronze.reader import read_options, source_format
from opl.bronze.registry import table_spec
from opl.bronze.schema import struct_for
from opl.bronze.snapshot import (
    SNAPSHOT_MONTH_COLUMN,
    SNAPSHOT_REF_DATE_COLUMN,
    ref_date_column,
    ref_date_from_instant,
)
from opl.config import OplConfig, require_month

RECORD_SOURCE = "rfb_cnpj_webdav"
# WHERE A GENERATED SOURCE'S BYTES CAME FROM, which for payments is this repository
# itself. Named as a value beside the RFB one rather than derived from the table,
# because `_record_source` answers "who produced this row" and the two answers are
# genuinely different kinds of thing: one is a WebDAV share on receita.fazenda.gov.br,
# the other is `opl.generator` run against a seed. A row that cannot say which is a row
# whose provenance has to be inferred from its table name.
GENERATED_RECORD_SOURCE = "opl_payment_generator"
# WHERE AN API-FED SOURCE'S BYTES CAME FROM, which for PTAX is the Banco Central's
# Olinda service. Named beside the other two for their reason, and it is the value that
# made `LANDING_GENERATED` unusable for this source: stamping BCB's published rates with
# `GENERATED_RECORD_SOURCE` would say this repository produced them, in the one column
# that answers who did. It names the INSTITUTION AND THE SERVICE rather than a URL --
# `RECORD_SOURCE` is `rfb_cnpj_webdav` on the same principle -- because an endpoint can
# be re-hosted without the provenance changing, and a column full of URLs invites a
# second spelling of one that `opl.extraction.ptax_source` already owns.
API_RECORD_SOURCE = "bcb_olinda_ptax"
# WHERE A DATABASE-FED SOURCE'S BYTES CAME FROM, which for merchant is an operational
# Postgres this project runs. Named beside the other three for their reason, and the
# reason it is not `API_RECORD_SOURCE` is the same shape that made `GENERATED_RECORD_SOURCE`
# unusable for PTAX, pointing the other way: `bcb_olinda_ptax` would attribute this
# project's own database to the Banco Central. It names the ENGINE AND THE REGISTRY rather
# than a DSN -- `rfb_cnpj_webdav` is the same principle -- because a host and port can move
# without the provenance changing, and because a DSN is the one string in this phase that
# must never be written into a table.
POSTGRES_RECORD_SOURCE = "opl_merchant_postgres"
# The one spelling of the column that records WHICH LANDED FILE a row came out of.
# It lives here because this is where the column is created, and it is a constant
# rather than a literal because `opl.bronze.reconcile` reads it back to build the
# file-grain query `opl.bronze.retention` decides deletes from: two spellings of it
# would be a rename in one place that makes that query return nothing and silently
# reclaim nothing.
SOURCE_FILE_COLUMN = "_source_file"
# `_batch_id`'s one spelling is `promote.BATCH_COLUMN`, imported above rather than
# restated here: it is created in `add_audit_columns` like the column above, but its
# READERS -- `promote.rows_of_batch`, `promote.batch_rows`, `retention.months_of_batch`
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


# --- WHY THE SOURCE-DIR GUARD BELOW ACCEPTS EITHER LANDING ROOT ----------------------
#
# Module level for the reason `opl.bronze.rules` gives above `rules_for`: this is the
# reasoning, not the guard, and inside the docstring it put the function past the
# project's 50-line limit.
#
# A file-fed table lands under `cnpj/<month>/<subdir>`, a generated one under
# `generated/<month>/<subdir>`, an api-fed one under `api/<month>/<subdir>` and a
# database-fed one under `postgres/<month>/<subdir>`
# (`opl.config`, `opl.bronze.registry_landing`). All four are rebuilt in the guard and
# the source dir must equal ONE of them.
#
# STILL AN EQUALITY, NEVER A PREFIX TEST. "Starts with one of the roots" would
# re-admit exactly the two shapes the equality exists to refuse: the month ROOT itself
# -- the F1.4b blocker, since it holds every other table's files and cloudFiles walks a
# source dir recursively -- and any `..` that climbs back out of the month.
#
# ANY ROOT ACCEPTED RATHER THAN THE RIGHT ONE SELECTED, and that is a decision. The
# function is handed a PATH, not a spec, so telling it which root to expect would mean
# threading the landing mode through `bronze_stream` for a comparison that is already
# exact: registered subdirs are unique across the whole registry
# (`registry._assert_no_two_tables_share_a_landing_subdir`), so no legitimate path can
# satisfy another root's rebuild, and no illegitimate one can satisfy any of them.
#
# THE TUPLE GREW WITH THE FOURTH LANDING MODE AND HAD TO, and again with the fifth. A root
# missing from it is not a laxity: it is a REFUSAL of every legitimate read of that root, at
# the top of the ingest, which is the right direction to fail in and is how this edit
# announces itself.
#
# IT WAS BLIND TO THE ONE VALUE IT WAS WRITTEN TO REFUSE, and that is what the guard's first
# line fixes. The equality rebuilds through `cfg.landing_table(subdir, month)`, and
# `opl.config.landing_cnpj_month` is `f"{...}/{month or self.month}"` -- so `month=""` or
# `None` resolves to the config's PINNED month INSIDE the rebuild. For a `source_dir` of the
# pinned month the comparison then PASSED, while the same empty string handed to
# `checkpoint_location` gave `.../_checkpoints//<table_key>`. That is the
# substituted-pinned-month pair this guard exists for, arriving in the one form the rebuild
# could not see it in. Not reachable from either entry point today -- both bind
# `require_month`, and `test_month_wiring.py` locks that -- so it is defence-in-depth; but
# the docstring says this function refuses the pair rather than trusting it, and until that
# line was added that was not true of every spelling of it.
#
# (This paragraph moved out of the docstring when the fifth root took the function to 51 of
# this project's 50-line limit -- the same remedy the two blocks above it record.)
def _assert_source_dir_is_this_months(cfg: OplConfig, source_dir: str, month: str) -> None:
    """Refuse a `source_dir` that is not one table's landing subdir for `month`.

    THE MONTH ARRIVES TWICE -- inside `source_dir`, and as `month` -- and the two
    are the same fact: which files are read, and which checkpoint records them as
    read. A disagreement drains one month's directory under another month's
    checkpoint, which is the hazard the state layout above exists to remove,
    reached from the source side instead. `bronze_ingest.py` threads ONE local into
    both for that reason; this refuses the caller that does not.

    REBUILT THROUGH `opl.config`'s own landing helpers rather than compared by prefix,
    so the layout is asked rather than re-spelled here. See the comment block above for
    what the equality refuses that a prefix test would admit, and why BOTH roots are
    accepted rather than the right one being selected.

    IT DEPENDS ON `registry._assert_subdirs_are_single_path_components`, and says so
    rather than being safe by luck: taking the last component and rebuilding is total
    only because a registered `subdir` is ONE directory name. Were that check
    relaxed to allow `a/b`, this comparison would refuse every legitimate call from
    such a table -- loudly, at the top of the ingest, which is the right direction to
    fail in, but it would be THIS function's assumption that broke, not that one's.

    See the comment block above for the one value this was BLIND to until the empty-month
    guard on its first line was added, and why that guard is defence-in-depth today."""
    require_month(month, action="read")
    subdir = source_dir.rsplit("/", 1)[-1]
    expected = (
        cfg.landing_table(subdir, month),
        cfg.landing_generated_table(subdir, month),
        cfg.landing_api_table(subdir, month),
        cfg.landing_postgres_table(subdir, month),
    )
    if source_dir not in expected:
        raise ValueError(
            f"refusing to read: source_dir={source_dir!r} is not a landing subdir of "
            f"month={month!r} -- expected one of {expected}. The "
            "month is half of BOTH the files read and the checkpoint that records them "
            "as read, so a disagreement here restarts a stream against a source it did "
            "not checkpoint, which Spark's recovery semantics call not allowed and "
            "likely to fail unpredictably. Pass the same month local that built "
            "source_dir."
        )


def add_common_audit_columns(
    df: DataFrame,
    *,
    batch_id: str,
    snapshot_month: str,
    record_source: str,
) -> DataFrame:
    """The four audit columns EVERY bronze row carries, whatever its source.

    SPLIT OUT OF `add_audit_columns` BY F1b TASK 3, and the seam is which column is a
    statement about the INGEST and which is a statement about the SOURCE.
    `_ingested_at`, `_record_source`, `_batch_id` and `_snapshot_month` are properties
    of the run: when it happened, who produced the bytes, which batch this is, which
    month the operator asked for. `_snapshot_ref_date` is not -- it is "the date the
    RFB declares in its own filename" (`opl.bronze.snapshot`), and a generated stream
    declares no such thing.

    STAMPING IT ANYWAY WOULD HAVE BEEN THE QUIET FAILURE, which is why this split
    exists rather than payments simply reusing the wider function.
    `snapshot.ref_date_column` yields NULL for any filename with no `.D<y><mm><dd>.`
    token, so payments bronze would carry an all-NULL column -- and
    `rules._unprovable_ref_date` exists precisely to REJECT that NULL, so the payments
    rule set would have had to omit its own control to let the table load. A rule
    deliberately left out so that a column it refuses can be written is the shape this
    repository calls a control that disappeared rather than failed. The column is
    absent instead, so there is nothing to refuse and nothing to excuse.

    `snapshot_month` is REQUIRED and has no default. A default would be one of two
    things, both bad: `opl.config`'s pinned month, which is how F1.2's ingest entry
    point silently tied every row to 2026-06, or the current month, which invents a
    fact. `record_source` is required for the same reason at a smaller scale -- a
    default would make the RFB the answer for a source that is not it.

    KEYWORD-ONLY: `batch_id`, `snapshot_month` and `record_source` are three adjacent
    `str`s, and a positional call that swapped any two type-checks and stamps every row
    of a batch with the wrong provenance."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_source", F.lit(record_source))
        # BATCH_COLUMN, not the literal it was until the F1.4a review. Every reader
        # of this column FILTERS on that constant, so the two spellings fail in
        # opposite directions: renaming the constant raises in six places, while
        # renaming this literal leaves `rows_of_batch` counting 0 rows of its own
        # batch -- a promote that reports success having appended nothing -- and
        # the file-grain proof matching nothing, so a reclaim deletes no bytes.
        .withColumn(BATCH_COLUMN, F.lit(batch_id))
        .withColumn(SNAPSHOT_MONTH_COLUMN, F.lit(snapshot_month))
    )


def add_audit_columns(
    df: DataFrame,
    batch_id: str,
    snapshot_month: str,
    record_source: str = RECORD_SOURCE,
) -> DataFrame:
    """The common four, plus the reference date a FILE-FED source declares in its
    own filename. The stamp every CNPJ ingest applies.

    Its signature is unchanged -- positional, with the RFB record source defaulted --
    because two live entry points call it that way and `tests/test_month_wiring.py`
    pins `add_audit_columns(..., snapshot_month=<the one month local>)` as one of four
    consumers that must read the same local. What changed is that the four columns it
    shares with every other source now live in `add_common_audit_columns`, so a
    generated source gets them without also getting a derivation that would be NULL
    for every one of its rows. See that function for why that mattered.

    Expects `_source_file` on `df`; every bronze stream adds it (see `bronze_stream`),
    and the reference date is derived from it."""
    return add_common_audit_columns(
        df,
        batch_id=batch_id,
        snapshot_month=snapshot_month,
        record_source=record_source,
    ).withColumn(
        SNAPSHOT_REF_DATE_COLUMN,
        ref_date_column(F.col(SOURCE_FILE_COLUMN), snapshot_month),
    )


def add_instant_audit_columns(
    df: DataFrame,
    *,
    batch_id: str,
    snapshot_month: str,
    record_source: str,
    instant_column: str,
) -> DataFrame:
    """The common four, plus the reference date derived from an instant the SOURCE carried.

    THE THIRD AUDIT-COLUMN PATH (plan T8), and the seam is the same one that split the
    first two: WHERE the reference date comes from. `add_audit_columns` reads the RFB's
    mainframe filename token; `add_common_audit_columns` omits the column because a
    generated or api-fed source declares no such thing. Neither works for a source whose
    snapshot instant travels IN THE ROW -- and this one cannot simply omit the column
    either, because `bronze_merchant` is the first non-file-fed source ever loaded into a
    VAULT SATELLITE and `opl.vault.satellites` reads `_snapshot_ref_date` unconditionally
    to build `applied_date`.

    `instant_column` IS A COORDINATE AND MUST COME FROM THE RESOLVED SPEC -- in practice
    `spec.snapshot_axis.column`, never a literal at the call site. It is the column the
    observation ledger groups by, so a stamp derived from a DIFFERENT column than the one
    the ledger reads would put `applied_date` and the axis one observation apart, and the
    satellite's `groupBy(hash_key, applied_date)` fold would collapse rows the ledger had
    kept distinct.

    KEYWORD-ONLY, and the argument is `add_common_audit_columns`': `batch_id`,
    `snapshot_month`, `record_source` and `instant_column` are four adjacent `str`s, and a
    positional call that swapped any two type-checks and stamps a whole batch wrongly."""
    return add_common_audit_columns(
        df,
        batch_id=batch_id,
        snapshot_month=snapshot_month,
        record_source=record_source,
    ).withColumn(SNAPSHOT_REF_DATE_COLUMN, ref_date_from_instant(F.col(instant_column)))


def lookup_type_column(file_path_col: Column) -> Column:
    # Build a nested CASE from the suffix map, matching the inner-file suffix in the path.
    col = F.lit(None)
    for suffix, lookup_type in LOOKUP_SUFFIX.items():
        col = F.when(file_path_col.endswith(f".{suffix}CSV"), F.lit(lookup_type)).otherwise(col)
    return col


# --- WHAT `bronze_stream` TAKES FROM THE CONTRACT, AND WHY THAT IS THE SEAM ----------
#
# TWO SOURCE FORMATS SINCE F1b TASK 3, dispatched on the CONTRACT and nowhere else
# (`opl.bronze.reader.source_format` / `read_options`). The RFB files are semicolon
# CSV; the generated payment stream is JSON Lines, because the drift class this
# lakehouse must exhibit is a new optional column appearing mid-stream -- which in CSV
# is a column-count change a positional reader either refuses outright or silently
# misaligns (`opl.contracts.payments` carries the argument).
#
# A `format=` PARAMETER WAS THE ALTERNATIVE AND IS REFUSED. It would be a coordinate
# arriving from somewhere other than the spec the caller resolved, which is the shape
# every task test in this repository exists to forbid -- and an entry point handed the
# wrong one reads JSON as CSV: one string column of NULLs per row, no error anywhere.
#
# THE SCHEMA IS SUPPLIED, WHICH IS WHAT MAKES `_rescued_data` THE DRIFT VERDICT. Auto
# Loader's schema evolution defaults to `addNewColumns` only when no schema is
# provided; given one it does not evolve, so a JSON key the contract does not declare
# cannot be absorbed into the read schema. With `rescuedDataColumn` set it lands in
# `_rescued_data` instead, and `dq._reject_reason` ranks `rescued_data_present` above
# every per-table rule -- so the payment stream's mid-stream drift column is
# quarantined rather than silently accepted. That is the whole reason
# `opl.contracts.payments` refuses to DECLARE that column, and if the drift is ever
# absorbed silently, one of those three facts moved.
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
    ``source_dir`` with the ``struct_for(table)`` schema and the reader options
    ``table``'s contract declares, adds ``_source_file``. Lookup-specific columns
    are added by the caller (see ``bronze_lookup_stream``).

    THE FORMAT AND ITS OPTIONS COME FROM THE CONTRACT, not from a parameter --
    ``opl.bronze.reader.source_format`` / ``read_options``. See the comment block
    above this function for that seam and for why the supplied schema is what makes
    ``_rescued_data`` the drift verdict.

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
        .option("cloudFiles.format", source_format(table))
        .option("cloudFiles.schemaLocation", schema_location(cfg, table_key, month=month))
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", RESCUED_DATA_COLUMN)
        .schema(struct_for(table))
    )
    for k, v in read_options(table).items():
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
