# databricks/src/generate_payments.py
"""Job task: derive one declared payment stream and land it in the Volume.

THE ANALOGUE OF `unzip_table`, FOR A SOURCE NOBODY DOWNLOADS. Every other bronze
table's landing dir is filled by something outside this job -- the extraction host
PUTs the RFB archives and `unzip_table` expands them. A generated table has no such
producer: its bytes do not exist until this task writes them, which is what
`opl.bronze.registry_landing.LANDING_GENERATED` means. So this runs FIRST among the
work tasks, exactly where the unzip runs in the CNPJ jobs, and the ingest that
follows reads what it wrote.

THE POOL IS THE ONLY RUN-TIME INPUT, and it is why this task needs a session at all.
`opl.generator` is a pure function of (seed, stream_id, pool); the first two are
declared in `opl.generator.profiles` and the third is 1,024 real `cnpj_basico` keys
drawn from `hub_empresa` -- 69,062,849 of them, in the workspace, built by F2 wave 1.
That draw is the whole integration claim: the payments reference companies that
actually exist, so `link_payment` and every downstream join have something to resolve
against. `cnpj_pool.pool_query` builds the SQL and executes nothing, so the generator
stays importable and testable with no Spark anywhere.

WHAT THIS TASK DOES NOT DECIDE. Not the seed, not the counts, not which defects are
injected -- all of that is `--params profile=<name>` selecting a declaration in the
repository, so every number a landed stream depends on is in a diff before the run.
And not the filename: `generated_landing.filename_for` derives it from the stream id, so a re-run
writes the same path and `emit_stream_file` compares bytes rather than overwriting.

IDEMPOTENT, BECAUSE `max_retries: 0` DOES NOT PREVENT A RETRY. A repair run re-derives
identical bytes and `emit_stream_file` reports the file as already present without
touching it -- which matters more here than for most tasks, because Auto Loader tracks
files by PATH: a second landing under a new name would be a second ingest of the same
payments, and the promote's `_batch_id` idempotence cannot see that.

argv: [table, month, profile] -- all three REQUIRED, none defaulted."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.generated_landing import EmittedFile, emit_stream_file
from opl.bronze.registry import LANDING_GENERATED, BronzeTable, table_spec
from opl.config import DEFAULT, require_month
from opl.generator.cnpj_pool import KEY_COLUMN, validated_pool
from opl.generator.defects import delivered_records
from opl.generator.profiles import POOL_SIZE, SENTINEL_PROFILE, StreamProfile, profile_for


def _refuse_a_table_nothing_generates(spec: BronzeTable) -> None:
    """Refuse a registered table whose bytes come from somewhere else.

    Handed `estabelecimentos`, this task would derive a payment stream and write it
    into the CNPJ landing dir that table's Auto Loader reads -- JSON lines under a
    stream reading semicolon CSV against a thirty-column schema. Every row would be
    rescued or NULL, the gate would reject the batch, and the diagnosis would start
    from a quarantine full of unrecognisable rows rather than from the wiring mistake.

    Compared against `LANDING_GENERATED` rather than for "not zips", the same way
    `reclaim_landing` states its own refusal: a fourth landing mode added later must
    be refused by default here, not admitted by an `else`."""
    if spec.landing != LANDING_GENERATED:
        raise ValueError(
            f"{spec.name} lands as {spec.landing!r}, and this task writes only tables "
            f"this lakehouse GENERATES (landing={LANDING_GENERATED!r}). Its files are "
            "produced by a downloader and expanded by unzip_table.py; deriving a "
            "payment stream into its landing dir would put JSON lines where a CSV "
            "stream reads, and every row would arrive rescued or NULL."
        )


def _counterparty_pool(spark: SparkSession, profile: StreamProfile) -> tuple[str, ...]:
    """The real `hub_empresa` keys this stream draws its counterparties from.

    REFUSES A SHORT POOL rather than generating from whatever came back, and that is
    the one thing a `LIMIT` cannot promise. `pool_query` asks for `POOL_SIZE` keys; a
    hub holding fewer -- an empty table, a wrong catalog, a vault that has not been
    loaded -- returns fewer, `validated_pool` accepts anything from two keys up, and
    the run would succeed having generated a DIFFERENT stream from the one the profile
    names. The pool's size and order decide which company gets which payment, so a
    short pool is a silently more concentrated payment stream, which is the failure
    `validated_pool`'s own duplicate refusal describes reached from the other side."""
    rows = spark.sql(profile.pool_query()).collect()
    pool = validated_pool(row[KEY_COLUMN] for row in rows)
    if len(pool) != POOL_SIZE:
        raise ValueError(
            f"the counterparty pool query returned {len(pool)} key(s) and {POOL_SIZE} "
            "were asked for. The pool's size and order decide which company gets which "
            "payment, so generating from a short one produces a stream that is not the "
            f"one {profile.name!r} declares -- and it reproduces perfectly, so nothing "
            "downstream would say so. Check that the vault's hub is loaded."
        )
    return pool


def _report(profile: StreamProfile, landed: EmittedFile) -> None:
    """What the run log says, and it is written to be quotable as evidence.

    The digest and the byte count are the same two numbers the golden pins are stated
    in, so this line and a local assertion compare directly with nothing re-derived.
    The predicted counts are printed BESIDE the measured row count rather than
    asserted, because they are properties of the declaration and this task's job is to
    show that what landed is what was declared.

    THE EVENT WINDOW IS THE THIRD LINE BECAUSE IT IS THE ONLY THING `between-snapshots`
    VARIES. Its rows, bytes and defect counts are `clean`'s, so the first two lines
    cannot tell the two streams apart; where the payments SIT is the whole claim, and
    an evidence document should be able to quote it from the run rather than re-derive
    it from `profiles.py`."""
    state = "already present, byte-identical" if landed.was_already_there else "written"
    print(
        f"generate_payments: profile={profile.name} stream_id={profile.stream_id} "
        f"{state} at {landed.path}"
    )
    print(
        f"generate_payments: rows={landed.row_count} (declared "
        f"{profile.delivered_row_count}) drifted={profile.drifted_row_count} "
        f"bytes={landed.byte_count} sha256={landed.sha256}"
    )
    print(
        f"generate_payments: event_time {profile.window_start} .. "
        f"{profile.last_event_time} ({profile.event_count} events, "
        f"{profile.event_interval_ms} ms apart)"
    )


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # Table first, and resolved BEFORE anything else: a mistyped table name is refused
    # by `table_spec` naming the valid ones, and none of these three refusals needs
    # Spark. An operator should not wait for a serverless session to be told which
    # argument is wrong.
    spec = table_spec(args[0] if args else "")
    _refuse_a_table_nothing_generates(spec)
    # NO DEFAULT, for `bronze_ingest`'s reason with a sharper edge here: this local
    # picks the directory the stream is WRITTEN into, and the ingest task that follows
    # resolves its own source dir from the same job parameter. A substituted month
    # would write into one month's landing dir and read another's -- a job that
    # succeeds having ingested nothing.
    month = require_month(args[1] if len(args) > 1 else None, action="generate")
    profile = profile_for(args[2] if len(args) > 2 else SENTINEL_PROFILE)
    spark = SparkSession.builder.getOrCreate()
    stream = profile.stream_spec(_counterparty_pool(spark, profile))
    landed = emit_stream_file(
        delivered_records(stream, profile.defects),
        stream,
        # The SAME `month` local for both, so the file cannot be staged under one
        # month and landed under another -- and `landing_generated_tmp` is outside
        # every directory an Auto Loader reads, so the half-written file the replace
        # below makes whole is never discoverable by the stream that reads the
        # finished one.
        directory=DEFAULT.landing_generated_table(spec.subdir, month),
        tmp_directory=DEFAULT.landing_generated_tmp(spec.subdir, month),
    )
    _report(profile, landed)


if __name__ == "__main__":
    main()
