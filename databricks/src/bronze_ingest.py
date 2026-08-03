# databricks/src/bronze_ingest.py
"""Job task: Auto Loader ingests a table's newly-landed CSVs into its staging
table. AvailableNow: each run drains only files the checkpoint has not seen --
staged multi-part ingestion.

Parameterised by table, so adding a CNPJ table costs a registry entry and a job
YAML, not another copy of this file. The lookup keeps its own entry point
(bronze_lookup_ingest.py) because it routes six differently-named files into one
table by filename suffix -- a genuine difference, not a parameter.

argv: [table, batch_id, month] -- all three REQUIRED, none defaulted."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.autoloader import add_audit_columns, bronze_stream, checkpoint_location
from opl.bronze.promote import require_batch_id
from opl.bronze.registry import LANDING_ZIPS, BronzeTable, table_spec
from opl.config import DEFAULT, require_month


def _cannot_ingest(spec: BronzeTable) -> str:
    """Why this task refuses `spec` -- saying only what is true OF THAT TABLE.

    The "run X instead" sentence is CONDITIONAL, and that is the whole point. The
    guard's condition is a landing mode; the reason this task cannot ingest the
    lookup is the routing column its stream does not add. Those two coincide today
    and are not the same fact. A message that hard-coded "run bronze_lookup_ingest.py"
    would become false prose the day a locally-landed table that is NOT the lookup is
    registered -- and this repo's founding defect was precisely a message that sent an
    operator somewhere wrong with confidence (a hardcoded quarantine name that "sent
    estab triagers to a table full of unrelated F1.2 lookup rows"). A refusal read
    mid-incident may say less; it may not say something untrue."""
    why = (
        f"{spec.name} does not land as zips (landing={spec.landing!r}), and this task "
        "ingests only tables whose files land as zips in a per-table subdir."
    )
    if spec.name == "lookup":
        return (
            f"{why} The lookup routes six differently-named files into ONE table by "
            "filename suffix, which needs the lookup_type column this stream does not "
            "add: run bronze_lookup_ingest.py instead."
        )
    return (
        f"{why} No entry point ingests it as it is registered: either it needs one "
        "written, or its landing mode in opl.bronze.registry is wrong."
    )


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # Table first, and resolved BEFORE the batch id: a mistyped table name is
    # refused by table_spec naming the valid ones, and neither refusal needs Spark.
    spec = table_spec(args[0] if args else "")
    if spec.landing != LANDING_ZIPS:
        # No job passes the lookup here (bronze_job.yml runs bronze_lookup_ingest),
        # so this is the manual or misconfigured invocation -- the same case
        # require_batch_id exists for. It has to be loud HERE: this stream adds no
        # `lookup_type`, so the rows would be read and only then refused by the
        # Delta append's schema check, after a serverless session and a full scan
        # of the landed files. The registry refuses unknown landing modes where
        # they are declared, so this comparison cannot swallow a typo.
        raise ValueError(_cannot_ingest(spec))
    batch_id = require_batch_id(args[1] if len(args) > 1 else "", action="ingest")
    # NO DEFAULT. This local is handed to `add_audit_columns(snapshot_month=...)`
    # below, and that parameter was deliberately given no default so the config's
    # pinned month could never be supplied silently -- which is exactly what
    # `else DEFAULT.month` here used to do. The guard was satisfied by the one
    # value it exists to refuse, which made the no-default decorative, and a
    # decorative guard is worse than none because the next reader believes the
    # hole is closed. Both job YAMLs always pass {{job.parameters.month}}, so the
    # fallback was only ever reachable by a manual or misconfigured run -- the
    # case that must be loud, since it stamped `_snapshot_month = 2026-06` on rows
    # read from the 2026-06 landing dir and was indistinguishable from a correct
    # run in both the log and the data.
    month = require_month(args[2] if len(args) > 2 else None, action="ingest")
    spark = SparkSession.builder.getOrCreate()
    df = bronze_stream(
        spark,
        DEFAULT,
        spec.contract,
        DEFAULT.landing_table(spec.subdir, month),
        spec.table_key,
        month=month,
    )
    # The SAME `month` the stream read from -- ONE local, fed to all four consumers,
    # so the snapshot the rows are stamped with cannot drift from the folder they
    # came out of, and neither can the Auto Loader state that decides which of that
    # folder's files are new. The checkpoint became the fourth consumer in F1.4b PR B
    # Task 5 Step 0, and it is the one where a divergence is not merely wrong data:
    # a checkpoint keyed on another month is a stream restarted against a source
    # directory it never checkpointed, which Spark's recovery semantics call "not
    # allowed" and "likely to fail with unpredictable errors".
    audited = add_audit_columns(df, batch_id=batch_id, snapshot_month=month)
    query = (
        audited.writeStream.format("delta")
        .option(
            "checkpointLocation", checkpoint_location(DEFAULT, spec.table_key, month=month)
        )
        .trigger(availableNow=True)
        .toTable(DEFAULT.table(spec.staging))
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
