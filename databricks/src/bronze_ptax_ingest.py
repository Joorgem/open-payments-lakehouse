# databricks/src/bronze_ptax_ingest.py
"""Job task: Auto Loader ingests an API-FED table's newly-landed JSON Lines into its
staging table. AvailableNow: each run drains only files the checkpoint has not seen.

WHY A FOURTH ENTRY POINT AND NOT A BRANCH IN `bronze_payments_ingest.py`, since "a second
spelling of ingest" is the defect this repository names most often -- and this one is
closer to an existing file than any before it. THE CODE THERE ALREADY SAID SO, IN
ADVANCE. `_refuse_a_table_this_does_not_ingest` compares against `LANDING_GENERATED`
rather than testing for "not zips", and its own docstring gives the reason: "a fourth
mode added later must be refused by default here rather than admitted by an `else`". That
task refuses PTAX by construction, deliberately, and reaching into it to add the fourth
mode would be spending a guard that was written for this exact moment.

The two differences are the same two that made the payments ingest its own file:

  - IT READS A THIRD LANDING ROOT. An api-fed table lands under `api/<month>/<subdir>`,
    a generated one under `generated/...` and a file-fed one under `cnpj/...`
    (`opl.config`, `opl.bronze.registry_landing`).
  - ITS `_record_source` NAMES THE BANCO CENTRAL. `GENERATED_RECORD_SOURCE` is
    `opl_payment_generator`, and stamping BCB's published rates with it would say this
    repository produced them -- in the one column that answers who produced a row. That
    single value is why `LANDING_GENERATED` was rejected for this source, so reusing the
    task that stamps it would reintroduce the claim the mode was rejected over.

IT STAMPS FOUR AUDIT COLUMNS, NOT FIVE, and for the payments ingest's reason:
`_snapshot_ref_date` is "the date the source declares in its own filename", which is a
fact about the RFB's mainframe naming convention. BCB declares no such thing, so
`add_common_audit_columns` is the right stamp -- and stamping the wider one would put an
all-NULL column on every row, which `rules._unprovable_ref_date` exists to REJECT, so the
PTAX rule set would have had to omit its own control to let the table load.

WHAT IS SHARED IS EVERYTHING THAT COULD DRIFT. The read is `autoloader.bronze_stream` --
one function, which takes its format, options and schema from the CONTRACT, so JSON-vs-CSV
is a lookup rather than a branch here. The source directory comes from
`registry_landing.landing_dir`, THE one place the landing-mode-to-root mapping is made,
rather than from this file knowing the layout. Every table name comes from `table_spec`.
And the gate, the promote and the failure task that follow are the SAME entry points every
other bronze job runs: staging -> DQ gate -> promote, unchanged.

NO `reclaim_landing` TASK EXISTS FOR THIS TABLE, and it is a consequence of the mode
rather than an omission: that task refuses anything that is not `LANDING_ZIPS`, because it
deletes landed files only where a zip in the sibling `zips/` dir is still the way back to
the source. The way back here is the REQUEST -- the window and the currency, which the
landing filename carries -- so the job stops at the promote.

argv: [table, batch_id, month] -- all three REQUIRED, none defaulted."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.autoloader import (
    API_RECORD_SOURCE,
    add_common_audit_columns,
    bronze_stream,
    checkpoint_location,
)
from opl.bronze.promote import require_batch_id
from opl.bronze.registry import LANDING_API, BronzeTable, landing_dir, table_spec
from opl.config import DEFAULT, require_month


def _refuse_a_table_this_does_not_ingest(spec: BronzeTable) -> None:
    """Refuse a table whose files are not fetched from an API, before anything is read.

    LOUD BEFORE THE SESSION, which is why `main` calls this the moment it has a spec and
    ahead of the batch id. Handed a CNPJ table, this would resolve a source directory
    under the api root that nothing has ever written to, start a serverless session,
    drain an empty directory and report SUCCESS having ingested zero rows -- and the row
    counts would be indistinguishable from a month in which no new file arrived. It also
    stamps four audit columns where that table's bronze carries five, so the append would
    then fail on a schema mismatch after the read.

    Compared against `LANDING_API` rather than for "not generated": the registry refuses
    unknown landing modes where they are declared, and a fifth mode added later must be
    refused by default here rather than admitted by an `else`. That is the same sentence
    `bronze_payments_ingest` wrote about the mode this file exists for."""
    if spec.landing != LANDING_API:
        raise ValueError(
            f"{spec.name} lands as {spec.landing!r}, and this task ingests only tables "
            f"served by an API (landing={LANDING_API!r}). Its files land under another "
            "root and may carry a source-declared reference date this stamp does not "
            "derive: run bronze_ingest.py, bronze_lookup_ingest.py for the lookup, or "
            "bronze_payments_ingest.py for a generated table."
        )


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    spec = table_spec(args[0] if args else "")
    _refuse_a_table_this_does_not_ingest(spec)
    batch_id = require_batch_id(args[1] if len(args) > 1 else "", action="ingest")
    # NO DEFAULT, and the reason `add_common_audit_columns` gives its `snapshot_month`
    # none: the config's pinned month is how F1.2 silently tied every row to 2026-06,
    # and here it would ALSO resolve another month's Auto Loader checkpoint while
    # reading this month's directory.
    month = require_month(args[2] if len(args) > 2 else None, action="ingest")
    spark = SparkSession.builder.getOrCreate()
    df = bronze_stream(
        spark,
        DEFAULT,
        spec.contract,
        # THE ONE MAPPING, asked rather than re-spelled. `landing_dir` takes the whole
        # spec, so this table's directory cannot drift from its declared landing mode --
        # and a mode no root serves is refused there rather than defaulting into a
        # directory holding another source's files, which cloudFiles walks RECURSIVELY.
        landing_dir(DEFAULT, spec, month),
        spec.table_key,
        month=month,
    )
    # The SAME `month` local, fed to all four consumers -- the directory read, the
    # inferred-schema store, the checkpoint that records which of that directory's
    # files are new, and the value stamped into every row. The checkpoint is the one
    # where a divergence is not merely wrong data: a stream restarted against a source
    # directory it never checkpointed is what Spark's recovery semantics call "not
    # allowed" and "likely to fail with unpredictable errors".
    audited = add_common_audit_columns(
        df,
        batch_id=batch_id,
        snapshot_month=month,
        # Named, not defaulted: `add_audit_columns` defaults to the RFB WebDAV share, and
        # a row that cannot say whether the Receita, this lakehouse's generator or the
        # Banco Central produced it is a row whose provenance has to be inferred from its
        # table name.
        record_source=API_RECORD_SOURCE,
    )
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
