# databricks/src/bronze_payments_ingest.py
"""Job task: Auto Loader ingests a GENERATED table's newly-landed JSON Lines into its
staging table. AvailableNow: each run drains only files the checkpoint has not seen.

WHY A THIRD ENTRY POINT AND NOT A BRANCH IN `bronze_ingest.py`, since "a second
spelling of ingest" is the defect this repository names most often. It is the same
answer `bronze_lookup_ingest.py` already gives: what differs is not a parameter.

  - IT READS THE OTHER LANDING ROOT. A generated table lands under
    `generated/<month>/<subdir>` and a file-fed one under `cnpj/<month>/<subdir>`
    (`opl.config`, `opl.bronze.registry_landing`).
  - IT STAMPS FOUR AUDIT COLUMNS, NOT FIVE. `_snapshot_ref_date` is "the date the
    source declares in its own filename", which is a fact about the RFB's mainframe
    naming convention; a generated stream declares no such thing, so
    `add_common_audit_columns` is the right stamp and `add_audit_columns` is not. See
    that function for why stamping an all-NULL column instead would have forced the
    payments rule set to omit its own control.
  - ITS `_record_source` NAMES THIS REPOSITORY, not a WebDAV share.

WHAT IS SHARED IS EVERYTHING THAT COULD DRIFT. The read is `autoloader.bronze_stream`
-- one function, which takes its format, options and schema from the CONTRACT, so
JSON-vs-CSV is a lookup rather than a branch here. Every table name comes from
`table_spec`, so no string in this file could ever send a triager to the wrong
quarantine. And the gate, the promote and the failure task that follow are the SAME
entry points every CNPJ job runs: staging -> DQ gate -> promote, unchanged.

THE DRIFT IS MEANT TO BE QUARANTINED BY THAT SHARED GATE, and this task does nothing
to help it. `opl.contracts.payments` deliberately does not declare the drift column,
so `struct_for("payments")` does not carry it; `bronze_stream` supplies that schema
and sets `rescuedDataColumn`, so Auto Loader has nowhere to put the undeclared key
except `_rescued_data`; and `dq._reject_reason` ranks `rescued_data_present` above
every per-table rule. If the drift is ever absorbed silently, one of those three moved.

argv: [table, batch_id, month] -- all three REQUIRED, none defaulted."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.autoloader import (
    GENERATED_RECORD_SOURCE,
    add_common_audit_columns,
    bronze_stream,
    checkpoint_location,
)
from opl.bronze.promote import require_batch_id
from opl.bronze.registry import LANDING_GENERATED, BronzeTable, table_spec
from opl.config import DEFAULT, require_month


def _refuse_a_table_this_does_not_ingest(spec: BronzeTable) -> None:
    """Refuse a table whose files are not generated, before anything else is read.

    LOUD BEFORE THE SESSION, which is why `main` calls this the moment it has a spec
    and ahead of the batch id. Handed a CNPJ table, this would resolve a source
    directory under the generated root that no downloader has ever written to, start a
    serverless session, drain an empty directory and report SUCCESS having ingested
    zero rows -- and the row counts would be indistinguishable from a month in which
    no new file arrived. It also stamps four audit columns where that table's bronze
    carries five, so the append would then fail on a schema mismatch after the read.

    Compared against `LANDING_GENERATED` rather than for "not zips": the registry
    refuses unknown landing modes where they are declared, and any mode added
    later must be refused by default here rather than admitted by an `else`."""
    if spec.landing != LANDING_GENERATED:
        raise ValueError(
            f"{spec.name} lands as {spec.landing!r}, and this task ingests only tables "
            f"this lakehouse GENERATES (landing={LANDING_GENERATED!r}). Its files land "
            "under the CNPJ root and carry a source-declared reference date this "
            "stamp does not derive: run bronze_ingest.py instead, or "
            "bronze_lookup_ingest.py for the lookup."
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
        DEFAULT.landing_generated_table(spec.subdir, month),
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
        # Named, not defaulted: `add_audit_columns` defaults to the RFB WebDAV share,
        # and a row that cannot say whether this lakehouse or the Receita produced it
        # is a row whose provenance has to be inferred from its table name.
        record_source=GENERATED_RECORD_SOURCE,
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
