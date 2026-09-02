# databricks/src/bronze_merchant_ingest.py
"""Job task: Auto Loader ingests a DATABASE-FED table's landed JSON Lines snapshots into
its staging table. AvailableNow: each run drains only files the checkpoint has not seen.

WHY A FIFTH ENTRY POINT AND NOT A BRANCH IN `bronze_ptax_ingest.py`, since "a second
spelling of ingest" is the defect this repository names most often. THE CODE THERE ALREADY
SAID SO, IN ADVANCE: `_refuse_a_table_this_does_not_ingest` compares against `LANDING_API`
rather than testing for "not generated", and its own docstring gives the reason -- "any
mode added later must be refused by default here rather than admitted by an `else`". That
task refuses this table by construction, deliberately, and reaching into it to add the
fifth mode would be spending a guard written for this exact moment.

The two differences are the same two that made each previous ingest its own file, and one
that no previous ingest has:

  - IT READS A FOURTH LANDING ROOT. A database-fed table lands under
    `postgres/<month>/<subdir>` (`opl.config`, `opl.bronze.registry_landing`).
  - ITS `_record_source` NAMES THE DATABASE. `API_RECORD_SOURCE` is `bcb_olinda_ptax`, and
    stamping this project's own operational Postgres with it would attribute those rows to
    the Banco Central -- in the one column that answers who produced a row.
  - IT STAMPS `_snapshot_ref_date` FROM A COLUMN THE ROW CARRIES, which is new. See below.

--- NOTHING IN THIS JOB PUTS THE FILE THERE, AND THAT IS THIS TASK'S ONE ODDITY --------

Every other bronze job has a task that FILLS the landing directory before the ingest runs:
`unzip_table` expands an archive the extraction host PUT, `generate_payments` derives the
stream from a seed, `fetch_ptax` calls an HTTP endpoint. This job has no such task, because
the producer runs OFF Databricks: `scripts/extract_merchant_snapshot.py` reads one
`REPEATABLE READ READ ONLY` transaction out of a Postgres container bound to
`localhost:5433` and PUTs the file through the Files API (plan T1). The reason is a
DIRECTION and not a firewall -- egress OUT of Databricks creates no route INTO a laptop
behind a home NAT -- and it is argued rather than measured, deliberately: no probe was ever
pointed at it and none is implied.

THE OPERATIONAL CONSEQUENCE, said plainly because a run that reports SUCCESS having
ingested nothing is indistinguishable from a month in which nothing arrived: the extractor
must have run, for both snapshots, before this job is launched.

--- FIVE AUDIT COLUMNS, AND THE FIFTH COMES FROM THE ROW -------------------------------

`bronze_ptax_ingest` stamps four and says why: `_snapshot_ref_date` is "the date the source
declares in its own filename", and BCB declares no such thing. This source declares no
filename token either -- AND IT CANNOT OMIT THE COLUMN, because this table is the first
non-file-fed source ever loaded into a vault SATELLITE and `opl.vault.satellites` reads
that column unconditionally to build `applied_date`. So `add_instant_audit_columns`
derives it from `_snapshot_at`, the instant the EXTRACTOR's transaction stamped and carried
in every row -- which is the same species ADR 0016 found for PTAX's quote date and the RFB's
applied date: three sources, three times, the key you must diff on is not in the payload.
THAT IS ADR 0017's PATTERN 1, which is where the three become a stated property rather than
three coincidences; 0016 is where the second of them was found.

THE INSTANT COLUMN COMES FROM THE SPEC, never from a literal here. `spec.snapshot_axis` is
what the vault's observation ledger groups by, so deriving `applied_date` from any other
column would put the satellite's fold and the ledger one observation apart.

WHAT IS SHARED IS EVERYTHING THAT COULD DRIFT. The read is `autoloader.bronze_stream` --
one function, which takes its format, options and schema from the CONTRACT. The source
directory comes from `registry_landing.landing_dir`, THE one place the landing-mode-to-root
mapping is made. Every table name comes from `table_spec`. And the gate, the promote and the
failure task that follow are the SAME entry points every other bronze job runs.

NO `reclaim_landing` TASK EXISTS FOR THIS TABLE, and it is a consequence of the mode rather
than an omission: that task refuses anything that is not `LANDING_ZIPS`, because a zip in
the sibling directory is the only way back to the source. The way back here is the DATABASE
-- and it is a weaker way back than a zip, because a snapshot is an instant that has passed,
so nothing is deleted.

argv: [table, batch_id, month] -- all three REQUIRED, none defaulted."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.autoloader import (
    POSTGRES_RECORD_SOURCE,
    add_instant_audit_columns,
    bronze_stream,
    checkpoint_location,
)
from opl.bronze.promote import require_batch_id
from opl.bronze.registry import LANDING_POSTGRES, BronzeTable, landing_dir, table_spec
from opl.config import DEFAULT, require_month


def _refuse_a_table_this_does_not_ingest(spec: BronzeTable) -> None:
    """Refuse a table whose files are not read out of a database, before anything is read.

    LOUD BEFORE THE SESSION, which is why `main` calls this the moment it has a spec and
    ahead of the batch id. Handed any other table, this would resolve a source directory
    under the postgres root that nothing has ever written to, start a serverless session,
    drain an empty directory and report SUCCESS having ingested zero rows -- and for THIS
    source that reading is especially dangerous, because a snapshot with no rows is exactly
    what a table every row of which was deleted looks like, and the vault would end-date
    every key it holds.

    It also derives `_snapshot_ref_date` from a column no other contract carries, so the
    stamp would fail on an unresolved column AFTER the read had been configured.

    Compared against `LANDING_POSTGRES` rather than for "not api": any mode added later
    must be refused by default here rather than admitted by an `else`. That is the same
    sentence `bronze_ptax_ingest` wrote about the mode this file exists for."""
    if spec.landing != LANDING_POSTGRES:
        raise ValueError(
            f"{spec.name} lands as {spec.landing!r}, and this task ingests only tables read "
            f"out of a database (landing={LANDING_POSTGRES!r}). Its files land under "
            "another root and derive their reference date another way: run bronze_ingest.py, "
            "bronze_lookup_ingest.py for the lookup, bronze_payments_ingest.py for a "
            "generated table, or bronze_ptax_ingest.py for an api-fed one."
        )


# --- WHAT `main` BELOW BINDS, AND WHY THAT PROSE IS UP HERE ----------------------------
#
# Module level for the reason `opl.bronze.rules` and `opl.bronze.registry_landing` state
# above their own long functions: it is the reasoning, not the code, and inside `main` it
# put that function past the project's 50-line limit.
#
# `month` HAS NO DEFAULT, and the reason `add_common_audit_columns` gives its own
# `snapshot_month` none: the config's pinned month is how F1.2 silently tied every row to
# 2026-06, and here it would ALSO resolve another month's Auto Loader checkpoint while
# reading this month's directory. ONE local feeds all four consumers -- the directory read,
# the inferred-schema store, the checkpoint that records which of that directory's files are
# new, and the value stamped into every row.
#
# `landing_dir` IS THE ONE MAPPING, asked rather than re-spelled. It takes the whole spec,
# so this table's directory cannot drift from its declared landing mode -- and a mode no
# root serves is refused there rather than defaulting into a directory holding another
# source's files, which cloudFiles walks RECURSIVELY.
#
# `_snapshot_month` IS STILL STAMPED AND IT IS STILL TRUE: it is "the month parameter the
# job ran with", which is an operational fact about the run. What it is NOT, for this source
# alone, is the snapshot AXIS -- both of this phase's snapshots are ingested by one job in
# one month, so the ledger reads `_snapshot_at` instead. Plan T7 refuses the alternative of
# labelling them two fabricated months.
#
# `record_source` IS NAMED AND NOT DEFAULTED: `add_audit_columns` defaults to the RFB WebDAV
# share, and a row that cannot say whether the Receita, this lakehouse's generator, the
# Banco Central or its own operational database produced it is a row whose provenance has to
# be inferred from its table name.
#
# `instant_column` COMES FROM THE SPEC, never a literal: it is the column the vault's
# observation ledger groups by, and `applied_date` has to be derived from that same one or
# the satellite's fold and the ledger sit one observation apart.
def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    spec = table_spec(args[0] if args else "")
    _refuse_a_table_this_does_not_ingest(spec)
    batch_id = require_batch_id(args[1] if len(args) > 1 else "", action="ingest")
    month = require_month(args[2] if len(args) > 2 else None, action="ingest")
    spark = SparkSession.builder.getOrCreate()
    df = bronze_stream(
        spark,
        DEFAULT,
        spec.contract,
        landing_dir(DEFAULT, spec, month),
        spec.table_key,
        month=month,
    )
    audited = add_instant_audit_columns(
        df,
        batch_id=batch_id,
        snapshot_month=month,
        record_source=POSTGRES_RECORD_SOURCE,
        instant_column=spec.snapshot_axis.column,
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
