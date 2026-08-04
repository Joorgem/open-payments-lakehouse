# databricks/src/reclaim_landing.py
"""Job task (runs after promote): delete the inner files THIS batch put in bronze.

Depends on promote, and only on its success: a file may go only once bronze
proves it holds that file's rows. The zips are never touched -- they live in a
sibling `zips/<table>` dir and are the only way back to the source if a parse
defect is found after ingestion, which is exactly what happened twice in F1.3.

THAT ARGUMENT ASSUMES A ZIP EXISTS IN THE VOLUME, which is true only for a table
whose landing mode is `zips`, so this task refuses every other one -- see
`_cannot_reclaim`.

WHAT THIS TASK IS ALLOWED TO FAIL ON, because "never fail the job" is not the
same as "never raise". The argument guards still raise, before Spark and before
anything is deleted: an unknown table (`table_spec`), a batch id that names no
batch (`require_batch_id`) or an absent/malformed month (`require_month`) each
mean this task does not know WHICH files it would be reclaiming, and reclaiming
under a guess is how the wrong table's landing dir gets emptied. What never
raises is the deletion itself -- past that point the rows are already in bronze,
so a file that cannot be removed is a quota problem, not a data problem, and must
not turn a green ingestion red.

argv: [table, batch_id, month] -- all three REQUIRED, none defaulted."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.promote import require_batch_id
from opl.bronze.registry import LANDING_ZIPS, BronzeTable, table_spec
from opl.bronze.retention import (
    LandingScope,
    RetentionOutcome,
    delete_files,
    files_of_batch,
    scope_to_landing_dir,
)
from opl.config import DEFAULT, require_month


def _cannot_reclaim(spec: BronzeTable) -> str:
    """Why this task refuses a table that does not land as zips.

    THE REASON IS AN ABSENT ARCHIVE, not a landing mode -- the mode is only how the
    absence is detected. Both this module's docstring and `opl.bronze.retention`'s
    rest on "the zips are the only way back to the source", and F1.3's incidents 3
    and 4 were parse defects found AFTER ingestion whose fix required re-reading
    that source. For a `local`-landed table there is no zip in the Volume to go
    back to: `scripts/extract_cnpj.py` unzips on the extraction host and PUTs only
    the inner file, so no zip of it ever reaches the Volume -- `cnpj/<month>/zips/`
    holds a subdirectory only for the groups that land AS zips (2026-06:
    `estabelecimentos`; 2026-07: `empresas`, `estabelecimentos`, `socios`) and
    never one for a `local`-landed table. The six lookup CSVs under `lookups/` are
    the ONLY copy in the workspace, and recovery would mean a fresh WebDAV download
    of a monthly snapshot the RFB may have rotated -- from a share ADR 0003
    measured at ~50% transient 500s.

    Refused rather than left to the operator's judgement because the ONE caller
    that can reach it is an operator: no job YAML runs this task for such a table
    (bronze_job.yml deliberately has no reclaim task), and the recorded procedure
    invokes this file by hand with a positional table name. `bronze_ingest.py` and
    `unzip_table.py` already refuse the same way; this task, the only one that
    DELETES, was the one that did not.

    Compares against LANDING_ZIPS rather than for LANDING_LOCAL, so a third landing
    mode added later is refused by default instead of inheriting a delete."""
    return (
        f"refusing to reclaim {spec.name}: it does not land as zips "
        f"(landing={spec.landing!r}), so NO ARCHIVE OF IT EXISTS IN THE VOLUME to "
        "recover from. Its files are unzipped on the extraction host and only the "
        "inner file is PUT, which makes the landed file the single copy in this "
        "workspace -- deleting it would leave a fresh RFB download of a snapshot "
        "that may already have been rotated as the only way back. This task's whole "
        "safety argument is the zip in the sibling zips/ dir, and for this table "
        "there is none."
    )


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # Table first and before Spark, like every other task here: a mistyped table
    # is refused by the registry naming the valid ones. It matters more here than
    # anywhere else -- the table decides both which rows are read as proof AND
    # which directory the deletes are confined to.
    spec = table_spec(args[0] if args else "")
    if spec.landing != LANDING_ZIPS:
        # Before the batch id and before Spark, because it is not a fact about this
        # run: no batch of this table may ever be reclaimed, so nothing after this
        # point needs to be resolved to know that.
        raise ValueError(_cannot_reclaim(spec))
    batch_id = require_batch_id(args[1] if len(args) > 1 else "", action="reclaim")
    # NO DEFAULT, by the same guard the two ingest tasks use -- it has to be the
    # SAME month the ingest was given, because the files this batch proved were read
    # out of that month's landing dir, and a WRONG-but-well-formed month puts every
    # proven file outside the scope and reclaims nothing.
    #
    # What makes it worth a crash rather than a fallback is specific to this task:
    # `DEFAULT.month` equals the job YAML's own default, so an omission stayed
    # invisible until the first run for another month -- and then this task printed
    # REFUSED for every proven file and exited green, a log that reads exactly like
    # the containment guard catching a real F1.3-style incident. `require_month`
    # carries the rest of the argument, including why a malformed month is half of
    # the delete boundary rather than something the boundary checks.
    month = require_month(args[2] if len(args) > 2 else None, action="reclaim")
    spark = SparkSession.builder.getOrCreate()
    bronze = DEFAULT.table(spec.bronze)
    if not spark.catalog.tableExists(bronze):
        # Split out from the empty proof set below because it means something
        # else entirely: the authority this task defers to does not exist. A
        # reader who saw only "nothing is proven persisted" could conclude the
        # batch was already reclaimed; the files are landed and bronze is gone.
        print(f"reclaim_landing: {bronze} does not exist, so nothing can be proven "
              f"persisted and nothing is deleted. The landed files of batch {batch_id} "
              "are still in the Volume, which is the recoverable direction -- re-run "
              "the ingest and promote for this month before reclaiming anything")
        return
    proven = files_of_batch(spark, bronze, batch_id)
    if not proven:
        _report_nothing_proven(bronze, batch_id, spec.name)
        return
    landing_dir = DEFAULT.landing_table(spec.subdir, month)
    scope = scope_to_landing_dir(proven, landing_dir)
    outcome = delete_files(scope.inside)
    _report(outcome, scope, batch_id=batch_id, table=spec.name, landing_dir=landing_dir)


def _report_nothing_proven(bronze: str, batch_id: str, table: str) -> None:
    """Bronze exists and holds no file of this batch: delete nothing, say why.

    THE DECISION: return green having deleted nothing, and name the causes rather
    than shrug. The safe action is the same for every cause -- an empty proof set
    proves nothing, so nothing may go -- but the causes are not the same event and
    the operator's next move differs, so a bare "nothing to do" would hide two of
    them. It does not raise, because the first cause below is the pipeline's own
    legitimate quiet path and failing it would turn every no-new-file run red;
    and because in all three cases the bytes simply stay, which is the direction
    that loses nothing."""
    print(f"reclaim_landing: {bronze} holds no row of batch {batch_id} -- nothing is "
          "proven persisted, so NOTHING WAS DELETED and the landed files stay. One of: "
          f"(a) this run's ingest found no new file for {table}, the flow's legitimate "
          "no-op, and there is genuinely nothing to reclaim; (b) the batch id names no "
          "batch this table ever ingested -- a well-formed id passed by hand is "
          "indistinguishable from a typo here, and require_batch_id only refuses a "
          "blank or the sentinel; (c) bronze was rebuilt after the promote, which "
          "destroyed the proof while leaving the files. Only (a) needs nothing done: "
          f"check with SELECT count(*) FROM {bronze} WHERE _batch_id = '{batch_id}'")


def _report(
    outcome: RetentionOutcome,
    scope: LandingScope,
    *,
    batch_id: str,
    table: str,
    landing_dir: str,
) -> None:
    """What the run log says happened. Four outcomes, none of them collapsed.

    Modelled on `landing._discard_remote`, which reports deleted / already-absent
    / STILL THERE apart for the reason this task needs too: an operator told
    nothing assumes the file is gone, and a file that could not be removed still
    holds Volume quota nobody knows about. The fourth -- refused -- is this task's
    own and is the loudest, because it says bronze holds rows sourced from outside
    the dir this table's stream reads."""
    print(f"reclaim_landing: batch={batch_id} table={table} "
          f"deleted={len(outcome.deleted)} already_absent={len(outcome.absent)} "
          f"failed={len(outcome.failed)} refused={len(scope.outside)}")
    for path, reason in outcome.failed:
        print(f"  STILL THERE: {path} -- {reason}")
    for path in scope.outside:
        # Not deleting it is the easy half. The hard half is that a row like this
        # should not exist: it means this table's stream read a file outside the
        # dir it was pointed at, which is how an F1.3 probe.txt in
        # `zips/estabelecimentos/` ended up in the lookup staging table.
        print(f"  REFUSED (left untouched): {path} -- bronze credits it to batch "
              f"{batch_id}, but it is not under {landing_dir}. This reclaim deletes "
              "only files in the dir this table's Auto Loader reads, so the zips and "
              "every other table's files are out of reach by construction. Investigate "
              "how a row of this table came from there before reclaiming anything else")
    if outcome.deleted or not outcome.absent:
        return
    # Every single file already gone. That is the expected shape of an idempotent
    # re-run -- and it is ALSO exactly what a path form this code cannot resolve
    # looks like, since an unlink of a path that exists nowhere raises
    # FileNotFoundError just like one that was already reclaimed. Two very
    # different facts with one signature, so the log refuses to let them read alike.
    print(f"  NOTE: all {len(outcome.absent)} proven file(s) were already gone and no "
          "byte was reclaimed. Expected on a repair run or a second reclaim of the "
          "same batch. If this is the FIRST reclaim of this batch, do not read it as "
          "success -- check that the files are really absent from the Volume "
          f"(databricks fs ls dbfs:{landing_dir}) before assuming the space was freed")


if __name__ == "__main__":
    main()
