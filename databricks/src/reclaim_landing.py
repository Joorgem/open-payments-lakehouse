# databricks/src/reclaim_landing.py
"""Job task (runs after promote): delete the inner files THIS batch put in bronze.

Depends on promote, and only on its success: a file may go only once the rows it
staged are accounted for and some of them are in bronze. The zips are never
touched -- they live in a sibling `zips/<table>` dir and are the only way back to
the source if a parse defect is found after ingestion, which is exactly what
happened twice in F1.3.

THAT ARGUMENT ASSUMES A ZIP EXISTS IN THE VOLUME, which is true only for a table
whose landing mode is `zips`, so this task refuses every other one -- see
`_cannot_reclaim` and `ANY_TABLE_FLAG`.

WHAT THIS TASK IS ALLOWED TO FAIL ON, because "never fail the job" is not the
same as "never raise". The argument guards still raise, before Spark and before
anything is deleted: an unknown table (`table_spec`), a batch id that names no
batch (`require_batch_id`) or a malformed month (`require_month`) each mean this
task does not know WHICH files it would be reclaiming, and reclaiming under a
guess is how the wrong table's landing dir gets emptied. So does an argument this
task does not read (`_positional_arguments`), because the reclaim then runs having
ignored what its operator believed they were configuring. So does a month that
CONTRADICTS the one the ingest stamped -- see `refuse_month_disagreement`, which
runs before the empty-proof return so that it fires on the batches whose month or
batch id is the very thing that is wrong. What never raises is the deletion itself
-- past that point the rows are already in bronze, so a file that cannot be
removed is a quota problem, not a data problem, and must not turn a green
ingestion red.

WHAT IS NOT PART OF THE PROOF IS PATH IDENTITY. The delete target is a
`_source_file` STRING, and nothing here checks that the object at that path today
is the object the batch read. In flow the gap was minutes; through
`repromote_triaged_batch` it is hours or days, and the documented recovery for a
parse defect is to re-land the SAME filenames. Recorded in full beside the wiring
that made it matter -- see `repromote_batch_job.yml`'s reclaim task.

argv: [table, batch_id, month?, --any-table?]

THE MONTH IS NOW OPTIONAL AND THAT IS NOT A RELAXATION OF `require_month`. It was
required because the alternative on the table was `DEFAULT.month`, a pinned guess
that equalled the job YAMLs' own default and so stayed invisible until the first
run for another month. The alternative now is the value the INGEST stamped into
`_snapshot_month` for this very batch, which is that run's own
`{{job.parameters.month}}` after it passed `require_month` -- data, not a guess,
and not read off the `_source_file` paths this month is used to judge. When a
month IS passed it is still validated before Spark and cross-checked against the
stamp WHERE THERE IS ONE, so the three CNPJ ingestion jobs that run this task
gain a control rather than losing one.

WHERE THERE IS NONE, THE CROSS-CHECK DOES NOT FIRE, and that is the honest width
of the control rather than a caveat: a pre-F1.4a staging shape has no
`_snapshot_month` column, so a passed month is returned unchecked and
`retention.scope_to_landing_dir`'s containment is the whole guard. See
`refuse_month_disagreement` for why that was not widened.

WHY THE TRIAGE PATH NEEDED THAT. `repromote_triaged_batch` runs hours or days
after the ingest, from a reconciliation row that names a table and a batch id and
nothing else. Requiring a month there would put the delete boundary in a value an
operator retypes from memory, whose wrong-but-well-formed shape refuses every
proven file and exits GREEN -- a log that reads exactly like the containment guard
catching a real F1.3-style incident.
"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.promote import require_batch_id
from opl.bronze.registry import LANDING_ZIPS, BronzeTable, table_spec
from opl.bronze.retention import (
    FileAccount,
    LandingScope,
    RetentionOutcome,
    delete_files,
    file_accounts_of_batch,
    months_of_batch,
    scope_to_landing_dir,
)
from opl.config import DEFAULT, require_month

# Declares "I am a job task wired into a job that serves EVERY registered table",
# which today is exactly one caller: `repromote_batch_job.yml`. Modelled on
# `promote_batch.py`'s `--in-flow` and for the identical reason -- the same fact
# is an operator error in one context and a legitimate no-op in the other, and the
# caller is the only one that can say which.
#
# WHAT IT DOES NOT DO IS LOOSEN THE DELETE. A table with no archive in the Volume
# deletes nothing either way; the flag changes whether that is red or green. A
# repromote of `payments` -- the stranded batch the reconciliation view prints a
# remedy for right now -- would otherwise end RED on its last task having promoted
# perfectly, which trains an operator to read red as noise.
#
# Fail-closed by absence: a hand invocation omits it and still gets the refusal,
# which is the caller `_cannot_reclaim` was written for.
ANY_TABLE_FLAG = "--any-table"


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

    Refused rather than left to the operator's judgement because the caller that
    can reach it by name is an operator: the THREE ingestion job YAMLs that run
    this task at all (empresas, estabelecimentos, socios) run it only for tables
    that land as zips -- `bronze_job.yml` declares no such task and says why -- and
    the recorded procedure invokes this file by hand with a positional table name.
    `bronze_ingest.py` and `unzip_table.py` already refuse the same way; this task,
    the only one that DELETES, was the one that did not. The one job that reaches it
    for ANY table says so with `ANY_TABLE_FLAG` and gets this same text as a green
    no-op.

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
    positional = _positional_arguments(args)
    # Table first and before Spark, like every other task here: a mistyped table
    # is refused by the registry naming the valid ones. It matters more here than
    # anywhere else -- the table decides both which rows are read as proof AND
    # which directory the deletes are confined to.
    spec = table_spec(positional[0] if positional else "")
    no_archive = _refuse_a_table_with_no_archive(spec, args)
    # AFTER that refusal and BEFORE the green no-op it allows. `--any-table` says a
    # table with no archive is a legitimate no-op for this caller; it never said an
    # invocation naming no batch is one. Until F4's review it suppressed both, and
    # `reclaim_landing.py payments "" --any-table` exited 0 green -- a flag added
    # for one guard silencing another.
    batch_id = require_batch_id(positional[1] if len(positional) > 1 else "", action="reclaim")
    if no_archive:
        print(f"reclaim_landing: {_cannot_reclaim(spec)}")
        return
    # Validated HERE when it is given, so a malformed one still never reaches a
    # serverless session; the two functions below own absence and disagreement,
    # both of which need the batch's own rows to answer.
    given = require_month(positional[2], action="reclaim") if len(positional) > 2 else None
    spark = SparkSession.builder.getOrCreate()
    bronze = DEFAULT.table(spec.bronze)
    if not spark.catalog.tableExists(bronze):
        _report_no_bronze(bronze, batch_id)
        return
    staging = DEFAULT.table(spec.staging)
    accounts = file_accounts_of_batch(spark, spec, batch_id)
    proven = tuple(account for account in accounts if account.reclaimable)
    held = tuple(account for account in accounts if not account.reclaimable)
    stamped = months_of_batch(spark, staging, batch_id)
    # Read ONCE and cross-checked BEFORE the empty-proof return below, which is
    # where the check could not see: a batch proving no file returned green several
    # lines above it, so a contradicting month went undetected on exactly the runs
    # whose batch id or month is the thing that is wrong.
    refuse_month_disagreement(given, stamped, batch_id, staging)
    if not proven:
        _report_nothing_proven(bronze, batch_id, spec.name, held)
        return
    month = resolve_month(given, stamped, batch_id, staging)
    landing_dir = DEFAULT.landing_table(spec.subdir, month)
    scope = scope_to_landing_dir([account.source_file for account in proven], landing_dir)
    outcome = delete_files(scope.inside)
    _report(outcome, scope, held, batch_id=batch_id, table=spec.name, landing_dir=landing_dir)


def _positional_arguments(args: list[str]) -> list[str]:
    """The positional arguments, with everything this task does not read REFUSED.

    A FOURTH ARGUMENT USED TO BE DISCARDED AND THE FILES DELETED ANYWAY. The header's
    `argv:` line is the whole surface, and stripping the flag then reading `[0]`,
    `[1]`, `[2]` reads a mistyped `--dry-run` as nothing at all -- an operator who
    assumed this task has one gets a run that ignores it and unlinks every proven
    file. `create_dataops_views.main`, this same phase's other task, already refuses
    exactly this: a task given a parameter it does not read is a job YAML that
    believes it is configuring something.

    A REPEATED FLAG IS REFUSED TOO, because `--any-table --any-table` reads
    identically to one today. The duplicate that matters is not a typed one: it is a
    paste that put the flag where the month was meant, which silently drops a delete
    boundary an operator did pass."""
    repeats = args.count(ANY_TABLE_FLAG)
    if repeats > 1:
        raise ValueError(
            f"refusing to reclaim: {ANY_TABLE_FLAG} was passed {repeats} times "
            f"({args!r}). It is a switch and not a count, so a second one is a paste "
            "over an argument this task DOES read -- most likely the month. NOTHING "
            "WAS DELETED"
        )
    positional = [arg for arg in args if arg != ANY_TABLE_FLAG]
    if len(positional) > 3:
        raise ValueError(
            f"refusing to reclaim: this task takes [table, batch_id, month?] and was "
            f"handed {positional!r}. NOTHING WAS DELETED. Nothing here reads a fourth "
            f"argument, so a flag this task does not have -- a --dry-run, say -- would "
            f"otherwise be discarded in silence while every proven file was unlinked. "
            f"The only flag is {ANY_TABLE_FLAG}; see the header's argv line"
        )
    return positional


def _refuse_a_table_with_no_archive(spec: BronzeTable, args: list[str]) -> bool:
    """Whether this table's landed files may never go -- raising unless told otherwise.

    ANSWERED BEFORE THE BATCH ID AND BEFORE SPARK, because it is not a fact about
    this run: no batch of this table may ever be reclaimed, so nothing after this
    point needs to be resolved to know that. `True` is the flagged caller's green
    no-op, which `main` still puts BEHIND `require_batch_id` -- see there.

    THE SPLIT IS BY ASYMMETRY OF RECOVERABILITY, and that is the reason the two
    halves of this one answer sit on opposite sides of `require_batch_id`. The
    REFUSAL is a fact about the TABLE that no later value can change, so resolving
    anything first would only delay a raise that is already certain. The NO-OP is a
    fact about THIS INVOCATION -- "for this caller, that refusal is expected" -- and
    an invocation that names no batch has not established that it IS the invocation
    the flag was meant for. Green is the answer that can be wrong, so it is the one
    that waits for the guards."""
    if spec.landing == LANDING_ZIPS:
        return False
    if ANY_TABLE_FLAG in args:
        return True
    raise ValueError(_cannot_reclaim(spec))


def resolve_month(
    given: str | None, stamped: tuple[str, ...], batch_id: str, staging: str
) -> str:
    """The month that is half the delete boundary: stamped by the ingest, or agreed.

    TAKES THE STAMP RATHER THAN READING IT, so `main` queries `_snapshot_month`
    once and the two things done with it happen at the two points they belong at:
    the contradiction is `refuse_month_disagreement`, called before the
    empty-proof return, while this runs only once a file is going to be unlinked.

    DERIVED WHEN NOTHING WAS PASSED, from `_snapshot_month` on this batch's staging
    rows -- see `retention.months_of_batch` for why that column and not the file
    paths. Refused when the batch does not name exactly ONE month, which is the
    only shape a derivation may take: zero means the proof's own source is gone or
    predates the column, more than one means a batch spanning months, and neither
    is a directory this task may guess at.

    A PASSED MONTH COMES BACK AS IT CAME, because everything that can refuse it has
    already run: `require_month` in `main` before Spark, and the cross-check."""
    if given is not None:
        return given
    if len(stamped) != 1:
        raise ValueError(_month_underivable(stamped, batch_id, staging))
    # Validated even though it came from the data: this value is interpolated raw
    # into `landing_table`, so it is half OF the delete path rather than something
    # the path checks, and where it came from does not change that.
    return require_month(stamped[0], action="reclaim")


def refuse_month_disagreement(
    given: str | None, stamped: tuple[str, ...], batch_id: str, staging: str
) -> None:
    """Raise when a passed month disagrees with the one the INGEST stamped.

    THE CONTROL THE THREE CNPJ INGESTION JOBS GAIN, since they have always passed
    `{{job.parameters.month}}`. A month that disagrees with the stamp used to
    produce the WORST log this task can emit: every proven file outside the scope,
    `REFUSED (left untouched)` on each one telling the operator to investigate how
    a row of this table came from there, zero bytes freed, exit 0. It raises now,
    naming both months, before a single unlink.

    AN EMPTY STAMP IS NOT A DISAGREEMENT, AND THAT IS THE WIDTH OF THIS CONTROL
    rather than a caveat on it. A pre-F1.4a staging shape has no `_snapshot_month`
    column and a batch whose staging rows are gone stamps nothing, so a passed
    month is returned unchecked there and containment is the whole guard.
    REJECTED: treating an absent stamp as a refusal. It would turn every reclaim
    over such a shape red while freeing not one byte more, and the rebuild that
    leaves those shapes is documented and expected -- so this raises only where
    two answers actually contradict, and says so here rather than letting a flat
    'it raises now' stand as the claim.

    NOT REACHED AT ALL WHEN BRONZE IS MISSING, which is deliberate and is the one
    return that still stands ahead of it: without the authority the batch cannot be
    judged on any question, and a message about the month would misreport where the
    run stopped. `_report_no_bronze` says the harder fact instead."""
    if stamped and given is not None and (given,) != stamped:
        raise ValueError(_month_disagreement(given, stamped, batch_id, staging))


def _month_disagreement(
    given: str, stamped: tuple[str, ...], batch_id: str, staging: str
) -> str:
    return (
        f"refusing to reclaim: month={given!r} was passed, but batch {batch_id} was "
        f"ingested under _snapshot_month={list(stamped)} according to {staging}. The "
        "month is half the delete boundary -- landing_table(subdir, month) is the ONLY "
        "directory these deletes are confined to -- so the two cannot both be right and "
        "this task will not pick one. NOTHING WAS DELETED. Either the month parameter "
        "of this run is wrong (pass the SAME month the ingest was given, or pass none "
        "at all and it is taken from the stamp), or this batch id belongs to a "
        "different run than the one intended."
    )


def _month_underivable(stamped: tuple[str, ...], batch_id: str, staging: str) -> str:
    return (
        f"refusing to reclaim: no month was passed and batch {batch_id} does not name "
        f"exactly one in {staging} -- _snapshot_month is {list(stamped)}. NOTHING WAS "
        "DELETED and the landed files stay, which is the recoverable direction. Zero "
        "means the staging rows of this batch are gone, or that this staging table "
        "predates the _snapshot_month column (the documented rebuild leaves such "
        "shapes); more than one means a single batch id spans months, which one "
        "ingest run cannot produce and is worth understanding before anything is "
        "unlinked. There is no default to fall back on: opl.config's pinned month is "
        "the worst guess available, and substituting it is how a delete boundary lands "
        "on a month this run never ingested. Check what the batch was ingested under "
        f"(SELECT DISTINCT _snapshot_month FROM {staging} WHERE _batch_id = "
        f"'{batch_id}'); if the answer is nothing, this file takes the month as its "
        "third positional argument and an operator can run it by hand with the month "
        "the ingest was given."
    )


def _report_no_bronze(bronze: str, batch_id: str) -> None:
    """Bronze itself is missing: the authority this task defers to does not exist.

    Split out from the empty proof set below because it means something else
    entirely. A reader who saw only "nothing is proven persisted" could conclude
    the batch was already reclaimed; the files are landed and bronze is gone."""
    print(f"reclaim_landing: {bronze} does not exist, so nothing can be proven "
          f"persisted and nothing is deleted. The landed files of batch {batch_id} "
          "are still in the Volume, which is the recoverable direction -- re-run "
          "the ingest and promote for this month before reclaiming anything")


def _report_nothing_proven(
    bronze: str, batch_id: str, table: str, held: tuple[FileAccount, ...]
) -> None:
    """No file of this batch reconciles: delete nothing, say why, per file.

    THE DECISION: return green having deleted nothing, and name the causes rather
    than shrug. The safe action is the same for every cause -- an unproven file
    proves nothing, so nothing may go -- but the causes are not the same event and
    the operator's next move differs, so a bare "nothing to do" would hide most of
    them. It does not raise, because the first cause below is the pipeline's own
    legitimate quiet path and failing it would turn every no-new-file run red; and
    because in all cases the bytes simply stay, which is the direction that loses
    nothing."""
    if held:
        print(f"reclaim_landing: batch {batch_id} touched {len(held)} file(s) and NOT ONE "
              "of them is accounted for, so NOTHING WAS DELETED and the landed files "
              "stay. A file may go only when every row it staged is in bronze or in "
              "quarantine and at least one is in bronze")
        _report_held_back(held, batch_id)
        return
    # NOT "bronze holds no row of this batch", which this line said until F4's second
    # correction pass and which the DROPPED line above can contradict outright: a batch
    # whose rows all carry a NULL `_source_file` yields no account at all, so the green
    # message claimed bronze was empty while handing the operator a count that comes
    # back non-zero. What is true in every case here is that no FILE is proven.
    print(f"reclaim_landing: no file of batch {batch_id} is proven persisted in "
          f"{bronze}, so NOTHING WAS DELETED and the landed files stay. One of: "
          f"(a) this run's ingest found no new file for {table}, the flow's legitimate "
          "no-op, and there is genuinely nothing to reclaim; (b) the batch id names no "
          "batch this table ever ingested -- a well-formed id passed by hand is "
          "indistinguishable from a typo here, and require_batch_id only refuses a "
          "blank or the sentinel; (c) bronze was rebuilt after the promote, which "
          "destroyed the proof while leaving the files; (d) every row of this batch "
          "names NO file, in which case a DROPPED line above counted them and the "
          "query below comes back NON-ZERO -- rows exist, no delete target does. Only "
          f"(a) needs nothing done: check with SELECT count(*) FROM {bronze} WHERE "
          f"_batch_id = '{batch_id}'")


def _report_held_back(held: tuple[FileAccount, ...], batch_id: str) -> None:
    """The files the repaired proof refused, with the counts that refused them.

    NEW IN F4 AND THE POINT OF THE REPAIR. The old proof asked "does bronze hold a
    row of this file", so it had no such category to report: every file it named
    was deleted. This one can hold a file back, and a control that declines
    silently is one nobody can tell from a control that never ran. Each line is
    also directly checkable against `dataops_reconciliation_by_file`, which runs
    the same predicate."""
    for account in held:
        print(f"  HELD BACK (left untouched): {account.source_file} -- staged="
              f"{account.staged} promoted={account.promoted} "
              f"quarantined={account.quarantined} unaccounted={account.unaccounted}. "
              "Not every row of this file is accounted for, so bronze holding some of "
              "them does not prove the file may go. Read it in "
              "workspace.default.dataops_reconciliation_by_file WHERE batch_id = "
              f"'{batch_id}'")


def _report(
    outcome: RetentionOutcome,
    scope: LandingScope,
    held: tuple[FileAccount, ...],
    *,
    batch_id: str,
    table: str,
    landing_dir: str,
) -> None:
    """What the run log says happened. Five outcomes, none of them collapsed.

    Modelled on `landing._discard_remote`, which reports deleted / already-absent
    / STILL THERE apart for the reason this task needs too: an operator told
    nothing assumes the file is gone, and a file that could not be removed still
    holds Volume quota nobody knows about. The fourth -- refused -- is this task's
    own and is the loudest, because it says bronze holds rows sourced from outside
    the dir this table's stream reads. The fifth -- held back -- says the file is
    this table's own and its rows do not add up."""
    print(f"reclaim_landing: batch={batch_id} table={table} "
          f"deleted={len(outcome.deleted)} already_absent={len(outcome.absent)} "
          f"failed={len(outcome.failed)} refused={len(scope.outside)} "
          f"held_back={len(held)}")
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
    _report_held_back(held, batch_id)
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
