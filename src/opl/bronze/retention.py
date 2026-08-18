"""Reclaim landed inner files once every row they staged is accounted for.

WHY THIS EXISTS: after F1.3, 16,743,815,717 B of consumed inner CSVs sat in the
Volume (part 0 alone is 6,780,467,695 B) beside the 5.26 GB of zips they came
from. The Auto Loader checkpoint holds their paths, so deleting them un-ingests
nothing.

WHY THE ZIPS STAY: F1.3's incidents 3 and 4 were PARSING defects found AFTER
ingestion, and fixing them required re-reading the source. The zip preserves that
capability at roughly a third of the bytes; a consumed CSV preserves nothing.

AND WHERE THERE IS NO ZIP, NOTHING MAY BE RECLAIMED AT ALL. The paragraph above
is the whole safety argument of this module, and it is a statement about the
`zips`-landed tables only: a `local`-landed table's zip never reaches the Volume
(`scripts/extract_cnpj.py` unzips on the extraction host and PUTs only the inner
file), so its landed file is the single copy in the workspace and "recovery" means
re-downloading a monthly snapshot the RFB may have rotated. These functions do not
know a table's landing mode and deliberately decide nothing -- the refusal lives at
the boundary, in `databricks/src/reclaim_landing.py`, beside the argument guards.
Until the F1.4a review it lived nowhere, and this docstring's promise was
load-bearing for a table it did not hold for.

WHY THE UNIT IS THE FILE AND NOT THE DIRECTORY: F1.3 ingests incrementally --
several batches per month -- so deleting a table's landing dir on one batch's
promote would destroy parts that are landed but not yet ingested.

WHY THE PROOF SET IS STILL NOT TRUSTED AS A DELETE LIST: `file_accounts_of_batch`
reads `_source_file` out of a Delta table, and that column was written by a stream,
not by a human -- so it names whatever that stream discovered. F1.3 proved that a
stream discovers more than intended: a probe.txt planted in
`zips/estabelecimentos/` was ingested by a stream reading the month root, because
cloudFiles walks a source dir RECURSIVELY. Rows written that way carry a
`_source_file` under `zips/`, and handing one to `delete_files` would destroy the
archive this module's second paragraph exists to keep. `scope_to_landing_dir` is
what makes "the rows are accounted for" and "it is this table's landed file" two
separate conditions, both required.

WHY THE PROOF IS NO LONGER "BRONZE HOLDS A ROW OF THIS FILE" (F4). That was
`files_of_batch`, whose docstring called its query "the whole safety argument --
what it returns is exactly what may be removed". It is airtight only under an
all-or-nothing gate, where bronze holding *a* row of a file implies bronze holds
*every* row of it. `repromote_triaged_batch` breaks the equivalence by design:
`promote_batch` re-applies the DQ rules and appends only the passing rows, so a file
with one rejected row has its clean rows in bronze and its rejected row only in
quarantine -- measured, socios' 3,583 rejects span 20 distinct `_source_file` values.
Wiring the reclaim into that path (ADR 0006, "the fix is to give the triage path the
reclaim it lacks") is what made the gap reachable, so the proof had to be repaired in
the same change that reached it.

The repaired proof is `reconcile.RECLAIMABLE_SQL`: a file may go when every row it
staged is accounted for -- `promoted + quarantined = staged` at FILE grain -- AND at
least one of them is in bronze. It IMPLIES the old condition (`promoted > 0` is its
first conjunct) and refuses cases the old one passed, so the delete set can only
shrink. `files_of_batch` is deleted rather than deprecated: a weaker proof left in
the module is a weaker proof somebody calls.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from opl.bronze.promote import BATCH_COLUMN
from opl.bronze.reconcile import file_accounts_sql
from opl.bronze.registry import BronzeTable
from opl.bronze.snapshot import SNAPSHOT_MONTH_COLUMN
from opl.config import DEFAULT, OplConfig

# `_source_file` is `_metadata.file_path`, i.e. a URI, and this workspace uses both
# spellings of the same Volume object: F1.3's evidence records that `databricks fs`
# needs `dbfs:/Volumes/...` where the cluster's FUSE mount is a bare `/Volumes/...`.
# Only the bare form can be unlinked, and the difference is INVISIBLE if left
# unhandled -- `Path("dbfs:/Volumes/...")` is a relative path that exists nowhere, so
# every unlink would raise FileNotFoundError, every file would be reported ALREADY
# ABSENT, and a reclaim that freed no bytes would look exactly like an idempotent
# re-run of one that had.
_DBFS_SCHEME = "dbfs:"
_PARENT = ".."
# Both separators, not `os.sep`, for `registry._assert_subdirs_are_single_path_
# components`'s reason: this repo is developed on Windows and runs on Databricks
# Linux, so a backslash that looks inert where it is written is a real separator
# where it runs -- and here the two halves disagree, which is worse than either.
# See `scope_to_landing_dir` for what that costs.
_BACKSLASH = "\\"


@dataclass(frozen=True)
class RetentionOutcome:
    """What a reclaim actually did. Three outcomes kept apart, deliberately.

    Same shape as `landing._discard_remote`: an operator told nothing assumes the
    file is gone, and a file that could not be removed still holds Volume quota.
    A boolean would collapse "nothing to do" and "could not do it"."""

    deleted: tuple[str, ...]
    absent: tuple[str, ...]
    failed: tuple[tuple[str, str], ...]  # (path, reason)


@dataclass(frozen=True)
class LandingScope:
    """The proof set split into what may be deleted and what may not.

    A FOURTH outcome, kept out of `RetentionOutcome` on purpose: those three
    describe what a delete DID, and a refused path was never offered to one. It is
    also a different kind of event -- a failed unlink is an infrastructure problem
    an operator retries, while a path outside the landing dir means bronze holds
    rows sourced from somewhere this table's stream should never have read, which
    is a pipeline defect to investigate before anything else is reclaimed."""

    # Resolved to the FUSE form, because that is the string that gets unlinked and
    # therefore the string the log must show.
    inside: tuple[str, ...]
    # The ORIGINAL `_source_file` values, unresolved: this is the evidence an
    # operator takes back to the bronze table, so it must match what is stored there.
    outside: tuple[str, ...]


@dataclass(frozen=True)
class FileAccount:
    """One landed file's three counts for one batch, and the verdict over them.

    A row of `dataops_reconciliation_by_file` narrowed to one table and one batch.
    The counts travel WITH the verdict, and not because a caller might want them:
    a file held back is the loudest thing this module can report, and "held back"
    without `staged=2 promoted=1 quarantined=0` says a control fired without
    saying what it saw -- which is the shape of every guard this repository has
    had to go back and prove could fail."""

    source_file: str
    staged: int
    promoted: int
    quarantined: int
    reclaimable: bool

    @property
    def unaccounted(self) -> int:
        """Rows of this file that reached neither bronze nor quarantine.

        Negative means bronze and quarantine together hold MORE than staging ever
        did -- `reconcile`'s `over_promoted`, which is also `reclaimable = false`."""
        return self.staged - self.promoted - self.quarantined


def _absent_roles(spark: SparkSession, spec: BronzeTable, config: OplConfig) -> tuple[str, ...]:
    """The roles whose tables do NOT exist, for `file_accounts_sql`'s `skip`.

    Asked here rather than handled by a `try` around the query: an
    AnalysisException naming a missing table cannot be told apart from one naming
    a missing COLUMN, and the second is a schema drift a reclaim must not swallow."""
    return tuple(
        role
        for role, table in (
            ("staging", spec.staging),
            ("bronze", spec.bronze),
            ("quarantine", spec.quarantine),
        )
        if not spark.catalog.tableExists(config.table(table))
    )


def file_accounts_of_batch(
    spark: SparkSession,
    spec: BronzeTable,
    batch_id: str,
    *,
    config: OplConfig = DEFAULT,
) -> tuple[FileAccount, ...]:
    """Every landed file this batch touched, with the repaired proof over each.

    THE WHOLE SAFETY ARGUMENT, and unlike the query it replaces it is stated over
    all three tables: what may be removed is a file whose every staged row is
    accounted for -- in bronze or in quarantine -- and at least one of them in
    bronze. `reconcile.RECLAIMABLE_SQL` is that predicate, spelled once and shared
    with the view an operator reads, so a dashboard saying `reclaimable = true`
    and a task unlinking the file are the same sentence.

    Returns EVERY file, not just the admissible ones, so the caller can report the
    ones it held back with the counts that held them. Empty when the batch touched
    nothing -- the same answer, and the same delete-nothing action, as a table that
    does not exist; which of those happened is the job task's to tell apart.

    A NULL `_source_file` is dropped: it names no path, so it can neither be
    unlinked nor be evidence about one."""
    accounts = spark.sql(
        file_accounts_sql(spec, config, skip=_absent_roles(spark, spec, config)),
        args={"batch_id": batch_id},
    ).collect()
    return tuple(
        sorted(
            (
                FileAccount(
                    row["source_file"],
                    row["staged"],
                    row["promoted"],
                    row["quarantined"],
                    bool(row["reclaimable"]),
                )
                for row in accounts
                if row["source_file"]
            ),
            key=lambda account: account.source_file,
        )
    )


def months_of_batch(spark: SparkSession, staging_table: str, batch_id: str) -> tuple[str, ...]:
    """The distinct `_snapshot_month` values STAGING stamped on this batch.

    THE OTHER HALF OF THE DELETE BOUNDARY, taken from the data instead of retyped.
    `landing_table(subdir, month)` is the only directory deletes are confined to,
    and in a repromote -- hours or days after the ingest -- nobody has the ingest
    run's month in hand. `_snapshot_month` is not read off the file path: the
    ingest stamped it from its own `{{job.parameters.month}}`, already through
    `require_month`, which is what keeps the boundary independent of the
    `_source_file` values it is used to judge.

    Staging and not bronze, because staging is where the proof's denominator comes
    from: a batch whose staged rows are gone reclaims nothing anyway, so the month
    and the proof become unavailable together rather than one outliving the other.

    A TUPLE AND NOT A STRING, because the caller has to see "not exactly one".
    Empty for a missing table or a staging shape with no such column (the
    pre-F1.4a rebuild leaves those), more than one for a batch that somehow spans
    months -- and both are refusals, made by the caller, which is where the
    fallback to `DEFAULT.month` would otherwise be written."""
    if not spark.catalog.tableExists(staging_table):
        return ()
    staged = spark.read.table(staging_table)
    if SNAPSHOT_MONTH_COLUMN not in staged.columns:
        return ()
    rows = (
        staged.filter(F.col(BATCH_COLUMN) == batch_id)
        .select(SNAPSHOT_MONTH_COLUMN)
        .distinct()
        .collect()
    )
    return tuple(sorted(row[SNAPSHOT_MONTH_COLUMN] for row in rows if row[SNAPSHOT_MONTH_COLUMN]))


def fuse_path(source_file: str) -> str:
    """The filesystem path a `_source_file` URI names, as the cluster sees it.

    Handles the ONE scheme this Volume produces (`dbfs:`) and leaves everything
    else exactly as it came. That is not laziness, it is the fail-closed half of
    the design: an unrecognised form is passed through unchanged, fails the
    containment check in `scope_to_landing_dir`, and is REPORTED -- where a
    best-effort rewrite would have manufactured a plausible path and unlinked it."""
    if not source_file.startswith(_DBFS_SCHEME):
        return source_file
    # `dbfs:/Volumes/...` and `dbfs:///Volumes/...` name the same object; collapse
    # the leading slashes so one comparison covers both.
    return "/" + source_file[len(_DBFS_SCHEME):].lstrip("/")


def scope_to_landing_dir(paths: Iterable[str], landing_dir: str) -> LandingScope:
    """Split the proof set into files under `landing_dir` and everything else.

    `landing_dir` is ONE table's landing subdir for ONE month -- the exact
    directory that table's Auto Loader read. That is the narrowest boundary that
    still contains every file this reclaim legitimately targets, and it excludes,
    by construction: the sibling `zips/<table>` dir (the only way back to the
    source), every other table's subdir, the `_checkpoints`/`_schemas` state
    dirs, and an earlier month's files, which are landed and un-reclaimed but are
    not what this batch proved.

    PurePosixPath and not Path: a UC Volume is a POSIX FUSE mount on Databricks
    Linux, while this repo is developed on Windows, where `Path` treats `\\` as a
    separator too and reads `/Volumes/...` as drive-relative. A containment check
    whose answer depends on the developer's OS is not a guard.

    A `..` component is refused rather than normalised. `PurePosixPath` does not
    collapse it (it cannot -- that needs the filesystem), so `is_relative_to`
    would answer about a path that is not the one to be unlinked.

    A BACKSLASH ANYWHERE is refused for the sharper form of the same problem:
    this check judges by POSIX grammar and `delete_files` executes through
    `pathlib.Path`, which on Windows reads `\\` as a separator. `.../estabelecimentos
    /..\\..\\zips\\x.zip` is ONE component with no `..` part to POSIX -- it passes
    both checks above and reads as INSIDE -- while the executor traverses two
    levels out of the landing dir and into the zips. Unreachable today (a POSIX
    FUSE mount never yields `\\`) and refused anyway: a containment check that
    disagrees with its own executor is a bypass waiting for the reachability to
    change, which is exactly why the registry refuses both separators too."""
    root = PurePosixPath(landing_dir)
    inside: list[str] = []
    outside: list[str] = []
    for path in paths:
        resolved = fuse_path(path)
        candidate = PurePosixPath(resolved)
        if (
            _BACKSLASH in resolved
            or _PARENT in candidate.parts
            or not candidate.is_relative_to(root)
        ):
            outside.append(path)
        else:
            inside.append(resolved)
    return LandingScope(tuple(inside), tuple(outside))


def delete_files(paths: Iterable[str]) -> RetentionOutcome:
    """Remove each path, reporting the three outcomes apart. Never raises.

    Never raises because this runs AFTER a successful promote: the rows are in
    bronze either way, so a file that cannot be removed is a quota problem, not a
    data problem, and must not turn a green ingestion red. Never silent either --
    a leak that is cleaned quietly is a leak nobody fixes.

    An already-absent file is `absent`, not an error: a Databricks repair run
    re-executes this task with the same {{job.run_id}}, so a second pass over an
    already-reclaimed batch is the expected case, not an exception to it.

    Deletes exactly what it is given and decides nothing: which paths are eligible
    is `scope_to_landing_dir`'s call, made against the table's landing dir, and
    keeping that decision out of here is what stops it from being defaulted."""
    deleted: list[str] = []
    absent: list[str] = []
    failed: list[tuple[str, str]] = []
    for path in paths:
        try:
            Path(path).unlink()
        except FileNotFoundError:
            absent.append(path)
        except OSError as exc:
            failed.append((path, str(exc)))
        else:
            deleted.append(path)
    return RetentionOutcome(tuple(deleted), tuple(absent), tuple(failed))
