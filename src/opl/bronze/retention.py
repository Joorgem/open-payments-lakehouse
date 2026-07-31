"""Reclaim landed inner files once bronze proves it holds their rows.

WHY THIS EXISTS: after F1.3, 16,743,815,717 B of consumed inner CSVs sat in the
Volume (part 0 alone is 6,780,467,695 B) beside the 5.26 GB of zips they came
from. The Auto Loader checkpoint holds their paths, so deleting them un-ingests
nothing.

WHY THE ZIPS STAY: F1.3's incidents 3 and 4 were PARSING defects found AFTER
ingestion, and fixing them required re-reading the source. The zip preserves that
capability at roughly a third of the bytes; a consumed CSV preserves nothing.

WHY THE UNIT IS THE FILE AND NOT THE DIRECTORY: F1.3 ingests incrementally --
several batches per month -- so deleting a table's landing dir on one batch's
promote would destroy parts that are landed but not yet ingested.

WHY THE PROOF SET IS STILL NOT TRUSTED AS A DELETE LIST: `files_of_batch` reads
`_source_file` out of a Delta table, and that column was written by a stream, not
by a human -- so it names whatever that stream discovered. F1.3 proved that a
stream discovers more than intended: a probe.txt planted in
`zips/estabelecimentos/` was ingested by a stream reading the month root, because
cloudFiles walks a source dir RECURSIVELY. Rows written that way carry a
`_source_file` under `zips/`, and handing one to `delete_files` would destroy the
archive this module's second paragraph exists to keep. `scope_to_landing_dir` is
what makes "bronze proves it" and "it is this table's landed file" two separate
conditions, both required.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from opl.bronze.autoloader import SOURCE_FILE_COLUMN
from opl.bronze.promote import BATCH_COLUMN

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


def files_of_batch(spark: SparkSession, bronze_table: str, batch_id: str) -> list[str]:
    """The distinct landed files BRONZE holds rows of, for this batch.

    Bronze and not staging, on purpose: staging holds rows that have been read but
    not yet promoted, and a file whose rows are only in staging has not been
    proven persisted. Deleting it would be unrecoverable without going back to the
    zip. This query is the whole safety argument -- what it returns is exactly
    what may be removed.

    Empty for a table that does not exist, matching `promote.rows_of_batch`'s
    "0 if it does not exist": the safe action is the same either way (delete
    nothing). What the emptiness MEANS differs, and telling those causes apart is
    the calling job task's job, not this function's -- see reclaim_landing.py."""
    if not spark.catalog.tableExists(bronze_table):
        return []
    rows = (
        spark.read.table(bronze_table)
        .filter(F.col(BATCH_COLUMN) == batch_id)
        .select(SOURCE_FILE_COLUMN)
        .distinct()
        .collect()
    )
    return sorted(row[SOURCE_FILE_COLUMN] for row in rows if row[SOURCE_FILE_COLUMN])


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
