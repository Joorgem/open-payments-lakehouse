# scripts/migrate_lookups_to_subdir.py
"""One-off: move the six lookup CSVs into their own landing subdir, and clear the
Auto Loader state that points at the old path.

WHY A RELOAD AND NOT JUST A MOVE: to Auto Loader a new path is a new file, so the
move forces a second ingest. The lookup promote used to OVERWRITE bronze from the
WHOLE staging table, so a second batch in staging would have written 2x the rows;
the promote is now a scoped append, which fixes the mechanism only if bronze
starts clean. So staging, quarantine and bronze are all dropped and rebuilt from
one batch. It costs nothing -- a few thousand rows -- and it buys a lookup bronze
that is append-only and single-batch from its first commit, so "did we duplicate?"
never needs asking.

THIS SCRIPT DOES THE FILE AND STATE MOVES ONLY. The reload is left to a real run
of the real ingestion job, on purpose: running the actual flow is what makes it
the acceptance test for the whole refactor -- registry, parameterised ingest,
single gate, single promote, new layout, both snapshot columns -- rather than a
script pretending to be one. A script that reproduced the ingest would prove only
that the script works. So the last thing this does is print the remaining steps,
in order, for a human.

RUNS OFF DATABRICKS, through the Files API (two-layer topology, ADR 0002). It
needs no Spark session and touches no table.

usage: migrate_lookups_to_subdir.py <month>
"""
from __future__ import annotations

import hashlib
import sys
import tempfile
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from opl.bronze.autoloader import checkpoint_location, schema_location
from opl.bronze.lookup_routing import LOOKUP_SUFFIX, lookup_type_from_filename
from opl.bronze.registry import BronzeTable, table_spec
from opl.config import DEFAULT, require_month
from opl.extraction.landing import upload_client, upload_to_volume

USAGE = "usage: migrate_lookups_to_subdir.py <month>   (e.g. 2026-06)"

# The landed lookup filenames end in `<CODE>CSV` (`F.K03200$Z.D60613.CNAECSV`).
# DERIVED from the routing map rather than restated as six literals, because the
# two have to name the same six files: a literal list here that drifted from
# `LOOKUP_SUFFIX` would move a file the router does not recognise, and that file's
# rows would land in bronze with `lookup_type` NULL -- a successful run that
# quietly lost a whole lookup.
LOOKUP_FILE_SUFFIXES: tuple[str, ...] = tuple(sorted(f"{code}CSV" for code in LOOKUP_SUFFIX))

_HASH_CHUNK = 1 << 20


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(_HASH_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def suffix_of(path: str) -> str | None:
    """Which lookup suffix `path` ends with, or None if it is not a lookup CSV."""
    for suffix in LOOKUP_FILE_SUFFIXES:
        if path.endswith(suffix):
            return suffix
    return None


def lookup_files(w: WorkspaceClient, month_dir: str) -> dict[str, str]:
    """The lookup CSVs sitting directly in the month root, exactly one per suffix.

    Matched by their known suffixes rather than by "every file in here": the month
    root is precisely the shared directory this migration exists to stop using, so
    a blanket match would sweep up whatever else is there -- and F1.3 proved
    something else IS there, when a probe.txt planted under this root's `zips/`
    subdir was ingested by the stream reading the root.

    ONE FILE PER SUFFIX, not merely six files. A count of six is satisfied by two
    CNAECSVs and no PAISCSV, which is a set that looks complete and would reload a
    lookup table missing an entire `lookup_type`. Refusing anything it does not
    recognise is the point: a partial migration is the one outcome with no clean
    retry, because the re-run can no longer see the set it was supposed to move.

    Only files DIRECTLY in `month_dir` are considered -- `list_directory_contents`
    is not recursive, and the per-table subdirs beneath it are skipped explicitly.
    """
    found: dict[str, list[str]] = {suffix: [] for suffix in LOOKUP_FILE_SUFFIXES}
    for entry in w.files.list_directory_contents(month_dir):
        if entry.is_directory or not entry.path:
            continue
        suffix = suffix_of(entry.path)
        if suffix is not None:
            found[suffix].append(entry.path)
    wrong = {suffix: paths for suffix, paths in found.items() if len(paths) != 1}
    if wrong:
        raise RuntimeError(
            f"refusing to migrate: {month_dir} does not hold exactly one file per lookup "
            f"suffix. Expected one each of {', '.join(LOOKUP_FILE_SUFFIXES)}; these are "
            f"wrong: { {k: v for k, v in sorted(wrong.items())} }. Nothing was moved and "
            "no state was cleared. A suffix with zero files usually means the move ALREADY "
            "RAN (check the destination subdir); a suffix with two means an extra copy is "
            "in the month root and a human has to decide which one is the snapshot."
        )
    return {suffix: paths[0] for suffix, paths in sorted(found.items())}


def purge_state_dir(w: WorkspaceClient, path: str) -> int | None:
    """Delete `path` and everything under it. None if it was not there at all.

    RECURSIVE BY HAND because the Files API has no recursive delete.
    `w.files.delete_directory(directory_path)` takes exactly one argument in
    databricks-sdk 0.123.0 -- there is no `recursive` parameter -- and its own
    docstring says it "Deletes an empty directory. To delete a non-empty
    directory, first delete all of its contents." A checkpoint directory is never
    empty, so a single call cannot do this.

    That mattered more than a missing kwarg usually does: passing `recursive=True`
    raises TypeError, and a TypeError swallowed by a blanket `except Exception`
    that prints "could not clear ..." reads exactly like the benign
    already-absent case. The operator would go on to the reload with the old
    path's checkpoint still in place. So the failure modes are kept apart here:
    absent is fine and returns None; anything else propagates.

    Post-order -- contents, then the directory -- because that is the only order
    `delete_directory` accepts. Depth is bounded by the checkpoint layout, which
    is a handful of levels.
    """
    try:
        entries = list(w.files.list_directory_contents(path))
    except NotFound:
        return None
    removed = 0
    for entry in entries:
        if entry.is_directory:
            removed += purge_state_dir(w, entry.path) or 0
        else:
            w.files.delete(entry.path)
            removed += 1
    w.files.delete_directory(path)
    return removed


def clear_state(w: WorkspaceClient, spec: BronzeTable) -> None:
    """Remove this table's Auto Loader checkpoint and inferred schema.

    BOTH, because both reference the OLD path: the checkpoint holds those paths as
    consumed, and the schema was inferred under that location. Clearing them is
    what makes the reload a clean FIRST ingest rather than an Auto Loader carrying
    opinions about a directory that no longer holds anything.

    Called TWICE by `main` -- before the moves and after them. It is idempotent
    (`purge_state_dir` returns None for a directory that is not there), which is
    what makes the second call cost nothing; see the comment at the second call
    site for the window it closes.
    """
    for state in (
        checkpoint_location(DEFAULT, spec.table_key),
        schema_location(DEFAULT, spec.table_key),
    ):
        removed = purge_state_dir(w, state)
        if removed is None:
            print(f"migrate: {state} was already absent")
        else:
            print(f"migrate: cleared {state} ({removed} object(s))")


def move_verified(w: WorkspaceClient, src: str, dest_dir: str, workdir: Path) -> tuple[str, int]:
    """Copy `src` into `dest_dir`, PROVE the copy, and only then delete the original.

    WHAT THE PROOF IS. Three checks, in order:
      1. the downloaded local file's size equals the remote source's
         `content_length` -- the download was not short;
      2. `upload_to_volume` compares the landed object's `content_length` against
         that same local file -- the upload was not short. That check exists
         because a single PUT of 341,333,959 bytes once stored 273,373,127 and
         returned no error; see its docstring;
      3. the landed object is downloaded BACK and its SHA-256 compared with the
         local file's -- the landed BYTES are the bytes this process read.

    Check 3 is what makes the delete safe, and SIZE EQUALITY ALONE WOULD NOT BE.
    Size equality proves an object is not truncated and proves nothing about its
    content: this repo already leans on size-alone equality in
    `unzip_volume`'s idempotence skip, which leaves a right-sized file of
    different content untouched (pinned by
    `test_skips_already_extracted_with_matching_size`). Under ADR 0007's
    multipart path the ways to reach a right-length wrong-content object
    multiply -- every part is its own retried request against its own presigned
    URL, and `upload_to_volume` documents that it ASSUMES EXCLUSIVE OWNERSHIP of
    its target, so a second concurrent writer to the same path can leave an
    object of exactly the right length holding neither writer's file.

    WHAT CHECK 3 STILL DOES NOT PROVE: that the source object was itself intact
    before this ran (nothing here has a manifest to compare it against -- the
    WebDAV byte count that vouched for it was checked at landing time), and that
    nothing rewrote either object after the read-back. It is a proof about this
    copy, not about the snapshot.

    DO NOT GENERALISE THIS to the Estabelecimentos extracts. It is affordable
    only because the six lookups total a few hundred KB; the same three passes
    over 16.7 GB of consumed CSVs is not a verification, it is an outage.
    """
    name = src.rsplit("/", 1)[-1]
    original = w.files.get_metadata(src).content_length
    local = workdir / name
    w.files.download_to(src, str(local))
    downloaded = local.stat().st_size
    if original is None or downloaded != original:
        raise RuntimeError(
            f"{src}: downloaded {downloaded} B but the source object reports {original} B. "
            "Refusing to upload a copy that is already wrong -- nothing was written to the "
            "destination and the original is untouched."
        )
    target = upload_to_volume(w, local, dest_dir)
    readback = workdir / f"{name}.landed"
    w.files.download_to(target, str(readback))
    if _sha256(readback) != _sha256(local):
        raise RuntimeError(
            f"{target}: the landed object's SHA-256 does not match the {downloaded} B copy "
            f"read from {src}, though both are {downloaded} B. REFUSING TO DELETE THE "
            "ORIGINAL -- it is still in the month root and is still the snapshot. The "
            "landed object is the suspect one; remove it by hand, and do not re-run until "
            "you know what else was writing to that path."
        )
    w.files.delete(src)
    return target, downloaded


def _print_next_steps(spec: BronzeTable, month: str, files: dict[str, str]) -> None:
    """The remaining steps, in order, for a human. See the module docstring for
    why the reload is not scripted."""
    bronze = DEFAULT.table(spec.bronze)
    types = sorted(lookup_type_from_filename(path) for path in files.values())
    print("\nNEXT, BY HAND, IN THIS ORDER:\n")
    print("  0. CAPTURE THE PRE-MIGRATION STATE -- before step 2 drops it. This is the")
    print("     only reconciliation reference derived from THIS workspace, and the drop")
    print("     destroys it:")
    print(f"       SELECT lookup_type, count(*) FROM {bronze} GROUP BY 1 ORDER BY 1;")
    print(f"       SELECT count(*), count(DISTINCT _batch_id) FROM {bronze};")
    print("     Expect the pre-state to hold MORE than these six files' rows, and expect")
    print("     the reload to be SMALLER: the old lookup stream read the month root")
    print("     recursively and the old promote overwrote bronze from the WHOLE staging")
    print("     table, so rows sourced from outside the six files can be in there (the")
    print("     F1.3 probe.txt under zips/estabelecimentos/ is the known one). A row with")
    print("     a NULL lookup_type, or a _source_file not among the six, is that defect --")
    print("     its absence afterwards is the fix, not a loss.\n")
    print("  1. DEPLOY THIS BRANCH FIRST:")
    print("       cd databricks && databricks bundle deploy -t free")
    print("     The reload is only the acceptance test if the job runs THIS code. Until")
    print("     this deploy, the wheel on the workspace is the OLD one, whose lookup")
    print("     stream reads the month ROOT and reads it RECURSIVELY (F1.3: a probe.txt")
    print("     planted in zips/estabelecimentos/ was ingested by a stream on the root).")
    print("     The subdir the move just created is UNDER that root, so an old-wheel run")
    print("     would not ingest nothing -- it would re-ingest all six files from their")
    print("     new location, with pre-snapshot-column code and the old whole-staging")
    print("     promote, and would recreate the checkpoint this script just cleared.\n")
    print(f"  2. DROP TABLE IF EXISTS {DEFAULT.table(spec.staging)};")
    print(f"     DROP TABLE IF EXISTS {DEFAULT.table(spec.quarantine)};")
    print(f"     DROP TABLE IF EXISTS {bronze};\n")
    print(f"  3. databricks bundle run bronze_cnpj_lookup -t free --params month={month}")
    print("     Expect it green through ingest -> dq_gate_batch -> check_bad_rows(true)")
    print("     -> promote.\n")
    _print_reconciliation(bronze, types)


def _print_reconciliation(bronze: str, types: list[str]) -> None:
    """Step 4: what to compare, and where the comparison comes from.

    NO TARGET ROW COUNT IS PRINTED, deliberately. `docs/f1.2-bronze-run-evidence.md`
    does record one, per `lookup_type` and in total, but a figure copied into this
    script is a second spelling of that doc's that nothing keeps in step -- and the
    caveats that make it the right number (it is the DQ-GOOD count of these same
    six files, under a lookup rule set unchanged since) live next to it in the doc,
    where they will be read with it. Step 0's capture is the reference this script
    can actually vouch for, because the operator takes it minutes earlier off the
    same workspace.

    Per `lookup_type` and not just a total, because a total is one opaque delta
    while a breakdown says WHICH lookup moved -- and the file/type mapping printed
    below is derived from the filenames this run actually moved.
    """
    print("  4. RECONCILE:")
    print(f"       SELECT lookup_type, count(*) FROM {bronze} GROUP BY 1 ORDER BY 1;")
    print("       SELECT count(*) rows, count(DISTINCT lookup_type) types,")
    print("              count(DISTINCT _batch_id) batches, min(_snapshot_ref_date) min_ref,")
    print("              max(_snapshot_ref_date) max_ref,")
    print("              count(*) - count(_snapshot_ref_date) null_ref_date")
    print(f"       FROM {bronze};")
    print("     Derived from what this run moved -- these this script does assert:")
    print(f"       types   = {len(types)}  -> {types}")
    print("       batches = 1  -> one run ingested everything. The old promote wrote from")
    print("                       whole staging, so 2 batches meant 2x the rows.")
    print("       null_ref_date = 0, and min_ref = max_ref = the date in the filenames'")
    print("                       .D<Ymmdd> token (both months observed carry exactly one).")
    print("     ROW COUNT: compare per lookup_type against step 0's capture first, then")
    print("     independently against the per-lookup_type table under 'Promoted-table")
    print("     verification' in docs/f1.2-bronze-run-evidence.md. Read the number there,")
    print("     with its caveats -- this script does not restate it. If they disagree,")
    print("     STOP and reconcile before writing any evidence doc.")


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        raise ValueError(USAGE)
    # NO DEFAULT MONTH. `require_month`'s docstring names four entry points that
    # each wrote `month or DEFAULT.month` and so satisfied a guard with the single
    # value it was built to refuse; this script would have been the fifth, and the
    # worst of them: the month picks BOTH the directory read from and the directory
    # written to, and the pinned value equals the job YAMLs' own default, so a
    # substituted one migrates whichever month that happens to be with nothing in
    # the log naming it.
    month = require_month(args[0], action="migrate the lookup files")
    spec = table_spec("lookup")
    w = upload_client()
    month_dir = DEFAULT.landing_cnpj_month(month)
    dest_dir = DEFAULT.landing_table(spec.subdir, month)
    files = lookup_files(w, month_dir)
    print(f"migrate: {len(files)} lookup file(s) in {month_dir} -> {dest_dir}")

    # STATE FIRST, FILES SECOND -- the opposite of the obvious order, on purpose.
    # A failure clearing the checkpoint then leaves NOTHING moved, so the whole
    # script is a clean retry. The other order leaves six moved files and a stale
    # checkpoint, and the re-run refuses because the month root no longer holds
    # the set (`lookup_files` above).
    clear_state(w, spec)

    total = 0
    with tempfile.TemporaryDirectory(prefix="opl-lookup-migrate-") as tmp:
        workdir = Path(tmp)
        for suffix, src in files.items():
            target, size = move_verified(w, src, dest_dir, workdir)
            print(f"migrate: {suffix} {src} -> {target} ({size} B, "
                  f"lookup_type={lookup_type_from_filename(target)})")
            total += size
    print(f"migrate: {len(files)} file(s), {total} B, verified and now under {dest_dir}")

    # CLEARED AGAIN, and this is not belt-and-braces -- it closes a window the
    # first call cannot.
    #
    # The tempting claim is that the window between the two is empty because
    # "every stream reads its own subdir now". That is true of THIS BRANCH and
    # false of the WORKSPACE at this moment: the deployed wheel is still the old
    # one, because the deploy is step 1 of the procedure printed below, i.e. after
    # this script. The old lookup stream reads the month ROOT, RECURSIVELY -- F1.3
    # proved the recursion when a probe.txt planted in zips/estabelecimentos/ was
    # ingested by a stream on the root -- and the new subdir is UNDER that root.
    # So an old-wheel run landing between the clear and the moves would find the
    # six files at their new paths, ingest them, and RECREATE the checkpoint at
    # this very location, silently restoring the state the first call removed and
    # costing the clean-first-ingest guarantee. It would surface only at
    # reconcile, as a row count nobody could explain.
    #
    # Reachability, stated honestly: no job is scheduled on this workspace, so it
    # takes a concurrent manual run. This is cheap (`purge_state_dir` is
    # idempotent and returns None for an absent directory) and the alternative is
    # a guarantee that rests on nobody doing anything, so it is closed rather than
    # documented.
    clear_state(w, spec)
    _print_next_steps(spec, month, files)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
