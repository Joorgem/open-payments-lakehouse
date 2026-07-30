"""Unzip RFB part zips already staged inside a UC Volume into the table landing
subdir, ON Databricks (serverless, over ``/Volumes/...`` FUSE paths).

WHY this exists: the giants are uploaded as their compressed ZIPs into a
``zips`` subdir and unzipped in place on the cluster into the table landing
subdir -- the two-layer control-plane/data-plane topology of ADR 0002/0004.

The reason has changed, and the old one should not be repeated. This was
originally forced by a 5 GiB single-PUT ceiling, which Estabelecimentos part 0's
inner CSV (6,780,467,695 B / 6.78 GB, measured) exceeded. That ceiling was never
a Files API property -- it belonged to the ``databricks-sdk`` 0.40 pin then in
force -- and since ADR 0007 adopted the multipart upload path it does not exist
at all: a 6.78 GB object would upload fine today.

What keeps the design is arithmetic, not a limit. Part 0's ZIP is
2,128,818,559 B against 6,780,467,695 B unzipped, so uploading compressed moves
under a third of the bytes over a link measured at ~67 MB/min -- roughly 53 min
saved on that part alone -- and the unzip runs on the cluster where the bytes
already are. It also keeps the Volume from holding both copies of every giant.

The logic is pure ``zipfile`` over directories: no Spark/Java, unit-tested
locally with tmp dirs. Idempotent -- a zip whose inner file already exists at
the expected uncompressed size is skipped, so re-runs are safe; a member that
failed to extract leaves nothing behind in the landing dir and is re-extracted
next time.

Nothing half-written is allowed to EXIST in ``dest_dir``, because that dir is what
the Estabelecimentos Auto Loader reads with no ``pathGlobFilter``: an orphaned
``.tmp`` would be discovered and ingested as if it were a complete CSV. Hence the
temporary is written to a caller-supplied ``tmp_dir`` outside every watched dir
and only ``os.replace``d in when it is whole -- cleanup after a failure is a
best effort (an ``unlink`` can itself fail on a FUSE path), and a best effort is
not a guarantee, so the guarantee cannot rest on it.

Defense in depth against a bad input object: a zip that is missing bytes yet
still carries its original tail (see ``_reject_negative_header_offset``) is
rejected with ``CorruptZipError`` naming the archive, rather than blowing up
deep inside ``zipfile`` on an unexplained negative seek."""
from __future__ import annotations

import os
import shutil
import zipfile
from pathlib import Path

_COPY_CHUNK = 8 << 20  # 8 MiB -- stream members; never .read() a 6.78 GB CSV whole.


class CorruptZipError(ValueError):
    """A zip whose central directory points outside the archive it lives in."""


def unzip_dir(
    zips_dir: str | Path, dest_dir: str | Path, *, tmp_dir: str | Path
) -> list[Path]:
    """Unzip every ``*.zip`` in ``zips_dir`` (sorted by name) into ``dest_dir``.

    Each RFB zip contains exactly one inner file (``ValueError`` otherwise --
    same convention as ``opl.extraction.landing.unzip_single``). If the inner
    file already exists in ``dest_dir`` with size == the zip's recorded
    uncompressed size (``ZipInfo.file_size``), it is skipped untouched;
    otherwise it is streamed out to a ``.tmp`` name IN ``tmp_dir``, size-verified,
    and atomically renamed into ``dest_dir`` -- so no partial file is ever created
    there, and a failure has nothing there to clean up. Returns the dest paths
    (extracted and skipped alike) in processing order.

    ``tmp_dir`` IS REQUIRED, and required to be on the same filesystem as
    ``dest_dir`` (``os.replace`` across filesystems raises ``EXDEV``; a UC Volume
    FUSE path and the system temp dir are different filesystems). It has no
    default because this module cannot know which dirs the Auto Loaders read --
    that is the caller's knowledge, and getting it wrong reintroduces exactly the
    orphan this argument exists to prevent. On Databricks the caller takes it from
    ``OplConfig.landing_tmp``, which documents why that location is invisible to
    every stream. The ``.tmp`` name is deterministic per inner file, so a re-run
    truncates a previous run's orphan rather than accumulating beside it."""
    zips_dir = Path(zips_dir)
    dest_dir = Path(dest_dir)
    tmp_dir = Path(tmp_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    results: list[Path] = []
    # Fail-fast on purpose: a corrupt part aborts the batch rather than being
    # collected, so the operator re-uploads and re-runs instead of discovering
    # the next bad part only after a full pass over the good ones.
    for zip_path in sorted(zips_dir.glob("*.zip")):
        results.append(_unzip_one(zip_path, dest_dir, tmp_dir))
    return results


def _unzip_one(zip_path: Path, dest_dir: Path, tmp_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as z:
        members = [m for m in z.infolist() if not m.is_dir()]
        if len(members) != 1:
            raise ValueError(
                f"{zip_path.name}: expected 1 inner file, got {len(members)}"
            )
        info = members[0]
        dest = dest_dir / Path(info.filename).name

        if dest.exists() and dest.stat().st_size == info.file_size:
            return dest  # already extracted at the expected size -- skip untouched.

        _reject_negative_header_offset(zip_path, info)

        tmp = tmp_dir / (dest.name + ".tmp")
        # The .tmp goes in `tmp_dir`, NOT beside `dest`: `dest_dir` is the landing
        # subdir the Estabelecimentos Auto Loader reads with NO pathGlobFilter (only
        # the lookup stream filters `*CSV` -- see opl.bronze.autoloader), so a .tmp
        # there is a file that stream discovers and ingests as if it were a complete
        # 30-column CSV. Nor does the idempotence skip above catch it: that compares
        # the size of the FINAL name, which a .tmp never reaches. Writing it
        # elsewhere is what makes that unreachable; the cleanup below then only
        # tidies up, and is allowed to fail without costing the guarantee. It used to
        # be the guarantee, and an unlink that fails on a FUSE path (the handler
        # prints "STILL THERE") left an ingestible partial behind. Covers a mid-copy
        # failure (a stalled FUSE read, a damaged member `zipfile` only rejects at
        # its trailing CRC) as well as the size mismatch.
        try:
            with z.open(info) as src, open(tmp, "wb") as out:
                shutil.copyfileobj(src, out, _COPY_CHUNK)

            extracted_size = tmp.stat().st_size
            if extracted_size != info.file_size:
                raise ValueError(
                    f"{zip_path.name}: extracted {extracted_size} bytes, "
                    f"expected {info.file_size}"
                )
            # Atomic, replaces an existing target on Windows too, and crosses dirs
            # -- but never filesystems: `tmp_dir` is documented to share
            # `dest_dir`'s, because a cross-device os.replace raises EXDEV instead.
            os.replace(tmp, dest)
        except BaseException:
            # BaseException, not Exception: a KeyboardInterrupt/SystemExit landing
            # mid-copy strands exactly the same file. missing_ok because a
            # successful os.replace has already consumed the .tmp.
            try:
                tmp.unlink(missing_ok=True)
            except OSError as cleanup_exc:
                # Never mask the real failure with a cleanup one -- but never go quiet
                # either: what is left is no longer ingestible, yet a member of this
                # size is up to 6.78 GB of Volume quota nobody knows about.
                print(f"  cleanup: could not remove {tmp}: {cleanup_exc} -- STILL THERE "
                      "(harmless to ingestion: no stream reads that dir; it does hold "
                      "space until the next run truncates it)")
            raise
    return dest


def _reject_negative_header_offset(zip_path: Path, info: zipfile.ZipInfo) -> None:
    """Fail with a diagnosis instead of the EINVAL seek a short archive causes.

    When an archive is missing bytes but still carries its original tail (what a
    short-written upload produces), CPython's
    ``concat = ecd_location - size_cd - offset_cd`` goes negative and shifts every
    member's ``header_offset`` below zero. ``ZipFile(...)`` and ``infolist()``
    both succeed on such a file; the break only lands at ``z.open(member)``, as
    ``OSError: [Errno 22] Invalid argument`` from seeking to a negative position
    -- a message that says nothing about the actual problem. This is how the F1.3
    Estabelecimentos job failed twice on a Volume object that had landed
    273,373,127 of its 341,333,959 bytes."""
    if info.header_offset < 0:
        raise CorruptZipError(
            f"{zip_path.name}: member {info.filename!r} has a negative local "
            f"header offset ({info.header_offset}) -- the archive is shorter than "
            "the offsets its own central directory advertises, i.e. an incomplete "
            "or short-written upload. Re-upload the zip and re-run."
        )
