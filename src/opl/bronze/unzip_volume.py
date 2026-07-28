"""Unzip RFB part zips already staged inside a UC Volume into the table landing
subdir, ON Databricks (serverless, over ``/Volumes/...`` FUSE paths).

WHY this exists: the Files API caps a single-PUT upload at 5 GiB (databricks-sdk
0.40), but the largest RFB parts (Estabelecimentos part 0's inner CSV is ~14 GB
uncompressed) exceed that. So the giants are uploaded as their compressed ZIPs
(which fit under the cap) into a ``zips`` subdir, then unzipped in place on the
cluster into the table landing subdir -- the two-layer control-plane/data-plane
topology of ADR 0002/0004.

The logic is pure ``zipfile`` over directories: no Spark/Java, unit-tested
locally with tmp dirs. Idempotent -- a zip whose inner file already exists at
the expected uncompressed size is skipped, so re-runs are safe; a member that
failed to extract leaves nothing behind and is re-extracted next time.

Nothing half-written is allowed to survive in ``dest_dir``, because that dir is
what the Estabelecimentos Auto Loader reads with no ``pathGlobFilter``: an
orphaned ``.tmp`` would be discovered and ingested as if it were a complete CSV.

Defense in depth against a bad input object: a zip that is missing bytes yet
still carries its original tail (see ``_reject_negative_header_offset``) is
rejected with ``CorruptZipError`` naming the archive, rather than blowing up
deep inside ``zipfile`` on an unexplained negative seek."""
from __future__ import annotations

import os
import shutil
import zipfile
from pathlib import Path

_COPY_CHUNK = 8 << 20  # 8 MiB -- stream members; never .read() a ~14 GB CSV whole.


class CorruptZipError(ValueError):
    """A zip whose central directory points outside the archive it lives in."""


def unzip_dir(zips_dir: str | Path, dest_dir: str | Path) -> list[Path]:
    """Unzip every ``*.zip`` in ``zips_dir`` (sorted by name) into ``dest_dir``.

    Each RFB zip contains exactly one inner file (``ValueError`` otherwise --
    same convention as ``opl.extraction.landing.unzip_single``). If the inner
    file already exists in ``dest_dir`` with size == the zip's recorded
    uncompressed size (``ZipInfo.file_size``), it is skipped untouched;
    otherwise it is streamed out to a ``.tmp`` name, size-verified, and atomically
    renamed over the target -- and any failure in between removes that ``.tmp``,
    so ``dest_dir`` never holds a partial file. Returns the dest paths (extracted
    and skipped alike) in processing order."""
    zips_dir = Path(zips_dir)
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    results: list[Path] = []
    # Fail-fast on purpose: a corrupt part aborts the batch rather than being
    # collected, so the operator re-uploads and re-runs instead of discovering
    # the next bad part only after a full pass over the good ones.
    for zip_path in sorted(zips_dir.glob("*.zip")):
        results.append(_unzip_one(zip_path, dest_dir))
    return results


def _unzip_one(zip_path: Path, dest_dir: Path) -> Path:
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

        tmp = dest.with_name(dest.name + ".tmp")
        # NO failure between creating the .tmp and the atomic rename may leave it
        # behind. `dest_dir` is the landing subdir the Estabelecimentos Auto Loader
        # reads with NO pathGlobFilter (only the lookup stream filters `*CSV` --
        # see opl.bronze.autoloader), so an orphaned .tmp is a file that stream
        # discovers and ingests as if it were a complete CSV. Nor does the
        # idempotence skip above catch it: that compares the size of the FINAL
        # name, which a .tmp never reaches. Covers a mid-copy failure (a stalled
        # FUSE read, a damaged member `zipfile` only rejects at its trailing CRC)
        # as well as the size mismatch.
        try:
            with z.open(info) as src, open(tmp, "wb") as out:
                shutil.copyfileobj(src, out, _COPY_CHUNK)

            extracted_size = tmp.stat().st_size
            if extracted_size != info.file_size:
                raise ValueError(
                    f"{zip_path.name}: extracted {extracted_size} bytes, "
                    f"expected {info.file_size}"
                )
            os.replace(tmp, dest)  # atomic; replaces existing target on Windows too.
        except BaseException:
            # BaseException, not Exception: a KeyboardInterrupt/SystemExit landing
            # mid-copy strands exactly the same file. missing_ok because a
            # successful os.replace has already consumed the .tmp.
            try:
                tmp.unlink(missing_ok=True)
            except OSError as cleanup_exc:
                # Never mask the real failure with a cleanup one -- but never go
                # quiet either: an operator told nothing assumes the orphan is gone.
                print(f"  cleanup: could not remove {tmp}: {cleanup_exc} -- STILL THERE")
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
