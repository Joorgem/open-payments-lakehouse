"""Unzip RFB part zips already staged inside a UC Volume into the table landing
subdir, ON Databricks (serverless, over ``/Volumes/...`` FUSE paths).

WHY this exists: the Files API caps a single-PUT upload at 5 GiB (databricks-sdk
0.40), but the largest RFB parts (Estabelecimentos part 0's inner CSV is ~14 GB
uncompressed) exceed that. So the giants are uploaded as their compressed ZIPs
(which fit under the cap) into a ``zips`` subdir, then unzipped in place on the
cluster into the table landing subdir -- the two-layer control-plane/data-plane
topology of ADR 0002/0004.

Each zip is staged to the node's local disk before being opened: the Volumes
FUSE mount serves sequential streams but rejects ``zipfile``'s random-access
backward seek to a member's local header (``OSError: [Errno 22]`` on
serverless), so reading a member in place is impossible. Writing the extracted
member back to the Volume is a sequential stream, hence supported.

The logic is pure ``zipfile`` over directories: no Spark/Java, unit-tested
locally with tmp dirs. Idempotent -- a zip whose inner file already exists at
the expected uncompressed size is skipped, so re-runs are safe and a run that
died mid-extract (leaving only a ``.tmp``) re-extracts next time."""
from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
from pathlib import Path

_COPY_CHUNK = 8 << 20  # 8 MiB -- stream members; never .read() a ~14 GB CSV whole.


def unzip_dir(zips_dir: str | Path, dest_dir: str | Path) -> list[Path]:
    """Unzip every ``*.zip`` in ``zips_dir`` (sorted by name) into ``dest_dir``.

    Each RFB zip contains exactly one inner file (``ValueError`` otherwise --
    same convention as ``opl.extraction.landing.unzip_single``). If the inner
    file already exists in ``dest_dir`` with size == the zip's recorded
    uncompressed size (``ZipInfo.file_size``), it is skipped untouched;
    otherwise it is streamed out to a ``.tmp`` name and atomically renamed over
    the target, then its size is verified. Returns the dest paths (extracted and
    skipped alike) in processing order."""
    zips_dir = Path(zips_dir)
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    results: list[Path] = []
    for zip_path in sorted(zips_dir.glob("*.zip")):
        results.append(_unzip_one(zip_path, dest_dir))
    return results


def _unzip_one(zip_path: Path, dest_dir: Path) -> Path:
    # Staged one zip at a time and dropped on exit, so peak local disk is one zip.
    with tempfile.TemporaryDirectory() as staging:
        staged = Path(staging) / zip_path.name
        shutil.copyfile(zip_path, staged)  # OS-streamed; part 0 is ~2 GB.
        return _extract_single(staged, dest_dir)


def _extract_single(zip_path: Path, dest_dir: Path) -> Path:
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

        tmp = dest.with_name(dest.name + ".tmp")
        with z.open(info) as src, open(tmp, "wb") as out:
            shutil.copyfileobj(src, out, _COPY_CHUNK)

        extracted_size = tmp.stat().st_size
        if extracted_size != info.file_size:
            tmp.unlink()
            raise ValueError(
                f"{zip_path.name}: extracted {extracted_size} bytes, "
                f"expected {info.file_size}"
            )
        os.replace(tmp, dest)  # atomic; replaces existing target on Windows too.
    return dest
