# src/opl/extraction/landing.py
"""Unzip a CNPJ part (single inner K-file) and land raw files into a UC Volume
via the Databricks control plane (two-layer topology, ADR 0002)."""
from __future__ import annotations

import zipfile
from pathlib import Path

from databricks.sdk import WorkspaceClient
from opl.config import DEFAULT

LANDING_VOLUME_DIR = DEFAULT.landing_cnpj_root


class UploadIntegrityError(OSError):
    """A Files API upload landed a byte count different from the local source."""


def unzip_single(zip_path: Path, dest_dir: Path) -> Path:
    zip_path = Path(zip_path)
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as z:
        members = [m for m in z.namelist() if not m.endswith("/")]
        if len(members) != 1:
            raise ValueError(f"{zip_path.name}: expected 1 inner file, got {len(members)}")
        inner = members[0]
        z.extract(inner, dest_dir)
    return dest_dir / inner


def upload_to_volume(w: WorkspaceClient, local_path: Path, volume_dir: str) -> str:
    """PUT ``local_path`` into ``volume_dir`` and verify the landed byte count.

    WHY the verification: a single-PUT ``w.files.upload()`` of a 341 MB zip was
    observed to return without error having written only 273 MB -- bytes missing
    from the MIDDLE of the object, tail intact. Nothing downstream noticed until
    ``zipfile`` computed a negative member offset from the still-original central
    directory and died on an EINVAL seek, two job runs later. So every upload is
    checked against the source size here, at the only point where the truth is
    still cheap to establish.

    WHY the cleanup: an upload that fails may leave nothing behind (a PUT that
    timed out at the SDK's 5-minute default left no object at all) or a partial
    one. A partial object is the dangerous case -- it reads as a valid zip until
    ``z.open()``. So no failure path is allowed to leave a readable half-written
    object: the target is deleted before the error propagates.

    Raises ``UploadIntegrityError`` if the remote size differs from the local one
    or cannot be read at all. Deliberately does NOT retry: the caller owns that
    policy; this function's contract is to fail loudly and leave nothing behind.
    """
    local_path = Path(local_path)
    target = f"{volume_dir.rstrip('/')}/{local_path.name}"
    expected = local_path.stat().st_size
    try:
        with open(local_path, "rb") as f:
            w.files.upload(target, f, overwrite=True)
    except BaseException:
        _discard_remote(w, target)
        raise

    actual = w.files.get_metadata(target).content_length
    if actual is None:
        _discard_remote(w, target)
        raise UploadIntegrityError(
            f"{target}: upload of {expected} bytes could not be verified -- the "
            "Files API reported no content-length for the remote object"
        )
    if actual != expected:
        _discard_remote(w, target)
        raise UploadIntegrityError(
            f"{target}: uploaded {expected} bytes but the remote object is "
            f"{actual} bytes ({expected - actual} missing) -- the PUT was "
            "short-written; re-upload before reading it"
        )
    return target


def _discard_remote(w: WorkspaceClient, target: str) -> None:
    """Best-effort delete of a failed upload's target.

    A clean failure leaves no object, so the DELETE 404s -- that is the expected
    case, not an error. Any delete failure is swallowed on purpose: the caller
    must see the real problem, never a cleanup exception masking it.
    """
    try:
        w.files.delete(target)
    except Exception:  # noqa: BLE001 - cleanup must never mask the original error
        pass
