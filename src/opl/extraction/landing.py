# src/opl/extraction/landing.py
"""Unzip a CNPJ part (single inner K-file) and land raw files into a UC Volume
via the Databricks control plane (two-layer topology, ADR 0002)."""
from __future__ import annotations

import zipfile
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.errors import NotFound
from opl.config import DEFAULT

LANDING_VOLUME_DIR = DEFAULT.landing_cnpj_root
UPLOAD_PROFILE = "opl-free"

# WHY widen this: `retry_timeout_seconds` is the SDK's TOTAL wall-clock budget
# for one API call across every attempt (`_base_client.py:78`, default 300 s),
# and `retried()` only checks that deadline BETWEEN attempts (sdk retries.py).
# So the budget never interrupts a PUT in flight; what it does is make the first
# retryable failure fatal once the budget is spent -- which is how a long upload
# ends as `Timed out after 0:05:00`: some attempt failed and there was no time
# left to retry it. Estabelecimentos3.zip (366,824,247 B) died that way. At the
# ~67 MB/min measured upstream to this workspace a ~340 MB part needs ~5 min,
# i.e. the whole default budget, so a part that size spends it inside its first
# attempt and has no retry left. (What that first failure was is not recorded,
# and duration alone cannot produce this error, so no size threshold is claimed
# here -- the widening is justified by the mechanism, not by a correlation.)
# The largest payload is bigger still:
# extract_cnpj.py PUTs uncompressed inner CSVs (Simples' is several GB,
# up to the Files API's 5 GiB single-PUT cap => ~75 min at that rate). 2 h
# covers one full attempt of the worst case plus room for a retry.
UPLOAD_RETRY_TIMEOUT_SECONDS = 2 * 60 * 60
# `http_timeout_seconds` is deliberately NOT touched: it is the per-socket
# connect/read inactivity timeout handed straight to `requests`
# (`_base_client.py:95`, default 60 s), not part of the budget above. No
# evidence implicates it, and raising it only turns a dead socket into a hang.


class UploadIntegrityError(OSError):
    """A Files API upload landed a byte count different from the local source."""


def upload_client(**auth: object) -> WorkspaceClient:
    """Build the WorkspaceClient every Volume upload path must use.

    `retry_timeout_seconds` is a Config field, not a WorkspaceClient.__init__
    kwarg (databricks-sdk 0.40), so it has to travel through an explicit Config;
    passing it to the client raises TypeError. `auth` defaults to the local
    `opl-free` CLI profile; tests pass an explicit host/token so this factory
    needs no credentials.
    """
    return WorkspaceClient(
        config=Config(
            retry_timeout_seconds=UPLOAD_RETRY_TIMEOUT_SECONDS,
            **(auth or {"profile": UPLOAD_PROFILE}),
        )
    )


def unzip_single(zip_path: Path, dest_dir: Path) -> Path:
    # Same "exactly one inner member, then extract" job as
    # opl.bronze.unzip_volume, deliberately without that module's
    # negative-header-offset guard: this only ever opens a zip just downloaded to
    # local disk whose byte count WebDavClient already checked against the WebDAV
    # manifest, so the short-archive-with-intact-tail case the guard diagnoses
    # cannot reach here. unzip_volume opens Volume objects landed by a PUT, where
    # it can and did.
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
    ``z.open()``. So no failure path from the PUT onwards is allowed to leave a
    readable half-written object: the target is deleted before the error
    propagates. That guard spans the verification call too -- a 503 from
    ``get_metadata`` leaves an object of unknown length, and unverified is not
    verified. It deliberately does NOT span opening the local file: a local
    failure (a missing source, or the Windows PermissionError an AV scanner
    causes) means this call never wrote a byte, and deleting a remote object it
    never touched would destroy an already-correctly-landed part on a re-run.

    ASSUMES EXCLUSIVE OWNERSHIP of ``target``: the cleanup deletes by path, so
    two processes uploading the same part concurrently can have one's failure
    delete the other's good object. Land each part from one uploader only.

    Raises ``UploadIntegrityError`` if the remote size differs from the local one
    or cannot be read at all. Deliberately does NOT retry: the caller owns that
    policy; this function's contract is to fail loudly and leave nothing behind.
    """
    local_path = Path(local_path)
    target = f"{volume_dir.rstrip('/')}/{local_path.name}"
    expected = local_path.stat().st_size
    with open(local_path, "rb") as f:
        try:
            w.files.upload(target, f, overwrite=True)
            actual = w.files.get_metadata(target).content_length
            if actual is None:
                raise UploadIntegrityError(
                    f"{target}: upload of {expected} bytes could not be verified "
                    "-- the Files API reported no content-length for the remote "
                    "object"
                )
            if actual != expected:
                raise UploadIntegrityError(
                    f"{target}: uploaded {expected} bytes but the remote object is "
                    f"{actual} bytes ({expected - actual} missing) -- the PUT was "
                    "short-written; re-upload before reading it"
                )
        except BaseException:
            _discard_remote(w, target)
            raise
    return target


def _discard_remote(w: WorkspaceClient, target: str) -> None:
    """Best-effort delete of a failed upload's target, reporting what happened.

    Never re-raises: the caller must see the real problem, not a cleanup
    exception masking it. But silence is not an option either -- an operator who
    is told nothing assumes the corrupt object is gone. So the three outcomes are
    reported apart: deleted, nothing to delete (the 404 a clean failure leaves
    behind, expected), and a genuine delete failure that leaves the object in
    place.
    """
    try:
        w.files.delete(target)
    except NotFound:
        print(f"  cleanup: nothing to delete at {target} (no object was left behind)")
    except Exception as exc:
        print(f"  cleanup: delete FAILED for {target}: {exc} -- it is STILL THERE")
    else:
        print(f"  cleanup: deleted {target}")
