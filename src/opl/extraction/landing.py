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

# WHAT THIS BOUNDS, re-derived against the multipart path adopted in ADR 0007.
# `retry_timeout_seconds` is the SDK's total wall-clock budget for ONE retried
# operation across all its attempts, and `retried()` checks that deadline only
# BETWEEN attempts (sdk retries.py). So it never interrupts a request in flight;
# what it does is make the first retryable failure fatal once the budget is
# spent. That mechanism is unchanged -- what changed is the unit of work it
# applies to.
#
# Under the old single-PUT path the unit was the whole file, which is why this
# was once 2 h: Estabelecimentos0.zip is 2,128,818,559 B, ~32 min at the
# ~67 MB/min measured upstream to this workspace, and 2 h covered one attempt
# plus a retry. Under multipart there is NO whole-file operation to bound. In
# databricks-sdk 0.123.0 the budget is applied per operation, in two places:
#   * `_BaseClient.do` -- each control-plane call (initiate-upload, fetch part
#     URLs, complete-upload);
#   * `_retry_cloud_idempotent_operation` (mixins/files.py) -- each presigned
#     part PUT to cloud storage, together with
#     `experimental_files_ext_cloud_api_max_retries` (3).
# The largest unit is therefore ONE PART, not one file.
#
# Sizing it, from measured part geometry (see ADR 0007). The SDK picks the
# smallest part size giving <= 100 parts: Estabelecimentos0.zip lands on 50 MiB
# parts (41 of them), every other zip we upload on 10 MiB parts. Uploads run
# `files_ext_multipart_upload_default_parallelism` = 10 parts concurrently, so
# the ~67 MB/min link is shared ten ways and a single 50 MiB part takes
# ~52.4 MB / (67/10 MB/min) ~= 7.8 min of wall clock -- an order of magnitude
# longer than the same part would take alone. That is the number this budget has
# to clear.
#
# 30 min is ~3.8x that worst-case part, so the largest part can fail and be
# retried the 3 times the cloud-retry cap allows without the budget going flat
# mid-way. Note the SDK's own 300 s default is SMALLER than one worst-case part
# here, which would recreate the F1.3 failure exactly at part granularity: the
# budget spent inside the first attempt, so the first retryable failure is fatal
# (that is how Estabelecimentos3.zip, 366,824,247 B, died as
# `Timed out after 0:05:00`). So this stays explicit rather than reverting to
# the default -- but it is 30 min because a part takes ~8 min, not 2 h because a
# file takes ~32 min. The old value is not merely oversized now, it is sized
# against an operation that no longer exists.
UPLOAD_RETRY_TIMEOUT_SECONDS = 30 * 60
# `http_timeout_seconds` is deliberately NOT touched: it is the per-socket
# connect/read inactivity timeout handed straight to `requests`
# (`_base_client.py:95`, default 60 s), not part of the budget above. No
# evidence implicates it, and raising it only turns a dead socket into a hang.


class UploadIntegrityError(OSError):
    """A Files API upload landed a byte count different from the local source."""


def upload_client(**auth: object) -> WorkspaceClient:
    """Build the WorkspaceClient every Volume upload path must use.

    `retry_timeout_seconds` is a Config field, not a WorkspaceClient.__init__
    kwarg, so it has to travel through an explicit Config; passing it to the
    client raises `TypeError: WorkspaceClient.__init__() got an unexpected
    keyword argument 'retry_timeout_seconds'`. True of the old 0.40.0 pin and
    re-verified against 0.123.0 -- `WorkspaceClient.__init__` still takes no
    **kwargs and exposes no retry/timeout parameter.

    WHY THE BUDGET IS SET AFTER CONSTRUCTION rather than passed as a Config
    kwarg, which reads like a pointless two-step: since databricks-sdk 0.123.0
    `Config.__init__` ends with `_resolve_host_metadata()`, a best-effort GET of
    `{host}/.well-known/databricks-config` used to discover `account_id` /
    `workspace_id` / `discovery_url`. It builds its probe client from
    `self.retry_timeout_seconds` -- the SDK's own comment there says it does that
    to avoid "blocking Config() initialization for ~5 minutes when the host is
    unreachable". The consequence for us is the inverse of the intent: handing
    the widened upload budget to `Config(...)` hands it to that probe too, so an
    unreachable or blackholed host makes `upload_client()` block for the whole
    budget -- 2 h under the old value -- before falling back and doing anything.
    Constructing with the SDK default and raising the budget afterwards keeps the
    discovery probe on the SDK's own 300 s bound while the uploads that follow
    get the 30 min they need. Verified: the client reads the field when it builds
    its `_BaseClient`, so assigning before `WorkspaceClient(...)` takes effect.

    We do not need what the probe resolves -- `auth` carries an explicit host and
    token (or the `opl-free` CLI profile) -- but the SDK exposes no way to skip
    it, so bounding it is the available lever. Tests neutralise it outright;
    a hermetic unit test must not depend on DNS.

    `auth` defaults to the local `opl-free` CLI profile; tests pass an explicit
    host/token so this factory needs no credentials.
    """
    config = Config(**(auth or {"profile": UPLOAD_PROFILE}))
    config.retry_timeout_seconds = UPLOAD_RETRY_TIMEOUT_SECONDS
    return WorkspaceClient(config=config)


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

    WHY the verification: a single-PUT ``w.files.upload()`` of a 341,333,959-byte
    zip returned without error having stored only 273,373,127 -- the object was
    ``original[67960832:]``, i.e. a dropped PREFIX with the tail (and therefore
    the original central directory) intact. That is a bug in the SDK version in
    use when it happened, not an inference: in ``databricks-sdk`` <= 0.41.0
    (this repo was pinned at 0.40.0 then) ``_BaseClient._perform`` rewinds
    the request body only inside its ``if error is not None`` branch, so a raised
    ``ConnectionError``/``Timeout`` skips the rewind; ``retried()`` re-sends from
    the offset where the stream failed and ``requests`` recomputes
    ``Content-Length`` as ``st_size - tell()``. The retry is thus a complete,
    well-formed PUT of the remainder, which the server stores -- and
    341,333,959 - 67,960,832 is exactly the 273,373,127 observed. Fixed upstream
    in databricks-sdk-py#878, released 0.42.0; this repo is far above that floor
    now (ADR 0007) and no longer uses the single-PUT path at all.

    So this check is DEFENCE IN DEPTH, not a stand-in for a fix we are missing.
    It compares the landed object's ``content_length`` against the local file's
    size, which is independent of how the bytes got there -- one PUT, or the 41
    presigned parts Estabelecimentos0.zip is now split into. That independence is
    the point: multipart moves the ways an upload can be short (a part silently
    dropped, a part written twice, a complete-upload that commits a subset) but
    does not remove them, and it enlarges the surface -- every part is its own
    request, retried under its own budget, against presigned URLs that expire.
    A whole-object size check is the one assertion that survives all of it, and
    it was the only thing that caught the defect the first time. (An earlier
    account read the loss as a gap in the MIDDLE; that was retracted -- a
    dropped prefix and a middle gap yield
    identical arithmetic downstream, and the object's head was never read before
    it was deleted, so only the mechanism settles it.) Nothing downstream noticed
    until ``zipfile`` computed a negative member offset from the still-original
    central directory and died on an EINVAL seek, two job runs later. So every
    upload is checked against the source size here, at the only point where the
    truth is still cheap to establish.

    WHY the cleanup: an upload that fails may leave nothing behind (a PUT that
    timed out at the SDK's 5-minute default left no object at all) or a partial
    one -- and under multipart an aborted or half-committed session is another
    way to get one. A partial object is the dangerous case -- it reads as a
    valid zip until
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
