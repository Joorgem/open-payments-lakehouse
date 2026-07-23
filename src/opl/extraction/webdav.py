"""Minimal Nextcloud public-share WebDAV client: list a directory (PROPFIND)
and download a file with Range-based resume + size integrity check.

The live RFB WebDAV server is flaky (~50% transient HTTP 500s observed in
practice), so both the PROPFIND (list_dir) and GET (download) requests are
wrapped in a shared retry-with-backoff helper."""
from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import requests

_DAV = "{DAV:}"

# Module-level, monkeypatchable sleep so tests never actually wait.
_sleep = time.sleep

_RETRYABLE_STATUS = {500, 502, 503, 504}
_MAX_ATTEMPTS = 5
_BACKOFF_BASE = 0.5  # seconds: 0.5, 1, 2, 4 (2**n * base for n in 0..3)


class IntegrityError(Exception):
    """Downloaded file size did not match the expected/advertised size."""


@dataclass(frozen=True)
class FileEntry:
    name: str
    rel_path: str
    size: int
    is_dir: bool


def _request_with_retry(request_fn: Callable[[], requests.Response]) -> requests.Response:
    """Call request_fn(), retrying on transient network errors or 5xx status.

    Retries up to _MAX_ATTEMPTS times with exponential backoff (0.5, 1, 2, 4s).
    On final failure, re-raises the last error.
    """
    last_error: Exception | None = None
    for attempt in range(_MAX_ATTEMPTS):
        try:
            resp = request_fn()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_error = exc
        else:
            if resp.status_code not in _RETRYABLE_STATUS:
                return resp
            last_error = requests.exceptions.HTTPError(
                f"transient http {resp.status_code}", response=resp
            )
            close = getattr(resp, "close", None)
            if callable(close):
                close()
        if attempt < _MAX_ATTEMPTS - 1:
            _sleep(_BACKOFF_BASE * (2 ** attempt))
    assert last_error is not None
    raise last_error


class WebDavClient:
    def __init__(self, base_url: str, token: str, session: requests.Session | None = None):
        self.base_url = base_url.rstrip("/")
        self.auth = (token, "")
        self.session = session or requests.Session()

    def _url(self, rel_path: str) -> str:
        return f"{self.base_url}/{rel_path.strip('/')}"

    def list_dir(self, rel_path: str) -> list[FileEntry]:
        resp = _request_with_retry(lambda: self.session.request(
            "PROPFIND", self._url(rel_path) + "/", auth=self.auth,
            headers={"Depth": "1"}, timeout=60,
        ))
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        base_marker = f"/{rel_path.strip('/')}/"
        entries: list[FileEntry] = []
        for r in root.findall(f"{_DAV}response"):
            href = r.findtext(f"{_DAV}href") or ""
            prop = r.find(f"{_DAV}propstat/{_DAV}prop")
            is_dir = prop is not None and prop.find(
                f"{_DAV}resourcetype/{_DAV}collection") is not None
            # skip the directory-self entry (href ends exactly at the listed dir)
            if href.rstrip("/").endswith(base_marker.rstrip("/")):
                continue
            name = href.rstrip("/").split("/")[-1]
            size_txt = prop.findtext(f"{_DAV}getcontentlength") if prop is not None else None
            entries.append(FileEntry(
                name=name,
                rel_path=f"{rel_path.strip('/')}/{name}",
                size=int(size_txt) if size_txt else 0,
                is_dir=is_dir,
            ))
        return entries

    def download(self, rel_path: str, dest: Path, expected_size: int | None = None) -> Path:
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        resume_from = dest.stat().st_size if dest.exists() else 0
        headers = {"Range": f"bytes={resume_from}-"} if resume_from else {}
        with _request_with_retry(lambda: self.session.get(
            self._url(rel_path), auth=self.auth, headers=headers,
            stream=True, timeout=120,
        )) as r:
            if r.status_code == 416:
                # Server says the requested range is beyond the resource's size,
                # which happens when resume_from already equals the full file size.
                already_done = dest.exists() and dest.stat().st_size == expected_size
                if expected_size is not None and already_done:
                    return dest
                # Stale/inconsistent partial file: discard it and re-fetch from scratch.
                if dest.exists():
                    dest.unlink()
                return self.download(rel_path, dest, expected_size)

            r.raise_for_status()

            # Only treat this as a genuine resume if the server actually honored the
            # Range request (206 Partial Content). A server that ignores Range and
            # replies 200 sends the FULL body, so appending it onto existing partial
            # bytes would corrupt the file — fall back to writing from scratch.
            resumed = resume_from > 0 and r.status_code == 206
            effective_start = resume_from if resumed else 0
            mode = "ab" if resumed else "wb"

            with open(dest, mode) as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)

            target = expected_size
            if target is None:
                cl = r.headers.get("Content-Length")
                target = (effective_start + int(cl)) if cl else None
        actual = dest.stat().st_size
        if target is not None and actual != target:
            raise IntegrityError(f"{rel_path}: expected {target} bytes, got {actual}")
        return dest
