# tests/test_webdav.py
import pytest

import opl.extraction.webdav as webdav_mod
from opl.extraction.webdav import FileEntry, IntegrityError, WebDavClient

PROPFIND_XML = b"""<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
  <d:response><d:href>/public.php/webdav/2026-07/</d:href>
    <d:propstat><d:prop><d:getcontentlength/>
    <d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>
  <d:response><d:href>/public.php/webdav/2026-07/Qualificacoes.zip</d:href>
    <d:propstat><d:prop><d:getcontentlength>980</d:getcontentlength>
    <d:resourcetype/></d:prop></d:propstat></d:response>
</d:multistatus>"""


class _Resp:
    def __init__(self, status, content=b"", headers=None):
        self.status_code = status
        self.content = content
        self.headers = headers or {}
    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"http {self.status_code}")
    def iter_content(self, chunk_size):
        yield self.content
    def __enter__(self): return self
    def __exit__(self, *a): return False


def test_list_dir_parses_propfind(monkeypatch):
    client = WebDavClient("https://h/public.php/webdav", "TOK")
    monkeypatch.setattr(client.session, "request",
                        lambda *a, **k: _Resp(207, PROPFIND_XML))
    entries = client.list_dir("2026-07")
    files = [e for e in entries if not e.is_dir]
    assert len(files) == 1
    assert files[0] == FileEntry(name="Qualificacoes.zip",
                                 rel_path="2026-07/Qualificacoes.zip",
                                 size=980, is_dir=False)


PROPFIND_XML_NO_LENGTH = b"""<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
  <d:response><d:href>/public.php/webdav/2026-07/</d:href>
    <d:propstat><d:prop><d:getcontentlength/>
    <d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>
  <d:response><d:href>/public.php/webdav/2026-07/Empresas0.zip</d:href>
    <d:propstat><d:prop>
    <d:resourcetype/></d:prop></d:propstat></d:response>
</d:multistatus>"""


def test_list_dir_missing_contentlength_yields_none_size(monkeypatch):
    """A PROPFIND response whose file entry has NO getcontentlength element
    must parse to size=None (unknown), not size=0 (which would collide with
    a real empty file and wrongly fail the download's integrity check)."""
    client = WebDavClient("https://h/public.php/webdav", "TOK")
    monkeypatch.setattr(client.session, "request",
                        lambda *a, **k: _Resp(207, PROPFIND_XML_NO_LENGTH))
    entries = client.list_dir("2026-07")
    files = [e for e in entries if not e.is_dir]
    assert len(files) == 1
    assert files[0] == FileEntry(name="Empresas0.zip",
                                 rel_path="2026-07/Empresas0.zip",
                                 size=None, is_dir=False)


def test_download_size_mismatch_raises(monkeypatch, tmp_path):
    client = WebDavClient("https://h/public.php/webdav", "TOK")
    monkeypatch.setattr(client.session, "get",
                        lambda *a, **k: _Resp(200, b"abc", {"Content-Length": "3"}))
    with pytest.raises(IntegrityError):
        client.download("2026-07/x.zip", tmp_path / "x.zip", expected_size=999)


def test_download_writes_file(monkeypatch, tmp_path):
    client = WebDavClient("https://h/public.php/webdav", "TOK")
    monkeypatch.setattr(client.session, "get",
                        lambda *a, **k: _Resp(200, b"abc", {"Content-Length": "3"}))
    out = client.download("2026-07/x.zip", tmp_path / "x.zip", expected_size=3)
    assert out.read_bytes() == b"abc"


def test_download_resume_honored_appends(monkeypatch, tmp_path):
    dest = tmp_path / "x.zip"
    dest.write_bytes(b"abc")  # partial file already on disk (3 bytes)
    client = WebDavClient("https://h/public.php/webdav", "TOK")
    monkeypatch.setattr(
        client.session, "get",
        lambda *a, **k: _Resp(
            206, b"def",
            {"Content-Length": "3", "Content-Range": "bytes 3-5/6"},
        ),
    )
    out = client.download("2026-07/x.zip", dest, expected_size=6)
    assert out.read_bytes() == b"abcdef"


def test_download_range_ignored_returns_200_no_corruption(monkeypatch, tmp_path):
    dest = tmp_path / "x.zip"
    dest.write_bytes(b"abc")  # partial file already on disk (3 bytes)
    full_body = b"abcdef"
    client = WebDavClient("https://h/public.php/webdav", "TOK")
    monkeypatch.setattr(
        client.session, "get",
        lambda *a, **k: _Resp(200, full_body, {"Content-Length": str(len(full_body))}),
    )
    out = client.download("2026-07/x.zip", dest, expected_size=len(full_body))
    # Server ignored Range and sent the full body: old partial bytes must be
    # discarded (not appended onto), leaving exactly the full body.
    assert out.read_bytes() == full_body


def test_download_already_complete_416(monkeypatch, tmp_path):
    dest = tmp_path / "x.zip"
    dest.write_bytes(b"abcdef")  # already fully downloaded (6 bytes)
    client = WebDavClient("https://h/public.php/webdav", "TOK")
    monkeypatch.setattr(client.session, "get", lambda *a, **k: _Resp(416, b""))
    out = client.download("2026-07/x.zip", dest, expected_size=6)
    assert out.read_bytes() == b"abcdef"


def test_download_retries_on_transient_500_then_succeeds(monkeypatch, tmp_path):
    monkeypatch.setattr(webdav_mod, "_sleep", lambda *_a, **_k: None)
    calls = {"n": 0}

    def fake_get(*_a, **_k):
        calls["n"] += 1
        if calls["n"] <= 2:
            return _Resp(500, b"")
        return _Resp(200, b"abc", {"Content-Length": "3"})

    client = WebDavClient("https://h/public.php/webdav", "TOK")
    monkeypatch.setattr(client.session, "get", fake_get)
    out = client.download("2026-07/x.zip", tmp_path / "x.zip", expected_size=3)
    assert out.read_bytes() == b"abc"
    assert calls["n"] == 3


def test_list_dir_retries_on_transient_503_then_succeeds(monkeypatch):
    monkeypatch.setattr(webdav_mod, "_sleep", lambda *_a, **_k: None)
    calls = {"n": 0}

    def fake_request(*_a, **_k):
        calls["n"] += 1
        if calls["n"] == 1:
            return _Resp(503, b"")
        return _Resp(207, PROPFIND_XML)

    client = WebDavClient("https://h/public.php/webdav", "TOK")
    monkeypatch.setattr(client.session, "request", fake_request)
    entries = client.list_dir("2026-07")
    files = [e for e in entries if not e.is_dir]
    assert len(files) == 1
    assert calls["n"] == 2


def test_download_raises_after_exhausting_retries(monkeypatch, tmp_path):
    monkeypatch.setattr(webdav_mod, "_sleep", lambda *_a, **_k: None)
    calls = {"n": 0}

    def always_500(*_a, **_k):
        calls["n"] += 1
        return _Resp(500, b"")

    client = WebDavClient("https://h/public.php/webdav", "TOK")
    monkeypatch.setattr(client.session, "get", always_500)
    with pytest.raises(webdav_mod.requests.exceptions.HTTPError):
        client.download("2026-07/x.zip", tmp_path / "x.zip", expected_size=3)
    assert calls["n"] == webdav_mod._MAX_ATTEMPTS
