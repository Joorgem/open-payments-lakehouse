# tests/test_webdav.py
import pytest

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
