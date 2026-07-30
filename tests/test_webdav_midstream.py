"""Mid-stream failure/resume behavior of WebDavClient.download.

Uses a fake requests.Session whose responses die partway through
iter_content, to prove the client resumes from the bytes already written
instead of losing the whole transfer. No network, no sleeping (module _sleep
is monkeypatched out).
"""
from types import SimpleNamespace
from unittest import mock

import pytest
import requests

import opl.extraction.webdav as webdav
from opl.extraction.webdav import WebDavClient

PAYLOAD = b"0123456789" * 100  # 1000 bytes


class _FlakyResponse:
    """Streams `body` but raises ChunkedEncodingError after `die_after` bytes
    (None = stream everything)."""

    def __init__(self, status_code, body, die_after=None, headers=None):
        self.status_code = status_code
        self._body = body
        self._die_after = die_after
        self.headers = headers or {"Content-Length": str(len(body))}

    def iter_content(self, chunk_size):
        sent = 0
        for i in range(0, len(self._body), chunk_size):
            chunk = self._body[i:i + chunk_size]
            if self._die_after is not None and sent + len(chunk) > self._die_after:
                # emit the partial chunk up to the failure point, then die
                partial = chunk[: self._die_after - sent]
                if partial:
                    yield partial
                raise requests.exceptions.ChunkedEncodingError("stream died")
            sent += len(chunk)
            yield chunk

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(response=self)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _client_with(responses):
    """WebDavClient whose session.get pops scripted responses; asserts each
    call's Range header against the ('expected_range', response) script."""
    session = mock.Mock()
    script = list(responses)

    def fake_get(url, auth, headers, stream, timeout):
        expected_range, resp = script.pop(0)
        assert headers.get("Range") == expected_range, (
            f"expected Range={expected_range!r}, got {headers.get('Range')!r}")
        return resp

    session.get = fake_get
    return WebDavClient("https://x/webdav", "tok", session=session), script


def test_resumes_after_midstream_death(tmp_path, monkeypatch):
    monkeypatch.setattr(webdav, "_sleep", lambda s: None)
    dest = tmp_path / "f.zip"
    client, script = _client_with([
        # first GET: no Range, dies after 400 bytes
        (None, _FlakyResponse(200, PAYLOAD, die_after=400)),
        # resume: Range from 400, 206 with the remainder. The Content-Range is
        # not decoration: download() now requires a 206 to PROVE its body starts
        # where the Range asked, so a fake that omits it is not a server we could
        # ever meet (RFC 9110 makes it mandatory on a single-range 206).
        ("bytes=400-", _FlakyResponse(
            206, PAYLOAD[400:], headers={"Content-Length": str(len(PAYLOAD) - 400),
                                         "Content-Range": "bytes 400-999/1000"})),
    ])
    out = client.download("2026-06/f.zip", dest, expected_size=len(PAYLOAD))
    assert out.read_bytes() == PAYLOAD
    assert not script  # both scripted responses consumed


def test_multiple_midstream_deaths_then_success(tmp_path, monkeypatch):
    monkeypatch.setattr(webdav, "_sleep", lambda s: None)
    dest = tmp_path / "f.zip"
    client, script = _client_with([
        (None, _FlakyResponse(200, PAYLOAD, die_after=300)),
        ("bytes=300-", _FlakyResponse(206, PAYLOAD[300:], die_after=300,
                                      headers={"Content-Length": str(700),
                                               "Content-Range": "bytes 300-999/1000"})),
        ("bytes=600-", _FlakyResponse(206, PAYLOAD[600:],
                                      headers={"Content-Length": str(400),
                                               "Content-Range": "bytes 600-999/1000"})),
    ])
    out = client.download("2026-06/f.zip", dest, expected_size=len(PAYLOAD))
    assert out.read_bytes() == PAYLOAD
    assert not script


def test_stream_resume_budget_exhausted_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(webdav, "_sleep", lambda s: None)
    dest = tmp_path / "f.zip"
    # first request + _MAX_STREAM_RESUMES resumes, ALL dying mid-stream
    n = webdav._MAX_STREAM_RESUMES
    responses = [(None, _FlakyResponse(200, PAYLOAD, die_after=10))]
    offset = 10
    for _ in range(n):
        responses.append((f"bytes={offset}-", _FlakyResponse(
            206, PAYLOAD[offset:], die_after=10,
            headers={"Content-Length": str(len(PAYLOAD) - offset),
                     "Content-Range": f"bytes {offset}-{len(PAYLOAD) - 1}/{len(PAYLOAD)}"})))
        offset += 10
    client, script = _client_with(responses)
    with pytest.raises(requests.exceptions.ChunkedEncodingError):
        client.download("2026-06/f.zip", dest, expected_size=len(PAYLOAD))
    assert not script


def test_no_regression_full_stream_no_retry(tmp_path, monkeypatch):
    monkeypatch.setattr(webdav, "_sleep", lambda s: None)
    dest = tmp_path / "f.zip"
    client, script = _client_with([(None, _FlakyResponse(200, PAYLOAD))])
    out = client.download("2026-06/f.zip", dest, expected_size=len(PAYLOAD))
    assert out.read_bytes() == PAYLOAD


def test_range_start_parses_a_normal_content_range():
    from opl.extraction.webdav import _range_start
    resp = SimpleNamespace(headers={"Content-Range": "bytes 1048576-2097151/2097152"})
    assert _range_start(resp) == 1048576


def test_range_start_is_none_when_the_header_is_absent_or_junk():
    from opl.extraction.webdav import _range_start
    assert _range_start(SimpleNamespace(headers={})) is None
    assert _range_start(SimpleNamespace(headers={"Content-Range": "bytes */2097152"})) is None
    assert _range_start(SimpleNamespace(headers={"Content-Range": "pages 1-2/3"})) is None


def test_a_206_that_ignored_the_range_is_not_treated_as_a_resume(tmp_path):
    """A 206 whose body starts at 0 must TRUNCATE, not append.

    Appending it onto the K bytes already on disk yields a file of
    K + full_size -- right shape, wrong bytes, and the size check is the only
    thing that would catch it."""
    dest = tmp_path / "part.zip"
    dest.write_bytes(b"A" * 4)  # a partial file already on disk

    body = b"B" * 10
    client, script = _client_with([
        ("bytes=4-", _FlakyResponse(
            206, body, headers={"Content-Range": f"bytes 0-{len(body) - 1}/{len(body)}"})),
    ])

    client.download("x/part.zip", dest, expected_size=len(body))

    assert dest.read_bytes() == body, "the stale prefix must be discarded, not appended to"
    assert not script


def test_a_206_honouring_the_range_still_appends(tmp_path):
    dest = tmp_path / "part.zip"
    dest.write_bytes(b"A" * 4)

    tail = b"B" * 6
    client, script = _client_with([
        ("bytes=4-", _FlakyResponse(206, tail, headers={"Content-Range": "bytes 4-9/10"})),
    ])

    client.download("x/part.zip", dest, expected_size=10)

    assert dest.read_bytes() == b"A" * 4 + tail
    assert not script
