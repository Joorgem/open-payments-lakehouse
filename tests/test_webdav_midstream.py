"""Mid-stream failure/resume behavior of WebDavClient.download.

Uses a fake requests.Session whose responses die partway through
iter_content, to prove the client resumes from the bytes already written
instead of losing the whole transfer. No network, no sleeping (module _sleep
is monkeypatched out).
"""
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
        # resume: Range from 400, 206 with the remainder
        ("bytes=400-", _FlakyResponse(
            206, PAYLOAD[400:], headers={"Content-Length": str(len(PAYLOAD) - 400)})),
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
                                      headers={"Content-Length": str(700)})),
        ("bytes=600-", _FlakyResponse(206, PAYLOAD[600:],
                                      headers={"Content-Length": str(400)})),
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
            headers={"Content-Length": str(len(PAYLOAD) - offset)})))
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
