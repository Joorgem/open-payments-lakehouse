# tests/test_landing_client.py
"""Unit tests for the upload WorkspaceClient factory.

Hermetic: no network, no credentials, no `.databrickscfg` profile -- the client
is built from an explicit dummy host/token, which the SDK resolves locally (no
auth call happens at construction time).
"""
from __future__ import annotations

from opl.extraction.landing import UPLOAD_RETRY_TIMEOUT_SECONDS, upload_client

_DUMMY_AUTH = {"host": "https://example.cloud.databricks.com", "token": "dummy"}


def test_upload_client_widens_the_retry_budget():
    """The widened budget must actually reach the client the uploads use.

    Regression guard: `retry_timeout_seconds` is NOT a WorkspaceClient.__init__
    kwarg in databricks-sdk 0.40 -- it is a Config field. Passing it as a kwarg
    raises TypeError on the very first upload run, which no other test in the
    suite would have caught.
    """
    w = upload_client(**_DUMMY_AUTH)
    assert w.config.retry_timeout_seconds == UPLOAD_RETRY_TIMEOUT_SECONDS
    assert UPLOAD_RETRY_TIMEOUT_SECONDS == 2 * 60 * 60


def test_upload_client_leaves_the_socket_timeout_at_the_sdk_default():
    """Only the retry budget is widened. `http_timeout_seconds` is the per-socket
    inactivity timeout (SDK default 60 s, `_base_client.py:95`); widening it turns
    a dead socket into a multi-hour hang and nothing implicated it."""
    w = upload_client(**_DUMMY_AUTH)
    assert w.config.http_timeout_seconds is None  # unset => SDK's own 60 s
