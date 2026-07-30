# tests/test_landing_client.py
"""Unit tests for the upload WorkspaceClient factory.

Hermetic: no network, no credentials, no `.databrickscfg` profile -- the client
is built from an explicit dummy host/token.

That used to be true for free. It is not any more: since databricks-sdk 0.123.0
`Config.__init__` ends with `_resolve_host_metadata()`, which GETs
`{host}/.well-known/databricks-config` to discover account/workspace ids. Against
a dummy host that call cannot succeed, and because it is wrapped in the SDK's
`retried()` it keeps trying until the config's retry budget is spent -- so these
two tests simply HUNG when the SDK was upgraded (measured: `pytest` killed at
120 s, with the traceback parked in `clock.sleep` under
`config.py:_resolve_host_metadata`). The `_no_host_metadata_probe` fixture below
neutralises the probe, which is the honest fix for a unit test: a hermetic test
must not depend on DNS. `upload_client` bounds the same probe in production by a
different route -- see its docstring.
"""
from __future__ import annotations

import pytest

from databricks.sdk.config import Config
from opl.extraction.landing import UPLOAD_RETRY_TIMEOUT_SECONDS, upload_client

_DUMMY_AUTH = {"host": "https://example.cloud.databricks.com", "token": "dummy"}


@pytest.fixture(autouse=True)
def _no_host_metadata_probe(monkeypatch):
    """Stop `Config.__init__` reaching the network. See the module docstring."""
    monkeypatch.setattr(Config, "_resolve_host_metadata", lambda self: None)


def test_upload_client_widens_the_retry_budget():
    """The widened budget must actually reach the client the uploads use.

    Regression guard: `retry_timeout_seconds` is NOT a WorkspaceClient.__init__
    kwarg -- it is a Config field. That held for the old 0.40 pin and still holds
    on 0.123.0 (`__init__` takes no **kwargs and exposes no retry/timeout
    parameter), so no SDK upgrade has retired the Config hop. Passing it as a
    kwarg raises TypeError on the very first upload run, which no other test in
    the suite would have caught -- and this test is the only thing standing
    between a future "simplify this away" and that failure.
    """
    w = upload_client(**_DUMMY_AUTH)
    assert w.config.retry_timeout_seconds == UPLOAD_RETRY_TIMEOUT_SECONDS
    assert UPLOAD_RETRY_TIMEOUT_SECONDS == 30 * 60


def test_upload_client_keeps_the_widened_budget_off_the_discovery_probe():
    """The budget must be raised AFTER `Config.__init__`, not passed into it.

    `_resolve_host_metadata` builds its probe client from
    `self.retry_timeout_seconds`, so handing the upload budget to `Config(...)`
    would let an unreachable host block `upload_client()` for the whole budget
    before falling back. Constructing at the SDK default and raising afterwards
    is what keeps that probe on the SDK's own 300 s bound. This asserts the
    ordering directly: whatever `Config.__init__` saw, it was not 30 minutes.
    """
    seen: list[object] = []
    real_init = Config.__init__

    def spy(self, *args, **kwargs):
        real_init(self, *args, **kwargs)
        seen.append(self.retry_timeout_seconds)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(Config, "__init__", spy)
        w = upload_client(**_DUMMY_AUTH)

    assert seen, "Config.__init__ was never called -- the spy missed the path"
    assert all(v != UPLOAD_RETRY_TIMEOUT_SECONDS for v in seen), (
        f"the widened budget reached Config.__init__ ({seen}), so the host-metadata "
        "probe would inherit it"
    )
    assert w.config.retry_timeout_seconds == UPLOAD_RETRY_TIMEOUT_SECONDS


def test_upload_client_leaves_the_socket_timeout_at_the_sdk_default():
    """Only the retry budget is widened. `http_timeout_seconds` is the per-socket
    inactivity timeout (SDK default 60 s, `_base_client.py:95`); widening it turns
    a dead socket into a multi-hour hang and nothing implicated it."""
    w = upload_client(**_DUMMY_AUTH)
    assert w.config.http_timeout_seconds is None  # unset => SDK's own 60 s
