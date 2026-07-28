# tests/conftest.py
"""Shared hermetic doubles for the Databricks Files API.

Every test that exercises an upload path uses this double rather than a bare
``mock.Mock()``: a Mock auto-creates whatever attribute is asked of it, so a
test built on one keeps passing after ``upload_to_volume`` stops verifying the
landed size or stops cleaning up. This double records those calls, so their
absence is visible.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest


class FakeFilesApi:
    """Stands in for ``WorkspaceClient.files``: records the PUT, the verification
    GET and the cleanup DELETE, and reports a controllable remote
    ``content_length``. ``upload_error`` / ``metadata_error`` / ``delete_error``
    inject failures. No network, no Databricks."""

    def __init__(
        self,
        remote_size: int | None,
        upload_error: Exception | None = None,
        metadata_error: Exception | None = None,
        delete_error: Exception | None = None,
    ):
        self.remote_size = remote_size
        self.upload_error = upload_error
        self.metadata_error = metadata_error
        self.delete_error = delete_error
        self.uploads: list[tuple[str, bool]] = []
        self.metadata_calls: list[str] = []
        self.deletes: list[str] = []

    def upload(self, file_path, contents, overwrite=False):
        contents.read()  # the real API consumes the handle
        self.uploads.append((file_path, overwrite))
        if self.upload_error is not None:
            raise self.upload_error

    def get_metadata(self, file_path):
        self.metadata_calls.append(file_path)
        if self.metadata_error is not None:
            raise self.metadata_error
        return SimpleNamespace(content_length=self.remote_size)

    def delete(self, file_path):
        self.deletes.append(file_path)
        if self.delete_error is not None:
            raise self.delete_error


@pytest.fixture
def fake_workspace_client():
    """Factory: ``fake_workspace_client(remote_size, **injected_errors)`` returns
    an object exposing only ``.files``, which is all the upload paths touch."""

    def _make(
        remote_size: int | None,
        upload_error: Exception | None = None,
        metadata_error: Exception | None = None,
        delete_error: Exception | None = None,
    ):
        return SimpleNamespace(
            files=FakeFilesApi(
                remote_size,
                upload_error=upload_error,
                metadata_error=metadata_error,
                delete_error=delete_error,
            )
        )

    return _make
