# tests/conftest.py
"""Shared hermetic doubles for the Databricks Files API, plus the Spark session.

Every test that exercises an upload path uses this double rather than a bare
``mock.Mock()``: a Mock auto-creates whatever attribute is asked of it, so a
test built on one keeps passing after ``upload_to_volume`` stops verifying the
landed size or stops cleaning up. This double records those calls, so their
absence is visible.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest


def _pin_pyspark_interpreter() -> None:
    """Make Spark's Python workers run the interpreter the driver runs.

    Measured, not theoretical: without this the workers launch from the first
    ``python`` on PATH (3.14 on this machine) against a 3.12 driver, and every
    Spark test dies inside py4j with ``AssertionError: SRE module mismatch`` --
    a message that names neither Python nor PATH. It has to happen BEFORE a
    SparkContext exists, because that is when the worker command is captured.

    Called at import as well as from the fixture: several older test modules
    build their own session via ``opl.spark.local_session`` and never see the
    fixture, and they were failing this way before this file pinned it.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


_pin_pyspark_interpreter()


@pytest.fixture(scope="session")
def spark():
    """The one local Delta-enabled session the Spark tests share.

    Wraps ``opl.spark.local_session`` rather than building a second session:
    that factory documents itself as mirroring what Databricks applies on DBR
    16.4, and a copy of its config here would be a copy that drifts. Imported
    inside the fixture so that collecting the Spark-free tests does not require
    pyspark to be installed. Task 10 hardens this.
    """
    _pin_pyspark_interpreter()
    from opl.spark import local_session

    session = local_session("test-suite")
    yield session
    session.stop()


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
