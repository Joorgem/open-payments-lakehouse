# tests/test_landing_unzip.py
import zipfile
from types import SimpleNamespace

import pytest

from opl.extraction.landing import UploadIntegrityError, unzip_single, upload_to_volume


class _FakeFilesApi:
    """Stands in for ``WorkspaceClient.files``: records the PUT and reports a
    controllable remote ``content_length``. No network, no Databricks."""

    def __init__(self, remote_size: int | None):
        self.remote_size = remote_size
        self.uploads: list[tuple[str, bool]] = []
        self.metadata_calls: list[str] = []

    def upload(self, file_path, contents, overwrite=False):
        contents.read()  # the real API consumes the handle
        self.uploads.append((file_path, overwrite))

    def get_metadata(self, file_path):
        self.metadata_calls.append(file_path)
        return SimpleNamespace(content_length=self.remote_size)


def _fake_workspace_client(remote_size: int | None):
    return SimpleNamespace(files=_FakeFilesApi(remote_size))


def test_upload_to_volume_returns_target_when_remote_size_matches(tmp_path):
    src = tmp_path / "Estabelecimentos1.zip"
    src.write_bytes(b"z" * 4096)
    w = _fake_workspace_client(4096)

    target = upload_to_volume(w, src, "/Volumes/workspace/default/landing/zips/")

    assert target == "/Volumes/workspace/default/landing/zips/Estabelecimentos1.zip"
    assert w.files.uploads == [(target, True)]
    assert w.files.metadata_calls == [target]


def test_upload_to_volume_raises_when_the_remote_object_is_short(tmp_path):
    """The F1.3 incident: w.files.upload() returned without error but landed
    273,373,127 of 341,333,959 bytes, and the corruption only surfaced later as
    an EINVAL seek inside zipfile. The upload itself must fail loudly instead."""
    src = tmp_path / "Estabelecimentos1.zip"
    src.write_bytes(b"z" * 4096)
    w = _fake_workspace_client(3072)

    with pytest.raises(UploadIntegrityError) as excinfo:
        upload_to_volume(w, src, "/Volumes/workspace/default/landing/zips")

    message = str(excinfo.value)
    assert "Estabelecimentos1.zip" in message
    assert "4096" in message and "3072" in message
    assert "1024" in message  # the missing-bytes delta


def test_upload_to_volume_raises_when_content_length_is_missing(tmp_path):
    """No content-length means the write could not be confirmed -- that is a
    verification failure, not a pass."""
    src = tmp_path / "Estabelecimentos1.zip"
    src.write_bytes(b"z" * 4096)
    w = _fake_workspace_client(None)

    with pytest.raises(UploadIntegrityError, match="Estabelecimentos1.zip"):
        upload_to_volume(w, src, "/Volumes/workspace/default/landing/zips")


def test_unzip_single_extracts_inner_file(tmp_path):
    zp = tmp_path / "Qualificacoes.zip"
    with zipfile.ZipFile(zp, "w") as z:
        z.writestr("K3241.D60711.QUALSCSV", b'"01";"Administrador"\r\n')
    out = unzip_single(zp, tmp_path / "out")
    assert out.read_bytes().startswith(b'"01"')


def test_unzip_single_rejects_multimember(tmp_path):
    zp = tmp_path / "bad.zip"
    with zipfile.ZipFile(zp, "w") as z:
        z.writestr("a", b"x")
        z.writestr("b", b"y")
    with pytest.raises(ValueError):
        unzip_single(zp, tmp_path / "out")
