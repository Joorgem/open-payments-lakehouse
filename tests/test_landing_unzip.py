# tests/test_landing_unzip.py
import zipfile
from types import SimpleNamespace

import pytest

from databricks.sdk.errors import NotFound
from opl.extraction import landing
from opl.extraction.landing import UploadIntegrityError, unzip_single, upload_to_volume


class _FakeFilesApi:
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


def _fake_workspace_client(
    remote_size: int | None,
    upload_error: Exception | None = None,
    metadata_error: Exception | None = None,
    delete_error: Exception | None = None,
):
    return SimpleNamespace(
        files=_FakeFilesApi(
            remote_size,
            upload_error=upload_error,
            metadata_error=metadata_error,
            delete_error=delete_error,
        )
    )


def test_upload_to_volume_returns_target_when_remote_size_matches(tmp_path):
    src = tmp_path / "Estabelecimentos1.zip"
    src.write_bytes(b"z" * 4096)
    w = _fake_workspace_client(4096)

    target = upload_to_volume(w, src, "/Volumes/workspace/default/landing/zips/")

    assert target == "/Volumes/workspace/default/landing/zips/Estabelecimentos1.zip"
    assert w.files.uploads == [(target, True)]
    assert w.files.metadata_calls == [target]
    assert w.files.deletes == []  # a good object must never be cleaned up


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


def test_upload_to_volume_deletes_the_short_object_before_raising(tmp_path):
    """A short-written object that stays in the Volume reads as a valid zip until
    z.open(). Leave nothing behind: the next run must see a missing file, not a
    plausible-looking corrupt one."""
    src = tmp_path / "Estabelecimentos1.zip"
    src.write_bytes(b"z" * 4096)
    w = _fake_workspace_client(3072)

    with pytest.raises(UploadIntegrityError):
        upload_to_volume(w, src, "/Volumes/workspace/default/landing/zips")

    assert w.files.deletes == ["/Volumes/workspace/default/landing/zips/Estabelecimentos1.zip"]


def test_upload_to_volume_deletes_and_reraises_when_the_put_itself_fails(tmp_path):
    """Estabelecimentos3.zip (366,824,247 B) died with `Timed out after 0:05:00`
    mid-PUT. A timed-out PUT may leave a partial object, so clean up -- but the
    caller must still see the original TimeoutError, not UploadIntegrityError."""
    src = tmp_path / "Estabelecimentos3.zip"
    src.write_bytes(b"z" * 4096)
    boom = TimeoutError("Timed out after 0:05:00")
    w = _fake_workspace_client(4096, upload_error=boom)

    with pytest.raises(TimeoutError) as excinfo:
        upload_to_volume(w, src, "/Volumes/workspace/default/landing/zips")

    assert excinfo.value is boom
    assert not isinstance(excinfo.value, UploadIntegrityError)
    assert w.files.deletes == ["/Volumes/workspace/default/landing/zips/Estabelecimentos3.zip"]
    assert w.files.metadata_calls == []  # nothing to verify once the PUT blew up


def test_upload_to_volume_deletes_and_reraises_when_the_verification_call_fails(tmp_path):
    """The verification GET is as fallible as the PUT: a transient 503 there
    leaves an object of unknown length in the Volume. Unverified is not verified
    -- the cleanup must cover the GET too, and the original error must survive."""
    src = tmp_path / "Estabelecimentos3.zip"
    src.write_bytes(b"z" * 4096)
    boom = OSError("503 Service Unavailable")
    w = _fake_workspace_client(4096, metadata_error=boom)

    with pytest.raises(OSError) as excinfo:
        upload_to_volume(w, src, "/Volumes/workspace/default/landing/zips")

    assert excinfo.value is boom
    assert not isinstance(excinfo.value, UploadIntegrityError)
    assert w.files.deletes == ["/Volumes/workspace/default/landing/zips/Estabelecimentos3.zip"]


def test_upload_to_volume_does_not_delete_when_the_local_file_cannot_be_opened(tmp_path):
    """A purely local failure must not touch the remote object. On Windows an AV
    scanner or a stray handle raises PermissionError on open(); if that happened
    on a re-run of an already-correctly-landed part, cleanup would delete a good
    object that this process never wrote a byte to."""
    src = tmp_path / "Estabelecimentos1.zip"
    src.write_bytes(b"z" * 4096)
    w = _fake_workspace_client(4096)

    def boom(*_a, **_kw):
        raise PermissionError(13, "The process cannot access the file")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(landing, "open", boom, raising=False)
        with pytest.raises(PermissionError):
            upload_to_volume(w, src, "/Volumes/workspace/default/landing/zips")

    assert w.files.uploads == []
    assert w.files.deletes == []  # nothing was PUT, so nothing may be deleted


def test_upload_to_volume_surfaces_the_integrity_error_when_cleanup_fails(tmp_path, capsys):
    """A cleanup DELETE that itself fails must not mask the real problem -- but
    it must also not let an operator believe the corrupt object was removed."""
    src = tmp_path / "Estabelecimentos1.zip"
    src.write_bytes(b"z" * 4096)
    w = _fake_workspace_client(3072, delete_error=OSError("503 Service Unavailable"))

    with pytest.raises(UploadIntegrityError, match="short-written"):
        upload_to_volume(w, src, "/Volumes/workspace/default/landing/zips")

    assert w.files.deletes == ["/Volumes/workspace/default/landing/zips/Estabelecimentos1.zip"]
    out = capsys.readouterr().out
    assert "delete FAILED" in out and "503 Service Unavailable" in out


def test_upload_to_volume_reports_the_expected_404_as_nothing_to_delete(tmp_path, capsys):
    """A clean failure leaves no object at all, so the cleanup DELETE 404s. That
    is the expected case, not a cleanup failure, and must read as such."""
    src = tmp_path / "Estabelecimentos1.zip"
    src.write_bytes(b"z" * 4096)
    w = _fake_workspace_client(3072, delete_error=NotFound("File not found"))

    with pytest.raises(UploadIntegrityError, match="short-written"):
        upload_to_volume(w, src, "/Volumes/workspace/default/landing/zips")

    out = capsys.readouterr().out
    assert "nothing to delete" in out
    assert "FAILED" not in out


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
