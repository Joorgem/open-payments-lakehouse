# tests/test_extract_giants_cli.py
"""Unit tests for scripts/extract_giants.py's CLI wiring.

Pure-Python, no network, no credentials, no `.databrickscfg` profile: the
upload client factory is stubbed out and a FakeClient stands in for the real
WebDavClient.
"""
from __future__ import annotations

import importlib.util
import zipfile
from pathlib import Path

import pytest

from opl.extraction import landing
from opl.extraction.webdav import FileEntry

_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "extract_giants.py"
_spec = importlib.util.spec_from_file_location("extract_giants_cli", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

class FakeClient:
    """Stands in for WebDavClient: no network, writes a valid 1-member zip."""

    def __init__(self, present_names: set[str]):
        self.present_names = present_names
        self.downloaded: list[str] = []

    def list_dir(self, month: str) -> list[FileEntry]:
        return [
            FileEntry(name=n, rel_path=f"{month}/{n}", size=100, is_dir=False)
            for n in self.present_names
        ]

    def download(self, rel_path: str, dest: Path, expected_size: int | None = None) -> Path:
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(dest, "w") as z:
            z.writestr(Path(rel_path).stem + ".CSV", "dummy content")
        self.downloaded.append(rel_path)
        return dest


def test_upload_uses_the_shared_widened_timeout_client(tmp_path, monkeypatch):
    """The uploading path must go through the library factory, not a bare
    WorkspaceClient: the factory is where the widened retry budget lives."""
    assert cli.upload_client is landing.upload_client

    sentinel = object()
    monkeypatch.setattr(cli, "upload_client", lambda **_auth: sentinel)
    seen: list[object] = []

    def fake_upload_zips(w, paths, cfg, table, month):
        seen.append(w)
        return [f"/Volumes/x/{p.name}" for p in paths]

    monkeypatch.setattr(cli, "upload_zips", fake_upload_zips)
    client = FakeClient(present_names={"Estabelecimentos1.zip"})
    rc = cli.run(client, "2026-07", "Estabelecimentos", [1], str(tmp_path), upload=True)

    assert rc == 0
    assert seen == [sentinel]


def test_run_without_upload_builds_no_client(tmp_path, monkeypatch):
    def boom(**_auth):
        raise AssertionError("no WorkspaceClient may be built when --upload is off")

    monkeypatch.setattr(cli, "upload_client", boom)
    client = FakeClient(present_names={"Estabelecimentos1.zip"})
    rc = cli.run(client, "2026-07", "Estabelecimentos", [1], str(tmp_path), upload=False)
    assert rc == 0
    assert client.downloaded == ["2026-07/Estabelecimentos1.zip"]


def test_summary_reports_the_failure_count(tmp_path, monkeypatch, capsys):
    """A run whose every upload died printed `1/1 downloaded, 0 uploaded` -- the
    same line a --no-upload run prints. The failure count has to be in it."""
    monkeypatch.setattr(cli, "upload_client", lambda **_auth: object())

    def boom(*_a, **_kw):
        raise TimeoutError("Timed out after 0:05:00")

    monkeypatch.setattr(cli, "upload_zips", boom)
    client = FakeClient(present_names={"Estabelecimentos1.zip"})
    rc = cli.run(client, "2026-07", "Estabelecimentos", [1], str(tmp_path), upload=True)

    assert rc == 1
    assert "done: 1/1 downloaded, 0 uploaded (1 failed)" in capsys.readouterr().out


def test_parse_parts_defaults_to_every_part():
    assert cli._parse_parts(None, "Estabelecimentos") == list(range(10))


def test_parse_parts_rejects_out_of_range():
    with pytest.raises(ValueError, match="out of range"):
        cli._parse_parts("0,10", "Estabelecimentos")
