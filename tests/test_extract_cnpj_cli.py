# tests/test_extract_cnpj_cli.py
"""Unit tests for scripts/extract_cnpj.py's run()/main() orchestration.

Pure-Python, no network, no JVM, no Docker: a FakeClient stands in for the
real WebDavClient (implements list_dir + download only), and the upload client
factory is stubbed out so no real WorkspaceClient is ever constructed.
"""
from __future__ import annotations

import importlib.util
import sys
import zipfile
from pathlib import Path

import pytest

from opl.extraction import landing
from opl.extraction.webdav import FileEntry

_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "extract_cnpj.py"
_spec = importlib.util.spec_from_file_location("extract_cnpj_cli", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)


class FakeClient:
    """Stands in for WebDavClient: no network, writes a valid 1-member zip
    on download() so unzip_single() has something real to unpack."""

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
        inner_name = Path(rel_path).stem + ".TXT"
        with zipfile.ZipFile(dest, "w") as z:
            z.writestr(inner_name, "dummy content")
        self.downloaded.append(rel_path)
        return dest


def test_run_complete_month_upload_false_lands_all(tmp_path):
    groups = ["Cnaes", "Motivos"]
    client = FakeClient(present_names={"Cnaes.zip", "Motivos.zip"})
    rc = cli.run(client, "2026-07", groups, str(tmp_path), upload=False)
    assert rc == 0
    assert sorted(client.downloaded) == ["2026-07/Cnaes.zip", "2026-07/Motivos.zip"]
    unz_dir = tmp_path / "2026-07" / "unz"
    assert sorted(p.name for p in unz_dir.iterdir()) == ["Cnaes.TXT", "Motivos.TXT"]


def test_upload_uses_the_shared_widened_timeout_client(tmp_path, monkeypatch):
    """This path uploads the UNCOMPRESSED inner CSV (Simples' is several GB), so
    it needs the widened retry budget even more than the giants path does -- it
    used to build a bare WorkspaceClient carrying the SDK's 300 s default."""
    assert cli.upload_client is landing.upload_client

    sentinel = object()
    monkeypatch.setattr(cli, "upload_client", lambda **_auth: sentinel)
    seen: list[object] = []

    def fake_upload_to_volume(w, inner, volume_dir):
        seen.append(w)
        return f"{volume_dir}/{inner.name}"

    monkeypatch.setattr(cli, "upload_to_volume", fake_upload_to_volume)
    client = FakeClient(present_names={"Cnaes.zip"})
    rc = cli.run(client, "2026-07", ["Cnaes"], str(tmp_path), upload=True)

    assert rc == 0
    assert seen == [sentinel]


def test_run_without_upload_builds_no_client(tmp_path, monkeypatch):
    def boom(**_auth):
        raise AssertionError("no WorkspaceClient may be built when --no-upload is set")

    monkeypatch.setattr(cli, "upload_client", boom)
    client = FakeClient(present_names={"Cnaes.zip"})
    assert cli.run(client, "2026-07", ["Cnaes"], str(tmp_path), upload=False) == 0


def test_run_missing_expected_file_returns_1(tmp_path):
    groups = ["Cnaes", "Motivos"]
    # Motivos.zip is not present on the share -> SKIP -> non-zero exit.
    client = FakeClient(present_names={"Cnaes.zip"})
    rc = cli.run(client, "2026-07", groups, str(tmp_path), upload=False)
    assert rc == 1
    assert client.downloaded == ["2026-07/Cnaes.zip"]


def test_run_require_complete_bails_out_without_downloading(tmp_path):
    groups = ["Cnaes", "Motivos"]
    client = FakeClient(present_names={"Cnaes.zip"})
    rc = cli.run(
        client, "2026-07", groups, str(tmp_path), upload=False, require_complete=True
    )
    assert rc == 2
    assert client.downloaded == []  # no download attempted


def test_run_calls_list_dir_exactly_once(tmp_path):
    groups = ["Cnaes"]
    client = FakeClient(present_names={"Cnaes.zip"})
    calls = {"n": 0}
    original_list_dir = client.list_dir

    def counting_list_dir(month):
        calls["n"] += 1
        return original_list_dir(month)

    client.list_dir = counting_list_dir
    rc = cli.run(client, "2026-07", groups, str(tmp_path), upload=False)
    assert rc == 0
    assert calls["n"] == 1


def test_parse_groups_returns_none_for_default():
    assert cli._parse_groups(None) is None


def test_parse_groups_strips_and_drops_empties():
    assert cli._parse_groups(" Cnaes , Motivos ,, ") == ["Cnaes", "Motivos"]


def test_parse_groups_rejects_unknown_name():
    with pytest.raises(ValueError, match="Bogus"):
        cli._parse_groups("Cnaes,Bogus")


def test_main_invalid_groups_returns_2(monkeypatch, capsys):
    monkeypatch.setattr(
        sys, "argv",
        ["extract_cnpj.py", "--month", "2026-07", "--groups", "Cnaes,Bogus"],
    )
    rc = cli.main()
    assert rc == 2
    captured = capsys.readouterr()
    assert "Bogus" in captured.out
