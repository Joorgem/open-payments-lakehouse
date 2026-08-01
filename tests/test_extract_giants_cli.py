# tests/test_extract_giants_cli.py
"""Unit tests for scripts/extract_giants.py's CLI wiring.

Pure-Python, no network, no credentials, no `.databrickscfg` profile: the
upload client factory is stubbed out and a FakeClient stands in for the real
WebDavClient.
"""
from __future__ import annotations

import importlib.util
import inspect
import zipfile
from pathlib import Path

import pytest

from opl.bronze.registry import UnknownTable, table_spec
from opl.config import DEFAULT
from opl.extraction import landing
from opl.extraction.giants import upload_zips
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


def test_the_zips_land_in_the_dir_the_unzip_task_reads_back(tmp_path, monkeypatch):
    """One field for one directory. This script spelled the zips dir
    `FILE_GROUPS[group]["table"]` while `unzip_table.py` spells it
    `landing_zips(spec.subdir, month)` -- identical for estabelecimentos and
    DIVERGENT for the lookup ("lookup" against "lookups"), so the agreement was a
    coincidence of the one entry F1.4b is about to copy-paste twice.

    Asserted against `spec.subdir` and against the FILE_GROUPS key by name, because
    the two are the same string for this table: what fails here is the SOURCE of the
    string, which is the only thing that can drift."""
    monkeypatch.setattr(cli, "upload_client", lambda **_auth: object())
    seen: list[str] = []

    def fake_upload_zips(w, paths, cfg, subdir, month):
        seen.append(cfg.landing_zips(subdir, month))
        return [f"/Volumes/x/{p.name}" for p in paths]

    monkeypatch.setattr(cli, "upload_zips", fake_upload_zips)
    client = FakeClient(present_names={"Estabelecimentos1.zip"})

    rc = cli.run(client, "2026-07", "Estabelecimentos", [1], str(tmp_path), upload=True)

    assert rc == 0
    spec = table_spec("estabelecimentos")
    assert seen == [DEFAULT.landing_zips(spec.subdir, "2026-07")]
    assert seen == [(
        "/Volumes/workspace/default/landing/cnpj/2026-07/zips/estabelecimentos"
    )]


def test_upload_zips_takes_the_registry_subdir_and_not_a_free_string():
    """The library half: `upload_zips` builds `zips/<subdir>` from what it is given,
    so the caller owning that string is what keeps it the registry's."""
    assert "subdir" in inspect.signature(upload_zips).parameters


def test_landing_zips_for_an_unregistered_table_is_refused_before_any_download(
        tmp_path, monkeypatch):
    """Landing zips for a group with no bronze table would put gigabytes in a Free
    Edition Volume under a directory name no `unzip_table.py` run can name -- the
    waste this branch reclaimed 16.7 GB of. Refused before the first byte.

    SIMPLES, because F1.4b registered Empresas and Socios and this test used to ride
    on their absence. The property is about a FILE_GROUPS entry with no registry
    entry, not about those two tables, and Simples is that case today: a real RFB
    group, a real contract in `cnpj_schemas.TABLES`, no bronze table. When Simples is
    registered, this test needs the next such group -- and if there is none, the
    property has no witness left and the test should be retired deliberately rather
    than repointed at something that does not demonstrate it."""
    # `pytest.fail`, not a stub object: "before the first byte" also means before a
    # workspace client is built. `run` resolves the subdir on its first statement and
    # constructs `upload_client()` four lines later, so a change that swapped those
    # two would leave a stub-returning version of this test green while the refusal
    # had moved to AFTER an authenticated connection was opened.
    monkeypatch.setattr(cli, "upload_client", lambda **_auth: pytest.fail("built"))
    monkeypatch.setattr(cli, "upload_zips", lambda *_a, **_kw: pytest.fail("uploaded"))
    client = FakeClient(present_names={"Simples0.zip"})

    with pytest.raises(UnknownTable, match="simples"):
        cli.run(client, "2026-07", "Simples", [0], str(tmp_path), upload=True)

    assert client.downloaded == []


def test_an_unregistered_table_still_downloads_with_upload_off(tmp_path, monkeypatch):
    """The registry answers "where in the VOLUME", so a download-only capture never
    asks it -- which is what keeps a group fetchable before anything registers its
    table. Simples for the reason given above: it is the unregistered group now that
    Empresas and Socios are entries."""
    monkeypatch.setattr(cli, "upload_client", lambda **_auth: pytest.fail("built"))
    client = FakeClient(present_names={"Simples0.zip"})

    assert cli.run(client, "2026-07", "Simples", [0], str(tmp_path), upload=False) == 0
    assert client.downloaded == ["2026-07/Simples0.zip"]


def test_parse_parts_defaults_to_every_part():
    assert cli._parse_parts(None, "Estabelecimentos") == list(range(10))


def test_parse_parts_rejects_out_of_range():
    with pytest.raises(ValueError, match="out of range"):
        cli._parse_parts("0,10", "Estabelecimentos")
