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

from opl.bronze.registry import UnknownTable, table_spec
from opl.config import DEFAULT
from opl.extraction import landing
from opl.extraction.cnpj_source import RECORTE_GROUPS
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
    uploads: list[tuple[str, str]] = []

    def fake_upload_to_volume(w, inner, volume_dir):
        seen.append(w)
        uploads.append((volume_dir, inner.name))
        return f"{volume_dir}/{inner.name}"

    monkeypatch.setattr(cli, "upload_to_volume", fake_upload_to_volume)
    client = FakeClient(present_names={"Cnaes.zip"})
    rc = cli.run(client, "2026-07", ["Cnaes"], str(tmp_path), upload=True)

    assert rc == 0
    assert seen == [sentinel]
    # This stub asserted the CLIENT and ignored `volume_dir` entirely, which is why
    # no test saw the target still being the month root after Task 8 moved the
    # lookups into `lookups/`. Asserted here too, at the only other call site.
    assert uploads == [
        (DEFAULT.landing_table(table_spec("lookup").subdir, "2026-07"), "Cnaes.TXT")
    ]


def _record_targets(monkeypatch) -> list[str]:
    targets: list[str] = []

    def fake_upload_to_volume(w, inner, volume_dir):
        targets.append(volume_dir)
        return f"{volume_dir}/{inner.name}"

    monkeypatch.setattr(cli, "upload_client", lambda **_auth: object())
    monkeypatch.setattr(cli, "upload_to_volume", fake_upload_to_volume)
    return targets


def test_the_lookups_land_in_the_registered_subdir_and_not_the_month_root(
        tmp_path, monkeypatch):
    """THE DEFECT THIS FILE MISSED. Task 8 moved the six lookup CSVs out of the
    month root into `lookups/` so `bronze_lookup_stream` could read its own
    directory with no `pathGlobFilter` -- and moved them with a one-off migration
    script, leaving this producer landing new months in the month root.

    Nothing would have failed: the files would land, the job would run, and
    `bronze_lookup_ingest` would report SUCCESS having ingested zero rows, because
    an empty source dir is indistinguishable from nothing-new-to-read. So the
    target is asserted against the registry AND against the month root by name --
    the second assertion is the regression, and the first is what makes it a
    registry answer rather than a literal that can drift from `spec.subdir`."""
    targets = _record_targets(monkeypatch)
    client = FakeClient(present_names={"Cnaes.zip", "Municipios.zip"})

    rc = cli.run(client, "2026-07", ["Cnaes", "Municipios"], str(tmp_path), upload=True)

    expected = DEFAULT.landing_table(table_spec("lookup").subdir, "2026-07")
    assert rc == 0
    assert targets == [expected, expected]
    assert DEFAULT.landing_cnpj_month("2026-07") not in targets
    assert expected.endswith("/2026-07/lookups")


def test_a_group_with_no_registered_table_is_refused_before_any_download(
        tmp_path, monkeypatch):
    """Simples: in `RECORTE_GROUPS`, and in no registry entry.

    Refusing is the decision. The alternative was to keep landing it in the month
    root, which is where nothing reads and where the six lookups' own defect lived
    -- and which would be landing several GB of inner CSV into a Free Edition
    Volume for a table no job can name.

    BEFORE the download, and asserted on `client.downloaded`: inside the loop the
    `except Exception` would have turned this into a per-file ERROR line after the
    bytes were already on the wire."""
    _record_targets(monkeypatch)
    client = FakeClient(present_names={"Simples.zip"})

    with pytest.raises(UnknownTable) as excinfo:
        cli.run(client, "2026-07", ["Simples"], str(tmp_path), upload=True)

    message = str(excinfo.value)
    assert "simples" in message
    assert "opl.bronze.registry" in message and "--no-upload" in message
    assert client.downloaded == []


def test_the_same_group_still_downloads_with_no_upload(tmp_path, monkeypatch):
    """The other half of the ruling above: the registry answers "where in the
    VOLUME", so a capture that lands nothing never asks it. That is what keeps ADR
    0003's full dev recorte -- Simples included -- downloadable."""
    def boom(**_auth):
        raise AssertionError("no WorkspaceClient may be built when --no-upload is set")

    monkeypatch.setattr(cli, "upload_client", boom)
    client = FakeClient(present_names={"Simples.zip"})

    assert cli.run(client, "2026-07", ["Simples"], str(tmp_path), upload=False) == 0
    assert client.downloaded == ["2026-07/Simples.zip"]


def test_a_zips_landed_group_is_refused_and_named_the_script_that_takes_it(
        tmp_path, monkeypatch):
    """The registry owns HOW a table's bytes reach the Volume, and this script is
    the LOCAL producer. Symmetric to `extract_giants.py` refusing a single-part
    group, and to `unzip_table.py` / `bronze_ingest.py` refusing anything that is
    not zips -- the refusal that was missing on the producer side.

    Before the fix this call landed a 6.7 GB `.ESTABELE` extract in the month root;
    routing it through the registry alone would have landed it in
    `estabelecimentos/`, which the estab ingest reads -- correct by accident, at
    three times the bytes and skipping the in-Volume unzip the flow expects."""
    _record_targets(monkeypatch)
    client = FakeClient(present_names={"Estabelecimentos0.zip"})

    with pytest.raises(ValueError) as excinfo:
        cli.run(client, "2026-07", ["Estabelecimentos"], str(tmp_path), upload=True)

    message = str(excinfo.value)
    assert "extract_giants.py" in message and "zips" in message
    assert client.downloaded == []


def test_main_maps_a_landing_refusal_to_the_usage_exit_code(monkeypatch, capsys):
    """`--groups Simples` is a usage mistake, and this repo hands an operator a
    message, not a traceback. rc=2 is what `_parse_groups`' unknown name already
    returns, and this is the same kind of event."""
    monkeypatch.setattr(cli, "upload_client", lambda **_auth: object())
    monkeypatch.setattr(
        cli, "WebDavClient",
        lambda *_a, **_kw: FakeClient(present_names={"Simples.zip"}),
    )
    monkeypatch.setattr(
        sys, "argv",
        ["extract_cnpj.py", "--month", "2026-07", "--groups", "Simples"],
    )

    assert cli.main() == 2
    assert "no bronze table is registered" in capsys.readouterr().out


def test_every_lookup_group_of_the_recorte_lands_in_the_one_lookup_subdir():
    """Six differently-named files, ONE landing dir -- the property the routing by
    filename suffix rests on. Read off the resolver rather than off the registry, so
    a per-group target reintroduced here (a `Cnaes/` dir of its own, say) fails:
    the lookup stream reads exactly one directory."""
    lookups = [g for g in RECORTE_GROUPS if g != "Simples"]
    assert len(lookups) == 6
    dirs = {cli._landing_dir(group, "2026-07") for group in lookups}
    assert dirs == {DEFAULT.landing_table(table_spec("lookup").subdir, "2026-07")}


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
