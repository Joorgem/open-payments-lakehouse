from pathlib import Path
from unittest import mock

from opl.config import DEFAULT
from opl.extraction.giants import download_parts, part_files, upload_zips


def test_config_landing_subdir_helpers():
    assert DEFAULT.landing_zips("estabelecimentos") == \
        "/Volumes/workspace/default/landing/cnpj/2026-06/zips/estabelecimentos"
    assert DEFAULT.landing_table("estabelecimentos") == \
        "/Volumes/workspace/default/landing/cnpj/2026-06/estabelecimentos"
    assert DEFAULT.landing_table("estabelecimentos", "2026-07").endswith(
        "/2026-07/estabelecimentos"
    )


def test_part_files_estabelecimentos():
    files = part_files("Estabelecimentos")
    assert len(files) == 10
    assert files[0] == "Estabelecimentos0.zip"
    assert files[9] == "Estabelecimentos9.zip"


def test_download_parts_selects_and_passes_manifest_size(tmp_path):
    client = mock.Mock()
    entry0 = mock.Mock()
    entry0.name = "Estabelecimentos1.zip"
    entry0.size = 111
    entry0.is_dir = False
    entry1 = mock.Mock()
    entry1.name = "Estabelecimentos2.zip"
    entry1.size = 222
    entry1.is_dir = False
    client.list_dir.return_value = [entry0, entry1]
    client.download.side_effect = lambda rel, dest, expected_size: Path(dest)

    out = download_parts(client, "2026-06", "Estabelecimentos", tmp_path, parts=[1, 2])
    assert [p.name for p in out] == ["Estabelecimentos1.zip", "Estabelecimentos2.zip"]
    sizes = [c.kwargs["expected_size"] for c in client.download.call_args_list]
    assert sizes == [111, 222]


def test_download_parts_missing_on_server_raises(tmp_path):
    client = mock.Mock()
    client.list_dir.return_value = []  # nothing on the server
    import pytest
    with pytest.raises(FileNotFoundError):
        download_parts(client, "2026-06", "Estabelecimentos", tmp_path, parts=[1])


def test_upload_zips_targets_zips_subdir(tmp_path, fake_workspace_client):
    f = tmp_path / "Estabelecimentos1.zip"
    f.write_bytes(b"zip")
    # The shared double (not a bare Mock) so this still fails if upload_to_volume
    # ever stops verifying the landed size or starts deleting a good object.
    w = fake_workspace_client(f.stat().st_size)
    target = (
        "/Volumes/workspace/default/landing/cnpj/2026-06/zips/estabelecimentos"
        "/Estabelecimentos1.zip"
    )

    assert upload_zips(w, [f], DEFAULT, "estabelecimentos", "2026-06") == [target]
    assert w.files.uploads == [(target, True)]
    assert w.files.metadata_calls == [target]
    assert w.files.deletes == []
