# tests/integration/test_landing_live.py
import pytest

from databricks.sdk import WorkspaceClient
from opl.extraction.cnpj_source import (
    SHARE_TOKEN,
    WEBDAV_BASE,
)
from opl.extraction.landing import LANDING_VOLUME_DIR, unzip_single, upload_to_volume
from opl.extraction.webdav import WebDavClient

pytestmark = pytest.mark.integration

_MONTH = "2026-06"


def test_download_unzip_and_land_lookup(tmp_path):
    wd = WebDavClient(WEBDAV_BASE, SHARE_TOKEN)
    entry = next(e for e in wd.list_dir(_MONTH) if e.name == "Qualificacoes.zip")
    zp = wd.download(entry.rel_path, tmp_path / "Qualificacoes.zip", expected_size=entry.size)
    inner = unzip_single(zp, tmp_path / "unz")
    assert inner.stat().st_size > 0

    w = WorkspaceClient(profile="opl-free")
    target = upload_to_volume(w, inner, f"{LANDING_VOLUME_DIR}/{_MONTH}")
    got = w.files.download(target).contents.read()
    assert got == inner.read_bytes()
