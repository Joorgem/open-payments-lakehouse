# tests/integration/test_landing_live.py
import pytest

from opl.bronze.registry import table_spec
from opl.config import DEFAULT
from opl.extraction.cnpj_source import (
    SHARE_TOKEN,
    WEBDAV_BASE,
)
from opl.extraction.landing import unzip_single, upload_client, upload_to_volume
from opl.extraction.webdav import WebDavClient

pytestmark = pytest.mark.integration

_MONTH = "2026-06"


def test_download_unzip_and_land_lookup(tmp_path):
    wd = WebDavClient(WEBDAV_BASE, SHARE_TOKEN)
    entry = next(e for e in wd.list_dir(_MONTH) if e.name == "Qualificacoes.zip")
    zp = wd.download(entry.rel_path, tmp_path / "Qualificacoes.zip", expected_size=entry.size)
    inner = unzip_single(zp, tmp_path / "unz")
    assert inner.stat().st_size > 0

    w = upload_client()
    # The lookup's REGISTERED landing dir, not the month root this used to write to.
    # It was the second producer the F1.4a review's finding 1 covers: run with live
    # credentials it PUT a real Qualificacoes CSV into `cnpj/<month>/`, where no
    # stream reads it and where `reclaim_landing` -- scoped to the table's landing
    # dir by construction -- can never take it back out again.
    target = upload_to_volume(
        w, inner, DEFAULT.landing_table(table_spec("lookup").subdir, _MONTH)
    )
    got = w.files.download(target).contents.read()
    assert got == inner.read_bytes()
