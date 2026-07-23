# tests/integration/test_webdav_live.py
import pytest

from opl.extraction.cnpj_source import (
    SHARE_TOKEN,
    WEBDAV_BASE,
    check_month_complete,
)
from opl.extraction.webdav import WebDavClient

pytestmark = pytest.mark.integration

# A month known to exist per F0 research (gapless 2023-05..2026-07).
_MONTH = "2026-06"


def _client():
    return WebDavClient(WEBDAV_BASE, SHARE_TOKEN)


def test_list_month_contains_lookup_tables():
    entries = _client().list_dir(_MONTH)
    names = {e.name for e in entries}
    assert "Qualificacoes.zip" in names
    assert "Empresas0.zip" in names


def test_download_tiny_lookup_file(tmp_path):
    client = _client()
    entry = next(e for e in client.list_dir(_MONTH) if e.name == "Qualificacoes.zip")
    out = client.download(entry.rel_path, tmp_path / "Qualificacoes.zip",
                         expected_size=entry.size)
    assert out.stat().st_size == entry.size > 0


def test_recorte_month_is_complete():
    from opl.extraction.cnpj_source import RECORTE_GROUPS
    ok, missing = check_month_complete(_client(), _MONTH, RECORTE_GROUPS)
    assert ok, f"missing recorte files in {_MONTH}: {missing}"
