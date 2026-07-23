# tests/test_cnpj_source.py
from opl.extraction.cnpj_source import (
    RECORTE_GROUPS,
    check_month_complete,
    expected_files,
)
from opl.extraction.webdav import FileEntry


def test_expected_files_expands_parts():
    files = expected_files(["Empresas", "Cnaes"])
    assert "Empresas0.zip" in files and "Empresas9.zip" in files
    assert files.count("Empresas0.zip") == 1
    assert "Cnaes.zip" in files
    assert len([f for f in files if f.startswith("Empresas")]) == 10


def test_check_month_complete_detects_missing():
    class FakeClient:
        def list_dir(self, rel):
            return [FileEntry("Cnaes.zip", f"{rel}/Cnaes.zip", 100, False)]
    ok, missing = check_month_complete(FakeClient(), "2026-07", ["Cnaes", "Motivos"])
    assert ok is False
    assert "Motivos.zip" in missing and "Cnaes.zip" not in missing


def test_check_month_complete_all_present():
    class FakeClient:
        def list_dir(self, rel):
            return [
                FileEntry("Cnaes.zip", f"{rel}/Cnaes.zip", 100, False),
                FileEntry("Motivos.zip", f"{rel}/Motivos.zip", 100, False),
            ]
    ok, missing = check_month_complete(FakeClient(), "2026-07", ["Cnaes", "Motivos"])
    assert ok is True and missing == []


def test_recorte_is_small_tables_only():
    # recorte must not pull the 10-part giants
    assert "Empresas" not in RECORTE_GROUPS
    assert "Estabelecimentos" not in RECORTE_GROUPS
    assert "Cnaes" in RECORTE_GROUPS
