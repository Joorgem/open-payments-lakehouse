# tests/test_config.py
from opl.config import DEFAULT, OplConfig


def test_defaults_match_free_edition_layout():
    assert DEFAULT.catalog == "workspace"
    assert DEFAULT.schema == "default"
    assert DEFAULT.volume_root == "/Volumes/workspace/default/landing"
    assert DEFAULT.landing_cnpj_root == "/Volumes/workspace/default/landing/cnpj"


def test_month_path_and_table_helpers():
    assert DEFAULT.landing_cnpj_month() == (
        "/Volumes/workspace/default/landing/cnpj/2026-06"
    )
    assert DEFAULT.landing_cnpj_month("2026-07") == (
        "/Volumes/workspace/default/landing/cnpj/2026-07"
    )
    assert DEFAULT.table("bronze_cnpj_lookup") == (
        "workspace.default.bronze_cnpj_lookup"
    )


def test_is_frozen():
    import dataclasses
    with __import__("pytest").raises(dataclasses.FrozenInstanceError):
        OplConfig().catalog = "other"  # type: ignore[misc]
