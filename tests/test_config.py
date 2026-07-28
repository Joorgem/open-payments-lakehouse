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


def test_the_unzip_staging_dir_is_outside_every_dir_an_auto_loader_reads():
    """`landing_tmp` exists so a half-written file is never created where a stream
    can see it. Two source paths have to miss it: the estabelecimentos stream reads
    `landing_table(...)` with no pathGlobFilter, and the lookup stream reads
    `landing_cnpj_month(...)` recursively. Being outside the month root clears both
    without relying on a glob -- while staying inside `volume_root`, i.e. inside the
    one UC Volume, which is what lets os.replace rename out of it into the landing
    dir (a cross-filesystem replace raises EXDEV)."""
    staging = DEFAULT.landing_tmp("estabelecimentos", "2026-07")

    assert staging == "/Volumes/workspace/default/landing/_tmp/cnpj/2026-07/estabelecimentos"
    assert not staging.startswith(DEFAULT.landing_table("estabelecimentos", "2026-07"))
    assert not staging.startswith(DEFAULT.landing_cnpj_month("2026-07"))
    assert not staging.startswith(DEFAULT.landing_cnpj_root)
    assert staging.startswith(DEFAULT.volume_root)  # same Volume => same filesystem
    assert DEFAULT.landing_tmp("estabelecimentos").endswith("/2026-06/estabelecimentos")


def test_is_frozen():
    import dataclasses
    with __import__("pytest").raises(dataclasses.FrozenInstanceError):
        OplConfig().catalog = "other"  # type: ignore[misc]
