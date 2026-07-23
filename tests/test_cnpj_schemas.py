# tests/test_cnpj_schemas.py
from opl.contracts.cnpj_schemas import (
    CNPJ_KEY_COLUMNS,
    CSV_DIALECT,
    FILE_GROUPS,
    TABLES,
    columns_for,
)


def test_column_counts_match_official_layout():
    assert len(TABLES["empresas"]) == 7
    assert len(TABLES["estabelecimentos"]) == 30
    assert len(TABLES["socios"]) == 11
    assert len(TABLES["simples"]) == 7
    assert TABLES["lookup"] == ["codigo", "descricao"]


def test_key_columns_and_order():
    assert TABLES["empresas"][0] == "cnpj_basico"
    assert TABLES["estabelecimentos"][:3] == ["cnpj_basico", "cnpj_ordem", "cnpj_dv"]
    assert "cnae_fiscal_secundaria" in TABLES["estabelecimentos"]
    assert "cnpj_basico" in CNPJ_KEY_COLUMNS


def test_csv_dialect_is_cp1252_semicolon_headerless():
    assert CSV_DIALECT["encoding"] == "cp1252"
    assert CSV_DIALECT["sep"] == ";"
    assert CSV_DIALECT["header"] is None


def test_file_groups_partition_counts():
    assert FILE_GROUPS["Empresas"]["parts"] == 10
    assert FILE_GROUPS["Empresas"]["table"] == "empresas"
    assert FILE_GROUPS["Cnaes"]["parts"] == 1
    assert FILE_GROUPS["Cnaes"]["table"] == "lookup"


def test_columns_for_unknown_raises():
    import pytest
    with pytest.raises(KeyError):
        columns_for("nope")
