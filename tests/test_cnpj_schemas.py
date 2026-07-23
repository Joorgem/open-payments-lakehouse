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


def test_full_column_order_golden():
    """Golden-copy lock on the FULL ordered column layouts.

    Column order is authoritative (headerless CSVs) and propagates through
    the entire pipeline. Unlike the count/prefix checks above, this test
    catches an accidental reorder (e.g. a swap of two adjacent columns)
    that would otherwise pass silently.
    """
    assert TABLES["empresas"] == [
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel",
    ]
    assert TABLES["estabelecimentos"] == [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ]
    assert TABLES["socios"] == [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio_razao_social",
        "cpf_cnpj_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_do_representante",
        "qualificacao_representante_legal",
        "faixa_etaria",
    ]
    assert TABLES["simples"] == [
        "cnpj_basico",
        "opcao_pelo_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ]
    assert TABLES["lookup"] == ["codigo", "descricao"]


def test_all_file_groups_present_and_correctly_mapped():
    assert set(FILE_GROUPS.keys()) == {
        "Empresas",
        "Estabelecimentos",
        "Socios",
        "Simples",
        "Cnaes",
        "Motivos",
        "Municipios",
        "Naturezas",
        "Paises",
        "Qualificacoes",
    }

    for group_name, table_name in (
        ("Empresas", "empresas"),
        ("Estabelecimentos", "estabelecimentos"),
        ("Socios", "socios"),
    ):
        assert FILE_GROUPS[group_name]["parts"] == 10
        assert FILE_GROUPS[group_name]["table"] == table_name

    assert FILE_GROUPS["Simples"]["parts"] == 1
    assert FILE_GROUPS["Simples"]["table"] == "simples"

    for lookup_group in (
        "Cnaes",
        "Motivos",
        "Municipios",
        "Naturezas",
        "Paises",
        "Qualificacoes",
    ):
        assert FILE_GROUPS[lookup_group]["parts"] == 1
        assert FILE_GROUPS[lookup_group]["table"] == "lookup"


def test_csv_dialect_full():
    assert CSV_DIALECT["quotechar"] == '"'
    assert CSV_DIALECT["date_format"] == "%Y%m%d"
    assert CSV_DIALECT["decimal"] == ","
