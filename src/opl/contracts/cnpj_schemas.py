"""Versioned, headerless column layouts for the Receita Federal CNPJ open data.
Column order is authoritative (files have NO header) and was verified against
the official RFB layout PDF + real downloaded data (see
docs/superpowers/research/cnpj-ingestion-facts.md). All CNPJ key columns are
strings (alphanumeric CNPJ effective 2026-07-31)."""

CSV_DIALECT = {
    "encoding": "cp1252",
    "sep": ";",
    "quotechar": '"',
    "header": None,
    "date_format": "%Y%m%d",
}

TABLES: dict[str, list[str]] = {
    "empresas": [
        "cnpj_basico", "razao_social", "natureza_juridica",
        "qualificacao_responsavel", "capital_social", "porte_empresa",
        "ente_federativo_responsavel",
    ],
    "estabelecimentos": [
        "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
        "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
        "motivo_situacao_cadastral", "nome_cidade_exterior", "pais",
        "data_inicio_atividade", "cnae_fiscal_principal",
        "cnae_fiscal_secundaria", "tipo_logradouro", "logradouro", "numero",
        "complemento", "bairro", "cep", "uf", "municipio", "ddd_1",
        "telefone_1", "ddd_2", "telefone_2", "ddd_fax", "fax",
        "correio_eletronico", "situacao_especial", "data_situacao_especial",
    ],
    "socios": [
        "cnpj_basico", "identificador_socio", "nome_socio_razao_social",
        "cpf_cnpj_socio", "qualificacao_socio", "data_entrada_sociedade",
        "pais", "representante_legal", "nome_do_representante",
        "qualificacao_representante_legal", "faixa_etaria",
    ],
    "simples": [
        "cnpj_basico", "opcao_pelo_simples", "data_opcao_simples",
        "data_exclusao_simples", "opcao_mei", "data_opcao_mei",
        "data_exclusao_mei",
    ],
    "lookup": ["codigo", "descricao"],
}

FILE_GROUPS: dict[str, dict] = {
    "Empresas": {"prefix": "Empresas", "parts": 10, "table": "empresas"},
    "Estabelecimentos": {"prefix": "Estabelecimentos", "parts": 10, "table": "estabelecimentos"},
    "Socios": {"prefix": "Socios", "parts": 10, "table": "socios"},
    "Simples": {"prefix": "Simples", "parts": 1, "table": "simples"},
    "Cnaes": {"prefix": "Cnaes", "parts": 1, "table": "lookup"},
    "Motivos": {"prefix": "Motivos", "parts": 1, "table": "lookup"},
    "Municipios": {"prefix": "Municipios", "parts": 1, "table": "lookup"},
    "Naturezas": {"prefix": "Naturezas", "parts": 1, "table": "lookup"},
    "Paises": {"prefix": "Paises", "parts": 1, "table": "lookup"},
    "Qualificacoes": {"prefix": "Qualificacoes", "parts": 1, "table": "lookup"},
}

CNPJ_KEY_COLUMNS: frozenset[str] = frozenset(
    {"cnpj_basico", "cnpj_ordem", "cnpj_dv", "cpf_cnpj_socio", "representante_legal"}
)


def columns_for(table: str) -> list[str]:
    """Return the ordered column list for a table. Raises KeyError if unknown."""
    return list(TABLES[table])
