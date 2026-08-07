# src/opl/vault/domains/cnpj.py
"""The CNPJ domain's vault tables and grains. Wave 1: `hub_empresa` and
`sat_empresa_dados` (Task 3), then `hub_estabelecimento`, its two satellites and
`link_empresa_estabelecimento` (Task 4).

THIS FILE IS DATA. Every guard that could refuse what is below lives in
`opl.vault.registry`, and every mechanism that reads it lives in `opl.vault.hubs`,
`opl.vault.satellites` and `opl.vault.observation`. Adding a domain means adding a
file like this one and nothing else -- see `opl/vault/domains/__init__.py` for why
that property is load-bearing rather than tidy.

NO TABLE NAME IS SPELLED HERE THAT ANOTHER MODULE ALREADY OWNS. The bronze source and
its quarantine come from `opl.bronze.registry`, whose entry keeps each table's
staging/bronze/quarantine triple in one literal precisely so a second spelling cannot
drift -- the documented defect being a quarantine name hardcoded in a job YAML that
"sent estab triagers to a table full of unrelated F1.2 lookup rows". Catalog and schema
come from `opl.config.DEFAULT.table`. The only literals below are the VAULT's own
names, which nothing else owns yet.

THE BUSINESS KEY IS CNPJ BÁSICO, EIGHT CHARACTERS, AND NEVER NUMERIC. Measured across
69.1M company rows: `cnpj_basico` is 8/8 characters with zero non-numeric values
today, so the zero-pad is defensive rather than corrective -- and it must stay a STRING
pad, because alphanumeric CNPJs take effect 2026-07-31 and an `int()` round trip would
lose a leading zero the moment one arrives. `BusinessKeyColumn(width=8)` is what
carries that; `opl.vault.hashing_spark.zero_padded_column` is what refuses an overlong
value rather than truncating it onto another company's key.

THE SATELLITE'S PAYLOAD IS FOUR COLUMNS AND THE EMPRESAS CONTRACT HAS SEVEN. Razão
social, natureza jurídica, capital social and porte are the descriptive facts about a
company; `qualificacao_responsavel` and `ente_federativo_responsavel` are attributes of
a PERSON and of a public entity attached to the company, and they belong to a different
satellite whenever something needs them. The split is not cosmetic: a satellite records
change, and lumping columns that change at different rates into one `hash_diff` writes
a row for the whole payload every time the fastest-moving column twitches.
`tests/vault/test_cnpj_vault.py::test_a_change_outside_the_payload_produces_no_second_
row` is what keeps the two apart.

THE ESTABLISHMENT'S BUSINESS KEY IS THE CNPJ COMPLETO AND IS CARRIED AS ITS THREE
COMPONENTS, not as a derived fourteen-character string. `cnpj_basico` (8) +
`cnpj_ordem` (4) + `cnpj_dv` (2) is fourteen digits and those widths are EXACT across
72.3M rows in both snapshots, with zero non-numeric values -- so the zero-pads below
are defensive, like empresas'. Three columns rather than one derived column, for three
reasons and not for convenience: bronze holds the three and holds no fourteen-character
spelling, so a derived key would be a fourth spelling nothing else can join on; the
hash standard length-prefixes and `||`-joins each component, which is a strictly
stronger encoding than a bare concatenation (`ordem='1', dv='23'` and `ordem='12',
dv='3'` concatenate to the same fourteen characters unpadded, and to different digests
here); and a derived column would need a new field on `Hub` -- an expression, not a
name -- which no other table in the vault wants. THE ORDER IS THE KEY: the standard
concatenates in declaration order, so (basico, ordem, dv) and (dv, ordem, basico) are
different digests for the same establishment, and this is the one place that order is
written down.

THE TWO ESTABLISHMENT SATELLITES SPLIT ADDRESS FROM STATUS, and that is one cut rather
than the finest available one. An address change is a physical move; a situação
cadastral change is a registry event; the two arrive on different rows of the RFB's
history for the same establishment, so a single satellite would rewrite the whole
sixteen-column payload every time either moved. MEASURED across the 71,874,444
establishments present in both 2026-06 and 2026-07
(`01f192ac-d8be-1e59-99e5-05717e28efcc`):

    `_dados`     (6 columns)   1,076,696 changed   1.50%
    `_endereco`  (10 columns)    570,075 changed   0.79%

    nome_fantasia 31,912 | cnae_fiscal_principal 84,588
    situacao_cadastral 976,355 | motivo_situacao_cadastral 976,333

THE SPLIT IS WORTH 1.9x AND THE RIDE-ALONG COSTS 30x, which is the honest reading of
those numbers and the one that argues the known cost. `nome_fantasia` (31,912) sits in
`_dados` beside `situacao_cadastral` (976,355), so a status change rewrites a name that
moved thirty times less often -- a wider spread than the 1.50/0.79 the cut itself
rests on. It is still one cut and not the finest: `situacao_cadastral` and
`motivo_situacao_cadastral` move together almost exactly (976,355 against 976,333,
because the motivo is what explains the situação), so those two genuinely belong in one
payload, and the case for a third satellite is `nome_fantasia` plus the CNAEs rather
than a general subdivision. Splitting `_dados` later means rewriting
`sat_estabelecimento_dados`; ADDING one of the unmodelled columns below is a new
satellite and touches nothing.

`UNMODELLED_ESTABELECIMENTO_COLUMNS` IS DECLARED RATHER THAN LEFT AS A GAP. Eleven of
the contract's thirty columns are in neither satellite, and a column that is simply
absent from both payload tuples is indistinguishable from a column someone forgot.
Declaring them makes `test_the_two_satellites_and_the_key_partition_the_estabelecimentos
_contract` total: every contract column is a business key, a payload of exactly one
satellite, or a deliberate omission with a reason beside it, and a column added to the
contract turns that test red."""
from __future__ import annotations

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT
from opl.vault.observation import ObservationGrain
from opl.vault.registry import BusinessKeyColumn, Hub, Link, Satellite, VaultDomain

_EMPRESAS = bronze_table_spec("empresas")
_ESTABELECIMENTOS = bronze_table_spec("estabelecimentos")

# Eight characters, per the RFB's own layout and the `cnpj_basico_len8` CHECK
# constraint bronze re-asserts after every promote.
CNPJ_BASICO_WIDTH = 8

HUB_EMPRESA = Hub(
    name="hub_empresa",
    hash_key="hub_empresa_hk",
    business_keys=(BusinessKeyColumn(name="cnpj_basico", width=CNPJ_BASICO_WIDTH),),
)

SAT_EMPRESA_DADOS = Satellite(
    name="sat_empresa_dados",
    parent=HUB_EMPRESA.name,
    payload_columns=("razao_social", "natureza_juridica", "capital_social", "porte_empresa"),
)

# Where the rows come from. Qualified here, once, so neither loader nor job task
# spells a catalog or a schema.
EMPRESAS_SOURCE = DEFAULT.table(_EMPRESAS.bronze)

# The observation ledger at hub grain, over the same two tables. Keyed on the RAW
# `cnpj_basico` rather than the padded one, which is the same value today (8/8
# characters) and is the right choice regardless: the ledger answers "what did we SEE",
# and what we saw is the column as bronze holds it.
EMPRESA_GRAIN = ObservationGrain.in_default_schema(
    name=HUB_EMPRESA.name,
    bronze=_EMPRESAS.bronze,
    quarantine=_EMPRESAS.quarantine,
    key_columns=HUB_EMPRESA.business_key_columns,
)

# The other two components of the CNPJ completo, per the RFB layout. `cnpj_ordem`
# distinguishes the company's establishments (0001 is the matriz) and `cnpj_dv` is the
# two check digits; 8 + 4 + 2 = 14.
CNPJ_ORDEM_WIDTH = 4
CNPJ_DV_WIDTH = 2

HUB_ESTABELECIMENTO = Hub(
    name="hub_estabelecimento",
    hash_key="hub_estabelecimento_hk",
    business_keys=(
        BusinessKeyColumn(name="cnpj_basico", width=CNPJ_BASICO_WIDTH),
        BusinessKeyColumn(name="cnpj_ordem", width=CNPJ_ORDEM_WIDTH),
        BusinessKeyColumn(name="cnpj_dv", width=CNPJ_DV_WIDTH),
    ),
)

SAT_ESTABELECIMENTO_DADOS = Satellite(
    name="sat_estabelecimento_dados",
    parent=HUB_ESTABELECIMENTO.name,
    payload_columns=(
        "nome_fantasia",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
    ),
)

SAT_ESTABELECIMENTO_ENDERECO = Satellite(
    name="sat_estabelecimento_endereco",
    parent=HUB_ESTABELECIMENTO.name,
    payload_columns=(
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        # The foreign-address pair. In `_endereco` and not in `_dados` because they
        # ARE the address for an establishment outside Brazil -- the same fact the
        # eight columns above carry for one inside it.
        "nome_cidade_exterior",
        "pais",
    ),
)

# Every estabelecimentos column that is neither a business key nor in a payload above,
# with the reason. See the module docstring for why this is a declared value.
UNMODELLED_ESTABELECIMENTO_COLUMNS = (
    # An attribute of the RELATIONSHIP -- "is this establishment the company's matriz?"
    # -- not of the establishment alone, so it belongs on a satellite of
    # `link_empresa_estabelecimento`, which this vault has no shape for yet.
    "identificador_matriz_filial",
    # Contact details. A third change rate: a phone number moves without an address or
    # a status moving. `sat_estabelecimento_contato` whenever something needs them.
    "ddd_1",
    "telefone_1",
    "ddd_2",
    "telefone_2",
    "ddd_fax",
    "fax",
    "correio_eletronico",
    # Lifecycle dates and the special-situation pair, which is a SECOND status axis
    # (recuperação judicial and the like) and would drag `_dados` between two rates.
    "data_inicio_atividade",
    "situacao_especial",
    "data_situacao_especial",
)

LINK_EMPRESA_ESTABELECIMENTO = Link(
    name="link_empresa_estabelecimento",
    hash_key="link_empresa_estabelecimento_hk",
    # HIERARCHICAL, AND THE ORDER IS PARENT THEN CHILD. Every establishment belongs to
    # exactly one company and shares its `cnpj_basico`, so this link is a hierarchy
    # rather than a many-to-many -- and it is still a link, not a foreign key on the
    # hub, because a hub row asserts only that a key exists. The order is what the
    # link's hash key concatenates in; reading parent-to-child is the only reason it is
    # this way round rather than the other.
    hubs=(HUB_EMPRESA.name, HUB_ESTABELECIMENTO.name),
)

# Estabelecimentos is a SECOND FEED FOR `hub_empresa` as well as the source of
# everything above: it carries `cnpj_basico`, so `load_hub(HUB_EMPRESA, source_table=
# ESTABELECIMENTOS_SOURCE)` is a legitimate load and the hub's anti-join makes running
# both feeds free. That is DV2 working as advertised rather than a special case, and it
# is why `HubLoadResult.already_present` is a whole-table count rather than a
# window-scoped one.
ESTABELECIMENTOS_SOURCE = DEFAULT.table(_ESTABELECIMENTOS.bronze)

# Hub grain over estabelecimentos, shared by both satellites -- they record change at
# the same grain and differ only in payload. Keyed on the RAW columns for
# `EMPRESA_GRAIN`'s reason, and in the HUB'S ORDER because
# `opl.vault.satellites._grain_key_mismatch` requires the two declarations to be the
# same list rather than merely the same set; see that function for the argument.
ESTABELECIMENTO_GRAIN = ObservationGrain.in_default_schema(
    name=HUB_ESTABELECIMENTO.name,
    bronze=_ESTABELECIMENTOS.bronze,
    quarantine=_ESTABELECIMENTOS.quarantine,
    key_columns=HUB_ESTABELECIMENTO.business_key_columns,
)

DOMAIN = VaultDomain(
    name="cnpj",
    tables=(
        HUB_EMPRESA,
        SAT_EMPRESA_DADOS,
        HUB_ESTABELECIMENTO,
        SAT_ESTABELECIMENTO_DADOS,
        SAT_ESTABELECIMENTO_ENDERECO,
        LINK_EMPRESA_ESTABELECIMENTO,
    ),
)
