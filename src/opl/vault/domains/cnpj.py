# src/opl/vault/domains/cnpj.py
"""The CNPJ domain's vault tables and grains. Wave 1, Task 3: `hub_empresa` and
`sat_empresa_dados`.

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
row` is what keeps the two apart."""
from __future__ import annotations

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT
from opl.vault.observation import ObservationGrain
from opl.vault.registry import BusinessKeyColumn, Hub, Satellite, VaultDomain

_EMPRESAS = bronze_table_spec("empresas")

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

DOMAIN = VaultDomain(name="cnpj", tables=(HUB_EMPRESA, SAT_EMPRESA_DADOS))
