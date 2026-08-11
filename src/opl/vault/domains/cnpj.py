# src/opl/vault/domains/cnpj.py
"""The CNPJ domain's vault tables and grains. Wave 1: `hub_empresa` and
`sat_empresa_dados` (Task 3), then `hub_estabelecimento`, its two satellites and
`link_empresa_estabelecimento` (Task 4), `link_company_partner` and its effectivity
satellite (Task 5), then the six reference tables below (Task 6).

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
establishments present in both 2026-06 and 2026-07, over the FULL payloads exactly as
declared below (`01f192de-b784-1e33-a64b-625fad698c1a`):

    `_dados`     (6 columns)   1,211,834 changed   1.69%
    `_endereco`  (10 columns)    570,075 changed   0.79%

    nome_fantasia 31,912 | cnae_fiscal_principal 84,588
    situacao_cadastral 976,355 | motivo_situacao_cadastral 976,333
    (per-column, `01f192ac-d8be-1e59-99e5-05717e28efcc`, four of `_dados`' six)

RE-MEASURED AFTER TASK 7's CORRECTION PASS, AND THE FIRST FIGURE WAS A LOWER BOUND.
This block originally quoted 1,076,696 / 1.50% for `_dados` against a run that covered
only FOUR of its six columns, omitting `cnae_fiscal_secundaria` and
`data_situacao_cadastral`. The correction pass caught the mismatch between the run's
scope and the payload it was attributed to; the re-run above closes it. `_dados` was
UNDERSTATED -- 1,076,696 -> 1,211,834 -- so the ratio between the payloads goes from
~1.9x to ~2.13x and the split's justification is STRONGER than first claimed, not
weaker. Worth keeping visible: a scope error is not automatically an overclaim, and
this one ran the other way.

`_endereco` NEEDED NO CORRECTION -- 570,075 EITHER WAY -- AND THAT IS ITS OWN FINDING.
The two columns the earlier run omitted are `nome_cidade_exterior` and `pais`, the
foreign-address pair, and they changed on ZERO rows across all 71.8M establishments.
So the placement argued below ("they ARE the address for an establishment outside
Brazil") is supported from the other direction as well: they belong with the address,
and over this pair of snapshots they cost nothing to carry there. Measured after the
fact, and stated as such rather than as a reason the placement was chosen.

THE SPLIT IS WORTH ~2.13x AND THE RIDE-ALONG COSTS 30x, which is the honest reading of
those numbers and the one that argues the known cost. `nome_fantasia` (31,912) sits in
`_dados` beside `situacao_cadastral` (976,355), so a status change rewrites a name that
moved thirty times less often -- a wider spread than the 1.69/0.79 the cut itself
rests on. It is still one cut and not the finest: `situacao_cadastral` and
`motivo_situacao_cadastral` have near-identical change counts (976,355 against 976,333)
and the motivo is what explains the situação, so those two almost certainly belong in
one payload -- INFERRED, not measured, and the distinction is worth keeping because two
columns can share a marginal count while changing on disjoint rows. No cross-tab was
run. The case for a third satellite is `nome_fantasia` plus the CNAEs rather
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

from opl.bronze.lookup_routing import LOOKUP_SUFFIX
from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT
from opl.vault.observation import ObservationGrain
from opl.vault.registry import (
    BusinessKeyColumn,
    EffectivitySatellite,
    Hub,
    Link,
    LinkEnd,
    ReferenceTable,
    Satellite,
    VaultDomain,
)

_EMPRESAS = bronze_table_spec("empresas")
_ESTABELECIMENTOS = bronze_table_spec("estabelecimentos")
_SOCIOS = bronze_table_spec("socios")

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

# --------------------------------------------------------------------------- #
# Socios: the partnership relationship, and no `hub_socio`. See ADR 0011.
# --------------------------------------------------------------------------- #

LINK_COMPANY_PARTNER = Link(
    name="link_company_partner",
    hash_key="link_company_partner_hk",
    # SELF-REFERENCING, AND THAT IS THE FINDING THAT REMOVED `hub_socio`. All 310,374
    # of 310,374 distinct PJ partner CNPJs have their eight-digit root present in
    # empresas, zero unresolved (`01f19063-44ef-132a-8aa7-9068b624b370`) -- a corporate
    # partner is not a new business object, it is an empresa already in the hub. The
    # PARTNER end is `identifying=False`: its `cnpj_basico` is the first eight
    # characters of `cpf_cnpj_socio`, which is a dependent-child key below, so the
    # reference is a FUNCTION of the identity rather than a part of it.
    hubs=(
        LinkEnd(hub=HUB_EMPRESA.name, role="company"),
        LinkEnd(hub=HUB_EMPRESA.name, role="partner", identifying=False),
    ),
    # THE MEASURED BUSINESS KEY OF A PARTNERSHIP ROW, and neither component belongs to
    # a hub. `cpf_cnpj_socio` is masked at SOURCE by the RFB to six middle digits for a
    # PF partner: the key space is 10^6 and 999,853 of it is occupied -- 99.99%
    # saturated -- so a `hub_socio` on it merges unrelated people onto every key by
    # construction. (~27.3 partnership ROWS per key; distinct PEOPLE per key is
    # unmeasured and the mask makes it unmeasurable, so the row figure is an upper
    # bound and the saturation is the argument.) The relationship carries the grain
    # instead. See ADR 0011.
    dependent_child_keys=(
        BusinessKeyColumn(name="identificador_socio"),
        BusinessKeyColumn(name="cpf_cnpj_socio"),
    ),
)

SAT_EFF_COMPANY_PARTNER = EffectivitySatellite(
    name="sat_eff_company_partner",
    parent=LINK_COMPANY_PARTNER.name,
    # THE WINDOW'S OPEN, DELIVERED, UNDER THE SOURCE'S OWN NAME. Populated on 100% of
    # 2026-07's rows with no `00000000` sentinel (`01f19063-53c0-1f06-89f1-6aade0691af8`),
    # so the entry date is the RFB's own fact. The close is ours and is named in our
    # vocabulary (`last_observed_on`, `closed_by`) precisely so the two are not read as
    # claims of the same strength.
    entry_column="data_entrada_sociedade",
)

SOCIOS_SOURCE = DEFAULT.table(_SOCIOS.bronze)

# THE OBSERVATION LEDGER AT LINK GRAIN, which is the grain the effectivity satellite
# must gate on and NOT the hub grain: a partner who loses one of two partnerships is
# `absent_after_observation` at link grain and plainly `observed` at hub grain. Keyed
# on the link's identity columns, in hash order -- `opl.vault.effectivity` refuses a
# grain that is not exactly `domains.link_identity_columns(LINK_COMPANY_PARTNER)`.
COMPANY_PARTNER_GRAIN = ObservationGrain.in_default_schema(
    name=LINK_COMPANY_PARTNER.name,
    bronze=_SOCIOS.bronze,
    quarantine=_SOCIOS.quarantine,
    key_columns=(
        *HUB_EMPRESA.business_key_columns,
        *LINK_COMPANY_PARTNER.dependent_child_key_columns,
    ),
)

# Every socios column that is neither a key nor the effectivity window's open, with the
# reason -- declared for `UNMODELLED_ESTABELECIMENTO_COLUMNS`' reason, and here the
# first two are a HARD refusal rather than a deferral.
UNMODELLED_SOCIOS_COLUMNS = (
    # MASKED BY UNITY CATALOG, AND THE MASK APPLIES ON READ TO THE TABLE OWNER TOO --
    # measured against live bronze (`01f192b4-a6f8-1849-9955-f321d9742180`): both come
    # back as the literal three-character string `***`. A satellite payload containing
    # either would store `***` on every row and its `hash_diff` would be constant. This
    # is also why the PF key cannot be disambiguated by name, which is the obvious
    # repair for the collision above: it would hash `***`. See ADR 0008 and ADR 0011.
    "nome_socio_razao_social",
    "nome_do_representante",
    # Descriptive facts about the RELATIONSHIP, which belong to a descriptive satellite
    # on the link -- a table this vault has no loader for yet (`opl.vault.satellites`
    # takes a Hub). Adding one touches nothing that exists.
    "qualificacao_socio",
    "faixa_etaria",
    "pais",
    "representante_legal",
    "qualificacao_representante_legal",
)

# --------------------------------------------------------------------------- #
# The six reference tables: natural key, no hub, no hash key. `opl.vault.specs`
# argues the kind (see `ReferenceTable` there); `opl.vault.reference` is the loader and
# argues why these six tables cannot have a change-detection or end-dating path
# exercised on them -- `bronze_cnpj_lookup` is 2026-06 ONLY, because the 2026-07
# lookup zips were never published in that month's set. There is no second
# observation, so no `hash_diff` to compare, no `applied_date` sequence, and no
# absence for the observation ledger to report. See that module's docstring for
# what is proven by a synthetic second month instead, and what is not claimed.
# --------------------------------------------------------------------------- #

_LOOKUP = bronze_table_spec("lookup")
LOOKUP_SOURCE = DEFAULT.table(_LOOKUP.bronze)

# THE SIX TYPES, READ FROM `LOOKUP_SUFFIX` RATHER THAN RETYPED -- one spelling of
# the six tags, not a second one hand-copied from the docs. THE BRIEF FOR THIS TASK
# LISTS FIVE (CNAE, município, natureza jurídica, motivo, qualificação de sócio) AND
# OMITS PAÍS -- but `bronze_cnpj_lookup` carries país as a sixth, identically shaped
# (`codigo`/`descricao`) reference type with no property that distinguishes it from
# the other five (no masking, no different change rate, no relationship attribute),
# so leaving it out would be an unmodelled column with no argument beside it, which
# is the one thing `UNMODELLED_ESTABELECIMENTO_COLUMNS` above exists never to be.
# All six are modelled; the brief's list of five is read as incomplete, not as an
# exclusion.
REF_CNAE = ReferenceTable(
    name="ref_cnae", lookup_type=LOOKUP_SUFFIX["CNAE"],
    natural_key="codigo", payload="descricao",
)
REF_MOTIVO = ReferenceTable(
    name="ref_motivo", lookup_type=LOOKUP_SUFFIX["MOTI"],
    natural_key="codigo", payload="descricao",
)
REF_MUNICIPIO = ReferenceTable(
    name="ref_municipio", lookup_type=LOOKUP_SUFFIX["MUNIC"],
    natural_key="codigo", payload="descricao",
)
REF_NATUREZA_JURIDICA = ReferenceTable(
    name="ref_natureza_juridica", lookup_type=LOOKUP_SUFFIX["NATJU"],
    natural_key="codigo", payload="descricao",
)
REF_PAIS = ReferenceTable(
    name="ref_pais", lookup_type=LOOKUP_SUFFIX["PAIS"],
    natural_key="codigo", payload="descricao",
)
REF_QUALIFICACAO = ReferenceTable(
    name="ref_qualificacao", lookup_type=LOOKUP_SUFFIX["QUALS"],
    natural_key="codigo", payload="descricao",
)

REFERENCE_TABLES = (
    REF_CNAE, REF_MOTIVO, REF_MUNICIPIO, REF_NATUREZA_JURIDICA, REF_PAIS, REF_QUALIFICACAO,
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
        LINK_COMPANY_PARTNER,
        SAT_EFF_COMPANY_PARTNER,
        *REFERENCE_TABLES,
    ),
)
