# tests/vault/conftest.py
"""What every vault test is written against: the two real snapshots, the bronze row
shape, and the estabelecimentos fixture that Task 4 built and Task 5 must not rebuild.

WHY THIS FILE EXISTS, AND IT IS NOT TIDINESS. `tests/vault/test_estabelecimento_vault
.py` reached exactly 800 lines -- the project's own file cap -- with roughly 160 of them
being the fixture rather than the assertions, and Task 5 needed a new file. Copying that
fixture into a second module would have cost a second `CREATE DATABASE`, a second pair
of Delta writes and a second five-table load per run, in a suite that already cannot be
run in one `pytest` invocation inside the tool's timeout. Extracting it makes the
materialisation package-wide and shared, and gives the capped file the headroom it no
longer had.

THE FIXTURE NAMES CARRY THEIR TABLE. A package-scoped fixture called `source` or
`loaded` would be silently injected into any vault test that asked for one -- including
a future test that meant its OWN source and simply had not defined it yet, which would
then run green against another table's data and report it. `estab_source` cannot be
requested by accident. The two modules that predate this file keep their own
module-scoped `source` and `target`, which shadow nothing here because nothing here is
called that.

WHAT IS SHARED AND WHAT IS NOT. Everything above the estabelecimentos section is
generic: the months, the two dates the RFB itself stamps, the audit columns bronze adds
to every contract, and the two helpers that write and derive Delta tables. Below it are
the two table fixtures, each kept whole rather than parameterised, because their rows
ARE their argument -- each module's docstring reads them against the measurement they
mirror. Both moved here for the same reason: their modules hit the 800-line cap and
their materialisations cost seconds that a second file must not pay twice."""
from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from opl.contracts.cnpj_schemas import columns_for
from opl.vault import domains
from opl.vault.hubs import load_hub
from opl.vault.links import load_link
from opl.vault.observation import ObservationGrain
from opl.vault.satellites import load_satellite

# The two months real bronze holds, and the ref dates the RFB stamps in its own
# filenames -- NOT month-end, which is why `applied_date` cannot be derived from
# `_snapshot_month` (see `opl.vault.columns`).
JUN, JUL = "2026-06", "2026-07"
JUN_REF, JUL_REF = date(2026, 6, 13), date(2026, 7, 11)

# A THIRD MONTH THAT IS REAL IN THE SOURCE AND NOT IN OUR BRONZE. The RFB publishes a
# gapless monthly snapshot from 2023-05 onward; F1 loaded two of them. Every fixture
# that mirrors the measurement uses JUN and JUL only -- MAY exists so the branches that
# need THREE observations of one key can be exercised at all, above all a relationship
# that departs and returns, which two months structurally cannot show. A test using it
# is testing a mechanism, never a measurement, and says so.
MAY, MAY_REF = "2026-05", date(2026, 5, 9)

REF_DATES = {MAY: MAY_REF, JUN: JUN_REF, JUL: JUL_REF}

# FAR FROM EITHER REF DATE, AND THAT IS THE POINT. `load_date` is when WE loaded and
# `applied_date` is when the fact was true at the source; a loader that crossed them
# would still produce two plausible-looking columns. A load stamped in 2027 cannot be
# confused with a snapshot taken in 2026.
LOADED_AT = datetime(2027, 3, 1, 9, 30, 0)
RELOADED_AT = datetime(2027, 4, 2, 18, 15, 0)

RECORD_SOURCE_VALUE = "rfb_cnpj_webdav"
INGESTED_AT = datetime(2026, 8, 1, 0, 0, 0)

# The seven columns bronze adds to every contract, in the order the ingest produces
# them, with the two that are NOT strings typed as they really are -- a fixture that
# declared `_snapshot_ref_date` a string would hand the loaders a shape bronze does not
# have and would make `applied_date` a string in every satellite built on it.
AUDIT_DDL = (
    "_rescued_data string, _source_file string, _ingested_at timestamp, "
    "_record_source string, _batch_id string, _snapshot_month string, "
    "_snapshot_ref_date date"
)
# The one column the quarantine carries that bronze does not, per
# `opl.bronze.masking.QUARANTINE_COLUMNS`: the gate appends it last.
REJECT_REASON_DDL = ", _dq_reject_reason string"


def bronze_schema(contract: str) -> str:
    """A bronze table's DDL for `contract`: every contract column STRING, then the
    audit columns.

    DERIVED FROM THE CONTRACT rather than restated. Thirty columns spelled a second
    time in a fixture is thirty chances for the fixture to describe a table bronze does
    not have, and the totality tests that check a satellite's payload against the
    contract are only meaningful if the fixture really is the contract."""
    return ", ".join(f"{column} string" for column in columns_for(contract)) + ", " + AUDIT_DDL


def quarantine_schema(contract: str) -> str:
    """The bronze shape plus the DQ gate's reject reason."""
    return bronze_schema(contract) + REJECT_REASON_DDL


def audit_values(
    month: str, *, source_file: str, record_source: str = RECORD_SOURCE_VALUE
) -> tuple:
    """The seven audit values one bronze row carries, in `AUDIT_DDL`'s order.

    `record_source` is a parameter because `add_audit_columns` takes one: a month
    re-ingested under a different source label carries a different value for the same
    business key, and that is the only thing that can tell the hub's "earliest"
    `record_source` apart from an arbitrary one."""
    return (
        None,
        source_file,
        INGESTED_AT,
        record_source,
        "batch-1",
        month,
        REF_DATES[month],
    )


def write_delta(spark, table: str, schema: str, rows: list[tuple]) -> None:
    """`rows` appended to a managed Delta table.

    Delta rather than a temp view, everywhere in this package, for the reason
    `test_observation.py::tables` MEASURED: a view over `createDataFrame`
    re-materialises from the driver on every query and made reads ~3x slower."""
    (spark.createDataFrame(rows, schema)
     .write.format("delta").mode("append").saveAsTable(table))


def derived_table(spark, db: str, name: str, frame) -> str:
    """`frame` written to a fresh Delta table in `db`, and its qualified name.

    For the tests that feed a loader a DELIBERATELY BROKEN source -- a dropped column,
    a typed column -- so the refusal is met on the read path production uses rather
    than on a temp view."""
    table = f"{db}.{name}_{uuid4().hex[:8]}"
    frame.write.format("delta").mode("append").saveAsTable(table)
    return table


@pytest.fixture(scope="session")
def vault_database(spark, tmp_path_factory):
    """Factory: `vault_database("socios")` creates a throwaway Delta database and
    returns its name; every one created is dropped when the session ends.

    SESSION-SCOPED SO A MODULE-SCOPED FIXTURE CAN USE IT. pytest refuses a
    narrower-scoped dependency, and the two fixtures below are module-scoped because
    `saveAsTable` costs seconds. The databases live under `tmp_path_factory`, so the
    files go where pytest cleans up even if a drop fails."""
    created: list[str] = []

    def _make(prefix: str) -> str:
        db = f"{prefix}_{uuid4().hex[:8]}"
        root = tmp_path_factory.mktemp(prefix)
        spark.sql(f"CREATE DATABASE {db} LOCATION '{root.as_uri()}'")
        created.append(db)
        return db

    yield _make
    for db in created:
        spark.sql(f"DROP DATABASE {db} CASCADE")


# --------------------------------------------------------------------------- #
# The estabelecimentos fixture (Task 4), shared rather than rebuilt
# --------------------------------------------------------------------------- #

ESTABELECIMENTO_HUB = domains.table_spec("hub_estabelecimento")
EMPRESA_HUB = domains.table_spec("hub_empresa")
ESTABELECIMENTO_DADOS = domains.table_spec("sat_estabelecimento_dados")
ESTABELECIMENTO_ENDERECO = domains.table_spec("sat_estabelecimento_endereco")
EMPRESA_ESTABELECIMENTO_LINK = domains.table_spec("link_empresa_estabelecimento")
EMPRESA_ESTABELECIMENTO_HUBS = domains.linked_hubs(EMPRESA_ESTABELECIMENTO_LINK)

# Establishments as (cnpj_basico, cnpj_ordem, cnpj_dv), RAW as bronze holds them.
E_STATUS = ("10000001", "0001", "23")     # situação cadastral moves; the address does not
E_ADDRESS = ("10000001", "0002", "04")    # the address moves; the status does not
E_UNCHANGED = ("20000002", "0001", "55")  # nothing moves
E_NEW_IN_JULY = ("30000003", "0001", "66")
# The padding pair. Their components concatenate to the same eleven characters
# UNPADDED ("40000004" + "1" + "23" and "40000004" + "12" + "3") and to different
# canonical keys once padded -- which is what makes a naive concatenation visible.
E_SHORT_ORDEM = ("40000004", "1", "23")
E_SHORT_TWIN = ("40000004", "12", "3")
# The four `encoding_replacement_char` rejects: June's bronze, July's quarantine.
E_REJECTED = (
    ("50000001", "0001", "81"),
    ("50000002", "0001", "82"),
    ("50000003", "0001", "83"),
    ("50000004", "0001", "84"),
)

ESTABELECIMENTO_CONTRACT = tuple(columns_for("estabelecimentos"))
_ESTABELECIMENTO_SCHEMA = bronze_schema("estabelecimentos")

_ESTABELECIMENTO_DEFAULTS = {
    "identificador_matriz_filial": "1",
    "nome_fantasia": "PADARIA CENTRAL",
    "situacao_cadastral": "02",
    "data_situacao_cadastral": "20050301",
    "motivo_situacao_cadastral": "00",
    "nome_cidade_exterior": None,
    "pais": None,
    "data_inicio_atividade": "20050301",
    "cnae_fiscal_principal": "4721102",
    "cnae_fiscal_secundaria": "4729699",
    "tipo_logradouro": "RUA",
    "logradouro": "DAS FLORES",
    "numero": "100",
    "complemento": None,
    "bairro": "CENTRO",
    "cep": "01001000",
    "uf": "SP",
    "municipio": "7107",
    "ddd_1": "11",
    "telefone_1": "30300000",
    "ddd_2": None,
    "telefone_2": None,
    "ddd_fax": None,
    "fax": None,
    "correio_eletronico": "contato@x.br",
    "situacao_especial": None,
    "data_situacao_especial": None,
}


def estabelecimento_row(key: tuple[str, str, str], month: str, **overrides) -> tuple:
    """One bronze estabelecimentos row: the whole contract plus every audit column the
    ingest stamps -- this layer reads three of them and must meet bronze's real shape."""
    values = dict(_ESTABELECIMENTO_DEFAULTS)
    values.update(zip(("cnpj_basico", "cnpj_ordem", "cnpj_dv"), key, strict=True))
    values.update(overrides)
    return tuple(values[column] for column in ESTABELECIMENTO_CONTRACT) + audit_values(
        month,
        source_file=f"/Volumes/x/cnpj/{month}/estabelecimentos/K3241.K03200Y1.D60613.ESTABELE",
    )


def _estabelecimento_bronze_rows() -> list[tuple]:
    """The fixture's bronze rows, meant to be read top to bottom -- the shape IS the
    argument."""
    rows = [
        estabelecimento_row(E_STATUS, JUN),
        estabelecimento_row(E_STATUS, JUL, situacao_cadastral="08",
                            data_situacao_cadastral="20260701",
                            motivo_situacao_cadastral="01"),
        estabelecimento_row(E_ADDRESS, JUN),
        estabelecimento_row(E_ADDRESS, JUL, logradouro="DAS ACACIAS", numero="250"),
        estabelecimento_row(E_UNCHANGED, JUN),
        estabelecimento_row(E_UNCHANGED, JUL),
        estabelecimento_row(E_NEW_IN_JULY, JUL),
        estabelecimento_row(E_SHORT_ORDEM, JUN),
        estabelecimento_row(E_SHORT_ORDEM, JUL),
        estabelecimento_row(E_SHORT_TWIN, JUN),
        estabelecimento_row(E_SHORT_TWIN, JUL),
    ]
    # In June's bronze and nowhere in July's. June's quarantine is EMPTY, as measured.
    rows.extend(estabelecimento_row(key, JUN) for key in E_REJECTED)
    return rows


def _estabelecimento_quarantine_rows() -> list[tuple]:
    """July's rejects, with the gate's reason and the replacement char that earned it."""
    return [
        (*estabelecimento_row(key, JUL, nome_fantasia="PADARIA CENTR�L"),
         "encoding_replacement_char")
        for key in E_REJECTED
    ]


@pytest.fixture(scope="module")
def estab_source(spark, vault_database):
    """A throwaway Delta database holding one bronze estabelecimentos table and its
    quarantine, in the two months real bronze has."""
    db = vault_database("estab_vault")
    bronze, quarantine = f"{db}.estabelecimentos", f"{db}.estab_q"

    write_delta(spark, bronze, _ESTABELECIMENTO_SCHEMA, _estabelecimento_bronze_rows())
    write_delta(spark, quarantine, quarantine_schema("estabelecimentos"),
                _estabelecimento_quarantine_rows())

    grain = ObservationGrain(
        name="hub_estabelecimento", bronze_table=bronze, quarantine_table=quarantine,
        key_columns=ESTABELECIMENTO_HUB.business_key_columns,
    )
    return SimpleNamespace(db=db, bronze=bronze, quarantine=quarantine, grain=grain)


@pytest.fixture
def estab_target(estab_source):
    """Fresh table names per test, for the tests that WRITE -- sharing one would make
    idempotence pass for the wrong reason."""
    db, suffix = estab_source.db, uuid4().hex[:8]
    return SimpleNamespace(
        hub=f"{db}.hub_{suffix}",
        empresa_hub=f"{db}.emp_{suffix}",
        dados=f"{db}.dados_{suffix}",
        endereco=f"{db}.end_{suffix}",
        link=f"{db}.link_{suffix}",
    )


def load_estabelecimento_vault(spark, source, names, *, load_date=LOADED_AT, months=None):
    """One load of each of the five tables, in dependency order, over `months`.

    `hub_empresa` is loaded FROM ESTABELECIMENTOS, which is the real design and not a
    fixture shortcut: that table carries `cnpj_basico`, so it is a second feed for the
    hub and the anti-join makes running both feeds free."""
    names.hub_result = load_hub(
        spark, ESTABELECIMENTO_HUB, source_table=source.bronze, target_table=names.hub,
        load_date=load_date, months=months,
    )
    names.empresa_hub_result = load_hub(
        spark, EMPRESA_HUB, source_table=source.bronze, target_table=names.empresa_hub,
        load_date=load_date, months=months,
    )
    for satellite, table in ((ESTABELECIMENTO_DADOS, "dados"),
                             (ESTABELECIMENTO_ENDERECO, "endereco")):
        # `report_diagnostics=True` BECAUSE THIS FIXTURE'S READERS ASSERT THE TWO NUMBERS.
        # The loader defaults it OFF -- on 72.3M rows those two counts were most of a
        # 5,635 s load and both answered 0 -- and off they come back `None`, which is
        # "not measured" and not a zero anything can be checked against. The tests reading
        # this fixture claim `candidate_departures == 0` (our own gate is not a departure)
        # and `collapsed_duplicates == 0` (one row per key per month here), so this fixture
        # has to be a measuring load. The default path is pinned in
        # `tests/vault/test_satellite_diagnostics.py`, on both arms.
        setattr(names, f"{table}_result", load_satellite(
            spark, satellite, hub=ESTABELECIMENTO_HUB, source_table=source.bronze,
            target_table=getattr(names, table), load_date=load_date,
            grain=source.grain, months=months, report_diagnostics=True,
        ))
    names.link_result = load_link(
        spark, EMPRESA_ESTABELECIMENTO_LINK, hubs=EMPRESA_ESTABELECIMENTO_HUBS,
        source_table=source.bronze, target_table=names.link, load_date=load_date,
        months=months,
    )
    return names


@pytest.fixture(scope="module")
def estab_loaded(spark, estab_source):
    """One load of every table over both months, shared by every read-only assertion.
    Module-scoped because `saveAsTable` costs seconds and none of these tests writes;
    tests that load again take `estab_target` instead."""
    db = estab_source.db
    names = SimpleNamespace(
        hub=f"{db}.hub_shared", empresa_hub=f"{db}.emp_shared",
        dados=f"{db}.dados_shared", endereco=f"{db}.end_shared",
        link=f"{db}.link_shared",
    )
    return load_estabelecimento_vault(spark, estab_source, names)


# --------------------------------------------------------------------------- #
# The socios fixture (Task 5), shared rather than rebuilt
# --------------------------------------------------------------------------- #

SOCIOS_LINK = domains.table_spec("link_company_partner")
SOCIOS_EFF = domains.table_spec("sat_eff_company_partner")
SOCIOS_IDENTITY = domains.link_identity_columns(SOCIOS_LINK)

# What a UC-masked column returns to every reader the mask function does not admit --
# the table owner included. Not a placeholder for a real name: this IS the read.
MASKED_READ = "***"

# Relationships as (cnpj_basico, identificador_socio, cpf_cnpj_socio), RAW as bronze
# holds them, which is also the ledger's key and the link's identity.
R_STAYS = ("10000001", "2", "***111111**")
R_DEPARTS = ("10000001", "2", "***222222**")      # June only, and NOT quarantined
R_REJECTED = ("10000001", "2", "***333333**")     # June's bronze, July's quarantine
R_NEW_IN_JULY = ("10000002", "2", "***444444**")
R_PJ = ("10000004", "1", "90000001000199")        # a partner that IS a company
R_PJ_PARENT = ("90000001", "2", "***666666**")    # ...and that company's own partner
R_FOREIGN_A = ("10000005", "3", None)             # no business key at all
R_FOREIGN_B = ("10000005", "3", None)             # ...and neither has this one
R_TWINNED = ("10000006", "2", "***777777**")      # two people, one masked CPF, one firm
R_ELSEWHERE = ("10000007", "2", "***777777**")    # the SAME masked CPF, another firm

PARTNER_ROOT = "90000001"

SOCIOS_CONTRACT = tuple(columns_for("socios"))
SOCIOS_SCHEMA = bronze_schema("socios")

_SOCIOS_DEFAULTS = {
    "nome_socio_razao_social": MASKED_READ,
    "qualificacao_socio": "49",
    "pais": None,
    "representante_legal": "***000000**",
    "nome_do_representante": MASKED_READ,
    "qualificacao_representante_legal": "00",
    "faixa_etaria": "5",
}


def socios_row(relationship: tuple[str, str, str | None], month: str, *, entrada: str,
         record_source: str = RECORD_SOURCE_VALUE, **overrides) -> tuple:
    """One bronze socios row: the whole contract plus every audit column the ingest
    stamps. `entrada` is `data_entrada_sociedade`, the window's DELIVERED open.

    `record_source` is a parameter because `add_audit_columns` takes one -- a month
    re-ingested under another label carries a different value for the same key -- and
    because it is what breaks a tie on the entry date in the dedup rule."""
    values = dict(_SOCIOS_DEFAULTS)
    values.update(zip(SOCIOS_IDENTITY, relationship, strict=True))
    values["data_entrada_sociedade"] = entrada
    values.update(overrides)
    return tuple(values[column] for column in SOCIOS_CONTRACT) + audit_values(
        month,
        source_file=f"/Volumes/x/cnpj/{month}/socios/K3241.K03200Y2.D60613.SOCIOCSV",
        record_source=record_source,
    )


def _socios_bronze_rows() -> list[tuple]:
    """The fixture's bronze rows, meant to be read top to bottom -- the shape IS the
    argument."""
    return [
        socios_row(R_STAYS, JUN, entrada="20100101"),
        socios_row(R_STAYS, JUL, entrada="20100101"),
        # The 65,444 in miniature: gone from July's bronze and NOT in its quarantine.
        socios_row(R_DEPARTS, JUN, entrada="20110202"),
        # The 1,781 in miniature: gone from July's bronze because WE removed it.
        socios_row(R_REJECTED, JUN, entrada="20120303"),
        socios_row(R_NEW_IN_JULY, JUL, entrada="20260701"),
        socios_row(R_PJ, JUN, entrada="20140505"),
        socios_row(R_PJ, JUL, entrada="20140505"),
        # The PJ partner's own row, which is what puts `90000001` in hub_empresa --
        # measured: all 310,374 PJ partner roots are present in empresas, zero
        # unresolved (`01f19063-44ef-132a-8aa7-9068b624b370`).
        socios_row(R_PJ_PARENT, JUN, entrada="20130404"),
        socios_row(R_PJ_PARENT, JUL, entrada="20130404"),
        # Two foreign partners of one company. Same key -- (company, '3', NULL) -- in
        # both months, because neither carries one.
        socios_row(R_FOREIGN_A, JUN, entrada="20150606"),
        socios_row(R_FOREIGN_B, JUN, entrada="20160707"),
        socios_row(R_FOREIGN_A, JUL, entrada="20150606"),
        socios_row(R_FOREIGN_B, JUL, entrada="20160707"),
        # THE DEDUP CASE. Two June rows on one link key with two delivered entry dates;
        # the EARLIEST must win, and the fold must be counted.
        socios_row(R_TWINNED, JUN, entrada="20200101"),
        socios_row(R_TWINNED, JUN, entrada="20150301"),
        socios_row(R_TWINNED, JUL, entrada="20200101"),
        # The same masked CPF at ANOTHER company: a different relationship.
        socios_row(R_ELSEWHERE, JUN, entrada="20170808"),
        socios_row(R_ELSEWHERE, JUL, entrada="20170808"),
    ]


def _socios_quarantine_rows() -> list[tuple]:
    """July's reject, with the gate's reason. Absent from July's bronze because of us."""
    return [(*socios_row(R_REJECTED, JUL, entrada="20120303", qualificacao_socio=""),
             "null_or_empty_qualificacao_socio")]


@pytest.fixture(scope="module")
def socios_source(spark, vault_database):
    """A throwaway Delta database holding one bronze socios table and its quarantine,
    in the two months real bronze has."""
    db = vault_database("socios_vault")
    bronze, quarantine = f"{db}.socios", f"{db}.socios_q"

    write_delta(spark, bronze, SOCIOS_SCHEMA, _socios_bronze_rows())
    write_delta(spark, quarantine, quarantine_schema("socios"), _socios_quarantine_rows())

    grain = ObservationGrain(
        name=SOCIOS_LINK.name, bronze_table=bronze, quarantine_table=quarantine,
        key_columns=SOCIOS_IDENTITY,
    )
    return SimpleNamespace(db=db, bronze=bronze, quarantine=quarantine, grain=grain)


@pytest.fixture
def socios_target(socios_source):
    """Fresh table names per test, for the tests that WRITE -- sharing one would make
    idempotence pass for the wrong reason."""
    db, suffix = socios_source.db, uuid4().hex[:8]
    return SimpleNamespace(
        hub=f"{db}.hub_{suffix}",
        link=f"{db}.link_{suffix}",
        eff=f"{db}.eff_{suffix}",
    )
