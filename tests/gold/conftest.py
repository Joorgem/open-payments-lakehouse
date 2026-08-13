# tests/gold/conftest.py
"""A vault in miniature -- `hub_empresa` and `sat_empresa_dados`, built by the REAL
loaders over a bronze fixture -- and the gold tables derived from it.

BUILT BY THE VAULT'S OWN LOADERS AND NOT HAND-WRITTEN, which is the decision this file
is. A satellite table written directly by `createDataFrame` would let the dimension pass
against a version chain the vault cannot actually produce -- the wrong `applied_date`
type, a `hash_diff` that never repeats, a company with two rows on one date. Running
`load_hub` and `load_satellite` costs seconds and makes the fixture's shape a
CONSEQUENCE of the layer below rather than a claim about it.

NOTHING IS IMPORTED FROM `tests/vault/conftest.py`, and the duplication is deliberate in
the shape `tests/test_job_yaml_wiring.py`'s docstring already argues: a test module
importing another test package's conftest gives this suite a collection-order dependency
it does not otherwise have, and `tests/` is not a package, so the import would have to
reach across two `__init__.py`-bearing directories that share no root. What is copied is
six lines of DDL derived from the contract; what is not copied is any assertion.

THE FIXTURE CARRIES A THREE-VERSION COMPANY, AND THE THIRD MONTH IS NOT IN BRONZE. Real
bronze holds 2026-06 and 2026-07 only, so every company in it has at most two satellite
versions -- and with two versions the "version n's `valid_to` is version n+1's
`valid_from`" chain has exactly one adjacent pair per company, in which one end is
always the floor and the other always the ceiling. The MIDDLE version, whose `valid_from`
and `valid_to` are BOTH real dates, would never be built. 2026-05 exists in the RFB's
gapless monthly series and not in our bronze, so a company observed in all three is a
mechanism test and never a measurement -- the same reason and the same month
`tests/vault/conftest.py` declares MAY for.

THE `00000000` COMPANY IS IN HERE BECAUSE IT IS REAL. `docs/f1b-run-evidence.md` section
2.4 records `00000000` as `hub_empresa`'s lowest key on the live data, which is why the
ghost row must NOT be keyed on it -- a ghost there would silently merge every unresolved
payment onto a company that exists. The fixture carries the key so that claim is
asserted against a hub that actually holds it."""
from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from opl.contracts.cnpj_schemas import columns_for
from opl.gold import registry as gold_registry
from opl.gold.dimensions import load_dimension
from opl.vault import domains
from opl.vault.hubs import load_hub
from opl.vault.observation import ObservationGrain
from opl.vault.satellites import load_satellite

HUB = domains.table_spec("hub_empresa")
SAT = domains.table_spec("sat_empresa_dados")
DIM = gold_registry.table_spec("dim_company")

# The two months real bronze holds, plus the one the RFB publishes and we never loaded.
# The ref dates are the RFB's own, from its filenames -- NOT month-end, which is why
# `applied_date` cannot be derived from `_snapshot_month`.
MAY, JUN, JUL = "2026-05", "2026-06", "2026-07"
REF_DATES = {MAY: date(2026, 5, 9), JUN: date(2026, 6, 13), JUL: date(2026, 7, 11)}
WINDOW = (MAY, JUN, JUL)

# FAR FROM EVERY REF DATE, for `tests/vault/conftest.py`'s reason: `load_date` is when
# WE loaded and `valid_from` is when the fact was true at the source, so a build stamped
# in 2027 cannot be confused with a snapshot taken in 2026. Well below year 3000, which
# is where `F.lit(datetime)` stops working on this dev box -- see `opl.gold.columns`.
BUILT_AT = datetime(2027, 3, 1, 9, 30, 0)
REBUILT_AT = datetime(2027, 4, 2, 18, 15, 0)

RECORD_SOURCE_VALUE = "rfb_cnpj_webdav"
INGESTED_AT = datetime(2026, 8, 1, 0, 0, 0)

# The seven columns bronze adds to every contract, in the order the ingest produces
# them, with the two that are not strings typed as they really are.
AUDIT_DDL = (
    "_rescued_data string, _source_file string, _ingested_at timestamp, "
    "_record_source string, _batch_id string, _snapshot_month string, "
    "_snapshot_ref_date date"
)

EMPRESAS_CONTRACT = tuple(columns_for("empresas"))
EMPRESAS_SCHEMA = (
    ", ".join(f"{column} string" for column in EMPRESAS_CONTRACT) + ", " + AUDIT_DDL
)
QUARANTINE_SCHEMA = EMPRESAS_SCHEMA + ", _dq_reject_reason string"

# `hub_empresa`'s REAL lowest key on the live data -- see the module docstring.
C_ZERO = "00000000"
C_TWO_VERSIONS = "10000001"  # razão social moves once: the 139,968 in miniature
C_ONE_VERSION = "10000002"  # nothing moves: the 68,922,881 in miniature
C_THREE_VERSIONS = "10000004"  # MAY/JUN/JUL: the only source of a MIDDLE version
C_JULY_ONLY = "30000003"  # never observed in MAY or JUN

# Every company and how many satellite versions it must produce. Read by the tests
# rather than restated there, so the arithmetic below has one source.
VERSIONS_OF = {
    C_ZERO: 1,
    C_TWO_VERSIONS: 2,
    C_ONE_VERSION: 1,
    C_THREE_VERSIONS: 3,
    C_JULY_ONLY: 1,
}
SOURCE_VERSIONS = sum(VERSIONS_OF.values())


def empresas_row(
    cnpj: str,
    month: str,
    *,
    razao: str = "ACME LTDA",
    natureza: str = "2062",
    capital: str = "1000,00",
    porte: str = "05",
) -> tuple:
    """One bronze empresas row: the whole contract plus every audit column the ingest
    stamps, because the vault loaders read three of them."""
    return (
        cnpj,
        razao,
        natureza,
        "49",
        capital,
        porte,
        None,
        None,
        f"/Volumes/x/cnpj/{month}/empresas/K3241.K03200Y0.D60613.EMPRECSV",
        INGESTED_AT,
        RECORD_SOURCE_VALUE,
        "batch-1",
        month,
        REF_DATES[month],
    )


def _bronze_rows() -> list[tuple]:
    """The fixture's bronze rows, meant to be read top to bottom -- the shape IS the
    argument, and each block is one row of `VERSIONS_OF`."""
    return [
        empresas_row(C_ZERO, JUN, razao="ZERO KEY LTDA"),
        empresas_row(C_ZERO, JUL, razao="ZERO KEY LTDA"),
        empresas_row(C_TWO_VERSIONS, JUN, razao="ACME LTDA"),
        empresas_row(C_TWO_VERSIONS, JUL, razao="ACME PARTICIPACOES SA"),
        empresas_row(C_ONE_VERSION, JUN, razao="BETA SA"),
        empresas_row(C_ONE_VERSION, JUL, razao="BETA SA"),
        empresas_row(C_THREE_VERSIONS, MAY, capital="1000,00"),
        empresas_row(C_THREE_VERSIONS, JUN, capital="5000,00"),
        empresas_row(C_THREE_VERSIONS, JUL, capital="370000,00"),
        empresas_row(C_JULY_ONLY, JUL, razao="GAMMA ME"),
    ]


@pytest.fixture(scope="session")
def gold_database(spark, tmp_path_factory):
    """Factory: `gold_database("dim")` creates a throwaway Delta database and returns
    its name; every one created is dropped when the session ends.

    SESSION-SCOPED SO A MODULE-SCOPED FIXTURE CAN USE IT -- pytest refuses a
    narrower-scoped dependency, and the vault fixtures below are module-scoped because
    `saveAsTable` costs seconds."""
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


@pytest.fixture(scope="module")
def empresas_bronze(spark, gold_database):
    """One bronze empresas table and its (empty) quarantine, in three months."""
    db = gold_database("gold_dim")
    bronze, quarantine = f"{db}.empresas", f"{db}.empresas_q"
    (
        spark.createDataFrame(_bronze_rows(), EMPRESAS_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(bronze)
    )
    (
        spark.createDataFrame([], QUARANTINE_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(quarantine)
    )
    grain = ObservationGrain(
        name=HUB.name,
        bronze_table=bronze,
        quarantine_table=quarantine,
        key_columns=HUB.business_key_columns,
    )
    return SimpleNamespace(db=db, bronze=bronze, quarantine=quarantine, grain=grain)


def load_vault(spark, source, names, *, months=WINDOW):
    """One `hub_empresa` load and one `sat_empresa_dados` load over `months`, by the
    real loaders. Returns `names`, with the two results attached."""
    names.hub_result = load_hub(
        spark, HUB, source_table=source.bronze, target_table=names.hub,
        load_date=BUILT_AT, months=list(months),
    )
    names.sat_result = load_satellite(
        spark, SAT, hub=HUB, source_table=source.bronze, target_table=names.sat,
        load_date=BUILT_AT, grain=source.grain, months=list(months),
    )
    return names


def vault_names(db: str, suffix: str) -> SimpleNamespace:
    """Fresh hub/satellite/dimension table names in `db`."""
    return SimpleNamespace(
        hub=f"{db}.hub_{suffix}", sat=f"{db}.sat_{suffix}", dim=f"{db}.dim_{suffix}"
    )


def build_dimension(spark, names, *, load_date=BUILT_AT, months=WINDOW, **overrides):
    """`dim_company` over the vault tables in `names`, with the loader's own arguments
    overridable -- so a test that needs a mismatched spec passes one rather than
    reaching into the loader."""
    arguments = {
        "satellite": SAT,
        "hub": HUB,
        "source_table": names.sat,
        "hub_table": names.hub,
        "target_table": names.dim,
        "load_date": load_date,
        "months": tuple(months),
    }
    arguments.update(overrides)
    return load_dimension(spark, DIM, **arguments)


@pytest.fixture(scope="module")
def vault_loaded(spark, empresas_bronze):
    """The vault over all three months, shared by every read-only assertion."""
    return load_vault(spark, empresas_bronze, vault_names(empresas_bronze.db, "shared"))


@pytest.fixture(scope="module")
def dim_loaded(spark, vault_loaded):
    """One `dim_company` build over that vault, and its result. Module-scoped because
    every test below it only reads; the tests that LOAD AGAIN take `gold_target`."""
    result = build_dimension(spark, vault_loaded)
    return SimpleNamespace(names=vault_loaded, result=result, table=vault_loaded.dim)


@pytest.fixture
def gold_target(spark, empresas_bronze):
    """A fresh vault AND a fresh dimension name per test, for the tests that write --
    sharing one would make idempotence pass for the wrong reason."""
    return vault_names(empresas_bronze.db, uuid4().hex[:8])
