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
from pyspark.sql import functions as F

from opl.contracts import payments, ptax
from opl.contracts.cnpj_schemas import columns_for
from opl.gold import registry as gold_registry
from opl.gold.conformed import load_conformed_dimension
from opl.gold.dimensions import instant_literal, load_dimension
from opl.gold.facts import load_fact
from opl.gold.registry import (
    DIM_CHANNEL,
    DIM_COMPANY,
    DIM_CURRENCY,
    DIM_DATE,
    FACT_PAYMENT,
)
from opl.gold.specs import fact_keys
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


def as_collected(spark, value: datetime) -> datetime:
    """`value` as `collect()` hands it back -- which is NOT `value` on a box whose
    operating system is not on the session's timezone.

    MEASURED, AND IT IS TWO ZONES MEETING IN THE DRIVER RATHER THAN A BUG IN EITHER.
    `opl.gold.dimensions.instant_literal` writes an instant by casting ISO text, which
    Spark parses in the SESSION zone (`opl.config.SESSION_TIMEZONE`, pinned to UTC);
    pyspark converts a TIMESTAMP back to Python through `datetime.fromtimestamp`, which
    reads the OPERATING SYSTEM's. On this UTC-3 dev box the two differ by three hours, so
    `row[VALID_FROM] == VALID_FROM_FLOOR` is FALSE about a dimension that is perfectly
    correct, and would have been true only by the accident of the session having
    inherited the OS zone.

    Round-tripping the expected value through the same two conversions is what keeps the
    assertion about the DATA. It is a Spark call per use and the tests that need it are
    few; a cached constant would be one more thing to keep in step with the session."""
    return spark.range(1).select(instant_literal(value).alias("v")).collect()[0]["v"]


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


# --- THE OTHER HALF OF THE STAR: the payments, the conformed dimensions, and the fact ---
#
# WHY THESE LIVE IN THE CONFTEST AND NOT BESIDE THE TESTS THAT ASSERT ON THEM. Two reasons,
# and the second is the one that decided it. First, this file is already "a vault in
# miniature and the gold tables derived from it" -- the fact is the last of those, and a
# star whose fixture is split across two files is a star nobody can read at once. Second,
# `tests/gold/test_fact_payment.py` reached 894 lines with them inline, against the
# project's 800-line cap, and the master protocol's remedy for that is a SPLIT rather than
# a condensation: "a previous condensation to hit exactly 800 lost two real arguments while
# claiming it had not". Moving the fixtures is the split that costs nothing, because they
# are module-scoped either way and no second vault gets built.
#
# THE PAYMENT FIXTURE IS NOT `test_conformed.py`'s. That module defines its own
# `payments_bronze` at module level, which shadows anything here; this one is called
# `fact_source` so the two cannot be confused, and it carries redeliveries and a legitimate
# repeat that the conformed tests have no use for.

# Bronze stores every contract column as a STRING and adds the seven audit columns the
# ingest stamps; the loader reads one of those (`_record_source`), so the fixture carries
# all of them rather than the one -- a table shaped like the real one cannot pass for the
# wrong reason.
PAYMENTS_SCHEMA = (
    ", ".join(f"{column} string" for column in payments.COLUMNS) + ", " + AUDIT_DDL
)

NATURAL_KEY = HUB.business_key_columns[0]

# THE THREE INSTANTS THE FIXTURE IS BUILT AROUND. `BOUNDARY` is the July snapshot to the
# microsecond, which is what makes the half-open interval testable: inclusive at both ends,
# it would match the version that closed there AND the one that opened there.
BEFORE = "2026-06-20T00:00:00.000Z"
BOUNDARY = f"{REF_DATES[JUL].isoformat()}T00:00:00.000Z"
AFTER = "2026-08-01T13:53:15.000Z"

# The capital figures `C_THREE_VERSIONS` carries in the fixture's three snapshots. The July
# one is `47070968`'s real value (`docs/f3-run-evidence.md` section 0.5, P6).
CAPITAL_IN_JUNE = "5000,00"
CAPITAL_IN_JULY = "370000,00"


def payment(
    identity: str,
    event_time: str,
    *,
    payer: str = C_THREE_VERSIONS,
    payee: str = C_ONE_VERSION,
    amount: str = "100.00",
    method: str = "PIX",
    currency: str = "BRL",
) -> tuple:
    """One bronze payment row: the whole contract plus every audit column the ingest
    stamps. `emitted_at` equals `event_time` -- lateness is a property of the DELIVERY and
    this layer reads neither."""
    return (
        identity, event_time, event_time, payer, payee, amount, currency, method,
        None,
        "/Volumes/x/generated/2026-08/payments/part-0.jsonl",
        INGESTED_AT,
        RECORD_SOURCE_VALUE,
        "batch-1",
        "2026-08",
        REF_DATES[JUL],
    )


def _payment_rows() -> list[tuple]:
    """Seven deliveries of five payments over four business-attribute tuples.

    READ TOP TO BOTTOM -- the shape IS the argument, and it is the smallest fixture in
    which every one of the three counts differs from the other two:

        7 rows delivered
        5 distinct transaction_id   (two byte-identical redeliveries)
        4 distinct attribute tuples (one LEGITIMATE REPEAT: tx-b repeats tx-a's payment
                                     under its own id, at a different instant)

    A fixture in which any two of those agree is a fixture that cannot tell a duplicate
    from a repeat, which is `opl.contracts.payments`' own sentence about the stream."""
    return [
        # tx-a and tx-b are the SAME payment made twice -- the legitimate repeat -- and
        # they straddle the July snapshot, which is what makes T-B's assertion possible.
        payment("tx-a", BEFORE),
        payment("tx-b", AFTER),
        # Exactly ON the boundary, to the microsecond.
        payment("tx-c", BOUNDARY, amount="200.00", method="TED"),
        # The reversed pair: the three-version company is the PAYEE here, so the two roles
        # cannot borrow each other's answer.
        payment("tx-d", AFTER, payer=C_ONE_VERSION, payee=C_THREE_VERSIONS,
                amount="300.00", method="BOLETO"),
        payment("tx-e", BEFORE, amount="400.00", method="TED"),
        # The two redeliveries: byte-identical to tx-a and tx-d, as `opl.generator.defects`
        # emits them ("the same bytes again: a redelivery, not a repeat").
        payment("tx-a", BEFORE),
        payment("tx-d", AFTER, payer=C_ONE_VERSION, payee=C_THREE_VERSIONS,
                amount="300.00", method="BOLETO"),
    ]


DELIVERED_ROWS = 7
DISTINCT_IDENTITIES = 5
DISTINCT_TUPLES = 4


@pytest.fixture(scope="module")
def fact_source(spark, empresas_bronze):
    """The bronze payments this star is built from."""
    table = f"{empresas_bronze.db}.bronze_payments_fact"
    (
        spark.createDataFrame(_payment_rows(), PAYMENTS_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(table)
    )
    return table


@pytest.fixture(scope="module")
def conformed_tables(spark, empresas_bronze, vault_loaded, fact_source):
    """`dim_date`, `dim_channel` and `dim_currency` over that same payment population, by
    the real conformed loader -- keyed by dimension name, which is how the fact loader
    takes them."""
    tables = {}
    for dimension in (DIM_DATE, DIM_CHANNEL, DIM_CURRENCY):
        target = f"{empresas_bronze.db}.{dimension.name}_fact_fixture"
        load_conformed_dimension(
            spark,
            dimension,
            fact_table=fact_source,
            target_table=target,
            load_date=BUILT_AT,
            applied_date_table=vault_loaded.sat if dimension is DIM_DATE else None,
        )
        tables[dimension.name] = target
    return tables


# THE THREE PTAX QUOTES THE FIXTURE STAR CONVERTS AT, AND EVERY ONE OF THEM IS MEASURED.
# `docs/f-api-run-evidence.md` §0.2 and §1.2 carry the requests: 2026-06-19 venda 5.14420
# published 13:03:25.555497, 2026-06-22 venda 5.13950 published 13:06:19.750415, 2026-07-31
# venda 5.07730 published 13:10:31.061071 -- all three READ AS BRT, which is T3's ruling and
# is what puts them three hours later than the text reads.
#
# NO INVENTED RATE, WHICH IS A CHOICE AND NOT AN ACCIDENT. A fixture is free to make up a
# number, and a made-up PTAX rate is the one kind that would be indistinguishable from a real
# one in a diff -- so the fixture carries only quotes with a request behind them, and the
# three happen to cover every payment instant `_payment_rows` emits: 2026-06-20T00:00Z
# resolves back to 06-19, 2026-07-11T00:00Z to 06-22, and 2026-08-01T13:53:15Z to 07-31.
#
# ALL-STRING, because that is what bronze lands (`opl.contracts.ptax`: the rates are carried
# as the DIGITS BCB published, never as a float, so `5.14420` keeps its trailing zero).
_PTAX_QUOTES = (
    ("2026-06-19", "USD", "2026-06-19 13:03:25.555497", "5.14360", "5.14420"),
    ("2026-06-22", "USD", "2026-06-22 13:06:19.750415", "5.13890", "5.13950"),
    ("2026-07-31", "USD", "2026-07-31 13:10:31.061071", "5.07670", "5.07730"),
)


def ptax_table(spark, db: str, rows=_PTAX_QUOTES) -> str:
    """`bronze_ptax` as the fact reads it: all-string, one row per (currency, quote_date).

    CREATED ONCE PER DATABASE AND NOT APPENDED TO, because `bronze_ptax` is written
    `mode("append")` in the real pipeline and a fixture that appended on every call would be
    reproducing exactly the duplicate `(currency, quote_date)` rows `opl.gold.fx` refuses --
    turning every test into an accidental probe of that refusal. A test that WANTS the
    duplicate passes its own rows under its own name."""
    table = f"{db}.bronze_ptax_{len(rows)}"
    if not spark.catalog.tableExists(table):
        schema = ", ".join(f"{column} string" for column in ptax.COLUMNS)
        (
            spark.createDataFrame(list(rows), schema)
            .write.format("delta").mode("append").saveAsTable(table)
        )
    return table


@pytest.fixture(scope="module")
def fx_source(spark, empresas_bronze):
    """The fixture star's PTAX series, under the name `build_fact` defaults to."""
    return ptax_table(spark, empresas_bronze.db)


def build_fact(
    spark, *, dim_loaded, fact_source, conformed_tables, target, fx_source=None, **overrides
):
    """`fact_payment` over the fixture star, with the loader's own arguments overridable --
    so a test that needs a mismatched spec passes one rather than reaching into the
    loader.

    `fx_source` DEFAULTS RATHER THAN BEING REQUIRED, which is the one argument here that is
    not a fixture. The FX series is an input every build needs and almost no test is ABOUT,
    so defaulting it kept F-API Task 4 from adding one parameter to sixteen call sites and a
    fixture to sixteen signatures -- and the default is derived from the fact source's own
    database, so it cannot silently point at another test's table."""
    arguments = {
        "dimension": DIM_COMPANY,
        "hub": HUB,
        "conformed": (DIM_DATE, DIM_CHANNEL, DIM_CURRENCY),
        "source_table": fact_source,
        "dimension_table": dim_loaded.table,
        "conformed_tables": conformed_tables,
        "fx_source_table": fx_source or ptax_table(spark, fact_source.split(".")[0]),
        "target_table": target,
        "load_date": BUILT_AT,
    }
    arguments.update(overrides)
    return load_fact(spark, FACT_PAYMENT, **arguments)


@pytest.fixture
def fact_target(empresas_bronze):
    """A fresh fact table name per test -- sharing one would make idempotence pass for the
    wrong reason."""
    return f"{empresas_bronze.db}.fact_payment_{uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def fact_loaded(spark, empresas_bronze, dim_loaded, fact_source, conformed_tables):
    """One `fact_payment` build over that star, and its result. Module-scoped because
    every test below it only reads; the tests that LOAD AGAIN take `fact_target`."""
    target = f"{empresas_bronze.db}.fact_payment_shared"
    result = build_fact(
        spark, dim_loaded=dim_loaded, fact_source=fact_source,
        conformed_tables=conformed_tables, target=target,
    )
    return result, target


def recovered(spark, fact_table, dim_company_table, conformed_tables):
    """The fact with every BUSINESS ATTRIBUTE recovered through the dimensions it keys.

    THIS IS THE STAR WORKING, AND IT IS ALSO THE ONLY WAY TO ASK THE FACT WHAT A PAYMENT
    WAS. A Kimball fact carries surrogate keys, not natural ones, so the tuple bronze holds
    as five columns lives here as two `company_sk`, one `channel_key`, one `currency_key`
    and one `amount`. The recovery joins on the SURROGATE key, which the dimension load
    measures to be unique, so it is exactly 1:1 and cannot fan out -- unlike a join on
    `cnpj_basico`, which would match every version and is the reason the fact does not
    carry it."""
    frame = spark.read.table(fact_table)
    company = spark.read.table(dim_company_table)
    for counterparty, key in FACT_PAYMENT.roles:
        frame = frame.join(
            company.select(
                F.col(DIM_COMPANY.surrogate_key).alias(key),
                F.col(NATURAL_KEY).alias(counterparty),
            ),
            on=key,
            how="left",
        )
    for dimension in (DIM_CHANNEL, DIM_CURRENCY):
        frame = frame.join(
            spark.read.table(conformed_tables[dimension.name]).select(
                F.col(dimension.surrogate_key).alias(fact_keys(dimension)[0]),
                F.col(dimension.natural_key).alias(dimension.fact_column),
            ),
            on=fact_keys(dimension)[0],
            how="left",
        )
    return frame


def version_of(spark, dim_company_table, surrogate_key: int):
    """The one `dim_company` row a surrogate key names."""
    return (
        spark.read.table(dim_company_table)
        .where(F.col(DIM_COMPANY.surrogate_key) == surrogate_key)
        .collect()
    )


def row_of(spark, fact_table, identity: str):
    return (
        spark.read.table(fact_table)
        .where(F.col(FACT_PAYMENT.grain_key) == identity)
        .collect()
    )
