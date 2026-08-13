"""`pit_estabelecimento`: the point-in-time table, and the collapse it exists to defeat.

THE TEST THIS FILE IS BUILT AROUND IS A COMPARISON, NOT AN INVARIANT. A PIT table that
merely exists proves nothing -- every row of it can be well-formed while the mechanism it
is named for does nothing. So the closing test below runs BOTH queries over one fixture:
the naive `(hash key, applied_date)` equi-join at the later snapshot, and the as-of answer
the PIT delivers, and asserts the first returns strictly fewer keys than the second. It
then names a key present in the second and absent from the first, and reads that key's two
pointers to show its address comes from the EARLIER snapshot while its registration data
comes from the later one.

THE PHASE PLAN'S ORIGINAL CLOSING TEST COULD NOT HAVE FIRED, and this file is what
replaced it. It asked for "a key whose two satellites changed on DIFFERENT
`applied_date`s"; there are exactly two snapshot dates and a second version can only land
on the later one, so the query returns zero for a vault in which the mechanism is
perfectly exercised. It would have retired a real mechanism on a false negative
(`docs/f3-run-evidence.md` §0.5, T2).

THE FIXTURE IS BUILT BY THE VAULT'S OWN LOADERS, for `tests/gold/conftest.py`'s reason:
satellites written directly by `createDataFrame` would let this table pass against a
version chain the vault cannot actually produce. Its six establishments are the real
populations in miniature, one row of Task 0's decomposition each -- see `_KEYS` below.

NOTHING IS IMPORTED FROM `tests/vault/conftest.py`, which owns an estabelecimentos
fixture of its own. Same reason `tests/gold/conftest.py` gives for not importing it: a
test module reaching across two `__init__.py`-bearing test packages that share no root
buys a collection-order dependency this suite does not otherwise have. What is copied is
DDL derived from the contract; what is not copied is any assertion."""
from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pyspark.sql import functions as F

from opl.contracts.cnpj_schemas import columns_for
from opl.gold.pit import PIT_RECORD_SOURCE, as_of_dates, load_pit, pit_rows
from opl.gold.registry import DIM_COMPANY, PIT_ESTABELECIMENTO
from opl.vault import domains
from opl.vault.columns import APPLIED_DATE, LOAD_DATE, RECORD_SOURCE
from opl.vault.hubs import load_hub
from opl.vault.observation import ObservationGrain
from opl.vault.satellites import load_satellite

from .conftest import AUDIT_DDL, BUILT_AT, INGESTED_AT, JUL, JUN, RECORD_SOURCE_VALUE, REF_DATES

PIT = PIT_ESTABELECIMENTO
HUB = domains.table_spec(PIT.hub)
SATELLITES = tuple(domains.table_spec(name) for name in PIT.satellites)
DADOS, ENDERECO = SATELLITES
DADOS_POINTER, ENDERECO_POINTER = PIT.pointer_columns

WINDOW = (JUN, JUL)
JUN_REF, JUL_REF = REF_DATES[JUN], REF_DATES[JUL]

CONTRACT = tuple(columns_for("estabelecimentos"))
BRONZE_SCHEMA = ", ".join(f"{column} string" for column in CONTRACT) + ", " + AUDIT_DDL
QUARANTINE_SCHEMA = BRONZE_SCHEMA + ", _dq_reject_reason string"

# THE SIX ESTABLISHMENTS, AND EACH ONE IS A ROW OF TASK 0's DECOMPOSITION
# (`docs/f3-run-evidence.md` §0.5, T2) rather than a convenient shape:
#
#   dados only    1,141,850 keys whose registration data moved in July and whose address
#                 did not -- a naive LEFT join hands each of them a NULL address that is
#                 sitting in the June row, still in force. THE KEY T-A NAMES.
#   endereco only   499,630 the other way round.
#   both             69,984 (`B`) -- the ONLY population the naive equi-join can see, and
#                 the reason it answers 514,504 where the as-of answer is 72,318,968.
#   neither      the 70.2M that moved in neither, invisible to the naive join as well.
#   july only       444,520 first observed in July; absent from the June as-of layer
#                 ENTIRELY, which is T-B's rule.
#   june only             4 observed in June and not in July -- 71,874,448 June keys
#                 against 71,874,444 in both. Still IN FORCE at 2026-07-11: nothing
#                 departs, so its June pointers carry forward. The population everyone
#                 forgets, and the reason the July layer is the hub's whole key set.
_KEYS = {
    "dados only": ("10000001", "0001", "23"),
    "endereco only": ("10000001", "0002", "04"),
    "both": ("20000002", "0001", "55"),
    "neither": ("20000003", "0001", "77"),
    "july only": ("30000003", "0001", "66"),
    "june only": ("40000004", "0001", "88"),
}

# Which as-of layers each key must appear in. Read by the tests rather than restated, so
# the predicted row count below has ONE source.
_LAYERS = {
    key: (JUN_REF, JUL_REF) if name != "july only" else (JUL_REF,)
    for name, key in _KEYS.items()
}

# THE PREDICTION, DERIVED FROM `_LAYERS` AND PUBLISHED BEFORE THE BUILD. Five keys at
# 2026-06-13 plus six at 2026-07-11 = eleven. Its real-data twin is 71,874,448 +
# 72,318,968 = 144,193,416, and the arithmetic is identical: keys first observed at or
# before the layer's date.
EXPECTED_ROWS = sum(len(layers) for layers in _LAYERS.values())

_DEFAULTS = {
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


def _row(key: tuple[str, str, str], month: str, **overrides) -> tuple:
    """One bronze estabelecimentos row: the whole contract plus every audit column the
    ingest stamps, because the vault loaders read three of them."""
    values = dict(_DEFAULTS)
    values.update(zip(("cnpj_basico", "cnpj_ordem", "cnpj_dv"), key, strict=True))
    values.update(overrides)
    return tuple(values[column] for column in CONTRACT) + (
        None,
        f"/Volumes/x/cnpj/{month}/estabelecimentos/K3241.K03200Y1.D60613.ESTABELE",
        INGESTED_AT,
        RECORD_SOURCE_VALUE,
        "batch-1",
        month,
        REF_DATES[month],
    )


def _bronze_rows() -> list[tuple]:
    """The fixture's bronze rows, meant to be read top to bottom -- the shape IS the
    argument, and each block is one entry of `_KEYS`."""
    return [
        _row(_KEYS["dados only"], JUN),
        _row(_KEYS["dados only"], JUL, situacao_cadastral="08",
             motivo_situacao_cadastral="01"),
        _row(_KEYS["endereco only"], JUN),
        _row(_KEYS["endereco only"], JUL, logradouro="DAS ACACIAS", numero="250"),
        _row(_KEYS["both"], JUN),
        _row(_KEYS["both"], JUL, situacao_cadastral="08", logradouro="DAS ACACIAS"),
        _row(_KEYS["neither"], JUN),
        _row(_KEYS["neither"], JUL),
        _row(_KEYS["july only"], JUL),
        _row(_KEYS["june only"], JUN),
    ]


@pytest.fixture(scope="module")
def estab_bronze(spark, gold_database):
    """One bronze estabelecimentos table and its (empty) quarantine, in two months."""
    db = gold_database("gold_pit")
    bronze, quarantine = f"{db}.estabelecimentos", f"{db}.estab_q"
    (
        spark.createDataFrame(_bronze_rows(), BRONZE_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(bronze)
    )
    (
        spark.createDataFrame([], QUARANTINE_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(quarantine)
    )
    grain = ObservationGrain(
        name=HUB.name, bronze_table=bronze, quarantine_table=quarantine,
        key_columns=HUB.business_key_columns,
    )
    return SimpleNamespace(db=db, bronze=bronze, quarantine=quarantine, grain=grain)


def load_vault(spark, source, names, *, months=WINDOW):
    """One `hub_estabelecimento` load and one load of each satellite, by the real
    loaders. Returns `names`, with the satellite tables mapped BY SATELLITE NAME -- which
    is the shape `load_pit` takes, so a test cannot swap two tables by hand."""
    load_hub(
        spark, HUB, source_table=source.bronze, target_table=names.hub,
        load_date=BUILT_AT, months=list(months),
    )
    for satellite in SATELLITES:
        load_satellite(
            spark, satellite, hub=HUB, source_table=source.bronze,
            target_table=names.satellites[satellite.name], load_date=BUILT_AT,
            grain=source.grain, months=list(months),
        )
    return names


def vault_names(db: str, suffix: str) -> SimpleNamespace:
    return SimpleNamespace(
        hub=f"{db}.hub_{suffix}",
        satellites={
            satellite.name: f"{db}.sat_{index}_{suffix}"
            for index, satellite in enumerate(SATELLITES)
        },
        pit=f"{db}.pit_{suffix}",
    )


def build(spark, names, *, target=None, load_date=BUILT_AT, **overrides):
    """`pit_estabelecimento` over the vault tables in `names`, with the loader's own
    arguments overridable -- so a test that needs a mismatched pairing passes one rather
    than reaching into the loader."""
    arguments = {
        "hub": HUB,
        "hub_table": names.hub,
        "satellite_tables": names.satellites,
        "target_table": target or names.pit,
        "load_date": load_date,
    }
    arguments.update(overrides)
    return load_pit(spark, PIT, **arguments)


@pytest.fixture(scope="module")
def vault_loaded(spark, estab_bronze):
    """The vault over both months, shared by every read-only assertion."""
    return load_vault(spark, estab_bronze, vault_names(estab_bronze.db, "shared"))


@pytest.fixture(scope="module")
def pit_loaded(spark, vault_loaded):
    """One PIT build over that vault. Module-scoped because everything below it reads;
    the tests that LOAD AGAIN take `pit_target`."""
    result = build(spark, vault_loaded)
    return SimpleNamespace(names=vault_loaded, result=result, table=vault_loaded.pit)


@pytest.fixture
def pit_target(spark, estab_bronze):
    """A fresh vault AND a fresh PIT name per test, for the tests that write -- sharing
    one would make idempotence pass for the wrong reason."""
    return vault_names(estab_bronze.db, uuid4().hex[:8])


def hash_key_of(spark, names, key: tuple[str, str, str]) -> str:
    """The hub's own digest for one establishment, read FROM THE HUB rather than
    recomputed -- a test that re-derived it would be asserting against a second spelling
    of the standard, which is the thing `opl.vault.loading` exists to prevent."""
    business = dict(zip(HUB.business_key_columns, key, strict=True))
    row = (
        spark.read.table(names.hub)
        .where(" AND ".join(f"{column} = '{value}'" for column, value in business.items()))
        .select(HUB.hash_key)
        .collect()
    )
    assert len(row) == 1, f"{key} is not exactly one hub row: {row}"
    return row[0][0]


def naive_keys_at(spark, names, when: date) -> set[str]:
    """The keys a naive `(hash key, applied_date)` equi-join returns at `when`.

    THE QUERY EVERYBODY WRITES, spelled out here so the comparison is against the real
    alternative and not against a strawman: both satellites joined on the hash key AND on
    the snapshot date, which is what a reader does when they see `applied_date` in both
    tables and no PIT to route through."""
    frames = [
        spark.read.table(names.satellites[satellite.name])
        .where(F.col(APPLIED_DATE) == F.lit(when))
        .select(HUB.hash_key)
        for satellite in SATELLITES
    ]
    joined = frames[0].join(frames[1], on=HUB.hash_key, how="inner")
    return {row[0] for row in joined.collect()}


def pit_keys_at(spark, table: str, when: date) -> set[str]:
    return {
        row[0]
        for row in spark.read.table(table)
        .where(F.col(PIT.as_of_column) == F.lit(when))
        .select(HUB.hash_key)
        .collect()
    }


def pointers_of(spark, table: str, hash_key: str, when: date):
    rows = (
        spark.read.table(table)
        .where((F.col(HUB.hash_key) == hash_key) & (F.col(PIT.as_of_column) == F.lit(when)))
        .collect()
    )
    assert len(rows) == 1, f"{hash_key} has {len(rows)} rows at {when}, expected exactly 1"
    return rows[0]


# --- T-A: the PIT must DEMONSTRATE the collapse, not merely exist ---------------------


def test_the_naive_equi_join_answers_fewer_keys_than_the_pit_and_one_of_them_is_named(
    spark, vault_loaded, pit_loaded
):
    """T-A's closing test, and it is the reason this table is built at all.

    THE COMPARISON. At the later snapshot the naive join can only see keys carrying a row
    in BOTH satellites on that exact date; every key with a version in force is in the
    PIT. On real data that is 514,504 against 72,318,968, a gap of 71,804,464 rows
    (`docs/f3-run-evidence.md` §0.5). Here it is two against six, by the same arithmetic
    and over the same six populations.

    THE NAMED KEY. `dados only` changed its registration data in July and did not move.
    The naive join drops it -- there is no `_endereco` row at 2026-07-11 -- and a naive
    LEFT join would hand it a NULL address instead. The PIT holds it with its `_dados`
    pointer on the LATER snapshot and its `_endereco` pointer on the EARLIER one, which is
    the address that is still in force and that the naive answer cannot reach."""
    naive = naive_keys_at(spark, vault_loaded, JUL_REF)
    as_of = pit_keys_at(spark, pit_loaded.table, JUL_REF)
    assert naive < as_of, "the PIT must return a strict superset, or it earns nothing"
    assert len(naive) == 2 and len(as_of) == len(_KEYS) == 6

    named = hash_key_of(spark, vault_loaded, _KEYS["dados only"])
    assert named in as_of and named not in naive
    pointers = pointers_of(spark, pit_loaded.table, named, JUL_REF)
    assert pointers[DADOS_POINTER] == JUL_REF, "its registration data moved in July"
    assert pointers[ENDERECO_POINTER] == JUN_REF, (
        "its address is the JUNE row, still in force -- which is precisely what the naive "
        "join cannot see and what a naive LEFT join would return as NULL"
    )


def test_the_mirror_population_resolves_the_other_way_round(
    spark, vault_loaded, pit_loaded
):
    """The 499,630 in miniature: moved in July, registration data unchanged. Asserted
    because a PIT that only ever carried the LATER date forward on one side would pass the
    test above and be wrong on this one."""
    named = hash_key_of(spark, vault_loaded, _KEYS["endereco only"])
    pointers = pointers_of(spark, pit_loaded.table, named, JUL_REF)
    assert (pointers[DADOS_POINTER], pointers[ENDERECO_POINTER]) == (JUN_REF, JUL_REF)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("dados only", (JUL_REF, JUN_REF)),
        ("endereco only", (JUN_REF, JUL_REF)),
        ("both", (JUL_REF, JUL_REF)),
        ("neither", (JUN_REF, JUN_REF)),
        ("july only", (JUL_REF, JUL_REF)),
        ("june only", (JUN_REF, JUN_REF)),
    ],
)
def test_every_population_carries_the_version_in_force_at_the_later_snapshot(
    spark, vault_loaded, pit_loaded, name, expected
):
    """Every one of the six, at 2026-07-11, and the two that matter most are the ends:
    `neither` never moved and must carry June on both sides, and `june only` was not
    observed in July AT ALL and must still be present carrying June -- nothing departs, so
    its June version is the version in force."""
    named = hash_key_of(spark, vault_loaded, _KEYS[name])
    pointers = pointers_of(spark, pit_loaded.table, named, JUL_REF)
    assert (pointers[DADOS_POINTER], pointers[ENDERECO_POINTER]) == expected


# --- T-B: the grain, and the row count published before the run ----------------------


def test_the_grain_is_one_row_per_key_per_as_of_date_and_the_count_is_the_prediction(
    spark, vault_loaded, pit_loaded
):
    """T-B. The as-of set is MEASURED as the distinct union of `applied_date` across both
    satellites, the row count is the number of (key, as-of) pairs at or after each key's
    first observation, and both are published before the build rather than read off it.

    THE REAL-DATA TWIN OF THIS ARITHMETIC IS 71,874,448 + 72,318,968 = 144,193,416, and
    the second term is the HUB's whole key set rather than July's observed 72,318,964 --
    the four June-only keys are still in force."""
    result = pit_loaded.result
    assert result.as_of_dates == (JUN_REF, JUL_REF)
    assert result.appended == EXPECTED_ROWS == 11
    assert result.rows_per_as_of == ((JUN_REF, 5), (JUL_REF, 6))
    assert result.hub_keys == len(_KEYS) == 6
    duplicated = (
        spark.read.table(pit_loaded.table)
        .groupBy(HUB.hash_key, PIT.as_of_column).count()
        .where("count > 1").count()
    )
    assert duplicated == 0, "a key twice at one instant returns two versions to every read"


def test_a_key_first_observed_at_the_later_date_is_absent_from_the_earlier_layer(
    spark, vault_loaded, pit_loaded
):
    """T-B's closing test. `july only` did not exist at 2026-06-13 and must not appear
    there -- not with NULL pointers, not with a ghost, ABSENT. A row for it would assert
    that we had something to say about an establishment we had never seen, and every
    as-of count at that date would be wrong by the 444,520 keys of its real population."""
    named = hash_key_of(spark, vault_loaded, _KEYS["july only"])
    assert named not in pit_keys_at(spark, pit_loaded.table, JUN_REF)
    assert named in pit_keys_at(spark, pit_loaded.table, JUL_REF)
    assert len(pit_keys_at(spark, pit_loaded.table, JUN_REF)) == len(_KEYS) - 1


def test_every_row_carries_this_modules_own_record_source_and_the_builds_load_date(
    spark, pit_loaded
):
    """A PIT row carries no delivered value -- only a pointer this module computed -- so
    the RSRC it names is this module and not the RFB, which is the distinction
    `opl.gold.columns` draws for the conformed dimensions and for the ghost."""
    written = spark.read.table(pit_loaded.table)
    assert {row[0] for row in written.select(RECORD_SOURCE).distinct().collect()} == {
        PIT_RECORD_SOURCE
    }
    assert {row[0] for row in written.select(LOAD_DATE).distinct().collect()} == {BUILT_AT}


def test_the_projection_order_is_the_specs_own(spark, pit_loaded):
    """A Delta append matches POSITIONALLY unless `mergeSchema` says otherwise, so two
    builds projecting these columns in two orders would write each other's values with
    neither failing -- and the two pointer columns are the same type, so nothing about the
    result would look wrong."""
    assert spark.read.table(pit_loaded.table).columns == [
        HUB.hash_key, PIT.as_of_column, DADOS_POINTER, ENDERECO_POINTER,
        LOAD_DATE, RECORD_SOURCE,
    ]


# --- T-C: a DATE is never cast to a TIMESTAMP ----------------------------------------


@pytest.mark.parametrize("zone", ["UTC", "America/Sao_Paulo", "Asia/Tokyo"])
def test_the_as_of_dates_and_the_pointers_do_not_move_with_the_session_timezone(
    spark, vault_loaded, pit_target, zone
):
    """T-C's closing test, and it is a REGRESSION GUARD FOR A DEFECT THIS PROJECT SHIPPED
    TWICE. A `TIMESTAMP` is UTC micros and every conversion resolves through
    `spark.sql.session.timeZone`, which is `America/Sao_Paulo` on this box and UTC on
    serverless. Measured in this repository: `company_sk` took three different values
    under three session zones because it hashes `valid_from`, a TIMESTAMP derived from a
    DATE cast; and `CAST('2026-08-01T00:00:00.000Z' AS TIMESTAMP)` lands on 2026-07-31
    here (`opl.gold.conformed.day_of` and its test).

    `pit_estabelecimento` NEVER CASTS `applied_date`. Its as-of dates are DATE literals
    built through `DateType.toInternal` -- days since epoch, no zone -- and its pointers
    are the satellite's own DATE column carried through a `max`. This test builds the
    whole table under three zones and asserts the (key, as-of, pointer, pointer) tuples
    are identical.

    `load_date` IS DELIBERATELY NOT COMPARED, and that is a statement rather than an
    omission: it IS a TIMESTAMP, it DOES move with the session zone, and it is a record of
    when we ran rather than an answer the star gives. The columns this test covers are
    every column a reader joins or filters on."""
    original = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.session.timeZone", zone)
        result = build(spark, vault_loaded, target=pit_target.pit)
        read = {
            (row[HUB.hash_key], row[PIT.as_of_column],
             row[DADOS_POINTER], row[ENDERECO_POINTER])
            for row in spark.read.table(pit_target.pit).collect()
        }
    finally:
        spark.conf.set("spark.sql.session.timeZone", original)
    assert result.as_of_dates == (JUN_REF, JUL_REF)
    assert len(read) == EXPECTED_ROWS
    assert {as_of for _key, as_of, _a, _b in read} == {JUN_REF, JUL_REF}
    named = hash_key_of(spark, vault_loaded, _KEYS["dados only"])
    assert (named, JUL_REF, JUL_REF, JUN_REF) in read


# --- T-E: the joins, and the NULL that has cost this repository before ---------------


def test_a_satellite_carrying_a_null_applied_date_is_refused_before_anything_is_derived(
    spark, estab_bronze, vault_loaded, pit_target
):
    """T-E. `.plans/HANDOFF.md` records a `LEFT ANTI JOIN … USING` on a nullable key
    inventing **8,757 phantom departures**, because NULL never matches under plain
    equality. This module has exactly one join and both its keys are non-NULL -- the hash
    key because `opl.vault.hashing_spark._encoded` returns a token on its NULL branch too,
    and `as_of_date` because of this refusal.

    WHAT IT PREVENTS, precisely: a NULL `applied_date` becomes a member of the as-of set,
    then a NULL literal in the grid. Every comparison against it is NULL rather than
    false, so the window fills nothing; the re-run's anti-join never matches it, so the
    whole layer is appended again on every run; and the row count still looks like a
    multiple of the key count."""
    broken = f"{estab_bronze.db}.sat_null_applied_{uuid4().hex[:8]}"
    source = spark.read.table(vault_loaded.satellites[DADOS.name])
    (
        source.unionByName(
            source.limit(1).withColumn(APPLIED_DATE, F.lit(None).cast("date"))
        )
        .write.format("delta").mode("append").saveAsTable(broken)
    )
    tables = {**vault_loaded.satellites, DADOS.name: broken}
    with pytest.raises(ValueError, match=f"carries a NULL {APPLIED_DATE}"):
        build(spark, vault_loaded, target=pit_target.pit, satellite_tables=tables)


def test_the_second_build_over_an_unchanged_vault_appends_nothing(
    spark, estab_bronze, pit_target
):
    """`max_retries: 0` does not prevent a retry (master protocol §4.4), so the branch a
    retry lands in has to stay green. The anti-join is on (hash key, `as_of_date`), and it
    is correct under plain equality only because neither can be NULL."""
    load_vault(spark, estab_bronze, pit_target)
    first = build(spark, pit_target)
    again = build(spark, pit_target, load_date=datetime(2027, 9, 9, 9, 9, 9))
    assert first.appended == EXPECTED_ROWS
    assert (again.appended, again.already_present) == (0, EXPECTED_ROWS)
    assert again.rows_per_as_of == first.rows_per_as_of


def test_a_vault_that_gained_a_later_snapshot_appends_that_layer_and_moves_no_pointer(
    spark, estab_bronze, pit_target
):
    """THE PROPERTY THAT SEPARATES THIS LOADER FROM `opl.gold.dimensions`, and it is worth
    a test rather than a paragraph. An SCD2 chain must REFUSE a source that gained a
    snapshot -- a version's `valid_to` is the next version's `valid_from`, and an append
    cannot revise a closed interval. A PIT pointer is `max(applied_date <= as_of_date)`,
    which a LATER date cannot change, so the new snapshot is a new layer and nothing
    already written moves.

    Built here by loading June alone first, then both months -- which is exactly the
    operational shape, a table built before the next snapshot landed."""
    load_vault(spark, estab_bronze, pit_target, months=(JUN,))
    june_only = build(spark, pit_target)
    assert june_only.as_of_dates == (JUN_REF,)
    before = {
        (row[HUB.hash_key], row[DADOS_POINTER], row[ENDERECO_POINTER])
        for row in spark.read.table(pit_target.pit).collect()
    }

    load_vault(spark, estab_bronze, pit_target)
    grown = build(spark, pit_target, load_date=datetime(2027, 9, 9, 9, 9, 9))
    assert grown.as_of_dates == (JUN_REF, JUL_REF)
    assert grown.already_present == len(before) == 5
    assert grown.appended == EXPECTED_ROWS - 5 == 6
    after = {
        (row[HUB.hash_key], row[DADOS_POINTER], row[ENDERECO_POINTER])
        for row in spark.read.table(pit_target.pit)
        .where(F.col(PIT.as_of_column) == F.lit(JUN_REF)).collect()
    }
    assert after == before, "a later snapshot must not revise a pointer already written"


def test_a_vault_that_gained_an_earlier_snapshot_is_refused_before_the_first_write(
    spark, estab_bronze, pit_target
):
    """THE ONE CASE AN APPEND-ONLY PIT CANNOT DO, and the anti-join cannot see it: an
    out-of-order backfill REVISES pointers at as-of dates already written, and the (hash
    key, `as_of_date`) pair does not move -- so the re-run would drop every revised row as
    already present and leave the stale pointer in place.

    Built by loading JULY alone first, then both months, so the second build's new as-of
    date lands BEFORE one the table already holds. `opl.vault.loading.changed_rows`
    records the same hazard one layer down and reaches the same ruling: load in order."""
    load_vault(spark, estab_bronze, pit_target, months=(JUL,))
    july_only = build(spark, pit_target)
    assert july_only.as_of_dates == (JUL_REF,)
    load_vault(spark, estab_bronze, pit_target)
    with pytest.raises(ValueError, match="which is BEFORE"):
        build(spark, pit_target, load_date=datetime(2027, 9, 9, 9, 9, 9))


# --- the pairings a copied call would get wrong --------------------------------------


def test_a_pit_handed_another_hub_is_refused(spark, vault_loaded, pit_target):
    """Handed another hub the union is on a hash key of another name, so this either fails
    inside Spark's analysis or -- where two hubs spell their hash key alike -- merges two
    key spaces without failing. Refused by name before either can happen."""
    with pytest.raises(ValueError, match="was handed hub"):
        build(spark, vault_loaded, target=pit_target.pit,
              hub=domains.table_spec("hub_empresa"))


def test_a_pit_handed_a_table_for_a_satellite_it_does_not_name_is_refused(
    spark, vault_loaded, pit_target
):
    """The mapping is keyed BY SATELLITE NAME precisely so two tables cannot be swapped by
    position; this is the other half of that decision. A missing key would leave that
    satellite's pointer NULL on every row -- a build that reports success and answers "no
    address on record" for every establishment in the country."""
    tables = dict(vault_loaded.satellites)
    tables["sat_empresa_dados"] = tables.pop(ENDERECO.name)
    with pytest.raises(ValueError, match="was handed tables for"):
        build(spark, vault_loaded, target=pit_target.pit, satellite_tables=tables)
    with pytest.raises(ValueError, match="was handed tables for"):
        build(spark, vault_loaded, target=pit_target.pit,
              satellite_tables={DADOS.name: vault_loaded.satellites[DADOS.name]})


def test_the_pit_loader_refuses_a_spec_of_another_kind(spark, vault_loaded, pit_target):
    """`dim_company` is a registered gold table spelled correctly and is the single most
    plausible string to reach this loader from a copied job task. It has no hub, no
    satellites and no as-of column; handed here it must fail on the kind rather than
    several lines in on an attribute."""
    with pytest.raises(AttributeError):
        load_pit(
            spark, DIM_COMPANY, hub=HUB, hub_table=vault_loaded.hub,
            satellite_tables=vault_loaded.satellites, target_table=pit_target.pit,
            load_date=BUILT_AT,
        )


def test_the_as_of_set_is_measured_from_the_satellites_and_never_declared(
    spark, vault_loaded
):
    """Both ends come from the vault and neither is a literal in `src/opl/gold`. A pair of
    dates declared in a spec would be a second spelling of a value the vault owns, and it
    goes stale the day a snapshot lands -- which is `opl.gold.conformed.covered_span`'s
    argument, applied to the axis this table is indexed on."""
    assert as_of_dates(spark, PIT, vault_loaded.satellites) == (JUN_REF, JUL_REF)
    one = {DADOS.name: vault_loaded.satellites[DADOS.name]}
    assert as_of_dates(spark, PIT, one) == (JUN_REF, JUL_REF)


def test_the_frame_is_derivable_without_the_write(spark, vault_loaded):
    """`pit_rows` is public for `opl.gold.dimensions.dimension_rows`' reason -- the frame
    is worth having without the write, both for a test and for whatever reads it next."""
    rows = pit_rows(
        spark, PIT, hub=HUB, hub_table=vault_loaded.hub,
        satellite_tables=vault_loaded.satellites, dates=(JUN_REF, JUL_REF),
        load_date=BUILT_AT,
    )
    assert rows.count() == EXPECTED_ROWS
