"""`hub_empresa` and `sat_empresa_dados` over a fixture shaped like real bronze.

THIS FILE IS HALF OF THE EVIDENCE, DELIBERATELY, AND SAYS SO. The phase's premise is
that `hash_diff` detects a real change across two real snapshots, and the only way to
show that is against 69M rows in Databricks -- which CI cannot do, because
`.github/workflows/ci.yml` carries `GITHUB_TOKEN` and nothing else and runs local
Spark. So the proof is two artefacts: this fixture, which guards the MECHANIC on every
push, and a measurement against real bronze recorded in the task report. Neither
stands in for the other, and this file must not be read as evidence about the RFB's
data.

WHAT THE FIXTURE MIRRORS AND WHERE IT DELIBERATELY DOES NOT. The measured shape
between 2026-06-13 and 2026-07-11 (`01f19061-4f47-1b47-ab3a-1880491dda04`) is:
105,820 companies changed `razao_social`, 50,300 capital, 26,724 natureza jurídica,
9,440 porte -- and **zero keys departed**, because the RFB retains baixadas rather
than removing them. One company per changed column, plus an unchanged control, is
that shape at the smallest size that carries it.

`C_DEPARTED` IS THE ONE ROW THAT MIRRORS NOTHING REAL, and it is here on purpose.
Real empresas exercises no departure at all, so the branch that must write no row and
no end-date for an absent key would be untested on this table -- and a satellite that
silently end-dated would pass a fixture built strictly to the measurement. Tasks 4 and
5 have real departures (4 on estabelecimentos, 65,444 on socios at link grain); this
fixture carries a synthetic one so the path they need is exercised here first. The
task report states plainly that the real data does not exercise it.
"""
from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pyspark.sql import functions as F

from opl.bronze.autoloader import add_audit_columns
from opl.vault import domains
from opl.vault.columns import APPLIED_DATE, HASH_DIFF, LOAD_DATE, RECORD_SOURCE
from opl.vault.hashing import hash_key
from opl.vault.hubs import load_hub
from opl.vault.loading import (
    BRONZE_RECORD_SOURCE,
    SNAPSHOT_MONTH_COLUMN,
    SNAPSHOT_REF_DATE_COLUMN,
)
from opl.vault.observation import ObservationGrain
from opl.vault.registry import BusinessKeyColumn, Hub, Satellite
from opl.vault.satellites import load_satellite

HUB = domains.table_spec("hub_empresa")
SAT = domains.table_spec("sat_empresa_dados")

JUN, JUL = "2026-06", "2026-07"
JUN_REF, JUL_REF = date(2026, 6, 13), date(2026, 7, 11)

# FAR FROM EITHER REF DATE, AND THAT IS THE POINT. `load_date` is when WE loaded and
# `applied_date` is when the fact was true at the source; a loader that crossed them
# would still produce two plausible-looking columns. A load stamped in 2027 cannot be
# confused with a snapshot taken in 2026.
LOADED_AT = datetime(2027, 3, 1, 9, 30, 0)
RELOADED_AT = datetime(2027, 4, 2, 18, 15, 0)

RECORD_SOURCE_VALUE = "rfb_cnpj_webdav"

_EMPRESAS = (
    "cnpj_basico string, razao_social string, natureza_juridica string, "
    "qualificacao_responsavel string, capital_social string, porte_empresa string, "
    "ente_federativo_responsavel string, _rescued_data string, _source_file string, "
    "_ingested_at timestamp, _record_source string, _batch_id string, "
    "_snapshot_month string, _snapshot_ref_date date"
)
_REJECT_REASON = ", _dq_reject_reason string"

C_NAME = "10000001"       # razão social changes -- the acceptance test's subject
C_UNCHANGED = "10000002"  # nothing changes -- the acceptance test's control
C_CAPITAL = "10000003"
C_NATUREZA = "10000004"
C_PORTE = "10000005"
C_NEW_IN_JULY = "10000006"
C_DEPARTED = "10000007"   # June only. Synthetic -- see the module docstring.
C_OFF_PAYLOAD = "10000008"  # only non-payload columns change
C_QUARANTINED = "10000009"  # rejected in both months, never in bronze

_REF_DATES = {JUN: JUN_REF, JUL: JUL_REF}


def _row(
    cnpj: str,
    month: str,
    *,
    razao: str = "ACME LTDA",
    natureza: str = "2062",
    capital: str = "1000,00",
    porte: str = "05",
    qualificacao: str = "49",
    ente: str | None = None,
) -> tuple:
    """One bronze empresas row. Every audit column populated as the ingest stamps
    them, because the loaders read three of them and must not be handed a shape
    bronze does not have."""
    return (
        cnpj, razao, natureza, qualificacao, capital, porte, ente,
        None, f"/Volumes/x/cnpj/{month}/empresas/K3241.K03200Y0.D60613.EMPRECSV",
        datetime(2026, 8, 1, 0, 0, 0), RECORD_SOURCE_VALUE, "batch-1",
        month, _REF_DATES[month],
    )


def _write(spark, table: str, schema: str, rows: list[tuple]) -> None:
    (spark.createDataFrame(rows, schema)
     .write.format("delta").mode("append").saveAsTable(table))


@pytest.fixture(scope="module")
def source(spark, tmp_path_factory):
    """A throwaway Delta database holding one bronze empresas table and its
    quarantine, in the two months real bronze has.

    Delta rather than temp views, and module-scoped, for the reasons
    `tests/vault/test_observation.py::tables` measured: a managed Delta table is a
    file scan with a reusable plan, where a view over `createDataFrame`
    re-materialises from the driver on every query."""
    db = f"cnpj_vault_{uuid4().hex[:8]}"
    root = tmp_path_factory.mktemp("cnpj_vault")
    spark.sql(f"CREATE DATABASE {db} LOCATION '{root.as_uri()}'")
    bronze, quarantine = f"{db}.empresas", f"{db}.empresas_q"

    _write(spark, bronze, _EMPRESAS, [
        _row(C_NAME, JUN, razao="ACME LTDA"),
        _row(C_NAME, JUL, razao="ACME PARTICIPACOES SA"),
        _row(C_UNCHANGED, JUN, razao="BETA SA"),
        _row(C_UNCHANGED, JUL, razao="BETA SA"),
        _row(C_CAPITAL, JUN, capital="1000,00"),
        _row(C_CAPITAL, JUL, capital="5000,00"),
        _row(C_NATUREZA, JUN, natureza="2062"),
        _row(C_NATUREZA, JUL, natureza="2038"),
        _row(C_PORTE, JUN, porte="01"),
        _row(C_PORTE, JUL, porte="05"),
        _row(C_NEW_IN_JULY, JUL, razao="GAMMA ME"),
        _row(C_DEPARTED, JUN, razao="DELTA EIRELI"),
        # Two columns that are NOT in the satellite's payload move between the
        # months. The satellite must not notice.
        _row(C_OFF_PAYLOAD, JUN, qualificacao="49", ente=None),
        _row(C_OFF_PAYLOAD, JUL, qualificacao="16", ente="UNIAO"),
    ])
    _write(spark, quarantine, _EMPRESAS + _REJECT_REASON, [
        (*_row(C_QUARANTINED, JUN, razao=""), "null_or_empty_razao_social"),
        (*_row(C_QUARANTINED, JUL, razao=""), "null_or_empty_razao_social"),
    ])

    grain = ObservationGrain(
        name="hub_empresa", bronze_table=bronze, quarantine_table=quarantine,
        key_columns=("cnpj_basico",),
    )
    yield SimpleNamespace(db=db, bronze=bronze, quarantine=quarantine, grain=grain)
    spark.sql(f"DROP DATABASE {db} CASCADE")


@pytest.fixture
def target(source):
    """Fresh hub and satellite table names per test: these are the tables under
    test and every load writes to them, so sharing one across tests would make
    idempotence pass for the wrong reason."""
    suffix = uuid4().hex[:8]
    return SimpleNamespace(hub=f"{source.db}.hub_{suffix}", sat=f"{source.db}.sat_{suffix}")


def _load_hub(spark, source, target, *, load_date=LOADED_AT, months=None):
    return load_hub(
        spark, HUB, source_table=source.bronze, target_table=target.hub,
        load_date=load_date, months=months,
    )


def _load_sat(spark, source, target, *, load_date=LOADED_AT, months=None):
    return load_satellite(
        spark, SAT, hub=HUB, source_table=source.bronze, target_table=target.sat,
        load_date=load_date, grain=source.grain, months=months,
    )


def _sat_rows(spark, target) -> dict[tuple[str, date], dict]:
    """`{(cnpj_basico, applied_date): row}` for the whole satellite, joined back to
    the hub's business key so the assertions can read a company rather than a digest.

    The WHOLE satellite, not the rows a test cares about: an equality assertion
    against this dict fails on a missing row, an extra row and a wrong value alike."""
    joined = (
        spark.read.table(target.sat).alias("s")
        .join(spark.read.table(target.hub).alias("h"), HUB.hash_key)
    )
    return {
        (row["cnpj_basico"], row[APPLIED_DATE]): row.asDict() for row in joined.collect()
    }


@pytest.fixture(scope="module")
def loaded(spark, source, tmp_path_factory):
    """One hub load and one satellite load over both months, shared by every
    read-only assertion below.

    Module-scoped because a Delta `saveAsTable` on this box costs seconds and none of
    the tests using it writes. Tests that load again (idempotence, the incremental
    path, every refusal) take the function-scoped `target` fixture instead."""
    names = SimpleNamespace(hub=f"{source.db}.hub_shared", sat=f"{source.db}.sat_shared")
    hub_result = _load_hub(spark, source, names)
    sat_result = _load_sat(spark, source, names)
    names.hub_result, names.sat_result = hub_result, sat_result
    return names


# --------------------------------------------------------------------------- #
# The acceptance test
# --------------------------------------------------------------------------- #

def test_the_satellite_holds_two_rows_for_a_changed_company_and_one_for_an_unchanged(
    spark, loaded
):
    """THE ACCEPTANCE TEST, and the phase's premise with it.

    `C_NAME`'s razão social changes between the two snapshots and `C_UNCHANGED`'s does
    not, from identical source shapes -- same table, same months, same columns. Two
    rows against one is the entire claim that `hash_diff` is delta-driven rather than
    snapshot-driven, and it is the number that separates this satellite from a copy of
    bronze with extra columns.

    The applied dates are asserted too, not just the counts: two rows both stamped
    2026-06-13 would satisfy a count-only assertion and would be a satellite that had
    written the same observation twice."""
    rows = _sat_rows(spark, loaded)

    changed = sorted(applied for cnpj, applied in rows if cnpj == C_NAME)
    unchanged = sorted(applied for cnpj, applied in rows if cnpj == C_UNCHANGED)

    assert changed == [JUN_REF, JUL_REF]
    assert unchanged == [JUN_REF]
    assert rows[(C_NAME, JUN_REF)]["razao_social"] == "ACME LTDA"
    assert rows[(C_NAME, JUL_REF)]["razao_social"] == "ACME PARTICIPACOES SA"
    assert rows[(C_UNCHANGED, JUN_REF)]["razao_social"] == "BETA SA"


@pytest.mark.parametrize(
    ("company", "column", "before", "after"),
    [
        (C_NAME, "razao_social", "ACME LTDA", "ACME PARTICIPACOES SA"),
        (C_CAPITAL, "capital_social", "1000,00", "5000,00"),
        (C_NATUREZA, "natureza_juridica", "2062", "2038"),
        (C_PORTE, "porte_empresa", "01", "05"),
    ],
)
def test_a_change_in_each_payload_column_on_its_own_produces_a_second_row(
    spark, loaded, company, column, before, after
):
    """One company per payload column, each moving ONLY that column.

    Parameterised rather than folded into the acceptance test because `hash_diff` is
    a hash over four columns and a bug that dropped one of them from the hash would
    leave the other three detecting change perfectly. All four are real populations:
    razão social 105,820, capital 50,300, natureza jurídica 26,724, porte 9,440."""
    rows = _sat_rows(spark, loaded)

    assert sorted(applied for cnpj, applied in rows if cnpj == company) == [JUN_REF, JUL_REF]
    assert rows[(company, JUN_REF)][column] == before
    assert rows[(company, JUL_REF)][column] == after


def test_a_change_outside_the_payload_produces_no_second_row(spark, loaded):
    """`qualificacao_responsavel` and `ente_federativo_responsavel` are columns of the
    empresas contract that this satellite deliberately does not carry. Both move for
    `C_OFF_PAYLOAD` between the months.

    The mirror image of the test above, and it is what stops the obvious
    over-correction: a `hash_diff` taken over every source column would make the four
    tests above pass and would write a row every time an unmodelled column twitched."""
    rows = _sat_rows(spark, loaded)

    assert sorted(applied for cnpj, applied in rows if cnpj == C_OFF_PAYLOAD) == [JUN_REF]


# --------------------------------------------------------------------------- #
# load_date, applied_date, and the two never crossing
# --------------------------------------------------------------------------- #

def test_applied_date_is_the_snapshot_ref_date_and_load_date_is_the_callers(spark, loaded):
    """The separation this whole phase exists for, asserted as an inequality rather
    than as two shapes.

    `applied_date` is when the fact was true at the SOURCE -- the date the RFB puts in
    its own filename, 2026-06-13 and 2026-07-11, which is not month-end and is not
    derivable from `_snapshot_month`. `load_date` is when WE loaded, and it is the
    same value on both of `C_NAME`'s rows because one load wrote them both.

    A loader that sourced `applied_date` from the load clock would make both rows
    carry 2027-03-01 and the history would collapse; one that sourced `load_date` from
    the snapshot would make the two rows disagree about when we ran."""
    rows = _sat_rows(spark, loaded)

    june, july = rows[(C_NAME, JUN_REF)], rows[(C_NAME, JUL_REF)]

    assert (june[APPLIED_DATE], july[APPLIED_DATE]) == (JUN_REF, JUL_REF)
    assert june[LOAD_DATE] == july[LOAD_DATE] == LOADED_AT
    assert {row[LOAD_DATE] for row in rows.values()} == {LOADED_AT}
    assert {row[RECORD_SOURCE] for row in rows.values()} == {RECORD_SOURCE_VALUE}


def test_the_load_date_is_the_one_the_caller_passed_and_not_a_clock(spark, source, target):
    """Injectable, and asserted by loading the SAME data twice with two different
    stamps into two different targets. A loader calling `now()` internally cannot be
    asserted against at all -- the test would have to compare against another
    `now()`, which passes for any implementation."""
    _load_hub(spark, source, target, load_date=LOADED_AT)
    other = SimpleNamespace(hub=f"{target.hub}_b", sat=f"{target.sat}_b")
    _load_hub(spark, source, other, load_date=RELOADED_AT)

    first = {row[LOAD_DATE] for row in spark.read.table(target.hub).collect()}
    second = {row[LOAD_DATE] for row in spark.read.table(other.hub).collect()}

    assert first == {LOADED_AT}
    assert second == {RELOADED_AT}


# --------------------------------------------------------------------------- #
# The hub
# --------------------------------------------------------------------------- #

def test_the_hub_holds_exactly_one_row_per_business_key(spark, loaded):
    """Eight bronze keys, eight hub rows, eight distinct hash keys.

    The distinctness half is the one that matters: a hub with the right ROW COUNT and
    two companies sharing a digest is the worst failure this layer can produce, and it
    is invisible to a count."""
    rows = spark.read.table(loaded.hub).collect()

    assert len(rows) == 8
    assert len({row["cnpj_basico"] for row in rows}) == 8
    assert len({row[HUB.hash_key] for row in rows}) == 8
    assert C_QUARANTINED not in {row["cnpj_basico"] for row in rows}


def test_reloading_the_hub_appends_nothing(spark, source, target):
    """Idempotence, in the shape the bronze promote settled on: a re-run must not
    duplicate. The second load is stamped with a DIFFERENT `load_date`, so a row
    silently rewritten rather than skipped would show up as a changed stamp even if
    the count happened to hold."""
    first = _load_hub(spark, source, target, load_date=LOADED_AT)
    second = _load_hub(spark, source, target, load_date=RELOADED_AT)

    rows = spark.read.table(target.hub).collect()

    assert (first.appended, second.appended) == (8, 0)
    assert second.already_present == 8
    assert len(rows) == 8
    assert {row[LOAD_DATE] for row in rows} == {LOADED_AT}


def test_a_key_first_seen_in_the_later_month_enters_the_hub_when_that_month_loads(
    spark, source, target
):
    """The incremental path: June alone gives seven keys, and July adds the one born
    in it. `C_DEPARTED` is in June and stays in the hub afterwards -- a hub is
    insert-only and a departure is not a delete."""
    june = _load_hub(spark, source, target, months=[JUN])
    both = _load_hub(spark, source, target, months=[JUN, JUL])

    keys = {row["cnpj_basico"] for row in spark.read.table(target.hub).collect()}

    assert (june.appended, both.appended) == (7, 1)
    assert C_NEW_IN_JULY in keys
    assert C_DEPARTED in keys


def test_the_hub_hash_key_is_the_digest_the_python_standard_gives(spark, loaded):
    """The tie between the loader and `opl.vault.hashing`, at the value level.

    `tests/vault/test_hashing_spark.py` proves the two SPELLINGS agree; this proves
    the LOADER calls the standard on the right components -- an eight-character
    zero-padded `cnpj_basico`, alone, and nothing else. A loader that hashed the raw
    column plus the month would pass every equivalence test in that file."""
    rows = {
        row["cnpj_basico"]: row[HUB.hash_key] for row in spark.read.table(loaded.hub).collect()
    }

    assert rows[C_NAME] == hash_key([C_NAME])
    assert rows[C_UNCHANGED] == hash_key([C_UNCHANGED])


def test_the_satellite_keys_on_the_same_digest_as_the_hub(spark, loaded):
    """A satellite whose hash key does not equal its hub's is a satellite that joins
    to nothing -- and joins to nothing SILENTLY, as an empty result rather than an
    error. Both loaders derive the key from the same `Hub` spec, and this is what says
    so at the value level."""
    hub_keys = {row[HUB.hash_key] for row in spark.read.table(loaded.hub).collect()}
    sat_keys = {row[HUB.hash_key] for row in spark.read.table(loaded.sat).collect()}

    assert sat_keys
    assert sat_keys <= hub_keys


def test_the_hub_carries_exactly_the_dv2_metadata_and_its_business_key(spark, loaded):
    """Per master spec section 4.2: hash key, `load_date` (LDTS) and `record_source`
    (RSRC), plus the business key itself. Pinned as a list so a column arriving or
    leaving is a deliberate edit."""
    assert spark.read.table(loaded.hub).columns == [
        HUB.hash_key, "cnpj_basico", LOAD_DATE, RECORD_SOURCE
    ]


# --------------------------------------------------------------------------- #
# The satellite: absence, end-dating, and re-runs
# --------------------------------------------------------------------------- #

def test_the_satellite_has_no_end_date_column_at_all(spark, loaded):
    """THE MUTATION-RESISTANT HALF of "no inferred end-date".

    A test asserting that a departed key's row is not end-dated passes trivially if
    there is no end-date column -- and would keep passing if someone added the column
    and populated it only for OTHER keys. Pinning the whole column list is what makes
    "this satellite does not end-date" a property of the table rather than of the one
    row a test happened to look at.

    Absence is expressed by the observation ledger, which is derived and never
    materialised here; see ADR 0010."""
    assert spark.read.table(loaded.sat).columns == [
        HUB.hash_key, LOAD_DATE, APPLIED_DATE, RECORD_SOURCE, HASH_DIFF,
        "razao_social", "natureza_juridica", "capital_social", "porte_empresa",
    ]


def test_a_departed_key_gets_no_row_for_the_month_it_is_absent_from(spark, loaded):
    """`C_DEPARTED` is in June's bronze and absent from July's. It gets its June row
    and NOTHING for July -- no row saying the payload changed, no row saying it did
    not, and no end-date on the June row.

    The count is reported instead: the ledger says one key is a candidate delete over
    this window, and the satellite acted on it in no way whatsoever. That is the
    reported-not-acted-on shape ADR 0010 asks for -- a key vanishing because our own
    gate rejected it must never reach a satellite as a departure from the registry,
    and the only way to keep that true is for the satellite to have no departure
    behaviour to get wrong.

    REAL EMPRESAS EXERCISES NONE OF THIS: all 68,629,147 keys of 2026-06 are present
    in 2026-07. `C_DEPARTED` is synthetic, and the task report says so."""
    rows = _sat_rows(spark, loaded)

    assert sorted(applied for cnpj, applied in rows if cnpj == C_DEPARTED) == [JUN_REF]
    assert loaded.sat_result.candidate_departures == 1


def test_a_key_only_ever_in_quarantine_reaches_neither_table(spark, loaded):
    """`C_QUARANTINED` fails `null_or_empty_razao_social` in both months and reaches
    bronze in neither. The ledger calls it `rejected_by_our_gate`, which is an absence
    WE caused -- and the vault's answer to that is silence, in both the hub and the
    satellite, rather than a row asserting anything about a company we threw away.

    THIS PASSES BY CONSTRUCTION AND THE DOCSTRING SAYS SO RATHER THAN IMPLYING A GUARD:
    both loaders read bronze, and a key that never reached bronze has no candidate row
    to be filtered out of. There is no branch here to delete, so no mutation of the
    loaders turns this red. It is worth keeping because the CONSTRUCTION is the
    property -- a future loader sourced from bronze UNION quarantine, or one that
    treated a quarantined key as a departure, is exactly what it would catch."""
    rows = _sat_rows(spark, loaded)

    assert not [cnpj for cnpj, _ in rows if cnpj == C_QUARANTINED]
    assert C_QUARANTINED not in {
        row["cnpj_basico"] for row in spark.read.table(loaded.hub).collect()
    }


def test_reloading_the_satellite_appends_nothing(spark, source, target):
    """The re-run, again with a different `load_date` so a rewritten row is visible
    as a changed stamp and not only as a changed count."""
    _load_hub(spark, source, target)
    first = _load_sat(spark, source, target, load_date=LOADED_AT)
    second = _load_sat(spark, source, target, load_date=RELOADED_AT)

    rows = spark.read.table(target.sat).collect()

    assert second.appended == 0
    assert len(rows) == first.appended
    assert {row[LOAD_DATE] for row in rows} == {LOADED_AT}


def test_loading_july_after_june_appends_only_what_changed(spark, source, target):
    """The real operating shape: June is already in the satellite when July's snapshot
    arrives, so the comparison has to be against the PERSISTED row rather than against
    another row in the same batch.

    June writes one row per company present in June (seven). July then writes only the
    four whose payload moved plus the one born in July -- and writes nothing for
    `C_UNCHANGED` or `C_OFF_PAYLOAD`, whose payloads did not."""
    _load_hub(spark, source, target, months=[JUN, JUL])
    june = _load_sat(spark, source, target, months=[JUN])
    july = _load_sat(spark, source, target, months=[JUN, JUL])

    rows = _sat_rows(spark, target)

    assert (june.appended, july.appended) == (7, 5)
    assert sorted(applied for cnpj, applied in rows if cnpj == C_UNCHANGED) == [JUN_REF]
    assert sorted(applied for cnpj, applied in rows if cnpj == C_NAME) == [JUN_REF, JUL_REF]
    assert sorted(applied for cnpj, applied in rows if cnpj == C_NEW_IN_JULY) == [JUL_REF]


# --------------------------------------------------------------------------- #
# Refusals
# --------------------------------------------------------------------------- #

def test_a_month_no_snapshot_ever_loaded_is_refused_before_anything_is_written(
    spark, source, target
):
    """The observation ledger's window guard, reached through the satellite -- and
    this is what makes consulting the ledger load-bearing rather than decorative here.

    Without it, `months=['2026-09']` selects no bronze row, the satellite writes
    nothing and reports success. With it, the load refuses: a month with no row on
    either side would make every key in the window a candidate delete, conjured out of
    a typo or an ingest that never ran."""
    _load_hub(spark, source, target)

    with pytest.raises(ValueError, match="2026-09"):
        _load_sat(spark, source, target, months=[JUN, JUL, "2026-09"])

    assert not spark.catalog.tableExists(target.sat)


def test_a_satellite_loaded_against_a_hub_that_is_not_its_parent_is_refused(
    spark, source, target
):
    """The two arguments are separate because the loaders must not depend on the
    global registry to be testable -- which means nothing but this check stops them
    being mismatched. A satellite keyed on the wrong hub's digest joins to nothing,
    and reports success doing it."""
    stranger = Hub(
        name="hub_estabelecimento", hash_key="hub_estabelecimento_hk",
        business_keys=(BusinessKeyColumn(name="cnpj_basico", width=8),),
    )

    with pytest.raises(ValueError, match="hub_empresa"):
        load_satellite(
            spark, SAT, hub=stranger, source_table=source.bronze,
            target_table=target.sat, load_date=LOADED_AT, grain=source.grain,
        )


def test_an_overlong_business_key_fails_the_load_rather_than_merging_two_companies(
    spark, source, target
):
    """Spark's `lpad` TRUNCATES: `lpad('100000012', 8, '0')` is `'10000001'`, which is
    `C_NAME`'s real key. Left unguarded, a nine-character `cnpj_basico` would not
    produce a bad row -- it would merge two different companies onto one hub key and
    interleave their two histories in one satellite.

    Bronze refuses this upstream (`cnpj_basico_len8` is a Delta CHECK constraint on
    the empresas table, and the DQ gate rejects the rows before the promote), so this
    is defence in depth over a table those two already cover. It is here because the
    consequence is unrecoverable and the guard is one branch."""
    bad = f"{source.db}.overlong_{uuid4().hex[:8]}"
    _write(spark, bad, _EMPRESAS, [_row("100000012", JUN), _row(C_NAME, JUN)])

    with pytest.raises(Exception, match="refusing to truncate"):
        load_hub(
            spark, HUB, source_table=bad, target_table=target.hub, load_date=LOADED_AT
        )


def test_a_source_column_that_is_not_a_string_is_refused_by_name(spark, source, target):
    """Every column the CNPJ contracts declare is STRING, `capital_social` included,
    and that is deliberate -- alphanumeric CNPJs take effect 2026-07-31 and a numeric
    capital would carry a locale in its formatting.

    A typed source column is not a small divergence: Spark casts it silently and hashes
    the cast, where `hash_key` would raise on a value with no `.strip()`. One spelling
    answers and the other refuses, which no equivalence test over string corpora can
    see -- so it is refused at the boundary instead."""
    typed = f"{source.db}.typed_{uuid4().hex[:8]}"
    (spark.read.table(source.bronze)
     .withColumn("capital_social", F.col("capital_social").cast("double"))
     .write.format("delta").mode("append").saveAsTable(typed))
    _load_hub(spark, source, target)

    with pytest.raises(TypeError, match="capital_social"):
        load_satellite(
            spark, SAT, hub=HUB, source_table=typed, target_table=target.sat,
            load_date=LOADED_AT, grain=source.grain,
        )


def test_the_bronze_audit_columns_this_layer_reads_are_the_ones_the_ingest_writes(spark):
    """`opl.vault.loading` names three bronze columns, and one of them
    (`_record_source`) is a SECOND SPELLING of a bare literal in
    `opl.bronze.autoloader.add_audit_columns` -- there is no constant there to import.

    So it is cross-checked rather than trusted, in the shape
    `_assert_prefixes_match_their_file_groups` uses on the registry's `prefix` field:
    this runs the real stamping function and asserts the vault reads the columns it
    writes. Renamed there and not here, the loaders would select a column that is not
    present and fail in Spark, naming the column and nothing about why.

    The record-source VALUE is pinned too, because this module's fixture asserts
    against it -- a fixture carrying a value bronze does not produce would make every
    `record_source` assertion above self-referential."""
    stamped = add_audit_columns(
        spark.createDataFrame(
            [("/Volumes/x/cnpj/2026-06/empresas/K3241.K03200Y0.D60613.EMPRECSV",)],
            "_source_file string",
        ),
        batch_id="batch-1",
        snapshot_month=JUN,
    )

    assert {BRONZE_RECORD_SOURCE, SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN} <= set(
        stamped.columns
    )
    assert stamped.first()[BRONZE_RECORD_SOURCE] == RECORD_SOURCE_VALUE
    assert stamped.first()[SNAPSHOT_REF_DATE_COLUMN] == JUN_REF


def test_a_satellite_whose_payload_column_is_missing_from_the_source_is_refused(
    spark, source, target
):
    """A payload column present in the spec and absent from bronze would otherwise
    surface as a Spark `AnalysisException` naming neither the satellite nor the
    table."""
    absent = Satellite(
        name="sat_empresa_dados", parent="hub_empresa", payload_columns=("nome_fantasia",)
    )

    with pytest.raises(ValueError, match="nome_fantasia"):
        load_satellite(
            spark, absent, hub=HUB, source_table=source.bronze,
            target_table=target.sat, load_date=LOADED_AT, grain=source.grain,
        )
