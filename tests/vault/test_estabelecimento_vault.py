"""`hub_estabelecimento`, its two satellites and `link_empresa_estabelecimento`, over a
fixture shaped like real bronze.

THIS FILE IS HALF OF THE EVIDENCE, DELIBERATELY, exactly as `test_cnpj_vault.py` is:
CI runs local Spark with no Databricks credential, so the fixture guards the MECHANIC
on every push and the measurement against 72.3M real rows lives in the task report.
Neither stands in for the other.

WHAT THE FIXTURE MIRRORS. The measured estabelecimentos shape across 2026-06 and
2026-07 (controller run `01f191f3-6c96-15d2-84db-514bfcff2ce5`) is:

| month | observed | rejected_by_our_gate | absent_before_first | absent_after |
|---|---|---|---|---|
| 2026-06 | 71,874,448 | 0 | 444,520 | 0 |
| 2026-07 | 72,318,964 | 4 | 0 | **0** |

Every number that is not a volume is reproduced below: June's quarantine is EMPTY,
July's holds exactly four keys, those four are absent from July's bronze, one key is
born in July (the 444,520 in miniature), and **nothing is `absent_after_observation`
in either month**.

THE ACCEPTANCE TEST HERE IS ONE HALF OF TASK 2'S AND MUST NOT BE READ AS MORE. Those
four keys are the `rejected` half: our own DQ gate widened between the runs and
quarantined them (`encoding_replacement_char`), so their disappearance from bronze is
OURS. Because estabelecimentos has ZERO true departures, this table alone cannot tell a
correct ledger from one that labels every departure `rejected` -- it would pass both.
The other half is socios in Task 5 (65,444 departures, not one of them quarantined),
and the discrimination lives in
`tests/vault/test_observation.py::test_a_departure_reads_as_our_gate_on_one_table_and_
as_the_sources_on_the_other`, which carries both populations. What THIS file adds is
that the two satellites and the link, loaded from that data, record no change and no
departure for those four keys.

THE PADDING PAIR IS SYNTHETIC AND SAYS SO. `cnpj_basico`/`cnpj_ordem`/`cnpj_dv` are
8/4/2 characters on every one of the 72.3M real rows in both months, with zero
non-numeric values, so the zero-pads are defensive and the real data exercises none of
them. `E_SHORT_ORDEM` and `E_SHORT_TWIN` are here because an unpadded key is the way
this vault merges two establishments onto one hash key, and a fixture built strictly to
the measurement would leave that branch untested."""
from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from opl.contracts.cnpj_schemas import columns_for
from opl.vault import domains
from opl.vault.columns import APPLIED_DATE, HASH_DIFF, LOAD_DATE, RECORD_SOURCE
from opl.vault.domains.cnpj import UNMODELLED_ESTABELECIMENTO_COLUMNS
from opl.vault.hashing import hash_key
from opl.vault.hubs import load_hub
from opl.vault.links import load_link
from opl.vault.observation import (
    MONTH_COLUMN,
    STATE_COLUMN,
    ObservationGrain,
    ObservationState,
    observation_ledger,
)
from opl.vault.satellites import load_satellite

HUB = domains.table_spec("hub_estabelecimento")
EMPRESA_HUB = domains.table_spec("hub_empresa")
DADOS = domains.table_spec("sat_estabelecimento_dados")
ENDERECO = domains.table_spec("sat_estabelecimento_endereco")
LINK = domains.table_spec("link_empresa_estabelecimento")
LINK_HUBS = domains.linked_hubs(LINK)

OBSERVED = ObservationState.OBSERVED.value
REJECTED = ObservationState.REJECTED_BY_OUR_GATE.value
BEFORE = ObservationState.ABSENT_BEFORE_FIRST_OBSERVATION.value
AFTER = ObservationState.ABSENT_AFTER_OBSERVATION.value

JUN, JUL = "2026-06", "2026-07"
JUN_REF, JUL_REF = date(2026, 6, 13), date(2026, 7, 11)
_REF_DATES = {JUN: JUN_REF, JUL: JUL_REF}

# Far from either ref date, for `test_cnpj_vault.py`'s reason: a loader that crossed
# `load_date` with `applied_date` would still produce two plausible-looking columns.
LOADED_AT = datetime(2027, 3, 1, 9, 30, 0)
RELOADED_AT = datetime(2027, 4, 2, 18, 15, 0)
RECORD_SOURCE_VALUE = "rfb_cnpj_webdav"

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

_CONTRACT = tuple(columns_for("estabelecimentos"))
_AUDIT_DDL = (
    "_rescued_data string, _source_file string, _ingested_at timestamp, "
    "_record_source string, _batch_id string, _snapshot_month string, "
    "_snapshot_ref_date date"
)
# DERIVED FROM THE CONTRACT rather than restated: thirty columns spelled a second time
# is thirty chances for the fixture to describe a table bronze does not have, and the
# partition test below is only meaningful if this really is the contract.
_SCHEMA = ", ".join([f"{column} string" for column in _CONTRACT]) + ", " + _AUDIT_DDL
_REJECT_REASON = ", _dq_reject_reason string"

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


def _padded(key: tuple[str, str, str]) -> tuple[str, str, str]:
    """The canonical 8/4/2 spelling of a raw establishment key -- what the hub stores
    and what the digest is taken over."""
    basico, ordem, dv = key
    return (basico.zfill(8), ordem.zfill(4), dv.zfill(2))


def _row(key: tuple[str, str, str], month: str, **overrides) -> tuple:
    """One bronze estabelecimentos row: the whole contract plus every audit column the
    ingest stamps, because this layer reads three of them and must not be handed a
    shape bronze does not have."""
    values = dict(_DEFAULTS)
    values.update(zip(("cnpj_basico", "cnpj_ordem", "cnpj_dv"), key, strict=True))
    values.update(overrides)
    return tuple(values[column] for column in _CONTRACT) + (
        None,
        f"/Volumes/x/cnpj/{month}/estabelecimentos/K3241.K03200Y1.D60613.ESTABELE",
        datetime(2026, 8, 1, 0, 0, 0),
        RECORD_SOURCE_VALUE,
        "batch-1",
        month,
        _REF_DATES[month],
    )


def _bronze_rows() -> list[tuple]:
    """The fixture's bronze rows, meant to be read top to bottom -- the shape IS the
    argument. A function rather than a literal inside `source` only so that fixture
    stays under the fifty-line cap."""
    rows = [
        _row(E_STATUS, JUN),
        _row(E_STATUS, JUL, situacao_cadastral="08",
             data_situacao_cadastral="20260701", motivo_situacao_cadastral="01"),
        _row(E_ADDRESS, JUN),
        _row(E_ADDRESS, JUL, logradouro="DAS ACACIAS", numero="250"),
        _row(E_UNCHANGED, JUN),
        _row(E_UNCHANGED, JUL),
        _row(E_NEW_IN_JULY, JUL),
        _row(E_SHORT_ORDEM, JUN),
        _row(E_SHORT_ORDEM, JUL),
        _row(E_SHORT_TWIN, JUN),
        _row(E_SHORT_TWIN, JUL),
    ]
    # In June's bronze and nowhere in July's. June's quarantine is EMPTY, as measured.
    rows.extend(_row(key, JUN) for key in E_REJECTED)
    return rows


def _quarantine_rows() -> list[tuple]:
    """July's rejects, with the DQ gate's own reason string and the replacement
    character that earned it."""
    return [
        (*_row(key, JUL, nome_fantasia="PADARIA CENTR�L"), "encoding_replacement_char")
        for key in E_REJECTED
    ]


def _write(spark, table: str, schema: str, rows: list[tuple]) -> None:
    (spark.createDataFrame(rows, schema)
     .write.format("delta").mode("append").saveAsTable(table))


@pytest.fixture(scope="module")
def source(spark, tmp_path_factory):
    """A throwaway Delta database holding one bronze estabelecimentos table and its
    quarantine, in the two months real bronze has.

    Delta rather than temp views, and module-scoped, for the reasons
    `tests/vault/test_observation.py::tables` measured: a managed Delta table is a file
    scan with a reusable plan, where a view over `createDataFrame` re-materialises from
    the driver on every query and made every reading test ~3x slower."""
    db = f"estab_vault_{uuid4().hex[:8]}"
    root = tmp_path_factory.mktemp("estab_vault")
    spark.sql(f"CREATE DATABASE {db} LOCATION '{root.as_uri()}'")
    bronze, quarantine = f"{db}.estabelecimentos", f"{db}.estab_q"

    _write(spark, bronze, _SCHEMA, _bronze_rows())
    _write(spark, quarantine, _SCHEMA + _REJECT_REASON, _quarantine_rows())

    grain = ObservationGrain(
        name="hub_estabelecimento", bronze_table=bronze, quarantine_table=quarantine,
        key_columns=HUB.business_key_columns,
    )
    yield SimpleNamespace(db=db, bronze=bronze, quarantine=quarantine, grain=grain)
    spark.sql(f"DROP DATABASE {db} CASCADE")


@pytest.fixture
def target(source):
    """Fresh table names per test, for the tests that WRITE. Sharing one across tests
    would make idempotence pass for the wrong reason."""
    suffix = uuid4().hex[:8]
    return SimpleNamespace(
        hub=f"{source.db}.hub_{suffix}",
        empresa_hub=f"{source.db}.emp_{suffix}",
        dados=f"{source.db}.dados_{suffix}",
        endereco=f"{source.db}.end_{suffix}",
        link=f"{source.db}.link_{suffix}",
    )


def _load_all(spark, source, names, *, load_date=LOADED_AT, months=None):
    """One load of each of the five tables, in dependency order, over `months`.

    `hub_empresa` is loaded FROM ESTABELECIMENTOS here, and that is the real design
    rather than a fixture shortcut: estabelecimentos carries `cnpj_basico`, so it is a
    second feed for that hub and the hub's anti-join makes running both feeds free."""
    names.hub_result = load_hub(
        spark, HUB, source_table=source.bronze, target_table=names.hub,
        load_date=load_date, months=months,
    )
    names.empresa_hub_result = load_hub(
        spark, EMPRESA_HUB, source_table=source.bronze, target_table=names.empresa_hub,
        load_date=load_date, months=months,
    )
    for satellite, table in ((DADOS, "dados"), (ENDERECO, "endereco")):
        setattr(names, f"{table}_result", load_satellite(
            spark, satellite, hub=HUB, source_table=source.bronze,
            target_table=getattr(names, table), load_date=load_date,
            grain=source.grain, months=months,
        ))
    names.link_result = load_link(
        spark, LINK, hubs=LINK_HUBS, source_table=source.bronze,
        target_table=names.link, load_date=load_date, months=months,
    )
    return names


@pytest.fixture(scope="module")
def loaded(spark, source):
    """One load of every table over both months, shared by every read-only assertion.

    Module-scoped because a Delta `saveAsTable` costs seconds on this box and none of
    the tests using it writes. Tests that load again take `target` instead."""
    names = SimpleNamespace(
        hub=f"{source.db}.hub_shared", empresa_hub=f"{source.db}.emp_shared",
        dados=f"{source.db}.dados_shared", endereco=f"{source.db}.end_shared",
        link=f"{source.db}.link_shared",
    )
    return _load_all(spark, source, names)


def _sat_rows(spark, names, table: str) -> dict[tuple[tuple[str, str, str], date], dict]:
    """`{(padded key, applied_date): row}` for one satellite, joined back to the hub so
    assertions read an establishment rather than a digest.

    Keyed on (key, applied_date) so a duplicate row would overwrite its twin; callers
    read per-key row LISTS, which is what makes a missing or extra row visible."""
    joined = (
        spark.read.table(table).alias("s")
        .join(spark.read.table(names.hub).alias("h"), HUB.hash_key)
    )
    return {
        ((row["cnpj_basico"], row["cnpj_ordem"], row["cnpj_dv"]), row[APPLIED_DATE]):
            row.asDict()
        for row in joined.collect()
    }


def _applied(rows: dict, key: tuple[str, str, str]) -> list[date]:
    return sorted(applied for stored, applied in rows if stored == _padded(key))


# --------------------------------------------------------------------------- #
# The acceptance test: the `rejected` half of Task 2's
# --------------------------------------------------------------------------- #

def test_the_four_keys_our_gate_rejected_are_rejected_and_never_departures(
    spark, source, loaded
):
    """THE ACCEPTANCE TEST. Four establishments are in June's bronze, in July's
    quarantine, and in July's bronze nowhere. The ledger must call that
    `rejected_by_our_gate` -- an absence WE caused -- and the two satellites must
    record no change and no departure for them.

    WHY BOTH HALVES ARE ASSERTED TOGETHER. The state alone would pass on a ledger that
    never emits `absent_after_observation` at all; the departure count alone would pass
    on a ledger that emits nothing. Together they pin the mapping in both directions
    ON THIS DATA -- and on this data only, which is the limit the module docstring
    states: estabelecimentos has zero true departures, so a ledger that blames our gate
    for every disappearance passes this test in full. Task 5's socios half is what
    tells the two apart; this is one of its two populations, not a proof on its own."""
    ledger = observation_ledger(spark, source.grain)
    states = {
        ((row["cnpj_basico"], row["cnpj_ordem"], row["cnpj_dv"]), row[MONTH_COLUMN]):
            row[STATE_COLUMN]
        for row in ledger.collect()
    }
    dados, endereco = _sat_rows(spark, loaded, loaded.dados), _sat_rows(
        spark, loaded, loaded.endereco
    )

    for key in E_REJECTED:
        assert states[(key, JUN)] == OBSERVED
        assert states[(key, JUL)] == REJECTED
        assert _applied(dados, key) == [JUN_REF]
        assert _applied(endereco, key) == [JUN_REF]
    assert [state for state in states.values() if state == AFTER] == []
    assert loaded.dados_result.candidate_departures == 0
    assert loaded.endereco_result.candidate_departures == 0


def test_a_key_born_in_july_is_absent_before_its_first_observation_and_not_a_departure(
    spark, source
):
    """The 444,520 in miniature. A key whose first appearance is July is absent in
    June, and calling that a candidate delete would have the ledger assert half a
    million false departures in the first month it covers."""
    ledger = observation_ledger(spark, source.grain)
    states = {
        ((row["cnpj_basico"], row["cnpj_ordem"], row["cnpj_dv"]), row[MONTH_COLUMN]):
            row[STATE_COLUMN]
        for row in ledger.collect()
    }

    assert states[(E_NEW_IN_JULY, JUN)] == BEFORE
    assert states[(E_NEW_IN_JULY, JUL)] == OBSERVED


# --------------------------------------------------------------------------- #
# The fourteen-digit business key
# --------------------------------------------------------------------------- #

def test_the_hub_hash_key_is_the_digest_over_the_padded_triple_in_declaration_order(
    spark, loaded
):
    """The tie between the loader and `opl.vault.hashing`, at the value level, for the
    vault's first MULTI-COLUMN key -- where there are two new ways to be wrong.

    Both are asserted as inequalities rather than left to the positive case: a digest
    over the components REVERSED and a digest over the RAW (unpadded) triple are what a
    loader that ignored declaration order, or that skipped `zero_padded_column`, would
    produce, and each of them satisfies "the hub has a hash key" perfectly."""
    rows = {
        (row["cnpj_basico"], row["cnpj_ordem"], row["cnpj_dv"]): row[HUB.hash_key]
        for row in spark.read.table(loaded.hub).collect()
    }
    key = _padded(E_SHORT_ORDEM)

    assert rows[key] == hash_key(list(key))
    assert rows[key] != hash_key(list(reversed(key)))
    assert rows[key] != hash_key(list(E_SHORT_ORDEM))
    assert rows[_padded(E_UNCHANGED)] == hash_key(list(_padded(E_UNCHANGED)))


def test_two_establishments_whose_raw_components_concatenate_alike_stay_apart(
    spark, loaded
):
    """`('40000004', '1', '23')` and `('40000004', '12', '3')` are the same eleven
    characters if you concatenate the raw columns, and two different establishments.

    WHAT SEPARATES THEM IS THE LENGTH PREFIX, NOT THE PAD, and this test was measured
    rather than assumed: mutating `_padded_components` to drop the zero-pad leaves it
    GREEN, because `S1:1||S2:23` and `S2:12||S1:3` are distinct encodings whatever the
    padding. So this covers a different mutation from the digest test above -- a
    `concat_ws` or an `md5(a || b || c)` in place of the standard, which is the
    "simplification" a multi-column key invites and which merges these two
    establishments onto one hash key with no error attached. The pad's own coverage is
    the digest test; the truncation guard's is below."""
    rows = {
        (row["cnpj_basico"], row["cnpj_ordem"], row["cnpj_dv"]): row[HUB.hash_key]
        for row in spark.read.table(loaded.hub).collect()
    }

    assert set(rows) >= {_padded(E_SHORT_ORDEM), _padded(E_SHORT_TWIN)}
    assert rows[_padded(E_SHORT_ORDEM)] != rows[_padded(E_SHORT_TWIN)]


def test_the_hub_stores_the_padded_key_and_holds_one_row_per_establishment(spark, loaded):
    """Ten establishments, ten rows, ten distinct digests -- and the stored key is the
    CANONICAL one, so the value beside the digest is the value the digest was taken
    over. The distinctness half is what a row count cannot see."""
    rows = spark.read.table(loaded.hub).collect()
    keys = {(row["cnpj_basico"], row["cnpj_ordem"], row["cnpj_dv"]) for row in rows}

    assert len(rows) == 10
    assert len(keys) == 10
    assert len({row[HUB.hash_key] for row in rows}) == 10
    assert _padded(E_SHORT_ORDEM) in keys
    assert E_SHORT_ORDEM not in keys


def test_the_hub_carries_exactly_the_dv2_metadata_and_its_three_key_columns(spark, loaded):
    """Pinned as a list so a column arriving or leaving is a deliberate edit, and in
    the key's declaration order -- the order the digest is taken in."""
    assert spark.read.table(loaded.hub).columns == [
        HUB.hash_key, "cnpj_basico", "cnpj_ordem", "cnpj_dv", LOAD_DATE, RECORD_SOURCE
    ]


# --------------------------------------------------------------------------- #
# The hierarchical link
# --------------------------------------------------------------------------- #

def _link_rows(spark, names) -> dict[tuple[str, str, str], dict]:
    """`{padded establishment key: link row}`, resolved through the establishment hub."""
    estab = {
        row[HUB.hash_key]: (row["cnpj_basico"], row["cnpj_ordem"], row["cnpj_dv"])
        for row in spark.read.table(names.hub).collect()
    }
    return {
        estab[row[HUB.hash_key]]: row.asDict()
        for row in spark.read.table(names.link).collect()
    }


def test_the_link_hash_key_is_the_digest_over_both_hubs_business_keys_in_order(
    spark, loaded
):
    """The link's own key is the standard applied to hub_empresa's business key
    followed by hub_estabelecimento's -- four components, `cnpj_basico` appearing in
    both, which is correct: the link's identity is BOTH keys.

    The two inequalities are the mutations that would leave every join working and the
    table re-keyed: the hubs concatenated the other way round, and the link keyed on
    the establishment alone (dropping the repeated `cnpj_basico`)."""
    rows = _link_rows(spark, loaded)
    basico, ordem, dv = _padded(E_STATUS)
    row = rows[(basico, ordem, dv)]

    assert row[LINK.hash_key] == hash_key([basico, basico, ordem, dv])
    assert row[LINK.hash_key] != hash_key([basico, ordem, dv, basico])
    assert row[LINK.hash_key] != hash_key([basico, ordem, dv])


def test_the_links_hub_references_are_exactly_the_digests_the_two_hubs_hold(spark, loaded):
    """A link whose references are not its hubs' digests joins to nothing, SILENTLY --
    an empty result rather than an error. Set equality both ways, so a reference the
    hub does not have and a hub row no link reaches are both red."""
    link = spark.read.table(loaded.link).collect()
    estab = {row[HUB.hash_key] for row in spark.read.table(loaded.hub).collect()}
    empresa = {
        row[EMPRESA_HUB.hash_key]
        for row in spark.read.table(loaded.empresa_hub).collect()
    }

    assert {row[HUB.hash_key] for row in link} == estab
    assert {row[EMPRESA_HUB.hash_key] for row in link} == empresa
    assert len(link) == 10


def test_two_establishments_of_one_company_are_two_links_onto_one_company_key(
    spark, loaded
):
    """The hierarchy, which is the whole reason this link exists rather than a column
    on the hub. `10000001` has two establishments and `40000004` has two; each pair is
    two link rows sharing one `hub_empresa_hk` and carrying two distinct
    `hub_estabelecimento_hk`."""
    rows = _link_rows(spark, loaded)

    for company, pair in (("10000001", (E_STATUS, E_ADDRESS)),
                          ("40000004", (E_SHORT_ORDEM, E_SHORT_TWIN))):
        linked = [rows[_padded(key)] for key in pair]
        assert {row[EMPRESA_HUB.hash_key] for row in linked} == {hash_key([company])}
        assert len({row[HUB.hash_key] for row in linked}) == 2
        assert len({row[LINK.hash_key] for row in linked}) == 2


def test_the_link_carries_its_two_references_the_dv2_metadata_and_nothing_else(
    spark, loaded
):
    """No payload, no `applied_date`, no end-date column -- a link row asserts that the
    relationship was seen, and when it held is an effectivity satellite's statement to
    make. Pinned as a list so any of those arriving is a deliberate edit, and in the
    link's declared hub order."""
    assert spark.read.table(loaded.link).columns == [
        LINK.hash_key, EMPRESA_HUB.hash_key, HUB.hash_key, LOAD_DATE, RECORD_SOURCE
    ]


def test_reloading_the_link_appends_nothing(spark, source, target):
    """Idempotence, with a DIFFERENT `load_date` on the second run so a row silently
    rewritten rather than skipped shows up as a changed stamp even if the count
    happens to hold."""
    _load_all(spark, source, target)
    first = target.link_result
    second = load_link(
        spark, LINK, hubs=LINK_HUBS, source_table=source.bronze,
        target_table=target.link, load_date=RELOADED_AT,
    )
    rows = spark.read.table(target.link).collect()

    assert (first.appended, second.appended) == (10, 0)
    assert second.already_present == 10
    assert len(rows) == 10
    assert {row[LOAD_DATE] for row in rows} == {LOADED_AT}


def test_loading_july_after_june_adds_only_the_relationship_born_in_july(
    spark, source, target
):
    """The incremental path. June holds nine establishments and July adds the one born
    in it; the four rejected keys are already in the link from June and a link is
    insert-only, so they stay."""
    june = load_link(
        spark, LINK, hubs=LINK_HUBS, source_table=source.bronze,
        target_table=target.link, load_date=LOADED_AT, months=[JUN],
    )
    both = load_link(
        spark, LINK, hubs=LINK_HUBS, source_table=source.bronze,
        target_table=target.link, load_date=RELOADED_AT, months=[JUN, JUL],
    )

    assert (june.appended, both.appended) == (9, 1)
    assert spark.read.table(target.link).count() == 10


def test_a_link_handed_its_hubs_in_the_wrong_order_is_refused(spark, source, target):
    """The refusal that only a link needs. A reordered pair produces two CORRECT
    reference columns and a link hash key computed over the business keys concatenated
    backwards -- every row present, every join working, and the table's identity column
    disagreeing with what the next load computes."""
    with pytest.raises(ValueError, match="CONCATENATED IN ORDER"):
        load_link(
            spark, LINK, hubs=tuple(reversed(LINK_HUBS)), source_table=source.bronze,
            target_table=target.link, load_date=LOADED_AT,
        )

    assert not spark.catalog.tableExists(target.link)


def test_an_overlong_key_component_fails_the_link_load_rather_than_merging_two_pairs(
    spark, source, target
):
    """THE TASK 3 REVIEW'S I2, APPLIED TO THE NEW LOADER BEFORE IT COULD BE FOUND AGAIN.

    That review's finding was that `hub_candidates` pads TWICE -- once for the digest
    and once for the stored key -- so an overlong-key test that only exercised
    `load_hub` stayed green when one call site was deleted, while `satellite_candidates`
    (one call site) was left free to merge two companies onto one digest.
    `link_candidates` is a THIRD single-call-site consumer, through
    `link_hash_key_expression`, so it needs its own.

    Spark's `lpad` truncates: `lpad('00012', 4, '0')` is `'0001'`, which is
    `E_SHORT_ORDEM`'s canonical ordem. Unguarded, a five-character `cnpj_ordem` would
    not produce a bad row -- it would give two different establishments one link hash
    key and one hub reference."""
    bad = f"{source.db}.overlong_{uuid4().hex[:8]}"
    _write(spark, bad, _SCHEMA, [
        _row(("40000004", "00012", "23"), JUN), _row(E_SHORT_ORDEM, JUN)
    ])

    with pytest.raises(Exception, match="refusing to truncate"):
        load_link(
            spark, LINK, hubs=LINK_HUBS, source_table=bad,
            target_table=target.link, load_date=LOADED_AT,
        )


def test_a_link_handed_a_hub_it_does_not_name_is_refused(spark, source, target):
    """The loaders take their hubs as free arguments so they can be tested against
    throwaway specs, which means nothing but this check stops them being mismatched."""
    with pytest.raises(ValueError, match="link_empresa_estabelecimento"):
        load_link(
            spark, LINK, hubs=(EMPRESA_HUB, EMPRESA_HUB), source_table=source.bronze,
            target_table=target.link, load_date=LOADED_AT,
        )


# --------------------------------------------------------------------------- #
# The two satellites, and the column split between them
# --------------------------------------------------------------------------- #

def test_the_two_satellites_and_the_key_partition_the_estabelecimentos_contract():
    """Every one of the contract's thirty columns is a business key, the payload of
    EXACTLY ONE satellite, or a declared omission -- and the three sets are disjoint.

    THIS IS THE TEST THAT STOPS THE TWO SATELLITES DISAGREEING ABOUT WHAT THEY OWN. A
    column in both payloads is recorded twice and its two `hash_diff` histories drift;
    a column in neither is indistinguishable from a column someone forgot, which is why
    the omissions are a declared tuple with a reason beside each group rather than an
    absence. Pure -- no Spark, no fixture -- so it runs on every push in milliseconds
    and a contract column added upstream turns it red here."""
    contract = set(_CONTRACT)
    key = set(HUB.business_key_columns)
    dados, endereco = set(DADOS.payload_columns), set(ENDERECO.payload_columns)
    omitted = set(UNMODELLED_ESTABELECIMENTO_COLUMNS)

    assert dados & endereco == set()
    assert key & (dados | endereco | omitted) == set()
    assert omitted & (dados | endereco) == set()
    assert key | dados | endereco | omitted == contract
    assert len(key) + len(dados) + len(endereco) + len(omitted) == len(_CONTRACT)


def test_a_status_change_writes_a_second_dados_row_and_leaves_the_address_alone(
    spark, loaded
):
    """`E_STATUS` moves `situacao_cadastral`, `data_situacao_cadastral` and
    `motivo_situacao_cadastral` between the snapshots and moves no address column.

    Two rows in `_dados` and ONE in `_endereco` is the split working. The second
    assertion is the one that would fail on a single lumped satellite, or on two
    satellites whose payloads overlapped -- both would write the address again for a
    change that is not the address's."""
    dados, endereco = _sat_rows(spark, loaded, loaded.dados), _sat_rows(
        spark, loaded, loaded.endereco
    )

    assert _applied(dados, E_STATUS) == [JUN_REF, JUL_REF]
    assert _applied(endereco, E_STATUS) == [JUN_REF]
    assert dados[(_padded(E_STATUS), JUN_REF)]["situacao_cadastral"] == "02"
    assert dados[(_padded(E_STATUS), JUL_REF)]["situacao_cadastral"] == "08"


def test_an_address_change_writes_a_second_endereco_row_and_leaves_the_status_alone(
    spark, loaded
):
    """The mirror image: `E_ADDRESS` moves `logradouro` and `numero` and nothing else.
    Both directions are asserted because one alone is satisfied by a satellite that
    records everything."""
    dados, endereco = _sat_rows(spark, loaded, loaded.dados), _sat_rows(
        spark, loaded, loaded.endereco
    )

    assert _applied(endereco, E_ADDRESS) == [JUN_REF, JUL_REF]
    assert _applied(dados, E_ADDRESS) == [JUN_REF]
    assert endereco[(_padded(E_ADDRESS), JUN_REF)]["logradouro"] == "DAS FLORES"
    assert endereco[(_padded(E_ADDRESS), JUL_REF)]["logradouro"] == "DAS ACACIAS"


def test_a_change_in_an_unmodelled_column_produces_no_row_in_either_satellite(
    spark, source, target
):
    """`identificador_matriz_filial`, `correio_eletronico` and `data_inicio_atividade`
    are declared omissions, and an omission has to be one in the DATA and not only in a
    tuple. All three move for one establishment across the two months and neither
    satellite may notice -- which is what stops the obvious over-correction of hashing
    every source column."""
    moved = f"{source.db}.moved_{uuid4().hex[:8]}"
    _write(spark, moved, _SCHEMA, [
        _row(E_UNCHANGED, JUN),
        _row(E_UNCHANGED, JUL, identificador_matriz_filial="2",
             correio_eletronico="novo@x.br", data_inicio_atividade="20260701"),
    ])
    grain = ObservationGrain(
        name="hub_estabelecimento", bronze_table=moved,
        quarantine_table=source.quarantine, key_columns=HUB.business_key_columns,
    )
    results = [
        load_satellite(
            spark, satellite, hub=HUB, source_table=moved,
            target_table=f"{target.dados}_{satellite.name}", load_date=LOADED_AT,
            grain=grain,
        )
        for satellite in (DADOS, ENDERECO)
    ]

    assert [result.appended for result in results] == [1, 1]


def test_each_satellite_writes_exactly_its_own_payload_and_no_end_date(spark, loaded):
    """THE MUTATION-RESISTANT HALF of both claims at once.

    A test that a departed key's row is not end-dated passes trivially where there is
    no end-date column, and a test that one satellite ignores the other's columns
    passes on a satellite that writes them and never compares them. Pinning both whole
    column lists makes each a property of the TABLE rather than of the row a test
    happened to read."""
    assert spark.read.table(loaded.dados).columns == [
        HUB.hash_key, LOAD_DATE, APPLIED_DATE, RECORD_SOURCE, HASH_DIFF,
        *DADOS.payload_columns,
    ]
    assert spark.read.table(loaded.endereco).columns == [
        HUB.hash_key, LOAD_DATE, APPLIED_DATE, RECORD_SOURCE, HASH_DIFF,
        *ENDERECO.payload_columns,
    ]


def test_both_satellites_key_on_the_same_digest_as_the_hub(spark, loaded):
    """Two satellites on one hub is the first time this can be wrong in two places.
    Both derive the key from the same `Hub` spec; this says so at the value level,
    where an empty join would otherwise be a silently empty history."""
    hub_keys = {row[HUB.hash_key] for row in spark.read.table(loaded.hub).collect()}

    for table in (loaded.dados, loaded.endereco):
        keys = {row[HUB.hash_key] for row in spark.read.table(table).collect()}
        assert keys
        assert keys <= hub_keys


def test_a_grain_declaring_the_key_columns_in_another_order_is_refused(
    spark, source, target
):
    """THE FIRST MULTI-COLUMN KEY MAKES THIS REACHABLE, and the decision it forced is
    argued in `opl.vault.satellites._grain_key_mismatch`: the permuted grain would
    ANSWER identically (groupBy does not care about order), and it is refused so that
    the grain's column list and the hub's stay one list rather than two sets.

    Distinguished from the coarser/finer refusal by its own message, because telling
    someone whose columns are right that their grain is "coarser or finer" sends them
    looking for a bug that is not there."""
    permuted = ObservationGrain(
        name="hub_estabelecimento", bronze_table=source.bronze,
        quarantine_table=source.quarantine,
        key_columns=("cnpj_dv", "cnpj_ordem", "cnpj_basico"),
    )

    with pytest.raises(ValueError, match="different order"):
        load_satellite(
            spark, DADOS, hub=HUB, source_table=source.bronze,
            target_table=target.dados, load_date=LOADED_AT, grain=permuted,
        )

    assert not spark.catalog.tableExists(target.dados)
