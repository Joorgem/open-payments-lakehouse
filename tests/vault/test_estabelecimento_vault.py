"""`hub_estabelecimento`, its two satellites and `link_empresa_estabelecimento`, over a
fixture shaped like real bronze.

HALF OF THE EVIDENCE, DELIBERATELY, exactly as `test_cnpj_vault.py` is: CI runs local
Spark with no credential, so this guards the MECHANIC and the measurement against
72.3M real rows lives in the task report. Neither stands in for the other.

WHAT THE FIXTURE MIRRORS -- the measured shape, controller run
`01f191f3-6c96-15d2-84db-514bfcff2ce5`:

| month | observed | rejected_by_our_gate | absent_before_first | absent_after |
|---|---|---|---|---|
| 2026-06 | 71,874,448 | 0 | 444,520 | 0 |
| 2026-07 | 72,318,964 | 4 | 0 | **0** |

Every number that is not a volume is reproduced in `conftest.py`, which now OWNS the
fixture: June's quarantine is EMPTY, July's holds exactly four keys absent from July's
bronze, one key is born in July (the 444,520 in miniature), and nothing is
`absent_after_observation` in either month. The fixture moved there when Task 5 needed a
second vault-table module and this file was at the 800-line cap; see that file for why
sharing the materialisation matters more than keeping it beside its assertions.

THE ACCEPTANCE TEST IS ONE HALF OF TASK 2'S AND MUST NOT BE READ AS MORE. Those four
are the `rejected` half -- our own gate widened between the runs
(`encoding_replacement_char`), so their disappearance is OURS. Estabelecimentos has
ZERO true departures, so this table alone cannot tell a correct ledger from one that
labels every departure `rejected`; it passes on both. The other half is socios in
`test_socios_vault.py` (65,444 departures, none quarantined), and the discrimination
lives in `test_observation.py::test_a_departure_reads_as_our_gate_on_one_table_and_as_
the_sources_on_the_other`. What THIS file adds is that the satellites and the link,
loaded from that data, record no change and no departure for the four.

THE PADDING PAIR IS SYNTHETIC AND SAYS SO. The three key columns are 8/4/2 characters
on every one of the 72.3M real rows in both months with zero non-numeric values, so the
pads are defensive and real data exercises none of them. `E_SHORT_ORDEM` and
`E_SHORT_TWIN` exist because an unpadded key is how this vault would merge two
establishments onto one hash key, and a fixture built strictly to the measurement would
leave that branch untested."""
from __future__ import annotations

from datetime import date
from uuid import uuid4

import pytest
from pyspark.sql import functions as F

from opl.vault.columns import APPLIED_DATE, HASH_DIFF, LOAD_DATE, RECORD_SOURCE
from opl.vault.domains.cnpj import UNMODELLED_ESTABELECIMENTO_COLUMNS
from opl.vault.hashing import hash_key
from opl.vault.links import load_link
from opl.vault.observation import (
    MONTH_COLUMN,
    STATE_COLUMN,
    ObservationGrain,
    ObservationState,
    observation_ledger,
)
from opl.vault.satellites import load_satellite

from .conftest import (
    E_ADDRESS,
    E_NEW_IN_JULY,
    E_REJECTED,
    E_SHORT_ORDEM,
    E_SHORT_TWIN,
    E_STATUS,
    E_UNCHANGED,
    EMPRESA_ESTABELECIMENTO_HUBS,
    EMPRESA_ESTABELECIMENTO_LINK,
    EMPRESA_HUB,
    ESTABELECIMENTO_CONTRACT,
    ESTABELECIMENTO_DADOS,
    ESTABELECIMENTO_ENDERECO,
    ESTABELECIMENTO_HUB,
    JUL,
    JUL_REF,
    JUN,
    JUN_REF,
    LOADED_AT,
    RELOADED_AT,
    bronze_schema,
    derived_table,
    estabelecimento_row,
    load_estabelecimento_vault,
    write_delta,
)

HUB = ESTABELECIMENTO_HUB
DADOS = ESTABELECIMENTO_DADOS
ENDERECO = ESTABELECIMENTO_ENDERECO
LINK = EMPRESA_ESTABELECIMENTO_LINK
LINK_HUBS = EMPRESA_ESTABELECIMENTO_HUBS

OBSERVED = ObservationState.OBSERVED.value
REJECTED = ObservationState.REJECTED_BY_OUR_GATE.value
BEFORE = ObservationState.ABSENT_BEFORE_FIRST_OBSERVATION.value
AFTER = ObservationState.ABSENT_AFTER_OBSERVATION.value

_SCHEMA = bronze_schema("estabelecimentos")


def _padded(key: tuple[str, str, str]) -> tuple[str, str, str]:
    """The canonical 8/4/2 spelling of a raw key: what the hub stores and hashes."""
    basico, ordem, dv = key
    return (basico.zfill(8), ordem.zfill(4), dv.zfill(2))


def _sat_rows(spark, names, table: str) -> dict[tuple[tuple[str, str, str], date], dict]:
    """`{(padded key, applied_date): row}` for one satellite, joined back to the hub so
    assertions read an establishment rather than a digest. A duplicate row would
    overwrite its twin here, so callers read per-key row LISTS instead."""
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


def _states(spark, grain) -> dict[tuple[tuple[str, str, str], str], str]:
    """`{(raw key, month): observation_state}` for the whole ledger.

    Keyed on the RAW triple, not the padded one: the ledger reads bronze's own columns
    and never hashes, because what it answers is "what did we SEE"."""
    return {
        ((row["cnpj_basico"], row["cnpj_ordem"], row["cnpj_dv"]), row[MONTH_COLUMN]):
            row[STATE_COLUMN]
        for row in observation_ledger(spark, grain).collect()
    }


# --------------------------------------------------------------------------- #
# The acceptance test: the `rejected` half of Task 2's
# --------------------------------------------------------------------------- #

def test_the_four_keys_our_gate_rejected_are_rejected_and_never_departures(
    spark, estab_source, estab_loaded
):
    """THE ACCEPTANCE TEST. Four establishments are in June's bronze, in July's
    quarantine, and in July's bronze nowhere. The ledger must call that
    `rejected_by_our_gate` -- an absence WE caused -- and the two satellites must
    record no change and no departure for them.

    WHY BOTH HALVES ARE ASSERTED TOGETHER. The state alone passes on a ledger that
    never emits `absent_after_observation`; the count alone passes on one that emits
    nothing. Together they pin the mapping ON THIS DATA -- and only there, per the
    module docstring: a ledger blaming our gate for every disappearance passes in full,
    and Task 5's socios half is what tells the two apart."""
    states = _states(spark, estab_source.grain)
    dados, endereco = _sat_rows(spark, estab_loaded, estab_loaded.dados), _sat_rows(
        spark, estab_loaded, estab_loaded.endereco
    )

    for key in E_REJECTED:
        assert states[(key, JUN)] == OBSERVED
        assert states[(key, JUL)] == REJECTED
        assert _applied(dados, key) == [JUN_REF]
        assert _applied(endereco, key) == [JUN_REF]
    assert [state for state in states.values() if state == AFTER] == []
    assert estab_loaded.dados_result.candidate_departures == 0
    assert estab_loaded.endereco_result.candidate_departures == 0


def test_a_key_born_in_july_is_absent_before_its_first_observation_and_not_a_departure(
    spark, estab_source
):
    """The 444,520 in miniature. A key whose first appearance is July is absent in
    June, and calling that a candidate delete would have the ledger assert half a
    million false departures in the first month it covers."""
    states = _states(spark, estab_source.grain)

    assert states[(E_NEW_IN_JULY, JUN)] == BEFORE
    assert states[(E_NEW_IN_JULY, JUL)] == OBSERVED


# --------------------------------------------------------------------------- #
# The fourteen-digit business key
# --------------------------------------------------------------------------- #

def test_the_hub_hash_key_is_the_digest_over_the_padded_triple_in_declaration_order(
    spark, estab_loaded
):
    """The tie between the loader and `opl.vault.hashing`, at the value level, for the
    vault's first MULTI-COLUMN key -- where there are two new ways to be wrong.

    Both are asserted as inequalities rather than left to the positive case: a digest
    over the components REVERSED and a digest over the RAW (unpadded) triple are what a
    loader that ignored declaration order, or that skipped `zero_padded_column`, would
    produce, and each of them satisfies "the hub has a hash key" perfectly."""
    rows = {
        (row["cnpj_basico"], row["cnpj_ordem"], row["cnpj_dv"]): row[HUB.hash_key]
        for row in spark.read.table(estab_loaded.hub).collect()
    }
    key = _padded(E_SHORT_ORDEM)

    assert rows[key] == hash_key(list(key))
    assert rows[key] != hash_key(list(reversed(key)))
    assert rows[key] != hash_key(list(E_SHORT_ORDEM))
    assert rows[_padded(E_UNCHANGED)] == hash_key(list(_padded(E_UNCHANGED)))


def test_two_establishments_whose_raw_components_concatenate_alike_stay_apart(
    spark, estab_loaded
):
    """`('40000004', '1', '23')` and `('40000004', '12', '3')` are the same eleven
    characters if you concatenate the raw columns, and two different establishments.

    WHAT SEPARATES THEM IS THE LENGTH PREFIX, NOT THE PAD -- measured, not assumed:
    dropping the zero-pad from `_padded_components` leaves this GREEN, because
    `S8:40000004||S1:1||S2:23` and `S8:40000004||S2:12||S1:3` are distinct encodings
    either way. So it covers a different mutation from the digest test above: a
    `concat_ws` or `md5(a || b || c)` in place of the standard, the "simplification" a
    multi-column key invites, which merges these two onto one hash key with no error
    attached. The pad's own coverage is the digest test; truncation's is below."""
    rows = {
        (row["cnpj_basico"], row["cnpj_ordem"], row["cnpj_dv"]): row[HUB.hash_key]
        for row in spark.read.table(estab_loaded.hub).collect()
    }

    assert set(rows) >= {_padded(E_SHORT_ORDEM), _padded(E_SHORT_TWIN)}
    assert rows[_padded(E_SHORT_ORDEM)] != rows[_padded(E_SHORT_TWIN)]


def test_the_hub_stores_the_padded_key_and_holds_one_row_per_establishment(spark, estab_loaded):
    """Ten establishments, ten rows, ten distinct digests -- and the stored key is the
    CANONICAL one, so the value beside the digest is the value the digest was taken
    over. The distinctness half is what a row count cannot see."""
    rows = spark.read.table(estab_loaded.hub).collect()
    keys = {(row["cnpj_basico"], row["cnpj_ordem"], row["cnpj_dv"]) for row in rows}

    assert len(rows) == 10
    assert len(keys) == 10
    assert len({row[HUB.hash_key] for row in rows}) == 10
    assert _padded(E_SHORT_ORDEM) in keys
    assert E_SHORT_ORDEM not in keys


def test_the_hub_carries_exactly_the_dv2_metadata_and_its_three_key_columns(spark, estab_loaded):
    """Pinned as a list so a column arriving or leaving is a deliberate edit, and in
    the key's declaration order -- the order the digest is taken in."""
    assert spark.read.table(estab_loaded.hub).columns == [
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
    spark, estab_loaded
):
    """The link's own key is the standard over hub_empresa's business key followed by
    hub_estabelecimento's -- four components, `cnpj_basico` in both, which is correct:
    the link's identity is BOTH keys. The two inequalities are the mutations that leave
    every join working and the table re-keyed -- the hubs concatenated the other way
    round, and the link keyed on the establishment alone."""
    rows = _link_rows(spark, estab_loaded)
    basico, ordem, dv = _padded(E_STATUS)
    row = rows[(basico, ordem, dv)]

    assert row[LINK.hash_key] == hash_key([basico, basico, ordem, dv])
    assert row[LINK.hash_key] != hash_key([basico, ordem, dv, basico])
    assert row[LINK.hash_key] != hash_key([basico, ordem, dv])


def test_the_links_hub_references_are_exactly_the_digests_the_two_hubs_hold(spark, estab_loaded):
    """A link whose references are not its hubs' digests joins to nothing, SILENTLY --
    an empty result rather than an error. Set equality both ways, so a reference the
    hub does not have and a hub row no link reaches are both red."""
    link = spark.read.table(estab_loaded.link).collect()
    estab = {row[HUB.hash_key] for row in spark.read.table(estab_loaded.hub).collect()}
    empresa = {
        row[EMPRESA_HUB.hash_key]
        for row in spark.read.table(estab_loaded.empresa_hub).collect()
    }

    assert {row[HUB.hash_key] for row in link} == estab
    assert {row[EMPRESA_HUB.hash_key] for row in link} == empresa
    assert len(link) == 10


def test_two_establishments_of_one_company_are_two_links_onto_one_company_key(
    spark, estab_loaded
):
    """The hierarchy, which is why this is a link and not a column on the hub. Each of
    `10000001` and `40000004` has two establishments: two link rows sharing one
    `hub_empresa_hk` and carrying two distinct `hub_estabelecimento_hk`."""
    rows = _link_rows(spark, estab_loaded)

    for company, pair in (("10000001", (E_STATUS, E_ADDRESS)),
                          ("40000004", (E_SHORT_ORDEM, E_SHORT_TWIN))):
        linked = [rows[_padded(key)] for key in pair]
        assert {row[EMPRESA_HUB.hash_key] for row in linked} == {hash_key([company])}
        assert len({row[HUB.hash_key] for row in linked}) == 2
        assert len({row[LINK.hash_key] for row in linked}) == 2


def test_the_link_carries_its_two_references_the_dv2_metadata_and_nothing_else(
    spark, estab_loaded
):
    """No payload, no `applied_date`, no end-date -- a link row asserts the relationship
    was seen, and when it HELD is an effectivity satellite's statement. Pinned as a list,
    in the link's declared hub order, so any arrival is a deliberate edit."""
    assert spark.read.table(estab_loaded.link).columns == [
        LINK.hash_key, EMPRESA_HUB.hash_key, HUB.hash_key, LOAD_DATE, RECORD_SOURCE
    ]


_TABLES = ("hub", "empresa_hub", "dados", "endereco", "link")

# What one load over both months writes to each of the five. Pinned so the re-run test
# asserts the FIRST pass too: "appended nothing" is satisfied by a loader that appended
# nothing the first time either.
_FIRST_PASS = {"hub": 10, "empresa_hub": 8, "dados": 11, "endereco": 11, "link": 10}


def test_reloading_the_hub_the_satellites_and_the_link_appends_nothing(
    spark, estab_source, estab_target
):
    """Idempotence across ALL FIVE tables, with a different `load_date` on the second
    run so a row silently rewritten rather than skipped shows up as a changed stamp even
    where the count happens to hold.

    NOT ONLY THE LINK. Each loader reaches idempotence by its own route -- the hubs and
    the link by an anti-join on a hash key, the satellites by dropping candidates
    already persisted BEFORE the change window -- and two satellites on one hub is a
    configuration this vault did not have before."""
    first = dict(_FIRST_PASS)
    load_estabelecimento_vault(spark, estab_source, estab_target)
    appended = {name: getattr(estab_target, f"{name}_result").appended for name in _TABLES}
    load_estabelecimento_vault(spark, estab_source, estab_target, load_date=RELOADED_AT)
    again = {name: getattr(estab_target, f"{name}_result").appended for name in _TABLES}

    assert appended == first
    assert again == dict.fromkeys(_TABLES, 0)
    for name in _TABLES:
        rows = spark.read.table(getattr(estab_target, name)).collect()
        assert len(rows) == first[name]
        assert {row[LOAD_DATE] for row in rows} == {LOADED_AT}


def test_loading_july_after_june_adds_only_the_relationship_born_in_july(
    spark, estab_source, estab_target
):
    """The incremental path. June holds nine establishments and July adds the one born
    in it; the four rejected keys are already in the link from June and a link is
    insert-only, so they stay."""
    june = load_link(
        spark, LINK, hubs=LINK_HUBS, source_table=estab_source.bronze,
        target_table=estab_target.link, load_date=LOADED_AT, months=[JUN],
    )
    both = load_link(
        spark, LINK, hubs=LINK_HUBS, source_table=estab_source.bronze,
        target_table=estab_target.link, load_date=RELOADED_AT, months=[JUN, JUL],
    )

    assert (june.appended, both.appended) == (9, 1)
    assert spark.read.table(estab_target.link).count() == 10


def test_a_link_handed_its_hubs_in_the_wrong_order_is_refused(spark, estab_source, estab_target):
    """The refusal that only a link needs. A reordered pair produces two CORRECT
    reference columns and a link hash key computed over the business keys concatenated
    backwards -- every row present, every join working, and the table's identity column
    disagreeing with what the next load computes."""
    with pytest.raises(ValueError, match="CONCATENATED IN ORDER"):
        load_link(
            spark, LINK, hubs=tuple(reversed(LINK_HUBS)), source_table=estab_source.bronze,
            target_table=estab_target.link, load_date=LOADED_AT,
        )

    assert not spark.catalog.tableExists(estab_target.link)


def test_an_overlong_key_component_fails_the_link_load_rather_than_merging_two_pairs(
    spark, estab_source, estab_target
):
    """THE TASK 3 REVIEW'S I2, APPLIED TO THE NEW LOADER BEFORE IT COULD BE FOUND AGAIN.

    That finding was that `hub_candidates` pads TWICE, so an overlong-key test through
    `load_hub` alone stayed green when one call site was deleted while
    `satellite_candidates` (one site) was left free to merge two companies onto one
    digest. `link_candidates` is a THIRD single-site consumer, so it needs its own.

    Spark's `lpad` truncates: `lpad('00012', 4, '0')` is `'0001'`, `E_SHORT_ORDEM`'s
    canonical ordem. Unguarded, a five-character `cnpj_ordem` gives two different
    establishments one link hash key and one hub reference."""
    bad = f"{estab_source.db}.overlong_{uuid4().hex[:8]}"
    write_delta(spark, bad, _SCHEMA, [
        estabelecimento_row(("40000004", "00012", "23"), JUN),
        estabelecimento_row(E_SHORT_ORDEM, JUN),
    ])

    with pytest.raises(Exception, match="refusing to truncate"):
        load_link(
            spark, LINK, hubs=LINK_HUBS, source_table=bad,
            target_table=estab_target.link, load_date=LOADED_AT,
        )


def test_a_source_missing_one_hubs_key_column_is_refused_by_name(spark, estab_source, estab_target):
    """`link_candidates` requires EVERY participating hub's business key to be a column
    of the one source. The review found that guard untested while `links.py`'s docstring
    claimed it ("an error naming the missing column rather than a NULL reference").

    `cnpj_ordem` belongs to hub_estabelecimento alone, so this is red both if the check
    is DELETED (Spark then fails in the `select`, naming neither link nor hub) and if it
    is NARROWED to the first hub's columns -- the shape a two-hub loader invites.
    **Task 5 copies this loader**, so the gap is closed here rather than noted."""
    without = derived_table(
        spark, estab_source.db, "without", spark.read.table(estab_source.bronze).drop("cnpj_ordem")
    )

    with pytest.raises(ValueError, match="cnpj_ordem"):
        load_link(
            spark, LINK, hubs=LINK_HUBS, source_table=without,
            target_table=estab_target.link, load_date=LOADED_AT,
        )

    assert not spark.catalog.tableExists(estab_target.link)


def test_a_typed_key_column_is_refused_by_the_link_loader_before_it_hashes(
    spark, estab_source, estab_target
):
    """The sibling of `test_cnpj_vault.py::test_a_source_column_that_is_not_a_string_is_
    refused_by_name`, for the third consumer of the hash standard.

    Spark casts a typed column silently and hashes the CAST, where `hash_key` raises on
    a value with no `.strip()`: one spelling answers and the other refuses, which no
    equivalence test over string corpora can see. `cnpj_dv` is again the second hub's
    alone, so narrowing the check to the first hub's key leaves this red."""
    typed = derived_table(
        spark, estab_source.db, "typed",
        spark.read.table(estab_source.bronze).withColumn(
            "cnpj_dv", F.col("cnpj_dv").cast("int")
        ),
    )

    with pytest.raises(TypeError, match="cnpj_dv"):
        load_link(
            spark, LINK, hubs=LINK_HUBS, source_table=typed,
            target_table=estab_target.link, load_date=LOADED_AT,
        )

    assert not spark.catalog.tableExists(estab_target.link)


def test_a_link_handed_a_hub_it_does_not_name_is_refused(spark, estab_source, estab_target):
    """The loaders take their hubs as free arguments so they can be tested against
    throwaway specs, which means nothing but this check stops them being mismatched.

    Matched on "was handed" rather than on the link's name: every refusal this loader
    raises names the link, so the name alone would also match a message about something
    else entirely."""
    with pytest.raises(ValueError, match="was handed"):
        load_link(
            spark, LINK, hubs=(EMPRESA_HUB, EMPRESA_HUB), source_table=estab_source.bronze,
            target_table=estab_target.link, load_date=LOADED_AT,
        )


# --------------------------------------------------------------------------- #
# The two satellites, and the column split between them
# --------------------------------------------------------------------------- #

def test_the_two_satellites_and_the_key_partition_the_estabelecimentos_contract():
    """Every one of the contract's thirty columns is a business key, the payload of
    EXACTLY ONE satellite, or a declared omission -- and the three sets are disjoint.

    THIS IS THE TEST THAT STOPS THE TWO SATELLITES DISAGREEING ABOUT WHAT THEY OWN. A
    column in both payloads has two `hash_diff` histories that drift; a column in
    neither is indistinguishable from one someone forgot, which is why the omissions
    are a declared tuple. Pure -- no Spark -- so a contract column added upstream turns
    it red in milliseconds."""
    contract = set(ESTABELECIMENTO_CONTRACT)
    key = set(HUB.business_key_columns)
    dados, endereco = set(DADOS.payload_columns), set(ENDERECO.payload_columns)
    omitted = set(UNMODELLED_ESTABELECIMENTO_COLUMNS)

    assert dados & endereco == set()
    assert key & (dados | endereco | omitted) == set()
    assert omitted & (dados | endereco) == set()
    assert key | dados | endereco | omitted == contract
    assert len(key) + len(dados) + len(endereco) + len(omitted) == len(ESTABELECIMENTO_CONTRACT)


def test_a_status_change_writes_a_second_dados_row_and_leaves_the_address_alone(
    spark, estab_loaded
):
    """`E_STATUS` moves `situacao_cadastral`, `data_situacao_cadastral` and
    `motivo_situacao_cadastral` between the snapshots and moves no address column.

    Two rows in `_dados` and ONE in `_endereco` is the split working. The second
    assertion is the one that would fail on a single lumped satellite, or on two
    satellites whose payloads overlapped -- both would write the address again for a
    change that is not the address's."""
    dados, endereco = _sat_rows(spark, estab_loaded, estab_loaded.dados), _sat_rows(
        spark, estab_loaded, estab_loaded.endereco
    )

    assert _applied(dados, E_STATUS) == [JUN_REF, JUL_REF]
    assert _applied(endereco, E_STATUS) == [JUN_REF]
    assert dados[(_padded(E_STATUS), JUN_REF)]["situacao_cadastral"] == "02"
    assert dados[(_padded(E_STATUS), JUL_REF)]["situacao_cadastral"] == "08"


def test_an_address_change_writes_a_second_endereco_row_and_leaves_the_status_alone(
    spark, estab_loaded
):
    """The mirror image: `E_ADDRESS` moves `logradouro` and `numero` and nothing else.
    Both directions, because one alone passes on a satellite that records everything."""
    dados, endereco = _sat_rows(spark, estab_loaded, estab_loaded.dados), _sat_rows(
        spark, estab_loaded, estab_loaded.endereco
    )

    assert _applied(endereco, E_ADDRESS) == [JUN_REF, JUL_REF]
    assert _applied(dados, E_ADDRESS) == [JUN_REF]
    assert endereco[(_padded(E_ADDRESS), JUN_REF)]["logradouro"] == "DAS FLORES"
    assert endereco[(_padded(E_ADDRESS), JUL_REF)]["logradouro"] == "DAS ACACIAS"


def test_a_change_in_an_unmodelled_column_produces_no_row_in_either_satellite(
    spark, estab_source, estab_target
):
    """`identificador_matriz_filial`, `correio_eletronico` and `data_inicio_atividade`
    are declared omissions, and an omission has to be one in the DATA and not only in a
    tuple. All three move for one establishment across the two months and neither
    satellite may notice -- which is what stops the obvious over-correction of hashing
    every source column."""
    moved = f"{estab_source.db}.moved_{uuid4().hex[:8]}"
    write_delta(spark, moved, _SCHEMA, [
        estabelecimento_row(E_UNCHANGED, JUN),
        estabelecimento_row(E_UNCHANGED, JUL, identificador_matriz_filial="2",
             correio_eletronico="novo@x.br", data_inicio_atividade="20260701"),
    ])
    grain = ObservationGrain(
        name="hub_estabelecimento", bronze_table=moved,
        quarantine_table=estab_source.quarantine, key_columns=HUB.business_key_columns,
    )
    results = [
        load_satellite(
            spark, satellite, hub=HUB, source_table=moved,
            target_table=f"{estab_target.dados}_{satellite.name}", load_date=LOADED_AT,
            grain=grain,
        )
        for satellite in (DADOS, ENDERECO)
    ]

    assert [result.appended for result in results] == [1, 1]


def test_each_satellite_writes_exactly_its_own_payload_and_no_end_date(spark, estab_loaded):
    """THE MUTATION-RESISTANT HALF of both claims at once.

    A test that a departed key's row is not end-dated passes trivially where there is
    no end-date column, and a test that one satellite ignores the other's columns
    passes on a satellite that writes them and never compares them. Pinning both whole
    column lists makes each a property of the TABLE rather than of the row a test
    happened to read."""
    assert spark.read.table(estab_loaded.dados).columns == [
        HUB.hash_key, LOAD_DATE, APPLIED_DATE, RECORD_SOURCE, HASH_DIFF,
        *DADOS.payload_columns,
    ]
    assert spark.read.table(estab_loaded.endereco).columns == [
        HUB.hash_key, LOAD_DATE, APPLIED_DATE, RECORD_SOURCE, HASH_DIFF,
        *ENDERECO.payload_columns,
    ]


def test_both_satellites_key_on_the_same_digest_as_the_hub(spark, estab_loaded):
    """Two satellites on one hub is the first time this can be wrong in two places. Both
    derive the key from one `Hub` spec; an empty join is a silently empty history."""
    hub_keys = {row[HUB.hash_key] for row in spark.read.table(estab_loaded.hub).collect()}

    for table in (estab_loaded.dados, estab_loaded.endereco):
        keys = {row[HUB.hash_key] for row in spark.read.table(table).collect()}
        assert keys
        assert keys <= hub_keys


def test_a_grain_declaring_the_key_columns_in_another_order_is_refused(
    spark, estab_source, estab_target
):
    """THE FIRST MULTI-COLUMN KEY MAKES THIS REACHABLE, and the decision it forced is
    argued in `opl.vault.satellites._grain_key_mismatch`: the permuted grain would
    ANSWER identically, and is refused so the grain's column list and the hub's stay one
    list rather than two sets. It has its OWN message, because telling someone whose
    columns are right that their grain is "coarser or finer" sends them after a bug
    that is not there."""
    permuted = ObservationGrain(
        name="hub_estabelecimento", bronze_table=estab_source.bronze,
        quarantine_table=estab_source.quarantine,
        key_columns=("cnpj_dv", "cnpj_ordem", "cnpj_basico"),
    )

    with pytest.raises(ValueError, match="different order"):
        load_satellite(
            spark, DADOS, hub=HUB, source_table=estab_source.bronze,
            target_table=estab_target.dados, load_date=LOADED_AT, grain=permuted,
        )

    assert not spark.catalog.tableExists(estab_target.dados)
