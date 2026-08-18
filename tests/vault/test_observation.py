"""The observation ledger, tested against fixtures that reproduce the two real
shapes bronze actually has -- because the property this module exists to prove is
one that NO SINGLE TABLE can demonstrate.

WHY THE ACCEPTANCE TEST SPANS TWO TABLES, stated precisely. An earlier version of this
reasoning said "no single table carries both absence states", which is FALSE and was
corrected by measurement -- but the correction OVERSHOT and is itself corrected here
(Task 7's pass). What the measurement shows is that socios at link grain carries
`rejected_by_our_gate` AND an absence state IN THE SAME MONTH, and both absence states
across the two months: 2026-06 is 27,832,321 observed / 5 siblings / 1,792 rejected /
219,370 absent_BEFORE / 0 after, and 2026-07 is 27,986,258 / 5 / 1,781 / 0 before /
65,444 absent_AFTER. FOUR of five in either month -- no month of any table can carry
five, since a key is either before its first observation or after its last, never both.
The true statement is about the CAUSE OF A DEPARTURE, what a satellite actually reads:

  - estabelecimentos 2026-06 -> 2026-07 has exactly **4** departures (keys in June's
    bronze, gone from July's) and **all 4** are our own DQ gate's doing -- the
    `encoding_replacement_char` rejects. Zero departures on that table are the
    source's.
  - socios at link grain has **65,444** departures and **not one** of them is in
    July's quarantine. Zero departures on that table are ours.

So a ledger that blames every departure on our gate passes an
estabelecimentos-only test in full, and a ledger that blames every departure on the
source passes a socios-only test in full. Only a probe carrying both populations
tells a correct ledger from either degenerate one, and
`test_a_departure_reads_as_our_gate_on_one_table_and_as_the_sources_on_the_other`
is that probe. Both degenerate implementations were written and run against this
file; the failures are recorded in the task report.

THE FIXTURES ARE SMALL AND SHAPED, NOT SAMPLED. Each reproduces the state mix its
real table has, at the smallest size that carries it -- the point is the shape, not
the volume:

| fixture | mirrors | the shape it carries |
|---|---|---|
| `estab` | estabelecimentos | every departure is a quarantine, so `absent_after` is empty |
| `socios` | socios | no departure is a quarantine, so `rejected_by_our_gate` is empty |
| `empresas` | empresas | no departures at all; one key rejected every month, never observed |
| `returning` | nothing, a property fixture | a key that leaves and comes back, over three months |

MODULE-SCOPED, deliberately. Every test here READS; none writes. Rebuilding the
fixtures per test would multiply the suite's slowest operation by the number of
assertions for no isolation gain, and a shared fixture that some test mutated would
be a defect in that test rather than in the fixture. All four are real Delta tables;
the `tables` fixture records what a temp-view alternative was measured to cost, and
why it lost.

`ObservationGrain`'s OWN CONSTRUCTION REFUSALS ARE `test_observation_grain.py`, split
out by F-DB Task 1 at exactly 800 lines, BEFORE Task 2's snapshot axis (T7), which has
since landed -- this said "still to come" and ADR 0010 names why that matters: a claim
that something has not happened yet is the first thing a reader trusts and the last
thing anyone revisits. The seam was already here and undrawn: those were the only
CONSTRUCTION-TIME refusals in this module, because a grain that refuses its
arguments never reaches a table. What is left here is the LEDGER -- its derivation over
the four fixtures, and its own argument guards, three of which run against real data and
one of which is worth an eager Spark job. Read together they say "this grain, over these
tables"; alone neither is a claim about the ledger, which is why each file's docstring
points at the other."""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from uuid import uuid4

import pytest

from opl.vault.observation import (
    FIRST_OBSERVED_COLUMN,
    IN_BRONZE_COLUMN,
    IN_QUARANTINE_COLUMN,
    MONTH_COLUMN,
    STATE_COLUMN,
    ObservationGrain,
    ObservationState,
    observation_ledger,
)

# The state values as they land in the data. Spelled through the enum here so a
# rename of a member is a compile-time-ish break in every test, and pinned as
# literals ONCE, in `test_the_five_state_values_are_pinned`, so a rename of a
# VALUE -- which is what a downstream satellite filters on -- cannot pass quietly.
OBSERVED = ObservationState.OBSERVED.value
SIBLINGS = ObservationState.OBSERVED_WITH_REJECTED_SIBLINGS.value
REJECTED = ObservationState.REJECTED_BY_OUR_GATE.value
BEFORE = ObservationState.ABSENT_BEFORE_FIRST_OBSERVATION.value
AFTER = ObservationState.ABSENT_AFTER_OBSERVATION.value

JUN, JUL, AUG = "2026-06", "2026-07", "2026-08"
# The RFB's own filename tokens for the two snapshots, carried as DATE exactly as
# bronze carries them. The ledger must ignore this column; it is here so that
# "ignores it" is exercised rather than assumed.
JUN_REF, JUL_REF, AUG_REF = date(2026, 6, 13), date(2026, 7, 11), date(2026, 8, 8)

_ESTAB = (
    "cnpj_basico string, cnpj_ordem string, cnpj_dv string, correio_eletronico string, "
    "_snapshot_month string, _snapshot_ref_date date"
)
_SOCIOS = (
    "cnpj_basico string, identificador_socio string, cpf_cnpj_socio string, "
    "nome_socio_razao_social string, _snapshot_month string, _snapshot_ref_date date"
)
_EMPRESAS = (
    "cnpj_basico string, razao_social string, _snapshot_month string, _snapshot_ref_date date"
)
_RETURNING = "cnpj_basico string, _snapshot_month string, _snapshot_ref_date date"
_REJECT_REASON = ", _dq_reject_reason string"

# Establishment keys, as (cnpj_basico, cnpj_ordem, cnpj_dv).
E_STAYS = ("11111111", "0001", "55")
E_ALSO_STAYS = ("22222222", "0001", "66")
E_QUARANTINED = ("33333333", "0001", "77")  # in June's bronze, in July's quarantine
E_NEW = ("44444444", "0001", "88")  # first seen in July -- NOT a June departure

# Sócios. One partner (`P_MANY`) holds two partnerships, which is what makes the hub
# grain coarser than the link grain and is the whole reason both exist.
C_KEPT, C_SIBLING, C_LEFT, C_FOREIGN, C_NEW, C_OTHER = (
    "10000001", "10000002", "10000003", "10000004", "10000005", "10000007",
)
P_MANY, P_SPLIT, P_NEW = "***111111**", "***222222**", "***333333**"

# The three-month property fixture's keys.
K_RETURNS, K_STAYS, K_REJECTED_FIRST = "30000001", "30000002", "30000003"


def _link(cnpj: str, ident: str, socio: str | None) -> tuple[str, str, str | None]:
    return (cnpj, ident, socio)


@pytest.fixture(scope="module")
def tables(spark, tmp_path_factory):
    """A throwaway Delta database holding the four fixture shapes.

    DELTA FOR ALL FOUR, AND THAT WAS MEASURED RATHER THAN ASSUMED -- twice, in both
    directions. A Delta `saveAsTable` costs ~22 s on this box against ~0 for a temp
    view, so the EIGHT of them here put ~137 s of setup in front of tests that only ever
    read (25 of them when this was measured at Task 2, 22 now that the grain refusals
    live in `test_observation_grain.py`; the counts read "ten" and
    "25" until Task 7's pass, and every TIMING below is a Task-2 measurement rather than
    a current one). Converting the two property fixtures (`empresas`, `returning`) to temp
    views did cut setup to 92 s exactly as predicted. It also made every test that
    reads them roughly THREE TIMES slower -- `empresas` 9.2 s -> 26.0 s, `returning`
    8.9 s -> 27.4 s -- because a view over `createDataFrame` is a local relation that
    re-materialises from the driver on every query, while a managed Delta table is a
    file scan with a reusable plan. Net effect on the module: 238 s -> 273 s. The
    conversion was reverted on that number.

    So Delta here is not (only) the realism argument that the acceptance test should
    stand on the read path production uses. On this box it is also the faster
    fixture, and the intuitive optimisation is the slower one."""
    db = f"observation_{uuid4().hex[:8]}"
    root = tmp_path_factory.mktemp("observation")
    spark.sql(f"CREATE DATABASE {db} LOCATION '{root.as_uri()}'")

    names = SimpleNamespace(
        estab=f"{db}.estab", estab_q=f"{db}.estab_q",
        socios=f"{db}.socios", socios_q=f"{db}.socios_q",
        empresas=f"{db}.empresas", empresas_q=f"{db}.empresas_q",
        returning=f"{db}.returning", returning_q=f"{db}.returning_q",
    )
    _build_estab(spark, names)
    _build_socios(spark, names)
    _build_empresas(spark, names)
    _build_returning(spark, names)

    names.estab_grain = ObservationGrain(
        name="hub_estabelecimento", bronze_table=names.estab, quarantine_table=names.estab_q,
        key_columns=("cnpj_basico", "cnpj_ordem", "cnpj_dv"),
    )
    # ONE TABLE, TWO GRAINS -- the parameterisation this module claims. Nothing in
    # `observation_ledger` branches on which of these it was handed.
    names.link_grain = ObservationGrain(
        name="link_company_partner", bronze_table=names.socios, quarantine_table=names.socios_q,
        key_columns=("cnpj_basico", "identificador_socio", "cpf_cnpj_socio"),
    )
    names.hub_grain = ObservationGrain(
        name="socio_key", bronze_table=names.socios, quarantine_table=names.socios_q,
        key_columns=("cpf_cnpj_socio",),
    )
    names.empresas_grain = ObservationGrain(
        name="hub_empresa", bronze_table=names.empresas, quarantine_table=names.empresas_q,
        key_columns=("cnpj_basico",),
    )
    names.returning_grain = ObservationGrain(
        name="returning", bronze_table=names.returning, quarantine_table=names.returning_q,
        key_columns=("cnpj_basico",),
    )
    yield names
    spark.sql(f"DROP DATABASE {db} CASCADE")


def _write(spark, table: str, schema: str, rows: list[tuple]) -> None:
    (spark.createDataFrame(rows, schema)
     .write.format("delta").mode("append").saveAsTable(table))


def _build_estab(spark, names) -> None:
    """The measured estabelecimentos shape: 2026-06 quarantine EMPTY, 2026-07
    quarantine holding exactly the departures, plus a key born in July."""
    _write(spark, names.estab, _ESTAB, [
        (*E_STAYS, "a@x.br", JUN, JUN_REF),
        (*E_ALSO_STAYS, "b@x.br", JUN, JUN_REF),
        (*E_QUARANTINED, "c@x.br", JUN, JUN_REF),
        (*E_STAYS, "a@x.br", JUL, JUL_REF),
        (*E_ALSO_STAYS, "b@x.br", JUL, JUL_REF),
        (*E_NEW, "d@x.br", JUL, JUL_REF),
    ])
    _write(spark, names.estab_q, _ESTAB + _REJECT_REASON, [
        (*E_QUARANTINED, "c�@x.br", JUL, JUL_REF, "encoding_replacement_char"),
    ])


def _build_socios(spark, names) -> None:
    """The measured sócios shape. Three facts it reproduces, all real:

    - `C_LEFT`'s partnership with `P_SPLIT` departs and is NOT quarantined -- one of
      the 65,444.
    - `C_SIBLING`'s partnership with `P_MANY` appears TWICE in July's source, once
      clean and once with an empty name; the link business key
      `(cnpj_basico, identificador_socio, cpf_cnpj_socio)` is therefore in bronze AND
      in quarantine in the same month. That is the measured 5-of-1,786 overlap at
      link grain, and it exists because the link BK is not unique in the source
      (27,990,592 rows over 27,986,263 distinct triples).
    - `C_FOREIGN` is a foreign partner: `cpf_cnpj_socio` is NULL, as it is for all
      12,824 of them. A NULL business key must still match itself month to month.

    NOTHING here is in quarantine and absent from bronze in the same month, and that
    is deliberate: it is what makes this fixture faithful to "no departure on socios
    is ours", and therefore what makes the degenerate-implementation probe honest."""
    _write(spark, names.socios, _SOCIOS, [
        (C_KEPT, "2", P_MANY, "ALICE", JUN, JUN_REF),
        (C_SIBLING, "2", P_MANY, "ALICE", JUN, JUN_REF),
        (C_LEFT, "2", P_SPLIT, "BOB", JUN, JUN_REF),
        (C_OTHER, "2", P_SPLIT, "BOB", JUN, JUN_REF),
        (C_FOREIGN, "3", None, "FOREIGN HOLDING", JUN, JUN_REF),
        (C_KEPT, "2", P_MANY, "ALICE", JUL, JUL_REF),
        (C_SIBLING, "2", P_MANY, "ALICE", JUL, JUL_REF),
        (C_OTHER, "2", P_SPLIT, "BOB", JUL, JUL_REF),
        (C_FOREIGN, "3", None, "FOREIGN HOLDING", JUL, JUL_REF),
        (C_NEW, "2", P_NEW, "CARLA", JUL, JUL_REF),
    ])
    _write(spark, names.socios_q, _SOCIOS + _REJECT_REASON, [
        (C_SIBLING, "2", P_MANY, "", JUL, JUL_REF, "null_or_empty_nome_socio_razao_social"),
    ])


def _build_empresas(spark, names) -> None:
    """The measured empresas shape: zero departures of either kind -- the RFB retains
    baixadas rather than removing them -- and one company rejected in BOTH months,
    the same one each time, which never reaches bronze at all."""
    _write(spark, names.empresas, _EMPRESAS, [
        ("20000001", "ACME LTDA", JUN, JUN_REF),
        ("20000002", "BETA SA", JUN, JUN_REF),
        ("20000001", "ACME LTDA", JUL, JUL_REF),
        ("20000002", "BETA SA", JUL, JUL_REF),
    ])
    _write(spark, names.empresas_q, _EMPRESAS + _REJECT_REASON, [
        ("20000009", "", JUN, JUN_REF, "null_or_empty_razao_social"),
        ("20000009", "", JUL, JUL_REF, "null_or_empty_razao_social"),
    ])


def _build_returning(spark, names) -> None:
    """Three months, and the two things only three months can show.

    `K_RETURNS` leaves in July and comes back in August -- the case that makes
    "before first observation / after LAST observation" unstable and this module's
    past-only split necessary.

    `K_REJECTED_FIRST` is quarantined in June, absent in July, and in bronze in
    August. It is the only fixture key whose answer depends on
    `first_observed_month` being a `min` over bronze UNION quarantine rather than
    over bronze alone, and it exists because that decision was previously credited to
    `_state_column`'s branch order, which cannot provide it."""
    _write(spark, names.returning, _RETURNING, [
        (K_RETURNS, JUN, JUN_REF),
        (K_STAYS, JUN, JUN_REF),
        (K_STAYS, JUL, JUL_REF),
        (K_RETURNS, AUG, AUG_REF),
        (K_STAYS, AUG, AUG_REF),
        (K_REJECTED_FIRST, AUG, AUG_REF),
    ])
    _write(spark, names.returning_q, _RETURNING + _REJECT_REASON, [
        (K_REJECTED_FIRST, JUN, JUN_REF, "null_or_empty_razao_social"),
    ])


# MEMOISED, and only because every table here is written once by a module-scoped
# fixture and read by everything after it. One `observation_ledger(...).collect()` is
# two shuffles and a cross join through local Spark on Windows -- seconds, every time
# -- and without this the same four ledgers are recomputed a dozen times over,
# minutes of suite runtime buying no assertion. Keyed by grain name and window; the
# database name carries a uuid, so no key survives a run.
_LEDGERS: dict[tuple, dict] = {}


def _ledger(spark, grain, months=None) -> dict:
    """`{(business key, month): state}` for the whole ledger.

    A dict of the WHOLE result, not a lookup of the rows a test cares about: an
    equality assertion against a dict fails on a missing row, an extra row and a
    wrong state alike, so a test cannot pass by looking only where the answer is
    right."""
    # KEYED ON EVERYTHING THE ANSWER DEPENDS ON, which `(bronze_table, name, months)`
    # was not: `test_a_key_column_missing_from_one_side_is_refused_by_name` builds a
    # grain whose name and bronze table both equal `link_grain`'s and whose quarantine
    # table and key columns do not. That collided. It is harmless today only because
    # that test calls `observation_ledger` directly -- the next test routed through
    # here would have silently read the wrong ledger and passed.
    memo = (
        grain.bronze_table, grain.quarantine_table, grain.key_columns, grain.name,
        None if months is None else tuple(months),
    )
    if memo not in _LEDGERS:
        rows = observation_ledger(spark, grain, months=months).collect()
        _LEDGERS[memo] = {
            (tuple(row[column] for column in grain.key_columns), row[MONTH_COLUMN]):
                row[STATE_COLUMN]
            for row in rows
        }
    return _LEDGERS[memo]


def test_the_five_state_values_are_pinned():
    """The strings a satellite will filter on, typed out once.

    Pinned as literals for the same reason `test_hashing` pins its digests: the
    enum member and its value can drift apart silently, and it is the VALUE that
    lands in the data and in whatever query reads it next.

    `absent_after_observation` is deliberately NOT called a delete. It is a
    candidate delete and never an asserted one -- a caller who wants a departure
    signal has to map this state onto one themselves, in their own code, where the
    choice is visible in review."""
    assert [state.value for state in ObservationState] == [
        "observed",
        "observed_with_rejected_siblings",
        "rejected_by_our_gate",
        "absent_before_first_observation",
        "absent_after_observation",
    ]


def test_every_combination_of_bronze_and_quarantine_gets_its_own_state(spark, tables):
    """The derivation table, one assertion per row of it, on real fixture data.

    (bronze, quarantine) -> state, plus the two positions absence can occupy:

    | in bronze | in quarantine | state |
    |---|---|---|
    | yes | no  | observed |
    | yes | yes | observed_with_rejected_siblings |
    | no  | yes | rejected_by_our_gate |
    | no  | no  | absent_before_first_observation / absent_after_observation |
    """
    link = _ledger(spark, tables.link_grain)
    estab = _ledger(spark, tables.estab_grain)

    assert link[(_link(C_KEPT, "2", P_MANY), JUL)] == OBSERVED
    assert link[(_link(C_SIBLING, "2", P_MANY), JUL)] == SIBLINGS
    assert estab[(E_QUARANTINED, JUL)] == REJECTED
    assert link[(_link(C_NEW, "2", P_NEW), JUN)] == BEFORE
    assert link[(_link(C_LEFT, "2", P_SPLIT), JUL)] == AFTER


def test_the_estabelecimentos_shape_reports_every_departure_as_our_own_gate(spark, tables):
    """The whole ledger for the table where absence and quarantine coincide.

    `absent_after_observation` is EMPTY here, exactly as it is on the real table:
    the only key that leaves is the one we quarantined. A ledger that blamed every
    departure on our gate would pass this test, which is why it is not the
    acceptance test."""
    assert _ledger(spark, tables.estab_grain) == {
        (E_STAYS, JUN): OBSERVED, (E_STAYS, JUL): OBSERVED,
        (E_ALSO_STAYS, JUN): OBSERVED, (E_ALSO_STAYS, JUL): OBSERVED,
        (E_QUARANTINED, JUN): OBSERVED, (E_QUARANTINED, JUL): REJECTED,
        (E_NEW, JUN): BEFORE, (E_NEW, JUL): OBSERVED,
    }
    assert AFTER not in _ledger(spark, tables.estab_grain).values()


def test_the_socios_link_grain_reports_its_departure_as_the_sources(spark, tables):
    """The whole ledger for the table where absence and quarantine do NOT coincide.

    `rejected_by_our_gate` is EMPTY here: July's one quarantined row's link key is
    also in July's bronze (its duplicate sibling passed), so nothing on this table
    is missing because of us. A ledger that blamed every departure on the source
    would pass this test, which is why it is not the acceptance test either."""
    ledger = _ledger(spark, tables.link_grain)

    assert ledger == {
        (_link(C_KEPT, "2", P_MANY), JUN): OBSERVED,
        (_link(C_KEPT, "2", P_MANY), JUL): OBSERVED,
        (_link(C_SIBLING, "2", P_MANY), JUN): OBSERVED,
        (_link(C_SIBLING, "2", P_MANY), JUL): SIBLINGS,
        (_link(C_LEFT, "2", P_SPLIT), JUN): OBSERVED,
        (_link(C_LEFT, "2", P_SPLIT), JUL): AFTER,
        (_link(C_OTHER, "2", P_SPLIT), JUN): OBSERVED,
        (_link(C_OTHER, "2", P_SPLIT), JUL): OBSERVED,
        (_link(C_FOREIGN, "3", None), JUN): OBSERVED,
        (_link(C_FOREIGN, "3", None), JUL): OBSERVED,
        (_link(C_NEW, "2", P_NEW), JUN): BEFORE,
        (_link(C_NEW, "2", P_NEW), JUL): OBSERVED,
    }
    assert REJECTED not in ledger.values()


def test_a_departure_reads_as_our_gate_on_one_table_and_as_the_sources_on_the_other(
    spark, tables
):
    """THE ACCEPTANCE TEST. Two tables, two departures, two different causes, one
    assertion block -- and neither half proves anything alone.

    `E_QUARANTINED` and `(C_LEFT, 2, P_SPLIT)` are the same event as far as bronze
    is concerned: a business key present in June and absent from July. The ledger
    must separate them on the quarantine evidence and nothing else, because the
    satellites downstream treat them oppositely -- one is a candidate departure from
    the registry, the other is our own pipeline and must never reach a satellite as
    a departure at all.

    Run against a ledger that labels every departure `rejected_by_our_gate`, the
    socios half goes red. Against one that labels every departure
    `absent_after_observation`, the estabelecimentos half goes red. Both were built
    and run; the output is in the task report."""
    estab = _ledger(spark, tables.estab_grain)
    socios = _ledger(spark, tables.link_grain)

    assert estab[(E_QUARANTINED, JUN)] == OBSERVED
    assert estab[(E_QUARANTINED, JUL)] == REJECTED
    assert socios[(_link(C_LEFT, "2", P_SPLIT), JUN)] == OBSERVED
    assert socios[(_link(C_LEFT, "2", P_SPLIT), JUL)] == AFTER


def test_the_same_table_at_hub_grain_gives_a_different_answer_than_at_link_grain(
    spark, tables
):
    """The grain is a parameter, and the proof is that it CHANGES THE ANSWER.

    Same table, same months, two `ObservationGrain`s differing only in
    `key_columns`. `P_SPLIT` lost one of its two partnerships in July:

    - at LINK grain `(C_LEFT, 2, P_SPLIT)` is `absent_after_observation` -- the
      relationship is gone,
    - at HUB grain `P_SPLIT` is plainly `observed` -- the partner is still there,
      through `C_OTHER`.

    That divergence is why Task 5's effectivity satellite cannot be gated by a
    hub-grain ledger: it closes windows on a relationship, and at hub grain the
    relationship's disappearance is invisible.

    The overlap runs the other way too. `P_MANY` is `observed_with_rejected_siblings`
    at hub grain because one of its partnership rows was rejected while another
    passed -- the measured 679-of-680 shape, which exists precisely because a hub key
    is coarser than a source row."""
    hub = _ledger(spark, tables.hub_grain)

    assert hub == {
        ((P_MANY,), JUN): OBSERVED, ((P_MANY,), JUL): SIBLINGS,
        ((P_SPLIT,), JUN): OBSERVED, ((P_SPLIT,), JUL): OBSERVED,
        ((None,), JUN): OBSERVED, ((None,), JUL): OBSERVED,
        ((P_NEW,), JUN): BEFORE, ((P_NEW,), JUL): OBSERVED,
    }
    link = _ledger(spark, tables.link_grain)
    assert link[(_link(C_LEFT, "2", P_SPLIT), JUL)] == AFTER
    assert hub[((P_SPLIT,), JUL)] == OBSERVED


def test_a_key_first_seen_in_the_later_month_is_not_a_departure_in_the_earlier_one(
    spark, tables
):
    """The state the measurement forced into existence.

    A grid of every key against every month marks a key born in July as absent in
    June -- and if `absent` meant "candidate delete", the ledger would assert
    **444,520** false departures on estabelecimentos in 2026-06 alone, and 219,370
    on socios at link grain. They are not departures; they are entities that did not
    exist yet. Both absences are real absences and neither is a delete, but only one
    of them is even a CANDIDATE, so they must not share a name."""
    estab = _ledger(spark, tables.estab_grain)
    link = _ledger(spark, tables.link_grain)

    assert estab[(E_NEW, JUN)] == BEFORE
    assert link[(_link(C_NEW, "2", P_NEW), JUN)] == BEFORE
    # The discriminator is per key, not per month: the same June carries a genuine
    # post-observation absence on socios and a pre-birth one, side by side.
    assert link[(_link(C_LEFT, "2", P_SPLIT), JUL)] == AFTER


def test_a_null_business_key_component_still_matches_itself_across_months(spark, tables):
    """12,824 foreign partners carry `cpf_cnpj_socio` NULL, so the ledger has to
    treat NULL as a value that equals itself.

    It does, because presence is folded by `groupBy` rather than by an equality
    join: `NULL = NULL` is NULL in SQL, so a join-based derivation would fail to
    match the June row to the July row and report the key `absent_after_observation`
    in every month while it sat in bronze the whole time -- a wrong answer with no
    error attached to it. This test is what stops a future refactor to a join."""
    link = _ledger(spark, tables.link_grain)
    hub = _ledger(spark, tables.hub_grain)

    assert link[(_link(C_FOREIGN, "3", None), JUN)] == OBSERVED
    assert link[(_link(C_FOREIGN, "3", None), JUL)] == OBSERVED
    assert hub[((None,), JUN)] == OBSERVED
    assert hub[((None,), JUL)] == OBSERVED


def test_a_key_rejected_in_every_month_is_never_reported_absent(spark, tables):
    """Empresas: one company fails `null_or_empty_razao_social` in both months and
    reaches bronze in neither.

    It is `rejected_by_our_gate` twice, never an absence -- the quarantine is itself
    evidence that the source published the key.

    WHAT THIS PINS IS THE BRANCH'S EXISTENCE, NOT ITS POSITION, and the distinction
    was got wrong here first. This docstring used to claim it held the ORDER --
    quarantine consulted before the absence split -- and no test can hold that,
    because the order is inert: a quarantined month is inside the `min` behind
    `first_observed_month`, so the pre-birth branch cannot fire on it whichever side
    of it the quarantine branch sits. Probe 2 (deleting the quarantine branch
    entirely) turns this red; swapping two branches never would. The property the old
    claim was reaching for is pinned by
    `test_a_month_we_rejected_counts_as_having_seen_the_key` instead."""
    assert _ledger(spark, tables.empresas_grain) == {
        (("20000001",), JUN): OBSERVED, (("20000001",), JUL): OBSERVED,
        (("20000002",), JUN): OBSERVED, (("20000002",), JUL): OBSERVED,
        (("20000009",), JUN): REJECTED, (("20000009",), JUL): REJECTED,
    }


def test_a_key_with_no_quarantine_row_at_all_still_gets_a_state(spark, tables):
    """`K_RETURNS` and `K_STAYS` have no quarantine row at all, in any month: an
    empty side must contribute no state rather than emptying the result, which is
    what an inner join would do here. `K_REJECTED_FIRST`'s June quarantine row rides
    in the same ledger and does contribute a state -- that is the other property
    this fixture carries, pinned separately by
    `test_a_month_we_rejected_counts_as_having_seen_the_key`.

    RENAMED IN TASK 7's PASS from `test_a_quarantine_table_with_no_rows_at_all_still_
    produces_a_ledger` -- accurate at `f347b5d` where `returning_q` was written empty,
    falsified by `e1951b0` adding `K_REJECTED_FIRST`'s June row to it, which updated
    this docstring and not the name. Assertions unchanged: the property was always
    about a KEY with no quarantine row. Nothing else referenced the old name."""
    ledger = _ledger(spark, tables.returning_grain, months=[JUN, JUL])

    assert ledger == {
        (("30000001",), JUN): OBSERVED, (("30000001",), JUL): AFTER,
        (("30000002",), JUN): OBSERVED, (("30000002",), JUL): OBSERVED,
        (("30000003",), JUN): REJECTED, (("30000003",), JUL): AFTER,
    }


def test_a_month_we_rejected_counts_as_having_seen_the_key(spark, tables):
    """`K_REJECTED_FIRST`: quarantined in June, absent in July, in bronze in August.

    `first_observed_month` is a `min` over bronze UNION quarantine (`_grid`), so
    June -- a quarantine-only month for this key -- counts as having seen it.
    Get that wrong (`first_observed_month` as "first observed IN BRONZE") and July
    reads as `absent_before_first_observation`, as if the key did not exist until
    August; get it right and July is `absent_after_observation`, because being
    rejected in June is itself evidence the source published the key. Both
    `_grid` and `_state_column` name this test as the one that pins the decision;
    this is that test."""
    ledger = _ledger(spark, tables.returning_grain, months=[JUN, JUL, AUG])

    assert ledger[(("30000003",), JUN)] == REJECTED
    assert ledger[(("30000003",), JUL)] == AFTER
    assert ledger[(("30000003",), AUG)] == OBSERVED

    row = (observation_ledger(spark, tables.returning_grain)
           .filter(f"cnpj_basico = '30000003' and {MONTH_COLUMN} = '{JUL}'")
           .collect())
    assert row[0][FIRST_OBSERVED_COLUMN] == JUN


def test_a_key_that_returns_keeps_the_label_it_had_before_the_later_month_arrived(
    spark, tables
):
    """The absence split is defined on the PAST only, and this is the test that
    holds it there.

    `30000001` is in June, gone in July, back in August. Asked for June and July,
    July is `absent_after_observation`. Asked for all three, July is STILL
    `absent_after_observation` -- appending a later month does not relabel an
    earlier one.

    An implementation that defined the split as "before first observation / after
    LAST observation" would fail this: with August present, July is no longer after
    the last observation, and the row would silently change meaning as new data
    arrived. Every state here is a statement about the months already read."""
    two_months = _ledger(spark, tables.returning_grain, months=[JUN, JUL])
    three_months = _ledger(spark, tables.returning_grain, months=[JUN, JUL, AUG])

    assert two_months[(("30000001",), JUL)] == AFTER
    assert three_months[(("30000001",), JUL)] == AFTER
    assert three_months[(("30000001",), AUG)] == OBSERVED
    assert three_months == {
        (("30000001",), JUN): OBSERVED,
        (("30000001",), JUL): AFTER,
        (("30000001",), AUG): OBSERVED,
        (("30000002",), JUN): OBSERVED,
        (("30000002",), JUL): OBSERVED,
        (("30000002",), AUG): OBSERVED,
        (("30000003",), JUN): REJECTED,
        (("30000003",), JUL): AFTER,
        (("30000003",), AUG): OBSERVED,
    }


def test_the_months_are_discovered_when_the_caller_names_none(spark, tables):
    """The default is every month the two tables hold between them, which is the
    safe default: the absence states are relative to the window the ledger was asked
    for, so the widest window is the one that truncates the least history."""
    discovered = _ledger(spark, tables.returning_grain)

    assert {month for _, month in discovered} == {JUN, JUL, AUG}
    assert len(discovered) == 9


def test_naming_one_month_scopes_both_the_rows_and_the_keys(spark, tables):
    """A narrowed window is narrowed on BOTH axes, and the consequence is stated
    rather than hidden.

    Asked for June alone, the ledger reports June's three establishment keys and
    says nothing about `E_NEW`, which is not in June's data at all -- it is not
    reported `absent`, it is not reported. And `E_QUARANTINED`, a departure over the
    full window, is simply `observed`: within June there is nothing to depart from.

    That is what "the states are relative to the window" means, and it is why the
    default reads every month. A caller that narrows the window is choosing to have
    less history consulted, and the ledger cannot warn them -- the months it was not
    given are months it never read."""
    june_only = _ledger(spark, tables.estab_grain, months=[JUN])

    assert june_only == {
        (E_STAYS, JUN): OBSERVED,
        (E_ALSO_STAYS, JUN): OBSERVED,
        (E_QUARANTINED, JUN): OBSERVED,
    }


def test_the_ledger_holds_exactly_one_row_per_business_key_per_month(spark, tables):
    """The stated grain, asserted as a count rather than inferred from the dicts
    above -- those are built BY key and month, so a duplicate row would silently
    overwrite its twin and every equality assertion in this file would still pass.

    Sócios' July source holds two rows for one link key (the sibling case) and the
    ledger has to fold them into one row, not two."""
    for grain, keys, months in (
        (tables.link_grain, 6, 2),
        (tables.hub_grain, 4, 2),
        (tables.estab_grain, 4, 2),
        (tables.returning_grain, 3, 3),
    ):
        pairs = [
            (tuple(row[column] for column in grain.key_columns), row[MONTH_COLUMN])
            for row in observation_ledger(spark, grain)
            .select(*grain.key_columns, MONTH_COLUMN).collect()
        ]
        assert len(pairs) == keys * months, grain.name
        assert len(set(pairs)) == keys * months, grain.name


def test_the_ledger_carries_the_evidence_its_state_was_derived_from(spark, tables):
    """The two booleans and the first-observed month are output columns, not
    internals. The state is a LABEL over them; keeping the facts beside it is what
    lets an operator check a verdict without re-deriving it, and what lets the
    controller count the state mix against real bronze in one query."""
    row = (observation_ledger(spark, tables.estab_grain)
           .filter(f"cnpj_basico = '{E_QUARANTINED[0]}' and {MONTH_COLUMN} = '{JUL}'")
           .collect())

    assert len(row) == 1
    assert row[0][IN_BRONZE_COLUMN] is False
    assert row[0][IN_QUARANTINE_COLUMN] is True
    assert row[0][FIRST_OBSERVED_COLUMN] == JUN
    assert row[0][STATE_COLUMN] == REJECTED


def test_a_key_column_missing_from_one_side_is_refused_by_name(spark, tables):
    """The quarantine table is a separate table with its own schema, and a key
    column present in bronze and absent there would otherwise surface as a Spark
    `AnalysisException` naming neither the grain nor which of the two tables was
    short."""
    grain = ObservationGrain(
        name="link_company_partner", bronze_table=tables.socios,
        quarantine_table=tables.empresas_q, key_columns=("cnpj_basico", "cpf_cnpj_socio"),
    )

    with pytest.raises(ValueError, match="cpf_cnpj_socio"):
        observation_ledger(spark, grain)


def test_an_empty_month_list_is_refused(spark, tables):
    """`months=[]` would return an empty ledger -- no rows, no error, and a caller
    reading "no departures" out of it."""
    with pytest.raises(ValueError, match="at least one month"):
        observation_ledger(spark, tables.estab_grain, months=[])


def test_a_bare_string_month_is_refused(spark, tables):
    """`months="2026-07"` iterates to seven one-character strings, none of them a
    month, and would otherwise produce an empty ledger by a different route."""
    with pytest.raises(TypeError, match="bare str"):
        observation_ledger(spark, tables.estab_grain, months="2026-07")


def test_a_value_that_is_not_a_month_is_refused(spark, tables):
    """Delegated to `opl.config.is_month`, which is the one spelling of that rule in
    this repository -- `2026-13` has the shape and names no month."""
    with pytest.raises(ValueError, match="2026-13"):
        observation_ledger(spark, tables.estab_grain, months=[JUN, "2026-13"])


def test_a_month_with_no_row_on_either_side_is_refused(spark, tables):
    """`_window`'s guard, and the reason it is worth an eager Spark job: `2026-09`
    is well-formed and names no month either table carries. Answering instead of
    refusing would give every key in the window an `absent_after_observation` row
    for it -- a candidate delete for the whole table, conjured out of a typo or an
    ingest that never ran."""
    with pytest.raises(ValueError, match="2026-09.*candidate delete"):
        observation_ledger(spark, tables.estab_grain, months=[JUN, JUL, "2026-09"])


def test_a_month_present_on_only_one_side_is_accepted(spark):
    """The guard's own message calls this case out as accepted, so it needs a test
    that cannot pass by accident: without it, the guard could tighten from "no row
    on EITHER side" to "present in bronze" in a later edit and nothing here would
    notice.

    Two temp views stand in for bronze and quarantine -- no Delta write needed, and
    none of the four module fixtures happens to have a month that is quarantine-only.
    `quarantine` carries July on its own; `bronze` never does."""
    bronze_view, quarantine_view = f"one_sided_bronze_{uuid4().hex[:8]}", (
        f"one_sided_quarantine_{uuid4().hex[:8]}"
    )
    spark.createDataFrame(
        [("A", JUN, JUN_REF)], _RETURNING
    ).createOrReplaceTempView(bronze_view)
    spark.createDataFrame(
        [("A", JUN, JUN_REF, "reason"), ("B", JUL, JUL_REF, "reason")],
        _RETURNING + _REJECT_REASON,
    ).createOrReplaceTempView(quarantine_view)
    grain = ObservationGrain(
        name="one_sided", bronze_table=bronze_view, quarantine_table=quarantine_view,
        key_columns=("cnpj_basico",),
    )

    ledger = observation_ledger(spark, grain, months=[JUN, JUL]).collect()

    assert {row[MONTH_COLUMN] for row in ledger} == {JUN, JUL}
