"""The merchant domain against real Spark: the first link across TWO SOURCES, and the
first end-dating this lakehouse has ever produced.

WHAT IS BEING SHOWN, AND WHAT IS NOT. Every number below is over a synthesised bronze
fixture on local Spark. `bronze_merchant` holds ZERO rows on Databricks (evidence §3) and
nothing has been ingested, so NOTHING here is a claim about the phase's predicted 1,088
link rows or its 16 closes -- Task 6 owns that run. What this file proves is that the
MECHANISM exists and fires: a hard delete at link grain produces a row carrying
`is_active=false` and `closed_by='absent_after_observation'`, which no test and no run in
this repository had ever produced before. F2 measured zero departures across 68,629,147
RFB keys, so `load_effectivity_satellite`'s closing path had never executed on any data.

THE FIXTURE IS THE MEASURED POPULATION IN MINIATURE, ONE ROW PER CLASS, and it is meant
to be read top to bottom -- the shape IS the argument:

  M_STAYS      present in both, unchanged                     the ordinary case
  M_TWIN       a SECOND merchant on M_STAYS' own CNPJ ROOT    the link is not degenerate
               ...and its `updated_at` MOVES with no payload change
  M_UPDATED    payload changes                                the satellite's second row
  M_DELETED    snapshot 1 only, NOT quarantined               THE HARD DELETE
  M_REJECTED   snapshot 1's bronze, snapshot 2's quarantine   must NOT close a window
  M_NEW        snapshot 2 only                                the arrival
  M_REPOINTED  same merchant, a DIFFERENT CNPJ at snapshot 2  why both ends are identifying

M_REPOINTED IS THE ROW THAT ARGUES THE MODELLING. With `merchant_id` alone as the link's
identity it keeps its link hash key when the company changes, the old relationship never
becomes `absent_after_observation`, and no window is ever closed for it. With `cnpj` in
the digest it is one departure and one arrival -- and it is STILL `observed` at hub grain,
which is the hub-versus-link distinction `opl.vault.effectivity` states in prose and
nothing had ever exercised. This file asserts both halves of that at once: the descriptive
satellite's hub-grain ledger reports ONE candidate departure and the effectivity
satellite's link-grain ledger closes TWO windows, over the same two snapshots.

TWO SNAPSHOTS ON TWO CALENDAR DAYS, WHICH IS T8 AND NOT A CONVENIENCE. `applied_date` is a
DATE derived from `_snapshot_at`, and `opl.vault.satellites` dedups on
`groupBy(hash_key, applied_date)` with a tie-break it calls the worst fold it performs.
Two snapshots taken on ONE day fold into one row with no error and no red test, and both
update classes vanish. The instants below are 2026-08-16 and 2026-08-17.

AND THE AXIS IS `_snapshot_at`, NOT `_snapshot_month`. Both instants fall in 2026-08, so
on the monthly default the ledger folds them into one observation, M_DELETED reads
`observed`, and the closing path has no producer -- no error, no red test. That is the
whole of T7, and this fixture is where it stops being an argument."""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from uuid import uuid4

import pytest

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.bronze.snapshot_axis import INSTANT_SNAPSHOT
from opl.contracts import merchant as merchant_contract
from opl.contracts.catalogue import columns_for
from opl.vault import domains
from opl.vault.columns import CLOSED_BY, IS_ACTIVE, LAST_OBSERVED_ON
from opl.vault.domains.merchant_domain import UNMODELLED_MERCHANT_COLUMNS
from opl.vault.effectivity import CLOSING_STATE, load_effectivity_satellite
from opl.vault.hubs import load_hub
from opl.vault.links import load_link
from opl.vault.loading import hash_key_for_end, link_hash_key_expression
from opl.vault.observation import ObservationGrain
from opl.vault.registry import identifying_hubs, identity_derivations_of
from opl.vault.satellites import load_satellite

from .conftest import AUDIT_DDL, INGESTED_AT, LOADED_AT, REJECT_REASON_DDL, write_delta

HUB = domains.table_spec("hub_merchant")
EMPRESA_HUB = domains.table_spec("hub_empresa")
DADOS = domains.table_spec("sat_merchant_dados")
LINK = domains.table_spec("link_merchant_empresa")
EFF = domains.table_spec("sat_eff_merchant_empresa")
LINK_HUBS = domains.linked_hubs(LINK)
LINK_IDENTITY = domains.link_identity_columns(LINK)
# The COLUMNS are half the grain; these are the other half -- the prefixes those columns
# are read through, which is what makes the ledger's values the link's identity rather
# than a value space many-to-one onto it. See the branch test below.
LINK_IDENTITY_PREFIXES = identity_derivations_of(LINK)

_MERCHANT_SPEC = bronze_table_spec("merchant")
MERCHANT_CONTRACT = tuple(columns_for("merchant"))

# The two observations, on two calendar days. Fixed width, UTC, `.US` with the trailing
# zeros KEPT -- `opl.bronze.snapshot_axis` argues why the width is load-bearing: the
# ledger's before/after split is a LEXICOGRAPHIC comparison being used as a chronological
# one, and `...01.1` sorts after `...01.09`.
S1 = "2026-08-16T22:10:46.710500Z"
S2 = "2026-08-17T22:11:03.884210Z"
# What `opl.bronze.snapshot.ref_date_from_instant` derives from each, as bronze holds it.
S1_REF, S2_REF = "2026-08-16", "2026-08-17"
# Both instants fall in ONE calendar month, which is the whole reason this source declares
# a finer axis. Stamped from the job's month parameter, exactly as bronze stamps it.
INGEST_MONTH = "2026-08"

RECORD_SOURCE_VALUE = "postgres_merchant"

# (merchant_id, cnpj). The UUIDs are `::text`-rendered, thirty-six characters, which is
# why `hub_merchant`'s business key declares NO width.
M_STAYS = ("11111111-1111-4111-8111-111111111111", "10000001000199")
M_TWIN = ("22222222-2222-4222-8222-222222222222", "10000001000288")
M_UPDATED = ("33333333-3333-4333-8333-333333333333", "10000002000199")
M_DELETED = ("44444444-4444-4444-8444-444444444444", "10000003000199")
M_REJECTED = ("55555555-5555-4555-8555-555555555555", "10000004000199")
M_NEW = ("66666666-6666-4666-8666-666666666666", "10000005000199")
M_REPOINTED_BEFORE = ("77777777-7777-4777-8777-777777777777", "10000006000199")
M_REPOINTED_AFTER = ("77777777-7777-4777-8777-777777777777", "10000007000199")
# THE BRANCH: one merchant, ONE eight-character root, two different full CNPJs. Held out
# of the shared fixture below and given a database of its own, because it is not a class
# of the measured population -- it is the input that reaches the ledger-grain hole T5b
# left, and it belongs to the test that names it rather than to every count in this file.
M_BRANCH_BEFORE = ("88888888-8888-4888-8888-888888888888", "10000008000199")
M_BRANCH_AFTER = ("88888888-8888-4888-8888-888888888888", "10000008000288")

# Every eight-character root the fixture reaches, which is what `hub_empresa` must hold
# before the link may be written -- `links.refuse_unloaded_hubs` refuses an empty one.
EMPRESA_ROOTS = (
    "10000001", "10000002", "10000003", "10000004", "10000005", "10000006", "10000007",
)

_MERCHANT_DEFAULTS = {
    "legal_name": "PADARIA CENTRAL LTDA",
    "trade_name": "PADARIA CENTRAL",
    "status": "active",
    "mcc": "5812",
    "settlement_account": "0001-00012345",
    "risk_tier": "standard",
    "credit_limit": "50000.00",
    "onboarded_on": "2024-03-01",
    "updated_at": "2026-08-01 09:00:00.000000+00",
    "_pg_snapshot": "1200:1200:",
    "_pg_wal_lsn": "0/1A2B3C4",
}

_MERCHANT_SCHEMA = (
    ", ".join(f"{column} string" for column in MERCHANT_CONTRACT) + ", " + AUDIT_DDL
)
_QUARANTINE_SCHEMA = _MERCHANT_SCHEMA + REJECT_REASON_DDL


def merchant_row(key: tuple[str, str], instant: str, **overrides) -> tuple:
    """One bronze merchant row: the whole contract plus every audit column the ingest
    stamps.

    `_snapshot_ref_date` is DERIVED FROM THE INSTANT rather than from a filename token,
    which is T8's third audit-column path -- the RFB's comes from `.D<Y><MM><DD>.` and
    this source has no such filename. The fixture spells the derived value so a change
    to that derivation shows up here rather than as a satellite silently folding two
    days into one."""
    values = dict(_MERCHANT_DEFAULTS)
    values.update(zip(("merchant_id", "cnpj"), key, strict=True))
    values["_snapshot_at"] = instant
    values.update(overrides)
    ref_date = S1_REF if instant == S1 else S2_REF
    return tuple(values[column] for column in MERCHANT_CONTRACT) + (
        None,
        f"/Volumes/x/postgres/merchant/{instant}/merchant.jsonl",
        INGESTED_AT,
        RECORD_SOURCE_VALUE,
        "batch-1",
        INGEST_MONTH,
        date.fromisoformat(ref_date),
    )


def _bronze_rows() -> list[tuple]:
    """The fixture's bronze rows, meant to be read top to bottom."""
    return [
        merchant_row(M_STAYS, S1),
        merchant_row(M_STAYS, S2),
        # A SECOND MERCHANT ON THE SAME CNPJ ROOT, which is what keeps the link from
        # being degenerate -- 64 of the seeded 1,024 companies carry two. Its
        # `updated_at` moves and NOTHING a reader cares about does, which is the
        # watermark's silent class in miniature and pins `updated_at` out of the payload.
        merchant_row(M_TWIN, S1),
        merchant_row(M_TWIN, S2, updated_at="2026-08-17 22:05:00.000000+00"),
        merchant_row(M_UPDATED, S1),
        merchant_row(
            M_UPDATED, S2, status="suspended", risk_tier="elevated",
            credit_limit="12000.00", trade_name=None,
            updated_at="2026-08-17 22:06:00.000000+00",
        ),
        # THE HARD DELETE. In snapshot 1 and in NEITHER of snapshot 2's tables.
        merchant_row(M_DELETED, S1),
        # OUR OWN GATE'S DOING, not Postgres'. In snapshot 2's quarantine below.
        merchant_row(M_REJECTED, S1),
        merchant_row(M_NEW, S2),
        # THE RE-POINT: one merchant_id, two companies, two link keys.
        merchant_row(M_REPOINTED_BEFORE, S1),
        merchant_row(M_REPOINTED_AFTER, S2, updated_at="2026-08-17 22:07:00.000000+00"),
    ]


def _quarantine_rows() -> list[tuple]:
    """Snapshot 2's reject, with the gate's reason. Absent from snapshot 2's bronze
    because of US, which is the distinction ADR 0010 is about and the one thing that
    keeps this row from closing a window."""
    return [
        (*merchant_row(M_REJECTED, S2, legal_name="PADARIA CENTR�L"),
         "encoding_replacement_char")
    ]


_EMPRESA_SCHEMA = "cnpj_basico string, " + AUDIT_DDL


def _empresa_rows() -> list[tuple]:
    """A minimal empresas bronze feed, so `hub_empresa` EXISTS and is populated before
    the link is written.

    A SEPARATE TABLE, WHICH IS THE POINT OF THE WHOLE PHASE. This hub is loaded from the
    Receita's own file feed by `vault_empresa_job`, and the merchant link reaches it
    across a source boundary -- Files and Databases meeting on one hub. `bronze_merchant`
    has no `cnpj_basico` column at all, so there is no version of this fixture in which
    one table feeds both."""
    return [
        (root, None, "/Volumes/x/cnpj/2026-06/empresas/K3241.K03200Y0.D60613.EMPRECSV",
         INGESTED_AT, "rfb_cnpj_webdav", "batch-0", "2026-06",
         date(2026, 6, 13))
        for root in EMPRESA_ROOTS
    ]


@pytest.fixture(scope="module")
def merchant_source(spark, vault_database):
    """A throwaway Delta database holding one bronze merchant table, its quarantine, and
    the empresas feed `hub_empresa` is loaded from."""
    db = vault_database("merchant_vault")
    bronze, quarantine = f"{db}.merchant", f"{db}.merchant_q"
    empresas = f"{db}.empresas"

    write_delta(spark, bronze, _MERCHANT_SCHEMA, _bronze_rows())
    write_delta(spark, quarantine, _QUARANTINE_SCHEMA, _quarantine_rows())
    write_delta(spark, empresas, _EMPRESA_SCHEMA, _empresa_rows())

    common = {
        "bronze_table": bronze,
        "quarantine_table": quarantine,
        "snapshot_axis": INSTANT_SNAPSHOT,
    }
    return SimpleNamespace(
        db=db, bronze=bronze, quarantine=quarantine, empresas=empresas,
        hub_grain=ObservationGrain(
            name=HUB.name, key_columns=HUB.business_key_columns, **common
        ),
        link_grain=ObservationGrain(
            name=LINK.name, key_columns=LINK_IDENTITY,
            key_prefixes=LINK_IDENTITY_PREFIXES, **common
        ),
    )


@pytest.fixture(scope="module")
def branch_source(spark, vault_database):
    """One merchant, observed twice, whose full `cnpj` changes while its eight-character
    ROOT does not -- and nothing else.

    ITS OWN DATABASE RATHER THAN A ROW IN `_bronze_rows`, deliberately. The shared fixture
    is "the measured population in miniature, one row per class" and every count in this
    file is read off it; a branch is not a class the seeded population has (see the test
    below for why it cannot), so adding it there would move seven assertions to prove one.
    The effectivity loader reads bronze and quarantine and no hub table, so two rows and
    an empty quarantine are the whole input."""
    db = vault_database("merchant_branch")
    bronze, quarantine = f"{db}.branch", f"{db}.branch_q"

    write_delta(spark, bronze, _MERCHANT_SCHEMA,
                [merchant_row(M_BRANCH_BEFORE, S1), merchant_row(M_BRANCH_AFTER, S2)])
    write_delta(spark, quarantine, _QUARANTINE_SCHEMA, [])
    return SimpleNamespace(db=db, bronze=bronze, quarantine=quarantine)


@pytest.fixture
def merchant_target(merchant_source):
    """Fresh table names per test, for the tests that WRITE -- sharing one would make
    idempotence pass for the wrong reason."""
    db, suffix = merchant_source.db, uuid4().hex[:8]
    return SimpleNamespace(
        hub=f"{db}.hub_{suffix}",
        empresa_hub=f"{db}.emp_{suffix}",
        dados=f"{db}.dados_{suffix}",
        link=f"{db}.link_{suffix}",
        eff=f"{db}.eff_{suffix}",
    )


def load_merchant_vault(spark, source, names, *, months=None):
    """One load of each of the five tables, in dependency order, over `months`.

    `hub_empresa` IS LOADED FROM ITS OWN SOURCE, which no other fixture in this package
    has had to do: every table `load_estabelecimento_vault` writes comes off one bronze
    table, because estabelecimentos carries `cnpj_basico`. Here the two ends of the link
    genuinely live in two feeds, and the empresa load takes the MONTHLY axis because that
    is what its own source declares."""
    names.empresa_hub_result = load_hub(
        spark, EMPRESA_HUB, source_table=source.empresas,
        target_table=names.empresa_hub, load_date=LOADED_AT, months=None,
    )
    names.hub_result = load_hub(
        spark, HUB, source_table=source.bronze, target_table=names.hub,
        load_date=LOADED_AT, months=months, axis=INSTANT_SNAPSHOT,
    )
    # `report_diagnostics=True` because this fixture's readers assert both numbers, and
    # because the job YAML sets it true for this source -- see that file for why the
    # cost argument that makes it default-off does not carry at 1,088 rows.
    names.dados_result = load_satellite(
        spark, DADOS, hub=HUB, source_table=source.bronze, target_table=names.dados,
        load_date=LOADED_AT, grain=source.hub_grain, months=months,
        report_diagnostics=True,
    )
    names.link_result = load_link(
        spark, LINK, hubs=LINK_HUBS,
        hub_tables={HUB.name: names.hub, EMPRESA_HUB.name: names.empresa_hub},
        source_table=source.bronze, target_table=names.link, load_date=LOADED_AT,
        months=months, axis=INSTANT_SNAPSHOT,
    )
    names.eff_result = load_effectivity_satellite(
        spark, EFF, link=LINK, hubs=LINK_HUBS, source_table=source.bronze,
        target_table=names.eff, load_date=LOADED_AT, grain=source.link_grain,
        months=months,
    )
    return names


@pytest.fixture(scope="module")
def merchant_loaded(spark, merchant_source):
    """One load of every table over both snapshots, shared by every read-only
    assertion."""
    db = merchant_source.db
    names = SimpleNamespace(
        hub=f"{db}.hub_shared", empresa_hub=f"{db}.emp_shared",
        dados=f"{db}.dados_shared", link=f"{db}.link_shared", eff=f"{db}.eff_shared",
    )
    return load_merchant_vault(spark, merchant_source, names, months=[S1, S2])


# --- the hub and its satellite -------------------------------------------------------


def test_the_hub_holds_one_row_per_merchant_id_ever_seen(spark, merchant_loaded):
    """Seven merchants across two snapshots, and the re-pointed one is ONE key: its
    `merchant_id` did not move, only the company it points at."""
    assert merchant_loaded.hub_result.appended == 7
    assert spark.read.table(merchant_loaded.hub).count() == 7


def test_a_moved_updated_at_writes_no_satellite_row(spark, merchant_loaded):
    """M_TWIN's `updated_at` moves between the snapshots and nothing else does.

    THIS IS THE ASSERTION THAT KEEPS THE TWO AXES APART. `updated_at` is landed in bronze
    -- the phase's headline is measured against it -- and is deliberately NOT in
    `sat_merchant_dados`' payload: a payload carrying it would move `hash_diff` for every
    row the source's `BEFORE UPDATE` trigger touched, and the 48-that-move against the
    24-that-do-not would stop being distinguishable at the satellite with every count
    still green."""
    assert spark.read.table(merchant_loaded.dados).count() == 8, (
        "one row per merchant, plus M_UPDATED's second"
    )

    twin = _satellite_rows_for(spark, merchant_loaded, M_TWIN)
    assert len(twin) == 1


def test_a_changed_payload_writes_a_second_satellite_row(spark, merchant_loaded):
    assert len(_satellite_rows_for(spark, merchant_loaded, M_UPDATED)) == 2


def test_a_re_pointed_merchant_moves_the_link_and_not_the_satellite(
    spark, merchant_loaded
):
    """`cnpj` is the LINK's identity and is deliberately not a satellite payload column,
    so re-pointing a merchant writes no descriptive row -- the description did not
    change. What it writes is a new link key and a closed window on the old one."""
    assert len(_satellite_rows_for(spark, merchant_loaded, M_REPOINTED_BEFORE)) == 1


def test_the_hub_grain_ledger_reports_one_departure_where_the_link_grain_closes_two(
    merchant_loaded
):
    """THE HUB-VERSUS-LINK DISTINCTION, ON REAL ROWS, FOR THE FIRST TIME.
    `opl.vault.effectivity` states it in prose -- a key that loses one of two
    relationships is `absent_after_observation` at LINK grain and plainly `observed` at
    hub grain -- and nothing had ever produced the divergence, because F2 measured ZERO
    departures at either grain across 68,629,147 keys.

    Here M_DELETED departs at both grains and M_REPOINTED departs only at link grain, so
    the hub-grain diagnostic says 1 and the effectivity load closes 2. A hub-grain ledger
    driving the effectivity satellite would leave the re-pointed relationship open
    forever, with nothing failing."""
    assert merchant_loaded.dados_result.candidate_departures == 1
    assert merchant_loaded.eff_result.closed == 2


def test_the_key_the_link_the_payload_and_the_open_partition_the_merchant_contract():
    """Every one of the eleven POSTGRES columns is the hub's business key, part of the
    link's identity, the satellite's payload, the effectivity window's delivered open, or
    a declared omission -- and the five sets are disjoint.

    ON `cnpj.py`'s PRECEDENT, AND FOR THE REASON THAT FILE ALREADY HELD AND THIS ONE ONLY
    ARGUED. `merchant_domain.py`'s docstring accounts for each of the four columns
    `sat_merchant_dados` does not carry, and the arguments are right; what it lacked was
    the mechanical half. A twelfth column added to `opl.contracts.merchant.SOURCE_COLUMNS`
    would have landed in bronze, been modelled by nothing, and turned no test red --
    indistinguishable from one someone forgot, which is exactly what
    `UNMODELLED_MERCHANT_COLUMNS` exists never to be. Pure, no Spark: an upstream column
    turns it red in milliseconds.

    OVER `SOURCE_COLUMNS` AND NOT THE FOURTEEN-COLUMN CONTRACT. The other three are the
    OBSERVING TRANSACTION's stamps -- `_snapshot_at`, `_pg_snapshot`, `_pg_wal_lsn` --
    which this lakehouse writes rather than reads from Postgres, and one of them is this
    source's snapshot AXIS rather than a fact about a merchant. They are not modelled and
    must not be: `opl.contracts.merchant` already asserts at import that the two groups
    partition the contract and that the leading underscore is what tells them apart, so
    this test's scope is total without restating that guard.

    `cnpj` IS TAKEN OFF THE LINK RATHER THAN NAMED. It is accounted for because it is the
    link's identity, and a test that spelled it would still pass on the day the link
    stopped keying on it."""
    source = set(merchant_contract.SOURCE_COLUMNS)
    key = set(HUB.business_key_columns)
    link_only = set(LINK_IDENTITY) - key
    payload, omitted = set(DADOS.payload_columns), set(UNMODELLED_MERCHANT_COLUMNS)
    entry = {EFF.entry_column}
    sets = (key, link_only, payload, entry, omitted)

    assert set().union(*sets) == source
    assert sum(len(part) for part in sets) == len(merchant_contract.SOURCE_COLUMNS)
    assert source.isdisjoint(merchant_contract.OBSERVATION_COLUMNS)


def test_the_diagnostics_were_measured_rather_than_skipped(merchant_loaded):
    """`None` is NOT `0`, which is the distinction `SatelliteLoadResult` refuses to blur
    -- so the 1 above is only evidence because this load was asked to measure."""
    assert merchant_loaded.dados_result.collapsed_duplicates == 0


# --- the link across two sources -----------------------------------------------------


def test_the_link_holds_one_row_per_merchant_and_company_pair_ever_seen(
    spark, merchant_loaded
):
    """Eight: seven merchants, of which the re-pointed one contributes TWO pairs.

    M_REJECTED contributes one, from snapshot 1's bronze -- our own gate removing its
    snapshot 2 row does not un-see the relationship we already observed."""
    assert merchant_loaded.link_result.appended == 8
    assert spark.read.table(merchant_loaded.link).count() == 8


def test_two_merchants_on_one_company_are_two_link_rows_and_one_hub_key(
    spark, merchant_loaded
):
    """The link is not degenerate, which is what the 64 second merchants in the seeded
    population exist for. Both rows carry the SAME `hub_empresa_hk` -- the digest
    `load_hub` wrote for `10000001` -- and different `hub_merchant_hk`."""
    rows = spark.read.table(merchant_loaded.link).select(
        HUB.hash_key, EMPRESA_HUB.hash_key
    ).collect()
    by_empresa: dict[str, set[str]] = {}
    for row in rows:
        by_empresa.setdefault(row[EMPRESA_HUB.hash_key], set()).add(row[HUB.hash_key])
    shared = [merchants for merchants in by_empresa.values() if len(merchants) > 1]

    assert len(shared) == 1 and len(shared[0]) == 2


def test_the_empresa_reference_is_the_digest_the_cnpj_domains_own_hub_wrote(
    spark, merchant_loaded
):
    """THE INTEGRATION CLAIM, ASSERTED RATHER THAN ARGUED. The link's empresa reference is
    derived from the first eight characters of a fourteen-character `cnpj`, and
    `hub_empresa` was loaded from an entirely different bronze table that spells the same
    value `cnpj_basico`. If `hash_key_for_end` were a second spelling of the standard the
    two would differ and every join would return nothing, silently -- which is the
    quietest wrong answer this layer gives.

    An anti-join, not a count: it is the referential claim itself."""
    hub_keys = spark.read.table(merchant_loaded.empresa_hub).select(EMPRESA_HUB.hash_key)
    dangling = (spark.read.table(merchant_loaded.link)
                .select(EMPRESA_HUB.hash_key)
                .join(hub_keys, on=EMPRESA_HUB.hash_key, how="left_anti")
                .count())

    assert dangling == 0


def test_the_generic_link_loader_writes_this_link(spark, merchant_source, merchant_target):
    """`load_link` and NOT a third bespoke loader, which is the whole of T5b: the
    derivation is declared on the end and `build_registry` checks it against the hub, so
    the generic loader can compute the reference through `hash_key_over` -- the seam
    `opl.vault.partners` was already the only caller of."""
    names = load_merchant_vault(spark, merchant_source, merchant_target, months=[S1, S2])

    assert names.link_result.appended == 8


# --- THE END-DATING ------------------------------------------------------------------


def test_a_hard_delete_produces_a_closing_row(spark, merchant_loaded):
    """THE FIRST END-DATING IN THIS LAKEHOUSE, and the row this whole phase exists for.

    A merchant deleted from Postgres between the two snapshots leaves the link key out of
    snapshot 2's bronze, the ledger calls it `absent_after_observation`, and the
    effectivity satellite writes a row carrying `is_active=false`, the `applied_date` of
    the last snapshot that SAW it in `last_observed_on`, and the state that authorised the
    close in `closed_by`. Every one of those three is a DERIVED claim: Postgres never told
    us the relationship ended."""
    closing = _effectivity_rows(spark, merchant_loaded, M_DELETED)
    closed = [row for row in closing if not row[IS_ACTIVE]]

    assert len(closed) == 1
    assert closed[0][CLOSED_BY] == CLOSING_STATE.value == "absent_after_observation"
    assert str(closed[0][LAST_OBSERVED_ON]) == S1_REF
    assert str(closed[0]["applied_date"]) == S2_REF


def test_the_delivered_open_is_carried_onto_the_closing_row(spark, merchant_loaded):
    """A closing row has no source row -- that is what makes it a close -- so its
    `onboarded_on` comes from the last snapshot we DID observe. The one row where a
    reader most needs both ends of the window is the one where the open would otherwise
    be NULL."""
    closed = [row for row in _effectivity_rows(spark, merchant_loaded, M_DELETED)
              if not row[IS_ACTIVE]]

    assert closed[0][EFF.entry_column] == "2024-03-01"


def test_a_row_our_own_gate_rejected_closes_no_window(spark, merchant_loaded):
    """THE GATE, AND IT IS THE REASON THE CLOSE IS DRIVEN BY THE LEDGER RATHER THAN BY A
    DIFF. M_REJECTED is absent from snapshot 2's bronze for exactly the same reason
    M_DELETED is -- there is no row -- and the two are not the same fact: one is Postgres
    deleting, the other is our own DQ gate quarantining. ADR 0010 is about that
    distinction and ADR 0011 measures what confusing them costs (1,781 sócio keys per
    month whose windows would be falsely closed). A host-side differ structurally cannot
    tell them apart, because quarantine does not exist at that point."""
    rows = _effectivity_rows(spark, merchant_loaded, M_REJECTED)

    assert [row[IS_ACTIVE] for row in rows] == [True]


def test_a_re_pointed_merchant_closes_the_relationship_it_left(spark, merchant_loaded):
    """WHY BOTH ENDS ARE IDENTIFYING, demonstrated rather than argued. With `merchant_id`
    alone in the link's digest this merchant keeps its link hash key across the re-point,
    the old (merchant, company) pair never becomes absent, and NO closing row is written.
    With `cnpj` in the digest it is one close and one open."""
    left = _effectivity_rows(spark, merchant_loaded, M_REPOINTED_BEFORE)
    joined = _effectivity_rows(spark, merchant_loaded, M_REPOINTED_AFTER)

    assert [row[IS_ACTIVE] for row in left] == [True, False]
    assert [row[CLOSED_BY] for row in left][1] == CLOSING_STATE.value
    assert [row[IS_ACTIVE] for row in joined] == [True]


def test_a_branch_on_one_root_is_one_open_window_and_never_a_close(spark, branch_source):
    """THE LEDGER'S GRAIN IS THE VALUE THE LINK KEYS ON, AND NOT THE COLUMN IT READS IT
    FROM -- the hole T5b left, on the one input that reaches it.

    `identity_columns_of` answers this link's grain with the SOURCE column `cnpj`,
    fourteen characters, and the link hashes `substring(cnpj, 1, 8)`. That map is
    MANY-TO-ONE, so a merchant that keeps its eight-character root and changes its full
    `cnpj` -- a branch, a check-digit correction, an ordinary `UPDATE` the seeded DDL
    permits -- is TWO ledger keys and ONE link hash key. The first becomes
    `absent_after_observation` while the second is `observed`, and the satellite writes an
    active row AND a closing row on the same `applied_date` for the same hash key: a
    relationship simultaneously open and closed, with `appended=3, closed=1` reading
    entirely ordinary in the run log. `changed_rows`' `F.lag` is partitioned only by hash
    key and ordered only by `applied_date`, so which of the two a reader gets is not even
    stable between runs.

    UNREACHABLE BY THIS REPOSITORY'S OWN DATA and asserted anyway:
    `scripts/merchant_population.py` derives `merchant_id = uuid5(NAMESPACE, cnpj)`, so a
    changed `cnpj` changes the surrogate too and `mutated()` never touches `cnpj` at all.
    The schema this phase models is an operational Postgres table where `cnpj` is a
    mutable `text` column, and the mechanism is what is on offer here.

    NOT M_REPOINTED, WHICH IS THE CASE THAT ALREADY WORKED: that merchant changes ROOT
    (`10000006` -> `10000007`), so its two ledger keys are two link hash keys and the
    close is real. Only a branch inside ONE root reaches this."""
    grain = ObservationGrain(
        name=LINK.name, bronze_table=branch_source.bronze,
        quarantine_table=branch_source.quarantine, key_columns=LINK_IDENTITY,
        key_prefixes=LINK_IDENTITY_PREFIXES, snapshot_axis=INSTANT_SNAPSHOT,
    )
    target = f"{branch_source.db}.eff_branch_{uuid4().hex[:8]}"

    result = load_effectivity_satellite(
        spark, EFF, link=LINK, hubs=LINK_HUBS, source_table=branch_source.bronze,
        target_table=target, load_date=LOADED_AT, grain=grain, months=[S1, S2],
    )
    rows = spark.read.table(target).orderBy("applied_date").collect()

    assert _link_key(spark, M_BRANCH_BEFORE) == _link_key(spark, M_BRANCH_AFTER), (
        "one root is one link hash key -- the premise the ledger has to be keyed on"
    )
    assert [row[IS_ACTIVE] for row in rows] == [True]
    assert result.closed == 0


def test_a_grain_that_drops_the_links_derivation_is_refused_before_spark(
    spark, branch_source
):
    """THE GUARD THAT COULD NOT SEE THIS, MADE ABLE TO. `_refuse_a_mismatched_link_grain`
    compared the grain's key COLUMNS against `identity_columns_of`'s, and both sides call
    the same function -- so it was satisfied by construction while the ledger sat one
    many-to-one map finer than the link. Names cannot carry a derivation;
    `identity_derivations_of` is the other half, and this grain names every right column
    and reads them raw.

    REFUSED RATHER THAN ANSWERED, and before a session: the load it would otherwise
    perform writes an active row and a closing row for one hash key on one
    `applied_date`, which no count in `EffectivityLoadResult` distinguishes from an
    ordinary load."""
    raw = ObservationGrain(
        name=LINK.name, bronze_table=branch_source.bronze,
        quarantine_table=branch_source.quarantine, key_columns=LINK_IDENTITY,
        snapshot_axis=INSTANT_SNAPSHOT,
    )

    with pytest.raises(ValueError, match="names agree and the VALUES would not"):
        load_effectivity_satellite(
            spark, EFF, link=LINK, hubs=LINK_HUBS, source_table=branch_source.bronze,
            target_table=f"{branch_source.db}.never", load_date=LOADED_AT, grain=raw,
            months=[S1, S2],
        )


def test_a_relationship_present_in_both_snapshots_writes_one_open_and_no_close(
    spark, merchant_loaded
):
    """Delta-driven, through the same `changed_rows` a descriptive satellite uses: a
    relationship that did not change state writes ONE row rather than two."""
    assert [row[IS_ACTIVE] for row in _effectivity_rows(spark, merchant_loaded, M_STAYS)] \
        == [True]


def test_the_effectivity_load_is_idempotent(spark, merchant_source, merchant_target):
    """A re-run finds every (key, applied_date) persisted, drops them before the window,
    and appends nothing -- including the closing rows, which are the rows a
    delete-then-append loader would duplicate."""
    names = load_merchant_vault(spark, merchant_source, merchant_target, months=[S1, S2])
    first = names.eff_result.appended

    again = load_effectivity_satellite(
        spark, EFF, link=LINK, hubs=LINK_HUBS, source_table=merchant_source.bronze,
        target_table=names.eff, load_date=LOADED_AT, grain=merchant_source.link_grain,
        months=[S1, S2],
    )

    assert first == 10
    assert again.appended == 0
    assert again.closed == 0


def test_the_monthly_axis_would_have_folded_both_snapshots_and_closed_nothing(
    spark, merchant_source, merchant_target
):
    """T7's defect, REPRODUCED, so the axis is a measured decision rather than a
    paragraph. Both instants are in 2026-08, so a ledger derived on `_snapshot_month`
    puts both observations in one row per key: every key is present in the only month it
    is asked about, no key can reach `absent_after_observation`, and the end-dating path
    has no producer at all. No error, no red test -- which is why this is asserted from
    the failing side."""
    monthly = ObservationGrain(
        name=LINK.name, bronze_table=merchant_source.bronze,
        quarantine_table=merchant_source.quarantine, key_columns=LINK_IDENTITY,
        key_prefixes=LINK_IDENTITY_PREFIXES,
    )
    load_merchant_vault(spark, merchant_source, merchant_target, months=[S1, S2])

    folded = load_effectivity_satellite(
        spark, EFF, link=LINK, hubs=LINK_HUBS, source_table=merchant_source.bronze,
        target_table=f"{merchant_source.db}.eff_monthly_{uuid4().hex[:8]}",
        load_date=LOADED_AT, grain=monthly, months=["2026-08"],
    )

    assert folded.closed == 0


# --- helpers -------------------------------------------------------------------------


def _hub_key(spark, key: tuple[str, str]) -> str:
    """`hub_merchant`'s digest for one merchant, computed through the shipped
    expression rather than restated."""
    frame = spark.createDataFrame([key], "merchant_id string, cnpj string")
    return frame.select(
        hash_key_for_end(LINK.ends[0], HUB).alias("k")
    ).first()["k"]


def _link_key(spark, key: tuple[str, str]) -> str:
    """`link_merchant_empresa`'s own digest for one (merchant, company) pair, through
    the shipped expression -- so a test cannot pass by agreeing with a restatement."""
    frame = spark.createDataFrame([key], "merchant_id string, cnpj string")
    return frame.select(
        link_hash_key_expression(LINK, identifying_hubs(LINK, LINK_HUBS)).alias("k")
    ).first()["k"]


def _satellite_rows_for(spark, loaded, key: tuple[str, str]) -> list:
    return (spark.read.table(loaded.dados)
            .where(f"{HUB.hash_key} = '{_hub_key(spark, key)}'")
            .orderBy("applied_date")
            .collect())


def _effectivity_rows(spark, loaded, key: tuple[str, str]) -> list:
    return (spark.read.table(loaded.eff)
            .where(f"{LINK.hash_key} = '{_link_key(spark, key)}'")
            .orderBy("applied_date")
            .collect())
