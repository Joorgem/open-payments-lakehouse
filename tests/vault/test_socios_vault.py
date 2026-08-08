"""`link_company_partner` and `sat_eff_company_partner`, over a fixture shaped like real
bronze -- and the reason there is no `hub_socio`.

HALF OF THE EVIDENCE, DELIBERATELY, exactly as the other two vault modules are: CI runs
local Spark with no credential, so this guards the MECHANIC and the measurement against
27.99M real rows lives in the task report.

WHAT THE FIXTURE MIRRORS -- the measured five-state distribution at LINK grain
(`01f191f3-ad7e-1edf-b0bd-063c4f1b7db6`):

| month | observed | siblings | rejected_by_our_gate | absent_before | absent_after |
|---|---|---|---|---|---|
| 2026-06 | 27,832,321 | 5 | 1,792 | 219,370 | 0 |
| 2026-07 | 27,986,258 | 5 | 1,781 | 0 | **65,444** |

Every number that is not a volume is reproduced below in miniature: one relationship
departs and is NOT in July's quarantine (the 65,444), one is in July's quarantine and
absent from July's bronze (the 1,781), one is born in July (the 219,370 seen from the
other end), and the rest are present in both.

THIS IS THE OTHER HALF OF TASK 2'S ACCEPTANCE PROOF AND IT DOES NOT PROVE THE
DISTINCTION ON ITS OWN. Socios supplies 65,444 true departures and NOT ONE departure
caused by our own gate; estabelecimentos supplies four departures that are ALL our
gate's and zero true ones. So a ledger that blamed the source for every disappearance
passes this file in full, and one that blamed our gate for every disappearance passes
`test_estabelecimento_vault.py` in full. The discrimination lives in
`test_observation.py::test_a_departure_reads_as_our_gate_on_one_table_and_as_the_sources_
on_the_other`, which is cross-table and already exists. What THIS file adds is that the
effectivity satellite ACTS on the distinction: one window closes and the other does not.

THE MASKED COLUMNS ARE `***` IN THE FIXTURE BECAUSE THEY ARE `***` ON A REAL READ.
Measured against live bronze (`01f192b4-a6f8-1849-9955-f321d9742180`): the Unity Catalog
column mask on `nome_socio_razao_social` and `nome_do_representante` applies to the table
owner too, and both come back as the literal three-character string. `cpf_cnpj_socio` is
NOT UC-masked -- its `***NNNNNN**` shape is the RFB's OWN masking, baked into the data --
which is what makes the link's business key computable and stable at all, and is a
distinction worth keeping straight: a grant change could alter the first and cannot
alter the second."""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pyspark.sql import functions as F

from opl.bronze.masking import MASKED_COLUMNS
from opl.contracts.cnpj_schemas import columns_for
from opl.vault import domains
from opl.vault.columns import (
    APPLIED_DATE,
    CLOSED_BY,
    IS_ACTIVE,
    LAST_OBSERVED_ON,
    LOAD_DATE,
    RECORD_SOURCE,
)
from opl.vault.domains.cnpj import UNMODELLED_SOCIOS_COLUMNS
from opl.vault.effectivity import CLOSING_STATE, load_effectivity_satellite
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
from opl.vault.partners import load_partner_link

from .conftest import (
    JUL,
    JUL_REF,
    JUN,
    JUN_REF,
    LOADED_AT,
    MAY,
    MAY_REF,
    RELOADED_AT,
    audit_values,
    bronze_schema,
    derived_table,
    quarantine_schema,
    write_delta,
)

LINK = domains.table_spec("link_company_partner")
EFF = domains.table_spec("sat_eff_company_partner")
EMPRESA_HUB = domains.table_spec("hub_empresa")
LINK_HUBS = domains.linked_hubs(LINK)
IDENTITY = domains.link_identity_columns(LINK)

COMPANY_REFERENCE = LINK.ends[0].reference_column(LINK_HUBS[0])
PARTNER_REFERENCE = LINK.ends[1].reference_column(LINK_HUBS[1])

OBSERVED = ObservationState.OBSERVED.value
REJECTED = ObservationState.REJECTED_BY_OUR_GATE.value
BEFORE = ObservationState.ABSENT_BEFORE_FIRST_OBSERVATION.value
AFTER = ObservationState.ABSENT_AFTER_OBSERVATION.value

# What a UC-masked column returns to every reader the mask function does not admit --
# the table owner included. Not a placeholder for a real name: this IS the read.
MASKED = "***"

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

_CONTRACT = tuple(columns_for("socios"))
_SCHEMA = bronze_schema("socios")

_DEFAULTS = {
    "nome_socio_razao_social": MASKED,
    "qualificacao_socio": "49",
    "pais": None,
    "representante_legal": "***000000**",
    "nome_do_representante": MASKED,
    "qualificacao_representante_legal": "00",
    "faixa_etaria": "5",
}


def _row(relationship: tuple[str, str, str | None], month: str, *, entrada: str,
         **overrides) -> tuple:
    """One bronze socios row: the whole contract plus every audit column the ingest
    stamps. `entrada` is `data_entrada_sociedade`, the window's DELIVERED open."""
    values = dict(_DEFAULTS)
    values.update(zip(IDENTITY, relationship, strict=True))
    values["data_entrada_sociedade"] = entrada
    values.update(overrides)
    return tuple(values[column] for column in _CONTRACT) + audit_values(
        month, source_file=f"/Volumes/x/cnpj/{month}/socios/K3241.K03200Y2.D60613.SOCIOCSV"
    )


def _bronze_rows() -> list[tuple]:
    """The fixture's bronze rows, meant to be read top to bottom -- the shape IS the
    argument."""
    return [
        _row(R_STAYS, JUN, entrada="20100101"),
        _row(R_STAYS, JUL, entrada="20100101"),
        # The 65,444 in miniature: gone from July's bronze and NOT in its quarantine.
        _row(R_DEPARTS, JUN, entrada="20110202"),
        # The 1,781 in miniature: gone from July's bronze because WE removed it.
        _row(R_REJECTED, JUN, entrada="20120303"),
        _row(R_NEW_IN_JULY, JUL, entrada="20260701"),
        _row(R_PJ, JUN, entrada="20140505"),
        _row(R_PJ, JUL, entrada="20140505"),
        # The PJ partner's own row, which is what puts `90000001` in hub_empresa --
        # measured: all 310,374 PJ partner roots are present in empresas, zero
        # unresolved (`01f19063-44ef-132a-8aa7-9068b624b370`).
        _row(R_PJ_PARENT, JUN, entrada="20130404"),
        _row(R_PJ_PARENT, JUL, entrada="20130404"),
        # Two foreign partners of one company. Same key -- (company, '3', NULL) -- in
        # both months, because neither carries one.
        _row(R_FOREIGN_A, JUN, entrada="20150606"),
        _row(R_FOREIGN_B, JUN, entrada="20160707"),
        _row(R_FOREIGN_A, JUL, entrada="20150606"),
        _row(R_FOREIGN_B, JUL, entrada="20160707"),
        # THE DEDUP CASE. Two June rows on one link key with two delivered entry dates;
        # the EARLIEST must win, and the fold must be counted.
        _row(R_TWINNED, JUN, entrada="20200101"),
        _row(R_TWINNED, JUN, entrada="20150301"),
        _row(R_TWINNED, JUL, entrada="20200101"),
        # The same masked CPF at ANOTHER company: a different relationship.
        _row(R_ELSEWHERE, JUN, entrada="20170808"),
        _row(R_ELSEWHERE, JUL, entrada="20170808"),
    ]


def _quarantine_rows() -> list[tuple]:
    """July's reject, with the gate's reason. Absent from July's bronze because of us."""
    return [(*_row(R_REJECTED, JUL, entrada="20120303", qualificacao_socio=""),
             "null_or_empty_qualificacao_socio")]


@pytest.fixture(scope="module")
def source(spark, vault_database):
    """A throwaway Delta database holding one bronze socios table and its quarantine,
    in the two months real bronze has."""
    db = vault_database("socios_vault")
    bronze, quarantine = f"{db}.socios", f"{db}.socios_q"

    write_delta(spark, bronze, _SCHEMA, _bronze_rows())
    write_delta(spark, quarantine, quarantine_schema("socios"), _quarantine_rows())

    grain = ObservationGrain(
        name=LINK.name, bronze_table=bronze, quarantine_table=quarantine,
        key_columns=IDENTITY,
    )
    return SimpleNamespace(db=db, bronze=bronze, quarantine=quarantine, grain=grain)


@pytest.fixture
def target(source):
    """Fresh table names per test, for the tests that WRITE -- sharing one would make
    idempotence pass for the wrong reason."""
    suffix = uuid4().hex[:8]
    return SimpleNamespace(
        hub=f"{source.db}.hub_{suffix}",
        link=f"{source.db}.link_{suffix}",
        eff=f"{source.db}.eff_{suffix}",
    )


def _load_all(spark, source, names, *, load_date=LOADED_AT, months=None, grain=None):
    """One load of the three tables, in dependency order, over `months`.

    `hub_empresa` is loaded FROM SOCIOS, which is the real design: socios carries
    `cnpj_basico`, so it is a third feed for the hub and the anti-join makes running
    every feed free. It is also what makes the PJ partner's reference assertable --
    `90000001` is in the hub because it appears as a company in its own right."""
    names.hub_result = load_hub(
        spark, EMPRESA_HUB, source_table=source.bronze, target_table=names.hub,
        load_date=load_date, months=months,
    )
    names.link_result = load_partner_link(
        spark, LINK, hubs=LINK_HUBS, source_table=source.bronze,
        target_table=names.link, load_date=load_date, months=months,
    )
    names.eff_result = load_effectivity_satellite(
        spark, EFF, link=LINK, hubs=LINK_HUBS, source_table=source.bronze,
        target_table=names.eff, load_date=load_date,
        grain=grain or source.grain, months=months,
    )
    return names


@pytest.fixture(scope="module")
def loaded(spark, source):
    """One load of every table over both months, shared by every read-only assertion."""
    names = SimpleNamespace(
        hub=f"{source.db}.hub_shared", link=f"{source.db}.link_shared",
        eff=f"{source.db}.eff_shared",
    )
    return _load_all(spark, source, names)


def _link_key(relationship: tuple[str, str, str | None]) -> str:
    """The link's own hash key for a relationship, computed by the PURE spelling of the
    standard -- so every assertion below ties the Spark expression to
    `opl.vault.hashing` at the value level rather than to itself."""
    company, kind, partner = relationship
    return hash_key([company.zfill(8), kind, partner])


def _link_rows(spark, names) -> dict[str, dict]:
    """`{link hash key: row}` for the whole link table."""
    return {row[LINK.hash_key]: row.asDict() for row in spark.read.table(names.link).collect()}


def _eff_rows(spark, names) -> dict[tuple[str, date], dict]:
    """`{(link hash key, applied_date): row}` for the whole effectivity satellite."""
    return {
        (row[LINK.hash_key], row[APPLIED_DATE]): row.asDict()
        for row in spark.read.table(names.eff).collect()
    }


def _applied(rows: dict, relationship: tuple[str, str, str | None]) -> list[date]:
    key = _link_key(relationship)
    return sorted(applied for stored, applied in rows if stored == key)


def _states(spark, grain) -> dict[tuple[tuple, str], str]:
    """`{(raw identity triple, month): observation_state}` for the whole ledger."""
    return {
        (tuple(row[column] for column in IDENTITY), row[MONTH_COLUMN]): row[STATE_COLUMN]
        for row in observation_ledger(spark, grain).collect()
    }


# --------------------------------------------------------------------------- #
# The acceptance test: the `absent` half of Task 2's
# --------------------------------------------------------------------------- #

def test_a_departure_closes_a_window_and_a_key_our_own_gate_removed_does_not(
    spark, source, loaded
):
    """THE ACCEPTANCE TEST, and the one that makes the ledger load-bearing rather than
    reported. Two relationships leave July's bronze for two different reasons. One is
    `absent_after_observation` -- the source stopped publishing it -- and its window
    closes. The other is `rejected_by_our_gate` -- it is in July's quarantine, so WE
    removed it -- and its window must stay open, because the source never said it ended.

    THE TWO HALVES ARE ASSERTED TOGETHER FOR THE SAME REASON TASK 4's ARE. The close
    alone passes on a satellite that closes on any absence; the non-close alone passes
    on one that never closes anything. Together they pin the gate ON THIS DATA -- and
    only there: this table has no departure caused by our gate and estabelecimentos has
    no true departure, so neither file alone can tell a correct ledger from one that
    blames every disappearance on the same cause. See the module docstring.

    `last_observed_on` IS NOT AN END DATE and the assertion says so: it is June's ref
    date, the last month we SAW the relationship, not a date the RFB ever published."""
    states = _states(spark, source.grain)
    rows = _eff_rows(spark, loaded)

    assert states[(R_DEPARTS, JUL)] == AFTER
    assert states[(R_REJECTED, JUL)] == REJECTED
    assert _applied(rows, R_DEPARTS) == [JUN_REF, JUL_REF]
    assert _applied(rows, R_REJECTED) == [JUN_REF]

    closing = rows[(_link_key(R_DEPARTS), JUL_REF)]
    assert closing[IS_ACTIVE] is False
    assert closing[LAST_OBSERVED_ON] == JUN_REF
    assert closing[CLOSED_BY] == CLOSING_STATE.value
    assert rows[(_link_key(R_REJECTED), JUN_REF)][IS_ACTIVE] is True
    assert loaded.eff_result.closed == 1


def test_a_relationship_born_in_july_is_absent_before_its_first_observation(spark, source):
    """The 219,370 in miniature. A key whose first appearance is July is absent in June,
    and calling that a candidate delete would have the ledger assert two hundred
    thousand false departures in the first month it covers."""
    states = _states(spark, source.grain)

    assert states[(R_NEW_IN_JULY, JUN)] == BEFORE
    assert states[(R_NEW_IN_JULY, JUL)] == OBSERVED


def test_a_relationship_present_in_both_months_writes_one_row_and_stays_active(
    spark, loaded
):
    """The control. An effectivity satellite is delta-driven on `is_active`, so a
    relationship that simply persists writes ONE row -- the assertion that fails on a
    loader writing a row per key per load, which is the record-tracking-satellite shape
    ADR 0010 rejected on cost."""
    rows = _eff_rows(spark, loaded)

    assert _applied(rows, R_STAYS) == [JUN_REF]
    assert rows[(_link_key(R_STAYS), JUN_REF)][IS_ACTIVE] is True
    assert rows[(_link_key(R_STAYS), JUN_REF)][LAST_OBSERVED_ON] is None
    assert rows[(_link_key(R_STAYS), JUN_REF)][CLOSED_BY] is None


def test_a_relationship_that_returns_opens_a_second_window(spark, source, target):
    """Three observations of one key: present, gone, back. The second window is what a
    close-only implementation cannot produce, and what "defined on the past only" is for
    -- the June row keeps saying `absent_after_observation` once August-shaped data
    arrives rather than being revised.

    THE THIRD MONTH IS SYNTHETIC, and this test is about a MECHANISM rather than about
    the RFB: our bronze holds two months, which structurally cannot show a return.

    `R_ELSEWHERE` IS IN ALL THREE MONTHS AND IS NOT DECORATION. The ledger's window is
    the months the DATA holds, so a table with only May and July rows has no June at
    all -- and that is correct, not a gap to work around: a month whose snapshot was
    never loaded is not evidence that anything departed in it. The second relationship
    is what makes June a loaded month, and therefore what makes the absence real."""
    table = f"{source.db}.returns_{uuid4().hex[:8]}"
    write_delta(spark, table, _SCHEMA, [
        _row(R_STAYS, MAY, entrada="20100101"), _row(R_STAYS, JUL, entrada="20100101"),
        *(_row(R_ELSEWHERE, month, entrada="20170808") for month in (MAY, JUN, JUL)),
    ])
    grain = ObservationGrain(
        name=LINK.name, bronze_table=table, quarantine_table=source.quarantine,
        key_columns=IDENTITY,
    )
    load_effectivity_satellite(
        spark, EFF, link=LINK, hubs=LINK_HUBS, source_table=table,
        target_table=target.eff, load_date=LOADED_AT, grain=grain,
    )
    rows = _eff_rows(spark, target)

    assert _applied(rows, R_STAYS) == [MAY_REF, JUN_REF, JUL_REF]
    assert [rows[(_link_key(R_STAYS), day)][IS_ACTIVE]
            for day in (MAY_REF, JUN_REF, JUL_REF)] == [True, False, True]
    assert rows[(_link_key(R_STAYS), JUN_REF)][LAST_OBSERVED_ON] == MAY_REF


# --------------------------------------------------------------------------- #
# No hub_socio: what the link's key is, and what the partner reference resolves to
# --------------------------------------------------------------------------- #

def test_the_link_key_is_the_company_key_and_the_two_dependent_child_keys_in_order(
    spark, loaded
):
    """The link's identity is exactly the measured business key of a partnership row --
    hub_empresa's padded `cnpj_basico`, then `identificador_socio`, then
    `cpf_cnpj_socio` -- and NOT the partner reference, which is a function of the third
    component rather than a part of the key.

    The three inequalities are the mutations that leave every join working and the table
    re-keyed: the components reversed, the company key alone (which would merge all of a
    company's partnerships into one row), and the digest a link including the derived
    partner root would produce."""
    rows = _link_rows(spark, loaded)
    company, kind, partner = R_PJ

    assert _link_key(R_PJ) in rows
    assert _link_key(R_PJ) != hash_key([partner, kind, company.zfill(8)])
    assert _link_key(R_PJ) != hash_key([company.zfill(8)])
    assert _link_key(R_PJ) != hash_key([company.zfill(8), PARTNER_ROOT, kind, partner])


def test_a_pj_partner_resolves_to_the_empresa_hub_and_no_one_else_resolves_at_all(
    spark, loaded
):
    """THE FINDING THAT REMOVED `hub_socio`. A corporate partner is an empresa already
    in the hub, so its reference is `hub_empresa`'s own digest over the eight-character
    root of `cpf_cnpj_socio` -- and it really joins, which is the half a computed digest
    does not give for free.

    EVERY OTHER PARTNER'S REFERENCE IS NULL, and that is the assertion that matters
    most. `hash_key_column` never returns NULL, so an unconditional derivation would
    give a PF partner the digest of `substring('***777777**', 1, 8)` -- a
    perfectly-shaped hash of garbage that joins to nothing and looks exactly like a
    company reference."""
    rows = _link_rows(spark, loaded)
    hub = {row["cnpj_basico"] for row in spark.read.table(loaded.hub).collect()}

    assert rows[_link_key(R_PJ)][PARTNER_REFERENCE] == hash_key([PARTNER_ROOT])
    assert PARTNER_ROOT in hub
    assert rows[_link_key(R_PJ)][PARTNER_REFERENCE] == rows[_link_key(R_PJ_PARENT)][
        COMPANY_REFERENCE
    ]
    for relationship in (R_STAYS, R_TWINNED, R_FOREIGN_A):
        assert rows[_link_key(relationship)][PARTNER_REFERENCE] is None


def test_every_company_reference_is_a_digest_the_hub_holds(spark, loaded):
    """A reference that is not its hub's digest joins to nothing SILENTLY -- an empty
    result rather than an error. Set containment rather than equality, because the hub
    also holds companies that are only ever partners' parents."""
    link = spark.read.table(loaded.link).collect()
    hub = {row[EMPRESA_HUB.hash_key] for row in spark.read.table(loaded.hub).collect()}

    assert {row[COMPANY_REFERENCE] for row in link} <= hub
    assert {row[PARTNER_REFERENCE] for row in link if row[PARTNER_REFERENCE]} <= hub


def test_one_masked_cpf_at_two_companies_is_two_relationships(spark, loaded):
    """The PF key space is 10^6 and 999,853 of it is occupied, so the same masked CPF
    belongs to many unrelated people -- which is precisely why it is a dependent-child
    key of the RELATIONSHIP and not a hub. Two companies sharing it are two link rows,
    because the company is part of the key."""
    rows = _link_rows(spark, loaded)

    assert R_TWINNED[2] == R_ELSEWHERE[2]
    assert _link_key(R_TWINNED) != _link_key(R_ELSEWHERE)
    assert {_link_key(R_TWINNED), _link_key(R_ELSEWHERE)} <= set(rows)


def test_two_partners_of_one_company_sharing_a_masked_cpf_collapse_and_are_counted(
    spark, loaded
):
    """THE COST OF THE DECISION, ASSERTED RATHER THAN CONCEDED IN PROSE. Inside ONE
    company the masked CPF is all we have, so two rows carrying it are one link row --
    two people merged into one relationship, and there is no repair: the obvious one is
    to add the name, and the name reads `***`.

    What makes it a decision rather than a defect is that it is COUNTED. 4,329 of
    2026-07's 27,990,592 rows are measured collisions on this key; a silent `DISTINCT`
    would have folded them with nothing said."""
    rows = _link_rows(spark, loaded)

    assert len([key for key in rows if key == _link_key(R_TWINNED)]) == 1
    assert loaded.link_result.collapsed_duplicates == 3
    assert loaded.eff_result.collapsed_duplicates == 3


def test_a_foreign_partners_null_key_never_becomes_a_real_key(spark, loaded):
    """12,824 foreign partners carry `cpf_cnpj_socio` NULL -- no business key at all.
    They are ADMITTED rather than excluded, under the hash standard's NULL token, and
    the consequences are asserted in both directions: the relationship is in the link
    (so the link's key universe matches the ledger's, which is what the effectivity
    satellite gates on), its partner reference is NULL rather than a digest of nothing,
    and one company's several foreign partners are ONE row.

    The inequality is the mutation that matters: a loader treating NULL as the empty
    string, or as the literal 'None', would produce a key that collides with a real
    one."""
    rows = _link_rows(spark, loaded)
    key = _link_key(R_FOREIGN_A)

    assert R_FOREIGN_A == R_FOREIGN_B
    assert key in rows
    assert rows[key][PARTNER_REFERENCE] is None
    assert rows[key]["cpf_cnpj_socio"] is None
    assert key != hash_key([R_FOREIGN_A[0].zfill(8), R_FOREIGN_A[1], ""])
    assert key != hash_key([R_FOREIGN_A[0].zfill(8), R_FOREIGN_A[1], "None"])


def test_the_link_carries_two_references_two_dependent_child_keys_and_the_metadata(
    spark, loaded
):
    """Pinned as a list, in write order, so a column arriving or leaving is a deliberate
    edit. No payload and no `applied_date`: a link row asserts the relationship was
    seen, and WHEN it held is the effectivity satellite's statement."""
    assert spark.read.table(loaded.link).columns == [
        LINK.hash_key, COMPANY_REFERENCE, PARTNER_REFERENCE,
        "identificador_socio", "cpf_cnpj_socio", LOAD_DATE, RECORD_SOURCE,
    ]
    assert len(spark.read.table(loaded.link).collect()) == 9


# --------------------------------------------------------------------------- #
# The effectivity satellite: a delivered open, a derived close
# --------------------------------------------------------------------------- #

def test_the_satellite_keeps_the_delivered_open_under_the_sources_own_name(spark, loaded):
    """THE EPISTEMICS ARE IN THE COLUMN LIST. `data_entrada_sociedade` is the RFB's
    column name and the RFB's value, carried verbatim; `is_active`, `last_observed_on`
    and `closed_by` are ours and are named in our vocabulary. A reader cannot mistake
    the inferred close for the delivered open without ignoring both.

    Pinned as a list for `_in_column_order`'s reason: `mode("append")` matches by
    position, so a reordering would write our inference into the source's column."""
    assert spark.read.table(loaded.eff).columns == [
        LINK.hash_key, LOAD_DATE, APPLIED_DATE, RECORD_SOURCE, IS_ACTIVE,
        EFF.entry_column, LAST_OBSERVED_ON, CLOSED_BY,
    ]
    rows = _eff_rows(spark, loaded)
    assert rows[(_link_key(R_STAYS), JUN_REF)][EFF.entry_column] == "20100101"
    assert len(rows) == 10


def test_the_closing_row_carries_the_open_the_last_observed_month_delivered(spark, loaded):
    """A closing row has no source row -- that is what makes it a close -- so its
    delivered open and its `record_source` are carried forward from the last month we
    DID observe. Leaving them NULL would blank the window's open on the one row where a
    reader most needs both ends of it."""
    closing = _eff_rows(spark, loaded)[(_link_key(R_DEPARTS), JUL_REF)]

    assert closing[EFF.entry_column] == "20110202"
    assert closing[RECORD_SOURCE] == "rfb_cnpj_webdav"


def test_the_dedup_rule_keeps_the_earliest_delivered_entry_date(spark, loaded):
    """THE RULE, ASSERTED ON THE VALUE. Two June rows for one link key deliver
    `20200101` and `20150301`; the satellite must carry the EARLIER one. Deterministic,
    order-independent, and -- unlike a lowest-`hash_diff` tie-break -- not arbitrary:
    the open of a window is the earliest moment the source claims it began.

    The inequality is what a `first()` or a `max` would produce, either of which
    satisfies "one row per key per month" perfectly."""
    row = _eff_rows(spark, loaded)[(_link_key(R_TWINNED), JUN_REF)]

    assert row[EFF.entry_column] == "20150301"
    assert row[EFF.entry_column] != "20200101"


def test_reloading_the_hub_the_link_and_the_satellite_appends_nothing(spark, source, target):
    """Idempotence across all three, with a different `load_date` on the second run so a
    row silently rewritten rather than skipped shows up as a changed stamp even where
    the count happens to hold."""
    first = {"hub": 7, "link": 9, "eff": 10}
    _load_all(spark, source, target)
    appended = {name: getattr(target, f"{name}_result").appended for name in first}
    _load_all(spark, source, target, load_date=RELOADED_AT)
    again = {name: getattr(target, f"{name}_result").appended for name in first}

    assert appended == first
    assert again == dict.fromkeys(first, 0)
    for name, count in first.items():
        rows = spark.read.table(getattr(target, name)).collect()
        assert len(rows) == count
        assert {row[LOAD_DATE] for row in rows} == {LOADED_AT}


def test_loading_july_after_june_adds_only_what_july_changed(spark, source, target):
    """The incremental path, which is where a satellite seeded only by its own batch
    goes wrong. June writes eight active rows; July adds the relationship born in it and
    the close for the one that departed, and rewrites nothing that persisted."""
    june = _load_all(spark, source, target, months=[JUN])
    counts = (june.link_result.appended, june.eff_result.appended, june.eff_result.closed)
    both = _load_all(spark, source, target, load_date=RELOADED_AT, months=[JUN, JUL])

    assert counts == (8, 8, 0)
    assert (both.link_result.appended, both.eff_result.appended) == (1, 2)
    assert both.eff_result.closed == 1


# --------------------------------------------------------------------------- #
# The mask, the contract, and the refusals
# --------------------------------------------------------------------------- #

def test_no_column_unity_catalog_masks_is_read_into_the_vault(spark, loaded):
    """WHAT THE VAULT LOAD ACTUALLY SEES. The UC mask on socios applies on read to the
    table owner too, so `nome_socio_razao_social` and `nome_do_representante` return the
    literal `***` -- measured against live bronze, not assumed. This test closes the
    loop in both directions: the two columns bronze masks are exactly the two this
    domain declares unmodelled, and no vault table built from a fixture where they hold
    `***` contains `***` anywhere.

    The second half is what a declaration alone cannot give. A satellite payload
    carrying either column would store `***` on every row and its `hash_diff` would be
    constant -- change detection that can never fire, with no error attached."""
    masked = set(MASKED_COLUMNS["socios"])

    assert masked <= set(UNMODELLED_SOCIOS_COLUMNS)
    assert masked.isdisjoint(IDENTITY)
    assert EFF.entry_column not in masked
    for table in (loaded.link, loaded.eff):
        frame = spark.read.table(table)
        strings = [name for name, kind in frame.dtypes if kind == "string"]
        found = frame.filter(
            F.greatest(*(F.col(name) == F.lit(MASKED) for name in strings))
        )
        assert found.count() == 0


def test_the_link_the_satellite_and_the_declared_omissions_partition_the_contract():
    """Every one of the socios contract's eleven columns is part of the link's identity,
    the effectivity satellite's delivered open, or a declared omission -- and the three
    sets are disjoint.

    A column absent from all three is indistinguishable from one someone forgot, which
    is why the omissions are a declared tuple with a reason beside each. Pure -- no
    Spark -- so a contract column added upstream turns it red in milliseconds."""
    contract = set(_CONTRACT)
    identity, omitted = set(IDENTITY), set(UNMODELLED_SOCIOS_COLUMNS)
    entry = {EFF.entry_column}

    assert identity & omitted == set()
    assert entry & (identity | omitted) == set()
    assert identity | entry | omitted == contract
    assert len(identity) + len(entry) + len(omitted) == len(_CONTRACT)


def test_the_generic_link_loader_refuses_the_link_it_cannot_write(spark, source, target):
    """`load_link` writes one reference per hub from the columns that hub is named
    after, and this link's partner end is derived. Left unrefused it would not crash --
    `link_candidates` would compute a partner reference from `cnpj_basico`, giving every
    partner the COMPANY's digest, so every relationship would appear to be a company
    partnered with itself."""
    with pytest.raises(ValueError, match="dependent-child keys or a non-identifying"):
        load_link(
            spark, LINK, hubs=LINK_HUBS, source_table=source.bronze,
            target_table=target.link, load_date=LOADED_AT,
        )

    assert not spark.catalog.tableExists(target.link)


def test_the_partner_loader_refuses_a_link_whose_dependent_child_keys_it_cannot_read(
    spark, source, target
):
    """The loader derives the partner root from `cpf_cnpj_socio` BY NAME, so a link
    declaring other dependent-child keys would load, be idempotent, join -- and leave
    every partner reference NULL. The refusal is what makes the coupling visible."""
    from opl.vault.registry import BusinessKeyColumn, Link

    renamed = Link(
        name=LINK.name, hash_key=LINK.hash_key, hubs=LINK.hubs,
        dependent_child_keys=(BusinessKeyColumn(name="qualificacao_socio"),
                              BusinessKeyColumn(name="faixa_etaria")),
    )

    with pytest.raises(ValueError, match="derives the partner"):
        load_partner_link(
            spark, renamed, hubs=LINK_HUBS, source_table=source.bronze,
            target_table=target.link, load_date=LOADED_AT,
        )


def test_a_grain_at_hub_grain_is_refused_by_the_effectivity_loader(spark, source, target):
    """THE MISTAKE THAT WOULD FAIL SILENTLY AND FOREVER. A partner who loses one of two
    partnerships is `absent_after_observation` at LINK grain and plainly `observed` at
    hub grain, so a hub-grain ledger reports no departure and every window stays open --
    a satellite that answers, writes rows, and never closes anything."""
    coarser = ObservationGrain(
        name="hub_empresa", bronze_table=source.bronze,
        quarantine_table=source.quarantine, key_columns=("cnpj_basico",),
    )

    with pytest.raises(ValueError, match="keyed on the LINK's identity"):
        load_effectivity_satellite(
            spark, EFF, link=LINK, hubs=LINK_HUBS, source_table=source.bronze,
            target_table=target.eff, load_date=LOADED_AT, grain=coarser,
        )

    assert not spark.catalog.tableExists(target.eff)


def test_a_typed_key_column_is_refused_by_both_loaders_before_they_hash(
    spark, source, target
):
    """Spark casts a typed column silently and hashes the CAST, where `hash_key` raises
    on a value with no `.strip()`: one spelling answers and the other refuses, which no
    equivalence test over string corpora can see. Both loaders are checked, because each
    projects the source itself."""
    typed = derived_table(
        spark, source.db, "typed",
        spark.read.table(source.bronze).withColumn(
            "identificador_socio", F.col("identificador_socio").cast("int")
        ),
    )
    grain = ObservationGrain(
        name=LINK.name, bronze_table=typed, quarantine_table=source.quarantine,
        key_columns=IDENTITY,
    )

    for load in (
        lambda: load_partner_link(
            spark, LINK, hubs=LINK_HUBS, source_table=typed, target_table=target.link,
            load_date=LOADED_AT),
        lambda: load_effectivity_satellite(
            spark, EFF, link=LINK, hubs=LINK_HUBS, source_table=typed,
            target_table=target.eff, load_date=LOADED_AT, grain=grain),
    ):
        with pytest.raises(TypeError, match="identificador_socio"):
            load()
