"""The gold registry's guards, and the one collision no other layer can see.

WHY GOLD HAS A REGISTRY OF ITS OWN RATHER THAN A ROW IN THE VAULT'S. The vault's
totality lock (`tests/test_vault_job_wiring.py::test_every_registered_vault_table_is
_loaded_by_exactly_one_task`) reads `set(opl.vault.domains.REGISTRY)` and demands that
every name in it be loaded by exactly one task of a `vault_*.yml`. A dimension
registered there would turn that lock red on the day it was declared, and the repair
would be an edit to a file sitting at exactly 800 lines. The seam is real besides: a
`Scd2Dimension` is not a `VaultTable`, no vault loader can write one, and
`_entry_point_for` would assert on its kind.

THE ONE GUARD THAT EXISTS ONLY BECAUSE THIS IS THE THIRD LAYER. Free Edition ships one
catalog and one schema (`opl.config`: `workspace.default`), so bronze, the vault and
gold all write their Delta tables into ONE namespace holding thirty-odd tables. Gold is
the first artefact that can collide across a layer boundary, and the collision is silent
in the worst way: `mode("append")` into a name another layer owns does not fail, it
appends rows of one shape into a table of another, or -- when the shapes agree by
accident -- quietly merges two populations. Neither the bronze registry nor the vault
registry can see it, because neither imports the other.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from opl.bronze.registry import REGISTRY as BRONZE_REGISTRY
from opl.contracts import payments
from opl.gold.columns import IS_CURRENT, VALID_FROM, VALID_TO
from opl.gold.registry import (
    DIM_CHANNEL,
    DIM_COMPANY,
    DIM_CURRENCY,
    DIM_DATE,
    FACT_PAYMENT,
    PIT_ESTABELECIMENTO,
    REGISTRY,
    CalendarDimension,
    EnumeratedDimension,
    PaymentFact,
    PointInTimeTable,
    Scd2Dimension,
    UnknownGoldTable,
    build_registry,
    table_spec,
)
from opl.vault import domains

_GOLD = Path(__file__).resolve().parents[2] / "src" / "opl" / "gold"
_MODULE = _GOLD / "registry.py"

# The two modules that refuse at import, and what each is asked to prove. `specs.py`
# holds the per-table guards (a `__post_init__` calls them) and `registry.py` the
# whole-set ones (`build_registry` calls them, and this module's own foot calls that) --
# so only one of the two can be asked whether the registry is built at import, and both
# must be asked whether every guard they define is reached at all.
_GUARD_MODULES = {"registry.py": True, "specs.py": False}


def _dimension(**overrides) -> Scd2Dimension:
    """A well-formed dimension, with one field replaced -- so every refusal below is
    reached by changing exactly the thing it refuses."""
    fields = {
        "name": "dim_probe",
        "surrogate_key": "probe_sk",
        "source_satellite": "sat_empresa_dados",
    }
    fields.update(overrides)
    return Scd2Dimension(**fields)


def _enumerated(**overrides) -> EnumeratedDimension:
    """A well-formed enumerated dimension, same shape and same purpose as `_dimension`."""
    fields = {
        "name": "dim_probe_enum",
        "surrogate_key": "probe_enum_key",
        "natural_key": "probe_code",
        "fact_column": payments.EVENT_TIME_COLUMN,
        "members": ("A", "B"),
    }
    fields.update(overrides)
    return EnumeratedDimension(**fields)


def _calendar(**overrides) -> CalendarDimension:
    fields = {
        "name": "dim_probe_date",
        "surrogate_key": "probe_date_key",
        "natural_key": "probe_full_date",
        "fact_column": payments.EVENT_TIME_COLUMN,
        "applied_date_source": "sat_empresa_dados",
        "roles": ("probe_event_date_key",),
    }
    fields.update(overrides)
    return CalendarDimension(**fields)


def _pit(**overrides) -> PointInTimeTable:
    """A well-formed point-in-time table, same shape and purpose as `_dimension`.

    ITS HUB AND SATELLITES ARE THE REAL ONES, because `build_registry` resolves them
    against the live vault registry -- a probe pointing at invented names would be refused
    by the PIT guard before the cross-layer guard under test could fire, and the sweep
    would then be measuring the wrong refusal."""
    fields = {
        "name": "pit_probe",
        "hub": "hub_estabelecimento",
        "satellites": ("sat_estabelecimento_dados", "sat_estabelecimento_endereco"),
        "as_of_column": "probe_as_of",
    }
    fields.update(overrides)
    return PointInTimeTable(**fields)


def _fact(**overrides) -> PaymentFact:
    """A well-formed payment fact, same shape and purpose as `_dimension`.

    ITS `company_dimension` AND `conformed` NAME THE REAL TABLES, for `_pit`'s reason: the
    whole-set guards resolve both against this registry, so a probe pointing at invented
    names would be refused by the fact guard before the cross-layer guard under test could
    fire, and the sweep would then be measuring the wrong refusal. The sweeps below hand
    `build_registry` a single spec, and the cross-layer and duplicate-name guards run
    FIRST -- which is the "individually wrong before collectively wrong" order
    `opl.bronze.registry` states and which is what keeps this factory usable there."""
    fields = {
        "name": "fact_probe",
        "grain_key": payments.IDENTITY_COLUMN,
        "measure": "amount",
        "company_dimension": "dim_company",
        "roles": (
            ("payer_cnpj_basico", "probe_payer_sk"),
            ("payee_cnpj_basico", "probe_payee_sk"),
        ),
        "conformed": ("dim_date", "dim_channel", "dim_currency"),
    }
    fields.update(overrides)
    return PaymentFact(**fields)


# Every kind the gold registry knows, with a factory that builds a well-formed one. The
# cross-layer guards below are parametrised over this rather than over `Scd2Dimension`
# alone: a name collision is a property of the NAME, so a guard that only saw one kind
# would be a guard the second kind walks past.
#
# `pit` IS THE KIND THAT PROVED THE ARGUMENT RATHER THAN ILLUSTRATING IT. It is the first
# entry here with no `surrogate_key` and no `fact_column`, and adding it turned
# `_assert_no_two_dimensions_draw_from_one_payment_column` into an `AttributeError` at
# import of every gold module -- that guard skipped `Scd2Dimension` and assumed everything
# else had a `fact_column`. A sweep parametrised over one kind would not have found it.
#
# `fact` IS THE FIFTH AND IT IS IN HERE FOR THE SAME REASON, CHECKED THE SAME WAY. It has
# no `fact_column` either, so it walks the same line the PIT broke; the difference is that
# this time the direction was checked before the kind was added rather than after, and the
# guard was already an inclusion.
KINDS = {
    "scd2": _dimension,
    "enumerated": _enumerated,
    "calendar": _calendar,
    "pit": _pit,
    "fact": _fact,
}

# HOW THE COLLIDING NAME IS SPELLED, and the second entry is the whole point. Unity
# Catalog and Spark resolve identifiers CASE-INSENSITIVELY, so `SAT_EMPRESA_DADOS` and
# `sat_empresa_dados` are ONE Delta table -- and the sweeps below iterate the registries'
# own strings, which are all lower case, so a byte comparison passes every one of them
# while the upper-cased spelling of the same table walks straight through. Measured
# before the guard was casefolded: `sat_empresa_dados` refused, `SAT_EMPRESA_DADOS`
# ACCEPTED, same for `ref_pais` and `bronze_cnpj_empresas`. It is the exact defect
# `opl.bronze.registry_collisions` fixed in F1.4b, reintroduced at the one boundary no
# other file polices.
SPELLINGS = {"as declared": str, "upper-cased": str.upper}


def test_the_registered_star_is_one_fact_four_dimensions_and_one_pit():
    """The pin. `dim_company` is derived from ONE satellite and that satellite's name is
    the whole of what the loader is told -- the payload, the parent hub and the business
    key are all read from the vault registry rather than restated here, so a second
    spelling of any of them cannot exist to drift.

    SIX ENTRIES AND ONLY FIVE OF THEM ARE IN THE STAR. `pit_estabelecimento` is a
    navigation table nothing in this star joins to: `dim_company` is at empresa grain,
    `dim_merchant` at estabelecimento grain is deferred until payments carry a 14-digit
    CNPJ, and `dim_geography` is skipped. It is registered because it is BUILT, and the
    registry's job is to hold every gold table this repository writes -- but it is named
    apart here so nobody reads six and counts a six-table star."""
    assert set(REGISTRY) == {
        "dim_company", "dim_date", "dim_channel", "dim_currency", "pit_estabelecimento",
        "fact_payment",
    }
    spec = table_spec("dim_company")
    assert (spec.surrogate_key, spec.source_satellite) == ("company_sk", "sat_empresa_dados")


def test_dim_channel_is_drawn_from_payment_method_and_never_from_the_drift_column():
    """T-C, and it reads `DRIFT_COLUMNS` rather than spelling `payment_channel` again.

    A channel dimension is the single most plausible reason anybody would ever declare
    `payment_channel` in the payment contract, and declaring it destroys F1b's whole
    drift class in three silent ways that `opl.contracts.payments` enumerates: the
    serialiser would emit it on every record, an Auto Loader read schema built from the
    contract would absorb it instead of rescuing it, and a DISTINCT over the business
    attributes would drop every pre-drift row. The dimension is therefore drawn from
    `payment_method`, which every record carries, and the registry refuses a drift
    column at import."""
    assert DIM_CHANNEL.fact_column == "payment_method"
    assert DIM_CHANNEL.fact_column not in payments.DRIFT_COLUMNS
    assert DIM_CHANNEL.members == payments.PAYMENT_METHODS
    for dimension in (DIM_DATE, DIM_CHANNEL, DIM_CURRENCY):
        assert dimension.fact_column not in payments.DRIFT_COLUMNS


@pytest.mark.parametrize("drifting", payments.DRIFT_COLUMNS)
def test_a_conformed_dimension_drawn_from_a_drift_column_is_refused_at_import(drifting):
    """The guard behind the assertion above, driven through every declared drift column
    rather than through the one that exists today."""
    with pytest.raises(ValueError, match="is a drift column"):
        _enumerated(fact_column=drifting)
    with pytest.raises(ValueError, match="is a drift column"):
        _calendar(fact_column=drifting)


def test_a_conformed_dimension_drawn_from_a_column_the_contract_does_not_declare():
    """The other half of the drift refusal, and the reason gold may spell a contract
    column name at all: a name v1 does not carry is refused at import, so the copy in
    `opl.gold.registry` cannot go stale behind a rename."""
    with pytest.raises(ValueError, match="no column the payment contract declares"):
        _enumerated(fact_column="payment_methd")


def test_dim_date_has_exactly_one_business_date_role():
    """T-B. The governing spec asks for role-playing across transação / autorização /
    liquidação -- three roles. The payment contract carries `event_time` and
    `emitted_at`, and `emitted_at` is a DELIVERY fact, not a business date: it is when
    the generator released the record, which is a property of the delivery that may
    repeat, not of the payment. So there is ONE business date role and the other two are
    missing rather than manufactured.

    WHAT WOULD ADD A SECOND: an `authorized_at` or a `settled_at` column in
    `opl.contracts.payments`, which is a change to the GENERATOR's contract (and to the
    schema version it declares), not to this layer."""
    assert DIM_DATE.roles == ("event_date_key",)
    assert len(DIM_DATE.roles) == 1
    assert DIM_DATE.fact_column == payments.EVENT_TIME_COLUMN
    assert payments.EMITTED_AT_COLUMN not in DIM_DATE.roles


def test_table_spec_refuses_an_unknown_name_and_names_the_alternatives():
    """Refused BEFORE Spark, like `opl.bronze.registry.table_spec` and
    `opl.vault.registry.table_spec`: an operator who mistyped a table should not wait for
    a serverless session to be told so, and the message has to carry the valid list or it
    sends them to the source."""
    with pytest.raises(UnknownGoldTable, match="dim_company"):
        table_spec("dim_compnay")


def test_a_dimension_whose_source_satellite_no_domain_registers_is_refused():
    with pytest.raises(ValueError, match="which no vault domain registers"):
        build_registry((_dimension(source_satellite="sat_empresa_dado"),))


def test_a_dimension_whose_source_is_registered_but_is_not_a_satellite_is_refused():
    """`hub_empresa` is a registered vault table and it is not a satellite. The loader
    reads `payload_columns` and resolves a parent hub, so this pairing would die inside
    Spark's analysis naming a dataclass field rather than a table."""
    with pytest.raises(ValueError, match="is not a satellite"):
        build_registry((_dimension(source_satellite="hub_empresa"),))


@pytest.mark.parametrize("column", [VALID_FROM, VALID_TO, IS_CURRENT, "load_date"])
def test_a_surrogate_key_that_is_a_column_the_loader_writes_is_refused(column):
    """The collision is quiet rather than fatal: the loader writes both, the projection
    keeps one value, and the column is still there full of plausible timestamps."""
    with pytest.raises(ValueError, match="the loader writes"):
        _dimension(surrogate_key=column)


def test_a_surrogate_key_that_is_a_payload_column_of_its_source_is_refused():
    """Needs the SOURCE to be resolved, which is why it is a whole-set guard and not a
    `__post_init__` check: `razao_social` is a legal column name until you know which
    satellite this dimension reads."""
    with pytest.raises(ValueError, match="is a payload column of"):
        build_registry((_dimension(surrogate_key="razao_social"),))


def test_a_source_column_colliding_with_a_column_the_loader_writes_is_refused():
    """The other direction, and it is the one the vault cannot refuse for us:
    `opl.vault.specs._validated_columns` checks a payload against the VAULT's four
    metadata names and knows nothing about `valid_from`. A satellite payload column of
    that name is legal in the vault and would be overwritten here."""
    satellite = domains.table_spec("sat_empresa_dados")
    collided = type(satellite)(
        name=satellite.name, parent=satellite.parent, payload_columns=(VALID_FROM, "porte_empresa")
    )
    with pytest.raises(ValueError, match="the loader writes that itself"):
        build_registry((_dimension(),), vault_tables={satellite.name: collided})


@pytest.mark.parametrize("spelling", sorted(SPELLINGS))
@pytest.mark.parametrize("kind", sorted(KINDS))
@pytest.mark.parametrize("vault_table", sorted(domains.REGISTRY))
def test_a_gold_table_named_like_a_vault_table_is_refused(vault_table, kind, spelling):
    """ONE FLAT SCHEMA. `opl.config.OplConfig.table` puts every layer's Delta tables in
    `workspace.default`, so a gold name equal to a vault name is one `mode("append")`
    away from writing dimension rows into a satellite.

    OVER EVERY KIND (T-E), because the collision is a property of the NAME and of the
    flat namespace, not of what the table holds. A static dimension is the kind most
    likely to acquire a short, generic name -- `dim_date` is one word away from a lookup
    somebody adds later -- and a guard that had only ever been driven through
    `Scd2Dimension` would be a guard the new kind walks past with the suite still
    green.

    OVER EVERY SPELLING, because until this parameter existed the sweep inherited the
    guard's own blind spot: it fed the guard the registry's own lower-case strings, which
    a byte comparison catches, and never the upper-cased spelling of the same physical
    table, which it did not. See `SPELLINGS`."""
    with pytest.raises(ValueError, match="already owned by the vault"):
        build_registry((KINDS[kind](name=SPELLINGS[spelling](vault_table)),))


@pytest.mark.parametrize("spelling", sorted(SPELLINGS))
@pytest.mark.parametrize("kind", sorted(KINDS))
@pytest.mark.parametrize("bronze_table", sorted(BRONZE_REGISTRY))
def test_a_gold_table_named_like_any_of_a_bronze_tables_three_delta_names_is_refused(
    bronze_table, kind, spelling
):
    """All THREE names, not just the bronze one. A promote appends into staging and the
    DQ gate appends into quarantine, so a dimension sitting on either is reached by a
    job nobody would think to look at -- and the quarantine is the documented case
    (`opl.bronze.registry`: a quarantine name hardcoded in a job YAML "sent estab
    triagers to a table full of unrelated F1.2 lookup rows").

    Over every spelling for the test above's reason."""
    spec = BRONZE_REGISTRY[bronze_table]
    for name in (spec.staging, spec.bronze, spec.quarantine):
        with pytest.raises(ValueError, match="already owned by bronze"):
            build_registry((KINDS[kind](name=SPELLINGS[spelling](name)),))


@pytest.mark.parametrize("spelling", sorted(SPELLINGS))
@pytest.mark.parametrize("kind", sorted(KINDS))
def test_two_gold_tables_of_any_kind_claiming_one_name_are_refused(kind, spelling):
    """One Delta name, two specs: one of them would load into the other's table with both
    runs reporting success. Over every kind for the two tests above's reason, and over
    every spelling because two gold tables differing only in case are also one table.

    THE TWO SPECS DIFFER ONLY IN THE SPELLING OF THE NAME, and they used to differ in
    their surrogate key as well -- which made this sweep unable to reach a kind that has
    no surrogate key. What the guard refuses is the NAME being claimed twice, so the
    second key was never part of the subject."""
    make = KINDS[kind]
    with pytest.raises(ValueError, match="both declare a gold table"):
        build_registry((make(), make(name=SPELLINGS[spelling](make().name))))


def test_a_refusal_over_two_spellings_of_one_name_reports_both_of_them():
    """THE MESSAGE, and it is why the guard cannot simply casefold in silence. The two
    strings it reports are not equal, so an operator handed only the normalised one would
    search the source for a name nobody wrote. `opl.bronze.registry_collisions
    ._delta_name_collision` draws the same distinction and adds the case explanation ONLY
    when the spellings differ -- a plain duplicate annotated with one sends the reader
    looking for a difference that is not there."""
    vault_table = sorted(domains.REGISTRY)[0]
    with pytest.raises(ValueError, match="CASE-INSENSITIVE") as refused:
        build_registry((_dimension(name=vault_table.upper()),))
    assert vault_table in str(refused.value) and vault_table.upper() in str(refused.value)
    with pytest.raises(ValueError) as plain:
        build_registry((_dimension(name=vault_table),))
    assert "CASE-INSENSITIVE" not in str(plain.value), (
        "an ordinary duplicate carries a case explanation, which sends the operator "
        "looking for a difference that is not there"
    )


def test_two_conformed_dimensions_drawing_from_one_fact_column_are_refused():
    """CONFORMANCE, made checkable. "Conformed" means one dimension answers one question
    for every fact that asks it; two dimensions over `payment_method` are two answers,
    and the fact would carry two keys resolving to the same five members. Nothing about
    it fails -- both build, both are well-formed, and a report joining one and a report
    joining the other agree until the day their member sets do not."""
    with pytest.raises(ValueError, match="both draw from the payment column"):
        build_registry(
            (
                _enumerated(name="dim_probe_a", surrogate_key="a_key"),
                _enumerated(name="dim_probe_b", surrogate_key="b_key"),
            )
        )


def test_a_calendar_dimension_whose_applied_date_source_is_not_a_satellite_is_refused():
    """`dim_date`'s span reaches back to the earliest `applied_date` in the vault, so it
    names the satellite it reads those from -- and that name is resolved against the vault
    registry exactly as an SCD2 dimension's is. It is a DIFFERENT field from
    `Scd2Dimension.source_satellite` on purpose: a calendar dimension reads that table's
    date column and none of its payload, and one name for two relationships is how
    `gold_load_dimension` would get far enough into building a dimension out of the wrong
    kind to write rows."""
    with pytest.raises(ValueError, match="which no vault domain registers"):
        build_registry((_calendar(applied_date_source="sat_empresa_dado"),))
    with pytest.raises(ValueError, match="is not a satellite"):
        build_registry((_calendar(applied_date_source="hub_empresa"),))


@pytest.mark.parametrize("kind", ["enumerated", "calendar"])
@pytest.mark.parametrize("column", [VALID_FROM, VALID_TO, IS_CURRENT, "load_date"])
def test_a_conformed_key_that_is_a_column_a_gold_loader_writes_is_refused(kind, column):
    """The same collision `Scd2Dimension` refuses, over both key columns of both new
    kinds. `valid_from` and friends are refused here even though a conformed dimension
    has no version chain: one gold namespace, one reserved set, and a dimension that
    named its natural key `is_current` would be one `unionByName` away from a column
    that means two things."""
    make = KINDS[kind]
    with pytest.raises(ValueError, match="the loader writes"):
        make(surrogate_key=column)
    with pytest.raises(ValueError, match="the loader writes"):
        make(natural_key=column)


@pytest.mark.parametrize("kind", ["enumerated", "calendar"])
def test_a_conformed_dimension_whose_two_keys_are_one_name_is_refused(kind):
    """The projection writes both, so one value survives and the other is silently the
    surrogate key -- with every row still present and every join still resolving."""
    # ONE NAME, BOUND ONCE AND PASSED TWICE, rather than the same literal spelled into
    # both arguments. Two quoted keyword assignments on one line are the shape gitleaks'
    # `generic-api-key` rule matches, and it reported this test as a leaked credential --
    # see `.gitleaks.toml` for what it actually captured and why the repair lives there
    # rather than here. Binding the name also states the subject better: the test is about
    # ONE name reaching both parameters.
    one_name = "probe"
    with pytest.raises(ValueError, match="its surrogate key and its natural key"):
        KINDS[kind](natural_key=one_name, surrogate_key=one_name)


def test_an_enumerated_dimension_needs_members_and_they_must_be_distinct():
    """A member list with a repeat is two rows on one surrogate key -- the hash is over
    the natural key -- so every fact row joining that member matches twice. Empty is the
    other end: a dimension of nothing but its ghost, which every fact row then resolves
    to, reporting a clean 100% unresolved as a clean 100% resolved."""
    with pytest.raises(ValueError, match="declares no member"):
        _enumerated(members=())
    with pytest.raises(ValueError, match="declares .* twice"):
        _enumerated(members=("A", "A"))


def test_a_calendar_dimension_needs_at_least_one_role_and_no_repeats():
    """The roles are the fact's own foreign-key column names. Zero roles is a dimension
    no fact reaches; a repeated role is two columns of one name in `fact_payment`."""
    with pytest.raises(ValueError, match="declares no role"):
        _calendar(roles=())
    with pytest.raises(ValueError, match="declares .* twice"):
        _calendar(roles=("event_date_key", "event_date_key"))


# --- the point-in-time kind ----------------------------------------------------------


def test_the_pit_points_at_two_satellites_of_one_hub_and_names_its_pointers_after_them():
    """The pin, and the pointer names are DERIVED rather than declared -- a second
    spelling of a satellite name would be a column name that goes stale on a rename, and a
    PIT whose pointer columns disagree with the satellites they point at is a table every
    as-of join silently misses."""
    assert PIT_ESTABELECIMENTO.hub == "hub_estabelecimento"
    assert PIT_ESTABELECIMENTO.satellites == (
        "sat_estabelecimento_dados", "sat_estabelecimento_endereco",
    )
    assert PIT_ESTABELECIMENTO.pointer_columns == (
        "sat_estabelecimento_dados_applied_date",
        "sat_estabelecimento_endereco_applied_date",
    )
    for satellite in PIT_ESTABELECIMENTO.satellites:
        assert domains.table_spec(satellite).parent == PIT_ESTABELECIMENTO.hub


@pytest.mark.parametrize(
    "satellites",
    [(), ("sat_estabelecimento_dados",), ("sat_estabelecimento_dados",) * 2],
    ids=["none", "one", "one twice"],
)
def test_a_pit_over_fewer_than_two_distinct_satellites_is_refused(satellites):
    """WHAT A PIT IS FOR, made checkable. It exists to defeat timeline collapse between
    two satellites that change on independent dates. Over ONE there is nothing to
    collapse -- the naive equi-join on that satellite's own `applied_date` already IS the
    as-of answer -- so the table would promise a mechanism it does not perform. The repeat
    is the same defect wearing two names: both pointer columns would be named after one
    satellite and the second would overwrite the first."""
    with pytest.raises(ValueError, match="at least TWO satellites|twice"):
        _pit(satellites=satellites)


def test_a_pit_whose_as_of_column_is_spelled_applied_date_is_refused():
    """One character of ambiguity reconstructing the defect. The pointers ARE
    `<satellite>_applied_date`, so a bare `applied_date` here invites
    `pit.applied_date = sat.applied_date` -- the naive equi-join this table replaces --
    and that join RETURNS AN ANSWER, which is why it has to be refused at declaration."""
    with pytest.raises(ValueError, match="the naive equi-join this table exists to"):
        _pit(as_of_column="applied_date")


@pytest.mark.parametrize("column", [VALID_FROM, VALID_TO, IS_CURRENT, "load_date"])
def test_a_pit_as_of_column_that_is_a_column_the_loader_writes_is_refused(column):
    """One gold namespace, one reserved set -- the collision `Scd2Dimension` and both
    conformed kinds refuse, over the one column name a PIT declares for itself."""
    with pytest.raises(ValueError, match="the loader writes itself"):
        _pit(as_of_column=column)


def test_a_pit_whose_as_of_column_is_its_hubs_hash_key_is_refused():
    """A WHOLE-SET GUARD AND NOT A `__post_init__` CHECK, for the same reason
    `_assert_no_surrogate_key_collides_with_its_source` is one: `hub_estabelecimento_hk`
    is a perfectly good column name until you know which hub this table is over."""
    hub = domains.table_spec("hub_estabelecimento")
    with pytest.raises(ValueError, match="is .*'s hash key"):
        build_registry((_pit(as_of_column=hub.hash_key),))


def test_a_pit_whose_hub_is_unregistered_or_is_not_a_hub_is_refused():
    """A PIT's spine is a hub's KEY SET. Handed a satellite instead, the frame it would
    cross with the as-of dates is a version chain, so every key appears once per version
    and the grain is silently wrong rather than absent.

    THE WRONG-KIND PROBE IS A SATELLITE OF ANOTHER HUB, not one of this PIT's own: the
    spec refuses a hub that is also one of its satellites at declaration, so a probe using
    `sat_estabelecimento_dados` would be refused before `build_registry` ran and this test
    would be measuring the other guard."""
    with pytest.raises(ValueError, match="which no vault domain registers"):
        build_registry((_pit(hub="hub_estabeleciment"),))
    with pytest.raises(ValueError, match="is not a hub"):
        build_registry((_pit(hub="sat_empresa_dados"),))


def test_a_pit_pointing_at_a_satellite_of_another_hub_is_refused():
    """THE GUARD THAT IS SILENT WITHOUT IT, and the argument is about what a PIT does
    rather than what it joins: the keys and the pointers are UNIONED on the hash key, so a
    satellite of another hub is caught by column name TODAY only because the two hubs
    spell their hash keys differently. Nothing in `opl.vault.specs` requires that; where
    two hubs agree, the union succeeds and every pointer is taken over another hub's
    history."""
    with pytest.raises(ValueError, match="hangs off"):
        build_registry(
            (_pit(satellites=("sat_estabelecimento_dados", "sat_empresa_dados")),)
        )
    with pytest.raises(ValueError, match="not a registered satellite"):
        build_registry(
            (_pit(satellites=("sat_estabelecimento_dados", "hub_empresa")),)
        )


def test_a_pit_naming_its_own_hub_as_a_satellite_is_refused():
    """The hub carries no `applied_date`, so the pointer over it would fail inside Spark's
    analysis naming a column rather than a table -- and if a hub ever grew one, it would
    point at a table that has no versions to point at."""
    with pytest.raises(ValueError, match="both its hub and one of its satellites"):
        _pit(satellites=("hub_estabelecimento", "sat_estabelecimento_dados"))


def test_a_pit_needs_a_name_a_hub_and_an_as_of_column():
    for blank in ({"name": " "}, {"hub": ""}, {"as_of_column": None}):
        with pytest.raises(ValueError):
            _pit(**blank)


# --- the fact kind -------------------------------------------------------------------


def test_the_fact_carries_a_role_key_for_every_counterparty_the_contract_declares():
    """T-A's DECLARATION half. The acceptance this phase inherited -- "every
    `fact_payment` row resolves to exactly one `dim_company` version" -- is ill-formed: a
    correct row resolves to TWO, one per role. This pins that both roles exist, that both
    resolve against the SAME dimension (which is what "conformed" means), and that the
    counterparty half of each pair is the contract's own column and not a copy."""
    assert FACT_PAYMENT.roles == (
        ("payer_cnpj_basico", "payer_company_sk"),
        ("payee_cnpj_basico", "payee_company_sk"),
    )
    assert tuple(c for c, _k in FACT_PAYMENT.roles) == payments.COUNTERPARTY_COLUMNS
    assert FACT_PAYMENT.company_dimension == DIM_COMPANY.name
    assert FACT_PAYMENT.role_keys == ("payer_company_sk", "payee_company_sk")
    assert FACT_PAYMENT.grain_key == payments.IDENTITY_COLUMN


@pytest.mark.parametrize(
    "roles",
    [
        (("payer_cnpj_basico", "payer_company_sk"),),
        (("payee_cnpj_basico", "payee_company_sk"),),
        (("payer_cnpj_basico", "a"), ("payer_cnpj_basico", "b")),
    ],
    ids=["payer only", "payee only", "payer twice"],
)
def test_a_fact_that_does_not_resolve_every_counterparty_is_refused(roles):
    """THE READING OF THE PLAN'S CLOSING TEST THAT IS ACTUALLY SATISFIABLE, refused at
    declaration. A fact joining on the payer alone has the right row count, a clean 100%
    resolution rate, and no payee at all -- so every report grouped by payee returns
    nothing and nothing about the build fails. "Payer twice" is the same defect wearing two
    roles, which is why the guard reads `COUNTERPARTY_COLUMNS` rather than counting."""
    with pytest.raises(ValueError, match="must play exactly one role|declares roles for"):
        _fact(roles=roles)


@pytest.mark.parametrize("attribute", payments.BUSINESS_ATTRIBUTE_COLUMNS)
def test_a_fact_grained_on_a_business_attribute_is_refused(attribute):
    """T-D AT DECLARATION, over every business attribute rather than over the one somebody
    would reach for first. A legitimate repeat is a DIFFERENT `transaction_id` under an
    IDENTICAL attribute tuple, so a grain taken from that tuple deletes 1,600 real payments
    on today's bronze and returns a plausible 18,400 -- with the duplicate count still
    reporting 150, because that number is `COUNT(*) - COUNT(DISTINCT <grain>)` by
    definition of the operation and cannot fail."""
    with pytest.raises(ValueError, match="BUSINESS ATTRIBUTE"):
        _fact(grain_key=attribute)


def test_a_fact_grained_or_measured_on_a_column_the_contract_does_not_declare():
    """The other half of the grain refusal, and the reason gold may spell a contract column
    name at all: a name v1 does not carry turns the import of every gold module red rather
    than failing inside Spark's analysis after a session has started."""
    with pytest.raises(ValueError, match="no column the payment contract declares"):
        _fact(grain_key="transaction_di")
    with pytest.raises(ValueError, match="not a business attribute of a payment"):
        _fact(measure=payments.EMITTED_AT_COLUMN)


@pytest.mark.parametrize("column", [VALID_FROM, VALID_TO, IS_CURRENT, "load_date"])
def test_a_role_key_that_is_a_column_a_gold_loader_writes_is_refused(column):
    """One gold namespace, one reserved set -- the collision every other kind here refuses,
    over the two column names a fact declares for itself."""
    with pytest.raises(ValueError, match="the gold loaders write that column"):
        _fact(roles=(("payer_cnpj_basico", column), ("payee_cnpj_basico", "b")))


@pytest.mark.parametrize("column", ["amount", "event_time", "transaction_id"])
def test_a_role_key_that_is_a_column_the_fact_projects_from_the_payment_is_refused(column):
    """The collision no other kind has, because no other kind projects contract columns. A
    fact carries the grain, the measure and `event_time` under the contract's own names, so
    a role key spelled `amount` is one projection writing two values into one column -- the
    measure survives or the key does, every row is present, and the join matches nothing."""
    with pytest.raises(ValueError, match="already projects that name from the payment"):
        _fact(roles=(("payer_cnpj_basico", column), ("payee_cnpj_basico", "b")))


def test_a_fact_whose_company_dimension_is_missing_or_is_not_an_scd2_dimension():
    """Resolved against THIS registry and not the vault's, which is what makes this kind
    different from every other one here: a fact reads bronze and joins to a GOLD table.
    Handed a conformed dimension it would look for a half-open interval in a table that has
    none -- caught by Spark, and only after a serverless session had started."""
    star = (DIM_COMPANY, DIM_DATE, DIM_CHANNEL, DIM_CURRENCY)
    with pytest.raises(ValueError, match="not a registered SCD2 dimension"):
        build_registry((_fact(company_dimension="dim_compnay"), *star))
    with pytest.raises(ValueError, match="not a registered SCD2 dimension"):
        build_registry((_fact(company_dimension="dim_date"), *star))


def test_a_conformed_dimension_the_fact_does_not_reach_is_refused():
    """THE ONLY MECHANICAL ANSWER THIS REPOSITORY HAS TO "DECORATIVE IN A STAR SCHEMA".
    A conformed dimension exists to be reached by a fact; one the fact does not name builds
    fine, is well-formed, and returns its members and no facts in every report. Stated as an
    EQUALITY against the registry's conformed set, so the omission turns the import red
    rather than being a convention somebody has to remember."""
    with pytest.raises(ValueError, match="reaches .* and this registry holds"):
        build_registry(
            (_fact(conformed=("dim_date", "dim_channel")), DIM_COMPANY, DIM_DATE,
             DIM_CHANNEL, DIM_CURRENCY)
        )
    with pytest.raises(ValueError, match="reaches .* and this registry holds"):
        build_registry((_fact(), DIM_COMPANY, DIM_DATE, DIM_CHANNEL))


def test_two_of_the_facts_projected_columns_sharing_a_name_are_refused():
    """A WHOLE-SET GUARD BECAUSE HALF THE COLUMN LIST IS OTHER TABLES'. Two enumerated
    dimensions sharing a `surrogate_key` are legal on their own -- nothing in this registry
    refuses it, and their NAMES differ -- and the fact then projects one foreign key where
    it declares two. Both are integers, so nothing fails and one dimension is simply
    unreachable."""
    collided = EnumeratedDimension(
        name=DIM_CURRENCY.name,
        surrogate_key=DIM_CHANNEL.surrogate_key,
        natural_key=DIM_CURRENCY.natural_key,
        fact_column=DIM_CURRENCY.fact_column,
        members=DIM_CURRENCY.members,
    )
    with pytest.raises(ValueError, match="projects .* more than once"):
        build_registry((_fact(), DIM_COMPANY, DIM_DATE, DIM_CHANNEL, collided))


def test_the_facts_conformed_keys_are_the_role_and_never_the_dimensions_own_key():
    """`dim_date`'s own key is `date_key` and the fact's column is `event_date_key`,
    because a date dimension is the one kind a fact reaches under a name saying WHICH date.
    The other two have no second name to take, so their fact key IS their surrogate key --
    and the fact's declared order is the order those keys are projected in, which a Delta
    append matches positionally."""
    assert [DIM_DATE.fact_key, DIM_CHANNEL.fact_key, DIM_CURRENCY.fact_key] == [
        "event_date_key", "channel_key", "currency_key",
    ]
    assert DIM_DATE.fact_key == DIM_DATE.roles[0] != DIM_DATE.surrogate_key
    assert FACT_PAYMENT.conformed == ("dim_date", "dim_channel", "dim_currency")


def test_a_calendar_with_two_roles_cannot_hand_a_fact_one_date_key():
    """The derivation is only total under a stated condition, so it refuses rather than
    taking the first role. A second role is a second BUSINESS DATE -- an `authorized_at` or
    a `settled_at` in the payment contract -- and it needs a second `fact_column` beside it;
    without one, every role but the first would be a key derived from the wrong column and
    named after the right one."""
    two_roles = _calendar(roles=("event_date_key", "settled_date_key"))
    with pytest.raises(ValueError, match="derives its date key from ONE column"):
        assert two_roles.fact_key


def test_a_fact_needs_a_name_a_grain_a_measure_a_dimension_and_a_conformed_set():
    for blank in (
        {"name": " "}, {"grain_key": ""}, {"measure": None},
        {"company_dimension": ""}, {"conformed": ()},
    ):
        with pytest.raises(ValueError):
            _fact(**blank)


def test_a_dimension_needs_a_name_a_surrogate_key_and_a_source():
    for blank in ({"name": " "}, {"surrogate_key": ""}, {"source_satellite": None}):
        with pytest.raises(ValueError):
            _dimension(**blank)


def test_the_registry_is_read_only():
    """The registry is DATA. A caller who could `REGISTRY[...] = ...` could add a table
    that never passed a guard, which is the whole reason `build_registry` exists."""
    with pytest.raises(TypeError):
        REGISTRY["dim_smuggled"] = _dimension()  # type: ignore[index]


def _module_level(tree: ast.Module, prefix: str) -> set[str]:
    return {
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name.startswith(prefix)
    }


def _called_at_import(tree: ast.Module, name: str) -> bool:
    """Is `name` called by a MODULE-LEVEL statement -- bare or through an assignment?

    Both shapes, because the guards are called bare (`_assert_...(REGISTRY)`) and
    `build_registry` is called into a name (`REGISTRY = build_registry(TABLES)`), and a
    check that admitted only one of the two would silently pass the other by."""
    return any(
        isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == name
        for node in tree.body
        if isinstance(node, ast.Expr | ast.Assign | ast.AnnAssign) and node.value is not None
    )


@pytest.mark.parametrize("module", sorted(_GUARD_MODULES))
def test_every_guard_this_module_defines_is_run_at_import(module):
    """The wiring lock `tests/bronze/test_registry_guard_wiring.py` holds for bronze,
    restated for gold rather than shared -- the two modules are read by different globs
    and a helper crossing that seam would make each file's claim rest on the other's.

    What it closes: a guard that is defined, tested, and never called is a guard whose
    absence is invisible everywhere except in production. `build_registry` is called at
    import in `registry.py`'s own foot, so a malformed registry breaks the import of
    every module that reads it rather than the one job that touches that table.

    OVER `specs.py` TOO, since F3 Task 3 split the kinds out of the registry. Its guards
    are called from a `__post_init__` rather than at module level -- which is what makes
    them run before pyspark and before any registry exists -- so the same "defined and
    never called" hole exists there, one layer down."""
    path = _GOLD / module
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=module)
    defined = _module_level(tree, "_assert_")
    assert defined, f"no _assert_* guard is defined at module level in opl/gold/{module}"
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id.startswith("_assert_")
    }
    assert defined == called, (
        f"guards defined and never called: {sorted(defined - called)}; called and not "
        f"defined here: {sorted(called - defined)}"
    )
    if _GUARD_MODULES[module]:
        assert _called_at_import(tree, "build_registry"), (
            f"opl/gold/{module} does not call build_registry at import, so a malformed "
            "registry would be discovered by whichever job touched the table first"
        )
