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
    DIM_CURRENCY,
    DIM_DATE,
    REGISTRY,
    CalendarDimension,
    EnumeratedDimension,
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


# Every kind the gold registry knows, with a factory that builds a well-formed one. The
# cross-layer guards below are parametrised over this rather than over `Scd2Dimension`
# alone: a name collision is a property of the NAME, so a guard that only saw one kind
# would be a guard the second kind walks past.
KINDS = {"scd2": _dimension, "enumerated": _enumerated, "calendar": _calendar}


def test_the_registered_star_is_dim_company_and_the_three_conformed_dimensions():
    """The pin. `dim_company` is derived from ONE satellite and that satellite's name is
    the whole of what the loader is told -- the payload, the parent hub and the business
    key are all read from the vault registry rather than restated here, so a second
    spelling of any of them cannot exist to drift."""
    assert set(REGISTRY) == {"dim_company", "dim_date", "dim_channel", "dim_currency"}
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


@pytest.mark.parametrize("kind", sorted(KINDS))
@pytest.mark.parametrize("vault_table", sorted(domains.REGISTRY))
def test_a_gold_table_named_like_a_vault_table_is_refused(vault_table, kind):
    """ONE FLAT SCHEMA. `opl.config.OplConfig.table` puts every layer's Delta tables in
    `workspace.default`, so a gold name equal to a vault name is one `mode("append")`
    away from writing dimension rows into a satellite.

    OVER EVERY KIND (T-E), because the collision is a property of the NAME and of the
    flat namespace, not of what the table holds. A static dimension is the kind most
    likely to acquire a short, generic name -- `dim_date` is one word away from a lookup
    somebody adds later -- and a guard that had only ever been driven through
    `Scd2Dimension` would be a guard the new kind walks past with the suite still
    green."""
    with pytest.raises(ValueError, match="already owned by the vault"):
        build_registry((KINDS[kind](name=vault_table),))


@pytest.mark.parametrize("kind", sorted(KINDS))
@pytest.mark.parametrize("bronze_table", sorted(BRONZE_REGISTRY))
def test_a_gold_table_named_like_any_of_a_bronze_tables_three_delta_names_is_refused(
    bronze_table, kind
):
    """All THREE names, not just the bronze one. A promote appends into staging and the
    DQ gate appends into quarantine, so a dimension sitting on either is reached by a
    job nobody would think to look at -- and the quarantine is the documented case
    (`opl.bronze.registry`: a quarantine name hardcoded in a job YAML "sent estab
    triagers to a table full of unrelated F1.2 lookup rows")."""
    spec = BRONZE_REGISTRY[bronze_table]
    for name in (spec.staging, spec.bronze, spec.quarantine):
        with pytest.raises(ValueError, match="already owned by bronze"):
            build_registry((KINDS[kind](name=name),))


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_two_gold_tables_of_any_kind_claiming_one_name_are_refused(kind):
    """One Delta name, two specs: one of them would load into the other's table with both
    runs reporting success. Over every kind for the two tests above's reason."""
    make = KINDS[kind]
    with pytest.raises(ValueError, match="both declare a gold table"):
        build_registry((make(), make(surrogate_key="other_key")))


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
    with pytest.raises(ValueError, match="its surrogate key and its natural key"):
        KINDS[kind](natural_key="probe_key", surrogate_key="probe_key")


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
