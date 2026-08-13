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
from opl.gold.columns import IS_CURRENT, VALID_FROM, VALID_TO
from opl.gold.registry import (
    REGISTRY,
    Scd2Dimension,
    UnknownGoldTable,
    build_registry,
    table_spec,
)
from opl.vault import domains

_MODULE = Path(__file__).resolve().parents[2] / "src" / "opl" / "gold" / "registry.py"


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


def test_the_registered_star_is_dim_company_over_sat_empresa_dados():
    """The pin. `dim_company` is derived from ONE satellite and that satellite's name is
    the whole of what the loader is told -- the payload, the parent hub and the business
    key are all read from the vault registry rather than restated here, so a second
    spelling of any of them cannot exist to drift."""
    assert set(REGISTRY) == {"dim_company"}
    spec = table_spec("dim_company")
    assert (spec.surrogate_key, spec.source_satellite) == ("company_sk", "sat_empresa_dados")


def test_table_spec_refuses_an_unknown_name_and_names_the_alternatives():
    """Refused BEFORE Spark, like `opl.bronze.registry.table_spec` and
    `opl.vault.registry.table_spec`: an operator who mistyped a table should not wait for
    a serverless session to be told so, and the message has to carry the valid list or it
    sends them to the source."""
    with pytest.raises(UnknownGoldTable, match="dim_company"):
        table_spec("dim_compnay")


def test_two_gold_tables_claiming_one_name_are_refused():
    """One Delta name, two specs: one of them would load into the other's table with
    both runs reporting success."""
    with pytest.raises(ValueError, match="both declare a gold table"):
        build_registry((_dimension(), _dimension(surrogate_key="other_sk")))


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


@pytest.mark.parametrize("vault_table", sorted(domains.REGISTRY))
def test_a_gold_table_named_like_a_vault_table_is_refused(vault_table):
    """ONE FLAT SCHEMA. `opl.config.OplConfig.table` puts every layer's Delta tables in
    `workspace.default`, so a gold name equal to a vault name is one `mode("append")`
    away from writing dimension rows into a satellite."""
    with pytest.raises(ValueError, match="already owned by the vault"):
        build_registry((_dimension(name=vault_table),))


@pytest.mark.parametrize("bronze_table", sorted(BRONZE_REGISTRY))
def test_a_gold_table_named_like_any_of_a_bronze_tables_three_delta_names_is_refused(
    bronze_table,
):
    """All THREE names, not just the bronze one. A promote appends into staging and the
    DQ gate appends into quarantine, so a dimension sitting on either is reached by a
    job nobody would think to look at -- and the quarantine is the documented case
    (`opl.bronze.registry`: a quarantine name hardcoded in a job YAML "sent estab
    triagers to a table full of unrelated F1.2 lookup rows")."""
    spec = BRONZE_REGISTRY[bronze_table]
    for name in (spec.staging, spec.bronze, spec.quarantine):
        with pytest.raises(ValueError, match="already owned by bronze"):
            build_registry((_dimension(name=name),))


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


def test_every_guard_this_module_defines_is_run_at_import():
    """The wiring lock `tests/bronze/test_registry_guard_wiring.py` holds for bronze,
    restated for gold rather than shared -- the two modules are read by different globs
    and a helper crossing that seam would make each file's claim rest on the other's.

    What it closes: a guard that is defined, tested, and never called is a guard whose
    absence is invisible everywhere except in production. `build_registry` is called at
    import in this module's own foot, so a malformed registry breaks the import of every
    module that reads it rather than the one job that touches that table."""
    tree = ast.parse(_MODULE.read_text(encoding="utf-8"), filename=_MODULE.name)
    defined = _module_level(tree, "_assert_")
    assert defined, "no _assert_* guard is defined at module level in opl/gold/registry.py"
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
    assert _called_at_import(tree, "build_registry"), (
        "opl/gold/registry.py does not call build_registry at import, so a malformed "
        "registry would be discovered by whichever job touched the table first"
    )
