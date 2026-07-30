"""Which table each job task touches, locked BEFORE the registry refactor moves it.

These are characterization tests: they assert today's behaviour so the refactor
that follows can only preserve it. The defect they exist to prevent is real and
documented in bronze_estabelecimentos_job.yml -- a hardcoded quarantine name
"sent estab triagers to a table full of unrelated F1.2 lookup rows".

Job scripts under databricks/src are entry points, not part of the opl wheel, so
they are loaded by path with the same importlib pattern the other task tests use.
Nothing here starts Spark: every assertion is about wiring, not data."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[1] / "databricks" / "src"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_wiring", _SRC / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# (script, the qualified tables its module-level wiring resolves to)
EXPECTED_TABLES = {
    "bronze_ingest": {"workspace.default.bronze_cnpj_lookup_staging"},
    "bronze_estab_ingest": {"workspace.default.bronze_cnpj_estab_staging"},
    "dq_gate_batch": {
        "workspace.default.bronze_cnpj_estab_staging",
        "workspace.default.bronze_cnpj_estab_quarantine",
    },
    "promote_batch": {
        "workspace.default.bronze_cnpj_estab_staging",
        "workspace.default.bronze_cnpj_estabelecimentos",
        "workspace.default.bronze_cnpj_estab_quarantine",
    },
}


@pytest.mark.parametrize("script,expected", sorted(EXPECTED_TABLES.items()))
def test_each_task_resolves_the_tables_it_is_supposed_to(script, expected):
    """Every table name the script's source mentions, via the constants it imports.

    Asserted as a SET, not a substring search: a script that starts touching a
    second table is exactly the regression this locks against, and a substring
    check would not see it."""
    from opl.config import DEFAULT
    module = _load(script)
    names = {
        DEFAULT.table(value)
        for key, value in vars(module).items()
        if key.isupper() and isinstance(value, str) and value.startswith("bronze_cnpj_")
    }
    # promote_batch names its bronze table in a module constant; the staging and
    # quarantine arrive as imported constants, which vars() also exposes.
    assert names == expected, f"{script} resolves {names}, expected {expected}"


def test_the_two_gates_scope_differently_today():
    """dq_gate is whole-table; dq_gate_batch is batch-scoped. The refactor
    collapses them onto the batch-scoped one, which is carry-forward #7."""
    whole = (_SRC / "dq_gate.py").read_text(encoding="utf-8")
    scoped = (_SRC / "dq_gate_batch.py").read_text(encoding="utf-8")
    assert "batch_rows(" not in whole
    assert "batch_rows(" in scoped


def test_the_lookup_promote_overwrites_and_the_estab_promote_appends():
    """The semantic difference the lookup migration has to resolve: an overwrite
    from the WHOLE staging table would write 2x the rows once a second batch
    exists, which moving the lookup files creates."""
    lookup = (_SRC / "promote.py").read_text(encoding="utf-8")
    estab = (_SRC / "promote_batch.py").read_text(encoding="utf-8")
    assert 'mode("overwrite")' in lookup
    assert "promote_batch(" in estab


@pytest.mark.parametrize(
    "script,rule_set",
    [("dq_gate_batch", "estabelecimentos"), ("promote_batch", "estabelecimentos")],
)
def test_each_task_uses_its_own_rule_set(script, rule_set):
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    assert f'rules_for("{rule_set}")' in source


def test_the_estab_constraints_are_the_ones_bronze_carries_today():
    source = (_SRC / "promote_batch.py").read_text(encoding="utf-8")
    assert "cnpj_basico SET NOT NULL" in source
    assert "cnpj_basico_len8" in source
    assert "length(trim(cnpj_basico)) = 8" in source


def test_the_lookup_constraints_are_the_ones_bronze_carries_today():
    source = (_SRC / "promote.py").read_text(encoding="utf-8")
    assert "codigo SET NOT NULL" in source
    assert "codigo_not_blank" in source
    assert "length(trim(codigo)) > 0" in source
