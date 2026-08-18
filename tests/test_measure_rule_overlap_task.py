# tests/test_measure_rule_overlap_task.py
"""The overlap measurement's job entry point: what it refuses, and WHICH TABLE it reads.

Loaded by path -- the `databricks/src` scripts are job entry points, not part of the opl
wheel -- and nothing here starts Spark: `main` refuses a stray argument before it builds a
session, and the corpus claim below is read off the source.

THE CORPUS IS STAGING, AND THAT IS THE DECISION THIS FILE PINS. Three of four F4 audits
concluded this measurement had to wait for a Unity Catalog mask repair, because 3,583 of
the 5,589 rows in the live quarantine tables (64%) are rejected on
`nome_socio_razao_social`, which a column mask renders as `***` to every principal that
is not in `opl_pii_readers` -- including the table owner. Measuring the overlap over
QUARANTINE would therefore have measured the mask.

It does not read quarantine. `opl.bronze.masking.masked_table_ddls` covers bronze and
quarantine and deliberately never staging (`test_the_control_covers_bronze_and_quarantine
_and_never_staging`), because a mask on staging would make the next promote read `***` and
write it into bronze, and would stop `null_or_empty_nome_socio_razao_social` rejecting
anything -- `***` is neither null nor empty. Staging holds EVERY row Auto Loader read,
promoted and rejected alike, unmasked: measured 2026-08-18, `bronze_cnpj_socios_staging`
holds 55,830,826 rows of which 0 read `***` and 3,583 are blank in that column -- exactly
the two months of rejects. So the ordering problem the audits raised does not exist, and
this test is what keeps that true: a future edit pointing the measurement at
`spec.quarantine` reads a masked column and silently measures the control instead of the
data."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from opl.bronze.registry import REGISTRY

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"
_SCRIPT = _SRC / "measure_rule_overlap.py"


def _load():
    spec = importlib.util.spec_from_file_location("measure_rule_overlap", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_an_argument_this_task_does_not_read_is_refused_rather_than_discarded():
    """A parameter reaching a task that reads none is a job YAML configuring nothing.

    Refused BEFORE the session, which is what makes this assertable with no Spark: an
    operator who hands it a table name has a belief about what the run will measure, and
    the run is total over the registry whatever they typed."""
    with pytest.raises(ValueError, match="takes no arguments"):
        _load().main(["socios"])


def test_it_reads_staging_and_neither_quarantine_nor_bronze():
    """The corpus, read off the source. See this module's docstring for why it matters.

    Field names rather than table names: the script resolves everything off a registry
    spec, so `spec.quarantine` is the only spelling a drift could take, and no literal
    table name appears in the file at all."""
    source = _SCRIPT.read_text(encoding="utf-8")
    code = "\n".join(
        line for line in source.splitlines() if not line.strip().startswith("#")
    )
    assert "spec.staging" in code
    assert "spec.quarantine" not in code
    assert "spec.bronze" not in code
    named = sorted(
        {
            value
            for spec in REGISTRY.values()
            for value in (spec.staging, spec.bronze, spec.quarantine, spec.table_key)
            if value in code
        }
    )
    assert not named, f"the task names bronze table(s) {named} directly"


def test_it_is_total_over_the_registry_rather_than_a_hand_written_list():
    """Every registered table, or the measurement's own scope is a thing to maintain.

    ADR 0006's six cells covered three tables; the hole it left open is one it never
    looked at, because the query was hand-written per table. A count that has to be
    extended by hand when a table is registered goes stale in the direction that reports
    a clean overlap over a contract nobody measured."""
    module = _load()
    assert "REGISTRY" in _SCRIPT.read_text(encoding="utf-8")
    assert module.TASK == "measure_rule_overlap"


def test_the_task_calls_a_bare_main_so_a_failure_fails_the_run():
    """`spark_python_task` runs the file; a `main()` that is never called exits 0 and
    reports a SUCCESSFUL run as FAILED-to-do-anything. Every task under databricks/src
    calls a bare `main()`."""
    source = _SCRIPT.read_text(encoding="utf-8")
    assert source.rstrip().endswith('if __name__ == "__main__":\n    main()')
