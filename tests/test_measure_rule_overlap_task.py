# tests/test_measure_rule_overlap_task.py
"""The overlap measurement's job entry point: what it refuses, and WHICH TABLE it reads.

Loaded by path -- the `databricks/src` scripts are job entry points, not part of the opl
wheel -- and NOTHING HERE STARTS SPARK. `main` refuses a stray argument before it builds a
session; the corpus claim below is read off the source; the sweep test replaces both the
session builder and `_measure`, since what it asserts is which specs the loop reaches; and
the emitter is driven with dicts, because it indexes rows by column name and a `Row` is not
the only thing that does.

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

from opl.bronze.promote import BATCH_COLUMN
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


class _NoSession:
    """`SparkSession`'s builder shape and nothing else.

    `main` builds a session before it sweeps, and the sweep is what this file asserts, so
    the session is replaced rather than started -- the object below is only ever handed
    to a `_measure` that is itself replaced. Starting Spark here would put a JVM launch
    into a file whose whole point is that it needs none."""

    class builder:  # the attribute pyspark spells lowercase, spelled the same way here
        @staticmethod
        def getOrCreate() -> str:
            return "no session was needed"


def test_the_sweep_visits_every_registered_table_and_the_summary_counts_what_it_visited(
    monkeypatch, capsys
):
    """Every registered table, or the measurement's own scope is a thing to maintain.

    ADR 0006's six cells covered three tables; the hole it left open is one it never
    looked at, because the query was hand-written per table. A count that has to be
    extended by hand when a table is registered goes stale in the direction that reports
    a clean overlap over a contract nobody measured.

    THIS ASSERTS THE VISITED SET, and the version it replaces could not. Until F4's
    correction pass this test read `assert "REGISTRY" in _SCRIPT.read_text(...)` -- and
    the word REGISTRY appears twice in the entry point's own MODULE DOCSTRING, so the
    assertion held with the import and the loop both deleted. Narrowing `main` to
    `list(REGISTRY.values())[:3]` left all four tests in this file green.

    AND IT ASSERTS THE SUMMARY LINE, which is the other half of the same defect: that
    line printed `len(REGISTRY)`, not the number of tables measured, so the narrowed
    sweep printed "7 tables" over three with every per-rule number below it still
    correct -- and ADR 0006's published "fifteen pairs, seven contracts" would have been
    false with nothing in the log to contradict it. `_measure` returns a distinct row
    count per call here, so a total assembled from anything but its return values is
    wrong rather than coincidentally right."""
    module = _load()
    visited: list[str] = []

    def _record(spark: object, spec: object) -> int:
        visited.append(spec.name)
        return len(visited)  # 1, 2, 3, ... so the summed total pins the summands

    monkeypatch.setattr(module, "SparkSession", _NoSession)
    monkeypatch.setattr(module, "_measure", _record)
    module.main([])
    assert visited == list(REGISTRY), "the sweep is not total over the registry"
    expected = len(REGISTRY) * (len(REGISTRY) + 1) // 2
    assert capsys.readouterr().out.splitlines()[-1] == (
        f"{module.TASK}: {len(visited)} tables, {expected} staged rows read"
    )


def test_a_staging_table_with_no_batches_still_gets_a_line(capsys):
    """A table that prints nothing is a table the log cannot distinguish from unmeasured.

    An empty staging table groups into no rows, so the per-batch loop had nothing to
    iterate and the whole contract vanished from a run log whose summary counted it --
    the same shape as the summary that printed `len(REGISTRY)`, one degree less
    reachable. Not live on the 2026-08-18 sweep (all 15 pairs carried rows) and cheap to
    close."""
    module = _load()
    assert module._emit_rows("socios", [], [module.ROW_COUNT]) == 0
    printed = capsys.readouterr().out.splitlines()
    assert [line.split("\t") for line in printed] == [
        # The task name as a LITERAL: it is the grep handle every line of the report
        # carries, and `module.TASK` on both sides of an assertion pins nothing.
        ["measure_rule_overlap", "socios", module.NO_BATCH, module.ROW_COUNT, "0"]
    ]


def test_a_table_with_batches_prints_every_key_of_every_batch_and_totals_the_rows(capsys):
    """The control for the test above: the ordinary path is unchanged by it.

    Dicts rather than `Row`s -- `_emit_rows` indexes by column name and both do that --
    so the emitter's shape is asserted with no session. Two batches with different row
    counts, so a total taken from anything but `ROW_COUNT` (the batch count, the key
    count, the last row) is wrong here rather than accidentally right."""
    module = _load()
    keys = [module.ROW_COUNT, "null_or_empty_codigo"]
    rows = [
        {BATCH_COLUMN: "b1", module.ROW_COUNT: 3, "null_or_empty_codigo": 1},
        {BATCH_COLUMN: "b2", module.ROW_COUNT: 40, "null_or_empty_codigo": 0},
    ]
    assert module._emit_rows("lookup", rows, keys) == 43
    printed = [line.split("\t") for line in capsys.readouterr().out.splitlines()]
    assert [row[2:] for row in printed] == [
        ["b1", module.ROW_COUNT, "3"],
        ["b1", "null_or_empty_codigo", "1"],
        ["b2", module.ROW_COUNT, "40"],
        ["b2", "null_or_empty_codigo", "0"],
    ]


def test_the_task_calls_a_bare_main_so_a_failure_fails_the_run():
    """`spark_python_task` runs the file; a `main()` that is never called exits 0 and
    reports a SUCCESSFUL run as FAILED-to-do-anything. Every task under databricks/src
    calls a bare `main()`."""
    source = _SCRIPT.read_text(encoding="utf-8")
    assert source.rstrip().endswith('if __name__ == "__main__":\n    main()')
