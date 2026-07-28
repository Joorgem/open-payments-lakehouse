# tests/test_promote_batch_task.py
"""Unit tests for the `databricks/src/promote_batch.py` job task: which tables it
promotes between, and when the constraint DDL runs.

Loaded by path with the same importlib pattern as `tests/test_extract_cnpj_cli.py`
-- the `databricks/src` scripts are job entry points, not part of the opl wheel.
No JVM and no workspace: the Spark session is a recorder and the promote itself
is stubbed, because what is under test here is the wiring, not Spark.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from opl.bronze.promote import PromoteRefused, PromoteResult
from opl.bronze.rules import rules_for

_SCRIPT = Path(__file__).resolve().parents[1] / "databricks" / "src" / "promote_batch.py"
_spec = importlib.util.spec_from_file_location("promote_batch_task", _SCRIPT)
assert _spec is not None and _spec.loader is not None
task = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(task)


class FakeSpark:
    """Records the SQL the task issues. Nothing else of a session is used."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, statement: str) -> None:
        self.statements.append(statement)


def _stub_session(monkeypatch) -> FakeSpark:
    spark = FakeSpark()
    monkeypatch.setattr(
        task, "SparkSession",
        SimpleNamespace(builder=SimpleNamespace(getOrCreate=lambda: spark)),
    )
    return spark


def _record_promote(monkeypatch, result=None, raises=None) -> dict:
    seen: dict[str, object] = {}

    def fake_promote(spark, batch_id, *, staging_table, bronze_table, rules):
        seen.update(batch_id=batch_id, staging_table=staging_table,
                    bronze_table=bronze_table, rules=rules)
        if raises is not None:
            raise raises
        return result

    monkeypatch.setattr(task, "promote_batch", fake_promote)
    return seen


def test_it_promotes_estab_staging_into_estab_bronze(monkeypatch):
    spark = _stub_session(monkeypatch)
    seen = _record_promote(monkeypatch, PromoteResult("999", 7, 0, already_promoted=False))

    task.main(["999"])

    assert seen["batch_id"] == "999"
    assert seen["staging_table"] == "workspace.default.bronze_cnpj_estab_staging"
    assert seen["bronze_table"] == "workspace.default.bronze_cnpj_estabelecimentos"
    assert [n for n, _ in seen["rules"]] == [n for n, _ in rules_for("estabelecimentos")]
    assert len(spark.statements) == 3  # NOT NULL, DROP CONSTRAINT, ADD CONSTRAINT


def test_it_reasserts_the_constraints_on_an_already_promoted_batch(monkeypatch, capsys):
    """The repair run: the append had committed, this DDL is what failed, so the
    re-run must skip the append and still reach the DDL."""
    spark = _stub_session(monkeypatch)
    _record_promote(monkeypatch, PromoteResult("999", 0, 4, already_promoted=True))

    task.main(["999"])

    assert len(spark.statements) == 3
    assert any("ADD CONSTRAINT cnpj_basico_len8" in s for s in spark.statements)
    out = capsys.readouterr().out
    assert "ALREADY" in out and "append skipped" in out
    assert "4 rejected row(s)" in out  # the rejects a human agreed to accept


def test_it_reports_the_accepted_reject_count(monkeypatch, capsys):
    _stub_session(monkeypatch)
    _record_promote(monkeypatch, PromoteResult("999", 9506870, 1, already_promoted=False))

    task.main(["999"])

    out = capsys.readouterr().out
    assert "appended 9506870 rows" in out
    assert "1 rejected row(s)" in out


def test_a_refused_promote_runs_no_ddl(monkeypatch):
    """A refusal must not touch production DDL: the parameterless accident used
    to DROP and re-ADD the CHECK constraint over all 71.9M bronze rows."""
    spark = _stub_session(monkeypatch)
    _record_promote(monkeypatch, raises=PromoteRefused("nope"))

    with pytest.raises(PromoteRefused):
        task.main(["999"])

    assert spark.statements == []


def test_no_argument_at_all_is_refused_by_the_real_guard(monkeypatch):
    """End to end through the real `promote_batch`: a missing batch_id reaches
    neither a table nor the DDL. This is the forgotten-`--params` operator run
    that used to print 'appended 0 rows' and exit 0."""
    spark = _stub_session(monkeypatch)

    with pytest.raises(PromoteRefused, match="names no batch"):
        task.main([])

    assert spark.statements == []
