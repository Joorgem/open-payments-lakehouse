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

from opl.bronze.promote import (
    PromoteOutcome,
    PromoteRefused,
    PromoteResult,
    plan_promotion,
)
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

    def fake_promote(spark, batch_id, *, staging_table, bronze_table, rules, in_flow):
        seen.update(batch_id=batch_id, staging_table=staging_table,
                    bronze_table=bronze_table, rules=rules, in_flow=in_flow)
        if raises is not None:
            raise raises
        return result

    monkeypatch.setattr(task, "promote_batch", fake_promote)
    return seen


def _result(outcome, *, appended=0, rejected=0, bronze_rows=0, batch_id="999"):
    return PromoteResult(batch_id, outcome, appended, rejected, bronze_rows)


def test_it_promotes_estab_staging_into_estab_bronze(monkeypatch):
    spark = _stub_session(monkeypatch)
    seen = _record_promote(monkeypatch, _result(PromoteOutcome.APPENDED, appended=7))

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
    _record_promote(monkeypatch,
                    _result(PromoteOutcome.ALREADY_PROMOTED, rejected=4, bronze_rows=9506870))

    task.main(["999"])

    assert len(spark.statements) == 3
    assert any("ADD CONSTRAINT cnpj_basico_len8" in s for s in spark.statements)
    out = capsys.readouterr().out
    assert "ALREADY" in out and "append skipped" in out
    assert "9506870" in out  # the count that made it "already promoted", not a boolean
    assert "4 rejected row(s)" in out  # the rejects a human agreed to accept


def test_a_repromote_after_a_rebuild_that_dropped_staging_still_reaches_the_ddl(
        monkeypatch, capsys):
    """Bronze holds the batch, staging no longer does. The append is skipped, the
    DDL still runs, and the log says the promotable count could not be
    re-checked -- it does not claim a verified match it did not make."""
    spark = _stub_session(monkeypatch)
    _record_promote(
        monkeypatch,
        _result(PromoteOutcome.ALREADY_PROMOTED_STAGING_GONE, bronze_rows=9506870),
    )

    task.main(["315230730740144"])

    assert len(spark.statements) == 3
    out = capsys.readouterr().out
    assert "ALREADY" in out and "9506870" in out
    assert "staging no longer" in out


def test_it_never_reports_a_reject_count_it_could_not_re_derive(monkeypatch, capsys):
    """The reject count is derived from staging, and this outcome is defined by
    staging no longer holding the batch -- so this task cannot know it. It printed the
    line unconditionally anyway: "0 rejected row(s) of batch X stay in quarantine",
    which is a claim about the quarantine table that nothing here checked, and which
    contradicts the outcome's own docstring ("the log must not claim a verified
    match"). An operator recovering a batch reads that as "nothing was quarantined".

    The result comes from the REAL `plan_promotion`, not a hand-written one: a test
    that re-spells what this outcome carries would keep passing on the very
    placeholder it exists to forbid."""
    _stub_session(monkeypatch)
    planned = plan_promotion(
        "315230730740144", bronze_rows=9506870, staged_promotable=0, staged_rejected=0,
        in_flow=False, staging_table="stg", bronze_table="bronze",
    )
    assert planned.outcome is PromoteOutcome.ALREADY_PROMOTED_STAGING_GONE
    _record_promote(monkeypatch, planned)

    task.main(["315230730740144"])

    out = capsys.readouterr().out
    assert "rejected row(s) of batch" not in out  # no count is a fact here
    assert "NOT knowable" in out and "quarantine" in out


def test_it_reports_the_accepted_reject_count(monkeypatch, capsys):
    _stub_session(monkeypatch)
    _record_promote(monkeypatch,
                    _result(PromoteOutcome.APPENDED, appended=9506870, rejected=1))

    task.main(["999"])

    out = capsys.readouterr().out
    assert "appended 9506870 rows" in out
    assert "1 rejected row(s)" in out


def test_the_in_flow_flag_marks_the_batch_id_as_this_run_s_own(monkeypatch):
    """The ingestion flow passes {{job.run_id}} -- the id of the run executing
    this very task -- so an empty batch means "no new files", not a bad id. The
    operator job passes a human-supplied id naming an EARLIER run, which the task
    cannot tell apart from a typo, so the flag (and not the id's shape) carries
    the difference."""
    _stub_session(monkeypatch)
    seen = _record_promote(monkeypatch, _result(PromoteOutcome.APPENDED, appended=7))

    task.main(["999", task.IN_FLOW_FLAG])

    assert seen["batch_id"] == "999"  # the flag is not mistaken for the batch id
    assert seen["in_flow"] is True


def test_without_the_flag_the_promote_is_strict(monkeypatch):
    """Fail-closed default: an operator run, and anything that forgot the flag,
    gets the refusal rather than a green run that promoted nothing."""
    _stub_session(monkeypatch)
    seen = _record_promote(monkeypatch, _result(PromoteOutcome.APPENDED, appended=7))

    task.main(["999"])

    assert seen["in_flow"] is False


def test_a_run_that_ingested_nothing_succeeds_and_runs_no_ddl(monkeypatch, capsys):
    """The pipeline's only legitimate no-op: a scheduled run whose Auto Loader
    found no new files. It must end green, and it must not re-validate the CHECK
    constraint over all 71.9M bronze rows to promote nothing."""
    spark = _stub_session(monkeypatch)
    _record_promote(monkeypatch, _result(PromoteOutcome.NOTHING_INGESTED))

    task.main(["999", task.IN_FLOW_FLAG])

    out = capsys.readouterr().out
    assert "ingested no rows" in out and "nothing to promote" in out
    assert spark.statements == []


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
