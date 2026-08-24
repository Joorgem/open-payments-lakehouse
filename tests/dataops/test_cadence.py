"""The declared cadence: total, self-consistent, and resting on a premise that is asserted.

WHY THE FIRST TEST IS ABOUT YAML AND NOT ABOUT THIS MODULE. `cadence.py`'s whole reason for
existing is that nothing in this repository schedules an ingest, so freshness has no
threshold to derive and one has to be declared instead. That premise is a fact about
`databricks/resources/`, it was true on 2026-08-18, and nothing stopped it changing. A
declaration whose reason has quietly become false is worse than no declaration."""
from __future__ import annotations

import importlib.util
import sys

import pytest
import yaml
from job_yaml import RESOURCES

from opl.bronze.registry import REGISTRY
from opl.dataops import cadence as cadence_module
from opl.dataops.cadence import (
    CADENCE,
    DECLARED,
    NO_SOURCE_AXIS,
    PAUSED,
    Cadence,
    _assert_a_rule_is_never_declared_over_a_column_that_is_not_there,
    _assert_every_registered_table_declares_a_cadence,
    _assert_no_source_axis_matches_the_stamp,
    _assert_only_a_declared_cadence_carries_a_number,
    _omits_the_unprovable_ref_date_rule,
    declares_source_date,
)

# The three keys a Databricks job uses to run itself. `schedule` is cron, `trigger` is
# file/table arrival, `continuous` never stops.
_SCHEDULING_KEYS = ("schedule", "trigger", "continuous")


def test_nothing_in_the_bundle_schedules_a_run_so_the_cadence_has_to_be_declared():
    """The premise `cadence.py` rests on, asserted rather than remembered.

    All 29 `ingest` task runs in this workspace were launched by hand, and the measured
    spread of `MAX(_ingested_at)` across the seven bronze tables is hours to 18 days. If a
    schedule is ever added, the expected cadence stops being a declaration nobody enforces
    and becomes a claim about something the platform now does -- and this test is what says
    so, in the commit that adds it, rather than a year later on a dashboard."""
    scheduled = []
    for path in sorted(RESOURCES.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for name, job in (document.get("resources", {}).get("jobs", {}) or {}).items():
            scheduled += [
                f"{path.name}:{name}:{key}" for key in _SCHEDULING_KEYS if key in job
            ]
    assert scheduled == [], (
        f"{scheduled} now schedule themselves, so `opl.dataops.cadence`'s premise -- that "
        "every ingest is launched by hand and freshness therefore has no threshold to "
        "derive -- is no longer true. Re-decide the cadence against what the schedule says"
    )


def test_the_cadence_is_total_over_the_bronze_registry():
    assert set(CADENCE) == set(REGISTRY)


def test_the_totality_guard_catches_a_table_that_was_registered_and_not_declared(monkeypatch):
    """Proves the lock above can fail, in the shape it would fail in: a table added to the
    registry and not here falls out of the freshness view entirely -- present in bronze,
    absent from the metric, and nothing reports an absence."""
    monkeypatch.setattr(
        cadence_module, "CADENCE", {k: v for k, v in CADENCE.items() if k != "ptax"}
    )
    with pytest.raises(ValueError, match="not total over the bronze registry"):
        _assert_every_registered_table_declares_a_cadence()


def test_only_a_declared_cadence_carries_a_number():
    for table, cadence in CADENCE.items():
        assert (cadence.kind == DECLARED) == (cadence.every_days is not None), table
        assert cadence.why.strip(), table


def test_the_number_guard_catches_a_threshold_sitting_beside_a_paused_table(monkeypatch):
    """Proves the lock can fail, and this is the defect the whole module is aimed at.

    A number on the `paused` entry is compared against by the freshness ladder's last arm,
    so `lookup` -- deliberately not ingested, with the decision recorded -- would be
    reported as an overdue ingest. That is the alert an operator mutes in week one."""
    broken = dict(CADENCE)
    broken["lookup"] = Cadence(kind=PAUSED, every_days=45, why=CADENCE["lookup"].why)
    monkeypatch.setattr(cadence_module, "CADENCE", broken)
    with pytest.raises(ValueError, match="Only 'declared' carries a number"):
        _assert_only_a_declared_cadence_carries_a_number()


def test_the_paused_entry_cites_the_decision_that_paused_it():
    """A status nobody can trace to a decision is a status an operator has to re-litigate."""
    why = CADENCE["lookup"].why
    assert CADENCE["lookup"].kind == PAUSED
    assert "2026-06" in why and "scope decision" in why
    assert "f1.4b-pr-b-run-evidence.md 25.5" in why


def test_the_tables_with_no_source_date_are_the_ones_whose_stamp_never_writes_it():
    """This module's `NO_SOURCE_AXIS` declarations, held to the audit stamp that decides."""
    declared = {table for table, c in CADENCE.items() if c.kind == NO_SOURCE_AXIS}
    derived = {spec.name for spec in REGISTRY.values() if not declares_source_date(spec)}
    assert declared == derived == {"payments", "ptax"}


def test_the_stamp_cross_check_catches_a_declaration_that_has_gone_stale(monkeypatch):
    """Proves the lock can fail. Declared `no_source_axis` for a table that HAS the column,
    the freshness view reports it structurally dateless while a real date sits in it;
    declared the other way, the view builds `MAX(_snapshot_ref_date)` over a column that is
    not there and the CREATE fails in the workspace, after a commit."""
    broken = dict(CADENCE)
    broken["merchant"] = Cadence(kind=NO_SOURCE_AXIS, every_days=None, why="stale")
    monkeypatch.setattr(cadence_module, "CADENCE", broken)
    with pytest.raises(ValueError, match="audit stamp the registry chooses"):
        _assert_no_source_axis_matches_the_stamp()


def test_no_rule_reads_a_snapshot_date_column_that_is_never_written():
    """A rule declared over an absent column is skipped on every frame forever.

    `REQUIRES_COLUMN` makes the skip silent, so the control's absence from any report is
    indistinguishable from the control having run and found nothing -- this repository's
    most-hunted species, and the reason this is an import-time refusal."""
    _assert_a_rule_is_never_declared_over_a_column_that_is_not_there()
    for spec in REGISTRY.values():
        if not declares_source_date(spec):
            assert _omits_the_unprovable_ref_date_rule(spec), spec.name


def test_the_known_ref_date_rule_gap_is_still_exactly_lookup():
    """The OTHER direction, pinned rather than refused, because it is somebody else's
    open question. `lookup` carries `_snapshot_ref_date` and has no rule refusing a NULL in
    it; `rules.py` records that as a scope line held open by F1.4b's boundaries and not as
    a decision that lookup cannot drift. If the gap is ever closed, this fails and the
    exception comes out -- which is the only way a known gap stops being permanent."""
    gap = {
        spec.name
        for spec in REGISTRY.values()
        if declares_source_date(spec) and _omits_the_unprovable_ref_date_rule(spec)
    }
    assert gap == {"lookup"}, (
        f"the set of tables carrying _snapshot_ref_date with no rule refusing a NULL in it "
        f"is now {sorted(gap)}. rules.py records exactly one, and calls it a known gap"
    )


def _reimported_cadence():
    """A SECOND execution of `cadence.py`'s module body, from its own file.

    Not `importlib.reload`, which would rebind the module every other test in this suite
    imported from. This builds a throwaway module under a throwaway NAME and runs the body
    -- which is the only way to observe what the import-time CALLS do.

    THE THROWAWAY NAME IS ENTERED IN `sys.modules` AND REMOVED AGAIN, which the sibling
    version of this helper in `tests/triage_agent/test_incidents_declaration.py` does not
    have to do.
    `cadence.py` declares a `@dataclass` under `from __future__ import annotations`, so its
    field annotations are STRINGS, and `dataclasses` resolves them by looking the defining
    class's `__module__` up in `sys.modules` -- which raises `AttributeError: 'NoneType'`
    on a module that is not there. Registering under `_cadence_reimported` rather than
    under `opl.dataops.cadence` is what keeps the real module bound for everyone else."""
    spec = importlib.util.spec_from_file_location(
        "opl.dataops._cadence_reimported", cadence_module.__file__
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        del sys.modules[spec.name]
    return module


def _declaration(module) -> dict[str, tuple]:
    """`CADENCE` as plain fields, so two executions of the module can be compared.

    NOT `==` ON THE DATACLASSES. The re-executed module defines its OWN `Cadence` class, and
    `dataclass.__eq__` returns `NotImplemented` for an instance of a different class -- so
    equal declarations compare unequal, which is an artefact of the re-execution and not a
    fact about either declaration."""
    return {
        table: (cadence.kind, cadence.every_days, cadence.why)
        for table, cadence in module.CADENCE.items()
    }


def test_the_guards_run_at_import_so_deleting_the_call_is_a_failure_not_a_silent_loss(
    monkeypatch,
):
    """The half every `pytest.raises` sibling above leaves open, closed here.

    Each of the four refusals in this file is paired with a test that calls it on a broken
    declaration and requires a raise. NONE of them proves the refusals RUN. Measured
    2026-08-24: deleting all four calls at the bottom of `cadence.py` left this file at
    `10 passed` -- every guard still provably able to fail, and none of them wired to
    anything. That is this repository's most-hunted species one level up: not a check that
    cannot fail, but a check that can fail and is never asked.

    So `REGISTRY` gains a table no cadence declares and the module body is executed again;
    the ValueError has to come out of the IMPORT. With the calls deleted, the re-execution
    returns a module and this fails.

    The first line is the control: re-executing an UNMUTATED module must succeed, or the
    raise below could be about the re-execution rather than about the declaration."""
    assert _declaration(_reimported_cadence()) == _declaration(cadence_module)

    monkeypatch.setitem(REGISTRY, "a_table_no_cadence_declares", REGISTRY["ptax"])
    with pytest.raises(ValueError, match="not total over the bronze registry"):
        _reimported_cadence()
