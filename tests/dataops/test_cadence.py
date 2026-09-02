"""The declared cadence: total, self-consistent, and resting on a premise that is asserted.

WHY THE FIRST TEST IS ABOUT THE BUNDLE AND NOT ABOUT THIS MODULE. `cadence.py`'s whole
reason for existing is that no ingest here starts on a clock, so freshness has no threshold
to derive and one has to be declared instead. That premise is a fact about `databricks/`,
it was true on 2026-08-18, and nothing stopped it changing -- and it DID change: this test
read `no schedule block exists` until F8 declared some, and it went red when they landed,
which is what it was written for. It then stayed red unread -- that phase's own review
reported the branch failing on the README lock and on nothing else. What replaces it is the
narrower property the declaration actually needs, and the mechanism behind it is owned by
`tests/test_bundle_targets_and_schedules.py` rather than restated here. A declaration whose
reason has quietly become false is worse than no declaration.

NO COUNT OF SCHEDULES IS WRITTEN HERE OR IN `cadence.py` ANY MORE, and the deletion is the
correction rather than a tidy-up. Both said "twelve", locked by nothing, and the number was
already wrong inside the same phase: F8's second correction pass took two cadences back off
when one of them turned out to contradict this module's own `lookup` entry. Derive it:

    git grep -l quartz_cron_expression databricks/resources/ | wc -l
"""
from __future__ import annotations

import importlib.util
import sys

import pytest
from job_yaml import FIRING_KEYS, JOB_OF, bundle_docs, job_of, keys_anywhere

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

# The key whose ABSENCE from every committed bundle file is what leaves the deployment
# mode deciding whether a schedule fires. `tests/test_bundle_targets_and_schedules.py`
# owns that rule and its failure arm; this file reads the same absence because the
# numbers below rest on it.
_PAUSE = "pause_status"


def test_no_schedule_in_this_bundle_can_fire_so_the_cadence_stays_a_declaration():
    """The premise `cadence.py` rests on, asserted rather than remembered.

    ALL 29 `ingest` task runs in this workspace were launched by hand, and the measured
    spread of `MAX(_ingested_at)` across the seven bronze tables is hours to 18 days. F8
    declared cadences, so `no schedule exists` -- what this test asserted until then -- is
    no longer the property that keeps the numbers in `cadence.py` a
    declaration. TWO THINGS DO: the source writes no `pause_status`, so the target's mode
    decides; and the target this bundle deploys is `mode: development`, under which the
    CLI renders `PAUSED`. Either one changing means a run can start on a clock.

    THIS IS NOT THE FULL MECHANISM AND DOES NOT PRETEND TO BE. What the CLI actually
    renders is observed in `tests/test_bundle_targets_and_schedules.py`, which needs
    credentials; what is read here is only the two committed facts this file's numbers
    rest on."""
    docs = bundle_docs()
    targets = docs["databricks.yml"]["targets"]
    deployed = sorted(name for name, target in targets.items() if target.get("default"))
    modes = [targets[name].get("mode") for name in deployed]
    written = sorted(name for name, doc in docs.items() if _PAUSE in keys_anywhere(doc))
    assert (deployed, modes, written) == (["free"], ["development"], []), (
        f"the default target is {deployed} in mode {modes}, and {written} write "
        f"`{_PAUSE}`. `opl.dataops.cadence`'s premise -- that every ingest is still "
        "launched by hand, because every declared schedule deploys PAUSED under the one "
        "target this repository deploys -- no longer holds. Re-decide the cadence against "
        "what the schedule now does, rather than adjusting this to restore the green"
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


def _fires(table: str) -> list[str]:
    """Which of `FIRING_KEYS` the job that ingests `table` declares. Usually none."""
    job = job_of(JOB_OF[table])
    return [key for key in FIRING_KEYS if job.get(key)]


def _paused_jobs_that_fire(cadence=CADENCE) -> list[str]:
    """Jobs that INGEST a table declared PAUSED and nevertheless declare a way to start.

    THE CROSS-CHECK F8 SHIPPED WITHOUT. That phase put `bronze_cnpj_lookup` -- the job that
    ingests `lookup` -- on `0 0 6 15 * ?` while this module went on declaring `lookup`
    deliberately not ingested on a recorded scope decision, and the freshness view went on
    printing `paused_by_decision` beside it. Two claims in one tree and nothing compared
    them: the bundle's locks read the bundle, and this file's locks read the wheel.

    NEITHER SPELLING IS MADE HERE. `JOB_OF` is the registry-to-job mapping
    `tests/test_job_yaml_wiring.py` already holds total over the registry, and `FIRING_KEYS`
    is declared once in `job_yaml` and read by the bundle's own classification lock."""
    return [
        f"{table} is declared {PAUSED} and {JOB_OF[table]} declares {_fires(table)}"
        for table, entry in sorted(cadence.items())
        if entry.kind == PAUSED and _fires(table)
    ]


def test_no_job_that_ingests_a_paused_table_declares_a_way_to_start_itself():
    """A cadence declared for the ingest of a table this repository calls un-ingested.

    ONE DIRECTION ONLY, AND THE OTHER IS DELIBERATELY NOT ASSERTED. A `DECLARED` table whose
    job carries no schedule is not a defect: this module's header says every ingest here is
    launched by hand, and that a declared rhythm is an expectation about the SOURCE rather
    than a promise about the bundle. Asserting that direction would refuse the state the
    file already argues is the normal one."""
    assert not _paused_jobs_that_fire()


def test_the_paused_cross_check_catches_a_cadence_declared_for_a_paused_table():
    """Proves the lock can fail, in the shape it DID fail in.

    The table moved to PAUSED is chosen BY HAVING a job that fires, so the arm names no job,
    no table and no cron: it goes red for the pairing rather than for a literal, and it
    reports loudly if this bundle ever stops scheduling any ingest at all."""
    scheduled = sorted(table for table in CADENCE if _fires(table))
    assert scheduled, "no ingestion job declares a way to start; this arm has nothing to pair"
    broken = dict(CADENCE)
    broken[scheduled[0]] = Cadence(kind=PAUSED, every_days=None, why="paused by this arm")
    assert _paused_jobs_that_fire(broken)


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
