"""The declaration behind the last-N comparison, held against the bundle. No Spark here.

WRITTEN AS TWO FILES FROM THE START, WHICH IS THE ONE PROCESS LESSON THIS PHASE HAS PAID
FOR TWICE. `test_incidents.py` reached 845 lines and `test_severity.py` 798 before each was
split at this same seam, and each split cost a review pass of its own. Nothing here touches
a fixture, a view, `spark` or `SystemTables`, so every test in this file runs without a JVM;
what needs one is in `tests/triage_agent/test_history.py`, and NEITHER FILE IMPORTS THE
OTHER.

AND NOTHING ENFORCES THAT FIRST PARAGRAPH, which is said here rather than assumed. Adding a
Spark test to this file costs the property silently and no test goes red --
`tests/test_size_caps.py` covers the line count and nothing covers the JVM. That guard was
considered and deliberately not built for T1's and T3's reason: every cheap spelling of it
is this repository's hunted species one level down, since a signature scan passes while a
module-scope session, an autouse fixture or a transitive import still starts a JVM.
`docs/f6-run-evidence.md` section 3 carries it as unguarded across all three of the
declaration files, this one among them, and records the one measurement made of this file's
no-JVM property: it ran under a plugin replacing both `launch_gateway`s and
`subprocess.Popen`, nothing tripped, and the same plugin on `tests/triage_agent/
test_history.py` raised, which is the positive control the other two files' measurements did
not have. A RUN IS NOT A GUARD. Nothing in the suite makes this property fail, and the
measurement stops being about this file on the commit after it.

WHAT IS PROVEN HERE, AND EVERY LOCK HAS A MUTATION BESIDE IT. A test that reads a file
passes just as happily on a typo in its own extraction as on correct wiring, so each lock
below is fired on the drift it exists to refuse, and each import-time guard is fired on the
declaration it exists to refuse rather than called over data that already passed at import
-- which is what T1's first version did, and deleting its import-time calls left that file
GREEN.

THE COLOUR IS NAMED AND NEVER THE TOTAL. A test count goes stale on the next commit that
adds one, which this repository has ruled twice.
"""
from __future__ import annotations

import importlib.util
import re
import shutil
import sys
from pathlib import Path

import pytest
import yaml
from job_yaml import RESOURCES

from opl.bronze import reconcile as reconcile_module
from opl.config import DEFAULT
from opl.dataops.telemetry import TASK_TELEMETRY_VIEW
from opl.triage_agent import history as history_module
from opl.triage_agent import incidents as incidents_module
from opl.triage_agent.history import (
    GATE_SPELLINGS,
    HISTORY_READINGS,
    HISTORY_TASK_KEY,
    LIVE,
    N_EXECUTIONS,
    RETIRED,
    GateSpelling,
    _assert_a_live_gate_spelling_exists,
    _assert_every_gate_spelling_declares_a_status_and_a_reason,
    _assert_no_reading_word_is_another_modules,
    _assert_the_task_keys_are_three_different_roles,
    history_sql,
    live_gate_spellings,
    retired_gate_spellings,
)
from opl.triage_agent.incidents import DQ_GATE_TASK_KEY, TABLE_OF_JOB
from opl.triage_agent.severity import SEVERITIES

# `{{tasks.<task_key>.values.<name>}}`, which is how a Databricks condition task reads a
# value another task published. THE VALUE NAME IS NOT PINNED HERE, on purpose:
# `bad_row_count` is spelled in `databricks/src/dq_gate_batch.py` and in seven YAMLs, and a
# third spelling in a test would be one more copy to go stale. What this lock is about is
# WHICH TASK the condition reads from, so that is what it extracts.
_TASK_VALUE = re.compile(r"^\{\{tasks\.([A-Za-z0-9_]+)\.values\.[A-Za-z0-9_]+\}\}$")

# The module's own file, read as TEXT by the import-time proofs at the bottom. Taken from
# the imported module rather than composed from a repo root, so a moved file cannot leave
# these tests executing something that is no longer the module under test.
_SOURCE = Path(history_module.__file__)


def test_the_history_reads_the_telemetry_view_this_project_deploys():
    """The default relation is the deployed F4 view, so the test seam cannot leak into it.

    `view=` exists for `SystemTables`' reason -- otherwise the query is asserted by nobody
    until a workspace run -- and the same lock applies here as on T1's feed: what deploys
    is spelled from `config` and the view's own constant rather than retyped."""
    assert DEFAULT.table(TASK_TELEMETRY_VIEW) in history_sql()
    assert "workspace.default.dataops_task_telemetry" in history_sql()


def test_the_declared_window_reaches_both_the_row_and_the_predicate_as_one_number():
    """N is declared once and lands in TWO places, so they cannot disagree.

    A row saying `executions_requested = 5` beside a reading computed against 3 would be a
    self-describing row describing itself wrongly, and nothing in the output could show it.
    The second half is the discriminating one: a query built with another window has to
    move BOTH, or this test would pass on a hardcoded predicate."""
    default = history_sql()
    assert f"{N_EXECUTIONS} AS executions_requested" in default
    assert f"prior_executions < {N_EXECUTIONS}" in default

    narrower = history_sql(executions=2)
    assert "2 AS executions_requested" in narrower
    assert "prior_executions < 2" in narrower
    assert f"prior_executions < {N_EXECUTIONS}" not in narrower


@pytest.mark.parametrize(
    "window", [0, -1, True, 1.5, "5", None], ids=["zero", "negative", "bool", "float",
                                                  "string", "none"]
)
def test_a_window_that_is_not_a_positive_whole_number_is_refused(window):
    """N is interpolated into a predicate, so it is checked before it gets there.

    `True` is in the list because `isinstance(True, int)` is True in Python and `True == 1`
    -- a window that passed a truthiness check would silently build "fewer than 1", under
    which ten of this workspace's eleven incidents read as complete."""
    with pytest.raises(ValueError, match="not a positive integer"):
        history_sql(executions=window)


# ----------------------------------------------------------------------------------
# The gate spellings, held against the bundle. No Spark below this line.
# ----------------------------------------------------------------------------------


def _condition_gates_of_bundle(root: Path = RESOURCES) -> dict[str, str]:
    """Every job whose `check_bad_rows` condition task consumes a gate, and which gate.

    READ OFF THE WIRING RATHER THAN OFF A LIST OF KNOWN JOBS, which is 0.5's standard for
    this bundle: a new bronze job carrying a gate is exactly the drift this lock exists to
    catch, and a list would have to be updated by the same person who forgot to.

    FIVE STRUCTURAL FACTS ARE ASSERTED WHILE READING, not merely the name: the condition
    task is declared once, it really is a `condition_task`, its `left` reads another task's
    published value, that task is one this job declares, and the condition DEPENDS ON it.
    The last one is what makes `check_bad_rows` total over gate runs -- a condition task
    that read a value from a task it does not wait for would run on batches whose gate had
    not published, and the identity behind this module's key would stop holding. All five
    are fired, one mutated bundle each, by `test_the_checks_the_sweep_makes_while_reading_
    are_fired_on_the_drift_each_refuses`."""
    found: dict[str, str] = {}
    for path in sorted(root.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job in (document.get("resources", {}).get("jobs", {}) or {}).values():
            tasks = job.get("tasks", [])
            conditions = [t for t in tasks if t.get("task_key") == HISTORY_TASK_KEY]
            if not conditions:
                continue
            assert len(conditions) == 1, f"{path.name} declares {len(conditions)} of them"
            found[job["name"]] = _gate_consumed_by(conditions[0], tasks, path.name)
    return found


def _gate_consumed_by(condition: dict, tasks: list[dict], where: str) -> str:
    """The task key `condition` reads its value from, checked four ways.

    IT REALLY IS A CONDITION TASK -- asserted, and with a message. Until this line existed
    that claim was carried by a bare `KeyError` from the subscript below: the drift was
    caught, but by accident and with nothing for a reader to act on. The other three are
    that it reads ANOTHER TASK's published value, that the task it names is one this job
    declares, and that it DEPENDS ON the one it reads from -- which is what makes
    `check_bad_rows` total over gate runs.

    EACH OF THE FOUR IS FIRED, on a copy of `bronze_payments_job.yml` carrying the drift it
    refuses, by `test_the_checks_the_sweep_makes_while_reading_are_fired_on_the_drift_each_
    refuses`. This docstring said the opposite until that test existed, and it was true when
    it said it: they ran over the real bundle, which satisfies all four, and nothing proved
    they could fail. A check nobody has seen fail is this repository's hunted species."""
    assert "condition_task" in condition, (
        f"{where}'s {HISTORY_TASK_KEY} declares "
        f"{sorted(key for key in condition if key.endswith('_task'))} and no "
        "`condition_task`, so it decides nothing and the gate this lock reads is nobody's"
    )
    reference = condition["condition_task"]["left"]
    match = _TASK_VALUE.match(reference)
    assert match, f"{where}'s condition task reads {reference!r}, not another task's value"
    gate = match.group(1)
    assert gate in {task["task_key"] for task in tasks}, (
        f"{where}'s condition task reads a value from {gate!r}, which the job does not "
        "declare"
    )
    waits_for = {dependency["task_key"] for dependency in condition.get("depends_on", [])}
    assert gate in waits_for, (
        f"{where}'s {HISTORY_TASK_KEY} reads {gate!r}'s value without depending on it, so "
        "it can run on a batch whose gate published nothing"
    )
    return gate


def test_every_job_that_gates_wires_the_history_key_to_a_declared_live_spelling():
    """THE LOCK, AND IT IS EQUALITY IN BOTH DIRECTIONS ON BOTH AXES.

    Jobs: the set of gating jobs the bundle declares equals the set `incidents.TABLE_OF_JOB`
    declares, so a bronze job that gained a gate without a `check_bad_rows` -- or lost its
    condition task in a rename -- fails here in the commit that does it. That equality is
    also the discriminating arm this file cannot do without: every other assertion below is
    satisfied by a reader that found NOTHING, which is exactly how T1's rename lock passed
    on `{}` until the surviving jobs were named.

    Spellings: every gate the bundle's condition tasks consume is declared LIVE here, and
    every LIVE spelling is consumed by at least one of them -- so a third gate spelling
    landing in the bundle fails, and a spelling declared live that no job runs any more
    fails too rather than sitting in the declaration looking like protection."""
    wired = _condition_gates_of_bundle()

    assert set(wired) == set(TABLE_OF_JOB)
    assert set(wired.values()) == set(live_gate_spellings())
    assert live_gate_spellings() == ("dq_gate_batch",)


def _declared_task_keys(root: Path = RESOURCES) -> set[str]:
    """Every task key the bundle declares, over every job of every file under `root`.

    A parameter rather than a constant for `_condition_gates_of_bundle`'s reason: the lock
    below is about a name being ABSENT, and an absence is satisfied by a reader that found
    nothing -- so the same sweep has to be runnable over a bundle that DOES carry the name."""
    keys: set[str] = set()
    for path in sorted(root.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job in (document.get("resources", {}).get("jobs", {}) or {}).values():
            keys |= {task["task_key"] for task in job.get("tasks", [])}
    return keys


def test_a_retired_spelling_is_history_the_bundle_cannot_confirm_and_must_not_carry():
    """The asymmetry, stated: the retired name is in the telemetry and in NO YAML.

    A lock written as "the declaration equals what the bundle says" in both directions
    would fail on `dq_gate` -- which is real history, five job runs the workspace still
    serves under that name -- so the retired half is locked the other way round: it must
    appear nowhere in the bundle. Re-introducing that name as a live task therefore fails
    here, in the commit that does it, rather than quietly meaning two things at once."""
    declared_task_keys = _declared_task_keys()

    assert HISTORY_TASK_KEY in declared_task_keys, "the sweep read no tasks at all"
    assert retired_gate_spellings() == ("dq_gate",)
    assert set(retired_gate_spellings()) & declared_task_keys == set()


def _mutated_bundle(tmp_path: Path, filename: str, old: str, new: str) -> Path:
    """The whole `resources/` directory copied, with one file's substring replaced.

    The DIRECTORY and not the one file, because the reader sweeps a directory and a
    one-file root would fail for the wrong reason -- the mutation would be indistinguishable
    from six jobs having vanished. The replacement is asserted to have applied: a probe that
    silently changed nothing proves the lock catches nothing."""
    root = tmp_path / "resources"
    shutil.copytree(RESOURCES, root)
    target = root / filename
    original = target.read_text(encoding="utf-8")
    drifted = original.replace(old, new)
    assert drifted != original, f"the mutation {old!r} did not apply -- this proves nothing"
    target.write_text(drifted, encoding="utf-8", newline="\n")
    return root


def test_the_lock_catches_a_third_gate_spelling_the_declaration_does_not_know(tmp_path):
    """A gate renamed AGAIN is the measured history of this project, not a hypothesis.

    `dq_gate` became `dq_gate_batch` mid-project and the telemetry marked nothing, which
    cost the lookup's three incidents their whole history under the naive key. The next
    rename fails here instead: the condition task's `left` reference names the new
    spelling, which is not declared, so the spelling equality breaks. What this CANNOT do
    is recover runs already recorded under the old name -- that half is closed by keying
    the history on `check_bad_rows` and by carrying the retired spelling as data."""
    root = _mutated_bundle(tmp_path, "bronze_payments_job.yml", "dq_gate_batch", "dq_gate_v3")

    wired = _condition_gates_of_bundle(root)
    assert wired["opl-bronze-payments"] == "dq_gate_v3"
    assert set(wired) == set(TABLE_OF_JOB), "the reader must still see all seven jobs"
    assert set(wired.values()) != set(live_gate_spellings())


def test_the_lock_catches_the_history_key_renamed_in_the_bundle(tmp_path):
    """The rename that would silently shorten EVERY history in this project.

    If `check_bad_rows` is renamed in the bundle and not here, this module keeps counting a
    task that no longer runs: every incident's baseline decays to whatever the old name
    still has in the timeline and then to zero, with nothing raising. The job drops out of
    the sweep, so the job equality breaks in the commit that does it.

    THE SURVIVING SIX ARE NAMED, because both other assertions here are negative and a
    reader that found nothing satisfies both -- which is precisely how T1's equivalent test
    once passed over `{}`."""
    root = _mutated_bundle(
        tmp_path, "bronze_socios_job.yml", HISTORY_TASK_KEY, "check_bad_rows_v2"
    )

    wired = _condition_gates_of_bundle(root)
    assert wired == {
        job: "dq_gate_batch" for job in TABLE_OF_JOB if job != "opl-bronze-cnpj-socios"
    }
    assert "opl-bronze-cnpj-socios" not in wired
    assert set(wired) != set(TABLE_OF_JOB)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ("- task_key: promote", "- task_key: check_bad_rows", "declares 2 of them"),
        ("condition_task:", "notebook_task:",
         "declares ['notebook_task'] and no `condition_task`"),
        ('left: "{{tasks.dq_gate_batch.values.bad_row_count}}"', 'left: "0"',
         "condition task reads '0', not another task's value"),
        ('left: "{{tasks.dq_gate_batch.values.bad_row_count}}"',
         'left: "{{tasks.dq_gate_v3.values.bad_row_count}}"',
         "reads a value from 'dq_gate_v3', which the job does not declare"),
        ("depends_on: [{ task_key: dq_gate_batch }]", "",
         "reads 'dq_gate_batch''s value without depending on it"),
    ],
    ids=["declared-twice", "not-a-condition-task", "not-a-task-value",
         "gate-the-job-does-not-declare", "reads-without-waiting"],
)
def test_the_checks_the_sweep_makes_while_reading_are_fired_on_the_drift_each_refuses(
    tmp_path, old, new, message
):
    """The five checks the reader makes while sweeping, each on a bundle carrying its drift.

    THEY WERE UNFIRED UNTIL THIS TEST, while this file's header promised the opposite, and
    that is the contradiction closed here: a reader's own assertions are as capable of being
    wrong as the wiring they read, and one nobody has seen fail is indistinguishable from
    one that cannot. Each row is one substitution in `bronze_payments_job.yml` with the
    whole `resources/` tree copied around it, and the MESSAGE is asserted rather than only
    the raise -- so a row cannot pass on a different check firing than the one it names.

    THE FIRST ROW FIRES A CHECK IN THE SWEEP ITSELF and not in `_gate_consumed_by`: two
    `check_bad_rows` tasks in one job would leave the lock resolving the key to an arbitrary
    one of them. The LAST is the one this module rests on -- a condition task that reads a
    gate's value without waiting for it runs on batches whose gate published nothing, and
    `check_bad_rows` stops being total over gate runs, which is the identity behind the
    history key."""
    root = _mutated_bundle(tmp_path, "bronze_payments_job.yml", old, new)

    with pytest.raises(AssertionError) as refused:
        _condition_gates_of_bundle(root)

    assert message in str(refused.value)


def test_the_retired_half_of_the_lock_catches_the_old_name_re_introduced_as_a_task(tmp_path):
    """The other direction of the lock, fired: `dq_gate` back in a YAML as a live task.

    `test_a_retired_spelling_is_history_the_bundle_cannot_confirm_and_must_not_carry` holds
    that the retired name appears in NO job YAML -- an ABSENCE, which is the shape a reader
    that found nothing satisfies. That test's own first assertion covers the reader finding
    nothing; what it had no input for is a bundle that DOES carry the name. This is that
    input. The drift is a revert away from real history: `dq_gate` is the name the LOOKUP's
    gate ran under until mid-project, which is why it is carried as data and not deleted --
    and the job mutated here is not that job, because the lock is about the whole bundle."""
    root = _mutated_bundle(
        tmp_path, "bronze_payments_job.yml", "- task_key: dq_gate_batch", "- task_key: dq_gate"
    )
    declared = _declared_task_keys(root)

    assert HISTORY_TASK_KEY in declared, "the sweep must still see the surviving tasks"
    assert set(retired_gate_spellings()) & declared == {"dq_gate"}


# ----------------------------------------------------------------------------------
# The import-time guards, each fired on the declaration it refuses.
# ----------------------------------------------------------------------------------


def test_the_declaration_is_two_spellings_one_live_and_one_retired():
    """The readable statement of the contract -- and TWO of its three lines can FAIL.

    This docstring used to say the whole test restated refusals that already ran at import
    and therefore could not fail here. That is true of the LAST line only. The first two are
    among the few things holding the RETIRED entry in place: delete `dq_gate` from the
    declaration and both go red, taking `test_a_retired_spelling_is_history_the_bundle_
    cannot_confirm_and_must_not_carry` with them; declare a third spelling LIVE and both go
    red again, alongside the bundle lock. Only `gate.why.strip()` restates
    `_assert_every_gate_spelling_declares_a_status_and_a_reason`, which has already run.

    DESCRIBING A LIVE GUARD AS INERT IS AN INVITATION TO DELETE IT, which is the inverse of
    the species this repository hunts, so the sentence is corrected rather than softened.
    What the mutations underneath prove is a different thing: that each import-time guard
    CAN fail, and that it is ASKED. No assertion here shows either."""
    assert set(GATE_SPELLINGS) == {"dq_gate", "dq_gate_batch"}
    assert live_gate_spellings() + retired_gate_spellings() == ("dq_gate_batch", "dq_gate")
    assert all(gate.why.strip() for gate in GATE_SPELLINGS.values())


@pytest.mark.parametrize(
    ("spelling", "message"),
    [
        (GateSpelling(status="deprecated", why="a third word"), "neither"),
        (GateSpelling(status=LIVE, why="   "), "declares no reason"),
    ],
    ids=["unknown-status", "empty-reason"],
)
def test_the_status_guard_catches_a_spelling_no_direction_of_the_lock_covers(
    monkeypatch, spelling, message
):
    """Proves the first guard can FAIL, in both shapes it refuses.

    A third status is the dangerous one: the bundle lock holds one direction for LIVE and
    the other for RETIRED, so a spelling wearing neither word is present in the declaration
    and checked by NOTHING -- protection that reads as protection and is not."""
    monkeypatch.setattr(history_module, "GATE_SPELLINGS", {**GATE_SPELLINGS, "x": spelling})
    with pytest.raises(ValueError, match=message):
        _assert_every_gate_spelling_declares_a_status_and_a_reason()


def test_the_live_guard_catches_a_declaration_the_bundle_lock_would_range_over_emptily(
    monkeypatch,
):
    """Proves the guard-the-guard can FAIL, and that the state it refuses is invisible.

    With every spelling retired, `test_every_job_that_gates_wires_the_history_key_to_a_
    declared_live_spelling` compares the bundle's gates against an empty set -- and the
    FIRST half of it, the job equality, still passes, which is why the emptiness has to be
    refused at import rather than left to that test to notice."""
    retired_only = {
        name: GateSpelling(status=RETIRED, why=gate.why)
        for name, gate in GATE_SPELLINGS.items()
    }
    monkeypatch.setattr(history_module, "GATE_SPELLINGS", retired_only)

    assert live_gate_spellings() == ()
    with pytest.raises(ValueError, match="no gate spelling is declared live"):
        _assert_a_live_gate_spelling_exists()


def test_the_role_guard_catches_the_history_key_being_one_of_the_things_it_counts(
    monkeypatch,
):
    """Proves the third guard can FAIL, in both directions it refuses.

    The gate arm: if the history key were also a declared gate spelling, the identity that
    justifies it -- 5 + 24 = 29 -- would be a set compared with itself. The incident arm: if
    the history key were `fail_on_dq`, `prior_incidents` would equal `prior_executions` on
    every input, and the terminal-state defect this module is built against would be
    indistinguishable from the fix."""
    monkeypatch.setattr(
        history_module,
        "GATE_SPELLINGS",
        {**GATE_SPELLINGS, HISTORY_TASK_KEY: GateSpelling(status=LIVE, why="drift")},
    )
    with pytest.raises(ValueError, match="a declared gate spelling"):
        _assert_the_task_keys_are_three_different_roles()

    monkeypatch.setattr(history_module, "GATE_SPELLINGS", GATE_SPELLINGS)
    monkeypatch.setattr(history_module, "HISTORY_TASK_KEY", DQ_GATE_TASK_KEY)
    with pytest.raises(ValueError, match="both the history key and the incident key"):
        _assert_the_task_keys_are_three_different_roles()


@pytest.mark.parametrize(
    ("readings", "message"),
    [
        (("history_complete", "history_complete"), "spells a word twice"),
        ((*HISTORY_READINGS, SEVERITIES[0]), "answer two questions"),
    ],
    ids=["duplicate", "borrowed-from-severity"],
)
def test_the_vocabulary_guard_catches_a_word_that_answers_two_questions(
    monkeypatch, readings, message
):
    """Proves the fourth guard can FAIL, within this file and across the row's neighbours.

    A reading that collided with a severity would put one string on a row that carries
    both columns, and a renderer could not say which question it answered. That is
    `severity._assert_no_grade_is_spelled_twice`'s requirement, and the borrowed word here
    is taken FROM `severity.SEVERITIES` rather than typed, so the test cannot drift off the
    vocabulary it is about."""
    monkeypatch.setattr(history_module, "HISTORY_READINGS", readings)
    with pytest.raises(ValueError, match=message):
        _assert_no_reading_word_is_another_modules()


def _executed(path: Path):
    """`path` executed as a throwaway module, registered and then removed from `sys.modules`.

    Not `importlib.reload`, which would rebind the module every other test imported from.
    REGISTERED UNDER A THROWAWAY NAME AND REMOVED AGAIN, for the reason
    `test_severity_declaration.py`'s sibling gives: this module declares a `@dataclass`
    under `from __future__ import annotations`, so `dataclasses` resolves the string
    annotations by looking the defining class's `__module__` up in `sys.modules` and raises
    `AttributeError` on a module that is not there. Measured here before it was read
    there."""
    spec = importlib.util.spec_from_file_location(
        "opl.triage_agent._history_reimported", path
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        del sys.modules[spec.name]
    return module


def _reimported_history():
    """A SECOND execution of `history.py`'s own body, unchanged."""
    return _executed(_SOURCE)


def _reimported_with(tmp_path: Path, old: str, new: str):
    """The module's SOURCE mutated by one substitution, then executed.

    THE TWO GUARDS BELOW HAVE NO EXTERNAL HOOK AND THIS IS THE ONLY HONEST WAY TO FIRE
    THEM AT IMPORT. `monkeypatch` reaches the third and fourth calls because those read a
    name from another module (`incidents`, `reconcile`) that the body re-imports; the first
    two range over `GATE_SPELLINGS`, which the body DEFINES, so nothing outside can change
    what they see. Mutating the source is `test_incidents_declaration.py`'s YAML-copy
    technique applied to a Python file, and the substitution is asserted to be unique and
    to have applied -- a probe that changed nothing proves nothing.

    THE UNIQUENESS ASSERTION IS KEPT AND IT IS NOT FREE. `status=LIVE,` stops being unique
    the moment a SECOND live spelling is declared, and both callers below would then fail on
    a naming message rather than on the thing they assert -- an unrelated change reddening
    two guard tests. That is ruled the better failure: it is loud, it names the substring
    that stopped being unique, and it costs one line to repair. The alternative, a
    positional or regex substitution, stays green while mutating whichever entry happened to
    come first, which is a probe that proves something about a row nobody chose."""
    original = _SOURCE.read_text(encoding="utf-8")
    assert original.count(old) == 1, f"{old!r} appears {original.count(old)} times, not once"
    copy = tmp_path / "history_mutated.py"
    copy.write_text(original.replace(old, new), encoding="utf-8", newline="\n")
    return _executed(copy)


def test_the_FIRST_guard_runs_at_import_so_deleting_the_call_is_a_silent_loss_no_more(
    tmp_path,
):
    """The half every `pytest.raises` sibling above leaves open.

    Each guard is proven able to fail; none of those proves it is ever ASKED. T1 measured
    that gap in its own file -- deleting both import-time calls left the suite GREEN -- so
    each of the four calls at the bottom of `history.py` gets a test that only IT reddens.

    THIS ONE'S MUTATION IS DISCRIMINATED BY ITS MESSAGE AND NOT ONLY BY THE RAISE. A
    spelling wearing an unknown status is also invisible to `live_gate_spellings()`, so the
    SECOND guard would raise on this same source -- with a different sentence. Matching on
    `neither` is what makes deleting the first call red here rather than green.

    The first line is the control: an UNMUTATED re-execution must succeed, or the raise
    could be about the re-execution rather than about the declaration."""
    assert _reimported_history().HISTORY_READINGS == HISTORY_READINGS

    with pytest.raises(ValueError, match="neither"):
        _reimported_with(tmp_path, "status=LIVE,", 'status="deprecated",')


def test_the_SECOND_guard_runs_at_import_and_refuses_a_lock_with_nothing_to_range_over(
    tmp_path,
):
    """The declaration in which every spelling is retired, executed rather than described.

    The first guard passes on this source -- `retired` is a known status -- so only the
    second can raise, and it must, because the bundle lock's LIVE direction would otherwise
    hold vacuously over the empty set."""
    with pytest.raises(ValueError, match="no gate spelling is declared live"):
        _reimported_with(tmp_path, "status=LIVE,", "status=RETIRED,")


def test_the_THIRD_guard_runs_at_import_and_is_fired_from_the_module_it_reads(monkeypatch):
    """The role collision, fired from `incidents.py`, which is where it would come from.

    This guard refuses the history key being one of the task keys it is supposed to be
    total over, and the way that ships is a rename one module away. `history.py` reads
    `DQ_GATE_TASK_KEY` at import, so re-executing its body against a renamed one is exactly
    the commit that would make `prior_incidents` equal `prior_executions` on every input."""
    assert _reimported_history().HISTORY_TASK_KEY == HISTORY_TASK_KEY

    monkeypatch.setattr(incidents_module, "DQ_GATE_TASK_KEY", HISTORY_TASK_KEY)
    with pytest.raises(ValueError, match="both the history key and the incident key"):
        _reimported_history()


def test_the_FOURTH_guard_runs_at_import_and_is_fired_from_the_module_it_reads(monkeypatch):
    """The word collision, fired from `reconcile.py`, for T3's reason at one more remove.

    A verdict renamed onto one of these readings would put a single string on a row that
    carries both columns. `history.py` reads the four verdict names at import, so the
    rename that would cause it is the input this test supplies."""
    monkeypatch.setattr(reconcile_module, "RECONCILED", HISTORY_READINGS[0])
    with pytest.raises(ValueError, match="answer two questions"):
        _reimported_history()
