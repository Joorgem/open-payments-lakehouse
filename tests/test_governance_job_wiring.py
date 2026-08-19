# tests/test_governance_job_wiring.py
"""What `dataops_views_job.yml` HANDS its tasks, and in what order.

A THIRD YAML LOCK FILE, AND THE REASON IT IS NOT IN EITHER OF THE OTHER TWO IS THE
SAME REASON IT IS NEEDED AT ALL. `test_job_yaml_wiring.py` asks which table a job
hands its tasks and is parametrized over `JOB_OF` -- the seven INGESTION jobs, one per
registered table. `test_job_yaml_launch_guards.py` asks what refuses a run at launch
and is total over every YAML, which is how the governance job is guarded at all. The
governance job is the one job that hands a task a TABLE while being in neither
parametrization: the paste lock never opens this file, and neither does
`test_only_the_masked_table_creates_its_bronze_table_up_front_and_does_it_first`,
whose two directions are both statements about a table's own ingestion job.

It is a separate FILE rather than five more functions in the 660-line wiring module
for the reason every split in this repository has been done for: that file is 140
lines from a cap this phase has already split three files at, and `test_gold_job_
wiring.py` and `test_vault_job_wiring.py` are the precedent for one job family, one
file.

WHY THE JOB CARRIES A MASKING TASK AT ALL (F4 Task 5b). `ensure_masked_table` is the
only task in this repository that issues `CREATE OR REPLACE FUNCTION` for the socios
column mask, so F4's repair of the PREDICATE -- `is_member('opl_pii_readers')`, for an
`is_account_group_member` that no group creatable from this workspace can make return
true -- could otherwise reach Unity Catalog only by re-running the socios INGESTION
flow, whose `unzip` re-extracts the zips and re-lands the 2,852,557,826 B the same
phase reclaimed. The governance job runs no unzip, already carries the revision guard,
and needs no new YAML file to be classified in the launch-guard lists.

WHAT THAT COSTS IS A COORDINATE, WHICH IS WHAT THIS FILE IS FOR. Every other task in
that job is parameterless and total over a registry. This one takes a table, and every
wrong value it could carry is the name of a table that EXISTS and declares no mask --
so the task prints "declares no masked column", returns, and the run is GREEN with the
predicate unrepaired. That is the ingestion jobs' paste defect, in a file the paste
lock does not read.

AND THE JOB CARRIES A CHECK ON WHAT THAT TASK LEAVES BEHIND (F4's correction pass).
`ensure_masked_table` running is not evidence that the predicate changed: the safety
check this job published -- the masked column reads `***` afterwards -- is true under the
repair, true without it, and true when the task returned early, because
`opl_pii_readers` is empty by decision and both spellings therefore take the same `ELSE`
branch. `assert_mask_predicate` reads `information_schema.routines` instead, and this
file pins that it runs AFTER the repair and does not gate the revokes.

The argv contract itself is not re-asserted here: `test_job_yaml_wiring.py` reads it
out of the script, and its paste lock fails on the socios ingestion job the moment
`ensure_masked_table` stops taking a table as its first argument.

Nothing here starts Spark and nothing here loads a job script: every assertion is
about wiring, not data."""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from job_yaml import (
    REPO,
    RESOURCES,
    REVISION_GUARD,
    ancestors,
    job_of,
    mutated,
    script_of,
    tasks_of,
)

from opl.bronze.masking import MASKED_COLUMNS
from opl.bronze.registry import REGISTRY, table_spec

GOVERNANCE_JOB = "dataops_views_job.yml"
MASK_TASK = "ensure_masked_table"
GOVERNANCE_TASK = "apply_pii_governance"
PREDICATE_TASK = "assert_mask_predicate"


def _tables_declaring_a_mask() -> set[str]:
    """Registered tables whose CONTRACT declares a masked column.

    Derived rather than listed, from the same two modules the task derives it from:
    `MASKED_COLUMNS` is keyed by contract so the mask follows the data, and a table's
    contract comes from the registry. A list typed here would be a third spelling of
    something that already has two, and it would go stale in the silent direction."""
    return {
        table for table in REGISTRY if MASKED_COLUMNS.get(table_spec(table).contract, ())
    }


def _mask_tasks_of(root: Path) -> dict[str, list[str]]:
    """Every task of the governance job that runs `ensure_masked_table`, and what it
    is handed.

    Read by SCRIPT rather than by task key -- a second mask task under another key is
    still a mask task, and the set below has to see it. Keyed by task key so a failure
    message can name the offender."""
    return {
        key: task["spark_python_task"]["parameters"]
        for key, task in tasks_of(GOVERNANCE_JOB, root).items()
        if "spark_python_task" in task
        and script_of(task, f"{GOVERNANCE_JOB}:{key}") == MASK_TASK
    }


def _assert_the_governance_job_masks_every_masked_table(root: Path = RESOURCES) -> None:
    handed = _mask_tasks_of(root)
    surplus = {key: given for key, given in handed.items() if len(given) != 1}
    assert not surplus, (
        f"{GOVERNANCE_JOB} hands {surplus} to {MASK_TASK}, which takes exactly one "
        "argument -- the table. Anything after it is read by nothing, so a second value "
        "here is a paste no run will ever complain about"
    )
    named = {given[0] for given in handed.values()}
    assert named == _tables_declaring_a_mask(), (
        f"{GOVERNANCE_JOB} runs {MASK_TASK} for {sorted(named)}, and the registered "
        f"tables whose contract declares a mask are {sorted(_tables_declaring_a_mask())}. "
        "A masked table missing here has no path to the mask DDL except its own ingestion "
        "job, whose unzip re-lands the files F4 reclaimed; a table named here that "
        "declares no mask makes the task a no-op and the run GREEN with nothing masked"
    )


def _assert_the_mask_repair_is_not_behind_the_task_that_raises(
    root: Path = RESOURCES,
) -> None:
    assert MASK_TASK in ancestors(tasks_of(GOVERNANCE_JOB, root), GOVERNANCE_TASK), (
        f"{GOVERNANCE_JOB}: {GOVERNANCE_TASK} no longer waits for {MASK_TASK}. The edge "
        "points this way because the governance task RAISES UngovernedRead on a read it "
        "cannot withdraw, and nothing downstream of a raise runs -- so reversed, an ALL "
        "PRIVILEGES holder would block the mask repair, which is the control that hides "
        "those names from exactly that reader"
    )


def test_the_governance_job_masks_every_masked_table_and_hands_it_nothing_else():
    """THE COORDINATE THE PASTE LOCK CANNOT SEE, because it is not in an ingestion job.

    Both directions, and both are silent. A masked table missing from this job leaves
    the mask DDL reachable only by the ingest flow that re-lands 2.8 GB; a table named
    here that declares no mask is a task that returns having issued nothing, in a run
    that reports SUCCESS."""
    _assert_the_governance_job_masks_every_masked_table()


def test_the_mask_repair_runs_ahead_of_the_governance_task_that_can_fail_the_run():
    """The one edge inside that job, in the only direction that keeps the repair
    reachable.

    `apply_pii_governance` ends by raising on any read it could not withdraw -- an
    `ALL PRIVILEGES` grant, or a `SELECT` inherited from the schema. Behind it, the mask
    repair would be skipped in exactly the runs where somebody can read those tables."""
    _assert_the_mask_repair_is_not_behind_the_task_that_raises()


def test_the_governance_mask_lock_catches_the_table_name_a_paste_would_leave(tmp_path):
    """Proves the set equality can fail, in the shape that is silent.

    `estabelecimentos` is a real registered table that declares no masked column, so the
    task prints "declares no masked column", returns, and the run is GREEN -- with the
    deployed mask still carrying the predicate this workspace cannot open."""
    root = mutated(
        GOVERNANCE_JOB,
        tmp_path,
        'parameters: ["socios"]',
        'parameters: ["estabelecimentos"]',
    )
    with pytest.raises(AssertionError, match="GREEN with nothing masked"):
        _assert_the_governance_job_masks_every_masked_table(root=root)


def test_the_governance_mask_lock_catches_a_second_argument_the_task_would_ignore(
    tmp_path,
):
    """Proves the OTHER assertion can fail. `ensure_masked_table` resolves `args[0]` and
    reads nothing else, so a month or a revision pasted in beside the table changes
    nothing at run time and is invisible everywhere except here."""
    root = mutated(
        GOVERNANCE_JOB,
        tmp_path,
        'parameters: ["socios"]',
        'parameters: ["socios", "{{job.parameters.revision}}"]',
    )
    with pytest.raises(AssertionError, match="takes exactly one"):
        _assert_the_governance_job_masks_every_masked_table(root=root)


def test_the_edge_lock_catches_the_mask_repair_moved_off_the_governance_task(tmp_path):
    """Proves the edge assertion can fail, in the direction that would look tidy.

    Re-pointed at the revision guard, the two tasks run in parallel off it: the ordering
    stops being a property of the YAML and becomes whatever the scheduler did, and every
    run in which `apply_pii_governance` raises is a run whose mask repair may or may not
    have happened.

    THE MUTATION NAMES THE TASK AND NOT ONLY THE EDGE, because two tasks now declare
    `depends_on: [{ task_key: ensure_masked_table }]` and `mutated` replaces the first
    occurrence. Targeting the edge alone would re-point the PREDICATE CHECK, leave this
    assertion true, and report a probe that proves nothing."""
    root = mutated(
        GOVERNANCE_JOB,
        tmp_path,
        f"- task_key: {GOVERNANCE_TASK}\n"
        f"          depends_on: [{{ task_key: {MASK_TASK} }}]",
        f"- task_key: {GOVERNANCE_TASK}\n"
        f"          depends_on: [{{ task_key: {REVISION_GUARD} }}]",
    )
    with pytest.raises(AssertionError, match="no longer waits for"):
        _assert_the_mask_repair_is_not_behind_the_task_that_raises(root=root)


# --------------------------------------------------------------------------
# The deployed predicate, which is the one thing this job's own run could not tell
# --------------------------------------------------------------------------


def _predicate_tasks_of(root: Path) -> set[str]:
    """Every task of this job that reads the deployed mask predicate back.

    By SCRIPT, like the mask tasks above: a check under another task key is still the
    check, and one that has been re-pointed at another entry point is not."""
    return {
        key
        for key, task in tasks_of(GOVERNANCE_JOB, root).items()
        if "spark_python_task" in task
        and script_of(task, f"{GOVERNANCE_JOB}:{key}") == PREDICATE_TASK
    }


def _assert_the_deployed_predicate_is_read_back_after_the_repair(
    root: Path = RESOURCES,
) -> None:
    tasks = tasks_of(GOVERNANCE_JOB, root)
    named = _predicate_tasks_of(root)
    assert named, (
        f"{GOVERNANCE_JOB} runs no {PREDICATE_TASK}, so nothing in the run asserts which "
        "predicate the deployed mask function actually carries. The check this job used "
        "to publish -- that the masked column reads `***` afterwards -- is true under the "
        "repair, without it, and when the mask task returned early, because the reader "
        "group is empty by decision: one branch, and it cannot be taken"
    )
    for key in sorted(named):
        assert MASK_TASK in ancestors(tasks, key), (
            f"{GOVERNANCE_JOB}:{key} does not wait for {MASK_TASK}, so it reads the "
            "routine definition that was there BEFORE the CREATE OR REPLACE FUNCTION "
            "meant to repair it -- an assertion about the previous deploy, reported "
            "against this one"
        )


def _assert_the_predicate_check_does_not_gate_the_revokes(
    root: Path = RESOURCES,
) -> None:
    tasks = tasks_of(GOVERNANCE_JOB, root)
    assert PREDICATE_TASK not in ancestors(tasks, GOVERNANCE_TASK), (
        f"{GOVERNANCE_JOB}: {GOVERNANCE_TASK} now waits for {PREDICATE_TASK}, so a stale "
        "deployed predicate stops the REVOKES. That is the wrong direction for the same "
        "reason the edge above points the way it does: this check can only report, every "
        "statement the governance task issues either withdraws a read or writes an inert "
        "tag, and a failure state must not disable its own mitigation"
    )


def test_the_deployed_predicate_is_asserted_by_a_task_that_runs_after_the_repair():
    """THE NINTH INSTANCE OF THIS PHASE'S SECOND SPECIES, closed where it can fail.

    Nothing in this repository read `information_schema.routines` -- one grep hit, a
    comment -- so the body of the function four column masks resolve to was known only
    for as long as somebody remembered to look. It was wrong for three months."""
    _assert_the_deployed_predicate_is_read_back_after_the_repair()


def test_the_predicate_check_reports_and_never_gates_the_governance_task():
    """The placement, asserted rather than left to whoever edits the YAML next. The
    check can only report; the governance task tightens. Behind it, a run that could not
    prove which predicate is deployed would also decline to remove a reader."""
    _assert_the_predicate_check_does_not_gate_the_revokes()


def test_the_predicate_lock_catches_the_check_re_pointed_at_another_entry_point(tmp_path):
    """Proves the presence assertion can fail, in the shape that stays green everywhere
    else: the task key survives, the `depends_on` survives, and the run does a second
    thing it already did instead of the one observation that discriminates."""
    root = mutated(
        GOVERNANCE_JOB,
        tmp_path,
        f"python_file: ../src/{PREDICATE_TASK}.py",
        "python_file: ../src/create_dataops_views.py",
    )
    with pytest.raises(AssertionError, match="asserts which predicate"):
        _assert_the_deployed_predicate_is_read_back_after_the_repair(root=root)


def test_the_predicate_lock_catches_the_check_reading_before_the_repair(tmp_path):
    """Proves the ORDERING half can fail. Re-pointed at the revision guard, the check
    races the `CREATE OR REPLACE FUNCTION` it exists to verify -- and the run it reports
    on is the previous one. `mutated` replaces the first occurrence, which is this
    task's own edge; the governance task's is asserted separately above."""
    root = mutated(
        GOVERNANCE_JOB,
        tmp_path,
        f"depends_on: [{{ task_key: {MASK_TASK} }}]",
        f"depends_on: [{{ task_key: {REVISION_GUARD} }}]",
    )
    with pytest.raises(AssertionError, match="does not wait for"):
        _assert_the_deployed_predicate_is_read_back_after_the_repair(root=root)


def test_the_placement_lock_catches_the_check_moved_in_front_of_the_revokes(tmp_path):
    """Proves the placement assertion can fail, in the direction that would look like
    tightening: the governance task waits for the check, and a run that cannot prove the
    deployed predicate now also leaves an out-of-band reader in place."""
    root = mutated(
        GOVERNANCE_JOB,
        tmp_path,
        f"- task_key: {GOVERNANCE_TASK}\n"
        f"          depends_on: [{{ task_key: {MASK_TASK} }}]",
        f"- task_key: {GOVERNANCE_TASK}\n"
        f"          depends_on: [{{ task_key: {PREDICATE_TASK} }}]",
    )
    with pytest.raises(AssertionError, match="stops the REVOKES"):
        _assert_the_predicate_check_does_not_gate_the_revokes(root=root)


# --------------------------------------------------------------------------
# What the YAML's own prose says a test holds
# --------------------------------------------------------------------------

# Either a cited tests file or a cited test name, whichever comes first. ONE pattern with
# an alternation rather than two passes, because `tests/bronze/test_masking.py` contains
# `test_masking` and a second pass would read the filename as a function name and go red
# on a citation that is correct.
_CITATION = re.compile(r"tests/[\w/]+\.py|\btest_\w+\b")


def _assert_every_test_this_yaml_cites_is_where_it_says(root: Path = RESOURCES) -> None:
    """A test name in a header belongs to the file cited before it, or to nothing."""
    text = (root / GOVERNANCE_JOB).read_text(encoding="utf-8")
    cited: str | None = None
    checked = 0
    for token in (match.group(0) for match in _CITATION.finditer(text)):
        if token.endswith(".py"):
            cited = token
            assert (REPO / cited).exists(), (
                f"{GOVERNANCE_JOB} cites {cited}, which is not a file in this repository"
            )
            continue
        assert cited is not None, (
            f"{GOVERNANCE_JOB} names {token} with no tests file cited before it, so a "
            "reader has nowhere to go and this lock has nothing to check it against"
        )
        source = (REPO / cited).read_text(encoding="utf-8")
        assert f"def {token}(" in source, (
            f"{GOVERNANCE_JOB} says {cited} holds {token}, and it does not. A header that "
            "sends a reviewer to the wrong file is worse than one that cites nothing: the "
            "file it names is the one they will conclude is policing this coordinate"
        )
        checked += 1
    assert checked, "no (file, test) citation was checked; the pattern matches nothing"


def test_every_test_this_jobs_header_cites_is_defined_in_the_file_it_names():
    """THE HEADER POINTED AT A FILE THAT COULD NOT HOLD THE TEST, and said so itself.

    Until F4's correction pass this YAML credited `test_job_yaml_wiring.py` with
    `test_the_governance_job_masks_every_masked_table_and_hands_it_nothing_else`, which
    lives here -- in a module whose own docstring argues at length that
    `test_job_yaml_wiring.py` never opens this YAML. So the header sent a reviewer to the
    one file that by its own argument cannot be policing the coordinate this job's single
    parameterised task carries.

    THE OTHER HALF OF THAT CORRECTION IS NOT LOCKABLE AND THAT IS WORTH SAYING: the same
    header claimed `ensure_masked_table` "issues DDL that only ever tightens", which is a
    prose argument about a statement's effect. No test can hold that. What a test CAN hold
    is that a citation resolves, which is the half that goes stale silently."""
    _assert_every_test_this_yaml_cites_is_where_it_says()


def test_the_citation_lock_catches_the_file_that_cannot_hold_the_test(tmp_path):
    """Proves it can fail, by restoring the exact defect an external review found."""
    root = mutated(
        GOVERNANCE_JOB,
        tmp_path,
        "`tests/test_governance_job_wiring.py`'s",
        "`tests/test_job_yaml_wiring.py`'s",
    )
    with pytest.raises(AssertionError, match="and it does not"):
        _assert_every_test_this_yaml_cites_is_where_it_says(root=root)


# --------------------------------------------------------------------------
# The per-task pins the other three job families each lock for their own
# --------------------------------------------------------------------------


def test_every_task_of_this_job_runs_unretried_in_the_declared_environment():
    """THE FOURTH SPELLING OF AN ASSERTION THREE OTHER FILES MAKE, and this job was in
    none of their parametrizations.

    `test_job_yaml_wiring.py` is parametrized over `JOB_OF` -- the seven ingestion jobs;
    `test_gold_job_wiring.py` and `test_vault_job_wiring.py` over their own families. The
    launch-guard file IS total over every YAML, and it asserts `max_retries: 0` on the
    GUARD task alone and environment-key equality without the version or the dependency.
    So the three tasks this job had before F4 were unpinned on all three, and the two it
    gained would have been too. It lives HERE rather than in a fifth parametrization for
    the reason this file exists at all: one job family, one file.

    Neither pin is decoration. A retry is a SECOND run of a task under the same
    `{{job.run_id}}`, and `environment_version: "3"` is the one serverless client version
    this wheel installs under at all -- version 2's Python 3.11.10 rejects it."""
    environments = {
        environment["environment_key"]: environment["spec"]
        for environment in job_of(GOVERNANCE_JOB)["environments"]
    }
    for key, task in tasks_of(GOVERNANCE_JOB).items():
        if "spark_python_task" not in task:
            continue
        where = f"{GOVERNANCE_JOB}:{key}"
        assert task.get("max_retries") == 0, f"{where} does not declare max_retries: 0"
        assert task.get("environment_key") in environments, (
            f"{where} names environment {task.get('environment_key')!r}, which this job "
            f"does not declare ({sorted(environments)})"
        )
        spec = environments[task["environment_key"]]
        assert spec["environment_version"] == "3"
        assert spec["dependencies"] == ["../../dist/*.whl"]
