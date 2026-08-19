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

The argv contract itself is not re-asserted here: `test_job_yaml_wiring.py` reads it
out of the script, and its paste lock fails on the socios ingestion job the moment
`ensure_masked_table` stops taking a table as its first argument.

Nothing here starts Spark and nothing here loads a job script: every assertion is
about wiring, not data."""
from __future__ import annotations

from pathlib import Path

import pytest
from job_yaml import RESOURCES, REVISION_GUARD, ancestors, mutated, script_of, tasks_of

from opl.bronze.masking import MASKED_COLUMNS
from opl.bronze.registry import REGISTRY, table_spec

GOVERNANCE_JOB = "dataops_views_job.yml"
MASK_TASK = "ensure_masked_table"
GOVERNANCE_TASK = "apply_pii_governance"


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
    have happened."""
    root = mutated(
        GOVERNANCE_JOB,
        tmp_path,
        f"depends_on: [{{ task_key: {MASK_TASK} }}]",
        f"depends_on: [{{ task_key: {REVISION_GUARD} }}]",
    )
    with pytest.raises(AssertionError, match="no longer waits for"):
        _assert_the_mask_repair_is_not_behind_the_task_that_raises(root=root)
