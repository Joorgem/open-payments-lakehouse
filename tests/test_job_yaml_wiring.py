"""Which table each job YAML HANDS its tasks.

SPLIT OUT OF `test_task_wiring.py`, which crossed this project's 800-line file
limit when F1.4b added the empresas and socios jobs. The seam is not the line
count: it is the one that file's own docstring already named as its TWO HALVES.
Everything left there reads the SCRIPTS under `databricks/src` and pins each one
against the spec it resolved from argv; everything here reads the YAML under
`databricks/resources` and pins which spec each job hands it. Two reasons to edit
are two files -- the half left behind changes when an ENTRY POINT changes, and this
one changes when a JOB is added, which is the axis this phase has been growing
along.

NEITHER HALF IS A WIRING CLAIM ALONE, which is why each file's docstring points at
the other. A script that resolves its spec perfectly still ingests whatever table
its job YAML hands it, and nothing on the other side of the seam can see the
argument that resolution starts from -- that argument is a literal in a YAML file
written by copying the previous table's. `bronze_ingest.py` handed
"estabelecimentos" by the empresas job reads estabelecimentos' landing dir under
estabelecimentos' checkpoint and writes estabelecimentos' staging, with every test
in `test_task_wiring.py` green. And the run does not error: it SUCCEEDS, having
done the wrong thing.

AND IT HAS BEEN SPLIT AGAIN, BY F-API TASK 2, AT 781 OF 800 LINES. Protocol §4.9 and
§4.12 say whoever touches a file at the cap splits it first, and this phase's two
list entries run six and nine comment lines in this file's house style. The new seam
is the one this file's own body already drew with a section header: everything above
it asks WHICH TABLE a job hands its tasks, and the deployed-revision block below it
asked what refuses a run at LAUNCH -- a wheel from another revision, or a `month`
nobody passed. That half is now `test_job_yaml_launch_guards.py`.

THE READERS WENT TO `tests/job_yaml.py` AND WERE NOT COPIED, which is the one way
this split differs from the last. That one had no shared helper and its docstring
says so; this seam cuts through every YAML reader here, so they are extracted into a
module that holds no test at all. See that file for why it is not imported from this
one.

AND ONE JOB THAT HANDS A TASK A TABLE IS NOT IN THIS FILE'S PARAMETRIZATION AT ALL.
Every lock below iterates `JOB_OF`, which maps a registered table to its INGESTION
job. F4 Task 5b put `ensure_masked_table` in `dataops_views_job.yml` as well -- the
only path by which the repaired mask predicate reaches the workspace without an unzip
that re-lands what F4 reclaimed -- so that job now spells a table name the paste lock
here never reads. `tests/test_governance_job_wiring.py` is what reads it.

Nothing here starts Spark and nothing here loads a job script: every assertion is
about wiring, not data."""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from job_yaml import (
    JOB_OF,
    RESOURCES,
    REVISION_GUARD,
    SRC,
    ancestors,
    job_of,
    mutated,
    script_of,
    tasks_of,
)

from opl.bronze.masking import MASKED_COLUMNS
from opl.bronze.registry import LANDING_ZIPS, REGISTRY, table_spec
from opl.contracts import payments
from opl.generator.profiles import PROFILES

# The `argv:` line every entry point's docstring ends with. Read rather than
# restated, see `_first_argument_of`.
_ARGV_LINE = re.compile(r"^argv: \[([^\]]*)\]", re.M)

# The task value the condition task branches on, and the task that publishes it.
_GATE_VALUE = re.compile(r"^\{\{tasks\.([\w-]+)\.values\.bad_row_count\}\}$")

# The `profile=<key>` entries `bronze_payments_job.yml`'s header documents its one
# operator-facing parameter with. Anchored so the entry must OPEN a comment line -- three
# of the five carry their description on the same line and two do not, so neither
# `$`-anchoring nor a bare `in text` would read the list -- and the key is captured whole,
# which is what lets the check below see a SURPLUS entry rather than only a missing one.
_PROFILE_LINE = re.compile(r"^#\s+profile=([\w-]+)(?:\s|$)", re.M)


def _first_argument_of(script: str) -> str:
    """What the entry point's OWN argv contract says its first argument is.

    Read out of the script rather than restated as a list in this file: a list here
    would be a second spelling of each entry point's contract, and the copy that
    goes stale is the one no job run ever executes. `bronze_lookup_ingest` is the
    one entry point that takes no table -- it routes six differently-named files
    into ONE table by filename suffix -- and it says so on this same line.

    Exactly one such line is required. With none, this helper would classify every
    task as taking no table and the lock below would pass every job YAML without
    ever comparing a parameter."""
    source = (SRC / f"{script}.py").read_text(encoding="utf-8")
    found = _ARGV_LINE.findall(source)
    assert len(found) == 1, (
        f"{script}.py declares {len(found)} `argv: [...]` lines, expected exactly 1 -- "
        "this lock reads that line to learn whether the task is handed a table, and "
        "without it every job YAML passes unread"
    )
    return found[0].split(",")[0].strip()


def test_every_registered_table_has_an_ingestion_job():
    """`JOB_OF` is what every lock in this section iterates, so a table missing
    from it is a table none of them look at."""
    assert set(JOB_OF) == set(REGISTRY)
    missing = [job for job in JOB_OF.values() if not (RESOURCES / job).exists()]
    assert not missing, f"declared job YAML(s) that do not exist: {missing}"


def _assert_every_task_is_handed_its_own_table(table: str, root: Path = RESOURCES) -> None:
    job_yml = JOB_OF[table]
    for key, task in tasks_of(job_yml, root).items():
        if "spark_python_task" not in task:
            continue  # the condition task runs no file and takes no parameters
        where = f"{job_yml}:{key}"
        parameters = task["spark_python_task"]["parameters"]
        takes_a_table = _first_argument_of(script_of(task, where)) == "table"
        named = [p for p in parameters if p in REGISTRY]
        expected = [table] if takes_a_table else []
        assert named == expected, (
            f"{where} is handed {parameters}, naming registered table(s) {named} where "
            f"it must name {expected} -- a task handed another table's name reads that "
            "table's landing dir, or appends into its bronze, and the run SUCCEEDS"
        )
        if takes_a_table:
            assert parameters[0] == table, (
                f"{where} is handed {parameters}: the table is this entry point's FIRST "
                f"argument, and {parameters[0]!r} is not it"
            )


@pytest.mark.parametrize("table", sorted(JOB_OF))
def test_every_job_hands_every_task_its_own_table_and_no_other(table):
    """THE PASTE LOCK. Each of these files was written by copying the previous
    table's, and a table name left behind by that copy is not a broken run -- it is
    a green one that ingested, gated, promoted or reclaimed the wrong table.

    Both halves matter. That no OTHER registered table is named anywhere in the
    file catches the leftover; that the table is the FIRST parameter catches it
    being handed where the batch id or the month belongs, which
    `test_the_socios_job_masks_before_it_ingests` and the argv contracts cannot
    see."""
    _assert_every_task_is_handed_its_own_table(table)


def test_the_paste_lock_catches_a_job_left_pointing_at_the_table_it_was_copied_from(
    tmp_path,
):
    """Proves the lock above can fail, in the exact shape of the defect: the
    empresas job's unzip left reading estabelecimentos' zips."""
    root = mutated(
        "bronze_empresas_job.yml",
        tmp_path,
        'parameters: ["empresas", "{{job.parameters.month}}"]',
        'parameters: ["estabelecimentos", "{{job.parameters.month}}"]',
    )
    with pytest.raises(AssertionError, match="naming registered table"):
        _assert_every_task_is_handed_its_own_table("empresas", root=root)


_TABLE_PLACEHOLDER = "<this job's own table>"


def _erased(value, table: str):
    """`value` with every occurrence of the job's own table name blanked out."""
    if isinstance(value, dict):
        return {key: _erased(item, table) for key, item in value.items()}
    if isinstance(value, list):
        return [_erased(item, table) for item in value]
    return _TABLE_PLACEHOLDER if value == table else value


def _shape_of(table: str, root: Path = RESOURCES) -> list[dict]:
    """A job's task list, in order, with only its own table name erased.

    What is left is everything a paste can get wrong other than the table string:
    which file each task runs, what it waits for, what it retries, which
    environment it runs in."""
    return [_erased(task, table) for task in job_of(JOB_OF[table], root)["tasks"]]


def test_the_empresas_job_is_the_estabelecimentos_job_with_one_string_changed():
    """The paste, asserted as a paste.

    The lock above compares the table STRINGS, and that is not all a copy can get
    wrong: `python_file: ../src/unzip_table.py` under `task_key: ingest` is handed
    this job's own table, passes every check in this file so far and every check in
    `test_task_wiring.py`, and runs green -- it re-unzips, the gate then finds zero
    rows in the batch, `bad_row_count` is 0, and the promote no-ops on an empty
    in-flow batch. Nothing errors and nothing is ingested. A dropped
    `max_retries: 0` and a swapped `depends_on` are the same class of thing.

    So the claim made here is the one the file's own header makes: apart from the
    table it names, this job IS the Estabelecimentos job."""
    assert _shape_of("empresas") == _shape_of("estabelecimentos"), (
        "the empresas job is no longer the estabelecimentos job with its table "
        "string changed -- one of them has a task, a python_file, a dependency, a "
        "retry count or an environment the other does not"
    )


def test_the_socios_job_is_that_same_shape_plus_the_masking_task():
    """As above, and the difference is exactly one task and one edge.

    Stated as a difference rather than asserted separately because that is the
    review question this file has to answer: what makes socios' job different from
    the table it was copied from? One task ahead of the unzip, one dependency
    pointing at it, and nothing else.

    The revision guard is FIRST in both jobs and is therefore part of the shape both
    share; what socios inserts is still one task, now between the guard and the unzip.
    The re-pointed `depends_on` below is the edge the masking task interposes on:
    everywhere else the unzip waits for the guard, here it waits for the masks, which
    wait for the guard."""
    shape = _shape_of("socios")
    assert shape[0]["task_key"] == REVISION_GUARD
    assert shape[1]["task_key"] == "ensure_masked_table"
    assert shape[1]["depends_on"] == [{"task_key": REVISION_GUARD}]
    rest = [dict(task) for task in shape[2:]]
    assert rest[0]["task_key"] == "unzip"
    assert rest[0].pop("depends_on") == [{"task_key": "ensure_masked_table"}]
    rest[0]["depends_on"] = [{"task_key": REVISION_GUARD}]
    assert [shape[0]] + rest == _shape_of("estabelecimentos"), (
        "the socios job differs from the estabelecimentos job by more than the "
        "masking task and the edge into the unzip"
    )


def test_the_socios_job_masks_before_it_ingests():
    """Ordering is the control. If ingest could run first, the append would create
    the bronze table holding unmasked names and the mask would follow the data."""
    tasks = tasks_of("bronze_socios_job.yml")
    assert tasks["unzip"]["depends_on"] == [{"task_key": "ensure_masked_table"}]


def _assert_the_masks_precede_every_other_task(table: str, root: Path = RESOURCES) -> None:
    tasks = tasks_of(JOB_OF[table], root)
    # The revision guard is the ONE thing allowed to run before the masks, and it is
    # allowed because it touches nothing: it compares two strings and returns. ADR
    # 0008's claim is that the control was applied before any byte landed, and the
    # masking task is still the first task that reaches a table at all. The guard being
    # ahead of it is what stops a wrong wheel from creating those tables with a
    # masking module that does not match the source anyone reviewed.
    # EXACTLY the guard, not "the guard or nothing". Accepting `[]` here would let this
    # lock pass a job in which the guard no longer precedes the masks, and would leave
    # that hole closed only by the guard's own test in the other half of this split --
    # so each lock would be sound only in company. The masking task's position is ADR
    # 0008's whole control; the guard's is ADR 0009's; neither should depend on the
    # other's test.
    assert tasks["ensure_masked_table"].get("depends_on") == [{"task_key": REVISION_GUARD}], (
        f"{JOB_OF[table]}: ensure_masked_table waits on "
        f"{tasks['ensure_masked_table'].get('depends_on')} rather than exactly the "
        "revision guard -- either something that touches a table now runs before the "
        "masks, or the guard no longer runs before the DDL that creates them"
    )
    for key in tasks:
        if key in ("ensure_masked_table", REVISION_GUARD):
            continue
        assert "ensure_masked_table" in ancestors(tasks, key), (
            f"{JOB_OF[table]}:{key} can start before ensure_masked_table has finished. "
            "Any task that runs first can create the bronze table by appending to it, "
            "and then the mask arrives AFTER the personal names did"
        )


@pytest.mark.parametrize("table", sorted(JOB_OF))
def test_only_the_masked_table_creates_its_bronze_table_up_front_and_does_it_first(table):
    """Which job carries `ensure_masked_table`, and that it precedes everything.

    The strong direction is masked => present and first: for socios the whole
    control is that the table exists, empty and masked, BEFORE any byte of it is
    read, because `promote_batch`'s append would otherwise create it with the names
    already in it (ADR 0008).

    The reverse direction is asserted too, and it is not symmetry for its own sake.
    The task is a documented no-op for a contract that declares no masked column, so
    a copy of it into another job would pass every other test in this file while
    telling the next reader that that table's names are masked. Where the control is
    has to be legible from the YAML."""
    tasks = tasks_of(JOB_OF[table])
    if not MASKED_COLUMNS.get(table_spec(table).contract, ()):
        assert "ensure_masked_table" not in tasks, (
            f"{JOB_OF[table]} runs ensure_masked_table for {table}, which declares no "
            "masked column -- the task is a no-op there and reads as a control that "
            "does not exist"
        )
        return
    assert "ensure_masked_table" in tasks, (
        f"{table} declares masked columns and {JOB_OF[table]} has no "
        "ensure_masked_table task: its bronze table would be created by the first "
        "append, holding unmasked personal names"
    )
    _assert_the_masks_precede_every_other_task(table)


def test_the_ordering_lock_catches_an_ingest_that_no_longer_waits_for_the_masks(tmp_path):
    """Proves the lock above can fail. Dropping one `depends_on` line is all it
    takes -- the tasks are still both there, and the run is still green."""
    root = mutated(
        "bronze_socios_job.yml",
        tmp_path,
        "          depends_on: [{ task_key: ensure_masked_table }]\n",
        "",
    )
    with pytest.raises(AssertionError, match="can start before ensure_masked_table"):
        _assert_the_masks_precede_every_other_task("socios", root=root)


def test_the_masks_lock_catches_a_masking_task_that_no_longer_waits_for_the_guard(
    tmp_path,
):
    """Proves the OTHER half of that lock can fail -- the `== [guard]` equality, which
    had no mutation while its two siblings did.

    `[]` is the mutation because it is what the equality exists to reject and what a
    weaker `in`-style check would have admitted. With the masking task depending on
    nothing, it becomes the job's first task and the revision guard no longer stands
    ahead of the DDL that creates the masked tables: a wheel nobody deployed would
    create them with whatever `opl.bronze.masking` it happens to carry. The run is
    still green, both tasks are still present, and the masks still precede every
    ingest -- so nothing else in this file notices."""
    root = mutated(
        "bronze_socios_job.yml",
        tmp_path,
        f"          depends_on: [{{ task_key: {REVISION_GUARD} }}]\n",
        "          depends_on: []\n",
    )
    with pytest.raises(AssertionError, match="rather than exactly the"):
        _assert_the_masks_precede_every_other_task("socios", root=root)


@pytest.mark.parametrize("table", sorted(JOB_OF))
def test_the_gate_verdict_routes_promotion_to_true_and_the_failure_to_false(table):
    """The one branch in these jobs, and swapping its two outcomes is silent in the
    direction that matters. Wired backwards, a batch WITH rejected rows takes the
    promote edge -- and the promote re-applies the DQ rules, so it appends only the
    passing rows and exits 0. `fail_on_dq`, the task whose entire deliverable is the
    triager's first instruction, never runs, and nobody is told the quarantine has
    anything in it."""
    tasks = tasks_of(JOB_OF[table])
    condition = tasks["check_bad_rows"]["condition_task"]
    assert (condition["op"], condition["right"]) == ("EQUAL_TO", "0")
    gate = _GATE_VALUE.fullmatch(condition["left"])
    assert gate is not None, (
        f"{JOB_OF[table]}: the condition reads {condition['left']!r}, which is not a "
        "task's bad_row_count -- an unresolved reference is not a verdict"
    )
    assert script_of(tasks[gate.group(1)], f"{JOB_OF[table]}:{gate.group(1)}") == (
        "dq_gate_batch"
    )
    assert tasks["promote"]["depends_on"] == [
        {"task_key": "check_bad_rows", "outcome": "true"}
    ]
    assert tasks["fail_on_dq"]["depends_on"] == [
        {"task_key": "check_bad_rows", "outcome": "false"}
    ]


# --- the reclaim task, on both paths that promote ---------------------------------
#
# WHY THIS SIDE OF THE SPLIT. The seam `test_job_yaml_launch_guards.py`'s docstring
# draws is "everything left there asks WHICH TABLE a job hands its tasks, and
# everything here asks what stops a run". Nothing below stops a run: the reclaim is
# the last thing either job does, and `--any-table` turns a refusal into a green
# no-op rather than refusing anything. What is asserted is which tasks a job
# DECLARES and which arguments it HANDS them, which is this file's whole subject and
# the same subject as the paste lock and the two shape locks above.
_RECLAIM = "reclaim_landing"
_REPROMOTE_JOB = "repromote_batch_job.yml"

# The flag, read out of the script rather than spelled a second time here -- the same
# treatment `_first_argument_of` gives the argv contract, and for the sharper reason:
# this module must not IMPORT `reclaim_landing.py`, which imports pyspark at module
# scope, and nothing here starts Spark.
_ANY_TABLE_FLAG = re.compile(r'^ANY_TABLE_FLAG = "([^"]+)"', re.M)


def _any_table_flag(src: Path = SRC) -> str:
    source = (src / f"{_RECLAIM}.py").read_text(encoding="utf-8")
    found = _ANY_TABLE_FLAG.findall(source)
    assert len(found) == 1, (
        f"{_RECLAIM}.py declares {len(found)} ANY_TABLE_FLAG constants, expected exactly "
        "1 -- this lock compares the job YAML against the script's own spelling, and "
        "without it the comparison would be against a literal typed here"
    )
    return found[0]


def _assert_the_reclaim_is_declared_exactly_where_an_archive_exists(
    table: str, root: Path = RESOURCES
) -> None:
    tasks = tasks_of(JOB_OF[table], root)
    expected = table_spec(table).landing == LANDING_ZIPS
    assert (_RECLAIM in tasks) is expected, (
        f"{JOB_OF[table]} {'has no' if expected else 'declares a'} {_RECLAIM} task and "
        f"{table} lands as {table_spec(table).landing!r}. The zip in the sibling zips/ "
        "dir is this task's whole safety argument: where one exists the reclaim is a "
        "control that must run, and where none does the task must not be here at all"
    )
    if not expected:
        return
    assert tasks[_RECLAIM].get("depends_on") == [{"task_key": "promote"}], (
        f"{JOB_OF[table]}:{_RECLAIM} waits on {tasks[_RECLAIM].get('depends_on')} rather "
        "than exactly the promote. The promote's success IS the precondition -- the rows "
        "are persisted -- and a reclaim that no longer waits for it deletes landed files "
        "on the strength of a proof that has not been written yet"
    )


@pytest.mark.parametrize("table", sorted(JOB_OF))
def test_the_reclaim_is_wired_exactly_where_a_zip_survives_and_waits_for_the_promote(
    table,
):
    """WHERE the one task that DELETES is declared, derived from the registry.

    Three of the seven ingestion jobs carry it, and the three are exactly the tables
    that land as zips -- not a count typed here. `bronze_job.yml` has none and says
    why: a `local`-landed table's zip never reaches the Volume, so its landed file is
    the single copy in the workspace. Both directions are asserted because both are
    silent. A missing reclaim leaves consumed CSVs accumulating against a Volume with
    no published quota, which is how 8,212,278,423 B of 2026-06 sat there; a reclaim
    copied into a job whose table has no archive would delete the last copy."""
    _assert_the_reclaim_is_declared_exactly_where_an_archive_exists(table)


def _assert_the_repromote_reclaims_after_its_promote(root: Path = RESOURCES) -> None:
    tasks = job_of(_REPROMOTE_JOB, root)["tasks"]
    assert [task["task_key"] for task in tasks] == [REVISION_GUARD, "promote", _RECLAIM], (
        f"{_REPROMOTE_JOB} declares {[task['task_key'] for task in tasks]}. The reclaim "
        "is the third task and the point of F4: this is the one path that promotes a "
        "GATED batch, and without it a gate that fires every month leaves the reclaim "
        "unreachable for the life of the project (ADR 0006)"
    )
    reclaim = tasks[-1]
    assert reclaim.get("depends_on") == [{"task_key": "promote"}], (
        f"{_REPROMOTE_JOB}:{_RECLAIM} waits on {reclaim.get('depends_on')} rather than "
        "exactly the promote. That edge is what carries the precondition ADR 0006 named "
        "-- these rows are persisted -- and on the guard instead it would delete landed "
        "files of a batch this run never promoted"
    )
    assert reclaim["spark_python_task"]["parameters"] == [
        "{{job.parameters.table}}", "{{job.parameters.batch_id}}", _any_table_flag(),
    ], (
        f"{_REPROMOTE_JOB}:{_RECLAIM} is handed "
        f"{reclaim['spark_python_task']['parameters']}. Three arguments, in this order: "
        "the table this job's promote just appended for, the batch it appended, and the "
        "flag saying this job serves EVERY registered table. Without the flag a "
        "repromote of payments, ptax, merchant or lookup ends RED on its last task after "
        "a promote that worked -- which is how an operator learns to read red as noise. "
        "A month is deliberately absent; see the task's own comment"
    )


def test_the_repromote_job_reclaims_after_its_promote_for_whatever_table_it_was_given():
    """THE WIRING F4 EXISTS TO ADD, LOCKED.

    It had no lock at all until this pass: deleting the task, dropping the flag or
    re-pointing the dependency at the revision guard each left the whole suite green,
    because every shape lock in this file iterates `JOB_OF` -- the seven per-table
    ingestion jobs -- and the operator job is in none of them. That is the exact
    defect the commit that added this task invokes twice: a control that cannot be
    told from one that never ran, applied to the wiring rather than to a guard."""
    _assert_the_repromote_reclaims_after_its_promote()


def test_the_reclaim_lock_catches_the_task_being_deleted_again(tmp_path):
    """Proves the lock above can fail, in the shape it was written for: the state
    `repromote_batch_job.yml` was in for the life of the project. Nothing errors,
    the repromote still succeeds, and the landing files of every gated batch stay."""
    root = mutated(
        _REPROMOTE_JOB,
        tmp_path,
        "        - task_key: reclaim_landing\n"
        "          depends_on: [{ task_key: promote }]\n",
        "",
    )
    with pytest.raises(AssertionError, match="reclaim is the third task"):
        _assert_the_repromote_reclaims_after_its_promote(root=root)


def test_the_reclaim_lock_catches_a_dependency_moved_off_the_promote(tmp_path):
    """Proves the SECOND assertion can fail. Re-pointed at the revision guard the
    reclaim runs beside the promote rather than after it -- both tasks present, run
    still green, and the deletes are taken against whatever bronze held before this
    run appended anything."""
    root = mutated(
        _REPROMOTE_JOB,
        tmp_path,
        "depends_on: [{ task_key: promote }]",
        f"depends_on: [{{ task_key: {REVISION_GUARD} }}]",
    )
    with pytest.raises(AssertionError, match="rather than exactly the promote"):
        _assert_the_repromote_reclaims_after_its_promote(root=root)


def test_the_registry_derived_reclaim_lock_catches_an_ingestion_job_losing_its_reclaim(
    tmp_path,
):
    """Proves the OTHER lock can fail, on the side the repromote's does not cover.
    Estabelecimentos still lands as zips, so the task is still required -- and the
    job without it ingests, gates, promotes and exits 0, leaving every consumed CSV
    of the batch in the Volume with nothing in the log naming them."""
    root = mutated(
        "bronze_estabelecimentos_job.yml",
        tmp_path,
        "        - task_key: reclaim_landing\n"
        "          depends_on: [{ task_key: promote }]\n",
        "",
    )
    with pytest.raises(AssertionError, match="has no reclaim_landing task"):
        _assert_the_reclaim_is_declared_exactly_where_an_archive_exists(
            "estabelecimentos", root=root
        )


def test_the_flag_reader_catches_a_second_declaration_of_the_constant(tmp_path):
    """Proves the ASSERTION INSIDE the reader can fail, which nothing probed.

    `_any_table_flag` exists so the YAML lock compares against the script's OWN
    spelling rather than a literal typed here, and `len(found) == 1` is what makes
    that comparison well-defined. With two declarations the regex returns the first,
    which is the one Python then overwrites -- so the lock would assert the YAML
    against a constant the module does not use, and pass while the deployed flag was
    the other one. The probe writes the second declaration rather than deleting the
    first, because zero already fails loudly at `found[0]`."""
    src = tmp_path / "src"
    src.mkdir()
    source = (SRC / f"{_RECLAIM}.py").read_text(encoding="utf-8")
    flag = _any_table_flag()
    (src / f"{_RECLAIM}.py").write_text(
        source.replace(
            f'ANY_TABLE_FLAG = "{flag}"',
            f'ANY_TABLE_FLAG = "{flag}"\nANY_TABLE_FLAG = "--every-table"',
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="declares 2 ANY_TABLE_FLAG constants"):
        _any_table_flag(src=src)


def test_the_registry_derived_reclaim_lock_catches_a_reclaim_where_no_zip_survives(
    tmp_path,
):
    """Proves the OTHER DIRECTION of the registry-derived lock, which had no probe.

    The `is expected` assertion covers two failures and only the missing-task one was
    exercised. This is the worse half: the lookup lands LOCAL, so its six landed CSVs
    are the only copy in this workspace, and a reclaim task pasted into its job from a
    CNPJ one -- the exact way every other task in these files arrived -- would hand
    `reclaim_landing.py` a table it refuses. The job would go RED on its last task
    with the delete refused, which is the recoverable direction; what the lock
    protects is that the refusal never becomes a green no-op, since `--any-table` is
    one paste away."""
    root = mutated(
        JOB_OF["lookup"],
        tmp_path,
        "      environments:\n",
        "        - task_key: reclaim_landing\n"
        "          depends_on: [{ task_key: promote }]\n"
        "          max_retries: 0\n"
        "          environment_key: opl_env\n"
        "          spark_python_task:\n"
        "            python_file: ../src/reclaim_landing.py\n"
        '            parameters: ["lookup", "{{job.run_id}}", "{{job.parameters.month}}"]\n'
        "      environments:\n",
    )
    with pytest.raises(AssertionError, match="declares a reclaim_landing task"):
        _assert_the_reclaim_is_declared_exactly_where_an_archive_exists("lookup", root=root)


def test_the_ingestion_reclaim_lock_catches_a_dependency_moved_off_the_promote(tmp_path):
    """Proves the ingestion jobs' SECOND assertion can fail on its own terms.

    The only probe this lock had deleted the whole task, which trips the FIRST
    assertion and leaves the dependency check unexercised. Re-pointed at `ingest` the
    reclaim runs beside the gate rather than after the promote: the deletes are taken
    against whatever bronze held before this run appended anything, on a batch whose
    rows may still be about to be rejected."""
    root = mutated(
        JOB_OF["estabelecimentos"],
        tmp_path,
        "depends_on: [{ task_key: promote }]",
        "depends_on: [{ task_key: ingest }]",
    )
    with pytest.raises(AssertionError, match="rather than exactly the promote"):
        _assert_the_reclaim_is_declared_exactly_where_an_archive_exists(
            "estabelecimentos", root=root
        )


def test_the_reclaim_lock_catches_the_any_table_flag_being_dropped(tmp_path):
    """Proves the THIRD assertion can fail, and this is the one a reader would call
    cosmetic. Without the flag the reclaim RAISES for the four tables that have no
    zip in the Volume, so the remedy the reconciliation view prints for the stranded
    payments batch ends red on a repromote that promoted perfectly."""
    root = mutated(
        _REPROMOTE_JOB,
        tmp_path,
        ', "--any-table"]',
        "]",
    )
    with pytest.raises(AssertionError, match="Three arguments"):
        _assert_the_repromote_reclaims_after_its_promote(root=root)


def test_the_payments_job_names_every_declared_profile_and_invents_none():
    """THE PROSE THAT WENT STALE ONCE ALREADY, AND THE HALF NOTHING WAS CLOSING.

    `bronze_payments_job.yml` documents the legal values of its `profile` parameter as an
    indented `profile=<key>` list, because the parameter's whole contract is "one of the
    keys declared in `opl.generator.profiles`" and an operator reads the YAML rather than
    the module. Nothing compared the two: a fourth profile landed while the comment named
    three (fixed in `6325fc3`), and F-API's fifth was the next drift queued to happen.

    BOTH DIRECTIONS, AND THE SECOND IS THE ONE A "DID YOU UPDATE THE COMMENT" HABIT MISSES.
    A declared key missing from the YAML is an operator who cannot discover a stream that
    exists. A key in the YAML that no longer exists is worse: `profile_for` refuses it
    naming the real ones, so the run fails -- after the revision guard, the session start
    and the pool query have all succeeded -- and the operator's source of truth was the
    thing that lied.

    READ AS A SET FROM THE TEXT rather than by asking whether each key appears somewhere in
    the file. `"clean" in text` is true of any YAML mentioning `profile=clean-something`,
    and it can never catch a surplus entry at all."""
    text = (RESOURCES / JOB_OF[payments.CONTRACT]).read_text(encoding="utf-8")
    documented = set(_PROFILE_LINE.findall(text))
    assert documented == set(PROFILES), (
        f"{JOB_OF[payments.CONTRACT]} documents profiles {sorted(documented)} and "
        f"opl.generator.profiles declares {sorted(PROFILES)}. The YAML comment is what an "
        "operator reads to learn the legal values of --params profile=..., so a missing key "
        "hides a stream and a surplus one sends a run to a refusal after the session starts."
    )
    assert len(documented) == 5, "a sanity floor: the regex must actually be matching"


@pytest.mark.parametrize("table", sorted(JOB_OF))
def test_every_task_runs_unretried_in_the_declared_serverless_environment(table):
    """`max_retries: 0` and the environment, on every task of every job.

    Neither is decoration. A retry is a SECOND run of a task under the same
    `{{job.run_id}}`, i.e. the same `_batch_id`, which is the identity the promote's
    idempotence is built on -- and `environment_version: "3"` is the one serverless
    client version this wheel installs under at all (pyproject records the probe:
    version 2's Python 3.11.10 rejects it outright)."""
    job = job_of(JOB_OF[table])
    environments = {
        environment["environment_key"]: environment["spec"]
        for environment in job["environments"]
    }
    for key, task in tasks_of(JOB_OF[table]).items():
        if "spark_python_task" not in task:
            continue  # a condition task runs nowhere and retries nothing
        where = f"{JOB_OF[table]}:{key}"
        assert task.get("max_retries") == 0, f"{where} does not declare max_retries: 0"
        assert task.get("environment_key") in environments, (
            f"{where} names environment {task.get('environment_key')!r}, which this job "
            f"does not declare ({sorted(environments)})"
        )
        spec = environments[task["environment_key"]]
        assert spec["environment_version"] == "3"
        assert spec["dependencies"] == ["../../dist/*.whl"]
