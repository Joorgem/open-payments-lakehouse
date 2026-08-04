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

Nothing here starts Spark and nothing here loads a job script: every assertion is
about wiring, not data. NO HELPER IS SHARED ACROSS THE SEAM -- the AST helpers
stayed with the scripts they read and every YAML helper arrived here whole, so
nothing was copied. `_SRC` is re-derived below rather than imported from the other
half, because that is how every task test in this directory derives it (one line,
from `parents[1]`), and because a test module importing another test module would
give this suite a collection-order dependency it does not otherwise have."""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from opl.bronze.masking import MASKED_COLUMNS
from opl.bronze.provenance import SENTINEL_REVISION, is_object_name
from opl.bronze.registry import REGISTRY, table_spec
from opl.config import SENTINEL_MONTH, is_month

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"
_RESOURCES = _REPO / "databricks" / "resources"

# Which ingestion-flow job serves which registered table. Total over the registry
# and asserted so below: a table registered without a job here is a table nothing
# ingests, and the next person to copy a job YAML would have no list to add to.
_JOB_OF = {
    "lookup": "bronze_job.yml",
    "estabelecimentos": "bronze_estabelecimentos_job.yml",
    "empresas": "bronze_empresas_job.yml",
    "socios": "bronze_socios_job.yml",
}

# WHICH JOBS REFUSE A RUN BUILT FROM AN UNEXPECTED REVISION, and which one deliberately
# does not. Every YAML under `databricks/resources` must appear in one of these two, and
# `test_every_job_yaml_is_either_guarded_or_deliberately_not` asserts it -- `_JOB_OF`
# above covers the four ingestion jobs only, so a lock driven off it alone would say
# nothing about the two jobs that ingest nothing, and "a job missing the guard fails"
# would quietly not hold for exactly the files nobody thinks about.
_REVISION_GUARD = "assert_deployed_revision"

_GUARDED_JOBS = (
    "bronze_job.yml",
    "bronze_empresas_job.yml",
    "bronze_estabelecimentos_job.yml",
    "bronze_socios_job.yml",
    # The operator job, and its inclusion is a decision rather than completeness: a
    # repromote APPENDS TO BRONZE, the system of record, re-applying whatever DQ rules
    # the deployed wheel happens to carry. Run against a stale wheel it appends rows
    # the current rules reject. Its header's isolation argument is about what STARTS
    # the job, not about which code the job then runs (ADR 0009).
    "repromote_batch_job.yml",
)

_UNGUARDED_JOBS = {
    # The one exclusion, and the reason is that a guard here would remove a diagnostic
    # in the exact case it exists for: smoke's entire purpose is to answer "does the
    # deployed wheel import and can it read config", i.e. it is what you run WHEN YOU
    # SUSPECT THE DEPLOYMENT. It writes nothing, so a wrong revision costs a re-run.
    # It reports the deployed revision instead of refusing on it, which
    # tests/test_assert_deployed_revision_task.py pins from the script side.
    "smoke_job.yml": "the probe you run when the deployment itself is in doubt",
}

_PYTHON_FILE_PREFIX = "../src/"

# The `argv:` line every entry point's docstring ends with. Read rather than
# restated, see `_first_argument_of`.
_ARGV_LINE = re.compile(r"^argv: \[([^\]]*)\]", re.M)

# The task value the condition task branches on, and the task that publishes it.
_GATE_VALUE = re.compile(r"^\{\{tasks\.([\w-]+)\.values\.bad_row_count\}\}$")


def _job_of(job_yml: str, root: Path = _RESOURCES) -> dict:
    """The ONE job declared in `job_yml`.

    One is asserted, not assumed: these files are one-job-per-table deliberately
    -- each header says why, and the reason is that a shared `{{job.run_id}}` is a
    shared `_batch_id` -- so a helper that silently read the first of several would
    be reading a job whose existence already broke the batch model."""
    doc = yaml.safe_load((root / job_yml).read_text(encoding="utf-8"))
    jobs = doc["resources"]["jobs"]
    assert len(jobs) == 1, (
        f"{job_yml} declares {len(jobs)} jobs, expected exactly 1 -- one job per table "
        "is what keeps one _batch_id to one table's ingest"
    )
    return next(iter(jobs.values()))


def _tasks_of(job_yml: str, root: Path = _RESOURCES) -> dict[str, dict]:
    """The job's tasks, keyed by `task_key`."""
    tasks = _job_of(job_yml, root)["tasks"]
    keys = [task["task_key"] for task in tasks]
    assert len(keys) == len(set(keys)), (
        f"{job_yml} declares a task_key twice ({keys}); this lock resolves a key to one "
        "task and would check an arbitrary one of them"
    )
    return {task["task_key"]: task for task in tasks}


def _script_of(task: dict, where: str) -> str:
    """The databricks/src entry point a task runs, checked to exist."""
    python_file = task["spark_python_task"]["python_file"]
    assert python_file.startswith(_PYTHON_FILE_PREFIX), (
        f"{where} runs {python_file!r}, which is not under {_PYTHON_FILE_PREFIX}"
    )
    script = python_file[len(_PYTHON_FILE_PREFIX):].removesuffix(".py")
    assert (_SRC / f"{script}.py").exists(), (
        f"{where} runs {python_file!r}, which is not a file under databricks/src"
    )
    return script


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
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    found = _ARGV_LINE.findall(source)
    assert len(found) == 1, (
        f"{script}.py declares {len(found)} `argv: [...]` lines, expected exactly 1 -- "
        "this lock reads that line to learn whether the task is handed a table, and "
        "without it every job YAML passes unread"
    )
    return found[0].split(",")[0].strip()


def _ancestors(tasks: dict[str, dict], key: str) -> set[str]:
    """Every task that must have finished before `key` may start.

    Transitive, because the property being asserted is transitive: what matters is
    that no bytes can land before the masks are on, not which task happens to be
    named in one `depends_on`. A dependency on a task the job does not declare is a
    failure here rather than a KeyError at run time."""
    seen: set[str] = set()
    frontier = [key]
    while frontier:
        for dependency in tasks[frontier.pop()].get("depends_on", []):
            name = dependency["task_key"]
            assert name in tasks, (
                f"a task depends on {name!r}, which this job does not declare"
            )
            if name not in seen:
                seen.add(name)
                frontier.append(name)
    return seen


def test_every_registered_table_has_an_ingestion_job():
    """`_JOB_OF` is what every lock in this section iterates, so a table missing
    from it is a table none of them look at."""
    assert set(_JOB_OF) == set(REGISTRY)
    missing = [job for job in _JOB_OF.values() if not (_RESOURCES / job).exists()]
    assert not missing, f"declared job YAML(s) that do not exist: {missing}"


def _assert_every_task_is_handed_its_own_table(table: str, root: Path = _RESOURCES) -> None:
    job_yml = _JOB_OF[table]
    for key, task in _tasks_of(job_yml, root).items():
        if "spark_python_task" not in task:
            continue  # the condition task runs no file and takes no parameters
        where = f"{job_yml}:{key}"
        parameters = task["spark_python_task"]["parameters"]
        takes_a_table = _first_argument_of(_script_of(task, where)) == "table"
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


@pytest.mark.parametrize("table", sorted(_JOB_OF))
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


def _mutated(job_yml: str, tmp_path: Path, old: str, new: str) -> Path:
    """`job_yml` copied into `tmp_path` with one substring replaced.

    The mutation is asserted to have applied: a probe that silently changed nothing
    proves the lock catches nothing."""
    root = tmp_path / "resources"
    root.mkdir(parents=True, exist_ok=True)
    original = (_RESOURCES / job_yml).read_text(encoding="utf-8")
    assert old in original, f"the mutation target {old!r} is not in {job_yml}"
    (root / job_yml).write_text(original.replace(old, new, 1), encoding="utf-8")
    return root


def test_the_paste_lock_catches_a_job_left_pointing_at_the_table_it_was_copied_from(
    tmp_path,
):
    """Proves the lock above can fail, in the exact shape of the defect: the
    empresas job's unzip left reading estabelecimentos' zips."""
    root = _mutated(
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


def _shape_of(table: str, root: Path = _RESOURCES) -> list[dict]:
    """A job's task list, in order, with only its own table name erased.

    What is left is everything a paste can get wrong other than the table string:
    which file each task runs, what it waits for, what it retries, which
    environment it runs in."""
    return [_erased(task, table) for task in _job_of(_JOB_OF[table], root)["tasks"]]


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
    assert shape[0]["task_key"] == _REVISION_GUARD
    assert shape[1]["task_key"] == "ensure_masked_table"
    assert shape[1]["depends_on"] == [{"task_key": _REVISION_GUARD}]
    rest = [dict(task) for task in shape[2:]]
    assert rest[0]["task_key"] == "unzip"
    assert rest[0].pop("depends_on") == [{"task_key": "ensure_masked_table"}]
    rest[0]["depends_on"] = [{"task_key": _REVISION_GUARD}]
    assert [shape[0]] + rest == _shape_of("estabelecimentos"), (
        "the socios job differs from the estabelecimentos job by more than the "
        "masking task and the edge into the unzip"
    )


def test_the_socios_job_masks_before_it_ingests():
    """Ordering is the control. If ingest could run first, the append would create
    the bronze table holding unmasked names and the mask would follow the data."""
    tasks = _tasks_of("bronze_socios_job.yml")
    assert tasks["unzip"]["depends_on"] == [{"task_key": "ensure_masked_table"}]


def _assert_the_masks_precede_every_other_task(table: str, root: Path = _RESOURCES) -> None:
    tasks = _tasks_of(_JOB_OF[table], root)
    # The revision guard is the ONE thing allowed to run before the masks, and it is
    # allowed because it touches nothing: it compares two strings and returns. ADR
    # 0008's claim is that the control was applied before any byte landed, and the
    # masking task is still the first task that reaches a table at all. The guard being
    # ahead of it is what stops a wrong wheel from creating those tables with a
    # masking module that does not match the source anyone reviewed.
    # EXACTLY the guard, not "the guard or nothing". Accepting `[]` here would let this
    # lock pass a job in which the guard no longer precedes the masks, and would leave
    # that hole closed only by the guard's own test three screens down -- so each lock
    # would be sound only in company. The masking task's position is ADR 0008's whole
    # control; the guard's is ADR 0009's; neither should depend on the other's test.
    assert tasks["ensure_masked_table"].get("depends_on") == [{"task_key": _REVISION_GUARD}], (
        f"{_JOB_OF[table]}: ensure_masked_table waits on "
        f"{tasks['ensure_masked_table'].get('depends_on')} rather than exactly the "
        "revision guard -- either something that touches a table now runs before the "
        "masks, or the guard no longer runs before the DDL that creates them"
    )
    for key in tasks:
        if key in ("ensure_masked_table", _REVISION_GUARD):
            continue
        assert "ensure_masked_table" in _ancestors(tasks, key), (
            f"{_JOB_OF[table]}:{key} can start before ensure_masked_table has finished. "
            "Any task that runs first can create the bronze table by appending to it, "
            "and then the mask arrives AFTER the personal names did"
        )


@pytest.mark.parametrize("table", sorted(_JOB_OF))
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
    tasks = _tasks_of(_JOB_OF[table])
    if not MASKED_COLUMNS.get(table_spec(table).contract, ()):
        assert "ensure_masked_table" not in tasks, (
            f"{_JOB_OF[table]} runs ensure_masked_table for {table}, which declares no "
            "masked column -- the task is a no-op there and reads as a control that "
            "does not exist"
        )
        return
    assert "ensure_masked_table" in tasks, (
        f"{table} declares masked columns and {_JOB_OF[table]} has no "
        "ensure_masked_table task: its bronze table would be created by the first "
        "append, holding unmasked personal names"
    )
    _assert_the_masks_precede_every_other_task(table)


def test_the_ordering_lock_catches_an_ingest_that_no_longer_waits_for_the_masks(tmp_path):
    """Proves the lock above can fail. Dropping one `depends_on` line is all it
    takes -- the tasks are still both there, and the run is still green."""
    root = _mutated(
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
    root = _mutated(
        "bronze_socios_job.yml",
        tmp_path,
        f"          depends_on: [{{ task_key: {_REVISION_GUARD} }}]\n",
        "          depends_on: []\n",
    )
    with pytest.raises(AssertionError, match="rather than exactly the"):
        _assert_the_masks_precede_every_other_task("socios", root=root)


@pytest.mark.parametrize("table", sorted(_JOB_OF))
def test_the_gate_verdict_routes_promotion_to_true_and_the_failure_to_false(table):
    """The one branch in these jobs, and swapping its two outcomes is silent in the
    direction that matters. Wired backwards, a batch WITH rejected rows takes the
    promote edge -- and the promote re-applies the DQ rules, so it appends only the
    passing rows and exits 0. `fail_on_dq`, the task whose entire deliverable is the
    triager's first instruction, never runs, and nobody is told the quarantine has
    anything in it."""
    tasks = _tasks_of(_JOB_OF[table])
    condition = tasks["check_bad_rows"]["condition_task"]
    assert (condition["op"], condition["right"]) == ("EQUAL_TO", "0")
    gate = _GATE_VALUE.fullmatch(condition["left"])
    assert gate is not None, (
        f"{_JOB_OF[table]}: the condition reads {condition['left']!r}, which is not a "
        "task's bad_row_count -- an unresolved reference is not a verdict"
    )
    assert _script_of(tasks[gate.group(1)], f"{_JOB_OF[table]}:{gate.group(1)}") == (
        "dq_gate_batch"
    )
    assert tasks["promote"]["depends_on"] == [
        {"task_key": "check_bad_rows", "outcome": "true"}
    ]
    assert tasks["fail_on_dq"]["depends_on"] == [
        {"task_key": "check_bad_rows", "outcome": "false"}
    ]


@pytest.mark.parametrize("table", sorted(_JOB_OF))
def test_every_task_runs_unretried_in_the_declared_serverless_environment(table):
    """`max_retries: 0` and the environment, on every task of every job.

    Neither is decoration. A retry is a SECOND run of a task under the same
    `{{job.run_id}}`, i.e. the same `_batch_id`, which is the identity the promote's
    idempotence is built on -- and `environment_version: "3"` is the one serverless
    client version this wheel installs under at all (pyproject records the probe:
    version 2's Python 3.11.10 rejects it outright)."""
    job = _job_of(_JOB_OF[table])
    environments = {
        environment["environment_key"]: environment["spec"]
        for environment in job["environments"]
    }
    for key, task in _tasks_of(_JOB_OF[table]).items():
        if "spark_python_task" not in task:
            continue  # a condition task runs nowhere and retries nothing
        where = f"{_JOB_OF[table]}:{key}"
        assert task.get("max_retries") == 0, f"{where} does not declare max_retries: 0"
        assert task.get("environment_key") in environments, (
            f"{where} names environment {task.get('environment_key')!r}, which this job "
            f"does not declare ({sorted(environments)})"
        )
        spec = environments[task["environment_key"]]
        assert spec["environment_version"] == "3"
        assert spec["dependencies"] == ["../../dist/*.whl"]


# --- THE DEPLOYED-REVISION GUARD (ADR 0009) ---------------------------------------
#
# Why this section is in a YAML lock and not only in the task's own unit test: the
# guard is a task like any other, so every way a job YAML can be wrong about a task
# applies to it -- absent, present but waiting on the unzip, handed the wrong
# parameter, or sitting in a second environment that installs a different wheel than
# the work does. None of those fail a run; the first three make the run green with the
# guard doing nothing useful, and the fourth makes it green having verified a wheel no
# task uses. CI validates the repository and never what is deployed, which is the whole
# reason the guard exists -- so the guard's own wiring is the one thing that had better
# not depend on somebody having looked.


def test_every_job_yaml_is_either_guarded_or_deliberately_not():
    """The classification is TOTAL over `databricks/resources/*.yml`.

    A new job YAML must be added to one of the two lists, and the choice is the point:
    "does a run of this job against a wheel built from another revision matter?" has an
    answer for every job, and the answer for the four ingestion jobs and the repromote
    is yes. Left to a glob, a job added later would inherit whichever behaviour the
    glob happened to give it."""
    declared = set(_GUARDED_JOBS) | set(_UNGUARDED_JOBS)
    present = {path.name for path in _RESOURCES.glob("*.yml")}
    assert declared == present, (
        f"unclassified job YAML(s): {sorted(present - declared)}; classified but absent: "
        f"{sorted(declared - present)}"
    )
    assert set(_JOB_OF.values()) <= set(_GUARDED_JOBS), (
        "an ingestion job is not guarded -- these are the jobs that move GB and append "
        f"to bronze: {sorted(set(_JOB_OF.values()) - set(_GUARDED_JOBS))}"
    )


def _assert_the_revision_guard_precedes_every_other_task(
    job_yml: str, root: Path = _RESOURCES
) -> None:
    tasks = _tasks_of(job_yml, root)
    assert _REVISION_GUARD in tasks, (
        f"{job_yml} has no {_REVISION_GUARD} task, so a run of it against a wheel built "
        "from another revision succeeds -- which is what happened on 2026-08-01, when a "
        "socios re-run reported SUCCESS having masked only bronze because the workspace "
        "was still running a bundle deployed four commits earlier"
    )
    guard = tasks[_REVISION_GUARD]
    assert not guard.get("depends_on"), (
        f"{job_yml}: the guard waits on something, so the run has already done that "
        "something by the time it learns it is running the wrong code. TRAP 2 -- a check "
        "after the unzip reports the problem once several GB have moved"
    )
    assert _script_of(guard, f"{job_yml}:{_REVISION_GUARD}") == _REVISION_GUARD
    assert guard.get("max_retries") == 0, f"{job_yml}: the guard does not declare max_retries: 0"
    for key in tasks:
        if key == _REVISION_GUARD:
            continue
        assert _REVISION_GUARD in _ancestors(tasks, key), (
            f"{job_yml}:{key} can start before {_REVISION_GUARD} has finished, so the "
            "guard no longer stands between a wrong deployment and the work"
        )


@pytest.mark.parametrize("job_yml", _GUARDED_JOBS)
def test_the_revision_guard_runs_first_and_everything_else_waits_for_it(job_yml):
    _assert_the_revision_guard_precedes_every_other_task(job_yml)


def _assert_the_revision_default_cannot_pass(job_yml: str, root: Path = _RESOURCES) -> None:
    parameters = {
        parameter["name"]: parameter.get("default")
        for parameter in _job_of(job_yml, root).get("parameters", [])
    }
    assert "revision" in parameters, (
        f"{job_yml} declares no `revision` job parameter, so there is nothing for "
        "--params revision=... to reach and the guard has no expected value"
    )
    default = parameters["revision"]
    assert not is_object_name(default), (
        f"{job_yml}'s revision default is {default!r}, which the guard ACCEPTS as a whole "
        "object name -- so a run launched without --params revision=... would pass the "
        "check against a revision nobody chose. A job-parameter default cannot validate "
        "anything; it can only be a value that refuses"
    )
    assert default == SENTINEL_REVISION, (
        f"{job_yml}'s revision default is {default!r} rather than the sentinel the code "
        f"names ({SENTINEL_REVISION!r}). Two spellings of one sentinel is a default that "
        "drifts into a value nobody checked"
    )


@pytest.mark.parametrize("job_yml", _GUARDED_JOBS)
def test_the_guard_is_handed_the_runs_revision_and_the_default_refuses(job_yml):
    """The parameter, its default, and the one thing the guard is handed.

    `{{job.parameters.revision}}` is the whole carrier: it is what makes the expected
    value arrive from the LAUNCH -- from the operator's own repository at the moment the
    run is submitted -- rather than from anything the deploy stamped. A deploy-time
    value would make expected and actual two names for the same act, and the incident
    this guard exists for is a deploy that never happened (ADR 0009)."""
    _assert_the_revision_default_cannot_pass(job_yml)
    guard = _tasks_of(job_yml)[_REVISION_GUARD]
    assert guard["spark_python_task"]["parameters"] == ["{{job.parameters.revision}}"], (
        f"{job_yml}: the guard is handed {guard['spark_python_task']['parameters']}, not "
        "the run's revision parameter"
    )


@pytest.mark.parametrize("job_yml", _GUARDED_JOBS)
def test_the_guard_verifies_the_same_wheel_the_work_installs(job_yml):
    """A guard in a second environment verifies a second wheel and proves nothing.

    Each of these jobs declares ONE environment, and every `spark_python_task` runs in
    it -- including the guard. That is what makes "the deployed wheel is the one you
    expect" a statement about the tasks that follow, rather than about an install only
    the guard ever performed."""
    tasks = _tasks_of(job_yml)
    keys = {
        task["environment_key"] for task in tasks.values() if "spark_python_task" in task
    }
    assert keys == {tasks[_REVISION_GUARD]["environment_key"]}, (
        f"{job_yml} runs its tasks in {sorted(keys)}: the guard shares an environment "
        "with only some of the work, so the rest installs a wheel it never checked"
    )


def test_the_unguarded_job_carries_neither_the_guard_nor_a_revision_parameter():
    """The exclusion, ASSERTED. An exclusion left to be inferred from absence is
    indistinguishable from the guard having been forgotten -- and this file's whole
    subject is what a copied YAML forgets. `smoke_job.yml` is excluded because it is the
    probe you run when the deployment itself is in doubt; it reports the deployed
    revision instead (ADR 0009)."""
    for job_yml, why in _UNGUARDED_JOBS.items():
        job = _job_of(job_yml)
        assert _REVISION_GUARD not in _tasks_of(job_yml), (
            f"{job_yml} now carries the guard, but it is listed as unguarded because "
            f"it is {why}. Move it to _GUARDED_JOBS or take the task out"
        )
        names = {parameter["name"] for parameter in job.get("parameters", [])}
        assert "revision" not in names, (
            f"{job_yml} takes a revision parameter that nothing in it checks, which "
            "reads as a guard that is not there"
        )


def test_the_guard_ordering_lock_catches_a_task_that_no_longer_waits_for_it(tmp_path):
    """Proves the lock above can fail. One `depends_on` line, and the unzip runs
    alongside the guard rather than after it -- both tasks still present, run still
    green, several GB moved before anything was verified."""
    root = _mutated(
        "bronze_estabelecimentos_job.yml",
        tmp_path,
        f"          depends_on: [{{ task_key: {_REVISION_GUARD} }}]\n",
        "",
    )
    with pytest.raises(AssertionError, match="can start before"):
        _assert_the_revision_guard_precedes_every_other_task(
            "bronze_estabelecimentos_job.yml", root=root
        )


def _assert_the_month_default_cannot_pass(job_yml: str, root: Path = _RESOURCES) -> None:
    parameters = {
        parameter["name"]: parameter.get("default")
        for parameter in _job_of(job_yml, root).get("parameters", [])
    }
    assert "month" in parameters, (
        f"{job_yml} declares no `month` job parameter, so there is nothing for "
        "--params month=... to reach and every task falls back on a month nobody chose"
    )
    default = parameters["month"]
    assert not is_month(default), (
        f"{job_yml}'s month default is {default!r}, which `require_month` ACCEPTS as a "
        "real month -- so a run launched without --params month=... would ingest that "
        "month against an EMPTY month-scoped checkpoint, treat every one of its files "
        "as new, and append the whole month into staging under a fresh _batch_id. "
        "promote.rows_of_batch keys idempotence on _batch_id, so it cannot see the "
        "duplication and would carry it into bronze: nothing fails and the row counts "
        "double. A job-parameter default cannot validate anything; it can only refuse"
    )
    assert default == SENTINEL_MONTH, (
        f"{job_yml}'s month default is {default!r} rather than the sentinel the code "
        f"names ({SENTINEL_MONTH!r}). Two spellings of one sentinel is a default that "
        "drifts into a value nobody checked"
    )


@pytest.mark.parametrize("job_yml", sorted(set(_JOB_OF.values())))
def test_the_month_default_refuses_rather_than_naming_a_month_nobody_chose(job_yml):
    """The `month` default, locked for the reason the `revision` default is.

    THIS ONE CHANGED MEANING WITHOUT ITS TEXT CHANGING, which is why it needs a lock
    of its own rather than the operator's memory. `default: "2026-06"` was harmless
    while the Auto Loader checkpoint was keyed on `table_key` alone: an un-parameterised
    launch read 2026-06's landing dir against a checkpoint that already recorded every
    one of those files, so it drained nothing. Month-scoping that state (F1.4b PR B
    Task 5 Step 0) made the same launch find an EMPTY checkpoint and re-ingest a full
    month -- and because `promote.rows_of_batch` keys idempotence on `_batch_id`, the
    fresh batch is invisible to it and the duplicate reaches bronze. The consequence is
    written in `checkpoint_location`'s own docstring; this asserts the four YAMLs an
    operator launches from do not walk into it.

    Over the ingestion jobs only, because they are the ones that read a landing dir and
    advance a checkpoint. `repromote_batch_job.yml` and `smoke_job.yml` take no month
    at all -- adding one is what this parametrization would have to be taught about,
    and `_JOB_OF` is the list that already knows which jobs ingest."""
    _assert_the_month_default_cannot_pass(job_yml)


def test_the_month_default_lock_catches_the_real_month_it_used_to_carry(tmp_path):
    """Proves the lock above can fail, in the exact value and exact shape of the defect.

    `2026-06` is not an invented mutation: it is what all four of these files carried
    until this branch, and it is a fully-promoted month. Restoring it makes every run
    launched without `--params month=...` a second ingest of data bronze already holds
    -- green, with the row counts doubled and nothing in the log naming the month."""
    root = _mutated(
        "bronze_estabelecimentos_job.yml",
        tmp_path,
        f'default: "{SENTINEL_MONTH}"',
        'default: "2026-06"',
    )
    with pytest.raises(AssertionError, match="row counts.*double"):
        _assert_the_month_default_cannot_pass(
            "bronze_estabelecimentos_job.yml", root=root
        )


def test_the_revision_default_lock_catches_a_default_that_would_pass_a_run(tmp_path):
    """Proves the OTHER lock can fail, in the shape that would be silent.

    A `revision:` default that happens to be a real object name turns every run
    launched without `--params revision=...` into a check that passes -- against
    whatever commit was pasted into the YAML, forever. That is the guard reporting
    green while verifying nothing, which is worse than no guard at all."""
    root = _mutated(
        "bronze_socios_job.yml",
        tmp_path,
        f'default: "{SENTINEL_REVISION}"',
        'default: "62ce88003113dc1ca198b19cfd00f5f5e20b9bd3"',
    )
    with pytest.raises(AssertionError, match="would pass"):
        _assert_the_revision_default_cannot_pass("bronze_socios_job.yml", root=root)
