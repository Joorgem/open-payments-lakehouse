"""Which gold table each gold job task builds, and from what -- the fourth file in the
wiring seam `test_task_wiring.py`, `test_job_yaml_wiring.py` and
`test_vault_job_wiring.py` already draw between a SCRIPT and the JOB that hands it
arguments.

WHY A FOURTH FILE. Each of the three has a subject and its own kind of mistake.
`test_job_yaml_wiring.py` owns the BRONZE ingestion flow (a landing dir, a checkpoint, a
gate verdict, a promote) and, because the revision guard is a property of every job
rather than of a layer, it also owns the guard -- so this file does not re-assert the
guard's position, its parameter or its environment, and `gold_dim_company_job.yml` is in
its `_GUARDED_JOBS` instead. `test_vault_job_wiring.py` owns the vault, where a task is
handed TWO registry keys. This file's subject is gold, where the mistakes are different
again and where the file it would otherwise have joined stood at exactly 800 lines when
this one was written -- the cap `tests/test_size_caps.py` enforces, and whoever touches
such a file splits it first.

WHAT A GOLD TASK CAN GET WRONG THAT NO OTHER LAYER CAN. A gold job is handed ONE registry
key and resolves everything else -- the satellite it reads, that satellite's parent hub,
the payload, the business key -- from the vault registry, which is the decision
`opl.gold.registry.Scd2Dimension` is. So the paste this file is shaped around is not a
leftover SOURCE (there is no source parameter to leave behind) but a leftover TABLE: a
task handed `sat_empresa_dados`, or `bronze_cnpj_empresas`, names a table that EXISTS in
another layer's registry and would be refused only at run time, after a serverless start.

AND IT IS NOW HALF A FILE, WHICH F3 TASK 4 HAD NO CHOICE ABOUT. It stood at 805 lines --
already past the project's 800-line cap, and the master protocol's own rule is that
whoever touches such a file splits it FIRST -- and a fourth gold entry point took it to
946. The split runs along the section comment this file already carried: everything about
a YAML declaring a TASK stayed here, everything about a PYTHON MODULE under
`databricks/src` moved to `tests/test_gold_entry_points.py`. Nothing crosses the seam but
`_REPO` and `_SRC`; `_TASK_PARAMETERS` below in particular did NOT go with it, because one
mapping in two files is the defect this repository keeps paying for.

Nothing here starts Spark, and nothing here loads a script: this file reads YAML and
asserts it against the registries."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from opl.bronze.registry import REGISTRY as BRONZE_REGISTRY
from opl.config import SENTINEL_MONTH, is_month
from opl.gold.registry import REGISTRY as GOLD_REGISTRY
from opl.vault import domains

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"
_RESOURCES = _REPO / "databricks" / "resources"

# THE GOLD JOBS, ENUMERATED. A glob would silently give a job added later whichever
# behaviour it happened to inherit, and the totality lock below -- every registered gold
# table built by exactly one task -- is only a claim about gold if this list is the whole
# of it. `test_the_gold_jobs_are_the_gold_yamls_on_disk` closes the other direction.
_GOLD_JOBS = (
    "gold_dim_company_job.yml",
    "gold_conformed_dimensions_job.yml",
    "gold_pit_estabelecimento_job.yml",
    "gold_fact_payment_job.yml",
)

# NAMED `gold_*` AND NOT `vault_*`, AND THAT IS A DECISION RATHER THAN A LABEL.
# `test_vault_job_wiring.py::test_the_vault_jobs_are_the_vault_yamls_on_disk` globs
# `vault_*.yml` and asserts the result equals its own list, and the totality lock beside
# it demands that every table in `opl.vault.domains.REGISTRY` be loaded by exactly one
# task of one of those files. A gold job named `vault_gold_...` would be swept into both.
_VAULT_PREFIX = "vault_"

_GUARD = "assert_deployed_revision"
_PYTHON_FILE_PREFIX = "../src/"
_MONTHS_PARAMETER = "{{job.parameters.months}}"
_LOAD_DATE_REFERENCE = "{{job.start_time.iso_datetime}}"

# WHAT EACH GOLD ENTRY POINT IS HANDED, BY SCRIPT -- and in neither case is there a
# SOURCE parameter, which is the point. A gold dimension declares what it derives from, so
# the pairing cannot be got wrong by a copied YAML the way a vault task's (vault table,
# bronze source) pair can.
#
# THE SECOND ONE TAKES NO `months` AND THAT IS THE DECISION THIS MAPPING RECORDS. Every
# other build job in this repository takes a month window: a bronze ingest STAMPS it, a
# vault load NARROWS to it, and the SCD2 build DECLARES it so a build refuses when the
# vault has gained a snapshot the launch did not name -- it must, because a version's end
# date is the next version's start and an append cannot revise a closed interval. A
# conformed dimension has no interval: `dim_channel` and `dim_currency` have no snapshot
# input at all, and a snapshot `dim_date` did not know about is days to APPEND with no
# surrogate key moving. A parameter the task ignored would be decoration, and a lock that
# demanded one of every gold task would be a lock forcing it.
#
# THE THIRD TAKES NO `months` EITHER, AND FOR A THIRD REASON. A PIT pointer is
# `max(applied_date <= as_of_date)`, so a snapshot loaded AFTER every one the table was
# built over moves nothing already written -- it is a layer to append. The one case that
# IS a revision, a snapshot backfilled BETWEEN two the table holds, is refused by
# `opl.gold.pit` from the dates on disk; a parameter would state an intention where the
# refusal reads the table.
#
# AND THE FOURTH FOR A FOURTH, WHICH IS THE ONE THAT IS ABOUT THE GRAIN RATHER THAN ABOUT
# THE MECHANISM. `months` names a window over the RFB's SNAPSHOTS; `fact_payment` is
# grained on PAYMENTS, whose arrival has nothing to do with a snapshot month. A batch that
# landed since the last build is a set of new `transaction_id` values, and
# `opl.gold.facts._new_rows` finds them by reading the target.
_TASK_PARAMETERS = {
    "gold_load_dimension": ("table", "months", "load_date"),
    "gold_load_conformed_dimension": ("table", "load_date"),
    "gold_load_pit": ("table", "load_date"),
    "gold_load_fact": ("table", "load_date"),
}

# The two `{{...}}` references no task may spell for itself, and which position each one
# occupies is read from `_TASK_PARAMETERS` rather than hardcoded.
_REFERENCES = {"months": _MONTHS_PARAMETER, "load_date": _LOAD_DATE_REFERENCE}


def _job_of(job_yml: str, root: Path = _RESOURCES) -> dict:
    """The ONE job declared in `job_yml`, asserted rather than assumed -- one job per
    plan task, so that each loader's wall-clock is a task duration somebody can read off
    the run."""
    doc = yaml.safe_load((root / job_yml).read_text(encoding="utf-8"))
    jobs = doc["resources"]["jobs"]
    assert len(jobs) == 1, f"{job_yml} declares {len(jobs)} jobs, expected exactly 1"
    return next(iter(jobs.values()))


def _tasks_of(job_yml: str, root: Path = _RESOURCES) -> dict[str, dict]:
    tasks = _job_of(job_yml, root)["tasks"]
    keys = [task["task_key"] for task in tasks]
    assert len(keys) == len(set(keys)), f"{job_yml} declares a task_key twice ({keys})"
    return {task["task_key"]: task for task in tasks}


def _script_of(task: dict, where: str) -> str:
    python_file = task["spark_python_task"]["python_file"]
    assert python_file.startswith(_PYTHON_FILE_PREFIX), (
        f"{where} runs {python_file!r}, which is not under {_PYTHON_FILE_PREFIX}"
    )
    script = python_file[len(_PYTHON_FILE_PREFIX):].removesuffix(".py")
    assert (_SRC / f"{script}.py").exists(), (
        f"{where} runs {python_file!r}, which is not a file under databricks/src"
    )
    return script


def _build_tasks(job_yml: str, root: Path = _RESOURCES) -> dict[str, tuple[str, str]]:
    """Every BUILD task of `job_yml`: task_key -> (script, gold table).

    The revision guard is excluded because it builds nothing, and its own wiring is
    asserted in `test_job_yaml_wiring.py`, which owns that argument."""
    found: dict[str, tuple[str, str]] = {}
    for key, task in _tasks_of(job_yml, root).items():
        if key == _GUARD:
            continue
        parameters = task["spark_python_task"]["parameters"]
        script = _script_of(task, f"{job_yml}:{key}")
        assert script in _TASK_PARAMETERS, (
            f"{job_yml}:{key} runs {script}.py, which is not a gold entry point this "
            f"file knows the parameters of ({', '.join(sorted(_TASK_PARAMETERS))})"
        )
        expected = _TASK_PARAMETERS[script]
        assert len(parameters) == len(expected), (
            f"{job_yml}:{key} runs {script}.py and is handed {parameters}; that entry "
            f"point takes exactly {list(expected)}"
        )
        found[key] = (script, parameters[0])
    return found


# --- the locks -----------------------------------------------------------------------


def test_the_gold_jobs_are_the_gold_yamls_on_disk():
    """`_GOLD_JOBS` is what every lock below iterates, so a gold job missing from it is a
    job none of them look at -- and the totality lock underneath would then say "every
    gold table is built" while ignoring the file that builds some of them."""
    on_disk = {path.name for path in _RESOURCES.glob("gold_*.yml")}
    assert on_disk == set(_GOLD_JOBS), (
        f"gold job YAML(s) not in _GOLD_JOBS: {sorted(on_disk - set(_GOLD_JOBS))}; "
        f"listed but absent: {sorted(set(_GOLD_JOBS) - on_disk)}"
    )


def test_no_gold_job_is_named_like_a_vault_job():
    """THE NAME IS LOAD-BEARING, not cosmetic. `test_vault_job_wiring.py` globs
    `vault_*.yml` and asserts the result equals its own list, then demands that every
    table in the VAULT registry be loaded by exactly one task of those files.
    A gold job whose filename began `vault_` would be swept into that glob and would turn
    both locks red -- the first because it is not in the list, the second because its
    task names a table no vault domain registers."""
    for job_yml in _GOLD_JOBS:
        assert not job_yml.startswith(_VAULT_PREFIX), (
            f"{job_yml} is named like a vault job; test_vault_job_wiring.py's glob would "
            "claim it"
        )


def test_every_registered_gold_table_is_built_by_exactly_one_task():
    """THE TOTALITY LOCK. A registered gold table with no task is a table nothing builds
    -- which is the state this repository was in for its whole history until this branch:
    the only mention of a dimensional layer anywhere in it was ADR 0011 forbidding a
    `dim_socio` (`src/opl/gold/__init__.py` records the measurement, and why the grep
    that used to be quoted here asserted nothing). A table built by TWO tasks is worse
    than either: the loader is append-only and idempotent, so the second task succeeds
    having appended nothing, and the run reports two builds."""
    built: dict[str, list[str]] = {}
    for job_yml in _GOLD_JOBS:
        for key, (_script, table) in _build_tasks(job_yml).items():
            built.setdefault(table, []).append(f"{job_yml}:{key}")
    assert set(built) == set(GOLD_REGISTRY), (
        f"registered gold tables no job builds: {sorted(set(GOLD_REGISTRY) - set(built))}; "
        f"tasks naming an unregistered table: {sorted(set(built) - set(GOLD_REGISTRY))}"
    )
    twice = {table: where for table, where in built.items() if len(where) > 1}
    assert twice == {}, f"gold tables built by more than one task: {twice}"


def _assert_every_task_is_handed_a_gold_table_and_no_other(
    job_yml: str, root: Path = _RESOURCES
) -> None:
    other_layers = set(domains.REGISTRY) | {
        name
        for spec in BRONZE_REGISTRY.values()
        for name in (spec.name, spec.staging, spec.bronze, spec.quarantine)
    }
    for key, task in _tasks_of(job_yml, root).items():
        if key == _GUARD:
            continue
        parameters = task["spark_python_task"]["parameters"]
        where = f"{job_yml}:{key}"
        assert parameters[0] in GOLD_REGISTRY, (
            f"{where} is handed {parameters}: the first parameter must be a registered "
            f"gold table ({', '.join(sorted(GOLD_REGISTRY))}), and {parameters[0]!r} is "
            "not one"
        )
        trespassing = [value for value in parameters if value in other_layers]
        assert not trespassing, (
            f"{where} is handed {parameters}, naming {trespassing} -- table(s) another "
            "layer's registry owns. A gold task takes ONE registry key and resolves its "
            "satellite, that satellite's parent hub, the payload and the business key "
            "from the vault; a vault or bronze name here is a paste that would be "
            "refused only after a serverless start"
        )


@pytest.mark.parametrize("job_yml", _GOLD_JOBS)
def test_every_task_is_handed_a_gold_table_and_no_other(job_yml):
    _assert_every_task_is_handed_a_gold_table_and_no_other(job_yml)


@pytest.mark.parametrize("job_yml", _GOLD_JOBS)
def test_every_task_is_handed_the_jobs_own_window_and_the_runs_start_time(job_yml):
    """The parameters no task may spell for itself, in the positions its own entry point
    declares them.

    THE WINDOW, wherever a task takes one, because it is a DECLARATION checked against
    the source rather than a filter applied to it (`opl.gold.dimensions`): an SCD2 build
    cannot be narrowed, so the parameter's whole job is to make a build refuse when the
    vault holds snapshots the launch did not name. A literal here would make it
    unsettable at launch, which is the one thing that would turn it back into decoration.

    THE LOAD TIMESTAMP, on every build task of every gold job, because both loaders give
    `load_date` no default precisely so it is a value the job's own parameters pin rather
    than a clock reading inside a task -- and `{{job.start_time.iso_datetime}}` is one
    value for every task of one run and survives a retry.

    DRIVEN OFF `_TASK_PARAMETERS` rather than off fixed positions, so the conformed
    task's missing `months` is a declared difference and not a hole: this lock still
    demands its `load_date` reference, in the position that task's argv puts it."""
    for key, task in _tasks_of(job_yml).items():
        if key == _GUARD:
            continue
        parameters = task["spark_python_task"]["parameters"]
        declared = _TASK_PARAMETERS[_script_of(task, f"{job_yml}:{key}")]
        expected = [_REFERENCES[name] for name in declared[1:]]
        assert parameters[1:] == expected, (
            f"{job_yml}:{key} is handed {parameters}: after the table it takes "
            f"{list(declared[1:])}, which must be spelled {expected}"
        )


def _months_taking_jobs() -> tuple[str, ...]:
    """The gold jobs at least one of whose tasks takes a month window.

    THE EXEMPTION IS DERIVED, NOT LISTED, which is what stops it becoming a way to opt
    out. A job is excused the `months` default lock exactly when no task of it takes a
    `months` argument at all -- read from `_TASK_PARAMETERS`, the same mapping the
    position lock above reads -- so a job that grows a month-taking task acquires the
    lock in the same edit, and a second list nobody updates cannot exist."""
    return tuple(
        job_yml
        for job_yml in _GOLD_JOBS
        if any(
            "months" in _TASK_PARAMETERS[script]
            for script, _table in _build_tasks(job_yml).values()
        )
    )


def test_a_gold_job_whose_tasks_take_no_window_declares_no_window_parameter():
    """A `months` parameter nothing reads is a parameter an operator would pass and a
    build would ignore -- which reads, in a run log, exactly like a window that was
    applied. The conformed job declares none, and this asserts the absence rather than
    letting it be inferred from a lock that simply skipped the file."""
    for job_yml in set(_GOLD_JOBS) - set(_months_taking_jobs()):
        names = {
            parameter["name"] for parameter in _job_of(job_yml).get("parameters", [])
        }
        assert "months" not in names, (
            f"{job_yml} declares a months parameter and no task of it takes one, so an "
            "operator passing --params months=... would change nothing and the run log "
            "would still name the window they chose"
        )


def _assert_the_months_default_cannot_pass(job_yml: str, root: Path = _RESOURCES) -> None:
    parameters = {
        parameter["name"]: parameter.get("default")
        for parameter in _job_of(job_yml, root).get("parameters", [])
    }
    assert "months" in parameters, (
        f"{job_yml} declares no `months` job parameter, so there is nothing for "
        "--params months=... to reach and every task falls back on a window nobody chose"
    )
    default = parameters["months"]
    assert not is_month(default), (
        f"{job_yml}'s months default is {default!r}, which `is_month` ACCEPTS as a real "
        "month -- so a run launched without --params months=... would declare that window "
        "and be refused against the vault's real snapshots, or, on the day the vault "
        "happens to hold exactly it, would build silently under a window nobody chose"
    )
    assert default == SENTINEL_MONTH, (
        f"{job_yml}'s months default is {default!r} rather than the sentinel the code "
        f"names ({SENTINEL_MONTH!r}). One sentinel, one spelling -- it is shared with the "
        "bronze ingestion jobs and the four vault jobs, whose reasons for it differ and "
        "whose value must not fork"
    )


@pytest.mark.parametrize("job_yml", _months_taking_jobs())
def test_the_months_default_refuses_rather_than_naming_a_window_nobody_chose(job_yml):
    _assert_the_months_default_cannot_pass(job_yml)


@pytest.mark.parametrize("job_yml", _GOLD_JOBS)
def test_every_gold_task_runs_unretried_in_the_declared_serverless_environment(job_yml):
    """`max_retries: 0` and the environment, on every task of every gold job.

    A retry matters here for a reason neither bronze's `_batch_id` argument nor the
    vault's anti-join argument covers: this loader REFUSES a target it did not write in
    the same run when the source has moved on, so a retry that lands after a successful
    write is the branch that has to stay green -- and it does, because the build is
    idempotent by (surrogate key, `valid_to`). `environment_version: "3"` is the one
    serverless client version this wheel installs under at all."""
    job = _job_of(job_yml)
    environments = {
        environment["environment_key"]: environment["spec"]
        for environment in job["environments"]
    }
    for key, task in _tasks_of(job_yml).items():
        where = f"{job_yml}:{key}"
        assert task.get("max_retries") == 0, f"{where} does not declare max_retries: 0"
        assert task.get("environment_key") in environments, (
            f"{where} names environment {task.get('environment_key')!r}, which this job "
            f"does not declare ({sorted(environments)})"
        )
        spec = environments[task["environment_key"]]
        assert spec["environment_version"] == "3"
        assert spec["dependencies"] == ["../../dist/*.whl"]


# --- mutation probes: the locks above, proved able to fail ---------------------------


def _mutated(job_yml: str, tmp_path: Path, old: str, new: str) -> Path:
    """`job_yml` copied into `tmp_path` with one substring replaced, the mutation
    asserted to have applied -- a probe that silently changed nothing proves nothing."""
    root = tmp_path / "resources"
    root.mkdir(parents=True, exist_ok=True)
    original = (_RESOURCES / job_yml).read_text(encoding="utf-8")
    assert old in original, f"the mutation target {old!r} is not in {job_yml}"
    (root / job_yml).write_text(original.replace(old, new, 1), encoding="utf-8")
    return root


def test_the_paste_lock_catches_the_task_left_naming_the_satellite_it_derives_from(
    tmp_path,
):
    """The defect in its exact shape, and it is the likeliest one: the dimension's
    parameter replaced by the vault table it reads. Both names exist, both are spelled
    correctly, and the run would fail only after a serverless start."""
    root = _mutated(
        "gold_dim_company_job.yml", tmp_path, '- "dim_company"', '- "sat_empresa_dados"'
    )
    with pytest.raises(AssertionError, match="must be a registered gold table"):
        _assert_every_task_is_handed_a_gold_table_and_no_other(
            "gold_dim_company_job.yml", root=root
        )


def test_the_months_default_lock_catches_a_real_window_nobody_would_have_chosen(tmp_path):
    """Proves the default lock can fail. A `months: "2026-06"` default makes every
    un-parameterised run declare a one-snapshot window -- which the loader then compares
    against the vault's real snapshot set, so it is refused today and would build
    silently on the day the vault happens to hold exactly that."""
    root = _mutated(
        "gold_dim_company_job.yml", tmp_path, f'default: "{SENTINEL_MONTH}"', 'default: "2026-06"'
    )
    with pytest.raises(AssertionError, match="ACCEPTS as a real month"):
        _assert_the_months_default_cannot_pass("gold_dim_company_job.yml", root=root)
