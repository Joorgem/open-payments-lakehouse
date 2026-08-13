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
again and where the file it would otherwise have joined is at exactly 800 lines
(`.plans/2026-08-10-master-route-and-autonomy-protocol.md` section 4.12: whoever touches
it splits it first).

WHAT A GOLD TASK CAN GET WRONG THAT NO OTHER LAYER CAN. A gold job is handed ONE registry
key and resolves everything else -- the satellite it reads, that satellite's parent hub,
the payload, the business key -- from the vault registry, which is the decision
`opl.gold.registry.Scd2Dimension` is. So the paste this file is shaped around is not a
leftover SOURCE (there is no source parameter to leave behind) but a leftover TABLE: a
task handed `sat_empresa_dados`, or `bronze_cnpj_empresas`, names a table that EXISTS in
another layer's registry and would be refused only at run time, after a serverless start.

Nothing here starts Spark. The entry point is loaded by path with the importlib pattern
the other task tests use -- `databricks/src` scripts are job entry points, not part of
the opl wheel."""
from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest
import yaml

from opl.bronze.registry import REGISTRY as BRONZE_REGISTRY
from opl.config import DEFAULT, SENTINEL_MONTH, is_month
from opl.gold.registry import REGISTRY as GOLD_REGISTRY
from opl.vault import domains

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"
_RESOURCES = _REPO / "databricks" / "resources"

# THE GOLD JOBS, ENUMERATED. A glob would silently give a job added later whichever
# behaviour it happened to inherit, and the totality lock below -- every registered gold
# table built by exactly one task -- is only a claim about gold if this list is the whole
# of it. `test_the_gold_jobs_are_the_gold_yamls_on_disk` closes the other direction.
_GOLD_JOBS = ("gold_dim_company_job.yml",)

_ENTRY_POINTS = ("gold_load_dimension",)

# NAMED `gold_*` AND NOT `vault_*`, AND THAT IS A DECISION RATHER THAN A LABEL.
# `test_vault_job_wiring.py::test_the_four_vault_jobs_are_the_vault_yamls_on_disk` globs
# `vault_*.yml` and asserts the result equals its own list, and the totality lock beside
# it demands that every table in `opl.vault.domains.REGISTRY` be loaded by exactly one
# task of one of those files. A gold job named `vault_gold_...` would be swept into both.
_VAULT_PREFIX = "vault_"

_GUARD = "assert_deployed_revision"
_PYTHON_FILE_PREFIX = "../src/"
_MONTHS_PARAMETER = "{{job.parameters.months}}"
_LOAD_DATE_REFERENCE = "{{job.start_time.iso_datetime}}"

# [table, months, load_date] -- and there is NO source parameter, which is the point.
# A gold dimension declares the satellite it derives from, so the pairing cannot be got
# wrong by a copied YAML the way a vault task's (vault table, bronze source) pair can.
_TASK_PARAMETERS = ("table", "months", "load_date")


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
        assert len(parameters) == len(_TASK_PARAMETERS), (
            f"{job_yml}:{key} runs {script}.py and is handed {parameters}; that entry "
            f"point takes exactly {list(_TASK_PARAMETERS)}"
        )
        found[key] = (script, parameters[0])
    return found


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tree(script: str) -> ast.Module:
    return ast.parse((_SRC / f"{script}.py").read_text(encoding="utf-8"), filename=script)


def _non_docstring_strings(tree: ast.Module) -> list[str]:
    """Every string literal in `tree` that is not a docstring -- this repository's prose
    names the thing a module must NOT do in order to explain why, so a check over the raw
    text would refuse the explanation along with the thing."""
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.FunctionDef | ast.ClassDef)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    }
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    ]


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
    `vault_*.yml` and asserts the result equals its own four-entry list, then demands
    that every table in the VAULT registry be loaded by exactly one task of those files.
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
    -- which is the state this repository was in for its whole history until this branch,
    with `grep -ril "kimball|dim_|fact_"` returning nothing at all. A table built by TWO
    tasks is worse than either: the loader is append-only and idempotent, so the second
    task succeeds having appended nothing, and the run reports two builds."""
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
    """The two parameters no task may spell for itself.

    THE WINDOW, because it is a DECLARATION checked against the source rather than a
    filter applied to it (`opl.gold.dimensions`): an SCD2 build cannot be narrowed, so
    the parameter's whole job is to make a build refuse when the vault holds snapshots
    the launch did not name. A literal here would make it unsettable at launch, which is
    the one thing that would turn it back into decoration.

    THE LOAD TIMESTAMP, because `load_dimension` gives `load_date` no default precisely
    so it is a value the job's own parameters pin rather than a clock reading inside a
    task -- and `{{job.start_time.iso_datetime}}` is one value for every task of one run
    and survives a retry."""
    for key, task in _tasks_of(job_yml).items():
        if key == _GUARD:
            continue
        parameters = task["spark_python_task"]["parameters"]
        assert parameters[1:3] == [_MONTHS_PARAMETER, _LOAD_DATE_REFERENCE], (
            f"{job_yml}:{key} is handed {parameters}: the second and third must be "
            f"{_MONTHS_PARAMETER} and {_LOAD_DATE_REFERENCE}"
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


@pytest.mark.parametrize("job_yml", _GOLD_JOBS)
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


# --- the entry point itself ----------------------------------------------------------


@pytest.mark.parametrize("script", _ENTRY_POINTS)
def test_no_gold_entry_point_spells_a_catalog_or_a_schema(script):
    """The qualification comes from `opl.config.DEFAULT.table` and from nowhere else.

    `opl.gold.registry` states the division this task is the other half of: a spec
    carries an unqualified name and the loader takes qualified tables as arguments. Gold
    is where that division is most easily broken, because this task qualifies THREE
    names -- the dimension, the satellite it reads and that satellite's hub -- and Free
    Edition's single catalog is what would make a hardcoded one invisible."""
    qualification = f"{DEFAULT.catalog}.{DEFAULT.schema}."
    spelled = [
        value for value in _non_docstring_strings(_tree(script)) if qualification in value
    ]
    assert not spelled, (
        f"{script}.py spells {qualification!r} in {spelled}. Catalog and schema come from "
        "opl.config.DEFAULT.table(name); a second spelling is a coordinate that drifts "
        "the day this project is on a workspace with more than one catalog"
    )
    qualifies = [
        node
        for node in ast.walk(_tree(script))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "table"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "DEFAULT"
    ]
    assert len(qualifies) == 3, (
        f"{script}.py calls DEFAULT.table(...) {len(qualifies)} times; a gold build "
        "qualifies exactly three names -- the dimension, its source satellite and that "
        "satellite's parent hub"
    )


@pytest.mark.parametrize("script", _ENTRY_POINTS)
def test_no_gold_entry_point_raises_system_exit(script):
    """Serverless runs these under IPython, where an uncaught `SystemExit` reports a
    SUCCESSFUL run as FAILED. Every task under databricks/src calls a bare `main()`."""
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    assert "SystemExit" not in source
    assert source.rstrip().endswith('if __name__ == "__main__":\n    main()')


def test_the_entry_point_refuses_a_table_no_gold_registry_registers_before_spark():
    """`opl.gold.registry.table_spec` refuses before a session, naming the alternatives
    -- an operator who mistyped a table should not wait for a serverless start to be
    told so."""
    task = _load("gold_load_dimension")
    with pytest.raises(ValueError, match="unknown gold table"):
        task.main(["dim_compnay", "2026-06+2026-07", "2026-08-12T12:00:00"])


def test_the_entry_point_refuses_a_vault_table_handed_where_the_dimension_belongs():
    """The paste this layer actually produces. `sat_empresa_dados` is a real table, in a
    real registry, and it is the very table `dim_company` derives from -- so it is the
    single most plausible string to end up in that parameter. It is not a gold table, and
    the refusal has to say so before a session."""
    task = _load("gold_load_dimension")
    with pytest.raises(ValueError, match="unknown gold table"):
        task.main(["sat_empresa_dados", "2026-06+2026-07", "2026-08-12T12:00:00"])


def test_the_window_and_the_timestamp_are_validated_before_the_session():
    """BOTH HALVES IN ONE RUN, and neither is reachable any other way without Spark: a
    good window and a load date that cannot parse must fail on the LOAD DATE, which
    proves the window was accepted -- and that both guards stand ahead of
    `SparkSession.builder.getOrCreate()`, because a guard after the session would start
    one to reject an argument."""
    task = _load("gold_load_dimension")
    with pytest.raises(ValueError, match="ISO-8601"):
        task.main(["dim_company", "2026-06+2026-07", ""])
    with pytest.raises(ValueError, match="no months were given"):
        task.main(["dim_company", SENTINEL_MONTH, "2026-08-12T12:00:00"])


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
