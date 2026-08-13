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
from datetime import date
from pathlib import Path

import pytest
import yaml

from opl.bronze.registry import REGISTRY as BRONZE_REGISTRY
from opl.config import (
    DEFAULT,
    SENTINEL_MONTH,
    SESSION_TIMEZONE,
    SESSION_TIMEZONE_CONFIG,
    is_month,
)
from opl.gold.columns import GHOST_ROWS
from opl.gold.dimensions import DimensionLoadResult
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
)

_ENTRY_POINTS = ("gold_load_dimension", "gold_load_conformed_dimension", "gold_load_pit")

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
_TASK_PARAMETERS = {
    "gold_load_dimension": ("table", "months", "load_date"),
    "gold_load_conformed_dimension": ("table", "load_date"),
    "gold_load_pit": ("table", "load_date"),
}

# The two `{{...}}` references no task may spell for itself, and which position each one
# occupies is read from `_TASK_PARAMETERS` rather than hardcoded.
_REFERENCES = {"months": _MONTHS_PARAMETER, "load_date": _LOAD_DATE_REFERENCE}

# The two `opl.config` names every session pin is written from. Spelled as the IDENTIFIER
# and not as the value, because that is what the AST locks below look for: a task that
# hardcoded `"UTC"` would satisfy a check on the value and would be the second spelling.
SESSION_TIMEZONE_CONFIG_NAME = "SESSION_TIMEZONE_CONFIG"
SESSION_TIMEZONE_NAME = "SESSION_TIMEZONE"

# How many names each entry point qualifies through `opl.config.DEFAULT.table`. All three
# qualify three, for three different threes: the SCD2 task qualifies the dimension, its
# source satellite and that satellite's parent hub; the conformed task qualifies the
# dimension, the fact source whose members it measures against, and the satellite a
# calendar spans; the PIT task qualifies the table, its hub, and its satellites -- the
# last of those in ONE call inside a comprehension over the declared list, which is why
# the number is three and not four.
_QUALIFIED_NAMES = {
    "gold_load_dimension": 3, "gold_load_conformed_dimension": 3, "gold_load_pit": 3,
}


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


# --- the entry point itself ----------------------------------------------------------


@pytest.mark.parametrize("script", _ENTRY_POINTS)
def test_no_gold_entry_point_spells_a_catalog_or_a_schema(script):
    """The qualification comes from `opl.config.DEFAULT.table` and from nowhere else.

    `opl.gold.registry` states the division this task is the other half of: a spec
    carries an unqualified name and the loader takes qualified tables as arguments. Gold
    is where that division is most easily broken, because a gold task qualifies THREE
    names -- and Free Edition's single catalog is what would make a hardcoded one
    invisible."""
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
    assert len(qualifies) == _QUALIFIED_NAMES[script], (
        f"{script}.py calls DEFAULT.table(...) {len(qualifies)} times, not "
        f"{_QUALIFIED_NAMES[script]}. The SCD2 task qualifies the dimension, its source "
        "satellite and that satellite's parent hub; the conformed task qualifies the "
        "dimension, the fact source it measures against and the satellite a calendar "
        "spans"
    )


def _timezone_pins(tree: ast.Module) -> list[tuple[str, ...]]:
    """Every `<something>.conf.set(A, B)` in `tree`, as the pair of NAMES it was handed.

    Names and not values, because the point of the lock is that neither half is spelled
    here: a task that wrote `"UTC"` inline would pass a check on the value and would be
    the second spelling this repository keeps paying for."""
    return [
        tuple(argument.id for argument in node.args if isinstance(argument, ast.Name))
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "set"
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "conf"
    ]


def test_the_pinned_session_timezone_is_utc_and_is_spelled_once():
    """The two values every pin below is made of, asserted where they are DECLARED.

    A `TIMESTAMP` is UTC micros and every conversion into or out of one resolves through
    `spark.sql.session.timeZone`; local Spark inherits the OS zone and serverless
    defaults to UTC, so an unpinned gold build writes `dim_company`'s version boundaries
    three hours apart in the two places while the fact asks as of an `event_time` the
    payment contract stamps in UTC. `opl.config` argues it at length."""
    assert (SESSION_TIMEZONE_CONFIG, SESSION_TIMEZONE) == ("spark.sql.session.timeZone", "UTC")


@pytest.mark.parametrize("script", _ENTRY_POINTS)
def test_every_gold_entry_point_pins_the_session_timezone_after_it_gets_the_session(script):
    """THE PIN, ON THE ONE OBJECT THAT CAN CARRY IT.

    Serverless hands the task a session that already exists, so this cannot be a builder
    option the way `opl.spark.local_session` spells it -- it has to be set on the session
    the task was given, and it has to be set BEFORE anything reads a table, because the
    setting decides what `CAST(applied_date AS TIMESTAMP)` means.

    ASSERTED AS THE TWO IMPORTED NAMES rather than as a string: `opl.config` owns both
    halves for the reason it owns the catalog and the schema, and a task spelling `"UTC"`
    for itself is a coordinate that drifts."""
    tree = _tree(script)
    pins = _timezone_pins(tree)
    assert (SESSION_TIMEZONE_CONFIG_NAME, SESSION_TIMEZONE_NAME) in pins, (
        f"{script}.py does not call spark.conf.set({SESSION_TIMEZONE_CONFIG_NAME}, "
        f"{SESSION_TIMEZONE_NAME}); it found {pins}. Without it the build inherits "
        "whatever zone the cluster defaults to, and every interval bound it writes moves "
        "with it"
    )
    def line_of(matches) -> int:
        found = [node.lineno for node in ast.walk(tree) if matches(node)]
        assert len(found) == 1, f"{script}.py has {len(found)} of the call, expected 1"
        return found[0]

    got_session = line_of(
        lambda node: isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "getOrCreate"
    )
    pinned = line_of(
        lambda node: isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "set"
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "conf"
    )
    assert got_session < pinned, (
        f"{script}.py pins the session timezone on line {pinned}, before it has a "
        f"session on line {got_session}"
    )


def test_the_local_session_factory_pins_the_same_timezone_the_entry_points_do():
    """The other half, and it must be the SAME two names.

    `opl.spark.local_session` is what every Spark test in this repository runs against,
    so a local session left on the operating system's zone means the suite asserts one
    set of instants and the workspace writes another -- which is exactly the gap this
    lock exists to close. It is a builder `.config(...)` there rather than a `conf.set`
    because that factory CREATES the session; the values are the same."""
    source = (_REPO / "src" / "opl" / "spark.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="spark.py")
    configured = [
        tuple(argument.id for argument in node.args if isinstance(argument, ast.Name))
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "config"
    ]
    assert (SESSION_TIMEZONE_CONFIG_NAME, SESSION_TIMEZONE_NAME) in configured, (
        "opl/spark.py does not pass "
        f"({SESSION_TIMEZONE_CONFIG_NAME}, {SESSION_TIMEZONE_NAME}) to .config(...); it "
        f"passes {configured}"
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


@pytest.mark.parametrize(
    ("script", "wrong_kind"),
    [
        ("gold_load_dimension", "dim_date"),
        ("gold_load_dimension", "pit_estabelecimento"),
        ("gold_load_conformed_dimension", "dim_company"),
        ("gold_load_conformed_dimension", "pit_estabelecimento"),
        ("gold_load_pit", "dim_company"),
        ("gold_load_pit", "dim_date"),
    ],
)
def test_each_gold_entry_point_refuses_the_kind_the_other_one_builds(script, wrong_kind):
    """THE PASTE THE SECOND GOLD JOB MADE POSSIBLE, and the THIRD one makes six pairings
    of it. Every name here is registered, every one is spelled correctly, `table` is the
    only parameter two of the three tasks take, and each would be refused only after a
    serverless session had started if the kind were not checked on the driver.

    IT IS NOT A THEORETICAL SWAP. `gold_load_dimension` resolves its source through
    `spec.source_satellite`; `CalendarDimension` deliberately spells its own satellite
    field `applied_date_source` and `PointInTimeTable` spells its own `satellites` for
    exactly this reason, so the wrong kind cannot walk far enough into that task to build
    something plausible out of a spec that is not one.

    THE PIT IS BOTH DIRECTIONS AND THAT IS WHY IT IS THREE PAIRS AND NOT ONE. A kind added
    to the registry acquires the OTHER tasks' refusals for free only if those refusals are
    inclusions (`isinstance(spec, Scd2Dimension)`) rather than exclusions -- and this
    parametrisation is what proves they are, on the one file where an exclusion was
    actually found (see `opl.gold.registry
    ._assert_no_two_dimensions_draw_from_one_payment_column`)."""
    task = _load(script)
    with pytest.raises(ValueError):
        task.main([wrong_kind, "2026-06+2026-07", "2026-08-12T12:00:00"][: len(
            _TASK_PARAMETERS[script]
        )])


def test_the_conformed_entry_point_refuses_a_table_no_gold_registry_registers():
    """Refused before Spark, naming the alternatives, exactly as its sibling is."""
    task = _load("gold_load_conformed_dimension")
    with pytest.raises(ValueError, match="unknown gold table"):
        task.main(["dim_dat", "2026-08-12T12:00:00"])


def test_the_conformed_entry_point_validates_its_timestamp_before_the_session():
    """The only argument it takes besides the table, and it is checked ahead of
    `SparkSession.builder.getOrCreate()` -- a guard after the session would start one to
    reject an argument."""
    task = _load("gold_load_conformed_dimension")
    with pytest.raises(ValueError, match="ISO-8601"):
        task.main(["dim_channel", ""])


def test_the_pit_entry_point_refuses_a_table_no_gold_registry_registers():
    """Refused before Spark, naming the alternatives, exactly as its two siblings are."""
    task = _load("gold_load_pit")
    with pytest.raises(ValueError, match="unknown gold table"):
        task.main(["pit_estabeleciment", "2026-08-12T12:00:00"])


def test_the_pit_entry_point_validates_its_timestamp_before_the_session():
    """The only argument it takes besides the table, checked ahead of
    `SparkSession.builder.getOrCreate()` -- a guard after the session would start one to
    reject an argument, and this job's session is the most expensive in the repository."""
    task = _load("gold_load_pit")
    with pytest.raises(ValueError, match="ISO-8601"):
        task.main(["pit_estabelecimento", ""])


@pytest.mark.parametrize(
    ("appended", "already_present", "last_layer", "hub_keys", "expected"),
    [
        (144_193_416, 0, 72_318_968, 72_318_968, "which reconciles"),
        (144_193_416, 0, 72_318_960, 72_318_968, "does NOT reconcile"),
        (0, 144_193_416, 72_318_968, 72_318_968, "the re-run is a no-op"),
    ],
    ids=["reconciles", "hub keys with no history", "idempotent re-run"],
)
def test_the_pit_reconciliation_note_tells_three_states_apart(
    appended, already_present, last_layer, hub_keys, expected
):
    """THE THREE STATES MUST NOT READ ALIKE, which is why the note is a function.

    A shortfall means hub keys for which NO satellite ever wrote a row: they are absent
    from every layer of this table, and a run that printed only its own row count would
    leave that invisible -- the failure `gold_load_dimension.py` records for the mirror
    case, a satellite version whose hash key matched no hub row. The idempotent re-run is
    the third state and must not report a shortfall of everything.

    The numbers are Task 0's published ones, so this test also pins the reconciliation
    the job YAML predicts (`docs/f3-run-evidence.md` §0.5)."""
    task = _load("gold_load_pit")
    result = task.PitLoadResult(
        table="workspace.default.pit_estabelecimento",
        appended=appended,
        already_present=already_present,
        as_of_dates=(date(2026, 6, 13), date(2026, 7, 11)),
        hub_keys=hub_keys,
        rows_per_as_of=((date(2026, 6, 13), 71_874_448), (date(2026, 7, 11), last_layer)),
    )
    assert expected in task._reconciliation_note(result)


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


# --- the line the run log actually carries -------------------------------------------


def _dimension_result(*, appended: int, already_present: int, versions: int = 4):
    """A `DimensionLoadResult` with the two counts a caller controls.

    Hand-built, in `tests/vault/test_satellite_diagnostics.py::_result`'s shape and for
    its reason: the note is a pure function of a frozen dataclass, and driving it through
    a real 69.2M-row load to see one sentence would test the loader instead."""
    return DimensionLoadResult(
        table="workspace.default.dim_company",
        appended=appended,
        already_present=already_present,
        source_versions=versions,
        distinct_keys=appended + already_present,
    )


def test_the_idempotent_re_run_line_cannot_be_printed_over_a_short_dimension():
    """THE DEFECT IN ITS EXACT SHAPE. `_reconciliation_note`'s re-run branch fired on
    `appended == 0 and already_present` and compared `already_present` to NOTHING -- so a
    dimension permanently short of a dangling satellite version printed a clean "the
    re-run is a no-op" on every later run, and the shortfall the function exists to make
    visible was invisible on the one branch a retry lands in.

    THE TWO INPUTS DIFFER BY ONE ROW AND MUST NOT READ ALIKE: four versions plus the
    ghost is five, and a target holding four is the state the old branch called a no-op.

    `main` IS ASSERTED TO PRINT THROUGH IT, which is what keeps this from being
    self-referential -- a test of the note alone stays green while `main` prints the old
    sentence beside it. Same closing assertion as the vault's."""
    task = _load("gold_load_dimension")
    expected = 4 + GHOST_ROWS

    short_rerun = task._reconciliation_note(
        _dimension_result(appended=0, already_present=4)
    )
    whole_rerun = task._reconciliation_note(
        _dimension_result(appended=0, already_present=expected)
    )
    fresh = task._reconciliation_note(
        _dimension_result(appended=expected, already_present=0)
    )
    short_fresh = task._reconciliation_note(
        _dimension_result(appended=4, already_present=0)
    )

    assert "does NOT reconcile" in short_rerun and "no-op" not in short_rerun
    assert "does NOT reconcile" in short_fresh
    assert "no-op" in whole_rerun and "does NOT reconcile" not in whole_rerun
    assert str(expected) in whole_rerun, (
        "the re-run line names no total, so it reconciles against nothing -- which is "
        "the defect this test exists for"
    )
    assert "which reconciles" in fresh and "no-op" not in fresh
    assert "_reconciliation_note(result)" in Path(task.__file__).read_text(
        encoding="utf-8"
    ), "main no longer prints through _reconciliation_note, so this test measures nothing"


def test_the_ghost_row_count_is_one_constant_across_the_loader_and_the_task():
    """`GHOST_ROWS` is `opl.gold.columns`', and both gold entry points and both loaders
    read it. It was spelled twice -- once in `opl.gold.conformed`, once in this task --
    while `load_dimension` enforced nothing, so the two could disagree and the run log
    would print the arithmetic that agreed with itself. The loader now REFUSES a target
    that is not `source_versions + GHOST_ROWS`, which makes a second spelling a way to
    print a reconciliation against a total the load rejected."""
    task = _load("gold_load_dimension")
    assert task.GHOST_ROWS is GHOST_ROWS == 1
    source = (_SRC / "gold_load_dimension.py").read_text(encoding="utf-8")
    assert "GHOST_ROWS = " not in source, "the task declares a GHOST_ROWS of its own"


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
