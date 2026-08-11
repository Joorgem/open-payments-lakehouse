"""Which vault table each vault job task loads, from which bronze source, and in what
order -- the third file in the wiring seam `test_task_wiring.py` and
`test_job_yaml_wiring.py` already draw between a SCRIPT and the JOB that hands it
arguments.

WHY A THIRD FILE AND NOT A SECTION OF THE SECOND. `test_job_yaml_wiring.py` is at 731
lines against this project's 800-line cap, and the seam is real rather than a line
count: that file's subject is the BRONZE ingestion flow -- a table's landing dir, its
checkpoint, its gate verdict, its promote -- and every helper in it resolves a
`opl.bronze.registry` key. This file's subject is the vault, where a task is handed TWO
registry keys (a vault table and a bronze source) and the mistakes are different in
kind. The one thing that does cross is the revision guard, and it stays there: the four
vault jobs are in its `_GUARDED_JOBS` so the guard's position, its parameter and its
environment are asserted by the file that owns that argument, not re-asserted here.

WHAT A COPIED VAULT JOB GETS WRONG, which is what every lock below is shaped around.
These four YAMLs were written from one template and each task carries a vault table name
AND a bronze table name, so a paste can leave either behind -- and both wrong values are
the names of tables that EXIST:

  - `sat_empresa_dados` loaded from `estabelecimentos`. The satellite's payload columns
    are not in that contract, so this one fails -- but only inside Spark, after a
    session and a scan, and only because `refuse_non_string_columns` happens to name
    them. `test_every_task_reads_a_bronze_table_that_carries_the_columns_its_loader
    _demands` refuses it here, before a deploy.
  - `hub_estabelecimento` loaded from `empresas`. `cnpj_ordem` and `cnpj_dv` are not in
    that contract either -- same lock.
  - `link_company_partner` pointed at `vault_load_link.py`. `load_link` refuses it, and
    the refusal is the whole reason there are two link loaders; the lock below derives
    which entry point a link needs from the SAME condition that refusal tests, so the
    two cannot drift.
  - A satellite whose parent hub is loaded by a LATER task in the same job. Nothing
    fails: the satellite computes its own hash key from the source rather than joining
    to the hub, so what it produces is rows referencing hub rows that do not exist yet.

AND THE ONE THAT IS NOT A PASTE: a window too narrow to close an effectivity window.
That is not a wrong value at all -- it is a legitimate, well-formed window that cannot
produce the table's defining output, and it reports success. The guard, its reason, and
the zero it exists to prevent are pinned in the last section of this file and measured
against real Spark in `tests/vault/test_effectivity_window.py`.

Nothing here starts Spark: every assertion is about wiring. The entry points are loaded
by path with the importlib pattern the other task tests use -- `databricks/src` scripts
are job entry points, not part of the opl wheel."""
from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest
import yaml

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT, SENTINEL_MONTH, is_month
from opl.contracts.cnpj_schemas import columns_for
from opl.vault import domains
from opl.vault.domains import cnpj as cnpj_domain
from opl.vault.job_params import optional_flag
from opl.vault.observation import ObservationGrain
from opl.vault.registry import (
    EffectivitySatellite,
    Hub,
    Link,
    ReferenceTable,
    Satellite,
    VaultTable,
)

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"
_RESOURCES = _REPO / "databricks" / "resources"

# THE FOUR VAULT JOBS, ENUMERATED. A glob would silently give a job added later
# whichever behaviour it happened to inherit, and the totality lock below -- every
# registered vault table is loaded by exactly one task -- is only a claim about the
# vault if this list is the whole of it. `test_the_four_vault_jobs_are_the_vault_yamls
# _on_disk` closes the other direction.
_VAULT_JOBS = (
    "vault_empresa_job.yml",
    "vault_estabelecimento_job.yml",
    "vault_partner_job.yml",
    "vault_reference_job.yml",
)

_ENTRY_POINTS = (
    "vault_load_hub",
    "vault_load_satellite",
    "vault_load_link",
    "vault_load_partner_link",
    "vault_load_effectivity",
    "vault_load_reference",
)

_GUARD = "assert_deployed_revision"
_PYTHON_FILE_PREFIX = "../src/"
_MONTHS_PARAMETER = "{{job.parameters.months}}"
_LOAD_DATE_REFERENCE = "{{job.start_time.iso_datetime}}"

# THE ONE ENTRY POINT THAT TAKES A FIFTH PARAMETER. Every vault load task is handed
# [table, source, months, load_date]; the descriptive satellite takes `report_diagnostics`
# after them, because it is the only loader here with optional work to skip. The arity is
# derived from the SCRIPT below rather than allowed to be "4 or 5", so a fifth parameter
# handed to a loader that ignores it -- the shape a copy of a satellite task produces --
# is refused here instead of being passed to nothing.
_DIAGNOSTICS_SCRIPT = "vault_load_satellite"

# The one loader per kind, EXCEPT for links -- see `_entry_point_for`, where the split
# between the two link loaders is derived rather than listed.
_ENTRY_POINT_OF_KIND: dict[type, str] = {
    Hub: "vault_load_hub",
    Satellite: "vault_load_satellite",
    EffectivitySatellite: "vault_load_effectivity",
    ReferenceTable: "vault_load_reference",
}


# --- reading the YAMLs ---------------------------------------------------------------


def _job_of(job_yml: str, root: Path = _RESOURCES) -> dict:
    """The ONE job declared in `job_yml`.

    One is asserted rather than assumed, for the reason the bronze half asserts it: a
    helper that silently read the first of several would be reading a job whose
    existence already broke the decomposition -- one job per plan task, so that each
    loader's wall-clock is a task duration somebody can read off the run."""
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


def _load_tasks(job_yml: str, root: Path = _RESOURCES) -> dict[str, tuple[str, str, str]]:
    """Every LOAD task of `job_yml`: task_key -> (script, vault table, bronze source).

    The revision guard is excluded because it loads nothing, and its own wiring is
    asserted in `test_job_yaml_wiring.py`, which owns that argument."""
    found: dict[str, tuple[str, str, str]] = {}
    for key, task in _tasks_of(job_yml, root).items():
        if key == _GUARD:
            continue
        parameters = task["spark_python_task"]["parameters"]
        script = _script_of(task, f"{job_yml}:{key}")
        names = ["table", "source", "months", "load_date"]
        if script == _DIAGNOSTICS_SCRIPT:
            names.append("report_diagnostics")
        assert len(parameters) == len(names), (
            f"{job_yml}:{key} runs {script}.py and is handed {parameters}; that entry "
            f"point takes exactly {names}"
        )
        found[key] = (script, parameters[0], parameters[1])
    return found


def _ancestors(tasks: dict[str, dict], key: str) -> set[str]:
    """Every task that must have finished before `key` may start, transitively."""
    seen: set[str] = set()
    frontier = [key]
    while frontier:
        for dependency in tasks[frontier.pop()].get("depends_on", []):
            name = dependency["task_key"]
            assert name in tasks, f"a task depends on {name!r}, which this job does not declare"
            if name not in seen:
                seen.add(name)
                frontier.append(name)
    return seen


# --- what each vault table needs -----------------------------------------------------


def _is_a_derived_link(link: Link) -> bool:
    """Does this link have an end `load_link` cannot compute?

    THE SAME CONDITION `opl.vault.links._refuse_a_link_this_loader_cannot_write` TESTS,
    read from the spec rather than restated as a list of table names. That refusal is
    the reason there are two link loaders at all: `load_link` computes every end's
    reference from the columns that hub is NAMED after, and a non-identifying end's
    business key is derived instead -- so both ends of `link_company_partner` would be
    hashed from `cnpj_basico` and every relationship would read as a company partnered
    with itself, with the right row count and working joins. Deriving the routing from
    the condition means a wave-2 link with a dependent-child key is routed correctly by
    this lock on the day it is registered, rather than passing it by omission."""
    return bool(link.dependent_child_keys) or any(not end.identifying for end in link.ends)


def _entry_point_for(spec: VaultTable) -> str:
    """The one `databricks/src` script that can load `spec`."""
    if isinstance(spec, Link):
        return "vault_load_partner_link" if _is_a_derived_link(spec) else "vault_load_link"
    entry_point = _ENTRY_POINT_OF_KIND.get(type(spec))
    assert entry_point is not None, (
        f"vault table {spec.name!r} is a {type(spec).__name__}, a kind no entry point "
        "under databricks/src loads. A new table kind needs one, or it is a registered "
        "table no job can write"
    )
    return entry_point


def _required_source_columns(spec: VaultTable) -> tuple[str, ...]:
    """The source columns the loader for `spec` will demand by name.

    MIRRORS EACH LOADER'S OWN `refuse_non_string_columns` CALL, which is the list that
    decides whether a (vault table, bronze source) pairing can work at all. Asserting it
    here is what turns "that pairing fails in Spark, eventually, if we are lucky" into
    "that pairing is refused before the bundle is deployed"."""
    if isinstance(spec, Hub):
        return spec.business_key_columns
    if isinstance(spec, Satellite):
        return (*domains.parent_hub(spec).business_key_columns, *spec.payload_columns)
    if isinstance(spec, Link):
        hubs = domains.linked_hubs(spec)
        if _is_a_derived_link(spec):
            # `partner_link_candidates` refuses the COMPANY end's key and the two
            # dependent-child keys; the partner end is derived from one of the latter.
            return (*hubs[0].business_key_columns, *spec.dependent_child_key_columns)
        return tuple(name for hub in hubs for name in hub.business_key_columns)
    if isinstance(spec, EffectivitySatellite):
        link = domains.parent_link(spec)
        return (*domains.link_identity_columns(link), spec.entry_column)
    assert isinstance(spec, ReferenceTable), f"no column list is known for {spec.name!r}"
    return (spec.natural_key, spec.payload)


def _parents_in(spec: VaultTable) -> tuple[str, ...]:
    """The vault tables `spec`'s rows reference, which must be loaded before it."""
    if isinstance(spec, Satellite):
        return (domains.parent_hub(spec).name,)
    if isinstance(spec, Link):
        return tuple(dict.fromkeys(spec.hub_names))
    if isinstance(spec, EffectivitySatellite):
        return (spec.parent,)
    return ()


# --- the locks -----------------------------------------------------------------------


def test_the_four_vault_jobs_are_the_vault_yamls_on_disk():
    """`_VAULT_JOBS` is what every lock below iterates, so a vault job missing from it
    is a job none of them look at -- and the totality lock underneath would then say
    "every vault table is loaded" while ignoring the file that loads some of them."""
    on_disk = {path.name for path in _RESOURCES.glob("vault_*.yml")}
    assert on_disk == set(_VAULT_JOBS), (
        f"vault job YAML(s) not in _VAULT_JOBS: {sorted(on_disk - set(_VAULT_JOBS))}; "
        f"listed but absent: {sorted(set(_VAULT_JOBS) - on_disk)}"
    )


def test_every_registered_vault_table_is_loaded_by_exactly_one_task():
    """THE TOTALITY LOCK, and it is the one this phase's goal rests on: "every vault
    table F2 wave 1 modelled exists in `workspace.default`, built by its own loader".

    A registered table with no task is a table nothing loads -- which is precisely the
    state this branch was in until these four YAMLs existed, seventeen modules and 932
    tests deep, with `grep -rl vault databricks/` returning nothing. And a table loaded
    by TWO tasks is worse than either: both loaders are insert-only anti-joins, so the
    second one succeeds having appended nothing, and the run reports two loads."""
    loaded: dict[str, list[str]] = {}
    for job_yml in _VAULT_JOBS:
        for key, (_, table, _source) in _load_tasks(job_yml).items():
            loaded.setdefault(table, []).append(f"{job_yml}:{key}")
    registered = set(domains.REGISTRY)
    assert set(loaded) == registered, (
        f"registered vault tables no job loads: {sorted(registered - set(loaded))}; "
        f"tasks naming an unregistered table: {sorted(set(loaded) - registered)}"
    )
    # `hub_empresa` IS THE ONE EXCEPTION AND IT IS DECLARED, not tolerated: it has two
    # feeds (empresas, and estabelecimentos' `cnpj_basico`), so it is loaded twice ON
    # PURPOSE and the second load must append 0. Every other table is loaded once.
    twice = {table: where for table, where in loaded.items() if len(where) > 1}
    assert twice == {
        "hub_empresa": ["vault_empresa_job.yml:hub_empresa",
                        "vault_estabelecimento_job.yml:hub_empresa_from_estabelecimentos"]
    }, f"vault tables loaded by more than one task: {twice}"


def _assert_every_task_runs_the_entry_point_its_kind_needs(
    job_yml: str, root: Path = _RESOURCES
) -> None:
    for key, (script, table, _source) in _load_tasks(job_yml, root).items():
        expected = _entry_point_for(domains.table_spec(table))
        assert script == expected, (
            f"{job_yml}:{key} runs {script}.py for vault table {table!r}, which needs "
            f"{expected}.py. Each loader in opl.vault takes one kind of spec; the wrong "
            "one either refuses (which is the good case) or writes a plausible table "
            "about something else"
        )


@pytest.mark.parametrize("job_yml", _VAULT_JOBS)
def test_every_task_runs_the_entry_point_its_tables_kind_needs(job_yml):
    _assert_every_task_runs_the_entry_point_its_kind_needs(job_yml)


def _assert_every_source_carries_what_its_loader_demands(
    job_yml: str, root: Path = _RESOURCES
) -> None:
    for key, (_script, table, source) in _load_tasks(job_yml, root).items():
        spec = domains.table_spec(table)
        contract = set(columns_for(bronze_table_spec(source).contract))
        needed = _required_source_columns(spec)
        missing = [column for column in needed if column not in contract]
        assert not missing, (
            f"{job_yml}:{key} loads {table!r} from bronze {source!r}, whose contract has "
            f"no {missing}. The loader refuses those columns by name, so this pairing "
            "cannot work -- and it is exactly what a copied task leaves behind, because "
            "both names are tables that exist"
        )


@pytest.mark.parametrize("job_yml", _VAULT_JOBS)
def test_every_task_reads_a_bronze_table_that_carries_the_columns_its_loader_demands(job_yml):
    """The (vault table, bronze source) PAIRING, checked against the columns the loader
    will demand -- before a deploy rather than inside a serverless session."""
    _assert_every_source_carries_what_its_loader_demands(job_yml)


@pytest.mark.parametrize("job_yml", _VAULT_JOBS)
def test_a_table_is_loaded_after_every_table_it_references_that_the_same_job_loads(job_yml):
    """Dependency order, and it is NOT what makes the digests agree.

    Every loader computes its hub or link references from the source through
    `opl.vault.loading.hash_key_expression` rather than joining to the parent, so a
    satellite loaded first still keys correctly. What the order buys is that no row ever
    references a parent row that does not exist yet -- a dangling reference nothing
    errors on, on tables that are insert-only, so the repair is deleting rows by hand.

    Scoped to parents THIS job loads, deliberately. `vault_partner_job.yml` does not load
    `hub_empresa`, which both ends of its link reference: that hub is the empresa job's,
    and no `depends_on` reaches across two jobs. What covers that half is not this lock
    but `opl.vault.links.refuse_unloaded_hubs`, inside both link loaders."""
    tasks = _tasks_of(job_yml)
    loads = _load_tasks(job_yml)
    task_of_table = {table: key for key, (_, table, _source) in loads.items()}
    for key, (_script, table, _source) in loads.items():
        for parent in _parents_in(domains.table_spec(table)):
            if parent not in task_of_table or task_of_table[parent] == key:
                continue
            assert task_of_table[parent] in _ancestors(tasks, key), (
                f"{job_yml}:{key} loads {table!r}, which references {parent!r}, and can "
                f"start before {task_of_table[parent]!r} has finished. Nothing would "
                "fail: the rows would reference parent rows that are not there yet"
            )


@pytest.mark.parametrize("job_yml", _VAULT_JOBS)
def test_every_task_is_handed_the_jobs_own_window_and_the_runs_start_time(job_yml):
    """The two parameters no task may spell for itself.

    THE WINDOW, because every task of one job must read the same months: a job whose
    satellite ran over a narrower window than its hub would produce a satellite that
    silently describes fewer keys, with both tasks green. The reference
    `{{job.parameters.months}}` is what makes one `--params` reach all of them.

    THE LOAD TIMESTAMP, because `opl.vault.hubs` gives `load_date` no default precisely
    so that it is "a value the job's own parameters pin" rather than a clock reading
    inside a loader -- and a task calling `datetime.now()` would reintroduce that one
    layer out, leaving the hub and its satellite disagreeing about when the run
    happened. `{{job.start_time.iso_datetime}}` is one value for every task of one run
    and survives a retry."""
    for key, task in _tasks_of(job_yml).items():
        if key == _GUARD:
            continue
        parameters = task["spark_python_task"]["parameters"]
        assert parameters[2:4] == [_MONTHS_PARAMETER, _LOAD_DATE_REFERENCE], (
            f"{job_yml}:{key} is handed {parameters}: the third and fourth must be "
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
        "month -- so a run launched without --params months=... would load that window "
        "silently. Worse for the partner job than for any bronze one: a single-month "
        "default there makes the effectivity satellite close ZERO windows and report "
        "success. A job-parameter default cannot validate anything; it can only refuse"
    )
    assert default == SENTINEL_MONTH, (
        f"{job_yml}'s months default is {default!r} rather than the sentinel the code "
        f"names ({SENTINEL_MONTH!r}). One sentinel, one spelling -- it is shared with the "
        "four bronze ingestion jobs, whose reason for it does not transfer (bronze STAMPS "
        "the month onto rows; the vault only reads it) but whose value must not fork"
    )


@pytest.mark.parametrize("job_yml", _VAULT_JOBS)
def test_the_months_default_refuses_rather_than_naming_a_window_nobody_chose(job_yml):
    _assert_the_months_default_cannot_pass(job_yml)


@pytest.mark.parametrize("job_yml", _VAULT_JOBS)
def test_the_diagnostics_flag_is_declared_exactly_where_a_task_is_handed_one(job_yml):
    """THE ONLY VAULT JOB PARAMETER WHOSE DEFAULT IS NOT A REFUSAL, and the three ways
    that can go wrong are all silent. Declared and passed to no task: settable at launch,
    connected to nothing, so `--params report_diagnostics=true` runs the cheap load and
    the "not measured" line reads as a bug. Passed but not declared: an unresolved
    reference reaches the task verbatim and is refused there, after a serverless start.
    Declared with a default that parses ON: every un-parameterised run pays for two full
    extra passes over the source, which this flag exists to stop being unconditional.

    The default is parsed by the SAME function the task parses it with, so "off" here
    means what the loader will do rather than what the string looks like."""
    name = _load(_DIAGNOSTICS_SCRIPT).DIAGNOSTICS_PARAMETER
    reference = f"{{{{job.parameters.{name}}}}}"
    tasks = _tasks_of(job_yml)
    handed = [key for key, (script, _t, _s) in _load_tasks(job_yml).items()
              if script == _DIAGNOSTICS_SCRIPT]
    declared = {
        parameter["name"]: parameter.get("default")
        for parameter in _job_of(job_yml).get("parameters", [])
    }

    assert (name in declared) == bool(handed), (
        f"{job_yml} declares {sorted(declared)} and hands {name!r} to {handed}: declared "
        "and passed to no task is a launch parameter wired to nothing; passed and not "
        "declared hands its tasks an unresolved reference"
    )
    for key in handed:
        parameters = tasks[key]["spark_python_task"]["parameters"]
        assert parameters[4] == reference, (
            f"{job_yml}:{key} is handed {parameters} -- the fifth must be {reference}, "
            "never a literal, or the job parameter cannot reach it"
        )
    if handed:
        assert optional_flag(declared[name], parameter=name) is False, (
            f"{job_yml}'s {name} default is {declared[name]!r}, which parses ON. Every run "
            "launched without --params would pay for a second full scan of the source and "
            "a materialised observation ledger it never asked for"
        )


@pytest.mark.parametrize("job_yml", _VAULT_JOBS)
def test_every_vault_task_runs_unretried_in_the_declared_serverless_environment(job_yml):
    """`max_retries: 0` and the environment, on every task of every vault job.

    A retry matters here for a reason the bronze jobs' `_batch_id` argument does not
    cover: these loaders are insert-only anti-joins and are NOT concurrency-safe
    (`opl.vault.hubs`), so two attempts overlapping can both see a key absent and both
    append it. `environment_version: "3"` is the one serverless client version this
    wheel installs under at all."""
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


# --- the entry points themselves -----------------------------------------------------


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tree(script: str) -> ast.Module:
    return ast.parse((_SRC / f"{script}.py").read_text(encoding="utf-8"), filename=script)


def _non_docstring_strings(tree: ast.Module) -> list[str]:
    """Every string literal in `tree` that is not a docstring.

    The docstrings are excluded for `test_git_is_consulted_at_build_time_and_nowhere_the
    _artefact_runs`' reason: this repository's prose names the thing a module must NOT do
    in order to explain why, and a check over the raw text would refuse the explanation
    along with the thing. Comments never reach the AST, so they are free."""
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
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
        and id(node) not in docstrings
    ]


@pytest.mark.parametrize("script", _ENTRY_POINTS)
def test_no_vault_entry_point_spells_a_catalog_or_a_schema(script):
    """The qualification comes from `opl.config.DEFAULT.table` and from nowhere else.

    `opl.vault.registry` states the division these tasks are the other half of: a spec
    carries an unqualified name, the loaders take a qualified table as an argument, and
    `opl.config` is consulted "by whatever calls a loader" -- which, until this branch,
    was nothing at all. A literal `workspace.default.` here would be that consultation
    forked, and Free Edition's single catalog is what would make the fork invisible."""
    qualification = f"{DEFAULT.catalog}.{DEFAULT.schema}."
    spelled = [
        value for value in _non_docstring_strings(_tree(script)) if qualification in value
    ]
    assert not spelled, (
        f"{script}.py spells {qualification!r} in {spelled}. Catalog and schema come "
        "from opl.config.DEFAULT.table(name); a second spelling is a coordinate that "
        "drifts the day this project is on a workspace with more than one catalog"
    )
    qualifies = [
        node for node in ast.walk(_tree(script))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "table"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "DEFAULT"
    ]
    assert qualifies, f"{script}.py never calls DEFAULT.table(...), so it qualifies nothing"


@pytest.mark.parametrize("script", _ENTRY_POINTS)
def test_no_vault_entry_point_raises_system_exit(script):
    """Serverless runs these under IPython, where an uncaught `SystemExit` reports a
    SUCCESSFUL run as FAILED. Every task under databricks/src calls a bare `main()`;
    these six are new, so the shape is pinned rather than assumed."""
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    assert "SystemExit" not in source
    assert source.rstrip().endswith('if __name__ == "__main__":\n    main()')


def _grains_the_jobs_build() -> list[tuple[str, str, str, ObservationGrain]]:
    """Every `(job, task, script, grain)` the vault YAMLs' load tasks build, skipping the
    tasks whose loader takes no observation grain.

    Extracted from the test below only so that test stays inside the project's 50-line
    cap. The branch is on SPEC KIND because that is what decides which entry point owns a
    table, and it is the same discrimination the two loaders make internally."""
    satellite_task = _load("vault_load_satellite")
    effectivity_task = _load("vault_load_effectivity")
    built: list[tuple[str, str, str, ObservationGrain]] = []
    for job_yml in _VAULT_JOBS:
        for key, (script, table, source) in _load_tasks(job_yml).items():
            spec = domains.table_spec(table)
            if isinstance(spec, Satellite):
                grain = satellite_task.grain_for(
                    domains.parent_hub(spec), bronze_table_spec(source)
                )
            elif isinstance(spec, EffectivitySatellite):
                grain = effectivity_task.grain_for(
                    domains.parent_link(spec), bronze_table_spec(source)
                )
            else:
                continue
            built.append((job_yml, key, script, grain))
    return built


def test_the_grain_this_task_builds_is_the_grain_the_domain_declares():
    """THE ONE PLACE THE JOB LAYER RE-DERIVES SOMETHING THE DOMAIN ALREADY DECLARED, and
    the reason it is asserted rather than argued.

    `opl/vault/domains/cnpj.py` declares `EMPRESA_GRAIN`, `ESTABELECIMENTO_GRAIN` and
    `COMPANY_PARTNER_GRAIN` -- an observation grain is not a registered table, so there
    is no way for a job to reach one by name without a mapping that would itself be the
    second spelling. So the two satellite entry points call the SAME constructor over the
    SAME two registry entries, and this compares the result against what the domain
    declares, per (satellite task, bronze source) pairing the YAMLs actually carry.

    The grain is the argument whose mistakes are invisible in the output: it decides the
    reported departure count for a descriptive satellite and WHICH WINDOWS CLOSE for the
    effectivity one. The loaders' own `_refuse_a_mismatched_grain` covers the runtime
    half; this covers the half that would otherwise be a paragraph of prose."""
    declared = {
        value.name: value
        for value in vars(cnpj_domain).values()
        if isinstance(value, ObservationGrain)
    }
    built = _grains_the_jobs_build()
    for job_yml, key, script, grain in built:
        assert grain.name in declared, (
            f"{job_yml}:{key} ({script}) builds a grain called {grain.name!r}, which "
            f"opl.vault.domains.cnpj declares no constant for ({sorted(declared)})"
        )
        assert grain == declared[grain.name], (
            f"{job_yml}:{key} builds {grain} and the domain declares "
            f"{declared[grain.name]} -- the job layer and the domain disagree about "
            "which tables the ledger reads or which columns key it"
        )
    # DERIVED FROM THE REGISTRY rather than written as a literal: every registered table
    # whose loader takes an observation grain must have had one checked. With the
    # totality lock above -- each registered table loaded by exactly one task -- these
    # are the same number, so a wave-2 satellite is covered here on the day it is
    # registered rather than on the day somebody remembers to raise a constant.
    expected = sum(
        isinstance(spec, Satellite | EffectivitySatellite)
        for spec in domains.REGISTRY.values()
    )
    assert len(built) == expected, (
        f"{len(built)} of the {expected} registered tables whose loader takes an "
        "observation grain had one checked here"
    )


# --- the window that cannot close a window -------------------------------------------
#
# The tension this phase names, and the guard that closes it. `observation_ledger`
# derives its key universe from the same window it reports on, so over ONE month every
# key it knows about is present in the only month it is asked about and no key can reach
# `absent_after_observation` -- the one state that closes an effectivity window. A
# one-month window therefore closes ZERO windows for any data, always, and reports
# success doing it. `tests/vault/test_effectivity_window.py` measures that zero against
# real Spark; what is pinned here is that the job layer refuses the window rather than
# running it.

_A_GOOD_LOAD_DATE = "2026-08-09T12:00:00"


def test_the_effectivity_task_refuses_a_window_too_narrow_to_close_anything():
    """The guard, driven through `main` rather than called directly, so what is pinned
    is that it is ON the path and not merely present in the file."""
    task = _load("vault_load_effectivity")
    with pytest.raises(ValueError, match="closes a window on absence"):
        task.main(["sat_eff_company_partner", "socios", "2026-07", _A_GOOD_LOAD_DATE])


def test_the_window_guard_runs_before_the_session_and_lets_two_months_through():
    """BOTH HALVES IN ONE RUN, and neither is reachable any other way without Spark.

    Two months and a load date that cannot parse: the failure must be the LOAD DATE's,
    which proves the window guard passed on two months -- and it proves the guard stands
    ahead of `SparkSession.builder.getOrCreate()`, because a guard after the session
    would start one to reject an argument. A refusal that costs a serverless start is a
    refusal an operator learns to route around."""
    task = _load("vault_load_effectivity")
    with pytest.raises(ValueError, match="ISO-8601"):
        task.main(["sat_eff_company_partner", "socios", "2026-06+2026-07", ""])


def test_a_repeated_month_cannot_inflate_the_window_past_the_guard():
    """`months=2026-07+2026-07` is two entries and ONE month.

    The ledger folds a duplicate away and answers the same, so nothing in the library
    cares -- which is exactly why the refusal lives in `opl.vault.job_params` and why it
    is worth a test of its own. The guard above measures narrowness by COUNTING months;
    admitted, this typo would carry a one-month window straight past it and back into the
    zero-closes load."""
    task = _load("vault_load_effectivity")
    with pytest.raises(ValueError, match="more than once"):
        task.main(["sat_eff_company_partner", "socios", "2026-07+2026-07", _A_GOOD_LOAD_DATE])


def test_a_task_handed_no_diagnostics_flag_runs_the_cheap_load_rather_than_refusing():
    """THE ONE ABSENT JOB PARAMETER THIS PACKAGE DEFAULTS INSTEAD OF REFUSING, asserted
    because it is the exception to everything above it in this section. A missing window
    is refused: every default is a load nobody chose. A missing FLAG has a default that
    claims LESS rather than something wrong -- neither diagnostic is measured and the
    result says `None`, which nothing can read as a zero. Absence has to keep working
    besides: `test_an_entry_point_handed_a_table_of_the_wrong_kind_refuses_before_spark`
    drives `main` with four arguments, as does any operator's older launch command."""
    name = _load(_DIAGNOSTICS_SCRIPT).DIAGNOSTICS_PARAMETER

    assert optional_flag(None, parameter=name) is False
    assert optional_flag("", parameter=name) is False
    assert optional_flag("false", parameter=name) is False
    assert optional_flag("true", parameter=name) is True
    assert optional_flag(" TRUE ", parameter=name) is True


def test_a_diagnostics_flag_the_parser_cannot_read_is_refused_and_not_read_as_off():
    """`report_diagnostics=yes` is an operator ASKING for the measurement.

    Defaulted, their run comes back with `None` in both fields -- byte-identical to the
    run they were trying not to launch -- and there is nothing in the log to say the
    parameter was ignored. The refusal costs a relaunch; the default costs the
    measurement they came for."""
    with pytest.raises(ValueError, match="report_diagnostics='yes'"):
        optional_flag("yes", parameter="report_diagnostics")


@pytest.mark.parametrize(
    "script,table",
    [
        ("vault_load_hub", "sat_empresa_dados"),
        ("vault_load_satellite", "hub_empresa"),
        ("vault_load_link", "ref_cnae"),
        ("vault_load_effectivity", "link_company_partner"),
        ("vault_load_reference", "sat_eff_company_partner"),
    ],
)
def test_an_entry_point_handed_a_table_of_the_wrong_kind_refuses_before_spark(script, table):
    """The refusal that makes the YAML lock above more than a style rule.

    `domains.table_spec` refuses a name no domain registers; what it cannot refuse is a
    REGISTERED table of the wrong kind, which is what a copied task produces. Without
    this the mistake arrives as an `AttributeError` inside Spark's analysis, naming a
    dataclass field rather than a table."""
    task = _load(script)
    with pytest.raises(ValueError, match="was handed vault table"):
        task.main([table, "empresas", "2026-06+2026-07", _A_GOOD_LOAD_DATE])


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


def test_the_loader_lock_catches_the_partner_link_pointed_at_the_generic_link_loader(
    tmp_path,
):
    """The defect in its exact shape: `vault_load_link.py` for `link_company_partner`.

    It is one path segment, and `load_link` does refuse it at run time -- so the run
    fails rather than lying. This lock moves that from a failed workspace run to a red
    test, which matters because the run it would fail is the one that has already loaded
    27.99M link rows' worth of nothing and burned a serverless start."""
    root = _mutated(
        "vault_partner_job.yml",
        tmp_path,
        "python_file: ../src/vault_load_partner_link.py",
        "python_file: ../src/vault_load_link.py",
    )
    with pytest.raises(AssertionError, match="which needs vault_load_partner_link"):
        _assert_every_task_runs_the_entry_point_its_kind_needs(
            "vault_partner_job.yml", root=root
        )


def test_the_source_lock_catches_a_satellite_left_reading_the_table_it_was_copied_from(
    tmp_path,
):
    """`sat_empresa_dados` from `estabelecimentos`: both names exist, both are spelled
    correctly, and the pairing is nonsense. Spark would find it -- after a session, on
    the payload columns the contract does not have."""
    root = _mutated(
        "vault_empresa_job.yml",
        tmp_path,
        '              - "sat_empresa_dados"\n              - "empresas"',
        '              - "sat_empresa_dados"\n              - "estabelecimentos"',
    )
    with pytest.raises(AssertionError, match="whose contract has no"):
        _assert_every_source_carries_what_its_loader_demands(
            "vault_empresa_job.yml", root=root
        )


def test_the_months_default_lock_catches_a_real_window_nobody_would_have_chosen(tmp_path):
    """Proves the default lock can fail, in the shape that is silent HERE and was not in
    bronze. A `months: "2026-07"` default makes every un-parameterised run of the partner
    job close zero windows and report success -- the whole failure this file's last
    section exists for, reached through a YAML default rather than through a launch."""
    root = _mutated(
        "vault_partner_job.yml", tmp_path, f'default: "{SENTINEL_MONTH}"', 'default: "2026-07"'
    )
    with pytest.raises(AssertionError, match="close ZERO windows"):
        _assert_the_months_default_cannot_pass("vault_partner_job.yml", root=root)
