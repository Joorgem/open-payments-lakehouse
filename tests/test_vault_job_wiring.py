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
kind. The one thing that does cross is the revision guard, and it stays there: all five
vault jobs are in its `_GUARDED_JOBS` so the guard's position, its parameter and its
environment are asserted by the file that owns that argument, not re-asserted here.

WHAT A COPIED VAULT JOB GETS WRONG, which is what every lock below is shaped around.
These five YAMLs were written from one template and each task carries a vault table name
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
produce the table's defining output, and it reports success. What is pinned HERE is that
no job YAML can hand a task one silently -- `test_the_months_default_refuses_rather_than
_naming_a_window_nobody_chose`. That the ENTRY POINT refuses it when handed one anyway is
`tests/test_vault_entry_points.py`, and the zero it exists to prevent is measured against
real Spark in `tests/vault/test_effectivity_window.py`.

AND THIS FILE IS NOW A PAIR, SPLIT BY F-DB TASK 1 AT EXACTLY 800 LINES. F-DB's
`vault_merchant_job.yml` HAS SINCE BEEN ADDED to `_VAULT_JOBS`, which is what that split
made room for -- and the totality lock now covers eighteen registered tables across TWO
domains rather than fourteen across one. The seam is the one this repository has drawn
twice already and named both times:
`test_task_wiring.py` reads the SCRIPTS and `test_job_yaml_wiring.py` reads the JOB that
hands them arguments; `test_gold_entry_points.py` and `test_gold_job_wiring.py` are that
pair one layer along. The vault had both halves in one file. What went to
`tests/test_vault_entry_points.py` is everything that consults no YAML at all: the two
sweeps over the six entry points, and the refusals reached by driving a script's own
`main()`. What stayed is everything that parametrizes over `_VAULT_JOBS`. Which side a
lock lands on is decided by what makes it CHANGE -- a new job changes this file, a new
entry point changes that one -- and F-DB adds a job and no entry point, which is why the
other half moved out rather than this one. That prediction held: Task 5 added
`vault_merchant_job.yml` and `tests/test_vault_entry_points.py` needed no edit, because
all four loaders its tasks run already existed.

Nothing here starts Spark: every assertion is about wiring. The two locks below that
consult a script load it by path with the importlib pattern the other task tests use --
`databricks/src` scripts are job entry points, not part of the opl wheel."""
from __future__ import annotations

import importlib.util
import pkgutil
from pathlib import Path

import pytest
import yaml

from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import SENTINEL_MONTH, is_month
from opl.contracts.catalogue import columns_for
from opl.vault import domains
from opl.vault.job_params import optional_flag
from opl.vault.links import undeclared_derived_ends
from opl.vault.observation import ObservationGrain
from opl.vault.registry import (
    EffectivitySatellite,
    Hub,
    Link,
    ReferenceTable,
    Satellite,
    VaultTable,
)

# `columns_for` COMES FROM `opl.contracts.catalogue` AND NOT FROM `cnpj_schemas`, SINCE
# F-DB. The pairing lock below reads the contract of whatever bronze table a task names,
# and three of those contracts are not the Receita's file layouts at all --
# `cnpj_schemas.columns_for` raises KeyError for `merchant`. The catalogue is the join
# over every source, which is the question this lock is actually asking.

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"
_RESOURCES = _REPO / "databricks" / "resources"

# THE FIVE VAULT JOBS, ENUMERATED. A glob would silently give a job added later
# whichever behaviour it happened to inherit, and the totality lock below -- every
# registered vault table is loaded by exactly one task -- is only a claim about the
# vault if this list is the whole of it. `test_the_five_vault_jobs_are_the_vault_yamls
# _on_disk` closes the other direction.
_VAULT_JOBS = (
    "vault_empresa_job.yml",
    "vault_estabelecimento_job.yml",
    "vault_partner_job.yml",
    "vault_reference_job.yml",
    # F-DB Task 5, and the first vault job over a source that is not the Receita's file
    # feed. Every lock below is total over it unchanged; what it needs from this list is
    # the same thing the other four do.
    "vault_merchant_job.yml",
)

# THE JOBS WHOSE `report_diagnostics` DEFAULT IS ON, WITH THE REASON -- a declared
# exception rather than a hole in the lock below, on `_UNGUARDED_JOBS`' shape in
# `test_job_yaml_launch_guards.py`. The default is off everywhere else because the two
# diagnostics were most of a 5,635 s load over 69M keys and both answered 0.
_DIAGNOSTICS_DEFAULT_ON = {
    "vault_merchant_job.yml": (
        "1,088 rows, where the argument for off was measured on 69M -- and off, the "
        "departure figure is None, which is this phase's headline at hub grain"
    ),
}

_GUARD = "assert_deployed_revision"
_PYTHON_FILE_PREFIX = "../src/"
_MONTHS_PARAMETER = "{{job.parameters.months}}"
_LOAD_DATE_REFERENCE = "{{job.start_time.iso_datetime}}"

# THE ONE ENTRY POINT THAT TAKES A FIFTH PARAMETER. Every vault load task is handed
# [table, source, months, load_date]; the descriptive satellite takes `report_diagnostics`
# after them, because it is the only loader here with optional work to skip. The arity is
# derived from the SCRIPT below rather than allowed to be "4 or 5", so a fifth parameter
# handed to a loader that ignores it -- the shape a copy of a satellite task produces --
# is refused here instead of being passed to nothing. Spelled in
# `tests/test_vault_entry_points.py` too, which asks a different thing of it: the
# parameter NAME the loader declares, rather than the arity of the task handed to it.
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
    and since F-DB it is the same FUNCTION rather than a restatement of it:
    `undeclared_derived_ends` is exported from that module for exactly this caller. That
    refusal is the reason there are two link loaders at all -- `load_link` computes every
    end's reference from the columns that hub is NAMED after, so an undeclared derived
    end would hash both ends of `link_company_partner` from `cnpj_basico` and every
    relationship would read as a company partnered with itself, with the right row count
    and working joins.

    THE CONDITION MOVED AND THE ROUTING DID NOT, WHICH IS WHAT THIS RE-DERIVATION IS FOR.
    It read `any(not end.identifying ...)`, and that flag was a PROXY for "derived".
    `link_merchant_empresa`'s empresa end is derived AND identifying, so under the old
    spelling this lock would have routed it to `vault_load_partner_link.py` -- a loader
    that would refuse it -- while under the flag's stated MEANING it belongs on
    `vault_load_link.py`, which can now write it. Asking the loader's own function means
    the two cannot disagree about which entry point a link needs."""
    return bool(link.dependent_child_keys) or bool(undeclared_derived_ends(link))


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
        # `link_candidates` asks the END where its hub's key lives -- the hub's own
        # column names, or the columns a `LinkEnd.key_from` declares. Restated as
        # `hub.business_key_columns` this lock would demand `cnpj_basico` from
        # `bronze_merchant`, which has no such column, and refuse a pairing that works.
        return tuple(
            name
            for end, hub in zip(spec.ends, hubs, strict=True)
            for name in end.source_columns(hub)
        )
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


def test_the_five_vault_jobs_are_the_vault_yamls_on_disk():
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
        _assert_the_diagnostics_default_is_off_unless_declared_otherwise(
            job_yml, name, declared[name]
        )


def _assert_the_diagnostics_default_is_off_unless_declared_otherwise(
    job_yml: str, name: str, default: str
) -> None:
    """Off, unless the job is in `_DIAGNOSTICS_DEFAULT_ON` with a reason beside it.

    AN EXCEPTION WITH A NAME RATHER THAN A WEAKER LOCK, on `_UNGUARDED_JOBS`' shape. The
    default is off because two extra full passes over 69M keys were most of a 5,635 s
    load; that is a COST argument, and it has an answer per source rather than in
    general. On 1,088 rows the cost is nothing and the departure figure is the phase's
    headline -- and off, `load_satellite` reports it as `None`, which it deliberately
    keeps distinguishable from a measured 0. A headline that depends on an override at
    launch is a headline somebody forgets to ask for.

    Both directions are asserted, so an entry that outlives its job's default fails here
    rather than silently exempting a job that no longer needs exempting."""
    parsed = optional_flag(default, parameter=name)
    if job_yml in _DIAGNOSTICS_DEFAULT_ON:
        assert parsed is True, (
            f"{job_yml} is listed in _DIAGNOSTICS_DEFAULT_ON because {name} costs it "
            f"nothing ({_DIAGNOSTICS_DEFAULT_ON[job_yml]}), and its default is "
            f"{default!r}, which parses OFF. Take the entry out or turn the default on"
        )
        return
    assert parsed is False, (
        f"{job_yml}'s {name} default is {default!r}, which parses ON. Every run "
        "launched without --params would pay for a second full scan of the source and "
        "a materialised observation ledger it never asked for. If that cost is nothing "
        "for this source, say so in _DIAGNOSTICS_DEFAULT_ON rather than here"
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


# --- the one thing the job layer re-derives ------------------------------------------
#
# The two locks below are the only ones here that consult a SCRIPT, and both do it in
# service of a YAML claim: which grain the task a YAML declares will build, and which
# parameter name the loader a YAML hands a fifth argument to declares. Everything that
# asks a script a question of its own -- what it must not spell, and what it refuses
# when driven -- is `tests/test_vault_entry_points.py`.


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _grains_the_domains_declare() -> dict[str, ObservationGrain]:
    """Every `ObservationGrain` bound at module level by any domain module, by name.

    The package is scanned the way `opl.vault.registry.discover_domains` scans it --
    underscore-prefixed modules skipped -- so this is total over the domains that exist
    rather than over the ones this file happened to import."""
    found: dict[str, ObservationGrain] = {}
    for info in pkgutil.iter_modules(list(domains.__path__)):
        if info.name.startswith("_"):
            continue
        module = importlib.import_module(f"{domains.__name__}.{info.name}")
        for value in vars(module).values():
            if isinstance(value, ObservationGrain):
                found[value.name] = value
    return found


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
    `COMPANY_PARTNER_GRAIN`, and `merchant_domain.py` declares two more -- an observation
    grain is not a registered table, so there is no way for a job to reach one by name
    without a mapping that would itself be the second spelling. So the two satellite entry
    points call the SAME constructor over the SAME two registry entries, and this compares
    the result against what the domain declares, per (satellite task, bronze source)
    pairing the YAMLs actually carry.

    IT SWEEPS THE DOMAINS PACKAGE RATHER THAN ONE MODULE, SINCE F-DB. It read
    `vars(cnpj_domain)` while there was one domain, which would have reported the merchant
    grains as "declared by no constant" -- a lock that goes red for the right reason and
    names the wrong cause. The sweep is `discover_domains`' own mechanism, so a wave-2
    domain is covered on the day its file lands rather than on the day somebody adds an
    import here.

    The grain is the argument whose mistakes are invisible in the output: it decides the
    reported departure count for a descriptive satellite and WHICH WINDOWS CLOSE for the
    effectivity one. The loaders' own `_refuse_a_mismatched_grain` covers the runtime
    half; this covers the half that would otherwise be a paragraph of prose."""
    declared = _grains_the_domains_declare()
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
    job close zero windows and report success -- the whole failure the window section of
    `tests/test_vault_entry_points.py` exists for, reached through a YAML default rather
    than through a launch."""
    root = _mutated(
        "vault_partner_job.yml", tmp_path, f'default: "{SENTINEL_MONTH}"', 'default: "2026-07"'
    )
    with pytest.raises(AssertionError, match="close ZERO windows"):
        _assert_the_months_default_cannot_pass("vault_partner_job.yml", root=root)
