"""What the REPOSITORY OUTSIDE THE WHEEL says the blast radius is: job YAMLs and job
entry points. The other half of `test_blast_radius.py`. No Spark in here.

SPLIT AT A SEAM RATHER THAN AT A LINE, and the seam is the one this repository has now
drawn four times: `test_task_wiring.py` reads the SCRIPTS and `test_job_yaml_wiring.py`
reads the JOB that hands them arguments; `test_gold_entry_points.py` and
`test_gold_job_wiring.py` are that pair one layer along; `test_vault_entry_points.py` and
`test_vault_job_wiring.py` are it again. What decides which side a test lands on is what
makes it CHANGE. Everything here changes when `databricks/` changes -- a new vault loader
task, a task repointed at another source, a gold entry point that starts or stops reading a
bronze table. Everything in `test_blast_radius.py` changes when
`opl.triage_agent.blast_radius` changes.

IT WAS ONE FILE FOR THE LENGTH OF ONE TEST RUN, AND THAT IS RECORDED RATHER THAN TIDIED
AWAY. Written as a single file it measured 807 lines against this project's 800-line cap and
`tests/test_size_caps.py` said so before anything was committed -- which is the cap test
doing exactly its job, and is the fourth cap crossing this phase has produced. The split was
made before any behaviour was added on top of it: at the split not one assertion had been
edited, and the two files held the same tests the one file held. Both have grown since.

NOTHING ENFORCES THE NO-SPARK PROPERTY, said here rather than left to be assumed, for
`test_incidents_declaration.py`'s reason: every cheap spelling of that guard passes while an
autouse fixture or a transitive import still starts a JVM. Importing pyspark is not the same
thing and does happen -- `opl.vault.domains` imports it at module scope. What costs this
suite 25-33 s is a SparkSession, and nothing here asks for one.

TWO OF THE THREE TRAPS ARE RUN HERE, because both are properties of how the bundle is READ:
the `task_key` that disagrees with the parameter it carries, and the vault table with two
bronze parents. The third -- two bronze tables reaching gold with no vault table in between
-- has its premise established here (no loader task names either of them) and its defence
fired in the other file, where the import guard lives."""
from __future__ import annotations

import ast
import importlib
import shutil
from pathlib import Path

import pytest
import yaml
from job_yaml import PYTHON_FILE_PREFIX, RESOURCES, SRC

from opl.bronze.registry import REGISTRY
from opl.gold.registry import REGISTRY as GOLD_REGISTRY
from opl.triage_agent import blast_radius as blast_radius_module
from opl.triage_agent.blast_radius import (
    DIRECT_TO_GOLD,
    VAULT_LOADS_FROM,
    blast_radius,
)
from opl.vault import domains

# ----------------------------------------------------------------------------------
# Leg 1: the bundle reads. Keyed on the PARAMETER, swept over every YAML.
# ----------------------------------------------------------------------------------


def _loader_tasks_of_bundle(root: Path = RESOURCES) -> list[tuple[str, str, str]]:
    """Every vault loader task in the bundle: (task_key, vault table, bronze source).

    A TASK IS A VAULT LOADER IFF ITS FIRST PARAMETER NAMES A REGISTERED VAULT TABLE, which
    is a fact rather than a convention. The alternatives were both refused: a list of
    `vault_*.yml` filenames (what `tests/test_vault_job_wiring.py` uses -- the test below
    says which shape escapes that list and which does not), and a match on the
    `vault_load_*.py` entry point (a naming convention, which `incidents.py`'s header calls
    a second spelling nobody wrote down). The parameter is what the running task is handed,
    and `_gold_entry_points_of_bundle` runs this same rule on leg 3.

    IT CANNOT COLLIDE WITH A GOLD TASK'S FIRST PARAMETER, and that is not luck:
    `opl.gold.registry_guards._assert_no_gold_name_is_owned_by_another_layer` refuses a gold
    table whose name the vault holds, at gold's own import."""
    found: list[tuple[str, str, str]] = []
    for path in sorted(root.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job in (document.get("resources", {}).get("jobs", {}) or {}).values():
            for task in job.get("tasks", []) or []:
                parameters = (task.get("spark_python_task") or {}).get("parameters") or []
                if len(parameters) < 2 or parameters[0] not in domains.REGISTRY:
                    continue
                assert parameters[1] in REGISTRY, (
                    f"{path.name}:{task['task_key']} loads {parameters[0]!r} from "
                    f"{parameters[1]!r}, which no bronze table is registered under"
                )
                found.append((task["task_key"], parameters[0], parameters[1]))
    return found


def _vault_loads_of_bundle(root: Path = RESOURCES) -> dict[str, tuple[str, ...]]:
    """The bundle's own bronze-to-vault edges, in `VAULT_LOADS_FROM`'s shape.

    THE SAME BRONZE SOURCE TWICE FOR ONE VAULT TABLE IS REFUSED HERE rather than deduped
    into invisibility: two tasks loading one table from one source is a real defect (both
    loaders are insert-only anti-joins, so the second appends nothing and the run reports
    two loads) and collapsing it would make this reader agree with the declaration for the
    wrong reason."""
    found: dict[str, list[str]] = {}
    for key, table, source in _loader_tasks_of_bundle(root):
        assert source not in found.get(table, []), (
            f"{key} is the second task loading {table!r} from {source!r}"
        )
        found.setdefault(table, []).append(source)
    return {table: tuple(sorted(sources)) for table, sources in found.items()}


def _vault_loads_keyed_on_task_key(root: Path = RESOURCES) -> dict[str, tuple[str, ...]]:
    """THE NAIVE READER, kept in the repository so its wrong answer is a fact and not a
    warning. Identical to the one above except that it takes the vault table from the
    `task_key` instead of from the parameter the task is handed."""
    found: dict[str, list[str]] = {}
    for key, _table, source in _loader_tasks_of_bundle(root):
        found.setdefault(key, []).append(source)
    return {table: tuple(sorted(sources)) for table, sources in found.items()}


def test_the_vault_load_declaration_is_what_the_bundle_hands_its_loader_tasks():
    """THE LOCK LEG 1 RESTS ON, and it is equality in both directions.

    `VAULT_LOADS_FROM` is data in the wheel because `databricks/resources/` is not IN the
    wheel -- `packages = ["src/opl"]` -- so a module that read the YAMLs at run time would
    work here and raise in the workspace. What makes data safe rather than merely honest is
    that it cannot go stale silently: a new loader task, a table repointed at another
    source, or a vault job removed all fail here in the commit that does it."""
    assert _vault_loads_of_bundle() == VAULT_LOADS_FROM


def test_the_lock_catches_a_loader_left_reading_the_table_it_was_copied_from(tmp_path):
    """Proves the lock above can FAIL. A test that reads a file passes just as happily on a
    typo in its own extraction as on correct wiring.

    The mutation is the paste `tests/test_vault_job_wiring.py`'s header names first: a vault
    task left reading the source of the job it was copied from. Both values are the names of
    tables that EXIST, so nothing about the YAML looks wrong.

    THE POSITIVE ARM IS THE DISCRIMINATING ONE. `mutated != VAULT_LOADS_FROM` alone is
    satisfied by a reader that found nothing at all, which is how the sibling lock in
    `test_incidents_declaration.py` once passed on `{}`; the exact tuple below is what says
    the reader read the mutated file and read it correctly."""
    root = tmp_path / "resources"
    shutil.copytree(RESOURCES, root)
    target = root / "vault_empresa_job.yml"
    original = target.read_text(encoding="utf-8")
    drifted = original.replace('- "empresas"', '- "socios"', 1)
    assert drifted != original, "the mutation did not apply -- this test proves nothing"
    target.write_text(drifted, encoding="utf-8")

    mutated = _vault_loads_of_bundle(root)
    assert mutated != VAULT_LOADS_FROM
    assert mutated["hub_empresa"] == ("estabelecimentos", "socios")
    assert mutated["sat_empresa_dados"] == ("empresas",)
    assert {
        table: sources
        for table, sources in mutated.items()
        if table != "hub_empresa"
    } == {
        table: sources
        for table, sources in VAULT_LOADS_FROM.items()
        if table != "hub_empresa"
    }


def test_the_sweep_finds_a_loader_task_in_a_job_file_no_name_pattern_would_match(tmp_path):
    """The reader is keyed on the parameter, so a job file's NAME decides nothing.

    This is the property that separates it from `tests/test_vault_job_wiring.py`, whose
    `_VAULT_JOBS` list is closed by a `vault_*.yml` glob -- and what escapes that glob is
    NARROWER than the glob makes it sound, which was measured rather than reasoned. A
    loader task in a BRAND-NEW YAML is caught: `tests/test_job_yaml_launch_guards.py`
    classifies every file `databricks/resources/*.yml` matches and fails on one it does not
    know. What is invisible is a loader task added to an EXISTING file already classified
    there under a name that is not `vault_*` -- a `vault_load_satellite.py` task injected
    into `smoke_job.yml` leaves the five job-wiring files green and reddens this one. That
    hole is REPORTED AND NOT FIXED, because `tests/test_vault_job_wiring.py` is not this
    task's file to change. Renaming the merchant job here changes not one edge."""
    root = tmp_path / "resources"
    shutil.copytree(RESOURCES, root)
    (root / "vault_merchant_job.yml").rename(root / "zz_a_job_named_nothing_like_it.yml")

    assert not list(root.glob("vault_merchant*.yml"))
    assert _vault_loads_of_bundle(root) == VAULT_LOADS_FROM
    assert _vault_loads_of_bundle(root)["sat_merchant_dados"] == ("merchant",)


def test_reading_the_task_key_instead_of_the_parameter_gets_exactly_one_edge_wrong():
    """TRAP 1, RUN. The authority is the parameter, never the name of the thing carrying it.

    ONE task out of nineteen disagrees, which is the whole hazard: eighteen of the naive
    reader's entries are right, so its output is a manifest that looks complete. What it
    does is invent `hub_empresa_from_estabelecimentos` -- a vault table nothing registers --
    and, in the same move, delete the estabelecimentos feed from the real `hub_empresa`.

    THE COUNT OF DISAGREEING TASKS IS ASSERTED SEPARATELY from the mapping difference,
    because the mapping difference alone would be satisfied by twelve disagreements as
    happily as by one, and "exactly one" is the sentence the module header makes."""
    disagreeing = [
        key for key, table, _source in _loader_tasks_of_bundle() if key != table
    ]
    assert disagreeing == ["hub_empresa_from_estabelecimentos"]

    naive = _vault_loads_keyed_on_task_key()
    assert naive != VAULT_LOADS_FROM
    assert set(naive) - set(VAULT_LOADS_FROM) == {"hub_empresa_from_estabelecimentos"}
    assert set(VAULT_LOADS_FROM) - set(naive) == set()
    assert naive["hub_empresa"] == ("empresas",)
    assert VAULT_LOADS_FROM["hub_empresa"] == ("empresas", "estabelecimentos")


def test_the_naive_reader_costs_estabelecimentos_the_two_gold_tables_it_reaches():
    """And what that one wrong edge does to the ANSWER, which is where it stops being a
    curiosity about YAML.

    Driven through the module's own injectable seam rather than by patching a global, so
    what is being shown is the declaration's effect on the published radius. `dim_company`
    reads `hub_empresa`, and `fact_payment` reads `dim_company`, so an incident on
    estabelecimentos loses BOTH -- and keeps `pit_estabelecimento`, which is why the
    remaining answer is still a plausible list of tables."""
    naive = _vault_loads_keyed_on_task_key()
    lost = blast_radius_module._radius(
        "estabelecimentos", loads_from=naive, direct=DIRECT_TO_GOLD
    )
    assert blast_radius("estabelecimentos").gold == (
        "dim_company", "fact_payment", "pit_estabelecimento",
    )
    assert lost.gold == ("pit_estabelecimento",)


# ----------------------------------------------------------------------------------
# Trap 2: the vault table with two bronze parents, as the bundle declares it.
# ----------------------------------------------------------------------------------


def test_the_shared_hub_is_declared_with_both_parents_and_both_appear_in_the_bundle():
    """`hub_empresa` is the vault's one deliberate double load, from both directions.

    The bundle side is read rather than retyped; the declaration side is the tuple. Both
    tasks are named so the assertion cannot be satisfied by one file declaring it twice."""
    loaders = {
        (key, source)
        for key, table, source in _loader_tasks_of_bundle()
        if table == "hub_empresa"
    }
    assert loaders == {
        ("hub_empresa", "empresas"),
        ("hub_empresa_from_estabelecimentos", "estabelecimentos"),
    }
    assert VAULT_LOADS_FROM["hub_empresa"] == ("empresas", "estabelecimentos")


# ----------------------------------------------------------------------------------
# Trap 3: the premise -- no vault loader task names either of the two.
# ----------------------------------------------------------------------------------


def test_no_vault_loader_task_in_the_bundle_names_payments_or_ptax():
    """The premise of leg 3, read off the bundle rather than asserted about it.

    This is what makes the empty vault leg below a FACT about the pipeline instead of an
    omission in the declaration -- and it is the arithmetic that says a bronze -> vault ->
    gold walk really does return nothing for these two."""
    sourced = {source for _key, _table, source in _loader_tasks_of_bundle()}
    assert sourced == set(REGISTRY) - {"payments", "ptax"}
    assert blast_radius("payments").vault == ()
    assert blast_radius("ptax").vault == ()


# ----------------------------------------------------------------------------------
# Leg 3's key set: the gold entry points the BUNDLE names, read with `ast`.
# ----------------------------------------------------------------------------------


def _contract_aliases(tree: ast.Module) -> dict[str, str]:
    """Local name -> `opl.contracts` submodule, for every `from opl.contracts import ...`."""
    return {
        (alias.asname or alias.name): alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "opl.contracts"
        for alias in node.names
    }


def _bronze_spec_alias(tree: ast.Module) -> str | None:
    """The local name bound to `opl.bronze.registry.table_spec`, or None if unimported."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "opl.bronze.registry":
            for alias in node.names:
                if alias.name == "table_spec":
                    return alias.asname or alias.name
    return None


def _gold_entry_points_of_bundle(
    root: Path = RESOURCES, src: Path = SRC
) -> dict[str, tuple[str, ...]]:
    """Every `databricks/src` script the bundle hands a GOLD table name to, and which.

    A TASK IS A GOLD LOADER IFF ITS FIRST PARAMETER NAMES A REGISTERED GOLD TABLE, which is
    `_loader_tasks_of_bundle`'s rule one layer along -- and taking it here is the whole of
    this correction. Keyed on a `gold_load_*.py` glob instead, this file would have REFUSED
    a filename convention on leg 1 and RELIED on one for leg 3, and an entry point named
    anything else would have been swept by nothing.

    IT CANNOT COLLIDE WITH A VAULT TASK'S FIRST PARAMETER, for the reason
    `_loader_tasks_of_bundle` gives from the other side:
    `opl.gold.registry_guards._assert_no_gold_name_is_owned_by_another_layer` refuses a gold
    table whose name the VAULT holds. It CAN collide with a bronze task's -- that guard
    holds bronze's Delta names (`bronze_payments`), not its registry keys (`payments`), and
    all seven keys are accepted as gold names. None is one today, and nothing guards that.

    THE THREE CONFORMED TASKS SHARE ONE SCRIPT, so the value is a tuple: the mapping is
    script -> tables built by it, and a script is opened once however many tasks run it.

    ITS FAILURE PROFILE, ALL THREE ARMS MEASURED against
    `test_the_two_gold_loaders_that_read_no_bronze_table_are_seen_and_are_empty`. A
    non-loader task handed a gold table name is LOUD: its script joins that test's per-file
    dict. A missed BUILD task is loud there too -- transpose `dim_company`'s first two
    parameters and its totality over `GOLD_REGISTRY` fails, `missing: {'dim_company'}`. A
    script reading bronze whose task is handed no gold table name is SILENT, and nothing
    narrows it: that totality is over gold TABLES, and such a script builds none."""
    found: dict[str, list[str]] = {}
    for path in sorted(root.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job in (document.get("resources", {}).get("jobs", {}) or {}).values():
            for task in job.get("tasks", []) or []:
                task_run = task.get("spark_python_task") or {}
                parameters = task_run.get("parameters") or []
                if not parameters or parameters[0] not in GOLD_REGISTRY:
                    continue
                python_file = task_run["python_file"]
                assert python_file.startswith(PYTHON_FILE_PREFIX), (
                    f"{path.name}:{task['task_key']} builds {parameters[0]!r} by running "
                    f"{python_file!r}, which is not under {PYTHON_FILE_PREFIX}"
                )
                name = python_file[len(PYTHON_FILE_PREFIX):]
                assert (src / name).exists(), (
                    f"{path.name}:{task['task_key']} runs {python_file!r}, which is not a "
                    f"file under {src}"
                )
                found.setdefault(name, []).append(parameters[0])
    return {name: tuple(sorted(tables)) for name, tables in found.items()}


def _bronze_tables_the_gold_entry_points_read(
    root: Path = RESOURCES, src: Path = SRC
) -> dict[str, set[str]]:
    """Per gold entry point, the bronze registry keys it resolves a table name from.

    READ BY `ast` AND NOT BY GREP, and the key itself is never matched as text: what is
    found is a call to whatever local name `opl.bronze.registry.table_spec` was imported
    under, whose argument is `<contract>.CONTRACT`, and the contract module is then IMPORTED
    and asked. So a contract renaming its key moves this reader with it.

    WHAT THIS SWEEP CANNOT SEE, stated as what it CAN, because the accept set is one shape
    and the misses are not enumerable. An indirection applied to a read that IS declared is
    CAUGHT: the sweep then returns a smaller set than `DIRECT_TO_GOLD` and the equality
    below fails. The blind case is a NEW, UNDECLARED read, and this reader recognises
    EXACTLY ONE spelling -- a call written as a bare name, that name being the one
    `from opl.bronze.registry import table_spec` bound in this module, whose FIRST
    POSITIONAL argument is `<name>.CONTRACT` with `<name>` bound by `from opl.contracts
    import <module>`. ANYTHING ELSE CONTRIBUTES NOTHING. Three were constructed and
    watched, each letting a fresh undeclared read of bronze `merchant` past with the answer
    unchanged: `reg.table_spec(...)` after `import opl.bronze.registry as reg`;
    `from opl.contracts.merchant import CONTRACT`; and `table_spec(args[0] if args else
    "")`, which is `databricks/src`'s own live idiom.

    A CALL WHOSE ARGUMENT IS A LOCAL NAME -- a helper taking the contract as a parameter --
    is the one miss that is LOUD: the assertion below names the file and the name."""
    found: dict[str, set[str]] = {}
    for name in sorted(_gold_entry_points_of_bundle(root, src)):
        tree = ast.parse((src / name).read_text(encoding="utf-8"))
        aliases, reader = _contract_aliases(tree), _bronze_spec_alias(tree)
        keys: set[str] = set()
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
                continue
            argument = node.args[0] if node.args else None
            if node.func.id != reader or not isinstance(argument, ast.Attribute):
                continue
            if argument.attr != "CONTRACT" or not isinstance(argument.value, ast.Name):
                continue
            alias = aliases.get(argument.value.id)
            assert alias is not None, (
                f"{name} resolves a bronze table from {ast.unparse(argument)}, whose "
                f"{argument.value.id!r} this reader cannot trace to an `opl.contracts` "
                "submodule -- it follows `from opl.contracts import <module>` and nothing "
                "else, so the bronze table this call reads is not in the answer"
            )
            module = importlib.import_module(f"opl.contracts.{alias}")
            keys.add(module.CONTRACT)
        found[name] = keys
    return found


def test_the_direct_to_gold_sources_are_the_bronze_tables_the_gold_loaders_read():
    """LEG 3'S KEY SET IS ATTESTED BY THE ENTRY POINTS, which the brief expected of nothing.

    The gold job YAMLs carry no source parameter, so the bundle cannot hold this leg -- but
    `databricks/src/gold_load_*.py` names its bronze reads in code, and that code is in the
    repository even though it is not in the wheel. So the KEYS of `DIRECT_TO_GOLD` are held
    equal, both directions, to the bronze tables those entry points resolve.

    WHAT THIS DOES NOT ATTEST, AND IT IS THE DECLARATION'S REMAINING HUMAN PART: which GOLD
    table each read belongs to. `gold_load_conformed_dimension.py` is handed the same
    `fact_table` for `dim_date`, `dim_channel` and `dim_currency`, and only the calendar
    uses it for anything it writes. Nothing here can see that difference."""
    per_file = _bronze_tables_the_gold_entry_points_read()
    assert set().union(*per_file.values()) == set(DIRECT_TO_GOLD)


def test_the_two_gold_loaders_that_read_no_bronze_table_are_seen_and_are_empty():
    """The discriminating arm of the sweep above, which is otherwise a set union of four
    files and would report the same answer if two of them were never opened.

    `gold_load_dimension.py` reads its satellite and that satellite's hub;
    `gold_load_pit.py` reads a hub and two satellites. Neither touches bronze, and the sweep
    has to SEE them and find nothing rather than skip them and find nothing.

    THE ENUMERATION IS ASSERTED TOTAL OVER THE GOLD REGISTRY in the same test, because "the
    sweep saw four files" and "the sweep saw every gold TABLE the bundle builds" are
    different sentences and only the second one closes leg 3. Total over TABLES, not over
    scripts: the two coincide only because each gold table has one build task today."""
    per_file = _bronze_tables_the_gold_entry_points_read()
    assert per_file == {
        "gold_load_conformed_dimension.py": {"payments"},
        "gold_load_dimension.py": set(),
        "gold_load_fact.py": {"payments", "ptax"},
        "gold_load_pit.py": set(),
    }
    built = _gold_entry_points_of_bundle()
    assert set(per_file) == set(built)
    assert {table for tables in built.values() for table in tables} == set(GOLD_REGISTRY)


def test_the_gold_sweep_finds_an_entry_point_no_name_pattern_would_match(tmp_path):
    """The gold reader is keyed on the task's first PARAMETER, so a SCRIPT's name decides
    nothing -- the same property `test_the_sweep_finds_a_loader_task_in_a_job_file_no_name
    _pattern_would_match` asserts for leg 1, and the reason leg 3 no longer globs
    `gold_load_*.py`.

    `gold_load_fact.py` is renamed and the fact job repointed at the new name. Under a glob
    that file simply stops being opened, and `ptax` -- whose only read among the GOLD ENTRY
    POINTS is in it -- leaves the sweep's answer with nothing raising, because the
    remaining files still resolve `payments` and a set union of three files is a perfectly
    plausible one. The positive arm is the discriminating one: the renamed file has to
    appear with BOTH its bronze reads."""
    resources, src = tmp_path / "resources", tmp_path / "src"
    shutil.copytree(RESOURCES, resources)
    shutil.copytree(SRC, src)
    (src / "gold_load_fact.py").rename(src / "zz_a_script_named_nothing_like_it.py")
    job = resources / "gold_fact_payment_job.yml"
    original = job.read_text(encoding="utf-8")
    repointed = original.replace(
        "../src/gold_load_fact.py", "../src/zz_a_script_named_nothing_like_it.py", 1
    )
    assert repointed != original, "the mutation did not apply -- this test proves nothing"
    job.write_text(repointed, encoding="utf-8")

    assert not list(src.glob("gold_load_fact*.py"))
    per_file = _bronze_tables_the_gold_entry_points_read(resources, src)
    assert per_file["zz_a_script_named_nothing_like_it.py"] == {"payments", "ptax"}
    assert set().union(*per_file.values()) == set(DIRECT_TO_GOLD)


def test_the_gold_sweep_names_the_spelling_it_cannot_trace_rather_than_raising_a_key_error(
    tmp_path,
):
    """The reader follows `from opl.contracts import <module>` and nothing else, so a call
    handed a LOCAL name -- a helper taking the contract as a parameter -- cannot be resolved
    to a contract at all. That line raised a bare `KeyError` on the alias dict before this
    pass: loud, and undiagnostic. It now names the file and the name.

    THE ASSERTION IS FIRED HERE RATHER THAN TRUSTED, because an unwatched refusal is the
    defect this correction pass was opened for. And it is NOT one of the sweep's blind
    spellings: a spelling the reader does not recognise is silent, which is what the
    reader's own docstring is about. This is the one miss that is loud."""
    resources, src = tmp_path / "resources", tmp_path / "src"
    shutil.copytree(RESOURCES, resources)
    shutil.copytree(SRC, src)
    entry = src / "gold_load_fact.py"
    entry.write_text(
        entry.read_text(encoding="utf-8")
        + "\n\ndef _resolve(handed_the_contract):\n"
        "    return bronze_table_spec(handed_the_contract.CONTRACT)\n",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="'handed_the_contract'"):
        _bronze_tables_the_gold_entry_points_read(resources, src)
