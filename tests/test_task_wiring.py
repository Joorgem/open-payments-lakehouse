"""Which table each job task touches.

Written as characterization tests BEFORE the registry refactor moved any of it:
they asserted the wiring as it stood so the refactor could only preserve it. The
defect they exist to prevent is real and documented in
bronze_estabelecimentos_job.yml -- a hardcoded quarantine name "sent estab
triagers to a table full of unrelated F1.2 lookup rows".

Job scripts under databricks/src are entry points, not part of the opl wheel, so
they are loaded by path with the same importlib pattern the other task tests use.
Nothing here starts Spark: every assertion is about wiring, not data.

TWO HALVES, AND THE SECOND ONE ARRIVED IN F1.4b. The first half reads the SCRIPTS:
a task must resolve every coordinate from the one spec it got from argv. That is
only half a wiring claim, because a script that resolves its spec perfectly still
ingests whatever table its job YAML hands it -- and the YAMLs are written by
copying the previous table's file, which is the paste the second half exists to
refuse. A job that reads the wrong table's landing dir, or promotes into the wrong
table's bronze, does not error: it SUCCEEDS, having done the wrong thing.

TO WHOEVER HITS THE RED HERE DURING THE REGISTRY REFACTOR: these describe a
structure the refactor deliberately deletes, so some of them fail BY
CONSTRUCTION once a script takes its table from a job parameter instead of a
module constant. That is this net doing its job on schedule, not a broken test.
Rewrite each one against the registry -- feed it a table key, assert the same
resolved coordinates -- rather than deleting it. Which property each one exists
to preserve is written in its own docstring, for exactly this moment.

That already happened twice, on schedule. Task 6 parameterised the two ingest
scripts, and their `EXPECTED_TABLES` entries went red exactly as predicted -- a
script that reads the registry has no module constants left to enumerate. Task 7
collapsed the two gates into one and the two promotes into one, which took the
last four entries with it, and `EXPECTED_TABLES` along with them: there is no job
task left under databricks/src that names a bronze table, so there is nothing
anywhere for a constant-enumerating lock to read.

Nothing was dropped. Each entry was rewritten into the property that replaces it:
every coordinate must be a field of the ONE spec `main()` resolved from argv, so
a table's staging, bronze and quarantine cannot drift apart -- which is the drift
that "sent estab triagers to a table full of unrelated F1.2 lookup rows". The
helpers that resolved module constants (`_load`, `_bound_table`) went with the
constants they read; `_deref` below is `_bound_table`'s surviving half."""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

from opl.bronze.masking import MASKED_COLUMNS
from opl.bronze.registry import REGISTRY, table_spec

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"
_RESOURCES = _REPO / "databricks" / "resources"

# Every job task under databricks/src that resolves a table. Enumerated rather
# than globbed: a new entry point must be a deliberate addition to this list, and
# a glob would silently give a newly-added script a free pass. `smoke.py` is the
# only other script there and it touches no table at all.
_TABLE_TASKS = [
    "bronze_ingest",
    "bronze_lookup_ingest",
    "unzip_table",
    "dq_gate_batch",
    "promote_batch",
    "fail_on_dq",
    "reclaim_landing",
    # F1.4b. The deliberate addition this list's docstring asks for: it is the first
    # entry point that CREATES a bronze table rather than writing to one the append
    # made, so a literal table name in it would hand-build a table under a name the
    # registry does not know -- and the promote would then create the real one,
    # unmasked, on its first append.
    "ensure_masked_table",
]


@pytest.mark.parametrize("script", _TABLE_TASKS)
def test_no_task_names_a_bronze_table_directly_any_more(script):
    """The collapse's whole point. A task that spells a table name is a task
    whose staging/quarantine pair can drift from the one the registry declares --
    which is how a triager was sent to a table full of unrelated rows.

    Comment lines are stripped before the check: a comment that cites the table a
    real incident happened in is this repo's house style, and is not wiring."""
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    code = "\n".join(
        line for line in source.splitlines() if not line.strip().startswith("#")
    )
    assert "bronze_cnpj_" not in code, f"{script}.py names a bronze table directly"
    assert "table_spec(" in code


def _main_of(script: str) -> ast.FunctionDef:
    tree = ast.parse((_SRC / f"{script}.py").read_text(encoding="utf-8"), filename=f"{script}.py")
    mains = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main"]
    assert len(mains) == 1, f"{script}.py does not define exactly one module-level main()"
    return mains[0]


def _sole_call(main: ast.FunctionDef, name: str, script: str) -> ast.Call:
    """The one call to `name(...)` in the task's main, or a failure saying so.

    Modelled on `_gate_quarantine` in test_fail_on_dq_task.py, and for its reason:
    a lock that silently reads the first of several matches, or none at all,
    passes just as happily on wiring it never looked at."""
    calls = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Name) and node.func.id == name)
            or (isinstance(node.func, ast.Attribute) and node.func.attr == name)
        )
    ]
    assert len(calls) == 1, (
        f"{script}.py main() makes {len(calls)} call(s) to {name}(), expected exactly 1 -- "
        "this lock is reading the wrong call, or none, so it would pass on wiring it "
        "never saw"
    )
    return calls[0]


def _locals_of(main: ast.FunctionDef, script: str) -> dict[str, ast.expr]:
    pairs = [
        (target.id, node.value)
        for node in ast.walk(main)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    ]
    names = [name for name, _ in pairs]
    assert len(names) == len(set(names)), (
        f"{script}.py main() binds a local name twice; this lock resolves a name to a "
        "single assignment and would pick an arbitrary one of them"
    )
    return dict(pairs)


def _deref(expr: ast.expr, scope: dict[str, ast.expr], where: str) -> ast.expr:
    """Follow an argument through one or more local hops (`tbl`, `quarantine`).

    `_bound_table`'s surviving half. That helper then resolved the identifier it
    landed on against the loaded module, because the table was a module constant;
    no task has one any more, so what it lands on is a spec field and `_spec_field`
    takes over. Any shape it does not understand raises instead of returning
    something -- an unrecognised call site must be a red test, never a quiet
    pass."""
    seen: set[str] = set()
    while isinstance(expr, ast.Name):
        assert expr.id not in seen, f"{where}: {expr.id} resolves in a cycle"
        assert expr.id in scope, (
            f"{where}: the table comes from `{expr.id}`, which main() does not assign -- "
            "this lock can no longer see which table is used"
        )
        seen.add(expr.id)
        expr = scope[expr.id]
    return expr


def _spec_field(expr: ast.expr, where: str) -> str:
    """The `spec.<field>` an argument is, or a failure saying it is not one.

    The parameterised twin of `_bound_table`: there is no constant to resolve any
    more, so what an argument must be is a field of the spec `main()` resolved."""
    assert (
        isinstance(expr, ast.Attribute)
        and isinstance(expr.value, ast.Name)
        and expr.value.id == "spec"
    ), f"{where}: expected a field of the resolved spec, got {ast.dump(expr)[:120]}"
    return expr.attr


def _table_arg(expr: ast.expr, where: str) -> ast.expr:
    """The single argument of a `DEFAULT.table(...)` call."""
    assert (
        isinstance(expr, ast.Call)
        and isinstance(expr.func, ast.Attribute)
        and expr.func.attr == "table"
    ), f"{where}: expected a DEFAULT.table(...) call, got {ast.dump(expr)[:120]}"
    assert len(expr.args) == 1 and not expr.keywords, f"{where}: unexpected DEFAULT.table args"
    return expr.args[0]


def test_the_parameterised_ingest_binds_every_coordinate_to_the_one_resolved_spec():
    """Which spec FIELD each argument receives -- not merely that a registry is read.

    This closes a gap that was graded Minor while the ingest scripts were
    per-table: nothing locked their call sites, so a literal redirect inside one
    passed the whole suite. Parameterising them makes the redirect cheaper, not
    harder -- `spec.quarantine` in place of `spec.staging`, or a second
    `table_spec("lookup")` -- and both leave `table_spec(` in the source, so the
    source-level test above stays green through either. What is pinned here is
    that ONE spec, resolved from argv rather than from a literal, feeds every
    coordinate: the schema read with, the dir read from, the checkpoint deciding
    which files are new, and the table written to."""
    main = _main_of("bronze_ingest")
    scope = _locals_of(main, "bronze_ingest")
    resolved = _sole_call(main, "table_spec", "bronze_ingest")
    assert not any(isinstance(arg, ast.Constant) for arg in resolved.args), (
        "bronze_ingest.py resolves its spec from a literal; the table is a job "
        "parameter, and a literal here pins every job that runs this file to one table"
    )
    assert scope.get("spec") is resolved, (
        "bronze_ingest.py main() no longer binds the table_spec(...) result to `spec`, "
        "so this lock cannot tell which spec the coordinates below came from"
    )
    stream = _sole_call(main, "bronze_stream", "bronze_ingest")
    assert len(stream.args) >= 5, "bronze_stream() no longer takes contract/table_key here"
    bound = {
        "contract": _spec_field(stream.args[2], "bronze_stream contract"),
        "source_dir": _spec_field(
            _sole_call(main, "landing_table", "bronze_ingest").args[0], "landing_table"
        ),
        "checkpoint": _spec_field(
            _sole_call(main, "checkpoint_location", "bronze_ingest").args[1], "checkpoint"
        ),
        "written": _spec_field(
            _table_arg(_sole_call(main, "toTable", "bronze_ingest").args[0], "toTable"),
            "toTable",
        ),
    }
    assert bound == {
        "contract": "contract",
        "source_dir": "subdir",
        "checkpoint": "table_key",
        "written": "staging",
    }


def test_the_lookup_ingest_writes_the_lookup_tables_and_only_those():
    """What `EXPECTED_TABLES["bronze_ingest"]` used to pin, restated for the registry.

    The old entry asserted that script's write target resolved to lookup staging.
    The registry rewrite dropped the constant that lock read, so for one commit the
    lookup ingest was protected LESS than before: retargeting it to
    `table_spec("estabelecimentos")` cross-wired lookup rows into estab staging,
    under the estab checkpoint, with every test green.

    Note the inversion against the parameterised script above: there a string
    literal is the defect, because the table is a job parameter. Here the literal
    is the REQUIREMENT -- this entry point exists for exactly one table, and any
    other value silently redirects it."""
    main = _main_of("bronze_lookup_ingest")
    scope = _locals_of(main, "bronze_lookup_ingest")
    resolved = _sole_call(main, "table_spec", "bronze_lookup_ingest")
    assert (
        len(resolved.args) == 1
        and isinstance(resolved.args[0], ast.Constant)
        and resolved.args[0].value == "lookup"
    ), (
        "bronze_lookup_ingest.py resolves a spec that is not the lookup's; this entry "
        "point is the lookup's alone, and any other table here writes its rows into "
        f"another table's staging -- got {ast.dump(resolved)[:120]}"
    )
    assert scope.get("spec") is resolved, (
        "bronze_lookup_ingest.py main() no longer binds the table_spec(...) result to "
        "`spec`, so this lock cannot tell which spec the coordinates below came from"
    )
    bound = {
        "checkpoint": _spec_field(
            _sole_call(main, "checkpoint_location", "bronze_lookup_ingest").args[1],
            "checkpoint",
        ),
        "written": _spec_field(
            _table_arg(
                _sole_call(main, "toTable", "bronze_lookup_ingest").args[0], "toTable"
            ),
            "toTable",
        ),
    }
    assert bound == {"checkpoint": "table_key", "written": "staging"}


def _resolved_spec(main: ast.FunctionDef, scope: dict[str, ast.expr], script: str) -> ast.Call:
    """The one `table_spec(...)` call, checked to be argv-driven and bound to `spec`.

    Shared by the gate and the promote, which the ingest tests spell out inline
    because the lookup ingest's requirement is the opposite one (a literal)."""
    resolved = _sole_call(main, "table_spec", script)
    assert not any(isinstance(arg, ast.Constant) for arg in resolved.args), (
        f"{script}.py resolves its spec from a literal; the table is a job parameter, "
        "and a literal here pins every job that runs this file to one table"
    )
    assert scope.get("spec") is resolved, (
        f"{script}.py main() no longer binds the table_spec(...) result to `spec`, so "
        "this lock cannot tell which spec the coordinates below came from"
    )
    return resolved


def _qualified_spec_fields(main: ast.FunctionDef, script: str) -> list[str]:
    """Every spec field that main() hands to `DEFAULT.table(...)`, as a sorted list.

    A list rather than a set: a task that qualified the same coordinate twice under
    two different names would be invisible to a set, and this is the lock that has
    to see a task touching a table it should not."""
    return sorted(
        _spec_field(_table_arg(node, f"{script} DEFAULT.table"), f"{script} DEFAULT.table")
        for node in ast.walk(main)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "table"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "DEFAULT"
    )


def test_the_promote_binds_every_coordinate_to_the_one_resolved_spec():
    """Which spec FIELD each argument receives -- not merely that a registry is read.

    What `EXPECTED_TABLES["promote_batch"]` used to pin, restated. That entry
    enumerated module constants, and a mutation probe proved it was not enough on
    its own: redirecting `staging_table=` to the quarantine constant and
    `bronze_table=` to a literal, while leaving every constant imported, kept it
    green. That mutation is a promote which reads the quarantine and appends into
    the lookup table. Parameterising made the same redirect cheaper, not harder --
    `spec.quarantine` for `spec.staging` is one identifier -- so the binding is
    what has to be pinned.

    The last assertion is the set the old entry was: exactly the three coordinates
    of one table, no fourth, and each qualified exactly once."""
    main = _main_of("promote_batch")
    scope = _locals_of(main, "promote_batch")
    _resolved_spec(main, scope, "promote_batch")
    call = _sole_call(main, "promote_batch", "promote_batch")
    bound = {
        kw.arg: _spec_field(
            _table_arg(
                _deref(kw.value, scope, f"promote_batch {kw.arg}="),
                f"promote_batch {kw.arg}=",
            ),
            f"promote_batch {kw.arg}=",
        )
        for kw in call.keywords
        if kw.arg in {"staging_table", "bronze_table"}
    }
    assert bound == {"staging_table": "staging", "bronze_table": "bronze"}
    # The quarantine is named in the recovery hint only, so it has no keyword to
    # bind; it is covered by the whole-main sweep below.
    assert _qualified_spec_fields(main, "promote_batch") == [
        "bronze", "quarantine", "staging",
    ]


def test_the_gate_binds_the_table_it_reads_and_the_table_it_writes_to_one_spec():
    """What `EXPECTED_TABLES["dq_gate_batch"]` pinned, restated: the table the gate
    READS the batch from and the table it WRITES rejects to must stay distinct, and
    must be the staging and quarantine of the SAME resolved spec.

    Collapsing them is not hypothetical -- a gate that reads its batch from the
    quarantine, or writes rejects into staging, is a one-identifier edit at either
    call site, and `table_spec(` would still be in the source afterwards."""
    main = _main_of("dq_gate_batch")
    scope = _locals_of(main, "dq_gate_batch")
    _resolved_spec(main, scope, "dq_gate_batch")
    read = _sole_call(main, "batch_rows", "dq_gate_batch")
    assert len(read.args) >= 2, "batch_rows() no longer takes the table positionally"
    written = _sole_call(main, "saveAsTable", "dq_gate_batch")
    assert len(written.args) >= 1, "saveAsTable() no longer takes the table positionally"
    bound = {
        "read": _spec_field(
            _table_arg(
                _deref(read.args[1], scope, "dq_gate_batch batch_rows table"),
                "dq_gate_batch batch_rows table",
            ),
            "dq_gate_batch batch_rows table",
        ),
        "written": _spec_field(
            _table_arg(
                _deref(written.args[0], scope, "dq_gate_batch saveAsTable"),
                "dq_gate_batch saveAsTable",
            ),
            "dq_gate_batch saveAsTable",
        ),
    }
    assert bound == {"read": "staging", "written": "quarantine"}
    assert _qualified_spec_fields(main, "dq_gate_batch") == ["quarantine", "staging"]


def test_the_one_surviving_gate_is_the_batch_scoped_one():
    """What `test_the_two_gates_scope_differently_today` locked, after the collapse.

    It pinned that `dq_gate.py` was whole-table and `dq_gate_batch.py` was
    batch-scoped, so the collapse could only go one way. It went that way: the
    whole-table gate is gone and the lookup inherits batch scoping, which is
    carry-forward #7 paid as a consequence. A gate that quietly went back to
    evaluating the whole staging table would re-wedge every clean batch behind one
    historical bad row, so the surviving direction is asserted, not assumed."""
    assert not (_SRC / "dq_gate.py").exists(), (
        "the whole-table gate is back; the lookup would stop being batch-scoped"
    )
    scoped = (_SRC / "dq_gate_batch.py").read_text(encoding="utf-8")
    assert "batch_rows(" in scoped


def test_the_one_surviving_promote_appends_a_batch_for_every_table():
    """What `test_the_lookup_promote_overwrites_and_the_estab_promote_appends`
    locked, after the collapse -- and the one semantic change this refactor makes.

    The deleted `promote.py` overwrote the lookup's bronze table from the WHOLE
    staging table, which writes 2x the rows the moment a second batch exists. The
    lookup now goes through the batch promote instead. `promote_batch(` alone
    would be a weaker claim than this test's name -- it would still hold if the
    shared helper started overwriting -- so the absence of an overwrite is
    asserted too. Bronze holds 71.9M estab rows; an overwrite from one batch's
    staging rows is the destructive direction."""
    assert not (_SRC / "promote.py").exists(), (
        "the overwriting promote is back; a second lookup batch would double its rows"
    )
    source = (_SRC / "promote_batch.py").read_text(encoding="utf-8")
    assert "promote_batch(" in source
    assert 'mode("overwrite")' not in source


def test_the_reclaim_proves_persistence_from_bronze_and_deletes_only_under_landing():
    """The one task that DELETES, so the two coordinates it binds are the two that
    decide whether a file survives.

    WHICH TABLE PROVES IT: `spec.bronze`, never `spec.staging`. Staging holds rows
    that have been read but not yet promoted, so a file whose rows are only there
    has not been proven persisted -- and once its bytes are gone the only way back
    is re-unzipping the source. `spec.staging` for `spec.bronze` is a
    one-identifier edit that leaves `table_spec(` in the source and every other
    test in this file green, which is exactly the class of edit this module was
    written for.

    WHICH DIRECTORY IS IN REACH: `spec.subdir`, this table's own landing dir. Its
    sibling `zips/<table>` holds the only copies of the source, and the last
    assertion is what keeps them unreachable: bronze is the only coordinate this
    task qualifies at all."""
    main = _main_of("reclaim_landing")
    scope = _locals_of(main, "reclaim_landing")
    _resolved_spec(main, scope, "reclaim_landing")
    proof = _sole_call(main, "files_of_batch", "reclaim_landing")
    assert len(proof.args) >= 2, "files_of_batch() no longer takes the table positionally"
    bound = {
        "proof": _spec_field(
            _table_arg(
                _deref(proof.args[1], scope, "reclaim_landing files_of_batch table"),
                "reclaim_landing files_of_batch table",
            ),
            "reclaim_landing files_of_batch table",
        ),
        "deletes_under": _spec_field(
            _sole_call(main, "landing_table", "reclaim_landing").args[0],
            "reclaim_landing landing_table",
        ),
    }
    assert bound == {"proof": "bronze", "deletes_under": "subdir"}
    assert _qualified_spec_fields(main, "reclaim_landing") == ["bronze"]


@pytest.mark.parametrize("script", ["dq_gate_batch", "promote_batch"])
def test_each_task_takes_its_rule_set_from_the_spec_it_resolved(script):
    """What `test_each_task_uses_its_own_rule_set` locked, restated.

    It asserted the literal `rules_for("estabelecimentos")` in each source. One
    task serves every table now, so the rule set has to follow the SAME spec the
    coordinates came from -- a task gating estab rows against the lookup's rules
    would pass rows the estab contract rejects."""
    main = _main_of(script)
    scope = _locals_of(main, script)
    _resolved_spec(main, scope, script)
    call = _sole_call(main, "rules_for", script)
    assert len(call.args) == 1, f"{script}.py: rules_for() no longer takes one argument"
    assert _spec_field(call.args[0], f"{script} rules_for") == "contract"


def test_the_promote_takes_its_constraint_ddl_from_the_spec():
    """What the two `..._constraints_are_the_ones_bronze_carries_today` tests
    locked, restated -- the DDL itself is now Task 4's
    `test_the_constraints_are_the_ones_the_live_tables_carry`, against the
    registry, so re-spelling it here would be a copy that can drift.

    What is left for this file is the wiring: the promote must issue the resolved
    spec's statements and hold no DDL of its own. `cnpj_basico` exists in three of
    the four CNPJ contracts, so a constraint copied back into this script would
    look correct while asserting another table's key."""
    source = (_SRC / "promote_batch.py").read_text(encoding="utf-8")
    code = "\n".join(
        line for line in source.splitlines() if not line.strip().startswith("#")
    )
    assert "ALTER TABLE" not in code, "promote_batch.py spells DDL of its own again"
    assert "spec.constraints" in code


# ---------------------------------------------------------------------------
# The other half: which table each job YAML HANDS its tasks.
#
# Everything above pins a script against the spec it resolved. Nothing above can
# see the argument that resolution starts from, and that argument is a literal in
# a YAML file written by copying the previous table's. `bronze_ingest.py` handed
# "estabelecimentos" by the empresas job reads estabelecimentos' landing dir under
# estabelecimentos' checkpoint and writes estabelecimentos' staging -- with every
# test above green, and the run SUCCEEDS.
# ---------------------------------------------------------------------------

# Which ingestion-flow job serves which registered table. Total over the registry
# and asserted so below: a table registered without a job here is a table nothing
# ingests, and the next person to copy a job YAML would have no list to add to.
_JOB_OF = {
    "lookup": "bronze_job.yml",
    "estabelecimentos": "bronze_estabelecimentos_job.yml",
    "empresas": "bronze_empresas_job.yml",
    "socios": "bronze_socios_job.yml",
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
    this job's own table, passes every check in this file so far, and runs green --
    it re-unzips, the gate then finds zero rows in the batch, `bad_row_count` is 0,
    and the promote no-ops on an empty in-flow batch. Nothing errors and nothing is
    ingested. A dropped `max_retries: 0` and a swapped `depends_on` are the same
    class of thing.

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
    pointing at it, and nothing else."""
    shape = _shape_of("socios")
    assert shape[0]["task_key"] == "ensure_masked_table"
    rest = [dict(task) for task in shape[1:]]
    assert rest[0]["task_key"] == "unzip"
    assert rest[0].pop("depends_on") == [{"task_key": "ensure_masked_table"}]
    assert rest == _shape_of("estabelecimentos"), (
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
    assert not tasks["ensure_masked_table"].get("depends_on"), (
        f"{_JOB_OF[table]}: ensure_masked_table waits on something, so it is no longer "
        "the first thing the run does"
    )
    for key in tasks:
        if key == "ensure_masked_table":
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
