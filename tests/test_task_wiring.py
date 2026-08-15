"""Which table each job task touches.

Written as characterization tests BEFORE the registry refactor moved any of it:
they asserted the wiring as it stood so the refactor could only preserve it. The
defect they exist to prevent is real and documented in
bronze_estabelecimentos_job.yml -- a hardcoded quarantine name "sent estab
triagers to a table full of unrelated F1.2 lookup rows".

Job scripts under databricks/src are entry points, not part of the opl wheel, so
they are loaded by path with the same importlib pattern the other task tests use.
Nothing here starts Spark: every assertion is about wiring, not data.

TWO HALVES, AND THE SECOND ONE ARRIVED IN F1.4b AND THEN MOVED OUT. This file is
the first half: it reads the SCRIPTS, and a task must resolve every coordinate from
the one spec it got from argv. That is only half a wiring claim, because a script
that resolves its spec perfectly still ingests whatever table its job YAML hands it
-- and the YAMLs are written by copying the previous table's file, which is the
paste the second half exists to refuse. A job that reads the wrong table's landing
dir, or promotes into the wrong table's bronze, does not error: it SUCCEEDS, having
done the wrong thing.

THAT SECOND HALF IS NOW `test_job_yaml_wiring.py`, split out when the two F1.4b
jobs carried this file to 853 lines, over the 800-line limit. The seam is the one
this paragraph already drew and not a line count: this file changes when an ENTRY
POINT changes, that one changes when a JOB is added. Read together they say "this
script, handed this table"; alone neither is a wiring claim, which is why each
file's docstring points at the other. Nothing was shared across the seam and so
nothing was copied: the AST helpers below read scripts and stayed, every YAML
helper went whole.

AND THE SERVERLESS-CAPABILITY SWEEP LEFT IN F-API'S FIX PASS, at 831 lines, when the two
PTAX entry points got the coordinate locks the other five already had. It is now
`tests/test_serverless_capabilities.py`, and again the seam was already drawn -- it sat
under a section header of its own because it is a different subject: it asks whether a
module uses a capability the deploy target refuses, over a file set (`src/opl/**` plus
`databricks/src/*`) that no other lock here looks at, and it says nothing about which
table anything touches. This file changes when an entry point changes; that one changes
when the platform refuses something new.

AND THE MONTH LOCK LEFT IN F-DB TASK 1, at 783 lines, with F-DB's Postgres ingest entry
point still to be added. It is now `tests/test_month_wiring.py`, and the seam is this
file's own title: the month is a JOB PARAMETER, it appears in no registry, and its
consumers are a landing directory, an Auto Loader checkpoint and an audit column rather
than a staging, bronze or quarantine name. This file changes when a table's COORDINATES
change; that one changes when an entry point gains or loses a consumer of its window.
Read together they say "one spec feeds every coordinate, one month feeds every
consumer"; a task can satisfy either completely while failing the other.

THAT SPLIT IS THE FIRST ONE OUT OF THIS FILE WITH SOMETHING SHARED ACROSS THE SEAM, so
the three AST readers both halves ask for -- `main_of`, `sole_call`, `locals_of` -- went
to `tests/task_ast.py` and were NOT copied, on `tests/job_yaml.py`'s precedent and for
its reason. Everything below that resolves a `opl.bronze.registry` spec stayed: only
this half has a spec to resolve.

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
from task_ast import locals_of, main_of, sole_call

from opl.bronze.registry import REGISTRY

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"

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
    # F1b Task 3, and both are deliberate additions in this list's own sense. The
    # ingest is the third spelling of "read a landing dir into staging" and the one
    # that reads the OTHER landing root; the generator is the first entry point that
    # WRITES a landing dir, so a literal subdir in it would drop a stream into a
    # directory no registered table's stream reads -- an ingest that drains nothing
    # and reports SUCCESS.
    "bronze_payments_ingest",
    "generate_payments",
    # F-API Task 2, and both are deliberate additions in this list's own sense. The
    # ingest is the fourth spelling of "read a landing dir into staging" and the one that
    # reads the THIRD landing root; the fetch is the first entry point that makes an
    # outbound HTTP call, so a literal subdir in it would write a landed file into a
    # directory no registered table's stream reads -- an ingest that drains nothing and
    # reports SUCCESS, after 60 requests have already been made.
    "bronze_ptax_ingest",
    "fetch_ptax",
]


@pytest.mark.parametrize("script", _TABLE_TASKS)
def test_no_task_names_a_bronze_table_directly_any_more(script):
    """The collapse's whole point. A task that spells a table name is a task
    whose staging/quarantine pair can drift from the one the registry declares --
    which is how a triager was sent to a table full of unrelated rows.

    DERIVED FROM THE REGISTRY SINCE F-API'S FIX PASS, and the literal it replaced was
    `bronze_cnpj_`. That prefix is every RFB table's and NO other source's, so a task
    spelling `bronze_payments_staging` or `bronze_ptax_staging` passed this sweep -- on two
    entry points added by a phase whose own tables are the ones outside the prefix. Every
    registered name is asked for now, in every role, so a third source is covered on the day
    it is registered rather than on the day somebody widens a string here.

    ON WORD BOUNDARIES, WHICH IS NOT FASTIDIOUSNESS -- a plain substring match turned this
    red on both PTAX tasks the moment it was widened, and correctly by its own rule and
    wrongly in fact: `bronze_payments` is the payments BRONZE TABLE and also the stem of
    `bronze_payments_ingest.py`, which both new tasks name in a refusal message that tells
    an operator which entry point to run instead. `_` is a word character, so
    `\\bbronze_payments\\b` does not match inside `bronze_payments_ingest` while
    `bronze_ptax_staging` still matches itself exactly.

    Comment lines are stripped before the check: a comment that cites the table a
    real incident happened in is this repo's house style, and is not wiring."""
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    code = "\n".join(
        line for line in source.splitlines() if not line.strip().startswith("#")
    )
    named = sorted(
        {
            value
            for spec in REGISTRY.values()
            for value in (spec.staging, spec.bronze, spec.quarantine, spec.table_key)
            if re.search(rf"\b{re.escape(value)}\b", code)
        }
    )
    assert not named, f"{script}.py names bronze table(s) {named} directly"
    assert "table_spec(" in code


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
    main = main_of("bronze_ingest")
    scope = locals_of(main, "bronze_ingest")
    resolved = sole_call(main, "table_spec", "bronze_ingest")
    assert not any(isinstance(arg, ast.Constant) for arg in resolved.args), (
        "bronze_ingest.py resolves its spec from a literal; the table is a job "
        "parameter, and a literal here pins every job that runs this file to one table"
    )
    assert scope.get("spec") is resolved, (
        "bronze_ingest.py main() no longer binds the table_spec(...) result to `spec`, "
        "so this lock cannot tell which spec the coordinates below came from"
    )
    stream = sole_call(main, "bronze_stream", "bronze_ingest")
    assert len(stream.args) >= 5, "bronze_stream() no longer takes contract/table_key here"
    bound = {
        "contract": _spec_field(stream.args[2], "bronze_stream contract"),
        "source_dir": _spec_field(
            sole_call(main, "landing_table", "bronze_ingest").args[0], "landing_table"
        ),
        "checkpoint": _spec_field(
            sole_call(main, "checkpoint_location", "bronze_ingest").args[1], "checkpoint"
        ),
        "written": _spec_field(
            _table_arg(sole_call(main, "toTable", "bronze_ingest").args[0], "toTable"),
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
    main = main_of("bronze_lookup_ingest")
    scope = locals_of(main, "bronze_lookup_ingest")
    resolved = sole_call(main, "table_spec", "bronze_lookup_ingest")
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
            sole_call(main, "checkpoint_location", "bronze_lookup_ingest").args[1],
            "checkpoint",
        ),
        "written": _spec_field(
            _table_arg(
                sole_call(main, "toTable", "bronze_lookup_ingest").args[0], "toTable"
            ),
            "toTable",
        ),
    }
    assert bound == {"checkpoint": "table_key", "written": "staging"}


def _resolved_spec(main: ast.FunctionDef, scope: dict[str, ast.expr], script: str) -> ast.Call:
    """The one `table_spec(...)` call, checked to be argv-driven and bound to `spec`.

    Shared by the gate and the promote, which the ingest tests spell out inline
    because the lookup ingest's requirement is the opposite one (a literal)."""
    resolved = sole_call(main, "table_spec", script)
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
    main = main_of("promote_batch")
    scope = locals_of(main, "promote_batch")
    _resolved_spec(main, scope, "promote_batch")
    call = sole_call(main, "promote_batch", "promote_batch")
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
    main = main_of("dq_gate_batch")
    scope = locals_of(main, "dq_gate_batch")
    _resolved_spec(main, scope, "dq_gate_batch")
    read = sole_call(main, "batch_rows", "dq_gate_batch")
    assert len(read.args) >= 2, "batch_rows() no longer takes the table positionally"
    written = sole_call(main, "saveAsTable", "dq_gate_batch")
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
    main = main_of("reclaim_landing")
    scope = locals_of(main, "reclaim_landing")
    _resolved_spec(main, scope, "reclaim_landing")
    proof = sole_call(main, "files_of_batch", "reclaim_landing")
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
            sole_call(main, "landing_table", "reclaim_landing").args[0],
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
    main = main_of(script)
    scope = locals_of(main, script)
    _resolved_spec(main, scope, script)
    call = sole_call(main, "rules_for", script)
    assert len(call.args) == 1, f"{script}.py: rules_for() no longer takes one argument"
    assert _spec_field(call.args[0], f"{script} rules_for") == "contract"


def _whole_spec_arg(expr: ast.expr, where: str) -> str:
    """The argument is the resolved `spec` itself, not one of its fields.

    `registry_landing.landing_dir` and `landing_tmp_dir` take the WHOLE spec on purpose --
    their docstring says why: the landing root is resolved from `spec.landing`, so handing
    them `spec.subdir` would put the mapping back in the caller. So the two PTAX tasks
    cannot be locked with `_spec_field`, and this is the assertion that replaces it."""
    assert isinstance(expr, ast.Name) and expr.id == "spec", (
        f"{where}: expected the resolved spec itself, got {ast.dump(expr)[:120]}"
    )
    return expr.id


def test_the_ptax_ingest_binds_every_coordinate_to_the_one_resolved_spec():
    """THE FOURTH INGEST, LOCKED LIKE THE FIRST -- and it had no such lock at all.

    F-API Task 2 added two entry points and neither got the coordinate lock the other five
    have, so a redirect inside either passed the whole suite: `spec.quarantine` in place of
    `spec.staging`, or a second `table_spec("payments")`, and both leave `table_spec(` in
    the source so the anti-hardcode sweep above stays green too.

    What is pinned is that ONE spec, resolved from argv, feeds every coordinate: the schema
    read with, the directory read from, the checkpoint deciding which of its files are new,
    and the table written to. The source directory is asked of `landing_dir` and takes the
    WHOLE spec, which is the difference from `bronze_ingest`'s lock and is deliberate --
    the landing root comes from `spec.landing` there rather than from this file knowing the
    layout."""
    main = main_of("bronze_ptax_ingest")
    scope = locals_of(main, "bronze_ptax_ingest")
    _resolved_spec(main, scope, "bronze_ptax_ingest")
    stream = sole_call(main, "bronze_stream", "bronze_ptax_ingest")
    assert len(stream.args) >= 5, "bronze_stream() no longer takes contract/table_key here"
    bound = {
        "contract": _spec_field(stream.args[2], "ptax bronze_stream contract"),
        "source_dir": _whole_spec_arg(
            sole_call(main, "landing_dir", "bronze_ptax_ingest").args[1], "ptax landing_dir"
        ),
        "checkpoint": _spec_field(
            sole_call(main, "checkpoint_location", "bronze_ptax_ingest").args[1],
            "ptax checkpoint",
        ),
        "written": _spec_field(
            _table_arg(sole_call(main, "toTable", "bronze_ptax_ingest").args[0], "toTable"),
            "ptax toTable",
        ),
    }
    assert bound == {
        "contract": "contract",
        "source_dir": "spec",
        "checkpoint": "table_key",
        "written": "staging",
    }
    # STAGING AND NOTHING ELSE. This task must not qualify bronze or quarantine at all:
    # the promote reads staging as trusted input and the gate writes the quarantine, and an
    # ingest that could name either could append raw rows into a table that has passed DQ.
    assert _qualified_spec_fields(main, "bronze_ptax_ingest") == ["staging"]


def test_the_fetch_writes_only_into_the_directories_its_own_spec_resolves():
    """THE WRITER'S HALF, and the reason it needs a lock of its own: this is the first
    entry point in the repository that makes an outbound HTTP call, and the file it lands
    is the input the ingest task then reads.

    Both directories come from the SAME resolved spec through
    `registry_landing.landing_dir`/`landing_tmp_dir`, so the file cannot be staged under
    one root and landed under another -- which is an `os.replace` across FUSE mounts, or
    worse a successful move into a directory no stream reads. And it must qualify NO Delta
    table at all: a fetch that named one is a fetch that could write to it.

    `generate_payments` is the same shape one root along and is locked by
    `test_month_wiring.py`'s `_MONTH_CONSUMERS` only, which is the gap this closes for
    the newer pair."""
    main = main_of("fetch_ptax")
    scope = locals_of(main, "fetch_ptax")
    _resolved_spec(main, scope, "fetch_ptax")
    for callee in ("landing_dir", "landing_tmp_dir"):
        call = sole_call(main, callee, "fetch_ptax")
        assert len(call.args) == 3, f"fetch_ptax: {callee}() no longer takes (cfg, spec, month)"
        _whole_spec_arg(call.args[1], f"fetch_ptax {callee}")
    assert _qualified_spec_fields(main, "fetch_ptax") == [], (
        "fetch_ptax.py qualifies a Delta table; it writes a file into a Volume and reads "
        "nothing, so a table name here is a coordinate it has no use for"
    )


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
