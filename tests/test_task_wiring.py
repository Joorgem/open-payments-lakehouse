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
from pathlib import Path

import pytest

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


def _kwarg(call: ast.Call, name: str, where: str) -> ast.expr:
    """The value of a `name=` keyword on `call`, or a failure saying it is absent.

    An absent keyword must be a red test rather than a skipped assertion: the whole
    point below is that the month is passed EVERYWHERE it is consumed, so "not
    passed" is the defect, not a case to tolerate."""
    for keyword in call.keywords:
        if keyword.arg == name:
            return keyword.value
    raise AssertionError(f"{where}: this call has no {name}= keyword")


# Every place an ingest entry point hands the month to something, as
# (callee, positional index, keyword) -- exactly one of the last two is used.
# ENUMERATED, not discovered: a fifth consumer added without an entry here would
# be a fifth chance for the month to diverge, and a lock that globbed for `month`
# would pass on whatever it happened to find.
_MONTH_CONSUMERS: dict[str, list[tuple[str, int | None, str | None]]] = {
    "bronze_ingest": [
        # the directory the files are READ from
        ("landing_table", 1, None),
        # the state that records which of those files have been read
        ("bronze_stream", None, "month"),
        ("checkpoint_location", None, "month"),
        # the value stamped into every row
        ("add_audit_columns", None, "snapshot_month"),
    ],
    "bronze_lookup_ingest": [
        ("bronze_lookup_stream", 2, None),
        ("checkpoint_location", None, "month"),
        ("add_audit_columns", None, "snapshot_month"),
    ],
    # F1b Task 3. Four consumers again, and the same four FACTS -- the directory read,
    # the state that records which of its files have been read, and the value stamped
    # into every row -- reached through the second landing root and the four-column
    # audit stamp a generated source takes.
    "bronze_payments_ingest": [
        ("landing_generated_table", 1, None),
        ("bronze_stream", None, "month"),
        ("checkpoint_location", None, "month"),
        ("add_common_audit_columns", None, "snapshot_month"),
    ],
    # The WRITER, and it belongs in this lock for the same reason every reader does:
    # its month decides which directory the stream is written into, and the ingest task
    # that follows resolves ITS source dir from the same job parameter. A month that
    # diverged here would write one month's landing dir and read another's -- a job
    # whose every task reports SUCCESS having ingested nothing. The staging dir is the
    # second consumer because a file staged under one month and replaced into another
    # is a cross-device rename that fails, or worse, does not.
    "generate_payments": [
        ("landing_generated_table", 1, None),
        ("landing_generated_tmp", 1, None),
    ],
}


@pytest.mark.parametrize("script", sorted(_MONTH_CONSUMERS))
def test_every_consumer_of_the_month_reads_the_one_required_local(script):
    """ONE month local, bound by `require_month`, feeding every consumer.

    `bronze_ingest.py` already said why for two of them -- "the SAME `month` the
    stream read from -- one local, fed to both, so the snapshot the rows are
    stamped with cannot drift from the folder they came out of". F1.4b PR B Task 5
    Step 0 added a third and fourth consumer, and the third is the one that makes
    this a lock rather than a tidiness check: the Auto Loader checkpoint. Keyed on a
    month that is not the source directory's, a run drains 2026-07's files under
    2026-06's checkpoint -- Spark's recovery semantics call a changed source
    "generally not allowed ... likely to fail with unpredictable errors".

    The second half is that the local is `require_month`'s result. A local bound to
    `DEFAULT.month`, or to `args[2] if len(args) > 2 else DEFAULT.month`, would
    satisfy every consistency check above while being the ONE value four entry
    points have already substituted by accident (`require_month`'s own docstring
    names them). Consistent and wrong is what this repo keeps paying for.

    Reads the AST, so it sees the wiring rather than a run: nothing imports Spark."""
    main = _main_of(script)
    scope = _locals_of(main, script)
    names: set[str] = set()
    for callee, position, keyword in _MONTH_CONSUMERS[script]:
        where = f"{script}: {callee}"
        call = _sole_call(main, callee, script)
        passed = call.args[position] if keyword is None else _kwarg(call, keyword, where)
        assert isinstance(passed, ast.Name), (
            f"{where} is handed {ast.dump(passed)[:80]}, not a local name -- a second "
            "lookup of the month here can name a month the rest of this run did not use"
        )
        names.add(passed.id)
    assert len(names) == 1, (
        f"{script}.py feeds the month to its consumers from {sorted(names)} -- more than "
        "one local, so the checkpoint, the source dir and the stamped column can diverge"
    )
    bound = scope.get(names.pop())
    assert (
        isinstance(bound, ast.Call)
        and isinstance(bound.func, ast.Name)
        and bound.func.id == "require_month"
    ), (
        f"{script}.py's month local is not `require_month(...)`'s result; a defaulted "
        "month is consistent across every consumer above and still wrong in all of them"
    )
    # AND THE SAME PROPERTY FROM THE OTHER SIDE, because the enumeration above cannot
    # see a consumer it does not name: a fifth one added tomorrow as
    # `something(..., month=DEFAULT.month)` satisfies every assertion so far by being
    # invisible to them. This needs no list -- the substitution has exactly one
    # spelling, an attribute access for `month` on the config -- so it catches the
    # consumer this file has not been taught about yet. `require_month`'s docstring
    # names four entry points that each wrote it; two of them are these.
    #
    # THE SECOND DOOR: the owner is matched on its LAST dotted component, not on being
    # a bare `Name`. `isinstance(node.value, ast.Name)` saw `DEFAULT.month` and missed
    # `opl.config.DEFAULT.month` -- the same substitution written with the import spelled
    # out, which is a normal thing to write and which this file's own imports make
    # available. Unparsing and taking the tail catches every spelling of the owner while
    # still requiring that it BE the config object, so `spec.month` or a local
    # `parsed.month` is untouched.
    substitutions = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.Attribute)
        and node.attr == "month"
        and ast.unparse(node.value).rsplit(".", 1)[-1] in {"DEFAULT", "cfg"}
    ]
    assert not substitutions, (
        f"{script}.py main() reads the config's pinned month directly ("
        f"{len(substitutions)} occurrence(s)). That value equals the job YAMLs' own "
        "default, so substituting it changes nothing observable until the first run "
        "for another month -- and then it is wrong in the data, the checkpoint or a "
        "delete boundary, with nothing in the log naming the month that was used"
    )


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


# --- serverless refuses explicit caching -----------------------------------------------

_CACHING_CALLS = frozenset({"persist", "cache", "unpersist"})


def _python_sources_that_run_on_databricks() -> list[Path]:
    """Every module that can execute inside a job: the wheel's `opl` package and the
    entry points beside it. Test code is excluded because it runs on local Spark, where
    caching is supported and is a legitimate thing for a fixture to do."""
    repo = Path(__file__).resolve().parents[1]
    return sorted((repo / "src" / "opl").rglob("*.py")) + sorted(
        (repo / "databricks" / "src").glob("*.py")
    )


@pytest.mark.parametrize(
    "path", _python_sources_that_run_on_databricks(), ids=lambda p: p.name
)
def test_no_module_that_runs_on_databricks_asks_the_engine_to_cache(path: Path):
    """`persist()`/`cache()` are a HARD REFUSAL on serverless, not a slow path.

    THIS IS A REGRESSION GUARD FOR A FAILURE THAT ONLY THE DEPLOY TARGET PRODUCES, which
    is why it is a source check rather than a behavioural one. Local Spark supports
    caching happily, so every test in this repository passes with a `persist()` in place
    and the refusal appears for the first time in the workspace:

        [NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless
        compute. SQLSTATE: 0A000

    Measured 2026-08-09 on run `604594149706864`, where `effectivity._append_and_count
    _closes` called `rows.persist()` and failed the task twice. Nothing was written --
    the call precedes both the count and the append -- but the phase lost the run, and a
    reviewer had independently recommended adding a SECOND `persist()` a few lines above
    it, which would have failed the same way.

    The remedy for a frame consumed twice on this platform is to materialise it or to
    restructure so it is read once. Asking the engine to hold it is not available.

    THE CHECK IS OVER THE AST AND NOT OVER THE TEXT, which is not fastidiousness: the
    first spelling of this guard matched substrings and went red on `effectivity.py`,
    whose docstring now EXPLAINS the refusal and therefore contains `rows.persist()` as
    prose. A guard that cannot tell a call from a mention of a call punishes documenting
    the very thing it is guarding."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    offenders = sorted({
        f"{node.func.attr}() at line {node.lineno}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _CACHING_CALLS
    })
    assert not offenders, (
        f"{path.name} calls {offenders} -- serverless refuses explicit caching outright "
        "(NOT_SUPPORTED_WITH_SERVERLESS), so this runs locally and fails in the workspace"
    )
