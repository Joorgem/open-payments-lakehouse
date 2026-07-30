"""Which table each job task touches, locked BEFORE the registry refactor moves it.

These are characterization tests: they assert today's behaviour so the refactor
that follows can only preserve it. The defect they exist to prevent is real and
documented in bronze_estabelecimentos_job.yml -- a hardcoded quarantine name
"sent estab triagers to a table full of unrelated F1.2 lookup rows".

Job scripts under databricks/src are entry points, not part of the opl wheel, so
they are loaded by path with the same importlib pattern the other task tests use.
Nothing here starts Spark: every assertion is about wiring, not data.

TO WHOEVER HITS THE RED HERE DURING THE REGISTRY REFACTOR: these describe a
structure the refactor deliberately deletes, so some of them fail BY
CONSTRUCTION once a script takes its table from a job parameter instead of a
module constant. That is this net doing its job on schedule, not a broken test.
Rewrite each one against the registry -- feed it a table key, assert the same
resolved coordinates -- rather than deleting it. Which property each one exists
to preserve is written in its own docstring, for exactly this moment.

That already happened once, on schedule: Task 6 parameterised the two ingest
scripts, and their `EXPECTED_TABLES` entries went red exactly as predicted --
a script that reads the registry has no module constants left to enumerate. They
were rewritten below, not deleted, into the property that replaces the old one:
the coordinates must come from ONE resolved spec, so a table's staging and
quarantine cannot drift apart. The gate and promote entries still hold today's
constants; they are Task 7's turn."""
from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest

from opl.config import DEFAULT

_SRC = Path(__file__).resolve().parents[1] / "databricks" / "src"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_wiring", _SRC / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# (script, the qualified tables its module-level wiring resolves to)
#
# The two ingest scripts are deliberately absent. They used to be here
# (`bronze_ingest` -> lookup staging, `bronze_estab_ingest` -> estab staging) and
# both entries went red the moment Task 6 pointed them at the registry, because
# neither script imports a table constant any more -- there is nothing for this
# test to enumerate. What replaces it for them is the pair of tests further down:
# the source must go through `table_spec`, and every coordinate must be an
# attribute of the ONE spec it resolved.
EXPECTED_TABLES = {
    "dq_gate_batch": {
        "workspace.default.bronze_cnpj_estab_staging",
        "workspace.default.bronze_cnpj_estab_quarantine",
    },
    "promote_batch": {
        "workspace.default.bronze_cnpj_estab_staging",
        "workspace.default.bronze_cnpj_estabelecimentos",
        "workspace.default.bronze_cnpj_estab_quarantine",
    },
}


@pytest.mark.parametrize("script,expected", sorted(EXPECTED_TABLES.items()))
def test_each_task_resolves_the_tables_it_is_supposed_to(script, expected):
    """Every table name the script's source mentions, via the constants it imports.

    Asserted as a SET, not a substring search: a script that starts touching a
    second table is exactly the regression this locks against, and a substring
    check would not see it."""
    from opl.config import DEFAULT
    module = _load(script)
    names = {
        DEFAULT.table(value)
        for key, value in vars(module).items()
        if key.isupper() and isinstance(value, str) and value.startswith("bronze_cnpj_")
    }
    # promote_batch names its bronze table in a module constant; the staging and
    # quarantine arrive as imported constants, which vars() also exposes.
    assert names == expected, f"{script} resolves {names}, expected {expected}"


@pytest.mark.parametrize("script", ["bronze_ingest", "bronze_lookup_ingest"])
def test_the_parameterised_ingest_resolves_its_tables_from_the_registry(script):
    """The characterization above asserted module constants. The parameterised
    scripts have none by design -- they read the registry -- so what must hold now
    is that they go through `table_spec` rather than naming a table themselves."""
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    assert "table_spec(" in source
    assert "bronze_cnpj_" not in source, (
        f"{script}.py names a table directly; it must resolve names through "
        "the registry so a table's staging/quarantine pair cannot drift"
    )


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


def _bound_table(expr: ast.expr, scope: dict[str, ast.expr], module, where: str) -> str:
    """The qualified table an argument expression evaluates to, TODAY.

    Follows `DEFAULT.table(X)` and one or more hops through a local (`tbl`,
    `quarantine`), resolving a constant NAME against the loaded module so the
    answer is the real coordinate rather than the identifier's spelling. Any shape
    it does not understand raises instead of returning something -- an
    unrecognised call site must be a red test, never a quiet pass."""
    seen: set[str] = set()
    while isinstance(expr, ast.Name):
        assert expr.id not in seen, f"{where}: {expr.id} resolves in a cycle"
        assert expr.id in scope, (
            f"{where}: the table comes from `{expr.id}`, which main() does not assign -- "
            "this lock can no longer see which table is used"
        )
        seen.add(expr.id)
        expr = scope[expr.id]
    assert (
        isinstance(expr, ast.Call)
        and isinstance(expr.func, ast.Attribute)
        and expr.func.attr == "table"
        and isinstance(expr.func.value, ast.Name)
        and expr.func.value.id == "DEFAULT"
    ), f"{where}: expected a DEFAULT.table(...) call, got {ast.dump(expr)[:120]}"
    assert len(expr.args) == 1 and not expr.keywords, f"{where}: unexpected DEFAULT.table args"
    arg = expr.args[0]
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return DEFAULT.table(arg.value)
    assert isinstance(arg, ast.Name), f"{where}: DEFAULT.table({ast.dump(arg)[:80]})"
    value = getattr(module, arg.id, None)
    assert isinstance(value, str), f"{where}: {arg.id} is not a str constant on the module"
    return DEFAULT.table(value)


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


def test_the_estab_promote_binds_each_table_to_the_argument_it_feeds_today():
    """Which table each PARAMETER receives -- not merely which names the file mentions.

    The test above enumerates module-level constants, and a mutation probe proved
    that is not enough on its own: redirecting `staging_table=` to the quarantine
    constant and `bronze_table=` to a literal, while leaving every constant
    imported, kept it green. That mutation is a promote which reads the quarantine
    and appends into the lookup table, and Task 7 rewrites exactly these call
    sites, so the binding is the thing that has to be pinned.

    Locks bindings as they stand, NOT a post-refactor rule: `bronze_table` is fed
    a local assigned from a module constant and `staging_table` an imported one,
    and both shapes are resolved to the coordinate they produce."""
    module = _load("promote_batch")
    main = _main_of("promote_batch")
    scope = _locals_of(main, "promote_batch")
    call = _sole_call(main, "promote_batch", "promote_batch")
    bound = {
        kw.arg: _bound_table(kw.value, scope, module, f"promote_batch {kw.arg}=")
        for kw in call.keywords
        if kw.arg in {"staging_table", "bronze_table"}
    }
    assert bound == {
        "staging_table": "workspace.default.bronze_cnpj_estab_staging",
        "bronze_table": "workspace.default.bronze_cnpj_estabelecimentos",
    }


def test_the_estab_gate_reads_staging_and_writes_the_estab_quarantine_today():
    """Same binding lock on the gate: the table it READS the batch from and the
    table it WRITES rejects to must stay distinct and stay these two.

    Collapsing them is not hypothetical -- a gate that reads its batch from the
    quarantine, or writes rejects into staging, is a one-identifier edit at either
    call site, and every constant would still be imported afterwards."""
    module = _load("dq_gate_batch")
    main = _main_of("dq_gate_batch")
    scope = _locals_of(main, "dq_gate_batch")
    read = _sole_call(main, "batch_rows", "dq_gate_batch")
    assert len(read.args) >= 2, "batch_rows() no longer takes the table positionally"
    written = _sole_call(main, "saveAsTable", "dq_gate_batch")
    assert len(written.args) >= 1, "saveAsTable() no longer takes the table positionally"
    assert _bound_table(read.args[1], scope, module, "dq_gate_batch batch_rows table") == (
        "workspace.default.bronze_cnpj_estab_staging"
    )
    assert _bound_table(written.args[0], scope, module, "dq_gate_batch saveAsTable") == (
        "workspace.default.bronze_cnpj_estab_quarantine"
    )


def test_the_two_gates_scope_differently_today():
    """dq_gate is whole-table; dq_gate_batch is batch-scoped. The refactor
    collapses them onto the batch-scoped one, which is carry-forward #7."""
    whole = (_SRC / "dq_gate.py").read_text(encoding="utf-8")
    scoped = (_SRC / "dq_gate_batch.py").read_text(encoding="utf-8")
    assert "batch_rows(" not in whole
    assert "batch_rows(" in scoped


def test_the_lookup_promote_overwrites_and_the_estab_promote_appends():
    """The semantic difference the lookup migration has to resolve: an overwrite
    from the WHOLE staging table would write 2x the rows once a second batch
    exists, which moving the lookup files creates."""
    lookup = (_SRC / "promote.py").read_text(encoding="utf-8")
    estab = (_SRC / "promote_batch.py").read_text(encoding="utf-8")
    assert 'mode("overwrite")' in lookup
    assert "promote_batch(" in estab
    # Both sides of the asymmetry, not just the lookup one. `promote_batch(` alone
    # says the estab task DELEGATES to the shared helper, which is a weaker claim
    # than the name of this test makes: it would still hold if that helper started
    # overwriting. The estab bronze table is the 71.9M-row one, so an overwrite
    # from a single batch's staging rows is the destructive direction.
    assert 'mode("overwrite")' not in estab


@pytest.mark.parametrize(
    "script,rule_set",
    [("dq_gate_batch", "estabelecimentos"), ("promote_batch", "estabelecimentos")],
)
def test_each_task_uses_its_own_rule_set(script, rule_set):
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    assert f'rules_for("{rule_set}")' in source


def test_the_estab_constraints_are_the_ones_bronze_carries_today():
    source = (_SRC / "promote_batch.py").read_text(encoding="utf-8")
    assert "cnpj_basico SET NOT NULL" in source
    assert "cnpj_basico_len8" in source
    assert "length(trim(cnpj_basico)) = 8" in source


def test_the_lookup_constraints_are_the_ones_bronze_carries_today():
    source = (_SRC / "promote.py").read_text(encoding="utf-8")
    assert "codigo SET NOT NULL" in source
    assert "codigo_not_blank" in source
    assert "length(trim(codigo)) > 0" in source
