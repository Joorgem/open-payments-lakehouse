# tests/loader_scripts.py
"""How a vault entry point under `databricks/src` is FOUND and READ. No test lives
here, and that absence is the point -- pytest collects nothing from a module matching
no `python_files` pattern.

ONE OF THIS SUITE'S PLAIN READER MODULES, beside `tests/job_yaml.py`,
`tests/task_ast.py` and `tests/vault_job_demands.py` -- AND IT EXISTS FOR A REASON THOSE
DO NOT SHARE. Their argument is anti-duplication: "two copies of `sole_call` is two
copies of the assertion that makes a lock a lock, and the copy that goes stale is the one
whose failure message nobody has read in a year." NOTHING HERE IS DUPLICATED. Every
function below has exactly one consumer, `tests/test_vault_entry_points.py`, and each
would be perfectly correct sitting inside it. The reason is the next paragraph's alone:
that file hit the line cap. Anti-duplication is why `main_of` and `locals_of` are
imported from `task_ast` rather than rewritten here; it is not why this file exists. No
ordinal is written on purpose: two more such modules are in flight on this branch, so
"the fourth" would be false on the merged tree without anybody editing the line.

WHAT FORCED IT, MEASURED. `tests/test_vault_entry_points.py` reached 850 lines against
this project's strictly-under-800 cap while F2 wave 2's correction was closing two locks
that could not fail. Master protocol section 4.12 says whoever touches a file at the cap
splits it first. The seam is the one that file's own docstring already drew -- what makes
a lock CHANGE decides which side it lands on -- read one level down: the READERS here
change when a script's shape changes, and the tests there change when a lock's claim
changes. Nothing was deleted and no assertion was weakened; the functions below are the
ones that file carried. FIVE OF THE TEN KEPT THEIR NAME MINUS THE LEADING UNDERSCORE
(`load`, `non_docstring_strings`, `parent_resolver_of`, `resolution_and_session_lines`,
`loader_call_in_main`); the other five were RENAMED -- `_tree` to `tree_of`,
`_loader_scripts` to `all_scripts`, `_scripts_exposing_the_seam` to `exposing_the_seam`,
`_the_one_script_accepting` to `the_one_accepting`, `_kind_a_script_accepts` to
`kind_accepted_by` -- because a private spelling reads wrong on a module whose whole
surface is public. The importing file aliases all ten back to the names its tests already
used, so no CALL SITE moved; a PROSE reference to an old name is a different matter, and
this split left two of those dangling until a review found them.

AND THE `importlib` LOADER IS HERE, WHICH REVERSES `task_ast.py`'S JUDGEMENT FOR THIS
FILE ALONE AND NOT FOR EVERY OTHER MODULE CARRYING THAT IDIOM. That module declined to
hoist `_load` because "neither half of this split executes a job script" and because
hoisting a twelve-way duplicated idiom is a change to twelve files rather than a
consequence of one split. THE "TWELVE" IS F-DB'S FIGURE AND IS NOT RE-ASSERTED HERE: a
re-derivation over `tests/` today finds 37 modules referencing `spec_from_file_location`
and no population of them that comes to twelve, and the sub-counts move with how "wraps
it in a helper" is spelled -- so no replacement number is written either. What survives
is the ARGUMENT, which never turned on the count: hoisting an idiom out of every module
that carries it is a change to all of them, and this split is not that.
Both halves of THIS split execute one: `kind_accepted_by` resolves a class in the
script's own namespace and `exposing_the_seam` asks a module whether it has an
attribute. So the loader travels with the readers built on it, and the twelve other
modules keep their private copies untouched -- the count is unchanged, not grown.

`main_of` AND `locals_of` ARE IMPORTED FROM `task_ast` AND NOT REWRITTEN. `locals_of`
already refuses a `main` that binds one local name twice, with the argument for why a
lock resolving a name to a single assignment cannot be handed two; that refusal is
exactly what stops a task from computing the parent arguments and then rebinding over
them, so re-deriving it here would have been the second copy that rots."""
from __future__ import annotations

import ast
import importlib.util
from functools import cache
from pathlib import Path

from task_ast import locals_of, main_of

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "databricks" / "src"


def load(name: str):
    """The script, executed as a module, so a reader can ask it about its own names."""
    spec = importlib.util.spec_from_file_location(f"{name}_task", SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def tree_of(script: str) -> ast.Module:
    return ast.parse((SRC / f"{script}.py").read_text(encoding="utf-8"), filename=script)


def non_docstring_strings(tree: ast.Module) -> list[str]:
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


@cache
def all_scripts() -> tuple[str, ...]:
    """Every vault entry point, from the DIRECTORY rather than from a written list."""
    return tuple(sorted(path.stem for path in SRC.glob("vault_load_*.py")))


@cache
def exposing_the_seam() -> tuple[str, ...]:
    """Every loader script exposing a `parent_arguments` seam, over the glob -- ONE
    SPELLING for the pairing sweep's skip, the guard under it and the `main`-binding
    lock, so none of the three answers about a different population."""
    return tuple(script for script in all_scripts() if hasattr(load(script), "parent_arguments"))


@cache
def kind_accepted_by(script: str) -> type:
    """The spec class this script's `required_spec` call names, resolved in the script's
    own namespace so the reader cannot disagree with the script about which class it is.

    The assertions are about this READER and not about the script: each says the shape it
    could not parse and tells the next author to re-derive rather than to trust a sweep
    that quietly matched nothing."""
    calls = [
        node
        for node in ast.walk(tree_of(script))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "required_spec"
    ]
    assert len(calls) == 1, (
        f"{script}.py calls required_spec {len(calls)} times, so 'the kind this entry "
        "point accepts' is no longer one answer. Re-read the script before trusting the "
        "sweeps that use this."
    )
    call = calls[0]
    named = (
        call.args[1] if len(call.args) > 1
        else next((kw.value for kw in call.keywords if kw.arg == "kind"), None)
    )
    assert isinstance(named, ast.Name), (
        f"{script}.py does not name its spec kind as a bare class name, so this reader "
        "cannot resolve it. Re-derive this helper rather than narrowing the sweep."
    )
    kind = getattr(load(script), named.id, None)
    assert isinstance(kind, type), (
        f"{script}.py names spec kind {named.id!r}, which is not a class in its own "
        "namespace"
    )
    return kind


@cache
def parent_resolver_of(script: str) -> str:
    """The one `domains.parent_*` this script resolves a parent with, or `''` for a
    script that resolves none (the hub, link and reference loaders take the spec itself).

    Read off the AST rather than off the module, because what is being asked is which
    resolver this script CALLS -- a name imported and never used would answer wrongly."""
    names = sorted(
        {
            node.func.attr
            for node in ast.walk(tree_of(script))
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "domains"
            and node.func.attr.startswith("parent_")
        }
    )
    assert len(names) < 2, (
        f"{script}.py resolves a parent through {names}, so which one routes a table is "
        "no longer one answer. Re-read the script before trusting the sweep that uses it."
    )
    return names[0] if names else ""


def the_one_accepting(spec) -> str:
    """The single entry point whose `required_spec` kind admits `spec`."""
    accepting = [script for script in all_scripts() if isinstance(spec, kind_accepted_by(script))]
    assert len(accepting) == 1, (
        f"vault table {spec.name!r} is a {type(spec).__name__}, and {len(accepting)} "
        f"entry points under databricks/src accept that kind: {accepting}. Zero means a "
        "registered table no job can run at all; two means a copied YAML can pick the "
        "wrong one and neither will refuse it."
    )
    return accepting[0]


def resolution_and_session_lines(script: str) -> tuple[tuple[int, ...], ...]:
    """Where `main` works out what its satellite keys on -- a `domains.parent_*` call or
    its own `parent_arguments` seam -- and where it takes a Spark session.

    SCOPED TO `main` AND NOT TO THE MODULE, which is the choice `task_ast.main_of` exists
    to make: a helper defined below `main` that resolved a parent would redden the
    ordering lock on a line no job reaches. THAT IS THE WHOLE OF THE ARGUMENT, and the
    second half this once carried is struck as false: a `getOrCreate` in a helper ABOVE
    `main` would NOT hide an inversion behind a smaller number, because at module scope it
    yields two session lines and `len(sessions) == 1` fires first, loudly. The risk is
    one-directional. It is not live today, and it would not look like a mistake."""
    main = main_of(script)
    calls = [node for node in ast.walk(main) if isinstance(node, ast.Call)]
    attributes = [node for node in calls if isinstance(node.func, ast.Attribute)]
    resolutions = [
        node.lineno for node in attributes
        if isinstance(node.func.value, ast.Name)
        and node.func.value.id == "domains" and node.func.attr.startswith("parent_")
    ] + [
        node.lineno for node in calls
        if isinstance(node.func, ast.Name) and node.func.id == "parent_arguments"
    ]
    sessions = [node.lineno for node in attributes if node.func.attr == "getOrCreate"]
    return tuple(sorted(resolutions)), tuple(sorted(sessions))


def loader_call_in_main(script: str) -> tuple[ast.Call, dict[str, ast.expr]]:
    """The one call in `main` that is handed the Spark session -- THE LOADER, DERIVED
    RATHER THAN NAMED -- together with every name `main` binds.

    Spelling `load_satellite` here would be a hand-written population of one, in a suite
    whose own rule is that populations are derived and not listed, and it would pass
    unchanged over a script repointed at some other function. The session is the derived
    handle, and it is the SAME fact the ordering lock above turns on, read from the other
    side: the parent is resolved BEFORE the session, and what the call that TAKES the
    session is handed must be that resolution's answer.

    `locals_of` supplies the bindings, and it sees ASSIGNMENTS ONLY -- `ast.Assign` with
    a bare-name target. It does not see `|=`, item assignment or `.update()`, so a name
    rebound is caught while the dict MUTATED IN PLACE is invisible to it and to every
    lock built on it. What that costs is declared beside the lock that uses this, in
    `tests/test_vault_entry_points.py`, and it is declared there rather than fixed here
    deliberately. Both assertions below are about this reader, and both say re-derive."""
    main = main_of(script)
    bound = locals_of(main, script)
    sessions = [
        name for name, value in bound.items()
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute)
        and value.func.attr == "getOrCreate"
    ]
    assert len(sessions) == 1, (
        f"{script}.py main() binds {len(sessions)} Spark sessions, so which of its calls "
        "is the loader is not one answer. Re-read the script before trusting this lock."
    )
    taking = [
        node for node in ast.walk(main) if isinstance(node, ast.Call)
        and any(
            isinstance(given, ast.Name) and given.id == sessions[0]
            for given in [*node.args, *(word.value for word in node.keywords)]
        )
    ]
    assert len(taking) == 1, (
        f"{script}.py main() hands its session to {len(taking)} calls, so which one is "
        "its loader is not readable from here. Re-derive this helper."
    )
    return taking[0], bound
