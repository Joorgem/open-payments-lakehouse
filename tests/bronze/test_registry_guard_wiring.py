"""The wiring lock: every registry guard is CALLED at import, wherever it is DEFINED.

SPLIT OUT OF `test_registry_guards.py`, which is the file it certifies. The seam is
the third one this group of files has needed, and it follows the same rule as the
first two: `test_registry.py` changes when a TABLE is added, `test_registry_guards.py`
changes when a GUARD changes, and this file changes when the registry's MODULE LAYOUT
changes. Those are three reasons to edit, so they are three files -- and the layout is
the axis F2 is about to move along, with `test_registry_guards.py` at 700 lines of an
800 cap and a new guard's tests still to land in it.

WHY THE LOCK EXISTS AT ALL: every refusal test in `test_registry_guards.py` invokes a
guard DIRECTLY, so it proves the function refuses and says nothing about whether
anything ever runs it. Found by mutation probe while the two paste guards were being
written -- commenting out all three of their calls at the foot of registry.py left the
registry suite, then 33 tests, 33/33 GREEN.

WHY IT HAD TO BE TAUGHT A SECOND MODULE: `defined` was scoped to `FunctionDef` nodes in
`registry.py`'s own file. A guard extracted to a sibling module therefore dropped out of
BOTH `defined` and `called`, so `defined - called` stayed empty and this lock kept
passing while certifying nothing about the moved guard -- the exact vacuity it exists to
remove, turned on itself, and measured before the extraction rather than after.
`test_the_lock_is_not_blind_to_a_guard_in_a_second_module` is the proof that it is
closed. That proof is SYNTHETIC on purpose: the real modules are correctly wired, which
is precisely why the blind version passed over them."""
from __future__ import annotations

import ast
from pathlib import Path

from opl.bronze import registry

# The module every consumer imports, and therefore the one whose body is what "at
# import" MEANS. See `_guard_wiring` for why calls are read from this file alone while
# definitions are read from all of them.
_PRIMARY = "registry.py"

# Which files may define a registry guard. A GLOB over the package directory rather than
# a list of module names, so the next split is covered before it is written: a lock that
# has to be edited to see a new module is a lock that gets forgotten, and being forgotten
# is how this one came to certify nothing. `registry*.py` rather than `*.py` because the
# naming convention is the contract -- an `_assert_*` at module level in a REGISTRY
# module is a guard -- and `opl.bronze` is full of modules for which that is not true.
_GUARD_MODULES = "registry*.py"


def _registry_guard_sources() -> dict[str, str]:
    """Every registry module's source, keyed by filename.

    `utf-8-sig`, not `utf-8`: Python's own tokenizer strips a leading BOM, so a BOM'd
    registry.py imports fine and only THIS read would choke on it -- turning a wiring
    regression into a SyntaxError from a test that never mentions encodings. Not
    hypothetical; a Windows editor put one there while the first version of this test
    was being written."""
    package = Path(registry.__file__).parent
    return {
        path.name: path.read_text(encoding="utf-8-sig")
        for path in sorted(package.glob(_GUARD_MODULES))
    }


def _guard_wiring(sources: dict[str, str]) -> tuple[dict[str, set[str]], set[str]]:
    """The `_assert_*` guards each module DEFINES, and the names `registry.py` CALLS
    where its module body runs.

    Takes the sources rather than reading them, so the lock can be exercised against a
    synthetic second module -- which is the only way to prove it is not blind, since the
    real ones are wired correctly. Read off the AST rather than the module objects,
    because that is the only place the difference is visible: a guard that is defined and
    never called is a perfectly ordinary function attribute at runtime, indistinguishable
    from a wired one.

    DEFINITIONS SPAN EVERY MODULE; CALLS ARE `registry.py`'s BODY ALONE, and the
    asymmetry is a decision rather than an oversight. registry.py is the module every
    consumer imports, and the call block at its foot carries ordering comments that are
    load-bearing -- shape before reserved-name, individually-wrong before
    collectively-wrong -- so guard calls scattered across the modules that define them
    would make that order invisible and unreviewable. It also removes a hole for free: a
    name CALLED in registry.py's body must be bound there, so the module defining it was
    imported, and there is no way for a guard module to look wired without being run. A
    guard called from its own module's body reads as unwired here, deliberately, and the
    failure message names where the call belongs.

    Keyed by filename rather than flattened, so the guard-the-guard below can insist that
    every module matching the glob actually contributed a guard."""
    defined = {
        filename: {
            node.name
            for node in ast.parse(source).body
            if isinstance(node, ast.FunctionDef) and node.name.startswith("_assert_")
        }
        for filename, source in sources.items()
    }
    return defined, _names_called_where_the_module_body_runs(ast.parse(sources[_PRIMARY]))


def _called_name(func: ast.expr) -> str:
    """What a call site spells: `f(...)` and `module.f(...)` alike, or "" for neither.

    THE ATTRIBUTE HALF IS THE SECOND TRAP IN THE EXTRACTION, and it is the mirror of the
    first. This matched `ast.Name` only, so a registry.py that called a moved guard as
    `registry_collisions._assert_no_two_tables_share_a_delta_name(REGISTRY)` -- an
    `ast.Attribute` -- would have had that call go unseen, and the lock would have
    reported a correctly wired guard as UNWIRED. A wiring test whose diagnosis can be
    wrong about working code gets working code "fixed", which is the same defect as a
    lock that passes blind, pointed the other way.

    registry.py imports the guards by name today, so only the `ast.Name` half is
    exercised by the live modules. Matching both is what stops the lock from silently
    depending on that import style staying chosen."""
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _names_called_where_the_module_body_runs(tree: ast.Module) -> set[str]:
    """Every name called from code that executes on import.

    Descends into module-level `if`/`try`/`with` bodies but NOT into function, class or
    lambda bodies -- a call inside a `def` runs when that def is called, which is the
    whole distinction this file is trying to draw.

    Broader than matching a bare top-level call statement, which is what this did first.
    That version reported a guard wired as `if True: _assert_x()` as UNWIRED, telling a
    contributor to add a call that is already there and already running.

    The residual limit, stated because it is real: a guard called under a module-level
    `if` that is FALSE at import counts as wired here. Accepted -- the alternative is
    evaluating module-level conditions statically, and no registry module has ever had a
    conditional guard call. What this rules out is the case that actually happens: the
    call deleted, or a new guard never wired at all."""
    called: set[str] = set()

    def walk(node: ast.AST) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(
                child,
                ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.Lambda,
            ):
                continue
            if isinstance(child, ast.Call):
                called.add(_called_name(child.func))
            walk(child)

    walk(tree)
    return called


def _unwired(sources: dict[str, str]) -> set[str]:
    """The guards `sources` define that registry.py never calls: what the lock reports."""
    defined_by_module, called = _guard_wiring(sources)
    return set().union(*defined_by_module.values()) - called


def test_every_guard_any_registry_module_defines_is_actually_called_at_import():
    """What makes every refusal test in `test_registry_guards.py` mean something.

    A guard that is defined and unwired is worth exactly as much as no guard, and the
    entire point of these refusals is that they happen AT IMPORT and not only in CI --
    a CI test protects a merge, not the ad-hoc run of a branch whose tests have not been
    run, which is how these jobs get launched while a phase is in flight. Tests named
    `..._at_import` that pass with the import wiring deleted are a vacuity an F1.4a
    implementer already found once in this repo.

    Structural rather than per-guard, so it does not have to be remembered: every phase
    after this may add guards, and each new one is covered the moment it is written, in
    whichever registry module it is written in. The naming convention is the contract --
    an `_assert_*` at module level in a registry module is a guard, and a guard is called
    at import from registry.py. A helper that is not meant to run at import must not be
    named `_assert_*` (see `_malformed_subdir_reason`, `_reserved_subdirs`,
    `_file_group_prefixes`, `_delta_name_collision`).

    Does NOT pin the ORDER of the calls, which is load-bearing for two of them and
    documented in comments there. Wiring and ordering are separate properties; this
    closes the first."""
    sources = _registry_guard_sources()
    defined_by_module, _ = _guard_wiring(sources)
    # Guard the guard: a walk that found nothing satisfies `defined <= called`
    # vacuously. Named guards rather than a count -- a count says "the AST walk is
    # wrong" at the phase that legitimately retires one, which is a false accusation
    # pointed at the wrong file.
    primary = defined_by_module[_PRIMARY]
    assert {"_assert_contracts_exist", "_assert_landing_modes_known"} <= primary, (
        f"the AST walk found {sorted(primary)} in {_PRIMARY}, missing guards it certainly "
        "defines -- the walk is broken, not the module"
    )
    # The same protection for the OTHER modules, which cannot be named in advance: one
    # that matches the glob and contributes no guard is either a broken walk or a file
    # that should not have matched, and either way this lock is not covering it.
    barren = sorted(name for name, guards in defined_by_module.items() if not guards)
    assert not barren, (
        f"{barren} match {_GUARD_MODULES!r} but the walk found no guard in them, so this "
        "lock is certifying nothing about those files -- fix the walk, or stop matching "
        "modules that hold no guard"
    )
    unwired = _unwired(sources)
    assert not unwired, (
        f"the registry modules define {sorted(unwired)} but {_PRIMARY} never calls them "
        "at module import, so they refuse nothing -- a direct-call test of such a guard "
        f"passes while the registry it guards is unprotected. Add the call to {_PRIMARY}'s "
        "guard block, in the ordered position its comment explains, and import the name "
        "there if it lives in another module."
    )


_MOVED_GUARD_MODULE = "def _assert_moved(registry):\n    raise ValueError('nope')\n"


def test_the_lock_is_not_blind_to_a_guard_in_a_second_module():
    """GUARDS THE GUARD, against the one failure this lock cannot report itself.

    Measured before the extraction that provoked it: with `defined` scoped to
    registry.py's own file, a guard moved to a sibling module fell out of BOTH sides of
    `defined - called`, so the difference stayed empty and the test above stayed green
    while the moved guard was covered by nothing. A lock that passes blind is worse than
    one that breaks, because it is read as evidence.

    SYNTHETIC sources, not the real files, and that is the point: the real modules are
    wired correctly, so running the lock over them cannot tell a lock that sees them from
    one that sees nothing.

    Three cases, because two of them are the traps this extraction had to walk between.
    The first fixes the blindness. The second is its inverse -- a lock that reported the
    moved guard as unwired when it is called by name would be a false accusation, and
    working code gets "fixed" when a test accuses it. The third is the attribute call
    site: `module.guard(...)` is an `ast.Attribute`, invisible to a walk that matches
    `ast.Name` only, and matching it is what keeps the lock correct whichever import
    style registry.py uses."""
    moved = {
        "registry.py": "from opl.bronze.registry_collisions import _assert_moved\n",
        "registry_collisions.py": _MOVED_GUARD_MODULE,
    }
    defined_by_module, _ = _guard_wiring(moved)
    assert defined_by_module["registry_collisions.py"] == {"_assert_moved"}, (
        "a guard in a second module is invisible to the lock -- this is the blindness "
        "the extraction would have walked into"
    )
    assert _unwired(moved) == {"_assert_moved"}, (
        "a guard moved out of registry.py and never re-called must be REPORTED unwired"
    )

    called_by_name = dict(moved, **{
        "registry.py": "from opl.bronze.registry_collisions import _assert_moved\n"
                       "_assert_moved(REGISTRY)\n",
    })
    assert not _unwired(called_by_name), (
        "a guard that IS called by name must not be reported unwired"
    )

    called_by_attribute = dict(moved, **{
        "registry.py": "from opl.bronze import registry_collisions\n"
                       "registry_collisions._assert_moved(REGISTRY)\n",
    })
    assert not _unwired(called_by_attribute), (
        "`module.guard(...)` is an ast.Attribute, and a walk that matches ast.Name only "
        "reads it as no call at all -- so the lock would accuse a wired guard"
    )
