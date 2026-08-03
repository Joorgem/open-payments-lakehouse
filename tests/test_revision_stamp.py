# tests/test_revision_stamp.py
"""The build hook that puts a revision INTO the wheel -- `hatch_build.py`.

This is the half of ADR 0009 that has to exist before the guard means anything:
nothing in this artefact encoded a source revision, so the "actual" side of the
comparison had to be created. The hook generates `opl/_revision.py` into the wheel
at `uv build --wheel` time, which is what `databricks bundle deploy` runs.

THE HERMETIC TESTS BUILD THROWAWAY GIT REPOSITORIES, because what is under test is
what the hook reads out of a tree, and the only tree the suite could otherwise use
is this one -- whose state changes under the person running the tests. A test that
asked git for the answer and compared it against the hook asking git for the answer
would agree with itself in every state, including the broken ones.

The one test that builds a real wheel does so into a temp dir. It must never build
into `dist/`: that directory is deploy input, the job YAMLs reach it through
`dependencies: ["../../dist/*.whl"]`, and a test that wrote there would be a test
that changes what the next deploy uploads."""
from __future__ import annotations

import ast
import importlib.util
import shutil
import subprocess
import tomllib
import zipfile
from pathlib import Path

import pytest

from opl.bronze.provenance import WrongRevision, assert_revision_matches, is_object_name

_REPO = Path(__file__).resolve().parents[1]


def _load_hook_module():
    """`hatch_build.py`, loaded by path.

    It sits at the repo root and is not part of any package -- hatchling reads it
    through the `[tool.hatch.build.targets.wheel.hooks.custom]` entry, not through an
    import -- so the suite loads it the same way it loads the `databricks/src` entry
    points and `scripts/`: by file location, with no sys.path edit."""
    spec = importlib.util.spec_from_file_location("opl_hatch_build", _REPO / "hatch_build.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_HOOK = _load_hook_module()
CustomBuildHook = _HOOK.CustomBuildHook
STAMPED_MODULE = _HOOK.STAMPED_MODULE
WHEEL_INPUTS = _HOOK.WHEEL_INPUTS
revision_module_source = _HOOK.revision_module_source
wheel_revision = _HOOK.wheel_revision


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args], capture_output=True, text=True, check=True
    ).stdout.strip()


def _commit(root: Path, message: str) -> str:
    """A commit in a throwaway repo, in a way that cannot depend on this machine.

    Identity, signing and hooks are all forced off for the fixture: a global
    `commit.gpgsign` or a global `core.hooksPath` would otherwise decide whether
    these tests can run. `--no-verify` here is about a temp repo with no hooks worth
    running and says nothing about this project's own commits, which run theirs."""
    _git(root, "add", "-A")
    _git(
        root,
        "-c", "user.name=test",
        "-c", "user.email=test@example.invalid",
        "-c", "commit.gpgsign=false",
        "commit", "--no-verify", "-q", "-m", message,
    )
    return _git(root, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A minimal tree shaped like this one: a wheel input under `src/`, a
    `pyproject.toml`, and something outside both that a build cannot see."""
    root = tmp_path / "throwaway"
    (root / "src" / "pkg").mkdir(parents=True)
    (root / "src" / "pkg" / "__init__.py").write_text("", encoding="utf-8")
    (root / "docs").mkdir()
    (root / "docs" / "notes.md").write_text("committed\n", encoding="utf-8")
    (root / "pyproject.toml").write_text("[project]\nname = 'pkg'\n", encoding="utf-8")
    _git(root, "init", "-q")
    _commit(root, "one")
    return root


def test_a_clean_tree_stamps_the_commit_the_wheel_was_built_from(repo):
    head = _git(repo, "rev-parse", "HEAD")
    assert wheel_revision(repo) == head
    assert is_object_name(wheel_revision(repo)), (
        "the stamp is not a whole object name, so no run could ever expect it"
    )
    assert assert_revision_matches(expected=head, actual=wheel_revision(repo)) == head


def test_a_modified_wheel_input_stamps_something_no_run_can_expect(repo):
    """THE DECISION ABOUT DIRTY TREES, asserted rather than described.

    `git rev-parse HEAD` answers identically in a modified tree, so a bare SHA would
    let a wheel built from uncommitted code claim a commit that does not describe it.
    A run launched with `--params revision=$(git rev-parse HEAD)` then MATCHES, and
    the provenance claim in the log is false in the one direction nobody would think
    to check. Refusing costs a `git commit`; accepting costs the whole guard."""
    head = _git(repo, "rev-parse", "HEAD")
    (repo / "src" / "pkg" / "__init__.py").write_text("CHANGED = True\n", encoding="utf-8")
    stamped = wheel_revision(repo)

    assert stamped != head
    assert stamped.startswith(head), (
        "the dirty stamp no longer names the commit it was built from, so the refusal "
        "cannot tell an operator which commit to compare against"
    )
    assert not is_object_name(stamped)
    with pytest.raises(WrongRevision) as excinfo:
        assert_revision_matches(expected=head, actual=stamped)
    assert "uncommitted" in str(excinfo.value)


def test_an_untracked_file_under_src_counts_as_modified(repo):
    """Because it is IN the wheel. Hatchling packages `src/opl` off the filesystem,
    not out of git, so a file nobody committed ships all the same -- and a stamp that
    only watched tracked modifications would name a commit that never contained it."""
    head = _git(repo, "rev-parse", "HEAD")
    (repo / "src" / "pkg" / "extra.py").write_text("SHIPPED = True\n", encoding="utf-8")
    assert wheel_revision(repo) != head


def test_a_new_file_that_cannot_reach_the_wheel_does_not_refuse_a_run(repo):
    """THE OTHER HALF OF THE SAME DECISION, and the reason the watched set is narrow.

    Untracked docs, evidence files and phase notes cannot change the artefact, and
    this phase writes them WHILE runs are going on. A guard that refused a
    multi-hour ingestion because `docs/` had a new file in it is a guard operators
    learn to route around, which is worse than not having one."""
    head = _git(repo, "rev-parse", "HEAD")
    (repo / "docs" / "run-evidence.md").write_text("in progress\n", encoding="utf-8")
    (repo / "notes-at-the-root.md").write_text("scratch\n", encoding="utf-8")
    assert wheel_revision(repo) == head


def test_the_watched_paths_are_the_paths_the_wheel_is_actually_built_from():
    """A silent hole, closed: add a second package to the wheel and forget this list,
    and the stamp stops noticing changes to it -- a dirty build would then be stamped
    clean and would pass the guard. So the list is checked against the wheel's own
    declaration rather than trusted."""
    config = tomllib.loads((_REPO / "pyproject.toml").read_text(encoding="utf-8"))
    packages = config["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
    watched = {Path(p) for p in WHEEL_INPUTS}
    for package in packages:
        parents = set(Path(package).parents) | {Path(package)}
        assert watched & parents, (
            f"the wheel contains {package!r}, which no watched path covers: "
            f"{sorted(WHEEL_INPUTS)}. A change there would be stamped as clean"
        )
    assert Path("pyproject.toml") in watched, (
        "the wheel's metadata comes from pyproject.toml, so a change to it changes the "
        "artefact"
    )


def test_a_tree_that_is_not_a_repository_stamps_nothing_the_guard_will_accept(tmp_path):
    """Building outside a checkout is not an error the build should die on -- an sdist
    unpacked somewhere has no `.git` and must still be installable. It is a wheel
    that cannot say what it came from, and the guard refuses it, which is trap 3
    reaching the case it was written for."""
    (tmp_path / "src").mkdir()
    stamped = wheel_revision(tmp_path)
    assert stamped == ""
    with pytest.raises(WrongRevision):
        assert_revision_matches(expected="0" * 40, actual=stamped)


class _FakeHook:
    """Enough of a hook instance for `initialize`/`finalize` to run.

    The real `BuildHookInterface.__init__` takes seven positional arguments whose
    order is hatchling's business, not ours; constructing one here would pin this
    test to a version of that signature. What the hook actually uses is `self.root`."""

    def __init__(self, root: Path) -> None:
        self.root = str(root)


def _initialize(root: Path, version: str) -> dict:
    build_data: dict = {"force_include": {}}
    hook = _FakeHook(root)
    CustomBuildHook.initialize(hook, version, build_data)
    return build_data


def test_a_standard_wheel_build_force_includes_the_stamp_inside_the_package(repo):
    build_data = _initialize(repo, "standard")
    assert list(build_data["force_include"].values()) == [STAMPED_MODULE]
    generated = Path(next(iter(build_data["force_include"])))
    namespace: dict = {}
    exec(compile(generated.read_text(encoding="utf-8"), str(generated), "exec"), namespace)
    assert namespace["REVISION"] == _git(repo, "rev-parse", "HEAD")


def test_an_editable_install_is_left_unstamped_on_purpose(repo):
    """LOAD-BEARING, not tidiness, and measured before it was written (ADR 0009).

    An editable install points at a working tree that changes under it, so any
    revision stamped into one is stale from the next keystroke -- and a local
    `uv run pytest` would then satisfy a provenance check with a value nothing
    verified. Worse, hatchling force-includes into the editable wheel too, which
    creates a shadow `site-packages/opl/` directory holding only this module: the
    package still resolves to `src/opl`, so the stamp is unreachable AND there are
    now two directories competing for the name.

    `tests/bronze/test_provenance.py` asserts the consequence from the other end --
    that THIS tree reports no revision and is therefore refused."""
    assert _initialize(repo, "editable") == {"force_include": {}}


def test_the_generated_module_is_python_that_defines_exactly_the_revision():
    """It is executed by the deployed artefact, so a stamp that is not valid Python is
    an ImportError inside every guarded job rather than a refusal that explains
    itself."""
    source = revision_module_source("0" * 40)
    namespace: dict = {}
    exec(compile(source, "<stamp>", "exec"), namespace)
    assert namespace["REVISION"] == "0" * 40
    assert [name for name in namespace if not name.startswith("__")] == ["REVISION"]


def test_the_hook_is_registered_for_the_wheel_target():
    """Cheap, and it covers the failure the slow test below covers expensively: a hook
    nobody registered generates nothing, and every guarded job then refuses every run
    for want of a stamp."""
    config = tomllib.loads((_REPO / "pyproject.toml").read_text(encoding="utf-8"))
    hooks = config["tool"]["hatch"]["build"]["targets"]["wheel"]["hooks"]
    assert "custom" in hooks, "the wheel target declares no custom build hook"
    declared = hooks["custom"].get("path", "hatch_build.py")
    assert (_REPO / declared).exists()


def test_the_wheel_this_repo_builds_really_carries_the_stamp(tmp_path):
    """THE END-TO-END CLAIM, through the real `uv build --wheel` the bundle runs.

    Every other test here reads the hook directly, which cannot see whether hatchling
    is configured to call it, whether `force_include`'s destination lands inside the
    package, or whether the file survives into the archive. This one opens the wheel.

    Deliberately NOT into `dist/`: that is what the next `bundle deploy` uploads."""
    uv = shutil.which("uv")
    assert uv is not None, (
        "uv is not on PATH, so this test cannot build the artefact it exists to open. "
        "It is not skipped: every documented command in this repo starts with `uv`"
    )
    built = subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(tmp_path)],
        cwd=_REPO, capture_output=True, text=True,
    )
    if built.returncode != 0:  # pragma: no cover - a broken build is its own failure
        pytest.fail(f"uv build --wheel failed:\n{built.stdout}\n{built.stderr}")
    wheels = list(tmp_path.glob("*.whl"))
    assert len(wheels) == 1, f"expected one wheel, got {[w.name for w in wheels]}"

    with zipfile.ZipFile(wheels[0]) as archive:
        assert STAMPED_MODULE in archive.namelist(), (
            f"the built wheel has no {STAMPED_MODULE}; every guarded job would refuse "
            f"every run. Archive holds: {archive.namelist()}"
        )
        namespace: dict = {}
        exec(compile(archive.read(STAMPED_MODULE), STAMPED_MODULE, "exec"), namespace)
    assert namespace["REVISION"] == wheel_revision(_REPO)


_RUNTIME_SOURCES = sorted(
    (_REPO / "src" / "opl").rglob("*.py")
) + sorted((_REPO / "databricks" / "src").glob("*.py"))


@pytest.mark.parametrize("source", _RUNTIME_SOURCES, ids=lambda p: p.name)
def test_git_is_consulted_at_build_time_and_nowhere_the_artefact_runs(source):
    """ADR 0009's last consequence, asserted over every file that ships or runs.

    The build hook asks git, once, on the machine that builds the wheel. Nothing in
    the wheel and nothing under `databricks/src` may: there is no repository beside
    the running artefact -- `bundle deploy` syncs files, not a `.git` -- so a runtime
    call either crashes or, worse, gets an answer from the operator's own repository
    and compares it against itself.

    Over the AST rather than the text, and that is not fastidiousness: the refusal
    message in `opl.bronze.provenance` has to print the exact launch command, which
    contains the very words a substring check would ban. What is banned is the ABILITY
    to shell out, not the ability to explain."""
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=source.name)
    for node in ast.walk(tree):
        module = None
        if isinstance(node, ast.Import):
            shelling_out = (a.name for a in node.names if a.name.split(".")[0] == "subprocess")
            module = next(shelling_out, None)
        elif isinstance(node, ast.ImportFrom) and node.module:
            module = node.module if node.module.split(".")[0] == "subprocess" else None
        assert module is None, (
            f"{source.name} imports {module!r}. The revision must be read from what the "
            "artefact CARRIES, not asked of a repository at run time"
        )
        if isinstance(node, ast.Attribute):
            assert node.attr not in {"system", "popen", "Popen", "check_output"}, (
                f"{source.name} calls {node.attr} -- the same hole through another door"
            )
