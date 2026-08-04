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
import os
import shutil
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

import pytest

from opl.bronze.provenance import (
    STAMP_MODULE,
    WrongRevision,
    assert_revision_matches,
    is_object_name,
)

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
DEPLOYMENT_INPUTS = _HOOK.DEPLOYMENT_INPUTS
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
    """A minimal tree shaped like this one: a wheel input under `src/`, a job entry
    point under `databricks/src/` that a deploy SYNCS rather than packages, a
    `pyproject.toml`, the hook itself, and something outside all of them that no
    deployment can reach."""
    root = tmp_path / "throwaway"
    (root / "src" / "pkg").mkdir(parents=True)
    (root / "src" / "pkg" / "__init__.py").write_text("", encoding="utf-8")
    (root / "databricks" / "src").mkdir(parents=True)
    (root / "databricks" / "src" / "task.py").write_text(
        "def main():\n    pass\n", encoding="utf-8"
    )
    (root / "docs").mkdir()
    (root / "docs" / "notes.md").write_text("committed\n", encoding="utf-8")
    (root / "pyproject.toml").write_text("[project]\nname = 'pkg'\n", encoding="utf-8")
    (root / "hatch_build.py").write_text("# stand-in for the real hook\n", encoding="utf-8")
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


def test_a_modified_deployment_input_stamps_something_no_run_can_expect(repo):
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


def _covers(watched: set[Path], path: str) -> bool:
    """Is `path` inside something the stamp watches?"""
    candidate = Path(path)
    return bool(watched & (set(candidate.parents) | {candidate}))


def test_the_watched_paths_cover_everything_a_deploy_puts_in_the_workspace():
    """A silent hole this test ITSELF used to certify as closed.

    Its first version derived the expected set from `[tool.hatch...packages]` alone --
    it validated the watched paths against the WHEEL and never against the
    DEPLOYMENT. So `databricks/`, which `bundle deploy` syncs into the workspace and
    which every job's `python_file` is read from, was unwatched and green: an
    uncommitted edit to a job entry point shipped under a bare stamp equal to HEAD,
    the guard passed, and the run executed code that was never committed.

    So the requirement is derived from FOUR sources of truth, each owned elsewhere:
    the wheel's packages, the bundle's sync root (the directory holding
    `databricks.yml`), the wheel's metadata file, and the hook's own registered path.
    Add a fifth thing to the deployment and this test is what should go red."""
    config = tomllib.loads((_REPO / "pyproject.toml").read_text(encoding="utf-8"))
    wheel = config["tool"]["hatch"]["build"]["targets"]["wheel"]
    watched = {Path(p) for p in DEPLOYMENT_INPUTS}

    bundles = [path for path in _REPO.rglob("databricks.yml") if ".venv" not in path.parts]
    assert len(bundles) == 1, f"expected one bundle definition, found {bundles}"
    required = {
        "the wheel's package": wheel["packages"],
        "the bundle's sync root": [str(bundles[0].parent.relative_to(_REPO).as_posix())],
        "the wheel's metadata": ["pyproject.toml"],
        # The stamp's own logic: this file decides what the revision SAYS, including
        # whether the dirty check runs at all, so an uncommitted edit to it would
        # otherwise produce a clean stamp for a commit that does not contain it.
        "the build hook": [wheel["hooks"]["custom"].get("path", "hatch_build.py")],
    }
    for what, paths in required.items():
        for path in paths:
            assert _covers(watched, path), (
                f"{path!r} reaches the workspace as {what}, and no watched path covers "
                f"it: {sorted(DEPLOYMENT_INPUTS)}. An uncommitted change there would be "
                "stamped as clean, and the guard would pass a run built from it"
            )


@pytest.mark.parametrize(
    ("path", "why"),
    [
        (
            "databricks/src/task.py",
            "a job entry point: not packaged, SYNCED into the workspace by the same "
            "deploy, and read from there by every task's python_file",
        ),
        (
            "hatch_build.py",
            "the stamp's own logic, which decides what the revision says and whether "
            "the dirty check runs at all",
        ),
    ],
)
def test_an_uncommitted_deployment_input_stamps_dirty_even_outside_the_wheel(repo, path, why):
    """IMPORTANT 1 AND 2, and the reason they were one defect: the watched set was
    reasoned from what the WHEEL contains, so both of these were outside it.

    Neither file is packaged, and nothing re-reads either at run time -- so the
    `+dirty` stamp at BUILD time is the only thing that can refuse a run whose entry
    points or whose stamping logic were never committed. Before the fix, editing either
    produced a bare stamp equal to HEAD and the guard passed."""
    head = _git(repo, "rev-parse", "HEAD")
    (repo / path).write_text("# an uncommitted edit\n", encoding="utf-8")
    stamped = wheel_revision(repo)
    assert stamped != head, (
        f"{path} was stamped as a clean HEAD, and it is {why}"
    )
    with pytest.raises(WrongRevision):
        assert_revision_matches(expected=head, actual=stamped)


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


def test_the_stamp_lands_at_the_path_the_runtime_imports_it_from():
    """The contract between the two halves, and a probe is why it is a test.

    Pointing `STAMPED_MODULE` at the wheel ROOT instead of inside the package leaves
    `opl._revision` unimportable -- i.e. every guarded job refuses every run -- and
    every test in this file stayed GREEN through that mutation, because each of them
    named the same constant the hook did. Self-consistent is not correct. So the
    destination is pinned against the import it exists to satisfy, which
    `opl.bronze.provenance` owns."""
    assert STAMPED_MODULE == f"{STAMP_MODULE.replace('.', '/')}.py", (
        f"the hook force-includes {STAMPED_MODULE!r}, but the runtime imports "
        f"{STAMP_MODULE!r}. A stamp at any other path is a wheel that cannot be asked "
        "what it was built from"
    )


@pytest.mark.slow
def test_the_wheel_this_repo_builds_can_be_asked_what_it_was_built_from(tmp_path):
    """THE END-TO-END CLAIM, through the real `uv build --wheel` that the bundle runs
    and then through the IMPORT the deployed job actually performs.

    MARKED `slow`, AND STILL SELECTED BY DEFAULT (see `addopts` in pyproject.toml).
    The marker is a diagnostic, not a deselection: this test's runtime is a real `uv
    build --wheel`, which on a cold uv cache resolves and downloads before it builds,
    so it is the one test here that can fail a CI job by timing out while the
    behaviour it checks is fine. The marker is what lets that failure be read as
    environment rather than as a regression in the stamp.

    Every other test here reads the hook directly, which cannot see whether hatchling
    is configured to call it, whether the destination lands inside the package, or
    whether the file survives into the archive. And opening the archive is not enough
    either, per the probe above -- so the wheel is unpacked and a SEPARATE interpreter
    is asked the same question `assert_deployed_revision` asks, with the unpacked tree
    ahead of this editable checkout on its path. Nothing but the standard library is
    needed for that import, which is why no install and no network is.

    Deliberately NOT built into `dist/`: that is what the next `bundle deploy`
    uploads, and a test that wrote there would change what gets deployed."""
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

    unpacked = tmp_path / "unpacked"
    with zipfile.ZipFile(wheels[0]) as archive:
        archive.extractall(unpacked)
    asked = subprocess.run(
        [
            sys.executable, "-c",
            "import opl;from opl.bronze.provenance import built_revision;"
            "print(built_revision());print(opl.__file__)",
        ],
        cwd=tmp_path,  # never the repo: `src/` must not be reachable as a sibling
        env={**os.environ, "PYTHONPATH": str(unpacked)},
        capture_output=True, text=True,
    )
    assert asked.returncode == 0, (
        f"the built wheel could not be asked for its revision: {asked.stderr}"
    )
    revision, loaded_from = asked.stdout.splitlines()[:2]
    assert unpacked.name in loaded_from, (
        f"the probe imported opl from {loaded_from}, not from the unpacked wheel, so it "
        "measured this checkout instead of the artefact"
    )
    assert revision == wheel_revision(_REPO), (
        f"the built wheel reports {revision!r} as its revision; this tree's is "
        f"{wheel_revision(_REPO)!r}"
    )


# EVERY FILE THAT SHIPS OR RUNS, which is what the test below claims and what this
# list has to actually be. The wheel (`src/opl`) and the job entry points
# (`databricks/src`) were the first two; `scripts/backfill_snapshot_columns.py` is the
# third and was missed, because it lives under `scripts/` beside genuinely local tools
# (`extract_cnpj.py`, `migrate_lookups_to_subdir.py`) that run on the extraction host
# and may shell out freely. Its own docstring says it runs ON Databricks as a
# `spark_python_task` -- so it is in the same position as `databricks/src`: no `.git`
# beside it, and a `git rev-parse` there either crashes or answers from the operator's
# own repository. Named one by one rather than globbed off `scripts/`, so adding a
# local-only tool does not silently widen the ban and adding a second deployed script
# is a deliberate line here.
_DEPLOYED_SCRIPTS = ("backfill_snapshot_columns.py",)

_RUNTIME_SOURCES = (
    sorted((_REPO / "src" / "opl").rglob("*.py"))
    + sorted((_REPO / "databricks" / "src").glob("*.py"))
    + [_REPO / "scripts" / name for name in _DEPLOYED_SCRIPTS]
)


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
