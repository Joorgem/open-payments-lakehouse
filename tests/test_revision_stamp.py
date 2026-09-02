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
import yaml
from job_yaml import BUNDLE, bundle_files

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


# WHAT MAKES A FILE A BUNDLE ROOT, AND IT IS NOT WHAT THE FILE IS CALLED. The derivation
# below used to be `_REPO.rglob("databricks.yml")` with the answer asserted to be one
# file. Measured against the CLI (v1.8.0) on scratch bundles, that is three spellings
# short of the question: `databricks.yml`, `databricks.yaml`, `bundle.yml` and
# `bundle.yaml` are each located and validated `exit=0` and render the job they declare,
# while `foo.yml`, `databricks.json` and `bundle.json` are refused with `unable to locate
# bundle root: databricks.yml not found` -- THE CLI'S OWN ERROR NAMES ONE SPELLING WHILE
# IT ACCEPTS FOUR. So a second bundle spelled `bundle.yml` left that glob returning one
# file and the count assertion green, with the second bundle's directory unwatched by the
# dirty check -- the one thing that stamps the wheel `+dirty` and makes every guarded job
# refuse a run built from uncommitted code.
#
# WIDENING THE GLOB TO THE FOUR NAMES WAS REFUSED, and not on taste: it commits a
# four-member list of filenames derived from the behaviour of a CLI that names one of
# them, and it goes stale the day a fifth is accepted, silently and in the direction of a
# false green. What the CLI REQUIRES of a root is derivable instead, and that is what is
# asked here. A root with no bundle NAME is refused -- `unable to define default
# workspace root: bundle name not defined`, measured alike for a `databricks.yml`
# declaring no `bundle:` key, for one whose `bundle:` mapping carries no `name`, and for
# an empty file. The name is what has to be there; the filename is what the CLI is free
# to change.
#
# THE QUESTION IS ASKED AT EVERY DEPTH, which is measured rather than defensive: the CLI
# takes the same name from `targets.<t>.bundle.name` and from
# `environments.<t>.bundle.name`, both `exit=0`. Enumerating the paths a name may arrive
# by is the same mistake as enumerating the filenames one level up, so every mapping is
# asked instead of a counted list of them.
_BUNDLE = "bundle"
_BUNDLE_NAME = "name"


def _declares_a_named_bundle(node) -> bool:
    """Whether `node` carries a `bundle:` mapping with a `name`, at any depth."""
    if isinstance(node, dict):
        declared = node.get(_BUNDLE)
        if isinstance(declared, dict) and _BUNDLE_NAME in declared:
            return True
        return any(_declares_a_named_bundle(value) for value in node.values())
    if isinstance(node, list):
        return any(_declares_a_named_bundle(item) for item in node)
    return False


def _ignored(root: Path, paths: list[Path]) -> set[str]:
    """Which of `paths` git's own ignore rules exclude, as posix paths relative to `root`.

    NEITHER TIDINESS NOR SPEED. What is being defended is
    `git status --porcelain -- DEPLOYMENT_INPUTS`, and that command cannot report an
    ignored path at all -- so a bundle root under `.venv/`, `dist/` or `data/` could
    not make the stamp dirty whatever that tuple held, and demanding its directory be
    watched would be a red no edit to the tuple can clear. Asked of git rather than
    spelled out here, because a written-down list of directories to skip goes stale in
    the worst available direction: a LOCAL red, on a box that has run a deploy or holds
    working notes, that CI never reproduces.

    NUL-SEPARATED AND IN BYTES, WHICH IS NOT FASTIDIOUSNESS -- the first version of this
    was `text=True` with newline-separated paths, and a bundle root planted under a
    git-ignored directory went through it untouched. Two things went wrong at once and
    each alone was enough: text mode translates the `\\n` it writes into `\\r\\n` on
    Windows, so git received paths ending in a carriage return and treated it as part of
    the filename; and git then quoted those unusual names on the way back, so every string
    returned carried a `"` at each end. The set never matched anything and the filter
    silently did nothing, which nothing but a plant in an ignored directory would show.

    `check-ignore` exits 1 when nothing matches, so only a third code is a failure."""
    if not paths:
        return set()
    done = subprocess.run(
        ["git", "-C", str(root), "check-ignore", "-z", "--stdin"],
        input=b"".join(
            path.relative_to(root).as_posix().encode("utf-8") + b"\0" for path in paths
        ),
        capture_output=True,
    )
    assert done.returncode in (0, 1), (
        f"git check-ignore could not answer for {root}: "
        f"{done.stderr.decode('utf-8', 'replace').strip()!r}. This derivation cannot tell "
        "which files the stamp is structurally blind to, so it refuses rather than "
        "reporting a set it was unable to narrow"
    )
    return {entry.decode("utf-8") for entry in done.stdout.split(b"\0") if entry}


def _bundle_roots(root: Path) -> list[Path]:
    """Every file under `root` that DECLARES a bundle, whatever it happens to be named.

    THE WALK IS `job_yaml.bundle_files`, which is where the suffixes a bundle document
    may carry are decided; this module reads that list instead of spelling a second one.
    It is one suffix WIDER than a bundle root can be -- `databricks.json` and
    `bundle.json` are refused as roots, measured -- so a JSON file declaring a named
    bundle is reported here and could not be one. That is over-strict in the direction
    that is loud; the other direction is the false green this block exists for.

    A FILE THAT WILL NOT PARSE IS A FAULT, NOT A SKIP. Skipping would report the expected
    value because the derivation could not look, which is the shape ADR 0018 names. Every
    file this walk reads parses today, and one that stops should say so rather than drop
    out of a set other arms then call total."""
    swept = bundle_files(root)
    ignored = _ignored(root, swept)
    found = []
    for path in swept:
        relative = path.relative_to(root).as_posix()
        if relative in ignored:
            continue
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (yaml.YAMLError, UnicodeDecodeError) as exc:
            raise AssertionError(
                f"{relative} carries a bundle-document suffix and does not parse "
                f"({type(exc).__name__}), so nothing here can say whether it declares a "
                "bundle. Fix the file, or move it out of the tree this walk reads"
            ) from exc
        if _declares_a_named_bundle(document):
            found.append(path)
    return found


def _roots_of_this_repository() -> list[Path]:
    """`_bundle_roots(_REPO)` with the floor every arm over it has to read first.

    THE ARM BELOW IS A CLAIM ABOUT A SET SOMETHING WALKED, and a walk that returns
    nothing satisfies "every root is watched" exactly as a correct tree does. The floor
    is the bundle this repository actually ships, taken from `job_yaml` -- the module
    that owns where the bundle is -- rather than typed here a second time, so a move of
    it is a red rather than a silent pass. It is a known member and not a count: a count
    would be a claim about the repository that the next bundle falsifies."""
    roots = _bundle_roots(_REPO)
    assert BUNDLE in roots, (
        f"the bundle this repository ships is at {BUNDLE}, and the content derivation "
        f"did not find it: {[str(path) for path in roots]}. Every claim below is about a "
        "set this walk read, so a walk that cannot find the known root earns none of them"
    )
    return roots


def _reaches_the_workspace(wheel: dict) -> dict[str, list[str]]:
    """What a deploy puts in the workspace, keyed by what puts it there.

    FOUR SOURCES OF TRUTH, each owned somewhere other than `DEPLOYMENT_INPUTS`, which is
    the whole point: the single-source version of the arm below was what certified the
    omission of `databricks/` as correct. The wheel's packages and the hook's registered
    path come from `pyproject.toml`; the sync roots come from what the files themselves
    DECLARE, which is not what they are named -- see the block above. Add a fifth thing
    to the deployment and this is where it goes."""
    return {
        "the wheel's package": wheel["packages"],
        "a bundle sync root": sorted(
            {path.parent.relative_to(_REPO).as_posix() for path in _roots_of_this_repository()}
        ),
        "the wheel's metadata": ["pyproject.toml"],
        # The stamp's own logic: this file decides what the revision SAYS, including
        # whether the dirty check runs at all, so an uncommitted edit to it would
        # otherwise produce a clean stamp for a commit that does not contain it.
        "the build hook": [wheel["hooks"]["custom"].get("path", "hatch_build.py")],
    }


def test_the_watched_paths_cover_everything_a_deploy_puts_in_the_workspace():
    """A silent hole this test ITSELF used to certify as closed.

    Its first version derived the expected set from `[tool.hatch...packages]` alone --
    it validated the watched paths against the WHEEL and never against the
    DEPLOYMENT. So `databricks/`, which `bundle deploy` syncs into the workspace and
    which every job's `python_file` is read from, was unwatched and green: an
    uncommitted edit to a job entry point shipped under a bare stamp equal to HEAD,
    the guard passed, and the run executed code that was never committed.

    IT NO LONGER ASSERTS THAT THERE IS EXACTLY ONE BUNDLE, and that is a replacement
    rather than a relaxation. The count was standing in for this check and could not
    carry it: a second root under any of the other three accepted spellings left it
    counting one. Now every root found is checked, so a second one inside `databricks/`
    passes -- the stamp already watches that directory -- and a second one anywhere the
    stamp does not look fails here, which is the case the count was there for.

    WHAT IT DOES NOT ESTABLISH: that every root the CLI would accept has been found. It
    reads what `job_yaml.bundle_files` walks, so a root above this checkout, or one
    reached only through a symlink out of it, is out of range; it drops what git's ignore
    rules exclude, deliberately, because the stamp is a `git status` that is blind to
    those as well; and a root declaring only `include:` takes its bundle name from the
    included file, so the directory named here would be that file's and not the CLI's
    root. Nor does it claim the covering path is SUFFICIENT -- only that one exists."""
    config = tomllib.loads((_REPO / "pyproject.toml").read_text(encoding="utf-8"))
    wheel = config["tool"]["hatch"]["build"]["targets"]["wheel"]
    watched = {Path(p) for p in DEPLOYMENT_INPUTS}

    for what, paths in _reaches_the_workspace(wheel).items():
        for path in paths:
            assert _covers(watched, path), (
                f"{path!r} reaches the workspace as {what}, and no watched path covers "
                f"it: {sorted(DEPLOYMENT_INPUTS)}. An uncommitted change there would be "
                "stamped as clean, and the guard would pass a run built from it"
            )


@pytest.mark.parametrize(
    ("name", "body", "declares", "why"),
    [
        (
            "nothing-the-cli-would-locate.yml",
            "bundle:\n  name: probe\n",
            True,
            "a name the CLI would not locate a root under is still found, which is what "
            "makes this a content derivation rather than a list of filenames waiting to "
            "be one short",
        ),
        (
            "databricks.yml",
            "resources:\n  jobs: {}\n",
            False,
            "the CLI refuses a root with no bundle name -- `unable to define default "
            "workspace root: bundle name not defined` -- so the accepted filename is not "
            "what makes a file a root, and keying on the name is what earns that",
        ),
        (
            "under-a-target.yml",
            "targets:\n  dev:\n    bundle:\n      name: probe\n",
            True,
            "the CLI takes the name from `targets.<t>.bundle.name` too, and from "
            "`environments.<t>.bundle.name`; asking every depth commits to neither list",
        ),
    ],
)
def test_a_bundle_root_is_what_a_file_declares_and_not_what_it_is_named(
    repo, name, body, declares, why
):
    """THE DERIVATION ITSELF, on a tree whose contents this test decides.

    The arm above reads the real repository, where there is one bundle and every
    spelling question is hypothetical -- so it would stay green under a derivation that
    had quietly gone back to matching a filename. These three cases are the ones that
    separate the two, and each is measured against the CLI rather than assumed.

    IT DOES NOT ESTABLISH that the CLI would accept the files it calls roots, nor that it
    would refuse the one it does not. The three verdicts quoted here were measured on CLI
    v1.8.0 by hand; nothing in CI re-measures them, and a CLI that changed its mind would
    leave this arm green and its reasons stale."""
    (repo / "docs" / name).write_text(body, encoding="utf-8")
    found = [path.name for path in _bundle_roots(repo)]
    assert (found == [name]) is declares, f"{found} was derived from {name}: {why}"


@pytest.mark.parametrize(
    ("directory", "covered", "why"),
    [
        (
            "docs",
            False,
            "a second bundle root outside every watched path is the defect this block "
            "was rewritten for: nothing would stamp an uncommitted edit to it dirty, so "
            "a run built from it would carry a bare stamp equal to HEAD and be accepted",
        ),
        (
            "databricks/second",
            True,
            "a second root INSIDE the watched sync root is deliberately fine, and this "
            "case is what says so: the dirty check already sweeps `databricks`, so the "
            "replacement for the old `== 1` count must not refuse it",
        ),
    ],
)
def test_a_planted_second_bundle_root_is_watched_only_where_the_stamp_looks(
    repo, directory, covered, why
):
    """THE PLANTED POSITIVE, kept as an arm instead of deleted with the commit that ran
    it -- which is exactly what left the old glob's gap unlocked in the first place.

    A second root is planted under a spelling the CLI accepts and the old derivation did
    not look for, and the answer is read out of the same `_covers` the arm above uses.
    The `docs` case is the one that must FAIL a real tree; the `databricks/second` case
    is the behaviour this replacement intends and is not an accident of it.

    IT DOES NOT ESTABLISH anything about `DEPLOYMENT_INPUTS` being right. It reads that
    tuple; the arm above is what asserts this repository's own roots are inside it."""
    (repo / directory).mkdir(parents=True, exist_ok=True)
    (repo / directory / "bundle.yml").write_text(
        "bundle:\n  name: second\n", encoding="utf-8"
    )
    roots = _bundle_roots(repo)
    assert [path.name for path in roots] == ["bundle.yml"], (
        f"the plant under {directory} was not derived as a bundle root: {roots}"
    )
    watched = {Path(p) for p in DEPLOYMENT_INPUTS}
    reached = {
        _covers(watched, root.parent.relative_to(repo).as_posix()) for root in roots
    }
    assert reached == {covered}, why


def test_a_bundle_root_git_ignores_is_dropped_and_one_it_does_not_survives(repo):
    """THE EXCLUSION, ASSERTED IN BOTH DIRECTIONS IN ONE ARM, because a filter is exactly
    the shape that can silently stop filtering.

    It did. The first `_ignored` here matched nothing at all -- Windows text mode put a
    carriage return on every path it handed git, git quoted the odd names on the way back,
    and the returned set intersected nothing. Every arm in this file stayed green, because
    no ignored directory in this repository holds a bundle root; only a plant in one
    showed it. So both roots are planted together: an empty answer fails as loudly as an
    unfiltered one, and neither can be reached by the filter doing nothing.

    WHY THE EXCLUSION IS THERE AT ALL is `_ignored`'s own docstring, and it is not
    convenience: the stamp is `git status --porcelain`, which cannot report an ignored
    path, so no edit to `DEPLOYMENT_INPUTS` could ever clear a red raised over one.

    IT DOES NOT ESTABLISH that an ignored bundle root is harmless. It cannot be deployed
    from this repository and it cannot make the stamp dirty; whether someone runs the CLI
    on it by hand is outside anything the stamp sees."""
    (repo / ".gitignore").write_text("scratch/\n", encoding="utf-8")
    for directory in ("scratch", "databricks"):
        (repo / directory).mkdir(exist_ok=True)
        (repo / directory / "bundle.yml").write_text(
            "bundle:\n  name: probe\n", encoding="utf-8"
        )
    found = [path.relative_to(repo).as_posix() for path in _bundle_roots(repo)]
    assert found == ["databricks/bundle.yml"], (
        f"two bundle roots were planted, one under a git-ignored directory, and the "
        f"derivation returned {found}. Either the ignore filter stopped filtering or it "
        "dropped a root the stamp can see"
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
