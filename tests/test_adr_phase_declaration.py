# tests/test_adr_phase_declaration.py
"""The PHASE column of `docs/adr/README.md` against git, and against itself.

THE HALF OF `tests/test_adr_index.py` THAT NEEDS A REPOSITORY. Everything that file
asserts is read out of the ADR documents; the phase, the merge sha and the branch cannot
be -- they are declared in `scripts/adr_index.py` -- so they are re-derived from git here.
The split happened when the other file reached 796 lines against a strictly-under-800 cap
and the next addition was the one below; the readers both halves share went to
`tests/adr_files.py` rather than being copied.

WHAT RUNS WHERE. The git half SKIPS in CI: `actions/checkout@v4` runs at its default
`fetch-depth: 1`, so `git log --diff-filter=A` has nothing to walk. It skips with the
reason printed rather than passing over an empty history, which would be a check reporting
the expected value because it could not look. The phase LABEL against the declared branch
needs no git and does run there.

AND THE BRANCH OF AN `UNMERGED` ROW IS NOW PUT TO GIT TOO, which it was not. A MERGED row's
sha and branch are both driven against the merge subject; an `unmerged` row reached
`_is_on_main` and NOTHING ELSE, so the branch string was never asked of git at all.
Reproduced against the live row before this arm existed: replacing it with a name no branch
has ever carried left this file green and `generate_adr_index.py --check` reporting the page
current -- branch names are not rendered into `docs/adr/README.md`, so `--check` structurally
cannot see it either. Wrong only while a phase is in flight, which is exactly when nobody is
looking.

WHAT THAT ARM CLAIMS, AND THE LINE IT DOES NOT CROSS. It asks git whether the declared branch
EXISTS in this checkout's refs -- no history, no network, no remote call. It therefore catches
a branch that does not exist. It does NOT catch a branch that exists and is the wrong one:
nothing here asks whether that branch contains the ADR, because `git branch --contains` needs
the history CI does not have and the answer would be a skip everywhere it mattered.

IT NEEDS THE BRANCHES TO HAVE BEEN FETCHED, so it carries a second gate beside the history
one, and the whole arm skips where they have not been -- measured on a `--depth 1 --branch`
clone, which carries exactly two refs, both naming the branch it checked out, and no
`origin/main`. Folding the branch question into the same arm means that arm now needs both;
the alternative was a check that quietly stopped looking in some checkouts, which is this
file's own subject.

AND THE CASE THAT DID NOT EXIST UNTIL AN ADR WAS WRITTEN AFTER THIS LOCK. Every declared
merge was a real sha while all twenty ADRs predated the index. ADR 0021 is the first ADR
written in a phase that is still open, and its merge commit DOES NOT EXIST YET -- any sha
declared for it would be invented. So `adr_index.UNMERGED` is declared instead, and it is
REFUTABLE: the moment the ADR's adding commit becomes an ancestor of `origin/main` the
declaration is stale and this file says so, with its own failure arm below. A sentinel
that only ever meant "do not look" would be the hole this suite is written against.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from adr_files import REPO, adr_paths, branch_names_phase, load

# What `_merge_subject_for` answers when git found no adding commit, and when it found one
# that no merge carries to `origin/main`. Both are REPORTED rather than asserted on: what
# refutes an `unmerged` declaration is `_is_on_main`, and these two strings say the same
# thing about a branch commit and about a file nobody has committed yet.
_NEVER_ADDED = "<never added on this branch>"
_NO_MERGE = "<no merge; committed straight onto main>"

# The phrase the branch-existence fault carries, so the arms below can pick it out of a fault
# list that may hold others. A marker rather than a whole-message comparison: the message is
# written for the reader who has to fix it and will be reworded, and an arm keyed to its exact
# wording would go quietly vacuous on the first edit.
_NO_SUCH_REF = "resolves to no ref"

# SUFFIXED ONTO A REAL BRANCH NAME BY THE TWO ARMS THAT PLANT ONE, never used as a name on its
# own. Suffixing keeps the phase label matched by the branch -- `branch_names_phase` asks for
# the phase's tokens contiguously, and appending tokens leaves them where they were -- so the
# fault those arms assert on is the branch-existence one and not a phase mismatch riding along.
_NEVER_A_BRANCH = "-not-a-real-branch-at-all"


def _adding_commit(path: Path) -> str:
    """The OLDEST commit that added this ADR, or `""` if this branch has none."""
    added = subprocess.run(
        ["git", "log", "--diff-filter=A", "--format=%H", "--", str(path.relative_to(REPO))],
        cwd=REPO, capture_output=True, text=True, check=True,
    ).stdout.split()
    return added[-1] if added else ""


def _merge_subject_for(path: Path) -> str:
    """`<merge sha> <subject>` of the merge that first brought this ADR to `origin/main`."""
    added = _adding_commit(path)
    if not added:
        return _NEVER_ADDED
    merges = subprocess.run(
        ["git", "rev-list", "--ancestry-path", "--merges", f"{added}..origin/main"],
        cwd=REPO, capture_output=True, text=True, check=True,
    ).stdout.split()
    if not merges:
        return _NO_MERGE
    return subprocess.run(
        ["git", "log", "-1", "--format=%h %s", merges[-1]],
        cwd=REPO, capture_output=True, text=True, check=True,
    ).stdout.strip()


def _is_on_main(path: Path) -> bool:
    """Whether this ADR's adding commit is already an ancestor of `origin/main`.

    THIS IS THE WHOLE OF WHAT REFUTES THE `unmerged` SENTINEL, and it is deliberately not
    a comparison against `_merge_subject_for`'s text. That text answers `_NO_MERGE` both
    for a branch commit AND for one that landed on `main` with no merge commit, and it
    answers `_NEVER_ADDED` for an ADR written but not yet committed -- a state that is
    transient by construction, invisible in CI, and perfectly consistent with `unmerged`.
    Failing on it would be a false alarm of the kind that gets a check loosened. Being an
    ancestor of `origin/main` is the one observation that makes `unmerged` false."""
    added = _adding_commit(path)
    if not added:
        return False
    return subprocess.run(
        ["git", "merge-base", "--is-ancestor", added, "origin/main"],
        cwd=REPO, capture_output=True, text=True,
    ).returncode == 0


def _branch_ref(branch: str) -> str:
    """The ref this checkout resolves `branch` to -- its own first, then origin's -- or `""`.

    EXISTENCE AND NOTHING MORE. `--verify --quiet` answers it with no history walk, no
    network and no remote call, which is what makes it runnable at all; `git branch
    --contains` would answer the stronger question and needs history CI does not have.

    ORIGIN'S REF COUNTS TOO, and that is a deliberate widening of what the reproduction
    called for. A clone that has fetched but never checked the branch out carries only
    `refs/remotes/origin/<branch>`, and failing there would be a false alarm of exactly the
    kind that gets a check loosened -- while a name no branch has ever carried resolves in
    neither namespace, which is the case this exists for."""
    for ref in (f"refs/heads/{branch}", f"refs/remotes/origin/{branch}"):
        found = subprocess.run(
            ["git", "rev-parse", "--verify", "--quiet", ref],
            cwd=REPO, capture_output=True, text=True,
        )
        if not found.returncode:
            return ref
    return ""


def _fetched_branches() -> list[str]:
    """Branch refs this checkout carries OTHER than the one it has checked out.

    THE CAPABILITY PROBE FOR THE BRANCH ARM, and it is not the question that arm asks. A
    `git clone --depth 1 --branch <b>` carries exactly two refs -- `<b>` and `origin/<b>` --
    and can therefore answer nothing about any other branch, so this returns empty there and
    the arm skips. Measured on such a clone of this repository rather than assumed."""
    head = subprocess.run(
        ["git", "symbolic-ref", "--quiet", "--short", "HEAD"],
        cwd=REPO, capture_output=True, text=True,
    ).stdout.strip()
    listed = subprocess.run(
        ["git", "for-each-ref", "--format=%(refname:short)", "refs/heads/", "refs/remotes/"],
        cwd=REPO, capture_output=True, text=True,
    )
    ignored = {"origin/HEAD", head, f"origin/{head}"}
    return [ref for ref in listed.stdout.split() if ref not in ignored]


def _declaration_faults(idx, path: Path) -> list[str]:
    """Everything git disagrees with in one ADR's declared `(phase, merge, branch)`.

    ALL THREE DECLARED FACTS, SEPARATELY. The first spelling of this asked
    `if merge not in subject and branch not in subject` -- an OR, so the branch name alone
    satisfied it and the declared sha went unchecked whenever the branch was right, while
    the phase label was compared to nothing at all. `("F99", "0000000",
    "feat/f6-rca-agent")` was declared for an ADR and the test reported green.

    The sha and the branch are checked against git's own merge subject; the PHASE label is
    checked against that branch, because git carries no other record of it."""
    number = path.name[:4]
    phase, merge, branch = idx.PHASES[number]
    subject = _merge_subject_for(path)
    faults = []
    if merge == idx.UNMERGED:
        if _is_on_main(path):
            faults.append(
                f"{number}: declared {merge}, but its adding commit is an ancestor "
                f"of origin/main and git says {subject!r} -- re-declare it with the "
                "merge that brought it there"
            )
        if not _branch_ref(branch):
            faults.append(
                f"{number}: declared branch {branch!r}, which this checkout {_NO_SUCH_REF} "
                "-- neither its own nor origin's. An unmerged row has no merge subject to "
                "check the branch against, so this is the only thing git can be asked about "
                "it; a branch that exists and is the WRONG one is not caught here"
            )
    else:
        if merge not in subject:
            faults.append(f"{number}: declared merge {merge}; git says {subject!r}")
        if branch not in subject:
            faults.append(f"{number}: declared branch {branch}; git says {subject!r}")
    if not branch_names_phase(phase, branch):
        faults.append(f"{number}: declared phase {phase!r} is not named by {branch!r}")
    return faults


def _history_is_deep() -> bool:
    """Whether git here can answer `which merge brought this file to main`."""
    probe = subprocess.run(
        ["git", "rev-parse", "--is-shallow-repository"],
        cwd=REPO, capture_output=True, text=True,
    )
    if probe.returncode or probe.stdout.strip() != "false":
        return False
    ref = subprocess.run(
        ["git", "rev-parse", "--verify", "origin/main"],
        cwd=REPO, capture_output=True, text=True,
    )
    return ref.returncode == 0


def _skip_without_history() -> None:
    if not _history_is_deep():
        pytest.skip(
            "shallow clone or no origin/main: the phase derivation needs full history. "
            "This is CI's default checkout, so this arm does not run there"
        )


def _skip_without_fetched_branches() -> None:
    if not _fetched_branches():
        pytest.skip(
            "this checkout carries no branch ref but its own, so it cannot say whether a "
            "declared branch exists. That is CI's default checkout and a `--depth 1 "
            "--branch` clone, so this arm is a developer-box arm and does not run there"
        )


def test_the_declared_phase_is_what_git_says():
    """The declared merge sha and branch against GIT, for every ADR on disk.

    TWO GATES NOW, because an `unmerged` row's branch is put to git here as well and that
    needs the branches to have been fetched. Both skips name their own reason. The rejected
    alternative was to make the branch half conditional inside `_declaration_faults`, which
    would leave this arm reporting the same green in a checkout that had silently stopped
    asking -- the shape this whole file is written against."""
    _skip_without_history()
    _skip_without_fetched_branches()
    idx = load("adr_index", "adr_index.py")
    wrong = [fault for path in adr_paths() for fault in _declaration_faults(idx, path)]
    assert not wrong, wrong


def test_an_unmerged_declaration_is_refused_once_the_adr_reaches_main(monkeypatch):
    """THE SENTINEL'S OWN FAILURE ARM: `unmerged` must stop being true.

    DERIVED, NOT TYPED. The subject ADR is whichever one git says is already on
    `origin/main` -- not a number written into this arm, which would punish a renumbering
    instead of the drift. Its declaration is rewritten to `UNMERGED` in memory and the
    fault list must name it; if it does not, a stale `unmerged` would sit on the page
    forever after the phase that wrote it closed."""
    _skip_without_history()
    idx = load("adr_index", "adr_index.py")
    merged = [path for path in adr_paths() if _is_on_main(path)]
    assert merged, "no ADR on this branch is on origin/main; re-point this arm"
    path = merged[0]
    number = path.name[:4]
    phase, _merge, branch = idx.PHASES[number]
    monkeypatch.setitem(idx.PHASES, number, (phase, idx.UNMERGED, branch))
    faults = _declaration_faults(idx, path)
    assert any("ancestor of origin/main" in fault for fault in faults), faults


def test_a_phase_label_is_not_matched_by_a_branch_naming_its_sub_phase():
    """THE ONLY PHASE CONFUSION THIS TABLE OFFERS, which flattened containment missed.

    `F1.4` and `F1.4b` are both declared, against branches differing by one character in
    the same place. Stripped to `[a-z0-9]` and asked for substring containment, each was
    found inside the other's branch -- `f14` inside `f14bprb...`, and `f14b` inside
    `f14bronze...`, where that `b` is the first letter of `bronze`. The check rejected
    `F9`, a phase from another universe, and accepted the one pair it exists for.

    The block at the end is the whole declaration, checked WITHOUT git -- so unlike
    `test_the_declared_phase_is_what_git_says` this half runs in CI."""
    assert branch_names_phase("F1.4", "feat/f1-4-bronze-generalisation")
    assert branch_names_phase("F1.4b", "feat/f1-4b-empresas-socios")
    assert branch_names_phase("F1.4b PR B", "feat/f1-4b-pr-b-second-month")
    assert branch_names_phase("F2 wave 1", "feat/f2-wave-1-cnpj-vault")
    assert not branch_names_phase("F1.4", "feat/f1-4b-pr-b-second-month")
    assert not branch_names_phase("F1.4b", "feat/f1-4-bronze-generalisation")
    assert not branch_names_phase("F9", "feat/f6-rca-agent")
    idx = load("adr_index", "adr_index.py")
    wrong = [
        number for number, (phase, _merge, branch) in idx.PHASES.items()
        if not branch_names_phase(phase, branch)
    ]
    assert not wrong, f"{wrong}: the declared phase is not named by the declared branch"


def _first_declared_row(idx) -> tuple[str, Path, str]:
    """The lowest-numbered declared ADR: its number, its file, and its declared phase.

    DERIVED RATHER THAN TYPED, for the reason the sentinel arm above already gives: a number
    written into an arm punishes a renumbering instead of the drift it exists to catch."""
    number = sorted(idx.PHASES)[0]
    path = next(path for path in adr_paths() if path.name.startswith(number))
    return number, path, idx.PHASES[number][0]


def test_a_branch_git_has_never_heard_of_is_refused(monkeypatch):
    """THE PLANTED POSITIVE, MADE PERMANENT.

    The name is built by suffixing the declared branch, which keeps the phase label matched
    by it, so the fault this asserts on is the branch-existence one and not a phase mismatch
    riding along. Reproduced before the arm existed: the live row rewritten to a name like
    this left this file green and `generate_adr_index.py --check` reporting the page current.

    IT IS GATED ON HISTORY THOUGH ITS OWN QUESTION IS NOT, and that is measured rather than
    cautious. The first version of this arm carried no gate, on the reasoning that a name no
    branch has ever carried resolves in no checkout. Run against a `--depth 1` clone it did
    not pass -- it ERRORED: `_declaration_faults` reaches `_merge_subject_for` first, which
    runs `git rev-list ..origin/main` under `check=True`, and that is fatal where there is no
    `origin/main`. Ungated, this arm would have turned CI red rather than running there."""
    _skip_without_history()
    idx = load("adr_index", "adr_index.py")
    number, path, phase = _first_declared_row(idx)
    invented = f"{idx.PHASES[number][2]}{_NEVER_A_BRANCH}"
    assert not _branch_ref(invented), f"{invented} resolves here; re-point this arm"
    monkeypatch.setitem(idx.PHASES, number, (phase, idx.UNMERGED, invented))
    faults = _declaration_faults(idx, path)
    assert any(_NO_SUCH_REF in fault for fault in faults), faults


def test_a_branch_this_checkout_carries_satisfies_the_same_arm(monkeypatch):
    """THE GREEN SIDE, without which the arm above is one that reddens on everything.

    The branch it stands on is this checkout's OWN, so it resolves wherever HEAD is on a
    branch rather than depending on which phase happens to be in flight. A detached HEAD --
    CI's pull-request checkout -- has no such branch and skips with the reason printed.

    IT PROVES EXISTENCE AND NOT CORRECTNESS. This branch is almost certainly not the one the
    row should declare, and the arm passes anyway: that is exactly the limit this file's
    docstring states.

    THE HISTORY GATE IS HERE FOR THE ARM ABOVE'S REASON, not for its own: the fault list it
    reads is built by a function that walks to `origin/main` before it ever reaches the
    branch."""
    _skip_without_history()
    here = subprocess.run(
        ["git", "symbolic-ref", "--quiet", "--short", "HEAD"],
        cwd=REPO, capture_output=True, text=True,
    ).stdout.strip()
    if not here:
        pytest.skip("HEAD is detached, so this checkout has no branch of its own to stand on")
    idx = load("adr_index", "adr_index.py")
    number, path, phase = _first_declared_row(idx)
    monkeypatch.setitem(idx.PHASES, number, (phase, idx.UNMERGED, here))
    assert not [fault for fault in _declaration_faults(idx, path) if _NO_SUCH_REF in fault]
    monkeypatch.setitem(idx.PHASES, number, (phase, idx.UNMERGED, f"{here}{_NEVER_A_BRANCH}"))
    assert [fault for fault in _declaration_faults(idx, path) if _NO_SUCH_REF in fault]
