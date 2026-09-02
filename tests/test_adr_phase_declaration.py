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

AND THE CASE THAT DID NOT EXIST UNTIL AN ADR WAS WRITTEN AFTER THIS LOCK. Every declared
merge was a real sha while all twenty ADRs predated the index. An ADR written in a phase
that is still OPEN has no merge commit at all -- any sha declared for it would be
invented -- and this branch carries one. So `adr_index.UNMERGED` is declared instead, and
it is REFUTABLE: the moment the ADR's adding commit becomes an ancestor of `origin/main`
the declaration is stale and this file says so, with its own failure arm below. A sentinel
that only ever meant "do not look" would be the hole this suite is written against.

THE ADR'S NUMBER IS DELIBERATELY NOT WRITTEN INTO THIS FILE, here or in the arm below.
TWO phases were open at once when this lock was written, each with its own unmerged ADR on
its own branch, so a paragraph naming one number is false on the other branch and false
again on the tree that merges both -- and a test naming one would punish a renumbering
instead of the drift it exists to catch. `_declaration_faults` reads the sentinel out of
`adr_index.PHASES` and the arm derives its subject from git, so the set of ADRs either one
applies to is re-derived on every run rather than typed once and left.
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


def test_the_declared_phase_is_what_git_says():
    """The declared merge sha and branch against GIT, for every ADR on disk."""
    _skip_without_history()
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
