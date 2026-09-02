# tests/ledger_sources.py
"""What the unexercised ledger POINTS AT: the source lines its anchors name, the corpus
its sweep reads, and the git object store its evidence cites. No test lives here, and
nothing here opens `docs/unexercised-ledger.md`.

THE SEAM IS BY SUBJECT AND THE FILE ITSELF DREW IT. `tests/test_unexercised_ledger.py`
carried a banner reading *"WHAT THE REPOSITORY SAYS. Nothing below opens the ledger
document."* -- a claim of disjointness its author had already checked -- and that is the
block extracted here. What stays there is the other subject: what the DOCUMENT says
(its ids, buckets, totals and rows, all string work over one file), the COMPARISONS
between the two, and the failure arms. So the split is not "readers versus tests" by
arithmetic; it is *the ledger* versus *everything the ledger refers to*, and the two
halves reach for different things -- one for a markdown table, the other for the
filesystem and for `git`.

WHY IT HAPPENED NOW. `tests/test_unexercised_ledger.py` stood at **799 lines** against a
strictly-under-800 cap (master protocol section 4.12) and F2 wave 2 owed it rows. There
was no line to add one in. `tests/job_yaml.py`, `tests/task_ast.py`,
`tests/vault_job_demands.py` and `tests/adr_files.py` are this suite's precedent for the
shape, including their rule that a test module must not import another test module: a
plain module under `tests/` gives the suite no collection-order dependency, because pytest
collects nothing from it and it declares no fixture.

ONE FUNCTION THE OLD BLOCK HELD DID NOT COME, AND SAYING WHICH IS THE POINT.
`_unconsolidated_sections` stayed behind, because it is a COMPARISON -- it asks whether a
section the sweep found is one the document declares -- and it calls `_headings`, which
reads the document. It was the single member of that block reaching back across the seam,
which is why it now sits with the other comparisons rather than being dragged here and
dragging `_headings` after it.

`corpus_files` AND NOT `corpus`, which is the one rename the extraction forced.
`swept_sections` takes a `corpus` PARAMETER so the failure arms can hand it a document
without writing one into the tree; a module-level function of that name would be shadowed
by it at every call site inside this file. The parameter is the name the arms already
spell, so the function moved instead."""
from __future__ import annotations

import re
import subprocess
from functools import lru_cache
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# How many lines after the anchor a claim may be found in. `window` argues the number.
ANCHOR_WINDOW = 3

# THE SWEEP, AT TWO WIDTHS, deliberate rather than drift. `PUBLISHED_PATTERN` is what §8
# prints for a reader; it misses `## 3. What F7 leaves unrun, and where the rest of it lives`
# in `docs/f7-run-evidence.md` -- ledger-shaped, in a file the glob matches, missed on WORDING
# ALONE, and the precedent an F8 author would copy. The sweep test asserts CONTAINMENT.
PUBLISHED_PATTERN = r"^#+ .*(unexercised|did not exercise)"
SECTION_PATTERN = (r"^#+ .*(unexercis|did not exercise|didn't exercise|leaves unrun"
                   r"|not exercised)")

# How a swept section §0.4 does not declare can still be accounted for: by saying in its own
# body that its debt lives in the consolidated file, which `docs/f7-run-evidence.md` §3 does.
DEFERRAL = "unexercised-ledger.md"


def norm(text: str) -> str:
    """Whitespace collapsed, so a wrapped source line and a table cell compare equal."""
    return " ".join(text.split())


@lru_cache(maxsize=64)
def source_lines(path: str) -> tuple[str, ...]:
    resolved = REPO / path
    assert resolved.is_file(), f"{path} is not a file in this repository"
    return tuple(resolved.read_text(encoding="utf-8").splitlines())


def window(path: str, line: int) -> str:
    """The anchored line plus the two after it, whitespace collapsed.

    THREE LINES AND NOT ONE. A claim in these documents is a bullet that wraps, so a quote a
    reader would call "the line" often spans two; a window of one would redden on a re-wrap
    that changed nothing.

    AND HERE IS WHAT THAT TOLERATES, MEASURED -- the earlier wording, *"three is still tight
    enough that a claim which MOVES goes red"*, was false for the size of move that happens. 179
    of the 182 claims match on the anchor line ALONE, and over `docs/f5-run-evidence.md`'s 22
    anchors: inserting ONE or TWO lines above them reddens NOTHING, THREE reddens all 22, DELETING
    one reddens all 22 at once. THE TOLERANCE IS ASYMMETRIC, the window running forward only --
    concrete and pending, since writing the broker-probe result into that file is an insertion.
    `ANCHOR_WINDOW` stays at three: one buys precision at a red on every re-wrap."""
    lines = source_lines(path)
    assert 1 <= line <= len(lines), f"{path}:{line} is past the end of the file"
    return norm(" ".join(lines[line - 1:line - 1 + ANCHOR_WINDOW]))


def corpus_files() -> dict[str, str]:
    """The phase records the sweep reads: `docs/*-evidence.md`, and ONLY that.

    NOT "anywhere under `docs/`", WHICH IS WHAT THIS FILE CLAIMED IN TWO PLACES. The glob is one
    level deep and matches one suffix: `docs/adr/0016-*.md` and `0017-*.md` both carry a `### What
    ships UNEXERCISED` it does not see, and `0008` an *"unexercised here by choice"*. THAT
    EXCLUSION IS DEFENSIBLE AND §7 MAKES IT; claiming to sweep all of `docs/` while globbing one
    suffix was not. A dict, so the arms can add a document without writing one into the tree."""
    return {path.relative_to(REPO).as_posix(): path.read_text(encoding="utf-8")
            for path in sorted((REPO / "docs").glob("*-evidence.md"))}


def swept_sections(pattern: str = SECTION_PATTERN,
                   corpus: dict[str, str] | None = None,
                   ) -> list[tuple[str, str, list[str], str]]:
    """(file, heading, the headings it nests under, its body) for every ledger-shaped section.

    THIS IS THE HALF A DECLARED LIST CANNOT SUPPLY. `_heading_misses` proves the ten declared
    sections still exist; only a sweep proves there is no ELEVENTH one that nobody added.
    Ancestors and body come with it, because "undeclared" is not on its own the question --
    `_unconsolidated_sections`, which stayed in the test module, says what is."""
    found: list[tuple[str, str, list[str], str]] = []
    for name, text in (corpus_files() if corpus is None else corpus).items():
        lines = text.splitlines()
        above: list[tuple[int, str]] = []
        for number, line in enumerate(lines):
            hashes = re.match(r"^(#+) ", line)
            if not hashes:
                continue
            level = len(hashes.group(1))
            while above and above[-1][0] >= level:
                above.pop()
            if re.match(pattern, line, re.IGNORECASE):
                end = next((i for i in range(number + 1, len(lines))
                            if re.match(rf"^#{{1,{level}}} ", lines[i])), len(lines))
                found.append((name, line.strip(), [head for _, head in above],
                              "\n".join(lines[number:end])))
            above.append((level, line.strip()))
    return found


def swept_headings(pattern: str = SECTION_PATTERN,
                   corpus: dict[str, str] | None = None) -> set[tuple[str, str]]:
    """Just the (file, heading) pairs of the sweep, which is the shape §0.4 declares."""
    return {(name, heading) for name, heading, _, _ in swept_sections(pattern, corpus)}


def commit_resolves(sha: str) -> bool:
    return subprocess.run(["git", "cat-file", "-e", f"{sha}^{{commit}}"], cwd=REPO,
                          capture_output=True).returncode == 0


@lru_cache(maxsize=1)
def history_is_deep() -> bool:
    """Whether a `commit:` token can be resolved in this clone at all.

    CI COULD NOT ANSWER IT AND THE LEDGER LOCK FAILED THERE RATHER THAN SAY SO. `ci.yml`'s
    `test` job uses `actions/checkout@v4` and sets no `fetch-depth` -- only `secret-scan` asks
    for `0` -- so it runs at the action's default of `1`, and §4.2's `commit:2d077a8`, an F5-era
    commit, is not in the object store. Reproduced with `git clone --depth 1`: two tests turned
    red for a reason with nothing to do with the ledger. A CHECK THAT CANNOT LOOK MUST SAY SO,
    NOT REPORT THE VALUE IT EXPECTED -- `commit:0000000` and a real commit are INDISTINGUISHABLE
    here, so "does not resolve" read as "fine" is the green-that-reads-like-verification that
    document is about. `tests/test_adr_phase_declaration.py` sets the rule for this checkout;
    this is its second user."""
    probe = subprocess.run(["git", "rev-parse", "--is-shallow-repository"], cwd=REPO,
                           capture_output=True, text=True)
    return probe.returncode == 0 and probe.stdout.strip() == "false"
