# scripts/adr_index.py
"""What `docs/adr/README.md` is made of: the ADR set read, plus what cannot be read.

THE PAIR: this module derives and declares; `scripts/generate_adr_index.py` renders and
writes. Split because together they cross the 800-line file cap, and at a seam that is
real rather than arithmetic -- everything here is a fact about the ADRs, nothing here
is a fact about markdown.

WHY THIS IS A GENERATOR AND NOT A PAGE SOMEBODY TYPED. This repository's signature
defect is a hand-maintained list that nobody updates: the route doc's status column
was stale for five phases, the "guards found" tally is spelled five ways in five
places and none of them moved when it grew, and ADR 0009's job list named five jobs
of a bundle that had grown past it until F7 Task 3 struck it. A hand-written index of
this ADR set would be that defect, introduced by the phase whose subject is that
defect. So the index is derived from the files, and `tests/test_adr_index.py` fails
when the index and the ADR set disagree.

WHERE THIS LIVES, AND WHAT WAS REJECTED. `scripts/` is this repository's home for
tools that run on a developer's host against the repo tree
(`migrate_lookups_to_subdir.py`, `validate_cnpj_snapshots.py`), and
`tests/test_revision_stamp.py`'s own comment names it as the place holding "genuinely
local tools ... that run on the extraction host and may shell out freely" -- the one
tree where consulting git is not banned. REJECTED: `src/opl/`, which is the wheel
that is built and synced to Databricks, where `docs/` does not exist and a git call
either crashes or answers from the operator's own repository (ADR 0009's last
consequence, asserted over every wheel file by `test_revision_stamp.py`). REJECTED:
`databricks/src/`, which is job entry points; two repo-wide sweeps parametrise over
that directory and would each acquire a test asserting things about a file that is
not a job task.

THREE FACTS ARE DERIVED FROM THE FILES AND TWO ARE DECLARED, AND THE PAGE SAYS WHICH.
Title, status and decision structure are read out of each ADR: they cannot go stale
because nothing stores them twice. The PHASE and the reversal READINGS cannot be
derived -- see `PHASES` and `READINGS` -- so they are declarations that carry their
citation, in the shape `src/opl/dataops/cadence.py` uses, and each is locked by a
totality assertion that fires when the ADR set moves underneath it.

WHAT THE CONDITION EXTRACTOR CAN AND CANNOT SEE, STATED HERE RATHER THAN LEFT TO BE
DISCOVERED. The repository spells "what would reverse this" three ways: a
`### What would reverse this decision` section, a `### What would change this decision`
section, and an inline `**What reverses it:**` paragraph under a numbered decision. This
module reads all three and NOTHING ELSE, and WHICH ADRs use each spelling is derived by
`adrs_by_spelling` rather than typed into a range that nothing checks. One consequence it
does not hide: ADR 0013 carries a fourth spelling in its Consequences -- *"A second
month-pair could reverse this"* -- which is not counted.

AND THE GRAMMAR UNDERNEATH THOSE THREE, WHICH IS THE FLOOR A CONDITION HAS TO CLEAR.
Code fences and the HTML blocks that render as nothing -- comments, `<pre>`, `<script>`,
`<style>`, `<textarea>` -- are masked out before anything is parsed (`_mask_non_prose`),
so a heading or a `**What reverses it:**` line inside one neither ends a section early
nor invents a condition. EVERY reversal section in a file is read, not only the first.
Inside a conditions section a bullet is `-`, `*`, `+` or `N.`, indented at most three
spaces; a bullet indented four or more is not a condition of its own. An extractor whose
limit is not written down is a count nobody can check.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_ADR_DIR = _REPO / "docs" / "adr"
INDEX = _ADR_DIR / "README.md"

# The ADR files themselves. `README.md` is not matched by this glob, so the index
# cannot index itself -- and `_assert_the_sweep_found_the_adr_set` refuses a glob that
# stopped matching rather than letting an empty sweep render an empty page.
_ADR_GLOB = "0*.md"

_STATUS_WORDS = ("Accepted", "Proposed", "Rejected", "Deprecated", "Superseded")

# The three recognised spellings of a reversal condition, as constants rather than as
# literals repeated in the renderer -- `adrs_by_spelling` is what the page prints instead
# of a hand-typed ADR range.
SECTION_REVERSE = "### What would reverse this decision"
SECTION_CHANGE = "### What would change this decision"
INLINE_MARKER = "**What reverses it:**"
SPELLINGS = (SECTION_REVERSE, SECTION_CHANGE, INLINE_MARKER)

# A fence opener or closer: CommonMark allows up to three leading spaces and three or
# more backticks or tildes. A bullet: the same three-space allowance, and every marker
# CommonMark accepts -- `-`, `*`, `+` and `N.`. The first draft of this file read `-` and
# `N.` at column zero only, so ADR 0012's `- Spark exposing...` respelled `* Spark
# exposing...` -- one character, still valid markdown -- deleted a stated condition from
# the page with the whole suite green.
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
_BULLET = re.compile(r"^ {0,3}(?:\d+\.|[-*+])\s+", re.M)

# AND THE HTML THAT RENDERS AS NOTHING, which a fence-only mask let straight through. An
# HTML comment (CommonMark block type 2) renders as nothing at all; a `<pre>`, `<script>`,
# `<style>` or `<textarea>` block (type 1) renders as nothing or as literal text. Either
# way its contents are not prose, and a reading that parses them publishes a heading, a
# bullet or a marker that no rendered document contains. A comment opened AND closed on
# one line is a span cut out of that line; a comment or a type-1 block opened at the start
# of a line runs to its closer, or to the end of the file if the closer never comes, which
# is CommonMark's rule rather than a guess. Anchored at the line start on purpose: a
# `<!--` inside backticks mid-sentence is a document talking ABOUT comments, and masking
# from there to the end of the file would be this fix inventing the defect it removes.
_COMMENT_SPAN = re.compile(r"<!--.*?-->")
_HTML_OPEN = re.compile(r"^ {0,3}(?:<!--|</?(pre|script|style|textarea)\b)", re.I)

# The five states a reversal condition can be read into. `NOT_READ` is the DEFAULT and
# it is a state rather than a blank, because a condition nobody has looked at and a
# condition somebody looked at and found unmet are different facts, and a table that
# renders them the same is the reassuring wrong answer this project hunts.
#
# `LOOKS_MET` is the state worth having the table for at all: something arrived that
# resembles the condition and does not satisfy it. Collapsed into a boolean it reads
# as MET, which is how a decision gets reversed by a resemblance.
MET = "MET"
NOT_MET = "NOT MET"
LOOKS_MET = "LOOKS MET, IS NOT"
UNCLOSABLE = "UNCLOSABLE"
NOT_READ = "NOT READ"

_STATES = (MET, NOT_MET, LOOKS_MET, UNCLOSABLE, NOT_READ)


@dataclass(frozen=True, kw_only=True)
class Condition:
    """One stated reversal condition, as the ADR spells it."""

    adr: str
    decision: str  # "" for a section-form condition; "D5" for an inline one.
    text: str
    spelling: str  # which of `SPELLINGS` this condition was written in.


@dataclass(frozen=True, kw_only=True)
class Adr:
    """One ADR, entirely as read from its file. Nothing here is declared."""

    number: str
    path: str
    title: str
    status: str | None  # None where the file carries no `## Status` section at all.
    status_is_qualified: bool
    decision_headings: tuple[str, ...]
    conditions: tuple[Condition, ...]


@dataclass(frozen=True, kw_only=True)
class Reading:
    """A DECLARED reading of one condition, keyed by a fragment of the condition's text.

    `anchor` must match exactly one condition of `adr`, in both directions -- so a
    reworded or deleted condition fails at import instead of leaving a reading
    attached to nothing. `date` is when the reading was taken, not when this file was
    edited; `why` is the citation, and it is the part to argue with."""

    adr: str
    anchor: str
    state: str
    date: str
    why: str


# --------------------------------------------------------------------------------
# DERIVED FROM THE FILES
# --------------------------------------------------------------------------------


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _closes_fence(line: str, fence: str) -> bool:
    """Whether `line` closes an open `fence`: same character, at least as long, bare."""
    found = _FENCE.match(line)
    return bool(
        found
        and found.group(1)[0] == fence[0]
        and len(found.group(1)) >= len(fence)
        and not found.group(2).strip()
    )


def _mask_non_prose(text: str) -> str:
    """`text` with every code fence and HTML block emptied, one blank line for each.

    NOTHING ELSE IN THIS MODULE KNOWS WHAT A FENCE OR AN HTML BLOCK IS, and the
    line-shaped rules below are only safe if the non-prose is gone before they run. Each
    of these was demonstrated against a version that did less. A fenced `# comment` read
    as a heading and ended a conditions section early, dropping every bullet after it; a
    fenced `**What reverses it:**` invented a condition nobody wrote -- 41 stated
    conditions became 40 and 42, BOTH WITH THE SUITE GREEN. Then, against the fence-only
    mask: a commented-out `### What would change this decision` in ADR 0016 published a
    condition nobody wrote, 41 to 42 with `13 passed`; and ADR 0012's third condition
    wrapped in `<!--` / `-->` deleted itself from the ADR while the page went on
    publishing it and `--check` reported *is current*.

    Emptied rather than dropped, so the masked text still holds one line per line of the
    file and every offset this module computes stays inside one consistent text."""
    out, fence, closer = [], "", ""
    for line in _COMMENT_SPAN.sub("", text).split("\n"):
        if fence or closer:
            out.append("")
            fence = "" if fence and _closes_fence(line, fence) else fence
            closer = "" if closer and closer in line.lower() else closer
            continue
        found = _FENCE.match(line)
        html = None if found else _HTML_OPEN.match(line)
        if found and not (found.group(1)[0] == "`" and "`" in found.group(2)):
            fence = found.group(1)
        elif html:
            closer = f"</{html.group(1).lower()}>" if html.group(1) else "-->"
            # CommonMark ends the block on the line that opened it when that line already
            # carries the closer. Without this, `<pre>x</pre>` or a stray `</pre>` opens a
            # block nothing ever closes and blanks the REST OF THE FILE -- this fix
            # inventing the silent deletion it was written to remove.
            closer = "" if closer in line.lower() else closer
        else:
            out.append(line)
            continue
        out.append("")
    return "\n".join(out)


def _title_of(text: str, number: str) -> str:
    """The H1, minus its `ADR NNNN --` prefix. EVERY H1, not the first.

    Refuses rather than guesses: an H1 that does not name this file's own number is a
    renumbered or copy-pasted ADR, and an index that silently prints the wrong number
    beside the right title is worse than one that will not build. A SECOND H1 is that
    same defect arriving by append, and this function's first draft could not see it --
    it read the first `# ` line and stopped, so a second `# ADR ...` glued onto the end
    of ADR 0005 changed nothing and the suite stayed green."""
    h1s = [ln for ln in text.split("\n") if ln.startswith("# ")]
    if len(h1s) != 1:
        raise ValueError(
            f"ADR {number}: the file carries {len(h1s)} H1 lines, {h1s[:3]}. An ADR has "
            "exactly one, and a second is how a copy-paste arrives"
        )
    match = re.match(r"^# ADR (\d{4})\s*[—–-]\s*(.+?)\s*$", h1s[0])
    if match is None:
        raise ValueError(f"ADR {number}: H1 {h1s[0]!r} is not `# ADR NNNN — title`")
    if match.group(1) != number:
        raise ValueError(
            f"{number}: the filename says {number} and the H1 says {match.group(1)}"
        )
    return match.group(2)


def _section(text: str, heading: str) -> str | None:
    """The body under an exact `## <heading>` line, up to the next heading of any level."""
    found = re.search(
        rf"^## {re.escape(heading)}\s*$(.*?)(?=^#{{1,6}} |\Z)", text, re.M | re.S
    )
    return None if found is None else found.group(1)


def _status_of(text: str, number: str) -> tuple[str | None, bool]:
    """(status word, whether the section carries prose beyond it).

    ABSENCE IS REPORTED AS ABSENCE. ADRs 0001, 0002 and 0003 carry no `## Status`
    section, and the honest answer is `None` rather than the `Accepted` that would be
    true of almost any ADR ever written -- a default that reports the expected value
    for any project is not a reading. Equally, a Status whose first word is outside
    the vocabulary RAISES instead of being passed through: the index must not invent a
    status, and it must not print a sentence fragment in a column readers scan."""
    body = _section(text, "Status")
    if body is None:
        return None, False
    stripped = body.strip()
    word = re.match(r"\*{0,2}([A-Za-z]+)", stripped)
    if word is None or word.group(1) not in _STATUS_WORDS:
        raise ValueError(
            f"ADR {number}: `## Status` starts {stripped[:60]!r}, whose first word is "
            f"not one of {_STATUS_WORDS}. Widen the vocabulary deliberately rather "
            "than letting the index print prose in a status column"
        )
    bare = re.fullmatch(r"\*{0,2}[A-Za-z]+\*{0,2}\.?", stripped)
    return word.group(1), bare is None


def _decision_headings(text: str) -> tuple[str, ...]:
    """Every `## Decision...` heading, verbatim and in file order.

    A bare `## Decision` and `## Decision 5 — ...` are both counted, because both are
    what the file contains. ADR 0006 has one of each -- its original Decision section
    and the later `Decision 3, resolved` that supersedes part 3 of it -- and the
    enumerated list on the page is what stops the count 2 from surprising anyone."""
    return tuple(re.findall(r"^## (Decision.*?)\s*$", text, re.M))


def _decision_spans(text: str) -> list[tuple[int, str]]:
    """(offset, label) for each numbered `## Decision N` heading, in file order."""
    return [
        (found.start(), f"D{found.group(1)}")
        for found in re.finditer(r"^## Decision (\d+)\b.*$", text, re.M)
    ]


def _inline_conditions(text: str, number: str) -> list[Condition]:
    """The `**What reverses it:**` paragraphs, attributed to the decision they sit under.

    ATTRIBUTED BY POSITION, NOT BY ORDINAL, and the difference is a real defect rather
    than a hypothetical: ADR 0018 has SEVEN numbered decisions and exactly ONE marker,
    so pairing the Nth marker with the Nth decision would file 0018's only reversal
    condition under Decision 1 by luck and would misfile every marker in any ADR that
    ever skips one."""
    marker = r"^\*\*What reverses it:\*\*\s*(.*?)(?=\n\n|\n\*\*|\Z)"
    spans = _decision_spans(text)
    out = []
    for found in re.finditer(marker, text, re.M | re.S):
        owner = [label for offset, label in spans if offset < found.start()]
        out.append(
            Condition(
                adr=number,
                decision=owner[-1] if owner else "",
                text=_flatten(found.group(1)),
                spelling=INLINE_MARKER,
            )
        )
    return out


def _section_conditions(text: str, number: str) -> list[Condition]:
    """The bullets under EVERY `### What would reverse|change this decision`.

    Stops at the next heading of ANY level, which is load-bearing for ADR 0006: its
    conditions section is immediately followed by the `#### Where the three stand`
    subsection, whose three numbered items are READINGS of those conditions and not
    three more conditions. A lookahead that only stopped at `##` counted six.

    EVERY section rather than the first, which `re.search` gave: an ADR stating conditions
    under two decisions would have published the first set and dropped the second. No ADR
    carries two sections today -- which is exactly why nothing would have noticed."""
    out: list[Condition] = []
    for found in re.finditer(
        r"^### What would (reverse|change) this decision\s*$(.*?)(?=^#{1,6} |\Z)",
        text,
        re.M | re.S,
    ):
        spelling = SECTION_REVERSE if found.group(1) == "reverse" else SECTION_CHANGE
        out.extend(
            Condition(adr=number, decision="", text=_flatten(bullet), spelling=spelling)
            for bullet in _BULLET.split(found.group(2))[1:]
            if bullet.strip()
        )
    return out


def _flatten(chunk: str) -> str:
    """One paragraph of markdown as a single line, blockquotes and nested bullets cut.

    The conditions are prose written to be read in place; the table needs one line
    each. Cutting at the first blank line keeps the condition and drops the argument
    under it, which is where the ADR is the better document anyway."""
    lines = []
    for line in chunk.split("\n"):
        if not line.strip():
            break
        if line.lstrip().startswith((">", "|", "- ", "* ", "+ ")) and lines:
            break
        lines.append(line.strip())
    return re.sub(r"\s+", " ", " ".join(lines)).strip()


def adrs_by_spelling(adrs: tuple[Adr, ...]) -> dict[str, tuple[str, ...]]:
    """Which ADRs use each recognised spelling, READ FROM THE FILES.

    The page has to state the extractor's three spellings, and its first draft named the
    ADRs using each as hand-typed ranges -- `(0006)`, `(0010-0015)`, `(0018-0020)` --
    that nothing compared to anything. That is the species this whole pair exists to
    refuse, so the lists are derived and the page prints what comes back."""
    out: dict[str, list[str]] = {spelling: [] for spelling in SPELLINGS}
    for adr in adrs:
        for condition in adr.conditions:
            if adr.number not in out[condition.spelling]:
                out[condition.spelling].append(adr.number)
    return {spelling: tuple(numbers) for spelling, numbers in out.items()}


def read_adrs() -> tuple[Adr, ...]:
    """Every ADR file, parsed -- non-prose masked first. The one source of the page."""
    out = []
    for path in sorted(_ADR_DIR.glob(_ADR_GLOB)):
        number = path.name[:4]
        text = _mask_non_prose(_read(path))
        status, qualified = _status_of(text, number)
        out.append(
            Adr(
                number=number,
                path=path.name,
                title=_title_of(text, number),
                status=status,
                status_is_qualified=qualified,
                decision_headings=_decision_headings(text),
                conditions=tuple(
                    _section_conditions(text, number) + _inline_conditions(text, number)
                ),
            )
        )
    return tuple(out)


# --------------------------------------------------------------------------------
# DECLARED, BECAUSE THE FILES DO NOT CARRY IT
# --------------------------------------------------------------------------------

# THE PHASE EACH ADR WAS WRITTEN IN, WITH THE MERGE THAT PROVES IT.
#
# NOT DERIVED AT RUN TIME, AND THE REASON IS CI RATHER THAN TASTE. The derivation is
# `git rev-list --ancestry-path <adding commit>..origin/main | tail -1`, whose branch
# name carries the phase; it needs the whole history and the `origin/main` ref. CI's
# `test` job checks out at `actions/checkout@v4`'s DEFAULT `fetch-depth: 1` -- only
# `secret-scan` sets `fetch-depth: 0` -- so a derived phase column would be
# unavailable in exactly the place the lock is supposed to bite, and a column that
# degrades quietly under a shallow clone is worse than one that is declared.
#
# So it is declared WITH ITS CITATION, re-derivable by
# `git log -1 --format=%s <merge>`, total-checked against the ADR set at import, and
# cross-checked against git by `test_the_declared_phase_is_what_git_says` wherever the
# clone is deep enough for git to answer. That test SKIPS on a shallow clone with the
# reason printed; it does not pass.
#
# AND THE ONE CASE A DECLARED SHA CANNOT COVER. An ADR written in a phase that has not
# merged yet has NO merge commit; every sha that could be typed for it would be
# invented, which is worse than saying so. `UNMERGED` says so, and
# `tests/test_adr_phase_declaration.py` checks it in both directions -- git must agree
# there is no merge, and the moment the ADR's adding commit becomes an ancestor of
# `origin/main` the declaration is stale and goes red. A sentinel that only meant "do
# not look" would be the hole this table is written against.
UNMERGED = "unmerged"

PHASES: dict[str, tuple[str, str, str]] = {
    #  adr      phase          merge      branch the merge names
    "0001": ("F1.1", "707f8c2", "f1.1-cnpj-extraction"),
    "0002": ("F1.1", "707f8c2", "f1.1-cnpj-extraction"),
    "0003": ("F1.1", "707f8c2", "f1.1-cnpj-extraction"),
    "0004": ("F1.2", "f55ed48", "f1.2-bronze-autoloader"),
    "0005": ("F1.3", "21bd469", "f1.3-estabelecimentos"),
    "0006": ("F1.3", "21bd469", "f1.3-estabelecimentos"),
    "0007": ("F1.4", "b3cbbba", "feat/f1-4-bronze-generalisation"),
    "0008": ("F1.4b", "5f53c49", "feat/f1-4b-empresas-socios"),
    "0009": ("F1.4b PR B", "44018ad", "feat/f1-4b-pr-b-second-month"),
    "0010": ("F2 wave 1", "4a79bfd", "feat/f2-wave-1-cnpj-vault"),
    "0011": ("F2 wave 1", "4a79bfd", "feat/f2-wave-1-cnpj-vault"),
    "0012": ("F2 wave 1", "4a79bfd", "feat/f2-wave-1-cnpj-vault"),
    "0013": ("F2 wave 1", "4a79bfd", "feat/f2-wave-1-cnpj-vault"),
    "0014": ("F3", "abee2bb", "feat/f3-gold-kimball"),
    "0015": ("F3", "abee2bb", "feat/f3-gold-kimball"),
    "0016": ("F-API", "0054df1", "feat/f-api-ptax"),
    "0017": ("F-DB", "43876b3", "feat/f-db-postgres-snapshot-diff"),
    "0018": ("F4", "3bd2f52", "feat/f4-dataops"),
    "0019": ("F5", "5d769a3", "feat/f5-streaming"),
    "0020": ("F6", "ef123ac", "feat/f6-rca-agent"),
    # RE-DECLARED AT ITS OWN MERGE, which is what the comment this replaces instructed:
    # "this row goes stale the moment the PR merges, and it is meant to: re-declare it
    # THEN with the merge that carried it." It read `UNMERGED` from the moment ADR 0022
    # was written until PR #36 landed, and the lock named the fix the instant it went
    # stale -- "git says 9dcb0cf ... re-declare it with the merge that brought it there".
    #
    # THE FIRST ROW IN THIS TABLE DECLARED BY ITS OWN PHASE. 0019 and 0020 were filled in
    # by F7 in 8a91c72, and that is not a precedent to follow: 8a91c72 is the commit that
    # CREATED this file, so F5 and F6 had nowhere to declare their own merges. A pattern
    # with one possible cause is not a precedent.
    "0022": ("F2w2", "9dcb0cf", "f2w2/payment-link"),
}

# THE READINGS THAT HAVE BEEN TAKEN. Everything not named here renders `NOT READ`,
# which is the honest default: a condition nobody has looked at must not print as
# `NOT MET`, because `NOT MET` is a measurement and "nobody looked" is not.
#
# NINE OF THESE ARE F7 TASK 0's, RE-KEYED TO THE CONDITION EACH ACTUALLY NAMES. Two of
# Task 0's rows did not survive that re-keying and are not invented into this table. The
# reason each was dropped is here rather than in a document to go and find: its ADR 0008
# row quotes a sentence ADR 0008 REFUTES rather than a reversal condition ADR 0008 states
# -- ADR 0008 states none -- and its ADR 0015 row reads "MET and falsified", which is a
# reading of the EXTRAPOLATION inside the condition, not of the condition.
READINGS: tuple[Reading, ...] = (
    Reading(
        adr="0006",
        anchor="Per-reason counts that are not first-match-wins",
        state=MET,
        date="2026-08-18",
        why=(
            "`src/opl/bronze/rule_overlap.py` shipped in F4 Task 3 — one aggregate pass "
            "per (table, batch) counting every rule independently, over every registered "
            "contract. ADR 0006's own `Where the three stand` item 1"
        ),
    ),
    Reading(
        adr="0006",
        anchor="A non-degenerate numerator per table",
        state=UNCLOSABLE,
        date="2026-08-18",
        why=(
            "ADR 0006's own words: *\"NOT SHIPPED, AND NO CODE CAN SHIP IT. This "
            "condition is evidentiary\"* — it asks for observations the source has to "
            "supply, and no change to this repository can supply them"
        ),
    ),
    Reading(
        adr="0006",
        anchor="The reclaim decoupling shipped first",
        state=MET,
        date="2026-08-18",
        why=(
            "F4 Task 2 wired `reclaim_landing` onto the triage path; the 2026-08-18 run "
            "freed 8,212,278,423 B (`docs/f4-run-evidence.md`, ADR 0006 line 536)"
        ),
    ),
    Reading(
        adr="0010",
        anchor="The quarantine losing its durability",
        state=NOT_MET,
        date="2026-08-30",
        why=(
            "`src/opl/bronze/retention.py` reclaims landed inner CSVs from the Volume "
            "and nothing else; it reads the quarantine only to account rows. No policy "
            "in this repository deletes a quarantine row"
        ),
    ),
    Reading(
        adr="0010",
        anchor="A source delivering deletes",
        state=LOOKS_MET,
        date="2026-08-30",
        why=(
            "F-DB produced **16 hard DELETEs** (`docs/f-db-run-evidence.md`) — but ADR "
            "0017 Decision 2 lands FULL snapshots and DERIVES the diff in the lakehouse, "
            "so no source states a departure. Absence is still inferred, which is the "
            "whole premise this condition would remove"
        ),
    ),
    Reading(
        adr="0011",
        anchor="Measuring how many `rejected_by_our_gate` keys have an OPEN window",
        state=NOT_MET,
        date="2026-08-30",
        why=(
            "still the number nobody has, and the reason given here until F7 T4 was false. "
            "`rejected_by_our_gate` is NOT witnessless: ADR 0010's own measured table gives "
            "4 rows (estabelecimentos 2026-07), 1,792 and 1,781 (socios). What had never "
            "been witnessed was the state for MERCHANT, and F7 T4 ended that too -- one "
            "row, `bad_cnpj_shape`, run 529699767706804. The condition asks for a "
            "measurement of how many such keys have an OPEN window, and nobody has taken it"
        ),
    ),
    Reading(
        adr="0012",
        anchor="A feed that must carry one of the forty",
        state=LOOKS_MET,
        date="2026-08-30",
        why=(
            "the UTF-8 feed arrived (F-DB merchant) and the project took a THIRD option "
            "neither branch of this condition names: reject at the bronze gate under "
            "`unhashable_case_divergence` (`src/opl/bronze/rules.py`). Merchant's keys "
            "are ASCII, so the divergence stays latent and the choice is still unforced"
        ),
    ),
    Reading(
        adr="0014",
        anchor="A customer entity arriving in F2 wave 2",
        state=NOT_MET,
        date="2026-08-30",
        why="F2 wave 2 is unstarted, and F7's plan §8 names it as such rather than deferring it",
    ),
    Reading(
        adr="0015",
        anchor="A dimension large enough that drop-and-rebuild stops being affordable",
        state=NOT_MET,
        date="2026-08-13",
        why=(
            "the opposite of met, and the ADR was amended in place to say so: "
            "`dim_company`'s 69,202,818 rows **built in 120 s** on Free Edition "
            "serverless, against this bullet's own 2,000-6,000 s extrapolation, which "
            "the ADR records as falsified. A rebuild costs two minutes"
        ),
    ),
    Reading(
        adr="0020",
        anchor="Unity Catalog lineage becoming readable and complete",
        state=LOOKS_MET,
        date="2026-08-29",
        why=(
            "lineage IS readable — `system.access.table_lineage`, **3,327 rows and 72 "
            "distinct `target_table_full_name`** (the COLUMN matters: F7 re-measured "
            "3,340 rows, 72 distinct `target_table_full_name` and **67** distinct "
            "`target_table_name`, and reading those two against each other as a drop "
            "nearly published a retention finding that was two columns), 2026-07-24 to "
            "2026-08-28, statement `01f1a4c2-1cfb-115a-a947-5a2fbc1aec10` — and it "
            "carries the hard "
            "`bronze_payments -> fact_payment` edge this decision said a vault-path walk "
            "would miss. It is not COMPLETE: it records EXECUTIONS (`event_time`) while "
            "the manifest states STRUCTURE, so it answers *nothing downstream* for a "
            "table whose loader has not run inside the retention window"
        ),
    ),
    Reading(
        adr="0022",
        anchor="a workspace that will launch a run",
        state=NOT_MET,
        date="2026-09-02",
        why=(
            "taken by the F8 session and RE-STATED here rather than inherited silently, "
            "because it is the condition that decides whether F2 wave 2 can ever close: "
            "`bundle deploy` creating a NEW job answers **403 `PERMISSION_DENIED`** and "
            "`jobs/run-now` is refused, while reads, `jobs/update` and a deploy over "
            "resources that ALREADY EXIST all still work — so the refusal is about "
            "LAUNCHING and not about deploying. Last run terminating `SUCCESS` in this "
            "workspace: **2026-08-28T18:32:13Z**. That is exactly the half protocol §9's "
            "conditions 1 and 4 need, so the phase ships unclosed and says so"
        ),
    ),
)


# --------------------------------------------------------------------------------
# THE GUARDS. Run at import, so nothing renders over a declaration that has rotted.
# --------------------------------------------------------------------------------


def _assert_the_sweep_found_the_adr_set(adrs: tuple[Adr, ...]) -> None:
    """GUARD THE GUARD: every assertion below passes trivially over zero ADRs.

    A glob that stopped matching, a `cwd` pointing elsewhere or a moved directory
    would each render a clean, empty, wrong page. A floor rather than an exact count,
    because an exact count is itself a claim that goes stale on the next ADR -- which
    is the species this file exists to catch, and committing it inside the guard would
    be the joke told twice."""
    if len(adrs) < 20:
        raise ValueError(
            f"the ADR sweep found {len(adrs)} files under {_ADR_DIR} matching "
            f"{_ADR_GLOB!r}; there were twenty at F7. The paths are wrong, not the ADRs"
        )
    numbers = [adr.number for adr in adrs]
    if len(set(numbers)) != len(numbers):
        raise ValueError(f"two ADR files claim the same number: {sorted(numbers)}")


def _assert_every_adr_declares_a_phase(adrs: tuple[Adr, ...]) -> None:
    """TOTAL over the ADR set, in both directions.

    This is the assertion that makes the index un-rottable in the direction that
    matters: a twenty-first ADR arrives with no phase and the generator REFUSES,
    instead of rendering nineteen rows and a silence. The reverse half matters too --
    a declaration for a deleted or renumbered ADR outlives its subject exactly the way
    `test_size_caps.py`'s allow-list is written not to."""
    declared, found = set(PHASES), {adr.number for adr in adrs}
    if declared != found:
        raise ValueError(
            f"the phase declaration is not total over docs/adr/: undeclared "
            f"{sorted(found - declared)}, declared but absent {sorted(declared - found)}. "
            "Add the ADR's phase with the merge commit that proves it"
        )


def _assert_every_reading_anchors_to_exactly_one_condition(adrs: tuple[Adr, ...]) -> None:
    """Each declared reading names a condition that is still there, and only one.

    EXACT IN BOTH DIRECTIONS. A reworded condition drops its anchor to zero matches; a
    condition split in two takes it to more than one. Either way the reading has
    stopped meaning what it said, and it fails here rather than sitting on the page
    beside prose it no longer describes."""
    by_adr = {adr.number: adr.conditions for adr in adrs}
    for reading in READINGS:
        if reading.state not in _STATES:
            raise ValueError(f"{reading.adr}: state {reading.state!r} is not one of {_STATES}")
        hits = [c for c in by_adr.get(reading.adr, ()) if reading.anchor in c.text]
        if len(hits) != 1:
            raise ValueError(
                f"ADR {reading.adr}: the reading anchored on {reading.anchor!r} matches "
                f"{len(hits)} of its {len(by_adr.get(reading.adr, ()))} stated conditions. "
                "The condition was reworded, split or deleted -- re-take the reading "
                "rather than re-pointing the anchor"
            )


def _assert_no_condition_carries_two_readings(adrs: tuple[Adr, ...]) -> None:
    """THE OTHER DIRECTION, and without it `readings_for` loses a row in silence.

    The guard above is exact from the READING's side: one reading, exactly one condition.
    It says nothing from the CONDITION's side, and `readings_for` is a dict keyed by
    condition text -- so a second, later reading of a condition already read overwrites
    the first, drops its state from the published table and makes *"N have been read"*
    under-report, with every test green. A reading log that grows a second reading of one
    condition is the ordinary way this table evolves; it fails here instead."""
    for adr in adrs:
        for condition in adr.conditions:
            hits = [
                r for r in READINGS if r.adr == adr.number and r.anchor in condition.text
            ]
            if len(hits) > 1:
                raise ValueError(
                    f"ADR {adr.number}: {len(hits)} readings anchor on the single "
                    f"condition {condition.text[:60]!r} -- {[r.anchor for r in hits]}. "
                    "Only the last would reach the page. Re-anchor them, or replace the "
                    "earlier reading rather than adding beside it"
                )


def readings_for(adr: Adr) -> dict[str, Reading]:
    """The declared reading of each of this ADR's conditions, by condition text.

    At most one per condition: `_assert_no_condition_carries_two_readings` refuses a
    second at import, because this mapping would silently keep only the last."""
    return {
        condition.text: reading
        for reading in READINGS
        if reading.adr == adr.number
        for condition in adr.conditions
        if reading.anchor in condition.text
    }


# READ AND CHECKED AT IMPORT, the shape `src/opl/dataops/cadence.py` uses. Nothing in
# this repository may render a page over a declaration that has stopped matching its
# subject, and the loudest place to find that out is before any output exists.
ADRS: tuple[Adr, ...] = read_adrs()

_assert_the_sweep_found_the_adr_set(ADRS)
_assert_every_adr_declares_a_phase(ADRS)
_assert_every_reading_anchors_to_exactly_one_condition(ADRS)
_assert_no_condition_carries_two_readings(ADRS)
