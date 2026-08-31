# tests/test_adr_index.py
"""`docs/adr/README.md` against the ADR files, RE-DERIVED rather than re-generated.

WHAT ELSE WOULD PRODUCE THIS GREEN, which is ADR 0018's standing instruction turned on
this file. Import the generator, call `render()`, compare, and green means either the index
is right OR the generator is wrong in the same way twice. That test IS here, as
`test_the_index_on_disk_is_byte_identical_to_what_the_generator_renders`, but it is
deliberately not the lock; its own docstring says why.

SO EVERY DERIVED FACT IS READ OUT OF THE ADR FILES AGAIN, BY CODE THAT SHARES ONE THING
WITH THE GENERATOR AND NAMES IT. `_independent_read` walks the file top to bottom carrying
the current `##` section as state, where the generator matches a multi-line regex per fact.
`test_every_condition_the_index_quotes_is_in_the_adr_it_names` asks a third question --
whether the rendered condition survives as a contiguous run of words in the ADR -- immune
to the generator's flattening. Where the two disagree, the test names the fact and the ADR.

THE ONE SHARED THING IS THE MASK, AND AN EARLIER DRAFT OF THIS FILE CLAIMED OTHERWISE.
`_prose_lines` is `adr_index._mask_non_prose` typed again -- same regexes, same predicates,
same state, same order -- so the two CANNOT disagree about a fence or an HTML block, EVEN
WHERE BOTH ARE WRONG. Duplicated rather than imported so this file goes on reading the ADR
files when `scripts/adr_index.py` REFUSES to import; a one-sided edit runs into
`test_the_two_maskings_agree_over_the_battery`, and that battery is the only place the
mask's own behaviour is asserted.

AND THE COMPARISON IS PROVED CAPABLE OF FAILING, in the tree, on every run: each arm below
mutates an index in memory and asserts `_disagreements` names that one mutation. They were
also run against the TREE, once, in F7 T1 -- a twenty-first ADR written to `docs/adr/`, ADR
0001's H1 renamed, ADR 0004's Status set to `Deprecated`, ADR 0012's second condition
reworded. Each turned this file red, each was reverted by inverse substitution and proved
identical by sha256, and the status run found the first draft of
`test_the_comparison_goes_red_when_a_status_changes` satisfiable by any row.

AND AGAIN IN EACH CORRECTION PASS, over edits that were GREEN against the draft before
them. Three against the unmasked reading -- ADR 0012's `- Spark exposing...` respelled
`* Spark exposing...`, a fence opened inside a conditions section, a fence carrying a
`**What reverses it:**` line -- took 41 stated conditions to 40, 40 and 42, silently. Three
more against the fence-only mask, all HTML: a commented-out
`### What would change this decision` on ADR 0016 published a condition nobody wrote (41 to
42, `13 passed`); ADR 0012's third condition wrapped in `<!--` / `-->` left the ADR stating
two while the page went on publishing three and `--check` said *is current*; a commented-
out `# TODO` on ADR 0005 refused to build at all, diagnosing *"the file carries 2 H1
lines"* over a document that renders one.

FIVE OF THE SIX ARE INERT NOW -- the page byte-identical, the tally unmoved -- the right
outcome for markup that renders as nothing. THE SIXTH IS NOT, AND MUST NOT BE: commenting a
bullet out really does delete it, so the page has to move, and
`test_the_reversal_table_holds_a_row_for_every_condition_every_adr_states` reports
`{'0012': (2, 3)}` -- the silent deletion turned into a named one.

WHAT IS DECLARED RATHER THAN DERIVED, AND THEREFORE NOT LOCKED HERE. The PHASE column and
the reversal READINGS cannot be read out of the ADRs; `scripts/adr_index.py` asserts both
are TOTAL over the ADR set at import. The declared merge sha and branch are re-derived
from git by `test_the_declared_phase_is_what_git_says`, which SKIPS in CI, where
`actions/checkout@v4` runs at its default `fetch-depth: 1`. The phase LABEL against the
declared branch needs no git and does run there, in
`test_a_phase_label_is_not_matched_by_a_branch_naming_its_sub_phase`.
"""
from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
_ADR_DIR = _REPO / "docs" / "adr"
_INDEX = _ADR_DIR / "README.md"

_STATUS_WORDS = ("Accepted", "Proposed", "Rejected", "Deprecated", "Superseded")

# The same grammars `scripts/adr_index.py` states, spelled again here rather than imported
# -- the module docstring carries why, and what it costs. A fence: up to three leading
# spaces, then three or more backticks or tildes. A bullet: the same allowance and all
# four markers markdown accepts. The HTML: a comment, and CommonMark's four type-1 tags.
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
_BULLET = re.compile(r"^ {0,3}(?:\d+\.|[-*+])\s+\S")
_COMMENT_SPAN = re.compile(r"<!--.*?-->")
_HTML_OPEN = re.compile(r"^ {0,3}(?:<!--|</?(pre|script|style|textarea)\b)", re.I)

# The index table's own heading, matched as a SHAPE so the count inside it can be
# compared against the ADR files. It was the literal `## The twenty` and nothing here
# could see that it had stopped being true.
_HEADING = re.compile(r"^All (\d+) ADRs$")


def _load(name: str, filename: str):
    """One script, loaded by path and REGISTERED under `name` before it executes.

    Registered first for two reasons that both bite silently: `dataclasses` resolves
    `KW_ONLY` through `sys.modules[cls.__module__]` and raises on a module that is not
    there yet, and `generate_adr_index.py` does `import adr_index`, which would otherwise
    load a SECOND copy whose declarations could drift from the one these tests assert on.

    LOADED INSIDE THE TESTS THAT NEED IT, NEVER AT MODULE SCOPE, and that is not style.
    `scripts/adr_index.py` REFUSES AT IMPORT when an ADR has no declared phase. At module
    scope that refusal becomes a collection error and takes the file-reading tests down
    with it, so the run reports *"could not import"* where it should report *"0021 has no
    row in the index"* -- and it is why the mask is duplicated rather than imported."""
    spec = importlib.util.spec_from_file_location(name, _REPO / "scripts" / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------------
# THE SECOND READING. Shares the generator's MASK, by construction, and nothing else.
# --------------------------------------------------------------------------------


def _adr_paths() -> list[Path]:
    return sorted(_ADR_DIR.glob("0*.md"))


def _prose_lines(text: str) -> list[str]:
    """`text` as lines, with every code fence and every HTML block blanked out.

    NEITHER READING KNOWS MARKDOWN, so the non-prose has to go before either of them runs
    or both are wrong in the same place -- which is agreement, not a check. Demonstrated
    against the versions that did less: a fenced `# comment` ends a conditions section
    early, a fenced `**What reverses it:**` invents a condition, and an HTML comment did
    both again plus refused the whole build over an H1 that renders as nothing.

    NOT AN INDEPENDENT IMPLEMENTATION, WHICH THIS DOCSTRING ONCE CLAIMED. It is
    `adr_index._mask_non_prose` typed again -- same regexes, same predicates, same state,
    same order; only the return type differs. The two cannot disagree about a fence or an
    HTML block, and their blind spots are the same. The module docstring carries what the
    duplication buys and what checks it."""
    out, fence, closer = [], "", ""
    for line in _COMMENT_SPAN.sub("", text).split("\n"):
        found = _FENCE.match(line)
        if fence or closer:
            out.append("")
            closes = found and found.group(1)[0] == fence[:1] and not found.group(2).strip()
            if fence and closes and len(found.group(1)) >= len(fence):
                fence = ""
            elif closer and closer in line.lower():
                closer = ""
            continue
        opening = found and not (found.group(1)[0] == "`" and "`" in found.group(2))
        html = None if found else _HTML_OPEN.match(line)
        if opening:
            fence = found.group(1)
        elif html:
            closer = f"</{html.group(1).lower()}>" if html.group(1) else "-->"
            # Closed on its own line -- `<pre>x</pre>`, or a stray `</pre>`. Without this
            # the block runs to the end of the file and deletes it.
            closer = "" if closer in line.lower() else closer
        else:
            out.append(line)
            continue
        out.append("")
    return out


def _title_after_dash(line: str) -> str:
    """The H1's title half, sliced at its first dash. NAMED WHEN THERE IS NO DASH.

    THE TWO READINGS PICK THE DASH DIFFERENTLY, ON PURPOSE, AND IT IS LEFT THAT WAY.
    `scripts/adr_index.py` takes whichever of `[—–-]` sits immediately after the ADR
    number; this one takes the first em dash, then en dash, then ` - `, wherever it falls.
    On an H1 carrying two kinds they would differ -- and that arrives as a NAMED title
    disagreement out of `_disagreements`, never as silence. Spelling them the same would
    buy agreement, not a check. An earlier draft here sliced on an em dash alone and raised
    `IndexError` on the others: a crash, in the one file whose job is to name a
    disagreement."""
    for dash in ("—", "–", " - "):
        _head, separator, rest = line.partition(dash)
        if separator:
            return rest.strip()
    return f"<H1 with no dash separator: {line!r}>"


def _independent_read(path: Path) -> dict[str, object]:
    """One ADR's title, `## Decision` headings and `## Status` body, by a line scan.

    DELIBERATELY NOT THE GENERATOR'S SHAPE. It walks the file top to bottom carrying
    the current section as a piece of state, and slices the H1 on its FIRST dash --
    where the generator matches a multi-line regex per fact and captures groups. Two
    readings of one file, so a defect in either spelling shows up as a disagreement
    rather than as agreement with itself."""
    title, section, decisions, status = None, None, [], []
    seen_status_heading = False
    for line in _prose_lines(path.read_text(encoding="utf-8")):
        if title is None and line.startswith("# ADR "):
            title = _title_after_dash(line)
        elif line.startswith("#") and " " in line:
            section = line.lstrip("#").strip()
            if section.startswith("Decision"):
                decisions.append(section)
            seen_status_heading = seen_status_heading or line.strip() == "## Status"
        elif section == "Status":
            status.append(line)
    return {
        "title": title,
        "status_body": "\n".join(status).strip(),
        "has_status_section": seen_status_heading,
        "decisions": tuple(decisions),
    }


def _expected_status_cell(read: dict[str, object]) -> str:
    """What the status column must say, spelled independently of the renderer.

    Absence renders as absence. A Status body whose first word is outside the vocabulary
    renders as the body itself, which will not match any cell the generator writes --
    correctly, because the generator raises on that case and the two must not silently
    agree on a value neither of them should produce."""
    if not read["has_status_section"]:
        return "**no `## Status`**"
    body = str(read["status_body"])
    word = re.match(r"\*{0,2}([A-Za-z]+)", body)
    if word is None or word.group(1) not in _STATUS_WORDS:
        return f"<unparseable status: {body[:40]!r}>"
    bare = re.fullmatch(r"\*{0,2}[A-Za-z]+\*{0,2}\.?", body)
    return word.group(1) if bare else f"{word.group(1)}, qualified"


# --------------------------------------------------------------------------------
# THE INDEX, PARSED AS TEXT. The generator is not consulted.
# --------------------------------------------------------------------------------


def _cells(row: str) -> list[str]:
    """A markdown table row's cells. Splits on UNESCAPED pipes only."""
    return [cell.strip() for cell in re.split(r"(?<!\\)\|", row)[1:-1]]


def _rows_under(text: str, heading: str) -> list[list[str]]:
    """Table rows under one `## <heading>`, and NOT under any other.

    SCOPED BY SECTION rather than by the shape of the link in column one, a defect this
    file found in its own first draft: both tables open their rows with
    `| [0006](0006-....md) |`, so a link-shaped match read every reversal row as an index
    row and reported every condition count one too high. A parser that identifies a table
    by what its cells look like will eventually find another table that looks like it."""
    section, rows = None, []
    for line in _prose_lines(text):
        if line.startswith("## "):
            section = line[3:].strip()
        elif section == heading and line.startswith("| [") and "]" in line:
            rows.append(_cells(line))
    return rows


def _index_heading(text: str) -> str | None:
    """The index table's `## All N ADRs` heading, or `None` if there is not exactly one.

    THE NUMBER IN IT IS THE POINT, and `_disagreements` compares it to the ADR files.
    `None` rather than a raise, so a page that lost the heading arrives as a named
    disagreement beside every other one instead of as an error that hides them."""
    headings = [line[3:].strip() for line in _prose_lines(text) if line.startswith("## ")]
    named = [heading for heading in headings if _HEADING.fullmatch(heading)]
    return named[0] if len(named) == 1 else None


def _index_rows(text: str) -> dict[str, list[str]]:
    """The index table, keyed by ADR number. Empty when the heading is unrecognisable."""
    heading = _index_heading(text)
    if heading is None:
        return {}
    return {re.match(r"\[(\d{4})\]", cells[0]).group(1): cells  # type: ignore[union-attr]
            for cells in _rows_under(text, heading)}


def _reversal_rows(text: str) -> list[tuple[str, str]]:
    """(ADR number, condition text) for every row of the reversal table."""
    return [
        (re.match(r"\[(\d{4})", cells[0]).group(1), cells[1].replace("\\|", "|"))  # type: ignore[union-attr]
        for cells in _rows_under(text, "Reversal conditions")
    ]


def _normalise(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _disagreements(index_text: str, paths: list[Path]) -> list[str]:
    """Every way the index and the ADR files fail to say the same thing.

    ONE FUNCTION, used by the real comparison AND by every failure arm, so each arm
    exercises the code that guards the tree rather than a copy of it."""
    rows = _index_rows(index_text)
    found = []
    heading = _index_heading(index_text)
    if heading is None:
        found.append("the index carries no single `## All N ADRs` heading over its table")
    elif int(_HEADING.fullmatch(heading).group(1)) != len(paths):  # type: ignore[union-attr]
        found.append(
            f"the index heading says `{heading}` and docs/adr/ holds {len(paths)} ADR files"
        )
    for path in paths:
        number = path.name[:4]
        read = _independent_read(path)
        if number not in rows:
            found.append(f"{number}: the ADR exists and the index has no row for it")
            continue
        cells = rows[number]
        if cells[1] != str(read["title"]).replace("|", "\\|"):
            found.append(f"{number}: index title {cells[1]!r} != file title {read['title']!r}")
        if cells[3] != _expected_status_cell(read):
            found.append(
                f"{number}: index status {cells[3]!r} != {_expected_status_cell(read)!r}"
            )
        if cells[4] != str(len(read["decisions"])):
            found.append(
                f"{number}: index says {cells[4]} `## Decision` sections, the file has "
                f"{len(read['decisions'])}"
            )
    orphans = sorted(set(rows) - {p.name[:4] for p in paths})
    found.extend(f"{n}: the index has a row and no ADR file carries that number" for n in orphans)
    return found


# --------------------------------------------------------------------------------
# THE TESTS
# --------------------------------------------------------------------------------


def test_the_second_reading_is_reading_the_adrs_and_not_an_empty_tree():
    """GUARD THE GUARD. Every comparison below passes trivially over zero ADRs.

    A moved directory, a glob that stopped matching or a `cwd` pointing elsewhere would
    each report agreement between two empty sets. Floors rather than exact counts: an
    exact count is a claim that goes stale on the next ADR, which is the species this
    whole file exists to catch."""
    paths = _adr_paths()
    assert len(paths) >= 20, f"only {len(paths)} ADR files found under {_ADR_DIR}"
    reads = [_independent_read(path) for path in paths]
    assert all(read["title"] for read in reads), "an ADR was read with no title at all"
    assert sum(len(read["decisions"]) for read in reads) >= 20
    assert sum(1 for read in reads if read["has_status_section"]) >= 15
    assert _INDEX.exists(), "the index has never been generated"
    text = _INDEX.read_text(encoding="utf-8")
    assert len(_index_rows(text)) >= len(paths)
    # The reversal floor is DERIVED. It was the literal `>= 40` against 41 actual, so
    # losing exactly one condition cleared it -- a floor one below the number it guards
    # is not a floor. The `>= 20` beside it stays typed on purpose: it is the
    # non-emptiness claim this whole test is, and the line under it is the detector.
    scanned = sum(_scan_conditions(path) for path in paths)
    assert scanned >= 20, f"the condition scan found {scanned} across {len(paths)} ADRs"
    assert len(_reversal_rows(text)) >= scanned, (
        f"the index renders {len(_reversal_rows(text))} reversal rows against {scanned} "
        "conditions the scan finds in the ADR files"
    )


def test_the_index_says_what_the_adr_files_say():
    """THE LOCK. Title, status and decision structure, re-derived and compared.

    A new ADR with no row, a renamed title, a changed status, a decision section added
    or removed -- each shows up here, named, without the generator being consulted."""
    disagreements = _disagreements(_INDEX.read_text(encoding="utf-8"), _adr_paths())
    assert not disagreements, (
        f"{disagreements}. Regenerate with `uv run python scripts/generate_adr_index.py` "
        "-- and if regenerating does not fix it, one of the two readings of the ADR is "
        "wrong. Which one is not decided here: this file's reading is a line scan and the "
        "generator's is a regex capture, and the disagreement above names the fact to go "
        "and read for yourself"
    )


def test_the_index_names_every_adr_that_carries_no_status_section():
    """F7 plan §5 prediction 3, pinned so it cannot quietly stop being true.

    Three ADRs -- 0001, 0002 and 0003 -- carry no `## Status` section, and the index must
    SAY SO rather than print the `Accepted` that would be true of almost any ADR ever
    written. Asserted as a set derived from the files, not as the literal three: adding a
    Status to one of them is a legitimate change that this test then follows, while an
    index that stopped marking an ADR that still has none fails."""
    without = {
        p.name[:4] for p in _adr_paths() if not _independent_read(p)["has_status_section"]
    }
    assert without, "no ADR lacks a `## Status` section; prediction 3's premise is gone"
    rows = _index_rows(_INDEX.read_text(encoding="utf-8"))
    marked = {number for number, cells in rows.items() if cells[3] == "**no `## Status`**"}
    assert marked == without, (
        f"the index marks {sorted(marked)} as carrying no `## Status`; the files say "
        f"{sorted(without)}"
    )


def test_every_condition_the_index_quotes_is_in_the_adr_it_names():
    """A THIRD QUESTION, asked so a reworded condition cannot survive re-rendering.

    Not "does the extractor produce this string" -- that is the generator asking itself.
    Whitespace-normalised containment: the rendered condition must survive as a
    contiguous run of words in the ADR. Immune to the generator's flattening rules, and
    it goes red the moment an ADR's condition is edited and the index is not rebuilt."""
    sources = {path.name[:4]: _normalise(path.read_text(encoding="utf-8")) for path in _adr_paths()}
    missing = [
        (number, condition[:70])
        for number, condition in _reversal_rows(_INDEX.read_text(encoding="utf-8"))
        if _normalise(condition) not in sources[number]
    ]
    assert not missing, (
        f"{missing} appear in the index's reversal table and not in the ADR they name. "
        "The condition was reworded and the index was not regenerated"
    )


def test_the_reversal_table_holds_a_row_for_every_condition_every_adr_states():
    """TOTAL over the conditions, counted by a scan that is not the generator's.

    Markers and section bullets are counted by walking lines; the generator splits a
    captured section with a regex. A condition added to an ADR and absent from the table
    is exactly the *"hand-maintained list nobody updates"* this page exists to refuse."""
    counted = {}
    for path in _adr_paths():
        counted[path.name[:4]] = _scan_conditions(path)
    rendered: dict[str, int] = {}
    for number, _condition in _reversal_rows(_INDEX.read_text(encoding="utf-8")):
        rendered[number] = rendered.get(number, 0) + 1
    wrong = {
        number: (counted[number], rendered.get(number, 0))
        for number in counted
        if counted[number] != rendered.get(number, 0)
    }
    assert not wrong, (
        f"{wrong} -- (conditions in the ADR, rows in the index). The extractor's three "
        "recognised spellings are named in `scripts/adr_index.py`"
    )


def _scan_conditions(path: Path) -> int:
    """Conditions in one ADR, by a line walk: section bullets plus inline markers.

    NON-PROSE IS BLANKED FIRST and the bullet grammar is markdown's four markers with
    CommonMark's three-space allowance, because the first draft was neither: it read `-`
    and `N.` at column zero and knew no fences, so ADR 0012's condition respelled with `*`
    vanished and a fence in a conditions section swallowed the rest, both with this file
    green. An HTML comment then did the same again."""
    total, in_section = 0, False
    for line in _prose_lines(path.read_text(encoding="utf-8")):
        if line.startswith("**What reverses it:**"):
            total += 1
        elif line.startswith("#") and " " in line:
            in_section = line.lstrip("#").strip() in (
                "What would reverse this decision",
                "What would change this decision",
            )
        elif in_section and _BULLET.match(line):
            total += 1
    return total


def test_the_index_is_lf_only_and_ends_in_exactly_one_newline():
    """`pathlib.write_text` emits CRLF on Windows; this page is written LF-only.

    A page regenerated on the wrong platform would otherwise arrive as a whole-file diff
    with no content change in it, which is the diff nobody reads. ABOUT THIS FILE ONLY:
    `git ls-files --eol docs/adr/` shows ADR bodies carrying CRLF in the working tree
    while their committed blobs are LF, which `core.autocrlf=input` keeps out of
    `git diff`. Nothing here converts them.

    The count in the message was `body.count(b'chr')` -- the three literal bytes `c`, `h`,
    `r` -- so the only failure this assertion could report was "0 CR bytes", which
    contradicts the assertion that produced it."""
    body = _INDEX.read_bytes()
    carriage_returns = body.count(b"\r")
    assert carriage_returns == 0, f"the index carries {carriage_returns} CR bytes"
    assert body.endswith(b"\n") and not body.endswith(b"\n\n")


def test_the_index_on_disk_is_byte_identical_to_what_the_generator_renders():
    """STALENESS, and it is the WEAK test on this page -- see the module docstring.

    It shares the generator's whole code path, so it reports green whenever the file was
    regenerated, including from a generator that reads the ADRs wrongly. It is here
    because "somebody edited the page by hand" and "somebody added an ADR and forgot"
    are both real, and neither is caught by the derivation tests when the hand edit
    happens to agree with the files. The lock is the tests above."""
    renderer = _load("generate_adr_index", "generate_adr_index.py")
    assert renderer.render().encode("utf-8") == _INDEX.read_bytes(), (
        "docs/adr/README.md is stale. Run `uv run python scripts/generate_adr_index.py`"
    )


def test_every_declared_reading_still_anchors_to_a_condition_that_exists():
    """The import-time guard, named so its failure is legible in a test report.

    `scripts/adr_index.py` runs this at import, so a broken anchor already refuses to
    render. Restating it here means the failure arrives as a named test rather than as a
    collection error, and it pins the property against someone loosening the guard."""
    idx = _load("adr_index", "adr_index.py")
    idx._assert_every_reading_anchors_to_exactly_one_condition(idx.ADRS)
    idx._assert_every_adr_declares_a_phase(idx.ADRS)
    assert {r.state for r in idx.READINGS} <= set(idx._STATES)


def _phase_tokens(text: str) -> list[str]:
    """A phase label or a branch name as its lowercased alphanumeric runs."""
    return re.findall(r"[a-z0-9]+", text.lower())


def _branch_names_phase(phase: str, branch: str) -> bool:
    """Whether `branch`'s tokens carry `phase`'s, contiguously and in order.

    TOKENS RATHER THAN A FLATTENED STRING, because flattening cannot separate a phase from
    its own sub-phase and this table holds exactly that pair. The first spelling stripped
    both to `[a-z0-9]` and asked for substring containment, so `F1.4` -> `f14` was found
    inside `feat/f1-4b-pr-b-second-month` and `F1.4b` -> `f14b` was found inside
    `feat/f1-4-bronze-generalisation` -- the `b` coming from `bronze`. It rejected `F9`,
    which is a phase from another universe, and accepted the only confusion available
    here. Split on the separators, `f1`, `4` and `4b` stay distinct tokens and both
    swaps fail."""
    want, have = _phase_tokens(phase), _phase_tokens(branch)
    return any(have[at:at + len(want)] == want for at in range(len(have) - len(want) + 1))


def test_the_declared_phase_is_what_git_says():
    """The declared merge sha and branch against GIT. This half does not run in CI.

    ALL THREE DECLARED FACTS, SEPARATELY. The first draft asked
    `if merge not in subject and branch not in subject` -- an OR, so the branch name alone
    satisfied it and the declared sha went unchecked whenever the branch was right, while
    the phase label was compared to nothing at all. `("F99", "0000000",
    "feat/f6-rca-agent")` was declared for an ADR and this test reported green.

    The sha and the branch are checked against git's own merge subject; the PHASE label is
    checked against that branch, because git carries no other record of it. That third
    check is a declaration against a declaration, and it is worth having because the
    branch on the other side is the one git just confirmed -- it also needs no git, so
    `test_a_phase_label_is_not_matched_by_a_branch_naming_its_sub_phase` runs it in CI.

    CI's `test` job uses `actions/checkout@v4` at its default `fetch-depth: 1`, so
    `git log --diff-filter=A` has nothing to walk. It SKIPS with the reason rather than
    passing over an empty history, which would be a check reporting the expected value
    because it could not look -- the exact defect ADR 0018 names."""
    if not _history_is_deep():
        pytest.skip(
            "shallow clone or no origin/main: the phase derivation needs full history. "
            "This is CI's default checkout, so this arm does not run there"
        )
    idx = _load("adr_index", "adr_index.py")
    wrong = []
    for path in _adr_paths():
        number = path.name[:4]
        phase, merge, branch = idx.PHASES[number]
        subject = _merge_subject_for(path)
        if merge not in subject:
            wrong.append(f"{number}: declared merge {merge}; git says {subject!r}")
        if branch not in subject:
            wrong.append(f"{number}: declared branch {branch}; git says {subject!r}")
        if not _branch_names_phase(phase, branch):
            wrong.append(f"{number}: declared phase {phase!r} is not named by {branch!r}")
    assert not wrong, wrong


def _history_is_deep() -> bool:
    """Whether git here can answer `which merge brought this file to main`."""
    probe = subprocess.run(
        ["git", "rev-parse", "--is-shallow-repository"],
        cwd=_REPO, capture_output=True, text=True,
    )
    if probe.returncode or probe.stdout.strip() != "false":
        return False
    ref = subprocess.run(
        ["git", "rev-parse", "--verify", "origin/main"],
        cwd=_REPO, capture_output=True, text=True,
    )
    return ref.returncode == 0


def _merge_subject_for(path: Path) -> str:
    """`<merge sha> <subject>` of the merge that first brought this ADR to `origin/main`."""
    added = subprocess.run(
        ["git", "log", "--diff-filter=A", "--format=%H", "--", str(path.relative_to(_REPO))],
        cwd=_REPO, capture_output=True, text=True, check=True,
    ).stdout.split()
    if not added:
        return "<never added on this branch>"
    merges = subprocess.run(
        ["git", "rev-list", "--ancestry-path", "--merges", f"{added[-1]}..origin/main"],
        cwd=_REPO, capture_output=True, text=True, check=True,
    ).stdout.split()
    if not merges:
        return "<no merge; committed straight onto main>"
    return subprocess.run(
        ["git", "log", "-1", "--format=%h %s", merges[-1]],
        cwd=_REPO, capture_output=True, text=True, check=True,
    ).stdout.strip()


# --------------------------------------------------------------------------------
# THE FAILURE ARMS. A comparison whose red has never been seen is not a comparison.
# --------------------------------------------------------------------------------


def test_the_comparison_goes_red_when_a_title_is_renamed():
    """Rename an ADR in the INDEX and the file stops agreeing with it."""
    text = _INDEX.read_text(encoding="utf-8")
    mutated = text.replace("Dual-target version pinning", "Dual-target version pinnning", 1)
    assert mutated != text, "the fixture string is gone; re-point this arm"
    found = _disagreements(mutated, _adr_paths())
    assert any("0001" in message and "title" in message for message in found), found


def test_the_comparison_goes_red_when_a_status_changes():
    """A status the index prints and the ADR does not say.

    NAMED TO ONE ROW, and the first draft was not: it replaced the first
    `| Accepted | 1 |` it found anywhere and asserted only that SOME message mentioned a
    status. Running the on-disk arm -- ADR 0004's own Status edited to `Deprecated` --
    turned that arm red, because the mutation it makes and the mutation on disk cancelled
    each other and no status disagreement was left. An arm that can be satisfied by
    somebody else's defect, or silenced by one, is not measuring its own."""
    text = _INDEX.read_text(encoding="utf-8")
    mutated = "\n".join(
        _with_status(line, "Proposed") if line.startswith("| [0004](") else line
        for line in text.split("\n")
    )
    assert mutated != text, "ADR 0004 has no index row; re-point this arm"
    found = _disagreements(mutated, _adr_paths())
    assert any("0004" in message and "status" in message for message in found), found


def _with_status(row: str, status: str) -> str:
    """One index row with its status cell replaced. Cell 3, counted, not matched."""
    cells = re.split(r"(?<!\\)\|", row)
    cells[4] = f" {status} "
    return "|".join(cells)


def test_the_comparison_goes_red_when_the_heading_count_stops_matching_the_files():
    """THE HEADING THE FIRST DRAFT TYPED, which is this page's own subject turned on it.

    `## The twenty` was a literal in the generator. A twenty-first ADR moved every
    computed number on the page -- the row count, the conditions tally, the "N of the M
    ADRs" line -- and left the heading saying twenty, with nothing here able to see it.
    The pass/skip counts once quoted here were from an older draft of this file and did
    not reproduce, so they are deleted rather than re-measured. The count is rendered from
    `len(adrs)` now and compared to the ADR files here; this arm decrements it and asserts
    the comparison notices."""
    text = _INDEX.read_text(encoding="utf-8")
    heading = _index_heading(text)
    assert heading is not None, "the index has no `## All N ADRs` heading; re-point this arm"
    declared = int(_HEADING.fullmatch(heading).group(1))
    mutated = text.replace(f"## {heading}", f"## All {declared - 1} ADRs", 1)
    assert mutated != text, "the heading did not substitute; re-point this arm"
    found = _disagreements(mutated, _adr_paths())
    assert any("heading says" in message for message in found), found


# --------------------------------------------------------------------------------
# THE MASK AND THE DECLARATIONS, TESTED DIRECTLY. Nothing in the tree exercises these.
# --------------------------------------------------------------------------------

# (name, document, the mask's output). The ADR tree carries ordinary backtick fences and
# nothing else this masking code handles: `grep` finds no tilde fence, no fence longer than
# three, no blockquoted fence, and no line beginning with `<` in any of the twenty. So
# every case below except the plain fences is a construct the tree would never exercise,
# which is how a fence-only mask survived to a third review.
_BATTERY: tuple[tuple[str, str, str], ...] = (
    ("prose is left alone", "alpha\nbeta", "alpha\nbeta"),
    ("backtick fence", "a\n```\n# x\n```\nb", "a\n\n\n\nb"),
    ("tilde fence", "a\n~~~\n# x\n~~~\nb", "a\n\n\n\nb"),
    ("backticks do not close a tilde fence", "a\n~~~\n```\n# x\n~~~\nb", "a\n\n\n\n\nb"),
    ("three do not close a fence of four", "a\n````\n```\n# x\n````\nb", "a\n\n\n\n\nb"),
    ("an info string is not a closer", "a\n```python\n# x\n```\nb", "a\n\n\n\nb"),
    ("a backtick in the info string is not a fence", "a\n```f`b\nc", "a\n```f`b\nc"),
    ("three leading spaces still open a fence", "a\n   ```\n# x\n   ```\nb", "a\n\n\n\nb"),
    ("a comment closed on its own line", "a\n<!-- x -->\nb", "a\n\nb"),
    ("a comment over lines", "a\n<!--\n# x\n-->\nb", "a\n\n\n\nb"),
    ("blank lines do not end a comment", "a\n\n<!--\n\n- c\n\n-->\n\nb", "a\n\n\n\n\n\n\n\nb"),
    ("an unclosed comment runs to the end", "a\n<!--\n# x\nb", "a\n\n\n"),
    ("a pre block", "a\n<pre>\n# x\n</pre>\nb", "a\n\n\n\nb"),
    ("a comment closer does not close a pre", "a\n<pre>\n-->\n# x\n</pre>\nb", "a\n\n\n\n\nb"),
    ("a pre opened and closed on one line", "a\n<pre>x</pre>\nb", "a\n\nb"),
    ("a stray closing tag is one line, not the rest", "a\n</pre>\nb", "a\n\nb"),
    ("a script block", "a\n<script>\n# x\n</script>\nb", "a\n\n\n\nb"),
    ("a comment inside a fence cannot run away", "a\n```\n<!--\n```\nb", "a\n\n\n\nb"),
    ("a fence inside a comment does not open", "a\n<!--\n```\n-->\nb", "a\n\n\n\nb"),
    ("a mid-line comment opener is not a block", "a <!-- b\nc", "a <!-- b\nc"),
    ("a blockquoted fence is not masked", "> ```\n> # x\n> ```", "> ```\n> # x\n> ```"),
)


def test_the_mask_blanks_every_construct_that_renders_as_nothing():
    """THE MASK'S OWN BEHAVIOUR, asserted instead of inferred from the page not moving.

    A fence-only mask let three HTML documents through, and none of them was a missing
    construct so much as a missing test -- the ADR tree exercises none of this.

    The last two cases are LIMITS rather than wins, here to be seen: a `<!--` mid-line is
    left alone, because a document discussing comments in backticks would otherwise mask
    itself to the end of the file; and a fence inside a blockquote is left alone, which is
    safe only because a `>`-prefixed line is not a heading, a bullet or a marker to
    either reading."""
    wrong = [
        (name, "\n".join(_prose_lines(doc)), expected)
        for name, doc, expected in _BATTERY
        if "\n".join(_prose_lines(doc)) != expected
    ]
    assert not wrong, f"{len(wrong)} of {len(_BATTERY)} cases: {wrong}"


def test_the_two_maskings_agree_over_the_battery():
    """THE DUPLICATION'S GUARD, and it asserts AGREEMENT rather than independence.

    `_prose_lines` and `adr_index._mask_non_prose` are one algorithm typed twice, so they
    cannot catch each other being wrong -- the module docstring carries why they are
    duplicated anyway. What they can still do is stop agreeing, which is what a one-sided
    edit to either looks like, and this is where that arrives."""
    idx = _load("adr_index", "adr_index.py")
    wrong = [
        name for name, doc, _expected in _BATTERY
        if "\n".join(_prose_lines(doc)) != idx._mask_non_prose(doc)
    ]
    assert not wrong, f"the two maskings disagree about {wrong}"


def test_a_phase_label_is_not_matched_by_a_branch_naming_its_sub_phase():
    """THE ONLY PHASE CONFUSION THIS TABLE OFFERS, which flattened containment missed.

    `F1.4` and `F1.4b` are both declared, against branches differing by one character in
    the same place. Stripped to `[a-z0-9]` and asked for substring containment, each was
    found inside the other's branch -- `f14` inside `f14bprb...`, and `f14b` inside
    `f14bronze...`, where that `b` is the first letter of `bronze`. The check rejected
    `F9`, a phase from another universe, and accepted the one pair it exists for.

    The block at the end is the whole declaration, checked WITHOUT git -- so unlike
    `test_the_declared_phase_is_what_git_says` this half runs in CI."""
    assert _branch_names_phase("F1.4", "feat/f1-4-bronze-generalisation")
    assert _branch_names_phase("F1.4b", "feat/f1-4b-empresas-socios")
    assert _branch_names_phase("F1.4b PR B", "feat/f1-4b-pr-b-second-month")
    assert _branch_names_phase("F2 wave 1", "feat/f2-wave-1-cnpj-vault")
    assert not _branch_names_phase("F1.4", "feat/f1-4b-pr-b-second-month")
    assert not _branch_names_phase("F1.4b", "feat/f1-4-bronze-generalisation")
    assert not _branch_names_phase("F9", "feat/f6-rca-agent")
    idx = _load("adr_index", "adr_index.py")
    wrong = [
        number for number, (phase, _merge, branch) in idx.PHASES.items()
        if not _branch_names_phase(phase, branch)
    ]
    assert not wrong, f"{wrong}: the declared phase is not named by the declared branch"


def test_a_second_reading_of_one_condition_is_refused(monkeypatch):
    """`readings_for` keys by condition TEXT, so a second reading overwrites the first.

    The anchor guard is exact from the READING's side -- one reading, one condition -- and
    says nothing from the condition's side. Two readings of one condition therefore drop a
    row's state off the published table and make *"N have been read"* under-report, with
    every other test green. A reading log that grows a later reading of a condition
    already read is the ordinary way this table evolves, so it fails at import instead.

    RESTORED BY `monkeypatch`, NOT LEFT REBOUND. `_load` registers the module in
    `sys.modules`, and `generate_adr_index.py` reaches for exactly that object, so a
    rebound `READINGS` left behind here would be read by any later test that renders --
    a test mutating the tree its neighbours assert on."""
    idx = _load("adr_index", "adr_index.py")
    adr = next(a for a in idx.ADRS if a.conditions)
    text = adr.conditions[0].text
    monkeypatch.setattr(idx, "READINGS", tuple(
        idx.Reading(adr=adr.number, anchor=anchor, state=state, date="2026-08-30", why="x")
        for anchor, state in ((text, idx.MET), (text[:-5], idx.NOT_MET))
    ))
    with pytest.raises(ValueError, match="readings anchor on the single"):
        idx._assert_no_condition_carries_two_readings(idx.ADRS)


def test_every_conditions_section_in_an_adr_is_read_and_not_only_the_first():
    """`re.search` read one section per file, and no ADR carries two -- so nothing looked.

    The page publishes the extractor's floor, and *"only the first section"* was part of
    that floor without ever being written down. Asserted over a synthetic document rather
    than by editing an ADR, because a second conditions section in a real ADR is a change
    to what that ADR says."""
    idx = _load("adr_index", "adr_index.py")
    doc = (
        "## Decision 1\n\n### What would reverse this decision\n\n- the first\n\n"
        "## Decision 2\n\n### What would change this decision\n\n- the second\n"
    )
    found = idx._section_conditions(doc, "0000")
    assert [c.text for c in found] == ["the first", "the second"], found
    assert [c.spelling for c in found] == [idx.SECTION_REVERSE, idx.SECTION_CHANGE]


def test_the_comparison_goes_red_when_an_adr_has_no_row():
    """THE TWENTY-FIRST ADR, which is the failure this whole page exists to prevent.

    The row is deleted from the index rather than a file being written to the tree, so
    the arm is hermetic; the disagreement it produces is the same one a real new ADR
    produces, because `_disagreements` reaches the same branch either way."""
    text = _INDEX.read_text(encoding="utf-8")
    mutated = "\n".join(
        line for line in text.split("\n") if not line.startswith("| [0020](0020-")
    )
    assert mutated != text, "the fixture row is gone; re-point this arm"
    found = _disagreements(mutated, _adr_paths())
    assert any("0020" in message and "no row" in message for message in found), found
