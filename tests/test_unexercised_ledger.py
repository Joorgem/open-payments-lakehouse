# tests/test_unexercised_ledger.py
"""`docs/unexercised-ledger.md` against the nine documents it consolidates, RE-DERIVED.

WHY THIS FILE EXISTS, and it is the whole point of the document beside it. Nine documents each
published a *what is still unexercised* list and nobody ever read them as one list. Entries had
been closed by later work and never struck -- ten of them by the same document that still
carries them, and two closed BEFORE they were written. A finding that is recorded and not
consumed is indistinguishable from a finding nobody made, and a TENTH hand-maintained list with
no obliged consumer would reproduce that defect exactly. This file is the consumer.

THE LOAD-BEARING LOCK IS THE ANCHOR CHECK. Every entry carries an id `key:line` and a claim
quoted verbatim from that line. `test_every_anchor_still_points_at_its_claim` reads the source
and asserts the quote is still in a three-line window at the anchor. That is what stops the
consolidated document drifting from its sources the way ADR 0009's job list drifted from
`databricks/resources/` -- and the window is three lines, not one, so a re-wrap of a paragraph
does not redden it. `_window` states the tolerance that buys, MEASURED rather than asserted.

AND THE SET OF ROWS IS ITSELF LOCKED, which it was not when this file was written. `_SECTIONS`
below is a hand-typed list of headings -- this document's own defect class, inside its lock -- so
a section added and not wired up contributed rows every check here reported green over.
`_rows_outside_the_sections` counts the published grep against the rows that list reaches.

WHAT IS NOT LOCKED HERE, named rather than left to be discovered:

  * THE BUCKET. Deciding that `stream._require_currencies` fires on an edit while
    `unhashable_case_divergence` fires on data is a judgement about what a guard PROTECTS. No
    parser reads that, it is hand-assigned, and the document says so at the top.
  * THE *what would exercise it* TEXT. Prose. Only its presence is asserted -- and that
    `nothing` is not one of its values, because by the ledger's own rule an entry whose
    exerciser is nothing leaves the ledger and becomes a caveat. That rule has never been
    mechanical before this file.
  * WHETHER AN ENTRY IS STILL TRUE. No test can run a Databricks job. Closing an entry is a
    human act, which is why the document has a CLOSED section at all.
  * STATEMENT IDS AND RUN IDS in the CLOSED evidence. They cost a live API call each and this
    file makes none, so their SHAPE is checked and nothing else. Saying so is the point: the
    alternative is a green that reads like verification and is not.

AND THE COMPARISON IS PROVED CAPABLE OF FAILING, on every run: the arms at the bottom mutate
the document IN MEMORY -- one fact at a time -- and assert the failure names that fact and no
other. A lock nobody has watched go red is not a lock.

WHAT LEFT, AND THE SEAM IT LEFT ALONG. This file reached **799 lines** against a
strictly-under-800 cap with F2 wave 2's rows still owed, so the half that reads THE
REPOSITORY -- the source lines the anchors name, the `docs/*-evidence.md` corpus the sweep
walks, and the git object store the CLOSED evidence cites -- is now `tests/ledger_sources.py`,
which carries the argument for the seam. What stayed is the other subject: what the DOCUMENT
says, the comparisons between the two, and the arms. `_unconsolidated_sections` stayed even
though it sat inside the block that left, because it is a COMPARISON and reaches back across
the seam through `_headings`; it is the reason the departed block's banner claim -- *"nothing
below opens the ledger document"* -- was true only in the narrow sense that it never called
`_ledger()` itself.
"""
from __future__ import annotations

import re
from collections import Counter

import pytest

# ALIASED TO THE NAMES THIS FILE ALREADY USED, so the split moved definitions and
# touched no call site. `tests/ledger_sources.py` carries the seam, and why
# `_unconsolidated_sections` did not go with the block it used to sit in.
from ledger_sources import DEFERRAL as _DEFERRAL
from ledger_sources import PUBLISHED_PATTERN as _PUBLISHED_PATTERN
from ledger_sources import REPO as _REPO
from ledger_sources import commit_resolves as _commit_resolves
from ledger_sources import corpus_files as _corpus
from ledger_sources import history_is_deep as _history_is_deep
from ledger_sources import norm as _norm
from ledger_sources import source_lines as _source_lines
from ledger_sources import swept_headings as _swept_headings
from ledger_sources import swept_sections as _swept_sections
from ledger_sources import window as _window

_LEDGER = _REPO / "docs" / "unexercised-ledger.md"

# Section heading -> bucket. EVERY table of entries must be named here, and NOTHING USED TO
# CHECK THAT -- sixteen headings typed by hand, iterated by `_entries` alone, so a seventeenth
# section contributed nothing SILENTLY. `### 3.9` was added by the same commit as this file and
# is already missing here, harmless only because it carries prose.
_SECTIONS = {
    "### 1.1 Refusals": "STANDING LIMITS",
    "### 1.2 Blind spots": "STANDING LIMITS",
    "### 1.3 Added by F7 T4": "STANDING LIMITS",
    "## 2. PUBLISHED CAVEATS": "PUBLISHED CAVEATS",
    "### 3.1 Bronze ingest and the DQ gate": "STILL UNEXERCISED",
    "### 3.2 Vault loaders": "STILL UNEXERCISED",
    "### 3.3 Gold": "STILL UNEXERCISED",
    "### 3.4 Streaming": "STILL UNEXERCISED",
    "### 3.5 Governance": "STILL UNEXERCISED",
    "### 3.6 DataOps and triage": "STILL UNEXERCISED",
    "### 3.7 Extraction and sources": "STILL UNEXERCISED",
    "### 3.8 CI and platform": "STILL UNEXERCISED",
    "### 4.1 Closed and never struck": "CLOSED",
    "### 4.2 Closed and struck, or updated in place": "CLOSED",
    "## 5. NO LONGER MEANINGFUL": "NO LONGER MEANINGFUL",
}

_KEYS_TABLE = "### 0.4 The sources, keyed"
_TOTALS_TABLE = "### 0.5 The totals"
_ID = re.compile(r"^`([a-z0-9]+):(\d+)`$")

# The row grep §0.5 publishes as the document's own entry count, held HERE as the single
# authority and asserted present THERE. Counting it rather than reading it makes it a lock.
_ROW_GREP = r"^\| `[a-z0-9]+:[0-9]+` \|"
_ROW_ID = re.compile(r"^\| `([a-z0-9]+:[0-9]+)` \|")


# *what would exercise it*, answered with nothing but not spelled `nothing`. THE BARE DASHES
# ARE THE DAMAGING ONES: an em dash is markdown's empty cell, so a row could say "no answer"
# in the notation a reader reads as exactly that, and satisfy the check about it.
_NO_EXERCISER = re.compile(
    r"[*`_\s]*(nothing|none|nil|n/?a|tbd|unknown|\?+|[-–—]+)"
    r"(\s+(yet|at all|here|so far|known|planned|identified|in this repository))?"
    r"[*`_.\s]*",
    re.IGNORECASE)

# The CLOSED evidence tokens, held here because `_evidence_findings` now asserts a CLOSED
# row produces one, not merely that the ones it produced resolve.
_EVIDENCE = re.compile(r"\b(commit|anchor|run|stmt):(\S+)")
_NO_EVIDENCE = "cites no commit:, anchor:, run: or stmt: evidence"


# --------------------------------------------------------------------------------
# READING MARKDOWN. Shapes only: a heading, and the tables under it.
# --------------------------------------------------------------------------------


def _tables(text: str, heading: str) -> list[list[list[str]]]:
    """Every markdown table under `heading`, until the next heading of any level."""
    lines = text.splitlines()
    starts = [i for i, line in enumerate(lines) if line.startswith(heading)]
    assert len(starts) == 1, f"{heading!r} appears {len(starts)} times, expected once"
    tables: list[list[list[str]]] = []
    current: list[list[str]] = []
    for line in lines[starts[0] + 1:]:
        if line.startswith("#"):
            break
        if not line.startswith("|"):
            if current:
                tables.append(current)
                current = []
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if all(c and set(c) <= {"-", ":"} for c in cells):
            continue
        current.append(cells)
    if current:
        tables.append(current)
    assert tables, f"no table under {heading!r}"
    return tables


def _rows(text: str, heading: str) -> list[list[str]]:
    """The body rows of the FIRST table under `heading`, header dropped."""
    return _tables(text, heading)[0][1:]


# --------------------------------------------------------------------------------
# WHAT THE DOCUMENT SAYS.
# --------------------------------------------------------------------------------


def _keys(text: str) -> dict[str, str]:
    """`key` -> repository path, from both tables of §0.4.

    A key declared twice must name the same file both times -- `f2ws` legitimately appears
    twice because that document carries its ledger in two sections."""
    mapping: dict[str, str] = {}
    for table in _tables(text, _KEYS_TABLE):
        for row in table[1:]:
            key, path = row[0].strip("`"), row[1].strip("`")
            assert mapping.setdefault(key, path) == path, f"{key!r} names two files"
    return mapping


def _headings(text: str) -> list[tuple[str, str]]:
    """The (file, ledger heading) pairs declared in §0.4's first table."""
    return [(row[1].strip("`"), row[2].strip("`"))
            for row in _tables(text, _KEYS_TABLE)[0][1:]]


def _entries(text: str) -> list[dict[str, object]]:
    """Every entry in the document, as {id, key, line, claim, bucket, section, cells}."""
    found: list[dict[str, object]] = []
    for heading, bucket in _SECTIONS.items():
        for row in _rows(text, heading):
            match = _ID.match(row[0])
            assert match, f"{row[0]!r} under {heading!r} is not a `key:line` id"
            found.append({"id": row[0].strip("`"), "key": match.group(1),
                          "line": int(match.group(2)), "claim": row[1],
                          "bucket": bucket, "section": heading, "cells": row})
    return found


def _rows_outside_the_sections(text: str) -> dict[str, int]:
    """Every `key:line` row `_SECTIONS` did not reach, and how many times it appears.

    THE DEFECT THIS DOCUMENT EXISTS TO END, REPRODUCED INSIDE ITS OWN LOCK. A section nobody
    wired into `_SECTIONS` was not partially checked, it was not checked at all: a `### 3.10`
    with an anchor past end of file, an undeclared key and an exerciser of `nothing` leaves every
    other check here green. §0.5 PUBLISHES THE GREP; this counts it. A NEGATIVE number is the
    other direction -- a row `_entries` reached that the grep does not."""
    counted = Counter(match.group(1) for match in
                      (_ROW_ID.match(line) for line in text.splitlines()) if match)
    for entry in _entries(text):
        counted[str(entry["id"])] -= 1
    return {row: count for row, count in counted.items() if count}


def _stated_totals(text: str) -> dict[str, int]:
    """The bucket totals the document publishes in §0.5, TOTAL included."""
    return {row[0].strip("*").strip(): int(row[1].strip("*").replace(",", ""))
            for row in _rows(text, _TOTALS_TABLE)}


def _derived_totals(text: str) -> dict[str, int]:
    """The same totals, counted off the entry tables."""
    counted: dict[str, int] = {bucket: 0 for bucket in set(_SECTIONS.values())}
    for entry in _entries(text):
        counted[str(entry["bucket"])] += 1
    counted["TOTAL"] = sum(counted.values())
    return counted


# --------------------------------------------------------------------------------
# THE COMPARISONS, each returning what it found rather than asserting.
# --------------------------------------------------------------------------------


def _unconsolidated_sections(text: str,
                             corpus: dict[str, str] | None = None) -> dict[str, str]:
    """Every ledger-shaped section in the corpus that this document neither takes nor is given.

    A SECTION IS ACCOUNTED FOR IN EXACTLY THREE WAYS, all mechanical: §0.4 DECLARES IT, so its
    entries were lifted into this document; IT NESTS INSIDE A DECLARED ONE, as `### Not exercised
    by choice, and the choice is recorded` sits under `## 3. What is still unexercised` in both
    `f4` and `f5` -- part of a declared ledger, not a competing one, and the widened pattern
    reaches it; or IT DEFERS, naming `unexercised-ledger.md` in its own body, which
    `docs/f7-run-evidence.md` §3 does in as many words.

    ANYTHING ELSE IS AN ELEVENTH LIST NOBODY CONSOLIDATED, and it is named here. THIS IS WEAKER
    THAN THE EQUALITY THIS CHECK USED TO ASSERT, at the price of a pattern wide enough to see the
    wording F7 introduced -- so name the hole: a section carrying real debt that ALSO links this
    file in passing is excused by the third rule. The narrower reading, undeclared means red,
    cannot run here at all: §0.4 is keyed and every declared key must be used by an entry, so a
    section contributing no entries has no way to be declared."""
    declared = set(_headings(text))
    misses: dict[str, str] = {}
    for name, heading, ancestors, body in _swept_sections(corpus=corpus):
        if ((name, heading) in declared or _DEFERRAL in body
                or any((name, ancestor) in declared for ancestor in ancestors)):
            continue
        misses[f"{name} :: {heading}"] = (
            f"a ledger section §0.4 does not declare and whose body never names {_DEFERRAL}: "
            "consolidate it or defer to the ledger in as many words")
    return misses


def _anchor_misses(text: str) -> dict[str, str]:
    """Every entry whose quoted claim is no longer at the line its id names."""
    keys = _keys(text)
    misses: dict[str, str] = {}
    for entry in _entries(text):
        key = str(entry["key"])
        if key not in keys:
            misses[str(entry["id"])] = f"key {key!r} is not declared in {_KEYS_TABLE}"
            continue
        window = _window(keys[key], int(entry["line"]))
        if _norm(str(entry["claim"])) not in window:
            misses[str(entry["id"])] = f"claim not found in {keys[key]}:{entry['line']}"
    return misses


def _heading_misses(text: str) -> dict[str, str]:
    """Every declared (file, heading) pair that no longer resolves exactly once."""
    misses: dict[str, str] = {}
    for path, heading in _headings(text):
        lines = _source_lines(path)
        hits = [i for i, line in enumerate(lines, 1) if line.strip() == heading]
        if len(hits) != 1:
            misses[f"{path} :: {heading}"] = f"{len(hits)} matches, expected 1"
    return misses


def _exerciser_misses(text: str) -> dict[str, str]:
    """Every carry-forward entry that names no exerciser, or names `nothing`.

    THE LEDGER'S OWN RULE, MECHANICAL FOR THE FIRST TIME. `nothing` is a legal answer to
    *what would exercise it* -- it just means the entry is a CAVEAT and belongs in §2, not
    in the carry-forward. Nine documents stated that rule and none enforced it.

    AND THE RULE IS ABOUT THE ANSWER, NOT THE WORD. The first spelling refused the literal
    string `nothing` and nothing else, so `none`, `n/a`, `nothing yet`, `-` and `—` all passed
    -- the dashes being the ones that bite. `_NO_EXERCISER` refuses it however it is spelled."""
    misses: dict[str, str] = {}
    for entry in _entries(text):
        if entry["bucket"] != "STILL UNEXERCISED":
            continue
        cells = list(entry["cells"])
        cell = _norm(cells[2]) if len(cells) > 2 else ""
        if not cell:
            misses[str(entry["id"])] = "names no *what would exercise it*"
        elif _NO_EXERCISER.fullmatch(cell):
            misses[str(entry["id"])] = f"{cell!r} is no exerciser; it belongs in §2 CAVEATS"
    return misses


def _evidence_findings(text: str) -> tuple[dict[str, str], dict[str, str]]:
    """(what is wrong, what this clone could not check) over every CLOSED row's evidence.

    `commit:` and `anchor:` are resolved here. `run:` and `stmt:` CANNOT BE CHECKED WITHOUT A
    LIVE API CALL and this file makes none, so only their shape is asserted -- named rather than
    quietly passed, because a green that reads like verification and is not is the defect this
    document is about.

    A ROW THAT CITES NOTHING AT ALL IS THE FIRST FINDING, and this used to miss it outright: it
    reported problems with the tokens it found and never asked whether it had found any, so an
    evidence cell rewritten as prose -- or one `commit:` mistyped `sha:` -- came back clean.
    CLOSED is the bucket this document is about, `_exerciser_misses` already demands presence
    for the carry-forward, and all 34 CLOSED rows cite a token today.

    THE SECOND RETURN IS THE HONEST HALF. On a depth-1 clone, which is what CI checks out, a
    `commit:` token is unresolvable BY CONSTRUCTION and carries no information either way, so it
    is reported unchecked rather than folded into a pass or a failure. See `_history_is_deep`."""
    misses: dict[str, str] = {}
    unchecked: dict[str, str] = {}
    deep = _history_is_deep()
    for entry in _entries(text):
        if entry["bucket"] != "CLOSED":
            continue
        tokens = _EVIDENCE.findall(list(entry["cells"])[3])
        if not tokens:
            misses[str(entry["id"])] = _NO_EVIDENCE
            continue
        for kind, value in tokens:
            problem = _evidence_problem(kind, value)
            if not problem:
                continue
            where = f"{entry['id']} {kind}:{value}"
            if kind == "commit" and not deep:
                unchecked[where] = "shallow clone: this commit is not in the object store"
            else:
                misses[where] = problem
    return misses, unchecked


def _evidence_misses(text: str) -> dict[str, str]:
    """The half of `_evidence_findings` this clone was actually able to check."""
    return _evidence_findings(text)[0]


def _evidence_problem(kind: str, value: str) -> str:
    """What is wrong with one evidence token, or '' if nothing is."""
    if kind == "commit":
        return "" if _commit_resolves(value) else "commit does not resolve"
    if kind == "anchor":
        path, _, line = value.rpartition(":")
        if not (_REPO / path).is_file():
            return f"{path} is not a file in this repository"
        return "" if int(line) <= len(_source_lines(path)) else "line is past end of file"
    if kind == "run":
        return "" if value.isdigit() else "a run id is all digits"
    return "" if re.fullmatch(r"[0-9a-f-]{20,}", value) else "not a statement id's shape"


def _prose_disagreements(text: str) -> dict[str, tuple[int, int]]:
    """The two counts §0.1 states in a sentence, against the rows that produce them.

    A NUMBER IN PROSE IS THE FIRST THING TO ROT, and the document's own entry `f6:1883` is a
    count that went stale three times before its phase replaced it with a measurement. So the
    two the opening paragraph needs are read back out of it by shape and re-derived."""
    closed = _rows(text, "### 4.1 Closed and never struck")
    keys = _keys(text)
    same_file = sum(
        1 for row in closed
        if keys.get(str(_ID.match(row[0]).group(1)), "?") in row[3])
    stated = {
        "never_struck": int(_one(text, r"\*\*(\d+) entries had been closed by later work")),
        "same_document": int(_one(text, r"\*\*(\d+) of them by the same document")),
    }
    derived = {"never_struck": len(closed), "same_document": same_file}
    return {name: (stated[name], value)
            for name, value in derived.items() if stated[name] != value}


def _one(text: str, pattern: str) -> str:
    """The one match of `pattern`, group 1. Two matches is as much a failure as none."""
    hits = re.findall(pattern, text)
    assert len(hits) == 1, f"{pattern!r} matches {len(hits)} times, expected exactly 1"
    return hits[0]


def _duplicate_ids(text: str) -> dict[str, list[str]]:
    """Every id appearing in more than one row, with the buckets it appears in."""
    seen: dict[str, list[str]] = {}
    for entry in _entries(text):
        seen.setdefault(str(entry["id"]), []).append(str(entry["bucket"]))
    return {key: value for key, value in seen.items() if len(value) > 1}


def _total_disagreements(text: str) -> dict[str, tuple[int, int]]:
    """Every bucket where the published total and the counted rows differ."""
    stated, derived = _stated_totals(text), _derived_totals(text)
    return {bucket: (stated.get(bucket), count)
            for bucket, count in derived.items() if stated.get(bucket) != count}


def _ledger() -> str:
    return _LEDGER.read_text(encoding="utf-8")


# --------------------------------------------------------------------------------
# THE LOCK.
# --------------------------------------------------------------------------------


def test_every_anchor_still_points_at_its_claim():
    """THE LOAD-BEARING ONE: every quoted claim is still at the line its id names."""
    assert _anchor_misses(_ledger()) == {}


def test_the_ten_declared_source_sections_still_resolve():
    """Nine documents, ten sections. A rename or a deletion goes red here."""
    assert _heading_misses(_ledger()) == {}


def test_the_sweep_accounts_for_every_ledger_section_in_the_phase_records():
    """An eleventh ledger section in `docs/*-evidence.md` that nobody consolidated reddens this.

    NOT "anywhere under `docs/`", WHICH IS WHAT THIS DOCSTRING USED TO SAY. The corpus is the
    phase records; `_corpus` names what that excludes and why. The first two assertions are
    different failures: a declared heading the sweep no longer finds is a rename or a deletion,
    a swept heading nothing accounts for is the tenth list starting again.

    THE LAST TWO ARE CONTAINMENT WHERE THERE USED TO BE EQUALITY. The published pattern and this
    file's were one string, so a reader's command could not drift from this one; they now differ
    in one direction only, because the published one misses a real ledger section on wording."""
    text = _ledger()
    assert set(_headings(text)) <= _swept_headings()
    assert _unconsolidated_sections(text) == {}
    assert _PUBLISHED_PATTERN in text, "the document must publish a sweep a reader can run"
    assert _swept_headings(_PUBLISHED_PATTERN) <= _swept_headings()


def test_no_entry_is_in_two_buckets():
    """A bucket is a claim about ONE entry; two homes for one id is a contradiction."""
    assert _duplicate_ids(_ledger()) == {}


def test_every_carry_forward_entry_names_what_would_exercise_it():
    """And `nothing` is not one of the answers, because it forces the entry into §2."""
    assert _exerciser_misses(_ledger()) == {}


def test_the_published_totals_are_what_the_tables_hold():
    """The five bucket counts and their sum, re-derived off the rows themselves."""
    assert _total_disagreements(_ledger()) == {}


def test_every_row_the_published_grep_counts_is_reached_by_a_declared_section():
    """Rows are what this document is made of, and nothing checked that it holds them all.

    §0.5 prints the grep; this counts it against the sections `_SECTIONS` reaches. A table
    under a heading nobody wired up is the one edit that used to produce silence here."""
    text = _ledger()
    assert _rows_outside_the_sections(text) == {}
    assert _ROW_GREP in text, "the document must publish the row grep this file counts"


def test_every_closed_row_s_evidence_resolves_as_far_as_it_can():
    """Commits and file anchors resolve; run and statement ids get their shape checked.

    AND WHAT IT COULD NOT CHECK IS SAID OUT LOUD RATHER THAN PASSED OVER. The assertion runs
    first and unconditionally, so an anchor past end of file, a malformed run id or a CLOSED row
    citing nothing still redden CI; only then, if this clone is too shallow for a `commit:` token,
    does this SKIP naming what it could not reach rather than report the expected value."""
    misses, unchecked = _evidence_findings(_ledger())
    assert misses == {}
    if unchecked:
        pytest.skip(f"depth-1 clone, so {sorted(unchecked)} cannot be resolved here. This is "
                    "CI's default checkout, so the commit half does not run there")


def test_the_two_counts_the_opening_paragraph_states_are_what_the_rows_hold():
    """The headline finding is two numbers in a sentence, and both are re-derived."""
    assert _prose_disagreements(_ledger()) == {}


def test_every_declared_key_is_used_and_every_used_key_is_declared():
    """A key nobody uses is dead weight; a key nobody declares cannot be resolved."""
    text = _ledger()
    assert {str(entry["key"]) for entry in _entries(text)} == set(_keys(text))


def test_the_document_publishes_the_command_that_re_derives_it():
    """A reader must be able to run what this file runs, or the totals are decoration."""
    text = _ledger()
    for fragment in ("uv run pytest tests/test_unexercised_ledger.py",
                     "docs/unexercised-ledger.md"):
        assert fragment in text.split("### 0.5 The totals", 1)[1]


# --------------------------------------------------------------------------------
# THE FAILURE ARMS. Each mutates the document in memory and asserts the miss is NAMED.
# --------------------------------------------------------------------------------


def _mutated(old: str, new: str) -> str:
    text = _ledger()
    assert text.count(old) == 1, f"{old!r} appears {text.count(old)} times"
    return text.replace(old, new)


# One real carry-forward row, held once because three arms rewrite its exerciser cell.
_F5_914_EXERCISER = "a Databricks compute that permits the read"
_F5_914 = f"| `f5:914` | The ring-buffer cap on serverless. | {_F5_914_EXERCISER} |"


def test_a_claim_that_no_longer_matches_its_source_is_named_and_nothing_else_is():
    """One word changed in one quoted claim."""
    assert _anchor_misses(_mutated(
        "| `f5:871` | The CI `redpanda` job. |",
        "| `f5:871` | The CI `kafka` job. |",
    )) == {"f5:871": "claim not found in docs/f5-run-evidence.md:871"}


def test_an_anchor_moved_to_the_wrong_line_is_named():
    """The claim is untouched; only the line number moves, which is the drift that bites."""
    assert set(_anchor_misses(_mutated("| `fdb:1441` |", "| `fdb:1341` |"))) == {"fdb:1341"}


def test_a_renamed_source_section_is_named():
    """A heading in §0.4 that no longer exists in the file it points at."""
    misses = _heading_misses(_mutated("`## 3. What ships UNEXERCISED` |\n| `fdb`",
                                      "`## 3. What ships UNEXERCISED!` |\n| `fdb`"))
    assert misses == {
        "docs/f-api-run-evidence.md :: ## 3. What ships UNEXERCISED!": "0 matches, expected 1"}


def test_an_entry_in_two_buckets_is_named():
    """The same id filed under STILL UNEXERCISED and under PUBLISHED CAVEATS."""
    duplicates = _duplicate_ids(_mutated("| `f6:2312` |", "| `f6:2333` |"))
    assert duplicates == {"f6:2333": ["PUBLISHED CAVEATS", "STILL UNEXERCISED"]}


def test_an_exerciser_of_nothing_is_named_and_told_where_it_belongs():
    """The ledger's own rule: a `nothing` leaves the carry-forward and becomes a caveat."""
    assert _exerciser_misses(_mutated(
        "| `f5:968` | The orphan topic left by a fixture that dies during SETUP. | a session "
        "that raises during setup and never reaches the finaliser |",
        "| `f5:968` | The orphan topic left by a fixture that dies during SETUP. | nothing |",
    )) == {"f5:968": "'nothing' is no exerciser; it belongs in §2 CAVEATS"}


def test_an_exerciser_that_spells_nothing_another_way_is_named_too():
    """Eight spellings of the same non-answer, all of which used to pass.

    THE DASHES MATTER MOST: `—` is markdown's conventional empty cell, so a row could say
    *there is no answer* in the notation a reader reads as exactly that."""
    for spelling in ("none", "None.", "n/a", "N/A", "nothing yet", "—", "-", "**nothing**"):
        found = _exerciser_misses(_mutated(_F5_914, _F5_914.replace(_F5_914_EXERCISER,
                                                                    spelling)))
        assert set(found) == {"f5:914"}, spelling


def test_an_empty_exerciser_is_named():
    """An entry that names no exerciser at all is the same defect, one step further."""
    assert set(_exerciser_misses(
        _mutated(_F5_914, _F5_914.replace(_F5_914_EXERCISER, " ")))) == {"f5:914"}


def test_a_published_total_that_drifts_from_the_rows_is_named():
    """One digit in §0.5, against the rows the document carries. BOTH INTEGERS ARE
    DERIVED -- typed, this arm reddened on a LEGITIMATE row move, which is a
    hand-maintained count inside the lock, the defect the document ends."""
    b = "STILL UNEXERCISED"
    n = _stated_totals(_ledger())[b]
    assert _total_disagreements(_mutated(f"| {b} | {n} |", f"| {b} | {n - 1} |")) \
        == {b: (n - 1, n)}


def test_a_deleted_row_moves_two_totals_and_both_are_named():
    """Deleting an entry has to be a decision someone types out, in two places."""
    assert set(_total_disagreements(_mutated(
        "| `f6:2189` | `emit`'s fence refusal | a live reject reason containing the marker |\n",
        ""))) == {"STILL UNEXERCISED", "TOTAL"}


def test_a_closed_row_whose_commit_does_not_resolve_is_named():
    """The evidence half: a commit that is not in this repository.

    SKIPS ON A DEPTH-1 CLONE, which is the finding rather than an accommodation: there
    `commit:0000000` and the real `commit:2d077a8` are both unresolvable, so this would
    assert the file tells apart two things it demonstrably cannot."""
    if not _history_is_deep():
        pytest.skip("depth-1 clone: no commit outside the tip resolves, so a wrong sha and an "
                    "absent one are indistinguishable. This is CI's default checkout")
    misses = _evidence_misses(_mutated("commit:2d077a8", "commit:0000000"))
    assert misses == {"f5:952 commit:0000000": "commit does not resolve"}


def test_a_closed_row_whose_evidence_prefix_is_mistyped_cites_nothing_and_is_named():
    """`sha:` is not `commit:`, and the row it labels stops citing evidence at all.

    THE SAME ARM COVERS AN EVIDENCE CELL REWRITTEN AS PROSE, and needs no history."""
    assert _evidence_misses(_mutated("commit:2d077a8", "sha:2d077a8")) == {
        "f5:952": _NO_EVIDENCE}


def test_a_closed_row_whose_anchor_is_past_the_end_of_its_file_is_named():
    """And the other evidence half: a `file:line` that no longer exists."""
    misses = _evidence_misses(_mutated("anchor:docs/f4-run-evidence.md:542 |",
                                       "anchor:docs/f4-run-evidence.md:99542 |"))
    assert misses == {
        "f2ws:641 anchor:docs/f4-run-evidence.md:99542": "line is past end of file"}


def test_an_undeclared_key_is_named_rather_than_silently_skipped():
    """An id whose key is in no §0.4 table cannot be resolved, and says so."""
    assert _anchor_misses(_mutated("| `pgsrc:552` |", "| `pgsrcx:552` |")) == {
        "pgsrcx:552": f"key 'pgsrcx' is not declared in {_KEYS_TABLE}"}


def test_a_headline_count_that_drifts_from_section_4_1_is_named():
    """The opening paragraph's `24`, against the rows §4.1 actually carries."""
    assert _prose_disagreements(_mutated(
        "**24 entries had been closed",
        "**23 entries had been closed")) == {"never_struck": (23, 24)}


# THE ONE §4.1 ROW THIS ARM MOVES, HELD ONCE RATHER THAN TWICE. F2 wave 2 RESTATED this
# row's *what closed it* cell: `link_payment` declared a second derivation on an identifying
# end, so "the only link with a declared derivation on an identifying end" stopped being true
# of `link_merchant_empresa`, and the cell now says what actually closed the row. The row had
# been spelled out TWICE below -- once as the search string and once as the replacement --
# so the restatement had to be retyped in both, a correction cost that bought nothing: THE
# MUTATION THIS ARM MAKES IS THE ANCHOR AND NOTHING ELSE. The replacement is derived from the
# row instead. Coupling to the document is unchanged -- reword the cell and `_mutated`'s
# count-of-one assertion fails on the search string, loudly, which is how this was found.
_FDB_1494 = (
    "| `fdb:1494` | `ObservationGrain.key_prefixes` AND `key_expression` HAVE "
    "RUN ZERO ROWS ON DATABRICKS | the run of record loaded "
    "`link_merchant_empresa`, whose non-empty `key_prefixes` are what those "
    "two functions read. **RESTATED BY F2 WAVE 2, NOT WEAKENED:** this cell "
    "used to read *\"the only link with a declared derivation on an "
    "identifying end\"*, which `link_payment` falsified by declaring TWO. What "
    "made this link the one that closed the row is not that it was the only "
    "link to declare a derivation, but that its prefixes REACH A GRAIN — "
    "`sat_link_payment` is transactional, takes `axis=` and reaches none — so "
    "the same run still closes it | anchor:docs/f-db-run-evidence.md:1225 |")


def test_a_row_moved_out_of_section_4_1_is_named_by_both_counts_it_moves():
    """Retyping one CLOSED row's evidence to another document moves the second count.

    AND THE MUTATION IS ASSERTED TO MUTATE before it is trusted, which is this phase's own
    lesson twice over: a replacement that happens to equal its search string applies
    cleanly, reports green and has changed nothing."""
    moved = _FDB_1494.replace("docs/f-db-run-evidence.md:1225", "docs/f4-run-evidence.md:542")
    assert moved != _FDB_1494, "the anchor swap did not change the row"
    assert _prose_disagreements(_mutated(_FDB_1494, moved)) == {"same_document": (10, 9)}


def test_a_source_section_dropped_from_the_declaration_is_named_by_the_sweep():
    """The sweep still finds it; the table no longer declares it, and the two disagree.

    AND IT TAKES ITS SUBSECTION WITH IT. `### Not exercised by choice, and the choice is
    recorded` is accounted for only by nesting inside a declared heading, so dropping the
    parent surfaces both -- the containment rule where it costs something."""
    row = "| `f5` | `docs/f5-run-evidence.md` | `## 3. What is still unexercised` |"
    text = _mutated(row + "\n", "")
    assert set(_unconsolidated_sections(text)) == {
        "docs/f5-run-evidence.md :: ## 3. What is still unexercised",
        "docs/f5-run-evidence.md :: ### Not exercised by choice, and the choice is recorded"}
    assert set(_headings(text)) < _swept_headings(), "the sweep still finds what was dropped"


def test_a_new_ledger_section_in_the_corpus_is_named_unless_it_defers():
    """THE DIRECTION THE SWEEP EXISTS FOR, and it was the one direction nothing exercised.

    The arm above mutates the DECLARATION. Nothing exercised a section APPEARING -- an F8
    document opening a tenth list -- the event the sweep is for. Both halves are asserted,
    because the excuse has to be provably an excuse."""
    corpus = _corpus()
    corpus["docs/f8-run-evidence.md"] = ("# F8\n\n## 2. What ran\n\nrows.\n\n"
                                         "## 3. What F8 leaves unrun\n\n- a claim nobody read\n")
    assert _unconsolidated_sections(_ledger(), corpus) == {
        "docs/f8-run-evidence.md :: ## 3. What F8 leaves unrun":
            f"a ledger section §0.4 does not declare and whose body never names {_DEFERRAL}: "
            "consolidate it or defer to the ledger in as many words"}
    corpus["docs/f8-run-evidence.md"] += f"\nIt lives in `docs/{_DEFERRAL}`.\n"
    assert _unconsolidated_sections(_ledger(), corpus) == {}


def test_the_sweep_sees_the_spelling_f7_set_and_the_published_grep_does_not():
    """The wording that made this sweep blind, named at both widths.

    `## 3. What F7 leaves unrun, and where the rest of it lives` is ledger-shaped, in a file
    the glob matches, missed on wording alone -- and the precedent an F8 author would copy.
    This arm fails if the pattern is narrowed back; the strict subset stops the two being
    re-unified by quietly narrowing this file's to match."""
    f7 = ("docs/f7-run-evidence.md",
          "## 3. What F7 leaves unrun, and where the rest of it lives")
    assert f7 in _swept_headings()
    assert f7 not in _swept_headings(_PUBLISHED_PATTERN)
    assert _swept_headings(_PUBLISHED_PATTERN) < _swept_headings()


def test_a_row_under_a_section_nobody_declared_contributes_nothing_and_is_named():
    """THE DEFECT CLASS OF THIS WHOLE DOCUMENT, REPRODUCED INSIDE ITS OWN LOCK.

    A `### 3.10` that `_SECTIONS` does not name carries two rows breaking three rules at once:
    an anchor past end of file, a key §0.4 never declared, an exerciser of `nothing`. The last
    two assertions are the point -- every other check here reports GREEN over all of it."""
    text = _mutated("## 4. CLOSED — what a later phase actually did",
                    "### 3.10 Added by nobody\n\n| id | claim | what would exercise it |\n"
                    "|---|---|---|\n"
                    "| `f5:99999` | a claim at a line that does not exist | nothing |\n"
                    "| `nosuch:1` | a claim under a key nobody declared | nothing |\n\n"
                    "## 4. CLOSED — what a later phase actually did")
    assert _rows_outside_the_sections(text) == {"f5:99999": 1, "nosuch:1": 1}
    assert _anchor_misses(text) == {} and _exerciser_misses(text) == {}
    assert _total_disagreements(text) == {} and _duplicate_ids(text) == {}
