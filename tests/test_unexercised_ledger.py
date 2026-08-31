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
does not redden it while a claim that MOVES does.

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
"""
from __future__ import annotations

import re
import subprocess
from functools import lru_cache
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_LEDGER = _REPO / "docs" / "unexercised-ledger.md"

# Section heading -> bucket. EVERY table of entries in the document must be named here, so a
# new section that nobody wired up contributes nothing silently -- which is the defect class
# this whole document exists to end.
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
_ANCHOR_WINDOW = 3

# The sweep the document publishes in its own closing block. Written HERE as the single
# authority and asserted to be present THERE, so the command a reader runs and the one
# this file runs cannot drift apart.
_SECTION_PATTERN = r"^#+ .*(unexercised|did not exercise)"


# --------------------------------------------------------------------------------
# READING MARKDOWN. Shapes only: a heading, and the tables under it.
# --------------------------------------------------------------------------------


def _norm(text: str) -> str:
    """Whitespace collapsed, so a wrapped source line and a table cell compare equal."""
    return " ".join(text.split())


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
# WHAT THE REPOSITORY SAYS. Nothing below opens the ledger document.
# --------------------------------------------------------------------------------


@lru_cache(maxsize=64)
def _source_lines(path: str) -> tuple[str, ...]:
    resolved = _REPO / path
    assert resolved.is_file(), f"{path} is not a file in this repository"
    return tuple(resolved.read_text(encoding="utf-8").splitlines())


def _window(path: str, line: int) -> str:
    """The anchored line plus the two after it, whitespace collapsed.

    THREE LINES AND NOT ONE. A claim in these documents is a bullet that wraps, so a quote a
    reader would call "the line" often spans two; a window of one would redden on a re-wrap
    that changed nothing. Three is still tight enough that a claim which MOVES goes red."""
    lines = _source_lines(path)
    assert 1 <= line <= len(lines), f"{path}:{line} is past the end of the file"
    return _norm(" ".join(lines[line - 1:line - 1 + _ANCHOR_WINDOW]))


def _swept_headings() -> set[tuple[str, str]]:
    """Every ledger heading under `docs/`, found by the sweep rather than read off a table.

    THIS IS THE HALF A DECLARED LIST CANNOT SUPPLY. `_heading_misses` proves the ten declared
    sections still exist; only a sweep proves there is no ELEVENTH one that nobody added."""
    found: set[tuple[str, str]] = set()
    for path in sorted((_REPO / "docs").glob("*-evidence.md")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if re.match(_SECTION_PATTERN, line, re.IGNORECASE):
                found.add((path.relative_to(_REPO).as_posix(), line.strip()))
    return found


def _commit_resolves(sha: str) -> bool:
    return subprocess.run(["git", "cat-file", "-e", f"{sha}^{{commit}}"], cwd=_REPO,
                          capture_output=True).returncode == 0


# --------------------------------------------------------------------------------
# THE COMPARISONS, each returning what it found rather than asserting.
# --------------------------------------------------------------------------------


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
    in the carry-forward. Nine documents stated that rule and none enforced it."""
    misses: dict[str, str] = {}
    for entry in _entries(text):
        if entry["bucket"] != "STILL UNEXERCISED":
            continue
        cells = list(entry["cells"])
        cell = _norm(cells[2]) if len(cells) > 2 else ""
        if not cell:
            misses[str(entry["id"])] = "names no *what would exercise it*"
        elif re.fullmatch(r"[*`_]*nothing[*`_.]*", cell, re.IGNORECASE):
            misses[str(entry["id"])] = "exerciser is `nothing`; it belongs in §2 CAVEATS"
    return misses


def _evidence_misses(text: str) -> dict[str, str]:
    """Every CLOSED row whose offline-checkable evidence does not resolve.

    `commit:` and `anchor:` are resolved here. `run:` and `stmt:` CANNOT BE CHECKED WITHOUT A
    LIVE API CALL and this file makes none, so only their shape is asserted -- named rather
    than quietly passed, because a green that reads like verification and is not is the
    defect this document is about."""
    misses: dict[str, str] = {}
    for entry in _entries(text):
        if entry["bucket"] != "CLOSED":
            continue
        for token in re.findall(r"\b(commit|anchor|run|stmt):(\S+)", list(entry["cells"])[3]):
            kind, value = token
            problem = _evidence_problem(kind, value)
            if problem:
                misses[f"{entry['id']} {kind}:{value}"] = problem
    return misses


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


def test_the_sweep_finds_exactly_the_sections_the_document_declares():
    """An eleventh ledger section anywhere under `docs/` reddens this, declared or not."""
    text = _ledger()
    assert _swept_headings() == set(_headings(text))
    assert _SECTION_PATTERN in text, "the document must publish the sweep this file runs"


def test_no_entry_is_in_two_buckets():
    """A bucket is a claim about ONE entry; two homes for one id is a contradiction."""
    assert _duplicate_ids(_ledger()) == {}


def test_every_carry_forward_entry_names_what_would_exercise_it():
    """And `nothing` is not one of the answers, because it forces the entry into §2."""
    assert _exerciser_misses(_ledger()) == {}


def test_the_published_totals_are_what_the_tables_hold():
    """The five bucket counts and their sum, re-derived off the rows themselves."""
    assert _total_disagreements(_ledger()) == {}


def test_every_closed_row_s_evidence_resolves_as_far_as_it_can():
    """Commits and file anchors resolve; run and statement ids get their shape checked."""
    assert _evidence_misses(_ledger()) == {}


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
    )) == {"f5:968": "exerciser is `nothing`; it belongs in §2 CAVEATS"}


def test_an_empty_exerciser_is_named():
    """An entry that names no exerciser at all is the same defect, one step further."""
    assert set(_exerciser_misses(_mutated(
        "| `f5:914` | The ring-buffer cap on serverless. | a Databricks compute that permits "
        "the read |",
        "| `f5:914` | The ring-buffer cap on serverless. |  |",
    ))) == {"f5:914"}


def test_a_published_total_that_drifts_from_the_rows_is_named():
    """One digit in §0.5, against the rows the document actually carries."""
    assert _total_disagreements(_mutated("| STILL UNEXERCISED | 114 |",
                                         "| STILL UNEXERCISED | 113 |")) \
        == {"STILL UNEXERCISED": (113, 114)}


def test_a_deleted_row_moves_two_totals_and_both_are_named():
    """Deleting an entry has to be a decision someone types out, in two places."""
    assert set(_total_disagreements(_mutated(
        "| `f6:2189` | `emit`'s fence refusal | a live reject reason containing the marker |\n",
        ""))) == {"STILL UNEXERCISED", "TOTAL"}


def test_a_closed_row_whose_commit_does_not_resolve_is_named():
    """The evidence half: a commit that is not in this repository."""
    misses = _evidence_misses(_mutated("commit:2d077a8", "commit:0000000"))
    assert misses == {"f5:952 commit:0000000": "commit does not resolve"}


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


def test_a_row_moved_out_of_section_4_1_is_named_by_both_counts_it_moves():
    """Retyping one CLOSED row's evidence to another document moves the second count."""
    assert _prose_disagreements(_mutated(
        "| `fdb:1494` | `ObservationGrain.key_prefixes` AND `key_expression` HAVE RUN ZERO "
        "ROWS ON DATABRICKS | the run of record loaded `link_merchant_empresa`, the only link "
        "with a declared derivation on an identifying end | anchor:docs/f-db-run-evidence.md:"
        "1225 |",
        "| `fdb:1494` | `ObservationGrain.key_prefixes` AND `key_expression` HAVE RUN ZERO "
        "ROWS ON DATABRICKS | the run of record loaded `link_merchant_empresa`, the only link "
        "with a declared derivation on an identifying end | anchor:docs/f4-run-evidence.md:"
        "542 |")) == {"same_document": (10, 9)}


def test_a_source_section_dropped_from_the_declaration_is_named_by_the_sweep():
    """The sweep still finds it; the table no longer declares it, and the two disagree."""
    row = "| `f5` | `docs/f5-run-evidence.md` | `## 3. What is still unexercised` |"
    text = _mutated(row + "\n", "")
    assert _swept_headings() - set(_headings(text)) == {
        ("docs/f5-run-evidence.md", "## 3. What is still unexercised")}
