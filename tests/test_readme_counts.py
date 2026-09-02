# tests/test_readme_counts.py
"""`README.md`'s counts against the things they count, RE-DERIVED from the repository.

WHY THIS FILE EXISTS. The README's *What is built* table was ten hand-typed numbers and
**CI did not read `README.md` at all**. Every one of them moves on the next merge -- a
bronze table, an ADR, a job task, a test -- and that is the exact mechanism issue #25 rode
for twelve days while three phases merged past it. Publishing the derivation commands in
the README is a convenience for a reader; it is not a CONSUMER of the finding, and a
finding with no obliged consumer decays into decoration. This file is the consumer.

READ BY SHAPE, NOT BY POSITION. Nothing here indexes a row. Each fact is found by a needle
that must match EXACTLY ONE row or sentence, so a reworded table fails loudly with the
needle in the message instead of silently comparing the wrong cell. Reordering the table
changes nothing; deleting the row it names turns this red.

DERIVED WITHOUT READING THE README. Every expectation comes from the registries, the bundle
YAMLs, `git ls-files`, a real collection, or the evidence document's own table -- never from
the prose being checked. So a green here means the two agree, not that one was copied.

AND THE COMPARISON IS PROVED CAPABLE OF FAILING, on every run: the arms at the bottom mutate
the README IN MEMORY -- one number at a time -- and assert `_disagreements` names that one
fact and no other. Three more were run against the TREE, in F7 T2's correction, one from
each direction. `**22 / 102**` was edited to `**22 / 100**` in the file and reported
`{'bundle_tasks': (97, 99)}`, then restored by inverse substitution and proved byte-identical
by sha256 -- never by `git checkout`. A twenty-first ADR reported `{'adrs': (20, 21),
'adrs_in_prose': (20, 21)}`, and needed `git add -N` first, because `git ls-files` is blind to
an untracked file; it was removed with `git reset -- <path>` and deleted. A twenty-second job
YAML reported `{'bundle_jobs': (21, 22), 'bundle_tasks': (99, 100), 'guard_bundle_jobs':
(21, 22)}` with no staging at all, because the bundle facts are globbed off disk.

WHAT IS NOT LOCKED HERE, named rather than left to be discovered. The figures with no tracked
derivation -- 60 HTTPS requests in 52 s, 192,973 bytes, "ten parts each", "about half the
time", the exactly-once arms, the LLM control's trial counts -- are checked by nothing in this
file, and the README's opening claim is scoped to the table below it for exactly that reason.
That the seven unlabelled evidence documents are the SEVEN OLDEST is true and was measured
from `git log --diff-filter=A`, and is deliberately not asserted here: CI checks out at
`fetch-depth: 1`, where that history does not exist.
"""
from __future__ import annotations

import re
import subprocess
import sys
from collections import Counter
from functools import lru_cache
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_README = _REPO / "README.md"
_RESOURCES = _REPO / "databricks" / "resources"
_ROW_EVIDENCE = _REPO / "docs" / "f1.4b-pr-b-run-evidence.md"

# The English number words this file has to read, DECLARED. An unmapped word is a KeyError
# naming it, which is the loud failure a silent `.get(w, 0)` would swallow.
# WIDENED IN F2 WAVE 2, AND WIDENING WHAT A LOCK CAN READ IS NOT RELAXING WHAT IT
# DEMANDS. The tree began deriving 21 and 22 for counts this map had no words for, and
# three patterns below captured with `(\w+)`, which cannot match a hyphen. The lock
# could therefore not READ a value the repository now derives, and the only way to keep
# it green without this change was to write a number the tree does not derive -- the
# exact failure it exists to catch. No assertion is loosened here: a disagreement still
# fails, and `guard_bundle_jobs` already used `([\w-]+)`, so the hyphen was expressible
# in one half of that sentence and not the other.
_WORDS = {"two": 2, "four": 4, "seven": 7, "eight": 8, "ten": 10, "thirteen": 13,
          "seventeen": 17, "twenty": 20, "twenty-one": 21, "twenty-two": 22}

# The vault's five kinds, as the README spells them against the class the registry builds.
_VAULT_KINDS = {"hubs": "Hub", "links": "Link", "satellites": "Satellite",
                "effectivity satellites": "EffectivitySatellite",
                "reference tables": "ReferenceTable"}

_BOLD = re.compile(r"\*\*(.+?)\*\*")
_TICKED = re.compile(r"`([a-z0-9_]+)`")
_MONTH = re.compile(r"\d{4}-\d{2}")


# --------------------------------------------------------------------------------
# READING MARKDOWN. Shapes only: a heading, a table under it, a row carrying a needle.
# --------------------------------------------------------------------------------


def _ints(text: str) -> list[int]:
    """Every integer in `text`, thousands separators removed."""
    return [int(m.replace(",", "")) for m in re.findall(r"\d[\d,]*\d|\d", text)]


def _bold_ints(cell: str) -> list[int]:
    """Every integer inside a `**bold**` span of `cell`, in order."""
    return [n for span in _BOLD.findall(cell) for n in _ints(span)]


def _table(text: str, heading: str) -> tuple[list[str], list[list[str]]]:
    """The first markdown table under `heading`, as (header cells, body rows).

    The separator row is dropped by its shape rather than by its position. It is NOT
    required: a table written without one parses here exactly as one written with it,
    and nothing in this file notices. That is deliberate -- the separator's absence
    stops GitHub rendering a table at all, which a reader meets before any test would."""
    lines = text.splitlines()
    starts = [i for i, line in enumerate(lines) if line.startswith(heading)]
    assert len(starts) == 1, f"{heading!r} appears {len(starts)} times, expected once"
    rows: list[list[str]] = []
    for line in lines[starts[0] + 1:]:
        if not line.startswith("|"):
            if rows:
                break
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if all(set(c) <= {"-", ":"} and c for c in cells):
            continue
        rows.append(cells)
    assert len(rows) >= 2, f"no table under {heading!r}"
    return rows[0], rows[1:]


def _row_with(rows: list[list[str]], needle: str) -> list[str]:
    """The one row carrying `needle`. Two matches is as much a failure as none."""
    hits = [r for r in rows if any(needle in c for c in r)]
    assert len(hits) == 1, f"{needle!r} matches {len(hits)} rows, expected exactly 1"
    return hits[0]


def _one(text: str, pattern: str) -> re.Match[str]:
    """The one match of `pattern` in `text`."""
    hits = list(re.finditer(pattern, text))
    assert len(hits) == 1, f"{pattern!r} matches {len(hits)} times, expected exactly 1"
    return hits[0]


# --------------------------------------------------------------------------------
# WHAT THE README SAYS.
# --------------------------------------------------------------------------------


_NEEDLES = (
    "registered tables",
    "distinct landing modes",
    "effectivity satellites",
    "pit_estabelecimento",
    "derived views",
    "Asset Bundle jobs",
    "ADRs in",
    "run-evidence",
    "collected",
)


def _stated_table(text: str) -> dict[str, object]:
    """The nine rows of *What is built*, each found by a needle.

    EVERY ROW MUST BE CLAIMED BY A NEEDLE, which is the half a row-by-row read cannot
    supply. Deleting a row reddens the needle that looked for it, but ADDING one was
    invisible: a tenth row carrying an invented count passed, in a table the README
    describes as failing "when the table drifts" -- and a new row is the table drifting.
    That is the mechanism issue #25 rode for twelve days, so the count is asserted rather
    than the rows iterated."""
    _, rows = _table(text, "## What is built")
    claimed = [r for r in rows if any(n in "".join(r) for n in _NEEDLES)]
    assert len(claimed) == len(rows), (
        f"{len(rows) - len(claimed)} row(s) of *What is built* are claimed by no needle "
        f"in {sorted(_NEEDLES)}, so nothing derives them: "
        f"{[r for r in rows if r not in claimed]}"
    )
    landing = _row_with(rows, "distinct landing modes")
    vault = _row_with(rows, "effectivity satellites")
    gold = _row_with(rows, "pit_estabelecimento")
    deploy = _row_with(rows, "Asset Bundle jobs")
    tests = _row_with(rows, "collected")
    breakdown = _ints(vault[-1].split("**")[-1])
    terms = [t.strip() for t in vault[1].split("·")]
    return {
        "bronze_tables": _bold_ints(_row_with(rows, "registered tables")[-1])[0],
        "landing_modes": frozenset(_TICKED.findall(landing[1])),
        "landing_mode_count": _bold_ints(landing[-1])[0],
        "vault_total": _bold_ints(vault[-1])[0],
        "vault_breakdown": frozenset(
            zip((_VAULT_KINDS[t] for t in terms), breakdown, strict=True)),
        "gold_tables": frozenset(_TICKED.findall(gold[1])),
        "gold_count": _bold_ints(gold[-1])[0],
        "dataops_views": _bold_ints(_row_with(rows, "derived views")[-1])[0],
        "bundle_jobs": _bold_ints(deploy[-1])[0],
        "bundle_tasks": _bold_ints(deploy[-1])[1],
        "adrs": _bold_ints(_row_with(rows, "ADRs in")[-1])[0],
        "evidence_docs": _bold_ints(_row_with(rows, "run-evidence and validation")[-1])[0],
        "tests_selected": _bold_ints(tests[-1])[0],
        "tests_collected": _ints(tests[1])[0],
    }


def _stated_prose(text: str) -> dict[str, object]:
    """The counts the README states in sentences rather than in the table."""
    guard = _one(text, r"\*\*([\w-]+) of the ([\w-]+) bundle jobs open with "
                       r"`assert_deployed_revision`")
    return {
        "cnpj_rows": _ints(_one(text, r"\*\*([\d,]+) rows\*\* of CNPJ bronze").group(1))[0],
        "snapshots_per_table": _WORDS[
            _one(text, r"\*\*(\w+) monthly snapshots each\*\*").group(1).lower()],
        "guarded_jobs": _WORDS[guard.group(1).lower()],
        "guard_bundle_jobs": _WORDS[guard.group(2).lower()],
        "adrs_in_prose": _WORDS[
            _one(text, r"one architectural decision per file, ([\w-]+) of them")
            .group(1).lower()],
        "evidence_docs_in_prose": _WORDS[
            _one(text, r"([\w-]+) run-evidence and validation documents, each recording")
            .group(1).lower()],
        "labelled_docs": _WORDS[
            _one(text, r"\*\*(\w+) of them\*\* carry the labelling convention")
            .group(1).lower()],
        "unlabelled_docs": _WORDS[
            _one(text, r"The other \*\*(\w+)\*\* are\s+the oldest").group(1).lower()],
    }


def _stated(text: str) -> dict[str, object]:
    """Everything this file checks, read out of `text`."""
    return {**_stated_table(text), **_stated_prose(text)}


# --------------------------------------------------------------------------------
# WHAT THE REPOSITORY SAYS. Nothing below opens README.md.
# --------------------------------------------------------------------------------


def _from_registries() -> dict[str, object]:
    """The bronze, vault, gold and DataOps declarations, imported and counted.

    Imported HERE and not at module scope, and that is not style. At module scope a
    registry that refuses to import becomes a COLLECTION error, and the run then reports
    "could not import tests/test_readme_counts.py" over a file whose subject is the
    README -- the diagnosis pointing away from the thing that broke."""
    from opl.bronze.registry import REGISTRY as BRONZE
    from opl.dataops.views import DATAOPS_VIEWS
    from opl.gold.registry import REGISTRY as GOLD
    from opl.vault.domains import DOMAINS
    from opl.vault.registry import build_registry

    vault = build_registry(DOMAINS)
    kinds = Counter(type(v).__name__ for v in vault.values())
    return {
        "bronze_tables": len(BRONZE),
        "landing_modes": frozenset(t.landing for t in BRONZE.values()),
        "landing_mode_count": len({t.landing for t in BRONZE.values()}),
        "vault_total": len(vault),
        "vault_breakdown": frozenset(kinds.items()),
        "gold_tables": frozenset(GOLD),
        "gold_count": len(GOLD),
        "dataops_views": len(DATAOPS_VIEWS),
    }


def _from_bundle() -> dict[str, object]:
    """The bundle's jobs, their tasks, and how many open with the provenance guard."""
    import yaml

    jobs: dict[str, dict] = {}
    for path in sorted(_RESOURCES.glob("*.yml")):
        loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        jobs.update((loaded.get("resources") or {}).get("jobs") or {})
    first = [job["tasks"][0]["task_key"] for job in jobs.values() if job.get("tasks")]
    return {
        "bundle_jobs": len(jobs),
        "bundle_tasks": sum(len(job.get("tasks") or []) for job in jobs.values()),
        "guarded_jobs": first.count("assert_deployed_revision"),
        "guard_bundle_jobs": len(jobs),
    }


def _tracked(*args: str) -> list[str]:
    """`git ls-files`, which is the only authority on what is IN this repository."""
    out = subprocess.run(["git", "ls-files", *args], cwd=_REPO, capture_output=True,
                         text=True, check=True).stdout
    return [line for line in out.splitlines() if line]


def _from_git() -> dict[str, object]:
    """The ADR files, the evidence documents, and how many carry the label convention."""
    adrs = _tracked("docs/adr/0*.md")
    docs = [p for p in _tracked("docs") if re.fullmatch(r"docs/[^/]+\.md", p)]
    labelled = [p for p in docs
                if "Controller-verified" in (_REPO / p).read_text(encoding="utf-8")]
    return {
        "adrs": len(adrs),
        "adrs_in_prose": len(adrs),
        "evidence_docs": len(docs),
        "evidence_docs_in_prose": len(docs),
        "labelled_docs": len(labelled),
        "unlabelled_docs": len(docs) - len(labelled),
    }


def _from_collection() -> dict[str, object]:
    """A real default collection, which is the only thing that knows the test counts.

    `PYTEST_ADDOPTS` is dropped from the child's environment on purpose: inherited, a CI
    runner's `-x` or `-k` would silently change the number this compares."""
    import os

    env = {k: v for k, v in os.environ.items() if k != "PYTEST_ADDOPTS"}
    out = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider"],
        cwd=_REPO, capture_output=True, text=True, check=False, env=env).stdout
    match = re.search(r"(\d[\d,]*)/(\d[\d,]*) tests collected", out)
    assert match, f"no collection summary in:\n{out[-2000:]}"
    return {"tests_selected": _ints(match.group(1))[0],
            "tests_collected": _ints(match.group(2))[0]}


def _from_evidence() -> dict[str, object]:
    """The 337,712,651 rows, summed out of `f1.4b-pr-b-run-evidence.md` §21.2 itself."""
    text = _ROW_EVIDENCE.read_text(encoding="utf-8")
    header, rows = _table(text, "### 21.2 Row counts")
    months = [i for i, cell in enumerate(header) if _MONTH.fullmatch(cell)]
    wanted = {"empresas", "socios", "estabelecimentos"}
    cells = [_ints(row[i])[0] for row in rows if row[0] in wanted for i in months]
    assert len(cells) == len(wanted) * len(months), f"§21.2 gave {len(cells)} cells"
    return {"cnpj_rows": sum(cells), "snapshots_per_table": len(months)}


@lru_cache(maxsize=1)
def _derived() -> dict[str, object]:
    """Every expectation, derived once. The collection subprocess costs ~8 s."""
    return {**_from_registries(), **_from_bundle(), **_from_git(),
            **_from_collection(), **_from_evidence()}


def _disagreements(text: str) -> dict[str, tuple[object, object]]:
    """Every fact where `text` and the repository disagree, as (stated, derived)."""
    stated = _stated(text)
    return {key: (stated.get(key), value)
            for key, value in _derived().items() if stated.get(key) != value}


def _readme() -> str:
    return _README.read_text(encoding="utf-8")


# --------------------------------------------------------------------------------
# THE LOCK.
# --------------------------------------------------------------------------------


def test_every_count_in_the_readme_is_what_the_repository_derives():
    """The whole table and every prose count, in one comparison that names its misses."""
    assert _disagreements(_readme()) == {}


def test_the_landing_modes_row_names_the_modes_it_counts():
    """The five backticked modes are the registry's, not just five of something."""
    stated = _stated(_readme())
    assert stated["landing_modes"] == _derived()["landing_modes"]
    assert stated["landing_mode_count"] == len(stated["landing_modes"])


def test_the_gold_row_names_the_tables_it_counts():
    """Same question one layer up: the six names, not the number six."""
    stated = _stated(_readme())
    assert stated["gold_tables"] == _derived()["gold_tables"]
    assert stated["gold_count"] == len(stated["gold_tables"])


def test_the_vault_breakdown_sums_to_the_total_it_is_written_beside():
    """`**20** (3 · 4 · 5 · 2 · 6)` has to be arithmetic as well as true."""
    stated = _stated(_readme())
    assert sum(n for _, n in stated["vault_breakdown"]) == stated["vault_total"]


def test_the_four_sources_table_holds_one_row_per_source_it_announces():
    """Internal consistency, not a derivation: the heading, the sentence and the rows.

    A taxonomy is not derivable from code -- what IS checkable is that adding a row
    without moving the two places that say "four" cannot pass."""
    text = _readme()
    heading = "## The four sources, and how each one arrives"
    _, rows = _table(text, heading)
    assert len(rows) == _WORDS["four"]
    assert _WORDS[_one(text, r"(\w+) kinds of source are ingested").group(1).lower()] \
        == len(rows)


def test_the_commands_the_readme_publishes_are_the_ones_this_file_re_derives():
    """The published block has to reach every derivation, or a reader cannot check it."""
    block = _readme().split("## Re-deriving these numbers", 1)[1]
    for fragment in ("opl.bronze.registry", "opl.vault.registry", "opl.gold.registry",
                     "opl.dataops.views", "databricks/resources/*.yml",
                     "docs/adr/0*.md", "pytest --collect-only",
                     "Controller-verified", "21.2 Row counts",
                     "tests/test_readme_counts.py"):
        assert fragment in block, f"{fragment!r} has no published command"


# --------------------------------------------------------------------------------
# THE FAILURE ARMS. Each mutates the README in memory and asserts the miss is NAMED.
# --------------------------------------------------------------------------------


def _mutated(old: str, new: str) -> str:
    text = _readme()
    assert text.count(old) == 1, f"{old!r} appears {text.count(old)} times"
    return text.replace(old, new)


def test_a_changed_table_count_is_named_and_nothing_else_is():
    """One digit in the bronze row."""
    assert set(_disagreements(_mutated(
        "an Auto Loader read, a batch-scoped DQ gate and a quarantine table | **7** |",
        "an Auto Loader read, a batch-scoped DQ gate and a quarantine table | **8** |",
    ))) == {"bronze_tables"}


def test_a_dropped_landing_mode_is_named_even_though_the_count_still_says_five():
    """The mode NAMES are compared, so renaming one is not hidden by the number."""
    assert set(_disagreements(_mutated("`api`, `postgres`)", "`api`, `pgsql`)"))) \
        == {"landing_modes"}


def test_a_reworded_vault_breakdown_is_named():
    """Moving one of the five sub-counts, leaving the total alone."""
    assert set(_disagreements(_mutated("**20** (3 · 4 · 5 · 2 · 6)",
                                       "**20** (3 · 4 · 6 · 1 · 6)"))) == {"vault_breakdown"}


def test_a_changed_task_count_is_named():
    """The `22 / 102` cell, second number only."""
    assert set(_disagreements(_mutated("| **22 / 102** |", "| **22 / 101** |"))) \
        == {"bundle_tasks"}


def test_a_changed_test_count_is_named():
    """The one count that needs a real collection to contradict.

    THE STATED VALUE IS READ, NEVER TYPED. Pinned to a literal, this arm went red the
    moment the README's count legitimately moved -- `_mutated` refuses a string that is
    no longer there -- so the arm punished the correction instead of the drift. That is
    a hand-maintained count living inside the lock."""
    stated = _stated(_readme())["tests_selected"]
    row = f"(the rest need Docker) | **{stated:,}** |"
    assert set(_disagreements(_mutated(row, row.replace(f"{stated:,}", f"{stated - 1:,}")))) \
        == {"tests_selected"}


def test_a_prose_count_that_drifts_from_its_table_row_is_named():
    """`docs/adr/` says "twenty-one of them" in a bullet and **21** in the table."""
    assert set(_disagreements(_mutated(
        "one architectural decision per file, twenty-one of them",
        "one architectural decision per file, seventeen of them"))) == {"adrs_in_prose"}


def test_a_changed_row_total_is_named():
    """The largest number in the file, against six cells of §21.2."""
    assert set(_disagreements(_mutated("**337,712,651 rows**", "**337,712,650 rows**"))) \
        == {"cnpj_rows"}


def test_a_changed_guard_tally_is_named():
    """ADR 0009's own species: a job list that stops moving when the jobs do."""
    assert set(_disagreements(_mutated(
        "**Twenty-one of the twenty-two bundle jobs",
        "**Seven of the twenty-two bundle jobs"))) == {"guarded_jobs"}


def test_a_changed_label_tally_is_named():
    """How many evidence documents carry Controller-verified, and how many do not.

    THE WORD IS READ OUT OF THE README, never typed, so this arm fails on a DRIFT and
    not on a correction. Typed, it would have gone red the moment F7's documents moved
    the tally -- punishing the fix instead of the defect."""
    word = re.search(r"\*\*(\w+) of them\*\* carry the labelling convention",
                     _readme()).group(1)
    other = next(w for w in _WORDS if w != word.lower() and _WORDS[w] != _WORDS[word.lower()])
    assert set(_disagreements(_mutated(
        f"**{word} of them** carry the labelling convention",
        f"**{other.capitalize()} of them** carry the labelling convention"))) \
        == {"labelled_docs"}
