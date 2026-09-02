# scripts/generate_adr_index.py
"""Render `docs/adr/README.md` from `scripts/adr_index.py`. The write half of the pair.

    uv run python scripts/generate_adr_index.py            # write the index
    uv run python scripts/generate_adr_index.py --check    # exit 1 if it is stale

`--check` exists so a reader who does not want to touch the tree can still ask the
question the test asks. It is the same comparison and it is deliberately NOT the thing
that locks the index: `tests/test_adr_index.py` re-derives the page's facts from the
ADR files with its own extractor rather than calling this module, because a check that
runs the generator and compares against the generator's own output only asserts that a
function is deterministic.

WRITTEN AS BYTES, LF, ASSERTED. `pathlib.write_text` silently emits CRLF on Windows, so a
text-mode write would give this page one line ending on one platform and another
elsewhere, and arrive as a whole-file diff with no content change in it. The assertion
below is cheap and the defect it refuses is invisible in review. IT IS A CLAIM ABOUT THIS
FILE AND NOTHING ELSE: an earlier draft of this docstring said "every file under
`docs/adr/` is uniformly LF", and `git ls-files --eol docs/adr/` refutes it. An ADR body
can carry CRLF in the working tree while its committed blob is LF -- under
`core.autocrlf=input` git normalises on read, so `git diff` is silent about it. Nothing
here converts them, and nothing here should.
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent


def _sibling(name: str):
    """`scripts/<name>.py`, loaded by path and registered ONCE under `name`.

    NOT `sys.path.insert(...)` followed by a plain import, which is what this file did
    first: that mutates the interpreter's module search path for everything running after
    it, in a process this script does not own, and never unwinds it. Registered in
    `sys.modules` because `tests/test_adr_index.py` loads the same file by path and must
    get THIS object -- two live copies of `adr_index` would each read the ADRs and could
    disagree about what is declared."""
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {name} from {_SCRIPTS}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


idx = _sibling("adr_index")

_GENERATOR = "scripts/generate_adr_index.py"
_TEST = "tests/test_adr_index.py"


def _cell(text: str) -> str:
    """One markdown table cell. Pipes escaped, because condition prose contains them."""
    return text.replace("|", "\\|")


def _status_cell(adr: idx.Adr) -> str:
    """The status column, with absence printed as absence.

    Three ADRs carry no `## Status` section. Rendering them as `Accepted` would be the
    index inventing the one value that is true of almost every ADR ever written, so
    they are marked, and the marking is what a reader can act on."""
    if adr.status is None:
        return "**no `## Status`**"
    return f"{adr.status}, qualified" if adr.status_is_qualified else adr.status


def _index_table(adrs: tuple[idx.Adr, ...]) -> list[str]:
    """One row per ADR: number, title, phase, status, and two derived counts."""
    rows = [
        "| # | decision | phase | status | `## Decision` sections | reversal conditions |",
        "|---|---|---|---|---|---|",
    ]
    for adr in adrs:
        phase, merge, _branch = idx.PHASES[adr.number]
        rows.append(
            f"| [{adr.number}]({adr.path}) "
            f"| {_cell(adr.title)} "
            f"| {phase} (`{merge}`) "
            f"| {_status_cell(adr)} "
            f"| {len(adr.decision_headings)} "
            f"| {len(adr.conditions) or '**none stated**'} |"
        )
    return rows


def _numbered_decisions(adrs: tuple[idx.Adr, ...]) -> list[str]:
    """Every `## Decision N` heading, verbatim, for the ADRs that number them.

    THIS IS THE COLUMN THE BRIEF ASKED FOR AS *"its load-bearing decision in one line"*,
    and it is a section rather than a column for a reason worth stating. Most of these
    titles ALREADY ARE that sentence -- *"there is no `hub_socio`, and the
    effectivity satellite is driven by disappearance rather than by replacement"* is not
    a topic, it is the decision. A sixth column beside it would be either a duplicate or
    a hand-written paraphrase of every ADR, in the one file whose whole purpose is to
    kill hand-maintained lists. The multi-decision ADRs are the case the titles do NOT
    cover, because a title cannot carry eight decisions -- so those are enumerated, from
    their own headings, and the reversal table cites them by number."""
    out = []
    for adr in adrs:
        numbered = [h for h in adr.decision_headings if h != "Decision"]
        if not numbered:
            continue
        out.append("")
        out.append(f"**[{adr.number}]({adr.path}) — {_cell(adr.title)}**")
        out.append("")
        out.extend(f"- {_cell(heading)}" for heading in numbered)
    return out


def _numbered_decisions_intro(adrs: tuple[idx.Adr, ...]) -> str:
    """COUNTED, not typed.

    The first draft of this line said *"the five ADRs whose decisions are numbered"* and
    was already wrong when it was written -- ADR 0006 carries a `## Decision 3, resolved`
    beside its original `## Decision`, so there are six. A hand-typed count inside the
    page whose subject is hand-maintained lists is the joke told twice, and it is left
    recorded here rather than quietly fixed."""
    numbered = [a for a in adrs if any(h != "Decision" for h in a.decision_headings)]
    return (
        f"The **{len(numbered)}** ADRs whose decisions are numbered, from their own "
        "headings. The reversal table below cites them by number."
    )


def _reversal_rows(adrs: tuple[idx.Adr, ...]) -> list[str]:
    """One row per STATED condition, over every ADR, in file order."""
    rows = [
        "| ADR | condition, as the ADR states it | state | read | on what |",
        "|---|---|---|---|---|",
    ]
    for adr in adrs:
        readings = idx.readings_for(adr)
        for condition in adr.conditions:
            reading = readings.get(condition.text)
            where = f"{adr.number}{' ' + condition.decision if condition.decision else ''}"
            rows.append(
                f"| [{where}]({adr.path}) "
                f"| {_cell(condition.text)} "
                f"| **{reading.state if reading else idx.NOT_READ}** "
                f"| {reading.date if reading else '—'} "
                f"| {_cell(reading.why) if reading else '*nobody has taken this reading*'} |"
            )
    return rows


def _reversal_counts(adrs: tuple[idx.Adr, ...]) -> str:
    """The tallies, computed here so they cannot disagree with the table under them."""
    stated = [c for adr in adrs for c in adr.conditions]
    read = sum(len(idx.readings_for(adr)) for adr in adrs)
    with_none = [adr.number for adr in adrs if not adr.conditions]
    by_state: dict[str, int] = {}
    for adr in adrs:
        for reading in idx.readings_for(adr).values():
            by_state[reading.state] = by_state.get(reading.state, 0) + 1
    tally = ", ".join(f"{n} {state}" for state, n in sorted(by_state.items()))
    return (
        f"**{len(stated)} conditions, stated by {len(adrs) - len(with_none)} of the "
        f"{len(adrs)} ADRs. {read} have been read** ({tally}); the remaining "
        f"{len(stated) - read} are marked `NOT READ`, which is not `NOT MET`. "
        f"**{len(with_none)} ADRs state no reversal condition at all** — "
        + ", ".join(with_none)
        + " — and that is a finding rather than an omission of this page's."
    )


def _preamble() -> list[str]:
    return f"""# Architecture Decision Records

**Generated. Do not edit this file.** `{_GENERATOR}` writes it from the ADRs
themselves; `{_TEST}` fails when the two disagree — a new ADR, a renamed title, a
changed status, a reworded reversal condition.

```bash
uv run python {_GENERATOR}            # rewrite this page
uv run python {_GENERATOR} --check    # exit 1 if it is stale
uv run pytest {_TEST}
```

**Three of these facts are read out of the files and two are declared, and the
difference matters when you are deciding what to trust.** The **title**, the **status**
and the **`## Decision` / reversal-condition structure** are parsed from each ADR, so
they cannot drift: nothing stores them twice. The **phase** and every reversal
**reading** cannot be derived from the files — they are declarations in
`scripts/adr_index.py`, each carrying the commit or the measurement that proves it, and
each locked by a totality assertion that refuses to render when the ADR set moves
underneath it.

> **Why the phase is declared and not derived.** The derivation is real —
> `git rev-list --ancestry-path <adding commit>..origin/main | tail -1` lands on a merge
> whose branch names the phase, and the sha beside each phase below is that merge. But
> CI's `test` job checks out at `actions/checkout@v4`'s default `fetch-depth: 1`, so
> that history is not there when the lock runs. A column that degrades quietly under a
> shallow clone is worse than one that is declared and cross-checked where git can
> answer. An ADR written in a phase that has not merged yet has no such sha to declare,
> so it reads `unmerged` — and the lock refuses that word the moment git says the ADR
> reached `main`.
""".split("\n")


# THE GRAMMAR THE EXTRACTOR STATES AS ITS FLOOR, and the mask the two readings SHARE.
# A constant rather than prose inside `_limits`, so this can grow without pushing that
# function over the 50-line cap.
_GRAMMAR = """**And the grammar underneath them, which is the floor a condition has to clear to be
counted at all.** Code fences and the HTML that renders as nothing — comments, and
`<pre>`, `<script>`, `<style>` and `<textarea>` blocks — are masked out before either
reading parses anything, so a heading or a `**What reverses it:**` line inside one neither
ends a section early nor invents a condition. Inside a conditions section a bullet is `-`,
`*`, `+` or `N.`, indented at most three spaces; one indented four or more is not counted
as a condition of its own. Every reversal section in a file is read, not only the first.
Each condition is flattened to a single line and cut at the first blank line, so the
argument under it stays in the ADR, which is the better document for it.

**The two readings SHARE that mask, and therefore share its blind spots.** Everything else
about them differs — this generator captures with multi-line regexes, the test walks lines
— but the mask is one algorithm typed twice, so neither reading can catch the other being
wrong about a fence or an HTML block. It is typed twice rather than imported for one
reason: `tests/test_adr_index.py` has to go on reading the ADR files when
`scripts/adr_index.py` REFUSES to import, which is what it does the moment an ADR arrives
with no declared phase. A test asserts the two masks agree over a battery of constructs,
so a one-sided edit to either goes red."""


def _limits(adrs: tuple[idx.Adr, ...]) -> list[str]:
    """The extractor's declared floor. WHICH ADRs use each spelling is derived, not typed.

    The first draft named them as the ranges `(0006)`, `(0010–0015)` and `(0018–0020)`,
    which nothing compared to the files -- a hand-typed list inside the page whose subject
    is hand-typed lists. It also named only the three spellings and said nothing about the
    grammar under them, which is where the extractor actually loses conditions."""
    using = idx.adrs_by_spelling(adrs)

    def _who(spelling: str) -> str:
        return ", ".join(using[spelling]) or "*no ADR uses this spelling today*"

    return f"""## What this page cannot see

**The reversal-condition extractor reads three spellings and nothing else.** Which ADRs
use each is read out of the files, not typed here:

- a `### What would reverse this decision` section — {_who(idx.SECTION_REVERSE)}
- a `### What would change this decision` section — {_who(idx.SECTION_CHANGE)}
- an inline `**What reverses it:**` paragraph under a numbered decision — {_who(idx.INLINE_MARKER)}

{_GRAMMAR}

Two things this leaves uncounted, and neither is hidden:

- **ADR 0013 carries a fourth spelling** in its Consequences — *"A second month-pair
  could reverse this"* — which is not counted here.
- **ADR 0018's Consequences claim four refusals "each with a reversal condition"** while
  only its Decision 1 uses the marker.

**A count whose extractor's limit is not written down is a count nobody can check.**
Widen the extractor in `scripts/adr_index.py` if a fourth spelling is worth adopting;
do not widen it by editing this file.
""".split("\n")


def render() -> str:
    adrs = idx.ADRS
    lines = [
        *_preamble(),
        # COUNTED, NEVER TYPED. This heading was the literal `## The twenty` until the
        # review of this task put a twenty-first ADR in the tree: every computed number on
        # the page moved, the heading did not, and nothing in the suite could see it. The
        # pass/skip counts once quoted here came from an older draft of the test file and
        # did not reproduce, so they are deleted rather than re-measured.
        f"## All {len(adrs)} ADRs",
        "",
        *_index_table(adrs),
        "",
        "## The numbered decisions",
        "",
        _numbered_decisions_intro(adrs),
        *_numbered_decisions(adrs),
        "",
        "## Reversal conditions",
        "",
        _reversal_counts(adrs),
        "",
        "`MET` and `NOT MET` are measurements. **`LOOKS MET, IS NOT` is the state this"
        " table exists for**: something arrived that resembles the condition and does"
        " not satisfy it, which collapsed into a boolean reads as `MET` — and that is"
        " how a decision gets reversed by a resemblance. `UNCLOSABLE` means no change"
        " to this repository can ever close it. `NOT READ` means nobody has looked.",
        "",
        *_reversal_rows(adrs),
        "",
        *_limits(adrs),
    ]
    return "\n".join(lines).rstrip("\n") + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate docs/adr/README.md.")
    parser.add_argument(
        "--check", action="store_true", help="exit 1 if the index on disk is stale"
    )
    args = parser.parse_args(argv)
    body = render().encode("utf-8")
    if b"\r" in body:
        raise ValueError("the rendered index carries CR bytes; this page is written LF-only")
    if args.check:
        current = idx.INDEX.read_bytes() if idx.INDEX.exists() else b""
        if current != body:
            print(f"{idx.INDEX} is STALE; run `uv run python {_GENERATOR}`")
            return 1
        print(f"{idx.INDEX} is current")
        return 0
    idx.INDEX.write_bytes(body)
    lines = body.count(b"\n")
    print(f"wrote {idx.INDEX}: {len(body)} bytes, {lines} lines, 0 CR")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
