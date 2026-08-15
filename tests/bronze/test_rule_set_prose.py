# tests/bronze/test_rule_set_prose.py
"""The rule-set SUMMARY in `opl.bronze.rules` against the rule sets it summarises.

WHY A TEST ABOUT COMMENTS EXISTS AT ALL. `rules.py` carries two long comment blocks that
enumerate what each contract's gate can and cannot fire, and that block -- not the dict
below it -- is what a reader consults before changing a rule. F-API found four separate
stale claims surviving inside files whose own subject was a stale claim, and every one of
them had the same mechanism: prose outlived the code it described because nothing compared
the two. This is the cheapest comparison that is still mechanical -- names, in both
directions, with no natural language read at all.

WHAT IT CANNOT DO, so it is not mistaken for more. It checks that the summary NAMES the
right rules; it cannot check that what the summary SAYS about a rule is true. The encoding
fold's shadowing, which this same pass wrote into that block, is a claim about ordering that
only a Spark test can hold (`test_a_replacement_character_is_caught_but_only_currency_
REPORTS_it`). What this closes is the drift that has actually happened twice: a rule set
edited while the paragraph above it stayed as it was.

ITS OWN FILE because `tests/bronze/test_rules.py` stands at 759 of this project's 800-line
limit and is a Spark suite; this needs no session and would pay for one on every run of it.
The seam is the same one that file's docstring names -- what changes here is the PROSE
contract, not the gate's behaviour.
"""
from __future__ import annotations

import re
from pathlib import Path

from opl.bronze import rules as rules_module
from opl.bronze.registry import REGISTRY
from opl.bronze.rules import rules_for

# A backticked lower-case identifier, optionally ending in the `*` this codebase uses for a
# derived family (`null_or_empty_*`, one rule per contract column).
_BACKTICKED = re.compile(r"`([a-z][a-z0-9_]*\*?)`")

# The prefixes every reject reason in this repository is built from. Anything backticked in
# the prose that starts with one of these is being named AS A RULE, which is what makes the
# reverse direction below possible without parsing English.
_REASON_PREFIXES = ("null_or_empty", "bad_", "unparseable_", "unprovable_", "encoding_")

# `rescued_data_present` is `opl.bronze.dq`'s own reason, ranked above every per-table rule
# and produced by no rule set, so the summary names it legitimately while `rules_for` never
# yields it. One exception, declared with its reason rather than left as a mystery pass.
_NOT_A_TABLE_RULE = frozenset({"rescued_data_present"})


def _prose() -> str:
    """`rules.py`'s comment lines, with names glued back together across line wraps.

    COMMENTS ONLY, not docstrings: the two summary blocks are comments, and a docstring
    sweep would also pick up every function's own prose about its own rule, where a name
    appearing is guaranteed rather than informative.

    The glue matters. This repository wraps a long identifier by breaking after an
    underscore and continuing on the next comment line, so `null_or_empty_\n# cnpj_basico`
    is one name written over two lines; without the join it would read as a name ending in
    `_` and the rule it documents would look undocumented."""
    source = Path(rules_module.__file__).read_text(encoding="utf-8").splitlines()
    comments = [
        line.strip().lstrip("#").strip()
        for line in source
        if line.lstrip().startswith("#")
    ]
    return re.sub(r"_\n", "_", "\n".join(comments))


def _named_in_the_prose() -> tuple[set[str], set[str]]:
    """(exact names, `*`-family prefixes) the comment blocks name as rules."""
    named = {
        name for name in _BACKTICKED.findall(_prose()) if name.startswith(_REASON_PREFIXES)
    }
    return {n for n in named if not n.endswith("*")}, {
        n.rstrip("*") for n in named if n.endswith("*")
    }


def _live_rules() -> set[str]:
    """Every reason any registered contract's rule set can produce."""
    return {
        name
        for spec in REGISTRY.values()
        for name, _ in rules_for(spec.contract)
    }


def test_every_rule_any_contract_produces_is_named_in_the_summary():
    """A rule added without a line in the block above `rules_for` is a rule an operator
    meets first in a quarantine table.

    A family is allowed to be named by its `*` form -- `null_or_empty_*` is one line for as
    many rules as the contract has columns, and spelling twenty-eight of them out would be a
    second copy of the contracts. Anything else has to be named exactly."""
    exact, families = _named_in_the_prose()
    undocumented = sorted(
        name
        for name in _live_rules()
        if name not in exact and not any(name.startswith(f) for f in families)
    )
    assert not undocumented, (
        f"{undocumented} can be the recorded reject reason on a live table and the rule-set "
        "summary in rules.py does not name them. The block above `rules_for` is what a "
        "reader consults for what can and cannot fire; add the rule to it."
    )


def test_every_rule_the_summary_names_is_one_some_contract_still_produces():
    """THE DIRECTION THIS PHASE KEPT FAILING: prose outliving its subject.

    A rule renamed or dropped leaves its paragraph behind, describing a reason no
    quarantine table can ever hold -- and the paragraph reads exactly as authoritative as
    the ones that are still true. That is how `length(trim(quote_date)) = 10` survived in
    three places after the CHECK became a regex.

    A `*` family must still match at least one live rule, so deleting a contract's required
    rules would not leave `null_or_empty_*` standing on its own."""
    exact, families = _named_in_the_prose()
    live = _live_rules()
    stale = sorted((exact - live) - _NOT_A_TABLE_RULE)
    assert not stale, (
        f"the rule-set summary in rules.py names {stale}, which no registered contract's "
        "rule set produces. Either the rule was renamed and the prose was not, or it was "
        "deleted and its paragraph stayed -- retire the paragraph with the rule."
    )
    empty_families = sorted(
        family
        for family in families
        if not any(name.startswith(family) for name in live)
    )
    assert not empty_families, f"{empty_families} describes a family with no live members"


def test_the_sweep_is_reading_the_prose_and_not_an_empty_string():
    """Guard the guard, and it is the one that matters for a text sweep: both assertions
    above pass trivially over prose that was never found -- a moved file, a comment style
    that stopped starting at `#`, a regex that matches nothing.

    The numbers are floors rather than exact counts on purpose: an exact count here would
    itself be a claim about the file going stale on the next rule, which is the species this
    file exists to catch."""
    exact, families = _named_in_the_prose()
    assert len(exact) >= 5, f"only {sorted(exact)} was found in rules.py's comments"
    assert families, "the `*` family convention is no longer readable from the prose"
    assert len(_live_rules()) >= 20
