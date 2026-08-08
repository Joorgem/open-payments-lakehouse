# src/opl/vault/months.py
"""ONE spelling of "which months was I asked for", shared by the observation ledger
and by `opl.vault.loading`, through which every loader in this package reaches it.

(This line read "by both loaders" until Task 7's correction pass. It was written when
the package had two; it now has six, and `opl.vault.loading` is not a loader but the
layer they share -- which is exactly the two importers the next paragraph names.)

WHY THIS MODULE EXISTS AT ALL, since it is thirty lines. `opl.vault.observation` and
`opl.vault.loading` each grew their own copy of the same three refusals -- a bare
`str`, an empty sequence, a value that is not a month -- differing only in the
consequence each spells out. That is a second spelling of one rule, which is the
defect `opl.config` records having been bitten by: two spellings of the month rule
left `2026-13` refused at two of four entry points, and the half that was missed was
the one where an impossible month became a delete boundary. The stakes are lower here
(a window that selects nothing rather than a containment guard that passes vacuously)
and the shape is identical, so it is worth removing while it is still cheap.

THE RULE ITSELF IS NOT HERE EITHER. `opl.config.is_month` is the one spelling of
"YYYY-MM naming a real month" in this repository and this module asks it. What is
here is only the three refusals around it.

`consequence` AND `column` ARE PARAMETERS BECAUSE THE CALLERS REALLY DO DIFFER, and
that difference is worth keeping rather than flattening into one generic message.
An empty window makes the ledger say "nothing was rejected and nothing departed" and
makes a loader say "success, zero rows"; those are different wrong answers and an
operator reading one of them needs to be told which. Same idiom as
`opl.config.require_month`'s `action`.

PURE, AND DELIBERATELY: `opl.config` is the only import, so `opl.vault.observation`
takes on no new dependency by asking, and neither does anything else."""
from __future__ import annotations

from collections.abc import Sequence

from opl.config import is_month


def validated_months(
    months: Sequence[str] | None, *, column: str, consequence: str
) -> tuple[str, ...] | None:
    """`months` as a frozen tuple, or `None` for "every month the data holds".

    All three refusals below produce an EMPTY RESULT rather than an error if they are
    let through, and an empty result is the hardest wrong answer in this layer to
    notice: it has no error attached and it is shaped exactly like the correct answer
    to a different question.

    WHAT THIS CANNOT CHECK, stated so no caller reads more into it: whether a
    well-formed month was ever LOADED. That needs the data, and
    `opl.vault.observation._window` is where it is asked -- see that function for why
    the answer is worth an eager Spark job."""
    if months is None:
        return None
    if isinstance(months, str):
        raise TypeError(
            f"months received a bare str {months!r} -- a str is a Sequence[str] "
            "structurally, so no type checker catches this and it iterates to its "
            f"individual characters; pass a sequence, e.g. [{months!r}]"
        )
    window = tuple(months)
    if not window:
        raise ValueError(
            "months is empty -- name at least one month, or pass None for every month "
            f"the data holds. {consequence}"
        )
    not_months = [value for value in window if not is_month(value)]
    if not_months:
        raise ValueError(
            f"months contains {not_months} -- every value must be YYYY-MM with MM in "
            f"01-12, matching {column} exactly, or it selects no rows at all. "
            f"{consequence}"
        )
    return window
