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

THE RULE ITSELF IS NOT HERE EITHER, AND SINCE F-DB IT IS NOT ONE RULE. It arrives on
a `SnapshotAxis` -- the source's own answer to "what does one value of my snapshot
column look like" -- and the monthly axis asks `opl.config.is_month`, which stays the
one spelling of "YYYY-MM naming a real month" in this repository. What is here is only
the three refusals around whichever predicate the axis carries.

WHY THE AXIS AND NOT A `column` STRING, which is what this parameter was. The column
name alone makes the refusal MESSAGE right and the refusal itself wrong: a source
observed at instants rather than months would have every one of its window values
rejected here as "not YYYY-MM", at three call sites, one of them before Spark starts.
The name and the shape rule are one fact about one source and they have to travel
together; `opl.bronze.snapshot_axis` is where that is argued.

`consequence` IS STILL A PARAMETER BECAUSE THE CALLERS REALLY DO DIFFER, and that
difference is worth keeping rather than flattening into one generic message. An empty
window makes the ledger say "nothing was rejected and nothing departed" and makes a
loader say "success, zero rows"; those are different wrong answers and an operator
reading one of them needs to be told which. Same idiom as `opl.config.require_month`'s
`action`.

STILL PURE, AND DELIBERATELY: the axis module imports `re`, `dataclasses` and
`opl.config` and nothing else, so `opl.vault.observation` takes on no Spark dependency
by asking and neither does anything else. The function name is unchanged because every
caller in the tree still passes months; it is the TYPE of the window that has been
generalised, not the vocabulary of the jobs that pass one."""
from __future__ import annotations

from collections.abc import Iterable, Sequence

from opl.bronze.snapshot_axis import SnapshotAxis


def validated_months(
    months: Sequence[str] | None, *, axis: SnapshotAxis, consequence: str
) -> tuple[str, ...] | None:
    """`months` as a frozen tuple, or `None` for "every month the data holds".

    All three refusals below produce an EMPTY RESULT rather than an error if they are
    let through, and an empty result is the hardest wrong answer in this layer to
    notice: it has no error attached and it is shaped exactly like the correct answer
    to a different question.

    WHAT THIS CANNOT CHECK, stated so no caller reads more into it: whether a
    well-formed month was ever LOADED. That needs the data, so it is a second function --
    `refuse_unloaded_months` below -- which the callers reach once they have the months
    the data actually holds. See it for why the answer is worth an eager Spark job."""
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
    not_months = [value for value in window if not axis.accepts(value)]
    if not_months:
        raise ValueError(
            f"months contains {not_months} -- every value must be {axis.shape}, "
            f"matching {axis.column} exactly, or it selects no rows at all. "
            f"{consequence}"
        )
    return window


def refuse_unloaded_months(
    months: Sequence[str],
    *,
    loaded: Iterable[str],
    tables: Sequence[str],
    consequence: str,
) -> None:
    """Refuse a named month that no row of `tables` carries.

    THE SECOND HALF OF THE MONTH RULE, AND THE HALF `validated_months` STRUCTURALLY
    CANNOT DO. That one checks the SHAPE of a value before Spark exists; this one checks
    the value against the data, so it can only run once somebody has collected the
    months the tables hold. `2026-09` is well-formed and may name a typo, or a month
    whose ingest failed.

    ONE SPELLING, TWO CALLERS, WHICH IS WHY IT IS HERE AND NOT INLINE IN EITHER OF THEM.
    `opl.vault.observation._window` asks it over bronze UNION quarantine, because an
    unloaded month there manufactures a candidate delete for the whole key space; and
    `opl.vault.satellite_grain` asks it over the ONE source a satellite with no
    observation grain reads, because there an unloaded month selects no rows and the
    load reports success having written nothing. Same rule, two consequences, and
    `consequence` is a parameter for `validated_months`' reason: those are different
    wrong answers and an operator reading one of them needs to be told which.

    `tables` IS A SEQUENCE AND IS NAMED IN THE MESSAGE, because the refusal is only
    actionable if the reader knows what was consulted. Two tables read "no row of 'a' or
    'b' carries", which is what `_window`'s own message said before this function
    existed and what the ledger's tests match on."""
    unloaded = sorted(set(months) - set(loaded))
    if not unloaded:
        return
    consulted = " or ".join(repr(table) for table in tables)
    raise ValueError(
        f"months names {unloaded}, which no row of {consulted} carries. Refusing rather "
        f"than answering: {consequence}"
    )
