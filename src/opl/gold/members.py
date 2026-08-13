# src/opl/gold/members.py
"""What each conformed dimension CONTAINS, as plain Python rows -- the calendar's
arithmetic and the enumerated domains' shape, with no pyspark anywhere.

PURE, FOR `opl.gold.columns`' REASON AND ONE MORE OF ITS OWN. A date dimension is the
one part of this layer with real arithmetic in it -- month boundaries, weekday
numbering, quarter derivation -- and arithmetic asserted through a Spark session is
arithmetic asserted at seconds per case. Fifty rows built in the driver and handed to
`createDataFrame` cost nothing at this scale and can be checked in milliseconds, so the
calendar's correctness does not depend on a JVM being installed.

THE NAMES ARE DECLARED TUPLES AND NEVER `calendar.month_name` OR `strftime('%A')`, and
this is a defect avoided rather than a style. Both of those read the process LOCALE: the
same build, run on a Portuguese-configured box, writes `agosto` into a column another run
wrote `August` into, with nothing failing and two populations of rows in one table
disagreeing about the same day. A lakehouse whose column values depend on the operating
system's language settings has a reproducibility problem it cannot see.

WEEKDAY NUMBERING IS ISO -- MONDAY IS 1 -- AND IT IS STATED BECAUSE THE ALTERNATIVES ARE
LIVE. Python's `date.weekday()` is Monday = 0, its `isoweekday()` is Monday = 1, and
Spark's own `dayofweek` is SUNDAY = 1. Three conventions, all reachable from this file's
neighbourhood, differing by exactly the amount that makes a "weekend" filter select
Friday and Saturday. `is_weekend` is carried as its own column for that reason: a reader
should never have to know which convention `day_of_week` follows in order to ask the
question everybody actually asks of it.

THE CALENDAR IS CONTIGUOUS AND INCLUSIVE AT BOTH ENDS, unlike every interval in
`opl.gold.columns`. That is not an inconsistency: those are as-of intervals over
CONTINUOUS time, where a half-open bound is what makes every instant belong to exactly
one version; this is an enumeration of discrete days, where "up to but not including
2026-08-01" would simply be missing the day the payments are on."""
from __future__ import annotations

from datetime import date, timedelta

__all__ = [
    "CALENDAR_ATTRIBUTES",
    "DAY_NAMES",
    "MONTH_NAMES",
    "calendar_members",
    "calendar_schema",
    "enumerated_members",
    "enumerated_schema",
]

# Indexed by month number minus one, and by ISO weekday minus one. Declared rather than
# formatted -- see the module docstring for the locale defect that avoids.
MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)
DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

# THE ATTRIBUTE COLUMNS EVERY CALENDAR ROW CARRIES, AFTER ITS NATURAL KEY AND IN THIS
# ORDER. The order is load-bearing, not tidy, for `opl.gold.dimensions._versioned`'s
# reason: a Delta append matches POSITIONALLY unless `mergeSchema` says otherwise, so two
# builds projecting these in two orders would write each other's values into each other's
# columns without failing.
CALENDAR_ATTRIBUTES = (
    "year",
    "quarter",
    "month",
    "month_name",
    "day_of_month",
    "day_of_week",
    "day_name",
    "is_weekend",
)

_CALENDAR_TYPES = ("int", "int", "int", "string", "int", "int", "string", "boolean")

# Saturday and Sunday, in the ISO numbering this module uses throughout.
_WEEKEND = (6, 7)


def calendar_schema(natural_key: str) -> str:
    """The DDL for a calendar row: the natural key as a DATE, then the attributes.

    THE NATURAL KEY'S NAME IS THE SPEC'S AND IS NOT SPELLED HERE, which is the same
    division `opl.gold.registry` draws for table names: this module knows what a calendar
    row CONTAINS, and `opl.gold.specs.CalendarDimension` knows what the column that holds
    the day is called. A `full_date` literal here would be a second spelling of a name the
    fact must join on."""
    attributes = ", ".join(
        f"{name} {kind}" for name, kind in zip(CALENDAR_ATTRIBUTES, _CALENDAR_TYPES, strict=True)
    )
    return f"{natural_key} date, {attributes}"


def _calendar_row(day: date) -> tuple:
    """One day, as its natural key followed by `CALENDAR_ATTRIBUTES` in order."""
    weekday = day.isoweekday()
    return (
        day,
        day.year,
        (day.month - 1) // 3 + 1,
        day.month,
        MONTH_NAMES[day.month - 1],
        day.day,
        weekday,
        DAY_NAMES[weekday - 1],
        weekday in _WEEKEND,
    )


def calendar_members(first: date, last: date) -> tuple[tuple, ...]:
    """Every calendar day from `first` to `last`, both included, in order.

    REFUSES AN INVERTED SPAN RATHER THAN RETURNING NOTHING, and the distinction is the
    whole point: an empty calendar is a `dim_date` holding nothing but its ghost, which
    every fact row then resolves to -- so a build with its bounds the wrong way round
    would report a clean load of a dimension that answers "unknown" about every payment.
    An exception here is the difference between a bug that is seen and a number that is
    believed."""
    if last < first:
        raise ValueError(
            f"a calendar from {first.isoformat()} to {last.isoformat()} ends before it "
            "starts, so it would hold no days at all -- and a date dimension holding "
            "nothing but its ghost resolves every fact row to the unknown member with "
            "the build reporting success"
        )
    span = (last - first).days
    return tuple(_calendar_row(first + timedelta(days=offset)) for offset in range(span + 1))


def enumerated_schema(natural_key: str) -> str:
    """The DDL for an enumerated member: one string column, the natural key.

    ONE COLUMN, AND THE ABSENCE OF A SECOND IS A DECISION. Kimball's `dim_channel` would
    normally carry a code and a display name; this contract declares codes only (`PIX`,
    `TED`, `BOLETO`, ...), so a `channel_name` column would hold text this repository
    invented for a value somebody else's payment rails own. `opl.gold.columns` draws the
    same line for the ghost's payload and for the same reason."""
    return f"{natural_key} string"


def enumerated_members(members: tuple[str, ...]) -> tuple[tuple[str], ...]:
    """The declared domain, one row per member, in DECLARATION order.

    Order is preserved rather than sorted because the surrogate key is a hash of the
    member itself and never of its position -- so nothing here depends on the order, and
    a reader comparing the table against `opl.contracts.payments` should see the same
    sequence in both rather than have to re-sort one of them."""
    return tuple((member,) for member in members)
