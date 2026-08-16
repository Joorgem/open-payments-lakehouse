# src/opl/bronze/snapshot_axis.py
"""WHICH COLUMN SAYS "WHEN WE OBSERVED THIS", AND WHAT ITS VALUES LOOK LIKE -- one
declaration per SOURCE, rather than one constant per repository.

THE DEFECT THIS EXISTS TO REMOVE. `opl.vault.observation` derives its ledger at the
grain of (business key, snapshot axis) and the axis was a module constant,
`_snapshot_month`, whose values are `YYYY-MM`. Every source this lakehouse had was a
monthly file drop, so the constant was true. It stops being true for a source observed
TWICE IN ONE CALENDAR MONTH: both observations carry the same `_snapshot_month`, the
ledger folds them into one row per key, and a key present in the first and gone from
the second is `observed` -- not `absent_after_observation`. No error, no red test, and
the one state the vault's end-dating path reads has no producer. A Postgres snapshot
diff is exactly that source.

THE AXIS IS A PROPERTY OF THE SOURCE, WHICH IS WHY IT IS DECLARED ON `BronzeTable` AND
NOT ON A GRAIN. `databricks/src/vault_load_satellite.grain_for` and its effectivity
twin build every `ObservationGrain` generically from a `(Hub|Link, BronzeTable)` pair
with no mapping anywhere; a field on the grain alone would have to be re-decided at
each of those call sites, and would not reach `opl.vault.loading.read_snapshot_window`
or the vault jobs' window parameter, both of which also read the axis. Declared here,
carried onto the grain by that same derivation, it is decided once per source.

WHY THIS FILE AND NOT `opl.bronze.snapshot`, which already owns the column NAMES.
`snapshot.py` imports pyspark: it is Spark `Column` expressions and nothing else. This
value has to be readable by `opl.bronze.registry`, and
`test_the_registry_still_imports_where_pyspark_is_not_installed` pins that the registry
imports in an environment with no Spark -- the extraction scripts run there. So the
declaration and the expressions cannot share a module, and the declaration is the half
that must stay pure. Nothing here imports anything but `re`, `dataclasses` and
`opl.config`.

THE SHAPE RULE TRAVELS WITH THE COLUMN, and that is the half a "just a column name"
version would have left broken. `opl.vault.months.validated_months` refuses a window
value that is not `YYYY-MM`, and it is called from THREE places -- the ledger, every
loader through `opl.vault.loading`, and `opl.vault.job_params.required_months`, which
runs before Spark starts. A source whose axis is an instant would be refused at all
three by a validator that still knew only about months, so the axis carries the
predicate that decides what one of its values looks like.

THE MONTHLY AXIS ASKS `opl.config.is_month` RATHER THAN CARRYING A REGEX. That module
is the one spelling of "YYYY-MM naming a real month" in this repository, and a second
copy here is precisely the defect it records having been bitten by -- two spellings of
the month rule left `2026-13` refused at two of four entry points. `_is_instant` below
asks it too, for its leading seven characters, for the same reason.

LEXICOGRAPHIC ORDER MUST BE CHRONOLOGICAL ORDER, for every axis declared here. The
ledger's absence split is a string comparison (`observation._state_column`: `month <
first_observed_month`) and `opl.vault.loading.earliest_record_source` takes a `min`
over the axis. `YYYY-MM` has that property. So does the fixed-width UTC instant below,
and BOTH of its qualifiers are load-bearing: fixed width, because `::text` trims
trailing fractional zeros and `'...01.1'` sorts after `'...01.09'`; UTC, because an
offset-bearing rendering sorts by local wall clock and orders two instants backwards
across a zone change."""
from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass

from opl.config import MONTH_PATTERN, is_month


@dataclass(frozen=True)
class SnapshotAxis:
    """One source's answer to "when did we observe this row", as a bronze column plus
    the rule for what one of its values looks like.

    Frozen and compared by value, so `BronzeTable` and `ObservationGrain` holding the
    same axis are holding the same thing rather than two that happen to agree today.

    `accepts` IS A FIELD AND NOT A METHOD, deliberately. A method would put the
    predicate in the class and make every axis share it -- which is the module constant
    this type exists to replace, reintroduced one level up. As an instance field it is
    stored in the instance `__dict__`, so `axis.accepts(value)` reads back the plain
    function and is never bound as a method; there is no default, so no callable ever
    reaches the class body where it would be.

    `shape` IS PROSE FOR A REFUSAL AND IS NOT PARSED. `validated_months` puts it in the
    message an operator reads when a window value is rejected before Spark starts, and
    "every value must be YYYY-MM with MM in 01-12" is the sentence that sends them to
    the right fix. A generic "the value did not match" would not."""

    name: str
    column: str
    shape: str
    accepts: Callable[[str], bool]


MONTHLY_SNAPSHOT = SnapshotAxis(
    name="monthly snapshot",
    column="_snapshot_month",
    shape="YYYY-MM with MM in 01-12",
    accepts=is_month,
)

# `to_char(<timestamptz> AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`, which is
# what a Postgres extraction renders its snapshot instant with. Twenty-seven characters,
# always: `.US` is six digits with the trailing zeros KEPT, where `::text` would trim
# them and break the sort this module's docstring requires.
INSTANT_WIDTH = 27

# Everything from the day onward. The YEAR AND MONTH ARE NOT MATCHED HERE -- `_is_instant`
# hands those to `is_month`, so this pattern cannot become a second spelling of the month
# rule.
_INSTANT_TAIL_PATTERN = (
    r"-(0[1-9]|[12][0-9]|3[01])"  # day
    r"T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"  # hour:minute:second
    r"\.[0-9]{6}Z"  # microseconds, then the UTC marker
)
# Anchored at both ends because it is matched against a SLICE rather than the whole value,
# and an unanchored tail would accept trailing rubbish inside a correct width.
_INSTANT_TAIL = re.compile(f"^{_INSTANT_TAIL_PATTERN}$")

# THE WHOLE VALUE, IN ONE PATTERN, for a consumer that cannot slice -- which is what a
# Spark `rlike` is. `_is_instant` below asks `is_month` about the first seven characters
# and this pattern about the rest; a DQ rule running inside the engine has no `is_month` to
# ask, so it needs the month half written out. Composed from `opl.config.MONTH_PATTERN` and
# the tail above rather than typed again, so there is exactly one spelling of each half and
# a change to either reaches both consumers. THE ONLY THING THAT IS NOT SHARED IS THE
# WIDTH CHECK, and it is not redundant on the Spark side even though this pattern is
# anchored: Java's `$` matches before a trailing line terminator, so a landed value with a
# newline glued on satisfies `rlike` and would not satisfy `_is_instant`. See
# `opl.bronze.snapshot.ref_date_from_instant`, which asserts both.
INSTANT_PATTERN = f"^{MONTH_PATTERN}{_INSTANT_TAIL_PATTERN}$"


def _is_instant(value: str) -> bool:
    """Is `value` the fixed-width UTC instant above?

    THE SHAPE, NOT THE CALENDAR, and the difference is worth stating rather than
    leaving to be discovered: `2026-02-31T00:00:00.000000Z` passes here. Refusing it
    would need a date parse, and this predicate runs on a job PARAMETER before Spark
    exists -- its job is to catch the operator who passed `2026-08` or an offset-bearing
    rendering, not to re-implement a calendar. Nothing downstream depends on the day
    being real: the value is compared against the column bronze already holds, so an
    impossible day selects no rows, which `observation._window` then refuses by name.

    The width is checked FIRST and separately, because it is the qualifier that carries
    the sort property and a caller reading this function should see it stated rather
    than have to infer it from a regex."""
    if len(value) != INSTANT_WIDTH:
        return False
    return is_month(value[:7]) and _INSTANT_TAIL.match(value[7:]) is not None


INSTANT_SNAPSHOT = SnapshotAxis(
    name="snapshot instant",
    column="_snapshot_at",
    shape=f"YYYY-MM-DDTHH:MM:SS.uuuuuuZ ({INSTANT_WIDTH} characters, UTC)",
    accepts=_is_instant,
)
