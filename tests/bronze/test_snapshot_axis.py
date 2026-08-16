"""The snapshot axis as a DECLARATION: what a value of one looks like, that its
values sort chronologically as strings, and that every table registered today still
declares the monthly one.

WHY THE SORT PROPERTY IS TESTED AND NOT JUST DOCUMENTED. Two places consume an axis's
values with a string comparison standing in for a chronological one --
`observation._state_column`'s `month < first_observed_month`, which decides whether an
absence is a departure or a key that had not appeared yet, and
`loading.earliest_record_source`'s `min`, which decides which `record_source` a hub row
keeps forever. Neither can detect an axis that sorts differently; both would produce a
full table of plausible wrong answers. So the property belongs to the axis, and this is
where it is held.

THE REGISTRY ASSERTION IS THE OTHER HALF OF F-DB TASK 2's ACCEPTANCE. The axis became a
field on `BronzeTable` for a source that did not exist yet, and the thing that had to stay
true is that the SIX tables the lakehouse had actually loaded read exactly the column they
read before. Asserting the DEFAULT is not the same claim: a default is what an omitted
declaration means, and this asserts that every one of those entries really did omit it.

TASK 4 REGISTERED THE SEVENTH, so the pair of assertions below is now a PARTITION of the
registry rather than one statement about all of it: six names on the monthly axis, one on
the instant axis. Both halves are set equalities, so neither an eighth table nor a second
instant source can arrive without a deliberate edit here."""
from __future__ import annotations

import pytest

from opl.bronze.registry import REGISTRY
from opl.bronze.snapshot_axis import (
    INSTANT_SNAPSHOT,
    INSTANT_WIDTH,
    MONTHLY_SNAPSHOT,
    SnapshotAxis,
)
from opl.config import is_month


def test_every_registered_table_is_still_on_the_monthly_axis():
    """The whole of Task 2's behavioural claim, in one assertion.

    Every source this lakehouse had ingested when the axis was generalised is a monthly
    snapshot, and the generalisation must not have moved one of them: the field was added
    with a default so that no registry entry had to change, and this is what says none did.

    THE SEVENTH TABLE ARRIVED, AND THIS TEST WENT RED EXACTLY AS ITS OWN DOCSTRING SAID IT
    WOULD -- "a seventh table that declares a finer axis will turn this red, which is the
    right moment for a human to read the diff". The diff was read: F-DB Task 4 registers
    `merchant`, a Postgres snapshot observed TWICE inside one calendar month, and plan T7
    is why it cannot stay on the default. So the claim is narrowed to what it was always
    ABOUT -- the six tables that were on the monthly axis before the field existed -- and
    that set is named EXPLICITLY rather than computed as "the registry minus the ones that
    disagree", which would be a test that can never fail again.

    Adding an eighth table turns this red too, and that is the point: an axis is a
    modelling decision per source, and the only wrong way to arrive at one is by paste."""
    on_the_monthly_axis = {
        name for name, spec in REGISTRY.items() if spec.snapshot_axis == MONTHLY_SNAPSHOT
    }

    assert on_the_monthly_axis == {
        "lookup",
        "estabelecimentos",
        "empresas",
        "socios",
        "payments",
        "ptax",
    }
    assert MONTHLY_SNAPSHOT.column == "_snapshot_month"


def test_the_only_table_off_the_monthly_axis_is_the_postgres_snapshot():
    """The complement, stated as a set rather than as "at least one".

    `INSTANT_SNAPSHOT` had NO production reference at all when Task 2 landed it -- its own
    unexercised-ledger entry says so. This is the line that retires that entry, and it is a
    set equality so that a second source arriving on the instant axis has to be a
    deliberate edit here rather than something this assertion absorbs."""
    off_axis = {
        name: spec.snapshot_axis
        for name, spec in REGISTRY.items()
        if spec.snapshot_axis != MONTHLY_SNAPSHOT
    }

    assert off_axis == {"merchant": INSTANT_SNAPSHOT}
    assert INSTANT_SNAPSHOT.column == "_snapshot_at"


def test_the_monthly_axis_asks_the_one_spelling_of_the_month_rule():
    """Identity, not equivalence. A second regex here that happened to agree with
    `opl.config.is_month` today is exactly the shape that left `2026-13` refused at two
    of four entry points -- the defect `opl.vault.months` cites as its own reason to
    exist. The axis must ASK, so `is` rather than a sample of values."""
    assert MONTHLY_SNAPSHOT.accepts is is_month


@pytest.mark.parametrize(
    "value",
    [
        "2026-08-15T17:23:01.123456Z",
        "2026-01-01T00:00:00.000000Z",
        "2026-12-31T23:59:59.999999Z",
    ],
)
def test_the_instant_axis_accepts_what_postgres_renders(value):
    """`to_char(... AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`, which is the
    rendering F-DB's extraction is pinned to. Its width is fixed at 26 digits plus the
    marker, because that is what makes the sort below work."""
    assert len(value) == INSTANT_WIDTH
    assert INSTANT_SNAPSHOT.accepts(value)


@pytest.mark.parametrize(
    ("value", "why"),
    [
        ("2026-08", "a month is not an instant, and this is the value the OLD axis took"),
        ("2026-08-15 17:23:01.123456+00", "`::text`: a space, not `T`; `+00`, not `Z`"),
        ("2026-08-15T17:23:01.1Z", "trailing fractional zeros trimmed -- sorts wrongly"),
        ("2026-08-15T17:23:01.123456+00:00", "an offset rendering, not UTC-normalised"),
        ("2026-13-15T17:23:01.123456Z", "month 13, which the leading seven refuse"),
        ("2026-08-32T17:23:01.123456Z", "day 32"),
        ("2026-08-15T24:00:00.000000Z", "hour 24"),
        ("2026-08-15T17:23:01.123456Zx", "correct prefix, trailing rubbish"),
    ],
)
def test_the_instant_axis_refuses_a_rendering_that_is_not_the_pinned_one(value, why):
    """Each of these is a rendering something on the way here really produces, and
    every one of them would be accepted by a validator that read "ISO-8601". The axis
    pins ONE spelling because two spellings of an instant do not sort together."""
    assert not INSTANT_SNAPSHOT.accepts(value), why


def test_the_instant_axis_admits_an_impossible_DAY_and_says_so():
    """SCOPE, PINNED RATHER THAN DISCOVERED. `_is_instant` checks the SHAPE and not the
    calendar, so 31 February passes. That is stated in its docstring and asserted here
    so nobody later reads the predicate as a date parse and builds on it: the value is
    compared against a column bronze already holds, so an impossible day selects no
    rows, and `observation._window` refuses a window value no row carries by name."""
    assert INSTANT_SNAPSHOT.accepts("2026-02-31T00:00:00.000000Z")


@pytest.mark.parametrize("axis", [MONTHLY_SNAPSHOT, INSTANT_SNAPSHOT])
def test_sorting_an_axis_lexicographically_sorts_it_chronologically(axis):
    """THE PROPERTY TWO CONSUMERS DEPEND ON AND NEITHER CAN CHECK. Written as one
    parametrised test over every declared axis rather than two hand-written ones, so an
    axis added later is asked the question rather than exempted from it."""
    chronological = {
        MONTHLY_SNAPSHOT: ["2025-12", "2026-01", "2026-02", "2026-09", "2026-10"],
        INSTANT_SNAPSHOT: [
            "2025-12-31T23:59:59.999999Z",
            "2026-01-01T00:00:00.000000Z",
            "2026-01-01T00:00:00.000001Z",
            "2026-01-01T00:00:00.090000Z",
            "2026-01-01T00:00:00.100000Z",
            "2026-01-01T09:00:00.000000Z",
            "2026-01-01T10:00:00.000000Z",
        ],
    }[axis]

    assert all(axis.accepts(value) for value in chronological)
    assert sorted(chronological) == chronological


def test_an_axis_is_frozen_and_compared_by_value():
    """`BronzeTable` and `ObservationGrain` both hold one, and the grain's is CARRIED
    from the table's. Compared by value, "the grain reads the source's axis" is a fact a
    test can assert; compared by identity it would be an accident of module import
    order. Frozen for the reason every spec in this repository is: config is data."""
    same = SnapshotAxis(
        name=MONTHLY_SNAPSHOT.name,
        column=MONTHLY_SNAPSHOT.column,
        shape=MONTHLY_SNAPSHOT.shape,
        accepts=MONTHLY_SNAPSHOT.accepts,
    )

    assert same == MONTHLY_SNAPSHOT
    assert INSTANT_SNAPSHOT != MONTHLY_SNAPSHOT
    with pytest.raises(AttributeError):
        MONTHLY_SNAPSHOT.column = "_snapshot_at"
