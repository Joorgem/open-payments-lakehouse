"""THE LEDGER OVER AN AXIS FINER THAN A MONTH -- the claim F-DB T7 makes, closed here
rather than in the phase that will depend on it.

WHAT THIS FILE IS FOR, STATED AGAINST THE TASK THAT WROTE IT. Task 2's acceptance is
that the OLD answer did not move: the CNPJ grains are byte-identical and
`test_observation.py` returns the same five states over the two real RFB months. That
is the important half and it is asserted where it already was. It is also not
sufficient, because a generalisation that changes nothing and enables nothing is
indistinguishable from churn -- so this file asks the other question, on a synthetic
table, and it is the only place the new behaviour is exercised at all.

THE DEFECT, REPRODUCED BEFORE IT IS FIXED. `test_two_snapshots_in_one_calendar_month_
collapse_on_the_monthly_axis` runs the ledger over the SAME rows on the OLD axis and
asserts the wrong answer: a merchant present in the first observation and gone from the
second reads `observed`, because both observations carry `2026-08` and the fold below
`observation_ledger`'s `groupBy` cannot tell them apart. No error, no NULL, no red test
anywhere -- which is why this had to be found by reading `observation.py:124` rather
than by running anything. It is asserted as a CONTROL rather than described in prose:
without it, the test beneath it proves that the instant axis works and not that
anything was ever broken.

THE FIXTURE IS A MERCHANT REGISTRY BECAUSE THAT IS WHAT F-DB LANDS, and it is
deliberately NOT the real thing: no contract, no registry entry, no DQ rules, no
`bronze_merchant`. None of those exist yet and this file must not pre-empt the modelling
Task 4 and Task 5 own. What it needs from a source is exactly two properties -- a
business key, and two observations inside one calendar month -- so it declares three
columns and stops.

TEMP VIEWS AND NOT DELTA TABLES, so this file starts a Spark session and writes
nothing. `observation_ledger` reads through `spark.read.table`, which resolves a
temporary view, and nothing here loads a vault table; `tests/vault/test_loading.py`
uses the same shape for the same reason. What it costs its `run_suite.sh` chunk is one
Spark module setup and no Delta write."""
from __future__ import annotations

from uuid import uuid4

import pytest

from opl.bronze.snapshot_axis import INSTANT_SNAPSHOT, MONTHLY_SNAPSHOT
from opl.vault.observation import (
    STATE_COLUMN,
    ObservationGrain,
    ObservationState,
    observation_ledger,
)

OBSERVED = ObservationState.OBSERVED.value
SIBLINGS = ObservationState.OBSERVED_WITH_REJECTED_SIBLINGS.value
REJECTED = ObservationState.REJECTED_BY_OUR_GATE.value
BEFORE = ObservationState.ABSENT_BEFORE_FIRST_OBSERVATION.value
AFTER = ObservationState.ABSENT_AFTER_OBSERVATION.value

# TWO OBSERVATIONS, ONE CALENDAR MONTH, AND THAT IS THE WHOLE POINT OF THE PAIR. Both
# carry `_snapshot_month = '2026-08'`, which is what the month parameter of a job run on
# either day would stamp -- so the two values below are the ONLY thing that can separate
# them. Rendered exactly as T4's pinned `to_char` produces them, trailing fractional
# zeros included.
S1 = "2026-08-15T09:00:00.000000Z"
S2 = "2026-08-16T09:00:00.000000Z"
MONTH = "2026-08"

_SCHEMA = "merchant_id string, _snapshot_at string, _snapshot_month string"
_QUARANTINE_SCHEMA = _SCHEMA + ", _dq_reject_reason string"

# One merchant per state the ledger can report, named for what it does rather than for
# the state, so a test that asserted the wrong state would not read as self-consistent.
STAYS = "m-stays"
LEAVES = "m-leaves"  # in S1, hard-DELETEd from the database before S2
ARRIVES = "m-arrives"  # INSERTed between the two, so absent from S1 and not departed
SIBLING = "m-sibling"  # in S2's bronze AND S2's quarantine: two rows, one passed
REJECTED_ONE = "m-rejected"  # in S1's bronze, in S2's QUARANTINE only -- our gate, not a delete


def _rows(pairs: list[tuple[str, str]], *, reason: str | None = None) -> list[tuple]:
    """(merchant, snapshot instant) pairs, each stamped with the ONE month both fall in."""
    if reason is None:
        return [(merchant, at, MONTH) for merchant, at in pairs]
    return [(merchant, at, MONTH, reason) for merchant, at in pairs]


@pytest.fixture(scope="module")
def merchant_tables(spark):
    """A two-observation merchant registry and its quarantine, as temp views."""
    suffix = uuid4().hex[:8]
    bronze, quarantine = f"axis_bronze_{suffix}", f"axis_quarantine_{suffix}"
    spark.createDataFrame(
        _rows([
            (STAYS, S1), (STAYS, S2),
            (LEAVES, S1),
            (ARRIVES, S2),
            (SIBLING, S1), (SIBLING, S2),
            (REJECTED_ONE, S1),
        ]),
        _SCHEMA,
    ).createOrReplaceTempView(bronze)
    spark.createDataFrame(
        _rows([(SIBLING, S2), (REJECTED_ONE, S2)], reason="probe"),
        _QUARANTINE_SCHEMA,
    ).createOrReplaceTempView(quarantine)
    return bronze, quarantine


def _grain(tables, axis) -> ObservationGrain:
    bronze, quarantine = tables
    return ObservationGrain(
        name="hub_merchant", bronze_table=bronze, quarantine_table=quarantine,
        key_columns=("merchant_id",), snapshot_axis=axis,
    )


def _states(spark, grain) -> dict[tuple[str, str], str]:
    return {
        (row["merchant_id"], row[grain.snapshot_column]): row[STATE_COLUMN]
        for row in observation_ledger(spark, grain).collect()
    }


def test_two_snapshots_in_one_calendar_month_collapse_on_the_monthly_axis(
    spark, merchant_tables
):
    """THE DEFECT, ASSERTED. Read on `_snapshot_month`, the two observations are one:
    every key gets ONE row, and `m-leaves` -- present in the first and hard-deleted
    before the second -- reads `observed`.

    `absent_after_observation` is the only state the vault's end-dating path acts on
    (`effectivity._departures` filters on `CLOSING_STATE` and nothing else), so on this
    axis this source can produce no departure at all, whatever the database did. The
    answer is not an error and not a NULL; it is a confident wrong one."""
    states = _states(spark, _grain(merchant_tables, MONTHLY_SNAPSHOT))

    assert set(states) == {
        (merchant, MONTH) for merchant in (STAYS, LEAVES, ARRIVES, SIBLING, REJECTED_ONE)
    }
    assert states[(LEAVES, MONTH)] == OBSERVED
    assert AFTER not in states.values()


def test_the_same_rows_on_the_instant_axis_report_all_five_states(spark, merchant_tables):
    """THE SAME DERIVATION, THE SAME FIVE STATES, THE SAME BRANCH ORDER -- over the
    column the source declares instead of the one the module used to hardcode.

    `m-leaves` is now `absent_after_observation` at the second observation, which is the
    state F-DB's headline needs a producer for. Every other row is asserted too, because
    a test that checked only the departure could not distinguish "the axis works" from
    "the second observation lost every key that was not re-observed"."""
    grain = _grain(merchant_tables, INSTANT_SNAPSHOT)

    assert _states(spark, grain) == {
        (STAYS, S1): OBSERVED,
        (STAYS, S2): OBSERVED,
        # THE ONE THIS PHASE EXISTS FOR: seen, then gone, inside one calendar month.
        (LEAVES, S1): OBSERVED,
        (LEAVES, S2): AFTER,
        # INSERTed between the two. Absent from S1 and NOT a departure -- the split that
        # 444,520 establishments forced, holding at an axis 44,000 times finer.
        (ARRIVES, S1): BEFORE,
        (ARRIVES, S2): OBSERVED,
        (SIBLING, S1): OBSERVED,
        (SIBLING, S2): SIBLINGS,
        # OUR gate removed it, so the vault must never read this as a departure.
        (REJECTED_ONE, S1): OBSERVED,
        (REJECTED_ONE, S2): REJECTED,
    }


def test_a_window_on_the_instant_axis_is_validated_against_that_axis(spark, merchant_tables):
    """The window's SHAPE rule travels with the column, which is the half a column name
    alone would have left broken -- `months=['2026-08']` against an instant axis is a
    value the old validator would have accepted and that selects no row of this table.

    And an instant the tables do not carry is refused rather than answered, which is
    `_window`'s largest-blast-radius guard reaching the new axis unchanged: admitted, it
    would give every key an absence row for an observation that never happened."""
    grain = _grain(merchant_tables, INSTANT_SNAPSHOT)

    with pytest.raises(ValueError, match="2026-08"):
        observation_ledger(spark, grain, months=[MONTH]).collect()

    with pytest.raises(ValueError, match="2026-08-17T09:00:00.000000Z"):
        observation_ledger(spark, grain, months=[S1, "2026-08-17T09:00:00.000000Z"]).collect()

    narrowed = observation_ledger(spark, grain, months=[S1]).collect()
    assert {row[grain.snapshot_column] for row in narrowed} == {S1}
