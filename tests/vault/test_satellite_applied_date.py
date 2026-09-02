# tests/vault/test_satellite_applied_date.py
"""`load_satellite`'s refusal of a candidate whose `applied_date` came out NULL, and the
silent drop it exists to prevent.

WHY THIS IS ITS OWN MODULE. The two sibling refusals -- a source missing the declared
column, and a reader the column's representation cannot support -- live in
`tests/vault/test_payments_satellite.py`, which is the natural home and is at 798 of this
project's strictly-under-800 lines. Master protocol section 4.12 says whoever touches a
file at the cap splits it first; splitting an 800-line module in a correction round is a
larger change than the one being made, so the third refusal starts a focused module
instead. It shares that file's fixture rather than building a second one, so the rows the
refusal is driven over are the rows the link and the satellite are asserted against.

WHAT THE DEFECT WAS. F2 wave 2 replaced an unconditional `_snapshot_ref_date` projection
with a per-satellite `AppliedDateSource` and did not carry across the control that made
the old projection safe. `opl.bronze.snapshot.ref_date_column` leaves an underivable
reference date NULL *because* `rules._unprovable_ref_date` rejects those rows at the gate;
the payments rule set has no shape rule on `event_time` at all, so a DQ-passing but
unparseable value reached the satellite with `applied_date = NULL` and
`loading.changed_rows`' closing `left_semi` on (hash key, `applied_date`) discarded it --
`NULL = NULL` is not true. Measured on this fixture with one impossible day: 4 link rows,
3 satellite rows, and a re-run appending 0, so idempotence hid it.

WHY THE THIRD TEST IS NOT REDUNDANT WITH THE FIRST. The refusal is only worth its eager
pass if the thing it refuses would really have been lost, and a refusal that fires is
equally consistent with a loader that would have raised anyway. So the drop is driven
directly against `changed_rows`, which is where it happens, and asserted as a row count.
Without it the first test would be pinning a message rather than a consequence.
"""
from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F

from opl.bronze.snapshot_axis import MONTHLY_SNAPSHOT
from opl.contracts import payments as payments_contract
from opl.vault import domains
from opl.vault.columns import APPLIED_DATE, HASH_DIFF
from opl.vault.loading import changed_rows
from opl.vault.satellites import load_satellite

from .conftest import LOADED_AT, derived_table

SAT = domains.table_spec("sat_link_payment")
LINK = domains.table_spec("link_payment")
LINK_HUBS = domains.linked_hubs(LINK)

# A value the payments DQ gate PASSES and no calendar holds: non-empty, so
# `null_or_empty_event_time` is satisfied, and an impossible day, so `to_date` answers
# NULL under both `spark.sql.legacy.timeParserPolicy` settings (`opl.bronze.snapshot`
# measures that on pyspark 3.5.9). The gate has no shape rule on this column, which is
# the whole reason the vault has to hold this line itself.
UNPARSEABLE_EVENT_TIME = "2026-06-31T12:00:00.000Z"


def _one_bad_event_time(spark, payments_source):
    """The shared fixture's five rows, with `t-0002`'s `event_time` made underivable."""
    identity = payments_contract.IDENTITY_COLUMN
    event_time = payments_contract.EVENT_TIME_COLUMN
    return derived_table(
        spark, payments_source.db, "bad_event_time",
        spark.read.table(payments_source.bronze).withColumn(
            event_time,
            F.when(F.col(identity) == F.lit("t-0002"), F.lit(UNPARSEABLE_EVENT_TIME))
            .otherwise(F.col(event_time)),
        ),
    )


def test_a_candidate_with_no_applied_date_is_refused_before_anything_is_written(
    spark, payments_source
):
    """The refusal names the satellite, the column it reads and the table it read it
    from, because those three are what an operator has to look at, and it fires before
    the append so there is no half-written table to clean up."""
    source = _one_bad_event_time(spark, payments_source)
    target = f"{payments_source.db}.sat_undated"

    with pytest.raises(ValueError, match="is NULL, read from"):
        load_satellite(
            spark, SAT, link=LINK, hubs=LINK_HUBS,
            source_table=source, target_table=target,
            load_date=LOADED_AT, axis=MONTHLY_SNAPSHOT,
        )

    assert not spark.catalog.tableExists(target)


def test_the_same_source_without_the_bad_row_still_loads_every_payment(
    spark, payments_source
):
    """THE OTHER DIRECTION, AND IT IS WHAT KEEPS THE REFUSAL FROM BEING A BLANKET ONE. A
    predicate inverted, or one that answered true on any input, would redden here: the
    unedited fixture carries four distinct (link hash key, event day) pairs and all four
    are written."""
    target = f"{payments_source.db}.sat_dated"
    result = load_satellite(
        spark, SAT, link=LINK, hubs=LINK_HUBS,
        source_table=payments_source.bronze, target_table=target,
        load_date=LOADED_AT, axis=MONTHLY_SNAPSHOT,
    )

    assert result.appended == 4
    assert spark.read.table(target).filter(F.col(APPLIED_DATE).isNull()).count() == 0


def test_changed_rows_drops_an_undated_candidate_which_is_what_the_refusal_prevents(
    spark
):
    """THE CONSEQUENCE, DRIVEN WHERE IT HAPPENS. `changed_rows` closes on a `left_semi`
    over (key, `applied_date`), and an equi-join never matches a NULL -- so the row goes
    missing with nothing raised and nothing counted. Driven with `existing=None`, which
    is a FIRST load: the drop does not need a populated target, which is why the re-run
    that appends 0 looks exactly like idempotence."""
    key = LINK.hash_key
    candidates = spark.createDataFrame(
        [("k-dated", date(2026, 6, 1), "d1"), ("k-undated", None, "d2")],
        f"{key} string, {APPLIED_DATE} date, {HASH_DIFF} string",
    )

    survived = {row[key] for row in changed_rows(candidates, None, key).collect()}

    assert survived == {"k-dated"}
