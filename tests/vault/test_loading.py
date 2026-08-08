"""The shared loader mechanics: the month window, the row count, and the hash-key
expression's dependence on the spec.

WHY THIS FILE EXISTS AT ALL, since `test_cnpj_vault.py` drives all of it indirectly:
the Task 3 review found `_validated_months` and the missing-`_snapshot_month` refusal
UNREACHED -- `load_hub(months="2026-06")` was not asserted anywhere. Every refusal
here answers the same failure, and it is the one this layer is least able to notice:
a load that writes NOTHING and reports success looks exactly like a load that had
nothing to write.

THE THREE MONTH REFUSALS ARE NOT TESTED TWICE. They live in `opl.vault.months`, shared
with the observation ledger since the review flagged the duplication, and
`tests/vault/test_observation.py` already exercises them through the ledger. What is
asserted here is that the LOADER path reaches them -- which is a different claim, and
was the false one."""
from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4

import pytest

from opl.vault.hashing import hash_key
from opl.vault.hubs import load_hub
from opl.vault.loading import (
    changed_rows,
    hash_key_expression,
    read_snapshot_window,
    rows_in,
)
from opl.vault.months import validated_months
from opl.vault.registry import BusinessKeyColumn, Hub

JUN, JUL = "2026-06", "2026-07"
_SCHEMA = "cnpj_basico string, _snapshot_month string, _snapshot_ref_date date"


@pytest.fixture(scope="module")
def snapshots(spark):
    """Two months of one column, as a temp view. No Delta write: nothing here reads
    enough rows for a managed table's reusable plan to pay for its ~22s of setup, which
    is the trade `tests/vault/test_observation.py::tables` measured in the other
    direction for a fixture that IS read heavily."""
    name = f"loading_{uuid4().hex[:8]}"
    spark.createDataFrame(
        [("10000001", JUN, date(2026, 6, 13)), ("10000002", JUL, date(2026, 7, 11))],
        _SCHEMA,
    ).createOrReplaceTempView(name)
    return name


def test_a_named_month_narrows_the_read_and_none_reads_everything(spark, snapshots):
    assert read_snapshot_window(spark, snapshots, [JUN]).count() == 1
    assert read_snapshot_window(spark, snapshots, [JUN, JUL]).count() == 2
    assert read_snapshot_window(spark, snapshots, None).count() == 2


def test_a_bare_string_month_is_refused(spark, snapshots):
    """`months="2026-06"` iterates to seven one-character strings, none of them a
    month, so the filter matches nothing and the load writes nothing -- successfully.
    No type checker catches it: `str` satisfies `Sequence[str]` structurally."""
    with pytest.raises(TypeError, match="bare str"):
        read_snapshot_window(spark, snapshots, "2026-06")


def test_an_empty_month_list_is_refused(spark, snapshots):
    with pytest.raises(ValueError, match="at least one month"):
        read_snapshot_window(spark, snapshots, [])


def test_a_value_that_is_not_a_month_is_refused(spark, snapshots):
    """Delegated to `opl.config.is_month`, the one spelling of that rule in this
    repository: `2026-13` has the shape and names no month."""
    with pytest.raises(ValueError, match="2026-13"):
        read_snapshot_window(spark, snapshots, [JUN, "2026-13"])


def test_a_source_with_no_snapshot_month_column_is_refused_when_a_window_is_named(
    spark
):
    """A table without `_snapshot_month` cannot be month-scoped, and the failure would
    otherwise be a Spark `AnalysisException` naming a column rather than saying that
    this source is not a monthly snapshot."""
    name = f"unmonthly_{uuid4().hex[:8]}"
    spark.createDataFrame([("10000001",)], "cnpj_basico string").createOrReplaceTempView(name)

    with pytest.raises(ValueError, match="_snapshot_month"):
        read_snapshot_window(spark, name, [JUN])


def test_a_source_with_no_snapshot_month_column_is_read_whole_when_no_window_is_named(
    spark
):
    """The refusal above is scoped to a NAMED window, which is what makes it a
    statement about the argument rather than about the table. `months=None` asks for
    everything and needs no month column to deliver it."""
    name = f"unmonthly_{uuid4().hex[:8]}"
    spark.createDataFrame([("10000001",)], "cnpj_basico string").createOrReplaceTempView(name)

    assert read_snapshot_window(spark, name, None).count() == 1


def test_the_month_consequence_reaches_the_message():
    """The shared validator takes its consequence from the caller so a loader's empty
    window does not tell an operator about an empty LEDGER. Asserted directly, because
    both callers pass a literal and nothing else would notice a swap."""
    with pytest.raises(ValueError, match="probe consequence"):
        validated_months([], column="_snapshot_month", consequence="probe consequence")


def test_a_count_of_a_table_that_does_not_exist_is_zero(spark):
    """`rows_in` is how both loaders learn what a target already held, and the first
    load of any vault table asks it about a table that is not there yet. Raising would
    make every table's first load fail."""
    assert rows_in(spark, f"nonexistent_{uuid4().hex[:8]}") == 0


def test_the_hash_key_expression_follows_the_specs_column_order(spark, snapshots):
    """Two hubs over the same columns in opposite order must produce DIFFERENT digests,
    because the standard length-prefixes each component and joins on `||`.

    The order is the spec's declaration order and that is not incidental -- it is
    written down once, in the domain module, and a hub that reordered its
    `business_keys` would re-key itself. Compared against `hash_key` on both sides, so
    this pins which order goes with which digest rather than only that they differ."""
    frame = spark.createDataFrame([("AB", "CD")], "left string, right string")
    forward = Hub(
        name="h", hash_key="h_hk",
        business_keys=(BusinessKeyColumn(name="left"), BusinessKeyColumn(name="right")),
    )
    backward = Hub(
        name="h", hash_key="h_hk",
        business_keys=(BusinessKeyColumn(name="right"), BusinessKeyColumn(name="left")),
    )

    row = frame.select(
        hash_key_expression(forward).alias("forward"),
        hash_key_expression(backward).alias("backward"),
    ).first()

    assert row["forward"] == hash_key(["AB", "CD"])
    assert row["backward"] == hash_key(["CD", "AB"])
    assert row["forward"] != row["backward"]


def test_a_business_key_with_no_width_is_not_padded(spark):
    """`width=None` means "take the value as it is", not "width unknown": padding a
    name or a free-text identifier would invent characters. Only a caller who knows the
    canonical width may assert one."""
    frame = spark.createDataFrame([("7",)], "k string")
    hub = Hub(name="h", hash_key="h_hk", business_keys=(BusinessKeyColumn(name="k"),))

    assert frame.select(hash_key_expression(hub).alias("d")).first()["d"] == hash_key(["7"])


# --------------------------------------------------------------------------- #
# `changed_rows`' own precondition, asserted on `changed_rows`.
# --------------------------------------------------------------------------- #

_CHANGED_SCHEMA = "k string, applied_date date, hash_diff string"
_JUN_REF, _JUL_REF = date(2026, 6, 13), date(2026, 7, 11)


def test_changed_rows_drops_a_candidate_its_target_already_holds(spark):
    """THE PRECONDITION THE DOCSTRING CALLED LOAD-BEARING, ASSERTED ON THE FUNCTION THAT
    OWNS IT rather than on the two loaders that used to satisfy it by hand.

    Both callers ran this anti-join themselves before calling, so the one step of
    `changed_rows`' contract that was NOT shared was the step its own docstring named as
    the reason for sharing. A third caller omitting it does not fail: two rows land at
    one position in the window, `lag` marks whichever it orders second as unchanged, and
    when the non-persisted one lands first it survives the filter and is appended again
    -- a duplicate on roughly half of re-runs, non-deterministically.

    `('A', JUN)` below is offered as a candidate AND is already persisted, so it must
    not come back. `('A', JUL)` must, because its digest differs from the persisted
    June row's -- which is also the assertion that `existing` still SEEDS the window
    rather than merely filtering it."""
    candidates = spark.createDataFrame(
        [("A", _JUN_REF, "d1"), ("A", _JUL_REF, "d2"), ("B", _JUN_REF, "d3")],
        _CHANGED_SCHEMA,
    )
    existing = spark.createDataFrame([("A", _JUN_REF, "d1")], _CHANGED_SCHEMA)

    rows = changed_rows(candidates, existing, "k").collect()

    assert sorted((row["k"], row["applied_date"]) for row in rows) == [
        ("A", _JUL_REF), ("B", _JUN_REF)
    ]


def test_changed_rows_appends_nothing_when_every_candidate_is_already_persisted(spark):
    """Idempotence at the shared level: a re-run offers exactly what is on disk and
    gets back nothing. Asserted here as well as through the loaders because this is the
    property the anti-join exists for, and it now lives in one place."""
    rows = spark.createDataFrame(
        [("A", _JUN_REF, "d1"), ("A", _JUL_REF, "d2")], _CHANGED_SCHEMA
    )

    assert changed_rows(rows, rows, "k").count() == 0


def test_changed_rows_with_no_target_treats_every_first_row_as_changed(spark):
    """`existing=None` is a first load, where there is nothing to drop and nothing to
    seed the window with. The contrast case, so the two assertions above read as the
    anti-join firing rather than as a constant."""
    candidates = spark.createDataFrame(
        [("A", _JUN_REF, "d1"), ("A", _JUL_REF, "d1"), ("B", _JUN_REF, "d3")],
        _CHANGED_SCHEMA,
    )

    rows = changed_rows(candidates, None, "k").collect()

    # ('A', JUL) repeats ('A', JUN)'s digest, so it is unchanged and dropped -- by the
    # `lag`, which is the OTHER half of this function and must still work untouched.
    assert sorted((row["k"], row["applied_date"]) for row in rows) == [
        ("A", _JUN_REF), ("B", _JUN_REF)
    ]


def test_the_loader_path_refuses_a_bare_string_month(spark, snapshots):
    """The refusals above are on `read_snapshot_window`; this is the one that says
    `load_hub` actually routes through it. The review's finding was precisely that
    `load_hub(months="2026-06")` was unasserted."""
    hub = Hub(
        name="h", hash_key="h_hk",
        business_keys=(BusinessKeyColumn(name="cnpj_basico", width=8),),
    )

    with pytest.raises(TypeError, match="bare str"):
        load_hub(
            spark, hub, source_table=snapshots, target_table="unused",
            load_date=datetime(2027, 1, 1), months="2026-06",
        )
