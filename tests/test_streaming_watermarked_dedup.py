# tests/test_streaming_watermarked_dedup.py
"""The half of T4+T5 that RUNS things, minus the broker: the chain's own refusals, the
comparison of two arms against the prediction, and the three counts the dedup is read off.

WHAT IS NEXT DOOR. `tests/test_streaming_lateness.py` covers the arithmetic that places the
two watermark delays -- corpus in, delays out, no session anywhere. This file is about what
`opl.streaming.watermarked_dedup` does with them: the chain's own arguments, and the four
things `prove_boundary` refuses about a pair of arms once they have run.

THOSE FOUR, IN THE ORDER `prove_boundary` ASKS THEM. Two arms that read different records
are not one experiment; an arm that split into other than the batches the boundary's rate
limit predicts was measured against margins derived for a read that did not happen; two arms
that landed the SAME count found no boundary at all; and a pair whose difference is not the
number the boundary PREDICTED is a partial drop wearing a measurement's clothes.

THE SECOND AND THE FOURTH ARE THE NEW ONES, AND THE FOURTH IS THE ONE THE PHASE FELL INTO.
The third -- the zero-drop refusal -- shipped before this pass and fires on zero and on
nothing else, so a difference of 97 against a declared 100, the number this phase's
published prediction actually met, cleared every check there was and returned an object that
reads as a clean measurement. The second is the same seam from the other side: the rate
limit is passed once to `boundary_for` and once to `payment_stream`, and nothing compared
the two.

THE CORPUS IS BUILT ONCE AT MODULE LEVEL and costs about a second and a half. It is here
because a `BoundaryEvidence` needs a real `LatenessBoundary` to carry, and a hand-built one
would let the prediction under test be typed in rather than derived.
"""
from __future__ import annotations

import pytest

from opl.contracts.payments import (
    BUSINESS_ATTRIBUTE_COLUMNS,
    COLUMNS,
    IDENTITY_COLUMN,
)
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import delivered_records
from opl.generator.profiles import POOL_SIZE, PROFILES
from opl.streaming import watermarked_dedup as wd
from opl.streaming.lateness import DROPPING, KEEPING, boundary_for
from opl.streaming.watermarked_dedup import (
    DEDUP_KEY,
    BoundaryEvidence,
    LandedArm,
    dedup_shape,
)

_PROFILE = PROFILES["promotable"]
_DEFECTS = _PROFILE.defects
_POOL = validated_pool([f"{n:08d}" for n in range(1, POOL_SIZE + 1)])
_SPEC = _PROFILE.stream_spec(_POOL)
_RECORDS = delivered_records(_SPEC, _DEFECTS)

# The rate limit the integration run reads at. Copied rather than imported, because
# `tests/` is not a package -- `tests/test_streaming_lateness.py` says what catches the
# copies diverging.
_PER_TRIGGER = 133

# What a run at that limit consumes and how it splits: 10,150 delivered records in 77
# micro-batches. Both are asserted against Spark's own progress in
# `tests/integration/test_late_arrival_boundary.py`; here they are what a plausible
# `LandedArm` has to carry, because `_refuse_a_split_the_boundary_was_not_derived_for`
# reads exactly this pair.
_DELIVERED = _PROFILE.delivered_row_count
_BATCHES = tuple(range(-(-_DELIVERED // _PER_TRIGGER)))


def _boundary():
    return boundary_for(_RECORDS, _DEFECTS, max_offsets_per_trigger=_PER_TRIGGER)


def _arm(name: str, *, landed: int, input_rows: int = _DELIVERED, batches=_BATCHES) -> LandedArm:
    """An arm that a run at `_PER_TRIGGER` could actually have produced.

    THE DEFAULTS ARE THE SHIPPED RUN'S, and that is load-bearing rather than tidy: the
    split refusal compares `len(batch_ids)` against `ceil(input_rows / limit)`, so an arm
    built with two arbitrary batch ids would be refused before any test below reached the
    thing it is about."""
    return LandedArm(
        arm=name,
        delay_ms=1,
        batch_ids=batches,
        input_rows=input_rows,
        landed_rows=landed,
        path=f"/tmp/{name}",
    )


def _evidence(*, dropping: int, keeping: int) -> BoundaryEvidence:
    return BoundaryEvidence(
        boundary=_boundary(),
        dropping=_arm(DROPPING, landed=dropping),
        keeping=_arm(KEEPING, landed=keeping),
    )


def _frame(spark, rows):
    """A stand-in landed table: the identity and the business attributes, which is the
    whole of what `dedup_shape` reads."""
    columns = [IDENTITY_COLUMN, *BUSINESS_ATTRIBUTE_COLUMNS]
    return spark.createDataFrame(list(rows), " string, ".join(columns) + " string")


def _row(identity: str | None, attributes: str):
    """One landed row: `identity` over a business tuple named by `attributes`.

    The attribute VALUES decide a legitimate repeat, so two rows sharing `attributes` and
    differing in `identity` are exactly the shape T5 must not collapse."""
    return (identity, *(f"{attributes}-{column}" for column in BUSINESS_ATTRIBUTE_COLUMNS))


def test_an_empty_dedup_key_is_refused_before_the_frame_is_touched():
    """TRAP 1. `withWatermark` in front of nothing discards no row -- the watermark only
    bites inside a stateful operator -- so this chain would land every record including the
    late ones and report a run in which nothing was late. MEASURED: that run lands all
    10,150, neither dropped nor deduplicated.

    `None` AS THE FRAME IS THE POINT: it says the refusal happens before any DataFrame
    method is reached, which a later refusal would fail with an AttributeError."""
    with pytest.raises(ValueError, match="the dedup key is empty"):
        wd.watermarked_dedup(None, delay_ms=1, keys=())


@pytest.mark.parametrize(
    "keys",
    [
        BUSINESS_ATTRIBUTE_COLUMNS,
        (IDENTITY_COLUMN, BUSINESS_ATTRIBUTE_COLUMNS[0]),
        ("event_time",),
    ],
    ids=["the-business-tuple", "the-identity-plus-one-attribute", "no-identity-at-all"],
)
def test_a_key_that_would_collapse_a_legitimate_repeat_is_refused(keys):
    """T5's TENSION, REFUSED IN SHIPPED CODE RATHER THAN ASSERTED AFTERWARDS.

    A legitimate repeat is a SECOND PAYMENT carrying an earlier one's payer, payee, amount,
    currency and method under its OWN `transaction_id` -- ordinary business, 800 of them in
    `promotable`. A dedup keyed on those attributes takes them along with the redeliveries,
    and a test asserting only "150 fewer rows landed" would not see it. MEASURED at the
    shipped rate limit: that key lands 9,624 rows -- 526 collapsed rather than 150 -- and
    leaves 424 of the 800 repeats. Not a round 9,200, because the WINDOWED operator only
    collapses the repeats whose copies fall inside its state window: the damage is an
    artefact of the batching, which is exactly what makes it hard to notice.

    Three keys, because the refusal has two halves: a key touching the attributes is refused
    however much else it carries, and a key that omits the identity is refused even when it
    touches no attribute at all."""
    with pytest.raises(ValueError, match="is not a dedup key this corpus"):
        wd.watermarked_dedup(None, delay_ms=1, keys=keys)


def test_the_shipped_key_is_the_identity_and_survives_its_own_refusal():
    """The floor under the parametrised refusal above: a guard that refused EVERY key would
    pass all three of those cases and ship a module nothing can call."""
    assert DEDUP_KEY == (IDENTITY_COLUMN,)
    wd._refuse_a_key_that_would_collapse_a_legitimate_repeat(DEDUP_KEY)
    wd._refuse_a_key_that_cannot_make_the_stream_stateful(DEDUP_KEY)


def test_a_watermark_column_that_shadowed_a_contract_column_is_refused(monkeypatch):
    """`withColumn` REPLACES a column of the same name, so a collision raises nowhere: it
    would overwrite a payment field with a timestamp and land the overwrite in the sink.
    Monkeypatched, so the shipped constant stays what it is while the guard is asked the
    question it exists for."""
    monkeypatch.setattr(wd, "EVENT_INSTANT_COLUMN", COLUMNS[0])
    with pytest.raises(AssertionError, match="would silently overwrite"):
        wd._assert_the_instant_column_does_not_shadow_the_contract()


def test_arms_that_did_not_read_the_same_records_are_refused():
    """The two arms differ in ONE THING and it is the delay. A drained checkpoint, a
    republished topic or a different split makes their landed counts differ for a reason
    that is not the watermark -- and no row count can separate the two causes."""
    refuse = wd._refuse_arms_that_did_not_read_the_same_corpus
    refuse(_arm(DROPPING, landed=9_900), _arm(KEEPING, landed=10_000))
    with pytest.raises(RuntimeError, match="one experiment only while they read"):
        refuse(_arm(DROPPING, landed=9_900, input_rows=10_149), _arm(KEEPING, landed=10_000))
    with pytest.raises(RuntimeError, match="one experiment only while they read"):
        refuse(_arm(DROPPING, landed=9_900), _arm(KEEPING, landed=10_000, batches=(0, 1, 2)))


def test_an_arm_that_split_differently_from_the_boundarys_rate_limit_is_refused():
    """THE SECOND MOUTH OF THE PREDICTION SEAM, AND IT IS AN ARGUMENT PASSED TWICE.

    `boundary_for(..., max_offsets_per_trigger=N)` derives the delays and
    `payment_stream(..., max_offsets_per_trigger=N)` splits the read. They are two call
    sites, nothing carried the value from one to the other, and until this refusal existed
    nothing compared the results -- so a run reading at one limit under delays derived for
    another would have been reported as a boundary.

    THE ARM THAT AGREES IS ASSERTED FIRST, because a refusal that fired on everything would
    pass both cases below and stop the shipped run dead. 10,150 records at 133 a trigger is
    77 batches; a read that came back in ONE batch is trap 2 arriving through the door that
    was opened for the rate limit, and a read that came back in 76 consumed the corpus in
    batches larger than the margins were computed for."""
    refuse = wd._refuse_a_split_the_boundary_was_not_derived_for
    boundary = _boundary()
    refuse(_arm(DROPPING, landed=9_900), boundary)

    with pytest.raises(RuntimeError, match="predicts 77"):
        refuse(_arm(DROPPING, landed=9_900, batches=(0,)), boundary)
    with pytest.raises(RuntimeError, match="in 76 batches"):
        refuse(_arm(DROPPING, landed=9_900, batches=_BATCHES[:-1]), boundary)


def test_a_pair_whose_arms_landed_the_same_count_is_refused():
    """TRAP 3. A watermark wide enough to drop nothing passes every assertion downstream of
    it, so "the arms agree" is refused rather than reported as a clean run."""
    refuse = wd._refuse_a_pair_that_dropped_nothing
    refuse(_evidence(dropping=9_900, keeping=10_000))
    with pytest.raises(RuntimeError, match="dropped nothing that the one at"):
        refuse(_evidence(dropping=10_000, keeping=10_000))


def test_a_pair_that_dropped_the_wrong_number_is_refused_and_97_is_that_number():
    """TRAP 4, AND IT IS THE ONE THIS PHASE FELL INTO RATHER THAN AN INVENTED CASE.

    `BoundaryEvidence` has carried the PREDICTION (`boundary.dropped_rows`) beside the
    MEASUREMENT (`keeping - dropping`) since it existed and compared neither. Trap 3's
    refusal fires on ZERO and on nothing else, so every other wrong difference passed --
    including the one this phase produced: a published prediction of 100 that the run met
    with 97.

    THE THREE ASSERTIONS BELOW ARE ONE MUTATION EACH, and the first is what the shipped run
    does. 9,900 against 10,000 is the difference the declaration names; 9,903 is the 97-drop
    that used to pass; 9,800 is an over-drop, which the same comparison has to catch in the
    other direction or it is a floor rather than an equality.

    AND THE PREDICTION IS DERIVED, NOT TYPED. `_evidence` builds its boundary with
    `boundary_for` over the declared corpus, so the 100 on the prediction side comes from
    `promotable`'s `DefectSpec` -- the same place the integration run's does."""
    refuse = wd._refuse_a_drop_the_boundary_did_not_predict
    assert _boundary().dropped_rows == _DEFECTS.late_count == 100

    refuse(_evidence(dropping=9_900, keeping=10_000))

    with pytest.raises(RuntimeError, match=r"differ by 97 rows .* predicted 100"):
        refuse(_evidence(dropping=9_903, keeping=10_000))
    with pytest.raises(RuntimeError, match=r"differ by 200 rows .* predicted 100"):
        refuse(_evidence(dropping=9_800, keeping=10_000))


def test_the_dropped_row_count_is_the_difference_between_the_two_arms():
    assert _evidence(dropping=9_900, keeping=10_000).dropped_rows == 100


def test_the_three_counts_separate_a_redelivery_from_a_legitimate_repeat(spark):
    """THE INSTRUMENT T5 IS READ OFF, over a table whose answer is stated by construction.

    Four identities over three attribute tuples -- so one of them is a legitimate repeat --
    and then one identity landed twice. `collapsed_rows` sees the repeated identity;
    `surviving_repeats` sees the shared attributes; and only having both makes them visible
    as different things. A measure reporting either alone would call this table correct
    under a dedup that collapsed the repeat as well."""
    landed = _frame(
        spark,
        [_row("id-1", "A"), _row("id-2", "A"), _row("id-3", "B"), _row("id-4", "C")],
    )
    shape = dedup_shape(landed)
    assert (shape.row_count, shape.distinct_identities) == (4, 4)
    assert shape.distinct_business_attributes == 3
    assert shape.surviving_repeats == 1
    assert shape.collapsed_rows == 0

    with_a_redelivery = _frame(
        spark,
        [_row("id-1", "A"), _row("id-2", "A"), _row("id-3", "B"), _row("id-1", "A")],
    )
    repeated = dedup_shape(with_a_redelivery)
    assert (repeated.row_count, repeated.distinct_identities) == (4, 3)
    assert repeated.collapsed_rows == 1
    assert repeated.surviving_repeats == 1, (
        "and the legitimate repeat is still visible beside the redelivery, which is the "
        "one thing a count of 'rows removed' cannot say"
    )


def test_an_empty_landed_table_is_refused_rather_than_described(spark):
    """Every count over it is 0, all of them are true, and they are what a run that never
    happened also reports."""
    with pytest.raises(RuntimeError, match="refusing to describe an empty table"):
        dedup_shape(_frame(spark, []))


def test_a_null_identity_is_refused_rather_than_counted(spark):
    """`from_json` returns a struct of NULLs for a value it could not parse, and
    `select(id).distinct().count()` counts NULL as a VALUE -- so a failed parse would read
    as one extra distinct identity rather than as a failed parse."""
    broken = _frame(spark, [_row("id-1", "A"), _row(None, "B"), _row(None, "C")])
    with pytest.raises(RuntimeError, match="landed rows carry a NULL"):
        dedup_shape(broken)
