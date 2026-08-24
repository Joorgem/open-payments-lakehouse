# src/opl/streaming/watermarked_dedup.py
"""T4 + T5 -- THE LATE-ARRIVAL BOUNDARY AND THE DEDUP, WHICH ARE ONE OPERATOR CHAIN.

    withWatermark(event_time, delay) -> dropDuplicatesWithinWatermark(transaction_id)

They are one chain because Structured Streaming makes them one: a watermark only DROPS
inside a stateful operator, and `dropDuplicatesWithinWatermark` is the stateful operator
whose state that watermark expires. Split across two modules they would be two halves
neither of which can be measured on its own.

WHERE THE DELAYS COME FROM IS `opl.streaming.lateness`, AND THAT IS A DIFFERENT SEAM. This
module runs things: it builds the chain, runs two arms through
`opl.streaming.ingest.write_payment_stream`, and compares what they landed. `lateness` is
pure arithmetic over the delivered corpus -- no session, no broker, no clock -- which is
what lets a run publish its prediction before it starts. The chain is not what was split;
one of its arguments is.

--- THE FOUR WAYS THIS EXPERIMENT PASSES WITHOUT MEASURING WHAT IT REPORTS ---------------

Each is refused below, in shipped code, rather than remembered by whoever writes the test.

  1. A WATERMARK ON A STATELESS STREAM DROPS NOTHING. `withWatermark(...)` followed by a
     plain sink discards no row: the watermark is metadata on a column until a stateful
     operator reads it. A run that set one, landed all 10,150 rows and concluded "no late
     data was dropped" measured the absence of a stateful operator.
     MEASURED: that run lands 10,150 -- neither dropped nor deduplicated.
     REFUSED BY `_refuse_a_key_that_cannot_make_the_stream_stateful`, because an empty
     dedup key is the one argument that degrades this chain back to a projection.

  2. A WATERMARK THAT NEVER ADVANCES DROPS NOTHING EITHER. The watermark is computed from
     data ALREADY SEEN, so in a single micro-batch it is still at its floor and no row can
     be behind it. Several batches are required.
     MEASURED: the same corpus at the same delay with no `maxOffsetsPerTrigger` lands
     10,000 -- every redelivery collapsed and every late arrival kept.
     REFUSED BY `lateness._refuse_a_read_that_cannot_advance_a_watermark`.

  3. A WATERMARK WIDE ENOUGH TO DROP NOTHING PASSES EVERY ASSERTION. So the acceptance is
     TWO RUNS OVER ONE CORPUS at two delays, and THE PRODUCT IS THE DIFFERENCE BETWEEN THE
     TWO LANDED COUNTS. One arm alone is a demonstration; the pair is a measurement.
     REFUSED BY `_refuse_a_pair_that_dropped_nothing` and
     `_refuse_arms_that_did_not_read_the_same_corpus`.

  4. AND A DROP OF THE WRONG SIZE READS AS A CLEAN PAIR, WHICH IS THE ONE THIS PHASE FELL
     INTO. `BoundaryEvidence` carries the PREDICTION (`boundary.dropped_rows`, the declared
     late count) beside the MEASUREMENT (`keeping - dropping`) and, until the refusal named
     below existed, compared neither: trap 3's refusal fires on ZERO and on nothing else,
     so a pair that dropped 97 where 100 were declared cleared every shipped check and came
     back as a measurement. 97 IS NOT A HYPOTHETICAL NUMBER -- it is what this phase's
     published prediction actually met, and `lateness`'s first section is the account of it.
     REFUSED BY `_refuse_a_drop_the_boundary_did_not_predict`.

     THE SAME SEAM HAS A SECOND MOUTH AND IT IS THE RATE LIMIT. That argument is passed
     TWICE, at two call sites -- once to `lateness.boundary_for`, which derives the delays
     under it, and once to `ingest.payment_stream`, which splits the read with it -- and
     nothing compared the two. A run whose query split differently from the split the
     margins were computed for derives its delays for a batching that never happened.
     REFUSED BY `_refuse_a_split_the_boundary_was_not_derived_for`, which compares
     `ceil(input_rows / limit)` against the arm's own `batch_ids`.

--- T5's TENSION, AND WHY THE KEY IS THE IDENTITY AND NOTHING ELSE ----------------------

The corpus carries two things that look alike in a row count and are opposites:

    800 LEGITIMATE REPEATS   the same payer/payee/amount/currency/method as an earlier
                             row, carrying THEIR OWN `transaction_id` -- a customer paying
                             one supplier the same amount twice. ORDINARY BUSINESS. It
                             must survive every dedup.
    150 REDELIVERIES         one `transaction_id` delivered twice, byte for byte.

A dedup keyed on the business tuple takes both, and a test that only checked "150 fewer
rows landed" would not see it. MEASURED, over this corpus at the shipped rate limit and the
keeping delay: that key lands 9,624 rows -- 526 collapsed rather than 150, so 376 ordinary
payments destroyed -- and the surviving repeats fall from 800 to 424.

AND 376 RATHER THAN ALL 800 IS ITSELF WORTH THE LINE, because it is what makes the trap
hard to see. An unwindowed `dropDuplicates` on that tuple would collapse all 950 and leave
a suspiciously round 9,200; the WINDOWED operator only collapses the repeats whose copies
fall inside its state window, so the number it destroys is an artefact of the rate limit --
measured at 424 surviving here and at 407 under a different one -- which no declaration
predicts and no round figure flags. So `dedup_shape` reports the surviving repeats as a
number in their own right and `_refuse_a_key_that_would_collapse_a_legitimate_repeat`
refuses the tuple outright.

AND THIS DOES NOT RE-BLUR WHAT T3 SEPARATED. A redelivery is a property of the DATA: the
producer sent one id twice, `opl.generator.defects` injected it on purpose, and F1b already
measures it. Exactly-once is a property of PROCESSING, counted over
`opl.streaming.ingest.PROCESSING_IDENTITY` -- the Kafka coordinate pair -- by
`opl.streaming.exactly_once`, which is blind to `transaction_id` for exactly this reason.
The two modules dedup on two different keys because they answer two different questions,
and neither key can answer the other's.

--- THE PARSED INSTANT IS ADDED AND THEN DROPPED ----------------------------------------

`opl.bronze.schema.struct_for` reads every contract column as a STRING and `withWatermark`
needs a timestamp, so the chain derives `event_instant` from `event_time`, watermarks on
it, dedups, and DROPS IT AGAIN -- leaving exactly the schema `opl.streaming.ingest` lands,
so the two sinks stay comparable. Casting `event_time` in place would instead give this
module's table a column of a type no other table in this phase has.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from opl.contracts.payments import (
    BUSINESS_ATTRIBUTE_COLUMNS,
    COLUMNS,
    EVENT_TIME_COLUMN,
    IDENTITY_COLUMN,
)
from opl.streaming.ingest import write_payment_stream
from opl.streaming.lateness import ARMS, DROPPING, KEEPING, LatenessBoundary

# The dedup key. ONE COLUMN, and it is the record's identity rather than its attributes --
# see the module docstring's T5 section for the 800 rows that difference decides.
DEDUP_KEY = (IDENTITY_COLUMN,)

# The derived timestamp the watermark is declared on, and the pattern that produces it.
# `XXX` rather than a literal `'Z'`: `opl.generator.instants.to_text` always emits `Z`, so
# under this project's UTC session the two spellings agree -- and only the zone-aware one
# still agrees on a session whose `spark.sql.session.timeZone` was left alone.
EVENT_INSTANT_COLUMN = "event_instant"
INSTANT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"


def _assert_the_instant_column_does_not_shadow_the_contract() -> None:
    """Refuse AT IMPORT if the derived instant collides with a contract column.

    `withColumn` REPLACES a column of the same name, so a collision would not raise: it
    would overwrite a payment field with a timestamp, land the overwrite in the sink, and
    leave every count here taken over a column that no longer holds what it is named for.
    `opl.streaming.ingest` runs the same guard over its own three columns."""
    if EVENT_INSTANT_COLUMN not in COLUMNS:
        return
    raise AssertionError(
        f"{EVENT_INSTANT_COLUMN!r} is both this module's derived watermark column and a "
        "payments contract column. `withColumn` would silently overwrite the contract's "
        "value with a timestamp rather than fail."
    )


_assert_the_instant_column_does_not_shadow_the_contract()


def _refuse_a_key_that_cannot_make_the_stream_stateful(keys: tuple[str, ...]) -> None:
    """TRAP 1, IN SHIPPED CODE. An empty key is the one argument that would leave
    `withWatermark` standing in front of nothing, and a watermark in front of nothing
    drops nothing -- see the module docstring's first numbered paragraph."""
    if keys:
        return
    raise ValueError(
        "the dedup key is empty. `withWatermark` alone discards no row -- the watermark "
        "only bites inside a stateful operator -- so this chain would land every record "
        "including the late ones, and report it as a run in which nothing was late."
    )


def _refuse_a_key_that_would_collapse_a_legitimate_repeat(keys: tuple[str, ...]) -> None:
    """T5's tension, refused rather than asserted. A key drawn from the business attributes
    collapses legitimate repeats along with the redeliveries -- measured over `promotable`:
    526 rows rather than 150, and 376 of the 800 repeats gone -- while every "fewer rows
    landed" assertion stays green."""
    collisions = sorted(set(keys) & set(BUSINESS_ATTRIBUTE_COLUMNS))
    if not collisions and IDENTITY_COLUMN in keys:
        return
    raise ValueError(
        f"{list(keys)} is not a dedup key this corpus can be measured with. It must "
        f"contain {IDENTITY_COLUMN!r} and none of {list(BUSINESS_ATTRIBUTE_COLUMNS)}: a "
        "legitimate repeat is a SECOND PAYMENT carrying an earlier one's attributes under "
        f"its own id, and a key touching {collisions or 'those columns'} would collapse "
        "ordinary business into the redelivery count."
    )


def watermarked_dedup(
    frame: DataFrame, *, delay_ms: int, keys: tuple[str, ...] = DEDUP_KEY
) -> DataFrame:
    """The chain: parse the instant, watermark it, dedup on the identity, drop the instant.

    `dropDuplicatesWithinWatermark` rather than `dropDuplicates`, and the difference is
    state rather than semantics: plain `dropDuplicates` expires state only through a key
    that carries the event time, and this key deliberately does not carry one, so its state
    would grow for the life of the query. The operator introduced for exactly this shape
    expires a key `delay_ms` past its own event time instead."""
    _refuse_a_key_that_cannot_make_the_stream_stateful(keys)
    _refuse_a_key_that_would_collapse_a_legitimate_repeat(keys)
    stamped = frame.withColumn(
        EVENT_INSTANT_COLUMN, F.to_timestamp(F.col(EVENT_TIME_COLUMN), INSTANT_PATTERN)
    )
    return (
        stamped.withWatermark(EVENT_INSTANT_COLUMN, f"{delay_ms} milliseconds")
        .dropDuplicatesWithinWatermark(list(keys))
        .drop(EVENT_INSTANT_COLUMN)
    )


@dataclass(frozen=True, kw_only=True)
class LandedArm:
    """One arm: the delay it read at, what SPARK says it consumed, and what landed.

    `input_rows` and `batch_ids` come from the query's own progress -- the SOURCE side --
    and `landed_rows` from the Delta table. Two independent instruments, which is what
    separates "the arms differ because of the watermark" from "the arms differ because one
    of them read less"."""

    arm: str
    delay_ms: int
    batch_ids: tuple[int, ...]
    input_rows: int
    landed_rows: int
    path: str


def run_arm(
    frame: DataFrame,
    *,
    arm: str,
    delay_ms: int,
    root: str,
    topic: str,
    minimum_rows: int = 1,
) -> LandedArm:
    """Run `frame` through the chain at `delay_ms` and land it under `root/arm`."""
    path, checkpoint = f"{root}/{arm}/sink", f"{root}/{arm}/ckpt"
    ingested = write_payment_stream(
        watermarked_dedup(frame, delay_ms=delay_ms),
        path=path,
        checkpoint=checkpoint,
        topic=topic,
        minimum_rows=minimum_rows,
    )
    return LandedArm(
        arm=arm,
        delay_ms=delay_ms,
        batch_ids=ingested.batch_ids,
        input_rows=ingested.input_rows,
        landed_rows=frame.sparkSession.read.format("delta").load(path).count(),
        path=path,
    )


@dataclass(frozen=True, kw_only=True)
class BoundaryEvidence:
    """THE PRODUCT: two landed counts over one corpus, and the rows between them."""

    boundary: LatenessBoundary
    dropping: LandedArm
    keeping: LandedArm

    @property
    def dropped_rows(self) -> int:
        return self.keeping.landed_rows - self.dropping.landed_rows


def _refuse_arms_that_did_not_read_the_same_corpus(
    dropping: LandedArm, keeping: LandedArm
) -> None:
    """The arms differ in ONE THING and it is the delay. If they consumed different records
    -- a drained checkpoint, a topic republished between them, a rate limit that split
    differently -- their landed counts differ for a reason that is not the watermark, and
    no row count can tell the two causes apart."""
    if (dropping.input_rows, dropping.batch_ids) == (keeping.input_rows, keeping.batch_ids):
        return
    raise RuntimeError(
        f"the {DROPPING} arm consumed {dropping.input_rows} records across batches "
        f"{dropping.batch_ids} and the {KEEPING} arm consumed {keeping.input_rows} across "
        f"{keeping.batch_ids}. The two arms are one experiment only while they read the "
        "same records, and their difference would otherwise be a difference in the read."
    )


def _refuse_a_split_the_boundary_was_not_derived_for(
    arm: LandedArm, boundary: LatenessBoundary
) -> None:
    """THE RATE LIMIT IS PASSED TWICE AND THIS IS WHERE THE TWO COPIES MEET.
    `lateness.boundary_for` takes it to derive the delays; `ingest.payment_stream` takes it
    to split the read. They are separate arguments at separate call sites, and until this
    function existed nothing compared them -- so a run reading at one limit under delays
    derived for another would report a difference against a prediction made for a batching
    that never happened.

    `boundary.batches_over(arm.input_rows)` IS THE COMPARISON, and it is exact rather than
    a bound because the arithmetic it guards is: `lateness.watermark_margins` puts
    delivered position `p` in batch `p // per_trigger` and computes that batch's frontier
    from the prefix ending there, so its whole account of where each watermark sits is a
    statement about a read that took EXACTLY that many offsets per batch, in delivered
    order. A run that split any other way was compared against margins belonging to a
    different read, and NEITHER DIRECTION IS ASSUMED HARMLESS: `boundary_for` refuses 69 of
    the 260 limits it was measured over, so which limit a read actually used is a question
    that already has wrong answers.

    THAT ARITHMETIC HAS A PRECONDITION AND IT IS ONE PARTITION. `p // per_trigger` is a map
    from delivered position to batch only while delivered order IS offset order, which is
    what a single-partition topic gives and what
    `tests/integration/test_late_arrival_boundary.py`'s corpus fixture says it publishes to
    for exactly this reason. What a multi-partition read would do to the batch count is not
    something this phase has measured, so the message below names the rate limit, which is
    the thing it can compare, rather than diagnosing a cause it cannot see."""
    predicted = boundary.batches_over(arm.input_rows)
    if len(arm.batch_ids) == predicted:
        return
    raise RuntimeError(
        f"the {arm.arm} arm consumed {arm.input_rows} records in {len(arm.batch_ids)} "
        f"batches {arm.batch_ids}, and the boundary's rate limit of "
        f"{boundary.max_offsets_per_trigger} predicts {predicted}. The delays were derived "
        "for a read split that way, so a query that split differently was measured against "
        "margins computed for a run that did not happen."
    )


def _refuse_a_pair_that_dropped_nothing(evidence: BoundaryEvidence) -> None:
    """TRAP 3, IN SHIPPED CODE. Two arms that landed the same count found no boundary, and
    "no late data was lost" is what that reports.

    KEPT BESIDE THE REFUSAL BELOW RATHER THAN FOLDED INTO IT. Zero is a strict subset of
    "not the predicted number" and this message names the case a reader actually hits --
    the two arms agreeing -- which the general one would report as an arithmetic
    disagreement. It runs first for that reason."""
    if evidence.dropped_rows > 0:
        return
    raise RuntimeError(
        f"both arms landed {evidence.keeping.landed_rows} rows, so the watermark at "
        f"{evidence.boundary.dropping_delay_ms} ms dropped nothing that the one at "
        f"{evidence.boundary.keeping_delay_ms} ms kept. A boundary that drops nothing is "
        "one no run can distinguish from an absent one -- and every late-arrival assertion "
        "below it would have passed."
    )


def _refuse_a_drop_the_boundary_did_not_predict(evidence: BoundaryEvidence) -> None:
    """TRAP 4, IN SHIPPED CODE, AND IT IS THE ONE THIS PHASE FELL INTO.

    `BoundaryEvidence` has held the prediction and the measurement side by side since it
    existed and compared neither. The refusal above fires on ZERO; every other wrong number
    passed. So a pair dropping 97 where the declaration says 100 returned an object whose
    every field reads as a clean measurement, and the only comparison of the two numbers in
    this repository was one line of an integration file that `addopts` deselects, needs a
    running broker, and pays for two full arms over 10,150 records before it reaches that
    line. `tests/test_streaming_watermarked_dedup.py` now makes the same comparison in the
    default invocation, over an evidence pair built by hand.

    97 IS THE NUMBER THIS PHASE ACTUALLY PRODUCED -- `lateness`'s first section is the
    account of the published prediction that met it -- so the case this refuses is the case
    that happened, not one imagined for the guard.

    THE DIFFERENCE IS EXACT AND MUST BE. `boundary.dropped_rows` is the declared late count
    and the cancellation that makes it exact even when a late record was also redelivered
    is argued on that property; a tolerance here would be a place for the next 97 to sit."""
    if evidence.dropped_rows == evidence.boundary.dropped_rows:
        return
    raise RuntimeError(
        f"the arms differ by {evidence.dropped_rows} rows "
        f"({evidence.keeping.landed_rows} kept minus {evidence.dropping.landed_rows} "
        f"dropped) and the boundary predicted {evidence.boundary.dropped_rows}. A "
        "difference of the wrong size is not a smaller boundary: it means some injected "
        "record was behind no watermark this read produced, or that rows which were not "
        "late went with the ones that were -- and either way the number this experiment "
        "reports is not a count of late arrivals."
    )


def prove_boundary(
    frame: DataFrame,
    *,
    boundary: LatenessBoundary,
    root: str,
    topic: str,
    minimum_rows: int = 1,
) -> BoundaryEvidence:
    """Run the SAME corpus at both delays and return the pair. THE T4 EXPERIMENT.

    Two checkpoints and two sinks, because both arms must start from the beginning of the
    topic: one checkpoint would leave the second arm with a drained offset log, a run of
    zero rows and -- but for `write_payment_stream`'s floor -- a difference of exactly the
    first arm's landed count."""
    arms = {
        arm: run_arm(
            frame,
            arm=arm,
            delay_ms=boundary.delay_of(arm),
            root=root,
            topic=topic,
            minimum_rows=minimum_rows,
        )
        for arm in ARMS
    }
    _refuse_arms_that_did_not_read_the_same_corpus(arms[DROPPING], arms[KEEPING])
    for arm in ARMS:
        _refuse_a_split_the_boundary_was_not_derived_for(arms[arm], boundary)
    evidence = BoundaryEvidence(
        boundary=boundary, dropping=arms[DROPPING], keeping=arms[KEEPING]
    )
    _refuse_a_pair_that_dropped_nothing(evidence)
    _refuse_a_drop_the_boundary_did_not_predict(evidence)
    return evidence


@dataclass(frozen=True, kw_only=True)
class DedupShape:
    """What a landed table holds, in the three counts T5 needs and no fewer.

    `row_count` alone cannot separate a dedup that worked from one that collapsed too much,
    and `distinct_identities` alone cannot see the legitimate repeats at all. The third
    count is the one that makes the trap visible."""

    row_count: int
    distinct_identities: int
    distinct_business_attributes: int

    @property
    def surviving_repeats(self) -> int:
        """Payments carrying an earlier payment's attributes under their own id. MUST BE
        GREATER THAN ZERO, or the dedup was never asked a hard question."""
        return self.distinct_identities - self.distinct_business_attributes

    @property
    def collapsed_rows(self) -> int:
        """Rows the dedup removed, given how many identities survived it."""
        return self.row_count - self.distinct_identities


def _refuse_a_null_identity(frame: DataFrame) -> None:
    """The identity is what every count below is taken over, and `from_json` returns a
    struct of NULLs for a value it could not parse -- so a broken parse would report itself
    as one extra distinct identity rather than as a broken parse.
    `opl.streaming.exactly_once` refuses the same shape over its own key pair, for the
    8,761 rows this repository lost to a distinct count that dropped NULLs."""
    missing = frame.filter(F.col(IDENTITY_COLUMN).isNull()).count()
    if missing == 0:
        return
    raise RuntimeError(
        f"{missing} landed rows carry a NULL {IDENTITY_COLUMN!r}. That column is the dedup "
        "key and the identity every count here is taken over, so these numbers would be "
        "describing a failed parse rather than a dedup."
    )


def dedup_shape(frame: DataFrame) -> DedupShape:
    """The three counts, over a table that is refused if it is empty or unparsed.

    `select(...).distinct().count()` and never `countDistinct(...)`: SQL's
    `COUNT(DISTINCT ...)` drops rows that are NULL in any argument, which would make a
    failed parse look like a smaller, confident number."""
    _refuse_a_null_identity(frame)
    row_count = frame.count()
    if row_count == 0:
        raise RuntimeError(
            "refusing to describe an empty table: every count is 0, all of them are true, "
            "and they are what a run that never happened also reports."
        )
    return DedupShape(
        row_count=row_count,
        distinct_identities=frame.select(IDENTITY_COLUMN).distinct().count(),
        distinct_business_attributes=(
            frame.select(*BUSINESS_ATTRIBUTE_COLUMNS).distinct().count()
        ),
    )
