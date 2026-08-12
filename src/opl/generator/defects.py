# src/opl/generator/defects.py
"""The three defects F1b carries on purpose -- duplicates, late arrivals, schema drift --
each switched on its own, each countable while the other two are off.

ADDITIVE BY CONSTRUCTION. `delivered_records(spec, NO_DEFECTS)` is `records_for(spec)`,
the same dicts in the same order, so the golden pin on the clean stream is untouched by
this module existing. Every defect is a transformation applied to a clean stream that was
already reproducible; none of them reaches back into the derivation. That is what keeps
"the pinned stream moved" a statement about `opl.generator.stream` alone.

INDEPENDENTLY SWITCHABLE MEANS THE POSITIONS DO NOT MOVE, which is stronger than three
booleans and is the property a test isolating one class actually needs. Each class draws
its positions under its OWN derivation purpose, indexed by the CLEAN stream's emission
index -- so the rows that duplicate are the same rows whether or not lateness is on, and
the drift boundary falls in the same place whether or not anything duplicates. A
selection that consumed a shared sequence, or that keyed off the delivery position after
an earlier class had reordered it, would make every isolated measurement true only of the
combination it was measured in.

--- DUPLICATES ------------------------------------------------------------------------
A duplicate is THE SAME EVENT DELIVERED TWICE: the record's bytes, again, carrying the
`transaction_id` it already carried. It is not a second payment and it has no timestamps
of its own -- both would make it a legitimate repeat, which the clean stream already
emits (`spec.repeat_count`) precisely so that the two are distinguishable. Hence:

    COUNT(*) - COUNT(DISTINCT transaction_id)  == duplicate_count   (redeliveries only)
    COUNT(DISTINCT business attributes)        == spec.base_count   (unmoved by either)

A legitimate repeat adds one to both terms of the first line and cancels; a duplicate
adds one to the first only. If those two counts ever agree with each other, the fixture
is measuring nothing.

THE REDELIVERY IS DELIVERED IMMEDIATELY AFTER THE ROW IT REPEATS, and that is a decision
rather than a convenience. A redelivery carries its original's `emitted_at` verbatim --
it has to, or it is not byte-identical -- so placing it anywhere else would produce a
file whose row order disagrees with its own `emitted_at` column. Under the ordering
reading of lateness that disagreement is nothing; under a row-order reading of the same
English sentence ("records already emitted") every duplicate would count as a late
arrival, and the two classes would contaminate each other in whichever reading Task 4's
query author happened to pick. Keeping row order and `emitted_at` order identical makes
BOTH readings agree, and costs nothing measurable: the duplicate count is positional-
independent, and bronze is an unordered Delta table where file position does not survive
ingest at all. The rejected alternative -- scattering redeliveries down the file, which
is what a real redelivery batch looks like -- buys realism nothing downstream can read,
at the price of that contamination.

--- LATE ARRIVALS ----------------------------------------------------------------------
A late arrival is a record whose `event_time` precedes the `event_time` of records
already emitted. NO CLOCK IS READ: lateness is injected by adding `late_by_ms` to a
chosen record's `emitted_at` and to nothing else. Its `event_time` -- when the payment
happened -- is untouched, because the payment did not move; only the delivery did.

`late_by_ms` must be at least `LATENESS_WINDOW_MS`, so every injected record crosses at
least one tumbling window boundary: a windowed aggregation over `event_time`, keyed off
`emitted_at`, had already closed and emitted that window before the row turned up. That
is the consequence lateness has, as opposed to the relation lateness is.

AND THE INJECTION IS VERIFIED AGAINST THE MEASUREMENT, not assumed. Delaying the LAST
record of a stream makes it no later than it was -- nothing follows it to be newer -- and
delaying every record by a constant is just a longer emission lag. Both produce a stream
with a `late_count` field and zero late arrivals in it, which is the exact shape of a
fixture that proves nothing. `_require_the_injected_lateness_is_measurable` therefore
runs `opl.generator.measures.late_arrivals` -- the same function Task 4's query mirrors,
not a second spelling of it -- over the delivery and REFUSES when the set it measures is
not the set that was injected.

--- SCHEMA DRIFT -------------------------------------------------------------------------
A new optional column appears mid-stream: from `drift_from_index` onward, every record
carries `opl.contracts.payments.DRIFT_COLUMN`, and no record before it does. Schema
drift rather than value drift, because it is the class that tests the contract layer and
the DQ gate; a shifted distribution is something the vault does not model.

KEYED ON THE EMISSION INDEX, NOT ON THE DELIVERY POSITION. The two are the same sequence
until lateness moves rows, and keying on delivery position would mean that switching
lateness on silently changed which records carry the column -- the independence property
above, lost. Keyed on emission index, "the producer started sending this field at 09:15"
is what it says, and a late record from before that instant correctly arrives without it.

A REDELIVERY CARRIES ITS ORIGINAL'S BYTES, drift column and all, because byte-identity is
what a redelivery IS. A pre-drift record redelivered after the drift point therefore
still lacks the column -- which is exactly what a processor replaying an old message
does, and is the one place where "byte-identical" and "everything after position N
carries the column" could have been made to disagree.

THE TRAP THIS COLUMN SETS, AND WHY IT IS DISARMED HERE AND NOT ONLY DOCUMENTED. Every
record before the drift point is NULL in the drift column. A `COUNT(DISTINCT ...)` that
included it would drop that entire population -- silently, in the same shape as the 8,761
rows this repository has already lost to that behaviour, and on this phase's own
evidence. `opl.contracts.payments._assert_no_drifting_column_is_declared_by_v1` refuses
AT IMPORT to let the column into `COLUMNS`, `REQUIRED_COLUMNS` or
`BUSINESS_ATTRIBUTE_COLUMNS`, and `opl.generator.measures` gives the dropping count a
name that says so. Nothing here has to be remembered.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from opl.contracts.payments import DRIFT_COLUMN, DRIFT_VALUES
from opl.generator.derivation import digest, pick, require_whole
from opl.generator.events import PaymentEvent, record_of
from opl.generator.instants import MILLISECONDS_PER_HOUR
from opl.generator.measures import late_arrivals
from opl.generator.stream import PURPOSES, StreamSpec, generate, purpose_for

# The tumbling window the lateness claim is stated against. One hour, and it is a module
# constant rather than a spec field because it is part of what "late" MEANS here, not
# part of what a run asks for: changing it changes which streams are legal, which is a
# code change with a diff. Nothing downstream reads it -- a watermark is Task 3's
# decision -- so it is a floor on the injected delay and a unit for the assertion, and
# it is deliberately not written into any record.
LATENESS_WINDOW_MS = MILLISECONDS_PER_HOUR

# THE DEFECT DERIVATION PURPOSES, in the shared namespace `opl.generator.stream.PURPOSES`
# opens. Checked against that tuple at import: a defect purpose equal to a stream purpose
# would tie a record's duplicate-selection digest to its own amount, giving a stream that
# reproduces perfectly and encodes a correlation nobody chose.
PURPOSE_DUPLICATE_POSITION = "duplicate-position"
PURPOSE_LATE_POSITION = "late-position"
PURPOSE_DRIFT_VALUE = "drift-value"

DEFECT_PURPOSES = (
    PURPOSE_DUPLICATE_POSITION,
    PURPOSE_LATE_POSITION,
    PURPOSE_DRIFT_VALUE,
)


def _assert_defect_purposes_are_distinct() -> None:
    """Fail at import if a defect purpose collides with another one, or with a stream
    purpose, once `hash_key` has upper-cased them all.

    ACROSS BOTH MODULES, which is the half a per-module check would miss: the purpose is
    hashed, not namespaced, so `opl.generator.stream`'s guard passing says nothing about
    a collision introduced here."""
    folded = [purpose.upper() for purpose in (*PURPOSES, *DEFECT_PURPOSES)]
    if len(set(folded)) != len(folded):
        raise ValueError(
            f"a defect purpose collides with another purpose after upper-casing: "
            f"{sorted(folded)}. `hash_key` normalises its components, so two fields "
            "drawing under one purpose draw the SAME value at every index -- here that "
            "would make which rows duplicate a function of what those rows cost."
        )


_assert_defect_purposes_are_distinct()


@dataclass(frozen=True, kw_only=True)
class DefectSpec:
    """Which defects a delivery carries, and how much of each. Frozen, `kw_only`.

    EVERY FIELD DEFAULTS TO OFF, so `DefectSpec()` -- exported as `NO_DEFECTS` -- is the
    clean stream and a caller enabling one class states exactly one thing. `kw_only` for
    `StreamSpec`'s reason, sharpened: `duplicate_count` and `late_count` are adjacent,
    both `int`, both small, and a positional swap would produce a stream that validates,
    reproduces and carries the wrong defect in the right quantity.

    `late_by_ms` is separate from `late_count` rather than a per-record draw: one delay
    for every late record makes the injected set's effect on the ordering a single
    reviewable number, and a spread of delays adds a distribution nothing measures."""

    duplicate_count: int = 0
    late_count: int = 0
    late_by_ms: int = 0
    drift_from_index: int | None = None

    def __post_init__(self) -> None:
        require_whole(self.duplicate_count, name="duplicate_count")
        require_whole(self.late_count, name="late_count")
        require_whole(self.late_by_ms, name="late_by_ms")
        _require_the_delay_matches_the_count(self.late_count, self.late_by_ms)
        if self.drift_from_index is not None:
            _require_the_drift_is_mid_stream(self.drift_from_index)

    @property
    def is_clean(self) -> bool:
        """Whether this spec injects nothing at all."""
        return not (self.duplicate_count or self.late_count or self.drift_from_index)


def _require_the_delay_matches_the_count(late_count: int, late_by_ms: int) -> None:
    """Refuse a lateness that is declared and cannot happen, in either direction.

    A `late_count` with too small a delay is the dangerous half: it produces a stream
    that reports late arrivals in its spec and holds none, so a test asserting "the
    measured set is the injected set" would be comparing two empty sets and passing.
    The reverse -- a delay with nothing to apply it to -- is refused too, because two
    specs that differ only in an unused field are two names for one stream."""
    if late_count and late_by_ms < LATENESS_WINDOW_MS:
        raise ValueError(
            f"late_count={late_count} with late_by_ms={late_by_ms} is below one window "
            f"({LATENESS_WINDOW_MS} ms). A late arrival must cross at least one window "
            "boundary, or the aggregation it is meant to arrive behind never closed."
        )
    if late_by_ms and not late_count:
        raise ValueError(
            f"late_by_ms={late_by_ms} with late_count=0 delays nothing. Two specs that "
            "differ only in a field neither stream reads are two names for one stream."
        )


def _require_the_drift_is_mid_stream(drift_from_index: int) -> None:
    """Refuse a drift that starts at the first record.

    MID-stream is the whole class. A column present from position 0 is a column the
    stream simply has: there is no pre-drift population to be NULL in it, nothing for a
    reader holding the v1 schema to rescue, and nothing for a `COUNT(DISTINCT ...)` to
    drop. The upper bound needs the stream's length and is checked in
    `_require_defects_fit`."""
    require_whole(drift_from_index, name="drift_from_index")
    if drift_from_index < 1:
        raise ValueError(
            "drift_from_index must be at least 1: a column that every record carries is "
            "not drift, and a stream with no pre-drift rows has nothing to measure the "
            "drift against"
        )


NO_DEFECTS = DefectSpec()


def _require_defects_fit(spec: StreamSpec, defects: DefectSpec) -> None:
    """Refuse a `DefectSpec` this `StreamSpec` cannot carry. Cross-checks only.

    Kept out of `DefectSpec.__post_init__` because none of it is knowable there: a defect
    spec is a description of what to inject, and how many rows exist to inject into is
    the stream's business. The alternative -- one spec class holding both -- would mean
    every stream and every defect combination were one frozen object, and the isolation
    property this module is built on is exactly the ability to vary them separately."""
    if defects.late_count and defects.late_by_ms <= spec.event_interval_ms:
        raise ValueError(
            f"late_by_ms={defects.late_by_ms} does not exceed event_interval_ms="
            f"{spec.event_interval_ms}, so a delayed record is still emitted before the "
            "next one happened and no ordering is disturbed. Lateness is a REORDERING, "
            "not a delay."
        )
    ceiling = spec.event_count - 1
    if defects.drift_from_index is not None and defects.drift_from_index > ceiling:
        raise ValueError(
            f"drift_from_index={defects.drift_from_index} leaves no drifted record in a "
            f"stream of {spec.event_count}: at most {ceiling} records may precede the "
            "drift, or the column that is supposed to appear never does"
        )


def _selected(spec: StreamSpec, *, purpose: str, count: int, candidates: range) -> frozenset[int]:
    """`count` of `candidates`, chosen by sorting them on their own digest.

    The sampling-without-replacement shape `opl.generator.stream._repeat_positions` uses,
    and for its reasons: one sort, no rejection loop, no worst case as `count` approaches
    the candidate space, and `sorted` is stable so even equal digests resolve the same
    way twice. Drawn over CANDIDATES rather than over the whole stream, because the two
    classes exclude different positions and the exclusion is part of the class."""
    if count > len(candidates):
        raise ValueError(
            f"{count} positions were asked for under {purpose!r} and only "
            f"{len(candidates)} are eligible ({candidates}). Injecting fewer than asked "
            "would make every count downstream measured rather than predicted."
        )
    ordered = sorted(
        candidates,
        key=lambda position: digest(
            seed=spec.seed, purpose=purpose_for(spec, purpose), index=position
        ),
    )
    return frozenset(ordered[:count])


def duplicate_positions(spec: StreamSpec, defects: DefectSpec) -> frozenset[int]:
    """The emission indices whose record is delivered a second time.

    EVERY position is eligible, including the last: a redelivery is a property of the
    delivery and nothing about the stream's shape forbids the final record being sent
    twice."""
    return _selected(
        spec,
        purpose=PURPOSE_DUPLICATE_POSITION,
        count=defects.duplicate_count,
        candidates=range(spec.event_count),
    )


def late_positions(spec: StreamSpec, defects: DefectSpec) -> frozenset[int]:
    """The emission indices whose `emitted_at` is delayed by `defects.late_by_ms`.

    THE LAST RECORD IS NOT ELIGIBLE, and that is structural rather than fastidious:
    lateness is "emitted after records whose `event_time` is newer than its own", and
    nothing has a newer `event_time` than the last event. Delaying it would add a field
    to a spec and nothing to the stream, and `_require_the_injected_lateness_is_
    measurable` would then refuse the whole delivery for a reason two steps removed from
    the cause."""
    return _selected(
        spec,
        purpose=PURPOSE_LATE_POSITION,
        count=defects.late_count,
        candidates=range(spec.event_count - 1),
    )


def drift_positions(spec: StreamSpec, defects: DefectSpec) -> frozenset[int]:
    """The emission indices whose record carries `DRIFT_COLUMN`.

    A SUFFIX, NOT A SAMPLE. Drift is the producer changing what it sends, so it applies
    from an instant onward; a scattered subset would be a field that flickers, which is a
    different defect (and one no schema reader would meet as drift)."""
    if defects.drift_from_index is None:
        return frozenset()
    _require_defects_fit(spec, defects)
    return frozenset(range(defects.drift_from_index, spec.event_count))


def _drift_value(spec: StreamSpec, *, index: int) -> str:
    """The drift column's value for the record emitted at `index`."""
    return pick(
        DRIFT_VALUES,
        seed=spec.seed,
        purpose=purpose_for(spec, PURPOSE_DRIFT_VALUE),
        index=index,
    )


def _delayed(event: PaymentEvent, defects: DefectSpec) -> PaymentEvent:
    """`event` released `defects.late_by_ms` later, and otherwise untouched.

    `replace` rather than mutation, for the reason the dataclass is frozen: the clean
    event is a fact, and a delivery that carried a mutated one would make "the clean
    stream is unchanged" depend on nobody having generated a defective stream first.

    `event_time_ms` IS NOT TOUCHED. The payment did not move; the delivery did. Adding
    the delay to both would produce a record that is simply later, which is a stream with
    a different window, not a stream with a late arrival in it."""
    return replace(event, emitted_at_ms=event.emitted_at_ms + defects.late_by_ms)


def _delivery_order(spec: StreamSpec, defects: DefectSpec) -> tuple[tuple[int, PaymentEvent], ...]:
    """(emission index, event) in delivery order, lateness applied, no redeliveries yet.

    Sorted by `emitted_at` with the emission index as the tie-break, so the file's row
    order and its `emitted_at` column never disagree. The emission index is carried
    alongside because the other two classes are keyed on it and this sort is exactly the
    step that stops it being the position in the result."""
    _require_defects_fit(spec, defects)
    late = late_positions(spec, defects)
    events = tuple(
        _delayed(event, defects) if index in late else event
        for index, event in enumerate(generate(spec))
    )
    ordered = tuple(
        (index, events[index])
        for index in sorted(range(len(events)), key=lambda at: (events[at].emitted_at_ms, at))
    )
    _require_the_injected_lateness_is_measurable(ordered, late)
    return ordered


def _require_the_injected_lateness_is_measurable(
    ordered: tuple[tuple[int, PaymentEvent], ...], late: frozenset[int]
) -> None:
    """Refuse a delivery whose measured late arrivals are not the injected ones.

    MEASURED BY `opl.generator.measures.late_arrivals`, the same function the tests and
    Task 4's SQL mirror, rather than by a second implementation of the definition living
    here. A private re-spelling would agree with itself forever, including on the day it
    stopped agreeing with the query it exists to predict.

    Run over the ORIGINALS, before redeliveries are inserted: a redelivery of a late
    record is itself late by every definition -- it carries that record's two timestamps
    verbatim -- so measuring after insertion would compare `late_count` against
    `late_count + <redeliveries of late records>` and refuse a correct stream."""
    delivered = [record_of(event) for _, event in ordered]
    measured = {ordered[position][0] for position in late_arrivals(delivered)}
    if measured != late:
        raise ValueError(
            f"lateness was injected at emission indices {sorted(late)} and measures as "
            f"{sorted(measured)}. A record delayed past nothing newer than itself is not "
            "late, and a stream whose spec claims late arrivals it does not contain is a "
            "fixture that passes its own test by measuring two empty sets."
        )


def delivered_records(spec: StreamSpec, defects: DefectSpec) -> tuple[dict[str, str], ...]:
    """The stream `spec` describes, delivered with the defects `defects` asks for.

    The counterpart of `opl.generator.stream.records_for`, and equal to it exactly when
    `defects` is `NO_DEFECTS` -- asserted in the tests, so this module cannot quietly
    become a thing the clean stream passes through.

    Returns rendered records rather than events because two of the three defects are not
    expressible as events: a redelivery is one record twice, and a drifted record carries
    a key `PaymentEvent` has no field for. Rendering here keeps `PaymentEvent` the shape
    of a payment rather than the shape of a delivery."""
    drifted = drift_positions(spec, defects)
    duplicated = duplicate_positions(spec, defects)
    rows: list[dict[str, str]] = []
    for index, event in _delivery_order(spec, defects):
        record = record_of(event)
        if index in drifted:
            record = {**record, DRIFT_COLUMN: _drift_value(spec, index=index)}
        rows.append(record)
        if index in duplicated:
            rows.append(dict(record))  # the same bytes again: a redelivery, not a repeat
    return tuple(rows)
