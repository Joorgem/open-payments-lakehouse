# src/opl/generator/stream.py
"""The clean payment stream: a declared spec in, a tuple of events out, nothing else.

WHAT "CLEAN" MEANS HERE, precisely, because the word is doing real work. This stream
contains NO duplicates, NO late arrivals and NO drift. Those are F1b Task 2, and they
are absent on purpose: a defect injected before the seed is known to reproduce
byte-identically gives any later failure two candidate causes, and telling them apart
costs more than building the clean stream first.

WHAT IT DOES CONTAIN THAT LOOKS LIKE A DEFECT AND IS NOT: LEGITIMATE REPEATS. A
repeat is a payment that duplicates an earlier payment's BUSINESS ATTRIBUTES -- same
payer, same payee, same amount, same currency, same method -- while carrying its own
`transaction_id` and its own timestamps. That is a customer paying the same supplier
the same amount twice, which is ordinary business and must survive every dedup. A
DUPLICATE, when Task 2 adds it, is the same `transaction_id` delivered twice. If the
clean stream contained no repeats there would be nothing for a duplicate to be
confused with, and a dedup that collapsed both would pass every test. See
`opl.contracts.payments` for the full argument.

THE COUNTS ARE EXACT, AND THAT IS BUILT RATHER THAN HOPED FOR. Over a generated
stream:

    DISTINCT transaction_id            == spec.event_count
    DISTINCT business-attribute tuple  == spec.event_count - spec.repeat_count

The second equality is the one that needs work. Drawing attributes at random would
let two BASE events collide by chance -- a small pool and five methods make that
likely, not exotic -- and the distinct count would drift below the arithmetic by an
amount nobody could predict. So a base event whose attribute tuple has already been
produced is RE-DERIVED under an incremented salt until it is new, and the generator
REFUSES rather than emitting a collision if it cannot find one. Task 2's duplicate
count then subtracts from a number that is known, not measured.

TWO TIMESTAMPS, ONE ORDER, AND NO CLOCK. `event_time` is the window's start plus the
event's index times `event_interval_ms`; `emitted_at` is that plus a constant
`emission_lag_ms`. Two properties follow, and both are what Task 2 will break on
purpose:

    for every record:  emitted_at >= event_time        (nothing is emitted before it
                                                        happened)
    across the stream: sorting by emitted_at gives the same order as sorting by
                       event_time                       (nothing arrives late)

A late arrival is precisely a record for which the second sentence is false -- it is
emitted after records whose `event_time` is newer than its own. Stating lateness as an
ordering between two derived columns is what lets it exist without a wall clock ever
being read.

THE POOL IS AN ARGUMENT. `StreamSpec.cnpj_pool` is handed in, already canonical, by
`opl.generator.cnpj_pool.validated_pool`. This module never queries anything. Every
counterparty in the output is a member of that pool, so the resolution rate against
`hub_empresa` is 100% exactly when the pool came from `hub_empresa` -- one boundary,
one claim, one measurement.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

from opl.contracts.payments import CURRENCIES, PAYMENT_METHODS
from opl.generator.cnpj_pool import validated_pool
from opl.generator.derivation import digest, draw_below, pick, require_whole
from opl.generator.events import PaymentEvent, record_of
from opl.generator.instants import from_text

# The amount domain, in centavos: R$1.00 to R$50,000.00. Module constants rather than
# spec fields, because they are part of what this generator IS rather than of what a
# particular run asks for -- widening the range changes every amount in every stream
# ever generated, which is a code change with a diff, not a parameter.
MIN_AMOUNT_CENTS = 100
MAX_AMOUNT_CENTS = 5_000_000

# How many times a base event's attributes may be re-derived before the generator
# gives up. The attribute space is |pool| * (|pool| - 1) * |amounts| * |currencies| *
# |methods|, which for the smallest legal pool is already 2 * 1 * 4,999,901 * 1 * 5 --
# so exhausting 64 salts means the caller asked for more distinct events than the
# space holds, and the honest answer is a refusal naming that.
MAX_ATTRIBUTE_ATTEMPTS = 64

# THE DERIVATION PURPOSES. Every drawn value names the field it is for, so adding a
# field never shifts an existing one (see `opl.generator.derivation`). Distinctness is
# asserted at import CASE-INSENSITIVELY, because `hash_key` upper-cases its components
# -- so "payer" and "PAYER" would be one purpose and two fields would silently draw
# the same value forever.
#
# PUBLIC, ALONG WITH `purpose_for` BELOW, BECAUSE THE PURPOSE NAMESPACE IS SHARED.
# `opl.generator.defects` draws under its own purposes from the same (seed, stream_id)
# space, and a defect purpose colliding with one of these would tie a defect's position
# to a payment's amount -- perfectly correlated, perfectly reproducible, and chosen by
# nobody. That module asserts its purposes against this tuple at import, which it can
# only do if the tuple has a name it is allowed to read.
PURPOSE_PAYER = "payer"
PURPOSE_PAYEE = "payee"
PURPOSE_AMOUNT = "amount"
PURPOSE_CURRENCY = "currency"
PURPOSE_METHOD = "payment-method"
PURPOSE_REPEAT_POSITION = "repeat-position"
PURPOSE_REPEAT_SOURCE = "repeat-source"
PURPOSE_TRANSACTION_ID = "transaction-id"

PURPOSES = (
    PURPOSE_PAYER,
    PURPOSE_PAYEE,
    PURPOSE_AMOUNT,
    PURPOSE_CURRENCY,
    PURPOSE_METHOD,
    PURPOSE_REPEAT_POSITION,
    PURPOSE_REPEAT_SOURCE,
    PURPOSE_TRANSACTION_ID,
)


def _assert_purposes_are_distinct() -> None:
    """Fail at import if two purposes collide once `hash_key` has upper-cased them.

    NOT COSMETIC, and not caught by reading the list: two fields sharing a purpose
    draw the SAME 256-bit value at every index, so (for instance) a stream's payer
    index and payee index would be equal for every record. The payee redraw below
    would then push every payee one place along the pool, forever -- a stream that
    looks fine, reproduces perfectly, and encodes a relationship nobody chose."""
    folded = [purpose.upper() for purpose in PURPOSES]
    if len(set(folded)) != len(folded):
        raise ValueError(
            f"two derivation purposes collide after upper-casing: {sorted(folded)}. "
            "`hash_key` normalises its components, so purposes differing only in case "
            "are one purpose and the two fields drawing under them are perfectly "
            "correlated."
        )


_assert_purposes_are_distinct()

# A stream id names one emission run. UPPER CASE ONLY, and that is not a style rule:
# `hash_key` upper-cases every component, so `"a"` and `"A"` would derive the SAME
# stream while reading as two. Refusing lower case makes the case-folding a no-op
# instead of a trap. No separators, because the id is also the natural filename stem
# for whatever writes the stream out.
_STREAM_ID_ALPHABET = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")


@dataclass(frozen=True, kw_only=True)
class _Attributes:
    """What a payment WAS, before it is given an identity or a time.

    Frozen, so it is hashable and can go straight into the set that keeps base events
    distinct -- which is the same tuple `events.business_attributes_of` reads back off
    a rendered record, one representation apart."""

    payer_cnpj_basico: str
    payee_cnpj_basico: str
    amount_cents: int
    currency: str
    payment_method: str


@dataclass(frozen=True, kw_only=True)
class StreamSpec:
    """Everything one generated stream depends on. Frozen: a spec is the seed's twin.

    `kw_only` for `PaymentEvent`'s reason, with a sharper example here:
    `event_interval_ms` and `emission_lag_ms` are adjacent, both `int`, both
    milliseconds, and swapping them produces a stream that validates, reproduces, and
    describes a different world.

    `cnpj_pool` MUST ALREADY BE CANONICAL -- the output of
    `opl.generator.cnpj_pool.validated_pool`. This class refuses a pool it would have
    had to normalise rather than normalising it silently, because the pool's ORDER
    decides which company gets which payment: a spec that quietly sorted its input
    would generate a different stream from the one its literal argument describes, and
    the argument is what a reader would reconstruct the run from."""

    seed: int
    stream_id: str
    event_count: int
    repeat_count: int
    window_start: str
    event_interval_ms: int
    emission_lag_ms: int
    cnpj_pool: tuple[str, ...]

    def __post_init__(self) -> None:
        require_whole(self.seed, name="seed")
        require_whole(self.repeat_count, name="repeat_count")
        require_whole(self.emission_lag_ms, name="emission_lag_ms")
        _require_stream_id(self.stream_id)
        _require_positive(self.event_count, name="event_count")
        _require_positive(self.event_interval_ms, name="event_interval_ms")
        _require_room_for_repeats(self.event_count, self.repeat_count)
        _require_canonical_pool(self.cnpj_pool)
        from_text(self.window_start)  # refuses any spelling `to_text` would not emit

    @property
    def base_count(self) -> int:
        """How many events carry attributes no earlier event carried."""
        return self.event_count - self.repeat_count


def _require_positive(value: int, *, name: str) -> int:
    """`value` as an `int` of at least one, or refuse.

    Separate from `require_whole` because zero is a different mistake here: a
    `event_interval_ms` of 0 gives every event the same `event_time`, which does not
    fail -- it produces a stream in which the emission order and the event order are
    trivially equal, so the late-arrival test would pass over a stream that cannot
    express lateness at all."""
    require_whole(value, name=name)
    if value < 1:
        raise ValueError(
            f"{name} must be at least 1, got {value} -- zero produces a stream whose "
            "timestamps cannot distinguish one event from the next, which makes every "
            "ordering property vacuously true"
        )
    return value


def _require_stream_id(stream_id: str) -> str:
    """A non-empty upper-case identifier, or refuse. See `_STREAM_ID_ALPHABET`."""
    if not isinstance(stream_id, str) or not stream_id:
        raise ValueError(f"stream_id must be a non-empty str, got {stream_id!r}")
    illegal = sorted(set(stream_id) - _STREAM_ID_ALPHABET)
    if illegal:
        raise ValueError(
            f"stream_id {stream_id!r} carries {illegal}, which it may not. The id is "
            "hashed into every derivation and `hash_key` upper-cases its components, "
            "so a lower-case id would derive the SAME stream as its upper-case twin "
            "while reading as a different one. Allowed: A-Z, 0-9, '-' and '_'."
        )
    return stream_id


def _require_room_for_repeats(event_count: int, repeat_count: int) -> None:
    """Refuse a stream with no base event, or with a repeat at position zero.

    A repeat copies an EARLIER base event's attributes, so position 0 can never be
    one and there must be at least one base event to copy. Both collapse into
    `repeat_count <= event_count - 1`, and the message says which half is meant."""
    if repeat_count > event_count - 1:
        raise ValueError(
            f"repeat_count={repeat_count} leaves no room in event_count={event_count}: "
            "a legitimate repeat copies an EARLIER event's attributes, so position 0 "
            "is always a base event and at most event_count - 1 positions can repeat"
        )


def _require_canonical_pool(pool: tuple[str, ...]) -> None:
    """Refuse a pool that is not exactly `validated_pool`'s own output."""
    canonical = validated_pool(pool)
    if tuple(pool) != canonical:
        raise ValueError(
            "cnpj_pool is not canonical: it holds the right keys in another order. "
            "Pass `opl.generator.cnpj_pool.validated_pool(...)`'s result, which is "
            "sorted -- the pool's order decides which company gets which payment, so "
            "normalising it here would silently generate a different stream from the "
            "one this spec literally describes."
        )


def purpose_for(spec: StreamSpec, field: str) -> str:
    """The derivation purpose for `field` within `spec`'s stream.

    The stream id is folded in HERE rather than passed as a separate component, so
    every draw in the module reads as one purpose string and two streams with the same
    seed and different ids share no value at all.

    PUBLIC because `opl.generator.defects` must spell this exactly, not similarly: a
    second `f"{spec.stream_id}/{field}"` written at another call site would keep working
    until one of them gained a separator or a case fold, at which point the defect
    positions of every stream ever generated would move with no diff to point at."""
    return f"{spec.stream_id}/{field}"


def _counterparties(spec: StreamSpec, *, index: int, salt: int) -> tuple[str, str]:
    """(payer, payee) for event `index`, guaranteed distinct.

    THE PAYEE IS DRAWN FROM A SPACE ONE SMALLER AND THEN STEPPED OVER THE PAYER,
    rather than redrawn until it differs. Redrawing would make the number of hash
    evaluations depend on the values drawn, which is the property
    `opl.generator.derivation` exists not to have; this is exact, unbiased and
    constant-cost. `pool` is guaranteed to hold at least two keys by
    `cnpj_pool.MINIMUM_POOL_SIZE`, so the reduced bound is never zero.

    A company paying itself is a real thing (intra-group settlement) and is excluded
    anyway: it would make `payer == payee` for a fraction of rows, and every
    counterparty-pair count downstream would then have to say whether it meant that
    case, for no gain in what this stream is for."""
    pool = spec.cnpj_pool
    payer_at = draw_below(
        seed=spec.seed, purpose=purpose_for(spec, PURPOSE_PAYER), index=index,
        bound=len(pool), salt=salt,
    )
    payee_at = draw_below(
        seed=spec.seed, purpose=purpose_for(spec, PURPOSE_PAYEE), index=index,
        bound=len(pool) - 1, salt=salt,
    )
    if payee_at >= payer_at:
        payee_at += 1
    return pool[payer_at], pool[payee_at]


def _attributes(spec: StreamSpec, *, index: int, salt: int) -> _Attributes:
    """The candidate business attributes for event `index` under `salt`."""
    payer, payee = _counterparties(spec, index=index, salt=salt)
    span = MAX_AMOUNT_CENTS - MIN_AMOUNT_CENTS + 1
    cents = MIN_AMOUNT_CENTS + draw_below(
        seed=spec.seed, purpose=purpose_for(spec, PURPOSE_AMOUNT), index=index,
        bound=span, salt=salt,
    )
    return _Attributes(
        payer_cnpj_basico=payer,
        payee_cnpj_basico=payee,
        amount_cents=cents,
        currency=pick(
            CURRENCIES, seed=spec.seed, purpose=purpose_for(spec, PURPOSE_CURRENCY),
            index=index, salt=salt,
        ),
        payment_method=pick(
            PAYMENT_METHODS, seed=spec.seed, purpose=purpose_for(spec, PURPOSE_METHOD),
            index=index, salt=salt,
        ),
    )


def _distinct_attributes(
    spec: StreamSpec, *, index: int, seen: set[_Attributes]
) -> _Attributes:
    """Attributes for base event `index` that no earlier base event produced.

    REFUSES RATHER THAN REPEATING once `MAX_ATTRIBUTE_ATTEMPTS` salts are exhausted.
    A silent collision here would not fail anything -- it would produce a stream with
    one fewer distinct attribute tuple than the arithmetic says, so Task 2's duplicate
    count would be measured against a number that is wrong by an amount nobody knows.
    A refusal naming the pool size is the one outcome that keeps that count exact."""
    for salt in range(MAX_ATTRIBUTE_ATTEMPTS):
        candidate = _attributes(spec, index=index, salt=salt)
        if candidate not in seen:
            return candidate
    raise ValueError(
        f"event {index} could not be given business attributes distinct from the "
        f"{len(seen)} already emitted, after {MAX_ATTRIBUTE_ATTEMPTS} attempts. The "
        f"pool holds {len(spec.cnpj_pool)} companies; ask for fewer events or extract "
        "a larger pool. Emitting the collision instead would make the stream's "
        "distinct-attribute count unpredictable, and Task 2 subtracts from it."
    )


def _repeat_positions(spec: StreamSpec) -> frozenset[int]:
    """Which positions in the stream carry a legitimate repeat.

    SELECTED BY SORTING THE CANDIDATE POSITIONS ON THEIR OWN DIGEST and taking the
    first `repeat_count`, rather than by drawing positions and discarding collisions.
    Sampling without replacement by rejection would make the work depend on how many
    collisions occurred, and would degrade badly exactly when `repeat_count`
    approaches `event_count`; this is one sort and has no worst case. `sorted` is
    stable, so even the impossible case of two equal digests resolves deterministically.

    Position 0 is excluded: a repeat copies an EARLIER base event, and there is none."""
    candidates = range(1, spec.event_count)
    ordered = sorted(
        candidates,
        key=lambda position: digest(
            seed=spec.seed,
            purpose=purpose_for(spec, PURPOSE_REPEAT_POSITION),
            index=position,
        ),
    )
    return frozenset(ordered[: spec.repeat_count])


def _transaction_id(spec: StreamSpec, *, index: int) -> str:
    """The event's identity: 64 lower-case hex characters, opaque by construction.

    A FUNCTION OF THE EMISSION POSITION AND NOTHING ELSE -- not of the payer, not of
    the amount, not of the time. That is the T2 property in one line: two events with
    identical business attributes sit at different positions, so they get different
    ids, and a dedup that collapses them is caught. An id derived from the attributes
    would make a legitimate repeat indistinguishable from a redelivery and every dedup
    claim downstream unfalsifiable.

    Opaque: nothing may parse it. It carries no time, no counterparty and no ordering,
    and `opl.contracts.payments` says so where a consumer will read it."""
    return digest(
        seed=spec.seed, purpose=purpose_for(spec, PURPOSE_TRANSACTION_ID), index=index
    ).hex()


def _event_at(spec: StreamSpec, *, index: int, attributes: _Attributes) -> PaymentEvent:
    """Give `attributes` an identity and its two timestamps.

    `event_time` is the window start plus whole intervals; `emitted_at` is that plus a
    constant lag, so in the clean stream the two orders agree exactly. No clock is
    read; see the module docstring on why lateness is an ordering rather than a delay."""
    event_time_ms = from_text(spec.window_start) + index * spec.event_interval_ms
    return PaymentEvent(
        transaction_id=_transaction_id(spec, index=index),
        event_time_ms=event_time_ms,
        emitted_at_ms=event_time_ms + spec.emission_lag_ms,
        **asdict(attributes),
    )


def generate(spec: StreamSpec) -> tuple[PaymentEvent, ...]:
    """The clean stream `spec` describes, in emission order.

    `spec.event_count` events; `spec.repeat_count` of them carry an earlier event's
    business attributes under their own `transaction_id`; the rest carry attributes no
    other event carries. No duplicates, no late arrivals, no drift -- those are Task
    2, and this function is the thing they will be layered onto."""
    repeats = _repeat_positions(spec)
    seen: set[_Attributes] = set()
    bases: list[_Attributes] = []
    events: list[PaymentEvent] = []
    for index in range(spec.event_count):
        if index in repeats:
            source = draw_below(
                seed=spec.seed, purpose=purpose_for(spec, PURPOSE_REPEAT_SOURCE),
                index=index, bound=len(bases),
            )
            attributes = bases[source]
        else:
            attributes = _distinct_attributes(spec, index=index, seen=seen)
            seen.add(attributes)
            bases.append(attributes)
        events.append(_event_at(spec, index=index, attributes=attributes))
    return tuple(events)


def records_for(spec: StreamSpec) -> tuple[dict[str, str], ...]:
    """`generate(spec)` rendered into the contract's string columns, in order.

    The shape everything downstream wants: `opl.generator.events.to_jsonl` turns it
    into the file's text, and a test walks it to assert that no value is anything but
    a non-empty `str`."""
    return tuple(record_of(event) for event in generate(spec))
