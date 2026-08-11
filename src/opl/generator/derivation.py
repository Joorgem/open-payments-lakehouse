# src/opl/generator/derivation.py
"""Every varying value in the stream, derived from (seed, purpose, index) by hash.

COUNTER-BASED, NOT A SEEDED PRNG, AND THE DIFFERENCE IS THE WHOLE POINT. A
`random.Random(seed)` is a STATEFUL sequence: the tenth value depends on how many
draws the nine before it made, so adding one draw anywhere shifts every value after
it, and reproducing record 5,000 means replaying 5,000 records. Here each value is a
pure function of the record's own index and a `purpose` label, so:

  - a record can be derived in isolation, which is what lets a test assert one
    record's fields without generating the stream around it;
  - adding a NEW derived field changes nothing about the existing ones, because it
    draws under its own purpose rather than consuming from a shared stream;
  - the output is independent of iteration order, so a future parallel or chunked
    emitter cannot change a single byte.

AND IT IS PORTABLE, WHICH `random` IS NOT. CPython documents that `random.random()`
reproduces for a given seed across versions; it makes NO such promise for `choice`,
`randrange`, `shuffle` or `sample`, and those are exactly the calls a generator
reaches for. `Random.choice` was reimplemented on `_randbelow` in 3.11. This stream
has to reproduce byte-for-byte across a Windows CPython 3.12 dev box and a Databricks
serverless runtime, so it rests on SHA-256 -- which is specified -- instead of on an
implementation detail that has already moved once.

THE DIGEST IS `opl.vault.hashing.hash_key`, NOT A SECOND ENCODER. That module already
owns this project's one answer to "how do several components become one digest without
two different component lists colliding": each component is length-prefixed and
NULL/empty/whitespace-distinguished before a `||` join. Re-spelling that here as
`sha256(f"{seed}:{purpose}:{index}")` would be a second encoding to keep correct, and
one whose collision argument nobody has made -- `("a:b", "c")` and `("a", "b:c")`
collide under it. What this module adds is the SEMANTICS on top of the digest: which
tuple identifies which value. Note that `hash_key` upper-cases its components, so two
`purpose` labels differing only in case are the SAME purpose; `_assert_purposes_are_
distinct` in `opl.generator.stream` refuses that at import rather than leaving two
fields silently correlated.

MODULO BIAS IS PRESENT, MEASURED, AND ACCEPTED, rather than unmentioned. `draw_below`
reduces a 256-bit integer modulo a bound. That is biased unless the bound divides
2^256, and the bias for a bound of B is at most B / 2^256 in relative terms -- for the
largest bound this generator uses (an amount range under 10^7) that is below 2^-230.
No test that could ever run would distinguish it from uniform. Rejection sampling was
rejected for a concrete reason and not for cost: it makes the number of hash
evaluations depend on the values drawn, so "record N's amount" would stop being a
function of N alone, which is the property the rest of this module exists to provide.
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import TypeVar

from opl.vault.hashing import hash_key

# Prefixed onto every derivation so that a digest from this module can never equal a
# vault business-key digest computed from the same strings. They would have to be very
# unlucky strings, and the cost of removing the question is one component.
NAMESPACE = "opl-payment-generator"

_Item = TypeVar("_Item")


def require_whole(value: int, *, name: str) -> int:
    """`value` as a non-negative `int`, or refuse.

    `bool` is refused explicitly even though it IS an `int` in Python: `seed=True`
    would otherwise derive the same stream as `seed=1` while reading, at the call
    site, as a flag someone passed to the wrong parameter."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int, got {value!r} ({type(value).__name__})")
    if value < 0:
        raise ValueError(
            f"{name} must be zero or greater, got {value} -- a negative one is a "
            "caller that computed an offset wrong, and it would derive a perfectly "
            "plausible-looking record from it"
        )
    return value


def digest(*, seed: int, purpose: str, index: int, salt: int = 0) -> bytes:
    """The 32 raw bytes identifying (seed, purpose, index, salt).

    `salt` exists for ONE caller: the base-event builder re-derives a record's
    business attributes when the tuple it produced has already been produced, so that
    every base event is distinct and the repeat count is exact rather than approximate
    (see `opl.generator.stream`). It defaults to 0, so every other call spells the
    three-component tuple it means.

    Returns bytes rather than the hex `hash_key` yields, because every consumer below
    wants an integer and `int.from_bytes` is the direct route."""
    if not purpose:
        raise ValueError("purpose must be a non-empty label naming the field being derived")
    require_whole(seed, name="seed")
    require_whole(index, name="index")
    require_whole(salt, name="salt")
    return bytes.fromhex(hash_key([NAMESPACE, str(seed), purpose, str(index), str(salt)]))


def draw(*, seed: int, purpose: str, index: int, salt: int = 0) -> int:
    """The digest above as a big-endian unsigned 256-bit integer."""
    return int.from_bytes(digest(seed=seed, purpose=purpose, index=index, salt=salt), "big")


def draw_below(*, seed: int, purpose: str, index: int, bound: int, salt: int = 0) -> int:
    """A value in `range(bound)`. See the module docstring on modulo bias.

    Refuses a bound of zero rather than raising `ZeroDivisionError` several frames
    deep: every call site's bound is a length or a range width, and a zero one means
    the caller was handed an empty pool or an empty value domain -- a message naming
    that is worth more than the interpreter's."""
    if bound <= 0:
        raise ValueError(
            f"bound must be positive, got {bound} -- nothing can be drawn from an "
            f"empty domain (purpose={purpose!r})"
        )
    return draw(seed=seed, purpose=purpose, index=index, salt=salt) % bound


def pick(items: Sequence[_Item], *, seed: int, purpose: str, index: int, salt: int = 0) -> _Item:
    """One element of `items`, chosen by index rather than by identity.

    POSITIONAL, so the choice is a function of the sequence's ORDER. That is stated
    because it has a consequence a caller must accept: reordering
    `opl.contracts.payments.PAYMENT_METHODS` re-draws the method of every payment in
    every stream ever generated. Those tuples are therefore declared in a fixed order
    and never sorted at the point of use.

    Refuses a bare `str` for `hash_key`'s reason: a `str` satisfies `Sequence[str]`
    structurally, so `pick("PIX", ...)` would type-check and return a single
    CHARACTER."""
    if isinstance(items, str):
        raise TypeError(
            f"pick() received a bare str {items!r} -- a str is a Sequence, so this "
            "would choose one CHARACTER; pass a sequence of the values you mean"
        )
    if not items:
        raise ValueError(f"pick() received an empty sequence (purpose={purpose!r})")
    return items[draw_below(seed=seed, purpose=purpose, index=index, bound=len(items), salt=salt)]
