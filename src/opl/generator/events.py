# src/opl/generator/events.py
"""One payment event, and the single crossing from typed values into contract strings.

THIS MODULE IS WHERE "THE GENERATOR KNOWS TYPES" AND "BRONZE STORES STRINGS" MEET, and
it is deliberately the ONLY place they do. `PaymentEvent` below is typed -- integer
milliseconds, integer cents -- because arithmetic on a timestamp or a money amount
must not be done on text. `record_of` returns `dict[str, str]`, and that annotation is
the contract: no `int`, no `float`, no `bool`, no `datetime` and no `None` can leave
this module inside a contract column. A type checker refuses the first, and
`tests/test_payment_generator.py` refuses all of them at run time over a whole stream.

WHY THAT PARTICULAR SEAM, rather than "the writer casts" or "bronze casts". Bronze is
all-string for the CNPJ files because the source ships text and casting early destroys
leading zeros, decimal-comma amounts and future alphanumeric CNPJs. The payment stream
has no such excuse handed to it -- it is generated, so it COULD emit JSON numbers and
let Auto Loader infer types. It does not, for one reason: a JSON number and a JSON
null are two ways for a field to arrive without characters in it, and the moment both
exist, "this bronze cell is NULL" has two possible causes (the field was absent, or it
was present and unparseable) and Task 2's schema drift stops being measurable. With
every contract value a string and no contract value ever empty, a NULL in the payments
bronze table has exactly one cause: the JSON did not carry the field.

NO FLOAT EXISTS ANYWHERE IN THE PATH. An amount is an integer number of centavos and
is rendered by integer division; `12345` becomes `"123.45"` by arithmetic that has no
rounding mode to get wrong. A float would break byte-identity from a seed before it
broke arithmetic: `repr` chooses the shortest string that round-trips, which is a
property of the runtime's float formatting, not of the value.

JSON LINES, `\\n`, AND NEVER `os.linesep`. `to_jsonl` joins on a literal `"\\n"`. On
Windows an `os.linesep` join -- or a text-mode write with default newline translation
-- produces `\\r\\n`, so the same seed would yield different bytes on the dev box and
on Databricks and the byte-identity property would be true only within one OS. This
repository has already paid for that lesson in a file-rewrite probe that "restored"
a file and left it byte-changed. The writer (F1b Task 3) must open in binary or pass
`newline=""` for the same reason; `to_jsonl` returning TEXT rather than bytes keeps
that decision visible at the write, where it belongs.
"""
from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

from opl.contracts.payments import (
    AMOUNT_SCALE,
    BUSINESS_ATTRIBUTE_COLUMNS,
    COLUMNS,
)
from opl.generator.instants import to_text

# 10 ** AMOUNT_SCALE, computed once. The minor units in one major unit: 100 centavos
# in one real. Derived from the contract rather than written as `100`, so a currency
# with a different scale is a contract edit and not a hunt through renderers.
_MINOR_UNITS = 10**AMOUNT_SCALE

# The line separator of the emitted file, as a named constant so the ban on
# `os.linesep` has something to point at. See the module docstring.
LINE_SEPARATOR = "\n"


def format_amount(cents: int) -> str:
    """An integer number of minor units as a fixed-scale decimal string.

    `12345` -> `"123.45"`, `5` -> `"0.05"`, `100` -> `"1.00"`. Always exactly
    `AMOUNT_SCALE` fractional digits, always with a leading integer digit, never in
    exponent notation -- the three ways a float-formatted amount goes wrong.

    Refuses zero and negatives. A payment of nothing is not a payment, and a negative
    one is a refund or a reversal -- a different event with a different meaning, which
    this contract does not model. Emitting one as a payment would put a number in the
    data whose sign carries semantics nothing declares."""
    if isinstance(cents, bool) or not isinstance(cents, int):
        raise TypeError(f"cents must be an int, got {cents!r} ({type(cents).__name__})")
    if cents <= 0:
        raise ValueError(
            f"cents must be positive, got {cents} -- a zero or negative amount is a "
            "reversal or a refund, which is a different event this contract does not "
            "model, not a payment with an unusual sign"
        )
    return f"{cents // _MINOR_UNITS}.{cents % _MINOR_UNITS:0{AMOUNT_SCALE}d}"


@dataclass(frozen=True, kw_only=True)
class PaymentEvent:
    """One payment, in the generator's own typed terms. Frozen: an event is a fact.

    `kw_only`, for `opl.bronze.registry.BronzeTable`'s reason and with a sharper
    example: `payer_cnpj_basico` and `payee_cnpj_basico` are adjacent, both `str`, and
    both eight characters, so a positional construction that swapped them would type-
    check, pass every shape check, and reverse the direction of every payment in the
    stream. Keyword-only makes field order unable to matter.

    `event_time_ms` / `emitted_at_ms` are NOT `datetime`s -- see
    `opl.generator.instants` for why the generator carries integers -- and they are
    named with the unit because a bare `event_time` holding an integer is exactly the
    kind of field someone divides by 1000 in the wrong place."""

    transaction_id: str
    event_time_ms: int
    emitted_at_ms: int
    payer_cnpj_basico: str
    payee_cnpj_basico: str
    amount_cents: int
    currency: str
    payment_method: str


# ONE RENDERER PER CONTRACT COLUMN, keyed by the column name, and checked against
# `COLUMNS` at import by `_assert_every_column_has_a_renderer` below. Written as a
# table rather than as a dict literal inside `record_of`, for a reason that only shows
# up in Task 2: adding a column to the contract and forgetting to render it would
# otherwise produce a stream missing that column, silently, and adding a renderer for
# a column the contract does not declare would put an undeclared key in the JSON --
# where Auto Loader would meet it as schema drift the generator never meant to inject.
# Both directions are refused at import instead.
_RENDERERS: dict[str, Callable[[PaymentEvent], str]] = {
    "transaction_id": lambda event: event.transaction_id,
    "event_time": lambda event: to_text(event.event_time_ms),
    "emitted_at": lambda event: to_text(event.emitted_at_ms),
    "payer_cnpj_basico": lambda event: event.payer_cnpj_basico,
    "payee_cnpj_basico": lambda event: event.payee_cnpj_basico,
    "amount": lambda event: format_amount(event.amount_cents),
    "currency": lambda event: event.currency,
    "payment_method": lambda event: event.payment_method,
}


def _assert_every_column_has_a_renderer() -> None:
    """Fail at import if `_RENDERERS` and `COLUMNS` disagree, in either direction.

    A plain ValueError, at import, beside the declaration: neither half of the
    disagreement fails at run time. A missing renderer drops a column from every
    record, and a surplus one adds a key the contract does not declare -- which the
    bronze DQ gate would read as unexpected drift arriving from a generator that
    believed it was emitting a clean stream."""
    missing = [column for column in COLUMNS if column not in _RENDERERS]
    surplus = [column for column in _RENDERERS if column not in COLUMNS]
    if missing or surplus:
        raise ValueError(
            f"the record renderer and the payments contract disagree: {missing} have "
            f"no renderer, {surplus} render a column the contract does not declare. "
            "The first drops a column from every emitted record; the second injects "
            "schema drift the generator did not mean to inject."
        )


_assert_every_column_has_a_renderer()


def record_of(event: PaymentEvent) -> dict[str, str]:
    """`event` as the contract's string columns, in `COLUMNS` order.

    THE ONE CROSSING from types into text. Key order matters: `json.dumps` writes keys
    in insertion order, so iterating `COLUMNS` here -- rather than `_RENDERERS` -- is
    what makes the contract's declared order the emitted line's order, and the
    byte-identity property is stated over those bytes.

    Every value is produced by a renderer that cannot yield `""`: `to_text` always
    writes 24 characters, `format_amount` refuses non-positive input, and the three
    remaining columns are non-empty strings validated when the event is built. That is
    what makes "no contract column is ever blank" a property of construction rather
    than a rule someone has to remember."""
    return {column: _RENDERERS[column](event) for column in COLUMNS}


def business_attributes_of(record: dict[str, str]) -> tuple[str, ...]:
    """The `BUSINESS_ATTRIBUTE_COLUMNS` of `record`, as a tuple, in declaration order.

    WHAT THE PAYMENT WAS, with the identity and both timestamps left out. Two records
    with equal tuples are the same payment happening twice -- a legitimate repeat --
    unless they also share a `transaction_id`, in which case they are one payment
    delivered twice. Spelled once, here, because the T2 count is taken over exactly
    these columns and a second spelling at a call site is how that count would drift
    from the stream it describes."""
    return tuple(record[column] for column in BUSINESS_ATTRIBUTE_COLUMNS)


def to_jsonl(records: Sequence[dict[str, str]] | Iterable[dict[str, str]]) -> str:
    """`records` as JSON Lines text: one compact object per line, `\\n`-terminated.

    TERMINATED, NOT SEPARATED -- the last line carries a newline too. Both spellings
    parse, and terminating is what makes concatenating two generated chunks produce a
    valid file rather than one fused line; the emitter is going to write more than one
    file per stream.

    `separators=(",", ":")` removes the space `json.dumps` puts after each separator
    by default. Not for size: for byte-identity that is decided HERE rather than by a
    default that has changed shape between Python versions before.

    `ensure_ascii=False` because the file is UTF-8 and a future column carrying a
    razão social should arrive as its own characters rather than as `\\uXXXX` escapes.
    Deterministic either way -- the escaping rule is fixed -- so this is a
    readability choice, not a reproducibility one."""
    return "".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")) + LINE_SEPARATOR
        for record in records
    )
