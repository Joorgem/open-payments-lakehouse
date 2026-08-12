# src/opl/generator/instants.py
"""Time, as this generator carries it: an integer number of milliseconds since the
Unix epoch, rendered into exactly one text form.

NO WALL CLOCK EXISTS IN THIS MODULE, and that is the constraint the whole phase turns
on. There is no `now()`, no `utcnow()`, no `time.time()`, and nothing that could
acquire one: an instant enters through `from_text` (a value someone WROTE DOWN, in a
stream spec that is part of the seed) and leaves through `to_text`. A record's
`event_time` is the window's start plus a whole number of intervals; its `emitted_at`
is derived from its position in the emission sequence. Both are therefore functions of
the spec, and re-running the generator a year later produces the same characters.

INTEGER MILLISECONDS, NOT `datetime`, NOT `float` SECONDS, INSIDE THE GENERATOR. A
`float` epoch second cannot represent every millisecond of 2026 exactly, and its
`repr` is a platform-and-version-dependent shortest-round-trip string -- so an amount
of arithmetic that looks exact would move the last digit of a timestamp between
Windows and Databricks and break byte-identity, which is the one property that must
not be fragile. `datetime` arithmetic is exact but drags a `tzinfo` object through
every comparison and offers `now()` to anyone who imports it. Integers do neither.

ONE TEXT FORM, `YYYY-MM-DDTHH:MM:SS.mmmZ`, AND EVERY OTHER SPELLING IS REFUSED. Not
because the others are wrong as ISO-8601 -- `+00:00`, a missing fractional part, a
`+00:00` offset with six fractional digits are all legal -- but because `to_text` must
be the exact inverse of `from_text` for the round trip to be assertable, and an input
grammar wider than the output grammar makes that false the moment a caller writes a
legal variant. `Z` rather than `+00:00`: it is the shorter of the two, it is what
Auto Loader's JSON timestamp inference accepts, and it cannot be confused with a local
offset that happens to be zero today.

MILLISECONDS AND NOT MICROSECONDS, stated because the choice bounds what Task 2 can
express. Three fractional digits is the resolution of the emission sequence, so two
records cannot be emitted within the same millisecond and still be ordered by
`emitted_at`. The generator advances by whole intervals well above 1 ms, so this is a
bound nothing currently approaches; a spec that violated it is refused in
`opl.generator.stream` rather than silently producing ties.
"""
from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

MILLISECONDS_PER_SECOND = 1_000
MILLISECONDS_PER_MINUTE = 60 * MILLISECONDS_PER_SECOND
MILLISECONDS_PER_HOUR = 60 * MILLISECONDS_PER_MINUTE
MILLISECONDS_PER_DAY = 24 * MILLISECONDS_PER_HOUR

# The one anchor. UTC explicitly rather than a naive datetime: a naive one would make
# `strftime` and any comparison depend on the machine's zone, which is exactly the
# ambient input this module exists to have none of.
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# The output grammar, used to REFUSE input that does not match it. Anchored, and each
# field's digit count exact -- `\d{3}` for the fraction, so a two- or six-digit
# fraction is refused rather than quietly re-rendered as three. `[0-9]` rather than
# `\d` for `cnpj_schemas`' reason: `\d` admits non-ASCII decimal digits, which no
# timestamp this project writes contains and which `strptime` would then accept.
_TEXT = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$"
)
_STRPTIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def to_text(milliseconds: int) -> str:
    """`milliseconds` since the epoch as `YYYY-MM-DDTHH:MM:SS.mmmZ`.

    The fractional part is formatted from the integer remainder rather than from
    `strftime("%f")` -- `%f` renders SIX digits and would have to be truncated, and a
    truncation is a rounding decision made in a format string where nobody would look
    for one.

    Refuses a value before the epoch. Nothing this generator emits predates 1970, and
    allowing negatives would mean deciding what `-1 % 1000` should render as -- a
    question with a defensible answer and no caller."""
    if isinstance(milliseconds, bool) or not isinstance(milliseconds, int):
        raise TypeError(
            f"milliseconds must be an int, got {milliseconds!r} "
            f"({type(milliseconds).__name__}) -- a float cannot represent every "
            "millisecond exactly and its repr is platform-dependent"
        )
    if milliseconds < 0:
        raise ValueError(
            f"milliseconds must be zero or greater, got {milliseconds} -- an instant "
            "before 1970-01-01T00:00:00Z is a caller that subtracted a window from an "
            "offset in the wrong order"
        )
    moment = EPOCH + timedelta(milliseconds=milliseconds)
    return f"{moment.strftime('%Y-%m-%dT%H:%M:%S')}.{milliseconds % MILLISECONDS_PER_SECOND:03d}Z"


def from_text(text: str) -> int:
    """The inverse of `to_text`: milliseconds since the epoch, or refuse.

    THE SHAPE IS CHECKED BEFORE `strptime`, AND THAT ORDER IS LOAD-BEARING. `%f`
    accepts one to six fractional digits, so `strptime` alone would happily read
    `...T00:00:00.5Z` as 500 ms and `...T00:00:00.500000Z` as the same value -- and
    `to_text` renders only one of those, so the round trip would break for input the
    parser had already accepted. The regex makes the accepted grammar exactly the
    emitted one.

    Refuses a naive or offset-bearing spelling by the same mechanism: `+00:00` does
    not match, so a caller cannot introduce a second spelling of UTC that renders back
    differently."""
    if not isinstance(text, str) or not _TEXT.match(text):
        raise ValueError(
            f"{text!r} is not an instant. The one accepted form is "
            "YYYY-MM-DDTHH:MM:SS.mmmZ -- exactly three fractional digits and a "
            "literal Z, because `to_text` emits exactly that and the two have to be "
            "inverses. '+00:00', a missing fraction and a six-digit fraction are all "
            "legal ISO-8601 and all refused here for that reason."
        )
    moment = datetime.strptime(text, _STRPTIME_FORMAT).replace(tzinfo=UTC)
    elapsed = moment - EPOCH
    if elapsed.days < 0:
        raise ValueError(f"{text!r} is before the epoch; this generator emits no such instant")
    return (
        elapsed.days * MILLISECONDS_PER_DAY
        + elapsed.seconds * MILLISECONDS_PER_SECOND
        + elapsed.microseconds // 1_000
    )
