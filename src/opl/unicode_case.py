# src/opl/unicode_case.py
"""The characters JDK 17 and CPython 3.12 UPPER-CASE DIFFERENTLY, pinned as data.

WHY THIS IS A MODULE AND NOT A CONSTANT IN ONE OF THE TWO PLACES THAT NEEDS IT. The set is
a fact about a RUNTIME PAIR -- Java's case table against CPython's -- and two layers act on
it. `opl.vault.hashing_spark` has a second spelling of the business-key hash whose Spark
half calls `F.upper` (Java's table) and whose Python half calls `str.upper()` (CPython's),
so a character in this set produces two digests for one business key. `opl.bronze.rules`
has to REFUSE such a character at the gate, before it reaches a `hash_diff`. Bronze may not
import the vault -- that is a layer inversion -- and `src/` may not import from `tests/`,
where this set was first measured. So it lives above both, beside `opl.spark` and
`opl._version_check`, which are the other two modules describing the runtime rather than
the data.

MEASURED, NOT ENUMERATED BY HAND. `tests/vault/test_hashing_spark.py` sweeps every one of
the ~1,525 cased characters in Unicode through both spellings and asserts the diverging set
is EXACTLY this one -- an equality, in both directions, which is the whole safety property:
a JDK bump onto Java 21 (Unicode 15) makes these forty AGREE, and agreeing changes their
digests just as disagreeing does. Either direction re-keys any vault row containing one, so
either must be a decision rather than a surprise. A curated list could not have found these:
they are a VERSION skew, and nothing about them is a shape a human would think to include.

THE VERSIONS THE MEASUREMENT WAS TAKEN UNDER: java.version 17.0.19 (Unicode 13.0, and
`.github/workflows/ci.yml` pins temurin 17, the same table) against CPython 3.12.13 (Unicode
15.0). All forty gained a case mapping in Unicode 14.0.

HOW THE FORTY SPLIT, DERIVED FROM THE SET BELOW RATHER THAN COUNTED BY EYE: five are in
the BMP (U+2C5F, U+A7C1, U+A7D1, U+A7D7, U+A7D9) and THIRTY-FIVE are astral, being the
four ranges below at 11 + 15 + 7 + 2. This module and two others quoted "twenty-nine" --
one wrong number typed once and then copied twice, which is what happens to an arithmetic
claim that lives only in prose. It is now asserted in
`tests/bronze/test_merchant_rules.py::test_the_astral_count_the_docstrings_quote_is_
DERIVED_from_the_set`, so the next edit to the set moves the number or goes red.

THE THREE EXCLUSIONS INSIDE THE SPAN ARE THE PART THAT CANNOT BE WRITTEN FROM MEMORY.
U+10597-U+105BC is a 38-character range and three of its members -- U+105A2, U+105B2 and
U+105BA -- do NOT diverge. Prose that says "the U+10597-U+105BC span" without them names
forty-three, which is what `hashing_spark`'s docstring did until it was corrected. The
ranges below are written to exclude them, and the sweep is what proves the exclusion.
"""
from __future__ import annotations

UNICODE_VERSION_DIVERGENCE: frozenset[int] = frozenset(
    {0x2C5F, 0xA7C1, 0xA7D1, 0xA7D7, 0xA7D9}
    | set(range(0x10597, 0x105A2))
    | set(range(0x105A3, 0x105B2))
    | set(range(0x105B3, 0x105BA))
    | {0x105BB, 0x105BC}
)


def _class_body(code_points: frozenset[int]) -> str:
    """`code_points` as the inside of a regex character class, ranges collapsed.

    `\\x{...}` RATHER THAN THE LITERAL CHARACTERS, and that is the whole reason this is
    built instead of typed. THIRTY-FIVE of the forty are ASTRAL (above U+FFFF): written
    literally into a pattern they are a SURROGATE PAIR in the pattern string, and a class
    containing a bare surrogate pair is not a class containing that character. Java's
    `\\x{h...h}` names a code point directly, so the class means the same thing whatever
    the pattern string's own encoding does."""
    ordered = sorted(code_points)
    spans: list[tuple[int, int]] = []
    for point in ordered:
        if spans and point == spans[-1][1] + 1:
            spans[-1] = (spans[-1][0], point)
        else:
            spans.append((point, point))
    return "".join(
        f"\\x{{{low:X}}}" if low == high else f"\\x{{{low:X}}}-\\x{{{high:X}}}"
        for low, high in spans
    )


# A JAVA-REGEX CHARACTER CLASS OVER THE SET ABOVE, for `F.col(...).rlike(...)`. DERIVED
# from the pinned set rather than typed beside it, so the pattern cannot come to name a
# different set than the sweep measures -- which is the drift a second spelling always has,
# and this module exists because of a second spelling.
DIVERGENT_CHARACTER_CLASS = f"[{_class_body(UNICODE_VERSION_DIVERGENCE)}]"
