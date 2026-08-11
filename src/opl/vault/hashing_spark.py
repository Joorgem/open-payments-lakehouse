# src/opl/vault/hashing_spark.py
"""The business-key hash standard as a Spark expression: the SAME encoding as
`opl.vault.hashing`, evaluated by Catalyst instead of by CPython.

THE DECISION THIS MODULE IS, AND THE ALTERNATIVE IT REJECTS.

`hash_key` is pure Python and its correctness is pinned by TEN hard-coded digests
(nine when this paragraph was written at Task 3; the accented one arrived with
`test_a_pinned_digest_for_an_accented_component` in the same task's fix round).
This layer has to key 69,062,849 companies per month, and 144M establishment rows
behind them. Those two facts pull in opposite directions and there were two ways out:

  1. A PYTHON UDF WRAPPING `hash_key`. One source of truth: any change to the standard
     is caught by the pinned digests, because the digests exercise the very code the
     vault runs. Rejected, on two counts and not only the obvious one.
     - COST. A Python UDF serialises every row out of the JVM, through a Python
       worker, and back. It also blocks whole-stage codegen and Photon, so the loss is
       not the marshalling alone -- the surrounding plan gets slower too. At 69M rows
       per table per month, on a Free Edition workspace, that is the difference between
       a load and a load that does not finish. `opl.bronze.snapshot` already made this
       call for a much cheaper derivation ("a UDF would break Catalyst over 144M
       rows") and this is the same call on twenty times the work.
     - AVAILABILITY. `opl.vault.hashing` is deliberately importable where pyspark is
       not installed, because the extraction scripts run off-Databricks. A UDF is the
       one shape that cannot be tested without a session, which puts the standard's
       ten pinned digests behind a Spark dependency they currently do not have.
  2. A SPARK-NATIVE EXPRESSION -- `sha2` over a `concat_ws` of the same encoding.
     CHOSEN, and it is a SECOND SPELLING of the standard, which is the exact defect
     this repository has already been bitten by: `opl.config` records how two spellings
     of one month rule left `2026-13` refused at two of four entry points. A drift here
     is worse than that one -- it re-keys the vault, silently, and no pinned digest
     would see it because the pinned digests exercise the Python spelling.

SO THE SECOND SPELLING IS PAID FOR, IN TWO INSTALMENTS, and neither is optional.

FIRST, THE LITERALS ARE IMPORTED, NOT RE-TYPED. `DELIMITER`, `NULL_TOKEN`,
`EMPTY_TOKEN`, `WHITESPACE_TOKEN` and `_STRING_TAG` all come from `opl.vault.hashing`.
`_STRING_TAG` is private and imported across a module boundary anyway, exactly as
`opl.bronze.registry` imports the `_assert_*` guards from `registry_collisions`, and
for the same reason: the underscore says "not part of the public surface", which stays
true -- this module is the standard's other half, not a consumer of it. What remains
duplicated is the SHAPE of the expression (the branch order, the join, the length
prefix), which is what the second instalment covers.

SECOND, AN EQUIVALENCE TEST IS MANDATORY AND IS THE ONLY THING THIS MODULE'S TESTS
ASSERT. `tests/vault/test_hashing_spark.py` pins no digest of its own: it evaluates
this expression and `hash_key` over the same inputs and requires them equal. It has
two halves, and the second exists because the first is not enough.

  - A CURATED CORPUS, over every ordered PAIR of it, covering NULL, `""`,
    whitespace-only, a component containing `||`, one containing `:`, one that already
    looks like an encoded segment, the token spellings `N`/`E`/`W`, accented text, an
    upper-casing that changes length, and the masked-CPF shape `***NNNNNN**`. This is
    the half that covers the JOIN, which a single-component test cannot reach at all.
  - A SWEEP OF EVERY CASED CHARACTER IN UNICODE, single-component, ~1,525 rows. A
    curated list STRUCTURALLY CANNOT catch a case-table difference -- see the version
    skew below, which the F2 Task 3 review found and the corpus above had missed
    entirely. A hand-picked corpus only ever fails for a reason someone anticipated.

Without both, this module is an unguarded re-keying waiting to happen.

THE FIRST PLACE THE TWO SPELLINGS CANNOT SIMPLY AGREE: TRIM. Python's `str.strip()`
removes 29 different characters -- tab, NBSP, the Unicode spaces, and four ASCII
separators. Spark SQL's `trim(str)` removes ASCII SPACE (U+0020) and nothing else.
Bronze is parsed from cp1252 RFB CSVs where a NBSP inside a razão social is an ordinary
thing, so `trim` would encode `"\\xa0"` as `W` in Python and as `S1:\\xa0` in Spark: two
digests, one business key, forever. `TRIMMED_CHARACTERS` below names Python's set
explicitly and `_TRIM_PATTERN` is BUILT from it, so the regex cannot drift from the
tuple -- and `test_the_trim_class_names_exactly_the_characters_python_strips` asserts
the tuple against `str.isspace()` over all 1,114,112 code points, in both directions,
so the tuple cannot drift from Python.

THE SECOND PLACE, AND IT IS NOT CLOSED, ONLY PINNED: THE UNICODE VERSION SKEW.
`F.upper` bottoms out in Java's `String.toUpperCase`, whose case table is the JDK's
UNICODE VERSION -- JDK 17 ships Unicode 13.0. `str.upper()` uses CPython's, which for
3.12 is Unicode 15.0. **Neither is pinned anywhere in this repository.** Measured over
every cased character (java.version 17.0.19, CPython 3.12.13): the two spellings
produce different digests for exactly FORTY characters, all of which gained a case
mapping in Unicode 14.0 -- U+2C5F, U+A7C1, U+A7D1, U+A7D7, U+A7D9, and the
U+10597-U+105BC span MINUS U+105A2, U+105B2 and U+105BA, which do not diverge. (The
three exclusions were missing from this sentence until Task 7's correction pass, which
made the enumeration beside "exactly FORTY" name forty-three. `UNICODE_VERSION_DIVERGENCE`
in `tests/vault/test_hashing_spark.py` is the authority; this prose is a reading of it
and the test is what runs.) A DBR upgrade onto Java 21 would re-key any vault row
containing one.

WHAT IS DONE ABOUT IT, since it cannot be fixed from here. The forty are pinned as an
EQUALITY in `test_the_two_spellings_upper_case_every_cased_character_the_same_way`, so
a JDK bump in either direction -- introducing new divergences, or removing these --
turns the suite red rather than re-keying the vault quietly. No CNPJ bronze row can
currently hold one: the CSV dialect is cp1252 and none of the forty is encodable in
it, which `test_no_character_the_two_spellings_disagree_about_can_reach_cnpj_bronze`
asserts against the imported dialect rather than a restated encoding name. A wave-2
feed that is not cp1252-bound reaches them immediately, and should read this paragraph
first.

THE GAPS THAT REMAIN, STATED RATHER THAN LEFT TO BE FOUND:

  - `UTF8String.toUpperCase` falls back for non-ASCII input to Java's
    `String.toUpperCase()` in the DEFAULT LOCALE, where Python's `str.upper()` is
    locale-independent. The two disagree in a Turkish or Azerbaijani locale (dotted and
    dotless i). The cased sweep contains `i`, so that divergence turns the suite red on
    such a machine rather than re-keying silently -- which is the behaviour we want
    from a gap we cannot close from here. THE CURATED CORPUS DOES NOT CONTAIN `i` AND
    NEVER DID: this paragraph credited it with that safety before the Task 3 review,
    and the sweep is what makes the claim true.
  - `NULL` is passed through by `zero_padded_column` where `zero_pad_cnpj` would raise
    on a value with no `len()`. A Spark expression cannot refuse one row without
    failing the query, and a NULL business key is already refused upstream by bronze's
    `cnpj_basico` NOT NULL constraint.
  - Neither spelling applies a Unicode normal form, so NFC and NFD spellings of the
    same accented name still produce two digests. That is `opl.vault.hashing`'s stated
    gap and this module inherits it unchanged, which is the correct outcome: the two
    spellings agree, including about what they do not do."""
from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from opl.vault.hashing import (
    _STRING_TAG,
    DELIMITER,
    EMPTY_TOKEN,
    NULL_TOKEN,
    WHITESPACE_TOKEN,
)

# Exactly the characters `str.strip()` removes, as of Python 3.12: the ASCII
# whitespace and separators, NBSP and NEL, and the Unicode space separators. Written
# out rather than derived at import because deriving it means scanning 1,114,112 code
# points on every import of the vault; `test_the_trim_class_names_exactly_the_
# characters_python_strips` does that scan once, in the suite, and asserts this tuple
# equals it in both directions -- so a future Python that changed the set turns a test
# red instead of splitting the vault's keys.
TRIMMED_CHARACTERS: tuple[str, ...] = tuple(
    chr(code)
    for code in (
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
        0x85, 0xA0, 0x1680,
        *range(0x2000, 0x200B),
        0x2028, 0x2029, 0x202F, 0x205F, 0x3000,
    )
)

# A Java character class of the above, spelled as `\uXXXX` escapes so no control
# character ends up inside a pattern string, and so the class is independent of the
# JDK's own Unicode version -- `\s` under `(?U)` would be Java's idea of white space,
# which omits U+001C-U+001F and is therefore NOT Python's.
_TRIM_CLASS = "[" + "".join(rf"\u{ord(character):04X}" for character in TRIMMED_CHARACTERS) + "]"

# Leading run or trailing run. `regexp_replace` replaces every match, so one pattern
# does both ends; a string that is entirely whitespace is consumed by the first
# alternative and leaves nothing for the second to match.
_TRIM_PATTERN = f"^{_TRIM_CLASS}+|{_TRIM_CLASS}+$"


def _normalised(component: Column) -> Column:
    """`component.strip().upper()`, in Catalyst.

    STRIP BEFORE UPPER, matching `_encode_component`'s order rather than the other
    one. No character this vault carries upper-cases into whitespace, so the two
    orders agree on every real input -- which is exactly why the order is worth
    matching deliberately instead of relying on that."""
    return F.upper(F.regexp_replace(component, _TRIM_PATTERN, ""))


def _encoded(component: Column) -> Column:
    """One component, encoded `N` / `E` / `W` / `S<len>:<norm>`.

    The four branches in `_encode_component`'s order, and the order is significant
    for the same reason it is there: `isNull` before `== ""` before "normalises to
    empty" before everything else. The three degenerate branches must not collapse
    into one another -- an empty string and a whitespace-only string are distinct
    defensive cases, and NULL is distinct from both.

    The length prefix is taken from the NORMALISED string, as it is in Python: taking
    it from the raw one would let two components with different padding and the same
    trimmed content encode to different byte counts mid-string, which breaks the
    unambiguous-decoding argument the whole format rests on."""
    normalised = _normalised(component)
    return (
        F.when(component.isNull(), F.lit(NULL_TOKEN))
        .when(component == F.lit(""), F.lit(EMPTY_TOKEN))
        .when(normalised == F.lit(""), F.lit(WHITESPACE_TOKEN))
        .otherwise(
            F.concat(
                F.lit(_STRING_TAG),
                F.length(normalised).cast("string"),
                F.lit(":"),
                normalised,
            )
        )
    )


def hash_key_column(components: Sequence[Column]) -> Column:
    """The business-key hash as a Column: SHA-256 over the `||`-joined encoding.

    Returns the lower-case hex digest `sha2` produces, which is the form
    `hashlib.sha256(...).hexdigest()` produces too -- the two are comparable without
    a case fold, and the equivalence test compares them literally.

    `concat_ws` RATHER THAN `concat` with interleaved literals, and the difference
    matters: `concat` propagates NULL, so one NULL component would make the whole
    digest NULL. `concat_ws` skips NULLs instead -- which would be worse, silently
    joining the remaining components as if the NULL one were not there -- and neither
    behaviour is reachable, because `_encoded` returns a non-NULL literal on every
    branch including the NULL one. That is the property that makes either function
    safe here, and it is the reason the NULL branch is a TOKEN rather than a passthrough.

    REFUSES A BARE `Column`, and this failure is worse than the bare-`str` one
    `hash_key` refuses. `Column.__getitem__` answers any index -- it is how struct and
    array access is spelled -- so iterating a bare `Column` yields `col[0]`, `col[1]`,
    ... and never raises `StopIteration`. `hash_key_column(F.col("cnpj_basico"))` does
    not return a wrong digest; it hangs.

    REFUSES AN EMPTY `components`, for `hash_key`'s reason: `concat_ws('||')` over no
    arguments is the empty string, so every caller who built its column list wrong
    would silently share `sha2('', 256)`."""
    if isinstance(components, Column):
        raise TypeError(
            "hash_key_column() received a bare Column -- a Column answers any index, "
            "so iterating it never terminates and this call would hang rather than "
            "fail; pass a one-element list instead, e.g. hash_key_column([column])"
        )
    if not components:
        raise ValueError(
            "hash_key_column() received zero components -- a business key must have "
            "at least one, or every caller that passes an empty list would silently "
            "share the digest of the empty string"
        )
    return F.sha2(F.concat_ws(DELIMITER, *(_encoded(column) for column in components)), 256)


def zero_padded_column(column: Column, *, width: int) -> Column:
    """`zero_pad_cnpj`'s Spark twin: left-pad to `width` with `"0"`, FAILING THE QUERY
    rather than truncating an overlong value.

    `F.lpad` alone is the trap this exists for. It truncates:
    `lpad('100000012', 8, '0')` is `'10000001'`, another company's real key -- so an
    overlong value would not produce a bad row, it would merge two different companies
    onto one hub hash key and interleave their two histories in one satellite. That is
    the worst failure this layer can produce and it comes with no error attached.

    THE REFUSAL IS A QUERY FAILURE BECAUSE A COLUMN EXPRESSION HAS NO OTHER MOVE.
    `zero_pad_cnpj` raises on the one value; `raise_error` inside a `when` fails the
    whole load. That asymmetry is deliberate and is the right way round: an aborted
    load is re-runnable and a silently merged pair of companies is not. The branch is
    only evaluated when the condition holds -- `CaseWhen` generates a real `if` -- so
    the cost on every ordinary row is one length comparison.

    NULL PASSES THROUGH, where `zero_pad_cnpj(None)` would raise on a value with no
    `len()`. `length(NULL)` is NULL, the comparison is NULL, `lpad(NULL, ...)` is NULL,
    and `_encoded` maps that to the NULL token -- a legitimate if degenerate key.
    Bronze's `cnpj_basico` NOT NULL constraint means no such row reaches here."""
    if width <= 0:
        raise ValueError(f"width must be a positive integer, got {width!r}")
    return (
        F.when(
            F.length(column) > F.lit(width),
            F.raise_error(
                F.concat(
                    F.lit(
                        f"refusing to truncate a business-key component to {width} "
                        "characters: a truncated value collides with another CNPJ's "
                        "true prefix and merges two companies onto one hash key. The "
                        "offending value was "
                    ),
                    column,
                )
            ).cast("string")
        )
        .otherwise(F.lpad(column, width, "0"))
    )


def refuse_non_string_columns(frame: DataFrame, columns: Sequence[str]) -> None:
    """Refuse before hashing if any named column is missing or is not a STRING.

    AT THE BOUNDARY, because this is the divergence no equivalence test over string
    corpora can see. Every column the CNPJ contracts declare is STRING and that is a
    deliberate decision -- it includes `capital_social`, and alphanumeric CNPJs take
    effect 2026-07-31 -- but nothing stops a caller pointing a loader at a table where
    one column has been typed. Spark would silently cast it and hash the cast;
    `hash_key` would raise `AttributeError` on a value with no `.strip()`. One spelling
    answers and the other refuses, so the digests are not merely different, they are
    not comparable at all.

    A `TypeError`, matching `hash_key`'s refusal of a bare `str`: the argument is of
    the wrong type, not out of range. A missing column is a `ValueError`, matching
    `ObservationGrain`'s equivalent -- the caller named something that is not there."""
    missing = [name for name in columns if name not in frame.columns]
    if missing:
        raise ValueError(
            f"{missing} are not columns of the source, which has "
            f"{sorted(frame.columns)}. A hash over a column that is not there cannot "
            "be spelled, and a spec naming one is a spec pointed at the wrong table"
        )
    typed = {
        name: frame.schema[name].dataType.simpleString()
        for name in columns
        if not isinstance(frame.schema[name].dataType, StringType)
    }
    if typed:
        raise TypeError(
            f"the business-key hash standard is defined over STRING columns and "
            f"{typed} are not. Spark would cast them silently and hash the cast, "
            "where opl.vault.hashing.hash_key raises on a value with no .strip() -- "
            "so the two spellings of the standard would not merely disagree, they "
            "would not be comparable. Every CNPJ contract column is STRING on "
            "purpose; cast at the boundary if a source really is typed, and decide "
            "the cast's format deliberately"
        )
