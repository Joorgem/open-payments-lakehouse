"""The Spark spelling of the business-key hash standard, tested ONLY as an
equivalence against the pure-Python one.

WHY THAT IS THE WHOLE SHAPE OF THIS FILE. `opl.vault.hashing` is the standard, and
its nine pinned digests are what make a change to it deliberate. `hashing_spark` is
a SECOND SPELLING of that standard -- the thing `opl.config`'s month rule was bitten
by, where `2026-13` ended up refused at two of four entry points because one rule
had two implementations. A second spelling of the HASH is worse: a drift re-keys the
vault, and no pinned digest would see it, because the pinned digests exercise the
Python one.

So there are no digests pinned here and no behaviour asserted here in its own right.
Every test below asks the same question -- does the Spark expression return exactly
what `hash_key` returns -- over inputs chosen so that a divergence has nowhere to
hide. If this file is green, the second spelling is not a second standard.

THE ADVERSARIAL SHAPES, and why each one is in the corpus:

| shape | what it would catch |
|---|---|
| `None` | the NULL token, which is selected by TYPE in Python and by `isNull` here |
| `""` | `E`, which must not collapse onto `W` |
| whitespace-only | `W`, and the trim definition -- see the two whitespace tests |
| `"  padded  "` | that trim collapses incidental padding, in both spellings |
| `"a||b"` | the delimiter inside a component: the length prefix is the defence |
| `"a:b"` | the length prefix's own separator inside a component |
| `"S3:ABC"` | a component that LOOKS like an encoded segment already |
| `"N"`, `"E"`, `"W"` | a component that looks like a token: must encode `S1:N` etc. |
| `"JOSÉ"` | a non-ASCII character: Spark's `upper` takes a different code path |
| `"ß"`, `"ﬁ"` | upper-casing that CHANGES LENGTH, so the length prefix moves |
| `"***111111**"` | the masked-CPF shape, which is a real business key in socios |
| `"\\t"`, `"\\xa0"` | characters Python calls whitespace and Spark's own `trim` does not |
"""
from __future__ import annotations

import sys
import unicodedata

import pytest
from pyspark.sql import functions as F

from opl.contracts.cnpj_schemas import CSV_DIALECT
from opl.vault.hashing import hash_key, zero_pad_cnpj
from opl.vault.hashing_spark import (
    TRIMMED_CHARACTERS,
    hash_key_column,
    refuse_non_string_columns,
    zero_padded_column,
)

# Every shape the standard's four branches distinguish, plus the ones the length
# prefix exists for, plus the two real business-key shapes this vault carries.
CORPUS = (
    None, "", " ", "   ", "\t", "\n", "\r\n", "\xa0", "　", " ",
    "abc", "ABC", "  padded  ", "\tabc\t",
    "a||b", "||", "|", "a:b", ":", "S3:ABC", "S1:N",
    "N", "E", "W", "n", "e", "w",
    "JOSÉ", "josé", "JOSé", "ß", "ﬁ", "ç",
    "***111111**", "12345678", "0", "00000001",
)

# EVERY CHARACTER WHOSE UPPER CASE DIFFERS FROM ITSELF, swept out of the whole code
# space rather than hand-picked. This is the Task 3 review's Critical finding and its
# repair: a curated corpus structurally CANNOT detect a case-table difference between
# the two spellings, because a case table is not a shape anyone thinks to write down.
# The corpus above caught the trim divergence only because trim was tested
# deliberately. 1,525 rows, about a second of Spark.
#
# Surrogates are excluded because they are not characters: `chr(0xD800)` cannot be
# encoded to UTF-8 at all, so neither spelling can be handed one.
CASED_CHARACTERS = tuple(
    chr(code)
    for code in range(0x110000)
    if not (0xD800 <= code <= 0xDFFF) and chr(code).upper() != chr(code)
)

# THE FORTY CHARACTERS THE TWO SPELLINGS DISAGREE ABOUT TODAY, pinned exactly.
#
# THE CAUSE IS A UNICODE VERSION SKEW AND NEITHER SIDE IS PINNED ANYWHERE. `F.upper`
# bottoms out in Java's `String.toUpperCase`, whose case table is the JDK's Unicode
# version -- JDK 17 ships Unicode 13.0. `str.upper()` uses CPython's, which for 3.12
# is Unicode 15.0. All forty below gained a case mapping in Unicode 14.0, so Java
# leaves them unchanged and Python upper-cases them, and the digests differ. Measured
# on java.version 17.0.19 against CPython 3.12.13 / unicodedata 15.0.0; CI pins
# `temurin` `17` (`.github/workflows/ci.yml`), which is the same case table.
#
# ASSERTED AS AN EQUALITY, NOT AS AN ALLOW-LIST, and that is deliberate. A JDK bump
# onto Java 21 (Unicode 15) would make these forty AGREE -- which changes their
# digests, which re-keys any vault row containing one. A change in EITHER direction is
# a re-keying and must be a decision, so both turn this test red. That is the whole
# safety property: the alternative is a DBR upgrade silently re-keying the vault.
UNICODE_VERSION_DIVERGENCE = frozenset(
    {0x2C5F, 0xA7C1, 0xA7D1, 0xA7D7, 0xA7D9}
    | set(range(0x10597, 0x105A2))
    | set(range(0x105A3, 0x105B2))
    | set(range(0x105B3, 0x105BA))
    | {0x105BB, 0x105BC}
)


def _spark_digests(spark, rows: list[tuple], schema: str, columns: list[str]) -> list[str]:
    """`hash_key_column` evaluated over `rows`, returned in the rows' own order.

    An explicit `ordinal` column rather than trusting `collect()` to preserve input
    order: a one-partition local relation happens to, and a test that quietly
    depends on that would start comparing the wrong pairs the day the fixture grows
    a shuffle. Zipping two lists that are silently misaligned is exactly the failure
    an equivalence test must not have."""
    ordered = [(index, *row) for index, row in enumerate(rows)]
    frame = spark.createDataFrame(ordered, f"ordinal int, {schema}")
    collected = (
        frame.withColumn("digest", hash_key_column([F.col(name) for name in columns]))
        .select("ordinal", "digest")
        .collect()
    )
    return [row["digest"] for row in sorted(collected, key=lambda row: row["ordinal"])]


def test_the_spark_spelling_agrees_with_the_python_standard_on_one_component(spark):
    """THE MANDATORY EQUIVALENCE TEST, single-component half.

    One component is where every encoding branch is reachable in isolation: `N`,
    `E`, `W` and `S<len>:<norm>` each own a disjoint set of the corpus, so a
    divergence in any ONE branch shows up here rather than being masked by a
    neighbour's contribution to a joined string."""
    rows = [(value,) for value in CORPUS]

    digests = _spark_digests(spark, rows, "a string", ["a"])

    assert digests == [hash_key([value]) for value in CORPUS]


def test_the_spark_spelling_agrees_with_the_python_standard_on_every_pair(spark):
    """THE MANDATORY EQUIVALENCE TEST, multi-component half -- every ordered pair
    of the corpus against itself.

    Pairs, not just singles, because the join is half the standard: the
    length-prefix argument is about two different component LISTS never producing
    the same encoded string, and a single-component test cannot exercise a join at
    all. `["a||b", "c"]` and `["a", "b||c"]` are both in here, and they are the pair
    the whole format was chosen for."""
    rows = [(left, right) for left in CORPUS for right in CORPUS]

    digests = _spark_digests(spark, rows, "a string, b string", ["a", "b"])

    assert digests == [hash_key([left, right]) for left, right in rows]


def test_the_two_spellings_upper_case_every_cased_character_the_same_way(spark):
    """THE CASE-TABLE SWEEP. Every one of the 1,525 characters whose upper case
    differs from itself, hashed by both spellings, with the currently-divergent set
    pinned exactly.

    WHY A SWEEP AND NOT MORE CORPUS ENTRIES. The Task 3 review found forty characters
    the two spellings disagree about, and the curated corpus above could not have
    caught any of them at any size a human would write: they are a Unicode VERSION
    skew between the JDK's case table and CPython's, and nothing about them is a
    "shape" you would think to include. A sweep is the only form of this test that
    can fail for a reason nobody anticipated, which is the only reason to have it.

    WHAT THE PIN BUYS. `UNICODE_VERSION_DIVERGENCE` is asserted as an EQUALITY, so
    both a JDK bump that introduces new divergences and one that removes these forty
    turn this red -- because both re-key any vault row containing an affected
    character, and a re-keying must be a decision. See the constant for the measured
    versions.

    IT ALSO REPAIRS A CLAIM THE MODULE DOCSTRING WAS MAKING FALSELY. That docstring
    said a locale divergence in `String.toUpperCase` -- the Turkish dotted/dotless i --
    "would make the equivalence test go red on such a machine". The curated corpus
    contains no `i` at all, so it would not have. This sweep does contain `i`
    (`'i'.upper() != 'i'`), so the claim is now true, and it is true because of this
    test rather than in spite of it."""
    rows = [(character,) for character in CASED_CHARACTERS]

    digests = _spark_digests(spark, rows, "a string", ["a"])
    diverged = frozenset(
        ord(character)
        for character, digest in zip(CASED_CHARACTERS, digests, strict=True)
        if digest != hash_key([character])
    )

    assert diverged == UNICODE_VERSION_DIVERGENCE, (
        "the Spark and Python spellings of the hash standard now disagree about a "
        f"different set of characters. Java case table: "
        f"{spark.sparkContext._jvm.System.getProperty('java.version')}; CPython "
        f"{sys.version.split()[0]} / unicodedata {unicodedata.unidata_version}. "
        f"Newly disagreeing: {sorted(diverged - UNICODE_VERSION_DIVERGENCE)}; newly "
        f"agreeing: {sorted(UNICODE_VERSION_DIVERGENCE - diverged)}. EITHER "
        "direction re-keys every vault row containing one of those characters, so "
        "this is a decision and not a test to update: re-read "
        "opl.vault.hashing_spark's docstring on the version skew, decide whether the "
        "vault is re-keyed or the runtime is pinned, and only then move this set."
    )
    assert 0x69 not in diverged, "the Turkish-locale claim in hashing_spark's docstring"


def test_no_character_the_two_spellings_disagree_about_can_reach_cnpj_bronze():
    """The reachability half, and the reason the forty are pinned rather than fixed.

    Bronze is parsed with `CSV_DIALECT["encoding"]`, which is `cp1252` -- imported
    here rather than restated, so this test follows a change to the dialect instead of
    going stale against one. A cp1252 byte stream cannot produce any character outside
    the 256 that codec decodes to, and not one of the forty is among them. So no CNPJ
    bronze row can currently hold a character the two spellings disagree about.

    THAT IS A STATEMENT ABOUT TODAY'S FEEDS AND NOT ABOUT THE STANDARD. A wave-2
    source that is not cp1252-bound -- a UTF-8 payment feed, say -- reaches them
    immediately, and so does a JDK bump. Pure Python: no session needed to check what
    an encoding can produce."""
    for code in sorted(UNICODE_VERSION_DIVERGENCE):
        with pytest.raises(UnicodeEncodeError):
            chr(code).encode(CSV_DIALECT["encoding"])


def test_the_trim_class_names_exactly_the_characters_python_strips():
    """The Spark spelling cannot call `str.strip()`, so it names the characters
    `str.strip()` removes -- and this asserts the naming is exact, in both
    directions, over the whole code space.

    THE DIVERGENCE THIS EXISTS FOR IS NOT HYPOTHETICAL, and it is why the module
    does not simply use Spark's `trim`. Spark SQL's `trim(str)` removes ASCII SPACE
    only (U+0020); Python's `str.strip()` removes 29 different characters, tab and
    NBSP among them. Bronze is parsed from Latin-1 RFB CSVs, where a NBSP inside a
    razão social is an ordinary thing. Left as `trim`, `"\\xa0"` would encode `W` in
    Python and `S1:\\xa0` in Spark -- two digests for one business key, silently, for
    the lifetime of the vault.

    A pure-Python assertion over all 1,114,112 code points: it needs no Spark and
    it is the only version of this claim that is total. The companion test below
    proves the regex BUILT from this tuple actually strips them."""
    assert set(TRIMMED_CHARACTERS) == {
        chr(code) for code in range(0x110000) if chr(code).isspace()
    }


def test_every_character_python_calls_whitespace_is_stripped_by_the_spark_spelling(spark):
    """The other half: the constant above is right, and the REGEX built from it
    behaves.

    Each of the 29 characters, alone, is a whitespace-only component and must encode
    `W` -- so all 29 digests are the same digest, and it is `hash_key([" "])`. A
    character the regex missed would encode `S1:<char>` and stand out as a digest of
    its own."""
    whitespace = [chr(code) for code in range(0x110000) if chr(code).isspace()]
    rows = [(character,) for character in whitespace]

    digests = _spark_digests(spark, rows, "a string", ["a"])

    assert digests == [hash_key([" "])] * len(whitespace)


def test_a_component_containing_the_delimiter_cannot_be_confused_with_two(spark):
    """The collision the length prefix exists to close, asserted on the SPARK side
    as a difference rather than only as agreement.

    The equivalence tests above would stay green if BOTH spellings collided --
    agreement with a broken standard is still agreement. This one says the two
    digests differ, which is the property, and it is the reason this test is not
    redundant with the pair test that already contains these inputs."""
    rows = [("a||b", "c"), ("a", "b||c")]

    first, second = _spark_digests(spark, rows, "a string, b string", ["a", "b"])

    assert first != second
    assert (first, second) == (hash_key(["a||b", "c"]), hash_key(["a", "b||c"]))


def test_zero_padding_agrees_with_the_python_standard(spark):
    """`zero_pad_cnpj`'s Spark twin, over the shapes it is defensive about: an
    already-correct value (padding is a no-op), a short one, and the empty string."""
    values = ["12345678", "1234567", "1", "", "0"]
    rows = [(value,) for value in values]
    frame = spark.createDataFrame(rows, "a string")

    padded = [
        row["padded"]
        for row in frame.withColumn(
            "padded", zero_padded_column(F.col("a"), width=8)
        ).collect()
    ]

    assert sorted(padded) == sorted(zero_pad_cnpj(value, width=8) for value in values)


def test_an_overlong_value_fails_the_job_rather_than_being_truncated(spark):
    """The worst failure this layer can produce, and Spark's `lpad` produces it by
    default: `lpad('123456789', 8, '0')` returns `'12345678'`, silently mapping a
    nine-character value onto another company's true eight-character key.

    `zero_pad_cnpj` refuses in Python. The Spark spelling cannot raise per row
    without failing the query, so failing the query is what it does -- an aborted
    load is recoverable and a merged pair of companies is not."""
    frame = spark.createDataFrame([("123456789",)], "a string")

    with pytest.raises(Exception, match="refusing to truncate"):
        frame.withColumn("padded", zero_padded_column(F.col("a"), width=8)).collect()


def test_a_width_that_is_not_positive_is_refused_before_spark():
    with pytest.raises(ValueError, match="positive"):
        zero_padded_column(F.col("a"), width=0)


def test_hashing_no_components_at_all_is_refused():
    """`hash_key`'s empty-list refusal, in the spelling that has to carry it too:
    without this, `hash_key_column([])` would build `sha2(concat_ws('||'), 256)` --
    the digest of the empty string, shared by every caller who built its column
    list wrong."""
    with pytest.raises(ValueError, match="at least one"):
        hash_key_column([])


def test_a_bare_column_is_refused():
    """The `Column` twin of `hash_key`'s bare-`str` refusal, and it fails WORSE than
    that one: `Column.__getitem__` answers any index, so iterating a bare `Column`
    yields `col[0]`, `col[1]`, ... forever. `hash_key_column(F.col("a"))` would not
    return a wrong digest; it would hang."""
    with pytest.raises(TypeError, match="bare Column"):
        hash_key_column(F.col("a"))


def test_a_non_string_column_is_refused_by_name(spark):
    """Every column this standard hashes is STRING, and in bronze that is a
    deliberate contract decision that includes `capital_social`.

    An implicit cast is what makes this worth refusing rather than tolerating: Spark
    would happily `upper(trim(<int>))` and hash `'1000'`, while `hash_key` would
    raise `AttributeError` on the same value having no `.strip()`. So the two
    spellings do not merely differ on a typed column -- one of them answers and the
    other one refuses, which is the shape of divergence no equivalence test over
    string corpora can see."""
    frame = spark.createDataFrame([(1, "a")], "n int, a string")

    with pytest.raises(TypeError, match="n"):
        refuse_non_string_columns(frame, ["n", "a"])
