# tests/bronze/test_merchant_rules.py
"""The DQ gate's verdict on a Postgres merchant snapshot, and the CHECK the gate's own
docstrings compare themselves against.

ITS OWN FILE, on `test_ptax_rules.py`'s and `test_payment_rules.py`'s precedent:
`test_rules.py`'s fixtures are built on `cnpj_schemas.TABLES` -- `_row(contract, ...)`
indexes it directly -- so every merchant fixture would have to route around the one helper
that file is organised on. The seam is the one this suite already uses: what changes here
is a SOURCE, not the gate's machinery.

WHAT IS ASSERTED ELSEWHERE AND NOT REPEATED: that a registered table has a rule set at all
(`test_rules.py::test_every_registered_table_has_a_rule_set`, which sweeps the registry),
that every predicate is a zero-argument factory, and that every rule this set produces is
named in `rules.py`'s summary block (`test_rule_set_prose.py`).

THIS SET IS LESS TAUTOLOGICAL THAN PTAX's, AND THE REASON IS WORTH STATING RATHER THAN
LEAVING THE GREEN TO READ AS COVERAGE. Those rows are built by this repository from a
response its own extraction layer already validated. These rows come out of an OPERATIONAL
DATABASE that is seeded by one script, mutated by another and reachable by any `psql` on
the box -- so `bad_cnpj_shape`, `bad_onboarded_on_shape` and `encoding_replacement_char`
guard a real boundary rather than restating an upstream refusal. `bad_snapshot_at_shape` is
the sharpest of them: the observation ledger's before/after split is a STRING COMPARISON on
that column, so a wrong shape does not fail anything -- it sorts wrongly."""
from __future__ import annotations

import re

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from opl.bronze.dq import REJECT_COLUMN, RESCUED_DATA_COLUMN, evaluate
from opl.bronze.registry import table_spec
from opl.bronze.rules import rules_for
from opl.bronze.schema import struct_for
from opl.bronze.snapshot_axis import INSTANT_WIDTH, _is_instant
from opl.contracts.merchant import COLUMNS, CONTRACT, REQUIRED_COLUMNS
from opl.unicode_case import UNICODE_VERSION_DIVERGENCE

_REPLACEMENT_CHAR = "�"

# A row that passes every rule, so each test states only the field it is about. Shaped like
# what `scripts/seed_merchant_db` actually produces and Postgres actually renders: a CNPJ
# whose root carries a LEADING ZERO (142 of the 1,024 pinned roots do), a `numeric(14,2)`
# with its scale, an accented `legal_name` bounded below U+0250 by plan T10, and an
# `updated_at` in `::text`'s own spelling rather than the axis's.
_CLEAN = {
    "merchant_id": "00057343-0001-4a2e-8f00-000000000001",
    "cnpj": "00057343000129",
    "legal_name": "COMÉRCIO ATLÂNTICO LTDA",
    "trade_name": None,
    "status": "active",
    "mcc": "5411",
    "settlement_account": "001-0042-00001234",
    "risk_tier": "low",
    "credit_limit": "12345.60",
    "onboarded_on": "2019-03-14",
    "updated_at": "2026-07-15 08:31:02.400500+00",
    "_snapshot_at": "2026-08-16T22:10:47.000123Z",
    "_pg_snapshot": "918:918:",
    "_pg_wal_lsn": "0/1A2B3C8",
}
# The column the third audit path derives and the gate's last rule refuses when it is NULL.
# It is not a contract column, so it is added to the frame separately -- exactly as the
# ingest adds it.
_REF_DATE = "_snapshot_ref_date"


def _row(**overrides: str | None) -> tuple[str | None, ...]:
    """One all-string merchant row in contract order.

    Refuses an override that is not a contract column, for `test_ptax_rules._row`'s reason:
    `_row(trade_names="")` (typo) would otherwise build a perfectly CLEAN row and then have
    a reject reason asserted against it -- failing for a reason unrelated to the typo, or
    passing because something else was dirty."""
    unknown = sorted(set(overrides) - set(COLUMNS))
    if unknown:
        raise AssertionError(f"{unknown} is not a merchant column -- {', '.join(COLUMNS)}")
    return tuple(overrides.get(column, _CLEAN[column]) for column in COLUMNS)


def _frame(spark, rows, *, ref_dates: list[str | None] | None = None, rescued=None):
    """A staging-shaped frame: the contract's columns, plus `_snapshot_ref_date`.

    THE REF DATE IS ALWAYS PRESENT, unlike in the PTAX and payments frames, because for
    this source it always is: `add_instant_audit_columns` stamps it from `_snapshot_at`, so
    `unprovable_snapshot_ref_date` is a rule with a column to read rather than one skipped
    by `REQUIRES_COLUMN`. Explicit schema, never inference: `trade_name` is NULL in the
    clean row and Spark cannot determine an all-null column's type."""
    fields = [*struct_for(CONTRACT).fields, StructField(_REF_DATE, StringType())]
    dates = ["2026-08-16"] * len(rows) if ref_dates is None else ref_dates
    if rescued is not None:
        fields.append(StructField(RESCUED_DATA_COLUMN, StringType()))
        return spark.createDataFrame(
            [(*row, date, r) for row, date, r in zip(rows, dates, rescued, strict=True)],
            StructType(fields),
        )
    return spark.createDataFrame(
        [(*row, date) for row, date in zip(rows, dates, strict=True)], StructType(fields)
    )


def _reasons(spark, rows, **kwargs) -> list[str | None]:
    evaluated = evaluate(_frame(spark, rows, **kwargs), rules_for(CONTRACT))
    return [row[REJECT_COLUMN] for row in evaluated.collect()]


def test_the_merchant_rule_order_is_pinned():
    """First-match-wins makes order part of the contract, and this set decides what the
    first rows of a live quarantine table say about themselves.

    The shape is the same argument every other set makes: what is MISSING, then what is the
    wrong SHAPE, then what cannot be PARSED, then what is damaged in its BYTES -- and then,
    LAST, the one rule that describes the FILE rather than the row. Ranked any higher,
    `unprovable_snapshot_ref_date` would become the reason printed across a whole
    quarantine, burying the per-row defects a triager can act on.

    NOTE WHICH COLUMN HAS NO `null_or_empty_` RULE. `trade_name` is the one nullable column
    of this contract and the source emits NULL, `''` and a name for it deliberately."""
    assert [name for name, _ in rules_for(CONTRACT)] == [
        "null_or_empty_merchant_id",
        "null_or_empty_cnpj",
        "null_or_empty_legal_name",
        "null_or_empty_status",
        "null_or_empty_mcc",
        "null_or_empty_settlement_account",
        "null_or_empty_risk_tier",
        "null_or_empty_credit_limit",
        "null_or_empty_onboarded_on",
        "null_or_empty_updated_at",
        "null_or_empty__snapshot_at",
        "null_or_empty__pg_snapshot",
        "null_or_empty__pg_wal_lsn",
        "bad_cnpj_shape",
        "bad_onboarded_on_shape",
        "bad_snapshot_at_shape",
        "unparseable_credit_limit",
        "encoding_replacement_char",
        "unhashable_case_divergence",
        "unprovable_snapshot_ref_date",
    ]


def test_there_is_one_required_rule_per_required_column_and_none_for_the_nullable_one():
    """Derived from the contract, not listed, so a v2 column arrives with its rule.

    The second half is what this test is really for: `trade_name` must have NO
    `null_or_empty_` rule. `_null_or_blank` treats `''` as blank, so one would reject every
    row whose trade name the source deliberately left empty -- and the reason that column is
    nullable at all is to demonstrate that the landing path keeps NULL and `''` apart."""
    produced = {name for name, _ in rules_for(CONTRACT)}
    required = {f"null_or_empty_{column}" for column in REQUIRED_COLUMNS}

    assert {name for name in produced if name.startswith("null_or_empty_")} == required
    assert "null_or_empty_trade_name" not in produced
    assert len(COLUMNS) == 14


def test_the_clean_row_is_accepted(spark):
    """Guard the guard. Everything below asserts a rejection, and all of it would pass if
    the set rejected every row -- including a NULL `trade_name`, which is the shape roughly
    one row in eight of the seeded population actually lands."""
    assert _reasons(spark, [_row()]) == [None]


def test_an_empty_trade_name_is_accepted_and_stays_distinct_from_a_null_one(spark):
    """THE ONE THING THAT COLUMN IS NULLABLE FOR, asserted at the gate rather than only in
    the extraction: both values must reach bronze, and a rule set that rejected either
    would make the distinction unobservable downstream."""
    assert _reasons(spark, [_row(trade_name=""), _row(trade_name=None)]) == [None, None]


@pytest.mark.parametrize("column", sorted(REQUIRED_COLUMNS))
@pytest.mark.parametrize("blank", ["", "   ", None])
def test_every_required_column_is_refused_when_blank(spark, column, blank):
    """One rule per required column, so the reject reason names WHICH column was empty
    rather than only that one of them was -- these strings are DATA a triager filters a
    quarantine on."""
    assert _reasons(spark, [_row(**{column: blank})]) == [f"null_or_empty_{column}"]


@pytest.mark.parametrize(
    "cnpj",
    [
        "00057343",  # the ROOT: eight characters, and what a writer confusing the two lands
        "0005734300012",  # thirteen: one digit short
        "000573430001299",  # fifteen
        "57343000129",  # the leading zeros eaten by a numeric round trip
        "0005734300012x",  # right width, wrong alphabet
        "0005734300012 ",  # right width, a trailing space -- a DIFFERENT key in `text`
    ],
)
def test_a_cnpj_that_is_not_fourteen_digits_is_refused(spark, cnpj):
    """THE INTEGRATION CLAIM, checked at the gate rather than after the fact. The premise
    is that merchants join to real companies by business key through
    `link_merchant_empresa`, and the failure that breaks it silently is a numeric round
    trip eating a leading zero: 142 of the 1,024 pinned counterparty roots have one, so
    13.9% of this pool would resolve to nothing with every row count green.

    THE TRAILING SPACE IS NOT PEDANTRY. `_basico_length` TRIMS, because the RFB ships
    fixed-width padded CSV and trimming there reads the source's own dialect. This value is
    rendered from a Postgres `text` column, where a trailing space IS a different key -- and
    a rule that trimmed would be LOOSER than the CHECK the promote applies afterwards."""
    assert _reasons(spark, [_row(cnpj=cnpj)]) == ["bad_cnpj_shape"]


@pytest.mark.parametrize(
    "onboarded", ["14-03-2019", "2019-3-14", "2019/03/14", "2019-02-31", "not a date"]
)
def test_an_onboarded_on_that_is_not_a_real_iso_day_is_refused(spark, onboarded):
    """THE EFFECTIVITY SATELLITE'S ENTRY COLUMN. `opl.vault.effectivity` records that a
    NULL entry date SORTS FIRST in Spark and beats a delivered one, so an unparseable entry
    date does not fail -- it wins a window it should have lost, which is a wrong end-dating
    in the one phase whose headline is the first end-dating this lakehouse has ever fired.

    `2019-02-31` is the case a shape check alone misses: it has the digits and names no
    day, which is why the predicate is a regex AND a `to_date`."""
    assert _reasons(spark, [_row(onboarded_on=onboarded)]) == ["bad_onboarded_on_shape"]


@pytest.mark.parametrize(
    "instant",
    [
        "2026-08-16 22:10:47.000123+00",  # `::text`'s own rendering -- space, offset, no Z
        "2026-08-16T22:10:47.000123",  # no zone marker at all
        "2026-08-16T22:10:47.123Z",  # trailing fractional zeros TRIMMED
        "2026-08-16T22:10:47.000123z",  # lower case
        "2026-13-16T22:10:47.000123Z",  # a month that is not one
        "2026-08-16T24:10:47.000123Z",  # an hour that is not one
        "2026-08",  # the MONTH axis, which is what a paste from another source lands
    ],
)
def test_a_snapshot_instant_that_is_not_the_pinned_rendering_is_refused(spark, instant):
    """THE AXIS, AND THE RULE WITH THE LEAST TAUTOLOGICAL READING IN THIS FILE. The
    observation ledger's before/after-first-observation split is a STRING COMPARISON on this
    column and `loading.earliest_record_source` takes a `min` over it, so a value of the
    wrong shape does not raise -- it sorts wrongly, and every state the ledger returns is
    plausible.

    Both qualifiers of the rendering are represented above. `::text` is the FIXED-WIDTH
    failure twice over -- a space instead of `T`, `+00` instead of `Z` -- and the trimmed
    trailing zeros are the sort property directly: `...47.1` sorts AFTER `...47.09`."""
    assert _reasons(spark, [_row(_snapshot_at=instant)]) == ["bad_snapshot_at_shape"]


def test_an_instant_with_a_trailing_newline_is_refused_by_the_length_and_not_the_regex(spark):
    """JAVA'S `$` MATCHES BEFORE A TRAILING LINE TERMINATOR, which is why the width is
    checked beside an already-anchored pattern rather than trusted to be implied by it.

    The control is the second assertion: the Python predicate the vault job's WINDOW
    PARAMETER is validated by refuses this value, so without the length check the gate and
    that predicate would give two different answers about one string."""
    assert _reasons(spark, [_row(_snapshot_at="2026-08-16T22:10:47.000123Z\n")]) == [
        "bad_snapshot_at_shape"
    ]
    assert not _is_instant("2026-08-16T22:10:47.000123Z\n")


@pytest.mark.parametrize("limit", ["1.234,50", "R$ 1234.50", "1234.5e400", "abc"])
def test_a_credit_limit_that_does_not_read_as_a_decimal_is_refused(spark, limit):
    """Near-tautological -- Postgres renders `numeric(14,2)` under pinned GUCs -- and kept
    because this column is in the satellite's `hash_diff`: a value that casts to NULL makes
    two genuinely different payloads digest the same."""
    assert _reasons(spark, [_row(credit_limit=limit)]) == ["unparseable_credit_limit"]


def test_the_declared_precision_is_exercised_rather_than_nominal(spark):
    """`merchant_population.MAX_CREDIT_LIMIT` is `999999999999.99` -- exactly fourteen
    digits -- and roughly four seeded rows carry it. A cast narrower than the DDL would NULL
    a value the source is entitled to send and fail the WHOLE run, since the gate is
    all-or-nothing."""
    assert _reasons(spark, [_row(credit_limit="999999999999.99")]) == [None]
    assert _reasons(spark, [_row(credit_limit="9999999999999.99")]) == [
        "unparseable_credit_limit"
    ]


def test_a_replacement_character_is_caught_in_the_columns_no_earlier_rule_inspects(spark):
    """U+FFFD IS MOJIBAKE AND NOTHING ELSE, and this docstring used to claim more.

    It said this rule was the guard plan T10 asks for -- against the forty characters
    JDK 17 and CPython 3.12 upper-case differently. It is not, and cannot be: those forty
    are valid, correctly decoded characters and this rule looks for exactly one character,
    U+FFFD, which is what Java's decoder substitutes SILENTLY where Python raises. The two
    have no relationship beyond both being about text. T10's guard is
    `unhashable_case_divergence`, tested below.

    What this rule is for is unchanged and is the reason it is live on this contract:
    U+FFFD is the only in-band evidence that a byte was lost on the way in."""
    for column in ("legal_name", "trade_name", "status", "mcc", "risk_tier"):
        assert _reasons(spark, [_row(**{column: f"x{_REPLACEMENT_CHAR}y"})]) == [
            "encoding_replacement_char"
        ], column


def test_a_replacement_character_in_the_cnpj_is_SHADOWED_by_the_shape_rule(spark):
    """First-match-wins, and the fold is still total over all fourteen columns. Said out
    loud because these strings are data an operator filters a quarantine on: a U+FFFD in
    `cnpj` breaks the digit test, one in `onboarded_on` breaks the date shape, and one in
    `_snapshot_at` breaks the instant shape -- so the row is always REJECTED and nothing
    gets through, but `encoding_replacement_char` can never be the REPORTED reason for any
    of the three."""
    assert _reasons(spark, [_row(cnpj=f"0005734300012{_REPLACEMENT_CHAR}")]) == [
        "bad_cnpj_shape"
    ]


# --- T10: the forty characters the two hash spellings disagree about ------------------
#
# A BMP member and an ASTRAL one, named by code point and then ASSERTED against the pinned
# set rather than trusted. The astral half is not decoration: THIRTY-FIVE of the forty are
# above U+FFFF, and a character class written with the literal characters instead of
# `\x{...}` would contain a surrogate pair rather than the code point -- which is why
# `opl.unicode_case` derives the class from the set. A test using only U+A7C1 would pass
# over a pattern that could not see the other thirty-five.
#
# This comment said "twenty-nine" in both places, as did `unicode_case` and
# `rule_predicates` -- one wrong number typed once and copied twice. The test below now
# derives it, which is the only reason the three readings can be trusted to agree.
_DIVERGENT_BMP = "ꟁ"
_DIVERGENT_ASTRAL = "\U00010597"

# U+105A2 SITS INSIDE THE SPAN AND DOES NOT DIVERGE, which is the control that makes the
# assertions below about a measured SET rather than about a range somebody eyeballed. The
# prose in `hashing_spark` named forty-three characters until it was corrected, for exactly
# this reason.
_INSIDE_THE_SPAN_BUT_AGREEING = "\U000105a2"


def test_the_astral_count_the_docstrings_quote_is_DERIVED_from_the_set():
    """THE NUMBER, TAKEN FROM THE SET INSTEAD OF FROM A SENTENCE.

    `unicode_case._class_body`, `rule_predicates._case_divergence_check` and the comment
    above all justify the `\\x{...}` spelling by saying how many of the forty are astral,
    and all three said TWENTY-NINE. Computed from `UNICODE_VERSION_DIVERGENCE` it is
    thirty-five: five BMP members and four plane-1 ranges of 11 + 15 + 7 + 2. One wrong
    number typed once and copied twice, which is what an arithmetic claim does when it
    lives only in prose -- and this file is where the claim decides a FIXTURE, so a reader
    checking whether `_DIVERGENT_ASTRAL` is representative was being told the wrong size
    of the thing it represents.

    NO SPARK: this is arithmetic over a frozenset, so it costs its chunk nothing. It is
    here rather than beside `unicode_case` because `unicode_case` has no test module of
    its own and this file already imports the set and already quotes the number."""
    astral = {point for point in UNICODE_VERSION_DIVERGENCE if point > 0xFFFF}

    assert len(UNICODE_VERSION_DIVERGENCE) == 40, "the pinned set is no longer forty"
    assert len(astral) == 35, (
        f"{len(astral)} of the {len(UNICODE_VERSION_DIVERGENCE)} pinned characters are "
        "astral, and three docstrings say thirty-five. Correct them together or the next "
        "reader gets the same wrong number from three places at once"
    )
    assert len(UNICODE_VERSION_DIVERGENCE) - len(astral) == 5


@pytest.mark.parametrize("character", [_DIVERGENT_BMP, _DIVERGENT_ASTRAL])
@pytest.mark.parametrize("column", ["legal_name", "trade_name"])
def test_a_case_divergent_character_is_refused_by_its_OWN_named_rule(spark, column, character):
    """PLAN T10's GUARD, WHERE T10 SAID IT BELONGS: the bronze gate, not the seeder.

    The seeder bound protects the seed and nothing else -- not a manual `psql` INSERT, not
    the mutation script, not a re-seed with different literals. This runs on every landed
    row whatever wrote it.

    THE REASON IS ASSERTED AND NOT JUST THE REJECTION, because the gate is first-match-wins
    and a test asserting only "the row was rejected" would pass on the wrong rule. In
    particular it would pass if `encoding_replacement_char` -- which ranks ABOVE this and
    was until now the only thing anyone pointed at for T10 -- had fired. It cannot: these
    are valid characters and that rule looks for U+FFFD.

    WHAT IT PREVENTS. `F.upper` uses Java's case table (JDK 17, Unicode 13.0) and
    `str.upper()` uses CPython's (3.12, Unicode 15.0); the two disagree about exactly these
    forty. One in `legal_name` reaches the satellite's `hash_diff`, the Python and Spark
    digests disagree on real data, and NOTHING GOES RED, because the loaders only ever use
    the Spark spelling."""
    assert ord(character) in UNICODE_VERSION_DIVERGENCE, "the fixture left the pinned set"

    assert _reasons(spark, [_row(**{column: f"ACME {character} LTDA"})]) == [
        "unhashable_case_divergence"
    ]


def test_a_character_inside_the_span_that_AGREES_is_left_alone(spark):
    """GUARD THE GUARD, and for this rule it is the assertion that matters most.

    A predicate that refused the whole U+10597-U+105BC range would pass every assertion
    above and reject three characters this lakehouse hashes identically in both spellings.
    The rule has to mean the MEASURED set, and the only way to show that is a member of the
    range that is not a member of the set."""
    assert ord(_INSIDE_THE_SPAN_BUT_AGREEING) not in UNICODE_VERSION_DIVERGENCE

    assert _reasons(spark, [_row(legal_name=f"ACME {_INSIDE_THE_SPAN_BUT_AGREEING} LTDA")]) == [
        None
    ]


def test_a_case_divergent_character_in_the_cnpj_is_SHADOWED_by_the_shape_rule(spark):
    """The fold is total over all fourteen columns and first-match-wins is still the
    contract, so said out loud for `encoding_replacement_char`'s reason: these strings are
    data an operator filters a quarantine on. One of the forty in `cnpj` breaks the digit
    test, so the row is REJECTED and nothing gets through, but this rule can never be the
    REPORTED reason there. The columns it can be reported for are `legal_name` and
    `trade_name` -- which are the columns T10 says a UTF-8 source reaches the forty
    through."""
    assert _reasons(spark, [_row(cnpj=f"0005734300012{_DIVERGENT_BMP}")]) == ["bad_cnpj_shape"]


def test_an_unprovable_reference_date_is_refused_last(spark):
    """THE HALF PLAN T8 EXISTS FOR. This source cannot derive the column from a filename
    and cannot omit it either -- `opl.vault.satellites` reads it unconditionally to build
    `applied_date` -- so the third audit path stamps it from `_snapshot_at` and this rule
    keeps its job: an instant the derivation could not read yields NULL, and the row is
    rejected in the gate rather than reaching a satellite with no applied date."""
    assert _reasons(spark, [_row()], ref_dates=[None]) == ["unprovable_snapshot_ref_date"]


def test_a_row_with_two_faults_reports_the_first_rule_that_matches(spark):
    """Order is the contract. A row that is both blank in a required column and malformed
    in another is described by what is MISSING, not by the shape of what is left."""
    assert _reasons(spark, [_row(cnpj="", onboarded_on="nope")]) == ["null_or_empty_cnpj"]


def test_rescued_data_outranks_every_rule_here(spark):
    """`dq._reject_reason` ranks `rescued_data_present` above every per-table rule, which is
    what makes an UNDECLARED Postgres column a quarantine rather than a silent adoption. The
    extraction's own catalog read refuses that upstream; this is the boundary underneath it,
    for a file landed by another revision's wheel or copied in by hand."""
    assert _reasons(spark, [_row()], rescued=['{"loyalty_tier":"gold"}']) == [
        "rescued_data_present"
    ]


# --- the gate and the promote-time CHECK, compared against each other -------------------


def _instant_check_pattern() -> str:
    """The regex out of the registry's `_snapshot_at_instant_shape` CHECK statement.

    Read from the live constraint rather than restated, so the comparison below is between
    the DECLARATION and the axis instead of between two things this file typed."""
    statement = next(
        s for s in table_spec("merchant").constraints if "regexp_like" in s
    )
    return re.search(r"regexp_like\(_snapshot_at, '(.+)'\)\)", statement).group(1)


def test_the_promote_time_CHECK_accepts_exactly_what_the_axis_predicate_accepts():
    """THE CROSS-CHECK THAT MAKES A NECESSARY SECOND SPELLING SAFE.

    The CHECK cannot reuse `snapshot_axis.INSTANT_PATTERN`: `promote_batch._assert_
    constraints` issues `statement.format(table=tbl)`, so a `{6}` quantifier raises
    IndexError from `str.format` AFTER the append has committed. So the constraint spells
    `[0-9]` out digit by digit -- and this is what says the two spellings agree, over the
    same probes the gate is tested on above plus the value that must be ACCEPTED.

    THE WIDTH IS THE OTHER HALF OF THE CONSTRAINT and is asserted separately, because the
    regex alone cannot carry it: `$` matches before a trailing line terminator in this
    engine too."""
    pattern = _instant_check_pattern()
    probes = [
        "2026-08-16T22:10:47.000123Z",
        "2026-08-16 22:10:47.000123+00",
        "2026-08-16T22:10:47.123Z",
        "2026-13-16T22:10:47.000123Z",
        "2026-08-16T24:10:47.000123Z",
        "2026-08",
        "2026-02-31T00:00:00.000000Z",  # a shape, not a calendar -- BOTH accept it
    ]
    for probe in probes:
        assert bool(re.match(pattern, probe)) == _is_instant(probe), probe

    assert f"length(_snapshot_at) = {INSTANT_WIDTH}" in next(
        s for s in table_spec("merchant").constraints if "regexp_like" in s
    )


def test_the_check_carries_no_brace_quantifier_that_str_format_would_eat():
    """`promote_batch._assert_constraints` issues `statement.format(table=tbl)`. A `{6}`
    raises IndexError there -- after the append has committed, on the run that was meant to
    assert the constraint. `test_registry.py` sweeps every table for this; the merchant
    entry gets its own line because it is the first constraint here whose natural spelling
    is a quantified pattern, so the trap is one edit away rather than hypothetical."""
    for statement in table_spec("merchant").constraints:
        assert statement.format(table="workspace.default.bronze_merchant")
