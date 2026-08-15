# tests/bronze/test_ptax_rules.py
"""The DQ gate's verdict on the PTAX series -- and, at the foot of this file, the ONE
promote-time CHECK the gate's own docstrings compare themselves against.

That second part is here rather than in `test_registry.py` because it is the other half of
a sentence `bad_quote_date_shape` already carries: the gate tells two ten-character
spellings apart before anything reaches bronze, and the CHECK runs at the promote after
the append has committed. Both halves of a comparison in one file; `test_registry.py`
keeps the DDL string pin, which is a different claim from what the engine refuses.

ITS OWN FILE, for `test_payment_rules.py`'s reason: `test_rules.py`'s fixtures are built
on `cnpj_schemas.TABLES` -- `_row(contract, ...)` indexes it directly -- so every PTAX
fixture would have to route around the one helper that file is organised on. The seam is
the one this suite already uses: what changes here is a SOURCE, not the gate's machinery.

WHAT IS ASSERTED ELSEWHERE AND NOT REPEATED: that a registered table has a rule set at
all (`test_rules.py::test_every_registered_table_has_a_rule_set`, which sweeps the
registry), and that every predicate is a zero-argument factory (same file, derived from
`REQUIRED_FIELDS`).

MOST OF THESE RULES ARE NEAR-TAUTOLOGIES AND THIS FILE SAYS SO IN EVERY DOCSTRING RATHER
THAN LETTING THE GREEN READ AS COVERAGE. The record the gate judges is built by
`databricks/src/fetch_ptax.py` from a response `opl.extraction.ptax_source` has already
validated, so a blank column or an unreadable rate means the emitter changed shape, not
that BCB sent something odd. The rows below are therefore SYNTHESISED defects -- they
exercise the rule, and none of them is a body the API has ever returned. `bad_quote_date_
shape` is the one exception, and it is the one this phase actually invites."""
from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from opl.bronze.dq import REJECT_COLUMN, RESCUED_DATA_COLUMN, evaluate, split
from opl.bronze.registry import table_spec
from opl.bronze.rules import rules_for
from opl.bronze.schema import struct_for
from opl.contracts.ptax import COLUMNS, CONTRACT, REQUIRED_COLUMNS

_REPLACEMENT_CHAR = "�"

# A row that passes every rule, so each test states only the field it is about. The
# values are the ones F-API Task 0 measured for 2026-06-19 and published in
# `docs/f-api-run-evidence.md`, digits included: a fixture built from invented numbers
# would not show that `5.14420` keeps its trailing zero all the way to the gate.
_CLEAN = {
    "quote_date": "2026-06-19",
    "currency": "USD",
    "data_hora_cotacao": "2026-06-19 13:03:25.555497",
    "cotacao_compra": "5.14360",
    "cotacao_venda": "5.14420",
}


def _row(**overrides: str | None) -> tuple[str | None, ...]:
    """One all-string PTAX row in contract order.

    Refuses an override that is not a contract column, for `test_payment_rules._row`'s
    reason: `_row(cotacao_vendas="")` (typo) would otherwise build a perfectly CLEAN row
    and then have a reject reason asserted against it -- failing for a reason unrelated
    to the typo, or passing because something else was dirty."""
    unknown = sorted(set(overrides) - set(COLUMNS))
    if unknown:
        raise AssertionError(f"{unknown} is not a ptax column -- {', '.join(COLUMNS)}")
    return tuple(overrides.get(column, _CLEAN[column]) for column in COLUMNS)


def _frame(spark, rows, *, rescued: list[str | None] | None = None):
    """A staging-shaped frame: the contract's columns, optionally plus `_rescued_data`.

    Explicit schema, never inference: `_rescued_data` is all-NULL in a clean row and
    Spark cannot determine an all-null column's type."""
    schema = struct_for(CONTRACT)
    if rescued is None:
        return spark.createDataFrame(list(rows), schema)
    widened = StructType([*schema.fields, StructField(RESCUED_DATA_COLUMN, StringType())])
    return spark.createDataFrame(
        [(*row, value) for row, value in zip(rows, rescued, strict=True)], widened
    )


def _reasons(spark, rows) -> list[str | None]:
    evaluated = evaluate(_frame(spark, rows), rules_for(CONTRACT))
    return [row[REJECT_COLUMN] for row in evaluated.collect()]


def test_the_ptax_rule_order_is_pinned():
    """First-match-wins makes order part of the contract, and this set decides what the
    first rows of a live quarantine table say about themselves.

    The shape is the same argument every other set makes: what is MISSING, then what is
    the wrong SHAPE, then what cannot be PARSED, then what is damaged in its BYTES.

    `unprovable_snapshot_ref_date` is absent because the COLUMN is -- BCB declares no
    reference date in a filename, so `add_common_audit_columns` does not stamp one and
    there is nothing for the rule to refuse. That omission is a consequence, not a
    choice."""
    assert [name for name, _ in rules_for(CONTRACT)] == [
        "null_or_empty_quote_date",
        "null_or_empty_currency",
        "null_or_empty_data_hora_cotacao",
        "null_or_empty_cotacao_compra",
        "null_or_empty_cotacao_venda",
        "bad_quote_date_shape",
        "unparseable_cotacao_compra",
        "unparseable_cotacao_venda",
        "unparseable_data_hora_cotacao",
        "encoding_replacement_char",
    ]


def test_there_is_one_required_rule_per_contract_column():
    """Derived, not listed, so a v2 column arrives with its rule.

    Every column is required here, which is not a judgement call about the source but a
    restatement of what the extraction layer already refuses -- see the contract.

    Note what is NOT asserted here and why. `len(REQUIRED_COLUMNS) == len(COLUMNS)` was,
    and it cannot fail: the contract declares `REQUIRED_COLUMNS = COLUMNS`, so those are
    two names for ONE tuple object and the comparison is a tuple against itself. What can
    fail, and is what this test is for, is that the rule set carries one rule per required
    column and that no `null_or_empty_*` rule exists for anything else."""
    produced = {name for name, _ in rules_for(CONTRACT)}
    required = {f"null_or_empty_{column}" for column in REQUIRED_COLUMNS}
    assert required <= produced
    assert {name for name in produced if name.startswith("null_or_empty_")} == required
    assert len(COLUMNS) == 5


def test_the_clean_row_is_accepted(spark):
    """Guard the guard. Everything below asserts a rejection, and all of it would pass
    if the set rejected every row -- including the measured 2026-06-19 quote, which is
    the shape the whole phase lands."""
    assert _reasons(spark, [_row()]) == [None]


@pytest.mark.parametrize("column", COLUMNS)
@pytest.mark.parametrize("blank", ["", "   ", None])
def test_a_blank_or_absent_column_is_rejected_by_name(spark, column, blank):
    """NEAR-TAUTOLOGICAL AGAINST THE LIVE SOURCE, and it earns its place anyway.

    `ptax_source.quotes_in` refuses a response row missing any API field and carries the
    request's two values onto every quote, so nothing BCB returns can produce this. What
    it catches is the layer in between: `fetch_ptax.record_of` emptying a column, which
    is a change nothing else would report.

    The reason names WHICH column, rather than a single `missing_required_field`, because
    these strings are DATA -- they sit in a quarantine table an operator filters on."""
    assert _reasons(spark, [_row(**{column: blank})]) == [f"null_or_empty_{column}"]


@pytest.mark.parametrize(
    "quote_date",
    [
        # THE MISTAKE THIS PHASE INVITES. The endpoint is asked in MM-DD-YYYY in single
        # quotes, so this is what a writer that reused the request's own spelling lands.
        # Ten characters, non-blank, and it joins to NOTHING in gold.
        "06-19-2026",
        # Shape-valid, names no day -- so the registry's CHECK, which is that shape and
        # nothing more, ACCEPTS it. This is the value that makes the gate strictly stronger
        # than the constraint rather than a duplicate of it, and the Delta test at the foot
        # of this file lands it to prove the acceptance is real.
        "2026-13-45",
        # The other direction: a real date in a spelling nothing downstream parses.
        "19/06/2026",
        "2026-6-19",
    ],
)
def test_a_quote_date_that_is_not_an_iso_day_is_rejected(spark, quote_date):
    """THE ONE RULE HERE THAT IS NOT NEAR-TAUTOLOGICAL.

    `06-19-2026` is the value the phase's own API format produces, and it is what a writer
    that reused the request's own spelling lands.

    WHAT THIS ADDS OVER THE REGISTRY'S CHECK IS NO LONGER THAT THE CHECK IS BLIND TO IT.
    That CHECK was `length(trim(quote_date)) = 10`, which accepted this string; F-API's fix
    pass made it `regexp_like` on the same digit shape, so it refuses it too -- and a
    docstring here went on saying otherwise, which is the species this phase keeps finding.
    Two differences survive and both point the same way: this predicate ALSO requires
    `to_date` to name a real day, which is why `2026-13-45` is in the list above and is
    ACCEPTED by the CHECK; and this runs in the gate, before anything reaches bronze, while
    the CHECK runs at the promote, AFTER the append has committed."""
    assert _reasons(spark, [_row(quote_date=quote_date)]) == ["bad_quote_date_shape"]


@pytest.mark.parametrize("column", ["cotacao_compra", "cotacao_venda"])
@pytest.mark.parametrize("rate", ["5,14420", "R$ 5.14", "five", "5.14.420"])
def test_a_rate_that_does_not_read_as_a_number_is_rejected(spark, column, rate):
    """NEAR-TAUTOLOGICAL: the writer stamps `str(Decimal(...))` from a value already
    parsed with `Decimal` from the raw response text.

    It earns its place on an all-or-nothing gate because of what a NULL rate does rather
    than how likely it is: `amount_brl` is `amount_original * venda`, so an unreadable
    venda converts every payment on that date at nothing and lowers a total by an amount
    nobody can name. The comma spelling is the plausible one -- a locale-aware formatter
    anywhere on the path produces it, and `5,14420` is how the number is written in the
    country that publishes it."""
    assert _reasons(spark, [_row(**{column: rate})]) == [f"unparseable_{column}"]


def test_a_rate_keeps_its_trailing_zero_through_the_gate(spark):
    """The digit-fidelity claim, checked where it would be lost.

    `5.14420` is what BCB published; `json.loads` on the live body yields the float
    `5.0773`-style value and `str()` drops the trailing zero. Bronze is all-string, so
    the gate must not be the place a rate is normalised -- and a parse rule written as a
    CAST-and-write-back rather than a CAST-and-test would do exactly that."""
    accepted, rejected = split(_frame(spark, [_row()]), rules_for(CONTRACT))
    assert rejected.count() == 0
    assert accepted.collect()[0]["cotacao_venda"] == "5.14420"


@pytest.mark.parametrize(
    "published",
    [
        # Neither the ISO 'T' spelling nor the API's space spelling: this is what a
        # writer that reformatted the stamp would produce.
        "19/06/2026 13:03",
        "not-a-timestamp",
        "",
    ],
)
def test_a_publication_instant_that_cannot_be_read_is_rejected(spark, published):
    """T3'S COMPARATOR, and the reason this is the most load-bearing of the three parse
    rules even though it is as near-tautological as the other two.

    The rate for a payment is the most recent quote whose publication instant precedes
    the payment's own. A value `to_timestamp` cannot read becomes NULL in that
    comparison: the row drops out of the as-of resolution and the payment silently
    resolves to an OLDER quote. Nothing is missing and nothing fails -- the answer is
    just the wrong rate, which is the class this gate exists to stop.

    `""` is here to pin the ORDER rather than this rule: a blank stamp trips
    `null_or_empty_data_hora_cotacao` first, and first-match-wins means that is the
    reason recorded. It is asserted below rather than expected here."""
    expected = (
        "null_or_empty_data_hora_cotacao" if not published
        else "unparseable_data_hora_cotacao"
    )
    assert _reasons(spark, [_row(data_hora_cotacao=published)]) == [expected]


def test_a_bare_time_resolves_to_TODAYS_DATE_and_is_therefore_refused(spark):
    """THE TENSION THIS RULE WAS TIGHTENED TO CLOSE, and it asserts the VALUE rather than
    only the verdict -- because a test that pinned the verdict is exactly how three
    readers in a row inherited a wrong number.

    The rule was `to_timestamp(...).isNull()` alone, and the test here asserted
    `== [None]`: the bare time is accepted. That assertion is true under EITHER value, so
    it verified that the rule did not fire and said nothing about what the rule let
    through. The docstring, two commit messages and a published evidence section all said
    `13:03:25.555497` becomes `1970-01-01T13:03:25`, "a real instant fifty-six years early
    that every payment sorts after". Nobody measured it.

    IT BECOMES TODAY'S DATE. So the landed value was NON-DETERMINISTIC -- the same bytes
    yield a different instant tomorrow, and bronze's whole contract is being a function of
    its input -- and the consequence was INVERTED: today is later than every payment in
    this phase's June/July window, so the row drops out of every as-of set and the payment
    resolves to an OLDER quote, verbatim the failure the rule exists to prevent.

    THE FIRST ASSERTION IS THE MEASUREMENT, RE-TAKEN ON EVERY RUN, and it is compared
    against `current_date()` rather than against a literal on purpose: a literal
    `2026-08-14` in this file would be a second number going stale in exactly the way the
    retracted one did. It fails if the resolved instant ever changes -- to 1970-01-01, to
    NULL, to anything else -- which is what a value pin means.

    THE SECOND IS THE FIX: the rule now REFUSES the shape, so it fails the moment anyone
    reverts to the format-agnostic parse. The two together are the whole closing argument.

    A pinned `to_timestamp` PATTERN is still the wrong fix and is not what changed here --
    see `test_every_fractional_second_width_the_series_uses_is_accepted`."""
    bare = "13:03:25.555497"
    resolved = (
        _frame(spark, [_row(data_hora_cotacao=bare)])
        .select(
            F.to_timestamp(F.col("data_hora_cotacao")).cast("date").alias("day"),
            F.current_date().alias("today"),
        )
        .collect()[0]
    )
    assert resolved["day"] == resolved["today"], (
        "Spark dates a bare time with the date the QUERY RAN, which is what makes the "
        "landed value non-deterministic; if this is ever false the retracted 1970-01-01 "
        "claim, or something new, is what the rule would be letting through"
    )
    assert resolved["day"] != date(1970, 1, 1), "the retracted claim, refused explicitly"
    assert _reasons(spark, [_row(data_hora_cotacao=bare)]) == [
        "unparseable_data_hora_cotacao"
    ]


@pytest.mark.parametrize(
    "published",
    [
        # NO DATE AT ALL: today's date, non-deterministically. The headline case.
        "13:03:25.555497",
        "13:03:25",
        # NO TIME AT ALL: determinate, and midnight -- an instant BCB never published, so
        # T3's comparison silently becomes the calendar-day one the contract's provenance
        # guard exists to refuse. `to_timestamp` accepts it; the shape does not.
        "2026-06-19",
        # A SPELLING THE SERIES DOES NOT USE, accepted by Spark's parser and refused here
        # because whether a spelling is a publication instant is ONE decision shared with
        # `ptax_source.PUBLICATION_FORMATS`, which takes a space and not a `T`. Bronze
        # lands the string BCB sent; a reformatted one means something rewrote it.
        "2026-06-19T13:03:25.555497",
        # Seven fractional digits: past what `%f` takes, so the extraction refuses it too.
        "2026-06-19 13:03:25.5554970",
        # Padded. `to_timestamp` trims; the column is landed as-is, and every other shape
        # rule in this module (`bad_quote_date_shape`) anchors without trimming.
        "  2026-06-19 13:03:25  ",
    ],
)
def test_a_stamp_whose_instant_its_own_text_does_not_determine_is_refused(spark, published):
    """THE PROPERTY, over every way of failing it that Spark's parser would accept.

    Each of these returns a non-NULL timestamp from `to_timestamp`, so the rule as first
    shipped accepted all six. What they have in common is that the instant is not fully
    determined by the landed text -- either because the text names no date (the first two,
    which are dated by the clock) or because something between the response and the file
    rewrote a stamp that had already parsed, which is the one thing this rule is second
    at."""
    assert _reasons(spark, [_row(data_hora_cotacao=published)]) == [
        "unparseable_data_hora_cotacao"
    ]


def test_a_shaped_stamp_that_names_no_instant_is_still_refused_by_the_parse(spark):
    """WHY THE RULE IS TWO CHECKS AND NOT ONE, in the direction the shape cannot see.

    `2026-13-45 11:00:00` has the shape exactly and names neither a month nor a day, so
    the shape half accepts it and `to_timestamp` returns NULL. It is the same pairing
    `bad_quote_date_shape` makes -- a regex for the spelling, a parse for the day -- and
    it is what keeps the parse half load-bearing rather than decorative after the
    tightening."""
    assert _reasons(spark, [_row(data_hora_cotacao="2026-13-45 11:00:00")]) == [
        "unparseable_data_hora_cotacao"
    ]


@pytest.mark.parametrize(
    "published",
    [
        # THE SERIES USES EVERY WIDTH FROM 1 TO 6, AND THIS LIST USED TO NAME THREE. Each
        # value below is a REAL published stamp, re-read from the live endpoint through
        # `scripts/probe_ptax.py`'s own request layer -- not a width invented to fill a
        # gap. This phase's own 42 landed quotes carry widths 2/3/4/5/6 across 2/2/1/2/35
        # rows, so the retired enumeration "1 in 1984, 3 in 2025, 6 in 2026" was falsified
        # by the extraction window it was written for. A pinned pattern rejects real rows,
        # which is why the rule is format-agnostic.
        "1984-12-03 11:29:00.0",  # 1 digit -- quote date 1984-11-28, published 5 days on
        "2026-06-03 13:06:26.54",  # 2 -- and the run log rendered it `.540000`, %f padding
        "2025-04-23 13:06:30.416",  # 3 -- the earlier of the duplicate pair (sec 0.7)
        "2026-07-23 13:11:15.6614",  # 4
        "2026-06-09 13:12:05.30888",  # 5
        "2026-06-19 13:03:25.555497",  # 6
        "2026-06-19 13:03:25",  # none -- no observed row, so an absence is not a refusal
    ],
)
def test_every_fractional_second_width_the_series_uses_is_accepted(spark, published):
    """GUARDS THE GUARD, against the version of this rule that would look tighter.

    A rule pinned to `yyyy-MM-dd HH:mm:ss.SSSSSS` accepts most 2026 rows and rejects
    1984-12-03, 2025-04-23 and FIVE of this phase's own landed quotes -- every one of them
    a real row this endpoint returns. It would pass every test written from a sample of
    the phase's window and reject the series.

    THIS IS THE HALF THE TIGHTENING HAD TO KEEP, and it is why the fix is a shape whose
    fractional group is `{1,6}`-or-absent rather than a `to_timestamp` format string. `{1,6}`
    is not an enumeration of observed widths -- it is `%f`'s own range, which is what makes
    it survive a width nobody has seen yet, and `ptax_source.PUBLICATION_FORMATS` carries the
    no-fraction spelling for the same reason: an absence must not be a refusal."""
    assert _reasons(spark, [_row(data_hora_cotacao=published)]) == [None]


# --- THE SEAM: `_INSTANT_SHAPE` AGAINST `ptax_source`'s OWN PARSE ---------------------
#
# `rules._INSTANT_SHAPE`'s comment block used to say it matched `PUBLICATION_FORMATS`
# "exactly", and that was measured FALSE: `strptime`'s `%m`, `%d`, `%H`, `%M` and `%S` each
# accept an unpadded field where the regex demands two digits. The claim is now an
# ASYMMETRY, so it is pinned as one -- a table of spellings with BOTH verdicts recorded,
# each verified against the real implementation on both sides. `_the_extraction_reads`
# calls `ptax_source._publication_instant` rather than re-looping over the formats, so
# there is no second spelling of the parse in here to drift from the first.
#
# The property that matters is the DIRECTION, asserted over the whole table below: no
# spelling is accepted by the gate and refused by the extraction. That is what makes the
# gap safe -- a value cannot reach bronze past the extraction and then be called valid at
# the gate -- and the reverse gap (extraction accepts, gate refuses) is exactly the five
# unpadded rows, which is the cost `rules.py` now states rather than claims away.
_GATE_REFUSES, _GATE_ACCEPTS = True, False
_SEAM_CASES = [
    # (spelling, the gate refuses it, the extraction reads it)
    ("2026-06-19 13:03:25.555497", _GATE_ACCEPTS, True),  # what the phase actually lands
    ("2026-06-19 13:03:25", _GATE_ACCEPTS, True),  # the fraction-less second spelling
    ("2026-6-19 13:03:25", _GATE_REFUSES, True),  # unpadded month: `%m` takes it
    ("2026-06-9 13:03:25", _GATE_REFUSES, True),  # unpadded day
    ("2026-06-19 1:03:25", _GATE_REFUSES, True),  # unpadded hour
    ("2026-06-19 13:3:25", _GATE_REFUSES, True),  # unpadded minute
    ("2026-06-19 13:03:5", _GATE_REFUSES, True),  # unpadded second
    ("26-06-19 13:03:25", _GATE_REFUSES, False),  # `%Y` is the one field that wants four
    ("2026-13-45 11:00:00", _GATE_REFUSES, False),  # shaped, names no instant: both refuse
]


def _the_extraction_reads(stamp: str) -> bool:
    """Whether `ptax_source` would land a quote carrying `stamp`. The extraction's own
    decision, taken by its own function -- a copy of the `PUBLICATION_FORMATS` loop here
    would be a second spelling of the thing this file exists to compare against."""
    from opl.extraction.ptax_source import PtaxResponseRefused, _publication_instant

    try:
        _publication_instant(stamp, date(2026, 6, 19))
    except PtaxResponseRefused:
        return False
    return True


@pytest.mark.parametrize(("stamp", "gate_refuses", "extraction_reads"), _SEAM_CASES)
def test_the_gate_accepts_no_spelling_the_extraction_would_refuse(
    spark, stamp, gate_refuses, extraction_reads
):
    """Both verdicts on one spelling, so neither side can move in silence.

    The five unpadded rows are the divergence itself: the extraction lands them and the
    gate fails the whole run on them, under a reason ("does not read as a determinate
    publication instant") that is untrue of a stamp `strptime` read perfectly well. If
    `PUBLICATION_FORMATS` is ever padded, or the shape's quantifiers widened to `{1,2}`,
    those rows fail here and the paragraph in `rules.py` is what needs rewriting."""
    reasons = _reasons(spark, [_row(data_hora_cotacao=stamp)])
    expected = ["unparseable_data_hora_cotacao"] if gate_refuses else [None]
    assert reasons == expected, f"the GATE's verdict on {stamp!r} moved"
    assert _the_extraction_reads(stamp) is extraction_reads, (
        f"the EXTRACTION's verdict on {stamp!r} moved, so the asymmetry `rules.py` "
        "states about these two layers is no longer the one measured here"
    )


def test_no_case_in_the_seam_table_is_gate_accepted_and_extraction_refused():
    """THE DIRECTION, over the verified table rather than over an adjective.

    Each row above is checked against both implementations, so this reads the measurement
    rather than a declaration. It is the whole safety argument for the gap: the gate being
    STRICTER means a landed file cannot hold a stamp the extraction blessed and the gate
    then rejects for a reason untrue of it -- and the opposite pairing, if it ever appeared,
    would be a value the extraction refuses reaching bronze unremarked."""
    wrong_way = [
        stamp for stamp, refuses, reads in _SEAM_CASES if not refuses and not reads
    ]
    assert not wrong_way, (
        f"{wrong_way} would be accepted by the gate and refused by the extraction, which "
        "inverts the direction of the divergence -- see `rules._INSTANT_SHAPE`"
    )
    assert [stamp for stamp, refuses, reads in _SEAM_CASES if refuses and reads], (
        "the table no longer holds a single spelling the extraction reads and the gate "
        "refuses, so the asymmetry rules.py describes has been closed -- delete the "
        "paragraph rather than leaving it as this phase's next stale claim"
    )


@pytest.mark.parametrize("column", COLUMNS)
def test_a_replacement_character_is_caught_but_only_currency_REPORTS_it(spark, column):
    """THE ENCODING CHECK IS SHADOWED ON FOUR OF THE FIVE COLUMNS IT IS FOLDED OVER, and
    that is measured here rather than left as a surprise in a quarantine table.

    "Shadowed" is a stronger and different statement from the "near-tautology" every other
    docstring in this file makes, so it earns its own test. First-match-wins is the gate's
    contract, and an earlier CONTENT rule sits on every column except `currency`: any
    U+FFFD in `quote_date` breaks `bad_quote_date_shape`'s regex, any U+FFFD in either rate
    makes the decimal cast NULL, and any U+FFFD in `data_hora_cotacao` breaks the
    publication-instant shape. So the row is always REJECTED -- nothing gets through -- but
    the reason a triager filters on names the content rule, and `encoding_replacement_char`
    can only ever be the recorded reason for one of the five columns.

    WHY IT IS STILL RIGHT TO FOLD IT OVER ALL FIVE. The fold is derived from the contract
    (`rules._encoding_check`), so a v2 column arrives covered; `currency` proves the fold
    reaches a column no other rule inspects; and the four shadowed columns are shadowed by
    rules that describe the same row correctly, which is a reporting fact rather than a
    coverage gap. What would be wrong is believing the string
    `encoding_replacement_char` in a live quarantine tells you WHICH column held the
    character -- on this table it tells you it was `currency`.

    THE ORDER IS ALSO WHAT SAYS THE FOLD IS LAST DELIBERATELY: a row whose bytes are
    damaged is reported as such only once nothing simpler explains it, which is the
    argument every rule set in this repository makes."""
    shadowing = {
        "quote_date": "bad_quote_date_shape",
        "cotacao_compra": "unparseable_cotacao_compra",
        "cotacao_venda": "unparseable_cotacao_venda",
        "data_hora_cotacao": "unparseable_data_hora_cotacao",
        "currency": "encoding_replacement_char",
    }
    dirty = _CLEAN[column] + _REPLACEMENT_CHAR
    assert _reasons(spark, [_row(**{column: dirty})]) == [shadowing[column]]


def test_a_row_with_two_faults_reports_the_first_rule_that_matches(spark):
    """First-match-wins, stated on a row that trips two rules.

    A row with a blank venda AND an API-shaped quote_date is described by the MISSING
    column rather than by the wrong-shaped one -- what is absent before what is the wrong
    shape, which is the ordering argument every set in this repository makes."""
    assert _reasons(spark, [_row(cotacao_venda="", quote_date="06-19-2026")]) == [
        "null_or_empty_cotacao_venda"
    ]


# --- AND THE OTHER HALF OF THE COMPARISON: THE PROMOTE'S OWN CHECK -------------------
#
# `bad_quote_date_shape`'s docstring compares itself against the registry's CHECK
# constraint, and until F-API's fix pass nothing anywhere exercised the comparison. The
# docstring said "length alone accepts `06-19-2026`", which was true of the CHECK as
# shipped -- named `quote_date_iso_shape` and spelled `length(trim(quote_date)) = 10`, so
# the constraint named for the ISO shape admitted the one non-ISO spelling this phase
# invites. The fix pass replaced it with `regexp_like`, and the sentence went stale in three
# places rather than being retired with it; what the docstring claims NOW is that the gate
# is strictly stronger, which is two values and not an adjective: `06-19-2026` is refused by
# both, and `2026-13-45` is refused by the gate and LANDS past the CHECK. Both are asserted
# below. `tests/bronze/test_registry.py` pins the DDL string; this is the behavioural half,
# against a real Delta transaction log, because a CHECK is a claim about what the engine
# will refuse and not about what the string says.


def test_the_iso_shape_check_refuses_the_apis_own_spelling_on_a_real_delta_table(
    spark, tmp_path
):
    """The registry's `quote_date` DDL, applied, and asked about four real spellings.

    Through `statement.format(table=...)` exactly as `promote_batch._assert_constraints`
    issues it, so this also exercises the reason the regex carries no `{n}` quantifier: a
    brace here is a format field, and `[0-9]{4}` would raise IndexError inside the promote
    after the append had committed.

    `06-19-2026` is the case that matters and the one the old CHECK accepted -- ten
    characters, non-blank, and it joins to nothing in gold with every count green. The
    other three are the shapes a length check cannot see either way, kept so a future
    weakening of the pattern (say `[0-9-]+`) has something to fail against. `2026-13-45` is
    INSERTED rather than refused -- the acceptance half of "the gate is strictly stronger",
    which the comment block above explains.

    THE COLUMN IS DECLARED NOT NULL AT CREATE TIME, AND THAT IS A MEASURED LOCAL LIMIT
    rather than a shortcut. Applying the tuple's own
    `ALTER COLUMN quote_date SET NOT NULL` to a table created from a DataFrame fails on
    open-source Delta 3.x -- "Cannot change nullable column to non-nullable" -- while
    Databricks accepts it, which is why the promote issues it there and why F1.4b's live
    tables carry it. So the column starts non-nullable here and all three statements are
    issued in the promote's own order, leaving the CHECK as the only thing that can refuse
    a row."""
    database = f"ptax_check_{tmp_path.name}"
    spark.sql(f"CREATE DATABASE {database} LOCATION '{tmp_path.as_uri()}'")
    table = f"{database}.bronze_ptax"
    try:
        spark.sql(f"CREATE TABLE {table} (quote_date STRING NOT NULL) USING DELTA")
        spark.sql(f"INSERT INTO {table} VALUES ('2026-06-19')")
        applied = [
            statement.format(table=table)
            for statement in table_spec("ptax").constraints
            if "quote_date" in statement
        ]
        assert len(applied) == 3, "the quote_date DDL is NOT NULL, DROP and ADD"
        for statement in applied:
            spark.sql(statement)
        for refused in ("06-19-2026", "2026-6-19", "19/06/2026", "2026-06-1x"):
            with pytest.raises(Exception, match="quote_date_iso_shape"):
                spark.sql(f"INSERT INTO {table} VALUES ('{refused}')")
        spark.sql(f"INSERT INTO {table} VALUES ('2026-13-45')")
        assert sorted(row["quote_date"] for row in spark.table(table).collect()) == [
            "2026-06-19",
            "2026-13-45",
        ], "the CHECK is the shape only; naming a real day is the GATE's added clause"
    finally:
        spark.sql(f"DROP DATABASE {database} CASCADE")


def test_an_undeclared_bcb_field_is_rejected_as_rescued_data_above_every_rule(spark):
    """WHAT A REAL UPSTREAM CHANGE LOOKS LIKE, and it is the only drift this source can
    produce -- the contract declares no drift column of its own, because fabricating one
    would attribute a field to the Banco Central that it never sent.

    `struct_for("ptax")` carries only the five contract columns, `bronze_stream` supplies
    that schema and sets `rescuedDataColumn`, and `dq._reject_reason` ranks
    `rescued_data_present` above every per-table rule. So a field BCB adds is QUARANTINED
    with a reason naming the drift, rather than absorbed or reported as something
    narrower."""
    frame = _frame(spark, [_row()], rescued=['{"cotacaoMedia":"5.14390"}'])
    assert [row[REJECT_COLUMN] for row in evaluate(frame, rules_for(CONTRACT)).collect()] == [
        "rescued_data_present"
    ]
