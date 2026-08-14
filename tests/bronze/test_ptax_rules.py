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
    restatement of what the extraction layer already refuses -- see the contract."""
    produced = {name for name, _ in rules_for(CONTRACT)}
    assert {f"null_or_empty_{column}" for column in REQUIRED_COLUMNS} <= produced
    assert len(REQUIRED_COLUMNS) == len(COLUMNS) == 5


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
        # Shape-valid, names no day. The registry's length CHECK accepts it.
        "2026-13-45",
        # The other direction: a real date in a spelling nothing downstream parses.
        "19/06/2026",
        "2026-6-19",
    ],
)
def test_a_quote_date_that_is_not_an_iso_day_is_rejected(spark, quote_date):
    """THE ONE RULE HERE THAT IS NOT NEAR-TAUTOLOGICAL.

    `06-19-2026` is the value the phase's own API format produces, and it is the reason
    the registry's `length(trim(quote_date)) = 10` CHECK is not enough on its own: that
    constraint accepts this string, and it runs at the promote, AFTER the append has
    committed. This runs in the gate, before anything reaches bronze."""
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
        # The series' fractional-second width is NOT stable: 1 digit in 1984, 3 in 2025,
        # 6 in 2026. A pinned pattern works on this phase's range and rejects the series,
        # which is why the rule is format-agnostic.
        "1984-12-03 11:29:00.0",
        "2025-04-23 13:02:31.416",
        "2026-06-19 13:03:25.555497",
        "2026-06-19 13:03:25",
    ],
)
def test_every_fractional_second_width_the_series_uses_is_accepted(spark, published):
    """GUARDS THE GUARD, against the version of this rule that would look tighter.

    A rule pinned to `yyyy-MM-dd HH:mm:ss.SSSSSS` accepts every 2026 row and rejects
    1984-12-03 and 2025-04-23 -- both of which are real rows this endpoint returns. It
    would pass every test written from this phase's own window and reject the series.

    THIS IS THE HALF THE TIGHTENING HAD TO KEEP, and it is why the fix is a shape whose
    fractional group is `{1,6}`-or-absent rather than a `to_timestamp` format string. The
    four values here are the three widths the series is known to use plus no fraction at
    all, which no observed row has -- present so an absence is not a refusal, exactly as
    `ptax_source.PUBLICATION_FORMATS` carries the same second spelling for the same
    reason."""
    assert _reasons(spark, [_row(data_hora_cotacao=published)]) == [None]


def test_a_replacement_character_in_any_column_is_rejected(spark):
    """The encoding check, folded over all five columns.

    LIVE RATHER THAN INHERITED, for the reason it is live on payments: the serialiser
    returns TEXT, and a writer that did not encode UTF-8 explicitly hands Java bytes it
    cannot map -- and Java's decoder substitutes U+FFFD SILENTLY where Python raises.
    That character is then the only in-band evidence that the bytes on disk are not the
    bytes that were serialised.

    `currency` is the column carrying it here precisely because no earlier rule looks at
    its content: a value that is non-blank reaches the encoding check, and this is what
    says the fold covers a column no other rule inspects."""
    assert _reasons(spark, [_row(currency=f"US{_REPLACEMENT_CHAR}")]) == [
        "encoding_replacement_char"
    ]


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
# constraint -- "that one runs at the promote, AFTER the append has committed, and length
# alone accepts `06-19-2026`" -- and until F-API's fix pass nothing anywhere exercised the
# second half of that sentence. It was true: the CHECK was named `quote_date_iso_shape`
# and spelled `length(trim(quote_date)) = 10`, so the constraint named for the ISO shape
# admitted the one non-ISO spelling this phase invites. `tests/bronze/test_registry.py`
# pins the DDL string; this is the behavioural half, against a real Delta transaction log,
# because a CHECK is a claim about what the engine will refuse and not about what the
# string says.


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
    weakening of the pattern (say `[0-9-]+`) has something to fail against.

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
        assert [row["quote_date"] for row in spark.table(table).collect()] == [
            "2026-06-19"
        ], "the ISO value is the only one that landed"
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
