# tests/test_ptax_source.py
"""Every body below is a REAL PTAX response, captured 2026-08-13 and pasted whole --
`@odata.context` included, because one of the assertions is about what a body does NOT
contain. The only hand-built bodies are the ones that exercise refusals the live series
has never produced, and each says so where it is used."""
from __future__ import annotations

import ast
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from opl.extraction import ptax_source
from opl.extraction.ptax_source import (
    BRASILIA,
    COMPRA_FIELD,
    ENVELOPE_ROWS,
    MAX_PUBLICATION_SPREAD,
    PTAX_ENDPOINT,
    PUBLISHED_FIELD,
    RESPONSE_FIELDS,
    VENDA_FIELD,
    PtaxResponseRefused,
    fetch_series,
    quote_dates,
    quote_url,
    quotes_in,
    sole_quote,
)

_REPO = Path(__file__).resolve().parents[1]
_CONTEXT = (
    '"@odata.context":"https://was-p.bcnet.bcb.gov.br/olinda/servico/PTAX/versao/v1/'
    'odata$metadata#_CotacaoDolarPeriodo"'
)

# The series' FIRST quote date, and the one row in 3.6 years of probing where publication
# and quote date do not fall on the same day.
BODY_1984 = (
    "{" + _CONTEXT + ',"value":[{"cotacaoCompra":2814.00000,"cotacaoVenda":2828.00000,'
    '"dataHoraCotacao":"1984-12-03 11:29:00.0"}]}'
)
BODY_2026_06_19 = (
    "{" + _CONTEXT + ',"value":[{"cotacaoCompra":5.14360,"cotacaoVenda":5.14420,'
    '"dataHoraCotacao":"2026-06-19 13:03:25.555497"}]}'
)
BODY_2026_06_22 = (
    "{" + _CONTEXT + ',"value":[{"cotacaoCompra":5.13890,"cotacaoVenda":5.13950,'
    '"dataHoraCotacao":"2026-06-22 13:06:19.750415"}]}'
)
BODY_2026_07_31 = (
    "{" + _CONTEXT + ',"value":[{"cotacaoCompra":5.07670,"cotacaoVenda":5.07730,'
    '"dataHoraCotacao":"2026-07-31 13:10:31.061071"}]}'
)
# The fan-out with a witness: two publications of one quote, 27 ms apart, agreeing.
BODY_2025_04_23 = (
    "{" + _CONTEXT + ',"value":[{"cotacaoCompra":5.68740,"cotacaoVenda":5.68800,'
    '"dataHoraCotacao":"2025-04-23 13:06:30.416"},{"cotacaoCompra":5.68740,'
    '"cotacaoVenda":5.68800,"dataHoraCotacao":"2025-04-23 13:06:30.443"}]}'
)
# THE RANGE REQUEST THAT SUCCEEDS SILENTLY, captured live from
# `@di='11-28-1984'&@df='11-29-1984'`. Two quote dates, and BOTH publish on 1984-12-03 --
# so attribution is many-to-one, not merely absent. Their rates are IDENTICAL, which is
# what makes this the decisive body: every other check in the module passes on it.
BODY_1984_RANGE = (
    "{" + _CONTEXT + ',"value":[{"cotacaoCompra":2814.00000,"cotacaoVenda":2828.00000,'
    '"dataHoraCotacao":"1984-12-03 11:29:00.0"},{"cotacaoCompra":2814.00000,'
    '"cotacaoVenda":2828.00000,"dataHoraCotacao":"1984-12-03 16:38:00.0"}]}'
)
# A weekend: the API answers 200 with an empty envelope, not an error.
BODY_NO_QUOTE = "{" + _CONTEXT + ',"value":[]}'


def test_the_quote_date_survives_a_publication_five_days_later():
    """THE TEST THAT CLOSES T3, on the one response where the two can be told apart.

    T3 resolves a payment against the most recent quote whose PUBLICATION instant
    precedes the payment's own. That is implementable only if the layer surfaces BOTH the
    publication instant and the quote date -- and the API ships only the first.

    WHY IT CANNOT PASS UNDER THE IMPLEMENTATION THIS GUARDS AGAINST. An extraction that
    reads the quote date off `dataHoraCotacao` answers 1984-12-03; the first assertion is
    exactly that it answers 1984-11-28. The last block says why no cleverness rescues
    that implementation: the quote date does not occur anywhere in the body, in any
    spelling, so the only place it can have come from is the request. On every day this
    phase extracts the two coincide, so the wrong implementation passes a 2026 fixture,
    ships, and is wrong as a rule."""
    quote = sole_quote(quotes_in(BODY_1984, date(1984, 11, 28)))
    assert quote is not None
    assert quote.quote_date == date(1984, 11, 28)
    assert quote.published_at.date() == date(1984, 12, 3)
    assert quote.published_at.date() - quote.quote_date == timedelta(days=5)
    assert quote.published_raw == "1984-12-03 11:29:00.0"
    assert str(quote.venda) == "2828.00000"
    for spelling in ("1984-11-28", "11-28-1984", "28-11-1984", "28/11/1984", "1984/11/28"):
        assert spelling not in BODY_1984, (
            f"{spelling!r} is in the response after all, so the quote date COULD have "
            "been read off the body and this test no longer separates the two "
            "implementations"
        )
    assert "11-28-1984" in quote_url(date(1984, 11, 28))


def test_a_range_request_is_not_offered_because_its_rows_cannot_be_attributed():
    """The mechanism behind the test above, asserted rather than left to a docstring.

    The quote date is carried from the request, so a request that names more than one
    quote date carries nothing usable. `quote_url` therefore takes a single date and puts
    it in both bounds, and a span is a sequence of those rather than one wide call."""
    url = quote_url(date(2026, 6, 19))
    assert url.count("06-19-2026") == 2
    assert url.split("@di=")[1].split("&")[0] == url.split("@df=")[1].split("&")[0]
    assert not hasattr(ptax_source, "range_url")


def test_the_date_format_is_month_first_in_single_quotes_and_is_not_iso():
    """`MM-DD-YYYY`, quoted, which is the spelling everyone who assumes gets wrong. The
    31st is deliberate: it is the one day of the month where a day-first reading cannot
    silently produce a valid date, so a swapped format fails here rather than in June."""
    url = quote_url(date(2026, 1, 31))
    assert "@di='01-31-2026'" in url
    assert "@df='01-31-2026'" in url
    assert "2026-01-31" not in url
    assert "31-01-2026" not in url
    assert url.endswith("&$format=json")


def _f0_ptax_url() -> str:
    """The literal `scripts/validate_cnpj_snapshots.py` has carried since F0, read out of
    that file rather than re-typed here. Parsed, never executed: the script imports `opl`
    at module scope and running it would make this test a network test."""
    script = _REPO / "scripts" / "validate_cnpj_snapshots.py"
    tree = ast.parse(script.read_text(encoding="utf-8"), filename=script.name)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "PTAX" for target in node.targets
        ):
            return str(ast.literal_eval(node.value))
    raise AssertionError("scripts/validate_cnpj_snapshots.py no longer defines PTAX")


def test_the_endpoint_and_its_date_format_are_f0s_and_not_a_second_spelling():
    """A SECOND SPELLING OF AN ENDPOINT IS THE DEFECT CLASS THIS REPOSITORY POLICES
    HARDEST, so this compares against F0's own literal instead of re-stating it. The date
    format is compared the same way: F0's URL asks for 02 January 2026, and this module
    asked for the same day must spell it identically."""
    f0 = _f0_ptax_url()
    assert f0.startswith(PTAX_ENDPOINT), (
        f"F0 carries {f0!r}; this module spells the endpoint {PTAX_ENDPOINT!r}"
    )
    assert "@di='01-02-2026'" in f0
    assert "@di='01-02-2026'" in quote_url(date(2026, 1, 2))


def test_the_trailing_zero_the_api_published_survives_the_parse():
    """`5.07730` is five decimals wide in the bulletin and `fx_rate` is `decimal(18,5)`
    (T4). A float round trip loses the fifth digit, and the loss is invisible: 5.0773 and
    5.07730 are the same number and different strings, and only the string is checkable
    against BCB. The wrong answer is computed here rather than described."""
    quote = sole_quote(quotes_in(BODY_2026_07_31, date(2026, 7, 31)))
    assert quote is not None
    assert quote.venda == Decimal("5.07730")
    assert str(quote.venda) == "5.07730"
    assert str(quote.compra) == "5.07670"
    through_a_float = json.loads(BODY_2026_07_31)["value"][0][VENDA_FIELD]
    assert str(through_a_float) == "5.0773"


def test_the_publication_instant_is_read_as_brasilia_time():
    """T3's zone ruling, pinned. Publication at 13:03 BRT is 16:03 UTC; read as UTC it
    would be 10:03 in Brasilia, three hours before the bulletin it comes from exists.
    BRT is also the fail-safe direction -- it places publication later, so a wrong zone
    makes a payment fall back to an older rate rather than use an unpublished one."""
    quote = sole_quote(quotes_in(BODY_2026_06_19, date(2026, 6, 19)))
    assert quote is not None
    assert quote.published_at.utcoffset() == timedelta(hours=-3)
    assert quote.published_at == datetime(2026, 6, 19, 13, 3, 25, 555497, tzinfo=BRASILIA)
    assert quote.published_at.astimezone(UTC).hour == 16
    assert quote.published_raw == "2026-06-19 13:03:25.555497", (
        "bronze lands source bytes; the parsed instant is this module's reading and must "
        "not be what gets written as if BCB had sent it"
    )


def test_two_publications_of_one_quote_reduce_to_the_earlier_stamp():
    """The fan-out that HAS a witness: 2025-04-23, two rows, 27 ms apart, agreeing on
    both rates. Reduced per response, where one URL is one quote date and one endpoint is
    one currency, so the reduce key is the request itself.

    THE EARLIER STAMP IS KEPT, AND THAT IS THE ONE CHOICE HERE THAT CHANGES T3'S ANSWER.
    The rates AGREE -- `sole_quote` has already refused the group otherwise -- so BCB had
    published this rate at `.416` and the second row re-publishes a number that was
    already knowable. Keeping `.443` would deny a payment that fell between the two stamps
    a rate it could already have used and send it back to the previous business day at a
    DIFFERENT rate, which is precisely the model plan T3 retracted.

    The wrong answer is COMPUTED here rather than described: the last two assertions are
    the same payment instant read against both reduces, and they disagree."""
    quotes = quotes_in(BODY_2025_04_23, date(2025, 4, 23))
    assert len(quotes) == 2
    assert quotes[1].published_at - quotes[0].published_at == timedelta(milliseconds=27)
    assert quotes[0].venda == quotes[1].venda and quotes[0].compra == quotes[1].compra
    one = sole_quote(quotes)
    assert one is not None
    assert one.published_raw == "2025-04-23 13:06:30.416"
    assert str(one.venda) == "5.68800"
    assert one.quote_date == date(2025, 4, 23)
    paid_at = datetime(2025, 4, 23, 13, 6, 30, 430000, tzinfo=BRASILIA)
    assert one.published_at <= paid_at, (
        "the reduce kept a publication instant LATER than a payment the rate had already "
        "been published for, so T3 denies that payment a rate BCB had already released "
        "and falls back to the previous business day"
    )
    assert max(quote.published_at for quote in quotes) > paid_at, (
        "the max() this replaces, evaluated rather than described: under it the same "
        "payment sees no quote for its own date at all"
    )


def test_the_reduce_is_per_response_and_is_not_the_one_the_landed_table_needs():
    """THE BUILD-BLOCKER THIS COMMIT CLOSES. `sole_quote` reduces ONE response, and the
    first version of its docstring presented that as REPLACING the reduce gold does before
    the FX join. It does not: bronze is written `mode("append")` (`opl.bronze.promote`), so
    a second extraction over the same span lands a second row for every
    `(currency, quote_date)` already reduced here -- rows this function structurally cannot
    see, being invoked once per response. A re-run is an ordinary event, so the request-
    layer reduce is an ADDITION and the whole-table reduce stays downstream.

    THREE ASSERTIONS, BECAUSE THE CLAIM CAN REVERT THREE WAYS. The module could grow a
    whole-table reduce; `sole_quote` could quietly accept rows from several quote dates and
    thereby become one; and the docstrings could stop telling the next implementer that the
    landed table still needs reducing. The last is pinned to the words on purpose -- the
    defect being closed here WAS a docstring, so a test that only checked behaviour would
    have passed against the version that shipped the wrong claim."""
    for name in (
        "reduce_quotes",
        "dedupe_quotes",
        "latest_per_quote_date",
        "sole_quote_per_date",
        "reduce_series",
        "unique_quotes",
    ):
        assert not hasattr(ptax_source, name), (
            f"{name} implies a whole-table reduce lives in this module. It cannot: this "
            "layer never holds two responses at once, and bronze appends across runs"
        )
    friday = quotes_in(BODY_2026_06_19, date(2026, 6, 19))
    monday = quotes_in(BODY_2026_06_22, date(2026, 6, 22))
    with pytest.raises(PtaxResponseRefused, match="quote dates"):
        sole_quote(friday + monday)
    said = f"{ptax_source.__doc__}\n{sole_quote.__doc__}"
    assert 'mode("append")' in said and "MUST STILL REDUCE" in said, (
        "the per-response limit and the downstream obligation are the whole of the "
        "correction; a reader who cannot find them here will re-derive the wrong claim"
    )
    assert "rather than in gold before the FX join" not in said, (
        "that is the retracted claim, restored. The request-layer reduce is an addition "
        "to the gold-side one, never a replacement for it"
    )


def test_publications_too_far_apart_to_be_one_quote_are_refused():
    """THE BOUND WITHOUT WHICH THE REDUCE ASSERTS NOTHING, and the body is the real range
    response for 1984-11-28..1984-11-29.

    WHY THIS BODY IS THE DECISIVE ONE. Both quote dates publish on 1984-12-03, so a range
    response cannot be attributed even in principle -- the collision is many-to-one, not a
    missing column. And their rates are IDENTICAL, so the disagreement branch does not fire;
    `quotes_in` carries the one REQUESTED quote date onto both rows, so the mixed-date
    refusal cannot fire either. Every other check in this module passes on this body. Before
    the bound, `sole_quote` reduced it to a single quote whose publication instant was five
    hours away from the other row's, and the extraction landed a rate under a quote date
    that was never asked for.

    The bound's two sides are pinned rather than trusted, because a number chosen from one
    measurement can be widened by anyone: 27 ms is the widest re-publication of a single
    quote in 42 years, and six minutes is the tightest gap between two DISTINCT quotes
    (1996-04-10 published 18:36, 1996-04-11 published 18:30 -- the later date first)."""
    assert timedelta(milliseconds=27) < MAX_PUBLICATION_SPREAD < timedelta(minutes=6), (
        "the bound must not fire on the series' own widest re-publication, and must not "
        "admit the series' closest pair of distinct quotes"
    )
    leaked = quotes_in(BODY_1984_RANGE, date(1984, 11, 28))
    assert len(leaked) == 2
    assert leaked[0].venda == leaked[1].venda, "identical rates: no disagreement to catch"
    assert {quote.quote_date for quote in leaked} == {date(1984, 11, 28)}, (
        "one requested quote date on both rows, so the mixed-date refusal cannot see this"
    )
    assert leaked[1].published_at - leaked[0].published_at == timedelta(hours=5, minutes=9)
    with pytest.raises(PtaxResponseRefused) as refusal:
        sole_quote(leaked)
    message = str(refusal.value)
    assert "5:09:00" in message and "11-28-1984" in message
    assert "1984-12-03 16:38:00.0" in message, (
        "the refusal has to carry both stamps: a spread without them leaves the reader "
        "unable to tell a range request from a genuine re-publication"
    )


def test_two_rows_that_disagree_on_the_rate_are_refused_rather_than_reduced():
    """THE BRANCH WITH NO WITNESS IN THE SERIES, which is why the body is the real
    2025-04-23 response with one digit changed -- 903 rows over 3.6 years contain no
    disagreement, so the only way to reach this at all is to build one. Named in
    `sole_quote`'s docstring as shipping unexercised by the source.

    The refusal has to carry BOTH rates: a message saying only that rows disagreed leaves
    the reader unable to tell a 27 ms duplicate from a genuine correction."""
    disagreeing = BODY_2025_04_23.replace("5.68800", "5.68900", 1)
    with pytest.raises(PtaxResponseRefused) as refusal:
        sole_quote(quotes_in(disagreeing, date(2025, 4, 23)))
    message = str(refusal.value)
    assert "5.68900" in message and "5.68800" in message
    assert "04-23-2025" in message


def test_a_day_the_series_has_no_quote_for_is_not_a_refusal():
    """Saturday 2026-06-20 -- one of the four load-bearing dates in this project, all of
    which are Saturdays. The API answers 200 with an empty envelope, and T3 resolves such
    a day by falling back over the series, so an absence must reach the caller as an
    absence rather than as an error or as a NULL-rate row."""
    assert quotes_in(BODY_NO_QUOTE, date(2026, 6, 20)) == ()
    assert sole_quote(()) is None


@pytest.mark.parametrize("field", RESPONSE_FIELDS)
def test_a_row_missing_any_of_the_three_fields_is_refused(field: str):
    """All three are load-bearing: two are the conversion and the third is what T3
    compares against. A renamed field arrives here as a missing one, which is the check
    that makes accepting UNKNOWN extra fields safe."""
    row = {
        COMPRA_FIELD: 5.14360,
        VENDA_FIELD: 5.14420,
        PUBLISHED_FIELD: "2026-06-19 13:03:25.555497",
    }
    del row[field]
    body = json.dumps({ENVELOPE_ROWS: [row]})
    with pytest.raises(PtaxResponseRefused) as refusal:
        quotes_in(body, date(2026, 6, 19))
    assert field in str(refusal.value)


@pytest.mark.parametrize(
    "body, expected",
    [
        ("<html>service temporarily unavailable</html>", "not JSON"),
        ('{"error":{"code":"500"}}', "member"),
        ("[]", "member"),
        ('{"' + ENVELOPE_ROWS + '":{"cotacaoVenda":5.14420}}', "rather than a"),
        ('{"' + ENVELOPE_ROWS + '":["cotacaoVenda"]}', "where a quote row was expected"),
    ],
    ids=["html-interstitial", "odata-error", "bare-list", "object-not-list", "row-not-object"],
)
def test_a_body_that_is_not_the_odata_envelope_is_refused(body: str, expected: str):
    """The 200-carrying-an-interstitial case is the one that matters: it parses as zero
    rows under a forgiving reader, and zero rows is exactly what a weekend looks like. A
    day the API failed on would then be indistinguishable from a day with no quote, and
    T3 would fall back over a hole nobody could see."""
    with pytest.raises(PtaxResponseRefused) as refusal:
        quotes_in(body, date(2026, 6, 19))
    assert expected in str(refusal.value)
    assert "06-19-2026" in str(refusal.value)


@pytest.mark.parametrize(
    "stamp", ["19/06/2026 13:03", "2026-06-19T13:03:25.555497", "", 20260619]
)
def test_a_publication_instant_that_cannot_be_read_is_refused(stamp: object):
    """An unreadable stamp is not a row with one missing column: T3 compares this instant
    against the payment's own, so a row without it cannot take part in the join at all.
    Landing it and resolving on the quote date instead is precisely the silent degradation
    the ruling forbids."""
    body = json.dumps(
        {
            ENVELOPE_ROWS: [
                {COMPRA_FIELD: 5.14360, VENDA_FIELD: 5.14420, PUBLISHED_FIELD: stamp}
            ]
        }
    )
    with pytest.raises(PtaxResponseRefused) as refusal:
        quotes_in(body, date(2026, 6, 19))
    assert PUBLISHED_FIELD in str(refusal.value)


@pytest.mark.parametrize("rate", [None, True, "not a rate", {}])
def test_a_rate_that_is_not_a_number_is_refused(rate: object):
    """`True` is in this list by name. It is an `int` to Python, so an unguarded
    `Decimal(value)` would land a rate of exactly 1 -- the one wrong value an FX rate can
    wear while looking like a deliberate identity conversion."""
    body = json.dumps(
        {
            ENVELOPE_ROWS: [
                {
                    COMPRA_FIELD: rate,
                    VENDA_FIELD: 5.14420,
                    PUBLISHED_FIELD: "2026-06-19 13:03:25.555497",
                }
            ]
        }
    )
    with pytest.raises(PtaxResponseRefused) as refusal:
        quotes_in(body, date(2026, 6, 19))
    assert COMPRA_FIELD in str(refusal.value)


def test_a_quote_published_before_the_date_it_is_a_quote_for_is_refused():
    """FREE, AND TRUE ON EVERY LIVE ROW IN THIS FILE -- asserted rather than claimed, first
    block below. The series only ever publishes on or AFTER the quote date, and the 1984 row
    is the extreme: five days late, which is the whole reason this module carries two fields.

    WHAT IT CATCHES IS A RANGE REQUEST STAMPED WITH THE LATER OF ITS BOUNDS. Rows carry no
    quote date, so a wide call must stamp one onto all of them; stamp `last` and every row
    belonging to an earlier quote date arrives published before the date it claims. The
    second block is exactly that shape, built from a real body by asking for a date one day
    after its publication instant.

    The other direction -- stamping `first` -- keeps the dates consistent and is caught by
    the publication-spread bound instead, since the closest two distinct quotes in 42 years
    publish six minutes apart. Neither guard covers both, which is why both exist."""
    for body, day in (
        (BODY_1984, date(1984, 11, 28)),
        (BODY_2026_06_19, date(2026, 6, 19)),
        (BODY_2026_06_22, date(2026, 6, 22)),
        (BODY_2026_07_31, date(2026, 7, 31)),
        (BODY_2025_04_23, date(2025, 4, 23)),
    ):
        for quote in quotes_in(body, day):
            assert quote.published_at.date() >= quote.quote_date, (
                f"{body} publishes before its own quote date, so this guard is not free"
            )
    with pytest.raises(PtaxResponseRefused) as refusal:
        quotes_in(BODY_1984, date(1984, 12, 4))
    message = str(refusal.value)
    assert "1984-12-03 11:29:00.0" in message and "1984-12-04" in message


def _one_row_body(**overrides: object) -> str:
    """A body carrying one row of the 2026-06-19 response with fields overridden."""
    row: dict[str, object] = {
        COMPRA_FIELD: 5.14360,
        VENDA_FIELD: 5.14420,
        PUBLISHED_FIELD: "2026-06-19 13:03:25.555497",
    }
    row.update(overrides)
    return json.dumps({ENVELOPE_ROWS: [row]})


@pytest.mark.parametrize(
    "rate", ["NaN", "-NaN", "sNaN", "Infinity", "-Infinity", "0", "0.00000", "-5.14420"]
)
def test_a_rate_that_is_not_a_finite_positive_price_is_refused(rate: str):
    """`Decimal("NaN")` AND `Decimal("Infinity")` RAISE NOTHING, which is `Decimal(True)`'s
    argument one step stronger -- and `True` was already refused by name here while these
    were not. A non-finite rate landed as its own text, cast to NULL in Spark, and T3
    clause 3 turns a NULL rate into a refusal three layers and one job run downstream.

    ZERO IS THE ONE THAT HIDES: `amount_brl` is `amount_original * venda`, so a zero venda
    converts every USD payment on its date to exactly 0.00 while the row count, the null
    count and the rejection count all report clean. Measured: no row in the series since
    1984-11-28 carries a compra or venda at or below zero, so none of these refuses a real
    row. `compra > venda` and an absurd magnitude are NOT refused, and the module says
    which sixteen rows and which two extremes forbid it."""
    with pytest.raises(PtaxResponseRefused) as refusal:
        quotes_in(_one_row_body(**{COMPRA_FIELD: rate}), date(2026, 6, 19))
    assert COMPRA_FIELD in str(refusal.value)
    assert "06-19-2026" in str(refusal.value)


def test_two_identical_nan_rows_are_refused_as_a_rate_and_not_as_a_disagreement():
    """FAILING SAFE FOR THE WRONG REASON, which is why this is its own test.

    `Decimal("NaN") != Decimal("NaN")`, so before the finiteness check two BYTE-IDENTICAL
    NaN rows built two distinct rate pairs and took the DISAGREEMENT branch -- the
    extraction stopped, which is the right verdict, and told the reader that two rows
    contradicted each other on a rate they both spell the same way. A refusal that names
    the wrong cause costs the next reader the whole diagnosis."""
    row = f'{{"{COMPRA_FIELD}":"NaN","{VENDA_FIELD}":"NaN",'
    stamp = f'"{PUBLISHED_FIELD}":"2026-06-19 13:03:25.555497"}}'
    body = "{" + _CONTEXT + f',"value":[{row}{stamp},{row}{stamp}]}}'
    assert body.count("NaN") == 4, "two rows, byte-identical, both rates unreadable"
    with pytest.raises(PtaxResponseRefused) as refusal:
        sole_quote(quotes_in(body, date(2026, 6, 19)))
    message = str(refusal.value)
    assert COMPRA_FIELD in message and "finite" in message
    assert "DISAGREE" not in message, (
        "these rows do not disagree -- they are the same bytes twice, and the rate is "
        "unreadable in both"
    )


def test_the_series_is_fetched_one_quote_date_at_a_time():
    """The loop is the price of attribution, and this is what it costs: one request per
    calendar day, weekends included, each naming a single quote date in both bounds.

    It also shows the pair T2's window rests on surviving the layer as two DIFFERENT
    rates -- 5.14420 on Friday and 5.13950 on Monday -- which is what makes two payments
    on one calendar day resolve differently either side of a publication instant."""
    series = {
        date(2026, 6, 19): BODY_2026_06_19,
        date(2026, 6, 20): BODY_NO_QUOTE,
        date(2026, 6, 21): BODY_NO_QUOTE,
        date(2026, 6, 22): BODY_2026_06_22,
    }
    bodies = {quote_url(day): body for day, body in series.items()}
    asked: list[str] = []

    def fake_fetch(url: str) -> str:
        asked.append(url)
        return bodies[url]

    quotes = fetch_series(date(2026, 6, 19), date(2026, 6, 22), fake_fetch)
    assert len(asked) == len(series)
    for url in asked:
        assert url.split("@di=")[1].split("&")[0] == url.split("@df=")[1].split("&")[0]
    assert [quote.quote_date for quote in quotes] == [date(2026, 6, 19), date(2026, 6, 22)]
    assert [str(quote.venda) for quote in quotes] == ["5.14420", "5.13950"]
    assert {quote.currency for quote in quotes} == {"USD"}


def test_an_inverted_span_is_refused_rather_than_landing_an_empty_series():
    """A span that ends before it starts yields no days, so the extraction would land
    zero quotes and every count downstream would report it clean."""
    with pytest.raises(ValueError):
        list(quote_dates(date(2026, 8, 1), date(2026, 6, 3)))
    assert list(quote_dates(date(2026, 6, 3), date(2026, 6, 3))) == [date(2026, 6, 3)]


def test_a_span_where_no_day_answers_a_quote_is_refused():
    """THE LIKELY VERSION OF THE TEST ABOVE, which was the one left open. An inverted span
    is a caller's typo and was refused; every request answering nothing is an outage, or
    worse, and returned `()` raising nothing -- landing an empty series that every
    downstream count reports as clean.

    IT IS NOT ONLY AN OUTAGE, and that is why it is a refusal. MEASURED: `@di='2026-06-19'`
    -- an ISO date where this endpoint wants `MM-DD-YYYY`, the format this repository polices
    hardest -- answers HTTP 200 with `"value":[]`. So the malformed request fails by wearing
    a Saturday's exact shape on every day of the span. No single response can tell them
    apart, which is why the check cannot live in `quotes_in`.

    A weekend INSIDE a span stays an absence rather than a refusal: the second block is the
    boundary, one real quote carrying the whole span."""
    weekend = {
        quote_url(date(2026, 6, 20)): BODY_NO_QUOTE,
        quote_url(date(2026, 6, 21)): BODY_NO_QUOTE,
    }
    asked: list[str] = []

    def only_empty(url: str) -> str:
        asked.append(url)
        return weekend[url]

    with pytest.raises(PtaxResponseRefused) as refusal:
        fetch_series(date(2026, 6, 20), date(2026, 6, 21), only_empty)
    assert "no quote at all" in str(refusal.value)
    assert "2026-06-20" in str(refusal.value) and "2026-06-21" in str(refusal.value)
    assert len(asked) == 2, "every day was asked before the span was refused"

    bodies = {**weekend, quote_url(date(2026, 6, 19)): BODY_2026_06_19}
    survived = fetch_series(date(2026, 6, 19), date(2026, 6, 21), lambda url: bodies[url])
    assert [quote.quote_date for quote in survived] == [date(2026, 6, 19)], (
        "one quote is enough: the absences inside a span are still absences"
    )


def test_an_empty_body_is_refused_because_zero_rows_is_what_a_weekend_looks_like():
    """THE OBLIGATION THE `Fetch` CONTRACT PUTS ON ITS CALLER, enforced as far as it can be
    from inside a module that never sees a status code.

    An empty body is `response.text` on a 204, on a dropped connection, and on an error
    whose status nobody checked. Olinda's own error body is refused by the same path for a
    different reason: measured, every error shape it serves is
    `/*{ "codigo": ..., "mensagem": ... }*/`, and the `/*` wrapper is not JSON."""
    for body in ("", "   ", "\n"):
        with pytest.raises(PtaxResponseRefused, match="EMPTY body"):
            quotes_in(body, date(2026, 6, 19))


def test_nothing_here_can_send_a_credential_because_nothing_here_makes_the_request():
    """THE API IS PUBLIC AND UNAUTHENTICATED, asserted over the AST rather than the text:
    the module docstring SAYS the words "token" and "credential", and a substring check
    would punish it for explaining the very thing it is guarding.

    The transport is injected, so this module imports no HTTP client and has nowhere to
    attach a header. The same walk covers the pyspark ban -- this layer runs on the
    extraction host, where pyspark is an optional extra that is usually absent."""
    url = quote_url(date(2026, 6, 19))
    assert not any(word in url.lower() for word in ("token", "auth", "secret", "password"))
    tree = ast.parse(Path(ptax_source.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert imported.isdisjoint({"requests", "urllib", "http", "pyspark"}), (
        f"ptax_source imports {sorted(imported)}; it builds requests and reads responses, "
        "and executing them is the caller's"
    )
