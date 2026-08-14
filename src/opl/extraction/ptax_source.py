# src/opl/extraction/ptax_source.py
"""PTAX source knowledge: the one shape this API can be asked in, and the validation of
what it answers. Builds the request and reads the response; the transport is the
caller's.

THE API IS PUBLIC AND UNAUTHENTICATED. BCB/Olinda's OData service takes no token, no
header, no basic-auth pair and no credential of any kind -- unlike the RFB share in
`cnpj_source.py`, whose public share id at least LOOKS like one. Nothing here should ever
grow a secret, and the module cannot quietly acquire the ability to send one: it imports
no HTTP client at all, which `tests/test_ptax_source.py` asserts over the AST.

THE ENDPOINT IS NOT RE-DERIVED. It is the URL `scripts/validate_cnpj_snapshots.py:28-32`
has carried since F0, including the `MM-DD-YYYY`-in-single-quotes date format, which is
not ISO and is got wrong by everyone who assumes. A second spelling of an endpoint is the
defect class this repository polices hardest, so the test does not re-type the format
either -- it reads that script's own literal and compares.

THE QUOTE DATE IS NOT IN THE RESPONSE, AND THAT IS THIS MODULE'S ENTIRE SHAPE. The API
ships exactly three fields per row -- `cotacaoCompra`, `cotacaoVenda`, `dataHoraCotacao`
-- and `dataHoraCotacao` is a PUBLICATION instant, not the quote's date. The two coincide
on every day this phase extracts and DO NOT coincide in 1984, where the 1984-11-28 quote
comes back stamped `1984-12-03 11:29:00.0` and the string `1984-11-28` appears nowhere in
the body. Two consequences, and they are the reason this file exists:

  * the requested quote date is CARRIED alongside the response and never derived from
    `dataHoraCotacao`; and
  * a request may therefore name only ONE quote date. A range answers with rows whose
    quote dates cannot be told apart, so `quote_url` takes a single date and
    `fetch_series` is a loop over days rather than one wide call.

Plan T3 -- the rate for a payment is the most recent quote whose PUBLICATION instant
precedes the payment's own -- is implementable only if both survive to bronze. A layer
returning the API's own three fields makes T3 degrade silently into the calendar-day
comparison it forbids, and it would look correct on every day this phase extracts.

THE REDUCE IN `sole_quote` IS PER RESPONSE AND DOES NOT SURVIVE THE LANDING. It sees one
response -- one currency, one quote date -- and reduces the rows in it. That is an
ADDITION to the reduce a consumer of the landed table must do, and never a replacement
for it: bronze is written `mode("append")` (`opl.bronze.promote`), so a second extraction
over the same span lands a SECOND row for every `(currency, quote_date)` this function
already reduced to one, and it cannot see those rows because it is invoked once per
response and never over the table. A RE-RUN IS AN ORDINARY EVENT -- a repair, a widened
window, a retried task -- so any consumer joining against this data MUST reduce over the
landed table itself first. Nothing in this module does that, and nothing in this module
can.

DECIMAL, FROM THE RAW TEXT. `json.loads` turns the API's `5.07730` into the float
`5.0773`, and `str()` on that drops the trailing zero -- so a rate carried through a
float is a different string from the one BCB published. Everything here parses with
`parse_float=Decimal`.

NO pyspark, and no `requests`. This module runs on the extraction host, where pyspark is
an optional extra that is usually absent, and its I/O is injected so every test of it is
hermetic.
"""
from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

# Read from scripts/validate_cnpj_snapshots.py:28-32, not re-derived.
PTAX_ENDPOINT = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@di,dataFinalCotacao=@df)"
)
PTAX_DATE_FORMAT = "%m-%d-%Y"  # NOT ISO, and the single quotes below are part of it.

# `CotacaoDolarPeriodo` IS the currency: one endpoint, one pair. The quote is BRL per
# USD -- source knowledge rather than a contract decision, so it is stated here and both
# sides are named, because the direction is what an FX defect gets wrong silently:
# `amount_brl` is `amount_original * venda`, and dividing instead lands a number that is
# the right shape, plausible in magnitude, and wrong by a factor of ~26.
QUOTED_CURRENCY = "USD"
BASE_CURRENCY = "BRL"

# T3: `dataHoraCotacao` IS READ AS BRASILIA TIME, and the argument is measured rather
# than assumed -- every 2026 row publishes at ~13:0x, the PTAX bulletin hour in Brasilia,
# and read as UTC that would be ~10:0x local, before the bulletin exists. It is also the
# fail-safe direction: it places publication three hours LATER, so a wrong zone makes a
# payment fall back to an older rate rather than use one not yet published.
#
# A FIXED OFFSET, NOT `zoneinfo`. Brazil has had no DST since 2019 (Decreto 9.772/2019)
# and this phase extracts 2026 only, so the two agree over every row that will be landed.
# `zoneinfo` would require the `tzdata` package wherever the wheel runs -- Windows ships
# no system tz database -- and `pyproject.toml:18-29` records how little room the
# serverless install budget has. The limit, stated rather than left to be discovered: a
# publication instant inside a pre-2019 Brazilian DST window is an hour late here. The
# 1984-12-03 stamp in the tests is not one of them; tzdata puts that day at UTC-03:00.
BRASILIA = timezone(timedelta(hours=-3))

COMPRA_FIELD = "cotacaoCompra"
VENDA_FIELD = "cotacaoVenda"
PUBLISHED_FIELD = "dataHoraCotacao"
RESPONSE_FIELDS = (COMPRA_FIELD, VENDA_FIELD, PUBLISHED_FIELD)
ENVELOPE_ROWS = "value"

# The API's fractional seconds are 1 digit in 1984 (`.0`), 3 in 2025 (`.416`) and 6 in
# 2026 (`.555497`), and `%f` takes 1 to 6. The second spelling is for a stamp with no
# fraction at all, which no observed row has: it is here so an absence is not a refusal.
PUBLICATION_FORMATS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")

# A URL in, a response body out. Injected rather than imported so this module executes
# no I/O: `requests 2.32.2` is in the Databricks serverless base environment (measured on
# run 1112844532335593), and the timeout, retry and status policy are the caller's.
Fetch = Callable[[str], str]


class PtaxResponseRefused(Exception):
    """A response could not be trusted to mean what the caller will do with it.

    Every refusal below names the request that produced the body, because a PTAX row
    wrong in the fifth decimal is a payment converted at a rate nobody can trace back to
    a bulletin."""


@dataclass(frozen=True)
class PtaxQuote:
    """One PTAX quote, with its quote date CARRIED and its publication instant SURVIVING.

    `quote_date` is the date the request filtered on. It is not in the response and is
    never derived from `published_at`: the two differ by five days in 1984 (module
    docstring), so deriving would be right for this phase and wrong as a rule.

    `published_raw` is the API's own `dataHoraCotacao` string, kept beside the parsed
    instant because bronze lands source bytes -- `str(published_at)` would land this
    module's zone decision as though BCB had sent it.

    `compra`/`venda` keep the source's own names so a landed column maps back to the
    response field it came from. Gold's `fx_rate` reads `venda` (plan T4)."""

    quote_date: date
    currency: str
    published_at: datetime
    published_raw: str
    compra: Decimal
    venda: Decimal


def quote_url(quote_date: date) -> str:
    """The URL for exactly ONE quote date, which is the only shape whose rows can be
    attributed to a date at all.

    `@di` and `@df` are deliberately the same date. A wider window answers with rows
    carrying no quote date of their own, and the only way to put one back would be to
    read it off `dataHoraCotacao` -- correct for 2026, wrong in 1984, and wrong as a
    rule."""
    asked = quote_date.strftime(PTAX_DATE_FORMAT)
    return f"{PTAX_ENDPOINT}?@di='{asked}'&@df='{asked}'&$format=json"


def _envelope_rows(body: str, quote_date: date) -> list[object]:
    """The OData envelope's row list, or a refusal saying what arrived instead."""
    url = quote_url(quote_date)
    try:
        parsed = json.loads(body, parse_float=Decimal)
    except json.JSONDecodeError as exc:
        raise PtaxResponseRefused(
            f"{url} answered something that is not JSON ({exc}). An HTML interstitial "
            "carrying a 200 would parse as no rows at all, so this is refused rather "
            "than read as a day the series simply has no quote for"
        ) from exc
    if not isinstance(parsed, dict) or ENVELOPE_ROWS not in parsed:
        arrived = sorted(parsed) if isinstance(parsed, dict) else type(parsed).__name__
        raise PtaxResponseRefused(
            f"{url} answered JSON with no {ENVELOPE_ROWS!r} member ({arrived}), so it is "
            "not the OData envelope this endpoint returns"
        )
    rows = parsed[ENVELOPE_ROWS]
    if not isinstance(rows, list):
        raise PtaxResponseRefused(
            f"{url} answered {ENVELOPE_ROWS!r} as {type(rows).__name__} rather than a "
            "list of rows, so nothing here can be read as a quote"
        )
    return rows


def _rate(row: dict[str, object], field: str, quote_date: date) -> Decimal:
    """One rate, as the digits BCB published.

    `bool` is refused by name: it is an `int` to Python, so `Decimal(True)` would land a
    rate of exactly 1 without a word, and 1.0 is the one value a broken FX rate can wear
    while looking deliberate."""
    value = row[field]
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return Decimal(value)
    if isinstance(value, str):
        try:
            return Decimal(value)
        except InvalidOperation:
            pass
    raise PtaxResponseRefused(
        f"{quote_url(quote_date)} answered {field}={value!r}, which is not a rate. A row "
        "whose rate cannot be read converts every payment that reaches it at nothing"
    )


def _publication_instant(raw: object, quote_date: date) -> datetime:
    """`dataHoraCotacao` as an instant in Brasilia time -- the one field T3's rule reads."""
    if isinstance(raw, str):
        for spelling in PUBLICATION_FORMATS:
            try:
                return datetime.strptime(raw, spelling).replace(tzinfo=BRASILIA)
            except ValueError:
                continue
    raise PtaxResponseRefused(
        f"{quote_url(quote_date)} answered {PUBLISHED_FIELD}={raw!r}, which no known "
        "spelling parses. T3 resolves a payment's rate by comparing this instant against "
        "the payment's own, so an unreadable one is not a row with one missing column -- "
        "it is a row that cannot take part in the join at all"
    )


def quotes_in(body: str, quote_date: date) -> tuple[PtaxQuote, ...]:
    """Every row the response carries, validated, with `quote_date` carried onto each.

    ZERO ROWS IS NOT AN ERROR: weekends and holidays have no quote, and T3 resolves a
    payment on one by falling back over the series. What is refused is a row that cannot
    be read.

    AN UNKNOWN EXTRA FIELD IS NOT REFUSED, which is a decision and not an oversight: a
    RENAMED field already fires the missing-field refusal below, while an ADDED one takes
    nothing away from what this layer reads, and refusing it would stop the extraction
    over a change that costs nothing."""
    quotes: list[PtaxQuote] = []
    for row in _envelope_rows(body, quote_date):
        if not isinstance(row, dict):
            raise PtaxResponseRefused(
                f"{quote_url(quote_date)} answered a {type(row).__name__} where a quote "
                "row was expected"
            )
        missing = [field for field in RESPONSE_FIELDS if field not in row]
        if missing:
            raise PtaxResponseRefused(
                f"{quote_url(quote_date)} answered a row without {missing}; it carries "
                f"{sorted(row)}. Every one of {list(RESPONSE_FIELDS)} is load-bearing -- "
                "the rates are the conversion and the stamp is what T3 compares against"
            )
        quotes.append(
            PtaxQuote(
                quote_date=quote_date,
                currency=QUOTED_CURRENCY,
                published_at=_publication_instant(row[PUBLISHED_FIELD], quote_date),
                published_raw=str(row[PUBLISHED_FIELD]),
                compra=_rate(row, COMPRA_FIELD, quote_date),
                venda=_rate(row, VENDA_FIELD, quote_date),
            )
        )
    return tuple(quotes)


def _refuse_a_second_quote_date(quotes: tuple[PtaxQuote, ...]) -> None:
    """Refuse a group spanning more than one quote date.

    THIS IS WHAT MAKES THE PER-RESPONSE SCOPE STRUCTURAL rather than a sentence in a
    docstring. Every group this module builds comes from one response and therefore
    carries one quote date, so nothing inside can reach this. It exists for the caller
    that mistakes `sole_quote` for the reduce the LANDED TABLE needs and hands it rows
    from several dates: under that reading a disagreement between two perfectly good
    quotes on different days would be refused as a contradiction, and an agreement across
    a flat week would collapse five quotes into one. Bronze appends, so that caller is not
    hypothetical -- something downstream has to reduce, and it must not be this."""
    dates = sorted({quote.quote_date for quote in quotes})
    if len(dates) > 1:
        raise PtaxResponseRefused(
            f"sole_quote was handed rows for {len(dates)} quote dates ({dates[0]} .. "
            f"{dates[-1]}), and it reduces ONE response, whose rows all carry the single "
            "quote date its URL asked for. The reduce the landed table needs is a "
            "different one and lives downstream: bronze appends, so a re-run lands a "
            "second row per (currency, quote_date) that this function never sees."
        )


def sole_quote(quotes: tuple[PtaxQuote, ...]) -> PtaxQuote | None:
    """The one quote for a `(currency, quote_date)` IN ONE RESPONSE, or None if that
    response carries none.

    THE SCOPE IS ONE RESPONSE, AND THIS IS NOT THE REDUCE THE LANDED TABLE NEEDS. Here
    the key IS the request -- one endpoint is one currency and one URL is one quote date
    -- so two rows in one response can only be two publications of one quote, and that is
    the only fan-out this function is able to see. It is invoked once per response and
    never over the table.

    SO IT DOES NOT SURVIVE THE LANDING, and the correction is worth stating plainly
    because the first version of this docstring got it backwards. Bronze is written
    `mode("append")` (`opl.bronze.promote`), so a second extraction over the same span
    lands a second row for every `(currency, quote_date)` reduced here, and no amount of
    care at the request can see across responses or across runs. THE CONSUMER OF THE
    LANDED TABLE MUST STILL REDUCE OVER THAT TABLE. This is an addition to that reduce,
    not a replacement for it, and a re-run is an ordinary event rather than an incident.

    MEASURED: 2025-04-23 carries TWO rows, publication stamps 27 ms apart, AGREEING on
    both rates.

    THE DISAGREEMENT BRANCH HAS NO WITNESS. No quote date in the series holds rows that
    differ, so this branch ships unexercised by the source; the only body that has ever
    taken it is hand-built, in `tests/test_ptax_source.py`. That is a finding rather than
    a defect -- the alternative is picking one of two contradicting rates in silence."""
    if not quotes:
        return None
    _refuse_a_second_quote_date(quotes)
    rates = {(quote.compra, quote.venda) for quote in quotes}
    if len(rates) > 1:
        raise PtaxResponseRefused(
            f"{quote_url(quotes[0].quote_date)} answered {len(quotes)} rows that DISAGREE "
            f"on the rate: {sorted(map(str, rates))}. Taking either one silently picks "
            "the rate every payment on this date converts at, so the extraction stops here"
        )
    return max(quotes, key=lambda quote: quote.published_at)


def fetch_quote(quote_date: date, fetch: Fetch) -> PtaxQuote | None:
    """The single quote for one date, or None if the series has none for it."""
    return sole_quote(quotes_in(fetch(quote_url(quote_date)), quote_date))


def quote_dates(first: date, last: date) -> Iterator[date]:
    """Every calendar day in `[first, last]`, inclusive.

    An inverted span is a caller's bug rather than a bad response, and it would otherwise
    yield nothing and land an empty series that every downstream count reports as clean."""
    if first > last:
        raise ValueError(f"the span {first}..{last} ends before it starts")
    current = first
    while current <= last:
        yield current
        current += timedelta(days=1)


def fetch_series(first: date, last: date, fetch: Fetch) -> tuple[PtaxQuote, ...]:
    """Every quote in `[first, last]`, in date order, ONE REQUEST PER QUOTE DATE.

    The loop is the price of attribution, and it is cheap: a response is ~220 bytes and
    the phase's span is 42 quotes. A single wide request would be one round trip and
    would return rows this layer could not attach a quote date to.

    Days with no quote are ABSENT from the result rather than present as None: T3
    resolves them by falling back over the series, and asserting the series is gapless in
    business days belongs to the layer that lands it, not to this one."""
    found = (fetch_quote(day, fetch) for day in quote_dates(first, last))
    return tuple(quote for quote in found if quote is not None)
