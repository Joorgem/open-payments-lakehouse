"""F-API Task 0 probe: the five PTAX measurements the phase is built on, each one
re-runnable and each number printed beside the exact request that produced it.

WHY THIS FILE EXISTS. The first version of the F-API plan published two dollar rates
as "measured before the plan was written", and no artefact in this repository carried
them or the request that would produce them. That is the third appearance of a
provenance defect this project has already struck twice (`docs/f3-run-evidence.md`
§0.5). A number without the request that produced it is not a measurement, so the
rates are demoted to predictions until this script confirms or falsifies them. Run it
and the output is the evidence; the numbers are never hand-carried into a document.

THE API IS PUBLIC AND UNAUTHENTICATED. There is no token, no header and no secret to
add here, and nothing in this file should ever grow one.

THE ENDPOINT IS NOT RE-DERIVED. It is the URL `scripts/validate_cnpj_snapshots.py`
has carried since F0, including the `MM-DD-YYYY`-in-single-quotes date format, which
is not ISO and is got wrong by everyone who assumes. A second spelling of an endpoint
is the defect class this repository polices hardest.

DECIMAL, FROM THE RAW TEXT. `json.loads` turns the API's `5.07730` into the float
`5.0773`, and `str()` on that drops the trailing zero -- so a test pinning the
published string would fail against a perfectly correct extraction. Everything here
parses with `json.loads(..., parse_float=Decimal)` so the API's own digits survive,
and §3 below prints the two spellings side by side so the hazard is visible rather
than described.

MEASURES ONLY. This script builds no table, contract, registry entry or profile. The
holiday table in §2 is a DIAGNOSTIC used to classify an absence in a report; the
phase's resolution rule reads the PTAX series itself and never a calendar (plan T3).
"""

import json
from collections.abc import Iterator
from datetime import date, timedelta
from decimal import Decimal
from typing import NamedTuple

import requests

# Carried over from scripts/validate_cnpj_snapshots.py:28-33 -- read, not re-derived.
PTAX_ENDPOINT = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@di,dataFinalCotacao=@df)"
)
PTAX_DATE_FORMAT = "%m-%d-%Y"  # NOT ISO, and the single quotes below are part of it.
HEADERS = {"User-Agent": "open-payments-lakehouse-f-api-probe/1.0"}
TIMEOUT_SECONDS = 60

# 1. How far back the series goes: a bracket, not a guess. Each of these is one request.
FLOOR_BRACKET = (
    (date(1970, 1, 1), date(1970, 12, 31)),
    (date(1980, 1, 1), date(1980, 12, 31)),
    (date(1984, 1, 1), date(1984, 11, 27)),
    (date(1984, 11, 28), date(1984, 11, 28)),
)

# 2. The contiguous walk. Covers Corpus Christi (2026-06-04) and every date the fact
#    reaches after the fifth profile lands.
WALK_FIRST = date(2026, 5, 25)
WALK_LAST = date(2026, 8, 5)

# 3. The two rates the plan published without provenance, as strings so the trailing
#    zero is part of the claim being tested.
PREDICTIONS = ((date(2026, 7, 31), "5.07730"), (date(2026, 6, 19), "5.14420"))

# 4. The pair T2's window rests on: if these two rates are equal, the window moves.
GATE_PAIR = (date(2026, 6, 19), date(2026, 6, 22))

# 5. Every date the fact reaches after the fifth profile lands, plus the holiday case
#    that only the series can exercise.
DATES_THE_FACT_NEEDS = (
    date(2026, 6, 20),
    date(2026, 6, 21),
    date(2026, 6, 22),
    date(2026, 8, 1),
)
HOLIDAY_CASE = date(2026, 6, 4)
MAX_FALLBACK_STEPS = 15

# 6. The whole modern span in one request: does an absence the calendar cannot explain
#    exist at all, and does any single date carry more than one row?
SURVEY_FIRST = date(2023, 1, 1)
SURVEY_LAST = date(2026, 8, 5)


class Quote(NamedTuple):
    """One PTAX row. `asked_for` is the quote date the request filtered on; `published`
    is the API's own `dataHoraCotacao` string, which is a PUBLICATION instant and is
    NOT always on the quote's own date -- see the 1984 rows in §1's output."""

    asked_for: date | None
    published: str
    compra: Decimal
    venda: Decimal


def quote_url(first: date, last: date) -> str:
    """The exact URL for a quote-date window, printed beside every number below."""
    di = first.strftime(PTAX_DATE_FORMAT)
    df = last.strftime(PTAX_DATE_FORMAT)
    return f"{PTAX_ENDPOINT}?@di='{di}'&@df='{df}'&$format=json"


def fetch_raw(first: date, last: date) -> tuple[str, str]:
    """Return the response body and the URL. Raises on anything but a 200."""
    url = quote_url(first, last)
    response = requests.get(url, headers=HEADERS, timeout=TIMEOUT_SECONDS)
    if response.status_code != 200:
        raise RuntimeError(f"PTAX answered {response.status_code} for {url}")
    return response.text, url


def fetch_window(first: date, last: date) -> tuple[list[Quote], str]:
    """Quotes for a window, parsed with Decimal so the API's digits survive."""
    body, url = fetch_raw(first, last)
    rows = json.loads(body, parse_float=Decimal).get("value", [])
    asked_for = first if first == last else None
    quotes = [
        Quote(asked_for, row["dataHoraCotacao"], row["cotacaoCompra"], row["cotacaoVenda"])
        for row in rows
    ]
    return quotes, url


def print_quotes(quotes: list[Quote], url: str, indent: str = "  ") -> None:
    """Print every retrieved quote with its date, both rates and its own request URL."""
    if not quotes:
        print(f"{indent}(no quote)  url={url}")
        return
    for quote in quotes:
        asked = quote.asked_for.isoformat() if quote.asked_for else "-"
        print(
            f"{indent}quote_date={asked}  published={quote.published}"
            f"  compra={quote.compra}  venda={quote.venda}  url={url}"
        )


def days_between(first: date, last: date) -> Iterator[date]:
    """Every calendar day in [first, last], inclusive."""
    current = first
    while current <= last:
        yield current
        current += timedelta(days=1)


def easter_sunday(year: int) -> date:
    """Anonymous Gregorian computus -- computed, never tabulated, so the movable
    holidays below can be checked rather than trusted."""
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    lam = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * lam) // 451
    month = (h + lam - 7 * m + 114) // 31
    day = ((h + lam - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def bank_holidays(year: int) -> dict[date, str]:
    """DIAGNOSTIC ONLY -- used to say whether an absence in the series is explained.
    The phase resolves rates from the series itself and never from a calendar (T3):
    a calendar is a second spelling of "is there a quote" and the two can disagree.

    This list is TODAY's and does not back-date statutory changes, which is not a
    shortcut but the demonstration: Black Consciousness became a national holiday only
    from 2024 (Lei 14.759/2023), so §6 finds a quote on 2023-11-20, a day this table
    calls a holiday. That is T3's hazard measured rather than argued -- a calendar that
    drifts against its source hands the join a rate for a day the source never had."""
    easter = easter_sunday(year)
    movable = {
        easter - timedelta(days=48): "Carnival Monday",
        easter - timedelta(days=47): "Carnival Tuesday",
        easter - timedelta(days=2): "Good Friday",
        easter + timedelta(days=60): "Corpus Christi",
    }
    fixed = {
        date(year, 1, 1): "New Year",
        date(year, 4, 21): "Tiradentes",
        date(year, 5, 1): "Labour Day",
        date(year, 9, 7): "Independence",
        date(year, 10, 12): "Our Lady of Aparecida",
        date(year, 11, 2): "All Souls",
        date(year, 11, 15): "Republic",
        date(year, 11, 20): "Black Consciousness",
        date(year, 12, 25): "Christmas",
    }
    return {**movable, **fixed}


def measure_1_series_floor() -> None:
    """How far back does CotacaoDolarPeriodo return data? Bracketed, not asserted."""
    print("\n=== 1. SERIES DEPTH ===")
    for first, last in FLOOR_BRACKET:
        quotes, url = fetch_window(first, last)
        print(f"{first} .. {last}: {len(quotes)} row(s)")
        print_quotes(quotes[:3], url)
    print(
        "  NOTE: the 1984-11-28 row is published 1984-12-03, so `dataHoraCotacao` is a\n"
        "  PUBLICATION instant and the quote date it was filtered on is absent from the\n"
        "  response. In 2026 the two coincide; the extraction must not assume they do."
    )


def walk_per_date(first: date, last: date) -> dict[date, Quote | None]:
    """One request per calendar day, so an absence carries its own re-runnable URL."""
    found: dict[date, Quote | None] = {}
    for day in days_between(first, last):
        quotes, url = fetch_window(day, day)
        if len(quotes) > 1:
            print(f"  !! {day}: {len(quotes)} rows for ONE date -- fan-out, url={url}")
        found[day] = quotes[0] if quotes else None
        print_quotes(quotes, url)
    return found


def classify_absence(day: date, holidays: dict[date, str]) -> str:
    """Weekend, holiday, or the finding: an absence nobody predicted."""
    if day.weekday() >= 5:
        return "weekend"
    if day in holidays:
        return f"holiday ({holidays[day]})"
    return "UNEXPLAINED -- a fallback case nobody predicted"


def measure_2_gaps() -> None:
    """Every weekday in the walk with no quote, classified."""
    print(f"\n=== 2. GAPS INSIDE BUSINESS WEEKS, {WALK_FIRST} .. {WALK_LAST} ===")
    per_date = walk_per_date(WALK_FIRST, WALK_LAST)
    ranged, range_url = fetch_window(WALK_FIRST, WALK_LAST)
    print(f"  range cross-check: {len(ranged)} row(s)  url={range_url}")
    by_publication = {quote.published[:10] for quote in ranged}
    present = {day.isoformat() for day, quote in per_date.items() if quote}
    agree = by_publication == present
    print(f"  per-date and range agree on which dates carry a quote: {agree}")
    holidays = bank_holidays(WALK_FIRST.year)
    print("  absences:")
    for day, quote in sorted(per_date.items()):
        if quote is None:
            print(f"    {day} {day.strftime('%a')}: {classify_absence(day, holidays)}")
    corpus = HOLIDAY_CASE in per_date and per_date[HOLIDAY_CASE] is None
    print(f"  2026-06-04 (Corpus Christi) absent: {corpus}")


def report_digit_fidelity(body: str, url: str) -> None:
    """Show, rather than describe, what a float parse does to the API's own digits."""
    as_decimal = json.loads(body, parse_float=Decimal)["value"]
    as_float = json.loads(body)["value"]
    for decimal_row, float_row in zip(as_decimal, as_float, strict=True):
        print(
            f"    Decimal -> {decimal_row['cotacaoVenda']}"
            f"   |   float -> {float_row['cotacaoVenda']}   url={url}"
        )


def measure_3_predictions() -> None:
    """Confirm or falsify the two rates the plan published without provenance."""
    print("\n=== 3. THE TWO PUBLISHED PREDICTIONS ===")
    for day, predicted in PREDICTIONS:
        quotes, url = fetch_window(day, day)
        print(f"{day}: predicted venda {predicted}")
        print_quotes(quotes, url)
        actual = str(quotes[0].venda) if quotes else "(none)"
        verdict = "CONFIRMED" if actual == predicted else "FALSIFIED"
        print(f"  {verdict}: predicted {predicted}, API returned {actual}")
        body, raw_url = fetch_raw(day, day)
        report_digit_fidelity(body, raw_url)


def measure_4_window_gate() -> None:
    """T2's window needs these two dates to carry DIFFERENT rates."""
    print("\n=== 4. THE GATE ON T2's WINDOW ===")
    seen: list[Quote] = []
    for day in GATE_PAIR:
        quotes, url = fetch_window(day, day)
        print(f"{day}:")
        print_quotes(quotes, url)
        seen.extend(quotes)
    if len(seen) != len(GATE_PAIR):
        print("  ONE OF THE GATE DATES HAS NO QUOTE -- the window cannot stand.")
        return
    first, second = seen[0], seen[1]
    if first.venda == second.venda:
        print(f"  EQUAL ({first.venda}) -- THE TWO-RATE PROPERTY FAILS. THE WINDOW MOVES.")
    else:
        print(f"  DIFFERENT: {first.venda} vs {second.venda} (delta {second.venda - first.venda})")
    print(
        "  The rates differ, but the resolution rule is an INSTANT comparison (T3.1):\n"
        "  a payment may only use a quote already PUBLISHED when it happened. Both\n"
        "  publication instants are printed above -- compare them against the window's\n"
        "  close before treating the later date as reachable."
    )


def resolve_backwards(day: date) -> date | None:
    """Which quote date does `day` fall back to? One request per step, each printed."""
    for step in range(MAX_FALLBACK_STEPS):
        candidate = day - timedelta(days=step)
        quotes, url = fetch_window(candidate, candidate)
        print_quotes(quotes, url, indent="    ")
        if quotes:
            return candidate
    return None


def measure_5_extraction_range() -> None:
    """How far back the extraction must reach to resolve every needed date gaplessly."""
    print("\n=== 5. THE EXTRACTION RANGE ===")
    needed = (*DATES_THE_FACT_NEEDS, HOLIDAY_CASE)
    resolved: list[date] = []
    for day in needed:
        print(f"  {day} ({day.strftime('%a')}) resolves to:")
        target = resolve_backwards(day)
        print(f"    -> {target}")
        if target is not None:
            resolved.append(target)
    if not resolved:
        print("  NOTHING RESOLVED -- the series does not cover these dates.")
        return
    print(f"  distinct dates the fact needs: {len(DATES_THE_FACT_NEEDS)}")
    print(f"  earliest quote date needed: {min(resolved)}")
    print(f"  latest date the fact reaches: {max(needed)}")
    print(f"  EXTRACTION RANGE: {min(resolved)} .. {max(needed)} inclusive, gapless")


def measure_6_modern_survey() -> None:
    """One request over the whole modern span. Two questions the 73-day walk is too
    short to answer: does an absence exist that a national-holiday calendar cannot
    explain, and does any one date carry MORE THAN ONE row -- the fan-out §4 says the
    FX join must reduce before it joins, and refuse rather than `max()` if the rows
    disagree. Keyed on publication date, which §1 shows is not the quote date in 1984;
    §2's cross-check is what licenses the equation for the modern span."""
    print(f"\n=== 6. MODERN-ERA SURVEY, {SURVEY_FIRST} .. {SURVEY_LAST} ===")
    quotes, url = fetch_window(SURVEY_FIRST, SURVEY_LAST)
    by_date: dict[str, list[Quote]] = {}
    for quote in quotes:
        by_date.setdefault(quote.published[:10], []).append(quote)
    print(f"  {len(quotes)} row(s), {len(by_date)} distinct dates  url={url}")
    holidays: dict[date, str] = {}
    for year in range(SURVEY_FIRST.year, SURVEY_LAST.year + 1):
        holidays.update(bank_holidays(year))
    unexplained = [
        day
        for day in days_between(SURVEY_FIRST, SURVEY_LAST)
        if day.isoformat() not in by_date and day.weekday() < 5 and day not in holidays
    ]
    print(f"  weekday absences the national-holiday calendar does NOT explain: {len(unexplained)}")
    for day in unexplained:
        print(f"    {day} {day.strftime('%a')}: UNEXPLAINED -- the calendar disagrees")
    # The converse, which is T3's actual claim: a calendar can disagree the other way.
    disagreeing = [
        day
        for day in days_between(SURVEY_FIRST, SURVEY_LAST)
        if day in holidays and day.isoformat() in by_date
    ]
    print(f"  national holidays that DO carry a quote: {len(disagreeing)}")
    for day in disagreeing:
        print(f"    {day} {day.strftime('%a')}: {holidays[day]}, and quoted anyway")
    for published_date, rows in sorted(by_date.items()):
        if len(rows) > 1:
            print(f"  FAN-OUT on {published_date}: {len(rows)} rows for one date")
            print_quotes(rows, url, indent="    ")


def main() -> None:
    print("PTAX probe -- public, unauthenticated BCB/Olinda OData. No credential involved.")
    print(f"endpoint: {PTAX_ENDPOINT}")
    measure_1_series_floor()
    measure_2_gaps()
    measure_3_predictions()
    measure_4_window_gate()
    measure_5_extraction_range()
    measure_6_modern_survey()


if __name__ == "__main__":
    main()
