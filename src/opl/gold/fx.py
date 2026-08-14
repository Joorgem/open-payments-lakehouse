# src/opl/gold/fx.py
"""Resolve the PTAX rate a payment converted at: the most recent quote whose PUBLICATION
INSTANT precedes the payment's own instant, read from the landed series and never from a
holiday calendar.

--- THE RULE, AND WHY IT IS AN INSTANT AND NOT A DAY (phase plan T3, T4d) ------------

A calendar-day join is right for every row this phase lands and wrong in principle, and the
difference is visible in one measurable place: the `cross-currency` stream opens at
2026-06-22T08:00Z and closes at 21:53:15Z, and the 2026-06-22 quote publishes at
`dataHoraCotacao 2026-06-22 13:06:19.750415` READ AS BRT, i.e. 2026-06-22T16:06:19.750415Z.
So two payments ON ONE CALENDAR DAY, in one stream, in one currency, convert at two
different rates -- 5.14420 for the 5,836 events before the bulletin and 5.13950 for the
4,164 after it. No day-grain implementation can produce that, and a day-grain
implementation would ALSO hand a payment at 2026-06-22T01:53Z a rate published 14 h 13 m
later, which is a rate from the future in the one project whose headline is
as-of-KNOWN-time.

THE ZONE IS BRT AND THE ARGUMENT IS MEASURED, NOT ASSUMED. Every 2026 row publishes at
~13:0x, which is the PTAX bulletin hour in Brasília; read as UTC it would be ~10:0x local,
before the bulletin exists. It is also the fail-safe direction -- it places publication
three hours LATER, so a wrong zone makes a payment fall back to an older rate rather than
use one not yet published. The offset comes from `opl.extraction.ptax_source.BRASILIA` and
is rendered rather than restated, so this layer and the extraction cannot disagree about it;
`str(timezone(timedelta(hours=-3)))` is `'UTC-03:00'`, which Spark accepts as a zone id, and
`to_utc_timestamp` cancels the session zone on both sides so the instant does not move with
`spark.sql.session.timeZone`.

--- WHY THE FX SIDE IS REDUCED *BEFORE* THE JOIN (T4c) --------------------------------

`opl.gold.facts._versions_the_fact_can_reach` may broadcast because its row count is a
proven bound. AN FX JOIN HAS NO SUCH ARGUMENT. `bronze_ptax` is written `mode("append")`
(`opl.bronze.promote`), so a second extraction over the same window lands a SECOND row for
every `(currency, quote_date)` it covers -- and `opl.contracts.ptax` says so in the
imperative: "ANY CONSUMER OF THIS TABLE MUST REDUCE IT TO ONE ROW PER (currency,
quote_date) ITSELF." `opl.extraction.ptax_source.sole_quote` reduces ONE RESPONSE and
cannot see across runs, so a re-run -- an ordinary event, not an incident -- would double
every USD fact row through this join. And
`facts._refuse_a_row_count_that_is_not_one_per_delivered_identity` catches a fan-out only
AFTER the append, in a message whose own words are "THE TABLE ON DISK IS ALREADY WRITTEN".

So `rate_intervals` reduces first, and TWO ROWS THAT DISAGREE ON A RATE ARE REFUSED RATHER
THAN RESOLVED -- never `max()`, never `min()` over disagreeing values. Taking either would
silently pick the rate every payment on that date converts at. Two rows that AGREE reduce to
the EARLIER publication stamp, which is `sole_quote`'s decision applied to the table: the
rate is identical either way and only its availability moves, so keeping the later stamp
would deny a payment between the two a rate that had already been published.

THE DISAGREEMENT BRANCH SHIPS UNEXERCISED and must not be reported as working. Task 0 walked
903 rows over 3.6 years and found one duplicate pair (2025-04-23, 27 ms apart); the fix pass
found a second (2001-12-21, identical stamps). Both AGREE.

--- WHY THE JOIN IS A HALF-OPEN INTERVAL AND CANNOT FAN OUT --------------------------

Once there is one row per `(currency, quote_date)`, the series becomes an interval table:
each quote is in force from its own publication instant until the NEXT quote's, and the last
one until `opl.gold.columns.VALID_TO_CEILING`. That is the same mechanism
`opl.gold.dimensions` writes for `dim_company` and the same predicate
`facts._resolved` reads it with -- `from <= t < to`, and NEVER `BETWEEN`, which is inclusive
at both ends and matches two rows at a boundary. The intervals partition the timeline per
currency, so a payment matches AT MOST ONE quote BY CONSTRUCTION rather than by a count
taken afterwards.

THE LOW END IS NOT FLOORED, AND THAT IS THE OPPOSITE OF `dim_company` ON PURPOSE. That
dimension floors its first version at the epoch because a lookup convention about a company
we know something about is better than "unknown"; a RATE THAT DID NOT EXIST YET CANNOT BE
APPLIED. A payment below the series' first publication instant therefore matches nothing and
`refuse_payments_no_rate_can_be_resolved` stops the build -- T3 clause 3, and the same
argument `facts._refuse_payments_no_instant_can_be_read` makes: a NULL rate gives a NULL
`amount_brl` and lowers every total by an amount nobody can name.

--- WHAT IS *NOT* ASSERTED HERE, STATED SO IT IS NOT MISTAKEN FOR AN OVERSIGHT ------

GAPLESSNESS IN BUSINESS DAYS IS NOT CHECKED, and the phase plan's T3 clause 2 asks for it.
It is declined with the mechanism in hand rather than skipped: any bound on the gap between
two consecutive quotes is either a Brazilian holiday calendar -- the SECOND SPELLING OF "is
there a quote" that T3's whole ruling refuses, because the two can disagree and the series
itself carries the witness (2023-11-20 has a quote and today's holiday list calls it a
holiday) -- or a number drawn from this one extraction window, which is the species
`ptax_source` already refuses for magnitude ceilings ("a bound that cannot be chosen from
the data is not a guard, it is a guess"). A three-day Friday-to-Monday gap and a four-day
holiday weekend are both normal, and Carnival makes five. What is done instead is
REPORTING: `FxSeries` carries the quote count and the first and last publication instant, so
a bounded or holey extraction is visible in the run log as three numbers rather than
invisible behind a rate that resolved.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from opl.contracts import ptax
from opl.extraction.ptax_source import BRASILIA
from opl.gold.columns import VALID_TO_CEILING
from opl.gold.dimensions import instant_literal

__all__ = [
    "AMOUNT_BRL",
    "FX_RATE",
    "FX_RATE_DATE",
    "FX_RATE_SCALE",
    "FX_RATE_TYPE",
    "FxSeries",
    "converted_amount",
    "rate_intervals",
    "refuse_payments_no_rate_can_be_resolved",
    "with_resolved_rates",
]

# THE THREE COLUMNS THIS MODULE PUTS IN THE FACT. They are DECLARED as literals in
# `opl.gold.registry` -- `fx_rate` and `amount_brl` as `DerivedMeasure`s, `fx_rate_date` as
# the `fact_column` of `dim_date`'s derived role -- and `opl.gold.facts
# ._refuse_a_fact_this_loader_cannot_derive` refuses a spec that spells any of them
# differently. So the two copies cannot drift, and the refusal is at the one place that can
# see both, before the first write. The alternative -- `registry.py` importing this module --
# would put pyspark behind the import of every gold spec, which is what
# `opl.gold.spec_fields` exists to prevent.
FX_RATE = "fx_rate"
FX_RATE_DATE = "fx_rate_date"
AMOUNT_BRL = "amount_brl"

# `fx_rate` IS NOT `opl.gold.facts.AMOUNT_TYPE`, AND THE ARITHMETIC IS THE ARGUMENT.
# `decimal(18, 2)` would round 5.14420 to 5.14 and put `amount_brl` about 0.08% wrong on
# every USD row -- plausible in magnitude, in a column nobody re-derives. FIVE is the scale
# the series itself publishes, at every magnitude it has ever carried: 5.14420 in 2026,
# 2828.00000 at the 1984 floor, 0.82900 at the 1994 low, 71153.00000 at the 1993 high. The
# precision is 18 for `AMOUNT_TYPE`'s reason -- five integer digits are needed and a DECIMAL
# leaves no room for the binary rounding a DOUBLE would put into a rate whose whole contract
# is that it is the digits BCB published.
FX_RATE_SCALE = 5
FX_RATE_TYPE = f"decimal(18, {FX_RATE_SCALE})"

# THE PUBLICATION ZONE, RENDERED FROM THE EXTRACTION'S OWN DECISION rather than restated. A
# second spelling of an offset is a second spelling of T3's ruling, and this one would be
# silent: three hours is not a shape any test notices, it is a rate one business day old.
_BRASILIA_ZONE = str(BRASILIA)

# The FX side's own columns, prefixed so nothing they carry can collide with a payment
# column while both frames are joined -- `opl.gold.facts._resolved`'s decision, and for its
# reason: both sides carry a `currency`, so a condition written against ambiguous names
# would resolve to whichever Spark picked.
_FX_CURRENCY = "_fx_currency"
_FX_QUOTE_DATE = "_fx_quote_date"
_FX_RATE = "_fx_rate"
_FX_FROM = "_fx_from"
_FX_TO = "_fx_to"
_DISTINCT_RATES = "_distinct_rates"
_UNREADABLE = "_unreadable"


@dataclass(frozen=True)
class FxSeries:
    """The reduced series and the three numbers that make a BOUNDED extraction visible.

    REPORTED AND NEVER REFUSED -- see the module docstring for why gaplessness is not
    asserted here. A run log carrying "42 quotes, 2026-06-03 .. 2026-08-01" is what lets a
    reader see that the window the fact needed was actually landed; a rate that resolved
    proves only that SOME quote preceded the payment."""

    intervals: DataFrame
    quotes: int
    first_published: object
    last_published: object


def _quote_date() -> Column:
    """The quote date as a DATE, out of bronze's all-string ISO text.

    `quote_date` is stamped from the REQUEST and is ISO by contract, with the DQ gate's
    `bad_quote_date_shape` refusing anything else -- including `06-19-2026`, the API's own
    spelling and the value this phase invites."""
    return F.to_date(F.col(ptax.QUOTE_DATE_COLUMN))


def _published_instant() -> Column:
    """`data_hora_cotacao` as an INSTANT, its wall clock read as Brasília time.

    THE PARSE IS FORMAT-AGNOSTIC AND THE GATE IS WHAT MAKES THAT SAFE, which is a dependency
    worth naming rather than assuming. The series' fractional-second width is 1 digit in
    1984, 3 in 2025 and 6 in 2026, so any single `to_timestamp` pattern refuses real rows --
    and `opl.bronze.rules.unparseable_data_hora_cotacao` is two checks where a pinned pattern
    would be one: a SHAPE regex that refuses every spelling whose instant its own text does
    not determine (a bare time, which resolves to TODAY'S DATE and is therefore
    non-deterministic, a bare date, a `T` separator, seven digits, surrounding whitespace)
    and the parse itself. A row reaching this function has passed both, so what is left for
    this layer is the NULL count, which `refuse_payments_no_rate_can_be_resolved` turns into
    a refusal.

    `to_utc_timestamp` AND NOT AN INTERVAL ADDITION. Adding three hours to a parsed wall
    clock is only correct while the session zone is UTC, and `tests/gold/test_fact_payment.py`
    deliberately builds the whole fact under America/Sao_Paulo to prove nothing in it moves
    with the zone. This spelling uses the session zone on both sides, so the two cancel."""
    return F.to_utc_timestamp(F.to_timestamp(F.col(ptax.PUBLISHED_AT_COLUMN)), _BRASILIA_ZONE)


def _reduced(quotes: DataFrame) -> DataFrame:
    """One row per `(currency, quote_date)`, carrying the rate, the EARLIEST publication
    instant, and the two counts the refusal below reads.

    `count_distinct` OVER ONE COLUMN and not over the pair, per master protocol §4.8: the
    banned multi-column form drops NULL-bearing rows, which is how 8,761 rows once went
    missing here. It also IGNORES NULLs on its own, so a rate that failed to cast would be
    invisible to it -- which is why the NULLs are counted separately rather than trusted to
    raise the distinct count.

    `venda` AND NOT `compra`, and the direction is the thing an FX defect gets wrong
    silently: `opl.contracts.ptax` records that this pair is where a wrong choice "lands a
    number of the right shape, plausible in magnitude, and wrong". SIXTEEN real rows in the
    series carry `compra > venda`, so the two are not even ordered."""
    rate = F.col(ptax.VENDA_COLUMN).cast(FX_RATE_TYPE)
    published = _published_instant()
    quote_date = _quote_date()
    return quotes.groupBy(
        F.col(ptax.CURRENCY_COLUMN).alias(_FX_CURRENCY), quote_date.alias(_FX_QUOTE_DATE)
    ).agg(
        F.count_distinct(rate).alias(_DISTINCT_RATES),
        F.count(
            F.when(rate.isNull() | published.isNull() | quote_date.isNull(), 1)
        ).alias(_UNREADABLE),
        F.min(rate).alias(_FX_RATE),
        F.min(published).alias(_FX_FROM),
    )


def _refuse_a_quote_date_that_does_not_reduce_to_one_rate(reduced: DataFrame) -> int:
    """Refuse two landed rows for one `(currency, quote_date)` that DISAGREE on the rate, or
    a row whose rate or publication instant cannot be read.

    NEVER `max()`, NEVER `min()` OVER DISAGREEING VALUES, which is the phase plan's ruling
    and the module docstring's argument: either choice silently picks the rate every payment
    on that date converts at. The unreadable count rides along in the same aggregate because
    it is the same failure at a different layer -- a value the gate accepted and this cast
    could not read -- and because a NULL rate would otherwise reduce to a NULL and convert
    every payment on that date to NULL.

    RETURNS THE QUOTE COUNT, so the one collect this function costs also produces the number
    `FxSeries` reports rather than being paid for twice."""
    measured = reduced.agg(
        F.count(F.lit(1)).alias("quotes"),
        F.max(_DISTINCT_RATES).alias("rates"),
        F.sum(_UNREADABLE).alias("unreadable"),
    ).collect()[0]
    if measured["rates"] and measured["rates"] > 1:
        raise ValueError(
            f"refusing to resolve FX: one (currency, quote_date) in "
            f"{ptax.BRONZE_TABLE} carries {measured['rates']} DIFFERENT "
            f"{ptax.VENDA_COLUMN} values. Two landed rows for one quote date are two "
            "publications of ONE quote and must agree; taking either silently picks the "
            "rate every payment on that date converts at. In 3.6 years of series the two "
            "duplicate pairs that exist both AGREE, so this is a landing to investigate "
            "rather than a tie to break. Nothing has been written by this run"
        )
    if measured["unreadable"]:
        raise ValueError(
            f"refusing to resolve FX: {measured['unreadable']} rows of "
            f"{ptax.BRONZE_TABLE} carry a {ptax.VENDA_COLUMN} that is not a "
            f"{FX_RATE_TYPE}, a {ptax.PUBLISHED_AT_COLUMN} from which no instant can be "
            f"read, or a {ptax.QUOTE_DATE_COLUMN} that is no calendar day. The first two "
            "reduce to NULL and convert every payment on that quote date to NULL; the third "
            f"resolves a rate and a NULL {FX_RATE_DATE}, whose date key joins to nothing. "
            "The DQ gate refuses all three shapes, so this is a row that did not come "
            "through it"
        )
    return measured["quotes"]


def rate_intervals(quotes: DataFrame) -> FxSeries:
    """The landed PTAX series as a HALF-OPEN INTERVAL TABLE: one row per `(currency,
    quote_date)`, in force from its own publication instant until the next quote's.

    THE REDUCE AND THE REFUSAL HAPPEN HERE, BEFORE ANY JOIN -- see the module docstring for
    why after is too late. The window is tiny (42 quotes in this phase's range), so the
    `lead()` below shuffles nothing that matters and the whole frame is broadcastable by the
    caller.

    THE CEILING IS `VALID_TO_CEILING`, shared with `dim_company` rather than spelled again:
    one lakehouse should not have two answers to "when does the open interval end", and
    `opl.gold.columns` argues why it is 2999-12-31 and not 9999-12-31 on this stack."""
    reduced = _reduced(quotes)
    count = _refuse_a_quote_date_that_does_not_reduce_to_one_rate(reduced)
    ordered = Window.partitionBy(_FX_CURRENCY).orderBy(_FX_FROM)
    bounds = reduced.select(
        _FX_CURRENCY,
        _FX_QUOTE_DATE,
        _FX_RATE,
        _FX_FROM,
        F.coalesce(
            F.lead(F.col(_FX_FROM)).over(ordered), instant_literal(VALID_TO_CEILING)
        ).alias(_FX_TO),
    )
    ends = reduced.agg(F.min(_FX_FROM).alias("first"), F.max(_FX_FROM).alias("last")).collect()[0]
    return FxSeries(
        intervals=bounds,
        quotes=count,
        first_published=ends["first"],
        last_published=ends["last"],
    )


def with_resolved_rates(
    frame: DataFrame,
    intervals: DataFrame,
    *,
    instant: Column,
    day: Column,
    currency_column: str,
    reporting_currency: str,
) -> DataFrame:
    """`frame` with `fx_rate` and `fx_rate_date` set to the quote in force at each payment's
    own instant -- or, for a payment already in the reporting currency, to 1 and the
    payment's own day.

    THE REPORTING CURRENCY CONSULTS NO QUOTE AT ALL, BY DEFINITION AND NOT BY LOOKUP. `BRL`
    is what this lakehouse reports in, so its rate is exactly 1 and no series is involved:
    `payments.REPORTING_CURRENCY` is asserted to be `CURRENCIES[0]` at the contract's own
    import, and the PTAX endpoint quotes USD against BRL, so a BRL row matching a PTAX
    interval would be the defect rather than the answer.

    AND ITS `fx_rate_date` IS THE PAYMENT'S OWN DAY, WHICH IS A DECISION AND NOT A
    CONVENIENCE. The alternative is NULL, which reads as more honest -- no quote was
    consulted -- and is worse in a star: `date_format(NULL)` is NULL, so 35,095 rows would
    carry an unjoinable foreign key, every report grouped by `fx_rate_date_key` would drop
    them, and the orphan count would report the whole BRL population as a data-quality
    finding. The day the payment happened, at a rate of 1, is a true statement about an
    identity conversion and it keeps the column total.

    THAT DAY ARRIVES AS AN ARGUMENT AND IS NOT DERIVED FROM `instant` HERE, AND A TEST
    CAUGHT THE DIFFERENCE. `to_date(<timestamp>)` RENDERS an instant, in the SESSION zone --
    so under America/Sao_Paulo a payment at 2026-06-20T00:00:00Z became 2026-06-19 and
    `fx_rate_date_key` moved with a cluster setting, which is verbatim the failure
    `opl.gold.conformed.day_of` exists to prevent and which
    `tests/gold/test_fact_payment.py::test_the_fact_is_unchanged_when_it_is_built_under_a
    _non_utc_session_zone` refused. The caller passes `day_of(event_time)` -- ten characters
    of the producer's own ISO text, zone-free -- so this column is as stable as
    `event_date_key` and derived by the same spelling.

    A BROADCAST LEFT JOIN ON A HALF-OPEN INTERVAL, `from <= t < to` and never `BETWEEN` --
    `opl.gold.columns` argues the operator where the sentinels are declared. LEFT rather than
    INNER so an unresolvable payment does not vanish: it arrives with a NULL rate and is
    COUNTED, then refused by name."""
    matched = (_FX_CURRENCY, _FX_QUOTE_DATE, _FX_RATE, _FX_FROM, _FX_TO)
    joined = frame.join(
        F.broadcast(intervals.select(*matched)),
        (F.col(currency_column) == F.col(_FX_CURRENCY))
        & (instant >= F.col(_FX_FROM))
        & (instant < F.col(_FX_TO)),
        how="left",
    )
    in_reporting = F.col(currency_column) == F.lit(reporting_currency)
    return joined.withColumns(
        {
            FX_RATE: F.when(in_reporting, F.lit(1).cast(FX_RATE_TYPE)).otherwise(
                F.col(_FX_RATE)
            ),
            FX_RATE_DATE: F.when(in_reporting, day).otherwise(F.col(_FX_QUOTE_DATE)),
        }
    ).drop(*matched)


def converted_amount(*, amount_column: str, amount_type: str) -> Column:
    """The delivered amount in the reporting currency: `amount * fx_rate`, rounded HALF-UP
    to the contract's own scale AT THE ROW.

    ROUNDED AT THE ROW, AND THE CONSEQUENCE IS PUBLISHED RATHER THAN HIDDEN.
    `decimal(18,2) * decimal(18,5)` is `decimal(37,7)` in Spark, and casting that back to
    the amount's own type rounds HALF-UP (measured: 1.23 * 1.50000 = 1.845 -> 1.85, where
    half-even would give 1.84). So `SUM(amount_brl)` is NOT `SUM(amount) * rate` to the
    cent, by up to half a centavo per row, and `docs/f-api-run-evidence.md` says so in those
    words so a reader does not file the difference as a defect.

    THE ALTERNATIVE IS WORSE AND WAS WEIGHED: carrying seven decimals into the fact would
    make `amount_brl` a column whose scale no currency explains, unsummable against any
    ledger, and it would still round the moment anybody reported it -- once, over a total,
    where nobody can see which rows moved."""
    return (F.col(amount_column).cast(amount_type) * F.col(FX_RATE)).cast(amount_type)


def refuse_payments_no_rate_can_be_resolved(name: str, *, unresolved_rates: int) -> None:
    """Refuse a payment for which no published quote precedes its own instant -- T3 clause 3,
    and the DERIVED measure's own pre-write refusal.

    IT IS THE MIRROR OF `opl.gold.facts._refuse_payments_no_instant_can_be_read` AND IT HAS
    TO BE ITS OWN CHECK. That one reads `fact.measure` off BRONZE, before anything is
    derived, so it cannot see a column the FX join produces -- and a NULL `fx_rate` is the
    same failure in the same shape: `amount_brl` comes out NULL, every SUM comes back smaller
    with nothing to show for it, and the row count, the resolution rate and the orphan counts
    are all clean.

    A REFUSAL AND NOT A NULL, which is the phase plan's ruling rather than this module's
    preference: a payment below the series' first publication instant is a fact about the
    EXTRACTION WINDOW, and the repair is to extend it and re-run. It ships UNEXERCISED --
    nothing in this phase's range sits below 2026-06-03 -- and the evidence says so rather
    than reporting it as working."""
    if unresolved_rates:
        raise ValueError(
            f"refusing to build {name!r}: {unresolved_rates} payments carry no {FX_RATE}. "
            "No landed PTAX quote for that currency was published before the payment's own "
            f"instant, so the row is below the series' first quote or names a currency "
            f"{ptax.BRONZE_TABLE} does not cover. A NULL rate gives a NULL {AMOUNT_BRL} and "
            "lowers every total by an amount nobody can name -- extend the extraction "
            "window and re-run. Nothing has been written by this run"
        )
