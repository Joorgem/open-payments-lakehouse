# src/opl/gold/fact_guards.py
"""How the fact READS a payment's own columns, and everything that REFUSES what it reads --
extracted from `opl.gold.facts` and called from it.

WHY A SECOND MODULE, AND WHY THIS SEAM. F-API Task 4 added the FX columns, their pre-write
refusal and a guard over the fact's derived declarations, and `facts.py` reached 960 lines
against the project's 800-line cap (master protocol section 4.12: whoever touches a file at
the cap splits it FIRST). The seam is the same one `opl.gold.registry_guards` took in this
same phase, one layer down: what STOPS a build lives apart from what BUILDS it, so the
loader reads as the four things it does -- check, derive, append, reconcile -- rather than as
a list of refusals with a projection somewhere in the middle.

THE TWO CONSTANTS COME WITH THE REFUSALS AND THAT IS NOT AN ACCIDENT OF THE LINE COUNT.
`ISO_INSTANT_FORMAT` and `AMOUNT_TYPE` exist BECAUSE of the refusals below: the format is
chosen so that a value carrying no zone designator yields NULL instead of resolving through
`spark.sql.session.timeZone`, and `_refuse_payments_no_instant_can_be_read` is what turns
that NULL into a stop -- without it the choice would only make the wrong answer quieter. The
same is true of the type: the cast is what produces the NULL the same function refuses. Each
refusal's message NAMES the format or the type it enforces, so a reader who has one has both.
Both are re-exported by `opl.gold.facts`, so no consumer's import line moved.

WHERE EACH REFUSAL SITS RELATIVE TO THE WRITE, which is the property this module exists to
make legible in one place:

  * BEFORE ANY READ -- `_refuse_a_mismatched_source` and
    `_refuse_a_fact_this_loader_cannot_derive`. Both are answerable from the specs alone, so
    they run before a table is touched and can name a declaration rather than a column.
  * BEFORE THE FIRST WRITE, OVER BRONZE -- `_refuse_payments_no_instant_can_be_read` and
    `_refuse_a_deduplication_that_lost_a_business_tuple`. Affordable because bronze is small;
    there is nothing to keep, so the message says so.
  * BEFORE THE FIRST WRITE, OVER THE DERIVED FRAME -- `_refuse_unresolved_rates`. It cannot
    be one of the two above: the column it counts does not exist until the FX join has run.
  * AFTER THE WRITE -- `_refuse_a_row_count_that_is_not_one_per_delivered_identity` alone,
    and its message begins by saying the table is already on disk. It is the only one that
    cannot be moved earlier, because the number it checks is a property of the target.
"""
from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from opl.contracts import payments
from opl.gold.fx import (
    AMOUNT_BRL,
    FX_RATE,
    FX_RATE_DATE,
    refuse_payments_no_rate_can_be_resolved,
)
from opl.gold.spec_fields import FROM_DERIVED
from opl.gold.specs import ConformedDimension, PaymentFact, Scd2Dimension
from opl.vault.registry import Hub

__all__ = ["AMOUNT_TYPE", "ISO_INSTANT_FORMAT", "event_instant"]

# The payment column that decides whether a quote is consulted at all -- the reporting
# currency converts at exactly 1 with no lookup. SPELLED OUT for
# `opl.gold.registry.DIM_CHANNEL.fact_column`'s reason: the payment contract names it only as
# a member of `BUSINESS_ATTRIBUTE_COLUMNS` and not as a constant. It cannot drift behind a
# rename, because `_refuse_a_fact_this_loader_cannot_derive` below requires the fact to
# DECLARE this exact name as an input of its rate measure, and the contract refuses a
# business attribute it does not carry.
_CURRENCY_COLUMN = "currency"


# THE ZONE DESIGNATOR IS PART OF THE PATTERN AND THAT IS THE WHOLE POINT -- see the module
# docstring. `XXX` accepts `+HH:MM` and the literal `Z`; a value carrying neither yields
# NULL here, where a plain cast would resolve it through the session timezone.
ISO_INSTANT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

# THE MEASURE'S TYPE. The scale is the CONTRACT's (`AMOUNT_SCALE`, two, because BRL has
# centavos) rather than a literal, so a currency with a different minor unit moves it in
# one place. The precision is 18: `MAX_AMOUNT_CENTS` is 5,000,000, i.e. seven digits before
# the point, and a DECIMAL leaves no room for the binary rounding a DOUBLE would introduce
# into a column whose whole contract is that it is an exact number of centavos.
AMOUNT_TYPE = f"decimal(18, {payments.AMOUNT_SCALE})"


def event_instant(column: str) -> Column:
    """The INSTANT an ISO-8601 payment timestamp names, parsed with its ZONE REQUIRED.

    NOT `CAST(... AS TIMESTAMP)`, and the module docstring tabulates why with the
    measurement: the cast agrees with this to the microsecond for text that carries `Z`,
    and ACCEPTS text that does not, resolving it through `spark.sql.session.timeZone`. This
    yields NULL for that value, which `_refuse_payments_no_instant_can_be_read` turns into
    a refusal naming the rows.

    NOT `opl.gold.conformed.day_of` EITHER, and the two must not be confused. That one
    reads a calendar DAY out of the first ten characters, because a day rendered from an
    instant moves with the session zone; this one reads the INSTANT, which does not move at
    all. The fact carries both -- `event_date_key` from the text, the as-of comparison from
    the instant -- and they answer different questions about the same column."""
    return F.to_timestamp(F.col(column), ISO_INSTANT_FORMAT)


def _refuse_a_mismatched_source(
    fact: PaymentFact,
    dimension: Scd2Dimension,
    hub: Hub,
    conformed: Sequence[ConformedDimension],
) -> None:
    """The fact, its dimension, that dimension's hub and the conformed dimensions arrive as
    four arguments, so something has to check they belong together.

    Four arguments for `opl.gold.dimensions._refuse_a_mismatched_source`'s reason: a loader
    that resolved its sources through the module-level registry could not be driven with a
    throwaway spec. What each mismatch would cost is worth naming. Another dimension builds
    a well-formed fact against another table's version chain. A hub with a COMPOSITE
    business key cannot be reached from one counterparty column at all -- the join would be
    on the first component and would match every company sharing it. And the conformed
    dimensions are checked IN ORDER, because they are projected in order and a Delta append
    matches POSITIONALLY: two of them transposed writes each key into the other's column,
    and all three are integers, so nothing fails."""
    if dimension.name != fact.company_dimension:
        raise ValueError(
            f"payment fact {fact.name!r} resolves against {fact.company_dimension!r} and "
            f"was handed dimension {dimension.name!r}. Both role keys would be as-of "
            "lookups into another table's version chain, and the build would not fail "
            "doing it -- resolve the dimension with opl.gold.registry.table_spec rather "
            "than passing one by hand"
        )
    if len(hub.business_key_columns) != 1:
        raise ValueError(
            f"payment fact {fact.name!r} joins one counterparty column to "
            f"{dimension.name!r}, whose business key is "
            f"{list(hub.business_key_columns)} -- {len(hub.business_key_columns)} columns. "
            "A composite key cannot be reached from one column: the join would match on "
            "the first component alone and resolve every company that shares it"
        )
    handed = tuple(item.name for item in conformed)
    if handed != fact.conformed:
        raise ValueError(
            f"payment fact {fact.name!r} reaches {list(fact.conformed)} IN THAT ORDER and "
            f"was handed {list(handed)}. The keys are projected in the declared order and "
            "a Delta append matches positionally, so a permutation writes each dimension's "
            "key into another's column -- and all of them are integers, so nothing fails"
        )


# THE ONE PLACE THE TWO SPELLINGS OF THE FX COLUMNS CAN BE COMPARED, WHICH IS WHY THE NEXT
# TWO GUARDS EXIST. `opl.gold.registry` DECLARES `fx_rate`, `amount_brl` and `fx_rate_date` as
# literals -- `opl.gold.fx` cannot be imported there, because that would put pyspark behind
# the import of every gold spec and `gold_load_fact.py` refuses a mistyped table BEFORE
# `getOrCreate()` -- and `opl.gold.fx` declares the same three names as the columns it writes.
# This module imports both, so it is the only file that can ask whether they agree, and it
# asks before the first write.
#
# THEY ARE REFUSALS AND NOT TESTS, because what they catch is silent in the way this
# repository cares about. A spec renaming `fx_rate` to `rate` would leave
# `with_resolved_rates` writing `fx_rate` into a frame whose projection then asks for `rate`
# -- an `AnalysisException`, loudly. But a spec that DROPPED `amount_brl` while keeping
# `fx_rate` would build a fact with no additive measure at all, and every count in the run log
# would still be right.
#
# TWO FUNCTIONS AND NOT ONE, because they are answerable from different arguments: the
# measures from the fact alone, the role from the conformed dimensions it was handed. Written
# as one it crossed the 50-line cap on prose, which is the shape §6 of the phase plan records
# as this repository's own recurring cost.


def _refuse_a_fact_whose_measures_this_loader_cannot_derive(fact: PaymentFact) -> None:
    """Refuse a fact whose derived measures are not the two this loader computes, in order.

    THE CURRENCY COLUMN IS CHECKED THROUGH THE RATE MEASURE'S OWN INPUTS, which is what stops
    `_CURRENCY_COLUMN` above being a second spelling nothing guards."""
    if fact.derived_names != (FX_RATE, AMOUNT_BRL):
        raise ValueError(
            f"payment fact {fact.name!r} derives {list(fact.derived_names)} and this loader "
            f"computes exactly ({FX_RATE!r}, {AMOUNT_BRL!r}) in that order -- the rate from "
            "the PTAX series and the converted amount from the rate. Declaration order is "
            "computation order, and a fact missing the converted measure would build with no "
            "additive measure at all while every count in the run log stayed right"
        )
    inputs = {item.name: item.inputs for item in fact.derived}
    if _CURRENCY_COLUMN not in inputs[FX_RATE]:
        raise ValueError(
            f"payment fact {fact.name!r} computes {FX_RATE!r} from "
            f"{list(inputs[FX_RATE])}, which does not include {_CURRENCY_COLUMN!r}. That is "
            "the column deciding whether a quote is consulted at all -- the reporting "
            "currency converts at exactly 1 with no lookup -- and this loader reads it by "
            "that name, so a contract that renamed it must rename this declaration too"
        )


def _refuse_a_derived_role_this_loader_cannot_produce(
    fact: PaymentFact, conformed: Sequence[ConformedDimension]
) -> None:
    """Refuse a conformed dimension reached through a derived role over a column this loader
    does not write.

    THE ONLY DERIVED COLUMN THAT EXISTS IS `fx_rate_date`, and a role declaring any other one
    would fail inside Spark's analysis after a session has started, naming a column rather
    than the declaration that asked for it."""
    derived_columns = {
        role.fact_column
        for item in conformed
        for role in item.fact_roles
        if role.source == FROM_DERIVED
    }
    if derived_columns - {FX_RATE_DATE}:
        raise ValueError(
            f"payment fact {fact.name!r} reaches a conformed dimension through a derived "
            f"role over {sorted(derived_columns - {FX_RATE_DATE})}, and this loader produces "
            f"only {FX_RATE_DATE!r}. A derived role whose column no projection writes fails "
            "inside Spark's analysis after a session has started, naming a column rather "
            "than the declaration that asked for it"
        )


def _refuse_payments_no_instant_can_be_read(
    fact: PaymentFact, *, unreadable_instants: int, unreadable_amounts: int
) -> None:
    """Refuse a payment whose `event_time` carries no zone-qualified instant, or whose
    measure is not a number.

    BOTH ARE SILENT AND BOTH ARE THE SAME SHAPE. A NULL instant matches no half-open
    interval, so the row resolves to the ghost in BOTH roles and the build reports a
    resolution rate that is merely lower; a NULL measure sums to nothing and lowers every
    total by an amount nobody can name. The payment contract makes every column required
    and non-empty and the DQ gate rejects both spellings of absent, so either count above
    zero is a bronze row that did not come through the gate -- or an `event_time` written
    without its `Z`, which is the one this loader's parse exists to catch."""
    if unreadable_instants:
        raise ValueError(
            f"refusing to build {fact.name!r}: {unreadable_instants} bronze rows carry an "
            f"{payments.EVENT_TIME_COLUMN!r} from which no instant can be read. The format "
            f"is {ISO_INSTANT_FORMAT} and the ZONE DESIGNATOR IS REQUIRED -- a value "
            "without one would otherwise be resolved through spark.sql.session.timeZone, "
            "which makes the as-of answer a function of a cluster setting. Every such row "
            "matches no half-open interval, so it would resolve to the unknown member in "
            "BOTH roles with the build reporting success"
        )
    if unreadable_amounts:
        raise ValueError(
            f"refusing to build {fact.name!r}: {unreadable_amounts} bronze rows carry a "
            f"{fact.measure!r} that is not a {AMOUNT_TYPE}. It would be written NULL, and "
            "a SUM over the fact would come back smaller with nothing to show for it. The "
            "payment contract makes every column required and the DQ gate rejects blanks, "
            "so this is a row that did not come through the gate"
        )



def _refuse_a_deduplication_that_lost_a_business_tuple(
    fact: PaymentFact, *, source_tuples: int, retained_tuples: int
) -> None:
    """Refuse a deduplication that changed WHAT THE PAYMENTS WERE.

    A redelivery is byte-identical to its original, so removing it removes no tuple: this
    number must be equal, exactly, and a shortfall is one `transaction_id` delivered twice
    with DIFFERENT business attributes -- which is not a redelivery at all but a corrupt
    stream, and which no resolution rate and no row count can see. It is checked before the
    first write because there is nothing to keep."""
    if retained_tuples != source_tuples:
        raise ValueError(
            f"refusing to build {fact.name!r}: bronze holds {source_tuples} distinct "
            f"business-attribute tuples and the deduplicated payments hold "
            f"{retained_tuples}. Deduplication is by {fact.grain_key!r} and a redelivery "
            "is byte-identical to its original, so this cannot change -- a shortfall means "
            "one identity was delivered twice carrying DIFFERENT attributes, and the copy "
            "that survived was chosen by a sort. Nothing has been written by this run"
        )


def _refuse_a_row_count_that_is_not_one_per_delivered_identity(
    fact: PaymentFact, *, target_table: str, source_table: str, held: int, identities: int
) -> None:
    """Refuse a fact that is not exactly one row per distinct `grain_key` in bronze. THE
    GRAIN, ENFORCED INSTEAD OF OBSERVED.

    ONE NUMBER COVERING THE TWO FAILURES THAT NOTHING ELSE HERE SEES, and they push it in
    opposite directions. TOO HIGH is a MULTI-MATCH: a payment resolving to two versions of
    one company, which `BETWEEN` produces at a version boundary and a broken version chain
    produces everywhere. A fan-out does NOT lower the resolution rate -- both matches
    resolve -- so the count is the only measurement that can see it. TOO LOW is a
    deduplication taken over the wrong columns: on today's bronze, over the business
    attributes, that is 18,400 rows against 20,000 identities and 1,600 real payments
    deleted.

    CHECKED ON THE COUNT HELD AND NOT ON THE COUNT APPENDED, for `opl.gold.dimensions
    ._refuse_a_count_that_is_not_every_version_plus_the_ghost`'s reason: it is an invariant
    of every state this loader accepts, the idempotent re-run included, and the re-run is
    the branch a retry lands in (`max_retries: 0` does not prevent one).

    AFTER THE WRITE, so the message says the rows are on disk."""
    if held == identities:
        return
    diagnosis = (
        "a payment resolving to MORE THAN ONE dimension version -- a multi-match, which "
        "a resolution rate cannot see because both matches resolve -- or, on a re-run, "
        "payments this table holds that bronze no longer delivers"
        if held > identities
        else "payments deleted by a deduplication taken over something other than "
        f"{fact.grain_key!r}. Over the business attributes it is the LEGITIMATE REPEATS "
        "that go, which are real payments the stream emits on purpose"
    )
    raise ValueError(
        f"refusing to accept {target_table}: it holds {held} rows and {source_table} "
        f"holds {identities} distinct {fact.grain_key} values. The grain of this fact is "
        f"ONE ROW PER PAYMENT EVENT, so the difference is {diagnosis}. THE TABLE ON DISK "
        "IS ALREADY WRITTEN and must be dropped before a repaired build"
    )


def _refuse_unresolved_rates(fact: PaymentFact, rows: DataFrame) -> None:
    """Count the derived rows that carry no rate, and refuse before the append.

    ONE AGGREGATE OVER THE DERIVED FRAME, which is the SECOND time it is derived and is said
    plainly in the module docstring rather than hidden. It is affordable for
    `_measured_source`'s reason -- 40,150 rows through one window and three broadcast joins,
    where the dimension is the expensive thing and is not touched again.

    AND IT MUST BE HERE RATHER THAN IN `_bronze`. That function refuses over BRONZE, before
    anything is derived, so it cannot see a column the FX join produces; this is the same
    refusal at the only point where the column exists and nothing has been written.

    ONE COUNT AND NOT TWO. `amount_brl` is NULL exactly when `fx_rate` is, because the
    delivered amount has already been refused unreadable by `_bronze` -- so a second count
    over `amount_brl` would be true under every implementation and would read as two checks.
    The quote DATE's own absence is refused one layer up, over the series, where the message
    can name the landed row."""
    measured = rows.agg(F.count(F.when(F.col(FX_RATE).isNull(), 1)).alias("rates")).collect()[0]
    refuse_payments_no_rate_can_be_resolved(fact.name, unresolved_rates=measured["rates"])


