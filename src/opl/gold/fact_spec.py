# src/opl/gold/fact_spec.py
"""The star's FACT kind and the measures it declares -- `PaymentFact` and `DerivedMeasure`,
each refusing at its own `__post_init__` everything that can be decided about one fact in
isolation.

SPLIT OUT OF `opl.gold.specs` AT THE MOMENT F-API TASK 4 WIDENED IT PAST THE CAP. That
file's own docstring had pre-declared the first split ("only when this file approaches the
project's 800-line cap do the kinds move wholesale to an `opl.gold.specs` module") and this
is the same argument one level down: the fact gained two fields, two guards and a derived
measure kind, and the file reached 854 of 800. The seam is DEPENDENCY DIRECTION --
`opl.gold.spec_fields` <- this module <- `opl.gold.specs` -- because the `GoldTable` union
has to see both the fact and the dimensions, so exactly one of the two may import the
other. `specs` keeps the union and every consumer keeps importing `PaymentFact` from there.

WHICH GUARD LIVES WHERE, unchanged by the move: everything here is answerable from one fact
and nothing else -- is the grain the event's own identity, is the delivered measure a
business attribute, does every counterparty play a role, is exactly one measure summable,
does every derived measure's input exist. Everything that needs the OTHER tables -- a
conformed dimension the registry does not hold, a foreign key two of them spell the same --
stays in `opl.gold.registry_guards`.

NOTHING HERE IMPORTS PYSPARK, which is load-bearing rather than tidy: a fact must be
refusable in a plain `python -c`, so `gold_load_fact.py` can reject a mistyped table before
`getOrCreate()`.

--- WHY A MEASURE DECLARES ITS ADDITIVITY (F-API T4a) --------------------------------

`PaymentFact` carried one `measure: str` and nothing anywhere said what summing it means.
That was harmless while `opl.contracts.payments.CURRENCIES` had one member and became wrong,
silently, the moment a second one landed: `SUM(amount)` over a mixed-currency column is a
number with no unit, and it was still the one column the spec called *the measure*. Nothing
failed, and nothing could -- there was no field to be wrong.

THREE ADDITIVITIES AND NOT A BOOLEAN, because the three columns this fact now carries are
three different answers. `amount_brl` is ADDITIVE -- one currency, sum it. `amount` is
ADDITIVE ONLY WITHIN A CURRENCY -- a legitimate measure under `GROUP BY currency` and
meaningless without one. `fx_rate` is NON-ADDITIVE -- a ratio, whose sum is nonsense and
whose unweighted mean is wrong even where a reader wants an average. A boolean would have to
call two of those three the same thing, and the one it merged them into is the one a reader
sums.

AND THE DELIVERED MEASURE IS NOT THE DERIVED ONE, WHICH IS WHY BOTH ARE DECLARED.
`_assert_the_measure_is_one_the_payment_carries` refuses `amount_brl` correctly: no payment
carries it. `opl.gold.facts._measured_source` goes further and makes the point structural --
it reads `fact.measure` off BRONZE, before anything is derived, to count unreadable amounts,
so a derived name in that field is an `AnalysisException` on a column bronze does not have
rather than a guard's message. So `measure` stays `amount` and keeps its pre-write refusal,
and the converted column is declared in `derived`, where it gets a refusal of its own over
the frame the FX join produced.
"""
from __future__ import annotations

from dataclasses import dataclass

from opl.contracts import payments
from opl.gold.columns import DIMENSION_COLUMNS
from opl.gold.spec_fields import (
    ADDITIVE,
    ADDITIVE_WITHIN_CURRENCY,
    ADDITIVITIES,
    _assert_every_field_is_named,
    _assert_one_of,
)

__all__ = ["DerivedMeasure", "PaymentFact"]


@dataclass(frozen=True, kw_only=True)
class DerivedMeasure:
    """A fact column this layer COMPUTES: its name, the columns it is computed from, and
    what summing it means.

    `inputs` IS DECLARED AND IS CHECKED AGAINST THE COMPUTATION ORDER, which is the half
    that makes this more than documentation. `amount_brl` is computed from `amount` and
    `fx_rate`, and `_assert_the_derived_measures_are_computable` refuses an input that is
    neither a payment-contract column nor a derived measure declared EARLIER in the same
    tuple -- so the declaration order IS the computation order, and a measure cannot claim
    an input nothing produces.

    THE NAME IS REFUSED IN BOTH DIRECTIONS. A derived measure that were a contract column
    would be one projection writing the delivered value and the computed one into one
    column, so one survives, every row is still present, and every total is plausible; a
    derived measure named like a column the LOADER writes is the same collision against
    `load_date`."""

    name: str
    inputs: tuple[str, ...]
    additivity: str

    def __post_init__(self) -> None:
        _assert_every_field_is_named(
            "derived measure",
            self.name,
            {"name": self.name, "additivity": self.additivity},
        )
        _assert_one_of(
            "derived measure", self.name, "an additivity", self.additivity, ADDITIVITIES
        )
        object.__setattr__(self, "inputs", tuple(self.inputs))
        self._assert_the_name_is_this_layers_own()
        self._assert_the_inputs_are_a_set_this_measure_is_not_in()

    def _assert_the_name_is_this_layers_own(self) -> None:
        """Refuse a derived name the payment delivers or the loaders write."""
        if self.name in payments.COLUMNS:
            raise ValueError(
                f"derived measure {self.name!r} is a column the payment contract delivers "
                f"({list(payments.COLUMNS)}). One projection writes the delivered value "
                "and the computed one into one column, so one survives, every row is still "
                "present and every total is plausible"
            )
        if self.name in DIMENSION_COLUMNS:
            raise ValueError(
                f"derived measure {self.name!r} is a column the gold loaders write "
                f"themselves ({', '.join(sorted(DIMENSION_COLUMNS))}). One projection, two "
                "values, and the measure is silently a timestamp"
            )

    def _assert_the_inputs_are_a_set_this_measure_is_not_in(self) -> None:
        """Refuse an empty, repeating or self-referential input list.

        EMPTY is a measure that declares itself computed from nothing, which is the one
        shape the computability guard cannot check: there is no input for it to resolve, so
        the declaration passes and asserts nothing. SELF-REFERENTIAL is the same defect one
        step along -- `amount_brl` computed from `amount_brl` resolves against the tuple's
        own earlier entries, so it would pass the moment the name were declared twice."""
        if not self.inputs:
            raise ValueError(
                f"derived measure {self.name!r} declares no input. A measure computed from "
                "nothing cannot be checked against what the fact carries, so the "
                "declaration would pass and assert nothing"
            )
        seen: set[str] = set()
        for value in self.inputs:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"derived measure {self.name!r} declares a blank input: {self.inputs!r}"
                )
            if value in seen:
                raise ValueError(
                    f"derived measure {self.name!r} declares {value!r} twice as an input: "
                    f"{self.inputs!r}"
                )
            seen.add(value)
        if self.name in seen:
            raise ValueError(
                f"derived measure {self.name!r} declares itself as one of its own inputs. "
                "It would resolve against an earlier entry of the same name rather than "
                "failing, so a name declared twice would make this a definition of itself"
            )


def _assert_the_grain_is_the_events_own_identity(name: str, grain_key: str) -> None:
    """Refuse a fact grain the contract does not carry, and refuse a BUSINESS ATTRIBUTE
    outright -- which is a 3,200-payment deletion declared in one field.

    THE ARITHMETIC, ON TODAY'S BRONZE -- WHICH IS FOUR PROMOTED STREAMS AND NOT THE TWO
    THIS PARAGRAPH USED TO NAME. Every profile carries `_REPEAT_COUNT = 800` LEGITIMATE
    REPEATS: a DIFFERENT `transaction_id` under an IDENTICAL business-attribute tuple,
    emitted on purpose so a duplicate has something to be confused with
    (`opl.contracts.payments`). 40,150 rows hold 40,000 distinct `transaction_id` and 36,800
    distinct tuples -- so a fact grained on the attributes, or on a "natural key" built from
    (payer, payee, amount, currency, payment_method), which is the obvious thing to reach
    for, silently deletes 3,200 real payments and returns a plausible 36,800. (It read
    20,150 / 20,000 / 18,400 / 1,600 until F-API Task 4, which is what a count written into
    a docstring does when a stream lands.)

    THE DEDUP ACCEPTANCE CANNOT CATCH IT, which is why this is a refusal at declaration and
    not a test over a load. "The build removed 150 duplicates" is `COUNT(*) - COUNT(DISTINCT
    <grain>)` BY DEFINITION OF THE OPERATION: it re-measures bronze's own arithmetic and
    comes out right whichever column the dedup was taken over."""
    if grain_key not in payments.COLUMNS:
        raise ValueError(
            f"payment fact {name!r} is grained on {grain_key!r}, which is no column the "
            f"payment contract declares ({', '.join(payments.COLUMNS)}). The projection "
            "would fail inside Spark's analysis, after a session has started"
        )
    if grain_key in payments.BUSINESS_ATTRIBUTE_COLUMNS:
        raise ValueError(
            f"payment fact {name!r} is grained on {grain_key!r}, which is a BUSINESS "
            f"ATTRIBUTE ({', '.join(payments.BUSINESS_ATTRIBUTE_COLUMNS)}). A legitimate "
            "repeat is a different transaction_id under an identical attribute tuple, so "
            "this grain deletes every one of them: 3,200 real payments on today's bronze, "
            "leaving a plausible 36,800 and a dedup count that still reports 150"
        )


def _assert_the_measure_is_one_the_payment_carries(
    name: str, measure: str, grain_key: str
) -> None:
    """Refuse a DELIVERED measure that is not a business attribute of the payment.

    A FACT'S DELIVERED MEASURE IS WHAT THE EVENT WAS WORTH AS THE PAYMENT CARRIED IT, and
    the contract's own partition is the check: `BUSINESS_ATTRIBUTE_COLUMNS` is what the
    payment WAS, as opposed to which delivery of it a row is. A measure taken from outside
    it would be a number summed over a property of the DELIVERY -- `emitted_at` aggregates
    to something, and it means nothing.

    THIS IS WHY `amount_brl` IS A `DerivedMeasure` AND NOT THIS FIELD, and the refusal is
    right rather than in the way -- the module docstring carries the mechanism."""
    if measure not in payments.BUSINESS_ATTRIBUTE_COLUMNS:
        raise ValueError(
            f"payment fact {name!r} measures {measure!r}, which is not a business "
            f"attribute of a payment ({', '.join(payments.BUSINESS_ATTRIBUTE_COLUMNS)}). "
            "A measure outside that tuple is a property of the DELIVERY, and a SUM over "
            "one is a number with no business meaning"
        )
    if measure == grain_key:
        raise ValueError(
            f"payment fact {name!r} spells {measure!r} as both its grain and its measure. "
            "One projection writes both into one column, so one value survives and every "
            "row is still there"
        )


def _assert_exactly_one_measure_is_additive(
    name: str, *, measure: str, additivity: str, derived: tuple[DerivedMeasure, ...]
) -> None:
    """Refuse a fact with no summable measure, with two, or whose DELIVERED measure claims
    to be summable across a currency domain that has more than one member.

    THE SECOND HALF IS F-API T4a, AND IT FIRES ON A DECLARATION THAT A VALUE CHANGE MADE
    WRONG. `amount` was this fact's only measure and summing it meant something while
    `payments.CURRENCIES` held one member. The moment the domain gained USD, `SUM(amount)`
    became a mixed-currency number with no unit -- and nothing anywhere failed, because the
    declaration had not changed. Reading the DOMAIN rather than the data is what makes this a
    refusal at import instead of a measurement after a load: the fact-side mix is a property
    of which streams have landed, and the domain is a property of the contract.

    EXACTLY ONE, because "the measure" is what a reader sums without being told which column
    to sum. Zero leaves the star with no answer to that question; two leave the answer to
    whichever column a report picked, and those two disagree by the rate."""
    _assert_one_of("payment fact", name, "a measure additivity", additivity, ADDITIVITIES)
    if additivity == ADDITIVE and len(payments.CURRENCIES) > 1:
        raise ValueError(
            f"payment fact {name!r} declares its delivered measure {measure!r} {ADDITIVE} "
            f"and the currency domain is {list(payments.CURRENCIES)}. A SUM over a column "
            f"denominated in more than one currency is a number with no unit: declare it "
            f"{ADDITIVE_WITHIN_CURRENCY} and let a DERIVED measure converted into one "
            "currency be the additive one"
        )
    additive = [measure] * (additivity == ADDITIVE) + [
        item.name for item in derived if item.additivity == ADDITIVE
    ]
    if len(additive) != 1:
        raise ValueError(
            f"payment fact {name!r} declares {len(additive)} {ADDITIVE} measures "
            f"({additive}) and needs exactly one. A reader sums THE measure without being "
            "told which column that is: with none there is no answer, and with two the "
            "answer is whichever column a report picked -- and they disagree by the rate"
        )


def _assert_the_derived_measures_are_computable(
    name: str, derived: tuple[DerivedMeasure, ...], *, grain_key: str, measure: str
) -> None:
    """Refuse a derived measure whose inputs nothing produces, or whose name another of the
    fact's own columns already holds.

    THE DECLARATION ORDER IS THE COMPUTATION ORDER, and that is what this checks. An input
    resolves against the payment contract or against a derived measure declared EARLIER in
    the tuple -- so `amount_brl`'s `fx_rate` input is resolvable only because `fx_rate` is
    declared above it, and swapping the two turns the import of every gold module red
    instead of producing a NULL column that lowers every total by an amount nobody can name.

    IT IS NOT A TOPOLOGICAL SORT, DELIBERATELY. A projection is a list of columns in one
    `select`, so the order a reader sees IS the order the frame builds them in; resolving a
    dependency graph here would let the declaration disagree with the projection and leave
    the two to be reconciled by whoever reads the failure."""
    produced = {grain_key, measure, *payments.COLUMNS}
    for item in derived:
        if item.name in produced:
            raise ValueError(
                f"payment fact {name!r} derives {item.name!r} and already projects that "
                f"name ({sorted(produced)}). One projection writes two values into one "
                "column, so one of them survives and every row is still present"
            )
        unresolved = sorted(set(item.inputs) - produced)
        if unresolved:
            raise ValueError(
                f"payment fact {name!r} computes {item.name!r} from {unresolved}, which "
                "neither the payment contract carries nor an earlier derived measure "
                f"produces (available: {sorted(produced)}). Declaration order IS "
                "computation order: a measure declared above the one it reads resolves to "
                "nothing, and a NULL measure lowers every total by an amount nobody can name"
            )
        produced.add(item.name)


def _assert_every_counterparty_plays_exactly_one_role(
    name: str, roles: tuple[tuple[str, str], ...]
) -> None:
    """Refuse a fact that does not carry a foreign key for EVERY counterparty the contract
    declares -- one per role, both roles, into ONE dimension.

    THE PHASE PLAN'S CLOSING TEST IS ILL-FORMED AND ITS DANGEROUS READING IS REFUSED HERE.
    It asks that "every `fact_payment` row resolve to exactly one `dim_company` version". A
    correct row resolves to TWO, one per role; the reading that IS satisfiable is a fact
    that joins on the PAYER alone and never builds the payee's key, and nothing about that
    fails -- the row count is right, the resolution rate is a clean 100%, and every report
    grouping by payee returns nothing.

    `COUNTERPARTY_COLUMNS` AND NOT A COUNT OF TWO. `opl.contracts.payments` names the two as
    their own tuple and refuses at import any member of it that is not also a business
    attribute; a check for "two roles" would accept a fact that played the payer twice."""
    counterparties = tuple(counterparty for counterparty, _key in roles)
    keys = tuple(key for _counterparty, key in roles)
    if len(set(counterparties)) != len(counterparties) or set(counterparties) != set(
        payments.COUNTERPARTY_COLUMNS
    ):
        raise ValueError(
            f"payment fact {name!r} declares roles for {list(counterparties)}, and the "
            f"contract's counterparties are {list(payments.COUNTERPARTY_COLUMNS)}. Every "
            "one of them must play exactly one role: a fact that resolves the payer alone "
            "is a star in which half of every payment is invisible, with the row count "
            "right and the resolution rate a clean 100%"
        )
    if len(set(keys)) != len(keys):
        raise ValueError(
            f"payment fact {name!r} gives two roles the same foreign key column: "
            f"{list(keys)}. One projection writes both, so one role's key survives and "
            "every payment appears to have paid itself"
        )


def _assert_the_facts_own_columns_are_its_own(
    name: str,
    keys: tuple[str, ...],
    grain_key: str,
    measure: str,
    derived_names: tuple[str, ...] = (),
) -> None:
    """Refuse a role key the loader would overwrite, or that is already a column of the
    payment this fact projects -- delivered or DERIVED.

    THE RESERVED SET IS GOLD'S WHOLE ONE, for `opl.gold.specs._assert_the_key_columns_are
    _the_dimensions_own`'s reason -- one namespace, one reserved set. The CONTRACT set is
    this kind's own addition: a fact projects the grain, the measure and `event_time` under
    the names the contract gives them, so a role key spelled `amount` is one projection
    writing two values into one column, with every row present and the measure silently a
    hash.

    THE DERIVED NAMES ARE IN IT BECAUSE THEY ARE PROJECTED TOO. `fx_rate` and `amount_brl`
    are columns of this fact exactly as `amount` is, so a role key spelled `fx_rate` is the
    same collision as one spelled `amount` -- and it is the quieter of the two, because a
    company surrogate key and a rate are both numbers of plausible magnitude."""
    for key in keys:
        if key in derived_names:
            raise ValueError(
                f"payment fact {name!r} names {key!r} as a role's foreign key, and the fact "
                f"DERIVES a measure of that name ({list(derived_names)}). One projection "
                "writes both, so the surrogate key and the computed measure become one "
                "column of plausible numbers"
            )
        if key in DIMENSION_COLUMNS:
            raise ValueError(
                f"payment fact {name!r} names {key!r} as a role's foreign key, and the "
                f"gold loaders write that column themselves "
                f"({', '.join(sorted(DIMENSION_COLUMNS))}). One projection, two values, "
                "and the key is silently a timestamp -- every join on it matches nothing"
            )
        if key in payments.COLUMNS or key in (grain_key, measure):
            raise ValueError(
                f"payment fact {name!r} names {key!r} as a role's foreign key, and the "
                f"fact already projects that name from the payment "
                f"({', '.join(payments.COLUMNS)}). One projection writes both, so the "
                "delivered value disappears and the column is full of plausible numbers"
            )


def _assert_the_conformed_dimensions_are_a_set(name: str, conformed: tuple[str, ...]) -> None:
    """Refuse an empty or repeating conformed list -- NOT `opl.gold.specs._assert_the
    _declared_values_are_a_set`, whose two messages are about a DIMENSION's members and
    would misdiagnose both of these.

    EMPTY is a fact that reaches no conformed dimension at all: `dim_date`, `dim_channel`
    and `dim_currency` would each be a table nothing joins to, which is the "decorative in a
    star schema" charge `opl.gold.pit` levels at itself and the one thing a FACT can stop
    being true. REPEATED is a column projected twice under one name: the second overwrites
    the first, and both happen to be the same value, so the table is correct and one column
    wide of what its schema says."""
    if not conformed:
        raise ValueError(
            f"payment fact {name!r} declares no conformed dimension. Every conformed "
            "dimension in this star exists to be reached by this fact; a fact that reaches "
            "none leaves all of them decorative, with every build reporting success"
        )
    if len(set(conformed)) != len(conformed):
        raise ValueError(
            f"payment fact {name!r} declares a conformed dimension twice: {conformed}. Its "
            "foreign key would be projected twice under one name, and the second would "
            "overwrite the first with the same value -- a table one column narrower than "
            "its own declaration, and nothing fails"
        )


@dataclass(frozen=True, kw_only=True)
class PaymentFact:
    """The star's fact: ONE ROW PER PAYMENT EVENT, a degenerate `transaction_id`, and a
    foreign key per counterparty ROLE into one shared `dim_company`.

    TWO ROLE-PLAYING KEYS INTO ONE DIMENSION, WHICH IS WHAT `roles` IS FOR AND WHAT THE
    PHASE PLAN NEVER SAID. `opl.contracts.payments.COUNTERPARTY_COLUMNS` is two columns;
    conformance means ONE `dim_company` answers for both, so the fact carries
    `payer_company_sk` and `payee_company_sk` and both resolve against the same table.
    `_assert_every_counterparty_plays_exactly_one_role` argues why that is a refusal rather
    than a convention.

    THE COUNTERPARTY HALF OF EACH PAIR IS THE CONTRACT'S; THE KEY HALF IS THIS LAYER'S. That
    is why the pairs are DECLARED and not derived: `payer_company_sk` is a name gold
    invents, and deriving it by string surgery on `payer_cnpj_basico` would be a rename away
    from silently producing a column nothing joins. The counterparty half cannot drift,
    because the guard above refuses any spelling the contract does not carry.

    TWO MEASURES ARE DECLARED WHERE THERE WAS ONE, AND THE DELIVERED ONE IS NO LONGER THE
    SUMMABLE ONE (F-API T4a). `measure` is still `amount` -- the column the payment carries,
    and the column `opl.gold.facts._measured_source` refuses over BRONZE before anything is
    derived -- and `measure_additivity` now says what summing it means, which for a fact
    whose currency domain has two members is ADDITIVE ONLY WITHIN A CURRENCY. `derived`
    carries the columns this layer computes, and one of them, `amount_brl`, is the single
    additive measure a reader sums without being told which column to sum.

    `conformed` IS DECLARED IN PROJECTION ORDER, exactly as `PointInTimeTable.satellites` is
    and for the same reason: a Delta append matches POSITIONALLY, so the order is the table's
    shape. It is also what makes "no conformed dimension is decorative" checkable --
    `opl.gold.registry_guards` refuses a registry whose conformed dimensions are not exactly
    the set this fact reaches. It is a list of dimension NAMES and not of keys, and since
    F-API T4b one of those dimensions answers TWO of the fact's keys: `opl.gold.specs
    .fact_keys` is what turns a name into its columns.

    NO `source_satellite` AND NO `hub`. A fact reads BRONZE, not the vault: its rows are
    payments, and the only vault-derived thing it touches is `dim_company`, which it names
    through `company_dimension` -- a GOLD table, resolved against the gold registry rather
    than against `opl.vault.domains`. That is the field that makes this kind refusable in an
    entry point before a session exists.

    `kw_only`, like every spec here: `name`, `grain_key`, `measure`, `measure_additivity`
    and `company_dimension` are five adjacent strings, and a positional construction that
    permuted them would type-check perfectly and register a fact grained on `amount`."""

    name: str
    grain_key: str
    measure: str
    measure_additivity: str
    company_dimension: str
    roles: tuple[tuple[str, str], ...]
    derived: tuple[DerivedMeasure, ...]
    conformed: tuple[str, ...]

    def __post_init__(self) -> None:
        _assert_every_field_is_named(
            "payment fact",
            self.name,
            {
                "name": self.name,
                "grain key": self.grain_key,
                "measure": self.measure,
                "company dimension": self.company_dimension,
            },
        )
        object.__setattr__(self, "roles", tuple(tuple(role) for role in self.roles))
        object.__setattr__(self, "derived", tuple(self.derived))
        object.__setattr__(self, "conformed", tuple(self.conformed))
        _assert_the_grain_is_the_events_own_identity(self.name, self.grain_key)
        _assert_the_measure_is_one_the_payment_carries(self.name, self.measure, self.grain_key)
        _assert_exactly_one_measure_is_additive(
            self.name,
            measure=self.measure,
            additivity=self.measure_additivity,
            derived=self.derived,
        )
        _assert_the_derived_measures_are_computable(
            self.name, self.derived, grain_key=self.grain_key, measure=self.measure
        )
        _assert_every_counterparty_plays_exactly_one_role(self.name, self.roles)
        _assert_the_facts_own_columns_are_its_own(
            self.name, self.role_keys, self.grain_key, self.measure, self.derived_names
        )
        _assert_the_conformed_dimensions_are_a_set(self.name, self.conformed)

    @property
    def role_keys(self) -> tuple[str, ...]:
        """The foreign-key columns this fact carries into `company_dimension`, in
        DECLARATION order -- which is the order they are projected in, and a Delta append
        matches positionally."""
        return tuple(key for _counterparty, key in self.roles)

    @property
    def derived_names(self) -> tuple[str, ...]:
        """The derived measure columns, in DECLARATION order -- which is both the projection
        order (a Delta append matches positionally) and the COMPUTATION order, since
        `_assert_the_derived_measures_are_computable` resolves each measure's inputs against
        the ones declared above it."""
        return tuple(item.name for item in self.derived)

    @property
    def additive_measure(self) -> str:
        """The one column a reader may SUM without a `GROUP BY currency`.

        Total by `_assert_exactly_one_measure_is_additive`, and an unpacking rather than a
        `next(...)` for `opl.gold.specs.contract_role`'s reason: if that guard were ever
        weakened this raises instead of quietly answering with whichever column came
        first."""
        (name,) = [self.measure] * (self.measure_additivity == ADDITIVE) + [
            item.name for item in self.derived if item.additivity == ADDITIVE
        ]
        return name
