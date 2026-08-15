"""The FACT kind's own refusals -- what one `PaymentFact` may declare about itself, and the
two whole-set checks that are about the fact rather than about the registry.

SPLIT OUT OF `test_registry.py` WHEN F-API TASK 4 TOOK IT PAST THE CAP. That file reached 945
lines of the project's 800 (master protocol section 4.12: whoever touches a file at the cap
splits it FIRST), and the seam is the one the SOURCE took in the same commit: `opl.gold.specs`
kept the dimension kinds and `opl.gold.fact_spec` took the fact, so the tests follow.

WHAT STAYED BEHIND, because the division is not "everything mentioning the fact". The
cross-layer name guards, the duplicate-name guards and the conformed-dimension guards sweep
over EVERY kind including this one -- a name collision is a property of the name -- so they
stay with the registry. What is here is what only a fact can be wrong about: its grain, its
measures, its role keys, the dimension it resolves against, and the two derived-measure
refusals T4a added.

THE PROBES COME FROM `spec_probes.py` AND ARE NOT REDECLARED HERE. Two copies of `_fact` is
the drift this repository polices hardest, and a factory whose two copies disagree makes each
file's refusals a claim about a different spec.
"""
from __future__ import annotations

import pytest

from opl.contracts import payments
from opl.gold.columns import IS_CURRENT, VALID_FROM, VALID_TO
from opl.gold.registry import (
    DIM_CHANNEL,
    DIM_COMPANY,
    DIM_CURRENCY,
    DIM_DATE,
    FACT_PAYMENT,
    REGISTRY,
    DerivedMeasure,
    EnumeratedDimension,
    build_registry,
)
from opl.gold.spec_fields import (
    ADDITIVE,
    ADDITIVE_WITHIN_CURRENCY,
    FROM_DERIVED,
    NON_ADDITIVE,
)
from opl.gold.specs import fact_keys

from .spec_probes import _dimension, _fact

# --- the fact kind -------------------------------------------------------------------


def test_the_fact_carries_a_role_key_for_every_counterparty_the_contract_declares():
    """T-A's DECLARATION half. The acceptance this phase inherited -- "every
    `fact_payment` row resolves to exactly one `dim_company` version" -- is ill-formed: a
    correct row resolves to TWO, one per role. This pins that both roles exist, that both
    resolve against the SAME dimension (which is what "conformed" means), and that the
    counterparty half of each pair is the contract's own column and not a copy."""
    assert FACT_PAYMENT.roles == (
        ("payer_cnpj_basico", "payer_company_sk"),
        ("payee_cnpj_basico", "payee_company_sk"),
    )
    assert tuple(c for c, _k in FACT_PAYMENT.roles) == payments.COUNTERPARTY_COLUMNS
    assert FACT_PAYMENT.company_dimension == DIM_COMPANY.name
    assert FACT_PAYMENT.role_keys == ("payer_company_sk", "payee_company_sk")
    assert FACT_PAYMENT.grain_key == payments.IDENTITY_COLUMN


@pytest.mark.parametrize(
    "roles",
    [
        (("payer_cnpj_basico", "payer_company_sk"),),
        (("payee_cnpj_basico", "payee_company_sk"),),
        (("payer_cnpj_basico", "a"), ("payer_cnpj_basico", "b")),
    ],
    ids=["payer only", "payee only", "payer twice"],
)
def test_a_fact_that_does_not_resolve_every_counterparty_is_refused(roles):
    """THE READING OF THE PLAN'S CLOSING TEST THAT IS ACTUALLY SATISFIABLE, refused at
    declaration. A fact joining on the payer alone has the right row count, a clean 100%
    resolution rate, and no payee at all -- so every report grouped by payee returns
    nothing and nothing about the build fails. "Payer twice" is the same defect wearing two
    roles, which is why the guard reads `COUNTERPARTY_COLUMNS` rather than counting."""
    with pytest.raises(ValueError, match="must play exactly one role|declares roles for"):
        _fact(roles=roles)


@pytest.mark.parametrize("attribute", payments.BUSINESS_ATTRIBUTE_COLUMNS)
def test_a_fact_grained_on_a_business_attribute_is_refused(attribute):
    """T-D AT DECLARATION, over every business attribute rather than over the one somebody
    would reach for first. A legitimate repeat is a DIFFERENT `transaction_id` under an
    IDENTICAL attribute tuple, so a grain taken from that tuple deletes 3,200 real payments
    on today's bronze and returns a plausible 36,800 -- with the duplicate count still
    reporting 150, because that number is `COUNT(*) - COUNT(DISTINCT <grain>)` by
    definition of the operation and cannot fail.

    *(These read 1,600 / 18,400 until the consolidation pass -- the two-promoted-stream
    figures, two streams out of date. `gold_fact_payment_job.yml` predicts 3,200 / 36,800
    and the run of 2026-08-14 measured both.)*"""
    with pytest.raises(ValueError, match="BUSINESS ATTRIBUTE"):
        _fact(grain_key=attribute)


def test_a_fact_grained_or_measured_on_a_column_the_contract_does_not_declare():
    """The other half of the grain refusal, and the reason gold may spell a contract column
    name at all: a name v1 does not carry turns the import of every gold module red rather
    than failing inside Spark's analysis after a session has started."""
    with pytest.raises(ValueError, match="no column the payment contract declares"):
        _fact(grain_key="transaction_di")
    with pytest.raises(ValueError, match="not a business attribute of a payment"):
        _fact(measure=payments.EMITTED_AT_COLUMN)


@pytest.mark.parametrize("column", [VALID_FROM, VALID_TO, IS_CURRENT, "load_date"])
def test_a_role_key_that_is_a_column_a_gold_loader_writes_is_refused(column):
    """One gold namespace, one reserved set -- the collision every other kind here refuses,
    over the two column names a fact declares for itself."""
    with pytest.raises(ValueError, match="the gold loaders write that column"):
        _fact(roles=(("payer_cnpj_basico", column), ("payee_cnpj_basico", "b")))


@pytest.mark.parametrize("column", ["amount", "event_time", "transaction_id"])
def test_a_role_key_that_is_a_column_the_fact_projects_from_the_payment_is_refused(column):
    """The collision no other kind has, because no other kind projects contract columns. A
    fact carries the grain, the measure and `event_time` under the contract's own names, so
    a role key spelled `amount` is one projection writing two values into one column -- the
    measure survives or the key does, every row is present, and the join matches nothing."""
    with pytest.raises(ValueError, match="already projects that name from the payment"):
        _fact(roles=(("payer_cnpj_basico", column), ("payee_cnpj_basico", "b")))


def test_a_fact_whose_company_dimension_is_missing_or_is_not_an_scd2_dimension():
    """Resolved against THIS registry and not the vault's, which is what makes this kind
    different from every other one here: a fact reads bronze and joins to a GOLD table.
    Handed a conformed dimension it would look for a half-open interval in a table that has
    none -- caught by Spark, and only after a serverless session had started."""
    star = (DIM_COMPANY, DIM_DATE, DIM_CHANNEL, DIM_CURRENCY)
    with pytest.raises(ValueError, match="not a registered SCD2 dimension"):
        build_registry((_fact(company_dimension="dim_compnay"), *star))
    with pytest.raises(ValueError, match="not a registered SCD2 dimension"):
        build_registry((_fact(company_dimension="dim_date"), *star))


def test_a_conformed_dimension_the_fact_does_not_reach_is_refused():
    """THE ONLY MECHANICAL ANSWER THIS REPOSITORY HAS TO "DECORATIVE IN A STAR SCHEMA".
    A conformed dimension exists to be reached by a fact; one the fact does not name builds
    fine, is well-formed, and returns its members and no facts in every report. Stated as an
    EQUALITY against the registry's conformed set, so the omission turns the import red
    rather than being a convention somebody has to remember."""
    with pytest.raises(ValueError, match="reaches .* and this registry holds"):
        build_registry(
            (_fact(conformed=("dim_date", "dim_channel")), DIM_COMPANY, DIM_DATE,
             DIM_CHANNEL, DIM_CURRENCY)
        )
    with pytest.raises(ValueError, match="reaches .* and this registry holds"):
        build_registry((_fact(), DIM_COMPANY, DIM_DATE, DIM_CHANNEL))


def test_two_of_the_facts_projected_columns_sharing_a_name_are_refused():
    """A WHOLE-SET GUARD BECAUSE HALF THE COLUMN LIST IS OTHER TABLES'. Two enumerated
    dimensions sharing a `surrogate_key` are legal on their own -- nothing in this registry
    refuses it, and their NAMES differ -- and the fact then projects one foreign key where
    it declares two. Both are integers, so nothing fails and one dimension is simply
    unreachable."""
    collided = EnumeratedDimension(
        name=DIM_CURRENCY.name,
        surrogate_key=DIM_CHANNEL.surrogate_key,
        natural_key=DIM_CURRENCY.natural_key,
        fact_column=DIM_CURRENCY.fact_column,
        members=DIM_CURRENCY.members,
    )
    with pytest.raises(ValueError, match="projects .* more than once"):
        build_registry((_fact(), DIM_COMPANY, DIM_DATE, DIM_CHANNEL, collided))


def test_the_facts_conformed_keys_are_the_roles_and_never_the_dimensions_own_key():
    """`dim_date`'s own key is `date_key` and the fact's columns are `event_date_key` and
    `fx_rate_date_key`, because a date dimension is the one kind a fact reaches under names
    saying WHICH date. The other two have no second name to take, so their fact key IS their
    surrogate key -- and the declared order is the order those keys are projected in, which a
    Delta append matches positionally.

    THREE DIMENSIONS AND FOUR KEYS, which is what `fact_keys` exists to make expressible: the
    singular `fact_key` property it replaced could only ever answer one of `dim_date`'s two,
    and it RAISED rather than choosing."""
    assert [fact_keys(item) for item in (DIM_DATE, DIM_CHANNEL, DIM_CURRENCY)] == [
        ("event_date_key", "fx_rate_date_key"), ("channel_key",), ("currency_key",),
    ]
    assert fact_keys(DIM_DATE) == tuple(role.key for role in DIM_DATE.roles)
    assert DIM_DATE.surrogate_key not in fact_keys(DIM_DATE)
    assert FACT_PAYMENT.conformed == ("dim_date", "dim_channel", "dim_currency")
    assert not hasattr(DIM_DATE, "fact_key"), (
        "the singular property is GONE rather than deprecated: left in place it would keep "
        "answering with one of two keys at every site that had not been updated"
    )


def test_the_calendars_span_column_is_the_contract_roles_and_not_the_derived_ones():
    """`fact_column` became a PROPERTY over the contract-sourced role, so there is no second
    spelling of `event_time` to drift. `covered_span` reads it over `bronze_payments`, which
    is why it cannot be `fx_rate_date`: that column exists only in the fact."""
    assert DIM_DATE.fact_column == payments.EVENT_TIME_COLUMN
    assert DIM_DATE.fact_column in payments.COLUMNS
    derived = [role for role in DIM_DATE.roles if role.source == FROM_DERIVED]
    assert [role.fact_column for role in derived] == ["fx_rate_date"]
    assert "fx_rate_date" not in payments.COLUMNS


def test_a_fact_needs_a_name_a_grain_a_measure_a_dimension_and_a_conformed_set():
    for blank in (
        {"name": " "}, {"grain_key": ""}, {"measure": None},
        {"company_dimension": ""}, {"conformed": ()},
    ):
        with pytest.raises(ValueError):
            _fact(**blank)


def test_a_dimension_needs_a_name_a_surrogate_key_and_a_source():
    for blank in ({"name": " "}, {"surrogate_key": ""}, {"source_satellite": None}):
        with pytest.raises(ValueError):
            _dimension(**blank)



# --- T4a: the measure that stopped being summable ------------------------------------


def test_the_delivered_measure_is_no_longer_the_additive_one_and_the_derived_one_is():
    """THE DECLARATION F-API T4a EXISTS TO CHANGE, pinned as the three columns it produces.

    `amount` is what the payment DELIVERED, denominated in whatever currency it carried, and
    with two members in the domain `SUM(amount)` is a number with no unit. So it is declared
    ADDITIVE ONLY WITHIN A CURRENCY, `fx_rate` is NON-ADDITIVE (a ratio, whose sum is nonsense
    and whose unweighted mean answers a question nobody asked), and `amount_brl` -- computed
    here, in one currency -- is the single measure a reader sums without being told which
    column to sum."""
    assert FACT_PAYMENT.measure == "amount"
    assert FACT_PAYMENT.measure_additivity == ADDITIVE_WITHIN_CURRENCY
    assert [(item.name, item.additivity) for item in FACT_PAYMENT.derived] == [
        ("fx_rate", NON_ADDITIVE),
        ("amount_brl", ADDITIVE),
    ]
    assert FACT_PAYMENT.additive_measure == "amount_brl"
    assert FACT_PAYMENT.derived_names == ("fx_rate", "amount_brl")


def test_a_delivered_measure_declared_additive_is_refused_while_the_domain_is_mixed():
    """THE GUARD THAT FIRES ON A DECLARATION A *VALUE CHANGE* MADE WRONG, which is the whole
    of T4a. `amount` was this fact's only measure and summing it meant something while
    `payments.CURRENCIES` held one member; the moment the domain gained USD, `SUM(amount)`
    became a mixed-currency number with no unit -- and nothing anywhere failed, because the
    DECLARATION had not changed.

    IT READS THE DOMAIN AND NOT THE DATA, and that is what makes it a refusal at import
    rather than a measurement after a load: the fact-side currency mix is a property of which
    streams have landed, and the domain is a property of the contract. The assertion is
    load-bearing only while the domain has more than one member, which the first line states
    rather than assumes -- with a one-member domain the same declaration is legitimate and
    this guard is silent, which is exactly the state the phase left behind."""
    assert len(payments.CURRENCIES) > 1
    with pytest.raises(ValueError, match="denominated in more than one currency"):
        _fact(measure_additivity=ADDITIVE, derived=())


def test_a_fact_needs_exactly_one_additive_measure_and_never_two():
    """The measure is what a reader sums WITHOUT being told which column to sum. With none the
    star has no answer to that question; with two the answer is whichever column a report
    picked, and those two disagree by the rate."""
    with pytest.raises(ValueError, match="declares 0 additive measures"):
        _fact(derived=())
    with pytest.raises(ValueError, match="declares 2 additive measures"):
        _fact(
            derived=(
                DerivedMeasure(name="probe_a", inputs=("amount",), additivity=ADDITIVE),
                DerivedMeasure(name="probe_b", inputs=("amount",), additivity=ADDITIVE),
            )
        )


def test_a_derived_measure_reading_an_input_nothing_produces_is_refused():
    """DECLARATION ORDER IS COMPUTATION ORDER, and this is what checks it. `amount_brl`'s
    `fx_rate` input resolves ONLY because `fx_rate` is declared above it -- swapping the two
    turns the import of every gold module red instead of producing a NULL column that lowers
    every total by an amount nobody can name.

    IT IS NOT A TOPOLOGICAL SORT, deliberately: a projection is a list of columns in one
    `select`, so the order a reader sees IS the order the frame builds them in."""
    ordered = (
        DerivedMeasure(name="probe_rate", inputs=("currency",), additivity=NON_ADDITIVE),
        DerivedMeasure(name="probe_brl", inputs=("amount", "probe_rate"), additivity=ADDITIVE),
    )
    assert _fact(derived=ordered).derived_names == ("probe_rate", "probe_brl")
    with pytest.raises(ValueError, match="computes .* from"):
        _fact(derived=tuple(reversed(ordered)))
    with pytest.raises(ValueError, match="computes .* from"):
        _fact(
            derived=(
                DerivedMeasure(name="probe_brl", inputs=("amont",), additivity=ADDITIVE),
            )
        )


def test_a_derived_measure_named_like_a_column_the_payment_delivers_is_refused():
    """One projection writing the delivered value and the computed one into one column: one
    survives, every row is still present, and every total is plausible. Both directions of
    the collision are refused -- against the contract's columns and against a role key."""
    with pytest.raises(ValueError, match="column the payment contract delivers"):
        DerivedMeasure(name="amount", inputs=("currency",), additivity=ADDITIVE)
    with pytest.raises(ValueError, match="DERIVES a measure of that name"):
        _fact(
            roles=(
                ("payer_cnpj_basico", "probe_amount_brl"),
                ("payee_cnpj_basico", "probe_payee_sk"),
            )
        )


def test_a_derived_measure_declaring_no_input_or_itself_is_refused():
    """EMPTY is the one shape the computability guard cannot check -- there is no input for it
    to resolve, so the declaration passes and asserts nothing. SELF-REFERENTIAL is the same
    defect one step along: it would resolve against an earlier entry of the same name rather
    than failing, so a name declared twice would make the measure a definition of itself."""
    with pytest.raises(ValueError, match="declares no input"):
        DerivedMeasure(name="probe_brl", inputs=(), additivity=ADDITIVE)
    with pytest.raises(ValueError, match="declares itself as one of its own inputs"):
        DerivedMeasure(name="probe_brl", inputs=("probe_brl",), additivity=ADDITIVE)
    with pytest.raises(ValueError, match="twice as an input"):
        DerivedMeasure(name="probe_brl", inputs=("amount", "amount"), additivity=ADDITIVE)


def test_an_additivity_token_outside_its_closed_set_is_refused():
    """Every consumer BRANCHES on these values, so an unknown one silently takes a fallback
    branch rather than failing: a misspelled `reads` would read a member out of something that
    is not one, and a misspelled `additivity` would leave the additive count at zero -- which
    the fact's own guard then reports as a star with no summable measure, a true statement
    about a typo and the wrong diagnosis."""
    with pytest.raises(ValueError, match="which is not one of"):
        DerivedMeasure(name="probe_brl", inputs=("amount",), additivity="aditive")
    with pytest.raises(ValueError, match="which is not one of"):
        _fact(measure_additivity="aditive")


def test_the_four_conformed_fact_keys_are_distinct_across_the_whole_projection():
    """`dim_date` answers two of the fact's columns since T4b, so the collision guard has to
    see FOUR keys where it used to see three. Both date keys are integer `yyyyMMdd` values, so
    a second role spelled like the first would overwrite it and nothing else in this
    repository would notice."""
    keys = [key for name in FACT_PAYMENT.conformed for key in fact_keys(REGISTRY[name])]
    assert keys == ["event_date_key", "fx_rate_date_key", "channel_key", "currency_key"]
    projected = [
        *FACT_PAYMENT.role_keys, *keys, FACT_PAYMENT.grain_key, FACT_PAYMENT.measure,
        *FACT_PAYMENT.derived_names, payments.EVENT_TIME_COLUMN,
    ]
    assert len(set(projected)) == len(projected) == 11
