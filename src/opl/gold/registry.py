# src/opl/gold/registry.py
"""The gold tables this star holds, the kinds they may be, and the guards they pass at
import. `opl.bronze.registry`'s shape, and deliberately NOT `opl.vault.registry`'s.

WHY THE TABLE LIST IS INLINE HERE AND NOT DISCOVERED FROM A `domains/` PACKAGE. The
vault's per-domain registry exists to satisfy one specific claim: wave 2 adds
`hub_account`, `hub_customer` and `link_payment` with a git diff of "+1 file, 0
modified", and a registry carrying the table list would be the file that breaks it. Gold
stakes no such claim, and Kimball's model actively refuses the decomposition -- a
CONFORMED dimension is one `dim_company` shared by every fact, so "which domain owns
it" has no answer. What gold does have is bronze's problem: a small, closed list of
tables whose names collide with things, which is why this file is shaped like
`opl.bronze.registry` -- declared tables, guards at the foot, refusal at import.

WHERE THE NEXT TABLE GOES, so the decision is not re-made. The kinds a gold table may be
live in `opl.gold.specs` and the tables themselves are declared at the foot of this file.
This module carried `Scd2Dimension` until F3 Task 3, whose two new kinds and three new
tables took it to 728 lines -- the point its own docstring pre-decided the move at, and
one kind short of the fact Task 4 adds. `opl.gold.specs` argues the seam; the short
version is that a check answerable about ONE table belongs beside its dataclass and a
check that needs the other tables, or the other layers, belongs in `build_registry`.

AND THE HEADROOM IS NOW SIXTEEN LINES, WHICH IS THE NEXT TASK'S PROBLEM AND IS SAID HERE
SO IT IS NOT DISCOVERED. F3 Task 4's fact added two whole-set guards and one declaration
and took this file to 784 against the project's 800-line cap (master protocol section
4.12: whoever touches a file at the cap splits it FIRST). The split that is already argued
is the one the kinds took: the whole-set guards are an `opl.gold.registry_guards` waiting
to happen, in `opl.bronze.registry_collisions`' shape, leaving the declarations and
`build_registry`'s call order here. It is not made now because a split of eight guards to
gain sixteen lines would be a diff nobody can review beside a task that also builds a
fact -- and the next table this file gains cannot say that.

WHY THE THREE CONFORMED TABLES ARE TWO KINDS AND NOT THREE. `dim_channel` and
`dim_currency` are a value domain the payment contract already declares, written out one
row per member: one kind, differing only in which tuple they name. `dim_date` is not --
its members are a contiguous range of calendar days whose span is DERIVED at build time
from the dates the star must cover, because fifty date literals here would be a copy of a
calendar that goes stale the day a snapshot lands.

THE GUARDS RUN WHERE THE MISTAKE IS, in the house style both other registries use:
everything checkable about ONE table in isolation is refused in its `__post_init__`,
before pyspark and before any registry exists; everything that needs to see the other
tables -- or the OTHER LAYERS -- is refused in `build_registry`, which this module calls
in its own foot, so a malformed registry breaks the import of every module that reads it
rather than the one job that touches that table. A CI test protects a merge; it does not
protect the ad-hoc run of a branch whose tests have not been run, which is exactly how
these jobs get launched while a phase is in flight.

THE CROSS-LAYER GUARD IS THIS FILE'S OWN, AND NOTHING ELSE IN THE REPOSITORY CAN HOLD
IT. Databricks Free Edition ships one catalog and one schema, so `opl.config.OplConfig
.table` puts bronze's fifteen Delta tables, the vault's fourteen and gold's into ONE
namespace. Gold is the first artefact that can collide across a layer boundary, and the
collision is silent in the worst available way: every loader in this repository writes
with `mode("append")`, which does not refuse a name another layer owns -- it appends
rows of one shape into a table of another, or, where the shapes agree by accident,
merges two populations with both runs reporting success. `opl.bronze.registry` cannot
see it (it does not import the vault) and `opl.vault.registry` cannot see it (it does
not import bronze, and deliberately: bronze's registry must import where pyspark is
not installed). This module imports both, which it can afford to because gold has no
life outside Spark, and it is therefore the only place the question can be asked. THE
GUARD ITSELF now lives in `opl.gold.registry_guards`; this module is still the one that
imports both layers and calls it, so the sentence above is unchanged in substance.

AND THE SPLIT THE PARAGRAPH ABOVE PRE-DECLARED HAS HAPPENED. F-API Task 4 is "the next
table this file gains" -- it widens the fact's columns and gives `dim_date` a second role --
so the nine whole-set guards moved wholesale to `opl.gold.registry_guards` before anything
was added, which is master protocol section 4.12's rule rather than a preference. What is
left here is what this file was always for: the DECLARATIONS, the ORDER `build_registry`
runs the guards in, and `table_spec`."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from types import MappingProxyType

from opl.contracts import payments
from opl.gold.registry_guards import (
    _assert_every_dimension_reads_a_registered_satellite,
    _assert_every_fact_reaches_every_dimension_this_star_holds,
    _assert_every_pit_resolves_its_hub_and_its_satellites,
    _assert_no_gold_name_is_owned_by_another_layer,
    _assert_no_source_column_collides_with_a_column_the_loader_writes,
    _assert_no_surrogate_key_collides_with_its_source,
    _assert_no_two_columns_of_one_fact_share_a_name,
    _assert_no_two_dimensions_draw_from_one_payment_column,
    _assert_no_two_gold_tables_share_a_name,
)
from opl.gold.spec_fields import (
    ADDITIVE,
    ADDITIVE_WITHIN_CURRENCY,
    FROM_CONTRACT,
    FROM_DERIVED,
    NON_ADDITIVE,
    READS_DATE,
    READS_ISO_TEXT,
    FactRole,
)
from opl.gold.specs import (
    CalendarDimension,
    ConformedDimension,
    DerivedMeasure,
    EnumeratedDimension,
    GoldTable,
    PaymentFact,
    PointInTimeTable,
    Scd2Dimension,
)
from opl.vault import domains
from opl.vault.registry import VaultTable

__all__ = [
    "DIM_CHANNEL",
    "DIM_COMPANY",
    "DIM_CURRENCY",
    "DIM_DATE",
    "FACT_PAYMENT",
    "PIT_ESTABELECIMENTO",
    "REGISTRY",
    "TABLES",
    "CalendarDimension",
    "ConformedDimension",
    "DerivedMeasure",
    "EnumeratedDimension",
    "FactRole",
    "GoldTable",
    "PaymentFact",
    "PointInTimeTable",
    "Scd2Dimension",
    "UnknownGoldTable",
    "build_registry",
    "table_spec",
]


class UnknownGoldTable(ValueError):
    """A gold table name that is not registered.

    A `ValueError` and not a `KeyError`, for the two reasons
    `opl.bronze.registry.UnknownTable` and `opl.vault.registry.UnknownVaultTable` both
    record: `KeyError.__str__` re-`repr`s its argument, so prose written for an
    operator's run log arrives quoted and escaped; and an `except KeyError` several
    frames up in a job entry point would swallow a mistyped table name and replace this
    message with a generic one."""


def build_registry(
    tables: Iterable[GoldTable],
    *,
    vault_tables: Mapping[str, VaultTable] | None = None,
) -> Mapping[str, GoldTable]:
    """Every registered gold table by name, or refuse -- the whole-set guards.

    `vault_tables` DEFAULTS TO THE REAL VAULT REGISTRY and is an argument at all so a
    test can drive each refusal against a throwaway spec, which is the property
    `opl.vault.registry.build_registry` has for the same reason. Bronze's names are read
    from `opl.bronze.registry` directly: nothing about them is worth substituting, and
    the guard's whole subject is the LIVE namespace.

    Returns a read-only mapping: the registry is data, and a caller who could
    `registry[...] = ...` could add a table that never passed a guard.

    THE GUARDS LIVE IN `opl.gold.registry_guards` AND ARE CALLED HERE, in this one ordered
    block, for `opl.bronze.registry`'s reason: the ORDER is load-bearing -- individually
    wrong before collectively wrong, and the fact's conformed set resolved before its column
    names are looked up -- and it has to be reviewable in one place rather than distributed
    across the file the guards live in."""
    collected = tuple(tables)
    known = domains.REGISTRY if vault_tables is None else vault_tables
    _assert_no_gold_name_is_owned_by_another_layer(collected, known)
    by_name = _assert_no_two_gold_tables_share_a_name(collected)
    _assert_no_two_dimensions_draw_from_one_payment_column(collected)
    _assert_every_fact_reaches_every_dimension_this_star_holds(collected, by_name)
    _assert_no_two_columns_of_one_fact_share_a_name(collected, by_name)
    _assert_every_dimension_reads_a_registered_satellite(collected, known)
    _assert_every_pit_resolves_its_hub_and_its_satellites(collected, known)
    _assert_no_surrogate_key_collides_with_its_source(collected, known)
    _assert_no_source_column_collides_with_a_column_the_loader_writes(collected, known)
    return MappingProxyType(by_name)


def table_spec(name: str) -> GoldTable:
    """The registered spec for `name`, or refuse naming the alternatives.

    Refuses BEFORE Spark, like both sibling registries: an operator who mistyped a table
    should not wait for a serverless session to be told so."""
    try:
        return REGISTRY[name]
    except KeyError:
        raise UnknownGoldTable(
            f"unknown gold table {name!r} -- registered tables are: "
            f"{', '.join(sorted(REGISTRY))}. Every gold job task takes the table name "
            "as its first parameter; check the `table` parameter of the task that "
            "failed rather than assuming the registry is missing an entry"
        ) from None


# --------------------------------------------------------------------------- #
# The star. F3 Task 1 built the dimension the fact must reach; Task 3 adds the three
# conformed dimensions, and Task 4 builds the fact that references all four.
# --------------------------------------------------------------------------- #

# `dim_company` AT EMPRESA GRAIN AND NOT `dim_merchant` AT ESTABELECIMENTO GRAIN, which
# is the phase plan's T1 ruling and is worth restating where the table is declared. The
# master spec asks for an SCD2 dimension at estabelecimento grain (14-digit CNPJ)
# inheriting company attributes through the link; F1b's payment contract carries
# `payer_cnpj_basico` and `payee_cnpj_basico`, which are EIGHT characters, and all 1,024
# generated counterparties resolve to `hub_empresa`. A dimension at estabelecimento
# grain would be a dimension the fact cannot join to -- decorative, in a star schema.
# `dim_merchant` becomes reachable when payments carry a 14-digit CNPJ, which is a
# change to the GENERATOR's contract and not to this layer.
#
# THE SURROGATE KEY IS `company_sk` AND THE NATURAL KEY IS `cnpj_basico`, WHICH THE
# SATELLITE DOES NOT CARRY. `sat_empresa_dados` holds `hub_empresa_hk` and the payload;
# the business key lives in `hub_empresa`. So this dimension is a JOIN and not a
# projection, and that join is the cost of DV2's own decomposition rather than a choice
# made here -- see `opl.gold.dimensions`, which pays it once.
DIM_COMPANY = Scd2Dimension(
    name="dim_company",
    surrogate_key="company_sk",
    source_satellite="sat_empresa_dados",
)

# `dim_date`, AND ITS FACT-SIDE CARDINALITY IS 3. `docs/f3-run-evidence.md` §0.5 (P1-P3)
# measured the two August streams' 20,000 payments all on 2026-08-01; `between-snapshots`
# adds 2026-06-20 and F-API's `cross-currency` adds 2026-06-22, whose whole window sits
# inside one calendar day in BOTH UTC and BRT -- so the count is 3 and not 4, against a span
# of fifty days. (This comment said "1 today and TWO once that profile lands" until F-API
# Task 4; a fact-side count written into a comment is what goes stale when a stream lands,
# which is why `opl.gold.conformed` MEASURES `fact_side_cardinality` on every load rather
# than letting the evidence say "thin".)
#
# TWO ROLES, AND THE SECOND ONE IS NOT THE ONE §4.3 ASKED FOR. The governing spec asks for
# role-playing across transação / autorização / liquidação. `opl.contracts.payments` carries
# `event_time` and `emitted_at`; `emitted_at` is when the GENERATOR RELEASED the record,
# which is a property of the delivery -- the thing that may legitimately repeat -- and not a
# date the business transacted on. Counting it would buy a bigger number and a `dim_date`
# join that means nothing, so it is still refused. What DID arrive is `fx_rate_date_key`:
# the quote date whose rate a payment converted at, which is a real second date the star can
# group by and which no payment contract column carries. `opl.gold.spec_fields` argues why
# that made the role a four-field declaration instead of a string.
DIM_DATE = CalendarDimension(
    name="dim_date",
    surrogate_key="date_key",
    natural_key="full_date",
    # WHERE THE SPAN'S OTHER END COMES FROM. `dim_date` must contain both RFB
    # `applied_date`s -- 2026-06-13 and 2026-07-11 -- because `dim_company`'s version
    # boundaries sit on them, and a calendar that cannot name the day a version opened is
    # a calendar the star's own history falls outside of. They are read from this
    # satellite's `applied_date` column at build time rather than declared: a date
    # literal here would be a second spelling of a value the vault owns.
    applied_date_source="sat_empresa_dados",
    roles=(
        # THE CONTRACT-SOURCED ROLE, AND THERE MAY BE EXACTLY ONE. It is the column
        # `covered_span` measures this calendar's span from, over `bronze_payments` --
        # which is why the derived role below cannot be it: `fx_rate_date` does not exist
        # in the fact SOURCE at all, only in the fact.
        FactRole(
            key="event_date_key",
            fact_column=payments.EVENT_TIME_COLUMN,
            source=FROM_CONTRACT,
            reads=READS_ISO_TEXT,
        ),
        # THE DERIVED ROLE. `opl.gold.fx` resolves it per payment from the PTAX series and
        # the payment's own instant, so it is `FROM_DERIVED` and the guard refuses it being
        # a contract column -- the mirror of the refusal above. `READS_DATE` because the FX
        # join produces a real `date` where bronze hands the contract ISO TEXT; both roles
        # then go through one key mechanism, so the fact and the dimension cannot drift.
        #
        # ITS ORPHANS ARE REPORTED AND NOT REFUSED, which is the price of a derived role and
        # is paid deliberately. Every quote date this phase can resolve to -- 2026-06-19,
        # 2026-06-22, 2026-07-31 -- is inside the 2026-06-13 .. 2026-08-01 span, so the
        # count is predicted 0; a fallback reaching below 2026-06-13 would be a number in
        # the run log rather than a silent join to nothing.
        FactRole(
            key="fx_rate_date_key",
            fact_column="fx_rate_date",
            source=FROM_DERIVED,
            reads=READS_DATE,
        ),
    ),
)

# `dim_channel` FROM `payment_method`, AND NEVER FROM `payment_channel`. The drift column
# is undeclared by design and `_assert_the_fact_column_is_one_the_contract_carries`
# refuses it at import; `opl.contracts.payments`' own `DRIFT_COLUMN` block records that
# the name was chosen to be TEMPTING for exactly a dimension like this one.
#
# FIVE MEMBERS AND A FACT-SIDE CARDINALITY OF FIVE. `opl.generator.stream` picks a method
# by index into `PAYMENT_METHODS` for every event, so all five rails appear in 40,150
# payments with overwhelming probability, and the measurement is what says so. (It read
# 20,150 until F-API landed the fourth and fifth streams, and this dimension is no longer
# "the one of these three that is not a constant column": `dim_currency` below reached
# fact-side cardinality 2 in the same phase.)
DIM_CHANNEL = EnumeratedDimension(
    name="dim_channel",
    surrogate_key="channel_key",
    natural_key="channel_code",
    # SPELLED OUT because `opl.contracts.payments` names this column only as a member of
    # `BUSINESS_ATTRIBUTE_COLUMNS`, not as a constant. The guard above refuses any name
    # v1 does not carry, so this copy cannot drift behind a rename -- it turns the import
    # of every gold module red instead.
    fact_column="payment_method",
    members=payments.PAYMENT_METHODS,
)

# `dim_currency`, AND IT HAS TWO MEMBERS AND A FACT-SIDE CARDINALITY OF TWO. It read "AND
# IT HAS EXACTLY ONE MEMBER. `CURRENCIES = ("BRL",)` ... A dimension of one member cannot be
# wrong, and no test over it can fail" until F-API T1 split the declared DOMAIN from the
# tuple each stream DRAWS from: `payments.CURRENCIES` is `("BRL", "USD")` now, this
# declaration is the only way the dimension gains a member, and the `cross-currency` profile
# put 4,905 USD rows against 35,095 BRL ones in the fact. The two numbers the phase publishes
# -- 2 members, fact-side cardinality 2 -- are what retires the sentence above.
#
# THE CONTRACT'S PREDICTION WAS THE RIGHT ONE, WHICH IS WORTH RECORDING WHERE IT CAME TRUE.
# `opl.contracts.payments` says the column exists "so that a second currency is a value
# change instead of a schema change". It was: the domain gained a member, this line reads the
# domain, and no dimension was added. What DID change is the fact -- `fx_rate`,
# `fx_rate_date_key` and `amount_brl` -- because a currency mix without a conversion is a
# `SUM` with no unit (F-API T4a), and that half was not free.
DIM_CURRENCY = EnumeratedDimension(
    name="dim_currency",
    surrogate_key="currency_key",
    natural_key="currency_code",
    fact_column="currency",
    members=payments.CURRENCIES,
)

# `pit_estabelecimento`, AND IT IS THE ONLY TABLE IN THIS REGISTRY NOTHING IN THE STAR
# REACHES. `dim_company` is at empresa grain, `dim_merchant` at estabelecimento grain is
# deferred until payments carry a 14-digit CNPJ, and `dim_geography` is skipped -- so no
# fact and no dimension joins to this table. That is the same "decorative in a star
# schema" charge the phase plan levels at `dim_merchant`, and it is recorded here rather
# than left for a reader to notice: `opl.gold.pit`'s docstring states it at length and
# names the ONE change that would pull it in.
#
# IT IS BUILT ANYWAY, ON A MEASUREMENT AND NOT ON COMPLETENESS. This is the only place in
# this vault where two satellites with different change rates hang off one hub, and Task 0
# measured the timeline collapse they cause: the naive `(hash key, applied_date)` join at
# 2026-07-11 returns 514,504 keys where the as-of answer is 72,318,968
# (`docs/f3-run-evidence.md` §0.5). 1,141,850 establishments would be handed a NULL
# address that is sitting in the June row, still in force, and 499,630 the other way
# round. A mechanism with a 71,804,464-row consequence is worth demonstrating even where
# the demonstration sits outside the star.
#
# THE TWO SATELLITES ARE DECLARED IN THE VAULT'S OWN ORDER (`_dados` then `_endereco`),
# which is the order the pointer columns are projected in and therefore the order a Delta
# append matches POSITIONALLY. Permuting them re-writes each pointer into the other's
# column, and every read still returns two dates.
PIT_ESTABELECIMENTO = PointInTimeTable(
    name="pit_estabelecimento",
    hub="hub_estabelecimento",
    satellites=("sat_estabelecimento_dados", "sat_estabelecimento_endereco"),
    # `as_of_date` AND NOT `applied_date`, which the spec refuses -- the pointers ARE
    # named `<satellite>_applied_date`, so a bare `applied_date` here would invite
    # `pit.applied_date = sat.applied_date`, the naive equi-join this table replaces.
    as_of_column="as_of_date",
)

# `fact_payment`, AND IT IS WHY EVERY TABLE ABOVE EXISTS. One row per payment EVENT --
# `opl.contracts.payments`' own grain sentence, unchanged by this layer -- with the
# processor's `transaction_id` carried as a DEGENERATE dimension (a key with no dimension
# table, because there is nothing to say about a transaction id that the fact row does not
# already say) and five foreign keys.
#
# TWO OF THOSE FIVE ARE THE SAME DIMENSION PLAYED TWICE, WHICH THE PHASE PLAN NEVER SAID.
# `opl.contracts.payments.COUNTERPARTY_COLUMNS` is two columns, and conformance means ONE
# `dim_company` answers for both -- so the fact carries `payer_company_sk` AND
# `payee_company_sk`, both resolved AS OF the payment's own `event_time`, and the plan's
# closing test ("every row resolves to exactly one `dim_company` version") is ill-formed:
# a correct row resolves to TWO. `opl.gold.fact_spec._assert_every_counterparty_plays_exactly
# _one_role` refuses the reading that is satisfiable, which is a fact that joins on the
# payer alone. (It cited `opl.gold.specs`, where the fact kind lived until F-API T4a split it
# out; that module re-exports `PaymentFact` and not its guards, so the old name resolved to
# nothing.)
#
# THE KEY HALF OF EACH PAIR IS A NAME THIS LAYER INVENTS AND IS THEREFORE DECLARED; the
# counterparty half is the CONTRACT's and is refused at import if it is not. Deriving
# `payer_company_sk` from `payer_cnpj_basico` by string surgery would be one rename away
# from a column nothing joins to, with the load reporting success.
#
# `amount` IS SPELLED OUT for `dim_channel.fact_column`'s reason: the payment contract
# names it only as a member of `BUSINESS_ATTRIBUTE_COLUMNS`, not as a constant, and the
# guard that refuses a measure v1 does not carry is what stops this copy drifting behind a
# rename -- it turns the import of every gold module red instead.
#
# AND IT IS NO LONGER THE SUMMABLE ONE (F-API T4a). `amount` is what the payment DELIVERED,
# in the currency it was denominated in, and with two currencies in the domain `SUM(amount)`
# is a number with no unit. So it is declared ADDITIVE ONLY WITHIN A CURRENCY and
# `amount_brl` -- computed here, in one currency -- is the one measure a reader sums without
# being told which column to sum. `_assert_exactly_one_measure_is_additive` reads
# `payments.CURRENCIES` rather than the data, so declaring `amount` additive again is a
# refusal at import and not a wrong number in a report.
FACT_PAYMENT = PaymentFact(
    name="fact_payment",
    grain_key=payments.IDENTITY_COLUMN,
    measure="amount",
    measure_additivity=ADDITIVE_WITHIN_CURRENCY,
    company_dimension="dim_company",
    roles=(
        ("payer_cnpj_basico", "payer_company_sk"),
        ("payee_cnpj_basico", "payee_company_sk"),
    ),
    # IN PROJECTION ORDER *AND* IN COMPUTATION ORDER, which for these two is the same tuple
    # read twice: a Delta append matches POSITIONALLY, and
    # `_assert_the_derived_measures_are_computable` resolves each measure's inputs against
    # the ones declared above it. Swapping these two is refused at import rather than
    # producing a NULL column.
    derived=(
        # `fx_rate` IS NOT `AMOUNT_TYPE`, AND THAT IS THE WHOLE OF ITS TYPE ARGUMENT.
        # `decimal(18, 2)` would round 5.14420 to 5.14 and put `amount_brl` about 0.08%
        # wrong on every USD row -- plausibly, in a column nobody would re-derive. It
        # carries the series' own five digits (`opl.gold.fx.FX_RATE_TYPE`).
        #
        # NON-ADDITIVE, AND THE MEAN IS WRONG TOO. A ratio's sum is nonsense, and an
        # UNWEIGHTED mean over fact rows answers a question nobody asked: the rate a
        # portfolio converted at is `SUM(amount_brl) / SUM(amount)` over one currency, not
        # `AVG(fx_rate)`. It is a legitimate fact column as a DENORMALISATION -- the rate
        # this row actually used, so a reader can re-derive the conversion without joining
        # the series back.
        #
        # ITS INPUTS ARE THE TWO PAYMENT COLUMNS THAT DECIDE WHICH QUOTE APPLIES, not the
        # series' own: `currency` says whether a quote is consulted at all (BRL is 1.0 by
        # definition) and `event_time` says which quote had been published by then.
        DerivedMeasure(
            name="fx_rate",
            inputs=("currency", payments.EVENT_TIME_COLUMN),
            additivity=NON_ADDITIVE,
        ),
        # THE ONE ADDITIVE MEASURE. `amount * fx_rate`, rounded HALF-UP to the contract's
        # own `AMOUNT_SCALE` at the ROW -- so `SUM(amount_brl)` is not `SUM(amount) * rate`
        # to the cent, and `docs/f-api-run-evidence.md` says so in those words rather than
        # leaving a reader to file it as a defect.
        DerivedMeasure(
            name="amount_brl",
            inputs=("amount", "fx_rate"),
            additivity=ADDITIVE,
        ),
    ),
    # ALL THREE, AND THE REGISTRY REFUSES ANYTHING LESS. `_assert_every_fact_reaches_every
    # _dimension_this_star_holds` states it as an EQUALITY against the registry's conformed
    # set, so a conformed dimension added later without a key here turns the import red
    # rather than quietly becoming a table nothing joins to.
    #
    # THREE NAMES AND FOUR KEYS SINCE F-API T4b. `dim_date` answers two of the fact's
    # columns -- `event_date_key` and `fx_rate_date_key` -- so this is a list of DIMENSIONS
    # and `opl.gold.specs.fact_keys` is what turns one into its columns. Permuting the three
    # re-writes each key into another's column, and every one of them is an integer, so
    # nothing fails.
    conformed=("dim_date", "dim_channel", "dim_currency"),
)

TABLES: tuple[GoldTable, ...] = (
    DIM_COMPANY, DIM_DATE, DIM_CHANNEL, DIM_CURRENCY, PIT_ESTABELECIMENTO, FACT_PAYMENT,
)

# AT IMPORT, in this module's own foot, for the reason both sibling registries state:
# a malformed registry must break the import of every module that reads it rather than
# the one job that touches the table it is malformed about.
REGISTRY: Mapping[str, GoldTable] = build_registry(TABLES)
