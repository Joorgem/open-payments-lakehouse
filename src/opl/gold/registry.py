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
life outside Spark, and it is therefore the only place the question can be asked."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from types import MappingProxyType

from opl.bronze.registry import REGISTRY as BRONZE_REGISTRY
from opl.contracts import payments
from opl.gold.columns import DIMENSION_COLUMNS
from opl.gold.specs import (
    CalendarDimension,
    ConformedDimension,
    EnumeratedDimension,
    GoldTable,
    Scd2Dimension,
)
from opl.vault import domains
from opl.vault.registry import Satellite, VaultTable

__all__ = [
    "DIM_CHANNEL",
    "DIM_COMPANY",
    "DIM_CURRENCY",
    "DIM_DATE",
    "REGISTRY",
    "TABLES",
    "CalendarDimension",
    "ConformedDimension",
    "EnumeratedDimension",
    "GoldTable",
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




def _same_table_note(name: str, other: str) -> str:
    """Why two names that are not equal are nonetheless one Delta table -- and NOTHING
    when they are equal.

    `opl.bronze.registry_collisions._delta_name_collision`'s shape, verbatim in intent:
    the comparison is casefolded, so the two strings a refusal reports may differ, and an
    operator handed only a normalised name would search the source for a string nobody
    wrote. Conditional because the ordinary duplicate is the case that actually happens,
    and annotating it with a case explanation sends the reader looking for a difference
    that is not there."""
    if name == other:
        return ""
    return (
        f" -- spelled {other!r} there, and Unity Catalog and Spark identifiers are "
        "CASE-INSENSITIVE, so two spellings that differ only in case name ONE physical "
        "table"
    )


def _bronze_delta_names() -> Mapping[str, tuple[str, str]]:
    """Every Delta name bronze owns, by its CASEFOLDED spelling, mapped to that name as
    bronze writes it and to what it is -- staging, bronze table or quarantine -- so a
    refusal can say which one was collided with and in whose words.

    ALL THREE AND NOT ONLY THE BRONZE ONE. A promote appends into staging and the DQ
    gate appends into quarantine, so a dimension sitting on either is reached by a job
    nobody would think to look at -- and the quarantine is the documented case, the one
    `opl.bronze.registry`'s own docstring records as having "sent estab triagers to a
    table full of unrelated F1.2 lookup rows"."""
    owners: dict[str, tuple[str, str]] = {}
    for spec in BRONZE_REGISTRY.values():
        for role, name in (
            ("staging table", spec.staging),
            ("bronze table", spec.bronze),
            ("quarantine", spec.quarantine),
        ):
            owners[name.casefold()] = (name, f"{spec.name}'s {role}")
    return owners


def _assert_no_gold_name_is_owned_by_another_layer(
    tables: Iterable[GoldTable], vault_tables: Mapping[str, VaultTable]
) -> None:
    """Refuse a gold table whose name a bronze or vault table already holds.

    THE GUARD THE FLAT SCHEMA FORCES -- see the module docstring for why no other file
    in this repository can hold it. Ordered FIRST among the whole-set guards for
    `opl.bronze.registry`'s "individually wrong before collectively wrong" reason: a
    name another layer owns is wrong on its own, and reporting a duplicate first would
    tell the operator to rename one of two tables that both must be renamed.

    CASEFOLDED, WHICH IS THE WHOLE POINT AND WAS MISSING. UC and Spark resolve
    identifiers case-insensitively, so `SAT_EMPRESA_DADOS` IS the satellite -- and under
    the byte comparison this used until now it was ACCEPTED while `sat_empresa_dados` was
    refused, with every consequence below intact. Measured, on `sat_empresa_dados`,
    `ref_pais` and `bronze_cnpj_empresas` alike. It is exactly the defect
    `opl.bronze.registry_collisions` fixed in F1.4b, and this is the one boundary no
    other file in this repository polices."""
    bronze = _bronze_delta_names()
    vault = {name.casefold(): name for name in vault_tables}
    for table in tables:
        folded = table.name.casefold()
        if folded in vault:
            raise ValueError(
                f"gold table {table.name!r} is a name already owned by the vault"
                f"{_same_table_note(table.name, vault[folded])}. "
                "Free Edition ships ONE catalog and ONE schema, so both would resolve "
                "to the same Delta table -- and every loader in this repository writes "
                "with mode('append'), which does not refuse a name it does not own: it "
                "appends dimension rows into the satellite, or merges two populations "
                "where the shapes happen to agree, with both runs reporting success"
            )
        if folded in bronze:
            owned, role = bronze[folded]
            raise ValueError(
                f"gold table {table.name!r} is a name already owned by bronze "
                f"({role}){_same_table_note(table.name, owned)}. One catalog, one "
                "schema, and every writer in this repository appends -- so the promote "
                "or the DQ gate would append into this dimension, or this loader into "
                "theirs, without failing"
            )


def _assert_no_two_gold_tables_share_a_name(tables: Iterable[GoldTable]) -> dict[str, GoldTable]:
    """Every gold table by name, refusing a name two of them claim -- IN ANY CASE.

    Returns the mapping rather than only checking, so there is no second loop that could
    build a different one -- `opl.vault.registry._collected_tables`' shape. The returned
    mapping is keyed by the DECLARED spelling, because that is what a job parameter names
    and what `table_spec` must answer to; only the collision check is casefolded, which
    is the same split `opl.bronze.registry_collisions` makes.

    CASEFOLDED for the guard above's reason, and needed here too: two gold specs called
    `dim_x` and `DIM_X` are one Delta table, so one of them loads into the other with
    both runs reporting success -- and this file is where a second kind gets declared."""
    collected: dict[str, GoldTable] = {}
    seen: dict[str, str] = {}
    for table in tables:
        folded = table.name.casefold()
        if folded in seen:
            raise ValueError(
                f"two specs both declare a gold table called {table.name!r}"
                f"{_same_table_note(table.name, seen[folded])}. One of "
                "them would load into the other's Delta table, with both runs "
                "reporting success"
            )
        seen[folded] = table.name
        collected[table.name] = table
    return collected


def _satellite_readers(tables: Iterable[GoldTable]) -> Iterable[tuple[GoldTable, str]]:
    """Every gold table that names a vault satellite, with the name it declares.

    TWO FIELDS, ONE ITERATION, and the two fields are deliberately not one name -- see
    the module docstring. `Scd2Dimension` reads a satellite's VERSIONS; a
    `CalendarDimension` reads one date column of one and none of its payload. Both must
    resolve against the vault registry, so the resolution is shared; nothing else about
    them is."""
    for table in tables:
        if isinstance(table, Scd2Dimension):
            yield table, table.source_satellite
        elif isinstance(table, CalendarDimension):
            yield table, table.applied_date_source


def _scd2(tables: Iterable[GoldTable]) -> Iterable[Scd2Dimension]:
    """The SCD2 dimensions among `tables`, for the guards whose subject is a satellite's
    PAYLOAD -- which no other kind projects, so no other kind can collide with it."""
    return (table for table in tables if isinstance(table, Scd2Dimension))


def _source_satellite(
    table: GoldTable, declared: str, vault_tables: Mapping[str, VaultTable]
) -> Satellite:
    """`table`'s source, resolved against the vault registry, or refuse naming it.

    ONE RESOLUTION, SHARED BY THE GUARD AND BY THE TWO CHECKS THAT FOLLOW IT, for
    `opl.vault.registry._link_hubs`' reason: a resolver that repeated the guard's
    conditions in a weaker form is how a registry that passed its guards still returns
    something wrong."""
    source = vault_tables.get(declared)
    if source is None:
        raise ValueError(
            f"gold dimension {table.name!r} derives from {declared!r}, "
            f"which no vault domain registers. Registered: "
            f"{', '.join(sorted(vault_tables))}"
        )
    if not isinstance(source, Satellite):
        raise ValueError(
            f"gold dimension {table.name!r} derives from {declared!r}, "
            "which is not a satellite. An SCD2 dimension is a satellite's version chain "
            "with a surrogate key on it: the loader reads `payload_columns` and resolves "
            "a parent hub, so any other kind fails inside Spark's analysis naming a "
            "dataclass field rather than a table"
        )
    return source


def _assert_every_dimension_reads_a_registered_satellite(
    tables: Iterable[GoldTable], vault_tables: Mapping[str, VaultTable]
) -> None:
    """Refuse a dimension whose source is missing or is not a satellite."""
    for table, declared in _satellite_readers(tables):
        _source_satellite(table, declared, vault_tables)


def _assert_no_two_dimensions_draw_from_one_payment_column(
    tables: Iterable[GoldTable],
) -> None:
    """Refuse two conformed dimensions drawing from one payment-contract column.

    CONFORMANCE, MADE CHECKABLE. "Conformed" means one dimension answers one question
    for every fact that asks it. Two dimensions over `payment_method` are two answers:
    the fact would carry two foreign keys resolving to the same five members, and
    nothing about it fails -- both build, both are well-formed, and a report joining one
    agrees with a report joining the other right up until their member sets diverge."""
    drawn: dict[str, str] = {}
    for table in tables:
        if isinstance(table, Scd2Dimension):
            continue
        if table.fact_column in drawn:
            raise ValueError(
                f"{drawn[table.fact_column]!r} and {table.name!r} both draw from the "
                f"payment column {table.fact_column!r}. A conformed dimension answers "
                "ONE question for every fact that asks it; two of them are two keys on "
                "one column, agreeing until their member sets do not"
            )
        drawn[table.fact_column] = table.name


def _assert_no_surrogate_key_collides_with_its_source(
    tables: Iterable[GoldTable], vault_tables: Mapping[str, VaultTable]
) -> None:
    """Refuse a surrogate key that is already a column the source delivers.

    A WHOLE-SET GUARD AND NOT A `__post_init__` CHECK, because it cannot be answered
    about one table in isolation: `razao_social` is a perfectly good surrogate-key name
    until you know which satellite this dimension reads. The parent hub's business key
    is checked with the payload for the same reason -- `cnpj_basico` is written into the
    dimension from the hub, so a surrogate key of that name loses one of the two.

    SCD2 ONLY, and that is a statement about what a payload can collide with rather than
    an exemption: a conformed dimension projects no column of any satellite, so there is
    nothing of its source's for its keys to overwrite."""
    for table in _scd2(tables):
        source = _source_satellite(table, table.source_satellite, vault_tables)
        delivered = {
            **{name: "a payload column of" for name in source.payload_columns},
            **{
                name: "a business-key column of the parent hub of"
                for name in domains.parent_hub(source).business_key_columns
            },
        }
        if table.surrogate_key in delivered:
            raise ValueError(
                f"gold dimension {table.name!r} names {table.surrogate_key!r} as its "
                f"surrogate key, and that is {delivered[table.surrogate_key]} "
                f"{source.name!r}. The projection writes both into one column, so the "
                "delivered value disappears and the column is still there, full of "
                "plausible numbers"
            )


def _assert_no_source_column_collides_with_a_column_the_loader_writes(
    tables: Iterable[GoldTable], vault_tables: Mapping[str, VaultTable]
) -> None:
    """Refuse a source column named like one of the dimension's own.

    THE DIRECTION THE VAULT CANNOT REFUSE FOR US, and the gap is exact:
    `opl.vault.specs._validated_columns` refuses a payload column that collides with
    `opl.vault.columns.METADATA_COLUMNS` -- `load_date`, `record_source`, `applied_date`,
    `hash_diff` -- and knows nothing about `valid_from`, `valid_to` or `is_current`. A
    satellite payload column of one of those names is legal in the vault, correct in the
    vault, and would be silently overwritten here by the interval this loader computes.

    SCD2 ONLY, for the guard above's reason."""
    for table in _scd2(tables):
        source = _source_satellite(table, table.source_satellite, vault_tables)
        hub = domains.parent_hub(source)
        for role, columns in (
            ("payload column", source.payload_columns),
            ("business-key column", hub.business_key_columns),
        ):
            collided = sorted(set(columns) & DIMENSION_COLUMNS)
            if collided:
                raise ValueError(
                    f"gold dimension {table.name!r} reads {source.name!r}, whose "
                    f"{role}s include {collided}, and the loader writes that itself "
                    f"({', '.join(sorted(DIMENSION_COLUMNS))}). The vault does not "
                    "refuse it -- its own reserved set is four other names -- so the "
                    "column arrives here legal and leaves the projection replaced"
                )


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
    `registry[...] = ...` could add a table that never passed a guard."""
    collected = tuple(tables)
    known = domains.REGISTRY if vault_tables is None else vault_tables
    _assert_no_gold_name_is_owned_by_another_layer(collected, known)
    by_name = _assert_no_two_gold_tables_share_a_name(collected)
    _assert_no_two_dimensions_draw_from_one_payment_column(collected)
    _assert_every_dimension_reads_a_registered_satellite(collected, known)
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

# `dim_date`, AND ITS FACT-SIDE CARDINALITY IS 1 TODAY. Task 0 measured every payment in
# `bronze_payments` at 2026-08-01 (`docs/f3-run-evidence.md` §0.5, P1-P3: 10,000 distinct
# `event_time` values, all on one day), and the `between-snapshots` profile adds a second
# day, 2026-06-20. So the fact reaches ONE member of this dimension today and TWO once
# that profile lands, against a span of about fifty days. That is not an argument against
# building it -- a conformed date dimension is what makes the fact's date column a
# question anybody can group by -- but it IS the number that must be published beside it,
# which is why `opl.gold.conformed` measures `fact_side_cardinality` from the fact rather
# than letting the evidence say "thin".
#
# ONE ROLE, NOT THREE AND NOT TWO. The governing spec §4.3 asks for role-playing across
# transação / autorização / liquidação. `opl.contracts.payments` carries `event_time` and
# `emitted_at`; `emitted_at` is when the GENERATOR RELEASED the record, which is a
# property of the delivery -- the thing that may legitimately repeat -- and not a date
# the business transacted on. Counting it as a second role would buy a bigger number and
# a `dim_date` join that means nothing. A second role arrives with an `authorized_at` or
# a `settled_at` column in the payment contract, which bumps `SCHEMA_VERSION` and is a
# change to the generator, not to gold.
DIM_DATE = CalendarDimension(
    name="dim_date",
    surrogate_key="date_key",
    natural_key="full_date",
    fact_column=payments.EVENT_TIME_COLUMN,
    # WHERE THE SPAN'S OTHER END COMES FROM. `dim_date` must contain both RFB
    # `applied_date`s -- 2026-06-13 and 2026-07-11 -- because `dim_company`'s version
    # boundaries sit on them, and a calendar that cannot name the day a version opened is
    # a calendar the star's own history falls outside of. They are read from this
    # satellite's `applied_date` column at build time rather than declared: a date
    # literal here would be a second spelling of a value the vault owns.
    applied_date_source="sat_empresa_dados",
    roles=("event_date_key",),
)

# `dim_channel` FROM `payment_method`, AND NEVER FROM `payment_channel`. The drift column
# is undeclared by design and `_assert_the_fact_column_is_one_the_contract_carries`
# refuses it at import; `opl.contracts.payments`' own `DRIFT_COLUMN` block records that
# the name was chosen to be TEMPTING for exactly a dimension like this one.
#
# FIVE MEMBERS AND A FACT-SIDE CARDINALITY OF FIVE -- the one of these three that is not
# a constant column wearing a dimension's name. `opl.generator.stream` picks a method by
# index into `PAYMENT_METHODS` for every event, so all five rails appear in 20,150
# payments with overwhelming probability, and the measurement is what says so.
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

# `dim_currency`, AND IT HAS EXACTLY ONE MEMBER. `CURRENCIES = ("BRL",)`, every payment
# carries it, so every fact row points at that one row: fact-side cardinality 1 against
# 1 member. A dimension of one member cannot be wrong, and no test over it can fail --
# which is the honest thing to say about it rather than "thin".
#
# IT IS BUILT ANYWAY, FOR TWO REASONS THAT ARE NOT DECORATION. The contract's own comment
# says the column exists "so that a second currency is a value change instead of a schema
# change"; this dimension is where that value change lands, and a star that gained FX
# later would otherwise have to add a dimension AND rewrite the fact's projection. And
# the amount's two decimal places are a consequence of BRL's minor unit -- the fact
# carries a scale it cannot explain unless something holds the currency.
DIM_CURRENCY = EnumeratedDimension(
    name="dim_currency",
    surrogate_key="currency_key",
    natural_key="currency_code",
    fact_column="currency",
    members=payments.CURRENCIES,
)

TABLES: tuple[GoldTable, ...] = (DIM_COMPANY, DIM_DATE, DIM_CHANNEL, DIM_CURRENCY)

# AT IMPORT, in this module's own foot, for the reason both sibling registries state:
# a malformed registry must break the import of every module that reads it rather than
# the one job that touches the table it is malformed about.
REGISTRY: Mapping[str, GoldTable] = build_registry(TABLES)
