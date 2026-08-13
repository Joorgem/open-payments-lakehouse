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
life outside Spark, and it is therefore the only place the question can be asked."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from types import MappingProxyType

from opl.bronze.registry import REGISTRY as BRONZE_REGISTRY
from opl.contracts import payments
from opl.gold.columns import DIMENSION_COLUMNS, LOAD_DATE, RECORD_SOURCE
from opl.gold.specs import (
    CalendarDimension,
    ConformedDimension,
    EnumeratedDimension,
    GoldTable,
    PaymentFact,
    PointInTimeTable,
    Scd2Dimension,
)
from opl.vault import domains
from opl.vault.registry import Hub, Satellite, VaultTable

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
    "EnumeratedDimension",
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
    agrees with a report joining the other right up until their member sets diverge.

    THE FILTER IS AN INCLUSION AND WAS AN EXCLUSION, WHICH IS THE CORRECTION F3 TASK 2
    HAD TO MAKE BEFORE IT COULD REGISTER ANYTHING. It read `if isinstance(table,
    Scd2Dimension): continue` -- "everything that is not SCD2 has a `fact_column`" -- a
    claim that was true of the three kinds that existed and that nothing checked. Adding
    `PointInTimeTable`, which has no `fact_column` because nothing joins to it, turned
    this line into an `AttributeError` raised at IMPORT of `opl.gold.registry`, i.e. at
    import of every gold module and every gold job. An exclusion list is a guard that
    assumes the shape of the kinds it has not met; the inclusion below names the two
    kinds this question is actually about, so the next kind is simply not asked."""
    drawn: dict[str, str] = {}
    for table in tables:
        if not isinstance(table, ConformedDimension):
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


def _pit_hub(table: PointInTimeTable, vault_tables: Mapping[str, VaultTable]) -> Hub:
    """`table`'s hub, resolved against the vault registry, or refuse naming it.

    `_source_satellite`'s shape and its reason: one resolution shared by the guard and by
    everything that follows it, so a resolver cannot repeat the guard's conditions in a
    weaker form."""
    hub = vault_tables.get(table.hub)
    if hub is None:
        raise ValueError(
            f"point-in-time table {table.name!r} is built over {table.hub!r}, which no "
            f"vault domain registers. Registered: {', '.join(sorted(vault_tables))}"
        )
    if not isinstance(hub, Hub):
        raise ValueError(
            f"point-in-time table {table.name!r} is built over {table.hub!r}, which is "
            "not a hub. A PIT's spine is a hub's KEY SET -- one row per key per as-of "
            "date -- and every other vault kind either has no key set of its own or has "
            "one at a grain the satellites below it do not share"
        )
    return hub


def _assert_every_pit_resolves_its_hub_and_its_satellites(
    tables: Iterable[GoldTable], vault_tables: Mapping[str, VaultTable]
) -> None:
    """Refuse a PIT whose hub or satellites are missing, are the wrong kind, or -- the
    one that matters -- do not belong together.

    THE PARENTAGE CHECK IS THIS GUARD'S WHOLE POINT AND IT IS SILENT WHEN IT FAILS. A PIT
    joins nothing: it UNIONS the hub's keys with each satellite's (hash key, applied_date)
    pairs and groups them. Handed a satellite of ANOTHER hub, the union is between a
    column called `hub_estabelecimento_hk` and one called `hub_empresa_hk` -- which
    `unionByName` refuses loudly ONLY while the two hubs spell their hash keys
    differently. They do today, and nothing in `opl.vault.specs` requires it: two hubs may
    name their hash key the same string, at which point the union succeeds, the group-by
    merges two key spaces, and every pointer for a key that exists in both is taken over
    the wrong satellite's history. The refusal is here so it cannot depend on a naming
    accident in another package.

    THE HASH KEY AND THE AS-OF COLUMN ARE CHECKED TOGETHER FOR THE SAME REASON THE SPEC
    COULD NOT DO IT: `as_of_date` is a perfectly good column name until you know which
    hub this table is over, and a hub whose hash key is spelled that way would have both
    written into one column by one projection."""
    for table in tables:
        if not isinstance(table, PointInTimeTable):
            continue
        hub = _pit_hub(table, vault_tables)
        _assert_the_as_of_column_is_not_the_hubs_hash_key(table, hub)
        for declared in table.satellites:
            _assert_the_satellite_hangs_off_this_pits_hub(table, declared, hub, vault_tables)


def _assert_the_as_of_column_is_not_the_hubs_hash_key(
    table: PointInTimeTable, hub: Hub
) -> None:
    if table.as_of_column == hub.hash_key:
        raise ValueError(
            f"point-in-time table {table.name!r} names {table.as_of_column!r} as its "
            f"as-of column, and that is {hub.name!r}'s hash key. One projection writes "
            "both into one column, so every row's key is a date or every row's as-of is a "
            "digest -- and the table is still the right size"
        )


def _assert_the_satellite_hangs_off_this_pits_hub(
    table: PointInTimeTable,
    declared: str,
    hub: Hub,
    vault_tables: Mapping[str, VaultTable],
) -> None:
    source = vault_tables.get(declared)
    if not isinstance(source, Satellite):
        raise ValueError(
            f"point-in-time table {table.name!r} points at {declared!r}, which is not a "
            f"registered satellite. Registered: {', '.join(sorted(vault_tables))}. A PIT "
            "points at version chains; a hub has no `applied_date` and a link's chain is "
            "at another grain"
        )
    if source.parent != hub.name:
        raise ValueError(
            f"point-in-time table {table.name!r} is built over {hub.name!r} and points at "
            f"{declared!r}, which hangs off {source.parent!r}. The two are unioned on the "
            "hash key, so this is caught by name today only because the two hubs spell "
            "their hash keys differently -- nothing requires that. Where they agree, the "
            "union succeeds and every pointer is taken over another hub's history"
        )


def _facts(tables: Iterable[GoldTable]) -> Iterable[PaymentFact]:
    """The facts among `tables`. AN INCLUSION, like `_scd2` and unlike the guard this
    repository has already been bitten by -- see
    `_assert_no_two_dimensions_draw_from_one_payment_column`."""
    return (table for table in tables if isinstance(table, PaymentFact))


def _assert_every_fact_reaches_every_dimension_this_star_holds(
    tables: Iterable[GoldTable], by_name: Mapping[str, GoldTable]
) -> None:
    """Refuse a fact whose company dimension is missing or is the wrong kind, and refuse a
    registry whose CONFORMED dimensions are not exactly the set the fact reaches.

    THE SECOND HALF IS THE ONE WORTH HAVING, and it is the only mechanical answer this
    repository has to the charge it levels at `pit_estabelecimento`. A conformed dimension
    exists to be reached by a fact: `dim_channel` costs a job task and a table and buys
    nothing at all unless `fact_payment` carries a key into it. Left as a convention, a
    dimension added later is a dimension the fact silently does not reach, with both
    builds reporting success and every report over it returning one row per member and no
    facts. Stated as an equality, adding a conformed dimension without adding it to the
    fact turns the import of every gold module red.

    IT IS AN EQUALITY AND NOT A SUBSET IN BOTH DIRECTIONS ON PURPOSE. A fact naming a
    conformed dimension the registry does not hold would fail in Spark's analysis on a
    column; a registry holding one the fact does not name fails nowhere, which is exactly
    why the check has to be written from the registry's side as well.

    THE SCD2 SIDE IS RESOLVED AGAINST *THIS* REGISTRY AND NOT THE VAULT'S, which is the
    difference between this kind and every other one here: a fact reads BRONZE and joins to
    a GOLD table, so `company_dimension` is a gold name. Handed a conformed dimension it
    would look for a satellite's version chain in a table that has no interval at all --
    every as-of predicate over a missing column, caught by Spark and only after a session
    has started."""
    conformed = {
        name for name, table in by_name.items() if isinstance(table, ConformedDimension)
    }
    for fact in _facts(tables):
        dimension = by_name.get(fact.company_dimension)
        if not isinstance(dimension, Scd2Dimension):
            raise ValueError(
                f"payment fact {fact.name!r} resolves its counterparties against "
                f"{fact.company_dimension!r}, which is not a registered SCD2 dimension of "
                f"this star ({', '.join(sorted(by_name))}). Both role keys are as-of "
                "lookups over a half-open interval, and only an SCD2 dimension has one"
            )
        if set(fact.conformed) != conformed:
            raise ValueError(
                f"payment fact {fact.name!r} reaches {sorted(fact.conformed)} and this "
                f"registry holds the conformed dimensions {sorted(conformed)}. A conformed "
                "dimension no fact reaches is a table nothing joins to -- it builds, it is "
                "well-formed, and every report over it returns its members and no facts"
            )


def _assert_no_two_columns_of_one_fact_share_a_name(
    tables: Iterable[GoldTable], by_name: Mapping[str, GoldTable]
) -> None:
    """Refuse a fact whose projected columns are not distinct.

    A WHOLE-SET GUARD BECAUSE HALF THE COLUMN LIST IS OTHER TABLES'. The role keys, the
    grain and the measure are the fact's own and `opl.gold.specs` refuses a collision among
    them; the conformed foreign keys are DERIVED from the dimensions the fact reaches
    (`fact_key`), so the collision that this catches is one nobody can see from either spec
    alone -- two enumerated dimensions sharing a `surrogate_key`, which nothing else in
    this file refuses, or a calendar whose role is spelled like a role key.

    ORDERED AFTER THE GUARD ABOVE, which is load-bearing rather than tidy: that one
    establishes that every name in `fact.conformed` is a registered conformed dimension, so
    the lookup below cannot raise a `KeyError` from a mistyped name and hide behind it."""
    for fact in _facts(tables):
        columns = [
            *fact.role_keys,
            *(by_name[name].fact_key for name in fact.conformed),
            fact.grain_key,
            fact.measure,
            payments.EVENT_TIME_COLUMN,
            LOAD_DATE,
            RECORD_SOURCE,
        ]
        repeated = sorted({name for name in columns if columns.count(name) > 1})
        if repeated:
            raise ValueError(
                f"payment fact {fact.name!r} projects {repeated} more than once "
                f"({columns}). One projection writes two values into one column, so one "
                "of them survives, every row is still present and every join on the lost "
                "key matches nothing"
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
# a correct row resolves to TWO. `opl.gold.specs._assert_every_counterparty_plays_exactly
# _one_role` refuses the reading that is satisfiable, which is a fact that joins on the
# payer alone.
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
FACT_PAYMENT = PaymentFact(
    name="fact_payment",
    grain_key=payments.IDENTITY_COLUMN,
    measure="amount",
    company_dimension="dim_company",
    roles=(
        ("payer_cnpj_basico", "payer_company_sk"),
        ("payee_cnpj_basico", "payee_company_sk"),
    ),
    # IN PROJECTION ORDER, and a Delta append matches POSITIONALLY -- the same property
    # `PIT_ESTABELECIMENTO.satellites` carries. Permuting these three re-writes each key
    # into another's column, and all three are integers, so nothing fails.
    #
    # ALL THREE, AND THE REGISTRY REFUSES ANYTHING LESS. `_assert_every_fact_reaches_every
    # _dimension_this_star_holds` states it as an EQUALITY against the registry's conformed
    # set, so a conformed dimension added later without a key here turns the import red
    # rather than quietly becoming a table nothing joins to.
    conformed=("dim_date", "dim_channel", "dim_currency"),
)

TABLES: tuple[GoldTable, ...] = (
    DIM_COMPANY, DIM_DATE, DIM_CHANNEL, DIM_CURRENCY, PIT_ESTABELECIMENTO, FACT_PAYMENT,
)

# AT IMPORT, in this module's own foot, for the reason both sibling registries state:
# a malformed registry must break the import of every module that reads it rather than
# the one job that touches the table it is malformed about.
REGISTRY: Mapping[str, GoldTable] = build_registry(TABLES)
