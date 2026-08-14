# src/opl/gold/registry_guards.py
"""The gold registry's WHOLE-SET guards: everything that cannot be answered about one
table in isolation, called from `opl.gold.registry` at its own import.

WHY A SECOND MODULE AT ALL, AND WHY NOW. `opl.gold.registry`'s docstring pre-declared this
split and named the moment: the whole-set guards are "an `opl.gold.registry_guards` waiting
to happen, in `opl.bronze.registry_collisions`' shape ... It is not made now because a split
of eight guards to gain sixteen lines would be a diff nobody can review beside a task that
also builds a fact -- AND THE NEXT TABLE THIS FILE GAINS CANNOT SAY THAT." F-API Task 4 is
that task: it widens the fact's column list and gives `dim_date` a second role, and master
protocol section 4.12 says whoever touches a file at the cap splits it FIRST. So this is a
scheduled split rather than a discovered one.

THE SEAM IS NOT THE LINE COUNT, which would put anything here. What lives in this module is
every check that needs to see the OTHER tables or the OTHER LAYERS: a name bronze or the
vault already owns, a name two gold specs claim, a payment column two dimensions draw from,
a satellite that must resolve against the vault registry, a fact whose conformed set is not
the registry's, two of a fact's own columns that collide only once the dimensions' fact keys
are known. What stays in `registry.py` is the DECLARATIONS, `build_registry`'s call ORDER --
which is load-bearing and has to be reviewable in one place -- and `table_spec`.

THE TABLES ARRIVE AS ARGUMENTS, and that is the only reason these functions take any.
`registry.py` calls them at its own import, so importing `REGISTRY` back from here would be
a cycle; a parameter leaves the dependency edge in ONE direction and one place, and a guard
that takes the set it validates says in its signature what it reads.
`opl.bronze.registry_collisions` records the same decision and rejects the same alternative
(a deferred import, which would hide the cycle rather than remove it).

WHAT IS *NOT* HERE. `vault_tables` still defaults inside `build_registry` rather than in
these signatures: the default is `opl.vault.domains.REGISTRY`, and a guard that reached for
a live registry when handed none would be substitutable in name only -- which is the
property `tests/gold/test_registry.py` drives every refusal below through.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping

from opl.bronze.registry import REGISTRY as BRONZE_REGISTRY
from opl.contracts import payments
from opl.gold.columns import DIMENSION_COLUMNS, LOAD_DATE, RECORD_SOURCE
from opl.gold.specs import (
    CalendarDimension,
    ConformedDimension,
    GoldTable,
    PaymentFact,
    PointInTimeTable,
    Scd2Dimension,
    fact_keys,
)
from opl.vault import domains
from opl.vault.registry import Hub, Satellite, VaultTable


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
    grain, the measure and the DERIVED measures are the fact's own and `opl.gold.fact_spec`
    refuses a collision among them; the conformed foreign keys come from the dimensions the
    fact reaches (`opl.gold.specs.fact_keys`), so the collision that this catches is one
    nobody can see from either spec alone -- two enumerated dimensions sharing a
    `surrogate_key`, which nothing else in this file refuses, or a calendar whose role is
    spelled like a role key or like a derived measure.

    `fact_keys` AND NOT `fact_key`, WHICH IS WHERE F-API T4b REACHES THIS FILE. The singular
    property returned one string per dimension and raised over a calendar with two roles, so
    this comprehension was one of its four consumers. Iterating the tuple is what makes the
    guard TOTAL over the projection: `dim_date` contributes `event_date_key` AND
    `fx_rate_date_key`, and a second role spelled like the first is refused here rather than
    silently overwriting it -- the two are both integer date keys, so nothing else would.

    ORDERED AFTER THE GUARD ABOVE, which is load-bearing rather than tidy: that one
    establishes that every name in `fact.conformed` is a registered conformed dimension, so
    the lookup below cannot raise a `KeyError` from a mistyped name and hide behind it."""
    for fact in _facts(tables):
        columns = [
            *fact.role_keys,
            *(key for name in fact.conformed for key in fact_keys(by_name[name])),
            fact.grain_key,
            fact.measure,
            *fact.derived_names,
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


