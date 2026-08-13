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

WHERE THE NEXT TABLE GOES, so the decision is not re-made. `dim_date`, `dim_channel` and
`dim_currency` are NOT SCD2 -- they have no version chain and no `applied_date` to order
one by -- and `fact_payment` is not a dimension at all. Each is a new KIND: its
dataclass and its `__post_init__` land in this file beside `Scd2Dimension`, and only
when this file approaches the project's 800-line cap do the kinds move wholesale to an
`opl.gold.specs` module. That is exactly the split `opl.vault.specs` made out of
`opl.vault.registry`, and its docstring argues the shape at length; there is no reason
to re-derive it here, and every reason not to pre-build it for kinds that do not exist.

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
from dataclasses import dataclass
from types import MappingProxyType

from opl.bronze.registry import REGISTRY as BRONZE_REGISTRY
from opl.gold.columns import DIMENSION_COLUMNS
from opl.vault import domains
from opl.vault.registry import Satellite, VaultTable

__all__ = [
    "DIM_COMPANY",
    "REGISTRY",
    "TABLES",
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


@dataclass(frozen=True, kw_only=True)
class Scd2Dimension:
    """A Kimball type-2 dimension derived from ONE Data Vault satellite: a surrogate
    key, and the name of the satellite whose versions become its rows.

    THREE FIELDS, AND EVERYTHING ELSE IS READ FROM THE VAULT. The payload columns are
    the satellite's, the natural key and its zero-pad width are the parent hub's, and
    the parent hub is `opl.vault.domains.parent_hub`'s answer. None of them is declared
    here, and that is the decision this spec is: a dimension that restated its source's
    payload would be a second spelling of a column list, and the copy that goes stale is
    always the one no load ever reads. It also makes the extension free -- a satellite
    that gains a payload column gains it in the dimension on the next load, with no gold
    edit at all.

    `kw_only`, like `opl.bronze.registry.BronzeTable` and every vault kind: `name`,
    `surrogate_key` and `source_satellite` are three adjacent strings, so a positional
    construction that permuted them would type-check perfectly and register a dimension
    called `company_sk` reading a satellite called `dim_company`.

    NO TARGET TABLE, NO CATALOG, NO SCHEMA. A spec carries an unqualified `name` and the
    loader takes the qualified table as an argument, so `opl.config` is consulted by
    whatever calls the loader and nowhere in this layer -- the same division
    `opl.vault.registry` states and `tests/test_gold_job_wiring.py` asserts over the
    entry point."""

    name: str
    surrogate_key: str
    source_satellite: str

    def __post_init__(self) -> None:
        for role, value in (
            ("name", self.name),
            ("surrogate key", self.surrogate_key),
            ("source satellite", self.source_satellite),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"a gold dimension needs a {role}, got {value!r}. A dimension with "
                    "no source satellite is a table nothing can derive"
                )
        if self.surrogate_key in DIMENSION_COLUMNS:
            raise ValueError(
                f"dimension {self.name!r} names {self.surrogate_key!r} as its surrogate "
                f"key, and the loader writes that column itself "
                f"({', '.join(sorted(DIMENSION_COLUMNS))}). The collision does not "
                "crash: one projection writes two values into one column, so the "
                "surrogate key is silently a timestamp or a flag and every fact that "
                "joins on it matches nothing"
            )


# THE UNION EVERY GUARD BELOW READS RATHER THAN RESTATES, so a kind added above extends
# each refusal in one edit. One member today; it is a union rather than a bare alias so
# that adding `dim_date`'s kind is a word here and not a rewrite of five signatures.
GoldTable = Scd2Dimension


def _bronze_delta_names() -> Mapping[str, str]:
    """Every Delta name bronze owns, mapped to what it is -- staging, bronze table or
    quarantine -- so a refusal can say which one was collided with.

    ALL THREE AND NOT ONLY THE BRONZE ONE. A promote appends into staging and the DQ
    gate appends into quarantine, so a dimension sitting on either is reached by a job
    nobody would think to look at -- and the quarantine is the documented case, the one
    `opl.bronze.registry`'s own docstring records as having "sent estab triagers to a
    table full of unrelated F1.2 lookup rows"."""
    owners: dict[str, str] = {}
    for spec in BRONZE_REGISTRY.values():
        for role, name in (
            ("staging table", spec.staging),
            ("bronze table", spec.bronze),
            ("quarantine", spec.quarantine),
        ):
            owners[name] = f"{spec.name}'s {role}"
    return owners


def _assert_no_gold_name_is_owned_by_another_layer(
    tables: Iterable[GoldTable], vault_tables: Mapping[str, VaultTable]
) -> None:
    """Refuse a gold table whose name a bronze or vault table already holds.

    THE GUARD THE FLAT SCHEMA FORCES -- see the module docstring for why no other file
    in this repository can hold it. Ordered FIRST among the whole-set guards for
    `opl.bronze.registry`'s "individually wrong before collectively wrong" reason: a
    name another layer owns is wrong on its own, and reporting a duplicate first would
    tell the operator to rename one of two tables that both must be renamed."""
    bronze = _bronze_delta_names()
    for table in tables:
        if table.name in vault_tables:
            raise ValueError(
                f"gold table {table.name!r} is a name already owned by the vault. "
                "Free Edition ships ONE catalog and ONE schema, so both would resolve "
                "to the same Delta table -- and every loader in this repository writes "
                "with mode('append'), which does not refuse a name it does not own: it "
                "appends dimension rows into the satellite, or merges two populations "
                "where the shapes happen to agree, with both runs reporting success"
            )
        if table.name in bronze:
            raise ValueError(
                f"gold table {table.name!r} is a name already owned by bronze "
                f"({bronze[table.name]}). One catalog, one schema, and every writer in "
                "this repository appends -- so the promote or the DQ gate would append "
                "into this dimension, or this loader into theirs, without failing"
            )


def _assert_no_two_gold_tables_share_a_name(tables: Iterable[GoldTable]) -> dict[str, GoldTable]:
    """Every gold table by name, refusing a name two of them claim.

    Returns the mapping rather than only checking, so there is no second loop that could
    build a different one -- `opl.vault.registry._collected_tables`' shape."""
    collected: dict[str, GoldTable] = {}
    for table in tables:
        if table.name in collected:
            raise ValueError(
                f"two specs both declare a gold table called {table.name!r}. One of "
                "them would load into the other's Delta table, with both runs "
                "reporting success"
            )
        collected[table.name] = table
    return collected


def _source_satellite(
    table: GoldTable, vault_tables: Mapping[str, VaultTable]
) -> Satellite:
    """`table`'s source, resolved against the vault registry, or refuse naming it.

    ONE RESOLUTION, SHARED BY THE GUARD AND BY THE TWO CHECKS THAT FOLLOW IT, for
    `opl.vault.registry._link_hubs`' reason: a resolver that repeated the guard's
    conditions in a weaker form is how a registry that passed its guards still returns
    something wrong."""
    source = vault_tables.get(table.source_satellite)
    if source is None:
        raise ValueError(
            f"gold dimension {table.name!r} derives from {table.source_satellite!r}, "
            f"which no vault domain registers. Registered: "
            f"{', '.join(sorted(vault_tables))}"
        )
    if not isinstance(source, Satellite):
        raise ValueError(
            f"gold dimension {table.name!r} derives from {table.source_satellite!r}, "
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
    for table in tables:
        _source_satellite(table, vault_tables)


def _assert_no_surrogate_key_collides_with_its_source(
    tables: Iterable[GoldTable], vault_tables: Mapping[str, VaultTable]
) -> None:
    """Refuse a surrogate key that is already a column the source delivers.

    A WHOLE-SET GUARD AND NOT A `__post_init__` CHECK, because it cannot be answered
    about one table in isolation: `razao_social` is a perfectly good surrogate-key name
    until you know which satellite this dimension reads. The parent hub's business key
    is checked with the payload for the same reason -- `cnpj_basico` is written into the
    dimension from the hub, so a surrogate key of that name loses one of the two."""
    for table in tables:
        source = _source_satellite(table, vault_tables)
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
    vault, and would be silently overwritten here by the interval this loader computes."""
    for table in tables:
        source = _source_satellite(table, vault_tables)
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
# The star. One table today: F3 Task 1 builds the dimension the fact must reach.
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

TABLES: tuple[GoldTable, ...] = (DIM_COMPANY,)

# AT IMPORT, in this module's own foot, for the reason both sibling registries state:
# a malformed registry must break the import of every module that reads it rather than
# the one job that touches the table it is malformed about.
REGISTRY: Mapping[str, GoldTable] = build_registry(TABLES)
