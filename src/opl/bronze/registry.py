"""One literal per bronze table: the single answer to "what is table X?".

WHY DECLARED AND NOT DERIVED: the live names follow no single pattern --
`bronze_cnpj_estab_staging` is abbreviated where `bronze_cnpj_estabelecimentos`
is spelled out, and the lookup uses `lookup` where estab uses `estab`. Deriving
`f"bronze_cnpj_{name}_staging"` would be DRY-er and would force renaming Delta
tables, one of them holding 71,874,448 rows, to satisfy an aesthetic.

So the point is not less repetition. The point is that each table's
staging/bronze/quarantine TRIPLE lives in one literal, where it cannot drift --
and drift is the documented defect: a quarantine name hardcoded in a job YAML
"sent estab triagers to a table full of unrelated F1.2 lookup rows"."""
from __future__ import annotations

from dataclasses import dataclass

from opl.contracts.cnpj_schemas import TABLES

# How a table's raw files reach the Volume.
LANDING_ZIPS = "zips"    # PUT the zip, unzip in the Volume (multi-part groups)
LANDING_LOCAL = "local"  # unzip locally, PUT the inner file (the tiny lookups)
LANDING_MODES = frozenset({LANDING_ZIPS, LANDING_LOCAL})


class UnknownTable(ValueError):
    """A table name that is not registered. Raised before Spark, on purpose.

    A ValueError, NOT a KeyError, though the lookup it guards is a dict lookup.
    Two reasons, both learned the hard way from messages that had to survive into
    a Databricks run log:

    1. `KeyError.__str__` re-`repr`s its argument. The message below is prose
       written to be read by an operator at 3am; raised as a KeyError it arrives
       quoted, with escaped newlines, as `"unknown bronze table 'x' -- ..."`.
    2. A KeyError is silently catchable by code that never named it. `table_spec`
       is called from job entry points, and entry points are exactly where
       `except KeyError` wrappers around argument parsing live -- one of those
       several frames up would swallow a mistyped table name and replace this
       message with a generic usage line, which is the opposite of the point.

    The table name is an operator-supplied value validated at a boundary, which
    is ValueError's job, and matches how `require_batch_id` refuses."""


@dataclass(frozen=True, kw_only=True)
class BronzeTable:
    """Everything table-specific about one bronze table. Frozen: config is data.

    `kw_only`: `landing` and `prefix` are adjacent and both str-ish, so a
    positional construction that swapped them would type-check and silently point
    a table at the wrong landing mode. Keyword-only makes the field order unable
    to matter rather than merely currently-harmless -- every construction site
    already passes keywords, so this costs nothing and closes the trap."""

    name: str
    contract: str
    table_key: str
    staging: str
    bronze: str
    quarantine: str
    subdir: str
    landing: str
    prefix: str | None
    # DDL re-asserted after every promote. `{table}` is filled with the qualified
    # bronze name by the caller. A tuple, not a list: the spec is frozen and its
    # fields have to be too, or `constraints.append(...)` would mutate shared state.
    constraints: tuple[str, ...]


REGISTRY: dict[str, BronzeTable] = {
    "lookup": BronzeTable(
        name="lookup",
        contract="lookup",
        table_key="bronze_cnpj_lookup",
        staging="bronze_cnpj_lookup_staging",
        bronze="bronze_cnpj_lookup",
        quarantine="bronze_cnpj_lookup_quarantine",
        subdir="lookups",
        landing=LANDING_LOCAL,
        # The six lookups arrive as six differently-named single files, routed to
        # one table by filename suffix (opl.bronze.lookup_routing), so no single
        # prefix identifies them.
        prefix=None,
        constraints=(
            "ALTER TABLE {table} ALTER COLUMN codigo SET NOT NULL",
            "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS codigo_not_blank",
            "ALTER TABLE {table} ADD CONSTRAINT codigo_not_blank "
            "CHECK (length(trim(codigo)) > 0)",
        ),
    ),
    "estabelecimentos": BronzeTable(
        name="estabelecimentos",
        contract="estabelecimentos",
        table_key="bronze_cnpj_estab",
        staging="bronze_cnpj_estab_staging",
        bronze="bronze_cnpj_estabelecimentos",
        quarantine="bronze_cnpj_estab_quarantine",
        subdir="estabelecimentos",
        landing=LANDING_ZIPS,
        # Explicit rather than implied by the FILE_GROUPS dict key (carry-forward
        # #10): the key happening to equal the prefix is a coincidence nothing
        # enforces, and a group whose key drifted from its prefix would go looking
        # for files that are not there.
        prefix="Estabelecimentos",
        constraints=(
            "ALTER TABLE {table} ALTER COLUMN cnpj_basico SET NOT NULL",
            "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS cnpj_basico_len8",
            "ALTER TABLE {table} ADD CONSTRAINT cnpj_basico_len8 "
            "CHECK (length(trim(cnpj_basico)) = 8)",
        ),
    ),
}


def table_spec(name: str) -> BronzeTable:
    """The registered spec for `name`, or refuse naming the valid alternatives.

    Refuses BEFORE Spark, like `require_batch_id`: an operator who mistyped a
    table should not wait for a serverless session to be told so."""
    try:
        return REGISTRY[name]
    except KeyError:
        valid = ", ".join(sorted(REGISTRY))
        raise UnknownTable(
            f"unknown bronze table {name!r} -- registered tables are: {valid}. "
            "Every job task takes the table name as a parameter; check the "
            "`table` parameter of the job that failed."
        ) from None


def _assert_contracts_exist() -> None:
    """Fail at import if a spec names a contract that does not exist.

    At import rather than at use: a registry entry pointing at a missing contract
    is a typo, and a typo should not wait for the one job run that touches that
    table to surface."""
    for spec in REGISTRY.values():
        if spec.contract not in TABLES:
            raise UnknownTable(
                f"{spec.name} names contract {spec.contract!r}, which is not in "
                f"cnpj_schemas.TABLES ({', '.join(sorted(TABLES))})"
            )


def _assert_landing_modes_known() -> None:
    """Fail at import if a spec names a landing mode that does not exist.

    AT THE BOUNDARY, not in the consumer that dispatches on it. Nothing reads
    `landing` yet -- that dispatch is Task 6's -- so it is tempting to argue a bad
    value would fail loudly there. It would not: a dispatch written as
    `if landing == LANDING_ZIPS: ... else: ...` swallows a typo into the `else`
    branch, and a table that should have been unzipped in the Volume gets treated
    as a tiny local lookup, silently. A value that is wrong is refused where it is
    DECLARED; leaning on a downstream consumer to notice is exactly the coupling
    this registry exists to remove.

    A plain ValueError rather than UnknownTable: nothing here is an unknown
    *table*, and UnknownTable's docstring describes an operator-supplied name at a
    job boundary, which a mode typo committed to source is not."""
    for spec in REGISTRY.values():
        if spec.landing not in LANDING_MODES:
            raise ValueError(
                f"{spec.name} names landing mode {spec.landing!r}, which is not one "
                f"of: {', '.join(sorted(LANDING_MODES))}"
            )


_assert_contracts_exist()
_assert_landing_modes_known()
