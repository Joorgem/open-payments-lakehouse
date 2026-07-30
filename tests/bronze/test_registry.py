"""The registry is the one place that answers "what is table X?".

Its value is not less repetition -- the names are DECLARED, not derived, because
deriving them would force renaming live Delta tables (bronze_cnpj_estab_staging
abbreviated against bronze_cnpj_estabelecimentos spelled out) to satisfy a
pattern. Its value is that each table's staging/bronze/quarantine triple lives in
one literal, where it cannot drift. Drift is the documented defect: a hardcoded
quarantine name "sent estab triagers to a table full of unrelated F1.2 lookup
rows"."""
from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from opl.bronze.registry import (
    LANDING_LOCAL,
    LANDING_ZIPS,
    REGISTRY,
    UnknownTable,
    table_spec,
)
from opl.contracts.cnpj_schemas import TABLES


def test_the_two_live_tables_keep_the_exact_names_they_have_today():
    """`prefix` and `landing` are pinned here too, because the uniqueness tests
    cover DUPLICATION and neither covers a TYPO. `prefix="Estabelecimento"`
    (singular) is unique, passes every other test, and under-ingests silently --
    the same defect class the uniqueness tests close, reached by another route."""
    lookup = table_spec("lookup")
    assert lookup.staging == "bronze_cnpj_lookup_staging"
    assert lookup.bronze == "bronze_cnpj_lookup"
    assert lookup.quarantine == "bronze_cnpj_lookup_quarantine"
    assert lookup.table_key == "bronze_cnpj_lookup"
    assert lookup.subdir == "lookups"
    assert lookup.prefix is None
    assert lookup.landing == LANDING_LOCAL

    estab = table_spec("estabelecimentos")
    assert estab.staging == "bronze_cnpj_estab_staging"
    assert estab.bronze == "bronze_cnpj_estabelecimentos"
    assert estab.quarantine == "bronze_cnpj_estab_quarantine"
    assert estab.table_key == "bronze_cnpj_estab"
    assert estab.subdir == "estabelecimentos"
    assert estab.prefix == "Estabelecimentos"
    assert estab.landing == LANDING_ZIPS


def test_no_two_tables_share_a_staging_bronze_or_quarantine_name():
    """The defect class this registry exists to close, asserted directly.

    Checked ACROSS the three roles, not within each: a table whose quarantine
    equals another's staging would route rejects into a table a promote reads."""
    seen: dict[str, str] = {}
    for spec in REGISTRY.values():
        for role, value in (
            ("staging", spec.staging),
            ("bronze", spec.bronze),
            ("quarantine", spec.quarantine),
        ):
            assert value not in seen, (
                f"{spec.name}.{role} == {value!r}, already used by {seen[value]}"
            )
            seen[value] = f"{spec.name}.{role}"


def test_no_two_tables_share_a_landing_subdir():
    """Same defect as a shared quarantine, one layer down in the Volume.

    Two tables pointed at one landing directory is the recursive-discovery
    failure F1.3 documented: a stream reading a dir it was not meant to read
    ingested a probe planted in a sibling subdir. F1.4b adds Empresas and Socios
    by copy-pasting these entries, and `subdir` is one of the fields that does
    NOT contain the table's own bronze name -- so a careful find/replace over
    `bronze_cnpj_*` sails straight past it."""
    seen: dict[str, str] = {}
    for spec in REGISTRY.values():
        assert spec.subdir not in seen, (
            f"{spec.name}.subdir == {spec.subdir!r}, already used by {seen[spec.subdir]}"
        )
        seen[spec.subdir] = spec.name


def test_no_two_tables_share_a_file_prefix():
    """The third leg of the copy-paste trio, and the one that fails silently.

    A stale `subdir` collides two tables in one landing dir; a stale `prefix`
    just goes looking for files that are not there and under-ingests without
    erroring -- the class this project rejected globs for. In the Scenario B
    probe a stale prefix was caught only INCIDENTALLY, because that paste left
    `subdir` stale too; a paste that fixes `subdir` and misses `prefix` passed
    everything until this test existed.

    `None` is skipped, not defaulted, because the lookup's absent prefix is a
    real property and not a gap: its six lookups arrive as six differently-named
    single files routed into one table by filename suffix
    (`opl.bronze.lookup_routing`), so no single prefix identifies them. Treating
    absent as a value to be filled in would invent a prefix that cannot exist;
    treating two absences as a collision would forbid a second such table."""
    seen: dict[str, str] = {}
    for spec in REGISTRY.values():
        if spec.prefix is None:
            continue
        assert spec.prefix not in seen, (
            f"{spec.name}.prefix == {spec.prefix!r}, already used by {seen[spec.prefix]}"
        )
        seen[spec.prefix] = spec.name


def test_every_registered_table_has_a_contract():
    for spec in REGISTRY.values():
        assert spec.contract in TABLES, f"{spec.name} names contract {spec.contract!r}"


def test_every_constraint_references_a_column_of_its_own_contract():
    """Catches constraints copy-pasted from a table with different key columns.

    Deliberately a substring check against the contract's column list and NOT a
    regex that extracts column names from the DDL: a half-parser for SQL would be
    more fragile than the gap it closes, and would fail on the next constraint
    shape nobody anticipated. Asking only that each statement mentions at least
    one column the table actually has is enough to catch a wholesale copy-paste.

    Known and accepted limit: it cannot catch a paste between two tables that
    share a key column -- estabelecimentos, empresas and socios are all keyed on
    `cnpj_basico`, so estab's constraints on a socios entry would satisfy this.
    `test_no_two_tables_share_a_landing_subdir` is what catches that same paste."""
    for spec in REGISTRY.values():
        columns = TABLES[spec.contract]
        for statement in spec.constraints:
            assert any(column in statement for column in columns), (
                f"{spec.name} constraint {statement!r} names no column of its "
                f"contract {spec.contract!r} ({', '.join(columns)}) -- "
                "constraints copy-pasted from another table?"
            )


def test_every_registered_table_has_a_checkpoint_namespace_of_its_own():
    keys = [spec.table_key for spec in REGISTRY.values()]
    assert len(keys) == len(set(keys)), f"table_key collision in {keys}"


def test_an_unknown_table_is_refused_by_name_and_lists_the_valid_ones():
    with pytest.raises(UnknownTable) as excinfo:
        table_spec("estabelecimento")  # a real typo: singular
    message = str(excinfo.value)
    assert "estabelecimento" in message
    assert "estabelecimentos" in message and "lookup" in message


def test_the_refusal_reaches_an_operator_as_prose_not_as_a_repr():
    """Why UnknownTable is a ValueError and not a KeyError, pinned.

    `KeyError.__str__` re-`repr`s its argument, so this message -- written to be
    read in a Databricks run log -- would arrive wrapped in quotes with escaped
    newlines. Regressing the base class to KeyError makes this test fail rather
    than quietly degrading every operator-facing refusal."""
    with pytest.raises(UnknownTable) as excinfo:
        table_spec("estabelecimento")
    message = str(excinfo.value)
    assert not message.startswith(("'", '"')), f"message arrived repr-wrapped: {message}"
    assert message.startswith("unknown bronze table")
    # And it must not be swallowable by an `except KeyError` that never named it.
    assert not isinstance(excinfo.value, KeyError)


def test_a_spec_is_frozen():
    """Narrowed from a blind `Exception` (ruff B017): a bare `Exception` would
    also be satisfied by `table_spec` itself blowing up, so the test could stay
    green while proving nothing about frozen-ness. FrozenInstanceError is what a
    frozen dataclass raises and nothing else here does."""
    spec = table_spec("lookup")
    with pytest.raises(FrozenInstanceError):
        spec.staging = "something_else"  # type: ignore[misc]


def test_the_constraints_are_the_ones_the_live_tables_carry():
    assert table_spec("lookup").constraints == (
        "ALTER TABLE {table} ALTER COLUMN codigo SET NOT NULL",
        "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS codigo_not_blank",
        "ALTER TABLE {table} ADD CONSTRAINT codigo_not_blank "
        "CHECK (length(trim(codigo)) > 0)",
    )
    assert table_spec("estabelecimentos").constraints == (
        "ALTER TABLE {table} ALTER COLUMN cnpj_basico SET NOT NULL",
        "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS cnpj_basico_len8",
        "ALTER TABLE {table} ADD CONSTRAINT cnpj_basico_len8 "
        "CHECK (length(trim(cnpj_basico)) = 8)",
    )
