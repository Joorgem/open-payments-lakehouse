# tests/test_merchant_contract.py
"""`opl.contracts.merchant` -- the columns, the provenance split, and the four guards it
refuses at import with.

ITS OWN FILE, on `tests/test_ptax_contract.py`'s precedent: a contract module is DATA plus
import-time refusals, and the tests that pin it need no Spark session and no database.
`tests/test_postgres_source.py` is about the layer that BUILDS records; this is about what
a record is allowed to contain.

NOTHING HERE STARTS SPARK AND NOTHING HERE CONNECTS TO POSTGRES. The contract imports
nothing, which is asserted below over the AST rather than believed."""
from __future__ import annotations

import ast
import importlib
from dataclasses import replace
from pathlib import Path

import pytest

from opl.bronze.registry import REGISTRY
from opl.bronze.snapshot_axis import INSTANT_SNAPSHOT
from opl.contracts import merchant
from opl.contracts.catalogue import CONTRACT_COLUMNS, columns_for

# The fourteen columns, in the one order that decides the landed bytes. A GOLDEN COPY,
# written here by hand: derived from the module it checks it would assert nothing, and the
# order is what the landing writer's byte-identity refusal is taken over.
_COLUMNS = (
    "merchant_id",
    "cnpj",
    "legal_name",
    "trade_name",
    "status",
    "mcc",
    "settlement_account",
    "risk_tier",
    "credit_limit",
    "onboarded_on",
    "updated_at",
    "_snapshot_at",
    "_pg_snapshot",
    "_pg_wal_lsn",
)


def test_the_columns_are_exactly_these_in_this_order():
    """The order is authoritative for the emitted JSON's key order and therefore for its
    bytes, which the landing path refuses a difference in."""
    assert merchant.COLUMNS == _COLUMNS


def test_the_eleven_source_columns_are_the_ddl_and_the_three_stamps_are_ours():
    """The split the module documents itself by, pinned as two literals.

    `updated_at` is on the SOURCE side and `_snapshot_at` on the stamped side, and that is
    the pin that matters: they are both timestamps, they move together on a quiet table,
    and collapsing them keys the observation ledger on when a row last CHANGED rather than
    on when this lakehouse LOOKED."""
    assert merchant.SOURCE_COLUMNS == _COLUMNS[:11]
    assert merchant.OBSERVATION_COLUMNS == ("_snapshot_at", "_pg_snapshot", "_pg_wal_lsn")
    assert "updated_at" in merchant.SOURCE_COLUMNS
    assert "_snapshot_at" not in merchant.SOURCE_COLUMNS


def test_every_stamped_column_is_underscored_and_no_source_column_is():
    """The naming rule read off the live declaration, in both directions.

    A reader is told they can answer "where did this value come from?" from the name
    alone. That sentence is only true while this holds, and it is cheaper to assert than
    to remember."""
    assert all(column.startswith("_") for column in merchant.OBSERVATION_COLUMNS)
    assert not any(column.startswith("_") for column in merchant.SOURCE_COLUMNS)


def test_trade_name_is_the_one_nullable_column_and_it_is_not_required():
    """THE COLUMN THE PHASE NEEDS TO BE NULLABLE, and the reason is not tidiness.

    `rules._null_or_blank` treats `''` as blank, so a `trade_name` in `REQUIRED_COLUMNS`
    would reject every row whose trade name the source deliberately left empty --
    `scripts/merchant_population._trade_name` returns NULL, `''` and a name, on purpose,
    because a column that is only ever one of them cannot demonstrate that the landing
    path keeps NULL and `''` apart."""
    assert merchant.NULLABLE_COLUMNS == ("trade_name",)
    assert "trade_name" not in merchant.REQUIRED_COLUMNS
    assert set(merchant.REQUIRED_COLUMNS) | {"trade_name"} == set(merchant.COLUMNS)


def test_the_snapshot_axis_column_this_contract_declares_is_the_axis_the_ledger_reads():
    """The one place these two spellings meet.

    The contract imports nothing, so it cannot ask `opl.bronze.snapshot_axis` what the
    instant axis is called; `INSTANT_SNAPSHOT.column` and `SNAPSHOT_AT_COLUMN` are two
    literals of one string. The registry refuses the disagreement at import
    (`_assert_a_non_monthly_axis_is_a_contract_column`); this says the value they agree on
    is the one both sides were written for."""
    assert merchant.SNAPSHOT_AT_COLUMN == INSTANT_SNAPSHOT.column == "_snapshot_at"


def test_the_cnpj_is_fourteen_characters_and_its_root_is_eight():
    """Both are LENGTHS, never numbers: 142 of the 1,024 pinned counterparty roots carry a
    leading zero, so a numeric round trip destroys 13.9% of this pool in silence."""
    assert merchant.CNPJ_WIDTH == 14
    assert merchant.CNPJ_BASICO_WIDTH == 8


def test_no_value_domain_is_declared_for_the_three_categorical_columns():
    """`status`, `mcc` and `risk_tier` are domains that GAIN members.

    Declaring one here would put it in reach of a registry CHECK, and the payments entry
    already records what that costs: an append to the tuple becomes a MIGRATION on a live
    bronze table, i.e. a schema change made by a value edit."""
    declared = {name for name in vars(merchant) if name.isupper()}
    assert not declared & {"STATUSES", "RISK_TIERS", "MCCS", "STATUS_VALUES"}


def test_the_contract_module_imports_nothing_and_is_therefore_data():
    """THE REQUIREMENT, not a tidiness note: `opl.contracts.catalogue` joins this module
    into the mapping the registry reads, and the registry is imported by the extraction
    scripts, which run on a host where pyspark is usually absent.

    `from __future__ import annotations` is not an import of anything: it is a compiler
    directive that binds no module."""
    tree = ast.parse(Path(merchant.__file__).read_text(encoding="utf-8"))
    imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Import | ast.ImportFrom)
        and not (isinstance(node, ast.ImportFrom) and node.module == "__future__")
    ]
    assert not imports, (
        f"{merchant.__file__} imports {[ast.dump(node) for node in imports]}; it is a "
        "contract, and everything that reads it lives elsewhere"
    )


# --- the four import-time guards, each shown to be able to FAIL -----------------------
#
# `monkeypatch.setattr` then a direct call, which is how `test_ptax_contract.py` exercises
# the same shape: the guards read module globals, so patching one and calling the guard is
# the whole mechanism without re-importing anything.


def test_the_live_declaration_passes_all_four_of_its_own_guards():
    merchant._assert_every_column_is_declared_once()
    merchant._assert_the_provenance_split_is_a_partition()
    merchant._assert_the_names_agree_with_the_groups()
    merchant._assert_required_is_the_ddl_read_across()


def test_a_column_claimed_by_both_provenance_groups_is_refused(monkeypatch):
    """The edit this contract exists to refuse, at its smallest: `_snapshot_at` declared
    as a Postgres column as well as a stamp."""
    monkeypatch.setattr(
        merchant, "SOURCE_COLUMNS", merchant.SOURCE_COLUMNS + ("_snapshot_at",)
    )
    with pytest.raises(ValueError, match="BOTH read from Postgres and stamped"):
        merchant._assert_the_provenance_split_is_a_partition()


def test_a_column_in_neither_provenance_group_is_refused(monkeypatch):
    monkeypatch.setattr(merchant, "COLUMNS", merchant.COLUMNS + ("nobody_owns_me",))
    with pytest.raises(ValueError, match="without a provenance group"):
        merchant._assert_the_provenance_split_is_a_partition()


def test_a_stamped_column_named_like_a_source_column_is_refused(monkeypatch):
    """The partition alone is satisfied by ANY assignment; this is what makes the
    underscore rule readable rather than merely currently-true."""
    monkeypatch.setattr(merchant, "OBSERVATION_COLUMNS", ("snapshot_at",))
    with pytest.raises(ValueError, match="not named with the '_' prefix"):
        merchant._assert_the_names_agree_with_the_groups()


def test_a_postgres_column_named_like_a_stamp_is_refused(monkeypatch):
    monkeypatch.setattr(merchant, "SOURCE_COLUMNS", merchant.SOURCE_COLUMNS + ("_x",))
    with pytest.raises(ValueError, match="reserves for values it stamps"):
        merchant._assert_the_names_agree_with_the_groups()


def test_a_column_that_is_neither_required_nor_nullable_is_refused(monkeypatch):
    """A column in neither is a column the DQ gate never asks about -- the quiet
    direction, which is why the partition is asserted rather than the required set alone."""
    monkeypatch.setattr(merchant, "COLUMNS", merchant.COLUMNS + ("ungated",))
    with pytest.raises(ValueError, match="are in neither"):
        merchant._assert_required_is_the_ddl_read_across()


def test_a_nullable_column_that_is_also_required_is_refused(monkeypatch):
    monkeypatch.setattr(merchant, "NULLABLE_COLUMNS", ("trade_name", "status"))
    with pytest.raises(ValueError, match="are in both"):
        merchant._assert_required_is_the_ddl_read_across()


def test_a_repeated_column_name_is_refused(monkeypatch):
    monkeypatch.setattr(merchant, "COLUMNS", merchant.COLUMNS + ("cnpj",))
    with pytest.raises(ValueError, match="repeats a name"):
        merchant._assert_every_column_is_declared_once()


def test_an_empty_column_list_is_refused(monkeypatch):
    monkeypatch.setattr(merchant, "COLUMNS", ())
    with pytest.raises(ValueError, match="COLUMNS is empty"):
        merchant._assert_every_column_is_declared_once()


def test_the_module_still_imports_after_every_patch_above_is_undone():
    """`monkeypatch` restores globals, but the module object is shared across this whole
    session -- so this re-executes the real declaration and its guards from source, which
    is the only thing that says the patches above left nothing behind."""
    importlib.reload(merchant)
    assert merchant.COLUMNS == _COLUMNS


# --- the catalogue and the registry ---------------------------------------------------


def test_the_catalogue_carries_this_contract_and_its_columns():
    """The third of catalogue.py's three edits is a SEPARATELY-WRITTEN literal, not a
    derivation off `_SINGLE_CONTRACT_SOURCES`. This is what says it was made."""
    assert CONTRACT_COLUMNS[merchant.CONTRACT] == merchant.COLUMNS
    assert columns_for(merchant.CONTRACT) == merchant.COLUMNS


def test_the_registry_carries_exactly_the_names_this_contract_declares():
    """Every string in the registry entry is LIFTED from this module; `name` is the only
    literal there, because a registry key is that dict's own namespace."""
    spec = REGISTRY["merchant"]
    assert spec.contract == merchant.CONTRACT
    assert spec.table_key == merchant.BRONZE_TABLE_KEY
    assert spec.staging == merchant.BRONZE_STAGING_TABLE
    assert spec.bronze == merchant.BRONZE_TABLE
    assert spec.quarantine == merchant.BRONZE_QUARANTINE_TABLE
    assert spec.subdir == merchant.LANDING_SUBDIR


def test_the_three_roles_are_three_different_tables():
    """A staging name equal to the bronze name would make the promote append a table onto
    itself; a quarantine equal to either sends a triager to the wrong rows."""
    names = (
        merchant.BRONZE_STAGING_TABLE,
        merchant.BRONZE_TABLE,
        merchant.BRONZE_QUARANTINE_TABLE,
    )
    assert len(set(names)) == 3


def test_the_bronze_names_collide_with_nothing_another_table_owns():
    """The registry's own guards refuse this at import; asserting it here is what makes a
    green import mean the guard ran rather than that it was deleted."""
    others = [spec for spec in REGISTRY.values() if spec.contract != merchant.CONTRACT]
    claimed = {name for spec in others for name in (spec.staging, spec.bronze, spec.quarantine)}
    assert claimed.isdisjoint(
        {merchant.BRONZE_STAGING_TABLE, merchant.BRONZE_TABLE, merchant.BRONZE_QUARANTINE_TABLE}
    )
    assert merchant.LANDING_SUBDIR not in {spec.subdir for spec in others}


def test_a_non_monthly_axis_naming_a_column_its_contract_lacks_is_refused():
    """Guard the cross-check. `SNAPSHOT_AT_COLUMN` and `INSTANT_SNAPSHOT.column` are two
    literals of one string, and this is the guard that stops them drifting -- so it has to
    be shown able to fire."""
    from opl.bronze import registry as registry_module

    broken = dict(REGISTRY)
    broken["merchant"] = replace(
        REGISTRY["merchant"],
        snapshot_axis=replace(INSTANT_SNAPSHOT, column="_taken_at"),
    )
    with pytest.raises(ValueError, match="_taken_at"):
        registry_module._assert_a_non_monthly_axis_is_a_contract_column(broken)
