# tests/test_payment_contract.py
"""What the payment contract DECLARES, and the properties those declarations hold.

The contract is data. Its guards refuse a broken declaration at import, and this file
is what says the declarations are the ones intended -- the same split
`tests/bronze/test_registry.py` and `tests/bronze/test_registry_guards.py` make, in one
file because the contract is one module.

THE ORDER PIN IS NOT DECORATION. `COLUMNS` decides the key order of every emitted JSON
object, and the byte-identity property T1 rests on is stated over those bytes. A
reordering is a change to every file the generator has ever produced, so it is pinned
here as a golden copy rather than derived."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from opl.bronze.registry import REGISTRY, UnknownTable, table_spec
from opl.contracts import payments
from opl.contracts.cnpj_schemas import TABLES
from opl.contracts.payments import (
    AMOUNT_SCALE,
    BRONZE_QUARANTINE_TABLE,
    BRONZE_STAGING_TABLE,
    BRONZE_TABLE,
    BRONZE_TABLE_KEY,
    BUSINESS_ATTRIBUTE_COLUMNS,
    COLUMNS,
    CONTRACT,
    COUNTERPARTY_COLUMNS,
    CURRENCIES,
    DRIFT_COLUMN,
    DRIFT_COLUMNS,
    DRIFT_VALUES,
    EMITTED_AT_COLUMN,
    EVENT_TIME_COLUMN,
    IDENTITY_COLUMN,
    LANDING_SUBDIR,
    PAYMENT_METHODS,
    REQUIRED_COLUMNS,
    SCHEMA_VERSION,
)

_MODULE = Path(payments.__file__)


def test_the_columns_are_exactly_these_in_this_order():
    """Golden copy. Order is authoritative for the emitted bytes, not for parsing --
    JSON keys are named -- so a reorder is invisible to every consumer and visible to
    the byte-identity assertion, which is exactly the shape a golden pin exists for."""
    assert COLUMNS == (
        "transaction_id",
        "event_time",
        "emitted_at",
        "payer_cnpj_basico",
        "payee_cnpj_basico",
        "amount",
        "currency",
        "payment_method",
    )


def test_the_identity_is_the_event_and_is_not_one_of_its_attributes():
    """THE GRAIN, asserted rather than only written down: one row per payment event,
    `transaction_id` identifying the EVENT.

    The identity being outside `BUSINESS_ATTRIBUTE_COLUMNS` is what makes the T2
    distinction expressible at all. If it were an attribute, "same payment happening
    twice" and "same payment delivered twice" would be the same row shape and no query
    could tell them apart."""
    assert IDENTITY_COLUMN == "transaction_id"
    assert IDENTITY_COLUMN not in BUSINESS_ATTRIBUTE_COLUMNS


def test_neither_timestamp_is_a_business_attribute():
    """The one that keeps the T2 test from being vacuous.

    `event_time` advances per event, so including it in the attribute tuple would make
    every tuple unique by construction -- `DISTINCT` over the attributes and `DISTINCT`
    over `transaction_id` would agree, the T2 assertion would pass, and it would be
    measuring the clock rather than the repeats."""
    assert EVENT_TIME_COLUMN not in BUSINESS_ATTRIBUTE_COLUMNS
    assert EMITTED_AT_COLUMN not in BUSINESS_ATTRIBUTE_COLUMNS
    assert (EVENT_TIME_COLUMN, EMITTED_AT_COLUMN) == ("event_time", "emitted_at")


def test_the_counterparties_are_the_two_columns_task_4_measures():
    """100% resolution against `hub_empresa` is measured over exactly these."""
    assert COUNTERPARTY_COLUMNS == ("payer_cnpj_basico", "payee_cnpj_basico")
    assert set(COUNTERPARTY_COLUMNS) <= set(BUSINESS_ATTRIBUTE_COLUMNS)


def test_v1_declares_every_column_required_and_no_optional_one():
    """What makes "a NULL means the field was absent" true for the clean stream.

    THIS DOCSTRING USED TO PREDICT ITS OWN FAILURE and the prediction was wrong, which is
    recorded here rather than quietly overwritten. Task 0 wrote that F1b Task 2's drift
    would add an OPTIONAL column and that `REQUIRED_COLUMNS` would stop equalling
    `COLUMNS`. Task 2 landed and neither happened: a drift column the contract declares is
    not drift. `COLUMNS` is what the serialiser walks, so a column in it is emitted on
    every record; `REQUIRED_COLUMNS` is what a read schema is built from, so a column in it
    is absorbed instead of rescued. The drifted rows are rows written under a LATER version
    this lakehouse has not adopted, and the version bumps when it adopts one.

    So the equality still holds, and the two names are still separate -- see
    `test_the_drift_column_is_declared_but_not_by_v1` for what the contract knows about the
    column without declaring it."""
    assert SCHEMA_VERSION == 1
    assert REQUIRED_COLUMNS == COLUMNS
    assert DRIFT_COLUMN not in COLUMNS


def test_the_value_domains_are_declared_and_upper_case_ascii():
    """The generator picks BY INDEX into these tuples, so their contents and their
    order are both part of every stream ever generated. Upper-case ASCII with no
    spaces, matching how the RFB spells its own coded values."""
    assert CURRENCIES == ("BRL",)
    assert PAYMENT_METHODS == ("PIX", "TED", "BOLETO", "CARTAO_CREDITO", "CARTAO_DEBITO")
    for value in (*CURRENCIES, *PAYMENT_METHODS):
        assert value.isascii() and value == value.upper() and " " not in value
    assert AMOUNT_SCALE == 2


def test_the_drift_column_is_declared_but_not_by_v1():
    """THE NAME EXISTS ONCE AND BELONGS TO NO DECLARED TUPLE, which is the shape the drift
    class needs and the only shape that keeps it catchable.

    Named here because a string shared by a generator, a DQ gate and an evidence query must
    have exactly one spelling -- this repository has already paid for a quarantine name
    spelled twice. Absent from `COLUMNS`, `REQUIRED_COLUMNS` and
    `BUSINESS_ATTRIBUTE_COLUMNS` because a declared drift column is emitted on every
    record, absorbed by the read schema, and able to drop the pre-drift population out of a
    distinct count. The import-time guard is exercised in
    `tests/test_null_drop_trap.py`."""
    assert DRIFT_COLUMN == "payment_channel"
    assert DRIFT_COLUMNS == (DRIFT_COLUMN,)
    assert DRIFT_VALUES == ("MOBILE", "INTERNET_BANKING", "TERMINAL")
    for value in DRIFT_VALUES:
        assert value and value.isascii() and value == value.upper() and " " not in value
    declared = {*COLUMNS, *REQUIRED_COLUMNS, *BUSINESS_ATTRIBUTE_COLUMNS, *COUNTERPARTY_COLUMNS}
    assert not set(DRIFT_COLUMNS) & declared


def test_the_contract_module_imports_nothing_and_is_therefore_data():
    """`cnpj_schemas` imports nothing; so does this. Asserted over the AST rather than
    trusted, because the first thing a future editor reaches for is
    `from opl.vault.domains.cnpj import CNPJ_BASICO_WIDTH` -- and that module imports
    pyspark, which would make the contract un-importable on the extraction host and
    drag a Spark dependency into every consumer of a column list."""
    tree = ast.parse(_MODULE.read_text(encoding="utf-8"))
    imported = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Import | ast.ImportFrom)
        and not (isinstance(node, ast.ImportFrom) and node.module == "__future__")
    ]
    assert not imported, (
        f"{_MODULE.name} imports {[ast.dump(n) for n in imported]} -- the contract is "
        "DATA and every mechanism that reads it lives elsewhere"
    )


# --- the import-time guard -------------------------------------------------------------


def test_a_column_in_one_tuple_and_not_the_other_is_refused(monkeypatch):
    """The guard's whole reason: the serialiser walks `COLUMNS` and the repeat test
    walks `BUSINESS_ATTRIBUTE_COLUMNS`, and neither fails when they disagree."""
    monkeypatch.setattr(payments, "COLUMNS", COLUMNS + ("settlement_id",))
    with pytest.raises(ValueError, match="identity . timestamps . business attributes"):
        payments._assert_the_columns_partition_cleanly()


def test_a_repeated_column_name_is_refused(monkeypatch):
    """A repeated JSON key is last-one-wins, so the duplicate would not even be
    visible in the output that a reader would use to diagnose it."""
    monkeypatch.setattr(payments, "BUSINESS_ATTRIBUTE_COLUMNS", ("amount", "amount"))
    monkeypatch.setattr(
        payments,
        "COLUMNS",
        (IDENTITY_COLUMN, EVENT_TIME_COLUMN, EMITTED_AT_COLUMN, "amount", "amount"),
    )
    with pytest.raises(ValueError, match="repeats a name"):
        payments._assert_the_columns_partition_cleanly()


def test_a_counterparty_that_is_not_a_business_attribute_is_refused(monkeypatch):
    """Task 4 measures resolution over the counterparties and T2 counts distinct
    business attributes; a counterparty outside that tuple is measured by one and
    invisible to the other."""
    monkeypatch.setattr(payments, "COUNTERPARTY_COLUMNS", ("transaction_id",))
    with pytest.raises(ValueError, match="not a subset"):
        payments._assert_the_columns_partition_cleanly()


def test_the_live_declaration_passes_its_own_guard():
    """Guard the guard: every refusal above is monkeypatched, so without this the
    three tests would still pass if the live tuples were broken."""
    payments._assert_the_columns_partition_cleanly()


# --- the bronze naming triple ----------------------------------------------------------


def test_the_bronze_names_are_the_ones_declared_today():
    """These strings will name live Delta tables. Pinned per role, in the shape
    `tests/bronze/test_registry.py` pins the CNPJ ones, because the failure a
    uniqueness check cannot see is a SWAP between two roles of the same table."""
    assert BRONZE_STAGING_TABLE == "bronze_payments_staging"
    assert BRONZE_TABLE == "bronze_payments"
    assert BRONZE_QUARANTINE_TABLE == "bronze_payments_quarantine"
    assert BRONZE_TABLE_KEY == "bronze_payments"
    assert LANDING_SUBDIR == "payments"
    assert CONTRACT == "payments"


def test_the_three_roles_are_three_different_tables():
    """A quarantine that equals a staging name routes rejects into a table a promote
    reads -- the defect `opl.bronze.registry` exists to make impossible. `table_key`
    is allowed to equal `bronze`, exactly as `lookup`'s does: a checkpoint namespace
    and a Delta table are different kinds of name in different namespaces."""
    roles = (BRONZE_STAGING_TABLE, BRONZE_TABLE, BRONZE_QUARANTINE_TABLE)
    assert len(set(roles)) == 3


def test_the_bronze_names_collide_with_nothing_the_registry_already_owns():
    """The property that lets Task 3 lift this block into `REGISTRY` unchanged.

    Checked ACROSS roles, not within each, for `test_no_two_tables_share_a_staging_
    bronze_or_quarantine_name`'s reason: a payments quarantine that equalled a CNPJ
    staging name would route payment rejects into a table a CNPJ promote reads."""
    taken = {
        value
        for spec in REGISTRY.values()
        for value in (spec.staging, spec.bronze, spec.quarantine)
    }
    ours = {BRONZE_STAGING_TABLE, BRONZE_TABLE, BRONZE_QUARANTINE_TABLE}
    assert not (ours & taken), f"{sorted(ours & taken)} is already a registered name"
    assert BRONZE_TABLE_KEY not in {spec.table_key for spec in REGISTRY.values()}
    assert LANDING_SUBDIR not in {spec.subdir for spec in REGISTRY.values()}


def test_payments_is_deliberately_not_registered_yet_and_the_refusal_says_so():
    """THE SCOPE LINE OF F1b TASK 0, asserted so it cannot be crossed by accident.

    A key in `opl.bronze.REGISTRY` is bound by four invariants, and every one is
    satisfied by an artefact TASK 3 owns:

      1. `test_every_registered_table_has_an_ingestion_job` asserts
         `set(_JOB_OF) == set(REGISTRY)` -- a registered table needs a job YAML.
      2. `test_every_registered_table_has_a_rule_set` needs `rules_for(contract)` to
         yield the DQ gate's verdict on this source.
      3. `registry._assert_contracts_exist` refuses AT IMPORT a contract absent from
         `cnpj_schemas.TABLES`, so registering payments means giving the registry a
         contract catalogue spanning both sources.
      4. `registry._assert_prefixes_match_their_file_groups` refuses AT IMPORT a
         contract no `FILE_GROUPS` entry feeds -- and a GENERATED source will never
         have one, so that guard needs a landing mode beside `zips` and `local`.

    Registering it here would mean authoring Task 3's job resource and DQ rules inside
    Task 0, or breaking four green invariants. The names above are asserted
    collision-free against the live registry instead, so the lift is one insertion."""
    assert CONTRACT not in TABLES
    with pytest.raises(UnknownTable) as excinfo:
        table_spec(CONTRACT)
    assert "unknown bronze table 'payments'" in str(excinfo.value)
