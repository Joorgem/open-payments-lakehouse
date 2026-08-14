# tests/test_ptax_contract.py
"""What the PTAX contract DECLARES, and the properties those declarations hold.

Same shape as `tests/test_payment_contract.py`, and for the same reason: the contract is
data, its guards refuse a broken declaration at import, and this file is what says the
declarations are the ones intended.

THE COLUMN-PROVENANCE SPLIT IS THE SUBJECT HERE, where the payment contract's subject is
the identity/attribute split. Two tuples say where each column came from -- the request
this lakehouse made, or the response BCB sent -- and the naming asymmetry is what makes
that legible from a column name alone. The tests below check the rule holds in both
directions: nothing named in the API's alphabet is stamped by us, and nothing named in
ours is read out of the body.

THE API'S OWN FIELD NAMES ARE READ FROM `opl.extraction.ptax_source`, NOT RETYPED. That
module is where the response contract lives; a second spelling of `cotacaoVenda` in this
file would be exactly the drift the repository polices hardest, and it would go stale in
the direction that does not announce itself -- the contract column would stop
corresponding to the field it is named after, and every row would still land."""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from opl.bronze.registry import REGISTRY
from opl.contracts import catalogue, payments, ptax
from opl.contracts.catalogue import CONTRACT_COLUMNS
from opl.contracts.cnpj_schemas import TABLES
from opl.contracts.ptax import (
    BRONZE_QUARANTINE_TABLE,
    BRONZE_STAGING_TABLE,
    BRONZE_TABLE,
    BRONZE_TABLE_KEY,
    COLUMNS,
    COMPRA_COLUMN,
    CONTRACT,
    CURRENCY_COLUMN,
    LANDING_SUBDIR,
    PUBLISHED_AT_COLUMN,
    QUOTE_DATE_COLUMN,
    RATE_COLUMNS,
    REQUEST_COLUMNS,
    REQUIRED_COLUMNS,
    RESPONSE_COLUMNS,
    SCHEMA_VERSION,
    VENDA_COLUMN,
)
from opl.extraction import ptax_source

_MODULE = Path(ptax.__file__)


def test_the_columns_are_exactly_these_in_this_order():
    """Golden copy. Order is authoritative for the emitted bytes rather than for
    parsing -- JSON keys are named -- so a reorder is invisible to every consumer and
    visible only to a pin like this one and to the landing writer's byte comparison."""
    assert COLUMNS == (
        "quote_date",
        "currency",
        "data_hora_cotacao",
        "cotacao_compra",
        "cotacao_venda",
    )
    assert SCHEMA_VERSION == 1
    assert CONTRACT == "ptax"


def test_the_request_columns_are_stamped_by_us_and_the_response_columns_are_bcbs():
    """The split, pinned as the two tuples rather than inferred from the names."""
    assert REQUEST_COLUMNS == ("quote_date", "currency")
    assert RESPONSE_COLUMNS == ("data_hora_cotacao", "cotacao_compra", "cotacao_venda")
    assert RATE_COLUMNS == ("cotacao_compra", "cotacao_venda")


def _snake_case(field: str) -> str:
    """`cotacaoVenda` -> `cotacao_venda`. The one transformation this contract applies."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", field).lower()


def test_every_response_column_is_the_api_field_it_is_named_after():
    """THE CROSS-CHECK, and the reason the response half is not free to be renamed.

    `opl.extraction.ptax_source.RESPONSE_FIELDS` is the API's own three field names, read
    from the endpoint `scripts/validate_cnpj_snapshots.py` has carried since F0. Each
    contract column is that field snake_cased and otherwise untouched, which is what makes
    a landed column traceable back to the response field it came out of without a mapping
    table anywhere.

    Read rather than retyped: a literal `"cotacaoVenda"` in this file would be a second
    spelling of a value that module already owns, and the copy that goes stale is the one
    no request ever executes.

    Compared as SETS, and that is a decision rather than laziness: the two tuples are
    ordered by different authorities. `RESPONSE_FIELDS` orders the fields a response row
    must carry; `RESPONSE_COLUMNS` orders bytes in a landed record, where the publication
    instant leads because it is the column T3 reads. Requiring one order would be
    asserting that the extraction layer's tuple decides this contract's emitted bytes,
    which is exactly the coupling neither module has."""
    assert {_snake_case(field) for field in ptax_source.RESPONSE_FIELDS} == {
        PUBLISHED_AT_COLUMN,
        COMPRA_COLUMN,
        VENDA_COLUMN,
    }


def test_no_request_column_is_named_in_the_apis_alphabet():
    """The other half of the naming rule, which is what the asymmetry documents.

    A camelCase field name snake_cased carries an underscore between two Portuguese
    words; `quote_date` and `currency` are English and neither is a field the API sends.
    Asserted as membership in the response field set rather than by spelling, because the
    claim is about PROVENANCE, not about language: no column we stamp may be one BCB
    sends, or the reader can no longer tell which of the two put a value there."""
    api_fields = {_snake_case(field) for field in ptax_source.RESPONSE_FIELDS}
    assert not set(REQUEST_COLUMNS) & api_fields


def test_the_quote_date_is_carried_from_the_request_and_never_from_the_publication():
    """The single decision this whole module exists to hold, stated as a test.

    `PtaxQuote` carries both, and they are different fields: `quote_date` is what the
    request filtered on and `published_at` is the API's `dataHoraCotacao`. The contract
    keeps both columns for that reason -- collapse them and plan T3's instant rule becomes
    a calendar-day comparison that is right for every 2026 row and wrong in 1984."""
    fields = ptax_source.PtaxQuote.__dataclass_fields__
    assert "quote_date" in fields and "published_at" in fields
    assert QUOTE_DATE_COLUMN in REQUEST_COLUMNS
    assert PUBLISHED_AT_COLUMN in RESPONSE_COLUMNS
    assert QUOTE_DATE_COLUMN != PUBLISHED_AT_COLUMN


def test_v1_declares_every_column_required():
    """And it MATCHES the extraction layer rather than deciding independently.

    `ptax_source.quotes_in` refuses a response row missing any of the API's three fields,
    and it carries the request's two values onto every quote, so a record that reaches
    the landing writer cannot be missing a column. A gate looser than that would tolerate
    exactly the columns a bug between the two could empty."""
    assert REQUIRED_COLUMNS == COLUMNS
    assert {_snake_case(f) for f in ptax_source.RESPONSE_FIELDS} <= set(REQUIRED_COLUMNS)


def test_no_drift_column_is_declared_and_none_is_hidden_in_a_neighbour():
    """The absence, asserted, because an absence left to be inferred is indistinguishable
    from a forgotten declaration.

    `opl.contracts.payments` declares `DRIFT_COLUMN` because F1b must EXHIBIT drift and
    the payment stream is ours to shape. This source is BCB's: fabricating a field and
    stamping it with a `_record_source` that names the Banco Central would be a lie about
    a real institution in the one column that answers who produced a row.

    The second half is the one that would actually happen: the payments drift column
    leaking in through a paste. `payment_channel` is not a PTAX column and must not
    become one."""
    assert not hasattr(ptax, "DRIFT_COLUMN")
    assert not hasattr(ptax, "DRIFT_COLUMNS")
    assert not set(payments.DRIFT_COLUMNS) & set(COLUMNS)


def test_no_currency_domain_is_declared():
    """No `CURRENCIES` here, deliberately, and it is the same argument the payments
    registry entry makes for having no CHECK on its own currency column: a second
    currency must be a VALUE change rather than a schema change, and a declared domain
    invites a CHECK that would silently make it a migration on a live table.

    The currency the landed rows carry comes from knowing which endpoint was called --
    `ptax_source.QUOTED_CURRENCY`, which is source knowledge -- not from a domain here."""
    assert not hasattr(ptax, "CURRENCIES")
    assert CURRENCY_COLUMN in REQUEST_COLUMNS
    assert ptax_source.QUOTED_CURRENCY == "USD"


def test_the_contract_module_imports_nothing_and_is_therefore_data():
    """`cnpj_schemas` imports nothing, `payments` imports nothing, and so does this.

    Asserted over the AST rather than trusted, because the obvious edit is
    `from opl.extraction.ptax_source import RESPONSE_FIELDS` -- which would make the
    catalogue, and therefore `opl.bronze.registry`, import a module the extraction host
    has no reason to load, to derive five strings that are pinned by the test above
    instead."""
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


# --- the two import-time guards --------------------------------------------------------


def test_the_live_declaration_passes_both_of_its_own_guards():
    """Guard the guards: everything below refuses a synthesised declaration, and every
    one of those refusals would also be produced by a guard that refused everything.
    These run at import, so a false positive breaks every module that reads the
    catalogue -- including the extraction scripts that never touch Spark."""
    ptax._assert_every_column_is_declared_once()
    ptax._assert_the_provenance_split_is_a_partition()


def test_a_column_claimed_by_both_provenance_groups_is_refused(monkeypatch):
    """THE EDIT THIS MODULE EXISTS TO REFUSE: `quote_date` becoming a projection of
    `data_hora_cotacao`.

    It is the natural simplification -- the response already carries a date-looking
    string, and on every day this phase extracts the two agree -- and it passes every
    2026 fixture. What it destroys is plan T3's instant rule, which needs the quote's
    own date AND the instant its quote was published to be two values."""
    monkeypatch.setattr(ptax, "RESPONSE_COLUMNS", RESPONSE_COLUMNS + (QUOTE_DATE_COLUMN,))
    with pytest.raises(ValueError) as excinfo:
        ptax._assert_the_provenance_split_is_a_partition()
    message = str(excinfo.value)
    assert "quote_date" in message
    assert "PUBLICATION instant" in message
    assert "1984" in message


def test_a_column_in_neither_provenance_group_is_refused(monkeypatch):
    """The other direction, and it is what stops the split from quietly stopping to mean
    anything. A column whose provenance nobody stated makes the naming rule this contract
    documents itself with unreadable, and it is reachable precisely because `COLUMNS` is
    spelled out rather than derived from the two groups."""
    monkeypatch.setattr(ptax, "COLUMNS", COLUMNS + ("fx_rate",))
    with pytest.raises(ValueError, match="without a provenance group"):
        ptax._assert_the_provenance_split_is_a_partition()


def test_a_provenance_column_no_record_carries_is_refused(monkeypatch):
    """The same equality read the other way: a group naming a column `COLUMNS` does not
    carry is a claim about a field no landed row holds."""
    monkeypatch.setattr(ptax, "REQUEST_COLUMNS", REQUEST_COLUMNS + ("requested_at",))
    with pytest.raises(ValueError, match="without being a column"):
        ptax._assert_the_provenance_split_is_a_partition()


def test_a_rate_column_the_response_does_not_carry_is_refused(monkeypatch):
    """A rate this lakehouse COMPUTED, declared as one BCB published. It would reach the
    DQ gate's parse rules, which are stated about digits a bulletin carries."""
    monkeypatch.setattr(ptax, "RATE_COLUMNS", RATE_COLUMNS + (QUOTE_DATE_COLUMN,))
    with pytest.raises(ValueError, match="not carried by the response"):
        ptax._assert_the_provenance_split_is_a_partition()


def test_a_repeated_column_name_is_refused(monkeypatch):
    """A repeated JSON key is last-one-wins, so the duplicate would not even be visible
    in the output a reader would use to diagnose it."""
    monkeypatch.setattr(ptax, "COLUMNS", COLUMNS + (VENDA_COLUMN,))
    with pytest.raises(ValueError, match="repeats a name"):
        ptax._assert_every_column_is_declared_once()


def test_an_empty_column_list_is_refused(monkeypatch):
    """The one shape that passes every other check here and builds a read schema with no
    fields: every value would land in `_rescued_data` and the whole batch would be
    rejected, with the diagnosis starting from a quarantine rather than from this file."""
    monkeypatch.setattr(ptax, "COLUMNS", ())
    with pytest.raises(ValueError, match="COLUMNS is empty"):
        ptax._assert_every_column_is_declared_once()


# --- the catalogue, and the collision guard this phase widened -------------------------


def test_the_catalogue_carries_this_contract_and_its_columns():
    assert CONTRACT_COLUMNS[CONTRACT] == COLUMNS
    assert CONTRACT not in TABLES


def test_two_non_rfb_sources_claiming_one_key_are_refused(monkeypatch):
    """THE HOLE F-API TASK 2 CLOSED, exercised.

    The guard compared only the payments key against the RFB half, so payments colliding
    with PTAX -- the two sources written by this repository, neither constrained by an
    external file layout, and therefore the pair most likely to collide -- was invisible
    to it. `CONTRACT_COLUMNS` is a last-one-wins merge, so that collision would silently
    replace one source's column list with the other's: `struct_for("payments")` would
    build the PTAX columns, the Auto Loader would read JSON Lines of payment events
    against them, and every row would arrive rescued or NULL with nothing raising.

    Deleting the guard's body leaves this red and the RFB half green, which is what makes
    it a test of the widening rather than of the guard it replaced."""
    monkeypatch.setattr(
        catalogue,
        "_SINGLE_CONTRACT_SOURCES",
        (
            (payments.CONTRACT, "opl.contracts.payments"),
            (payments.CONTRACT, "opl.contracts.ptax"),
        ),
    )
    with pytest.raises(ValueError) as excinfo:
        catalogue._assert_no_contract_is_declared_twice()
    message = str(excinfo.value)
    assert "opl.contracts.payments" in message and "opl.contracts.ptax" in message
    assert "last-one-wins" in message


def test_a_non_rfb_source_claiming_an_rfb_key_is_still_refused(monkeypatch):
    """The half that already existed, kept exercised through the rewrite."""
    monkeypatch.setattr(
        catalogue, "_SINGLE_CONTRACT_SOURCES", (("empresas", "opl.contracts.ptax"),)
    )
    with pytest.raises(ValueError, match="opl.contracts.cnpj_schemas"):
        catalogue._assert_no_contract_is_declared_twice()


def test_the_live_catalogue_passes_its_own_guard():
    catalogue._assert_no_contract_is_declared_twice()


# --- the bronze names, and that nothing else owns them ---------------------------------


def test_the_bronze_names_are_the_ones_declared_today():
    assert BRONZE_TABLE_KEY == "bronze_ptax"
    assert BRONZE_STAGING_TABLE == "bronze_ptax_staging"
    assert BRONZE_TABLE == "bronze_ptax"
    assert BRONZE_QUARANTINE_TABLE == "bronze_ptax_quarantine"
    assert LANDING_SUBDIR == "ptax"


def test_the_three_roles_are_three_different_tables():
    """A staging that equalled its own quarantine would have the promote read rejected
    rows back as trusted input."""
    roles = (BRONZE_STAGING_TABLE, BRONZE_TABLE, BRONZE_QUARANTINE_TABLE)
    assert len(set(roles)) == 3


def test_the_registry_carries_exactly_the_names_this_contract_declares():
    """The lift, asserted as a lift. Every string in the registry entry comes from this
    module and `name` is the only literal there, so a value that drifted would be a value
    spelled twice -- which is the whole defect the registry exists to remove."""
    spec = REGISTRY["ptax"]
    assert (spec.contract, spec.table_key, spec.subdir) == (
        CONTRACT,
        BRONZE_TABLE_KEY,
        LANDING_SUBDIR,
    )
    assert (spec.staging, spec.bronze, spec.quarantine) == (
        BRONZE_STAGING_TABLE,
        BRONZE_TABLE,
        BRONZE_QUARANTINE_TABLE,
    )


def test_the_bronze_names_collide_with_nothing_another_table_owns():
    """The names are declared HERE and lifted into `opl.bronze.registry` by the entry
    that registers this table -- so they have to be collision-free against every table
    the registry already owns before that lift can be legal. The registry's own guards
    refuse the collision at import; this is the same statement made from the side that
    declares the strings, which is where a paste from the payments block would land."""
    taken = {
        value
        for spec in REGISTRY.values()
        if spec.contract != CONTRACT
        for value in (spec.staging, spec.bronze, spec.quarantine, spec.table_key)
    }
    mine = {BRONZE_STAGING_TABLE, BRONZE_TABLE, BRONZE_QUARANTINE_TABLE, BRONZE_TABLE_KEY}
    assert not (taken & mine), f"{sorted(taken & mine)} is already owned by another table"
    subdirs = {spec.subdir for spec in REGISTRY.values() if spec.contract != CONTRACT}
    assert LANDING_SUBDIR not in subdirs
