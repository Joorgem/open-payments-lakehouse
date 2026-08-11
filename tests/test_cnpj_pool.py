# tests/test_cnpj_pool.py
"""The counterparty pool boundary: the one place "these CNPJs are real" is checkable.

THREE OF THESE TESTS ARE CROSS-CHECKS, NOT ASSERTIONS ABOUT THIS MODULE.
`opl.generator.cnpj_pool` cannot import `opl.vault.domains.cnpj` -- that module imports
`opl.vault.observation`, which imports pyspark, and the generator has to stay
importable where pyspark is not installed. So the hub's name, its business key and its
width are spelled a second time in the pool module, and a second spelling is handled
here the way this repository handles every one it cannot remove: converted into a
cross-check rather than trusted. A test may import pyspark; tests run where it exists.

That is the same treatment `BronzeTable.prefix` gets against `FILE_GROUPS`
(`registry._assert_prefixes_match_their_file_groups`) and for the same reason: a typo
in one of these is unique, passes every other check, and silently queries a table that
does not exist or a column that is not the business key."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from opl.config import DEFAULT
from opl.generator import cnpj_pool
from opl.generator.cnpj_pool import (
    CNPJ_BASICO_WIDTH,
    HUB_TABLE_NAME,
    KEY_COLUMN,
    MINIMUM_POOL_SIZE,
    pool_query,
    validated_pool,
)
from opl.vault.domains.cnpj import CNPJ_BASICO_WIDTH as VAULT_WIDTH
from opl.vault.domains.cnpj import HUB_EMPRESA

_MODULE = Path(cnpj_pool.__file__)


def _keys(count: int, *, start: int = 1) -> list[str]:
    """`count` well-formed eight-character keys. Never `str(n)`: the padding is the
    property half these tests are about."""
    return [f"{n:08d}" for n in range(start, start + count)]


# --- the cross-checks ------------------------------------------------------------------


def test_the_hub_is_the_one_the_vault_declares():
    """A typo here queries a table that does not exist, which fails loudly -- but a
    typo that names ANOTHER real table (`hub_estabelecimento`) does not: it returns
    eight-character-plus keys that pass every shape check and resolve against the
    wrong hub, so Task 4's 100% would be measured against a hub the payments were
    never drawn from."""
    assert HUB_TABLE_NAME == HUB_EMPRESA.name


def test_the_key_column_is_the_hubs_own_business_key():
    """`hub_empresa` is keyed on one column. Asserted against the hub's declaration
    rather than against the literal `"cnpj_basico"`, so a re-keying of the hub turns
    this red instead of leaving the pool query selecting a column that no longer is
    the key."""
    assert HUB_EMPRESA.business_key_columns == (KEY_COLUMN,)


def test_the_width_is_the_one_the_vault_declares():
    assert CNPJ_BASICO_WIDTH == VAULT_WIDTH == 8


def test_the_module_executes_nothing_and_imports_no_engine():
    """The architectural line, over the AST rather than by reading.

    The generator takes the pool as an INPUT. This module builds the query and
    validates the result; a `pyspark` or `databricks` import arriving here -- through
    any future edit, not just a literal `import pyspark` -- would put a session
    between the generator and its own unit tests and move the "these are real CNPJs"
    claim back inside the code path it exists to sit outside of."""
    tree = ast.parse(_MODULE.read_text(encoding="utf-8"))
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
    banned = [m for m in modules if m.split(".")[0] in {"pyspark", "databricks", "delta"}]
    assert not banned, f"{_MODULE.name} imports {banned}; it must execute nothing"


# --- the query -------------------------------------------------------------------------


def test_the_query_names_no_catalog_or_schema_of_its_own():
    """`opl.config.DEFAULT.table` is the only spelling of catalog and schema in this
    repository, and this query is the first SQL the generator side emits.

    COMMENT LINES ARE STRIPPED BEFORE THE SEARCH, for the reason
    `test_no_module_that_runs_on_databricks_asks_the_engine_to_cache` states about its
    own AST check: the module's comments EXPLAIN where the 69,062,849 keys live, so a
    text search that cannot tell a mention from a use would punish documenting the
    very thing it is guarding. `promote_batch`'s DDL lock strips comments the same
    way."""
    assert DEFAULT.table(HUB_TABLE_NAME) in pool_query(size=10, seed=1)
    code = "\n".join(
        line
        for line in _MODULE.read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("#")
    )
    assert f"{DEFAULT.catalog}." not in code, "the module spells a catalog of its own"


def test_the_query_orders_by_a_hash_of_the_key_and_not_by_the_key():
    """`ORDER BY cnpj_basico LIMIT n` is deterministic too, and returns a contiguous
    block of the numerically lowest roots -- companies registered in the same era with
    correlated everything. Every one is real, so the resolution measurement reads 100%
    either way; the test that matters cannot see the difference, which is exactly why
    the weaker choice has to be refused here."""
    statement = pool_query(size=10, seed=1)
    assert "ORDER BY sha2(concat(cnpj_basico, '1'), 256)" in statement
    assert "ORDER BY cnpj_basico" not in statement
    assert statement.endswith("LIMIT 10")


def test_the_seed_moves_the_sample():
    assert pool_query(size=10, seed=1) != pool_query(size=10, seed=2)
    assert pool_query(size=10, seed=1) == pool_query(size=10, seed=1)


@pytest.mark.parametrize("seed", ["1", 1.0, -1, True, None])
def test_a_seed_that_is_not_a_whole_number_is_refused(seed):
    """The seed is interpolated into the statement, so the type check is also the
    reason no string a caller passes can reach the SQL. `True` is refused explicitly
    because it IS an `int` in Python and would silently mean seed 1."""
    with pytest.raises(ValueError, match="seed must be"):
        pool_query(size=10, seed=seed)


@pytest.mark.parametrize("size", [0, 1, -5, "10", None])
def test_a_size_below_the_minimum_is_refused(size):
    with pytest.raises(ValueError, match="size must be"):
        pool_query(size=size, seed=1)


# --- the pool --------------------------------------------------------------------------


def test_a_pool_is_sorted_so_the_stream_does_not_depend_on_row_order():
    """Spark decides the order rows arrive in. The generator picks counterparties BY
    INDEX, so an unsorted pool would make the payment stream depend on a partitioning
    decision nobody controls -- reproducibility failing one step behind the seed."""
    forwards = validated_pool(_keys(5))
    backwards = validated_pool(list(reversed(_keys(5))))
    assert forwards == backwards == tuple(sorted(_keys(5)))


def test_a_padded_key_is_refused_even_though_its_length_looks_right():
    """THE DANGEROUS ONE. `' 1234567'` is eight characters, so a width check alone
    accepts it -- and it joins to nothing on an exact-match key, which is the
    100%-resolution claim failing silently rather than loudly."""
    with pytest.raises(ValueError, match="whitespace"):
        validated_pool([" 1234567", "00000002"])


def test_a_fourteen_character_cnpj_completo_is_refused():
    """The pool keys `hub_empresa` on its eight-character root. A CNPJ completo would
    resolve against `hub_estabelecimento` and against nothing here."""
    with pytest.raises(ValueError, match="14 characters, not 8"):
        validated_pool(["12345678000195", "00000002"])


def test_a_repeated_key_is_refused_rather_than_collapsed():
    """`hub_empresa` is unique on its business key, so a repeat means the query fanned
    out or read something that is not the hub. De-duplicating silently would leave a
    pool that is merely smaller and more concentrated than asked for -- a defect that
    surfaces three phases later as "why are 40% of payments to one company?"."""
    with pytest.raises(ValueError, match="repeats 1 key"):
        validated_pool(["00000001", "00000002", "00000001"])


def test_a_pool_too_small_for_a_payer_and_a_payee_is_refused():
    with pytest.raises(ValueError, match=f"at least {MINIMUM_POOL_SIZE}"):
        validated_pool(["00000001"])


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (12345678, "is a int, not a str"),
        (None, "is a NoneType, not a str"),
        ("", "is empty"),
        ("        ", "whitespace"),
        ("1234567", "is 7 characters, not 8"),
    ],
)
def test_an_unusable_key_is_refused_by_name_and_by_position(value, expected):
    """The message names the POSITION as well as the value: a pool arrives as tens of
    thousands of rows, and "one of them is wrong" is not an actionable refusal."""
    with pytest.raises(ValueError) as excinfo:
        validated_pool(["00000001", value, "00000003"])
    message = str(excinfo.value)
    assert expected in message
    assert "pool entry 1" in message


def test_an_alphanumeric_key_is_accepted():
    """Alphanumeric CNPJs take effect 2026-07-31. Nothing in this module converts to
    `int`, so the width check is the only rule and a letter passes it -- asserted so
    that a future "tidy-up" adding `value.isdigit()` turns red.

    The expected ORDER is `('00000002', '0000000A')` and that is not a typo: the pool
    sorts by code point, where `'2'` (U+0032) precedes `'A'` (U+0041). Worth pinning
    rather than writing as `tuple(sorted(...))`, because it is the first place the
    alphanumeric era changes an ordering somebody might have assumed was numeric."""
    assert validated_pool(["0000000A", "00000002"]) == ("00000002", "0000000A")
