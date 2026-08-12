# src/opl/generator/cnpj_pool.py
"""The counterparty CNPJ pool: where the generator's real business keys come from, and
the one boundary at which "these are real CNPJs" is a checkable claim.

THE GENERATOR TAKES THE POOL AS AN INPUT AND NEVER FETCHES IT. That is an
architectural decision, not a data one, and it is worth stating why since the obvious
shape is the other one -- have the generator query `hub_empresa` and draw from what
comes back.

  - The generator's entire value is that it is a pure function of its arguments. A
    Spark dependency inside it would make every unit test need a session, would make
    the stream depend on a table's contents at the moment of the run, and would put
    "did this reproduce?" beyond the reach of any local test.
  - Smearing the fetch through the generator also smears the CLAIM. "The counterparty
    CNPJs are real" would then be a property of a query buried in a code path, asserted
    nowhere. Here it is a property of ONE function's input, so Task 4's measurement --
    100% resolution against `hub_empresa`, in the shape that measured 310,374/310,374
    for PJ partners -- is measuring exactly the thing this module produces. A rate
    below 100% is a finding, not a tolerance.

SO THIS MODULE BUILDS THE QUERY AND VALIDATES ITS RESULT, AND EXECUTES NOTHING. It
imports no pyspark and no databricks SDK; `pool_query` returns SQL TEXT and
`validated_pool` takes the values a caller already has. Running the query belongs to
whatever has a session -- a notebook, a job task, or F1b Task 4's evidence run.

WHY IT MUST NOT IMPORT `opl.vault.domains.cnpj`, WHICH OWNS THE NAMES BELOW.
`domains/cnpj.py` imports `opl.vault.observation`, which imports pyspark. Importing it
here would make the generator un-importable wherever pyspark is not installed -- which
is the extraction host, and is the same property `tests/bronze/test_registry.py::
test_the_registry_still_imports_where_pyspark_is_not_installed` holds down for the
bronze registry.

So `HUB_TABLE_NAME` and `KEY_COLUMN` below are a SECOND SPELLING of values that module
owns, and they are handled the way this repository handles a second spelling it cannot
remove: converted into a CROSS-CHECK rather than trusted. `tests/test_cnpj_pool.py`
asserts each against `opl.vault.domains.cnpj`, which a test may import because tests
run where pyspark exists. That is the same treatment `BronzeTable.prefix` gets against
`FILE_GROUPS`, and for the same reason -- a typo here is unique, passes every other
check, and silently queries a table that does not exist or a column that is not the
business key.
"""
from __future__ import annotations

from collections.abc import Iterable

from opl.config import DEFAULT

# Cross-checked against `opl.vault.domains.cnpj.HUB_EMPRESA.name` by the tests. The
# hub whose business key the payments integrate on: 69,062,849 keys in
# `workspace.default` as of F2 wave 1's run.
HUB_TABLE_NAME = "hub_empresa"

# Cross-checked against `opl.vault.domains.cnpj.HUB_EMPRESA.business_key_columns`.
KEY_COLUMN = "cnpj_basico"

# Cross-checked against `opl.vault.domains.cnpj.CNPJ_BASICO_WIDTH`. Measured 8/8
# characters over 69.1M company rows, so this is a shape the data already has rather
# than one imposed on it.
CNPJ_BASICO_WIDTH = 8

# A payer and a payee that are not the same company need two keys. Refused at the
# boundary rather than discovered as an infinite redraw inside the generator.
MINIMUM_POOL_SIZE = 2


def _reason_key_is_unusable(value: object) -> str | None:
    """Why `value` cannot be a counterparty key, or None if it can.

    THE THREE REFUSALS ARE THREE DIFFERENT FAILURES, not one shape check spelled three
    ways. A non-`str` is a caller handing over `Row` objects instead of the column. A
    wrong width is a query that selected the wrong column, or a `cnpj_completo`
    fourteen characters long that would resolve against nothing. A value that is not
    already stripped is the dangerous one: it has the right LENGTH only if the
    whitespace is counted, so it looks correct in a width check and joins to nothing
    on an exact-match key -- which is precisely the 100%-resolution claim failing
    silently."""
    if not isinstance(value, str):
        return f"is a {type(value).__name__}, not a str"
    if value != value.strip():
        return "carries leading or trailing whitespace, so it would join to nothing"
    if not value:
        return "is empty"
    if len(value) != CNPJ_BASICO_WIDTH:
        return f"is {len(value)} characters, not {CNPJ_BASICO_WIDTH}"
    return None


def validated_pool(values: Iterable[str]) -> tuple[str, ...]:
    """`values` as the canonical counterparty pool, or refuse and say which value.

    SORTED, so the pool is independent of the order the query's rows arrived in. The
    generator picks counterparties BY INDEX, so pool order decides which company gets
    which payment -- and a pool whose order came from Spark's row order would make the
    stream depend on a partitioning decision nobody controls. Sorting makes the same
    set of keys always produce the same stream, which is the reproducibility claim
    reaching one step further back than the generator itself.

    DUPLICATES ARE REFUSED, NOT COLLAPSED. `hub_empresa` is unique on its business key
    by construction, so a repeated value means the query is wrong -- a join that
    fanned out, or a source that is not the hub. Silently de-duplicating would turn
    that into a pool that is merely smaller and more biased than asked for, which is
    the kind of defect that surfaces three phases later as "why are 40% of payments to
    the same company?".

    NEVER NUMERIC, at any point: no `int()` round trip anywhere in this module.
    Alphanumeric CNPJs take effect 2026-07-31 and a numeric conversion loses a leading
    zero the moment one arrives."""
    pool = tuple(values)
    for position, value in enumerate(pool):
        reason = _reason_key_is_unusable(value)
        if reason is not None:
            raise ValueError(
                f"pool entry {position} ({value!r}) {reason}. Every counterparty key "
                f"is a {KEY_COLUMN} exactly as {HUB_TABLE_NAME} holds it, or the "
                "generated payments resolve against nothing and the integration is "
                "decoration."
            )
    if len(set(pool)) != len(pool):
        repeated = sorted({value for value in pool if pool.count(value) > 1})
        raise ValueError(
            f"the pool repeats {len(repeated)} key(s), e.g. {repeated[:3]}. "
            f"{HUB_TABLE_NAME} is unique on {KEY_COLUMN}, so a repeat means the query "
            "fanned out or read something that is not the hub -- refusing rather than "
            "de-duplicating, because a silently smaller pool is a silently more "
            "concentrated payment stream."
        )
    if len(pool) < MINIMUM_POOL_SIZE:
        raise ValueError(
            f"the pool holds {len(pool)} key(s); at least {MINIMUM_POOL_SIZE} are "
            "needed, because a payment has a payer and a payee and they are not the "
            "same company"
        )
    return tuple(sorted(pool))


def pool_query(*, size: int, seed: int) -> str:
    """The SQL that extracts a pool of `size` real keys, spread by `seed`.

    NOT EXECUTED HERE. Returned as text so that the caller with a session runs it and
    this module stays importable without one.

    ORDERED BY A HASH OF THE KEY, NOT BY THE KEY. `ORDER BY cnpj_basico LIMIT n` is
    deterministic too, and it returns a CONTIGUOUS BLOCK of the numerically lowest
    roots -- companies registered in the same era, in the same states, with correlated
    everything. Every one of them is real, so the resolution measurement would still
    read 100%, which is exactly why the weaker choice is tempting: the test that
    matters cannot see the difference. Hashing spreads the sample across the whole key
    space at the cost of one sort.

    THE SEED IS IN THE HASH, so two streams asking for different seeds get different
    companies rather than the same ones in a different order. It is interpolated into
    the SQL as a decimal integer and nothing else -- `int` is required by the type
    check below, so there is no string a caller could pass that reaches the statement.

    `sha2(..., 256)` rather than `rand()`: `rand()` without a seed is
    non-deterministic, `rand(seed)` is deterministic per partition layout rather than
    per row, and a partition layout is not something this project controls. A hash of
    the row's own key is a function of the row alone."""
    if isinstance(size, bool) or not isinstance(size, int) or size < MINIMUM_POOL_SIZE:
        raise ValueError(
            f"size must be an int of at least {MINIMUM_POOL_SIZE}, got {size!r}"
        )
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError(f"seed must be a non-negative int, got {seed!r}")
    return (
        f"SELECT {KEY_COLUMN} FROM {DEFAULT.table(HUB_TABLE_NAME)} "
        f"ORDER BY sha2(concat({KEY_COLUMN}, '{seed}'), 256) "
        f"LIMIT {size}"
    )
