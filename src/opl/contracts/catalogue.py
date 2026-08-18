# src/opl/contracts/catalogue.py
"""Every column contract this lakehouse ingests, in one mapping, keyed the way
`opl.bronze.registry.BronzeTable.contract` keys it.

WHY THIS EXISTS, and it is one of the four invariants F1b Task 0 named as binding
the payments registry entry. `registry._assert_contracts_exist` refuses AT IMPORT a
spec naming a contract that is not in `cnpj_schemas.TABLES` -- so registering a
SECOND SOURCE means the registry has to be able to ask a question spanning both, and
`cnpj_schemas` is the wrong place to answer it. That module's docstring says what it
is: the RFB's headerless column layouts, verified against the official layout PDF. A
generated payment stream is none of those things, and putting it in there would make
`FILE_GROUPS`, `CSV_DIALECT` and `CNPJ_KEY_COLUMNS` sit beside a contract none of
them describe.

SO THE CATALOGUE IS A JOIN, NOT A HOME. Each source keeps its own contract module
and this one only says which contracts exist and what columns each carries. Nothing
here declares a column: every value below comes from the module that owns it, so
there is no second spelling to drift.

IT IMPORTS NO SPARK, and must not. `opl.bronze.registry` imports this, and the
registry is imported by the extraction scripts, which run on a host where pyspark is
an optional extra that is usually not installed
(`tests/bronze/test_registry.py::test_the_registry_still_imports_where_pyspark_is_
not_installed` holds that down). Both sources' contract modules are pure data --
`cnpj_schemas` imports nothing at all and `opl.contracts.payments` is asserted to
import nothing by `tests/test_payment_contract.py` -- so this stays free.

TUPLES, WHERE `cnpj_schemas.columns_for` HANDS OUT A LIST. `TABLES` holds mutable
lists and `columns_for` copies one per call; a caller that keeps the result -- and
`rules._encoding_check` keeps it inside a closure that outlives the call, which is
why it already writes `tuple(...)` defensively -- would otherwise be one `.append`
away from silently changing which columns a rule set already handed to a running job
is checking. A tuple removes the question instead of documenting it.
"""
from __future__ import annotations

from opl.contracts import merchant, payments, ptax
from opl.contracts.cnpj_schemas import TABLES

# Every source that is NOT the RFB's own file layouts, as (contract key, owning module).
# A LIST RATHER THAN TWO NAMES IN A GUARD, because the guard below has to compare these
# against EACH OTHER as well as against the RFB half -- see it for what that fixes.
_SINGLE_CONTRACT_SOURCES = (
    (payments.CONTRACT, "opl.contracts.payments"),
    (ptax.CONTRACT, "opl.contracts.ptax"),
    # F-DB Task 4. The third non-RFB source, and the third whose key could collide with
    # either of the two above rather than only with the RFB half -- which is exactly the
    # blindness F-API Task 2 found in the guard below when there were two.
    (merchant.CONTRACT, "opl.contracts.merchant"),
)


def _assert_no_contract_is_declared_twice() -> None:
    """Fail at import if two sources claim one contract key.

    THE MERGE BELOW IS LAST-ONE-WINS, which is the whole reason this runs. A contract key
    that collided with another source's would silently REPLACE that contract's column
    list: `struct_for("empresas")` would build some other source's columns, the Auto
    Loader would read the RFB's semicolon CSV against them, and every row would arrive
    NULL or rescued. Nothing raises -- the dict is perfectly valid -- so the collision
    has to be refused where it is made.

    IT COMPARED ONLY THE PAYMENTS KEY AGAINST THE RFB HALF UNTIL F-API TASK 2, and that
    made it blind in the one direction this phase opened. With two single-contract
    sources, `{payments.CONTRACT} & set(TABLES)` says nothing about payments colliding
    with PTAX -- and those two are the pair most likely to collide, because they are the
    two written by this repository rather than by the Receita, and neither is constrained
    by an external file layout. The merge is still last-one-wins, so that collision was
    exactly as silent as the one the guard did catch.

    So the check is now total over the mapping's inputs: no non-RFB source may claim an
    RFB key, and no two non-RFB sources may claim the same key.

    This is also, verbatim, the assertion F1b Task 0 wrote as
    `test_payments_is_deliberately_not_registered_yet_and_the_refusal_says_so`'s first
    line (`CONTRACT not in TABLES`). That test's scope line is now spent -- Task 3
    registered the table -- and the property it opened with survives here as a guard
    rather than being deleted with it."""
    claimed: dict[str, str] = {contract: "opl.contracts.cnpj_schemas" for contract in TABLES}
    for contract, module in _SINGLE_CONTRACT_SOURCES:
        if contract in claimed:
            raise ValueError(
                f"contract {contract!r} is declared by BOTH {claimed[contract]} and "
                f"{module}. The mapping below is a last-one-wins merge, so a shared key "
                "silently replaces one source's column list with the other's -- the "
                "reader would then parse one source's bytes against the other's columns "
                "and produce a table of NULLs rather than an error."
            )
        claimed[contract] = module


_assert_no_contract_is_declared_twice()

# EVERY contract a bronze table may declare, and the only thing that decides whether
# `opl.bronze.registry` accepts one. Built from the owning modules rather than
# restated: the CNPJ half is `cnpj_schemas.TABLES`, the payments half is
# `opl.contracts.payments.COLUMNS`, whose ORDER is authoritative for the emitted JSON
# bytes and is pinned as a golden copy in `tests/test_payment_contract.py`, and the PTAX
# half is `opl.contracts.ptax.COLUMNS`, whose order is authoritative for the same reason.
CONTRACT_COLUMNS: dict[str, tuple[str, ...]] = {
    **{contract: tuple(columns) for contract, columns in TABLES.items()},
    payments.CONTRACT: tuple(payments.COLUMNS),
    ptax.CONTRACT: tuple(ptax.COLUMNS),
    # The merchant half is `opl.contracts.merchant.COLUMNS`, whose order is authoritative
    # for the same reason. THIS LINE IS A SEPARATE EDIT FROM THE TWO ABOVE and is not
    # derived from `_SINGLE_CONTRACT_SOURCES`: that tuple carries (key, module NAME) for a
    # message, not the module object, so there is nothing there to read a column list off.
    # Adding a source is therefore three edits to this file -- the import, the collision
    # list, and this literal -- and missing this one makes
    # `registry._assert_contracts_exist` refuse at import and takes the whole suite red,
    # which is the loud direction and is why it has not been "simplified" into a loop.
    merchant.CONTRACT: tuple(merchant.COLUMNS),
}


def columns_for(contract: str) -> tuple[str, ...]:
    """The ordered column tuple for `contract`. Raises KeyError if unknown.

    A bare KeyError, matching `cnpj_schemas.columns_for` and `rules.rules_for`, and
    NOT `registry.UnknownTable`: that exception's docstring describes an
    operator-supplied TABLE name arriving at a job boundary, and a contract key is
    neither operator-supplied nor a table. The registry's own import-time guard is
    what turns a bad contract into prose an operator reads."""
    return CONTRACT_COLUMNS[contract]


def is_known(contract: str) -> bool:
    """Does any source declare `contract`? The predicate `registry` asks.

    Public so the registry's guard can ask instead of carrying a second spelling of
    `contract in CONTRACT_COLUMNS` -- the same treatment `opl.config.is_month` gets,
    and for the same reason: two spellings of one rule is how `2026-13` came to be
    refused at two of four entry points."""
    return contract in CONTRACT_COLUMNS
