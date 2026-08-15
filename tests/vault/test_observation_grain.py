"""`ObservationGrain` itself: what it refuses at CONSTRUCTION, before any table is
named and before Spark is asked anything.

SPLIT OUT OF `tests/vault/test_observation.py` BY F-DB TASK 1, at exactly 800 of this
project's 800-line cap, with F-DB Task 2 (T7) still to give the grain a snapshot axis
of its own. The seam is the one the two halves already had and nobody had drawn: this
is the ONLY part of that module with no `tables` fixture and no `spark` in any
signature. Everything left there reads four real Delta tables through
`observation_ledger`; nothing here touches a table at all, so this file starts no
Spark session and costs its `run_suite.sh` chunk one import.

WHICH SIDE A TEST LANDS ON IS DECIDED BY WHAT MAKES IT CHANGE. `ObservationGrain` is a
frozen dataclass with a validating `__post_init__`, and these six pin what that
validator refuses -- they change when the GRAIN's shape changes. The ledger's own
argument refusals (`months=[]`, a bare string month, `2026-13`, a month no table
carries) stayed with the ledger: they are `observation_ledger`'s guards, three of them
run against real fixture data, and one of them is worth an eager Spark job on purpose.

AND THIS IS WHERE F-DB TASK 2's GRAIN WORK GOES. T7 moves the snapshot axis onto
`BronzeTable` and makes `observation.py:208-210` -- the refusal against keying a grain
on its own axis, pinned below by `test_the_month_column_is_refused_as_a_business_key
_column` -- grain-aware rather than a comparison against a module constant. A guard
that silently stops guarding is the failure that refusal exists to prevent, so its
successor belongs beside it, in the half of the split that has room.

Both files' docstrings point at the other, because neither is a claim about the ledger
alone: a grain that constructs is not a grain that answers, and a ledger tested only
through valid grains has never been asked what an invalid one does."""
from __future__ import annotations

import pytest

from opl.config import DEFAULT
from opl.vault.observation import MONTH_COLUMN, ObservationGrain


def test_a_grain_with_no_key_columns_is_refused():
    """A grain with no business key would group the whole table into one row and
    report it `observed`, which is a plausible-looking answer to a question nobody
    asked. Same family as `hash_key`'s empty-components refusal, and refused at
    construction so it never reaches Spark."""
    with pytest.raises(ValueError, match="at least one"):
        ObservationGrain(name="x", bronze_table="b", quarantine_table="q", key_columns=())


def test_a_bare_string_key_column_is_refused():
    """`key_columns="cnpj_basico"` is a `Sequence[str]` structurally, so a type
    checker cannot catch it, and iterating it yields the twelve CHARACTERS of the
    column name. The failure downstream would be an `AnalysisException` naming a
    column called `c`, which points nowhere near the mistake."""
    with pytest.raises(TypeError, match="bare str"):
        ObservationGrain(
            name="x", bronze_table="b", quarantine_table="q", key_columns="cnpj_basico"
        )


def test_the_month_column_is_refused_as_a_business_key_column():
    """`_snapshot_month` is the ledger's other axis. Naming it as part of the
    business key would make every key trivially present in exactly the month it
    names and absent in all the others -- a full ledger of nonsense, with no error."""
    with pytest.raises(ValueError, match=MONTH_COLUMN):
        ObservationGrain(
            name="x", bronze_table="b", quarantine_table="q",
            key_columns=("cnpj_basico", MONTH_COLUMN),
        )


def test_a_repeated_key_column_is_refused():
    with pytest.raises(ValueError, match="more than once"):
        ObservationGrain(
            name="x", bronze_table="b", quarantine_table="q",
            key_columns=("cnpj_basico", "cnpj_basico"),
        )


def test_the_key_columns_are_frozen_into_a_tuple():
    """A list handed in stays a list on a frozen dataclass -- mutable, unhashable,
    and shared with whatever the caller does to it next."""
    grain = ObservationGrain(
        name="x", bronze_table="b", quarantine_table="q", key_columns=["a", "b"]
    )

    assert grain.key_columns == ("a", "b")
    with pytest.raises(AttributeError):
        grain.name = "y"


def test_a_grain_can_be_built_against_the_configured_catalog_and_schema():
    """Table names come from `opl.config`, never from a literal in this layer."""
    grain = ObservationGrain.in_default_schema(
        name="hub_empresa", bronze="bronze_cnpj_empresas",
        quarantine="bronze_cnpj_empresas_quarantine", key_columns=("cnpj_basico",),
    )

    assert grain.bronze_table == DEFAULT.table("bronze_cnpj_empresas")
    assert grain.quarantine_table == DEFAULT.table("bronze_cnpj_empresas_quarantine")
