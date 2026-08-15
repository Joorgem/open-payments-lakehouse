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

from opl.bronze.snapshot_axis import INSTANT_SNAPSHOT, MONTHLY_SNAPSHOT
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


def test_a_grain_defaults_to_the_monthly_axis():
    """The field F-DB Task 2 added, and the reason nothing that existed had to change.

    Every grain in `opl/vault/domains/cnpj.py` is constructed without naming an axis, so
    this default is what keeps those three declarations byte-identical across the change
    that made the axis a source's to declare."""
    grain = ObservationGrain(
        name="x", bronze_table="b", quarantine_table="q", key_columns=("cnpj_basico",)
    )

    assert grain.snapshot_axis == MONTHLY_SNAPSHOT
    assert grain.snapshot_column == MONTH_COLUMN


def test_the_axis_refusal_reads_THIS_GRAINS_AXIS_AND_NOT_THE_DEFAULT_ONE():
    """THE GUARD THAT WOULD OTHERWISE HAVE STOPPED GUARDING IN SILENCE.

    `observation.py`'s refusal of an axis-in-the-business-key compared against a MODULE
    CONSTANT. Once the axis became the source's, that comparison keeps passing its own
    test -- the CNPJ grains are monthly, so `_snapshot_month` is still refused -- while
    admitting the one thing it exists to refuse for every other source: a grain keyed on
    its OWN axis. The result is the failure the message names, a complete ledger of
    nonsense with no error, reached through the guard rather than around it.

    Both directions are asserted, because only the pair distinguishes "reads the grain's
    axis" from "refuses both columns": the instant grain must REFUSE `_snapshot_at` and
    ACCEPT `_snapshot_month`, which is an ordinary payload column to a Postgres source
    and not an axis at all."""
    with pytest.raises(ValueError, match=INSTANT_SNAPSHOT.column):
        ObservationGrain(
            name="x", bronze_table="b", quarantine_table="q",
            key_columns=("merchant_id", INSTANT_SNAPSHOT.column),
            snapshot_axis=INSTANT_SNAPSHOT,
        )

    permitted = ObservationGrain(
        name="x", bronze_table="b", quarantine_table="q",
        key_columns=("merchant_id", MONTH_COLUMN),
        snapshot_axis=INSTANT_SNAPSHOT,
    )
    assert permitted.snapshot_column == INSTANT_SNAPSHOT.column


def test_an_axis_named_at_the_configured_schema_entry_point_reaches_the_grain():
    """`in_default_schema` is what both `grain_for` functions call, so an axis that did
    not survive that constructor would be declared on `BronzeTable`, dropped here, and
    default silently back to months -- with the ledger folding two same-month
    observations into one and reporting a departure as `observed`."""
    grain = ObservationGrain.in_default_schema(
        name="hub_merchant", bronze="bronze_merchant", quarantine="bronze_merchant_quarantine",
        key_columns=("merchant_id",), snapshot_axis=INSTANT_SNAPSHOT,
    )

    assert grain.snapshot_axis == INSTANT_SNAPSHOT
    assert grain.snapshot_column == "_snapshot_at"
