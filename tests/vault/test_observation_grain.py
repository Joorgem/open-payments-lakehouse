"""`ObservationGrain` itself: what it refuses at CONSTRUCTION, before any table is
named and before Spark is asked anything.

SPLIT OUT OF `tests/vault/test_observation.py` BY F-DB TASK 1, at exactly 800 of this
project's 800-line cap, BEFORE F-DB Task 2 (T7) gave the grain a snapshot axis of its
own -- which it since has; this sentence said "still to give" and went stale inside the
same phase. The seam is the one the two halves already had and nobody had drawn: this
is the only part of that module that REFUSES AT CONSTRUCTION TIME -- before a session,
a table or a row exists. Nothing here touches a table, so this file starts no Spark
session and costs its `run_suite.sh` chunk one import.

THE FIRST DRAFT OF THIS PARAGRAPH SAID "the ONLY part with no `tables` fixture and no
`spark` in any signature", AND THAT WAS FALSE. Seven test functions in the pre-split
module took no arguments, not six: `test_the_five_state_values_are_pinned` also takes
none and reads no table, and it stayed -- it pins the ledger's state VOCABULARY, which
is the ledger's and not the grain's. So "everything left there reads four real Delta
tables" was false of it too. The seam is right; the sentence that described it was
measured wrong, and `run_suite.sh` leans on this paragraph to justify a chunk.

WHICH SIDE A TEST LANDS ON IS DECIDED BY WHAT MAKES IT CHANGE. `ObservationGrain` is a
frozen dataclass with a validating `__post_init__`, and the refusals here pin what that
validator refuses -- six at the split, NINE now, Task 2 having added the axis-aware ones
-- and they change when the GRAIN's shape changes. The ledger's own
argument refusals (`months=[]`, a bare string month, `2026-13`, a month no table
carries) stayed with the ledger: they are `observation_ledger`'s guards, three of them
run against real fixture data, and one of them is worth an eager Spark job on purpose.

AND THIS IS WHERE F-DB TASK 2's GRAIN WORK LANDED. T7 moved the snapshot axis onto
`BronzeTable` and made the refusal against keying a grain on its own axis -- pinned below
by `test_the_month_column_is_refused_as_a_business_key_column` -- grain-aware rather than
a comparison against a module constant. Its successor sits beside it:
`test_the_axis_refusal_reads_THIS_GRAINS_AXIS_AND_NOT_THE_DEFAULT_ONE`, which is the
assertion that the guard reads the grain's OWN axis, since a guard that silently stops
guarding is the failure that refusal exists to prevent.

WRITTEN IN THE PAST TENSE, AND THE LINE-NUMBER PIN IS GONE, both deliberately. This
paragraph described T7 as still to come while lines 5-6 above already recorded that it
had landed -- the second future-tense sentence in this docstring to go stale inside its
own phase, and the paragraph directly above says why prose that describes a seam is
exactly what gets measured wrong. It also pinned `observation.py:208-210`, which no
longer names the refusal it claimed to: a line range is a citation that rots on the next
edit to a file this one does not own, so the successor is named by TEST NAME, which
`grep` follows and a rename breaks loudly.

Both files' docstrings point at the other, because neither is a claim about the ledger
alone: a grain that constructs is not a grain that answers, and a ledger tested only
through valid grains has never been asked what an invalid one does."""
from __future__ import annotations

import pytest

from opl.bronze.snapshot_axis import INSTANT_SNAPSHOT, MONTHLY_SNAPSHOT
from opl.config import DEFAULT
from opl.vault.observation import MONTH_COLUMN, ObservationGrain
from opl.vault.specs import KeyPrefix


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


# --- the key prefixes, which F-DB Task 5's correction pass added ---------------------


def test_a_grain_declares_no_key_prefix_by_default():
    """The field that makes the ledger's KEY the value a derived link hashes, and the
    default that keeps every grain declared before it byte-identical: none of the three in
    `opl/vault/domains/cnpj.py` reads a key component through anything but its own name,
    so all three construct unchanged and answer unchanged."""
    grain = ObservationGrain(
        name="x", bronze_table="b", quarantine_table="q", key_columns=("cnpj_basico",)
    )

    assert grain.key_prefixes == ()


def test_a_key_prefix_on_a_column_the_grain_is_not_keyed_on_is_refused():
    """A prefix names the column it TRUNCATES, so one naming a column the grain does not
    key on applies to nothing -- and the ledger stays keyed on the raw value, which is the
    exact state the declaration was added to leave behind. Silent: the spec would read as
    though the derivation had taken effect."""
    with pytest.raises(ValueError, match="not among its key columns"):
        ObservationGrain(
            name="x", bronze_table="b", quarantine_table="q",
            key_columns=("merchant_id",),
            key_prefixes=(KeyPrefix(column="cnpj", width=8),),
        )


def test_two_key_prefixes_on_one_column_are_refused():
    """Only one can apply, and which one would depend on declaration order -- a grain
    whose VALUES depend on the order two lines were typed in."""
    with pytest.raises(ValueError, match="more than one key prefix"):
        ObservationGrain(
            name="x", bronze_table="b", quarantine_table="q",
            key_columns=("merchant_id", "cnpj"),
            key_prefixes=(
                KeyPrefix(column="cnpj", width=8), KeyPrefix(column="cnpj", width=14)
            ),
        )


def test_key_prefixes_reach_the_grain_through_the_configured_schema_entry_point():
    """`in_default_schema` is what `vault_load_effectivity.grain_for` calls, so a prefix
    dropped by that constructor would be declared on the link, checked against the link by
    `_refuse_a_mismatched_link_grain`, and then never applied to the source -- except that
    the check reads the grain, so the job would be refused rather than wrong. Asserted
    anyway: a refused deploy is not the outcome a correct declaration should have."""
    prefixes = (KeyPrefix(column="cnpj", width=8),)
    grain = ObservationGrain.in_default_schema(
        name="link_merchant_empresa", bronze="bronze_merchant",
        quarantine="bronze_merchant_quarantine",
        key_columns=("merchant_id", "cnpj"), key_prefixes=prefixes,
        snapshot_axis=INSTANT_SNAPSHOT,
    )

    assert grain.key_prefixes == prefixes
