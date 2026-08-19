"""The reconciliation, driven out of real tables rather than asserted as a string.

WHY THE VERDICTS ARE DRIVEN AND NOT MATCHED. A test that asserted the CASE expression
character by character would pass for a ladder whose arms nothing can enter, and this
project has now shipped four guards with exactly that defect -- an empty list that could
not have been non-empty, a hermetic fake answering from a dictionary the test supplied, a
`from_cache` key that never existed, and an expiry probe with no control. Every one
surfaced only when somebody asked whether the guard could fail. So the four verdicts here
are produced by running the shipped SQL over tables built to reach each arm, and the two
arms that the shipped ingestion flow cannot currently produce -- `over_promoted` and
`stranded_unexplained` -- are the ones this file is most careful to reach, because they
are the ones no workspace run will ever demonstrate.

WHY THE FIXTURE BUILDS ALL TWENTY-ONE OBJECTS. `batch_grain_sql` is total over `REGISTRY`,
so anything less would leave arms of the union unparsed, and the failure mode of an
unparsed arm is a view that refuses to be created at deploy time -- in the workspace,
after a commit, which is the most expensive place to learn it.

VIEWS, NOT DELTA TABLES. The reconciliation only ever reads `COUNT(*)` at a grain, so
what it reads needs rows and a schema and nothing else. Twenty-one Delta writes to prove
a GROUP BY parses would buy nothing and would put this module in the slow half of a suite
that already cannot be run end to end on this box.
"""
from __future__ import annotations

import pytest

from opl.bronze.reconcile import (
    BATCH_GRAIN_VIEW,
    FILE_GRAIN_VIEW,
    OVER_PROMOTED,
    RECONCILED,
    STRANDED_GATED,
    STRANDED_UNEXPLAINED,
    batch_grain_sql,
    create_view_ddl,
    file_accounts_sql,
    file_grain_sql,
)
from opl.bronze.registry import REGISTRY
from opl.config import OplConfig

_SCHEMA = "opl_reconcile_probe"
_CONFIG = OplConfig(catalog="spark_catalog", schema=_SCHEMA)

# (batch, file) pairs per table role, for the tables this module gives rows to. Every
# other registered table is built empty, which is also the state seven of them are in.
_BATCH_ROWS: dict[str, dict[str, list[tuple[str, str]]]] = {
    # staged = promoted + quarantined. The route does not matter: four CNPJ batches in
    # the live workspace reached this state through a repromote after the gate blocked
    # them, and they are finished.
    "lookup": {
        "staging": [("b1", "f1"), ("b1", "f1"), ("b1", "f1")],
        "bronze": [("b1", "f1"), ("b1", "f1")],
        "quarantine": [("b1", "f1")],
    },
    # The live defect: payments batch 592660596679630, 10,000 staged, 2,000 quarantined,
    # nothing promoted.
    "estabelecimentos": {
        "staging": [("b2", "f2"), ("b2", "f2"), ("b2", "f2")],
        "bronze": [],
        "quarantine": [("b2", "f2")],
    },
    # Rows missing with nothing rejected -- the shape a mid-stream `availableNow`
    # failure leaves. Never observed here: `ingest` is 29 for 29.
    "empresas": {
        "staging": [("b3", "f3"), ("b3", "f3")],
        "bronze": [],
        "quarantine": [],
    },
    # Bronze holds more than staging ever did. Unreachable through the shipped flow,
    # which is the reason it is a verdict rather than an assumption.
    "socios": {
        "staging": [("b4", "f4")],
        "bronze": [("b4", "f4"), ("b4", "f4")],
        "quarantine": [],
    },
    # Three files inside ONE reconciling batch, for the file grain: `fa` is fully
    # accounted for, `fb` has nothing in bronze, `fc` has a row in neither. At batch
    # grain this table reads 6/2/2 and is therefore NOT reconciled -- the point being
    # that a batch-grain verdict cannot see which file is safe to unlink.
    "payments": {
        "staging": [
            ("b5", "fa"), ("b5", "fa"),
            ("b5", "fb"), ("b5", "fb"),
            ("b5", "fc"), ("b5", "fc"),
        ],
        "bronze": [("b5", "fa"), ("b5", "fc")],
        "quarantine": [("b5", "fa"), ("b5", "fb")],
    },
}


def _rows_sql(rows: list[tuple[str, str]]) -> str:
    if not rows:
        return (
            "SELECT CAST(NULL AS STRING) AS _batch_id, CAST(NULL AS STRING) AS "
            "_source_file WHERE false"
        )
    values = ", ".join(f"('{batch}', '{file}')" for batch, file in rows)
    return f"SELECT * FROM VALUES {values} AS t(_batch_id, _source_file)"


@pytest.fixture(scope="module")
def probe(spark):
    """The whole registry, as views, in a schema this module owns and drops."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_SCHEMA}")
    for spec in REGISTRY.values():
        rows = _BATCH_ROWS.get(spec.name, {})
        for role, table in (
            ("staging", spec.staging),
            ("bronze", spec.bronze),
            ("quarantine", spec.quarantine),
        ):
            body = _rows_sql(rows.get(role, []))
            spark.sql(f"CREATE OR REPLACE VIEW {_CONFIG.table(table)} AS {body}")
    yield spark
    spark.sql(f"DROP DATABASE IF EXISTS {_SCHEMA} CASCADE")


def _by_source(spark, sql: str) -> dict:
    return {row["source"]: row for row in spark.sql(sql).collect()}


def test_the_four_verdicts_are_each_reachable(probe):
    """Every arm of the ladder, produced by the shipped SQL over real tables.

    The two that the ingestion flow cannot currently produce are here for the same
    reason the other two are: an arm that cannot be entered is a verdict that will never
    be wrong, and a reconciliation whose disagreement arm is unreachable agrees by
    construction."""
    rows = _by_source(probe, batch_grain_sql(_CONFIG))
    assert rows["lookup"]["verdict"] == RECONCILED
    assert rows["estabelecimentos"]["verdict"] == STRANDED_GATED
    assert rows["empresas"]["verdict"] == STRANDED_UNEXPLAINED
    assert rows["socios"]["verdict"] == OVER_PROMOTED


def test_the_unaccounted_count_is_the_rows_nothing_can_reach(probe):
    """The number an operator acts on: staged minus everything that landed somewhere."""
    rows = _by_source(probe, batch_grain_sql(_CONFIG))
    assert rows["estabelecimentos"]["unaccounted"] == 2
    assert rows["empresas"]["unaccounted"] == 2
    assert rows["lookup"]["unaccounted"] == 0
    assert rows["socios"]["unaccounted"] == -1


def test_the_remedy_is_printed_for_a_stranding_and_withheld_otherwise(probe):
    """A command beside the verdict, and NOT beside a batch that is already finished.

    `promote.require_batch_id`, `reclaim_landing._report_nothing_proven` and
    `backfill_prewrite.refuse_non_empty_quarantine` all print what resolves what they
    refused; a reconciliation that printed less would be below this repo's own standard.
    A remedy on a reconciled batch would be worse than none -- it is an invitation to
    re-promote a batch nothing is wrong with."""
    rows = _by_source(probe, batch_grain_sql(_CONFIG))
    stranded = rows["estabelecimentos"]["remedy"]
    assert "repromote_triaged_batch" in stranded
    # The table token is the REGISTRY key, which is what `table_spec` resolves and what
    # `repromote_batch_job.yml` takes -- not a second spelling of it.
    assert "table=estabelecimentos," in stranded
    assert "batch_id=b2," in stranded
    # Unexpanded on purpose: the guard's expected revision must come from the operator's
    # repository at launch, never from the wheel that created the view (ADR 0009).
    assert stranded.endswith("revision=$(git rev-parse HEAD)")
    assert rows["lookup"]["remedy"] is None
    assert rows["empresas"]["remedy"] is not None


def test_reclaimable_needs_every_row_of_the_file_accounted_for_and_one_in_bronze(probe):
    """`reclaim_landing`'s repaired safety proof, at the grain the delete happens at.

    `retention.files_of_batch` asked "does bronze hold a row of this file", which is
    equivalent to "every row" only under an all-or-nothing gate. The moment a batch
    reaches bronze through a repromote the two come apart: socios' 3,583 rejected rows
    span 20 distinct `_source_file` values whose clean rows ARE in bronze. `fa` below is
    that case and must stay reclaimable; `fc` is the case the old proof would have
    passed and this one refuses."""
    files = {
        row["source_file"]: row
        for row in probe.sql(file_grain_sql(_CONFIG)).collect()
        if row["source"] == "payments"
    }
    assert files["fa"]["reclaimable"] is True, "a rejected row IS an accounted-for row"
    assert files["fb"]["reclaimable"] is False, "nothing of this file is in bronze"
    assert files["fc"]["reclaimable"] is False, "one row of this file reached neither"
    assert files["fc"]["unaccounted"] == 1


def test_the_batch_grain_cannot_see_what_the_file_grain_decides(probe):
    """Why there are two views and not one.

    The payments batch below is stranded as a whole, and one of its three files is still
    safe to unlink. A reclaim keyed on the batch verdict would refuse a file it may
    remove; one keyed on "bronze holds a row" would remove a file it may not."""
    batch = _by_source(probe, batch_grain_sql(_CONFIG))["payments"]
    assert batch["verdict"] == STRANDED_GATED
    reclaimable = [
        row["source_file"]
        for row in probe.sql(file_grain_sql(_CONFIG)).collect()
        if row["source"] == "payments" and row["reclaimable"]
    ]
    assert reclaimable == ["fa"]


def test_every_registered_table_is_reconciled():
    """Total over `REGISTRY`, so a table registered later is reconciled that day.

    A hand-written list here would be the shape that left `reclaim_landing` wired into
    four jobs separately and missing from the fifth path entirely."""
    sql = batch_grain_sql()
    for spec in REGISTRY.values():
        for table in (spec.staging, spec.bronze, spec.quarantine):
            assert f".{table} " in f"{sql} ", f"{table} is in no arm of the union"
        assert f"'{spec.name}' AS source" in sql


def test_the_two_view_names_this_module_owns_are_prefixed_so_something_polices_them():
    """The `dataops_` prefix, which is the whole of what makes these names checkable.

    Free Edition ships one catalog and one schema, and the three collision guards range
    over the bronze, vault and gold registries -- so an object in none of them is checked
    by nothing at all. THE COLLISION LOCK ITSELF MOVED to
    `tests/dataops/test_views.py::test_no_dataops_view_collides_with_a_name_any_registry
    _owns` when F4 Task 4 added two more views: a lock over two of four is a lock that
    reports green about the half nobody added to it. What stays here is the naming rule,
    beside the module that names them."""
    assert BATCH_GRAIN_VIEW.startswith("dataops_")
    assert FILE_GRAIN_VIEW.startswith("dataops_")


def test_the_view_ddl_replaces_rather_than_skipping():
    """`CREATE OR REPLACE`, not `IF NOT EXISTS`.

    The opposite of the rule for a TABLE, and for the opposite reason: a table must not
    lose rows, a view has none, and `IF NOT EXISTS` on a view leaves an older wheel's
    definition standing while the run that was meant to replace it reports SUCCESS.

    Over `create_view_ddl` alone: the list of views it is applied to is
    `opl.dataops.views` now, and the four-view version of this assertion is beside it."""
    assert create_view_ddl("x", "SELECT 1", _CONFIG).startswith(
        f"CREATE OR REPLACE VIEW {_CONFIG.catalog}.{_SCHEMA}.x AS"
    )


def test_the_view_and_the_reclaim_decide_the_same_files(probe):
    """ONE predicate, executed twice, and the two answers compared.

    `file_accounts_sql` is what `retention.file_accounts_of_batch` runs on the
    DELETE path -- one table, one batch, no scan of the other twenty objects --
    while `file_grain_sql` is what an operator reads. If those could disagree, the
    dashboard would say `reclaimable = true` about a file the task refused, or
    worse, the other way round. `RECLAIMABLE_SQL` is the single spelling that stops
    it, and this asserts the two queries actually agree rather than that the
    constant appears in both.

    The batch is `b5`, whose three files are deliberately one of each: accounted
    for, nothing in bronze, and a row in neither."""
    spec = REGISTRY["payments"]
    narrow = {
        row["source_file"]: row["reclaimable"]
        for row in probe.sql(
            file_accounts_sql(spec, _CONFIG), args={"batch_id": "b5"}
        ).collect()
    }
    wide = {
        row["source_file"]: row["reclaimable"]
        for row in probe.sql(file_grain_sql(_CONFIG)).collect()
        if row["source"] == "payments" and row["batch_id"] == "b5"
    }

    assert narrow == wide
    assert narrow == {"fa": True, "fb": False, "fc": False}


def test_the_batch_id_is_bound_as_a_parameter_and_not_spliced_into_the_query(probe):
    """The one value in that query an operator types.

    `require_batch_id` refuses a blank and the sentinel and says nothing about
    quoting, and this query reaches Spark from a job task whose `batch_id` is a
    job parameter. Bound through `args=`, a batch id that is all punctuation
    matches nothing and returns nothing; spliced in, it would end the string. The
    probe below is the shape that closes the literal and appends a second
    statement -- it must be inert, not a syntax error and not a second query."""
    spec = REGISTRY["payments"]
    hostile = "b5' OR '1'='1"
    rows = probe.sql(
        file_accounts_sql(spec, _CONFIG), args={"batch_id": hostile}
    ).collect()

    assert rows == [], "a batch id that names nothing must match nothing"


def test_a_skipped_leg_leaves_its_count_at_zero_and_the_file_unreclaimable(probe):
    """`skip` exists so a reclaim can decide from whatever tables exist rather than
    raise on a quarantine that was never created. Every omission has to fail
    CLOSED, and the two that matter are asserted here: without staging there is no
    denominator, without bronze there is nothing persisted, and `fa` -- the one
    file that is otherwise reclaimable -- must go false in both."""
    spec = REGISTRY["payments"]
    for skipped in (("staging",), ("bronze",)):
        rows = {
            row["source_file"]: row["reclaimable"]
            for row in probe.sql(
                file_accounts_sql(spec, _CONFIG, skip=skipped), args={"batch_id": "b5"}
            ).collect()
        }
        assert rows["fa"] is False, f"skipping {skipped} must not admit a file"
    kept = {
        row["source_file"]: row["reclaimable"]
        for row in probe.sql(
            file_accounts_sql(spec, _CONFIG, skip=("quarantine",)),
            args={"batch_id": "b5"},
        ).collect()
    }
    # The one omission that is a true answer rather than a refusal: a table no
    # reject has ever reached has no quarantine, and `fb`'s two rows then look like
    # rows that reached nothing -- which is still a refusal.
    assert kept["fa"] is False and kept["fb"] is False
