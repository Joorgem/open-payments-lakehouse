"""A landed file is deleted only when bronze PROVES it holds that file's rows.

The invariant is not decoration. F1.3 ingests incrementally -- several batches
per month -- so "delete the table's directory after promote" would destroy parts
that are landed but not yet ingested. The unit is the FILE, and the authority is
bronze, not staging: only what is provably persisted goes.

The proof set is read out of a Delta table, so it is DATA, and the second half of
this module is about what happens when that data names something it should not.
`_source_file` is not an operator parameter and cannot be typed wrong -- but it
was WRITTEN by a stream, and F1.3 proved empirically that a stream can discover
files nobody meant it to: a probe.txt planted in `zips/estabelecimentos/` was
ingested by a stream reading the month root. Rows like that carry a `_source_file`
under `zips/`, and the zips are the only way back to the source.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from opl.bronze.retention import (
    delete_files,
    fuse_path,
    scope_to_landing_dir,
)

_LANDING = "/Volumes/workspace/default/landing/cnpj/2026-06/estabelecimentos"
_ZIPS = "/Volumes/workspace/default/landing/cnpj/2026-06/zips/estabelecimentos"


def test_it_reports_deleted_absent_and_failed_apart(tmp_path):
    """Three outcomes, never collapsed into a boolean -- the same shape
    landing._discard_remote uses. An operator told nothing assumes the file is
    gone; a file that is STILL THERE holds Volume quota nobody knows about."""
    present = tmp_path / "a.ESTABELE"
    present.write_text("x")
    missing = tmp_path / "b.ESTABELE"
    a_dir = tmp_path / "c.ESTABELE"
    a_dir.mkdir()  # unlink on a directory raises -- stands in for a FUSE failure

    outcome = delete_files([str(present), str(missing), str(a_dir)])

    assert outcome.deleted == (str(present),)
    assert outcome.absent == (str(missing),)
    assert [path for path, _ in outcome.failed] == [str(a_dir)]
    assert not present.exists()
    assert a_dir.exists(), "a failed delete must leave the thing it could not remove"


def test_deleting_nothing_is_a_clean_no_op(tmp_path):
    outcome = delete_files([])
    assert outcome.deleted == () and outcome.absent == () and outcome.failed == ()


def test_it_never_raises_on_a_failed_delete(tmp_path):
    """Retention runs after a successful promote. A file it cannot remove must
    not turn a green ingestion red -- the data is safely in bronze either way."""
    a_dir = tmp_path / "d.ESTABELE"
    a_dir.mkdir()
    outcome = delete_files([str(a_dir)])
    assert len(outcome.failed) == 1


def test_deleting_an_already_absent_file_twice_stays_a_clean_re_run(tmp_path):
    """Idempotence, as the property and not as a side effect of the first test:
    a repair run re-executes this task with the same {{job.run_id}}, so every file
    it already reclaimed comes back as absent. That is a NORMAL outcome, so it
    must not be an error and must not be counted as work done either."""
    present = tmp_path / "e.ESTABELE"
    present.write_text("x")

    first = delete_files([str(present)])
    second = delete_files([str(present)])

    assert first.deleted == (str(present),) and first.absent == ()
    assert second.deleted == () and second.absent == (str(present),)
    assert second.failed == ()


def test_a_file_under_the_zips_dir_is_refused_rather_than_deleted():
    """The worst thing this code could delete, refused where it is decided.

    The zips are the ONLY way back to the source: F1.3's incidents 3 and 4 were
    parsing defects found AFTER ingestion and fixing them meant re-reading it.
    A bronze row whose `_source_file` sits under `zips/` is not hypothetical --
    a stream pointed at a shared root discovers files RECURSIVELY, which is how
    an F1.3 probe.txt in `zips/estabelecimentos/` reached the lookup staging
    table. Such a row would hand this reclaim a zip to delete, and the zip is
    inside the same month root as the file it is allowed to delete."""
    scope = scope_to_landing_dir(
        [f"{_LANDING}/K3241.K03200Y0.D60613.ESTABELE", f"{_ZIPS}/Estabelecimentos1.zip"],
        _LANDING,
    )

    assert scope.inside == (f"{_LANDING}/K3241.K03200Y0.D60613.ESTABELE",)
    assert scope.outside == (f"{_ZIPS}/Estabelecimentos1.zip",)


@pytest.mark.parametrize(
    "path",
    [
        "/Volumes/workspace/default/landing/cnpj/2026-06/lookups/F.K03200$Z.D60613.QUALSCSV",
        # BOTH state layouts, because both exist in the Volume: the month-scoped one
        # every ingest writes since F1.4b PR B Task 5 Step 0, and the pre-Step-0
        # unscoped one that still holds 2026-06's state and is deliberately orphaned
        # rather than migrated. A reclaim must stay out of either.
        "/Volumes/workspace/default/landing/_checkpoints/2026-06/bronze_cnpj_estab/offsets/0",
        "/Volumes/workspace/default/landing/_checkpoints/bronze_cnpj_estab/offsets/0",
        "/Volumes/workspace/default/landing/cnpj/2026-05/estabelecimentos/K3241.ESTABELE",
        f"{_LANDING}/../../2026-06/zips/estabelecimentos/Estabelecimentos1.zip",
        "s3://somewhere/else/K3241.ESTABELE",
        "",
    ],
)
def test_only_this_table_s_own_landing_dir_for_this_month_is_in_scope(path):
    """Everything else is refused, including forms this code does not understand.

    Another table's landing dir, the Auto Loader's own checkpoint state, LAST
    month's files (still landed, still un-reclaimed, and not what this batch
    proved), a path that climbs back out through `..`, and a URI scheme that is
    not the Volume at all. Fail-CLOSED: an unrecognised path is refused and
    reported, never resolved to "probably fine" and unlinked."""
    assert scope_to_landing_dir([path], _LANDING).outside == (path,)


def test_a_candidate_carrying_a_backslash_is_refused_whatever_posix_reads_it_as():
    """The containment check must not judge by one grammar and delete by another.

    `PurePosixPath` sees `..\\..\\zips\\x.zip` as ONE component with no `..` part,
    so it reads as INSIDE the landing dir -- and `delete_files` then hands it to
    `pathlib.Path`, which on Windows treats every one of those backslashes as a
    separator and traverses out. Unreachable in production (a POSIX FUSE mount
    never yields `\\`) but this is precisely why
    `registry._assert_subdirs_are_single_path_components` refuses BOTH separators
    rather than `os.sep`: a value that looks inert where it is written is a real
    separator where it runs, and a check that disagrees with its own executor is
    a bypass waiting for the reachability to change."""
    escape = f"{_LANDING}/..\\..\\zips\\estabelecimentos\\Estabelecimentos1.zip"
    assert scope_to_landing_dir([escape], _LANDING).outside == (escape,)


def test_the_dbfs_uri_form_resolves_to_the_fuse_path_it_names():
    """`_source_file` is `_metadata.file_path`, a URI, and the Databricks CLI is
    documented in this repo's F1.3 evidence as needing the `dbfs:/Volumes/...`
    form rather than a bare `/Volumes/...` -- so both forms are live in this
    workspace. Unresolved, the `dbfs:` form is a relative path that exists
    nowhere: every unlink would raise FileNotFoundError, every file would be
    reported ALREADY ABSENT, and the reclaim would report success having
    reclaimed nothing."""
    assert fuse_path(f"dbfs:{_LANDING}/K3241.ESTABELE") == f"{_LANDING}/K3241.ESTABELE"
    assert fuse_path(f"{_LANDING}/K3241.ESTABELE") == f"{_LANDING}/K3241.ESTABELE"
    scope = scope_to_landing_dir([f"dbfs:{_LANDING}/K3241.ESTABELE"], _LANDING)
    assert scope.inside == (f"{_LANDING}/K3241.ESTABELE",) and scope.outside == ()


# --- the repaired proof, driven out of real Delta tables ------------------------
#
# WHY REAL TABLES AND NOT A DOUBLE. `file_accounts_of_batch` runs
# `reconcile.RECLAIMABLE_SQL` through Spark, so a fake answering from a dict the
# test supplied would assert the test's own arithmetic and pass over a predicate
# that never executed. This repository has shipped four guards whose output could
# not distinguish "passed" from "never ran", and every one surfaced only when
# somebody asked whether the guard could fail -- so the three tables below are
# written as Delta and the verdicts come back out of the query.
#
# THE SPEC IS `estabelecimentos` because it is a `zips`-landed table, i.e. one the
# reclaim will actually act on, and because its three coordinates are resolved from
# the registry rather than named here -- the same resolution the task performs.

_MONTH = "2026-06"
_OTHER_MONTH = "2026-07"


@pytest.fixture
def tables(spark, tmp_path):
    """A throwaway Delta database and an `OplConfig` pointing every coordinate at it.

    Real managed tables under tmp_path rather than in the repo's warehouse, like
    tests/bronze/test_promote.py's `tables`."""
    from opl.config import OplConfig

    db = f"retention_{uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE {db} LOCATION '{tmp_path.as_uri()}'")
    yield OplConfig(catalog="spark_catalog", schema=db)
    spark.sql(f"DROP DATABASE {db} CASCADE")


def _write(spark, config, table, rows, month=_MONTH):
    """One role's rows: (batch, file) pairs, stamped with a snapshot month.

    append, not overwrite: the table does not exist yet, and an overwrite of a
    not-yet-existing managed Delta table inside a database with an explicit
    LOCATION fails with "does not support truncate in batch mode" (measured on
    pyspark 3.5.9 / delta-spark 3.3.1, this repo's pins)."""
    spark.createDataFrame(
        [(batch, source_file, month) for batch, source_file in rows],
        "_batch_id string, _source_file string, _snapshot_month string",
    ).write.format("delta").mode("append").saveAsTable(config.table(table))


@pytest.fixture
def repromoted(spark, tables):
    """ONE batch of four files, in the state a REPROMOTE leaves behind.

    This is the shape that broke the old proof, and every file is a real case:

      f_clean       staged 2, bronze 2, quarantine 0 -- nothing was rejected.
      f_rejected    staged 3, bronze 2, quarantine 1 -- THE repromote case, and the
                    one the whole repair is about. `promote_batch` re-applies the DQ
                    rules and appends only the passing rows, so a file with one
                    rejected row has its clean rows in bronze and that row only in
                    quarantine. Measured shape: socios' 3,583 rejects span 20
                    distinct `_source_file` values exactly like this.
      f_stranded    staged 2, bronze 1, quarantine 0 -- one row reached NEITHER.
                    `files_of_batch` returned this file, because bronze holds a row
                    of it; unlinking it destroys the only copy of a row nothing has.
      f_quarantined staged 1, bronze 0, quarantine 1 -- fully accounted for and
                    nothing persisted. The equation alone would admit it.
    """
    from opl.bronze.registry import table_spec

    spec = table_spec("estabelecimentos")
    batch = "batch-repromoted"
    _write(spark, tables, spec.staging, [
        (batch, "f_clean"), (batch, "f_clean"),
        (batch, "f_rejected"), (batch, "f_rejected"), (batch, "f_rejected"),
        (batch, "f_stranded"), (batch, "f_stranded"),
        (batch, "f_quarantined"),
        # A second batch, whose files must not appear in the first batch's accounts.
        ("batch-other", "f_other"),
    ])
    _write(spark, tables, spec.bronze, [
        (batch, "f_clean"), (batch, "f_clean"),
        (batch, "f_rejected"), (batch, "f_rejected"),
        (batch, "f_stranded"),
        ("batch-other", "f_other"),
    ])
    _write(spark, tables, spec.quarantine, [
        (batch, "f_rejected"),
        (batch, "f_quarantined"),
    ])
    return spec, batch


def _accounts(spark, spec, batch, config):
    from opl.bronze.retention import file_accounts_of_batch

    return {
        account.source_file: account
        for account in file_accounts_of_batch(spark, spec, batch, config=config)
    }


def test_a_file_with_a_rejected_row_is_reclaimed_only_if_its_own_rows_reconcile(
        spark, tables, repromoted):
    """THE DEMANDED TEST, and the whole reason `files_of_batch` was replaced.

    `f_rejected` must GO: a rejected row is an accounted-for row, it sits in
    quarantine by design, and refusing this file would make the repair a
    relaxation of the reclaim rather than of nothing. `f_stranded` must STAY: the
    old proof passed it, because bronze holds a row of it, and one of its rows is
    in no table any count reads.

    The two differ ONLY in where the missing row went, which is exactly the
    distinction "does bronze hold a row of this file" cannot make."""
    spec, batch = repromoted
    accounts = _accounts(spark, spec, batch, tables)

    assert accounts["f_rejected"].reclaimable is True
    assert (accounts["f_rejected"].staged, accounts["f_rejected"].promoted,
            accounts["f_rejected"].quarantined) == (3, 2, 1)
    assert accounts["f_stranded"].reclaimable is False
    assert accounts["f_stranded"].unaccounted == 1
    assert accounts["f_clean"].reclaimable is True


def test_a_file_whose_rows_are_all_rejected_is_refused_though_it_reconciles(
        spark, tables, repromoted):
    """The conjunct that is not redundant. `promoted + quarantined = staged` holds
    for `f_quarantined` with promoted = 0, and nothing of that file is in the
    system of record. The quarantine is not persistence: it is where rows go
    BECAUSE they were not stored, so unlinking on its strength alone would delete
    the last copy of every row of the file."""
    spec, batch = repromoted
    account = _accounts(spark, spec, batch, tables)["f_quarantined"]

    assert account.unaccounted == 0, "this file's rows all reached a table"
    assert account.promoted == 0
    assert account.reclaimable is False


def _old_proof(spark, spec, batch, config) -> set[str]:
    """`files_of_batch` as it SHIPPED, re-executed here rather than reconstructed.

    The deleted function's whole body was this query, and running it is what makes
    the comparison below a cross-implementation one. Reading `promoted > 0` out of
    the new query's own output would have compared the repaired proof against a
    projection of itself: a bronze leg scoped to the wrong batch, or joined at the
    wrong grain, moves BOTH sides together and the implication still holds
    vacuously. This side reads bronze directly and knows nothing about staging,
    the quarantine or `RECLAIMABLE_SQL`."""
    from pyspark.sql.functions import col

    from opl.bronze.autoloader import SOURCE_FILE_COLUMN
    from opl.bronze.promote import BATCH_COLUMN

    return {
        row[SOURCE_FILE_COLUMN]
        for row in spark.read.table(config.table(spec.bronze))
        .filter(col(BATCH_COLUMN) == batch)
        .select(SOURCE_FILE_COLUMN)
        .distinct()
        .collect()
    }


def test_the_repaired_proof_admits_only_files_the_old_one_admitted(
        spark, tables, repromoted):
    """THE IMPLICATION, asserted rather than argued in a docstring.

    The old proof was `DISTINCT _source_file FROM <bronze> WHERE _batch_id = ...`,
    which is exactly `promoted > 0`. That is the FIRST CONJUNCT of the repaired
    one, so the new delete set is a SUBSET of the old: this control can only ever
    unlink fewer files than it did before, never one more. A repair to a retention
    guard that could admit a file the previous version refused would need an
    argument of its own, and this asserts there is none to make.

    The old side is COMPUTED by `_old_proof`, which runs the shipped query, rather
    than read back out of the new one -- see there for what that would have made
    this assertion worth."""
    spec, batch = repromoted
    accounts = _accounts(spark, spec, batch, tables)
    old_proof = _old_proof(spark, spec, batch, tables)
    repaired = {name for name, account in accounts.items() if account.reclaimable}

    assert repaired < old_proof, "the repaired proof must be strictly stronger here"
    assert old_proof == {"f_clean", "f_rejected", "f_stranded"}
    assert repaired == {"f_clean", "f_rejected"}


def test_a_row_that_names_no_file_is_dropped_and_the_drop_is_printed(
        spark, tables, repromoted, capsys):
    """THE SILENT DROP. A NULL `_source_file` names no path, so it can be neither
    unlinked nor evidence about a file and it is dropped -- but the task's five
    counters (deleted, already_absent, failed, refused, held_back) total over what
    SURVIVED the drop, so those rows leave every printed number balanced and
    appeared nowhere at all.

    The rows below are written to look RECLAIMABLE on purpose -- two staged, two
    promoted, so `promoted > 0 AND promoted + quarantined = staged` holds for their
    group. That makes the drop, and not the predicate, the thing that keeps them out
    of the delete list, which is exactly the case a reader has no way to see without
    the line this asserts."""
    spec, batch = repromoted
    _write(spark, tables, spec.staging, [(batch, None), (batch, None)])
    _write(spark, tables, spec.bronze, [(batch, None), (batch, None)])

    accounts = _accounts(spark, spec, batch, tables)
    out = capsys.readouterr().out

    assert set(accounts) == {"f_clean", "f_quarantined", "f_rejected", "f_stranded"}
    assert "DROPPED (names no file)" in out
    assert "2 staged, 2 promoted and 0 quarantined" in out
    assert batch in out
    # And the four real files are decided exactly as they were before those rows
    # existed: SQL keys NULL as a group of its own, so it takes nothing from theirs.
    assert accounts["f_clean"].reclaimable is True
    assert accounts["f_stranded"].reclaimable is False


def test_a_file_that_lost_the_column_on_one_row_is_held_back_rather_than_deleted(
        spark, tables):
    """THE FAIL-CLOSED HALF, verified rather than asserted in prose.

    A row whose `_source_file` went missing between staging and bronze does not
    merely vanish from the report -- it leaves the NAMED group short by one leg, so
    that group's `promoted + quarantined = staged` breaks and the file is held back.
    The drop can therefore never be the reason a file is unlinked: in every
    arrangement of the three tables it either changes nothing or refuses more."""
    from opl.bronze.registry import table_spec

    spec = table_spec("estabelecimentos")
    _write(spark, tables, spec.staging, [("b", "f_split"), ("b", "f_split")])
    _write(spark, tables, spec.bronze, [("b", "f_split"), ("b", None)])

    account = _accounts(spark, spec, "b", tables)["f_split"]

    assert (account.staged, account.promoted) == (2, 1)
    assert account.unaccounted == 1
    assert account.reclaimable is False


def test_the_accounts_are_scoped_to_the_batch_they_were_asked_about(
        spark, tables, repromoted):
    """A second batch's files are another batch's proof, and deleting them on this
    batch's behalf is the F1.3 defect the batch scoping exists for."""
    spec, batch = repromoted

    assert "f_other" not in _accounts(spark, spec, batch, tables)
    assert "f_other" in _accounts(spark, spec, "batch-other", tables)


def test_a_missing_quarantine_table_reads_as_no_rejects_rather_than_raising(
        spark, tables):
    """Three quarantines in this workspace hold zero rows and one of them was
    dropped and recreated; a table no reject has ever reached may not exist at all.
    Omitting the leg is the true answer (`quarantined = 0`) AND fail-closed if the
    table was dropped while holding rows -- the batch then stops reconciling and
    every file of it is refused."""
    from opl.bronze.registry import table_spec

    spec = table_spec("estabelecimentos")
    _write(spark, tables, spec.staging, [("b", "f"), ("b", "f")])
    _write(spark, tables, spec.bronze, [("b", "f"), ("b", "f")])

    accounts = _accounts(spark, spec, "b", tables)
    assert accounts["f"].quarantined == 0 and accounts["f"].reclaimable is True


def test_a_missing_staging_table_refuses_every_file_rather_than_admitting_it(
        spark, tables):
    """The direction that matters. With staging gone the denominator is gone, so
    the equation cannot hold and nothing is unlinked -- where the old proof, which
    never read staging, would have returned every file bronze held."""
    from opl.bronze.registry import table_spec

    spec = table_spec("estabelecimentos")
    _write(spark, tables, spec.bronze, [("b", "f")])

    accounts = _accounts(spark, spec, "b", tables)
    assert accounts["f"].promoted == 1, "the old proof would have named this file"
    assert accounts["f"].reclaimable is False


def test_a_batch_that_touched_nothing_has_no_accounts(spark, tables, repromoted):
    """The flow's legitimate no-op: an ingest that found no new file. Empty, not an
    error -- the job task is what tells that apart from a typed batch id."""
    spec, _ = repromoted
    assert _accounts(spark, spec, "batch-that-never-was", tables) == {}


# --- the month, which is the OTHER half of the delete boundary -------------------


def test_the_month_is_the_one_the_ingest_stamped_on_this_batch(spark, tables, repromoted):
    """Derived from `_snapshot_month`, not from the `_source_file` paths it is used
    to judge and not from `opl.config`'s pinned month. The stamp is the ingest run's
    own validated `{{job.parameters.month}}`, which is what makes it the same month
    whose landing dir the files were read out of."""
    from opl.bronze.retention import months_of_batch

    spec, batch = repromoted
    assert months_of_batch(spark, tables.table(spec.staging), batch) == (_MONTH,)


def test_a_batch_naming_two_months_is_reported_as_two_and_not_collapsed(
        spark, tables, repromoted):
    """One ingest run takes ONE month, so this cannot happen -- which is why it must
    not be answered with a `first()`. A tuple is what lets the caller refuse; a
    string would have picked a delete boundary out of a state nobody can explain."""
    from opl.bronze.retention import months_of_batch

    spec, batch = repromoted
    _write(spark, tables, spec.staging, [(batch, "f_clean")], month=_OTHER_MONTH)

    assert months_of_batch(spark, tables.table(spec.staging), batch) == (
        _MONTH, _OTHER_MONTH,
    )


def test_a_staging_shape_without_the_column_derives_nothing_rather_than_guessing(
        spark, tables):
    """The documented rebuild leaves pre-F1.4a staging shapes in place, and estab
    staging was the 35-column one until 2026-08-03. Empty is the honest answer and
    the caller refuses on it; a `DEFAULT.month` fallback here is precisely the
    substitution `require_month` exists to refuse, one layer further down."""
    from opl.bronze.registry import table_spec
    from opl.bronze.retention import months_of_batch

    spec = table_spec("estabelecimentos")
    staging = tables.table(spec.staging)
    spark.createDataFrame(
        [("b", "f")], "_batch_id string, _source_file string",
    ).write.format("delta").mode("append").saveAsTable(staging)

    assert months_of_batch(spark, staging, "b") == ()
    assert months_of_batch(spark, "no_such_db.no_such_table", "b") == ()


def test_a_wrong_month_puts_every_proven_file_outside_the_delete_boundary(tmp_path):
    """The fail-safe direction, exercised against REAL FILES ON DISK: the wrong
    month's landing dir contains none of them, so `scope_to_landing_dir` returns
    them all as `outside`, `delete_files` is handed nothing, and every byte is
    still there afterwards.

    This is the containment half of the month guard. The task refuses earlier and
    harder when it can -- `reclaim_landing.resolve_month` raises on a month that
    contradicts the stamp -- but this is what stands when nothing can be
    cross-checked, and no test exercised it against a file that actually existed."""
    landed = tmp_path / "cnpj" / _MONTH / "estabelecimentos"
    landed.mkdir(parents=True)
    part = landed / "K3241.ESTABELE"
    part.write_text("real bytes")
    wrong = str(tmp_path / "cnpj" / _OTHER_MONTH / "estabelecimentos")

    scope = scope_to_landing_dir([str(part)], wrong)
    outcome = delete_files(scope.inside)

    assert scope.inside == () and scope.outside == (str(part),)
    assert outcome.deleted == () and outcome.absent == () and outcome.failed == ()
    assert part.exists(), "a wrong month must refuse, never delete"
