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


@pytest.fixture
def bronze(spark, tmp_path):
    """A throwaway Delta database, like tests/bronze/test_promote.py's `tables`:
    real managed tables under tmp_path rather than in the repo's warehouse."""
    db = f"retention_{uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE {db} LOCATION '{tmp_path.as_uri()}'")
    yield f"{db}.bronze"
    spark.sql(f"DROP DATABASE {db} CASCADE")


def test_files_of_batch_returns_only_what_bronze_holds_for_that_batch(spark, bronze):
    """A file present in staging but not in bronze must NOT appear: it has not
    been proven persisted, and deleting it would be unrecoverable without the zip."""
    from opl.bronze.retention import files_of_batch

    spark.createDataFrame(
        [("/v/a.ESTABELE", "batch-1"),
         ("/v/a.ESTABELE", "batch-1"),
         ("/v/b.ESTABELE", "batch-2")],
        "_source_file string, _batch_id string",
    # append, not overwrite: the table does not exist yet, and an overwrite of a
    # not-yet-existing managed Delta table inside a database with an explicit
    # LOCATION fails with "Table bronze does not support truncate in batch mode"
    # (measured on pyspark 3.5.9 / delta-spark 3.3.1, this repo's pins).
    ).write.format("delta").mode("append").saveAsTable(bronze)

    assert files_of_batch(spark, bronze, "batch-1") == ["/v/a.ESTABELE"]
    assert files_of_batch(spark, bronze, "batch-2") == ["/v/b.ESTABELE"]
    assert files_of_batch(spark, bronze, "batch-3") == []


def test_files_of_batch_is_empty_for_a_table_that_does_not_exist(spark):
    """Same answer as "this batch put nothing there", and deliberately so: the
    only safe action is identical (delete nothing). What the two mean is NOT
    identical, so the job task -- not this function -- tells them apart in the
    log before it decides nothing may go."""
    from opl.bronze.retention import files_of_batch

    assert files_of_batch(spark, "no_such_db.no_such_table", "batch-1") == []
