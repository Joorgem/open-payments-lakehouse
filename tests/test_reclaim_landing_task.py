# tests/test_reclaim_landing_task.py
"""Unit tests for the `databricks/src/reclaim_landing.py` job task: WHEN it
deletes, when it refuses to, and that it says which of those happened.

Loaded by path with the same importlib pattern as
`tests/test_promote_batch_task.py` -- the `databricks/src` scripts are job entry
points, not part of the opl wheel. No JVM and no workspace: the session is a
recorder and the proof set is injected, because what is under test here is the
task's own decision. `file_accounts_of_batch`, `months_of_batch` and
`delete_files` are tested against a real Delta log and a real filesystem in
`tests/bronze/test_retention.py`.

The properties this file exists for, in order of what they cost if lost:
1. Nothing is deleted unless the batch's rows for that file ADD UP -- every one
   of them in bronze or in quarantine, and at least one in bronze. The
   arithmetic itself is driven out of real Delta tables in
   `tests/bronze/test_retention.py`; what is under test here is that this task
   deletes the files it admits and REPORTS the ones it held back.
2. An empty proof set deletes nothing, does not raise, and does not go quiet.
3. A delete that fails does not fail the job -- but the operator is told.
4. An argument the task cannot trust is refused BEFORE anything is deleted.
5. The month -- half the delete boundary -- is the one the INGEST stamped on this
   batch, and a passed month that contradicts it stops the task instead of
   quietly reclaiming nothing under a directory that never held these files.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from opl.bronze.promote import PromoteRefused
from opl.bronze.registry import UnknownTable
from opl.bronze.retention import FileAccount, RetentionOutcome

_SCRIPT = Path(__file__).resolve().parents[1] / "databricks" / "src" / "reclaim_landing.py"
_spec = importlib.util.spec_from_file_location("reclaim_landing_task", _SCRIPT)
assert _spec is not None and _spec.loader is not None
task = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(task)

_LANDING = "/Volumes/workspace/default/landing/cnpj/2026-06/estabelecimentos"
_ZIPS = "/Volumes/workspace/default/landing/cnpj/2026-06/zips/estabelecimentos"
_PART = f"{_LANDING}/K3241.K03200Y0.D60613.ESTABELE"


class FakeSpark:
    """Records which table was asked about. Nothing else of a session is used."""

    def __init__(self, exists: bool = True) -> None:
        self.exists = exists
        self.catalog = SimpleNamespace(tableExists=self._table_exists)
        self.asked: list[str] = []

    def _table_exists(self, table: str) -> bool:
        self.asked.append(table)
        return self.exists


def _stub_session(
    monkeypatch, exists: bool = True, months: tuple[str, ...] = ("2026-06",)
) -> FakeSpark:
    """The session, plus the month stamp every test needs the task to find.

    `months` defaults to the month the tests pass, so a test that says nothing
    about the month is testing the delete path and not the boundary. The two tests
    that ARE about the boundary override it, which is what makes the cross-check
    visible rather than a coincidence of both values being "2026-06"."""
    spark = FakeSpark(exists)
    monkeypatch.setattr(
        task, "SparkSession",
        SimpleNamespace(builder=SimpleNamespace(getOrCreate=lambda: spark)),
    )
    monkeypatch.setattr(task, "months_of_batch", lambda spark, staging, batch: months)
    return spark


def _account(source_file: str, staged=1, promoted=1, quarantined=0, reclaimable=True):
    return FileAccount(source_file, staged, promoted, quarantined, reclaimable)


def _stub_proof(monkeypatch, files: list[str], held: tuple = ()) -> dict:
    """The proof set, as accounts. `files` are admissible, `held` are not.

    Injected because what is under test here is the task's own decision -- which
    files it hands to `delete_files`, which it names in the log, and in which
    order it refuses. `reconcile.RECLAIMABLE_SQL` deciding those flags correctly is
    a property of real tables and is tested against them."""
    seen: dict[str, object] = {}

    def fake_accounts(spark, spec, batch_id):
        seen.update(spec=spec, batch_id=batch_id)
        return tuple(_account(path) for path in files) + tuple(held)

    monkeypatch.setattr(task, "file_accounts_of_batch", fake_accounts)
    return seen


def _record_deletes(monkeypatch, outcome: RetentionOutcome | None = None) -> list[str]:
    deleted: list[str] = []

    def fake_delete_files(paths):
        paths = list(paths)
        deleted.extend(paths)
        return outcome or RetentionOutcome(tuple(paths), (), ())

    monkeypatch.setattr(task, "delete_files", fake_delete_files)
    return deleted


def test_it_deletes_the_files_bronze_credits_to_this_batch(monkeypatch, capsys):
    spark = _stub_session(monkeypatch)
    seen = _stub_proof(monkeypatch, [_PART])
    deleted = _record_deletes(monkeypatch)

    task.main(["estabelecimentos", "999", "2026-06"])

    assert seen["spec"].name == "estabelecimentos"
    assert seen["batch_id"] == "999"
    assert spark.asked == ["workspace.default.bronze_cnpj_estabelecimentos"]
    assert deleted == [_PART]
    assert "deleted=1" in capsys.readouterr().out


def test_a_proven_file_outside_the_landing_dir_is_refused_and_reported(monkeypatch, capsys):
    """The worst case this task can be handed: a zip, credited to the batch by
    bronze itself. F1.3 proved a stream can discover files nobody pointed it at
    (a probe.txt in `zips/estabelecimentos/` reached the lookup staging table),
    and the zip is the only way back to the source. Refusing it silently would be
    half a fix -- such a row means the stream read outside its own dir, which is a
    defect to investigate, not a line to swallow."""
    _stub_session(monkeypatch)
    _stub_proof(monkeypatch, [_PART, f"{_ZIPS}/Estabelecimentos1.zip"])
    deleted = _record_deletes(monkeypatch)

    task.main(["estabelecimentos", "999", "2026-06"])

    assert deleted == [_PART], "a zip was handed to delete_files"
    out = capsys.readouterr().out
    assert "refused=1" in out
    assert "REFUSED (left untouched)" in out and "Estabelecimentos1.zip" in out


def test_a_month_that_contradicts_the_stamp_stops_the_task_before_any_delete(
        monkeypatch):
    """THE MONTH GUARD THIS TASK DID NOT HAVE. A wrong-but-well-formed month used
    to be answered by containment alone: every proven file falls outside the
    scope, `REFUSED (left untouched)` is printed for each one telling the operator
    to investigate how a row of this table came from there, zero bytes are freed
    and the run exits 0. That log is indistinguishable from the containment guard
    catching a real F1.3-style incident, while the actual defect -- a month
    parameter naming a month this batch was never ingested under -- is nowhere in
    it.

    The batch's own `_snapshot_month` is what settles it, and it is not a second
    opinion: it is the ingest run's own `{{job.parameters.month}}` after
    `require_month`, stamped into the rows. So the two cannot both be right, and
    the task refuses to pick."""
    _stub_session(monkeypatch, months=("2026-06",))
    _stub_proof(monkeypatch, [_PART])
    deleted = _record_deletes(monkeypatch)

    with pytest.raises(ValueError, match="2026-05") as excinfo:
        task.main(["estabelecimentos", "999", "2026-05"])

    assert "2026-06" in str(excinfo.value), "the message must name BOTH months"
    assert deleted == []


def test_with_no_stamp_a_wrong_month_still_refuses_every_file(monkeypatch, capsys):
    """What stands when nothing can be cross-checked -- a pre-F1.4a staging shape
    has no `_snapshot_month` column, so the passed month is all there is.

    Last month's landed files are still landed and still un-reclaimed, and they
    are not what THIS batch proved. Containment is the fail-safe direction and it
    is unchanged: refused, not deleted."""
    _stub_session(monkeypatch, months=())
    _stub_proof(monkeypatch, [_PART])
    deleted = _record_deletes(monkeypatch)

    task.main(["estabelecimentos", "999", "2026-05"])

    assert deleted == []
    assert "refused=1" in capsys.readouterr().out


def test_an_empty_proof_set_deletes_nothing_names_the_causes_and_stays_green(
        monkeypatch, capsys):
    """THE DECISION. An empty result has more than one cause -- an ingest that
    found no new file, a well-formed but wrong batch id, a bronze rebuilt after
    the promote -- and the safe action is the same for all three (delete nothing),
    so the code does not branch. The LOG does: a bare "nothing to do" would hide
    two causes that need an operator. It stays green because the first cause is
    the pipeline's own quiet path."""
    _stub_session(monkeypatch)
    _stub_proof(monkeypatch, [])
    deleted = _record_deletes(monkeypatch)

    task.main(["estabelecimentos", "999", "2026-06"])

    assert deleted == []
    out = capsys.readouterr().out
    assert "NOTHING WAS DELETED" in out
    assert "no new file" in out and "typo" in out and "rebuilt after the promote" in out


def test_a_missing_bronze_table_is_told_apart_from_an_empty_batch(monkeypatch, capsys):
    """Same action, different fact. "Bronze holds no row of this batch" and
    "bronze does not exist" both mean nothing may be deleted, but a reader of the
    first could conclude the batch was already reclaimed. The second says the
    authority itself is gone and the files are still there."""
    _stub_session(monkeypatch, exists=False)
    _stub_proof(monkeypatch, [_PART])  # would be deleted if the guard were missing
    deleted = _record_deletes(monkeypatch)

    task.main(["estabelecimentos", "999", "2026-06"])

    assert deleted == []
    assert "does not exist" in capsys.readouterr().out


def test_a_delete_that_failed_is_reported_and_does_not_fail_the_job(monkeypatch, capsys):
    """It runs after a successful promote: the rows are in bronze either way, so
    a file that cannot be removed is a quota problem, not a data problem. Silence
    is the other failure -- a file that is STILL THERE holds Volume quota."""
    _stub_session(monkeypatch)
    _stub_proof(monkeypatch, [_PART])
    _record_deletes(monkeypatch, RetentionOutcome((), (), ((_PART, "Input/output error"),)))

    task.main(["estabelecimentos", "999", "2026-06"])

    out = capsys.readouterr().out
    assert "failed=1" in out
    assert "STILL THERE" in out and "Input/output error" in out


def test_every_file_already_absent_is_not_reported_as_reclaimed_space(monkeypatch, capsys):
    """An idempotent re-run and a path form this code cannot resolve produce the
    IDENTICAL signature -- FileNotFoundError on every file. The first freed the
    space on an earlier run; the second never freed a byte and never will. The
    log must not let them read alike."""
    _stub_session(monkeypatch)
    _stub_proof(monkeypatch, [_PART])
    _record_deletes(monkeypatch, RetentionOutcome((), (_PART,), ()))

    task.main(["estabelecimentos", "999", "2026-06"])

    out = capsys.readouterr().out
    assert "already_absent=1" in out
    assert "do not read it as success" in out


def test_a_normal_delete_prints_no_absent_warning(monkeypatch, capsys):
    """The note above must fire on the ambiguous case only, or it is noise that
    gets filtered out and stops being read."""
    _stub_session(monkeypatch)
    _stub_proof(monkeypatch, [_PART])
    _record_deletes(monkeypatch)

    task.main(["estabelecimentos", "999", "2026-06"])

    assert "do not read it as success" not in capsys.readouterr().out


def test_an_unknown_table_is_refused_before_spark_and_before_any_delete(monkeypatch):
    """"Never fail the job" covers the DELETES, not the arguments. A task that
    does not know which table it is reclaiming does not know which directory it
    is confined to, and the whole safety argument is that directory."""
    spark = _stub_session(monkeypatch)
    deleted = _record_deletes(monkeypatch)

    with pytest.raises(UnknownTable) as excinfo:
        task.main(["estabelecimento", "999"])  # a real typo: singular

    assert "estabelecimentos" in str(excinfo.value)
    assert spark.asked == [] and deleted == []


def test_a_month_that_is_not_a_month_is_refused_before_spark_and_before_any_delete(
        monkeypatch):
    """The month defines HALF the delete boundary, so a malformed one is refused
    at the same boundary as a bad table -- before a serverless session is burned.

    `2026-06/zips` is the probe that matters: `landing_table` interpolates it raw,
    so the containment root would become the zips directory itself and the guard
    would admit every zip under it as "inside". The proof set here names one, to
    make the delete this refusal prevents concrete rather than notional."""
    spark = _stub_session(monkeypatch)
    _stub_proof(monkeypatch, [f"{_ZIPS}/Estabelecimentos1.zip"])
    deleted = _record_deletes(monkeypatch)

    with pytest.raises(ValueError, match="month"):
        task.main(["estabelecimentos", "999", "2026-06/zips"])

    assert spark.asked == [] and deleted == []


def test_an_impossible_month_is_refused_before_a_session_is_even_REQUESTED(monkeypatch):
    """`2026-13` is shape-valid, so nothing downstream of here can catch it.

    THE FAILURE IT PREVENTS IS A GREEN RUN. `landing_table` interpolates the month
    raw, so the containment root becomes `cnpj/2026-13/estabelecimentos` -- a
    directory that cannot exist. Every file bronze proved is then OUTSIDE it, so
    `delete_files` is handed nothing and `_report` prints
    `REFUSED (left untouched)` for each one, whose text tells the operator to
    "investigate how a row of this table came from there". That is the loudest
    message this task has, spent on an incident that did not happen, while the actual
    bug -- a month parameter naming no month -- is nowhere in the log. The proof set
    below is the REAL landed part for exactly that reason: under the shape-only check
    this call exited 0 having reclaimed nothing and having blamed the data.

    Stubs `getOrCreate` to RAISE rather than asserting `spark.asked == []` like its
    two neighbours: those prove no table was queried, this proves no serverless
    session was requested at all, which is the ruling `require_month` claims in its
    docstring ("Refuses BEFORE Spark") and the reason the range belongs in the shared
    guard instead of in `ref_date_column`, which is reached only by the ingests."""
    def _no_session():
        raise AssertionError(
            "SparkSession.builder.getOrCreate() was reached with an impossible month"
        )

    monkeypatch.setattr(
        task, "SparkSession",
        SimpleNamespace(builder=SimpleNamespace(getOrCreate=_no_session)),
    )
    _stub_proof(monkeypatch, [_PART])
    deleted = _record_deletes(monkeypatch)

    for impossible in ("2026-13", "2026-00"):
        with pytest.raises(ValueError, match="01-12") as excinfo:
            task.main(["estabelecimentos", "999", impossible])
        assert "refusing to reclaim" in str(excinfo.value)

    assert deleted == []


def test_no_month_at_all_is_taken_from_the_batch_and_never_from_the_pinned_one(
        monkeypatch):
    """The triage path's month, and the assertion that it is not a fallback.

    `repromote_triaged_batch` runs hours or days after the ingest, from a
    reconciliation row naming a table and a batch id; nobody there has the ingest
    run's month in hand. So an absent month is DERIVED from `_snapshot_month`
    rather than refused -- and the derivation must not be `opl.config`'s pinned
    month wearing a query.

    THE STAMP AND THE PART ARE 2026-07 ON PURPOSE. `DEFAULT.month` is "2026-06",
    so a fallback would scope containment to 2026-06, put this 2026-07 part
    outside it and reclaim nothing -- the exact green-and-empty run
    `require_month` was written to prevent. A test stamped 2026-06 would have
    passed against the fallback too, and would be pinning nothing."""
    july = "/Volumes/workspace/default/landing/cnpj/2026-07/estabelecimentos/K3241.ESTABELE"
    _stub_session(monkeypatch, months=("2026-07",))
    _stub_proof(monkeypatch, [july])
    deleted = _record_deletes(monkeypatch)

    task.main(["estabelecimentos", "999"])

    assert deleted == [july]


def test_a_batch_that_stamps_no_month_refuses_rather_than_falling_back(monkeypatch):
    """Zero is not "use the default". The staging rows are gone, or the table
    predates the column -- either way this task does not know which directory it
    would be deleting under, and reclaiming under a guess is how the wrong
    month's landing dir gets emptied. Nothing is deleted and the bytes stay,
    which is the recoverable direction."""
    _stub_session(monkeypatch, months=())
    _stub_proof(monkeypatch, [_PART])
    deleted = _record_deletes(monkeypatch)

    with pytest.raises(ValueError, match="does not name exactly one"):
        task.main(["estabelecimentos", "999"])

    assert deleted == []


def test_a_batch_stamped_with_two_months_refuses_rather_than_picking_one(monkeypatch):
    """One ingest run takes ONE month, so this state has no explanation -- which is
    why it must not be answered by taking the first. A delete boundary chosen out
    of an unexplained state is a delete boundary nobody can check."""
    _stub_session(monkeypatch, months=("2026-06", "2026-07"))
    _stub_proof(monkeypatch, [_PART])
    deleted = _record_deletes(monkeypatch)

    with pytest.raises(ValueError, match="does not name exactly one"):
        task.main(["estabelecimentos", "999"])

    assert deleted == []


def test_a_table_with_no_zip_in_the_volume_is_refused_before_spark_and_any_delete(
        monkeypatch):
    """THE ONE CASE WHERE THIS TASK COULD DELETE THE LAST COPY.

    Both this task's docstring and `opl.bronze.retention`'s rest on "the zips are
    the only way back to the source", which F1.3's incidents 3 and 4 made concrete:
    two parse defects found AFTER ingestion, both fixed by re-reading the source. A
    `local`-landed table has no zip in the Volume at all -- `extract_cnpj.py` unzips
    on the extraction host and PUTs only the inner file, so `cnpj/<month>/zips/`
    holds `estabelecimentos` and nothing else. Its landed files are the single copy
    in the workspace, and recovery means re-downloading a monthly snapshot the RFB
    may have rotated.

    `bronze_ingest.py` and `unzip_table.py` already refuse on this same field. This
    task, the only one that DELETES, did not -- and it is invoked BY HAND with a
    positional table name, so the caller who can reach it is precisely the one the
    guard is for. The proof set is a real landed lookup CSV, so what the refusal
    prevents is a delete this test would otherwise have recorded."""
    spark = _stub_session(monkeypatch)
    _stub_proof(
        monkeypatch,
        ["/Volumes/workspace/default/landing/cnpj/2026-06/lookups/F.K03200$Z.D60613.CNAECSV"],
    )
    deleted = _record_deletes(monkeypatch)

    with pytest.raises(ValueError) as excinfo:
        task.main(["lookup", "999", "2026-06"])

    message = str(excinfo.value)
    assert "lookup" in message
    # The reason has to be the ABSENT ARCHIVE, not the landing mode that detects it:
    # an operator reading "landing='local'" mid-incident learns nothing about why the
    # bytes may not go.
    assert "NO ARCHIVE" in message and "single copy" in message
    assert spark.asked == [] and deleted == []


def test_no_batch_id_at_all_is_refused_by_the_shared_guard(monkeypatch):
    """The forgotten-`--params` operator run. The same guard the promote and the
    gate use, so its message cannot drift -- `action="reclaim"` is what makes it
    name THIS task rather than misreport where the run stopped."""
    spark = _stub_session(monkeypatch)
    deleted = _record_deletes(monkeypatch)

    with pytest.raises(PromoteRefused, match="refusing to reclaim"):
        task.main(["estabelecimentos"])

    assert spark.asked == [] and deleted == []


def test_a_file_held_back_is_named_with_the_counts_that_held_it(monkeypatch, capsys):
    """A category the old proof could not have: it deleted every file it named.

    A control that declines silently cannot be told from a control that never
    ran, which is the defect four guards in this repository have already had. So
    the counts travel with the refusal -- and they are the same three columns
    `dataops_reconciliation_by_file` publishes, so an operator can check the line
    against the view without translating anything."""
    _stub_session(monkeypatch)
    _stub_proof(monkeypatch, [_PART], held=(_account(
        f"{_LANDING}/K3241.K03200Y0.D60613.ESTABELE2",
        staged=2, promoted=1, quarantined=0, reclaimable=False,
    ),))
    deleted = _record_deletes(monkeypatch)

    task.main(["estabelecimentos", "999", "2026-06"])

    out = capsys.readouterr().out
    assert deleted == [_PART], "the file that adds up must still go"
    assert "held_back=1" in out
    assert "HELD BACK (left untouched)" in out and "ESTABELE2" in out
    assert "staged=2 promoted=1 quarantined=0 unaccounted=1" in out


def test_a_batch_whose_every_file_is_held_back_deletes_nothing_and_says_why(
        monkeypatch, capsys):
    """Distinct from "bronze holds no row of this batch", and the distinction is
    the operator's next move. Bronze holds rows here -- it is the ARITHMETIC that
    refused, not the absence of a promote -- so the three-cause message about
    typos and rebuilt tables would send a triager to the wrong question."""
    _stub_session(monkeypatch)
    _stub_proof(monkeypatch, [], held=(_account(
        _PART, staged=3, promoted=1, quarantined=1, reclaimable=False,
    ),))
    deleted = _record_deletes(monkeypatch)

    task.main(["estabelecimentos", "999", "2026-06"])

    out = capsys.readouterr().out
    assert deleted == []
    assert "NOT ONE of them is accounted for" in out
    assert "unaccounted=1" in out
    assert "rebuilt after the promote" not in out, "that is a different fact"


def test_a_table_with_no_zip_is_a_green_no_op_when_the_job_serves_every_table(
        monkeypatch, capsys):
    """`repromote_triaged_batch` takes ANY registered table, and four of them land
    with no archive in the Volume -- payments among them, which is the table the
    reconciliation view prints a repromote remedy for right now.

    Without this flag that remedy ends RED on its last task, on a repromote that
    worked perfectly, which is how an operator learns to read red as noise. It
    loosens no delete: the reason is printed in full and not one byte is touched,
    exactly as the raise leaves it."""
    _stub_session(monkeypatch)
    deleted = _record_deletes(monkeypatch)

    task.main(["payments", "999", "--any-table"])

    out = capsys.readouterr().out
    assert deleted == []
    assert "NO ARCHIVE" in out and "single copy" in out


def test_the_any_table_flag_does_not_change_what_a_zips_table_reclaims(
        monkeypatch, capsys):
    """The flag says which CALLER is asking, never what may go. A table that lands
    as zips reclaims the same files with it as without it -- otherwise it would be
    a token that widens a delete, which is what `--in-flow` is careful not to be
    on the promote side."""
    _stub_session(monkeypatch)
    _stub_proof(monkeypatch, [_PART])
    deleted = _record_deletes(monkeypatch)

    task.main(["estabelecimentos", "999", "2026-06", "--any-table"])

    assert deleted == [_PART]
    assert "deleted=1" in capsys.readouterr().out


def test_the_flag_is_not_a_positional_argument(monkeypatch):
    """It is filtered out of the positionals before the month is read, the same way
    `promote_batch.py` filters `--in-flow`. If it were not, this call would read
    the flag as a month and `require_month` would refuse it -- which is a red run
    on a correct invocation, and the reason this is asserted rather than assumed."""
    _stub_session(monkeypatch, months=("2026-06",))
    _stub_proof(monkeypatch, [_PART])
    deleted = _record_deletes(monkeypatch)

    task.main(["estabelecimentos", "999", "--any-table"])

    assert deleted == [_PART]
