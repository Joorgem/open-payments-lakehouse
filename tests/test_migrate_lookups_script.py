# tests/test_migrate_lookups_script.py
"""Unit tests for scripts/migrate_lookups_to_subdir.py.

Hermetic: no network, no credentials, no Spark. `FakeVolume` stands in for
``WorkspaceClient.files`` with an in-memory object store, and it is deliberately
STRICTER than the real API in one place -- ``delete_directory`` refuses a
non-empty directory, exactly as the Files API documents -- because that is the
constraint the recursive purge exists to satisfy, and a permissive double would
let a single-call version pass.
"""
from __future__ import annotations

import importlib.util
import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

from databricks.sdk.errors import NotFound
from databricks.sdk.mixins.files import FilesExt
from databricks.sdk.service.files import DirectoryEntry
from opl.bronze.lookup_routing import LOOKUP_SUFFIX

_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "migrate_lookups_to_subdir.py"
_spec = importlib.util.spec_from_file_location("migrate_lookups_cli", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

_MONTH = "2026-06"
_MONTH_DIR = f"/Volumes/workspace/default/landing/cnpj/{_MONTH}"
_DEST_DIR = f"{_MONTH_DIR}/lookups"
# MONTH-SCOPED since F1.4b PR B Task 5 Step 0. This script clears the state THIS
# codebase would use for the month it is given; the pre-Step-0 path it actually
# cleared when it ran for 2026-06 (`_checkpoints/bronze_cnpj_lookup`) is orphaned
# and deliberately not touched, because clearing one unscoped directory while
# migrating one month would delete every other month's state too.
_CHECKPOINT = f"/Volumes/workspace/default/landing/_checkpoints/{_MONTH}/bronze_cnpj_lookup"


def _landed_name(suffix: str) -> str:
    return f"F.K03200$Z.D60613.{suffix}"


def _month_root_objects() -> dict[str, bytes]:
    """The six lookups loose in the month root, plus a zip in a subdir."""
    objects = {
        f"{_MONTH_DIR}/{_landed_name(s)}": f"{s};rows".encode()
        for s in cli.LOOKUP_FILE_SUFFIXES
    }
    objects[f"{_MONTH_DIR}/zips/estabelecimentos/Estabelecimentos0.zip"] = b"PK-not-a-lookup"
    return objects


class FakeVolume:
    """In-memory stand-in for ``WorkspaceClient.files``."""

    def __init__(self, objects: dict[str, bytes] | None = None):
        self.objects: dict[str, bytes] = dict(objects or {})
        self.deleted: list[str] = []
        self.deleted_dirs: list[str] = []
        self.list_errors: dict[str, Exception] = {}
        # Bytes to store INSTEAD of what was uploaded. The point of the double:
        # same length, different content.
        self.substitute: bytes | None = None

    def list_directory_contents(self, directory_path: str):
        if directory_path in self.list_errors:
            raise self.list_errors[directory_path]
        prefix = directory_path.rstrip("/") + "/"
        keys = sorted(k for k in self.objects if k.startswith(prefix))
        if not keys:
            raise NotFound(f"{directory_path} does not exist")
        entries: list[DirectoryEntry] = []
        seen: set[str] = set()
        for key in keys:
            head, _, tail = key[len(prefix):].partition("/")
            path = prefix + head
            if path in seen:
                continue
            seen.add(path)
            entries.append(
                DirectoryEntry(
                    path=path,
                    name=head,
                    is_directory=bool(tail),
                    file_size=None if tail else len(self.objects[key]),
                )
            )
        return iter(entries)

    def get_metadata(self, file_path: str):
        if file_path not in self.objects:
            raise NotFound(file_path)
        return SimpleNamespace(content_length=len(self.objects[file_path]))

    def download_to(self, file_path: str, destination: str, **_):
        if file_path not in self.objects:
            raise NotFound(file_path)
        Path(destination).write_bytes(self.objects[file_path])

    def upload(self, file_path: str, contents, overwrite: bool = False, **_):
        data = contents.read()
        self.objects[file_path] = self.substitute if self.substitute is not None else data

    def delete(self, file_path: str):
        self.objects.pop(file_path, None)
        self.deleted.append(file_path)

    def delete_directory(self, directory_path: str):
        prefix = directory_path.rstrip("/") + "/"
        remaining = [k for k in self.objects if k.startswith(prefix)]
        if remaining:
            raise RuntimeError(f"directory not empty: {directory_path} still holds {remaining}")
        self.deleted_dirs.append(directory_path)


def _client(objects: dict[str, bytes] | None = None) -> SimpleNamespace:
    return SimpleNamespace(files=FakeVolume(objects))


# --- the suffix set is derived, not restated -----------------------------------


def test_lookup_file_suffixes_are_derived_from_the_routing_map():
    """The migration and the router must name the same six files.

    A literal list in the script that drifted from ``LOOKUP_SUFFIX`` would move a
    file ``lookup_type_from_filename`` does not recognise, and its rows would land
    in bronze with ``lookup_type`` NULL -- a green run that lost a whole lookup.
    """
    assert cli.LOOKUP_FILE_SUFFIXES == tuple(sorted(f"{code}CSV" for code in LOOKUP_SUFFIX))
    assert len(cli.LOOKUP_FILE_SUFFIXES) == 6


def test_suffix_of_ignores_a_non_lookup_file():
    assert cli.suffix_of(f"{_MONTH_DIR}/zips/estabelecimentos/E0.zip") is None
    assert cli.suffix_of(f"{_MONTH_DIR}/{_landed_name('CNAECSV')}") == "CNAECSV"


# --- refusing a set it does not recognise --------------------------------------


def test_lookup_files_returns_exactly_one_path_per_suffix_and_skips_subdirs():
    found = cli.lookup_files(_client(_month_root_objects()), _MONTH_DIR)
    assert sorted(found) == sorted(cli.LOOKUP_FILE_SUFFIXES)
    assert all(path.startswith(f"{_MONTH_DIR}/") for path in found.values())
    assert not any("zips" in path for path in found.values())


def test_lookup_files_refuses_a_missing_suffix():
    objects = _month_root_objects()
    del objects[f"{_MONTH_DIR}/{_landed_name('PAISCSV')}"]
    with pytest.raises(RuntimeError, match="exactly one file per lookup suffix"):
        cli.lookup_files(_client(objects), _MONTH_DIR)


def test_lookup_files_refuses_a_duplicate_suffix_even_though_six_files_remain():
    """A COUNT of six is satisfied by two CNAECSVs and no PAISCSV.

    That set looks complete and would reload a lookup table missing an entire
    ``lookup_type``, which is why the check is per suffix rather than a length.
    """
    objects = _month_root_objects()
    del objects[f"{_MONTH_DIR}/{_landed_name('PAISCSV')}"]
    objects[f"{_MONTH_DIR}/F.K03200$Y.D60613.CNAECSV"] = b"second copy"
    assert len([k for k in objects if cli.suffix_of(k)]) == 6
    with pytest.raises(RuntimeError, match="CNAECSV"):
        cli.lookup_files(_client(objects), _MONTH_DIR)


# --- clearing the Auto Loader state -------------------------------------------


def test_delete_directory_still_takes_no_recursive_argument():
    """Pins the SDK fact the recursive purge exists for.

    ``files.delete_directory`` deletes an EMPTY directory and takes no
    ``recursive`` parameter. If a future SDK grows one, this test goes red and
    ``purge_state_dir`` can collapse into a single call -- until then, a
    ``recursive=True`` would raise TypeError, and a TypeError caught by a blanket
    handler reads exactly like the benign already-absent case while leaving the
    old path's checkpoint in place.

    ``FilesExt`` and not ``FilesAPI``: ``WorkspaceClient.files`` resolves to the
    mixin, so the base class is not the method this script invokes. The two agree
    today, which is exactly why pinning the wrong one would keep passing if they
    stopped agreeing.
    """
    assert "recursive" not in inspect.signature(FilesExt.delete_directory).parameters


def test_purge_state_dir_removes_contents_before_the_directory():
    state = f"/Volumes/workspace/default/landing/_checkpoints/{_MONTH}/bronze_cnpj_lookup"
    w = _client(
        {
            f"{state}/metadata": b"m",
            f"{state}/offsets/0": b"o",
            f"{state}/sources/0/rocksdb/log": b"r",
        }
    )
    removed = cli.purge_state_dir(w, state)
    assert removed == 3
    assert not w.files.objects
    # The double raises on a non-empty directory, so reaching every level at all
    # proves the walk is post-order.
    assert state in w.files.deleted_dirs
    assert f"{state}/sources/0/rocksdb" in w.files.deleted_dirs


def test_purge_state_dir_reports_an_absent_directory_as_none():
    """Already gone is fine and is not zero: None distinguishes "no such
    directory" from "an empty one", which are different facts about a checkpoint."""
    assert cli.purge_state_dir(_client({"/other/x": b"x"}), "/nope") is None


def test_purge_state_dir_does_not_swallow_a_real_failure():
    """A failure to clear the checkpoint must NOT read as already-absent.

    The whole reload rests on the old path's checkpoint being gone. A blanket
    ``except Exception: print("could not clear ...")`` would let the operator go on
    to the reload with it still in place.
    """
    w = _client({"/state/x": b"x"})
    w.files.list_errors["/state"] = PermissionError("403 on the state dir")
    with pytest.raises(PermissionError):
        cli.purge_state_dir(w, "/state")


# --- the move, and what makes the delete safe ---------------------------------


def test_move_verified_deletes_the_original_only_after_a_matching_readback(tmp_path):
    src = f"{_MONTH_DIR}/{_landed_name('CNAECSV')}"
    w = _client({src: b"01;desc\n02;desc\n"})
    target, size = cli.move_verified(w, src, _DEST_DIR, tmp_path)
    assert target == f"{_DEST_DIR}/{_landed_name('CNAECSV')}"
    assert size == len(b"01;desc\n02;desc\n")
    assert w.files.objects[target] == b"01;desc\n02;desc\n"
    assert w.files.deleted == [src]


def test_move_verified_refuses_to_delete_when_the_readback_differs_at_the_SAME_size(tmp_path):
    """The property this script lives or dies on.

    The landed object is exactly the right length and holds different bytes --
    which is what a second concurrent writer to the same target can leave behind
    under the multipart upload path. Size equality passes; the original must
    still be there.
    """
    src = f"{_MONTH_DIR}/{_landed_name('MUNICCSV')}"
    w = _client({src: b"AAAAAAAA"})
    w.files.substitute = b"BBBBBBBB"  # same 8 bytes long, different content
    with pytest.raises(RuntimeError, match="REFUSING TO DELETE THE ORIGINAL"):
        cli.move_verified(w, src, _DEST_DIR, tmp_path)
    assert src in w.files.objects, "the original was deleted despite an unproven copy"
    assert w.files.deleted == []


def test_move_verified_refuses_a_short_download_before_uploading_anything(tmp_path):
    src = f"{_MONTH_DIR}/{_landed_name('QUALSCSV')}"
    w = _client({src: b"0123456789"})
    w.files.get_metadata = lambda path: SimpleNamespace(content_length=99)
    with pytest.raises(RuntimeError, match="Refusing to upload a copy that is already wrong"):
        cli.move_verified(w, src, _DEST_DIR, tmp_path)
    assert f"{_DEST_DIR}/{_landed_name('QUALSCSV')}" not in w.files.objects
    assert w.files.deleted == []


# --- the month is refused, never defaulted ------------------------------------


@pytest.mark.parametrize("argv", [[], ["2026-06", "extra"]])
def test_main_refuses_the_wrong_number_of_arguments(argv):
    with pytest.raises(ValueError, match="usage:"):
        cli.main(argv)


@pytest.mark.parametrize("month", ["", "  ", "2026-6", "2026-06/zips", "june"])
def test_main_refuses_an_absent_or_malformed_month_before_touching_the_workspace(month):
    """No client is ever constructed: ``require_month`` runs first.

    There is deliberately no ``month or DEFAULT.month`` fallback here -- the month
    picks both the directory read from and the one written to, and the pinned
    default equals the job YAMLs' own, so a substitution would migrate whichever
    month that is with nothing in the log naming it.
    """
    with pytest.raises(ValueError, match="refusing to migrate the lookup files"):
        cli.main([month])


# --- the whole flow, in order -------------------------------------------------


def test_main_clears_the_state_before_moving_any_file(monkeypatch):
    """STATE FIRST, FILES SECOND. A failure clearing the checkpoint has to leave
    nothing moved, so the script is a clean retry; the other order leaves six
    moved files and a stale checkpoint, and the re-run refuses because the month
    root no longer holds the set."""
    objects = _month_root_objects()
    checkpoint = _CHECKPOINT
    objects[f"{checkpoint}/offsets/0"] = b"consumed-the-old-paths"
    w = _client(objects)
    order: list[str] = []
    real_purge, real_move = cli.purge_state_dir, cli.move_verified
    monkeypatch.setattr(
        cli, "purge_state_dir", lambda c, p: (order.append(f"purge:{p}"), real_purge(c, p))[1]
    )
    monkeypatch.setattr(
        cli,
        "move_verified",
        lambda c, s, d, wd: (order.append(f"move:{s}"), real_move(c, s, d, wd))[1],
    )
    monkeypatch.setattr(cli, "upload_client", lambda: w)

    assert cli.main([_MONTH]) == 0

    assert [step for step in order if step.startswith("purge")], "no state was cleared"
    first_move = next(i for i, step in enumerate(order) if step.startswith("move:"))
    first_purge = next(i for i, step in enumerate(order) if step.startswith("purge:"))
    assert first_purge < first_move, f"a file moved before the state was cleared: {order}"
    assert f"{checkpoint}/offsets/0" not in w.files.objects


def test_main_clears_the_state_again_after_the_moves(monkeypatch):
    """The second purge is an idempotent re-clear against a concurrent run of a
    wheel carrying TODAY's layout, and that is all it now is.

    IT USED TO CLAIM MORE, AND THAT CLAIM IS RETRACTED HERE. The window it was
    written for was the OLD wheel's: until step 1's deploy the workspace ran a
    lookup stream that read the month root RECURSIVELY, the new subdir sits under
    that root, so a run landing between the clear and the moves would ingest the six
    files from their new paths and recreate the checkpoint at the same location the
    first call had just emptied. Since F1.4b PR B Task 5 Step 0 month-scoped the
    state locations, a PRE-Step-0 wheel writes the unscoped
    `_checkpoints/<table_key>` instead -- which `clear_state` deliberately does not
    touch, and could not without deleting every other month's state. That window
    therefore MOVED, and this test does not close it. Accepted, not overlooked: the
    script is spent, its one 2026-06 run is history
    (`docs/f1.4a-migration-evidence.md`), and no pre-Step-0 wheel is deployed. Said
    out loud because a docstring that claims to cover a now-unguarded window reads
    as coverage nobody has -- which is the defect Step 0's own consequence-2 work
    was about.

    What is simulated below is the case that IS reachable: something recreates the
    state at the path this codebase writes, mid-move, and the second call removes it.
    """
    checkpoint = _CHECKPOINT
    w = _client(_month_root_objects())
    real_move = cli.move_verified

    def move_then_a_concurrent_old_wheel_run(client, src, dest, workdir):
        result = real_move(client, src, dest, workdir)
        client.files.objects[f"{checkpoint}/offsets/0"] = b"re-consumed the new paths"
        return result

    monkeypatch.setattr(cli, "move_verified", move_then_a_concurrent_old_wheel_run)
    monkeypatch.setattr(cli, "upload_client", lambda: w)

    assert cli.main([_MONTH]) == 0
    assert f"{checkpoint}/offsets/0" not in w.files.objects, (
        "a checkpoint recreated during the moves survived -- the reload would not be "
        "a clean first ingest"
    )


def test_clear_state_is_idempotent_when_nothing_is_there(capsys):
    """What makes the second call free. Both state dirs absent is the normal case
    for the second pass, and it must not be an error."""
    from opl.bronze.registry import table_spec

    w = _client({"/unrelated/x": b"x"})
    cli.clear_state(w, table_spec("lookup"), _MONTH)
    cli.clear_state(w, table_spec("lookup"), _MONTH)
    assert capsys.readouterr().out.count("was already absent") == 4


def test_clear_state_clears_the_state_of_the_month_it_was_given(capsys):
    """The month is not decoration here either: it selects WHICH month's Auto
    Loader state is cleared, and this script's own `month` local already selects
    the directory read from and the one written to. A second lookup of it -- or the
    config's pinned default -- would clear one month's checkpoint while moving
    another month's files, leaving the reload anything but a clean first ingest."""
    from opl.bronze.registry import table_spec

    other = "2026-07"
    w = _client({f"{_CHECKPOINT}/offsets/0": b"june", "/unrelated/x": b"z"})
    cli.clear_state(w, table_spec("lookup"), other)
    assert f"{_CHECKPOINT}/offsets/0" in w.files.objects, (
        "clearing 2026-07's state removed 2026-06's"
    )
    printed = capsys.readouterr().out
    assert other in printed and f"/{_MONTH}/" not in printed


def test_main_moves_every_lookup_and_prints_the_remaining_steps(monkeypatch, capsys):
    w = _client(_month_root_objects())
    monkeypatch.setattr(cli, "upload_client", lambda: w)
    assert cli.main([_MONTH]) == 0

    for suffix in cli.LOOKUP_FILE_SUFFIXES:
        assert f"{_DEST_DIR}/{_landed_name(suffix)}" in w.files.objects
        assert f"{_MONTH_DIR}/{_landed_name(suffix)}" not in w.files.objects
    # The zip in the subdir was never in scope.
    assert f"{_MONTH_DIR}/zips/estabelecimentos/Estabelecimentos0.zip" in w.files.objects

    out = capsys.readouterr().out
    # The DEPLOY step, and the TRUE reason for it. An old-wheel run does not
    # ingest nothing: its stream reads the month root RECURSIVELY, and the new
    # subdir is under that root, so it re-ingests all six files with
    # pre-snapshot-column code. A runbook that gives a false reason is worse than
    # one that gives a thin one, so the wording is asserted, not just the command.
    assert "databricks bundle deploy -t free" in out
    assert "RECURSIVELY" in out
    assert "would not ingest nothing" in out
    assert "recreate the checkpoint" in out
    assert f"databricks bundle run bronze_cnpj_lookup -t free --params month={_MONTH}" in out
    assert "CAPTURE THE PRE-MIGRATION STATE" in out
    assert "DROP TABLE IF EXISTS workspace.default.bronze_cnpj_lookup;" in out
    assert "batches = 1" in out
    assert "types   = 6" in out
    # No row-count target is restated here; the doc is cited instead.
    assert "docs/f1.2-bronze-run-evidence.md" in out
    assert "7408" not in out and "7,408" not in out
