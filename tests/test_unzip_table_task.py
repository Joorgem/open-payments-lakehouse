# tests/test_unzip_table_task.py
"""Unit test for the `databricks/src/unzip_table.py` job task: the three Volume
dirs it hands `unzip_dir`, for the table it is given.

Loaded by path with the same importlib pattern as `tests/test_promote_batch_task.py`
-- the `databricks/src` scripts are job entry points, not part of the opl wheel.
`unzip_dir` itself is stubbed: what is under test is the wiring, and specifically
that the dir this task stages half-written files in is NOT one an Auto Loader reads.
Passing the landing dir there is the whole defect the staging dir exists to prevent,
and it is a defect no unzip unit test can catch, because the library trusts the
caller for that path (it cannot know what the streams watch).
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from opl.config import DEFAULT

_SRC = Path(__file__).resolve().parents[1] / "databricks" / "src"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_it_stages_temporaries_outside_the_dir_the_ingest_task_reads(monkeypatch, capsys):
    task = _load("unzip_table")
    seen: dict[str, object] = {}

    def fake_unzip_dir(zips_dir, dest_dir, *, tmp_dir):
        seen.update(zips_dir=zips_dir, dest_dir=dest_dir, tmp_dir=tmp_dir)
        return [Path(dest_dir) / "F.K1.ESTABELE"]

    monkeypatch.setattr(task, "unzip_dir", fake_unzip_dir)

    task.main(["estabelecimentos", "2026-07"])

    assert seen["zips_dir"] == DEFAULT.landing_zips("estabelecimentos", "2026-07")
    # The dir bronze_ingest.py points its Auto Loader at, with no pathGlobFilter.
    assert seen["dest_dir"] == DEFAULT.landing_table("estabelecimentos", "2026-07")
    tmp_dir = str(seen["tmp_dir"])
    assert not tmp_dir.startswith(str(seen["dest_dir"]))
    # Nor inside the month root, which the lookup stream walks recursively.
    assert not tmp_dir.startswith(DEFAULT.landing_cnpj_month("2026-07"))
    # But inside the same Volume, so os.replace out of it is a rename, not EXDEV.
    assert tmp_dir.startswith(DEFAULT.volume_root)
    assert "1 inner files" in capsys.readouterr().out


def test_unzipping_a_table_that_does_not_land_as_zips_is_refused():
    """The lookup lands already-unzipped local files, so its zips subdir is empty.

    Refused rather than left as a no-op: an unzip that "succeeds" having extracted
    nothing reads as a green task, and the ingest downstream of it then finds an
    empty landing dir and also succeeds, having ingested nothing at all."""
    task = _load("unzip_table")
    with pytest.raises(ValueError) as excinfo:
        task.main(["lookup", "2026-06"])
    assert "zips" in str(excinfo.value)


def test_unzipping_an_unknown_table_is_refused_naming_the_real_ones():
    """The same refusal the ingest gets, from the same registry, before any I/O."""
    from opl.bronze.registry import UnknownTable

    task = _load("unzip_table")
    with pytest.raises(UnknownTable) as excinfo:
        task.main(["estabelecimento", "2026-06"])  # a real typo: singular
    assert "estabelecimentos" in str(excinfo.value)


@pytest.mark.parametrize("argv", [["estabelecimentos"], ["estabelecimentos", ""],
                                  ["estabelecimentos", "   "]])
def test_an_unzip_without_a_month_is_refused_rather_than_defaulted(argv, monkeypatch):
    """The FOURTH site of one defect on this branch, and the last one left.

    `add_audit_columns` refused a defaulted `snapshot_month` -- that is the ruling,
    not a site. `reclaim_landing` and both ingest tasks contradicted it by
    substituting `opl.config`'s pinned month anyway. This task was the fourth, and
    it is the earliest link in the chain: the month picks BOTH
    dirs it uses -- which zips are read and where the inner files are written -- so
    a defaulted month extracted 2026-06 into 2026-06 and then let the ingest that
    follows read exactly what it had just written. Every layer agreed about the
    wrong month, which is precisely why nothing in the log could say so.

    NOT VACUOUS: under the fallback this argv succeeded. The table is valid and
    lands as zips, so `main` went on to call `unzip_dir` against the 2026-06 dirs.
    `unzip_dir` is stubbed to fail loudly here rather than left unstubbed, so a
    regression that restored the default cannot pass by quietly doing nothing.

    Refused AFTER the landing-mode check on purpose: a table that has no zips to
    unzip is a wronger thing to be asked than a missing month, and reordering the
    two would change which refusal an operator sees for `["lookup"]`."""
    task = _load("unzip_table")
    monkeypatch.setattr(task, "unzip_dir", _fail_if_called)

    with pytest.raises(ValueError, match="no month was given") as excinfo:
        task.main(argv)
    assert "{{job.parameters.month}}" in str(excinfo.value)


@pytest.mark.parametrize("impossible", ["2026-13", "2026-00"])
def test_an_unzip_of_a_month_that_cannot_exist_is_refused(impossible, monkeypatch):
    """The other entry point `ref_date_column`'s range check never protected.

    That check -- `1 <= int(month) <= 12` -- was the only one in the repo, and it runs
    inside `add_audit_columns`, which this task does not call: no Spark, no audit
    columns, just `zipfile` over three dirs. So a shape-valid impossible month reached
    `unzip_dir` here, which created `cnpj/2026-13/estabelecimentos` and its `_tmp`
    sibling, found no `*.zip` in a `zips` dir that had just been mkdir'd empty, and
    printed "0 inner files present" as a green task. The ingest after it then read
    that empty landing dir and also succeeded. `require_month` now owns shape AND
    range, so all four entry points inherit it from one place."""
    task = _load("unzip_table")
    monkeypatch.setattr(task, "unzip_dir", _fail_if_called)

    with pytest.raises(ValueError, match="01-12") as excinfo:
        task.main(["estabelecimentos", impossible])
    assert "refusing to unzip" in str(excinfo.value)


def _fail_if_called(*_a, **_k):
    raise AssertionError("unzip_dir was reached with an unvalidated month")
