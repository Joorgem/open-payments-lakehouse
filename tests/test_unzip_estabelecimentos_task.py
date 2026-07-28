# tests/test_unzip_estabelecimentos_task.py
"""Unit test for the `databricks/src/unzip_estabelecimentos.py` job task: the three
Volume dirs it hands `unzip_dir`.

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

from opl.config import DEFAULT

_SCRIPT = (
    Path(__file__).resolve().parents[1] / "databricks" / "src" / "unzip_estabelecimentos.py"
)
_spec = importlib.util.spec_from_file_location("unzip_estabelecimentos_task", _SCRIPT)
assert _spec is not None and _spec.loader is not None
task = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(task)


def test_it_stages_temporaries_outside_the_dir_the_ingest_task_reads(monkeypatch, capsys):
    seen: dict[str, object] = {}

    def fake_unzip_dir(zips_dir, dest_dir, *, tmp_dir):
        seen.update(zips_dir=zips_dir, dest_dir=dest_dir, tmp_dir=tmp_dir)
        return [Path(dest_dir) / "F.K1.ESTABELE"]

    monkeypatch.setattr(task, "unzip_dir", fake_unzip_dir)
    monkeypatch.setattr(task.sys, "argv", ["unzip_estabelecimentos.py", "2026-07"])

    task.main()

    assert seen["zips_dir"] == DEFAULT.landing_zips("estabelecimentos", "2026-07")
    # The dir bronze_estab_ingest.py points its Auto Loader at, with no pathGlobFilter.
    assert seen["dest_dir"] == DEFAULT.landing_table("estabelecimentos", "2026-07")
    tmp_dir = str(seen["tmp_dir"])
    assert not tmp_dir.startswith(str(seen["dest_dir"]))
    # Nor inside the month root, which the lookup stream walks recursively.
    assert not tmp_dir.startswith(DEFAULT.landing_cnpj_month("2026-07"))
    # But inside the same Volume, so os.replace out of it is a rename, not EXDEV.
    assert tmp_dir.startswith(DEFAULT.volume_root)
    assert "1 inner files" in capsys.readouterr().out
