import zipfile

import pytest

from opl.bronze.unzip_volume import unzip_dir


def _make_zip(dir_, zip_name, inner_name, payload: bytes):
    p = dir_ / zip_name
    with zipfile.ZipFile(p, "w") as z:
        z.writestr(inner_name, payload)
    return p


def test_unzips_all_and_returns_paths(tmp_path):
    zips, dest = tmp_path / "z", tmp_path / "d"
    zips.mkdir(), dest.mkdir()
    _make_zip(zips, "Estabelecimentos1.zip", "F.K1.ESTABELE", b"a" * 100)
    _make_zip(zips, "Estabelecimentos2.zip", "F.K2.ESTABELE", b"b" * 200)
    out = unzip_dir(zips, dest)
    assert sorted(p.name for p in out) == ["F.K1.ESTABELE", "F.K2.ESTABELE"]
    assert (dest / "F.K1.ESTABELE").stat().st_size == 100


def test_skips_already_extracted_with_matching_size(tmp_path):
    zips, dest = tmp_path / "z", tmp_path / "d"
    zips.mkdir(), dest.mkdir()
    _make_zip(zips, "E1.zip", "F.K1.ESTABELE", b"a" * 100)
    (dest / "F.K1.ESTABELE").write_bytes(b"a" * 100)          # already there, right size
    before = (dest / "F.K1.ESTABELE").stat().st_mtime_ns
    out = unzip_dir(zips, dest)
    assert (dest / "F.K1.ESTABELE").stat().st_mtime_ns == before  # untouched
    assert [p.name for p in out] == ["F.K1.ESTABELE"]


def test_re_extracts_when_size_mismatches(tmp_path):
    zips, dest = tmp_path / "z", tmp_path / "d"
    zips.mkdir(), dest.mkdir()
    _make_zip(zips, "E1.zip", "F.K1.ESTABELE", b"a" * 100)
    (dest / "F.K1.ESTABELE").write_bytes(b"partial")           # stale partial
    unzip_dir(zips, dest)
    assert (dest / "F.K1.ESTABELE").stat().st_size == 100


def test_multi_member_zip_raises(tmp_path):
    zips, dest = tmp_path / "z", tmp_path / "d"
    zips.mkdir(), dest.mkdir()
    p = zips / "bad.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("one", b"1"), z.writestr("two", b"2")
    with pytest.raises(ValueError):
        unzip_dir(zips, dest)
