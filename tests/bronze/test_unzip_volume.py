import zipfile
from pathlib import Path

import pytest

from opl.bronze.unzip_volume import CorruptZipError, unzip_dir


def _make_zip(dir_, zip_name, inner_name, payload: bytes):
    p = dir_ / zip_name
    with zipfile.ZipFile(p, "w") as z:
        z.writestr(inner_name, payload)
    return p


def _short_write(src: Path, dst: Path, dropped: int) -> Path:
    """Write ``src`` to ``dst`` with its first ``dropped`` bytes missing, leaving
    the central directory and EOCD byte-identical.

    This is the shape of the F1.3 incident object: the pinned SDK retried a failed
    PUT without rewinding the body, so what landed was ``original[67960832:]`` --
    273,373,127 of 341,333,959 bytes, a dropped PREFIX with the tail intact, so the
    EOCD still advertised the original offsets. (A gap in the middle produces the
    identical arithmetic, which is why the observation alone could not tell the two
    apart and the mechanism had to; see the F1.3 evidence doc.) CPython then
    computes ``concat = ecd_location - size_cd - offset_cd`` = -dropped and shifts
    every member's ``header_offset`` by it, yielding a negative offset. Note that
    ``ZipFile(...)`` and ``infolist()`` both SUCCEED on such a file -- only
    ``z.open(member)`` breaks, as a negative seek."""
    raw = src.read_bytes()
    cd_start = raw.index(b"PK\x01\x02")
    assert 0 < dropped < cd_start, "the drop must land inside the member data"
    dst.write_bytes(raw[dropped:])
    return dst


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


def test_short_written_zip_raises_a_clear_error_instead_of_einval(tmp_path):
    """A zip that is short at the front but keeps its tail must be rejected with
    a message that names the problem, not with the raw
    ``OSError: [Errno 22] Invalid argument`` that a negative seek produces."""
    zips, dest = tmp_path / "z", tmp_path / "d"
    zips.mkdir(), dest.mkdir()
    good = _make_zip(tmp_path, "good.zip", "F.K1.ESTABELE", b"a" * 4096)
    bad = _short_write(good, zips / "Estabelecimentos1.zip", dropped=1024)

    # The fixture only proves something if it genuinely reproduces the incident.
    with zipfile.ZipFile(bad) as z:
        (info,) = z.infolist()
        assert info.header_offset == -1024

    with pytest.raises(CorruptZipError) as excinfo:
        unzip_dir(zips, dest)

    message = str(excinfo.value)
    assert "Estabelecimentos1.zip" in message
    assert "-1024" in message
    assert not isinstance(excinfo.value, OSError)  # not the raw EINVAL seek
    assert not list(dest.iterdir())  # nothing half-written left behind


def test_multi_member_zip_raises(tmp_path):
    zips, dest = tmp_path / "z", tmp_path / "d"
    zips.mkdir(), dest.mkdir()
    p = zips / "bad.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("one", b"1"), z.writestr("two", b"2")
    with pytest.raises(ValueError):
        unzip_dir(zips, dest)
