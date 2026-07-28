import zipfile
from pathlib import Path

import pytest

from opl.bronze import unzip_volume
from opl.bronze.unzip_volume import CorruptZipError, unzip_dir


def _make_zip(dir_, zip_name, inner_name, payload: bytes, compress=zipfile.ZIP_STORED):
    p = dir_ / zip_name
    with zipfile.ZipFile(p, "w", compress) as z:
        z.writestr(inner_name, payload)
    return p


def _corrupt_member_data(src: Path, dst: Path, run: int = 64) -> Path:
    """Copy ``src`` to ``dst`` with ``run`` bytes of the member's compressed data
    flipped, same length, so every offset and the whole central directory stay
    valid. ``zipfile`` accepts the archive and only fails while reading the member
    -- the shape of a member that survived a bad transfer with correct byte counts.
    """
    raw = bytearray(src.read_bytes())
    cd_start = raw.index(b"PK\x01\x02")
    mid = cd_start // 2
    assert mid + run < cd_start, "the flipped run must land inside the member data"
    for i in range(mid, mid + run):
        raw[i] ^= 0xFF
    dst.write_bytes(bytes(raw))
    return dst


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


def test_a_failure_inside_the_member_copy_leaves_no_partial_file(tmp_path, monkeypatch):
    """The path that can actually strand a half-written file. The guard above fires
    BEFORE anything is created, so "nothing left behind" is free there; here the
    ``.tmp`` already exists and holds bytes when the copy dies.

    Why it matters beyond tidiness: the ``.tmp`` is written into the landing subdir
    that the Estabelecimentos Auto Loader reads with NO ``pathGlobFilter`` (only the
    lookup stream filters ``*CSV`` -- see ``opl.bronze.autoloader``), so an orphan
    is a file that stream will discover and ingest as though it were a complete CSV.
    The idempotence skip cannot save it either: that compares sizes of the FINAL
    name, which a ``.tmp`` never reaches."""
    zips, dest = tmp_path / "z", tmp_path / "d"
    zips.mkdir(), dest.mkdir()
    _make_zip(zips, "Estabelecimentos1.zip", "F.K1.ESTABELE", b"a" * 4096)

    def die_mid_copy(src, out, length=None):
        out.write(src.read(512))  # a genuinely partial file, already on disk
        out.flush()
        raise TimeoutError("simulated stall partway through the member copy")

    monkeypatch.setattr(unzip_volume.shutil, "copyfileobj", die_mid_copy)

    with pytest.raises(TimeoutError):
        unzip_dir(zips, dest)

    assert not list(dest.iterdir()), "a partial .tmp was left in the Auto Loader's path"


def test_a_corrupt_member_leaves_no_partial_file(tmp_path):
    """Same guarantee on the shape that needs no patching to produce: a member whose
    deflate stream is damaged. ``zipfile`` only notices at the CRC check after the
    last byte, i.e. once the ``.tmp`` exists (and, for a real multi-GB member, is
    largely written)."""
    zips, dest = tmp_path / "z", tmp_path / "d"
    zips.mkdir(), dest.mkdir()
    good = _make_zip(
        tmp_path, "good.zip", "F.K1.ESTABELE",
        bytes(i % 251 for i in range(200_000)), compress=zipfile.ZIP_DEFLATED,
    )
    _corrupt_member_data(good, zips / "Estabelecimentos1.zip")

    with pytest.raises(zipfile.BadZipFile):
        unzip_dir(zips, dest)

    assert not list(dest.iterdir())


def test_multi_member_zip_raises(tmp_path):
    zips, dest = tmp_path / "z", tmp_path / "d"
    zips.mkdir(), dest.mkdir()
    p = zips / "bad.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("one", b"1"), z.writestr("two", b"2")
    with pytest.raises(ValueError):
        unzip_dir(zips, dest)
