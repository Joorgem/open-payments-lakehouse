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
    zips, dest, staging = tmp_path / "z", tmp_path / "d", tmp_path / "t"
    zips.mkdir(), dest.mkdir()
    _make_zip(zips, "Estabelecimentos1.zip", "F.K1.ESTABELE", b"a" * 100)
    _make_zip(zips, "Estabelecimentos2.zip", "F.K2.ESTABELE", b"b" * 200)
    out = unzip_dir(zips, dest, tmp_dir=staging)
    assert sorted(p.name for p in out) == ["F.K1.ESTABELE", "F.K2.ESTABELE"]
    assert (dest / "F.K1.ESTABELE").stat().st_size == 100
    assert list(staging.iterdir()) == []  # every .tmp was renamed away, none left


def test_skips_already_extracted_with_matching_size(tmp_path):
    zips, dest, staging = tmp_path / "z", tmp_path / "d", tmp_path / "t"
    zips.mkdir(), dest.mkdir()
    _make_zip(zips, "E1.zip", "F.K1.ESTABELE", b"a" * 100)
    (dest / "F.K1.ESTABELE").write_bytes(b"a" * 100)          # already there, right size
    before = (dest / "F.K1.ESTABELE").stat().st_mtime_ns
    out = unzip_dir(zips, dest, tmp_dir=staging)
    assert (dest / "F.K1.ESTABELE").stat().st_mtime_ns == before  # untouched
    assert [p.name for p in out] == ["F.K1.ESTABELE"]


def test_re_extracts_when_size_mismatches(tmp_path):
    zips, dest, staging = tmp_path / "z", tmp_path / "d", tmp_path / "t"
    zips.mkdir(), dest.mkdir()
    _make_zip(zips, "E1.zip", "F.K1.ESTABELE", b"a" * 100)
    (dest / "F.K1.ESTABELE").write_bytes(b"partial")           # stale partial
    unzip_dir(zips, dest, tmp_dir=staging)
    assert (dest / "F.K1.ESTABELE").stat().st_size == 100


def test_short_written_zip_raises_a_clear_error_instead_of_einval(tmp_path):
    """A zip that is short at the front but keeps its tail must be rejected with
    a message that names the problem, not with the raw
    ``OSError: [Errno 22] Invalid argument`` that a negative seek produces."""
    zips, dest, staging = tmp_path / "z", tmp_path / "d", tmp_path / "t"
    zips.mkdir(), dest.mkdir()
    good = _make_zip(tmp_path, "good.zip", "F.K1.ESTABELE", b"a" * 4096)
    bad = _short_write(good, zips / "Estabelecimentos1.zip", dropped=1024)

    # The fixture only proves something if it genuinely reproduces the incident.
    with zipfile.ZipFile(bad) as z:
        (info,) = z.infolist()
        assert info.header_offset == -1024

    with pytest.raises(CorruptZipError) as excinfo:
        unzip_dir(zips, dest, tmp_dir=staging)

    message = str(excinfo.value)
    assert "Estabelecimentos1.zip" in message
    assert "-1024" in message
    assert not isinstance(excinfo.value, OSError)  # not the raw EINVAL seek
    assert not list(dest.iterdir())  # nothing half-written left behind


def test_a_failure_inside_the_member_copy_leaves_no_partial_file(tmp_path, monkeypatch):
    """The path that can actually strand a half-written file. The guard above fires
    BEFORE anything is created, so "nothing left behind" is free there; here the
    ``.tmp`` already exists and holds bytes when the copy dies.

    Two things are asserted apart, because they have different consequences: the
    landing dir the Estabelecimentos Auto Loader reads must not hold the partial file
    (a correctness property -- that stream reads it with NO ``pathGlobFilter``, so a
    partial there is ingested as a complete CSV, and the idempotence skip compares
    sizes of the FINAL name, which a ``.tmp`` never reaches), and the staging dir must
    not keep it either (housekeeping -- a real member is up to 6.78 GB of Volume
    quota)."""
    zips, dest, staging = tmp_path / "z", tmp_path / "d", tmp_path / "t"
    zips.mkdir(), dest.mkdir()
    _make_zip(zips, "Estabelecimentos1.zip", "F.K1.ESTABELE", b"a" * 4096)

    def die_mid_copy(src, out, length=None):
        out.write(src.read(512))  # a genuinely partial file, already on disk
        out.flush()
        raise TimeoutError("simulated stall partway through the member copy")

    monkeypatch.setattr(unzip_volume.shutil, "copyfileobj", die_mid_copy)

    with pytest.raises(TimeoutError):
        unzip_dir(zips, dest, tmp_dir=staging)

    assert not list(dest.iterdir()), "a partial .tmp was left in the Auto Loader's path"
    assert not list(staging.iterdir()), "the abandoned .tmp was not cleaned up"


def test_the_temporary_is_never_created_inside_the_watched_dir(tmp_path, monkeypatch):
    """Cleanup is a best effort, so it must not be the only thing standing between a
    partial file and the Auto Loader.

    The test above proves the ``.tmp`` is removed when the copy dies. This one takes
    that removal away -- an ``unlink`` the Volume FUSE refuses is exactly the case the
    cleanup handler already prints "STILL THERE" for -- and asserts on the contents of
    ``dest_dir`` anyway. The Estabelecimentos stream reads that dir with NO
    ``pathGlobFilter``, so a surviving ``.tmp`` is ingested as a complete 30-column
    CSV; the idempotence skip cannot catch it either, because it compares the size of
    the FINAL name, which a ``.tmp`` never reaches. Nothing that fails may put a byte
    in there, whether or not the cleanup that follows works."""
    zips, dest, staging = tmp_path / "z", tmp_path / "d", tmp_path / "t"
    zips.mkdir(), dest.mkdir()
    _make_zip(zips, "Estabelecimentos1.zip", "F.K1.ESTABELE", b"a" * 4096)

    def die_mid_copy(src, out, length=None):
        out.write(src.read(512))  # a genuinely partial file, already on disk
        out.flush()
        raise TimeoutError("simulated stall partway through the member copy")

    def refuse_to_unlink(self, missing_ok=False):
        raise OSError("simulated: the Volume FUSE refused to remove the temporary")

    monkeypatch.setattr(unzip_volume.shutil, "copyfileobj", die_mid_copy)
    monkeypatch.setattr(Path, "unlink", refuse_to_unlink)

    with pytest.raises(TimeoutError):
        unzip_dir(zips, dest, tmp_dir=staging)

    assert [p.name for p in dest.iterdir()] == [], (
        "a partial file survived in the dir the Estabelecimentos Auto Loader reads"
    )
    # It is not lost, either: it is in the staging dir no stream looks at, under a
    # deterministic name the next run truncates rather than accumulating beside.
    assert [p.name for p in staging.iterdir()] == ["F.K1.ESTABELE.tmp"]


def test_a_corrupt_member_leaves_no_partial_file(tmp_path):
    """Same guarantee on the shape that needs no patching to produce: a member whose
    deflate stream is damaged. ``zipfile`` only notices at the CRC check after the
    last byte, i.e. once the ``.tmp`` exists (and, for a real multi-GB member, is
    largely written)."""
    zips, dest, staging = tmp_path / "z", tmp_path / "d", tmp_path / "t"
    zips.mkdir(), dest.mkdir()
    good = _make_zip(
        tmp_path, "good.zip", "F.K1.ESTABELE",
        bytes(i % 251 for i in range(200_000)), compress=zipfile.ZIP_DEFLATED,
    )
    _corrupt_member_data(good, zips / "Estabelecimentos1.zip")

    with pytest.raises(zipfile.BadZipFile):
        unzip_dir(zips, dest, tmp_dir=staging)

    assert not list(dest.iterdir())
    assert not list(staging.iterdir())


def test_multi_member_zip_raises(tmp_path):
    zips, dest, staging = tmp_path / "z", tmp_path / "d", tmp_path / "t"
    zips.mkdir(), dest.mkdir()
    p = zips / "bad.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("one", b"1"), z.writestr("two", b"2")
    with pytest.raises(ValueError):
        unzip_dir(zips, dest, tmp_dir=staging)
