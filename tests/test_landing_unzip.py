# tests/test_landing_unzip.py
import zipfile

import pytest

from opl.extraction.landing import unzip_single


def test_unzip_single_extracts_inner_file(tmp_path):
    zp = tmp_path / "Qualificacoes.zip"
    with zipfile.ZipFile(zp, "w") as z:
        z.writestr("K3241.D60711.QUALSCSV", b'"01";"Administrador"\r\n')
    out = unzip_single(zp, tmp_path / "out")
    assert out.read_bytes().startswith(b'"01"')


def test_unzip_single_rejects_multimember(tmp_path):
    zp = tmp_path / "bad.zip"
    with zipfile.ZipFile(zp, "w") as z:
        z.writestr("a", b"x")
        z.writestr("b", b"y")
    with pytest.raises(ValueError):
        unzip_single(zp, tmp_path / "out")
