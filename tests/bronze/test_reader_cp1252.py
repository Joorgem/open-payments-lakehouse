from pathlib import Path

import pytest

from opl.bronze.reader import csv_read_options, read_csv_batch
from opl.spark import local_session


def test_options_are_cp1252_semicolon_quoted_headerless():
    o = csv_read_options()
    assert o["encoding"] in ("cp1252", "windows-1252")
    assert o["sep"] == ";"
    assert o["quote"] == '"'
    assert o["header"] == "false"
    assert o["mode"] == "PERMISSIVE"


def test_reads_real_cp1252_bytes_preserving_accents_and_leading_zeros(tmp_path):
    # cp1252 bytes (NOT utf-8): a leading-zero key, accented text, decimal-comma.
    rows = [
        '"01";"AÇÃO E TECNOLOGIA"',
        '"0023";"SÃO JOÃO — SERVIÇOS"',   # em dash 0x97 lives in cp1252 0x80-0x9F
    ]
    raw = ("\r\n".join(rows) + "\r\n").encode("cp1252")
    f = tmp_path / "sample.CNAECSV"
    f.write_bytes(raw)

    spark = local_session("test-cp1252")
    try:
        df = read_csv_batch(spark, str(f), "lookup")
        assert df.columns == ["codigo", "descricao"]
        recs = {r.codigo: r.descricao for r in df.collect()}
        assert set(recs) == {"01", "0023"}                 # leading zeros preserved as string
        assert recs["01"] == "AÇÃO E TECNOLOGIA"           # accents round-trip
        assert recs["0023"] == "SÃO JOÃO — SERVIÇOS"       # 0x80-0x9F byte survives (proves cp1252)
    finally:
        spark.stop()


# Smallest real landed lookup (git-ignored /data/; present on the dev box only).
_REAL = Path("data/cnpj/2026-06/unz/F.K03200$Z.D60613.QUALSCSV")


@pytest.mark.skipif(not _REAL.exists(), reason="real landed lookup not present (git-ignored data/)")
def test_reads_real_landed_qualificacoes_file():
    spark = local_session("test-real-lookup")
    try:
        df = read_csv_batch(spark, str(_REAL), "lookup")
        assert df.columns == ["codigo", "descricao"]
        rows = df.collect()
        assert len(rows) > 10                              # Qualificações has dozens of codes
        assert all(r.codigo is not None and r.codigo != "" for r in rows)
        # accented Portuguese must be intact (not mojibake) somewhere in the set
        assert any(any(ch in (r.descricao or "") for ch in "ãáçõéíêô") for r in rows)
    finally:
        spark.stop()
