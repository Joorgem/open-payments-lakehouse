from pathlib import Path

import pytest

from opl.bronze.reader import csv_read_options, read_csv_batch


def test_options_are_cp1252_semicolon_quoted_headerless():
    o = csv_read_options()
    assert o["encoding"] in ("cp1252", "windows-1252")
    assert o["sep"] == ";"
    assert o["quote"] == '"'
    assert o["header"] == "false"
    assert o["mode"] == "PERMISSIVE"
    # Deliberate change on top of what F1.2 shipped: RFB quotes fields that
    # contain literal newlines, so the record separator cannot be trusted.
    # Pinned here so it cannot be dropped silently -- see
    # tests/bronze/test_reader_multiline.py for the incident it fixes.
    assert o["multiLine"] == "true"
    # The other half of RFC 4180 quoting (section 2.7): a quote inside a quoted
    # field is escaped by doubling it, and in RFC 4180 the escape character IS
    # the quote character. Spark's `escape` defaults to backslash, so without
    # this RFB's `""` is never unescaped and the value can absorb the `;`
    # delimiter. Pinned for the same reason as multiLine -- dropping it corrupts
    # free-text fields on 461 of the 4,753,436 records of Estabelecimentos6
    # while every DQ rule still passes. See test_reader_multiline.py.
    assert o["escape"] == '"'
    # Deliberately absent: with `escape` set, unescapedQuoteHandling changes
    # nothing for any well-formed RFC 4180 record, and its non-default values
    # only alter how genuinely malformed records parse (RAISE_ERROR would abort
    # the job). Asserted so nobody adds it without a measured reason.
    assert "unescapedQuoteHandling" not in o


def test_reads_real_cp1252_bytes_preserving_accents_and_leading_zeros(spark, tmp_path):
    # cp1252 bytes (NOT utf-8): a leading-zero key, accented text, decimal-comma.
    rows = [
        '"01";"AÇÃO E TECNOLOGIA"',
        '"0023";"SÃO JOÃO — SERVIÇOS"',   # em dash 0x97 lives in cp1252 0x80-0x9F
    ]
    # LF record separator: the real extracts carry zero CR bytes (see the
    # docstring of tests/bronze/test_reader_multiline.py, which also pins CRLF).
    raw = ("\n".join(rows) + "\n").encode("cp1252")
    f = tmp_path / "sample.CNAECSV"
    f.write_bytes(raw)

    df = read_csv_batch(spark, str(f), "lookup")
    assert df.columns == ["codigo", "descricao"]
    recs = {r.codigo: r.descricao for r in df.collect()}
    assert set(recs) == {"01", "0023"}                 # leading zeros preserved as string
    assert recs["01"] == "AÇÃO E TECNOLOGIA"           # accents round-trip
    assert recs["0023"] == "SÃO JOÃO — SERVIÇOS"       # 0x80-0x9F byte survives (proves cp1252)


# Smallest real landed lookup (git-ignored /data/; present on the dev box only).
_REAL = Path("data/cnpj/2026-06/unz/F.K03200$Z.D60613.QUALSCSV")


@pytest.mark.skipif(not _REAL.exists(), reason="real landed lookup not present (git-ignored data/)")

def test_reads_real_landed_qualificacoes_file(spark):
    df = read_csv_batch(spark, str(_REAL), "lookup")
    assert df.columns == ["codigo", "descricao"]
    rows = df.collect()
    assert len(rows) > 10                              # Qualificações has dozens of codes
    assert all(r.codigo is not None and r.codigo != "" for r in rows)
    # accented Portuguese must be intact (not mojibake) somewhere in the set
    assert any(any(ch in (r.descricao or "") for ch in "ãáçõéíêô") for r in rows)
