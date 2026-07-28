# tests/bronze/test_reader_multiline.py
"""Regression tests for the F1.3 run-1 bronze incident: RFB CSV files contain
literal newlines inside quoted fields (valid CSV per RFC 4180), and Spark's CSV
reader defaults to ``multiLine=false``, which splits such a record into two
rows -- a "parent" holding the leading fields with every trailing column NULL
(passes all DQ rules, gets promoted) and a "fragment" starting mid-field.

Measured on the real 2026-06 extracts with Python's ``csv`` module: 1 such
record in Estabelecimentos6 and 3 in Estabelecimentos8, out of 4,753,435
records each. Bronze therefore held the *correct* row count while carrying
damaged rows, so no count check could catch it -- hence these tests assert row
identity and trailing-column presence, never just ``count()``.

Exercised through ``read_csv_batch``: the local-testable twin of the Auto
Loader stream, sharing ``csv_read_options()`` with it (see reader.py)."""
from __future__ import annotations

from opl.bronze.reader import read_csv_batch
from opl.contracts.cnpj_schemas import columns_for
from opl.spark import local_session

_ESTAB_COLUMNS = columns_for("estabelecimentos")  # 30 columns, order is authoritative


def _estab_row(cnpj_basico: str, nome_fantasia: str) -> str:
    """One RFB-shaped Estabelecimentos record: 30 fields, every field quoted,
    ``;`` separated. Only the two columns the assertions read vary."""
    values = [
        cnpj_basico, "0001", "95", "1", nome_fantasia, "02", "20260101", "00",
        "", "", "20200101", "4712100", "5611203", "RUA", "DAS FLORES", "100",
        "ANDAR 16 TORRE 2 SALA 2", "CENTRO", "01001000", "SP", "7107", "11",
        "40041234", "", "", "", "", "CONTATO@EXEMPLO.COM.BR", "", "20260131",
    ]
    assert len(values) == len(_ESTAB_COLUMNS)
    return ";".join(f'"{v}"' for v in values)


def _write_cp1252_csv(path, records: list[str]) -> None:
    # \r\n record separator + cp1252 bytes: exactly how RFB ships these files.
    path.write_bytes(("\r\n".join(records) + "\r\n").encode("cp1252"))


def test_quoted_field_with_embedded_newline_stays_one_record(tmp_path):
    # Modeled on part 6 record 4,266,421: nome_fantasia == 'RIZZ CAMPOLIM\n'.
    broken = _estab_row("02546226", "RIZZ CAMPOLIM\n")
    f = tmp_path / "K3241.K03200Y6.D60613.ESTABELE"
    _write_cp1252_csv(f, [
        _estab_row("12345678", "PADARIA AÇAÍ"),
        broken,
        _estab_row("87654321", "MERCADO SÃO JOÃO"),
    ])

    spark = local_session("test-multiline-estab")
    try:
        rows = read_csv_batch(spark, str(f), "estabelecimentos").collect()
        # 3 records in, 3 records out: the embedded newline must NOT split a row.
        assert len(rows) == 3, \
            f"record split: {[r.cnpj_basico for r in rows]}"
        by_key = {r.cnpj_basico: r for r in rows}
        assert set(by_key) == {"12345678", "02546226", "87654321"}
        hit = by_key["02546226"]
        assert hit.nome_fantasia == "RIZZ CAMPOLIM\n"   # newline preserved in-field
        # The tell of the incident: on a split record every trailing column of
        # the surviving "parent" row is NULL, which passes every DQ rule.
        assert hit.data_situacao_especial == "20260131"
        assert hit.uf == "SP"
        assert hit.correio_eletronico == "CONTATO@EXEMPLO.COM.BR"
        # Strongest form of the same check: the record carrying the newline must
        # have the *same* null pattern as a clean record. (Quoted empty fields
        # read as NULL -- Spark's default nullValue is "" -- so "no nulls at
        # all" is not the invariant; "no nulls a clean row does not have" is.)
        nulls = [c for c in _ESTAB_COLUMNS if hit[c] is None]
        clean_nulls = [c for c in _ESTAB_COLUMNS if by_key["12345678"][c] is None]
        assert nulls == clean_nulls
    finally:
        spark.stop()


def test_lookup_quoted_field_with_embedded_newline_stays_one_record(tmp_path):
    # Same latent defect on the F1.2 lookup shape. No landed lookup is known to
    # contain an embedded newline, so this guards the shared option, not a
    # reproduced incident.
    f = tmp_path / "F.K03200$Z.D60613.CNAECSV"
    _write_cp1252_csv(f, [
        '"01";"AÇÃO E TECNOLOGIA"',
        '"02";"SERVIÇOS DE\nESCRITÓRIO"',
        '"03";"SÃO JOÃO — SERVIÇOS"',
    ])

    spark = local_session("test-multiline-lookup")
    try:
        rows = read_csv_batch(spark, str(f), "lookup").collect()
        assert len(rows) == 3, f"record split: {[r.codigo for r in rows]}"
        recs = {r.codigo: r.descricao for r in rows}
        assert recs["02"] == "SERVIÇOS DE\nESCRITÓRIO"
        assert recs["03"] == "SÃO JOÃO — SERVIÇOS"   # cp1252 0x80-0x9F still decodes
    finally:
        spark.stop()
