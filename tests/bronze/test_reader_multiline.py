# tests/bronze/test_reader_multiline.py
"""Regression tests for RFC 4180 quoted-field parsing in the bronze CSV reader.
Both halves of the standard bit us on the real RFB extracts, and both are
silent: every DQ rule passes and the row count reconciles, so damaged rows get
promoted.

Section 2.6 -- a quoted field may contain a literal newline. Spark defaults to
``multiLine=false``, which splits such a record into two rows: a "parent"
holding the leading fields with every trailing column NULL (passes all DQ
rules, gets promoted) plus a "fragment" starting mid-field. Measured with
Python's ``csv`` module on the real 2026-06 extracts: 1 such record in
Estabelecimentos6 and 3 in Estabelecimentos8, out of 4,753,435 records each.
Surfaced in F1.3 runs 5 and 6 and diagnosed after run 6 (run 1 was a different
incident -- a short-written upload and the unzip failure that followed).

Section 2.7 -- a quote inside a quoted field is escaped by DOUBLING it (``""``
means one literal ``"``). Spark's ``escape`` defaults to backslash, so RFB's
``""`` was never unescaped: the value kept its literal quotes and, with Spark's
default ``unescapedQuoteHandling=STOP_AT_DELIMITER``, could swallow the ``;``
delimiter plus the following field's quotes. A full byte scan of part 6 found
461 of 4,753,436 records whose double-quote count deviates from 2x30. This one
is pre-existing and independent of the ``multiLine`` flip.

Record separator: the real extracts are LF-only -- a full scan of the inner CSV
of Estabelecimentos6.zip counted 4,753,436 LF and **zero** CR bytes, and all six
landed lookup files are likewise CR-free. Fixtures here are LF by default;
``test_crlf_record_separator_also_parses`` pins that the reader would still cope
if RFB ever changed that, but CRLF is not what RFB ships.

Exercised through ``read_csv_batch``: the local-testable twin of the Auto
Loader stream, sharing ``csv_read_options()`` with it (see reader.py)."""
from __future__ import annotations

import csv
import io

from opl.bronze.reader import read_csv_batch
from opl.contracts.cnpj_schemas import columns_for

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


def _write_cp1252_csv(path, records: list[str], newline: str = "\n") -> None:
    """cp1252 bytes with an LF record separator -- what RFB actually ships
    (zero CR bytes across all 4,753,436 records of part 6). ``newline`` is
    overridable only so one test can prove CRLF would also parse."""
    path.write_bytes((newline.join(records) + newline).encode("cp1252"))


def _expected_fields(record: str) -> list[str | None]:
    """RFC 4180 ground truth via Python's ``csv``, whose defaults (``doublequote``
    on, no escapechar) are exactly the RFB dialect. Quoted empty fields read as
    NULL in Spark (default ``nullValue`` is ``""``), so normalise for comparison."""
    fields = next(csv.reader(io.StringIO(record, newline=""), delimiter=";", quotechar='"'))
    assert len(fields) == len(_ESTAB_COLUMNS)
    return [None if f == "" else f for f in fields]


# --- RFC 4180 section 2.6: literal newline inside a quoted field -------------


def test_quoted_field_with_embedded_newline_stays_one_record(spark, tmp_path):
    # Modeled on part 6 record 4,266,421: nome_fantasia == 'RIZZ CAMPOLIM\n'.
    broken = _estab_row("02546226", "RIZZ CAMPOLIM\n")
    f = tmp_path / "K3241.K03200Y6.D60613.ESTABELE"
    _write_cp1252_csv(f, [
        _estab_row("12345678", "PADARIA AÇAÍ"),
        broken,
        _estab_row("87654321", "MERCADO SÃO JOÃO"),
    ])

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


def test_lookup_quoted_field_with_embedded_newline_stays_one_record(spark, tmp_path):
    # Same latent defect on the F1.2 lookup shape. No landed lookup is known to
    # contain an embedded newline, so this guards the shared option, not a
    # reproduced incident.
    f = tmp_path / "F.K03200$Z.D60613.CNAECSV"
    _write_cp1252_csv(f, [
        '"01";"AÇÃO E TECNOLOGIA"',
        '"02";"SERVIÇOS DE\nESCRITÓRIO"',
        '"03";"SÃO JOÃO — SERVIÇOS"',
    ])

    rows = read_csv_batch(spark, str(f), "lookup").collect()
    assert len(rows) == 3, f"record split: {[r.codigo for r in rows]}"
    recs = {r.codigo: r.descricao for r in rows}
    assert recs["02"] == "SERVIÇOS DE\nESCRITÓRIO"
    assert recs["03"] == "SÃO JOÃO — SERVIÇOS"   # cp1252 0x80-0x9F still decodes


def test_crlf_record_separator_also_parses(spark, tmp_path):
    """RFB ships LF-only, but RFC 4180 section 2.1 specifies CRLF and nothing
    stops RFB from switching. Pinned so a future change of separator is a
    non-event; do NOT read this as a description of the current source bytes."""
    f = tmp_path / "K3241.K03200Y6.D60613.ESTABELE"
    _write_cp1252_csv(f, [
        _estab_row("12345678", "PADARIA AÇAÍ"),
        _estab_row("87654321", "MERCADO SÃO JOÃO"),
    ], newline="\r\n")

    rows = read_csv_batch(spark, str(f), "estabelecimentos").collect()
    assert len(rows) == 2
    by_key = {r.cnpj_basico: r for r in rows}
    assert set(by_key) == {"12345678", "87654321"}
    # A stray CR must not ride along on the last field of a record.
    assert by_key["12345678"].data_situacao_especial == "20260131"
    assert by_key["87654321"].nome_fantasia == "MERCADO SÃO JOÃO"


# --- RFC 4180 section 2.7: doubled quote inside a quoted field ---------------

# Verbatim record 13,730 of the inner CSV of
# data/cnpj/2026-06/giants/Estabelecimentos6.zip. The sharpest case in the file:
# complemento is `: ""A"";` -- a doubled quote AND a `;` inside the value, so a
# reader that misses section 2.7 swallows the delimiter and corrupts the NEXT
# field too.
_REAL_DELIM_SMUGGLER = (
    '"09516882";"0001";"17";"1";"";"08";"20130917";"01";"";"";"20080303";'
    '"3314719";"3321000,3314799";"RUA";"GERALDO PEIXOTO FILHO";"140";'
    '": ""A"";";"DUCILIA CARONE";"36520000";"MG";"5441";"32";"35511805";"32";'
    '"35513353";"32";"35513353";"VISAO@KONET.COM.BR";"";""'
)
# Verbatim record from the same file (cnpj_basico 09730578): the common shape,
# a doubled quote in the middle of free text. Accents kept as shipped, which
# also proves the fix does not disturb cp1252 decoding.
_REAL_COMMON = (
    '"09730578";"0001";"78";"1";"";"08";"20081231";"01";"";"";"20080706";'
    '"9492800";"";"RUA";"RUA ""I"" S/N - ENFRENTE A IGREJA CATÓLICA";"S/N";"";'
    '"SALLES DE OLIVEIRA";"87345000";"PR";"7475";"0";"0";"";"";"0";"0";"";"";""'
)
# Verbatim record from the same file (cnpj_basico 09980568): doubled quote plus a
# cp1252 high byte (0xBA, masculine ordinal) in the same field.
_REAL_HIGH_BYTE = (
    '"09980568";"0001";"90";"1";"";"08";"20081231";"01";"";"";"20080710";'
    '"9492800";"";"QUADRA";"QUADRA 40 BLOCO ""K"" APTº 01";"01";'
    '"CONDOMINIO VILLE BLANCHE II";"ESPLANADA III";"72876340";"GO";"1066";"0";'
    '"0";"";"";"0";"0";"varelaalexandre1@gmail.com";"";""'
)
# Verbatim record from the same file (cnpj_basico 09747664): three doubled
# quotes in one field, one of them adjacent to the closing quote.
_REAL_MULTI = (
    '"09747664";"0001";"93";"1";"";"08";"20081231";"01";"";"";"20080706";'
    '"9492800";"";"RUA";"RUA TAMBURI";"S/N°";"QUADRA ""02\' LOTE ""15""";'
    '"CENTRO";"77458000";"TO";"0335";"0";"0";"";"";"0";"0";"";"";""'
)
# Verbatim record from the same file (cnpj_basico 09921938): the doubled quote
# sits flush against the field's closing quote -- three quotes in a row.
_REAL_TRAILING = (
    '"09921938";"0001";"19";"1";"";"08";"20081231";"01";"";"";"20080709";'
    '"9492800";"";"RUA";"RUA ""D""";"101";"CASA";"VILA UNIÃO";"39925000";"MG";'
    '"5141";"0";"0";"";"";"0";"0";"edmilson.a.s@gmail.com";"";""'
)
# Verbatim record from the same file (cnpj_basico 10085335): the doubled quote
# OPENS the field -- `"""SUCUPIRA"` is one literal quote then SUCUPIRA.
_REAL_LEADING = (
    '"10085335";"0001";"03";"1";"";"08";"20081231";"01";"";"";"20080709";'
    '"9492800";"";"RUA";"RUA URUGUAI";"202";"CASA Ä";"""SUCUPIRA";"54480280";'
    '"PE";"2457";"0";"0";"";"";"0";"0";"";"";""'
)


def test_doubled_quote_in_field_does_not_swallow_the_delimiter(spark, tmp_path):
    """RFC 4180 section 2.7 on the real record that smuggles a ``;`` into a value.

    Against the pre-fix options (no ``escape``) this produced
    complemento == '": ""A""' and bairro == '";"DUCILIA CARONE"' -- the value
    kept its literal quotes and absorbed the delimiter plus the next field's
    quotes. Both rows reconcile and pass every DQ rule, so nothing else catches
    it."""
    f = tmp_path / "K3241.K03200Y6.D60613.ESTABELE"
    _write_cp1252_csv(f, [_estab_row("12345678", "PADARIA AÇAÍ"), _REAL_DELIM_SMUGGLER])

    rows = read_csv_batch(spark, str(f), "estabelecimentos").collect()
    assert len(rows) == 2, f"record split: {[r.cnpj_basico for r in rows]}"
    hit = next(r for r in rows if r.cnpj_basico == "09516882")
    # The two fields the defect corrupted, asserted as exact values.
    assert hit.complemento == ': "A";'
    assert hit.bairro == "DUCILIA CARONE"
    # The delimiter was absorbed, so every later field shifted too.
    assert hit.cep == "36520000"
    assert hit.uf == "MG"
    assert hit.correio_eletronico == "VISAO@KONET.COM.BR"


def test_real_doubled_quote_records_match_rfc4180_field_for_field(spark, tmp_path):
    """All 30 fields of six verbatim part-6 records, against Python's ``csv`` as
    the RFC 4180 oracle. Covers every doubled-quote position the real file
    contains: mid-text, beside a cp1252 high byte, three in one field, flush
    against the closing quote, and opening the field."""
    records = [
        _REAL_COMMON, _REAL_HIGH_BYTE, _REAL_MULTI, _REAL_TRAILING,
        _REAL_LEADING, _REAL_DELIM_SMUGGLER,
    ]
    expected = {r[0]: r for r in (_expected_fields(rec) for rec in records)}
    f = tmp_path / "K3241.K03200Y6.D60613.ESTABELE"
    _write_cp1252_csv(f, records)

    rows = read_csv_batch(spark, str(f), "estabelecimentos").collect()
    assert len(rows) == len(records), f"record split: {[r.cnpj_basico for r in rows]}"
    # Diff built by hand, and across all rows before asserting, so a failure
    # names every offending record and column and prints both values; a bare
    # list-vs-list assert truncates them and stops at the first bad row.
    diffs = [
        f"{row.cnpj_basico}.{c}: got {g!r} want {e!r}"
        for row in rows
        for c, g, e in zip(
            _ESTAB_COLUMNS,
            [row[col] for col in _ESTAB_COLUMNS],
            expected[row.cnpj_basico],
            strict=True,   # all three are the 30 contract columns
        )
        if g != e
    ]
    assert not diffs, "deviates from RFC 4180 -- " + "; ".join(diffs)
    # Spot-check the headline values so a broken oracle cannot make this pass.
    by_key = {r.cnpj_basico: r for r in rows}
    assert by_key["09730578"].logradouro == 'RUA "I" S/N - ENFRENTE A IGREJA CATÓLICA'
    assert by_key["09980568"].logradouro == 'QUADRA 40 BLOCO "K" APTº 01'
    assert by_key["09747664"].complemento == 'QUADRA "02\' LOTE "15"'
    assert by_key["09921938"].logradouro == 'RUA "D"'
    assert by_key["10085335"].bairro == '"SUCUPIRA'


def test_lookup_doubled_quote_in_field(spark, tmp_path):
    """Section 2.7 on the F1.2 lookup shape. No landed lookup contains a doubled
    quote -- a full byte count of all six shows exactly 4 quotes per record, i.e.
    2 per field and none inside a value -- so this guards the shared option
    against a future lookup that does, and pins that the option is harmless."""
    f = tmp_path / "F.K03200$Z.D60613.CNAECSV"
    _write_cp1252_csv(f, [
        '"01";"AÇÃO E TECNOLOGIA"',
        '"02";"RUA ""I"" — SERVIÇOS"',
        '"03";": ""A"";"',                  # doubled quote plus an in-value ";"
    ])

    rows = read_csv_batch(spark, str(f), "lookup").collect()
    assert len(rows) == 3, f"record split: {[r.codigo for r in rows]}"
    recs = {r.codigo: r.descricao for r in rows}
    assert recs["01"] == "AÇÃO E TECNOLOGIA"      # unquoting is unchanged
    assert recs["02"] == 'RUA "I" — SERVIÇOS'
    assert recs["03"] == ': "A";'
