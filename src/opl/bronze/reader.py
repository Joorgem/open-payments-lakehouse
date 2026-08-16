"""HOW EACH CONTRACT'S BYTES ARE PARSED: the source format and the reader options,
keyed by contract.

The CSV options are shared by the local batch reader and the Databricks Auto Loader
stream, so both parse the RFB cp1252 files byte-identically. Auto Loader (cloudFiles)
is Databricks-only; the batch reader here is the local-testable twin.

TWO FORMATS SINCE F1b TASK 3, DISPATCHED ON THE CONTRACT AND NOWHERE ELSE. The RFB
ships headerless semicolon CSV because that is what it ships; the payment stream is
JSON Lines because the drift class this lakehouse must exhibit is a NEW OPTIONAL
COLUMN APPEARING MID-STREAM, which in CSV is a column-count change that a positional
reader either refuses outright or silently misaligns (`opl.contracts.payments` carries
the full argument). The PTAX record is JSON Lines for a third reason again: it is one
object per quote, built from a validated response rather than from a file BCB sent, and
a semicolon-delimited rendering of it would invent a dialect nobody publishes.

THE DISPATCH IS TOTAL OVER THE CATALOGUE SINCE F-API TASK 2, and was a default before
that. See the comment block above `_SOURCE_FORMATS` for what a defaulted format costs
and why the failure is invisible in both directions.

CONTRACT-KEYED, LIKE `rules_for` AND `struct_for`, and that is the seam rather than a
parameter on the stream. A format passed in by the caller would be a coordinate that
does not come from the resolved spec -- the exact shape every task test in this
repository refuses -- and an ingest entry point handed the wrong one reads JSON as CSV
and produces one string column of NULLs per row without erroring."""
from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from opl.bronze.schema import struct_for
from opl.contracts.catalogue import CONTRACT_COLUMNS
from opl.contracts.cnpj_schemas import CSV_DIALECT, TABLES
from opl.contracts.merchant import CONTRACT as MERCHANT_CONTRACT
from opl.contracts.payments import CONTRACT as PAYMENTS_CONTRACT
from opl.contracts.ptax import CONTRACT as PTAX_CONTRACT

# The `cloudFiles.format` / `spark.read.format` names. Constants because both the
# format string and the option set are chosen from them below, and a literal in one
# place and a constant in the other is how the two come apart.
CSV_FORMAT = "csv"
JSON_FORMAT = "json"


def csv_read_options() -> dict[str, str]:
    return {
        "header": "false",
        "sep": CSV_DIALECT["sep"],           # ";"
        "quote": CSV_DIALECT["quotechar"],   # '"'
        # RFC 4180 section 2.7: a quote inside a quoted field is escaped by
        # DOUBLING it, so `""` in the bytes means one literal `"` in the value.
        # The escape character IS the quote character in RFC 4180 -- hence
        # quotechar here, not a separate dialect key. Spark's `escape` defaults
        # to backslash, so without this RFB's `""` is never unescaped: the value
        # keeps its literal quotes and, never seeing the closing quote, can
        # swallow the `;` delimiter plus the following field's quotes (Spark's
        # default unescapedQuoteHandling is STOP_AT_DELIMITER). Real record
        # 13,730 of the inner CSV of Estabelecimentos6 is
        #   ...;"140";": ""A"";";"DUCILIA CARONE";...
        # which without `escape` landed as complemento='": ""A""' and
        # bairro='";"DUCILIA CARONE"' instead of ': "A";' and 'DUCILIA CARONE'.
        # Of the first 107,363 records, 13 carry a quote inside a field and all
        # 13 differed field-by-field from Python's csv.reader; with `escape`, 0.
        # A full byte scan of part 6 found 461 of 4,753,436 records whose
        # double-quote count deviates from 2x30 (e.g. `"RUA ""I"" S/N"`), so
        # ~4,600 records across the ten-part snapshot are affected.
        #
        # Swallowing the delimiter is not a theoretical risk: RFB uses `;` as an
        # internal separator inside complemento (`QUADRA 07;BLOCO 702A;APT 201`)
        # in 6,261 of those same 107,363 records.
        #
        # Pre-existing and independent of multiLine: output was identical under
        # multiLine=true and multiLine=false for all 30 fields of all 107,363
        # records. `unescapedQuoteHandling` is deliberately NOT set -- with
        # `escape` in place it changes nothing for any well-formed RFC 4180
        # shape, and the only inputs where its values diverge are genuinely
        # malformed records (a lone unbalanced quote) that no setting parses the
        # way csv.reader does; RAISE_ERROR would abort the job on them.
        "escape": CSV_DIALECT["quotechar"],   # '"' -- RFC 4180 section 2.7, above
        "encoding": CSV_DIALECT["encoding"],  # "cp1252" (Java alias of windows-1252)
        "mode": "PERMISSIVE",
        # RFC 4180 section 2.6: RFB also ships records with literal newlines
        # inside quoted fields, measured at 1 record in Estabelecimentos6 and 3
        # in Estabelecimentos8 (of 4,753,435 each). Spark's default multiLine=false
        # splits each one into a NULL-tailed "parent" (which satisfies every DQ
        # rule and gets promoted) plus a garbage "fragment"; the row count still
        # matches the source, so no count check can catch it.
        #
        # Cost, stated because it is real: with multiLine Spark cannot split one
        # file across tasks, so the unit of parallelism becomes the file, not the
        # 128 MB block. Accepted for this workload -- Estabelecimentos ships as
        # ten parts in total (nine of ~320-370 MB plus part 0 at 2,128,818,559 B),
        # enough files to keep the cluster busy, and the lookup files are
        # single-part and small enough that a file was already one task. Part 0 is
        # the one-huge-file case: 6.78 GB of CSV in a single task ingested
        # 29,093,533 rows in about nine minutes, so the ceiling is livable at this
        # scale. One run, nothing isolated -- a data point, not a benchmark; the
        # trade is correctness for a known parallelism ceiling (ADR 0005).
        "multiLine": "true",
    }


def jsonl_read_options() -> dict[str, str]:
    """The reader options for a JSON Lines source. Deliberately three.

    `multiLine=false` IS THE WHOLE FORMAT, and it is set rather than left to the
    default. JSON Lines means one complete JSON object per line; with `multiLine=true`
    Spark reads each FILE as a single JSON document and a 180-line stream parses as
    one malformed record. It is also the split-friendly half: `multiLine=true` makes
    the file the unit of parallelism, which `csv_read_options` accepts for the RFB
    giants because correctness forced it and which nothing here needs.

    `encoding=UTF-8` states what `opl.bronze.generated_landing` writes. The
    generator's serialiser (`events.to_jsonl`) returns TEXT and the writer encodes UTF-8
    explicitly and opens in binary, precisely so that the bytes on disk are the bytes
    the golden digest was taken over; declaring the encoding here is the reading half
    of that same statement. Left unset, Spark would detect -- and a detector that
    guessed wrong would turn a mis-encoded byte into a silently different STRING
    rather than into the U+FFFD that `rules._encoding_check` refuses.

    `mode=PERMISSIVE` matches the CSV side and is what makes `rescuedDataColumn` the
    reporting channel: a line the schema cannot absorb is rescued rather than dropped
    (FAILFAST) or nulled. That column firing IS the drift verdict -- `dq._reject_reason`
    ranks `rescued_data_present` above every per-table rule -- so nothing here may
    quietly discard the evidence.

    NO `columnNameOfCorruptRecord`: with `rescuedDataColumn` set, Auto Loader routes
    unparseable input and unexpected fields into that one column, and a second
    corrupt-record column would split one verdict across two places the gate reads
    differently."""
    return {
        "multiLine": "false",
        "encoding": "UTF-8",
        "mode": "PERMISSIVE",
    }


# WHICH FORMAT EACH CONTRACT'S BYTES ARE WRITTEN IN. A TOTAL MAPPING, not a test with a
# fallback, and that is F-API Task 2's change rather than a tidy-up.
#
# It was `JSON_FORMAT if contract == PAYMENTS_CONTRACT else CSV_FORMAT`, which is a
# DEFAULT wearing a conditional: every contract that is not payments got semicolon CSV,
# including every contract added afterwards. The failure that produces is silent in both
# halves. Spark IGNORES CSV options on a JSON read, so nothing reports the mismatch; and
# CSV over JSON Lines parses each line as one field -- the whole JSON object lands in the
# first declared column and the remaining columns are PERMISSIVE-padded NULL. The DQ gate
# then reports `null_or_empty_<second column>` and a triager starts from a blank column
# when the fault is the format dispatch, one layer up, for the whole table.
#
# So an unclassified contract RAISES. A KeyError, bare, matching `catalogue.columns_for`
# and `rules.rules_for`: this key is neither operator-supplied nor a table, and the guard
# below is what turns the possibility into an import-time refusal anyway.
#
# THE RFB HALF IS DERIVED FROM `TABLES`, not listed. "Semicolon CSV" is not a fact about
# four particular contracts, it is what the Receita ships -- so a fifth RFB layout gets
# the right answer by construction, and a contract that is NOT an RFB layout cannot get
# one by accident.
_SOURCE_FORMATS: dict[str, str] = {
    **{contract: CSV_FORMAT for contract in TABLES},
    PAYMENTS_CONTRACT: JSON_FORMAT,
    PTAX_CONTRACT: JSON_FORMAT,
    # The Postgres merchant snapshot: one JSON object per row, and JSON Lines for a fourth
    # reason again -- `trade_name` is the only nullable column of the contract and the
    # source produces BOTH a NULL and an empty string for it. CSV cannot tell them apart
    # without `nullValue`/`emptyValue` options that are a per-source dialect nobody
    # publishes; JSON spells one `null` and the other `""`. `_FORMAT_OPTIONS` is untouched:
    # this is a third contract on a format that already has its option set.
    MERCHANT_CONTRACT: JSON_FORMAT,
}


def _assert_every_contract_declares_a_format() -> None:
    """Fail at import if the mapping above is not exactly the catalogue's contracts.

    AT IMPORT rather than at the call, because the call is where it stops being visible:
    a contract missing from the mapping raises inside the ingest task, after the stream
    has been configured, and a contract in the mapping that no source declares is a dead
    entry that reads as coverage. The registry already refuses a table whose contract the
    catalogue does not know; this is the same totality one layer along, over the question
    "and how are its bytes parsed?".

    A plain ValueError: nothing here is an unknown table, and no operator supplied it."""
    missing = sorted(set(CONTRACT_COLUMNS) - set(_SOURCE_FORMATS))
    if missing:
        raise ValueError(
            f"{missing} are declared contracts with no source format, so an ingest of one "
            "raises after the stream is built. Every contract is parsed as SOMETHING, and "
            "the answer is not allowed to be a default: CSV options on a JSON read are "
            "ignored by Spark, and CSV over JSON Lines lands the whole object in the first "
            "column with the rest PERMISSIVE-padded NULL -- a blank column in the gate's "
            "report, one layer below the actual fault."
        )
    stray = sorted(set(_SOURCE_FORMATS) - set(CONTRACT_COLUMNS))
    if stray:
        raise ValueError(
            f"{stray} declare a source format and no source declares them as contracts "
            f"({', '.join(sorted(CONTRACT_COLUMNS))}). An entry for a contract that does "
            "not exist reads as coverage and parses nothing."
        )


_assert_every_contract_declares_a_format()


# AND WHICH OPTIONS EACH FORMAT TAKES. A TOTAL MAPPING FOR THE SAME REASON one level
# along, and it closes the residual the totality guard above did not reach: `read_options`
# was `jsonl if source_format(...) == JSON else csv`, so a THIRD format -- parquet, avro,
# text -- got the semicolon-CSV dialect from an `else`. That is the same default wearing a
# conditional the dispatch above was fixed for, one question later, and it fails just as
# quietly: Spark ignores CSV options on a read that is not CSV.
#
# The factories are values here rather than being called, because `csv_read_options` builds
# a fresh dict per call and a module-level dict would be one object every caller could
# mutate.
_FORMAT_OPTIONS: dict[str, Callable[[], dict[str, str]]] = {
    CSV_FORMAT: csv_read_options,
    JSON_FORMAT: jsonl_read_options,
}


def _assert_every_declared_format_has_options() -> None:
    """Fail at import if a format some contract declares has no option set, or vice versa.

    The mirror of `_assert_every_contract_declares_a_format`, over the second question: a
    contract is parsed as SOMETHING, and that something is read with options that MATCH it.
    A format with no options would raise inside the ingest task after the stream was built;
    an option set no contract's format names is a dead entry that reads as coverage.

    A plain ValueError: nothing here is an unknown table, and no operator supplied it."""
    declared = set(_SOURCE_FORMATS.values())
    if declared != set(_FORMAT_OPTIONS):
        raise ValueError(
            f"the formats contracts declare ({sorted(declared)}) are not the formats with "
            f"option sets ({sorted(_FORMAT_OPTIONS)}). Every format is read with options "
            "that match it, and the answer is not allowed to be a default: Spark IGNORES "
            "CSV options on a read that is not CSV, so a format falling through to the CSV "
            "dialect parses with Spark's own defaults and nothing says so."
        )


_assert_every_declared_format_has_options()


def source_format(contract: str) -> str:
    """The `cloudFiles.format` / reader format `contract`'s files are written in.

    KeyError if the contract declares none -- see the comment block above for why an
    unclassified contract must raise rather than fall through to CSV."""
    return _SOURCE_FORMATS[contract]


def read_options(contract: str) -> dict[str, str]:
    """The reader options for `contract`, matching `source_format(contract)`.

    ONE DISPATCH, ASKED TWICE, rather than two independent ones: this reads
    `source_format` instead of re-testing the contract, so a format and its options
    cannot come apart. They would fail in the quiet direction if they did -- CSV
    options on a JSON read are simply ignored by Spark, so the stream would parse with
    Spark's own defaults and nothing would say so.

    IT IS ALSO WHY WIDENING ONE WIDENS BOTH. `PTAX_CONTRACT` was added to
    `_SOURCE_FORMATS` and nothing here changed; had this function carried its own
    `contract in (A, B)` test, the two would have had to be edited together forever, and
    the edit that was forgotten would be the silent one.

    KEYED RATHER THAN CONDITIONAL since F-API's fix pass, and that is the same correction
    the format dispatch itself got: `jsonl if ... == JSON else csv` handed a third format
    the semicolon-CSV dialect from an `else`. Now a format with no option set raises, and
    the guard above makes it an import-time refusal instead."""
    return _FORMAT_OPTIONS[source_format(contract)]()


def read_csv_batch(spark: SparkSession, path: str, table: str) -> DataFrame:
    reader = spark.read.schema(struct_for(table))
    for k, v in csv_read_options().items():
        reader = reader.option(k, v)
    return reader.csv(path)
