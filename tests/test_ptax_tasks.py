# tests/test_ptax_tasks.py
"""The two PTAX job entry points, refusing before the network and before Spark.

Modelled on `tests/test_payment_emit.py`'s task section, and for its reason: these are
job scripts under `databricks/src`, not wheel modules, so they are loaded by path and
every refusal asserted here happens before a session is built or a request is made.

NOTHING HERE TOUCHES THE NETWORK. `fetch_ptax` takes its transport as an argument to
`fetch_series`, and every test below either stops before that call or injects a body.
The one function that would make a request -- `fetch_ptax.fetch` -- is exercised against
a double that records what it was handed."""
from __future__ import annotations

import hashlib
import importlib.util
from datetime import date
from pathlib import Path

import pytest

from opl.bronze.registry import LANDING_API, UnknownTable, table_spec
from opl.contracts.ptax import COLUMNS
from opl.extraction.ptax_source import QUOTED_CURRENCY, PtaxResponseRefused, quote_url

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"

# One real response body, in the endpoint's own shape, for 2026-06-19. The numbers are
# F-API Task 0's measurements as published in `docs/f-api-run-evidence.md`.
#
# WRITTEN AS RAW TEXT AND NOT BUILT WITH `json.dumps`, and the first version of this
# fixture got it wrong in exactly the way the phase documents. `json.dumps({"cotacaoVenda":
# 5.14420})` emits `5.1442` -- Python parsed the literal as a float and the trailing zero
# was gone before the body existed -- so the fixture would have been asserting that a
# digit BCB publishes is dropped, and calling that correct. The API sends the zero; this
# body has to send it too, or nothing downstream of it is being tested at all.
_BODY = (
    '{"@odata.context":"...","value":[{'
    '"cotacaoCompra":5.14360,'
    '"cotacaoVenda":5.14420,'
    '"dataHoraCotacao":"2026-06-19 13:03:25.555497"}]}'
)
_EMPTY_BODY = '{"@odata.context":"...","value":[]}'


def _load(name: str):
    """A `databricks/src` entry point, by path."""
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def fetcher():
    """A transport double: answers `_BODY` for 2026-06-19 and nothing for any other day.

    It records every URL, so a test can say which requests were made rather than only
    what came back -- and the one-request-per-quote-date shape is a claim about the
    requests, not about the result."""

    class _Fetch:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def __call__(self, url: str) -> str:
            self.urls.append(url)
            return _BODY if quote_url(date(2026, 6, 19)) == url else _EMPTY_BODY

    return _Fetch()


# --- both tasks refuse before they do anything ---------------------------------------


@pytest.mark.parametrize("script", ["fetch_ptax", "bronze_ptax_ingest"])
def test_an_unknown_table_is_refused_before_anything_else(script):
    """The table is resolved first in both, so a typo is answered by the registry naming
    the valid tables rather than by a serverless session or 42 round trips."""
    with pytest.raises(UnknownTable, match="ptax"):
        _load(script).main(["ptaxx"])  # a real typo


@pytest.mark.parametrize("script", ["fetch_ptax", "bronze_ptax_ingest"])
def test_a_table_that_is_not_api_fed_is_refused_by_both_ptax_tasks(script):
    """The pairing guard, in both directions.

    `fetch_ptax` handed a CNPJ table would write PTAX records into the directory that
    table's semicolon-CSV Auto Loader reads. `bronze_ptax_ingest` handed one would read
    an api-root directory nothing has ever written to and report SUCCESS having ingested
    zero rows -- indistinguishable from a month in which no file arrived.

    NOTHING FOLLOWS THE TABLE IN THIS ARGV, and that is the assertion rather than a
    saving: in both tasks this refusal is the FIRST thing after the spec is resolved, so
    the table can be refused before a month, a batch id or a window is even looked at.
    The two signatures diverge at position 1 -- the fetch takes the month there, the
    ingest a batch id -- so one shared argv that is "valid in both" no longer exists, and
    the `match` is what keeps a missing-month ValueError from passing as this one.

    `payments` rather than a CNPJ table for the same reason: it is the NEAREST wrong
    answer, being the other table nothing downloads, and it is the one a paste from the
    job this YAML was copied from would leave behind."""
    with pytest.raises(ValueError, match=LANDING_API):
        _load(script).main(["payments"])


def test_the_ptax_ingest_refuses_a_missing_batch_id_and_a_missing_month():
    from opl.bronze.promote import PromoteRefused

    module = _load("bronze_ptax_ingest")
    with pytest.raises(PromoteRefused, match="batch"):
        module.main(["ptax"])
    with pytest.raises(ValueError, match="no month was given"):
        module.main(["ptax", "12345"])


def test_the_fetch_refuses_a_missing_month():
    """The month is the fetch's ONE argument besides the table, and it has no default:
    it picks the directory the file is written into, and the ingest task that follows
    resolves its own source dir from the same job parameter."""
    module = _load("fetch_ptax")
    with pytest.raises(ValueError, match="no month was given"):
        module.main(["ptax"])


def test_the_window_is_declared_rather_than_launched_and_nothing_can_omit_it():
    """WHAT REPLACED TWO JOB PARAMETERS, asserted as the property they were removed for.

    `first`/`last` were job parameters defaulting to a sentinel `opl.config` refused, and
    that sentinel was argued from a lock in `tests/test_job_yaml_launch_guards.py` which
    nobody had written -- so the two YAML defaults were tied to nothing. The edit that
    costs something is on the YAML side: `default: "2026-06-03"` pasted in, exactly as
    `default: "2026-06"` once was for `month`, makes every `--params`-less run fetch a
    real window, land it under the filename that window derives, and then BLOCK the
    window that was wanted, because `emit_records_file` refuses to overwrite bytes that
    differ from the ones it derived.

    So there are three claims here and each one is a way that class stays closed:

      * the fetch takes NO window argument. A third argv element is ignored, which is
        what says the value cannot be supplied at launch at all.
      * the dates are `date` objects, so the API's own `MM-DD-YYYY` spelling -- the
        refusal `require_quote_date` existed for -- is not expressible in them.
      * they are the range F-API Task 0 measured, pinned here rather than in a launch
        command, so a change to either is a diff before a run rather than a retyped
        argument after one."""
    from opl.extraction.ptax_window import WINDOW_FIRST, WINDOW_LAST

    module = _load("fetch_ptax")
    assert (WINDOW_FIRST, WINDOW_LAST) == (date(2026, 6, 3), date(2026, 8, 1))
    assert isinstance(WINDOW_FIRST, date) and isinstance(WINDOW_LAST, date)
    assert not hasattr(module, "require_quote_date"), (
        "the window is declared, so nothing in this task parses one out of argv"
    )
    source = (_SRC / "fetch_ptax.py").read_text(encoding="utf-8")
    assert "args[2]" not in source and "args[3]" not in source, (
        "fetch_ptax.py reads a third argument again, which is how the window becomes a "
        "job parameter with a default nobody checks"
    )


# --- the fetch's own pieces ----------------------------------------------------------


def test_the_whole_task_wires_the_window_the_month_and_the_record_together(
    monkeypatch, fetcher
):
    """`main` END TO END, hermetically: the real `fetch_series`, the real record builder
    and the real filename, with only the transport and the file write replaced.

    WHAT THIS SAYS THAT THE PIECES CANNOT SAY SEPARATELY is which value reaches which
    consumer. The WINDOW decides what is requested and what the file is called; the MONTH
    decides which directory it lands in, and the ingest task that follows resolves its own
    source dir from the same job parameter. A task that swapped them would pass every
    unit test above and land June's quotes where nothing reads them.

    Three requests for a three-day window, inclusive -- one per quote date, because a
    range answers with rows carrying no attributable quote date. Only 2026-06-19 has one
    in this double, so a task that had made ONE wide request would also produce one
    record; the URL list is what tells the two apart.

    THE WINDOW IS NARROWED HERE RATHER THAN TAKEN FROM THE DECLARATION, and the narrowing
    is what makes the URL list a readable assertion instead of sixty lines. The declared
    window is pinned by `test_the_window_is_declared_rather_than_launched_...`; what this
    test is about is that whatever the module declares is what reaches the request, the
    filename and the directory -- so it patches the module's own constants and asserts
    all three followed."""
    module = _load("fetch_ptax")
    monkeypatch.setattr(module, "WINDOW_FIRST", date(2026, 6, 18))
    monkeypatch.setattr(module, "WINDOW_LAST", date(2026, 6, 20))
    monkeypatch.setattr(module, "fetch", fetcher)
    captured: dict[str, object] = {}

    def _emit(records, filename, *, directory, tmp_directory):
        captured.update(
            records=records, filename=filename, directory=directory, tmp=tmp_directory
        )
        return module.EmittedFile(
            path=f"{directory}/{filename}",
            row_count=len(records),
            byte_count=0,
            sha256="",
            was_already_there=False,
        )

    monkeypatch.setattr(module, "emit_records_file", _emit)
    module.main(["ptax", "2026-08"])

    assert len(fetcher.urls) == 3, "three calendar days, three requests, inclusive"
    assert fetcher.urls == [quote_url(date(2026, 6, d)) for d in (18, 19, 20)]
    assert captured["filename"] == "usd-2026-06-18_2026-06-20.jsonl"
    assert captured["directory"].endswith("/api/2026-08/ptax")
    assert captured["tmp"].endswith("/_tmp/api/2026-08/ptax")
    assert captured["records"] == [
        {
            "quote_date": "2026-06-19",
            "currency": "USD",
            "data_hora_cotacao": "2026-06-19 13:03:25.555497",
            "cotacao_compra": "5.14360",
            "cotacao_venda": "5.14420",
        }
    ]


def test_the_record_is_the_five_contract_columns_in_contract_order(fetcher):
    """The crossing into text, pinned.

    `data_hora_cotacao` is the API's OWN string and not this repository's reading of it:
    `published_at` carries a Brasilia-time decision, and landing that would put a zone
    ruling into bronze as though BCB had sent it. Both rates keep every digit BCB
    published, trailing zero included -- which is the whole reason the extraction parses
    with `Decimal` from the raw text."""
    from opl.extraction.ptax_source import fetch_quote

    module = _load("fetch_ptax")
    quote = fetch_quote(date(2026, 6, 19), fetcher)
    record = module.record_of(quote)
    assert list(record) == list(COLUMNS), "the contract's order decides the emitted bytes"
    assert record == {
        "quote_date": "2026-06-19",
        "currency": "USD",
        "data_hora_cotacao": "2026-06-19 13:03:25.555497",
        "cotacao_compra": "5.14360",
        "cotacao_venda": "5.14420",
    }
    assert all(isinstance(value, str) and value for value in record.values())


def test_the_landing_filename_is_the_window_and_the_currency_and_nothing_else():
    """Auto Loader tracks files by PATH, so a run id, a month or a timestamp in this name
    would make every re-run a NEW file holding the same quotes -- ingested again under a
    fresh `_batch_id`, which the promote's idempotence cannot see.

    It is also one path component: a `/` here would put the file outside the landing dir
    the ingest reads."""
    module = _load("fetch_ptax")
    name = module.filename_for(QUOTED_CURRENCY, date(2026, 6, 3), date(2026, 8, 1))
    assert name == "usd-2026-06-03_2026-08-01.jsonl"
    assert "/" not in name and "\\" not in name
    # The same window twice is the same name; a different window is a different one.
    assert name == module.filename_for(QUOTED_CURRENCY, date(2026, 6, 3), date(2026, 8, 1))
    assert name != module.filename_for(QUOTED_CURRENCY, date(2026, 6, 4), date(2026, 8, 1))


def test_a_window_the_series_has_nothing_in_is_refused_rather_than_landed_empty(
    monkeypatch, fetcher
):
    """An empty file in the landing dir is ingested cleanly, rejects nothing, promotes
    nothing, and every task reports SUCCESS having moved zero rows -- and it then OWNS
    the filename this window derives, so the correct fetch of the same window is refused
    by the emitter ever afterwards.

    One day with no quote is normal (weekends, holidays) and is not this: the refusal is
    about a whole window, which is a property of the request.

    IT IS REFUSED ONE LAYER DOWN, in `ptax_source.fetch_series`, and asserted here through
    `main` rather than against a helper of this task's own. This file used to carry a copy;
    the condition was identical, so once the extraction layer refused it the local one could
    never fire. Through `main` the test says what actually matters -- that the task cannot
    reach `emit_records_file` with nothing to write -- and it would still hold if the
    refusal moved again."""
    module = _load("fetch_ptax")
    # A weekend, patched in for the reason the wiring test above patches its window: the
    # DECLARED window has 42 quotes in it, so the only way to reach this refusal through
    # `main` is to declare a window that has none.
    monkeypatch.setattr(module, "WINDOW_FIRST", date(2026, 6, 20))
    monkeypatch.setattr(module, "WINDOW_LAST", date(2026, 6, 21))
    monkeypatch.setattr(module, "fetch", fetcher)
    monkeypatch.setattr(
        module,
        "emit_records_file",
        lambda *a, **k: pytest.fail("an empty window reached the landing writer"),
    )
    with pytest.raises(PtaxResponseRefused, match="no quote at all"):
        module.main(["ptax", "2026-08"])
    assert len(fetcher.urls) == 2, "both days were asked before the window was refused"


def _reported_numbers(payload: bytes) -> tuple[str, ...]:
    """The three `rows=`/`bytes=`/`sha256=` fragments `_report` must print for `payload`.

    DERIVED FROM THE BYTES, never from a literal: these are the numbers `EmittedFile`
    carries, so computing them here from the file on disk is what makes the run log a
    measurement of the artefact rather than a restatement of what the emitter believed. One
    record per line, `\\n`-terminated, so the line count IS the row count."""
    return (
        f"rows={payload.count(b'\n')}",
        f"bytes={len(payload)}",
        f"sha256={hashlib.sha256(payload).hexdigest()}",
    )


# --- THE ONE TEST BELOW THAT WRITES A REAL FILE, AND WHY ITS PROSE IS UP HERE ---------
#
# Module level for `opl.bronze.rules`' reason: with the reasoning inside it the function
# stood at 54 lines against this project's 50-line limit, and the fix pass that closed its
# tautology added assertions rather than removing any.
#
# THE REFACTOR'S ENTIRE CONTENT, EXERCISED -- and until this test nothing was.
# `emit_records_file`'s `filename` parameter is the only thing that separates it from
# `emit_stream_file`: F-API Task 2 generalised the payment stream's emitter by lifting the
# name out of it. But the payment tests call the WRAPPER, which supplies its own name, and
# every PTAX test above monkeypatches `emit_records_file` away -- so on a phase whose
# subject is bytes on disk, no test wrote a PTAX record set to a file at all. A
# `filename_for` that returned a constant, a path separator or the payment stream's own name
# would have passed the whole suite.
#
# So this one writes. Real directories, the real emitter, the real serialiser, with only the
# transport replaced -- and it asserts the four things that make a landed file a landed file:
#
#   * the file exists at the derived name, inside the landing dir and not the tmp one;
#   * its BYTES are the serialised records, read back in binary, `\n`-terminated with no
#     carriage return -- the property this module opens in binary for;
#   * the staging directory is left empty, because the payload is `os.replace`d in;
#   * the REPORTED row count, byte count and digest are the file's own.
#
# THE FOURTH ONE WAS A TAUTOLOGY UNTIL THIS PASS, and it is the species this phase keeps
# finding. It read `sha256(payload) == sha256(landed.read_bytes())` with `payload` already
# bound to `landed.read_bytes()` -- the same bytes hashed twice, true under every
# implementation, including one whose `EmittedFile` reported zeros. Nothing anywhere
# compared `EmittedFile.sha256` or `.byte_count` against a file, so the docstring's claim
# that a run log's numbers and a local assertion compare directly was unexercised.
#
# IT IS NOW ASSERTED THROUGH THE RUN LOG ITSELF rather than against the dataclass, because
# the log line is what Task 5 quotes: `_report` prints `EmittedFile`'s three numbers, and
# `capsys` is where a workspace run's evidence and this file's `read_bytes()` meet. All
# three expected values are DERIVED from the bytes on disk -- no literal digest lives here,
# which is the same decision `test_a_bare_time_resolves_to_TODAYS_DATE...` made about a
# literal date.
#
# A SECOND CALL IS THE IDEMPOTENCE HALF, and it is what a repair run of this job does: the
# same window derives the same name and the same bytes, so the emitter reports the file as
# already present, byte-identical, with the same three numbers and no rewrite.


def test_the_records_reach_a_REAL_FILE_under_the_filename_this_task_derived(
    monkeypatch, fetcher, tmp_path, capsys
):
    """The real emitter, the real serialiser, real directories, and the run log's own
    numbers checked against the bytes. See the comment block above."""
    module = _load("fetch_ptax")
    monkeypatch.setattr(module, "WINDOW_FIRST", date(2026, 6, 18))
    monkeypatch.setattr(module, "WINDOW_LAST", date(2026, 6, 20))
    monkeypatch.setattr(module, "fetch", fetcher)
    landing, staging = tmp_path / "api" / "ptax", tmp_path / "_tmp" / "api" / "ptax"
    monkeypatch.setattr(module, "landing_dir", lambda *a: str(landing))
    monkeypatch.setattr(module, "landing_tmp_dir", lambda *a: str(staging))

    module.main(["ptax", "2026-08"])

    landed = landing / "usd-2026-06-18_2026-06-20.jsonl"
    assert [path.name for path in landing.iterdir()] == [landed.name]
    payload = landed.read_bytes()
    assert payload == (
        b'{"quote_date":"2026-06-19","currency":"USD",'
        b'"data_hora_cotacao":"2026-06-19 13:03:25.555497",'
        b'"cotacao_compra":"5.14360","cotacao_venda":"5.14420"}\n'
    )
    assert b"\r" not in payload
    assert not list(staging.iterdir()), "the payload is replaced in, never left staged"
    # The three numbers `_report` printed, against the file they describe.
    reported = capsys.readouterr().out
    for number in _reported_numbers(payload):
        assert number in reported, f"the run log does not carry {number} for the file it wrote"

    # The repair run: same window, same bytes, same numbers, no rewrite.
    module.main(["ptax", "2026-08"])
    assert landed.read_bytes() == payload
    repaired = capsys.readouterr().out
    assert "already present, byte-identical" in repaired
    for number in _reported_numbers(payload):
        assert number in repaired, f"the repair run reports {number} differently"


def test_a_non_200_is_a_refusal_and_never_an_empty_day(monkeypatch):
    """THE CONSTRAINT THE EXTRACTION LAYER PLACES ON ITS CALLERS, met here.

    2026-06-20 answers HTTP 200 with an empty row list because it is a Saturday, and
    that is indistinguishable from an HTML interstitial or an OData error to a forgiving
    reader. T3 resolves an absence by falling back over the series, so a failure read as
    an absence makes the fallback silently cross a hole. The status is checked before the
    body is ever handed on.

    The request is recorded rather than only refused, so this also says the task sends a
    named User-Agent and a bounded timeout: an anonymous caller makes a 403 from a
    governed egress path impossible to attribute, and an unbounded read on a task with
    `max_retries: 0` hangs the job rather than failing it."""
    module = _load("fetch_ptax")
    seen: dict[str, object] = {}

    class _Response:
        status_code = 503
        text = "<html>gateway</html>"

    def _get(url, headers=None, timeout=None):
        seen.update(url=url, headers=headers, timeout=timeout)
        return _Response()

    monkeypatch.setattr(module.requests, "get", _get)
    with pytest.raises(RuntimeError, match="503"):
        module.fetch(quote_url(date(2026, 6, 19)))
    assert "open-payments-lakehouse" in seen["headers"]["User-Agent"]
    assert isinstance(seen["timeout"], int) and seen["timeout"] > 0


def test_a_body_that_is_not_the_odata_envelope_is_refused_by_the_extraction_layer(tmp_path):
    """The other half of the same property, and it belongs to `ptax_source` rather than
    to this task -- asserted here so the pairing is visible from the entry point that
    would otherwise treat a refusal as no quote."""
    from opl.extraction.ptax_source import fetch_quote

    with pytest.raises(PtaxResponseRefused):
        fetch_quote(date(2026, 6, 19), lambda url: "<html>interstitial</html>")


def test_the_registered_ptax_table_is_the_one_these_tasks_are_built_for():
    """Guard the guard: every refusal above is about a table that is NOT api-fed, so all
    of them would still pass if `ptax` itself had stopped being one -- with both entry
    points then refusing the only table they exist to serve."""
    assert table_spec("ptax").landing == LANDING_API
