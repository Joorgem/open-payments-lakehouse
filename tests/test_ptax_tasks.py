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

    The remaining argv is valid in both, so the landing refusal is the only one that can
    fire. `payments` rather than a CNPJ table for the same reason: it is the NEAREST
    wrong answer, being the other table nothing downloads, and it is the one a paste from
    the job this YAML was copied from would leave behind."""
    with pytest.raises(ValueError, match=LANDING_API):
        _load(script).main(["payments", "2026-08", "2026-06-03", "2026-08-01"])


def test_the_ptax_ingest_refuses_a_missing_batch_id_and_a_missing_month():
    from opl.bronze.promote import PromoteRefused

    module = _load("bronze_ptax_ingest")
    with pytest.raises(PromoteRefused, match="batch"):
        module.main(["ptax"])
    with pytest.raises(ValueError, match="no month was given"):
        module.main(["ptax", "12345"])


def test_the_fetch_refuses_a_missing_month_and_each_missing_window_bound():
    """In order: the month is validated before either bound, and `first` before `last`,
    so each argv can only fail on the one argument it omits.

    The message names WHICH bound, because an operator with two date parameters and a
    complaint that says only "not a date" has to guess."""
    module = _load("fetch_ptax")
    with pytest.raises(ValueError, match="no month was given"):
        module.main(["ptax"])
    with pytest.raises(ValueError, match="no first quote date"):
        module.main(["ptax", "2026-08"])
    with pytest.raises(ValueError, match="no last quote date"):
        module.main(["ptax", "2026-08", "2026-06-03"])


def test_the_fetch_refuses_the_apis_own_date_format_rather_than_reinterpreting_it():
    """THE ASYMMETRY THIS PHASE IS BUILT ON, refused at the boundary.

    The endpoint is asked in `MM-DD-YYYY` in single quotes -- not ISO -- and
    `opl.extraction.ptax_source.quote_url` does that conversion. An operator who types
    the API's own spelling into a job parameter must not have it reinterpreted: `06-03-2026`
    is not June 3rd to `date.fromisoformat`, and a lenient parser that made it one would
    fetch a window nobody asked for and land it under a filename derived from it."""
    module = _load("fetch_ptax")
    with pytest.raises(ValueError, match="MM-DD-YYYY"):
        module.main(["ptax", "2026-08", "06-03-2026", "2026-08-01"])


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
    record; the URL list is what tells the two apart."""
    module = _load("fetch_ptax")
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
    module.main(["ptax", "2026-08", "2026-06-18", "2026-06-20"])

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


def test_a_window_the_series_has_nothing_in_is_refused_rather_than_landed_empty(tmp_path):
    """An empty file in the landing dir is ingested cleanly, rejects nothing, promotes
    nothing, and every task reports SUCCESS having moved zero rows -- and it then OWNS
    the filename this window derives, so the correct fetch of the same window is refused
    by the emitter ever afterwards.

    One day with no quote is normal (weekends, holidays) and is not this: the refusal is
    about a whole window, which is a property of the request."""
    module = _load("fetch_ptax")
    with pytest.raises(ValueError, match="carries no PTAX quote at all"):
        module._refuse_a_window_with_no_quotes((), date(2026, 6, 20), date(2026, 6, 21))


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
