# databricks/src/fetch_ptax.py
"""Job task: fetch the PTAX dollar series for a quote-date window and land it in the
Volume as JSON Lines.

THE ANALOGUE OF `unzip_table` AND `generate_payments`, FOR A SOURCE NOBODY DOWNLOADS AND
NOBODY GENERATES. Every bronze table's landing dir is filled by something before its
ingest runs: the extraction host PUTs the RFB archives and `unzip_table` expands them,
`generate_payments` derives the payment stream from a seed. PTAX's bytes exist -- BCB
published them -- but there is no FILE to fetch, so this task asks the endpoint and
writes what comes back. That is what `opl.bronze.registry_landing.LANDING_API` means, and
it is why this runs first among the work tasks.

IT LANDS A RECORD BUILT FROM THE VALIDATED RESPONSE, NOT THE RAW BODY, and each of the
three reasons is fatal on its own:

  * THE RAW BODY CARRIES NO QUOTE DATE. `dataHoraCotacao` is a PUBLICATION instant --
    measured, F-API Task 0: a quote requested for 1984-11-28 comes back stamped
    1984-12-03. The quote date is carried from the REQUEST, and a bronze table without it
    makes plan T3's instant rule degrade into the calendar-day comparison it forbids.
  * IT IS ONE JSON DOCUMENT. The reader sets `multiLine=false` (JSON Lines is one object
    per line), so a landed response body would parse as a single malformed record.
  * IT WOULD PUT EVERY FUTURE BCB FIELD IN `_rescued_data`. The read schema is built from
    the contract, `rescuedDataColumn` catches anything undeclared, and
    `dq._reject_reason` ranks `rescued_data_present` above every per-table rule -- so an
    upstream addition this project does not control would turn every run red. Landing the
    five contract columns means a NEW BCB field is a change this repository can adopt
    deliberately, rather than an outage.

ONE REQUEST PER CALENDAR DAY, WHICH IS FORCED RATHER THAN CHOSEN. A range request answers
with rows carrying no attributable quote date at all, so `ptax_source.fetch_series` loops
over days -- and it must loop over EVERY day, because a caller cannot know which ones carry
a quote without asking. It is cheap: ~220 bytes a response, 60 calendar days in this
phase's window (2026-06-03 .. 2026-08-01), of which 42 answer a quote and 18 answer HTTP
200 with an empty row list.

THIS DOCSTRING SAID "42 BUSINESS DAYS", AND IT WAS WRONG TWICE. The window holds 43
weekdays, not 42 -- 42 is the QUOTE count, one lower because Corpus Christi 2026-06-04 has
none -- and the loop is over calendar days regardless, so neither number was ever the call
count. `ptax_source.fetch_series`' own docstring was corrected when the run measured 60
(`docs/f-api-run-evidence.md` §2.5) and this caller was not. Measured 60 / 42 / 18 by the
run's own log; 60 / 43 / 17 / 1 by the calendar.

NO SPARK, AND NO SESSION IS CREATED. Nothing here reads a table: the request is HTTP and
the write is a file in the Volume. It stays a `spark_python_task` because that is how the
bundle runs a Python file, not because it needs an engine.

NO CREDENTIAL. BCB/Olinda's OData service is public and unauthenticated -- no token, no
header, no basic-auth pair. `opl.extraction.ptax_source` cannot even acquire the ability
to send one (it imports no HTTP client); this file has the client and must never grow a
secret to hand it.

IDEMPOTENT, BECAUSE `max_retries: 0` DOES NOT PREVENT A RETRY. The landing filename is a
function of the window and the currency and carries no run id, month or timestamp, so a
repair run derives the same path and `emit_records_file` compares bytes rather than
overwriting. Auto Loader tracks files by PATH: a run-scoped name would make every re-run
a second ingest of the same quotes, under a fresh `_batch_id` that the promote's
idempotence cannot see.

WHAT A RE-FETCH OF A REVISED WINDOW DOES, said because the source is not ours: it fails.
If BCB revises a quote inside a window already landed, the bytes differ under the same
name and the emitter REFUSES. That is the correct verdict -- silently overwriting would
leave bronze holding the old rate under a checkpoint that says the file was read, so the
revision could never arrive -- and the repair is a deliberate act, not a retry.

THE WINDOW IS DECLARED, NOT PASSED. `opl.extraction.ptax_window` carries the two quote
dates and says why they are not job parameters: they were, defaulting to a sentinel
argued from a lock nobody had written, and a real date pasted into that default makes
every `--params`-less run land a window nobody asked for -- under the filename that
window derives, which then blocks the correct one by this task's own refusal.

argv: [table, month] -- both REQUIRED, neither defaulted."""
import sys
from datetime import date

import requests

from opl.bronze.generated_landing import (
    STREAM_FILE_SUFFIX,
    EmittedFile,
    emit_records_file,
)
from opl.bronze.registry import (
    LANDING_API,
    BronzeTable,
    landing_dir,
    landing_tmp_dir,
    table_spec,
)
from opl.config import DEFAULT, require_month
from opl.contracts.ptax import (
    COLUMNS,
    COMPRA_COLUMN,
    CURRENCY_COLUMN,
    PUBLISHED_AT_COLUMN,
    QUOTE_DATE_COLUMN,
    VENDA_COLUMN,
)
from opl.extraction.ptax_source import QUOTED_CURRENCY, PtaxQuote, fetch_series
from opl.extraction.ptax_window import WINDOW_FIRST, WINDOW_LAST

# A browser-shaped default User-Agent is what a governed egress path is most likely to
# refuse, and an anonymous one makes a 403 impossible to attribute. Named for the project
# so BCB's own logs can identify the caller, matching `scripts/probe_ptax.py`.
HEADERS = {"User-Agent": "open-payments-lakehouse-f-api/1.0"}
# Per request, not for the whole window. 60 sequential calls at ~220 bytes each; a bound
# this generous only fires when the endpoint is genuinely not answering, which is a
# failure this task must not paper over by continuing with fewer quotes.
TIMEOUT_SECONDS = 60


def _refuse_a_table_this_does_not_fetch(spec: BronzeTable) -> None:
    """Refuse a table whose bytes come from somewhere else, before anything is requested.

    LOUD BEFORE THE NETWORK, which is why `main` calls this the moment it has a spec.
    Handed a CNPJ table, this would write PTAX records into the directory that table's
    semicolon-CSV Auto Loader reads, against a thirty-column schema: every row rescued or
    NULL, the batch rejected, and the diagnosis starting from a quarantine full of
    unrecognisable rows rather than from the wiring mistake. Handed the payments table it
    would land a file into a directory whose stream is mid-phase, under a filename the
    generator will later refuse to write over.

    Compared against `LANDING_API` rather than for "not zips", the same way
    `generate_payments` and `bronze_payments_ingest` state their own refusals: a fifth
    landing mode added later must be refused by default here rather than admitted by an
    `else`."""
    if spec.landing != LANDING_API:
        raise ValueError(
            f"{spec.name} lands as {spec.landing!r}, and this task fetches only tables "
            f"served by an API (landing={LANDING_API!r}). Its bytes are produced by a "
            "downloader or by this lakehouse's own generator: run unzip_table.py or "
            "generate_payments.py instead."
        )


def fetch(url: str) -> str:
    """The response body for `url`, or refuse naming the status.

    THE TRANSPORT, AND IT IS THE ONLY THING THIS FILE ADDS TO THE EXTRACTION LAYER.
    `opl.extraction.ptax_source` builds the request and validates the answer and imports
    no HTTP client at all, so the timeout and the status policy are the caller's -- which
    is here, the one place that runs on Databricks.

    A NON-200 IS A REFUSAL AND NEVER AN EMPTY DAY, which is the constraint the extraction
    layer states about its own callers. 2026-06-20 answers HTTP 200 with `"value":[]`
    because it is a Saturday, and that is indistinguishable from an HTML interstitial or
    an OData error object to a forgiving reader. T3 resolves an absence by falling back
    over the series, so a failure read as an absence makes the fallback silently cross a
    hole and hand a payment an older rate. This raises instead.

    NO RETRY, deliberately. The task is idempotent and cheap to relaunch, and a retry
    loop here would turn a systematic refusal -- a governed egress path, a renamed
    endpoint -- into a slow failure that looks like a network blip."""
    response = requests.get(url, headers=HEADERS, timeout=TIMEOUT_SECONDS)
    if response.status_code != 200:
        raise RuntimeError(
            f"PTAX answered {response.status_code} for {url}. This is refused rather "
            "than read as a day with no quote: the series' own absences arrive as 200 "
            "with an empty row list, and a failure treated as an absence makes the "
            "fallback cross a hole nobody can see afterwards."
        )
    return response.text


def record_of(quote: PtaxQuote) -> dict[str, str]:
    """`quote` as the contract's five string columns, in `COLUMNS` order.

    THE ONE CROSSING from validated values into text, and it is where the
    column-provenance split becomes bytes: the first two keys are what this lakehouse
    asked for, the last three are what BCB answered.

    `published_raw` AND NOT `str(published_at)`. Bronze lands source bytes, and
    `published_at` carries this repository's own Brasilia-time reading of the stamp --
    writing it here would land a zone decision as though BCB had sent it. The parsed
    instant is what T3 compares; the raw string is what the table records.

    `str(Decimal)` FOR BOTH RATES, never a float. The extraction layer parses with
    `parse_float=Decimal` from the raw response text precisely so `5.07730` does not
    become `5.0773`, and a float anywhere on this path would throw that away in the last
    step. Every value is non-empty by construction, which is what the gate's five
    `null_or_empty_*` rules assert rather than hope.

    Built by iterating `COLUMNS`, so the contract's declared order decides the emitted
    line's key order -- `json.dumps` writes keys in insertion order."""
    values = {
        QUOTE_DATE_COLUMN: quote.quote_date.isoformat(),
        CURRENCY_COLUMN: quote.currency,
        PUBLISHED_AT_COLUMN: quote.published_raw,
        COMPRA_COLUMN: str(quote.compra),
        VENDA_COLUMN: str(quote.venda),
    }
    return {column: values[column] for column in COLUMNS}


def filename_for(currency: str, first: date, last: date) -> str:
    """The landing filename for one currency over one quote-date window.

    THE WINDOW AND THE CURRENCY, AND NOTHING ELSE. No run id, no month, no timestamp:
    Auto Loader tracks files by PATH, so a run-scoped component would make each re-run a
    NEW file holding the same quotes -- ingested again under a fresh `_batch_id`, which
    the promote's idempotence is keyed on and therefore cannot see. `filename_for(spec)`
    in `opl.bronze.generated_landing` makes the identical argument for the payment stream.

    IT IS ALSO WHAT MAKES THE EMITTER'S REFUSAL MEANINGFUL. Two different windows are two
    different files, so they do not collide; the SAME window fetched twice is one file,
    and if its bytes have changed the emitter refuses rather than overwriting.

    THE CURRENCY COMES FROM THE ENDPOINT, NOT FROM A ROW. `CotacaoDolarPeriodo` IS the
    pair, so it is source knowledge and it is known before any request is made -- which
    means the path this run will write is decided by its ARGUMENTS, not by what came
    back. Reading it off `quotes[0]` would make the filename depend on the response, and
    an empty window would produce a name with a hole in it.

    The table is not in the name because the directory already carries it: this lands in
    `api/<month>/<subdir>`, one table's own landing dir, which no other stream reads."""
    return f"{currency.lower()}-{first.isoformat()}_{last.isoformat()}{STREAM_FILE_SUFFIX}"


def _report(
    quotes: tuple[PtaxQuote, ...], first: date, last: date, landed: EmittedFile
) -> None:
    """What the run log says, written to be quotable as evidence.

    The digest and the byte count are the two numbers a landed-file claim is stated in,
    so this line and a local assertion compare directly with nothing re-derived. The
    day counts are printed rather than asserted: weekends and holidays legitimately carry
    no quote, so "fewer quotes than days" is the normal case and only a reader who knows
    the calendar can judge it.

    NOTHING HERE CLAIMS THE SERIES IS GAPLESS. T3 needs it to be contiguous in BUSINESS
    days over the span the fact reaches, and that is a statement about days which are
    absent -- which neither this task nor the row-tagging DQ gate can express. It is
    reported as counts so the claim can be checked, not asserted so it can be believed."""
    state = "already present, byte-identical" if landed.was_already_there else "written"
    span = (last - first).days + 1
    print(
        f"fetch_ptax: window {first} .. {last} ({span} calendar days) -> "
        f"{len(quotes)} quote(s), {span - len(quotes)} day(s) with none"
    )
    print(f"fetch_ptax: {state} at {landed.path}")
    print(
        f"fetch_ptax: rows={landed.row_count} bytes={landed.byte_count} "
        f"sha256={landed.sha256}"
    )


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # Table first, and resolved BEFORE anything else: a mistyped table name is refused by
    # `table_spec` naming the valid ones, and neither of these two refusals needs the
    # network. An operator should not wait for 60 round trips to be told which argument
    # is wrong.
    spec = table_spec(args[0] if args else "")
    _refuse_a_table_this_does_not_fetch(spec)
    # NO DEFAULT, for `generate_payments`' reason: this local picks the directory the
    # file is WRITTEN into, and the ingest task that follows resolves its own source dir
    # from the same job parameter. A substituted month would write into one month's
    # landing dir and read another's -- a job that succeeds having ingested nothing.
    month = require_month(args[1] if len(args) > 1 else None, action="fetch")
    # THE WINDOW IS THE DECLARATION AND NOT AN ARGUMENT, so these two locals are read
    # rather than validated: a `date` cannot arrive in the API's own `MM-DD-YYYY`
    # spelling, and a window that was launched wrong is not a state this task can be in.
    # See `opl.extraction.ptax_window` for the parameters this replaced and why.
    first, last = WINDOW_FIRST, WINDOW_LAST
    # A WINDOW THE SERIES HAS NOTHING IN IS REFUSED BY `fetch_series` ITSELF, so this task
    # does not check it a second time. It used to, and the refusal moved DOWN a layer
    # rather than being dropped: the condition is identical -- no quote on any day asked
    # for -- and two spellings of one refusal means the second can never fire, which this
    # repository treats as worse than no guard because the next reader trusts it.
    #
    # What this file knows and the extraction layer does not is why the refusal has to
    # exist at all, so it is recorded here: an empty file in the landing dir is ingested
    # cleanly, the gate finds nothing to reject, the promote appends nothing, and every
    # task reports SUCCESS having moved zero rows -- indistinguishable from a month in
    # which nothing new arrived. The empty file then OWNS the name this window derives, so
    # the correct fetch of the same window is refused by the emitter ever afterwards.
    quotes = fetch_series(first, last, fetch)
    landed = emit_records_file(
        [record_of(quote) for quote in quotes],
        filename_for(QUOTED_CURRENCY, first, last),
        # THE ONE MAPPING, ASKED RATHER THAN RE-SPELLED, which is what `bronze_ptax_ingest`
        # already did: `landing_dir` takes the whole spec, so the directory this task
        # WRITES cannot drift from the one that task READS, and a mode no root serves is
        # refused there rather than defaulting into a directory holding another source's
        # files. This file used to build `landing_api_table` itself -- one directory
        # resolved two ways inside one job, which is the drift `landing_dir` exists to
        # remove.
        #
        # The SAME `month` local for both, so the file cannot be staged under one month
        # and landed under another -- and the tmp twin is outside every directory an Auto
        # Loader reads, so the half-written file the replace makes whole is never
        # discoverable by the stream that reads the finished one.
        directory=landing_dir(DEFAULT, spec, month),
        tmp_directory=landing_tmp_dir(DEFAULT, spec, month),
    )
    _report(quotes, first, last, landed)


if __name__ == "__main__":
    main()
