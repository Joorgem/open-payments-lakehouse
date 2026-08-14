# src/opl/contracts/ptax.py
"""Versioned schema contract for the BCB/Olinda PTAX dollar series (F-API), in the shape
`opl.contracts.payments` and `opl.contracts.cnpj_schemas` take: the columns are DATA
here, and every mechanism that reads them lives elsewhere.

IT IMPORTS NOTHING, and that is a requirement rather than a tidiness note.
`opl.contracts.catalogue` joins this module into the mapping `opl.bronze.registry` reads,
and the registry is imported by the extraction scripts, which run on a host where pyspark
is an optional extra that is usually absent. An import here is an import there.

THE GRAIN IS ONE ROW PER (currency, quote_date). Not per publication, not per request,
not per calendar day: one row is the ONE quote that a given currency had for a given
quote date. `opl.extraction.ptax_source.sole_quote` is where that reduction happens, at
the request, because there the key IS the request -- one endpoint is one currency and one
URL is one quote date, so two rows in one response can only be two publications of one
quote. Two rows that AGREE reduce to the later publication stamp; two that DISAGREE are
refused rather than resolved. Everything below assumes that reduction already happened.

--- THE COLUMN-PROVENANCE SPLIT, WHICH IS THIS MODULE'S WHOLE DESIGN ------------------

Five columns, in two groups, and WHICH GROUP A COLUMN IS IN IS LEGIBLE FROM ITS NAME:

  * `REQUEST_COLUMNS` are stamped by US, from the request that was made, and are named in
    ENGLISH -- `quote_date`, `currency`.
  * `RESPONSE_COLUMNS` are the API's own three fields, snake_cased and otherwise
    untouched -- `data_hora_cotacao`, `cotacao_compra`, `cotacao_venda`.

THE NAMING ASYMMETRY IS THE DOCUMENTATION. A column named in the API's alphabet came out
of the response body; a column named in ours was put there by this lakehouse. A reader who
knows only that rule can answer "where did this value come from?" for every column of this
table without opening a single other file, and a future column that is named in the wrong
alphabet is visibly wrong rather than merely undocumented.

AND THE SPLIT IS ENFORCED, NOT DESCRIBED. `_assert_the_provenance_split_is_a_partition`
refuses AT IMPORT any overlap between the two tuples and any column of `COLUMNS` that is
in neither. The overlap half is the one that matters, and the edit it exists to refuse is
specific and tempting: `quote_date` becoming a projection of `data_hora_cotacao`.

WHY THAT EDIT IS SILENT AND CATASTROPHIC. `dataHoraCotacao` is a PUBLICATION INSTANT, not
the quote's date, and the quote date is ABSENT from the response body -- measured, F-API
Task 0: a quote requested for 1984-11-28 comes back stamped `1984-12-03 11:29:00.0`, and
the string `1984-11-28` appears nowhere in it. On every day this phase extracts the two
coincide, so deriving one from the other passes every 2026 fixture. But the phase's
resolution rule (plan T3) is "the most recent quote whose PUBLICATION instant precedes the
payment's own instant", and it needs BOTH values to be a rule at all. Collapse them and
that rule degrades into the calendar-day comparison it forbids -- correctly for every 2026
row, wrongly in 1984, and with nothing anywhere to say which.

NO DRIFT COLUMN, AND THE ABSENCE IS A DECISION. `opl.contracts.payments` declares
`DRIFT_COLUMN` because F1b's phase requirement is to EXHIBIT schema drift and the payment
stream is ours to shape. This source is not ours: BCB decides what `CotacaoDolarPeriodo`
returns. Declaring a drift column here would mean this lakehouse fabricating a field and
attributing it to the Banco Central in `_record_source`, which is a lie about a real
institution written into the one column that answers who produced a row -- the same
objection that made `LANDING_GENERATED` unusable for this source. If BCB ever adds a
field, that is REAL drift and the gate reports it: `struct_for` builds from `COLUMNS`, so
an undeclared key has nowhere to go but `_rescued_data`.

ALL-STRING IN BRONZE, matching every other contract here (`opl.bronze.schema`). The rates
are carried as the DIGITS BCB PUBLISHED and never as a float: `json.loads` turns the API's
`5.07730` into `5.0773` and `str()` drops the trailing zero, so a rate that has been
through a float is a different string from the one the bulletin carries.
`opl.extraction.ptax_source` parses with `Decimal` from the raw text for that reason, and
this contract's job is to make sure nothing between there and bronze widens it back.

BRONZE LANDS A RECORD BUILT FROM THE VALIDATED RESPONSE, NOT THE RAW BODY, and the three
reasons are all fatal on their own: the raw body carries no quote date (above); it is ONE
JSON document, which the JSON Lines reader (`multiLine=false`) sees as a single malformed
record; and every future BCB field would land in `_rescued_data`, turning every run red
over an upstream change this project does not control.

NO CREDENTIAL, EVER. The endpoint is public and unauthenticated -- no token, no header, no
basic-auth pair. There is nothing here for a secret to attach to and there must never be.
"""
from __future__ import annotations

# The contract's version. It bumps when a column is ADOPTED, which for this source means
# BCB started sending a field and this lakehouse decided to declare it -- a change with a
# diff, not a side effect of a response widening.
SCHEMA_VERSION = 1

# The contract key, in the sense `opl.bronze.registry.BronzeTable.contract` means it.
CONTRACT = "ptax"

# --- the columns ---------------------------------------------------------------------

# STAMPED BY US, FROM THE REQUEST. English names, because that is what says so.
#
# `quote_date` is the date the request filtered on, in ISO `YYYY-MM-DD` -- which is NOT
# the format the API is asked in (`MM-DD-YYYY`, in single quotes, and not ISO). Writing
# the request's own spelling into this column is the mistake this phase invites: it joins
# to nothing in gold while every row count stays green, which is why the DQ gate carries
# `bad_quote_date_shape` rather than trusting the writer.
QUOTE_DATE_COLUMN = "quote_date"
# `currency` is the quoted currency of the endpoint that was called -- `CotacaoDolarPeriodo`
# IS the pair, so one endpoint is one currency and the value comes from knowing which URL
# was requested, not from anything in the body. NO VALUE DOMAIN IS DECLARED here and no
# CHECK constraint is registered for it, for the reason the payments entry gives: a second
# currency must be a VALUE change rather than a schema change, and a CHECK would silently
# make it a migration on a live table.
CURRENCY_COLUMN = "currency"
REQUEST_COLUMNS = (QUOTE_DATE_COLUMN, CURRENCY_COLUMN)

# THE API'S OWN THREE FIELDS, snake_cased and otherwise untouched. `cotacaoCompra` ->
# `cotacao_compra`, `cotacaoVenda` -> `cotacao_venda`, `dataHoraCotacao` ->
# `data_hora_cotacao`. Translating them ("bid", "ask", "published_at") would put a
# lakehouse-chosen word on a value BCB chose, and would destroy the one property that
# makes a landed column traceable back to the response field it came out of.
#
# `data_hora_cotacao` IS THE COLUMN T3'S RULE READS, and it is the reason this contract
# has five columns rather than the two -- (quote_date, venda) -- that every other sentence
# about "an FX rate table" implies.
PUBLISHED_AT_COLUMN = "data_hora_cotacao"
COMPRA_COLUMN = "cotacao_compra"
VENDA_COLUMN = "cotacao_venda"
RESPONSE_COLUMNS = (PUBLISHED_AT_COLUMN, COMPRA_COLUMN, VENDA_COLUMN)

# The two rate columns. Their own tuple because the DQ gate builds one parse rule per rate
# from it, and because `compra` and `venda` are the pair whose DIRECTION an FX defect gets
# wrong silently -- `amount_brl` is `amount_original * venda`, and the other one, or a
# division, lands a number of the right shape, plausible in magnitude, and wrong.
RATE_COLUMNS = (COMPRA_COLUMN, VENDA_COLUMN)

# ORDER IS AUTHORITATIVE, for `opl.contracts.payments.COLUMNS`' reason: these are JSON
# objects, where the key decides what a value MEANS, but the order still decides the
# BYTES, and the landing writer refuses a file whose bytes differ from the ones it derived.
#
# SPELLED OUT RATHER THAN `(*REQUEST_COLUMNS, *RESPONSE_COLUMNS)`, and that is what makes
# the partition guard below a check instead of a tautology. Derived, "a column in neither
# group" would be unreachable by construction and the guard would be decorative -- which
# this repository treats as worse than no guard, because the next reader believes the hole
# is closed.
COLUMNS = (
    QUOTE_DATE_COLUMN,
    CURRENCY_COLUMN,
    PUBLISHED_AT_COLUMN,
    COMPRA_COLUMN,
    VENDA_COLUMN,
)

# EVERY COLUMN IS REQUIRED, and this MATCHES `opl.extraction.ptax_source` rather than
# deciding independently. That layer refuses a response row missing ANY of
# `cotacaoCompra` / `cotacaoVenda` / `dataHoraCotacao` ("Every one of ... is load-bearing")
# and carries the request's own two values onto each quote, so a record that reaches the
# landing writer cannot be missing a column at all. A gate looser than the extraction
# would tolerate exactly the columns a bug between the two could empty; a gate stricter
# than it would be unreachable. Declared as its own name for payments' reason: `COLUMNS`
# is what every record CARRIES and its order decides the bytes, and this is what every
# record must carry NON-EMPTY, where order means nothing.
REQUIRED_COLUMNS = COLUMNS

# --- bronze naming -------------------------------------------------------------------
#
# The staging/bronze/quarantine triple in ONE literal block, which is the property
# `opl.bronze.registry` exists to hold: the documented defect is a quarantine name spelled
# somewhere else that "sent estab triagers to a table full of unrelated F1.2 lookup rows".
# The registry entry lifts every one of these; `name="ptax"` is the only literal there,
# because a registry KEY is that dict's own namespace.
BRONZE_TABLE_KEY = "bronze_ptax"
BRONZE_STAGING_TABLE = "bronze_ptax_staging"
BRONZE_TABLE = "bronze_ptax"
BRONZE_QUARANTINE_TABLE = "bronze_ptax_quarantine"
# The landing subdir, under the API root and not the generated one -- `opl.config` carries
# why a third root beat reusing `generated/`.
LANDING_SUBDIR = "ptax"


def _assert_every_column_is_declared_once() -> None:
    """Fail at import if `COLUMNS` repeats a name or carries none.

    A repeated key in a JSON object is LAST-ONE-WINS, so a duplicate would not even be
    visible in the emitted record -- and it would give `struct_for` two fields of one
    name, which Spark accepts and then resolves arbitrarily.

    Emptiness is refused separately because an empty contract is the one shape that
    passes every other check here and produces a read schema with no columns: the Auto
    Loader would route every field into `_rescued_data`, the gate would reject the whole
    batch, and the diagnosis would start from a quarantine rather than from this file.

    A plain ValueError: nothing here is an unknown table, and no operator supplied it.
    This is a source edit that broke a declaration."""
    if not COLUMNS:
        raise ValueError(
            "COLUMNS is empty, so struct_for(CONTRACT) would build a read schema with no "
            "fields and every value in the landed record would arrive as rescued data"
        )
    if len(set(COLUMNS)) != len(COLUMNS):
        raise ValueError(
            f"COLUMNS repeats a name: {COLUMNS}. A repeated key in a JSON object is "
            "last-one-wins, so the duplicate would not even be visible in the output."
        )


def _assert_the_provenance_split_is_a_partition() -> None:
    """Fail at import if a column is in BOTH provenance groups, or in NEITHER.

    THE POINT OF THIS MODULE, and the edit it refuses is a specific one: `quote_date`
    becoming a projection of `data_hora_cotacao`. The response does not carry the quote
    date -- `dataHoraCotacao` is a publication instant, and the two differ by five days in
    1984 (module docstring) -- so deriving one from the other is right for every day this
    phase extracts and wrong as a rule. Plan T3 resolves a payment's rate by comparing the
    publication instant against the payment's own; with one column doing both jobs that
    rule silently becomes the calendar-day comparison it forbids, and every count stays
    green.

    The other direction is refused because it is how the split stops meaning anything: a
    column in neither group is a column whose provenance nobody stated, and the naming
    rule this contract documents itself with ("named in the API's alphabet means it came
    from the response") is only readable while every column is on one side of it.

    Also refuses a declared provenance column that `COLUMNS` does not carry, which is the
    same equality read the other way: a group naming a column no record holds is a claim
    about a field that does not exist.

    A plain ValueError, for the reason above."""
    both = sorted(set(REQUEST_COLUMNS) & set(RESPONSE_COLUMNS))
    if both:
        raise ValueError(
            f"{both} are declared as BOTH request-stamped and response-carried. The "
            "request's quote date is not in the response and is never derived from "
            "data_hora_cotacao, which is a PUBLICATION instant: the two coincide on every "
            "day this phase extracts and differ by five days in 1984. One column doing "
            "both jobs turns T3's instant rule into the calendar-day comparison it "
            "forbids, correctly for 2026 and wrongly as a rule, with nothing to see."
        )
    unsplit = sorted(set(COLUMNS) ^ (set(REQUEST_COLUMNS) | set(RESPONSE_COLUMNS)))
    if unsplit:
        raise ValueError(
            f"{unsplit} appear in COLUMNS without a provenance group, or in a group "
            f"without being a column. COLUMNS is {COLUMNS}, REQUEST_COLUMNS is "
            f"{REQUEST_COLUMNS} and RESPONSE_COLUMNS is {RESPONSE_COLUMNS}. This contract "
            "documents itself by the alphabet a column is named in -- ours means the "
            "request stamped it, BCB's means the response carried it -- and a column on "
            "neither side makes that rule unreadable rather than merely undocumented."
        )
    strays = sorted(set(RATE_COLUMNS) - set(RESPONSE_COLUMNS))
    if strays:
        raise ValueError(
            f"{strays} are declared as rate columns and are not carried by the response. "
            "A rate is a number BCB published; one this lakehouse computed belongs in "
            "gold, where it can be named for what it is."
        )


_assert_every_column_is_declared_once()
_assert_the_provenance_split_is_a_partition()
