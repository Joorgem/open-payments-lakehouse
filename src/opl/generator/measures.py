# src/opl/generator/measures.py
"""What a delivered stream CONTAINS, read back off the rendered records and nothing else.

THIS MODULE IS NOT ALLOWED TO KNOW HOW THE DEFECTS WERE INJECTED, and the import list is
the enforcement: it imports the contract, the instant format and the attribute tuple, and
it does NOT import `opl.generator.defects`. That direction is the whole point. A
measurement that could consult the injection would confirm itself -- "127 duplicates
were injected, therefore there are 127 duplicates" is not a measurement, it is a
restatement -- and the F1b tests compare the two sides precisely because they are
computed by code that cannot see each other.

IT IS ALSO THE REFERENCE IMPLEMENTATION OF TASK 4'S QUERIES, in Python, over the same
columns bronze will hold. Every function below has an obvious SQL twin, and the twin is
named in its docstring. When the workspace run disagrees with these numbers, the
disagreement is between two implementations of one definition rather than between a
number and a sentence, which is the only shape that can be argued to a conclusion.

TWO COUNTS THAT LOOK THE SAME AND ARE NOT, WHICH IS THE REASON THIS MODULE EXISTS AT ALL:

    distinct_business_attributes(records)          -- cannot drop a row, ever
    distinct_over_dropping_nulls(records, columns) -- drops every row missing a column

They are two functions with two names rather than one function with a column list,
because `COUNT(DISTINCT a, b, c)` DROPS a row that is NULL in any of a, b, c and that
behaviour is invisible at the call site. This repository lost 8,761 rows to exactly that,
and the clean payment stream could not reproduce it -- no contract column is ever NULL,
so the defect was inert. THE DRIFT COLUMN ARMS IT: every record emitted before the drift
point lacks that key, so a distinct count that included it would silently exclude the
entire pre-drift population and return a smaller, confident number. `rows_dropped_by_null`
exists to make that population countable rather than merely arguable, and
`opl.contracts.payments` refuses at import to let the drift column into any tuple the
safe count is taken over.

LATENESS IS A RELATION BETWEEN TWO COLUMNS, NEVER A POSITION IN A FILE. `late_arrivals`
sorts by `emitted_at` and compares against the newest `event_time` among rows emitted
STRICTLY earlier; it never looks at the order the records were handed to it. That is what
lets it run unchanged against an ordered JSONL file and against an unordered Delta table,
and it is why a redelivered row -- which carries its original's `emitted_at` verbatim --
cannot register as late merely by sitting further down the file.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence

from opl.contracts.payments import (
    COLUMNS,
    DRIFT_COLUMN,
    EMITTED_AT_COLUMN,
    EVENT_TIME_COLUMN,
    IDENTITY_COLUMN,
)
from opl.generator.events import business_attributes_of
from opl.generator.instants import from_text

_Records = Sequence[Mapping[str, str]]


def late_arrivals(records: _Records) -> tuple[int, ...]:
    """The positions in `records` that arrived late, ascending.

    SQL twin: a row is late when its `event_time` is older than the maximum `event_time`
    over the rows whose `emitted_at` is STRICTLY smaller than its own.

    "Strictly" is the clause that costs a line here and would cost a defect later. Rows
    sharing one `emitted_at` were released together, so none of them can be late because
    of another -- and a byte-identical redelivery shares its original's `emitted_at`
    exactly, which is what keeps the duplicate class from leaking into this count.

    Returns positions rather than a count: an off-by-one in an injected set is only
    debuggable if the disagreement names rows."""
    events = [from_text(record[EVENT_TIME_COLUMN]) for record in records]
    emissions = [from_text(record[EMITTED_AT_COLUMN]) for record in records]
    order = sorted(range(len(records)), key=lambda position: (emissions[position], position))
    late: list[int] = []
    newest = -1  # the newest event_time among rows emitted strictly earlier; -1 precedes
    folded = 0  # how many of `order` are already accounted for in `newest`
    for position in order:
        while folded < len(order) and emissions[order[folded]] < emissions[position]:
            newest = max(newest, events[order[folded]])
            folded += 1
        if events[position] < newest:
            late.append(position)
    return tuple(sorted(late))


def crosses_a_window(record: Mapping[str, str], *, window_ms: int) -> bool:
    """Whether `record` was emitted in a later tumbling window than it happened in.

    SQL twin: `floor(unix_millis(emitted_at) / w) > floor(unix_millis(event_time) / w)`.

    NOT A SYNONYM FOR LATE, and reading it as one is the mistake this docstring exists to
    prevent: a perfectly punctual record whose `event_time` sits within `emission_lag_ms`
    of a window edge also crosses one. What it adds to the lateness relation is the
    consequence -- a windowed aggregation over `event_time`, keyed off `emitted_at`, had
    already closed and emitted that window before this row turned up."""
    if window_ms <= 0:
        raise ValueError(f"window_ms must be positive, got {window_ms}")
    return (
        from_text(record[EMITTED_AT_COLUMN]) // window_ms
        > from_text(record[EVENT_TIME_COLUMN]) // window_ms
    )


def distinct_identities(records: _Records) -> int:
    """SQL twin: `COUNT(DISTINCT transaction_id)`. One per payment EVENT."""
    return len({record[IDENTITY_COLUMN] for record in records})


def duplicate_row_count(records: _Records) -> int:
    """SQL twin: `COUNT(*) - COUNT(DISTINCT transaction_id)`. Redeliveries, and only them.

    A LEGITIMATE REPEAT CANNOT APPEAR IN THIS FIGURE, which is the T2 property in one
    line: a repeat carries its own `transaction_id`, so it adds one to both terms and
    cancels. That is why the identity may never be derived from the attributes."""
    return len(records) - distinct_identities(records)


def redelivered_identities(records: _Records) -> tuple[str, ...]:
    """Every `transaction_id` delivered more than once, sorted.

    WHICH ROW IS "THE DUPLICATE" IS NOT ASKED, because the data cannot answer it: a
    redelivery is byte-identical to the delivery it repeats, so the pair is unordered and
    any function claiming to pick the copy would be reporting an artefact of iteration
    order as a fact about the stream."""
    seen: dict[str, int] = {}
    for record in records:
        identity = record[IDENTITY_COLUMN]
        seen[identity] = seen.get(identity, 0) + 1
    return tuple(sorted(identity for identity, count in seen.items() if count > 1))


def distinct_business_attributes(records: _Records) -> int:
    """SQL twin: `COUNT(DISTINCT payer, payee, amount, currency, payment_method)`.

    THE SAFE COUNT, and safe for a structural reason rather than by review: it reads
    `BUSINESS_ATTRIBUTE_COLUMNS`, every member of which `opl.contracts.payments` refuses
    at import to let be anything but REQUIRED. A required column is present on every row,
    so no row can be dropped, so this number is comparable across a stream that drifted
    and one that did not.

    It also RAISES rather than drops if a record is missing one of those columns --
    `business_attributes_of` indexes -- which is the opposite of SQL's behaviour and
    deliberately so. A stream that lost a required column is a finding, not a smaller
    count."""
    return len({business_attributes_of(record) for record in records})


def distinct_over_dropping_nulls(records: _Records, columns: Sequence[str]) -> int:
    """SQL twin: `COUNT(DISTINCT <columns>)`, INCLUDING its row-dropping behaviour.

    THE UNSAFE COUNT, named for what it does. A record missing any of `columns` is
    excluded entirely -- not the missing value, the whole row -- exactly as SQL excludes
    a row that is NULL in any argument. Reach for this only to demonstrate the defect;
    `distinct_business_attributes` is the count a stream is described by.

    Pair every call with `rows_dropped_by_null` over the same columns, or the number
    returned is a count of an unnamed subset."""
    kept = [record for record in records if all(column in record for column in columns)]
    return len({tuple(record[column] for column in columns) for record in kept})


def rows_dropped_by_null(records: _Records, columns: Sequence[str]) -> int:
    """How many rows `distinct_over_dropping_nulls(records, columns)` never looked at.

    SQL twin: `COUNT(*) FILTER (WHERE <any column> IS NULL)`. Zero over any set of
    required columns; the entire pre-drift population the moment a drift column joins
    them, which is the defect this repository has already paid 8,761 rows for."""
    return sum(1 for record in records if any(column not in record for column in columns))


def drifted_rows(records: _Records) -> tuple[int, ...]:
    """The positions carrying `DRIFT_COLUMN`, ascending.

    SQL twin: `WHERE payment_channel IS NOT NULL` -- once the column exists in bronze at
    all. Until then the drifted rows are the ones Auto Loader rescued, and the twin is
    `WHERE _rescued_data IS NOT NULL`."""
    return tuple(position for position, record in enumerate(records) if DRIFT_COLUMN in record)


def undeclared_keys(records: _Records) -> tuple[str, ...]:
    """Every key some record carries that `COLUMNS` does not declare, sorted.

    THE GATE'S-EYE VIEW OF DRIFT: this is what a reader holding the v1 schema would push
    into `_rescued_data`, and therefore what `opl.bronze.dq.evaluate`'s
    `rescued_data_present` rule fires on. Kept separate from `drifted_rows` because they
    answer different questions -- which rows drifted, and what the drift consisted of --
    and because an undeclared key that is NOT the declared drift column is a generator
    bug rather than an injected defect."""
    return tuple(sorted({key for record in records for key in record if key not in COLUMNS}))
