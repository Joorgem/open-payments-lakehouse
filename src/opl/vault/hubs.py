# src/opl/vault/hubs.py
"""Load a DV2 hub: one row per business key, ever, insert-only.

WHAT A HUB ROW ASSERTS, because everything below follows from it: "this business key
exists". Not when it was true, not what it looked like -- a hub carries the key, its
hash, and the two pieces of DV2 metadata that say when WE first saw it and where it
came from. Descriptive facts belong to a satellite and relationships to a link, and a
hub that grew either would stop being join-safe for the other tables keying on it.

INSERT-ONLY IS WHY THERE IS NO DELETE PATH AND NO OBSERVATION LEDGER HERE. A key that
vanishes from a later snapshot has not stopped existing -- and on this data it does not
even vanish: all 68,629,147 empresas keys of 2026-06 are present in 2026-07, because
the RFB retains baixadas rather than removing them. The satellite consults the ledger
because absence is a statement about a PAYLOAD at a point in time; a hub has no such
statement to make, so wiring the ledger in here would be a derivation whose answer
nothing could act on. `opl.vault.satellites` is where it belongs and is where it is.

IDEMPOTENCE IS AN ANTI-JOIN, NOT A MERGE, and the three shapes were weighed the way
`opl.bronze.promote` weighed its own:
  - not MERGE: a MERGE rewrites files to reach a state that is already correct, and on
    a Free Edition workspace the rewrite of a 69M-row hub is the expensive half of a
    load that should be a no-op.
  - not delete-then-append: two Delta commits, so the failure window between them
    leaves the hub MISSING keys it already held.
  - an anti-join on the hash key, then an append. A key already present contributes
    nothing, which makes a re-run free as well as safe, and there is exactly one
    commit. NOT concurrency-safe: two loads running at once can both see a key absent
    and both append it. Same assumption as `promote_batch` -- the flow runs one load
    per run and an operator run is a deliberate human action.

`load_date` IS AN ARGUMENT AND HAS NO DEFAULT, which is the only shape a test can
assert. A loader stamping `current_timestamp()` internally can only be checked against
another clock reading, which passes for every implementation including a broken one --
and in the data it would make the hub's LDTS a record of when the pipeline happened to
run rather than a value the job's own parameters pin. Same argument, and the same
no-default, as `add_audit_columns`' `snapshot_month`."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.vault.columns import LOAD_DATE, RECORD_SOURCE
from opl.vault.hashing_spark import refuse_non_string_columns, zero_padded_column
from opl.vault.loading import (
    BRONZE_RECORD_SOURCE,
    SNAPSHOT_MONTH_COLUMN,
    earliest_record_source,
    hash_key_expression,
    read_snapshot_window,
    rows_in,
)
from opl.vault.registry import Hub


@dataclass(frozen=True)
class HubLoadResult:
    """What one hub load did. Both numbers are derived from the target's own row count
    before and after the append, so `appended` is what LANDED rather than what was
    planned -- there is no state in which this object reports a write that did not
    happen."""

    table: str
    appended: int
    # What the target already held before this load. Not "how many of this window's
    # keys were already there": the hub may hold keys from months outside the window,
    # and reporting a narrower number than the one that was measured would be a claim
    # the count cannot support.
    already_present: int


def hub_candidates(
    spark: SparkSession, hub: Hub, *, source_table: str, months: Sequence[str] | None
) -> DataFrame:
    """One row per business key in the window: the hash key, the padded business key,
    and the `record_source` of the EARLIEST month that key appeared in.

    EARLIEST, NOT ARBITRARY, and the aggregate that says so lives in
    `opl.vault.loading.earliest_record_source` -- shared with `load_link`, which is
    insert-only for the same reason and would otherwise carry a second spelling of it.
    See that function for why `min` and not `first`."""
    source = read_snapshot_window(spark, source_table, months)
    refuse_non_string_columns(source, hub.business_key_columns)
    keyed = source.select(
        hash_key_expression(hub).alias(hub.hash_key),
        # `zero_padded_column` again rather than a bare `lpad`, so the value stored
        # beside the digest is produced by the SAME function the digest was taken
        # over. A bare `lpad` here would be a second spelling that truncates where
        # the hash refuses -- and on the row where they disagreed, the hub would keep
        # the truncated key.
        *(
            F.col(key.name).alias(key.name) if key.width is None
            else zero_padded_column(F.col(key.name), width=key.width).alias(key.name)
            for key in hub.business_keys
        ),
        F.col(SNAPSHOT_MONTH_COLUMN),
        F.col(BRONZE_RECORD_SOURCE),
    )
    return earliest_record_source(keyed, [hub.hash_key, *hub.business_key_columns])


def load_hub(
    spark: SparkSession,
    hub: Hub,
    *,
    source_table: str,
    target_table: str,
    load_date: datetime,
    months: Sequence[str] | None = None,
) -> HubLoadResult:
    """Append every business key of `source_table` that `target_table` does not
    already hold, stamped with `load_date`.

    The write is a plain Delta append, which is one atomic commit, so there is no
    partial-hub state to recover from -- the same property `promote_batch` relies on
    and for the same reason.

    THE PADDED BUSINESS KEY IS WHAT IS STORED, not the raw column, so the value beside
    the digest is the value the digest was taken over. Storing the raw one would make
    the hub's own key column disagree with its hash for any row that needed padding --
    a discrepancy nothing would fail on, and which a human reconciling a digest by hand
    would hit first."""
    before = rows_in(spark, target_table)
    candidates = hub_candidates(spark, hub, source_table=source_table, months=months)
    if before:
        candidates = candidates.join(
            spark.read.table(target_table).select(hub.hash_key), on=hub.hash_key, how="left_anti"
        )
    (
        candidates.select(
            hub.hash_key,
            *hub.business_key_columns,
            F.lit(load_date).alias(LOAD_DATE),
            F.col(RECORD_SOURCE),
        )
        .write.format("delta").mode("append").saveAsTable(target_table)
    )
    return HubLoadResult(
        table=target_table,
        appended=rows_in(spark, target_table) - before,
        already_present=before,
    )
