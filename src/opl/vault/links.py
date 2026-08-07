# src/opl/vault/links.py
"""Load a DV2 link: one row per relationship between hub keys, ever, insert-only.

WHAT A LINK ROW ASSERTS, because everything below follows from it: "these hub keys
were seen together". Not when the relationship started, not when it ended, not what it
looked like -- a link carries its own hash key, one reference per participating hub,
and the two pieces of DV2 metadata saying when WE first saw the pair and where it came
from. Descriptive facts and effectivity windows belong to a satellite on the link,
which this vault does not have yet (`opl.vault.registry` says why, and refuses one).

SO THIS IS `load_hub` AT LINK GRAIN, AND IT IS DELIBERATELY THE SAME SHAPE. The
anti-join-then-append, the earliest-`record_source` aggregate, the injected
`load_date`, the whole-table before/after counts: all four are `opl.vault.hubs`'
decisions, taken there with their arguments, and shared here through
`opl.vault.loading` rather than re-derived. A link that reached for a MERGE, or that
stamped its own clock, would be a second answer to a question already settled -- and
the failure modes are identical, because both tables are insert-only key registries.

NO OBSERVATION LEDGER HERE, for `load_hub`'s reason and it is worth restating at link
grain because this is where it stops being obvious. On estabelecimentos the link keys
and the hub keys move together -- 4 keys leave 2026-07's bronze and all 4 are our own
gate's rejects -- but at SOCIOS link grain 65,444 relationships depart with none of
them quarantined, so absence at link grain is a real and much louder signal than at
hub grain. It is still not this loader's to act on: a link row is insert-only, so
there is nothing here for a departure to change. The table that must consult the
ledger is the EFFECTIVITY satellite that closes a relationship's window, and that is
Task 5's. Wiring the ledger in here would derive an answer nothing in this file could
use, which is the shape `opl.vault.hubs` refused for the hub.

THE HIERARCHICAL CASE IS NOT A SPECIAL CASE. `link_empresa_estabelecimento` is
one-to-many -- every establishment has exactly one company -- and it is still a link
rather than a column on `hub_estabelecimento`, because a hub row asserts only that a
key exists and a hub carrying a reference to another hub would stop being join-safe
for anything else keying on it. Nothing in this module knows the cardinality; a
many-to-many link loads through the same code."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.vault.columns import LOAD_DATE, RECORD_SOURCE
from opl.vault.hashing_spark import refuse_non_string_columns
from opl.vault.loading import (
    BRONZE_RECORD_SOURCE,
    SNAPSHOT_MONTH_COLUMN,
    earliest_record_source,
    hash_key_expression,
    link_hash_key_expression,
    read_snapshot_window,
    rows_in,
)
from opl.vault.registry import Hub, Link


@dataclass(frozen=True)
class LinkLoadResult:
    """What one link load did. Both numbers are derived from the target's own row count
    before and after the append, so `appended` is what LANDED rather than what was
    planned -- there is no state in which this object reports a write that did not
    happen."""

    table: str
    appended: int
    # What the target already held before this load, whole-table and not window-scoped,
    # for `HubLoadResult.already_present`'s reason: the link may hold relationships
    # from months outside the window, and reporting a narrower number than the one that
    # was measured would be a claim the count cannot support.
    already_present: int


def _refuse_mismatched_hubs(link: Link, hubs: Sequence[Hub]) -> None:
    """The link and its hubs arrive as two arguments, so something has to check they
    belong together -- AND THAT THEY ARRIVED IN THE RIGHT ORDER.

    They are separate arguments for `_refuse_a_mismatched_hub`'s reason: a loader that
    resolved the hubs through the module-level registry could not be tested against a
    throwaway spec, and the registry is the thing wave 2 must extend without this file
    changing. The cost is this check, and here it is strictly larger than the
    satellite's, because a link takes a LIST. A wrong hub gives a link that joins to
    nothing; a right pair in the WRONG ORDER gives a link whose two reference columns
    are correct and whose own hash key is a digest over the business keys concatenated
    backwards -- so every row is present, every join works, and the table's identity
    column disagrees with the one a re-load computes."""
    supplied = tuple(hub.name for hub in hubs)
    if supplied != tuple(link.hubs):
        raise ValueError(
            f"link {link.name!r} joins {tuple(link.hubs)} and was handed {supplied}. "
            "The link's own hash key is the standard over these hubs' business keys "
            "CONCATENATED IN ORDER, so a reordered pair re-keys the whole table while "
            "every reference column stays correct and every join keeps working -- "
            "resolve them with opl.vault.domains.linked_hubs rather than by hand"
        )


def link_candidates(
    spark: SparkSession,
    link: Link,
    hubs: Sequence[Hub],
    *,
    source_table: str,
    months: Sequence[str] | None,
) -> DataFrame:
    """One row per relationship in the window: the link's hash key, one hash-key
    reference per participating hub, and the `record_source` of the earliest month the
    relationship appeared in.

    EVERY HUB'S BUSINESS KEY MUST BE A COLUMN OF THIS ONE SOURCE, which is what makes a
    link loadable from a single table at all. `refuse_non_string_columns` says so by
    name: estabelecimentos carries `cnpj_basico` (hub_empresa's whole key) as well as
    the establishment triple, so one scan produces both references. A link whose ends
    live in two sources needs a join and is a different loader; refusing here means it
    arrives as an error naming the missing column rather than as a NULL reference.

    THE REFERENCES ARE COMPUTED, NOT LOOKED UP. Joining to the hubs to fetch their
    digests would make this load depend on the hubs having been loaded first and would
    silently drop a relationship whose hub row is missing. `hash_key_expression` is the
    same function `load_hub` keys with, so the digests agree by construction rather
    than by ordering."""
    source = read_snapshot_window(spark, source_table, months)
    components = [name for hub in hubs for name in hub.business_key_columns]
    refuse_non_string_columns(source, components)
    keyed = source.select(
        link_hash_key_expression(hubs).alias(link.hash_key),
        *(hash_key_expression(hub).alias(hub.hash_key) for hub in hubs),
        F.col(SNAPSHOT_MONTH_COLUMN),
        F.col(BRONZE_RECORD_SOURCE),
    )
    return earliest_record_source(
        keyed, [link.hash_key, *(hub.hash_key for hub in hubs)]
    )


def load_link(
    spark: SparkSession,
    link: Link,
    *,
    hubs: Sequence[Hub],
    source_table: str,
    target_table: str,
    load_date: datetime,
    months: Sequence[str] | None = None,
) -> LinkLoadResult:
    """Append every relationship of `source_table` that `target_table` does not already
    hold, stamped with `load_date`.

    IDEMPOTENT BY ANTI-JOIN ON THE LINK'S OWN HASH KEY, not on the tuple of hub
    references. The two select the same set -- the digest is a function of the
    references' inputs -- and the hash key is the better column to compare on: one
    instead of N, and the one a downstream satellite would key on. The three shapes
    `load_hub` weighed apply unchanged (not a MERGE, not delete-then-append, and NOT
    concurrency-safe), and `load_date` has no default for `load_hub`'s reason: a loader
    that stamps its own clock cannot be asserted against."""
    _refuse_mismatched_hubs(link, hubs)
    before = rows_in(spark, target_table)
    candidates = link_candidates(
        spark, link, hubs, source_table=source_table, months=months
    )
    if before:
        candidates = candidates.join(
            spark.read.table(target_table).select(link.hash_key),
            on=link.hash_key,
            how="left_anti",
        )
    (
        candidates.select(
            link.hash_key,
            *(hub.hash_key for hub in hubs),
            F.lit(load_date).alias(LOAD_DATE),
            F.col(RECORD_SOURCE),
        )
        .write.format("delta").mode("append").saveAsTable(target_table)
    )
    return LinkLoadResult(
        table=target_table,
        appended=rows_in(spark, target_table) - before,
        already_present=before,
    )
