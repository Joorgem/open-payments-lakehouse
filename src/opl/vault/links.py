# src/opl/vault/links.py
"""Load a DV2 link: one row per relationship between hub keys, ever, insert-only.

WHAT A LINK ROW ASSERTS, because everything below follows from it: "these hub keys
were seen together". Not when the relationship started, not when it ended, not what it
looked like -- a link carries its own hash key, one reference per participating hub,
and the two pieces of DV2 metadata saying when WE first saw the pair and where it came
from. Descriptive facts and effectivity windows belong to a satellite on the link --
and since Task 5 the vault has one of those two: `sat_eff_company_partner`, an
`EffectivitySatellite` loaded by `opl.vault.effectivity` and admitted by
`registry._assert_every_effectivity_satellite_hangs_off_a_link`. A DESCRIPTIVE
satellite on a link still does not exist and is still refused
(`registry._assert_every_satellite_hangs_off_a_hub`); `registry.py`'s "WHAT IS STILL
DELIBERATELY NOT HERE" says why. (This paragraph read "which this vault does not have
yet" until Task 7's correction pass -- true at Task 4, falsified by Task 5 one commit
range later, in the module the reader meets first.)

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
many-to-many link loads through the same code.

THE HUBS ARE NOT JOINED AND ARE STILL REQUIRED TO EXIST, which is not a contradiction
and is `refuse_unloaded_hubs`' whole subject. `link_candidates` COMPUTES each reference
from the source, so the digests agree with `load_hub`'s by construction and not by
ordering -- that property is unchanged. What ordering decides is whether the rows this
loader appends point at hub rows that are THERE, and on an insert-only table a dangling
reference is not repaired by a later load; somebody deletes rows by hand. So the hubs
arrive as `hub_tables`, a mapping from hub name to the table it was loaded into, and the
loader refuses before it writes if one of them is missing or empty. Read that function
for exactly what the refusal covers and, more importantly, what it does not.

`hub_tables` HAS NO DEFAULT ON EITHER LINK LOADER, and that is the argument for the shape
rather than a detail of it. An optional preflight is one a later job forgets to switch
on, and forgetting is the whole defect: `vault_partner_job.yml` has carried a paragraph
describing this hazard since it was written, and nothing enforced it. The two SPEC
refusals still run ahead of the preflight, because they are pure and because a caller who
mismatched the hubs should be told that rather than that the wrong hub's table is empty."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.vault.columns import LOAD_DATE, RECORD_SOURCE
from opl.vault.hashing_spark import refuse_non_string_columns
from opl.vault.loading import (
    BRONZE_RECORD_SOURCE,
    MONTHLY_SNAPSHOT,
    SnapshotAxis,
    earliest_record_source,
    hash_key_for_end,
    link_hash_key_expression,
    read_snapshot_window,
    rows_in,
)
from opl.vault.registry import Hub, Link, LinkEnd, identifying_hubs


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


def refuse_mismatched_hubs(link: Link, hubs: Sequence[Hub]) -> None:
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
    if supplied != link.hub_names:
        raise ValueError(
            f"link {link.name!r} joins {link.hub_names} and was handed {supplied}. "
            "The link's own hash key is the standard over these hubs' business keys "
            "CONCATENATED IN ORDER, so a reordered pair re-keys the whole table while "
            "every reference column stays correct and every join keeps working -- "
            "resolve them with opl.vault.domains.linked_hubs rather than by hand"
        )


def _refuse_a_hub_that_was_never_loaded(link: Link, hub: Hub, table: str, rows: int | None) -> None:
    """One hub's verdict: `rows is None` means the table is not there, 0 means it is
    there and empty, anything else passes.

    TWO MESSAGES BECAUSE THEY ARE TWO MISTAKES WITH TWO REPAIRS. A table that does not
    exist is a job that never ran in this workspace; a table that exists and holds
    nothing is a job that ran over a window which loaded nothing, and re-running the
    same window will do it again. One message covering both would send half its readers
    to the wrong command."""
    if rows is None:
        raise ValueError(
            f"link {link.name!r} references hub {hub.name!r}, and {table!r} does not "
            "exist. The link's references are COMPUTED from the source rather than "
            "joined to the hub, so this load would not fail -- it would append every "
            "relationship in the window with hash keys pointing at hub rows that are "
            "not there, and report success. Load that hub first; across two jobs no "
            "`depends_on` can say so, which is why this is checked here"
        )
    if rows == 0:
        raise ValueError(
            f"link {link.name!r} references hub {hub.name!r}, and {table!r} holds no "
            "rows. Every reference this load writes would dangle, silently, on a table "
            "that is insert-only -- so the repair is deleting rows by hand rather than "
            "re-running. A hub that exists and is empty is a hub whose own load ran "
            "over a window that matched nothing; check that window before this one"
        )


def refuse_unloaded_hubs(
    spark: SparkSession, link: Link, hubs: Sequence[Hub], hub_tables: Mapping[str, str]
) -> None:
    """Refuse, before anything is written, if a hub `link` references is MISSING or
    EMPTY in the workspace this load is writing into.

    WHAT IT CATCHES IS ONE THING AND IT IS THE THING THAT HAPPENED. `vault_partner_job`
    loads `link_company_partner`, both of whose ends reference `hub_empresa` -- a hub
    that job does not load and `vault_empresa_job` does. A Databricks `depends_on` does
    not cross a job boundary, so the ordering was an operator's to get right, and the
    wrong order does not fail: 28M link rows land with `company_hub_empresa_hk` pointing
    at nothing, and the run reports success.

    WHAT IT DOES NOT CATCH, said plainly because a guard that oversells its coverage is
    worse than none. This is an EXISTENCE test, not referential integrity:

      - A hub that exists and is populated but is missing SOME referenced key. The
        anti-join that would catch it costs about an extra full pass -- measured here at
        2,606 s for `hub_empresa_from_estabelecimentos`, anti-joining 144M rows to
        insert zero -- which is the wrong price to pay on every load, forever, for an
        ordering mistake. The partial case is also not the measured one: all 310,374 of
        310,374 PJ partner CNPJs resolve to `hub_empresa`
        (`01f19063-44ef-132a-8aa7-9068b624b370`), and the company end is derived from
        `cnpj_basico`, which is `hub_empresa`'s own business key. Measurements of two
        months, not guarantees -- and this guard is not one either.
      - A hub loaded over a NARROWER window than the link. It is non-empty, so this
        passes, and the link's keys from months the hub never saw dangle.
      - A `hub_tables` entry naming some other populated table. The probe asks whether a
        table has rows, not whether it is the hub."""
    for hub in hubs:
        table = hub_tables.get(hub.name)
        if table is None:
            raise ValueError(
                f"link {link.name!r} references hub {hub.name!r} and `hub_tables` names "
                f"{sorted(hub_tables)}. Every hub a link references must be named, or "
                "the one left out is the one whose absence this refusal exists to find"
            )
        # `rows_in` IS THE PROBE BECAUSE IT IS THE COUNT THIS LOADER ALREADY TAKES of
        # its own target one call later: Delta answers an unfiltered `count()` from the
        # transaction log's file statistics rather than by scanning, so the preflight is
        # two catalog operations per hub and no pass over the data. `tableExists` is
        # asked separately only so "never created" and "created and empty" can be told
        # apart -- `rows_in` alone answers 0 for both.
        rows = rows_in(spark, table) if spark.catalog.tableExists(table) else None
        _refuse_a_hub_that_was_never_loaded(link, hub, table, rows)


def undeclared_derived_ends(link: Link) -> tuple[LinkEnd, ...]:
    """The ends of `link` whose business key is neither named columns of the source nor
    a declared derivation.

    THIS IS THE CONDITION `_refuse_a_link_this_loader_cannot_write` TESTS, RE-DERIVED IN
    F-DB RATHER THAN RELAXED, and named so the job-wiring lock can route a link to an
    entry point by asking the same question rather than restating it. The old spelling
    was `not end.identifying`, and that flag was a PROXY: the refusal's own docstring
    gave the reason as "a non-identifying end's business key is not a column of the
    source under the hub's own name -- it is derived". `LinkEnd.key_from` makes that
    reason checkable directly, and the two questions come apart the moment an end is
    derived AND identifying -- `link_merchant_empresa`'s empresa end, whose `cnpj` must
    be in the link's digest or a merchant re-pointed to another company keeps its link
    hash key and no window is ever closed.

    SO THE GUARD IS STRICTLY SHARPER, NOT WEAKER. `link_company_partner`'s partner end
    declares no `key_from` and is non-identifying, so it is still refused, and its
    derivation still lives in `opl.vault.partners` -- which is not migrated, because
    that link also carries dependent-child keys this loader does not write and the
    migration would buy no capability while touching a loader proven over 33.13 GB."""
    return tuple(
        end for end in link.ends if end.key_from is None and not end.identifying
    )


def _refuse_a_link_this_loader_cannot_write(link: Link) -> None:
    """This loader writes one hub reference per end and nothing else.

    NOT IN `refuse_mismatched_hubs`, WHICH IS SHARED. That function answers "do these
    hubs belong to this link", which `opl.vault.partners` asks too; this one answers
    "can THIS loader write it", which is a statement about this module. Putting the two
    together made the partner loader refuse the very link it exists for.

    WHAT IT PREVENTS IS NOT A CRASH. A link with an UNDECLARED derived end would still
    load here: `link_candidates` would compute that end's reference from the columns its
    hub is NAMED after, so both ends of `link_company_partner` would be hashed from
    `cnpj_basico` and every relationship would read as a company partnered with
    itself -- right row count, working joins, nonsense. And its dependent-child keys
    would be hashed into the link's key by `link_hash_key_expression` and then not
    written, so the table's identity column would describe columns it does not have."""
    undeclared = undeclared_derived_ends(link)
    if link.dependent_child_keys or undeclared:
        raise ValueError(
            f"link {link.name!r} declares dependent-child keys or an end whose business "
            f"key is neither named columns of the source nor a declared derivation "
            f"({[end.hub for end in undeclared]}), and this loader writes hub references "
            "and nothing else. Such an end is derived, so both ends would be hashed from "
            "the same column and every relationship would read as a company partnered "
            "with itself. Either declare the derivation with `LinkEnd.key_from`, which "
            "this loader CAN write and `build_registry` checks against the hub, or use "
            "`opl.vault.partners.load_partner_link`, which knows socios' own; see its "
            "module docstring for why that one is separate"
        )


def reference_columns(link: Link, hubs: Sequence[Hub]) -> list[str]:
    """The link's hash key followed by one reference column per end, in write order.

    Spelled once and read by the projection, the aggregate and the final `select`, so
    the three cannot disagree about a role."""
    return [
        link.hash_key,
        *(end.reference_column(hub) for end, hub in zip(link.ends, hubs, strict=True)),
    ]


def source_columns(link: Link, hubs: Sequence[Hub]) -> list[str]:
    """Every column of the ONE source this loader reads a hub business key out of, in
    end order -- the list `refuse_non_string_columns` is handed.

    EVERY END'S BUSINESS KEY MUST BE READABLE FROM THIS ONE SOURCE, which is what makes
    a link loadable from a single table at all: estabelecimentos carries `cnpj_basico`
    (hub_empresa's whole key) as well as the establishment triple, so one scan produces
    both references. A link whose ends live in two sources needs a join and is a
    different loader; refusing on this list means it arrives as an error naming the
    missing column rather than as a NULL reference.

    "READABLE FROM", NOT "NAMED AFTER", SINCE F-DB, AND THAT IS ONE WORD OF WIDENING
    RATHER THAN A NEW CAPABILITY. `link_candidates`' docstring read "EVERY HUB'S BUSINESS
    KEY MUST BE A COLUMN OF THIS ONE SOURCE", which is what made T5's original ruling look
    buildable: `bronze_merchant` carries `cnpj` and no `cnpj_basico`, so `hub_empresa`'s
    key is not a column of it under that name. `LinkEnd.source_columns` answers where each
    end's key really lives -- the hub's own names, or the columns a `key_from` declares --
    and both the refusal and the expression read that one answer. What is unchanged is the
    requirement: ONE source, one scan, no join. `hash_key_for_end` is the expression half.

    A NAMED FUNCTION RATHER THAN A COMPREHENSION INSIDE THE CALLER, because the paragraph
    above is what it is FOR, and `link_candidates` reached this project's 50-line function
    cap carrying it. The prose moves with the code it describes."""
    return [
        name
        for end, hub in zip(link.ends, hubs, strict=True)
        for name in end.source_columns(hub)
    ]


def link_candidates(
    spark: SparkSession,
    link: Link,
    hubs: Sequence[Hub],
    *,
    source_table: str,
    months: Sequence[str] | None,
    axis: SnapshotAxis = MONTHLY_SNAPSHOT,
) -> DataFrame:
    """One row per relationship in the window: the link's hash key, one hash-key
    reference per participating hub, and the `record_source` of the earliest month the
    relationship appeared in.

    ONE SOURCE, ONE SCAN, AND EVERY END'S KEY READ OUT OF IT -- see `source_columns`
    above, which is the list this refuses on and where that requirement is argued.

    THE REFERENCES ARE COMPUTED, NOT LOOKED UP. Joining to the hubs to fetch their
    digests would make this load depend on the hubs having been loaded first and would
    silently drop a relationship whose hub row is missing. `hash_key_for_end` reaches
    `hash_key_column` through the hub's own widths and order for every end -- directly
    for an end the source names, and through `hash_key_over` for a declared derivation --
    so the digests agree with `load_hub`'s by construction rather than by ordering.

    THE REFERENCE COLUMN NAME COMES FROM THE END, NOT FROM THE HUB, and that is a
    correction rather than a nicety. `build_registry` validates a link's columns under
    `LinkEnd.reference_column`, which prefixes the role; this loader used to write them
    under `hub.hash_key`, which ignores it. A link with two roled ends on one hub was
    therefore validated as two distinct columns and written as one -- the exact
    collision the registry guard exists to prevent, reached by passing it."""
    source = read_snapshot_window(spark, source_table, months, axis=axis)
    refuse_non_string_columns(source, source_columns(link, hubs))
    keyed = source.select(
        link_hash_key_expression(link, identifying_hubs(link, hubs)).alias(link.hash_key),
        *(
            hash_key_for_end(end, hub).alias(end.reference_column(hub))
            for end, hub in zip(link.ends, hubs, strict=True)
        ),
        F.col(axis.column),
        F.col(BRONZE_RECORD_SOURCE),
    )
    return earliest_record_source(keyed, reference_columns(link, hubs), axis=axis)


def load_link(
    spark: SparkSession,
    link: Link,
    *,
    hubs: Sequence[Hub],
    hub_tables: Mapping[str, str],
    source_table: str,
    target_table: str,
    load_date: datetime,
    months: Sequence[str] | None = None,
    axis: SnapshotAxis = MONTHLY_SNAPSHOT,
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
    _refuse_a_link_this_loader_cannot_write(link)
    refuse_mismatched_hubs(link, hubs)
    refuse_unloaded_hubs(spark, link, hubs, hub_tables)
    before = rows_in(spark, target_table)
    candidates = link_candidates(
        spark, link, hubs, source_table=source_table, months=months, axis=axis
    )
    if before:
        candidates = candidates.join(
            spark.read.table(target_table).select(link.hash_key),
            on=link.hash_key,
            how="left_anti",
        )
    (
        candidates.select(
            *reference_columns(link, hubs),
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
