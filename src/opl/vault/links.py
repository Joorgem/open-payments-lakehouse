# src/opl/vault/links.py
"""Load a DV2 link: one row per relationship between hub keys, ever, insert-only.

WHAT A LINK ROW ASSERTS, because everything below follows from it: "these hub keys
were seen together". Not when the relationship started, not when it ended, not what it
looked like -- a link carries its own hash key, one reference per participating hub, any
DEPENDENT-CHILD KEYS it is identified by, and the two pieces of DV2 metadata saying when
WE first saw the relationship and where it came from. (This module wrote no
dependent-child key and refused every link declaring one until F2 wave 2, which is a
deferral ADR 0011 recorded by name and `_refuse_a_link_this_loader_cannot_write` still
carries the argument for; `link_payment` is the table that consumed it.)
Descriptive facts and effectivity windows belong to a satellite on the link -- and
since F2 wave 2 the vault has BOTH: `sat_eff_company_partner`, an `EffectivitySatellite`
loaded by `opl.vault.effectivity` and admitted by
`registry._assert_every_effectivity_satellite_hangs_off_a_link` (Task 5); and
`sat_link_payment`, an ordinary `Satellite` whose parent is `link_payment`, loaded by
`opl.vault.satellites` and admitted by
`registry_satellites.assert_every_satellite_hangs_off_a_hub_or_a_link`. (This paragraph
read "which this vault does not have yet" until Task 7's correction pass -- true at Task
4, falsified by Task 5 one commit range later -- and then said the descriptive one "still
does not exist and is still refused", which F2 wave 2 falsified in turn. Twice now, in
the module the reader meets first, a sentence about what does not exist has outlived the
thing not existing.)

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

    They are separate arguments for `satellites._resolved_parent`'s reason: a loader that
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


def non_identifying_ends(link: Link) -> tuple[LinkEnd, ...]:
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

    THE CONDITION IS `not end.identifying` ALONE, and the T1 review is why. The narrower
    `key_from is None and not identifying` shipped first, and the reviewer broke it by
    construction -- declaring `key_from=(KeyPrefix("cpf_cnpj_socio", 8),)` on socios'
    partner end made this loader ACCEPT `link_company_partner`. The refusal's own docstring
    carries what that would have written.

    WHY `identifying` IS PRINCIPLED HERE AND NOT A PROXY. `LinkEnd` defines
    `identifying=False` as "a reference the link RESOLVES rather than one it is IDENTIFIED
    BY" -- a FUNCTION of the link's identity. This loader computes each end's reference from
    that end's own columns, independently of the other ends, so such an end is exactly the
    one it cannot compute, whatever it declares. That does NOT conflate `identifying` with
    `key_from` (the defect F-DB corrected): a derived AND identifying end is still written.
    `identifying=False` is declared in one place in this repository (`domains/cnpj.py`), so
    this refuses no link that exists today.

    HALF OF WHY THAT LOADER IS NOT MIGRATED EXPIRED IN F2 WAVE 2, AND IT IS CORRECTED
    HERE RATHER THAN LEFT TO READ AS STILL TRUE. The reason given was "that link also
    carries dependent-child keys this loader does not write", and this loader writes them
    now. What is unchanged is the reason that actually decides it: socios' partner root is
    a CONDITIONAL slice -- the first eight characters of `cpf_cnpj_socio`, and only where
    `identificador_socio` says the partner is a company -- which no `KeyPrefix` can
    express and which `opl.vault.specs.KeyPrefix` refuses to grow an escape hatch for. So
    the migration is not merely unprofitable against a loader proven over 33.13 GB; it is
    not expressible in the declaration this loader reads."""
    return tuple(end for end in link.ends if not end.identifying)


def _refuse_a_link_this_loader_cannot_write(link: Link) -> None:
    """This loader writes one hub reference per end, the link's dependent-child keys, and
    nothing else.

    NOT IN `refuse_mismatched_hubs`, WHICH IS SHARED. That function answers "do these
    hubs belong to this link", which `opl.vault.partners` asks too; this one answers
    "can THIS loader write it", which is a statement about this module. Putting the two
    together made the partner loader refuse the very link it exists for.

    WHAT IT PREVENTS IS NOT A CRASH. A link with an UNDECLARED derived end would still
    load here: `link_candidates` would compute that end's reference from the columns its
    hub is NAMED after, so both ends of `link_company_partner` would be hashed from
    `cnpj_basico` and every relationship would read as a company partnered with
    itself -- right row count, working joins, nonsense.

    THE DEPENDENT-CHILD-KEY ARM IS GONE, AND IT WAS HALF OF THIS REFUSAL UNTIL F2 WAVE 2.
    It refused every link carrying one, on the ground that they "would be hashed into the
    link's key by `link_hash_key_expression` and then not written, so the table's identity
    column would describe columns it does not have" -- true of the loader as it stood, and
    ADR 0011 recorded the fix as deliberately deferred rather than impossible: "a small
    change -- `link_hash_key_expression` already hashes them -- and it should be made by
    the wave-2 task that has a table to point at it." `link_payment` is that table, and
    the projection is now in `link_candidates` and `link_columns`, so the reason this arm
    existed is gone rather than waived.

    THE TWO CONDITIONS WERE NEVER ONE, WHICH IS WHY THIS IS A NARROWING AND NOT A
    DELETION. `link_company_partner` satisfied BOTH, so a reader could take the refusal
    for a single rule about "complicated links"; it is still refused, by the arm that
    survives, and `tests/vault/test_payments_vault.py` fires both directions against the
    two real links so the pair cannot quietly collapse back into one."""
    undeclared = non_identifying_ends(link)
    if undeclared:
        raise ValueError(
            f"link {link.name!r} declares a NON-IDENTIFYING end "
            f"({[end.hub for end in undeclared]}), and this loader computes every end's "
            "reference from that end's own source columns, INDEPENDENTLY of the other "
            "ends. A non-identifying end is one the link RESOLVES rather than one it is "
            "identified by -- its reference is a function of the link's identity -- so "
            "computing it independently is exactly the wrong derivation, and it produces "
            "a plausible digest rather than an error. Declaring `LinkEnd.key_from` does "
            "NOT make it writable here and this refusal no longer accepts that as an "
            "answer: a `KeyPrefix` cannot express a CONDITIONAL slice, which is what "
            "socios' partner root is. Use `opl.vault.partners.load_partner_link`, which "
            "knows "
            "socios' own; see its module docstring for why that one is separate"
        )
    _refuse_a_width_bearing_dependent_child_key(link)


def _refuse_a_width_bearing_dependent_child_key(link: Link) -> None:
    """A dependent-child key declaring a `width` is refused, because this loader would
    HASH THE PADDED VALUE AND WRITE THE RAW ONE.

    FOUND BY THE T1 REVIEW, MEASURED ON A REAL SESSION rather than reasoned about: with
    `BusinessKeyColumn(name="transaction_id", width=12)`, `link_hash_key_expression`
    composes `lpad(transaction_id, 12, '0')` into the digest (`loading._padded`) while
    `link_columns` projects the bare column. The table's identity column is then a digest
    over a value the table does not hold, so the key cannot be recomputed from the row --
    and nothing fails, which is this repository's defining failure shape.

    THE LATENCY IS OLDER THAN THIS PHASE AND THAT IS NOT A REASON TO LEAVE IT. The same
    mismatch exists in `opl.vault.partners`, which shipped over 33.13 GB; socios' two
    dependent-child keys declare no width, so it was never reachable. What F2 wave 2
    changed is that the generic loader now writes dependent-child keys at all, so the
    first widthed declaration anywhere reaches it.

    REFUSED RATHER THAN REPAIRED, AND THE REPAIR IS NAMED SO THE CHOICE IS VISIBLE. The
    arguably better fix is to PROJECT the padded value, making the table hold the
    canonical form that was hashed -- which is what `zero_padded_column` does for a hub's
    business key, so there is a precedent pulling that way. It is not taken here because
    `link_columns` returns NAMES, read by the projection, the `GROUP BY` and the final
    `select` alike, and threading an expression through all three would change a write
    path proven over 33.13 GB to serve a declaration no table in this repository makes.
    **Refusing costs the first caller who wants one a decision; projecting would cost
    every existing row a re-derivation.** If a widthed dependent-child key is ever really
    wanted, delete this refusal and pad the projection -- do not do half of it."""
    widthed = [key.name for key in link.dependent_child_keys if key.width is not None]
    if widthed:
        raise ValueError(
            f"link {link.name!r} declares dependent-child keys with a width {widthed}, "
            "and this loader hashes the PADDED value into the link's hash key while "
            "projecting the RAW column -- so the identity column would be a digest over a "
            "value the row does not carry, and no re-load could reproduce it. Declare the "
            "key without a width if the source value is already canonical, or pad it "
            "upstream of this loader so the hashed and the written value are the same"
        )


def reference_columns(link: Link, hubs: Sequence[Hub]) -> list[str]:
    """The link's hash key followed by one reference column per end, in write order.

    Spelled once and read by `link_columns`, so the projection, the aggregate and the
    final `select` cannot disagree about a role."""
    return [
        link.hash_key,
        *(end.reference_column(hub) for end, hub in zip(link.ends, hubs, strict=True)),
    ]


def link_columns(link: Link, hubs: Sequence[Hub]) -> list[str]:
    """Every column the link carries that is not DV2 metadata, in write order: its hash
    key, one reference per end, then its dependent-child keys.

    THE DEPENDENT-CHILD KEYS COME LAST AND IN DECLARATION ORDER, matching
    `loading.link_hash_key_expression`, which hashes them after every hub. The two orders
    do not have to agree for the table to be correct -- these are column names and those
    are digest components -- but a reader reconciling a row by hand reads them side by
    side, and two orders would make that harder for no gain.

    ONE SPELLING, SHARED WITH `opl.vault.partners`, which had a private copy of this list
    under the name `_link_columns` from Task 5 until F2 wave 2 gave the generic loader the
    same projection. Two spellings of "what a link row holds" is how one loader gains a
    column the other silently drops: both write into tables the registry validated under
    ONE set of names, and a mismatch lands as a Delta schema error in a job rather than
    here.

    READ BY THE AGGREGATE AS WELL AS BY THE WRITE, which is the load-bearing half.
    `earliest_record_source` GROUPS BY this list, so a column missing from it is a column
    the fold collapses on -- two payments between one pair would become one row before
    anything was written, with the load reporting success."""
    return [*reference_columns(link, hubs), *link.dependent_child_key_columns]


def source_columns(link: Link, hubs: Sequence[Hub]) -> list[str]:
    """Every column of the ONE source this loader reads a KEY COMPONENT out of, in hash
    order -- each end's hub business key, then the dependent-child keys. The list
    `refuse_non_string_columns` is handed.

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

    THE DEPENDENT-CHILD KEYS ARE IN THIS LIST SINCE F2 WAVE 2, and they belong in it for
    the ends' own reason rather than by analogy. They are hashed into the link's digest by
    `link_hash_key_expression` -- through `_padded`, which reaches the same
    `hash_key_column` the references do -- so the hash standard's precondition applies to
    them: STRING columns, refused here by name. Left out, a `transaction_id` that arrived
    as a bigint would be cast silently and hashed as the cast, giving a table of plausible
    digests no re-load over a string column could ever reproduce. `opl.vault.partners`
    already refused its own two by name; this is the same list, derived.

    A NAMED FUNCTION RATHER THAN A COMPREHENSION INSIDE THE CALLER, because the paragraph
    above is what it is FOR, and `link_candidates` reached this project's 50-line function
    cap carrying it. The prose moves with the code it describes."""
    return [
        *(
            name
            for end, hub in zip(link.ends, hubs, strict=True)
            for name in end.source_columns(hub)
        ),
        *link.dependent_child_key_columns,
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
    reference per participating hub, its dependent-child keys, and the `record_source` of
    the earliest month the relationship appeared in.

    ONE SOURCE, ONE SCAN, AND EVERY KEY COMPONENT READ OUT OF IT -- see `source_columns`
    above, which is the list this refuses on and where that requirement is argued.

    THE DEPENDENT-CHILD KEYS ARE PROJECTED AS WELL AS HASHED, which is F2 wave 2's whole
    change here and is two properties rather than one. They reach the WRITE, so the
    table's identity column no longer describes columns the table does not have; and they
    reach the GROUP BY through `link_columns`, so two relationships differing only in a
    dependent-child key stay two rows instead of being folded into one by the aggregate.

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
        *(F.col(name) for name in link.dependent_child_key_columns),
        F.col(axis.column),
        F.col(BRONZE_RECORD_SOURCE),
    )
    return earliest_record_source(keyed, link_columns(link, hubs), axis=axis)


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
            *link_columns(link, hubs),
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
