# src/opl/vault/satellites.py
"""Load a DV2 descriptive satellite: one row per hash key per `applied_date`, written
only when the payload CHANGED. On a HUB or -- since F2 wave 2 -- on a LINK.

THE PARENT MAY BE EITHER, AND THAT IS ONE SIGNATURE CHANGE RATHER THAN A NEW KIND. This
loader took `hub: Hub` and `opl.vault.registry` refused a link-parented satellite in
those exact terms: "one parented on a LINK -- which DV2 does allow -- would be a
registered table nothing in this package can write. The guard and that signature have to
change together." Both moved in F2 wave 2 and `sat_link_payment` is the table that
consumed it. Nothing else about this loader differs between the two: the delta on
`hash_diff`, the dedup tie-break, the anti-join and the column order are the same code,
and the only branch is which expression keys the row (`_parent_key_expression`).

THE MECHANIC, AND THE PHASE'S PREMISE. `hash_diff` is the business-key hash standard
applied to the payload instead of to a business key. A candidate row is kept when its
`hash_diff` differs from the one that preceded it for the same hash key, in
`applied_date` order, and dropped when it does not -- so 69M companies with 105,820
changed razões sociais between two snapshots produce 105,820 second rows, not 69M.
Two rows for a company whose razão social moved and one for a company whose did not IS
the claim; `tests/vault/test_cnpj_vault.py` holds it over a fixture and the task report
carries the measurement against real bronze, because CI has no Databricks credential.

`applied_date` IS THE ORDERING AXIS, NOT `load_date`, and that is the departure this
phase exists to make. Classic DV2 orders a satellite by LDTS, which is correct only
when the load and the fact are the same event. Ours are not -- the RFB publishes a
snapshot dated 2026-06-13 and another dated 2026-07-11, and we load them whenever we
get to them, possibly both in one run. Ordering by `load_date` would make two
snapshots loaded in one job indistinguishable and their order arbitrary; ordering by
`applied_date` reconstructs the source's own history no matter when we ran.

WHAT THIS SATELLITE DOES NOT DO, and both are refusals rather than omissions:

  - **It writes no end-date and has no column to write one in.** A delta-driven
    satellite cannot tell "unchanged" from "not observed" -- both are the absence of a
    row -- so inferring a close from a missing row would end-date every key our own DQ
    gate happened to quarantine. That is ADR 0010's whole subject. The column list is
    pinned by test so the property belongs to the table rather than to the one row a
    test looked at.
  - **It does not act on a departure. It reports one.** The observation ledger is
    derived for the window and the count of `absent_after_observation` keys is returned
    in `SatelliteLoadResult`, where an operator sees it and no code branches on it. A
    caller who wants a departure signal maps that state onto one in their own code,
    where the choice is visible in review.

WHY CONSULTING THE LEDGER IS LOAD-BEARING HERE AND NOT DECORATIVE, stated precisely
because the tempting version of this wiring is not. Filtering candidates against the
ledger's observed states would be DEAD CODE: a candidate exists only because it has a
bronze row, and a bronze row makes the state `observed` or
`observed_with_rejected_siblings` by construction, so the filter could never remove
anything. A guard that cannot fire is worse than none, because the next reader
believes the hole is closed. What the ledger actually provides is two things that are
real: the departure count above, and `_window`'s refusal of a month with no row on
either side -- `months=['2026-09']` would otherwise select no bronze row, write
nothing, and report success.

AND SINCE F2 WAVE 2 THERE IS A SATELLITE WITH NO LEDGER AT ALL, WHICH IS THE DUAL OF THAT
PARAGRAPH RATHER THAN AN EXCEPTION TO IT. `sat_link_payment` hangs off a TRANSACTIONAL
link and declares `Satellite.transactional`; a payment is an event, so every key of every
earlier month is `absent_after_observation` in this one BY CONSTRUCTION and the departure
count would be a candidate delete per payment. A diagnostic whose only possible reading is
false is the same defect as a guard that cannot fire, seen from the other side --
`opl.vault.specs.Satellite` carries the measurement and the alternative that was rejected.
What that satellite does NOT lose is the second of the two real things: the window guard
is `satellite_grain.refuse_a_window_the_source_never_loaded`, the same rule over one table
instead of two, spelled once in `opl.vault.months`.

THE DEDUPLICATION RULE IS STATED, AND ON EMPRESAS IT NEVER FIRES. The source does not
guarantee one row per key per month -- at link grain on socios, 27,990,592 rows cover
27,986,263 distinct triples. Where two source rows share a hash key and an
`applied_date`, the one with the LOWEST `hash_diff` wins: deterministic, so two runs
over the same data agree, and free, because `min` over a struct is a partial aggregate
inside the grouping this loader already needs. Identical duplicates collapse silently.
On empresas the question was measured after the Task 3 review and the answer is ZERO
duplicate `(cnpj_basico, _snapshot_month)` rows across both months
(`01f19274-c1e0-1f3a-998a-ee0234483f5c`), so the tie-break is unexercised there today.

**THAT MEASUREMENT IS ABOUT EMPRESAS AND THIS LOADER NO LONGER ONLY LOADS EMPRESAS.**
Task 4 pointed it at `sat_estabelecimento_dados` and `sat_estabelecimento_endereco`
over 72.3M rows and the equivalent question -- duplicate
`(cnpj_basico, cnpj_ordem, cnpj_dv, _snapshot_month)` rows -- was never asked. **The
estabelecimentos duplicate rate is UNMEASURED**, not measured at zero, and the empresas
statement id above must not be read as covering it. The query that would settle it is
one `GROUP BY` (see the F2 wave-1 fix report); until it is run, the number to look at is
`SatelliteLoadResult.collapsed_duplicates` **on a load that was asked to measure it** --
see `report_diagnostics` below. Task 5 asked this question of its own tables and answered
it there; it did not come back for this one.

THE TWO REPORTED COUNTS ARE OPTIONAL AND DEFAULT TO OFF, AND THAT IS A COST DECISION
WITH A NUMBER BEHIND IT. The vault's first real run loaded `sat_empresa_dados` in
**5,635 s** against `hub_empresa`'s **281 s** over the same 69,062,849 keys
(`docs/f2-wave-1-workspace-run-evidence.md` §1.6). `load_hub` makes ONE pass over the
source; this loader made FOUR -- `satellite_candidates`, then `_collapsed_duplicates`
(a second full scan), then the ledger's all-keys x all-months grid for
`_candidate_departures`, then the append -- and the middle two WRITE NOTHING. Both
answered 0. Estabelecimentos is 72.3M keys with two satellites. So `load_satellite`
takes `report_diagnostics`, default False, and a load that was not asked reports both
counts as `None`.

`None` IS NOT `0`, AND KEEPING THEM APART IS THE POINT RATHER THAN A DETAIL. Those two
zeros are published as evidence that the dedup tie-break and the departure path are
unexercised by real data. A flag that turned a real 0 into a silent 0 would make that
evidence unfalsifiable, because nothing in the result or the log would separate a
measurement from a skip -- so the fields are `int | None`, `SatelliteLoadResult` refuses
a half-measured pair, and `databricks/src/vault_load_satellite.py` prints two different
sentences. WHAT IS **NOT** OPTIONAL, ON A LOAD THAT HAS A GRAIN AT ALL, is deriving the
ledger: that is what routes `months` through `observation._window` and its refusal of a
month with no row on either side, which is the second of the two things this module says
consulting the ledger really buys. The derivation runs on every such load; only the
`count()` over it is skipped. (That sentence had no qualifier until F2 wave 2, when a
TRANSACTIONAL satellite gained no ledger to derive -- and reaches the same refusal by
another route rather than losing it. `SatelliteLoadResult.ledger_derived` is what keeps
"nobody looked" apart from "there was nothing to look at".)

WHAT THE RULE COSTS WHERE IT DOES FIRE, since "deterministic" is not "correct". Bronze
is append-only and a corrected batch can be promoted for the same month, so two rows
for one key-month with DIFFERENT payloads are reachable. This loader picks one of them
silently -- there is no refusal -- and **a later re-load cannot correct the choice**,
because the anti-join drops a candidate on `(hash key, applied_date)` alone and never
looks at the payload. Repairing such a row means deleting it from the satellite by
hand. There IS now a count: `_collapsed_duplicates` reports the fold in every load's
result, which is what the three sibling loaders already did and what this one was
missing -- a silent fold whose choice cannot be revoked was the worst of the four to
leave uncounted. Task 5, whose link grain has 4,329 measured collisions, treated the
rule as a decision to make rather than one to inherit."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.vault.columns import APPLIED_DATE, HASH_DIFF, LOAD_DATE, RECORD_SOURCE
from opl.vault.hashing_spark import hash_key_column, refuse_non_string_columns
from opl.vault.links import refuse_mismatched_hubs
from opl.vault.links import source_columns as link_source_columns
from opl.vault.loading import (
    BRONZE_RECORD_SOURCE,
    SnapshotAxis,
    applied_date_expression,
    changed_rows,
    hash_key_expression,
    link_hash_key_expression,
    read_snapshot_window,
    rows_in,
)
from opl.vault.observation import (
    STATE_COLUMN,
    ObservationGrain,
    ObservationState,
    observation_ledger,
)
from opl.vault.registry import Hub, Link, Satellite, identifying_hubs
from opl.vault.satellite_grain import (
    refuse_a_window_the_source_never_loaded,
    snapshot_axis_for,
)
from opl.vault.specs import READS_DATE, READS_ISO_TEXT, AppliedDateSource

# Internal to `satellite_candidates`' tie-break, and named because it is selected
# through by field: a bare string at both ends is one typo from a column of NULLs.
_CHOSEN = "_chosen"


@dataclass(frozen=True)
class SatelliteLoadResult:
    """What one satellite load did, plus the two numbers it was asked to measure."""

    table: str
    appended: int
    # What the target already held before this load, whole-table and not window-scoped,
    # for `HubLoadResult.already_present`'s reason: the satellite may hold rows from
    # months outside the window, and reporting a narrower number than the one that was
    # measured would be a claim the count cannot support. This was the only result
    # object of the six without it, while `load_satellite` computed the number anyway.
    already_present: int
    # Source rows folded into another row sharing its (hash key, `applied_date`), by
    # `satellite_candidates`' lowest-`hash_diff` tie-break. Reported for the reason
    # `partners`, `reference` and `effectivity` report theirs, and here the reason is
    # sharper than for any of them: THIS fold discards a payload, silently, and a later
    # re-load cannot correct the choice (the anti-join drops the candidate on (hash key,
    # `applied_date`) alone and never looks at what was kept). See the module docstring.
    #
    # `None` WHEN THE LOAD WAS NOT ASKED TO MEASURE IT -- the type is what keeps that
    # apart from a measured 0, and the two readings are not close: a 0 says this loader
    # discarded no payload, a `None` says nobody looked.
    collapsed_duplicates: int | None
    # Keys the observation ledger calls `absent_after_observation` over this window:
    # present in an earlier month, absent here. A CANDIDATE delete and never an
    # asserted one -- it is equally the shape of a missed file, a dropped partition, or
    # an entity that returns next month. Reported so an operator can see it; acted on
    # nowhere in this module. `None` under the same rule as above.
    candidate_departures: int | None
    # Whether this load derived an observation ledger at all. FALSE ONLY FOR A
    # TRANSACTIONAL SATELLITE, which declares that there is no window to close and is
    # therefore loaded with no `ObservationGrain` (`opl.vault.specs.Satellite` argues
    # why). It is a FIELD and not an inference from `candidate_departures is None`,
    # because that is precisely the confusion the pair rule below exists to prevent:
    # "nobody looked" and "there was nothing to look at" are two states, and reading one
    # off the other would make the second unfalsifiable.
    #
    # DEFAULTED TRUE, so every load written before F2 wave 2 constructs this object
    # exactly as it did and gets exactly the refusal it did.
    ledger_derived: bool = True

    def __post_init__(self) -> None:
        """ONE FLAG GOVERNS BOTH, so a half-measured pair is a state no load can produce
        and no reader can interpret -- `collapsed_duplicates=0` beside
        `candidate_departures=None` claims the load both did and did not do the extra
        work. Refused in the type rather than trusted to its one caller, because the
        whole value of `None` here is that it means exactly one thing.

        THE PAIR RULE IS RESTATED RATHER THAN RELAXED BY F2 WAVE 2, and the difference
        matters. `ledger_derived=False` is a load with NO ledger, so a departure count was
        never available at any flag setting -- but `collapsed_duplicates` still was, and it
        is the number that matters most for a transactional satellite: a payment
        redelivered in a later month carries the SAME (link hash key, event day) as its
        first delivery, so the fold is live there where on empresas it is measured at
        zero. Reporting the fold and no departure is therefore a real state, and the arm
        below refuses the two that are NOT: a ledgerless load claiming a departure count,
        and -- on the ledger path, unchanged to the byte -- a half-measured pair."""
        if not self.ledger_derived:
            if self.candidate_departures is not None:
                raise ValueError(
                    f"a satellite load reported ledger_derived=False beside "
                    f"candidate_departures={self.candidate_departures!r}. A departure is "
                    "a state of the OBSERVATION LEDGER, and this load derived none -- so "
                    "the number cannot have been measured and naming one would put an "
                    "unsourced count in an operator's log"
                )
            return
        if (self.collapsed_duplicates is None) != (self.candidate_departures is None):
            raise ValueError(
                f"a satellite load reported collapsed_duplicates="
                f"{self.collapsed_duplicates!r} beside candidate_departures="
                f"{self.candidate_departures!r}. One report_diagnostics flag decides both, "
                "so they are measured together or not at all: None means NOT MEASURED and "
                "0 means measured and found none, and a pair carrying one of each cannot "
                "be read as either"
            )


# --- THE PARENT ARRIVES AS `hub=` OR AS `link=` PLUS `hubs=`, AND WHY IT IS TWO NAMES ---
#
# Module level for this file's standing reason (see the block above `load_satellite`):
# this is the reasoning, and inside a docstring it puts the function past the 50-line cap.
#
# THE OBVIOUS SHAPE IS ONE `parent: Hub | Link` ARGUMENT AND IT IS NOT TAKEN, because a
# link parent is not one value: `link_hash_key_expression` needs the link's HUBS to know
# each identifying end's widths and business-key order, so a `Link` alone cannot be keyed
# on. A single `parent` plus an optional `hubs` makes "a link with no hubs" expressible
# and then refused at run time, which is the same class of mistake one argument further
# away. As two names the pairing is structural: `link=` and `hubs=` travel together --
# exactly as they do on `load_effectivity_satellite`, which is the other loader over a
# link and reached this shape first -- and `hub=` alone is the whole of the hub case.
#
# AND `hub=` KEEPS ITS NAME, WHICH IS THE SECOND REASON AND THE SMALLER ONE. It is spelled
# by `databricks/src/vault_load_satellite.py` and by fifteen call sites in the suite;
# renaming it to `parent=` would have been a rename inside another agent's area this
# phase. Recorded rather than dressed up as design: if the two ever want to become one
# argument, `_resolved_parent` is the only thing that changes.


def _resolved_parent(
    satellite: Satellite, hub: Hub | None, link: Link | None, hubs: Sequence[Hub]
) -> Hub | Link:
    """The one table this satellite keys on, or refuse -- the four ways the pair is wrong.

    The parent arrives as free arguments for the reason this whole layer does: a loader
    that resolved it through the module-level registry could not be tested against a
    throwaway spec, and the registry is exactly the thing a new domain must be able to
    extend without this file changing. The cost is this check, and what it prevents is a
    satellite keyed on another table's digest -- which joins to nothing, silently, and
    reports success doing it."""
    if (hub is None) == (link is None):
        raise ValueError(
            f"satellite {satellite.name!r} was handed hub={hub!r} and link={link!r}. "
            "Exactly one is the parent: a satellite keys on ONE table's hash key, and "
            "neither passing both nor passing neither says which. Resolve it with "
            "opl.vault.domains.parent_of"
        )
    if hub is not None and hubs:
        raise ValueError(
            f"satellite {satellite.name!r} hangs off hub {hub.name!r} and was handed "
            f"hubs={[other.name for other in hubs]}. `hubs` is a LINK's ends' hubs and a "
            "hub has no ends, so this pair means the caller resolved a link somewhere and "
            "passed the wrong half of it"
        )
    parent = hub if hub is not None else link
    assert parent is not None  # noqa: S101 - narrowed by the two branches above
    if parent.name != satellite.parent:
        raise ValueError(
            f"satellite {satellite.name!r} declares parent {satellite.parent!r} and was "
            f"handed {parent.name!r}. Its hash key would be the wrong table's digest, so "
            "the satellite would join to nothing without failing -- resolve the parent "
            "with opl.vault.domains.parent_of rather than passing one by hand"
        )
    if isinstance(parent, Link):
        refuse_mismatched_hubs(parent, hubs)
    return parent


def _parent_key_expression(parent: Hub | Link, hubs: Sequence[Hub]) -> Column:
    """The digest this satellite's rows are keyed on, built from the parent's own spec.

    ONE SPELLING EACH, BORROWED AND NOT RE-DERIVED, which is `opl.vault.loading`'s whole
    subject: the hub branch is the expression `load_hub` wrote its keys with and the link
    branch is the one `load_link` wrote its hash key with, so a satellite joins to its
    parent by construction rather than by two derivations happening to agree. A second
    spelling here would not fail -- it would produce a satellite whose every join to its
    parent returns nothing, which is the quietest wrong answer in this layer."""
    if isinstance(parent, Hub):
        return hash_key_expression(parent)
    return link_hash_key_expression(parent, identifying_hubs(parent, hubs))


def _parent_source_columns(parent: Hub | Link, hubs: Sequence[Hub]) -> tuple[str, ...]:
    """The SOURCE columns the parent's key expression reads, for the string refusal.

    THE LINK BRANCH IS `links.source_columns` AND NOT THE HUBS' BUSINESS KEYS, which is
    the distinction that function exists to hold: an end may declare a `key_from`, so the
    columns read are the END's answer and not the hub's names, and the link's
    DEPENDENT-CHILD KEYS are read too because `link_hash_key_expression` hashes them. Left
    out, a `transaction_id` arriving as a bigint would be cast silently and hashed as the
    cast, giving a satellite keyed on digests no re-load over a string column could
    reproduce."""
    if isinstance(parent, Hub):
        return parent.business_key_columns
    return tuple(link_source_columns(parent, hubs))


# THE TWO REPRESENTATIONS OF A DAY THIS LOADER ACCEPTS, and which Spark type each one is.
# `READS_DATE` names a column that already IS a `date` -- `_snapshot_ref_date`, derived in
# bronze by `opl.bronze.snapshot.ref_date_column` -- and `READS_ISO_TEXT` names ISO-8601
# text, which bronze holds as a string because bronze is all-string for a contract column.
_APPLIED_DATE_TYPES = {READS_DATE: "date", READS_ISO_TEXT: "string"}


def _expected_source_type(declared: AppliedDateSource) -> str:
    """The Spark type `declared.reads` needs the source column to have, or refuse.

    TOTAL BY A NAMED REFUSAL AND NOT BY `KeyError`, which is the rule its neighbour
    already states and this dict did not follow. `opl.vault.loading.applied_date_
    expression` says of itself: "TOTAL OVER `APPLIED_DATE_READERS` BY REFUSAL AND NOT BY A
    FALLBACK. A reader with no branch raises naming itself." The mapping above answers the
    same question about the same closed set, and a third reader added to
    `APPLIED_DATE_READERS` and not to it raised a bare `KeyError` on a string a traceback
    would show as a dict subscript -- with nothing naming the set, the satellite, or the
    edit that has to accompany the other.

    AND IT IS THE ONE THAT ACTUALLY FIRES, WHICH IS WHY THE PROSE CANNOT BE LEFT TO THE
    NEIGHBOUR. `satellite_candidates` calls `_refuse_an_applied_date_the_source_cannot_
    provide` BEFORE it builds any expression, so through the loader this refusal is
    reached first and `applied_date_expression`'s own is unreachable -- that one is
    driven only by a test calling the builder directly. Leaving the `KeyError` here would
    have meant the pair's stated rule held only on the path nothing takes."""
    expected = _APPLIED_DATE_TYPES.get(declared.reads)
    if expected is None:
        raise ValueError(
            f"applied-date source on {declared.column!r} declares "
            f"reads={declared.reads!r}, and this loader knows no Spark type for it -- it "
            f"types {sorted(_APPLIED_DATE_TYPES)}. `AppliedDateSource` refuses a reader "
            "outside APPLIED_DATE_READERS at construction, so reaching here means a "
            "reader was added to that set and to loading.applied_date_expression and not "
            "to _APPLIED_DATE_TYPES -- the three have to move together"
        )
    return expected


def _refuse_an_applied_date_the_source_cannot_provide(
    source: DataFrame, satellite: Satellite, source_table: str
) -> None:
    """The declared applied-date column must be ON the source and be the TYPE its reader
    expects.

    THE FIRST HALF IS `observation._side`'S SHAPE, and for its reason: the declaration
    names a column by string, so a source that does not carry it is a `AnalysisException`
    several operators into a plan rather than prose naming the column and the table. It is
    not covered by `refuse_non_string_columns` above, because `_snapshot_ref_date` is a
    `date` and being refused as a non-string is exactly wrong for it.

    THE SECOND HALF IS THE ONE THAT FAILS SILENTLY, and it is `opl.gold.spec_fields`'
    `_assert_the_reader_matches_the_representation` asked one layer down. Neither
    mismatch raises in Spark: `substring` over a `date` casts it to text first and
    `F.col` over a string simply passes the string through. What the second produces is a
    satellite whose `applied_date` is a STRING while every other satellite's is a `date` --
    and `applied_date` is this loader's ORDERING AXIS, so the version chain would be
    ordered lexicographically over a rendering, and the written column would not match the
    one an existing table already holds. `mode("append")` matches by POSITION, so on a
    re-shaped table that lands as a type error or, worse, as a coerced column."""
    declared = satellite.applied_date_from
    if declared.column not in source.columns:
        raise ValueError(
            f"satellite {satellite.name!r} reads its applied_date from "
            f"{declared.column!r}, which {source_table!r} does not carry -- it has "
            f"{sorted(source.columns)}. A GENERATED or API-FED source has no "
            "`_snapshot_ref_date` at all (opl.bronze.autoloader.add_common_audit_columns "
            "omits it deliberately), which is why the column is declared per satellite"
        )
    expected = _expected_source_type(declared)
    actual = dict(source.dtypes)[declared.column]
    if actual == expected:
        return
    raise ValueError(
        f"satellite {satellite.name!r} reads {declared.column!r} as {declared.reads!r}, "
        f"which needs a {expected!r} column, and {source_table!r} types it {actual!r}. "
        "Neither direction raises in Spark: a substring over a date casts it to text "
        "first, and reading a string as a date leaves applied_date a STRING -- which is "
        "this loader's ordering axis and the column an existing satellite already holds "
        "as a date"
    )


def satellite_candidates(
    spark: SparkSession,
    satellite: Satellite,
    parent: Hub | Link,
    hubs: Sequence[Hub] = (),
    *,
    source_table: str,
    months: Sequence[str] | None,
    axis: SnapshotAxis,
) -> DataFrame:
    """One row per (hash key, applied_date) in the window, carrying the payload, its
    `hash_diff` and the source row's `record_source`.

    `applied_date` COMES FROM THE SATELLITE'S OWN DECLARATION, AND FOR EVERY SATELLITE
    WRITTEN BEFORE F2 WAVE 2 THAT DECLARATION IS `_snapshot_ref_date` AND NOT
    `_snapshot_month`. The two are separate bronze columns on purpose
    (`opl.bronze.snapshot`): the month is the operational identity of the run, the ref
    date is the date the RFB itself declares in its filename, and it is not month-end --
    2026-06 carries the 13th and 2026-07 the 11th. Deriving a date from the month would
    invent a day.

    IT IS A DECLARATION AND NO LONGER A CONSTANT BECAUSE THE CONSTANT WAS NOT UNIVERSAL.
    `bronze_payments` carries no `_snapshot_ref_date`: `add_common_audit_columns` omits it
    for a generated source, and stamping an all-NULL one would have forced the payments DQ
    set to drop `unprovable_snapshot_ref_date`. `opl.vault.specs.AppliedDateSource` is
    where that is argued and `opl.vault.loading.applied_date_expression` is where it
    becomes an expression."""
    source = read_snapshot_window(spark, source_table, months, axis=axis)
    payload = tuple(satellite.payload_columns)
    refuse_non_string_columns(
        source, (*_parent_source_columns(parent, hubs), *payload)
    )
    _refuse_an_applied_date_the_source_cannot_provide(source, satellite, source_table)
    keyed = source.select(
        _parent_key_expression(parent, hubs).alias(parent.hash_key),
        applied_date_expression(satellite.applied_date_from).alias(APPLIED_DATE),
        hash_key_column([F.col(column) for column in payload]).alias(HASH_DIFF),
        *(F.col(column) for column in payload),
        F.col(BRONZE_RECORD_SOURCE).alias(RECORD_SOURCE),
    )
    return (
        keyed.groupBy(parent.hash_key, APPLIED_DATE)
        .agg(F.min(F.struct(HASH_DIFF, *payload, RECORD_SOURCE)).alias(_CHOSEN))
        .select(parent.hash_key, APPLIED_DATE, f"{_CHOSEN}.*")
    )


def _collapsed_duplicates(
    spark: SparkSession,
    satellite: Satellite,
    parent: Hub | Link,
    hubs: Sequence[Hub],
    source_table: str,
    months: Sequence[str] | None,
    axis: SnapshotAxis,
) -> int:
    """Source rows in the window, minus distinct (hash key, `applied_date`) pairs.

    A SECOND PASS, DELIBERATELY, in the shape `opl.vault.partners._collapsed_duplicates`
    and `opl.vault.reference._collapsed_duplicates` use -- and this loader is the one
    that most needed it. The other three folds either discard nothing (`partners`,
    whose link rows carry no payload) or discard one delivered value under a rule the
    module argues for (`effectivity`'s earliest entry date). This one picks a PAYLOAD
    silently, and the module docstring records that a re-load cannot correct the pick.
    A fold with that consequence and no count was the one number an operator had no way
    to get. `satellite` is taken for the same reason `hub` is: the pair is what the
    caller already validated, and reading the source through anything else would be a
    second spelling of the grain this counts against.

    THE HASH KEY IS RECOMPUTED RATHER THAN THE RAW COLUMNS COUNTED, which costs a second
    digest over the window and is not interchangeable with the cheap version:
    `zero_padded_column` maps `'1'` and `'01'` onto one padded key, so distinct raw
    values can share a hash key. Counting the raw columns would report fewer duplicates
    than the fold actually performs, which is the wrong direction for a number whose
    whole job is to make the fold visible.

    THE FOLD IS LIVE ON A TRANSACTIONAL SATELLITE WHERE IT IS MEASURED AT ZERO ON
    EMPRESAS, which is why this count survives the ledger being optional. A payment
    redelivered in a later month is "the SAME payment seen twice"
    (`opl.contracts.payments`), and it carries the same `transaction_id` and the same
    `event_time` -- so its (link hash key, applied_date) is identical to the first
    delivery's and the two ARE folded here."""
    source = read_snapshot_window(spark, source_table, months, axis=axis)
    keyed = source.select(
        _parent_key_expression(parent, hubs).alias(parent.hash_key),
        applied_date_expression(satellite.applied_date_from).alias(APPLIED_DATE),
    )
    return keyed.count() - keyed.distinct().count()


def _candidate_departures(ledger: DataFrame) -> int:
    """How many (key, month) pairs `ledger` calls `absent_after_observation`.

    IT TAKES THE LEDGER RATHER THAN DERIVING ONE, AND THAT SPLIT IS WHAT MADE THE COUNT
    SAFE TO SKIP. This function used to call `observation_ledger` itself, which bundled
    two unrelated things into one optional step: a number for the operator's log, and
    the ONLY route by which `months` reaches `observation._window` and its refusal of a
    month with no row on either side. Skipping the pair would have dropped a guard the
    module docstring calls one of the two real things the ledger buys. So the derivation
    moved out to `load_satellite`, which does it unconditionally, and what is left here
    is the part that is genuinely only a report.

    THE DERIVATION IS LAZY PAST THAT REFUSAL, which is why moving it out costs nothing on
    a load that reports no count: `observation_ledger` runs one eager job -- the distinct
    months `_window` collects -- and returns a plan. The `crossJoin` grid over the whole
    key space and the fold over it are built only when something asks for rows, and this
    `count()` is the only thing in this module that does.

    Eager, and BEFORE anything is written, so the number belongs in the operator's log
    next to what was written rather than to a later run."""
    return ledger.filter(
        F.col(STATE_COLUMN) == F.lit(ObservationState.ABSENT_AFTER_OBSERVATION.value)
    ).count()


def _diagnostics(
    spark: SparkSession,
    satellite: Satellite,
    parent: Hub | Link,
    hubs: Sequence[Hub],
    source_table: str,
    months: Sequence[str] | None,
    ledger: DataFrame | None,
    axis: SnapshotAxis,
    *,
    report: bool,
) -> tuple[int | None, int | None]:
    """The two reported counts, or `(None, None)` when this load was not asked for them.

    `None` AND NEVER `0`, which is the whole reason this returns an optional pair rather
    than defaulting to zeros. The first real run of this loader reported 0 collapsed
    duplicates and 0 candidate departures, and both zeros are PUBLISHED as evidence that
    two paths are unexercised by real data; a skip that reported 0 would make that
    evidence unfalsifiable, because no reader could separate a measurement from an
    omission. See the module docstring for what the skip is worth in seconds.

    A `None` LEDGER IS A TRANSACTIONAL LOAD AND ITS DEPARTURE COUNT STAYS `None` EVEN WHEN
    THE FLAG IS ON -- there is no ledger to count over, and `SatelliteLoadResult` carries
    `ledger_derived=False` so the two `None`s cannot be read as one another. The FOLD is
    still measured, and on that satellite it is the number that matters."""
    if not report:
        return None, None
    collapsed = _collapsed_duplicates(
        spark, satellite, parent, hubs, source_table, months, axis
    )
    if ledger is None:
        return collapsed, None
    return collapsed, _candidate_departures(ledger)


def _in_column_order(
    rows: DataFrame, satellite: Satellite, parent: Hub | Link, load_date: datetime
) -> DataFrame:
    """The rows about to be written, in the satellite's declared column order.

    An explicit projection rather than whatever order the joins left behind, because
    the column ORDER is what a Delta append matches on when the table already exists --
    `mode("append")` is positional unless `mergeSchema` says otherwise, so two loads
    building the same columns in two orders would write the payload into each other's
    columns without failing. Metadata first, then payload, and
    `test_the_satellite_has_no_end_date_column_at_all` pins the whole list.

    SIX KINDS OF COLUMN AND NOT ONE MORE, WHICH IS WHAT MAKES THE REGISTRY'S COLLISION
    GUARD AS NARROW AS IT IS. A satellite on a LINK writes the link's hash key here and
    NONE of the link's other columns -- not its roled reference columns, not its
    dependent-child keys -- so those names cannot be taken from a payload column by this
    write. `opl.vault.registry_satellites` argues that from the other side."""
    return rows.select(
        parent.hash_key,
        F.lit(load_date).alias(LOAD_DATE),
        F.col(APPLIED_DATE),
        F.col(RECORD_SOURCE),
        F.col(HASH_DIFF),
        *(F.col(column) for column in satellite.payload_columns),
    )


def _append_changed(
    spark: SparkSession,
    candidates: DataFrame,
    satellite: Satellite,
    parent: Hub | Link,
    target_table: str,
    load_date: datetime,
    before: int,
) -> None:
    """Append the candidates whose payload changed, in the satellite's column order.

    Split out of `load_satellite` when the diagnostics became optional, so that function
    stays inside this project's 50-line cap. It is a single Delta append of one frame, so
    the split adds no state between `load_satellite`'s refusals and the committed rows --
    `before` is passed in rather than re-read for the same reason it is read at all: the
    result object's `appended` is an after-minus-before over one measurement point."""
    existing = None
    if before:
        existing = spark.read.table(target_table).select(
            parent.hash_key, APPLIED_DATE, HASH_DIFF
        )
    # The anti-join that used to sit here is inside `changed_rows` now -- it was the one
    # step of that function's contract each caller had to remember, and it was the step
    # the docstring called load-bearing. See `loading._without_persisted`.
    changed = changed_rows(candidates, existing, parent.hash_key)
    (
        _in_column_order(changed, satellite, parent, load_date)
        .write.format("delta").mode("append").saveAsTable(target_table)
    )


# WHY `load_satellite`'S ARGUMENT PROSE IS HERE. F-DB Task 2 added the axis paragraph and
# pushed this function to 56 lines against the `< 50 INCLUDING comments` cap (master
# protocol §4.9). No test enforces that cap, which is how it was reported compliant on a
# docstring-excluded measure; `opl.bronze.rules` moved prose to module level, above
# `rules_for`, for the
# same reason. Nothing here is dropped.
#
# `load_date` HAS NO DEFAULT, for `load_hub`'s reason: a loader that stamps its own clock
# cannot be asserted against, and in the data it would make the LDTS a record of when the
# pipeline happened to run rather than of the load it belongs to.
#
# `report_diagnostics` DEFAULTS OFF, AND OFF REPORTS `None` RATHER THAN `0`. On, this load
# pays a second full scan of the source and materialises the ledger's key-space grid to
# fill `collapsed_duplicates` and `candidate_departures`; off it pays neither, and "not
# measured" is a thing no reader can confuse with "measured, found none". The first real
# run spent most of 5,635 s on the two and both answered 0; see `_diagnostics`.
#
# THE AXIS COMES OFF THE GRAIN WHERE THERE IS ONE, AND IS A PARAMETER WHERE THERE IS NOT.
# For a satellite gated on an observation ledger the grain is required,
# `_refuse_a_mismatched_grain` has already pinned it to this source table, and its axis is
# the source's own declaration -- so an `axis=` argument beside it would be a second
# spelling of one decision, whose disagreement would land as a window that silently
# selected nothing, and it is refused. A TRANSACTIONAL satellite has no grain, so nothing
# else spells the axis and the parameter is the only spelling rather than a second one.
# `snapshot_axis_for` refuses every other combination of the three, and the parent's kind
# beside them: `opl.vault.satellite_grain` carries that argument at module level.
#
# THE PARENT IS `hub=` OR `link=` PLUS `hubs=`, argued in the comment block above
# `_resolved_parent`. Both are defaulted `None` so that exactly one may be given; neither
# is optional in the sense of omissible.
#
# IDEMPOTENT: a re-run finds every (key, applied_date) it would write already persisted,
# drops them before the window, and appends nothing. The write is a single Delta append, so
# there is no partial state between the refusals and the committed rows.
def load_satellite(
    spark: SparkSession,
    satellite: Satellite,
    *,
    hub: Hub | None = None,
    link: Link | None = None,
    hubs: Sequence[Hub] = (),
    source_table: str,
    target_table: str,
    load_date: datetime,
    grain: ObservationGrain | None = None,
    axis: SnapshotAxis | None = None,
    months: Sequence[str] | None = None,
    report_diagnostics: bool = False,
) -> SatelliteLoadResult:
    """Append a row for every (hash key, `applied_date`) whose payload changed.

    Idempotent, and its arguments-without-defaults are argued in the comment block
    above this function."""
    parent = _resolved_parent(satellite, hub, link, hubs)
    window_axis = snapshot_axis_for(satellite, parent, grain, axis, source_table)
    candidates = satellite_candidates(
        spark, satellite, parent, hubs,
        source_table=source_table, months=months, axis=window_axis,
    )
    # DERIVED ON EVERY LOAD THAT HAS A GRAIN, INCLUDING ONE THAT REPORTS NOTHING FROM IT:
    # it is the only route by which `months` reaches `observation._window`'s refusal of a
    # month with no row on either side, and it is lazy past that refusal. A transactional
    # load has no ledger and reaches the same refusal over ONE table instead of two.
    ledger = None
    if grain is not None:
        ledger = observation_ledger(spark, grain, months=months)
    else:
        refuse_a_window_the_source_never_loaded(spark, source_table, months, window_axis)
    collapsed, departures = _diagnostics(
        spark, satellite, parent, hubs, source_table, months, ledger, window_axis,
        report=report_diagnostics,
    )
    before = rows_in(spark, target_table)
    _append_changed(spark, candidates, satellite, parent, target_table, load_date, before)
    return SatelliteLoadResult(
        table=target_table,
        appended=rows_in(spark, target_table) - before,
        already_present=before,
        collapsed_duplicates=collapsed,
        candidate_departures=departures,
        ledger_derived=ledger is not None,
    )
