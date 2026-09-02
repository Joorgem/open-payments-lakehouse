# src/opl/vault/loading.py
"""What a hub load, a link load and a satellite load have in common: the hash-key
expressions, the earliest-observation aggregate, the month window, and the two bronze
columns every vault table reads.

ONE SPELLING OF THE HASH KEY, SHARED BY ALL THREE LOADERS, and this is the reason the
file exists rather than each loader building its own. A satellite's hash key IS its
hub's, and a link's hub REFERENCE is that same hub's -- if those are computed by
separate expressions, a divergence does not fail, it produces a satellite or a link
that joins to its hub and returns nothing. An empty join is the quietest wrong answer
in this layer: no error, no row count anomaly on either side, and every downstream
query simply reports that the company has no descriptive history and no
establishments. `hash_key_expression` is called by `load_hub`, `load_satellite`,
`load_link` and `load_partner_link`; `load_effectivity_satellite` keys through
`link_hash_key_expression`, which is in this file beside it. `load_reference_table` is
the one loader that calls neither, and deliberately: a reference table has no hash key
at all (`opl.vault.specs.ReferenceTable` states that as its own decision). So every
HASH KEY this vault writes is built here, and there is no second way to spell one.
(This sentence named exactly three loaders until Task 7's correction pass -- true when
it was written at Task 3, left behind by Tasks 5 and 6. The property it asserts is
unchanged: one spelling, in this module.)

IT SAYS "HASH KEY" AND NOT "DIGEST", WHICH IS A NARROWER CLAIM AND THE TRUE ONE. It
read "every digest this vault writes is built here" until the final whole-branch
review, and `hash_diff` is a digest this vault writes that is NOT built here:
`opl.vault.satellites.satellite_candidates` calls `hash_key_column` over the payload
directly. That is not a second spelling and nothing about it is wrong -- both reach
`opl.vault.hashing_spark.hash_key_column`, which is the one implementation of the
standard, and a payload is not a business key so there is no `Hub` to take its
components from. The property this module actually holds is about the BUSINESS-KEY
side: every expression that keys a row to a hub or a link is built from a spec, here.
The claim about the standard as a whole belongs to `hashing_spark`, and is that
module's to make.

THE MONTH WINDOW IS VALIDATED FOR SHAPE HERE AND FOR EXISTENCE ELSEWHERE. `is_month`
is the one spelling of "YYYY-MM naming a real month" in this repository -- the rule
`opl.config` records having been bitten by carrying twice -- so this module asks it
rather than re-deriving it. Whether a well-formed month was ever LOADED is a different
question and nothing here can answer it; the satellite gets that from the observation
ledger, whose `_window` refuses a month with no row on either side (see
`opl.vault.observation` for why that refusal is worth an eager Spark job)."""
from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from opl.bronze.snapshot import SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN
from opl.bronze.snapshot_axis import MONTHLY_SNAPSHOT, SnapshotAxis
from opl.vault.columns import APPLIED_DATE, HASH_DIFF, RECORD_SOURCE
from opl.vault.hashing_spark import hash_key_column, zero_padded_column
from opl.vault.months import validated_months
from opl.vault.registry import BusinessKeyColumn, Hub, Link, LinkEnd
from opl.vault.specs import READS_DATE, READS_ISO_TEXT, AppliedDateSource

# Bronze's own RSRC column, carried into the vault verbatim rather than re-derived.
#
# A SECOND SPELLING OF A LITERAL, AND CROSS-CHECKED RATHER THAN TRUSTED, in the shape
# `opl.bronze.registry._assert_prefixes_match_their_file_groups` uses: the value is
# written by `opl.bronze.autoloader.add_audit_columns` as a bare string, so there is no
# constant to import, and a rename there would leave this module selecting a column
# that is not there. `tests/vault/test_cnpj_vault.py::test_the_bronze_audit_columns_
# this_layer_reads_are_the_ones_the_ingest_writes` runs that function and asserts the
# three names below are among its output, which turns the duplicate into a cross-check.
BRONZE_RECORD_SOURCE = "_record_source"

# Re-exported so a loader imports its bronze column names from ONE place, and so the
# two that come from `opl.bronze.snapshot` are visibly the same values that module
# defines rather than restated strings.
__all__ = [
    "BRONZE_RECORD_SOURCE",
    "MONTHLY_SNAPSHOT",
    "SNAPSHOT_MONTH_COLUMN",
    "SNAPSHOT_REF_DATE_COLUMN",
    "SnapshotAxis",
    "applied_date_expression",
    "changed_rows",
    "earliest_record_source",
    "end_components",
    "hash_key_expression",
    "hash_key_for_end",
    "hash_key_over",
    "link_hash_key_expression",
    "read_snapshot_window",
    "rows_in",
]

# Internal to `earliest_record_source`, and named because it is selected through by
# field: a bare string at both ends is one typo from a column full of NULLs.
_FIRST_SEEN = "_first_seen"

# Internal to `changed_rows`. Named rather than inlined because both are selected
# through by string and a typo in one of the two spellings would silently make every
# row look new (`_PERSISTED` never true) or every row look old.
_PERSISTED = "_persisted"
_PREVIOUS = "_previous_value"


def rows_in(spark: SparkSession, table: str) -> int:
    """How many rows `table` holds, or 0 if it does not exist yet.

    THE BEFORE/AFTER PAIR BOTH LOADERS REPORT THEIR NUMBERS FROM, and it is a whole-
    table `count()` with no filter on purpose: Delta answers that from the transaction
    log's file statistics rather than by scanning, so the pair costs no pass over the
    data. Counting the frame about to be written instead would mean either recomputing
    an anti-join and a window, or caching a frame that on a first load is the entire
    69M-row table.

    What it buys beyond cost: `appended` is then what LANDED, not what was planned, so
    there is no state in which a result object reports a write that did not happen."""
    if not spark.catalog.tableExists(table):
        return 0
    return spark.read.table(table).count()


# THE CALENDAR DAY OF AN ISO-8601 INSTANT IS ITS FIRST TEN CHARACTERS, AND NEVER A CAST.
# `opl.gold.conformed.day_of` argues this at length and its argument is this one: a
# `CAST(... AS TIMESTAMP)` resolves the instant in the SESSION timezone, so under
# America/Sao_Paulo -- which local Spark inherits from the operating system unless
# `opl.config.SESSION_TIMEZONE` is pinned -- a midnight-UTC payment lands on the previous
# day. For a satellite that is worse than for a fact: `applied_date` is the ORDERING AXIS
# of the version chain (`opl.vault.satellites`' own second paragraph), so a cluster
# setting would decide which of two payloads a satellite calls the later one.
#
# TEN, AND NOT `opl.bronze.snapshot._INSTANT_DATE_WIDTH`, WHICH IS THE SAME NUMBER ABOUT A
# DIFFERENT STRING. That constant belongs to `ref_date_from_instant`, whose input is the
# 27-character microsecond rendering `opl.bronze.snapshot_axis.INSTANT_PATTERN` pins; the
# payment stream's `event_time` is 24 characters with THREE fractional digits
# (`opl.generator.instants.to_text`), so that function refuses it -- measured, it returns
# NULL for every payment row, on both the width check and the pattern. Reusing it would
# have produced an all-NULL `applied_date`, which `changed_rows` orders on. Ten characters
# is a property of ISO-8601 itself and is true of both renderings, which is why the
# derivation gold already uses is the one taken here.
_ISO_DAY_WIDTH = 10


def applied_date_expression(source: AppliedDateSource) -> Column:
    """A satellite's `applied_date`, as a Column over the source column it declares.

    THE ONE PLACE A DECLARATION BECOMES AN EXPRESSION, which is the seam
    `opl.vault.specs` names when it says the reader is data rather than a callable: that
    module imports no pyspark, so the branch has to be here, and being here it is one
    branch rather than one per loader.

    TOTAL OVER `APPLIED_DATE_READERS` BY REFUSAL AND NOT BY A FALLBACK. A reader with no
    branch raises naming itself; an `else` returning `F.col(...)` would read a
    27-character string as a date column and hand `changed_rows` a NULL ordering key for
    every row, which orders the whole version chain arbitrarily without failing."""
    if source.reads == READS_DATE:
        return F.col(source.column)
    if source.reads == READS_ISO_TEXT:
        return F.to_date(F.substring(F.col(source.column), 1, _ISO_DAY_WIDTH))
    raise ValueError(
        f"applied-date source on {source.column!r} declares reads={source.reads!r}, and "
        "this module has no expression for it. `AppliedDateSource` refuses a reader "
        "outside its closed set at construction, so reaching here means a reader was "
        "added to that set and not to this branch -- the pair has to move together"
    )


def _padded(keys: Sequence[BusinessKeyColumn], sources: Sequence[Column]) -> list[Column]:
    """`sources`, positionally matched to `keys` and padded to their declared widths.

    Zero-padding is applied per column and only where the spec declares a width, which
    is `BusinessKeyColumn`'s decision to carry: padding a name or a free-text
    identifier would invent characters, where padding `cnpj_basico` to eight recovers
    a leading zero a source may have dropped. `zero_padded_column` fails the query on
    an overlong value rather than truncating it onto another entity's true key.

    ORDER IS THE SPEC'S DECLARATION ORDER, and it is not incidental: the standard
    joins components with `||` and length-prefixes each, so two orders give two
    different digests for the same business key. The spec is where that order is
    written down once."""
    if len(keys) != len(sources):
        raise ValueError(
            f"{len(sources)} source columns were supplied for a key of {len(keys)} "
            "components. They are matched POSITIONALLY, so a shorter or longer list "
            "would pad and hash the wrong column"
        )
    return [
        source if key.width is None else zero_padded_column(source, width=key.width)
        for key, source in zip(keys, sources, strict=True)
    ]


def _padded_components(hub: Hub) -> list[Column]:
    """The hub's business-key columns, read from the columns named after them."""
    return _padded(hub.business_keys, [F.col(key.name) for key in hub.business_keys])


def hash_key_expression(hub: Hub) -> Column:
    """The hub's hash key, as a Column over the source's business-key columns."""
    return hash_key_column(_padded_components(hub))


def hash_key_over(hub: Hub, sources: Sequence[Column]) -> Column:
    """The hub's hash key, taken over columns that are NOT named after its business
    key -- the same standard, the same widths, the same order, read somewhere else.

    THE SELF-REFERENCE IS WHAT THIS EXISTS FOR. `link_company_partner` references
    `hub_empresa` twice: once for the company, whose `cnpj_basico` socios carries under
    that name, and once for the PARTNER company, whose `cnpj_basico` socios carries
    only as the first eight characters of `cpf_cnpj_socio`. Both references must be the
    digest `load_hub` wrote or the link joins to nothing, so the second one cannot be a
    second spelling of the standard -- it is this one, handed a different column."""
    return hash_key_column(_padded(hub.business_keys, list(sources)))


def _end_sources(end: LinkEnd, hub: Hub) -> list[Column]:
    """The RAW columns one link end's hub business key is read from, before padding.

    ONE LIST, TWO CONSUMERS, WHICH IS WHAT KEEPS THEM FROM DISAGREEING. `end_components`
    pads it for the LINK's own digest and `hash_key_for_end` hands the same list to
    `hash_key_over` for the end's HUB REFERENCE. If those were two derivations, a link
    could carry a reference column that joins correctly and an identity column computed
    over something else -- which is the failure `opl.vault.links.refuse_mismatched_hubs`
    describes for a reordered pair: every row present, every join working, and the
    table's identity disagreeing with the one a re-load computes.

    `key_from is None` IS THE PRE-F-DB BRANCH AND IS BYTE-IDENTICAL TO IT. It returns
    exactly `[F.col(key.name) for key in hub.business_keys]`, which is what
    `_padded_components` reads and what `hash_key_expression` hashes, so every end of
    both CNPJ links takes the same path through different words."""
    if end.key_from is None:
        return [F.col(key.name) for key in hub.business_keys]
    return [
        F.substring(F.col(prefix.column), 1, prefix.width) for prefix in end.key_from
    ]


def end_components(end: LinkEnd, hub: Hub) -> list[Column]:
    """One link end's hub business key, padded to the hub's declared widths, read from
    wherever the END says it lives -- the components a link's own hash key concatenates.

    THE DECLARATION IS THE ONLY THING THAT DIFFERS. The widths, the order and the padding
    are the hub's, through `_padded`, so a derived end takes the digest of a value padded
    exactly as `load_hub` padded the value it wrote. `build_registry` has already refused
    a prefix whose width is not the hub's, so the pad is a no-op on a well-formed
    declaration and a refusal rather than a truncation on a malformed value."""
    return _padded(hub.business_keys, _end_sources(end, hub))


def hash_key_for_end(end: LinkEnd, hub: Hub) -> Column:
    """The hub reference one link end writes: the hub's own hash key, taken over
    whatever columns that end declares.

    THROUGH `hash_key_over`, WHICH IS THE SEAM THAT ALREADY EXISTED -- "the same
    standard, the same widths, the same order, read somewhere else". `opl.vault.partners`
    was its only caller and reached it by hard-coding socios' derivation in a second
    loader; this reaches it from a DECLARATION, which is the whole of T5b. An end with no
    declaration hands it the hub's own columns, so the result is `hash_key_expression`'s
    to the byte."""
    return hash_key_over(hub, _end_sources(end, hub))


def link_hash_key_expression(link: Link, hubs: Sequence[Hub]) -> Column:
    """A link's own hash key: the standard over every IDENTIFYING end's hub business
    key, concatenated in the link's declared order, then its dependent-child keys.

    `hubs` IS THE IDENTIFYING ENDS' HUBS AND NOT EVERY END'S. A non-identifying end is
    a reference the link RESOLVES rather than one it is identified by -- the partner
    company's `cnpj_basico`, which is a function of `cpf_cnpj_socio` and is already in
    the digest through it. Hashing it as well would make the link's identity depend on
    a value we derived instead of only on values the source delivered.

    THE SAME STANDARD, A LONGER COMPONENT LIST -- not a hash of the hub hash keys, and
    the difference is worth stating because hashing the hashes is the commoner
    implementation. Three reasons for the business keys:

      - a hash of digests is a SECOND standard (an encoding over 64-character hex,
        whose null and empty branches never fire), and this repository already pays for
        one second spelling of the hash under an equivalence test;
      - the link's key stays derivable from the source row alone, so it can be
        reconciled by hand and computed without joining the hubs;
      - the components carry their own length prefix and `||` delimiter either way, so
        the concatenation is unambiguous without borrowing the hubs' digests for it.

    ONE COLUMN MAY APPEAR TWICE and that is correct, not a duplicate to collapse:
    `link_empresa_estabelecimento` joins a hub keyed on `cnpj_basico` to one keyed on
    (`cnpj_basico`, `cnpj_ordem`, `cnpj_dv`), and the link's key is both hubs' keys --
    dropping the repeat would make the link's digest the establishment's own with a
    prefix, which is a different claim about identity.

    THE UNAMBIGUITY IS WITHIN A FIXED HUB LIST, NOT ACROSS LINKS, and the difference
    matters before wave 2 adds a second link. The components are flattened with NO HUB
    BOUNDARY in the encoding, so a link over hubs keyed [x] + [y, z] and one over
    [x, y] + [z] produce the SAME digest for the same values. Unreachable today, and the
    reason has changed since this paragraph was written: there are now TWO links, not
    one (`link_empresa_estabelecimento` flattens to four components,
    `link_company_partner` to three), and the encoding is injective over component
    lists, so two lists of different lengths cannot collide. A digest is also only ever
    compared against others from the same `Link` spec. It becomes reachable the moment
    two links share a key space of the same width, and
    the repair then is to prefix the link's own name into the component list rather than
    to change what a component means. Stated here so that decision is made deliberately
    rather than discovered.

    DEPENDENT-CHILD KEYS COME LAST, after every hub, and that order is load-bearing for
    the same reason the hub order is -- the components are flattened with no boundary
    marker, so moving one re-keys the whole table.

    `hubs` IS ZIPPED STRICTLY AGAINST `link.identifying_ends` SINCE F-DB, which turns the
    paragraph above from an instruction into a refusal: every caller already passed
    `identifying_hubs(link, hubs)`, and a caller that passed something else got a digest
    over the wrong hubs rather than an error. The pairing is what a DERIVED end needs --
    `end_components` reads the columns the END declares, so `link_merchant_empresa` hashes
    the first eight characters of `cnpj` where `link_empresa_estabelecimento` hashes
    `cnpj_basico` by name. Every end written before F-DB declares nothing, so its
    components are `_padded_components(hub)` exactly as before."""
    components = [
        component
        for end, hub in zip(link.identifying_ends, hubs, strict=True)
        for component in end_components(end, hub)
    ]
    components += _padded(
        link.dependent_child_keys,
        [F.col(key.name) for key in link.dependent_child_keys],
    )
    return hash_key_column(components)


def earliest_record_source(
    keyed: DataFrame, group_by: Sequence[str], *, axis: SnapshotAxis = MONTHLY_SNAPSHOT
) -> DataFrame:
    """One row per group, carrying the `record_source` of the EARLIEST month the group
    appeared in.

    SHARED BY `load_hub` AND `load_link`, which is what the two insert-only loaders
    have in common: a row is inserted once and never updated, so its `record_source`
    describes the observation that CREATED it. EARLIEST, NOT ARBITRARY -- `first()` or
    `any_value()` would make the stored value depend on partition order, and two runs
    over the same data could disagree. `min` over a struct of (month, source) is a
    partial aggregate, so it costs no shuffle beyond the grouping that is already
    needed to collapse a key's many monthly rows into one.

    `keyed` must carry `axis.column` and `_record_source`; the result carries
    `group_by` and `record_source` and nothing else.

    "EARLIEST" IS A `min` OVER A STRING, so it is the axis's FORMAT that makes this
    chronological -- see `opl.bronze.snapshot_axis`, which is where both declared
    formats are pinned to sort that way and where the argument lives. Defaulted for
    `read_snapshot_window`'s reason: both callers here are over monthly sources and
    neither had to change."""
    return (
        keyed.groupBy(*group_by)
        .agg(F.min(F.struct(axis.column, BRONZE_RECORD_SOURCE)).alias(_FIRST_SEEN))
        .select(
            *group_by,
            F.col(f"{_FIRST_SEEN}.{BRONZE_RECORD_SOURCE}").alias(RECORD_SOURCE),
        )
    )


def _without_persisted(candidates: DataFrame, existing: DataFrame, key: str) -> DataFrame:
    """`candidates`, minus every (key, `applied_date`) pair `existing` already holds.

    RUN BEFORE THE UNION IN `changed_rows`, AND THAT ORDERING IS LOAD-BEARING RATHER
    THAN TIDY. Leaving a persisted pair in puts two rows at the same position in the
    window: whichever of the identical pair `lag` happens to order second is marked
    unchanged while the first is marked changed, so if the non-persisted one lands
    first it survives the filter and is appended again -- a duplicate on roughly half
    of re-runs, non-deterministically, with nothing failing.

    IT LIVES HERE AND NOT IN THE CALLERS, WHICH IS A CORRECTION. Both loaders performed
    this anti-join themselves while `changed_rows`' own docstring described it as part
    of what the two share -- so the single step that docstring called load-bearing was
    the one piece a THIRD caller could omit, and `opl.vault.registry`'s extensibility
    claim is precisely that there will be a third caller. A precondition cheaper to
    satisfy than to state belongs inside the function that needs it; `changed_rows` has
    both frames and both join keys, so there was never anything to hand out."""
    return candidates.join(
        existing.select(key, APPLIED_DATE), on=[key, APPLIED_DATE], how="left_anti"
    )


def changed_rows(
    candidates: DataFrame,
    existing: DataFrame | None,
    key: str,
    *,
    change_column: str = HASH_DIFF,
) -> DataFrame:
    """The candidates whose `change_column` differs from the one that preceded it for
    the same key, in `applied_date` order -- with anything already persisted at that
    (key, `applied_date`) dropped first, by `_without_persisted`.

    SHARED BY THE DESCRIPTIVE SATELLITE AND THE EFFECTIVITY SATELLITE, which differ only
    in what they watch: `hash_diff` over a payload, or `is_active` over a relationship.
    Everything below is subtle enough that a second copy would be a second thing to keep
    correct, and the divergence would show up as duplicate rows rather than as an error.

    THE PERSISTED ROWS SEED THE WINDOW, which is what makes an incremental load
    correct. June is already in the satellite when July's snapshot arrives, so July's
    comparison has to be against the row on disk rather than against another row in the
    same batch; a `lag` over the candidates alone would call July's value new. So
    `existing` is used twice and for opposite purposes -- it seeds the comparison and it
    removes the rows that would corrupt it -- which is why both uses are in one place.

    ORDERED BY `applied_date`, so a snapshot loaded out of order still lands in its
    true position. What that cannot repair is a row already written: backfilling June
    after July leaves July's row in place even though it is now redundant. Redundant,
    not wrong -- it still says the value was V on 2026-07-11 -- so the correction is to
    load in order, not to rewrite."""
    if existing is not None:
        candidates = _without_persisted(candidates, existing, key)
    lineage = candidates.select(key, APPLIED_DATE, change_column).withColumn(
        _PERSISTED, F.lit(False)
    )
    if existing is not None:
        lineage = lineage.unionByName(
            existing.select(key, APPLIED_DATE, change_column).withColumn(
                _PERSISTED, F.lit(True)
            )
        )
    ordered = Window.partitionBy(key).orderBy(APPLIED_DATE)
    changed = (
        lineage.withColumn(_PREVIOUS, F.lag(change_column).over(ordered))
        .filter(F.col(_PREVIOUS).isNull() | (F.col(_PREVIOUS) != F.col(change_column)))
        .filter(~F.col(_PERSISTED))
        .select(key, APPLIED_DATE)
    )
    return candidates.join(changed, on=[key, APPLIED_DATE], how="left_semi")


def _validated_months(
    months: Sequence[str] | None, axis: SnapshotAxis
) -> tuple[str, ...] | None:
    """The window, or `None` for "every month the source holds".

    Shares `opl.vault.months.validated_months` with the observation ledger rather than
    restating its three refusals; only the CONSEQUENCE differs, and it differs for a
    reason worth keeping. Every refusal there produces a load that writes NOTHING and
    reports success, which is the failure shape this layer is least able to notice: a
    vault table that gained no rows looks exactly like a vault table that had nothing
    to gain."""
    return validated_months(
        months,
        axis=axis,
        consequence="An empty or unmatched window loads nothing and reports success",
    )


def read_snapshot_window(
    spark: SparkSession,
    table: str,
    months: Sequence[str] | None,
    *,
    axis: SnapshotAxis = MONTHLY_SNAPSHOT,
) -> DataFrame:
    """`table`, narrowed to `months`, or the whole table when `months` is None.

    `None` IS THE DEFAULT EVERY CALLER SHOULD PREFER. A vault load is idempotent by
    hash key, so re-reading a month already loaded costs a scan and changes nothing,
    where omitting one leaves a hole that only a later full reload closes.

    `axis` IS KEYWORD-ONLY AND DEFAULTED, WHICH IS WHAT KEEPS THIS A GENERALISATION
    RATHER THAN A MIGRATION. Nine call sites across six loaders pass a window to this
    function and every one of them is over a monthly RFB or generated source; the
    default is what they already meant, so none of them had to change to keep being
    right. A source with a finer axis passes its own, and the two loaders that already
    take an `ObservationGrain` read it off THAT rather than accepting a second
    parameter -- a grain and an axis argument that could disagree would be two
    spellings of one decision, and the disagreement would land as a window that
    silently selected nothing."""
    frame = spark.read.table(table)
    window = _validated_months(months, axis)
    if window is None:
        return frame
    if axis.column not in frame.columns:
        raise ValueError(
            f"{table!r} has no {axis.column} column, so a {axis.name} window cannot be "
            "applied to it. Every bronze table carries the axis its BronzeTable "
            "declares; a source whose column is missing is not the snapshot its "
            "registry entry claims and must be loaded whole"
        )
    return frame.filter(F.col(axis.column).isin(list(window)))
