# src/opl/vault/observation.py
"""The observation ledger: for one business key and one snapshot month, WHAT WE SAW,
never what we conclude from it. Five states, derived from bronze and the DQ gate's
quarantine, at whatever grain the caller names.

THE PROBLEM THIS EXISTS FOR. An ordinary Data Vault satellite is delta-driven on a
`hash_diff`: it writes a row only when the payload changes. A key that is absent from
a load produces no comparison and therefore no row -- byte-identical, in the
satellite, to a key whose payload was unchanged. So "unchanged" and "not observed"
are indistinguishable in the one place a reader would look, and four real
establishments sit in exactly that hole: present in 2026-06's bronze, absent from
2026-07's, because a DQ rule widened between the runs and quarantined them
(`encoding_replacement_char`). Nothing in the vault says so.

WHY THIS IS DERIVED AND NOT A RECORD TRACKING SATELLITE. DV2's answer is an RTS --
one row per hash key per load it appeared in, written unconditionally, so absence is
a missing row rather than silence. The pattern exists because staging is transient.
OURS IS NOT: bronze is append-only, carries `_snapshot_month` and
`_snapshot_ref_date`, and is never dropped; the quarantine tables carry the rejects
with the same keys and the same durability. Presence per month is therefore already
recorded, and an RTS would re-record it at ~169M rows per month, forever, with no
compression pattern anyone has published, on a Free Edition workspace. Measured
against real bronze, THIS derivation -- five states, not the four-state draft -- runs
in 93s over estabelecimentos (72.3M keys x 2 months) and 26s over socios at link
grain, both first-run and not cache-served. See ADR 0010, whose cost table carries
both versions: the absence split cost 0s and 2s, i.e. nothing measurable.

THE FIVE STATES, and why the last two are two and not one:

| in bronze for M | in quarantine for M | state |
|---|---|---|
| yes | no  | `observed` |
| yes | yes | `observed_with_rejected_siblings` |
| no  | yes | `rejected_by_our_gate` -- not observed, and WE are the reason |
| no  | no  | an absence, split below |

The overlap in row two is not a corner case: of 680 quarantined sócio keys in
2026-07, 679 are also in bronze that same month (at link grain, 5 of 1,786). It
exists because every grain in this vault is COARSER THAN A SOURCE ROW -- one partner
key spans many partnership rows, and one row can be rejected while its siblings pass
-- so collapsing it into `observed` or `rejected` would mean picking a winner
silently.

THE ABSENCE SPLIT WAS FORCED BY MEASUREMENT, not by symmetry. A grid of every key
against every month marks a key whose first appearance is 2026-07 as absent in
2026-06 -- and on estabelecimentos that is **444,520 keys**, on socios at link grain
**219,370**. Calling that one thing "candidate delete" would have the ledger assert
half a million false departures in the first month it covers. So:

    `absent_before_first_observation`  -- no month at or before this one, WITHIN THE
                                          WINDOW READ, showed this key at all. Not a
                                          departure; there was nothing to depart from.
    `absent_after_observation`         -- we saw this key earlier and did not see it
                                          here. A CANDIDATE delete, never an asserted
                                          one.

DEFINED ON THE PAST ONLY, and that is load-bearing rather than tidy. The split could
have been "before first observation / after LAST observation", which reads more
naturally and is unstable: a key absent in July and back in August would be "after
last observation" while July is the end of the data and something else once August
lands, so a row already written would change meaning as new months arrived. Comparing
each month against the key's FIRST observation only can never be revised by a later
load. (The comparison is a STRING one being used as a chronological one, which is a
property of the axis's FORMAT rather than of this module: `_snapshot_month` is
`YYYY-MM`, and the fixed-width UTC instant beside it in `opl.bronze.snapshot_axis`
sorts the same way. That module holds both formats and the argument for why each has
the property.)

THE AXIS IS THE SOURCE'S, NOT THIS MODULE'S, SINCE F-DB. It used to be the module
constant `_snapshot_month`, which was true of every source this lakehouse had and
false the moment one was observed TWICE INSIDE ONE CALENDAR MONTH: both observations
carry one month value, the fold below collapses them, and a key present in the first
and gone from the second reads `observed` rather than `absent_after_observation` --
the only state the vault's end-dating path acts on. A `BronzeTable` declares its axis,
`grain_for` carries it onto the grain, and every read below asks the grain. The five
states, the branch order, the grid and the fold are UNCHANGED; what changed is which
column they are taken over.

THE STATES ARE RELATIVE TO THE WINDOW, AND FOR NARROWING THE LEDGER CANNOT WARN YOU. A
key observed in 2026-05 and asked about over `["2026-06", "2026-07"]` reads as
`absent_before_first_observation` in June, because the months it was not given are
months it never read. `months=None` -- every month the two tables hold -- is the
default for that reason. `first_observed_month` is an output column so the truncation
is at least visible: a key whose first observation equals the window's first month
may simply have been observed before it.

WIDENING IS THE OTHER DIRECTION AND IT IS REFUSED, not warned about, because it fails
far louder. A month with no row on either side -- a typo, an ingest that did not run --
gives EVERY key in the window an absence row for it, i.e. a candidate delete for the
whole table. `_window` refuses that; see it for why this one is worth an eager Spark
job when the three refusals above are free.

WHAT THIS MODULE DELIBERATELY DOES NOT DO:

  - **It does not assert a delete, and gives nobody a shortcut to one.** There is no
    `is_departure` helper and no state whose name contains "delete". A caller that
    wants a departure signal maps `absent_after_observation` onto one in their own
    code, where the choice is visible in review. That is the point: a key vanishing
    because our own gate rejected it must never reach a satellite as a departure from
    the registry, and the way to make that hard to get wrong is to make the wrong
    thing require typing.
  - **It does not hash.** The ledger is keyed on business-key columns in the clear, so
    a caller can consult it before or after applying `opl.vault.hashing.hash_key`. A
    Spark-side spelling of that standard would be a SECOND spelling of it, and this
    module is not where that gets decided. **A DECLARED PREFIX IS NOT AN EXCEPTION TO
    THIS.** Since F-DB Task 5's correction a grain may declare a `KeyPrefix`, and the
    ledger then keys on the first `width` characters of that column -- which is the
    value the link hashes, and without it the ledger is FINER than the link it gates.
    Everything the sentence above is for survives a prefix: the value stays readable,
    reconcilable against the source by eye, and joinable without the digest. What is
    refused is a hash, not a projection.
  - **It does not carry `_snapshot_ref_date`.** For an absent (key, month) there is no
    row on either side to take one from, so it would have to be asserted from the
    month -- a claim about a key that has no row. The month-to-ref-date mapping is one
    row per month and belongs wherever a caller needs a date, not here.
  - **It names no table.** Table names arrive on an `ObservationGrain`, and
    `ObservationGrain.in_default_schema` routes them through `opl.config` so no
    catalog or schema is ever spelled in this layer. The CNPJ domain's grains belong
    in `opl/vault/domains/cnpj.py`; this file is the mechanism.

HUB GRAIN AND LINK GRAIN ARE THE SAME CODE PATH. They differ only in `key_columns` --
`("cpf_cnpj_socio",)` against `("cnpj_basico", "identificador_socio",
"cpf_cnpj_socio")` -- and nothing below branches on which it was handed. They are not
interchangeable in USE: a partner who loses one of two partnerships is
`absent_after_observation` at link grain and plainly `observed` at hub grain, which is
why an effectivity satellite closing windows on a relationship has to read the link
grain. `tests/vault/test_observation.py` asserts that divergence over one table."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.bronze.snapshot_axis import MONTHLY_SNAPSHOT, SnapshotAxis
from opl.config import DEFAULT
from opl.vault.months import validated_months
from opl.vault.specs import KeyPrefix

# THE DEFAULT AXIS'S COLUMN, DERIVED AND NOT RESTATED, kept as a name because TESTS read
# it and because it is genuinely the column every source in this lakehouse carried before
# F-DB. (This said "the CNPJ domain and its tests"; `grep -rn MONTH_COLUMN src/ databricks/`
# finds only this definition, so the domain does not read it and the tests are the whole
# reason the name survives.) It is NOT what this module reads: every site
# below asks the GRAIN, because the axis is now the source's declaration and this
# constant is only the default one's spelling. A grain-dependent path that reaches for
# this name instead is the bug `ObservationGrain.snapshot_column` exists to prevent --
# it would answer correctly for all six registered tables today and silently wrongly
# for the first one that is not monthly -- `bronze_merchant`, which Task 4 adds. (This
# read "six of the seven" before that table existed; the registry holds SIX.)
MONTH_COLUMN = MONTHLY_SNAPSHOT.column

# THE EVIDENCE, kept beside the verdict. These three are output columns rather than
# internals: the state is a label derived from them, and an operator (or the query
# that counts the state mix against real bronze) should be able to check a verdict
# without re-deriving it.
IN_BRONZE_COLUMN = "in_bronze"
IN_QUARANTINE_COLUMN = "in_quarantine"
FIRST_OBSERVED_COLUMN = "first_observed_month"

STATE_COLUMN = "observation_state"


class ObservationState(Enum):
    """What we saw. Not what happened -- see the module docstring.

    A plain `Enum` over a `str` mixin, so `.value` has to be spelled at every point
    where the string reaches Spark or a comparison. The values are what land in the
    data and what a satellite will filter on; `test_the_five_state_values_are_pinned`
    holds them as literals for the same reason the hash standard pins its digests."""

    OBSERVED = "observed"
    # In bronze AND in quarantine in the same month: some rows carrying this key
    # passed and others were rejected. Real and dominant at hub grain (679 of 680
    # quarantined sócio keys in 2026-07), because a hub key is coarser than a row.
    OBSERVED_WITH_REJECTED_SIBLINGS = "observed_with_rejected_siblings"
    # Absent from bronze because OUR gate removed it. The source published this key;
    # the vault must never read this as a departure from the registry.
    REJECTED_BY_OUR_GATE = "rejected_by_our_gate"
    # Absent, and never seen at or before this month in the window read. Not a
    # departure -- 444,520 establishments are in this state in 2026-06 purely because
    # they first appear in 2026-07.
    ABSENT_BEFORE_FIRST_OBSERVATION = "absent_before_first_observation"
    # Absent, having been seen earlier. THE ONLY CANDIDATE DELETE, and a candidate is
    # all it is: it is equally the shape of a missed file, a dropped partition, or an
    # entity that returns next month.
    ABSENT_AFTER_OBSERVATION = "absent_after_observation"


@dataclass(frozen=True)
class ObservationGrain:
    """One grain to derive a ledger at: the two tables, and the columns that make a
    business key at that grain.

    THE GRAIN IS DATA, NOT A CODE PATH. Wave 1 needs two (hub and link) and wave 2
    will add more; a second copy of the derivation per grain would be two things to
    keep in step, and the divergence would be invisible until a satellite disagreed
    with the ledger gating it.

    `key_columns` IS VALIDATED AT CONSTRUCTION, before Spark, because each of the
    three refusals below is a caller mistake that otherwise produces a
    plausible-looking ledger rather than an error:

      - EMPTY collapses the whole table into one group, which reports `observed` for a
        single row that identifies nothing. Same family as `hash_key`'s refusal of an
        empty component list, and refused for the same reason: once a loader has
        consumed the answer, the mistake looks like a modelling decision.
      - A BARE `str` satisfies `Sequence[str]` structurally, so no type checker
        catches `key_columns="cnpj_basico"` -- it iterates to twelve one-character
        column names and fails inside Spark, naming a column called `c`.
      - THE SNAPSHOT AXIS in the key makes every key present in exactly the snapshot it
        names and absent in every other: a complete ledger of nonsense, no error.
      - A REPEATED column produces duplicate output columns, which every downstream
        `select` then has to disambiguate.

    `snapshot_axis` IS DEFAULTED, AND THAT DEFAULT IS WHAT MAKES THIS FIELD FREE TO ADD.
    Every grain this repository declares -- the three in `opl/vault/domains/cnpj.py` --
    is over a monthly RFB snapshot, so all three keep their exact construction and the
    ledger keeps its exact answer over the two real months. What the field buys is that
    the refusal above and the eleven reads below stop being about a module constant: a
    source observed twice inside one month declares a finer axis on its `BronzeTable`,
    `grain_for` carries it here, and the SAME code path derives the SAME five states
    over it. See `opl.bronze.snapshot_axis` for why the declaration lives on the source.

    IT IS CARRIED, NOT RE-DECIDED. Nothing constructs a grain by hand in a job: both
    `grain_for` functions take the axis off the `BronzeTable` they were already given.
    A grain whose axis disagreed with its bronze table's would read a column the table
    does not have -- which `_side` refuses by name rather than answering.

    `key_prefixes` IS THE OTHER HALF OF THE KEY, AND WITHOUT IT A GRAIN CAN ONLY NAME THE
    COLUMN IT READS AND NOT THE VALUE IT KEYS ON. `link_merchant_empresa` is identified by
    `substring(cnpj, 1, 8)` and its identity COLUMN is `cnpj`, fourteen characters; that
    map is many-to-one, so a grain keyed on the name alone is strictly finer than the link
    it gates, and `opl.vault.effectivity` states what finer costs -- it closes windows
    that never departed. Measured before this field existed: one merchant that kept its
    root and changed its full `cnpj` got an ACTIVE row and a CLOSING row on the same
    `applied_date` for the same hash key, with `appended=3, closed=1` reading ordinary.

    EMPTY FOR EVERY GRAIN DECLARED BEFORE F-DB TASK 5's CORRECTION, which is what makes it
    free to add: the three in `opl/vault/domains/cnpj.py` read every key component under
    its own name, so all three keep their exact construction and the ledger keeps its
    exact answer over the two real months. It is derived from the link spec by
    `registry.identity_derivations_of` rather than restated, exactly as `key_columns` is
    derived by `identity_columns_of`, and `effectivity._refuse_a_mismatched_link_grain`
    compares BOTH against the link."""

    name: str
    bronze_table: str
    quarantine_table: str
    key_columns: Sequence[str]
    snapshot_axis: SnapshotAxis = MONTHLY_SNAPSHOT
    key_prefixes: Sequence[KeyPrefix] = ()

    @property
    def snapshot_column(self) -> str:
        """The bronze column this grain's ledger is derived against.

        A property rather than eleven `grain.snapshot_axis.column` spellings: the two
        are the same read, and the short one is what makes the diff against the module
        constant it replaced legible."""
        return self.snapshot_axis.column

    def __post_init__(self) -> None:
        if isinstance(self.key_columns, str):
            raise TypeError(
                f"key_columns received a bare str {self.key_columns!r} -- a str is a "
                "Sequence[str] structurally, so this would key the grain on its "
                f"individual characters; pass a tuple, e.g. ({self.key_columns!r},)"
            )
        columns = tuple(self.key_columns)
        if not columns:
            raise ValueError(
                f"grain {self.name!r} has no key columns -- a business key needs at "
                "least one, or every row of the table folds into a single group that "
                "identifies nothing and reports itself observed"
            )
        # THE GRAIN'S OWN AXIS, NOT THE DEFAULT ONE. Read as a module constant this
        # guard would keep refusing `_snapshot_month` for a source that does not have
        # that column, and would WAVE THROUGH the axis of a source that has another --
        # a guard that has silently stopped guarding while still passing its own test.
        if self.snapshot_column in columns:
            raise ValueError(
                f"grain {self.name!r} names {self.snapshot_column} as a business-key "
                "column. It is the ledger's other axis: keying on it makes every key "
                "present in exactly the snapshot it names and absent in all the others"
            )
        if len(set(columns)) != len(columns):
            raise ValueError(
                f"grain {self.name!r} names a key column more than once ({columns}), "
                "which would put duplicate columns in the ledger"
            )
        object.__setattr__(self, "key_columns", columns)
        object.__setattr__(
            self, "key_prefixes", _validated_prefixes(self.key_prefixes, self.name, columns)
        )

    def key_expression(self, column: str) -> Column:
        """One key column, READ THE WAY THE GRAIN SAYS IT IS READ: the column itself, or
        its first `width` characters where a prefix is declared for it.

        ALIASED BACK TO THE SOURCE COLUMN'S OWN NAME, which is a decision and not an
        accident. The ledger's `cnpj` then holds eight characters where bronze holds
        fourteen -- the value is the LINK's identity component and the name is the column
        it was read from, exactly as `loading.hash_key_over` is "the same standard, read
        somewhere else". Renaming it to the hub's `cnpj_basico` would be the more honest
        label and would cost a SECOND hash expression in `effectivity._departures`, which
        today keys the ledger with the very expression that keyed bronze; a prefix is
        idempotent (`substring(substring(x, 1, w), 1, w) == substring(x, 1, w)`), so
        re-applying it to an already-derived value is a no-op and the one spelling
        survives. This module's `hash_key` bullet is not violated either: a prefix is not
        a hash, the value stays readable and reconcilable against the source by eye."""
        prefix = self._prefix_for(column)
        if prefix is None:
            return F.col(column)
        return F.substring(F.col(column), 1, prefix.width).alias(column)

    def _prefix_for(self, column: str) -> KeyPrefix | None:
        """The declared prefix on `column`, or None. At most one -- `_validated_prefixes`
        refuses a second, and `__post_init__` has already refused a repeated key column."""
        for prefix in self.key_prefixes:
            if prefix.column == column:
                return prefix
        return None

    @classmethod
    def in_default_schema(
        cls, *, name: str, bronze: str, quarantine: str, key_columns: Sequence[str],
        snapshot_axis: SnapshotAxis = MONTHLY_SNAPSHOT,
        key_prefixes: Sequence[KeyPrefix] = (),
    ) -> ObservationGrain:
        """A grain over two tables in the configured catalog and schema.

        THE ONLY PLACE THIS MODULE TOUCHES `opl.config`, and the reason it does:
        without it a domain file would spell the qualification itself, and a
        hardcoded `workspace.default` is one careless line away from that. Tests
        build grains over throwaway databases through the plain constructor, which
        is why this is a second entry point rather than the only one."""
        return cls(
            name=name,
            bronze_table=DEFAULT.table(bronze),
            quarantine_table=DEFAULT.table(quarantine),
            key_columns=tuple(key_columns),
            snapshot_axis=snapshot_axis,
            key_prefixes=tuple(key_prefixes),
        )


def _validated_prefixes(
    prefixes: Sequence[KeyPrefix], name: str, columns: tuple[str, ...]
) -> tuple[KeyPrefix, ...]:
    """`prefixes` frozen, having refused the two ways a derivation goes silently unapplied.

    `KeyPrefix` HAS ALREADY REFUSED A BLANK COLUMN AND A NON-POSITIVE WIDTH, at its own
    construction, so what is left here is the pairing against THIS grain:

      - A PREFIX ON A COLUMN THIS GRAIN IS NOT KEYED ON derives nothing. The ledger would
        be keyed on the raw column, i.e. exactly the finer-than-the-link grain the
        declaration was added to prevent, and the declaration would sit in the spec
        looking like it had taken effect.
      - TWO PREFIXES ON ONE COLUMN cannot both apply. `key_expression` takes the first,
        which would make the ledger's grain depend on declaration order silently."""
    frozen = tuple(prefixes)
    unkeyed = sorted({prefix.column for prefix in frozen} - set(columns))
    if unkeyed:
        raise ValueError(
            f"grain {name!r} declares a key prefix on {unkeyed}, which is not among its "
            f"key columns {columns}. A prefix names the column it TRUNCATES, so one "
            "naming a column the grain is not keyed on derives nothing and leaves the "
            "ledger keyed on the raw value -- finer than the link, which closes windows "
            "that never departed"
        )
    seen = [prefix.column for prefix in frozen]
    if len(set(seen)) != len(seen):
        raise ValueError(
            f"grain {name!r} declares more than one key prefix on the same column "
            f"({sorted(seen)}). Only one can apply and which one would depend on "
            "declaration order"
        )
    return frozen


def _validated_months(
    months: Sequence[str] | None, axis: SnapshotAxis
) -> tuple[str, ...] | None:
    """The window, or `None` for "every month the tables hold".

    DELEGATED TO `opl.vault.months` since the F2 Task 3 review, and the delegation is
    the point: the loaders need the identical three refusals, and the copy that grew
    there was a second spelling of one rule -- the shape `opl.config` records having
    been bitten by. What stays here is the CONSEQUENCE, because it is the ledger's own
    and is not the loaders': every refusal below would otherwise produce an empty
    ledger rather than an error, and an empty ledger reads as "nothing departed"."""
    return validated_months(
        months,
        axis=axis,
        consequence=(
            "An empty or unmatched window yields an empty ledger, which reads as "
            "'nothing was rejected and nothing departed'"
        ),
    )


def _side(
    spark: SparkSession, grain: ObservationGrain, table: str, months: tuple[str, ...] | None,
    *, from_bronze: bool
) -> DataFrame:
    """One side of the evidence: the grain's key columns and month, flagged.

    Projected to the key columns and the month and nothing else, which is what keeps
    the derivation's cost proportional to the KEY space rather than to the payload --
    bronze carries thirty columns and the ledger reads three of them.

    THROUGH `grain.key_expression`, WHICH IS WHERE A DECLARED PREFIX IS APPLIED, and it
    is the whole of the F-DB Task 5 correction. A `select` by NAME reads the column the
    link is keyed FROM rather than the value it is keyed ON, and where the two differ --
    `cnpj` at fourteen characters against the eight the link hashes -- the ledger is
    strictly finer than the link. The missing-column check above is unchanged and still
    covers it: a prefix may only name a column this grain is already keyed on."""
    frame = spark.read.table(table)
    missing = [
        column
        for column in (*grain.key_columns, grain.snapshot_column)
        if column not in frame.columns
    ]
    if missing:
        raise ValueError(
            f"grain {grain.name!r} needs {missing} in {table!r}, which has "
            f"{sorted(frame.columns)}. Bronze and its quarantine are separate tables "
            "with separate schemas, so a column present on one side can be absent on "
            "the other -- and the ledger's answer depends on both"
        )
    frame = frame.select(
        *(grain.key_expression(column) for column in grain.key_columns),
        grain.snapshot_column,
    )
    if months is not None:
        frame = frame.filter(F.col(grain.snapshot_column).isin(list(months)))
    return (frame
            .withColumn(IN_BRONZE_COLUMN, F.lit(from_bronze))
            .withColumn(IN_QUARANTINE_COLUMN, F.lit(not from_bronze)))


def _window(
    spark: SparkSession, presence: DataFrame, grain: ObservationGrain,
    months: tuple[str, ...] | None
) -> DataFrame:
    """The months the ledger will report on, refusing one the data has never seen.

    A MONTH WITH NO ROW ON EITHER SIDE IS REFUSED, and it is the most dangerous
    argument this module takes. `_validated_months` checks the SHAPE of a month;
    nothing there can know whether it was ever loaded. Name `2026-09` -- a typo, or a
    month whose ingest failed -- and every key in the window gets a row for it with
    nothing on either side, which is `absent_after_observation`: at real scale, 72
    MILLION candidate deletes manufactured out of a load that never ran. Every other
    argument mistake in this module is refused (an empty `months`, a bare `str`, a
    malformed month, a grain with no key columns); this one has the largest blast
    radius of the set and is the reason the check is worth an eager Spark job.

    ONE-SIDED EVIDENCE IS STILL EVIDENCE. The test is presence in bronze OR in
    quarantine, not both: estabelecimentos' 2026-06 has 71,874,448 bronze rows and
    zero rejects, and a month with rejects and no bronze rows is equally real.

    THE COST IS PAID ONLY BY A CALLER WHO NAMES MONTHS. With `months=None` the window
    IS the distinct months of the data, so it cannot contain one the data has not
    seen, and this returns that frame lazily without collecting anything."""
    axis = grain.snapshot_column
    loaded = presence.select(axis).distinct()
    if months is None:
        return loaded
    unloaded = sorted(set(months) - {row[axis] for row in loaded.collect()})
    if unloaded:
        raise ValueError(
            f"months names {unloaded}, which no row of {grain.bronze_table!r} or "
            f"{grain.quarantine_table!r} carries. Refusing rather than answering: "
            "every key in the window would get a row for that month with nothing on "
            "either side -- absent_after_observation, a candidate delete for the "
            "whole table, conjured out of a typo or an ingest that did not run. A "
            "month present on only ONE side is not this and is accepted"
        )
    return spark.createDataFrame([(month,) for month in months], f"{axis} string")


def _grid(
    spark: SparkSession, presence: DataFrame, grain: ObservationGrain,
    months: tuple[str, ...] | None
) -> DataFrame:
    """Every key the window shows, against every month of the window, flagged as
    seen nowhere -- the rows that make an absence a ROW rather than a silence.

    Each carries `first_observed_month`, the earliest month either table showed the
    key in: quarantine counts as showing it, because being rejected is itself
    evidence that the source published the key. THAT DECISION IS LOAD-BEARING and is
    pinned by `test_a_month_we_rejected_counts_as_having_seen_the_key` -- see
    `_state_column`, whose branch order is often credited with the property this line
    actually provides. It rides in on the grid so the fold below needs no second pass
    over the data and no join back onto the business key."""
    universe = presence.groupBy(*grain.key_columns).agg(
        F.min(grain.snapshot_column).alias(FIRST_OBSERVED_COLUMN)
    )
    window = _window(spark, presence, grain, months)
    return (universe.crossJoin(window)
            .withColumn(IN_BRONZE_COLUMN, F.lit(False))
            .withColumn(IN_QUARANTINE_COLUMN, F.lit(False)))


def _state_column(axis: str) -> Column:
    """The five states. Read in this order, and the order is INERT -- see below.

    `axis` IS THE GRAIN'S SNAPSHOT COLUMN, AND THE COMPARISON AGAINST IT IS A STRING
    COMPARISON BEING USED AS A CHRONOLOGICAL ONE. That is a property of the axis's
    FORMAT, not of this function: `YYYY-MM` sorts chronologically, and so does the
    fixed-width UTC instant `opl.bronze.snapshot_axis` declares beside it -- which is
    why both qualifiers are pinned there rather than left to a source to get right.
    An axis whose values did not sort that way would put every absence on the wrong
    side of the split below, with no error.

    QUARANTINE IS CONSULTED BEFORE THE ABSENCE SPLIT, AND THAT BUYS NOTHING. An
    earlier version of this docstring claimed it did: that a key we reject on its
    first sight would otherwise read `absent_before_first_observation` -- "we had not
    seen it yet" -- when we had seen it and thrown it away. **It could not.** For any
    month in which a key is in quarantine, that month is inside the `min` that
    produced `first_observed_month` (`_grid` takes it over bronze UNION quarantine),
    so `month < first_observed_month` is false there whatever the order. Swapping
    these two branches changes no answer on any input, no test can pin the order, and
    the reading is kept in this order only because it reads in the order of the
    evidence.

    THE PROPERTY THAT DOCSTRING WAS DESCRIBING IS REAL, AND IT LIVES ONE FUNCTION
    AWAY: `_grid` counting a quarantined month as having seen the key. Redefine
    `first_observed_month` as "first observed IN BRONZE" -- a plausible-looking
    cleanup -- and a key quarantined in June, absent in July and in bronze in August
    reads July as `absent_before_first_observation` instead of
    `absent_after_observation`. That is the failure this order LOOKS like it prevents
    and does not; `test_a_month_we_rejected_counts_as_having_seen_the_key` is what
    prevents it. Anyone tempted by that cleanup should start there, not here.

    The final branch is `otherwise`, not a fifth `when`: `first_observed_month` is
    never NULL (a key is in the universe only because some month observed it) and a
    month that is not before the first observation is at or after it, so there is no
    sixth case to fall through to."""
    return (
        F.when(
            F.col(IN_BRONZE_COLUMN) & F.col(IN_QUARANTINE_COLUMN),
            F.lit(ObservationState.OBSERVED_WITH_REJECTED_SIBLINGS.value),
        )
        .when(F.col(IN_BRONZE_COLUMN), F.lit(ObservationState.OBSERVED.value))
        .when(F.col(IN_QUARANTINE_COLUMN), F.lit(ObservationState.REJECTED_BY_OUR_GATE.value))
        .when(
            F.col(axis) < F.col(FIRST_OBSERVED_COLUMN),
            F.lit(ObservationState.ABSENT_BEFORE_FIRST_OBSERVATION.value),
        )
        .otherwise(F.lit(ObservationState.ABSENT_AFTER_OBSERVATION.value))
    )


def observation_ledger(
    spark: SparkSession, grain: ObservationGrain, *, months: Sequence[str] | None = None
) -> DataFrame:
    """One row per business key per month in the window, each carrying one
    `ObservationState` and the evidence it was derived from.

    THE GRAIN OF THE RESULT, stated because it is not the only defensible one: the
    key universe is every key either table shows in the window, and every key gets a
    row for EVERY month of the window -- including months in which it appears
    nowhere, which is the only way an absence can be a row rather than a silence.
    Keys seen only outside the window are not reported at all; narrowing `months`
    narrows both axes.

    NO JOIN ON THE BUSINESS KEY, ANYWHERE, and that is not a performance choice.
    `NULL = NULL` is NULL in SQL, so an equality join would fail to match a key to
    itself wherever a component is NULL -- 12,824 foreign partners carry
    `cpf_cnpj_socio` NULL -- and would report those keys absent in every month while
    they sat in bronze the whole time. A wrong answer with no error attached. Instead
    both sides and the all-keys-by-all-months grid are UNIONED and folded by
    `groupBy`, which treats NULL as a value that equals itself. The one join here is
    the `crossJoin` that builds the grid, and it has no condition to get wrong."""
    window = _validated_months(months, grain.snapshot_axis)
    presence = (_side(spark, grain, grain.bronze_table, window, from_bronze=True)
                .unionByName(_side(spark, grain, grain.quarantine_table, window,
                                   from_bronze=False)))

    # The grid covers every (key, month) the result can hold, so folding it together
    # with the two evidence sides gives exactly one row per pair: `max` of the flags
    # is a boolean OR, and `max` of the first-observed month picks the grid's value
    # over the evidence rows' NULL. `unionByName` is load-bearing here and not a
    # habit -- the grid's columns arrive in a different order.
    everything = presence.withColumn(
        FIRST_OBSERVED_COLUMN, F.lit(None).cast("string")
    ).unionByName(_grid(spark, presence, grain, window))
    return (
        everything.groupBy(*grain.key_columns, grain.snapshot_column)
        .agg(
            F.max(IN_BRONZE_COLUMN).alias(IN_BRONZE_COLUMN),
            F.max(IN_QUARANTINE_COLUMN).alias(IN_QUARANTINE_COLUMN),
            F.max(FIRST_OBSERVED_COLUMN).alias(FIRST_OBSERVED_COLUMN),
        )
        .withColumn(STATE_COLUMN, _state_column(grain.snapshot_column))
    )
