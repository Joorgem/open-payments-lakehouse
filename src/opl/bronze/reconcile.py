"""Does every row this project staged end up somewhere it can be found?

WHAT THIS ANSWERS, and why it is not already answered anywhere. Three tables hold
one ingest's rows: staging takes everything Auto Loader read, bronze takes what the
gate passed, quarantine takes what it rejected. Nothing in this repository has ever
compared the three. `promote.plan_promotion` compares two of them for ONE batch it
was handed; this asks the question over every batch that exists, without being told
which one to look at, which is the only way a batch nobody remembers can be found.

MEASURED BEFORE THIS FILE EXISTED, and it is why the view earns its place: payments
batch `592660596679630` holds 10,000 staged, 2,000 quarantined and **0 in bronze**.
Eight thousand rows that passed every DQ rule are in no table any count reads, and
they have been there since 2026-08-12. Every other (table, batch) pair in the
workspace reconciles exactly.

THE ARITHMETIC IS THE WHOLE THING: `staged = promoted + quarantined`. A batch where
it holds is finished, whatever route it took -- including the four CNPJ batches that
were gated and later repromoted, which is why a reconciliation is not a test for
"the gate fired". Where it does not hold, the difference is rows that exist and are
reachable by nothing.

FOUR VERDICTS, AND EACH ONE IS REACHABLE -- which is the property this file is most
careful about, because this project has now shipped four guards whose output could
not distinguish "passed" from "never ran". `tests/bronze/test_reconcile.py` drives
all four out of real Spark tables rather than asserting the SQL string, for exactly
that reason: a CASE arm nothing can enter is a verdict that will never be wrong.

  * `reconciled`           -- promoted + quarantined = staged. Nothing to do.
  * `stranded_gated`       -- rows are missing AND some were rejected, so the gate is
                              the explanation. This is `592660596679630`.
  * `stranded_unexplained` -- rows are missing and NOTHING was rejected. This is the
                              shape a mid-stream ingest failure leaves: Auto Loader's
                              `availableNow` drains in several microbatches, each its
                              own Delta commit AND its own checkpoint advance, so a
                              failure after the first commit leaves rows in staging
                              under a run id that will never come back, against files
                              the checkpoint has already consumed. It has never
                              happened here -- `ingest` is 29 for 29 -- and it is
                              named because a verdict this view cannot emit is a
                              failure mode this view does not cover.
  * `over_promoted`        -- bronze and quarantine together hold MORE than staging
                              ever did. Unreachable through the shipped flow, which is
                              precisely why it is a verdict rather than an assumption:
                              a double promote or a truncated staging table would
                              otherwise be silently rendered as `reconciled`'s
                              opposite-signed twin.

THE VERDICT IS SPELLED ONCE, IN SQL. An identical Python ladder beside it would be a
second spelling of the same rule, and this repository has paid for those: two
spellings of a sentinel, two spellings of a month, two spellings of a prefix. The
tests run the SQL.

IT ALSO PRINTS THE REMEDY, and that is a deliberate standard rather than a nicety.
`promote.require_batch_id`, `reclaim_landing._report_nothing_proven` and
`backfill_prewrite.refuse_non_empty_quarantine` all print the query or the DDL that
resolves what they refused. A reconciliation that reports a stranding and stays
silent about `repromote_triaged_batch` -- which ships, has run, and takes exactly
this batch id -- would print less than its own module's refusals do.

WHAT IT DOES NOT SAY IS WHETHER TO RUN THAT COMMAND. `592660596679630` is F1b's
drift-injection run and its 2,000 `rescued_data_present` rows are the proof the gate
catches schema drift; promoting the other 8,000 would also rewrite what the
observation ledger means for those keys. The recommendation is recorded in
`docs/f4-run-evidence.md` with an owner. A view that promoted rows would be a gate
bypass wearing a dashboard.

FILE GRAIN IS NOT A SECOND FEATURE, IT IS `reclaim_landing`'s SAFETY ARGUMENT.
`retention.files_of_batch` returned the distinct `_source_file` values BRONZE held
rows of, and its docstring called that query "the whole safety argument -- what it
returns is exactly what may be removed". That is airtight only under an
all-or-nothing gate, where bronze holding *a* row of a file implies bronze holds
*every* row of it. The moment a batch reaches bronze through a repromote it stops
being true: socios' 3,583 rejected rows span **20 distinct `_source_file` values**
whose clean rows are in bronze and whose rejected rows are only in quarantine, so a
proof phrased as "bronze holds rows of this file" would unlink a file bronze does not
hold every row of. `reclaimable` below is the repaired proof -- every staged row of
the file is accounted for, in bronze or in quarantine -- and it is a strengthening,
not a relaxation: it implies the old condition and refuses cases the old one passed.

IT IS NOW THE PROOF ITSELF AND NOT A REPORT OF ONE. When this file shipped, `reclaimable`
was a column a human could read beside a delete that was still taken on the old
condition. `RECLAIMABLE_SQL` and `file_accounts_sql` exist so that
`opl.bronze.retention` executes the same predicate on the delete path: `files_of_batch`
is gone, and the view and the reclaim can no longer disagree about which file may go.
"""
from __future__ import annotations

from opl.bronze.autoloader import SOURCE_FILE_COLUMN
from opl.bronze.promote import BATCH_COLUMN
from opl.bronze.registry import REGISTRY, BronzeTable
from opl.config import DEFAULT, OplConfig

# The two views this phase creates. Prefixed `dataops_` rather than `bronze_` on
# purpose: the three registry-collision guards range over the bronze, vault and gold
# registries, so an object in none of them is policed by nothing, and a name that
# could plausibly be a future table is how that gap turns into a collision.
# `tests/bronze/test_reconcile.py::test_the_view_names_collide_with_nothing_any_registry_owns`
# is the lock, since no registry can hold them.
BATCH_GRAIN_VIEW = "dataops_reconciliation"
FILE_GRAIN_VIEW = "dataops_reconciliation_by_file"

RECONCILED = "reconciled"
STRANDED_GATED = "stranded_gated"
STRANDED_UNEXPLAINED = "stranded_unexplained"
OVER_PROMOTED = "over_promoted"

# `reclaim_landing`'s repaired proof, over the three counts at FILE grain, and the
# only spelling of it: the file-grain view publishes it and `retention` executes it,
# so the number a dashboard shows and the predicate a delete is taken on cannot drift.
#
# IT IS A STRENGTHENING OF `promoted > 0`, NOT A REPLACEMENT FOR IT, and the
# implication is the point. `promoted > 0` is the whole of the proof `files_of_batch`
# used to offer -- bronze holds a row of this file -- and it survives here as the first
# conjunct, so every file this predicate admits was already admitted by the old one.
# The second conjunct only ever removes files, never adds one: the delete set is a
# SUBSET of what shipped before, which is the direction a retention control is allowed
# to move in. What it removes is exactly the case a repromote creates -- clean rows in
# bronze, rejected rows only in quarantine, and possibly rows in neither -- where the
# old proof read "bronze holds a row of this file" as "bronze holds every row of it".
#
# The `promoted > 0` conjunct is NOT redundant beside the equation: a file whose rows
# are ALL rejected satisfies `promoted + quarantined = staged` with `promoted = 0`, and
# nothing of it is in the system of record. The quarantine is not persistence.
#
# NONE OF THE THREE COUNTS CAN BE NULL, and if one ever were this predicate would fail
# CLOSED rather than admit a file. Cannot: every leg of `_counts_sql`'s union emits
# either a literal `0` or a `COUNT(*)`, neither of which is nullable, and a grouped row
# exists here only because some leg produced it -- so the outer `SUM` always runs over
# at least one non-null value. Would fail closed: SQL's three-valued logic makes a
# comparison against NULL neither true nor false, `AND` propagates that, and
# `retention.file_accounts_of_batch` reads the column through `bool(row["reclaimable"])`
# -- `bool(None)` is `False`. So the file is held back and reported with its counts,
# which is the direction that loses nothing.
# Written down rather than left to be re-derived: "it cannot happen" and "and if it did
# nothing would be deleted" are two claims, and only the second is a safety property.
RECLAIMABLE_SQL = "promoted > 0 AND promoted + quarantined = staged"

# First-match-wins, like `dq._reject_reason` and for the same reason -- the arms are
# not disjoint and the order is what decides. `over_promoted` FIRST because it is the
# only arm whose condition indicates the counts themselves cannot be trusted, and a
# ladder that tested it last would report an over-promoted batch as stranded.
_VERDICT_LADDER = (
    (OVER_PROMOTED, "promoted + quarantined > staged"),
    (RECONCILED, "promoted + quarantined = staged"),
    (STRANDED_GATED, "quarantined > 0"),
)

# What resolves a stranding, printed beside the verdict.
#
# `source` IS the `--params table=` value, not a second spelling of it: the view's
# `source` column carries `BronzeTable.name`, which is the REGISTRY key, which is what
# `table_spec` resolves and what `repromote_batch_job.yml` takes. So a table registered
# later gets a correct remedy with nothing to add here -- and there is no per-table
# branch to fall out of step with the registry.
#
# `$(git rev-parse HEAD)` is left UNEXPANDED on purpose. The revision the guard checks
# must come from the operator's own repository at the moment the run is launched, not
# from whatever wheel created this view; a view that baked in its own revision would
# hand every future operator a command that passes the guard while running the wrong
# code (ADR 0009).
_REMEDY_PREFIX = "databricks bundle run repromote_triaged_batch -t free --params table="
_REMEDY_SUFFIX = ",revision=$(git rev-parse HEAD)"


def verdict_case_sql() -> str:
    """The ladder as one CASE expression. Spelled once, here."""
    arms = "\n    ".join(
        f"WHEN {predicate} THEN '{name}'" for name, predicate in _VERDICT_LADDER
    )
    return f"CASE\n    {arms}\n    ELSE '{STRANDED_UNEXPLAINED}'\n  END"


def remedy_sql() -> str:
    """The repromote invocation, as one SQL expression over `source` and `batch_id`.

    NULL where nothing is stranded, rather than a command an operator might run
    against a batch that is already finished."""
    return (
        f"CASE WHEN verdict IN ('{STRANDED_GATED}', '{STRANDED_UNEXPLAINED}') THEN\n"
        f"    concat('{_REMEDY_PREFIX}', source, ',batch_id=', {BATCH_COLUMN}, "
        f"'{_REMEDY_SUFFIX}')\n  END"
    )


def _counts_sql(
    spec: BronzeTable,
    grain: tuple[str, ...],
    config: OplConfig,
    *,
    where: str = "",
    skip: tuple[str, ...] = (),
) -> str:
    """One table's three counts, at the given grain, as a UNION ALL rather than a join.

    A three-way outer join would have to coalesce the key from whichever side is
    present; this shape has no key to coalesce and includes a batch that exists in any
    of the three tables -- including one that exists ONLY in bronze, which is what makes
    `over_promoted` observable rather than merely declared.

    `skip` OMITS A LEG WHOSE TABLE DOES NOT EXIST, and every omission is fail-closed by
    construction rather than by a branch that has to remember to be: dropping staging
    leaves `staged = 0`, dropping bronze leaves `promoted = 0`, and `RECLAIMABLE_SQL`
    refuses both. Dropping quarantine leaves `quarantined = 0`, which is the true answer
    for a table no reject has ever reached -- and if a quarantine that HELD rows is
    dropped instead, the same 0 makes the batch fail to reconcile and the files are
    refused. The view builders never pass it (all 21 objects exist, and a missing one
    must fail the deploy loudly); `retention` does, because a reclaim must decide from
    whatever exists rather than raise an AnalysisException over a table that never had
    a reason to be created.

    SKIPPING ALL THREE IS REFUSED BY NAME rather than left to Spark. It emitted an
    empty union, so the caller wrapped `FROM (\\n\\n)` and Spark raised a PARSE ERROR
    -- a syntax complaint over SQL this module generated, where the fact is that none
    of the three tables exists. Unreachable through `reclaim_landing.main`, which
    returns on a missing bronze first, and reachable through
    `retention.file_accounts_of_batch`, which is public and whose `_absent_roles` can
    return all three."""
    keys = ", ".join(grain)
    clause = f"WHERE {where} " if where else ""
    legs = tuple(
        f"SELECT {keys}, {this} FROM {config.table(table)} {clause}GROUP BY {keys}"
        for role, table, this in (
            ("staging", spec.staging, "COUNT(*) AS staged, 0 AS promoted, 0 AS quarantined"),
            ("bronze", spec.bronze, "0 AS staged, COUNT(*) AS promoted, 0 AS quarantined"),
            ("quarantine", spec.quarantine, "0 AS staged, 0 AS promoted, COUNT(*) AS quarantined"),
        )
        if role not in skip
    )
    if not legs:
        raise ValueError(_no_legs(spec, skip))
    return "\n  UNION ALL ".join(legs)


def _no_legs(spec: BronzeTable, skip: tuple[str, ...]) -> str:
    return (
        f"cannot count {spec.name}: every leg was skipped ({sorted(skip)}), so there is "
        "no union to select from and no count to take. This means staging, bronze AND "
        "quarantine are all absent -- the table has never been ingested in this "
        "workspace -- which is a fact about the catalog and not a defect in this query. "
        "A caller deciding what to do about a batch of it has nothing to decide from: "
        "no row was staged, so no file can be proven and none may be reclaimed."
    )


def _source_sql(
    spec: BronzeTable,
    grain: tuple[str, ...],
    config: OplConfig,
    *,
    where: str = "",
    skip: tuple[str, ...] = (),
) -> str:
    """One table's row of the union, summed to the grain and named."""
    keys = ", ".join(grain)
    return (
        f"SELECT '{spec.name}' AS source, {keys}, "
        "SUM(staged) AS staged, SUM(promoted) AS promoted, SUM(quarantined) AS quarantined\n"
        f"FROM (\n  {_counts_sql(spec, grain, config, where=where, skip=skip)}\n)"
        f" GROUP BY {keys}"
    )


def batch_grain_sql(config: OplConfig = DEFAULT) -> str:
    """Every registered table, every batch, one row each, with a verdict and a remedy.

    Total over `REGISTRY` rather than over a hand-written list: a table registered
    later is reconciled the day it is registered, which is the opposite of how
    `reclaim_landing`'s wiring was left behind by four jobs that each added it
    separately."""
    counted = "\n  UNION ALL\n  ".join(
        _source_sql(spec, (BATCH_COLUMN,), config) for spec in REGISTRY.values()
    )
    return (
        f"WITH counted AS (\n  {counted}\n),\n"
        f"judged AS (SELECT *, {verdict_case_sql()} AS verdict FROM counted)\n"
        f"SELECT source, {BATCH_COLUMN} AS batch_id, staged, promoted, quarantined,\n"
        "  staged - promoted - quarantined AS unaccounted, verdict,\n"
        f"  {remedy_sql()} AS remedy\n"
        "FROM judged"
    )


def file_grain_sql(config: OplConfig = DEFAULT) -> str:
    """The same arithmetic one grain down, and `reclaim_landing`'s repaired proof.

    `reclaimable` is what the reclaim's proof asks -- not "does bronze hold a row of
    this file" but "is every row this file staged accounted for" -- and it is
    `RECLAIMABLE_SQL`, the same string `retention.file_accounts_of_batch` runs, so what
    this view reports and what a delete is taken on are one predicate."""
    grain = (BATCH_COLUMN, SOURCE_FILE_COLUMN)
    counted = "\n  UNION ALL\n  ".join(
        _source_sql(spec, grain, config) for spec in REGISTRY.values()
    )
    return (
        f"WITH counted AS (\n  {counted}\n)\n"
        f"SELECT source, {BATCH_COLUMN} AS batch_id, {SOURCE_FILE_COLUMN} AS source_file,\n"
        "  staged, promoted, quarantined,\n"
        "  staged - promoted - quarantined AS unaccounted,\n"
        f"  ({RECLAIMABLE_SQL}) AS reclaimable\n"
        "FROM counted"
    )


def file_accounts_sql(
    spec: BronzeTable, config: OplConfig = DEFAULT, *, skip: tuple[str, ...] = ()
) -> str:
    """ONE table's file-grain accounts for ONE batch, as parameterised SQL.

    The same three counts, the same grain and the same `RECLAIMABLE_SQL` the file-grain
    view publishes -- narrowed to a single spec and a single batch because a reclaim
    reads this on the delete path, and the view's `GROUP BY` over 21 objects and 144M
    rows is not what a job task should scan to decide about twenty files.

    `:batch_id` IS A NAMED PARAMETER MARKER, not an interpolation. The batch id reaches
    this from a job parameter an operator typed, and it is the one value in the query
    that is not derived from the registry; `require_batch_id` refuses a blank and the
    sentinel and says nothing about quoting. `spark.sql(..., args={...})` binds it as a
    literal, so there is no spelling of it that can end the string early.

    A CALLER THAT KNOWS A TABLE IS ABSENT PASSES IT IN `skip` -- see `_counts_sql` for
    why every omission fails closed."""
    grain = (BATCH_COLUMN, SOURCE_FILE_COLUMN)
    counted = _source_sql(
        spec, grain, config, where=f"{BATCH_COLUMN} = :batch_id", skip=skip
    )
    return (
        f"SELECT {SOURCE_FILE_COLUMN} AS source_file, staged, promoted, quarantined,\n"
        f"  ({RECLAIMABLE_SQL}) AS reclaimable\n"
        f"FROM (\n{counted}\n)"
    )


def create_view_ddl(view: str, body: str, config: OplConfig = DEFAULT) -> str:
    """`CREATE OR REPLACE VIEW`, and the OR REPLACE is the idempotence.

    Not `CREATE VIEW IF NOT EXISTS`, which is the right shape for a TABLE (it must not
    drop rows -- see `masking.masked_table_ddls`) and the wrong one for a view: a view
    holds no rows, and IF NOT EXISTS would leave a definition from an older wheel in
    place while the run that was supposed to replace it reported success."""
    return f"CREATE OR REPLACE VIEW {config.table(view)} AS\n{body}"


# `view_ddls` USED TO LIVE HERE AND WAS MOVED TO `opl.dataops.views` BY F4 TASK 4, which
# added two more views. Not a tidy-up: a second list of views in a second module is a set
# the `dataops_` collision lock does not range over, and "an object in no registry is
# policed by nothing" is the exact gap the prefix and that lock exist to close. There is
# one list now, it is what the job task loops over, and it is what the lock reads.
