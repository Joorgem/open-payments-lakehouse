# src/opl/dataops/freshness.py
"""How old is each bronze table, and how old is what is IN it. Two questions, never one.

TWO METRICS BECAUSE THEY HAVE DIFFERENT ANSWERS AND DIFFERENT DOMAINS.

  * **Pipeline freshness** -- `now - MAX(_ingested_at)`. When did this project last write
    this table? Defined for all seven bronze tables.
  * **Source freshness** -- `now - MAX(_snapshot_ref_date)`. How old is the observation
    the rows describe? Defined for FIVE: `payments` and `ptax` carry no such column,
    because a generated stream and an API response declare no date of their own, and
    `rules.py` argues at length that stamping one anyway would have meant dropping the
    control that refuses it -- a control omitted so that its own refused value could be
    written.

Collapsed into one number, the metric prints NULL for two of seven tables and invites the
reading that they are broken. Kept apart, each is defined exactly where it means something.

THE VERDICT IS ON SOURCE FRESHNESS ONLY, AND THAT IS A REFUSAL RATHER THAN AN OMISSION.
`opl.dataops.cadence` declares an expected rhythm for the sources that have a publisher.
Pipeline freshness has none to declare: `databricks/resources/` contains no `schedule:`,
no `trigger:` and no `continuous:` block, and all 29 `ingest` task runs in this workspace
were launched by hand -- measured spread, hours to 18 days. A threshold on that number
would be measuring when an operator last typed a command. So `pipeline_age_days` ships
with no status attached, which is the honest shape, and `cadence` carries the argument.

SEVEN STATUSES, AND THE ORDER OF THE LADDER IS THE WHOLE DESIGN.

  * `paused_by_decision` -- FIRST, and it outranks everything including a table with no
    rows at all. `lookup` is two snapshot months behind its three CNPJ siblings (2026-06
    against 2026-07, with merchant/payments/ptax at 2026-08). Measured 2026-08-18 21:55Z:
    its `last_source_date` is 2026-06-13, so `source_age_days` is 66 -- which, against the
    45-day expectation its siblings carry, is 21 days PAST, not 66. The age and the
    overshoot are different numbers and this header used to print the first as the second.
    Either way it is not overdue; it is a scope decision
    somebody recorded, and `cadence_note` prints the citation beside it. A metric that
    cannot tell "deliberately not ingested" from "ingest broken" is the alert an operator
    mutes in week one, after which it protects nothing.
  * `never_ingested`      -- the table holds no rows, so every age below it is NULL and
                             none of the remaining arms means anything.
  * `no_source_axis`      -- there is no `_snapshot_ref_date` to be stale. Structural.
                             `payments` and `ptax`, and the NULL in `source_age_days` for
                             them is "not applicable", not "unknown" and not "0".
  * `no_declared_cadence` -- there IS a source date and no defensible rhythm to judge it
                             against. Reported, not scored. `merchant`.
  * `source_date_missing` -- rows are there, a cadence is declared, and every
                             `_snapshot_ref_date` is NULL. THIS ARM IS THE ONE THAT STOPS
                             THE LADDER FAILING OPEN: without it, `source_age_days >
                             expected_every_days` is NULL against a NULL age, SQL's
                             three-valued logic makes that neither true nor false, and the
                             row falls through to `within_cadence` -- a table with no
                             observable date at all reported as fresh.
  * `overdue`             -- the only fault this view emits.
  * `within_cadence`      -- the ELSE, reachable only after the five arms above have
                             excluded every way the comparison could be meaningless.

THE LADDER IS SPELLED ONCE, IN SQL, and driven out of real tables by
`tests/dataops/test_freshness.py` rather than matched as a string -- for
`opl.bronze.reconcile`'s reason: an arm nothing can enter is a status that will never be
wrong, and three of these seven cannot be reached by anything in the live workspace today.

WHAT THIS VIEW COSTS. `MAX` over a timestamp column has no metadata shortcut, so reading
it scans that column across all seven bronze tables -- 337M rows, of which
`bronze_cnpj_estabelecimentos` is 144M. Measured against the live warehouse on 2026-08-18,
the same seven aggregates returned inside one `wait_timeout` window. It is a view: nothing
pays that until somebody reads it, and nothing writes a row to avoid it."""
from __future__ import annotations

from opl.bronze.registry import REGISTRY, BronzeTable
from opl.bronze.snapshot import SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN
from opl.config import DEFAULT, OplConfig
from opl.dataops.cadence import (
    CADENCE,
    NO_SOURCE_AXIS,
    PAUSED,
    UNDECLARED,
    Cadence,
    declares_source_date,
)

FRESHNESS_VIEW = "dataops_freshness"

# The stamp every bronze row carries, whatever its source
# (`opl.bronze.autoloader.add_common_audit_columns`). The other two columns this view reads
# ARE module constants and are imported; this one is a literal inside `withColumn`, and
# `masking.py` already spells it a second time in its DDL tuple -- so rather than add a
# third unchecked spelling,
# `tests/dataops/test_freshness.py::test_the_ingest_stamp_writes_the_column_this_view_reads`
# runs the stamp and asserts the column it produces is this one. A rename would otherwise
# be caught only by an UNRESOLVED_COLUMN at deploy, which is loud but is in the workspace.
INGESTED_AT_COLUMN = "_ingested_at"

PAUSED_BY_DECISION = "paused_by_decision"
NEVER_INGESTED = "never_ingested"
NO_SOURCE_DATE_COLUMN_STATUS = "no_source_axis"
NO_DECLARED_CADENCE = "no_declared_cadence"
SOURCE_DATE_MISSING = "source_date_missing"
OVERDUE = "overdue"
WITHIN_CADENCE = "within_cadence"

# First-match-wins, like `reconcile._VERDICT_LADDER` and `dq._reject_reason`. The arms are
# not disjoint and the ORDER is what decides; the header argues each position.
_STATUS_LADDER = (
    (PAUSED_BY_DECISION, f"cadence_kind = '{PAUSED}'"),
    (NEVER_INGESTED, "last_ingested_at IS NULL"),
    (NO_SOURCE_DATE_COLUMN_STATUS, f"cadence_kind = '{NO_SOURCE_AXIS}'"),
    (NO_DECLARED_CADENCE, f"cadence_kind = '{UNDECLARED}'"),
    (SOURCE_DATE_MISSING, "last_source_date IS NULL"),
    (OVERDUE, "source_age_days > expected_every_days"),
)


def status_case_sql() -> str:
    """The ladder as one CASE expression. Spelled once, here."""
    arms = "\n    ".join(f"WHEN {predicate} THEN '{name}'" for name, predicate in _STATUS_LADDER)
    return f"CASE\n    {arms}\n    ELSE '{WITHIN_CADENCE}'\n  END"


def sql_string_literal(text: str) -> str:
    r"""A SQL string literal, backslash-escaped -- and NOT SQL-standard doubled.

    PUBLIC BECAUSE THE LAST PARAGRAPH BELOW ALREADY PUBLISHES A CONTRACT -- reach for this
    rather than an f-string -- while an underscore name said the opposite, and
    `opl.triage_agent.severity` reached past it across a package boundary anyway. The
    contradiction was the name, so the name changed.

    THE OBVIOUS SPELLING IS WRONG HERE AND IT IS WRONG SILENTLY, which is why this is a
    function with a paragraph rather than an f-string. Measured 2026-08-18 on local Spark
    3.5.9 and on the `opl-free` SQL warehouse, the same answer from both:

        SELECT 'don''t', length('don''t')   ->   dont, 4

    `''` is the SQL-standard escape and Spark's lexer does not read it as one: it ends the
    literal and starts another, and adjacent string literals concatenate. So the apostrophe
    is DELETED from the operator's note, the query parses, the view is created, and nothing
    anywhere reports a problem. Backslash escaping is what Spark actually implements
    (`spark.sql.parser.escapedStringLiterals` defaults to false, which enables it), and it
    returns `don't`. The backslash itself is escaped first, or a note containing one would
    build an escape sequence out of the character after it.

    These strings are English prose written for an operator to read, so an apostrophe is a
    matter of time. Refusing the character instead would push the escaping problem into the
    prose, which is the half of this that a reviewer would have to keep noticing.

    IT HAS ONE OTHER CALLER, AND THE SWEEP BELOW IS TWO CLAIMS RATHER THAN ONE.
    `opl.triage_agent.severity.hold_note_sql` interpolates a declared hold's English `why`
    and calls this rather than spelling the escape again, so the sentence that used to
    stand here -- that this function is not reused -- is gone rather than softened. THE
    COMPLETENESS HALF IS F4's AND HAS EXPIRED: as of F4, every other site in this
    repository that interpolated a Python value into a SQL string was enumerated:
    `reconcile._REMEDY_PREFIX`/`_SUFFIX` and `_VERDICT_LADDER`, `promote.py:177`,
    `retention.py:262`, `reclaim_landing.py:329/383/402`, `dq_gate_batch.py:100`,
    `backfill_masks.py:90/91`, `cnpj_pool.py:167`, `ptax_source.py:202`. It names no
    `opl.triage_agent` module and all three of F6's build SQL by interpolation, so it is a
    phase-old census, not the present tense it was written in.

    THE SAFETY HALF STILL HOLDS, F6's sites included: none of them is exposed today. Not because
    any escapes -- each interpolates an identifier, a registry key, an enum or module constant,
    a declared bundle job name, a validated integer (`evidence`'s `limit`, `severity_rank_sql`'s
    `rank`) or a batch id, none of which has ever held an apostrophe and the integer of which
    cannot. F6's one site interpolating prose written for a human is `severity.hold_note_sql`,
    which calls this. So the hazard is LATENT, not closed: the day one of those constants gains
    an apostrophe -- a remedy line rephrased, a verdict given a possessive -- it behaves exactly
    as the doubled form does here, the character is deleted, the statement parses and nothing
    raises. Anyone building a SQL string out of English text should reach for this function, and
    anyone adding prose to a constant above should know that the site around it does not."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _measured_sql(spec: BronzeTable, config: OplConfig) -> str:
    """One bronze table's three maxima and its row count, as one arm of the union.

    `CAST(NULL AS DATE)` FOR THE TWO WITH NO SOURCE DATE, rather than omitting the column:
    every arm of a UNION ALL must have the same shape, and the NULL is the truthful value
    -- there is no such date. `cadence` declares those two `NO_SOURCE_AXIS` and an import
    guard holds that declaration to the same derivation this line uses, so the column can
    never be read off a table that does not have it."""
    source_date = (
        f"MAX({SNAPSHOT_REF_DATE_COLUMN})"
        if declares_source_date(spec)
        else "CAST(NULL AS DATE)"
    )
    return (
        f"SELECT '{spec.name}' AS source, COUNT(*) AS bronze_rows,\n"
        f"    MAX({INGESTED_AT_COLUMN}) AS last_ingested_at,\n"
        f"    MAX({SNAPSHOT_MONTH_COLUMN}) AS last_snapshot_month,\n"
        f"    {source_date} AS last_source_date\n"
        f"  FROM {config.table(spec.bronze)}"
    )


def _cadence_row_sql(table: str, cadence: Cadence) -> str:
    """One declaration, as a row of the inline VALUES the measurements join to.

    INLINE RATHER THAN A DELTA TABLE, and that is the point of shipping the cadence as
    data in the repository: a table would have to be written by something, and a
    declaration that lives in a table nobody writes is a threshold with no diff and no
    owner. Here the number reaching the view is the number in the commit."""
    days = "NULL" if cadence.every_days is None else str(cadence.every_days)
    note = sql_string_literal(cadence.why)
    return (
        f"SELECT '{table}' AS source, '{cadence.kind}' AS cadence_kind, "
        f"CAST({days} AS INT) AS expected_every_days, {note} AS cadence_note"
    )


def freshness_sql(config: OplConfig = DEFAULT) -> str:
    """Every registered bronze table, its two ages, its declared cadence and one status.

    Total over `REGISTRY` on the measurement side and over `CADENCE` on the declaration
    side, and the two sets are held equal at `opl.dataops.cadence` import -- so a table
    registered later is measured the day it is registered and cannot arrive without an
    expectation, which is the pair of failures a hand-written list here would allow."""
    measured = "\n  UNION ALL\n  ".join(
        _measured_sql(spec, config) for spec in REGISTRY.values()
    )
    declared = "\n  UNION ALL\n  ".join(
        _cadence_row_sql(table, cadence) for table, cadence in CADENCE.items()
    )
    return (
        f"WITH measured AS (\n  {measured}\n),\n"
        f"declared AS (\n  {declared}\n),\n"
        "aged AS (\n"
        "  SELECT m.source, m.bronze_rows, m.last_ingested_at,\n"
        "    datediff(current_date(), CAST(m.last_ingested_at AS DATE)) AS pipeline_age_days,\n"
        "    m.last_snapshot_month, m.last_source_date,\n"
        "    datediff(current_date(), m.last_source_date) AS source_age_days,\n"
        "    d.cadence_kind, d.expected_every_days, d.cadence_note\n"
        "  FROM measured m JOIN declared d ON d.source = m.source\n"
        ")\n"
        f"SELECT *, {status_case_sql()} AS source_freshness_status\nFROM aged"
    )
