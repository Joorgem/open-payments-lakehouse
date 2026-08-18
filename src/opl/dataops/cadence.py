# src/opl/dataops/cadence.py
"""How often each bronze table is EXPECTED to be re-ingested. Declared, not measured.

WHY THIS FILE EXISTS AT ALL, AND IT IS THE WHOLE ARGUMENT. `freshness` can measure how
long ago each bronze table was last written and how old the snapshot in it is. Neither
number is a metric until something says what it should be. Nothing in this repository
did: `databricks/resources/` contains **zero** `schedule:`, `trigger:` and `continuous:`
blocks -- every one of the 29 `ingest` task runs in this workspace was launched by hand
-- so a threshold invented inside a dashboard would be measuring when an operator last
typed a command and calling it data freshness. That is the standard this phase applied
to a documented retention figure and to a billing claim, and it applies to the phase's
own metric. `tests/dataops/test_cadence.py` asserts the zero, so if a schedule is ever
added, the premise of this file changes loudly rather than silently.

IT IS A DECLARATION AND IT SAYS SO. `every_days` is a number a human typed and can be
argued with, and `why` is where the argument is. That is strictly better than the same
number typed into a dashboard tile, for one reason: here it is in the diff.

WHY THE CADENCE IS DECLARED AGAINST **SOURCE** FRESHNESS AND NOT PIPELINE FRESHNESS.
`freshness` reports both -- `now - MAX(_ingested_at)` for all seven tables, and
`now - MAX(_snapshot_ref_date)` for the five that have one. Only the second has an
external rhythm to be late against: the RFB publishes one CNPJ snapshot per month, and
the reference dates this workspace holds (2026-06-13 and 2026-07-11) put the drop around
the 11th-13th. Pipeline freshness has no such anchor -- with no schedule, "18 days since
the last ingest" is a fact about an operator's week -- so this file declares NOTHING
about it and `freshness` reports it with no verdict attached. A threshold there would be
the invented number this file exists to refuse.

FOUR KINDS, BECAUSE THERE ARE FOUR HONEST ANSWERS AND `overdue` IS ONLY ONE OF THEM.
A metric that cannot tell "deliberately not ingested" from "ingest broken" is the alert
an operator mutes in week one, after which it protects nothing.

  * `DECLARED`      -- there is a rhythm and `every_days` states it.
  * `PAUSED`        -- this table is deliberately not being ingested, and `why` cites
                       the decision. NEVER rendered as a fault. `lookup` is the live
                       instance and it is exactly the case this file was written for.
  * `UNDECLARED`    -- the table has a source date but no defensible rhythm. The number
                       is absent rather than guessed.
  * `NO_SOURCE_AXIS`-- the table carries no `_snapshot_ref_date` at all, so there is no
                       source freshness for a cadence to govern. Structural, not stale.

THE `NO_SOURCE_AXIS` DECLARATIONS ARE A SECOND SPELLING AND ARE THEREFORE LOCKED against
the first. Which tables carry `_snapshot_ref_date` is decided by WHICH OF THE THREE AUDIT
STAMPS the ingest applies (`opl.bronze.autoloader`), and `declares_source_date` reads the
registry fields that choose between them. `_assert_no_source_axis_matches_the_stamp` makes
this file's four-kind declaration a cross-check on that mechanism rather than an
independent claim about the same fact -- the shape
`_assert_prefixes_match_their_file_groups` uses for the same reason.

AND THE DERIVATION IS NOT TAKEN FROM THE DQ RULE SETS, WHICH IS WHERE THIS FILE'S FIRST
DRAFT WAS WRONG. "The table's rule set omits `unprovable_snapshot_ref_date`" looks like
the same fact and is not: `rules.py` omits it from `payments` and `ptax` because the
column is not there, and from `lookup` for an unrelated SCOPE reason with the column
present and its own comment calling it a known gap. Derived that way, this file declared
`lookup` as having no source date -- the one table whose entry the whole file exists for
-- and the import guard below is what caught it. The rule sets are still cross-checked,
but only in the direction that is a defect either way: see
`_assert_a_rule_is_never_declared_over_a_column_that_is_not_there`.
"""
from __future__ import annotations

from dataclasses import dataclass

from opl.bronze.registry import REGISTRY, BronzeTable
from opl.bronze.registry_landing import FILE_FED_LANDING_MODES
from opl.bronze.rules import REQUIRES_COLUMN, rules_for
from opl.bronze.snapshot import SNAPSHOT_REF_DATE_COLUMN
from opl.bronze.snapshot_axis import MONTHLY_SNAPSHOT

DECLARED = "declared"
PAUSED = "paused"
UNDECLARED = "undeclared"
NO_SOURCE_AXIS = "no_source_axis"

KINDS = (DECLARED, PAUSED, UNDECLARED, NO_SOURCE_AXIS)

# ONE publication period plus a fortnight, and both halves are written down because the
# number is the only invented thing in this file. 31 days is the RFB's own rhythm -- one
# CNPJ snapshot per month. The 14 is an allowance for the fact that nothing schedules the
# ingest, so somebody has to notice the drop and launch the job; it is the part of this
# constant that is a judgement rather than an observation, and it is the part to argue
# with. Spelled once and shared by the three tables it governs, so there is no drift
# between them and no invitation to tune one.
_RFB_MONTHLY_DAYS = 45


@dataclass(frozen=True, kw_only=True)
class Cadence:
    """One table's expectation. Frozen, keyword-only: `kind` and `why` are both str.

    `every_days` is NULL for every kind but `DECLARED`, and that is asserted at import
    rather than left as a convention -- a number sitting beside `PAUSED` would be read
    by the view's ladder as an expectation the table is failing, which is precisely the
    reading this file exists to prevent."""

    kind: str
    every_days: int | None
    why: str


CADENCE: dict[str, Cadence] = {
    # THE ENTRY THIS FILE WAS WRITTEN FOR. `bronze_cnpj_lookup` holds 7,408 rows from
    # 2026-06 alone, two snapshot months behind its three CNPJ siblings, and it is not
    # broken: F1.4b PR B recorded that 2026-07 lookups were out of scope for the phase
    # because the zips were never on disk. Without this entry the source-freshness metric
    # reports it 66 days stale against a 45-day expectation and an operator either chases
    # a decision somebody already took or mutes the alert.
    "lookup": Cadence(
        kind=PAUSED,
        every_days=None,
        why=(
            "not ingested since 2026-06 -- recorded scope decision, not a fault: "
            "2026-07 lookups were out of scope for the phase and their zips were never "
            "on disk (docs/f1.4b-pr-b-run-evidence.md 25.5)"
        ),
    ),
    "empresas": Cadence(
        kind=DECLARED,
        every_days=_RFB_MONTHLY_DAYS,
        why="RFB publishes one CNPJ snapshot per month; nothing here schedules the ingest",
    ),
    "estabelecimentos": Cadence(
        kind=DECLARED,
        every_days=_RFB_MONTHLY_DAYS,
        why="RFB publishes one CNPJ snapshot per month; nothing here schedules the ingest",
    ),
    "socios": Cadence(
        kind=DECLARED,
        every_days=_RFB_MONTHLY_DAYS,
        why="RFB publishes one CNPJ snapshot per month; nothing here schedules the ingest",
    ),
    # NO NUMBER, AND THE ABSENCE IS THE POINT. The merchant snapshot is taken from an
    # operational Postgres by a host-side extractor whenever somebody runs it; there is no
    # publisher with a rhythm to be late against. A number here would be this file's own
    # invented threshold, one paragraph after refusing one.
    "merchant": Cadence(
        kind=UNDECLARED,
        every_days=None,
        why=(
            "snapshot taken on demand by a host-side extractor -- no external publisher "
            "and so no rhythm to be late against; a number here would be invented"
        ),
    ),
    # THE TWO WITH NO SOURCE FRESHNESS AT ALL. Not stale, not fresh: the question does not
    # apply. Cross-checked against `rules_for` at import -- see the header.
    "payments": Cadence(
        kind=NO_SOURCE_AXIS,
        every_days=None,
        why=(
            "a generated stream declares no date of its own, so bronze carries no "
            "_snapshot_ref_date and there is no source freshness to govern"
        ),
    ),
    "ptax": Cadence(
        kind=NO_SOURCE_AXIS,
        every_days=None,
        why=(
            "the API response declares no snapshot date of its own, so bronze carries no "
            "_snapshot_ref_date and there is no source freshness to govern"
        ),
    ),
}


def declares_source_date(spec: BronzeTable) -> bool:
    """Does this table's bronze carry `_snapshot_ref_date`? Asked of the audit stamp.

    THREE STAMPS AND THE REGISTRY CHOOSES BETWEEN THEM (`opl.bronze.autoloader`):
    a FILE-FED source gets `add_audit_columns`, which derives the date from the RFB's
    filename token; a source whose snapshot instant travels in the row gets
    `add_instant_audit_columns`, which derives it from the column its axis names; anything
    else gets `add_common_audit_columns`, which omits the column because the source
    declares no such date. `landing` and `snapshot_axis` are the two fields that decide,
    so this is the mechanism read back rather than a correlate of it."""
    return spec.landing in FILE_FED_LANDING_MODES or spec.snapshot_axis != MONTHLY_SNAPSHOT


def _omits_the_unprovable_ref_date_rule(spec: BronzeTable) -> bool:
    """Does this table's DQ rule set carry no rule reading `_snapshot_ref_date`?"""
    reasons = {reason for reason, _ in rules_for(spec.contract)}
    return not any(
        REQUIRES_COLUMN.get(reason) == SNAPSHOT_REF_DATE_COLUMN for reason in reasons
    )


def _assert_every_registered_table_declares_a_cadence() -> None:
    """TOTAL over `REGISTRY`, in both directions.

    A table registered later with no entry here would otherwise fall out of the freshness
    view silently -- present in bronze, absent from the metric -- which is the shape that
    left `reclaim_landing` wired into four jobs and missing from the fifth path."""
    declared, registered = set(CADENCE), set(REGISTRY)
    if declared != registered:
        raise ValueError(
            f"cadence is not total over the bronze registry: undeclared tables "
            f"{sorted(registered - declared)}, declared but unregistered "
            f"{sorted(declared - registered)}. Every bronze table needs an expectation "
            "or it is measured against nothing"
        )


def _assert_only_a_declared_cadence_carries_a_number() -> None:
    """`every_days` is set for `DECLARED` and for nothing else.

    A number beside `PAUSED` is the defect this whole file is aimed at: the freshness
    ladder would compare against it and report a recorded decision as an overdue ingest.
    A `DECLARED` kind without one compares against NULL, which SQL's three-valued logic
    turns into `within_cadence` -- a table that can never be late."""
    for table, cadence in CADENCE.items():
        if cadence.kind not in KINDS:
            raise ValueError(f"{table} declares cadence kind {cadence.kind!r}; known: {KINDS}")
        if (cadence.kind == DECLARED) != (cadence.every_days is not None):
            raise ValueError(
                f"{table} declares kind {cadence.kind!r} with every_days="
                f"{cadence.every_days!r}. Only {DECLARED!r} carries a number: a number on "
                f"any other kind is compared against, and {DECLARED!r} without one is a "
                "table that can never be late"
            )
        if not cadence.why.strip():
            raise ValueError(
                f"{table} declares a cadence with no reason. `why` is what the view prints "
                "beside the status, and a status nobody can trace to a decision is the "
                "alert that gets muted"
            )


def _assert_no_source_axis_matches_the_stamp() -> None:
    """The cross-check: this file's `NO_SOURCE_AXIS` and the audit stamp name the same set.

    Two spellings of one fact, made into a lock rather than left as two claims. If a table
    is registered whose stamp writes the column and this file is not updated, the freshness
    view would report `no_source_axis` over a column that is there -- and the reverse
    builds `MAX(_snapshot_ref_date)` over a column that is not, which fails the view's
    CREATE at deploy time rather than silently, but in the workspace and after a commit."""
    declared = {table for table, c in CADENCE.items() if c.kind == NO_SOURCE_AXIS}
    derived = {spec.name for spec in REGISTRY.values() if not declares_source_date(spec)}
    if declared != derived:
        raise ValueError(
            f"cadence declares {sorted(declared)} as having no source date, but the audit "
            f"stamp the registry chooses says {sorted(derived)}. One of the two is stale, "
            f"and the freshness view would be wrong about {sorted(declared ^ derived)}"
        )


def _assert_a_rule_is_never_declared_over_a_column_that_is_not_there() -> None:
    """The rule sets, cross-checked in the ONE direction that is a defect either way.

    A table with no `_snapshot_ref_date` whose rule set nevertheless carries
    `unprovable_snapshot_ref_date` gets a control that `REQUIRES_COLUMN` skips on every
    frame forever -- declared, never run, and indistinguishable in any output from a
    control that ran and found nothing. That is this repository's most-hunted species and
    it is worth an import-time refusal.

    THE OTHER DIRECTION IS DELIBERATELY NOT ASSERTED HERE. `lookup` carries the column and
    omits the rule, which `rules.py` records as a known gap held open by scope rather than
    by a decision. Refusing it at import would make this module fail over somebody else's
    open question; `tests/dataops/test_cadence.py` pins it instead, so closing the gap is
    noticed and nothing is blocked meanwhile."""
    wrong = sorted(
        spec.name
        for spec in REGISTRY.values()
        if not declares_source_date(spec) and not _omits_the_unprovable_ref_date_rule(spec)
    )
    if wrong:
        raise ValueError(
            f"{wrong} declare a rule reading {SNAPSHOT_REF_DATE_COLUMN} on a table whose "
            "audit stamp never writes that column, so the rule is skipped on every frame "
            "and its absence from a report is indistinguishable from a clean measurement"
        )


_assert_every_registered_table_declares_a_cadence()
_assert_only_a_declared_cadence_carries_a_number()
_assert_no_source_axis_matches_the_stamp()
_assert_a_rule_is_never_declared_over_a_column_that_is_not_there()
