# databricks/src/apply_pii_governance.py
"""Job task: make the `SELECT` grants on the socios tables equal the reviewed roster,
and classify their columns in the account's own governed tag vocabulary.

THE OTHER HALF OF THE MASK. `ensure_masked_table` decides what a reader who is
already through the door sees; this decides who is at the door. Both halves must hold
for a name to be read -- a principal in `opl_pii_readers` without `SELECT` cannot open
the table at all, and a principal with `SELECT` who is not in the group reads `***`.

A SEPARATE ENTRY POINT, AND THAT IS A DECISION RATHER THAN TIDINESS. Putting these
statements inside `ensure_masked_table` was tried and refused:
`test_every_table_is_created_before_any_mask_and_the_function_before_the_first_one`
holds `len(session.statements) == len(creates) + 1 + len(masks)` with the message
"the task issues a statement this lock does not know about". That lock is the reason
the ordering argument of the masking task is checkable at all, and loosening it to
admit grants would trade a proof for a convenience.

IT TAKES NO PARAMETER, like the other two tasks in its job. What it governs is total
over `opl.bronze.pii_governance.governed_contracts()`, which is derived from
`MASKED_COLUMNS` -- so a contract that gains a mask gains its grants and its tags on
the next run, and there is no coordinate for a job YAML to get wrong.

REVOKES FIRST, ACROSS EVERY TABLE, THEN GRANTS, THEN TAGS. The order is the failure
mode: `max_retries: 0` does not prevent a retry on INTERNAL_ERROR, and a run that dies
partway through should leave FEWER readers than it found, never more. Tags are inert
metadata and go last for the same reason -- a tag that did not get applied costs a
re-run; a grant that did costs a disclosure.

IDEMPOTENT, every statement. `SHOW GRANTS` is a read; a `GRANT` for a principal that
already holds the privilege and a `REVOKE` for one that does not are both no-ops, and
this task computes its plan from what the catalog reports rather than from what it
intends -- so a second run of a successful first run issues no GRANT and no REVOKE at
all. Re-applying an identical `SET TAGS` succeeds, measured.

AND THAT IDEMPOTENCE CLAIM USED TO BE FALSE IN ONE STATE, which is what the escalation
below is for. A `SELECT` inherited from the SCHEMA is returned by `SHOW GRANTS ON TABLE`
(measured), and `REVOKE SELECT ON TABLE` against it SUCCEEDS while removing nothing --
so the old reader re-issued that revoke on every run, forever, and printed `REVOKED`
each time over a statement that changed nothing. The plan is now computed from the
grants a `REVOKE SELECT ON TABLE` was measured to remove, so every `REVOKED` line this
task prints is backed by a statement that removed a row.

WHAT IT CANNOT WITHDRAW, IT REPORTS AND THEN FAILS ON. `ALL PRIVILEGES` on the table and
a `SELECT` inherited from the schema both confer a read and both survive this task's
REVOKE. Those are printed one line each and raised as `UngovernedRead` at the END of the
run -- after every revoke, grant and tag this task CAN apply, so the loud failure never
costs the work that was safe to do. A governance task that reported SUCCESS over a
reader of 55.8M personal names it had failed to remove would be the same absence this
module replaced, wearing a green tick.

AND THE LIST OF WHAT REACHES THAT BRANCH IS NOW EVERYTHING THIS PROJECT HAS NOT CLEARED.
`opl.bronze.pii_governance` drops an observed action only if it is in `HARMLESS_ACTIONS`
-- three privileges measured against this metastore and documented to confer no read --
so `MANAGE`, which can grant itself `SELECT` and which `REVOKE ALL PRIVILEGES` does not
remove, fails this run instead of being filtered out before anything looked at it. So
does a privilege the platform invents next year.

WHY IT REACHES STAGING, WHICH THE MASK TASK REFUSES TO NAME. `ensure_masked_table`
excludes staging because a MASK there would make `promote_batch` read `***` and append
it into bronze, and would silently stop the DQ rule that rejects a missing name. A
GRANT does none of that: it changes who may open the table and changes no value any
reader gets. Staging is where the exposure actually is -- names in the clear, 55.8M
rows, and nothing drains it -- so the table the mask cannot cover is the one this task
must.

WHY IT IS GUARDED. Its whole output is a function of the deployed wheel: the roster is
a constant in `opl.bronze.pii_governance`, and this task REVOKES the table-level SELECT
of anything outside it.
A wheel from another revision therefore issues a different set of revokes against the
tables holding personal data, reports SUCCESS, and leaves an access state nobody
reviewed. That is the sharpest answer any task in this job has to that question.

argv: [] -- this task takes none."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.pii_governance import (
    GOVERNED_ROLES,
    ObservedGrant,
    UngovernedRead,
    classified_columns,
    governed_contracts,
    grant_select_ddl,
    grants_to_settle,
    plan_grants,
    revocable_principals,
    revoke_select_ddl,
    set_column_tag_ddl,
    show_grants_sql,
    unrevocable_escalation,
    unrevocable_grants,
)
from opl.bronze.registry import REGISTRY
from opl.config import DEFAULT


def governed_tables() -> tuple[tuple[str, str], ...]:
    """(contract, qualified table) for every table this task governs.

    Total over the registry: every registered table whose CONTRACT declares a mask,
    in each of `GOVERNED_ROLES`. A contract keyed in `MASKED_COLUMNS` that no table
    ingests contributes nothing here, which is the same silent-inertness
    `tests/bronze/test_masking.py::test_every_masked_contract_is_one_a_registered_
    table_ingests` already refuses on the mask's behalf."""
    contracts = set(governed_contracts())
    return tuple(
        (spec.contract, DEFAULT.table(getattr(spec, role)))
        for spec in REGISTRY.values()
        if spec.contract in contracts
        for role in GOVERNED_ROLES
    )


def _observed_grants(spark, table: str) -> tuple[ObservedGrant, ...]:
    """Every grant the CATALOG reports on `table` that this task must settle, right now.

    Read rather than inferred, which is the whole of the revoke half: a plan built
    from the statements this run intends to issue could not express "somebody granted
    SELECT out of band", and that is exactly the hole ADR 0008 records as its own
    weakest paragraph -- "an absence, not a control".

    All FOUR columns, because the privilege alone does not say whether this task can
    withdraw it: `ObjectType` is what separates a grant on the table from one inherited
    from the schema, and both come back from this statement."""
    rows = spark.sql(show_grants_sql(table)).collect()
    return grants_to_settle(
        (row["Principal"], row["ActionType"], row["ObjectType"], row["ObjectKey"])
        for row in rows
    )


def _escalate(observed: dict[str, tuple[ObservedGrant, ...]]) -> tuple[str, ...]:
    """Print every read this task cannot withdraw, and hand them back to fail on.

    Printed before the first statement that CHANGES anything, so the log carries them
    even if a later statement kills the run."""
    escalations = tuple(
        unrevocable_escalation(table, grant)
        for table, grants in observed.items()
        for grant in unrevocable_grants(grants)
    )
    for escalation in escalations:
        print(f"apply_pii_governance: UNGOVERNED READ -- {escalation}")
    return escalations


def _apply_grants(spark, tables: tuple[tuple[str, str], ...]) -> tuple[str, ...]:
    """Revokes across every table first, then grants. See the header on ordering.

    Returns the escalations, which `main` raises after the tags: what this task cannot
    withdraw must not be swallowed, and must not cost the work it can do."""
    observed = {table: _observed_grants(spark, table) for _, table in tables}
    escalations = _escalate(observed)
    plans = {table: plan_grants(revocable_principals(grants))
             for table, grants in observed.items()}
    for table, plan in plans.items():
        for principal in plan.revoke:
            spark.sql(revoke_select_ddl(table, principal))
            print(f"apply_pii_governance: REVOKED SELECT on {table} from {principal} "
                  "-- it held a table-level SELECT and the roster does not name it")
    for table, plan in plans.items():
        for principal in plan.grant:
            spark.sql(grant_select_ddl(table, principal))
            print(f"apply_pii_governance: GRANTED SELECT on {table} to {principal}")
    return escalations


def _apply_tags(spark, tables: tuple[tuple[str, str], ...]) -> None:
    """The account's governed tag keys on the columns that carry a person's identity.

    The value is always the empty string: these policies admit no other, measured."""
    for contract, table in tables:
        for column, tag_key in classified_columns(contract):
            spark.sql(set_column_tag_ddl(table, column, tag_key))
            print(f"apply_pii_governance: {table}.{column} tagged {tag_key}")


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    if args:
        raise ValueError(
            f"apply_pii_governance takes no arguments and was handed {args!r}. What it "
            "governs is total over the contracts that declare a mask -- see the "
            "header's argv line."
        )
    tables = governed_tables()
    if not tables:
        print("apply_pii_governance: no registered table ingests a contract that "
              "declares a mask -- nothing to grant, revoke or tag")
        return
    spark = SparkSession.builder.getOrCreate()
    escalations = _apply_grants(spark, tables)
    _apply_tags(spark, tables)
    print(f"apply_pii_governance: {len(tables)} table(s) governed in "
          f"{DEFAULT.catalog}.{DEFAULT.schema}")
    if escalations:
        raise UngovernedRead(
            f"{len(escalations)} read(s) of a socios table cannot be withdrawn by this "
            "task, and every statement it COULD issue has been issued. "
            + " | ".join(escalations)
        )


if __name__ == "__main__":
    main()
