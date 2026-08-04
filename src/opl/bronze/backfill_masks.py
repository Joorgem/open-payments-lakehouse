# src/opl/bronze/backfill_masks.py
"""The UC column mask the backfill's overwrite may have dropped, and putting it back.

`mode("overwrite").saveAsTable` on an existing table is planned by Spark either as
an overwrite of the DATA (`OverwriteByExpression`, metadata kept) or as a replace
of the TABLE (`AtomicReplaceTableAsSelect`, which does not keep it) -- both plans
were OBSERVED for that exact write, and a replace drops CHECK constraints, NOT NULL
*and a column mask*. This module is the third of those three.

Its only caller is `scripts/backfill_snapshot_columns.py`, which keeps the write
itself and the constraint re-assertion -- so a backticked name below that is not
defined here (`_assert_constraints`, `fill_and_overwrite`,
`_warn_on_null_ref_dates`) is that script's.

WHY THE LIBRARY AND NOT A SIBLING UNDER `scripts/`, and what that means for
deploying the script as a job: see `opl.bronze.backfill_prewrite`, which carries
the full ruling. In one line -- the script already imports `opl.*`, so the wheel is
already on the job's path and carries this for free, whereas a sibling would rest
on `sys.path[0]` inside the IPython shell serverless executes the file in.

LESS OF A ONE-OFF THAN ITS SIBLING, which is the one thing that makes this
module's presence here easy rather than merely paid for. "Ask the catalog whether
it reports the mask, and refuse to report success until it does" is not migration
logic: `databricks/src/ensure_masked_table.py` issues the same statements from the
same module and verifies none of them. Its failure messages still name the backfill
because the backfill is what has run them, and they are left verbatim rather than
generalised on a control whose failure state is personal names in the clear.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from opl.bronze.masking import (
    MASK_FUNCTION,
    MASKED_COLUMNS,
    mask_function_ddl,
    masked_table_ddls,
    set_mask_ddl,
)
from opl.bronze.registry import BronzeTable
from opl.config import DEFAULT


def required_masks(spec: BronzeTable, tbl: str) -> tuple[str, ...]:
    """The columns UC must be masking on `tbl` once this run is over, or ().

    WHICH TABLES ARE MASKED IS `opl.bronze.masking`'S DECISION, asked rather than
    restated: `masked_table_ddls` names them, and it names bronze and quarantine
    and deliberately NOT staging. That exclusion is load-bearing in the other
    direction from everything else here -- `promote_batch` READS staging and writes
    what it read into bronze, so applying a mask there would put `***` into bronze
    permanently and would make the DQ gate stop rejecting rows with a missing name.
    A list of masked tables spelled again in this file could gain staging by a
    plausible-looking edit; asking the module that argues about it cannot.

    WHICH COLUMNS is `MASKED_COLUMNS`, keyed by CONTRACT, so a contract that is not
    masked returns () and the whole of the restoration below is a no-op. That is
    what lets this run against estabelecimentos unchanged."""
    columns = MASKED_COLUMNS.get(spec.contract, ())
    if not columns:
        return ()
    covered = tuple(
        table
        for table, _ in masked_table_ddls(
            bronze=DEFAULT.table(spec.bronze),
            quarantine=DEFAULT.table(spec.quarantine),
            contract=spec.contract,
        )
    )
    return columns if tbl in covered else ()


def mask_probe_sql(tbl: str) -> str:
    """The columns UC reports as masked on `tbl` -- the VERIFICATION, not the fix.

    `system.information_schema.column_masks` is the view ADR 0008 and both run
    evidence documents already query against this workspace, so it is a measured
    object rather than a guessed one; the three name filters follow
    `information_schema.columns`' own `table_catalog`/`table_schema`/`table_name`
    spelling, which §8.2 of the PR B evidence queries the same way.

    ASKED OF THE CATALOG RATHER THAN INFERRED FROM THE STATEMENT SUCCEEDING,
    because the failure this closes is precisely a statement that ran against a
    table whose metadata something else had changed underneath it. A `SET MASK`
    that returns without error proves the ALTER was accepted; it does not prove the
    catalog now reports a mask on the column an operator will read from."""
    catalog, schema, name = tbl.split(".")
    return (
        "SELECT column_name FROM system.information_schema.column_masks "
        f"WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' "
        f"AND table_name = '{name}'"
    )


def restore_masks(
    spark: SparkSession, spec: BronzeTable, tbl: str, *, wrote: bool
) -> tuple[str, ...]:
    """Re-apply the UC column masks the overwrite may have dropped. The columns it
    re-applied, so the caller can verify exactly those.

    WHY THIS EXISTS, and why it is not symmetric decoration beside
    `_assert_constraints`. `mode("overwrite").saveAsTable` on an existing table is
    planned either as an overwrite of the DATA (`OverwriteByExpression`, metadata
    kept) or as a replace of the TABLE (`AtomicReplaceTableAsSelect`, which does
    not keep it) -- both plans were OBSERVED for this exact write, and which one
    Unity Catalog picks is not knowable from here. A replace drops CHECK
    constraints, NOT NULL *and a column mask*. `_assert_constraints` put the first
    two back and nothing put the third back, which left the PII control as the one
    piece of metadata resting on the plan going the other way.

    "UNREACHABLE TODAY" WAS TRUE AND IS NOT THE POINT. Today a `socios --bronze`
    run is refused by `refuse_contradicting_month`, because that table carries both
    2026-06 and 2026-07; and `ensure_masked_table` re-applies `SET MASK`
    idempotently at the top of the next socios job. Both are properties of today's
    DATA and of a job that has to run next -- not of this code. A mask is not the
    control to leave resting on either.

    NOT A SECOND SPELLING OF THE DDL. The statements come from
    `opl.bronze.masking`, the same module `databricks/src/ensure_masked_table.py`
    issues them from, and both are idempotent by measurement: `CREATE OR REPLACE
    FUNCTION` is the documented way to modify a function a live mask references,
    and a second `SET MASK` on an already-masked column was PROBED against this
    workspace and succeeded (ADR 0008). So this costs nothing on the far commoner
    path where the plan kept the metadata and the mask never went anywhere.

    THE FUNCTION FIRST, then the masks -- `ensure_masked_table`'s ordering, and for
    its reason: a `SET MASK` naming a routine that does not exist fails the ALTER.
    A table REPLACE cannot drop the function (it is a separate object), but this
    script may also be the first thing to run in a workspace where the socios job
    has not, and `CREATE OR REPLACE` is free.

    NO WRITE, NOTHING TO RESTORE. The ALTER that adds the columns is a metadata
    commit that does not replace the table, so `wrote=False` is not a case where a
    mask can have been dropped -- and `fill_and_overwrite` returning False is
    exactly the 0-row quarantine path whose docstring argues that skipping the
    write is what removes the class.

    BEFORE `_assert_constraints`, and the order matters rather than being tidy. UC
    refuses a CHECK constraint on a table carrying a column mask
    (`COLUMN_MASKS_CHECK_CONSTRAINT_UNSUPPORTED`), which is why the registry fails
    at import if a masked contract declares one -- so for every contract that
    reaches here masked, `spec.constraints` is NOT NULL DDL only, and applying the
    mask first cannot make the constraint DDL fail. Going the other way would put
    the PII control behind a statement that re-validates metadata over the whole
    table, widening the window in which the rows are readable in the clear for no
    reason at all."""
    columns = required_masks(spec, tbl)
    if not columns or not wrote:
        # Said out loud in both directions: an operator has to be able to tell "this
        # table carries no mask" from "the restoration did not run".
        print(
            f"backfill: no column mask to restore on {tbl} -- "
            + (
                "no overwrite was issued, so the table cannot have been replaced"
                if columns
                else f"{spec.contract} declares no masked column"
            )
        )
        return ()
    function = DEFAULT.table(MASK_FUNCTION)
    spark.sql(mask_function_ddl(function))
    for column in columns:
        spark.sql(set_mask_ddl(tbl, column, function))
        print(f"backfill: re-applied the column mask on {tbl}.{column} ({function})")
    return columns


def observed_masks(spark: SparkSession, tbl: str) -> frozenset[str]:
    """The columns UC currently reports a mask on for `tbl`."""
    rows = spark.sql(mask_probe_sql(tbl)).collect()
    return frozenset(str(row["column_name"]) for row in rows)


def verify_masks_or_raise(
    observed: frozenset[str], required: tuple[str, ...], *, tbl: str, version: int
) -> None:
    """Refuse to report success while a required mask is not in the catalog.

    Pure, so both verdicts are covered by a test that needs no workspace -- which
    matters more here than for the row counts: the state this refuses is a
    populated table whose personal names are readable by anyone who can select
    from it, and it is reachable only through a plan Unity Catalog chooses.

    RAISES, and does not warn. `_warn_on_null_ref_dates` warns because a NULL
    reference date is a DESIGNED outcome that a correct run can produce. An absent
    mask is not: `restore_masks` has just issued the statement, so its absence
    means the statement did not take, and there is no reading of that on which the
    run succeeded."""
    unmasked = tuple(column for column in required if column not in observed)
    if not unmasked:
        return
    raise RuntimeError(
        f"backfill REFUSED to report success on {tbl}: the UC column mask is NOT in "
        f"the catalog for {', '.join(unmasked)} after this run re-applied it. THE "
        "DATA IS WRITTEN AND THE PERSONAL NAMES IN THOSE COLUMNS ARE READABLE IN "
        "THE CLEAR to every principal that can select from this table. This is why "
        "the overwrite is dangerous on a masked table at all: it may be planned as "
        "a table REPLACE, which drops the mask along with the constraints. Put the "
        "mask back before anything reads the table -- `databricks/src/"
        "ensure_masked_table.py` issues exactly these statements and is idempotent, "
        "so running that task against this contract is the supported repair. The "
        f"catalog currently reports masks on: {', '.join(sorted(observed)) or '(none)'}. "
        f"The write itself is still reversible with: RESTORE TABLE {tbl} TO VERSION "
        f"AS OF {version} -- but note that RESTORE moves the DATA and does not put a "
        "mask back, so the mask is the thing to fix first."
    )
