# databricks/src/assert_mask_predicate.py
"""Job task: read the DEPLOYED mask function's body back out of the catalog and FAIL
if it is not the predicate this wheel ships.

THE CHECK THIS JOB PUBLISHED BEFORE THIS TASK EXISTED COULD NOT FAIL, and it was the
safety check on a privacy deploy. It said that after the run
`SELECT nome_socio_razao_social FROM workspace.default.bronze_cnpj_socios LIMIT 1`
reads `***`, and that a NAME there would mean the repair had failed open. Measured
before any run: `***` was already the answer. `opl_pii_readers` has zero members by
decision, so `is_member(...)` and `is_account_group_member(...)` are BOTH false for
every principal and both `CASE` expressions take the same `ELSE` branch -- making `***`
the answer under all four of: the repair landed, the repair did not land,
`ensure_masked_table` returned early, the task never ran. One branch, and it could not
be taken while the roster stands empty, which is a standing decision and not a
transient.

WHAT THIS TASK OBSERVES INSTEAD DISCRIMINATES, and it is one statement:
`workspace.information_schema.routines` for `mask_personal_name`. Measured 2026-08-19,
before this correction pass ran anything: `routine_definition` is
`CASE WHEN is_account_group_member('opl_pii_readers') THEN name ELSE '***' END` and
`last_altered` is `2026-08-03T21:31:27.142Z` -- the F1.4b body. After a run that
repaired it, the body must contain `is_member(`. Both facts come out of one read, and
the read is CHEAP: no table is opened and no row of the 55.8M is touched.

WHY A TASK AND NOT A PARAGRAPH IN AN EVIDENCE DOCUMENT. Nothing in this repository read
that column -- a grep for `information_schema.routines`, `routine_definition` and
`DESCRIBE FUNCTION` across `src/`, `databricks/`, `tests/` and `scripts/` returned
exactly one hit, a comment. A deployed predicate that no run asserts is a control whose
state is known only for as long as somebody remembers to look, and the thing being
deployed here is the repair of a control that spent three months unopenable without
anything noticing.

WHY IT IS NOT AN ANCESTOR OF `apply_pii_governance`, which is the placement question a
new task in this job has to answer. This task can only report; every statement the
governance task issues either REVOKES a read or writes an inert tag, and its GRANT list
is computed from a roster that is empty by decision. So a stale predicate must not stop
the revokes -- the same direction `dataops_views_job.yml` already argues for the mask
task itself: a failure state must not disable its own mitigation. The two run in
parallel off `ensure_masked_table`, and a failure here fails the RUN, which is the
strongest stop any task in this job has.

WHY `last_altered` IS PRINTED AND NOT ASSERTED. The discriminating assertion is the
BODY; `last_altered` is what tells a repair from a body that was already correct, and
this task cannot assert it moved because it runs AFTER the statement that would have
moved it and so has no reading of the run's start. Handing it one as a job parameter
would be a coordinate that can be wrong, which is what every other task in this job is
built to avoid. So it goes in the log, where the operator and the run evidence read it.

argv: [] -- this task takes none. What it checks is the one function
`opl.bronze.masking` names, resolved from `opl.config`."""
import sys

from pyspark.sql import SparkSession

from opl.bronze.masking import (
    MASK_FUNCTION,
    MASK_PREDICATE,
    MASKED_COLUMNS,
    StaleMaskPredicate,
    deployed_predicate_sql,
    predicate_is_deployed,
)
from opl.config import DEFAULT


def _deployed(spark, function: str) -> tuple[object, object]:
    """(routine_definition, last_altered) for `function`, refusing an absent row.

    An empty result is not "no opinion": it means the mask's function is not in the
    catalog at all, in a job whose previous task exists to create it."""
    rows = spark.sql(deployed_predicate_sql(function)).collect()
    if not rows:
        raise StaleMaskPredicate(
            f"{function} is not in information_schema.routines at all, so the four "
            "column masks that reference it have nothing to resolve. The task before "
            "this one issues its CREATE OR REPLACE FUNCTION; a run that reached here "
            "without it is a run whose mask task did nothing"
        )
    return rows[0]["routine_definition"], rows[0]["last_altered"]


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    if args:
        raise ValueError(
            f"assert_mask_predicate takes no arguments and was handed {args!r}. It "
            "checks the one mask function opl.bronze.masking names -- see the header's "
            "argv line."
        )
    if not MASKED_COLUMNS:
        print("assert_mask_predicate: no contract declares a masked column -- there is "
              "no mask function for this task to read back")
        return
    spark = SparkSession.builder.getOrCreate()
    function = DEFAULT.table(MASK_FUNCTION)
    definition, altered = _deployed(spark, function)
    print(f"assert_mask_predicate: {function} last_altered {altered}")
    print(f"assert_mask_predicate: routine_definition {definition}")
    if not predicate_is_deployed(definition):
        raise StaleMaskPredicate(
            f"the deployed body of {function} does not carry {MASK_PREDICATE}: "
            f"{definition!r}. Every reader still sees the masked value -- the roster is "
            "empty, so both spellings hide from everyone -- but the control deployed "
            "here is not the one this wheel ships, and the repaired one is the only "
            "spelling this workspace can ever open. See ADR 0008."
        )
    print(f"assert_mask_predicate: {function} carries {MASK_PREDICATE}, which is the "
          "predicate this wheel ships and the only one a group creatable from this "
          "workspace can make true")


if __name__ == "__main__":
    main()
