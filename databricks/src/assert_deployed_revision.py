# databricks/src/assert_deployed_revision.py
"""Job task: refuse the run unless the wheel it installed was built from the revision
the run was launched for.

RUNS FIRST IN ITS JOB, ahead of everything -- ahead of the unzip in the per-table
jobs, ahead of `ensure_masked_table` in socios, ahead of the ingest in the lookup
job, ahead of the promote in the operator's repromote job. That position is the whole
value: a check that runs after the unzip reports the problem once several GB have
moved, and one that runs after `ensure_masked_table` reports it after DDL from an
unreviewed wheel has already touched the tables holding personal names.

WHAT IT COMPARES, and why it is two things (ADR 0009). The EXPECTED revision arrives
in argv from the launch -- `--params revision=$(git rev-parse HEAD)` -- so it is the
operator's local HEAD at the moment the run was submitted. The ACTUAL revision comes
from `built_revision()`, which reads what the DEPLOYED wheel carries. Two sources, or
there is no check: read both from the same place and the comparison passes always
while looking exactly like a working one.

CI VALIDATES THE REPOSITORY AND NEVER WHAT IS DEPLOYED, which is why this task is not
redundant with the test suite. A green run is evidence about the artefact in the
workspace, and that is HEAD only if somebody deployed HEAD -- a separate act, by
hand, with nothing checking it. On 2026-08-01 a socios re-run terminated SUCCESS
having masked only bronze, because the workspace was still running a bundle deployed
four commits earlier. Nothing went red.

NO SPARK, and nothing else either: this task imports argv and the comparison, and
that short import list is asserted by tests/test_assert_deployed_revision_task.py. A
session here would cost a serverless start on every run of every guarded job to
compare two strings, and anything else imported is either work done before refusing
or a second source for one of the two values.

argv: [revision]"""
import sys

from opl.bronze.provenance import assert_revision_matches, built_revision


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    # An absent argument is passed through as "" so that `assert_revision_matches`
    # refuses it with the message that names the launch command -- the same shape as
    # `table_spec(args[0] if args else "")` in every other task here. Absence must
    # refuse, not skip: "nothing to compare" would switch the check off in exactly the
    # case it exists for, a run launched by a path that forgot the parameter.
    expected = args[0] if args else ""
    # The two sources, side by side and on one line, deliberately: this is the line a
    # reviewer reads to see that the check has two of them.
    verified = assert_revision_matches(expected=expected, actual=built_revision())
    # The success line matters as much as the refusal. PR A's stale-bundle incident was
    # found by a human reading a task log against the source, and what made that
    # necessary is that no log line named the code that was running.
    print(
        f"assert_deployed_revision: OK -- the deployed wheel was built from {verified}, "
        "which is the revision this run was launched for. Every task after this one runs "
        "that code."
    )


if __name__ == "__main__":
    main()
