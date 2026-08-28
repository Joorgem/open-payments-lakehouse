# src/opl/triage_agent/incidents.py
"""Which DQ gates fired, ONE RECORD EACH. The feed every other F6 module reads.

WHAT AN INCIDENT IS HERE: one job run in which `fail_on_dq` ran. Not one task-run
attempt, and that distinction is the whole of this file.

THE COUNT, MEASURED 2026-08-24 against the `opl-free` workspace. Filtering
`dataops_task_telemetry` on `task_key = 'fail_on_dq'` returns **22 rows**, which are
**11 incidents**: every one of them carries exactly TWO rows, `attempt` 1 and 2, both
`result_state = 'FAILED'`, both `sql_telemetry = 'no_sql_attributed'`, both
`timeline_periods = 1`. That is `max_retries: 0` failing to prevent a retry -- a fact
this repository has now measured three times in three phases (`opl.dataops.telemetry`'s
header records 24 (job run, task key) pairs holding two `run_id`s, of which `fail_on_dq`
alone accounts for 11).

A FEED THAT RETURNS 11 AND A FEED THAT RETURNS 22 BOTH LOOK LIKE THEY WORKED, and that
is why this file's tests are shaped the way they are. Both are plausible numbers, both
render as a list of failures, and the wrong one double-counts every incident in this
corpus -- silently, because the duplicate rows are not identical and so do not look like
duplicates. ADR 0018's standing instruction is that when a check reports the expected
value you ask what ELSE would produce that value; a fixture with one attempt per incident
would report 11 whether or not the grouping exists, so `tests/triage_agent/
test_incidents.py` drives the real two-attempt shape and ALSO asserts that the naive
spelling returns 22. The guard is proven to be guarding something rather than decorating
something that was never at risk.

`attempts` IS KEPT AND REPORTED, NOT DISCARDED. "This incident ran two attempts" is a
fact a triager wants -- it is the difference between one failure and a job that retried
into the same wall -- and it is precisely the fact whose accidental double-count is the
trap. Collapsing the rows without carrying the number would trade a wrong answer for a
missing one.

THE SOURCE IS THE F4 VIEW AND NOT `system.lakeflow`. `opl.dataops.telemetry` already made
four corrections against the raw system tables -- the join key, the hour-sliced periods
that make `run_id` non-unique, `MAX` rather than `SUM` on durations that REPEAT across
those periods, and a 4.5x fan-out on the statement side. Reading the timeline directly
here would be a second spelling of all four, and the spelling that goes stale is the one
no deploy exercises. What this file adds is the ONE fold that view deliberately does not
do: it publishes one row per attempt, on purpose, because hiding a retry is worse than
showing it. Folding attempts into incidents is a consumer's job, and this is the consumer.

THE INCIDENT IS THAT THE GATE TASK RAN -- there is NO `result_state = 'FAILED'` predicate,
and its absence is a decision rather than an oversight. `fail_on_dq` is reached only when
`check_bad_rows` returns "false", i.e. only when the gate already found rejected rows, so
its mere presence in the timeline IS the incident. Filtering on the terminal state would
make the feed's membership depend on a column that is FAILED for a structural reason, and
would silently drop an incident whose attempt the platform has recorded with a NULL state
-- a task run the timeline has ingested but not finished, which `telemetry.py`'s header
documents as a live edge. What the states were is reported instead, in `result_states`,
where a reader can see them and an incident whose attempts DISAGREE is visible rather
than filtered.

ONE TASK KEY IS PINNED, AND THIS PROJECT HAS ALREADY RETIRED ONE SILENTLY. `dq_gate` became
`dq_gate_batch` mid-project; the telemetry keeps serving runs under BOTH names and marks
neither as superseded, so a history query keyed on the surviving spelling alone returns ONE
gate run for `opl-bronze-cnpj-lookup` whose real history is SIX -- and 1 raises nothing, is
not NULL, and is a perfectly plausible answer (`docs/f6-run-evidence.md` 0.8). That hazard
was therefore asked of THIS key rather than assumed away, and HOW it was asked is published
beside the answer, because a conclusion on its own cannot be falsified: a census of every
distinct `task_key` in `dataops_task_telemetry` with its job-run count and its window --
NOT a reading of the job YAMLs, which describe only today and are exactly what a retirement
leaves looking clean. Measured 2026-08-24: the workspace holds two retired/successor pairs,
`dq_gate`(5)/`dq_gate_batch`(24) and `reclaim`(1)/`reclaim_landing`(4), and NEITHER is a
failure task. `fail_on_dq` has no predecessor spelling here.

WHAT THAT DOES NOT BUY, STATED SO THE PARAGRAPH ABOVE IS NOT READ AS PROTECTION. The YAML
lock catches a FUTURE rename, in the commit that makes it, because the reader that sweeps
the bundle matches on this constant -- `test_the_lock_catches_a_gate_task_renamed_in_the_
bundle_and_not_here` fires it. It does not recover history: runs already recorded under a
retired spelling stay outside this feed until somebody widens the key by hand, and nothing
in the wheel can know they exist.

THE BATCH KEY IS A STRING AND THE TWO WAYS OF GETTING IT WRONG ARE BOTH SILENT. A
quarantine's `_batch_id` is of type STRING and equals the failing run's `job_run_id`.
Measured, both against real tables:

  * `t.run_id = CAST(q._batch_id AS BIGINT)` returns **0 rows and raises nothing**. It is
    the wrong key twice over -- `run_id` is the TASK-run id, not the job-run id -- and the
    cast makes the mismatch a type coercion rather than an error. Nothing distinguishes
    its empty result from a quarantine that is genuinely empty.
  * Joining the RAW timeline to a quarantine on the RIGHT key returns **4,000 rows for a
    2,000-row quarantine**, because the two attempt rows fan every quarantined row out
    twice. Every count taken off that join is exactly 2x, and 2x is not a number that
    looks wrong.

So `batch_id` is published as its own column, `CAST(job_run_id AS STRING)`. The cast is a
no-op today -- `job_run_id` is already STRING in the source view -- and it is written
anyway, because it is the column's TYPE that is the contract with the quarantine, and a
platform that ever widened `job_run_id` to a BIGINT would otherwise break the join in the
workspace and nowhere else. `tests/triage_agent/test_incidents.py` pins both halves by
RUNNING them: the wrong join, whose zero it asserts beside a non-empty control, and the cast,
over a fixture whose `job_run_id` really is a BIGINT -- which is the only input that can tell
the cast from the no-op it is today.

THE JOB NAME CARRIES A BUNDLE PREFIX AND IT MUST NOT BE PINNED. `databricks.yml`'s only
target is `mode: development`, which prefixes every deployed job name with
`[dev <operator>] `, so the runtime `job_name` of these eleven incidents is e.g.
`[dev <operator>] opl-bronze-cnpj-socios`. That prefix contains an operator's username.
CLAUDE.md forbids committing one -- the run-evidence docs redact the same string as
`<WINGET-PACKAGES-DIR>` for the same reason -- so it is stripped by a pattern that matches
ANY leading bracketed token (`_BUNDLE_PREFIX`), never by matching a name. A feed that
pinned the operator's spelling would also break for the next operator, which is the
cheaper half of why this is right.

`source` IS THE REGISTRY KEY, AND THE NAME IS THE SIBLING VIEWS' AND NOT A NEW ONE. Both F4
views already publish that value under that name -- `dataops_reconciliation.source` and
`dataops_freshness.source` -- so calling it `table` here would have made one value wear TWO
names across THREE sibling views, and every later join would carry a translation for a
rename that never had to happen. (Two names and not three: the third view is the one that
would have introduced the second spelling, which is the whole cost.) It is what
`table_spec` resolves and what `repromote_batch_job.yml` takes as its `table=` parameter,
so a remedy rendered from this column is still a command that runs.

AND THE PARAMETER MOVED WITH THE COLUMN, which is why this was one change and not two.
`incident_feed_sql` used to take `source=` meaning THE RELATION TO READ FROM. Emitting a
column called `source` out of a signature whose `source` means something else would have
reproduced, inside one function, the exact ambiguity the rename exists to remove -- so the
parameter is `view=` and the local it resolves into is `relation`.
(`freshness.py`'s `last_source_date`, `source_age_days` and `source_freshness_status` are
about the upstream publisher's axis and are a different word: the BARE column `source` means
the registry key in both F4 views, and now in three.)

THE MAPPING FORK, AND WHICH SIDE THIS FILE TOOK. Every bronze job YAML declares its
`fail_on_dq` task's table as a literal parameter, so the job-name-to-table mapping is
EXACT and not a heuristic. Two ways to have it here:

  1. **Derive it from the YAMLs at runtime.** REFUSED, and not on taste: the wheel is
     `[tool.hatch.build.targets.wheel] packages = ["src/opl"]`, so `databricks/resources/`
     is not in the artefact at all. `bundle deploy` syncs those files to the workspace,
     but nothing puts them on the running task's import path, and a module that read them
     would work in this repository and raise `FileNotFoundError` in the only place it
     matters. Reading a bundle YAML from inside the wheel is `opl.bronze.provenance`'s
     mistake in another costume -- asking the runtime for a fact that only the build host
     has.
  2. **Declare it as data, and lock the declaration with a test.** TAKEN. This is exactly
     `opl.dataops.cadence`'s pattern and its docstring argues the general case: a number
     or a name a human typed is strictly better than the same value typed into a dashboard,
     FOR ONE REASON -- here it is in the diff. What makes it safe rather than merely
     honest is that it cannot go stale silently: `tests/triage_agent/test_incidents_
     declaration.py::test_every_job_that_runs_the_dq_gate_declares_the_table_its_yaml_
     hands_it` reads every
     job YAML in the bundle, extracts each `fail_on_dq` task's parameter, and holds the
     result EQUAL to `TABLE_OF_JOB` in both directions. A new bronze job, a renamed job, or
     a job repointed at another table's pipeline all fail that test in the commit that does
     it.

A THIRD OPTION WAS CONSIDERED AND IS THE ONE THAT LOOKS CLEVEREST: derive the job name
from the registry, since THREE of the seven are `opl-bronze-<key>` (merchant, payments,
ptax) and the four CNPJ tables are `opl-bronze-cnpj-<key>`. REFUSED HARDEST. That is a
correlate of the mapping, not the mapping: the authority on a job's name is the `name:` in
its YAML, and the day somebody renames a job the derivation keeps answering confidently
and wrongly. `fail_on_dq`'s own
docstring records what a second spelling of a table name has already cost this project --
a hardcoded quarantine name "sent two real Estabelecimentos runs to the lookup
quarantine". A convention is a second spelling that has not been written down.

AN UNKNOWN JOB IS REPORTED WITH A NULL `source`, NEVER DROPPED. `element_at` on a map that
has no such key returns NULL, and so does the whole expression when `job_name` is itself
NULL -- which happens for real: `telemetry.py` keeps task runs whose job has aged out of
`system.lakeflow.jobs` (three of them in this workspace) precisely so they are not deleted
from the record. A DQ incident on a job this file does not know is the exact shape of a
stale declaration, and it has to be visible in the output rather than filtered out of it.

WHAT IS REFUSED HERE, AND IT WOULD HAVE LOOKED LIKE DILIGENCE: a cross-check that
`attempts` equals `MAX(attempt)`. It cannot fail. The source view computes `attempt` as
`ROW_NUMBER() OVER (PARTITION BY job_run_id, task_key ORDER BY started_at)` and this query
groups by that same partition, so the numbers are 1..n over exactly the rows being counted
and the two aggregates are equal by construction, on every input, forever. A check whose
output cannot distinguish "passed" from "never ran" is this repository's most-hunted
species; adding one here to guard against the very trap this file is about would have been
the joke version of it.

`result_states` IS SORTED AND THEREFORE NOT IN ATTEMPT ORDER, WHICH IS A LOSS WRITTEN DOWN
RATHER THAN A PROPERTY CLAIMED. `SORT_ARRAY` is there because Spark documents
`COLLECT_LIST` as non-deterministic -- its order follows the row order after a shuffle --
so the choice is between a sorted array and a column no assertion can pin. The price of
sorting is that an incident whose attempts DISAGREE shows THAT they disagree and not WHICH
attempt was which; recovering the pairing needs the source view, which publishes one row
per attempt on purpose. Every incident in this corpus is `['FAILED', 'FAILED']`, so the
price is unpaid today and is recorded here for the day it is not.

TWO NULL-SHAPED FACTS THE COLUMNS CARRY, WRITTEN DOWN BECAUSE NEITHER IS AN ERROR:
`MAX(ended_at)` skips NULLs, so `last_ended_at` is the end of the last attempt that
FINISHED, and it is NULL only when no attempt has. `COLLECT_LIST` skips NULLs too, so
`size(result_states) < attempts` means at least one attempt has no terminal state recorded
yet -- which is a fact about the timeline's ingestion, is recoverable from the two columns
side by side, and is why `attempts` is `COUNT(*)` rather than the array's length.
"""
from __future__ import annotations

from opl.bronze.registry import REGISTRY
from opl.config import DEFAULT, OplConfig
from opl.dataops.telemetry import TASK_TELEMETRY_VIEW

# The task key whose run IS the incident. One spelling, here: `databricks/src/fail_on_dq.py`
# is the entry point, `task_key: fail_on_dq` is what every bronze job YAML declares, and
# the test that reads those YAMLs uses this constant to find the task -- so a rename that
# reached the bundle and not this file fails that test rather than emptying the feed.
#
# PINNING ONE SPELLING IS A MEASURED RISK IN THIS WORKSPACE, NOT A NEUTRAL CHOICE: see the
# header's "ONE TASK KEY IS PINNED" for the `dq_gate` -> `dq_gate_batch` retirement, for the
# census that establishes `fail_on_dq` has no predecessor, and for what the lock below does
# and does not buy.
DQ_GATE_TASK_KEY = "fail_on_dq"

# Any LEADING BRACKETED TOKEN and the whitespace after it. `databricks.yml`'s only target
# is `mode: development`, which names deployed jobs `[dev <operator>] <name>`.
#
# MATCHES A SHAPE, NEVER A NAME, and that is the requirement rather than a nicety: the
# operator's username is in that prefix and CLAUDE.md forbids committing one. It also
# means the feed keeps working for the next operator and under a target that adds no
# prefix at all, where the pattern simply matches nothing.
#
# The doubled backslashes are the SQL string literal's escaping, not the regex's: Spark's
# parser consumes one level, so what the matcher receives is `^\[[^\]]*\]\s*`.
_BUNDLE_PREFIX = r"^\\[[^\\]]*\\]\\s*"

# Which job runs the DQ gate for which registered table. DECLARED, and locked against the
# bundle by `tests/triage_agent/test_incidents_declaration.py` -- see the header's
# "THE MAPPING FORK"
# for why this is data here rather than a read of the YAMLs, and what was rejected.
#
# The KEY is the job's `name:` as the YAML declares it, WITHOUT the dev prefix the runtime
# adds; the VALUE is the `fail_on_dq` task's own parameter, which is a `REGISTRY` key and
# therefore what `table_spec` resolves. Neither side is retyped from the other: the test
# reads both out of the same file.
TABLE_OF_JOB: dict[str, str] = {
    "opl-bronze-cnpj-empresas": "empresas",
    "opl-bronze-cnpj-estabelecimentos": "estabelecimentos",
    "opl-bronze-cnpj-lookup": "lookup",
    "opl-bronze-cnpj-socios": "socios",
    "opl-bronze-merchant": "merchant",
    "opl-bronze-payments": "payments",
    "opl-bronze-ptax": "ptax",
}


def stripped_job_name_sql(column: str = "job_name") -> str:
    """`column` with the bundle's dev prefix removed, as one SQL expression.

    Spelled once and used twice in the feed -- to label the row and to look the table up
    -- because two `regexp_replace` calls with two copies of the pattern is the shape
    where one of them gets fixed."""
    return f"regexp_replace({column}, '{_BUNDLE_PREFIX}', '')"


def table_of_job_sql(job_name: str = "job_name") -> str:
    """The declaration as a SQL map lookup over an already-stripped job name.

    A MAP AND NOT A CASE LADDER: the arms of a ladder are ordered and can overlap, and
    neither property is wanted here -- this is a lookup with one answer or none.

    `sorted` PUTS THE MAP IN NAME ORDER RATHER THAN IN THE ORDER SOMEBODY TYPED THE
    LITERAL, so re-sorting the declaration is not a diff in generated SQL. NO TEST DEPENDS
    ON IT and the sentence that said one did was wrong: `dict` is insertion-ordered, so
    this was already deterministic across runs without `sorted`, and the only comparison
    anywhere calls this same function on both sides."""
    pairs = ", ".join(f"'{job}', '{table}'" for job, table in sorted(TABLE_OF_JOB.items()))
    return f"element_at(map({pairs}), {job_name})"


def incident_feed_sql(view: str | None = None, config: OplConfig = DEFAULT) -> str:
    """Every DQ incident this workspace holds, one row each. The query, spelled once.

    `view` IS THE SEAM A TEST NEEDS, and it is the only reason this parameter exists --
    the same reason `telemetry.SystemTables` exists and the same reason its docstring
    gives: the alternative is a query whose behaviour is asserted by nobody until a
    workspace run, after a deploy, after a commit. It defaults to the view this project
    deploys, which `config` locates. It is NOT called `source`, and the header says why:
    that word is the emitted registry-key column, here and in both F4 views.

    THERE IS NO IDENTICAL PYTHON LADDER BESIDE THIS. `opl.bronze.reconcile` states the rule
    -- the verdict is spelled once, in SQL, because this repository has paid for two
    spellings of a sentinel, of a month and of a prefix. The grouping IS the logic here, so
    a Python re-fold of the same rows would be that mistake in its purest form.

    THE `GROUP BY` CARRIES `job_id`, `job_run_id` AND `job_name` RATHER THAN `MAX()`-ING
    THEM, which is `telemetry.py`'s choice and is made here for its reason: they cannot
    vary within one job run, so the two spellings are equivalent today and differ in how
    they FAIL. `MAX()` would label one row with the larger of two disagreeing values;
    grouping emits two rows and breaks the one-record-per-`job_run_id` property a reader
    can check. A loud wrong answer is the one this project keeps choosing.

    `task_key` is grouped and published even though the WHERE pins it to one value: it is
    what makes the record self-describing when a consumer has lost the query."""
    relation = view or config.table(TASK_TELEMETRY_VIEW)
    return (
        "WITH gate_runs AS (\n"
        "  SELECT job_run_id, job_id, task_key, started_at, ended_at, result_state,\n"
        f"    {stripped_job_name_sql()} AS job_name\n"
        f"  FROM {relation}\n"
        f"  WHERE task_key = '{DQ_GATE_TASK_KEY}'\n"
        ")\n"
        "SELECT job_run_id,\n"
        "  CAST(job_run_id AS STRING) AS batch_id,\n"
        f"  {table_of_job_sql()} AS source,\n"
        "  job_name, job_id, task_key,\n"
        "  COUNT(*) AS attempts,\n"
        "  MIN(started_at) AS first_started_at,\n"
        "  MAX(ended_at) AS last_ended_at,\n"
        "  SORT_ARRAY(COLLECT_LIST(result_state)) AS result_states\n"
        "FROM gate_runs\n"
        "GROUP BY job_run_id, job_id, job_name, task_key"
    )


def _assert_the_declared_jobs_cover_exactly_the_registered_tables() -> None:
    """TOTAL over `REGISTRY`, in both directions, and refused at import.

    The half of the declaration that can be checked from INSIDE the wheel is checked
    there. A table registered with no DQ-gate job here would fall out of the feed
    silently -- gated in the workspace, absent from triage -- which is the shape that left
    `reclaim_landing` wired into four jobs and missing from the fifth path. The other half
    (that these job names and tables are what the bundle actually declares) needs the
    YAMLs, which the wheel does not carry, and is the test's."""
    declared, registered = set(TABLE_OF_JOB.values()), set(REGISTRY)
    if declared != registered:
        raise ValueError(
            f"the DQ-gate job declaration is not total over the bronze registry: "
            f"registered tables with no job {sorted(registered - declared)}, jobs naming "
            f"an unregistered table {sorted(declared - registered)}. A table on the first "
            "list is gated in the workspace and invisible to triage"
        )


def _assert_no_two_jobs_claim_the_same_table() -> None:
    """One table, one job, and this is the direction the registry cannot see.

    Totality above is satisfied by a mapping in which two jobs both name `lookup` and one
    table is therefore missing -- no, it is not: the sets would differ. What totality does
    NOT catch is the day an eighth job is added naming a table another job already owns,
    because the VALUE set is unchanged. Then two `job_run_id`s resolve to one quarantine
    and a triager is sent to a table full of another pipeline's rows, which is the exact
    production defect `fail_on_dq`'s docstring records."""
    claimed: dict[str, list[str]] = {}
    for job, table in sorted(TABLE_OF_JOB.items()):
        claimed.setdefault(table, []).append(job)
    shared = {table: jobs for table, jobs in claimed.items() if len(jobs) > 1}
    if shared:
        raise ValueError(
            f"two jobs claim one table: {shared}. Each job run resolves to exactly one "
            "quarantine, and a table claimed twice sends a triager to another pipeline's "
            "rejected rows -- the drift that once sent Estabelecimentos runs to the lookup "
            "quarantine"
        )


_assert_the_declared_jobs_cover_exactly_the_registered_tables()
_assert_no_two_jobs_claim_the_same_table()
