"""What refuses a run at LAUNCH: the deployed-revision guard, and the two job
parameters whose defaults can only fail.

SPLIT OUT OF `test_job_yaml_wiring.py` BY F-API TASK 2, which stood at 781 of this
project's 800-line limit before this phase's job was added. Protocol §4.9/§4.12 say
whoever touches a file at the cap splits it first, and the seam is the one that
file's own body had already drawn with a section header rather than a line count:
everything left there asks WHICH TABLE a job hands its tasks, and everything here
asks what stops a run that nobody parameterised, or that runs code nobody deployed.
Two reasons to edit are two files -- that half changes when a JOB is added, this one
changes when a LAUNCH-TIME GUARD is added or a job parameter gains a sentinel.

WHY ANY OF THIS IS A YAML LOCK AND NOT ONLY THE TASK'S OWN UNIT TEST: the guard is a
task like any other, so every way a job YAML can be wrong about a task applies to it
-- absent, present but waiting on the unzip, handed the wrong parameter, or sitting
in a second environment that installs a different wheel than the work does. None of
those fail a run; the first three make the run green with the guard doing nothing
useful, and the fourth makes it green having verified a wheel no task uses. CI
validates the repository and never what is deployed, which is the whole reason the
guard exists -- so the guard's own wiring is the one thing that had better not depend
on somebody having looked.

THE `month` DEFAULT IS HERE TOO, AND IT IS NOT A REVISION GUARD. It sat under that
section header in the unsplit file, and it belongs on this side of the seam for the
property it shares rather than the subject: a job-parameter default cannot validate
anything, so its whole job is to be a value the code refuses. `revision`, `month` and
-- since F5 T8 -- `minimum_rows` are the THREE whose default is LOCKED against the
constant the code names, and the first two were a real object name or a real month once.

THE THIRD IS THE ONE THAT WAS NEVER A REAL VALUE, and it is here because a plausible
default would have been the measurement. `streaming_managed_broker_job.yml`'s
`minimum_rows` is the floor its run's entire product depends on: that job reads a broker
that stops answering in days, has no downstream test stating an exact count, and prints a
number that goes into an evidence document. A default of `1` would parse, would pass, and
would let a run that consumed one record of forty thousand report SUCCESS with a number
that looks exactly like the one that was meant.

THE TWO THAT ARE NOT, NAMED SO THE SENTENCE ABOVE IS NOT READ AS "THE ONLY THREE
SENTINELS". `bronze_payments_job.yml`'s `profile` defaults to
`opl.generator.profiles.SENTINEL_PROFILE` and `repromote_batch_job.yml`'s `batch_id` to
`opl.bronze.promote.SENTINEL_BATCH_ID`; both are refused by the code that reads them,
and NEITHER default is compared against its constant by any test in this repository. So
a YAML-side drift into a value nobody checks is still open for those two -- a smaller
hole than the same one for `month` (a repromote of the wrong batch is loud; a `profile`
that drifted lands another stream's bytes) and a real one. Recorded rather than closed:
it predates F-API, and F-API's own attempt to add a third pair of sentinels here is what
made it visible -- `first`/`last` were added to the PTAX job with a sentinel argued from
a lock in THIS file that nobody had written, and they are gone (the window is declared in
`opl.extraction.ptax_window`) rather than locked.

The readers live in `tests/job_yaml.py` -- extracted rather than copied, because this
seam cuts through every one of them. See that file."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from job_yaml import (
    JOB_OF,
    RESOURCES,
    REVISION_GUARD,
    ancestors,
    job_of,
    mutated,
    resource_files,
    script_of,
    tasks_of,
)

from opl.bronze.provenance import SENTINEL_REVISION, is_object_name
from opl.config import SENTINEL_MONTH, is_month
from opl.streaming.managed_broker import SENTINEL_MINIMUM_ROWS

# WHICH JOBS REFUSE A RUN BUILT FROM AN UNEXPECTED REVISION, and which one deliberately
# does not. Every YAML under `databricks/resources` must appear in one of these two, and
# `test_every_yaml_under_resources_is_classified` asserts it (together with the third
# list, `_NON_JOB_RESOURCES`, for files that declare no job at all) -- `JOB_OF`
# covers the ingestion jobs only, so a lock driven off it alone would say nothing about
# the jobs that ingest nothing, and "a job missing the guard fails" would quietly not
# hold for exactly the files nobody thinks about.
_GUARDED_JOBS = (
    "bronze_job.yml",
    "bronze_empresas_job.yml",
    "bronze_estabelecimentos_job.yml",
    "bronze_socios_job.yml",
    # The payments job, and the answer to this list's question is stronger for it than
    # for any ingestion job above. Those jobs move bytes somebody else produced, so a
    # wheel from another revision reads the same files with different code. This job's
    # bytes ARE the deployed wheel's output: `opl.generator` derives them, and a stale
    # wheel lands a DIFFERENT stream under the same filename -- after which every
    # pinned digest describes code nobody reviewed, and `emit_stream_file` refuses the
    # correct stream ever afterwards because a different file already sits there.
    "bronze_payments_job.yml",
    # The PTAX job, and this list's question has an answer here that no other entry has:
    # it is the first job in this repository whose tasks reach OUTSIDE the workspace. A
    # wheel from another revision could ask a different endpoint, or read the response
    # against a different contract -- and then land it under a filename derived from the
    # window, which `emit_records_file` refuses to overwrite ever afterwards. So a wrong
    # wheel does not merely write wrong rows here; it takes the name the correct rows
    # would have to be written under, and the repair is deleting a file from the Volume
    # by hand.
    "bronze_ptax_job.yml",
    # The merchant job, and this list's question has yet another answer here: its input was
    # produced by a DIFFERENT ARTEFACT ON A DIFFERENT MACHINE. Every other job in this list
    # either reads bytes somebody else published or writes its own with the same wheel that
    # then reads them. Here a host-side extractor built the file, and the wheel that ingests
    # it can be from another revision without anything in the workspace noticing -- it would
    # read a landed snapshot against a different contract, stamp `_snapshot_ref_date` with a
    # different derivation, and append the result to the table the vault's first end-dating
    # is measured from.
    "bronze_merchant_job.yml",
    # The operator job, and its inclusion is a decision rather than completeness: a
    # repromote APPENDS TO BRONZE, the system of record, re-applying whatever DQ rules
    # the deployed wheel happens to carry. Run against a stale wheel it appends rows
    # the current rules reject. Its header's isolation argument is about what STARTS
    # the job, not about which code the job then runs (ADR 0009).
    "repromote_batch_job.yml",
    # THE FOUR VAULT JOBS (F2 wave 1's workspace run), and the choice is the same one
    # every job in this list has to answer: does a run against a wheel built from
    # another revision matter? It matters MORE here than for an ingestion job. A vault
    # loader appends into a table whose identity is a HASH KEY, so a wheel carrying a
    # different `opl.vault.hashing_spark` -- one whitespace rule, one padding width, one
    # component order -- writes rows that are individually well-formed and key to
    # nothing the previous load wrote. Nothing fails, every join returns fewer rows, and
    # the repair is deleting rows by hand because the loaders are insert-only.
    "vault_empresa_job.yml",
    "vault_estabelecimento_job.yml",
    "vault_partner_job.yml",
    "vault_reference_job.yml",
    # THE FIRST GOLD JOB (F3 Task 1), and this list's question has a sharper answer for
    # it than for any entry above. An ingestion job moves bytes somebody else produced
    # and a vault load appends rows keyed on a digest; a gold build's ENTIRE OUTPUT is a
    # function of the deployed code -- the surrogate key is a hash whose input order is
    # one line of `opl.gold.dimensions`, and both interval bounds are sentinels declared
    # in `opl.gold.columns`. A wheel from another revision writes a dimension that is
    # individually well-formed, that a fact built from this revision joins to
    # incorrectly, and that reports success; and because the loader is append-only and
    # refuses a target it did not write in the same run, the repair is dropping a
    # 69.2M-row table by hand rather than re-running.
    "gold_dim_company_job.yml",
    # THE SECOND GOLD JOB (F3 Task 3), and the answer is the same one with nothing
    # softened by the tables being small. These three dimensions have no upstream at all
    # in the sense the four ingestion jobs do: `dim_channel`'s members ARE
    # `opl.contracts.payments.PAYMENT_METHODS`, its keys are `xxhash64` over them, and
    # `dim_date`'s span is derived by one function in `opl.gold.conformed`. Every row is
    # a function of the deployed wheel, so a wheel from another revision writes a
    # conformed dimension that is individually well-formed and that a fact built from
    # this revision joins to incorrectly -- with both runs reporting success.
    "gold_conformed_dimensions_job.yml",
    # THE THIRD GOLD JOB (F3 Task 2), and this list's question gains a COST dimension
    # here that no entry above has. `pit_estabelecimento` is a ~144M-row append whose
    # every column is a function of the deployed wheel: the as-of set is derived by one
    # function in `opl.gold.pit`, the pointer semantics are one window frame, and the
    # pointer COLUMN NAMES are derived from the satellite names in `opl.gold.specs`. A
    # wheel from another revision writes a table that is individually well-formed and
    # that every as-of read misses -- and because the loader is append-only, the repair
    # is dropping ~144M rows by hand rather than re-running.
    "gold_pit_estabelecimento_job.yml",
    # THE FOURTH GOLD JOB (F3 Task 4), and this list's question has its sharpest answer
    # here because this table's columns are a function of code deployed EARLIER as well as
    # of the wheel that builds it. Its two role keys ARE `dim_company`'s surrogate keys --
    # `xxhash64` over a business key and an `applied_date` -- and its three conformed keys
    # are DERIVED by `opl.gold.conformed` rather than looked up, so a wheel whose key
    # mechanism differs by one line writes a fact that is individually well-formed, joins
    # to nothing at all, and reports success. Every other job in this list writes rows that
    # are wrong; this one writes rows that are unreachable.
    "gold_fact_payment_job.yml",
    # The merchant vault job (F-DB Task 5), and this list's question has an answer here
    # that no other vault entry has: it is the only job in this repository that can write
    # a CLOSING row, and a close is a DERIVED claim -- the source never told us the
    # relationship ended, we inferred it from an absence. `sat_eff_merchant_empresa` is
    # insert-only like every other table here, so a wheel from another revision that
    # closed a window on a different condition leaves a row asserting a relationship
    # ended, on a table whose repair is deleting rows by hand. The other vault jobs write
    # keys and payloads that a later correct load re-derives; this one writes an
    # inference about something that is no longer there to re-read.
    "vault_merchant_job.yml",
    # F4 Task 1's views job, and this list's question has an answer here that no entry
    # above has, because this is the first guarded job that WRITES NOTHING. Every other
    # entry argues from rows: a wheel from another revision appends the wrong ones, and
    # the repair is deleting them by hand. This job issues `CREATE OR REPLACE VIEW` and
    # touches no row at all -- which makes the same question sharper rather than moot.
    # The view's DEFINITION is the deployed wheel's output: `opl.bronze.reconcile`
    # derives the SQL, the verdict ladder and the remedy string from `REGISTRY`, and OR
    # REPLACE takes the NAME the correct definition would have had. So a stale wheel
    # leaves a reconciliation that is individually well-formed, that classifies batches
    # by a ladder nobody reviewed, and that an operator then reads off a dashboard
    # believing it is the reviewed one. Nothing fails, nothing looks wrong, and the
    # artefact that would say otherwise is the thing that was overwritten.
    "dataops_views_job.yml",
    # F5 T8's managed-broker read, and this list's question has an answer here that is not
    # about the rows -- though the row argument holds too. What a stale wheel writes is a
    # DIFFERENT PROJECTION of the same Kafka records: the column names, the parse and the
    # kept raw value are all `opl.streaming.ingest`'s, and the sink is append-only behind a
    # checkpoint, so the repair is dropping the table and deleting the checkpoint by hand.
    # The sharper answer is that THIS JOB'S OUTPUT IS THE PRODUCT. It writes to a table
    # nothing else reads, deliberately unregistered; what leaves the run is a ROW COUNT
    # quoted in `docs/f5-run-evidence.md` as what this lakehouse read from a real broker on
    # the platform it deploys to. A count produced by a wheel nobody reviewed is a claim
    # about code that is not in this repository, published under this repository's name --
    # and unlike a wrong row, there is nothing left afterwards that could be re-read to
    # find out.
    "streaming_managed_broker_job.yml",
    # F6 T8's workspace run, and it is the SECOND guarded job that writes nothing -- so
    # the row argument every entry above leans on is unavailable here twice over. The
    # views job at least leaves a view behind that a later reader could re-read; this one
    # leaves a LOG. Its product is a set of GRADES -- a severity, a recommended action, a
    # comparison reading and a blast radius -- and every one of them is a function of a
    # ladder, a declaration and a manifest that live in the wheel and in nothing else. A
    # wheel from another revision emits a payload naming real batch ids and real reject
    # counts, carrying verdicts nobody reviewed, which then becomes a public GitHub issue
    # and the numbers in this phase's evidence document. That is
    # `streaming_managed_broker_job.yml`'s answer -- the output IS the product, and there
    # is nothing left afterwards to re-read -- with the additional property that a wrong
    # grade is not a wrong measurement of the workspace but a wrong JUDGEMENT of it,
    # published under this repository's name.
    "triage_job.yml",
)

_UNGUARDED_JOBS = {
    # The one exclusion, and the reason is that a guard here would remove a diagnostic
    # in the exact case it exists for: smoke's entire purpose is to answer "does the
    # deployed wheel import and can it read config", i.e. it is what you run WHEN YOU
    # SUSPECT THE DEPLOYMENT. It writes nothing, so a wrong revision costs a re-run.
    # It reports the deployed revision instead of refusing on it, which
    # tests/test_assert_deployed_revision_task.py pins from the script side.
    "smoke_job.yml": "the probe you run when the deployment itself is in doubt",
}

# THE THIRD CATEGORY, ADDED BY F4 TASK 4, AND IT IS NOT A THIRD ANSWER TO THIS FILE'S
# QUESTION -- IT IS A FILE THE QUESTION DOES NOT REACH.
#
# Both lists above ask "does a run of this job against a wheel built from another revision
# matter?" That question presupposes a run, and a run presupposes tasks. `databricks/
# resources/` was jobs-only until F4 declared a Dashboard resource there, and the
# classification was TOTAL over `*.yml` -- so the first non-job resource turned this file
# red with a message about an unclassified JOB, which is not what had happened.
#
# THE ALTERNATIVE WAS TO SCOPE THE GLOB TO FILES CONTAINING `resources.jobs`, AND IT WAS
# REJECTED. A glob that skips what it does not recognise reports green over exactly the
# file nobody classified -- which is the failure mode the exactness of these lists exists
# to prevent, and the reason `_UNGUARDED_JOBS` is a dict with a stated reason rather than
# an omission. A named third list keeps the "every YAML is accounted for" property whole
# and forces a sentence about anything new.
#
# WHAT A NON-JOB RESOURCE STILL HAS TO ANSWER lives beside it, not here: a dashboard runs
# no wheel, so the revision guard has nothing to guard, and what it CAN be wrong about --
# a committed warehouse id, a round-tripped JSON, embedded credentials -- is asserted in
# `tests/dataops/test_dashboard.py`.
_NON_JOB_RESOURCES = {
    "dataops_dashboard.yml": (
        "a Lakeview dashboard: no tasks, no wheel, so there is no run for a revision "
        "guard to stand in front of"
    ),
}

# THE RESOURCE KINDS THAT RUN NO CODE OF THIS PROJECT'S, WHICH IS THE PROPERTY THE THIRD
# LIST ACTUALLY DEPENDS ON. The first spelling of `test_the_non_job_resources_declare_no
# _job_at_all` asked only for the absence of a `jobs` key, and that is narrower than the
# question: a DAB `pipelines:` resource runs a wheel, declares no `jobs` key at all, and
# would therefore have been filed here and never asked whether a run of it against another
# revision's wheel matters. Nothing like that exists in this bundle today, which is exactly
# when the vocabulary is cheap to widen. An ALLOW-LIST rather than a deny-list, for the
# reason the glob was not narrowed: a check that skips what it does not recognise reports
# green over the one resource nobody classified.
_RESOURCE_KINDS_THAT_RUN_NOTHING = frozenset({"dashboards"})


def test_every_yaml_under_resources_is_classified():
    """The classification is TOTAL over the bundle documents in `databricks/resources`.

    THE SUFFIX SET IS `job_yaml`'s ONE TUPLE AND NOT A GLOB OF THIS MODULE'S OWN. It was
    `*.yml` here while `bundle_docs()` had learned more, and the gap was reachable rather
    than theoretical: a scheduled, unclassified `zz_probe_job.yaml` planted under
    `databricks/resources/` left this module reporting a full green underneath a docstring
    that said TOTAL. THE NAME STILL SAYS `yaml` because it is cited by name from outside
    this file -- `git grep -n test_every_yaml_under_resources_is_classified` -- while what
    is classified is every suffix `BUNDLE_DOC_SUFFIXES` carries.

    A new job YAML must be added to one of the two job lists, and the choice is the
    point: "does a run of this job against a wheel built from another revision matter?"
    has an answer for every job, and the answer for the ingestion jobs and the repromote
    is yes. Left to a glob, a job added later would inherit whichever behaviour the glob
    happened to give it.

    A YAML that declares no job at all goes in the third list instead, with the reason
    written out -- see the comment above `_NON_JOB_RESOURCES` for why the glob is not
    narrowed to job files instead."""
    declared = set(_GUARDED_JOBS) | set(_UNGUARDED_JOBS) | set(_NON_JOB_RESOURCES)
    present = {path.name for path in resource_files()}
    assert declared == present, (
        f"unclassified YAML(s) under databricks/resources: {sorted(present - declared)}; "
        f"classified but absent: {sorted(declared - present)}"
    )
    assert set(JOB_OF.values()) <= set(_GUARDED_JOBS), (
        "an ingestion job is not guarded -- these are the jobs that move GB and append "
        f"to bronze: {sorted(set(JOB_OF.values()) - set(_GUARDED_JOBS))}"
    )


def test_the_non_job_resources_declare_nothing_that_runs_this_projects_code():
    """THE THIRD LIST IS NOT A PLACE TO PUT THINGS, and this is what makes that true.

    Without this, a resource that runs a wheel and is filed under `_NON_JOB_RESOURCES`
    passes the totality check and is never asked for a revision guard -- so the third
    category, added to keep an exact classification honest, would become the way around it.

    THE QUESTION IS NOT "IS THERE A `jobs` KEY". That was the first spelling and it is a
    proxy: a DAB `pipelines:` resource runs a wheel and declares no `jobs` key, so it would
    have passed. The property is that every resource kind in the file is one that runs no
    code of this project's, and the allow-list is where a new kind has to argue for itself
    -- a resource kind nobody has thought about fails here rather than being waved
    through."""
    for resource_yml, why in _NON_JOB_RESOURCES.items():
        document = yaml.safe_load((RESOURCES / resource_yml).read_text(encoding="utf-8"))
        kinds = set(document.get("resources", {}))
        runners = sorted(kinds - _RESOURCE_KINDS_THAT_RUN_NOTHING)
        assert not runners, (
            f"{resource_yml} declares resource kind(s) {runners} but is filed as a non-job "
            f"resource ({why}), so nothing asks whether a run of it against a wheel from "
            "another revision matters. Move it to _GUARDED_JOBS or _UNGUARDED_JOBS, or add "
            f"the kind to _RESOURCE_KINDS_THAT_RUN_NOTHING with the argument for why it "
            "cannot run this project's code"
        )


def _assert_the_revision_guard_precedes_every_other_task(
    job_yml: str, root: Path = RESOURCES
) -> None:
    tasks = tasks_of(job_yml, root)
    assert REVISION_GUARD in tasks, (
        f"{job_yml} has no {REVISION_GUARD} task, so a run of it against a wheel built "
        "from another revision succeeds -- which is what happened on 2026-08-01, when a "
        "socios re-run reported SUCCESS having masked only bronze because the workspace "
        "was still running a bundle deployed four commits earlier"
    )
    guard = tasks[REVISION_GUARD]
    assert not guard.get("depends_on"), (
        f"{job_yml}: the guard waits on something, so the run has already done that "
        "something by the time it learns it is running the wrong code. TRAP 2 -- a check "
        "after the unzip reports the problem once several GB have moved"
    )
    assert script_of(guard, f"{job_yml}:{REVISION_GUARD}") == REVISION_GUARD
    assert guard.get("max_retries") == 0, f"{job_yml}: the guard does not declare max_retries: 0"
    for key in tasks:
        if key == REVISION_GUARD:
            continue
        assert REVISION_GUARD in ancestors(tasks, key), (
            f"{job_yml}:{key} can start before {REVISION_GUARD} has finished, so the "
            "guard no longer stands between a wrong deployment and the work"
        )


@pytest.mark.parametrize("job_yml", _GUARDED_JOBS)
def test_the_revision_guard_runs_first_and_everything_else_waits_for_it(job_yml):
    _assert_the_revision_guard_precedes_every_other_task(job_yml)


def _assert_the_revision_default_cannot_pass(job_yml: str, root: Path = RESOURCES) -> None:
    parameters = {
        parameter["name"]: parameter.get("default")
        for parameter in job_of(job_yml, root).get("parameters", [])
    }
    assert "revision" in parameters, (
        f"{job_yml} declares no `revision` job parameter, so there is nothing for "
        "--params revision=... to reach and the guard has no expected value"
    )
    default = parameters["revision"]
    assert not is_object_name(default), (
        f"{job_yml}'s revision default is {default!r}, which the guard ACCEPTS as a whole "
        "object name -- so a run launched without --params revision=... would pass the "
        "check against a revision nobody chose. A job-parameter default cannot validate "
        "anything; it can only be a value that refuses"
    )
    assert default == SENTINEL_REVISION, (
        f"{job_yml}'s revision default is {default!r} rather than the sentinel the code "
        f"names ({SENTINEL_REVISION!r}). Two spellings of one sentinel is a default that "
        "drifts into a value nobody checked"
    )


@pytest.mark.parametrize("job_yml", _GUARDED_JOBS)
def test_the_guard_is_handed_the_runs_revision_and_the_default_refuses(job_yml):
    """The parameter, its default, and the one thing the guard is handed.

    `{{job.parameters.revision}}` is the whole carrier: it is what makes the expected
    value arrive from the LAUNCH -- from the operator's own repository at the moment the
    run is submitted -- rather than from anything the deploy stamped. A deploy-time
    value would make expected and actual two names for the same act, and the incident
    this guard exists for is a deploy that never happened (ADR 0009)."""
    _assert_the_revision_default_cannot_pass(job_yml)
    guard = tasks_of(job_yml)[REVISION_GUARD]
    assert guard["spark_python_task"]["parameters"] == ["{{job.parameters.revision}}"], (
        f"{job_yml}: the guard is handed {guard['spark_python_task']['parameters']}, not "
        "the run's revision parameter"
    )


@pytest.mark.parametrize("job_yml", _GUARDED_JOBS)
def test_the_guard_verifies_the_same_wheel_the_work_installs(job_yml):
    """A guard in a second environment verifies a second wheel and proves nothing.

    Each of these jobs declares ONE environment, and every `spark_python_task` runs in
    it -- including the guard. That is what makes "the deployed wheel is the one you
    expect" a statement about the tasks that follow, rather than about an install only
    the guard ever performed."""
    tasks = tasks_of(job_yml)
    keys = {
        task["environment_key"] for task in tasks.values() if "spark_python_task" in task
    }
    assert keys == {tasks[REVISION_GUARD]["environment_key"]}, (
        f"{job_yml} runs its tasks in {sorted(keys)}: the guard shares an environment "
        "with only some of the work, so the rest installs a wheel it never checked"
    )


def test_the_unguarded_job_carries_neither_the_guard_nor_a_revision_parameter():
    """The exclusion, ASSERTED. An exclusion left to be inferred from absence is
    indistinguishable from the guard having been forgotten -- and the other half of this
    split has as its whole subject what a copied YAML forgets. `smoke_job.yml` is
    excluded because it is the probe you run when the deployment itself is in doubt; it
    reports the deployed revision instead (ADR 0009)."""
    for job_yml, why in _UNGUARDED_JOBS.items():
        job = job_of(job_yml)
        assert REVISION_GUARD not in tasks_of(job_yml), (
            f"{job_yml} now carries the guard, but it is listed as unguarded because "
            f"it is {why}. Move it to _GUARDED_JOBS or take the task out"
        )
        names = {parameter["name"] for parameter in job.get("parameters", [])}
        assert "revision" not in names, (
            f"{job_yml} takes a revision parameter that nothing in it checks, which "
            "reads as a guard that is not there"
        )


def test_the_guard_ordering_lock_catches_a_task_that_no_longer_waits_for_it(tmp_path):
    """Proves the lock above can fail. One `depends_on` line, and the unzip runs
    alongside the guard rather than after it -- both tasks still present, run still
    green, several GB moved before anything was verified."""
    root = mutated(
        "bronze_estabelecimentos_job.yml",
        tmp_path,
        f"          depends_on: [{{ task_key: {REVISION_GUARD} }}]\n",
        "",
    )
    with pytest.raises(AssertionError, match="can start before"):
        _assert_the_revision_guard_precedes_every_other_task(
            "bronze_estabelecimentos_job.yml", root=root
        )


def _assert_the_month_default_cannot_pass(job_yml: str, root: Path = RESOURCES) -> None:
    parameters = {
        parameter["name"]: parameter.get("default")
        for parameter in job_of(job_yml, root).get("parameters", [])
    }
    assert "month" in parameters, (
        f"{job_yml} declares no `month` job parameter, so there is nothing for "
        "--params month=... to reach and every task falls back on a month nobody chose"
    )
    default = parameters["month"]
    assert not is_month(default), (
        f"{job_yml}'s month default is {default!r}, which `require_month` ACCEPTS as a "
        "real month -- so a run launched without --params month=... would ingest that "
        "month against an EMPTY month-scoped checkpoint, treat every one of its files "
        "as new, and append the whole month into staging under a fresh _batch_id. "
        "promote.rows_of_batch keys idempotence on _batch_id, so it cannot see the "
        "duplication and would carry it into bronze: nothing fails and the row counts "
        "double. A job-parameter default cannot validate anything; it can only refuse"
    )
    assert default == SENTINEL_MONTH, (
        f"{job_yml}'s month default is {default!r} rather than the sentinel the code "
        f"names ({SENTINEL_MONTH!r}). Two spellings of one sentinel is a default that "
        "drifts into a value nobody checked"
    )


@pytest.mark.parametrize("job_yml", sorted(set(JOB_OF.values())))
def test_the_month_default_refuses_rather_than_naming_a_month_nobody_chose(job_yml):
    """The `month` default, locked for the reason the `revision` default is.

    THIS ONE CHANGED MEANING WITHOUT ITS TEXT CHANGING, which is why it needs a lock
    of its own rather than the operator's memory. `default: "2026-06"` was harmless
    while the Auto Loader checkpoint was keyed on `table_key` alone: an un-parameterised
    launch read 2026-06's landing dir against a checkpoint that already recorded every
    one of those files, so it drained nothing. Month-scoping that state (F1.4b PR B
    Task 5 Step 0) made the same launch find an EMPTY checkpoint and re-ingest a full
    month -- and because `promote.rows_of_batch` keys idempotence on `_batch_id`, the
    fresh batch is invisible to it and the duplicate reaches bronze. The consequence is
    written in `checkpoint_location`'s own docstring; this asserts the YAMLs an operator
    launches from do not walk into it.

    Over the ingestion jobs only, because they are the ones that read a landing dir and
    advance a checkpoint. `repromote_batch_job.yml` and `smoke_job.yml` take no month
    at all -- adding one is what this parametrization would have to be taught about,
    and `JOB_OF` is the list that already knows which jobs ingest."""
    _assert_the_month_default_cannot_pass(job_yml)


def test_the_month_default_lock_catches_the_real_month_it_used_to_carry(tmp_path):
    """Proves the lock above can fail, in the exact value and exact shape of the defect.

    `2026-06` is not an invented mutation: it is what all four of these files carried
    until F1.4b PR B, and it is a fully-promoted month. Restoring it makes every run
    launched without `--params month=...` a second ingest of data bronze already holds
    -- green, with the row counts doubled and nothing in the log naming the month."""
    root = mutated(
        "bronze_estabelecimentos_job.yml",
        tmp_path,
        f'default: "{SENTINEL_MONTH}"',
        'default: "2026-06"',
    )
    with pytest.raises(AssertionError, match="row counts.*double"):
        _assert_the_month_default_cannot_pass(
            "bronze_estabelecimentos_job.yml", root=root
        )


# The one job that declares a row floor at launch, named rather than swept: it is the only
# job in this repository whose product is a COUNT rather than a table other jobs read, and
# a parametrization over `_GUARDED_JOBS` would demand the parameter from twelve jobs that
# have no use for one.
_FLOOR_JOB = "streaming_managed_broker_job.yml"


def _assert_the_minimum_rows_default_cannot_pass(job_yml: str, root: Path = RESOURCES) -> None:
    parameters = {
        parameter["name"]: parameter.get("default")
        for parameter in job_of(job_yml, root).get("parameters", [])
    }
    assert "minimum_rows" in parameters, (
        f"{job_yml} declares no `minimum_rows` job parameter, so there is nothing for "
        "--params minimum_rows=... to reach and the task falls back on a floor nobody chose"
    )
    default = parameters["minimum_rows"]
    assert not str(default).strip().isdigit(), (
        f"{job_yml}'s minimum_rows default is {default!r}, which `require_minimum_rows` "
        "ACCEPTS as a floor -- so a run launched without --params minimum_rows=... would "
        "measure against a number nobody chose. This job's whole product is the count it "
        "prints: a floor of one accepts a run that consumed one record of forty thousand, "
        "reports SUCCESS, and puts a number into the evidence document that looks exactly "
        "like the one that was meant"
    )
    assert default == SENTINEL_MINIMUM_ROWS, (
        f"{job_yml}'s minimum_rows default is {default!r} rather than the sentinel the code "
        f"names ({SENTINEL_MINIMUM_ROWS!r}). Two spellings of one sentinel is a default "
        "that drifts into a value nobody checked"
    )


def test_the_minimum_rows_default_refuses_rather_than_naming_a_floor_nobody_chose():
    """The third locked sentinel, and the one that was never a real value.

    `revision` and `month` are locked because each WAS a working value once and stopped
    being one. This is locked before that can happen: the plausible default is `1`, which
    is `write_payment_stream`'s own, and which that function's docstring already says is a
    floor against ZERO and not against a short read."""
    _assert_the_minimum_rows_default_cannot_pass(_FLOOR_JOB)


def test_the_minimum_rows_lock_catches_the_plausible_default_that_would_pass(tmp_path):
    """Proves the lock above can fail, in the value somebody would actually type.

    `1` is not an invented mutation -- it is the shipped default of the function this
    parameter feeds. Restoring it here turns the floor off in the exact case it exists for:
    a run whose read stalled after one record ends GREEN, and the count it prints is the
    only artefact anyone will ever have of what the broker held."""
    root = mutated(
        _FLOOR_JOB, tmp_path, f'default: "{SENTINEL_MINIMUM_ROWS}"', 'default: "1"'
    )
    with pytest.raises(AssertionError, match="one record of forty thousand"):
        _assert_the_minimum_rows_default_cannot_pass(_FLOOR_JOB, root=root)


def test_no_other_job_takes_a_row_floor_nothing_reads():
    """THE OTHER DIRECTION, which the two `revision`/`month` locks each have and this one
    would otherwise lack: a `minimum_rows` parameter on a job whose task does not read one
    reads as a floor that is there, and is not.

    Total over `databricks/resources`, so it also covers the unguarded and non-job files --
    the parameter is cheap to paste. Total means `job_yaml`'s suffix tuple and not this
    module's own glob, for the reason the classification lock above states."""
    for path in resource_files():
        if path.name == _FLOOR_JOB:
            continue
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        for job in document.get("resources", {}).get("jobs", {}).values():
            names = {parameter["name"] for parameter in job.get("parameters", [])}
            assert "minimum_rows" not in names, (
                f"{path.name} takes a minimum_rows parameter that no task in it reads, "
                f"which reads as a floor that is not there. Only {_FLOOR_JOB} declares one"
            )


def test_the_revision_default_lock_catches_a_default_that_would_pass_a_run(tmp_path):
    """Proves the OTHER lock can fail, in the shape that would be silent.

    A `revision:` default that happens to be a real object name turns every run
    launched without `--params revision=...` into a check that passes -- against
    whatever commit was pasted into the YAML, forever. That is the guard reporting
    green while verifying nothing, which is worse than no guard at all."""
    root = mutated(
        "bronze_socios_job.yml",
        tmp_path,
        f'default: "{SENTINEL_REVISION}"',
        'default: "62ce88003113dc1ca198b19cfd00f5f5e20b9bd3"',
    )
    with pytest.raises(AssertionError, match="would pass"):
        _assert_the_revision_default_cannot_pass("bronze_socios_job.yml", root=root)
