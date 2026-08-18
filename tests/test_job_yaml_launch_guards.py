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
anything, so its whole job is to be a value the code refuses. `revision` and `month`
are the two whose default is LOCKED against the constant the code names, and both were
a real object name or a real month once.

THE TWO THAT ARE NOT, NAMED SO THE SENTENCE ABOVE IS NOT READ AS "THE ONLY TWO
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
from job_yaml import (
    JOB_OF,
    RESOURCES,
    REVISION_GUARD,
    ancestors,
    job_of,
    mutated,
    script_of,
    tasks_of,
)

from opl.bronze.provenance import SENTINEL_REVISION, is_object_name
from opl.config import SENTINEL_MONTH, is_month

# WHICH JOBS REFUSE A RUN BUILT FROM AN UNEXPECTED REVISION, and which one deliberately
# does not. Every YAML under `databricks/resources` must appear in one of these two, and
# `test_every_job_yaml_is_either_guarded_or_deliberately_not` asserts it -- `JOB_OF`
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


def test_every_job_yaml_is_either_guarded_or_deliberately_not():
    """The classification is TOTAL over `databricks/resources/*.yml`.

    A new job YAML must be added to one of the two lists, and the choice is the point:
    "does a run of this job against a wheel built from another revision matter?" has an
    answer for every job, and the answer for the ingestion jobs and the repromote is
    yes. Left to a glob, a job added later would inherit whichever behaviour the glob
    happened to give it."""
    declared = set(_GUARDED_JOBS) | set(_UNGUARDED_JOBS)
    present = {path.name for path in RESOURCES.glob("*.yml")}
    assert declared == present, (
        f"unclassified job YAML(s): {sorted(present - declared)}; classified but absent: "
        f"{sorted(declared - present)}"
    )
    assert set(JOB_OF.values()) <= set(_GUARDED_JOBS), (
        "an ingestion job is not guarded -- these are the jobs that move GB and append "
        f"to bronze: {sorted(set(JOB_OF.values()) - set(_GUARDED_JOBS))}"
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
