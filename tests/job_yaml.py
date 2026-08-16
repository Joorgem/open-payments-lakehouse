# tests/job_yaml.py
"""How a job YAML is READ. No test lives here, and that absence is the point.

THE THIRD FILE OF A TWO-WAY SPLIT, and it exists because this split's seam is not
the previous one's. `test_task_wiring.py` and `test_job_yaml_wiring.py` were split
along a line where nothing was shared -- the AST helpers read scripts and stayed,
every YAML helper went whole -- and both of their docstrings say so. The seam THIS
split runs along, WHICH TABLE A JOB HANDS ITS TASKS versus WHAT REFUSES A RUN AT
LAUNCH, cuts straight through the readers instead: both halves resolve a job, its
tasks, the entry point a task runs and the transitive ancestors of a task, and both
mutate a copy of a YAML to prove their own locks can fail.

SO THE READERS ARE EXTRACTED, NOT COPIED. A copy would be a second spelling of
`resources.jobs` in a repository whose entire subject here is what a copied file
forgets -- and the copy that goes stale is the one no job run ever executes.

NOT IMPORTED FROM THE OTHER TEST MODULE, which was the alternative and is refused
for the reason `test_job_yaml_wiring.py`'s docstring already gave about `_SRC`: a
test module importing another test module would give this suite a collection-order
dependency it does not otherwise have. A plain module under `tests/` has none --
pytest collects nothing from it, because it matches no `python_files` pattern, and
it declares no fixture.

WHAT IS HERE IS ONLY WHAT BOTH HALVES ASK. `JOB_OF` is here because both parametrize
over it; `GUARDED_JOBS` is NOT, because only the launch-guard half reads it, and a
declaration in a file that does not use it is a declaration nobody maintains.
"""
from __future__ import annotations

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "databricks" / "src"
RESOURCES = REPO / "databricks" / "resources"

PYTHON_FILE_PREFIX = "../src/"

# The task key of the deployed-revision guard (ADR 0009). Shared because both halves
# read it: the launch-guard half asserts it runs first everywhere, and the table half
# has to know which task is allowed to precede the masks.
REVISION_GUARD = "assert_deployed_revision"

# Which ingestion-flow job serves which registered table. Total over the registry and
# asserted so by `test_every_registered_table_has_an_ingestion_job`: a table registered
# without a job here is a table nothing ingests, and the next person to copy a job YAML
# would have no list to add to.
JOB_OF = {
    "lookup": "bronze_job.yml",
    "estabelecimentos": "bronze_estabelecimentos_job.yml",
    "empresas": "bronze_empresas_job.yml",
    "socios": "bronze_socios_job.yml",
    # F1b Task 3, and the first entry here whose job GENERATES its own input: payments
    # land as `generated`, so where the four above run `unzip_table.py` this one runs
    # `generate_payments.py`. Every lock is still total over it -- the paste lock, the
    # gate-verdict routing, the month default and the revision guard -- because what
    # changed is which task fills the landing dir, not the flow.
    "payments": "bronze_payments_job.yml",
    # F-API Task 2, and the first entry whose job FETCHES its own input over HTTP: PTAX
    # lands as `api`, so `fetch_ptax.py` sits where `unzip_table.py` sits in the CNPJ
    # jobs and where `generate_payments.py` sits in the payments one. Same flow again,
    # and every lock is still total over it -- only the producer task changes.
    "ptax": "bronze_ptax_job.yml",
    # F-DB Task 4, and the first entry whose job does NOT fill its own landing directory:
    # merchant lands as `postgres`, and the producer is a host-side script that cannot run
    # on Databricks at all (plan T1). So where the four CNPJ jobs run `unzip_table.py`, the
    # payments one `generate_payments.py` and the PTAX one `fetch_ptax.py`, this job's first
    # work task is the INGEST. The sentence this comment block has repeated three times --
    # "only the producer task changes" -- stops being true here: the producer is absent.
    # Every lock is still total over it, because every lock is about the tasks that ARE
    # declared.
    "merchant": "bronze_merchant_job.yml",
}


def job_of(job_yml: str, root: Path = RESOURCES) -> dict:
    """The ONE job declared in `job_yml`.

    One is asserted, not assumed: these files are one-job-per-table deliberately
    -- each header says why, and the reason is that a shared `{{job.run_id}}` is a
    shared `_batch_id` -- so a helper that silently read the first of several would
    be reading a job whose existence already broke the batch model."""
    doc = yaml.safe_load((root / job_yml).read_text(encoding="utf-8"))
    jobs = doc["resources"]["jobs"]
    assert len(jobs) == 1, (
        f"{job_yml} declares {len(jobs)} jobs, expected exactly 1 -- one job per table "
        "is what keeps one _batch_id to one table's ingest"
    )
    return next(iter(jobs.values()))


def tasks_of(job_yml: str, root: Path = RESOURCES) -> dict[str, dict]:
    """The job's tasks, keyed by `task_key`."""
    tasks = job_of(job_yml, root)["tasks"]
    keys = [task["task_key"] for task in tasks]
    assert len(keys) == len(set(keys)), (
        f"{job_yml} declares a task_key twice ({keys}); this lock resolves a key to one "
        "task and would check an arbitrary one of them"
    )
    return {task["task_key"]: task for task in tasks}


def script_of(task: dict, where: str) -> str:
    """The databricks/src entry point a task runs, checked to exist."""
    python_file = task["spark_python_task"]["python_file"]
    assert python_file.startswith(PYTHON_FILE_PREFIX), (
        f"{where} runs {python_file!r}, which is not under {PYTHON_FILE_PREFIX}"
    )
    script = python_file[len(PYTHON_FILE_PREFIX):].removesuffix(".py")
    assert (SRC / f"{script}.py").exists(), (
        f"{where} runs {python_file!r}, which is not a file under databricks/src"
    )
    return script


def ancestors(tasks: dict[str, dict], key: str) -> set[str]:
    """Every task that must have finished before `key` may start.

    Transitive, because the property being asserted is transitive: what matters is
    that no bytes can land before the masks are on, not which task happens to be
    named in one `depends_on`. A dependency on a task the job does not declare is a
    failure here rather than a KeyError at run time."""
    seen: set[str] = set()
    frontier = [key]
    while frontier:
        for dependency in tasks[frontier.pop()].get("depends_on", []):
            name = dependency["task_key"]
            assert name in tasks, (
                f"a task depends on {name!r}, which this job does not declare"
            )
            if name not in seen:
                seen.add(name)
                frontier.append(name)
    return seen


def mutated(job_yml: str, tmp_path: Path, old: str, new: str) -> Path:
    """`job_yml` copied into `tmp_path` with one substring replaced.

    The mutation is asserted to have applied: a probe that silently changed nothing
    proves the lock catches nothing."""
    root = tmp_path / "resources"
    root.mkdir(parents=True, exist_ok=True)
    original = (RESOURCES / job_yml).read_text(encoding="utf-8")
    assert old in original, f"the mutation target {old!r} is not in {job_yml}"
    (root / job_yml).write_text(original.replace(old, new, 1), encoding="utf-8")
    return root
