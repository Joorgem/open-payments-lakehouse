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
BUNDLE = REPO / "databricks" / "databricks.yml"

PYTHON_FILE_PREFIX = "../src/"

# THE SUFFIXES A BUNDLE DOCUMENT MAY CARRY, FOR THE SWEEPS IN THIS FILE AND THE MODULES THAT
# IMPORT THEM. Two sweeps that spelled the set for themselves drifted apart once already:
# `bundle_docs` learned `*.yaml` and the two sweeps in
# `tests/test_bundle_targets_and_schedules.py` did not, so a scheduled, unclassified
# `zz_probe_job.yaml` under `databricks/resources/` left both modules green.
#
# OTHER SWEEPS UNDER `tests/` STILL SPELL A SUFFIX FOR THEMSELVES and do not read this tuple.
# Which ones is derived rather than written down, because a written-down set of sites is what
# this phase has published short four times:
#
#     git grep -n -E '\.r?glob\("[a-z_]*\*\.(yml|yaml|json)"\)' -- tests/
#
# The set is the CLI's own, named in its own refusal of anything else -- *"must be YAML or
# JSON files."* -- and JSON is not theoretical: on a scratch bundle `include: resources/*.json`
# validates `exit=0` and renders the job declared in it. This repository writes `.yml`, which
# is exactly why a sweep could stop reading another suffix without anything going red.
BUNDLE_DOC_SUFFIXES = (".yml", ".yaml", ".json")

# THE CLI'S OWN OUTPUT DIRECTORY, EXCLUDED FROM THE BUNDLE-WIDE SWEEP BY DIRECTORY NAME.
# `databricks bundle deploy` writes `.databricks/bundle/<target>/resources.json` -- its
# record of what it deployed, and measured on this box that record CARRIES `pause_status`,
# because rendering it is the CLI's job and that is where its value lives. Reading it would
# make the "no committed bundle file declares `pause_status`" sweep RED on any box that has
# deployed and GREEN in CI, which has no `.databricks/` at all: a local/CI divergence
# pointing the wrong way. Excluded by NAME rather than by consulting git, which keeps
# `bundle_docs`'s deliberate filesystem-not-`git ls-files` property below.
#
# MATCHED AGAINST THE PATH RELATIVE TO THE SWEPT ROOT, not the absolute one: the first
# version read `path.parts`, which also sees the drive and the directories above the
# checkout, so a clone under a directory called `.databricks` emptied the sweep instead of
# narrowing it -- a green that means nothing was read.
CLI_OUTPUT_DIR = ".databricks"

# The task key of the deployed-revision guard (ADR 0009). Shared because both halves
# read it: the launch-guard half asserts it runs first everywhere, and the table half
# has to know which task is allowed to precede the masks.
REVISION_GUARD = "assert_deployed_revision"

# THE THREE KEYS THAT MAKE A JOB START WITHOUT AN OPERATOR. `schedule` is the one this
# repository declares; `trigger` (file arrival) and `continuous` are refused everywhere,
# because a job with a cadence AND a trigger has two and nothing here has argued for
# either. HERE RATHER THAN IN ONE TEST MODULE for this file's own reason: two halves ask
# the same question of the same YAMLs -- `tests/test_bundle_targets_and_schedules.py`
# classifies every job by whether it declares one of these, and `tests/dataops/
# test_cadence.py` asks it of the single job that ingests a table declared PAUSED. A
# second spelling of the tuple is a second spelling of "starts without an operator", and
# the copy that rots is the one nobody reads.
FIRING_KEYS = ("schedule", "trigger", "continuous")

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


def bundle_files(root: Path) -> list[Path]:
    """Files at any depth under `root` carrying a bundle-document suffix, sorted, with the
    CLI's own output directory dropped.

    WHICH SUFFIXES IS NOT THIS FUNCTION'S DECISION and neither is the exclusion:
    `BUNDLE_DOC_SUFFIXES` and `CLI_OUTPUT_DIR` above carry both, with their reasons, and
    `resource_files` below reads the same tuple. That is the whole point of them being
    module constants: sweeps that spell the set separately are sweeps that drift, and the
    drift is what this function was extracted to end."""
    return sorted(
        path
        for path in root.rglob("*")
        if path.suffix in BUNDLE_DOC_SUFFIXES
        and path.is_file()
        and CLI_OUTPUT_DIR not in path.relative_to(root).parts
    )


def resource_files(root: Path = RESOURCES) -> list[Path]:
    """The bundle documents directly in `root`, sorted: one directory, no descent.

    NOT `bundle_files`, which recurses. The SUFFIXES are shared with it, because that is the
    half that drifted, and they may be wider than the `include:` globs `databricks.yml`
    declares -- a file here that the bundle would not pick up is classified anyway. That is
    over-strict, which is the direction to be wrong in."""
    return sorted(
        path
        for path in root.iterdir()
        if path.suffix in BUNDLE_DOC_SUFFIXES and path.is_file()
    )


def bundle_docs() -> dict[str, object]:
    """What `bundle_files` finds ON DISK under the bundle root, parsed, keyed by its path
    relative to that root.

    A SUFFIX SWEEP, NOT A LIST OF WHAT THE BUNDLE READS AS SOURCE.
    `databricks/dashboards/dataops.lvdash.json` is a Lakeview export rather than a resource
    file, and this sweep parses it like one. Kept that way: the wide sweep's cost is a false
    RED over a file nobody deploys, which is loud and says look, and the narrow sweep's
    would be a false green.

    THE FILESYSTEM AND NOT `git ls-files`, DELIBERATELY, and the docstring used to say
    "every committed bundle file" while doing this -- a description of a narrower sweep
    than the code performs. The walk is the right one and the sentence was wrong: an
    UNTRACKED resource file is still a file `include:` picks up and `bundle deploy` sends,
    and `git ls-files` is blind to exactly those. `CLAUDE.md` records four false greens
    from that blindness already. So the sweep is a superset of the committed set, and a
    declaration that would deploy from OUTSIDE this directory is one it does not read.

    WHICH IS A REAL GAP AND IS NAMED RATHER THAN ROUNDED OFF. `include:` accepts a path
    outside the bundle root -- measured on a scratch bundle, `include: ../outside/*.yml`
    validates `exit=0` and renders the resource -- and no file this helper reads would
    mention it. What holds the line there is that adding such an entry means editing
    `databricks.yml`, which is one of the documents these sweeps read.

    WHICH SUFFIXES IT READS AND WHAT IT SKIPS ARE `bundle_files`', not this function's, and
    the reasons are stated where the two constants are declared. `yaml.safe_load` parses the
    JSON ones as well -- measured on every such file under `databricks/` on this box -- so
    one reader covers the set."""
    root = BUNDLE.parent
    return {
        str(path.relative_to(root)): yaml.safe_load(path.read_text(encoding="utf-8"))
        for path in bundle_files(root)
    }


def keys_anywhere(node) -> set[str]:
    """Every mapping key at any depth of a parsed document.

    EXTRACTED RATHER THAN COPIED, for this module's own reason. Two locks ask the same
    question of the same documents -- whether any committed file DECLARES
    `pause_status`, which is what leaves the deployment MODE deciding whether a
    schedule fires. `tests/test_bundle_targets_and_schedules.py` owns that rule and
    `tests/dataops/test_cadence.py` rests its whole premise on it. A second copy of the
    recursion would be a second spelling, and the copy that rots is the unread one."""
    if isinstance(node, dict):
        return set(node) | {k for v in node.values() for k in keys_anywhere(v)}
    if isinstance(node, list):
        return {k for item in node for k in keys_anywhere(item)}
    return set()


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
