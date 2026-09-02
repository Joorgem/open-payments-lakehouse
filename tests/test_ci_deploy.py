# tests/test_ci_deploy.py
"""THE CI DEPLOY AND THE DRIFT CHECK THAT MAKES A MISSING DEPLOY LOUD -- ADR 0021.

WHY THIS FILE EXISTS. ADR 0009 rejected binding a run's expected revision at deploy
time because *"nobody deployed at all"* then compares a value with itself and passes.
ADR 0021 answers that objection with a DETECTOR rather than a better value: CI
deploys on every push to `main` and then reads back the revision the WORKSPACE holds.
The whole mechanism therefore rests on two things being true of
`.github/workflows/ci.yml` and `scripts/check_deployed_revision.py`, neither of which
any CI run can currently demonstrate, because the secrets that arm the job do not
exist -- `gh secret list` prints nothing. **A mechanism that cannot run is exactly the
thing this repository locks in tests rather than trusts in prose.**

WHAT A CI RUN WILL NOT TELL ANYONE, AND WHAT THESE ARMS THEREFORE HAVE TO. While the
job is inert every step of it is SKIPPED, so a green `deploy` job says nothing at all
about whether it would deploy the right target, compare against the right revision, or
refuse an empty workspace. That is the shape ADR 0018 names -- a check reporting the
expected value because it could not look -- and it is why the arms below read the
declaration rather than the outcome.

THE SCRIPT'S ARMS ARE HERMETIC AND ITS REMOTE HALF IS NOT TESTED HERE. Nothing below
opens a socket. The functions that talk to a workspace were measured by hand against
the live one on 2026-09-02 -- a mismatch, a match, and a target nothing is deployed to
-- and what is locked here is everything that decides WHAT the script compares and
WHETHER it may report success, because those are the parts a future edit can get wrong
silently.
"""
from __future__ import annotations

import importlib.util
import io
import zipfile
from pathlib import Path

import pytest
import yaml

from opl.bronze.provenance import WrongRevision, assert_revision_matches

_REPO = Path(__file__).resolve().parents[1]
_WORKFLOW = _REPO / ".github" / "workflows" / "ci.yml"
_BUNDLE = _REPO / "databricks" / "databricks.yml"
_SCRIPT_PATH = _REPO / "scripts" / "check_deployed_revision.py"

# THE TARGET THAT MUST NEVER BE DEPLOYED FROM ANYWHERE, let alone unattended. Its
# `mode: production` is what renders every `schedule:` block UNPAUSED (ADR 0021
# Decision 2), and every guarded job then refuses at its first task, on a cadence.
_NEVER_DEPLOYED = "prod"


def _load(name: str, path: Path):
    """A module that is not on any import path, loaded by file location.

    The same four lines as `tests/test_revision_stamp.py`, for the same reason: neither
    `scripts/` nor `hatch_build.py` is part of a package, and adding one to `sys.path`
    would make the suite's import behaviour depend on which test ran first."""
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cli = _load("check_deployed_revision_cli", _SCRIPT_PATH)
hook = _load("opl_hatch_build_for_ci_deploy", _REPO / "hatch_build.py")


def _workflow() -> dict:
    return yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))


def _deploy_job() -> dict:
    job = _workflow()["jobs"].get("deploy")
    assert job, (
        "`.github/workflows/ci.yml` declares no `deploy` job. ADR 0021 Decision 1 rests "
        "on the deploy being an act of CI: without it, deploy-time binding is the answer "
        "ADR 0009 rejected, and nothing goes red when nobody deploys."
    )
    return job


def _steps() -> list[dict]:
    return list(_deploy_job()["steps"])


def _step_running(fragment: str) -> tuple[int, dict]:
    """The one step of the `deploy` job whose `run` contains `fragment`."""
    found = [(i, s) for i, s in enumerate(_steps()) if fragment in (s.get("run") or "")]
    assert len(found) == 1, (
        f"the `deploy` job has {len(found)} steps running {fragment!r}; exactly one is "
        f"expected. Steps: {[s.get('name') or s.get('uses') for s in _steps()]}"
    )
    return found[0]


def _flag_value(command: str, *flags: str) -> str | None:
    """The value following the first of `flags` in a shell command line."""
    tokens = command.split()
    for flag in flags:
        if flag in tokens:
            return tokens[tokens.index(flag) + 1]
    return None


def _rendered(job: dict) -> str:
    """One job's effective configuration as text -- KEYS INCLUDED, comments gone.

    YAML COMMENTS ARE ALREADY GONE by the time this runs, which is what makes the sweep
    below stronger than the `git grep -inE '^[^#]*databricks'` it replaces: a parser
    knows what a comment is, and `^[^#]*` only knows what a `#` is.

    RE-RENDERING RATHER THAN WALKING THE VALUES, and that is a correction rather than a
    style. The walker written first collected `node.values()` and no keys, so a job
    declaring `DATABRICKS_HOST: https://…` was INVISIBLE to it -- planted in the `test`
    job, and the arm below stayed green. The identical blind spot is in
    `tests/test_iac_terraform.py::_strings`, whose credential arm asks about
    `DATABRICKS_` and `secrets.` in exactly the same way; today no key there carries
    either, so that arm is correct and narrower than it reads."""
    return yaml.safe_dump(job, default_flow_style=False, sort_keys=True)


def test_the_deploy_job_deploys_and_then_reads_back_what_it_deployed():
    """BOTH HALVES AND IN THAT ORDER, because each alone is green over the other's job.

    A job that deploys and never looks is the hand-run deploy this repository already
    has, moved onto a runner: ADR 0009's incident 2 happened with a deploy in it. A job
    that looks and never deploys reports on whatever somebody last did from a laptop.
    And the order is not cosmetic -- run before the deploy, the check answers a question
    about the PREVIOUS merge and would go red on every correct one."""
    deploying, _ = _step_running("bundle deploy")
    checking, _ = _step_running("check_deployed_revision.py")
    assert deploying < checking, (
        "the `deploy` job reads the workspace's revision BEFORE it deploys, so it "
        "reports on the previous merge rather than on this one"
    )


def test_the_deploy_job_never_deploys_the_production_target():
    """ADR 0021 Decision 2, asserted rather than promised.

    `mode: production` is what writes `pause_status: UNPAUSED` onto every declared
    schedule -- measured, on a scratch bundle, both ways. Deploying that target
    unattended would start eleven cadences whose every guarded job refuses at its first
    task, on the only workspace this project has. The target exists to state a cadence,
    not to be deployed."""
    assert _NEVER_DEPLOYED not in _rendered(_deploy_job()).split(), (
        f"the `deploy` job names {_NEVER_DEPLOYED!r}, and `-t {_NEVER_DEPLOYED}` deploys "
        "the target "
        "ADR 0021 declares in order never to deploy: its mode unpauses every schedule in "
        "the bundle."
    )


def test_the_target_it_deploys_is_one_the_bundle_declares():
    """A typo here is a deploy that fails for a reason nobody would read as a typo, and
    a target the bundle stopped declaring is one this job would go on naming."""
    _, step = _step_running("bundle deploy")
    target = _flag_value(step["run"], "-t", "--target")
    declared = yaml.safe_load(_BUNDLE.read_text(encoding="utf-8"))["targets"]
    assert target in declared, (
        f"the `deploy` job deploys {target!r}; `databricks/databricks.yml` declares "
        f"{sorted(declared)}"
    )


def test_the_target_the_check_reads_is_the_target_the_deploy_writes():
    """TWO SPELLINGS OF ONE CHOICE, and they are in different files.

    The step deploys `-t <target>`; the check reads whichever target it is given, and
    is given none, so it takes the script's default. Move one and not the other and the
    check reads a deployment this job never wrote -- which refuses rather than passes
    (an empty deployment is not a pass, by construction), but refuses for a reason no
    log would explain."""
    _, deploying = _step_running("bundle deploy")
    _, checking = _step_running("check_deployed_revision.py")
    written = _flag_value(deploying["run"], "-t", "--target")
    read = _flag_value(checking["run"], "--target") or cli._DEFAULT_TARGET
    assert written == read, (
        f"the `deploy` job deploys {written!r} and then reads {read!r} back. The second "
        f"value comes from `{_SCRIPT_PATH.name}`'s own default when the step passes no "
        "`--target`."
    )


def test_the_deploy_job_runs_only_on_a_push_to_main():
    """A PULL REQUEST'S HEAD IS NOT `main`'S HEAD.

    This workflow triggers on `pull_request` as well as on pushes to `main`. Without
    this condition every PR would deploy its own branch into the one workspace there
    is, and the comparison that follows would then be against a revision nobody
    merged -- green, and about the wrong thing."""
    condition = str(_deploy_job().get("if", ""))
    for required in ("refs/heads/main", "push"):
        assert required in condition, (
            f"the `deploy` job's `if:` is {condition!r}, which does not restrict it to "
            f"{required!r}. This workflow also triggers on `pull_request`."
        )


def _gate_step() -> tuple[str, dict]:
    """The first step of the `deploy` job, which must be the one that decides.

    ITS ID IS DERIVED FROM THE FILE rather than typed here: what these arms need is
    that some first step publishes an arming decision and that everything after it
    reads THAT step, not that the step is called anything in particular."""
    first = _steps()[0]
    identifier = first.get("id")
    assert identifier, (
        "the `deploy` job's first step has no `id`, so no later step can read its "
        "decision and the gate cannot be what stands the job down"
    )
    assert "GITHUB_OUTPUT" in (first.get("run") or ""), (
        f"the first step {identifier!r} writes no output, so it decides nothing"
    )
    return identifier, first


def test_every_step_that_can_touch_the_workspace_stands_down_without_the_gate():
    """THE INERT SHIP, AND IT IS MECHANICAL RATHER THAN INTENDED.

    Creating a `DATABRICKS_TOKEN` secret is the operator's decision; until it is made,
    every step after the gate must be skipped -- including `checkout` and `uv sync`,
    which cost a runner minute for nothing, and above all the deploy and the check,
    which would fail on absent credentials and turn `main` red for a state nobody chose.

    A JOB-LEVEL `if:` CANNOT DO THIS and that is measured, not assumed: `secrets` is
    unavailable in `jobs.<id>.if` and so is `env`, and a workflow that reads either
    there fails to parse -- the run dies at 0s with no job created."""
    identifier, _ = _gate_step()
    expected = f"steps.{identifier}.outputs"
    unguarded = [
        step.get("name") or step.get("uses")
        for step in _steps()[1:]
        if expected not in str(step.get("if", ""))
    ]
    assert not unguarded, (
        f"{unguarded} run whether or not the gate armed. Every step after the gate has "
        f"to carry `if:` on `{expected}...`"
    )


def test_the_disarmed_path_says_why_instead_of_passing_quietly():
    """A SKIP THAT NOBODY SEES IS THE DEFECT THIS JOB EXISTS TO REMOVE.

    While the secrets are absent this job is GREEN with every substantive step skipped,
    and a green `deploy` beside a green `test` reads as "the workspace has main". So the
    disarmed branch has to say so where a reader of the run will see it: an annotation
    on the run, and a line in the job summary. Neither is decoration -- without them the
    only difference between "checked and correct" and "never looked" is the colour of a
    step nobody expands."""
    _, gate = _gate_step()
    run = gate["run"]
    for required in ("::warning", "GITHUB_STEP_SUMMARY", "INERT", "DATABRICKS_TOKEN"):
        assert required in run, (
            f"the gate step does not mention {required!r}, so a disarmed run cannot tell "
            f"a reader that nothing was deployed and nothing was checked:\n{run}"
        )


def test_no_ci_job_both_names_databricks_and_runs_the_suite():
    """THE SENTENCE SEVERAL COMMITTED FILES PUBLISH, MADE ENFORCEABLE.

    `tests/test_bundle_resource_allowlist.py -k swept_paths` needs the Databricks CLI
    and SKIPS where there is none, and committed files say that this is every CI job
    running the suite -- derive which with `git grep -ln test_ci_deploy -- .`, since
    each of them now cites this arm. Until this phase the claim was derivable by
    grepping `.github/` for the word; the `deploy` job makes the word appear, so it
    needed either a weaker sentence or a stronger check. This is the stronger check.

    IT IS WIDER THAN THE CLAIM, DELIBERATELY, in the way an allow-list is wider than
    the hazard it was written for: it refuses a suite-running job that merely sets
    `DATABRICKS_HOST`, which would be a legitimate thing to want (integration tests
    against a live workspace) and is exactly the kind of thing that should be argued
    for rather than arrive in a diff."""
    jobs = {name: _rendered(job) for name, job in _workflow()["jobs"].items()}
    naming = {name for name, text in jobs.items() if "databricks" in text.lower()}
    testing = {name for name, text in jobs.items() if "pytest" in text}
    assert naming, "no CI job names Databricks at all, so this sweep holds vacuously"
    assert testing, "no CI job runs pytest at all, so this sweep holds vacuously"
    assert not naming & testing, (
        f"{sorted(naming & testing)} both name Databricks and run pytest. Four documents "
        "and `tests/test_bundle_resource_allowlist.py` say the CLI-dependent arm skips "
        "in CI; a job that has both makes those sentences false without anything going "
        "red."
    )


def _job(marker: str, *, dependencies: list[str] | None = None, whl: str | None = None) -> dict:
    """One entry shaped like `jobs/list` returns them, measured against the live API."""
    settings: dict = {"deployment": {"kind": "BUNDLE", "metadata_file_path": marker}}
    if dependencies is not None:
        settings["environments"] = [{"spec": {"dependencies": dependencies}}]
    if whl is not None:
        settings["tasks"] = [{"libraries": [{"whl": whl}]}]
    return {"job_id": 1, "settings": settings}


_MARKER = "/Workspace/Users/someone@example.invalid/.bundle/opl/free/state/metadata.json"
_OTHER = "/Workspace/Users/someone@example.invalid/.bundle/opl/prod/state/metadata.json"


def test_the_wheels_are_read_from_both_places_a_job_can_name_one():
    """TWO SHAPES A JOB CAN NAME ITS WHEEL IN, AND NOT A CLAIM THAT THERE ARE ONLY TWO.

    Serverless jobs carry `environments[].spec.dependencies`; a classic task carries
    `libraries[].whl`. A reader of one of them returns an empty set over a deployment
    written in the other -- and an empty set is the "nobody deployed" refusal, so the
    failure would arrive as a confident red about the wrong thing. What the Jobs API
    can carry beyond these two is not derived here and nothing claims it is empty."""
    marker = "/.bundle/opl/free/"
    jobs = [
        _job(_MARKER, dependencies=["/a/one.whl", "/a/not-a-wheel.txt"]),
        _job(_MARKER, whl="/a/two.whl"),
        _job(_OTHER, dependencies=["/a/somebody-elses.whl"]),
    ]
    assert cli.deployed_wheels(jobs, marker) == {"/a/one.whl", "/a/two.whl"}


def test_a_deployment_with_no_wheel_is_refused_and_not_reported_as_a_pass():
    """THE VACUOUS GREEN, REFUSED AT THE ONE PLACE IT COULD ARRIVE.

    Nothing deployed, a renamed bundle, a target that was never deployed, a token whose
    principal cannot see the jobs: all four give an empty set here, and all four mean
    the check could not find its subject. Reporting success for any of them is the
    species this whole mechanism exists to remove, so the empty case is a refusal with
    a message that says as much."""
    with pytest.raises(WrongRevision) as refused:
        cli.one_wheel(set(), "/.bundle/opl/free/")
    assert "NOT a pass" in str(refused.value)


def test_two_wheels_in_one_deployment_are_refused_rather_than_picked_between():
    """ADR 0009's glob, arriving. The jobs install through `dist/*.whl` over a `dist/`
    that nothing cleans, so two wheels means the artefact a run executes is decided by
    a sort order -- and this check would then be reading one of them and reporting on
    the other. Neither is the answer; the refusal is."""
    with pytest.raises(WrongRevision) as refused:
        cli.one_wheel({"/a/one.whl", "/a/two.whl"}, "/.bundle/opl/free/")
    assert "more than one wheel" in str(refused.value)


def _wheel_bytes(member: str, source: str) -> bytes:
    """A zip archive holding one member, which is all a wheel is to this reader."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(member, source)
    return buffer.getvalue()


def test_the_stamp_the_build_hook_writes_is_the_stamp_this_check_reads():
    """THE CONTRACT BETWEEN THE TWO HALVES, THROUGH THE REAL GENERATOR.

    `hatch_build.py` writes the module and this script reads it; they are separated by
    a wheel build, an upload and a download, so nothing at run time would notice them
    drifting. A hand-written literal here would pass over exactly that drift, which is
    why the fixture is the hook's own `revision_module_source`."""
    revision = "b" * 40
    blob = _wheel_bytes(hook.STAMPED_MODULE, hook.revision_module_source(revision))
    assert cli.revision_in_wheel(blob) == revision
    assert assert_revision_matches(expected=revision, actual=revision) == revision


def test_a_wheel_with_no_stamp_is_refused_by_the_comparison_it_feeds():
    """An unstamped wheel is not "unknown, carry on": `opl.bronze.provenance` refuses it
    and names the build path that produces a stamped one. This arm exists because the
    empty string it returns LOOKS like a benign default, and would be one if the value
    went anywhere that treated it as such."""
    blob = _wheel_bytes("opl/__init__.py", "")
    assert cli.revision_in_wheel(blob) == ""
    with pytest.raises(WrongRevision) as refused:
        assert_revision_matches(expected="c" * 40, actual=cli.revision_in_wheel(blob))
    assert "does not say what revision it was built from" in str(refused.value)


def test_every_workspace_path_this_check_prints_has_the_user_name_removed():
    """THIS REPOSITORY IS PUBLIC AND SO ARE ITS CI LOGS.

    The wheel path this bundle's deployed jobs name begins `/Workspace/Users/<the
    operator>/` -- measured against the live workspace -- and this check prints paths in
    both its success line and two of its refusals. The redaction is therefore not
    tidiness: it is the difference between a green build and a workspace user name
    published on every merge."""
    path = "/Workspace/Users/someone@example.invalid/.bundle/opl/free/artifacts/x.whl"
    hidden = cli.redacted(path)
    assert "someone@example.invalid" not in hidden
    assert hidden.endswith("/.bundle/opl/free/artifacts/x.whl")
    assert cli.redacted("/Volumes/main/default/land/x.zip") == "/Volumes/main/default/land/x.zip"
