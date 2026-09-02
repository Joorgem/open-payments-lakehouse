"""Is the wheel THE WORKSPACE HOLDS the one `main` is at? -- ADR 0021's detector.

WHY THIS EXISTS, AND IT IS NOT A BETTER COMPARISON THAN THE ONE THE JOBS ALREADY
CARRY. ADR 0009 rejected binding a run's expected revision at DEPLOY time, because
"nobody deployed at all" then gives `expected == actual == the old sha` and passes --
the socios re-run that terminated SUCCESS having masked only bronze. ADR 0021 keeps
that objection and answers it somewhere else: the defect is a SILENCE, not a value, so
what repairs it is a DETECTOR FOR THE SILENCE placed where nobody can skip it. This
script is that detector, and `.github/workflows/ci.yml` runs it on every push to
`main`.

TWO SOURCES, WHICH IS THE WHOLE OF IT, exactly as in `opl.bronze.provenance`:

  * the EXPECTED revision is `main`'s head, handed in on the command line -- in CI by
    GitHub, as `github.sha`. Nothing here asks git for it and nothing here asks the
    workspace for it;
  * the ACTUAL revision is read out of the wheel the WORKSPACE holds. The path comes
    from the deployed job definitions rather than from this checkout, the bytes come
    from the workspace, and the stamp inside them was written by `hatch_build.py` on
    whichever machine built that wheel.

Read both from one place and the check passes always while looking exactly like a
working one. So nothing below derives one side from the other, and nothing below
consults `dist/`: a local wheel is evidence about this checkout, which is the thing
already known.

THE COMPARISON ITSELF IS NOT REIMPLEMENTED. `opl.bronze.provenance.assert_revision_
matches` is THE spelling of that rule -- it already refuses an unstamped artefact, a
mismatch and a `+dirty` build, with a different instruction for each -- and a second
spelling here is a second rule that can drift from it. Two spellings of one rule is
how `2026-13` came to be refused at two of four entry points.

WHAT A GREEN FROM THIS SCRIPT DOES NOT MEAN, stated because ADR 0021 states it: it
does not make a scheduled run's own log self-sufficient. A scheduled run still cannot
say, from inside itself, that it is executing `main`. This says that the workspace
held `main`'s head at the moment CI looked, and that somebody would have gone red if
it had not.

    uv run python scripts/check_deployed_revision.py --expected "$(git rev-parse main)"
"""
from __future__ import annotations

import argparse
import ast
import io
import re
import sys
import zipfile
from pathlib import Path

import yaml

from databricks.sdk import WorkspaceClient
from opl.bronze.provenance import STAMP_MODULE, WrongRevision, assert_revision_matches

_REPO = Path(__file__).resolve().parents[1]

# The bundle document is the ONE place the bundle's name is written. Reading it here
# rather than typing the name means a renamed bundle cannot leave this script matching
# jobs that no longer exist -- which would look exactly like "nobody deployed".
_BUNDLE_FILE = _REPO / "databricks" / "databricks.yml"

# Where the stamp sits INSIDE the wheel, derived from the import the deployed artefact
# performs rather than spelled again. `tests/test_revision_stamp.py` pins the build's
# destination against this same constant, for the reason its own probe found: every
# test that names the same literal the hook does stays green while the two diverge.
_STAMP_MEMBER = f"{STAMP_MODULE.replace('.', '/')}.py"
_REVISION_ATTRIBUTE = "REVISION"

# A workspace path carries the operator's user name. This repository is public and its
# CI logs are public with it, so every path this script prints goes through here first.
_USER_SEGMENT = re.compile(r"(/Workspace/Users/)[^/]+")

_DEFAULT_TARGET = "free"


def redacted(path: str) -> str:
    """A workspace path with the user segment removed, for printing."""
    return _USER_SEGMENT.sub(r"\1<user>", path)


def bundle_name(bundle_file: Path = _BUNDLE_FILE) -> str:
    """The bundle's name, as `databricks.yml` declares it."""
    declared = yaml.safe_load(bundle_file.read_text(encoding="utf-8"))
    name = (declared.get("bundle") or {}).get("name")
    if not name:
        raise SystemExit(f"{bundle_file} declares no `bundle.name`")
    return str(name)


def deployment_marker(name: str, target: str) -> str:
    """The substring a bundle deployment writes into every job it owns.

    The platform states the relation itself: a bundle-deployed job carries
    `deployment.metadata_file_path` pointing into that deployment's state directory,
    whose path contains `/.bundle/<bundle>/<target>/`. Matching on that instead of on
    a job NAME matters under `mode: development`, which prefixes every name with the
    deploying user -- a name match would be a match on the operator."""
    return f"/.bundle/{name}/{target}/"


def deployed_wheels(jobs: list[dict], marker: str) -> set[str]:
    """Every wheel the jobs of one bundle deployment install, as workspace paths.

    TWO PLACES A JOB CAN NAME A WHEEL are read -- a serverless environment's
    `dependencies` and a task's `libraries` -- because which one this bundle uses is a
    resource-YAML decision, and a reader of one of them would report an empty set as
    confidently as a real one. That is two shapes, not a claim that the Jobs API has
    only two: a wheel arriving by some third route would read here as no deployment,
    which refuses rather than passes."""
    found: set[str] = set()
    for job in jobs:
        settings = job.get("settings") or {}
        deployment = settings.get("deployment") or {}
        if marker not in (deployment.get("metadata_file_path") or ""):
            continue
        for environment in settings.get("environments") or []:
            spec = environment.get("spec") or {}
            found.update(d for d in (spec.get("dependencies") or []) if d.endswith(".whl"))
        for task in settings.get("tasks") or []:
            for library in task.get("libraries") or []:
                if library.get("whl"):
                    found.add(str(library["whl"]))
    return found


def revision_in_wheel(blob: bytes) -> str:
    """The revision a built wheel states, or `""` if it states none.

    OVER THE AST, NOT BY EXECUTING THE MODULE and not by a regex: this is source that
    arrived over the network, and `""` is a value `assert_revision_matches` already
    refuses with the right message, so an absent stamp needs no special case here."""
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        if _STAMP_MEMBER not in archive.namelist():
            return ""
        source = archive.read(_STAMP_MEMBER).decode("utf-8")
    for node in ast.parse(source, filename=_STAMP_MEMBER).body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Constant):
            continue
        if any(getattr(target, "id", None) == _REVISION_ATTRIBUTE for target in node.targets):
            return str(node.value.value)
    return ""


def one_wheel(wheels: set[str], marker: str) -> str:
    """Exactly one wheel, or refuse -- and NEITHER REFUSAL IS A SPECIAL CASE.

    ZERO is the "nobody deployed at all" incident in its strongest form: no job in
    this workspace claims to come from this bundle deployment, so there is nothing to
    compare and this script must NOT exit 0. A detector that reports success because
    it could not find its subject is the species ADR 0018 names and the one this whole
    mechanism exists to remove.

    MORE THAN ONE is the glob ADR 0009 warned about arriving: the jobs take their wheel
    through `dependencies: ["../../dist/*.whl"]` over a `dist/` that nothing cleans, so
    two wheels in one deployment means the run's artefact is decided by a sort order."""
    if not wheels:
        raise WrongRevision(
            "refusing: no job in this workspace declares itself deployed from "
            f"{marker!r}, so there is no deployed artefact to read a revision out of. "
            "Either nobody has ever run `databricks bundle deploy` for this target, or "
            "the deployment was removed. This is NOT a pass: a check that cannot find "
            "its subject and reports success is worth less than no check."
        )
    if len(wheels) > 1:
        raise WrongRevision(
            f"refusing: {sorted(redacted(w) for w in wheels)} -- one bundle deployment "
            "installs more than one wheel, so which artefact a run executes is decided "
            "by whatever order the platform resolves them in. The jobs take their wheel "
            'through the glob `dependencies: ["../../dist/*.whl"]` over a `dist/` that '
            "nothing cleans; clean it and deploy again."
        )
    return next(iter(wheels))


def deployed_revision(client: WorkspaceClient, marker: str) -> tuple[str, str]:
    """(wheel path, revision) for the one wheel this bundle deployment installs."""
    jobs = [job.as_dict() for job in client.jobs.list(expand_tasks=True)]
    wheel = one_wheel(deployed_wheels(jobs, marker), marker)
    with client.workspace.download(wheel) as handle:
        blob = handle.read()
    return wheel, revision_in_wheel(blob)


def _parse(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--expected",
        required=True,
        help="the revision the workspace SHOULD hold -- `main`'s head, never this "
             "script's own idea of it",
    )
    parser.add_argument("--target", default=_DEFAULT_TARGET, help="the bundle target to read")
    parser.add_argument("--profile", default=None, help="a CLI profile; omit to use the env")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse(argv)
    client = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    marker = deployment_marker(bundle_name(), args.target)
    try:
        wheel, actual = deployed_revision(client, marker)
        verified = assert_revision_matches(expected=args.expected, actual=actual)
    except WrongRevision as refused:
        print(f"check_deployed_revision: {refused}", file=sys.stderr)
        return 1
    print(f"check_deployed_revision: {redacted(wheel)} was built from {verified}, which "
          "is the revision this check was given. The workspace holds it.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
