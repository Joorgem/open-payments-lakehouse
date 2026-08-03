# hatch_build.py
"""Stamps the revision a wheel was built from INTO that wheel.

WHY THIS FILE EXISTS AT ALL. Nothing in this artefact encoded its source revision:
`pyproject.toml` declares a static `version = "0.0.0"` with no `dynamic`, no
hatch-vcs and no setuptools-scm, and the installed dist-info is literally
`open_payments_lakehouse-0.0.0.dist-info`. A run therefore had no way to say which
source it was executing -- which is how F1.4b PR A produced a green socios re-run
against a bundle deployed four commits earlier. `opl.bronze.provenance` compares
what this hook writes against the revision an operator launches the run for; ADR
0009 carries the argument for both halves.

WHY A GENERATED MODULE AND NOT THE PACKAGE VERSION. A SHA in the version is a SHA
in the wheel FILENAME, and the jobs receive the wheel through a glob --
`dependencies: ["../../dist/*.whl"]` -- over a `dist/` that nothing cleans. Today
the filename is constant, so a rebuild overwrites the one wheel and the glob cannot
be ambiguous; a version-shaped stamp would leave one wheel per commit there, all
matching, and would manufacture the very staleness this guard exists to catch.

WHY NOTHING IS WRITTEN INTO `src/`. The generated module is force-included from a
temporary directory, so it exists only inside the built wheel. A file written into
the source tree would be picked up by the EDITABLE install that every local
`uv run pytest` uses, and a developer's laptop would then satisfy a provenance
check with a value nothing verified. It could also be committed, which is a
provenance claim that goes stale silently.

WHY EDITABLE BUILDS ARE SKIPPED, measured rather than assumed: without the skip,
hatchling force-includes the stamp into the editable wheel too, which creates a
shadow `site-packages/opl/` holding only this module. The package still resolves to
`src/opl` -- a regular package beats a namespace portion -- so the stamp is
unreachable AND two directories now compete for the name. Skipping leaves a local
install with no stamp, which is exactly what `opl.bronze.provenance` refuses.

THIS IS THE ONE PLACE IN THIS REPOSITORY THAT ASKS GIT ANYTHING. It runs at build
time, on the machine that builds the wheel. Nothing in the wheel and nothing under
`databricks/src` may: there is no repository beside the running artefact, so a
runtime call would either crash or answer from the operator's own checkout and
compare it against itself. `tests/test_revision_stamp.py` enforces that.
"""
from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

# Where the stamp lands INSIDE the wheel: next to `_version_check.py`, which asks the
# same category of question (what is this artefact?) about the same artefact.
STAMPED_MODULE = "opl/_revision.py"

# WHAT COUNTS AS THE WHEEL'S OWN INPUTS, and the narrowness is the decision.
# `[tool.hatch.build.targets.wheel] packages = ["src/opl"]` plus the metadata in
# `pyproject.toml` is what ends up in the artefact, so those are the paths whose
# uncommitted state makes a bare SHA a false claim. Docs, evidence files and phase
# notes cannot change the wheel and must not refuse a run -- this phase writes them
# WHILE multi-hour runs are going on, and a guard that fires on an unrelated new file
# in `docs/` is one operators learn to route around.
# `tests/test_revision_stamp.py` checks this list against the wheel's own declaration:
# a second package added to the wheel and forgotten here would be stamped clean while
# holding uncommitted code.
WHEEL_INPUTS = ("src", "pyproject.toml")

# Appended when those inputs are dirty. It makes the stamp something no expected value
# can equal, which is the point: `git rev-parse HEAD` answers identically in a modified
# tree, so a bare SHA from one would pass the guard while describing code that was never
# committed. `opl.bronze.provenance` recognises this shape and says `git commit` rather
# than `bundle deploy`.
_DIRTY_SUFFIX = "+dirty"


def _git(root: str | Path, *arguments: str) -> str | None:
    """Git's answer, or `None` if it could not give one.

    `None` covers both "git is not installed" and "this is not a checkout" -- an
    sdist unpacked somewhere has no `.git` and must still be installable. Neither is
    an error the build should die on: it is a wheel that cannot say what it came from,
    and the guard refuses such a wheel at run time, where the operator can read why."""
    try:
        done = subprocess.run(
            ["git", "-C", str(root), *arguments],
            capture_output=True, text=True, check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return done.stdout.strip()


def wheel_revision(root: str | Path) -> str:
    """The revision to stamp for the tree at `root`, or `""` if it cannot be known.

    Clean inputs give a bare object name; dirty ones give one no run can expect. An
    unanswerable `git status` is treated as dirty-or-worse and returns `""`: not being
    able to tell whether the tree was modified is not evidence that it was not."""
    head = _git(root, "rev-parse", "HEAD")
    if head is None:
        return ""
    modified = _git(root, "status", "--porcelain", "--", *WHEEL_INPUTS)
    if modified is None:
        return ""
    return head if not modified else f"{head}{_DIRTY_SUFFIX}"


def revision_module_source(revision: str) -> str:
    """The generated module's text: one docstring and one assignment, nothing else.

    Executed by the deployed artefact, so it holds no logic at all -- the comparison
    lives in `opl.bronze.provenance`, where it is unit-tested. `!r` quotes the value
    rather than interpolating it raw: this is generated source, and generated source
    that pastes an outside string into a literal is a habit worth not having."""
    return (
        '"""GENERATED AT WHEEL BUILD TIME by hatch_build.py. Do not edit; do not commit.\n'
        "\n"
        "The git revision this wheel's contents were built from. `opl.bronze.provenance`\n"
        "compares it against the revision the run was launched for and refuses a\n"
        "mismatch, an absent value, and a build from a modified tree (ADR 0009).\n"
        "\n"
        "Absent from the source tree on purpose: an editable install must carry no\n"
        'revision, so that a developer\'s laptop cannot satisfy a provenance check."""\n'
        f"REVISION = {revision!r}\n"
    )


class CustomBuildHook(BuildHookInterface):
    """Generates `STAMPED_MODULE` into every non-editable wheel."""

    # Set by `initialize`, read by `finalize`. Declared here so that `finalize` after an
    # early return -- an editable build -- has something to find.
    _stamp_dir: str | None = None

    def initialize(self, version: str, build_data: dict) -> None:
        if version == "editable":
            return
        revision = wheel_revision(self.root)
        if not revision:
            # Not fatal, and deliberately so: the wheel is still built, still installs,
            # and states that it does not know its own revision -- which every guarded
            # job then refuses, naming the deploy path that produces a stamped one. A
            # build that died here would break installing from an unpacked sdist.
            print(
                "hatch_build: git could not name the revision of "
                f"{self.root} -- the wheel will be stamped with no revision, and every "
                "job carrying the deployed-revision guard will refuse to run it"
            )
        self._stamp_dir = tempfile.mkdtemp(prefix="opl-revision-")
        stamp = Path(self._stamp_dir) / Path(STAMPED_MODULE).name
        stamp.write_text(revision_module_source(revision), encoding="utf-8")
        build_data["force_include"][str(stamp)] = STAMPED_MODULE

    def finalize(self, version: str, build_data: dict, artifact_path: str) -> None:
        if self._stamp_dir is not None:
            shutil.rmtree(self._stamp_dir, ignore_errors=True)
            self._stamp_dir = None
