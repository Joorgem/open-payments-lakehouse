# tests/bundle_cli.py
"""How `databricks bundle validate` is RUN WITH THE WORKSPACE TAKEN AWAY. No test lives
here, and that absence is the point.

THE THIRD FILE OF A TWO-WAY SPLIT, the shape `tests/job_yaml.py` and `tests/adr_files.py`
already have in this suite. Two modules ask the CLI a question that only has an answer when
no credential is reachable: `tests/test_bundle_document_set.py` asks which SUFFIXES it reads
as bundle documents, and `tests/test_credential_skip_signature.py` asks what it SAYS when it
cannot authenticate. Neither can import the other -- a test module importing a test module
gives the suite a collection-order dependency, which is the reason both of the modules named
above exist -- so the runner they share lives here.

IT IS NOT A THIRD SPELLING OF THE TWO CLI CALLS ALREADY IN THIS SUITE, and the difference is
the subject rather than the style. `test_bundle_resource_allowlist._bundle_schema` asks the
CLI for its own schema and `test_bundle_targets_and_schedules._rendered` renders THIS
repository's bundle against the ambient environment -- it must, because what it asserts is
what this bundle renders to on a box that can reach the workspace. What is here runs the CLI
over a bundle the caller BUILT, with every credential removed, and asserts nothing at all.

THE ENVIRONMENT IS SCRUBBED BY PREFIX, NEVER BY NAMING THE VARIABLES. A list of names goes
stale the day the CLI learns another one, and it would go stale silently: the probe would
quietly authenticate and stop producing the state it exists to produce, which is a green
nobody earned. Measured on CLI v1.8.0 on a box holding a working `opl-free` profile: with
`DATABRICKS_CONFIG_FILE` pointed at a path that does not exist and no other `DATABRICKS_`
variable set, `bundle validate` fails at *default auth* and still writes the rendered
document to stdout -- which is why the callers read stdout and never the exit code.

NO `MSYS_NO_PATHCONV` IN THE CHILD ENVIRONMENT, for the reason
`test_bundle_resource_allowlist._bundle_schema` already gives: MSYS rewrites arguments when
an MSYS SHELL launches a native binary, a CPython child is not one, and no argument below
starts with a slash.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

# The prefix every variable the CLI reads a credential from carries. Scrubbing by prefix is
# this module's whole point -- see the docstring.
ENV_PREFIX = "DATABRICKS_"

# The one variable put BACK, pointed at a path that does not exist. Without it the CLI falls
# through to `~/.databrickscfg`, which on a developer box is exactly where the profile lives,
# and the scrub would remove nothing that mattered.
CONFIG_FILE = f"{ENV_PREFIX}CONFIG_FILE"

# The name of the file that is deliberately absent. It is placed under the caller's own
# scratch root rather than at a fixed absolute path, so two probes running at once cannot
# collide and nothing outside a `tmp_path` is ever named.
ABSENT_CONFIG = "no-such-databricks-config.ini"

NO_CLI = "no `databricks` CLI on PATH; this derivation is a developer-box arm, not a CI lock"


def scrubbed_environment(root: Path) -> dict[str, str]:
    """`os.environ` with every `DATABRICKS_` variable dropped and the config file redirected.

    A COPY, never a mutation of `os.environ`: this process is a pytest worker that other
    tests share, and a test that unset the operator's credentials for the rest of the run
    would be a failure nobody could attribute."""
    scrubbed = {k: v for k, v in os.environ.items() if not k.startswith(ENV_PREFIX)}
    scrubbed[CONFIG_FILE] = str(root / ABSENT_CONFIG)
    return scrubbed


def validate(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    """`databricks bundle validate <arguments>` over the bundle at `root`, or a skip.

    RETURNS THE COMPLETED PROCESS AND JUDGES NOTHING. One caller reads the rendered JSON on
    stdout and the other reads the refusal on stderr; a helper that asserted on the exit
    code would serve neither, because with the environment scrubbed the exit code is
    non-zero whatever the bundle says.

    THE ONLY SKIP IS THE CLI BEING ABSENT, which is `_bundle_schema`'s rule and for its
    reason: what these arms derive is the CLI's own answer, so a box without one cannot
    answer and says so, while a box that has one is held to what it said. `encoding` is
    named because the CLI's output carries non-ASCII and Windows would otherwise decode it
    in the ANSI codepage and raise."""
    cli = shutil.which("databricks")
    if cli is None:
        pytest.skip(NO_CLI)
    return subprocess.run(
        [cli, "bundle", "validate", *arguments],
        cwd=root,
        env=scrubbed_environment(root),
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
