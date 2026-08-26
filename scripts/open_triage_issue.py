# scripts/open_triage_issue.py
"""Print ONE drafted triage issue, and open it only when told to. The publisher.

WHY THIS FILE IS IN `scripts/` AND NOT IN THE WHEEL, AND IT IS THE CREDENTIAL BOUNDARY
RATHER THAN A FILING CONVENTION. `pyproject.toml` packages `["src/opl"]` and nothing else,
so this module is not in the artefact `databricks bundle deploy` builds and NOTHING RUNNING
IN THE WORKSPACE CAN IMPORT IT. That is the whole design. The alternative -- a Databricks
task calling the GitHub API -- needs a PAT in a secret scope: a new credential, a new human
gate, and a token with repository write sitting next to 55.8M rows of personal data, for a
POST a laptop can make. `gh` on the operator's box already carries `repo` scope
(docs/f6-run-evidence.md 0.2), so the credential this path needs is one that already exists
and already belongs to a person.

WHAT THE WORKSPACE SIDE DOES INSTEAD: it emits the issue as DATA.
`opl.triage_agent.issue.as_mapping` is JSON, a run writes it, and this reads it. So the
thing that crosses the boundary is a payload a person can read before anything is
published, which is `opl.bronze.reconcile`'s standard one layer up -- that view prints the
repromote command and runs none of it, because "a view that promoted rows would be a gate
bypass wearing a dashboard" (ADR 0018 Decision 3).

IT PRINTS BY DEFAULT AND POSTS ONLY UNDER `--post`. A mis-invocation, a wrong path, a
half-finished command line: none of them can open an issue. And `--batch-id` is REQUIRED
and takes exactly one incident -- there is no arm that loops the feed, so "publish
everything" is not a typo away, and a facts file holding eleven payloads still publishes at
most one per invocation.

WHAT IT DOES NOT PROTECT AGAINST, NAMED BESIDE WHAT IT DOES. It cannot tell a stale facts
file from a fresh one: a payload produced a week ago publishes as readily as one produced
a minute ago, and what a reader has instead is the body's provenance block -- which names
the run its PRODUCER claimed, checked by nothing here and labelled that way in the body.
It does not check the repository's visibility, so pointing `--repo` at a private fork and
at a public one look identical here. It does not read the issue back after posting. And it
takes the body from `opl.triage_agent.report`, so everything that file's header says about
what may reach a public artefact is what stands between this and a leak; this script adds
no redaction of its own and must not, because a second redaction here would be a second
spelling of that one.

usage:
    uv run python scripts/open_triage_issue.py --facts <payloads.json> --batch-id <id>
    uv run python scripts/open_triage_issue.py --facts <payloads.json> --batch-id <id> --post
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from opl.triage_agent.issue import TriageIssue, payloads_from_json
from opl.triage_agent.report import render_body, render_title

# The one external command this script can run. Named as a constant so the test that pins
# it has something to compare against, and so that "what can this thing execute" is
# answerable by reading one line rather than by trusting the prose above.
GH = "gh"


def build_parser() -> argparse.ArgumentParser:
    """The command line. `--batch-id` is required and NO OPTION SELECTS MORE THAN ONE.

    THE ABSENT FLAG IS THE FEATURE. A publisher that could loop the feed would put "open
    eleven issues" one word away from "open one", on a corpus whose incidents include five
    whose evidence is already gone. It is a separate function from `parse_args` so that a
    test can enumerate the options this script HAS rather than sweep its source for the
    ones it does not -- a sweep that a sentence in this docstring would defeat, measured."""
    parser = argparse.ArgumentParser(description="Print, or open, one triage issue.")
    parser.add_argument("--facts", required=True, type=Path,
                        help="JSON written by opl.triage_agent.issue.as_mapping (one, or a list)")
    parser.add_argument("--batch-id", required=True,
                        help="the ONE incident to publish, as its job_run_id")
    parser.add_argument("--repo", default=None,
                        help="owner/name; default is whatever `gh` infers from this checkout")
    parser.add_argument("--post", action="store_true",
                        help="actually open the issue. Without it, nothing leaves this machine")
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """One invocation's arguments."""
    return build_parser().parse_args(argv)


def select(payloads: tuple[TriageIssue, ...], batch_id: str) -> TriageIssue:
    """The one payload for `batch_id`, or refuse. NEVER A FIRST MATCH.

    Both refusals are reachable from an ordinary mistake: a mistyped id, and a facts file
    that accumulated two runs of one incident. Publishing the first of two would pick by
    file order, which is not a decision anybody took."""
    found = [payload for payload in payloads if payload.batch_id == batch_id]
    if not found:
        raise SystemExit(
            f"no payload for batch {batch_id}. This file holds "
            f"{sorted(payload.batch_id for payload in payloads)}"
        )
    if len(found) > 1:
        raise SystemExit(
            f"{len(found)} payloads for batch {batch_id}. Two readings of one incident "
            "cannot both be published, and picking the first would be file order deciding"
        )
    return found[0]


def gh_command(issue: TriageIssue, repo: str | None) -> list[str]:
    """The `gh` invocation, as a list. NEVER A SHELL STRING.

    `--body-file -` because the body is markdown with backticks, dollars and newlines in
    it: an argv element carries all three unharmed and a shell string does not. The remedy
    line alone contains `$(git rev-parse HEAD)`, which a shell would EXECUTE."""
    command = [GH, "issue", "create", "--title", render_title(issue), "--body-file", "-"]
    if repo:
        command += ["--repo", repo]
    return command


def post(issue: TriageIssue, repo: str | None) -> int:
    """Open the issue through `gh`, and return its exit code.

    `gh` AND NOT A PAT IN THIS PROCESS. This script never reads a token, never accepts one
    as an argument and never puts one in an environment variable: the credential stays
    where `gh auth` already keeps it, which is the whole reason the publisher is the thing
    on the laptop."""
    if shutil.which(GH) is None:
        raise SystemExit(
            f"`{GH}` is not on PATH. This publisher opens issues through the operator's own "
            "authenticated CLI on purpose -- there is no token argument and no fallback to "
            "the API, because a fallback would need a credential this path exists to avoid"
        )
    done = subprocess.run(
        gh_command(issue, repo), input=render_body(issue), text=True, check=False
    )
    return done.returncode


def main(argv: list[str] | None = None) -> int:
    """Print the issue; post it only under `--post`.

    THE PRINTED FORM IS THE POSTED FORM -- one `render_body` call in each arm, of the same
    record -- so what a reviewer reads on the terminal is what a stranger would read on
    GitHub, rather than a preview built by a second code path."""
    args = parse_args(argv)
    issue = select(payloads_from_json(args.facts.read_text(encoding="utf-8")), args.batch_id)
    if not args.post:
        print(render_title(issue))
        print()
        print(render_body(issue))
        print()
        print(f"NOT POSTED. This printed one issue and opened none. To open it, add --post, "
              f"which runs: {' '.join(gh_command(issue, args.repo))}")
        return 0
    return post(issue, args.repo)


if __name__ == "__main__":
    sys.exit(main())
