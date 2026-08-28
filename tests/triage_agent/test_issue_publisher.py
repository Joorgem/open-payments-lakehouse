# tests/triage_agent/test_issue_publisher.py
"""THE ONE THING IN THIS REPOSITORY THAT CAN PUT AN ISSUE IN FRONT OF A STRANGER.

No Spark, and no `gh` either: the subprocess is recorded rather than run, so this file
posts nothing while asserting exactly what a post would be. The one thing it cannot check
is that `gh issue create` behaves as documented -- that is a live call, it belongs to the
run that opens the phase's single real issue, and nothing here should be read as having
exercised it.

WHAT THIS FILE IS ABOUT, AND IT IS A CREDENTIAL BOUNDARY RATHER THAN A CLI. The publisher
lives in `scripts/`, which `pyproject.toml` does not package, so NOTHING RUNNING IN THE
WORKSPACE CAN IMPORT IT -- that is asserted here from both sides. The alternative design, a
Databricks task calling the GitHub API, needs a PAT in a secret scope: a new credential, a
new human gate, and a token with repository write sitting next to 55.8M rows of personal
data, for a POST a laptop can make.

THE DEFAULT IS TO PRINT. `opl.bronze.reconcile` prints the remedy and runs none of it, and
the same standard here means a mis-invocation, a wrong path or a half-finished command line
cannot open an issue. `--post` is the only door, `--batch-id` is required, and there is no
arm that loops the feed -- so "publish everything" is not a typo away on a corpus whose
eleven incidents include five whose evidence is already gone.

WHAT IS NOT COVERED HERE. Whether the repository the issue lands in is public, whether the
facts file is stale, and whether the body is safe to publish: the first two are named in
the script's own header as things it does not check, and the third is `report.py`'s pair of
privacy arms, which this file deliberately does not restate -- a second redaction on this
side would be a second spelling of that one.
"""
from __future__ import annotations

import importlib.util
import json
import subprocess
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

from opl.triage_agent.issue import as_mapping
from opl.triage_agent.report import render_body, render_title

from .issue_facts import EMPRESAS, PAYMENTS, issue

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "open_triage_issue.py"
_spec = importlib.util.spec_from_file_location("open_triage_issue_cli", _SCRIPT)
assert _spec is not None and _spec.loader is not None
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)


def _facts_file(tmp_path: Path, *records) -> Path:
    """A facts file as a producer writes it: the payloads, as JSON."""
    path = tmp_path / "payloads.json"
    path.write_text(json.dumps([as_mapping(record) for record in records]), encoding="utf-8")
    return path


class _Recorder:
    """Stands in for `subprocess.run`. It records and never executes.

    STRICTER THAN THE REAL THING IN ONE PLACE: it fails the test if anything reaches it
    with `shell=True`, which the real call would accept and would make the remedy line's
    `$(git rev-parse HEAD)` executable."""

    def __init__(self, returncode: int = 0):
        self.calls: list[tuple[list[str], str]] = []
        self.returncode = returncode

    def __call__(self, command, *, input=None, text=None, check=None, **kwargs):
        assert not kwargs.get("shell"), "the publisher must never reach a shell"
        self.calls.append((list(command), input))
        return SimpleNamespace(returncode=self.returncode)


def _refuse_to_run(*args, **kwargs):
    raise AssertionError("the publisher ran a subprocess without --post")


# ----------------------------------------------------------------------------------
# It prints. Posting takes a flag.
# ----------------------------------------------------------------------------------


def test_without_the_flag_it_prints_the_issue_and_runs_nothing(tmp_path, capsys, monkeypatch):
    """A mis-invocation cannot open an issue, which is the standard `dataops_reconciliation`
    set for the remedy it prints beside a stranding.

    THE PRINTED FORM IS THE POSTED FORM. Both arms call one `render_body` on one record, so
    what a reviewer reads on the terminal is what a stranger would read on GitHub -- and
    that is asserted rather than argued, because a preview built by a second code path is
    the shape where the two drift."""
    monkeypatch.setattr(cli.subprocess, "run", _refuse_to_run)
    drafted = issue(PAYMENTS)

    code = cli.main(["--facts", str(_facts_file(tmp_path, drafted)),
                     "--batch-id", drafted.batch_id])

    printed = capsys.readouterr().out
    assert code == 0
    assert render_title(drafted) in printed
    assert render_body(drafted) in printed
    assert "NOT POSTED" in printed


def test_the_printed_output_names_the_command_that_would_post_it(tmp_path, capsys, monkeypatch):
    """`promote.require_batch_id`, `reclaim_landing._report_nothing_proven` and
    `backfill_prewrite.refuse_non_empty_quarantine` all print the command that resolves what
    they refused. A publisher that stopped without saying how to proceed would print less
    than this project's own refusals do.

    WHAT IS ASSERTED IS A PROPERTY AND NOT THE JOIN. This test used to restate
    `" ".join(gh_command(...))`, which passes for any rendering of those words -- including
    the space-joined one it was reading. The title carries BACKTICKS (the first assertion is
    that control), so a joined line is a shell line an operator will paste and a shell will
    run `git rev-parse HEAD` out of. So: every argument is named, and the joined form is
    absent."""
    monkeypatch.setattr(cli.subprocess, "run", _refuse_to_run)
    drafted = issue(PAYMENTS)

    cli.main(["--facts", str(_facts_file(tmp_path, drafted)), "--batch-id", drafted.batch_id])

    printed = capsys.readouterr().out.split("NOT POSTED")[-1]
    argv = cli.gh_command(drafted, None)

    assert "`" in render_title(drafted), "the control: the title really does carry backticks"
    assert "--post" in printed
    for element in argv:
        assert repr(element) in printed, f"{element!r} is not named in the printed argv"
    assert " ".join(argv) not in printed, "a joined argv is a shell line, and this one is not"


def test_with_the_flag_it_hands_the_body_to_gh_on_stdin(tmp_path, monkeypatch):
    """`--body-file -` because the body is markdown with backticks, dollars and newlines in
    it. The remedy line alone carries `$(git rev-parse HEAD)`, which a shell would EXECUTE,
    so the body travels as stdin and the command as a list -- both asserted here, and the
    recorder refuses `shell=True` outright."""
    recorder = _Recorder()
    monkeypatch.setattr(cli.subprocess, "run", recorder)
    monkeypatch.setattr(cli.shutil, "which", lambda name: f"/usr/bin/{name}")
    drafted = issue(PAYMENTS)

    code = cli.main(["--facts", str(_facts_file(tmp_path, drafted)),
                     "--batch-id", drafted.batch_id, "--post"])

    assert code == 0
    ((command, body),) = recorder.calls
    assert command[0] == cli.GH
    assert command[:3] == [cli.GH, "issue", "create"]
    assert "--body-file" in command and "-" in command
    assert render_title(drafted) in command
    assert body == render_body(drafted)


def test_it_returns_what_gh_returned(tmp_path, monkeypatch):
    """A failed post must not exit 0. The publisher is the last step before a human reads a
    URL, and a green exit over a 403 is how "the CI path is refused" becomes "the CI path
    was never tried"."""
    monkeypatch.setattr(cli.subprocess, "run", _Recorder(returncode=1))
    monkeypatch.setattr(cli.shutil, "which", lambda name: f"/usr/bin/{name}")
    drafted = issue(PAYMENTS)

    assert cli.main(["--facts", str(_facts_file(tmp_path, drafted)),
                     "--batch-id", drafted.batch_id, "--post"]) == 1


def test_a_missing_gh_is_refused_by_name_and_no_token_path_is_offered(tmp_path, monkeypatch):
    """There is no fallback to the API and there must not be one: a fallback would need the
    credential this whole path exists to avoid. `gh auth` keeps the token, this script never
    reads one, and the refusal says so instead of degrading."""
    monkeypatch.setattr(cli.shutil, "which", lambda name: None)
    monkeypatch.setattr(cli.subprocess, "run", _refuse_to_run)
    drafted = issue(PAYMENTS)

    with pytest.raises(SystemExit, match="is not on PATH"):
        cli.main(["--facts", str(_facts_file(tmp_path, drafted)),
                  "--batch-id", drafted.batch_id, "--post"])


# ----------------------------------------------------------------------------------
# One incident per invocation, chosen explicitly.
# ----------------------------------------------------------------------------------


def test_the_batch_id_is_required_and_there_is_no_arm_that_loops_the_feed(tmp_path):
    """THE ABSENT FLAG IS THE FEATURE. `argparse` refuses the invocation with no
    `--batch-id`, and the second half is what keeps that meaningful: no option anywhere
    selects more than one incident, so "open eleven issues" is not a word away from "open
    one".

    THE OPTION SET IS ENUMERATED AND NOT SWEPT FOR. A source sweep for `--all` was written
    first and went red on the script's own docstring, which says there is no such flag: a
    ban on a STRING cannot tell a flag from a sentence about one. What is asserted instead
    is the whole option set, so any new flag -- however spelled -- fails this test and has
    to be argued for in the same commit."""
    with pytest.raises(SystemExit):
        cli.parse_args(["--facts", str(tmp_path / "nothing.json")])

    options = {
        option
        for action in cli.build_parser()._actions
        for option in action.option_strings
    }
    assert options == {"-h", "--help", "--facts", "--batch-id", "--repo", "--post"}


def test_a_batch_the_file_does_not_hold_is_refused_and_the_message_says_what_it_does(tmp_path):
    """A mistyped id must not publish the wrong incident and must not fail silently. Naming
    what the file holds is what turns a refusal into the next command."""
    path = _facts_file(tmp_path, issue(PAYMENTS), issue(EMPRESAS))

    with pytest.raises(SystemExit, match="no payload for batch 404"):
        cli.main(["--facts", str(path), "--batch-id", "404"])

    with pytest.raises(SystemExit, match=issue(EMPRESAS).batch_id):
        cli.main(["--facts", str(path), "--batch-id", "404"])


def test_two_readings_of_one_incident_are_refused_rather_than_resolved_by_file_order(tmp_path):
    """A facts file that accumulated two runs of one incident holds two payloads for one
    batch. Publishing the first would let file order decide which reading a stranger sees,
    which is not a decision anybody took."""
    path = _facts_file(tmp_path, issue(PAYMENTS), issue(PAYMENTS))

    with pytest.raises(SystemExit, match="2 payloads for batch"):
        cli.main(["--facts", str(path), "--batch-id", issue(PAYMENTS).batch_id])


def test_selecting_from_a_file_of_eleven_still_publishes_one(tmp_path, monkeypatch):
    """Reading a list is not the same permission as publishing one. The producer may write
    the whole feed; this invocation still posts exactly one issue, and the assertion is on
    the number of calls rather than on their content."""
    recorder = _Recorder()
    monkeypatch.setattr(cli.subprocess, "run", recorder)
    monkeypatch.setattr(cli.shutil, "which", lambda name: f"/usr/bin/{name}")
    path = _facts_file(tmp_path, issue(PAYMENTS), issue(EMPRESAS))

    cli.main(["--facts", str(path), "--batch-id", issue(EMPRESAS).batch_id, "--post"])

    assert len(recorder.calls) == 1
    ((command, _),) = recorder.calls
    assert issue(EMPRESAS).batch_id in command[command.index("--title") + 1]


# ----------------------------------------------------------------------------------
# The credential boundary, from both sides.
# ----------------------------------------------------------------------------------


def test_the_publisher_is_not_in_the_wheel_and_no_workspace_module_can_import_it():
    """THE BOUNDARY IS THE POINT OF THE FILE'S LOCATION, and both halves are checked here
    because either alone is satisfiable while the other fails: the wheel could package
    `scripts/` (and then a task could import it), or a `databricks/src` module could grow a
    path hack that loads it from the repository (which the bundle syncs).

    What is NOT checked: that no OTHER path to the GitHub API exists in the workspace code.
    A task that used `requests` and a secret would be invisible to this test, and what
    stands against that is the design argument, not a sweep."""
    packaged = tomllib.loads((_REPO / "pyproject.toml").read_text(encoding="utf-8"))
    wheel = packaged["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]

    assert wheel == ["src/opl"]
    assert not any(Path(entry).parts[0] == "scripts" for entry in wheel)

    importers = [
        path for path in (_REPO / "databricks").rglob("*.py")
        if "open_triage_issue" in path.read_text(encoding="utf-8")
    ]
    assert importers == [], f"{importers} reach the publisher from inside the workspace"


def test_the_publisher_reads_no_token_and_offers_no_way_to_pass_one():
    """The credential stays where `gh auth` keeps it. A `--token` argument or a
    `GITHUB_TOKEN` read would move the secret into this process, into shell history and into
    whatever CI later invoked it -- which is the gate this design exists to avoid opening.

    IT IS A SOURCE SWEEP AND IT IS BLIND TO AN INDIRECT SPELLING: `os.environ[some_name]`
    built at run time carries none of these strings."""
    source = _SCRIPT.read_text(encoding="utf-8")

    for spelling in ("GITHUB_TOKEN", "GH_TOKEN", "--token", "DATABRICKS_TOKEN", "os.environ"):
        assert spelling not in source, f"the publisher names {spelling}"
    assert "subprocess.run" in source and "shell=True" not in source


def test_the_only_command_this_script_can_run_is_gh():
    """One executable, named in one constant, so "what can this thing run" is answerable by
    reading a line rather than by trusting a paragraph.

    THE LAST LINE IS THE CONTROL FOR EVERY OTHER TEST IN THIS FILE: they replace
    `cli.subprocess.run` with a recorder, and if the shipped script carried a stub of its
    own they would all pass over something that never posts. It is the real function here."""
    assert cli.GH == "gh"
    assert cli.gh_command(issue(PAYMENTS), None)[0] == cli.GH
    assert cli.gh_command(issue(PAYMENTS), "owner/name")[-2:] == ["--repo", "owner/name"]
    assert cli.subprocess.run is subprocess.run
