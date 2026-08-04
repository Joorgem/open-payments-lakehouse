# tests/test_assert_deployed_revision_task.py
"""Unit test for the `databricks/src/assert_deployed_revision.py` job task.

WHAT IS UNDER TEST IS WHERE EACH OF THE TWO VALUES COMES FROM. The comparison
itself is `tests/bronze/test_provenance.py`; what this file exists for is trap 1 of
ADR 0009 -- a check that reads both the expected and the actual revision from the
same place passes always, and looks exactly like a working check while doing it.
Every behavioural test below therefore drives the artefact side to a value that is
NEITHER the local `HEAD` nor the value passed in argv, so that a self-comparison
fails here instead of passing quietly in a workspace.

Loaded by path with the same importlib pattern as `tests/test_ensure_masked_table_task.py`
-- the `databricks/src` scripts are job entry points, not part of the opl wheel.
"""
from __future__ import annotations

import ast
import importlib.util
import subprocess
from pathlib import Path

import pytest

from opl.bronze.provenance import SENTINEL_REVISION, WrongRevision

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"
_TASK = "assert_deployed_revision"

# Two object names, neither of them this repository's HEAD. The point of the second
# one is made in the module docstring: a check that fetched the "expected" value from
# the artefact, or the "actual" value from the local repo, would agree with itself
# and never see these.
_DEPLOYED = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_LAUNCHED_FOR = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _task(monkeypatch, deployed: str):
    """The task, with the DEPLOYED artefact reporting `deployed`.

    Patched at the name the task calls rather than at `opl.bronze.provenance`, so a
    task that stopped asking the artefact -- and started deriving the actual revision
    from something it can also derive the expected one from -- would no longer be
    affected by this fixture and would fail the tests below."""
    module = _load(_TASK)
    monkeypatch.setattr(module, "built_revision", lambda: deployed)
    return module


def _head() -> str:
    """This repository's HEAD, which is what a real run passes in argv."""
    return subprocess.run(
        ["git", "-C", str(_REPO), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()


def test_a_run_launched_for_the_revision_the_wheel_carries_passes(monkeypatch, capsys):
    task = _task(monkeypatch, _DEPLOYED)
    task.main([_DEPLOYED])
    out = capsys.readouterr().out
    assert _DEPLOYED in out, "the log does not name the revision that was verified"


def test_the_success_line_records_the_provenance_claim_in_the_run_log(monkeypatch, capsys):
    """The log is the artefact a reviewer reads afterwards. PR A's stale-bundle
    incident was found by reading a task log against the source, and the reason that
    took a human is that no log line named the code that was running."""
    task = _task(monkeypatch, _DEPLOYED)
    task.main([_DEPLOYED])
    assert f"{_TASK}: " in capsys.readouterr().out


def test_the_expected_revision_comes_from_argv_and_the_actual_from_the_ARTEFACT(
    monkeypatch,
):
    """TRAP 1, and the one test in this repo that a plausible simplification breaks.

    The run is launched for this repository's real HEAD -- the exact value the
    documented command `--params revision=$(git rev-parse HEAD)` produces -- while the
    deployed artefact reports something else. That is PR A's second incident in
    miniature: the operator's source moved, the workspace's did not.

    Rewrite `main()` to take both values from the artefact, or both from the local
    repository, and this test goes green-to-red: a self-comparison finds two equal
    values here and reports a clean pass on a run whose code is four commits old."""
    task = _task(monkeypatch, _DEPLOYED)
    with pytest.raises(WrongRevision) as excinfo:
        task.main([_head()])
    message = str(excinfo.value)
    assert _DEPLOYED in message and _head() in message


def test_a_mismatch_refuses_naming_both_revisions(monkeypatch):
    task = _task(monkeypatch, _DEPLOYED)
    with pytest.raises(WrongRevision) as excinfo:
        task.main([_LAUNCHED_FOR])
    message = str(excinfo.value)
    assert _DEPLOYED in message and _LAUNCHED_FOR in message


@pytest.mark.parametrize("argv", [[], [""], [SENTINEL_REVISION], ["   "]])
def test_no_usable_expected_revision_is_refused_rather_than_defaulted(argv, monkeypatch):
    """TRAP 3 at the entry point. The artefact side is deliberately a perfectly good
    object name here, so the only thing that can be refusing is the missing half."""
    task = _task(monkeypatch, _DEPLOYED)
    with pytest.raises(WrongRevision):
        task.main(argv)


def test_an_unstamped_artefact_is_refused_even_for_a_perfectly_good_expected_value(
    monkeypatch,
):
    """The mirror of the test above, and the shape a run gets if the wheel it
    installed was built by some path that does not stamp."""
    task = _task(monkeypatch, "")
    with pytest.raises(WrongRevision):
        task.main([_LAUNCHED_FOR])


def _tree() -> ast.Module:
    return ast.parse((_SRC / f"{_TASK}.py").read_text(encoding="utf-8"), filename=f"{_TASK}.py")


def _imported(tree: ast.Module) -> dict[str, set[str]]:
    """Every module this task imports, mapped to the names taken out of it.

    Over the AST rather than over the text, because the text is mostly prose: this
    repo's docstrings name the mechanisms a module must NOT use in order to explain
    why, and a substring check would refuse the explanation along with the thing."""
    found: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.setdefault(alias.name, set())
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.setdefault(node.module, set()).update(alias.name for alias in node.names)
    return found


def test_the_guard_consults_exactly_two_things_and_nothing_else():
    """TRAPS 1 AND 2 AT ONCE, as the smallest statement that covers both.

    One value comes from `sys.argv`, the other from the deployed wheel through
    `built_revision`, and this task imports NOTHING else -- no `subprocess` to ask a
    repository that does not exist next to the running artefact, no `os.environ`, and
    no `pyspark`, which would mean a serverless session start on every run of every
    guarded job in order to compare two strings.

    The import list is the whole check because it is the whole task. A second source
    of either value is the defect ADR 0009 calls trap 1: expected and actual read from
    the same place agree always, and a check that cannot fail is indistinguishable in
    a green run from one that passed."""
    imported = _imported(_tree())
    assert set(imported) == {"sys", "opl.bronze.provenance"}, (
        f"{_TASK}.py imports {sorted(imported)}. Both values must arrive from outside "
        "this task -- one from argv, one from the deployed wheel -- so anything else "
        "here is either a second source for one of them or work done before refusing"
    )
    assert imported["opl.bronze.provenance"] == {"assert_revision_matches", "built_revision"}, (
        f"{_TASK}.py takes {sorted(imported['opl.bronze.provenance'])} from provenance. "
        "`built_revision` IS the deployed side of the comparison; a task that stopped "
        "calling it is a task comparing the operator's value against itself"
    )


def test_the_entry_point_never_raises_system_exit():
    """Serverless runs these under IPython, where an uncaught `SystemExit` reports a
    SUCCESSFUL run as FAILED. Every task under databricks/src calls a bare `main()`;
    this one is new, so the shape is pinned rather than assumed."""
    source = (_SRC / f"{_TASK}.py").read_text(encoding="utf-8")
    assert "SystemExit" not in source
    assert source.rstrip().endswith('if __name__ == "__main__":\n    main()')


def test_the_smoke_job_reports_the_deployed_revision_without_refusing_anything(
    monkeypatch, capsys
):
    """The other half of ADR 0009's wiring decision, and the reason smoke is the one
    job with no guard: it is the probe you run WHEN YOU SUSPECT THE DEPLOYMENT, so a
    guard on it would remove the diagnostic in the case it exists for. Excluded from
    refusing, not excluded from provenance -- it prints what the deployed wheel says it
    was built from, which is how an operator reads the workspace without failing a job
    to find out.

    RUN, not read. The first version of this test asserted `"built_revision" in
    source`, and a probe that deleted the printed value from the f-string left it green
    -- the import alone satisfied it. So the value is driven to something no literal in
    that file could be and looked for in the output: that also pins WHERE it comes
    from, which is the artefact and not a constant."""
    smoke = _load("smoke")
    monkeypatch.setattr(smoke, "built_revision", lambda: _DEPLOYED)
    smoke.main()
    out = capsys.readouterr().out
    assert _DEPLOYED in out, (
        "smoke.py does not report the deployed revision; it is the only job that can "
        "answer 'what is actually deployed' without refusing, and ADR 0009 excludes it "
        f"from the guard on the strength of that. It printed: {out!r}"
    )
    assert "assert_revision_matches" not in (_SRC / "smoke.py").read_text(encoding="utf-8"), (
        "smoke.py refuses on a revision now -- ADR 0009 excludes it deliberately, and "
        "that exclusion is asserted in tests/test_job_yaml_wiring.py too"
    )
