# tests/bronze/test_provenance.py
"""The comparison behind `databricks/src/assert_deployed_revision.py`.

Pure: two strings in, a verified revision or a refusal out. No Spark, no
filesystem, no subprocess -- which is itself part of what is under test, because
the one thing this comparison must never do is go and find the answer for itself.
Both values have to arrive from OUTSIDE, from two different places, or the check
compares an artefact against itself and passes always (ADR 0009).

The wiring half -- that the deployed side really is the deployed side, and that the
guard runs before any task does work -- is `tests/test_assert_deployed_revision_task.py`
and `tests/test_job_yaml_wiring.py`. Neither half is a provenance claim alone."""
from __future__ import annotations

import pytest

from opl.bronze.provenance import (
    SENTINEL_REVISION,
    WrongRevision,
    assert_revision_matches,
    built_revision,
    is_object_name,
)

# Two real-shaped object names that are not each other. Spelled out rather than
# generated: a test that hashed something to get them would agree with a check that
# hashed the same thing.
_A = "62ce88003113dc1ca198b19cfd00f5f5e20b9bd3"
_B = "1cf3ea2aa1f4b5c6d7e8f9001122334455667788"


def test_a_matching_revision_is_accepted_and_returns_what_it_verified():
    """The value is returned, not merely approved, so the caller can print the
    revision it proved rather than the one it was handed -- the two are the same
    only when the check passed, which is exactly when the log is worth reading."""
    assert assert_revision_matches(expected=_A, actual=_A) == _A


def test_case_and_surrounding_whitespace_do_not_decide_a_run():
    """`git rev-parse HEAD` answers in lower case, so this is tolerance for a
    hand-pasted value and nothing more. A shell that hands the parameter through
    with a trailing newline must not produce a provenance failure that reads
    identically to a stale deployment."""
    assert assert_revision_matches(expected=f"  {_A.upper()}\n", actual=_A) == _A


@pytest.mark.parametrize(
    "expected",
    [None, "", "   ", SENTINEL_REVISION, "HEAD", "main", _A[:7], _A[:39], _A + "0", "zz" + _A[2:]],
)
def test_an_expected_revision_that_is_not_a_whole_object_name_refuses(expected):
    """TRAP 3, and the sentinel is the case that actually happens.

    Every job that carries the guard declares `revision:` with a default the guard
    refuses, because a job parameter must have SOME default. Treating an absent or
    unusable expected value as "nothing to check here" would make the check absent
    in precisely the situation it exists for: a run launched by a path that forgot
    `--params revision=...`.

    The abbreviation is refused for a stated reason rather than for strictness: the
    two values are compared for equality, and accepting a prefix means deciding how
    short a prefix is still evidence."""
    with pytest.raises(WrongRevision) as excinfo:
        assert_revision_matches(expected=expected, actual=_A)
    assert "revision" in str(excinfo.value)


@pytest.mark.parametrize("actual", [None, "", "   "])
def test_an_artefact_that_does_not_say_what_it_was_built_from_refuses(actual):
    """The local-install case, and the built-without-git case, and any future path
    that produces a wheel with no stamp. All three mean the same thing: this run
    cannot show which source it came from, which is not a weaker version of a pass."""
    with pytest.raises(WrongRevision) as excinfo:
        assert_revision_matches(expected=_A, actual=actual)
    message = str(excinfo.value)
    assert "bundle deploy" in message, "the refusal does not say how to get a stamped wheel"
    assert _A in message


def test_a_mismatch_names_BOTH_revisions_and_says_which_is_which():
    """The message is the deliverable here. PR A's stale-bundle incident was found by
    a human reading a task log against the source; a refusal that named only one side
    would leave the next person doing exactly that."""
    with pytest.raises(WrongRevision) as excinfo:
        assert_revision_matches(expected=_A, actual=_B)
    message = str(excinfo.value)
    assert _A in message and _B in message
    assert "bundle deploy" in message


def test_a_wheel_built_from_a_modified_tree_is_refused_though_it_names_this_commit():
    """`git rev-parse HEAD` answers the same in a dirty tree as in a clean one, so a
    bare SHA from a modified tree is a claim about code that was never committed. The
    build stamps such a wheel with something no expected value can equal (ADR 0009),
    and the refusal has to explain that rather than reading as a stale deployment --
    the fix is `git commit`, not `bundle deploy`."""
    with pytest.raises(WrongRevision) as excinfo:
        assert_revision_matches(expected=_A, actual=f"{_A}+dirty")
    message = str(excinfo.value)
    assert _A in message
    assert "uncommitted" in message


def test_a_stamp_that_is_not_an_object_name_at_all_refuses_quoting_it():
    """Not reachable from today's build hook, and asserted anyway: whatever ends up in
    the artefact is data this function did not write, and the one response that is
    never right is to accept it."""
    with pytest.raises(WrongRevision) as excinfo:
        assert_revision_matches(expected=_A, actual="built-by-hand")
    assert "built-by-hand" in str(excinfo.value)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (_A, True),
        (_A.upper(), True),
        ("0" * 64, True),  # a SHA-256 object name, for a repo that ever uses one
        ("0" * 39, False),
        ("0" * 41, False),
        ("0" * 63, False),
        (f"{_A}+dirty", False),
        (SENTINEL_REVISION, False),
        ("", False),
        (None, False),
    ],
)
def test_what_counts_as_a_whole_object_name(value, expected):
    """ONE spelling of the rule, because two spellings of one rule is how `2026-13`
    came to be refused at two of four entry points (see `opl.config.is_month`). The
    job YAMLs' `revision:` default is checked against THIS predicate by
    `tests/test_job_yaml_wiring.py`, so a default that would silently pass a run
    fails a test here rather than in the workspace."""
    assert is_object_name(value) is expected


def test_this_tree_carries_no_stamp_so_a_local_install_cannot_satisfy_the_check():
    """CONSTRAINT 3 WITH TEETH, asserted against the environment this suite is
    actually running in.

    The stamp is generated into the WHEEL by the build hook, and deliberately not
    into an editable install (ADR 0009: the tree an editable install points at
    changes under it, so any revision stamped there is stale by construction).
    So `uv run pytest` sees no stamp -- and the check must refuse rather than treat a
    developer's laptop as a deployment that proved something.

    IF THIS TEST EVER GOES RED, read it before fixing it: it means the suite is
    running against a built wheel rather than the working tree, and every other test
    in this file is then measuring a different artefact than the one being edited."""
    assert built_revision() == "", (
        "this tree reports a built revision. Either a generated src/opl/_revision.py "
        "has been committed -- it must not be, it is a build output -- or the suite is "
        "running against an installed wheel instead of the working tree"
    )
    with pytest.raises(WrongRevision):
        assert_revision_matches(expected=_A, actual=built_revision())
