# tests/test_ci_shards.py
"""The CI `test` job is split into shards, and this is what stops a shard losing tests.

WHY THIS FILE EXISTS. On 2026-09-01 the unsharded job was KILLED at GitHub's six-hour
ceiling on a tree that has passed in thirty minutes, so the suite was split four ways. A
split suite has one failure mode that a single job does not: **a test that runs in no shard
at all still reports green**. Nothing in pytest, and nothing in GitHub Actions, notices that
the union of four selections is smaller than it was. That is exactly the shape ADR 0018
names -- a check reporting the expected value because it could not look -- so the partition
is locked here rather than trusted.

THE PARTITION IS EXHAUSTIVE BY CONSTRUCTION AND THIS ASSERTS THE CONSTRUCTION. Three shards
name a directory; the fourth runs `tests` and IGNORES exactly those three. Union is
therefore everything, and a NEW top-level directory joins the fourth shard automatically
instead of running nowhere -- which is the property that matters, because the failure this
file guards is silent and arrives when somebody adds a directory, not when they edit this.

So the one thing that can break it is the ignore list drifting from the named shards, in
either direction:

  - a shard added without a matching `--ignore` -> its tests run TWICE, wasting a runner
    and making a flake twice as likely;
  - an `--ignore` added without a matching shard -> its tests run NOWHERE, and CI goes
    green over a directory nobody executes.

The second is the dangerous one and it is invisible in a green build. Both are one
comparison, below.

WHAT THIS FILE DELIBERATELY DOES NOT DO: collect the four shards and compare the union
against an unsharded collection. That is the direct proof and it was run when the split was
made -- 271 + 292 + 462 + 2,223 = 3,248, with zero missing, zero extra and zero duplicated
-- but it costs five full collections (~3 min) on every CI run of every shard, to re-derive
something the construction already guarantees. Re-run it by hand if the shape of the matrix
ever changes:

    uv run pytest --collect-only -q <shard args>   # for each shard, then compare sets
"""
from __future__ import annotations

from pathlib import Path

import yaml

_WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"
_IGNORE = "--ignore="


def _shards() -> list[dict[str, str]]:
    """The matrix entries of the `test` job, as the workflow declares them."""
    spec = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    include = spec["jobs"]["test"]["strategy"]["matrix"]["include"]
    assert include, "the `test` job declares no matrix; the shard lock has nothing to hold"
    return include


def _named_paths(shard: dict[str, str]) -> list[str]:
    """The paths a shard selects, ignoring its `--ignore=` flags."""
    return [tok for tok in shard["paths"].split() if not tok.startswith(_IGNORE)]


def _ignored_paths(shard: dict[str, str]) -> list[str]:
    """The paths a shard excludes."""
    return [tok[len(_IGNORE):] for tok in shard["paths"].split() if tok.startswith(_IGNORE)]


def test_exactly_one_shard_is_the_catch_all():
    """One shard must select `tests` wholesale, or a new directory runs nowhere."""
    catch_all = [s for s in _shards() if _named_paths(s) == ["tests"]]
    assert len(catch_all) == 1, (
        "exactly one shard must select `tests` and subtract the others; found "
        f"{[s['shard'] for s in catch_all]}"
    )


def test_the_ignore_list_is_exactly_the_other_shards():
    """THE LOCK. Ignore-set and named-shard-set must be equal, in both directions.

    Unequal one way duplicates a directory across two runners; unequal the other way
    drops it from CI entirely while every shard still reports green."""
    shards = _shards()
    catch_all = next(s for s in shards if _named_paths(s) == ["tests"])
    ignored = set(_ignored_paths(catch_all))
    named = {p for s in shards if s is not catch_all for p in _named_paths(s)}
    assert ignored == named, (
        f"ignored but not sharded (these run NOWHERE): {sorted(ignored - named)}; "
        f"sharded but not ignored (these run TWICE): {sorted(named - ignored)}"
    )


def test_only_the_catch_all_subtracts_anything():
    """A second shard carrying `--ignore` would make the union unreadable by inspection."""
    for shard in _shards():
        if _named_paths(shard) == ["tests"]:
            continue
        assert not _ignored_paths(shard), (
            f"shard {shard['shard']!r} both names paths and ignores paths, so the partition "
            "can no longer be read off the matrix"
        )


def test_every_sharded_directory_exists():
    """A renamed directory would silently shrink the suite; the shard would collect zero."""
    root = _WORKFLOW.resolve().parents[2]
    for shard in _shards():
        for path in _named_paths(shard) + _ignored_paths(shard):
            assert (root / path).is_dir(), (
                f"shard {shard['shard']!r} names {path!r}, which is not a directory"
            )


def test_no_shard_runs_the_suite_in_parallel_inside_one_runner():
    """`-n`/xdist is refused here, and the reason is measured rather than stylistic.

    `tests/bronze/test_ptax_rules.py` returned 11 failed / 31 passed with a second local
    Spark suite beside it and 42 passed on an identical rerun alone. Shards exist to put
    Spark suites on DIFFERENT machines; `-n auto` would put them back on one."""
    for shard in _shards():
        tokens = shard["paths"].split()
        assert not any(t == "-n" or t.startswith("-n") for t in tokens), (
            f"shard {shard['shard']!r} asks for xdist, which recreates the contention the "
            "split exists to remove"
        )
