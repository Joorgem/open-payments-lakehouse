# tests/test_size_caps.py
"""The two size caps this project states, MEASURED, over every tracked Python file.

WHY THIS FILE EXISTS, and it is a count rather than a principle: four separate cap claims
in this phase alone were wrong, and each was wrong in a way prose could not catch.

  1-3. Three tasks claimed compliance on a DOCSTRING-EXCLUDED reading of "50 lines". The
       rule is stated over the function, and a docstring is part of the function -- so the
       three claims were about a different measure than the one being asserted.
    4. One crossing went uncounted entirely. `vault_load_partner_link.main` went from 34
       lines to 51 in Task 2's own fix pass and nobody saw it, because the tree held 33
       over-cap functions before that commit and 33 after: `autoloader._assert_source_dir
       _is_this_months` left the set as `main` entered. A TOTAL is not a cap check. Only a
       named set can see one member swapped for another.

MEASURED AS `end_lineno - lineno + 1` OVER THE AST, which is the measure the rule states
and therefore counts comments, docstrings and blank lines inside the body. Not a heuristic:
the AST's `end_lineno` is the last line of the last statement, so `def` through the final
line inclusive is exactly what a reader counts by eye.

THE ALLOW-LIST IS EXACT IN BOTH DIRECTIONS, which is the only property that keeps it from
becoming a place to put things. An entry naming a function that is no longer over cap FAILS,
and so does one naming a function that no longer exists -- so the list SHRINKS as functions
are fixed, cannot rot behind a rename, and cannot quietly absorb a new crossing. A cap test
whose allow-list grows on demand is worse than no cap test, because it reports green over
exactly the change it exists to refuse.

THE 32 ENTRIES BELOW ARE NOT A BACKLOG THIS FILE IS ASKING ANYONE TO BURN DOWN. F-API ruled
that pre-existing over-cap functions are left alone; two of them (`snapshot.ref_date_column`
at 87 and `config.require_month` at 91) are named in 406667e's own commit body as exactly
that. What this file adds is that the set cannot GROW without the growth being a decision
someone typed out.

TRACKED FILES, VIA GIT, rather than a walk with an exclusion list. "Tracked" is the scope
the caps are stated over, git is the only authority on it, and an exclusion list would have
to learn about every new build or log directory on the day it appeared -- rotting in the
direction that reports green.
"""
from __future__ import annotations

import ast
import subprocess
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]

# The two caps, as the project states them: STRICTLY under, so a function at exactly 50 is
# over and a file at exactly 800 is over.
_FUNCTION_LINES = 50
_FILE_LINES = 800

# (path, function) pairs that are at or over `_FUNCTION_LINES` today, with their measured
# length beside them. Paths are git's own, forward-slashed on every platform.
_OVER_CAP: frozenset[tuple[str, str]] = frozenset(
    {
        ("scripts/backfill_snapshot_columns.py", "_assert_constraints"),  # 67
        ("scripts/backfill_snapshot_columns.py", "fill_and_overwrite"),  # 68
        ("scripts/extract_cnpj.py", "run"),  # 63
        ("scripts/extract_giants.py", "run"),  # 58
        ("scripts/migrate_lookups_to_subdir.py", "main"),  # 68
        ("scripts/migrate_lookups_to_subdir.py", "move_verified"),  # 59
        ("src/opl/bronze/backfill_masks.py", "restore_masks"),  # 71
        ("src/opl/bronze/backfill_prewrite.py", "refuse_contradicting_source_month"),  # 65
        ("src/opl/bronze/backfill_prewrite.py", "refuse_non_empty_quarantine"),  # 66
        ("src/opl/bronze/promote.py", "plan_promotion"),  # 90
        ("src/opl/bronze/promote.py", "promote_batch"),  # 60
        ("src/opl/bronze/provenance.py", "_refuse_the_mismatch"),  # 55
        ("src/opl/bronze/reader.py", "csv_read_options"),  # 56
        ("src/opl/bronze/snapshot.py", "ref_date_column"),  # 87
        ("src/opl/bronze/unzip_volume.py", "_unzip_one"),  # 56
        ("src/opl/config.py", "require_month"),  # 91
        ("src/opl/extraction/landing.py", "upload_to_volume"),  # 83
        ("src/opl/extraction/webdav.py", "download"),  # 86
        ("src/opl/vault/loading.py", "link_hash_key_expression"),  # 51
        ("src/opl/vault/partners.py", "load_partner_link"),  # 50
        ("tests/bronze/test_autoloader_helpers.py",
         "test_no_table_shares_or_nests_state_across_months_or_with_the_orphans"),  # 53
        ("tests/bronze/test_registry.py",
         "test_the_new_tables_carry_a_constraint_no_other_contract_could_have"),  # 55
        ("tests/bronze/test_registry_guard_wiring.py",
         "test_every_guard_any_registry_module_defines_is_actually_called_at_import"),  # 55
        ("tests/bronze/test_registry_guards.py",
         "test_the_registry_still_imports_where_pyspark_is_not_installed"),  # 51
        ("tests/bronze/test_rules.py",
         "test_a_blank_required_column_hides_the_lost_byte_behind_it"),  # 58
        ("tests/gold/test_dim_company.py",
         "test_the_surrogate_key_is_the_same_under_every_session_timezone"),  # 50
        ("tests/test_cnpj_schemas.py", "test_full_column_order_golden"),  # 72
        ("tests/test_gold_entry_points.py",
         "test_each_gold_entry_point_refuses_the_kind_the_other_one_builds"),  # 50
        ("tests/test_month_wiring.py",
         "test_every_consumer_of_the_month_reads_the_one_required_local"),  # 73
        ("tests/test_null_drop_trap.py", "_string_literals"),  # 62
        ("tests/test_revision_stamp.py",
         "test_the_wheel_this_repo_builds_can_be_asked_what_it_was_built_from"),  # 60
        ("tests/vault/test_observation.py", "tables"),  # 59
    }
)


def _tracked() -> tuple[str, ...]:
    """Every tracked `.py` path, as git spells it.

    `check=True` so a git that is absent or angry is a FAILURE rather than an empty sweep
    reporting green over nothing -- which is the shape the last assertion in this file
    guards a second time, from the other side."""
    listed = subprocess.run(
        ["git", "ls-files", "*.py"],
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    return tuple(sorted(path for path in listed.stdout.split("\n") if path.strip()))


def _functions(path: str) -> list[tuple[str, str, int]]:
    """(path, name, length) for every function in `path`, nested ones included.

    `ast.walk` rather than a scan of `tree.body`, because a helper defined INSIDE another
    function is still a function this cap is stated over -- and it is also counted within
    its parent's own length, which is the right answer twice: the parent is long partly
    because the child is."""
    tree = ast.parse((_REPO / path).read_text(encoding="utf-8"), filename=path)
    return [
        (path, node.name, node.end_lineno - node.lineno + 1)
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        and node.end_lineno is not None
    ]


def _measured_over_cap() -> dict[tuple[str, str], int]:
    return {
        (path, name): length
        for path in _tracked()
        for path, name, length in _functions(path)
        if length >= _FUNCTION_LINES
    }


def test_no_function_outside_the_allow_list_is_at_or_over_the_line_cap():
    """A NEW crossing, which is the direction three tasks in this phase got wrong.

    Fix it at the seam -- lift the prose or the argument to module level, or extract a
    named helper, both of which this repository has done repeatedly -- rather than by
    adding a line here. Deleting the explanation to fit is the one repair that is never
    right: the prose is why the function is trusted."""
    measured = _measured_over_cap()
    new = sorted(f"{path}::{name} ({measured[(path, name)]})"
                 for path, name in measured
                 if (path, name) not in _OVER_CAP)
    assert not new, (
        f"{new} are at or over the {_FUNCTION_LINES}-line function cap and are not on the "
        "allow-list. Measured as `end_lineno - lineno + 1` over the AST, which is the "
        "measure the rule states and therefore counts comments and docstrings."
    )


def test_every_allow_list_entry_is_a_function_that_is_still_over_the_cap():
    """THE DIRECTION THAT KEEPS THE LIST FROM ROTTING, and the reason this file is not a
    place to put things.

    An entry whose function was fixed, renamed or deleted fails here, so the list can only
    shrink without an edit and cannot outlive its subject. Without this half, a rename
    would leave a stale entry standing and the renamed function would arrive un-allowed
    anyway -- one crossing reported as two problems, neither of them the real one."""
    measured = _measured_over_cap()
    stale = sorted(f"{path}::{name}" for path, name in _OVER_CAP if (path, name) not in measured)
    assert not stale, (
        f"{stale} are on the over-cap allow-list and are no longer over it -- fixed, "
        "renamed, or gone. Delete the entry: the list shrinks as functions are repaired, "
        "and an entry that outlives its function is how an allow-list starts absorbing "
        "crossings nobody decided on."
    )


def test_no_tracked_python_file_reaches_the_file_cap():
    """The 800-line file cap, with no allow-list at all, because nothing has ever needed
    one: the three files that ran out of room in this phase were SPLIT rather than
    excused."""
    over = sorted(
        f"{path} ({len((_REPO / path).read_text(encoding='utf-8').splitlines())})"
        for path in _tracked()
        if len((_REPO / path).read_text(encoding="utf-8").splitlines()) >= _FILE_LINES
    )
    assert not over, (
        f"{over} are at or over the {_FILE_LINES}-line file cap. Split at a seam and keep "
        "the split commit free of behaviour, which is the discipline every split in this "
        "repository has been done under."
    )


def test_the_sweep_is_reading_the_tree_and_not_an_empty_listing():
    """GUARD THE GUARD, and for a sweep it is the assertion that matters most: all three
    tests above pass trivially over zero files. A git that returned nothing, a pathspec
    that stopped matching, or a `cwd` pointing somewhere else would each report green over
    a tree nobody looked at.

    Floors rather than exact counts, because an exact count here would itself be a claim
    about the repository that goes stale on the next file -- which is the species this file
    exists to catch, and committing it inside the guard would be the joke told twice."""
    tracked = _tracked()
    assert len(tracked) >= 200, f"only {len(tracked)} tracked Python files were found"
    assert "src/opl/config.py" in tracked, "the listing is not this repository's"
    assert sum(len(_functions(path)) for path in tracked) >= 1000
    assert len(_measured_over_cap()) >= 20, "the length measurement returned almost nothing"
