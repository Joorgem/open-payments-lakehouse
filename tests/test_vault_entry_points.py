"""What a vault entry point refuses, and what it must never spell -- read off the
scripts under `databricks/src` with no job YAML anywhere in sight.

SPLIT OUT OF `tests/test_vault_job_wiring.py` BY F-DB TASK 1, at exactly 800 of this
project's 800-line cap, with F-DB's `vault_merchant_job.yml` still to be added to
`_VAULT_JOBS` and to the totality lock. The seam is the one this repository has
already drawn twice and named both times: `test_task_wiring.py` reads the SCRIPTS and
`test_job_yaml_wiring.py` reads the JOB that hands them arguments; `test_gold_entry
_points.py` and `test_gold_job_wiring.py` are the same pair one layer along. The vault
had both halves in one file, and this is that file becoming the pair.

WHICH SIDE A LOCK LANDS ON IS DECIDED BY WHAT MAKES IT CHANGE, not by what it reads.
A new vault JOB -- which is what F-DB adds -- changes the YAML half: every lock there
parametrizes over `_VAULT_JOBS`. A new vault ENTRY POINT changes this half: the two
sweeps below parametrize over `_ENTRY_POINTS`, and the refusals underneath drive a
script's own `main()` with arguments no YAML is consulted for. F-DB adds a job and no
entry point, so the growth pressure is entirely on the other file, which is the whole
reason this one moved out rather than that one.

WHAT IS SHARED, STATED RATHER THAN LEFT TO BE DISCOVERED. `_load` is the five-line
`importlib` idiom eighteen test modules in this suite each carry privately --
`test_gold_entry_points.py` among them -- so a copy here is this repository's existing
practice and not a new second spelling. It is deliberately NOT hoisted into a plain
module the way `tests/job_yaml.py` and `tests/task_ast.py` were: those hold readers
that had drifted or would, and hoisting an idiom that is already eighteen-way
duplicated is a change to eighteen files rather than a consequence of this split.
`_DIAGNOSTICS_SCRIPT` is the other shared name and it is one string: the YAML half
reads it to check a task's arity, this half reads it to learn the parameter name the
loader itself declares.

Nothing here starts Spark, and that is asserted rather than hoped: every refusal
below is reached through `main()` BEFORE `SparkSession.builder.getOrCreate()`, which
is the property `test_the_window_guard_runs_before_the_session_and_lets_two_months
_through` exists to hold."""
from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest

from opl.config import DEFAULT
from opl.vault.job_params import optional_flag

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"

_ENTRY_POINTS = (
    "vault_load_hub",
    "vault_load_satellite",
    "vault_load_link",
    "vault_load_partner_link",
    "vault_load_effectivity",
    "vault_load_reference",
)

# THE ONE ENTRY POINT THAT TAKES A FIFTH PARAMETER, spelled here as well as in
# `test_vault_job_wiring.py` because the two halves ask different things of it: that
# file checks the arity of the task handed to it, this one reads the parameter NAME off
# the script so a test never restates it.
_DIAGNOSTICS_SCRIPT = "vault_load_satellite"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tree(script: str) -> ast.Module:
    return ast.parse((_SRC / f"{script}.py").read_text(encoding="utf-8"), filename=script)


def _non_docstring_strings(tree: ast.Module) -> list[str]:
    """Every string literal in `tree` that is not a docstring.

    The docstrings are excluded for `test_git_is_consulted_at_build_time_and_nowhere_the
    _artefact_runs`' reason: this repository's prose names the thing a module must NOT do
    in order to explain why, and a check over the raw text would refuse the explanation
    along with the thing. Comments never reach the AST, so they are free."""
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.FunctionDef | ast.ClassDef)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    }
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
        and id(node) not in docstrings
    ]


@pytest.mark.parametrize("script", _ENTRY_POINTS)
def test_no_vault_entry_point_spells_a_catalog_or_a_schema(script):
    """The qualification comes from `opl.config.DEFAULT.table` and from nowhere else.

    `opl.vault.registry` states the division these tasks are the other half of: a spec
    carries an unqualified name, the loaders take a qualified table as an argument, and
    `opl.config` is consulted "by whatever calls a loader" -- which, until this branch,
    was nothing at all. A literal `workspace.default.` here would be that consultation
    forked, and Free Edition's single catalog is what would make the fork invisible."""
    qualification = f"{DEFAULT.catalog}.{DEFAULT.schema}."
    spelled = [
        value for value in _non_docstring_strings(_tree(script)) if qualification in value
    ]
    assert not spelled, (
        f"{script}.py spells {qualification!r} in {spelled}. Catalog and schema come "
        "from opl.config.DEFAULT.table(name); a second spelling is a coordinate that "
        "drifts the day this project is on a workspace with more than one catalog"
    )
    qualifies = [
        node for node in ast.walk(_tree(script))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "table"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "DEFAULT"
    ]
    assert qualifies, f"{script}.py never calls DEFAULT.table(...), so it qualifies nothing"


@pytest.mark.parametrize("script", _ENTRY_POINTS)
def test_no_vault_entry_point_raises_system_exit(script):
    """Serverless runs these under IPython, where an uncaught `SystemExit` reports a
    SUCCESSFUL run as FAILED. Every task under databricks/src calls a bare `main()`;
    these six are new, so the shape is pinned rather than assumed."""
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    assert "SystemExit" not in source
    assert source.rstrip().endswith('if __name__ == "__main__":\n    main()')


# --- the window that cannot close a window -------------------------------------------
#
# The tension F2 wave 2 names, and the guard that closes it. `observation_ledger`
# derives its key universe from the same window it reports on, so over ONE month every
# key it knows about is present in the only month it is asked about and no key can reach
# `absent_after_observation` -- the one state that closes an effectivity window. A
# one-month window therefore closes ZERO windows for any data, always, and reports
# success doing it. `tests/vault/test_effectivity_window.py` measures that zero against
# real Spark; what is pinned here is that the ENTRY POINT refuses the window rather than
# running it. That the job YAMLs cannot hand it one silently is `test_vault_job_wiring
# .py`'s `test_the_months_default_refuses_rather_than_naming_a_window_nobody_chose`, and
# the two are the launch and the argument halves of one refusal.

_A_GOOD_LOAD_DATE = "2026-08-09T12:00:00"


def test_the_effectivity_task_refuses_a_window_too_narrow_to_close_anything():
    """The guard, driven through `main` rather than called directly, so what is pinned
    is that it is ON the path and not merely present in the file."""
    task = _load("vault_load_effectivity")
    with pytest.raises(ValueError, match="closes a window on absence"):
        task.main(["sat_eff_company_partner", "socios", "2026-07", _A_GOOD_LOAD_DATE])


def test_the_window_guard_runs_before_the_session_and_lets_two_months_through():
    """BOTH HALVES IN ONE RUN, and neither is reachable any other way without Spark.

    Two months and a load date that cannot parse: the failure must be the LOAD DATE's,
    which proves the window guard passed on two months -- and it proves the guard stands
    ahead of `SparkSession.builder.getOrCreate()`, because a guard after the session
    would start one to reject an argument. A refusal that costs a serverless start is a
    refusal an operator learns to route around."""
    task = _load("vault_load_effectivity")
    with pytest.raises(ValueError, match="ISO-8601"):
        task.main(["sat_eff_company_partner", "socios", "2026-06+2026-07", ""])


def test_a_repeated_month_cannot_inflate_the_window_past_the_guard():
    """`months=2026-07+2026-07` is two entries and ONE month.

    The ledger folds a duplicate away and answers the same, so nothing in the library
    cares -- which is exactly why the refusal lives in `opl.vault.job_params` and why it
    is worth a test of its own. The guard above measures narrowness by COUNTING months;
    admitted, this typo would carry a one-month window straight past it and back into the
    zero-closes load."""
    task = _load("vault_load_effectivity")
    with pytest.raises(ValueError, match="more than once"):
        task.main(["sat_eff_company_partner", "socios", "2026-07+2026-07", _A_GOOD_LOAD_DATE])


def test_a_task_handed_no_diagnostics_flag_runs_the_cheap_load_rather_than_refusing():
    """THE ONE ABSENT JOB PARAMETER THIS PACKAGE DEFAULTS INSTEAD OF REFUSING, asserted
    because it is the exception to everything above it in this section. A missing window
    is refused: every default is a load nobody chose. A missing FLAG has a default that
    claims LESS rather than something wrong -- neither diagnostic is measured and the
    result says `None`, which nothing can read as a zero. Absence has to keep working
    besides: `test_an_entry_point_handed_a_table_of_the_wrong_kind_refuses_before_spark`
    drives `main` with four arguments, as does any operator's older launch command."""
    name = _load(_DIAGNOSTICS_SCRIPT).DIAGNOSTICS_PARAMETER

    assert optional_flag(None, parameter=name) is False
    assert optional_flag("", parameter=name) is False
    assert optional_flag("false", parameter=name) is False
    assert optional_flag("true", parameter=name) is True
    assert optional_flag(" TRUE ", parameter=name) is True


def test_a_diagnostics_flag_the_parser_cannot_read_is_refused_and_not_read_as_off():
    """`report_diagnostics=yes` is an operator ASKING for the measurement.

    Defaulted, their run comes back with `None` in both fields -- byte-identical to the
    run they were trying not to launch -- and there is nothing in the log to say the
    parameter was ignored. The refusal costs a relaunch; the default costs the
    measurement they came for."""
    with pytest.raises(ValueError, match="report_diagnostics='yes'"):
        optional_flag("yes", parameter="report_diagnostics")


@pytest.mark.parametrize(
    "script,table",
    [
        ("vault_load_hub", "sat_empresa_dados"),
        ("vault_load_satellite", "hub_empresa"),
        ("vault_load_link", "ref_cnae"),
        ("vault_load_effectivity", "link_company_partner"),
        ("vault_load_reference", "sat_eff_company_partner"),
    ],
)
def test_an_entry_point_handed_a_table_of_the_wrong_kind_refuses_before_spark(script, table):
    """The refusal that makes the YAML lock in `test_vault_job_wiring.py` more than a
    style rule.

    `domains.table_spec` refuses a name no domain registers; what it cannot refuse is a
    REGISTERED table of the wrong kind, which is what a copied task produces. Without
    this the mistake arrives as an `AttributeError` inside Spark's analysis, naming a
    dataclass field rather than a table."""
    task = _load(script)
    with pytest.raises(ValueError, match="was handed vault table"):
        task.main([table, "empresas", "2026-06+2026-07", _A_GOOD_LOAD_DATE])
