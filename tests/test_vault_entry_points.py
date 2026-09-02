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

WHAT IS SHARED, AND THE JUDGEMENT THIS PARAGRAPH USED TO MAKE HAS BEEN OVERTURNED BY
ITS OWN CRITERION. Every reader that FINDS or READS a script now lives in
`tests/loader_scripts.py`, a plain module, and is imported back under the name used
here, so no call site moved (`tests/adr_files.py`'s pattern). What this paragraph argued
was that `_load` should not be hoisted, because hoisting an idiom twelve modules carry
privately is a change to twelve files rather than a consequence of a split -- still
true, and why those twelve keep their copies. What changed is that this file's readers
stopped BEING that idiom: the routing locks grew AST readers for a script's `main`, its
loader call, its parent resolution and the kind it accepts, and `task_ast.py`'s rule
then applied word for word -- two copies of the assertion that makes a lock a lock, and
the copy that rots is the one whose message nobody has read in a year. At 850 lines
against an 800 cap the split was owed regardless (master protocol section 4.12), and THE
NEXT SEAM IS ALREADY NAMED at the foot of this docstring: the registry-parametrized
section is the part of this file that changes when a TABLE arrives rather than when a
script does.

THE COUNTS IN THE PARAGRAPH THIS REPLACES ARE LEFT AS F-DB'S MEASUREMENT AND NOT
RE-ASSERTED HERE, because this round did not re-run them and a number nobody re-derived
is what half of F2 wave 2's findings were about. As F-DB measured it -- its own first
spelling said "eighteen" and reconciled under no reading -- 19 test modules referenced
`spec_from_file_location`, 12 wrapped it in a helper, 10 of those called it `_load`, and
the helpers ran 5 to 12 lines rather than five. What THIS round measured is only the
delta the hoist makes: one spelling moved out of a test module and into a plain one, so
the number of copies is unchanged and the number of test modules carrying one is one
lower.
`_DIAGNOSTICS_SCRIPT` is the other shared name and it is one string: the YAML half
reads it to check a task's arity, this half reads it to learn the parameter name the
loader itself declares.

Nothing here starts Spark, and that is asserted rather than hoped: every refusal
below is reached through `main()` BEFORE `SparkSession.builder.getOrCreate()`, which
is the property `test_the_window_guard_runs_before_the_session_and_lets_two_months
_through` exists to hold.

AND THE LAST SECTION OF THIS FILE PARAMETRIZES OVER THE REGISTRY RATHER THAN OVER
`_ENTRY_POINTS`, which is a departure from the sentence above about what decides which
side a lock lands on. It is still a lock about the SCRIPTS -- what it asks is which
script can run a registered table and whether that script can key on that table's
parent -- but its population has to be every satellite that EXISTS rather than every
entry point someone remembered to list, because the defect it exists to catch arrived
as a new TABLE and not as a new script. Its argument is the comment block above it."""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
from loader_scripts import SRC as _SRC
from loader_scripts import all_scripts as _loader_scripts
from loader_scripts import exposing_the_seam as _scripts_exposing_the_seam
from loader_scripts import load as _load
from loader_scripts import loader_call_in_main as _loader_call_in_main
from loader_scripts import non_docstring_strings as _non_docstring_strings
from loader_scripts import parent_resolver_of as _parent_resolver_of
from loader_scripts import resolution_and_session_lines as _resolution_and_session_lines
from loader_scripts import the_one_accepting as _the_one_script_accepting
from loader_scripts import tree_of as _tree

from opl.bronze.registry import REGISTRY as BRONZE_REGISTRY
from opl.config import DEFAULT
from opl.vault import domains
from opl.vault.job_params import optional_flag
from opl.vault.registry import EffectivitySatellite, Satellite
from opl.vault.satellite_grain import snapshot_axis_for
from opl.vault.satellites import _resolved_parent

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
# The tension F2 wave 1's workspace run names, and the guard that closes it. `observation_ledger`
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


# --- what a TRANSACTIONAL load may say about a count it never had ---------------------
#
# HERE AND NOT IN `tests/vault/test_satellite_diagnostics.py`, where this loader's other
# printed lines are pinned: that is a Spark module and these claims are about the text two
# pure functions return. Its `main` assertion is still the idiom they copy. The state is
# the SCRIPT'S OWN `SatelliteLoadResult` (`loader_scripts.kind_accepted_by`'s reason),
# whose
# `__post_init__` -- not this file -- is what makes `candidate_departures` `None`.

_A_ONE_MONTH_WINDOW = ("2026-07",)
_A_TWO_MONTH_WINDOW = ("2026-06", "2026-07")


def _transactional_result(task, collapsed: int | None):
    """What `load_satellite` returns for a satellite that derives no observation ledger."""
    return task.SatelliteLoadResult(
        table="sat_link_payment", appended=3, already_present=0,
        collapsed_duplicates=collapsed, candidate_departures=None, ledger_derived=False,
    )


def test_a_transactional_load_never_renders_a_departure_count_it_never_had():
    """`_departure_note`'s third arm, driven over BOTH window widths, because the
    sentence it replaces differs at each: over one month the second arm gives "None
    candidate departures, which is ZERO BY CONSTRUCTION over a one-month window", over two
    the third gives "None candidate departures (absent_after_observation, never
    asserted)". Both are claims about a ledger the load never built, and a test pinned to
    one would stay green over the arm being narrowed to the other."""
    task = _load(_DIAGNOSTICS_SCRIPT)
    result = _transactional_result(task, None)

    for months in (_A_ONE_MONTH_WINDOW, _A_TWO_MONTH_WINDOW):
        note = task._departure_note(months, result)
        assert "None" not in note, f"the departure note renders a null count: {note!r}"
        assert "TRANSACTIONAL" in note, (
            f"over {months} a ledgerless load prints {note!r}, which does not say why "
            "there is no count -- so a reader meets a missing number with no reason"
        )
    assert "_departure_note(months, result)" in Path(task.__file__).read_text(
        encoding="utf-8"
    ), "nothing prints through _departure_note any more, so this test measures nothing"


def test_the_line_an_unflagged_transactional_load_prints_promises_only_what_it_can():
    """THE ARM AN UNFLAGGED RUN ACTUALLY REACHES, which is the one the first fix missed.

    `report_diagnostics` defaults to false, so the SKIP is what a first `sat_link_payment`
    load prints -- and the ledger-bearing skip says NEITHER diagnostic was measured, that
    EACH costs a pass, and to re-run to measure THEM. All three are wrong for a
    transactional satellite, whose departure count exists at no setting of the flag, and
    an operator who follows it spends a pass for one of the two.

    THE CONTROL LINE IS THE MEASUREMENT, AND IT MUST DIFFER BY `ledger_derived` ALONE. It
    first ran on a TWO-month window while the transactional call ran on one, so the pair
    differed by the flag AND by the window and the discriminator was locked by neither:
    rewriting the branch to key on `len(months) < MONTHS_AN_ABSENCE_NEEDS` left this file
    green, and under that a ledger-bearing `sat_empresa_dados` over one month prints the
    fold-count line, the "ONLY count this flag buys on a transactional satellite" claim,
    and a rendered `None` -- the exact sentence `_departure_note`'s third arm exists to
    keep out of a task log. Both calls now take `_A_ONE_MONTH_WINDOW`."""
    task = _load(_DIAGNOSTICS_SCRIPT)
    result = _transactional_result(task, None)
    skipped = task._diagnostics_note(_A_ONE_MONTH_WINDOW, result)

    assert not skipped.startswith("NEITHER DIAGNOSTIC WAS MEASURED"), (
        "a transactional load prints the ledger-bearing skip line, which promises a "
        f"departure count no setting of {task.DIAGNOSTICS_PARAMETER} can produce"
    )
    assert task.DIAGNOSTICS_PARAMETER in skipped
    assert not re.search(r"\d", skipped), f"the skipped line carries a number: {skipped!r}"
    assert task._departure_note(_A_ONE_MONTH_WINDOW, result) in skipped, (
        "the skip line does not carry the departure note's own words, so what an "
        "operator reads and what _departure_note says can drift apart"
    )
    ledger_bearing = task._diagnostics_note(_A_ONE_MONTH_WINDOW, task.SatelliteLoadResult(
        table="sat_empresa_dados", appended=3, already_present=0,
        collapsed_duplicates=None, candidate_departures=None,
    ))
    assert ledger_bearing.startswith("NEITHER DIAGNOSTIC WAS MEASURED")


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


# --------------------------------------------------------------------------------
# The snapshot axis reaches the window validator, or the source is refused
# --------------------------------------------------------------------------------


@pytest.mark.parametrize("script", _ENTRY_POINTS)
def test_every_entry_point_resolving_a_source_honours_that_sources_axis(script):
    """Resolve a `BronzeTable` and you owe its axis to `required_months`, or a refusal.

    THE HOLE THIS CLOSES WAS FOUND BY REVIEW, NOT BY A TEST, WHICH IS WHY IT IS HERE.
    F-DB Task 2 made the snapshot axis a per-source declaration and wired it into two of
    the six entry points; the other four resolved the source two lines earlier and then
    called `required_months` without it. The consequence is not a mistyped argument -- it
    is that `hub_merchant` and `link_merchant_empresa`, which Task 5 loads through
    `vault_load_hub` and `vault_load_link`, were refused before Spark by the very refusal
    the axis exists to lift.

    AND THE SILENT HALF IS WORSE THAN THE LOUD ONE. Every bronze row carries
    `_snapshot_month` whatever its source declares, because
    `autoloader.add_common_audit_columns` stamps it unconditionally. So a month-shaped
    window handed to a non-monthly source is not refused at all: it validates, and
    `read_snapshot_window` then folds on a column the source does not key on. A test that
    only checked the loud direction would have passed on that.

    TWO WAYS TO SATISFY THIS, because two loaders cannot honour an axis at all.
    `load_partner_link` and `load_reference_table` take no `axis` and fold on
    `_snapshot_month` throughout, so their entry points REFUSE a non-monthly source
    instead -- `fetch_ptax._refuse_a_table_this_does_not_fetch`'s discipline, applied to
    the axis. Either discharges the obligation; neither may be silently absent."""
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    assert "bronze_table_spec(" in source, (
        f"{script} no longer resolves a bronze source, so this sweep's premise moved. "
        "Re-read the script before deleting the case."
    )
    carries_axis = "axis=axis" in source or "axis=source.snapshot_axis" in source
    refuses_non_monthly = "snapshot_axis != MONTHLY_SNAPSHOT" in source
    assert carries_axis or refuses_non_monthly, (
        f"{script} resolves a BronzeTable and then drops its snapshot axis. Pass "
        "`axis=source.snapshot_axis` into `required_months` (and into the loader, if it "
        "takes one), or refuse a source whose axis this loader cannot honour. Defaulting "
        "is the worst of the three: every bronze row carries `_snapshot_month` whatever "
        "its source declares, so the window validates and the fold reads the wrong column."
    )


# --- EVERY REGISTERED SATELLITE REACHES A LOADER THAT ACCEPTS ITS PARENT'S KIND -------
#
# WHY THIS ARGUMENT IS AT MODULE LEVEL. It belongs to two sweeps rather than one, and
# inside either docstring it puts that function past this project's `< 50 lines
# INCLUDING comments` cap (master protocol section 4.9); the block above
# `opl.vault.satellite_grain.snapshot_axis_for` is the precedent. Each docstring below
# keeps the sentence a reader of that particular sweep needs.
#
# THE GREEN OF THE FIRST SWEEP IS NOT ITS EVIDENCE, AND A LATER READER MUST NOT TAKE IT
# AS ONE. Until F2 wave 2 the KIND of a satellite and the KIND of its parent were
# perfectly correlated in this repository: every `Satellite` hung off a HUB and every
# `EffectivitySatellite` off a LINK, six tables and no exception. Measured on a sibling
# branch carrying all six and not `sat_link_payment`, `domains.parent_hub` answers for
# every `Satellite` and refuses every `EffectivitySatellite` -- so ROUTING BY KIND and
# ROUTING BY PARENT are indistinguishable there and this sweep passes on such a tree
# whether or not the mechanism works. It would pass because the correlation holds. That
# is this project's own defect species, the expected value produced by something other
# than the property asserted, and it is how the assumption survived six tables and four
# loaders without anyone noticing it had been made.
#
# SO THE EVIDENCE IS THE RED UNDER MUTATION, AND THE MUTATION THAT COUNTS IS ONE OVER A
# TABLE WHOSE KIND AND PARENT AGREE. Repointing `databricks/src/vault_load_satellite.py`
# at `domains.parent_link` leaves `sat_link_payment` GREEN -- its parent really is a link
# -- and reddens the four hub-parented satellites: that is this sweep answering about the
# RULE. Repointing it back at `domains.parent_hub` reddens `sat_link_payment` alone,
# which is it answering about today's gap. Both were run before this was believed, and
# neither on its own would have said what the sweep is worth.
#
# WHAT IT IS FOR, STATED AS THE FAILURE IT PREVENTS. `sat_link_payment` has no job task,
# so `tests/test_vault_job_wiring.py`'s totality lock is red for it -- AND THE 403 IS NOT
# WHY. `databricks/resources/` can gain a YAML on any day: the workspace refuses the
# DEPLOY of a new job, not the writing of a file. The two reasons are that the directory
# is F8's area this phase, and that the one task this phase could otherwise ship is the
# dodge its own plan rules out -- hanging the loader off an EXISTING job rather than
# declaring a new one, which is what the 403 refuses, and which would outlive the outage
# with nobody able to see why. A reader who takes the 403 for the cause writes the file
# the day it clears. The day someone
# adds that task, THAT red clears, the entry-point sweeps above pass, the source lock
# passes and `_grains_the_jobs_build` skips it on `spec.transactional` -- a fully green
# suite over a task that raises `ValueError` on the cluster, because the script resolved
# its parent with a function that refuses a link-parented satellite by name. This sweep
# is what makes that a red instead, and it is red the moment the SCRIPT regresses rather
# than only once a YAML exists.
#
# IT MUST ALSO FIRE ON THE SHAPE NOBODY HAS WRITTEN YET, which is why the second sweep
# exists beside the first. Resolving a parent without raising is not the whole of "this
# loader can run this table": a STATE satellite on a link -- legal DV2, refused today at
# import by `opl.vault.registry_satellites._refuse_a_transactionality_the_parent_does_
# not_support`, and named there as a deferral rather than a rule -- would resolve its
# parent perfectly well and then be refused by `snapshot_axis_for` for having no grain.
# The second sweep drives the script's OWN `parent_arguments` through that gate, so the
# day the deferral is lifted the pairing is checked rather than assumed.
#
# THE POPULATIONS ARE DERIVED AND NOT LISTED. The scripts come from a glob over
# `databricks/src`, the accepted kind from each script's own `required_spec` call, the
# resolver from its own `domains.parent_*` call, the tables from `domains.REGISTRY` and
# the sources from `opl.bronze.registry.REGISTRY`. F8's framing of the failure this
# avoids: "the population a check runs over is chosen by hand somewhere nobody
# re-derives" -- and a table added INSIDE the change is in no list anyone enumerated.
#
# BOTH GATES, IN THE ORDER `load_satellite` REACHES THEM, BECAUSE ONE OF THEM READS NO
# `hubs=` AT ALL. `snapshot_axis_for` decides a window from the satellite, the parent, the
# grain and the axis, never from the link's hubs -- so under it alone
# `"hubs": domains.linked_hubs(parent)` could be `()` and the sweep still passed, measured
# at zero tests. `opl.vault.links.refuse_mismatched_hubs` is what catches that, reached
# through `opl.vault.satellites._resolved_parent`, which runs FIRST in `load_satellite`
# and applies four refusals: neither or both of `hub=` and `link=`, a `hubs=` beside a
# hub, a parent the satellite does not declare, and the hub list itself. The second sweep
# drives that gate and then the axis one, so every key built is read by something.
#
# THE PRIVATE IMPORT IS A CHOICE AND NOT AN ACCIDENT. `_resolved_parent` has no public
# spelling and is literally `load_satellite`'s first statement. Restating its four
# refusals here would be the two-spellings defect this file exists to catch -- the copy
# that rots is the one whose failure message nobody has read in a year -- so the coupling
# to a private is taken deliberately, and the day it moves this sweep fails at import
# rather than going on asserting about a paraphrase of it.
#
# THE LOADER CALL IS DERIVED FROM THE SESSION AND NOT NAMED. `_loader_call_in_main` could
# have said `load_satellite`; that would be a hand-written population of one, in the file
# whose own paragraph above says the populations are derived and not listed, and it would
# pass unchanged over a script repointed at some other function. The session is the
# derived handle, and it is the SAME fact the ordering lock turns on, read from the other
# side: the parent is resolved BEFORE the session, and what the call that TAKES the
# session is handed must be that resolution's answer. The cost is that a `main` handing
# its session to two calls is refused rather than guessed at -- this file's standing
# discipline, and the same one `loader_scripts.kind_accepted_by` and
# `parent_resolver_of` keep.
#
# WHAT "SIX REFUSALS" COUNTS, since a gloss in the sweep's docstring got this wrong in
# the direction of precision and had to be struck. Measured by AST over
# `opl/vault/satellite_grain.py`: `snapshot_axis_for` raises FOUR times itself and
# delegates two more to `_transactional_axis`, which is where the repo's own "six" comes
# from. The struck gloss said the six were "its own", and excluded
# `_refuse_a_mismatched_grain`'s raises on a delegation test that `_transactional_axis`
# would have failed identically. For the record, that path carries three: two of its own
# and one in `_refuse_a_prefixed_hub_grain`.
#
# WHAT THE SEAM LOCK BELOW COVERS, AND THE THREE CLASSES IT DOES NOT -- DECLARED RATHER
# THAN IMPLIED, because the sentence that used to stand here promised a guarantee the
# lock cannot give. It said `task_ast.locals_of` refuses "a rebind between the seam and
# the loader", and glossed the lock as proving the name "bound once". It does not:
# `locals_of` collects `ast.Assign` with bare-name targets, so a NAME rebound is caught
# and a DICT MUTATED IN PLACE is not.
#
# COVERED, each driven as a construction and measured red here: re-inlining the dict;
# rebinding the name; and splatting the seam into a throwaway `dict()` while handing the
# loader its parent by hand. An independent review drove twelve constructions and
# reported seven red, four of them beyond these three (a session alias, a second call
# taking the session, a `dict()` wrapper and two conditional bindings); that count is the
# review's and is attributed rather than re-derived here.
#
# NOT COVERED (a), AND THIS ONE IS SILENT, WHICH IS THE WHOLE REASON IT IS WRITTEN DOWN.
# In-place mutation of the dict the seam returns -- `|=`, item assignment, `.update()` --
# is structurally invisible to `locals_of`. MEASURED: inserting
# `arguments |= {"axis": bronze_table_spec("merchant").snapshot_axis}` between the seam
# call and the loader leaves this file at 76 passed / 14 skipped, exit 0, byte-identical
# to the honest headline -- and NEITHER gate refuses it, because `_resolved_parent` never
# reads the axis and `_transactional_axis` returns the axis it is handed without checking
# it against `source_table`. Driven directly, `snapshot_axis_for` then answers
# `_snapshot_at` while the payments source declares `_snapshot_month`. The mirror
# direction is the one the axis sweep above already calls the worse half: a monthly axis
# on a non-monthly source VALIDATES, because `_snapshot_month` is stamped on every bronze
# row whatever its source declares, and the fold then reads a column nothing keys on.
#
# NOT COVERED (b). The lock never inspects the seam call's ARGUMENTS, so
# `parent_arguments(parent, <some other bronze spec>)` is green. It needs a second bronze
# spec in scope, which today's `main` has not got -- it binds exactly one, `source` -- but
# `bronze_table_spec` is imported there and one line would supply another.
#
# NOT COVERED (c), AND IT IS LOUD, WHICH IS WHY IT COSTS LESS THAN (a). An extra explicit
# keyword beside the splat -- `**arguments, hub=parent` -- passes the lock, measured at 76
# passed / exit 0. On a link parent `_resolved_parent` raises `ValueError`; on a hub
# parent the call raises `TypeError`. Either way a run fails rather than reporting
# success, which is the difference between (c) and (a).
#
# AND THE LOCK IS LEFT EXACTLY AS IT IS, DELIBERATELY. It has been strengthened twice
# already -- it began asserting nothing about `main` at all, then asserted a binding and
# a splat without asking what was splatted INTO. A third strengthening pass on one lock
# is this project's stop condition for "the model of the problem is wrong", and what the
# third review actually found was a FALSE SENTENCE rather than a weak assertion. Striking
# the sentence and declaring the gaps removes the defect that existed; a fourth iteration
# would not, and would cost the phase a stop.
#
# THE SECOND SWEEP IS AT 46 LINES OF THE STRICTLY-UNDER-50 FUNCTION CAP, MEASURED (it
# was 47 until the false gloss above came out of its docstring), so
# the next thing added to it moves something out first -- and this block is where that
# something goes. Recorded here rather than in its docstring because the first draft of
# this warning went there and took the function to 51, which is the warning proving
# itself at its own expense.
#
# EVERY BRONZE SOURCE, AND NOT THE ONE A YAML NAMES. The second sweep pairs each
# satellite with every registered bronze table, because the question it asks is about
# KINDS and must not be answerable by the source a job happens to hand the task -- which
# is exactly the thing `sat_link_payment` does not have. The axis in the answer still
# varies with the source (`merchant` is observed at instants and the rest monthly), which
# is what says the sweep is reading the source rather than ignoring it.


def test_the_hand_written_entry_point_list_is_the_directory_it_stands_for():
    """TWO POPULATIONS OF ONE THING IN ONE FILE, RECONCILED RATHER THAN LEFT EQUAL.

    `_ENTRY_POINTS` is typed by hand and parametrizes four sweeps above; `_loader_scripts`
    globs `databricks/src` and drives the ones below. They agree today and nothing said so,
    so a seventh `vault_load_*.py` nobody adds to the tuple escapes the catalog, the
    `SystemExit` and the axis sweeps while the glob's green reads as coverage of them."""
    assert set(_ENTRY_POINTS) == set(_loader_scripts()), (
        f"_ENTRY_POINTS names {sorted(_ENTRY_POINTS)} and databricks/src holds "
        f"{list(_loader_scripts())}. Every sweep parametrized over the tuple runs over "
        "the difference and nothing tells it to. Add the script, or delete the entry."
    )


def test_every_parent_a_vault_task_resolves_is_resolved_before_the_session():
    """`vault_load_satellite.main` says "diagnosing it must not cost a serverless start",
    and until this lock that ordering was true by reading and held by nothing: moving both
    resolutions below `getOrCreate()` killed zero tests, MEASURED. A refusal that costs a
    serverless start is one an operator routes around. The idiom is
    `tests/test_gold_entry_points.py`'s timezone pin, compared by AST line number; the
    population is every script resolving a parent AT ALL, read through
    `_parent_resolver_of` so it is the sweep below's own answer and asserted non-empty."""
    resolving = tuple(script for script in _loader_scripts() if _parent_resolver_of(script))
    assert resolving, (
        "no entry point under databricks/src resolves a parent through opl.vault.domains, "
        "so this sweep runs over nothing. Re-derive it against however they resolve one."
    )
    for script in resolving:
        resolutions, sessions = _resolution_and_session_lines(script)
        assert resolutions and len(sessions) == 1, (
            f"{script}.py has {len(resolutions)} parent resolutions and {len(sessions)} "
            "getOrCreate calls, so this reader cannot say which comes first."
        )
        assert max(resolutions) < sessions[0], (
            f"{script}.py resolves a parent on line {max(resolutions)}, after it takes a "
            f"session on line {sessions[0]}. Every refusal that resolution raises is "
            "about a job YAML and none of them needs a cluster to diagnose."
        )


_REGISTERED_SATELLITES = tuple(
    sorted(
        name
        for name, spec in domains.REGISTRY.items()
        if isinstance(spec, Satellite | EffectivitySatellite)
    )
)

_PAIRINGS = tuple(
    (table, source)
    for table in _REGISTERED_SATELLITES
    for source in sorted(BRONZE_REGISTRY)
)


@pytest.mark.parametrize("table", _REGISTERED_SATELLITES)
def test_every_registered_satellite_reaches_a_loader_that_accepts_its_parents_kind(table):
    """TOTAL OVER THE REGISTRY, AND OVER EVERY SATELLITE KIND -- a lock written for one
    table would be the population-chosen-by-hand defect this repository keeps meeting.

    Exactly one entry point accepts each satellite's kind, and the resolver THAT script
    uses answers for THIS table. `sat_link_payment` is a `Satellite` whose parent is a
    LINK, so it is the first table in this repository where routing by kind and routing
    by parent disagree; read the comment block above before taking this sweep's green as
    evidence of anything, because on a tree without that table the two rules cannot be
    told apart."""
    spec = domains.REGISTRY[table]
    script = _the_one_script_accepting(spec)
    resolver = _parent_resolver_of(script)
    assert resolver, (
        f"{script}.py is the only entry point accepting {table!r} and it resolves no "
        "parent through opl.vault.domains, so what this table would be keyed on is not "
        "readable here. Re-derive this sweep against however the script now resolves it."
    )
    try:
        parent = getattr(domains, resolver)(spec)
    except ValueError as refusal:
        pytest.fail(
            f"{script}.py is the only entry point that accepts {table!r} (a "
            f"{type(spec).__name__}), and it resolves its parent with domains."
            f"{resolver}, which REFUSES this table: {refusal} -- so the task would raise "
            "on the cluster while every YAML lock stayed green."
        )
    assert parent.name == spec.parent


@pytest.mark.parametrize("table,source", _PAIRINGS)
def test_the_arguments_a_satellite_task_builds_are_a_pairing_its_own_loader_accepts(
    table, source
):
    """THE SECOND HALF: routing to the right script is not the same as handing it
    arguments it accepts.

    The script's own `parent_arguments` is DRIVEN rather than restated, and its answer
    goes through BOTH gates `load_satellite` puts it through before it reads a row, in
    that order: `opl.vault.satellites._resolved_parent`'s four refusals, then
    `snapshot_axis_for`'s six, which is that module's own count and is accounted for in
    the block above. A grain where an axis belongs, an axis where a grain
    belongs, a transactional satellite on a hub or a state satellite on a link are
    refused by the second, so the shape nobody has written yet reddens here on the day it
    is registered; the link's own hubs are read only by the first, and the block above
    says what that cost while only the second was driven.

    A SCRIPT WITHOUT THAT SEAM IS SKIPPED, and the test below -- `test_the_pairing_sweep
    _drives_a_real_seam_and_covers_both_parent_kinds` -- is what keeps the skip from
    swallowing the sweep. `vault_load_effectivity.py` builds
    its grain and hands it to `load_effectivity_satellite`, whose link-grain comparison
    sits past a Spark session, and its ROUTING is covered by the sweep above while its
    grain is compared with the domain's own declaration by
    `tests/test_vault_job_wiring.py::test_the_grain_this_task_builds_is_the_grain_the
    _domain_declares`."""
    spec = domains.REGISTRY[table]
    task = _load(_the_one_script_accepting(spec))
    builder = getattr(task, "parent_arguments", None)
    if builder is None:
        pytest.skip(f"{task.__name__} exposes no parent_arguments seam to drive")
    bronze = BRONZE_REGISTRY[source]
    parent = domains.parent_of(spec)
    built = builder(parent, bronze)
    resolved = _resolved_parent(
        spec, built.get("hub"), built.get("link"), built.get("hubs", ())
    )
    assert resolved is parent, (
        f"{table!r}'s task built arguments naming {resolved.name!r} for a parent this "
        f"registry resolves as {parent.name!r}"
    )
    axis = snapshot_axis_for(
        spec, parent, built.get("grain"), built.get("axis"), DEFAULT.table(bronze.bronze)
    )
    assert axis == bronze.snapshot_axis, (
        f"{table!r} over {source!r} would read its window on {axis.column!r} while that "
        f"source declares {bronze.snapshot_axis.column!r}"
    )


# THIS GUARD ONCE WATCHED THE SEAM AND SAID NOTHING ABOUT THE POPULATION -- this file's
# own quoted failure, "the population a check runs over is chosen by hand somewhere nobody
# re-derives", instantiated INSIDE the guard written to prevent it. Its first version
# re-derived `driven` from `_REGISTERED_SATELLITES` and not from the tuple the sweep is
# parametrized over, so it answered about the registry while the sweep ran over
# `_PAIRINGS`. Both measured green with that guard in place: filtering transactional
# tables out of `_PAIRINGS` dropped all seven `sat_link_payment` cases silently, and
# `_PAIRINGS = ()` left pytest reporting `got empty parameter set` over a passing suite.
# Every claim below is now read off `_PAIRINGS` itself.
def test_the_pairing_sweep_drives_a_real_seam_and_covers_both_parent_kinds():
    """WITHOUT THIS THE SWEEP ABOVE COULD SKIP ITSELF INTO SILENCE OR RUN OVER NOTHING,
    which are two different ways for a parametrized sweep to report success.

    Three claims, argued in the block above: some entry point exposes the seam, the sweep
    is parametrized over EVERY registered satellite, and the tables it drives through the
    seam include BOTH a hub-parented and a link-parented one -- the last saying it
    exercises the branch and not one side of it, and false on every day before
    `sat_link_payment`."""
    exposing = _scripts_exposing_the_seam()
    assert exposing, (
        "no entry point under databricks/src exposes a parent_arguments seam, so the "
        "pairing sweep skips every case and asserts nothing"
    )
    parametrized = {table for table, _ in _PAIRINGS}
    assert parametrized == set(_REGISTERED_SATELLITES), (
        f"the pairing sweep is parametrized over {sorted(parametrized)} while the vault "
        f"registers satellites {sorted(_REGISTERED_SATELLITES)}. A satellite the sweep "
        "never runs over is one it asserts nothing about, and an empty _PAIRINGS makes "
        "every case above vacuous while pytest still reports success."
    )
    driven = {
        type(domains.parent_of(domains.REGISTRY[table])).__name__
        for table in parametrized
        if isinstance(domains.REGISTRY[table], Satellite)
        and _the_one_script_accepting(domains.REGISTRY[table]) in exposing
    }
    assert driven == {"Hub", "Link"}, (
        f"the pairing sweep drives satellites parented on {sorted(driven)} only. Both "
        "branches of parent_arguments need a registered table, or one of them is "
        "asserted by nothing -- which is exactly the state this vault was in before "
        "sat_link_payment was registered."
    )


def test_the_seam_the_pairing_sweep_drives_is_what_main_hands_the_loader():
    """NOTHING ABOVE BOUND `main` TO THE SEAM, AND THAT WAS MEASURED RATHER THAN FEARED.

    `parent_arguments` is public because the pairing sweep drives it -- but the sweep
    drives the FUNCTION, and until this test nothing drove `main`'s USE of it. Putting
    back the inline dict it was extracted from killed ZERO tests: the routing sweep stayed
    green (`parent_of` is still called), the pairing sweep stayed green (the function
    still exists and answers), the guard above stayed green, and `sat_link_payment` was
    back to `hub=<Link>` -- verbatim the failure the block above promises to prevent,
    under a fully green suite. `tests/vault/test_satellite_diagnostics.py` closes the same
    hole for the printed line with `"_diagnostics_note(months, result)" in ...`; this is
    that idiom at the AST.

    AND THE FIRST AST SPELLING OF IT WAS STILL SATISFIABLE BY A WRONG `main`, measured
    two ways. It asked only that SOME name be bound by `parent_arguments` and that SOME
    call splat it -- never that the loader was the call splatted into. So binding the
    seam and then rebinding over it, and binding the seam and splatting it into a
    throwaway `dict()` while handing the loader `hub=parent` by hand, each left this file
    green with `sat_link_payment` back to `hub=<Link>`. What is asserted now is the whole
    path: the call that takes the session splats exactly one bare name, and that name is
    what `parent_arguments` returned. WHAT THIS DOES NOT COVER IS DECLARED IN THE BLOCK
    ABOVE, in three classes, one of which is silent -- read it before quoting this lock
    as a guarantee."""
    for script in _scripts_exposing_the_seam():
        call, bound = _loader_call_in_main(script)
        splats = [word.value for word in call.keywords if word.arg is None]
        assert len(splats) == 1 and isinstance(splats[0], ast.Name), (
            f"{script}.py main() hands its loader {len(splats)} ** unpackings of a bare "
            "name. The seam's answer reaches the loader as exactly one, or what the task "
            "is keyed on stops being readable from here."
        )
        seam = bound.get(splats[0].id)
        assert (
            isinstance(seam, ast.Call) and isinstance(seam.func, ast.Name)
            and seam.func.id == "parent_arguments"
        ), (
            f"{script}.py main() splats {splats[0].id!r} into its loader, and that name "
            "is not what parent_arguments() returned. The pairing sweep drives that "
            "function; if the task does not, the sweep asserts about code no job runs."
        )
