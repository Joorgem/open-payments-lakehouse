"""The LANDING guards, exercised -- the pair that decides what a table's landing mode
obliges it to declare.

SPLIT OUT OF `test_registry_guards.py`, which stood at 784 of this project's 800-line
limit -- the closest file in the tree to the cap -- and which F-API's fix pass is about
to add a guard's tests to. §4.9/§4.12 say whoever touches a file at the cap splits it
first, so this lands alone.

THE SEAM IS THE ONE ITS OWN DOCSTRING NAMED AND THEN OUTGREW. That file says three
reasons to edit are three files: `test_registry.py` changes when a TABLE is added,
`test_registry_guards.py` when a GUARD changes, and `test_registry_guard_wiring.py`
when the registry's MODULE LAYOUT changes. "A guard changes" turned out to be two
reasons rather than one, and the source side had already split along the line between
them: `opl.bronze.registry_landing` is its own module precisely because it changes when
the LANDING LAYOUT changes, and everything below tests a guard defined there. What is
left behind tests the guards `registry.py` and `registry_collisions.py` define -- names,
subdirs, contracts and constraints -- none of which knows what a landing mode is.

It is also the axis this repository keeps moving along, which is what makes the seam a
prediction rather than a line count: F1b Task 3 scoped the file-fed cross-check when the
third mode arrived, F-API Task 2 added the fourth mode and made the pair complementary,
and this pass adds the classification guard. Three edits, one file, one subject.

Every test here works the way that file's do: put a spec that must not exist into
`REGISTRY` with `monkeypatch`, call the guard, and assert it refuses AND says why. They
are therefore direct-call tests, which proves a function refuses and says nothing about
whether anything ever runs it -- `test_registry_guard_wiring.py` is what closes that
vacuity, for the guards in this module as for every other."""
from __future__ import annotations

import sys
from dataclasses import replace

import pytest

from opl.bronze.registry import (
    FILE_FED_LANDING_MODES,
    LANDING_API,
    LANDING_LOCAL,
    LANDING_MODES,
    LANDING_ZIPS,
    NON_FILE_FED_LANDING_MODES,
    REGISTRY,
    BronzeTable,
    _assert_every_landing_mode_is_classified,
    _assert_no_table_nothing_downloads_claims_a_downloader,
    _assert_prefixes_match_their_file_groups,
    landing_dir,
)
from opl.config import DEFAULT


def test_a_prefix_that_disagrees_with_its_file_group_is_refused_at_import(monkeypatch):
    """The refusal exercised, not just today's entries proved clean.

    `Estabelecimento` (singular) is the probe on purpose: it is unique, it is a
    single directory name, it names no reserved dir, and it passes every other check
    in either registry test file. What it does is go looking for files that are not
    there and
    under-ingest without erroring -- the failure class this project rejected globs
    for."""
    trap = BronzeTable(
        name="estabelecimentos",
        contract="estabelecimentos",
        table_key="bronze_cnpj_estab",
        staging="bronze_cnpj_estab_staging",
        bronze="bronze_cnpj_estabelecimentos",
        quarantine="bronze_cnpj_estab_quarantine",
        subdir="estabelecimentos",
        landing=LANDING_ZIPS,
        prefix="Estabelecimento",  # singular: a real typo, unique, silent
        constraints=(),
    )
    monkeypatch.setitem(REGISTRY, "estabelecimentos", trap)

    with pytest.raises(ValueError) as excinfo:
        _assert_prefixes_match_their_file_groups(REGISTRY)
    message = str(excinfo.value)
    assert "'Estabelecimento'" in message and "'Estabelecimentos'" in message


def test_a_table_fed_by_several_groups_must_declare_no_prefix(monkeypatch):
    """The lookup's `None` is a real property, so the assertion has to hold in that
    direction too: six differently-named files routed into one table by filename
    suffix have no single prefix, and inventing one would look declarative while
    matching nothing."""
    trap = BronzeTable(
        name="lookup",
        contract="lookup",
        table_key="bronze_cnpj_lookup",
        staging="bronze_cnpj_lookup_staging",
        bronze="bronze_cnpj_lookup",
        quarantine="bronze_cnpj_lookup_quarantine",
        subdir="lookups",
        landing=LANDING_LOCAL,
        prefix="Cnaes",  # one of the six, which is worse than none
        constraints=(),
    )
    monkeypatch.setitem(REGISTRY, "lookup", trap)

    with pytest.raises(ValueError) as excinfo:
        _assert_prefixes_match_their_file_groups(REGISTRY)
    assert "prefix=None" in str(excinfo.value)


# --- THE COMPLEMENT GUARD (F-API Task 2) ---------------------------------------------
#
# `_assert_no_table_nothing_downloads_claims_a_downloader` had NO refusal test until this
# phase. It had a live-registry sweep in `test_registry.py`, which by this repository's
# own doctrine "would stay green if the guard were deleted" -- the sweep asserts today's
# entries are clean, and today's entries are clean whether or not anything refuses a
# dirty one. Both tests below fail with the guard's body replaced by `return`, because
# each one puts a spec into REGISTRY that the guard is the only thing refusing.
#
# THEY USE `landing="api"` DELIBERATELY, and that is the whole point rather than a
# convenience. Under the guard's previous scope (`!= LANDING_GENERATED`) both of these
# specs IMPORT CLEAN: `api` is not generated, so the mirror skipped it, and `api` is not
# file-fed, so the cross-check skipped it too -- a mode unguarded in both directions. A
# test written with `landing="generated"` would pass against the old guard and the new
# one alike, and would say nothing about the hole this phase closed.


def test_a_table_no_downloader_feeds_may_not_claim_a_file_group(monkeypatch):
    """One landing directory, two producers, and a stream that cannot tell them apart.

    The trap declares the fourth landing mode and the `estabelecimentos` contract, which
    six-part `FILE_GROUPS` entries feed. What would follow is not an error: the extraction
    host PUTs the RFB's archives into that contract's landing dir, this lakehouse writes
    its own record into it too, and cloudFiles reads the directory RECURSIVELY with no
    glob -- so one stream ingests both, against one schema, and reports SUCCESS."""
    trap = replace(
        REGISTRY["payments"],
        name="ptax",
        contract="estabelecimentos",
        landing=LANDING_API,
    )
    monkeypatch.setitem(REGISTRY, "ptax", trap)

    with pytest.raises(ValueError) as excinfo:
        _assert_no_table_nothing_downloads_claims_a_downloader(REGISTRY)
    message = str(excinfo.value)
    assert "'api'" in message, "the refusal must name the mode it FOUND"
    assert "generated" not in message, (
        "a PTAX operator reading a refusal about the payment generator goes looking in "
        "the wrong module"
    )
    assert "Estabelecimentos" in message


def test_a_table_no_downloader_feeds_may_not_claim_a_file_prefix(monkeypatch):
    """The second refusal, which is a different sentence about a different mistake.

    A prefix is the string a DOWNLOADER builds its file list from. Declared for a table
    nothing downloads it is a false statement in the file this repository treats as the
    answer to "what is table X?" -- and it enters that table into
    `test_no_two_tables_share_a_file_prefix`, where it competes for a real producer's
    string. Nothing reads it, so nothing fails; the registry simply says something untrue.

    The contract stays `payments`, which no FILE_GROUPS entry feeds, so the first refusal
    cannot fire and this reaches the second."""
    trap = replace(REGISTRY["payments"], name="ptax", landing=LANDING_API, prefix="Ptax")
    monkeypatch.setitem(REGISTRY, "ptax", trap)

    with pytest.raises(ValueError) as excinfo:
        _assert_no_table_nothing_downloads_claims_a_downloader(REGISTRY)
    message = str(excinfo.value)
    assert "'api'" in message and "'Ptax'" in message
    assert "generated" not in message
    assert "prefix=None" in message


# --- THE PARTITION, ASSERTED BY CALLING BOTH GUARDS (F-API's fix pass) ---------------
#
# WHAT WAS HERE WAS A TAUTOLOGY FOR THE PROPERTY IT WAS NAMED AFTER.
# `test_the_two_skips_partition_the_registry` built both "checked by" sets INSIDE the test
# from a predicate and its own negation over one dict, so disjointness and totality held
# for ANY predicate and ANY guard body -- neither guard was called at all until its last
# three lines. Change the cross-check's skip to `not in {LANDING_ZIPS}` and every `local`
# table falls through BOTH guards while that test stays green: measured, and it is why the
# rewrite below calls the functions instead of re-deriving their scopes.


def _refusals_of_the_pair() -> list[tuple[str, str]]:
    """Which of the two landing guards refuses the live `REGISTRY`, and what it said.

    Both are called, always, and their verdicts collected rather than short-circuited --
    "exactly one refused" is the assertion, and a helper that stopped at the first
    refusal could not tell it from "at least one"."""
    refused = []
    for guard in (
        _assert_prefixes_match_their_file_groups,
        _assert_no_table_nothing_downloads_claims_a_downloader,
    ):
        try:
            guard(REGISTRY)
        except ValueError as refusal:
            refused.append((guard.__name__, str(refusal)))
    return refused


@pytest.mark.parametrize("landing", sorted(LANDING_MODES))
def test_exactly_one_of_the_pair_refuses_a_spec_that_is_wrong_for_its_half(
    monkeypatch, landing
):
    """EVERY DECLARED MODE, HELD TO THE QUESTION ITS HALF ASKS -- by calling both guards.

    The trap is a spec that is WRONG for whichever half `landing` is in, so one of the two
    is obliged to refuse it and the other is obliged not to:

      * a FILE-FED mode on the `payments` contract, which no `cnpj_schemas.FILE_GROUPS`
        entry feeds. That is the cross-check's "no prefixes -> raise" branch, whose own
        message says the ingest "would report SUCCESS having read an empty source dir".
      * a mode nothing downloads, declaring a `prefix`. That is the mirror's second
        refusal -- a false sentence about a downloader that does not exist.

    EXACTLY ONE, which is two claims in one number. Two refusals would mean the scopes
    overlap and a table is held to contradictory requirements; ZERO means a mode is
    examined by a guard that has nothing to say about it, which is the whole defect this
    pair has had twice -- `api` in neither half before F-API Task 2, and `local` falling
    through both under a one-character change to the cross-check's skip.

    PARAMETRISED OVER `LANDING_MODES` rather than over a list, so a fifth mode arrives
    with its case already written and has to pass this on the same day it is declared."""
    file_fed = landing in FILE_FED_LANDING_MODES
    trap = replace(
        REGISTRY["payments"],
        name="probe",
        landing=landing,
        prefix=None if file_fed else "Probe",
    )
    monkeypatch.setitem(REGISTRY, "probe", trap)

    refused = _refusals_of_the_pair()
    assert len(refused) == 1, (
        f"landing={landing!r} was refused by {[name for name, _ in refused]}, and exactly "
        "one of the pair must: none means the mode is examined by a guard with nothing to "
        "say about it, two means the scopes overlap"
    )
    guard, message = refused[0]
    expected = (
        "_assert_prefixes_match_their_file_groups"
        if file_fed
        else "_assert_no_table_nothing_downloads_claims_a_downloader"
    )
    assert guard == expected, f"landing={landing!r} was answered by the wrong half"
    assert repr(landing) in message, "the refusal must name the mode it FOUND"


def test_an_undeclared_mode_still_falls_to_the_mirror_rather_than_through_both(monkeypatch):
    """THE COMPLEMENT'S RESIDUAL VALUE, kept exercised now that the classification is
    declared rather than inferred.

    `_assert_every_landing_mode_is_classified` refuses a DECLARED mode nobody classified,
    and `_assert_landing_modes_known` refuses a mode nobody declared -- both at import,
    both before either guard here runs. So this is defence in depth rather than the load
    path: the mirror's skip is a literal complement over any string, so a mode that
    reached these guards without passing either of those is still examined by one of them
    instead of by neither."""
    trap = replace(
        REGISTRY["payments"],
        name="future",
        landing="a-mode-nobody-declared",
        prefix="Future",
    )
    monkeypatch.setitem(REGISTRY, "future", trap)

    refused = _refusals_of_the_pair()
    assert [name for name, _ in refused] == [
        "_assert_no_table_nothing_downloads_claims_a_downloader"
    ]
    assert "'a-mode-nobody-declared'" in refused[0][1]


# --- THE CLASSIFICATION ITSELF, WHICH IS WHAT THE COMPLEMENT DOES NOT GUARANTEE -------
#
# The complement buys total EXAMINATION and not a total VERDICT. Which of the two
# questions a table is asked turns on membership of `FILE_FED_LANDING_MODES`, and until
# F-API's fix pass NOTHING guarded that membership: a fifth mode that IS file-fed and was
# not added to it would be examined by the mirror, which ACCEPTS (no file group and no
# prefix is the mirror's pass), so the cross-check's "no producer -> raise" branch would be
# LOST rather than moved -- it has no complement anywhere.


def test_the_live_classification_passes_its_own_guard():
    """Guard the guard. Every refusal below is of a synthesised classification, and all of
    them would also be produced by a guard that refused everything -- which runs at import,
    so a false positive breaks every module that reads the registry, including the
    extraction scripts that never touch Spark."""
    _assert_every_landing_mode_is_classified()
    assert FILE_FED_LANDING_MODES | NON_FILE_FED_LANDING_MODES == LANDING_MODES
    assert not FILE_FED_LANDING_MODES & NON_FILE_FED_LANDING_MODES


@pytest.mark.parametrize("half", ["FILE_FED_LANDING_MODES", "NON_FILE_FED_LANDING_MODES"])
def test_a_declared_mode_in_neither_half_is_refused_at_import(monkeypatch, half):
    """THE HOLE THIS GUARD CLOSES, in both directions of the edit that opens it.

    Dropping `api` from the non-file-fed half is the harmless-looking version; dropping a
    mode from the file-fed half is the one that costs something, because it is a real
    file-fed table whose missing producer nothing would then refuse. The guard reads the
    declaration and not the registry, so both are refused before any table names the mode.

    A fifth mode added to `LANDING_MODES` and to neither half is the same edit seen from
    the other side, and it is the one this repository will actually make."""
    module = sys.modules[_assert_every_landing_mode_is_classified.__module__]
    dropped = sorted(getattr(module, half))[0]
    monkeypatch.setattr(module, half, getattr(module, half) - {dropped})

    with pytest.raises(ValueError) as excinfo:
        _assert_every_landing_mode_is_classified()
    message = str(excinfo.value)
    assert repr(dropped) in message
    # The consequence, not just the fact: an operator told "classify it" would otherwise
    # pick the half that reads as tidier, and one of the two halves silently accepts.
    assert "SUCCESS having read an empty source dir" in message


def test_a_mode_declared_in_both_halves_is_refused_at_import(monkeypatch):
    """The other failure, and it is a different sentence: a mode that is both file-fed and
    not is asked two opposite questions -- a producer is required AND forbidden -- so
    whatever its tables declare, one of the pair refuses them. Refused where the
    classification is written rather than met as a contradiction at a table."""
    module = sys.modules[_assert_every_landing_mode_is_classified.__module__]
    monkeypatch.setattr(
        module, "NON_FILE_FED_LANDING_MODES", NON_FILE_FED_LANDING_MODES | {LANDING_ZIPS}
    )

    with pytest.raises(ValueError, match="BOTH file-fed and not") as excinfo:
        _assert_every_landing_mode_is_classified()
    assert repr(LANDING_ZIPS) in str(excinfo.value)


def test_a_classified_mode_that_is_not_a_landing_mode_is_refused_at_import(monkeypatch):
    """The same equality read the other way: a half naming a mode `LANDING_MODES` does not
    carry classifies something no spec can declare, which is how the two sets stop
    describing one universe -- and a mode later added under that name would inherit a
    classification nobody chose deliberately."""
    module = sys.modules[_assert_every_landing_mode_is_classified.__module__]
    monkeypatch.setattr(
        module, "FILE_FED_LANDING_MODES", FILE_FED_LANDING_MODES | {"ftp"}
    )

    with pytest.raises(ValueError, match="'ftp'"):
        _assert_every_landing_mode_is_classified()


# --- WHAT THE CLASSIFICATION GUARD DOES *NOT* CLOSE, EXERCISED RATHER THAN COMMENTED ---
#
# It closes the OMISSION edit -- a mode classified in neither half -- and not the MISFILING
# one. That residual was stated in a comment inside the test above, which is not where an
# operator reads a declaration, so it now sits above `FILE_FED_LANDING_MODES` in the source
# and is MEASURED here. Nothing in the module can refuse a misfiling: the classification is
# the only place this repository records whether a mode's bytes are downloaded, so there is
# no second declaration to cross-check it against.
#
# The test below therefore asserts the hole is a hole, which is deliberate and is the only
# honest shape for it: if a later change closes it, this test fails and names the paragraph
# to delete instead of leaving the source claiming a gap that no longer exists.


def test_a_file_fed_mode_MISFILED_as_non_file_fed_passes_both_guards_and_fails_at_the_root(
    monkeypatch,
):
    """The residual, in three steps and one refusal that is not either guard's.

    A fifth mode that IS file-fed, put in `NON_FILE_FED_LANDING_MODES`, is in exactly one
    half -- so the classification guard passes. A table on it whose contract has no
    `FILE_GROUPS` producer is then SKIPPED by the cross-check and ACCEPTED by the mirror (no
    group, no prefix is the mirror's pass), which is verbatim the hole the classification
    guard's own message describes. A table whose contract DOES have a producer is still
    caught, by the mirror's first refusal -- that case is `test_a_table_no_downloader_feeds_
    may_not_claim_a_file_group` above.

    WHAT CATCHES IT NEXT IS THE ROOT DISPATCH, and that is why the guard's "SUCCESS having
    read an empty source dir" is not what this edit alone produces: `_landing_and_tmp` serves
    each mode from one declared root and refuses a mode it has no branch for, so the misfiled
    mode has no landing dir at all and `landing_dir` raises before any Auto Loader is pointed
    anywhere. Reaching the silent version needs the same author to also give the mode a root
    -- a third edit, in a third place, all of them in this module."""
    module = sys.modules[_assert_every_landing_mode_is_classified.__module__]
    misfiled = "sftp"  # file-fed in reality: something downloads it
    monkeypatch.setattr(module, "LANDING_MODES", LANDING_MODES | {misfiled})
    monkeypatch.setattr(
        module, "NON_FILE_FED_LANDING_MODES", NON_FILE_FED_LANDING_MODES | {misfiled}
    )
    _assert_every_landing_mode_is_classified()  # the misfiling is invisible to it

    # `payments` because no FILE_GROUPS entry feeds it: a table with no producer at all.
    trap = replace(REGISTRY["payments"], name="probe", landing=misfiled, prefix=None)
    monkeypatch.setitem(REGISTRY, "probe", trap)
    assert not _refusals_of_the_pair(), (
        "the pair now refuses a misfiled file-fed mode. That is an improvement, not a "
        "failure -- delete the residual paragraph above `FILE_FED_LANDING_MODES` and this "
        "test with it, rather than leaving the source describing a hole that is closed"
    )

    with pytest.raises(ValueError, match="no landing root serves") as excinfo:
        landing_dir(DEFAULT, trap, "2026-08")
    assert repr(misfiled) in str(excinfo.value)
