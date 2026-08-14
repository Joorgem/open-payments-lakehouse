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

from dataclasses import replace

import pytest

from opl.bronze.registry import (
    FILE_FED_LANDING_MODES,
    LANDING_API,
    LANDING_LOCAL,
    LANDING_ZIPS,
    REGISTRY,
    BronzeTable,
    _assert_no_table_nothing_downloads_claims_a_downloader,
    _assert_prefixes_match_their_file_groups,
)


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


def test_the_two_skips_partition_the_registry(monkeypatch):
    """WHAT THE RENAME BOUGHT, asserted as a property rather than as prose.

    Each guard skips what the other checks, so every registered table -- under any
    landing mode that ever exists -- is examined by exactly one of the pair. A fifth mode
    added tomorrow needs no edit to either, which is precisely what was NOT true when
    both were scoped positively and `api` fell between them.

    A synthesised mode is used because the property has to hold for modes nobody has
    declared yet; `_assert_landing_modes_known` is what refuses this value in the live
    registry, and it runs before both of these."""
    trap = replace(REGISTRY["payments"], name="future", landing="a-mode-nobody-declared")
    monkeypatch.setitem(REGISTRY, "future", trap)

    checked_by_the_cross_check = {
        spec.name for spec in REGISTRY.values() if spec.landing in FILE_FED_LANDING_MODES
    }
    checked_by_the_mirror = {
        spec.name for spec in REGISTRY.values() if spec.landing not in FILE_FED_LANDING_MODES
    }
    assert not checked_by_the_cross_check & checked_by_the_mirror
    assert checked_by_the_cross_check | checked_by_the_mirror == {
        spec.name for spec in REGISTRY.values()
    }
    assert "future" in checked_by_the_mirror, (
        "an undeclared mode must fall to the mirror rather than through both"
    )
    # And the mirror actually refuses it, rather than merely being scoped over it: this
    # spec has no file group and no prefix, so it passes -- which is the correct verdict
    # and is what makes the scope real instead of vacuous.
    _assert_no_table_nothing_downloads_claims_a_downloader(REGISTRY)
    monkeypatch.setitem(REGISTRY, "future", replace(trap, prefix="Future"))
    with pytest.raises(ValueError, match="'a-mode-nobody-declared'"):
        _assert_no_table_nothing_downloads_claims_a_downloader(REGISTRY)
