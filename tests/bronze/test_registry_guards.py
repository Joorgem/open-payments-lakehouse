"""The registry's guards, exercised -- each refusal reached by the paste that
reaches it.

SPLIT OUT OF `test_registry.py`, which crossed this project's 800-line file limit
when F1.4b registered two more tables. The seam is not the line count: these tests
change when a GUARD changes, and the ones left behind change when a TABLE is added.
Two reasons to edit a file are two files -- and this half is the one that grows per
guard, which is the axis this phase has been growing along.

Every test here works the same way: put a spec that must not exist into `REGISTRY`
with `monkeypatch`, call the guard, and assert it refuses AND says why. They are
therefore direct-call tests, which proves a function refuses and says nothing about
whether anything ever runs it -- commenting out the calls at the bottom of registry.py
once left this whole file green. `test_registry_guard_wiring.py` is what closes that
vacuity, and it moved out of this file when it was taught to read more than one
registry module: what breaks it is a change to the registry's MODULE LAYOUT, which is
a third reason to edit and therefore a third file.

The traps are synthesised rather than committed to `REGISTRY`, for the obvious
reason: an entry that a guard refuses cannot exist in source, because it would break
the import of every module that reads the registry."""
from __future__ import annotations

import subprocess
import sys
import textwrap
from dataclasses import replace
from pathlib import Path

import pytest

from opl.bronze import registry
from opl.bronze.registry import (
    LANDING_LOCAL,
    LANDING_ZIPS,
    REGISTRY,
    BronzeTable,
    UnknownTable,
    _assert_contracts_exist,
    _assert_no_masked_contract_declares_a_check_constraint,
    _assert_no_table_claims_a_reserved_subdir,
    _assert_no_two_tables_share_a_contract,
    _assert_no_two_tables_share_a_landing_subdir,
    _assert_prefixes_match_their_file_groups,
    _assert_subdirs_are_single_path_components,
)

# The three name-collision guards live in their own module since F2 Task 0 -- see
# `opl.bronze.registry_collisions` for the seam. They take the registry they validate
# as an argument, because `registry.py` calls them at its own import and importing it
# back would be a cycle; `REGISTRY` above is the same dict object `monkeypatch.setitem`
# mutates, so every trap below reaches them exactly as it reached them in-place.
from opl.bronze.registry_collisions import (
    _assert_no_table_key_is_month_shaped,
    _assert_no_two_tables_share_a_checkpoint_namespace,
    _assert_no_two_tables_share_a_delta_name,
)


def test_a_pasted_delta_name_is_refused_at_import(monkeypatch):
    """The refusal exercised -- `test_no_two_tables_share_a_staging_bronze_or_
    quarantine_name` (test_registry.py) only proves today's entries are clean, and
    would stay green with no guard behind it at all.

    Verified by probe against the live registry before this guard existed: a paste
    of the estabelecimentos entry renamed everywhere EXCEPT the staging/bronze/
    quarantine triple IMPORTED CLEAN. Only that CI test caught it, and a CI
    test protects a merge -- it does not protect an ad-hoc job run against a branch
    whose tests have not been run, which is exactly how these jobs get launched
    while a phase is in flight.

    Two tables on one Delta name is the defect class this registry exists to close:
    appends from both land in one table, and `_batch_id` idempotence breaks because
    `rows_of_batch` counts the other table's rows as this batch's.

    `table_key` IS renamed here, unlike the rest of the paste, so that the collision
    under test is unambiguously a Delta name. A guard that only checked `table_key`
    would satisfy a version of this test that left it stale, and would be exactly as
    wrong as no guard at all. `test_a_pasted_checkpoint_namespace_is_refused_at_
    import` covers the stale `table_key` on its own."""
    pasted = replace(
        REGISTRY["estabelecimentos"],
        name="empresas",
        contract="empresas",
        subdir="empresas",
        prefix="Empresas",
        table_key="bronze_cnpj_empresas",
        # staging/bronze/quarantine deliberately NOT changed -- this IS the paste.
    )
    monkeypatch.setitem(REGISTRY, "empresas", pasted)

    with pytest.raises(ValueError, match="both declare Delta table"):
        _assert_no_two_tables_share_a_delta_name(REGISTRY)


def test_a_delta_name_reused_in_a_different_role_is_refused(monkeypatch):
    """What makes the check ONE namespace over three roles rather than three
    namespaces of one role each.

    Three per-role checks would pass this: `bronze_cnpj_estab_staging` appears once
    as a `staging` and once as a `quarantine`, so neither role's own namespace sees a
    duplicate. The consequence is worse than a plain duplicate -- the promote reads
    staging as trusted input while the triage writes rejects into the same table, so
    rows refused by DQ get read back as if they had passed it.

    Same defect class as the sibling test, reached past the shape of a guard that
    looks total and is not."""
    trap = replace(
        REGISTRY["lookup"],
        quarantine=REGISTRY["estabelecimentos"].staging,
    )
    monkeypatch.setitem(REGISTRY, "lookup", trap)

    with pytest.raises(ValueError, match="both declare Delta table"):
        _assert_no_two_tables_share_a_delta_name(REGISTRY)


def test_a_delta_name_that_differs_only_in_case_is_refused(monkeypatch):
    """Two spellings of one Unity Catalog table, which a byte comparison reads as two.

    UC and Spark identifiers are CASE-INSENSITIVE: `BRONZE_CNPJ_ESTAB_STAGING` and
    `bronze_cnpj_estab_staging` resolve to the same physical table, so an entry that
    declares the shouted spelling has taken estabelecimentos' staging table -- and
    before the guard casefolded, it imported clean. That is the sibling tests' defect
    class exactly (appends from two tables in one table, `_batch_id` idempotence
    counting the other's rows), reached past a guard that looks total.

    Not a theoretical spelling. It is what a paste through an editor's
    change-case-on-rename, or a value copied out of a SHOW TABLES listing, or a DDL
    file written in the uppercase-keyword style, produces.

    The message must carry BOTH spellings as written. An operator who reads a
    normalised name in the refusal goes looking through source for a string that is
    not there."""
    pasted = replace(
        REGISTRY["estabelecimentos"],
        name="empresas",
        contract="empresas",
        subdir="empresas",
        prefix="Empresas",
        table_key="bronze_cnpj_empresas",
        staging="BRONZE_CNPJ_ESTAB_STAGING",  # case-only difference: the same table
        bronze="bronze_cnpj_empresas",
        quarantine="bronze_cnpj_empresas_quarantine",
    )
    monkeypatch.setitem(REGISTRY, "empresas", pasted)

    with pytest.raises(ValueError) as excinfo:
        _assert_no_two_tables_share_a_delta_name(REGISTRY)
    message = str(excinfo.value)
    assert "both declare Delta table" in message
    assert "'BRONZE_CNPJ_ESTAB_STAGING'" in message, (
        "the operator must see the spelling they WROTE, not a normalised one"
    )
    assert "'bronze_cnpj_estab_staging'" in message, (
        "and the spelling it collides with, or there is nothing to compare it to"
    )


def test_a_delta_name_collision_in_one_spelling_reads_as_one_name(monkeypatch):
    """The case-insensitive message must not become noise in the ordinary case.

    Guarding the guard's diagnosis: the fix above has to report two spellings when
    there are two, and a plain duplicate when there is one. A message that always
    printed `'x' (spelled 'x' there)` would be read as a case problem by an operator
    who does not have one, and sent looking for a difference that is not there."""
    pasted = replace(REGISTRY["lookup"], quarantine=REGISTRY["estabelecimentos"].staging)
    monkeypatch.setitem(REGISTRY, "lookup", pasted)

    with pytest.raises(ValueError) as excinfo:
        _assert_no_two_tables_share_a_delta_name(REGISTRY)
    message = str(excinfo.value)
    assert "CASE-INSENSITIVE" not in message
    assert message.count("'bronze_cnpj_estab_staging'") == 1


def test_a_pasted_checkpoint_namespace_is_refused_at_import(monkeypatch):
    """`table_key` is the fourth name a paste leaves stale, and the quietest.

    It names no Delta table. It is the last component `autoloader.
    checkpoint_location` and `schema_location` put `_checkpoints/<month>/<table_key>`
    and `_schemas/<month>/<table_key>` in the Volume under, so two tables sharing one
    share an Auto Loader CHECKPOINT -- and a checkpoint is a record of which files
    are already processed. The second table's stream would start up believing the
    first's files were its own and already ingested, and report SUCCESS having
    written nothing. The shared `_schemas` entry compounds it by merging two
    unrelated schemas. Month-scoping (F1.4b PR B Task 5 Step 0) does not soften
    this: two tables ingesting the same month still collide, and every month
    collides, so the guard is exactly as strong as before.

    `test_every_registered_table_has_a_checkpoint_namespace_of_its_own`
    (test_registry.py) asserts the same property, and for the reason given in the
    sibling tests above that is not enough on its own."""
    pasted = replace(
        REGISTRY["estabelecimentos"],
        name="empresas",
        contract="empresas",
        subdir="empresas",
        prefix="Empresas",
        staging="bronze_cnpj_empresas_staging",
        bronze="bronze_cnpj_empresas",
        quarantine="bronze_cnpj_empresas_quarantine",
        # table_key deliberately NOT changed -- this IS the paste.
    )
    monkeypatch.setitem(REGISTRY, "empresas", pasted)

    with pytest.raises(ValueError, match="both claim checkpoint namespace"):
        _assert_no_two_tables_share_a_checkpoint_namespace(REGISTRY)


@pytest.mark.parametrize("table_key", ["2026-06", "2026-07", "2027-01"])
def test_a_month_shaped_table_key_is_refused_at_import(monkeypatch, table_key):
    """The guard the 800-line cap declined, now that F2 Task 0 has made room.

    It lived as a lock in `test_autoloader_helpers.py::test_no_table_shares_or_nests_
    state_across_months_or_with_the_orphans`, whose docstring said the structural
    version belonged in the registry and did not fit. A CI lock protects a merge; it
    does not protect the ad-hoc run of a branch whose tests have not been run, which is
    how these jobs get launched while a phase is in flight -- the same argument that put
    every other guard exercised in this file at import.

    What gets through is not an error. `autoloader` builds
    `_checkpoints/<month>/<table_key>`, and the pre-month-scoping `_checkpoints/
    <table_key>` directories still exist in the Volume holding 2026-06 state,
    deliberately unmigrated. A table_key of `2026-06` makes that orphan directory BE
    the month directory, so every table's 2026-06 state is written inside a checkpoint
    another query owns -- a new RocksDB store and offset log under a live one, which is
    the exact nesting month-scoping was introduced to make impossible.

    `2026-06` is the sharpest of the three because it is a month this Volume already
    holds state for. The other two are months no key collides with TODAY, and are here
    because the defect is the SHAPE, not the coincidence."""
    trap = replace(REGISTRY["socios"], table_key=table_key)
    monkeypatch.setitem(REGISTRY, "socios", trap)

    with pytest.raises(ValueError) as excinfo:
        _assert_no_table_key_is_month_shaped(REGISTRY)
    message = str(excinfo.value)
    assert "socios" in message and repr(table_key) in message
    # Refused for being a MONTH, not for colliding with another table -- the operator
    # told "give each table a table_key of its own" would rename it to another month.
    assert "which is a MONTH" in message
    assert "_checkpoints/<month>/<table_key>" in message


@pytest.mark.parametrize("table_key", ["2026-13", "2026-00", "26-06", "2026-6", "bronze_2026-06"])
def test_a_table_key_that_only_looks_month_shaped_is_left_alone(monkeypatch, table_key):
    """GUARDS THE GUARD, in the direction that costs something if it goes wrong.

    Everything above would also pass if this refused every table_key containing four
    digits and a hyphen, which would refuse a registry nobody can fix by renaming --
    and this guard runs at IMPORT, so a false positive breaks every module that reads
    the registry, including the extraction scripts that never touch Spark.

    `2026-13` and `2026-00` are the pointed cases and the reason the guard asks
    `opl.config.is_month` rather than carrying its own pattern: they have the SHAPE of
    a month and name no directory that can exist, because `require_month` refuses them
    -- so there is no `_checkpoints/2026-13` for a table to be confused with, and
    refusing them here would be a second spelling of the month rule diverging from the
    first. Two spellings of that rule is how `2026-13` came to be refused at two of
    four entry points in the first place."""
    trap = replace(REGISTRY["socios"], table_key=table_key)
    monkeypatch.setitem(REGISTRY, "socios", trap)

    # The other three live keys go through the guard untouched on every one of these
    # runs, so this also says the live registry is not refused. Deliberately NOT a pin
    # of the four names: that is `test_registry.py`'s side of the seam, and the strongest
    # enforcement of "the live registry passes" is the import at the top of this file --
    # the guard runs there, so a false positive fails collection of the whole suite.
    _assert_no_table_key_is_month_shaped(REGISTRY)


def test_a_pasted_subdir_is_refused_at_import(monkeypatch):
    """The paste F1.4b literally makes, refused where the value is DECLARED.

    `test_no_two_tables_share_a_landing_subdir` (test_registry.py) names the
    property; until this guard existed nothing enforced it outside CI. Verified by
    probe against the live registry: the estabelecimentos entry pasted and renamed
    everywhere EXCEPT `subdir` IMPORTED CLEAN. `subdir` is
    the field that does not contain the table's own bronze name, so a careful
    find/replace over `bronze_cnpj_*` sails straight past it -- and a CI test does
    not protect an ad-hoc run, or a branch whose tests have not been run.

    What gets through is not an error and does not look like one. Both tables' streams
    read the one landing dir, cloudFiles walks a source dir RECURSIVELY (F1.3,
    empirically: a probe.txt planted in `zips/estabelecimentos/` was ingested by a
    stream reading the month root), so each ingests the other's files -- and both runs
    report SUCCESS."""
    pasted = replace(
        REGISTRY["estabelecimentos"],
        name="empresas",
        contract="empresas",
        table_key="bronze_cnpj_empresas",
        staging="bronze_cnpj_empresas_staging",
        bronze="bronze_cnpj_empresas",
        quarantine="bronze_cnpj_empresas_quarantine",
        prefix="Empresas",
        # subdir deliberately NOT changed -- this IS the paste.
    )
    monkeypatch.setitem(REGISTRY, "empresas", pasted)

    with pytest.raises(ValueError, match="both claim landing subdir"):
        _assert_no_two_tables_share_a_landing_subdir()


def test_a_table_claiming_a_reserved_subdir_is_refused_by_name(monkeypatch):
    """The refusal itself, exercised -- `test_no_table_claims_a_directory_the_volume_
    layout_owns` (test_registry.py) only proves today's entries are clean, which
    would stay green if the guard were deleted.

    Synthesised rather than committed to REGISTRY for obvious reasons: the entry
    this refuses cannot exist in source, because it would break the import of every
    module that reads the registry."""
    trap = BronzeTable(
        name="socios",
        contract="lookup",
        table_key="bronze_cnpj_socios",
        staging="bronze_cnpj_socios_staging",
        bronze="bronze_cnpj_socios",
        quarantine="bronze_cnpj_socios_quarantine",
        subdir="zips",
        landing=LANDING_ZIPS,
        prefix="Socios",
        constraints=(),
    )
    monkeypatch.setitem(REGISTRY, "socios", trap)

    with pytest.raises(ValueError) as excinfo:
        _assert_no_table_claims_a_reserved_subdir()
    message = str(excinfo.value)
    assert "socios" in message and "'zips'" in message
    # The operator has to be told WHY, not just refused: the reason is recursion.
    assert "RECURSIVELY" in message


@pytest.mark.parametrize(
    "subdir",
    [
        "zips/estabelecimentos",  # inside the layout-owned zips dir
        "lookups/x",              # inside another table's source dir
        "zips\\estabelecimentos",  # Windows os.path accepts this separator too
        "",   # resolves landing_table(...) to the month root -- the F1.4b blocker
        ".",  # likewise
        "..",  # escapes to cnpj/, which contains every month
    ],
)
def test_a_subdir_that_is_a_path_rather_than_a_name_is_refused(monkeypatch, subdir):
    """The hole in BOTH subdir checks in test_registry.py -- uniqueness and the
    reserved-name list -- which each look total and are not.

    `zips/estabelecimentos` collides with no table, so uniqueness passes, and it
    does not equal `"zips"`, so the reserved-name check passes -- yet its stream
    reads INSIDE the layout-owned zips dir. `lookups/x` reads inside another
    table's source dir, where that table's stream discovers it recursively. Same
    defect class as the reserved names, reached past the guard built for them.

    `""` and `"."` are the sharpest: both make `landing_table(...)` the month root,
    which is precisely the state this whole branch removed."""
    trap = BronzeTable(
        name="socios",
        contract="lookup",
        table_key="bronze_cnpj_socios",
        staging="bronze_cnpj_socios_staging",
        bronze="bronze_cnpj_socios",
        quarantine="bronze_cnpj_socios_quarantine",
        subdir=subdir,
        landing=LANDING_ZIPS,
        prefix="Socios",
        constraints=(),
    )
    monkeypatch.setitem(REGISTRY, "socios", trap)

    with pytest.raises(ValueError) as excinfo:
        _assert_subdirs_are_single_path_components()
    message = str(excinfo.value)
    assert "socios" in message and repr(subdir) in message
    # Refused as MALFORMED, not as one more reserved name -- the distinction is the
    # decision recorded in the guard, and the message has to carry it.
    assert "ONE directory" in message


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
        _assert_prefixes_match_their_file_groups()
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
        _assert_prefixes_match_their_file_groups()
    assert "prefix=None" in str(excinfo.value)


def test_a_contract_claimed_by_two_tables_is_refused_at_import(monkeypatch):
    """What makes `spec_for_contract` single-valued, and therefore what makes the
    producer's landing dir a fact rather than a dict-order accident.

    The paste this refuses is F1.4b's: a `socios` entry copied from the lookup's and
    renamed everywhere except `contract`. Resolution by contract would then answer
    "lookup" with whichever entry came first, and socios' inner files would land in
    the lookup's own landing dir -- the cross-table contamination this branch
    removed."""
    trap = BronzeTable(
        name="socios",
        contract="lookup",
        table_key="bronze_cnpj_socios",
        staging="bronze_cnpj_socios_staging",
        bronze="bronze_cnpj_socios",
        quarantine="bronze_cnpj_socios_quarantine",
        subdir="socios",
        landing=LANDING_ZIPS,
        prefix=None,
        constraints=(),
    )
    monkeypatch.setitem(REGISTRY, "socios", trap)

    with pytest.raises(ValueError) as excinfo:
        _assert_no_two_tables_share_a_contract()
    message = str(excinfo.value)
    assert "socios" in message and "lookup" in message


def test_a_contract_typo_in_source_is_refused_as_a_value_error_not_an_unknown_table(
        monkeypatch):
    """UnknownTable is for an OPERATOR-SUPPLIED table name at a job boundary -- that
    is its docstring, and why it is a ValueError rather than a KeyError (so the prose
    reaches a run log unquoted). A contract typo committed to SOURCE is none of
    those: nobody supplied it, it is not an unknown *table*, and it breaks the import
    of every module that reads the registry rather than one run. This guard raised
    UnknownTable while its sibling `_assert_landing_modes_known` explained, in the
    same file, why that is the wrong exception."""
    trap = BronzeTable(
        name="lookup",
        contract="lookups",  # a real typo: plural
        table_key="bronze_cnpj_lookup",
        staging="bronze_cnpj_lookup_staging",
        bronze="bronze_cnpj_lookup",
        quarantine="bronze_cnpj_lookup_quarantine",
        subdir="lookups",
        landing=LANDING_LOCAL,
        prefix=None,
        constraints=(),
    )
    monkeypatch.setitem(REGISTRY, "lookup", trap)

    with pytest.raises(ValueError) as excinfo:
        _assert_contracts_exist()
    assert "lookups" in str(excinfo.value)
    assert not isinstance(excinfo.value, UnknownTable), (
        "a contract typo in source is not an operator's unknown table name"
    )


def test_a_masked_contract_declaring_a_check_constraint_is_refused_at_import(
        monkeypatch):
    """The paste F1.4b's own uniformity invites, refused where the value is DECLARED.

    socios is the one registered table with no CHECK, because Unity Catalog refuses
    a CHECK on a table carrying a column mask -- table-scoped, so the mask being on
    the name columns and the CHECK on `cnpj_basico` does not help. Every OTHER entry
    has the `cnpj_basico_len8` pair, so the natural edit that reintroduces this is
    making socios look like its neighbours again.

    WHY A GUARD AND NOT ONLY A TEST, which is the whole reason this is here and not
    an assertion over the live registry: the statement is issued by
    `promote_batch._assert_constraints`, AFTER the append has committed. So the
    failure is not a refused run -- it is a run that writes its rows into bronze and
    then fails, whose repair run correctly skips the committed append and fails
    again on the same statement. Unrepairable, on the one table holding personal
    names. `test_the_new_tables_carry_a_constraint_no_other_contract_could_have`
    (test_registry.py) pins today's tuple, and a CI test protects a merge, not the
    ad-hoc run of a branch whose tests have not been run.

    The trap is the exact pair that was removed, verbatim."""
    pasted = replace(
        REGISTRY["socios"],
        constraints=REGISTRY["socios"].constraints
        + (
            "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS cnpj_basico_len8",
            "ALTER TABLE {table} ADD CONSTRAINT cnpj_basico_len8 "
            "CHECK (length(trim(cnpj_basico)) = 8)",
        ),
    )
    monkeypatch.setitem(REGISTRY, "socios", pasted)

    with pytest.raises(ValueError) as excinfo:
        _assert_no_masked_contract_declares_a_check_constraint()
    message = str(excinfo.value)
    # Which table, which columns made it masked, and the UC error an operator would
    # otherwise have met at run time.
    assert "socios" in message
    assert "nome_do_representante" in message
    assert "COLUMN_MASKS_CHECK_CONSTRAINT_UNSUPPORTED" in message
    # And the fix, including that NOT NULL is NOT what is being refused -- the
    # obvious over-correction on reading this message is to strip the whole tuple.
    assert "SET NOT NULL is unaffected" in message


@pytest.mark.parametrize(
    "spelling",
    ["check (length(trim(cnpj_basico)) = 8)",
     "Check(length(trim(cnpj_basico)) = 8)"],
)
def test_a_lower_case_check_does_not_evade_the_guard(monkeypatch, spelling):
    """THE EVASION THIS GUARD SHIPPED WITH, closed and pinned.

    The pattern was `\\bCHECK\\b`, upper-case only, so a pasted entry spelling the
    keyword `check (...)` -- which Spark SQL accepts, and which is how the statement
    reads in plenty of documentation -- imported clean and hit
    `COLUMN_MASKS_CHECK_CONSTRAINT_UNSUPPORTED` at run time, after the append had
    committed. That is precisely the unrepairable failure the guard exists to move
    to import, so the guard's own case-sensitivity was a hole in it. Found
    independently by this branch's Task 5 review and by CodeRabbit on PR #6.

    Whitespace between the keyword and the parenthesis is optional here for the same
    reason: `CHECK(...)` parses, and a guard that required the space would be the
    same hole one character narrower."""
    pasted = replace(
        REGISTRY["socios"],
        constraints=REGISTRY["socios"].constraints
        + (f"ALTER TABLE {{table}} ADD CONSTRAINT cnpj_basico_len8 {spelling}",),
    )
    monkeypatch.setitem(REGISTRY, "socios", pasted)

    with pytest.raises(ValueError, match="COLUMN_MASKS_CHECK_CONSTRAINT_UNSUPPORTED"):
        _assert_no_masked_contract_declares_a_check_constraint()


def test_a_column_named_check_is_not_mistaken_for_a_constraint(monkeypatch):
    """The other half of the case change. Dropping the upper-case restriction gives
    up the protection it bought -- a future contract column called `check` -- so the
    pattern anchors on the predicate's opening parenthesis instead, which no
    `ALTER COLUMN` carries. Asserted rather than argued, because if this ever goes
    red the guard refuses a legitimate registry and nothing masked can be added."""
    named = replace(
        REGISTRY["socios"],
        constraints=("ALTER TABLE {table} ALTER COLUMN check SET NOT NULL",),
    )
    monkeypatch.setitem(REGISTRY, "socios", named)

    _assert_no_masked_contract_declares_a_check_constraint()


def test_dropping_a_check_is_still_allowed_on_a_masked_table(monkeypatch):
    """The guard refuses statements that CREATE a check, not every mention of one.

    `ALTER TABLE ... DROP CONSTRAINT IF EXISTS` succeeds against a masked table, and
    it is the FIRST STEP of masking a table that already carries a CHECK -- so a
    guard that matched `CONSTRAINT` rather than `CHECK` would refuse the migration
    out of the very incompatibility it exists to prevent.

    Not hypothetical for this repo: estabelecimentos carries `cnpj_basico_len8`
    today and holds 71.9M rows, so if it were ever masked the DROP is exactly the
    statement that would have to run first."""
    dropping = replace(
        REGISTRY["socios"],
        constraints=(
            "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS cnpj_basico_len8",
            "ALTER TABLE {table} ALTER COLUMN cnpj_basico SET NOT NULL",
        ),
    )
    monkeypatch.setitem(REGISTRY, "socios", dropping)

    _assert_no_masked_contract_declares_a_check_constraint()


def test_an_unmasked_contract_keeps_its_check_constraint():
    """GUARDS THE GUARD. Everything above would also pass if this refused EVERY
    CHECK, which would empty the constraint tuples of the three tables that are
    entitled to one -- including the lookup's `codigo_not_blank` and
    estabelecimentos' `cnpj_basico_len8`, live on 71.9M rows today. The live
    registry is the proof: it declares three CHECKs and imports."""
    _assert_no_masked_contract_declares_a_check_constraint()
    checked = [
        spec.name
        for spec in REGISTRY.values()
        if any("CHECK" in statement for statement in spec.constraints)
    ]
    assert sorted(checked) == ["empresas", "estabelecimentos", "lookup"], (
        f"expected the three unmasked tables to keep their CHECK, got {checked}"
    )


def test_the_registry_still_imports_where_pyspark_is_not_installed():
    """THE COST OF PUTTING THAT GUARD AT IMPORT, held down.

    The guard reads `opl.bronze.masking.MASKED_COLUMNS`, so this module now imports
    `masking`. That is only safe because `masking` imports
    `opl.contracts.cnpj_schemas` and nothing else -- and the day someone adds
    `from pyspark.sql import ...` to it, this module drags pyspark in behind it.

    Which breaks something specific and off-Databricks. pyspark is deliberately NOT
    a core dependency (pyproject: installing it into serverless breaks the wheel
    install); it is the `spark` extra. The EXTRACTION scripts -- `extract_cnpj`,
    which downloads ~15 GB from the RFB -- import this registry through
    `spec_for_contract` and run in environments that have no Spark at all. They
    would fail at import with an ImportError naming a package they have no reason
    to need, and the download would not start.

    A subprocess with a meta-path blocker rather than an inspection of the import
    lines: this asserts the PROPERTY (the registry imports without pyspark) instead
    of one spelling of its cause, so it also catches pyspark arriving through some
    third module rather than through `masking`. In-process would prove nothing --
    pyspark is installed here and already in `sys.modules` by the time this runs."""
    blocked = textwrap.dedent(
        """
        import sys

        class _NoPyspark:
            def find_spec(self, name, path=None, target=None):
                if name == "pyspark" or name.startswith("pyspark."):
                    raise ImportError(
                        "pyspark is not installed in the extraction environment"
                    )
                return None

        sys.meta_path.insert(0, _NoPyspark())
        from opl.bronze.registry import REGISTRY, spec_for_contract
        assert spec_for_contract("socios").bronze == "bronze_cnpj_socios"
        assert "pyspark" not in sys.modules
        print(len(REGISTRY))
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", blocked],
        capture_output=True,
        text=True,
        cwd=Path(registry.__file__).resolve().parents[3],
    )
    assert result.returncode == 0, (
        "opl.bronze.registry no longer imports without pyspark, which breaks every "
        "extraction script that runs off Databricks:\n" + result.stderr[-2000:]
    )
    assert result.stdout.strip() == str(len(REGISTRY))
