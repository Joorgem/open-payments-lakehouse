"""The registry is the one place that answers "what is table X?".

Its value is not less repetition -- the names are DECLARED, not derived, because
deriving them would force renaming live Delta tables (bronze_cnpj_estab_staging
abbreviated against bronze_cnpj_estabelecimentos spelled out) to satisfy a
pattern. Its value is that each table's staging/bronze/quarantine triple lives in
one literal, where it cannot drift. Drift is the documented defect: a hardcoded
quarantine name "sent estab triagers to a table full of unrelated F1.2 lookup
rows"."""
from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from opl.bronze.registry import (
    LANDING_LOCAL,
    LANDING_ZIPS,
    REGISTRY,
    RESERVED_SUBDIRS,
    BronzeTable,
    UnknownTable,
    _assert_contracts_exist,
    _assert_no_table_claims_a_reserved_subdir,
    _assert_no_two_tables_share_a_contract,
    _assert_prefixes_match_their_file_groups,
    _assert_subdirs_are_single_path_components,
    _malformed_subdir_reason,
    spec_for_contract,
    table_spec,
)
from opl.config import DEFAULT
from opl.contracts.cnpj_schemas import FILE_GROUPS, TABLES


def test_the_two_live_tables_keep_the_exact_names_they_have_today():
    """`prefix` and `landing` are pinned here too, because the uniqueness tests
    cover DUPLICATION and neither covers a TYPO. `prefix="Estabelecimento"`
    (singular) is unique, passes every other test, and under-ingests silently --
    the same defect class the uniqueness tests close, reached by another route."""
    lookup = table_spec("lookup")
    assert lookup.staging == "bronze_cnpj_lookup_staging"
    assert lookup.bronze == "bronze_cnpj_lookup"
    assert lookup.quarantine == "bronze_cnpj_lookup_quarantine"
    assert lookup.table_key == "bronze_cnpj_lookup"
    assert lookup.subdir == "lookups"
    assert lookup.prefix is None
    assert lookup.landing == LANDING_LOCAL

    estab = table_spec("estabelecimentos")
    assert estab.staging == "bronze_cnpj_estab_staging"
    assert estab.bronze == "bronze_cnpj_estabelecimentos"
    assert estab.quarantine == "bronze_cnpj_estab_quarantine"
    assert estab.table_key == "bronze_cnpj_estab"
    assert estab.subdir == "estabelecimentos"
    assert estab.prefix == "Estabelecimentos"
    assert estab.landing == LANDING_ZIPS


def test_no_two_tables_share_a_staging_bronze_or_quarantine_name():
    """The defect class this registry exists to close, asserted directly.

    Checked ACROSS the three roles, not within each: a table whose quarantine
    equals another's staging would route rejects into a table a promote reads."""
    seen: dict[str, str] = {}
    for spec in REGISTRY.values():
        for role, value in (
            ("staging", spec.staging),
            ("bronze", spec.bronze),
            ("quarantine", spec.quarantine),
        ):
            assert value not in seen, (
                f"{spec.name}.{role} == {value!r}, already used by {seen[value]}"
            )
            seen[value] = f"{spec.name}.{role}"


def test_no_two_tables_share_a_landing_subdir():
    """Same defect as a shared quarantine, one layer down in the Volume.

    Two tables pointed at one landing directory is the recursive-discovery
    failure F1.3 documented: a stream reading a dir it was not meant to read
    ingested a probe planted in a sibling subdir. F1.4b adds Empresas and Socios
    by copy-pasting these entries, and `subdir` is one of the fields that does
    NOT contain the table's own bronze name -- so a careful find/replace over
    `bronze_cnpj_*` sails straight past it."""
    seen: dict[str, str] = {}
    for spec in REGISTRY.values():
        assert spec.subdir not in seen, (
            f"{spec.name}.subdir == {spec.subdir!r}, already used by {seen[spec.subdir]}"
        )
        seen[spec.subdir] = spec.name


def test_no_table_claims_a_directory_the_volume_layout_owns():
    """The hole `test_no_two_tables_share_a_landing_subdir` structurally cannot see.

    That test compares tables against EACH OTHER. `subdir="zips"` collides with no
    table, so it passes -- and hands that table `cnpj/<month>/zips` as its source
    dir, which cloudFiles walks RECURSIVELY (F1.3, empirically: a probe.txt in
    `zips/estabelecimentos/` was ingested by a stream reading the month root). That
    one stream would swallow every other table's raw ZIPs and the multi-gigabyte
    `.ESTABELE` extracts, and it would do so as a SUCCESSFUL run -- which is why the
    refusal is at import and not left to a consumer to notice."""
    for spec in REGISTRY.values():
        assert spec.subdir not in RESERVED_SUBDIRS, (
            f"{spec.name}.subdir == {spec.subdir!r}, reserved by the Volume layout"
        )


def test_the_reserved_names_are_the_layout_s_own_and_not_a_stale_list():
    """Pins the DERIVATION, not just the values.

    `zips` is asserted against the path `landing_zips` actually builds, so renaming
    that directory without moving the reservation fails here rather than leaving a
    guard that reserves a name nothing uses. The literal on the right is the point:
    it is what an operator sees in the Volume."""
    assert DEFAULT.landing_zips("t", "2026-06").startswith(
        f"{DEFAULT.landing_cnpj_month('2026-06')}/zips/"
    )
    assert DEFAULT.landing_tmp("t", "2026-06").startswith(f"{DEFAULT.volume_root}/_tmp/")
    assert RESERVED_SUBDIRS == frozenset({"zips", "_tmp", "_schemas", "_checkpoints"})


def test_a_table_claiming_a_reserved_subdir_is_refused_by_name(monkeypatch):
    """The refusal itself, exercised -- the test above only proves today's entries
    are clean, which would stay green if the guard were deleted.

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
    """The hole in BOTH checks above, which each look total and are not.

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


def test_the_live_subdirs_are_single_directory_names():
    for spec in REGISTRY.values():
        assert _malformed_subdir_reason(spec.subdir) is None, (
            f"{spec.name}.subdir == {spec.subdir!r} is not a single directory name"
        )


def test_no_two_tables_share_a_file_prefix():
    """The third leg of the copy-paste trio, and the one that fails silently.

    A stale `subdir` collides two tables in one landing dir; a stale `prefix`
    just goes looking for files that are not there and under-ingests without
    erroring -- the class this project rejected globs for. In the Scenario B
    probe a stale prefix was caught only INCIDENTALLY, because that paste left
    `subdir` stale too; a paste that fixes `subdir` and misses `prefix` passed
    everything until this test existed.

    `None` is skipped, not defaulted, because the lookup's absent prefix is a
    real property and not a gap: its six lookups arrive as six differently-named
    single files routed into one table by filename suffix
    (`opl.bronze.lookup_routing`), so no single prefix identifies them. Treating
    absent as a value to be filled in would invent a prefix that cannot exist;
    treating two absences as a collision would forbid a second such table."""
    seen: dict[str, str] = {}
    for spec in REGISTRY.values():
        if spec.prefix is None:
            continue
        assert spec.prefix not in seen, (
            f"{spec.name}.prefix == {spec.prefix!r}, already used by {seen[spec.prefix]}"
        )
        seen[spec.prefix] = spec.name


def test_every_declared_prefix_agrees_with_the_file_group_that_downloads_it():
    """`prefix` has no production reader -- `cnpj_source.expected_files` builds the
    download list from `FILE_GROUPS[g]["prefix"]` -- so until F1.4a it was a SECOND
    SPELLING of a live value with nothing asserting the two agreed. That is the
    drift this whole registry exists to remove, present in the registry itself.

    Kept and tied down rather than deleted, because the field carries a net F1.4b is
    about to be tested by (see `test_no_two_tables_share_a_file_prefix`) and because
    carry-forward #10 asked for the prefix to be DECLARED. What the assertion buys
    is that the declaration can no longer be independently wrong: it either matches
    the string the downloader uses, or the import fails."""
    for spec in REGISTRY.values():
        groups = [g for g in FILE_GROUPS.values() if g["table"] == spec.contract]
        assert groups, f"{spec.name}: no FILE_GROUPS entry feeds {spec.contract!r}"
        prefixes = {g["prefix"] for g in groups}
        expected = next(iter(prefixes)) if len(prefixes) == 1 else None
        assert spec.prefix == expected, (
            f"{spec.name}.prefix == {spec.prefix!r} but its {len(groups)} file "
            f"group(s) spell it {expected!r}"
        )


def test_a_prefix_that_disagrees_with_its_file_group_is_refused_at_import(monkeypatch):
    """The refusal exercised, not just today's entries proved clean.

    `Estabelecimento` (singular) is the probe on purpose: it is unique, it is a
    single directory name, it names no reserved dir, and it passes every other check
    in this file. What it does is go looking for files that are not there and
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


def test_a_file_group_resolves_to_the_table_that_owns_its_contract():
    """What the extraction scripts ask, and the only question they may ask.

    `FILE_GROUPS[g]["table"]` is a CONTRACT key, not a registry key, so this goes
    through `spec_for_contract`. The six lookup groups collapse onto ONE spec, which
    is what makes the landing dir single."""
    assert spec_for_contract("estabelecimentos") is table_spec("estabelecimentos")
    resolved = {spec_for_contract(FILE_GROUPS[g]["table"]).name
                for g in ("Cnaes", "Motivos", "Qualificacoes")}
    assert resolved == {"lookup"}


def test_an_unregistered_contract_is_refused_and_says_what_to_do():
    """Simples: a real RFB group, a real contract, no bronze table. The producer
    must not answer this with the month root, which is where the six lookups used to
    sit loose and which no stream reads any more."""
    with pytest.raises(UnknownTable) as excinfo:
        spec_for_contract("simples")
    message = str(excinfo.value)
    assert "simples" in message
    assert "lookup" in message and "estabelecimentos" in message
    assert "opl.bronze.registry" in message


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


def test_every_registered_table_has_a_contract():
    for spec in REGISTRY.values():
        assert spec.contract in TABLES, f"{spec.name} names contract {spec.contract!r}"


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


def test_every_constraint_references_a_column_of_its_own_contract():
    """Catches constraints copy-pasted from a table with different key columns.

    Deliberately a substring check against the contract's column list and NOT a
    regex that extracts column names from the DDL: a half-parser for SQL would be
    more fragile than the gap it closes, and would fail on the next constraint
    shape nobody anticipated. Asking only that each statement mentions at least
    one column the table actually has is enough to catch a wholesale copy-paste.

    Known and accepted limit: it cannot catch a paste between two tables that
    share a key column -- estabelecimentos, empresas and socios are all keyed on
    `cnpj_basico`, so estab's constraints on a socios entry would satisfy this.
    `test_no_two_tables_share_a_landing_subdir` is what catches that same paste."""
    for spec in REGISTRY.values():
        columns = TABLES[spec.contract]
        for statement in spec.constraints:
            assert any(column in statement for column in columns), (
                f"{spec.name} constraint {statement!r} names no column of its "
                f"contract {spec.contract!r} ({', '.join(columns)}) -- "
                "constraints copy-pasted from another table?"
            )


def test_every_registered_table_has_a_checkpoint_namespace_of_its_own():
    keys = [spec.table_key for spec in REGISTRY.values()]
    assert len(keys) == len(set(keys)), f"table_key collision in {keys}"


def test_an_unknown_table_is_refused_by_name_and_lists_the_valid_ones():
    with pytest.raises(UnknownTable) as excinfo:
        table_spec("estabelecimento")  # a real typo: singular
    message = str(excinfo.value)
    assert "estabelecimento" in message
    assert "estabelecimentos" in message and "lookup" in message


def test_the_refusal_reaches_an_operator_as_prose_not_as_a_repr():
    """Why UnknownTable is a ValueError and not a KeyError, pinned.

    `KeyError.__str__` re-`repr`s its argument, so this message -- written to be
    read in a Databricks run log -- would arrive wrapped in quotes with escaped
    newlines. Regressing the base class to KeyError makes this test fail rather
    than quietly degrading every operator-facing refusal."""
    with pytest.raises(UnknownTable) as excinfo:
        table_spec("estabelecimento")
    message = str(excinfo.value)
    assert not message.startswith(("'", '"')), f"message arrived repr-wrapped: {message}"
    assert message.startswith("unknown bronze table")
    # And it must not be swallowable by an `except KeyError` that never named it.
    assert not isinstance(excinfo.value, KeyError)


def test_a_spec_is_frozen():
    """Narrowed from a blind `Exception` (ruff B017): a bare `Exception` would
    also be satisfied by `table_spec` itself blowing up, so the test could stay
    green while proving nothing about frozen-ness. FrozenInstanceError is what a
    frozen dataclass raises and nothing else here does."""
    spec = table_spec("lookup")
    with pytest.raises(FrozenInstanceError):
        spec.staging = "something_else"  # type: ignore[misc]


def test_the_constraints_are_the_ones_the_live_tables_carry():
    assert table_spec("lookup").constraints == (
        "ALTER TABLE {table} ALTER COLUMN codigo SET NOT NULL",
        "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS codigo_not_blank",
        "ALTER TABLE {table} ADD CONSTRAINT codigo_not_blank "
        "CHECK (length(trim(codigo)) > 0)",
    )
    assert table_spec("estabelecimentos").constraints == (
        "ALTER TABLE {table} ALTER COLUMN cnpj_basico SET NOT NULL",
        "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS cnpj_basico_len8",
        "ALTER TABLE {table} ADD CONSTRAINT cnpj_basico_len8 "
        "CHECK (length(trim(cnpj_basico)) = 8)",
    )
