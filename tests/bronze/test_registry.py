"""What the live registry SAYS: the values it declares today, and the properties
they hold as a set.

The registry is the one place that answers "what is table X?". Its value is not less
repetition -- the names are DECLARED, not derived, because deriving them would force
renaming live Delta tables (bronze_cnpj_estab_staging abbreviated against
bronze_cnpj_estabelecimentos spelled out) to satisfy a pattern. Its value is that
each table's staging/bronze/quarantine triple lives in one literal, where it cannot
drift. Drift is the documented defect: a hardcoded quarantine name "sent estab
triagers to a table full of unrelated F1.2 lookup rows".

THE GUARDS THEMSELVES ARE EXERCISED IN `test_registry_guards.py`, split out when
F1.4b carried this file over the 800-line limit. The seam is which change breaks
which file: everything here changes when a TABLE is added, and everything there
changes when a GUARD is added. Read together they say "these values, and nothing
else may be declared" -- neither half means much alone, which is why each file's
docstring points at the other.

Nothing in this file mutates `REGISTRY`. A test that needs a spec the registry must
never contain belongs on the other side of the seam."""
from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from opl.bronze.registry import (
    FILE_FED_LANDING_MODES,
    LANDING_API,
    LANDING_GENERATED,
    LANDING_LOCAL,
    LANDING_MODES,
    LANDING_ZIPS,
    REGISTRY,
    RESERVED_SUBDIRS,
    UnknownTable,
    _malformed_subdir_reason,
    landing_dir,
    spec_for_contract,
    table_spec,
)

# Extracted to their own module by F2 Task 0, and taking the registry they validate as
# an argument -- see `opl.bronze.registry_collisions`. Nothing about the property below
# changed with the move; the two guards are still two.
from opl.bronze.registry_collisions import (
    _assert_no_two_tables_share_a_checkpoint_namespace,
    _assert_no_two_tables_share_a_delta_name,
)
from opl.config import DEFAULT
from opl.contracts.catalogue import CONTRACT_COLUMNS
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


def test_the_four_live_tables_keep_the_exact_names_they_have_today():
    """Extends the two-table pin above. These strings name live Delta tables and
    live Volume directories; changing one is a migration, not an edit.

    `table_key` and `landing` are pinned here for the same reason the two-table pin
    carries them, and it is the sharper half for the two entries F1.4b PASTES. The
    uniqueness guards cannot see a SWAP: give empresas socios' `table_key` and
    socios empresas', and all four keys are still distinct, so every guard in
    registry.py passes while each stream reads the other's Auto Loader checkpoint.
    Same for a swapped `subdir` -- distinct, unrefused, and it lands Empresas files
    in the socios directory. A per-table pin is the only thing that sees a swap,
    because a swap is wrong about IDENTITY and every other check here is about
    collision. `contract` is the one field a swap cannot survive unassisted:
    `_assert_prefixes_match_their_file_groups` refuses it at import, because
    empresas' declared prefix would then be checked against Socios' file group."""
    empresas = table_spec("empresas")
    assert empresas.staging == "bronze_cnpj_empresas_staging"
    assert empresas.bronze == "bronze_cnpj_empresas"
    assert empresas.quarantine == "bronze_cnpj_empresas_quarantine"
    assert empresas.table_key == "bronze_cnpj_empresas"
    assert empresas.subdir == "empresas"
    assert empresas.prefix == "Empresas"
    assert empresas.landing == LANDING_ZIPS

    socios = table_spec("socios")
    assert socios.staging == "bronze_cnpj_socios_staging"
    assert socios.bronze == "bronze_cnpj_socios"
    assert socios.quarantine == "bronze_cnpj_socios_quarantine"
    assert socios.table_key == "bronze_cnpj_socios"
    assert socios.subdir == "socios"
    assert socios.prefix == "Socios"
    assert socios.landing == LANDING_ZIPS


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


def test_a_table_key_may_equal_its_own_tables_bronze_name():
    """Why `_assert_no_two_tables_share_a_delta_name` and
    `_assert_no_two_tables_share_a_checkpoint_namespace` are two guards and not one
    loop over four roles.

    The obvious implementation -- one `seen` dict, four roles, one pass -- refuses
    the LIVE registry at import: `lookup.table_key` and `lookup.bronze` are both
    `bronze_cnpj_lookup`, and that is correct, not drift. A checkpoint namespace and
    a Delta table are different kinds of name in different namespaces, and one is
    allowed to spell itself like the other. Folding them together would have made
    the import of every module that reads the registry fail on the first run.

    Pinned as a property rather than left to be rediscovered: the failure is loud but
    the CAUSE is not obvious, and the natural fix on seeing it -- drop `table_key`
    from the check -- silently reopens the checkpoint-collision hole. The refusals
    themselves live in test_registry_guards.py; this is the legitimate case they must
    not object to."""
    lookup = table_spec("lookup")
    assert lookup.table_key == lookup.bronze == "bronze_cnpj_lookup"
    # Neither guard may object to that, today or after F1.4b adds two more tables.
    _assert_no_two_tables_share_a_delta_name(REGISTRY)
    _assert_no_two_tables_share_a_checkpoint_namespace(REGISTRY)


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
        if spec.landing not in FILE_FED_LANDING_MODES:
            # A GENERATED table has no downloader and therefore no file group, which
            # is the invariant that blocked registering payments until F1b Task 3.
            # Skipped here and asserted in the OTHER direction below, so it is not
            # merely unchecked: it must have no group AND no prefix, which is stronger
            # than the cross-check it is excused from.
            continue
        groups = [g for g in FILE_GROUPS.values() if g["table"] == spec.contract]
        assert groups, f"{spec.name}: no FILE_GROUPS entry feeds {spec.contract!r}"
        prefixes = {g["prefix"] for g in groups}
        expected = next(iter(prefixes)) if len(prefixes) == 1 else None
        assert spec.prefix == expected, (
            f"{spec.name}.prefix == {spec.prefix!r} but its {len(groups)} file "
            f"group(s) spell it {expected!r}"
        )


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


def test_no_table_nothing_downloads_claims_a_downloader():
    """The COMPLEMENT of the prefix cross-check, over the live registry.

    `test_every_declared_prefix_agrees_with_the_file_group_that_downloads_it` skips
    every table that is not file-fed, because no `FILE_GROUPS` entry can feed one. This
    is what stops that skip from being a hole: such a table must have NO file group and
    NO prefix. A file group would put two producers -- a downloader and this lakehouse's
    own writer -- into one landing directory that one Auto Loader reads with no glob;
    a prefix would be a false sentence in the file this repository treats as the
    answer to "what is table X?", and would enter that table into
    `test_no_two_tables_share_a_file_prefix` competing for a real producer's string.

    THE SKIP IS THE EXACT COMPLEMENT OF THE OTHER SWEEP'S, and it was `!=
    LANDING_GENERATED` until F-API Task 2. Two positively-scoped sweeps say nothing
    about a mode nobody has invented yet -- `api` was neither file-fed nor generated, so
    it fell through both -- and pasting this loop with one constant changed would have
    left a fifth mode in the same hole. Written this way the two are total over the
    registry for any set of landing modes.

    Guard the guard: with no such table registered the loop is vacuous, so the last line
    asserts exactly which non-file-fed modes the live registry reaches -- a set rather
    than "at least one", so registering a table under a new mode has to be a deliberate
    edit here rather than something this sweep absorbs."""
    modes = set()
    for spec in REGISTRY.values():
        if spec.landing in FILE_FED_LANDING_MODES:
            continue
        modes.add(spec.landing)
        fed_by = [g for g, entry in FILE_GROUPS.items() if entry["table"] == spec.contract]
        assert not fed_by, (
            f"{spec.name} lands as {spec.landing!r}, which no downloader feeds, and "
            f"FILE_GROUPS {fed_by} feed it"
        )
        assert spec.prefix is None, (
            f"{spec.name} lands as {spec.landing!r} and declares prefix {spec.prefix!r} "
            "-- a prefix is what a DOWNLOADER builds its file list from"
        )
    assert modes == {LANDING_GENERATED, LANDING_API}


# --- WHERE A SPEC LANDS: `registry_landing.landing_dir` --------------------------------
#
# IT HAD NO TEST AT ALL BEFORE F-API TASK 2, and no caller either -- the two ingest entry
# points written since it appeared each built their own root's path directly. It has one
# now (`databricks/src/bronze_ptax_ingest.py`), which is what puts its `api` branch on a
# live path; these are what say the mapping is the one intended. Nothing below mutates
# REGISTRY: `replace` makes a spec object, and `landing_dir` takes a spec rather than
# reading the dict.


@pytest.mark.parametrize(
    ("landing", "root"),
    [
        (LANDING_ZIPS, DEFAULT.landing_cnpj_root),
        (LANDING_LOCAL, DEFAULT.landing_cnpj_root),
        (LANDING_GENERATED, DEFAULT.landing_generated_root),
        (LANDING_API, DEFAULT.landing_api_root),
    ],
)
def test_each_landing_mode_resolves_to_its_own_root(landing, root):
    """Every mode, to the root `opl.config` declares for it, for one month.

    THREE ROOTS AND NOT ONE, and the whole value of this function is that a consumer asks
    the landing mode instead of knowing the layout. A table resolved to the wrong root
    does not error: it reads a directory holding another source's files, which cloudFiles
    walks RECURSIVELY and with no glob."""
    spec = replace(table_spec("lookup"), landing=landing, subdir="probe")
    resolved = landing_dir(DEFAULT, spec, "2026-08")
    assert resolved == f"{root}/2026-08/probe"


def test_a_landing_mode_no_root_serves_is_refused_rather_than_defaulted():
    """The `else` this dispatch deliberately does not have.

    A mode that fell through to either root would give the table a source directory
    belonging to something else, and the run would SUCCEED having read it. The refusal
    names the registered modes so an operator meeting it knows what the value should have
    been."""
    spec = replace(table_spec("lookup"), landing="ftp")
    with pytest.raises(ValueError) as excinfo:
        landing_dir(DEFAULT, spec, "2026-08")
    message = str(excinfo.value)
    assert "'ftp'" in message and "RECURSIVELY" in message
    for mode in LANDING_MODES:
        assert mode in message


def test_every_registered_table_has_a_contract():
    """Against the CATALOGUE, not one source's module.

    `cnpj_schemas.TABLES` was the whole answer until F1b Task 3 registered a second
    source. Asking it now would fail on `payments` -- correctly, in the sense that
    payments is not an RFB file layout, and wrongly, in the sense that the question is
    "does a contract exist?" and it does. `opl.contracts.catalogue` is where the two
    sources are joined, and it refuses at import if they ever claim one key."""
    for spec in REGISTRY.values():
        assert spec.contract in CONTRACT_COLUMNS, (
            f"{spec.name} names contract {spec.contract!r}, which no source declares"
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
        columns = CONTRACT_COLUMNS[spec.contract]
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


def test_the_new_tables_carry_a_constraint_no_other_contract_could_have():
    """The one paste `test_every_constraint_references_a_column_of_its_own_contract`
    states, in its own docstring, that it cannot catch.

    That test asks only that a statement mentions SOME column the contract has --
    and estabelecimentos, empresas and socios are all keyed on `cnpj_basico`, so
    estab's whole constraint tuple pasted onto empresas satisfies it. Constraint-to-
    contract coherence structurally cannot tell three tables with one key column
    apart.

    So each new entry carries a statement on a column that is unique to ITS contract
    -- `razao_social` for empresas, `identificador_socio` for socios -- and that is
    the field the paste would be missing. This test is what makes "would be missing"
    mean "is refused": without it, the unique constraint makes the paste visible only
    to a human reading the diff, which is the standard this phase exists to stop
    relying on.

    Exact tuples rather than "mentions the unique column", because the DDL triple is
    itself copy-pasted: a DROP naming one constraint and an ADD naming another leaves
    the old check in place on every promote, and only equality sees that.

    SOCIOS CARRIES NO CHECK, and it is the only registered table that does not.
    Unity Catalog refuses a CHECK constraint on a table carrying a column mask, and
    socios is the masked table -- probed on the live workspace, where
    `ALTER COLUMN ... SET NOT NULL` SUCCEEDED against a masked table and
    `ADD CONSTRAINT ... CHECK (...)` FAILED with
    COLUMN_MASKS_CHECK_CONSTRAINT_UNSUPPORTED. So the `cnpj_basico_len8` pair is
    gone from socios alone and both NOT NULLs remain, which is what keeps the
    anti-paste property above intact: `identificador_socio` is still the statement
    a tuple pasted from another contract would be missing. What stops the pair being
    pasted back is not this test but
    `registry._assert_no_masked_contract_declares_a_check_constraint`, at import.

    Unlike its sibling above, this pins constraints for tables that do NOT exist in
    Delta yet -- they are created by this phase's first promote. It is a pin on what
    will be APPLIED, which is the same string either way."""
    unique_to_empresas = "razao_social"
    unique_to_socios = "identificador_socio"
    for contract, columns in TABLES.items():
        if contract != "empresas":
            assert unique_to_empresas not in columns, contract
        if contract != "socios":
            assert unique_to_socios not in columns, contract

    assert table_spec("empresas").constraints == (
        "ALTER TABLE {table} ALTER COLUMN cnpj_basico SET NOT NULL",
        "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS cnpj_basico_len8",
        "ALTER TABLE {table} ADD CONSTRAINT cnpj_basico_len8 "
        "CHECK (length(trim(cnpj_basico)) = 8)",
        "ALTER TABLE {table} ALTER COLUMN razao_social SET NOT NULL",
    )
    assert table_spec("socios").constraints == (
        "ALTER TABLE {table} ALTER COLUMN cnpj_basico SET NOT NULL",
        "ALTER TABLE {table} ALTER COLUMN identificador_socio SET NOT NULL",
    )


def test_the_ptax_table_carries_constraints_no_other_contract_could_have():
    """The same anti-paste property for the third source, and it needs its own test
    because the one above is written over `cnpj_schemas.TABLES` -- PTAX is not an RFB
    file layout, so that loop cannot see it at all.

    TWO COLUMNS, and each is unique to this contract across the WHOLE catalogue rather
    than across the RFB half: a constraint tuple pasted from any other registered table
    would be missing both, and one pasted FROM here onto another table names columns that
    table does not have, so `test_every_constraint_references_a_column_of_its_own_contract`
    refuses it.

    They are also the right columns. `quote_date` is the key the FX join resolves against
    and the value this phase invites a writer to get wrong -- the endpoint is asked in
    `MM-DD-YYYY`, so the request's own spelling produces a ten-character string that
    joins to nothing while every count stays green. `cotacao_venda` is the rate gold
    converts with: a NULL there is an `amount_brl` that lowers a total by an amount nobody
    can name.

    `quote_date_iso_shape` NOW ENFORCES ITS OWN NAME, which it did not. It was
    `length(trim(quote_date)) = 10`, and ten characters is exactly what `06-19-2026` is --
    so the constraint named for the ISO shape admitted the one non-ISO spelling this phase
    invites, while its own comment named that value as the thing to worry about. Every
    other CHECK here is named for what it checks (`cnpj_basico_len8` -> `length = 8`).
    `test_the_iso_shape_check_refuses_the_apis_own_spelling_on_a_real_delta_table` is the
    behavioural half of this pin, against Delta rather than against the string.

    NO CHECK ON `currency`, asserted as an absence for the reason the payments entry
    gives -- a second currency must be a VALUE change rather than a schema change, and a
    CHECK would silently make it a migration on a live table."""
    for contract, columns in CONTRACT_COLUMNS.items():
        if contract == "ptax":
            continue
        assert "quote_date" not in columns and "cotacao_venda" not in columns, contract

    assert table_spec("ptax").constraints == (
        "ALTER TABLE {table} ALTER COLUMN quote_date SET NOT NULL",
        "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS quote_date_iso_shape",
        "ALTER TABLE {table} ADD CONSTRAINT quote_date_iso_shape CHECK "
        "(regexp_like(quote_date, '^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$'))",
        "ALTER TABLE {table} ALTER COLUMN cotacao_venda SET NOT NULL",
    )
    assert not [s for s in table_spec("ptax").constraints if "currency" in s]


def test_every_constraint_survives_being_formatted_with_its_table():
    """THE TRAP THE ISO-SHAPE FIX WALKED UP TO, closed for every table rather than one.

    `promote_batch._assert_constraints` issues `statement.format(table=tbl)`, so any
    literal brace in a constraint is a format field: the natural spelling of the new
    regex, `[0-9]{4}-[0-9]{2}-[0-9]{2}`, raises `IndexError: Replacement index 4 out of
    range` -- and it raises inside the promote, AFTER the append has committed, on the run
    that was meant to assert the constraint. The repair run then correctly skips the
    committed append and fails on the same statement.

    So this asserts what the promote actually does, with `{table}` the only field any
    statement may carry. A brace-free regex is what the PTAX entry declares; this is what
    stops the next author's `{2}` from being discovered in a workspace."""
    for spec in REGISTRY.values():
        for statement in spec.constraints:
            formatted = statement.format(table="catalog.schema.tbl")
            assert "catalog.schema.tbl" in formatted
            assert "{" not in formatted and "}" not in formatted, (
                f"{spec.name} constraint {statement!r} still holds a brace after "
                "formatting, so it carries a format field that is not {table}"
            )
