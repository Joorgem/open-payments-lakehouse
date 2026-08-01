"""One literal per bronze table: the single answer to "what is table X?".

WHY DECLARED AND NOT DERIVED: the live names follow no single pattern --
`bronze_cnpj_estab_staging` is abbreviated where `bronze_cnpj_estabelecimentos`
is spelled out, and the lookup uses `lookup` where estab uses `estab`. Deriving
`f"bronze_cnpj_{name}_staging"` would be DRY-er and would force renaming Delta
tables, one of them holding 71,874,448 rows, to satisfy an aesthetic.

So the point is not less repetition. The point is that each table's
staging/bronze/quarantine TRIPLE lives in one literal, where it cannot drift --
and drift is the documented defect: a quarantine name hardcoded in a job YAML
"sent estab triagers to a table full of unrelated F1.2 lookup rows"."""
from __future__ import annotations

import re
from dataclasses import dataclass

from opl.bronze.masking import MASKED_COLUMNS
from opl.config import OplConfig
from opl.contracts.cnpj_schemas import FILE_GROUPS, TABLES

# How a table's raw files reach the Volume.
LANDING_ZIPS = "zips"    # PUT the zip, unzip in the Volume (multi-part groups)
LANDING_LOCAL = "local"  # unzip locally, PUT the inner file (the tiny lookups)
LANDING_MODES = frozenset({LANDING_ZIPS, LANDING_LOCAL})

# Values that make the derivation below independent of any real table or month --
# they are joined into a path and the added component is read back out, so they
# only have to be non-empty and contain no separator.
_LAYOUT_PROBE_TABLE = "__probe__"
_LAYOUT_PROBE_MONTH = "__month__"

# What disqualifies a `subdir` from being a single directory name. Both separators
# because Windows `os.path` accepts either; the aliases because each resolves
# `landing_table(...)` back onto a directory that already exists -- `""` and `"."`
# onto the month root (the F1.4b blocker itself), `".."` onto `cnpj/`, every month.
_PATH_SEPARATORS = ("/", "\\")
_RELATIVE_ALIASES = frozenset({"", ".", ".."})


def _component_under(path: str, prefix: str) -> str:
    """The single directory `path` adds directly under `prefix`.

    Raises rather than returning something wrong if `path` is not under `prefix`:
    that means the Volume layout moved, and a guard that silently derives the
    empty string from a moved layout reserves nothing while still looking green."""
    if not path.startswith(f"{prefix}/"):
        raise ValueError(
            f"the Volume layout moved: {path!r} is no longer under {prefix!r}, so "
            "RESERVED_SUBDIRS can no longer derive the names it has to reserve"
        )
    return path[len(prefix) + 1:].split("/", 1)[0]


def _reserved_subdirs() -> frozenset[str]:
    """Directory names no table's `subdir` may claim.

    DERIVED from `OplConfig`, not restated as a literal list, so the guard cannot
    drift from the layout it guards: renaming the zips directory in one place moves
    this reservation with it, where a literal would go on reserving a name nothing
    uses and stop reserving the one that matters.

    `zips` is the LIVE collision and the reason this exists. `landing_zips` puts
    every table's raw ZIPs at `cnpj/<month>/zips/<table>`, a SIBLING of each
    table's `landing_table` dir -- so a table registered with `subdir="zips"` would
    get `cnpj/<month>/zips` as its source dir, and because cloudFiles walks a source
    dir RECURSIVELY (F1.3, empirically: a probe.txt planted in
    `zips/estabelecimentos/` was ingested by a stream reading the month root) that
    one stream would swallow every other table's ZIPs and the multi-gigabyte
    `.ESTABELE` extracts. Subdir UNIQUENESS cannot see this: `zips` collides with no
    other table's subdir.

    `_tmp`, `_schemas` and `_checkpoints` are reserved DEFENSIVELY, and are a weaker
    case worth stating honestly: all three live under `volume_root`, not under
    `cnpj/<month>`, so `cnpj/<month>/_tmp` would not actually collide with
    `volume_root/_tmp` today. They are refused because a leading-underscore
    directory means "state, not data" everywhere else in this Volume, and an
    operator listing a month who found `_schemas` there would read it as state.

    `_tmp` is derived like `zips`. `_schemas` and `_checkpoints` are literals: they
    are built by `opl.bronze.autoloader.schema_location` / `checkpoint_location`,
    and `autoloader` imports THIS module, so deriving them would be a cycle. If
    either is renamed there and not here, this guard stops reserving the old name --
    the only consequence is that the weaker defensive half goes stale; the live
    `zips` collision stays covered either way."""
    cfg = OplConfig()
    month_root = cfg.landing_cnpj_month(_LAYOUT_PROBE_MONTH)
    return frozenset(
        {
            _component_under(
                cfg.landing_zips(_LAYOUT_PROBE_TABLE, _LAYOUT_PROBE_MONTH), month_root
            ),
            _component_under(
                cfg.landing_tmp(_LAYOUT_PROBE_TABLE, _LAYOUT_PROBE_MONTH), cfg.volume_root
            ),
            "_schemas",
            "_checkpoints",
        }
    )


RESERVED_SUBDIRS = _reserved_subdirs()


class UnknownTable(ValueError):
    """A table name that is not registered. Raised before Spark, on purpose.

    A ValueError, NOT a KeyError, though the lookup it guards is a dict lookup.
    Two reasons, both learned the hard way from messages that had to survive into
    a Databricks run log:

    1. `KeyError.__str__` re-`repr`s its argument. The message below is prose
       written to be read by an operator at 3am; raised as a KeyError it arrives
       quoted, with escaped newlines, as `"unknown bronze table 'x' -- ..."`.
    2. A KeyError is silently catchable by code that never named it. `table_spec`
       is called from job entry points, and entry points are exactly where
       `except KeyError` wrappers around argument parsing live -- one of those
       several frames up would swallow a mistyped table name and replace this
       message with a generic usage line, which is the opposite of the point.

    The table name is an operator-supplied value validated at a boundary, which
    is ValueError's job, and matches how `require_batch_id` refuses."""


@dataclass(frozen=True, kw_only=True)
class BronzeTable:
    """Everything table-specific about one bronze table. Frozen: config is data.

    `kw_only`: `landing` and `prefix` are adjacent and both str-ish, so a
    positional construction that swapped them would type-check and silently point
    a table at the wrong landing mode. Keyword-only makes the field order unable
    to matter rather than merely currently-harmless -- every construction site
    already passes keywords, so this costs nothing and closes the trap."""

    name: str
    contract: str
    table_key: str
    staging: str
    bronze: str
    quarantine: str
    subdir: str
    landing: str
    prefix: str | None
    # DDL re-asserted after every promote. `{table}` is filled with the qualified
    # bronze name by the caller. A tuple, not a list: the spec is frozen and its
    # fields have to be too, or `constraints.append(...)` would mutate shared state.
    constraints: tuple[str, ...]


REGISTRY: dict[str, BronzeTable] = {
    "lookup": BronzeTable(
        name="lookup",
        contract="lookup",
        table_key="bronze_cnpj_lookup",
        staging="bronze_cnpj_lookup_staging",
        bronze="bronze_cnpj_lookup",
        quarantine="bronze_cnpj_lookup_quarantine",
        subdir="lookups",
        landing=LANDING_LOCAL,
        # The six lookups arrive as six differently-named single files, routed to
        # one table by filename suffix (opl.bronze.lookup_routing), so no single
        # prefix identifies them.
        prefix=None,
        constraints=(
            "ALTER TABLE {table} ALTER COLUMN codigo SET NOT NULL",
            "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS codigo_not_blank",
            "ALTER TABLE {table} ADD CONSTRAINT codigo_not_blank "
            "CHECK (length(trim(codigo)) > 0)",
        ),
    ),
    "estabelecimentos": BronzeTable(
        name="estabelecimentos",
        contract="estabelecimentos",
        table_key="bronze_cnpj_estab",
        staging="bronze_cnpj_estab_staging",
        bronze="bronze_cnpj_estabelecimentos",
        quarantine="bronze_cnpj_estab_quarantine",
        subdir="estabelecimentos",
        landing=LANDING_ZIPS,
        # Explicit rather than implied by the FILE_GROUPS dict key (carry-forward
        # #10): the key happening to equal the prefix is a coincidence nothing
        # enforces, and a group whose key drifted from its prefix would go looking
        # for files that are not there. It is a SECOND SPELLING of the prefix the
        # downloader actually uses, which is why
        # `_assert_prefixes_match_their_file_groups` makes it a cross-check on that
        # one instead of an independent claim -- unasserted, it was #10 paid in
        # name only.
        prefix="Estabelecimentos",
        constraints=(
            "ALTER TABLE {table} ALTER COLUMN cnpj_basico SET NOT NULL",
            "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS cnpj_basico_len8",
            "ALTER TABLE {table} ADD CONSTRAINT cnpj_basico_len8 "
            "CHECK (length(trim(cnpj_basico)) = 8)",
        ),
    ),
    # The two F1.4b entries. Written as a PASTE of the one above, deliberately and
    # under the guards built for exactly that in Tasks 1-3: `subdir`, `table_key`,
    # the staging/bronze/quarantine triple, `contract` and `prefix` are each refused
    # at import if one of them is left stale. What no guard can see is a SWAP between
    # these two entries -- swapped subdirs are still unique, so uniqueness is blind to
    # them -- which is why every field of both is pinned per table in
    # `test_the_four_live_tables_keep_the_exact_names_they_have_today`.
    "empresas": BronzeTable(
        name="empresas",
        contract="empresas",
        table_key="bronze_cnpj_empresas",
        staging="bronze_cnpj_empresas_staging",
        bronze="bronze_cnpj_empresas",
        quarantine="bronze_cnpj_empresas_quarantine",
        subdir="empresas",
        landing=LANDING_ZIPS,
        prefix="Empresas",
        constraints=(
            "ALTER TABLE {table} ALTER COLUMN cnpj_basico SET NOT NULL",
            "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS cnpj_basico_len8",
            "ALTER TABLE {table} ADD CONSTRAINT cnpj_basico_len8 "
            "CHECK (length(trim(cnpj_basico)) = 8)",
            # razao_social, not a second cnpj_basico rule: the constraint set is
            # what a copy-paste from estabelecimentos would leave IDENTICAL, and
            # all three CNPJ contracts key on cnpj_basico -- so a constraint that
            # is unique to THIS contract is what makes the paste visible.
            # `test_every_constraint_references_a_column_of_its_own_contract` says
            # in its own docstring that it cannot see that paste (cnpj_basico is a
            # column of all three); what turns "visible" into "refused" is
            # `test_the_new_tables_carry_a_constraint_no_other_contract_could_have`.
            "ALTER TABLE {table} ALTER COLUMN razao_social SET NOT NULL",
        ),
    ),
    "socios": BronzeTable(
        name="socios",
        contract="socios",
        table_key="bronze_cnpj_socios",
        staging="bronze_cnpj_socios_staging",
        bronze="bronze_cnpj_socios",
        quarantine="bronze_cnpj_socios_quarantine",
        subdir="socios",
        landing=LANDING_ZIPS,
        prefix="Socios",
        # THE ONE REGISTERED TABLE WITH NO CHECK CONSTRAINT, because it is the masked
        # one: UC refuses a CHECK on a table carrying a mask, TABLE-scoped, so the
        # masks being on the name columns does not spare a CHECK on `cnpj_basico`.
        # Probed -- `SET NOT NULL` SUCCEEDED against a masked table and `ADD
        # CONSTRAINT ... CHECK (...)` FAILED -- so the loss is exactly the
        # `cnpj_basico_len8` pair and both NOT NULLs stay. WHAT IT COSTS, rather than
        # glossed: bronze stops re-asserting the 8-character key declaratively. That
        # was defence in depth, not the control -- `bad_cnpj_basico_length` rejects
        # those rows in the DQ gate, BEFORE the promote. ADR 0008 has both in full.
        constraints=(
            "ALTER TABLE {table} ALTER COLUMN cnpj_basico SET NOT NULL",
            # identificador_socio for empresas' razao_social reason: it is the one
            # column above that no other registered contract has, so it is the
            # statement a pasted constraint tuple would be missing.
            "ALTER TABLE {table} ALTER COLUMN identificador_socio SET NOT NULL",
        ),
    ),
}


def table_spec(name: str) -> BronzeTable:
    """The registered spec for `name`, or refuse naming the valid alternatives.

    Refuses BEFORE Spark, like `require_batch_id`: an operator who mistyped a
    table should not wait for a serverless session to be told so."""
    try:
        return REGISTRY[name]
    except KeyError:
        valid = ", ".join(sorted(REGISTRY))
        raise UnknownTable(
            f"unknown bronze table {name!r} -- registered tables are: {valid}. "
            "Every job task takes the table name as a parameter; check the "
            "`table` parameter of the job that failed."
        ) from None


def spec_for_contract(contract: str) -> BronzeTable:
    """The registered table whose rows come from `contract`'s files, or refuse.

    THE PRODUCER'S ENTRY POINT, and the reason it is not `table_spec`. The
    extraction scripts start from a `FILE_GROUPS` key, and that entry's `table`
    value is a CONTRACT key (`cnpj_schemas.TABLES`), not a registry key. The two
    happen to be the same string for both entries below, and NOTHING makes them
    the same: `table_spec(FILE_GROUPS[g]["table"])` would read as correct while
    resolving through that coincidence. F1.4b adds `socios` by copy-pasting these
    entries, and a paste that renames the entry but leaves `contract="lookup"`
    would answer `table_spec("socios")` with a spec whose `subdir` is `lookups` --
    so the extraction would land socios' inner files in the LOOKUP's landing dir,
    which is the cross-table contamination this branch removed, re-entered from
    the producer's side.

    Asking by contract is the question the producer actually has ("where do this
    file group's rows live?"), and it answers that paste with a refusal: no spec
    declares contract `socios`. Single-valued by
    `_assert_no_two_tables_share_a_contract`, so the first match is the only one.

    Refuses BEFORE anything is downloaded, for `table_spec`'s reason: an operator
    who named a group with no bronze table should not learn that after several GB
    are on the wire."""
    for spec in REGISTRY.values():
        if spec.contract == contract:
            return spec
    registered = ", ".join(sorted(spec.contract for spec in REGISTRY.values()))
    raise UnknownTable(
        f"no bronze table is registered for contract {contract!r} -- registered "
        f"contracts are: {registered}. Nothing may be landed in the Volume for it: "
        "a landing directory belongs to a registered table, and the one directory "
        "that needs no entry is the month ROOT, which is exactly where the six "
        "lookup CSVs used to sit loose and which no stream reads any more. Register "
        "the table in opl.bronze.registry before landing its files, or leave this "
        "group out of the run (--no-upload still downloads it)."
    )


def _assert_contracts_exist() -> None:
    """Fail at import if a spec names a contract that does not exist.

    At import rather than at use: a registry entry pointing at a missing contract
    is a typo, and a typo should not wait for the one job run that touches that
    table to surface.

    A plain ValueError, NOT UnknownTable, for the reason `_assert_landing_modes_
    known` states below and this function contradicted until the F1.4a review:
    UnknownTable's docstring describes an OPERATOR-SUPPLIED table name arriving at
    a job boundary, and it is a ValueError rather than a KeyError so that prose
    reaches an operator's run log unquoted. A contract typo committed to SOURCE is
    neither -- it is not an unknown *table*, no operator supplied it, and it
    breaks the import of every module that reads the registry rather than one
    run."""
    for spec in REGISTRY.values():
        if spec.contract not in TABLES:
            raise ValueError(
                f"{spec.name} names contract {spec.contract!r}, which is not in "
                f"cnpj_schemas.TABLES ({', '.join(sorted(TABLES))})"
            )


def _assert_no_two_tables_share_a_contract() -> None:
    """Fail at import if two specs claim the same contract.

    Not bookkeeping: `spec_for_contract` resolves a `FILE_GROUPS` entry to the
    table its files land for, and two specs on one contract makes that answer
    arbitrary -- the producer would land a group's files in whichever entry dict
    order reached first. Refused where it is DECLARED for the same reason as the
    landing modes: a consumer that takes the first match does not fail, it
    succeeds against the wrong directory.

    A plain ValueError: nothing here is an unknown table."""
    seen: dict[str, str] = {}
    for spec in REGISTRY.values():
        if spec.contract in seen:
            raise ValueError(
                f"{spec.name} and {seen[spec.contract]} both declare contract "
                f"{spec.contract!r}. `spec_for_contract` maps a FILE_GROUPS entry to "
                "the ONE table its files land for, so two claimants make that answer "
                "depend on dict order -- and the loser's landing subdir silently "
                "receives the winner's files. Give each table its own contract in "
                "cnpj_schemas.TABLES."
            )
        seen[spec.contract] = spec.name


# A statement that CREATES a CHECK constraint. `DROP CONSTRAINT IF EXISTS` is
# deliberately NOT matched: dropping a CHECK is legal on a masked table, and is in
# fact the first step of masking a table that already carries one -- so refusing it
# would refuse the migration out of this very incompatibility. Word-bounded and
# upper-case so it cannot fire on a future column whose name contains "check".
_CREATES_A_CHECK = re.compile(r"\bCHECK\b")


def _masked_check_collision(
    spec: BronzeTable, masked: tuple[str, ...], statement: str
) -> str:
    """The refusal text for a masked table declaring a CHECK.

    Extracted like `_delta_name_collision`, for its reason: the message is most of
    the guard by volume and inlining it puts that function past 50 lines. It names
    the UC error conditions so they can be searched for, and says explicitly that
    NOT NULL is not what is refused -- the obvious over-correction on meeting this
    message is to empty the constraint tuple."""
    return (
        f"{spec.name} declares a CHECK constraint and its contract "
        f"{spec.contract!r} is masked ({', '.join(masked)}). Unity Catalog refuses "
        "the two on ONE TABLE -- COLUMN_MASKS_CHECK_CONSTRAINT_UNSUPPORTED adding "
        "the CHECK to a masked table, COLUMN_MASKS_FEATURE_NOT_SUPPORTED."
        "CHECK_CONSTRAINT masking a CHECKed one -- and the refusal is table-scoped, "
        "so it does not help that the mask and the CHECK are on different columns. "
        f"The statement: {statement!r}. This is refused at IMPORT because "
        "promote_batch issues it AFTER the append commits: the run would write its "
        "rows into bronze and then fail, and the repair run, which correctly skips "
        "the committed append, would fail again on the same statement. Remove it, "
        "together with the DROP CONSTRAINT IF EXISTS that only existed to make it "
        "re-runnable -- the DQ rule that rejects those rows runs in the GATE, before "
        "the promote, so nothing violating the CHECK can reach bronze. ALTER COLUMN "
        "... SET NOT NULL is unaffected and may stay. See ADR 0008."
    )


def _assert_no_masked_contract_declares_a_check_constraint() -> None:
    """Fail at import if a table whose contract is MASKED also declares a CHECK.

    UC refuses the two on one table, in both directions
    (`COLUMN_MASKS_CHECK_CONSTRAINT_UNSUPPORTED` adding the CHECK to a masked table,
    `COLUMN_MASKS_FEATURE_NOT_SUPPORTED.CHECK_CONSTRAINT` masking a CHECKed one),
    and the refusal is TABLE-scoped. Probed: `SET NOT NULL` against a masked table
    SUCCEEDED, so this refuses a CHECK and says nothing about nullability. ADR 0008
    carries the probe.

    AT IMPORT, HERE, AND NOT IN A TEST OR IN `opl.bronze.masking`. The statement is
    issued by `promote_batch._assert_constraints`, which runs AFTER the append has
    committed -- so getting this wrong produces a run that writes its rows into
    bronze and THEN fails, whose repair run correctly skips the committed append and
    fails again on the same statement: an unrepairable task, on the one table
    holding personal names. A CI test protects a merge, not the ad-hoc run of a
    branch whose tests have not been run; and `promote_batch` -- which issues the
    statement -- imports the registry and NOT `masking`, so a guard living there
    would never run inside it. That import direction is the load-bearing fact, not
    the importer count: this module imports `masking` too. Every job task imports
    the registry.

    THE IMPORT DIRECTION IS LOAD-BEARING, and it is what lets this be a guard rather
    than a test: `opl.bronze.masking` imports `opl.contracts.cnpj_schemas` and
    NOTHING else -- no pyspark, no registry. This module is imported by the
    EXTRACTION scripts, which run off Databricks where pyspark is an optional extra
    usually not installed, so a pyspark import arriving here through `masking` would
    break `extract_cnpj` with an ImportError for a package it has no reason to need.
    `masking` must never import `registry`, and
    `test_the_registry_still_imports_where_pyspark_is_not_installed` keeps both
    halves true.

    A plain ValueError: nothing here is an unknown table."""
    for spec in REGISTRY.values():
        masked = MASKED_COLUMNS.get(spec.contract)
        if not masked:
            continue
        for statement in spec.constraints:
            if _CREATES_A_CHECK.search(statement):
                raise ValueError(_masked_check_collision(spec, masked, statement))


def _file_group_prefixes(contract: str) -> list[str]:
    """The distinct FILE_GROUPS prefixes whose zips feed `contract`, sorted."""
    return sorted({g["prefix"] for g in FILE_GROUPS.values() if g["table"] == contract})


def _assert_prefixes_match_their_file_groups() -> None:
    """Fail at import if a spec's `prefix` disagrees with FILE_GROUPS.

    WHY THE FIELD SURVIVES AT ALL, since no production code reads it:
    `cnpj_source.expected_files` builds the download list from
    `FILE_GROUPS[g]["prefix"]` and has to, because the lookup's six differently-
    named files have no single prefix to build from. So `prefix` is a SECOND
    SPELLING of a live value -- the exact shape of the drift this registry exists
    to remove -- and the F1.4a review offered two ways out: delete it, or tie it
    down.

    TIED DOWN, because the field is not dead weight. Carry-forward #10 asked for
    the prefix to be DECLARED rather than implied by a dict key;
    `test_no_two_tables_share_a_file_prefix` is one of the three copy-paste locks
    F1.4b is about to be tested by; and `prefix="Estabelecimento"` (singular) is
    unique, passes every other check, and under-ingests SILENTLY. Deleting the
    field would delete that net one commit before the phase that copy-pastes these
    entries. What this assertion does is convert the duplicate into a CROSS-CHECK:
    the registry's spelling must agree with the one the downloader uses, so the
    two can no longer drift -- they can only fail this import.

    THREE CASES, all stated so none is silent. Exactly one group feeding a
    contract means the prefix is that group's. More than one means no single prefix
    identifies the files (the lookup: six groups, routed into one table by filename
    suffix), so `None` is the only correct value -- `None` is a real property here,
    not a gap to be filled in. NO group feeding a registered contract is refused:
    that table has no producer at all, so its landing dir would never receive a
    file and the job built on it would report SUCCESS having ingested zero rows."""
    for spec in REGISTRY.values():
        prefixes = _file_group_prefixes(spec.contract)
        if not prefixes:
            raise ValueError(
                f"{spec.name} names contract {spec.contract!r}, which no "
                "cnpj_schemas.FILE_GROUPS entry feeds. Nothing would ever be "
                "downloaded or landed for it, and its ingest would report SUCCESS "
                "having read an empty source dir -- add the RFB file group, or drop "
                "the registry entry."
            )
        expected = prefixes[0] if len(prefixes) == 1 else None
        if spec.prefix != expected:
            raise ValueError(
                f"{spec.name} declares prefix {spec.prefix!r}, but the "
                f"{len(prefixes)} FILE_GROUPS entry/entries feeding its contract "
                f"{spec.contract!r} spell it {expected!r} ({', '.join(prefixes)}). "
                "The download list is built from the FILE_GROUPS prefix, so this "
                "field is a second spelling of it and is asserted rather than "
                "trusted: a prefix that is merely a typo (Estabelecimento, singular) "
                "collides with nothing and under-ingests without erroring. A table "
                "fed by SEVERAL groups must declare prefix=None -- no single prefix "
                "identifies its files."
            )


def _assert_landing_modes_known() -> None:
    """Fail at import if a spec names a landing mode that does not exist.

    AT THE BOUNDARY, not in the consumer that dispatches on it. When this was
    written nothing read `landing` at all; four consumers do now -- `bronze_ingest`
    and `unzip_table` refuse anything that is not zips, `reclaim_landing` refuses
    the same way because a locally-landed table has no archive to recover from, and
    `extract_cnpj`'s landing resolver refuses anything that is not local -- so it is
    tempting to argue a bad value would fail loudly in one of them. It would not: a
    dispatch written as
    `if landing == LANDING_ZIPS: ... else: ...` swallows a typo into the `else`
    branch, and a table that should have been unzipped in the Volume gets treated
    as a tiny local lookup, silently. A value that is wrong is refused where it is
    DECLARED; leaning on a downstream consumer to notice is exactly the coupling
    this registry exists to remove.

    A plain ValueError rather than UnknownTable: nothing here is an unknown
    *table*, and UnknownTable's docstring describes an operator-supplied name at a
    job boundary, which a mode typo committed to source is not."""
    for spec in REGISTRY.values():
        if spec.landing not in LANDING_MODES:
            raise ValueError(
                f"{spec.name} names landing mode {spec.landing!r}, which is not one "
                f"of: {', '.join(sorted(LANDING_MODES))}"
            )


def _malformed_subdir_reason(subdir: str) -> str | None:
    """Why `subdir` is not a single directory name, or None if it is."""
    if any(sep in subdir for sep in _PATH_SEPARATORS):
        return "contains a path separator"
    if subdir.strip() in _RELATIVE_ALIASES:
        return "is blank or names an existing directory instead of a new one"
    return None


def _assert_subdirs_are_single_path_components() -> None:
    """Fail at import if a `subdir` is a PATH rather than a directory name.

    A DECISION, not a fallout: a landing subdir is ONE component by definition, so
    a `/` in it is not a value to be checked against the reserved list -- it is
    MALFORMED. The alternative was to normalise and check only the first component,
    i.e. to accept nesting as long as the root is unreserved. Rejected: that is the
    strictly stronger claim (`we support nested landing dirs`) and it would have to
    hold against every directory that ever appears under a table dir, forever.
    Refusing nesting outright is one rule that needs no such forecast.

    What it closes, both reached PAST the two checks that look total:
    `subdir="zips/estabelecimentos"` collides with no table (uniqueness passes) and
    does not equal `"zips"` (the reserved check passes), yet reads INSIDE the
    layout-owned zips dir. `subdir="lookups/x"` reads inside another table's source
    dir, where that table's stream discovers it RECURSIVELY -- the exact defect this
    branch exists to remove, re-entered through the guard built for it.

    `""` and `"."` are refused for a sharper reason: both resolve
    `landing_table(...)` to `cnpj/<month>` itself, which is the F1.4b blocker
    verbatim -- a stream on the month root, recursively discovering every table's
    files. `".."` escapes to `cnpj/`, which contains every MONTH.

    Both separators, not `os.sep`: this repo is developed on Windows and runs on
    Databricks Linux, and Windows `os.path` accepts `/` and `\\` alike -- so a
    backslash that looks inert where it is written is a real separator where it
    runs. Ordered BEFORE the reserved-name check, which is what makes that check's
    exact-string comparison total rather than partial."""
    for spec in REGISTRY.values():
        reason = _malformed_subdir_reason(spec.subdir)
        if reason is not None:
            raise ValueError(
                f"{spec.name} declares subdir {spec.subdir!r}, which {reason}. A landing "
                "subdir names ONE directory directly under cnpj/<month>; it is not a path. "
                "A nested value puts this table's stream INSIDE a directory that already "
                "belongs to something else -- the layout's own (zips/...) or another "
                "table's -- and cloudFiles walks a source dir RECURSIVELY, so the files "
                "would be ingested by both tables, or the whole month root by this one. "
                "Use a single directory name."
            )


def _assert_no_table_claims_a_reserved_subdir() -> None:
    """Fail at import if a spec's `subdir` names a directory the layout owns.

    Beside the other two and for the same reason: a value that is wrong is refused
    where it is DECLARED. This one has to be here and cannot be left to a consumer,
    because the consumer would not fail -- `bronze_stream` would load
    `cnpj/<month>/zips` perfectly happily and ingest tens of gigabytes of another
    table's files into this table's staging, which is a successful run.

    Not covered by `test_no_two_tables_share_a_landing_subdir`: that test compares
    tables against EACH OTHER, and a reserved name collides with no table at all.

    Compares EXACT strings, which is total only because
    `_assert_subdirs_are_single_path_components` runs first and has already refused
    everything that is not one directory name. Reorder them and this check goes
    partial again: `zips/estabelecimentos` would sail past it."""
    for spec in REGISTRY.values():
        if spec.subdir in RESERVED_SUBDIRS:
            raise ValueError(
                f"{spec.name} claims landing subdir {spec.subdir!r}, which is reserved by "
                f"the Volume layout ({', '.join(sorted(RESERVED_SUBDIRS))}). Its stream "
                "would read that directory, and cloudFiles walks a source dir RECURSIVELY "
                "-- so it would ingest every other table's files underneath it, the raw "
                "ZIPs and the multi-gigabyte .ESTABELE extracts included, without erroring. "
                "Give the table a landing subdir of its own."
            )


def _assert_no_two_tables_share_a_landing_subdir() -> None:
    """Fail at import if two specs land in the same directory.

    AT IMPORT and not only in a test, because the consumer does not fail: two tables
    on one subdir means each one's stream reads the other's files, and cloudFiles
    walks a source dir RECURSIVELY -- so both ingest both, and both runs report
    SUCCESS. This is the exact paste F1.4b makes (copy the estabelecimentos entry,
    rename everything but `subdir`), and it was verified by probe to import CLEAN
    before this guard existed, caught only by a CI test. A CI test protects a merge.
    It does not protect an ad-hoc run, or a branch whose tests have not been run,
    which is exactly how these jobs get launched while a phase is in flight.

    `subdir` is the field that survives a CAREFUL rename, which is what makes it the
    dangerous one: it is the only member of the copy-paste trio that does not contain
    the table's own bronze name, so a find/replace over `bronze_cnpj_*` fixes every
    other field and leaves this one pointing at the source table's landing dir.

    Not a duplicate of the two subdir checks above. That pair compares each subdir
    against the LAYOUT's own names and against the shape of a directory name, and
    neither can see two tables colliding with EACH OTHER -- `estabelecimentos` twice
    is unreserved, well-formed, and wrong. Three checks, three disjoint holes.

    `prefix` needs no twin of this: `_assert_prefixes_match_their_file_groups`
    already cross-checks it against FILE_GROUPS, which is strictly stronger than
    uniqueness -- it catches `Estabelecimento` (singular), which is unique and
    under-ingests silently."""
    seen: dict[str, str] = {}
    for spec in REGISTRY.values():
        if spec.subdir in seen:
            raise ValueError(
                f"{spec.name} and {seen[spec.subdir]} both claim landing subdir "
                f"{spec.subdir!r}. Each table's stream reads its own subdir and "
                "cloudFiles walks it RECURSIVELY, so two tables sharing one means "
                "each ingests the other's files and both runs report SUCCESS. Give "
                "each table a landing subdir of its own."
            )
        seen[spec.subdir] = spec.name


def _delta_name_collision(owner: str, name: str, other: tuple[str, str]) -> str:
    """The refusal text for two specs naming one Delta table, in the words they wrote.

    The comparison is casefolded, so the two strings this reports may not be EQUAL --
    and an operator handed only a normalised name would search source for a string
    nobody wrote. Both spellings, and the reason they are the same table, but ONLY
    when they differ: a plain duplicate annotated with a case explanation sends the
    operator looking for a difference that is not there, and the ordinary duplicate is
    the case that actually happens."""
    other_owner, other_name = other
    note = "" if name == other_name else (
        f", which {other_owner} spells {other_name!r} -- Unity Catalog and Spark "
        "identifiers are CASE-INSENSITIVE, so two spellings that differ only in case "
        "name ONE physical table"
    )
    return (
        f"{owner} and {other_owner} both declare Delta table {name!r}{note}. "
        "Two tables on one Delta name cross-contaminate: appends from both land in "
        "one table, `_batch_id` idempotence counts the other table's rows as this "
        "batch's, and a quarantine that doubles as someone's staging feeds DQ "
        "rejects to a promote. Give each table its own staging/bronze/quarantine "
        "triple."
    )


def _assert_no_two_tables_share_a_delta_name() -> None:
    """Fail at import if two specs name the same Delta table in any of the three
    roles that name one.

    ONE namespace across all three roles rather than three checks of one role each,
    because the defect is the same whichever role collides and the roles are NOT
    disjoint in principle: a table whose `quarantine` equals another's `staging`
    passes every per-role check, and routes DQ REJECTS into a table a promote reads
    as trusted input. `test_no_two_tables_share_a_staging_bronze_or_quarantine_name`
    has compared them cross-role since F1.4a for exactly that reason; this is that
    test's property moved to where the values are DECLARED, because the CI test does
    not protect an ad-hoc run.

    Two tables appending to one bronze table also breaks `_batch_id` idempotence,
    since `rows_of_batch` would count the other table's rows for the same batch. And
    a stale `quarantine` in particular is the documented F1.2/F1.3 incident: a
    hardcoded quarantine name "sent estab triagers to a table full of unrelated F1.2
    lookup rows".

    Verified by probe to be reachable: before this existed, a paste of the
    estabelecimentos entry with the staging/bronze/quarantine triple left unchanged
    imported CLEAN.

    The three fields are spelled out rather than looped over by name with `getattr`.
    `getattr(spec, role)` would be shorter and would type as `Any`, so renaming a
    field of `BronzeTable` would break this guard silently as far as any static check
    is concerned -- and a guard that reads its subject reflectively is the one place
    that cost is not worth paying. It also matches
    `test_no_two_tables_share_a_staging_bronze_or_quarantine_name`, which asserts this
    same property in the same shape.

    `table_key` is deliberately absent: it is a Volume path component, not a table.
    See `_assert_no_two_tables_share_a_checkpoint_namespace`, which also records why
    it and `subdir` compare case-SENSITIVELY where this one does not.

    CASEFOLDED: UC and Spark identifiers are case-insensitive, so
    `staging="BRONZE_CNPJ_ESTAB_STAGING"` IS estabelecimentos' staging table -- and
    under the byte comparison this used until F1.4b it imported CLEAN, with every
    consequence above intact. `_delta_name_collision` reports both spellings.

    A plain ValueError: nothing here is an unknown table."""
    seen: dict[str, tuple[str, str]] = {}
    for spec in REGISTRY.values():
        for role, name in (
            ("staging", spec.staging),
            ("bronze", spec.bronze),
            ("quarantine", spec.quarantine),
        ):
            owner = f"{spec.name}.{role}"
            key = name.casefold()
            if key in seen:
                raise ValueError(_delta_name_collision(owner, name, seen[key]))
            seen[key] = (owner, name)


def _assert_no_two_tables_share_a_checkpoint_namespace() -> None:
    """Fail at import if two specs claim the same `table_key`.

    SEPARATE from the Delta-name check above, and this is the one counter-intuitive
    thing in this file, so it is stated rather than left to be rediscovered:
    `table_key` names NO Delta table. It is the namespace
    `autoloader.checkpoint_location` and `schema_location` build
    `_checkpoints/<table_key>` and `_schemas/<table_key>` from, so it lives in a
    different namespace and is ALLOWED to spell itself like a Delta table --
    `lookup.table_key` and `lookup.bronze` are both `bronze_cnpj_lookup` today, and
    that is correct, not drift. The obvious implementation, one `seen` dict over all
    four roles in one pass, therefore refuses the LIVE registry at import and breaks
    every module that reads it. Pinned by
    `test_a_table_key_may_equal_its_own_tables_bronze_name`, because the failure is
    loud but its cause is not, and the natural fix on seeing it -- drop `table_key`
    from the check -- silently reopens the hole below.

    Which is why this is its own guard rather than a dropped field. The collision it
    catches is the QUIETEST of the four. An Auto Loader checkpoint is a record of
    which files have already been processed, so two tables sharing one means the
    second stream starts up believing the first's files are its own and already
    ingested: it writes nothing and reports SUCCESS. The shared `_schemas` entry
    compounds it by merging two unrelated inferred schemas into one.

    CASE-SENSITIVE, where the Delta-name guard casefolds, and the asymmetry is a
    decision rather than an oversight. `table_key` and `subdir` are components of
    Volume PATHS, and a Volume is backed by object storage, where `bronze_cnpj_estab`
    and `BRONZE_CNPJ_ESTAB` are two directories -- so two spellings that differ only
    in case are not one checkpoint, and there is no collision here to refuse. The
    Delta guard casefolds because UC and Spark resolve identifiers case-insensitively,
    which is a property of the thing NAMED; extending that to a path would be a claim
    about storage this project has not measured, and it would refuse a registry that
    is merely oddly capitalised. Stated because the opposite reading -- that the
    casefold was simply forgotten here -- is the obvious one.

    A plain ValueError: nothing here is an unknown table."""
    seen: dict[str, str] = {}
    for spec in REGISTRY.values():
        if spec.table_key in seen:
            raise ValueError(
                f"{spec.name} and {seen[spec.table_key]} both claim checkpoint "
                f"namespace {spec.table_key!r}. `table_key` is what autoloader "
                "builds _checkpoints/<table_key> and _schemas/<table_key> from, and "
                "a checkpoint records which files are already processed -- so two "
                "tables sharing one means the second stream treats the first's "
                "files as its own and already ingested, writes nothing, and reports "
                "SUCCESS. Give each table a table_key of its own."
            )
        seen[spec.table_key] = spec.name


_assert_contracts_exist()
# Contract identity before anything derived FROM a contract: both checks below
# resolve FILE_GROUPS entries by `spec.contract`, and neither is meaningful until
# that contract is known to exist and to name one table only.
_assert_no_two_tables_share_a_contract()
_assert_prefixes_match_their_file_groups()
# Also keyed on the contract, so it belongs in this group: `MASKED_COLUMNS` is
# keyed by contract, not by table name, and asking whether THIS table is masked is
# meaningless until its contract is known to exist and to name one table only.
_assert_no_masked_contract_declares_a_check_constraint()
_assert_landing_modes_known()
# Shape before content: the reserved-name check compares exact strings, so it is
# total only over values already known to be a single directory name.
_assert_subdirs_are_single_path_components()
_assert_no_table_claims_a_reserved_subdir()
# Individually-wrong before collectively-wrong, so the operator is never told the
# wrong fix. Two tables both declaring subdir="zips" -- or both "" -- are a duplicate
# AND two reserved/malformed values, and uniqueness would report it first as "give
# each table a subdir of its own", which is advice to rename one of them to something
# else reserved. Ordered last, the operator is told the real problem: neither value
# may be used at all.
_assert_no_two_tables_share_a_landing_subdir()
# No ordering between these two or against anything above: they read fields nothing
# else validates, in two namespaces that are independent of each other by design.
_assert_no_two_tables_share_a_delta_name()
_assert_no_two_tables_share_a_checkpoint_namespace()
