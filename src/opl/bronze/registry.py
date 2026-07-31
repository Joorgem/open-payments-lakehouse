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

from dataclasses import dataclass

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


_assert_contracts_exist()
# Contract identity before anything derived FROM a contract: both checks below
# resolve FILE_GROUPS entries by `spec.contract`, and neither is meaningful until
# that contract is known to exist and to name one table only.
_assert_no_two_tables_share_a_contract()
_assert_prefixes_match_their_file_groups()
_assert_landing_modes_known()
# Shape before content: the reserved-name check compares exact strings, so it is
# total only over values already known to be a single directory name.
_assert_subdirs_are_single_path_components()
_assert_no_table_claims_a_reserved_subdir()
