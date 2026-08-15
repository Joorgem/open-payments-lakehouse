# src/opl/bronze/registry_subdirs.py
"""Which DIRECTORY a bronze table lands in, and every name it may not be.

SPLIT OUT OF `registry.py` BY F-API'S FIX PASS, when the sixth table's constraint prose
and the landing-classification guard's call carried that file to 802 of this project's
800-line limit. §6 of the F-API plan predicted the crossing and did not assign the split:
"`src/opl/bronze/registry.py` at 712 lines is not on the list above and belongs on it. The
payments `BronzeTable` plus its comment block is ~66 lines; a comparably documented sixth
entry lands the file at ~775-785." It landed at 802.

THE SEAM IS THE ONE THIS GROUP OF MODULES HAS USED FOUR TIMES ALREADY: `registry.py`
changes when a TABLE is added, `registry_collisions.py` when a whole-set NAME collision
guard is added, `registry_landing.py` when the landing LAYOUT changes -- and this file when
the VOLUME RESERVES A DIRECTORY, or when what counts as a directory NAME changes. That is a
fifth reason to edit and therefore a fifth file.

WHY NOT INTO `registry_landing.py`, which is the nearest neighbour and where a reader would
look first. Two reasons, and the second is the deciding one. Its subject is which ROOT a
table's bytes land under, keyed on the landing MODE -- `subdir` is the component below that
root and is the same string whatever the mode is, so the two questions have no shared
input. And that module is imported by `registry.py` for the mode constants, while
`_reserved_subdirs` here derives its answers from `OplConfig`'s Volume layout: folding them
together would put the layout derivation behind the mode constants every consumer imports,
for a file that would then be 520 lines with two subjects instead of two files with one
each.

`registry*.py` IS NOT A STYLE CHOICE IN THE FILENAME. `tests/bronze/
test_registry_guard_wiring.py` globs exactly that pattern to learn which modules may define
a registry guard, and refuses a guard `registry.py` calls that no matching module defines. A
split named `subdirs.py` would contribute no definitions, land its calls in `called` anyway,
and pass in silence.

THE THREE GUARDS TAKE THE REGISTRY AS AN ARGUMENT, which is a signature change from the
no-argument versions that lived in `registry.py`, and it is the same change every other
extracted guard made for the same reason: `registry.py` calls them at its own import, so
importing `REGISTRY` back from here would be a cycle. `registry_collisions` and
`registry_landing` both state it; this is the third instance of one rule.
"""
from __future__ import annotations

from opl.config import OplConfig

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


def _malformed_subdir_reason(subdir: str) -> str | None:
    """Why `subdir` is not a single directory name, or None if it is."""
    if any(sep in subdir for sep in _PATH_SEPARATORS):
        return "contains a path separator"
    if subdir.strip() in _RELATIVE_ALIASES:
        return "is blank or names an existing directory instead of a new one"
    return None


def _assert_subdirs_are_single_path_components(registry) -> None:
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
    for spec in registry.values():
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


def _assert_no_table_claims_a_reserved_subdir(registry) -> None:
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
    for spec in registry.values():
        if spec.subdir in RESERVED_SUBDIRS:
            raise ValueError(
                f"{spec.name} claims landing subdir {spec.subdir!r}, which is reserved by "
                f"the Volume layout ({', '.join(sorted(RESERVED_SUBDIRS))}). Its stream "
                "would read that directory, and cloudFiles walks a source dir RECURSIVELY "
                "-- so it would ingest every other table's files underneath it, the raw "
                "ZIPs and the multi-gigabyte .ESTABELE extracts included, without erroring. "
                "Give the table a landing subdir of its own."
            )


def _assert_no_two_tables_share_a_landing_subdir(registry) -> None:
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
    for spec in registry.values():
        if spec.subdir in seen:
            raise ValueError(
                f"{spec.name} and {seen[spec.subdir]} both claim landing subdir "
                f"{spec.subdir!r}. Each table's stream reads its own subdir and "
                "cloudFiles walks it RECURSIVELY, so two tables sharing one means "
                "each ingests the other's files and both runs report SUCCESS. Give "
                "each table a landing subdir of its own."
            )
        seen[spec.subdir] = spec.name
