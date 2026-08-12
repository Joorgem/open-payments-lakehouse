# src/opl/bronze/registry_landing.py
"""How a bronze table's raw bytes reach the Volume, and where they land.

SPLIT OUT OF `registry.py` BY F1b TASK 3, which is the fourth seam this group of
modules has needed and follows the rule the first three did: `registry.py` changes
when a TABLE is added, `registry_collisions.py` when a whole-set collision guard is
added, and this file when the LANDING LAYOUT changes. Adding a third landing mode
plus its root, its resolver and its two guards would have carried `registry.py` past
this project's 800-line limit; the seam is the axis the change moved along.

`registry*.py` IS NOT A STYLE CHOICE IN THE FILENAME. `tests/bronze/
test_registry_guard_wiring.py` globs exactly that pattern to learn which modules may
define a registry guard, and refuses a guard `registry.py` calls that no matching
module defines. A split named `landing.py` would contribute no definitions, land its
call in `called` anyway, and pass in silence.

--- THE THREE MODES ---------------------------------------------------------------

`zips` and `local` describe two ways the SAME producer delivers: the RFB ships
monthly archives, and the extraction host either PUTs the archive for the cluster to
unzip (the multi-part giants) or unzips locally and PUTs the inner file (the six tiny
lookups). Both are FILE-FED: something downloads bytes that already existed.

`generated` is the third, and it is a different KIND of source rather than a third
delivery mechanism. Nothing downloads a generated table, because its bytes do not
exist until this lakehouse writes them: `opl.generator` derives the payment stream
from a seed and `opl.bronze.generated_landing` puts it in the Volume. That distinction
is what the two guards at the foot of this module enforce in both directions.

THE ALTERNATIVE WAS TO HALF-GENERALISE `FILE_GROUPS` -- give payments an entry with
`parts: 1` and a prefix, so `_assert_prefixes_match_their_file_groups` stops
noticing. It was rejected in the plan and the reason is worth keeping beside the
code: `FILE_GROUPS` is what `cnpj_source.expected_files` builds a DOWNLOAD LIST from.
An entry there is a claim that a file with that prefix can be fetched from the RFB's
WebDAV share, and a fabricated one would make `extract_cnpj` go looking for
`Payments.zip` on a server that has never held it -- an under-ingest that reports
success, which is the exact class this registry exists to refuse.

--- THE TWO ROOTS -----------------------------------------------------------------

A file-fed table lands under `cnpj/<month>/<subdir>`; a generated one lands under
`generated/<month>/<subdir>` (`opl.config`, which documents why a second root beat
renaming the first). `landing_dir` below is the ONE place that mapping is made, so a
consumer asks the landing mode rather than knowing the layout -- which is what lets
`opl.bronze.autoloader`'s source-directory guard stay total across both roots without
learning about payments.
"""
from __future__ import annotations

from opl.config import OplConfig
from opl.contracts.cnpj_schemas import FILE_GROUPS

# How a table's raw files reach the Volume.
LANDING_ZIPS = "zips"    # PUT the zip, unzip in the Volume (multi-part groups)
LANDING_LOCAL = "local"  # unzip locally, PUT the inner file (the tiny lookups)
# F1b Task 3: nothing downloads it -- this lakehouse writes it. See the module
# docstring for why this is a mode rather than a `FILE_GROUPS` entry with a prefix.
LANDING_GENERATED = "generated"

LANDING_MODES = frozenset({LANDING_ZIPS, LANDING_LOCAL, LANDING_GENERATED})

# The modes a DOWNLOADER feeds, and therefore the ones for which a `FILE_GROUPS`
# entry is required rather than forbidden. Declared as its own set instead of
# `LANDING_MODES - {LANDING_GENERATED}`, because the two guards below read it in
# OPPOSITE directions and a subtraction would make a fourth mode default into the
# file-fed half silently -- which is the half that requires a producer to exist.
FILE_FED_LANDING_MODES = frozenset({LANDING_ZIPS, LANDING_LOCAL})


def landing_dir(cfg: OplConfig, spec, month: str) -> str:
    """The directory `spec`'s Auto Loader reads for `month`. THE one mapping.

    Takes the spec rather than `(subdir, landing)` for the reason every task test in
    this repository asserts: a coordinate must come from the ONE spec the caller
    resolved, or a table's landing dir can drift from its landing mode. Untyped
    because `BronzeTable` lives in `registry.py`, which imports THIS module -- an
    annotation would be a cycle, and the alternative (moving the dataclass here) puts
    the thing every consumer imports behind the thing that describes one of its
    fields.

    Refuses an unknown mode rather than falling through to a default. A dispatch
    written as `if landing == LANDING_ZIPS: ... else: ...` is exactly how a typo
    silently sends a generated table at the CNPJ root -- where cloudFiles would walk
    a month directory holding every other table's files, recursively."""
    if spec.landing in FILE_FED_LANDING_MODES:
        return cfg.landing_table(spec.subdir, month)
    if spec.landing == LANDING_GENERATED:
        return cfg.landing_generated_table(spec.subdir, month)
    raise ValueError(
        f"{spec.name} names landing mode {spec.landing!r}, which no landing root "
        f"serves. Registered modes are: {', '.join(sorted(LANDING_MODES))}. This is "
        "refused rather than defaulted because either default is a directory holding "
        "another source's files, and cloudFiles walks a source dir RECURSIVELY."
    )


def _file_group_prefixes(contract: str) -> list[str]:
    """The distinct FILE_GROUPS prefixes whose zips feed `contract`, sorted."""
    return sorted({g["prefix"] for g in FILE_GROUPS.values() if g["table"] == contract})


def _assert_landing_modes_known(registry) -> None:
    """Fail at import if a spec names a landing mode that does not exist.

    AT THE BOUNDARY, not in the consumer that dispatches on it. Five consumers read
    `landing` now -- `bronze_ingest` and `unzip_table` refuse anything that is not
    zips, `reclaim_landing` refuses the same way because a locally-landed table has
    no archive to recover from, `extract_cnpj`'s landing resolver refuses anything
    that is not local, and `bronze_payments_ingest` refuses anything that is not
    generated -- so it is tempting to argue a bad value would fail loudly in one of
    them. It would not: a dispatch written as
    `if landing == LANDING_ZIPS: ... else: ...` swallows a typo into the `else`
    branch, and a table that should have been unzipped in the Volume gets treated as
    a tiny local lookup, silently. A value that is wrong is refused where it is
    DECLARED; leaning on a downstream consumer to notice is exactly the coupling this
    registry exists to remove.

    A plain ValueError rather than UnknownTable: nothing here is an unknown *table*,
    and UnknownTable's docstring describes an operator-supplied name at a job
    boundary, which a mode typo committed to source is not."""
    for spec in registry.values():
        if spec.landing not in LANDING_MODES:
            raise ValueError(
                f"{spec.name} names landing mode {spec.landing!r}, which is not one "
                f"of: {', '.join(sorted(LANDING_MODES))}"
            )


# --- WHY `prefix` IS CROSS-CHECKED RATHER THAN DELETED, AND WHY THE CHECK IS NOW
# --- SCOPED TO FILE-FED TABLES ------------------------------------------------------
#
# Module level rather than inside the guard's docstring, for the reason
# `opl.bronze.rules` states above `rules_for`: this prose grew past the point where
# the function carrying it stayed under the project's 50-line limit. It is the
# reasoning, not the guard.
#
# NO PRODUCTION CODE READS THE FIELD. `cnpj_source.expected_files` builds the download
# list from `FILE_GROUPS[g]["prefix"]` and has to, because the lookup's six
# differently-named files have no single prefix to build from. So `prefix` is a SECOND
# SPELLING of a live value -- the exact shape of the drift this registry exists to
# remove -- and the F1.4a review offered two ways out: delete it, or tie it down.
#
# TIED DOWN, because the field is not dead weight. Carry-forward #10 asked for the
# prefix to be DECLARED rather than implied by a dict key;
# `test_no_two_tables_share_a_file_prefix` is one of the three copy-paste locks F1.4b
# was tested by; and `prefix="Estabelecimento"` (singular) is unique, passes every
# other check, and under-ingests SILENTLY. What the assertion does is convert the
# duplicate into a CROSS-CHECK: the registry's spelling must agree with the one the
# downloader uses, so the two can no longer drift -- they can only fail this import.
#
# THREE CASES AMONG FILE-FED TABLES, all handled so none is silent. Exactly one group
# feeding a contract means the prefix is that group's. More than one means no single
# prefix identifies the files (the lookup: six groups, routed into one table by
# filename suffix), so `None` is the only correct value -- a real property, not a gap
# to be filled in. NO group feeding a file-fed contract is refused: that table has no
# producer at all, so its landing dir would never receive a file and the job built on
# it would report SUCCESS having ingested zero rows.
#
# THE FOURTH CASE IS WHY THE FUNCTION GAINED A FILTER IN F1b TASK 3, and it is the
# invariant that blocked registering payments at all. A GENERATED table will never
# have a `FILE_GROUPS` entry, because nothing downloads it -- so the "no group" branch,
# correct for every file-fed table, is wrong for exactly one kind of source. Skipped
# there and REFUSED IN THE OTHER DIRECTION by
# `_assert_no_generated_table_claims_a_downloader`, so a generated table is not merely
# unchecked: it must have no group AND no prefix, which is a stronger statement than
# the one it is excused from.


def _assert_prefixes_match_their_file_groups(registry) -> None:
    """Fail at import if a FILE-FED spec's `prefix` disagrees with FILE_GROUPS.

    See the comment block above for what the field is, why it survives, and why
    generated tables are skipped here and checked by the guard below instead."""
    for spec in registry.values():
        if spec.landing not in FILE_FED_LANDING_MODES:
            continue
        prefixes = _file_group_prefixes(spec.contract)
        if not prefixes:
            raise ValueError(
                f"{spec.name} lands as {spec.landing!r} and names contract "
                f"{spec.contract!r}, which no cnpj_schemas.FILE_GROUPS entry feeds. "
                "Nothing would ever be downloaded or landed for it, and its ingest "
                "would report SUCCESS having read an empty source dir -- add the RFB "
                f"file group, drop the registry entry, or (if this lakehouse WRITES "
                f"the table rather than fetching it) declare landing="
                f"{LANDING_GENERATED!r}."
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


def _assert_no_generated_table_claims_a_downloader(registry) -> None:
    """Fail at import if a GENERATED table has a file group or a file prefix.

    THE MIRROR OF THE GUARD ABOVE, and the half that keeps its exemption from being a
    hole: "generated tables are unchecked" is how a landing mode becomes a way to opt
    out of the registry. Two distinct failures, refused separately because they say
    different things -- a file group means two producers writing one landing dir that
    one Auto Loader reads with no glob, and a prefix is a false sentence in the file
    this repository treats as the answer to "what is table X?". A plain ValueError:
    nothing here is an unknown table, and no operator supplied it."""
    for spec in registry.values():
        if spec.landing != LANDING_GENERATED:
            continue
        groups = sorted(g for g, entry in FILE_GROUPS.items() if entry["table"] == spec.contract)
        if groups:
            raise ValueError(
                f"{spec.name} declares landing={LANDING_GENERATED!r} and "
                f"cnpj_schemas.FILE_GROUPS entry/entries {groups} feed its contract "
                f"{spec.contract!r}. Two producers would write one landing directory "
                "-- a downloader and this lakehouse's own writer -- and the stream "
                "reading it has no glob to tell them apart. Pick one: either the "
                "table is downloaded (declare a file-fed landing mode) or it is "
                "generated (remove the file group)."
            )
        if spec.prefix is not None:
            raise ValueError(
                f"{spec.name} declares landing={LANDING_GENERATED!r} and prefix="
                f"{spec.prefix!r}. A prefix is the string a DOWNLOADER builds its "
                "file list from, so declaring one for a table nothing downloads is a "
                "false statement in the file this repository treats as the answer to "
                "'what is table X?' -- and it puts the generated table into "
                "`test_no_two_tables_share_a_file_prefix`, competing for a real "
                "producer's prefix. Generated tables declare prefix=None."
            )
