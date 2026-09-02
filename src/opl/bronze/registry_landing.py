# src/opl/bronze/registry_landing.py
"""How a bronze table's raw bytes reach the Volume, and where they land.

SPLIT OUT OF `registry.py` BY F1b TASK 3, which is a seam this group of modules has
needed: `registry.py` changes when a TABLE is added, `registry_collisions.py` when a
whole-set collision guard is added, and this file when the LANDING LAYOUT changes.
Adding a third landing mode plus its root, its resolver and its two guards would have
carried `registry.py` past this project's 800-line limit; the seam is the axis the
change moved along.

`registry*.py` IS NOT A STYLE CHOICE IN THE FILENAME. `tests/bronze/
test_registry_guard_wiring.py` globs exactly that pattern to learn which modules may
define a registry guard, and refuses a guard `registry.py` calls that no matching
module defines. A split named `landing.py` would contribute no definitions, land its
call in `called` anyway, and pass in silence.

--- THE FIVE MODES ----------------------------------------------------------------

`zips` and `local` describe two ways the SAME producer delivers: the RFB ships
monthly archives, and the extraction host either PUTs the archive for the cluster to
unzip (the multi-part giants) or unzips locally and PUTs the inner file (the six tiny
lookups). Both are FILE-FED: something downloads bytes that already existed.

`generated` is the third, and it is a different KIND of source rather than a third
delivery mechanism. Nothing downloads a generated table, because its bytes do not
exist until this lakehouse writes them: `opl.generator` derives the payment stream
from a seed and `opl.bronze.generated_landing` puts it in the Volume.

Each mode is CLASSIFIED as file-fed or not, in a declaration the import guards
(`_assert_every_landing_mode_is_classified`) refuse to leave half-written -- because the
two prefix guards below ask opposite questions and the classification is what decides
which one a table's declaration is held to.

`api` is the fourth (F-API Task 2), and it is a third kind again. Its bytes are
produced by somebody else -- the Banco Central's PTAX series -- but nothing
DOWNLOADS A FILE, because there is no file: a job task calls an HTTP endpoint, and a
record is built from the validated response and written into the Volume. So it is
neither file-fed nor generated, and `LANDING_GENERATED` was considered and REJECTED
for it: `GENERATED_RECORD_SOURCE` is `opl_payment_generator`, and stamping BCB's rows
with it would be a false claim in the one column that answers who produced a row.

THE ALTERNATIVE WAS TO HALF-GENERALISE `FILE_GROUPS` -- give payments an entry with
`parts: 1` and a prefix, so `_assert_prefixes_match_their_file_groups` stops
noticing. It was rejected in the plan and the reason is worth keeping beside the
code: `FILE_GROUPS` is what `cnpj_source.expected_files` builds a DOWNLOAD LIST from.
An entry there is a claim that a file with that prefix can be fetched from the RFB's
WebDAV share, and a fabricated one would make `extract_cnpj` go looking for
`Payments.zip` on a server that has never held it -- an under-ingest that reports
success, which is the exact class this registry exists to refuse.

`postgres` is the fifth (F-DB Task 4), and it is a fourth kind. Its bytes are produced by
an operational database THIS PROJECT RUNS, and -- uniquely so far -- nothing inside the
workspace produces or fetches them at all: the extractor runs on the EXTRACTION HOST and
PUTs a verified file through the Files API. Plan T1 is why, and the reason is a direction
rather than a firewall: the database is a container bound to `localhost:5433`, and egress
OUT of Databricks creates no route INTO a laptop behind a home NAT. `LANDING_API` was
considered and REJECTED for it -- that mode's own sentence is "somebody else's bytes,
fetched over HTTP, by a task inside the workspace", and all three clauses are false here;
`API_RECORD_SOURCE` is `bcb_olinda_ptax`, which would attribute this project's own database
to the Banco Central in the one column that answers who produced a row.

THE VISIBLE CONSEQUENCE IS IN THE JOB YAML, and it is what makes this a mode rather than a
detail. Every other ingestion job has a task that FILLS the landing directory --
`unzip_table`, `generate_payments`, `fetch_ptax` -- and this one has none, because that
step happens off Databricks before the job is launched.

--- THE FOUR ROOTS ----------------------------------------------------------------

A file-fed table lands under `cnpj/<month>/<subdir>`, a generated one under
`generated/<month>/<subdir>`, an api-fed one under `api/<month>/<subdir>` and a
database-fed one under `postgres/<month>/<subdir>`
(`opl.config`, which documents why each new root beat renaming an old one).
`landing_dir` below is the ONE place that mapping is made, so a consumer asks the
landing mode rather than knowing the layout -- which is what lets
`opl.bronze.autoloader`'s source-directory guard stay total across all four roots
without learning about any particular table.
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
# F-API Task 2: somebody else's bytes, and still no file to download. A job task calls
# an HTTP endpoint and writes a record built from the validated response. See the module
# docstring for why this is a fourth mode rather than a reuse of the third.
LANDING_API = "api"
# F-DB Task 4: bytes read out of an operational database by a HOST-SIDE extractor. See the
# module docstring for why this is a fifth mode rather than a reuse of the fourth.
LANDING_POSTGRES = "postgres"

LANDING_MODES = frozenset(
    {LANDING_ZIPS, LANDING_LOCAL, LANDING_GENERATED, LANDING_API, LANDING_POSTGRES}
)

# --- THE CLASSIFICATION, WHICH IS A DECLARATION AND IS GUARDED AS ONE ----------------
#
# The modes a DOWNLOADER feeds, and therefore the ones for which a `FILE_GROUPS` entry is
# REQUIRED; and the modes nothing downloads, for which one is FORBIDDEN. Two opposite
# questions, so which half a mode is in decides which question its tables are asked --
# and that is why both halves are declared positively instead of one being
# `LANDING_MODES - <the other>`.
#
# WHAT A SUBTRACTION WOULD COST, WHICH IS THE HOLE F-API'S FIX PASS CLOSED. Nothing used
# to guard this membership at all. A fifth mode that IS file-fed, added to `LANDING_MODES`
# and not to the set below, would be examined by the MIRROR -- whose empty-group +
# `None`-prefix path ACCEPTS -- so it would lose
# `_assert_prefixes_match_their_file_groups`' "no prefixes -> raise" branch, whose own
# message says the ingest "would report SUCCESS having read an empty source dir". That
# refusal has no complement anywhere, so it would be GONE rather than moved: a file-fed
# table with no producer, silently unchecked.
#
# SO THE PARTITION ITSELF IS ASSERTED AT IMPORT, by
# `_assert_every_landing_mode_is_classified` below. A mode in neither half, in both, or in
# a half without being a registered mode is refused where it is DECLARED -- which is the
# same argument `_assert_landing_modes_known` makes below about a mode's spelling, applied
# to its classification. Adding a fifth mode is therefore an edit HERE by construction,
# and that is the correction to three sentences this module used to carry: "the next mode
# needs no edit here at all" was true of a NON-file-fed mode and false of a file-fed one,
# and it was stated unconditionally.
#
# WHAT THAT GUARD STILL CANNOT SEE, STATED AT THE DECLARATION BECAUSE THIS IS WHERE THE
# MISTAKE IS MADE. It closes the OMISSION edit -- a mode classified nowhere -- and not the
# MISFILING one. A fifth mode that IS file-fed and is put in `NON_FILE_FED_LANDING_MODES`
# is in exactly one half, so the guard passes; a table on it whose contract has no
# `FILE_GROUPS` producer then reaches the cross-check, which SKIPS it, and the mirror,
# which ACCEPTS it (no group, no prefix is the mirror's pass) -- verbatim the hole the
# guard's own message describes, and no guard in this module refuses it. Nothing here can:
# the classification is the ONLY place this repository records whether a mode's bytes are
# downloaded, so no declaration exists for it to be checked against. A table whose contract
# DOES have a producer is still caught, by the mirror's first refusal.
#
# WHAT CATCHES IT NEXT, AND WHY THE SILENT VERSION NEEDS A THIRD EDIT. `_landing_and_tmp`
# below serves each mode from ONE declared root and refuses a mode it has no branch for, so
# a misfiled fifth mode has no landing dir at all: `landing_dir` raises before any Auto
# Loader is pointed anywhere, which is loud and is not the "SUCCESS over an empty source
# dir" the guard's message warns about. Reaching THAT needs the same author to also give
# the mode a root here. Measured both ways in
# `tests/bronze/test_registry_landing_guards.py` -- the hole is exercised rather than
# described, so closing it later fails a test that names this paragraph.
FILE_FED_LANDING_MODES = frozenset({LANDING_ZIPS, LANDING_LOCAL})
# The complement, DECLARED and not derived, for the reason above read the other way: this
# is the half where a `FILE_GROUPS` entry or a `prefix` is a false sentence rather than a
# requirement. `api` was added here by the fix pass; it was in neither half before, and
# the mirror caught it only because that guard's skip is a literal complement.
NON_FILE_FED_LANDING_MODES = frozenset({LANDING_GENERATED, LANDING_API, LANDING_POSTGRES})


# --- WHY THE `postgres` BRANCH BELOW RESOLVES A TMP DIR NOTHING EVER WRITES TO ---------
#
# Module level for the reason the two comment blocks further down are: this function is a
# dispatch and two refusals, and a fifth branch's argument put it past the project's
# 50-line limit.
#
# The tmp half is returned anyway rather than left to raise, because the value of ONE
# dispatch is that a landing dir and its staging twin can never come from two different
# roots -- and a mode that answered the first question and not the second would be exactly
# the drift this function exists to remove, re-entered as a hole.
#
# `opl.config.landing_postgres_tmp` carries why nothing stages there: every other mode's
# producer runs ON Databricks and stages inside the Volume so `os.replace` can make the
# file appear whole (one UC Volume is one FUSE mount, which is what makes that rename
# atomic). This source's producer runs on the extraction HOST -- it writes and verifies a
# LOCAL file and PUTs it through the Files API -- so there is no Volume-side staging step
# to point anywhere. It is on the standing unexercised list rather than absent.
def _landing_and_tmp(cfg: OplConfig, spec, month: str) -> tuple[str, str]:
    """`spec`'s landing directory for `month` AND its staging twin. THE one mapping.

    ONE DISPATCH FOR BOTH DIRECTORIES, which is the property rather than a saving of
    six lines. The staging dir is where a producer writes a file whole before
    `os.replace`-ing it into the landing dir, and `os.replace` works only within one
    Volume's FUSE mount -- so a landing dir resolved under one root with a tmp resolved
    under another is either an EXDEV failure or, worse, a successful move into a
    directory no stream reads. Two dispatches could drift into exactly that; this one
    cannot.

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
        return cfg.landing_table(spec.subdir, month), cfg.landing_tmp(spec.subdir, month)
    if spec.landing == LANDING_GENERATED:
        return (
            cfg.landing_generated_table(spec.subdir, month),
            cfg.landing_generated_tmp(spec.subdir, month),
        )
    if spec.landing == LANDING_API:
        return (
            cfg.landing_api_table(spec.subdir, month),
            cfg.landing_api_tmp(spec.subdir, month),
        )
    if spec.landing == LANDING_POSTGRES:
        return (
            cfg.landing_postgres_table(spec.subdir, month),
            cfg.landing_postgres_tmp(spec.subdir, month),
        )
    raise ValueError(
        f"{spec.name} names landing mode {spec.landing!r}, which no landing root "
        f"serves. Registered modes are: {', '.join(sorted(LANDING_MODES))}. This is "
        "refused rather than defaulted because either default is a directory holding "
        "another source's files, and cloudFiles walks a source dir RECURSIVELY."
    )


def landing_dir(cfg: OplConfig, spec, month: str) -> str:
    """The directory `spec`'s Auto Loader reads for `month`.

    IT HAD NO CALLER AT ALL BETWEEN F1b TASK 3 AND F-API TASK 2, and that is worth
    stating rather than leaving a reader to assume it was load-bearing: the two ingest
    entry points written in that window each built their own root's path directly.
    `databricks/src/bronze_ptax_ingest.py` is its first, and `fetch_ptax.py` is its
    second -- it built `landing_api_table` itself until this phase's fix pass, so one
    directory was resolved two ways inside one job, which is the drift this function
    exists to remove."""
    return _landing_and_tmp(cfg, spec, month)[0]


def landing_tmp_dir(cfg: OplConfig, spec, month: str) -> str:
    """Where a producer stages `spec`'s file before it is whole. OUTSIDE every source dir.

    Its own function rather than a second return value at the call site, so a caller
    that needs only the landing dir cannot bind the pair and hand the wrong half on --
    but the same dispatch, so the two halves are always of one root. cloudFiles walks a
    source dir RECURSIVELY and with no glob, so a partial file staged inside one is a
    file the ingest reads as if it were complete (`opl.bronze.unzip_volume`, F1.3)."""
    return _landing_and_tmp(cfg, spec, month)[1]


def _file_group_prefixes(contract: str) -> list[str]:
    """The distinct FILE_GROUPS prefixes whose zips feed `contract`, sorted."""
    return sorted({g["prefix"] for g in FILE_GROUPS.values() if g["table"] == contract})


def _assert_every_landing_mode_is_classified() -> None:
    """Fail at import if a landing mode is not in exactly one of the two halves.

    THE GUARD THE TWO SKIPS WERE STANDING IN FOR. It takes no registry: this is a claim
    about the DECLARATION, not about today's tables, and it must hold before any table
    declares the mode -- otherwise the first table to use a misfiled mode is the one that
    discovers it, by not being checked.

    Three refusals, and the middle one is the one that costs something. A mode in NEITHER
    half is a mode whose tables the file-fed cross-check skips and the mirror accepts (no
    file group, no prefix -> pass), so a file-fed table with no producer at all imports
    clean; see the comment block above `FILE_FED_LANDING_MODES`. A mode in BOTH would be
    asked two contradictory questions and refused whatever it declared. And a half naming a
    mode `LANDING_MODES` does not carry is a classification of something no spec can name,
    which is how the two sets stop describing the same universe.

    IT CLOSES THE OMISSION AND NOT THE MISFILING -- a file-fed mode put in the NON-file-fed
    half is in exactly one half and passes here. See the comment block above
    `FILE_FED_LANDING_MODES` for why no guard in this module can refuse that, and for what
    does catch it next.

    A plain ValueError, matching every other guard in this module: nothing here is an
    unknown table, and no operator supplied it -- this is a source edit that half-added a
    landing mode."""
    both = sorted(FILE_FED_LANDING_MODES & NON_FILE_FED_LANDING_MODES)
    unclassified = sorted(
        LANDING_MODES ^ (FILE_FED_LANDING_MODES | NON_FILE_FED_LANDING_MODES)
    )
    if both:
        raise ValueError(
            f"{both} are declared BOTH file-fed and not. The two halves ask opposite "
            "questions -- a file-fed table MUST have a cnpj_schemas.FILE_GROUPS producer, "
            "one nothing downloads must have none and no prefix -- so a mode in both is "
            "refused whatever its tables declare."
        )
    if unclassified:
        raise ValueError(
            f"{unclassified} are landing modes with no classification, or classified "
            f"without being modes. LANDING_MODES is {sorted(LANDING_MODES)}, "
            f"FILE_FED_LANDING_MODES is {sorted(FILE_FED_LANDING_MODES)} and "
            f"NON_FILE_FED_LANDING_MODES is {sorted(NON_FILE_FED_LANDING_MODES)}. An "
            "unclassified mode is not merely undocumented: the file-fed cross-check skips "
            "it and the mirror ACCEPTS it (no file group and no prefix is the mirror's "
            "pass), so a file-fed table with no producer would import clean and its "
            "ingest would report SUCCESS having read an empty source dir. Put the mode in "
            "the half that says which of the two questions its tables must answer."
        )


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
# invariant that blocked registering payments at all. A table NOTHING DOWNLOADS will
# never have a `FILE_GROUPS` entry -- so the "no group" branch, correct for every
# file-fed table, is wrong for every other kind of source. Skipped there and REFUSED IN
# THE OTHER DIRECTION by `_assert_no_table_nothing_downloads_claims_a_downloader`, so
# such a table is not merely unchecked: it must have no group AND no prefix, which is a
# stronger statement than the one it is excused from.
#
# AND THE PAIR IS NOW COMPLEMENTARY RATHER THAN TWO POSITIVE SCOPES, which is F-API
# Task 2's change and the reason the mirror guard was renamed. Both guards used to be
# scoped POSITIVELY -- this one to `FILE_FED_LANDING_MODES`, its mirror to
# `LANDING_GENERATED` -- so the fourth mode fell into NEITHER and was unguarded in both
# directions: `api` needed no file group (correct) and could have declared a prefix
# (a false sentence nothing would have refused). Two positive scopes leave a FIFTH mode
# uncovered again, one mode later. The mirror's skip is now the exact complement of this
# one's, so every registered table is checked by exactly one of the pair whatever modes
# exist later.
#
# WHAT THE COMPLEMENT DOES NOT BUY, corrected here because this comment claimed it and
# two other places claimed it too: "adding a mode requires no edit to either" is TRUE OF A
# NON-FILE-FED MODE ONLY. The complement makes EXAMINATION total -- every table is looked
# at by one of the pair -- and it does not make the VERDICT right. A fifth mode that is
# file-fed and is not added to `FILE_FED_LANDING_MODES` is examined by the mirror, which
# ACCEPTS it (no file group, no prefix is the mirror's pass), and the "no prefixes ->
# raise" branch below is lost rather than moved: that refusal has no complement anywhere.
# `_assert_every_landing_mode_is_classified` is what makes the membership itself a
# declaration, so a fifth mode is an edit to the classification by construction, and only
# the two guard bodies are left needing none.
#
# --- WHY THE MIRROR'S OWN PROSE IS UP HERE ------------------------------------------
#
# Its docstring grew to 52 lines when F-API Task 2 rewrote it, in a module whose FIRST
# comment block exists because a guard's prose outgrew the project's 50-line function
# limit. So the same remedy: the reasoning lives here and the function stays a loop and
# two refusals. What was there and is now here --
#
# ITS SKIP IS THE EXACT COMPLEMENT OF THE FILE-FED ONE'S, and that is the property the
# rename buys and the only thing it buys (see the paragraph above for what it does not).
# `_assert_prefixes_match_their_file_groups` continues on anything NOT in
# `FILE_FED_LANDING_MODES`; the mirror continues on anything IN it.
#
# IT WAS SCOPED TO `LANDING_GENERATED` UNTIL F-API TASK 2, and that was the defect. Two
# POSITIVELY-scoped guards do not cover the modes nobody has invented yet: `api` was not
# file-fed and was not generated, so it fell into neither and was free to declare a prefix
# that no cross-check would have compared against anything. Pasting the function with one
# constant changed would have closed that instance and left a FIFTH mode in the identical
# hole.
#
# THE MESSAGES NAME THE MODE THEY ACTUALLY FOUND rather than a constant, which matters now
# that more than one mode reaches that loop: a PTAX operator reading a refusal that said
# `landing='generated'` would go looking at the payment generator.


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


def _assert_no_table_nothing_downloads_claims_a_downloader(registry) -> None:
    """Fail at import if a table NO DOWNLOADER FEEDS has a file group or a file prefix.

    THE MIRROR OF THE GUARD ABOVE, and the half that keeps its exemption from being a
    hole: "these tables are unchecked" is how a landing mode becomes a way to opt out of
    the registry. Two distinct failures, refused separately because they say different
    things -- a file group means two producers writing one landing dir that one Auto
    Loader reads with no glob, and a prefix is a false sentence in the file this
    repository treats as the answer to "what is table X?". A plain ValueError: nothing
    here is an unknown table, and no operator supplied it.

    See the comment block above for what its complementary skip buys, what it does NOT
    buy, and why that prose is not in here."""
    for spec in registry.values():
        if spec.landing in FILE_FED_LANDING_MODES:
            continue
        groups = sorted(g for g, entry in FILE_GROUPS.items() if entry["table"] == spec.contract)
        if groups:
            raise ValueError(
                f"{spec.name} declares landing={spec.landing!r}, which no downloader "
                f"feeds, and cnpj_schemas.FILE_GROUPS entry/entries {groups} feed its "
                f"contract {spec.contract!r}. Two producers would write one landing "
                "directory -- a downloader and this lakehouse's own writer -- and the "
                "stream reading it has no glob to tell them apart. Pick one: either the "
                f"table is downloaded (declare one of {sorted(FILE_FED_LANDING_MODES)}) "
                "or it is not (remove the file group)."
            )
        if spec.prefix is not None:
            raise ValueError(
                f"{spec.name} declares landing={spec.landing!r} and prefix="
                f"{spec.prefix!r}. A prefix is the string a DOWNLOADER builds its "
                "file list from, so declaring one for a table nothing downloads is a "
                "false statement in the file this repository treats as the answer to "
                "'what is table X?' -- and it puts that table into "
                "`test_no_two_tables_share_a_file_prefix`, competing for a real "
                "producer's prefix. A table nothing downloads declares prefix=None."
            )
