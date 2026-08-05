"""The registry's guards over names that must not COLLIDE, and the shape one may not
take. Extracted from `opl.bronze.registry`, and called from it at import.

WHY A SECOND MODULE AT ALL. `registry.py` reached 798 lines of this project's 800-line
cap, and the cap had already declined a correctness improvement: the month-shaped
`table_key` guard below was written as a test-level LOCK in
`tests/bronze/test_autoloader_helpers.py` instead, because there was no room for it and
the reasoning this repo requires beside a guard. F2 adds tables and touches this area,
so the debt is paid before anything is built on it rather than discovered mid-task.

THE SEAM IS NOT THE LINE COUNT, which would put anything here. What lives in this module
are the guards over the two NAMESPACES a spec claims names in that have nothing to do
with its contract: the three Delta identifiers (staging/bronze/quarantine) and the
Volume path component `table_key`. What stays in `registry.py` is everything derived
from a spec's CONTRACT -- that it exists, that one table claims it, that a FILE_GROUPS
entry feeds it, whether it is masked -- together with the landing-subdir trio, which is
checked against `RESERVED_SUBDIRS`, a value `registry.py` derives from `OplConfig`.

WHAT `table_key` IS, stated once here because two of the three guards below rest on it
and it is the one counter-intuitive thing in this module. It names NO Delta table. It is
the last component `autoloader.checkpoint_location` and `schema_location` build
`_checkpoints/<month>/<table_key>` and `_schemas/<month>/<table_key>` from, so it lives
in a namespace of its own and is ALLOWED to spell itself like a Delta table --
`lookup.table_key` and `lookup.bronze` are both `bronze_cnpj_lookup` today, and that is
correct, not drift. The obvious implementation, one `seen` dict over all four roles in
one pass, therefore refuses the LIVE registry at import and breaks every module that
reads it. Pinned by `test_a_table_key_may_equal_its_own_tables_bronze_name`, because the
failure is loud but its cause is not, and the natural fix on seeing it -- drop
`table_key` from the check -- silently reopens the quietest hole of the four.

THE REGISTRY ARRIVES AS AN ARGUMENT, and that is the only reason these functions take
one. They read `REGISTRY`, which lives in `registry.py`, and `registry.py` calls them at
its own import -- so importing it back from here is a cycle, and the two ways out are a
deferred import inside each function or a parameter. The parameter, because it leaves
the dependency edge in ONE direction and one place (`registry.py` imports this module;
this module imports nothing of it at run time), and because a guard that takes the
mapping it validates says in its signature what it reads. A deferred import would hide
the cycle rather than remove it, and would make the import order of two modules
load-bearing for a refusal that has to happen before anything else does.

The refusal TEXT is unchanged from what these guards raised in `registry.py`, verbatim,
and is asserted verbatim by the tests: an operator reading a Databricks run log is the
audience, and moving a function between files is not a reason to reword the one thing
they see.

NO PYSPARK, DIRECTLY OR TRANSITIVELY. This is inherited, not re-decided: the extraction
scripts -- `extract_cnpj`, which downloads ~15 GB from the RFB -- import the registry
through `spec_for_contract` and run where pyspark is not installed at all (it is the
`spark` extra, deliberately not a core dependency, because installing it into serverless
breaks the wheel install). `opl.config`, imported below for `is_month`, imports `re` and
`dataclasses` and nothing else.
`test_the_registry_still_imports_where_pyspark_is_not_installed` holds that down as a
PROPERTY, through a subprocess with a meta-path blocker, rather than as one spelling of
its cause -- so it covers this module arriving in the chain too."""
from __future__ import annotations

from typing import TYPE_CHECKING

from opl.config import is_month

if TYPE_CHECKING:
    # The reverse edge, for type checkers only -- they do not execute it, so it is not
    # the run-time cycle the module docstring explains this module's signatures around.
    from collections.abc import Mapping

    from opl.bronze.registry import BronzeTable

    # A Mapping, not a dict: these guards only ever iterate `.values()`, and the
    # narrower type says so -- nothing here may reorder or repair the registry it is
    # refusing. Aliased because the guard names are long enough that the spelled-out
    # annotation pushes their signatures onto three lines, and a signature split over
    # three lines costs two of the fifty this project gives a function.
    Registry = Mapping[str, BronzeTable]


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


def _assert_no_two_tables_share_a_delta_name(registry: Registry) -> None:
    """Fail at import if two specs name one Delta table, in any of the three roles.

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

    Verified by probe: before this guard existed, a paste of the estabelecimentos
    entry with the staging/bronze/quarantine triple left unchanged imported CLEAN.

    The three fields are spelled out rather than looped over by name with `getattr`.
    `getattr(spec, role)` would be shorter and would type as `Any`, so renaming a
    field of `BronzeTable` would break this guard silently as far as any static check
    is concerned -- and a guard that reads its subject reflectively is the one place
    that cost is not worth paying. It is also the shape the CI test named above
    asserts the property in.

    `table_key` is deliberately absent -- see the module docstring, and the checkpoint
    guard below for why it and `subdir` compare case-SENSITIVELY where this one does not.

    CASEFOLDED: UC and Spark identifiers are case-insensitive, so
    `staging="BRONZE_CNPJ_ESTAB_STAGING"` IS estabelecimentos' staging table -- and
    under the byte comparison this used until F1.4b it imported CLEAN, with every
    consequence above intact. `_delta_name_collision` reports both spellings.

    A plain ValueError: nothing here is an unknown table."""
    seen: dict[str, tuple[str, str]] = {}
    for spec in registry.values():
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


def _month_shaped_table_key(spec_name: str, table_key: str) -> str:
    """The refusal text for a `table_key` that could be read as a month.

    Extracted like `_delta_name_collision` and `_masked_check_collision`, and measured
    rather than assumed: inlined, the guard is 49 lines of a 50-line cap that counts
    comments -- one amendment from the ceiling this module exists because of. A refusal
    text is also its own thing to grep for, separate from the reasoning about when to
    raise it."""
    return (
        f"{spec_name} claims checkpoint namespace {table_key!r}, which is a MONTH. "
        "`autoloader` builds _checkpoints/<month>/<table_key> and "
        "_schemas/<month>/<table_key>, and the pre-month-scoping _checkpoints/"
        "<table_key> directories still exist in the Volume holding 2026-06 state, "
        "deliberately unmigrated -- so a month-shaped table_key makes this table's "
        "orphaned state directory BE a month directory, and every table's state for "
        "that month is then written inside a checkpoint another query owns, its "
        "RocksDB store and offset log included. It also makes a name under "
        "_checkpoints/ unreadable: an operator listing it can no longer tell a month "
        "from a table. Give the table a table_key that is not YYYY-MM -- every "
        "registered one is its own bronze name or an abbreviation of it."
    )


def _assert_no_table_key_is_month_shaped(registry: Registry) -> None:
    """Fail at import if a `table_key` is `YYYY-MM`, and so could name a month dir.

    THE GUARD THE 800-LINE CAP DECLINED, and the reason this module exists. It shipped
    as a LOCK inside `tests/bronze/test_autoloader_helpers.py::test_no_table_shares_or_
    nests_state_across_months_or_with_the_orphans`, whose docstring said the structural
    version belonged in the registry and did not fit. The lock stays -- it is the
    premise the sampled comparison beneath it rests on, and a test that names its own
    premise is legible -- but it is no longer the only thing enforcing this.

    ITS OWN GUARD RATHER THAN A CLAUSE INSIDE THE UNIQUENESS CHECK BELOW, following the
    subdir trio in `registry.py`: shape, reservation and uniqueness are three checks
    over one field because they are three disjoint holes, and folding them together
    buys nothing but a longer function and a vaguer message. `table_key` gets two of
    those three, and they are equally disjoint -- a month-shaped key collides with no
    other table, and two colliding keys need not be month-shaped.

    WHY THE SHAPE IS THE ONLY SIDE WORTH GUARDING. The confusion needs a `<month>`
    component that equals a `<table_key>` component, and the month side is already
    total: `opl.config._MONTH` admits only `YYYY-MM` naming a real month, and every
    entry point routes through `require_month`. So the property rests entirely on no
    `table_key` being month-shaped, which is what this refuses.

    ASKS `is_month` RATHER THAN CARRYING A SECOND REGEX, and that is deliberate to the
    point of being the reason `is_month` is public. Two spellings of the month rule is
    how `2026-13` came to be refused at two of four entry points; a private pattern
    here would be a third. It also fixes the boundary correctly rather than by
    accident: `2026-13` is NOT refused as a table_key, because `require_month` refuses
    it as a month, so no `_checkpoints/2026-13` directory can ever exist for it to be
    confused with. Pinned by
    `test_a_table_key_that_only_looks_month_shaped_is_left_alone`.

    A plain ValueError: nothing here is an unknown table."""
    for spec in registry.values():
        if is_month(spec.table_key):
            raise ValueError(_month_shaped_table_key(spec.name, spec.table_key))


def _assert_no_two_tables_share_a_checkpoint_namespace(registry: Registry) -> None:
    """Fail at import if two specs claim the same `table_key`.

    SEPARATE from the Delta-name check above, because `table_key` is not a Delta name:
    see the module docstring for what it is and for why folding the four roles into one
    `seen` dict refuses the LIVE registry at import.

    That is why this is its own guard rather than a dropped field. The collision it
    catches is the QUIETEST of the four. An Auto Loader checkpoint is a record of
    which files have already been processed, so two tables sharing one means the
    second stream starts up believing the first's files are its own and already
    ingested: it writes nothing and reports SUCCESS. The shared `_schemas` entry
    compounds it by merging two unrelated inferred schemas into one.

    UNIQUENESS ONLY. The other way a `table_key` goes wrong -- being month-shaped, and
    so naming a directory that is also a month -- is refused by
    `_assert_no_table_key_is_month_shaped` above, which runs first for the reason its
    docstring gives.

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
    for spec in registry.values():
        if spec.table_key in seen:
            raise ValueError(
                f"{spec.name} and {seen[spec.table_key]} both claim checkpoint "
                f"namespace {spec.table_key!r}. `table_key` is what autoloader "
                "builds _checkpoints/<month>/<table_key> and _schemas/<month>/"
                "<table_key> from, and a checkpoint records which files are already "
                "processed -- so two tables sharing one means the second stream "
                "treats the first's files as its own and already ingested, writes "
                "nothing, and reports SUCCESS. Give each table a table_key of its own."
            )
        seen[spec.table_key] = spec.name
