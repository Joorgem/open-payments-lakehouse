"""One literal per bronze table: the single answer to "what is table X?".

WHY DECLARED AND NOT DERIVED: the live names follow no single pattern --
`bronze_cnpj_estab_staging` is abbreviated where `bronze_cnpj_estabelecimentos`
is spelled out, and the lookup uses `lookup` where estab uses `estab`. Deriving
`f"bronze_cnpj_{name}_staging"` would be DRY-er and would force renaming Delta
tables, one of them holding 144,193,412 rows, to satisfy an aesthetic.

So the point is not less repetition. The point is that each table's
staging/bronze/quarantine TRIPLE lives in one literal, where it cannot drift --
and drift is the documented defect: a quarantine name hardcoded in a job YAML
"sent estab triagers to a table full of unrelated F1.2 lookup rows"."""
from __future__ import annotations

import re
from dataclasses import dataclass

from opl.bronze.masking import MASKED_COLUMNS

# PRIVATE NAMES IMPORTED ACROSS A MODULE BOUNDARY, on purpose. The leading underscore
# says "not part of the registry's public surface", which stays true -- these are
# guards, and nothing outside this package and its tests calls a guard. The `_assert_`
# prefix is also the contract `tests/bronze/test_registry_guard_wiring.py` reads: an
# `_assert_*` defined at module level in any `registry*.py` is a guard, and a guard is
# CALLED from the block at the foot of THIS module. THE `registry*` HALF IS ENFORCED, not
# merely conventional: a guard called below whose module the lock's glob does not match
# turns `test_every_guard_called_at_import_is_defined_where_the_lock_can_see_it` red, so
# a future split has to be named `registry_*.py` or the lock stops seeing it. Imported
# by name rather than as
# `registry_collisions.x(...)` so the call block below reads the same for every guard,
# whichever file it lives in; the wiring lock is correct for either spelling.
from opl.bronze.registry_collisions import (
    _assert_no_table_key_is_month_shaped,
    _assert_no_two_tables_share_a_checkpoint_namespace,
    _assert_no_two_tables_share_a_delta_name,
)

# The landing modes, their roots and their two guards moved to `registry_landing.py`
# in F1b Task 3, when adding a third mode plus a second landing root would have taken
# this file past 800 lines. RE-EXPORTED HERE ON PURPOSE: `bronze_ingest`,
# `unzip_table`, `reclaim_landing` and `extract_cnpj` all import `LANDING_*` from this
# module, and rewriting four live import lines to move a constant is churn that buys
# nothing. `registry` stays the one module every consumer imports; where a name is
# DEFINED is this package's business.
from opl.bronze.registry_landing import (  # noqa: F401  (re-exported for consumers)
    FILE_FED_LANDING_MODES,
    LANDING_API,
    LANDING_GENERATED,
    LANDING_LOCAL,
    LANDING_MODES,
    LANDING_ZIPS,
    NON_FILE_FED_LANDING_MODES,
    _assert_every_landing_mode_is_classified,
    _assert_landing_modes_known,
    _assert_no_table_nothing_downloads_claims_a_downloader,
    _assert_prefixes_match_their_file_groups,
    landing_dir,
    landing_tmp_dir,
)

# The subdir machinery -- the reserved-name derivation and the three guards over the
# `subdir` field -- moved to `registry_subdirs.py` in F-API's fix pass, when the sixth
# table's constraint prose took this file to 802 lines. RE-EXPORTED for the landing
# constants' reason: `RESERVED_SUBDIRS` is read by `registry_collisions.py`'s prose and by
# two test modules that import it from here, and rewriting live import lines to move a
# derivation is churn that buys nothing.
from opl.bronze.registry_subdirs import (  # noqa: F401  (re-exported for consumers)
    RESERVED_SUBDIRS,
    _assert_no_table_claims_a_reserved_subdir,
    _assert_no_two_tables_share_a_landing_subdir,
    _assert_subdirs_are_single_path_components,
    _malformed_subdir_reason,
)
from opl.contracts import payments, ptax
from opl.contracts.catalogue import CONTRACT_COLUMNS, is_known


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
    # THE SECOND SOURCE, and the first entry here whose bytes nobody downloads.
    #
    # EVERY STRING BELOW IS LIFTED FROM `opl.contracts.payments`, not retyped. F1b
    # Task 0 pinned the staging/bronze/quarantine triple, the table key and the
    # landing subdir in one literal block there, asserted them collision-free against
    # this registry, and left the insertion to Task 3 -- so the lift is an import and
    # not a fifth place those names are spelled. `name` is the only literal, because
    # a registry KEY is this dict's own namespace: it is what an operator types as a
    # job parameter, and it happens to equal the contract for every table here
    # without anything making it so (`spec_for_contract` exists because of exactly
    # that coincidence).
    #
    # `landing=LANDING_GENERATED` IS WHAT MAKES THE ENTRY LEGAL. Two import-time
    # guards refuse it under any other mode -- `_assert_prefixes_match_their_file_
    # groups` because no FILE_GROUPS entry feeds `payments`, and its complement
    # `_assert_no_table_nothing_downloads_claims_a_downloader` because declaring one
    # would put two producers in one landing directory.
    #
    # NO `reclaim_landing` TASK EXISTS FOR THIS TABLE, which is a consequence of the
    # mode rather than an omission: that task refuses anything that is not
    # LANDING_ZIPS, because it deletes landed files only where a zip in the sibling
    # `zips/` dir is still the way back to the source. A generated table's way back
    # is the SEED -- `opl.generator` reproduces the file byte-for-byte from
    # (seed, stream_id, pool) -- which is a stronger guarantee than a retained
    # archive, and it is why the payments job stops at the promote.
    "payments": BronzeTable(
        name="payments",
        contract=payments.CONTRACT,
        table_key=payments.BRONZE_TABLE_KEY,
        staging=payments.BRONZE_STAGING_TABLE,
        bronze=payments.BRONZE_TABLE,
        quarantine=payments.BRONZE_QUARANTINE_TABLE,
        subdir=payments.LANDING_SUBDIR,
        landing=LANDING_GENERATED,
        # No downloader, so no prefix. Refused as a false statement by
        # `_assert_no_table_nothing_downloads_claims_a_downloader` if one is pasted in.
        prefix=None,
        # `transaction_id` IS THE COLUMN NO OTHER CONTRACT HAS, which is what makes
        # this tuple satisfy `test_the_new_tables_carry_a_constraint_no_other_
        # contract_could_have`: a constraint tuple pasted from any CNPJ table would
        # be missing it, and one pasted FROM here onto a CNPJ table names a column
        # that table does not have. It is also the right column on its own merits --
        # the whole duplicate/repeat distinction, and therefore every dedup claim
        # F1b makes, rests on the identity being present and non-blank.
        #
        # SHAPED LIKE THE LOOKUP'S `codigo_not_blank` TRIPLE, deliberately: NOT NULL,
        # then DROP IF EXISTS, then ADD, so a re-promote re-applies it cleanly. And
        # `length(trim(...)) > 0` rather than `= 64`: the id is a sha256 hex digest
        # today, but 64 is a fact of `stream._transaction_id`'s implementation and
        # spelling it here would be a second copy of an undeclared number that a
        # change to the derivation would leave behind as a failing promote.
        #
        # WHAT IS DELIBERATELY ABSENT, since the gate is all-or-nothing and every
        # statement here is a new way for a promote to fail over the WHOLE table: no
        # CHECK on `currency`. THE PREMISE THIS ARGUMENT USED TO REST ON IS NOW FALSE --
        # it said "`CURRENCIES` holds one member today", and F-API made the tuple
        # `("BRL", "USD")` -- so the argument is restated on what actually carries it.
        # `payments.CURRENCIES` is a VALUE DOMAIN that gains members: the contract says
        # so, `opl.gold.registry.DIM_CURRENCY` reads the tuple as its member set, and
        # this phase has now added to it once. A CHECK here would have turned that append
        # into a MIGRATION on a live bronze table -- an ALTER before the next promote
        # could succeed -- which is a schema change made by a value edit, and no count of
        # members is needed to see it. No number is quoted deliberately: a count in this
        # comment is a second copy of `len(CURRENCIES)` and would go stale again on the
        # third member.
        constraints=(
            "ALTER TABLE {table} ALTER COLUMN transaction_id SET NOT NULL",
            "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS transaction_id_not_blank",
            "ALTER TABLE {table} ADD CONSTRAINT transaction_id_not_blank "
            "CHECK (length(trim(transaction_id)) > 0)",
            "ALTER TABLE {table} ALTER COLUMN payer_cnpj_basico SET NOT NULL",
            "ALTER TABLE {table} ALTER COLUMN payee_cnpj_basico SET NOT NULL",
        ),
    ),
    # THE THIRD SOURCE, and the first whose bytes somebody else produced and nobody
    # downloaded: a job task calls BCB's public PTAX endpoint and writes a record built
    # from the validated response. `landing=LANDING_API` is what makes the entry legal,
    # under the SAME two guards the payments entry names above -- they are complements
    # since F-API Task 2, so this table is checked by the mirror rather than falling
    # between them, and it must have no file group AND no prefix.
    #
    # `LANDING_GENERATED` WAS CONSIDERED AND REJECTED, though it fits mechanically and
    # would have reused `bronze_payments_ingest.py` unchanged. That mode stamps
    # `_record_source = opl_payment_generator`, which would say this repository produced
    # the Banco Central's published rates -- a false claim in the one column that answers
    # who produced a row. `opl.config` carries the same argument for the third root.
    #
    # Every string is lifted from `opl.contracts.ptax`; `name` is the only literal, for
    # the reason the payments entry gives.
    "ptax": BronzeTable(
        name="ptax",
        contract=ptax.CONTRACT,
        table_key=ptax.BRONZE_TABLE_KEY,
        staging=ptax.BRONZE_STAGING_TABLE,
        bronze=ptax.BRONZE_TABLE,
        quarantine=ptax.BRONZE_QUARANTINE_TABLE,
        subdir=ptax.LANDING_SUBDIR,
        landing=LANDING_API,
        prefix=None,
        # `quote_date` AND `cotacao_venda` ARE THE COLUMNS NO OTHER CONTRACT HAS, which
        # is what satisfies `test_the_new_tables_carry_a_constraint_no_other_contract_
        # could_have`: a tuple pasted from any other entry here would be missing both,
        # and one pasted FROM here names columns no other table carries.
        #
        # They are also the right columns on their own merits. `quote_date` is the key
        # the FX join resolves against and the one this phase invites a writer to get
        # wrong -- the API is asked in `MM-DD-YYYY`, so writing the request's own
        # spelling produces a value that joins to nothing while every count stays green.
        # `cotacao_venda` is the rate gold converts with, so a NULL there is an
        # `amount_brl` that lowers every total by an amount nobody can name.
        #
        # `quote_date_iso_shape` WAS NAMED FOR A PROPERTY IT DID NOT ENFORCE, and that is
        # F-API's fix pass. It was `length(trim(quote_date)) = 10`, which ADMITS
        # `06-19-2026` -- the API's own spelling, and the exact value the constraint's own
        # comment says this phase invites. Measured on local Delta: the length CHECK
        # accepted that string, the regex refuses it along with `2026-6-19`, `19/06/2026`
        # and `2026-06-1x`, and accepts `2026-06-19`. Every other CHECK in this registry
        # is named for what it checks (`cnpj_basico_len8` -> `length = 8`), so the choice
        # was enforce the name or rename to the truth; the name is the useful half,
        # because a ten-character non-ISO date is precisely what a triager would not think
        # to look for behind a passing constraint called `iso_shape`.
        #
        # NO `{n}` QUANTIFIER, AND THAT IS NOT A STYLE CHOICE. `promote_batch.
        # _assert_constraints` issues `statement.format(table=tbl)`, so `[0-9]{4}` raises
        # IndexError from str.format -- AFTER the append has committed, on the run that was
        # meant to assert the constraint. Spelled out digit by digit instead, and
        # `test_every_constraint_survives_being_formatted_with_its_table` is what keeps the
        # next author from reintroducing it.
        #
        # NOT `trim(...)`, unlike the length CHECK it replaces: the gate's own
        # `bad_quote_date_shape` anchors on the raw column, and a CHECK that trimmed would
        # accept a padded value the gate refuses -- a constraint looser than the rule
        # upstream of it, which is the one direction this repository does not allow.
        #
        # WHAT IS DELIBERATELY ABSENT: no CHECK on `currency`, for exactly the reason the
        # payments entry gives -- the currency domain is a declaration that GAINS members
        # (F-API added one), and a CHECK would turn each addition into a migration on a
        # live table. Here it is weaker still: this column is decided by WHICH ENDPOINT
        # was called rather than by the body, so a wrong value means the task called a
        # different pair, which a value list would report as a bad row.
        constraints=(
            "ALTER TABLE {table} ALTER COLUMN quote_date SET NOT NULL",
            "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS quote_date_iso_shape",
            "ALTER TABLE {table} ADD CONSTRAINT quote_date_iso_shape CHECK "
            "(regexp_like(quote_date, "
            "'^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$'))",
            "ALTER TABLE {table} ALTER COLUMN cotacao_venda SET NOT NULL",
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
    run.

    ASKS `opl.contracts.catalogue`, NOT `cnpj_schemas.TABLES`, since F1b Task 3.
    This is the third of the four invariants that bound the payments entry: the
    registry now serves two sources, so the question "does this contract exist?"
    spans both and the catalogue is where that join is made. Asking one source's
    module would have made THIS function the place that decides a generated source
    cannot be registered."""
    for spec in REGISTRY.values():
        if not is_known(spec.contract):
            raise ValueError(
                f"{spec.name} names contract {spec.contract!r}, which no source "
                f"declares ({', '.join(sorted(CONTRACT_COLUMNS))}). Contracts live in "
                "opl.contracts.cnpj_schemas (the RFB files) and opl.contracts.payments "
                "(the generated stream), and opl.contracts.catalogue joins them."
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


# A statement that CREATES a CHECK: the keyword and its predicate's opening paren,
# CASE-INSENSITIVELY -- Spark SQL accepts `check (...)`, and the upper-case-only
# version of this let a lower-case paste import clean and fail after the append had
# committed. Still unmatched: `DROP CONSTRAINT IF EXISTS` (legal on a masked table,
# and the first step of masking one that carries a CHECK) and a column named `check`.
_CREATES_A_CHECK = re.compile(r"\bCHECK\s*\(", re.IGNORECASE)


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





_assert_contracts_exist()
# Contract identity before anything derived FROM a contract: both checks below
# resolve FILE_GROUPS entries by `spec.contract`, and neither is meaningful until
# that contract is known to exist and to name one table only.
_assert_no_two_tables_share_a_contract()
# THE LANDING MODE IS RESOLVED BEFORE ANYTHING KEYED ON IT, which is a new ordering
# constraint in F1b Task 3 and is the reason this call moved up two lines. Both prefix
# guards below now BRANCH on `spec.landing` -- one skips generated tables, the other
# checks only them -- so a spec carrying a typo'd mode would fall into neither and be
# exempted from both. Ordered this way, a bad mode is refused before any guard has to
# ask what it means.
_assert_landing_modes_known(REGISTRY)
# AND THE MODE'S CLASSIFICATION BEFORE THE GUARDS THAT BRANCH ON IT, which is the same
# ordering argument one line up carried one step further. `_assert_landing_modes_known`
# refuses a mode nobody DECLARED; this refuses a declared mode nobody CLASSIFIED as
# file-fed or not. Unclassified, its tables are skipped by the cross-check below and
# ACCEPTED by the mirror, so the "no FILE_GROUPS producer -> raise" branch is lost rather
# than moved -- and that branch's own message says the ingest "would report SUCCESS having
# read an empty source dir". It takes no registry because it is a claim about the
# declaration, which must hold before any table names the mode.
_assert_every_landing_mode_is_classified()
_assert_prefixes_match_their_file_groups(REGISTRY)
# The COMPLEMENT of the line above, and it must stay beside it: the prefix cross-check
# skips every table no downloader feeds, and this is the only thing that says anything
# about those. The two skips are exact complements since F-API Task 2, so between them
# they are total over the registry for any set of landing modes -- which is what stops a
# fifth mode falling into neither, the way `api` fell into neither when both were scoped
# positively. Total EXAMINATION and not a total verdict: which of the two questions a
# table is asked still turns on the classification the guard above now refuses to leave
# unstated. Split rather than folded in, for the reason the subdir trio is three
# functions -- each refusal is a different sentence about a different mistake.
_assert_no_table_nothing_downloads_claims_a_downloader(REGISTRY)
# Also keyed on the contract, so it belongs in this group: `MASKED_COLUMNS` is
# keyed by contract, not by table name, and asking whether THIS table is masked is
# meaningless until its contract is known to exist and to name one table only.
_assert_no_masked_contract_declares_a_check_constraint()
# Shape before content: the reserved-name check compares exact strings, so it is
# total only over values already known to be a single directory name.
_assert_subdirs_are_single_path_components(REGISTRY)
_assert_no_table_claims_a_reserved_subdir(REGISTRY)
# Individually-wrong before collectively-wrong, so the operator is never told the
# wrong fix. Two tables both declaring subdir="zips" -- or both "" -- are a duplicate
# AND two reserved/malformed values, and uniqueness would report it first as "give
# each table a subdir of its own", which is advice to rename one of them to something
# else reserved. Ordered last, the operator is told the real problem: neither value
# may be used at all.
_assert_no_two_tables_share_a_landing_subdir(REGISTRY)
# The last three live in `opl.bronze.registry_collisions` -- see that module for why the
# seam is there and why they take REGISTRY as an argument. They are called HERE, in this
# one ordered block, because this is the module every consumer imports and because the
# order below is load-bearing and has to be reviewable in one place.
#
# No ordering between this group and anything above: it reads fields nothing else
# validates, in namespaces that are independent of the contract's by design.
_assert_no_two_tables_share_a_delta_name(REGISTRY)
# Shape before collision, for the reason the subdir group above is ordered that way:
# two tables both keyed "2026-06" are a duplicate AND two month-shaped keys, and
# uniqueness reported first would tell the operator to "give each table a table_key of
# its own" -- advice to rename one of them to another value that is still a month.
_assert_no_table_key_is_month_shaped(REGISTRY)
_assert_no_two_tables_share_a_checkpoint_namespace(REGISTRY)
