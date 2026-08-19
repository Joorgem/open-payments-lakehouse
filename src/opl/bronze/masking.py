"""UC column masks for bronze tables that carry personal data.

WHY THE TABLES ARE CREATED HERE AND NOT BY THEIR WRITERS: every other bronze table
is created implicitly by `promote_batch`'s `saveAsTable(...)` in append mode, and
every quarantine table by the gate's, with the constraint DDL following. For a
table holding personal names that ordering is the whole problem -- the first write
would create the table with names in the clear, and the mask would arrive after.
Creating the table empty, masking it, and only then letting the write find it is
the only ordering in which "the control was applied when the data landed" is true.

TWO TABLES OF THE THREE, and the third is a refusal with a reason rather than an
unfinished edge: see `masked_table_ddls` for why masking STAGING would corrupt
bronze and silently disable the DQ gate, and ADR 0008 for what covers it instead.

WHY THE NAME AND NOT THE CPF: the RFB already masks CPF at source
(`***DDDDDD**`, six middle digits, irreversible), so the identifier is not the
exposure. The exposure is the civil name, which arrives complete. See ADR 0008.

WHAT THIS MODULE IS NOT. It builds SQL strings and nothing else -- no session, no
`spark.sql`, no catalog. That is what lets every statement below be pinned by a
unit test on a machine with no Databricks, which matters more here than elsewhere:
a mask on a column that does not exist, or a `CREATE TABLE` whose types the stream
cannot write, fails at RUN time, inside the job, after the table exists.
"""
from __future__ import annotations

from opl.contracts.cnpj_schemas import TABLES

# Unqualified, like every other object this project names: `DEFAULT.table(...)` at
# the call site decides the catalog and schema. A qualified constant here would be
# a second hardcoding of `workspace.default`.
MASK_FUNCTION = "mask_personal_name"
PII_READER_GROUP = "opl_pii_readers"

# THE PREDICATE, AS ONE CONSTANT, because it is now spelled in two places that must not
# disagree: the `CREATE OR REPLACE FUNCTION` below, and the read-back that asks the
# catalog whether that statement actually landed. Two literals would let the check pass
# against a body the DDL no longer emits -- which is precisely the failure the read-back
# exists to catch, wearing the check's own green tick.
MASK_PREDICATE = f"is_member('{PII_READER_GROUP}')"

# Per contract, the columns that hold a natural person's name. `socios` has TWO:
# `nome_socio_razao_social` (the partner, who may be a company or a person) and
# `nome_do_representante` (always a person -- a legal representative). The spec
# named only the first; the second is the same category of data, and masking one
# while leaving the other shows a control applied by column NAME rather than by
# looking at what the column holds.
#
# Keyed by CONTRACT, not by registry table name, so the mask follows the DATA: a
# second table ingesting the socios contract inherits it rather than needing its
# own entry. The cost is that a key naming no registered contract would be silently
# inert, which `test_every_masked_contract_is_one_a_registered_table_ingests` closes.
MASKED_COLUMNS: dict[str, tuple[str, ...]] = {
    "socios": ("nome_socio_razao_social", "nome_do_representante"),
}


def mask_function_ddl(qualified_function: str) -> str:
    """The masking function. FAILS CLOSED: `is_member` returns false for a group
    that does not exist, so a workspace where `opl_pii_readers` was never created
    shows every reader the masked value rather than the name.

    `is_member` AND NOT `is_account_group_member`, WHICH IS A REPAIR AND NOT A
    PREFERENCE. This predicate read `is_account_group_member` from F1.4b until F4,
    and in this workspace that function CANNOT BE MADE TO RETURN TRUE: it answers
    false for a workspace-local group the reader demonstrably belongs to, exactly
    one account group resolves at all (`account users`, i.e. everyone, which cannot
    be a control), and account SCIM is not served from a workspace host, so the
    account group this predicate names could not be created from here. The old
    docstring's promise that the control "becomes correct the moment
    `opl_pii_readers` exists" therefore named a moment that could not arrive, over
    55,827,243 rows, hiding from the owner as well. `is_member` reads the
    WORKSPACE-LOCAL group, which can be created (F4 created it), and it was measured
    inside a serverless job session -- as the user and as a `run_as` service
    principal -- to return true for a group the principal is in and FALSE FOR A GROUP
    THAT DOES NOT EXIST. So the substitution trades an unopenable control for an
    openable one with the same floor, and the paragraph above survives it verbatim.

    `CREATE OR REPLACE` rather than `CREATE IF NOT EXISTS`, and it is safe on a
    re-run: replacing a function that a column mask already references is the
    documented way to MODIFY a live mask, so the second run of this statement
    re-installs an identical body under an unchanged dependency."""
    return (
        f"CREATE OR REPLACE FUNCTION {qualified_function}(name STRING) "
        "RETURNS STRING "
        f"RETURN CASE WHEN {MASK_PREDICATE} "
        "THEN name ELSE '***' END"
    )


class StaleMaskPredicate(RuntimeError):
    """The DEPLOYED mask function does not carry the predicate this wheel ships.

    Raised rather than printed, and the reason is what the alternative was. The claim
    published as the safety check on this control's deploy was that
    `SELECT nome_socio_razao_social FROM ... bronze_cnpj_socios` reads `***` afterwards
    -- which it does under the repaired predicate, under the unrepaired one, under a
    task that returned early, and under a task that never ran, because
    `opl_pii_readers` is empty by decision and both spellings therefore take the same
    `ELSE` branch. A check with one reachable branch is not a check. The body of the
    function is the only observation that tells those cases apart."""


# WHAT THE CATALOG IS ASKED, AND WHY IT IS ASKED AT ALL. Measured 2026-08-18 and again
# 2026-08-19: `workspace.information_schema.routines` returns `CASE WHEN
# is_account_group_member('opl_pii_readers') THEN name ELSE '***' END` for
# `mask_personal_name`, `last_altered 2026-08-03T21:31:27.142Z` -- the F1.4b body, three
# commits of repair later. Nothing in this repository read that column before F4's
# correction pass: a grep for `information_schema.routines`, `routine_definition` and
# `DESCRIBE FUNCTION` across `src/`, `databricks/`, `tests/` and `scripts/` returned one
# hit, a comment in a job YAML.
def deployed_predicate_sql(qualified_function: str) -> str:
    """What the catalog currently says the mask function's BODY is.

    Filtered on all three parts of the name rather than on `routine_name` alone: the
    unqualified filter is what a person types at a prompt, and it would answer from any
    catalog or schema that happens to hold a function of that name."""
    parts = qualified_function.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"{qualified_function!r} is not a three-part function name; this read has to "
            "filter on catalog, schema and name, or it answers from another schema's "
            "function of the same name"
        )
    catalog, schema, name = parts
    return (
        f"SELECT routine_definition, last_altered "
        f"FROM {catalog}.information_schema.routines "
        f"WHERE routine_catalog = '{catalog}' AND routine_schema = '{schema}' "
        f"AND routine_name = '{name}'"
    )


def predicate_is_deployed(routine_definition: object) -> bool:
    """Whether a deployed body carries the predicate `mask_function_ddl` emits.

    A substring test discriminates on its own: `is_account_group_member(` does not
    contain `is_member(`. Case and whitespace are folded so the answer is about the
    predicate rather than about how the catalog chose to render it."""
    deployed = " ".join(str(routine_definition).split()).lower()
    return MASK_PREDICATE.lower() in deployed


# The columns bronze adds to every contract, WITH THEIR REAL TYPES. Measured off
# the live `workspace.default.bronze_cnpj_estabelecimentos` (37 columns = 30
# contract + these 7), not assumed: two of them are NOT strings, and declaring
# them STRING here would build a table the first append cannot match.
#
# Order matters as well as type -- these follow the contract columns in exactly the
# order the pipeline produces them: `_rescued_data` from the cloudFiles
# `rescuedDataColumn` option and `_source_file` from `bronze_stream`, then the five
# `add_audit_columns` appends in its own order.
METADATA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("_rescued_data", "STRING"),
    ("_source_file", "STRING"),
    ("_ingested_at", "TIMESTAMP"),
    ("_record_source", "STRING"),
    ("_batch_id", "STRING"),
    ("_snapshot_month", "STRING"),
    ("_snapshot_ref_date", "DATE"),
)


# The ONE column the quarantine table carries that bronze does not:
# `opl.bronze.dq.evaluate` appends the reject reason to the staging frame and the
# gate writes the whole evaluated frame, so quarantine is the bronze shape plus this.
#
# Spelled as a literal here for the same reason METADATA_COLUMNS is, and it is the
# stronger case of the two: `opl.bronze.dq` imports pyspark, and THIS module must not
# -- `registry` imports it, and the extraction scripts import `registry` on machines
# where pyspark is an optional extra usually not installed. So the name cannot be
# imported from its owner; `test_the_quarantine_column_is_the_one_the_gate_writes`
# reads it out of `dq` inside the test, where pyspark exists, and refuses a drift.
QUARANTINE_COLUMNS: tuple[tuple[str, str], ...] = (("_dq_reject_reason", "STRING"),)


def _create_table_ddl(table: str, columns: list[tuple[str, str]]) -> str:
    """`CREATE TABLE IF NOT EXISTS` over (name, SQL type) pairs.

    Backticked names so a contract column that ever collides with a reserved word is
    a column and not a parse error. No column does today, which is exactly why the
    day one does nothing would be watching for it."""
    declared = ", ".join(f"`{name}` {sql_type}" for name, sql_type in columns)
    return f"CREATE TABLE IF NOT EXISTS {table} ({declared}) USING DELTA"


def _bronze_columns(contract: str) -> list[tuple[str, str]]:
    """The contract's columns, all STRING, then bronze's metadata with its real
    types. The shape both the bronze table and its staging table carry."""
    return [(column, "STRING") for column in TABLES[contract]] + list(METADATA_COLUMNS)


def create_table_ddl(table: str, contract: str) -> str:
    """An EMPTY bronze table: the contract's columns, then bronze's metadata.

    CONTRACT columns are all STRING -- that is the bronze contract (ADR 0002),
    not a shortcut. METADATA columns are NOT: `_ingested_at` is TIMESTAMP and
    `_snapshot_ref_date` is DATE. An earlier draft of this function declared
    every column STRING, which would have created a socios table whose schema the
    first append could not match -- the append is what normally creates a bronze
    table, so creating it by hand means owning the schema the stream produces.

    `IF NOT EXISTS` and no `OR REPLACE`: this statement runs ahead of an ingest that
    may have already run, and `max_retries: 0` does not prevent a retry on
    INTERNAL_ERROR. A `CREATE OR REPLACE TABLE` that reached a populated bronze
    table would drop every row in it. Both socios tables this control covers are
    populated, and grow every month: 55,827,243 and 3,583 rows across 2026-06 and
    2026-07 as of 2026-08-03, so that is not a hypothetical."""
    return _create_table_ddl(table, _bronze_columns(contract))


def create_quarantine_ddl(table: str, contract: str) -> str:
    """An EMPTY quarantine table: the bronze shape plus the gate's reject reason.

    Created by hand for the SAME reason bronze is -- `dq_gate_batch`'s
    `saveAsTable(quarantine)` in append mode would otherwise create it with the
    rejected rows' names already in it, and a mask added afterwards would follow the
    data. Quarantine is the table a human is *expected* to open and read during
    triage, which makes it the one place where the read-time control is not
    theoretical.

    The column order is the writer's: `evaluate` uses `withColumn`, which appends,
    so the reject reason is last."""
    return _create_table_ddl(table, _bronze_columns(contract) + list(QUARANTINE_COLUMNS))


def masked_table_ddls(
    *, bronze: str, quarantine: str, contract: str
) -> tuple[tuple[str, str], ...]:
    """(qualified table, its `CREATE TABLE`) for every table this control covers, in
    creation order. Bronze and quarantine -- and, deliberately, NOT staging.

    WHY STAGING IS NOT HERE, and why that is a measured refusal rather than the
    unfinished half of the job. A UC column mask is applied "as soon as each row is
    fetched from the data source", to every reader that the mask function does not
    admit -- including the table owner, which the F1.4b run observed directly against
    `bronze_cnpj_socios`. Staging is not a leaf: `promote_batch` READS it
    (`spark.read.table(staging)`) and appends those very values to bronze. With
    `opl_pii_readers` absent, masking staging would make the next promote read `***`
    and write `***` into bronze permanently -- and the DQ gate, which evaluates
    `null_or_empty_nome_socio_razao_social` against the same read, would stop
    rejecting anything, because `***` is neither null nor empty. The 1,797 rows that
    rule caught in the live run would have landed. Databricks additionally documents
    that tables with column masks do not support streaming workloads on dedicated
    compute, and staging is written by `writeStream(...).toTable(...)`.

    So the mask on staging is not a smaller version of the mask on bronze; it is a
    control that silently disables another control and corrupts the system of record.
    It becomes correct only once the job's run-as principal is a member of
    `opl_pii_readers` -- the group now EXISTS (F4 created it) and is deliberately
    EMPTY, so the precondition is still unmet and staging is still excluded here.
    Staging is not ungoverned: `opl.bronze.pii_governance` grants and tags it, which
    is what a mask cannot do for a table `promote_batch` reads. See ADR 0008."""
    return (
        (bronze, create_table_ddl(bronze, contract)),
        (quarantine, create_quarantine_ddl(quarantine, contract)),
    )


def set_mask_ddl(table: str, column: str, qualified_function: str) -> str:
    """Attach the mask to one column.

    RE-APPLYING IT IS SAFE, and this is measured rather than argued. The SQL
    reference documents neither replacement nor an error for a column that already
    carries a mask; probed against the live workspace, a second
    `ALTER COLUMN ... SET MASK` on an already-masked column SUCCEEDED. So this
    statement is idempotent, which is what `max_retries: 0` not preventing a retry
    on INTERNAL_ERROR requires of it.

    NO `DROP MASK` FIRST, and that stays a decision rather than an omission now that
    the above is known. `DROP MASK` removes the mask "if any", so `DROP` + `SET`
    would ALSO be idempotent -- and it would take the mask OFF a populated table for
    the width of two statements on every monthly re-run. A privacy control that is
    briefly absent every month is worse than one that is never absent at all, and
    the re-apply path costs nothing. See ADR 0008."""
    return (
        f"ALTER TABLE {table} ALTER COLUMN `{column}` "
        f"SET MASK {qualified_function}"
    )
