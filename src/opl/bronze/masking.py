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
    """The masking function. FAILS CLOSED: `is_account_group_member` returns false
    for a group that does not exist, so a workspace where `opl_pii_readers` was
    never created shows every reader the masked value rather than the name.

    `CREATE OR REPLACE` rather than `CREATE IF NOT EXISTS`, and it is safe on a
    re-run: replacing a function that a column mask already references is the
    documented way to MODIFY a live mask, so the second run of this statement
    re-installs an identical body under an unchanged dependency."""
    return (
        f"CREATE OR REPLACE FUNCTION {qualified_function}(name STRING) "
        "RETURNS STRING "
        f"RETURN CASE WHEN is_account_group_member('{PII_READER_GROUP}') "
        "THEN name ELSE '***' END"
    )


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
    populated today (27,836,651 and 1,797 rows), so that is not a hypothetical."""
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
    It becomes correct the moment `opl_pii_readers` exists AND the job's run-as
    principal is a member of it, which is F4's work. See ADR 0008."""
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
