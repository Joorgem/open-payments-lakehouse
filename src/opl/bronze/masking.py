"""UC column masks for bronze tables that carry personal data.

WHY THE TABLE IS CREATED HERE AND NOT BY THE APPEND: every other bronze table is
created implicitly by `promote_batch`'s `saveAsTable(...)` in append mode, with
the constraint DDL following it. For a table holding personal names that ordering
is the whole problem -- the first append would create the table with names in the
clear, and the mask would arrive after. Creating the table empty, masking it, and
only then letting the append find it is the only ordering in which "the control
was applied when the data landed" is true.

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
    table would drop every row in it.

    Backticked names so a contract column that ever collides with a reserved word is
    a column and not a parse error. No column does today, which is exactly why the
    day one does nothing would be watching for it."""
    columns = [f"`{c}` STRING" for c in TABLES[contract]]
    columns += [f"`{name}` {sql_type}" for name, sql_type in METADATA_COLUMNS]
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)}) USING DELTA"
    )


def set_mask_ddl(table: str, column: str, qualified_function: str) -> str:
    """Attach the mask to one column.

    NO `DROP MASK` FIRST, and that is a decision rather than an omission. `ALTER
    COLUMN ... DROP MASK` removes the mask "if any", so `DROP` + `SET` would be
    provably idempotent -- and it would take the mask OFF a populated table for the
    width of two statements on every monthly re-run. A privacy control that is
    briefly absent every month is worse than one whose re-application may fail: the
    failure is loud, it happens with the mask already correctly in place, and the
    operator reads it in the run log. This is the ONE statement this task issues
    whose second run is not verified; see ADR 0008."""
    return (
        f"ALTER TABLE {table} ALTER COLUMN `{column}` "
        f"SET MASK {qualified_function}"
    )
