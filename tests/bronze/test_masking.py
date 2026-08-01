# tests/bronze/test_masking.py
"""The UC column mask for bronze tables that carry a natural person's name.

WHAT CAN BE TESTED HERE AND WHAT CANNOT. No DDL in this module reaches Unity
Catalog -- `is_account_group_member`, `SET MASK` and a three-part catalog name
exist only on Databricks, and the live application is a later task. So the tests
below are of two kinds, and the split is deliberate:

  * OVER THE SQL STRINGS, pinning what will be issued. A mask on a column that
    does not exist, or a `CREATE TABLE` whose types the ingest cannot write,
    fails at RUN time -- inside the job, after the table has been created -- so
    the value of pinning them locally is that the failure moves to CI.

  * OVER A REAL LOCAL DELTA TABLE, for the one claim that is not about Unity
    Catalog at all: that `create_table_ddl` builds a table the ingest's own
    DataFrame can be appended to. That claim is the whole risk of this task --
    everywhere else a bronze table is created BY the append, so its schema
    cannot disagree with the writer; here it is created by hand, and a
    disagreement is only discovered by the first real append.
"""
from __future__ import annotations

import re

import pytest

from opl.bronze.masking import (
    MASK_FUNCTION,
    MASKED_COLUMNS,
    METADATA_COLUMNS,
    PII_READER_GROUP,
    create_table_ddl,
    mask_function_ddl,
    set_mask_ddl,
)
from opl.bronze.registry import REGISTRY, table_spec
from opl.contracts.cnpj_schemas import TABLES


def test_socios_masks_both_personal_name_columns():
    """`nome_do_representante` is a natural person's name too. The spec named one
    column; masking one and not the other reads as a control applied by column
    name rather than by inspecting the contract."""
    assert MASKED_COLUMNS["socios"] == (
        "nome_socio_razao_social", "nome_do_representante")


def test_no_other_contract_declares_a_mask():
    assert set(MASKED_COLUMNS) == {"socios"}


def test_the_mask_reveals_only_to_a_named_group_and_fails_closed():
    """`is_account_group_member` returns FALSE for a group that does not exist, so a
    workspace where the group was never created shows every reader the masked
    value. That is the correct direction to fail."""
    ddl = mask_function_ddl("workspace.default.mask_personal_name")
    assert f"is_account_group_member('{PII_READER_GROUP}')" in ddl
    assert "ELSE '***'" in ddl
    # The qualified name the caller chose, not a name this module invents: the
    # function has to live in the same catalog.schema as the table, and
    # `DEFAULT.table(MASK_FUNCTION)` is what decides that.
    assert ddl.startswith("CREATE OR REPLACE FUNCTION workspace.default.mask_personal_name(")


def test_every_masked_column_exists_in_its_contract():
    """A masked column name that does not exist makes ALTER TABLE fail at run time,
    inside the job, after the table was created."""
    for contract, columns in MASKED_COLUMNS.items():
        for column in columns:
            assert column in TABLES[contract], (
                f"{contract} declares a mask on {column!r}, which is not one of its "
                f"columns ({', '.join(TABLES[contract])})")


def test_every_masked_contract_is_one_a_registered_table_ingests():
    """A mask keyed on a contract no `BronzeTable` declares is never applied.

    `ensure_masked_table` looks the columns up by `spec.contract`, so a key that
    matches no spec is not a loud failure -- it is a task that prints "declares no
    masked column", exits 0, and lets the names land unmasked. The mask is keyed by
    CONTRACT rather than by table name precisely so it follows the data rather than
    the table, which makes an orphaned key invisible from the job side."""
    registered = {spec.contract for spec in REGISTRY.values()}
    orphans = set(MASKED_COLUMNS) - registered
    assert not orphans, (
        f"{sorted(orphans)} declare masks but no registered table ingests them, so "
        "ensure_masked_table would never apply those masks and would say so as a "
        f"no-op. Registered contracts: {', '.join(sorted(registered))}"
    )


def test_a_masked_contracts_columns_are_declared_once_each():
    """A duplicate would issue the same `SET MASK` twice in one run, which is the
    re-run failure this task's idempotence argument is about, reached without a
    re-run."""
    for contract, columns in MASKED_COLUMNS.items():
        assert len(set(columns)) == len(columns), f"{contract} lists a column twice"


# --------------------------------------------------------------------------
# The generated SQL
# --------------------------------------------------------------------------

_CREATE = re.compile(r"^CREATE TABLE IF NOT EXISTS (?P<table>\S+) \((?P<body>.+)\) USING DELTA$")


def _declared_columns(ddl: str) -> tuple[tuple[str, str], ...]:
    """The (name, SQL type) pairs `create_table_ddl` declared, in order.

    Parses rather than string-matches so the assertions below are about the SCHEMA
    the DDL declares, not about its punctuation: a test that compared one long
    literal would go red on a harmless spacing change and would still not say which
    column was wrong."""
    match = _CREATE.match(ddl)
    assert match is not None, f"not a CREATE TABLE this parser understands: {ddl!r}"
    pairs = []
    for declaration in match.group("body").split(", "):
        name, _, sql_type = declaration.partition(" ")
        pairs.append((name.strip("`"), sql_type))
    return tuple(pairs)


def test_the_create_table_ddl_is_the_contract_then_bronzes_metadata():
    """ORDER AND TYPE BOTH, derived from the contract rather than restated.

    Restating the 18 columns as a literal would pass a contract change that this
    file simply had not been updated for -- and the whole point of creating this
    table by hand is that its schema has to equal the one the stream produces."""
    ddl = create_table_ddl("bronze_cnpj_socios", "socios")
    assert _declared_columns(ddl) == (
        tuple((column, "STRING") for column in TABLES["socios"]) + METADATA_COLUMNS
    )
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS bronze_cnpj_socios (")


def test_the_create_table_ddl_quotes_every_column_name():
    """Backticks, so a contract column that collides with a reserved word cannot
    turn a `CREATE TABLE` into a parse error at run time. The current contracts
    contain no such name, which is exactly why nothing would notice its arrival."""
    ddl = create_table_ddl("bronze_cnpj_socios", "socios")
    for column, _ in _declared_columns(ddl):
        assert f"`{column}`" in ddl


def test_two_of_the_metadata_columns_are_not_strings():
    """THE DEFECT THIS PINS, spelled as literals on purpose.

    An earlier draft declared all 37 columns of the estabelecimentos shape STRING.
    Bronze's CONTRACT columns are all STRING and that is a decision (ADR 0002); its
    METADATA columns are not, and the two that differ are produced by
    `F.current_timestamp()` and `F.to_date(...)`. A STRING declaration here builds a
    table the first append cannot write to -- and because this is the one bronze
    table not created BY that append, nothing else would catch it.

    The Spark test below derives the same fact from the producing code, which is the
    check that survives a change to it. This one is a literal so that flattening the
    list back to all-STRING is refused by name."""
    types = dict(METADATA_COLUMNS)
    assert types["_ingested_at"] == "TIMESTAMP"
    assert types["_snapshot_ref_date"] == "DATE"
    assert sorted(name for name, kind in METADATA_COLUMNS if kind == "STRING") == [
        "_batch_id", "_record_source", "_rescued_data", "_snapshot_month", "_source_file",
    ]


def test_no_metadata_column_collides_with_a_contract_column():
    """A collision would declare the column twice and fail the CREATE TABLE."""
    metadata = {name for name, _ in METADATA_COLUMNS}
    for contract, columns in TABLES.items():
        assert not metadata & set(columns), f"{contract} collides with bronze metadata"


def test_the_metadata_names_are_the_constants_the_writers_own():
    """Not re-spelled here. Every one of these names has a single owner elsewhere,
    and a second spelling in this module is a rename that builds a table whose
    column the stream then does not write."""
    from opl.bronze.autoloader import SOURCE_FILE_COLUMN
    from opl.bronze.promote import BATCH_COLUMN
    from opl.bronze.snapshot import SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN

    names = [name for name, _ in METADATA_COLUMNS]
    for owned in (SOURCE_FILE_COLUMN, BATCH_COLUMN, SNAPSHOT_MONTH_COLUMN,
                  SNAPSHOT_REF_DATE_COLUMN):
        assert owned in names


def test_the_rescued_column_is_the_one_the_stream_actually_configures():
    """`_rescued_data` has no constant to import -- it is a cloudFiles OPTION VALUE
    in `bronze_stream`. Read out of that source rather than restated, because it is
    the one metadata name a rename could change without this module noticing."""
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[2] / "src" / "opl" / "bronze" / "autoloader.py"
    ).read_text(encoding="utf-8")
    configured = re.findall(r'"rescuedDataColumn",\s*"([^"]+)"', source)
    assert len(configured) == 1, "bronze_stream no longer sets rescuedDataColumn exactly once"
    assert configured[0] == METADATA_COLUMNS[0][0], (
        f"the stream rescues into {configured[0]!r} but the CREATE TABLE declares "
        f"{METADATA_COLUMNS[0][0]!r}; the first append would find a column it cannot write"
    )


def test_the_set_mask_ddl_names_the_column_and_the_function():
    ddl = set_mask_ddl("workspace.default.bronze_cnpj_socios", "nome_do_representante",
                       "workspace.default.mask_personal_name")
    assert ddl == (
        "ALTER TABLE workspace.default.bronze_cnpj_socios "
        "ALTER COLUMN `nome_do_representante` "
        "SET MASK workspace.default.mask_personal_name"
    )


def test_the_mask_function_is_named_once_and_unqualified():
    """`MASK_FUNCTION` is a bare name, qualified by `DEFAULT.table(...)` at the call
    site like every other object this project names. A qualified constant here would
    hardcode `workspace.default` in a second place."""
    assert "." not in MASK_FUNCTION
    assert MASK_FUNCTION == "mask_personal_name"


def test_the_socios_check_constraint_still_collides_with_its_mask():
    """RECORDED, NOT RESOLVED -- and green today BECAUSE the collision is live.

    Unity Catalog refuses the two together, in both orders:
    `COLUMN_MASKS_CHECK_CONSTRAINT_UNSUPPORTED` ("Creating CHECK constraint on table
    <t> with column mask policies is not supported") and
    `COLUMN_MASKS_FEATURE_NOT_SUPPORTED.CHECK_CONSTRAINT` ("Setting column mask
    policies for tables with CHECK constraints"). Both are table-scoped, so it does
    not matter that the mask is on a name column and the CHECK is on `cnpj_basico`.

    socios declares both: this module masks two of its columns, and
    `promote_batch._assert_constraints` re-issues `spec.constraints` after EVERY
    append, which for socios includes `ADD CONSTRAINT cnpj_basico_len8 CHECK (...)`.
    So the live socios run appends its rows and then fails on the DDL, and the
    repair run -- which skips the already-committed append -- fails on it again.

    This test asserts the collision EXISTS so that whoever resolves it (by dropping
    the CHECK for masked contracts, or by moving the mask) is sent here and to ADR
    0008 rather than discovering both halves separately. See the ADR for why it is
    not resolved inside this task."""
    checks = [s for s in table_spec("socios").constraints if "CHECK" in s]
    assert checks, (
        "socios no longer declares a CHECK constraint -- if that was the deliberate "
        "resolution of the mask/CHECK collision, delete this test and the "
        "'Unresolved' section of ADR 0008 with it"
    )
    assert MASKED_COLUMNS.get("socios")


# --------------------------------------------------------------------------
# Against a real Delta table -- the claim that is not about Unity Catalog
# --------------------------------------------------------------------------

_ROUNDTRIP_TABLE = "masking_create_table_ddl_roundtrip"


def _ingest_shaped(spark, contract: str):
    """The DataFrame the socios ingest hands `saveAsTable`, built from the producing
    code rather than described.

    Three sources, all of them the real ones: `struct_for` for the contract columns,
    the two columns `bronze_stream` adds (the cloudFiles rescue column, which OSS
    Spark's CSV reader has no option for, and `_source_file`), and
    `add_audit_columns` for the five audit columns -- so the TYPES below are
    whatever that function actually produces today."""
    from pyspark.sql.types import StringType, StructField, StructType

    from opl.bronze.autoloader import SOURCE_FILE_COLUMN, add_audit_columns
    from opl.bronze.schema import struct_for

    schema = StructType(
        list(struct_for(contract).fields)
        + [
            StructField(METADATA_COLUMNS[0][0], StringType(), True),
            StructField(SOURCE_FILE_COLUMN, StringType(), True),
        ]
    )
    row = tuple("x" for _ in TABLES[contract]) + (
        None,
        "/Volumes/workspace/default/landing/cnpj/2026-06/socios/K3241.K03200Y1.D60613.SOCIOCSV",
    )
    return add_audit_columns(
        spark.createDataFrame([row], schema), batch_id="b1", snapshot_month="2026-06"
    )


def _shape(schema) -> tuple[tuple[str, str], ...]:
    return tuple((f.name, f.dataType.simpleString().upper()) for f in schema.fields)


def test_the_declared_metadata_is_what_the_ingest_code_actually_produces(spark):
    """DERIVED, not restated -- the half of the type pin that a change to the
    producing code cannot pass.

    `test_two_of_the_metadata_columns_are_not_strings` says TIMESTAMP and DATE as
    literals, which locks the list against being flattened. It cannot see the other
    direction: an audit column added to `add_audit_columns`, or one whose type
    changes there, leaves that test green and this table missing a column the first
    append writes."""
    produced = _shape(_ingest_shaped(spark, "socios").schema)
    contract_width = len(TABLES["socios"])
    assert produced[:contract_width] == tuple((c, "STRING") for c in TABLES["socios"])
    assert produced[contract_width:] == METADATA_COLUMNS


def test_the_hand_created_table_accepts_the_dataframe_the_ingest_writes(spark):
    """THE RISK OF THIS TASK, exercised end to end against real Delta.

    Every other bronze table is created BY `promote_batch`'s append, so its schema
    is the writer's by construction. This one is created by hand and then FOUND by
    that append, which is the only ordering in which the mask precedes the data --
    and it is also the only way for a bronze table's declared schema to be wrong.

    Local Delta rather than Unity Catalog: nothing about schema enforcement on an
    append is UC-specific, and the mask (which is) is not what this test is about.

    The re-runs are the second half. Both statements this task issues against the
    TABLE have to survive a retry, because `max_retries: 0` does not prevent one on
    INTERNAL_ERROR: creating it twice must not fail, and must not truncate the rows
    an earlier attempt's ingest already appended."""
    spark.sql(f"DROP TABLE IF EXISTS {_ROUNDTRIP_TABLE}")
    try:
        spark.sql(create_table_ddl(_ROUNDTRIP_TABLE, "socios"))
        assert spark.table(_ROUNDTRIP_TABLE).count() == 0
        assert _shape(spark.table(_ROUNDTRIP_TABLE).schema) == _declared_columns(
            create_table_ddl(_ROUNDTRIP_TABLE, "socios")
        )

        df = _ingest_shaped(spark, "socios")
        # Exactly how `opl.bronze.promote.promote_batch` writes.
        df.write.format("delta").mode("append").saveAsTable(_ROUNDTRIP_TABLE)
        landed = spark.table(_ROUNDTRIP_TABLE)
        assert landed.count() == 1
        assert _shape(landed.schema) == _shape(df.schema)

        # The retry: CREATE TABLE IF NOT EXISTS over a populated table.
        spark.sql(create_table_ddl(_ROUNDTRIP_TABLE, "socios"))
        assert spark.table(_ROUNDTRIP_TABLE).count() == 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {_ROUNDTRIP_TABLE}")


@pytest.mark.parametrize("wrong", ["STRING", "BIGINT"])
def test_the_roundtrip_test_above_is_not_vacuous(spark, wrong):
    """THE MUTATION PROBE, committed rather than run once and described.

    The test above only means something if Delta actually refuses a mistyped
    metadata column. It is not obvious that it does -- Spark casts freely in other
    contexts -- so the exact defect the brief corrects is reproduced here: declare
    `_ingested_at` as something other than TIMESTAMP and require the append to fail.

    If this ever goes green, the round-trip test above has stopped proving anything
    and the all-STRING draft would ship undetected.

    The refusal is named -- `DELTA_FAILED_TO_MERGE_FIELDS` on `_ingested_at` -- and
    not caught as a bare `Exception`, which would also be satisfied by a typo in the
    table name or by Delta being absent."""
    from pyspark.errors import AnalysisException

    table = f"{_ROUNDTRIP_TABLE}_mutant"
    mutated = create_table_ddl(table, "socios").replace(
        "`_ingested_at` TIMESTAMP", f"`_ingested_at` {wrong}"
    )
    assert f"`_ingested_at` {wrong}" in mutated, "the mutation did not apply"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    try:
        spark.sql(mutated)
        with pytest.raises(AnalysisException, match="DELTA_FAILED_TO_MERGE_FIELDS"):
            _ingest_shaped(spark, "socios").write.format("delta").mode(
                "append"
            ).saveAsTable(table)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
