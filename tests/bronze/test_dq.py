# tests/bronze/test_dq.py
from pyspark.sql import functions as F

from opl.bronze.dq import REJECT_COLUMN, evaluate, skip_notice, skipped_rules, split
from opl.bronze.rules import rules_for
from opl.bronze.snapshot import SNAPSHOT_REF_DATE_COLUMN
from opl.contracts.cnpj_schemas import TABLES


def _df(spark):
    # rows: (codigo, descricao, _rescued_data) — mimics the stream shape
    return spark.createDataFrame(
        [
            ("01", "AÇÃO", None),            # valid
            (None, "no code", None),         # invalid: null codigo
            ("  ", "blank code", None),      # invalid: empty codigo
            ("02", "mojib�de", None),   # invalid: replacement char (encoding fail)
            ("03", "extra cols", "{\"_c2\":\"x\"}"),  # invalid: rescued data present
            ("04", None, None),              # invalid: null descricao
            ("05", None, "{\"_c2\":\"y\"}"),  # both rescued data AND null descricao -> rescued wins
        ],
        ["codigo", "descricao", "_rescued_data"],
    )


def test_evaluate_tags_reasons(spark):
    out = {r.codigo: r[REJECT_COLUMN] for r in evaluate(_df(spark)).collect()}
    assert out["01"] is None
    assert out[None] == "null_or_empty_codigo"
    assert out["  "] == "null_or_empty_codigo"
    assert out["02"] == "encoding_replacement_char"
    assert out["03"] == "rescued_data_present"
    assert out["04"] == "null_or_empty_descricao"
    assert out["05"] == "rescued_data_present"


def test_split_partitions_good_and_bad(spark):
    good, bad = split(_df(spark))
    assert good.count() == 1
    assert bad.count() == 6
    assert REJECT_COLUMN not in good.columns   # dropped from good
    assert REJECT_COLUMN in bad.columns


def test_evaluate_tolerates_missing_rescued_column(spark):
    df = spark.createDataFrame([("01", "ok")], ["codigo", "descricao"])
    # no _rescued_data column present (local batch shape) -> must not crash
    out = evaluate(df).collect()
    assert out[0][REJECT_COLUMN] is None


# --- A skipped rule must be audible ------------------------------------------
#
# The skip is correct and stays correct. What was wrong is that it was INAUDIBLE,
# and the reachable silent path is rebuild + repromote: `plan_promotion`'s own
# docstring records that the documented rebuild drops bronze while LEAVING
# staging, so a batch staged before a derivation existed can be repromoted after
# it. `bronze_cnpj_estab_staging` was 35 columns until F1.4b PR B migrated it to
# 37 (2026-08-03), which closes that live instance and not the shape: the next
# derivation re-opens it. Repromoting such a batch evaluates a narrow frame
# (rule skipped, reason None), then appends into 37-column bronze, where Delta
# fills the absent column with NULL. Exactly the value the rule exists to refuse,
# waved in wordlessly. The control did not fail -- it disappeared.


def test_skipped_rules_names_each_rule_and_the_column_it_wanted():
    """Pure: the frame's column list is all this needs, so a task can ask before
    it has spent anything, and the answer names WHICH rule and WHICH column --
    'some rule was skipped' would send an operator to read the rule set."""
    rules = rules_for("empresas")
    assert skipped_rules(TABLES["empresas"], rules) == (
        ("unprovable_snapshot_ref_date", SNAPSHOT_REF_DATE_COLUMN),
    )
    assert skipped_rules(
        (*TABLES["empresas"], SNAPSHOT_REF_DATE_COLUMN), rules
    ) == ()


def test_the_notice_is_silent_when_every_rule_ran():
    """No line at all on the normal path. A warning printed by every healthy run
    is one an operator learns to skip past, which would leave the real one just
    as invisible as the silence it replaces."""
    columns = (*TABLES["empresas"], SNAPSHOT_REF_DATE_COLUMN)
    assert skip_notice(columns, rules_for("empresas"),
                       task="promote_batch", source="t") is None


def test_the_notice_names_the_rule_the_column_and_what_it_costs():
    """Readable in a Databricks run log with nothing else open, which is the whole
    requirement: the operator doing a rebuild+repromote is mid-incident and will
    not go query a schema to find out whether the gate ran."""
    notice = skip_notice(TABLES["empresas"], rules_for("empresas"),
                         task="promote_batch",
                         source="workspace.default.bronze_cnpj_estab_staging")
    assert notice is not None
    assert notice.startswith("promote_batch: ")
    assert "unprovable_snapshot_ref_date" in notice
    assert SNAPSHOT_REF_DATE_COLUMN in notice
    assert "workspace.default.bronze_cnpj_estab_staging" in notice
    # The consequence, not just the fact. A line saying only "rule skipped" does
    # not tell the operator that the rows still land in bronze carrying the NULL.
    assert "NULL" in notice
    # Not an error, and it must not read as one: skipping is correct for a table
    # written before the column existed, and a line that reads like a failure
    # sends an operator to repair something that is not broken.
    assert "not checked" in notice.lower()


def test_the_notice_and_the_gate_cannot_disagree(spark):
    """The property that makes the line trustworthy: it reports what the gate
    ACTUALLY did, in both directions, because both read the same function.

    A notice derived from a second, parallel spelling of the skip condition would
    be the worst of both worlds -- a log line asserting a rule was skipped when it
    ran, or worse, silence while it was skipped. Asserted as a round trip on a
    real frame rather than by reading the two implementations."""
    rules = rules_for("empresas")
    row = [("11111111", "ACME LTDA", "2062", "49", "1000,00", "05", "")]

    # A pre-derivation staging shape: no snapshot column. Rule skipped -> row reads
    # CLEAN, and the notice is what says so out loud.
    bare = spark.createDataFrame(row, TABLES["empresas"])
    assert [r[REJECT_COLUMN] for r in evaluate(bare, rules=rules).collect()] == [None]
    assert skip_notice(bare.columns, rules, task="t", source="s") is not None

    # The ingested shape: the column is there and NULL. Rule runs -> rejected, and
    # the notice must fall silent, or it would cry skip over a rule that fired.
    ingested = bare.withColumn(SNAPSHOT_REF_DATE_COLUMN, F.lit(None).cast("date"))
    assert [r[REJECT_COLUMN] for r in evaluate(ingested, rules=rules).collect()] == [
        "unprovable_snapshot_ref_date"
    ]
    assert skip_notice(ingested.columns, rules, task="t", source="s") is None
