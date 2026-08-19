# tests/bronze/test_rule_overlap.py
"""The all-matching-rules aggregate, driven out of real frames.

THE ONE PROPERTY THIS FILE EXISTS FOR: the >= 2 counter must be ABLE to report a
non-zero. ADR 0006's reversal condition 1 is a count, and a count whose only observed
value is zero is indistinguishable from a count that cannot count -- which is not a
hypothetical worry here. This project has shipped five guards whose output could not tell
"passed" from "never ran": an empty list that could not be non-empty, a hermetic fake
answering from a dict the test supplied, a `from_cache` key that never existed, an expiry
probe with no control, and a falsification criterion naming a deleted function. So
`test_a_row_matching_two_rules_is_seen_by_both_counters` builds a frame in which one row
matches exactly two rules and asserts the counter reads 1 -- and the test beside it drives
the SAME frame through `dq.evaluate` to show the gate reports only one of the two, which
is the difference the aggregate exists to measure.

THE FRAMES ARE REAL AND THE RULES ARE THE SHIPPED ONES. Nothing here supplies a predicate
or a count: every number comes from `rules_for(...)` evaluated by Spark over rows written
in this file, which is the only arrangement in which a wrong aggregate can fail."""
from __future__ import annotations

from opl.bronze.dq import REJECT_COLUMN, RESCUED_REASON, evaluate
from opl.bronze.promote import BATCH_COLUMN
from opl.bronze.registry import REGISTRY
from opl.bronze.rule_overlap import (
    HEADLINE_KEYS,
    RESCUED_AND_A_RULE,
    ROW_COUNT,
    RULES_MATCHED_2_OR_MORE,
    aggregate_columns,
    overlap_frame,
    rule_conditions,
)
from opl.bronze.rules import rules_for
from opl.bronze.snapshot import SNAPSHOT_REF_DATE_COLUMN
from opl.contracts.catalogue import CONTRACT_COLUMNS
from opl.contracts.cnpj_schemas import TABLES

# A lost byte, as Java's cp1252 decoder leaves it. The same character `rule_predicates`
# folds `encoding_replacement_char` over every column of a contract with.
_LOST_BYTE = "�"

_BLANK_AND_DAMAGED = (None, f"moji{_LOST_BYTE}bake")
_LOOKUP_COLUMNS = ["codigo", "descricao"]


def _lookup(spark, rows, columns=None):
    """A frame with an EXPLICIT string schema, never an inferred one.

    Inference reads the first row, and every frame here deliberately opens with a NULL
    in a required column -- which is `CANNOT_INFER_EMPTY_SCHEMA` rather than a string
    column. Declaring the types also keeps the frames the shape bronze actually has:
    all-string (ADR 0002)."""
    names = columns or _LOOKUP_COLUMNS
    return spark.createDataFrame(rows, ", ".join(f"`{name}` string" for name in names))


def _counts(spark, rows, columns=None, table="lookup"):
    """The whole-frame aggregate as a plain dict, so a test reads like the report."""
    df = _lookup(spark, rows, columns)
    return overlap_frame(df, rules_for(table)).collect()[0].asDict()


def test_a_row_matching_two_rules_is_seen_by_both_counters(spark):
    """THE TEST THIS MEASUREMENT IS WORTHLESS WITHOUT.

    One row, blank in a required column AND carrying U+FFFD in another -- exactly the
    pair ADR 0006 names as the shadowing hazard ("a row that is both blank in a required
    column and carries a lost byte elsewhere reports the TOLERATED reason"). Both rules
    must count it, and the >= 2 counter must read 1.

    The frame is three rows, not one, so a counter that returned the ROW COUNT, or the
    number of matching rules, or 1 for any non-empty frame, is wrong here rather than
    accidentally right."""
    counts = _counts(
        spark,
        [
            ("01", "clean"),           # matches nothing
            _BLANK_AND_DAMAGED,        # matches TWO rules
            (None, "blank only"),      # matches one
        ],
    )
    assert counts[ROW_COUNT] == 3
    assert counts["null_or_empty_codigo"] == 2
    assert counts["encoding_replacement_char"] == 1
    assert counts["null_or_empty_descricao"] == 0
    assert counts[RULES_MATCHED_2_OR_MORE] == 1


def test_the_gate_reports_only_the_first_of_the_two(spark):
    """The SAME row, through `dq.evaluate`: one reason, and it is the earlier rule.

    This is the whole justification for a second aggregate. `_dq_reject_reason` is
    first-match-wins, so the lost byte in that row is invisible to every count taken off
    the quarantine -- including the six cells ADR 0006 measured. A reader who takes this
    file's numbers for the gate's numbers is reading two different questions."""
    tagged = evaluate(_lookup(spark, [_BLANK_AND_DAMAGED])).collect()
    assert [row[REJECT_COLUMN] for row in tagged] == ["null_or_empty_codigo"]


def test_a_row_matching_one_rule_is_not_counted_as_an_overlap(spark):
    """The counter's other direction, and the control for the test above.

    Without it, a >= 2 counter that fired on >= 1 would pass the two-rule test and
    silently report every rejected row in the workspace as an overlap."""
    counts = _counts(spark, [("01", "clean"), (None, "blank only")])
    assert counts["null_or_empty_codigo"] == 1
    assert counts[RULES_MATCHED_2_OR_MORE] == 0


def test_a_null_predicate_counts_as_no_match_and_does_not_poison_the_overlap(spark):
    """The three-valued half of the arithmetic, which is where the spellings differ.

    A row NULL in every column makes `_encoding_check`'s fold NULL rather than false --
    `F.lit(False) | NULL` is NULL -- while both `null_or_empty_*` rules are TRUE of it.
    Two things must hold and neither is automatic. The encoding count must read 0: a NULL
    condition is not a match, which is exactly how `_reject_reason`'s `when` chain treats
    it, so the gate and this report cannot disagree about the row. And the >= 2 count must
    read 1: an implementation summing `condition.cast("int")` gets NULL for the row's
    match count, `NULL >= 2` is NULL, and the overlap this test exists to see disappears
    -- silently, and only for rows carrying a NULL, which is most of the real ones."""
    counts = _counts(spark, [(None, None)])
    assert counts["null_or_empty_codigo"] == 1
    assert counts["null_or_empty_descricao"] == 1
    assert counts["encoding_replacement_char"] == 0
    assert counts[RULES_MATCHED_2_OR_MORE] == 1
    assert evaluate(_lookup(spark, [(None, None)])).collect()[0][REJECT_COLUMN] == (
        "null_or_empty_codigo"
    )


def test_rescued_data_present_is_counted_apart_from_the_rules(spark):
    """`rescued_data_present` is not a rule and is not in the >= 2 count.

    It is a literal in `opl.bronze.dq`, applied above every per-table rule, and it is
    36% of the live quarantine (2,000 of 5,589 rows) -- F1b's deliberately injected
    schema drift. The row below is rescued AND blank in a required column: it matches
    exactly ONE rule, so `RULES_MATCHED_2_OR_MORE` must stay 0 while
    `RESCUED_AND_A_RULE` reads 1. A blended "two or more conditions" counter would
    report 1 for both and make the two facts indistinguishable."""
    counts = _counts(
        spark,
        [
            ("01", "clean", None),
            ("02", "drifted", '{"_c2":"x"}'),        # rescued, no rule matches
            (None, "blank", '{"_c2":"y"}'),          # rescued AND one rule
        ],
        columns=[*_LOOKUP_COLUMNS, "_rescued_data"],
    )
    assert counts[RESCUED_REASON] == 2
    assert counts["null_or_empty_codigo"] == 1
    assert counts[RULES_MATCHED_2_OR_MORE] == 0
    assert counts[RESCUED_AND_A_RULE] == 1


def test_a_frame_with_no_rescued_column_reports_zero_rather_than_raising(spark):
    """The local-batch shape. `evaluate` tolerates it, and so must this."""
    counts = _counts(spark, [("01", "clean")])
    assert counts[RESCUED_REASON] == 0
    assert counts[RESCUED_AND_A_RULE] == 0


def test_a_skipped_rule_is_absent_from_the_report_rather_than_reported_as_zero(spark):
    """A rule the gate would SKIP must not appear at all.

    `unprovable_snapshot_ref_date` declares a column outside the contract, so on a bare
    contract frame `dq.skipped_rules` says it does not run. Reporting it as 0 would say
    "this control was measured and found nothing" about a control that never ran -- the
    exact confusion `dq.skip_notice` exists to prevent. The skip set is asked of `dq`
    here, not re-decided, so the report and the gate cannot disagree about it."""
    columns = list(TABLES["estabelecimentos"])
    df = spark.createDataFrame([tuple("x" for _ in columns)], columns)
    reported = {reason for reason, _ in rule_conditions(df, rules_for("estabelecimentos"))}
    assert "unprovable_snapshot_ref_date" not in reported
    assert "bad_cnpj_basico_length" in reported
    assert SNAPSHOT_REF_DATE_COLUMN not in df.columns


def test_the_same_rule_is_reported_once_the_frame_carries_its_column(spark):
    """The control for the skip test: the rule is skippable, not missing.

    Without this, a `rule_conditions` that had simply dropped
    `unprovable_snapshot_ref_date` from the estabelecimentos set -- or a rule set that
    no longer contained it -- would pass the test above identically."""
    columns = [*TABLES["estabelecimentos"], SNAPSHOT_REF_DATE_COLUMN]
    df = spark.createDataFrame([tuple("x" for _ in columns)], columns)
    reported = {reason for reason, _ in rule_conditions(df, rules_for("estabelecimentos"))}
    assert "unprovable_snapshot_ref_date" in reported


def test_batch_grain_partitions_the_frame_and_keeps_the_overlap_with_its_batch(spark):
    """One scan, one row per batch -- and the overlap lands in the batch that holds it.

    Batch grain is what the entry point asks for, and the property that makes the
    coarser grain derivable is that batches partition the rows: the whole-frame numbers
    are the column-wise sums of these. Asserted rather than assumed, because a
    `groupBy` that leaked rows across keys would still total correctly."""
    rows = [
        ("01", "clean", "b1"),
        (*_BLANK_AND_DAMAGED, "b1"),
        (None, "blank only", "b2"),
    ]
    columns = [*_LOOKUP_COLUMNS, BATCH_COLUMN]
    df = _lookup(spark, rows, columns)
    per_batch = {
        row[BATCH_COLUMN]: row.asDict()
        for row in overlap_frame(df, rules_for("lookup"), group_by=(BATCH_COLUMN,)).collect()
    }
    assert per_batch["b1"][ROW_COUNT] == 2
    assert per_batch["b1"][RULES_MATCHED_2_OR_MORE] == 1
    assert per_batch["b2"][ROW_COUNT] == 1
    assert per_batch["b2"][RULES_MATCHED_2_OR_MORE] == 0
    whole = overlap_frame(df, rules_for("lookup")).collect()[0].asDict()
    assert whole["null_or_empty_codigo"] == sum(
        counts["null_or_empty_codigo"] for counts in per_batch.values()
    )


def test_the_report_leads_with_the_headline_keys_in_the_order_that_tuple_declares(spark):
    """`HEADLINE_KEYS` says "in this order", and this is what makes that a fact.

    Until F4's correction pass the tuple's only consumer was a length assertion, while
    `aggregate_columns` hand-wrote its four aliases and the job task derived its key list
    from `frame.columns` -- so the tuple described an order nothing read. It now supplies
    the order, and this pins the projection against it: the headline numbers first, in
    that sequence, then one column per running rule."""
    df = _lookup(spark, [("01", "clean")])
    columns = overlap_frame(df, rules_for("lookup")).columns
    assert columns[: len(HEADLINE_KEYS)] == list(HEADLINE_KEYS)
    assert set(columns[len(HEADLINE_KEYS) :]) == {r for r, _ in rules_for("lookup")}


def test_no_rule_running_omits_the_overlap_counters_rather_than_reporting_zero(spark):
    """The module's own principle, applied to its own headline counter.

    `test_a_skipped_rule_is_absent_from_the_report_rather_than_reported_as_zero` above
    refuses to print 0 for one control that never ran. `rules_matched_2_or_more` = 0 over
    a frame where NO rule ran says exactly that sentence about every control at once --
    and it is the number ADR 0006's reversal condition 1 is settled by, so it is the
    worst one to be able to read as a measurement. Both rule-derived counters are
    therefore absent, and the two that do not depend on a rule stay.

    LATENT, NOT LIVE, and driven through the shipped path rather than asserted of it: an
    empty rule set is the only way to reach this today, because `rules.REQUIRES_COLUMN`
    holds one entry and the smallest registered contract has three rules."""
    df = _lookup(spark, [("01", "clean")])
    assert overlap_frame(df, []).columns == [ROW_COUNT, RESCUED_REASON]
    running = overlap_frame(df, rules_for("lookup")).columns
    assert RULES_MATCHED_2_OR_MORE in running, "the control: a real rule set reports both"
    assert RESCUED_AND_A_RULE in running


def test_no_contract_can_name_a_rule_after_a_headline_alias():
    """Reason strings and the four headline aliases share ONE column namespace.

    Nothing asserted they were disjoint, and a collision is silent in the direction this
    project keeps finding: `aggregate_columns` would alias two columns `rows`, the job
    task's `row[key]` would read one of them, and a report would carry a rule's count
    under a headline name or the reverse. 54 distinct keys in the 2026-08-18 sweep -- 50
    reason names and 4 aliases -- so this is insurance, not a repair. Also pins that a
    contract cannot name the same rule twice, which collides the same way."""
    for table, spec in REGISTRY.items():
        names = [reason for reason, _ in rules_for(spec.contract)]
        clash = sorted(set(names) & set(HEADLINE_KEYS))
        assert not clash, f"{table}: rule reason(s) {clash} collide with a headline alias"
        assert len(names) == len(set(names)), f"{table}: a reason string is repeated"


def test_every_rule_of_every_contract_gets_its_own_aggregate_column(spark):
    """No contract's rule set can be silently half-measured.

    Over all seven registered contracts, against a frame carrying the contract's own
    columns plus the one metadata column a rule reaches for, so nothing is skipped and
    the projection is the full set. The projection is one column per running rule plus
    the four headline numbers; a rule missing from it is a rule whose count never
    appears in the report, which reads as zero to anyone who does not know the set.

    Empty frames on purpose: this asserts the SHAPE of the projection, and populating
    seven contracts would be seven fixtures asserting what `lookup`'s rows already do."""
    for table in REGISTRY:
        rules = rules_for(REGISTRY[table].contract)
        names = [*CONTRACT_COLUMNS[REGISTRY[table].contract], SNAPSHOT_REF_DATE_COLUMN]
        df = spark.createDataFrame([], ", ".join(f"`{name}` string" for name in names))
        running = rule_conditions(df, rules)
        assert len(running) == len(rules), f"{table}: a rule was skipped on a full frame"
        assert len(aggregate_columns(df, rules)) == len(running) + len(HEADLINE_KEYS)
