# src/opl/bronze/rule_overlap.py
"""How many rows match EACH bronze DQ rule, and how many match more than one.

WHAT THIS IS, IN ONE SENTENCE, BECAUSE IT IS EASY TO READ AS SOMETHING ELSE: this is
ADR 0006's reversal condition 1 and NOTHING ELSE. It is a MEASUREMENT. It changes no
gate, adds no tolerance, and is not a step towards adding one -- ADR 0006's condition 2
is evidentiary and no quantity of code closes it (empresas' numerator is 1 and
estabelecimentos' is 0 for the family a threshold would tolerate; both need at least six
monthly observations with a reject count >= 10). Condition 3, the reclaim decoupling,
shipped in this phase's Task 2. So of the three conditions that would reverse "no rate
threshold", this ships one, the reclaim shipped one, and the third cannot be shipped at
all. Nothing here is a licence to tolerate a reject.

WHY IT EXISTS. `dq._reject_reason` builds a first-match-wins `when` chain, so
`_dq_reject_reason` names the FIRST rule that fired and says nothing about the rest.
ADR 0006 rejected a per-reason tolerance on exactly that: a row that is both blank in a
required column and carrying a lost byte reports the blank -- the reason a threshold
would tolerate -- while `encoding_replacement_char`, which is never tolerated, is
invisible. A "never tolerated" reason that a tolerated reason can hide is not a never.
The ADR's own words for the precondition: "per-reason counts derived from **all**
matching rules, not from the first ... it must ship BEFORE any per-reason tolerance,
never with it."

BESIDE `evaluate`, NOT INSIDE IT, and the reason is not tidiness. `evaluate` runs on the
write path of every ingest and its output is a COLUMN ON EVERY ROW that a quarantine
table then stores; widening what it reports would change data an operator filters on and
would put an aggregate's cost on a path that does not need it. This is a different
question -- "over this whole frame, how many rows does each rule see?" -- answered by one
aggregate pass, and the two must not be able to disagree: every skip decision here is
ASKED of `dq.skipped_rules`, and the `_rescued_data` test is `dq.rescued_condition`, the
same expression the chain uses. Nothing about a rule is re-spelled here.

THE RULE TUPLE STAYS A 2-TUPLE. `(reason, predicate)` is unpacked by seven exact-list
assertions across four test files, by `dq.skipped_rules`, and at every one of the
`rules_for(` call sites. Nothing in this module widens it: the per-rule numbers come back
as a mapping KEYED BY REASON NAME, which is the only shape that adds a fact about a rule
without touching the rule.

`rescued_data_present` IS NOT A RULE AND IS REPORTED SEPARATELY. It is a literal in
`opl.bronze.dq`, applied ABOVE every per-table rule, and it appears in no `rules_for`
set. It is also not a rounding error: 2,000 of the 5,589 rows in the live quarantine
tables carry it -- 36% -- and they are F1b's deliberately injected schema drift, the
proof that Auto Loader's `rescuedDataColumn` catches a source that grew a field. Folding
it into a per-rule table keyed on `rules.py`'s prose would attribute a third of the
rejects to rules that never fired. It gets its own key, `RESCUED_REASON`, and it is
excluded from `RULES_MATCHED_2_OR_MORE`, which is a count of RULES.

WHY THERE ARE TWO OVERLAP NUMBERS AND NOT ONE, which is a decision rather than an extra:

  * `RULES_MATCHED_2_OR_MORE` -- rows matching two or more RULES. This is the number
    ADR 0006's condition 1 is about, and the one whose being non-zero means a per-reason
    tolerance would let a never-tolerated reason be shadowed by a tolerated one.
  * `RESCUED_AND_A_RULE` -- rows where `_rescued_data` is present AND at least one rule
    also matches. Reported because the ADR's shadowing argument has a DIRECTION and this
    is the other end of it. `rescued_data_present` outranks every rule, so it can never
    be the hidden reason; what it CAN do is hide a rule beneath it. That is harmless for
    a tolerance (the reason on top is one nobody proposes tolerating) and it is not
    harmless for a reader, because those rows are counted once in the per-rule numbers
    below and reported to the quarantine under a reason none of those rules produced.
    A single blended "two or more conditions" figure would have made the two
    indistinguishable, and each answers a question the other cannot.

NULL PREDICATES COUNT AS NO MATCH, THE SAME WAY THE GATE DECIDES. Every predicate here is
three-valued -- `col.contains(x)` is NULL when `col` is NULL -- and `F.when(cond, 1)
.otherwise(0)` does not select the branch on NULL, which is exactly what
`_reject_reason`'s `when` chain does with the same condition. So a row this module counts
under a reason is a row the gate would have been able to report under that reason, and a
row it does not count is one the gate could not. Written down rather than left to be
re-derived: the two agreeing is the whole value of the measurement, and `SUM(CASE WHEN p
THEN 1 ELSE 0 END)` versus `COUNT(CASE WHEN p THEN 1 END)` versus `SUM(int(p))` are three
spellings that differ precisely on NULL.

COUNTS, NOT PAIRS. This reports how many rows each rule matches and how many match two or
more; it does not report WHICH two. A pairwise table is 190 columns at the merchant
contract's 20 rules -- the widest rule set in the registry -- for an answer nobody needs
until the >= 2 count is non-zero somewhere, at which point the per-rule counts already
narrow it to the rules whose counts can overlap and a follow-up query answers it against
one table and one batch. (This paragraph read "435 columns at ... 30 rules" until F4's
correction pass. 435 is C(30,2) and the 30 was estabelecimentos' COLUMN count borrowed
into a sentence about merchant's RULES: NO CONTRACT IN THIS REPOSITORY HAS 30 RULES --
lookup 3, empresas 6, estabelecimentos 7, socios 7, ptax 10, payments 11, merchant 20,
64 in total, which is the 50 distinct reason names the 2026-08-18 sweep printed plus the
duplicates shared across contracts.)

WHAT IT DOES NOT DO: write anything. See `databricks/src/measure_rule_overlap.py` for why
its product is a printed number rather than a table."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from opl.bronze.dq import RESCUED_REASON, Rules, rescued_condition, skipped_rules

# The three keys that are not a rule's reason string. Spelled here and read by the entry
# point rather than retyped there: they are column aliases on the returned frame, so a
# second spelling would be a `KeyError` inside a job task after 338M rows have been
# scanned.
ROW_COUNT = "rows"
RULES_MATCHED_2_OR_MORE = "rules_matched_2_or_more"
RESCUED_AND_A_RULE = "rescued_and_at_least_one_rule"

# The two headline numbers DERIVED FROM THE RULES, named as a group because they are the
# two that must not be reported when no rule ran -- see `headline_keys`.
RULE_DERIVED_KEYS = (RULES_MATCHED_2_OR_MORE, RESCUED_AND_A_RULE)

# What the report prints before the per-rule numbers, IN THIS ORDER -- and the order is
# this tuple's rather than merely described by it: `aggregate_columns` aliases its
# headline aggregates by looking each key up here, so reordering this line reorders the
# report and deleting a key from it deletes the number. Until F4's correction pass this
# comment claimed an order nothing read; the tuple's only consumer was a length
# assertion, so renaming or reordering an alias turned nothing red.
# `RESCUED_REASON` is in this group and NOT among the rules, which is the module
# docstring's point made as data: it comes from `opl.bronze.dq`, not from any `rules_for`
# set.
HEADLINE_KEYS = (ROW_COUNT, RESCUED_REASON, *RULE_DERIVED_KEYS)


def headline_keys(conditions: tuple[tuple[str, Column], ...]) -> tuple[str, ...]:
    """The headline keys for a frame on which exactly `conditions` will run.

    EVERY key when at least one rule runs; the two `RULE_DERIVED_KEYS` are DROPPED when
    none does, and that is this module's own principle applied to its headline counter
    rather than an exception carved out for it. A rule the gate skips is absent from this
    report rather than printed as 0 (`rule_conditions`, and the test that pins it), for
    the reason `dq.skip_notice` exists: "this control was measured and found nothing" and
    "this control never ran" must not be the same line. A `rules_matched_2_or_more` of 0
    over a frame where NO rule ran is exactly that sentence about every control at once --
    and it is the sentence ADR 0006's reversal condition 1 is settled by.

    Absence over a `skipped` sentinel because absence is already how this module says it:
    a frame with 6 of 7 rules skipped prints one rule column and no zeros for the other
    six, so a 7-of-7 frame answering with a sentinel instead would be a second idiom for
    the same fact, reachable only in the degenerate case. LATENT, NOT LIVE:
    `rules.REQUIRES_COLUMN` holds exactly one entry today (`unprovable_snapshot_ref_date`),
    so no registered contract -- the smallest is lookup's 3 rules -- can empty its
    condition set on any frame."""
    if conditions:
        return HEADLINE_KEYS
    return tuple(key for key in HEADLINE_KEYS if key not in RULE_DERIVED_KEYS)


def rule_conditions(df: DataFrame, rules: Rules) -> tuple[tuple[str, Column], ...]:
    """(reason, condition) for every rule `dq.evaluate` would actually RUN on `df`.

    The skip set is ASKED of `dq.skipped_rules` rather than re-decided, for the reason
    `_reject_reason` asks it: a count that included a rule the gate skips would report a
    control as measured when the gate never ran it, which is the exact failure
    `skip_notice` exists to make audible.

    Returns the predicates already CALLED. They are zero-arg factories because
    `rules_for` is inspected in tests with no SparkSession; a DataFrame is in hand here,
    so a session is too."""
    skipped = {reason for reason, _ in skipped_rules(df.columns, rules)}
    return tuple(
        (reason, predicate())
        for reason, predicate in rules
        if reason not in skipped
    )


def _hits(condition: Column) -> Column:
    """1 where the condition is TRUE, 0 where it is false OR NULL.

    The `otherwise` is what makes NULL a non-match, which is the gate's own reading --
    see the module docstring. Not `cast("int")`, which propagates NULL and would make
    every SUM below NULL for any contract with a nullable column."""
    return F.when(condition, F.lit(1)).otherwise(F.lit(0))


def _matches_per_row(conditions: tuple[tuple[str, Column], ...]) -> Column:
    """How many RULES each row matches, as one row-wise sum of indicators.

    `F.lit(0)` as the base rather than the first condition, so an empty condition set is
    a total of 0 rather than an IndexError. THAT TOTAL IS NOT THEN REPORTED: the docstring
    here used to call the resulting zero "correctly zero instead of absent", which was
    this module exempting its own headline counter from the rule it applies to every
    other control. `headline_keys` drops the two rule-derived counters when nothing ran,
    so the 0 this returns for an empty set reaches no report."""
    total = F.lit(0)
    for _, condition in conditions:
        total = total + _hits(condition)
    return total


def aggregate_columns(df: DataFrame, rules: Rules) -> list[Column]:
    """Every number this module reports, as one projection: no rule is scanned twice.

    ONE AGGREGATE PASS, which is the shape rather than an optimisation. The alternative
    -- a filter and a count per rule -- is 7 scans of the 72,318,968-row estabelecimentos
    staging table, one per rule of that contract, and worse, it is 7 chances for the rule
    set the counts describe to differ from the rule set that produced any one of them.
    (This read "30 scans" until F4's correction pass, borrowing merchant's rule count --
    itself misstated as 30 -- into a sentence about the widest TABLE. The two are
    unrelated quantities: estabelecimentos is the widest table and has 7 rules; merchant
    has the widest rule set at 20 and is nowhere near the largest.)

    The headline aliases are ORDERED BY `HEADLINE_KEYS` and looked up in it, so that
    tuple's "in this order" is a fact about this projection rather than a comment beside
    it, and a key it does not name cannot reach the report."""
    conditions = rule_conditions(df, rules)
    rescued = rescued_condition(df)
    matched = _matches_per_row(conditions)
    headline = {
        ROW_COUNT: F.count(F.lit(1)),
        RESCUED_REASON: F.sum(_hits(rescued)),
        RULES_MATCHED_2_OR_MORE: F.sum(_hits(matched >= F.lit(2))),
        RESCUED_AND_A_RULE: F.sum(_hits(rescued & (matched >= F.lit(1)))),
    }
    return [
        *(headline[key].alias(key) for key in headline_keys(conditions)),
        *(F.sum(_hits(condition)).alias(reason) for reason, condition in conditions),
    ]


def overlap_frame(
    df: DataFrame, rules: Rules, *, group_by: tuple[str, ...] = ()
) -> DataFrame:
    """The aggregate, optionally at a grain.

    `group_by` empty gives one row for the whole frame; `group_by=(BATCH_COLUMN,)` gives
    one row per batch off a SINGLE scan, which is what the entry point asks for and why
    there is no per-batch filter anywhere in this module. Batches partition the rows, so
    a table-grain figure is the column-wise sum of its batch-grain rows -- including
    the >= 2 count, since every row belongs to exactly one batch. Batch grain is
    therefore strictly the more informative of the two and the coarser one is derivable
    from it, which is why only one is taken."""
    columns = aggregate_columns(df, rules)
    if group_by:
        return df.groupBy(*group_by).agg(*columns)
    return df.agg(*columns)
