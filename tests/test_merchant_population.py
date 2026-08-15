# tests/test_merchant_population.py
"""The F-DB population arithmetic, asserted with `docker compose` stopped.

WHY THIS RUNS WITHOUT A DATABASE. Every number in this phase's headline is arithmetic --
1,088 merchants over 1,024 CNPJs, 32 inserted, 48 updated visibly, 24 silently, 16 deleted,
8 committed out of order -- and arithmetic that only a container can check is arithmetic
that CI never checks. `scripts/merchant_population.py` imports no psycopg for exactly that
reason, which is the discipline plan Task 4 hands `postgres_source.py` one task early.

AND IT CLOSES THE PIN. Before this file, `scripts/merchant_cnpj_pool.txt` had no reader, no
caller and no test: `grep -rn merchant_cnpj_pool --include=*.py` returned nothing, and the
header's stripping rule described a reader that did not exist. The published sha256 was a
one-off manual check that nothing re-ran.
"""

import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from opl.generator.cnpj_pool import validated_pool

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import merchant_population as population  # noqa: E402

# The divergence set `tests/vault/test_hashing_spark.py` pins starts at U+2C5F. Bounding
# the seeded characters well below it is a stronger claim than "none of the forty appear",
# and it needs no second spelling of a set that lives in another test module.
DIVERGENCE_FLOOR = 0x2C5F
SEEDED_CHARACTER_CEILING = 0x0250


def _all_seeded_text(rows):
    for row in rows:
        yield from (row.merchant_id, row.cnpj, row.legal_name, row.status, row.mcc)
        yield from (row.settlement_account, row.risk_tier)
        if row.trade_name is not None:
            yield row.trade_name


# --------------------------------------------------------------------------------
# The pinned pool -- a reader, a caller and a test, which it had none of
# --------------------------------------------------------------------------------


def test_the_committed_pool_loads_through_the_generators_own_validator():
    """`validated_pool` is the boundary the generator already validates against."""
    pool = population.read_pool_file()
    assert len(pool) == population.POOL_SIZE == 1024
    assert len(set(pool)) == 1024
    assert list(pool) == sorted(pool)


def test_the_committed_pool_matches_the_published_sha256():
    """The digest `docs/f-db-run-evidence.md` §0.3 published, re-derived from the file.

    Computed over the body exactly as `grep -v '^#' <file> | sha256sum` sees it -- each key
    line followed by its newline -- because that is the command the evidence quotes.
    """
    assert population.pool_body_sha256() == population.POOL_BODY_SHA256


def test_the_obvious_reader_yields_1025_entries_and_validated_pool_refuses_it():
    """The header's stripping rule is only correct for `splitlines()`, and here is why.

    `read_text().split("\\n")` keeps the empty string after the final newline. That is a
    1,025-entry pool whose last entry is `''`, and `validated_pool` refuses it rather than
    passing a silently-wrong pool downstream -- fail-loud, which is the right outcome and
    is the reason this test asserts the refusal instead of the count alone.
    """
    raw = population.POOL_FILE.read_text(encoding="utf-8").split("\n")
    naive = [line for line in raw if not line.startswith(population.COMMENT_PREFIX)]
    assert len(naive) == 1025
    with pytest.raises(ValueError, match="is empty"):
        validated_pool(naive)


def test_142_of_the_pinned_keys_carry_a_leading_zero():
    """The measurement that decides `merchant.cnpj` is `text` and never a number."""
    pool = population.read_pool_file()
    assert sum(1 for key in pool if key.startswith("0")) == 142


# --------------------------------------------------------------------------------
# The population, and the arithmetic that has to close
# --------------------------------------------------------------------------------


def test_snapshot_1_is_1088_merchants_over_1024_cnpjs():
    """1,024 roots, 64 of them with a second establishment: the link is not degenerate."""
    plan = population.build_plan()
    assert len(plan.seed) == population.SNAPSHOT_1_ROWS == 1088
    assert len({row.merchant_id for row in plan.seed}) == 1088
    roots = [row.cnpj[:8] for row in plan.seed]
    assert len(set(roots)) == 1024
    assert sum(1 for root in set(roots) if roots.count(root) == 2) == 64


def test_the_published_class_counts_are_what_the_plan_builds():
    plan = population.build_plan()
    assert {klass.name: len(plan.rows_for(klass.name)) for klass in population.CHANGE_CLASSES} == {
        "out_of_order_commit": 8,
        "watermark_advance": 8,
        "insert": 32,
        "update_moving_updated_at": 48,
        "update_not_moving_updated_at": 24,
        "hard_delete": 16,
    }


def test_every_class_acts_on_a_disjoint_slice():
    """Disjointness by construction, not by inspection.

    Two classes sharing a row would double-count it in the headline, and the out-of-order
    class would additionally deadlock against whichever other class touched its rows while
    its transaction is held open.
    """
    plan = population.build_plan()
    updated_or_deleted = [
        row.merchant_id
        for klass in population.CHANGE_CLASSES
        if klass.presence is not population.Presence.INSERT
        for row in plan.rows_for(klass.name)
    ]
    assert len(updated_or_deleted) == len(set(updated_or_deleted)) == 104
    seeded = {row.merchant_id for row in plan.seed}
    assert set(updated_or_deleted) <= seeded
    assert not {row.merchant_id for row in plan.rows_for("insert")} & seeded


def test_snapshot_2_and_the_watermark_miss_both_close():
    """1088 + 32 - 16 = 1104, and 16 + 24 + 8 = 48."""
    inserted = population.CLASSES_BY_NAME["insert"].count
    deleted = population.CLASSES_BY_NAME["hard_delete"].count
    assert population.SNAPSHOT_1_ROWS + inserted - deleted == population.SNAPSHOT_2_ROWS == 1104
    missed = sum(
        klass.count
        for klass in population.CHANGE_CLASSES
        if klass.presence is population.Presence.DELETE
        or (klass.payload_changed and (klass.held_open or not klass.moves_updated_at))
    )
    assert missed == population.WATERMARK_MISS == 48


def test_the_watermark_advance_class_is_the_one_the_arithmetic_needs():
    """The class the plan's published table omits, kept honest by an assertion.

    The out-of-order miss requires `t1 < watermark_1`, and `watermark_1` is `max(updated_at)`
    over what snapshot 1 can see. Every seeded row predates t1, so without a write that
    commits BETWEEN t1 and snapshot 1 the watermark would be t1 itself and the miss would be
    a fabrication. That write must change no other column, or it would show up in the payload
    diff as a class nobody published.
    """
    advance = population.CLASSES_BY_NAME["watermark_advance"]
    assert advance.moves_updated_at and not advance.payload_changed
    assert advance.before_snapshot_1 and not advance.held_open
    assert advance.count == population.CLASSES_BY_NAME["out_of_order_commit"].count


def test_exactly_one_class_is_held_open_and_no_class_claims_both_orderings():
    """The ordering is DERIVED from these booleans, so an empty phase is a broken headline.

    `seed_merchant_db._phases` reads them rather than naming classes, which is what keeps
    the run order from drifting out of step with the published table.
    """
    assert [k.name for k in population.CHANGE_CLASSES if k.held_open] == ["out_of_order_commit"]
    assert [k.name for k in population.CHANGE_CLASSES if k.before_snapshot_1] == [
        "watermark_advance"
    ]
    with pytest.raises(ValueError, match="cannot both be held open"):
        population.ChangeClass(
            "impossible", 1, population.Presence.UPDATE, True, True, True, before_snapshot_1=True
        )


# --------------------------------------------------------------------------------
# Determinism
# --------------------------------------------------------------------------------


def test_the_same_pool_builds_the_same_rows_twice():
    """Same seed in, same rows out -- no `random()`, no `uuid4`, no `now()`."""
    assert population.build_plan() == population.build_plan()


def test_merchant_id_is_a_pure_function_of_the_cnpj():
    row = population.merchant("12345678", 1)
    assert row == population.merchant("12345678", 1)
    assert row.merchant_id != population.merchant("12345678", 2).merchant_id


def test_the_seeded_updated_at_window_is_literal_and_closed():
    """Every seeded stamp predates the mutation, so `mutate` can refuse a dirty table."""
    plan = population.build_plan()
    stamps = [row.updated_at for row in plan.seed]
    assert min(stamps) >= population.SEED_UPDATED_FLOOR
    assert max(stamps) < population.SEED_UPDATED_CEILING
    assert all(stamp.tzinfo is not None for stamp in stamps)


def test_the_check_digits_are_the_real_cnpj_algorithm():
    """Against a CNPJ whose digits are documented, not against this module's own output."""
    assert population.check_digits("112223330001") == "81"
    assert population.full_cnpj("11222333", 1) == "11222333000181"


# --------------------------------------------------------------------------------
# The column contract the schema and the vault depend on
# --------------------------------------------------------------------------------


def test_onboarded_on_is_always_present():
    """T5: a NULL entry date sorts FIRST in Spark and beats a delivered date."""
    plan = population.build_plan()
    assert all(isinstance(row.onboarded_on, date) for row in plan.seed)


def test_credit_limit_is_always_at_the_declared_scale():
    """`numeric(14,2)`, and the ceiling is exercised by a derivation rather than a comment."""
    plan = population.build_plan()
    assert all(row.credit_limit == row.credit_limit.quantize(Decimal("0.01")) for row in plan.seed)
    assert all(row.credit_limit <= population.MAX_CREDIT_LIMIT for row in plan.seed)
    assert sum(1 for row in plan.seed if row.credit_limit == population.MAX_CREDIT_LIMIT) > 0


def test_trade_name_carries_both_a_null_and_an_empty_string():
    """Plan §4: "nullable on purpose: a NULL that is not ''".

    A column that is only ever NULL, or only ever populated, cannot demonstrate that the
    landing path keeps the two distinct -- which is the only reason it is nullable.
    """
    plan = population.build_plan()
    assert sum(1 for row in plan.seed if row.trade_name is None) > 0
    assert sum(1 for row in plan.seed if row.trade_name == "") > 0


def test_no_seeded_character_can_reach_the_two_hash_spellings_divergence():
    """T10, as an assertion rather than as a promise not to.

    Seeding one of the forty characters JDK 17 and CPython 3.12 upper-case differently
    would produce a vault whose Python and Spark digests disagree on real data with NO test
    going red, because the loaders only ever use the Spark spelling. The bound below is far
    stricter than the set and needs no second spelling of it.
    """
    plan = population.build_plan()
    highest = max(ord(character) for text in _all_seeded_text(plan.seed) for character in text)
    assert highest < SEEDED_CHARACTER_CEILING < DIVERGENCE_FLOOR


# --------------------------------------------------------------------------------
# The one payload derivation
# --------------------------------------------------------------------------------


def test_every_update_class_runs_the_same_derivation_and_it_always_changes_the_row():
    """T2: an implementer who writes a branch per change class has produced the tautology.

    `mutated` is the whole payload difference between the update classes; what separates
    them is which transaction runs it and whether the trigger is armed. An update that
    changed nothing would be a class the snapshot diff cannot see, silently shrinking
    whichever count it belonged to -- so `status` steps a three-value cycle and the
    inequality holds for every row without a guard.
    """
    plan = population.build_plan()
    assert all(population.mutated(row) != row for row in plan.seed)
    assert all(population.mutated(row).status != row.status for row in plan.seed)


def test_the_mutation_never_touches_a_key_or_the_entry_date():
    """`merchant_id` keys the hub, `cnpj` keys the link, `onboarded_on` opens the window."""
    plan = population.build_plan()
    for row in plan.seed[:64]:
        after = population.mutated(row)
        assert (after.merchant_id, after.cnpj, after.onboarded_on) == (
            row.merchant_id,
            row.cnpj,
            row.onboarded_on,
        )


def test_the_mutation_reaches_every_trade_name_transition():
    """NULL -> name, name -> NULL and name -> '' all occur, none of them written down."""
    plan = population.build_plan()
    pairs = {(row.trade_name is None, population.mutated(row).trade_name is None)
             for row in plan.seed}
    assert (True, False) in pairs and (False, True) in pairs
    assert any(population.mutated(row).trade_name == "" for row in plan.seed)


def test_the_mutation_is_deterministic_and_stamps_nothing():
    """No `now()` reaches a mutated payload: the trigger owns `updated_at`, not this."""
    plan = population.build_plan()
    row = plan.seed[0]
    assert population.mutated(row) == population.mutated(row)
    assert population.mutated(row).updated_at == row.updated_at
    assert isinstance(row.updated_at, datetime)
