"""The merchant population and its mutation, derived once and executed nowhere.

WHY THIS IS NOT INSIDE `seed_merchant_db.py`. The numbers this phase's headline rests on
-- 1,088 seeded merchants, 32 inserted, 48 updated visibly, 24 updated silently, 16
deleted, 8 committed out of order -- are ARITHMETIC, and arithmetic that only a database
can check is arithmetic nobody checks. This module imports no psycopg and touches no
socket, so `tests/test_merchant_population.py` asserts every count, every disjointness and
every determinism claim with `docker compose` stopped. That is the same discipline plan
Task 4 hands `postgres_source.py` ("no I/O, the connection injected"), one task early.

THE CLASSES ARE A PROJECTION, NOT SIX CODE PATHS. T2 rules that an implementer who writes
a branch per change class has produced the tautology. So a class here is a row in
`CHANGE_CLASSES` carrying four independent booleans -- does the write arm the trigger, is
it held open across snapshot 1's read, does it commit before that read, does it change the
payload -- plus one presence verb. `seed_merchant_db._phases` reads those booleans to
order the run, so no code anywhere names a class to decide what to do with it.
`mutated()` is the ONE payload derivation and every UPDATE class runs exactly it; the
seeder turns a class into SQL with three statements (one INSERT, one UPDATE, one DELETE)
for all six. Adding a seventh class is a row in a table, not a branch.

WHAT IS CHOSEN AND WHAT IS NOT, stated here because the code is where it stops being
prose. The counts below are AUTHORED -- every one of them, including the 8. What is not
authored is that a row stamped `t1`, held open across the extract and committed after it,
is unreachable by `WHERE updated_at > watermark` forever. That is MVCC's, and the
`watermark_advance` class exists because the arithmetic does not close without it (see
`CHANGE_CLASSES`).

NEVER NUMERIC, ANYWHERE. 142 of the 1,024 pinned roots carry a leading zero. Every key in
this module is a `str` from end to end and there is no `int()` round trip on any of them.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from uuid import UUID, uuid5

from opl.generator.cnpj_pool import CNPJ_BASICO_WIDTH, validated_pool

# --------------------------------------------------------------------------------
# The pinned pool
# --------------------------------------------------------------------------------

POOL_FILE = Path(__file__).with_name("merchant_cnpj_pool.txt")
POOL_SIZE = 1024

# `grep -v '^#' scripts/merchant_cnpj_pool.txt | sha256sum`, published in
# `docs/f-db-run-evidence.md` §0.3. Asserted against the committed file by the tests --
# until this module existed the file had no reader, no caller and no test at all.
POOL_BODY_SHA256 = "82e6a447c28befd565eaedf0556bba1752da7b3ba7bdc8b87474cf2eba8aff18"

COMMENT_PREFIX = "#"


def pool_body(path: Path = POOL_FILE) -> str:
    """The file's key body exactly as `grep -v '^#' <file>` writes it to a pipe.

    `splitlines()`, not `split("\\n")`. The obvious reader yields 1,025 entries -- the
    empty string after the final newline -- and `validated_pool` refuses it, which is the
    right outcome and also proves the header's stripping rule is only correct for this
    spelling.
    """
    keys = [
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if not line.startswith(COMMENT_PREFIX)
    ]
    return "".join(f"{key}\n" for key in keys)


def read_pool_file(path: Path = POOL_FILE) -> tuple[str, ...]:
    """The committed pool, through the generator's own validator."""
    keys = pool_body(path).splitlines()
    return validated_pool(keys)


def pool_body_sha256(path: Path = POOL_FILE) -> str:
    return hashlib.sha256(pool_body(path).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------------
# One merchant
# --------------------------------------------------------------------------------

# A fixed v5 namespace, so `merchant_id` is a pure function of the CNPJ and a re-seed
# after a `docker compose down -v` produces byte-identical keys. No `uuid4` anywhere.
NAMESPACE = uuid5(UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8"), "opl/f-db/merchant")

STATUSES = ("active", "suspended", "closed")
RISK_TIERS = ("low", "medium", "high")

# Real MCCs, and `0742` is here on purpose: a four-character code with a leading zero,
# in a column that is `text` for the same reason `cnpj` is.
MCCS = ("0742", "4111", "5411", "5661", "5812", "5999", "7372", "8011")

# Brazilian bank codes, three characters, leading zeros load-bearing.
BANKS = ("001", "033", "077", "104", "237", "260", "336", "341")

# Portuguese, accented, and DELIBERATELY BOUNDED. T10 refuses seeding any of the forty
# characters JDK 17 and CPython 3.12 upper-case differently -- doing so would produce a
# vault whose two hash spellings disagree on real data with no test going red. Every
# character used below is under U+0250; the divergence set starts at U+2C5F. The bound is
# asserted by the tests rather than eyeballed.
PREFIXES = ("COMÉRCIO", "INDÚSTRIA", "DISTRIBUIDORA", "SERVIÇOS",
            "ATACADO", "LOGÍSTICA", "PARTICIPAÇÕES", "TECNOLOGIA")
CORES = ("ATLÂNTICO", "PLANALTO", "GUARANI", "IPIRANGA",
         "MARAJÓ", "SERTÃO", "PARAÍBA", "TIJUCA",
         "ARARIBÓIA", "CAMBUCI", "BOTAFOGO", "PINHEIROS",
         "MARACANÃ", "ITAPOÃ", "JAÇANÃ", "URUÇUCA")
SUFFIXES = ("LTDA", "S.A.", "EIRELI", "ME")
TRADE_WORDS = ("Loja", "Ponto", "Casa", "Empório", "Armazém", "Quiosque", "Mercearia", "Depósito")

# `numeric(14,2)`'s ceiling, seeded by a derivation rather than by a special case so the
# declared precision is exercised by roughly four rows instead of by a comment.
MAX_CREDIT_LIMIT = Decimal("999999999999.99")
CREDIT_BUMP = Decimal("1500.00")

ONBOARDED_FLOOR = date(2015, 1, 1)
ONBOARDED_SPAN_DAYS = 4018  # through 2025-12-31; every entry date predates the phase.

# The seeded `updated_at` window, as literals. `now()` is never a seeded value: a seed
# whose timestamps move cannot have a stable digest, and the digest is how a re-run is
# verified identical. Every seeded row therefore predates any mutation by construction,
# which `seed_merchant_db.py` re-checks against the server clock rather than assuming.
SEED_UPDATED_FLOOR = datetime(2026, 7, 1, tzinfo=UTC)
SEED_UPDATED_CEILING = datetime(2026, 8, 1, tzinfo=UTC)
_SEED_SPAN_SECONDS = int((SEED_UPDATED_CEILING - SEED_UPDATED_FLOOR).total_seconds())

_DV1_WEIGHTS = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
_DV2_WEIGHTS = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)


@dataclass(frozen=True)
class Merchant:
    """One row of `merchant`, in the plan §4 column order. Immutable; `mutated` copies."""

    merchant_id: str
    cnpj: str
    legal_name: str
    trade_name: str | None
    status: str
    mcc: str
    settlement_account: str
    risk_tier: str
    credit_limit: Decimal
    onboarded_on: date
    updated_at: datetime


COLUMNS = tuple(field for field in Merchant.__dataclass_fields__)


def _digest(*parts: str) -> bytes:
    """64 deterministic bytes for `parts`, which is the only entropy in this module."""
    return hashlib.sha512("|".join(parts).encode("utf-8")).digest()


def _pick(choices: Sequence[str], digest: bytes, offset: int) -> str:
    return choices[digest[offset] % len(choices)]


def _number(digest: bytes, start: int, stop: int, modulus: int) -> int:
    return int.from_bytes(digest[start:stop], "big") % modulus


def check_digits(base12: str) -> str:
    """The two real CNPJ verification digits for a 12-character base.

    Computed rather than invented. These establishments do not exist, so the digits are
    fabricated either way -- but a fabricated-and-internally-inconsistent CNPJ is one a
    later reader can prove wrong, and this column exists to be joined and validated.
    """
    digits = [int(character) for character in base12]
    for weights in (_DV1_WEIGHTS, _DV2_WEIGHTS):
        rest = sum(d * w for d, w in zip(digits, weights, strict=True)) % 11
        digits.append(0 if rest < 2 else 11 - rest)
    return f"{digits[12]}{digits[13]}"


def full_cnpj(root: str, ordem: int) -> str:
    """`root` plus a four-digit establishment ordinal plus its check digits, as text."""
    if len(root) != CNPJ_BASICO_WIDTH:
        raise ValueError(f"root {root!r} is not {CNPJ_BASICO_WIDTH} characters")
    base = f"{root}{ordem:04d}"
    return f"{base}{check_digits(base)}"


def _trade_name(digest: bytes) -> str | None:
    """NULL, empty, or a name -- and the first two are DIFFERENT on purpose (plan §4).

    A column that is only ever NULL or only ever populated cannot demonstrate that the
    landing path keeps a NULL distinct from an empty string, which is the one thing this
    column is nullable for.
    """
    if digest[29] % 8 == 0:
        return None
    if digest[29] % 32 == 3:
        return ""
    return f"{_pick(TRADE_WORDS, digest, 30)} {_pick(CORES, digest, 31).title()}"


def _credit_limit(digest: bytes) -> Decimal:
    if digest[13] == 0:
        return MAX_CREDIT_LIMIT
    return (Decimal(100_000 + _number(digest, 14, 18, 9_900_000)) / 100).quantize(Decimal("0.01"))


def merchant(root: str, ordem: int) -> Merchant:
    """The merchant at establishment `ordem` of `root`, as a pure function of both."""
    cnpj = full_cnpj(root, ordem)
    d = _digest(cnpj)
    return Merchant(
        merchant_id=str(uuid5(NAMESPACE, cnpj)),
        cnpj=cnpj,
        legal_name=f"{_pick(PREFIXES, d, 0)} {_pick(CORES, d, 1)} {_pick(SUFFIXES, d, 2)}",
        trade_name=_trade_name(d),
        status=_pick(STATUSES, d, 10),
        mcc=_pick(MCCS, d, 11),
        settlement_account=(
            f"{_pick(BANKS, d, 3)}-{_number(d, 4, 6, 10_000):04d}-{_number(d, 6, 10, 10**8):08d}"
        ),
        risk_tier=_pick(RISK_TIERS, d, 12),
        credit_limit=_credit_limit(d),
        onboarded_on=ONBOARDED_FLOOR + timedelta(days=_number(d, 18, 22, ONBOARDED_SPAN_DAYS)),
        updated_at=SEED_UPDATED_FLOOR
        + timedelta(seconds=_number(d, 22, 26, _SEED_SPAN_SECONDS),
                    microseconds=_number(d, 26, 29, 1_000_000)),
    )


def _next(cycle: Sequence[str], value: str) -> str:
    return cycle[(cycle.index(value) + 1) % len(cycle)]


def mutated(row: Merchant) -> Merchant:
    """THE ONE PAYLOAD DERIVATION. Every UPDATE class in `CHANGE_CLASSES` runs this.

    `status` steps through a three-value cycle, so `mutated(row) != row` holds for every
    row without a guard -- an update that changed nothing would be a class the diff cannot
    see, silently shrinking whichever count it belonged to.

    `credit_limit` is clamped at `numeric(14,2)`'s ceiling rather than allowed to overflow
    the declared precision, because the rows seeded AT that ceiling are deliberate.
    `trade_name` is re-derived through `_trade_name` under a mutation salt, so NULL -> name,
    name -> NULL and name -> '' all occur without any of them being written down.
    """
    d = _digest("f-db/mutation/20260815", row.merchant_id)
    return replace(
        row,
        status=_next(STATUSES, row.status),
        risk_tier=_next(RISK_TIERS, row.risk_tier),
        credit_limit=min(row.credit_limit + CREDIT_BUMP, MAX_CREDIT_LIMIT),
        trade_name=_trade_name(d),
    )


# --------------------------------------------------------------------------------
# The population, and the six classes that are a projection of three booleans
# --------------------------------------------------------------------------------

# Every 16th pinned root carries a second establishment: 1,024 + 64 = 1,088 merchants over
# 1,024 CNPJs, so `link_merchant_empresa` is not degenerate.
SECOND_ESTABLISHMENT_STRIDE = 16
# The inserted merchants take establishment 2 of roots that have only establishment 1.
# `8 mod 16 != 0`, so the two sets are disjoint by construction and not by inspection.
INSERT_STRIDE = 32
INSERT_PHASE = 8

SNAPSHOT_1_ROWS = 1088
SNAPSHOT_2_ROWS = 1104


class Presence(Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


@dataclass(frozen=True)
class ChangeClass:
    """One row of the population table, as three booleans and a verb."""

    name: str
    count: int
    presence: Presence
    #: does the write let the BEFORE UPDATE trigger stamp `updated_at`?
    moves_updated_at: bool
    #: is the write stamped BEFORE snapshot 1 and committed AFTER it?
    held_open: bool
    #: does the write change any column other than `updated_at`?
    payload_changed: bool
    #: is the write stamped AND committed before snapshot 1 is read?
    before_snapshot_1: bool = False

    def __post_init__(self) -> None:
        if self.held_open and self.before_snapshot_1:
            raise ValueError(f"{self.name} cannot both be held open and commit before snapshot 1")


CHANGE_CLASSES = (
    # THE HEADLINE. Stamped t1, held open across snapshot 1's read, committed after it.
    # `updated_at` orders by transaction START and visibility orders by COMMIT, so
    # `WHERE updated_at > watermark` never returns these rows and never will.
    ChangeClass("out_of_order_commit", 8, Presence.UPDATE, True, True, True),
    # NOT IN THE PLAN'S PUBLISHED TABLE, AND THE ARITHMETIC DOES NOT CLOSE WITHOUT IT.
    # The out-of-order miss needs `t1 < watermark_1`, and `watermark_1` is `max(updated_at)`
    # over the rows snapshot 1 can see. Every seeded row predates t1, so unless some write
    # commits between t1 and snapshot 1 the watermark IS t1 and `> watermark` misses nothing
    # -- the class would be a fabrication. This is that write: a touch that fires the trigger
    # and changes no other column, so it moves the watermark and is invisible to the payload
    # diff (it commits before snapshot 1 and never changes again). Reported as a finding.
    ChangeClass("watermark_advance", 8, Presence.UPDATE, True, False, False,
                before_snapshot_1=True),
    ChangeClass("insert", 32, Presence.INSERT, True, False, True),
    ChangeClass("update_moving_updated_at", 48, Presence.UPDATE, True, False, True),
    # The default-shaped trap: a write path with no trigger armed. `DEFAULT now()` is an
    # INSERT-time default and does not fire on UPDATE (measured, evidence §0.4).
    ChangeClass("update_not_moving_updated_at", 24, Presence.UPDATE, False, False, True),
    ChangeClass("hard_delete", 16, Presence.DELETE, False, False, False),
)

CLASSES_BY_NAME = {klass.name: klass for klass in CHANGE_CLASSES}

# 16 deletes + 24 silent updates + 8 out-of-order commits. The inserts and the visible
# updates DO clear the watermark, and the watermark-advancing touch commits before it.
WATERMARK_MISS = 48


@dataclass(frozen=True)
class Plan:
    """The seeded rows and the disjoint slice each change class acts on."""

    seed: tuple[Merchant, ...]
    by_class: dict[str, tuple[Merchant, ...]]

    def rows_for(self, name: str) -> tuple[Merchant, ...]:
        return self.by_class[name]


def _seed_rows(roots: Sequence[str]) -> tuple[Merchant, ...]:
    first = [merchant(root, 1) for root in roots]
    second = [
        merchant(root, 2)
        for index, root in enumerate(roots)
        if index % SECOND_ESTABLISHMENT_STRIDE == 0
    ]
    return tuple(sorted(first + second, key=lambda row: row.merchant_id))


def _inserted_rows(roots: Sequence[str]) -> tuple[Merchant, ...]:
    rows = [
        merchant(root, 2)
        for index, root in enumerate(roots)
        if index % INSERT_STRIDE == INSERT_PHASE
    ]
    return tuple(sorted(rows, key=lambda row: row.merchant_id))


def build_plan(roots: Sequence[str] | None = None) -> Plan:
    """The whole population and its slices, from the pinned pool.

    The slices are cut off the head of the merchant-id ordering, which is a UUIDv5 digest
    and therefore uncorrelated with every seeded attribute -- so no class is quietly
    biased toward a status, a tier or a CNPJ prefix. Disjointness is by construction.
    """
    keys = read_pool_file() if roots is None else validated_pool(roots)
    seed = _seed_rows(keys)
    by_class: dict[str, tuple[Merchant, ...]] = {}
    offset = 0
    for klass in CHANGE_CLASSES:
        if klass.presence is Presence.INSERT:
            by_class[klass.name] = _inserted_rows(keys)
            continue
        by_class[klass.name] = seed[offset : offset + klass.count]
        offset += klass.count
    return Plan(seed=seed, by_class=by_class)
