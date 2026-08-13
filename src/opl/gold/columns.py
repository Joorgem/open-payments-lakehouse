# src/opl/gold/columns.py
"""The columns every dimension in this gold layer carries, the ghost row's identity,
and the two interval sentinels that make an as-of lookup total.

PURE: NOTHING HERE IMPORTS PYSPARK, for `opl.vault.columns`' reason -- the registry's
per-table guards read `DIMENSION_COLUMNS`, and a spec must be declarable and refusable
without a session. The two names it re-exports come from `opl.vault.columns` rather
than being restated, because `load_date` and `record_source` mean in gold exactly what
they mean in the vault and one lakehouse should not have two spellings of LDTS.

THE INTERVAL IS HALF-OPEN: `valid_from <= t < valid_to`, AND `BETWEEN` IS FORBIDDEN.
`BETWEEN` is inclusive at BOTH ends, so an event at exactly a version boundary matches
the version that closed there AND the version that opened there -- a multi-match that
the star's own acceptance forbids, manufactured by the operator that looks most natural.
Half-open makes every instant belong to exactly one version, and it is why
`valid_to` is the NEXT version's `valid_from` rather than "a moment before" it: there is
no moment before, at any precision, that does not leave a gap the width of that
precision.

BOTH ENDS ARE FLOORED, AND NEITHER IS NULL. A NULL `valid_to` on the open version is the
commoner spelling and it is refused here twice over: it makes every as-of predicate a
three-way `OR valid_to IS NULL`, which a reader forgets exactly once, and NULL compares
false in the join so the omission LOSES rows silently rather than failing. The low
sentinel is the same argument at the other end -- with only the top floored, a payment
dated before the earliest snapshot resolves to no version of a perfectly well-known
company, and the star answers "unknown" about a row it plainly describes.

WHAT THE LOW SENTINEL DOES NOT CLAIM, stated because the obvious reading is wrong.
`valid_from = 1970-01-01` on a company's FIRST version does not assert that the company
existed then, or that its razão social was that. It is a LOOKUP CONVENTION: "for any
as-of time up to the next version, this is the state this dimension can offer". The
vault cannot support anything stronger -- an RFB snapshot is a state, not a birth
certificate, and `sat_empresa_dados` records when we OBSERVED a value, never when it
started being true.

IT IS UNCONDITIONAL, AND THE ALTERNATIVE WAS WEIGHED AND REJECTED FOR A CONCRETE
REASON. The tempting refinement is to floor only the versions whose `applied_date` is
the EARLIEST in the source, so that a company first observed in July starts on
2026-07-11 and a payment before it reaches the ghost -- which is more honest about our
ignorance, and would exercise the ghost with real data instead of leaving it
structurally unexercised. It is rejected because it makes `valid_from` depend on a
GLOBAL aggregate over the source: the day a phase backfills an earlier snapshot, every
first version's `valid_from` moves, and with it every `company_sk` derived from it --
silently re-keying a dimension that facts already reference. The unconditional floor is
stable under a backfill; the conditional one is not, and a surrogate key that changes
under a load nobody thought was destructive is the worse failure by a wide margin.

THE SENTINEL VALUES ARE CHOSEN AGAINST A MEASURED PLATFORM LIMIT, NOT BY CONVENTION.
Kimball's usual pair is 1900-01-01 and 9999-12-31, and BOTH are unusable in this
project's test environment: pyspark converts a timestamp back to Python through the C
runtime, and on this Windows dev box a value outside roughly the epoch..3000 range
cannot be read back at all. Measured, session timezone America/Sao_Paulo, pyspark
3.5.9:

    '1900-01-01 00:00:00'          collect -> OSError [Errno 22] Invalid argument
    '1970-01-01 00:00:00'          collect -> datetime(1970, 1, 1, 0, 0)
    '2999-12-31 23:59:59.999999'   collect -> datetime(2999, 12, 31, 23, 59, 59, 999999)
    '9999-12-31 23:59:59.999999'   collect -> OSError [Errno 22] Invalid argument

In-engine comparison works for all four, so a 9999 sentinel would be *writable* and
*joinable* and would fail only when a test tried to read a row -- i.e. it would make
every readable assertion about this dimension impossible on the machine the suite runs
on, for a value that is arbitrary either way. The epoch is the lowest instant this
stack can round-trip and is 53 years below the RFB's own open-data series (2023-05);
2999-12-31 is 973 years above anything this star can be asked about.

THE ONE CAVEAT, RECORDED RATHER THAN GLOSSED: the epoch floor is written as a LOCAL
instant, so on a Windows box east of Greenwich `1970-01-01 00:00:00` local is a
negative epoch value and would hit the same OSError. Linux (CI) handles negatives
fine. If that machine ever appears, the fix is to raise the floor, not to lower it
below the epoch.

THE SENTINELS ARE `datetime` VALUES AND ARE NEVER PASSED TO `F.lit` DIRECTLY.
`pyspark.sql.types.TimestampType.toInternal` converts through `time.mktime`, which
raises `OverflowError: mktime argument out of range` on this box for anything past year
3000 -- so a literal is built by ISO-formatting the value and casting the STRING, which
Spark parses in the session timezone with no Python conversion involved.
`opl.gold.dimensions.instant_literal` is the one place that happens."""
from __future__ import annotations

from datetime import datetime

from opl.vault.columns import LOAD_DATE, RECORD_SOURCE

# Re-exported so a gold module imports every column name it writes from ONE place, and
# so the two that come from `opl.vault.columns` are visibly the same values that module
# defines rather than restated strings. `ruff` would flag them unused otherwise.
__all__ = [
    "DIMENSION_COLUMNS",
    "GHOST_RECORD_SOURCE",
    "GHOST_SURROGATE_KEY",
    "IS_CURRENT",
    "LOAD_DATE",
    "RECORD_SOURCE",
    "VALID_FROM",
    "VALID_FROM_FLOOR",
    "VALID_TO",
    "VALID_TO_CEILING",
]

# When this version of the row STARTS being the answer, inclusive.
VALID_FROM = "valid_from"

# When it STOPS being the answer, EXCLUSIVE -- and it equals the next version's
# `valid_from` exactly, so the chain has no gap and no overlap at any precision.
VALID_TO = "valid_to"

# Whether this row is the version in force now. DERIVED, and derivable: it is
# `valid_to == VALID_TO_CEILING` and nothing else. Carried because the two questions a
# star gets asked are "as of when?" and "right now", and the second one should not
# require the reader to know the sentinel's value; refused as redundant by nobody,
# because a denormalisation that can disagree with its source is pinned by a test that
# compares the two on every row.
IS_CURRENT = "is_current"

# The interval sentinels. See the module docstring for why these two values and not
# 1900-01-01 / 9999-12-31, which is the convention they replace.
VALID_FROM_FLOOR = datetime(1970, 1, 1, 0, 0, 0)
VALID_TO_CEILING = datetime(2999, 12, 31, 23, 59, 59, 999999)

# THE GHOST ROW'S SURROGATE KEY, and it is negative for a reason beyond convention: no
# derived key can equal it, because every real key comes out of a hash and this one is
# reserved by being outside the population the fact resolves. A fact row that failed to
# resolve reaches it as `COALESCE(<as-of lookup>, GHOST_SURROGATE_KEY)` at build time --
# never by joining to it, which it cannot do, because the ghost carries no business key.
#
# NOT `'00000000'`, AND THAT IS THE POINT OF SPELLING IT HERE. Keying the unknown member
# on an all-zeros CNPJ básico is the obvious choice and it is wrong on this data:
# `docs/f1b-run-evidence.md` section 2.4 records `00000000` as `hub_empresa`'s REAL
# lowest key. A ghost keyed there would silently merge every unresolved payment onto a
# real company, with the join working and the row counts right.
GHOST_SURROGATE_KEY = -1

# The ghost's RSRC. A LITERAL NAMING THIS LOADER rather than NULL, because the row is
# manufactured and a triager reading it should be told so by the row itself; and because
# `record_source` is OURS, where the four payload columns are the RFB's delivered facts.
# The payload columns on the ghost are NULL for exactly that reason -- writing
# "(unknown)" into a razão social would put a value we invented in a column whose whole
# contract is that it carries a value somebody else delivered. Same distinction
# `opl.vault.columns` draws between `data_entrada_sociedade` and `last_observed_on`.
GHOST_RECORD_SOURCE = "opl.gold.dimensions:ghost"

# Refused as a surrogate key, a business key or a payload column by the registry's
# guards. A frozen set rather than a list so no caller can extend it in place.
#
# WIDER THAN `opl.vault.columns.METADATA_COLUMNS` AND THAT GAP IS THE GUARD'S REASON TO
# EXIST: the vault refuses a payload column called `load_date`, and knows nothing about
# `valid_from`. A satellite payload column of that name is perfectly legal in the vault
# and would be overwritten here without anything failing.
DIMENSION_COLUMNS = frozenset({VALID_FROM, VALID_TO, IS_CURRENT, LOAD_DATE, RECORD_SOURCE})
