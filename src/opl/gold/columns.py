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
project's test environment: pyspark converts a timestamp between Python and the engine
through the C runtime, and on this Windows dev box the writable-and-readable window is
1970-01-01 .. 3000-12-31 and nothing outside it. Measured, pyspark 3.5.9, both
directions and both paths, one value per year:

    year   F.lit(datetime)                     ISO string cast, then collect
    1899   OverflowError (time.mktime)         OSError [Errno 22] (fromtimestamp)
    1900   OverflowError                       OSError [Errno 22]
    1969   OverflowError                       OSError [Errno 22]
    1970   round-trips                         round-trips
    2999   round-trips                         round-trips
    3000   round-trips                         round-trips
    3001   OverflowError                       OSError [Errno 22]
    9999   OverflowError                       OSError [Errno 22]

THE TWO PATHS FAIL AT THE SAME PLACES, WHICH CORRECTS AN EARLIER READING OF THIS TABLE.
The ISO-string cast is not a wider range; it moves the failure from the WRITE (`mktime`,
in the driver) to the READ (`fromtimestamp`, in the driver), and 3000 was never the
boundary at either end -- 3001 is, and 1970 is. In-engine comparison works for every row
above, so a 9999 sentinel would be *writable* and *joinable* and would fail only when a
test tried to read a row: it would make every readable assertion about this dimension
impossible on the machine the suite runs on, for a value that is arbitrary either way.
The epoch is the lowest instant this stack can round-trip and is 53 years below the
RFB's own open-data series (2023-05); 2999-12-31 is 973 years above anything this star
can be asked about.

WHAT THE ISO CAST DOES BUY IS THE ZONE, AND IT IS WHY IT IS STILL THE ONLY WAY THESE TWO
VALUES REACH SPARK. `pyspark.sql.types.TimestampType.toInternal` converts through
`time.mktime`, which reads the DRIVER's operating-system zone; the cast is parsed by
Spark in the SESSION zone, which `opl.config.SESSION_TIMEZONE` pins to UTC. So the floor
is the epoch itself -- 0 micros -- on every machine, where `F.lit(VALID_FROM_FLOOR)`
would be midnight in whatever zone the driver's OS happens to be set to. That is a
NEGATIVE epoch value anywhere east of Greenwich, and the table above shows what the C
runtime does with one. `opl.gold.dimensions.instant_literal` is the one place these
literals are built, and every instant this layer writes goes through it -- `load_date`
included, so that one projection does not mix the two zones."""
from __future__ import annotations

from datetime import datetime

from opl.vault.columns import LOAD_DATE, RECORD_SOURCE

# Re-exported so a gold module imports every column name it writes from ONE place, and
# so the two that come from `opl.vault.columns` are visibly the same values that module
# defines rather than restated strings. `ruff` would flag them unused otherwise.
__all__ = [
    "CONFORMED_RECORD_SOURCE",
    "DIMENSION_COLUMNS",
    "GHOST_RECORD_SOURCE",
    "GHOST_ROWS",
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

# HOW MANY UNKNOWN MEMBERS A GOLD DIMENSION CARRIES: exactly one, in both loaders. Named
# here rather than in either of them because THREE places now reconcile against it -- the
# SCD2 loader's own refusal, the conformed loader's member count, and the line each entry
# point prints -- and `+ 1` written out three times is two edits away from three answers.
GHOST_ROWS = 1

# THE GHOST ROW'S SURROGATE KEY. Negative because that is the convention, and RESERVED BY
# MEASUREMENT rather than by arithmetic -- which is the correction this comment is.
# `xxhash64` returns the full signed 64-bit range, so a versioned row hashing to exactly
# -1 is an ordinary outcome of a hash function and not an impossible one; nothing about
# the value being negative puts it outside the population the fact resolves. What makes
# it safe is that the ghost lives in the SAME table as the versions, so a collision with
# it drops the table's distinct-key count below its row count and
# `opl.gold.dimensions._distinct_surrogate_keys` refuses the load -- one number covering
# a version-to-version collision and a version-onto-ghost collision alike, which is the
# reason that count is taken over ONE column and includes the ghost.
#
# A fact row that failed to resolve reaches it as
# `COALESCE(<as-of lookup>, GHOST_SURROGATE_KEY)` at build time -- never by joining to
# it, which it cannot do, because the ghost carries no business key.
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

# The RSRC every CONFORMED member carries, and it is a different question from the one
# above. `dim_company`'s rows carry `sat_empresa_dados`' own `record_source` -- the RFB
# delivered those values and the dimension is passing them on. A `dim_channel` row
# carries `PIX` because `opl.contracts.payments` DECLARES that rail, and a `dim_date` row
# exists because a span of days was derived here: there is no upstream to name, and
# naming the bronze table the span was measured from would claim the members came from
# it, which is exactly what they do not do (see `opl.gold.specs.EnumeratedDimension` for
# why the members are the contract's domain rather than the observed values).
#
# THE GHOST STILL USES `GHOST_RECORD_SOURCE` ABOVE, ACROSS BOTH LOADERS. Its value names
# `opl.gold.dimensions` because that is where the first ghost was written, and one
# spelling of "this row was manufactured, nobody delivered it" is worth more to a triager
# than a per-module string -- the ghost's meaning does not change with the loader that
# emitted it, and a triager filtering on it wants all of them.
CONFORMED_RECORD_SOURCE = "opl.gold.conformed"

# Refused as a surrogate key, a business key or a payload column by the registry's
# guards. A frozen set rather than a list so no caller can extend it in place.
#
# WIDER THAN `opl.vault.columns.METADATA_COLUMNS` AND THAT GAP IS THE GUARD'S REASON TO
# EXIST: the vault refuses a payload column called `load_date`, and knows nothing about
# `valid_from`. A satellite payload column of that name is perfectly legal in the vault
# and would be overwritten here without anything failing.
DIMENSION_COLUMNS = frozenset({VALID_FROM, VALID_TO, IS_CURRENT, LOAD_DATE, RECORD_SOURCE})
