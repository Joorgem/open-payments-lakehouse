# src/opl/bronze/snapshot.py
"""Which monthly snapshot a bronze row came from -- two columns, deliberately.

`_snapshot_month` is the OPERATIONAL identity: the month parameter the job ran
with. Always known, because it is an input rather than a derivation.

`_snapshot_ref_date` is the BUSINESS fact the vault's `applied_date` needs: the
date the RFB itself declares in the inner filename. The snapshot is NOT month-end
-- 2026-06 files carry `D60613` (the 13th) and 2026-07 carries `D60711`.

They can legitimately disagree (a June file left in the July folder), and storing
both is what makes that visible instead of quietly resolved by picking one.

WHY THIS IS DERIVED IN BRONZE and not in silver: bronze already derives a column
from a filename (`lookup_type`), and bronze's job is to record what the source
asserted at ingestion. Making silver re-parse the path would couple the vault to
the landing layout -- the layout F1.4a is changing right now, which is the proof
that the coupling is a bad bet.

Spark `Column` expressions only, with no pure-Python twin: a second
implementation would be a second thing to drift, and a UDF would break Catalyst
over 144M rows.
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from opl.config import is_month

SNAPSHOT_MONTH_COLUMN = "_snapshot_month"
SNAPSHOT_REF_DATE_COLUMN = "_snapshot_ref_date"

# `.D` then a single year digit, two month digits, two day digits, then `.`
# Anchored on the dots so it cannot match digits elsewhere in a mainframe name
# like `K3241.K03200Y0.D60613.ESTABELE`. The trailing dot is a LOOKAHEAD, not a
# consumed character, so two tokens sharing one dot still count as two -- which
# is what lets the ambiguity check below see them.
_TOKEN_CORE = r"\.D(\d)(\d{2})(\d{2})(?=\.)"

# The last path segment: the filename.
_LAST_SEGMENT = r"[^/]*$"

# Extraction is confined to the filename by composing the two. Without
# `_LAST_SEGMENT`, `regexp_extract` takes the leftmost match anywhere in the
# PATH, so a landing DIRECTORY whose name carried a token
# (`.../batch.D60601.reprocess/K3241.K03200Y0.D60613.ESTABELE`) would win and
# stamp the 1st instead of the 13th -- a wrong date that cross-checks CLEAN,
# because its month still agrees with the folder. Directory names are exactly
# what F1.4a is rearranging, so this is a guard on live churn, not a
# hypothetical. Probed, not assumed: deleting `+ _LAST_SEGMENT` here turns
# `test_a_token_in_a_DIRECTORY_name_does_not_win_over_the_filename` red. The
# first version of that test was VACUOUS -- its directory token was preceded by
# `/` rather than `.`, so `_TOKEN_CORE` never matched it and both regexes agreed.
_TOKEN = _TOKEN_CORE + _LAST_SEGMENT


def _token_parts(source_file: Column) -> tuple[Column, Column, Column, Column]:
    """(tokens found in the filename, year digit, month, day) for one path.

    ONE extraction shared by the two public functions below, so the thing that
    decides which characters of a mainframe name are the RFB's date is spelled
    once. `ref_date_column` needs all four; `token_month_column` needs the first
    three. Splitting the regex between them would let a change to the anchoring
    fix one caller and leave the other reading a different filename.

    `tokens` is the list of matches in the LAST path segment, so its size is what
    both callers test the ambiguity of; the three groups come from `_TOKEN`, whose
    leftmost match is the same one when the size is 1.
    """
    filename = F.regexp_extract(source_file, _LAST_SEGMENT, 0)
    tokens = F.regexp_extract_all(filename, F.lit(_TOKEN_CORE), F.lit(0))
    return (
        tokens,
        F.regexp_extract(source_file, _TOKEN, 1),
        F.regexp_extract(source_file, _TOKEN, 2),
        F.regexp_extract(source_file, _TOKEN, 3),
    )


def token_month_key(snapshot_month: str) -> str:
    """What `token_month_column` yields for a row that BELONGS to `snapshot_month`.

    `'2026-07'` -> `'6-07'`: the single year digit the RFB's filename actually
    carries, then the month. NOT `'2026-07'`, because the filename cannot say
    which decade it means -- `ref_date_column` takes the decade from the job
    parameter for exactly that reason, and a key claiming four year digits would
    assert a fact the token does not carry.

    The key exists so that a caller comparing a row's filename against a requested
    month compares STRINGS produced by one rule, rather than re-deriving
    `year[-1]` at every call site. Raises on a malformed month for the same reason
    `ref_date_column` does: the comparison it feeds would otherwise silently match
    nothing and read as "every file disagrees"."""
    if not is_month(snapshot_month):
        raise ValueError(
            f"snapshot_month must be 'YYYY-MM' naming a real month, got "
            f"{snapshot_month!r} -- this is the job's month parameter; a malformed "
            "one would make every filename look like it disagreed with it"
        )
    year, _, month = snapshot_month.partition("-")
    return f"{year[-1]}-{month}"


def token_month_column(source_file: Column) -> Column:
    """The month the FILENAME itself asserts, spelled as `token_month_key` spells
    it -- or NULL when the filename carries no token, or two.

    WHAT THIS IS FOR, and why it is not just `ref_date_column`'s inner half.
    `ref_date_column` answers "is this row's date PROVABLE against the month I was
    given?" and collapses every disagreement to NULL; it therefore cannot say
    WHICH other month a row claims. Backfilling a legacy table needs that second
    question answered BEFORE the write: a table with no `_snapshot_month` yet is
    exactly the case `refuse_contradicting_month` cannot see, and stamping it with
    a month its filenames contradict sends every reference date to NULL and then
    passes every post-write check, because the check reads the value it just wrote.

    NULL on zero tokens and NULL on two, matching `ref_date_column` exactly: a
    filename with two tokens does not assert one month, and guessing which is the
    RFB's is the thing this module refuses to do anywhere. A caller that needs to
    know how many rows are in that state counts the NULLs; it must not read a NULL
    as agreement."""
    tokens, year, month, _ = _token_parts(source_file)
    return F.when(F.size(tokens) == 1, F.concat_ws("-", year, month))


def ref_date_column(source_file: Column, snapshot_month: str) -> Column:
    """The RFB-declared reference date for a row, or NULL if it cannot be proven.

    The token carries a SINGLE year digit, which is ambiguous across decades
    (2016 and 2026 both end in 6). So the year comes from `snapshot_month` and
    only month/day come from the token -- and the two are then required to agree:
    the token's month must equal the folder's month and its year digit must equal
    the folder's year's last digit. Anything else is NULL.

    The decade stays the job parameter's word, by construction: no evidence in the
    filename can contradict it, so a job run with the wrong decade produces a date
    in that decade. That is not a silent lie -- `_snapshot_month` sits beside it
    carrying the same wrong month, so the operator error is visible in the row
    rather than hidden inside a derivation. See the test that pins this.

    NULL rather than a best guess, because a wrong `applied_date` is worse than a
    missing one: it would order the vault's history incorrectly with nothing to
    show for it. THE OTHER HALF IS BUILT (F1.4b): `rules._unprovable_ref_date`,
    registered as `unprovable_snapshot_ref_date` in every non-lookup contract,
    rejects the rows this function leaves NULL, so a month shipping a different
    filename format now goes red naming the cause instead of producing an all-NULL
    column silently. This docstring booked that debt; rules.py's own docstring for
    the rule records it as paid.

    `snapshot_month` is 'YYYY-MM' naming a real month. Raises ValueError on any
    other shape or an out-of-range month, before Spark builds anything -- a
    malformed month is a job-parameter bug, and the column it would produce (all
    NULL) hides it. `'2026-13'` passes a length-and-digits check and then fails
    exactly that way, which is why the range is checked too.

    THE RULE IS ASKED, NOT RESTATED: `opl.config.is_month` owns it, and this used
    to spell it again as `len(year) == 4 ... and 1 <= int(month) <= 12`. That second
    spelling is why `2026-13` was refused for the two ingest tasks (which reach here)
    and accepted for `unzip_table` and `reclaim_landing` (which do not) until
    `require_month` took the range on. The RAISE stays here rather than delegating to
    `require_month`: the consequence is different -- there a wrong landing path or
    delete boundary, here an all-NULL column that reads exactly like a source whose
    filename format changed -- and a message naming the wrong consequence sends the
    operator to the wrong place. Kept as a boundary check on a public function, not
    as the primary gate: every in-repo caller has already been through
    `require_month`, so in practice this now fires only for a direct caller."""
    if not is_month(snapshot_month):
        raise ValueError(
            f"snapshot_month must be 'YYYY-MM' naming a real month, got "
            f"{snapshot_month!r} -- this is the job's month parameter; a malformed "
            "one would silently produce an all-NULL reference date column"
        )
    # Split only after the guard, so `year[-1]` and the `token_month` comparison below
    # are reading a value already proven to be four digits and a real month.
    year, _, month = snapshot_month.partition("-")

    # Exactly one token in the filename, or NULL. Two of them
    # (`K3241.D60601.K03200Y0.D60613.ESTABELE`) both sit in the last segment and
    # both cross-check clean, so leftmost-wins would silently stamp the 1st. Which
    # one the RFB meant is not knowable from the name, and this module does not
    # guess: both months observed shipped exactly one token, so a second one is a
    # naming shape nobody has seen and NULL is the honest answer.
    #
    # The four come from `_token_parts`, shared with `token_month_column`, so both
    # functions read the same characters of the same filename.
    tokens, token_year, token_month, token_day = _token_parts(source_file)

    # regexp_extract yields "" when nothing matches, so a filename with no token
    # fails both comparisons and lands in the NULL branch without a special case.
    agrees = (
        (F.size(tokens) == 1)
        & (token_year == F.lit(year[-1]))
        & (token_month == F.lit(month))
    )

    candidate = F.concat_ws("-", F.lit(year), token_month, token_day)
    parsed = F.to_date(candidate, "yyyy-MM-dd")
    # Reading the day back OFF the parsed date instead of trusting the parser to
    # refuse an impossible one. A lenient parser would roll 2026-06-31 to 07-01,
    # and `spark.sql.legacy.timeParserPolicy` is a CLUSTER setting, not ours to
    # set -- so this removes the question rather than depending on the answer: any
    # roll lands on a day-of-month that differs from the token's, so the row goes
    # NULL whatever the parser did.
    #
    # HONEST SCOPE, measured on pyspark 3.5.9 (see the LEGACY test): under BOTH
    # policies `to_date` already returns NULL for 2026-06-31, so no input on this
    # version can make this check the deciding one. It is belt-and-braces against
    # a DBR whose parser is lenient, not a guard any test here closes. Kept
    # because it is one comparison and the alternative is a documented dependency
    # on a setting we do not control.
    closes = F.dayofmonth(parsed) == token_day.cast("int")
    return F.when(agrees & closes, parsed)
