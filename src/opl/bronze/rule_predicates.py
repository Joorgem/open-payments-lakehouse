# src/opl/bronze/rule_predicates.py
"""WHAT EACH BRONZE DQ RULE TESTS FOR: one predicate factory per defect SHAPE.

`opl.bronze.rules` is the other half and the two answer different questions. This module
answers "what does `bad_cnpj_basico_length` look for, and why that check rather than a
numeric one"; that one answers "which rules does THIS contract run, in what order, and what
does the order buy". Nothing here knows which contract runs which rule, and nothing there
knows what a rule inspects.

SPLIT OUT OF `rules.py` AT 791 OF THIS PROJECT'S 800-LINE FILE CAP, with no behaviour in the
split commit and the collected test count unmoved -- Task 1's discipline, which this
repository has now applied to five files. The seam was chosen so that `rules.py` keeps the
two things a reader consults it for: the per-contract REQUIRED_FIELDS declaration, and the
rule-set summary that `tests/bronze/test_rule_set_prose.py` holds against the sets below it.
That block reads `rules.py`'s comments and only those, so moving it would have moved a guard
along with its subject.

PREDICATES ARE ZERO-ARG FACTORIES (`Callable[[], Column]`) rather than eager `Column`
objects: PySpark cannot build a `Column` without an active `SparkContext`, and `rules_for`
is inspected -- names, unknown-table `KeyError` -- in pure-Python tests that hold no session.
The factory defers construction to `evaluate()` time, where a DataFrame, hence a live
session, always exists. `tests/bronze/test_rules.py` pins that all three shapes here are
factories: a lambda (`_null_or_blank`), a closure (`_encoding_check`) and a plain module
function (`_cnpj_basico_length`)."""
from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column
from pyspark.sql import functions as F

from opl.bronze.snapshot import SNAPSHOT_REF_DATE_COLUMN
from opl.bronze.snapshot_axis import INSTANT_PATTERN, INSTANT_WIDTH
from opl.contracts.catalogue import CONTRACT_COLUMNS
from opl.contracts.merchant import CNPJ_WIDTH
from opl.contracts.ptax import PUBLISHED_AT_COLUMN
from opl.unicode_case import DIVERGENT_CHARACTER_CLASS

_REPLACEMENT_CHAR = "�"

def _null_or_blank(col: str) -> Callable[[], Column]:
    return lambda: F.col(col).isNull() | (F.trim(F.col(col)) == "")

def _encoding_check(contract: str) -> Callable[[], Column]:
    """U+FFFD in ANY column of the contract, not two hand-picked ones.

    Bronze is all-string, so every column of every contract is a string column
    and the check is total by construction. Derived from TABLES rather than
    listed, so a contract gaining a column gains the check with it -- a list
    would go stale exactly where a new column is most likely to be mojibake.

    Carry-forward #5, and the reason it is not cosmetic: one record in
    `Estabelecimentos8` carries a byte (0x8f) that windows-1252 cannot decode at
    all. Python raises on it; Java's decoder substitutes U+FFFD SILENTLY, which
    makes that character the only in-band evidence a byte was lost (ADR 0006).
    WHICH COLUMN HOLDS IT IS `correio_eletronico`, and that answer arrived only
    once the check was total: the 2026-07 estabelecimentos ingest rejected four
    rows for `encoding_replacement_char`, all four in that column (observed
    2026-08-03). `nome_fantasia` and `logradouro` -- the hand-picked pair the check
    covered before -- are neither of them it. So the old rule was not merely a coin
    flip on the record it was written for; it would have missed it, and did: the
    same four records sit un-flagged in 2026-06's bronze, promoted by a run whose
    gate measured zero. See ADR 0006 and `docs/f1.4b-pr-b-run-evidence.md` §20.3.

    The chain starts at `F.lit(False)` rather than at the first column's
    `contains`, so the fold is total over a contract of any length instead of
    raising IndexError on an empty one. The three-valued semantics are identical
    either way: `False | NULL` is NULL, `NULL | True` is True, so a row is flagged
    if ANY column holds the character and is left alone when none does, whatever
    mix of NULLs it carries.

    `tuple(...)` snapshots the contract's column list -- `CONTRACT_COLUMNS` hands
    out tuples, so this is belt-and-braces today; it stays because the property is
    about this closure outliving the call, not about the catalogue's current type.

    IT IS A LIVE CONTROL ON THE PAYMENT STREAM, NOT INHERITED BOILERPLATE, which
    needs saying because the obvious reading is that a generated UTF-8 JSON stream
    cannot contain mojibake. It can, by exactly the route F1b's central risk runs
    along: `opl.generator.events.to_jsonl` returns TEXT, and a writer that did not
    encode UTF-8 explicitly -- or a reader that did not decode as UTF-8 -- hands Java
    bytes it cannot map, and Java's decoder substitutes U+FFFD SILENTLY where Python
    raises. That character is then the only in-band evidence the bytes on disk are
    not the bytes the golden digest was taken over."""
    columns = tuple(CONTRACT_COLUMNS[contract])

    def predicate() -> Column:
        chain = F.lit(False)
        for column in columns:
            chain = chain | F.col(column).contains(_REPLACEMENT_CHAR)
        return chain

    return predicate


def _case_divergence_check(contract: str) -> Callable[[], Column]:
    """A character the two hash spellings UPPER-CASE DIFFERENTLY, in ANY column.

    NOT A SECOND `_encoding_check`, AND THE DIFFERENCE IS THE WHOLE POINT. That rule finds
    U+FFFD -- mojibake, the in-band evidence that a byte was LOST. These forty characters
    are valid, correctly decoded, and arrive exactly as the source sent them. What is wrong
    with them is not the bytes: it is that `F.upper` bottoms out in Java's case table
    (JDK 17, Unicode 13.0) and `str.upper()` in CPython's (3.12, Unicode 15.0), and the two
    disagree about these forty. `opl.unicode_case` pins the set and carries the measurement.

    WHY BRONZE AND NOT THE SEEDER, which is plan T10's ruling and the reason this rule
    exists at all. Revision 1 bounded what `scripts/merchant_population.py` may write, and
    that protects the seed and nothing else -- not the mutation script, not a manual `psql`
    INSERT, not a re-seed with different literals. The CNPJ contracts get this guard for
    free at the BOUNDARY: their dialect is cp1252 and none of the forty is encodable in it,
    which `test_no_character_the_two_spellings_disagree_about_can_reach_cnpj_bronze`
    asserts against the imported dialect. A UTF-8 Postgres source has no such property, so
    the guard has to be where every other content constraint in this repository lives.

    WHAT IT PREVENTS, and it is the failure with nothing to see. A row carrying one reaches
    the satellite's `hash_diff`. The loaders only ever use the SPARK spelling, so nothing
    goes red -- the Python and Spark digests simply disagree on real data, and the day a
    DBR upgrade moves onto Java 21 (Unicode 15) the forty start AGREEING and every vault
    row containing one is silently re-keyed.

    A CLASS BUILT FROM THE PINNED SET rather than a literal beside it: twenty-nine of the
    forty are astral, and `opl.unicode_case` explains why they are spelled `\\x{...}`. The
    fold is total over the contract for `_encoding_check`'s reason -- derived, so a v2
    column arrives covered -- and it is SHADOWED on the columns an earlier rule already
    inspects, since the gate is first-match-wins: one of these in `cnpj` breaks
    `bad_cnpj_shape`'s digit test and one in `onboarded_on` breaks the ISO shape. The
    columns it can be the REPORTED reason for are the free-text ones, `legal_name` and
    `trade_name`, which are exactly the columns T10 says a UTF-8 source reaches them
    through."""
    columns = tuple(CONTRACT_COLUMNS[contract])

    def predicate() -> Column:
        chain = F.lit(False)
        for column in columns:
            chain = chain | F.col(column).rlike(DIVERGENT_CHARACTER_CLASS)
        return chain

    return predicate


# 8 characters, which is `cnpj_basico`'s width in every contract that carries a
# company root -- the three RFB ones under that name, and the payment stream's two
# counterparty columns under their own. Named once here because the rules below build
# on it under two different column names; it is the same fact about the same key
# space, and `opl.generator.cnpj_pool.CNPJ_BASICO_WIDTH` refuses a pool entry of any
# other width at the boundary where payments' keys are drawn from `hub_empresa`.
_CNPJ_BASICO_WIDTH = 8


def _basico_length(column: str) -> Callable[[], Column]:
    """`column` is not exactly 8 characters after trimming.

    A LENGTH check and not a numeric one: alphanumeric CNPJs take effect 2026-07-31,
    and an `int()` round trip loses a leading zero the moment one arrives
    (`cnpj_schemas`). That is not an abstract worry for the payment stream -- it is
    the precise failure F1b Task 4's 100%-resolution measurement exists to catch, and
    this rule catches the same thing one step earlier, in the gate, BEFORE the rows
    reach bronze and before anyone joins them to `hub_empresa`.

    Parameterised by column since F1b Task 3, where the same shape has to be asserted
    about `payer_cnpj_basico` and `payee_cnpj_basico`. The reason string is built from
    the column name at the call site, so `bad_cnpj_basico_length` -- which already
    exists as DATA in two live quarantine tables -- comes out byte-identical for the
    three RFB contracts."""
    return lambda: F.length(F.trim(F.col(column))) != _CNPJ_BASICO_WIDTH


def _cnpj_basico_length() -> Column:
    """The RFB contracts' `cnpj_basico` width rule, under its historical name.

    Kept as a zero-argument function rather than replaced by `_basico_length(
    "cnpj_basico")` at the three call sites: `test_every_predicate_is_a_zero_arg_
    factory_and_not_a_column` inspects the signature of every predicate, and this is
    the module's one plain-function example of the three shapes it deliberately
    carries (a lambda, a closure, a module function)."""
    return _basico_length("cnpj_basico")()


# The ISO date this lakehouse stamps into `quote_date`, as a SHAPE and then as a real
# date. Two checks in one predicate because each misses what the other catches: the
# regex refuses `06-19-2026` -- the API's own `MM-DD-YYYY`, which is what a writer that
# reused the request's spelling would land -- and `to_date` refuses `2026-13-45`, which
# has the shape and names no day.
_ISO_DATE_SHAPE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
_ISO_DATE_FORMAT = "yyyy-MM-dd"


def _bad_iso_date(column: str) -> Callable[[], Column]:
    """`column` is not an ISO `YYYY-MM-DD` naming a real day.

    THE MISTAKE THIS PHASE INVITES, refused at the gate. The PTAX endpoint is asked in
    `MM-DD-YYYY` in single quotes -- not ISO, and got wrong by everyone who assumes --
    so the natural bug in a landing writer is to stamp the request's own spelling into
    the record. Nothing about that fails: the column is present, non-blank, ten
    characters, and it JOINS TO NOTHING in gold while every row count stays green, which
    is the shape this repository refuses to ship.

    THE REGISTRY'S CHECK IS THE SAME SHAPE NOW, AND THIS PREDICATE IS STRICTLY STRONGER.
    `quote_date_iso_shape` was `length(trim(quote_date)) = 10`, which accepted
    `06-19-2026`; F-API's fix pass made it `regexp_like` on the same digit shape as
    `_ISO_DATE_SHAPE`, so it refuses that value too and "length alone accepts it" is no
    longer the distinction. What survives is: this predicate ALSO requires `to_date` to
    name a real day, so `2026-13-45` passes the CHECK and is refused HERE; and this runs
    in the gate, before anything reaches bronze, while the CHECK runs at the promote,
    AFTER the append has committed. Both differences point the same way -- the gate is
    never weaker than the constraint downstream of it, which is the one direction this
    repository does not allow. Both halves are pinned in tests/bronze/test_ptax_rules.py,
    the second against a real Delta transaction log."""
    return lambda: ~F.col(column).rlike(_ISO_DATE_SHAPE) | F.to_date(
        F.col(column), _ISO_DATE_FORMAT
    ).isNull()


def _unparseable_decimal(column: str, decimal_type: str) -> Callable[[], Column]:
    """`column` does not read as `decimal_type`.

    WHAT IT CATCHES AND WHAT IT DOES NOT, stated because the difference is not obvious and
    two contracts now rest on it: a cast to `decimal(p,s)` ROUNDS a value with too many
    fractional digits and returns NULL only when the value is not a number at all or
    exceeds the declared PRECISION. So this refuses `1,234.50`, `R$ 1234.50`, `1.2e400` and
    a value wider than `p` digits; it does not refuse a third decimal place. Scale is
    pinned upstream instead, by the DDL the value is rendered from."""
    return lambda: F.col(column).cast(decimal_type).isNull()


def _unparseable_rate(column: str) -> Callable[[], Column]:
    """`column` does not read as a decimal number.

    NEAR-TAUTOLOGICAL TODAY, AND SAID SO RATHER THAN DROPPED. The landing writer stamps
    `str(Decimal(...))` from a value `opl.extraction.ptax_source` already parsed with
    `Decimal` from the raw response text, so a rate that reaches bronze unparseable means
    the emitter changed shape -- a `repr(float)`, a locale-formatted comma, a truncation.
    The reason it earns a place on an all-or-nothing gate is what a NULL rate does
    downstream: `amount_brl` is `amount_original * venda`, so an unreadable venda is not
    a missing column, it is every payment on that date converted at nothing, lowering
    a total by an amount nobody can name.

    `decimal(18,5)` is the series' own scale -- five digits, which `5.14420` needs and
    which `decimal(18,2)` would round to `5.14`, putting `amount_brl` 0.0816% wrong on
    every row carrying THAT rate while looking deliberate. (Not "every row": 5.13950
    rounds to 5.14 too and is 0.0097% off the other way -- ADR 0016 retracts the wider
    phrasing, and the argument stands on the worse half.)"""
    return _unparseable_decimal(column, _RATE_TYPE)


_RATE_TYPE = "decimal(18,5)"
# The merchant registry's `credit_limit`, declared `numeric(14,2)` by the source's own DDL.
# THE PRECISION IS EXERCISED RATHER THAN NOMINAL: `merchant_population.MAX_CREDIT_LIMIT` is
# `999999999999.99`, which is exactly fourteen digits, so a cast narrower than the DDL
# would NULL a value the source is entitled to send and fail the whole run.
_CREDIT_LIMIT_TYPE = "decimal(14,2)"


# A publication INSTANT: a date, a space, a time to the second, and 1-6 fractional
# digits or none. Two checks in one predicate below, for `_bad_iso_date`'s reason -- each
# misses what the other catches. The shape refuses a value whose instant is not fully
# determined by its own text (see the comment block below `_ISO_DATE_SHAPE`'s siblings);
# `to_timestamp` refuses `2026-13-45 11:00:00`, which HAS the shape and names no instant.
#
# THE WIDTH IS 1 TO 6 BECAUSE THE SERIES USES 1, 3 AND 6, and this is the whole reason a
# single `to_timestamp` PATTERN is still the wrong fix: `yyyy-MM-dd HH:mm:ss.SSSSSS`
# rejects `1984-12-03 11:29:00.0` and `2025-04-23 13:02:31.416`, both real rows this
# endpoint returns. The parse stays format-agnostic; only the SHAPE is pinned, and it is
# pinned to a set rather than to one width -- because whether a spelling is a publication
# instant is ONE decision spanning the extraction layer and the gate, and a gate looser
# than the extraction tolerates exactly the values a bug between the two could produce.
#
# IT DOES NOT MATCH `ptax_source.PUBLICATION_FORMATS` EXACTLY, AND THE ASYMMETRY IS SAID
# HERE RATHER THAN CLAIMED AWAY -- an earlier version of this block said "exactly" and it
# was measured false. `strptime`'s `%m`, `%d`, `%H`, `%M` and `%S` each accept an UNPADDED
# field, so the extraction validates `2026-6-19 13:03:25`, `2026-06-9 13:03:25`,
# `2026-06-19 1:03:25` and the unpadded-minute and unpadded-second spellings, all five of
# which this shape refuses (`%Y` is the one field that does demand four digits, so
# `26-06-19 13:03:25` is refused by both). The fractional clause is the half that DOES
# agree: `{1,6}` is `%f`'s own range, and the fraction-less second spelling is why the
# group is optional.
#
# THE DIVERGENCE IS ONE-DIRECTIONAL AND THAT IS THE SAFE DIRECTION: everything this shape
# accepts, the extraction accepts, so no value can reach bronze past the extraction and
# then be called valid here. The cost of the gap, stated because it is not zero: if BCB
# ever published an unpadded stamp, the extraction would land it and this gate would fail
# the WHOLE run on a row the extraction had called valid, under a reason ("does not read as
# a determinate publication instant") that is untrue of it. The fix at that point is to
# widen these quantifiers to `{1,2}`, never to loosen the parse. Pinned across the seam by
# `test_the_gate_accepts_no_spelling_the_extraction_would_refuse` -- which fails if either
# side moves, so the paragraph above cannot go stale the way "exactly" did.
_INSTANT_SHAPE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?$"


def _unparseable_publication_instant() -> Column:
    """`data_hora_cotacao` does not read as a determinate publication instant.

    IT IS T3's COMPARATOR, which is why this is not the same rule as the two above wearing
    a different column name. The rate for a payment is the most recent quote whose
    PUBLICATION INSTANT precedes the payment's own; a value the comparison cannot read
    becomes NULL, the row drops out of the as-of resolution, and the payment silently
    resolves to an OLDER quote instead. Nothing is missing and nothing fails -- the answer
    is just the wrong rate.

    WHAT "DETERMINATE" ADDS, AND IT IS THIS RULE'S WHOLE HISTORY. It was
    `to_timestamp(...).isNull()` alone -- format-agnostic, so every fractional width the
    series uses is accepted, which is still right. But Spark's single-argument
    `to_timestamp` PARSES A BARE TIME, and it does not date it 1970: `13:03:25.555497`
    becomes TODAY'S DATE. Measured through `opl.spark.local_session` (pyspark 3.5.9): on
    2026-08-14 that text resolves to `2026-08-14 13:03:25.555497`, and tomorrow it
    resolves to something else. Two consequences, each fatal on its own:

      * IT IS NON-DETERMINISTIC. The same landed bytes yield a different instant on a
        different day, and being a function of its input is bronze's entire contract.
      * THE CONSEQUENCE IS INVERTED. Today is LATER than every payment in this phase's
        June/July window, so the row is excluded from every as-of set and the payment
        resolves to an older quote -- verbatim the failure this docstring says the rule
        exists to prevent. The retracted claim ("a real instant fifty-six years early
        that every payment sorts after") had it backwards.

    A bare DATE is refused by the same clause and for a weaker but real reason: it is
    determinate, and it is midnight -- an instant BCB never published, which turns T3's
    instant comparison into the calendar-day comparison the contract's own provenance
    guard exists to refuse.

    "CLOSED UPSTREAM" WAS FALSE AT THE BOUNDARY THIS GATE POLICES, which is why the fix is
    here and not only there. `ptax_source._publication_instant` does refuse both shapes --
    but `bronze_ptax_ingest` reads a DIRECTORY against `struct_for("ptax")` and never
    imports the extraction module, and this table deliberately has no `reclaim_landing`, so
    a landed file persists indefinitely. A file written by a wheel built from another
    revision, hand-repaired, or copied in meets no extraction guard whatsoever. That is
    exactly the boundary the five `null_or_empty_*` rules are justified by.

    The NULL case is a near-tautology with `null_or_empty_data_hora_cotacao`, which runs
    FIRST and therefore wins the reason -- first-match-wins is the gate's contract. That
    is the right ordering: a row with no stamp at all is described by the missing stamp,
    not by the stamp being unreadable."""
    return ~F.col(PUBLISHED_AT_COLUMN).rlike(_INSTANT_SHAPE) | F.to_timestamp(
        F.col(PUBLISHED_AT_COLUMN)
    ).isNull()


# A CNPJ as the merchant registry carries it: exactly `CNPJ_WIDTH` characters, every one a
# digit. TWO CHECKS IN ONE PREDICATE, for `_bad_iso_date`'s reason -- each misses what the
# other catches. The width alone accepts `1234567800011x`; the digit test alone accepts an
# eight-character root, which is the value a writer that landed `cnpj_basico` by mistake
# would produce and which would then join to `hub_empresa` while every count stayed green.
#
# NOT TRIMMED, unlike `_basico_length`. The RFB's fixed-width CSV pads its fields, so
# trimming there is reading the source's own dialect; this value is rendered by Postgres
# from a `text` column, where a leading space IS a different key. A rule that trimmed would
# be looser than the CHECK constraint the promote applies afterwards, which is the one
# direction this repository does not allow.
_DIGITS_ONLY = r"^[0-9]*$"


def _bad_cnpj(column: str) -> Callable[[], Column]:
    """`column` is not exactly `CNPJ_WIDTH` digits.

    A LENGTH-AND-ALPHABET check and never a numeric one. Measured (evidence §0.3): 142 of
    the 1,024 pinned counterparty roots carry a LEADING ZERO, so any cast to numeric
    destroys 13.9% of this pool silently -- and the failure is downstream and total, since
    the first eight characters of this column are what `link_merchant_empresa` joins to
    `hub_empresa` with. `int()` would also accept `1_4`, `+14` and ` 14`."""
    return lambda: (F.length(F.col(column)) != CNPJ_WIDTH) | ~F.col(column).rlike(
        _DIGITS_ONLY
    )


def _bad_snapshot_instant(column: str) -> Callable[[], Column]:
    """`column` is not the fixed-width UTC instant the snapshot axis is spelled in.

    THE AXIS, NOT A TIMESTAMP, and that is why this is not `_bad_iso_date` with a longer
    pattern. `opl.vault.observation` splits before/after a first observation with a STRING
    COMPARISON on this column, and `opl.vault.loading.earliest_record_source` takes a `min`
    over it -- so a value of the wrong shape does not fail anything, it SORTS WRONGLY. Both
    qualifiers of the rendering are load-bearing and both are checked here: fixed width,
    because `...01.1` sorts after `...01.09` once trailing fractional zeros are trimmed, and
    UTC, because an offset-bearing rendering orders two instants backwards across a zone
    change.

    THE LENGTH IS NOT REDUNDANT BESIDE THE ANCHORED PATTERN. `$` matches BEFORE A TRAILING
    LINE TERMINATOR -- measured, in Python's `re` as well as in the Java engine a Spark
    `rlike` runs -- so a value with a newline glued on satisfies the pattern in both engines
    while failing `snapshot_axis._is_instant`, which checks the width FIRST and is the
    predicate the vault job's window PARAMETER is validated by. Two answers to one question
    about one value.

    IT IS THE GATE'S HALF OF A PAIR. The registry's `snapshot_at_instant_shape` CHECK says
    the same thing at the promote, AFTER the append has committed; this runs before
    anything reaches bronze, so the gate is never weaker than the constraint downstream of
    it. Both are pinned against each other in tests/bronze/test_merchant_rules.py."""
    return lambda: ~F.col(column).rlike(INSTANT_PATTERN) | (
        F.length(F.col(column)) != INSTANT_WIDTH
    )


def _unprovable_ref_date() -> Column:
    """The reference date the RFB declares in its own filename, absent.

    `snapshot.ref_date_column` yields NULL whenever it cannot PROVE a date --
    no `.D<y><mm><dd>.` token in the filename, two of them, or a token whose
    month/year digit disagrees with the job's month parameter. That refusal was
    only half a control: nothing read the NULLs, so a month shipping a different
    filename shape produced an all-NULL column and a green run. This is the half
    that speaks, and it is the debt `snapshot.py`'s docstring booked to F1.4b.

    SAFE ON A LIVE TABLE BECAUSE IT IS MEASURED, the same precondition
    `municipio` had to meet: over the 71,874,448 rows of
    workspace.default.bronze_cnpj_estabelecimentos live when this was written, the
    NULL count for this column is 0, verified by a SQL query independent of the
    backfill script's own log (docs/f1.4a-migration-evidence.md) -- and the 2026-07
    ingest re-confirmed it on a further 72,318,968 staged rows whose only rejects
    were 4 `encoding_replacement_char`. So this rejects nothing that exists today.
    The gate is all-or-nothing -- any reject fails the run -- so that number is a
    precondition and not a footnote, and it is re-earned each month rather than
    inherited: it is a claim about a (month, rule set) pair.

    A row-level rule for a FILE-level fact, deliberately. The gate has no other
    vocabulary -- it tags rows -- and the shape that follows from that is the
    right one anyway: when a filename format changes, every row of that file
    carries the reason, the gate is all-or-nothing, and the run goes red with the
    reason naming the actual cause. A batch mixing one unparseable file with
    several good ones quarantines only the rows from the bad file, which is the
    behaviour a per-file count could not express.

    WHAT THIS DOES NOT CATCH, so it is not mistaken for more than it is: a token
    that parses but is WRONG (the RFB restating June's date on a July file) is a
    date this rule accepts. `_snapshot_month` sits beside it carrying the job's
    month, so the disagreement is visible in the row -- see snapshot.py on why
    both columns exist. Catching that would be a cross-column check, not this."""
    return F.col(SNAPSHOT_REF_DATE_COLUMN).isNull()
