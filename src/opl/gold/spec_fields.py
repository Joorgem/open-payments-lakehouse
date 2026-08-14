# src/opl/gold/spec_fields.py
"""What a gold spec's FIELDS may be: the two refusals every kind shares, and `FactRole` --
the one declaration that is smaller than a table.

WHY THIS FILE EXISTS AND WHERE THE SEAM IS. `opl.gold.specs` held every kind and every
guard, and F-API Task 4 widened two of them past the project's 800-line cap (master
protocol section 4.12: whoever touches a file at the cap splits it FIRST -- and section 6
of the phase plan recorded that its own estimate for a comparable file was low by 17 lines,
which is exactly what happened here, at 854). The split is by DEPENDENCY DIRECTION and not
by subject: this module is what both spec modules read and it reads neither of them, so
`opl.gold.fact_spec` may import the FACT's own guards from a file the dimension kinds also
use without either importing the other. `spec_fields` <- `fact_spec` <- `specs`, one
direction, no cycle, and the `GoldTable` union stays in `specs` where every consumer
already looks for it.

PURE, LIKE `opl.gold.columns` AND FOR THE SAME REASON: nothing here imports pyspark, so a
spec stays constructible and refusable in a plain `python -c` and an operator who mistyped
a declaration is told so before a serverless session starts costing money. The one
non-stdlib import is `opl.contracts.payments`, which is pure by its own decision and is
what makes `FactRole`'s two refusals checks rather than conventions.

--- WHY A ROLE IS FOUR FIELDS AND NOT A STRING (F-API T4b) ---------------------------

`CalendarDimension.roles` was a tuple of STRINGS and `fact_key` refused any length but
one, its own docstring prescribing "a second `fact_column` declared beside it". THAT
PRESCRIPTION DOES NOT FIT THE SECOND ROLE THIS STAR ACTUALLY GAINED.
`_assert_the_fact_column_is_one_the_contract_carries` below refuses any
`fact_column` outside `payments.COLUMNS`, and `fx_rate_date` can never be one: the FX join
PRODUCES it, in gold, out of `bronze_ptax` and the payment's own instant. A second
`fact_column` beside the first would have had to be a payment column to pass, so the
prescribed fix refuses the only second role there was.

So a role declares its SOURCE. `FROM_CONTRACT` means the payment contract carries the
column and the guard demands it; `FROM_DERIVED` means this layer computes it and the guard
demands the contract does NOT carry it -- the mirror refusal, and the one that keeps the
distinction from being a label anybody can put on anything.

AND IT DECLARES HOW THE COLUMN IS READ, which is a separate QUESTION from where it came from
even though the two answers are locked together. Bronze is all-string, so a contract-carried
instant is ISO-8601 TEXT and its day is ten characters (`opl.gold.conformed.day_of`, which
exists because a CAST would move the day with the session zone); a date this layer derived is
already a `date`, and reading ten characters off one would be a cast in the other direction.
Both roles then go on to the SAME key mechanism, so `opl.gold.conformed._member_key` still
cannot drift between the dimension side and the fact side -- which is the property that whole
function exists to hold.

AND BECAUSE THEY ARE LOCKED TOGETHER, THE PAIRING IS REFUSED AND NOT DOCUMENTED. This
paragraph said the reader "is not derivable from" the source, and that was the reason nothing
checked it: `reads` was validated for MEMBERSHIP of `ROLE_READERS` and never against the
column it reads. It IS derivable -- `READS_DATE` iff `FROM_DERIVED`, because nothing the
contract carries is anything but text -- and what the gap admitted was this phase's own zone
hazard wearing a declaration: see `FactRole._assert_the_reader_matches_the_declared_source`.

--- WHY THE ADDITIVITY TOKENS ARE DECLARED HERE AND USED THERE ------------------------

`opl.gold.fact_spec.DerivedMeasure` and `PaymentFact` both read them, and so does the run
log. They sit with the field guards rather than with the fact because they are a closed set
of declared VALUES, checked by `_assert_one_of` exactly as a role's source and reader are;
what a fact does with them is that module's subject and the argument for three of them
rather than a boolean is written where they are used.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from opl.contracts import payments
from opl.gold.columns import DIMENSION_COLUMNS

__all__ = [
    "ADDITIVE",
    "ADDITIVE_WITHIN_CURRENCY",
    "ADDITIVITIES",
    "FROM_CONTRACT",
    "FROM_DERIVED",
    "NON_ADDITIVE",
    "READS_DATE",
    "READS_ISO_TEXT",
    "READS_MEMBER",
    "ROLE_READERS",
    "ROLE_SOURCES",
    "FactRole",
]

# WHERE THE COLUMN A ROLE READS COMES FROM. The two are refused in opposite directions --
# see the module docstring -- so a role cannot be relabelled into passing.
FROM_CONTRACT = "contract"
FROM_DERIVED = "derived"
ROLE_SOURCES = (FROM_CONTRACT, FROM_DERIVED)

# HOW THE FACT READS THE COLUMN to get the member. `READS_ISO_TEXT` is bronze's all-string
# instant, whose day is its first ten characters; `READS_DATE` is a `date` this layer
# derived; `READS_MEMBER` is the value itself, which is what an enumerated domain's column
# holds.
READS_ISO_TEXT = "iso-instant-text"
READS_DATE = "date"
READS_MEMBER = "member"
ROLE_READERS = (READS_ISO_TEXT, READS_DATE, READS_MEMBER)

# WHAT SUMMING A FACT COLUMN MEANS. Three values and not a boolean; `opl.gold.fact_spec`
# argues why, where the three are declared against real columns.
ADDITIVE = "additive"
ADDITIVE_WITHIN_CURRENCY = "additive-within-currency"
NON_ADDITIVE = "non-additive"
ADDITIVITIES = (ADDITIVE, ADDITIVE_WITHIN_CURRENCY, NON_ADDITIVE)


def _assert_every_field_is_named(kind: str, name: str, fields: Mapping[str, object]) -> None:
    """Refuse a blank or non-string field, naming the role rather than the attribute.

    SHARED BY EVERY KIND IN BOTH SPEC MODULES rather than written per kind, because the
    message is the part that matters and copies of a message drift into several diagnoses
    of one defect. It moved here from `opl.gold.specs` when the fact kind moved out: two
    modules needed it and one of them could not import the other."""
    for role, value in fields.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"a gold {kind} needs a {role}, got {value!r}. {name!r} would be "
                "registered as a table no loader can project a column for"
            )


def _assert_one_of(kind: str, name: str, role: str, value: str, allowed: tuple[str, ...]) -> None:
    """Refuse a declared token outside its own domain.

    A CLOSED SET AND NOT A FREE STRING, because every consumer BRANCHES on these values: a
    misspelled `reads` falls through to the fallback branch and reads a member out of
    something that is not one, and a misspelled `additivity` leaves the additive count at
    zero -- which the fact's own guard then reports as a star with no summable measure, a
    true statement about a typo and the wrong diagnosis."""
    if value not in allowed:
        raise ValueError(
            f"gold {kind} {name!r} declares {role} {value!r}, which is not one of "
            f"{list(allowed)}. Every consumer branches on this value, so an unknown one "
            "silently takes a fallback branch rather than failing"
        )


def _assert_the_fact_column_is_one_the_contract_carries(name: str, fact_column: str) -> None:
    """Refuse a fact-side column that v1 does not declare, and refuse a DRIFT column
    loudly and first.

    THE DRIFT REFUSAL IS THE POINT OF THIS FUNCTION AND IT READS `DRIFT_COLUMNS`. A
    channel dimension is the single most plausible reason anybody would ever declare
    `payment_channel`, and `opl.contracts.payments` records what declaring it destroys:
    the serialiser emits it on every record so the stream carries no drift, an Auto
    Loader read schema built from the contract absorbs it so `rescued_data_present`
    never fires, and a `COUNT(DISTINCT ...)` over the business attributes drops every
    pre-drift row -- this repository's own 8,761-row defect. All three are silent.

    THE MEMBERSHIP CHECK IS WHAT LETS GOLD SPELL A CONTRACT COLUMN NAME AT ALL. Two of
    the three columns read here have no named constant in `opl.contracts.payments`
    (`payment_method` and `currency` are members of a tuple, not declarations), so the
    name is written out below -- and this refusal is what stops that copy going stale:
    a rename in the contract turns the import of every gold module red, in a message
    naming the columns v1 actually carries."""
    if fact_column in payments.DRIFT_COLUMNS:
        raise ValueError(
            f"dimension {name!r} draws from {fact_column!r}, which is a drift column "
            f"({', '.join(payments.DRIFT_COLUMNS)}) and is undeclared BY DESIGN. "
            "Declaring it to feed a dimension would end the drift class three silent "
            "ways -- every record would carry it, the read schema would absorb it "
            "instead of rescuing it, and a distinct count over the business attributes "
            "would drop the whole pre-drift population. Draw the channel from "
            "`payment_method`, which every record carries"
        )
    if fact_column not in payments.COLUMNS:
        raise ValueError(
            f"dimension {name!r} draws from {fact_column!r}, which is no column the "
            f"payment contract declares ({', '.join(payments.COLUMNS)}). The fact would "
            "resolve every row to the unknown member and report a clean 100%"
        )


@dataclass(frozen=True, kw_only=True)
class FactRole:
    """One foreign key a fact carries into a conformed dimension: what the FACT calls the
    key, which of the fact's own columns the member is read from, where that column came
    from, and how it is read.

    `key` IS GOLD'S NAME AND `fact_column` IS WHOEVER OWNS IT. `event_date_key` is a name
    this layer invents over `event_time`, which the payment contract owns; `fx_rate_date_key`
    is a name this layer invents over `fx_rate_date`, which this layer also produces. Both
    halves are declared because neither is derivable from the other -- deriving
    `<column>_key` by string surgery would be one rename away from a column nothing joins
    to, with the load reporting success.

    `kw_only`, like every spec in this repository: `key` and `fact_column` are adjacent
    strings and `source` and `reads` are two more, so a positional construction that
    permuted either pair would type-check perfectly and declare a role that reads the key
    column and keys the read one."""

    key: str
    fact_column: str
    source: str
    reads: str

    def __post_init__(self) -> None:
        _assert_every_field_is_named(
            "fact role",
            self.key,
            {
                "key": self.key,
                "fact column": self.fact_column,
                "source": self.source,
                "reader": self.reads,
            },
        )
        _assert_one_of("fact role", self.key, "a source", self.source, ROLE_SOURCES)
        _assert_one_of("fact role", self.key, "a reader", self.reads, ROLE_READERS)
        self._assert_the_key_is_the_facts_own()
        self._assert_the_column_matches_the_declared_source()
        self._assert_the_reader_matches_the_declared_source()

    def _assert_the_key_is_the_facts_own(self) -> None:
        """Refuse a key spelled like the column it is derived from, or like one the gold
        loaders write themselves."""
        if self.key == self.fact_column:
            raise ValueError(
                f"fact role {self.key!r} is derived FROM a column of that same name. One "
                "projection writes the surrogate key and the value it was derived from "
                "into one column, so one of them survives and every row is still there"
            )
        if self.key in DIMENSION_COLUMNS:
            raise ValueError(
                f"fact role {self.key!r} is a column the gold loaders write themselves "
                f"({', '.join(sorted(DIMENSION_COLUMNS))}). One projection, two values, "
                "and the key is silently a timestamp -- every join on it matches nothing"
            )

    def _assert_the_column_matches_the_declared_source(self) -> None:
        """Refuse a contract-sourced role over a column v1 does not carry, and a derived
        role over one it does.

        BOTH DIRECTIONS, WHICH IS WHAT MAKES `source` A DECLARATION RATHER THAN A LABEL.
        The first half DELEGATES to `_assert_the_fact_column_is_one_the_contract_carries`
        instead of restating it, and that delegation is load-bearing: it is what keeps the
        DRIFT refusal reachable for a calendar role. Written as its own membership check here,
        a `payment_channel` role would be refused for not being a contract column -- true,
        and the wrong diagnosis for the single most plausible reason anybody declares that
        name. The second half is this kind's own addition and it is what keeps the widening
        from being a hole: a role labelled `derived` over `event_time` would be built by the
        FX projection instead of by the payment's, and would arrive with the right type and
        another column's meaning."""
        carried = self.fact_column in payments.COLUMNS
        if self.source == FROM_CONTRACT:
            _assert_the_fact_column_is_one_the_contract_carries(
                f"role {self.key}", self.fact_column
            )
        if self.source == FROM_DERIVED and carried:
            raise ValueError(
                f"fact role {self.key!r} declares {self.fact_column!r} as {FROM_DERIVED} "
                "and the payment contract carries it. A delivered column labelled derived "
                "is built by the wrong projection: it arrives with the right type and "
                f"another column's meaning. Declare it {FROM_CONTRACT}"
            )

    def _assert_the_reader_matches_the_declared_source(self) -> None:
        """Refuse a reader the column's own REPRESENTATION cannot support -- `READS_DATE` for
        anything but a derived column, and a derived column read any other way.

        `reads` WAS CHECKED FOR MEMBERSHIP AND NEVER AGAINST THE COLUMN, and the failure mode
        is this phase's own zone hazard rather than a typo. Bronze is ALL-STRING, so no column
        the contract carries is a `date`: a contract-sourced role over `event_time` declaring
        `READS_DATE` passes every other guard, and `opl.gold.conformed._member_key` then applies
        `date_format` to raw ISO TEXT -- which CASTS it first, in the SESSION zone, so under
        America/Sao_Paulo every midnight-UTC payment keys to the previous day. That is the exact
        defect `day_of` exists to prevent, arriving through a declaration instead of through a
        cast, and until this guard the only thing that would have caught it was the non-UTC
        rebuild test in `tests/gold/test_fact_payment.py`.

        THE PAIRING IS DERIVABLE AND IS THEREFORE REFUSED RATHER THAN DOCUMENTED. A derived
        date is a `date` because this layer built it that way, and everything the contract
        carries is text -- so `READS_DATE` iff `FROM_DERIVED`, and `READS_ISO_TEXT` and
        `READS_MEMBER` are both statements about a string. It is not folded into the source
        check above because that one is about PROVENANCE and this one about TYPE: a role can get
        its provenance right and still read the column with the wrong reader."""
        if (self.reads == READS_DATE) == (self.source == FROM_DERIVED):
            return
        raise ValueError(
            f"fact role {self.key!r} declares {self.fact_column!r} as {self.source} and reads "
            f"it as {self.reads!r}. Bronze is ALL-STRING, so {READS_DATE!r} names a column "
            f"this layer DERIVED and the other readers name text: {FROM_CONTRACT}-sourced "
            f"roles read {READS_ISO_TEXT!r} or {READS_MEMBER!r}, {FROM_DERIVED} ones read "
            f"{READS_DATE!r}. The mismatch does not crash -- `date_format` over raw ISO text "
            "casts it in the SESSION zone, so every midnight-UTC payment keys to the previous "
            "day and the star's answer becomes a function of a cluster setting"
        )


def _assert_the_roles_are_a_set_with_one_contract_source(
    kind: str, name: str, roles: tuple[FactRole, ...]
) -> None:
    """Refuse an empty role tuple, two roles sharing a key or a column, and any number of
    contract-sourced roles other than exactly one.

    EXACTLY ONE CONTRACT SOURCE, AND IT IS NOT A TIDINESS RULE. It is the column the
    dimension's own SPAN is measured from: `opl.gold.conformed.covered_span` reads the FACT
    SOURCE, which is `bronze_payments`, and a derived column does not exist there at all.
    Zero contract-sourced roles leaves the calendar with no span to measure and nothing to
    fail about it; two leave the span a function of which one a reader picked. What the
    derived roles get instead is the orphan count, measured per role AFTER the write and
    REPORTED -- so a derived date outside the calendar's span is a number rather than a
    silent join to nothing.

    A KEY TWICE is one column projected twice under one name, the second overwriting the
    first. A COLUMN TWICE is two keys derived from one value: they agree on every row, so
    the fact is correct, one column wider than it needs to be, and every reader who grouped
    by the wrong one gets the right answer until the day the two diverge."""
    if not roles:
        raise ValueError(
            f"gold {kind} {name!r} declares no role. Nothing in the fact would carry a "
            "foreign key into it, so it would build, be well-formed, and return its own "
            "members and no facts"
        )
    for role, values in (
        ("key", [item.key for item in roles]),
        ("column", [item.fact_column for item in roles]),
    ):
        repeated = sorted({value for value in values if values.count(value) > 1})
        if repeated:
            raise ValueError(
                f"gold {kind} {name!r} declares the {role} {repeated} twice: {values}. "
                "Two of its foreign keys would be one column, or would be two columns "
                "derived from one value that agree on every row"
            )
    sourced = [item.key for item in roles if item.source == FROM_CONTRACT]
    if len(sourced) != 1:
        raise ValueError(
            f"gold {kind} {name!r} declares {len(sourced)} {FROM_CONTRACT}-sourced roles "
            f"({sourced}) and needs exactly one. Its span is measured from that role's "
            "column over the FACT SOURCE, where a derived column does not exist: none "
            "leaves the span unmeasurable, and two leave it a function of which role a "
            "reader picked"
        )


def _assert_a_calendars_roles_read_a_day(kind: str, name: str, roles: tuple[FactRole, ...]) -> None:
    """Refuse a calendar role that reads a MEMBER -- the one pairing `FactRole` itself cannot
    judge, because it needs the KIND.

    `FactRole._assert_the_reader_matches_the_declared_source` locks the reader to the column's
    representation, which leaves `READS_MEMBER` over a contract column legal -- and it IS legal,
    for an ENUMERATED dimension, whose member is exactly the string the column holds. For a
    CALENDAR it is the zone hazard from the other direction: `opl.gold.conformed._member_key`
    branches on the kind, so a calendar role reading a member hands `date_format` the RAW ISO
    INSTANT TEXT, which casts it in the SESSION zone and keys every midnight-UTC payment to the
    previous day. A calendar's contract role reads ISO TEXT and its derived roles read a DATE,
    so between the two guards each source has exactly one legal reader.

    IT IS A SEPARATE FUNCTION AND NOT A CLAUSE OF THE GUARD ABOVE because it is the only check
    here that is about ONE KIND: the guard above is called by whatever declares a role tuple,
    and this one only by the calendar."""
    reading_members = sorted(role.key for role in roles if role.reads == READS_MEMBER)
    if reading_members:
        raise ValueError(
            f"gold {kind} {name!r} declares {reading_members} reading {READS_MEMBER!r}, and a "
            f"calendar's member is a DAY. Its key is `date_format(...)`, so a role reading a "
            f"member hands that the column's RAW TEXT -- an ISO instant, cast in the SESSION "
            f"zone, which keys every midnight-UTC payment to the previous day. A contract role "
            f"reads {READS_ISO_TEXT!r} (ten characters, zone-free) and a derived one "
            f"{READS_DATE!r}"
        )
