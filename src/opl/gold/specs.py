# src/opl/gold/specs.py
"""The KINDS a gold table may be -- one dataclass per shape, each refusing at its own
`__post_init__` everything that can be decided about one table in isolation.

SPLIT OUT OF `opl.gold.registry` AT THE MOMENT ITS OWN DOCSTRING SAID TO. That file
carried `Scd2Dimension` alone and pre-decided the move: "only when this file approaches
the project's 800-line cap do the kinds move wholesale to an `opl.gold.specs` module".
F3 Task 3 added two kinds and three tables and took it to 728 lines, and F3 Task 4's
fact is another kind again -- so the split happens here rather than under the pressure
of a file already over the cap. It is the same split `opl.vault.specs` made out of
`opl.vault.registry`, for the same reason and with the same seam: a SPEC is what one
table declares about itself, a REGISTRY is what the set of them must satisfy together.

WHICH GUARD LIVES WHERE, because the seam is the whole value of the split. Everything
here is answerable from one spec and nothing else: is the surrogate key a column the
loader writes, is the fact column one the payment contract carries, does the member list
repeat. Everything that needs to see the OTHER tables -- a name two specs claim, a name
another layer owns, a satellite that must resolve against the vault registry -- stays in
`opl.gold.registry.build_registry`. A guard on the wrong side of that line is either
unreachable at import (a whole-set check written here) or reached too late (a
single-table check written there, after a session has started).

NOTHING HERE IMPORTS PYSPARK, and that is load-bearing rather than tidy: a spec must be
constructible and refusable in a plain `python -c`, so an operator who mistyped a table
name is told so before a serverless session starts costing money. The one non-stdlib
import is `opl.contracts.payments`, which is pure by its own decision."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from opl.contracts import payments
from opl.gold.columns import DIMENSION_COLUMNS

__all__ = [
    "CalendarDimension",
    "ConformedDimension",
    "EnumeratedDimension",
    "GoldTable",
    "Scd2Dimension",
]


@dataclass(frozen=True, kw_only=True)
class Scd2Dimension:
    """A Kimball type-2 dimension derived from ONE Data Vault satellite: a surrogate
    key, and the name of the satellite whose versions become its rows.

    THREE FIELDS, AND EVERYTHING ELSE IS READ FROM THE VAULT. The payload columns are
    the satellite's, the natural key and its zero-pad width are the parent hub's, and
    the parent hub is `opl.vault.domains.parent_hub`'s answer. None of them is declared
    here, and that is the decision this spec is: a dimension that restated its source's
    payload would be a second spelling of a column list, and the copy that goes stale is
    always the one no load ever reads. It also makes the extension free -- a satellite
    that gains a payload column gains it in the dimension on the next load, with no gold
    edit at all.

    `kw_only`, like `opl.bronze.registry.BronzeTable` and every vault kind: `name`,
    `surrogate_key` and `source_satellite` are three adjacent strings, so a positional
    construction that permuted them would type-check perfectly and register a dimension
    called `company_sk` reading a satellite called `dim_company`.

    NO TARGET TABLE, NO CATALOG, NO SCHEMA. A spec carries an unqualified `name` and the
    loader takes the qualified table as an argument, so `opl.config` is consulted by
    whatever calls the loader and nowhere in this layer -- the same division
    `opl.vault.registry` states and `tests/test_gold_job_wiring.py` asserts over the
    entry point."""

    name: str
    surrogate_key: str
    source_satellite: str

    def __post_init__(self) -> None:
        for role, value in (
            ("name", self.name),
            ("surrogate key", self.surrogate_key),
            ("source satellite", self.source_satellite),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"a gold dimension needs a {role}, got {value!r}. A dimension with "
                    "no source satellite is a table nothing can derive"
                )
        if self.surrogate_key in DIMENSION_COLUMNS:
            raise ValueError(
                f"dimension {self.name!r} names {self.surrogate_key!r} as its surrogate "
                f"key, and the loader writes that column itself "
                f"({', '.join(sorted(DIMENSION_COLUMNS))}). The collision does not "
                "crash: one projection writes two values into one column, so the "
                "surrogate key is silently a timestamp or a flag and every fact that "
                "joins on it matches nothing"
            )


def _assert_every_field_is_named(kind: str, name: str, fields: Mapping[str, object]) -> None:
    """Refuse a blank or non-string field, naming the role rather than the attribute.

    Shared by both conformed kinds rather than written twice, because the message is the
    part that matters and two copies of a message drift into two diagnoses of one
    defect."""
    for role, value in fields.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"a gold {kind} needs a {role}, got {value!r}. {name!r} would be "
                "registered as a table no loader can project a column for"
            )


def _assert_the_key_columns_are_the_dimensions_own(
    name: str, surrogate_key: str, natural_key: str
) -> None:
    """Refuse a key column the loader writes itself, and refuse the two keys being one.

    THE RESERVED SET IS GOLD'S WHOLE ONE, not the subset a conformed dimension writes. A
    conformed dimension has no version chain, so `valid_from` could not overwrite
    anything of its own -- but one gold namespace with one reserved set is what keeps a
    later `unionByName` between a conformed member and an SCD2 version from producing a
    column that means two things depending on the row."""
    for role, value in (("surrogate key", surrogate_key), ("natural key", natural_key)):
        if value in DIMENSION_COLUMNS:
            raise ValueError(
                f"dimension {name!r} names {value!r} as its {role}, and the loader "
                f"writes that column itself ({', '.join(sorted(DIMENSION_COLUMNS))}). "
                "The collision does not crash: one projection writes two values into "
                "one column, so the key is silently a timestamp or a flag and every "
                "fact that joins on it matches nothing"
            )
    if surrogate_key == natural_key:
        raise ValueError(
            f"dimension {name!r} spells {surrogate_key!r} as both its surrogate key and "
            "its natural key. The projection writes both into one column, so one value "
            "survives, every row is still present and every join still resolves"
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


def _assert_the_declared_values_are_a_set(kind: str, name: str, role: str, values: tuple) -> None:
    """Refuse an empty or repeating tuple of declared strings -- members, or roles.

    EMPTY AND REPEATED ARE THE SAME GUARD because they are the same failure at two
    scales: a dimension of nothing but its ghost resolves every fact row to the unknown
    member and reports it as a clean load, and a member declared twice puts two rows on
    one surrogate key so every fact row joining it matches both."""
    if not values:
        raise ValueError(
            f"gold {kind} {name!r} declares no {role}. A dimension holding nothing but "
            "its ghost resolves every fact row to the unknown member, and the load "
            "reports success"
        )
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"gold {kind} {name!r} declares a blank {role}: {values!r}")
        if value in seen:
            raise ValueError(
                f"gold {kind} {name!r} declares {value!r} twice as a {role}: {values!r}. "
                "Two rows on one surrogate key, and every fact row reaching it matches "
                "both -- silently"
            )
        seen.add(value)


@dataclass(frozen=True, kw_only=True)
class EnumeratedDimension:
    """A conformed dimension whose members ARE a value domain the payment contract
    declares: one row per member, no version chain, no source table at all.

    THE MEMBERS ARE THE CONTRACT'S DOMAIN AND NEVER THE OBSERVED VALUES, which is the
    one decision in this class. A dimension built from `SELECT DISTINCT payment_method
    FROM bronze_payments` cannot have an unobserved member -- so its member count and
    its fact-side cardinality are the same number by construction, and the star loses
    the ability to say that four of five rails were used. Taking the domain from
    `opl.contracts.payments` makes the two numbers independent, which is what makes
    `fact_side_cardinality` worth measuring at all.

    `fact_column` IS THE CONTRACT COLUMN THE FACT DRAWS THIS DIMENSION FROM, and
    `natural_key` is what the member column is called HERE. They differ on purpose:
    `dim_channel.channel_code` holds a `payment_method` value, and this spec is the one
    place the rename is written down -- so nothing downstream has to guess which of the
    two names a query means.

    `kw_only`, like every other spec in this repository: `natural_key` and `fact_column`
    are adjacent strings, and a positional construction that swapped them would
    type-check perfectly and produce a dimension whose members were fine and whose
    fact-side measurement counted the wrong column."""

    name: str
    surrogate_key: str
    natural_key: str
    fact_column: str
    members: tuple[str, ...]

    def __post_init__(self) -> None:
        _assert_every_field_is_named(
            "enumerated dimension",
            self.name,
            {
                "name": self.name,
                "surrogate key": self.surrogate_key,
                "natural key": self.natural_key,
                "fact column": self.fact_column,
            },
        )
        _assert_the_key_columns_are_the_dimensions_own(
            self.name, self.surrogate_key, self.natural_key
        )
        _assert_the_fact_column_is_one_the_contract_carries(self.name, self.fact_column)
        _assert_the_declared_values_are_a_set(
            "enumerated dimension", self.name, "member", self.members
        )


@dataclass(frozen=True, kw_only=True)
class CalendarDimension:
    """The date dimension: one row per calendar day over a span DERIVED from the dates
    the star must cover, plus the fact-side foreign keys that play its role.

    WHY THE SPAN IS NOT DECLARED HERE. It has to cover the payment window and both RFB
    `applied_date`s, and every one of those is a value in a table -- so a pair of dates
    in this file would be a second spelling of `2026-06-13` that goes stale the day a
    snapshot lands or a payment profile moves. `opl.gold.conformed` measures the span
    from the fact and from `applied_date_source` instead, which also makes the row count
    a falsifiable PREDICTION rather than an arithmetic restatement of a literal.

    `applied_date_source` IS NOT `Scd2Dimension.source_satellite` AND MUST NOT BE
    SPELLED LIKE IT -- see the module docstring. This dimension reads one DATE COLUMN of
    that satellite and none of its payload; the field name is what makes
    `gold_load_dimension`, handed this spec by a copied YAML, fail on an attribute
    instead of building a plausible SCD2 dimension out of it.

    `roles` ARE THE FACT'S OWN FOREIGN-KEY COLUMN NAMES, one per business date the
    payment contract carries. There is exactly ONE today and that is a finding rather
    than a shortfall; the declaration below argues it."""

    name: str
    surrogate_key: str
    natural_key: str
    fact_column: str
    applied_date_source: str
    roles: tuple[str, ...]

    def __post_init__(self) -> None:
        _assert_every_field_is_named(
            "calendar dimension",
            self.name,
            {
                "name": self.name,
                "surrogate key": self.surrogate_key,
                "natural key": self.natural_key,
                "fact column": self.fact_column,
                "applied-date source": self.applied_date_source,
            },
        )
        _assert_the_key_columns_are_the_dimensions_own(
            self.name, self.surrogate_key, self.natural_key
        )
        _assert_the_fact_column_is_one_the_contract_carries(self.name, self.fact_column)
        _assert_the_declared_values_are_a_set(
            "calendar dimension", self.name, "role", self.roles
        )


# THE TWO CONFORMED KINDS, AS ONE NAME. `opl.gold.conformed`'s loader takes either and
# branches once, on the kind, where the members come from -- so its signature is written
# against this union rather than against a `|` repeated at every function.
ConformedDimension = EnumeratedDimension | CalendarDimension

# THE UNION EVERY GUARD BELOW READS RATHER THAN RESTATES, so a kind added above extends
# each refusal in one edit. It is a union rather than a bare alias exactly so that adding
# `dim_date`'s kind was a word here and not a rewrite of five signatures -- which is what
# it turned out to be.
GoldTable = Scd2Dimension | ConformedDimension
