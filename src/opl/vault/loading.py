# src/opl/vault/loading.py
"""What a hub load and a satellite load have in common: the hash-key expression, the
month window, and the two bronze columns every vault table reads.

ONE SPELLING OF THE HASH KEY, SHARED BY BOTH LOADERS, and this is the reason the file
exists rather than the two loaders each building their own. A satellite's hash key IS
its hub's -- if the two are computed by two expressions, a divergence does not fail,
it produces a satellite that joins to its hub and returns nothing. An empty join is
the quietest wrong answer in this layer: no error, no row count anomaly on either
side, and every downstream query simply reports that the company has no descriptive
history. `hash_key_expression` is called by `load_hub` and by `load_satellite` and
there is no second way to spell it.

THE MONTH WINDOW IS VALIDATED FOR SHAPE HERE AND FOR EXISTENCE ELSEWHERE. `is_month`
is the one spelling of "YYYY-MM naming a real month" in this repository -- the rule
`opl.config` records having been bitten by carrying twice -- so this module asks it
rather than re-deriving it. Whether a well-formed month was ever LOADED is a different
question and nothing here can answer it; the satellite gets that from the observation
ledger, whose `_window` refuses a month with no row on either side (see
`opl.vault.observation` for why that refusal is worth an eager Spark job)."""
from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.bronze.snapshot import SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN
from opl.vault.hashing_spark import hash_key_column, zero_padded_column
from opl.vault.months import validated_months
from opl.vault.registry import Hub

# Bronze's own RSRC column, carried into the vault verbatim rather than re-derived.
#
# A SECOND SPELLING OF A LITERAL, AND CROSS-CHECKED RATHER THAN TRUSTED, in the shape
# `opl.bronze.registry._assert_prefixes_match_their_file_groups` uses: the value is
# written by `opl.bronze.autoloader.add_audit_columns` as a bare string, so there is no
# constant to import, and a rename there would leave this module selecting a column
# that is not there. `tests/vault/test_cnpj_vault.py::test_the_bronze_audit_columns_
# this_layer_reads_are_the_ones_the_ingest_writes` runs that function and asserts the
# three names below are among its output, which turns the duplicate into a cross-check.
BRONZE_RECORD_SOURCE = "_record_source"

# Re-exported so a loader imports its bronze column names from ONE place, and so the
# two that come from `opl.bronze.snapshot` are visibly the same values that module
# defines rather than restated strings.
__all__ = [
    "BRONZE_RECORD_SOURCE",
    "SNAPSHOT_MONTH_COLUMN",
    "SNAPSHOT_REF_DATE_COLUMN",
    "hash_key_expression",
    "read_snapshot_window",
    "rows_in",
]


def rows_in(spark: SparkSession, table: str) -> int:
    """How many rows `table` holds, or 0 if it does not exist yet.

    THE BEFORE/AFTER PAIR BOTH LOADERS REPORT THEIR NUMBERS FROM, and it is a whole-
    table `count()` with no filter on purpose: Delta answers that from the transaction
    log's file statistics rather than by scanning, so the pair costs no pass over the
    data. Counting the frame about to be written instead would mean either recomputing
    an anti-join and a window, or caching a frame that on a first load is the entire
    69M-row table.

    What it buys beyond cost: `appended` is then what LANDED, not what was planned, so
    there is no state in which a result object reports a write that did not happen."""
    if not spark.catalog.tableExists(table):
        return 0
    return spark.read.table(table).count()


def hash_key_expression(hub: Hub) -> Column:
    """The hub's hash key, as a Column over the source's business-key columns.

    Zero-padding is applied per column and only where the spec declares a width, which
    is `BusinessKeyColumn`'s decision to carry: padding a name or a free-text
    identifier would invent characters, where padding `cnpj_basico` to eight recovers
    a leading zero a source may have dropped. `zero_padded_column` fails the query on
    an overlong value rather than truncating it onto another entity's true key.

    ORDER IS THE SPEC'S DECLARATION ORDER, and it is not incidental: the standard
    joins components with `||` and length-prefixes each, so two orders give two
    different digests for the same business key. The spec is where that order is
    written down once."""
    return hash_key_column([
        F.col(key.name) if key.width is None
        else zero_padded_column(F.col(key.name), width=key.width)
        for key in hub.business_keys
    ])


def _validated_months(months: Sequence[str] | None) -> tuple[str, ...] | None:
    """The window, or `None` for "every month the source holds".

    Shares `opl.vault.months.validated_months` with the observation ledger rather than
    restating its three refusals; only the CONSEQUENCE differs, and it differs for a
    reason worth keeping. Every refusal there produces a load that writes NOTHING and
    reports success, which is the failure shape this layer is least able to notice: a
    vault table that gained no rows looks exactly like a vault table that had nothing
    to gain."""
    return validated_months(
        months,
        column=SNAPSHOT_MONTH_COLUMN,
        consequence="An empty or unmatched window loads nothing and reports success",
    )


def read_snapshot_window(
    spark: SparkSession, table: str, months: Sequence[str] | None
) -> DataFrame:
    """`table`, narrowed to `months`, or the whole table when `months` is None.

    `None` IS THE DEFAULT EVERY CALLER SHOULD PREFER. A vault load is idempotent by
    hash key, so re-reading a month already loaded costs a scan and changes nothing,
    where omitting one leaves a hole that only a later full reload closes."""
    frame = spark.read.table(table)
    window = _validated_months(months)
    if window is None:
        return frame
    if SNAPSHOT_MONTH_COLUMN not in frame.columns:
        raise ValueError(
            f"{table!r} has no {SNAPSHOT_MONTH_COLUMN} column, so a month window "
            "cannot be applied to it. Every bronze table carries one; a source that "
            "does not is not a monthly snapshot and must be loaded whole"
        )
    return frame.filter(F.col(SNAPSHOT_MONTH_COLUMN).isin(list(window)))
