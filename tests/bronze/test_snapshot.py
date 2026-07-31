"""The snapshot reference date is DERIVED and CROSS-CHECKED, never guessed.

The RFB names its inner files `...D<Y><MM><DD>...` -- `D60613` for 2026-06,
`D60711` for 2026-07. That single year digit is ambiguous across decades, so the
year comes from the job's month parameter and only MM/DD come from the token; a
token whose month disagrees with the folder, or whose year digit does not close,
yields NULL rather than a guess. Two observations of this format is two, which is
exactly why it refuses instead of assuming."""
from __future__ import annotations

import datetime as dt

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from opl.bronze.snapshot import ref_date_column  # noqa: E402

_ROOT = "/Volumes/workspace/default/landing/cnpj"
ESTAB = f"{_ROOT}/2026-06/estabelecimentos/K3241.K03200Y0.D60613.ESTABELE"
LOOKUP = f"{_ROOT}/2026-06/lookups/F.K03200$Z.D60613.CNAECSV"
JULY = f"{_ROOT}/2026-07/estabelecimentos/K3241.K03200Y1.D60711.EMPRECSV"


def _derive(spark, path: str, month: str):
    df = spark.createDataFrame([(path,)], "src string")
    return df.select(ref_date_column(F.col("src"), month).alias("d")).collect()[0]["d"]


def test_it_derives_the_declared_date_from_an_estabelecimentos_filename(spark):
    assert _derive(spark, ESTAB, "2026-06") == dt.date(2026, 6, 13)


def test_it_derives_the_declared_date_from_a_lookup_filename(spark):
    """Same token, a different filename shape -- the dollar sign in F.K03200$Z
    must not disturb the match."""
    assert _derive(spark, LOOKUP, "2026-06") == dt.date(2026, 6, 13)


def test_it_derives_a_second_month(spark):
    assert _derive(spark, JULY, "2026-07") == dt.date(2026, 7, 11)


def test_a_token_month_disagreeing_with_the_folder_is_null(spark):
    """A June file sitting in the July folder. This is the disagreement the two
    columns exist to make visible -- it must NOT be silently resolved."""
    assert _derive(spark, ESTAB, "2026-07") is None


def test_a_token_year_digit_disagreeing_with_the_folder_is_null(spark):
    """2025 ends in 5, the token says 6: the year digit does not close.

    The brief wrote this case as `2016-06`, which does NOT disagree -- 2016 and
    2026 both end in 6, the very ambiguity the derivation is built around. That
    month is pinned in its own test below, as the limitation it actually is."""
    assert _derive(spark, ESTAB, "2025-06") is None


def test_the_decade_is_the_job_parameters_word_and_nothing_can_contradict_it(spark):
    """The one input this cannot cross-check, pinned so it stays deliberate.

    A single year digit cannot distinguish 2016 from 2026, so a job run with the
    wrong decade gets a date in that decade rather than NULL. There is no second
    source of truth for the year in the filename, so refusing here would mean
    refusing every row. What keeps it honest is the OTHER column:
    `_snapshot_month` carries the same wrong month, so the operator error is
    visible in the row instead of buried in a derivation."""
    assert _derive(spark, ESTAB, "2016-06") == dt.date(2016, 6, 13)


def test_a_filename_with_no_token_is_null(spark):
    assert _derive(spark, "/Volumes/x/2026-06/lookups/Cnaes.csv", "2026-06") is None


def test_a_token_in_a_DIRECTORY_name_does_not_win_over_the_filename(spark):
    """The silent-wrong-date case: a token that cross-checks CLEAN but is wrong.

    A directory carrying a token is the leftmost match in the PATH, and its month
    agrees with the folder, so the year/month cross-check cannot catch it -- the
    row would claim the 1st when the file declares the 13th. Extraction is
    confined to the last path segment for exactly this.

    NOTE the `batch.` before the token. The first version of this test wrote the
    directory as `D60601.reprocess`, where the token is preceded by `/` and not
    by the `.` the pattern requires -- so neither regex matched it, both returned
    the 13th, and the test passed against the very code it was meant to indict.
    Deleting `_LAST_SEGMENT` from `_TOKEN` now turns this red; that was run."""
    path = "/Volumes/x/2026-06/batch.D60601.reprocess/K3241.K03200Y0.D60613.ESTABELE"
    assert _derive(spark, path, "2026-06") == dt.date(2026, 6, 13)


def test_two_tokens_in_one_filename_are_null_rather_than_leftmost_wins(spark):
    """The same wrong-date class as the directory case, one level in.

    Both tokens sit in the last segment, so confining the match to the filename
    does not help, and both cross-check clean against 2026-06 -- leftmost-wins
    would stamp the 1st while the file also declares the 13th. Which one the RFB
    meant is not decidable from the name.

    NULL, not a pick: both months observed ship exactly one token, so two is a
    naming shape nobody has seen, and this is the module whose entire premise is
    that a wrong applied_date costs more than a missing one. Pinned rather than
    left to the regex engine's leftmost-first habit, which is not a decision."""
    two = "/Volumes/x/2026-06/estabelecimentos/K3241.D60601.K03200Y0.D60613.ESTABELE"
    assert _derive(spark, two, "2026-06") is None


def test_an_impossible_day_is_null_not_a_shifted_date(spark):
    """Spark's to_date must not roll 2026-06-31 forward to 07-01."""
    bad = "/Volumes/x/2026-06/estabelecimentos/K3241.K03200Y0.D60631.ESTABELE"
    assert _derive(spark, bad, "2026-06") is None


def test_an_impossible_day_is_null_under_the_LEGACY_time_parser_too(spark):
    """The OUTCOME under both time-parser policies, since one is a cluster setting.

    What this pins, precisely: an impossible day yields NULL whether the cluster
    runs CORRECTED (Spark 3's default) or LEGACY. It does NOT prove the
    `dayofmonth` cross-check in `ref_date_column` does the work -- measured on
    pyspark 3.5.9, LEGACY refuses 2026-06-31 by itself, exactly as CORRECTED does:

        policy_set=CORRECTED readback=CORRECTED to_date_fmt=None ...
        policy_set=LEGACY    readback=LEGACY    to_date_fmt=None ...

    (also NULL for `to_date` with no format, `to_timestamp`, and `yyyyMMdd`.) So
    the lenient-parser roll the cross-check defends against is not reachable on
    this version, and no test here closes that guard. Said plainly rather than
    dressed up as a probe: a test that asserts a guard it cannot trigger is the
    vacuous kind this suite already shipped once."""
    previous = spark.conf.get("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    try:
        bad = "/Volumes/x/2026-06/estabelecimentos/K3241.K03200Y0.D60631.ESTABELE"
        assert _derive(spark, bad, "2026-06") is None
        # And the good path still parses under LEGACY -- a policy that broke the
        # normal case would otherwise hide behind the NULL above.
        assert _derive(spark, ESTAB, "2026-06") == dt.date(2026, 6, 13)
    finally:
        spark.conf.set("spark.sql.legacy.timeParserPolicy", previous)


@pytest.mark.parametrize(
    "bad",
    [
        "2026-6",
        "202606",
        "",
        "2026",
        "june-06",
        "2026-06-13",
        # Shape-valid, month impossible: these clear a length-and-digits check and
        # then match no folder on earth, so every row would be NULL -- the exact
        # outcome the ValueError exists to prevent, reached through the guard.
        "2026-13",
        "2026-00",
    ],
)
def test_a_malformed_month_parameter_raises_instead_of_building_a_null_column(spark, bad):
    """Loud while the plan is being BUILT, before a single row is read. An
    all-NULL column is what a malformed job parameter would otherwise produce,
    and that reads exactly like a source whose filename format changed -- two
    very different incidents, one symptom.

    Takes `spark` only because `F.col` needs a live JVM to build a Column at
    all; nothing here executes a query."""
    with pytest.raises(ValueError, match="YYYY-MM"):
        ref_date_column(F.col("src"), bad)
