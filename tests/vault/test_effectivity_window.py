"""What a window too narrow to close an effectivity window actually does -- measured,
and pinned as a PROPERTY OF THE WINDOW rather than as a bug.

WHY THIS IS ITS OWN FILE, and it is not the line count. `test_socios_vault.py` is at 791
of this project's 800-line cap and its subject is the satellite's MECHANIC -- which
window closes, which does not, what the tie-break picks, what the mask returns. The
subject here is one ARGUMENT: what `months` does to a load that derives its output from
absence. Two reasons to edit are two files, and this one changes when the job layer's
window policy changes, which is the axis F2's workspace run grows along.

THE PROPERTY, AND WHY IT IS TRUE FOR ANY DATA RATHER THAN FOR THIS FIXTURE.
`opl.vault.observation.observation_ledger` builds its key universe from the same window
it reports on -- "Keys seen only outside the window are not reported at all; narrowing
`months` narrows both axes" (`observation.py:401`). So over ONE month, every key the
ledger knows about is a key that month's bronze or quarantine showed, and it is
therefore PRESENT in the only month it is asked about. No key can reach
`absent_before_first_observation` (its first observation is that month) and none can
reach `absent_after_observation` (it is in the evidence for that month).
`opl.vault.effectivity.CLOSING_STATE` is `absent_after_observation` and nothing else.
A one-month window therefore closes exactly ZERO windows, always, whatever bronze holds.
`test_a_one_month_ledger_reports_no_absence_at_all` asserts the general statement; the
rest of this file asserts what it costs.

WHY THAT ZERO IS DANGEROUS RATHER THAN MERELY TRUE. Zero is also what a correct
incremental load with no departures prints, so the number cannot tell the two apart --
and F2 wave 1's sharpest acceptance figure is 65,444 closed windows over 2026-06 and
2026-07. Run as two single-month loads it is 0 and 0, both green, with a satellite that
is a row short and nothing anywhere saying so. `databricks/src/vault_load_effectivity.py`
REFUSES a one-month window before Spark for exactly this reason; that refusal is pinned
in `tests/test_vault_job_wiring.py`, and this file is the measurement it rests on.

NOTHING HERE IS A DEFECT IN A LOADER. Every count below is the code behaving exactly as
its own docstrings describe. That is the point: a test asserting a zero has to say
whether the zero is right, and this one is."""
from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from opl.vault import domains
from opl.vault.columns import APPLIED_DATE, IS_ACTIVE
from opl.vault.effectivity import load_effectivity_satellite
from opl.vault.hashing import hash_key
from opl.vault.observation import STATE_COLUMN, ObservationState, observation_ledger

from .conftest import JUL, JUN, LOADED_AT, R_DEPARTS, RELOADED_AT

LINK = domains.table_spec("link_company_partner")
EFF = domains.table_spec("sat_eff_company_partner")
LINK_HUBS = domains.linked_hubs(LINK)

# The three states a ledger can report about a month it has evidence for. Their
# complement -- the two absences -- is what a one-month window cannot produce.
PRESENT_STATES = frozenset(
    {
        ObservationState.OBSERVED.value,
        ObservationState.OBSERVED_WITH_REJECTED_SIBLINGS.value,
        ObservationState.REJECTED_BY_OUR_GATE.value,
    }
)


def _fresh(db: str) -> str:
    """A table name no other test in this module writes -- sharing one would make an
    incremental load's second step read the first step's neighbours."""
    return f"{db}.eff_window_{uuid4().hex[:8]}"


def _load(spark, source, table: str, *, months, load_date=LOADED_AT):
    """One effectivity load over `months`.

    THE LINK IS NOT LOADED FIRST, and that is not a shortcut: `load_effectivity_
    satellite` reads bronze and keys through `link_hash_key_expression`, never joining
    to the link table, so the link is irrelevant to every question this file asks. The
    job YAML still orders them, for the reason it gives there -- a satellite asserting a
    window for a relationship the link does not carry is a dangling reference."""
    return load_effectivity_satellite(
        spark, EFF, link=LINK, hubs=LINK_HUBS, source_table=source.bronze,
        target_table=table, load_date=load_date, grain=source.grain, months=months,
    )


def _rows(spark, table: str) -> list[SimpleNamespace]:
    return [
        SimpleNamespace(key=row[LINK.hash_key], applied=row[APPLIED_DATE],
                        active=row[IS_ACTIVE])
        for row in spark.read.table(table).collect()
    ]


def _departed_key() -> str:
    """`R_DEPARTS`' link hash key, by the PURE spelling of the standard -- so the
    assertion about WHICH row is missing is tied to `opl.vault.hashing` rather than to
    the Spark expression that is under test."""
    company, kind, partner = R_DEPARTS
    return hash_key([company.zfill(8), kind, partner])


@pytest.mark.parametrize("month", [JUN, JUL])
def test_a_one_month_ledger_reports_no_absence_at_all(spark, socios_source, month):
    """THE MECHANISM, asserted at the ledger rather than at the satellite, because it is
    the general statement: over one month the key universe and the reported month are
    derived from the same evidence, so every key in the ledger is present in the only
    month it is asked about.

    Both months are checked and they fail differently if this ever stops holding: June
    contains a relationship that departs and one our own gate rejects in July, and July
    contains a relationship born in it. Neither produces an absence when it is the whole
    window."""
    states = {
        row[STATE_COLUMN]
        for row in observation_ledger(spark, socios_source.grain, months=[month]).collect()
    }
    assert states <= PRESENT_STATES, (
        f"a ledger over the single month {month} reports {sorted(states - PRESENT_STATES)}. "
        "That would mean a key absent from the only month it was asked about, which the "
        "derivation cannot produce -- and the whole argument for the job layer's window "
        "guard rests on it not being able to"
    )


@pytest.fixture(scope="module")
def windows(spark, socios_source):
    """THE TWO ROUTES THROUGH JULY, loaded once and read by every test below.

    Both start from the same persisted June, so the only difference between them is the
    second load's `months` argument -- which is the whole subject of this file:

      NARROW  June, then July alone      -- what an operator types from muscle memory,
                                            every other job in this repo taking a
                                            singular `month`.
      WIDE    June, then June and July   -- what the phase's 65,444 needs.

    MODULE-SCOPED, AND THAT IS A COST DECISION TAKEN DELIBERATELY. Four loads is the
    fewest that can state the comparison, and each one derives the ledger, the dedup and
    two windows from Delta: run per-test these four became nine and this file cost 191 s
    of a 600 s chunk cap on a quiet box. `estab_loaded` and `test_socios_vault.py`'s
    `loaded` are module-scoped for the same reason, and the same rule applies -- nothing
    below writes."""
    narrow = _fresh(socios_source.db)
    wide = _fresh(socios_source.db)
    return SimpleNamespace(
        narrow=narrow,
        wide=wide,
        june=_load(spark, socios_source, narrow, months=[JUN]),
        july=_load(spark, socios_source, narrow, months=[JUL], load_date=RELOADED_AT),
        wide_june=_load(spark, socios_source, wide, months=[JUN]),
        both=_load(spark, socios_source, wide, months=[JUN, JUL], load_date=RELOADED_AT),
    )


def test_a_june_only_window_closes_zero_windows_because_there_is_no_later_month(windows):
    """The unsurprising zero, pinned separately so the pair reads as "both, and not for
    one shared cause".

    Here there is no later month for a key to be absent from at all. This is exactly why
    the surprising one below goes unnoticed: an operator who runs June, sees 0, runs
    July and sees 0 has seen the expected answer once and the wrong answer once, in the
    same shape.

    Asserted on BOTH June loads, because the two routes have to start from an identical
    state or the row-count comparison at the end of this file compares two things."""
    assert (windows.june.appended, windows.june.closed) == (8, 0)
    assert (windows.wide_june.appended, windows.wide_june.closed) == (8, 0)


def test_a_july_only_window_closes_zero_windows_even_after_june_was_loaded(windows):
    """THE ONE THAT IS SURPRISING, and the reason `months` is a LIST.

    June is already persisted, so the satellite HAS a prior observation of the departed
    relationship to close. It still closes nothing: the close comes from the ledger, the
    ledger over `['2026-07']` never sees a June-only key at all, and there is no
    `absent_after_observation` row for it to gate on. Persisting June first does not
    help, and nothing about a wider satellite would -- the narrowing is in the window,
    not in the table.

    In the real data this is the 65,444: keys present in 2026-06 and absent from
    2026-07. Run this way, the job reports `closed=0` and SUCCEEDS."""
    assert windows.july.closed == 0, (
        "a July-only window closed a window; if this is ever true the ledger has stopped "
        "narrowing its key universe with its window, and vault_load_effectivity.py's "
        "refusal of a one-month window is refusing something that would have worked"
    )
    # The relationship born in July, opened. So the load is not a no-op -- it appends,
    # and reports success, and is simply missing the one row that was the point.
    assert windows.july.appended == 1


def test_a_two_month_window_is_what_closes_the_window(spark, windows):
    """The control, and the whole difference from the two above is the argument. Same
    fixture, same loader, same June already persisted."""
    assert windows.both.closed == 1
    closing = [row for row in _rows(spark, windows.wide) if not row.active]
    assert [row.key for row in closing] == [_departed_key()]


def test_two_single_month_runs_leave_the_satellite_a_row_short_and_report_success(
    spark, windows
):
    """WHAT THE ZERO COSTS, stated as the difference between two tables rather than as a
    count nobody compares.

    Two single-month loads produce NINE rows and zero closes; June-then-both produces TEN
    and one close. The missing row is the close for the departed relationship, so the
    satellite says that relationship is still active -- a live wrong answer in the data,
    not a missing log line. Both runs are green.

    IT IS RECOVERABLE, AND THAT IS NOT A MITIGATION. A later load over both months does
    append the close (`changed_rows` seeds from what is persisted), so nothing is lost
    forever. What is lost is the phase's own acceptance step: the run that was supposed
    to turn "should close 65,444" into "closed 65,444" reported 0 and 0 and finished."""
    narrow_rows = _rows(spark, windows.narrow)

    assert (len(narrow_rows), windows.july.closed) == (9, 0)
    assert (len(_rows(spark, windows.wide)), windows.both.closed) == (10, 1)
    assert _departed_key() not in {row.key for row in narrow_rows if not row.active}
