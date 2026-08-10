# databricks/src/vault_load_effectivity.py
"""Job task: load one DV2 effectivity satellite on a link, over one month window -- and
the one vault task that REFUSES a window a run could otherwise report success on.

THE TENSION THIS FILE EXISTS TO CLOSE, and it is worth stating before anything else,
because the failure it guards is a GREEN run.

  `opl.vault.observation.observation_ledger` derives its key universe from the same
  window it reports on: "Keys seen only outside the window are not reported at all;
  narrowing `months` narrows both axes" (`observation.py:401`). With ONE month in the
  window, every key in the universe is a key that month's bronze or quarantine showed,
  so every key is present in the only month it is asked about and NO key can reach an
  absent state at all. `opl.vault.effectivity.CLOSING_STATE` is
  `absent_after_observation`. Therefore a one-month window closes ZERO windows, for any
  data, always.

  That is not a corner case here. F2 wave 1 predicts this table closes exactly 65,444
  windows: 65,444 relationships present in 2026-06 and absent from 2026-07. With
  `months=2026-07` alone those keys are not in the universe, so zero close. With
  `months=2026-06` alone there is no later month to be absent from, so zero close.
  TWO SEQUENTIAL SINGLE-MONTH RUNS GIVE 0 AND 0, and both report success, and 0 is what
  a clean run looks like. `tests/vault/test_effectivity_window.py` pins that zero
  against the fixture so it is a documented property of the window rather than a bug
  someone will one day "fix".

THE GUARD, AND WHY IT IS A REFUSAL AND NOT A WARNING. `_refuse_a_window_that_cannot_
close` rejects a window naming fewer than two months, before Spark. The three
candidates were:

  - REFUSE. Taken. The refusal is about the ARGUMENT and not about the data: the window
    is structurally incapable of producing this table's defining output, whatever bronze
    holds, so refusing cannot be wrong about a run that would have worked.
  - WARN LOUDLY IN THE LOG. Rejected. This project's own record is that a green run
    carrying a log line goes unread -- on 2026-08-01 a socios re-run terminated SUCCESS
    having masked only bronze, and it was found by a human reading a task log against
    the source days later. `opl.vault.satellites` states the rule this follows: a guard
    that cannot fire is worse than none, because the next reader believes the hole is
    closed. A warning is one step above silence and sits on the same side of it.
  - REPORT A COUNT AN OPERATOR MUST READ. Rejected because it already exists.
    `EffectivityLoadResult.closed` is returned, and this task prints it. Zero is exactly
    what it prints on a clean incremental load with no departures, so the number cannot
    distinguish "nothing departed" from "nothing COULD depart".

WHAT THE REFUSAL COSTS, stated so it is a decision and not an oversight. An operator
who wants only one month's link rows still gets them: the link is a separate task with
its own entry point and is not touched by this guard. Only this task refuses, before a
session starts, so the cost is one task start and a relaunch with a wider window -- and
the link's re-load is idempotent by anti-join on its own hash key, so nothing is spent
twice except a scan.

WHY NO OTHER VAULT TASK CARRIES THIS GUARD. Every other loader derives its output from
PRESENCE -- a hub row, a link row, a changed payload, a reference code -- and a
one-month window loads that month's rows exactly right. This is the only table whose
output is derived from ABSENCE, and absence is the one thing a one-month ledger cannot
express. `vault_load_satellite.py` annotates the same ledger's departure count rather
than refusing, for that reason and it says so -- and since that count became optional
there (`report_diagnostics`, default off) it may not be reported at all, which is
another reason the guard cannot be a number an operator is expected to read. THIS task
takes no such flag: `closed` is counted from the frame it writes, so it costs no extra
pass, and it is this table's output rather than a diagnostic.

THE SENTINEL DEFAULT IS KEPT AND ITS ORIGINAL RATIONALE DOES NOT TRANSFER. Bronze's
`month` default is a sentinel because a real month there re-ingests against an empty
month-scoped checkpoint and silently DOUBLES the row counts -- a stamping argument, and
the vault stamps nothing: bronze writes `_snapshot_month` onto rows and this layer only
reads it. The reason to keep it here is different and is its own: a forgotten `--params`
would otherwise become a silent load of a window nobody chose. See
`opl.vault.job_params`.

argv: [table, source, months, load_date]"""
import sys

from pyspark.sql import SparkSession

from opl.bronze.registry import BronzeTable
from opl.bronze.registry import table_spec as bronze_table_spec
from opl.config import DEFAULT
from opl.vault import domains
from opl.vault.effectivity import load_effectivity_satellite
from opl.vault.job_params import required_load_date, required_months, required_spec
from opl.vault.observation import ObservationGrain
from opl.vault.registry import EffectivitySatellite, Link

# The narrowest window in which a key can be absent from a month the ledger reports on:
# one month to be seen in, one later month to be missing from. Named rather than
# inlined as `< 2`, because it is the whole content of the guard below.
MONTHS_A_CLOSE_NEEDS = 2


def grain_for(link: Link, source: BronzeTable) -> ObservationGrain:
    """The observation grain for `link` over `source`'s bronze/quarantine pair.

    AT LINK GRAIN AND NOT AT HUB GRAIN, which is the distinction this table lives or
    dies on: a partner who loses one of two partnerships is `absent_after_observation`
    at LINK grain and plainly `observed` at hub grain, so a hub-grain ledger would
    report no departure and the window would stay open forever
    (`opl.vault.effectivity`). `domains.link_identity_columns` derives the columns from
    the link's own spec, in hash order, which is exactly what
    `_refuse_a_mismatched_link_grain` compares against."""
    return ObservationGrain.in_default_schema(
        name=link.name,
        bronze=source.bronze,
        quarantine=source.quarantine,
        key_columns=domains.link_identity_columns(link),
    )


def _refuse_a_window_that_cannot_close(months: tuple[str, ...], table: str) -> None:
    """Refuse a window in which no key can be absent -- see the module docstring.

    BEFORE SPARK, like every other argument refusal in `databricks/src`: nothing about
    counting the months in a job parameter needs a serverless session to diagnose, and a
    session started here would be a session spent producing the zero."""
    if len(months) >= MONTHS_A_CLOSE_NEEDS:
        return
    raise ValueError(
        f"refusing to load {table}: months={list(months)} names one month, and an "
        "effectivity satellite closes a window on absence. The observation ledger builds "
        "its key universe from the same window it reports on, so over one month every "
        "key it knows about is present in the only month it is asked about and NO key "
        "can reach absent_after_observation -- the one state that closes a window. This "
        "load would therefore append the window OPENS for that month, close exactly 0 "
        "windows whatever bronze holds, and report success; 0 is also what a clean "
        "incremental load with no departures prints, so the number cannot tell you which "
        "happened. F2 wave 1 expects 65,444 closes over 2026-06 and 2026-07, and two "
        "separate single-month runs of this task would give 0 and 0. Pass at least "
        f"{MONTHS_A_CLOSE_NEEDS} months: --params months=2026-06+2026-07"
    )


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    spec = required_spec(
        args[0] if args else "", EffectivitySatellite, loader="vault_load_effectivity"
    )
    source = bronze_table_spec(args[1] if len(args) > 1 else "")
    months = required_months(args[2] if len(args) > 2 else "", action=f"load {spec.name}")
    _refuse_a_window_that_cannot_close(months, spec.name)
    load_date = required_load_date(args[3] if len(args) > 3 else "")
    link = domains.parent_link(spec)
    spark = SparkSession.builder.getOrCreate()
    result = load_effectivity_satellite(
        spark,
        spec,
        link=link,
        hubs=domains.linked_hubs(link),
        source_table=DEFAULT.table(source.bronze),
        target_table=DEFAULT.table(spec.name),
        load_date=load_date,
        grain=grain_for(link, source),
        months=list(months),
    )
    # `closed` leads, because it is the number this phase exists to turn from "should"
    # into "did", and because every one of them is a DERIVED claim: the source never told
    # us a relationship ended, we inferred it from an absence our own DQ gate did not
    # cause.
    print(
        f"vault_load_effectivity: {result.table} +{result.appended} rows from "
        f"{DEFAULT.table(source.bronze)} over {list(months)}, of which {result.closed} "
        f"CLOSE a window (derived, gated on absent_after_observation); the target "
        f"already held {result.already_present} rows (whole table, not this window); "
        f"{result.collapsed_duplicates} source rows were folded into a row sharing "
        "their (link key, month), each discarding one delivered entry date"
    )


if __name__ == "__main__":
    main()
