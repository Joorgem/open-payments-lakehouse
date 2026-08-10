# src/opl/vault/job_params.py
"""What a VAULT JOB parameter must be, refused before Spark: the window, the load
timestamp, and the registered table an entry point was handed.

WHY THIS IS NOT `opl.config.require_month`, WHICH IS THE SAME IDEA FOR BRONZE. A
bronze task takes ONE month and STAMPS it into `_snapshot_month`; a vault task takes a
WINDOW and only READS that column. So the bronze refusal argues about a value written
into the data and into a delete boundary, and none of that transfers. What does
transfer is the shape: a job parameter must have some default, no window is a valid
one, and so the default's whole job is to fail.

WHY NOT IN `opl.vault.months` EITHER, which already owns "which months was I asked
for". That module is the LIBRARY's window rule -- thirty lines, pure, shared by the
observation ledger and by every loader through `opl.vault.loading`. What is here is
the JOB's rule: a separator, a sentinel default, a rejection of a repeated month, and
a load timestamp. `validated_months` is CALLED rather than re-spelled, so the three
refusals it owns (a bare str, an empty sequence, a value that is not a month) stay in
one place -- which is the defect `opl.config` records having been bitten by.

THE LIBRARY DEFAULT IS `None` AND A JOB DOES NOT INHERIT IT.
`opl.vault.loading.read_snapshot_window` says `None` -- every month the source holds --
is what every in-process caller should prefer, because a vault load is idempotent by
hash key, so re-reading a loaded month costs a scan and changes nothing while omitting
one leaves a hole. That argument is about a caller who already decided what to load. A
JOB is not that caller: `None` there is an unparameterised full-table scan of bronze,
cheap today at 72.3M rows and unbounded the month after, and it is also exactly the
shape an operator gets by forgetting `--params`. So every entry point REQUIRES a
window and the job-parameter default is a value this module refuses.

A REPEATED MONTH IS REFUSED, AND THAT IS NOT TIDINESS. `observation._window` folds a
duplicate away and the ledger answers the same, so nothing in the library cares. What
cares is `databricks/src/vault_load_effectivity.py`, which refuses a window too narrow
to close any effectivity window and measures narrowness by COUNTING the months it was
given. `months=2026-07+2026-07` is two entries and one month: admitted, it would walk
straight past that guard into the zero-closes load the guard exists to stop. A guard
whose input can be inflated by a typo is a guard that can be bypassed by one."""
from __future__ import annotations

from datetime import datetime
from typing import TypeVar

from opl.bronze.snapshot import SNAPSHOT_MONTH_COLUMN
from opl.config import SENTINEL_MONTH
from opl.vault import domains
from opl.vault.months import validated_months
from opl.vault.specs import VaultTable

_T = TypeVar("_T", bound=VaultTable)

# THE SEPARATOR BETWEEN MONTHS IN A JOB PARAMETER, AND IT IS NOT A COMMA FOR A REASON
# AN OPERATOR MEETS ON THE FIRST LAUNCH. `databricks bundle run --params` is a pflag
# `stringToString`, documented in `--help` as "comma separated k=v pairs": a value
# containing a comma is split by the CLI itself, so
# `--params months=2026-06,2026-07,revision=<sha>` arrives as three pairs, the middle
# one carrying no `=` at all. There IS a CSV-quoting escape, and it is exactly the kind
# of thing nobody remembers under an incident. `+` is special to neither the CLI nor
# bash, so the documented launch command needs no quoting:
#
#     databricks bundle run opl_vault_cnpj_partner -t free \
#       --params months=2026-06+2026-07,revision=$(git rev-parse HEAD)
MONTH_SEPARATOR = "+"

# The `{{...}}` reference the job YAMLs hand every load task as its `load_date`. Named
# here so the refusal below can print it: an unresolved reference arrives as this
# literal string, which is the one wrong value worth diagnosing by name.
LOAD_DATE_REFERENCE = "{{job.start_time.iso_datetime}}"

# THE TWO SPELLINGS A BOOLEAN JOB PARAMETER MAY TAKE, and there are two rather than the
# usual grab-bag of `1/yes/on/y` because a job parameter is written once in a YAML and
# read by `optional_flag` alone. Every additional accepted spelling is a value a reader
# has to know is accepted; every REJECTED one is diagnosed by the message below.
_FLAG_VALUES = {"true": True, "false": False}


def required_months(value: str | None, *, action: str) -> tuple[str, ...]:
    """The window `action` will load, or refuse. Never `None`, never empty.

    ABSENCE IS REFUSED, NOT DEFAULTED, for `opl.config.require_month`'s reason and not
    for its consequence. A vault loader handed no window would read the whole of
    bronze and report success, which is not wrong in the data -- it is the widest
    correct answer -- and that is precisely why it must not be the silent one: it is
    indistinguishable in the log from the window the operator meant to pass, and it
    grows without bound as months accumulate.

    THE SENTINEL IS THE SHAPE AN OPERATOR ACTUALLY HITS. `SENTINEL_MONTH` is what the
    vault job YAMLs default `months` to, so reaching here with it means the run was
    launched without `--params months=...`. It is refused as an ABSENCE rather than as
    a malformed value -- which is the truth of it -- so the message names the missing
    parameter instead of explaining what a month looks like. Its wording is singular
    because it is ONE sentinel shared with the four bronze ingestion jobs, and a second
    spelling of a sentinel is a default that drifts into a value nobody checked."""
    candidate = (value or "").strip()
    if not candidate or candidate == SENTINEL_MONTH:
        raise ValueError(
            f"refusing to {action}: no months were given, and there is no default to "
            "fall back on. The window is the ONE thing that decides which snapshots this "
            "load reads; an omitted one would read every month bronze holds and report "
            "success, which is indistinguishable in the log from the window that was "
            f"meant. Pass at least one month, separated by {MONTH_SEPARATOR!r} -- in the "
            "job YAMLs that is {{job.parameters.months}}, e.g. "
            f"--params months=2026-06{MONTH_SEPARATOR}2026-07"
        )
    window = tuple(part.strip() for part in candidate.split(MONTH_SEPARATOR))
    # CALLED FOR ITS THREE REFUSALS, which are `opl.vault.months`' to own -- a bare str,
    # an empty sequence, a value that is not a month. Its return for a non-None argument
    # is `tuple(months)`, which is `window` itself, so nothing usable is discarded here.
    validated_months(
        window,
        column=SNAPSHOT_MONTH_COLUMN,
        consequence=(
            "An empty or unmatched window makes this load write nothing and report "
            "success, and a vault table that gained no rows looks exactly like a vault "
            f"table that had nothing to gain. Refusing to {action}"
        ),
    )
    _refuse_a_repeated_month(window, action)
    return window


def _refuse_a_repeated_month(months: tuple[str, ...], action: str) -> None:
    """A month named twice is a typo, and it INFLATES the window's length.

    See the module docstring: the effectivity entry point's guard counts months, so
    admitting `2026-07+2026-07` would let a typo carry a one-month window past a guard
    that exists to refuse exactly that."""
    repeated = sorted({month for month in months if months.count(month) > 1})
    if repeated:
        raise ValueError(
            f"refusing to {action}: months names {repeated} more than once ({list(months)}). "
            "The ledger folds a duplicate away and answers the same, so this is refused "
            "for the OTHER reader of this value: the effectivity load measures whether a "
            "window is wide enough to close anything by counting the months it was given, "
            "and a repeat would carry a one-month window past that check"
        )


def optional_flag(value: str | None, *, parameter: str) -> bool:
    """A boolean job parameter, DEFAULTING OFF when the task was handed nothing.

    THE ASYMMETRY WITH `required_months` IS THE POINT, because everything else in this
    module refuses an absent value and this one does not. A missing window has no safe
    default: every candidate is a load nobody chose, and the widest is silent, unbounded
    and indistinguishable in the log from the window that was meant. A missing FLAG has
    a default that is both cheap and honest -- the work is skipped and the result says
    `None`, which `opl.vault.satellites.SatelliteLoadResult` keeps distinct from a
    measured 0. Nothing is claimed that was not measured, so nothing is lost by
    defaulting, and requiring it would only make every launch carry a parameter to say
    "as usual".

    A VALUE NOBODY CAN READ IS STILL REFUSED, rather than falling back on that default,
    and this is where a `bool(value)` shortcut would be actively harmful. Anyone passing
    `report_diagnostics=yes` is asking for the diagnostics; read as off, their run comes
    back with `None` in both fields -- byte-identical to the run they were trying not to
    launch -- and read by `bool` the string `"false"` would be TRUE, which is the same
    mistake in the other direction and costs hours rather than a relaunch."""
    candidate = (value or "").strip().lower()
    if not candidate:
        return False
    if candidate not in _FLAG_VALUES:
        raise ValueError(
            f"job parameter {parameter}={value!r} is not a boolean. Pass one of "
            f"{sorted(_FLAG_VALUES)} -- e.g. --params months=2026-06"
            f"{MONTH_SEPARATOR}2026-07,{parameter}=true. It is refused rather than "
            "read as the default, because a value the parser cannot read is an "
            "operator who asked for something, and defaulting would hand them back "
            "exactly the run they were trying not to launch"
        )
    return _FLAG_VALUES[candidate]


def required_load_date(value: str | None) -> datetime:
    """The DV2 load timestamp, taken from the RUN rather than from a clock in the task.

    ONE STAMP PER RUN, WHICH IS WHY IT IS A PARAMETER AND NOT `datetime.now()`.
    `opl.vault.hubs` gives `load_date` no default and says why: a loader that stamps its
    own clock can only be checked against another clock reading, and in the data it
    makes the LDTS a record of when the pipeline happened to run rather than "a value
    the job's own parameters pin". A task calling `now()` here would satisfy the
    signature and reintroduce the same thing one layer out -- and worse at job scale,
    because the hub and the satellite loaded by one run would then disagree about when
    that run happened. `{{job.start_time.iso_datetime}}` is one value for every task of
    one run, and it survives a task retry.

    AN UNRESOLVED REFERENCE IS DIAGNOSED BY NAME. If the reference is not one this
    workspace substitutes, the literal `{{job.start_time.iso_datetime}}` arrives here as
    a string; `fromisoformat` would refuse it with a message about a date format, which
    sends the reader to the wrong question."""
    candidate = (value or "").strip()
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        raise ValueError(
            f"refusing to load: load_date={value!r} is not an ISO-8601 timestamp. It is "
            f"the DV2 load timestamp and the job YAMLs pass {LOAD_DATE_REFERENCE} for it, "
            "so one run stamps every table it loads with one value. If that reference "
            "arrived here verbatim, this workspace did not substitute it and the task "
            "parameter needs a reference it does support; if it is empty, the task was "
            "launched without it"
        ) from None


def required_spec(name: str, kind: type[_T], *, loader: str) -> _T:
    """The registered vault table `name`, refusing one THIS entry point cannot load.

    `domains.table_spec` already refuses a name no domain registers, naming the
    alternatives. What it cannot refuse is a registered table of the wrong KIND, and
    that is the mistake a copied job YAML makes: `vault_load_hub.py` handed
    `sat_empresa_dados` would reach `load_hub`, which asks the spec for
    `business_keys`, and die inside Spark's analysis with an `AttributeError` naming a
    field rather than a table. Every loader in this package takes exactly one kind and
    each has its own entry point, so the pairing is checkable here, before a session."""
    spec = domains.table_spec(name)
    if not isinstance(spec, kind):
        raise ValueError(
            f"{loader} was handed vault table {name!r}, which is a "
            f"{type(spec).__name__} and not a {kind.__name__}. Each loader in "
            "opl.vault takes one kind of spec, so this pairing comes from a job YAML "
            "task naming a table the entry point beside it cannot write -- which is the "
            "shape a copied job resource produces"
        )
    return spec
