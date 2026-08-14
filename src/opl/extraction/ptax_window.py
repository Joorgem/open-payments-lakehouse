# src/opl/extraction/ptax_window.py
"""The quote-date window F-API extracts, DECLARED -- not passed in at launch.

IT WAS A PAIR OF JOB PARAMETERS AND THAT WAS REVERSED, so the reason lives here rather
than in a commit message nobody reads twice. `first`/`last` were `bronze_ptax_job.yml`
parameters defaulting to a refusing sentinel, and the sentinel was argued from a lock
that did not exist: `opl.config.SENTINEL_QUOTE_DATE` cited a comparison in
`tests/test_job_yaml_launch_guards.py` which nothing had written, so the two YAML
defaults were tied to nothing at all. F-API's Task 2 spec had already ruled the other
way and priced this exact cost -- "a `first`/`last` parameter would need two more
refusing sentinels and two more locks nobody has written".

THE DANGEROUS DIRECTION IS YAML-SIDE, and it is the edit `default: "2026-06"` already
was for `month` (fixed in F1.4b PR B). Paste `default: "2026-06-03"` in and every
`--params`-less run fetches a real window, lands it under the filename that window
derives -- and then, by `emit_records_file`'s own refusal, BLOCKS the window that was
actually wanted under that name until somebody deletes a file from the Volume by hand.
A declared constant cannot be launched wrong: there is nothing to omit.

`_WINDOW_START` IN `opl.generator.profiles` IS THE PRECEDENT AND THE ARGUMENT IS THE
SAME ONE. Every number a landed file depends on belongs in a diff BEFORE the run, where
it is reviewed, rather than in a launch command that is retyped. The landing filename is
derived from this window (`fetch_ptax.filename_for`), so these two dates are part of a
path in the Volume and part of what the Auto Loader checkpoint records as read.

WHY A MODULE OF ITS OWN RATHER THAN A CONSTANT IN `ptax_source`. That module is the
request/response contract -- how a quote is asked for and what makes an answer valid --
and it changes when the API does. This changes when the phase's measured range changes,
which is a different reason to edit and a different author on the other side of it: the
range below is Task 0's measurement, not BCB's shape. It is also why the two are
separately importable: `scripts/probe_ptax.py` and the fetch task want the range without
the parser, and the parser has 28 tests that must not turn red when a date moves.

DATES AND NOT STRINGS, which removes the one refusal the deleted parameters needed. The
endpoint is asked in `MM-DD-YYYY` in single quotes -- not ISO, and got wrong by everyone
who assumes -- so `require_quote_date` existed to refuse an operator who typed the API's
own spelling into a job parameter. A `date(2026, 6, 3)` literal cannot be written in the
API's spelling, so the whole class is structurally gone rather than guarded: that is what
made removing the parameters the smaller diff instead of writing two more locks.
"""
from __future__ import annotations

from datetime import date

# THE RANGE, MEASURED BY F-API TASK 0 AND NOT ARGUED: 2026-06-03 .. 2026-08-01, 42
# quotes, gapless in business days, with the probe that produced it committed as
# `scripts/probe_ptax.py` and its output in `docs/f-api-run-evidence.md`.
#
# 2026-06-03 IS A HARD FLOOR AND NOT A ROUND NUMBER. The only weekday absence in this
# span is 2026-06-04 (Corpus Christi, computed), which resolves back to 2026-06-03,
# venda 5.04150 -- so starting at 06-04 leaves the holiday case with nothing to fall
# back to, and plan T3 clause 3 makes a payment below the series' first quote a REFUSAL
# rather than a NULL. The floor is the fallback's target, not the fact's earliest date.
#
# 2026-08-01 IS THE LAST DAY THE FACT REACHES -- the `clean` + `promotable` payment
# window -- and it is a Saturday with no quote, which is the whole reason the series has
# to extend past what a naive bound would ask for rather than stop at it.
WINDOW_FIRST = date(2026, 6, 3)
WINDOW_LAST = date(2026, 8, 1)
