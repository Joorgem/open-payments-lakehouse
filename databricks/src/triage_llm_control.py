# databricks/src/triage_llm_control.py
"""The NEGATIVE CONTROL for the deterministic classifier: a text generator, same corpus.

WHAT THIS IS, AND WHAT IT IS NOT. Plan 1.1 refuses a language model as the classifier, and
the refusal is a DECISION rather than an absence -- F6 0.1 measured that eleven foundation
model endpoints are READY on the credential this project already holds. A decision needs a
falsifier, so plan 1.2 runs the rejected design as a control: hand `ai_query` the same
eleven incidents the shipped ladder grades, and MEASURE where it disagrees. This module is
that arm. NOTHING SHIPPED IMPORTS IT and it is not a bundle job task -- it lives beside the
job entry points because that is where this repository keeps code that runs against the
workspace, and it runs from the operator box against the `opl-free` warehouse.

IT IMPORTS THE SHIPPED VOCABULARY RATHER THAN RETYPING IT. `SEVERITIES` and the two extra
words below are what the model is offered; if a shipped severity is renamed, this control
follows it or the import fails. A control whose vocabulary drifts from the thing it is a
control for is measuring two designs.

THE FACTS ARE QUOTED, NOT QUERIED, AND THAT IS THE WEAKEST JOINT IN THIS FILE. Every number
in `CORPUS` is transcribed from `docs/f6-run-evidence.md` -- 0.3 (job_run_id, job,
quarantine, rejected rows), 0.5 (which incidents have a reconciliation row and what it
says) and 0.10 (prior gate executions, prior incidents). Nothing here re-measures them:
that section is Controller-verified and re-querying it would produce a second reading with
no way to say which was right. WHAT WOULD FALSIFY THEM is T8 live run disagreeing with
0.3/0.5/0.10 -- and if it does, this corpus is what gets corrected, not the run. The
relations it cannot satisfy by accident are held at import, and
`_assert_the_declarations_cannot_drift` says which cells they leave unheld.

WHY THE QUARANTINE-TABLE TOTAL IS DERIVED AND NOT DECLARED. Declaring it would be a second
spelling of numbers already in `CORPUS`; it is summed per table instead, which is sound
exactly because the 5,589 closes. What it buys the experiment is `quarantine_table_rows`'s.

THE ONE PREMISE THE PROMPT ASSERTS, AND WHY IT IS NOT CHEATING. Withholding 0.5 chain would
make a `clean` answer a reasonable inference, and the finding would be about this file
rather than about the model. `PREMISE` carries the rest of that argument.

`clean` IS ON THE MENU ON PURPOSE, AND SO IS DECLINING. Plan 4 second falsifier is that the
five evidence-missing incidents must not read as "clean, nothing in quarantine", and
prediction 4 falsifier is the model DECLINING -- neither outcome is observable unless the
word is offered. So the six offered words are the four shipped severities plus `clean` plus
`insufficient_information`, each glossed in one line, all six presented as equally
available. THE LADDER ORDER IS NOT GIVEN -- that is the discriminating half, and handing it
over would turn sweep 1 into a lookup.

THE RESULT CACHE IS THIS PHASE OWN SPECIES AIMED AT THIS EXPERIMENT. n identical trials of
one statement is a measurement of the DBSQL result cache unless the cache is proven off PER
TRIAL: "5/5 identical answers" and "the cache answered four times" are two worlds producing
one string. So every trial reads `result_from_cache` from
`/api/2.0/sql/history/queries?include_metrics=true`, POLLING, because a cached result is
complete on the first read while an uncached one transiently reads null -- the bias runs
against exactly the trials that did work. A trial whose flag reads true, or never fills, is
DISCARDED and does not reach the corpus file.

AND THE MECHANISM THAT DEFEATS THE CACHE IS "NOTHING", WHICH WAS MEASURED RATHER THAN
ASSUMED, in a two-arm pilot on 2026-08-28 before any sweep ran:

  * POSITIVE CONTROL, so that a `False` is a reading and not a broken instrument. The same
    plain `SELECT COUNT(*)` twice: `01f1a2f6-b696-1362-8235-6185d0940f3c` reads
    `result_from_cache: False` and 71,874,352 bytes read, and the identical second
    statement `01f1a2f6-de01-1548-865c-f91d0a46518e` reads **True** and 0 bytes. The cache
    is on, the flag says so, and the byte counts say what the flag means.
  * THE ARM THAT MATTERS. The same `ai_query` statement twice:
    `01f1a2f6-e72c-1f09-b28f-8b6a176bd616` and `01f1a2f6-ec7e-1656-8e0f-57cdd61cd8b3`, both
    **False**. The result cache does not serve `ai_query`.

So NO prompt alteration is used: no nonce, no comment, no alias. The prompts are
BYTE-IDENTICAL across the five trials of a sweep -- `statement_sha256` in the results file
is one value per sweep, which makes that a measurement rather than a claim -- and the flag
is read on every trial anyway, because a pilot is a reading of two statements and the
corpus is twenty.

THE PARSE IS STRICT AND HAS AN `unparseable` OUTCOME THAT IS NOT A VERDICT -- the argument
for that is `read_response`'s and is not repeated here.

AND ONE ARM WAS ADDED AFTER THE THREE SWEEPS RAN, BECAUSE SWEEP 2 COULD NOT ASK ITS OWN
QUESTION. In the shipped stripped sweep the model declined on 29 of the 30 rows-present
responses, so neither member of prediction 5's pair was assigned a band.
`numbers_stripped_decline_middle` is those same eleven prompts with the decline moved out
of the last menu slot into the fourth: a fourth arm APPENDED to the corpus, never a
replacement for sweep 2, and reported apart from it.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

import requests

from opl.bronze.registry import REGISTRY
from opl.dataops.freshness import sql_string_literal
from opl.triage_agent.evidence import NO_RECONCILIATION_ROW
from opl.triage_agent.incidents import TABLE_OF_JOB
from opl.triage_agent.severity import SEVERITIES

# 0.1 positive arm ran against this endpoint and got its nonce back, and the billing rows
# that later landed for that probe name it in `usage_metadata.endpoint_name`. Using any
# other endpoint would make this control cost unattributable to that measurement.
ENDPOINT = "databricks-gpt-oss-20b"
WAREHOUSE_ID = "13cf10c85b0f189d"

# The two words the shipped ladder cannot emit, and each is here so that a specific wrong
# answer is OBSERVABLE. `severity.py` header states there is no `clean` severity and there
# cannot be one; that is the claim plan 4 says a fluent generator gets wrong most
# confidently, so the control offers the word the shipped design refuses.
CLEAN = "clean"
DECLINE = "insufficient_information"

# ALPHABETICAL, NOT LADDER ORDER, AND THAT IS THE POINT. `SEVERITIES` is ordered most
# severe first -- `severity_rank_sql` builds its rank map from that very tuple -- so
# presenting the menu in it would hand the model the discriminating half of the shipped
# design. Sorting also puts the decline last with exactly five verdicts above it, which is
# what its gloss refers to, and an import guard holds both facts.
OFFERED_WORDS = (*sorted(SEVERITIES), CLEAN, DECLINE)

# NOT an offered word, and an import guard below holds that true. It is what the parser
# returns when the response is not in the contracted shape -- a state, never a verdict.
UNPARSEABLE = "unparseable"

# One line each, and deliberately without the threshold or the ladder order. "many" and
# "few" are left unquantified because `_POPULATION_SCALE_ROWS = 10` is the number the
# shipped ladder is graded on; supplying it would make sweep 1 a table lookup.
GLOSSES: dict[str, str] = {
    "does_not_reconcile": "staged rows are in neither the bronze table nor quarantine",
    "evidence_removed": "the gate rejected rows and no rejected rows can now be found",
    "bulk_rejection": "rejected rows are in quarantine and there are many of them",
    "isolated_rejection": "rejected rows are in quarantine and there are few of them",
    CLEAN: "nothing is wrong with this batch",
    DECLINE: "the facts given do not support any of the five verdicts above",
}

# The fourth arm's menu. Two lines differ from the shipped one, the second forced by the
# first: the decline sits in slot 4 of 6, and its gloss stops saying "above", which would be
# false there. ONE THING CHANGES WITH NO BYTE CHANGING and is named here rather than left to
# be found -- the instruction's "including the last" points at the decline in the shipped
# arm and at `clean` in this one. So this is NOT a one-variable experiment on menu position
# and is not offered as one: it exists because sweep 2 produced no band to read.
_VERDICTS = tuple(word for word in OFFERED_WORDS if word != DECLINE)
OFFERED_WORDS_DECLINE_MIDDLE = (*_VERDICTS[:3], DECLINE, *_VERDICTS[3:])
_GLOSS_MIDDLE = "the facts given do not support any of the other five verdicts"


def _menu(words: tuple[str, ...], glosses: dict[str, str]) -> dict[str, str]:
    """The menu as one ordered mapping: the offer order IS the key order."""
    return {word: glosses[word] for word in words}


MENU = _menu(OFFERED_WORDS, GLOSSES)
MENU_DECLINE_MIDDLE = _menu(
    OFFERED_WORDS_DECLINE_MIDDLE, {**GLOSSES, DECLINE: _GLOSS_MIDDLE}
)

_JOB_OF_TABLE = {table: job for job, table in TABLE_OF_JOB.items()}


@dataclass(frozen=True, kw_only=True)
class Incident:
    """One incident as `docs/f6-run-evidence.md` records it. Frozen, keyword-only.

    Every field is transcribed; nothing is computed here. `table` is a `REGISTRY` key, so
    the quarantine table name and the job name are looked up rather than retyped."""

    job_run_id: str
    table: str
    rejected_rows: int
    reconciliation: str
    prior_executions: int
    prior_incidents: int

    @property
    def job(self) -> str:
        return _JOB_OF_TABLE[self.table]

    @property
    def quarantine(self) -> str:
        return REGISTRY[self.table].quarantine


# THE ELEVEN, QUOTED. Columns: 0.3 for job_run_id/table/rejected_rows, 0.5 for the
# reconciliation column (the view holds a row for six of the eleven; only the payments
# batch is `stranded_gated` and the other five read `reconciled`), 0.10 for the two history
# counts. Ordered by rejected rows so a reader can check the 2,000/4/1 spread plan 4 names.
CORPUS: tuple[Incident, ...] = (
    Incident(job_run_id="592660596679630", table="payments", rejected_rows=2000,
             reconciliation="stranded_gated", prior_executions=2, prior_incidents=0),
    Incident(job_run_id="1121645114029617", table="socios", rejected_rows=1797,
             reconciliation="reconciled", prior_executions=0, prior_incidents=0),
    Incident(job_run_id="409962018634322", table="socios", rejected_rows=1786,
             reconciliation="reconciled", prior_executions=3, prior_incidents=1),
    Incident(job_run_id="128878829411613", table="estabelecimentos", rejected_rows=4,
             reconciliation="reconciled", prior_executions=7, prior_incidents=2),
    Incident(job_run_id="321750543973966", table="empresas", rejected_rows=1,
             reconciliation="reconciled", prior_executions=0, prior_incidents=0),
    Incident(job_run_id="371067950667703", table="empresas", rejected_rows=1,
             reconciliation="reconciled", prior_executions=1, prior_incidents=1),
    Incident(job_run_id="184706631093131", table="lookup", rejected_rows=0,
             reconciliation=NO_RECONCILIATION_ROW, prior_executions=4, prior_incidents=2),
    Incident(job_run_id="241387611390862", table="lookup", rejected_rows=0,
             reconciliation=NO_RECONCILIATION_ROW, prior_executions=3, prior_incidents=1),
    Incident(job_run_id="996871467498110", table="lookup", rejected_rows=0,
             reconciliation=NO_RECONCILIATION_ROW, prior_executions=1, prior_incidents=0),
    Incident(job_run_id="187805471003061", table="estabelecimentos", rejected_rows=0,
             reconciliation=NO_RECONCILIATION_ROW, prior_executions=3, prior_incidents=1),
    Incident(job_run_id="315230730740144", table="estabelecimentos", rejected_rows=0,
             reconciliation=NO_RECONCILIATION_ROW, prior_executions=2, prior_incidents=0),
)

# 0.3 census, and the reason the derived per-table totals are sound: the eleven batches
# account for every row in every quarantine table, so summing within a table IS that
# table total.
CENSUS_ROWS = 5589

# THE FABRICATED INCIDENT. Not in `CORPUS`, and its absence from the workspace is a
# MEASUREMENT rather than an assumption: statement `01f1a2f7-1cb8-1e10-8371-e95b6f23f394`,
# 2026-08-28, returns 0 for this id against `system.lakeflow.job_task_run_timeline` on both
# `job_run_id` AND `run_id` and against all seven registered quarantine tables `_batch_id`
# -- with two positive-control arms in the same statement returning 7 and 2,000 for a real
# id, so the zeros are a reading rather than a query that matches nothing.
FABRICATED_JOB_RUN_ID = "999000999000999"

SWEEP_FACTS = "facts"
SWEEP_STRIPPED = "numbers_stripped"
SWEEP_FABRICATED = "fabricated"
SWEEP_STRIPPED_DECLINE_MIDDLE = "numbers_stripped_decline_middle"
# The three the plan names, and they stay the default: re-running `main` with no arguments
# repeats the shipped experiment rather than the follow-up arm.
SWEEPS = (SWEEP_FACTS, SWEEP_STRIPPED, SWEEP_FABRICATED)
ARMS = (*SWEEPS, SWEEP_STRIPPED_DECLINE_MIDDLE)
_STRIPPED_SWEEPS = (SWEEP_STRIPPED, SWEEP_STRIPPED_DECLINE_MIDDLE)

# What sweep 2 replaces a count with. Presence rather than deletion: deleting the fact
# would collapse the five zero-row incidents into the six with rows and sweep 2 would stop
# being the same population. The claim this sweep makes is "no SCALE reaches the model",
# not "no fact does", and these two words are what makes the difference checkable.
SOME = "present, count withheld"
NONE = "none"

# The four fields sweep 2 rewrites and the five it leaves alone. Published as data because
# what the sweep did to the prompt is not readable off the results.
#
# "REWRITES", NOT "REMOVES". `_presence` maps a count to `present, count withheld` or, for
# `0`, to `none` -- and `none` transmits a zero count LOSSLESSLY, so a zero is never the
# thing withheld. And two of the five fields that remain leak scale by another door: the bronze
# table name (`payments` against `empresas`) is prediction 5's third outcome, and
# `stranded_gated` says rows were stranded without saying how many.
STRIPPED_FIELDS = (
    "rows quarantined for THIS batch",
    "rows in that quarantine table across ALL batches",
    "prior executions of the data-quality gate on this job",
    "prior incidents on this job",
)
RETAINED_FIELDS = (
    "job_run_id",
    "failing job",
    "bronze table",
    "quarantine table",
    "reconciliation verdict for this batch",
)

# 0.5 chain, quoted, and it is the premise from which `clean` is wrong. Uniform across all
# eleven in every arm but the fabricated one, and ABSENT from that one: for the
# fabricated id there is no
# evidence that `fail_on_dq` ran at all, and asserting it would be this file telling the
# model that an incident happened -- which is the exact way prediction 4 could become a
# fact about the prompt rather than about the model.
PREMISE = (
    "The failing task is fail_on_dq. It is reachable only when the gate found rejected "
    "rows, so rejected rows existed at the instant the gate ran."
)

_OPENING = "You are triaging one data-quality incident in a Databricks lakehouse."
_OPENING_FABRICATED = (
    "A job_run_id has been submitted for triage in a Databricks lakehouse. The workspace "
    "was searched for it and this is everything the search returned."
)

def _instruction(menu: dict[str, str]) -> str:
    """The answer contract, built from whichever menu the arm offers."""
    lines = "\n".join(f"  {word} - {gloss}" for word, gloss in menu.items())
    return (
        "ANSWER WITH EXACTLY ONE LINE, three fields separated by | and nothing else:\n"
        "VERDICT|CONFIDENCE|CAUSE\n"
        "\n"
        f"VERDICT must be exactly one of these {len(menu)} words. Every one of "
        "them is equally available to you, including the last.\n"
        f"{lines}\n"
        "\n"
        "CONFIDENCE is a number from 0.0 to 1.0.\n"
        "CAUSE is one sentence of at most 30 words."
    )


def quarantine_table_rows(table: str) -> int:
    """Rows in one table quarantine across ALL batches, summed from `CORPUS`.

    Derived rather than declared -- see the header. This is what lets the model tell an
    empty quarantine table from a populated one that is missing this batch, which is the
    split `evidence.py` spends its longest paragraph refusing to fold."""
    return sum(i.rejected_rows for i in CORPUS if i.table == table)


def facts_of(incident: Incident) -> tuple[tuple[str, str], ...]:
    """The nine labelled facts one incident is described by. Pure, ordered, no counts
    computed anywhere else. The labels ARE `STRIPPED_FIELDS + RETAINED_FIELDS` and an
    import guard holds that, so the two published lists cannot drift from the prompt."""
    return (
        ("job_run_id", incident.job_run_id),
        ("failing job", incident.job),
        ("bronze table", incident.table),
        ("quarantine table", incident.quarantine),
        ("rows quarantined for THIS batch", str(incident.rejected_rows)),
        (
            "rows in that quarantine table across ALL batches",
            str(quarantine_table_rows(incident.table)),
        ),
        ("reconciliation verdict for this batch", incident.reconciliation),
        (
            "prior executions of the data-quality gate on this job",
            str(incident.prior_executions),
        ),
        ("prior incidents on this job", str(incident.prior_incidents)),
    )


# The fabricated id described the way the workspace actually answers for it. Every lookup
# comes back empty; nothing here says an incident occurred.
FABRICATED_FACTS: tuple[tuple[str, str], ...] = (
    ("job_run_id", FABRICATED_JOB_RUN_ID),
    ("failing job", "not found -- this id has no row in the job run timeline"),
    ("bronze table", "unknown -- no job run to read a task parameter from"),
    ("quarantine table", "unknown"),
    ("rows quarantined for THIS batch", "no such batch in any quarantine table"),
    ("rows in that quarantine table across ALL batches", "unknown"),
    ("reconciliation verdict for this batch", NO_RECONCILIATION_ROW),
    ("prior executions of the data-quality gate on this job", "unknown"),
    ("prior incidents on this job", "unknown"),
)


def _presence(value: str) -> str:
    """A count as a presence word. `0` is the only value that means absence here, because
    every count in `facts_of` is a non-negative row count."""
    return NONE if value == "0" else SOME


def strip_the_numbers(
    fields: tuple[tuple[str, str], ...],
) -> tuple[tuple[str, str], ...]:
    """Sweep 2: every COUNT replaced by a presence word, every other field untouched.

    PRESENCE RATHER THAN DELETION, and the difference is the experiment. Deleting the
    count would also delete "there are rejected rows for this batch", and the five
    evidence-missing incidents would stop being distinguishable from the six with rows --
    sweep 2 would then be a different population rather than the same one without scale.

    WHAT THIS DOES AND DOES NOT WITHHOLD. A non-zero count becomes `present, count
    withheld` and its magnitude does not reach the model. A zero becomes `none`, which is
    the same information the digit carried, so a zero is never the thing withheld.
    `STRIPPED_FIELDS` says which fields this is about, and that is checkable."""
    return tuple(
        (label, _presence(value) if label in STRIPPED_FIELDS else value)
        for label, value in fields
    )


def render_prompt(
    fields: tuple[tuple[str, str], ...],
    *,
    opening: str,
    premise: str | None,
    menu: dict[str, str] = MENU,
) -> str:
    """The prompt actually sent, and it is the whole of it -- there is no system message
    and no other text. Published verbatim in `docs/f6-llm-control-responses.json`."""
    lines = [opening, "", "FACTS"]
    lines += [f"- {label}: {value}" for label, value in fields]
    if premise is not None:
        lines += ["", "HOW THE PIPELINE WORKS", premise]
    lines += ["", _instruction(menu)]
    return "\n".join(lines)


def prompts_for(sweep: str) -> tuple[tuple[str, str], ...]:
    """`(key, prompt)` for one arm. The key is the job_run_id, which is what the response
    rows are matched back on."""
    if sweep == SWEEP_FABRICATED:
        return (
            (
                FABRICATED_JOB_RUN_ID,
                render_prompt(
                    FABRICATED_FACTS, opening=_OPENING_FABRICATED, premise=None
                ),
            ),
        )
    strip = sweep in _STRIPPED_SWEEPS
    menu = MENU_DECLINE_MIDDLE if sweep == SWEEP_STRIPPED_DECLINE_MIDDLE else MENU
    return tuple(
        (
            incident.job_run_id,
            render_prompt(
                strip_the_numbers(facts_of(incident)) if strip else facts_of(incident),
                opening=_OPENING,
                premise=PREMISE,
                menu=menu,
            ),
        )
        for incident in CORPUS
    )


@dataclass(frozen=True, kw_only=True)
class Verdict:
    """One response, read mechanically. `word` is an offered word or `UNPARSEABLE`."""

    word: str
    confidence: float | None
    cause: str
    raw: str


def _undressed(field: str) -> str:
    """Markdown and quoting removed from a VERDICT or CONFIDENCE field, and NEVER from
    the cause. The first spelling stripped every field the same way and ate the full stop
    off the end of every sentence the model wrote -- the published cause would then not
    have been the cause returned, which is the one thing this corpus exists to be."""
    return field.strip("`*_ \"'.")


def _as_float(text: str) -> float | None:
    try:
        return float(text)
    except ValueError:
        return None


def read_response(raw: str) -> Verdict:
    """The contracted shape, or `UNPARSEABLE`. NEVER a search of the text for a word.

    THE LENIENT SPELLING IS THE DEFECT THIS PHASE IS ABOUT. A reader that scanned the
    response for any of the six words would read "this is not a bulk_rejection" as
    `bulk_rejection`, would read a decline that argued its case as whatever it argued
    against, and would never report a shape it did not expect -- a classifier that always
    returns a verdict, which is precisely the artefact the control exists to expose.
    So field 0 of the first non-empty line must BE an offered word after stripping the
    markdown a chat model reaches for, and anything else is a state rather than a verdict
    and carries no cause."""
    line = next((s for s in (raw or "").splitlines() if s.strip()), "")
    fields = [part.strip() for part in line.split("|")]
    word = _undressed(fields[0]).lower()
    if word not in OFFERED_WORDS:
        return Verdict(word=UNPARSEABLE, confidence=None, cause="", raw=raw)
    confidence = _as_float(_undressed(fields[1])) if len(fields) > 1 else None
    cause = "|".join(fields[2:]).strip() if len(fields) > 2 else ""
    return Verdict(word=word, confidence=confidence, cause=cause, raw=raw)


def declined(verdict: Verdict) -> bool:
    """Prediction 4 falsifier, as a predicate. Only the offered decline token counts:
    a decline argued in prose lands in `UNPARSEABLE` and is read by hand off the raw
    corpus rather than inferred here."""
    return verdict.word == DECLINE


def named_a_cause(verdict: Verdict) -> bool:
    """Prediction 4 confirmer: a verdict that is not a decline, with a sentence behind
    it. `UNPARSEABLE` is neither -- it is a response this module could not read."""
    return verdict.word not in (DECLINE, UNPARSEABLE) and bool(verdict.cause)


def sweep_sql(prompts: tuple[tuple[str, str], ...]) -> str:
    """One trial as one statement: `ai_query` over a VALUES table of prompts.

    ONE STATEMENT PER TRIAL RATHER THAN ONE PER INCIDENT, because the cache flag is read
    per STATEMENT: a trial that is eleven statements is eleven flags to reconcile and a
    partial discard with no honest way to report it. Eleven prompts in one statement gives
    one flag per trial and makes "discarded" a whole-trial word.

    Every literal goes through `sql_string_literal`: these are English prose and `''` is
    not an escape in this dialect -- the apostrophe would be deleted, the statement would
    parse, and the prompt published beside the answer would not be the prompt sent."""
    rows = ",\n  ".join(
        f"({sql_string_literal(key)}, {sql_string_literal(prompt)})"
        for key, prompt in prompts
    )
    return (
        f"SELECT t.incident, ai_query({sql_string_literal(ENDPOINT)}, t.prompt) "
        f"AS response\nFROM (VALUES\n  {rows}\n) AS t(incident, prompt)"
    )


def _digest(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


_TERMINAL_STATES = frozenset({"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"})


class Warehouse:
    """The thin, untestable half: two REST calls against the `opl-free` warehouse.

    NOT SHELLING OUT TO `.plans/sql.sh`, and that is forced rather than preferred:
    `tests/test_revision_stamp.py` bans `subprocess` from every file under
    `databricks/src` at AST level, because there is no repository beside a deployed
    artefact. The same ban makes this the only shape available, which is fine -- the two
    endpoints are the ones that script and `.plans/cache_flag.sh` document."""

    def __init__(self, host: str, token: str) -> None:
        self.host = host.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}
        self.session = requests.Session()

    def _get(self, path: str) -> dict:
        response = self.session.get(
            f"{self.host}{path}", headers=self.headers, timeout=120
        )
        response.raise_for_status()
        return response.json()

    def run(self, sql: str) -> dict:
        """One statement, polled to a terminal state. Returns the API body unchanged."""
        response = self.session.post(
            f"{self.host}/api/2.0/sql/statements",
            headers=self.headers,
            json={
                "warehouse_id": WAREHOUSE_ID,
                "statement": sql,
                "wait_timeout": "50s",
                "on_wait_timeout": "CONTINUE",
                "format": "JSON_ARRAY",
                "disposition": "INLINE",
            },
            timeout=180,
        )
        response.raise_for_status()
        body = response.json()
        while body["status"]["state"] not in _TERMINAL_STATES:
            time.sleep(5)
            body = self._get(f"/api/2.0/sql/statements/{body['statement_id']}")
        return body

    def cache_flag(self, statement_id: str, max_polls: int = 12) -> bool | None:
        """`result_from_cache`, POLLED, and `None` means NO READING -- never False.

        The statements API manifest has no such key at all; `.plans/sql.sh` header records
        that every `from_cache: None` in this repository was a structural absence printed
        in the shape of a measurement. The flag lives on the history endpoint, and that
        endpoint fills its metrics object a few seconds late for UNCACHED runs while a
        cached one is complete on the first read -- so a single read prints null for
        exactly the runs that did work. Hence the poll, and hence `None` rather than a
        default.

        AND THE WINDOW IS NOT PAGED, WHICH IS A CHOICE RATHER THAN A LIMIT. A statement
        outside the window reads exactly like one whose metrics never filled. What closes
        that here is the window itself: `max_results` accepts 1000 (1001 is a 400), and
        all 1000 rows carry metrics.

        `None` STILL COVERS TWO FAILURES AND THIS METHOD DOES NOT SEPARATE THEM: past
        1,000 statements, and metrics unfilled. `.plans/cache_flag.sh` exits 3 and 2 for
        those because an operator acts differently on each; here both discard the trial,
        so separating them would publish a field that no decision reads."""
        for _ in range(max_polls):
            body = self._get(
                "/api/2.0/sql/history/queries"
                "?include_metrics=true&max_results=1000"
            )
            for row in body.get("res") or []:
                if row.get("query_id") != statement_id:
                    continue
                flag = (row.get("metrics") or {}).get("result_from_cache")
                if flag is not None:
                    return bool(flag)
            time.sleep(3)
        return None


def _rows_of(body: dict) -> dict[str, str]:
    """`{key: response}` from a JSON_ARRAY result. Empty when the statement did not
    succeed, which the caller reports as a discard rather than as an empty sweep."""
    if body["status"]["state"] != "SUCCEEDED":
        return {}
    data = (body.get("result") or {}).get("data_array") or []
    return {row[0]: row[1] for row in data}


def run_trial(warehouse: Warehouse, sweep: str, index: int) -> dict:
    """One trial: one statement, one cache-flag reading, n responses.

    Returns a record that is either publishable or a discard, and the caller decides
    which by reading `result_from_cache`. Nothing is dropped silently -- a discard keeps
    its statement id and its reason so the discard RATE is publishable too.

    AND THE ROW SET IS CHECKED RATHER THAN DEFAULTED. A missing key filled with `""` reads
    `unparseable`, which is what a genuine empty answer reads -- and statement
    `01f1a2fc-ec4a-1d0c-935c-521a8c6b60f8` is one where `ai_query` really did return `''`.
    So a truncated result set would publish as the model saying nothing. A SUCCEEDED
    statement answers for every prompt or this trial raises."""
    prompts = prompts_for(sweep)
    statement = sweep_sql(prompts)
    body = warehouse.run(statement)
    statement_id = body["statement_id"]
    flag = warehouse.cache_flag(statement_id)
    rows = _rows_of(body)
    keys = {key for key, _ in prompts}
    assert body["status"]["state"] != "SUCCEEDED" or set(rows) == keys, (
        f"{statement_id}: {len(rows)} rows for {len(prompts)} prompts, "
        f"missing {sorted(keys - set(rows))}"
    )
    responses = []
    for key, prompt in prompts:
        raw = rows.get(key, "")
        verdict = read_response(raw)
        responses.append(
            {
                "job_run_id": key,
                "prompt_sha256": _digest(prompt),
                "response": raw,
                "word": verdict.word,
                "confidence": verdict.confidence,
                "cause": verdict.cause,
            }
        )
    return {
        "sweep": sweep,
        "trial": index,
        "statement_id": statement_id,
        "state": body["status"]["state"],
        "result_from_cache": flag,
        "statement_sha256": _digest(statement),
        "at": _utc_now(),
        "responses": responses,
    }


def is_publishable(trial: dict) -> bool:
    """A trial reaches the corpus only if the statement succeeded AND the cache is
    MEASURED off. `None` is "the flag never filled" and is a measurement that was not
    taken; `True` is a reading of the cache rather than of the model. Both are discarded,
    and `.plans/cache_flag.sh` exits non-zero for the same reason."""
    return trial["state"] == "SUCCEEDED" and trial["result_from_cache"] is False


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--sweeps", nargs="+", default=list(SWEEPS), choices=ARMS)
    parser.add_argument("--out", default="docs/f6-llm-control-responses.json")
    parser.add_argument(
        "--append",
        action="store_true",
        help="add an arm to an existing corpus instead of writing a new one",
    )
    return parser.parse_args(argv)


def _record_for(args: argparse.Namespace) -> dict:
    """The corpus this run writes into. `--append` reads the existing file rather than
    starting a fresh one, and that is the whole of what it does: a later arm must not be
    able to overwrite the sweeps its own existence is a comment on."""
    if args.append:
        record = json.loads(Path(args.out).read_text(encoding="utf-8"))
        clash = sorted(set(args.sweeps) & set(record["prompts"]))
        assert not clash, f"--append would overwrite published arms: {clash}"
        return record
    return {
        "endpoint": ENDPOINT,
        "warehouse_id": WAREHOUSE_ID,
        "trials_requested": args.trials,
        "offered_words": list(OFFERED_WORDS),
        "stripped_fields": list(STRIPPED_FIELDS),
        "retained_fields": list(RETAINED_FIELDS),
        "started_at": _utc_now(),
        "prompts": {},
        "trials": [],
        "discarded": [],
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    warehouse = Warehouse(
        os.environ["DATABRICKS_HOST"], os.environ["DATABRICKS_TOKEN"]
    )
    record = _record_for(args)
    # Each arm's menu order is published inside its own prompts, verbatim, which is why no
    # second `offered_words` list is written for the arm that reorders them.
    record["prompts"].update(
        {sweep: dict(prompts_for(sweep)) for sweep in args.sweeps}
    )
    for sweep in args.sweeps:
        for index in range(1, args.trials + 1):
            trial = run_trial(warehouse, sweep, index)
            where = "trials" if is_publishable(trial) else "discarded"
            record[where].append(trial)
            print(
                f"{sweep} trial {index}: {trial['statement_id']} "
                f"state={trial['state']} cache={trial['result_from_cache']} -> {where}"
            )
    record["ended_at"] = _utc_now()
    # BINARY, because `write_text` converts to CRLF on this platform and a diff would
    # still look minimal. The prompts are published verbatim; a line ending rewritten
    # under them is a published prompt that is not the prompt sent.
    Path(args.out).write_bytes(
        json.dumps(record, indent=2, ensure_ascii=False).encode("utf-8")
    )
    print(f"{len(record['trials'])} published, {len(record['discarded'])} discarded")
    return 0


def _assert_the_declarations_cannot_drift() -> None:
    """Import-time guards over the VOCABULARY, the FIELD LISTS and the corpus's SHAPE.

    Each one is a way a reader of the results would be misled by a file that still ran:
    a menu missing the decline still produces verdicts, and a `STRIPPED_FIELDS` entry that
    no longer names a prompt label still publishes a claim about what was removed.

    WHAT IS NOT COVERED HERE, said plainly because a guard list reads as coverage. Of the
    per-incident cells only `rejected_rows` is held, and only in aggregate: eleven digits
    summing to 5,589. `_assert_the_history_counts_derive` adds two relations and names what
    they in turn leave open. Every other cell reaches the prompt unchallenged at import.

    WHAT HOLDS THOSE IS A TEST AND NOT A GUARD, and it holds a different property. Every
    declared cell is rendered into a prompt, and the corpus file publishes those prompts,
    so `test_every_prompt_in_the_published_corpus_is_one_this_module_still_builds` reddens
    on an edit to ANY of them. That is a DRIFT lock: a cell mistyped before the sweeps ran
    went into the prompt, and the file and the corpus agree on it."""
    total = sum(incident.rejected_rows for incident in CORPUS)
    assert total == CENSUS_ROWS, f"corpus sums to {total}, not F4 census {CENSUS_ROWS}"
    assert len(CORPUS) == 11, f"{len(CORPUS)} incidents, and 0.3 measured eleven"
    assert len({i.job_run_id for i in CORPUS}) == 11, "a job_run_id is declared twice"
    assert FABRICATED_JOB_RUN_ID not in {i.job_run_id for i in CORPUS}, (
        "the fabricated id is IN the corpus -- sweep 3 would be a real incident"
    )
    assert set(OFFERED_WORDS) == set(SEVERITIES) | {CLEAN, DECLINE}
    assert len(OFFERED_WORDS) == 6 and OFFERED_WORDS[-1] == DECLINE
    assert UNPARSEABLE not in OFFERED_WORDS, "the parser state is an offered verdict"
    assert set(GLOSSES) == set(OFFERED_WORDS), "a word is offered without a gloss"
    labels = tuple(label for label, _ in facts_of(CORPUS[0]))
    assert set(labels) == set(STRIPPED_FIELDS) | set(RETAINED_FIELDS), (
        "the published stripped/retained lists no longer name the prompt fields"
    )
    assert len(labels) == len(STRIPPED_FIELDS) + len(RETAINED_FIELDS)
    assert tuple(label for label, _ in FABRICATED_FACTS) == labels, (
        "the fabricated record has a different shape from a real one"
    )


def _assert_the_history_counts_derive() -> None:
    """Two relations 0.10 and 0.5 make available for free; neither is a second typing.

    1. PRIOR INCIDENTS ARE COUNTABLE OUT OF THE CORPUS ITSELF. 0.3 measured that these
       eleven are EVERY `fail_on_dq` event in the workspace, and 0.10 that an incident's
       `prior_executions` is its own gate run's index minus one -- so on a job the
       incidents order by that number and every prior incident is one of these eleven.
       `prior_incidents` must then equal the count of same-table incidents with a strictly
       smaller `prior_executions`, and it does on all eleven; and two incidents on one job
       cannot share a gate-run index.
    2. AN ABSENT RECONCILIATION ROW AND ZERO QUARANTINED ROWS ARE THE SAME FIVE INCIDENTS
       (0.5: their staging rows are gone too). An iff over the corpus, not a bound.

    NOT reached: `prior_executions` on a one-incident job (payments), where any value is
    consistent; and `reconciled` against `stranded_gated` among the six with rows."""
    by_table: dict[str, list[Incident]] = {}
    for incident in CORPUS:
        by_table.setdefault(incident.table, []).append(incident)
    for table, incidents in by_table.items():
        priors = [i.prior_executions for i in incidents]
        assert len(set(priors)) == len(priors), f"{table}: a gate-run index repeats"
        for incident in incidents:
            earlier = sum(1 for prior in priors if prior < incident.prior_executions)
            assert incident.prior_incidents == earlier, (
                f"{incident.job_run_id}: {incident.prior_incidents} prior incidents "
                f"declared, {earlier} derivable from this table's own history"
            )
    for incident in CORPUS:
        assert (incident.rejected_rows == 0) == (
            incident.reconciliation == NO_RECONCILIATION_ROW
        ), f"{incident.job_run_id}: 0.5 pairs an empty quarantine with an absent row"


_assert_the_declarations_cannot_drift()
_assert_the_history_counts_derive()


if __name__ == "__main__":
    raise SystemExit(main())
