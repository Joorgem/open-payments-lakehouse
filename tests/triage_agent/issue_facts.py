# tests/triage_agent/issue_facts.py
"""The four facts for five of the eleven incidents, CONSTRUCTED. Not a test file.

WHY CONSTRUCTED AND NOT READ. `conftest.py`'s `probe` builds real tables and the shipped
statements run over them; that arm is `test_issue.py`, it needs a JVM, and it is where the
column names this record reads are held against the aliases the SQL emits. What the three
no-JVM files need is different: a body is a function of a RECORD, so the record is the
input, and driving it from Spark would make every assertion about prose wait on a session
and would still only reach the states this fixture's tables can produce.

EVERY QUANTITY BELOW IS THE RECORD'S OWN, AND WHERE IT IS NOT, THE LINE SAYS SO. The batch
ids, the reject counts, the census verdicts and the prior-execution counts are
`docs/f6-run-evidence.md` 0.3, 0.5 and 0.10. THEY ARE INPUT HERE AND ARE NOT REPRODUCED BY
ANYTHING IN THESE FILES: the queries that produce them are T1's, T2's and T4's and are
asserted there, against fixtures those tasks built. A test in this package that "confirmed"
2,000 rejected rows by reading a number this module typed would be confirming a
transcription.

THE STAGED/PROMOTED COUNTS FOR EVERYTHING EXCEPT PAYMENTS ARE INVENTED, and they are
`conftest.py`'s invention rather than a second one: that file chose counts so every batch
reconciles except payments, whose 10,000 / 0 / 2,000 / 8,000 IS the live stranding
(0.5). The five incidents with no reconciliation row carry NULL there, which is measured --
their staging rows are gone, so the view has nothing to say about them.

FIVE INCIDENTS AND NOT ELEVEN, chosen so that every state a body can render is reached by a
real incident: the only stranding and the only declared hold (payments), the mildest grade
(empresas), both removals told apart (lookup, estabelecimentos), and the personal column
name that a reject reason legitimately carries (socios). The other six differ from one of
these five only in quantities.
"""
from __future__ import annotations

from typing import Any

from opl.bronze.reconcile import RECONCILED, STRANDED_GATED
from opl.triage_agent.evidence import (
    EVIDENCE_MISSING_BATCH_ABSENT,
    EVIDENCE_MISSING_QUARANTINE_EMPTY,
    NO_RECONCILIATION_ROW,
    ROWS_PRESENT,
)
from opl.triage_agent.history import (
    INSUFFICIENT_HISTORY,
    N_EXECUTIONS,
    NO_PRIOR_EXECUTION,
)
from opl.triage_agent.issue import Provenance, TriageIssue, triage_issue
from opl.triage_agent.severity import (
    BULK_REJECTION,
    DOES_NOT_RECONCILE,
    EVIDENCE_REMOVED,
    HOLD_DO_NOT_PROMOTE,
    HOLDS,
    INVESTIGATE_THE_MISSING_BATCH,
    INVESTIGATE_THE_QUARANTINE_TABLE,
    ISOLATED_REJECTION,
    REVIEW_THE_QUARANTINED_ROWS,
    SEVERITIES,
)

from .conftest import (
    _EMPRESAS_BATCHES,
    _ESTAB_UNEXPLAINED,
    _LOOKUP_BATCHES,
    _PAYMENTS_BATCH,
    _SOCIOS_BATCHES,
)

# The batch ids come from `conftest.py` rather than being retyped: one corpus, one spelling,
# and the file that already holds them is the one both arms of this package read.
PAYMENTS_BATCH = _PAYMENTS_BATCH
EMPRESAS_BATCH = _EMPRESAS_BATCHES[0]
LOOKUP_BATCH = _LOOKUP_BATCHES[0]
ESTAB_BATCH = _ESTAB_UNEXPLAINED[0]
SOCIOS_BATCH = _SOCIOS_BATCHES[0]

# QUOTED from `dataops_reconciliation`'s own `remedy` column for this batch, which is what a
# real payload carries. Not built from `reconcile.py`'s private prefix constants: reaching
# into another module's privates to avoid typing a string is the dependency `evidence.py`
# refused for the replacement character, and what a fact fixture holds is what the view
# RETURNED, not how the view builds it.
PAYMENTS_REMEDY = (
    "databricks bundle run repromote_triaged_batch -t free --params "
    f"table=payments,batch_id={PAYMENTS_BATCH},revision=$(git rev-parse HEAD)"
)

# A provenance that says what it is. `produced_by` is required by `Provenance` itself, and
# a fixture that named a plausible job run would put a fabricated run id into every body
# these files render and into anything a reader copied out of a failure message.
#
# IT RECORDS ONE ID FOR FOUR FACTS AND THAT IS DELIBERATE, not an omission to tidy up: it is
# the shape both shipped provenances have, and it is the shape the body has to render
# honestly -- one named statement and three `not recorded` lines. A fixture that recorded
# all four would leave the absence arm of that rendering unreached by any constructed body.
# `telemetry_view` is left at its default for the same reason: these facts were not read
# from a relation, so naming one would be a fabrication.
PROVENANCE = Provenance(
    produced_by="pytest: facts constructed in tests/triage_agent/issue_facts.py, no run",
    statements=(("severity", "no statement -- constructed"),),
)


def _rank(severity: str) -> int:
    """The rank of a severity, from `SEVERITIES` itself.

    Typed ranks would be a second spelling of the ladder's order, and the one thing a
    constructed grade must not do is disagree with the module that grades."""
    return SEVERITIES.index(severity) + 1


def _facts(
    *,
    batch: str,
    source: str,
    job: str,
    severity: str,
    action: str,
    evidence: str,
    verdict: str,
    reading: str,
    rejected: int,
    table_rows: int,
    reasons: tuple[tuple[str | None, int], ...],
    counts: tuple[int | None, int | None, int | None, int | None],
    prior: int | None,
    prior_incidents: int | None,
    remedy: str | None = None,
    started: str = "2026-07-24 11:02:31",
) -> dict[str, Any]:
    """The four results one incident would produce, as the mappings the assembler takes."""
    staged, promoted, quarantined, unaccounted = counts
    return {
        "incident": {
            "batch_id": batch, "source": source, "job_name": job, "attempts": 2,
            "first_started_at": started, "result_states": ["FAILED", "FAILED"],
        },
        "severity": {
            "batch_id": batch, "source": source, "severity": severity,
            "severity_rank": _rank(severity), "recommended_action": action,
            "hold_note": HOLDS[batch].why if batch in HOLDS else None,
            "rejected_rows": rejected, "quarantine_table_rows": table_rows,
            "evidence": evidence, "staged": staged, "promoted": promoted,
            "quarantined": quarantined, "unaccounted": unaccounted, "verdict": verdict,
            "remedy": remedy,
        },
        "census": [
            {"batch_id": batch, "reject_reason": reason, "rejected_rows": rows}
            for reason, rows in reasons
        ],
        "history": {
            "batch_id": batch, "executions_requested": N_EXECUTIONS,
            "prior_executions": prior, "prior_incidents": prior_incidents,
            "history": reading,
        },
    }


# The workspace's largest incident, its only stranding, and the only batch carrying a
# declared hold. The hold's note is read out of `severity.HOLDS` rather than quoted, so a
# reworded decision cannot leave this fixture asserting the old words.
PAYMENTS = _facts(
    batch=PAYMENTS_BATCH, source="payments", job="opl-bronze-payments",
    severity=DOES_NOT_RECONCILE, action=HOLD_DO_NOT_PROMOTE, evidence=ROWS_PRESENT,
    verdict=STRANDED_GATED, reading=INSUFFICIENT_HISTORY, rejected=2000, table_rows=2000,
    reasons=(("rescued_data_present", 2000),), counts=(10000, 0, 2000, 8000),
    prior=2, prior_incidents=0, remedy=PAYMENTS_REMEDY, started="2026-08-12 09:14:52",
)

# One rejected row, the mildest grade the ladder emits, and the first gate run its job ever
# had -- so it differs from payments in the severity, the action, the history reading and
# the vault leg all at once.
EMPRESAS = _facts(
    batch=EMPRESAS_BATCH, source="empresas", job="opl-bronze-cnpj-empresas",
    severity=ISOLATED_REJECTION, action=REVIEW_THE_QUARANTINED_ROWS, evidence=ROWS_PRESENT,
    verdict=RECONCILED, reading=NO_PRIOR_EXECUTION, rejected=1, table_rows=2,
    reasons=(("null_or_empty_razao_social", 1),), counts=(3, 2, 1, 0),
    prior=0, prior_incidents=0,
)

# One of the three lookup firings F4 accounts for: the quarantine table was recreated a week
# after them, so it is EMPTY. Four prior gate runs, all under the retired `dq_gate` spelling.
LOOKUP = _facts(
    batch=LOOKUP_BATCH, source="lookup", job="opl-bronze-cnpj-lookup",
    severity=EVIDENCE_REMOVED, action=INVESTIGATE_THE_QUARANTINE_TABLE,
    evidence=EVIDENCE_MISSING_QUARANTINE_EMPTY, verdict=NO_RECONCILIATION_ROW,
    reading=INSUFFICIENT_HISTORY, rejected=0, table_rows=0, reasons=((None, 0),),
    counts=(None, None, None, None), prior=4, prior_incidents=2,
)

# One of the two nothing in the record explains: its quarantine table is POPULATED -- it
# holds the four rows of another estabelecimentos incident -- and holds none of this batch's.
ESTABELECIMENTOS = _facts(
    batch=ESTAB_BATCH, source="estabelecimentos", job="opl-bronze-cnpj-estabelecimentos",
    severity=EVIDENCE_REMOVED, action=INVESTIGATE_THE_MISSING_BATCH,
    evidence=EVIDENCE_MISSING_BATCH_ABSENT, verdict=NO_RECONCILIATION_ROW,
    reading=INSUFFICIENT_HISTORY, rejected=0, table_rows=4, reasons=((None, 0),),
    counts=(None, None, None, None), prior=3, prior_incidents=1,
)

# The incident whose reject reason NAMES A DECLARED-PERSONAL COLUMN, which is why it is
# here: `null_or_empty_nome_socio_razao_social` is the gate's own word for what it rejected,
# the body must carry it, and it is the one legal occurrence of that name in a public
# artefact. Its whole-table count is the pair's 3,583 -- two incidents three weeks apart --
# against this one's 1,797.
SOCIOS = _facts(
    batch=SOCIOS_BATCH, source="socios", job="opl-bronze-cnpj-socios",
    severity=BULK_REJECTION, action=REVIEW_THE_QUARANTINED_ROWS, evidence=ROWS_PRESENT,
    verdict=RECONCILED, reading=NO_PRIOR_EXECUTION, rejected=1797, table_rows=3583,
    reasons=(("null_or_empty_nome_socio_razao_social", 1797),), counts=(1800, 3, 1797, 0),
    prior=0, prior_incidents=0,
)

CONSTRUCTED = (PAYMENTS, EMPRESAS, LOOKUP, ESTABELECIMENTOS, SOCIOS)


def issue(
    facts: dict[str, Any],
    *,
    severity: dict[str, Any] | None = None,
    history: dict[str, Any] | None = None,
    incident: dict[str, Any] | None = None,
    census: list[dict[str, Any]] | None = None,
    provenance: Provenance | None = None,
) -> TriageIssue:
    """One record from one set of facts, with any fact patched.

    THE PATCH IS A MERGE AND NOT A REPLACEMENT, so a test that changes `hold_note` changes
    that and nothing else -- which is what makes a rendering difference attributable to the
    field the test named."""
    return triage_issue(
        incident={**facts["incident"], **(incident or {})},
        severity={**facts["severity"], **(severity or {})},
        census=census if census is not None else facts["census"],
        history={**facts["history"], **(history or {})},
        provenance=provenance or PROVENANCE,
    )
