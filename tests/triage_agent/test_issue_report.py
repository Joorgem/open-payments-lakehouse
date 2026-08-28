# tests/triage_agent/test_issue_report.py
"""WHAT THE BODY SAYS, and that it says something different for a different incident.

NOTHING HERE STARTS SPARK, and that is a property of what is being tested rather than a
speed choice: a body is a pure function of a `TriageIssue`, so a session would add a JVM to
every assertion about prose and reach fewer states than five constructed records do. The
arm that needs Spark is `test_issue.py`, which runs the shipped statements and holds the
field names this record reads against the aliases those statements emit. NOTHING ENFORCES
THE SPLIT: adding a Spark test to this file would cost the property with no test going red,
which is the same unguarded seam `test_incidents_declaration.py`, `test_severity_
declaration.py` and `test_history_declaration.py` each record for themselves.

THE THING THIS FILE EXISTS TO REFUSE. An issue body that reads like competent analysis for
ANY incident is not triage. So the tests are written as DIFFERENCES rather than as golden
files: two golden files differ somewhere by construction and would pass over a template
that ignored every field it interpolated. What is asserted is that the workspace's largest
incident and its smallest disagree in the four columns a triager acts on, and that each
disagreeing word is in its own body and absent from the other's.

WHAT THAT DOES NOT REACH, AND IT IS MOST OF THE PROSE. The constant sentences in
`report.py` -- what an empty quarantine implies, what a missing gate run implies -- are read
off `docs/f6-run-evidence.md` and cannot be checked by any test here. What is checked is
that they are not INTERCHANGEABLE: each declared word renders its own sentence, no two are
equal, and neither removal may be worded with the other's account.

THE PRIVACY ARM IN THIS FILE IS THE NAME COUNT AND IT IS HALF OF A PAIR. It sweeps the
rendered body for declared-personal column NAMES after removing the reject reasons the gate
itself declares -- so a name inside `null_or_empty_nome_socio_razao_social` is legal and a
name anywhere else is not. IT IS BLIND TO EVERY LEAK THAT NEVER SPELLS THE NAME: a row
VALUE pasted into a field adds nothing to this count, and the only thing that sees that is
`test_issue.py`'s taint sweep, which renders bodies from rows the shipped statements
returned over a fixture where every value carries a sentinel -- and which is itself blind to
a value transformed away from that sentinel. Each arm's only cover is the other's measured
blind spot, which is the pairing T2 and T3 established for the three publishable statements
and the graded row.
"""
from __future__ import annotations

import importlib.util

import pytest

from opl.bronze.dq import RESCUED_REASON
from opl.bronze.masking import MASKED_COLUMNS
from opl.bronze.reconcile import BATCH_GRAIN_VIEW, RECONCILED
from opl.bronze.registry import table_spec
from opl.bronze.rules import rules_for
from opl.triage_agent import evidence as evidence_module
from opl.triage_agent import history as history_module
from opl.triage_agent import report as report_module
from opl.triage_agent.evidence import CENSUS_VERDICTS
from opl.triage_agent.history import (
    GATE_RUN_ABSENT,
    HISTORY_COMPLETE,
    HISTORY_READINGS,
    INSUFFICIENT_HISTORY,
    NO_PRIOR_EXECUTION,
)
from opl.triage_agent.issue import FACTS, NOTHING_RECORDED, Provenance
from opl.triage_agent.report import (
    CENSUS_MEANING,
    HISTORY_MEANING,
    _assert_every_census_verdict_has_a_meaning,
    _assert_every_history_reading_has_a_meaning,
    _assert_no_two_words_share_a_sentence,
    render_body,
    render_title,
)
from opl.triage_agent.severity import (
    HOLD_DO_NOT_PROMOTE,
    PROMOTE_THE_CLEAN_ROWS,
    SEVERITIES,
)

from .issue_facts import (
    CONSTRUCTED,
    EMPRESAS,
    ESTABELECIMENTOS,
    LOOKUP,
    PAYMENTS,
    PROVENANCE,
    SOCIOS,
    issue,
)

# Every column this project declares personal, across every contract. Read out of the
# declaration that creates the masks rather than listed here, so a third masked column
# enters this sweep in the commit that declares it.
_DECLARED_PERSONAL = tuple(
    sorted({column for columns in MASKED_COLUMNS.values() for column in columns})
)

# Sentences that assert a comparison was made, or that an incident is unremarkable. NONE OF
# THEM MAY APPEAR, because `history.py` counts prior executions and compares nothing --
# "compared against the last 5, nothing anomalous" is false for ten of this workspace's
# eleven incidents and is the exact species this phase hunts. The present tense "compare
# against" is legal and is in two of `report.py`'s own sentences; the banned form is the
# claim that it HAPPENED.
_CLAIMS_A_COMPARISON = (
    "compared against",
    "nothing anomalous",
    "no anomalies",
    "as expected",
    "business as usual",
    "looks normal",
    "within normal",
)


def _sections(body: str) -> dict[str, str]:
    """The body split on its own `##` headings, so a claim can be made about one section."""
    found: dict[str, str] = {}
    heading = "the headline"
    for line in body.split("\n"):
        if line.startswith("## "):
            heading = line.removeprefix("## ")
        found[heading] = f"{found.get(heading, '')}\n{line}"
    return found


def _declared_reject_reasons(source: str) -> tuple[str, ...]:
    """Every reject reason the gate can write for `source`, from the rules themselves.

    THE STRIP LIST IS THE GATE'S DECLARATION AND NOT THE PAYLOAD'S. A reason that appears in
    a body without being one of these is not stripped and is swept like any other text --
    which is what keeps this arm from being defeated by a crafted `_dq_reject_reason`, since
    the census reads that column out of the quarantine and it is a row value like any
    other."""
    return (*(reason for reason, _ in rules_for(source)), RESCUED_REASON)


def _outside_the_reject_reasons(body: str, source: str) -> str:
    """The body with every declared reject reason for `source` removed."""
    stripped = body
    for reason in _declared_reject_reasons(source):
        stripped = stripped.replace(reason, "")
    return stripped


def _reimported_report():
    """A SECOND execution of `report.py`'s module body, from its own file.

    Not `importlib.reload`, which would rebind the module every other test imported from.
    This is `test_evidence_contract.py`'s helper for its reason: the only way to observe
    what the import-time CALLS do is to run the body again."""
    spec = importlib.util.spec_from_file_location(
        "opl.triage_agent._report_reimported", report_module.__file__
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ----------------------------------------------------------------------------------
# The two headline incidents, and the differences a triager acts on.
# ----------------------------------------------------------------------------------


def test_the_two_headline_incidents_disagree_in_every_column_a_triager_acts_on():
    """`592660596679630` (payments, 2,000 rows, the only stranding) against
    `321750543973966` (empresas, 1 row).

    THE ASSERTION IS THE DIFFERENCE AND NOT A PAIR OF GOLDEN FILES. Two golden bodies differ
    somewhere by construction -- the batch id alone -- and would pass over a renderer that
    interpolated the id and ignored the grade, the recommendation, the history and the
    graph. Each word below is required to be in its own body AND absent from the other's,
    which is what makes the pair load-bearing: a template printing every word for every
    incident fails the second half.

    MEASURED, the mutation that reddens it: rendering `_headline` from the constants
    `does_not_reconcile`/`hold_do_not_promote` instead of the fields fails on the empresas
    side of the first two pairs."""
    payments, empresas = issue(PAYMENTS), issue(EMPRESAS)
    left, right = render_body(payments), render_body(empresas)

    differences = (
        (payments.severity, empresas.severity),
        (payments.recommended_action, empresas.recommended_action),
        (payments.history, empresas.history),
        ("has NO vault loader task", "hub_empresa"),
    )
    for mine, theirs in differences:
        assert mine != theirs
        assert mine in left and mine not in right, f"{mine!r} is not what separates them"
        assert theirs in right and theirs not in left

    assert render_title(payments) != render_title(empresas)
    assert payments.severity_rank == 1 and empresas.severity_rank == len(SEVERITIES)


def test_the_title_carries_the_table_the_batch_the_grade_and_the_recommendation():
    """A title naming only the table would collide between two incidents of one job --
    socios has two, three weeks apart -- and the two things a reader triages by are the
    grade and what it says to do."""
    for facts in CONSTRUCTED:
        drafted = issue(facts)
        title = render_title(drafted)
        for part in (drafted.source, drafted.batch_id, drafted.severity,
                     drafted.recommended_action):
            assert part in title, f"the title does not carry {part!r}"


def test_the_stranded_batch_says_do_not_promote_against_its_own_size():
    """THE PHASE'S HEADLINE CASE: the largest incident in the workspace, recommending that
    nothing be promoted, with both facts in the same body.

    The falsifier is the second half. `docs/f4-run-evidence.md` 1.2's decision is DECLARED
    (`severity.HOLDS`) and T3's tests prove the SQL recommendation flips when it is removed;
    what this asserts is that the BODY follows -- with the hold gone and the action back to
    what the ladder derives, no sentence in the issue still says do not promote, while the
    2,000 rows and the 8,000 unaccounted are unchanged. A body that said it from the size
    would pass the first half and fail here."""
    body = render_body(issue(PAYMENTS))
    assert "do not promote" in body.lower()
    assert HOLD_DO_NOT_PROMOTE in body
    assert "2,000" in body and "8,000" in body

    without = render_body(issue(
        PAYMENTS,
        severity={"hold_note": None, "recommended_action": PROMOTE_THE_CLEAN_ROWS},
    ))
    assert "do not promote" not in without.lower()
    assert PROMOTE_THE_CLEAN_ROWS in without
    assert "2,000" in without and "8,000" in without, "the size is not what changed"


def test_the_hold_note_is_quoted_whole_and_carries_its_citation():
    """Verbatim, not paraphrased: a hold nobody can trace to a decision is one the next
    operator deletes, which is `cadence.py`'s argument for the shape and T3's for the note.

    The citation is asserted as part of the note rather than separately, because what makes
    the decision followable is that the two arrive together."""
    drafted = issue(PAYMENTS)
    body = render_body(drafted)

    assert drafted.hold_note is not None
    assert drafted.hold_note in body
    assert "docs/f4-run-evidence.md 1.2" in body


# ----------------------------------------------------------------------------------
# The five with no quarantine evidence: not clean, and not one story.
# ----------------------------------------------------------------------------------


def test_neither_removal_renders_as_clean_or_as_the_mildest_thing_this_agent_emits():
    """Zero rejected rows reads as "nothing to see" in every vocabulary anyone reaches for,
    and it is the opposite: `fail_on_dq` runs only after the gate has appended rejected
    rows, so zero rows today is evidence removed after the fact.

    THE RANK IS ASSERTED AGAINST THE LADDER'S LENGTH AND NOT AGAINST THE NUMBER 2, so this
    keeps meaning "not the mildest" if an arm is ever added. `reconciled` is required to be
    absent because these five have no reconciliation row at all -- rendering the word for
    that absence as the word for agreement is the collapse `evidence.py` refuses.

    A FOURTH ASSERTION WAS HERE AND COULD NOT FAIL: `"0 rejected" not in body.lower()`, over
    a renderer that writes `rejected rows in THIS batch: **0**`. Reversed word order makes
    that phrase unproducible by this template for any incident, so it was a green line about
    nothing. What replaces it is a phrase this template CAN produce -- the mildest
    recommendation the ladder emits, which reaches the body through `_headline` and must not
    reach it for these two."""
    for facts in (LOOKUP, ESTABELECIMENTOS):
        drafted = issue(facts)
        body = render_body(drafted)

        assert drafted.severity_rank < len(SEVERITIES), "graded as the mildest thing we emit"
        assert "removed after the fact" in body
        assert RECONCILED not in body
        assert PROMOTE_THE_CLEAN_ROWS not in body


def test_the_unexplained_pair_does_not_borrow_the_explained_trio_s_account():
    """T2's two-word split, carried through to the prose or undone at the last step.

    The lookup's three firings are accounted for -- F4 records the table being recreated a
    week later -- and `187805471003061` and `315230730740144` are accounted for by nothing:
    the estabelecimentos quarantine is NOT empty and holds neither of them. One sentence for
    both would hand the unexplained pair the only account that exists.

    THE WHOLE-TABLE COUNT IS THE OTHER HALF and it is what a reader checks the sentence
    against: 0 rows for the emptied table, 4 for the populated one this batch is missing
    from."""
    emptied, missing = issue(LOOKUP), issue(ESTABELECIMENTOS)
    empty, absent = render_body(emptied), render_body(missing)

    assert CENSUS_MEANING[emptied.evidence] != CENSUS_MEANING[missing.evidence]
    assert "the quarantine table is EMPTY" in empty
    assert "the quarantine table is EMPTY" not in absent
    assert "holds OTHER batches' rows" in absent
    assert "holds OTHER batches' rows" not in empty
    assert "**0** rows in total" in empty and "**4** rows in total" in absent


def test_the_absent_reconciliation_is_rendered_as_absent_and_not_as_four_zeroes():
    """Five of eleven incidents have no row in `dataops_reconciliation`, and their staging
    rows are gone too. `0` staged would be a claim about the batch; what is true is a claim
    about the view.

    The section is asserted to EXIST for them: dropping it would render the question as one
    that did not arise, which reads milder than any wording of the answer."""
    body = render_body(issue(LOOKUP))
    section = _sections(body)["Whether the batch reconciles"]

    assert "has no row for this batch" in section
    assert "absent rather than zero" in section
    assert "staged 0" not in section


def test_a_declared_reason_the_gate_did_not_fire_is_named_and_the_census_no_rows_row_is_not():
    """The breakdown filtered on `if group.rows`, so a reason with zero rows vanished from
    the body while the sum check stayed green -- and "this reason exists and fired nothing"
    read exactly like "this reason does not exist".

    THE SECOND HALF IS WHAT THAT FILTER WAS FOR, asserted here so the fix cannot undo it.
    `quarantine_census_sql` emits one row -- NULL reason, 0 rows -- for a batch the
    quarantine does not hold, and printing it would put a breakdown line under the five
    incidents whose whole subject is that there is nothing to break down."""
    batch = PAYMENTS["severity"]["batch_id"]
    body = render_body(issue(PAYMENTS, census=[
        {"batch_id": batch, "reject_reason": "rescued_data_present", "rejected_rows": 2000},
        {"batch_id": batch, "reject_reason": "null_or_empty_valor", "rejected_rows": 0},
    ]))

    assert "  - `null_or_empty_valor`: 0" in body
    assert "no reject reason recorded" not in render_body(issue(LOOKUP))


def test_a_multi_line_remedy_stays_in_the_block_and_a_blank_one_is_not_a_missing_one():
    """Both halves of the remedy rendering. It is `dataops_reconciliation`'s own COLUMN, so
    what its lines say is not this file's to assume: a single interpolation after six spaces
    put line one inside the code block and dropped every line after it into the issue as
    markdown. And `if issue.remedy` answered "the ladder prints no remedy for this verdict"
    over a column that came back blank, which is a claim about the ladder made from a fact
    about one row.

    THE CONTROL IS THE SHIPPED ONE-LINE REMEDY, which must still render indented, and the
    absence arm on an incident that really has no remedy -- without both, this passes over a
    renderer that stopped indenting or that lost the absence sentence."""
    assert f"      {PAYMENTS['severity']['remedy']}" in render_body(issue(PAYMENTS))

    many = render_body(issue(PAYMENTS, severity={"remedy": "first --flag\nsecond --flag"}))
    assert "      first --flag\n      second --flag" in many

    blank = render_body(issue(PAYMENTS, severity={"remedy": ""}))
    assert "present and BLANK" in blank
    assert "no remedy is printed for this verdict" not in blank
    assert "no remedy is printed for this verdict" in render_body(issue(EMPRESAS))


def test_no_body_publishes_a_measurement_of_the_corpus_on_one_incidents_page():
    """Every `no_reconciliation_row` body carried "Five of this workspace's eleven incidents
    are in this state" -- a measurement of a corpus, published per incident, changing on the
    twelfth with nothing in this repository able to notice, in an artefact that outlives the
    count by however long the issue stays open.

    THE SWEEP IS OVER THE WHOLE CORPUS AND NOT ONLY THE TWO REMOVALS, because the same shape
    is one edit away in any section. What is banned is a claim about a NUMBER OF INCIDENTS;
    the counts that stay are all this incident's own."""
    for facts in CONSTRUCTED:
        body = render_body(issue(facts)).lower()
        for claim in ("eleven incidents", "of this workspace's", "five of this"):
            assert claim not in body, f"{issue(facts).batch_id} publishes {claim!r}"


# ----------------------------------------------------------------------------------
# History: the number found, and never a comparison.
# ----------------------------------------------------------------------------------


def test_the_history_line_moves_with_the_number_found():
    """"Compared against the last 5" without the count is the species in one sentence, and
    at N = 5 ten of this workspace's eleven incidents are short of the window.

    FOUR DIFFERENT COUNTS MUST PRODUCE FOUR DIFFERENT SECTIONS. A constant sentence carrying
    only the reading word passes any single-count assertion and fails this one, which is why
    the readings are varied WITH the counts rather than alone."""
    rendered = {}
    for prior, reading in ((0, NO_PRIOR_EXECUTION), (1, INSUFFICIENT_HISTORY),
                           (2, INSUFFICIENT_HISTORY), (5, HISTORY_COMPLETE)):
        body = render_body(issue(PAYMENTS, history={
            "prior_executions": prior, "history": reading,
        }))
        rendered[prior] = _sections(body)["What history there is to compare against"]

    assert len(set(rendered.values())) == 4, "the history section is not a function of the count"
    for prior, section in rendered.items():
        assert f"FOUND: **{prior}**" in section
        assert "against the **5** this window asks for" in section


def test_a_history_that_could_not_be_counted_publishes_no_number_at_all():
    """T4's third absence word. A batch whose own gate run has aged out of the telemetry has
    no anchor, so nothing was counted -- and `0` there asserts "this table was never gated
    before", the most reassuring wrong answer available.

    THE ASSERTION IS ON THE ABSENCE OF THE NUMBER, not on the presence of the word: a body
    could carry `gate_run_absent` beside a fabricated `0` and read as thorough."""
    section = _sections(render_body(issue(PAYMENTS, history={
        "prior_executions": None, "prior_incidents": None, "history": GATE_RUN_ABSENT,
    })))["What history there is to compare against"]

    assert "FOUND: **not measured**" in section
    assert "**0**" not in section
    assert "absent rather than zero" in section


def test_no_body_claims_a_comparison_that_this_agent_never_makes():
    """`history.py` COMPARES NOTHING and says so; the body has to say so too.

    THE CONTROL IS IN THE TEST. An absence sweep over phrases nothing produces is green for
    a reason that has nothing to do with the renderer, so the same reader is pointed at a
    doctored body and required to find one."""
    for facts in CONSTRUCTED:
        body = render_body(issue(facts)).lower()
        found = [claim for claim in _CLAIMS_A_COMPARISON if claim in body]
        assert not found, f"{issue(facts).batch_id} claims {found}"
        assert "no comparison was made against it" in body

    doctored = f"{render_body(issue(PAYMENTS))}\ncompared against the last 5, nothing anomalous"
    assert [claim for claim in _CLAIMS_A_COMPARISON if claim in doctored.lower()]


# ----------------------------------------------------------------------------------
# The blast radius: which tables, never how much of them.
# ----------------------------------------------------------------------------------


def test_the_downstream_section_names_tables_and_holds_no_magnitude_at_all():
    """T5's rule at the last layer. A proportion classifies socios near 100% in this
    package's fixture and near 0% on the deploy, with no test able to tell the two apart --
    so nothing this file renders about the graph may be a number.

    NO DIGIT IN THE SECTION IS THE WHOLE ASSERTION, and it is stronger than banning the word
    "percent": a table count ("4 tables affected") is a ranking too, and `severity.py` owns
    ranking. It holds because no gold or vault table in this project has a digit in its
    name, which is asserted here rather than assumed.

    AND EACH NAME IS SPELLED ONE WAY. This section prints its tables twice -- once inside
    `blast_radius_note`'s sentence and once as bullets two lines below -- and the bullets
    carried backticks the sentence cannot: its own test requires the sentence to START with
    the table name. Two typographies for one set of names, on one screen, reads as two
    different kinds of thing to the stranger this issue is written for."""
    for facts in CONSTRUCTED:
        drafted = issue(facts)
        section = _sections(render_body(drafted))["What else is downstream"]

        assert not any(character.isdigit() for character in section), section
        for table in (*drafted.radius.vault, *drafted.radius.gold):
            assert table in section
            assert f"`{table}`" not in section, "the sentence cannot fence it, so nor may the list"


def test_the_table_that_reaches_gold_without_a_vault_table_says_so_out_loud():
    """`payments` is `592660596679630`'s table and has no vault loader task in the bundle,
    so a manifest walked bronze -> vault -> gold answers "nothing downstream" for the
    workspace's largest incident. An empty vault leg rendered as an empty answer would be
    that defect one layer up.

    The empresas arm is the control: it has a vault leg, so the bypass sentence must NOT be
    reachable for every incident."""
    payments = _sections(render_body(issue(PAYMENTS)))["What else is downstream"]
    empresas = _sections(render_body(issue(EMPRESAS)))["What else is downstream"]

    assert "has NO vault loader task in the bundle and still reaches gold" in payments
    assert "vault tables: none" in payments
    assert "fact_payment" in payments
    assert "has NO vault loader task" not in empresas


# ----------------------------------------------------------------------------------
# The counts a body may not confuse, and the one it must not publish.
# ----------------------------------------------------------------------------------


def test_the_whole_table_count_is_never_rendered_as_this_incidents_size():
    """`quarantine_table_rows` is a whole-table `COUNT(*)`. On the socios pair it reads
    3,583 beside this incident's 1,797 -- two incidents three weeks apart -- and a body that
    rendered it as the incident's size would publish a 3,583-row incident twice.

    The two numbers are asserted to be on different lines and each line to carry only its
    own, which is the only form that catches a swap.

    THE TENSE IS THE THIRD THING ON THIS LINE. It read "currently holds", in an artefact a
    person opens weeks after it was written, anchored to nothing but the free-text run name
    in the last section -- so the count is stated as of the drafting, which is the only
    moment this body can speak for."""
    lines = render_body(issue(SOCIOS)).split("\n")
    size = next(line for line in lines if "rejected rows in THIS batch" in line)
    table = next(line for line in lines if "the quarantine table `" in line)

    assert "1,797" in size and "3,583" not in size
    assert "3,583" in table and "1,797" not in table
    assert "not this incident's" in table
    assert "when this issue was drafted" in table and "currently" not in table


def test_no_body_names_a_declared_personal_column_outside_a_reject_reason_the_gate_wrote():
    """THE NAME ARM OF THE PRIVACY PAIR, and it is a count rather than an absence because
    ONE occurrence is legal: `null_or_empty_nome_socio_razao_social` is the gate's own word
    for what it rejected, it comes from `opl.bronze.rules`, and the body must carry it or it
    stops saying why 1,797 rows were rejected.

    So the declared reasons are stripped and what is left is swept. An UNDECLARED reason is
    not stripped, which is deliberate: the census reads `_dq_reject_reason` out of the
    quarantine, so a reason is a row value and a crafted one would otherwise be a hole in
    this arm.

    BOTH CONTROLS ARE HERE. The socios body must contain the name at least once, or the
    strip is doing nothing and this passes over a body that never had the column; and the
    same reader is pointed at a body carrying the name in a field that is NOT a reason and
    required to catch it.

    WHAT IT CANNOT SEE: a leak that never spells the name -- a row VALUE pasted into any
    field. `test_issue.py`'s taint sweep is the only arm that reaches that one."""
    for facts in CONSTRUCTED:
        drafted = issue(facts)
        stripped = _outside_the_reject_reasons(render_body(drafted), drafted.source)
        named = [column for column in _DECLARED_PERSONAL if column in stripped]
        assert not named, f"{drafted.batch_id} names {named} outside a reject reason"

    assert "nome_socio_razao_social" in render_body(issue(SOCIOS)), "nothing to strip"

    leaked = render_body(issue(SOCIOS, incident={"job_name": "job-nome_do_representante"}))
    assert "nome_do_representante" in _outside_the_reject_reasons(leaked, "socios")


def test_the_body_separates_what_was_measured_from_what_was_declared():
    """A number in a published issue is either something a statement returned in a run that
    can be named, or something a human typed into this repository. Both are legitimate and
    they are not the same kind of thing, and a body that does not separate them asks its
    reader to take the whole artefact on one level of trust.

    THE ABSENCE ARM IS THE HALF THAT MATTERS. A run that recorded no statement id must say
    so in words -- an omitted line reads as a question that did not arise, which is the same
    failure `evidence.py` refuses for a missing reconciliation row."""
    section = _sections(render_body(issue(PAYMENTS)))["Where this came from"]

    assert PROVENANCE.produced_by in section
    for fact, identifier in PROVENANCE.statements:
        assert fact in section and identifier in section
    assert "DECLARED, not measured" in section
    assert "TABLE_OF_JOB" in section and "HOLDS" in section

    bare = _sections(render_body(issue(PAYMENTS, provenance=Provenance(
        produced_by="a run that recorded nothing else",
    ))))["Where this came from"]
    assert bare.count(NOTHING_RECORDED) == len(FACTS) + 1, bare
    assert "a run that recorded nothing else" in bare


def test_the_statement_lines_name_every_fact_and_not_only_the_ones_a_run_recorded():
    """THE SHIPPED PROVENANCES RECORD ONE ID FOR FOUR FACTS, so a list built from the
    caller's tuple printed one line and said nothing about three quarters of the body.

    What is asserted is that the four lines are the four FACTS, that the recorded one
    carries its id and the other three carry the word for its absence, and that adding a
    second id moves exactly one line -- which is what separates a keyed rendering from a
    constant block of four."""
    one = _sections(render_body(issue(PAYMENTS)))["Where this came from"]
    two = _sections(render_body(issue(PAYMENTS, provenance=Provenance(
        produced_by="a run that recorded two",
        statements=(("severity", "id-sev"), ("census", "id-cen")),
    ))))["Where this came from"]

    for fact in FACTS:
        assert f"  - {fact}: " in one, f"no line for the {fact} fact"
    unrecorded = {
        name: {fact for fact in FACTS if f"  - {fact}: {NOTHING_RECORDED}" in section}
        for name, section in (("one", one), ("two", two))
    }

    assert unrecorded["one"] == set(FACTS) - {"severity"}
    assert unrecorded["two"] == set(FACTS) - {"severity", "census"}
    assert "id-sev" in two and "id-cen" in two


def test_the_headline_says_which_absence_each_of_its_three_arms_met():
    """THE THREE LINES NOBODY WATCHED, in the section every body opens with.

    `job_name`, the terminal states and `first_started_at` each have an absence arm and none
    of them had a test. All three are reachable: `incidents.py` reads `job_name` from
    `system.lakeflow.jobs`, which forgets a deleted job; `result_states` is a `COLLECT_LIST`
    that skips NULLs, so an attempt with no terminal state yet contributes nothing; and
    `first_started_at` is NULL for a run the timeline has no started attempt for. In a
    package whose doctrine is that an absence must never render as a measurement, these were
    the three renderings with nothing behind them.

    THE PRESENT ARM IS THE CONTROL AND IS ASSERTED ON THE SAME FIELDS: without it, a headline
    that printed the absence word unconditionally would pass every line below."""
    present = _sections(render_body(issue(PAYMENTS)))["the headline"]
    assert f"- job: `{PAYMENTS['incident']['job_name']}`" in present
    assert f"- first attempt started: `{PAYMENTS['incident']['first_started_at']}`" in present
    assert "terminal states `FAILED`, `FAILED`" in present

    absent = _sections(render_body(issue(PAYMENTS, incident={
        "job_name": None, "first_started_at": None, "result_states": [],
    })))["the headline"]

    assert "- job: `not recorded in the telemetry`" in absent
    assert "terminal states none recorded" in absent
    assert "- first attempt started: `not recorded`" in absent
    assert "None" not in absent, "the word for the absence, never the value's repr"


def test_the_relations_the_body_names_are_derived_and_the_one_that_is_not_says_so():
    """B1: `read_from` was a free tuple a caller filled in and no test held -- demonstrated
    by hardcoding a wrong relation and watching every body in the corpus name the payments
    quarantine with nothing objecting.

    THE ASSERTION IS THAT THE NAMES MOVE WITH `source`, which is what "derived" means and
    what a free field cannot do: the socios body names the socios quarantine and not the
    payments one, and neither body can name a relation the caller did not supply. The
    telemetry view is the control in the other direction -- it is still the caller's word,
    so it must be labelled as such and must NOT be labelled DERIVED.

    ITS UNSET ARM HAS ITS OWN WORDING rather than the word `not recorded` where a name goes.
    Every sibling line in this section ends in a fenced relation name; that one ended in a
    bare phrase -- "the task telemetry view not recorded, which ..." -- and scanned as a
    dropped word rather than as an absence."""
    payments = _sections(render_body(issue(PAYMENTS)))["Where this came from"]
    socios = _sections(render_body(issue(SOCIOS)))["Where this came from"]

    for drafted, section, other in ((issue(PAYMENTS), payments, socios),
                                    (issue(SOCIOS), socios, payments)):
        quarantine = table_spec(drafted.source).quarantine
        assert f"DERIVED by this wheel: the quarantine `{quarantine}`" in section
        assert quarantine not in other
        assert f"DERIVED by this wheel: the reconciliation view `{BATCH_GRAIN_VIEW}`" in section

    named = _sections(render_body(issue(PAYMENTS, provenance=Provenance(
        produced_by="a run that named its telemetry view", telemetry_view="cat.sch.a_view",
    ))))["Where this came from"]
    assert ("THE CALLER'S WORD, checked by nothing: the task telemetry view "
            "`cat.sch.a_view`") in named
    assert "DERIVED by this wheel: the task telemetry view" not in named
    assert ("the task telemetry view that the incident feed and the history read was "
            f"{NOTHING_RECORDED} by this run") in payments
    assert f"telemetry view {NOTHING_RECORDED}," not in payments


def test_a_backtick_in_a_row_value_cannot_turn_the_rest_of_the_body_into_markdown():
    """`reject_reason` IS A QUARANTINE ROW VALUE, read out of `_dq_reject_reason`, and
    `test_issue_report.py` already reasons about a crafted one for the name sweep -- then
    hands the same string to a public issue, where `@handle` notifies a real person and
    `#123` cross-links a real issue.

    THE ASSERTION IS ON THE FENCE AND NOT ON THE ABSENCE OF THE CHARACTERS. Removing `@`
    would be a redaction of a value this body is required to carry; what has to hold is that
    the span the value sits in does not CLOSE inside it, which is CommonMark's rule and is
    checked here by requiring a fence longer than the run the value contains.

    WHAT IT DOES NOT COVER: markdown outside a code span. `hold_note` is quoted whole as a
    blockquote and is declared in this repository, and no test here holds that line."""
    def _breakdown(reason: str) -> str:
        body = render_body(issue(PAYMENTS, census=[
            {"batch_id": PAYMENTS["severity"]["batch_id"],
             "reject_reason": reason, "rejected_rows": 2000},
        ]))
        return next(line for line in body.split("\n") if "@torvalds" in line)

    crafted = "null_or_empty` @torvalds `x"
    assert _breakdown(crafted) == f"  - ``{crafted}``: 2,000"
    assert _breakdown("a``b @torvalds") == "  - ```a``b @torvalds```: 2,000"


# ----------------------------------------------------------------------------------
# The declared sentences, and the three guards that keep them total.
# ----------------------------------------------------------------------------------


def test_every_declared_word_renders_its_own_sentence():
    """Total in both directions over both vocabularies, and no sentence shared.

    T2 split one absence word into two and T4 split another into three BECAUSE they mean
    different things. Two words rendering the same sentence would undo both splits in the
    one place a stranger reads, while every vocabulary test in this package stayed green."""
    assert set(CENSUS_MEANING) == set(CENSUS_VERDICTS)
    assert set(HISTORY_MEANING) == set(HISTORY_READINGS)

    sentences = [*CENSUS_MEANING.values(), *HISTORY_MEANING.values()]
    assert len(set(sentences)) == len(sentences)


@pytest.mark.parametrize(
    ("name", "value", "guard"),
    (
        ("CENSUS_VERDICTS", ("a_verdict_with_no_sentence",),
         _assert_every_census_verdict_has_a_meaning),
        ("HISTORY_READINGS", ("a_reading_with_no_sentence",),
         _assert_every_history_reading_has_a_meaning),
    ),
)
def test_a_word_with_no_sentence_here_is_refused(monkeypatch, name, value, guard):
    """A word with no sentence raises `KeyError` at RENDER time -- during a publish, on the
    incident that has it -- and the word most likely to be added is another absence.

    THE MUTATION IS ON `report.py`'s OWN BINDING AND THAT IS WHAT THE GUARD READS: `from
    evidence import CENSUS_VERDICTS` copies the tuple into this module's globals, so
    patching `evidence` after import moves nothing here -- measured, and it is why this
    parametrisation was rewritten. The arm that proves a word added UPSTREAM is caught is
    the re-import test at the foot of this file, where the module body runs again and reads
    the patched module."""
    monkeypatch.setattr(report_module, name, value)
    with pytest.raises(ValueError, match="no sentence for"):
        guard()


def test_a_sentence_left_behind_by_a_rename_is_refused_too(monkeypatch):
    """The other direction. A meaning whose word no longer exists sits in the table looking
    like coverage, and the incident that used to reach it now renders through a `KeyError`
    instead."""
    monkeypatch.setitem(CENSUS_MEANING, "a_verdict_evidence_does_not_emit", "a sentence")
    with pytest.raises(ValueError, match="sentences for"):
        _assert_every_census_verdict_has_a_meaning()


def test_two_words_sharing_one_sentence_is_refused(monkeypatch):
    """The guard that holds what the splits BOUGHT: the vocabulary can keep a distinction
    the artefact a person reads has lost."""
    monkeypatch.setitem(CENSUS_MEANING, "rows_present", HISTORY_MEANING[HISTORY_COMPLETE])
    with pytest.raises(ValueError, match="render the same sentence"):
        _assert_no_two_words_share_a_sentence()


def test_the_guards_run_at_import_so_deleting_a_call_is_a_failure_not_a_silent_loss(monkeypatch):
    """Calling a guard in a test and watching it raise says the guard works. It says nothing
    about whether anything CALLS it -- and T1 shipped exactly that gap, where deleting both
    import-time calls left its suite green.

    The first line is the control: re-executing an unmutated module must succeed, or the
    raise below could be about the re-execution rather than about the declaration."""
    assert _reimported_report().CENSUS_MEANING == CENSUS_MEANING

    monkeypatch.setattr(evidence_module, "CENSUS_VERDICTS", ("a_verdict_with_no_sentence",))
    with pytest.raises(ValueError, match="no sentence for"):
        _reimported_report()


def test_the_second_guard_runs_at_import_too_and_is_fired_from_the_other_module(monkeypatch):
    """Two guards, two import-time calls, each fired separately: one test covering both
    would pass with one of the calls deleted. The mutation is in `history.py`, because that
    is the module whose split this sentence table exists to carry through."""
    assert _reimported_report().HISTORY_MEANING == HISTORY_MEANING

    monkeypatch.setattr(history_module, "HISTORY_READINGS", ("a_reading_with_no_sentence",))
    with pytest.raises(ValueError, match="no sentence for"):
        _reimported_report()
