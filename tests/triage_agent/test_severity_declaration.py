"""What the grade is DECLARED to be, and what it may publish. No Spark in here.

SPLIT OUT OF `test_severity.py` AT A SEAM THAT WAS MEASURED RATHER THAN CHOSEN, when that
file reached 798 lines against this repository's 800-line cap. Nothing below takes `probe`,
`spark` or a relation handed through `view=`, so nothing here waits for the session the
other half needs. The move was made in a commit free of behaviour: no assertion edited,
nothing renamed, and the two files hold the tests the one file held. `_HELD_BATCH` is the
only name the UNDIVIDED file defined that both halves read, and it now lives in `conftest.py`
beside the `_PAYMENTS_BATCH` it aliases -- one definition, because two would be the drift its
own comment refuses. Other names are live in both halves; every one of them is IMPORTED by
both, and neither half defines a module-level name the other defines too.

AND NOTHING ENFORCES THE "NO SPARK" IN THE FIRST SENTENCE, which is stated here rather than
left to be assumed. A Spark test added to this file would silently cost it the whole point
of the seam and no test would go red: `tests/test_size_caps.py` measures lines, and nothing
in this repository measures whether a JVM started. `test_incidents_declaration.py` faced the
identical choice earlier in this phase and did not build the guard either -- every cheap
spelling is this repository's hunted species one level down, since a signature scan passes
while a module-scope session, an autouse fixture or a transitive import still starts a JVM.
`docs/f6-run-evidence.md` section 3 now records the property as unguarded for BOTH files and
names this one, with the no-JVM time measured on it -- which is what this project does with a
property it has chosen not to protect. It also records that one of the two reasons originally
given for not building the guard, that it would be a ONE-FILE special case, stopped holding
when the second file appeared, leaving the blindness stated above as the whole of what still
decides it. AND RECORDED IS NOT GUARDED: a Spark test added here goes green either way,
because section 3 is prose and no test reads it.

WHAT THE TWO DOCUMENT LOCKS HOLD AND WHAT THEY DO NOT. They hold that the hold's note still
quotes the three figures `docs/f4-run-evidence.md` 1.2 carries, and that
`_POPULATION_SCALE_ROWS` is still the number ADR 0006 condition 2 argues for -- each token
required on BOTH sides, so the test is red whichever one moves. They do not say either
figure is the RIGHT one for a triage grade: ADR 0006 derived its line for a rate estimate's
relative error, which `severity.py`'s header states as a non-transfer, and nothing here or
anywhere else in this repository closes that gap.

THE GUARDS ARE PROVEN ABLE TO FAIL AND PROVEN TO RUN, which are two claims and the second is
the one a `pytest.raises` sibling cannot make on its own. Each guard is fired on the
declaration it exists to refuse, and each import-time CALL is fired by re-executing
`severity.py`'s body -- one test per call, because a single test covering both stays green
with either call deleted. NO TOTAL IS QUOTED FOR ANY OF IT, which is
`test_evidence_contract.py`'s ruling: a total goes stale on the next commit that adds a
test. What none of it covers is a guard nobody wrote -- these show that the two which exist
are asked, not that they are the two that should exist.

AND WHAT THE GRADED STATEMENT MAY PUBLISH, WHICH HAS ONE ARM HERE AND ONE IN ANOTHER FILE.
This half counts declared-personal column NAMES in the generated SQL; that arm cannot see a
leak which never spells one, and `severity_sql` emits three `SELECT *`. The other arm is
`test_evidence_sample.py`'s taint sweep, which reads the VALUES the statement returns, and
the pairing is stated in both files because neither is sufficient alone. Beside it sits the
refusal that keeps an incident whose source is NULL from being graded at all.

THE SECTION NUMBERS BELOW ARE INHERITED FROM THE UNDIVIDED FILE and therefore start at 5,
which is `test_evidence_census.py`'s convention rather than a gap. The two tests above them
came from its sections 1 and 2 and are grouped by what they read rather than renumbered.
"""
from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

from opl.bronze import reconcile as reconcile_module
from opl.bronze.reconcile import RECONCILED
from opl.bronze.registry import REGISTRY, UnknownTable
from opl.contracts.catalogue import columns_for
from opl.triage_agent import severity as severity_module
from opl.triage_agent.severity import (
    _POPULATION_SCALE_ROWS,
    BULK_REJECTION,
    HOLDS,
    SEVERITIES,
    Hold,
    _assert_every_hold_names_a_registered_table_and_carries_a_reason,
    _assert_no_grade_is_spelled_twice,
    severity_sql,
)

from .conftest import _CONFIG, _HELD_BATCH

_REPO = Path(__file__).resolve().parents[2]
_F4_EVIDENCE = _REPO / "docs" / "f4-run-evidence.md"
_ADR_0006 = _REPO / "docs" / "adr" / "0006-bronze-dq-gate-policy.md"


# ----------------------------------------------------------------------------------
# The two documents this module borrows a figure from, opened rather than trusted.
# ----------------------------------------------------------------------------------


def test_the_hold_cites_a_section_that_exists_and_carries_the_measurement():
    """THE CITATION RESOLVES, checked by opening the document rather than by trusting it.

    This phase has already shipped one citation pointing at a section carrying no such
    measurement, and repointed seven more that named a deleted file -- `docs/f6-run-
    evidence.md` 0.9 exists because of the first. So the note's pointer is followed here:
    the section has to exist AND to carry the three numbers the note quotes from it.

    EACH TOKEN IS REQUIRED IN BOTH PLACES, so the test is red whichever side moves: a note
    that stops quoting the measurement, or a document that stops carrying it. Asserting
    them against the document alone would have been a lock on somebody else's file that
    this module's own prose could drift away from.

    THE SLICE ENDS AT THE NEXT HEADING OF ANY DEPTH, not the next level-2 one, or a later
    `### 1.3` in this chapter would silently widen it into somebody else's section. `docs/`
    is not in the wheel, which is why this is a test and not an import guard."""
    note = HOLDS[_HELD_BATCH].why
    text = _F4_EVIDENCE.read_text(encoding="utf-8")
    marker = "### 1.2 THE DECISION ON `592660596679630`"
    assert marker in text, f"{_F4_EVIDENCE.name} has no section 1.2 for the hold to cite"

    section = re.split(r"\n#{2,} ", text.split(marker, 1)[1], maxsplit=1)[0]
    for quoted in ("40,150", "40,000", "48,150", "gold_load_fact.py:125-143"):
        assert quoted in note, f"{quoted!r} left the hold's note; the argument is the note"
        assert quoted in section, (
            f"{quoted!r} is in the hold's note and not in the section the note cites. A "
            "citation that resolves to a section without the measurement is the defect "
            "docs/f6-run-evidence.md 0.9 was written to close"
        )
    assert HOLDS[_HELD_BATCH].source == "payments", (
        "the hold declares which table the decision is about, and docs/f6-run-evidence.md "
        "0.3 records this job run as the payments incident"
    )


def _collapsed(text: str) -> str:
    """Whitespace-collapsed, with the ADR's U+2265 spelled `>=`.

    One quoted sentence wraps at a different column in a markdown paragraph and in a Python
    docstring, so neither can be searched for the other's line breaks. And the ADR is
    markdown, which may hold U+2265; this repository's Python is ASCII, which may not --
    so the character is named by CODE POINT here rather than typed."""
    return " ".join(text.replace(chr(0x2265), ">=").split())


def test_the_population_scale_threshold_is_the_number_ADR_0006_argues_for():
    """THE FILE'S ONE CHOSEN NUMBER, HELD TO THE DOCUMENT IT IS BORROWED FROM.

    The corpus pins `_POPULATION_SCALE_ROWS` only to the open interval (4, 1797] -- 137
    passes every other test in this file. The header derives 10 from ADR 0006 condition 2
    and calls it the part of the file to argue with, so the derivation is FOLLOWED here the
    way `test_the_hold_cites_a_section_that_exists_and_carries_the_measurement` follows the
    hold's citation: the figure is read out of the document and compared, and the quoted
    sentence is required on both sides so the test is red whichever one moves.

    WHAT IT LOCKS AND WHAT IT CANNOT. It locks that the constant IS the ADR's figure and
    that this module still quotes the sentence it came from. It cannot say 10 is the RIGHT
    boundary for a triage grade -- ADR 0006 derived it for a rate estimate's relative
    error, which the header now states as a non-transfer -- and it does not cover the
    comment beside the constant, which restates the header where no test reads."""
    adr = _collapsed(_ADR_0006.read_text(encoding="utf-8"))
    found = re.search(r"reject count >= (\d+), so the Poisson relative error", adr)
    assert found, f"{_ADR_0006.name} no longer carries the sentence this module quotes"
    assert int(found.group(1)) == _POPULATION_SCALE_ROWS, (
        f"ADR 0006 condition 2 draws its line at {found.group(1)} and this module uses "
        f"{_POPULATION_SCALE_ROWS}. One of the two moved, and the header's derivation "
        "stopped being one"
    )

    quoted = (
        f"with a reject count >= {_POPULATION_SCALE_ROWS}, so the Poisson relative error "
        "falls under ~30%"
    )
    assert quoted in adr, "the ADR stopped carrying the measurement the header quotes"
    assert quoted in _collapsed(severity_module.__doc__), (
        "the header stopped quoting the sentence the constant is borrowed from, so the "
        "number is invented here after all"
    )


# ----------------------------------------------------------------------------------
# 5. The declaration's own guards, each proven able to fail and proven to RUN.
# ----------------------------------------------------------------------------------


def test_a_hold_on_a_table_no_registry_declares_is_refused(monkeypatch):
    """Proves the guard can fail, in the shape it would fail in: a hold naming a table
    nothing registers can never be carried by an incident, so the batch it was written for
    would be recommended for promotion by a declaration that looks present."""
    broken = {"999": Hold(source="a_table_no_registry_declares", why="x")}
    monkeypatch.setattr(severity_module, "HOLDS", broken)
    with pytest.raises(ValueError, match="not a registered bronze table"):
        _assert_every_hold_names_a_registered_table_and_carries_a_reason()


def test_a_hold_with_no_reason_is_refused(monkeypatch):
    """A refusal to act that cites nothing is one the next operator deletes -- which is
    `cadence.py`'s argument for the same guard on its own `why`."""
    monkeypatch.setattr(severity_module, "HOLDS", {"999": Hold(source="payments", why="   ")})
    with pytest.raises(ValueError, match="declares a hold with no reason"):
        _assert_every_hold_names_a_registered_table_and_carries_a_reason()


def test_a_severity_that_collides_with_a_reconciliation_verdict_is_refused(monkeypatch):
    """One string answering two questions on one row, refused at import.

    The result row carries `verdict`, `evidence` and `severity` side by side. A severity
    spelled the same as one of the other two would be indistinguishable in every output
    that formats them -- `evidence._assert_the_absence_word_is_not_a_reconciliation_verdict`
    states the requirement for one word and this applies it to ten."""
    monkeypatch.setattr(severity_module, "SEVERITIES", (RECONCILED, *SEVERITIES[1:]))
    with pytest.raises(ValueError, match="graded words here AND verdicts"):
        _assert_no_grade_is_spelled_twice()


def test_a_severity_spelled_twice_is_refused(monkeypatch):
    """The other direction, and it is silent without this: `severity_rank_sql` builds its
    map from `SEVERITIES`, so a repeated word simply loses one rank and two arms of the
    ladder become indistinguishable."""
    monkeypatch.setattr(severity_module, "SEVERITIES", (BULK_REJECTION, *SEVERITIES[1:]))
    with pytest.raises(ValueError, match="spells a word twice"):
        _assert_no_grade_is_spelled_twice()


def _reimported_severity():
    """A SECOND execution of `severity.py`'s module body, from its own file.

    Registered under a throwaway name in `sys.modules` and removed again, for the reason
    `tests/dataops/test_cadence.py`'s sibling gives: this module declares a `@dataclass`
    under `from __future__ import annotations`, so `dataclasses` resolves the string
    annotations by looking the defining class's `__module__` up in `sys.modules` and
    raises `AttributeError` on a module that is not there."""
    spec = importlib.util.spec_from_file_location(
        "opl.triage_agent._severity_reimported", severity_module.__file__
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        del sys.modules[spec.name]
    return module


def test_the_FIRST_guard_runs_at_import_so_deleting_the_call_is_a_failure_not_a_silent_loss(
    monkeypatch,
):
    """The half every `pytest.raises` sibling above leaves open.

    Each guard is proven able to fail; none of those proves it is ever ASKED. T1's review
    measured exactly this gap in its own file -- deleting both import-time calls left the
    suite green -- so the refusal has to come out of the IMPORT here.

    ONE TEST PER CALL, WHICH IS WHY ITS SIBLING BELOW EXISTS: a single test covering both guards
    passes with either call deleted, so each call has a mutation only its own test sees --
    deleting `_assert_no_grade_is_spelled_twice()`'s leaves THIS test green and the sibling red.
    NO TOTAL IS QUOTED FOR IT, and the sentence that stood here quoted one already gone stale: a
    total moves on the next commit that adds a test, which `test_evidence_contract.py`'s header
    ruled one task earlier. The first line is the control: an UNMUTATED re-execution must succeed,
    or the raise below could be about the re-execution rather than the declaration."""
    assert _holds_of(_reimported_severity()) == _holds_of(severity_module)

    monkeypatch.delitem(REGISTRY, HOLDS[_HELD_BATCH].source)
    with pytest.raises(ValueError, match="not a registered bronze table"):
        _reimported_severity()


def test_the_SECOND_guard_runs_at_import_too_and_is_fired_from_the_other_module(monkeypatch):
    """The other import-time call, fired the way T2 fires its own second guard.

    THE MUTATION IS MADE IN `reconcile.py` BECAUSE THAT IS WHERE THE COLLISION COMES FROM.
    This guard refuses a graded word that is also a verdict published on the same row, and
    the way that ships is a rename one module away. `severity.py` reads those four verdict
    names at import, so re-executing its body against a renamed one is exactly the commit
    that would put one string on a row answering two questions. First line is the
    control."""
    assert _reimported_severity().SEVERITIES == SEVERITIES

    monkeypatch.setattr(reconcile_module, "RECONCILED", BULK_REJECTION)
    with pytest.raises(ValueError, match="graded words here AND verdicts"):
        _reimported_severity()


def _holds_of(module) -> dict[str, tuple[str, str]]:
    """`HOLDS` as plain fields, so two executions of the module can be compared.

    NOT `==` on the dataclasses: the re-executed module defines its own `Hold` class and
    `dataclass.__eq__` returns `NotImplemented` across classes, so equal declarations
    would compare unequal for a reason that is about the re-execution."""
    return {batch: (hold.source, hold.why) for batch, hold in module.HOLDS.items()}


# ----------------------------------------------------------------------------------
# 6. What the graded statement may publish, and what it must refuse to read.
# ----------------------------------------------------------------------------------


def test_the_graded_statement_READS_no_declared_personal_column():
    """T2's lock, applied to the statement T3 adds, rather than left to a hand check.

    `severity_sql` is a NEW publishable statement and T6 puts what it returns into a GitHub
    issue. T2 states the rule -- `test_no_publishable_statement_READS_a_declared_personal_
    column` -- for the three statements it ships; this brings the fourth under it rather
    than leaving the next reader to verify by eye.

    THIS ARM DEMANDS MORE THAN T2's AND SEES NO MORE -- the sentence that stood here ranked it on
    the first half alone. `row_shapes_sql` may NAME a personal column, as a map key beside
    `masked`, because a triager must be told the column exists and that its value is withheld, so
    T2's check is a COUNT of exactly one, while `severity_sql` works only at batch grain and may
    name NO contract column at all. Neither arm sees a leak that never spells the name, and
    `severity_sql` emits three `SELECT *`. The other arm is `test_evidence_sample.py`'s taint
    sweep, which now walks this statement over `_TAINT_SWEEP` and reads the values it returns.

    THE CONTROL IS IN THE SAME STRING, because an absence assertion over the wrong text
    passes trivially -- which is how two successive versions of T1's cross-module lock
    passed under the mutation they existed to catch. The statement must read the socios
    quarantine and carry all four severity words, or it is not this statement."""
    statement = severity_sql("socios", _CONFIG)

    named = [column for column in columns_for("socios") if column in statement]
    assert named == [], (
        f"severity_sql names {named}. Severity is a property of the incident, both of the "
        "statements it composes work at batch grain, and this one is destined for a "
        "published issue -- so there is no column of the contract it has a reason to touch"
    )
    assert _CONFIG.table(REGISTRY["socios"].quarantine) in statement, "the control"
    assert all(word in statement for word in SEVERITIES), "the control"


def test_an_incident_whose_source_is_null_is_refused_rather_than_graded():
    """T1 emits a NULL `source` for a DQ gate on a job its declaration does not know, and
    that incident has no quarantine to census. Grading it would render a stale declaration
    exactly like a clean batch; the refusal is `evidence.py`'s and is inherited, not
    re-spelled."""
    with pytest.raises(UnknownTable, match="no bronze table can be resolved"):
        severity_sql(None, _CONFIG)
