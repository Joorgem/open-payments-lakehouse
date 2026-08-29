# tests/triage_agent/test_issue_payload.py
"""WHAT THE RECORD REFUSES TO BE, and what survives a file. No Spark here either.

SEPARATED FROM `test_issue_report.py` AT A SUBJECT SEAM AND BEFORE EITHER FILE WAS LONG,
which is the lesson three files in this package learned the expensive way: two of them were
split after reaching the cap, at 845 and 798 lines, each costing a review pass. That file
answers "what does the body SAY"; this one answers "what may be assembled at all, and what
survives being written to disk and read back by another process" -- different subjects that
change for different reasons.

THE REFUSALS ARE THE POINT OF THIS FILE AND EVERY ONE OF THEM IS REACHABLE BY A CORRECT-
LOOKING CALLER. Four statements are four round trips; the results are four mappings; and
nothing in a result row says which incident the row beside it is about. A caller that loops
the feed and reuses one variable produces an issue whose grade is one incident's and whose
history is another's, and every field in it is real. That artefact is fluent, plausible,
and about nothing -- which is this phase's species with the numbers filled in.

WHAT IS NOT REFUSED, AND IT IS NAMED HERE SO THE LIST ABOVE IS NOT READ AS COVER. Nothing
checks that the facts are FRESH, that the four statements ran against the same workspace, or
that the incident is still in the feed. A quarantine emptied between the census and the
grade is invisible to every check in this file. What the record carries about that is the
provenance block, and what a reader has is the run it names.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from opl.bronze.registry import UnknownTable
from opl.triage_agent.blast_radius import blast_radius
from opl.triage_agent.issue import (
    CENSUS_FACTS,
    FACTS,
    HISTORY_FACTS,
    INCIDENT_FACTS,
    SEVERITY_FACTS,
    MismatchedFacts,
    MissingFact,
    Provenance,
    as_mapping,
    from_mapping,
    payloads_from_json,
    triage_issue,
)
from opl.triage_agent.report import render_body, render_title
from opl.triage_agent.severity import HOLDS, SEVERITIES

from .issue_facts import CONSTRUCTED, EMPRESAS, LOOKUP, PAYMENTS, PROVENANCE, issue

_REPO = Path(__file__).resolve().parents[2]

# The one path shape this file drives at the two carriers `produced_by`'s own test does
# not reach. INVENTED, like every operator name in that test and for its reason.
_OPERATOR_PATH = r"C:\Users\an_operator\repo\out\facts.json"


def _json(*records) -> str:
    """What a producer writes: a list of payload mappings, as JSON."""
    return json.dumps([as_mapping(record) for record in records])


# ----------------------------------------------------------------------------------
# One incident, or nothing.
# ----------------------------------------------------------------------------------


def test_facts_from_two_incidents_are_refused_rather_than_fused():
    """The grade of the workspace's largest incident beside another incident's history.

    Every field in that record would be real and the body would read as competent analysis
    of a batch nobody looked at. It is one reused variable away in any caller that loops the
    feed, which is the only way anybody will ever call this."""
    with pytest.raises(MismatchedFacts, match="more than one incident"):
        triage_issue(
            incident=PAYMENTS["incident"], severity=PAYMENTS["severity"],
            census=PAYMENTS["census"], history=EMPRESAS["history"], provenance=PROVENANCE,
        )


def test_a_census_from_another_incident_is_caught_by_its_batch_id():
    """The census is the one fact handed in as a LIST, so it is the one a caller is most
    likely to build in a comprehension over the wrong loop variable."""
    with pytest.raises(MismatchedFacts, match="more than one incident"):
        issue(PAYMENTS, census=EMPRESAS["census"])


def test_two_facts_naming_two_bronze_tables_are_refused():
    """THE SECOND HALF AND NOT A RESTATEMENT OF THE FIRST. Two incidents of one job share no
    batch id, so the batch check cannot see a caller that read the incident from the feed
    and the grade from another table's cached result for the same batch."""
    with pytest.raises(MismatchedFacts, match="more than one bronze table"):
        issue(PAYMENTS, severity={"source": "socios"})


def test_a_census_that_does_not_sum_to_the_graded_number_is_refused():
    """The one cross-check two of the four facts allow. `severity_sql` SUMS this same census
    inside SQL, so a breakdown that does not add up to the graded number is a census from
    another reading of the quarantine -- and both results are internally consistent, so no
    other column can see it.

    The direction matters and is asserted: the message names both numbers, because "they
    disagree" without them sends a reader to the wrong statement."""
    with pytest.raises(MismatchedFacts, match="sum to 1 rejected rows.*computed from 2000"):
        issue(PAYMENTS, census=[
            {"batch_id": PAYMENTS["severity"]["batch_id"],
             "reject_reason": "rescued_data_present", "rejected_rows": 1},
        ])


@pytest.mark.parametrize(
    ("fact", "keys"),
    (("incident", INCIDENT_FACTS), ("severity", SEVERITY_FACTS), ("history", HISTORY_FACTS)),
)
def test_a_renamed_column_refuses_the_issue_instead_of_publishing_a_blank(fact, keys):
    """EVERY declared key of every fact, dropped one at a time.

    The field names here are the aliases another module's SQL emits and nothing but a test
    holds the two equal. What this asserts is the FAILURE MODE of that gap: a rename arrives
    as a refusal to build the issue, not as a body with a blank where the grade goes. A
    `.get()` in the reader would turn all of these green and publish the blank.

    `batch_id` is dropped through the same path but refuses in the identity check first, so
    the message differs; the assertion below is on the key's NAME, which both carry."""
    for key in keys:
        facts = {name: dict(rows) if name != "census" else list(rows)
                 for name, rows in PAYMENTS.items()}
        del facts[fact][key]
        with pytest.raises((MissingFact, MismatchedFacts), match=key):
            triage_issue(**facts, provenance=PROVENANCE)


def test_a_census_row_missing_a_column_refuses_too():
    """The census is read through the same door, one row at a time."""
    for key in CENSUS_FACTS:
        row = dict(PAYMENTS["census"][0])
        del row[key]
        with pytest.raises((MissingFact, MismatchedFacts), match=key):
            issue(PAYMENTS, census=[row])


def test_an_incident_with_no_source_is_refused_by_the_module_that_owns_that_message():
    """A NULL `source` is a real row of T1's feed -- a DQ gate on a job the declaration does
    not know -- and there is exactly one thing to say about it, already said in
    `evidence._spec_of_incident`. It arrives here through `blast_radius`, which imports that
    refusal rather than re-spelling it."""
    with pytest.raises(UnknownTable, match="no bronze table can be resolved"):
        issue(PAYMENTS, incident={"source": None}, severity={"source": None})


def test_a_job_name_still_wearing_the_bundle_prefix_is_refused():
    """THE ONE FIELD IN THIS RECORD THAT CAN CARRY A PERSONAL IDENTIFIER INTO A PUBLIC
    REPOSITORY. `databricks.yml`'s only target is `mode: development`, which names every
    deployed job `[dev <operator>] <name>` -- an operator's Windows username, which
    CLAUDE.md forbids committing and the run-evidence documents redact elsewhere.

    `incidents.py` strips it in SQL and every payload built from that feed is already clean;
    what this refuses is the caller that read `job_name` from the raw timeline instead,
    which is one join away in a workspace where both are available. IT IS A COARSER CHECK
    THAN THE STRIP AND NOT A SECOND SPELLING OF IT: the feed removes a leading bracketed
    token, this refuses a bracket anywhere in the name, so the two fail differently.

    Both doors are checked, because a facts FILE reaches the publisher without passing
    through the assembler at all."""
    with pytest.raises(MismatchedFacts, match="bracketed prefix"):
        issue(PAYMENTS, incident={"job_name": "[dev an_operator] opl-bronze-payments"})

    carried = {**as_mapping(issue(PAYMENTS)), "job_name": "[dev an_operator] opl-bronze-payments"}
    with pytest.raises(MismatchedFacts, match="bracketed prefix"):
        from_mapping(carried)


# ----------------------------------------------------------------------------------
# Provenance: the one thing a reader cannot recover from the body.
# ----------------------------------------------------------------------------------


def test_a_payload_that_cannot_say_what_produced_it_is_refused():
    """Every other line in a body can be re-derived by running the statements again; this
    one says WHICH running of them is being read. A blank is refused rather than rendered,
    because a body whose numbers trace to nothing is the artefact this phase refuses."""
    for blank in ("", "   "):
        with pytest.raises(ValueError, match="cannot say what produced it"):
            Provenance(produced_by=blank)


def test_the_record_carries_the_provenance_it_was_given_and_invents_none():
    """The default is empty, not plausible: a fixture or a caller that recorded no statement
    id must not end up with a body naming one."""
    bare = Provenance(produced_by="a run with nothing else recorded")
    assert issue(PAYMENTS, provenance=bare).provenance == bare
    assert bare.statements == () and bare.telemetry_view is None


@pytest.mark.parametrize(
    "produced_by",
    (
        r"uv run python C:\Users\an_operator\repo\scripts\open_triage_issue.py",
        "uv run python /home/an_operator/repo/scripts/open_triage_issue.py",
        "uv run python /Users/an_operator/repo/scripts/open_triage_issue.py",
        "databricks bundle run triage -t free [dev an_operator]",
        r"C:/Users/an_operator/repo",
        "uv run python ~an_operator/repo/scripts/open_triage_issue.py",
        "uv run python C:Users/an_operator/repo/scripts/open_triage_issue.py",
    ),
    ids=["windows", "home", "macos", "bundle-prefix", "forward-drive",
         "home-shorthand", "drive-relative"],
)
def test_a_produced_by_shaped_like_a_path_or_a_bundle_prefix_is_refused(produced_by):
    """THIS FIELD RENDERS VERBATIM INTO A PUBLIC ISSUE, in the section a reader is told to
    trust most, and it was required, unvalidated and documented as "the command a human
    typed" -- which on this box is a command carrying this operator's Windows username.
    CLAUDE.md forbids committing that string and the run-evidence documents redact it.

    THE PARAMETRISATION IS OVER SHAPES AND NOT OVER ONE SPELLING, so passing cannot depend
    on a single separator or a single platform's home directory. Every operator name here is
    INVENTED, which is the same rule `test_issue.py`'s fixture prefix follows: committing a
    real one so that a check could find it is the thing being prevented.

    THE LAST TWO SHAPES WERE MEASURED AS ACCEPTED and are the reason this list grew. `~an_
    operator/...` is the home directory in the shell's own shorthand, which the three
    spelled-out prefixes do not cover; `C:Users/...` is DRIVE-RELATIVE and has no separator
    after the colon, which the drive pattern required. Both carry an operator name and both
    are in the class the refusal message claims.

    WHAT IS NOT REFUSED, and the controls below are the proof rather than the claim -- FOUR
    controls for four shapes, because one job-run URL was standing in for a sentence that
    named four. A POSIX relative path is not refused either and is not claimed to be; a
    WINDOWS relative path IS, by the backslash shape, which is why the two are not one word
    in this docstring any more. No check in `issue.py` can see the four below, which is why
    `report.py` labels this field the caller's word instead of implying it was checked."""
    with pytest.raises(MismatchedFacts, match="filesystem path"):
        Provenance(produced_by=produced_by)

    for allowed in (
        "https://adb-1234567890.11.azuredatabricks.net/jobs/900/runs/592660596679630",
        "run by an_operator",
        "an_operator@example.invalid",
        "opl-runner-01",
    ):
        assert Provenance(produced_by=allowed).produced_by == allowed

    with pytest.raises(MismatchedFacts, match="filesystem path"):
        Provenance(produced_by=r"uv run python .\scripts\open_triage_issue.py")


@pytest.mark.parametrize(
    "provenance",
    (
        {"telemetry_view": _OPERATOR_PATH},
        {"statements": [["census", _OPERATOR_PATH]]},
    ),
    ids=["telemetry-view", "statement-id"],
)
def test_the_other_two_provenance_strings_are_refused_the_path_produced_by_is(provenance):
    """`produced_by` IS NOT THE ONLY FREE TEXT THIS RECORD PUTS IN A PUBLIC BODY.

    `telemetry_view` and every statement id are caller-supplied, are printed verbatim by
    `report._read_from` and `report._measured`, and were checked by nothing -- so the same
    Windows path the sibling above refuses reached the rendered body through the door the
    PUBLISHER uses. Driven through `from_mapping` for that reason: `triage_dq_incident.py`
    sets the view from `OplConfig` and records no statement ids, so the in-process caller
    cannot produce this, and the file can. `from_mapping`'s own docstring is where the rule
    is written: a file is not a trusted caller.

    ONE SHAPE LIST BEHIND ALL THREE FIELDS, which is why the shapes are parametrised on the
    sibling and not here: a second list for these two would be the second spelling this
    package refuses, and a test that re-drove seven shapes against it would make one look
    correct. The control is the second half -- an unmodified payload, and a qualified
    relation name, both of which must read back."""
    carried = as_mapping(issue(PAYMENTS, provenance=PROVENANCE))
    crafted = {**carried, "provenance": {**carried["provenance"], **provenance}}

    with pytest.raises(MismatchedFacts, match="filesystem path"):
        from_mapping(crafted)
    with pytest.raises(MismatchedFacts, match="filesystem path"):
        payloads_from_json(json.dumps([crafted]))

    assert from_mapping(carried).batch_id == carried["batch_id"]
    assert Provenance(
        produced_by="a run", telemetry_view="opl.dataops.dataops_task_telemetry"
    ).telemetry_view == "opl.dataops.dataops_task_telemetry"


def test_the_bundle_declares_no_target_that_prefixes_a_job_name_without_brackets():
    """WHAT MAKES THE BRACKET CHECK TOTAL IS THIS FILE AND NOT THAT FUNCTION.
    `_assert_no_operator_identifier_reaches_the_payload` refuses a bracket, because
    `mode: development` names jobs `[dev <operator>] <name>`; a target declaring
    `presets.name_prefix: dev_<operator>_` produces `dev_jorge_opl-bronze-payments`, which
    has no bracket and passes every check in the wheel.

    So the bundle is read and required to declare no other prefixing. A target that adds one
    fails HERE, in the commit that adds it, which is the only place the answer is knowable
    -- the wheel cannot read `databricks.yml` and must not.

    WHAT THIS DOES NOT COVER: a prefix applied outside the bundle -- by the CLI, by a
    workspace policy, or by a job created through the UI. Nothing in this repository sees
    those, and the taint arm that would is `test_issue.py`'s, which only sees what its own
    fixture plants."""
    bundle = yaml.safe_load(
        (_REPO / "databricks" / "databricks.yml").read_text(encoding="utf-8")
    )
    targets = bundle["targets"]

    assert targets, "no targets to check, so this test asserts nothing"
    for name, target in targets.items():
        assert target.get("mode") == "development", f"{name} is not the mode the check reads"
        assert "name_prefix" not in target, f"{name} prefixes job names outside the bracket"
        assert "name_prefix" not in (target.get("presets") or {}), (
            f"{name} declares a presets.name_prefix, which is a prefix with no bracket in it"
        )


@pytest.mark.parametrize(
    ("statements", "complaint"),
    (
        ((("root_cause", "id-1"),), "unknown facts"),
        ((("severity", "id-1"), ("severity", "id-2")), "repeated facts"),
        ((("severity", "   "),), "blank ids"),
    ),
    ids=["unknown", "repeated", "blank"],
)
def test_a_statement_line_that_is_not_one_fact_and_one_id_is_refused(statements, complaint):
    """A STATEMENT ID CANNOT BE DERIVED -- it exists only after execution -- SO IT IS
    CONSTRAINED INSTEAD. The key must be one of `FACTS`, once, with a non-blank id, which is
    the same refusal `from_mapping` makes for a stray payload key.

    That constraint is what lets `report.py` key its four lines on `FACTS` and print the word
    for a fact whose id was never recorded. Without it the body printed the caller's list and
    said nothing about the facts missing from it -- and both provenances this repository
    ships record ONE id for FOUR facts.

    WHAT IT DOES NOT CHECK: that the id names a statement that ran, that it ran against this
    workspace, or that it produced the numbers in the body. Nothing in this process can."""
    with pytest.raises(MismatchedFacts, match=complaint):
        Provenance(produced_by="a run", statements=statements)

    assert Provenance(produced_by="a run", statements=tuple(
        (fact, f"id-{fact}") for fact in FACTS
    )).statements[0][0] == FACTS[0]


# ----------------------------------------------------------------------------------
# The file: what a publisher on another machine reads back.
# ----------------------------------------------------------------------------------


def test_a_record_survives_json_and_renders_the_same_issue():
    """The publisher is a separate process reading a file another process wrote, so the
    round trip is the interface. THE BODY IS COMPARED AND NOT ONLY THE RECORD: two records
    can compare equal on fields a renderer never reads."""
    for facts in CONSTRUCTED:
        original = issue(facts)
        (restored,) = payloads_from_json(_json(original))

        assert restored == original
        assert render_title(restored) == render_title(original)
        assert render_body(restored) == render_body(original)


def test_a_file_holding_one_mapping_and_a_file_holding_a_list_both_read():
    """Both shapes are what a producer writes -- a run that triaged one incident and a run
    that triaged the feed -- and reading a list is not the same permission as PUBLISHING
    one, which is the publisher's refusal and not this one."""
    single = payloads_from_json(json.dumps(as_mapping(issue(PAYMENTS))))
    many = payloads_from_json(_json(issue(PAYMENTS), issue(EMPRESAS), issue(LOOKUP)))

    assert len(single) == 1 and len(many) == 3
    assert [record.batch_id for record in many] == [issue(facts).batch_id
                                                    for facts in (PAYMENTS, EMPRESAS, LOOKUP)]


def test_a_payload_carrying_a_key_this_wheel_has_no_field_for_is_refused():
    """A file is not a trusted caller: it was written by another process, possibly by
    another wheel, and what it holds can be put in front of a stranger. An unknown key is a
    payload written by code this wheel is not."""
    carried = {**as_mapping(issue(PAYMENTS)), "root_cause": "invented by something else"}
    with pytest.raises(MissingFact, match="root_cause"):
        from_mapping(carried)


def test_a_breakdown_that_does_not_sum_to_the_headline_is_refused_at_the_file_door_too():
    """THE DOOR THE PUBLISHER ACTUALLY USES, and it was the one that did not check.

    `from_mapping` rebuilt `reject_groups` directly and never called the assembler's sum
    check, while its own docstring, `issue.py`'s header and `__init__.py` all said every
    refusal still applied. Reproduced through `payloads_from_json` -- the publisher's only
    entry point -- a payload whose breakdown summed to 1 against a headline of 2,000 was
    ACCEPTED and rendered both numbers, three lines apart, in an issue about to be public.

    BOTH DOORS ARE ASSERTED IN ONE TEST AND THAT IS DELIBERATE: the claim being held is that
    they agree, and two tests could pass with one door open. The control is the first line --
    the unmodified payload must read back, or the raises below are about the round trip."""
    carried = as_mapping(issue(PAYMENTS))
    assert from_mapping(carried).rejected_rows == 2000

    broken = {**carried, "reject_groups": [{"reason": "rescued_data_present", "rows": 1}]}
    with pytest.raises(MismatchedFacts, match="sum to 1 rejected rows.*computed from 2000"):
        from_mapping(broken)
    with pytest.raises(MismatchedFacts, match="sum to 1 rejected rows.*computed from 2000"):
        payloads_from_json(json.dumps([broken]))


@pytest.mark.parametrize(
    "field", ("severity", "recommended_action", "verdict"),
)
def test_a_payload_whose_grade_is_not_a_word_this_package_declares_is_refused(field):
    """THE THREE FIELDS `report.py` PRINTS INSIDE BACKTICKS IT WRITES ITSELF.

    Their whole defence was the sentence "words this package chose", which is true of
    `severity_sql` -- a CASE ladder over declared literals cannot emit anything else -- and
    was false HERE, at the door the publisher actually reads. A backtick in any of them
    closes the span `report.py` opened, and what follows is live markdown in a public issue:
    `@handle` notifies a real person, `#123` cross-links a real one.

    THE VOCABULARIES ARE READ FROM THE MODULES THAT DECLARE THEM and not listed in
    `issue.py`, so a fifth verdict or a fifth grade reaches this check in the commit that
    declares it. The control is the first line: the unmodified payload must read back."""
    carried = as_mapping(issue(PAYMENTS))
    assert from_mapping(carried).batch_id == carried["batch_id"]

    crafted = {**carried, field: "a_word` @torvalds #1 `x"}
    with pytest.raises(MismatchedFacts, match="not words this package declares"):
        from_mapping(crafted)
    with pytest.raises(MismatchedFacts, match="not words this package declares"):
        payloads_from_json(json.dumps([crafted]))


def test_a_payload_whose_rank_disagrees_with_the_ladder_that_ordered_it_is_refused():
    """The rank is a FUNCTION of the severity, so it is re-derived rather than trusted --
    `_radius_of`'s pattern, applied to the other field a body prints without a formatter.

    That closes the field twice over: a payload cannot rank the workspace's worst grade
    fourth, and it cannot put a crafted string where the body prints `rank {n} of 4` with no
    `_code` and no `_count` between the value and the markdown."""
    carried = as_mapping(issue(PAYMENTS))
    assert carried["severity"] == SEVERITIES[0] and carried["severity_rank"] == 1

    with pytest.raises(MismatchedFacts, match="ordered worst-first"):
        from_mapping({**carried, "severity_rank": len(SEVERITIES)})


@pytest.mark.parametrize("field", ("attempts", "severity_rank", "executions_requested"))
def test_a_payload_printing_a_string_where_the_body_prints_a_bare_number_is_refused(field):
    """THE THREE NUMBERS NO FORMATTER STANDS IN FRONT OF. Every other count in a body goes
    through `report._count`, which raises out of `f"{value:,}"` on a string; these three are
    interpolated bare, and two of them inside `**`, so `"2** @torvalds #1 **"` for `attempts`
    is published as written.

    A `bool` IS REFUSED WITH THEM. `True` is an `int` in Python and would render as the word
    `True` where a reader is being shown how many times the gate task ran."""
    carried = as_mapping(issue(PAYMENTS))
    for crafted in ("2** @torvalds #1 **", True):
        with pytest.raises(MismatchedFacts, match="where this record holds whole numbers"):
            from_mapping({**carried, field: crafted})


def test_a_payload_carrying_a_hold_note_this_repository_never_declared_is_refused():
    """THE REASONING THIS REPLACES WAS UNSOUND AND WAS IN THREE HEADERS. `hold_note` was the
    one value left unfenced on the ground that it is "declared in this repository rather than
    read from a row". The chain is `HOLDS[batch].why` -> `sql_string_literal` into the graded
    CASE -> A RESULT ROW (it is in `SEVERITY_FACTS`) -> the payload JSON -> `from_mapping`,
    which performed no `HOLDS` lookup -> a blockquote in a public issue.

    So the lookup the sentence assumed is MADE, which is what makes the sentence true rather
    than removed: the wheel derives the note again and refuses a disagreement, exactly as it
    does for the blast radius.

    THE ABSENCE DIRECTION IS NOT REFUSED and the last line proves it: a held batch carrying
    no note is the state `test_issue_report.py` builds to show the recommendation flipping
    when the declaration is dropped, and a payload written before a hold existed is not a
    forged one. What is refused is prose this repository never wrote."""
    payments = as_mapping(issue(PAYMENTS))
    assert payments["hold_note"] == HOLDS[payments["batch_id"]].why

    with pytest.raises(MismatchedFacts, match="differs from"):
        from_mapping({**payments, "hold_note": "do not promote\n\n@torvalds owns this"})

    unheld = as_mapping(issue(EMPRESAS))
    assert unheld["batch_id"] not in HOLDS and unheld["hold_note"] is None
    with pytest.raises(MismatchedFacts, match="never declared"):
        from_mapping({**unheld, "hold_note": "a decision nobody took"})

    assert from_mapping({**payments, "hold_note": None}).hold_note is None


def test_a_provenance_a_file_carries_is_read_through_the_same_two_refusals():
    """A NESTED MAPPING IS STILL A FILE. The top-level keys were checked and this one field,
    whose value is itself a record, was unpacked by subscript -- so a stray key inside it was
    ignored and a missing one arrived as a `KeyError` with nothing naming the payload.

    The third arm is the one that matters most: `Provenance.__post_init__` runs on the way
    back in, so a file carrying a `produced_by` with an operator's path in it is refused at
    the publisher's door and not only at the assembler's."""
    carried = as_mapping(issue(PAYMENTS))

    with pytest.raises(MissingFact, match="read_from"):
        from_mapping({**carried, "provenance": {**carried["provenance"], "read_from": []}})

    without = {key: value for key, value in carried["provenance"].items()
               if key != "telemetry_view"}
    with pytest.raises(MissingFact, match="telemetry_view"):
        from_mapping({**carried, "provenance": without})

    tainted = {**carried["provenance"], "produced_by": r"uv run C:\Users\an_operator\x.py"}
    with pytest.raises(MismatchedFacts, match="filesystem path"):
        from_mapping({**carried, "provenance": tainted})


def test_a_payload_missing_a_field_is_refused_rather_than_defaulted():
    """The same door as a renamed SQL alias, from the other side."""
    carried = as_mapping(issue(PAYMENTS))
    del carried["hold_note"]
    with pytest.raises(MissingFact, match="hold_note"):
        from_mapping(carried)


def test_a_payload_whose_blast_radius_this_wheel_no_longer_derives_is_refused():
    """What this catches is a facts file produced by one wheel and published by another,
    in the one direction that matters: the downstream manifest gained or lost an edge
    between the run and the post.

    IT IS NOT A VERSION CHECK AND DOES NOT PRETEND TO BE ONE -- two wheels with identical
    manifests and different everything else compare equal here. The control is the first
    line: the unmodified payload must read back, or the raise below is about the round trip
    rather than about the radius."""
    carried = as_mapping(issue(PAYMENTS))
    assert from_mapping(carried).radius == blast_radius("payments")

    carried["radius"] = {**carried["radius"], "gold": ["dim_date"]}
    with pytest.raises(MismatchedFacts, match="downstream manifest changed"):
        from_mapping(carried)


def test_assembling_and_rendering_are_deterministic():
    """No clock, no randomness, no dictionary order reaching the output. A body that carried
    a render timestamp would make two issues about one incident differ in a field no fact
    decided, which is the property the whole design rests on."""
    first, second = issue(PAYMENTS), issue(PAYMENTS)

    assert first == second
    assert render_body(first) == render_body(second)
    assert as_mapping(first) == as_mapping(second)
