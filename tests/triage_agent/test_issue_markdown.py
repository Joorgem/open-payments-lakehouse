# tests/triage_agent/test_issue_markdown.py
"""WHAT A CRAFTED VALUE CAN DO TO A STRANGER'S BROWSER. One test per arm, one arm per test.

WHY THIS IS ITS OWN FILE. `test_issue_report.py` answers "does the body say something an
incident-specific fact decided"; this one answers "can a value in that body stop being a
value". Different subjects, and the second one is the only one whose failures are invisible
to every other test in this package -- a body with a broken code span renders, reads
correctly in a terminal, and is wrong only in a browser looking at a PUBLIC repository.

THE SHAPE OF EVERY TEST HERE IS: DELETE ONE ARM, WATCH ONE TEST GO RED. `report._code` had
three arms and exactly one of them -- the fence -- was pinned, measured by deleting each of
the other two and running the whole triage suite: 69 passed, twice. Two of those three were
LIVE BREAKOUTS on a reachable input, and the reachable input is `reject_reason`, a
quarantine ROW VALUE, which is the input `_code`'s own docstring says it was written for:

  * PAD REMOVED  -- ```@torvalds #1``   the opener is a three-run with no three-run closer,
                                        so no code span opens and `@torvalds` is live.
  * FOLD REMOVED -- `reason\\n\\n@torvalds  a blank line ends the paragraph the opening fence
                                        is in, so the span never closes.

NOTHING HERE STARTS SPARK, for `test_issue_report.py`'s reason: a body is a pure function of
a `TriageIssue`, and a JVM would buy nothing an assertion about a string needs.

WHAT THIS FILE DOES NOT COVER. Whether `@handle` and `#123` LINKIFY inside a GitHub issue
TITLE is unproven in both directions -- no evidence was found either way, and none is
assumed. What is proven and is fixed is the FORMATTING break: GitHub does render code spans
in titles, so a title is not the inert string it was treated as, and `batch_id` is fenced
there for the reason `_headline` fences it. There is no shell exposure to cover: the title
is one argv element handed to `gh`, and `test_issue_publisher.py` reads the publisher's
source to require that `shell=True` never appears.
"""
from __future__ import annotations

from dataclasses import replace

import pytest

from opl.triage_agent import report as report_module
from opl.triage_agent.report import _code, render_body, render_title
from opl.triage_agent.severity import SEVERITIES

from .issue_facts import PAYMENTS, issue

# The value a crafted `_dq_reject_reason` would carry: a handle that notifies a real person
# and an issue reference that cross-links a real issue, both INSIDE a value this body is
# required to print. Removing the characters would be a redaction of the gate's own word for
# what it rejected; what has to hold is that the span does not close inside them.
_LIVE = "@torvalds #1"


def _sections(body: str) -> dict[str, str]:
    """The body split on its own `##` headings. `test_issue_report.py`'s helper, re-spelled.

    NOT IMPORTED FROM THAT FILE, which would make one test file import another's privates
    and couple two suites that are split on purpose. It is four lines."""
    found: dict[str, str] = {}
    heading = "the headline"
    for line in body.split("\n"):
        if line.startswith("## "):
            heading = line.removeprefix("## ")
        found[heading] = f"{found.get(heading, '')}\n{line}"
    return found


def _reason_line(reason: str) -> str:
    """The census breakdown line a crafted reject reason renders to, from a whole body.

    THROUGH `render_body` AND NOT THROUGH `_code` ALONE, because what is being asserted is
    that the reachable input reaches the fence -- a `_code` unit test would stay green over
    a caller that stopped calling it."""
    body = render_body(issue(PAYMENTS, census=[
        {"batch_id": PAYMENTS["severity"]["batch_id"],
         "reject_reason": reason, "rejected_rows": 2000},
    ]))
    return next(line for line in body.split("\n") if "torvalds" in line)


# ----------------------------------------------------------------------------------
# `_code`, one arm at a time.
# ----------------------------------------------------------------------------------


def test_a_value_that_begins_with_a_backtick_is_padded_or_no_code_span_opens_at_all():
    """ARM 2 OF `_code`, AND IT IS A LIVE BREAKOUT RATHER THAN A COSMETIC.

    CommonMark opens a code span on a run of N backticks and closes it on the next run of
    EXACTLY N. A value beginning with a backtick, fenced without the pad, welds its own
    backtick onto the opener: ``` ``  + `x ``` is a three-run, the closer is still a
    two-run, no span exists, and everything after it -- `@torvalds`, `#1` -- is live markdown
    in a public issue. The pad is what CommonMark requires for this case and it is the reason
    the rule exists at all.

    BOTH ENDS ARE DRIVEN. The pad is applied to both sides whenever EITHER end carries a
    backtick, so a trailing-backtick value is the same defect mirrored and a one-sided
    padding would pass a leading-only test."""
    assert _code(f"`{_LIVE}") == f"`` `{_LIVE} ``"
    assert _code(f"{_LIVE}`") == f"`` {_LIVE}` ``"
    assert _code("`") == "`` ` ``"

    assert _reason_line(f"`{_LIVE}") == f"  - `` `{_LIVE} ``: 2,000"


def test_a_blank_line_in_a_row_value_cannot_end_the_paragraph_the_span_sits_in():
    """ARM 3 OF `_code`, AND IT IS THE OTHER LIVE BREAKOUT.

    A code span cannot cross a blank line: the blank line ends the paragraph the opening
    fence is in, the span never closes, and the fence renders as a literal backtick with live
    markdown after it. So every line ending in the value becomes a space. THAT IS A RENDERING
    CHOICE AND NOT A REDACTION -- the characters are still there, one of them is now a space.

    `\\r` IS ITS OWN REPLACEMENT AND IS DRIVEN SEPARATELY. A lone carriage return is a line
    ending on GitHub too, and `.replace("\\n", " ")` alone leaves it in place -- so the value
    a Windows producer wrote breaks a span the same way."""
    for ending in ("\n\n", "\r\r", "\r\n\r\n"):
        rendered = _code(f"reason{ending}{_LIVE} is live")
        assert "\n" not in rendered and "\r" not in rendered
        assert rendered.strip("`").strip().startswith("reason")
        assert _LIVE in rendered

    assert _code(f"reason\n\n{_LIVE}") == f"`reason  {_LIVE}`"
    assert _reason_line(f"reason\n\n{_LIVE}") == f"  - `reason  {_LIVE}`: 2,000"


def test_an_empty_value_renders_a_code_span_and_not_two_literal_backticks():
    """ARM 1 OF `_code`, GUARDED RATHER THAN RULED, because guarding it is two lines.

    This one is COSMETIC and the test says so: without the fallback, `_code("")` emits
    ` `` `, which CommonMark renders as two literal backticks rather than as anything live.
    Nothing breaks out. What is wrong is what a reader sees where a value should be -- two
    stray backticks in the middle of a sentence, in the section that is meant to be showing
    them the evidence.

    IT IS REACHABLE: `batch_id` and each of `result_states` are printed through `_code`
    without a falsy fallback of their own, so an empty string in either arrives here."""
    assert _code("") == "` `"
    assert _code("   ") == "`   `"

    body = render_body(issue(PAYMENTS, incident={"result_states": ["", "FAILED"]}))
    assert "terminal states ` `, `FAILED`" in body


# ----------------------------------------------------------------------------------
# The fields that reach the body, and the one that reaches the title.
# ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("field", "where"),
    (
        ("severity", "the headline"),
        ("recommended_action", "the headline"),
        ("verdict", "Whether the batch reconciles"),
    ),
    ids=["severity", "recommended-action", "verdict"],
)
def test_a_closed_vocabulary_word_is_fenced_like_every_other_value(field, where):
    """THE THREE FIELDS `report.py` USED TO INTERPOLATE INSIDE HAND-WRITTEN BACKTICKS.

    The stated ground was that they are closed vocabularies -- "words this package chose" --
    which is true of the SQL door, where a CASE ladder over declared literals is the only
    thing that can produce them, and was false of the JSON file the publisher actually reads.
    `from_mapping` now refuses a word that is not declared; this is the other half, because a
    check and a fence fail differently and the assembler makes no such check.

    THE CRAFTED VALUE REACHES THE BODY THROUGH `triage_issue`, which is the door that still
    does not constrain these -- so this test is asserting the fence and not the refusal."""
    crafted = f"a_word` {_LIVE} `x"
    drafted = issue(PAYMENTS, severity={field: crafted})
    section = _sections(render_body(drafted))[where]

    assert f"``{crafted}``" in section
    assert f"`{crafted}`" not in section.replace(f"``{crafted}``", "")


def test_the_rank_denominator_is_the_ladders_own_length_and_says_which_end_is_worse():
    """`len(SEVERITIES)` AND NOT A `4` TYPED INTO AN F-STRING. Hardcoding it to `4` left
    the whole triage suite green -- 69 passed -- which makes it exactly the constant that
    survives a fifth grade being added and then misreports every issue this package drafts.

    THE MUTATION IS ON `report.py`'s OWN BINDING, which is what the function reads: `from
    severity import SEVERITIES` copies the tuple into this module's globals, so patching
    `severity` after import moves nothing here (`test_issue_report.py` measured that for the
    census vocabularies and this is the same seam).

    AND THE DIRECTION IS ON THE PAGE. "rank 1 of 4" is a number a reader triages by with
    nothing anywhere in the body saying which end is worse."""
    body = render_body(issue(PAYMENTS))
    assert f"(rank 1 of {len(SEVERITIES)}, 1 is the worst)" in body
    assert len(SEVERITIES) == 4, "the fixture's expectation, not the rule"


def test_a_fifth_severity_moves_the_denominator_without_anyone_editing_the_body(monkeypatch):
    """The other half of the denominator: it MOVES. Asserting `of 4` against a ladder that
    is four long passes over a hardcoded `4`, so the ladder is lengthened and the body has to
    follow. Together the two tests pin the expression rather than its current value."""
    monkeypatch.setattr(report_module, "SEVERITIES", (*SEVERITIES, "a_fifth_grade"))
    assert "(rank 1 of 5, 1 is the worst)" in render_body(issue(PAYMENTS))


def test_the_title_fences_the_batch_id_because_github_renders_code_spans_in_titles():
    """A TITLE IS NOT INERT. GitHub renders backtick code spans in issue and PR titles, so a
    crafted value breaks out of one there exactly as it does in the body.

    `batch_id` IS FENCED FOR THE REASON `_headline` ALREADY GIVES, verbatim: it is a value
    the TIMELINE returned rather than a word this repository chose. The other three fields in
    the title are constrained instead of fenced -- `source` by `table_spec`, which raises
    `UnknownTable` at both doors, and the grade and the action by the vocabularies
    `from_mapping` checks -- and fencing all four would leave a title nobody can read.

    THE RECORD IS BUILT BY `replace` RATHER THAN THROUGH THE ASSEMBLER because the assembler
    requires all four facts to agree on the batch id, and what is under test is the title's
    formatting rather than the identity check that has its own file."""
    crafted = replace(issue(PAYMENTS), batch_id=f"592660596679630` {_LIVE}")
    title = render_title(crafted)

    assert f"batch ``592660596679630` {_LIVE}``:" in title
    assert crafted.batch_id in title, "fencing must not redact the value"
    assert title.startswith("[triage] payments batch ")


# ----------------------------------------------------------------------------------
# The one value still interpolated as prose.
# ----------------------------------------------------------------------------------


def test_a_hold_note_with_a_blank_line_cannot_leave_the_blockquote():
    """`f"> {note}"` QUOTES LINE ONE. This is `_remedy`'s defect, in the other section.

    A blank line ends a blockquote, so a note carrying one puts everything after it into the
    issue as ordinary markdown -- and the paragraph directly below, the one that vouches for
    the note as a DECLARED decision of this repository, then follows text the note's author
    wrote. A stranger reading the page cannot tell the two apart.

    THE NOTE'S CHAIN IS WHY THIS MATTERS AT THE RENDERER AND NOT ONLY AT THE DOOR:
    `HOLDS[batch].why` -> `sql_string_literal` -> the graded CASE -> A RESULT ROW -> the
    payload JSON -> here. `from_mapping` re-derives it from `HOLDS` now, which closes the
    file door; this closes the renderer, which any caller holding a record can reach."""
    note = f"see #1 and ping {_LIVE}\n\nNOT A QUOTE ANY MORE: **bold** [link](http://x)"
    body = render_body(issue(PAYMENTS, severity={"hold_note": note}))
    section = _sections(body)["The decision on record"]
    quoted, vouched = section.split("That note is a DECLARED hold")

    assert "NOT A QUOTE ANY MORE" in quoted, "the fixture's second line must be in the quote"
    for line in quoted.split("\n"):
        if line.strip() and not line.startswith("## "):
            assert line.startswith(">"), f"{line!r} left the blockquote"
    assert vouched.strip().startswith("(`opl.triage_agent.severity.HOLDS`)")


def test_a_blank_hold_note_is_not_a_missing_one_and_is_not_vouched_for_either():
    """VERBATIM THE DEFECT `_remedy` WAS FIXED FOR -- "an empty remedy is not a missing one"
    -- left standing in the section two headings above it.

    `severity.py` refuses a blank `why` AT IMPORT, so a blank note cannot come from `HOLDS`
    and the paragraph that says "that note is a DECLARED hold" would be a claim about
    `HOLDS` made from a fact about one payload. The absence arm is equally wrong in the other
    direction: "no hold is declared for this batch" is false when one is.

    THE THIRD ARM IS REACHABLE FROM THE ASSEMBLER, which is why it is a rendering arm and not
    only a refusal: `from_mapping` refuses a note `HOLDS` did not write, and `triage_issue`
    takes the graded row as it came."""
    body = render_body(issue(PAYMENTS, severity={"hold_note": "   "}))
    section = _sections(body)["The decision on record"]

    assert "its note is BLANK" in section
    assert "That note is a DECLARED hold" not in section
    assert "No hold is declared for this batch" not in section
    assert "None." not in section
