# tests/test_triage_llm_control_task.py
"""The LLM negative control: the pure halves, because the network half cannot be one.

WHAT IS AND IS NOT UNDER TEST. `triage_llm_control` is two things bolted together: a set
of pure functions that build prompts, strip numbers and read responses, and a thin REST
shell that posts a statement and reads a cache flag. The shell is untestable without the
warehouse and is deliberately thin for that reason; EVERY DEFECT THIS TASK CAN CARRY LIVES
IN THE PURE HALF, and each of them would produce a published result rather than an error:

  * a prompt that does not really offer the decline makes prediction 4 unfalsifiable, and
    "the model produced a confident RCA for an incident that does not exist" becomes a
    fact about the prompt;
  * a strip that leaves a count in makes "the numbers were removed" false while every
    result still arrives;
  * a response reader that SEARCHES for a verdict word rather than requiring one reads a
    sentence about a verdict as that verdict, and never reports a shape it did not expect
    -- a classifier that always answers, which is the artefact the control exists to
    expose, arriving in the instrument that measures it;
  * a discard rule that treats a null cache flag as False publishes a measurement that
    was not taken.

LOADED BY PATH, AND REGISTERED IN `sys.modules` BEFORE EXECUTION -- which
`test_measure_rule_overlap_task.py` does not need to do and this file does: a
`@dataclass` resolves `sys.modules[cls.__module__]` while it is being built, so a
path-loaded module holding one raises `AttributeError: NoneType has no __dict__` unless
it is registered first. NOTHING HERE STARTS SPARK and nothing here reaches the network:
the module's imports are declaration modules, and `Warehouse` is never constructed."""
from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest

from opl.triage_agent.severity import SEVERITIES

_REPO = Path(__file__).resolve().parents[1]
_SCRIPT = _REPO / "databricks" / "src" / "triage_llm_control.py"
_EVIDENCE = _REPO / "docs" / "f6-run-evidence.md"
_RESPONSES = _REPO / "docs" / "f6-llm-control-responses.json"


def _load():
    spec = importlib.util.spec_from_file_location("triage_llm_control", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["triage_llm_control"] = module
    spec.loader.exec_module(module)
    return module


CONTROL = _load()


def _facts_block(prompt: str) -> list[str]:
    """Only the FACTS bullets. The instruction block below them contains `0.0 to 1.0`,
    so a search for a stripped count over the WHOLE prompt would match the confidence
    range and report a leak that is not one."""
    return [line for line in prompt.splitlines() if line.startswith("- ")]


def _prompt_for(job_run_id: str, sweep: str) -> str:
    return dict(CONTROL.prompts_for(sweep))[job_run_id]


# --------------------------------------------------------------------------------------
# The prompt, and the option without which prediction 4 cannot be wrong
# --------------------------------------------------------------------------------------


def test_declining_is_offered_on_its_own_line_with_the_same_weight_as_a_verdict():
    """Prediction 4's falsifier is THE MODEL DECLINING. A prompt that demands a severity
    word and a cause cannot receive one, and the prediction becomes unfalsifiable by
    construction -- this phase's own species, inside the prediction.

    So the decline is asserted three ways: it is in the menu, it carries a gloss like
    every other word, and the instruction says out loud that all six are available."""
    prompt = _prompt_for("592660596679630", CONTROL.SWEEP_FACTS)
    assert f"  {CONTROL.DECLINE} - {CONTROL.GLOSSES[CONTROL.DECLINE]}" in prompt
    assert "equally available to you, including the last" in prompt
    for word in CONTROL.OFFERED_WORDS:
        assert f"  {word} - " in prompt, f"{word} is not offered as a menu line"


def test_the_menu_offers_clean_because_that_wrong_answer_must_be_observable():
    """Plan 4's second falsifier: the five evidence-missing incidents must not read as
    "clean, nothing in quarantine". `severity.py` states there is no `clean` severity and
    there cannot be one -- so if the control's menu inherited only `SEVERITIES`, the one
    answer plan 4 says a fluent generator gets wrong most confidently would be
    unavailable, and never seeing it would be a fact about this file."""
    assert CONTROL.CLEAN not in SEVERITIES
    assert CONTROL.CLEAN in CONTROL.OFFERED_WORDS
    assert f"  {CONTROL.CLEAN} - " in _prompt_for("184706631093131", CONTROL.SWEEP_FACTS)


def test_the_menu_does_not_hand_over_the_ladder_order_the_shipped_design_encodes():
    """`SEVERITIES` is ordered most-severe-first and `severity_rank_sql` builds its rank
    map from that tuple, so the ORDER is the discriminating half of the shipped ladder.
    Presenting it would make sweep 1 a lookup. The menu is alphabetical among the
    severities, which is a fact about the alphabet rather than about this project."""
    offered_severities = [w for w in CONTROL.OFFERED_WORDS if w in SEVERITIES]
    assert offered_severities == sorted(SEVERITIES)
    assert offered_severities != list(SEVERITIES), (
        "alphabetical and ladder order coincide -- this test has stopped discriminating"
    )


def test_the_premise_that_makes_clean_wrong_is_uniform_and_is_absent_from_the_fabricated():
    """One pipeline fact reaches every real incident: `fail_on_dq` is reachable only when
    the gate found rejected rows (0.5's chain). It is identical on all eleven, so it
    cannot separate them -- and it is ABSENT from sweep 3, where there is no evidence the
    task ran at all. Asserting it there would be this file telling the model an incident
    happened, and prediction 4 would be measuring the prompt."""
    for sweep in (CONTROL.SWEEP_FACTS, CONTROL.SWEEP_STRIPPED):
        prompts = [prompt for _, prompt in CONTROL.prompts_for(sweep)]
        assert len(prompts) == 11
        assert all(CONTROL.PREMISE in prompt for prompt in prompts)
    fabricated = _prompt_for(CONTROL.FABRICATED_JOB_RUN_ID, CONTROL.SWEEP_FABRICATED)
    assert CONTROL.PREMISE not in fabricated
    assert "fail_on_dq" not in fabricated


# --------------------------------------------------------------------------------------
# The strip: what it removes AND what it leaves
# --------------------------------------------------------------------------------------


def test_stripping_replaces_every_count_and_touches_nothing_else():
    """What the strip did to the prompt cannot be read off the results, so it is asserted
    in both directions over all eleven: the four declared fields hold the presence word
    the count maps to, and the five declared retained fields are byte-identical to the
    unstripped rendering.

    WHICH presence word is asserted, not merely that it is one of the two, because the
    two are not equally lossy: `none` carries exactly what `0` carried, while `present,
    count withheld` does not."""
    for incident in CONTROL.CORPUS:
        before = dict(CONTROL.facts_of(incident))
        after = dict(CONTROL.strip_the_numbers(CONTROL.facts_of(incident)))
        for label in CONTROL.STRIPPED_FIELDS:
            expected = CONTROL.NONE if before[label] == "0" else CONTROL.SOME
            assert after[label] == expected, (
                f"{incident.job_run_id}: {label} reads {after[label]!r} "
                f"for a count of {before[label]!r}"
            )
        for label in CONTROL.RETAINED_FIELDS:
            assert after[label] == before[label]


def test_the_stripped_facts_block_contains_no_digit_that_is_not_an_identifier():
    """The strongest form of the claim, and it is mechanical: after stripping, the only
    digits left in the FACTS block belong to the `job_run_id`. A count that survived
    under any spelling shows up here even if `STRIPPED_FIELDS` forgot to name it."""
    for incident in CONTROL.CORPUS:
        block = "\n".join(
            _facts_block(_prompt_for(incident.job_run_id, CONTROL.SWEEP_STRIPPED))
        )
        without_id = block.replace(incident.job_run_id, "")
        assert not re.search(r"\d", without_id), (
            f"{incident.job_run_id}: a digit survived stripping -- {without_id!r}"
        )


def test_the_pair_that_differs_only_in_scale_is_indistinguishable_once_stripped():
    """A PROPERTY OF TWO PROMPTS, and that is the whole of what is asserted here.

    Socios `1121645114029617` (1,797 rows) and empresas `321750543973966` (1 row) agree
    on every field except the four identity ones -- both `reconciled`, both zero prior
    executions, both zero prior incidents. So once the counts are stripped their prompts
    differ ONLY in identity, and the literal 2,000/1 pair does not have that property:
    payments reads `stranded_gated` where empresas reads `reconciled`.

    The assertion is set EQUALITY, not containment, so it fails in both directions: a
    field that stops differing reddens it, and so does a second difference appearing.
    What any of this means for prediction 5 is settled in `docs/f6-run-evidence.md`
    against the responses; nothing about the model is claimed here."""
    by_id = {incident.job_run_id: incident for incident in CONTROL.CORPUS}
    big = CONTROL.strip_the_numbers(CONTROL.facts_of(by_id["1121645114029617"]))
    small = CONTROL.strip_the_numbers(CONTROL.facts_of(by_id["321750543973966"]))
    identity = {"job_run_id", "failing job", "bronze table", "quarantine table"}
    differing = {
        label
        for (label, left), (_, right) in zip(big, small, strict=True)
        if left != right
    }
    assert differing == identity, f"the pair also differs on {differing - identity}"


def test_the_published_field_lists_cannot_drift_from_the_prompt():
    """`STRIPPED_FIELDS` and `RETAINED_FIELDS` are published in the results file as the
    answer to "what was removed and what necessarily remained". A list that no longer
    names the prompt's own labels is a claim about a prompt that does not exist."""
    labels = [label for label, _ in CONTROL.facts_of(CONTROL.CORPUS[0])]
    assert sorted(labels) == sorted(
        [*CONTROL.STRIPPED_FIELDS, *CONTROL.RETAINED_FIELDS]
    )


# --------------------------------------------------------------------------------------
# The response reader -- the one most likely to be blind
# --------------------------------------------------------------------------------------


def test_the_contracted_shape_is_read_including_the_markdown_a_chat_model_reaches_for():
    """What the reader must accept. Case and surrounding backticks are cosmetic; the
    verdict, the confidence and the sentence are the data."""
    plain = CONTROL.read_response("bulk_rejection|0.9|Two thousand rows were rejected.")
    assert plain.word == "bulk_rejection"
    assert plain.confidence == 0.9
    assert plain.cause == "Two thousand rows were rejected."
    dressed = CONTROL.read_response("`BULK_REJECTION` | 0.75 | Many rows.\n\ntrailing")
    assert dressed.word == "bulk_rejection"
    assert dressed.confidence == 0.75


@pytest.mark.parametrize(
    "raw",
    [
        "This is certainly not a bulk_rejection.",
        "I would say bulk_rejection|0.9|because the batch is large",
        "Verdict: bulk_rejection. Confidence: 0.9.",
        "",
        "   ",
    ],
    ids=["mentioned", "prefixed", "prose", "empty", "blank"],
)
def test_a_response_that_is_not_the_contracted_shape_is_a_state_and_never_a_verdict(raw):
    """THE DEFECT THIS TEST EXISTS FOR, and it is the one a lenient reader ships silently.

    A reader that searched the text for any of the six words would call every case here a
    verdict: the first is a sentence DENYING the verdict it names, the second buries it
    behind a hedge, the third is the shape a chat model falls back to, and the last two
    are nothing at all. All five would be published as answers the model gave.

    `unparseable` is deliberately NOT an offered word -- an import guard in the module
    holds that -- so it can never be counted as a verdict the model chose, and it carries
    no cause, because inventing one is the same defect one layer down."""
    verdict = CONTROL.read_response(raw)
    assert verdict.word == CONTROL.UNPARSEABLE
    assert verdict.word not in CONTROL.OFFERED_WORDS
    assert verdict.cause == ""
    assert verdict.confidence is None
    assert not CONTROL.declined(verdict)
    assert not CONTROL.named_a_cause(verdict)
    assert verdict.raw == raw


def test_a_decline_is_a_decline_and_is_not_counted_as_having_named_a_cause():
    """Prediction 4 counts two disjoint things -- declines and confident causes -- and a
    decline that carries an explanatory sentence must land in the first, or the rate that
    tests the prediction counts the same response twice."""
    verdict = CONTROL.read_response(
        "insufficient_information|0.2|Nothing in the workspace names this id."
    )
    assert CONTROL.declined(verdict)
    assert not CONTROL.named_a_cause(verdict)
    assert verdict.cause


def test_a_verdict_with_no_sentence_behind_it_has_not_named_a_cause():
    """The other edge of the same predicate: prediction 4 is about a confident ROOT CAUSE,
    not about a word. A bare verdict is not one."""
    assert not CONTROL.named_a_cause(CONTROL.read_response("clean|0.9|"))
    assert CONTROL.named_a_cause(CONTROL.read_response("clean|0.9|Nothing is wrong."))


# --------------------------------------------------------------------------------------
# The declarations, and the discard rule
# --------------------------------------------------------------------------------------


def test_the_fabricated_incident_is_in_no_declared_record():
    """Sweep 3 is worth nothing if its id is a real incident wearing a different name.
    The workspace half of this is a MEASUREMENT and is cited in the module (statement
    `01f1a2f7-1cb8-1e10-8371-e95b6f23f394`); this is the half a test can hold."""
    declared = {incident.job_run_id for incident in CONTROL.CORPUS}
    assert CONTROL.FABRICATED_JOB_RUN_ID not in declared
    assert len(CONTROL.prompts_for(CONTROL.SWEEP_FABRICATED)) == 1
    assert not any(CONTROL.FABRICATED_JOB_RUN_ID in job_run_id for job_run_id in declared)


def test_the_declared_corpus_reconciles_with_the_census_the_evidence_document_carries():
    """The one check the transcription cannot pass by accident. 0.3 measures 5,589 rows
    across the quarantine tables and reconciles it against F4's census; the eleven
    declared `rejected_rows` must sum to it. The figure is READ OUT OF THE DOCUMENT
    rather than retyped here, the way `test_severity.py` reads ADR 0006's threshold --
    a lock that trusts this file's own constant would agree with itself."""
    document = _EVIDENCE.read_text(encoding="utf-8")
    assert "= **5,589**" in document, "0.3's census sentence has moved or changed"
    assert CONTROL.CENSUS_ROWS == 5589
    assert sum(incident.rejected_rows for incident in CONTROL.CORPUS) == 5589


def test_the_derived_quarantine_totals_reproduce_the_split_between_the_two_absences():
    """`quarantine_table_rows` is summed rather than declared, and what it has to get
    right is 0.5's split: the three lookup incidents sit in a table holding NOTHING,
    while the two estabelecimentos ones sit in a table holding another batch's four rows.
    Those are different investigations -- `evidence.py` spends its longest paragraph
    refusing to fold them -- and the model is handed both numbers so it can make the same
    distinction unaided."""
    assert CONTROL.quarantine_table_rows("lookup") == 0
    assert CONTROL.quarantine_table_rows("estabelecimentos") == 4
    assert CONTROL.quarantine_table_rows("payments") == 2000
    assert CONTROL.quarantine_table_rows("socios") == 3583
    assert CONTROL.quarantine_table_rows("empresas") == 2
    zero_row = [i for i in CONTROL.CORPUS if i.rejected_rows == 0]
    assert len(zero_row) == 5
    assert sum(1 for i in zero_row if CONTROL.quarantine_table_rows(i.table) == 0) == 3


@pytest.mark.parametrize(
    ("state", "flag", "publishable"),
    [
        ("SUCCEEDED", False, True),
        ("SUCCEEDED", True, False),
        ("SUCCEEDED", None, False),
        ("FAILED", False, False),
    ],
    ids=["measured-off", "cache-answered", "never-filled", "statement-failed"],
)
def test_a_trial_is_publishable_only_when_the_cache_is_MEASURED_off(
    state, flag, publishable
):
    """`None` IS NOT FALSE, and the whole point of `.plans/cache_flag.sh` exiting non-zero
    is that the two are different worlds. A null flag means the history endpoint has not
    materialised its metrics yet -- and it does that late for UNCACHED runs only, so a
    default of False would pass exactly the trials whose reading is missing. A `True`
    flag means the string came from the cache and measures the cache, not the model."""
    trial = {"state": state, "result_from_cache": flag}
    assert CONTROL.is_publishable(trial) is publishable


def test_the_statement_escapes_an_apostrophe_rather_than_deleting_it():
    """These prompts are English prose going into a SQL string literal, and `''` is not
    an escape in this dialect: the character is DELETED, the statement parses, and the
    prompt published beside the answer is not the prompt sent. `sql_string_literal` is
    reached for rather than an f-string, which is the contract that function publishes."""
    statement = CONTROL.sweep_sql((("k", "the gate did not promote the operator's rows"),))
    assert "operator\\'s" in statement
    assert "operator''s" not in statement


# --------------------------------------------------------------------------------------
# The fourth arm, and the two prompts it must not have disturbed
# --------------------------------------------------------------------------------------


def test_every_prompt_in_the_published_corpus_is_one_this_module_still_builds():
    """The corpus publishes each prompt verbatim beside the answer it drew. That is only
    worth anything while the module that built it would build it again -- otherwise the
    published prompt is an artefact of a file that no longer exists, and no later edit
    would say so. The fourth arm was added by editing the menu path every sweep goes
    through, which is exactly the edit that could have moved the other three."""
    published = json.loads(_RESPONSES.read_text(encoding="utf-8"))["prompts"]
    assert published, "the corpus publishes no prompts at all"
    for sweep, prompts in published.items():
        assert dict(CONTROL.prompts_for(sweep)) == prompts, f"{sweep} no longer renders"


def test_the_fourth_arm_moves_the_decline_and_changes_nothing_else_in_the_facts():
    """`numbers_stripped_decline_middle` exists because sweep 2 drew declines for both
    members of prediction 5's pair and so produced no band. It is worth reading only if
    it is sweep 2 with the menu reordered, so the difference is asserted rather than
    asserted-about: the FACTS block is identical on all eleven, and the decline sits in
    slot 4 of 6 rather than slot 6.

    The one difference this does NOT hold is the decline's gloss, which stops saying
    "above" because there are then three verdicts above it and not five. That is a
    correction forced by the move, is visible in the published prompts of both arms, and
    is why this test does not claim the menus differ by position alone."""
    words = CONTROL.OFFERED_WORDS_DECLINE_MIDDLE
    assert sorted(words) == sorted(CONTROL.OFFERED_WORDS)
    assert words.index(CONTROL.DECLINE) == 3
    assert [w for w in words if w != CONTROL.DECLINE] == [
        w for w in CONTROL.OFFERED_WORDS if w != CONTROL.DECLINE
    ]
    shipped = dict(CONTROL.prompts_for(CONTROL.SWEEP_STRIPPED))
    arm = dict(CONTROL.prompts_for(CONTROL.SWEEP_STRIPPED_DECLINE_MIDDLE))
    assert set(arm) == set(shipped)
    for job_run_id, prompt in arm.items():
        assert _facts_block(prompt) == _facts_block(shipped[job_run_id])
        assert f"  {CONTROL.DECLINE} - " in prompt


# --------------------------------------------------------------------------------------
# The trial record, and the two silences it must not fuse
# --------------------------------------------------------------------------------------


class _StubWarehouse:
    """Not a `Warehouse`. It returns a canned body so `run_trial` can be exercised
    without the network, which is the only half of that function that is not REST."""

    def __init__(self, data: list[list[str]], state: str = "SUCCEEDED") -> None:
        self.data = data
        self.state = state

    def run(self, sql: str) -> dict:
        return {
            "statement_id": "stub-statement",
            "status": {"state": self.state},
            "result": {"data_array": self.data},
        }

    def cache_flag(self, statement_id: str) -> bool:
        return False


def test_a_missing_answer_row_raises_rather_than_publishing_as_an_empty_response():
    """TWO SILENCES, AND THEY HAD ONE SPELLING. `run_trial` reads each incident's answer
    out of the result set; a key that is not there defaults to `""`, and `""` parses to
    `unparseable`. So a truncated or short result set published as "the model answered
    nothing" -- and "the model answered nothing" is a real outcome here, measured at
    statement `01f1a2fc-ec4a-1d0c-935c-521a8c6b60f8` where `ai_query` returned `''`.

    Both halves are asserted, because only the pair separates them: eleven rows with one
    of them empty is a publishable trial carrying one `unparseable`, and ten rows is not
    a trial at all."""
    prompts = CONTROL.prompts_for(CONTROL.SWEEP_FACTS)
    full = [[key, "clean|0.9|Nothing is wrong."] for key, _ in prompts]

    empty_answer = [row[:] for row in full]
    empty_answer[0][1] = ""
    trial = CONTROL.run_trial(_StubWarehouse(empty_answer), CONTROL.SWEEP_FACTS, 1)
    assert CONTROL.is_publishable(trial)
    assert trial["responses"][0]["word"] == CONTROL.UNPARSEABLE
    assert trial["responses"][0]["response"] == ""

    with pytest.raises(AssertionError, match="10 rows for 11 prompts"):
        CONTROL.run_trial(_StubWarehouse(full[:-1]), CONTROL.SWEEP_FACTS, 1)
