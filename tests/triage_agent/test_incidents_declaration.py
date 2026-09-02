"""The declaration behind the incident feed, held equal to the bundle. No Spark in here.

SPLIT OUT OF `test_incidents.py`, AT A SEAM THAT WAS MEASURED RATHER THAN CHOSEN. Nothing
in this file touches that one's fixture machinery -- no `probe`, no view, no `spark`, no
`SystemTables` -- so every test here runs in under a second and every one of them used to
wait 25-33 s for a JVM session it never asked for. The parent had also reached 845 lines
against this repository's 800-line cap. The move was made in a commit free of behaviour:
no assertion edited, nothing renamed, and the two files hold the same tests they did.

AND NOTHING ENFORCES THAT FIRST SENTENCE, WHICH IS SAID HERE RATHER THAN LEFT TO BE
ASSUMED. Adding a Spark test to this file would silently cost it the property the split
bought, and no test would go red -- `tests/test_size_caps.py` covers the line count and
nothing covers the JVM. The guard was considered and deliberately not built: every cheap
spelling of it is this repository's hunted species one level down (a signature scan passes
while a module-scope session, an autouse fixture or a transitive import still starts a
JVM), and the honest spelling is a one-file special case inside a repo-wide sweep. It is
recorded in `docs/f6-run-evidence.md` section 3 as unguarded, which is what this project
does with a property it has chosen not to protect.

THREE THINGS ARE PROVEN HERE BY MUTATING SOMETHING AND REQUIRING A RAISE, which is
`tests/dataops/test_cadence.py`'s pattern and the half this file's first version dropped.
Each of the two import-time guards is fired on the declaration it exists to refuse, and
the import-time CALL is fired by re-executing the module body against a registry it cannot
satisfy. Calling the guards over VALID data -- which is what this file used to do -- only
restates their bodies over data that already passed at import: deleting both calls from
`incidents.py` left the file GREEN, which is how that was found. THE COLOUR IS NAMED AND
NOT THE TOTAL that stood here: a total goes stale on the next commit that adds a test, and
this file has since been split off and gained tests, so the number no longer reproduces.
That is `test_evidence_contract.py`'s ruling, applied by grep rather than where pointed.
"""
from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

import pytest
import yaml
from job_yaml import RESOURCES, resource_files

from opl.bronze.reconcile import batch_grain_sql
from opl.bronze.registry import REGISTRY
from opl.config import DEFAULT
from opl.dataops.freshness import freshness_sql
from opl.dataops.telemetry import TASK_TELEMETRY_VIEW
from opl.triage_agent import incidents as incidents_module
from opl.triage_agent.incidents import (
    DQ_GATE_TASK_KEY,
    TABLE_OF_JOB,
    _assert_no_two_jobs_claim_the_same_table,
    _assert_the_declared_jobs_cover_exactly_the_registered_tables,
    incident_feed_sql,
    table_of_job_sql,
)


def test_the_feed_reads_the_telemetry_view_this_project_deploys():
    """The default source is the deployed view, so the test seam cannot leak into it.

    `view` exists for the same reason `SystemTables` does -- otherwise the query is
    asserted by nobody until a workspace run -- and the same lock applies: what deploys is
    pinned here, spelled from `config` and the view's own constant rather than retyped.

    THE PARAMETER IS `view` AND NOT `source`, which is the rename this correction made and
    the reason this sentence is spelled carefully: in this module `source` is the EMITTED
    registry-key column, and a docstring that also used it for the test seam would put the
    one-signature ambiguity back that the rename was performed to remove."""
    assert DEFAULT.table(TASK_TELEMETRY_VIEW) in incident_feed_sql()
    assert "workspace.default.dataops_task_telemetry" in incident_feed_sql()


def _paren_depth(sql: str) -> list[int]:
    """Parenthesis depth at every character of `sql`, held at -1 inside a string literal.

    Literals are MARKED rather than counted, so a comma or a keyword sitting inside
    `concat('...,batch_id=', ...)` cannot be read as query structure.

    THE BACKSLASH ARM IS NOT DEFENSIVE, IT IS REACHABLE, and it was reached. A first
    version left the literal on the first `'` it met, so a backslash-escaped apostrophe
    ended the literal early and the rest of it was read as query structure -- surfacing as
    a bare `IndexError` from this file, naming neither the note that was edited nor the
    column this test is about. `opl.dataops.freshness.sql_string_literal` exists precisely so an
    operator's prose in `cadence.why` MAY carry an apostrophe, and its own docstring calls
    that "a matter of time"; `CLAUDE.md` records that `''` is not an escape on Databricks
    and that the backslash is. So the shape this parser must survive is the one the
    codebase invites. Fired by `test_a_backslash_escaped_apostrophe_does_not_end_a_literal`."""
    depths: list[int] = []
    depth, quoted, escaped = 0, False, False
    for character in sql:
        if quoted:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            else:
                quoted = character != "'"
            depths.append(-1)
            continue
        if character == "'":
            quoted = True
            depths.append(-1)
            continue
        depth += (character == "(") - (character == ")")
        depths.append(depth)
    return depths


def _projection_of(sql: str) -> tuple[list[str], str]:
    """The items the LAST top-level `SELECT` projects, and the relation it reads `FROM`.

    Top-level is the whole trick: every CTE's own `SELECT` sits at depth one or more, so
    what this finds is the query's outermost projection -- the columns it PUBLISHES."""
    depths = _paren_depth(sql)
    top = [i for i in range(len(sql)) if depths[i] == 0 and sql.startswith("SELECT ", i)]
    begin = top[-1] + len("SELECT ")
    stop = next(
        i for i in range(begin, len(sql)) if depths[i] == 0 and sql.startswith("FROM ", i)
    )
    items, start = [], begin
    for index in range(begin, stop):
        if sql[index] == "," and depths[index] == 0:
            items.append(sql[start:index].strip())
            start = index + 1
    items.append(sql[start:stop].strip())
    return items, sql[stop + len("FROM ") :].split()[0]


def _cte_body(sql: str, name: str) -> str:
    """The named CTE's body, taken by balanced parentheses rather than by a regex."""
    depths = _paren_depth(sql)
    marker = f"{name} AS ("
    at = next(i for i in range(len(sql)) if depths[i] == 0 and sql.startswith(marker, i))
    opened = at + len(marker) - 1
    return sql[opened + 1 : next(i for i in range(opened + 1, len(sql)) if depths[i] == 0)]


def _column_name(item: str) -> str:
    """One projection item's published name: its alias if it has one, else its column."""
    return item.rsplit(" AS ", 1)[-1].strip().rsplit(".", 1)[-1]


def _published_columns(sql: str) -> tuple[str, ...]:
    """The column names `sql` PUBLISHES, with a `SELECT *` resolved through its own CTE.

    NARROW ON PURPOSE AND NOT A SQL PARSER: an alias after the last top-level ` AS `, the
    bare column otherwise, and one level of `*`. It would answer wrongly for shapes these
    two views do not have, which is why the test below runs control assertions on it."""
    items, relation = _projection_of(sql)
    names: list[str] = []
    for item in items:
        if item != "*":
            names.append(_column_name(item))
            continue
        inner, _ = _projection_of(_cte_body(sql, relation))
        names.extend(_column_name(part) for part in inner)
    return tuple(names)


def test_the_registry_key_column_is_spelled_the_way_both_f4_views_already_spell_it():
    """`source` IS NOT A NEW NAME HERE, and this is what keeps that sentence true.

    `dataops_reconciliation` and `dataops_freshness` both publish the registry key under
    that name. Calling it `table` here -- which this feed did until this correction -- put
    ONE value under TWO names across three sibling views, and every later join between
    them would have carried a translation for a rename that never had to happen. The
    module header rests on a fact about two OTHER modules, so it is asserted rather than
    remembered: a rename on either side fails here, in the commit that does it.

    TWO KINDS OF EVIDENCE, BECAUSE ONE KIND HAS NOW GONE BLIND TWICE ON THIS EXACT FACT.

      * WHAT EACH VIEW PUBLISHES, read off its outer `SELECT` -- the half the sentence
        above is actually about, and the half both earlier drafts missed. `x in
        freshness_sql()` passed with the measurement leg renamed, because another leg still
        spelled `AS source` somewhere in the same string; counting `' AS source,'` then
        passed with the PUBLISHED column renamed to `table_name`, because those counts land
        on the CTE arms and the arms were untouched. Both asked whether a substring appears
        SOMEWHERE. This asks what the query emits, which is what a later join sees.
      * THE COUNT PER REGISTRY KEY, KEPT, for the direction the first half cannot see: a
        rename inside one UNION leg moves a number here, while the published column is a
        single `m.source` over the folded CTE and would not notice. Reconciliation names
        the key once per table, freshness twice -- it measures and it declares.

    THE CONTROLS ARE ON THE READER AND NOT ON THE VIEWS. A garbage tuple could contain
    `source` for reasons having nothing to do with either query, so `cadence_kind` (which
    can only be there if `*` resolved through the `aged` CTE) and `batch_id` (only if an
    ` AS ` alias was read) are asserted FIRST -- they say the tuple is the real published
    list, before `source` is looked for in it."""
    keys = len(REGISTRY)
    freshness_columns = _published_columns(freshness_sql())
    reconciliation_columns = _published_columns(batch_grain_sql())

    assert "cadence_kind" in freshness_columns, "the reader lost the `*` through `aged`"
    assert "batch_id" in reconciliation_columns, "the reader lost the ` AS ` alias"
    assert "source" in freshness_columns
    assert "source" in reconciliation_columns

    assert f"{table_of_job_sql()} AS source," in incident_feed_sql()
    assert batch_grain_sql().count("' AS source,") == keys
    assert freshness_sql().count("' AS source,") == 2 * keys


# ----------------------------------------------------------------------------------
# The declaration, held equal to the bundle. No Spark below this line.
# ----------------------------------------------------------------------------------


def _dq_gate_tables_of_bundle(root: Path = RESOURCES) -> dict[str, str]:
    """Every job in the bundle that runs the DQ gate, and the table its YAML hands it.

    Read from the PARSED YAML rather than by regex, and swept over every file in
    `resources/` rather than over a list of known jobs: a new bronze job carrying a
    `fail_on_dq` task is exactly the drift this lock exists to catch, and a list would
    have to be updated by the same person who forgot the declaration.

    "EVERY FILE" IS `job_yaml.resource_files`' ANSWER AND NOT A SUFFIX SPELLED HERE, which
    is what the sentence above had been claiming while reading one suffix of that set."""
    found: dict[str, str] = {}
    for path in resource_files(root):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job in (document.get("resources", {}).get("jobs", {}) or {}).values():
            gates = [
                task
                for task in job.get("tasks", [])
                if task.get("task_key") == DQ_GATE_TASK_KEY
            ]
            if not gates:
                continue
            assert len(gates) == 1, f"{path.name} declares {len(gates)} gate tasks"
            parameters = gates[0]["spark_python_task"]["parameters"]
            assert len(parameters) == 1, (
                f"{path.name} hands the gate {parameters}, expected one table name"
            )
            assert job["name"] not in found, f"two jobs are named {job['name']!r}"
            found[job["name"]] = parameters[0]
    return found


def test_a_backslash_escaped_apostrophe_does_not_end_a_literal():
    """The projection reader must survive the prose `cadence.why` is allowed to carry.

    THE TWO ARMS ARE THE POINT. `plain` is the same shape without an escape, so a failure
    on `escaped` is about the escape and not about the reader in general -- a single arm
    here would be satisfied by a reader that is broken for both.

    The literal in `escaped` carries a comma AND the word `FROM`, which are exactly the
    two tokens `_projection_of` reads as structure. A reader that leaves the literal at the
    escaped quote sees both, splits the projection at the comma and stops at a `FROM` that
    is not one.

    IT FAILS LOUDLY IN BOTH SIZES, AND THE TWO FAILURES ARE DIFFERENT, WHICH IS SAID HERE
    BECAUSE A DOCSTRING THAT QUOTED ONE OF THEM FOR THE OTHER WOULD BE THIS PHASE'S OWN
    SPECIES. Over the real `freshness_sql()`, with an apostrophe in a live `cadence.why`,
    the reader ran off the end and raised a bare `IndexError` -- measured by the reviewer
    who found this. Over the small SQL below it does not raise; it returns the wrong
    projection and the assertion names it. Neither outcome is a silent wrong list with
    `source` still in it, which is the only outcome that would matter."""
    plain = "SELECT 'a note' AS cadence_note, source FROM aged"
    escaped = "SELECT 'an operator\\'s, note FROM nowhere' AS cadence_note, source FROM aged"
    assert _projection_of(plain) == (["'a note' AS cadence_note", "source"], "aged")
    assert _projection_of(escaped) == (
        ["'an operator\\'s, note FROM nowhere' AS cadence_note", "source"],
        "aged",
    )


def test_every_job_that_runs_the_dq_gate_declares_the_table_its_yaml_hands_it():
    """THE LOCK THE MAPPING FORK RESTS ON, and it is equality in both directions.

    `TABLE_OF_JOB` is data in the wheel because `databricks/resources/` is not IN the
    wheel -- `packages = ["src/opl"]` -- so a module that read the YAMLs at run time would
    work here and raise in the workspace. What makes data safe rather than merely honest
    is that it cannot go stale silently: a new bronze job, a renamed job, or a job
    repointed at another table's pipeline all fail here, in the commit that does it."""
    assert _dq_gate_tables_of_bundle() == TABLE_OF_JOB


def test_the_lock_catches_a_job_pointed_at_another_tables_pipeline(tmp_path):
    """Proves the lock above can FAIL. A test that reads a file passes just as happily on
    a typo in its own extraction as on correct wiring.

    The mutation is the production defect itself: point the Estabelecimentos gate at the
    lookup table -- the drift that once sent Estabelecimentos triagers to a table full of
    unrelated F1.2 rows. The whole directory is copied rather than one file, because this
    reader sweeps a directory and a one-file root would fail for the wrong reason."""
    root = tmp_path / "resources"
    shutil.copytree(RESOURCES, root)
    target = root / "bronze_estabelecimentos_job.yml"
    original = target.read_text(encoding="utf-8")
    drifted = original.replace('parameters: ["estabelecimentos"]', 'parameters: ["lookup"]')
    assert drifted != original, "the mutation did not apply -- this test proves nothing"
    target.write_text(drifted, encoding="utf-8")

    mutated = _dq_gate_tables_of_bundle(root)
    assert mutated != TABLE_OF_JOB
    assert mutated["opl-bronze-cnpj-estabelecimentos"] == "lookup"


def test_the_declaration_is_total_over_the_bronze_registry_and_claims_no_table_twice():
    """The two properties, STATED. What can fail is the three tests below.

    This mirrors `tests/dataops/test_cadence.py::test_the_cadence_is_total_over_the_bronze
    _registry` and, like it, restates a refusal that already ran at import -- so it cannot
    fail here, because a mapping that broke either property would have failed collection.
    It is the readable statement of the contract; the mutations underneath are the proof."""
    assert set(TABLE_OF_JOB.values()) == set(REGISTRY)
    assert len(set(TABLE_OF_JOB.values())) == len(TABLE_OF_JOB)


def test_the_totality_guard_catches_a_registered_table_no_job_gates(monkeypatch):
    """Proves the first guard can FAIL, in the shape it would fail in.

    `cadence.py` pairs every import-time refusal with one of these and this module cites
    `cadence.py` as its pattern; the version of this file that only called the guards over
    VALID data was calling something that had already run at import, and could not fail.
    The defect: a table registered with no DQ-gate job here is gated in the workspace and
    absent from triage, and nothing reports an absence."""
    monkeypatch.setattr(
        incidents_module,
        "TABLE_OF_JOB",
        {job: table for job, table in TABLE_OF_JOB.items() if table != "ptax"},
    )
    with pytest.raises(ValueError, match="not total over the bronze registry"):
        _assert_the_declared_jobs_cover_exactly_the_registered_tables()


def test_the_injectivity_guard_catches_an_eighth_job_claiming_a_claimed_table(monkeypatch):
    """Proves the second guard can FAIL -- and that it sees a state the first one cannot.

    An eighth job naming `lookup` leaves the VALUE SET unchanged, so totality is still
    satisfied: that is asserted here rather than argued, because it is the whole reason
    two guards exist instead of one. What it costs is two `job_run_id`s resolving to one
    quarantine and a triager sent to another pipeline's rejected rows.

    THE VALUE SET IS ASSERTED AND NOT ONLY THE CALL. Calling the totality guard and
    watching it not raise is satisfied by a guard that does nothing, which is this
    repository's most-hunted species and is the reason the equality is spelled out beside
    it: the headline claim is that this mutation leaves the value set UNCHANGED, so that is
    what is checked, and the silent call is then evidence about the guard rather than the
    only evidence in the test."""
    eighth = {**TABLE_OF_JOB, "opl-bronze-cnpj-estabelecimentos-v2": "lookup"}
    monkeypatch.setattr(incidents_module, "TABLE_OF_JOB", eighth)

    assert set(eighth.values()) == set(TABLE_OF_JOB.values())
    assert len(eighth) == len(TABLE_OF_JOB) + 1
    _assert_the_declared_jobs_cover_exactly_the_registered_tables()
    with pytest.raises(ValueError, match="two jobs claim one table"):
        _assert_no_two_jobs_claim_the_same_table()


def _reimported_incidents():
    """A SECOND execution of `incidents.py`'s module body, from its own file.

    Not `importlib.reload`, which would rebind the module every other test imported from.
    This builds a throwaway module, never enters it into `sys.modules`, and runs the body
    -- which is the only way to observe what the import-time calls do."""
    spec = importlib.util.spec_from_file_location(
        "opl.triage_agent._incidents_reimported", incidents_module.__file__
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_guards_run_at_import_so_deleting_the_call_is_a_failure_not_a_silent_loss(
    monkeypatch,
):
    """The claim the previous version of this file MADE and did not test.

    It said calling the guards in a test meant "a future edit removing the import-time call
    is a failure rather than a silent loss". Deleting both calls at the bottom of
    `incidents.py` left the suite GREEN -- the colour, not the total that stood here, which
    has gone stale since -- so the sentence was supported by nobody checking it. This is the
    test that makes it true: `REGISTRY` gains a table no job
    gates, the module body is executed again, and the ValueError has to come out of the
    IMPORT. With the two calls deleted the re-execution returns a module and this fails.

    The first line is the control -- re-executing an UNMUTATED module must succeed, or the
    raise below could be about the re-execution rather than about the declaration."""
    assert _reimported_incidents().TABLE_OF_JOB == TABLE_OF_JOB

    monkeypatch.setitem(REGISTRY, "a_table_no_job_gates", REGISTRY["ptax"])
    with pytest.raises(ValueError, match="not total over the bronze registry"):
        _reimported_incidents()


def test_the_lock_catches_a_gate_task_renamed_in_the_bundle_and_not_here(tmp_path):
    """The half of the retired-task-key hazard this repository CAN close, fired.

    `dq_gate` -> `dq_gate_batch` happened in this project and the telemetry marked nothing
    (`docs/f6-run-evidence.md` 0.8), so a renamed gate task is measured history rather than
    a hypothesis. Nothing in the wheel can recover runs already recorded under a retired
    spelling -- the module header says so -- but the YAML reader matches on
    `DQ_GATE_TASK_KEY`, so a rename that reaches the bundle and not the constant drops that
    job out of the sweep and fails the lock in the commit that does it.

    THE SURVIVING SIX ARE NAMED, AND WITHOUT THAT LINE THIS TEST PASSED ON `{}`. Both of
    its assertions were negative -- a name absent, a mapping unequal -- and a reader that
    read NOTHING satisfies both: narrowing `_dq_gate_tables_of_bundle`'s sweep to a suffix
    no file under `resources/` carries failed the two sibling tests and left this one GREEN,
    reporting that a rename is caught by a reader that had found no jobs at all. The
    equality below is the discriminating arm, in the same test function, which is the
    standard this file states forty lines up and dropped here."""
    root = tmp_path / "resources"
    shutil.copytree(RESOURCES, root)
    target = root / "bronze_payments_job.yml"
    original = target.read_text(encoding="utf-8")
    renamed = original.replace(f"task_key: {DQ_GATE_TASK_KEY}", "task_key: fail_on_dq_v2")
    assert renamed != original, "the mutation did not apply -- this test proves nothing"
    target.write_text(renamed, encoding="utf-8")

    mutated = _dq_gate_tables_of_bundle(root)
    assert mutated == {
        job: table for job, table in TABLE_OF_JOB.items() if job != "opl-bronze-payments"
    }
    assert "opl-bronze-payments" not in mutated
    assert mutated != TABLE_OF_JOB
