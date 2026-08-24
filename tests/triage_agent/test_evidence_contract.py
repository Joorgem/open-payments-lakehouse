"""What `evidence.py` promises before a single row is read: the refusals and the locks.

SEPARATED FROM THE SPARK-RUNNING TESTS AT A SUBJECT SEAM AND NOT AT A LINE COUNT, and
decided before either file was written so there is no move for a later reader to have to
diff. Those tests run SQL over real tables and answer "what does this return"; this file
answers "what does this refuse, and what is it locked against" -- none of which needs a
row, a table or a JVM, and all of which changes for different reasons. It is
`test_incidents_declaration.py`'s seam, taken deliberately rather than after the cap was
hit.

THE HALF THIS ONE WAS SEPARATED FROM WAS ITSELF SPLIT LATER, at `ac984e5`, when it reached
the 800-line cap: `test_evidence_census.py` took the counts and the verdicts,
`test_evidence_sample.py` took the row states and the taint sweep. Where this header names
the other arm of a two-arm property, it names `test_evidence_sample.py`, which is where the
sweep went -- **and that repointing is why this paragraph exists**: the earlier text said
`test_evidence.py`, and a citation a reader cannot open is worse than none, because they
cannot tell a missing file from a withheld argument.

Nothing here starts Spark, and nothing here enforces that -- the same property, unguarded
for the same reason, that `test_incidents_declaration.py` records: every cheap spelling of
the guard is this repository's hunted species one level down, and the honest spelling is a
one-file special case inside a repo-wide sweep.

THE TWO IMPORT-TIME GUARDS ARE FIRED ON THE DECLARATION THEY EXIST TO REFUSE, and the
import-time CALL is fired by re-executing the module body. Calling a guard over data that
already passed at import only restates its body: T1 shipped exactly that, and deleting both
calls left its suite green. That is the mistake this file is written not to repeat.

"NO PUBLISHABLE STATEMENT READS A PERSONAL COLUMN" IS CARRIED BY TWO ARMS IN TWO FILES, AND
NEITHER OF THEM IS TOTAL. Stated here, at file level, because the temptation a later reader
has is to call one of them redundant -- and each arm's ONLY coverage is the other's measured
blind spot.

  * THIS FILE COUNTS THE COLUMN NAME in the generated SQL, needing no fixture, no row and no
    session. A read that spells the name is a second occurrence in any spelling of it:
    backticked, bare, aliased, or wrapped in a function. The function case is the one this
    arm alone can reach -- measured 2026-08-24, adding
    `SUBSTR(nome_socio_razao_social, 1, 3) AS initials` to `row_shapes_sql`'s outer
    projection puts the first three characters of a real partner name in a published row and
    `test_evidence_sample.py` passes ENTIRELY, because the transform drops the planted sentinel and
    leaves its taint sweep nothing to find.
  * IT IS BLIND TO EVERY LEAK THAT NEVER SPELLS THE NAME, and that door is open BY
    CONSTRUCTION: `row_shapes_sql` builds its inner CTE as `SELECT * FROM <quarantine>`, so
    `sampled.*` in the outer projection reads every personal column and adds ZERO to
    `shapes.count(column)`. Measured 2026-08-24: with `sampled.*` in that same outer
    projection, every test in THIS file passes -- blind -- while `test_evidence_sample.py`'s taint
    sweep goes red. The exact mirror of the bullet above. `struct(*)` and `t.*` are the same
    door by the same arithmetic -- neither spells the name -- and were NOT separately
    measured; only the star was.
  * `test_evidence_sample.py` IS THE OTHER ARM. It reads the RESULTS and looks for planted values,
    so it sees a leak in any spelling including a star -- and only where the leaked text
    still carries a sentinel, which is why its `_TAINT_SWEEP` walks a batch the eleven
    incidents do not.

AND NO PAIR OF PASS TOTALS IS QUOTED FOR ANY OF THAT, WHICH IS A CORRECTION RATHER THAN A
STYLE. The count check's docstring recorded its founding mutation as passing "with
`13 passed` and `24 passed`"; a re-run reproduces neither the numbers nor the colour,
because tests have been added to both files and `_TAINT_SWEEP` now reaches a batch where
that column holds a findable value -- so, measured 2026-08-24, that mutation is now caught
in BOTH files rather than passing through either. A total goes stale on the next commit that
adds a test, in the one place whose subject is a measurement of blindness. What is named
instead is the mutation and which FILE stayed green under it.
"""
from __future__ import annotations

import importlib.util

import pytest

from opl.bronze import reconcile as reconcile_module
from opl.bronze.autoloader import SOURCE_FILE_COLUMN
from opl.bronze.dq import RESCUED_DATA_COLUMN
from opl.bronze.masking import MASKED_COLUMNS
from opl.bronze.reconcile import (
    BATCH_GRAIN_VIEW,
    OVER_PROMOTED,
    RECONCILED,
    STRANDED_GATED,
    STRANDED_UNEXPLAINED,
)
from opl.bronze.registry import REGISTRY, UnknownTable, table_spec
from opl.bronze.rule_predicates import _REPLACEMENT_CHAR
from opl.config import DEFAULT
from opl.contracts.catalogue import columns_for
from opl.triage_agent import evidence as evidence_module
from opl.triage_agent.evidence import (
    CENSUS_VERDICTS,
    MASKED,
    NO_RECONCILIATION_ROW,
    PUBLISHABLE,
    REPLACEMENT_CHARACTER,
    SAMPLE_LIMIT,
    VALUE_STATES,
    _assert_every_masked_column_is_one_this_module_profiles,
    _assert_the_absence_word_is_not_a_reconciliation_verdict,
    evidence_sql,
    masked_columns,
    profiled_columns,
    quarantine_census_sql,
    reconciliation_sql,
    row_sample_sql,
    row_shapes_sql,
)

_SOCIOS = table_spec("socios")
_PAYMENTS = table_spec("payments")
_DECLARED_PERSONAL = ("nome_socio_razao_social", "nome_do_representante")


# ----------------------------------------------------------------------------------
# The masked set: one spelling, taken from the declaration that creates the masks.
# ----------------------------------------------------------------------------------


def test_the_masked_set_is_the_declaration_itself_and_not_a_copy_of_it():
    """`masking.MASKED_COLUMNS` is where the masks are GENERATED FROM, so it is the
    authority rather than a restatement of one -- which is the whole reason the catalog was
    not read instead. Held equal in both directions, and asserted to be non-empty: a set
    that had silently become empty would redact nothing and pass every "no leak" check that
    only looks for the wrong word."""
    assert set(masked_columns(_SOCIOS)) == set(MASKED_COLUMNS["socios"])
    assert masked_columns(_SOCIOS) == _DECLARED_PERSONAL
    assert masked_columns(_PAYMENTS) == (), "no payments column is declared personal"
    assert {spec.name for spec in REGISTRY.values() if masked_columns(spec)} == {"socios"}


def test_the_masked_columns_are_in_contract_order_and_not_declaration_order():
    """So that reordering the declaration is not a diff in generated SQL -- the same
    property `incidents.table_of_job_sql` gets from `sorted`, and for the same reason."""
    contract = columns_for("socios")
    assert list(masked_columns(_SOCIOS)) == [c for c in contract if c in _DECLARED_PERSONAL]
    assert contract.index("nome_socio_razao_social") < contract.index("nome_do_representante")


def test_every_profiled_column_is_the_contract_plus_the_rescued_blob():
    """`_rescued_data` is in and the other six metadata columns are out.

    It is the evidence for `rescued_data_present`, which is the single largest incident in
    the corpus (2,000 rows), so a profile without it says nothing about that one at all."""
    assert profiled_columns(_SOCIOS) == (*columns_for("socios"), RESCUED_DATA_COLUMN)
    assert set(_DECLARED_PERSONAL) <= set(profiled_columns(_SOCIOS))
    for spec in REGISTRY.values():
        assert set(masked_columns(spec)) <= set(profiled_columns(spec)), (
            f"{spec.name} declares a personal column this module would not profile, so "
            "nothing here would redact it"
        )


# ----------------------------------------------------------------------------------
# What may be published, read off the generated SQL rather than off a docstring.
# ----------------------------------------------------------------------------------


def test_no_publishable_statement_READS_a_declared_personal_column():
    """NAMING IT AND READING IT ARE DIFFERENT THINGS, and the distinction is the design.

    `row_shapes_sql` DOES publish the column's NAME -- as a map key beside the word
    `masked`, which is the whole point: a triager has to be told that this column is
    personal data and that its value is therefore not evidence here. What no publishable
    statement may do is READ it.

    SO THE CHECK IS A COUNT AND NOT AN ABSENCE, because an absence check has to guess the
    spelling of a read and there is more than one. The first version of this test banned
    only the BACKTICKED form the state expression builds, and `nome_socio_razao_social AS
    leaked_name` -- unbackticked, which is this module's own house spelling three lines away
    (`row_shapes_sql` writes `_dq_reject_reason AS reject_reason`) -- walked straight
    through it, green here and green in `test_evidence_sample.py` as both files then stood. What is
    asserted instead is the ONLY legal occurrence: the name appears in `row_shapes` EXACTLY
    ONCE, and that once is the quoted map key beside `masked`.

    SO A READ IS A SECOND OCCURRENCE IN ANY SPELLING OF THE NAME -- AND IN NO OTHER CASE.
    What that covers, what it cannot, and why the sweep in `test_evidence_sample.py` is not a
    duplicate of it are in this file's own header, with both mutations measured.

    THE CONTROL IS IN THE SAME STRINGS. Every UNMASKED socios column must be read in
    `row_shapes`, or this passes over a statement that reads nothing at all and over a
    reader pointed at the wrong text -- which is how two successive versions of T1's
    cross-module lock passed under the mutation they existed to catch. The census and the
    reconciliation work at batch grain, so they must not NAME a contract column at all."""
    statements = evidence_sql("socios")
    shapes = statements["row_shapes"]

    for column in _DECLARED_PERSONAL:
        assert shapes.count(column) == 1, (
            f"row_shapes names {column} {shapes.count(column)} times; the map key is the "
            "only legal occurrence, so any other one is a read"
        )
        assert f"'{column}', '{MASKED}'" in shapes, (
            f"{column} must still be NAMED, as a map key beside '{MASKED}', or the reader "
            "is not told the column exists and that its value is withheld"
        )

    for name in ("census", "reconciliation"):
        named = [c for c in columns_for("socios") if c in statements[name]]
        assert named == [], f"{name} names {named}; it works at batch grain and needs none"

    unmasked = [c for c in columns_for("socios") if c not in _DECLARED_PERSONAL]
    assert len(unmasked) == 9
    assert all(f"`{column}`" in shapes for column in unmasked)


def test_the_not_publishable_sample_withholds_the_personal_columns_and_the_rescued_blob():
    """`row_sample_sql` projects real values, so what it may NOT name is the tighter list.

    The blob goes with them for any contract that declares a personal column: it is
    UNPARSED SOURCE TEXT, so the same name can arrive inside it under another key, and
    redacting the named column while projecting the blob would be a control applied by
    column name rather than by what the column holds. Payments declares none, so it keeps
    the blob -- which is the arm that proves the withholding is conditional and not a
    blanket omission."""
    socios = row_sample_sql(_SOCIOS)
    for column in _DECLARED_PERSONAL:
        assert column not in socios
    assert RESCUED_DATA_COLUMN not in socios
    assert "cnpj_basico" in socios and "faixa_etaria" in socios

    assert RESCUED_DATA_COLUMN in row_sample_sql(_PAYMENTS)


def test_evidence_sql_returns_the_publishable_statements_and_cannot_reach_the_sample():
    """THE STRUCTURE, not a label. An issue renderer is handed the assembler's output; the
    statement that projects values is absent from it BY CONSTRUCTION, so reaching a row
    value takes a second, explicit call to a differently-named function.

    THE KEYS DO NOT IMPLY THE VALUES, which is why the last two lines are not restatements
    of the first. `PUBLISHABLE` is a tuple of KEY names, so returning
    `{"row_shapes": row_sample_sql(spec, config, limit=limit), ...}` satisfies both key
    assertions exactly; measured 2026-08-24, that substitution leaves both key assertions
    green and fails the identity check on the next line. The review that called that line
    unreachable was reading the keys as if they pinned the values, and they do not.

    WHAT THE IDENTITY CHECK CANNOT DO is see the same swap at a DIFFERENT bound: measured,
    `row_sample_sql(spec, config, limit=5)` under that key leaves the identity line GREEN --
    it is not the string `sample` holds -- and only the fingerprint below goes red.
    `_source_file` is `row_sample_sql`'s own projection at any limit and is named by none of
    the three publishable statements. Neither line covers the other."""
    statements = evidence_sql("socios")

    assert tuple(statements) == PUBLISHABLE
    assert set(statements) == {"census", "row_shapes", "reconciliation"}
    sample = row_sample_sql(_SOCIOS)
    assert sample not in statements.values()
    assert all(SOURCE_FILE_COLUMN not in sql for sql in statements.values()), (
        f"{SOURCE_FILE_COLUMN} is `row_sample_sql`'s own projection and no publishable "
        "statement's, so it is where the sample shows up under a bound the line above "
        "cannot match"
    )


def test_every_statement_binds_the_batch_id_and_labels_itself_with_the_registry_key():
    """One `args` binding serves all three, and each row says what it is about.

    The batch id is the one value that reaches these from outside the wheel, and it is a
    parameter marker rather than an interpolation -- `reconcile.file_accounts_sql`'s rule,
    for its reason: bound, a hostile id matches nothing; spliced, it ends the string."""
    for name, sql in evidence_sql("socios").items():
        assert ":batch_id" in sql, f"{name} does not bind the batch id"
        assert "AS batch_id" in sql and "'socios' AS source" in sql, name


def test_the_reconciliation_reads_the_view_this_project_deploys():
    """The default relation is F4's view, spelled from its own constant and `config` rather
    than retyped -- so the test seam cannot leak into what deploys.

    THE LITERAL BESIDE IT IS A DELIBERATE GOLDEN COPY AND NOT A HARDCODED COORDINATE TO BE
    TIDIED AWAY. `DEFAULT.table(BATCH_GRAIN_VIEW)` is computed from the same two names the
    statement is, so on its own it would still pass if `BATCH_GRAIN_VIEW` were renamed or
    `DEFAULT` re-pointed -- it would just agree with the new answer. The typed-out string
    is the only thing here that knows what the deployed coordinate IS, and a rename that
    reaches the workspace has to come through this line."""
    assert DEFAULT.table(BATCH_GRAIN_VIEW) in reconciliation_sql(_SOCIOS)
    assert "workspace.default.dataops_reconciliation" in reconciliation_sql(_SOCIOS)
    assert "elsewhere.recon" in reconciliation_sql(_SOCIOS, view="elsewhere.recon")


def test_the_row_reading_statements_read_the_quarantine_and_never_staging():
    """Staging holds every rejected row UNMASKED (ADR 0018 Decision 5), which is exactly
    why nothing here may read it: it is the readable copy, and the readable copy is the one
    that must not reach an artefact. The quarantine is the masked one.

    THE BRONZE TABLE IS NOT ASSERTED AGAINST and that is not an omission: `bronze_cnpj_
    socios` is a PREFIX of `bronze_cnpj_socios_quarantine`, so a substring test on it is
    true for every statement here and would be a check that cannot fail. Staging's name is
    not a prefix of anything, so that one means something."""
    for sql in (quarantine_census_sql(_SOCIOS), row_shapes_sql(_SOCIOS), row_sample_sql(_SOCIOS)):
        assert DEFAULT.table(_SOCIOS.quarantine) in sql
        assert _SOCIOS.staging not in sql
    assert _SOCIOS.bronze in _SOCIOS.quarantine, "the prefix the paragraph above rests on"


# ----------------------------------------------------------------------------------
# The vocabularies, and the words that must stay distinguishable.
# ----------------------------------------------------------------------------------


def test_the_replacement_character_is_the_one_the_gate_rejects_rows_for():
    """A second spelling of a CHARACTER is still a second spelling.

    The gate's `encoding_replacement_char` rule matches `rule_predicates._REPLACEMENT_CHAR`;
    a sampler matching a different character would report `present` for the very rows that
    reason describes, and no test that only read this module could see it. The code point
    is written out here as a third, independent spelling, so the two modules cannot agree
    on the wrong character together."""
    assert REPLACEMENT_CHARACTER == _REPLACEMENT_CHAR
    assert REPLACEMENT_CHARACTER == chr(0xFFFD)
    assert len(REPLACEMENT_CHARACTER) == 1


def test_the_absence_word_is_none_of_the_reconciliation_verdicts():
    """Reported AS absence means a word that cannot be read as a judgement.

    `reconciled` would claim the batch is finished and NULL is read as nothing wrong by the
    first consumer that formats it, so the column emits neither. Five of eleven incidents
    land here, which makes this the majority rendering rather than an edge."""
    verdicts = (RECONCILED, STRANDED_GATED, STRANDED_UNEXPLAINED, OVER_PROMOTED)

    assert NO_RECONCILIATION_ROW not in verdicts
    assert NO_RECONCILIATION_ROW not in CENSUS_VERDICTS
    assert len(set(CENSUS_VERDICTS)) == 3 and len(set(VALUE_STATES)) == 5


# ----------------------------------------------------------------------------------
# The refusals.
# ----------------------------------------------------------------------------------


def test_an_incident_whose_source_is_missing_is_refused_by_name():
    """T1 emits a NULL `source` for a gate that fired on a job its declaration does not
    know, ON PURPOSE, so a rename that reached the workspace and not the repository stays
    visible. There is then no quarantine to read, and returning an empty census for it
    would render a stale declaration exactly like a clean batch."""
    for missing in (None, "", "   "):
        with pytest.raises(UnknownTable, match="no bronze table can be resolved"):
            evidence_sql(missing)

    with pytest.raises(UnknownTable, match="unknown bronze table"):
        evidence_sql("a_table_this_project_does_not_register")


@pytest.mark.parametrize("limit", [0, -1, 1.5, "20", None, True])
def test_a_sample_limit_that_is_not_a_positive_whole_number_is_refused(limit):
    """It is written straight into a LIMIT clause, and an unbounded sample is a dump.

    `True` is in the list because `bool` is an `int` in Python, and `LIMIT true` is a
    parse error arriving from a value that passed an `isinstance` check."""
    with pytest.raises(ValueError, match="not a positive integer"):
        row_shapes_sql(_SOCIOS, limit=limit)
    with pytest.raises(ValueError, match="not a positive integer"):
        row_sample_sql(_SOCIOS, limit=limit)


def test_the_default_bound_is_carried_into_the_statement():
    """The control for the test above: the refusals prove nothing if the accepted value
    never reaches the SQL."""
    assert f"LIMIT {SAMPLE_LIMIT}" in row_shapes_sql(_SOCIOS)
    assert "LIMIT 7" in row_shapes_sql(_SOCIOS, limit=7)


# ----------------------------------------------------------------------------------
# The two import-time guards, fired on what they refuse -- and on the import itself.
# ----------------------------------------------------------------------------------


def test_a_mask_declaration_naming_a_column_no_contract_has_is_refused(monkeypatch):
    """The entry would redact NOTHING here -- `masked_columns` filters against the contract
    -- so a column a reader believes is masked would be profiled by reading its value. That
    is the failure this module exists to prevent, arriving through its own declaration.

    The mutation is asserted to be inert in the direction the guard does NOT cover: the
    contract's own column list is unchanged, so this is not a test of `columns_for`."""
    monkeypatch.setitem(MASKED_COLUMNS, "socios", (*_DECLARED_PERSONAL, "a_column_socios_lacks"))

    assert "a_column_socios_lacks" not in columns_for("socios")
    with pytest.raises(ValueError, match="a_column_socios_lacks"):
        _assert_every_masked_column_is_one_this_module_profiles()


def test_a_mask_declaration_naming_a_contract_no_source_declares_is_refused(monkeypatch):
    """The other half, and it fails differently: an unknown contract has no column list at
    all, so the check above would raise a bare KeyError naming nothing a reader can act
    on."""
    monkeypatch.setitem(MASKED_COLUMNS, "a_contract_nothing_declares", ("some_column",))

    with pytest.raises(ValueError, match="which no source declares"):
        _assert_every_masked_column_is_one_this_module_profiles()


def test_an_absence_word_that_collides_with_a_reconciliation_verdict_is_refused(monkeypatch):
    """A rename in `reconcile.py` that reached this word would break the one property that
    column has, silently: it would emit one string for a batch the view judged and a batch
    it cannot speak for."""
    monkeypatch.setattr(evidence_module, "NO_RECONCILIATION_ROW", RECONCILED)

    with pytest.raises(ValueError, match="also one of"):
        _assert_the_absence_word_is_not_a_reconciliation_verdict()


def _reimported_evidence():
    """A SECOND execution of `evidence.py`'s module body, from its own file.

    Not `importlib.reload`, which would rebind the module every other test imported from.
    This builds a throwaway module, never enters it into `sys.modules`, and runs the body
    -- which is the only way to observe what the import-time calls do."""
    spec = importlib.util.spec_from_file_location(
        "opl.triage_agent._evidence_reimported", evidence_module.__file__
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_guards_run_at_import_so_deleting_the_call_is_a_failure_not_a_silent_loss(
    monkeypatch,
):
    """Calling a guard in a test and watching it raise says the guard works. It says
    nothing about whether anything CALLS it -- and T1 shipped exactly that gap, where
    deleting both import-time calls left the suite green.

    The first line is the control: re-executing an UNMUTATED module must succeed, or the
    raise below could be about the re-execution rather than about the declaration."""
    assert _reimported_evidence().VALUE_STATES == VALUE_STATES

    monkeypatch.setitem(MASKED_COLUMNS, "socios", ("a_column_socios_lacks",))
    with pytest.raises(ValueError, match="a_column_socios_lacks"):
        _reimported_evidence()


def test_the_second_guard_runs_at_import_too_and_is_fired_from_the_other_module(monkeypatch):
    """The collision this one refuses is caused ELSEWHERE -- a rename in `reconcile.py` --
    so the mutation is made there and the import is what has to notice. `evidence.py` reads
    those four names at import, so a re-execution against a renamed verdict is exactly the
    commit that would ship the collision.

    Two guards, two import-time calls, and each of them fired separately: a single test
    covering both would pass with one of the calls deleted."""
    assert _reimported_evidence().NO_RECONCILIATION_ROW == NO_RECONCILIATION_ROW

    monkeypatch.setattr(reconcile_module, "RECONCILED", NO_RECONCILIATION_ROW)
    with pytest.raises(ValueError, match="also one of"):
        _reimported_evidence()
