# src/opl/triage_agent/evidence.py
"""The evidence for ONE incident: the census, a bounded sample, and the reconciliation.

WHAT THIS ANSWERS. `incidents.py` says WHICH job runs are being triaged. This says what is
actually in the workspace for one of them -- how many rows the gate rejected and under
which reason, what those rows look like, and whether the batch reconciles. It classifies
nothing: no severity, no recommended action, no issue text. Those are other modules', and
a module that both gathered the evidence and graded it would make the grade unfalsifiable
against the facts it was computed from.

`ZERO ROWS` IS TWO DIFFERENT WORLDS AND ONLY ONE OF THEM CAN FOLLOW A `fail_on_dq`. This
is the whole reason the census emits a verdict rather than only a count, and it is read
off the WIRING rather than off a docstring. `databricks/resources/bronze_payments_job.yml`
declares `check_bad_rows` as a `condition_task`, `op: EQUAL_TO`, left
`{{tasks.dq_gate_batch.values.bad_row_count}}`, right `"0"`; `promote` depends on outcome
`true` and `fail_on_dq` on outcome `false`. And `dq_gate_batch` APPENDS the rejected rows
to quarantine BEFORE it publishes `bad_row_count`. So the chain closes at both ends:

    `fail_on_dq` ran  =>  the gate SUCCEEDED and published a NON-ZERO count
                      =>  that many rows were in quarantine at that instant.

Zero rows there today therefore does not mean "the gate rejected nothing" -- it cannot, or
the incident would not exist. It means THE EVIDENCE WAS REMOVED AFTER THE FACT, which is
neither `clean` nor low severity. A census that returned an empty result set for that case
would render it exactly like a batch nobody ever gated, which is ADR 0018's most-hunted
species arriving in the middle of this phase's deliverable. So the census ALWAYS returns at
least one row, and the row carries the word for which world produced its zero.

AND THE TWO REMOVALS ARE NOT THE SAME REMOVAL, which is why there are two words and not
one. Measured 2026-08-24 over the eleven-incident corpus (`docs/f6-run-evidence.md` 0.3,
0.5): five incidents carry no quarantined rows.

  * `evidence_missing_quarantine_empty` -- the quarantine table holds NO rows at all, for
    any batch. Three lookup incidents are this, and F4 accounts for them: that table was
    recreated on 2026-07-31, a week after its firings. Whatever removed this batch's rows
    removed everything, so the batch is not singled out.
  * `evidence_missing_batch_absent` -- the quarantine holds rows for OTHER batches and
    none for this one. Two estabelecimentos incidents are this (`187805471003061` and
    `315230730740144`) and NOTHING IN THE RECORD EXPLAINS THEM: F4's sentence is about a
    quarantine table that is empty, and this one is not -- it holds the 4 rows of
    `128878829411613`. These two sit in a populated table and contributed nothing to it.

Folding those into one word would let the second borrow the first's explanation, and the
first's explanation is the only one that exists. The distinction is observable in SQL --
whole-table count against batch count -- so it is measured here rather than inferred by a
reader who happens to remember F4.

WHICH OF THE TWO WORDS AN INCIDENT GETS IS A FACT ABOUT THE QUARANTINE TABLE TODAY, not a
durable property of the incident. The discriminator is `quarantine_table_rows = 0`, read
at query time, so the mapping in the two bullets above -- the three lookup incidents to
`evidence_missing_quarantine_empty`, the two estabelecimentos incidents to
`evidence_missing_batch_absent` -- is a READING taken 2026-08-24 and not a label. The
moment `bronze_cnpj_lookup_quarantine` receives one reject from any later run, its three
incidents flip from the word F4 accounts for to the word nothing explains, and they will
be right to flip: a table that holds another batch's rows and none of theirs IS the second
finding, whatever it was last month. The split is durable; the assignment is not, and a
consumer that caches the assignment has cached a table state.

WHAT MAY BE PUBLISHED AND WHAT MAY NOT, AND IT IS STRUCTURAL RATHER THAN REMEMBERED. This
repository is PUBLIC and a later task of this phase opens a GitHub issue from these
records, so a row value that reaches an artefact reaches everyone.

  * PUBLISHABLE -- everything `evidence_sql` returns: `quarantine_census_sql`,
    `row_shapes_sql` and `reconciliation_sql`. None of the three projects a column VALUE.
    Every column they emit is an identifier the platform already publishes (a batch id, a
    registry key), a COUNT, a column NAME, a reject reason this repository declares, or a
    word from the closed vocabulary `VALUE_STATES`. `_dq_reject_reason` holds a declared
    string under BOTH of its sources -- `rules.py`'s rule names, and `dq.RESCUED_REASON`,
    which `dq.py` names as the one reason in that gate that is NOT a rule and appears in no
    `rules_for` set, and which is the reason on the largest incident in this corpus. Two
    sources, one property: written by the repository, never derived from a row.

    THAT IS A PROPERTY OF THE OUTPUT, SO IT IS ASSERTED AGAINST THE OUTPUT -- BY TWO CHECKS
    NEITHER OF WHICH IS TOTAL, and the split is stated here so neither is later dropped as
    redundant. `tests/triage_agent/test_evidence_sample.py` plants a sentinel in every fixture
    value EXCEPT those pinned to a state word a sentinel would destroy (`''`, NULL, `***`)
    and requires that no sentinel reaches these results. It catches a leak in ANY spelling,
    `SELECT *` included, and ONLY where the leaked text still carries a planted sentinel --
    so it is blind where the fixture planted none, and the corpus pins the most sensitive
    column of all: `nome_socio_razao_social` is `''` in both socios batches BECAUSE that
    emptiness is what the gate rejected them for, which is why `_TAINT_SWEEP` walks an
    invented batch as well as the eleven. And blind again to a leak that TRANSFORMS the
    value -- a `SUBSTR`, a hash, a length -- which carries no sentinel either.
    `test_evidence_contract.py` counts the column NAME in the generated SQL instead, needs
    no fixture and no row, and catches those transforms wherever they spell the name -- and
    is blind to any leak that never does. Both blind spots are MEASURED, each in the other's
    coverage; that file's header carries the two mutations and what each of them turned red.
  * NOT PUBLISHABLE -- `row_sample_sql`, which projects real values and exists because a
    human triager reading `encoding_replacement_char` needs the bytes. It is deliberately
    NOT returned by `evidence_sql`, so an issue renderer that calls the assembler cannot
    reach it by accident; reaching it is an explicit second call. It also never projects a
    declared-personal column (see below), so the worst it can leak is non-personal.

AND ONE OF THE THREE PUBLISHABLE STATEMENTS DOES RETURN AN EMPTY RESULT SET, which is the
shape everything above spends a page refusing, so it is named here rather than left to be
found. `row_shapes_sql` returns nothing at all for five of the eleven incidents -- there
are no quarantined rows to sample, so there is no row to shape. The asymmetry is
discharged rather than tolerated, and the census is what discharges it: `evidence_sql`
hands back both statements together, and the census ALWAYS returns a row carrying the word
for WHY there was nothing to sample. Read as a pair, an empty `row_shapes` is still told
apart from a query that never ran; read alone, it is not, which is why the pair and not
the statement is what this module publishes. What would be indefensible is an empty
CENSUS, and that is the one output this module cannot produce.

`***` IS THIS REPOSITORY'S OWN SPECIES AND IT HAS ALREADY COST F4 ONCE. ADR 0018 records a
privacy deploy check where `***` was the answer under all four possible outcomes. Here it
is sharper. The two socios incidents are 3,583 rows whose reject reason is
`null_or_empty_nome_socio_razao_social` -- the rejection IS that the name column is null or
empty -- and THAT SAME COLUMN CARRIES A UC MASK on the quarantine table. Measured
2026-08-24 over `information_schema.column_masks` and tabulated in
`docs/f6-run-evidence.md` 0.9 with its statement id, this workspace has exactly four column
masks and two of them are there:
`bronze_cnpj_socios_quarantine.nome_socio_razao_social` and `.nome_do_representante`. A
triager who samples that column sees `***` and cannot tell "masked from me" from "empty,
which is why the row was rejected" -- which is precisely the fact the reject reason
asserts. One string, two worlds, no way to tell them apart: the species, in the sampler.

SO THE SAMPLER DOES NOT REPORT A MASKED COLUMN'S VALUE AS EVIDENCE. `masked` is its own
word in `VALUE_STATES`, and it is emitted WITHOUT READING THE COLUMN -- the generated SQL
contains a literal where the other columns carry a CASE over the value. Two consequences,
and both are the point:

  * The word cannot be produced by data. A column holding the three characters `***` is
    reported `present`, never `masked`, because nothing reads it to compare.
  * The answer does not depend on WHO RUNS IT. `is_member('opl_pii_readers')` would hand a
    member the real name and everyone else `***`, so a value-reading sampler would produce
    a different artefact for different principals -- and an artefact whose contents turn on
    the caller's group membership is the one thing that must never be published. `masked`
    means "this project declares this column personal data, so its value is not evidence in
    this output, for any reader". It is a statement about the DECLARATION, not a reading.

WHERE THAT COLUMN *CAN* BE READ, NAMED SO THE GAP IS NOT LEFT LOOKING UNFILLABLE -- and
NOT read here. ADR 0018 Decision 5 measured that `masking.py` covers bronze and quarantine
and DELIBERATELY NEVER STAGING, and that staging holds every rejected row unmasked
(55,830,826 = 55,827,243 + 3,583). So the corroboration a triager wants exists, in the one
table this module must not put in an artefact. It is named here so a human with the right
grants knows where to look, and no output of this module reads it.

THE MASKED SET COMES FROM `masking.MASKED_COLUMNS` AND NOT FROM THE CATALOG, and the fork
was decided rather than defaulted. The alternative was
`system.information_schema.column_masks`, read at query time.

  1. **A CATALOG READ FAILS OPEN AND THIS MUST FAIL CLOSED.** An empty result from
     `column_masks` means "no column here is masked" AND "I could not see the masks" --
     wrong catalog, a grant the triage principal lacks, a table whose mask has not been
     applied yet -- and the two are indistinguishable, which is the exact defect this
     module is written against. Worse, they are indistinguishable in the direction that
     PUBLISHES: the sampler would conclude "unmasked" and start reporting `***` as a value.
     The declaration cannot be empty for the wrong reason; it is in the wheel.
  2. **IT IS KEYED BY CONTRACT, SO IT FOLLOWS THE DATA.** A second table ingesting the
     socios contract inherits the redaction here on the day it is registered, whereas a
     catalog read keyed on a table name would answer "not masked" for a table carrying the
     same personal names whose mask has not been applied -- which is exactly when a sampler
     has to be most conservative.

THE DECISION RESTS ON ARGUMENT 1 ALONE, AND A THIRD ARGUMENT THAT USED TO STAND HERE IS
WITHDRAWN RATHER THAN QUIETLY DROPPED. It said a catalog read would be "reading the
consequence and treating it as the definition". This repository's own code refutes that:
`masking.deployed_predicate_sql` and `StaleMaskPredicate` read
`information_schema.routines` for exactly that purpose -- the declaration alone could not
say whether the deploy had landed -- and that read-back is what caught a mask predicate
three commits stale. Reading the consequence to check it against the definition is
established practice here, not a category error, and an argument that would have banned
`assert_mask_predicate` cannot be one of the reasons this module is built the way it is.

AND THE REFUSAL LEAVES A GAP, IN THE DIRECTION ARGUMENT 1 IS WRITTEN AGAINST. `declared
UNION catalog` fails closed in BOTH directions; the declaration alone fails closed in only
one. A mask applied out of band to a table whose contract is not in `MASKED_COLUMNS` is
invisible to the declaration, so the sampler would read that column, receive `***` from
the mask it does not know about, and report it as `present` -- one string for two worlds,
arriving through the other door. `_assert_every_masked_column_is_one_this_module_profiles`
closes the declaration-to-contract direction only; nothing here closes
catalog-to-declaration, and nothing can, because the wheel must not need the catalog to
answer this question. It is small today -- every mask in this workspace is generated from
`MASKED_COLUMNS` -- and it is real. THE NAMED FIX IS DEPLOY-TIME AND NOT QUERY-TIME: a
task in `dataops_views_job`, beside the four `SET MASK`s it already issues and modelled on
`assert_mask_predicate`, that reads `system.information_schema.column_masks` back and
fails the run when the deployed set is not the set `masking.set_mask_ddl` generates. That
is F4's job and F6 is not expanding into it. REVERSAL CONDITION: build it the day a mask
reaches this workspace by any path other than `masking.set_mask_ddl` -- a hand-run
`SET MASK`, a second tool, or a masked table this repository does not register.

The half of a lock that can live inside the wheel is `_assert_every_masked_column_is_one_
this_module_profiles`, which refuses at import if the declaration ever names a column no
contract has: such an entry masks nothing here and would leave a real column profiled as
readable. The other half -- that the workspace's four masks are these four -- needs the
workspace and is quoted, not asserted: `docs/f6-run-evidence.md` 0.9 carries it,
controller-verified 2026-08-24, statement `01f19ff8-d9b0-1928-b669-cdc750ea7926`. That
section was written because this citation named the document and no section within it, and
resolved to NOTHING when a reviewer followed it -- in the paragraph whose whole purpose is
separating what this module asserts from what it quotes. AND A QUOTE STAYS A QUOTE: nothing
here re-reads that catalog at triage time, so a mask added or dropped after 2026-08-24 is a
change this module cannot see, which is the gap the paragraph above books rather than one
0.9 closes. The cost of being wrong in the safe direction is one column of detail; in the
other, it is personal data in a public issue.

WHAT THE RECONCILIATION ADDS, AND THAT ITS ABSENCE IS THE MAJORITY CASE. `dataops_
reconciliation` holds 15 rows and covers SIX of the eleven incidents: the five zero-row
incidents have no staging rows either, so the view that would count them cannot speak for
them. Its absence is therefore reported as absence -- `no_reconciliation_row`, a word that
is none of `reconcile.py`'s four verdicts and is locked against them at import -- and the
counts stay NULL rather than being coalesced to zero, which is F4's own rendering of "there
is no metric here" (`sql_telemetry = 'no_sql_attributed'`, never a 0). And where the view
DOES have a row, its verdict is passed through unchanged and NOT re-derived: only
`592660596679630` reads `stranded_gated`; every other batch reads `reconciled`, INCLUDING
four that fired the gate and were later repromoted. A reconciliation is not a test for "the
gate fired", so this column and the incident feed answer different questions and neither
may be read as the other.

WHAT IS NOT HANDLED, SAID RATHER THAN LEFT TO BE DISCOVERED. A quarantine table that does
not EXIST raises an AnalysisException naming it, which is the loud direction and is left
alone: `reconcile._counts_sql`'s `skip` exists because a RECLAIM must decide from whatever
tables are present, and a triage read has no such obligation. Every registered table's
quarantine exists in this workspace today -- ADR 0018 Decision 3 built
`dataops_reconciliation` on the ground that "every column it reads already exists on all 21
bronze-family tables", which is this same set of objects. That is F4's measurement
QUOTED, at F4's date: nothing here re-checks it, so a quarantine dropped since then arrives
as the AnalysisException above rather than as a verdict, which is the direction this
paragraph is content to be wrong in.
"""
from __future__ import annotations

from opl.bronze.autoloader import SOURCE_FILE_COLUMN
from opl.bronze.dq import REJECT_COLUMN, RESCUED_DATA_COLUMN
from opl.bronze.masking import MASKED_COLUMNS
from opl.bronze.promote import BATCH_COLUMN
from opl.bronze.reconcile import (
    BATCH_GRAIN_VIEW,
    OVER_PROMOTED,
    RECONCILED,
    STRANDED_GATED,
    STRANDED_UNEXPLAINED,
)
from opl.bronze.registry import BronzeTable, UnknownTable, table_spec
from opl.config import DEFAULT, OplConfig
from opl.contracts.catalogue import columns_for, is_known

# The three census verdicts. `rows_present` is the only one that is not a finding.
ROWS_PRESENT = "rows_present"
EVIDENCE_MISSING_QUARANTINE_EMPTY = "evidence_missing_quarantine_empty"
EVIDENCE_MISSING_BATCH_ABSENT = "evidence_missing_batch_absent"

CENSUS_VERDICTS = (
    ROWS_PRESENT,
    EVIDENCE_MISSING_QUARANTINE_EMPTY,
    EVIDENCE_MISSING_BATCH_ABSENT,
)

# What the reconciliation column says when `dataops_reconciliation` has no row for the
# batch. NOT NULL and NOT `reconciled`: five of eleven incidents land here, so this is the
# majority rendering rather than an edge, and a NULL passed through would be read as
# "nothing wrong" by the first consumer that formats it.
NO_RECONCILIATION_ROW = "no_reconciliation_row"

# THE CLOSED VOCABULARY A SAMPLED VALUE IS REPORTED IN. No value ever leaves this module
# through these -- each is a word about a value, chosen so that no two of the states this
# corpus actually contains can borrow each other's word.
#
# `masked` is emitted WITHOUT READING THE COLUMN (see the header). `null` and `empty` are
# split because the gate's own `null_or_empty_*` rules are the disjunction of exactly those
# two predicates -- `rule_predicates._null_or_blank` is `isNull() | trim(...) == ''` -- and
# a triager reading a row rejected by that rule wants to know WHICH half fired.
# `replacement_char` is U+FFFD, which ADR 0006 records as the only in-band evidence that a
# byte was lost; reporting such a value as `present` would say nothing about the one
# incident whose reason IS that character.
MASKED = "masked"
NULL_VALUE = "null"
EMPTY = "empty"
REPLACEMENT_CHAR = "replacement_char"
PRESENT = "present"

VALUE_STATES = (MASKED, NULL_VALUE, EMPTY, REPLACEMENT_CHAR, PRESENT)

# U+FFFD. A SECOND SPELLING OF A CHARACTER IS STILL A SECOND SPELLING, so this is held
# equal to `rule_predicates._REPLACEMENT_CHAR` -- the one the gate matches on -- by
# `test_the_replacement_character_is_the_one_the_gate_rejects_rows_for`. It is not
# imported: that name is private to its module, and a public constant reaching into one is
# a dependency neither module declared.
#
# WRITTEN AS AN ESCAPE SO THIS FILE STAYS ASCII, which is a smaller point than it looks:
# the character reaches the generated SQL identically either way, and a source file that
# carries it is one a reader, a diff or a console on this box's cp1252 default can mangle
# without saying so. `rule_predicates.py` spells it literally; there it is the thing being
# matched, here it is a constant.
REPLACEMENT_CHARACTER = "\ufffd"

# How many quarantined rows a sample reads. A SAMPLE AND NOT A CENSUS: there is no ORDER BY
# on it, so which rows come back is Spark's business and is not reproducible across runs.
# The counts a reader acts on come from `quarantine_census_sql`, which is exact and total
# over the batch; this bounds what a triager eyeballs. Twenty because the largest incident
# is 2,000 rows of ONE reason and the smallest is 1.
SAMPLE_LIMIT = 20

# The three statements `evidence_sql` returns, and the set is the structure rather than a
# label: `row_sample_sql` is absent from it BY CONSTRUCTION, so a renderer handed the
# assembler's output cannot reach a row value through it. See the header.
PUBLISHABLE = ("census", "row_shapes", "reconciliation")

# The census ladder, first match wins. It borrows `reconcile._VERDICT_LADDER`'s SHAPE and
# NOT its justification, because it does not have that ladder's property: THESE TWO ARMS
# ARE DISJOINT AND THE ORDER IS PRESENTATIONAL. A batch with quarantined rows has put them
# in the table, so `r.rejected_rows IS NOT NULL` implies `t.quarantine_table_rows >= 1` and
# the second arm cannot fire under the first; swapping them changes nothing any input can
# observe, which was measured by swapping them. It is written most-specific-first because
# that is how it reads. An earlier draft of this comment claimed an ordering hazard -- a
# batch WITH rows reported missing because its table was otherwise empty -- and that input
# does not exist; it was `reconcile.py`'s reason borrowed for a ladder that does not need
# one.
#
# `r.rejected_rows IS NOT NULL` AND NOT `r.reject_reason IS NOT NULL` IS NOT presentational,
# and it is the one real decision in these two lines. The join's right side is a GROUP BY,
# so `COUNT(*)` is non-null for every group it produces and NULL only when it produced
# none. A reject reason CAN be NULL in the data, and then the reason test would call a real
# rejected group missing -- `test_a_rejected_group_whose_reason_is_null_is_still_rows_
# present` supplies exactly that group and goes red under the substitution.
_CENSUS_LADDER = (
    (ROWS_PRESENT, "r.rejected_rows IS NOT NULL"),
    (EVIDENCE_MISSING_QUARANTINE_EMPTY, "t.quarantine_table_rows = 0"),
)


def masked_columns(spec: BronzeTable) -> tuple[str, ...]:
    """The columns of `spec`'s contract this project declares personal data.

    In CONTRACT order rather than declaration order, so the two callers below iterate one
    sequence and a reordered declaration is not a diff in generated SQL."""
    declared = set(MASKED_COLUMNS.get(spec.contract, ()))
    return tuple(column for column in columns_for(spec.contract) if column in declared)


def profiled_columns(spec: BronzeTable) -> tuple[str, ...]:
    """Every column `row_shapes_sql` reports a state for: the contract, then `_rescued_data`.

    `_rescued_data` IS IN HERE AND THE OTHER SIX METADATA COLUMNS ARE NOT, which is not a
    tidy line: it is the evidence for `rescued_data_present`, the reason behind the single
    largest incident in this corpus (2,000 rows on `592660596679630`), so a profile without
    it would say nothing at all about that one. The rest -- `_ingested_at`, `_batch_id`,
    `_record_source`, `_snapshot_month`, `_snapshot_ref_date`, `_source_file` -- are the
    pipeline's own stamps, identical for every row of a batch, and `_batch_id` is the key
    the sample is already filtered on."""
    return (*columns_for(spec.contract), RESCUED_DATA_COLUMN)


def value_state_sql(column: str, *, masked: bool) -> str:
    """One column's state, as SQL. A masked column is a LITERAL and is never read.

    That is what makes `masked` unforgeable by data: the column does not appear in the
    expression at all, so no value -- `***` included -- can produce the word, and no
    reader's group membership can change it."""
    if masked:
        return f"'{MASKED}'"
    quoted = f"`{column}`"
    return (
        f"CASE WHEN {quoted} IS NULL THEN '{NULL_VALUE}' "
        f"WHEN TRIM({quoted}) = '' THEN '{EMPTY}' "
        f"WHEN INSTR({quoted}, '{REPLACEMENT_CHARACTER}') > 0 THEN '{REPLACEMENT_CHAR}' "
        f"ELSE '{PRESENT}' END"
    )


def census_case_sql() -> str:
    """The ladder as one CASE expression. Spelled once, here."""
    arms = "\n    ".join(f"WHEN {predicate} THEN '{name}'" for name, predicate in _CENSUS_LADDER)
    return f"CASE\n    {arms}\n    ELSE '{EVIDENCE_MISSING_BATCH_ABSENT}'\n  END"


def quarantine_census_sql(spec: BronzeTable, config: OplConfig = DEFAULT) -> str:
    """One row per reject reason for `:batch_id` -- AND ONE ROW WHEN THERE ARE NONE.

    The `LEFT JOIN ... ON true` is the whole mechanism and not a flourish.
    `quarantine_total` is an ungrouped `COUNT(*)`, so it has exactly one row on every input
    including an empty table; the grouped side has none when the batch is absent. The join
    therefore cannot return zero rows, and the case that used to be an empty result set --
    the one that cannot be told from "no query ran" -- arrives as a row whose `evidence`
    names which of the two removals it is.

    `rejected_rows` is COALESCE'd to 0 and the counts are honest either way: the verdict
    beside it is what says whether that 0 means "nothing was rejected" (impossible after a
    `fail_on_dq`) or "the evidence is gone" (the only remaining reading).

    `quarantine_table_rows` IS NAMED FOR ITS GRAIN, WHICH IS THE ONLY COLUMN HERE THAT IS
    NOT THE INCIDENT'S. Every other value on this row is about (table, batch, reason); that
    one is a whole-table `COUNT(*)`, repeated identically on every row the statement
    returns. It was called `quarantine_rows`, and on the socios pair that reads 3,583
    beside `rejected_rows` of 1,797 and 1,786 -- which is precisely the number
    `docs/f6-run-evidence.md` 0.3 flags as the fusion hazard, "two incidents three weeks
    apart, not one", emitted on the incident row for EACH of them. A consumer that rendered
    it as this incident's size would publish a 3,583-row incident twice. The name is now
    the only thing it can be read as."""
    quarantine = config.table(spec.quarantine)
    return (
        "WITH quarantine_total AS (\n"
        f"  SELECT COUNT(*) AS quarantine_table_rows FROM {quarantine}\n),\n"
        "by_reason AS (\n"
        f"  SELECT {REJECT_COLUMN} AS reject_reason, COUNT(*) AS rejected_rows\n"
        f"  FROM {quarantine} WHERE {BATCH_COLUMN} = :batch_id\n"
        f"  GROUP BY {REJECT_COLUMN}\n"
        ")\n"
        f"SELECT '{spec.name}' AS source, :batch_id AS batch_id,\n"
        "  r.reject_reason, COALESCE(r.rejected_rows, 0) AS rejected_rows,\n"
        "  t.quarantine_table_rows,\n"
        f"  {census_case_sql()} AS evidence\n"
        "FROM quarantine_total t LEFT JOIN by_reason r ON true\n"
        "ORDER BY COALESCE(r.rejected_rows, 0) DESC, r.reject_reason"
    )


def row_shapes_sql(
    spec: BronzeTable, config: OplConfig = DEFAULT, *, limit: int = SAMPLE_LIMIT
) -> str:
    """A bounded sample of quarantined rows, rendered as STATES. PUBLISHABLE.

    One row per sampled quarantine row, carrying its reject reason and a
    `MAP<STRING, STRING>` from column name to a word in `VALUE_STATES`. No column value is
    projected anywhere in this statement, which is a property of its OUTPUT and is asserted
    against the output rather than against this sentence -- see the header."""
    _require_a_bound(limit)
    masked = set(masked_columns(spec))
    states = ",\n    ".join(
        f"'{column}', {value_state_sql(column, masked=column in masked)}"
        for column in profiled_columns(spec)
    )
    return (
        "WITH sampled AS (\n"
        f"  SELECT * FROM {config.table(spec.quarantine)}\n"
        f"  WHERE {BATCH_COLUMN} = :batch_id LIMIT {limit}\n"
        ")\n"
        f"SELECT '{spec.name}' AS source, :batch_id AS batch_id,\n"
        f"  {REJECT_COLUMN} AS reject_reason,\n"
        f"  map(\n    {states}\n  ) AS value_states\n"
        "FROM sampled"
    )


def row_sample_sql(
    spec: BronzeTable, config: OplConfig = DEFAULT, *, limit: int = SAMPLE_LIMIT
) -> str:
    """The same bounded sample with REAL VALUES. **NOT PUBLISHABLE.**

    For a human triager reading it interactively under their own grants -- the four
    `encoding_replacement_char` rows are a lost byte, and no state word substitutes for
    seeing it. `evidence_sql` does not return this, so an issue renderer handed the
    assembler's output cannot reach a value through it.

    IT NEVER PROJECTS A DECLARED-PERSONAL COLUMN, so the worst this can carry is
    non-personal. `_rescued_data` is withheld for any contract that declares one: the
    rescued blob is UNPARSED SOURCE TEXT, so the same name can arrive inside it under
    another key, and a redaction that covered the named column and not the blob would be a
    control applied by column name rather than by what the column holds -- which is
    `masking.py`'s own reason for masking `nome_do_representante` as well."""
    _require_a_bound(limit)
    masked = set(masked_columns(spec))
    readable = [column for column in columns_for(spec.contract) if column not in masked]
    if not masked:
        readable.append(RESCUED_DATA_COLUMN)
    projected = ", ".join(
        f"`{column}`" for column in (REJECT_COLUMN, SOURCE_FILE_COLUMN, *readable)
    )
    return (
        f"SELECT '{spec.name}' AS source, :batch_id AS batch_id, {projected}\n"
        f"FROM {config.table(spec.quarantine)}\n"
        f"WHERE {BATCH_COLUMN} = :batch_id LIMIT {limit}"
    )


def reconciliation_sql(
    spec: BronzeTable, config: OplConfig = DEFAULT, *, view: str | None = None
) -> str:
    """`dataops_reconciliation`'s row for this batch -- OR the word for its absence.

    The anchor-and-`LEFT JOIN` is `quarantine_census_sql`'s mechanism for its reason: five
    of eleven incidents have no row here, and a query that returned nothing for them would
    be indistinguishable from a query that failed to run.

    `f.matched` AND NOT `f.verdict IS NULL` is the discriminator. `reconcile.verdict_case_
    sql` has an ELSE arm so its verdict cannot be NULL today, and keying the absence on
    that would make this statement's correctness depend on a property of another module's
    CASE ladder rather than on whether a row was found. `view=` is the seam that makes that
    difference observable without waiting for `reconcile.py` to change:
    `test_a_reconciliation_row_whose_verdict_is_null_is_still_a_row_that_was_found` hands
    this statement a relation whose verdict is NULL for a matched row, and the substitution
    turns it red.

    THE COUNTS ARE LEFT NULL. Coalescing them to 0 would say "nothing was staged", which is
    a claim about the batch; NULL says "this view has no row", which is a claim about the
    view -- and F4 already settled which of those a missing metric gets."""
    relation = view or config.table(BATCH_GRAIN_VIEW)
    return (
        "WITH asked AS (SELECT 1 AS anchor),\n"
        f"found AS (\n  SELECT *, TRUE AS matched FROM {relation}\n"
        f"  WHERE source = '{spec.name}' AND batch_id = :batch_id\n)\n"
        f"SELECT '{spec.name}' AS source, :batch_id AS batch_id,\n"
        "  f.staged, f.promoted, f.quarantined, f.unaccounted,\n"
        f"  CASE WHEN f.matched IS NULL THEN '{NO_RECONCILIATION_ROW}' ELSE f.verdict END"
        " AS verdict,\n"
        "  f.remedy\n"
        "FROM asked a LEFT JOIN found f ON true"
    )


def evidence_sql(
    source: str | None,
    config: OplConfig = DEFAULT,
    *,
    limit: int = SAMPLE_LIMIT,
    view: str | None = None,
) -> dict[str, str]:
    """The publishable evidence for one incident, keyed by `PUBLISHABLE`.

    `source` IS T1's COLUMN, TAKEN RATHER THAN RE-DERIVED. `incident_feed_sql` already
    resolves the registry key from the job name against a declaration the bundle is locked
    against, and both F4 views publish that same value under that same name.

    EVERY STATEMENT TAKES EXACTLY `args={"batch_id": ...}` and projects `source` as a
    literal from the resolved spec, so one binding serves all three and every result row
    says what it is about without the caller re-labelling it."""
    spec = _spec_of_incident(source)
    return {
        "census": quarantine_census_sql(spec, config),
        "row_shapes": row_shapes_sql(spec, config, limit=limit),
        "reconciliation": reconciliation_sql(spec, config, view=view),
    }


def _spec_of_incident(source: str | None) -> BronzeTable:
    """The registered table an incident is about, or refuse naming what went wrong.

    A NULL `source` IS A REAL ROW OF T1's FEED AND NOT A CALLER'S MISTAKE. `incidents.py`
    reports an incident on a job its declaration does not know with `source` NULL rather
    than dropping it, precisely so a rename that reached the workspace and not the
    repository stays visible. There is then no quarantine to census and no table to
    reconcile, and inventing an empty result for it would render a stale declaration
    exactly like a clean batch."""
    if source is None or not str(source).strip():
        raise UnknownTable(
            "this incident's `source` is empty, so no bronze table can be resolved and "
            "there is no quarantine to read. `incident_feed_sql` emits NULL there when a "
            "DQ gate fired on a job `TABLE_OF_JOB` does not declare -- a job renamed in "
            "the bundle and not in the repository. Fix the declaration; do not read this "
            "as an incident with no evidence"
        )
    return table_spec(str(source))


def _require_a_bound(limit: int) -> None:
    """A sample has to be bounded by a positive whole number, and this is interpolated."""
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError(
            f"sample limit {limit!r} is not a positive integer. This value is written "
            "straight into a LIMIT clause, and a sample that is not bounded is a dump"
        )


def _assert_every_masked_column_is_one_this_module_profiles() -> None:
    """The redaction is TOTAL over the declaration, refused at import.

    A declared column that no contract has masks nothing here -- `masked_columns` filters
    against the contract, so the entry is silently inert and the column a reader believes
    is redacted would be profiled by reading it. That is the failure this whole module is
    written against, so it fails at import rather than at triage time."""
    for contract, columns in sorted(MASKED_COLUMNS.items()):
        if not is_known(contract):
            raise ValueError(
                f"the mask declaration names contract {contract!r}, which no source "
                "declares, so this module can profile no column of it and the mask is "
                "invisible to triage"
            )
        stray = sorted(set(columns) - set(columns_for(contract)))
        if stray:
            raise ValueError(
                f"the mask declaration names {stray} on contract {contract!r}, which has "
                f"no such column ({', '.join(columns_for(contract))}). Those entries "
                "redact nothing here, so a column a reader believes is masked would be "
                "reported by reading its value"
            )


def _assert_the_absence_word_is_not_a_reconciliation_verdict() -> None:
    """`no_reconciliation_row` has to be distinguishable from every verdict it stands in for.

    The whole requirement on that column is that absence is reported AS absence -- not as
    `reconciled`, not as NULL. A rename in `reconcile.py` that collided with this word
    would break that silently: the column would keep emitting one string for two worlds,
    which is this repository's most-hunted species and the reason this file exists."""
    verdicts = (RECONCILED, STRANDED_GATED, STRANDED_UNEXPLAINED, OVER_PROMOTED)
    if NO_RECONCILIATION_ROW in verdicts:
        raise ValueError(
            f"{NO_RECONCILIATION_ROW!r} is also one of `reconcile.py`'s verdicts "
            f"({', '.join(verdicts)}), so a batch the reconciliation cannot speak for "
            "would be indistinguishable from one it judged"
        )


_assert_every_masked_column_is_one_this_module_profiles()
_assert_the_absence_word_is_not_a_reconciliation_verdict()
