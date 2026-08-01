# tests/bronze/test_rules.py
import inspect

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from opl.bronze.dq import REJECT_COLUMN, evaluate, split
from opl.bronze.registry import REGISTRY
from opl.bronze.rules import REQUIRED_FIELDS, REQUIRES_COLUMN, rules_for
from opl.bronze.snapshot import SNAPSHOT_REF_DATE_COLUMN
from opl.contracts.cnpj_schemas import TABLES

_REPLACEMENT_CHAR = "�"

# Placeholders that make a synthetic row PASS every rule, so a test only has to
# state the one field it is about. Spelled out rather than "X" everywhere because
# the CNPJ key rules are shape-sensitive: an 8-character cnpj_basico is what keeps
# `bad_cnpj_basico_length` quiet, and a test about `municipio` that silently
# tripped the length rule instead would assert the wrong thing while staying green.
_ROW_DEFAULTS = {"cnpj_basico": "12345678", "cnpj_ordem": "0001", "cnpj_dv": "95"}


def _row(contract: str, **overrides: str | None) -> tuple[str | None, ...]:
    """One all-string row for `contract`, in contract column order.

    Bronze is all-string, so every column of every contract takes a str or None
    and this needs no per-column types.

    The unknown-override refusal is the point of the helper, not bookkeeping. A
    test written as `_row("estabelecimentos", municipo="")` (typo) would otherwise
    build a perfectly CLEAN row and then assert a reject reason against it -- the
    assertion would fail for a reason that has nothing to do with the typo, or
    worse, pass because some other column happened to carry the defect. Refused
    here, the typo names itself."""
    values = {column: _ROW_DEFAULTS.get(column, "X") for column in TABLES[contract]}
    unknown = sorted(set(overrides) - set(values))
    if unknown:
        raise AssertionError(
            f"{unknown} is not a column of contract {contract!r} -- "
            f"columns are: {', '.join(TABLES[contract])}"
        )
    return tuple(overrides.get(column, values[column]) for column in TABLES[contract])


def test_unknown_table_raises():
    with pytest.raises(KeyError):
        rules_for("nope")


def test_every_registered_table_has_a_rule_set():
    """A registry entry with no rule set fails INSIDE the Spark job, at promote,
    after ingest has already written staging -- and it arrives as a bare KeyError,
    which is precisely what `registry.UnknownTable`'s docstring exists to avoid: a
    KeyError re-`repr`s its message into a Databricks run log, and is silently
    swallowable by an `except KeyError` several frames up that never named it.

    Nothing else ties these two modules together. `REGISTRY` says a table exists and
    `rules_for` says how its rows are judged, and the two are edited in different
    files by different steps of the same phase -- so registering a table and
    forgetting its rules is a one-commit gap that no other check in this repo can
    see.

    A test and not an import assertion, DELIBERATELY: `rules.py` imports pyspark and
    `registry.py` does not, and importing one from the other would drag a Spark
    dependency into the pure-Python registry tests. That is a real cost paid for a
    real reason -- this file already imports pyspark, so the tie is asserted here
    where it is free.

    Passes with today's two tables. It is the guard for the phase that adds
    `empresas` and `socios`, not a red test now; verified non-vacuous by adding a
    third entry with no rule set and watching it fail.

    The `except KeyError` is the point, not defensive noise. `rules_for` raises a
    BARE KeyError for a table it does not know, so letting it propagate would fail
    this test with `KeyError: 'empresas'` and none of the prose below -- reproducing
    the unhelpful failure this guard exists to replace, in the very test meant to
    replace it. Caught and turned into the assertion message instead."""
    for spec in REGISTRY.values():
        try:
            rule_set = rules_for(spec.contract)
        except KeyError:
            rule_set = None
        assert rule_set, (
            f"{spec.name} is registered but rules_for({spec.contract!r}) yields no "
            "rule set -- add it to opl.bronze.rules before registering the table"
        )


def test_lookup_rules_names_and_order():
    names = [name for name, _ in rules_for("lookup")]
    assert names == ["null_or_empty_codigo", "null_or_empty_descricao",
                     "encoding_replacement_char"]


def test_estabelecimentos_rules_evaluate(spark):
    """The full contract, not the five columns the rules used to name. F1.4b folded
    the U+FFFD check over every column, so `rules_for("estabelecimentos")` resolves
    all thirty and a five-column projection no longer analyses -- see
    `test_a_rule_set_refuses_a_frame_that_is_missing_a_contract_column` for why
    that is the intended coupling rather than a fixture inconvenience."""
    cols = TABLES["estabelecimentos"]
    df = spark.createDataFrame(
        [
            _row("estabelecimentos", nome_fantasia="PADARIA AÇAÍ"),      # valid
            _row("estabelecimentos", cnpj_basico=None),                  # null cnpj_basico
            _row("estabelecimentos", cnpj_basico="1234567"),             # 7 chars, bad length
            _row("estabelecimentos", cnpj_ordem=None),                   # null ordem
            _row("estabelecimentos", cnpj_dv=None),                      # null dv
            _row("estabelecimentos", cnpj_basico="87654321",
                 nome_fantasia="MOJI�BAKE"),                             # replacement char
        ],
        cols,
    )
    out = {(r.cnpj_basico, r.cnpj_ordem, r.cnpj_dv): r[REJECT_COLUMN]
           for r in evaluate(df, rules=rules_for("estabelecimentos")).collect()}
    assert out[("12345678", "0001", "95")] is None
    assert out[(None, "0001", "95")] == "null_or_empty_cnpj_basico"
    assert out[("1234567", "0001", "95")] == "bad_cnpj_basico_length"
    assert out[("12345678", None, "95")] == "null_or_empty_cnpj_ordem"
    assert out[("12345678", "0001", None)] == "null_or_empty_cnpj_dv"
    # The replacement char is the ONLY in-band evidence that a byte was lost:
    # Java's cp1252 decoder substitutes U+FFFD silently where Python raises
    # (ADR 0006). Without this assertion the rule could be deleted from the
    # estabelecimentos set with the suite still green.
    assert out[("87654321", "0001", "95")] == "encoding_replacement_char"


def test_rescued_data_outranks_the_estabelecimentos_rules(spark):
    """The universal rescued_data_present check sits above every per-table rule
    (dq._reject_reason), so a rescued row must report that and not the per-table
    reason it also violates -- rescued data means the parse itself is suspect, so
    a narrower reason would misdirect the triage."""
    # Explicit schema: _rescued_data would be all-null in the clean row and
    # type inference cannot determine an all-null column's type. The full
    # contract, plus _rescued_data, because the encoding check is contract-wide.
    schema = StructType([
        StructField(c, StringType())
        for c in (*TABLES["estabelecimentos"], "_rescued_data")
    ])
    df = spark.createDataFrame(
        [
            # Violates bad_cnpj_basico_length AND carries rescued data.
            (*_row("estabelecimentos", cnpj_basico="1234567"), '{"_c30":"31 extra"}'),
            (*_row("estabelecimentos"), None),
        ],
        schema,
    )
    out = {r.cnpj_basico: r[REJECT_COLUMN]
           for r in evaluate(df, rules=rules_for("estabelecimentos")).collect()}
    assert out["1234567"] == "rescued_data_present"
    assert out["12345678"] is None


def test_lookup_default_golden_unchanged(spark):
    """evaluate() with NO rules arg must behave exactly as F1.2 shipped."""
    # explicit schema: _rescued_data is all-null, so type inference cannot
    # determine it (PySparkValueError CANNOT_DETERMINE_TYPE) without this.
    schema = StructType([
        StructField("codigo", StringType()),
        StructField("descricao", StringType()),
        StructField("_rescued_data", StringType()),
    ])
    df = spark.createDataFrame(
        [("01", "AÇÃO", None), (None, "x", None), ("02", None, None)],
        schema,
    )
    out = {r.codigo: r[REJECT_COLUMN] for r in evaluate(df).collect()}
    assert out["01"] is None
    assert out[None] == "null_or_empty_codigo"
    assert out["02"] == "null_or_empty_descricao"
    good, bad = split(df)
    assert good.count() == 1 and bad.count() == 2


# --- F1.4b: declared required fields, and U+FFFD over every column -----------

# Every reject reason `rules_for` has ever written into a LIVE quarantine table,
# per contract. Reject reasons are DATA, not log text: they sit in
# bronze_cnpj_lookup_quarantine and bronze_cnpj_estab_quarantine, and a triager
# filtering on one should not have to know which release wrote the row. Generating
# the required-field names from the column list is only safe because it reproduces
# these strings byte-identically -- asserted below rather than eyeballed.
_REASONS_ALREADY_IN_LIVE_QUARANTINE = {
    "lookup": frozenset({
        "null_or_empty_codigo", "null_or_empty_descricao", "encoding_replacement_char",
    }),
    "estabelecimentos": frozenset({
        "null_or_empty_cnpj_basico", "null_or_empty_cnpj_ordem", "null_or_empty_cnpj_dv",
        "bad_cnpj_basico_length", "encoding_replacement_char",
    }),
}

# The estabelecimentos encoding check exactly as it stood before this change: two
# hand-picked columns out of thirty. Kept as a CONTROL, not as dead history -- the
# generalisation is only proved by a row this predicate lets through and the new
# one catches, and a claim about what the old code did is worth nothing unless the
# old code is present to be run.
_OLD_HAND_PICKED_ENCODING_CHECK = (
    "encoding_replacement_char",
    lambda: F.col("nome_fantasia").contains(_REPLACEMENT_CHAR)
    | F.col("logradouro").contains(_REPLACEMENT_CHAR),
)


def test_no_live_quarantine_reason_string_is_retired():
    """Generating the required-field reasons must not rename any of them.

    The whole justification for `f"null_or_empty_{column}"` over a collapsed
    `missing_required_field` is that it comes out byte-identical to what the
    lookup and estabelecimentos sets already produce. If that identity ever
    breaks, rows written before the break and rows written after describe the
    same defect with two different words, in the same table, and every saved
    triage filter silently starts matching half the rows it used to.

    A subset assertion and not an equality: a rule set is ALLOWED to grow new
    reasons (this change adds `null_or_empty_municipio`; Task 3 adds another).
    What it may not do is drop or rename one."""
    for contract, historical in _REASONS_ALREADY_IN_LIVE_QUARANTINE.items():
        produced = {name for name, _ in rules_for(contract)}
        assert historical <= produced, (
            f"rules_for({contract!r}) no longer produces "
            f"{sorted(historical - produced)}, which already exists as data in that "
            "table's quarantine -- renaming a reject reason splits the triage "
            "vocabulary across releases"
        )


def test_every_predicate_is_a_zero_arg_factory_and_not_a_column():
    """The invariant this module's docstring is built on, now that three different
    shapes produce predicates: a lambda (`_null_or_blank`), a closure
    (`_encoding_check`) and a plain module function (`_cnpj_basico_length`).

    An eager `Column` cannot be constructed without an active SparkContext, so a
    predicate that stopped being a factory would break `rules_for` for every
    pure-Python caller -- `dq_gate_batch`'s and `promote_batch`'s argument
    resolution, and every test in this file that inspects names without a session.

    Asserted STRUCTURALLY rather than by "call it and see": the Spark fixture is
    session-scoped and process-wide, so by the time any test runs, a session may
    already exist and the regression would hide. `isinstance(..., Column)` and an
    empty signature hold whether or not a JVM is up."""
    for contract in ("lookup", "estabelecimentos", "empresas", "socios"):
        for reason, predicate in rules_for(contract):
            assert not isinstance(predicate, Column), (
                f"{contract}/{reason} is an eager Column; rules_for must stay "
                "importable and callable with no SparkContext"
            )
            assert callable(predicate), f"{contract}/{reason} is not callable"
            parameters = inspect.signature(predicate).parameters
            assert not parameters, (
                f"{contract}/{reason} takes {list(parameters)}; dq._reject_reason "
                "calls every predicate with no arguments"
            )


def test_every_required_field_is_a_column_of_its_contract():
    """A typo in REQUIRED_FIELDS is invisible until Spark resolves the column.

    `_null_or_blank("municipo")` builds fine -- `F.col` of a nonexistent name is a
    perfectly good unresolved Column -- and only explodes as an AnalysisException
    inside the DQ gate task, which is to say after ingest has already written
    staging. Pure Python, so it costs a millisecond in CI and needs no session."""
    for contract, required in REQUIRED_FIELDS.items():
        assert contract in TABLES, (
            f"REQUIRED_FIELDS declares contract {contract!r}, which is not in "
            f"cnpj_schemas.TABLES ({', '.join(sorted(TABLES))})"
        )
        missing = [column for column in required if column not in TABLES[contract]]
        assert not missing, (
            f"REQUIRED_FIELDS[{contract!r}] names {missing}, which "
            f"{'are' if len(missing) > 1 else 'is'} not "
            f"{'columns' if len(missing) > 1 else 'a column'} of that contract -- "
            "the rule would only fail as an AnalysisException inside the gate task"
        )


def test_the_estabelecimentos_rule_order_is_pinned():
    """First-match-wins makes order part of the contract, not a detail. Grouping
    the required-field rules moved bad_cnpj_basico_length after them, which
    changes the reported reason for a row that trips both. Both reasons are true
    of such a row -- this pins the choice so it cannot drift silently."""
    assert [name for name, _ in rules_for("estabelecimentos")] == [
        "null_or_empty_cnpj_basico",
        "null_or_empty_cnpj_ordem",
        "null_or_empty_cnpj_dv",
        "null_or_empty_municipio",
        "bad_cnpj_basico_length",
        "encoding_replacement_char",
        "unprovable_snapshot_ref_date",
    ]


def test_the_empresas_rule_order_is_pinned():
    """Pinned for the same first-match-wins reason as estabelecimentos, and
    additionally because this set is what a later task registers the table
    against: the reasons named here are the ones its first quarantine will hold,
    so they are fixed before any row carries them rather than after."""
    assert [name for name, _ in rules_for("empresas")] == [
        "null_or_empty_cnpj_basico",
        "null_or_empty_razao_social",
        "null_or_empty_natureza_juridica",
        "bad_cnpj_basico_length",
        "encoding_replacement_char",
        "unprovable_snapshot_ref_date",
    ]


def test_the_socios_rule_order_is_pinned():
    """See test_the_empresas_rule_order_is_pinned. `cpf_cnpj_socio` is deliberately
    NOT required: it is blank for the foreign shareholders that
    `nome_cidade_exterior`/`pais` exist to describe, so requiring it would reject
    a legitimate population wholesale on an all-or-nothing gate."""
    assert [name for name, _ in rules_for("socios")] == [
        "null_or_empty_cnpj_basico",
        "null_or_empty_identificador_socio",
        "null_or_empty_nome_socio_razao_social",
        "null_or_empty_qualificacao_socio",
        "bad_cnpj_basico_length",
        "encoding_replacement_char",
        "unprovable_snapshot_ref_date",
    ]


def test_a_row_missing_a_required_tail_column_is_rejected(spark):
    """The F1.3 parse defect's signature: a truncated record loses its LAST
    columns, so the last column that is never legitimately empty is what catches
    it. Not a generic trailing-NULL check -- empresas' real last column
    (ente_federativo_responsavel) is empty for every private company."""
    df = spark.createDataFrame(
        [_row("empresas", cnpj_basico="11111111", ente_federativo_responsavel=""),
         _row("empresas", cnpj_basico="22222222", razao_social=None,
              ente_federativo_responsavel="")],
        TABLES["empresas"],
    )
    reasons = {r["cnpj_basico"]: r[REJECT_COLUMN]
               for r in evaluate(df, rules=rules_for("empresas")).collect()}
    assert reasons["11111111"] is None, "an empty ente_federativo is legitimate"
    assert reasons["22222222"] == "null_or_empty_razao_social"


def test_the_replacement_char_is_checked_on_every_string_column(spark):
    """Carry-forward #5. The estabelecimentos set checked two hand-picked columns;
    a mojibake in any other one passed the gate silently."""
    df = spark.createDataFrame(
        [("11111111", "ACME LTDA", "2062", "49", "1000,00", "05",
          f"PREFEITURA DE S{_REPLACEMENT_CHAR}O")],
        TABLES["empresas"],
    )
    reasons = [r[REJECT_COLUMN]
               for r in evaluate(df, rules=rules_for("empresas")).collect()]
    assert reasons == ["encoding_replacement_char"]


def test_the_encoding_check_covers_a_column_the_hand_picked_pair_looked_away_from(spark):
    """The probe that actually closes carry-forward #5, on the table it was raised
    against.

    The empresas test above cannot close it: empresas had no rule set at all
    before this change, so "every column is covered" is unfalsifiable there --
    ANY implementation that looked at the one column the test dirties would pass.
    The guard is only closed by a row the OLD code demonstrably accepted.

    `bairro` is that row. It is column 17 of estabelecimentos' 30, it is neither
    `nome_fantasia` nor `logradouro`, and ADR 0006 records why this matters
    concretely: one record in Estabelecimentos8 carries a byte (0x8f) that
    windows-1252 cannot decode, Java's decoder substitutes U+FFFD for it
    SILENTLY, and *which column holds it is not known*. A two-column check
    against an unknown column is a coin flip.

    The old predicate is run here rather than described, so the "would have
    passed" half is measured and not asserted from memory. The last block runs
    the two columns it DID cover, because a generalisation that dropped them
    would satisfy every other assertion in this file."""
    columns = TABLES["estabelecimentos"]
    assert "bairro" in columns and "bairro" not in ("nome_fantasia", "logradouro")

    df = spark.createDataFrame(
        [_row("estabelecimentos", bairro=f"CENTR{_REPLACEMENT_CHAR}")], columns,
    )
    new = [r[REJECT_COLUMN]
           for r in evaluate(df, rules=rules_for("estabelecimentos")).collect()]
    assert new == ["encoding_replacement_char"]

    old = [r[REJECT_COLUMN]
           for r in evaluate(df, rules=[_OLD_HAND_PICKED_ENCODING_CHECK]).collect()]
    assert old == [None], (
        "the control is not a control: the pre-change two-column check already "
        "caught this row, so it proves nothing about the generalisation"
    )

    for covered in ("nome_fantasia", "logradouro"):
        still = spark.createDataFrame(
            [_row("estabelecimentos", **{covered: f"MOJI{_REPLACEMENT_CHAR}BAKE"})],
            columns,
        )
        assert [r[REJECT_COLUMN] for r in
                evaluate(still, rules=rules_for("estabelecimentos")).collect()] == [
            "encoding_replacement_char"
        ], f"the generalised check stopped covering {covered}, which it used to"


def test_the_lookup_encoding_check_now_covers_codigo_too(spark):
    """The SECOND live-table gate change this commit makes, smaller than
    `municipio` but just as real and much easier to miss.

    The lookup set's encoding check was `descricao` only; folding it over the
    contract adds `codigo`. A lookup row carrying U+FFFD in `codigo` and nothing
    else used to be ACCEPTED and is now rejected -- a new way for a lookup ingest
    to go red, on a table that has already run in production. Unlikely (codigo is
    a short RFB code, not free text) and right when it happens: a replacement
    character in a JOIN KEY is worse than one in a label, because it fails to
    match silently instead of merely displaying wrong.

    Neither `test_lookup_rules_names_and_order` nor
    `test_lookup_default_golden_unchanged` can see this. The reason NAME did not
    change and no row in the golden fixture carries the character -- only what the
    rule covers changed, which is invisible to any test that does not dirty the
    newly covered column."""
    df = spark.createDataFrame(
        [(f"0{_REPLACEMENT_CHAR}1", "ACAO"), ("02", "ACAO")], TABLES["lookup"],
    )
    out = {r.codigo: r[REJECT_COLUMN]
           for r in evaluate(df, rules=rules_for("lookup")).collect()}
    assert out[f"0{_REPLACEMENT_CHAR}1"] == "encoding_replacement_char"
    assert out["02"] is None


def test_estabelecimentos_gains_the_municipio_requirement(spark):
    """`municipio` is the required column that reaches PAST a truncation point.

    The three keys already checked sit at contract indices 0-2, so a record
    truncated anywhere after the third field keeps all three and passes -- which
    is exactly how both F1.3 parse defects got through. `municipio` is index 20 of
    30.

    Safe to add to a LIVE table only because it is measured: over all 71,874,448
    rows of workspace.default.bronze_cnpj_estabelecimentos the blank count for
    municipio is 0, so this rejects nothing that exists today. The gate is
    all-or-nothing, so that measurement is a precondition, not a nicety.

    The second row pins the first-match-wins consequence of the reorder: a row
    that is BOTH short in cnpj_basico and blank in municipio used to report
    `bad_cnpj_basico_length` and now reports `null_or_empty_municipio`. Both are
    true of it and it is rejected either way -- reporting change, not gate
    change."""
    columns = TABLES["estabelecimentos"]
    df = spark.createDataFrame(
        [
            _row("estabelecimentos", cnpj_basico="12345678", municipio=""),
            _row("estabelecimentos", cnpj_basico="1234567", municipio="   "),
            _row("estabelecimentos", cnpj_basico="87654321", municipio=None),
            _row("estabelecimentos", cnpj_basico="11111111", municipio="7107"),
        ],
        columns,
    )
    out = {r.cnpj_basico: r[REJECT_COLUMN]
           for r in evaluate(df, rules=rules_for("estabelecimentos")).collect()}
    assert out["12345678"] == "null_or_empty_municipio"
    assert out["1234567"] == "null_or_empty_municipio"
    assert out["87654321"] == "null_or_empty_municipio"
    assert out["11111111"] is None


def test_a_rule_set_refuses_a_frame_that_is_missing_a_contract_column(spark):
    """The coupling this change introduces, STATED rather than left to be
    rediscovered by whoever writes the next fixture.

    Folding the encoding check over the whole contract means `rules_for(contract)`
    can only be applied to a frame that CARRIES the whole contract: a projection
    that used to analyse fine is now an AnalysisException. That is the choice, not
    a side effect. The alternative -- intersecting the check with whatever columns
    the frame happens to have -- reads as robustness and is the strictly worse
    failure: a staging table missing a contract column is a broken ingest, and a
    gate that quietly narrowed itself to the columns it could find would report a
    CLEAN run over data it never looked at. Loud beats lenient at a quality gate,
    and production always has the full contract anyway -- ingest writes staging
    from `schema.struct_for(contract)`.

    Two fixtures in this file and one in test_promote.py had to widen for exactly
    this reason. Pinned here so the next one fails against a test that explains
    itself instead of against a raw UNRESOLVED_COLUMN several frames deep."""
    projection = spark.createDataFrame(
        [("12345678", "0001", "95")], ["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
    )
    with pytest.raises(AnalysisException, match="municipio"):
        evaluate(projection, rules=rules_for("estabelecimentos"))


def test_socios_rules_evaluate(spark):
    """The socios set end to end, including a mojibake in `nome_do_representante`
    -- a column no hand-picked pair would have chosen, and the one that carries
    free-text human names for the legal representative of a foreign or minor
    shareholder, i.e. precisely where accented cp1252 bytes live."""
    columns = TABLES["socios"]
    df = spark.createDataFrame(
        [
            _row("socios", cnpj_basico="12345678"),
            _row("socios", cnpj_basico="22222222", nome_socio_razao_social=None),
            _row("socios", cnpj_basico="33333333", qualificacao_socio="  "),
            _row("socios", cnpj_basico="4444444"),
            _row("socios", cnpj_basico="55555555",
                 nome_do_representante=f"JO{_REPLACEMENT_CHAR}O"),
            # cpf_cnpj_socio is blank for foreign shareholders and is NOT required.
            _row("socios", cnpj_basico="66666666", cpf_cnpj_socio=""),
        ],
        columns,
    )
    out = {r.cnpj_basico: r[REJECT_COLUMN]
           for r in evaluate(df, rules=rules_for("socios")).collect()}
    assert out["12345678"] is None
    assert out["22222222"] == "null_or_empty_nome_socio_razao_social"
    assert out["33333333"] == "null_or_empty_qualificacao_socio"
    assert out["4444444"] == "bad_cnpj_basico_length"
    assert out["55555555"] == "encoding_replacement_char"
    assert out["66666666"] is None


# --- F1.4b Task 3: the NULL snapshot.ref_date_column produces on purpose ------

# A clean empresas row in contract order, spelled once. Every test below varies
# only `_snapshot_ref_date`, so a literal per test would put six copies of the
# same seven fields in the file and invite one of them to drift into tripping an
# earlier rule -- which would make the test green for the wrong reason, since
# first-match-wins means an earlier reason HIDES the one under test.
#
# Built by `_row` rather than as a positional tuple, like every other fixture in
# this file: a contract that gained a column would break a 7-tuple with an arity
# error naming neither the contract nor the column, where `_row` re-derives the
# width and refuses a misspelled override by name.
_CLEAN_EMPRESAS_ROW = _row("empresas", cnpj_basico="11111111",
                           ente_federativo_responsavel="")
_SHORT_EMPRESAS_ROW = _row("empresas", cnpj_basico="1234567",
                           ente_federativo_responsavel="")


def test_a_row_whose_ref_date_could_not_be_proven_is_rejected(spark):
    """The NULL that snapshot.ref_date_column produces on purpose, finally read by
    something. A month whose filenames change shape yields an all-NULL column, and
    until now nothing looked."""
    df = (spark.createDataFrame([_CLEAN_EMPRESAS_ROW], TABLES["empresas"])
          .withColumn(SNAPSHOT_REF_DATE_COLUMN, F.lit(None).cast("date")))
    reasons = [r[REJECT_COLUMN]
               for r in evaluate(df, rules=rules_for("empresas")).collect()]
    assert reasons == ["unprovable_snapshot_ref_date"]


def test_the_ref_date_rule_is_skipped_when_the_column_is_absent(spark):
    """rules_for is used on a plain contract DataFrame in unit tests and on the
    ingested frame in the job. The rule must not explode on the former."""
    df = spark.createDataFrame([_CLEAN_EMPRESAS_ROW], TABLES["empresas"])
    assert [r[REJECT_COLUMN]
            for r in evaluate(df, rules=rules_for("empresas")).collect()] == [None]


def test_a_proven_ref_date_is_not_rejected(spark):
    """The other half of the rule, and NOT redundant with the two above.

    Between them those two are satisfied by a rule that rejects every row
    carrying the column at all -- the reject test dirties the column to NULL and
    the skip test removes it entirely, so neither ever presents a column that is
    PRESENT and VALID. That is the shape production is in for every month whose
    filenames parse, i.e. the shape a false positive would turn every run red on.

    2026-06-13 is not an arbitrary date: it is what `D60613` decodes to, the token
    the June files actually shipped (snapshot.py's docstring)."""
    df = (spark.createDataFrame([_CLEAN_EMPRESAS_ROW], TABLES["empresas"])
          .withColumn(SNAPSHOT_REF_DATE_COLUMN, F.lit("2026-06-13").cast("date")))
    assert [r[REJECT_COLUMN]
            for r in evaluate(df, rules=rules_for("empresas")).collect()] == [None]


def test_an_earlier_defect_outranks_the_unprovable_ref_date(spark):
    """Why the rule goes LAST, pinned rather than left to the order test alone.

    `unprovable_snapshot_ref_date` is a property of the FILE the row arrived in,
    not of the row: when the filename format changes, EVERY row of the batch
    carries it. Placed anywhere but last it would become the reason printed for
    the whole quarantine, burying the per-row defects (a truncated record, a lost
    byte) that a triager can actually act on. The order test pins the list; this
    pins what the list is FOR, on a row that trips both rules at once.

    The reverse direction matters too and is asserted below it: a row with a
    proven date and a real defect must still report the defect. A rule appended
    last cannot shadow anything, but it CAN be miswired into the chain -- an
    `.otherwise` in the wrong place would swallow the earlier reasons."""
    df = (spark.createDataFrame(
              [_SHORT_EMPRESAS_ROW,
               _row("empresas", cnpj_basico="22222222", razao_social=None,
                    ente_federativo_responsavel="")],
              TABLES["empresas"])
          .withColumn(SNAPSHOT_REF_DATE_COLUMN, F.lit(None).cast("date")))
    out = {r["cnpj_basico"]: r[REJECT_COLUMN]
           for r in evaluate(df, rules=rules_for("empresas")).collect()}
    assert out["1234567"] == "bad_cnpj_basico_length"
    assert out["22222222"] == "null_or_empty_razao_social"

    proven = (spark.createDataFrame([_SHORT_EMPRESAS_ROW], TABLES["empresas"])
              .withColumn(SNAPSHOT_REF_DATE_COLUMN, F.lit("2026-06-13").cast("date")))
    assert [r[REJECT_COLUMN]
            for r in evaluate(proven, rules=rules_for("empresas")).collect()] == [
        "bad_cnpj_basico_length"
    ]


def test_a_rule_naming_a_column_no_frame_has_still_fails_loudly(spark):
    """THE PROBE THAT CLOSES THE SKIP PATH. Without it, `_reject_reason`'s new
    `continue` is a hole with a test suite that cannot see into it.

    The skip is safe only because it is keyed on a DECLARED column name. The
    tempting implementation -- wrapping `predicate()` in
    `except AnalysisException: continue` -- passes every other test in this file
    and is a different thing: it also skips a rule whose column name is a TYPO,
    turning a broken rule into one that never fires, on a gate whose entire job
    is to fire. Both halves below are green under the declared lookup and red
    under the blanket except.

    First half: an undeclared reason naming a column no frame has must raise.
    Second half is the sharper one -- the reason IS in REQUIRES_COLUMN and the
    declared column IS present, but the predicate reads a typo of it. The lookup
    finds the declared column, does not skip, and Spark refuses. A blanket except
    would have called that rule "not applicable" and returned a clean frame."""
    df = (spark.createDataFrame([_CLEAN_EMPRESAS_ROW], TABLES["empresas"])
          .withColumn(SNAPSHOT_REF_DATE_COLUMN, F.lit(None).cast("date")))

    undeclared = [("typo_in_a_new_rule", lambda: F.col("_no_such_column").isNull())]
    with pytest.raises(AnalysisException, match="_no_such_column"):
        evaluate(df, rules=undeclared).collect()

    assert "unprovable_snapshot_ref_date" in REQUIRES_COLUMN
    typoed = [("unprovable_snapshot_ref_date",
               lambda: F.col("_snapshot_ref_dat").isNull())]
    with pytest.raises(AnalysisException, match="_snapshot_ref_dat"):
        evaluate(df, rules=typoed).collect()


def test_every_requires_column_entry_is_declared_against_a_real_rule():
    """A typo on either side of REQUIRES_COLUMN, caught in pure Python.

    A typo in the KEY silently un-declares the requirement: the reason is spelled
    one way in the rule set and another here, `.get` returns None, and the rule
    stops being skippable -- so `rules_for("empresas")` on a bare contract frame
    goes back to raising UNRESOLVED_COLUMN, in the DQ gate task, after ingest has
    written staging.

    A typo in the VALUE is worse and is the one this file cares most about: if a
    declared column were ever a CONTRACT column, a frame MISSING that contract
    column would skip the rule instead of failing, which is precisely the lenient
    behaviour `test_a_rule_set_refuses_a_frame_that_is_missing_a_contract_column`
    exists to forbid. The skip is only defensible for METADATA columns, whose
    absence means "this frame predates the derivation" rather than "this ingest
    is broken" -- so that is asserted, not assumed."""
    # REQUIRED_FIELDS over TABLES: every rule set is built from
    # `_required_rules(contract)`, so its keys are exactly the contracts that HAVE
    # a rule set. `TABLES` also holds `simples`, which has none yet -- sweeping it
    # would fail this guard with a bare KeyError about an unrelated contract.
    reasons = {name for contract in REQUIRED_FIELDS for name, _ in rules_for(contract)}
    for reason, column in REQUIRES_COLUMN.items():
        assert reason in reasons, (
            f"REQUIRES_COLUMN declares {reason!r}, which no rule set produces -- "
            "the declaration is inert and the rule it was meant to guard will "
            "raise UNRESOLVED_COLUMN on any frame without its column"
        )
        holders = [c for c in TABLES if column in TABLES[c]]
        assert not holders, (
            f"REQUIRES_COLUMN[{reason!r}] names {column!r}, a column of contract(s) "
            f"{holders}. Only metadata columns may be declared skippable: a frame "
            "missing a CONTRACT column is a broken ingest and must fail loudly, "
            "not be quietly narrowed around"
        )
