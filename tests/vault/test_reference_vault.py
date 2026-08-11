# tests/vault/test_reference_vault.py
"""`opl.vault.reference` over a fixture shaped like real `bronze_cnpj_lookup` --
and the collision this task exists to prevent.

THE TRAP, IN MINIATURE. `bronze_cnpj_lookup` is six reference types in one table,
told apart only by `_source_file`, and `codigo` collides ACROSS types wherever two
types share a code width: measured (`01f192c7-9820-18be-ba93-5167bf5e1ede`),
`codigo='05'` names both a motivo and a qualificação de sócio, because MOTI and
QUALS are both two characters wide. `R_MOTI_05`/`R_QUALS_05` below are that
collision in miniature, and `R_MUNIC_1200`/`R_NATJU_1200` are the other shared
width (MUNIC/NATJU, four characters). A loader that grouped this table on `codigo`
alone -- the mistake this whole task exists to refuse -- would merge each pair into
one row; `test_two_colliding_codes_land_in_separate_tables_with_their_own_
descriptions_intact` is the test that fails if it does.

WHAT THIS FILE DOES NOT AND CANNOT COVER. Real `bronze_cnpj_lookup` holds ONE month,
2026-06 -- the 2026-07 lookup zips were never published in that month's set -- so
there is no second REAL observation to mirror the way `test_cnpj_vault.py` mirrors a
measured `razao_social` change. `test_a_later_months_changed_description_is_not_
reflected` uses a SYNTHETIC second month to prove the mechanism (insert-only, no
update), exactly as `test_cnpj_vault.py`'s `C_DEPARTED` row is synthetic for the
branch empresas cannot exercise -- and says so, rather than being read as a
measurement this task made."""
from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from opl.vault.columns import LOAD_DATE, RECORD_SOURCE
from opl.vault.domains.cnpj import (
    REF_CNAE,
    REF_MOTIVO,
    REF_MUNICIPIO,
    REF_NATUREZA_JURIDICA,
    REF_PAIS,
    REF_QUALIFICACAO,
)
from opl.vault.reference import load_reference_table

from .conftest import (
    JUL,
    JUN,
    LOADED_AT,
    RECORD_SOURCE_VALUE,
    RELOADED_AT,
    audit_values,
    bronze_schema,
    quarantine_schema,
    write_delta,
)

_LOOKUP_SCHEMA = bronze_schema("lookup")

# The six RFB filename suffixes `lookup_type_from_filename` routes on -- see
# `opl.bronze.lookup_routing.LOOKUP_SUFFIX`. Spelled here only as the SUFFIX half of
# a filename this fixture builds; the SUFFIX -> TYPE mapping itself is never
# restated, only exercised through the loader's own call to that function.
_SUFFIXES = {
    "cnae": "CNAE", "motivo": "MOTI", "municipio": "MUNIC",
    "natureza_juridica": "NATJU", "pais": "PAIS", "qualificacao": "QUALS",
}


def _source_file(month: str, ref_type: str) -> str:
    suffix = _SUFFIXES[ref_type]
    return f"/Volumes/x/cnpj/{month}/lookups/F.K03200$Z.D60613.{suffix}CSV"


def lookup_row(
    codigo: str, descricao: str, ref_type: str, month: str, *,
    record_source: str = RECORD_SOURCE_VALUE,
) -> tuple:
    """One bronze lookup row: `codigo`, `descricao`, then every audit column the
    ingest stamps -- `_source_file` is what this whole module routes on."""
    return (codigo, descricao) + audit_values(
        month, source_file=_source_file(month, ref_type), record_source=record_source,
    )


# The two measured collision pairs, in miniature (see the module docstring).
R_MOTI_05 = ("05", "MOTIVO CINCO", "motivo")
R_QUALS_05 = ("05", "QUALIFICACAO CINCO", "qualificacao")
R_MUNIC_1200 = ("1200", "MUNICIPIO MIL E DUZENTOS", "municipio")
R_NATJU_1200 = ("1200", "NATUREZA MIL E DUZENTOS", "natureza_juridica")

# One more, non-colliding row per type, so a type's table is not JUST the collision
# code -- and so país (the sixth type the task brief's own list omits) is exercised
# too.
R_CNAE = ("0111301", "CULTIVO DE ARROZ", "cnae")
R_PAIS = ("232", "ESTADOS UNIDOS", "pais")
R_MOTI_OTHER = ("00", "SEM MOTIVO", "motivo")
R_QUALS_OTHER = ("49", "SOCIO-ADMINISTRADOR", "qualificacao")

# The dedup case: one CNAE codigo, two June rows, two different (descricao,
# record_source) pairs -- codigo is measured unique within a type today, so this is
# a MECHANISM probe, not a mirror of anything real. THE LOWER record_source WINS.
D_CODIGO = "9000001"
D_LOSING_SOURCE = RECORD_SOURCE_VALUE + "_zzz_reingest"


def _fixture_rows() -> list[tuple]:
    rows = [
        lookup_row(*R_CNAE, JUN),
        lookup_row(*R_PAIS, JUN),
        lookup_row(*R_MOTI_05, JUN),
        lookup_row(*R_QUALS_05, JUN),
        lookup_row(*R_MOTI_OTHER, JUN),
        lookup_row(*R_QUALS_OTHER, JUN),
        lookup_row(*R_MUNIC_1200, JUN),
        lookup_row(*R_NATJU_1200, JUN),
        # The dedup pair: same codigo and type, same month, two source rows.
        #
        # THE DESCRIPTIONS ARE ORDERED AGAINST THE RECORD SOURCES ON PURPOSE. The
        # tie-break is `min` over the STRUCT (month, record_source, descricao), so
        # `record_source` decides before `descricao` is ever compared. Give the losing
        # source the alphabetically LATER description and both orderings agree, and the
        # test cannot tell which field decided. `D_LOSING_SOURCE` therefore carries
        # 'ARROZ A': a struct comparison keeps 'ARROZ B', and a hypothetical
        # descricao-first one would keep 'ARROZ A'.
        lookup_row(D_CODIGO, "ARROZ A", "cnae", JUN, record_source=D_LOSING_SOURCE),
        lookup_row(D_CODIGO, "ARROZ B", "cnae", JUN),
    ]
    return rows


@pytest.fixture(scope="module")
def lookup_source(spark, vault_database):
    """A throwaway Delta database holding one bronze `lookup` table shaped like
    `bronze_cnpj_lookup`, and an empty quarantine beside it (no fixture test here
    needs a reject; `reference_candidates` never reads the quarantine, unlike a
    satellite's ledger -- reference tables have no absence state to report, see the
    module docstring)."""
    db = vault_database("lookup_vault")
    bronze, quarantine = f"{db}.lookup", f"{db}.lookup_q"
    write_delta(spark, bronze, _LOOKUP_SCHEMA, _fixture_rows())
    write_delta(spark, quarantine, quarantine_schema("lookup"), [])
    return SimpleNamespace(db=db, bronze=bronze, quarantine=quarantine)


def _target(db: str) -> str:
    return f"{db}.ref_{uuid4().hex[:8]}"


def _load(spark, source, ref, *, load_date=LOADED_AT, months=None, target=None):
    target = target or _target(source.db)
    result = load_reference_table(
        spark, ref, source_table=source.bronze, target_table=target,
        load_date=load_date, months=months,
    )
    return target, result


def _rows(spark, table: str) -> dict[str, str]:
    return {
        row["codigo"]: row["descricao"]
        for row in spark.read.table(table).select("codigo", "descricao").collect()
    }


def _full_rows(spark, table: str) -> list[tuple]:
    """Every row's (`codigo`, `descricao`, `load_date`, `record_source`), sorted.

    UNLIKE `_rows`, THIS DOES NOT COLLAPSE A DUPLICATE `codigo` INTO ONE DICT ENTRY --
    a broken anti-join leaving two rows for one code is an extra tuple here, not a
    coin-flip winner a dict comprehension would silently pick for you."""
    return sorted(
        (row["codigo"], row["descricao"], row[LOAD_DATE], row[RECORD_SOURCE])
        for row in spark.read.table(table)
        .select("codigo", "descricao", LOAD_DATE, RECORD_SOURCE)
        .collect()
    )


# --------------------------------------------------------------------------- #
# THE TRAP: two types sharing a codigo must not merge.
# --------------------------------------------------------------------------- #

def test_two_colliding_codes_land_in_separate_tables_with_their_own_descriptions_intact(
    spark, lookup_source
):
    """`R_MOTI_05` and `R_QUALS_05` share `codigo='05'` and nothing else. A loader
    that grouped on `codigo` alone -- across types -- would produce ONE row for
    `'05'` in EITHER table, with one of the two descriptions silently gone. This
    fails on that mistake and passes only when both tables carry their own,
    disjoint two rows."""
    motivo_table, _ = _load(spark, lookup_source, REF_MOTIVO)
    qualificacao_table, _ = _load(spark, lookup_source, REF_QUALIFICACAO)

    motivo_rows = _rows(spark, motivo_table)
    qualificacao_rows = _rows(spark, qualificacao_table)

    assert motivo_rows == {"05": "MOTIVO CINCO", "00": "SEM MOTIVO"}
    assert qualificacao_rows == {"05": "QUALIFICACAO CINCO", "49": "SOCIO-ADMINISTRADOR"}
    # THE ASSERTION THAT ACTUALLY CATCHES A MERGE: each table's own code is present
    # with ITS OWN description, and the OTHER type's row for the same code is
    # nowhere in it -- not merged, not overwritten, not present at all.
    assert motivo_rows["05"] != qualificacao_rows["05"]


def test_the_other_shared_width_pair_also_stays_separate(spark, lookup_source):
    """The four-character pair (município/natureza jurídica), so the collision test
    above is not read as a fact about two-character codes specifically."""
    municipio_table, _ = _load(spark, lookup_source, REF_MUNICIPIO)
    natureza_table, _ = _load(spark, lookup_source, REF_NATUREZA_JURIDICA)

    assert _rows(spark, municipio_table) == {"1200": "MUNICIPIO MIL E DUZENTOS"}
    assert _rows(spark, natureza_table) == {"1200": "NATUREZA MIL E DUZENTOS"}


def test_cnae_and_pais_load_only_their_own_rows(spark, lookup_source):
    """THE PAÍS DECISION, exercised rather than only declared: país loads through
    the identical loader as every other type, and picks up none of the other five
    types' rows -- including the ones sharing its own row SHAPE (`codigo`,
    `descricao`) but a different `_source_file`."""
    cnae_table, _ = _load(spark, lookup_source, REF_CNAE)
    pais_table, _ = _load(spark, lookup_source, REF_PAIS)

    assert _rows(spark, pais_table) == {"232": "ESTADOS UNIDOS"}
    assert "0111301" in _rows(spark, cnae_table)
    assert "232" not in _rows(spark, cnae_table)


# --------------------------------------------------------------------------- #
# A naming change is a loud failure, not a silently dropped type.
# --------------------------------------------------------------------------- #

def test_a_source_file_lookup_type_from_filename_cannot_classify_raises_before_any_write(
    spark, vault_database
):
    """A lookup CSV whose suffix is not one of the six routes to nothing this
    repository recognises. `lookup_type_from_filename` raises on it, and that raise
    happens before any candidate is built, for ANY reference table's load, rather
    than the row being silently excluded from every type's filter.

    THROUGH `load_reference_table`, THE WRITER, NOT ONLY `reference_candidates`:
    the earlier version of this test called the read-only helper directly, so "before
    any write" was true only because `load_reference_table` happens to call
    `_collapsed_duplicates` and `reference_candidates` before the append -- a fact
    nothing pinned. Asserting through the writer, plus that the target table was
    never created, is what actually closes the claim the test's name makes."""
    db = vault_database("lookup_naming_probe")
    bronze = f"{db}.lookup"
    target = f"{db}.ref_probe"
    write_delta(spark, bronze, _LOOKUP_SCHEMA, [
        ("99", "MISTERIOSO", *audit_values(
            JUN, source_file=f"/Volumes/x/cnpj/{JUN}/lookups/F.K99999$Z.D60613.XPTOCSV",
        )),
    ])

    with pytest.raises(ValueError, match="unknown lookup suffix"):
        load_reference_table(
            spark, REF_CNAE, source_table=bronze, target_table=target, load_date=LOADED_AT,
        )

    assert not spark.catalog.tableExists(target)


# --------------------------------------------------------------------------- #
# Single month: insert-only, no update, proven by a synthetic second month.
# --------------------------------------------------------------------------- #

def test_a_later_months_changed_description_is_not_reflected(spark, lookup_source):
    """A SYNTHETIC second month -- real `bronze_cnpj_lookup` has none, per the
    module docstring. Proves the loader's actual, tested behaviour: a `descricao`
    revised in a later snapshot for a `codigo` already loaded is NOT picked up,
    because the anti-join drops the candidate on `codigo` alone. This is the
    mechanism `opl.vault.reference`'s docstring states as a limitation; this test
    is what makes that statement checked rather than asserted.

    ROW COUNT FIRST, THEN VALUE: `_rows` collapses a duplicate `codigo` into one
    dict entry, so a broken anti-join that left BOTH June's and July's row for
    `'0111301'` in the target would still make the value assertion pass or fail on
    a coin flip depending on collection order. Asserting exactly one row closes that."""
    db = lookup_source.db
    revised_bronze = f"{db}.lookup_revised_{uuid4().hex[:8]}"
    write_delta(spark, revised_bronze, _LOOKUP_SCHEMA, [
        lookup_row("0111301", "CULTIVO DE ARROZ", "cnae", JUN),
        lookup_row("0111301", "CULTIVO DE ARROZ (REVISADO)", "cnae", JUL),
    ])
    target = _target(db)

    load_reference_table(
        spark, REF_CNAE, source_table=revised_bronze, target_table=target,
        load_date=LOADED_AT, months=[JUN],
    )
    second = load_reference_table(
        spark, REF_CNAE, source_table=revised_bronze, target_table=target,
        load_date=RELOADED_AT, months=[JUN, JUL],
    )

    assert spark.read.table(target).count() == 1
    assert _rows(spark, target)["0111301"] == "CULTIVO DE ARROZ"
    # THE MULTI-MONTH WINDOW THAT PINS THE I1 FIX: '0111301' recurs in BOTH June
    # and July here, which is the ordinary shape of a republished reference list,
    # not a same-month source duplicate -- `collapsed_duplicates` must read 0, not
    # 1. Projecting `codigo` alone (the pre-fix shape) would have reported 1.
    assert second.collapsed_duplicates == 0


def test_a_second_load_is_idempotent(spark, lookup_source):
    target, first = _load(spark, lookup_source, REF_PAIS)
    second = load_reference_table(
        spark, REF_PAIS, source_table=lookup_source.bronze, target_table=target,
        load_date=RELOADED_AT,
    )

    assert first.appended == 1
    assert second.appended == 0
    assert second.already_present == 1


def test_reloading_one_of_the_colliding_pair_stays_idempotent_and_still_separate(
    spark, lookup_source
):
    """`REF_PAIS` above shares no `codigo` with anything else, so a broken anti-join
    OR a broken routing filter could each pass it by accident. `REF_MOTIVO` is one
    half of the measured `codigo='05'` collision: re-loading it must not duplicate
    its own two rows AND must not pick up `REF_QUALIFICACAO`'s row for the same code
    on the second pass -- idempotence and routing, exercised together, on the type
    that actually collides."""
    target, first = _load(spark, lookup_source, REF_MOTIVO)
    second = load_reference_table(
        spark, REF_MOTIVO, source_table=lookup_source.bronze, target_table=target,
        load_date=RELOADED_AT,
    )

    assert first.appended == 2
    assert second.appended == 0
    assert _rows(spark, target) == {"05": "MOTIVO CINCO", "00": "SEM MOTIVO"}


def test_the_target_carries_the_injected_load_date_and_the_earliest_record_source(
    spark, lookup_source
):
    """Every loader in this package refuses to stamp its own clock -- `load_date`
    has no default, per `load_hub`'s reason: a loader that stamped `current_
    timestamp()` internally could not be asserted against at all. Nothing in this
    file had asserted `load_date` or `record_source` on a written row before this
    fix round, which is precisely the gap that reason describes: the no-default
    parameter existed and nothing checked it landed. `_full_rows` closes it, in the
    shape `test_cnpj_vault.py` holds every hub and satellite to."""
    target, _ = _load(spark, lookup_source, REF_PAIS, load_date=LOADED_AT)

    assert _full_rows(spark, target) == [
        ("232", "ESTADOS UNIDOS", LOADED_AT, RECORD_SOURCE_VALUE)
    ]


# --------------------------------------------------------------------------- #
# The dedup tie-break: measured to never fire, exercised as a mechanism anyway.
# --------------------------------------------------------------------------- #

def test_a_duplicate_codigo_within_one_type_is_folded_deterministically(spark, lookup_source):
    """Two June rows share `codigo='9000001'` within `cnae` -- unmeasured on real
    2026-06 bronze (every type is 1:1 on rows-to-distinct-codigo), so this is a
    mechanism probe rather than a mirror of anything real.

    THE LOWER `record_source` WINS, AND THE FIXTURE NOW LETS THAT BE OBSERVED. The
    struct is (month, `record_source`, `descricao`) and both rows share a month, so
    `record_source` is what decides -- but the pair used to give the losing source the
    later description too, and then BOTH field orders returned the same row and this
    assertion held against an implementation that compared `descricao` first. The
    losing row now carries `'ARROZ A'`, so keeping `'ARROZ B'` is a statement about
    `record_source` and nothing else."""
    target, result = _load(spark, lookup_source, REF_CNAE)

    assert _rows(spark, target)["9000001"] == "ARROZ B"
    assert result.collapsed_duplicates == 1


def test_a_type_with_no_duplicates_reports_zero_collapsed(spark, lookup_source):
    """The contrast case, so `collapsed_duplicates == 1` above is read as the
    dedup firing and not as a constant the loader always reports."""
    _, result = _load(spark, lookup_source, REF_PAIS)

    assert result.collapsed_duplicates == 0
