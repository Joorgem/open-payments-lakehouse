"""`load_satellite`'s two optional diagnostics: what they cost, what they report, and
the one thing they must never touch.

THE MEASUREMENT THAT PUT THIS FILE HERE. The vault's first real run loaded
`sat_empresa_dados` in **5,635 s** against `hub_empresa`'s **281 s** over the same
69,062,849 keys (`docs/f2-wave-1-workspace-run-evidence.md` §1.6). `load_hub` makes one
pass over the source; `load_satellite` made four, and TWO OF THEM WROTE NOTHING --
`_collapsed_duplicates` re-scanned the whole source to count a fold, and
`_candidate_departures` materialised the observation ledger's all-keys x all-months grid
to count an absence. Both came back 0. Estabelecimentos is 72.3M keys with two
satellites.

SO THEY ARE OPTIONAL AND OFF BY DEFAULT, AND `None` IS NOT `0`. That distinction is the
subject of half this file. Those two zeros are PUBLISHED, as evidence that the dedup
tie-break and the departure path are unexercised by real data; a flag that turned a real
0 into a silent 0 would make that evidence unfalsifiable, because nothing in the result
or the log would separate a measurement from a skip. `None` can only mean "not
measured".

WHAT IS PINNED HERE AND WHY EACH ONE IS NOT ENOUGH ON ITS OWN:

  - The fields are `None` when the flag is off -- AND the two functions are not CALLED.
    The field assertion alone passes an implementation that does all the work and then
    throws the numbers away, which is the entire cost this change exists to remove.
  - The numbers when the flag is on are the ones the fixture really carries, so "on"
    is a measurement rather than a constant.
  - **The rows written are identical either way.** The safety property: this flag
    changes cost and reporting, never data.
  - The ledger's refusal of a month nothing ever loaded SURVIVES the skip. That guard
    is not a diagnostic -- `opl.vault.satellites` calls it one of the two things
    consulting the ledger really buys -- and it lives inside the derivation the
    departure count was skipping. Deriving the ledger and counting from it are now two
    steps, and only the second is optional.
  - **The line the job task prints tells the two states apart.** A `None` that reaches
    an operator's log as `0` puts the whole distinction back where it started, and a
    task log is the one place nobody re-derives anything.
"""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from opl.vault import domains, satellites
from opl.vault.columns import APPLIED_DATE
from opl.vault.observation import ObservationGrain
from opl.vault.satellites import SatelliteLoadResult, load_satellite
from opl.vault.specs import KeyPrefix

from .conftest import (
    JUL,
    JUN,
    LOADED_AT,
    audit_values,
    bronze_schema,
    quarantine_schema,
    write_delta,
)

HUB = domains.table_spec("hub_empresa")
SAT = domains.table_spec("sat_empresa_dados")

_EMPRESAS = bronze_schema("empresas")

C_CHANGED = "70000001"     # razão social moves between the months
C_DEPARTED = "70000002"    # June only -- the one `absent_after_observation`
C_DUPLICATED = "70000003"  # two June rows on one (key, month) -- the one fold
C_QUARANTINED = "70000004"  # rejected in both months, so the ledger has a second side


def _row(cnpj: str, month: str, *, razao: str = "ACME LTDA") -> tuple:
    """One bronze empresas row, every audit column populated as the ingest stamps them."""
    return (cnpj, razao, "2062", "49", "1000,00", "05", None) + audit_values(
        month,
        source_file=f"/Volumes/x/cnpj/{month}/empresas/K3241.K03200Y0.D60613.EMPRECSV",
    )


def _bronze_rows() -> list[tuple]:
    """Six rows carrying exactly one fold and exactly one departure.

    BOTH DIAGNOSTICS MUST COME BACK NON-ZERO HERE, which is the fixture's whole job: on
    a source where the honest answer is 0, "measured" and "skipped" would differ only in
    `None` and every value assertion below would hold for the wrong reason."""
    return [
        _row(C_CHANGED, JUN, razao="ACME LTDA"),
        _row(C_CHANGED, JUL, razao="ACME PARTICIPACOES SA"),
        # In June and gone from July, and NOT in July's quarantine -- the shape the
        # ledger calls `absent_after_observation` and this loader only ever reports.
        _row(C_DEPARTED, JUN, razao="DELTA EIRELI"),
        # THE FOLD. Two source rows sharing (cnpj_basico, 2026-06) with different
        # payloads; the lowest `hash_diff` wins and the other payload is discarded.
        _row(C_DUPLICATED, JUN, razao="GAMMA ME"),
        _row(C_DUPLICATED, JUN, razao="GAMMA PARTICIPACOES"),
        _row(C_DUPLICATED, JUL, razao="GAMMA ME"),
    ]


@pytest.fixture(scope="module")
def source(spark, vault_database):
    """A throwaway Delta database holding one bronze empresas table and its quarantine.

    Its own database rather than `tests/vault/test_cnpj_vault.py`'s: that fixture mirrors
    the real 06->07 measurement and has ZERO folds and ZERO departures on purpose, which
    is exactly what this file cannot use."""
    db = vault_database("sat_diagnostics")
    bronze, quarantine = f"{db}.empresas", f"{db}.empresas_q"

    write_delta(spark, bronze, _EMPRESAS, _bronze_rows())
    write_delta(spark, quarantine, quarantine_schema("empresas"), [
        (*_row(C_QUARANTINED, JUN, razao=""), "null_or_empty_razao_social"),
        (*_row(C_QUARANTINED, JUL, razao=""), "null_or_empty_razao_social"),
    ])

    grain = ObservationGrain(
        name="hub_empresa", bronze_table=bronze, quarantine_table=quarantine,
        key_columns=("cnpj_basico",),
    )
    return SimpleNamespace(db=db, bronze=bronze, quarantine=quarantine, grain=grain)


@pytest.fixture
def target(source):
    """Two fresh satellite table names per test: every test here writes, and two of them
    write twice and compare the results."""
    suffix = uuid4().hex[:8]
    return SimpleNamespace(sat=f"{source.db}.sat_{suffix}", other=f"{source.db}.oth_{suffix}")


def _load(spark, source, table: str, *, months=(JUN, JUL), **kwargs):
    """One satellite load. `months` is always passed, because a named window is what
    makes `observation._window`'s refusal reachable at all -- with `None` the window IS
    the data's own months and cannot contain one the data has not seen."""
    return load_satellite(
        spark, SAT, hub=HUB, source_table=source.bronze, target_table=table,
        load_date=LOADED_AT, grain=source.grain, months=list(months), **kwargs,
    )


def _spy(monkeypatch, name: str) -> list:
    """Record every call to `satellites.<name>` and still perform it.

    DELEGATING RATHER THAN STUBBING, so one test can assert BOTH that the function ran
    and what it answered. A stub would make the value assertions self-referential."""
    calls: list = []
    original = getattr(satellites, name)

    def recorded(*args, **kwargs):
        calls.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(satellites, name, recorded)
    return calls


def _rows(spark, table: str) -> list[dict]:
    """Every row of `table` as a dict, in a deterministic order."""
    return [
        row.asDict()
        for row in sorted(
            (row for row in spark.read.table(table).collect()),
            key=lambda row: (row[HUB.hash_key], row[APPLIED_DATE]),
        )
    ]


def test_a_default_load_computes_neither_diagnostic_and_reports_none_rather_than_zero(
    spark, source, target, monkeypatch
):
    """THE POINT OF THE WHOLE CHANGE, and the call assertion is the half that carries it.

    A test that only checked the two fields were `None` would stay green against an
    implementation that made both passes over the source and then discarded the numbers
    -- i.e. against the exact 5,635 s this exists to remove. So the two functions are
    watched, and the claim is that they never ran."""
    folds = _spy(monkeypatch, "_collapsed_duplicates")
    departures = _spy(monkeypatch, "_candidate_departures")

    result = _load(spark, source, target.sat)

    assert (folds, departures) == ([], [])
    assert result.collapsed_duplicates is None
    assert result.candidate_departures is None
    assert result.appended > 0, "a load that wrote nothing would prove nothing about cost"


def test_the_flag_measures_both_and_gets_the_numbers_this_source_really_carries(
    spark, source, target, monkeypatch
):
    """The other arm, with the numbers asserted as VALUES rather than as "not None".

    One fold (two June rows on `C_DUPLICATED`) and one departure (`C_DEPARTED`, in June
    and absent from July with nothing in July's quarantine to explain it). Both are what
    the fixture was built to carry, so an implementation that returned a constant, or
    that measured the wrong window, is red here."""
    folds = _spy(monkeypatch, "_collapsed_duplicates")
    departures = _spy(monkeypatch, "_candidate_departures")

    result = _load(spark, source, target.sat, report_diagnostics=True)

    assert (len(folds), len(departures)) == (1, 1)
    assert result.collapsed_duplicates == 1
    assert result.candidate_departures == 1


def test_the_flag_changes_what_is_reported_and_never_a_row_that_is_written(
    spark, source, target
):
    """THE SAFETY PROPERTY. Two loads of the same source over the same window into two
    targets, one measured and one not, compared row for row and column for column.

    Whole rows rather than counts: a flag that changed which of two folded payloads won,
    or that shifted a column order, would keep every count identical. `appended` and
    `already_present` are compared too, because they are the fields that stay `int` on
    both sides and a change there would be a change in what landed."""
    quiet = _load(spark, source, target.sat)
    loud = _load(spark, source, target.other, report_diagnostics=True)

    assert _rows(spark, target.sat) == _rows(spark, target.other)
    assert spark.read.table(target.sat).columns == spark.read.table(target.other).columns
    assert (quiet.appended, quiet.already_present) == (loud.appended, loud.already_present)
    assert quiet.appended > 0


def test_a_month_no_snapshot_ever_loaded_is_refused_even_when_nothing_is_measured(
    spark, source, target
):
    """THE GUARD THAT MUST SURVIVE THE SKIP, and the reason the ledger's derivation and
    the departure count are now two steps rather than one function.

    `observation._window` refuses a month with no row on either side -- without it,
    `months=['2026-09']` selects no bronze row, the satellite writes nothing and reports
    success, and a widened window would instead manufacture a candidate delete for every
    key in the table. `opl.vault.satellites` names that refusal, not the count, as one of
    the two things consulting the ledger actually buys. It reaches this loader only by
    deriving the ledger, so the derivation happens on every load and only the `count()`
    is optional. Asserted on the DEFAULT path, which is the one that skips."""
    with pytest.raises(ValueError, match="2026-09"):
        _load(spark, source, target.sat, months=[JUN, JUL, "2026-09"])

    assert not spark.catalog.tableExists(target.sat)


def _entry_point():
    """`databricks/src/vault_load_satellite.py`, loaded by path.

    The job entry points are not part of the `opl` wheel -- `bundle deploy` syncs them
    and each task's `python_file` reads one from there -- so the suite loads them the way
    `tests/test_vault_job_wiring.py` does, by file location with no sys.path edit."""
    path = Path(__file__).resolve().parents[2] / "databricks" / "src"
    spec = importlib.util.spec_from_file_location("sat_task", path / "vault_load_satellite.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _result(collapsed: int | None, departures: int | None) -> SatelliteLoadResult:
    return SatelliteLoadResult(
        table="sat_empresa_dados", appended=7, already_present=0,
        collapsed_duplicates=collapsed, candidate_departures=departures,
    )


def test_the_line_a_skipped_run_prints_cannot_be_read_as_a_measured_zero():
    """THE SAME DISTINCTION, AT THE ONE PLACE AN OPERATOR ACTUALLY MEETS IT.

    `0 source rows were folded` is a measurement, and it is published as evidence that
    this loader's dedup tie-break is unexercised by real data. A skipped run that printed
    a zero -- or any number -- would put an unfalsifiable claim into a task log, and a
    task log is read once, by someone who will not re-derive it. So the skipped line
    carries NO count at all, says which parameter turned it off, and says what to pass.

    THE `main` ASSERTION IS WHAT KEEPS THIS FROM BEING SELF-REFERENTIAL: a test of
    `_diagnostics_note` alone stays green while `main` prints the old, unconditional
    sentence beside it."""
    task = _entry_point()

    skipped = task._diagnostics_note((JUN, JUL), _result(None, None))
    measured = task._diagnostics_note((JUN, JUL), _result(0, 0))

    assert skipped.startswith("NEITHER DIAGNOSTIC WAS MEASURED")
    assert not re.search(r"\d", skipped), f"the skipped line carries a number: {skipped!r}"
    assert task.DIAGNOSTICS_PARAMETER in skipped
    assert "0 source rows were folded" in measured
    assert "0 candidate departures" in measured
    assert "_diagnostics_note(months, result)" in Path(task.__file__).read_text(
        encoding="utf-8"
    ), "main no longer prints through _diagnostics_note, so this test measures nothing"


def test_a_hub_grain_declaring_a_key_prefix_is_refused(spark, source, target):
    """THE HOLE F-DB TASK 5's CORRECTION PASS OPENED WHILE CLOSING ANOTHER, refused where
    it would land. A grain may now be READ THROUGH a `KeyPrefix` -- the derivation that
    makes `link_merchant_empresa`'s ledger key on the eight characters its digest is over
    rather than on the fourteen bronze holds. A HUB has none: its business key is read
    from the columns it is named after (`loading._padded_components` is the whole of it),
    so there is nothing to compare a prefix against and it is refused outright.

    AND HERE THE MISTAKE POINTS THE OTHER WAY, which is why the refusal is worth its own
    test rather than falling out of the link's. On a link the missing prefix made the
    ledger FINER than the thing it gates; a prefix on a hub key makes it COARSER --
    `10000001` and `10000002` fold into one ledger key at width 7 -- so this file's whole
    subject, `candidate_departures`, would be reported only when the LAST company sharing
    a truncation left. Small, plausible, and about a key space no satellite row exists
    for."""
    truncated = ObservationGrain(
        name="hub_empresa", bronze_table=source.bronze,
        quarantine_table=source.quarantine, key_columns=("cnpj_basico",),
        key_prefixes=(KeyPrefix(column="cnpj_basico", width=7),),
    )

    with pytest.raises(ValueError, match="reads its business key from the columns"):
        load_satellite(
            spark, SAT, hub=HUB, source_table=source.bronze, target_table=target.sat,
            load_date=LOADED_AT, grain=truncated, months=[JUN, JUL],
        )

    assert not spark.catalog.tableExists(target.sat)


def test_a_result_cannot_call_one_diagnostic_measured_and_the_other_not():
    """One flag governs both, so the half-measured pair is a state no load can produce
    and no reader could interpret -- `collapsed_duplicates=0` beside
    `candidate_departures=None` says the load both did and did not do the extra work.

    Refused in the type's own constructor rather than trusted to the one caller, because
    the whole value of `None` here is that it means exactly one thing."""
    with pytest.raises(ValueError, match="measured together or not at all"):
        SatelliteLoadResult(
            table="sat_empresa_dados", appended=1, already_present=0,
            collapsed_duplicates=0, candidate_departures=None,
        )
