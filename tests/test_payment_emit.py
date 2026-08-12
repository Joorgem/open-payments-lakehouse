# tests/test_payment_emit.py
"""THE BYTES ON DISK, and the declared streams that produce them.

WHY THIS FILE IS THE ONE THAT MATTERS FOR F1b TASK 3. Six golden digests are pinned
across `tests/test_payment_generator.py` and `tests/test_payment_defects.py`, and
every one of them is taken over `to_jsonl(...).encode("utf-8")` -- over text a test
encoded itself. Not one of them says anything about what a WRITER puts in a file. On
Windows the default is that the two differ: text mode translates `\\n` into `\\r\\n`,
so all six pins stay green while the artefact that ships is a different file.

AND THE OBVIOUS CHECK CANNOT SEE IT. A test that reads the file back as TEXT compares
equal either way, because universal-newline decoding is exactly what turns `\\r\\n`
back into `\\n`. This repository has already paid for that: a mutation probe's
"restore the original" step passed a content comparison and left the file
byte-changed. So the assertions below read BYTES, and
`test_a_text_mode_read_is_blind_to_the_defect_the_byte_read_catches` demonstrates the
blindness rather than asserting it -- with a deterministic `newline="\\r\\n"` write, so
the probe is the same on this Windows box and on CI's Linux, where the platform
default would make it a no-op.

NOTHING HERE STARTS SPARK. The generator is pure, `emit` is filesystem-only, and
every job-entry-point refusal below happens before a session is built -- which is the
property those refusals exist to have.
"""
from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path

import pytest

from opl.bronze import generated_landing as emit_module
from opl.bronze.generated_landing import (
    STREAM_FILE_SUFFIX,
    emit_stream_file,
    filename_for,
    serialised_bytes,
)
from opl.bronze.registry import LANDING_GENERATED, table_spec
from opl.contracts.payments import DRIFT_COLUMN
from opl.generator import profiles as profiles_module
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import (
    LATENESS_WINDOW_MS,
    NO_DEFECTS,
    DefectSpec,
    delivered_records,
    drift_positions,
    duplicate_positions,
    late_positions,
)
from opl.generator.profiles import (
    POOL_SEED,
    POOL_SIZE,
    PROFILES,
    SENTINEL_PROFILE,
    profile_for,
)
from opl.generator.stream import StreamSpec

_SRC = Path(__file__).resolve().parents[1] / "databricks" / "src"

# A pool and a stream small enough that every test in this file is instant. The
# emitter is indifferent to length -- it serialises whatever it is handed -- so the
# byte properties below are as total over 24 records as over 10,000, and the declared
# profiles are exercised separately at their real size where that matters.
_POOL = validated_pool([f"{n:08d}" for n in range(1, 21)])
_SPEC = StreamSpec(
    seed=20260813,
    stream_id="F1B-EMIT",
    event_count=24,
    repeat_count=4,
    window_start="2026-08-01T00:00:00.000Z",
    event_interval_ms=250,
    emission_lag_ms=1_500,
    cnpj_pool=_POOL,
)


def _dirs(tmp_path: Path) -> tuple[Path, Path]:
    """(landing, staging), mirroring `landing_generated_table` / `_tmp`.

    Two separate directories, because that separation is what the emitter's atomicity
    rests on -- a half-written file must never EXIST where a stream reads."""
    return tmp_path / "landing", tmp_path / "_tmp"


def _emit(tmp_path: Path, defects: DefectSpec = NO_DEFECTS):
    landing, staging = _dirs(tmp_path)
    records = delivered_records(_SPEC, defects)
    return records, emit_stream_file(
        records, _SPEC, directory=landing, tmp_directory=staging
    )


# --- THE BYTE PROOF ---------------------------------------------------------------


def test_the_written_file_is_the_serialised_text_encoded_utf8_byte_for_byte(tmp_path):
    """THE ASSERTION THIS WHOLE MODULE EXISTS FOR.

    `read_bytes`, never `read_text`: the defect being refused is a newline
    translation, and text-mode reading is the one operation that normalises exactly
    that difference away. The expected value is built the way every golden pin in
    this phase is built -- `to_jsonl(records).encode("utf-8")` -- so this ties the
    file on disk to the digests, rather than to a second serialisation."""
    records, landed = _emit(tmp_path)
    expected = emit_module.to_jsonl(records).encode("utf-8")
    on_disk = Path(landed.path).read_bytes()

    assert on_disk == expected
    assert len(on_disk) == landed.byte_count
    assert hashlib.sha256(on_disk).hexdigest() == landed.sha256
    assert landed.row_count == len(records)


def test_the_landed_bytes_carry_no_carriage_return_anywhere(tmp_path):
    """The Windows trap, stated over the file rather than over the serialiser.

    `tests/test_payment_generator.py` already refuses `\\r` in the TEXT. This is the
    same claim one layer down, and it is the layer the text-level test cannot reach:
    the serialiser can be perfect and the writer still ship CRLF."""
    _, landed = _emit(tmp_path)
    on_disk = Path(landed.path).read_bytes()
    assert b"\r" not in on_disk
    assert on_disk.endswith(b"\n"), "to_jsonl TERMINATES lines; the last one is not special"


def test_a_text_mode_read_is_blind_to_the_defect_the_byte_read_catches(tmp_path):
    """THE PROBE THAT MAKES THE ASSERTION ABOVE WORTH HAVING.

    Without it, "we compare bytes" is a style preference. This writes the SAME text
    through a handle that translates newlines, then shows that a text-mode read
    reports it as identical while a byte read does not -- so a test written the
    obvious way would have passed over a file that is 24 bytes longer than the golden
    pin describes.

    `newline="\\r\\n"` is passed EXPLICITLY rather than relying on the platform, which
    is what makes this probe mean the same thing on this Windows box and on CI's
    Linux. On Linux the default is no translation at all, so a probe that leaned on
    `open(p, "w")` would silently become a no-op exactly where the suite is quoted
    from."""
    records, _ = _emit(tmp_path)
    text = emit_module.to_jsonl(records)
    translated = tmp_path / "translated.jsonl"
    with open(translated, "w", encoding="utf-8", newline="\r\n") as handle:
        handle.write(text)

    assert translated.read_text(encoding="utf-8") == text, (
        "the probe is not a probe: text-mode reading did not normalise the difference"
    )
    assert translated.read_bytes() != text.encode("utf-8")
    assert b"\r\n" in translated.read_bytes()
    assert len(translated.read_bytes()) == len(text.encode("utf-8")) + text.count("\n")


def test_the_writer_refuses_a_payload_that_carries_a_carriage_return(monkeypatch):
    """The guard inside `serialised_bytes` fired, not merely present.

    Monkeypatched at the seam the guard watches -- a future edit that reintroduced
    `os.linesep` would arrive here as exactly this payload. Without the probe the
    refusal could be deleted with the suite green, since nothing the generator can
    produce reaches it."""
    monkeypatch.setattr(emit_module, "to_jsonl", lambda records: '{"a":"b"}\r\n')
    with pytest.raises(ValueError, match="carriage return"):
        serialised_bytes([{"a": "b"}])


def test_the_landed_file_is_the_only_thing_left_and_the_staging_dir_is_empty(tmp_path):
    """`os.replace` MOVED it, so nothing half-written can be discovered.

    cloudFiles walks a source dir recursively and with no glob (an F1.3 probe.txt
    planted in a sibling directory was ingested by a stream reading the month root),
    so an orphaned `.jsonl` left in the landing dir would be read as if it were a
    complete stream. The staging dir being empty is the other half: a leftover there
    is harmless -- it is outside every watched directory -- but its absence is what
    says the replace happened rather than a copy."""
    landing, staging = _dirs(tmp_path)
    _, landed = _emit(tmp_path)
    assert [p.name for p in sorted(landing.iterdir())] == [f"{_SPEC.stream_id}.jsonl"]
    assert list(staging.iterdir()) == []
    assert Path(landed.path).parent == landing


def test_the_filename_is_the_stream_id_and_nothing_run_scoped(tmp_path):
    """A run-scoped filename would turn idempotence into an append.

    Auto Loader tracks files by PATH, so the same rows landed under a new name on a
    re-run are NEW rows -- ingested again, into staging, under a fresh `_batch_id`
    that `promote.rows_of_batch` cannot recognise as a duplicate. The row counts
    double and nothing fails, which is the same defect an un-parameterised month
    produces one layer up."""
    assert filename_for(_SPEC) == f"{_SPEC.stream_id}{STREAM_FILE_SUFFIX}"
    assert STREAM_FILE_SUFFIX == ".jsonl"
    first = _emit(tmp_path)[1]
    second = _emit(tmp_path)[1]
    assert first.path == second.path


# --- IDEMPOTENCE, BECAUSE max_retries: 0 DOES NOT PREVENT A RETRY -----------------


def test_a_second_identical_run_touches_nothing_and_says_so(tmp_path):
    """A repair run re-derives identical bytes; that is what a retry looks like here.

    The mtime is asserted unchanged rather than only the content: a writer that
    rewrote the same bytes would satisfy a content check while replacing a file Auto
    Loader may already have recorded as read."""
    _, first = _emit(tmp_path)
    before = Path(first.path).stat().st_mtime_ns

    _, second = _emit(tmp_path)
    assert second.was_already_there and not first.was_already_there
    assert (second.sha256, second.byte_count, second.row_count) == (
        first.sha256, first.byte_count, first.row_count
    )
    assert Path(second.path).stat().st_mtime_ns == before


def test_a_different_stream_under_the_same_name_is_refused_not_overwritten(tmp_path):
    """The third outcome, and the only one that is a finding.

    Same stream id, different defects, therefore different bytes. Overwriting would
    replace a file whose rows may already be in bronze, leaving that table describing
    a stream that no longer exists anywhere -- and Auto Loader, which keys on the
    path, would never read the replacement."""
    _emit(tmp_path)
    with pytest.raises(ValueError, match="already holds a different stream"):
        _emit(tmp_path, DefectSpec(duplicate_count=3))


# --- THE DECLARED PROFILES ---------------------------------------------------------


def test_the_profiles_are_the_ones_declared_today():
    """Golden pin. Every number here is an input to a stream that will be landed in a
    Volume and ingested into bronze, so a change to any of them re-generates a file
    the workspace already holds -- which `emit_stream_file` then refuses. Pinned so
    that is a deliberate act."""
    assert sorted(PROFILES) == ["clean", "drifting", "promotable"]
    assert (POOL_SIZE, POOL_SEED) == (1024, 20260812)
    for profile in PROFILES.values():
        assert (profile.event_count, profile.repeat_count) == (10_000, 800)
        assert (profile.event_interval_ms, profile.emission_lag_ms) == (5_000, 1_500)
        assert profile.window_start == "2026-08-01T00:00:00.000Z"

    clean, promotable, drifting = (PROFILES[n] for n in ("clean", "promotable", "drifting"))
    assert clean.defects == NO_DEFECTS
    assert promotable.defects == DefectSpec(
        duplicate_count=150, late_count=100, late_by_ms=LATENESS_WINDOW_MS
    )
    assert drifting.defects == DefectSpec(drift_from_index=8_000)


def test_the_predicted_row_counts_are_arithmetic_on_the_declaration():
    """PUBLISHABLE BEFORE THE RUN, which is the standing rule about predictions.

    A duplicate adds a row; a legitimate repeat does not (it is one of the
    `event_count`, carrying an earlier event's attributes under its own id); lateness
    reorders and drift widens, and neither adds one. So these four numbers are known
    without a session, a pool or a workspace."""
    assert PROFILES["clean"].delivered_row_count == 10_000
    assert PROFILES["promotable"].delivered_row_count == 10_150
    assert PROFILES["drifting"].delivered_row_count == 10_000
    assert PROFILES["drifting"].drifted_row_count == 2_000
    assert PROFILES["clean"].drifted_row_count == 0
    assert PROFILES["promotable"].drifted_row_count == 0


def test_every_declared_profile_asks_for_defects_its_stream_can_carry():
    """THE TOTAL CHECK THE IMPORT GUARD DELIBERATELY DOES NOT RUN.

    `profiles._assert_every_profile_describes_a_stream_that_can_exist` covers the two
    cross-checks that are free (`_require_defects_fit`) and leaves this one here,
    because each of these three calls digests EVERY candidate position -- nine sorts
    of 10,000 SHA-256 draws, which would be paid on every import of the generator by
    every test that touches it. An expensive total check belongs in a test; the cheap
    half belongs at import.

    Against the SMALLEST legal pool, because none of these bounds involves the pool:
    they compare a defect count against the positions eligible for it, which is a
    property of `event_count` alone."""
    probe = validated_pool(("00000001", "00000002"))
    for profile in PROFILES.values():
        spec = profile.stream_spec(probe)
        assert len(duplicate_positions(spec, profile.defects)) == (
            profile.defects.duplicate_count
        )
        assert len(late_positions(spec, profile.defects)) == profile.defects.late_count
        assert len(drift_positions(spec, profile.defects)) == profile.drifted_row_count


def test_the_three_profiles_derive_disjoint_identities():
    """WHY EACH PROFILE HAS ITS OWN SEED AND STREAM ID.

    All three land in one month and promote into one bronze table, so a shared
    identity space would make `COUNT(*) - COUNT(DISTINCT transaction_id)` over that
    table count collisions BETWEEN streams as if they were injected redeliveries --
    the one number F1b's duplicate claim is stated in.

    Measured over 24 indices rather than 10,000, and that is a proxy stated as one:
    `stream._transaction_id` is a function of (seed, stream_id, index), so
    disjointness at any index range is evidence about the derivation rather than
    about a sample. Generating three full streams here would cost half a minute for
    the same argument."""
    identities = []
    for profile in PROFILES.values():
        spec = StreamSpec(
            seed=profile.seed,
            stream_id=profile.stream_id,
            event_count=24,
            repeat_count=0,
            window_start=profile.window_start,
            event_interval_ms=profile.event_interval_ms,
            emission_lag_ms=profile.emission_lag_ms,
            cnpj_pool=_POOL,
        )
        identities.append({r["transaction_id"] for r in delivered_records(spec, NO_DEFECTS)})
    assert len(set.union(*identities)) == sum(len(s) for s in identities)


@pytest.mark.parametrize("name", ["", "   ", SENTINEL_PROFILE])
def test_an_absent_profile_is_refused_as_an_absence(name):
    """The sentinel is what the job YAML defaults to, so reaching it means the run was
    launched without `--params profile=...`. Refused as an ABSENCE rather than as an
    unknown name, the way `require_month` treats `SENTINEL_MONTH`: the message names
    the missing parameter instead of explaining what a profile is."""
    with pytest.raises(ValueError, match="no profile was given"):
        profile_for(name)


def test_an_unknown_profile_is_refused_naming_the_declared_ones():
    with pytest.raises(ValueError, match="unknown stream profile") as excinfo:
        profile_for("promotible")  # a real typo
    assert "promotable" in str(excinfo.value)


def test_the_drifting_profile_actually_drifts_and_the_others_do_not():
    """The claim the whole `drifting` profile exists to make, checked on a proxy
    stream so it costs nothing: the drift column appears from the declared index
    onward and NOWHERE before it.

    Not a restatement of `drift_positions`: this reads the rendered records, which is
    what Auto Loader will meet, and asserts the pre-drift population is entirely
    without the key -- the population a `COUNT(DISTINCT ..., payment_channel)` would
    silently drop."""
    spec = StreamSpec(
        seed=PROFILES["drifting"].seed,
        stream_id=PROFILES["drifting"].stream_id,
        event_count=100,
        repeat_count=0,
        window_start=PROFILES["drifting"].window_start,
        event_interval_ms=PROFILES["drifting"].event_interval_ms,
        emission_lag_ms=PROFILES["drifting"].emission_lag_ms,
        cnpj_pool=_POOL,
    )
    records = delivered_records(spec, DefectSpec(drift_from_index=80))
    carriers = [i for i, record in enumerate(records) if DRIFT_COLUMN in record]
    assert carriers == list(range(80, 100))
    assert all(DRIFT_COLUMN not in record for record in records[:80])


# --- THE TWO JOB ENTRY POINTS, REFUSING BEFORE SPARK ------------------------------


def _load(name: str):
    """A `databricks/src` entry point, by path. They are job scripts, not wheel
    modules, and every refusal below happens before a session is built."""
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("script", ["generate_payments", "bronze_payments_ingest"])
def test_an_unknown_table_is_refused_before_anything_else(script):
    """The table is resolved first in both, so a typo is answered by the registry
    naming the valid tables rather than by a serverless session."""
    from opl.bronze.registry import UnknownTable

    with pytest.raises(UnknownTable, match="payments"):
        _load(script).main(["payment"])  # a real typo: singular


@pytest.mark.parametrize("script", ["generate_payments", "bronze_payments_ingest"])
def test_a_table_that_is_not_generated_is_refused_by_both_payment_tasks(script):
    """The pairing guard, and it must refuse rather than proceed in both directions.

    `generate_payments` handed a CNPJ table would derive a payment stream into the
    directory that table's CSV Auto Loader reads. `bronze_payments_ingest` handed one
    would read a generated-root directory nothing has ever written to and report
    SUCCESS having ingested zero rows -- indistinguishable from a month in which no
    file arrived.

    The remaining argv is valid in both, so the landing refusal is the only one that
    can fire."""
    with pytest.raises(ValueError, match=LANDING_GENERATED):
        _load(script).main(["empresas", "2026-08", "clean"])


def test_the_generator_refuses_a_missing_month_and_a_missing_profile():
    """Both, in order: the month is validated before the profile is resolved, so the
    first argv can only fail on the month and the second only on the profile."""
    module = _load("generate_payments")
    with pytest.raises(ValueError, match="no month was given"):
        module.main(["payments"])
    with pytest.raises(ValueError, match="no profile was given"):
        module.main(["payments", "2026-08"])


def test_the_payments_ingest_refuses_a_missing_batch_id_and_a_missing_month():
    from opl.bronze.promote import PromoteRefused

    module = _load("bronze_payments_ingest")
    with pytest.raises(PromoteRefused, match="batch"):
        module.main(["payments"])
    with pytest.raises(ValueError, match="no month was given"):
        module.main(["payments", "12345"])


def test_the_registered_payments_table_is_the_one_these_tasks_are_built_for():
    """Guard the guard: every refusal above is about a table that is NOT generated, so
    all of them would still pass if `payments` itself had stopped being one -- with
    both entry points then refusing the only table they exist to serve."""
    spec = table_spec("payments")
    assert spec.landing == LANDING_GENERATED
    assert profiles_module.PROFILES  # and there is at least one stream to land there
