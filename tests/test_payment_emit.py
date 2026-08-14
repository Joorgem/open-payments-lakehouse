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

import dataclasses
import hashlib
import importlib.util
import inspect
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
    _require_defects_fit,
    delivered_records,
    drift_positions,
    duplicate_positions,
    late_positions,
)
from opl.generator.instants import from_text
from opl.generator.profiles import (
    BETWEEN_SNAPSHOTS,
    POOL_SEED,
    POOL_SIZE,
    PROFILES,
    SENTINEL_PROFILE,
    StreamProfile,
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
    path, would never read the replacement.

    The refusal's wording widened when F-API Task 2 generalised the emitter: it serves
    two producers now and cannot say "stream" about both. What is asserted here is the
    part that is about THIS producer -- both digests, so the reader can see which file
    is on disk and which one the run derived."""
    _, landed = _emit(tmp_path)
    with pytest.raises(ValueError) as excinfo:
        _emit(tmp_path, DefectSpec(duplicate_count=3))
    message = str(excinfo.value)
    assert "already holds different bytes" in message
    assert landed.sha256 in message, "the operator must see the digest that is on disk"


# --- THE DECLARED PROFILES ---------------------------------------------------------


# The three streams F1b landed, ingested and published evidence about. Named as a
# group because F3 added a fourth profile whose window is deliberately elsewhere, and
# every claim below that is about the SHARED WINDOW is about these three only -- while
# every claim about the shared SHAPE is still about all four.
_F1B_PROFILES = ("clean", "promotable", "drifting")


def test_the_profiles_are_the_ones_declared_today():
    """Golden pin. Every number here is an input to a stream that will be landed in a
    Volume and ingested into bronze, so a change to any of them re-generates a file
    the workspace already holds -- which `emit_stream_file` then refuses. Pinned so
    that is a deliberate act."""
    assert sorted(PROFILES) == ["between-snapshots", "clean", "drifting", "promotable"]
    assert (POOL_SIZE, POOL_SEED) == (1024, 20260812)
    for profile in PROFILES.values():
        assert (profile.event_count, profile.repeat_count) == (10_000, 800)
        assert (profile.event_interval_ms, profile.emission_lag_ms) == (5_000, 1_500)
    for name in _F1B_PROFILES:
        assert PROFILES[name].window_start == "2026-08-01T00:00:00.000Z"

    clean, promotable, drifting = (PROFILES[n] for n in _F1B_PROFILES)
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


def test_the_declared_profiles_derive_disjoint_identities():
    """WHY EACH PROFILE HAS ITS OWN SEED AND STREAM ID.

    All of them promote into one bronze table, so a shared identity space would make
    `COUNT(*) - COUNT(DISTINCT transaction_id)` over that table count collisions
    BETWEEN streams as if they were injected redeliveries -- the one number F1b's
    duplicate claim is stated in. This quantifies over `PROFILES` rather than over
    three names, so the fourth profile is covered by the same assertion instead of by
    a second one somebody has to remember to write.

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


# --- THE FOURTH PROFILE: THE "BEFORE" SIDE OF F3's AS-OF JOIN ----------------------
#
# The two `applied_date`s the CNPJ vault holds, spelled as LITERALS here rather than
# imported from `opl.generator.profiles`. That module's import guard compares the
# declared window against its own copy of these two dates; a test that imported the
# same constant would agree with it forever -- including on the day someone widened
# the constant to make a moved window legal. Measured in F2 wave 1 and restated in
# `docs/f3-run-evidence.md` section 0.5.
_EARLIER_APPLIED_DATE = "2026-06-13T00:00:00.000Z"
_LATER_APPLIED_DATE = "2026-07-11T00:00:00.000Z"


def test_the_fourth_profiles_whole_window_lies_strictly_inside_the_vault_interval():
    """THE CLAIM THE FOURTH PROFILE EXISTS TO MAKE, AND THE ONLY ONE ONLY IT CAN.

    F3 Task 0 measured that all 20,150 payments in bronze sit on 2026-08-01, after
    both `applied_date`s, so every as-of lookup resolves to the LAST version and the
    join is bit-identical to `WHERE valid_to = <sentinel>`. The existing three streams
    are the "after" side; this profile is the "before" side, and it is only that if
    its whole event window falls between the two dates.

    COMPUTED FROM THE DECLARATION ALONE. `window_start` is the first `event_time` --
    `stream._event_at` adds whole intervals to it -- and `last_event_time` is that
    plus `(event_count - 1) * event_interval_ms`. No pool, no session, no generated
    byte, so this is a prediction in the sense section 4.5 of the master protocol
    means.

    ASSERTED RATHER THAN REASONED ABOUT, because the reasoning is only true at today's
    numbers: 10,000 events 5,000 ms apart span 13 h 53 m 15 s against an interval 28
    days wide, which is enormous margin right up until `_EVENT_COUNT` or
    `_EVENT_INTERVAL_MS` moves. The day it stops fitting is the day the profile
    silently becomes a fourth copy of the other three."""
    profile = PROFILES[BETWEEN_SNAPSHOTS]
    first, last = from_text(profile.window_start), from_text(profile.last_event_time)

    assert from_text(_EARLIER_APPLIED_DATE) < first
    assert last < from_text(_LATER_APPLIED_DATE)
    assert (profile.window_start, profile.last_event_time) == (
        "2026-06-20T00:00:00.000Z", "2026-06-20T13:53:15.000Z"
    )
    assert last - first == (profile.event_count - 1) * profile.event_interval_ms
    assert last - first == 49_995_000  # 13 h 53 m 15 s, the F1b span unchanged


def test_a_window_moved_out_of_the_interval_is_refused_at_import(monkeypatch):
    """THE PROBE THAT MAKES THE GUARD WORTH HAVING.

    Without it the refusal is a branch nothing has ever taken, which is how a refusal
    path stays green and wrong. The doctored profile is the declaration with its window
    moved back onto F1b's -- the single most likely wrong edit, because it is what
    `_profile`'s default gives anyone who deletes the keyword argument."""
    moved = dataclasses.replace(
        PROFILES[BETWEEN_SNAPSHOTS], window_start="2026-08-01T00:00:00.000Z"
    )
    monkeypatch.setitem(profiles_module.PROFILES, BETWEEN_SNAPSHOTS, moved)
    with pytest.raises(ValueError, match="not strictly inside"):
        profiles_module._refuse_a_window_that_leaves_the_interval_it_was_declared_for()


def test_the_fourth_profiles_predicted_counts_are_arithmetic_on_the_declaration():
    """IT CARRIES NO DEFECT, AND THAT IS A DECLARATION RATHER THAN AN OMISSION.

    The profile exists to place payments in time. A fourth defect class would add a
    number to F1b's published totals -- 150 redeliveries, 100 late arrivals, 2,000
    drifted rows, all stated against `bronze_payments` as a whole -- and every one of
    those sentences would then be describing a table it no longer describes. The three
    sums below are the assertion that none of them moved.

    `delivered_row_count` equals `event_count` exactly because a duplicate is the only
    thing that adds a row; `drifted_row_count` is zero because `drift_from_index` is
    None. Both are arithmetic on the declaration, which is what lets Task 4 publish
    them before the run."""
    profile = PROFILES[BETWEEN_SNAPSHOTS]
    assert profile.defects == NO_DEFECTS and profile.defects.is_clean
    assert profile.delivered_row_count == profile.event_count == 10_000
    assert profile.drifted_row_count == 0

    assert sum(p.defects.duplicate_count for p in PROFILES.values()) == 150
    assert sum(p.defects.late_count for p in PROFILES.values()) == 100
    assert sum(p.drifted_row_count for p in PROFILES.values()) == 2_000
    assert sum(p.delivered_row_count for p in PROFILES.values()) == 40_150


def test_the_fourth_profile_lands_under_its_own_name_and_its_own_identities():
    """THE STREAM ID IS THE LANDING FILENAME AND HALF OF EVERY DERIVATION PURPOSE.

    A duplicated one would write two streams to one path -- the second run refused,
    loudly, but only after the first had landed and been ingested -- and would fold two
    profiles into one identity space, which is what
    `test_the_declared_profiles_derive_disjoint_identities` measures on the other side.
    The key/name agreement is the third: `profile_for("between-snapshots")` returning
    something that logs itself as "clean" is misdirection at the exact moment an
    operator is reading a log to find out what landed."""
    filenames = [filename_for(p.stream_spec(_POOL)) for p in PROFILES.values()]
    assert filename_for(PROFILES[BETWEEN_SNAPSHOTS].stream_spec(_POOL)) == (
        f"F3-BETWEEN-SNAPSHOTS{STREAM_FILE_SUFFIX}"
    )
    assert len(set(filenames)) == len(PROFILES) == 4
    assert all(profile.name == key for key, profile in PROFILES.items())
    assert profile_for(BETWEEN_SNAPSHOTS) is PROFILES[BETWEEN_SNAPSHOTS]
    profiles_module._assert_the_profiles_are_declared_consistently()


def test_the_three_f1b_streams_are_the_same_input_to_the_generator_as_before():
    """BYTE-IDENTITY, PINNED WHERE IT IS CHEAP TO PIN.

    F1b's published evidence -- 20,150 rows, 150 duplicates, 100 late arrivals, 2,000
    drifted rows, 1,024/1,024 CNPJ resolution -- rests on three files that must not
    move. `delivered_records` is a pure function of `(StreamSpec, DefectSpec)`, so
    pinning that PAIR is the whole input to the derivation and therefore the whole
    input to the bytes; generating the three 10,000-event streams to digest them would
    cost half a minute for a strictly weaker statement (it would also depend on the
    real pool, which needs a 69M-row table).

    Written as literal constructions rather than as a loop over `PROFILES`, because a
    pin that reads its expectation from the thing it is pinning is not a pin. The
    fourth profile is deliberately absent: adding it must not touch these three."""
    probe = validated_pool(("00000001", "00000002"))
    shape = {
        "event_count": 10_000, "repeat_count": 800,
        "window_start": "2026-08-01T00:00:00.000Z",
        "event_interval_ms": 5_000, "emission_lag_ms": 1_500, "cnpj_pool": probe,
    }
    expected = {
        "clean": (StreamSpec(seed=20260813, stream_id="F1B-CLEAN-2026-08", **shape),
                  NO_DEFECTS),
        "promotable": (StreamSpec(seed=20260814, stream_id="F1B-PROMOTABLE-2026-08", **shape),
                       DefectSpec(duplicate_count=150, late_count=100,
                                  late_by_ms=LATENESS_WINDOW_MS)),
        "drifting": (StreamSpec(seed=20260815, stream_id="F1B-DRIFTING-2026-08", **shape),
                     DefectSpec(drift_from_index=8_000)),
    }
    for name, (spec, defects) in expected.items():
        assert PROFILES[name].stream_spec(probe) == spec
        assert PROFILES[name].defects == defects


def test_the_factory_varies_exactly_the_fields_its_docstring_names():
    """THE DOCSTRING AND THE CODE, CHECKED AGAINST EACH OTHER RATHER THAN BOTH READ.

    `_profile` used to hardcode `window_start` while its docstring said the streams
    "differ ONLY in their id, their seed and their defects". Both halves were true, and
    the fourth profile makes the second one false -- so the parameter and the sentence
    had to move together. This repository has a documented case of exactly that pair
    drifting apart with the suite green: `drifted_row_count`'s docstring promised
    redeliveries its arithmetic did not count, and the test that guarded it was
    index-based in the same way (found by review on PR #14).

    THE ASSERTION IS STRUCTURAL, NOT TEXTUAL. The factory's parameters are the fields
    that MAY differ; every `StreamProfile` field that is not a parameter therefore
    cannot differ, and the third block proves that is what the declarations actually
    do. The docstring check is last and is the weakest of the three: it only catches a
    sentence that stopped naming a field, which is the half a structural check cannot
    see."""
    varying = set(inspect.signature(profiles_module._profile).parameters)
    assert varying == {"name", "stream_id", "seed", "defects", "window_start"}

    shared = {field.name for field in dataclasses.fields(StreamProfile)} - varying
    assert shared == {"event_count", "repeat_count", "event_interval_ms", "emission_lag_ms"}
    for field in shared:
        held = {getattr(profile, field) for profile in PROFILES.values()}
        assert len(held) == 1, f"{field} is no factory parameter and yet differs: {held}"

    doc = profiles_module._profile.__doc__
    for field in varying | shared:
        assert field in doc, f"the factory's docstring no longer names {field}"


def test_a_straddling_window_would_have_cost_the_shared_shape_and_is_not_refused():
    """WHY THE WINDOW SITS BETWEEN THE TWO DATES RATHER THAN ACROSS THEM.

    THE REASON IS NOT THE ONE IT LOOKS LIKE, AND THE ARITHMETIC IS HERE SO NOBODY HAS
    TO TAKE IT ON TRUST. A stream spanning the 28 days would need an interval of
    241,944 ms, and `_require_defects_fit` would NOT refuse lateness on it: that guard
    asks only that `late_by_ms` EXCEED the interval, and one lateness window is
    3,600,000 ms -- fourteen times larger. A straddle could carry all three defect
    classes.

    WHAT IT WOULD ACTUALLY COST IS THE SHARED SHAPE. 241,944 ms is 48x the interval the
    other three profiles run at, so the fourth stream would no longer be the same
    delivery moved in time; any difference an as-of join showed between it and them
    would have two candidate causes, which is the same argument
    `opl.generator.defects` makes for switching one defect class at a time. F3 Task 0
    pre-decided the between-window on that basis (`docs/f3-run-evidence.md` 0.3), and
    the existing streams already supply the "after" side, so nothing needs a straddle.

    ASSERTED IN BOTH DIRECTIONS: that the straddle is legal (so the false reason cannot
    survive here as folklore) and that the declared profile did not take it."""
    span = from_text(_LATER_APPLIED_DATE) - from_text(_EARLIER_APPLIED_DATE)
    profile = PROFILES[BETWEEN_SNAPSHOTS]
    straddling_interval = span // (profile.event_count - 1)
    assert (span, straddling_interval) == (28 * 24 * 3_600_000, 241_944)

    straddle = dataclasses.replace(
        profile.stream_spec(_POOL),
        window_start=_EARLIER_APPLIED_DATE,
        event_interval_ms=straddling_interval,
    )
    _require_defects_fit(straddle, DefectSpec(late_count=1, late_by_ms=LATENESS_WINDOW_MS))
    assert LATENESS_WINDOW_MS > straddling_interval

    assert profile.event_interval_ms == PROFILES["clean"].event_interval_ms == 5_000
    assert straddling_interval == 48 * profile.event_interval_ms + 1_944


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
