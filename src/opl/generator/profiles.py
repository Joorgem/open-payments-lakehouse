# src/opl/generator/profiles.py
"""The named streams this lakehouse generates, declared as DATA.

WHY A PROFILE AND NOT TWELVE JOB PARAMETERS. A `StreamSpec` plus a `DefectSpec` is
twelve numbers, and a job that took them as parameters would put twelve strings in a
YAML for an operator to parse, validate and get subtly wrong -- `late_by_ms` one zero
short is a stream that reports late arrivals in its spec and contains none, which is
the exact "fixture that passes its own test by measuring two empty sets" shape
`defects._require_the_delay_matches_the_count` exists to refuse. A profile makes the
launch one parameter whose only legal values are declared here, so every number a
generated stream depends on is in the repository, in a diff, before the run.

IT IS ALSO WHAT MAKES THE PREDICTIONS PUBLISHABLE BEFORE THE RUN. Row counts,
duplicate counts, late counts and drifted counts are all arithmetic on the fields
below and need no session -- so F1b Task 4 can state them with statement ids ahead of
launching anything, which is this project's standing rule about predictions. Only the
digest needs the run, because only the pool does.

--- WHY THERE ARE THREE, AND WHY THAT IS FORCED RATHER THAN CHOSEN -----------------

The DQ gate is ALL-OR-NOTHING: any rejected row in a batch sends the job down the
`fail_on_dq` branch and the promote never runs. Schema drift is a rejection --
deliberately, since the drift column is undeclared precisely so Auto Loader rescues
it and `rescued_data_present` fires. So ONE stream carrying all three defect classes
could never prove the phase's claims, because the drift would stop its own duplicates
and late arrivals from ever reaching bronze.

    CLEAN       proves the path. Zero rejects, rows in bronze. If this one goes red
                the failure is the pipeline, not the defect -- the same reason Task 1
                built a clean stream before Task 2 layered anything on it.
    PROMOTABLE  duplicates + late arrivals, and NO drift. Neither class is a DQ
                reject (a redelivery is a well-formed row; a late arrival is a
                relation between two columns), so this batch promotes and the
                duplicate and lateness claims are measurable IN BRONZE.
    DRIFTING    drift alone. The gate quarantines the drifted rows and the run stops
                at `fail_on_dq`; the claim is measured in the QUARANTINE table, and
                the 8,000 undrifted rows of that batch stay in staging unpromoted.
                That is the gate working, not a failure to design around.

THEIR STREAM IDS AND SEEDS DIFFER, WHICH IS LOAD-BEARING. `stream._transaction_id`
folds the stream id into its derivation purpose, so three profiles produce three
disjoint identity spaces -- and `COUNT(*) - COUNT(DISTINCT transaction_id)` over the
whole bronze table therefore still equals the injected redelivery count, instead of
counting collisions between two streams that happened to share a seed.

--- ONE COMPANY UNIVERSE, SHARED ---------------------------------------------------

`POOL_SIZE` and `POOL_SEED` are module constants rather than profile fields. A
profile describes a DELIVERY -- what was sent and how badly -- not a different
economy: the same companies pay each other across all three streams, which is what
makes a cross-profile join mean anything and what lets the 100%-resolution
measurement be one claim about one pool rather than three.
"""
from __future__ import annotations

from dataclasses import dataclass

from opl.generator.cnpj_pool import pool_query, validated_pool
from opl.generator.defects import (
    LATENESS_WINDOW_MS,
    NO_DEFECTS,
    DefectSpec,
    _require_defects_fit,
)
from opl.generator.stream import StreamSpec

# How many real `hub_empresa` keys every generated stream draws its counterparties
# from. 1,024 against 69,062,849 in the hub: large enough that the attribute space
# (|pool| * (|pool|-1) * |amounts| * |currencies| * |methods| ~ 2.6e13) makes a
# forced re-derivation in `_distinct_attributes` vanishingly unlikely at this stream
# length, and small enough that `pool_query`'s ORDER BY over the whole hub returns a
# top-N rather than a full sort of 69M rows.
POOL_SIZE = 1024

# The spread seed for that draw. Not any stream's `seed`: it selects WHICH companies
# exist in this lakehouse's payment world, which is a fact about the fixture rather
# than about a delivery, and `pool_query` hashes it into the ordering so that
# changing it re-draws the companies rather than reordering the same ones.
POOL_SEED = 20260812

# The job parameter default for `--params profile=...`, and a value `profile_for`
# refuses. Same shape and same reason as `opl.config.SENTINEL_MONTH` and
# `provenance.SENTINEL_REVISION`: a job parameter must have SOME default, no profile
# is a valid one, and so the default's whole job is to fail. A real profile here
# would make a forgotten `--params` generate and land a stream nobody asked for --
# under a filename that then refuses the stream that WAS asked for, because
# `emit_stream_file` will not overwrite a different file.
SENTINEL_PROFILE = "REQUIRED-PASS-A-PROFILE"


@dataclass(frozen=True, kw_only=True)
class StreamProfile:
    """One named stream: everything about it except which companies it draws from.

    Frozen and `kw_only` for `StreamSpec`'s reason, with the same sharp example:
    `event_interval_ms` and `emission_lag_ms` are adjacent, both `int`, both
    milliseconds, and a positional construction that swapped them would validate,
    reproduce, and describe a different world."""

    name: str
    stream_id: str
    seed: int
    event_count: int
    repeat_count: int
    window_start: str
    event_interval_ms: int
    emission_lag_ms: int
    defects: DefectSpec

    def stream_spec(self, cnpj_pool: tuple[str, ...]) -> StreamSpec:
        """This profile against a real pool. The pool is the only run-time input.

        `validated_pool` is applied HERE as well as at the caller, and that is not
        redundant: `StreamSpec` refuses a pool it would have had to normalise rather
        than normalising it silently, so a caller passing rows straight out of Spark
        -- in whatever order the partitions produced them -- would otherwise get a
        refusal one frame away from the query that caused it."""
        return StreamSpec(
            seed=self.seed,
            stream_id=self.stream_id,
            event_count=self.event_count,
            repeat_count=self.repeat_count,
            window_start=self.window_start,
            event_interval_ms=self.event_interval_ms,
            emission_lag_ms=self.emission_lag_ms,
            cnpj_pool=validated_pool(cnpj_pool),
        )

    @property
    def delivered_row_count(self) -> int:
        """How many LINES the landed file will hold, before any run happens.

        `event_count` plus the redeliveries: a duplicate is one extra row and a
        legitimate repeat is not (it is one of the `event_count`, carrying an earlier
        event's attributes under its own id). Lateness reorders and drift widens
        rows; neither adds one. This is a prediction, computable from the declaration
        alone, which is what §4's "publish predictions BEFORE the run" needs."""
        return self.event_count + self.defects.duplicate_count

    @property
    def drifted_row_count(self) -> int:
        """How many delivered rows will carry the undeclared drift column.

        Every emission index from `drift_from_index` onward, PLUS the redeliveries of
        those rows -- a redelivery carries its original's bytes, drift column and
        all. Zero when the profile declares no drift.

        THE DOCSTRING AND THE ARITHMETIC USED TO DISAGREE, AND THE TEST AGREED WITH THE
        BUG. `event_count - start` counts emission INDICES, which excludes the
        redeliveries the sentence above promises. `drifting` declares
        `duplicate_count=0`, so the published number was right by accident; a profile
        declaring BOTH would publish a prediction LOWER than the rows actually carrying
        the column -- and `tests/test_payment_emit.py` compares this against
        `drift_positions`, which is index-based too, **so the suite would have stayed
        GREEN while the prediction was wrong**. Found by review on PR #14.

        THE FIX IS A REFUSAL, NOT A LONGER SUM, and the choice is deliberate. Counting
        delivered rows needs `duplicate_positions`, which needs a full `StreamSpec`,
        which needs the CNPJ pool -- so a property that is honest about redeliveries
        stops being computable from the declaration alone, which is exactly what §4's
        "publish predictions BEFORE the run" asks of it. `_refuse_drift_beside_duplicates`
        makes the arithmetic below provably exact instead, and a future profile that
        genuinely wants both is told, in the refusal, what it has to implement first.
        This package does not half-support a case it has never tested."""
        start = self.defects.drift_from_index
        if start is None:
            return 0
        return self.event_count - start

    def pool_query(self) -> str:
        """The SQL that extracts this stream's counterparty pool. Not executed here."""
        return pool_query(size=POOL_SIZE, seed=POOL_SEED)


# The window every stream in this file happens in, and the shape that makes lateness
# mean something. 10,000 events five seconds apart is 13 hours 53 minutes -- THIRTEEN
# whole `LATENESS_WINDOW_MS` (one-hour) windows, 720 events each -- so a record
# delayed by one window is delivered BETWEEN punctual records rather than after all of
# them, which is what a real late arrival is and what a short stream cannot express
# (`tests/test_payment_defects.py` records why its own spec had to be lengthened for
# exactly this reason).
#
# TEN THOUSAND AND NOT FIFTY, AND THE REASON IS MEASURED. Generation is pure Python in
# the driver at roughly 1 ms per event (each one is several SHA-256 draws through
# `opl.vault.hashing.hash_key`), and it is done TWICE per stream: once to build the
# events, once more when `defects._require_the_injected_lateness_is_measurable` renders
# the delivery to check the injected late set against the measured one. At 50,000 that
# is 50 s per profile, measured on this dev box -- paid in the job on every run and, at
# the time, paid again on every import of this module by the guard below. Ten thousand
# is ~5 s, every count in this file stays exact, and nothing the phase claims depends
# on the fifth digit. The span is held constant by widening the interval instead, so
# the number of windows a late arrival crosses is unchanged.
_WINDOW_START = "2026-08-01T00:00:00.000Z"
_EVENT_COUNT = 10_000
_REPEAT_COUNT = 800
_EVENT_INTERVAL_MS = 5_000
_EMISSION_LAG_MS = 1_500


def _profile(name: str, stream_id: str, seed: int, defects: DefectSpec) -> StreamProfile:
    """One profile over the shared window. Only the four arguments differ.

    Written as a factory rather than three literal constructions, so that "these
    three streams differ ONLY in their id, their seed and their defects" is a
    property of the code rather than a claim about three blocks a reader has to
    diff. The shared fields are what makes the three comparable at all."""
    return StreamProfile(
        name=name,
        stream_id=stream_id,
        seed=seed,
        event_count=_EVENT_COUNT,
        repeat_count=_REPEAT_COUNT,
        window_start=_WINDOW_START,
        event_interval_ms=_EVENT_INTERVAL_MS,
        emission_lag_ms=_EMISSION_LAG_MS,
        defects=defects,
    )


PROFILES: dict[str, StreamProfile] = {
    "clean": _profile("clean", "F1B-CLEAN-2026-08", 20260813, NO_DEFECTS),
    "promotable": _profile(
        "promotable",
        "F1B-PROMOTABLE-2026-08",
        20260814,
        # 150 redeliveries and 100 late arrivals against 10,000 events: 1.5% and 1.0%,
        # which are big enough to be unmistakable in a count and small enough that the
        # stream still reads as a working payment feed with defects in it rather than
        # as a defect fixture. `late_by_ms` is exactly one window, the floor
        # `_require_the_delay_matches_the_count` enforces, so every late record crosses
        # a boundary a windowed aggregation had already closed.
        DefectSpec(duplicate_count=150, late_count=100, late_by_ms=LATENESS_WINDOW_MS),
    ),
    "drifting": _profile(
        "drifting",
        "F1B-DRIFTING-2026-08",
        20260815,
        # The producer starts sending `payment_channel` at index 8,000: 8,000 rows
        # before the change and 2,000 after it, in one file. Mid-stream by a long way
        # in both directions, which is what makes both populations countable -- and the
        # pre-drift 8,000 are exactly the rows a `COUNT(DISTINCT ..., payment_channel)`
        # would silently drop, this repository's own 8,761-row defect very nearly to
        # the row.
        DefectSpec(drift_from_index=8_000),
    ),
}


def profile_for(name: str) -> StreamProfile:
    """The declared profile called `name`, or refuse naming the valid ones.

    Refuses BEFORE Spark, like `table_spec` and `require_month`: nothing about an
    unknown profile needs a serverless session to diagnose, and the pool query it
    would otherwise reach is a sort over 69M rows.

    THE SENTINEL IS REFUSED AS AN ABSENCE, not as an unknown name, because that is
    what it is: reaching here with it means the run was launched without
    `--params profile=...`. Same treatment `require_month` gives `SENTINEL_MONTH`."""
    if name == SENTINEL_PROFILE or not name.strip():
        raise ValueError(
            "refusing to generate: no profile was given, and there is no default to "
            f"fall back on. The profile decides the seed, the size and the DEFECTS of "
            f"the stream that will be written into the Volume and ingested into "
            f"bronze, so a guessed one is a wrong stream landed under a filename that "
            f"then refuses the right one. Pass --params profile=<one of "
            f"{', '.join(sorted(PROFILES))}>."
        )
    try:
        return PROFILES[name]
    except KeyError:
        raise ValueError(
            f"unknown stream profile {name!r} -- declared profiles are: "
            f"{', '.join(sorted(PROFILES))}. Profiles are declared in "
            "opl.generator.profiles so that every number a generated stream depends "
            "on is in the repository before the run, rather than in a job parameter."
        ) from None


def _assert_the_profiles_are_declared_consistently() -> None:
    """Fail at import if a profile's key, name or stream id is not its own.

    THREE DISTINCT SILENT FAILURES, which is why one guard checks three things.

    A KEY THAT DISAGREES WITH ITS `name` makes the operator-facing parameter and the
    profile's own report describe different streams -- `profile_for("drifting")`
    returning something that logs itself as "promotable" is exactly the misdirection
    a hardcoded quarantine name once caused.

    A DUPLICATED STREAM ID IS THE DANGEROUS ONE. The id is the landing FILENAME
    (`opl.bronze.generated_landing.filename_for`), so two profiles sharing one would
    write to one path: the second run would find a different file already there and be
    refused -- loudly, but
    only after the first stream had landed and been ingested. It is also folded into
    every derivation purpose, so two profiles with one id and one seed are one stream
    under two names.

    A DUPLICATED SEED is not refused, deliberately: seed and stream id are hashed
    together, so two profiles may share a seed and still produce disjoint streams.
    Refusing it would forbid a legitimate declaration in order to police a field that
    is only half of the key."""
    for key, profile in PROFILES.items():
        if profile.name != key:
            raise ValueError(
                f"PROFILES[{key!r}] is named {profile.name!r}. The key is what an "
                "operator passes as --params profile=..., and the name is what the "
                "run reports; two spellings send a reader to the wrong stream."
            )
    ids = [profile.stream_id for profile in PROFILES.values()]
    if len(set(ids)) != len(ids):
        raise ValueError(
            f"two profiles share a stream_id: {sorted(ids)}. The stream id is the "
            "landing filename AND half of every derivation purpose, so a shared one "
            "means two profiles write to one path and derive from one namespace."
        )


def _assert_every_profile_describes_a_stream_that_can_exist() -> None:
    """Fail at import if a declared profile could not be generated.

    AT IMPORT, AND AGAINST A PROBE POOL, because the real pool needs a Spark session
    and a 69M-row table -- so the alternative is that a mistyped `late_by_ms` or an
    out-of-range `drift_from_index` is discovered by the job, in the workspace, after
    the revision guard, the session start and the pool query have all succeeded.

    THE PROBE POOL IS THE SMALLEST LEGAL ONE. `StreamSpec.__post_init__` validates the
    seed, the counts, the window and the intervals without generating anything, and
    `_require_defects_fit` adds the two cross-checks between the two specs: a delay
    that does not exceed the event interval disturbs no ordering, and a drift index
    past the end never drifts.

    `_require_defects_fit` is imported private, the way `registry.py` imports the
    guards in `registry_collisions.py`: it is the only spelling of those two rules, and
    a second copy here would be a second thing to drift from the module that owns the
    definition.

    WHAT IS DELIBERATELY NOT DONE HERE, AND WHERE IT IS DONE INSTEAD. The remaining
    bound -- that a defect count is not larger than the set of positions eligible for
    it -- is enforced by `defects._selected`, which raises naming the purpose and both
    numbers. Checking it here would mean calling `duplicate_positions`,
    `late_positions` and `drift_positions`, each of which digests EVERY candidate
    position: nine sorts of 10,000 SHA-256 draws, about two seconds, paid on every
    import of this module by every test that touches the generator. It is checked once
    in `tests/test_payment_emit.py` instead, which is where an expensive total check
    belongs, and at generation time it fails before a byte is written."""
    probe = validated_pool(("00000001", "00000002"))
    for profile in PROFILES.values():
        _require_defects_fit(profile.stream_spec(probe), profile.defects)


def _refuse_drift_beside_duplicates() -> None:
    """Fail at import if a profile declares drift AND duplicates in the same stream.

    `drifted_row_count` counts emission INDICES from `drift_from_index` onward. A
    redelivery carries its original's bytes -- drift column included -- so a profile
    declaring both would deliver MORE rows carrying the column than it predicts, and
    `tests/test_payment_emit.py` compares the prediction against `drift_positions`,
    which is index-based in the same way. **The suite would stay green while the
    published number was wrong**, which is the one failure this phase's whole method is
    built to prevent. Found by review on PR #14.

    REFUSED RATHER THAN SUMMED, because the honest sum needs `duplicate_positions`,
    which needs a full `StreamSpec`, which needs the 69M-row CNPJ pool -- and a
    prediction that cannot be computed from the declaration alone is no longer the kind
    of prediction §4 asks to be published before the run. Nothing needs the combination
    today: the drifting profile ends red at the DQ gate and never promotes, so pairing
    its rows with duplicates would prove nothing that the promotable profile does not
    already prove.

    A profile that genuinely wants both must first make `drifted_row_count` count
    delivered rows, and this message says so rather than leaving the next author to
    rediscover the trap."""
    for name, profile in PROFILES.items():
        drifts = profile.defects.drift_from_index is not None
        repeats = profile.defects.duplicate_count > 0
        if drifts and repeats:
            raise ValueError(
                f"profile {name!r} declares drift (from index "
                f"{profile.defects.drift_from_index}) beside "
                f"{profile.defects.duplicate_count} duplicate(s). `drifted_row_count` "
                "counts emission indices, so it would under-predict the delivered rows "
                "carrying the drift column -- and the test that guards it compares "
                "against index-based positions, so it would not go red. Make "
                "`drifted_row_count` count DELIVERED rows before declaring both"
            )


_assert_the_profiles_are_declared_consistently()
_assert_every_profile_describes_a_stream_that_can_exist()
_refuse_drift_beside_duplicates()
