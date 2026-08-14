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

--- WHY THERE ARE THREE DEFECT PROFILES, AND WHY THAT IS FORCED RATHER THAN CHOSEN --

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

--- AND A FOURTH, WHICH CARRIES NO DEFECT AND EXISTS TO PLACE PAYMENTS IN TIME ------

F3's headline is a fact-to-dimension join AS OF THE PAYMENT'S EVENT TIME, and a join
that cannot give two answers proves nothing. F3 Task 0 measured the payments already
in bronze: every one of the 20,150 rows sits on 2026-08-01, AFTER both of the CNPJ
vault's `applied_date`s (2026-06-13 and 2026-07-11), so every as-of lookup resolves to
the LAST version and the join is bit-identical to `WHERE valid_to = <sentinel>` --
which is the worse of the two possible failures, because nothing distinguishes its
answer from the naive one.

    BETWEEN-SNAPSHOTS  the same shape, the same pool and the same absence of defects
                       as CLEAN, moved to a window lying ENTIRELY between the two
                       `applied_date`s. The three above are the "after" side; this one
                       is the "before" side, and only the two together make the as-of
                       answer differ from the naive one. It carries no defect on
                       purpose: it exists to place payments in time, not to add a
                       fourth defect class, so every count F1b published about
                       `bronze_payments` survives its arrival unchanged.

--- AND A FIFTH, WHICH CARRIES TWO CURRENCIES SO THAT FX CAN BE WRONG ---------------

F-API's headline is an FX rate joined to a payment as of the instant the rate was
PUBLISHED, and every payment this lakehouse held before it was BRL -- so `fx_rate` would
be 1.0 on every row, `amount_brl` would equal `amount` everywhere, and `dim_currency`
would stay a constant column wearing a dimension's name. An FX layer measured on that
cannot be wrong, which is the same thing as saying nothing about it is testable.

    CROSS-CURRENCY  the same shape, the same pool and the same absence of defects as
                    CLEAN, drawing from ("BRL", "USD") instead of ("BRL",) and placed at
                    2026-06-22T08:00Z so that its window STRADDLES that day's PTAX
                    bulletin. Two properties no other profile can produce: a currency
                    that varies WITHIN one stream rather than between streams, and two
                    payments on one calendar day resolving to two different rates.

THE CURRENCY IS A PROPERTY OF THE PAYMENT AND NOT OF THE DELIVERY, which is why it is a
tuple here rather than one currency per profile. A per-profile constant would give
`dim_currency` two members too -- and it would be perfectly correlated with `_batch_id`,
so any `GROUP BY currency` would be a `GROUP BY batch` under another name. See the
declaration block below for the arithmetic of the window and for why the tuple is a
literal rather than `payments.CURRENCIES`.

THEIR STREAM IDS AND SEEDS DIFFER, WHICH IS LOAD-BEARING. `stream._transaction_id`
folds the stream id into its derivation purpose, so each profile produces its own
disjoint identity space -- and `COUNT(*) - COUNT(DISTINCT transaction_id)` over the
whole bronze table therefore still equals the injected redelivery count, instead of
counting collisions between two streams that happened to share a seed.

--- ONE COMPANY UNIVERSE, SHARED ---------------------------------------------------

`POOL_SIZE` and `POOL_SEED` are module constants rather than profile fields. A
profile describes a DELIVERY -- what was sent, how badly, and when -- not a different
economy: the same companies pay each other across every stream, which is what
makes a cross-profile join mean anything and what lets the 100%-resolution
measurement be one claim about one pool rather than one per stream. It is also the
whole force of `between-snapshots` and `cross-currency`: an as-of join across the five
streams compares the SAME companies at two different times, which a second pool would turn
into a comparison of two populations.
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
from opl.generator.instants import from_text, to_text
from opl.generator.stream import DEFAULT_CURRENCIES, StreamSpec

# How many real `hub_empresa` keys every generated stream draws its counterparties
# from. 1,024 against 69,062,849 in the hub: large enough that the attribute space
# (|pool| * (|pool|-1) * |amounts| * |spec.currencies| * |methods| >= 2.6e13) makes a
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

# WHERE A STREAM'S WINDOW SITS RELATIVE TO THE VAULT'S TWO `applied_date`s, DECLARED BY
# THE PROFILE RATHER THAN INFERRED FROM IT.
#
# F3 asserted this for ONE profile by name -- the guard read `PROFILES[BETWEEN_SNAPSHOTS]`
# -- so a fifth profile inherited none of it and could sit anywhere without a word going
# red. The obvious generalisation is wrong: a blanket "every profile sits between the two
# dates" would refuse the F1b three, which legitimately sit AFTER both and are the "after"
# side the as-of join needs. There is no single correct placement, so the placement becomes
# part of the declaration and the guard checks the declaration against the arithmetic.
#
# `before` HAS NO DECLARED PROFILE TODAY and is not speculative padding: it is the third of
# three exhaustive positions, and leaving it out would make the guard's dispatch a
# two-branch check that silently accepts anything below the earlier date. It is exercised
# in `tests/test_payment_profiles.py` rather than by a declaration, and §7 of the phase plan
# records that distinction rather than reporting the branch as working.
PLACEMENT_BEFORE = "before"
PLACEMENT_BETWEEN = "between"
PLACEMENT_AFTER = "after"
PLACEMENTS = (PLACEMENT_BEFORE, PLACEMENT_BETWEEN, PLACEMENT_AFTER)


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
    currencies: tuple[str, ...]
    placement: str
    defects: DefectSpec

    def __post_init__(self) -> None:
        """Refuse a placement that is not one of the three, AT CONSTRUCTION.

        Here rather than only in the import guard below, because the guard compares a
        DECLARED placement against the one the window actually has -- and an unrecognised
        spelling would make that comparison fail with a message about a window, when the
        mistake is a typo in a word. `dataclasses.replace` reaches this too, so a doctored
        profile in a test cannot invent a fourth placement either."""
        if self.placement not in PLACEMENTS:
            raise ValueError(
                f"profile {self.name!r} declares placement {self.placement!r}, which is "
                f"not one of {PLACEMENTS}. The placement says where this stream's window "
                "sits relative to the vault's two applied_dates, and the guard at the foot "
                "of this module refuses a window that contradicts it."
            )

    def stream_spec(self, cnpj_pool: tuple[str, ...]) -> StreamSpec:
        """This profile against a real pool. The pool is the only run-time input.

        `validated_pool` is applied HERE as well as at the caller, and that is not
        redundant: `StreamSpec` refuses a pool it would have had to normalise rather
        than normalising it silently, so a caller passing rows straight out of Spark
        -- in whatever order the partitions produced them -- would otherwise get a
        refusal one frame away from the query that caused it.

        `currencies` IS PASSED THROUGH AND THAT LINE IS LOAD-BEARING. `StreamSpec`
        defaults the field, so forgetting it here would silently generate a BRL-only
        stream out of a profile that declares a mix -- every count green, the FX layer
        back to a column that cannot be wrong. `placement` is deliberately NOT passed:
        it is a claim about where the window sits, checked at import, and the generator
        has no use for it."""
        return StreamSpec(
            seed=self.seed,
            stream_id=self.stream_id,
            event_count=self.event_count,
            repeat_count=self.repeat_count,
            window_start=self.window_start,
            event_interval_ms=self.event_interval_ms,
            emission_lag_ms=self.emission_lag_ms,
            cnpj_pool=validated_pool(cnpj_pool),
            currencies=self.currencies,
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
        the column -- and `tests/test_payment_profiles.py` compares this against
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

    @property
    def last_event_time(self) -> str:
        """The `event_time` of this stream's final event, before any run happens.

        `window_start` is the FIRST `event_time` by construction -- `stream._event_at`
        adds whole intervals to it and never subtracts -- so this and that field are
        the stream's event window, exactly. `event_count - 1` intervals rather than
        `event_count`, because index 0 sits ON the window start.

        WHY A PROFILE PUBLISHES ITS LAST INSTANT AT ALL. Where a stream sits in time is
        the only thing `between-snapshots` varies, and the only thing that makes it
        worth generating; a placement that can be asserted from the declaration is a
        placement that can be refused at import rather than discovered in a workspace
        as a join that quietly agreed with the naive one. Lateness moves `emitted_at`
        and never `event_time`, so this bound holds whatever a profile's defects
        say."""
        span = (self.event_count - 1) * self.event_interval_ms
        return to_text(from_text(self.window_start) + span)

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


# THE FOURTH WINDOW, AND THE INTERVAL IT HAS TO SIT INSIDE.
#
# The vault holds two `applied_date`s and nothing between them, so an as-of lookup has
# exactly two answers to choose from and a payment chooses the earlier one only by
# happening before the later date. 2026-06-20 puts the whole stream seven days after
# the first snapshot and twenty days before the second: 10,000 events 5,000 ms apart
# span 13 h 53 m 15 s, which is the F1b span unchanged, so the profile is CLEAN moved
# in time and nothing else. The containment is asserted at import by the guard at the
# foot of this file rather than left to this comment.
#
# WHY BETWEEN AND NOT ACROSS, AND THE REASON IS NOT A REFUSAL. A stream straddling the
# two dates would need `event_interval_ms = 241,944` (28 days over 9,999 intervals),
# and it would NOT be rejected: `_require_defects_fit` asks only that `late_by_ms`
# EXCEED the interval, and one lateness window is 3,600,000 ms -- fourteen times
# larger -- so a straddle could carry every defect class. What it would actually cost
# is the shared shape: 48x the other profiles' interval means the fourth stream is no
# longer the same delivery moved in time, and any difference an as-of join showed
# between it and them would have two candidate causes. Task 0 pre-decided the
# between-window on that basis (`docs/f3-run-evidence.md` 0.3) and the existing streams
# already supply the "after" side, so nothing needs the straddle. The arithmetic is
# recorded because the plan's own stated reason was a refusal that does not fire, and
# `tests/test_payment_profiles.py` pins both halves.
BETWEEN_SNAPSHOTS = "between-snapshots"
_BETWEEN_SNAPSHOTS_WINDOW_START = "2026-06-20T00:00:00.000Z"
_VAULT_APPLIED_DATES = ("2026-06-13T00:00:00.000Z", "2026-07-11T00:00:00.000Z")


# THE FIFTH PROFILE, AND THE ONLY ONE WHOSE CURRENCY IS A PROPERTY OF THE PAYMENT.
#
# WHY IT EXISTS AT ALL. Every payment this lakehouse held before it was BRL, so `fx_rate`
# would be 1.0 on every row of `fact_payment`, `amount_brl` would equal `amount`
# everywhere, and `dim_currency` would stay a constant column wearing a dimension's name.
# An FX layer measured on that data cannot be wrong, which is the same thing as saying
# nothing about it is testable. This stream is where FX becomes falsifiable.
#
# A TUPLE AND NOT A SCALAR, WHICH IS THE DECISION THAT MATTERS. Declaring one currency per
# profile -- BRL for four streams and USD for a fifth -- would also give `dim_currency` two
# members, and it would be wrong: the currency would be functionally determined by the
# delivery, perfectly correlated with `_batch_id`, so any `GROUP BY currency` would be a
# `GROUP BY batch` under another name. `payments.BUSINESS_ATTRIBUTE_COLUMNS` is "what the
# payment WAS, as opposed to which delivery of it this row is", and a per-delivery currency
# violates that sentence while passing every guard in this repository. So the mix is inside
# one stream.
#
# A LITERAL TUPLE AND NEVER `payments.CURRENCIES`, and this is the trap T1 exists to
# prevent, reproduced in the one place that would spring it. Writing
# `currencies=payments.CURRENCIES` reads as "draw from the domain" and would make this
# stream's bytes a function of the DOMAIN's length -- so a sixth currency added for some
# later phase would re-draw every row of this stream, which is exactly the coupling the
# domain/draw split was made to remove. The literal is the whole point.
#
# THE WINDOW IS THE FOURTH ONE PROPOSED AND THE FIRST THAT SURVIVES MEASUREMENT, and the
# three that did not are recorded in the phase plan rather than deleted. What it has to do
# is put payments on BOTH SIDES of a PTAX bulletin: the rate for a payment is the most
# recent quote whose PUBLICATION precedes the payment's own instant, so two payments on one
# calendar day, in one stream, in one currency, get DIFFERENT rates when one happens before
# that day's bulletin and one after. No calendar-day implementation can produce that.
#
#   - The 2026-06-22 quote publishes at `dataHoraCotacao 2026-06-22 13:06:19.750415`, read
#     as BRT, i.e. 2026-06-22T16:06:19.750415Z -- 29,179,750.415 ms after this window
#     opens. Events land on whole 5,000 ms steps, so indices 0..5,835 precede publication
#     and fall back to Friday 2026-06-19 (venda 5.14420) while 5,836..9,999 resolve
#     same-day (venda 5.13950). The 415 us remainder puts the boundary strictly between two
#     events, so no row sits on it and the `<=`-versus-`<` question decides nothing.
#   - THE WEEKEND CROSSING IS NOT LOST. Those 5,836 rows reach back past Sunday 2026-06-21
#     and Saturday 2026-06-20 to Friday 2026-06-19: a fallback across a whole weekend AND
#     across the unquoted part of a business day, on real fact rows.
#   - IT COSTS ONE `event_date_key` AND NOT TWO. The whole window sits inside 2026-06-22 in
#     BOTH UTC and BRT (08:00..21:53:15 Z is 05:00..18:53:15 BRT), so the distinct
#     `event_date_key` count goes 2 -> 3, and this profile's calendar day does not depend
#     on a timezone convention at all.
#   - 08:00 AND NOT 00:00, which is the only reason the window is not the F1b shape moved.
#     Opening at 00:00Z would close at 13:53:15Z -- 10:53 BRT, before the ~13:0x bulletin
#     hour -- so every row would fall back and the direct lookup would go unexercised for a
#     fifth time.
CROSS_CURRENCY = "cross-currency"
_CROSS_CURRENCY_WINDOW_START = "2026-06-22T08:00:00.000Z"
_CROSS_CURRENCY_CURRENCIES = ("BRL", "USD")


# --- WHAT THE FACTORY BELOW VARIES, AND WHY THAT PROSE IS UP HERE ---------------------
#
# Module level for the reason `opl.bronze.generated_landing` and `opl.bronze.rules` state
# above their own long functions: this reasoning grew past the point where the function
# carrying it stayed under the project's 50-line limit. F-API Task 3 added two parameters
# and the docstring took `_profile` to 57 lines, every one of the added lines prose -- which
# is the species §0.4 of the phase plan recorded twice, so it is repaired rather than
# repeated.
#
# THE INVARIANT, SPELLED SO IT CAN BE CHECKED RATHER THAN BELIEVED: the declared profiles
# differ in `name`, `stream_id`, `seed`, `defects`, `window_start`, `currencies` and
# `placement`, and share `event_count`, `repeat_count`, `event_interval_ms` and
# `emission_lag_ms`. Written as a factory rather than five literal constructions so that
# sentence is a property of the code -- the shared four cannot vary, because no argument
# exists that would let them -- rather than a claim about five blocks a reader has to diff.
# The shared shape is what makes the profiles comparable at all.
# `tests/test_payment_profiles.py::test_the_factory_varies_exactly_the_fields_its_docstring
# _names` reads this sentence's field names out of the signature and out of the dataclass,
# so the two cannot drift apart.
#
# `window_start` BECAME A PARAMETER AND THIS PROSE CHANGED IN THE SAME COMMIT, deliberately,
# and `currencies` and `placement` did the same one phase later. It was hardcoded while the
# sentence above read "differ ONLY in their id, their seed and their defects", and both
# halves were true; F3's `between-snapshots` profile makes that sentence false, and a factory
# whose docstring still described the old invariant is precisely the drift that let
# `drifted_row_count` promise redeliveries its arithmetic did not count while the suite
# stayed green (found by review on PR #14).
#
# TWO DEFAULTS AND ONE REQUIRED KEYWORD, AND THE ASYMMETRY IS THE POINT.
#
# THE DEFAULTS ARE WHAT KEEP F1b's BYTES WHERE THEY ARE. Not passing `window_start` or
# `currencies` is the identical construction the three F1b profiles already had, so their
# `StreamSpec`s -- and therefore their files, digests and every count published against
# them -- are untouched by the parameters existing. `currencies` defaults to a ONE-tuple,
# which is the whole of the byte-identity argument (see
# `opl.generator.stream.DEFAULT_CURRENCIES`).
#
# `placement` HAS NO DEFAULT, and that is a deliberate refusal to inherit. Every other field
# here describes what a stream IS; placement is a CLAIM about it that the guard at the foot
# of this module checks against the arithmetic, and a claim nobody made is not a claim that
# can be wrong. A default would also have to be `after` -- the F1b value -- so the one
# profile most likely to be added next, a new window somebody has not thought about, would
# silently inherit the assertion least likely to be true of it.


def _profile(
    name: str,
    stream_id: str,
    seed: int,
    defects: DefectSpec,
    *,
    placement: str,
    window_start: str = _WINDOW_START,
    currencies: tuple[str, ...] = DEFAULT_CURRENCIES,
) -> StreamProfile:
    """One profile over the shared shape. Only the parameters above differ.

    See the comment block above for the invariant this factory makes checkable, and for
    why two of its three keyword parameters have defaults and one does not."""
    return StreamProfile(
        name=name,
        stream_id=stream_id,
        seed=seed,
        event_count=_EVENT_COUNT,
        repeat_count=_REPEAT_COUNT,
        window_start=window_start,
        event_interval_ms=_EVENT_INTERVAL_MS,
        emission_lag_ms=_EMISSION_LAG_MS,
        currencies=currencies,
        placement=placement,
        defects=defects,
    )


PROFILES: dict[str, StreamProfile] = {
    "clean": _profile(
        "clean", "F1B-CLEAN-2026-08", 20260813, NO_DEFECTS, placement=PLACEMENT_AFTER
    ),
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
        placement=PLACEMENT_AFTER,
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
        placement=PLACEMENT_AFTER,
    ),
    BETWEEN_SNAPSHOTS: _profile(
        BETWEEN_SNAPSHOTS,
        # `F3-` rather than `F1B-` because the phase that needs this stream is the one
        # that names it, and no month suffix: the F1b ids carry one because their
        # window month and their landing month coincided, and this stream's do not --
        # its events are in June and it lands wherever the run's `--params month=...`
        # says. A suffix that agreed with only one of the two would misdirect an
        # operator listing the Volume, which is the same class of harm
        # `_assert_the_profiles_are_declared_consistently` refuses for a wrong `name`.
        "F3-BETWEEN-SNAPSHOTS",
        20260816,
        # NO DEFECTS, DECLARED RATHER THAN OMITTED. This profile exists to place
        # payments in time. A fourth defect class would move F1b's published totals --
        # 150 redeliveries, 100 late arrivals, 2,000 drifted rows, every one of them
        # stated against `bronze_payments` as a whole -- so those sentences would
        # start describing a table they no longer describe, for no claim F3 makes. It
        # also keeps `delivered_row_count` equal to `event_count`, and a clean batch
        # promotes: a drifted one would stop at `fail_on_dq` and never reach the fact.
        NO_DEFECTS,
        window_start=_BETWEEN_SNAPSHOTS_WINDOW_START,
        placement=PLACEMENT_BETWEEN,
    ),
    CROSS_CURRENCY: _profile(
        CROSS_CURRENCY,
        # `F-API-` rather than `F3-`, on `between-snapshots`' own precedent: the phase that
        # needs the stream names it, and no month suffix, because its events are in June
        # and it lands wherever `--params month=...` says.
        "F-API-CROSS-CURRENCY",
        # The next seed in the sequence, and it must be new for the reason
        # `_assert_the_profiles_are_declared_consistently` gives about ids: seed and stream
        # id are hashed TOGETHER, so a reused seed under a new id is already a disjoint
        # stream -- but a sequence a reader can follow is worth more than the reuse.
        20260817,
        # NO DEFECTS, DECLARED RATHER THAN OMITTED, for `between-snapshots`' reason
        # exactly. F1b's published totals -- 150 redeliveries, 100 late arrivals, 2,000
        # drifted rows -- are all stated against `bronze_payments` AS A WHOLE, so a fifth
        # defect class would make those sentences describe a table they no longer describe,
        # for no claim this phase makes. It also keeps `delivered_row_count` equal to
        # `event_count`, and a clean batch promotes: a drifted one stops at `fail_on_dq`
        # and never reaches the fact, which is where the FX columns are.
        NO_DEFECTS,
        window_start=_CROSS_CURRENCY_WINDOW_START,
        currencies=_CROSS_CURRENCY_CURRENCIES,
        placement=PLACEMENT_BETWEEN,
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

    AND IT IS WHAT MAKES T1's CURRENCY GUARD REACH EVERY PROFILE, which is worth stating
    because the phase plan asked for a guard here and this is where it already is.
    `stream_spec` builds a real `StreamSpec`, whose `__post_init__` calls
    `stream._require_currencies` -- so a profile drawing from outside the contract's
    domain, repeating a member, or reordering it is refused AT IMPORT by the loop below.
    A second loop over `PROFILES` spelling the same rule is exactly the duplication this
    repository polices hardest, so there is not one.

    WHAT IS DELIBERATELY NOT DONE HERE, AND WHERE IT IS DONE INSTEAD. The remaining
    bound -- that a defect count is not larger than the set of positions eligible for
    it -- is enforced by `defects._selected`, which raises naming the purpose and both
    numbers. Checking it here would mean calling `duplicate_positions`,
    `late_positions` and `drift_positions`, each of which digests EVERY candidate
    position: nine sorts of 10,000 SHA-256 draws, about two seconds, paid on every
    import of this module by every test that touches the generator. It is checked once
    in `tests/test_payment_profiles.py` instead, which is where an expensive total check
    belongs, and at generation time it fails before a byte is written."""
    probe = validated_pool(("00000001", "00000002"))
    for profile in PROFILES.values():
        _require_defects_fit(profile.stream_spec(probe), profile.defects)


def _refuse_drift_beside_duplicates() -> None:
    """Fail at import if a profile declares drift AND duplicates in the same stream.

    `drifted_row_count` counts emission INDICES from `drift_from_index` onward. A
    redelivery carries its original's bytes -- drift column included -- so a profile
    declaring both would deliver MORE rows carrying the column than it predicts, and
    `tests/test_payment_profiles.py` compares the prediction against `drift_positions`,
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


def observed_placement(profile: StreamProfile) -> str | None:
    """Where `profile`'s window ACTUALLY sits, or None if it contains a boundary.

    STRICT AT EVERY END, and the strictness is the same argument at each of them. A
    payment landing exactly ON an `applied_date` matches two company versions under a
    half-open interval join, which is the multi-match F3's own acceptance forbids -- so a
    window touching a boundary is not "before" or "after" it, and this returns None.

    None IS A FOURTH ANSWER RATHER THAN A REFUSAL, because the guard below is what turns
    it into one. A window straddling a date is a real thing to declare one day (it needs
    48x the shared event interval, which is why F3 rejected it -- see
    `tests/test_payment_profiles.py`), and this function's job is to report the arithmetic,
    not to decide what is allowed.

    PUBLIC, unlike the guards around it: `tests/test_payment_profiles.py` reads it to check
    the three placements against windows the declarations do not contain, and `before`
    has no declared profile at all -- so without a public reader its branch would be
    unreachable code wearing a green suite."""
    earlier, later = (from_text(instant) for instant in _VAULT_APPLIED_DATES)
    first = from_text(profile.window_start)
    last = from_text(profile.last_event_time)
    if last < earlier:
        return PLACEMENT_BEFORE
    if earlier < first and last < later:
        return PLACEMENT_BETWEEN
    if later < first:
        return PLACEMENT_AFTER
    return None


def _refuse_a_window_that_contradicts_its_declared_placement() -> None:
    """Fail at import if any profile's window is not where the profile says it is.

    WHERE A STREAM SITS IN TIME IS THE ONLY THING SOME OF THESE PROFILES VARY. Move
    `between-snapshots`' window and it becomes a fourth copy of the F1b three that lands
    under its own filename, promotes without complaint, and makes the as-of join it was
    added for resolve to the same version the naive join does. Nothing goes red -- the
    phase simply publishes a join whose answer does not change, which is the exact failure
    F3 Task 0 measured and that profile answers. It holds for `cross-currency` too, and
    with a second cost: pushed past the later `applied_date` it stops being the "before"
    side, and every FX row joins the population that already resolves to the last company
    version -- so the one stream carrying two currencies would say nothing about as-of
    joining either.

    IT READS THE DECLARATION AND ITERATES `PROFILES`, WHICH IS F-API's CHANGE. F3's
    version read `PROFILES[BETWEEN_SNAPSHOTS]` -- one hardcoded key -- so the fifth
    profile inherited none of it. A blanket "every profile sits between the two dates"
    would have been worse than nothing: it refuses the F1b three, which legitimately sit
    AFTER both dates and are the "after" side the join needs. So each profile declares its
    placement and this compares that word against the arithmetic.

    A STRADDLE IS CAUGHT BY THE SAME COMPARISON RATHER THAN BY A FOURTH BRANCH:
    `observed_placement` returns None for a window containing a boundary, and None equals
    no declared placement, so the refusal below fires with the window in the message.

    FROM THE DECLARATION ALONE AND AT IMPORT, for the reason
    `_assert_every_profile_describes_a_stream_that_can_exist` gives: `window_start` and
    `last_event_time` are arithmetic, so this costs no session, no pool and no
    generated byte, and the alternative is discovering it after a deploy, a run and an
    ingest have all succeeded."""
    earlier, later = _VAULT_APPLIED_DATES
    for name, profile in PROFILES.items():
        observed = observed_placement(profile)
        if observed == profile.placement:
            continue
        raise ValueError(
            f"profile {name!r} declares placement {profile.placement!r} and runs "
            f"{profile.window_start} .. {profile.last_event_time}, which is "
            f"{observed or 'astride one of the two dates'} relative to the vault's "
            f"applied_dates ({earlier} .. {later}). A payment on the wrong side of those "
            "dates resolves to the same company version the other streams already do, so "
            "the as-of join stops being distinguishable from `WHERE valid_to = "
            "<sentinel>` and this profile proves nothing it was added to prove."
        )


_assert_the_profiles_are_declared_consistently()
_assert_every_profile_describes_a_stream_that_can_exist()
_refuse_drift_beside_duplicates()
_refuse_a_window_that_contradicts_its_declared_placement()
