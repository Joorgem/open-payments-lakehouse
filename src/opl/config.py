"""Central lakehouse coordinates. Single source of truth for catalog/schema and
UC Volume paths (Databricks Free Edition ships only workspace.default — no main).
Frozen: config is data, never mutated in place."""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class OplConfig:
    catalog: str = "workspace"
    schema: str = "default"
    landing_volume: str = "landing"
    # READ `require_month` BELOW BEFORE REACHING FOR THIS as an entry point's
    # default. It is a convenience for path helpers whose caller has already
    # decided the month; it is NOT an answer to "the operator did not say".
    #
    # Four entry points had written this exact substitution by F1.4 --
    # `bronze_ingest`, `bronze_lookup_ingest`, `unzip_table` and
    # `reclaim_landing` -- after `add_audit_columns` had already refused it by
    # giving `snapshot_month` no default. It keeps coming back because
    # `DEFAULT.month` is the path of least resistance for anything that takes a
    # month, and because it never LOOKED wrong: this pinned value EQUALLED the job
    # YAMLs' own default until F1.4b PR B, so substituting it was invisible until the
    # first run for another month, and then it is wrong in data (`_snapshot_month`) or
    # in a delete boundary, with nothing in the log naming the month that was used.
    # Those YAMLs now default to `SENTINEL_MONTH`, so a substitution here no longer
    # hides in a no-op -- it silently replaces `require_month`'s refusal.
    month: str = "2026-06"

    @property
    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.landing_volume}"

    @property
    def landing_cnpj_root(self) -> str:
        return f"{self.volume_root}/cnpj"

    def landing_cnpj_month(self, month: str | None = None) -> str:
        return f"{self.landing_cnpj_root}/{month or self.month}"

    def landing_zips(self, table: str, month: str | None = None) -> str:
        return f"{self.landing_cnpj_month(month)}/zips/{table}"

    def landing_table(self, table: str, month: str | None = None) -> str:
        return f"{self.landing_cnpj_month(month)}/{table}"

    def landing_tmp(self, table: str, month: str | None = None) -> str:
        """Where a writer may stage a half-written file before it is a landed file.

        DELIBERATELY OUTSIDE `landing_cnpj_root`, and therefore outside every dir an
        Auto Loader reads: every stream reads its own `landing_table(...)` subdir with
        NO glob, and cloudFiles walks a source dir RECURSIVELY (empirically -- an F1.3
        probe planted in the `zips/` subdir was ingested by a stream reading the month
        root). A temporary anywhere under a watched dir would depend on a glob to stay
        invisible; one under `volume_root` cannot be reached by any source path at all.
        It sits beside `_schemas/` and `_checkpoints/` (see `opl.bronze.autoloader`),
        the same convention for state that lives in the Volume but is not data.

        STILL THE SAME FILESYSTEM as `landing_table`, which is what makes
        `os.replace` from here into there work: one UC Volume is one FUSE mount, so
        every path under `volume_root` is renameable onto every other. A system temp
        dir (`/tmp`, `%TEMP%`) is a different device and `os.replace` across devices
        raises `EXDEV` -- so "somewhere else" is not enough, it has to be here.

        Mirrors the landing layout (`<month>/<table>`) so an operator listing this
        tree can map each staging dir 1:1 onto the landing dir it feeds."""
        return f"{self.volume_root}/_tmp/cnpj/{month or self.month}/{table}"

    # --- the SECOND landing root: sources this lakehouse generates ------------------
    #
    # A SECOND ROOT RATHER THAN A RENAME OF THE FIRST, decided in
    # `.plans/2026-08-10-master-route-and-autonomy-protocol.md` §6 and restated here
    # because the alternative keeps looking cheaper than it is. `landing_cnpj_root` is
    # interpolated into every CNPJ landing, zips, tmp and reclaim path, into two live
    # Auto Loader source directories, and into the evidence documents that record where
    # 144M rows came from; renaming it to `landing_source_root` would touch all of that
    # to gain a word. What payments actually need is a root that is NOT `cnpj/`, and
    # adding one costs nothing that already exists.
    #
    # `generated/` AND NOT `payments/`, though payments are its only tenant today. The
    # discriminator is the LANDING MODE (`opl.bronze.registry_landing.LANDING_GENERATED`)
    # and not the table: what these files have in common is that no downloader produced
    # them, so a second generated source -- F5 replays the same stream through Redpanda
    # -- lands beside payments under one root instead of forcing a third one. The root is
    # a property of how the bytes arrive, exactly as `zips/` is.
    #
    # THE MONTH IS REQUIRED HERE AND FALLS BACK NOWHERE. `landing_cnpj_month` reads
    # `month or self.month`, which is the substitution `require_month` exists to refuse
    # and which four entry points wrote by accident; it keeps that fallback only because
    # its callers predate the guard. These helpers have no legacy call site to preserve,
    # so they ask `require_month` themselves -- which also closes the empty-string hole
    # `opl.bronze.autoloader._assert_source_dir_is_this_months` documents at length,
    # where `month=""` yields `generated//payments` and collapses onto the month root.
    @property
    def landing_generated_root(self) -> str:
        return f"{self.volume_root}/generated"

    def landing_generated_month(self, month: str) -> str:
        return (
            f"{self.landing_generated_root}/"
            f"{require_month(month, action='locate the generated landing month')}"
        )

    def landing_generated_table(self, table: str, month: str) -> str:
        """The directory a generated table's Auto Loader reads for `month`."""
        return f"{self.landing_generated_month(month)}/{table}"

    def landing_generated_tmp(self, table: str, month: str) -> str:
        """Where a generated table's writer may stage a half-written file.

        The twin of `landing_tmp`, under the same `_tmp/` root and for the same two
        reasons: it is OUTSIDE every directory an Auto Loader reads, so a partial file
        cannot be discovered by the stream that is about to read the finished one; and
        it is on the SAME UC Volume, which is one FUSE mount, so `os.replace` from here
        into `landing_generated_table` works where a system temp dir would raise EXDEV.
        `opl.bronze.unzip_volume` has been doing exactly this on Databricks since F1.3,
        which is why the pattern is reused rather than invented.

        Mirrors the landing layout (`<month>/<table>`) so an operator listing this tree
        maps each staging dir 1:1 onto the landing dir it feeds."""
        return (
            f"{self.volume_root}/_tmp/generated/"
            f"{require_month(month, action='locate the generated staging dir')}/{table}"
        )

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"


DEFAULT = OplConfig()

# A month is `YYYY-MM` NAMING A REAL MONTH and nothing else. Anchored, and explicit
# `[0-9]` rather than `[^/]` or `\d`, because this value is interpolated raw into the
# landing path every task above builds: `[^/]` would admit a backslash, and `\d`
# admits non-ASCII decimal digits, which name no directory the RFB ever wrote.
#
# THE RANGE (`01`-`12`) IS IN HERE, not in a caller, and that placement is the whole
# point. It was in a caller: `opl.bronze.snapshot.ref_date_column` spelled it as
# `1 <= int(month) <= 12`, so `2026-13` was refused for the two ingest tasks and
# accepted for `unzip_table` and `reclaim_landing`, which call `require_month`
# directly and never reach that function. Reclaim is where that costs something: the
# month IS the delete boundary, so an impossible month scopes containment to a
# directory that cannot exist, every proven file reads as outside it, and the task
# prints REFUSED for all of them and exits green -- a log indistinguishable from the
# containment guard catching a real F1.3 probe.txt incident. This branch already
# fixed that exact failure shape once, for an ABSENT month; the out-of-range case
# survived it because the check lived in a second place instead of in the guard all
# four entry points route through. `ref_date_column` now asks `is_month` rather than
# re-spelling the rule.
_MONTH = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")


def is_month(value: str) -> bool:
    """Is `value` a `YYYY-MM` naming a real month? THE one spelling of that rule.

    Public so `opl.bronze.snapshot.ref_date_column` can ask it instead of carrying a
    second implementation -- the two callers refuse for different REASONS (a wrong
    landing path and delete boundary here, an all-NULL derived column there) and so
    keep their own messages, but there is one predicate behind both. Two spellings of
    one rule is how `2026-13` came to be refused at two of four entry points."""
    return _MONTH.match(value) is not None


# The `month` job-parameter default in every ingestion job YAML, and a value
# `require_month` refuses BELOW as an absence. Same shape and same reason as
# `opl.bronze.provenance.SENTINEL_REVISION` and `opl.bronze.promote.SENTINEL_BATCH_ID`:
# a job parameter must have SOME default, no month is a valid one, and so the
# default's whole job is to fail.
#
# WHAT THE REAL MONTH THERE COST, and it is not the F1.2 stamping argument the pinned
# `month` above already carries. Until F1.4b PR B the four YAMLs defaulted to
# `"2026-06"`, and a run launched without `--params month=...` read that month's
# landing dir against a checkpoint that already recorded every one of its files -- a
# no-op, which is why the default looked harmless for two phases. Month-scoping the
# Auto Loader state (`opl.bronze.autoloader.checkpoint_location`) turned the same
# launch into a duplicate append: the month-scoped checkpoint is EMPTY, so all of
# 2026-06 is new, the rows land in staging under a fresh `_batch_id`, and
# `promote.rows_of_batch` keys idempotence on `_batch_id` -- so it cannot see the
# duplication and carries it into bronze. Nothing fails; the row counts double.
#
# `tests/test_job_yaml_wiring.py` asserts each YAML default is this exact value, for
# the reason the revision sentinel is asserted the same way: a default that happened
# to be a real month would put every un-parameterised run back on that path.
SENTINEL_MONTH = "REQUIRED-PASS-A-MONTH"


# THE SESSION TIMEZONE, PINNED, AND IT IS A CORRECTNESS SETTING RATHER THAN A
# PREFERENCE. A Spark `TIMESTAMP` is an INSTANT stored as UTC micros, and every
# conversion into or out of one -- `CAST(date AS timestamp)`, `CAST('1970-01-01
# 00:00:00' AS timestamp)`, `date_format` -- resolves through this setting. Nothing
# declares it today: local Spark inherits the operating system's zone (America/Sao_Paulo
# on this project's dev box) and Databricks serverless defaults to UTC, so the SAME code
# over the SAME rows writes instants three hours apart in the two places.
#
# WHAT THAT COSTS, MEASURED. `opl.gold.dimensions` turns an RFB `applied_date` (a DATE)
# into a version's `valid_from`, and the fact asks for a version as of an `event_time`
# the payment contract stamps in UTC (`...T00:00:00.000Z`). Under a UTC-3 session the
# 2026-06-13 boundary is written as 2026-06-13T03:00:00Z, so every payment in those
# three hours resolves to the version BEFORE the one it belongs to -- a wrong answer
# with nothing to see in any log. The RFB publishes a day, not an hour, and the only
# reading that agrees with the payment stream's own clock is midnight UTC.
#
# IT ALSO FIXES THE FLOOR AT EXACTLY THE EPOCH. `opl.gold.columns.VALID_FROM_FLOOR` is
# 1970-01-01 00:00:00, which under this pin is 0 micros on every machine. Written in the
# driver's own zone instead it is a NEGATIVE epoch value anywhere east of Greenwich, and
# both halves of pyspark's Python conversion refuse one on Windows -- measured, this
# box: `F.lit(datetime(1969, 6, 15))` raises `OverflowError` from `time.mktime`, and
# collecting a negative timestamp raises `OSError [Errno 22]` from
# `datetime.fromtimestamp`. See `opl.gold.dimensions.instant_literal`.
#
# TWO CONSTANTS AND NOT ONE, because both halves are spelled in more than one file: the
# key is set on a session in `opl.spark.local_session` and in each gold entry point, and
# a typo in it is a `conf.set` that silently configures nothing.
SESSION_TIMEZONE_CONFIG = "spark.sql.session.timeZone"
SESSION_TIMEZONE = "UTC"


def require_month(month: str | None, *, action: str) -> str:
    """The month `action` will work in, or refuse: absent and malformed both.

    LIVES HERE, beside the pinned `month` it exists to stop being substituted,
    and not in the module that first needed it. Four entry points need identical
    behaviour -- `bronze_ingest`, `bronze_lookup_ingest`, `unzip_table` and
    `reclaim_landing` -- and all four already import `opl.config`, so this adds no
    dependency edge to any of them. Putting it in `opl.bronze.retention`, where it
    started, would have made three tasks that only ever READ files import the
    module that deletes them in order to validate an argument, which reads as a
    far stranger thing than it is.
    Modelled on `require_batch_id`, down to `action`: the message has to name the
    task that is refusing, or it misreports where the run stopped.

    ABSENCE IS REFUSED, NOT DEFAULTED, and that is the decision this function
    exists to hold. `add_audit_columns` already made it for `snapshot_month`,
    which was deliberately given no default because either candidate is wrong --
    `opl.config`'s pinned month is how F1.2 tied every row to 2026-06 silently,
    and the current month invents a fact. Both ingest tasks then read that pinned
    month into a local and handed it straight to that parameter, which satisfied
    the guard with exactly the value it was built to refuse and made the
    no-default decorative. A decorative guard is worse than none: the next reader
    sees it and believes the hole is closed.

    WHY A CRASH BEATS THE SUBSTITUTION, concretely. The pinned month EQUALLED the
    job YAMLs' own default until F1.4b PR B, so an omission changed nothing
    observable; now those YAMLs default to `SENTINEL_MONTH` and a substitution
    silently replaces this refusal instead of vanishing into a no-op. On
    the first run for another month an ingest stamps `_snapshot_month = 2026-06`
    on rows read from the 2026-06 landing dir, and a reclaim scopes containment
    to 2026-06, reports every proven file as REFUSED and exits green. Neither
    log nor data distinguishes that from a correct run.

    MALFORMED IS THE SECOND HALF, and it is refused here rather than left to the
    caller's containment check because this value is interpolated RAW into
    `landing_table(...)`: it is not checked BY that path, it is half OF it.
    `month="2026-06/zips"` makes the path `cnpj/2026-06/zips/<table>` -- the ZIPS
    DIRECTORY -- which points an ingest's stream at the raw archives and moves a
    reclaim's delete boundary onto them. The zips are the only way back to the
    source when a parse defect is found after ingestion, which happened twice in
    F1.3. Same framing as `registry._assert_subdirs_are_single_path_components`:
    a month names ONE directory under `cnpj/`; it is not a path.

    MALFORMED INCLUDES OUT OF RANGE, which is the half that shape alone misses.
    `2026-13` and `2026-00` are `YYYY-MM`-shaped and name no directory that can
    exist, so they land on the WRONG-BUT-WELL-FORMED path above rather than being
    caught by it: a reclaim scopes containment to a month directory nobody ever
    wrote, reports every proven file REFUSED and exits green. See `_MONTH` for why
    the range is in the regex here and not in a caller.

    THE SENTINEL IS THE THIRD SHAPE OF ABSENCE, and it is the one an operator will
    actually hit. `SENTINEL_MONTH` is what the ingestion job YAMLs default `month` to,
    so reaching here with it means the run was launched without `--params month=...`.
    It is refused as an ABSENCE rather than as a malformed value -- which is the truth
    of it, and which is also how `require_batch_id` treats its own sentinel -- so the
    message is the one that names the missing parameter instead of explaining what a
    month looks like. It would be refused by `is_month` regardless; naming it here is
    what makes the refusal a decision rather than a shape check getting lucky.

    Refuses BEFORE Spark, like `table_spec` and `require_batch_id`: nothing about
    an absent or malformed month needs a serverless session to diagnose."""
    candidate = month or ""
    if not candidate.strip() or candidate == SENTINEL_MONTH:
        raise ValueError(
            f"refusing to {action}: no month was given, and there is no default to "
            "fall back on. The month selects the ONE directory under cnpj/ this task "
            "works in, and for an ingest it is also the value stamped into every "
            "row's _snapshot_month -- so a guessed month is a wrong answer written "
            "into the data or into a delete boundary, not a missing one. The config's "
            "pinned month is the worst guess available: it EQUALLED the job YAMLs' own "
            "default until F1.4b PR B, so substituting it stayed invisible until the "
            "first run for another month -- and now that those YAMLs default to a value "
            "this guard refuses, substituting it no longer disappears into a no-op, it "
            "silently replaces THIS refusal with a run against 2026-06. Pass the "
            "SAME month the rest of the flow was given -- in the job YAMLs that is "
            "{{job.parameters.month}}."
        )
    if not is_month(candidate):
        raise ValueError(
            f"refusing to {action}: month={month!r} is not a month. It has to be "
            "YYYY-MM with MM in 01-12 (e.g. 2026-06), because it names ONE directory "
            "under cnpj/ and is interpolated raw into the landing path this task reads "
            "from or deletes under -- it is not a path, and a value with a separator in "
            "it moves that path onto another directory. '2026-06/zips' points it at the "
            "raw ZIPs, which are the only way back to the source. '2026-13' has the "
            "shape and names no directory that can exist, so a reclaim scopes its "
            "delete boundary to a month nobody ever wrote, reports every proven file "
            "REFUSED and exits green. Pass the same month the rest of the flow was "
            "given."
        )
    return candidate
