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
    # month, and because it never LOOKS wrong: this pinned value equals the job YAMLs'
    # own default, so substituting it is invisible until the first run for
    # another month, and then it is wrong in data (`_snapshot_month`) or in a
    # delete boundary, with nothing in the log naming the month that was used.
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

    WHY A CRASH BEATS THE SUBSTITUTION, concretely. The pinned month equals the
    job YAMLs' own default, so an omission changes nothing observable today. On
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

    Refuses BEFORE Spark, like `table_spec` and `require_batch_id`: nothing about
    an absent or malformed month needs a serverless session to diagnose."""
    candidate = month or ""
    if not candidate.strip():
        raise ValueError(
            f"refusing to {action}: no month was given, and there is no default to "
            "fall back on. The month selects the ONE directory under cnpj/ this task "
            "works in, and for an ingest it is also the value stamped into every "
            "row's _snapshot_month -- so a guessed month is a wrong answer written "
            "into the data or into a delete boundary, not a missing one. The config's "
            "pinned month is the worst guess available: it equals the job YAMLs' own "
            "default, so the omission stays invisible until the first run for another "
            "month and nothing in the log names the month that was used. Pass the "
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
