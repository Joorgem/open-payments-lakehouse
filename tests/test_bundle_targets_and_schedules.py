# tests/test_bundle_targets_and_schedules.py
"""Which jobs declare a cadence, and WHO writes `pause_status`.

WHICH HALF IS ASSERTED HERE AND WHICH IS NOT -- read this before trusting the green.

  * ASSERTED, CREDENTIAL-FREE, IN CI: everything about the COMMITTED SOURCE. Every bundle
    document under `databricks/resources/` is classified scheduled or not, with a reason; a
    scheduled job declares a cron and a timezone and nothing else that fires it; an
    unscheduled one declares no `schedule`, `trigger` or `continuous`; and **no committed
    bundle file DECLARES `pause_status` at any depth** (the values, not the text -- the
    schedule comments explain the key, and the explanation is not the defect). Both targets
    are read out of `databricks/databricks.yml`, and `prod`'s root path must be interpolated
    rather than carry an operator's identity.

  * NOT ASSERTED IN CI: the RENDERING. `pause_status` is written by the CLI from the
    target's `mode`, and asking what it wrote means running `databricks bundle validate`,
    which needs credentials for this bundle -- it resolves the current user for the dev-mode
    prefix and a warehouse by `lookup:`. So the rendering arm skips on the CLI being absent
    and on a credential signature it RECOGNISES, and goes RED on anything else; `_rendered`
    carries that distinction and what it costs. It is not decoration: it is the only place
    the `PAUSED`/`UNPAUSED` split is observed rather than described, and it was run by hand
    on the tree that shipped it.

AND WHAT THE JUSTIFICATIONS THEMSELVES CLAIM, WHICH IS THE HALF THAT WAS MISSING. Until
F8's correction pass this module asserted only that a cron and a timezone EXIST, and F8's
reviewer reported moving PTAX to a Sunday small hour and rewriting every zone to UTC with
the suite still green -- neither mutation touches anything those assertions read, and each
falsifies the paragraph directly above the block it changes. So the properties those
paragraphs claim are asserted here, every expected value DERIVED from a file:

  * the three monthly tiers run in the order the comments give them -- bronze strictly
    before vault, vault strictly before gold -- on one shared day of the month;
  * a daily job fires strictly AFTER the publication band its own comment states, and
    on weekdays with no weekend day among them;
  * every schedule's `timezone_id` resolves to `opl.extraction.ptax_source.BRASILIA`'s
    offset all year -- the zone ADR 0016 Decision 2 reads the publication stamps in,
    and the reason these crons are not in UTC.

THE BAND IS PARSED OUT OF THE COMMENT ATTACHED TO THE JOB'S OWN `schedule:` KEY
(`band HH:MM-HH:MM`), so the number the cron has to beat cannot drift from the sentence
justifying it and cannot be satisfied by a sentence elsewhere in the file -- the search ran
over the WHOLE FILE, above a long header, until F8's second correction pass. A comment
rewrapped so `band` and the times fall on different lines fails LOUDLY rather than leaving
the hour resting on nothing, and a block stating TWO DIFFERENT bands is a fault rather than
a first-match. What that costs the author of a SUPERSEDED band is stated in the block it
costs it in, not here.

`zoneinfo` NEEDS A TZ DATABASE -- the system one on Linux, the `tzdata` package on
Windows. Where it is absent this file goes RED rather than skipping, because a zone
check that cannot resolve a zone has checked nothing.

WHAT LEFT THIS FILE. The resource allowlist moved to
`tests/test_bundle_resource_allowlist.py`, whole, with its arms: it answers neither of this
module's questions, and its own docstring carries what it covers and what it does not.

WHY THE SPLIT MATTERS AT ALL. `mode: development` renders `pause_status: PAUSED` and
`mode: production` renders `UNPAUSED`, from source that says neither. Write `pause_status`
into a job YAML and the target stops deciding: a schedule would then be unpaused under
`free` too, in a workspace where every guarded job refuses an unparameterised run (ADR
0021). The absence in the source is the mechanism, which is why absence is what is
asserted."""
from __future__ import annotations

import json
import re
import shutil
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
import yaml
from job_yaml import (
    BUNDLE,
    BUNDLE_DOC_SUFFIXES,
    FIRING_KEYS,
    RESOURCES,
    bundle_docs,
    job_of,
    keys_anywhere,
    resource_files,
)

from opl.extraction.ptax_source import BRASILIA

# THE KEY NO COMMITTED FILE MAY CARRY. Not a style rule: it is what makes the target's
# mode the thing that decides, and a job YAML that writes it takes that decision away.
_PAUSE = "pause_status"

# WHICH JOBS DECLARE A CADENCE, AND WHY THAT ONE. The question every entry answers is
# "does something outside this repository decide when this job's input exists?" -- and
# where the answer is a person or another machine, the answer is no schedule.
_SCHEDULED = {
    "bronze_empresas_job.yml": "the RFB CNPJ snapshot is monthly",
    "bronze_estabelecimentos_job.yml": "the RFB CNPJ snapshot is monthly",
    "bronze_socios_job.yml": "the RFB CNPJ snapshot is monthly",
    "bronze_ptax_job.yml": "the Banco Central publishes PTAX once per business day",
    "vault_empresa_job.yml": "follows the monthly empresas bronze it reads",
    "vault_estabelecimento_job.yml": "follows the monthly estabelecimentos bronze it reads",
    "vault_partner_job.yml": "follows the monthly socios bronze it reads",
    "gold_conformed_dimensions_job.yml": (
        "`dim_date`'s span is MEASURED at build time from `sat_empresa_dados`' "
        "applied dates, and that satellite is loaded by `vault_empresa_job.yml`"
    ),
    "gold_dim_company_job.yml": "follows the empresa vault load, which follows the snapshot",
    "gold_pit_estabelecimento_job.yml": "follows the estabelecimento vault load",
}

# AND THE JOBS THAT DELIBERATELY HAVE NONE. Each reason is this repository's own, taken
# from the file's header rather than invented here.
_UNSCHEDULED = {
    # F8's CORRECTION PASS MOVED THIS ONE OUT OF `_SCHEDULED`, and the reason is a
    # contradiction rather than a preference. F8 gave it the same "the RFB CNPJ snapshot is
    # monthly" that its three siblings carry -- true of the PUBLISHER, and beside the point
    # for this table: `opl.dataops.cadence` declares `lookup` `PAUSED`, deliberately not
    # ingested since 2026-06 on a scope decision F1.4b PR B recorded, and the freshness view
    # prints it as `paused_by_decision` rather than as a fault. One commit cannot hold both.
    # The half with evidence behind it is the recorded decision, so the cadence stays and the
    # schedule goes. `tests/dataops/test_cadence.py` now refuses the pairing outright.
    "bronze_job.yml": (
        "it ingests `lookup`, which `opl.dataops.cadence` declares PAUSED -- deliberately "
        "not ingested since 2026-06 on a recorded scope decision. A monthly cadence here "
        "would contradict a declaration this repository ships in the same tree"
    ),
    "bronze_payments_job.yml": (
        "its input is GENERATED by this wheel, per profile, and `profile` is a choice "
        "rather than a cadence -- its default is a sentinel the generator refuses"
    ),
    "bronze_merchant_job.yml": (
        "its input is produced by a host-side extractor on a laptop, and this job's own "
        "header states the consequence: launched before the snapshot has landed it drains "
        "an empty directory and reports SUCCESS, which for THIS table is what a total "
        "deletion looks like and would end-date every key in the vault"
    ),
    # MOVED OUT WITH THE BRONZE IT FOLLOWS, and stating it as a consequence is the point:
    # its own reason was "follows the monthly lookup bronze it reads", which stopped being
    # true the moment that bronze stopped being monthly. This is the third instance of one
    # shape -- a consumer of an uncadenced bronze carrying no cadence either.
    #
    # NOTHING COMPARES THE TWO, AND THAT IS AN ABSENT LOCK RATHER THAN AN UNLOCKABLE ONE.
    # A first draft of this comment said the follows-relation "lives in these strings, not
    # in any mapping a test can read", and that is false: every vault LOAD task names its
    # bronze source in its own argv, and `tests/test_vault_job_wiring.py` already resolves
    # exactly that (vault table, bronze source) pairing. What is missing is the comparison
    # against this classification -- a scheduled vault job whose bronze producer is
    # unscheduled -- and building it means lifting that reader into `job_yaml.py` rather
    # than spelling it a second time here, which is more than this correction took on. The
    # classification's TOTALITY is locked; which side of it a job belongs on is, for now,
    # an argument in the diff.
    "vault_reference_job.yml": "follows the lookup bronze, which has no cadence to follow",
    "vault_merchant_job.yml": "follows the merchant bronze, which has no cadence to follow",
    "gold_fact_payment_job.yml": "follows the payments bronze, which has no cadence",
    "dataops_views_job.yml": (
        "it issues CREATE OR REPLACE VIEW over definitions that live in the wheel; the "
        "event that changes them is a deploy, not a date"
    ),
    "repromote_batch_job.yml": (
        "an operator action taken AFTER a human has read a quarantine table. Its own "
        "header names isolation -- nothing automated reaches it -- as a safety property"
    ),
    "triage_job.yml": "incident-driven: what it triages is whatever incidents exist",
    "streaming_managed_broker_job.yml": (
        "a recorded run against a trial broker with a dated lifetime, not a standing "
        "service (ADR 0019 Decision 6)"
    ),
    "smoke_job.yml": (
        "the probe you run when the deployment itself is in doubt. A cadence would make "
        "the diagnostic answer a question nobody asked (ADR 0009)"
    ),
}

# The file that declares no job at all, and therefore nothing that could fire.
_NON_JOB = {"dataops_dashboard.yml"}


def _classification_faults(root=RESOURCES) -> list[str]:
    """Whether the three lists above are TOTAL over the bundle documents actually present.

    `resource_files` and not a glob of this module's own: the suffixes are `job_yaml`'s one
    tuple, because this sweep and `bundle_docs()` spelled them apart and a `.yaml` job walked
    in through the gap."""
    declared = set(_SCHEDULED) | set(_UNSCHEDULED) | _NON_JOB
    present = {path.name for path in resource_files(root)}
    if declared == present:
        return []
    return [
        f"unclassified: {sorted(present - declared)}; "
        f"classified but absent: {sorted(declared - present)}"
    ]


def _schedule_of(job: dict) -> dict:
    return job.get("schedule") or {}


def _firing_keys(job: dict) -> list[str]:
    return [key for key in FIRING_KEYS if job.get(key)]


def _schedule_faults(root=RESOURCES) -> list[str]:
    """Every bundle document present under `root`, against what its classification promises."""
    faults = []
    for path in resource_files(root):
        name = path.name
        if name in _NON_JOB:
            continue
        job = job_of(name, root)
        firing = _firing_keys(job)
        if name in _SCHEDULED:
            schedule = _schedule_of(job)
            if firing != ["schedule"]:
                faults.append(f"{name}: declares {firing or 'nothing that fires it'}")
            for key in ("quartz_cron_expression", "timezone_id"):
                if not schedule.get(key):
                    faults.append(f"{name}: its schedule declares no {key}")
            if _PAUSE in schedule:
                faults.append(f"{name}: writes {_PAUSE}, taking the decision off the target")
        elif name in _UNSCHEDULED and firing:
            faults.append(f"{name}: is declared unscheduled and yet declares {firing}")
    return faults


def _pause_status_faults(docs: dict[str, object]) -> list[str]:
    """No committed bundle file may DECLARE the key, at any depth.

    OVER THE PARSED DOCUMENT AND NOT THE RAW TEXT, which is the shape
    `tests/dataops/test_dashboard.py` already settled for the committed warehouse id:
    prose naming the hazard is not the hazard. Every schedule block in this bundle
    carries a comment saying that `pause_status` is the target's to write and not the
    source's -- a text sweep would refuse the explanation along with the defect, and a
    rule that cannot state its own reason is one the next author deletes. Comments are
    gone by the time YAML is a document, so what is checked is what would deploy."""
    return [
        f"{name}: declares {_PAUSE}"
        for name, doc in docs.items()
        if _PAUSE in keys_anywhere(doc)
    ]


def _target_faults(bundle_text: str) -> list[str]:
    """The two targets, and what each is for. Read out of the committed bundle root."""
    targets = yaml.safe_load(bundle_text)["targets"]
    faults = []
    if targets.get("free", {}).get("mode") != "development":
        faults.append(f"target `free` is {targets.get('free')}, not mode: development")
    if not targets.get("free", {}).get("default"):
        faults.append("target `free` is no longer `default: true`")
    prod = targets.get("prod", {})
    if prod.get("mode") != "production":
        faults.append(f"target `prod` is {prod}, not mode: production")
    root_path = prod.get("workspace", {}).get("root_path", "")
    if "${workspace.current_user.userName}" not in root_path:
        # `mode: production` refuses to validate without a root_path; the CLI's own
        # suggestion is a LITERAL user name, and taking it would commit an identity.
        faults.append(f"target `prod` root_path {root_path!r} is not interpolated")
    if "@" in root_path:
        faults.append(f"target `prod` root_path {root_path!r} carries a literal identity")
    return faults


def test_every_job_yaml_is_classified_scheduled_or_not():
    """TOTAL over `databricks/resources`, so a new job cannot inherit an answer.

    The question -- does something outside this repository decide when this job's input
    exists? -- has an answer for every job. NO SPLIT IS PUBLISHED HERE: the counts moved
    twice inside one phase, once when the schedules landed and once when this pass took two
    of them back off, and a number in a docstring is locked by nothing. Derive it:

        git grep -l quartz_cron_expression databricks/resources/ | wc -l

    Left to a glob, a job added later would silently inherit whichever answer the default
    was, with nobody saying why -- which is how the guard classification in
    `test_job_yaml_launch_guards.py` came to be exact in both directions too."""
    assert not _classification_faults()


def test_the_scheduled_jobs_declare_a_cron_a_zone_and_nothing_else_that_fires_them():
    """A cadence is a cron plus a zone. `trigger` and `continuous` are refused in both
    lists: a job with two ways of starting has two cadences and this bundle argues one."""
    assert not _schedule_faults()


def test_no_committed_bundle_file_writes_pause_status():
    """THE MECHANISM, ASSERTED AS AN ABSENCE.

    `pause_status` is written by the CLI from the target's `mode` -- `PAUSED` under
    `development`, `UNPAUSED` under `production`. A job YAML that writes it takes that
    decision away from the target, and the failure is silent: the schedule simply is not
    paused where the deployment mode says it should be. Swept over every committed bundle
    file -- the bundle root and every resource -- at any depth of the parsed document.
    `_pause_status_faults` carries why the sweep is over values rather than text."""
    assert not _pause_status_faults(bundle_docs())


def test_the_bundle_declares_the_two_targets_this_split_needs():
    """`free` stays the default development target; `prod` exists to be the other mode."""
    assert not _target_faults(BUNDLE.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------------
# THE RENDERING. The only place the split is OBSERVED, and it needs credentials.
# --------------------------------------------------------------------------------


# CREDENTIAL-STATE SIGNATURES THIS SKIP RECOGNISES -- recognition, not classification: an exit
# carrying neither string is RED whatever caused it. Measured on CLI v1.8.0: with no config
# file and no `DATABRICKS_HOST`/`DATABRICKS_TOKEN`, *"default auth: cannot configure default
# credentials"*; with a host and a rejected PAT, *"Invalid access token ... (403 403)"* from
# `GET /api/2.0/preview/scim/v2/Me` -- the identity endpoint, not anything that read this
# bundle.
_RECOGNISED_CREDENTIAL_FAILURES = ("cannot configure default credentials", "invalid access token")


def _rendered(target: str) -> dict:
    """`databricks bundle validate -t <target> -o json`, or skip with the reason.

    A SKIP NEEDS A SIGNATURE NAMED ABOVE; EVERY OTHER NON-ZERO EXIT IS RED, INCLUDING
    CREDENTIAL STATES SPELLED SOME WAY THAT TUPLE LACKS. Turning them all into skips
    swallowed a probe: a bundle that had stopped validating read exactly like a box with no
    token. THE COST: `bundle validate` reaches the workspace, so an outage on a credentialed
    box fails rather than skips -- a red says look, the skip said nothing."""
    cli = shutil.which("databricks")
    if cli is None:
        pytest.skip("no `databricks` CLI on PATH; the rendering half cannot run here")
    done = subprocess.run(
        [cli, "bundle", "validate", "-t", target, "-o", "json"],
        cwd=RESOURCES.parent, capture_output=True, text=True, encoding="utf-8",
    )
    if done.returncode and any(s in done.stderr.lower() for s in _RECOGNISED_CREDENTIAL_FAILURES):
        pytest.skip(f"`bundle validate -t {target}`: no usable credentials here")
    assert not done.returncode, (
        f"`bundle validate -t {target}` exited {done.returncode}: {done.stderr.strip()[:300]}"
    )
    return json.loads(done.stdout)["resources"]["jobs"]


def _rendered_pause_statuses(target: str) -> dict[str, str | None]:
    """What the CLI wrote as `pause_status` for each job that declares a schedule."""
    return {
        key: (job.get("schedule") or {}).get(_PAUSE)
        for key, job in _rendered(target).items()
        if job.get("schedule")
    }


@pytest.mark.parametrize(("target", "expected"), [("free", "PAUSED"), ("prod", "UNPAUSED")])
def test_the_target_mode_is_what_writes_pause_status(target: str, expected: str):
    """BOTH targets rendered, and the same source yielding the opposite value.

    SKIPS WITHOUT CREDENTIALS -- see this module's docstring for the measurement that
    says why that cannot be helped. A skip is not a pass and the reason is printed."""
    rendered = _rendered_pause_statuses(target)
    assert len(rendered) == len(_SCHEDULED), (
        f"-t {target} rendered {len(rendered)} scheduled jobs; the source declares "
        f"{len(_SCHEDULED)}"
    )
    wrong = {key: value for key, value in rendered.items() if value != expected}
    assert not wrong, f"-t {target} ({expected} expected) rendered {wrong}"


# --------------------------------------------------------------------------------
# THE FAILURE ARMS. Each mutation is DERIVED from the file, never typed beside it.
# --------------------------------------------------------------------------------

_SCHEDULE_BLOCK = re.compile(
    r"^ +schedule:\n(?: +\w+: .*\n)+", re.M
)


def _a_scheduled_job() -> tuple[str, str]:
    """The first classified-scheduled YAML and its text. NOT a filename typed here: a
    renamed job would then be punished by the arm instead of by the classification."""
    name = sorted(_SCHEDULED)[0]
    return name, (RESOURCES / name).read_text(encoding="utf-8")


def _copy_one(tmp_path, name: str, text: str):
    root = tmp_path / "resources"
    root.mkdir(parents=True, exist_ok=True)
    (root / name).write_text(text, encoding="utf-8")
    return root


def test_the_lock_goes_red_when_a_scheduled_job_loses_its_schedule(tmp_path):
    """Delete the block -- found by shape, so the cron is never typed into this arm.

    THE PRISTINE COPY IS CHECKED FIRST, and that is not ceremony. Without it the arm
    passes whenever a copied YAML produces a fault for ANY reason -- a broken copy, a
    reader that cannot find the job -- and would go on passing after the lock it exercises
    had stopped working."""
    name, text = _a_scheduled_job()
    assert not _schedule_faults(_copy_one(tmp_path / "pristine", name, text))
    found = _SCHEDULE_BLOCK.search(text)
    assert found, f"{name} carries no schedule block for this arm to remove"
    root = _copy_one(tmp_path / "cut", name, text.replace(found.group(0), "", 1))
    faults = _schedule_faults(root)
    assert any(name in fault and "nothing that fires it" in fault for fault in faults), faults


def test_the_sweep_goes_red_when_a_pause_status_is_written_into_the_source(tmp_path):
    """Paste the CLI's own key back into a job YAML and both halves must notice."""
    name, text = _a_scheduled_job()
    assert not _pause_status_faults({name: yaml.safe_load(text)})
    assert not _schedule_faults(_copy_one(tmp_path / "pristine", name, text))
    found = _SCHEDULE_BLOCK.search(text)
    assert found, f"{name} carries no schedule block for this arm to extend"
    mutated = text.replace(found.group(0), f'{found.group(0)}        {_PAUSE}: UNPAUSED\n', 1)
    assert mutated != text
    assert _pause_status_faults({name: yaml.safe_load(mutated)})
    root = _copy_one(tmp_path / "pasted", name, mutated)
    assert any(_PAUSE in fault for fault in _schedule_faults(root))


def test_the_classification_sweep_reads_every_suffix_a_resource_file_may_carry(tmp_path):
    """THE REVIEWER'S PROBE, MADE PERMANENT AND WALKED OVER THE WHOLE SUFFIX SET. A
    scheduled, unclassified, unguarded `zz_probe_job.yaml` under `databricks/resources/` left
    this module green, because these sweeps globbed `*.yml` while `bundle_docs()` had learned
    more. Both read `job_yaml`'s one tuple now, and this arm walks it."""
    root = _copied_tree(tmp_path)
    assert not _classification_faults(root)
    _, text = _a_scheduled_job()
    for suffix in BUNDLE_DOC_SUFFIXES:
        probe = f"zz_probe_job{suffix}"
        (root / probe).write_text(text, encoding="utf-8")
        faults = _classification_faults(root)
        (root / probe).unlink()
        assert any(probe in fault for fault in faults), (probe, faults)


def test_the_target_lock_goes_red_when_prod_commits_an_identity():
    """Swap the interpolation for a literal -- a FABRICATED one, never this box's."""
    text = BUNDLE.read_text(encoding="utf-8")
    token = "${workspace.current_user.userName}"
    assert token in text, "the bundle no longer interpolates the user; re-point this arm"
    faults = _target_faults(text.replace(token, "someone@example.invalid"))
    assert any("literal identity" in fault for fault in faults), faults
    assert any("not interpolated" in fault for fault in faults), faults


# --------------------------------------------------------------------------------
# WHAT THE JUSTIFICATIONS CLAIM. A cron and a zone EXISTING is not what any comment beside
# a schedule block promises -- see this module's docstring for the mutations that proved it.
# --------------------------------------------------------------------------------

# Quartz's six fields, in order. A seventh (year) is optional and this bundle writes none.
_CRON_FIELDS = ("second", "minute", "hour", "day_of_month", "month", "day_of_week")

# THE THREE MONTHLY TIERS, IN THE ORDER THE COMMENTS CLAIM THEY RUN. A job joins its tier
# by being NAMED for it, which is this directory's existing convention and already
# load-bearing (`tests/test_vault_job_wiring.py` globs `vault_*.yml`). The ORDER is the
# claim; every hour compared against it is read out of a file.
_TIERS = ("bronze", "vault", "gold")

_WEEKDAYS = ("MON", "TUE", "WED", "THU", "FRI")
_WEEKEND = ("SAT", "SUN")

# The publication band a daily job's own comment has to state, as `band HH:MM-HH:MM`.
_BAND = re.compile(r"band (\d{2}):(\d{2})-(\d{2}):(\d{2})")

# The `schedule:` key on its own line, used to find where the justifying comment ends.
_SCHEDULE_KEY = re.compile(r"^ +schedule:$", re.M)

_CRON_VALUE = re.compile(r'(quartz_cron_expression: )"[^"]+"')


def _cron_of(name: str, root=RESOURCES) -> dict[str, str]:
    """One scheduled job's quartz expression, by field name."""
    fields = _schedule_of(job_of(name, root))["quartz_cron_expression"].split()
    assert len(fields) == len(_CRON_FIELDS), (
        f"{name} declares a {len(fields)}-field quartz expression; this reader knows the "
        f"six {_CRON_FIELDS} and would silently mis-name every field of a seventh"
    )
    return dict(zip(_CRON_FIELDS, fields, strict=True))


def _cron_text(name: str, root=RESOURCES) -> str:
    return _schedule_of(job_of(name, root))["quartz_cron_expression"]


def _minutes(hour: str, minute: str) -> int:
    return int(hour) * 60 + int(minute)


def _tier_of(name: str) -> str | None:
    return next((tier for tier in _TIERS if name.startswith(f"{tier}_")), None)


def _monthly(root=RESOURCES) -> dict[str, dict[str, int]]:
    """{tier: {yaml: minutes past midnight}} for every schedule on a day of the MONTH."""
    found: dict[str, dict[str, int]] = {tier: {} for tier in _TIERS}
    for name in sorted(_SCHEDULED):
        cron, tier = _cron_of(name, root), _tier_of(name)
        if cron["day_of_month"].isdigit() and tier is not None:
            found[tier][name] = _minutes(cron["hour"], cron["minute"])
    return found


def _days_of_month(root=RESOURCES) -> set[str]:
    """The day every monthly schedule fires on. One shared day is what "tier" means."""
    return {
        _cron_of(name, root)["day_of_month"]
        for name in _SCHEDULED
        if _cron_of(name, root)["day_of_month"].isdigit()
    }


def _untiered_monthly(root=RESOURCES) -> list[str]:
    """Scheduled monthly jobs whose FILENAME names no tier -- which nothing used to see.

    `_tier_of` returns None for such a file, `_monthly` then drops it, and `_daily_faults`
    skips it for declaring a day of the month. So it escaped BOTH checks, and the escape is
    measured rather than argued: with these four lines removed from `_tier_faults`, the arm
    below hands a monthly job a name naming no tier and the whole module still passes. The
    naming convention is what the entire ordering claim rests on, so a monthly job outside
    it is not a job without a tier; it is an hour nothing compares."""
    return [
        name
        for name in sorted(_SCHEDULED)
        if _cron_of(name, root)["day_of_month"].isdigit() and _tier_of(name) is None
    ]


def _tier_faults(root=RESOURCES) -> list[str]:
    """The 06:00 -> 09:00 -> 12:00 ordering the schedule comments state, asserted.

    A cron offset is not a dependency (ADR 0021) and this does not pretend otherwise. What
    it refuses is the ordering INVERTING -- a gold build ahead of the vault load it says it
    follows -- which every one of those comments states as a fact and nothing checked."""
    monthly = _monthly(root)
    faults = [f"no monthly job runs in the {tier} tier" for tier in _TIERS if not monthly[tier]]
    faults += [
        f"{name}: fires on a day of the month and its filename names none of {_TIERS}, so "
        "no tier ordering compares its hour against anything"
        for name in _untiered_monthly(root)
    ]
    days = _days_of_month(root)
    if len(days) > 1:
        faults.append(f"the monthly tiers do not share one day of the month: {sorted(days)}")
    for earlier, later in zip(_TIERS, _TIERS[1:], strict=False):
        if not (monthly[earlier] and monthly[later]):
            continue
        last = max(monthly[earlier].items(), key=lambda item: item[1])
        first = min(monthly[later].items(), key=lambda item: item[1])
        if last[1] >= first[1]:
            faults.append(
                f"{earlier} tier's {last[0]} runs at minute {last[1]} of the day and {later} "
                f"tier's {first[0]} at {first[1]}: the tier order is not strict"
            )
    return faults


def _zone_faults(root=RESOURCES) -> list[str]:
    """Every schedule's zone, against the offset the publication instants are READ in.

    `BRASILIA` is `opl.extraction.ptax_source`'s own fixed offset, so the zone these crons
    run in is derived from the wheel rather than typed here. FOUR PROBES ACROSS THE YEAR
    because a zone carrying DST satisfies one instant and not the others, and an hour that
    moves twice a year is not the hour the comments promise."""
    faults = []
    for name in sorted(_SCHEDULED):
        zone = _schedule_of(job_of(name, root))["timezone_id"]
        try:
            info = ZoneInfo(zone)
        except (KeyError, ValueError) as unresolved:
            faults.append(f"{name}: timezone_id {zone!r} does not resolve here ({unresolved})")
            continue
        offsets = {info.utcoffset(datetime(2026, month, 15)) for month in (1, 4, 7, 10)}
        if offsets != {BRASILIA.utcoffset(None)}:
            faults.append(
                f"{name}: timezone_id {zone!r} runs at {sorted(map(str, offsets))}, and the "
                f"publication instants these cadences chase are read at {BRASILIA}"
            )
    return faults


def _justification_of(text: str) -> str:
    """The unbroken run of comment lines directly ABOVE this file's `schedule:` key.

    `_BAND` was searched over the WHOLE FILE, first match wins, above a long header -- so a
    `band HH:MM-HH:MM` written anywhere in it, about anything, satisfied the hour. What
    justifies a cron is the comment attached to it. A file with no `schedule:` key yields
    the empty string, and `_daily_faults` reports a missing band rather than reading on."""
    found = _SCHEDULE_KEY.search(text)
    if found is None:
        return ""
    lines = text[: found.start()].splitlines()
    kept: list[str] = []
    while lines and lines[-1].lstrip().startswith("#"):
        kept.append(lines.pop())
    return "\n".join(reversed(kept))


def _daily_faults(root=RESOURCES) -> list[str]:
    """A daily schedule against the two things its own comment claims.

    THE BAND IS READ OUT OF THE COMMENT ATTACHED TO THE JOB'S OWN `schedule:` KEY, so the
    hour cannot drift away from the sentence justifying it, a comment rewrapped so `band`
    and the times land on different lines fails here rather than silently unhooking the
    check, and a band stated elsewhere in the file no longer satisfies one stated here.

    AND A COMMENT STATING TWO DIFFERENT BANDS IS A FAULT, not a first-match: `search` would
    take whichever came first, and two intervals that disagree give the hour two things to
    sit after. Restating the interval in that block is easy enough that this check once went
    green over a rewritten PTAX comment carrying it twice. WHAT THAT COSTS THE AUTHOR OF A
    SUPERSEDED BAND is stated in the PTAX block, where it is paid."""
    faults = []
    for name in sorted(_SCHEDULED):
        cron = _cron_of(name, root)
        if cron["day_of_month"].isdigit():
            continue
        days = cron["day_of_week"].upper()
        if any(day in days for day in _WEEKEND):
            faults.append(f"{name}: its day-of-week {days!r} names a weekend day")
        if not any(day in days for day in _WEEKDAYS):
            faults.append(f"{name}: its day-of-week {days!r} names no weekday")
        justification = _justification_of((root / name).read_text(encoding="utf-8"))
        stated = set(_BAND.findall(justification))
        if len(stated) > 1:
            faults.append(f"{name}: its schedule comment states disagreeing bands {sorted(stated)}")
            continue
        band = _BAND.search(justification)
        if band is None:
            faults.append(f"{name}: states no `band HH:MM-HH:MM` for its hour to sit after")
            continue
        fires, ends = _minutes(cron["hour"], cron["minute"]), _minutes(band[3], band[4])
        if fires <= ends:
            faults.append(
                f"{name}: fires at {cron['hour']}:{cron['minute']}, which is not after the "
                f"{band[1]}:{band[2]}-{band[3]}:{band[4]} band it states"
            )
    return faults


def test_the_monthly_tiers_run_in_the_order_their_comments_claim():
    """bronze strictly before vault, vault strictly before gold, on one day of the month."""
    assert not _tier_faults()


def test_every_schedule_runs_in_the_zone_the_publication_instants_are_read_in():
    """Not `America/Sao_Paulo` typed here: the offset comes from the wheel's own constant."""
    assert not _zone_faults()


def test_a_daily_schedule_fires_on_weekdays_after_the_band_its_own_comment_states():
    """The hour is compared against the band the file states, not against a number here."""
    assert not _daily_faults()


# --------------------------------------------------------------------------------
# THEIR FAILURE ARMS. Each mutation is DERIVED from a file, and the two the reviewer
# applied to the whole tree -- PTAX onto a Sunday small hour, every zone to UTC -- have
# an arm each, because those are the two this repository is on record as not catching.
# --------------------------------------------------------------------------------


def _copied_tree(tmp_path):
    """The WHOLE resources directory, so a mutation is read in the context it lives in."""
    root = tmp_path / "resources"
    shutil.copytree(RESOURCES, root)
    return root


def _swap_cron(path, cron: str) -> None:
    text = path.read_text(encoding="utf-8")
    swapped, count = _CRON_VALUE.subn(lambda found: f'{found[1]}"{cron}"', text, count=1)
    assert count == 1, f"{path.name} carries no quartz expression for this arm to swap"
    path.write_text(swapped, encoding="utf-8")


def _the_daily_job(root) -> str:
    """The one scheduled YAML whose cron names days of the WEEK rather than of the month."""
    daily = [n for n in sorted(_SCHEDULED) if not _cron_of(n, root)["day_of_month"].isdigit()]
    assert len(daily) == 1, f"{daily} declare a weekday cadence; this arm expects exactly one"
    return daily[0]


def test_the_tier_lock_goes_red_when_gold_runs_before_the_vault_it_follows(tmp_path):
    """The gold job is handed the BRONZE job's own cron, read out of that file.

    No hour is typed here. An arm carrying the literal it mutates goes green the day the
    literal moves, and nobody re-reads an arm that is passing."""
    root = _copied_tree(tmp_path)
    assert not _tier_faults(root)
    bronze = min(name for name in _SCHEDULED if _tier_of(name) == "bronze")
    gold = min(name for name in _SCHEDULED if _tier_of(name) == "gold")
    _swap_cron(root / gold, _cron_text(bronze, root))
    faults = _tier_faults(root)
    assert any("tier order is not strict" in fault for fault in faults), faults


def test_the_zone_lock_goes_red_when_a_schedule_is_rewritten_to_utc(tmp_path):
    """The committed zone is READ out of the file and replaced; it is never typed here.

    UTC is the value ADR 0016 Decision 2 rules out, and rewriting every zone to it is the
    mutation that left the whole suite green before this lock existed."""
    root = _copied_tree(tmp_path)
    assert not _zone_faults(root)
    name = sorted(_SCHEDULED)[0]
    path = root / name
    zone = _schedule_of(job_of(name, root))["timezone_id"]
    path.write_text(path.read_text(encoding="utf-8").replace(zone, "UTC"), encoding="utf-8")
    faults = _zone_faults(root)
    assert any(name in fault for fault in faults), faults


def test_the_band_lock_goes_red_when_the_daily_job_fires_inside_its_own_band(tmp_path):
    """Both halves derived: the hour and minute come from the band THIS file states, and
    every other field from the cron it declares."""
    root = _copied_tree(tmp_path)
    assert not _daily_faults(root)
    name = _the_daily_job(root)
    band = _BAND.search(_justification_of((root / name).read_text(encoding="utf-8")))
    assert band, f"{name} states no band for this arm to move its cron into"
    cron = _cron_of(name, root)
    inside = " ".join([cron["second"], band[4], band[3], *(cron[f] for f in _CRON_FIELDS[3:])])
    _swap_cron(root / name, inside)
    faults = _daily_faults(root)
    assert any("is not after the" in fault for fault in faults), faults


def test_the_weekday_lock_goes_red_when_the_daily_job_is_moved_onto_a_weekend(tmp_path):
    """The reviewer's other PTAX mutation. Only the day-of-week field is replaced; every
    other field is the one the file declares."""
    root = _copied_tree(tmp_path)
    assert not _daily_faults(root)
    name = _the_daily_job(root)
    cron = _cron_of(name, root)
    weekend = " ".join([*(cron[f] for f in _CRON_FIELDS[:-1]), _WEEKEND[1]])
    _swap_cron(root / name, weekend)
    faults = _daily_faults(root)
    assert any("names a weekend day" in fault for fault in faults), faults


def test_the_tier_lock_goes_red_when_a_scheduled_monthly_job_carries_no_tier(
    tmp_path, monkeypatch
):
    """A monthly job whose FILENAME names no tier used to escape both new checks.

    Everything but the name is copied: the file, its cron and the classification entry all
    come from a bronze job that already exists, so the arm carries no hour and no
    expression of its own. `dataops_` is the prefix because it is a real one in this
    directory (`dataops_views_job.yml`, `dataops_dashboard.yml`) and names no tier."""
    root = _copied_tree(tmp_path)
    assert not _tier_faults(root)
    source = min(name for name in _SCHEDULED if _tier_of(name) == _TIERS[0])
    untiered = f"dataops_{source}"
    (root / untiered).write_text((root / source).read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.setitem(_SCHEDULED, untiered, f"{source} copied under a name naming no tier")
    faults = _tier_faults(root)
    assert any(untiered in fault and "names none of" in fault for fault in faults), faults


def test_the_band_lock_goes_red_when_the_band_moves_out_of_the_schedules_own_comment(tmp_path):
    """The band is MOVED, not deleted -- to the top of the same file, where the old
    whole-file search would have found it and called the hour justified. That is the defect
    this anchoring exists for: a long header sits above the block, and any
    `band HH:MM-HH:MM` in it, about anything at all, satisfied the comparison.

    EVERY LINE STATING THE BAND MOVES, WHICH IS THE ARM AGREEING WITH THE LOCK IT DEFENDS.
    `_daily_faults` compares a SET of bands, so two IDENTICAL mentions are permitted -- and
    against a file in that permitted state an arm moving only the first left one behind and
    failed with a bare `AssertionError: []`, broken by a state its own lock allows."""
    root = _copied_tree(tmp_path)
    assert not _daily_faults(root)
    path = root / _the_daily_job(root)
    text = path.read_text(encoding="utf-8")
    justification = _justification_of(text)
    stated = [line for line in justification.splitlines() if _BAND.search(line)]
    assert stated, f"{path.name} states no band in its schedule comment for this arm to move"
    kept = "\n".join(line for line in justification.splitlines() if line not in stated)
    moved = "\n".join(stated) + "\n" + text.replace(justification, kept, 1)
    assert _BAND.search(moved), "this arm dropped the band instead of moving it"
    assert not _BAND.search(_justification_of(moved)), "the band is still in the block"
    path.write_text(moved, encoding="utf-8")
    faults = _daily_faults(root)
    assert any("states no `band" in fault for fault in faults), faults


def test_the_band_lock_goes_red_when_a_schedule_comment_states_two_disagreeing_bands(tmp_path):
    """THE ARM THIS FILE PAID FOR BEFORE IT EXISTED.

    F8's second correction pass rewrote the PTAX comment, restated the interval in it once
    more, and the arm above went green over a file whose block then held two bands. Both
    were the same value there and no hour moved -- but `search` takes the first, so a second
    that DISAGREED would have decided nothing and been read by nobody. The second interval
    is derived by shifting the stated one an hour, never typed."""
    root = _copied_tree(tmp_path)
    assert not _daily_faults(root)
    path = root / _the_daily_job(root)
    text = path.read_text(encoding="utf-8")
    stated = next(line for line in _justification_of(text).splitlines() if _BAND.search(line))
    found = _BAND.search(stated)
    shifted = f"# band {int(found[1]) - 1:02d}:{found[2]}-{int(found[3]) - 1:02d}:{found[4]}"
    path.write_text(text.replace(stated, f"{stated}\n{shifted}", 1), encoding="utf-8")
    faults = _daily_faults(root)
    assert any("disagreeing bands" in fault for fault in faults), faults
