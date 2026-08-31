# ADR 0002 — Two-layer topology (extraction vs transformation)

## Context

> **AMENDED 2026-08-17 by F-DB. HALF OF THIS PARAGRAPH WAS MEASURED FALSE BY F-API ON
> 2026-08-14 AND F-API DID NOT COME BACK TO SAY SO.** The amendment is **scoped to this
> Context**. The Decision below stands unchanged, and so does everything from *"Validation
> notes"* onward — those are live decisions cited by `CLAUDE.md`, `README.md` and
> `docs/f0-validation-report.md`, and none of them is retracted here.

> **AND THAT RING-FENCE WAS DRAWN AROUND A FALSE SENTENCE. Amended 2026-08-31 by F7.**
> The scope above was chosen to keep a correction narrow, and it left standing, in
> *"Validation notes"*, the claim *"The blocked-egress mitigation is validated"* — which
> rests on the same F0 row this amendment's own evidence falsifies, and which
> `docs/f0-validation-report.md` strikes. One Consequence carried two more false claims.
> Each is struck where it stands, below. **A boundary drawn around a defect is how a
> correction pass leaves one behind.**

~~Databricks Free Edition serverless compute blocks outbound internet to
untrusted domains (DNS resolution fails); LinkedIn verification does not lift
this. Therefore BCB API calls and Postgres reads cannot run as Databricks jobs.~~

**What was measured** (`docs/f-api-run-evidence.md` §0.8): a serverless task resolved
`olinda.bcb.gov.br` to `150.171.109.72`, received HTTP 200 from the BCB, and pulled
**192,973 bytes** from an unrelated second host. The PTAX fetch then ran **in production** as
a job task — one `fetch` task, 52 s, 60 single-day HTTPS requests. **So "blocks outbound
internet to untrusted domains" is false as stated, and "BCB API calls cannot run as
Databricks jobs" is false by demonstration**: they do, on the critical path, today.

**The conclusion survives for Postgres, and the reason is a different one.** The database is
a container bound to `localhost:5433` on a development machine behind a home NAT. Egress
*out of* Databricks creates no route *into* that machine: these are opposite directions, and
the struck paragraph reached the right answer for Postgres by conflating them. What rules out
a Databricks-side Postgres read is **network topology**, not an egress policy.

**That reason is ARGUED, NOT MEASURED, and the distinction is the point of writing it down.**
There is no public address to point a probe at, so **no measurement is offered here and none
is implied**. A reader holding F-API's egress result must not be able to read this as a test
that was run. For the same discipline: **`psycopg[binary]` on serverless is unmeasured and
stays that way** — "we did not measure it" and "it does not work" are two sentences this
project keeps having to separate, and F-DB's extractor runs host-side for the topology reason
above rather than because a driver was found wanting.

**Why the amendment is here rather than in a new ADR.** The Decision this Context introduces
is unchanged and correct: extraction off Databricks, landing to a UC Volume, transformation
on Databricks. A new ADR superseding this one would retire a topology that is running, over a
premise defect in one paragraph. F-DB is the fourth source to depend on that topology and the
first whose reason for it is not the one written here.

## Decision
Split into (1) an Extraction & Landing layer that runs OFF Databricks (Docker
local / GitHub Actions, which have internet) and lands versioned files, uploaded
to a UC Volume via the Databricks control plane (PAT); and (2) a Transformation
layer on Databricks that consumes the Volume via Auto Loader. This is the
canonical ingest-vs-transform separation, validated by scripts/validate_uc_volume.py.

## Consequences
- The "same core transforms regardless of source" story strengthens the design.
- ~~Auth is PAT-only on Free; bundles use serverless + %pip.~~ **Serverless stands; the
  other two claims were measured false after this was written (F7, 2026-08-31).**
  *Auth:* **a service principal with an OAuth secret is available on Free Edition** —
  ADR 0008's 2026-08-18 note says so, *"The lag: group membership is not a switch"* read
  `is_member` on the `opl-free` warehouse through one, and
  `scripts/rebuild_pii_reader_sp.py` rebuilds it. PAT is what this project uses, not what
  Free allows. *`%pip`:* bundles install the wheel through an `environments:` block
  instead, and no bundle job installs anything with `%pip` —
  `grep -L 'dist/\*\.whl' databricks/resources/*.yml` names only
  `dataops_dashboard.yml`, which declares no job, and
  `grep -rn '%pip' databricks/` returns nothing.

## Validation notes (Task 5, harness phase)

- **Catalog/schema layout differs from the brief's assumption.** The brief's
  original probe assumed `main.default`. This Free Edition workspace does not
  have a `main` catalog at all — `databricks catalogs list --profile opl-free`
  returned only `workspace` (managed), `system`, and `samples`. The writable,
  auto-created schema is `workspace.default`. `scripts/validate_uc_volume.py`
  and the volume path were updated to target `/Volumes/workspace/default/landing`
  accordingly.
- **UC Volume upload+download roundtrip: OK.** This is the go/no-go gate for
  the topology above — the control-plane PAT path can land files into a UC
  Volume from off-Databricks compute, and Databricks-side code can read them
  back byte-identical. ~~The blocked-egress mitigation is validated.~~ **Struck
  2026-08-31 (F7): the roundtrip result stands, the inference does not.** The upload
  travels the control plane and touches serverless egress not at all, so it cannot
  validate a mitigation for blocked egress — and egress was not blocked.
  `docs/f0-validation-report.md` strikes the row this sentence rests on, under
  *"The first row was falsified, and the probe never tested it"*, and
  `scripts/validate_uc_volume.py`'s docstring has said so since 2026-08-17.
- **SDK gotcha:** `databricks-sdk==0.40.0`'s `WorkspaceClient.volumes.create`
  requires `volume_type` to be a `databricks.sdk.service.catalog.VolumeType`
  enum member, not the raw string `"MANAGED"` — passing a string raises
  `'str' object has no attribute 'value'` inside the SDK. Fixed by importing
  `VolumeType` and passing `VolumeType.MANAGED`.
- **Bundle host resolution:** `databricks bundle validate` rejected the
  brief's literal `workspace.host: ${DATABRICKS_HOST}` — Databricks Asset
  Bundles interpolate `${...}` as bundle variable/resource references, not
  shell env vars, so the literal string was compared against the profile's
  real host and failed. Fixed by omitting `workspace.host` from
  `databricks.yml` and relying on the `opl-free` CLI profile (`--profile`) to
  supply the host, which already carries the value sourced from `.env`.
  `databricks bundle validate --profile opl-free -t free` now returns
  `Validation OK!`.

## Dev credential scope (deliberate, documented)

The development PAT (`DATABRICKS_TOKEN` in the git-ignored `.env`) was issued
with **"all APIs" scope** rather than a narrower scope. This is a deliberate,
accepted trade-off for this harness/spike phase:

- The workspace is a personal, throwaway Databricks Free Edition account —
  not shared, not production, no real data or other tenants at risk.
- Free Edition's PAT scoping options are limited, and the harness needs to
  exercise a breadth of control-plane APIs (Unity Catalog volumes, files,
  bundles, jobs) while iterating quickly.
- Least-privilege scoping is explicitly waived here in favor of harness
  breadth and iteration speed. This should be revisited (narrower PAT scope,
  or a service principal with least-privilege OAuth) before any non-throwaway
  or shared workspace is used.
