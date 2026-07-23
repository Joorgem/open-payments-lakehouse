# ADR 0002 — Two-layer topology (extraction vs transformation)

## Context
Databricks Free Edition serverless compute blocks outbound internet to
untrusted domains (DNS resolution fails); LinkedIn verification does not lift
this. Therefore BCB API calls and Postgres reads cannot run as Databricks jobs.

## Decision
Split into (1) an Extraction & Landing layer that runs OFF Databricks (Docker
local / GitHub Actions, which have internet) and lands versioned files, uploaded
to a UC Volume via the Databricks control plane (PAT); and (2) a Transformation
layer on Databricks that consumes the Volume via Auto Loader. This is the
canonical ingest-vs-transform separation, validated by scripts/validate_uc_volume.py.

## Consequences
- The "same core transforms regardless of source" story strengthens the design.
- Auth is PAT-only on Free; bundles use serverless + %pip.

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
  back byte-identical. The blocked-egress mitigation is validated.
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
