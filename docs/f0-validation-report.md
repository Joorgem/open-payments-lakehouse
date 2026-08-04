# F0 Validation Report

| Premise | Probe | Result |
|---|---|---|
| Free Edition blocks serverless egress → two-layer topology needed | ADR 0002 + UC Volume upload | UC roundtrip OK — validated on the real Free Edition workspace |
| Control-plane upload to UC Volume works (topology mitigation) | `scripts/validate_uc_volume.py` | OK. `UC Volume upload+download roundtrip: OK` against `workspace.default.landing` (this workspace has no `main` catalog; `workspace.default` used instead, documented in ADR 0002). Byte-identical upload/download confirmed via the control-plane PAT from off-Databricks compute. |
| Asset Bundles validate on Free (PAT/serverless) | `databricks bundle validate` | OK. `databricks bundle validate --profile opl-free -t free` → `Validation OK!` (no warnings). Required dropping the literal `workspace.host: ${DATABRICKS_HOST}` from `databricks.yml` — bundle `${...}` syntax is for bundle variables, not shell env vars; the CLI profile resolves the host instead. |
| 2–3 consecutive CNPJ monthly snapshots downloadable | `scripts/validate_cnpj_snapshots.py` | OK, and better than the bar: 4/4 of the most recent months reachable (2026-04, 2026-05, 2026-06, 2026-07) via the Nextcloud/SERPRO+ WebDAV public share (`https://arquivos.receitafederal.gov.br/public.php/webdav/<YYYY-MM>/...`, HTTP Basic auth with the public share token). A supplemental PROPFIND (outside the committed script) showed a gapless monthly series from 2023-05 through 2026-07 — **39 months** of history available for SCD2 backfill. |

> **Correction, 2026-08-03 (F1.4b PR B Task 7).** This cell originally read
> "44+ months". That figure is not what the range beside it produces: 2023-05
> through 2026-07 inclusive is **39** months (8 + 12 + 12 + 7). This is an
> arithmetic correction to the count, **not** a re-measurement of availability —
> no PROPFIND was re-run for it, and the "4/4 most recent months reachable"
> result and the gapless-from-2023-05 finding are untouched. It matters because
> every later "N months remain uningested" figure is derived from it: with two
> months ingested (2026-06 and 2026-07, F1.4b), **37 of 39 remain**, not 42 of 44.
> The same 2023-05..2026-07 range is quoted in
> `scripts/validate_cnpj_snapshots.py` and `tests/integration/test_webdav_live.py`
> without a count, so neither needed changing.
| BCB PTAX API reachable from extraction layer | same script | OK (200), quote array present. Brief's PTAX URL worked unmodified. |

## Notes on deviations from the brief's assumptions
- CNPJ access is **not** a plain Apache directory tree as originally assumed; Receita Federal serves it via a Nextcloud instance ("SERPRO+") public share over WebDAV. The extraction layer must speak WebDAV + share-token Basic auth, not a naive static-file HTTP client. See Task 6 report and `scripts/validate_cnpj_snapshots.py` for the full investigation.
- The Databricks Free Edition workspace used for validation has no `main` catalog; `workspace.default` is the catalog/schema actually exercised end-to-end. Any F1+ work should either provision a dedicated catalog or continue using `workspace.default`, per ADR 0002.

## Go/no-go
**GO.** UC Volume upload = OK, and 4/4 (≥2 required) CNPJ monthly snapshots are reachable. F1 proceeds.
