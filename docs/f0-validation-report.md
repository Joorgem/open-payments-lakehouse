# F0 Validation Report

| Premise | Probe | Result |
|---|---|---|
| ~~Free Edition blocks serverless egress → two-layer topology needed~~ **FALSIFIED — see the note below** | ADR 0002 + UC Volume upload | ~~UC roundtrip OK — validated on the real Free Edition workspace~~ **The probe answered a different question than the premise asked** |
| **Postgres is reachable and behaves as a source** *(added 2026-08-17 by F-DB)* | none at F0; `scripts/probe_postgres.py` and its three siblings, F-DB Task 0 | **NOT PROBED AT F0.** Master spec §9 lists F0 as standing up "Docker Redpanda/**Postgres**/Spark" and no premise here was about a database (`grep -i postgres` → 0 hits). What existed was `tests/integration/test_postgres.py`: twelve lines, one `SELECT 1`, deselected by default. F-DB Task 0 took the five measurements F0 owed — the persistence matrix, the isolation levels, the stamp gap, the GUC rendering matrix and the out-of-order commit — and they are in `docs/f-db-run-evidence.md` §0.4 |
| Control-plane upload to UC Volume works (topology mitigation) | `scripts/validate_uc_volume.py` | OK. `UC Volume upload+download roundtrip: OK` against `workspace.default.landing` (this workspace has no `main` catalog; `workspace.default` used instead, documented in ADR 0002). Byte-identical upload/download confirmed via the control-plane PAT from off-Databricks compute. |
| Asset Bundles validate on Free (PAT/serverless) | `databricks bundle validate` | OK. `databricks bundle validate --profile opl-free -t free` → `Validation OK!` (no warnings). Required dropping the literal `workspace.host: ${DATABRICKS_HOST}` from `databricks.yml` — bundle `${...}` syntax is for bundle variables, not shell env vars; the CLI profile resolves the host instead. |
| 2–3 consecutive CNPJ monthly snapshots downloadable | `scripts/validate_cnpj_snapshots.py` | OK, and better than the bar: 4/4 of the most recent months reachable (2026-04, 2026-05, 2026-06, 2026-07) via the Nextcloud/SERPRO+ WebDAV public share (`https://arquivos.receitafederal.gov.br/public.php/webdav/<YYYY-MM>/...`, HTTP Basic auth with the public share token). A supplemental PROPFIND (outside the committed script) showed a gapless monthly series from 2023-05 through 2026-07 — **39 months** of history available for SCD2 backfill. |
| BCB PTAX API reachable from extraction layer | same script | OK (200), quote array present. Brief's PTAX URL worked unmodified. |

> **Correction, 2026-08-03 (F1.4b PR B Task 7).** Applies to the CNPJ snapshot
> row above. That cell originally read "44+ months". That figure is not what the
> range beside it produces: 2023-05 through 2026-07 inclusive is **39** months
> (8 + 12 + 12 + 7). This is an arithmetic correction to the count, **not** a
> re-measurement of availability — no PROPFIND was re-run for it, and the "4/4
> most recent months reachable" result and the gapless-from-2023-05 finding are
> untouched. It matters because every later "N months remain uningested" figure is
> derived from it: with two months ingested (2026-06 and 2026-07, F1.4b), **37 of
> 39 remain**, not 42 of 44. The same 2023-05..2026-07 range is quoted in
> `scripts/validate_cnpj_snapshots.py` and `tests/integration/test_webdav_live.py`
> without a count, so neither needed changing.
>
> *Placement note (same PR, final review):* this blockquote was originally inserted
> directly beneath the row it corrects, which put a blank line in the middle of the
> table — under GFM a table ends at the first blank line, so the PTAX row below it
> stopped rendering as a table row and appeared as literal pipe-delimited text.
> Since that row is one of the four go/no-go premises, the correction is now placed
> after the table and names the row it applies to.

## The first row was falsified, and the probe never tested it — added 2026-08-17 by F-DB

**The premise asked whether serverless egress is blocked. The probe was a UC Volume upload,
which travels the control plane and touches serverless egress not at all.** The Result column
then reported the upload's success as though it settled the premise. Nothing about egress was
measured at F0, in the row whose whole subject is egress — and the two-layer topology, which
is correct and is running, was recorded as *validated* on the strength of it.

**Measured on 2026-08-14** (`docs/f-api-run-evidence.md` §0.8): a serverless task resolved
`olinda.bcb.gov.br` to `150.171.109.72`, received HTTP 200 from the BCB and pulled **192,973
bytes** from an unrelated second host. An API fetch has since run **as a production job task**
— one `fetch` task, 52 s, 60 single-day HTTPS requests.

**What survives, and it is narrower and per-source.** The CNPJ share needs Range-resume over
multi-gigabyte downloads and unzipping before landing. The Postgres source is a container on a
development machine behind a home NAT, which outbound egress does not reach because it is the
opposite direction — **argued, not measured**, since there is no address to point a probe at.
[ADR 0002](adr/0002-two-layer-topology.md)'s Context carries the amendment, and
[ADR 0003](adr/0003-cnpj-extraction-layer.md) is amended in the same pass because it inherited
this premise and was never independently probed.

**The row is struck rather than deleted.** A falsified premise that a project recorded as
verified is worth more on the page than a tidy table, and this one was load-bearing for four
phases.

## Notes on deviations from the brief's assumptions
- CNPJ access is **not** a plain Apache directory tree as originally assumed; Receita Federal serves it via a Nextcloud instance ("SERPRO+") public share over WebDAV. The extraction layer must speak WebDAV + share-token Basic auth, not a naive static-file HTTP client. See Task 6 report and `scripts/validate_cnpj_snapshots.py` for the full investigation.
- The Databricks Free Edition workspace used for validation has no `main` catalog; `workspace.default` is the catalog/schema actually exercised end-to-end. Any F1+ work should either provision a dedicated catalog or continue using `workspace.default`, per ADR 0002.

## Go/no-go
**GO.** UC Volume upload = OK, and 4/4 (≥2 required) CNPJ monthly snapshots are reachable. F1 proceeds.
