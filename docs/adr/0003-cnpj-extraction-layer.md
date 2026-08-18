# ADR 0003 — CNPJ extraction layer (WebDAV → UC Volume)

## Context
RFB CNPJ open data is served from a Nextcloud "SERPRO+" public share over
WebDAV (public share id `YggdBLfdninEJX9`, Basic auth token=id/empty password),
NOT a plain file server. ~~Serverless Databricks cannot reach it (ADR 0002).~~

> **AMENDED 2026-08-17 by F-DB, in the same pass as ADR 0002's Context, because this sentence
> INHERITS that premise and was never independently probed.** ADR 0002's egress claim was
> measured false by F-API (a serverless task reached `olinda.bcb.gov.br` and ran the PTAX fetch
> in production), and **no serverless probe was ever pointed at
> `arquivos.receitafederal.gov.br`** — so this line asserts about the RFB share a limit that
> was only ever tested somewhere else, and tested false there.
>
> **The Decision below is unchanged**, and the honest statement of its ground is that the
> extraction layer runs off Databricks for the reasons that survive on their own: it needs
> Range-resume over multi-gigabyte downloads, it unzips before landing, and F0 built it there.
> **Whether serverless could reach this share is UNMEASURED**, and it is recorded as unmeasured
> rather than converted into either a limit or a capability.

## Decision
A pure-Python extraction layer (`opl.extraction`) runs off Databricks: lists a
month via PROPFIND, downloads with Range-resume + size integrity, unzips the
single inner K-file, and uploads raw files to a UC Volume
(`/Volumes/workspace/default/landing/cnpj/<month>/`). Schema contracts
(`opl.contracts.cnpj_schemas`) hold the authoritative headerless column layouts.
Dev uses a bounded recorte (6 lookup tables + Simples); the 10-part giants
(Empresas/Estabelecimentos/Socios, ~26 GB decompressed) are opt-in.

## Consequences
- The share token is a public share id (documented, `# gitleaks:allow`); a live
  integration test detects rotation (401).
- All CNPJ keys are strings (alphanumeric CNPJ effective 2026-07-31).
- Bronze (next plan) reads the landed files via Auto Loader.

## Validation notes (Task 6, harness phase)

- **The live RFB server is flaky (~50% transient HTTP 500s observed).** Manual
  probing during Tasks 2–6 showed roughly half of PROPFIND/GET requests to
  `arquivos.receitafederal.gov.br` returning transient HTTP 500s with no
  discernible pattern. `WebDavClient` was hardened with a shared
  retry-with-backoff wrapper (`_request_with_retry`) around both `list_dir`
  (PROPFIND) and `download` (GET): retries on `ConnectionError`, `Timeout`, and
  HTTP `{500,502,503,504}`, up to 5 attempts with exponential backoff
  (0.5s, 1s, 2s, 4s), re-raising the last error on final failure. This is
  required for any multi-file run against the live share to have a realistic
  chance of completing without manual re-runs.
- **Real run, tiny-lookup recorte only.** `docs/f1-extraction-evidence.md`
  artifacts a real `scripts/extract_cnpj.py` invocation for the six tiny
  lookup groups (`Cnaes,Motivos,Municipios,Naturezas,Paises,Qualificacoes`),
  landing all 6 files to the UC Volume in one run. `Simples` (~300 MB
  compressed) was deliberately excluded from the artifacted run to keep it
  fast and avoid pulling a large payload over a flaky link during evidence
  capture; it is still part of the default `RECORTE_GROUPS` for a full dev
  recorte run.
