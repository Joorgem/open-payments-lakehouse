# F1.1 extraction evidence (carry-forward I1)

Real, non-mocked run of `scripts/extract_cnpj.py` against the live RFB
Nextcloud WebDAV share, for the tiny-lookup subset of the dev recorte
(`Cnaes,Motivos,Municipios,Naturezas,Paises,Qualificacoes` — `Simples`,
~300 MB compressed, excluded from this artifacted run to keep it fast over a
flaky link; see ADR 0003).

## Command

```bash
set -a && source .env && set +a && uv run python scripts/extract_cnpj.py \
  --month 2026-06 --groups Cnaes,Motivos,Municipios,Naturezas,Paises,Qualificacoes
```

## Output (real, verbatim)

```
completeness 2026-06 for ['Cnaes', 'Motivos', 'Municipios', 'Naturezas', 'Paises', 'Qualificacoes']: ok=True missing=[]
  landed Cnaes.zip -> /Volumes/workspace/default/landing/cnpj/2026-06/F.K03200$Z.D60613.CNAECSV (88215 B)
  landed Motivos.zip -> /Volumes/workspace/default/landing/cnpj/2026-06/F.K03200$Z.D60613.MOTICSV (2944 B)
  landed Municipios.zip -> /Volumes/workspace/default/landing/cnpj/2026-06/F.K03200$Z.D60613.MUNICCSV (120435 B)
  landed Naturezas.zip -> /Volumes/workspace/default/landing/cnpj/2026-06/F.K03200$Z.D60613.NATJUCSV (4187 B)
  landed Paises.zip -> /Volumes/workspace/default/landing/cnpj/2026-06/F.K03200$Z.D60613.PAISCSV (5444 B)
  landed Qualificacoes.zip -> /Volumes/workspace/default/landing/cnpj/2026-06/F.K03200$Z.D60613.QUALSCSV (2355 B)
done: 6 files
```

6 of 6 targeted files landed to `/Volumes/workspace/default/landing/cnpj/2026-06/`
on the Databricks Free Edition workspace (`opl-free` profile), via the
control-plane PAT upload path validated in ADR 0002.

## RFB server reliability observation

Across Tasks 2–6, ad-hoc probing of `arquivos.receitafederal.gov.br` showed
roughly half of PROPFIND/GET requests returning transient HTTP 500s, with no
obvious pattern (not rate-limit-shaped, not consistently tied to file size).
This run happened to complete without hitting a 500 on any of the 6
downloads/list, so the transcript above shows no visible retries — but the
retry/backoff added to `WebDavClient` in this same task (commit `b656a05`,
`_request_with_retry`: 5 attempts, exponential backoff 0.5/1/2/4s, retrying on
`ConnectionError`/`Timeout`/HTTP `{500,502,503,504}`) is what makes runs like
this reliable in general — without it, a ~50%-per-request failure rate would
make a multi-file recorte run fail more often than not. Unit tests in
`tests/test_webdav.py` (`test_download_retries_on_transient_500_then_succeeds`,
`test_list_dir_retries_on_transient_503_then_succeeds`,
`test_download_raises_after_exhausting_retries`) exercise this behavior with
mocked transient failures.
