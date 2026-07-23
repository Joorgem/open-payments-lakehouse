# open-payments-lakehouse

> A production-grade lakehouse that ingests **real Brazilian public data**
> (Receita Federal CNPJ registry) and a realistic synthetic payment stream,
> integrates them by business key with **Data Vault 2.0**, serves a **Kimball**
> dimensional model on **Delta Lake**, and ships with full **DataOps** —
> CI/CD, schema contracts, quality gates, Unity Catalog governance,
> observability, and an AI agent that accelerates data-incident RCA.

**Status:** F0 (harness) — see `docs/f0-validation-report.md`.

## Why this isn't a tutorial repo
- Real, messy government data (latin-1, `;`-delimited, no header, monthly snapshots).
- A PySpark/Delta core tested locally in CI (`uv run pytest`), deployed to Databricks
  via Asset Bundles — dual-target, with honest Free Edition limits documented.

## Run locally
```
uv sync --all-groups
uv run pytest              # unit
docker compose up -d       # postgres + redpanda
uv run pytest -m integration
```
