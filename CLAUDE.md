# open-payments-lakehouse — agent context

## What this is
Flagship data-engineering project for job 102697. Real Receita CNPJ data +
synthetic payment stream → Data Vault 2.0 → Kimball dimensional on Delta Lake,
with full DataOps. See the design spec in the campaign repo.

## Commands
- Install: `uv sync --all-groups --all-extras`
- Test (unit): `uv run pytest`
- Test (integration): `docker compose up -d && uv run pytest -m integration`
- Lint: `uv run ruff check .`
- Databricks: profile `opl-free`; `cd databricks && databricks bundle validate -t free`

## Autonomy rules
- Autonomous: code, tests, docs, commits on a branch, bundle validate, local runs.
- Human gate (Jorge): creating accounts/tokens, marking repo public, final push to
  main.
- Architectural decisions are the agent's, taken from research and a complete
  reading of the project rather than handed to Jorge as a menu at each fork. State
  the decision and what drove it, and say what was rejected, so overruling stays
  cheap.
- Never commit secrets. PAT only in git-ignored .env / GitHub secret.
- English only. Conventional commits explaining *why*.

## Local environment

Local Spark requires a JVM and Hadoop native bits that are **not** part of the
`uv` environment and must be present on the machine running `uv run pytest`
(and are already installed on this dev box as **User-level** environment
variables):

| Variable | Value |
|---|---|
| `JAVA_HOME` | `C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot` |
| `HADOOP_HOME` | `C:\hadoop` |

- `winutils.exe` + `hadoop.dll` (Hadoop 3.3.6, from `github.com/cdarlint/winutils`)
  live in `C:\hadoop\bin\` and are required for local Spark on Windows.
- `%JAVA_HOME%\bin` and `%HADOOP_HOME%\bin` are prepended to the **User** `PATH`.
- These are User-level Windows environment variables: **a freshly opened
  terminal session picks them up automatically**, but a shell/tool session
  that was already running when they were set (or any non-interactive/agent
  shell spawned before that point) will **not** see them and must `export`
  them inline before running Spark-dependent commands, e.g.:
  ```bash
  export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
  export HADOOP_HOME="/c/hadoop"
  export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"
  ```
- Local Spark pin: `pyspark==3.5.9` (not `3.5.3`) to work around SPARK-53759, a
  Windows + Python 3.12 local-mode worker crash fixed upstream in 3.5.9. See
  ADR 0001 for why this is safe relative to the DBR 16.4 LTS (Spark 3.5.2)
  deploy target.
- Always invoke Spark/Python via `uv run ...`, never a bare `python` — there is
  a stray Python 3.14 install earlier on this machine's system `PATH` that is
  not the project's interpreter.

### Databricks CLI / credentials
- CLI: installed via winget (`Databricks.DatabricksCLI`, v1.8+). Its dir is on the
  **User** `PATH`, but (same caveat as `JAVA_HOME` above) an agent/tool shell
  spawned from a process that predates the setting will not see it — export
  inline in that case. Resolve the winget packages dir from the environment rather
  than hardcoding it: the literal path contains the operator's Windows username,
  which is what the run-evidence docs redact as `<WINGET-PACKAGES-DIR>` and must
  not be committed here either.
  ```bash
  # Git Bash: $LOCALAPPDATA is a Windows-style path, so convert it first.
  export PATH="$(cygpath -u "$LOCALAPPDATA")/Microsoft/WinGet/Packages/Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe:$PATH"
  ```
  If that dir has been renamed by a winget upgrade, find it with
  `winget list --id Databricks.DatabricksCLI` or
  `ls "$(cygpath -u "$LOCALAPPDATA")/Microsoft/WinGet/Packages" | grep -i databricks`.
- CLI profile: `opl-free` (configured via `databricks configure --profile opl-free`).
- Secrets live in a git-ignored `.env` (never committed) with `DATABRICKS_HOST`
  and `DATABRICKS_TOKEN`; source it before Databricks CLI/SDK commands:
  `set -a && source .env && set +a`.
- Asset Bundle (`databricks/databricks.yml`) does **not** set `workspace.host`
  — that field is for bundle variable interpolation, not shell env vars — the
  `opl-free` profile resolves the host instead. See ADR 0002.

### Local Docker stack
- Postgres is mapped to **host port 5433**, not the default 5432, to avoid
  colliding with other local projects. Connect via `localhost:5433` from the
  host; the container's internal port is still 5432.

### CNPJ data access
- Receita Federal's CNPJ monthly snapshots are **not** a plain static file
  server — they're served from a Nextcloud instance ("SERPRO+") via a public
  WebDAV share (`https://arquivos.receitafederal.gov.br/public.php/webdav/...`,
  HTTP Basic auth using the public share token as username). See
  `docs/f0-validation-report.md` and ADR/Task reports for F1 (extraction
  layer) for the full access pattern and history depth (gapless monthly
  snapshots from 2023-05 onward).
