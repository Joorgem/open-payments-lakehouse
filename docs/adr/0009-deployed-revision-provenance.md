# ADR 0009 — a run must name the revision it expects, and the wheel must say what it was built from

## Status
Accepted, and **the refusal has not yet been seen against the live workspace**.
The mechanism is argued below and locked by unit tests; the deliberate
wrong-revision deploy that proves it refuses before doing work is Step 6 of the
task that introduced it and is dispatched separately, against the live Free
Edition workspace. Until that entry appears in the phase's run-evidence doc, this
ADR describes a control proven locally and not yet observed biting in production.
A provenance check nobody has seen refuse is a provenance check nobody should
trust, and this paragraph is the honest form of that.

## Context

**CI validates the repository. It never validates what is deployed.** Every test
in this repo reads the working tree; a green Databricks run is evidence about the
artefact that is *in the workspace*, and that artefact is `HEAD` only if somebody
deployed `HEAD` — a separate act, performed by hand, with no check behind it.

That gap produced two incidents in one day during F1.4b PR A:

1. A wheel whose build predated every commit on the branch reached a run. It was
   caught by comparing timestamps by hand.
2. A socios re-run **terminated SUCCESS having masked only bronze**, because the
   workspace was still running the bundle deployed four commits earlier — the
   commits that masked the quarantine table had never left the laptop. Nothing
   went red. It was caught by reading the task log against the source.

Both are the same shape: the run was evidence about an artefact nobody had
checked the identity of. Neither is detectable from inside the repository.

**Nothing in the artefact encoded its source revision.** `pyproject.toml`
declares a static `version = "0.0.0"` with no `dynamic`, no hatch-vcs and no
setuptools-scm; the installed dist-info is literally
`open_payments_lakehouse-0.0.0.dist-info`. `opl._version_check` checks
`pyspark`/`delta-spark` and carries nothing about this project's own revision. So
a provenance check needed a value that did not exist yet, and inventing where it
comes from is the whole of this decision.

## Decision

Two values from two places, compared by a task that runs first in the job and
does nothing else.

**Expected — bound at RUN time, from the local repository:**

```
databricks bundle run bronze_cnpj_socios -t free \
  --params month=2026-07,revision=$(git rev-parse HEAD)
```

**Actual — read from the deployed artefact**, out of `opl/_revision.py`, a module
that does not exist in the source tree and is generated into the wheel by a
hatchling custom build hook (`hatch_build.py`) at the moment
`artifacts.opl_wheel.build: uv build --wheel` runs, i.e. during
`databricks bundle deploy`.

The comparison lives in `opl.bronze.provenance` (pure, no Spark, no filesystem,
no subprocess); `databricks/src/assert_deployed_revision.py` is the job task that
hands it the two values and prints the verdict.

### Why the expected value is bound at run time and not at deploy time

The obvious candidate was a bundle variable:
`databricks bundle deploy -t free --var="revision=$(git rev-parse HEAD)"`. It is
wrong, and it is wrong in the direction that matters. Both values would then be
stamped by the *same* `bundle deploy`:

| incident | expected | actual | verdict |
|---|---|---|---|
| stale wheel inside a fresh deploy | new HEAD | the wheel's old SHA | **caught** |
| **nobody deployed at all** | old SHA | the same old SHA | **passes** |

The second row is incident 2 above — the one this control exists for. A
deploy-time expected value satisfies "two different sources" in letter and
violates it in purpose, and worse, the wrong-revision deploy that is supposed to
prove the check works passes either way, so the proof could not distinguish a
working check from a vacuous one.

Binding the expected value at run time makes a forgotten deploy a mismatch,
because the wheel still carries the SHA of whenever it was last built. It catches
both incidents rather than one.

`--params` is not a new mechanism: the three per-table jobs already declare a
`month` job parameter and PR A drove them with `--params month=2026-06`, so
adding a second key is the smallest possible change to how a run is launched.

### Why the stamp is a generated module and not the package version

Putting the SHA in the version (hatch-vcs, or any `dynamic = ["version"]`) would
put it in the **wheel filename**, and the jobs receive the wheel through a glob:

```yaml
dependencies:
  - ../../dist/*.whl
```

over a `dist/` that nothing cleans. Today the filename is constant, so a rebuild
overwrites the single wheel and the glob cannot be ambiguous. A SHA in the
version would leave one wheel per commit in `dist/`, all matching the glob — it
would *manufacture* the two-wheel condition that is the most plausible mechanism
for incident 1. A version-shaped stamp is therefore not a neutral alternative to
a module-shaped one; it is the same class of defect as the one being fixed.

The glob itself is **not changed here, and it is now detected rather than
prevented**: whichever wheel wins the glob still has to name the revision the
operator passed, so a stale wheel from an uncleaned `dist/` refuses instead of
running. The same holds for any staleness further down the path we cannot see
from here — a serverless environment that caches an install by filename, for
instance. The guard does not need to enumerate those mechanisms; it only needs
the artefact to state its own identity.

### Why the build hook and not a stamp written into `src/`

The alternative was to make `artifacts.opl_wheel.build` stamp then build, writing
`src/opl/_revision.py` into the working tree. Rejected for two reasons, one of
them measured:

- The file would then be picked up by the **editable** install every local
  `uv run pytest` uses, so a developer's tree would carry a real-looking SHA and
  a provenance check would pass locally against a value nothing verified. An
  absent stamp must refuse (below); a stale-but-plausible one cannot.
- It puts a generated file in the source tree, where it can be committed, and
  makes the stamp exist only for builds that go through the bundle.

The hook **deliberately does nothing for editable builds** (`version ==
"editable"`), and that guard is load-bearing rather than tidy. Measured on a
throwaway package: without it, hatchling force-includes the stamp into the
editable wheel, which creates a shadow `site-packages/<pkg>/` directory holding
only `_revision.py`. The package still resolves to `src/<pkg>` (a regular package
beats a namespace portion), so the stamp is unreachable *and* there is now a
second directory competing for the package name. Skipping editable builds leaves
a local install with no stamp at all, which is exactly what we want it to have.

### Absent, empty or placeholder refuses

- The four ingestion jobs and the repromote job declare
  `revision: REQUIRED-PASS-A-REVISION` as the job-parameter default —
  `opl.bronze.provenance.SENTINEL_REVISION`, following the precedent of
  `promote_batch`'s sentinel `batch_id`. A run launched without `--params
  revision=...` is refused, not waved through.
- The expected value must be a whole object name (40 or 64 lowercase hex digits)
  and nothing else. That one rule refuses the sentinel, an empty string, a branch
  name, `HEAD`, and an abbreviated SHA — an abbreviation because the value is
  compared for equality, and accepting a prefix would mean deciding how short is
  short enough.
- An artefact with no stamp — a local editable install, or a wheel built where
  `git` was unavailable — reports no revision and is refused, naming the
  deployment path that produces one. **The wiring test asserts the YAML default
  is a value the guard refuses**, because a `revision:` default that happened to
  be a valid SHA would make a forgotten `--params` pass silently, which is the
  failure this whole ADR is about wearing a different hat.

### A SHA that matches while the tree differs

`git rev-parse HEAD` answers in a tree with uncommitted changes exactly as it
does in a clean one, so a naive stamp would let a wheel built from modified
sources claim a commit that does not describe it. The hook therefore measures
dirtiness — `git status --porcelain -- src pyproject.toml` — and stamps
`<sha>+dirty` when it is non-empty, which no expected value can equal, so the run
refuses and the message says why.

The set of paths is narrow **on purpose, and it is the wheel's own inputs**:
`[tool.hatch.build.targets.wheel] packages = ["src/opl"]` plus the metadata in
`pyproject.toml` are what end up in the artefact. Untracked docs, evidence files
and plans cannot change the wheel and must not refuse a run — a guard that fires
on an unrelated new file in `docs/` is a guard operators learn to route around,
and this phase writes evidence documents while it runs. Ignored paths
(`__pycache__/`, `dist/`, `.venv/`) are excluded by `--porcelain`'s own defaults,
which is why a local build does not read as dirty.

The consequence is deliberate and worth stating plainly: **you cannot launch a
job from a tree with uncommitted changes under `src/`.** Commit or stash first.
That is one command, and the alternative is a green run whose provenance claim is
false.

### Which jobs carry the guard, and which does not

Wired **first, ahead of every other task**, in the four ingestion jobs
(`bronze_cnpj_empresas`, `bronze_cnpj_estabelecimentos`, `bronze_cnpj_socios`,
`bronze_cnpj_lookup`) and in `repromote_triaged_batch`:

- ahead of `ensure_masked_table` in socios, which is the one job that already had
  a task before its `unzip`. The guard writes nothing and reads nothing, so it
  does not weaken ADR 0008's "the masks were applied before any byte landed" —
  the masking task is still the first thing that *touches a table*.
- ahead of `ingest` in the lookup job, which has no `unzip` task at all.
- in `repromote_triaged_batch`, because a repromote is an operator action against
  already-landed data and appends to bronze. A repromote run by a stale wheel
  appends rows the current rules would have rejected, into the table that is the
  system of record. The isolation argument in that job's header ("nothing
  automated reaches it") is an argument about *what starts* the job, not about
  which code it runs.

**`opl_smoke` deliberately does not carry the guard.** Its entire purpose is to
answer "does the deployed wheel import and can it read config" — it is the probe
you run precisely when you suspect the deployment, and a guard on it would make
the diagnostic unavailable in the case it exists for. It writes nothing, so a
wrong revision costs a re-run and nothing else. Instead it **reports**: its one
print line now names the revision the deployed wheel was built from, so an
operator can read what is in the workspace without having to make a job fail to
find out. Excluded from the guard, not excluded from provenance — and the wiring
test asserts that exclusion rather than leaving it to be inferred from absence.

## Consequences

- Every job run now takes a `revision` parameter. A forgotten one refuses in
  seconds, before Spark, having done nothing.
- A deploy that did not happen becomes a red run whose message names both
  revisions, instead of a green run against last week's code.
- The guard is a `spark_python_task` that never builds a session, so its cost is
  a serverless task start and no compute.
- `hatchling` is now a declared dev dependency. It was already present in the
  isolated build environment; the tests read `hatch_build.py`, and a test that
  imports a package nobody declared keeps working only for as long as whatever
  happens to pull it in stays.
- **This guard is for operator-launched runs.** None of these jobs is scheduled
  today. A scheduled run has no local repository to bind an expected revision
  from, so adding a schedule means answering this question again rather than
  passing the sentinel — and the sentinel refusing is the correct default in the
  meantime.
- `git` is consulted in exactly one place in this repo: the build hook, on the
  machine that builds the wheel. Neither `opl.bronze.provenance` nor the job task
  may shell out to it, and a test enforces that. The deployed side has no
  repository, so a check that consulted one there would either crash or compare
  the operator's repo against itself.
