# F2 wave 1 — Complete Run Evidence: the CNPJ raw vault, the observation ledger, the two spec departures, the suite reconciliation and the correction pass

**This document covers all of F2 wave 1 — Tasks 0 through 7** on branch
`feat/f2-wave-1-cnpj-vault`, from `44018ad` (merge base) to `f71355b`, plus
Task 7's own **five** commits and the final whole-branch review's fix wave
(both listed in [§4](#4-what-this-branch-builds)).
Task 0 recovers the headroom `src/opl/bronze/registry.py`
had run out of and closes two blind-pass routes in its wiring lock; Task 1 lands
the DV2 business-key hash standard; Task 2 derives the **observation ledger**
that this phase's central claim rests on (ADR 0010); Task 3 builds `hub_empresa`
and `sat_empresa_dados` with `applied_date` split from `load_date`; Task 4 adds
`hub_estabelecimento`, its two satellites and the hierarchical link (ADR 0013);
Task 5 removes `hub_socio` on a measurement and drives the effectivity satellite
by disappearance (ADR 0011); Task 6 adds six reference tables as a fifth kind;
Task 7 is this document, ADRs 0012 and 0013, `scripts/run_suite.sh` and the
correction pass.

**Nothing in wave 1 runs against the workspace.** Every vault loader in it has
been executed only against local-Spark Delta fixtures. Every workspace number
below is a measurement of the **source**, taken by the controller to justify or
refute a modelling decision — not a measurement of this code at scale. That
distinction is load-bearing and is repeated wherever a number could be mistaken
for the other kind.

## ⚠️ Read these four before trusting anything below

1. **Three modelled paths are not exercised by any data, real or synthetic at
   scale**, and this document reports them as untested rather than as working:
   empresas end-dating, the satellite's dedup tie-break, and reference-table
   history. [§18](#18-three-paths-this-phase-did-not-exercise).
2. **The `hash_diff` demonstration on real bronze
   ([§11.2](#112-hash_diff-and-applied_date-on-real-bronze--and-the-caveat-that-must-travel-with-it))
   uses a SQL stand-in, not `opl.vault.hashing`'s encoding.** It demonstrates the
   premise and the mechanic. It is not the module's output and its digests are
   not the vault's digests.
3. **The estabelecimentos change rates were first quoted over fewer columns than
   the payloads they described.** Found by the correction pass, **re-measured by
   the controller, and now closed**: `_dados` is **1,211,834 / 1.69%** over its
   full six columns, not 1,076,696 / 1.50% over four. The split's justification is
   **stronger** than first claimed — ~2.13×, not ~1.9×.
   [§23.2](#232-the-measurement-that-was-quoted-wider-than-it-was-taken--found-re-run-and-closed).
4. **The Unicode-skew pin is a strict equality over 40 known divergences**, so a
   JDK upgrade in **either** direction turns the suite red rather than silently
   re-keying the vault. [§19](#19-the-unicode-pin-is-an-equality-and-that-is-the-whole-safety-property).

Workspace host, org id and the operator's OS username are redacted, as in the
F1.2–F1.4b evidence docs. Databricks statement ids are kept — they are what every
workspace claim below is checked against.

---

## 1. How to read this document

**Every number comes from a quoted command or a quoted statement id.** A number
without one does not appear.

**Attribution is explicit and matters more here than in any previous phase**,
because this phase deliberately split the work: implementers built code and
local-Spark fixture tests and touched **no** workspace; the **controller** ran
every workspace measurement and every full-suite run. The reason was mechanical
— the agent shell caps a command at 600 s, the suite exceeds it, and three
implementer agents stalled waiting on backgrounded runs before the cause was
diagnosed.

Throughout:

- **Controller-verified** — run by the controller, independently of the
  implementer's report. Workspace measurements carry a statement id.
- **Implementer-reported** — from a task report, not independently re-run. Where
  only an implementer ran something, this document says so in the sentence.
- **Task 7** — run in this task, in this session. Quoted with its command.

**One thing the split cannot give you.** Controller re-verification catches a
transcription error or a stale head. It does not catch a shared wrong premise —
which is exactly how the controller's own "no single table carries both absence
states" survived being written into a respec, quoted into a task brief, and
copied into an ADR before a measurement refuted it
([§7.2](#72-the-controller-corrected-its-own-respec)).

## 2. Toolchain

- Local test/lint through `uv run`; local Spark `pyspark==3.5.9` (ADR 0001),
  Delta 3.3.1, JDK 17 (`java.version 17.0.19`), CPython 3.12.13.
- Workspace measurements through the Databricks SQL warehouse under profile
  `opl-free`, run by the controller. **No implementer had workspace access at any
  point in this phase.**
- `bash` 5.2.37 (Git Bash) for `scripts/run_suite.sh`.
- No bundle deploy, no job run, no workspace write occurred in this phase.

---

## 3. The suite, from one command

### 3.1 Why this needed a script

By the end of Task 6 the suite was ~1,460 s of work across four chunks and the
agent shell's hard cap is **600 s per command**. "The suite passes" had therefore
become four hand-stitched pytest invocations pasted into a ledger — four numbers
with four timestamps, which invites exactly the "which run was that from?"
ambiguity this document format exists to remove. A sum alone does not fix it: two
chunks that overlap by three tests while a third silently loses three still sums
correctly.

`scripts/run_suite.sh` runs the partition and prints **one reconciled summary**.
The reconciliation is in the script's output, not in prose around it:

- the chunks' collected node ids are compared **as sets** against the bare
  suite's, which is what proves the partition total *and* non-overlapping;
- the chunk pass counts are summed and compared against the suite's own
  `--collect-only` selected count — a second, independent derivation of the same
  number;
- a chunk that outgrows the cap fails **loudly, naming itself**, rather than
  being discovered as a mysterious kill with no output;
- exit 0 means the whole suite ran and reconciled. Exit 2 means **nothing is
  claimed about the suite** — a partial run *and* a `--collect-only` run both
  land there, each with its own banner, so neither can be pasted as evidence
  that the suite passes.

**It cannot report success while broken, and that guard was earned.** The first
end-to-end run printed `VERDICT: RECONCILED` and then emitted
`line 231: $'\n    fi\n    printf ': command not found` and
`line 241: $1: unbound variable` — **at line numbers past the end of the
228-line file that was running** — and exited **0**. Cause: bash reads a script
**incrementally, by byte offset, while executing it**, so a ~1,470 s run holds
the file open for twenty-five minutes, and the fix wave's own edit to it landed
inside that window; bash resumed at an offset that now pointed into different
text. Reproduced deliberately with a three-line probe, then fixed by ending the
file with `{ main "$@"; exit $?; }` — a compound command bash parses in full
before executing, so the `exit` is already in hand and nothing is read after it.
**The weaker `main "$@"` followed by `exit $?` was tested and does not work**:
that `exit` is read after `main` returns, from the offset that has already moved.
A tool whose purpose is to be quotable must not be able to print a green verdict
and then fail silently.

**Its log directory is repo-relative and git-ignored, and that is a privacy
decision.** The script prints its `logs:` line unconditionally and this document
tells an operator to paste that output verbatim; an earlier version rooted the
directory at `${TMPDIR:-/tmp}`, which on Windows is
`C:\Users\<operator>\AppData\Local\Temp` — so the paste would have carried the
operator's OS username into a published document that promises above to redact
it. The root is now `.run-suite-logs/`, printed relative, with **no absolute
path anywhere in the output**. That is the single `.gitignore` entry this branch
adds.

### 3.2 The partition reconciles — Task 7, verbatim

```
$ bash scripts/run_suite.sh --collect-only

opl test suite, run as a reconciled partition of 4 chunks
logs: .run-suite-logs/20260808-013951-477777

COLLECTING (no tests run yet)
  non-vault                 710 selected
  vault-cnpj-hashing         59 selected
  vault-estab-socios         51 selected
  vault-ledger-registry     102 selected
  (bare suite)              922 selected, 6 deselected

PARTITION
  partition vs suite      0 in no chunk, 0 in no suite run, 0 in two chunks

VERDICT: COLLECT-ONLY -- partition reconciled, no tests run

EXIT=2
```

710 + 59 + 51 + 102 = **922**, and the four chunks' node-id sets union to exactly
the suite's — no test in no chunk, no test in two chunks. (This is the
**collect-only** reconciliation, which runs no test and exits 2; the full run that
does is [§3.3](#33-the-suite-from-one-command-end-to-end--the-final-fix-wave-verbatim),
where the same four chunks collect 932 after the fix wave's ten new tests.)

### 3.3 The suite, from one command, end to end — the final fix wave, verbatim

**This is the run the whole script exists to make quotable, and it is the first
time anyone has had it.** Task 7 could only run two chunks through the script (the
600 s cap forbids the ~1,500 s the four take together); §3.4 below is the
four-hand-stitched measurement it had to fall back on. The command, its full
output, and its exit code:

```
$ bash scripts/run_suite.sh

opl test suite, run as a reconciled partition of 4 chunks
logs: .run-suite-logs/20260808-022948-502938

COLLECTING (no tests run yet)
  non-vault                 710 selected
  vault-cnpj-hashing         59 selected
  vault-estab-socios         54 selected
  vault-ledger-registry     109 selected
  (bare suite)              932 selected, 6 deselected

PARTITION
  partition vs suite      0 in no chunk, 0 in no suite run, 0 in two chunks

RUNNING
  non-vault                710 passed    0 failed    376s  rc=0
  vault-cnpj-hashing        59 passed    0 failed    244s  rc=0
  vault-estab-socios        54 passed    0 failed    555s  rc=0  !  within 2 min of the cap
  vault-ledger-registry    109 passed    0 failed    327s  rc=0

RECONCILIATION
  chunk passes summed        932
  --collect-only selected    932   (938 collected, 6 deselected)
  logs                    .run-suite-logs/20260808-022948-502938

VERDICT: RECONCILED -- 932 passed, 932 selected, agreed by two derivations

EXIT=0
```

**What this establishes that four stitched invocations could not.** 710 + 59 + 54
+ 109 = **932**, matched against the bare suite's own `--collect-only` count by a
second, independent derivation; and the node-id **sets** union to exactly the
suite's — `0 in no chunk, 0 in no suite run, 0 in two chunks` — which is what
proves the partition total *and* non-overlapping. One command, one timestamp, one
exit code. **Nothing trails the verdict**, which is the [§3.1](#31-why-this-needed-a-script)
guard doing its job.

**932, not 922**, because the final fix wave added ten tests: 3 on `changed_rows`'
own precondition, 4 on `discover_domains`' refusals, 2 on the satellite's newly
counted fold, and 1 on the link-grain refusal message. No new
`src/opl/**/*.py` file, so `test_revision_stamp` is unchanged.

### 3.3.1 ⚠️ The partition is one task away from needing a re-split

**`vault-estab-socios` ran 555 s against a 600 s cap** and tripped the script's own
warning — measured, not predicted. It was **519 s** on the controller's run
immediately before; the ten new tests put ~36 s into that chunk and moved it into
the warn band.

That is an operational fact for whoever adds wave 2's tables, and it is why the
script **fails loudly rather than warns** at the cap: the partition is a property
of today's runtimes, not a design. The thresholds are `!  within 2 min of the cap`
at 480 s and `!! OVER THE 600s CAP -- SPLIT THIS CHUNK` at 600 s, both overridable
via `SUITE_CHUNK_WARN` / `SUITE_CHUNK_CAP`.

**The next task that adds a socios or estabelecimentos test should expect to split
that chunk**, which is a two-line edit to `CHUNKS` in `scripts/run_suite.sh` — the
reconciliation then proves the new partition is still total and non-overlapping,
which is precisely the case it was built for.

### 3.4 The previous measurement, kept for provenance

**Controller-verified at `f71355b`**, four foreground chunks, before the script
could be run end to end — background runs had stopped surviving in that session
(three consecutive kills) and the suite no longer fits one 600 s call. Superseded
by §3.3 and kept because it is what the 922 figure elsewhere in this document
refers to:

| chunk | command | result |
|---|---|---|
| non-vault | `uv run pytest --ignore=tests/vault -q` | **710 passed**, 6 deselected (334.65 s) |
| vault: cnpj + hashing | `uv run pytest tests/vault/test_cnpj_vault.py tests/vault/test_hashing.py tests/vault/test_hashing_spark.py -q` | **59 passed** (219.60 s) |
| vault: estab + socios | `uv run pytest tests/vault/test_estabelecimento_vault.py tests/vault/test_socios_vault.py -q` | **51 passed** (567.55 s) |
| vault: ledger, registry, loading, reference | `uv run pytest tests/vault/test_loading.py tests/vault/test_observation.py tests/vault/test_registry.py tests/vault/test_reference_vault.py -q` | **102 passed** (330.38 s) |
| — | `uv run ruff check .` | All checks passed |

### 3.5 Task 7's own verification, after the correction pass

The correction pass edits docstrings and renames one test. The files it touched
were re-run:

```
$ uv run ruff check .
All checks passed!

$ uv run pytest tests/vault/test_hashing.py tests/vault/test_hashing_spark.py -q
31 passed in 34.98s

$ uv run pytest tests/vault/test_observation.py -q
28 passed in 145.50s (0:02:25)

$ uv run pytest tests/vault/test_socios_vault.py -q
26 passed in 270.77s (0:04:30)
```

`test_observation.py` is at **800 of 800 lines** after the pass — the correction
notes were condensed twice to stay under the cap, and the condensing was applied
to Task 7's own additions rather than to pre-existing argument, for the reason
[§22](#22-deferred-minors-triaged) records about Task 4.

### 3.6 Timing is contention, not a code property — a correction to this phase's own record

Observed full-suite wall clock on the same machine, in order: **586 s**
(`f1117e6`, 713 tests) → **850 s** (`f347b5d`, 739 tests) → **414 s**
(`e1951b0`, 742 tests). More tests, less than half the time.

The 850 s run overlapped with subagents doing their own local-Spark work; the
414 s run had the machine to itself. A planned Parquet-fixture optimisation had
been justified by the 586 → 850 jump; that jump is unattributable and **the
optimisation was dropped**. **Never compare two suite timings taken while
subagents were running.** Every timing in this document is reported as a
measurement of a moment, not as a property of the code.

---

## 4. What this branch builds

`git diff 44018ad...f71355b --stat` — the branch **through Task 6**, which is
where the code lands: **37 files changed, 11,188 insertions, 255 deletions**.
Task 7's own commits (listed below) add this document, three ADRs,
`scripts/run_suite.sh` and two correction passes on top of that. The vault
package, with line counts taken **after** those passes
(`wc -l src/opl/vault/*.py src/opl/vault/domains/*.py`):

| file | lines | what |
|---|---|---|
| `src/opl/vault/hashing.py` | 229 | the BK hash standard, pure Python, pyspark-free |
| `src/opl/vault/hashing_spark.py` | 316 | the same standard as a Catalyst expression (ADR 0012) |
| `src/opl/vault/observation.py` | 433 | the five-state observation ledger (ADR 0010) |
| `src/opl/vault/specs.py` | 487 | the five table kinds and their `__post_init__` guards |
| `src/opl/vault/registry.py` | 511 | discovery, the whole-set guards, the union |
| `src/opl/vault/loading.py` | 341 | the one spelling of the hash-key expression and the month window |
| `src/opl/vault/hubs.py` · `links.py` · `satellites.py` | 145 · 234 · 391 | the three generic loaders |
| `src/opl/vault/partners.py` · `effectivity.py` | 310 · 441 | the socios link and its window (ADR 0011) |
| `src/opl/vault/reference.py` | 273 | six reference tables, routed around a `codigo` collision |
| `src/opl/vault/domains/cnpj.py` | 413 | every table this domain declares |
| `src/opl/vault/columns.py` · `months.py` · `domains/__init__.py` | 92 · 74 · 69 | shared names and discovery |

Test files, all under the 800-line cap: `test_observation.py` **800 exactly**,
`test_socios_vault.py` 791, `test_cnpj_vault.py` 772, `test_registry.py` 758,
`test_estabelecimento_vault.py` 721, `conftest.py` 473, `test_reference_vault.py`
378, `test_hashing_spark.py` 341, `test_hashing.py` 311, `test_loading.py` 234.

**Counts are at HEAD, after the final fix wave**, which is why several moved:
`cnpj.py` was quoted at 404 here and was 410 even before that wave — a stale
count, found by the docs review. **Three files are now worth watching against the
cap**: `test_observation.py` at 800/800 (Task 7's correction notes were condensed
twice to fit), `test_socios_vault.py` at 791, and `test_registry.py` at 758 after
four discovery-refusal tests landed. `test_estabelecimento_vault.py` hit 840
during Task 4 and was brought back to exactly 800 before Task 5's fixtures were
extracted into `conftest.py`; that extraction is why it has room again.

**Tasks 0–6, oldest first** (`git log --oneline 44018ad..f71355b`):

```
e18e2fe test: the wiring lock could not see a guard that left registry.py
82cbe61 refactor: the collision guards move out, and the guard the cap blocked lands
ef8c3ce fix: close the wiring lock's own filename-shaped blind spot
ab7d879 feat: the DV2 business-key hash standard, length-prefixed against delimiter collision
f1117e6 fix: refuse a bare str business key, and correct two overclaiming docstrings
f347b5d feat: derive the observation ledger, and split absence into two states
e1951b0 fix: wire the observation window guard, and stop overclaiming a search gap
e02263f feat: the hash standard as a Spark expression, and the test that pays for it
e252553 feat: a vault registry holding no tables, so wave 2 adds a domain by adding a file
ec654cd feat: hub_empresa and sat_empresa_dados, with applied_date split from load_date
9dbb15a fix: sweep the case tables, pin the Unicode skew, and close three unguarded seams
6b3c502 feat: hub_estabelecimento, its two satellites, the hierarchical link, and the Link kind
d7bc920 fix: test the link loader's column guard, and put the measured change rates in
1d51770 test: share the vault fixtures, because the capped file had no room for task 5
9a4e4f8 feat: link_company_partner without a hub_socio, and the window it closes on absence
1e806b0 docs: ADR 0011, the phase's sharpest domain result -- the spec said hub, the data said bucket
c3b17a9 fix: one spelling of the identifying-end filter, and three guards that were missing
9bfa9d8 feat: reference tables as a fifth vault kind, routed around a codigo collision
f71355b fix: collapsed_duplicates counts what it claims, and specs.py gives kinds room
```

**Task 7, oldest first** (`git log --oneline f71355b..HEAD`) — five commits, not
one, and this document is only the third of them:

```
bca002b test: run the suite as one reconciled command, because four stitched ones cannot be quoted
16b6ade fix: twenty statements this phase falsified in its own files, two of them corrections that overshot
c57ae63 docs: ADRs 0012 and 0013, the two modelling decisions that lived only in a docstring
674aa40 docs: F2 wave 1 run evidence, with what it does not establish stated first
3188dc9 fix: the estabelecimentos change rates, re-measured over the full payloads
```

plus the final whole-branch review's fix wave, which is where the two
outward-facing findings below were closed.

**`CLAUDE.md` and `AGENTS.md` are untouched by the entire branch** —
`git diff 44018ad..HEAD` over those two paths is empty, verified by the
controller at four separate points in the phase. One implementer commit
(`3539bcd`, "unpublish the agent context") did touch them; it was **dropped**, as
out of scope and against a gate reserved to the human. It never reached the
branch under review.

**`.gitignore` was untouched until the final fix wave, which adds exactly one
line**: `/.run-suite-logs/`, the run-suite log root
([§3.1](#31-why-this-needed-a-script)). It was moved out of `${TMPDIR}` because
the script prints its path and this document tells an operator to paste that
output verbatim; on Windows `$TMPDIR` carries the operator's OS username. The
entry is the change that makes the new root ignorable, and is the only edit to
that file on this branch.

---

## 5. Pre-flight: the four handoff claims, re-measured

Controller-verified before Task 0 was dispatched. All four hold.

1. `main` at `44018ad`; PR #7 MERGED 2026-08-04T21:10:19Z; CI `success` on
   `44018ad`.
2. Two months present in all three large tables, ref dates **2026-06-13** and
   **2026-07-11**, ten `_source_file` values per table per month; `lookup` is
   single-month at **7,408** rows (`01f19060-ee5c-12e8-b94d-e31f53622441`).
3. The four `encoding_replacement_char` establishments: present in 2026-06
   bronze, absent from 2026-07 bronze, quarantined in 2026-07 only
   (`01f19061-1041-1e56-b6c3-a9ac80655d7c`). **These four are the entire
   population behind this phase's `rejected_by_our_gate` state on
   estabelecimentos.**
4. `src/opl/bronze/registry.py` at 798 lines against an 800 cap, in both
   worktrees.

**One handoff claim was wrong and is corrected here**: the handoff stated both
worktrees were at `44018ad`. The primary worktree was at `cd1ed22`
(feature-branch tip, pre-merge) at session start.

---

## 6. Pre-flight: six plan defects, found by measuring the plan's premises

The plan was measured against real bronze **before** any modelling task was
dispatched. Six defects were found, four of them blocking. All measurements
controller-run.

### D1 — the prescribed `hub_socio` business key does not identify

Measured over 2026-07's 27,990,592 socios rows
(`01f19061-9328-1159-a4e8-63a8b433237e`):

| `identificador_socio` | rows | distinct `cpf_cnpj_socio` | shape |
|---|---|---|---|
| 1 — PJ (a company) | 717,650 | 310,374 | 14 digits |
| 2 — PF (a person) | 27,260,118 | **999,853** | `***NNNNNN**` |
| 3 — foreign | 12,824 | **0 — all NULL** | — |

The RFB masks a natural person's CPF to six middle digits **at source**. The key
space is 10⁶ and **999,853 of it is occupied — 99.99% saturated**, so a key cannot
identify a person and the next PF partner published lands on one somebody already
holds. Foreign partners have no business key at all.

> **The "~27.3 unrelated people per key" that used to close this paragraph is a
> ROW count, corrected by the final whole-branch review.** 27,260,118 ÷ 999,853 ≈
> 27.3 counts partnership **rows** per key, and one person with several
> partnerships contributes several rows — so it is an **upper bound** on people
> per key, presented as a point estimate. Distinct people per key is
> **unmeasured**, and unmeasurable from this data: telling two of them apart on
> this key is exactly what the mask prevents. Nothing rests on it — 99.99%
> saturation is measured and is the whole argument. Corrected in ADR 0011 and in
> the three code comments that restate it.

The plan's supporting claim — "1,797 rows with no name are hubs with no business
identifier" — points at the wrong population: those rows are quarantined and
never reach bronze, and the name is not the business key.

### D2 — Task 4's acceptance test for the plan's central claim was vacuous

Measured 2026-06 → 2026-07 (`01f19061-707d-1eda-bbf1-a8302ffc3e79`,
`01f19061-e234-1617-bc9a-19f854e7b204`): estabelecimentos has **4** departed keys
and they are exactly the 4 quarantined ones; socios at link grain has **65,444**
departed keys and **0** of them quarantined; empresas has 0 of either.

So a ledger that labels every absence "we rejected it" passes an
estabelecimentos-only test in full. The acceptance probe had to be **cross-table**.

### D3 — the ledger's three states were not a partition

Measured 2026-07 socios (`01f19061-b62b-16c8-8c94-bca654ea0c54`): **680**
quarantined hub keys, **679 also in bronze the same month**; at link grain 1,786
quarantined, 5 also in bronze. "Observed AND rejected" is a real fourth state and
is dominant at hub grain.

### D4 — the chosen end-dating mechanic assumed one open link record

The plan picked AutomateDV's driving-key end-dating, which closes the old window
when a new driven key arrives "so that we do not have 2 open Link records".
Measured 2026-07 (`01f19061-d161-17c6-971d-23106c8d8bcf`): of 16,644,534
companies with partners, **8,266,470 (49.7%) have more than one simultaneous
partner**; max **2,573**, mean **1.681**. Two open link records is the normal
case, not the anomaly. The mechanic that fits is disappearance-driven.

### D5 — the extensibility claim was staked on a file that wave 2 must edit

The scope boundary stakes the DV2 extensibility demonstration on wave 2 being
"+N files, 0 modified", then specified a single `src/opl/vault/registry.py`
holding vault table names and grains — which wave 2 must edit to register
`hub_account` / `hub_customer` / `link_payment`. Structural, not a measurement.

### D6 — one acceptance test could not be a suite test

Task 3's "assert it against real bronze, not a fixture" cannot run in CI:
`.github/workflows/ci.yml` carries no Databricks credential and CI runs local
Spark. Split into a fixture pytest for the `hash_diff` mechanic plus a quoted
real-bronze measurement — which is [§11.2](#112-hash_diff-and-applied_date-on-real-bronze--and-the-caveat-that-must-travel-with-it).

### Checked and found still true, in the same pass

- Task 3's premise is real: **105,820** companies changed `razao_social` between
  the two snapshots (capital 50,300, natureza jurídica 26,724, porte 9,440)
  (`01f19061-4f47-1b47-ab3a-1880491dda04`).
- Task 0's trap is exactly as described: `_module_level_guard_wiring`
  `ast.parse`s only `registry.__file__` and scopes `defined` to `tree.body`
  FunctionDefs, so a guard moved to another module drops out of both `defined`
  and `called` and the lock stays green.
- Quarantined rows carry usable business keys — **0 NULL** in empresas and socios
  quarantine (`01f19061-aade-12eb-aee4-b43e66b22c3a`) — so the ledger's
  quarantine lookup is feasible.

---

## 7. Two corrections the controller made to its own respec

Both were sent mid-build rather than discovered afterwards, and both are
recorded here because they are the phase's clearest evidence that a
controller-authored premise is not automatically more reliable than an
implementer-authored one.

### 7.1 Absence before first observation is not absence after last

The four-state grid marks every future entity as `absent` in every earlier month:
**444,520 establishments and 219,370 partnerships "absent" in 2026-06**, all of
them keys whose first appearance is 2026-07. Not candidate deletes — **not yet
born**. This reshaped the design mid-build into five states.

### 7.2 The controller corrected its own respec

The Task 2 brief said *"no single table carries both absence states"*. **False** —
socios at link grain shows `rejected` and an absence state in the same month. The
true, narrower asymmetry is about **departure causation**: estabelecimentos' four
departures are all our gate's doing, none of socios' 65,444 are.

**That correction then overshot, and Task 7 corrects it again**: the replacement
claim ("socios carries all five states in one month") is also false and is
contradicted by ADR 0010's own table. See
[§23.1](#231-a-correction-that-overshot-in-three-places).

---

## 8. Task 0 — the registry cap, and a lock that could not see two blind spots

Commits `e18e2fe` → `82cbe61` → `ef8c3ce`.

**Controller-verified at `82cbe61`**, independently of the implementer's report:

```
$ uv run pytest -q
691 passed, 6 deselected in 709s

$ uv run ruff check .
All checks passed!
```

Baseline was 681, so +10 tests, matching the implementer's claim. Line counts:
`registry.py` **691** (was 798 — 109 lines of headroom recovered),
`registry_collisions.py` 256, `test_registry_guards.py` 675,
`test_registry_guard_wiring.py` 258.

**The finding worth keeping.** The review approved the task; the controller then
found, and confirmed before dispatching the fix, that the *new* lock had the same
class of blind spot as the old one, reached by filename instead of by moving a
function: `test_registry_guard_wiring.py:43` globbed `registry*.py` for guard
modules while `:35` named `registry.py` as primary, so a guard module named
outside the glob dropped out of `defined` while its call landed in `called` —
`defined - called` stays empty and the lock passes blind. Fixed by asserting
reverse containment, proved with a synthetic-sources probe
(`test_the_lock_is_not_blind_to_a_guard_module_the_glob_does_not_match`).

**Outcome, controller-verified.** `registry.py` 798 → **695** of 800. The guard
the cap had blocked since F1.4b (`_assert_no_table_key_is_month_shaped`) is in,
and the lock now refuses **two** blind-pass routes rather than zero.

**Suite evidence and a run that proves nothing.** Load-bearing figure: **693
passed / 6 deselected**, 756.87 s, on the fix-round tree. A second belt-and-braces
run against the committed `ef8c3ce` returned ruff clean but **its pytest summary
line is not recoverable**: the command piped pytest through `tail -3`, and
Spark's JVM teardown prints three process-termination lines to stderr *after* the
summary, displacing it. The chained `;` also meant the reported exit code was
ruff's. **That run is no evidence either way** — not a failure, just nothing. It
is reported because a run that produced nothing is exactly the sort of thing a
document like this quietly omits. `scripts/run_suite.sh` recovers the summary by
**matching** it rather than tailing, for this reason.

---

## 9. Task 1 — the hash standard, and two refusals found by probing rather than testing

Commits `ab7d879` → `f1117e6`.

**The design property everything else rests on.** The `||` delimiter's real
collision — `["a||b","c"]` and `["a","b||c"]` both join to `a||b||c` — is closed
by **length-prefixing** each component (`S<len>:<norm>`). Escaping and refusing
were considered and rejected in the module with reasons. NULL, empty and
whitespace are three disjoint one-character tags so trimming collapses incidental
padding without collapsing those three into each other, and the NULL token is
unforgeable **by type** (`is None`, before any string compare).

**Controller adversarial probe of the collision property** — the load-bearing one,
since every hub, link and satellite keys on it. 14,424 component lists over an
alphabet of tag lookalikes (`"S4:ABCD"`, `"N"`, `"E"`, `"W"`, `"S1:"`), delimiter
fragments (`"|"`, `"||"`, `"|||"`), colons, digits, whitespace and accented
characters → **0 encoding collisions**, 8,420 distinct encodings. All 8 targeted
pairs differ, including `["S4:ABCD"]` vs `["S","4:ABCD"]`. The Task 1 review
independently recomputed all nine then-pinned digests from hand-built encoded
strings and confirmed they are literal rather than derived, and could not break
the property either.

**Two refusals were found by probing, not by the tests, and that is the finding.**

- **`hash_key([])` returned `e3b0c44298fc1c14…`** — SHA-256 of the empty string —
  so any caller with a mis-specified or empty business-key column list got a
  plausible 64-character key, and they all shared it. Found by a controller probe.
  Now refused.
- **A bare `str` was accepted as a component list.** `str` IS a `Sequence[str]`,
  so no type checker catches it. Controller-verified: `hash_key("AB") ==
  hash_key(["A","B"])`, both `7475fedc505681df…`. A loader writing
  `hash_key(row.cnpj_basico)` instead of `hash_key([row.cnpj_basico])` keys on
  something else entirely, silently. Now refused, and `hash_key(["A","B"])` still
  returns `7475fedc…` — the legitimate call is unaffected.

Two docstring overclaims were verified false by the controller and corrected in
the same round: injectivity "whatever their content" (`hash_key(["ss"]) ==
hash_key(["ß"])`, an intended collapse but an inaccurate sentence), and an
`rjust`-not-`zfill` rationale that is wrong (`"A1".zfill(8) == "A1".rjust(8,"0")`;
they differ only on a leading sign, which no CNPJ carries).

**Controller full suite at `f1117e6`: 713 passed / 6 deselected in 586 s, ruff
clean.** Predicted 713, measured 713.

### 9.1 The `_RUNTIME_SOURCES` rule, which predicted the count three times

`tests/test_revision_stamp.py` builds `_RUNTIME_SOURCES` from
`(_REPO/"src"/"opl").rglob("*.py")`, so **every new file under `src/opl/` adds one
parametrised case** to `test_git_is_consulted_at_build_time_and_nowhere_the_artefact_runs`.
Task 1 added 2 files (+2 tests over the vault count), Task 3 added 8 (+8), Task 4
added 1 (+1). The rule accounted for every gap between predicted and measured
suite totals in this phase, and it is why implementer predictions ran low until
dispatches started stating it.

---

## 10. Task 2 — the observation ledger (ADR 0010)

Commits `f347b5d` → `e1951b0`.

### 10.1 The five-state model reproduces exactly against real bronze

**Controller-verified.** The five-state logic was replicated in SQL against real
bronze, **independently of the implementation**:

| table | month | observed | siblings | rejected_by_our_gate | absent_before | absent_after |
|---|---|---|---|---|---|---|
| estab | 2026-06 | 71,874,448 | 0 | 0 | **444,520** | 0 |
| estab | 2026-07 | 72,318,964 | 0 | **4** | 0 | **0** |
| socios link | 2026-06 | 27,832,321 | 5 | 1,792 | 219,370 | 0 |
| socios link | 2026-07 | 27,986,258 | 5 | 1,781 | 0 | **65,444** |

`01f191f3-6c96-15d2-84db-514bfcff2ce5` (estab),
`01f191f3-ad7e-1edf-b0bd-063c4f1b7db6` (socios).

**Estabelecimentos has zero true departures.** Its only four are our own gate's.
**No month of any table carries five states**, because a key is either before its
first observation or after its last and never both — see
[§23.1](#231-a-correction-that-overshot-in-three-places), where an earlier claim
to the contrary is corrected.

### 10.2 The cost measurement that settled derive-vs-materialise

**Controller-verified: 93 s (estab) / 26 s (socios) for the five-state
derivation, against 93 s / 24 s for the superseded four-state.** So **the absence
split is free**. A materialised record-tracking satellite would cost ~169M rows
per month, forever.

**Decision: derive, do not materialise — on the number.** ADR 0010.

### 10.3 The acceptance probe, and the vacuity it removed

Implementer-reported, and the failure lists were independently traced against the
test code by the reviewer. Four mutation probes, all producing real red output:

| probe | mutation | result |
|---|---|---|
| 1 | every departure → `rejected_by_our_gate` | **7 failed, 18 passed** |
| 2 | quarantine branch deleted | 5 failed, 20 passed |
| 3 | four-state (pre-correction) derivation | 5 failed, 20 passed |
| 4 | `observed_with_rejected_siblings` branch deleted | 3 failed, 22 **passed** |

**Probe 1 is the one that matters**: it went red cross-table but **green on the
estabelecimentos-only test**, which is the vacuity D2 existed to remove —
demonstrated rather than asserted. Probe 2 is its mirror: all three socios tests
passed under it.

### 10.4 The fixture lever, tested and refuted by the implementer against the controller's instruction

The controller instructed the implementer to convert two Delta fixtures to temp
views for a predicted ~90 s saving, based on the implementer's own earlier
estimate. It did, measured setup **137 s → 92 s exactly as predicted**, then found
every test reading them roughly **3× slower** (9.2 → 26.0 s, 8.9 → 27.4 s) because
a view over `createDataFrame` re-materialises from the driver per query where a
managed Delta table is a file scan with a reusable plan. **Net 238 s → 273 s.**
Reverted to 236 s, and reported that the premise of the instruction — its own
estimate — was wrong. **Revert accepted.**

Implementer-measured, inside a single agent doing one thing at a time, which is
why these numbers survive [§3.6](#36-timing-is-contention-not-a-code-property--a-correction-to-this-phases-own-record)'s
contention warning where the suite totals do not.

### 10.5 Two findings the review caught, both reproduced by the controller

1. **A window month with no data on either side labelled EVERY key
   `absent_after_observation`.** Reproduced by running the module: keys A, B and C
   all read as candidate deletes in an unloaded `2026-09`. At real scale that is
   **~72M false candidate deletes from a typo or a failed load**. The module
   refuses this exact class three times elsewhere; this was the one member left
   unguarded, undocumented and untested, with the largest blast radius. Now
   guarded, and — after a fix round that first shipped the guard with **no test** —
   pinned.
2. **Two docstrings credited a branch ordering with a correctness property it
   cannot have.** `first_observed_month` is `min(month)` over bronze ∪ quarantine,
   so for any month a key is quarantined in, `M < first_observed` is always false:
   swapping the two branches changes no answer on any input. The corrected
   docstring says plainly that **no test can hold** the claim rather than quietly
   substituting a different justification, and a new fixture
   (`K_REJECTED_FIRST` — quarantined June, absent July, bronze August) pins the
   real mechanism instead. **Controller-verified that the new test genuinely
   discriminates**: under a bronze-only `first_observed` the July state would read
   `absent_before_first_observation` and the test asserts both the state and
   `first_observed_month == JUN`.

### 10.6 A controller diagnosis that was wrong, and an implementer that said so

The Task 2 fix round left the tree broken — 14 failed, 29 passed — caught by
**running** it rather than by reading a completion notification. The controller
told the fresh implementer that a single `NameError` was "the single cause of all
14 failures". **It was not**: fixing it gave 4 failed / 39 passed. Four further
tests hardcoded a two-key fixture and broke when `K_REJECTED_FIRST` was added. The
implementer found it, fixed it, and flagged that it was outside the brief rather
than folding it in silently.

**Lesson, recorded as doctrine:** a confident single-cause diagnosis from a grep
of one error string is a hypothesis, not a finding — and a handover should say so.

---

## 11. Task 3 — `hub_empresa`, `sat_empresa_dados`, and the test that paid for itself

Commits `e02263f` → `e252553` → `ec654cd` → `9dbb15a`. 11 new files, **+2,466**,
**zero tracked files modified**.

### 11.1 The design tension was named in the dispatch, and its price was demanded up front

`hash_key` is pure Python pinned by literal digests; this task keys 69M rows in
Spark. A Python UDF keeps one source of truth and serialises every row; a
Spark-native expression is fast but is a **second spelling of the standard** —
and drift there re-keys the vault with no digest test able to see it. The
implementer was required to choose and defend, and if Spark-native, to ship a
mandatory equivalence test over adversarial shapes.

**It found a real divergence before shipping.** Spark's `trim` strips only U+0020
while Python's `str.strip()` strips 29 characters, so an NBSP inside a razão
social — entirely plausible in RFB CSVs — would have re-keyed the vault silently.
Fixed with a Python-derived trim class, guarded in both directions. This is the
single strongest argument in the phase for naming the tension and demanding the
test rather than reviewing the result. Now recorded as **ADR 0012**.

### 11.2 `hash_diff` and `applied_date` on real bronze — and the caveat that must travel with it

**Controller-verified** (`01f1926d-3977-17ac-ac74-7a95efc0cc45`; subjects located
via `01f1926d-0b67-1186-9c93-0643761d6ac4`):

> **⚠️ THE SIX DIGESTS BELOW WERE WRONG AND ARE CORRECTED — 2026-08-09, by the run.**
> The values originally published here (`7b5be2ebc84d`, `eae9293b1e21`,
> `11ccd931ac6e`, `4d5d82fca3e3`, `a3d6b665cc11`) came from the controller's **SQL
> replication** of the hash, which never reproduced the `S<len>:<norm>`
> length-prefixed component encoding this vault's standard uses
> (`hashing.py:118-148`). **No vault code has ever produced them.** The table now
> carries what `sat_empresa_dados` actually holds, cross-checked against
> `opl.vault.hashing.hash_key` — the pure-Python spelling, a THIRD implementation
> independent of both the Spark loader and the original SQL — run over the same
> bronze payloads. See `f2-wave-1-workspace-run-evidence.md` §1.4.
>
> **The replication was close enough to preserve equality and inequality**, which is
> why the demonstration below held and why nothing downstream was decided on a false
> premise. It was not close enough to produce the right strings. That is the exact
> shape of a check that confirms an argument while publishing wrong data, and this
> section asserted "nothing in the argument needs the strings" without knowing how
> much it would need that to be true.

| `cnpj_basico` | `applied_date` | razão social | payload hash (12) | satellite rows |
|---|---|---|---|---|
| 00000000 | 2026-06-13 | *(masked)* | `1219a5166aaf` | **1** |
| 00000000 | 2026-07-11 | *(masked)* — same value | `1219a5166aaf` | |
| 00006290 | 2026-06-13 | *(masked)* | `2742a6ded16c` | **2** |
| 00006290 | 2026-07-11 | *(masked)* — a different value | `be94af372f32` | |
| 00012453 | 2026-06-13 | *(masked)* | `64e1d8a254b3` | **2** |
| 00012453 | 2026-07-11 | *(masked)* — a different value | `636b9a248361` | |

Two satellite rows for a company whose razão social changed, one for a company
that did not, with `applied_date` taken from `_snapshot_ref_date` — the RFB's own
declared dates, **not month-end**. That is `hash_diff` and a separated
`applied_date` working on real data, and it is the answer to D6. **The digests
carry the whole demonstration**: equal across both dates on `00000000`, different
on the other two. Nothing in the argument needs the strings.

> **⚠️ THE RAZÃO SOCIAL VALUES ARE MASKED HERE AND WERE NOT ALWAYS.** This
> document quoted all six verbatim until the final whole-branch review, and one
> of them was a **natural person's full name followed by their CPF digits** —
> the RFB's own razão social convention for an MEI, where the "company name" *is*
> the individual. Publishing it in a repository intended to be public puts a
> named private individual into the record, which is the exact class of exposure
> [ADR 0008](adr/0008-pii-masking-socios.md)'s column
> mask exists to prevent one layer down, and the same class as the operator
> username this document redacts above. No other evidence document in `docs/`
> quotes a razão social value; F1.4b names the column and its null counts only.
> The `cnpj_basico` values stay, because they are the public identifiers the
> statement ids are checkable against, and because a CNPJ root is a company
> registration rather than a person.

> **⚠️ CAVEAT, and it must not be dropped.** The hash above is a **SQL stand-in** —
> plain `sha2(concat_ws('||', upper(trim(...)), …))` — and is **not**
> `opl.vault.hashing`'s encoding, which is length-prefixed and carries explicit
> `N`/`E`/`W` tokens. It demonstrates the **premise and the mechanic**, not the
> module's output. The module's own digests are pinned by its tests. **These two
> must not be conflated**, and this warning exists because they are easy to.

### 11.3 The satellite-row histogram, over all companies

**Controller-verified** (`01f19274-e39d-15ff-b5e6-6b104baa93fe`, 32 s): **1 row →
68,922,881 companies; 2 rows → 139,968; no other bucket.** Inside the predicted
[105,820 … 192,284] band and summing exactly to 69,062,849.

### 11.4 The review that swept the code space

**Task review: Needs fixes — 1 Critical, 3 Important, 7 Minor.** The strongest
review of the phase: it compared the two encoders over **all 1,112,064
non-surrogate code points** rather than sampling. See
[§19](#19-the-unicode-pin-is-an-equality-and-that-is-the-whole-safety-property).

**One of its eleven findings was rejected by controller measurement.** The review
claimed the working tree carried uncommitted changes to
`observation.py`/`test_observation.py`, so the 105-passed run was against
something other than `ec654cd`. Measured: tree clean, `git diff HEAD` over both
paths empty, and Task 3's commits never touched either file. **Reviewers are
wrong sometimes — this one was, on one item out of eleven, while being right
about a 40-character Unicode skew nobody else would have found.**

### 11.5 A probe that stayed green, reported rather than hidden

Probe 5 replaced `hash_key_expression`'s zero-pad with a bare `lpad` and **every
test stayed green**, because `hub_candidates` pads a second time. The implementer
reported this as its own honest finding rather than omitting it. The review then
showed the implementer's *explanation* was wrong in the direction that mattered:
`satellite_candidates` has exactly **one** `zero_padded_column` call site and the
overlong-key test only exercised `load_hub`, so the mutation left `load_satellite`
silently merging two companies onto one digest with nothing red. Closed with a
test; probe 5 now goes red at the satellite call site with the hub half still
raising in the same run.

### 11.6 The implementer corrected the controller on the encoding

The controller wrote: "Latin-1 decoding means bronze cannot hold a character above
U+00FF". The reader is **cp1252** (`src/opl/bronze/reader.py:47`, and CLAUDE.md
says so). This matters beyond pedantry: **cp1252 CAN produce characters above
U+00FF** — `Š Œ Ž Ÿ š œ ž` and assorted punctuation — so the ceiling argument was
false. The implementer asserted encodability against the imported
`CSV_DIALECT["encoding"]` instead, which is the correct test. **The same false
"Latin-1" claim survived in one docstring of the same file until Task 7's
correction pass** — [§23.3](#233-the-latin-1-claim-that-outlived-its-own-correction).

---

## 12. Task 4 — `hub_estabelecimento`, two satellites, the hierarchical link (ADR 0013)

Commits `6b3c502` → `d7bc920`. `--collect-only` **859 selected / 865 collected /
6 deselected**, reconciling +10 registry / +22 new file / +1 `links.py` via
`_RUNTIME_SOURCES`.

**Controller measurement settling the implementer's own open question**, over the
71,874,444 establishments present in both months, taken over the **full payloads**
as `domains/cnpj.py` declares them (`01f192de-b784-1e33-a64b-625fad698c1a`):

| payload | columns | changed | rate |
|---|---|---|---|
| `_dados` | 6 | **1,211,834** | **1.69%** |
| `_endereco` | 10 | **570,075** | **0.79%** |

> **⚠️ `_endereco`'s 570,075 IS A RAW COMPARISON AND THE VAULT DOES NOT USE ONE — measured
> 2026-08-09 by the workspace run.** `hash_diff` compares `_normalised` values
> (`strip().upper()`), under which the count is **569,614**: 347 establishments changed
> only in case and 114 more only in leading/trailing whitespace
> (`01f19522-2f3a…`, `01f19524-27f6…`). `sat_estabelecimento_endereco` therefore holds
> **72,888,582** rows and not the 72,889,043 predicted from this figure.
> **`_dados`' 1,211,834 was confirmed exactly by the run** — six coded columns where case
> and padding do not vary, against ten free-text ones where they do. The ratio below and
> ADR 0013's two-satellite argument are unaffected (569,614 moves 0.79% and ≈2.13× by less
> than their own rounding). See `f2-wave-1-workspace-run-evidence.md` §2.3.

**≈ 2.13×.** Per column — **controller-measured**, from the earlier run
(`01f192ac-d8be-1e59-99e5-05717e28efcc`) and covering **four of `_dados`' six**:
`nome_fantasia` 31,912 · `cnae_fiscal_principal` 84,588 · `situacao_cadastral`
976,355 · `motivo_situacao_cadastral` 976,333.

**But the sharpest rate boundary in this data is not the one the split was drawn
on**: inside `_dados`, `nome_fantasia` (31,912) against `situacao_cadastral`
(976,355) is a **30× spread** versus the 2.13× the split rests on. ADR 0013 records
the decision, the rejected finer cut, and the reason the cost asymmetry settles it.

**These are the RE-MEASURED figures.** The first ones quoted here were
1,076,696 / 1.50% for `_dados` over four of its six columns; the correction pass
found the scope mismatch and the controller re-ran it, which **raised** `_dados`
and took the ratio from ~1.9× to ~2.13× —
[§23.2](#232-the-measurement-that-was-quoted-wider-than-it-was-taken--found-re-run-and-closed).

**A probe that over-predicted, reported as such.** Probe B predicted 4 killed
assertions and killed **1**: dropping the shared zero-pad left three green because
`hub_candidates` pads a second time (Task 3's finding seen from the other side)
and because the concatenate-alike pair is separated by the **length prefix**, not
by the pad. Remedied with a new `link_candidates` truncation test with its own
single call site, plus a corrected docstring. **An over-prediction reported
honestly is worth more than a probe that quietly matched.**

**The review found one implementer claim understated rather than overstated**: the
explicit `reversed()` assertion does not cover `dv`-before-`ordem` for a 3-tuple,
but the *positive* equality against `hash_key(list(key))` catches any permutation
via the length prefixes. Protection real, reason wrong.

---

## 13. Task 5 — no `hub_socio`, and a window driven by disappearance (ADR 0011)

Commits `1d51770` → `9a4e4f8` → `1e806b0` → `c3b17a9`. Review: **Needs fixes**, 4
Important — all at the seam between a registry that had learned to express
`LinkEnd` roles, dependent-child keys and non-identifying ends, and loaders that
only partly honoured them. All four fixed.

**The measurement that removed `hub_socio` is D1** ([§6](#d1--the-prescribed-hub_socio-business-key-does-not-identify)).
**The measurement that made removing it cheap rather than lossy**: all **310,374
of 310,374** distinct PJ partner CNPJs resolve to `hub_empresa` on their 8-digit
root, **zero unresolved** (`01f19063-44ef-132a-8aa7-9068b624b370`). A corporate
partner is an empresa already in the hub, so the link is self-referencing for that
half.

**The window's open is source-delivered; only its close is ours.**
`data_entrada_sociedade` is populated on **100%** of 2026-07 rows with no
`00000000` sentinel (`01f19063-53c0-1f06-89f1-6aade0691af8`).

**The link business key is not unique in the source**: 27,990,592 rows over
27,986,263 distinct `(cnpj_basico, identificador_socio, cpf_cnpj_socio)` =
**4,329 collisions**; adding `qualificacao_socio` + `data_entrada_sociedade` still
leaves **3,088** exact duplicates. The link load therefore needs a stated dedup
rule, and has one (earliest delivered entry date, `F.min` over a struct).

**Controller measurement answering the task's Q3**
(`01f192c0-a8da-159e-81b6-0ed2cd6f1758`): the effectivity satellite **should
close** exactly **65,444** windows, **4** carrying a NULL partner key, with
**zero** overlap against July's 1,781 quarantine-only keys — **so no window
should be closed by our own gate**, which is the entire point of gating on the
ledger.

> **"Should", not "does", and the tense is the whole point.** That statement id
> is a **SQL query over bronze**, not a loader run — it computes which link keys
> the ledger would call `absent_after_observation`. **`load_effectivity_satellite`
> has never run against real bronze**, here or anywhere in this phase, so 65,444
> is a prediction the loader is expected to reproduce and not an outcome it
> produced. ADR 0011 has this right at its own `:287` ("the satellite **should**
> close"); this section said "closes", in the present tense and credited to a
> controller measurement, and it is the one place in this document where a loader
> is credited with an outcome. That is exactly the source-measurement /
> code-measurement conflation [§1](#1-how-to-read-this-document) and
> [§25.1](#25-if-a-reader-trusts-this-document-and-is-wrong-to-where-would-that-happen)
> exist to prevent, so it is corrected here rather than footnoted.

### 13.1 A controller error worth keeping, in the exact failure class the task exists to prevent

The controller's first attempt at that verification used
`LEFT ANTI JOIN … USING (k1,k2,k3)` — plain equality — so the **12,824 NULL-keyed
foreign partners never matched** and all read as departed: **74,201, i.e. 8,757
phantom departures.** Null-safe `<=>` gives 65,444.

**This is precisely the failure class Task 5 exists to prevent, and the controller
walked into it while verifying the fix.** The implementer put **both** numbers in
ADR 0011 rather than only the right one, reasoning that the next person
reconciling this table by hand will reach for the same join. Correct instinct, and
the reason it is repeated here.

---

## 14. Task 6 — reference tables as a fifth kind

Commits `9bfa9d8` → `f71355b`. Review **Approved** (3 Important, 5 Minor); fix
round closed all of them.

**The trap, measured** (`01f192c7-7c0b-169f-9a14-fae6761be7e9`,
`01f192c7-9820-18be-ba93-5167bf5e1ede`): `bronze_cnpj_lookup` holds six reference
types in one table distinguished only by `_source_file`, and `codigo` is unique
**within** a type and collides **across** types — `'05'` names both a motivo and a
qualificação, `'1200'` both a município and a natureza jurídica. A loader grouping
on `codigo` alone would silently merge two reference types into one row: right row
count, right column names, one description replaced by the other's, nothing
failing.

Closed by routing on `lookup_type_from_filename`, and **not** by reading bronze's
own `lookup_type` column — which is an independently coded second spelling of the
same rule and was deliberately refused. Exactly one derivation of type-from-filename
exists in `src/`.

**Three findings worth remembering:**

- **`collapsed_duplicates` counted the wrong thing on multi-month windows** —
  projecting the natural key alone rather than `(key, month)` like its cited
  precedent, so the first two-month load would have reported ~7,400 "folded
  duplicates" against a docstring resting on that number being small. **Zero on
  today's single-month data, which is why nothing caught it.**
- **A dict-keyed test helper hid duplicate rows from every content assertion**,
  and the implementer's own probe D had already shown the insert-only test staying
  green with the anti-join disabled. Now pinned by a row count and a reload of a
  **colliding** type.
- **The structural call was made rather than deferred.** `registry.py` was at
  799/800 with a new module split off on arithmetic rather than on a boundary.
  Five kind specs moved **verbatim** into `specs.py`; `registry.py` 799 → **502**,
  `specs.py` 486, every `from opl.vault.registry import X` call site unchanged,
  re-export pinned by an **identity** test. The re-reviewer diffed the 297-line
  move byte-for-byte.

---

## 15. The review record

Every task was reviewed, every review's findings were dispatched as a fix round,
and every fix round was re-reviewed against its own commit range.

| task | review verdict | Critical | Important | Minor | re-review |
|---|---|---|---|---|---|
| 0 | Approved | 0 | 1 | 6 | all 5 dispatched ADDRESSED |
| 1 | Approved | 0 | 1 | 4 | all 5 ADDRESSED |
| 2 | Needs fixes | 0 | 2 | 4 | all 5 ADDRESSED |
| 3 | Needs fixes | **1** | 3 | 7 | all ADDRESSED |
| 4 | Approved | 0 | 1 | 7 | all ADDRESSED |
| 5 | Needs fixes | 0 | 4 | 11 | all ADDRESSED |
| 6 | Approved | 0 | 3 | 5 | all ADDRESSED |

**What the re-reviews caught that the reviews did not**, and both are process
findings rather than defects:

- **Task 4's "no argument dropped" was false.** An implementer condensed ~20
  docstrings to bring a test file from 840 back to exactly 800 and claimed nothing
  was lost. Two real reasons were: the `source` fixture kept the negative reason
  Delta beats a temp view ("~3× slower reads") and dropped the positive one ("a
  managed Delta table is a file scan with a reusable plan"); `_sat_rows` dropped
  "which is what makes a missing or extra row visible" — the stated *purpose* of
  reading per-key row lists, leaving only the mechanism. Nothing incorrect was
  introduced and no assertion weakened, so it is Minor — but **only a reviewer
  told to spot-check condensed docstrings against their originals would have
  known.**
- **Task 3's "I3 caught a real bug in my own tests" was a minor overstatement.**
  One instance is demonstrable from the diff — a test passing a grain over the
  wrong table, **corrected** rather than **relaxed**. The report's second claimed
  instance is new code from the same round rather than a pre-existing assertion.
  Recorded, not waved through.

---

## 16. What the mutation probes prove, and the two that did not

Every task ran mutation probes. The ones worth quoting are the ones that came
back wrong.

| task | probe | outcome |
|---|---|---|
| 0 | move a guard out of `registry.py` | red on the new lock, **green on the old one copied verbatim** — the blindness demonstrated |
| 1 | five mutations of the encoding | each turned at least one test red; no gap found |
| 2 | probe 1, "every departure is our gate's" | red cross-table, **green on the estabelecimentos-only test** — the vacuity demonstrated |
| 2 | probe 2, quarantine branch deleted | red on estab, **green on all three socios tests** — the mirror vacuity |
| 3 | probe 5, bare `lpad` in the hash expression | **stayed green.** Self-declared; the reason given was wrong; closed by a new satellite-side test |
| 4 | probe B, drop the shared zero-pad | **predicted 4, killed 1.** Self-declared; remedied with a new test |
| 5 | probe A, close on either absence state | red on the acceptance test — "the exact defect the whole ledger exists to prevent" |
| 6 | probe A, routing filter removed | 5 of 8 red; 3 correctly orthogonal |

**Two probes came back weaker than predicted and both were reported by the
implementer that ran them.** That is the property worth having: a probe that
quietly matches its prediction tells you less than one that does not and is said
out loud.

---

## 17. The wave-2 extensibility claim, narrowed twice

The plan stakes the DV2 extensibility demonstration on wave 2 adding a domain as
"+N files, 0 modified". The claim as first written was broader than what holds and
was narrowed twice, both times by review:

| wave-2 table | "+1 file, 0 modified"? |
|---|---|
| `hub_account`, `hub_customer` | **yes** — registry and `load_hub` unchanged |
| their satellites | **yes** — registry and `load_satellite` unchanged |
| `link_payment` **without** `transaction_id` | **yes** |
| `link_payment` **with** `transaction_id` as a dependent-child key | **no** — the *spec* registers, the *loader* refuses it |

`links._refuse_a_link_this_loader_cannot_write` refuses **every** link carrying a
dependent-child key, and `link_candidates` does not project one. Implementing that
projection was considered in Task 5 and deferred deliberately: it would be a
generic path with no consumer in the repository and no exercise against real data,
which is the shape this package has refused since Task 3.

The claim is exercised by
`test_a_new_domain_of_hubs_satellites_and_links_is_discovered_without_editing_any_file`,
which builds a throwaway domain carrying wave 2's three tables **by name** and
registers it through the real discovery mechanism. **It does not cover a domain
introducing a new table kind** — that lands in `opl.vault.specs` plus a word in
the union, exactly as `Link` and `EffectivitySatellite` and `ReferenceTable` did,
which is an edit inside wave 1 and is what the plan always expected.

---

## 18. Three paths this phase did not exercise

**The repo's standard is that a guard is only closed by the probe that closes it.**
These three are modelled, tested against synthetic fixtures, and **not exercised
by any real data**. They are reported here as untested, not as working. All three
are controller-measured.

### 18.1 Empresas end-dating — zero departures exist

**All 68,629,147 keys of 2026-06 are present in 2026-07**
(`01f19061-4f47-1b47-ab3a-1880491dda04`). The RFB **retains baixadas** — a company
that closes stays in the file with a changed status — rather than removing them.

So `hub_empresa`'s satellite has **no departure to end-date**, at all, in either
month. The path exists, is tested against a synthetic `C_DEPARTED` fixture, and
has never run against a real departure. Nothing in this phase's evidence says
otherwise.

### 18.2 The satellite dedup tie-break — it never fires

**Zero duplicate `(cnpj_basico, _snapshot_month)` rows in either month** —
68,629,147 and 69,062,849, counts equal to distinct
(`01f19274-c1e0-1f3a-998a-ee0234483f5c`).

The satellite's deterministic tie-break (earliest month, then lowest
`record_source`) is therefore **unmeasured on real data**, not "confirmed absent
by measurement of the mechanism". It is proven only as a mechanism, on synthetic
fixtures. The socios link, by contrast, has **4,329 measured collisions** and does
exercise its own dedup rule — which is why the two must not be reported together.

### 18.3 Reference-table history — there is none to have

`bronze_cnpj_lookup` is **single-month** (2026-06, 7,408 rows;
`01f19060-ee5c-12e8-b94d-e31f53622441`) because the 2026-07 lookup zips were never
published in that month's set. So for reference tables there is:

- **no `hash_diff` comparison** — nothing to compare a `descricao` against;
- **no `applied_date` sequence** — one observation cannot be ordered;
- **no absence for the observation ledger to report**, so the ledger's absence
  states are **unreachable** for these six tables.

The loader is insert-only for exactly this reason, and states the consequence
rather than hiding it: **if the RFB ever revises a code's description in a later
snapshot, this loader will not pick it up.** The anti-join drops the candidate
because its `codigo` is already present, and the row already written keeps its
first-seen `descricao` forever. The mechanism is proven by a **synthetic** second
month, since real bronze has none.

### 18.4 And the one that is not a gap

**Nothing in wave 1 has run against the workspace at all.** Every loader in
`src/opl/vault/` has executed only against local-Spark Delta fixtures. The
predicted row counts, the predicted departure counts and the predicted satellite
widths are **arithmetic from controller measurements of the source**, not
observations of this code. Wave 1's deliverable is the model and its guards; the
run is not in scope for it, and this document does not imply otherwise anywhere.

---

## 19. The Unicode pin is an equality, and that is the whole safety property

`F.upper` bottoms out in Java's `String.toUpperCase`, whose case table is the
**JDK's** Unicode version; `str.upper()` uses CPython's. **JDK 17 ships Unicode
13.0, CPython 3.12 ships Unicode 15.0, and neither is pinned anywhere in this
repository.**

**Measured over every cased character** (`java.version 17.0.19`, CPython
3.12.13): **1,525 cased characters swept, 40 divergent** — U+2C5F, U+A7C1, U+A7D1,
U+A7D7, U+A7D9, and the U+10597–U+105BC span minus U+105A2, U+105B2 and U+105BA.
All forty gained a case mapping in Unicode 14.0. **None is encodable in cp1252**,
so no CNPJ bronze row can hold one today.

Three independent derivations agree: the Task 3 reviewer's sweep of all 1,112,064
non-surrogate code points, an independent **controller** sweep
(`probe_unicode_skew.py`), and the scoped re-reviewer's own recomputation, which
got the same 1,525 and the same 40. A separate controller parity probe over **24
adversarial Unicode cases** — length-changing upper-cases (`ß`→SS, `ﬁ`, `ﬃ`, `ŉ`,
`ǰ`), Turkish dotless-i hazards, NFC vs NFD, 4-byte emoji, the ohm sign, six
exotic whitespace characters — found **0 mismatches**.

**The forty are pinned as a strict EQUALITY, not as an allow-list**, and this is
the detail that carries the safety property. The controller asked only for a
known-exclusion set; the implementer implemented the stronger property and
explained why. An allow-list goes red only when *new* divergences appear. A Java 21
(Unicode 15) runtime would make these forty **agree**, which changes their
digests, which re-keys any vault row containing one — **just as surely as a new
divergence would**. So a JDK upgrade in **either direction** turns the suite red
rather than silently re-keying the vault. The scoped re-reviewer verified the pin
is `diverged == UNICODE_VERSION_DIVERGENCE` and not `<=`.

**A red test here is not a broken test.** It is the decision point. Anyone
updating `UNICODE_VERSION_DIVERGENCE` to make CI green is choosing to re-key the
vault and should say so out loud. ADR 0012 is the record.

**A gap, stated:** `probe_unicode_skew.py` and `probe_hash_parity.py` are session
scratchpad scripts and are **not in the repository**. What is in the repository,
and is what actually guards the property, is the committed sweep in
`tests/vault/test_hashing_spark.py`.

---

## 20. What this PR does not settle

1. **Nothing has run at scale.** See [§18.4](#184-and-the-one-that-is-not-a-gap).
   No loader's cost, no loader's row count, no loader's concurrency behaviour is
   observed. `~90 s` of Spark per satellite load is an implementer estimate from
   fixtures, not a measurement.
2. **The observation ledger's cost is measured but its consumption pattern is
   not.** 93 s per consultation on the largest grain, with no caching in this
   layer. A job that consults it many times pays it many times.
3. **No job or bundle wiring exists.** Nothing in `databricks/` runs any of this.
4. **`record_source` does not name the source table.** Task 3 predicted Task 5
   would break it when PJ partners give `hub_empresa` a second feed. It was not
   fixed; the prediction stands unresolved.
5. **Out-of-order loads leave a redundant satellite row.** Documented, not
   guarded.
6. **Neither loader is concurrency-safe.** Anti-join-then-append is not atomic.
7. **The vacuum hazard sharpened.** ADR 0010 made the quarantine a vault input;
   ADR 0011 makes it one whose **loss causes false closes** rather than a lost
   distinction. A vacuum policy dropping `bronze_cnpj_socios_quarantine` would
   turn 1,781 `rejected_by_our_gate` keys per month into `absent_after_observation`
   and the satellite would end-date them.
8. **The estabelecimentos rates are one month-pair, and only one.** The column
   scope is now closed ([§23.2](#232-the-measurement-that-was-quoted-wider-than-it-was-taken--found-re-run-and-closed)),
   but 1.69% / 0.79% is a single observation. The **zero** change on
   `nome_cidade_exterior` and `pais` is likewise a fact about these two snapshots,
   not a property of the columns.
9. **`situacao_cadastral` and `motivo_situacao_cadastral` "belong together" is
   inferred, not jointly measured.** 976,355 against 976,333 is consistent with it
   and domain-plausible; no cross-tab was run, and two columns can share a marginal
   count while changing on disjoint rows.
10. **Duplicate months in a caller-supplied list produce duplicate `(key, month)`
    ledger rows** — e.g. `months=[JUN, JUN, JUL]`. Pre-existing, contradicts the
    module's own stated grain, and is the same family as three refusals the module
    already implements. A `sorted(set(...))` would close it.
11. **A hub with two satellites derives the observation ledger twice, and that is
    a known accepted cost.** `load_satellite` consults the ledger per load — for
    the departure count and for `_window`'s refusal of an unloaded month — so
    loading `sat_estabelecimento_dados` and `sat_estabelecimento_endereco` pays
    the derivation **twice over the same grain**, and ADR 0010 measures that
    derivation at **93 s** on estabelecimentos. Nothing is wrong with either
    result; the second one is simply recomputed. Left rather than fixed:
    de-duplicating it means either caching across loader calls (state this layer
    deliberately has none of) or hoisting the ledger into a caller that does not
    exist yet. **A wave-2 job loading several satellites per hub should hoist it**,
    and this is the note that says so before someone measures a job and is
    surprised.
12. **The estabelecimentos duplicate-row rate is unmeasured.** The zero
    `(cnpj_basico, _snapshot_month)` duplicate rate that justifies the satellite's
    silent dedup tie-break (`01f19274-c1e0-1f3a-998a-ee0234483f5c`) is an
    **empresas** measurement; Task 4 pointed the same loader at two
    estabelecimentos satellites over 72.3M rows and the equivalent question was
    never asked. `SatelliteLoadResult.collapsed_duplicates` now reports what each
    load folds, so a real load answers it; the source-side query is in the
    final-fix report for the controller. Stated as unmeasured rather than left to
    read as covered by the empresas number.

---

## 21. `CLAUDE.md` names a job number in a repository intended to be public

Raised in this phase, unresolved, and **nothing was touched**. A dropped commit
tried to `git rm --cached` the agent context files; that was the wrong remedy —
the governing spec mandates both files be published as meta-layer evidence — but
the underlying exposure is real. The proposal on the table is to edit the first
section to describe the project without naming the job, keeping the file
published. **Awaiting the human's ruling.**

---

## 22. Deferred minors, triaged

Minors that survived their own task's fix round, and what Task 7 did with each.

| # | minor | disposition |
|---|---|---|
| 1 | `_assert_prefixes_match_their_file_groups` is 51 lines, over the 50-line rule | **Left.** Pre-existing at `cd1ed22`, untouched by this branch; everything Task 0 wrote or moved is ≤49 |
| 2 | `hash_key(b"AB")` raises `AttributeError: 'int' object has no attribute 'strip'` | **Left.** Loud, not silent — no wrong digest — only an obscure message. Ruled outside the `str` finding's scope, which is right |
| 3 | `test_a_quarantine_table_with_no_rows_at_all_still_produces_a_ledger` was a misnomer | **FIXED** — [§23.4](#234-a-test-whose-name-asserted-the-opposite-of-its-own-fixture) |
| 4 | the guard's "accepted" side only directly tested for the quarantine-only month | **Left.** The guard is symmetric by construction (one `loaded` set off the union); the symmetry is what a future edit would break, and that is worth a test the day someone edits it |
| 5 | duplicate months produce duplicate ledger rows | **Left**, and promoted to [§20](#20-what-this-pr-does-not-settle) so it is visible outside a minors list |
| 6 | stale cross-reference to a renamed registry test | **Already fixed** before Task 7; re-verified — both sites name the current test |
| 7 | `tests/vault/test_hashing_spark.py:223` says "Latin-1" | **FIXED** — [§23.3](#233-the-latin-1-claim-that-outlived-its-own-correction) |
| 8 | `_refuse_a_mismatched_grain`'s key-columns check is order-sensitive | **Verified recorded, not fixed** — [§24](#24-checked-and-found-true) item 1. The decision is deliberate and is argued where a reader meets the code |
| 9 | two docstring reasons lost to a cap-forced condensation | **Left** for the file split that will come; recorded in [§15](#15-the-review-record) so it is not lost with the minors list |
| 10 | name-only hub comparison in `links.py` / `satellites.py` | **Left.** House style, unreachable via `linked_hubs`, ruled not-for-fixing |
| 11 | `situacao`/`motivo` "belong together" argued, not jointly measured | **Wording corrected** to mark it as inference; promoted to [§20](#20-what-this-pr-does-not-settle) |
| 12 | Task 5's I1 fix rests on structure, not on a red test | **Left, and stated.** What protects it is that key and guard read the same `identifying_hubs` function — real, but not a red test against a future third caller reintroducing its own `hubs[0]` shortcut. The pattern for such a test exists (`test_registry.py` keeps a throwaway two-identifying-end `Link`), so this was feasible and was not built |

---

## 23. The correction pass — what this phase falsified in its own committed files

F1.4b found **eleven** false statements in already-committed documents by going
looking rather than waiting to trip over one. Task 7's brief said to assume there
is at least one here. **There were twenty**, in eleven files, and the two most
consequential were introduced **by corrections** — a claim retracted correctly and
then replaced with a different false one.

Everything below states what was checked, **including what was checked and found
still true** ([§24](#24-checked-and-found-true)) — a pass that reports only its
hits is indistinguishable from one that stopped early.

> **Whose measurement.** This pass is **Task 7's**: the greps, the reads and the
> line-by-line verification of each claim were all run here. Every item was
> verified against the artifact it describes before being called false — including
> four items where an initial finding was checked and turned out to need
> narrowing, and one where **Task 7's own first correction was itself wrong** and
> was caught before it landed ([§23.10](#2310-a-correction-that-was-wrong-and-was-caught-before-it-landed)).
> A correction pass with one source and no independent re-check is exactly the
> kind of claim a second reader should want attributed.

### 23.1 A correction that overshot, in three places

The controller's respec said *"no single table carries both absence states"*. That
is false and was corrected. **The correction then read: "socios at link grain
carries all five states in one month (27,986,258 / 5 / 1,781 / 65,444 in
2026-07)". That is also false**, and it is contradicted by the very table it sits
beside.

`absent_before_first_observation` and `absent_after_observation` are **mutually
exclusive within a month by construction** — a key is either before its first
observation or after its last, never both. So no month of any table can carry
five. ADR 0010's own five-state table lists exactly **four** states for socios
2026-07 and four for socios 2026-06.

What the measurement actually supports, and what the original argument needed:
socios at link grain carries `rejected_by_our_gate` **and** an absence state in the
**same month**, and **both** absence states across the two months.

**Corrected in three places**, all of which carried the same sentence:

- `docs/adr/0010-observation-ledger-over-a-lossy-extract.md:304-308` — the
  retraction block, corrected in place with the arithmetic shown.
- `tests/vault/test_observation.py:5-10` — the module docstring, which is where a
  reader of the acceptance test meets the argument.
- This document, which would otherwise have inherited it.

### 23.2 The measurement that was quoted wider than it was taken — found, re-run, and closed

**This is the pass's one hit on a controller measurement rather than on an
implementer's prose, and it is the only one that produced a new number.**

`src/opl/vault/domains/cnpj.py:63-64` read:

```
    `_dados`     (6 columns)   1,076,696 changed   1.50%
    `_endereco`  (10 columns)    570,075 changed   0.79%
```

Six and ten are the **payload** widths — verified by counting
`SAT_ESTABELECIMENTO_DADOS.payload_columns` (6) and
`SAT_ESTABELECIMENTO_ENDERECO.payload_columns` (10). The controller's record of the
run scoped the aggregates to **four** columns for `_dados` (the four named
per-column beside them) and **eight** for `_endereco` (the domestic-address eight,
without the `nome_cidade_exterior` / `pais` pair); the task's proposed query text
covers seven columns in total. Task 7 has no workspace access, so it could not
settle which reading was right — it stated the rates as **lower bounds** and named
the query that would settle them.

**The controller re-ran it over the full payloads**
(`01f192de-b784-1e33-a64b-625fad698c1a`, the same 71,874,444 establishments):

| payload | columns | changed | rate | previously quoted |
|---|---|---|---|---|
| `_dados` | 6 | **1,211,834** | **1.69%** | 1,076,696 over 4 columns |
| `_endereco` | 10 | **570,075** | **0.79%** | 570,075 over 8 columns |

**`_dados` was understated, so the correction runs in the direction nobody
predicted.** The omitted `cnae_fiscal_secundaria` and `data_situacao_cadastral`
lift it from 1,076,696 to 1,211,834, taking the ratio between the payloads from
~1.9× to **~2.13×**. **ADR 0013's decision is better supported than it claimed**,
not worse. A measurement whose scope has drifted from the thing it is quoted
against is not automatically an overclaim — this one was an underclaim, and a pass
that only went looking for overclaims would have had no reason to find it.

**`_endereco` needed no correction at all — 570,075 either way — and that is its
own finding.** The two omitted columns are `nome_cidade_exterior` and `pais`, and
they changed on **zero rows across all 71,874,444 establishments**. ADR 0013 places
them in `_endereco` on the argument that they *are* the address for an
establishment outside Brazil; this supports the placement from a second direction —
they belong with the address **and** they cost nothing to carry there — while
being, plainly, a fact measured after the placement was chosen rather than a reason
it was.

**What was unaffected:** the per-column figures (`nome_fantasia` 31,912,
`cnae_fiscal_principal` 84,588, `situacao_cadastral` 976,355,
`motivo_situacao_cadastral` 976,333) come from the earlier run
`01f192ac-d8be-1e59-99e5-05717e28efcc` and cover **four of `_dados`' six**. They
are per-column and the aggregate's scope does not touch them, so the **30×**
intra-`_dados` spread stands exactly as argued; only the number it is contrasted
against moved, 1.9× → 2.13×.

**Updated everywhere the old figures appeared**: `domains/cnpj.py`, ADR 0013 and
this document. Both figures are kept side by side rather than the old one deleted,
because which one a reader is looking at determines whether the ratio they quote is
right.

### 23.3 The Latin-1 claim that outlived its own correction

`tests/vault/test_hashing_spark.py:223` read *"Bronze is parsed from Latin-1 RFB
CSVs"*. The reader is **cp1252** — `src/opl/bronze/reader.py:47`
(`"encoding": CSV_DIALECT["encoding"],  # "cp1252"`), `src/opl/contracts/cnpj_schemas.py:8`,
and CLAUDE.md states it explicitly.

**This is the same false claim the controller was corrected on during Task 3, in
the same file where the correction was applied elsewhere.** The same file says
cp1252 correctly at lines 200, 202 and 207, and `src/opl/vault/hashing_spark.py`
— the module this docstring paraphrases — says it correctly at 63, 84 and 87.

It matters beyond pedantry: **cp1252 can produce characters above U+00FF** (`Š Œ
Ž Ÿ š œ ž` and assorted punctuation), so any argument resting on a U+00FF ceiling
is false. The file's reachability test already does the right thing — it asserts
encodability against the **imported** `CSV_DIALECT["encoding"]` rather than
restating a bound — and the correction says so, so the next reader does not
reintroduce the ceiling argument.

### 23.4 A test whose name asserted the opposite of its own fixture

`tests/vault/test_observation.py:512`,
`test_a_quarantine_table_with_no_rows_at_all_still_produces_a_ledger`. The name
was accurate at `f347b5d`, where `returning_q` was written empty. `e1951b0` added
`K_REJECTED_FIRST`'s June quarantine row to that fixture and updated the docstring
**but not the name**, so the file carried a test asserting in its name the
opposite of what its fixture holds — and the docstring two lines below conceded
it.

Renamed to `test_a_key_with_no_quarantine_row_at_all_still_gets_a_state`, which is
what the assertions have always covered. **No assertion changed**; no other file
referenced the old name (verified by grep across `.py` and `.md`). The rename and
its reason are recorded in the docstring.

### 23.5 The prose a mechanical rename garbled

A `source` → `socios_source` rename in Task 5 rewrote the **English word**
"source" wherever it meant the RFB. `socios_source` is a pytest fixture — a
`SimpleNamespace` of table names — and it does not publish, claim or own anything.
Seven sites in `tests/vault/test_socios_vault.py`, all corrected; the originals
survive verbatim in `src/opl/vault/effectivity.py`, which makes each substitution
provable rather than guessed:

| line | as committed | `effectivity.py`'s original |
|---|---|---|
| 24 | "blamed the socios_source for every disappearance" | "the source" |
| 191 | "the socios_source stopped publishing it" | `:24` "the source stopped publishing it" |
| 193 | "the socios_source never said it ended" | "the source" |
| 418 | "write our inference into the socios_source's column" | the RFB's own `data_entrada_sociedade` |
| 429 | "A closing row has no socios_source row" | `:269` "A closing row has no source row" |
| 443 | "the earliest moment the socios_source claims it began" | `:58` "the earliest moment the source claims the relationship began" |
| 596 | "each projects the socios_source itself" | "the source table itself" |

`tests/vault/` was grepped for the same pattern against `estab_source`,
`empresa_source`, `lookup_source` and the `*_target` fixtures: **no other file has
it.** Behaviour was never affected; in a repository where docstrings are
load-bearing, a documentation-quality regression in the file that carries a
retraction is worth the seven edits.

### 23.6 The claim the file could not support, in the file it was about

`tests/vault/test_socios_vault.py:24` asserted, in the test file's own module
docstring, that *"a ledger that blamed the source for every disappearance passes
this file in full"*. **It does not.** ADR 0011 and the Task 5 report had already
retracted exactly this sentence, so **the retraction and the artifact it describes
disagreed**.

Verified by reading the assertions rather than by trusting the retraction.
`test_a_departure_closes_a_window_and_a_key_our_own_gate_removed_does_not` goes red
in three places under that degenerate ledger:

- `assert states[(R_REJECTED, JUL)] == REJECTED` → would be
  `absent_after_observation`;
- `assert _applied(rows, R_REJECTED) == [JUN_REF]` → `_departures` filters on the
  closing state, so `R_REJECTED` would gain a JUL_REF closing row;
- `assert loaded.eff_result.closed == 1` → would be 2.

Three further tests go red with it, so "in full" is doubly wrong.

**The reason the sentence was written is the interesting part, and the correction
keeps it.** It is true of the **real table** — socios has 65,444 departures and not
one caused by our gate — and false of **this fixture**, which deliberately carries
`R_REJECTED` (June's bronze, July's quarantine) precisely so both satellite
branches can be asserted side by side. The claim conflated the table with the
fixture. The corrected docstring separates the two explicitly, and the same false
claim was corrected a **second time** inside the acceptance test's own docstring
at `:196-199`, where it had been restated.

### 23.7 Six stale "not yet" claims — decisions that outlived the code they described

Each was true when written and falsified by a later task in the same phase,
without the earlier file being revisited. All verified by locating the artifact
that falsifies them.

| # | file:line | claim | falsified by |
|---|---|---|---|
| 1 | `docs/adr/0010:9-11` | "What has not happened yet is a satellite consuming this ledger" — **in the Status section** | the same ADR's own last Consequences bullet: `sat_eff_company_partner` consumes it as of Task 5 |
| 2 | `src/opl/vault/links.py:8-9` | "effectivity windows belong to a satellite on the link, which this vault does not have yet" | `src/opl/vault/effectivity.py`; `SAT_EFF_COMPANY_PARTNER` at `domains/cnpj.py:273`; `registry._assert_every_effectivity_satellite_hangs_off_a_link` exists to **admit** it. Only the DESCRIPTIVE satellite is still refused, which the correction now says |
| 3 | `tests/vault/test_hashing.py:17-18` | "there is no satellite yet" | four satellites exist and their wiring is pinned in `test_cnpj_vault.py` and `test_estabelecimento_vault.py` |
| 4 | `src/opl/vault/loading.py:171-172` | "Unreachable today — **there is one link**" | two links are registered. The *conclusion* survives on a different ground, now stated: the two flatten to component lists of length 4 and 3, and the encoding is injective over component lists |
| 5 | `src/opl/vault/columns.py:12-13` | "`opl.vault.registry` — whose `__post_init__` guards are the only thing that reads `METADATA_COLUMNS`" | `registry.py` does not import `opl.vault.columns` at all. Task 6's fix round moved the guards to `opl.vault.specs` |
| 6 | `src/opl/vault/months.py:2-3` | "shared by the observation ledger and by **both loaders**" | six loaders; and `opl.vault.loading` is not a loader but the layer they share — which the module's *next* paragraph already said correctly |
| 7 | `src/opl/vault/loading.py:13-14` | "`hash_key_expression` is called by `load_hub`, `load_satellite` and `load_link`" | `load_partner_link` calls it too (`partners.py:199`), and `load_effectivity_satellite` reaches the encoding through `link_hash_key_expression`. The property the sentence exists to assert — one spelling, in this module — is unchanged; only the enumeration was stale. Correcting **this** item is where Task 7 introduced its own error, [§23.10](#2310-a-correction-that-was-wrong-and-was-caught-before-it-landed) |

### 23.8 Four stale counts

| # | file:line | claim | measured |
|---|---|---|---|
| 1 | `src/opl/vault/columns.py:3-6` | "FOUR NAMES, IN ONE PLACE, AND NOTHING ELSE IN THIS MODULE… the two loaders" | **seven** name constants plus two frozensets; **six** loaders |
| 2 | `src/opl/vault/hashing_spark.py:7,24` and `tests/vault/test_hashing_spark.py:5` | "nine hard-coded digests" | **ten** distinct literals across eleven assertions in `test_hashing.py` (lines 46, 53, 87, 90, 104, 129, 130, 143, 144, 154, 174; 154 and 174 share one). Nine was right until the accented digest arrived in Task 3's fix round |
| 3 | `src/opl/vault/hashing_spark.py:77-78` | "exactly FORTY characters — U+2C5F, U+A7C1, U+A7D1, U+A7D7, U+A7D9 and U+10597-U+105BC" | the enumeration beside "exactly FORTY" names **forty-three**. `UNICODE_VERSION_DIVERGENCE` excludes **U+105A2, U+105B2 and U+105BA** from that span. The pinned set really is 40; the prose reading of it was not |
| 4 | `tests/vault/test_observation.py:121-122` | "ten of them put ~137 s of setup in front of 25 tests" | **eight** `_write` calls in the fixture; **28** test functions in the module (25 when the timing was taken). The timings are Task 2 measurements and are now labelled as such rather than restated as current |

Item 3 is the one worth dwelling on: the file's own frozenset is correct and the
committed test is what runs, so **nothing was ever wrong in the guard** — the
prose describing it was. That is the shape most of this pass found.

### 23.9 A fraction with nothing behind it, and an inference dressed as a measurement

Two of a different kind, both in the "consequences" register where a number is
least likely to be re-derived by a reader.

**`docs/adr/0010:326` read: "here it is weaker still, because a third of the
absence signal is our own doing."** No reading of that ADR's own table yields a
third. Task 7 replaced it with two fractions — **0.49%** of non-`observed` rows
and **"4 of 65,448 — 0.006%"** of departures.

> **⚠️ AND THOSE TWO REPLACEMENTS WERE ALSO WRONG, found by the final
> whole-branch review and withdrawn without a fourth figure.** Both count only
> estabelecimentos' 4 gate-caused departures while excluding socios' 1,781
> `rejected_by_our_gate` keys from **both** sides — the keys ADR 0011 says
> closing would "have the vault assert that 1,781 partnerships ended" and
> [§20.7](#20-what-this-pr-does-not-settle) says a lost quarantine would
> end-date. On the docs' own framing the departure figure is `1,785 / 67,229` ≈
> **2.7%**, ~440× the 0.006%; and 91% of the 0.49%'s 732,921 denominator is
> pre-birth grid artifacts that no end-dating satellite acts on, so restricted to
> real absence (69,021) the gate share is **5.2%**. **"A third" was too high and
> both corrections understate.** No replacement fraction is offered because none
> is computable: every one of them needs to know how many of the 1,781 have an
> open window, which is **unmeasured and has no statement id**.
>
> **This is [§23.1](#231-a-correction-that-overshot-in-three-places)'s pattern
> happening inside the bullet that documents it** — twice in the same sentence,
> in the document whose closing section already states that a retraction gets
> less scrutiny than the original.

The argument does not need a fraction and is stronger without one: what makes a
derived delete weak is that **any** of the silence can be ours, not how much —
and on estabelecimentos the four rejects are **100%** of that table's departures.
That is what ADR 0010 now says, with all three withdrawn versions recorded above
it so the next reader inherits the history rather than a fourth number.

**`src/opl/vault/domains/cnpj.py:71-75` read that `situacao_cadastral` and
`motivo_situacao_cadastral` "move together almost exactly (976,355 against
976,333) … so those two genuinely belong in one payload."** Those are two
**marginal** counts. Two columns can share a marginal count while changing on
entirely disjoint rows; no cross-tab was run and none exists. The conclusion is
domain-plausible and probably right — the motivo is what explains the situação —
but it is an **inference**, and the file now says so. Task 4's review had ruled
this "not overclaimed" on the grounds that the file distinguishes MEASURED from
argued; on re-reading, this particular sentence did not.

### 23.10 A correction that was wrong, and was caught before it landed

Task 7's first edit to `src/opl/vault/loading.py` corrected the stale
three-loader enumeration to say that `load_effectivity_satellite` **and
`load_reference_table`** reach the hash through `link_hash_key_expression` and
`hash_key_over`.

**`load_reference_table` does neither.** `src/opl/vault/reference.py` imports
`BRONZE_RECORD_SOURCE`, `SNAPSHOT_MONTH_COLUMN` and `read_snapshot_window` from
`opl.vault.loading` and no hashing function at all, because a **reference table
has no hash key** — `opl.vault.specs.ReferenceTable` states that as its own
decision ("Adding one here would be a second, unused spelling of `codigo`").

Caught by grepping for the callers instead of trusting the enumeration that
prompted the edit, and corrected before the commit. Recorded because a correction
pass that introduces a false statement while removing one is worse than no pass,
and because it is the same failure mode as everything in
[§23.7](#237-six-stale-not-yet-claims--decisions-that-outlived-the-code-they-described):
an enumeration written from memory rather than from a grep.

---

## 24. Checked and found true

The negative results, because they are the evidence that the pass did not stop
early.

1. **The order-sensitive grain check is recorded where a reader meets the code, not
   only in a task report.** Task 3's `_refuse_a_mismatched_grain` compares
   `tuple(grain.key_columns)` against `hub.business_key_columns` **as a list**, and
   Task 4 deliberately kept the order-sensitivity. `src/opl/vault/satellites.py:140-154`
   argues it in the function's own docstring — conceding first that the tempting
   justification is wrong (`groupBy` is order-insensitive, so a permuted grain
   answers identically), then giving the real one (the hub's order IS load-bearing
   because the hash concatenates in it, and anything later pairing the two lists
   positionally would pair the wrong columns). **The refusal message itself spells
   out the one-line fix.** `domains/cnpj.py` points at that function by name where
   it declares a grain. Nothing about this decision lives only in a task report.
2. **Every backtick-quoted cross-reference in `src/opl/vault/` and `tests/vault/`
   resolves.** Each was checked against its definition, including the one the
   ledger flagged as stale after Task 3 — `registry.py:24-25` and
   `test_registry.py:9-10` both now name
   `test_a_new_domain_of_hubs_satellites_and_links_is_discovered_without_editing_any_file`,
   which is the actual definition. That one was fixed before Task 7. Fourteen
   further cross-references were spot-checked to their definitions and all resolve.
3. **`registry.py`'s wave-2 wording is correct, and deliberately narrow.** It says
   wave 2 does not need **"this file"**, which is true of the registry — and ADR
   0011 blesses exactly that wording while recording that the **loader** refuses a
   link with a dependent-child key. The narrow claim and the ADR agree
   ([§17](#17-the-wave-2-extensibility-claim-narrowed-twice)).
4. **cp1252 is stated correctly everywhere except the one site corrected.**
   `hashing_spark.py:63, 84, 87` and `test_hashing_spark.py:200, 202, 207`. **No
   U+00FF ceiling is asserted anywhere** — the reachability test imports the
   dialect rather than restating a bound, which is the right shape.
5. **`test_observation.py:404-406`'s discrimination claim is true.** *"Run against a
   ledger that labels every departure `rejected_by_our_gate`, the socios half goes
   red. Against one that labels every departure `absent_after_observation`, the
   estabelecimentos half goes red."* Verified against the assertions on both sides.
   This file's version of the claim is honest where
   `test_socios_vault.py`'s was not, because its socios fixture deliberately has no
   quarantine-only key — and it says so.
6. **`test_estabelecimento_vault.py:26-27` and `:154-156` are true.** *"This table
   alone cannot tell a correct ledger from one that labels every departure
   `rejected`; it passes on both."* The estab fixture's only departures are the four
   already-rejected ones, and `E_NEW_IN_JULY`'s June absence is pre-birth rather
   than a disappearance. **The estabelecimentos half of the phase's asymmetry claim
   is true; only the socios half was wrong** ([§23.6](#236-the-claim-the-file-could-not-support-in-the-file-it-was-about)).
7. **The domain arithmetic is internally consistent.** Spot-verified across
   `domains/cnpj.py`, `partners.py`, `effectivity.py`, ADR 0011 and the socios test
   fixtures: `27,990,592 − 27,986,263 = 4,329`; `5 + 1,781 = 1,786`;
   `717,650 + 27,260,118 + 12,824 = 27,990,592`; `27,260,118 / 999,853 ≈ 27`
   **partnership rows per key — an upper bound on people per key, not an estimate
   of it** ([§6](#d1--the-prescribed-hub_socio-business-key-does-not-identify));
   `8,266,470 / 16,644,534 = 49.7%`; `27,990,592 / 16,644,534 = 1.681`;
   `74,201 − 65,444 = 8,757`; `71,874,448 − 4 = 71,874,444`; the socios five-state
   table sums to 28,053,488 distinct keys in **both** months. The re-measured
   estabelecimentos rates check out the same way: `1,211,834 / 71,874,444 = 1.686%`
   → 1.69%, `570,075 / 71,874,444 = 0.793%` → 0.79%, and
   `1,211,834 / 570,075 = 2.126` → ~2.13×.
8. **`TRIMMED_CHARACTERS` really is 29 characters and
   `UNICODE_VERSION_DIVERGENCE` really is 40 elements** — counted from the
   frozensets, not from the prose describing them.
9. **The protected paths really are untouched.** `git diff 44018ad..HEAD` over
   `CLAUDE.md` and `AGENTS.md` is empty, re-verified in Task 7 and in the final
   fix wave as well as at four earlier points. `.gitignore` was in this list
   until that fix wave, which adds one line for the run-suite log root and says
   so in [§4](#4-what-this-branch-builds); the reason it had to move is
   [§3.1](#31-why-this-needed-a-script).
10. **No hardcoded catalog or schema exists in `src/opl/vault/`.** Every table name
    goes through `DEFAULT.table(...)`.

---

## 25. If a reader trusts this document and is wrong to, where would that happen?

The question this task is required to answer, and the honest list is not short.

1. **In reading a source measurement as a code measurement.** This is the biggest
   one and it is structural. Every large number here — 69,062,849, 65,444, 4,329,
   1,211,834 — measures **RFB bronze**, taken to justify or refute a modelling
   decision. **None of them is an output of the code in this branch.** A reader who
   remembers "the vault loaded 69M companies" has the wrong model; the vault has
   loaded nothing.
2. **In the suite total — and this one is now CLOSED.** It read: "922 is a
   `--collect-only` count and a sum of four chunk runs from a single controller
   session at `f71355b` … **nobody has yet run `scripts/run_suite.sh` end to
   end**, so the script's own value is argued rather than demonstrated." It has
   since been run end to end twice, and the second run is quoted in full at
   [§3.3](#33-the-suite-from-one-command-end-to-end--the-final-fix-wave-verbatim):
   **932 passed, 932 selected, agreed by two derivations, exit 0**, one command
   and one timestamp. The residual risk is smaller and different: it is a single
   run on a single machine, and [§3.6](#36-timing-is-contention-not-a-code-property--a-correction-to-this-phases-own-record)'s
   contention warning applies to its four timings even though the pass counts are
   contention-free.
3. **In §11.2's digests.** They are a SQL stand-in and are labelled as such in two
   places, which is two more than the number of places a reader is likely to check.
4. **In an aggregate whose column scope drifted from its query.** The
   estabelecimentos rates did exactly that, and it was found by Task 7 rather than
   by the measurement or by either review — then re-run and closed
   ([§23.2](#232-the-measurement-that-was-quoted-wider-than-it-was-taken--found-re-run-and-closed)). **If
   one quoted measurement's scope drifted, others may have.** The re-run makes the
   risk concrete rather than hypothetical: this document now carries **two**
   statement ids for the same table because the first answered a narrower question
   than the sentence around it claimed. Nothing else here rests on an aggregate
   whose column set is stated separately from its query — but that is an assertion
   about this document, not a proof.
5. **In the mutation probes.** Each is a claim that a *specific* wrong
   implementation goes red. Two of them ([§16](#16-what-the-mutation-probes-prove-and-the-two-that-did-not))
   came back weaker than predicted. A probe set is a lower bound on the tests'
   discriminating power, never an upper one.
6. **In "the review approved it".** Task 0's review approved a task whose new lock
   still had a blind spot the controller found afterwards. Task 3's review was the
   strongest in the phase and was wrong on one item of eleven. **An approval is one
   reader's failure to find a defect.**
7. **In the correction pass itself.** It has **one source**. Every item was
   verified against its artifact, and one of Task 7's own corrections was wrong and
   was caught ([§23.10](#2310-a-correction-that-was-wrong-and-was-caught-before-it-landed))
   — which is evidence the method has teeth and equally evidence that a pass of
   this kind produces errors of its own.
8. **In the timings.** Every wall-clock figure is a property of a *(machine,
   contention, moment)* triple. The same branch measured 586 s, 850 s and 414 s
   ([§3.6](#36-timing-is-contention-not-a-code-property--a-correction-to-this-phases-own-record)).
   Do not plan against any of them.
9. **In treating "not exercised" as "probably fine".** [§18](#18-three-paths-this-phase-did-not-exercise)
   lists three modelled paths no real data reaches. Two of the phase's real defects
   — `collapsed_duplicates` counting the wrong thing, and the empty-`hash_key`
   digest — were in code that today's data never exercises, and were found by
   probing rather than by running. **The unexercised paths are where the next
   defect is.**

**The general form, which this phase earned the right to state:** the two
documents that were most wrong in it were the two that had already been corrected
once. A retraction is not a guarantee that what replaced it is true, and the
replacement gets less scrutiny than the original precisely because it arrives
wearing the authority of a correction.

---

## 26. Predictions published *before* the run that tests them (workspace-run Task 2.5)

This section is dated **2026-08-09**, after the sections above, and it exists because
the workspace-run plan asserted something about this document that was not true. Its
acceptance table said "every one is published in `docs/f2-wave-1-run-evidence.md` as a
**prediction**". Three of its six numbers — **69,202,817**, **72,318,968** and
**28,051,707** — appeared **zero times** anywhere in this file, and three more modelled
tables had no prediction at all. A phase whose whole method is "reconcile a run against a
number published beforehand" cannot have half its table unpublished, and a number first
written down *after* the run that produced it is not a prediction. So these are recorded
here, with statement ids, while **no loader has yet run**.

**Every number below is still a prediction.** The tense is deliberate, exactly as
[§13](#13-task-5--the-effectivity-satellite) uses it: these say *should*, not *does*.

| table | predicted rows | derivation | statement |
|---|---|---|---|
| `hub_empresa` | **69,062,849** | distinct `cnpj_basico`, both months | `01f1943d-11d7-16bf-ac0d-316e1828fe92` |
| `sat_empresa_dados` | **69,202,817** | 69,062,849 first observations + 139,968 changed | `01f1943d-6606-1f0a-b21d-f1f998e9bd3c` |
| `hub_estabelecimento` | **72,318,968** | distinct (`cnpj_basico`,`cnpj_ordem`,`cnpj_dv`), both months | `01f1943f-30ff-18a3-805c-4bd37c7bed46` |
| `sat_estabelecimento_dados` | **73,530,802** | 72,318,968 + 1,211,834 changed (`01f192de-b784…`) | derived from the two |
| `sat_estabelecimento_endereco` | ~~**72,889,043**~~ **FALSIFIED — actual 72,888,582** | predicted as 72,318,968 + 570,075; the real change count is **569,614**, because 570,075 is a RAW comparison and the vault normalises. See `f2-wave-1-workspace-run-evidence.md` §2.3 | `01f19524-27f6-15cc-9f73-4c3f72dbdafa` |
| `link_empresa_estabelecimento` | **72,318,968** | hierarchical, one parent per child, so link grain = estabelecimento grain | `01f1943f-30ff-18a3-805c-4bd37c7bed46` |
| `link_company_partner` | **28,051,707** | distinct (`cnpj_basico`,`identificador_socio`,`cpf_cnpj_socio`), both months, NULL-safe | `01f1943f-5e50-1a47-ab68-abd92f120ec8` |

### 26.1 The estabelecimentos duplicate rate, measured for the first time — it is **zero**

`satellites.py:57-66` recorded this rate as **unmeasured, not zero**, and
[§18.2](#182-the-satellite-dedup-tie-break--it-never-fires) said the same. Measured now
(`01f1943f-30ff-18a3-805c-4bd37c7bed46`): **0** duplicate
(`cnpj_basico`,`cnpj_ordem`,`cnpj_dv`,`_snapshot_month`) tuples, over both months.

So `collapsed_duplicates` **will not fire** on estabelecimentos either, and the
satellite's deterministic tie-break stays **unexercised on real data** — proven as a
mechanism on synthetic fixtures only, exactly as it already was for empresas. The
workspace-run plan expected this measurement to possibly make the tie-break "load-bearing
rather than latent"; it resolves the other way, and the honest report is that the path is
still unexercised rather than newly confirmed. The socios link remains the only place in
this vault with measured collisions (4,329) that actually exercises a dedup rule.

### 26.2 Two ways to compute these numbers wrongly, both of which produce a plausible answer

Both were hit while deriving the table above, and both are recorded because each
produces a number close enough to the right one to be published without suspicion.

**A raw payload comparison overcounts satellite rows.** Comparing the payload `struct`
directly gives `sat_empresa_dados` = **69,202,818**, one more than the truth, off a
`changed_rows` of **139,969** rather than 139,968. The vault's `hash_diff` compares
`_normalised` values — `strip().upper()`, `hashing_spark.py:160-167` — so two payloads
differing only in case or in the 29-character trim class are the **same** to the loader.
Measured (`01f1943d-6606-1f0a-b21d-f1f998e9bd3c`): `changed_raw` **139,969**,
`changed_upper` **139,968**, `case_only_changes` **1**. Exactly one company in 69 million
changed its razão social's case and nothing else. **A prediction for a satellite must be
computed the way its loader computes it, or it is a prediction of a different table.**

**`COUNT(DISTINCT a,b,c)` silently drops every row where any component is NULL.** At link
grain it returns **28,042,946** against the null-safe `GROUP BY`'s **28,051,707** — short
by **8,761**, because 25,653 socios rows carry `cpf_cnpj_socio` NULL. This is the same
failure family as the `LEFT ANTI JOIN … USING` that manufactured **8,757** phantom
departures ([§13](#13-task-5--the-effectivity-satellite)), in a different operator: SQL's
NULL is not a value to `DISTINCT` any more than it is to `=`. `GROUP BY` treats it as one,
which is why the ledger is built on `groupBy` and unions rather than joins
(`observation.py:404-411`). **The two wrong answers here differ by 4 — 8,761 against
8,757 — which is precisely how little the shape of a NULL bug tells you about its size.**

### 26.3 What this section does not claim

- **No loader has run.** Every number above is still SQL over bronze, and the whole point
  of the phase this section belongs to is to replace them with loader output.
- The two `sat_estabelecimento_*` totals are **derived**, not directly measured: they add
  a newly measured key count to a change count published earlier (`01f192de-b784…`). A
  direct measurement was attempted and abandoned — 16 `regexp_replace` calls over 144M
  rows with a window function exceeded the query timeout, and the honest record is that
  the arithmetic is the source rather than a single statement.
- The citations this document's own §12 and §14 carry for `01f19061-707d…` and
  `01f192c7-7c0b…` were **checked and are correct here**: they measure departed keys and
  the `codigo` collision respectively, which is what those sections use them for. It was
  the workspace-run plan that pointed them at a union count and a row total.
