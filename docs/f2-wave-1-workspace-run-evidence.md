# F2 wave 1 — running the vault, and what the run said

Companion to `docs/f2-wave-1-run-evidence.md`, which published the **predictions**.
This document records what happened when the loaders ran. Where the two disagree, the
disagreement is written down rather than resolved by editing the prediction.

**Controller-verified** means the controller ran the command and read the output.
**Reported** means a task's own stdout said it. Both appear below and are labelled.

---

## 1. Task 3 — `hub_empresa` + `sat_empresa_dados`, the first vault load that ever ran

Until this run, `grep -rl vault databricks/` returned nothing and no vault loader had
executed anywhere. Seventeen modules, four ADRs and a published set of predictions had
never met data through their own code path.

**The run.** Job `opl-vault-cnpj-empresa`, run `750119983560449`, launched with
`months=2026-06+2026-07,revision=4c9d1b886b455c848e2a1ec624519cd07bc19ed7`.
Result **SUCCESS**.

| task | result | duration |
|---|---|---|
| `assert_deployed_revision` | SUCCESS | **34 s** |
| `hub_empresa` | SUCCESS | **281 s** |
| `sat_empresa_dados` | SUCCESS | **5,635 s** (1 h 34 m) |

### 1.1 The counts reconcile exactly

**Controller-verified** (`01f194c9-c89b-1175-bd65-7ddc9a5351e5`,
`01f194d9-2cee-15ea-914f-298daf3feb46`):

| table | predicted | **actual** | |
|---|---|---|---|
| `hub_empresa` | 69,062,849 | **69,062,849** | ✅ |
| `sat_empresa_dados` | 69,202,817 | **69,202,817** | ✅ |

Three counts were asked of the hub, not one: **rows, distinct `hub_empresa_hk`, and
distinct `cnpj_basico` are all 69,062,849.** A correct total with repeated digests would
pass a single count and mean the hash had collided two companies onto one key; asking all
three is what makes this evidence that the hash is **injective over this load** rather
than evidence that the row count is right.

The satellite's own grain is likewise clean: **distinct (`hub_empresa_hk`,
`applied_date`) = 69,202,817 = the row count**, so no key/date pair repeats.
`69,202,817 − 69,062,849 = 139,968` — exactly the predicted number of companies carrying
a second row, arrived at from the loader's output rather than assumed from the prediction.

`applied_date` spans **2026-06-13 → 2026-07-11**, the RFB's own declared reference dates
from the inner filenames, **not month-end**, confirming that `applied_date` is
source-derived and not run-derived.

### 1.2 The provenance guard ran for the first time, and it read something

ADR 0009's guard had been **proven locally only**. It has now executed in the workspace
and **accepted**, and its own output names the revision it read:

```
assert_deployed_revision: OK -- the installed wheel was built from
4c9d1b886b455c848e2a1ec624519cd07bc19ed7, which is the revision this run was
launched for.
```

Naming the SHA is what distinguishes a guard that compared from a guard that passed
vacuously. **Only the ACCEPT half is now proven.** The refusal path — a wheel from
another commit, or a `+dirty` stamp — is still unexercised in the workspace, exactly as
it was before this run, and must not be reported as proven because its sibling was.

**The guard also caught its author.** The Task 2.5 documentation commit moved `HEAD` to
`4c9d1b8` after the deploy that had stamped `3299a78`, which would have made every run
refuse. The rule "deploy after every commit, no commits between a deploy and its runs"
was violated by the person who had written it into the plan that morning. The fix was a
redeploy, re-verified **by artefact**: the deployed wheel's sha256 changed from
`3a69a1e3…` to `012863cb…`, which is how the re-upload was confirmed to have happened —
the deploy's own output mentions only "Uploading bundle files" and would have supported
either conclusion.

### 1.3 `{{job.start_time.iso_datetime}}` resolves — a question `bundle validate` could not answer

Task 1's review flagged this reference as unverifiable short of a real run: a dynamic
value reference that `bundle validate` cannot substitute, feeding `load_date`.

**Controller-verified:** `COUNT(DISTINCT load_date) = 1` across `sat_empresa_dados`. One
timestamp for the whole run, shared by the hub and satellite tasks, which is what the
YAML's comment claims it buys and what a loader stamping its own clock would not produce.
An unresolved reference would instead have reached `required_load_date` as a literal and
refused. **Resolved, and now measured rather than assumed.**

### 1.4 The three companies: the structure reproduces, the published digests do not

Task 3's acceptance test is `docs/f2-wave-1-run-evidence.md` §11.2, which published six
payload digests. **Controller-verified** (`01f194d9-6c46-1935-af3a-52b932e7a88b`):

| `cnpj_basico` | `applied_date` | §11.2 predicted | **loader wrote** | rows |
|---|---|---|---|---|
| 00000000 | 2026-06-13 | `7b5be2ebc84d` | **`1219a5166aaf`** | **1** ✅ |
| 00006290 | 2026-06-13 | `eae9293b1e21` | **`2742a6ded16c`** | **2** ✅ |
| 00006290 | 2026-07-11 | `11ccd931ac6e` | **`be94af372f32`** | |
| 00012453 | 2026-06-13 | `4d5d82fca3e3` | **`64e1d8a254b3`** | **2** ✅ |
| 00012453 | 2026-07-11 | `a3d6b665cc11` | **`636b9a248361`** | |

**The structure reproduces exactly** — one row for the unchanged control, two for each
changed company, on the two RFB dates, equal-then-different in the pattern §11.2 rests
its argument on. **Every digest string differs.**

**The loader is right and §11.2 is wrong**, and this was settled by a third
implementation rather than by preferring one of the two. `opl.vault.hashing.hash_key` —
the **pure-Python** spelling, not the Spark one the loader used — was run locally over
`00000000`'s June payload read straight from bronze:

```
vault hash_key(payload) [12] = 1219a5166aaf
loader wrote                 = 1219a5166aaf
evidence 11.2 published      = 7b5be2ebc84d
```

So the Spark loader agrees with the vault's own hash standard, independently computed.
§11.2's digests came from the controller's **SQL replication** of the hash, which did not
reproduce the `S<len>:<norm>` length-prefixed component encoding
(`hashing.py:118-148`) — a replication close enough to preserve equality and inequality,
which is why the demonstration held, and different enough to produce six wrong strings.

§11.2 itself says "**the digests carry the whole demonstration**: equal across both dates
on `00000000`, different on the other two. **Nothing in the argument needs the
strings.**" That sentence is now doing more work than its author intended: the argument
survives intact and the strings are retracted. **The razão social values stay masked** —
one is an MEI, a private individual's name plus CPF digits.

### 1.5 Two modelled paths are still unexercised, and this run did not change that

**Reported** by `vault_load_satellite`'s own stdout:

```
+69202817 rows ...; the target already held 0 rows;
0 source rows were folded into a row sharing their (hash key, applied_date);
0 candidate departures (absent_after_observation, never asserted)
```

- **End-dating: not exercised.** 0 candidate departures, because the RFB retains baixadas
  and all of 2026-06's keys are in 2026-07. The path exists and is tested against a
  synthetic fixture; **no real departure has ever reached it.**
- **The satellite dedup tie-break: not exercised.** 0 collapsed duplicates, consistent
  with the 0 duplicate `(key, month)` pairs measured in
  `f2-wave-1-run-evidence.md` §26.1 before this run.

Both were predicted to come out this way and both did. **Reporting them as confirmed
would be the error**: a path that ran zero rows through it is not a path that works.

### 1.6 Cost, measured for the first time

| | rows | duration | files | size |
|---|---|---|---|---|
| `hub_empresa` | 69,062,849 | 281 s | 49 | **2.459 GB** |
| `sat_empresa_dados` | 69,202,817 | 5,635 s | 512 | **5.716 GB** |

**8.175 GB and 1 h 39 m for the smallest of the four planned loads.**

**The satellite costs 20× the hub, and most of it is not the write.** Where `load_hub`
makes one pass (distinct + anti-join), `load_satellite` makes four:
`satellite_candidates`, then `_collapsed_duplicates` (a second full scan of the source),
then `_candidate_departures` (a full `observation_ledger` derivation, including the
`crossJoin` that builds the all-keys × all-months grid), then the append. **Two of those
four exist only to populate reported fields** — `collapsed_duplicates` and
`candidate_departures` — and both returned **0**.

`f2-wave-1-run-evidence.md` measured the five-state ledger derivation at **93 s** for
estabelecimentos. That was **SQL on the `13cf10c85b0f189d` warehouse**, and it does not
transfer to the PySpark implementation on Free Edition serverless. The premise was
measured; the output was not, and this is the gap the phase exists to expose.

**Consequence for Task 4, stated before running it rather than after:** estabelecimentos
is 72.3M keys with **two** satellites, one carrying a 10-column payload, plus a link.
On this structure that is several hours, most of it spent on two counts that came back
zero. Whether those two diagnostics are worth a full scan and a full ledger derivation
per load is now a decision with a number behind it.

#### 1.6.1 The decision that number bought, taken before Task 4 ran

`load_satellite` now takes **`report_diagnostics`, defaulting OFF**, and the two job
YAMLs carry it as a job parameter defaulting to `"false"`. Off, neither count is
computed and both are reported as **`None`** — the types are `int | None` and
`SatelliteLoadResult` refuses a pair carrying one of each, because **`None` is "not
measured" and `0` is "measured, found none"** and the two zeros above are published as
evidence. A flag that turned a real 0 into a silent 0 would make that evidence
unfalsifiable. The task prints two different sentences for the two states, and the
skipped one carries no number at all.

**What is NOT skipped is deriving the ledger.** That derivation is the only route by
which `months` reaches `observation._window`'s refusal of a month with no row on either
side — a guard `satellites.py` names as one of the two things consulting the ledger
really buys, and without which `months=['2026-09']` writes nothing and reports success.
So the derivation runs on every load and only the `count()` over it is optional; past
that refusal `observation_ledger` is a plan, so the `crossJoin` grid is never
materialised on a load that reports nothing. Launch a measuring run with
`--params months=…,revision=…,report_diagnostics=true` — which is the way to answer the
still-open question of the **estabelecimentos duplicate rate**, unmeasured rather than
measured at zero.

#### 1.6.2 The change is real and the framing around it was wrong — measured by review

The paragraph above, the commit that introduced it, and **the controller's own statement
of the option before choosing it** ("~4× faster") all implied the two optional passes
were most of the satellite's cost. **They are not.** An independent reviewer built two
synthetic fixtures and timed `load_satellite` both ways on local Spark:

| fixture | flag off | flag on | ratio |
|---|---|---|---|
| 300,000 keys/month, contention-free | **72.43 s** | 79.63 s | **1.10×** |
| 900,000 keys/month, some contention | **192.39 s** | 233.65 s | **1.21×** |

Decomposed at 900,000 keys: constructing `observation_ledger` — the part that is **not**
skippable, including `_window`'s eager job — costs **1.76 s**; the `.count()` over it that
`_candidate_departures` now gates costs **20.35 s**; `_collapsed_duplicates`' independent
second scan costs **28.56 s**.

**So the narrow laziness claim is TRUE and the headline was false.** `_window`'s eager
work is a single-column `distinct()` that scales with distinct months rather than with row
count, and the `crossJoin` grid genuinely is never materialised without an action — which
is exactly why deriving the ledger unconditionally keeps the month guard for ~1.76 s
instead of ~22 s. But the two diagnostics are **9–18% of `load_satellite`'s wall clock,
not "the expensive part"**. The other 82–91% is in code this change never touched:
`satellite_candidates` hashing every source row, and `_append_changed`'s lag window plus
the Delta write. On a first load `existing` is `None`, so the entire candidate set must be
hashed and windowed regardless of any flag.

**The consequence for Task 4, corrected before it runs rather than after.** Expect roughly
a **10–20%** reduction from this flag, not an order of magnitude, and do not expect
estabelecimentos to approach `load_hub`'s per-row cost. **The satellite cost problem is
still substantially open**, and the place to look is the hashing/window/write path, not
the ledger.

Stated against the measurement: it is `local[2]` Spark on synthetic data, not Free Edition
serverless on real bronze, and the 900,000-key run had CPU contention. The *direction*
reproduced at two scales with one run contention-free; the exact percentage is not a
promise about the cluster.

**Carried to Task 5, confirmed by the same review:** `effectivity._observed` is a lazy,
**uncached** DataFrame consumed twice — once by the `collapsed` action and once through
`_statements` — so its full DAG runs twice over ~28M socios rows. The repair is
`persist()`, not a flag, because the count is already a column of the frame the write
needs. Pre-existing, out of scope for this commit, and now measured rather than suspected.
