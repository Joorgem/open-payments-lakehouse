# F7 — Polish: the phase whose subject is a claim that went false and nobody was obliged to notice

**Written 2026-08-31, at the close of F7.** ~~F7 is this project's **last phase**, so this is also
its last phase record.~~ **That was true when it was committed and false the next day** — and it
is this document's own subject arriving in its own opening sentence. The owner decided, after this
was written, to keep expanding the repository, and work outside F7's plan has already merged: PR
[#34](https://github.com/Joorgem/open-payments-lakehouse/pull/34), *"ci: split the test job into
four shards on separate runners, and lock the partition"*, merged **2026-09-01T20:23:01Z**.

```bash
gh pr view 34 --repo Joorgem/open-payments-lakehouse --json number,title,state,mergedAt
```

**Nothing beyond that is planned or named here**, and this correction claims no more than that:
not that a next phase exists, only that F7 is not the last one.

**Third instance of this phase's species, and the one with the most interesting cause.**
[`docs/postmortem.md`](postmortem.md) §1.1's pull-request count was overtaken by a merge **49
seconds after it was committed**; §2.4 below said *"Today is 2026-08-31"* until midnight passed
while the phase waited on CI. **Both were measurements the world then moved under** — a merge
landed, a clock ran. **This one was never a measurement.** It said what would happen next, and
somebody decided otherwise after it was committed. **A claim about what comes next is not made
safe by being true when it is written, and no test in this repository can hold one true.**

**Every other sentence in this document that leaned on F7 being last is struck in place against
this paragraph**, and the two that used it as an *argument* — §2.4's reason for not waiting on the
broker expiry, and §3's reason for giving two of F7's own open paths ledger ids — keep their
conclusions and lose only their reason. [`docs/postmortem.md`](postmortem.md) §6 and
[`docs/unexercised-ledger.md`](unexercised-ledger.md) §0.5 carry the same correction, each pointing
back here.

**How to read the labels, and there are exactly two.**

- **Controller-verified** — the controller ran the command in one of this phase's measurement
  passes and read the output.
- ***Reported*** — an implementer, a reviewer, or a task's stdout said it.

**Not every claim below carries one, and the sentence that stood here said they all did.** §0 sets
**Controller-verified** as its own default in the line under its heading, and says so; the rest of
the document labels a paragraph where the label is the point and leaves the others bare. **Read a bare
paragraph as *Reported*.** This document was written by T7's implementer, so everything it derived
while writing carries the weaker label of the two — which is the honest reading of the convention
`docs/f6-run-evidence.md` sets, and it is why those claims are published with the command that
re-derives them. **Prefer running the command to trusting the number.** That is not a flourish:
§1.3 below is a number this phase published twice, corrected once, and got wrong both times,
found by running the one-line command that settles it.

**Predictions are published BEFORE the runs that test them** (master protocol §4.5). A number
first written down after the run that produced it is not a prediction. §2 marks all five, and the
falsified ones stay struck in place rather than being reworded into something that could not be
wrong.

> **THE STATEMENT IDS BELOW EXPIRE.** Measured in F5 at ~5 days; an expired id returns the API's
> own named refusal. They are provenance for work of the last day. **Job `run_id`s are durable**
> and are what a later reader should follow.

> **The phase plan is not part of this repository.** It lives in a git-ignored working directory.
> F3 shipped a section pointing a public reader at that directory and they reached nothing, so no
> path to it is given here — everything a reader needs from it is restated against something a
> public reader can check: a command, a commit, an issue number, a run id, or a statement id.

---

## 0. Task 0 — measured before this phase's plan existed

Taken 2026-08-29/30 on `main` at `ef123ac`, before a line of the plan was written.
**Controller-verified** unless a paragraph says otherwise.

### 0.1 The defect this phase exists to fix had already been filed, by this project, against this project

**Issue [#25](https://github.com/Joorgem/open-payments-lakehouse/issues/25) was opened
2026-08-18T21:45:30Z** — *"README on main states Data Vault, Gold/Kimball and UC governance are
'roadmap, not built' — all three are on the same branch"* — and
**[#26](https://github.com/Joorgem/open-payments-lakehouse/issues/26)** twenty-two seconds later.
**Both were still open when this document was written**, and pull requests have gone on merging
into `main` on top of them — **including this phase's own.** How many is a fact with a short shelf
life, so it is published as a command rather than as an integer;
[`docs/postmortem.md`](postmortem.md) §1.1 records what happened to the integer that stood here:

```bash
gh issue list --repo Joorgem/open-payments-lakehouse --state all --json number,state,createdAt
gh pr list --repo Joorgem/open-payments-lakehouse --state merged --limit 60 \
  --json number,mergedAt --jq '.[]|select(.mergedAt>"2026-08-18T21:45:30Z")'
```

**This is a process defect, not a documentation defect, and it is the whole subject of
[`docs/postmortem.md`](postmortem.md)** — which is where the cross-phase reading lives and is not
restated here. F7's addition to the species F5 named and F6 measured is one sentence: **the
migration of a defect out of the code and into the document that judges it can be *detected* and
still not stop.**

### 0.2 Two falsehoods in the public record, standing on `main` at the phase's start

Both **Controller-verified**, and both re-checkable today:

```bash
# pinned to the commit `main` carried when the phase opened, so these stay reproducible
git ls-tree -r ef123ac --name-only -- docs/specs/   # -> docs/specs/README.md, and nothing else
git grep -n 'blocked-egress mitigation is validated' ef123ac -- docs/
git grep -n 'blocked-egress mitigation is validated'          # today's tree, for the contrast
```

- **`docs/specs/README.md` advertised a document set that does not exist** — *"one per phase … the
  trail from intent to executed result"* — against **exactly one path in that directory: the
  README itself.** Zero specs, in every phase that shipped. **This is the F3 defect the project
  had already named in its own words:** a section pointing a public reader at a directory where
  they reach nothing.
- **ADR 0002 asserted *"The blocked-egress mitigation is validated."*** The F0 row that sentence
  rests on is **struck in `docs/f0-validation-report.md`** — its first row, marked FALSIFIED, with
  the reason that *"the probe answered a different question than the premise asked"*: the upload
  travels the control plane and touches serverless egress not at all. **So the claim was struck in
  one file and standing in another, on this project's most load-bearing architectural premise** —
  and ADR 0002's own 2026-08-17 amendment had explicitly ring-fenced that section as still
  standing, which is how a correction pass leaves a defect behind inside its own boundary.

**Both are closed on this phase's branches, and neither had merged when this was written.**
§1.1 names the pull request that carries each, and the second command above is how a reader
confirms which tree they are reading.

### 0.3 Nine ledgers had never been read as one list

***Reported*,** from the phase's own audit and now superseded by a maintained artefact: nine
run-evidence documents each publish a *what is still unexercised* section — ten sections, because
one document carries two — and **nobody had ever read them together.** What that cost is measured
rather than asserted, and it is published in
[`docs/unexercised-ledger.md`](unexercised-ledger.md) §4.1.

Task 0's own reading of that list is what **prediction 1** was built on, and §2.1 records that the
reading was wrong in more than one independent way.

### 0.4 UC lineage is readable, and it answers a different question from the declared manifest

**Controller-verified**, 2026-08-29: `system.access.table_lineage` is populated and queryable on
this workspace, and it **does** carry `bronze_payments → fact_payment` — the exact edge ADR 0020
Decision 5 said a declared vault-path walk would miss.

**It does not reverse that decision, and the reason is this phase's own species.** Lineage records
**executions**, stamped with an `event_time`; the declared manifest states **structure**. A blast
radius built on lineage answers *"nothing downstream"* for a table whose gold loader has not run
inside the retention window — **the same reassuring wrong answer, arriving through the source that
would look most authoritative.** §1.4 turns that argument into a measurement.

### 0.5 A dated obligation comes due inside this phase, and one site already claimed it was met

ADR 0019 Decision 6 wrote this project a documentary obligation: the Redpanda Serverless trial's
credits expire **~2026-09-03**, after which
`databricks/resources/streaming_managed_broker_job.yml` fails at the metadata fetch — and *"expired
trial"*, *"revoked ACL"*, *"wrong username"* and *"no route"* all produce **one** string, so only a
date distinguishes a dead trial account from a regression in this repository.

```bash
git grep -n '2026-09-03' 24fcefe   # five lines, three files, on `main` as this phase found it
git grep -n '2026-09-03'           # today's tree
```

**`docs/f5-run-evidence.md` states the obligation in its own words**, in the ledger entry that
carries the date: ***"This document must say the broker is gone once it is."*** **The job's header
claimed that had already happened.** §1.7 and §2.4 record what was done about it, and what
was not.

---

## 1. What was built and run

### 1.1 The seven tasks, and where each landed

***Reported*** for the mapping; **Controller-verified** for the commits, which resolve.

| task | what it produced | where it is |
|---|---|---|
| **T1** | the **generated** ADR index with a dated reversal-conditions table — `scripts/adr_index.py`, `scripts/generate_adr_index.py`, `docs/adr/README.md`, `tests/test_adr_index.py` | `8a91c72`, **merged** in PR [#31](https://github.com/Joorgem/open-payments-lakehouse/pull/31) at `24fcefe` |
| **T2** | the README rebuilt from measurement, `tests/test_readme_counts.py`, and `docs/specs/README.md` deleted (§0.2) | PR [#32](https://github.com/Joorgem/open-payments-lakehouse/pull/32) |
| **T3** | ADR amendments — 0002's two false claims (§0.2), 0009's job list, 0008's fourth unstruck site, and the citations into the git-ignored working directory that a public reader could not follow | `d1786b1`, `834afab`, `671675b`, `9203aa4`, PR [#33](https://github.com/Joorgem/open-payments-lakehouse/pull/33) |
| **T4** | [`docs/unexercised-ledger.md`](unexercised-ledger.md) and `tests/test_unexercised_ledger.py`, **and the phase's only workspace run** (§1.2) | `d5670c9`, and run `529699767706804` |
| **T5** | the UC lineage cross-check | **this document's §1.4** — a measurement this phase took, not a document that needed its own reason to exist |
| **T6** | [`docs/postmortem.md`](postmortem.md) | `ad78b19` |
| **T7** | this document, and the broker job header (§1.7) | `086f149`, and the file you are reading |

**Two of the seven were still open pull requests when this was written, and one of them is what
closes issue #25.** That is a fact with a short shelf life, so it is published as a command rather
than as a state:

```bash
gh pr list --repo Joorgem/open-payments-lakehouse --state all --limit 10 \
  --json number,state,headRefName,title
gh issue list --repo Joorgem/open-payments-lakehouse --state open
```

**A reader who finds #25 and #26 still open after this phase is looking at the exact failure the
phase is about**, and [`docs/postmortem.md`](postmortem.md) §6 names it as one of the four things
that would falsify it.

### 1.2 The one workspace run — and a FAILED run was the success condition

**Controller-verified, 2026-08-31.** One quarantined `bronze_merchant` row. This is the phase's
only workspace run, and **it was declared before the result arrived that a FAILED run is what
success looks like here** — ***Reported***, and it has to be: that declaration lives in the
git-ignored phase plan and no reader of this repository can check it. What a reader **can** check
is the wiring below, which routes a batch holding a rejected row to `fail_on_dq` and excludes
`promote` — the job's terminal task exists to fail when the gate rejects anything.

**Before** — statement `01f1a57a-d61a-1cf9-830a-b436c7f084df`:

| table | rows |
|---|---|
| `workspace.default.bronze_merchant` | 2,192 |
| `workspace.default.bronze_merchant_quarantine` | **0** |
| `workspace.default.bronze_merchant_staging` | 2,192 |

**The probe was one INSERT into the Postgres registry**, not a corrupted existing row, so the
seeded population's published digest stays byte-identical and the probe stays attributable by
`legal_name`. It carries a **13-digit** CNPJ: a dropped digit is the likeliest registry typo, and
it is the only merchant rule the real registry can hold at all (§1.3). Registry 1,088 → 1,089 rows;
the extractor's single `REPEATABLE READ READ ONLY` transaction landed 1,089 rows / 477,596 bytes as
the third file in that month's landing directory, so Auto Loader saw it as unseen.

**Run `529699767706804`**, job `opl-bronze-merchant` (bundle key `bronze_merchant`), month
**2026-08**, deployed from a clean tree so the wheel carries no `+dirty` stamp:

| task | state |
|---|---|
| `assert_deployed_revision` | SUCCESS — the ADR 0009 guard passed against the freshly deployed wheel |
| `ingest` | SUCCESS |
| `dq_gate_batch` | SUCCESS |
| `check_bad_rows` | SUCCESS — it evaluated, and the condition came back false |
| `promote` | **SKIPPED / EXCLUDED** — fail-closed held |
| `fail_on_dq` | **FAILED** — the intended terminal state |

The wiring those six states exercise is in the tracked bundle and needs no workspace access to
read: `promote` depends on `check_bad_rows` with `outcome: "true"`, and `fail_on_dq` with
`outcome: "false"`.

```bash
grep -n 'task_key\|outcome' databricks/resources/bronze_merchant_job.yml
```

**After** — statement `01f1a57b-e745-14e0-9d1d-dfef2158916e`: **bronze 2,192, unchanged ·
quarantine 1 · staging 3,281** (= 2,192 + the 1,089-row batch).

**The witness row** — statement `01f1a57b-ece6-1d4f-9f1b-43a4d7dac93d`. It carries
`_dq_reject_reason` **`bad_cnpj_shape`** and that rule alone, so **first-match-wins held**;
`_record_source` **`opl_merchant_postgres`**, which names the database rather than any upstream
publisher; `_pg_snapshot` **`1835:1835:`** and `_pg_wal_lsn` **`0/5C0E580`**, the extractor's own
transaction coordinates, which survived the whole path into the quarantine table; and
`_rescued_data` **NULL**, so no schema drift was involved.

> **ADR 0006's fail-closed policy is now demonstrated on real data at batch scale: one bad row in
> 1,089 stopped the batch, and the system of record did not move.** Until this run, the false arm
> of `check_bad_rows` and the `fail_on_dq` task had never executed, and
> `bronze_merchant_quarantine` had never held a row. Both are recorded as closed, with this run id
> as their evidence, at [`docs/unexercised-ledger.md`](unexercised-ledger.md) §4.2 — ids `fdb:1521`
> and `fdb:1522`.

**And the sentence this run was originally justified by was false.** The phase plan said
`rejected_by_our_gate` *"has never had a witness in any source"*. **ADR 0010's own measured table
had said otherwise since F-DB** — 4 rows for estabelecimentos 2026-07, and 1,792 and 1,781 at the
socios link grain. The true sentence is the narrower one: **never witnessed for `merchant`**, and
that is what this run ended.

> **THE GENERALISATION HAD ALREADY BEEN PROPAGATED INTO A GENERATED, TEST-LOCKED PAGE.** T1's
> hand-declared reading in `scripts/adr_index.py` cited the plan line — a line labelled *Reported*
> — as the `why` for a reversal condition in `docs/adr/README.md`, and that merged to `main` in PR
> #31. **A *Reported* claim was elevated to a dated reading in the phase's first deliverable, and
> ADR 0010 had the measurement the whole time.** Corrected on this branch: the reading's state
> stays `NOT MET`, because nobody has measured the quantity it asks for, and only the false `why`
> is replaced. **This is the phase's headline defect reproduced inside the phase's own first
> deliverable**, and it is specimen (c) of [`docs/postmortem.md`](postmortem.md) §3.1.

### 1.3 The finding that did not need the run — and a count this phase published twice and got wrong twice

Derived from the registry DDL against the merchant rule set **before anything was run**, and
checkable with two commands and no workspace:

```bash
sed -n '117,121p' scripts/seed_merchant_db.py
uv run python -c "from opl.bronze.rules import rules_for; \
  rs = rules_for('merchant'); print(len(rs)); print([r[0] for r in rs])"
```

| column | Postgres type | what it makes unrepresentable |
|---|---|---|
| `cnpj` | `text NOT NULL`, **no CHECK** | nothing — `bad_cnpj_shape` **is** reachable, which is why the probe used it |
| `onboarded_on` | `date NOT NULL` | **`bad_onboarded_on_shape` cannot occur in any row the registry can hold** |
| `credit_limit` | `numeric(14,2) NOT NULL` | **`unparseable_credit_limit` cannot occur in any row the registry can hold** |
| `_snapshot_at` | stamped by the extractor inside the reading transaction, never read from the registry | `bad_snapshot_at_shape` is not a source property at all |

**So three of the seven named merchant rules do not defend against registry content.** They defend
against corruption in transit, a schema change, or a different source arriving on the same
contract. **They are standing limits with a reason, not debt** — no run can close them and none
should be asked to. They are published as such at
[`docs/unexercised-ledger.md`](unexercised-ledger.md) §1.3, in a bucket that did not exist before
this phase.

> **AND THE SIZE OF THE RULE SET WAS PUBLISHED TWICE INSIDE THIS PHASE AND WAS WRONG BOTH TIMES.**
> A working note said **eight** named rules; its own correction said the set was **nineteen**,
> *"twelve required plus seven named"*. **The command above returns twenty: thirteen
> `null_or_empty_*` required rules plus the seven named ones.** The correction fixed the half it
> was looking at and introduced an error in the half it was not — which is this phase's most
> repeated finding, landing on the arithmetic of the finding itself. **The corrected figure never
> reached a tracked file**, because [`docs/unexercised-ledger.md`](unexercised-ledger.md) §1.3
> publishes the *derivation* — `plus seven named rules`, with the `sed` range that shows them —
> and never the total. **A number that was never typed cannot go stale.**

A fourth rule of the same family, `unhashable_case_divergence`, is unreachable by **choice** rather
than by type — the phase refused to seed one of the forty divergent characters purely to "prove" a
rule — so it stays in the carry-forward as `fdb:1471` rather than joining the standing limits.

### 1.4 UC lineage, published BESIDE the declared manifest — T5's deliverable

**Controller-verified, 2026-08-31**, from `system.access.table_lineage` on this workspace, with
statement ids beside the figures. **The declared manifest keeps its authority and lineage is
published beside it** — the two answer different questions (§0.4), and **the disagreements are the
artefact.**

#### The population, re-measured — and the phase's first reading compared two different columns

Statements `01f1a582-200b-126d-9087-fb594c2a388c` and `01f1a582-3434-1e4a-87eb-6093022e4af1`:

| measure | value |
|---|---|
| rows | **3,340** |
| `COUNT(DISTINCT target_table_name)` | **67** |
| `COUNT(DISTINCT target_table_full_name)` | **72** |
| rows with a **NULL target** | **1,606 — 48% of the table** |
| rows with a NULL source | 76 |
| `COUNT(DISTINCT source_table_name)` | 83 |
| window | 2026-07-24T01:23:20Z → **2026-08-31T20:38:30Z** |

A reader with a Databricks workspace re-derives the whole row set with one statement. The numbers
above are **this** workspace on **this** date and are not a property of the product:

```sql
SELECT COUNT(*)                                                   AS rows,
       COUNT(DISTINCT target_table_name)                          AS distinct_target_name,
       COUNT(DISTINCT target_table_full_name)                     AS distinct_target_full_name,
       SUM(CASE WHEN target_table_name IS NULL THEN 1 ELSE 0 END) AS null_target,
       MIN(event_time) AS window_start, MAX(event_time) AS window_end
FROM system.access.table_lineage
```

> **A DROP FROM 72 TO 67 WAS NEARLY PUBLISHED AS RETENTION EXPIRING EVENTS, AND IT IS NOT ONE.**
> §0.4's reading recorded **72**; the re-measurement recorded **67**. They are **two different
> columns** of the same table, both true, and the fully-qualified count is still 72. The finding
> came from re-deriving both rather than validating the earlier number, and it is recorded because
> it is the shape of an error this project has paid for repeatedly: a second reading that agrees
> with the first *about the wrong thing*.

> **AND THE AMBIGUOUS PHRASING HAD ALREADY BEEN PUBLISHED INTO THE GENERATED INDEX, TWO DAYS
> EARLIER.** The reversal-condition reading for **ADR 0020 Decision 5** — dated 2026-08-29,
> declared in `scripts/adr_index.py` and rendered into the test-locked
> [`docs/adr/README.md`](adr/README.md) — **read** *"3,327 rows, **72 distinct target tables**"*
> when PR #31 merged it to `main`. Past tense, and the command is pinned to that commit, because
> **the page is generated and this branch regenerated it**: today's file no longer carries the
> phrase.
>
> ```bash
> git grep -n '0020 D5' 24fcefe -- docs/adr/README.md   # the wording, where it was true
> grep -n '0020 D5' docs/adr/README.md                  # and what stands in its place today
> ```
>
> **Its verdict is right and survives this measurement.** ADR 0020 Decision 5's reversal condition
> is *"Unity Catalog lineage becoming readable **and complete**"*, and the page marks it
> **`LOOKS MET, IS NOT`** — readable, not complete — which is exactly what §1.4 goes on to measure
> in detail. **What does not survive is the phrase.** *"Distinct target tables"* names neither
> column unambiguously, and it is the same phrase that nearly produced the false finding in the box
> above. This is the **second** time this phase's first deliverable carried a Task 0 wording that a
> later measurement refined — the first is in §1.2 — and it is recorded here **and** regenerated:
> `scripts/adr_index.py` was corrected on this branch, so the page names the column, and this box
> is the only place the replaced wording survives. **Quietly regenerated is what it must not be**,
> and a generated page cannot keep its own history, which is what this box is for.

**The 48% NULL-target fraction is §0.4's argument in concrete form**: lineage is not a structural
statement about the warehouse, and half its rows do not name a target at all.

**The window's last event is this phase's own workspace run.**
`bronze_merchant_staging → bronze_merchant_quarantine` at **2026-08-31T20:37:05Z**, against a table
maximum of 20:38:30Z. The one run F7 made is visible in the instrument F7 is publishing.

#### The declared manifest, derived rather than transcribed

Every row below comes out of the shipped module, and the command that prints it needs no
workspace:

```bash
uv run python -c "
from opl.bronze.registry import REGISTRY
from opl.triage_agent.blast_radius import blast_radius
for k in sorted(REGISTRY):
    r = blast_radius(k); print(k, len(r.vault), list(r.gold))"
```

| bronze | declared vault tables | declared gold |
|---|---|---|
| `empresas` | 2 | `dim_company`, `dim_date`, `fact_payment` |
| `estabelecimentos` | 5 | `dim_company`, `fact_payment`, `pit_estabelecimento` |
| `lookup` | 6 | **none** |
| `merchant` | 4 | **none** |
| `payments` | 0 | `dim_date`, `fact_payment` |
| `ptax` | 0 | `fact_payment` |
| `socios` | 2 | **none** |

#### The disagreements, and they run in both directions

**a. Two declared vault edges are absent from lineage.** `empresas → hub_empresa` and
`merchant → hub_merchant` are declared and do not appear; `hub_empresa` appears in lineage **only**
from `bronze_cnpj_estabelecimentos`. A blast radius built on lineage would answer *"`hub_empresa`
is not downstream of `empresas`"*, which is **false**.

**b. Three gold tables have no inbound provenance at all.** `dim_date`, `dim_channel` and
`dim_currency` carry **only self-edges**. Their build reads nothing lineage records, so **both**
declared paths into `dim_date` are invisible.

**c. Lineage records self-edges; the manifest has no such concept** — the three conformed
dimensions above, and two bronze/staging tables besides.

**d. AND LINEAGE CARRIES A TRUE EDGE THE MANIFEST CANNOT EXPRESS.**
`bronze_cnpj_socios_quarantine → sat_eff_company_partner`, and the same shape for
`bronze_merchant_quarantine → sat_eff_merchant_empresa`. **The effectivity satellites read the
quarantine table**, which is exactly ADR 0010's five-state design over bronze ∪ quarantine — and
the manifest's `vault` tuple does not distinguish a quarantine read from a bronze read. **So the
cross-check earns its keep in both directions: lineage is missing edges the manifest declares, and
carries a real one the manifest cannot say.**

**e. Lineage also carries F4's DataOps views** as targets of nearly every bronze, staging and
quarantine table. The manifest models none of that **deliberately**: ADR 0018 Decision 1 rules that
DataOps *derives*, so a view is not a blast-radius consequence.

**The decision stands, and now on a measurement rather than an argument.** 1,606 of 3,340 rows name
no target, two declared vault edges are absent, and three gold tables have no inbound provenance —
so a blast radius built on lineage would answer *"nothing downstream"* for real edges, which is the
reassuring wrong answer the declaration exists to refuse.

**ADR 0020 Decision 5's own reversal condition is what this settles.** It asks for lineage that is
*readable **and complete***; the generated index has carried it as `LOOKS MET, IS NOT` since
2026-08-29 on the readability half alone, and the measurements above are the completeness half,
named edge by edge. **The condition remains unmet, and the reading now rests on evidence rather
than on an argument. What would meet it, unchanged: lineage acquiring a structural, non-event view
that survives retention.**

### 1.5 The consolidated ledger, and the test that refuses a tenth hand-maintained list

[`docs/unexercised-ledger.md`](unexercised-ledger.md) replaces the **authority** of the ten sections
it consolidates, not their existence — they stay where they are as point-in-time records, because
this project's most valuable property is that it keeps a wrong belief beside its correction rather
than deleting it.

**Do not retype its numbers into this document.** They are re-derived, and both commands are the
ledger's own:

```bash
grep -cE '^\| `[a-z0-9]+:[0-9]+` \|' docs/unexercised-ledger.md
uv run pytest tests/test_unexercised_ledger.py -q
```

**Five buckets, and the classification is the deliverable** rather than the concatenation — how
many entries each holds is what the two commands above print, and the second one fails naming the
bucket that moved. The buckets: **standing limits** that no run can ever close, **published
caveats** that the ledger's own rule ejects, the **carry-forward**, what is **closed**, and what
stopped being meaningful.
**The count of source sites is deliberately not published as a number anywhere** — it
was counted by hand and nothing derives it, which is exactly the property that makes a number rot,
and the ledger says so in its own §0.5.

**The test is what stops this from being a tenth list.** `tests/test_unexercised_ledger.py`
resolves every anchor against the source line it quotes, re-counts every published total, refuses
an entry that sits in two buckets or names no exerciser, and **sweeps `docs/*-evidence.md` for any
ledger heading that is not one of the ten declared ones.**

> **THAT SWEEP BINDS THIS DOCUMENT, AND IT IS THE REASON §3 BELOW IS SHAPED THE WAY IT IS.** This
> file matches the sweep's glob. Had §3 been given a heading in the shape the previous nine phases
> used, `tests/test_unexercised_ledger.py` would have gone **red on an eleventh ledger section,
> written by the phase that consolidated the other ten.** A test that would catch its own author
> is the only kind worth having.

### 1.6 The postmortem

[`docs/postmortem.md`](postmortem.md), `ad78b19`. Its subject is §0.1 — **not an inventory of what
was built**, which nine phase documents already do at a length nobody should read twice. It names
nine specimens, every one from the phase that set out to fix documents, and it publishes four
conditions that would falsify it. It is not summarised here, because a summary is a second copy of
a claim, and a second copy going stale is the mechanism it is about.

### 1.7 The dated broker line — NOT discharged, and the header that claimed it was

`databricks/resources/streaming_managed_broker_job.yml` stated that `docs/f5-run-evidence.md`
*"carries the run ids, the row counts **and the date the broker went away**"*. **It carries the
first two.** The third cannot exist yet: the credits expire **~2026-09-03**, and this was
written on **2026-08-31**.

**A future obligation written in the present tense is how a promise gets read as a discharge**, and
a reader who trusted that header would go looking for a date that is not there. `086f149` corrects
the header to say the date is **owed**, names the probe it must come from, and says why it may not
be written from the calendar.

**The obligation itself is NOT discharged by this phase**, and §2.4 is the reason.

---

## 2. Predictions, published before the runs that test them

**Where these were first written, and it has to be said.** All five were published in the phase
plan on **2026-08-30**, before any of the runs that test them. That plan is in a git-ignored
working directory, **so a reader of this repository cannot check that claim** — the *provenance of
the date* is ***Reported***, while each outcome below carries its own label. Each prediction named
what would falsify it, and **each falsifier was a real outcome rather than a hedge — except one,
and that failure is published rather than tidied away.**

| # | prediction | status |
|---|---|---|
| 1 | ~~One quarantined `bronze_merchant` row closes **exactly six** ledger entries~~ | **FALSIFIED** — it closes **two** |
| 2 | UC lineage does **not** cover all seven bronze tables' paths to gold | **CONFIRMED IN SUBSTANCE — AND THE WORDING IS ILL-POSED** |
| 3 | The generated ADR index cannot parse a `## Status` block for **at least three** ADRs | **CONFIRMED** — exactly three |
| 4 | The Redpanda broker stops answering on or after **2026-09-03** | **UNSETTLEABLE INSIDE THE PHASE, BY CONSTRUCTION** |
| 5 | Closing the README defect leaves the GitHub repo description and topics stale | **CONFIRMED** |

### 2.1 — FALSIFIED, in the "fewer" direction, and the count is two

> *"One quarantined `bronze_merchant` row closes exactly six ledger entries. **Falsified by:**
> closing fewer — an entry needing something the row does not supply — **or more**, which would
> mean the ledger was double-counting."*

**It closes two, unconditionally**: `fdb:1521` (*"`fail_on_dq` and the `check_bad_rows` FALSE
branch never ran"*) and `fdb:1522` (*"`bronze_merchant_quarantine` has never held a row"*). Both
are in [`docs/unexercised-ledger.md`](unexercised-ledger.md) §4.2 with run `529699767706804` as
their evidence. **Controller-verified** by the run in §1.2; the ledger placement is re-derivable
with `uv run pytest tests/test_unexercised_ledger.py -q`.

**The prediction named a count and never a set.** It said *"exactly six"* and stopped there, so
there is nothing to check the two against, and **no ordinal here would be checkable either** — an
ordinal implying a set nobody published is the same species this section is about, so the ones
that stood here are gone rather than renumbered. What *can* be published is the set the
consolidated ledger identifies, and what each of those would actually need:

- **`fdb:1471`** — `unhashable_case_divergence` needs the poison to be one of the forty divergent
  characters. **And one row can never close it together with `fdb:1523`, the `_rescued_data`
  entry:** `rescued_data_present` sits above every per-table rule under first-match-wins, so a
  schema-drifted row can never *also* be reported as a case divergence. **The two are mutually
  exclusive on a single row by construction**, which is a fact about the rule ordering that the
  prediction's arithmetic silently assumed away.
- **`fdb:1526`, and ADR 0011's reversal condition with it** — both need the **vault** re-run after
  a repromote, and ADR 0011 asks for the **socios** number, so it would close in mechanism and
  stay open in quantity. §3 records why that re-run was refused.
- **`fdb:1525`** — *"`closed_by` has exactly one value"* — is **closable by no row of any shape**,
  which is the sharpest thing in this prediction. It is true **by construction**:

  ```bash
  grep -n CLOSING_STATE src/opl/vault/effectivity.py
  ```

  `CLOSING_STATE` is pinned to `absent_after_observation` and nothing else, so nothing can give
  `closed_by` a second value. **An entry whose falsifier does not exist is not debt** — and the
  ledger now books it as a published caveat rather than as carry-forward.
- **`fdb:1480`** — closed **before the prediction was written**, by F-DB's own 2,192-row merchant
  run, and **never struck.** It sits in [`docs/unexercised-ledger.md`](unexercised-ledger.md)
  §4.1, *closed and never said so*, the section that measures this phase's subject.

**The falsification rests on the two that closed, and not on the list above.** The run closed two
where the prediction said six; the list is why *six* was never reachable from the nine ledgers as
they stood — one entry closed for weeks, one closable by nothing, and two that cannot be closed by
the same row. **The prediction was assembled by reading nine ledgers as an inventory rather than as
claims**, which is precisely what the consolidated ledger exists to stop.

### 2.2 — CONFIRMED IN SUBSTANCE, AND THE WORDING IS ILL-POSED, WHICH IS PUBLISHED RATHER THAN REWORDED

> *"UC lineage does NOT cover all seven bronze tables' paths to gold, because it records
> executions. **Falsified by:** all seven appearing."*

**The substance holds, and §1.4 measures it.** On the four bronze tables that declare a gold path,
lineage's coverage is genuinely incomplete: `payments → dim_date` and `empresas → dim_date` are
declared and **absent**, because `dim_date`'s build reads nothing lineage records.

**But the stated falsifier is unreachable.** Three of the seven — `lookup`, `merchant` and
`socios` — **declare no gold path at all**, which the command in §1.4 prints in one line. *"All
seven appearing"* therefore cannot happen, whatever lineage does. **A prediction whose falsifier
cannot occur is not a prediction**, and this one sat inside the prediction list of the phase whose
hunted species — ADR 0018's — is exactly a check that reports the expected value no matter what is
true.

**It is recorded here rather than quietly reworded**, because restating a prediction in the
vocabulary its result later suggested is how a prediction stops being able to be wrong. The
substantive claim survives on the four tables where it can be tested; the arithmetic in its
falsifier does not.

### 2.3 — CONFIRMED: exactly three, and the honest command reads the ADRs rather than the index

> *"The generated ADR index cannot parse a `## Status` block for at least three ADRs — 0001, 0002
> and 0003 have none. **Falsified by:** the generator finding one, which would mean the Task 0 read
> was wrong."*

```bash
# what the generated page says about itself
grep -c 'no `## Status`' docs/adr/README.md

# what the ADRs say, which is the claim the prediction actually makes
for f in docs/adr/0*.md; do grep -q '^## Status' "$f" || echo "$f"; done
```

The second command returns **exactly three paths — `0001-dual-target-versions.md`,
`0002-two-layer-topology.md` and `0003-cnpj-extraction-layer.md`** — and it is the one that settles
the prediction. **The first checks the generator against its own output**, which would report the
expected value if the generator's parser and its reporting were wrong in the same direction. Both
are published because the pair is the point: **the cheap command and the correct one are evidence
for different things**, and only the second can falsify the generator. They agree today.

### 2.4 — UNSETTLEABLE INSIDE THE PHASE, BY CONSTRUCTION, AND THAT IS THE SECOND MALFORMED ONE

> *"The Redpanda broker stops answering on or after 2026-09-03. **Falsified by:** it still
> answering — in which case the sites carrying the date are corrected to what was measured, and ADR
> 0019 Decision 6's obligation is re-dated rather than discharged."*

**The plan set this prediction a test date that falls AFTER the phase's own work**, and no
probe taken before ~2026-09-03 could decide it. So it is not pending: **it could never have
been settled here, and that was true the day it was written.**

**TWO OF THE FIVE PREDICTIONS WERE MALFORMED, IN THE PHASE THAT HUNTS MALFORMED CHECKS.**
§2.2's falsifier could not occur — three of the seven tables declare no gold path, so *"all
seven appearing"* was unreachable. This one's falsifier was reachable but not in time. **A
prediction whose test the phase cannot run is a check that reports the expected value for the
whole phase**, which is ADR 0018's species arriving in the list of things this phase promised
to measure.

> **This paragraph said *"Today is 2026-08-31"* until the clock rolled past midnight while the
> phase waited on CI, and it went false without anyone touching the file.** It is fixed here by
> dating the sentence instead of dating the reader, which is the same repair the pull-request
> count needed in §0.1 and §1.1 — **a word like *today* is a hand-maintained value with no
> maintainer.** The substance never moved.

**ADR 0019 Decision 6's obligation is NOT discharged, and it is not abandoned either.** It is
carried as `f5:980` in [`docs/unexercised-ledger.md`](unexercised-ledger.md), with what would
exercise it written beside it and a test that fails if the row drifts from its source — which
is the machinery §1.5 exists to provide, doing the job it was built for. The future reader is
already protected without it: `streaming_managed_broker_job.yml`'s header says in its own words
that this is **a recorded run, not a job a future reader can run green**, and `086f149`
corrected the one site that claimed the date had already been written (§1.7).

**So waiting for the expiry would have bought one thing: replacing `~2026-09-03` with an exact
date in one document.** It would not have protected a reader who is already protected, and it
would have held ~~the project's last phase~~ **the phase** open for a string.

> **The premise under that last clause is gone, and the decision it justified got better.** It was
> written believing F7 was the last phase, which made not waiting a trade: the obligation would
> outlive the last thing able to discharge it. **F7 is not the last phase** (see the header), so
> the obligation is not stranded by this phase closing. It is `f5:980` in
> [`docs/unexercised-ledger.md`](unexercised-ledger.md) §3.4, a row later work can close by
> probing the cluster on or after the expiry and writing what came back into
> `docs/f5-run-evidence.md` — whichever way it comes back. **What was a debt with nobody left to
> collect it is a debt with a collector, and the row already states what would settle it.**

**Writing the line now from the calendar would publish a prediction in the shape of an
observation** — the exact failure ADR 0019 Decision 1 rejected its own first probe over, and the
reason the date matters at all is that *"expired trial"*, *"revoked ACL"*, *"wrong username"* and
*"no route"* produce one indistinguishable string.

> **An unmarked prediction is a worse-looking outcome and a better-behaved one.** A phase record
> that marks five of five looks complete; this one marks four and says why the fifth cannot be
> marked. **The obligation survives this phase and is carried as `f5:980`** in
> [`docs/unexercised-ledger.md`](unexercised-ledger.md) §3.4 — the one artefact here that is
> maintained rather than historical, and therefore the only place the obligation can still be found
> after F7 stops.

### 2.5 — CONFIRMED, and the stale figure is the exact one the README replaced

> *"Closing the README defect will leave the GitHub repo description and topics stale, because they
> are not tracked files and no commit touches them. **Falsified by:** them being current, which
> would mean somebody maintained them out of band."*

```bash
gh repo view Joorgem/open-payments-lakehouse --json description,repositoryTopics
```

- **The description still contains "71.9M rows"** — the exact figure T2's README replaced, so the
  defect the phase closed in the tracked file survives verbatim in the metadata printed beside it
  on the repository's front page. It also describes the project as a PySpark/Delta core on
  Databricks Free Edition and names **none** of the Data Vault, the Kimball star, the Unity Catalog
  governance or the triage agent.
- **The topics** are `apache-spark, auto-loader, data-engineering, data-quality, databricks,
  delta-lake, lakehouse, medallion-architecture, open-data, pyspark, python, unity-catalog` —
  medallion architecture is the highest layer any of them names: no data vault, no dimensional
  modelling, no governance, no DataOps.

**They are repository settings, not tracked files.** No commit reaches them and no test can lock
them, which is why the prediction could be made with confidence and why closing it is **not** this
phase's to do — the same column as making the repository public, which is the owner's. The exact
`gh repo edit` commands were handed over rather than run unilaterally.

**If they are still stale after this document published that they are stale, that is the species
this phase hunts, in the one place F7 chose not to reach** — and §3 carries it as such.

---

## 3. What F7 leaves unrun, and where the rest of it lives

**This section is deliberately short, and its shortness is the deliverable.** Protocol §9
condition 6 asks a phase to publish its own gaps. Nine phases did that and produced nine lists
nobody read together; **F7's answer is one maintained list with a test, and a tenth phase-shaped
list written here would be the exact defect T4 spent the phase ending.**

**Everything this project ships that has never had rows through it is in
[`docs/unexercised-ledger.md`](unexercised-ledger.md)** — classified, anchored to the line it
quotes, and re-derived by `tests/test_unexercised_ledger.py`. **That file is the carry-forward, and
it is the only list of its kind here that is maintained rather than historical.** What follows is
not a competitor to it: these are the five things **F7 itself** did not do, each of which is either
already an id in that file or names why it cannot be one.

> **THAT RULE WAS STATED HERE BEFORE IT WAS TRUE OF ITEMS 3 AND 5, AND THE CLOSING REVIEW CAUGHT
> IT.** It searched the ledger for both and found neither: they existed only in this document, and
> ~~**F7 is the last phase, so a path recorded only in a phase document has no consumer left**~~
> **a path recorded only in a phase document has no consumer** — which is the exact shape of the
> rows [`docs/unexercised-ledger.md`](unexercised-ledger.md) §4.1 measures. They are ids now,
> `repromote:21` and `vaultreg:5`, and the sentence above is left standing rather than softened
> because the gap it did not cover is the finding.
>
> **The struck reason was false the day after it was written, and the two ids it argued for are
> better placed for it, not worse.** F7 being last made those paths unreachable by anybody; with
> the repository still being expanded (see the header) they are reachable, and a reachable path
> buried in a phase document is precisely what §4.1's rows are. **So the rows stay.** What they
> have in the ledger and cannot have here is a place a later reader will open:
> [`docs/unexercised-ledger.md`](unexercised-ledger.md) §8 sets out the rule that would oblige the
> next closing review to say which of *that file's* §3 rows it closed. **It is proposed, not part
> of protocol §9 condition 6** — §8 says so in its own words, and §3.3(h) of
> [`docs/postmortem.md`](postmortem.md) argues it.

**1. The dated broker probe.** §2.4. Due ~2026-09-03, three days after this document. Carried as
`f5:980`. *What would exercise it:* a metadata fetch against the cluster on or after the expiry,
with the result written into `docs/f5-run-evidence.md` — whichever way it comes back.

**2. The GitHub repository description and topics.** §2.5. **Not in the ledger, and it should not
be** — every ledger id anchors to a line in a tracked file, and there is no such line to anchor to.
That absence is itself the finding. *What would exercise it:* the owner running `gh repo edit`.
Until then `gh repo view --json description,repositoryTopics` is the only check, and nothing in CI
can run a check against a settings page.

**3. `repromote_triaged_batch` was not run.** The 1,089-row batch §1.2 quarantined **is still
sitting unpromoted in staging**, which is what fail-closed means and is not a defect.
`databricks/resources/repromote_batch_job.yml` is the path that promotes it after triage. Carried
as `repromote:21`. *What would exercise it:* running that job for `table=merchant` against the
batch that run left in staging.

**4. The vault was deliberately not re-run over that snapshot, and the reason is a hazard this
phase priced before it bit.** The observation ledger's presence universe is bronze ∪ quarantine
with `months=None`, so re-running `opl_vault_merchant` over the poisoned snapshot **without an
intervening repromote** puts every previously observed key in neither table at that instant — which
is `absent_after_observation`, which **is** `CLOSING_STATE` (§2.1). On today's population that is
roughly **1,103 false closes against 16 real ones, appended to an append-only satellite and
indistinguishable afterwards.** The safe order is: land, let `bronze_merchant` fail at
`fail_on_dq`, run `repromote_triaged_batch`, and only then run `opl_vault_merchant`. It is written
out at [`docs/unexercised-ledger.md`](unexercised-ledger.md) §3.9, placed deliberately in front of
the entry a future reader would otherwise try to close first.

**5. F2 wave 2 was never started, and this phase did not start it.** The phase plan refused new
features in its own words, and named this as staying unstarted (***Reported*** — that plan is
git-ignored). Carried as `vaultreg:5`, against the sentence in `src/opl/vault/registry.py` that
stakes DV2's extensibility claim on wave 2's three tables: the mechanism has never been exercised
by anything but a throwaway fixture domain. *What would exercise it:* wave 2 itself. It is
repeated here so ~~the last phase record~~ **this record** does not read as a claim of
completeness.

> **One thing this section cannot do, and it is worth stating ~~at the end of the last phase
> record~~ plainly.** Whether a ledger row is *still* true is not derivable by any test here — no
> test can run a Databricks job. Closing a row is a human act, and the only defence F7 built
> against another silent closure is that **closing one is now a visible edit to a single tracked
> file** rather than a strike buried five hundred lines inside a two-thousand-line phase document.
> [`docs/unexercised-ledger.md`](unexercised-ledger.md) §8 proposes the process rule that would
> make it stick, and names the half of protocol §9 condition 6 that is missing: **a phase must
> publish what it closed of somebody else's list, not only its own.**
