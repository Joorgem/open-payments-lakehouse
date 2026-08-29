# F5 — Streaming: what was measured, and what was predicted before it was

**Controller-verified** means the controller ran the command in this session and read the output.
**Reported** means an implementer, a reviewer or a task's stdout said it. Every claim below carries
one of the two labels, on `docs/f4-run-evidence.md`'s model — that document's preamble explains why
the split exists, and this phase inherits both the rule and the reason.

**Predictions are published BEFORE the run that tests them** (master protocol §4.5). A number first
written down after the run that produced it is not a prediction. §2 is where they live, and §2.1 was
written while the run that tests it was already in flight and its result unknown.

**Where the labels bind, stated because this phase applied them unevenly and was caught twice.**
Every claim in §0–§2 carries one. **§3 is uniformly *Reported* unless a cell says otherwise** — it is
a ledger of what has NOT run, and its entries are read off the code and the record rather than
measured. A hybrid such as *"controller-verified via a reviewer's probe"* is **not** a label this
document defines; a reviewer's measurement is *Reported*, and where the controller re-ran something
separately that is said as its own sentence.

**And the statement ids below have expired, which F4's preamble warned of and this one first
omitted.** `01f19c26-a459-…`, `01f19c26-dfef-…` and `01f19c27-5a37-…` now return the API's own named
refusal — `Error: The statement <id> was not found.` — at ~5 days. That is **expiry, not
fabrication**, and the two are distinguishable: the API names the statement, while MSYS path-rewriting
returns a bare `Error: Not Found` that names nothing (F4 §0.1's correction). **A statement id is
provenance for work of the last day**; the job `run_id`s in this document are durable and are what a
later reader should follow.

> **The phase plan is NOT part of this repository.** It lives in a git-ignored working directory, so
> no link to it is given: F3 shipped a section pointing a public reader at that directory and they
> reached nothing. Everything a reader needs from it is here.

---

## 0. Task 0 — measured before this phase's plan existed

The phase had one premise nobody had ever tested, and it decided the shape of everything else.

### 0.1 Redpanda was declared and had never been started

**Controller-verified 2026-08-19.** `docker-compose.yml` declares `redpanda`
(`redpandadata/redpanda:v24.2.7`, advertising `PLAINTEXT://localhost:9092`) beside `postgres`, and
has since F0. **The container had never been created on this box** — on `docker compose up -d` the
postgres container was four days old while redpanda was *Created* for the first time.

- `rpk cluster health` → `Healthy: true`, controller 0, zero leaderless partitions.
- Round trip: topic `opl-probe`, three records produced at offsets 0/1/2, consumed back identical.
- `tests/integration/test_redpanda.py` **already existed** and produced/consumed through
  `confluent_kafka` — marked `integration`, deselected by `addopts`, **never run in CI**.
  `.github/workflows/ci.yml`'s own comment names it as a reason a bare `-m integration` would go red.

### 0.2 THE DESIGN SPEC'S PARENTHETICAL IS FALSE, AND IT HAD NEVER BEEN MEASURED

The master design spec declares the topology and then states a limit in parentheses: *"gerador →
Kafka (Redpanda em Docker) local; Auto Loader file-streaming no Databricks (**limitação honesta: Free
não conecta Kafka externo**)"*. That sentence shaped the architecture of this phase before anybody
tested it.

**Controller-verified 2026-08-19.** Job run **`592571730849259`**, task run **`208476979455163`**,
SUCCESS, on Free Edition serverless jobs compute:

| target | result | what it establishes |
|---|---|---|
| `github.com:443` | **CONNECTED**, 20 ms | serverless has outbound egress at all |
| `1.1.1.1:53` | **CONNECTED**, 0 ms | egress is not 443-only |
| `github.com:22` | **CONNECTED**, 16 ms | second non-443 confirmation |
| **`portquiz.net:9092`** | **CONNECTED**, 205 ms | **egress on the Kafka port itself** |
| `portquiz.net:443` | CONNECTED, 96 ms | control: the host is up, so the cell above is about the port |
| `177.115.164.129:9092` | **timed out**, 8,007 ms | this dev box is not reachable |
| `192.168.15.9:9092` | timed out, 8,008 ms | negative control: RFC1918 is unroutable |
| `127.0.0.1:9092` | **`ConnectionRefusedError`, 0 ms** | **the probe can report a refusal**, so a timeout is a real absence of route |

And the reader exists on both computes: `SHOW FUNCTIONS LIKE '*kafka*'` on the `opl-free` warehouse
returns **`read_kafka`** (statement `01f19c26-a459-1763-a951-2485be31ac6e`), and
`spark.readStream.format("kafka")` builds a plan on jobs compute.

**So the blocker is not the platform, it is the NAT.** The dev box is `192.168.15.9` behind a
residential router; nothing forwards 9092. **§1.2 closes this end to end** — a serverless job has
since read rows from a managed broker over SASL_SSL.

### 0.3 THE FIRST PROBE THIS PHASE WROTE COULD NOT FAIL

**Controller-verified, and recorded rather than deleted.** The first reachability test used
`read_kafka` from the SQL warehouse. Against `177.115.164.129:9092`, where nothing listens, it
returned

```
kafkashaded.org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment. Call: listNodes
```

and against **`github.com:443`, which certainly accepts TCP**, it returned *the same string*. No
egress, wrong protocol and nothing-listening all produce one output. Per ADR 0018 that is not a
check. It cost two statements — `01f19c26-dfef-1c82-b235-192c2f8086d3` and
`01f19c27-5a37-1854-a2ab-608ac1bd2d8b`, 3 m 22 s and 3 m 06 s — and it is why §0.2 is a socket matrix
carrying two negative controls.

**A second trap in the same exchange, and it nearly cost an hour.** The `PARSE_SYNTAX_ERROR` those
probes returned echoed the bootstrap address back as `177.115.164.0`, two characters short of what
was sent. The statement sent was correct: the two probes' caret positions differ by exactly **6**,
which is `len('177.115.164.129:9092') − len('github.com:443')` and not the 4 the `.0` spelling gives.
**The echo was wrong and the statement was right** — an error message is evidence about the error,
not about what you sent.

### 0.4 Local Spark reads Redpanda, and the connector is a real setup cost

**Controller-verified.** `pyspark 3.5.9` bundles **no** Kafka jar. With
`spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9`, Ivy resolved **11 artifacts,
57,002 kB** in ~12 s and the read returned the three probe records with their partitions and offsets.

**Those 11 are the Kafka connector's closure, and a later section of this document says 14** — the
two are reconciled here rather than left for a reader to notice. A session that asks for Kafka
*and* Delta resolves **14**; Delta's three (`delta-spark`, `delta-storage`, `antlr4-runtime`) resolve
whether or not Kafka is requested, so the connector's own cost is the 11 / 57,002 kB above and the
cache a CI job restores holds all 14 / ~64,317 kB.

**Consequence, pre-decided before any implementer met it:** the jar does **not** go on
`opl.spark.local_session`, because that would give all 2,683 tests a Maven resolution and a network
dependency at session start.

---

## 1. What has been built and run

### 1.1 T1 — one corpus, two transports

**Committed at `6fa242a`.** `src/opl/streaming/` publishes a declared profile's records to a Kafka
topic through **the serialiser that already exists**, so every digest F1b pinned still describes the
stream: `b"".join(message_values(records)) == generated_landing.serialised_bytes(records)`, byte for
byte, with the newline terminator carried **inside** each message value.

**Controller-verified**: `json` is imported nowhere under `src/opl/streaming`; ruff clean; 24 unit
tests and 4 size-cap tests pass; 5 tests pass under `-m redpanda` against the live local broker;
both parametrised sweeps carry the two new modules.

> **A CONTROLLER-LEVEL CHECK THAT WAS ITSELF THE SPECIES.** `tests/test_size_caps.py` reads
> `git ls-files`, so it is **blind to untracked files**. Run against the new work before staging, it
> reported **green having measured none of it**. The caps were only really checked after `git add`.
> Recorded because the controller published it as a verification before noticing.

**The review chain, because it is why the diff looks as it does.** Implementer → independent reviewer
→ correction → review of the correction → correction 2.

| stage | outcome |
|---|---|
| independent reviewer | **12 findings, 3 HIGH** |
| review of the correction | **6 findings — every serious one in a NEW CLAIM the correction wrote, none in a bug it fixed** |

**Reported** by the reviewer, and it is the phase's first instance of the species: the internal
rebuild guard was near-circular. Measured against the **real** serialiser — terminator dropped
entirely → **ACCEPTED**; default `json` separators → **ACCEPTED**; `sort_keys=True` → **ACCEPTED**.
Its docstring claimed three defects and caught one. A line-count cross-route now refuses the first,
and the prose says plainly that a change to the shared serialiser is caught by the **external** pin
`b45f1dc7…` rather than there.

**And the live test's stated defence did not defend.** Its docstring claimed the pinned `range(24)`
offsets would catch *"a topic published to twice and consumed once"*. The reviewer published the
corpus twice to a fresh topic and ran the test's own assertions: offsets `range(24)` **True**, bytes
**True**, sha256 **True** — **it passed over 48 records.** The floor was real; there was no ceiling.
A watermark-summing ceiling now refuses it, and — because the first correction shipped that ceiling
with **no test of its failure arm**, which is the same species one level up — a live test publishes
the corpus twice and asserts the refusal.

### 1.2 THE MANAGED BROKER, AND THE PREMISE CLOSED END TO END

**Controller-verified 2026-08-19.** The cluster was not created by this phase — Redpanda provisions a
`welcome` Serverless cluster at signup, `STATE_READY` in `us-east-1`, public networking enabled. The
SASL user, four ACLs and the topic were created through the Cloud APIs.

| step | result |
|---|---|
| user `opl`, `SASL_MECHANISM_SCRAM_SHA_256` | created |
| ACLs: TOPIC `*`, GROUP `*`, CLUSTER `kafka-cluster`, TRANSACTIONAL_ID `*` | four, `OPERATION_ALL` / `PERMISSION_TYPE_ALLOW` |
| topic `opl-payments`, 3 partitions, RF 3 | created |
| produce + consume from the dev box over SASL_SSL / SCRAM-SHA-256 | round trip OK, partition 2 offset 0 |
| **produce on the dev box, read from a Databricks serverless job** | **`spark.read.format("kafka")` returned the row** — job run **`382097683531247`** |

**§0.2 had shown only a plaintext TCP connect. This is the SASL_SSL handshake, the SCRAM exchange,
the metadata fetch and a row crossing from a laptop to the workspace.**

> **NO LATENCY MAY BE QUOTED FROM IT.** The same read took **19,606 ms** on its first run and
> **2,730 ms** on the second, over identical data. That is F4 Task 6's first-uncached-run trap
> reproduced on this phase's first streaming measurement.

#### A SECRET'S VALUE IS SCRUBBED FROM EVERY LOG LINE, AND `opl` IS THIS PROJECT'S OWN PREFIX

**Controller-verified with a control.** The first run stored the Kafka **username** in the Databricks
secret scope beside the password. Its output read

```
row | p=2 off=0 | [REDACTED]-cloud-probe-c84ef1c5
```

for a record whose value was `opl-cloud-probe-c84ef1c5`. Databricks replaces every occurrence of a
**secret's value** in task output, and the username was `opl` — the prefix of this repository's
catalog, its topic, its job names and its package.

**The control, because an explanation published without testing it is the error F4 retracted:**
`kafka_user` was deleted from the scope, the literal put in its place, **nothing else changed**, and
the same row came back `opl-cloud-probe-c84ef1c5` — job run **`838423822976396`**.

**The rule: only the PASSWORD is a secret.** A bootstrap host and a username are coordinates, not
credentials, and making a short common string a secret turns `[REDACTED]` into the published answer
for table, job and topic names — **a silent substitution that preserves every other number, in a
document whose whole purpose is quoting run output.** A reader cannot tell a redaction from a value.

#### The trial balance is NOT readable from the API this service account has

**Controller-verified, two attempts, then abandoned.** `/v1/billing`, `/v1/usage`,
`/v1/subscriptions` and `/v1/organizations` return the control plane's own JSON `NOT_FOUND` — the
service's refusal, not a shell artefact. The cluster's Prometheus endpoint returns **401** both with
the control-plane bearer token and with SASL basic auth. **So this phase publishes no figure for how
much of the trial credit is left.** What does not depend on it: the corpus is 40,150 records at
~293 B — ≈ **12 MB per full replay** — and the producer counts what it sends.

**The broker is ephemeral, and that is a design constraint.** The credits expire ~2026-09-03 and the
cluster stops. Nothing in `databricks/resources/` may be left pointing at it, and this document must
say the broker is gone once it is — otherwise the next reader re-runs the job, gets a connection
failure, and reads a dead trial account as a regression in this repository.

---

### 1.3 T6 — the CI job, WRITTEN AND NEVER RUN

**Stated in §1 because a reader looks here for what was built, and until the agent that wrote this phase's ADR said so this
job appeared in this document only inside a subordinate clause.** `.github/workflows/ci.yml` gains a
`redpanda` job that starts the broker from the committed `docker-compose.yml` and runs
`uv run pytest -m redpanda -v` — 22 tests across 5 files, so the streaming proof runs somewhere other
than one Windows box.

**It uses compose rather than a `services:` block** to avoid a second spelling of the broker's launch
arguments beside the one the compose file already carries — the defect class this repository polices
hardest, and the one the `postgres` job above it pays for today by duplicating its service
definition. **The reason first written for that choice was factually false** (that a service container
cannot supply a container command; it can, since the 2026-04-02 changelog), **and the correction of
it carried two invented dates**. Both are recorded in the file rather than quietly fixed, and the
decision survives on the second ground, which was always the stronger one.

> **NOT ONE CI JOB ON THIS BRANCH HAS EVER EXECUTED.** `ci.yml` fires on pushes to `main` and on
> pull requests; this branch has had neither. **The job's first run is its first execution**, which
> also makes it the first measurement of whether the Windows JVM-startup race — 2 failures in 5
> starts here — exists on `ubuntu-latest`. The 75-minute ceiling is set from a 38-minute Windows
> measurement and is an expectation about Linux, not evidence about it. §3 lists it as unexercised.

## 2. Predictions, published before the runs that test them

### 2.1 THE EXACTLY-ONCE PROOF — published 2026-08-19, while the run was in flight and its result unknown

**Written before any number came back.** The experiment is two arms over **one fault and the same
offsets**: a `foreachBatch` sink that appends with no idempotency key (**NAIVE**), and the same sink
appending under Delta's `txnAppId`/`txnVersion` (**GUARDED**). The fault lands after the batch's data
write commits and before the streaming checkpoint commits — the only window in which a replay can
double-write.

**The prediction, in the form that can be wrong:**

| arm | rows after restart | distinct `transaction_id` | excess |
|---|---|---|---|
| **NAIVE** | **greater than the corpus** | ~~equal to the corpus~~ | **equal to the replayed micro-batch's row count, and greater than zero** |
| **GUARDED** | equal to the corpus | ~~equal to the corpus~~ | **0** |

> **THE `transaction_id` COLUMN OF THAT TABLE IS FALSIFIED, IT IS THE CONTROLLER'S OWN, AND IT IS THE
> EXACT ERROR THE PARAGRAPH THREE BELOW IT WARNS AGAINST.** Measured: **24** in both arms, against a
> corpus of **29 records**. The cell is true only if *"the corpus"* silently changes meaning between
> one column and the next — 29 delivered records in the `rows after restart` column, 24 distinct
> events in this one. **A prediction whose terms shift between two cells of one row is not a
> prediction; it is two, and one of them is unstated.**
>
> It should have read: **equal to the corpus's distinct transaction ids (24), which is deliberately
> NOT the number of records (29).**
>
> Caught by the independent reviewer of T2+T3, who was asked to check the prediction clause by clause
> and did. **The shipped code never had this confusion** — `_DELIVERED = 29` and `_CLEAN_ROWS = 24`
> are separate constants, asserted separately. The defect is in the evidence document, written by the
> controller, immediately above its own warning not to blur the two.
>
> The unedited prediction is recoverable and this retraction does not disturb it:
> `git cat-file -p 59eee0bd3cb35c65895d196ffa9db5f5d181266e`.

**What falsifies it, and each is a real outcome rather than a hedge:**

- **NAIVE excess of 0 falsifies the whole experiment**, not the guarantee. It would mean the fault
  never reached the window between the data commit and the offset commit — so the GUARDED arm's zero
  would be worth nothing, because a zero produced by a fault that did not fire is the same zero
  either way. **This is the outcome this phase most needs to be able to see**, and it is why the
  NAIVE arm exists at all.
- **GUARDED excess greater than 0** falsifies the exactly-once claim itself, and would be the more
  valuable of the two findings.
- **Either arm reading zero rows** falsifies nothing and means the stream consumed nothing — a
  checkpoint that had already drained the topic, or `startingOffsets` pointing past the corpus. Every
  assertion in this experiment therefore carries a non-zero floor on rows actually processed.

**And a distinction this phase must not blur, stated before the numbers arrive.** The 150 duplicates
F1b injects are the *producer* delivering one `transaction_id` twice — a property of the data, already
measured in bronze (40,150 rows, 40,000 distinct). Exactly-once is a property of *processing*. A
document that lets a reader take one for the other has published the wrong claim, however correct its
arithmetic.

### 2.2 THE RESULT — CONFIRMED, AND THE NEGATIVE CONTROL FIRED

**Reported** — and the label matters here more than anywhere else in this document, so it is stated
against the preamble's own definition rather than loosely. The numbers were produced by T2+T3's
implementer and **reproduced by the independent reviewer** through their own probe and again through
the shipped suite: two routes, neither of them the implementer's report. The preamble defines a
reviewer's measurement as *Reported*, so that is what this is.
**What the controller verified separately** is narrower and is stated as its own thing: that the
suite asserting these numbers passes on this box — ~~348 tests over the streaming and sweep files~~,
a figure with no file list behind it and which no combination of the streaming and sweep files
reproduces; the re-runnable form is
`uv run pytest tests/test_streaming_*.py tests/test_payment_streaming.py tests/test_size_caps.py -q`
— that
ruff is clean, and that the size caps hold once the files are staged.

> **THIS SECTION SAID "CONTROLLER-VERIFIED" UNTIL T7's AUTHOR READ IT AGAINST THE PREAMBLE.** The
> phase's headline result carried the one label the document asks a reader to trust, and carried the
> wrong one — while §2.4 and §2.5 got the same distinction right. A convention applied unevenly is
> weaker than no convention, because a reader cannot tell which sections were careful.

**The corpus is 29 delivered records** — 24 distinct events plus 5 injected redeliveries — over one
partition at `maxOffsetsPerTrigger=10`, so `availableNow` resolves batches of **10 / 10 / 9** before
the first one runs. **It is NOT F1b's 40,150-record corpus, and no number here may be attached to
that one.** The scale was chosen so a single micro-batch is a countable unit; the mechanism does not
depend on it.

| arm | landed | distinct deliveries | **duplicates** | distinct `transaction_id` |
|---|---|---|---|---|
| **NAIVE** — `foreachBatch`, plain append | **39** | 29 | **10** | 24 |
| **GUARDED** — `txnAppId` / `txnVersion` | **29** | 29 | **0** | 24 |

**Every clause of §2.1 that could be tested was**, and the falsified one is struck above rather than
removed. The excess is **10 = the replayed batch's row count > 0**, as predicted.

**And the reviewer strengthened it past what the tests asserted:** the duplicated rows *are* offsets
**10–19** — the killed batch — each landed **exactly twice**. That identity is now asserted rather
than measured once.

#### The fault was located from the filesystem, not from the code

The claim that decides the whole experiment is *where* the fault lands. **Reported** — measured by
the reviewer through their own probe, reading the two artefacts that answer it — the streaming checkpoint
and the Delta log — in both arms, at the instant between the fault and the restart:

```
delta commits       : [...0000.json, ...0001.json]   <- batch 1's DATA WRITE committed
checkpoint offsets/ : ['0', '1']                     <- written BEFORE a batch runs
checkpoint commits/ : ['0']                          <- written AFTER a batch finishes
delta rows          : 20   offsets in table: [0..19]
```

`commits/1` does not exist while `offsets/1` does: **the data was durable and the offsets were not.**
That is the window, identically in both arms. A millisecond timeline from two independent clocks puts
the replay's data commit strictly between batch 1's data commit and batch 1's offset commit, and
`offsets/1`'s mtime is from the *faulted* run — so the restart reused the same offset-log entry
rather than planning a new batch.

**The GUARDED arm's mechanism is visible in the same place:** its Delta log carries `txn` actions for
versions 0/1/2, and the replay of batch 1 **produced no Delta commit at all** — the transaction was
dropped. The NAIVE arm's `txn` actions are `[]`.

#### Why `transaction_id` cannot be the thing counted, measured rather than argued

**24 in both arms** — the identity column reports the same number whether the pipeline is
exactly-once or not, because the corpus carries deliberate redeliveries. A proof counted over it
would have shown the two arms identical and concluded the opposite of the truth. The measure is taken
over `(kafka_partition, kafka_offset)`, the coordinate of the *delivery*, and
`.select(...).distinct().count()` is used rather than `countDistinct` — this repository lost 8,761
rows to `COUNT(DISTINCT …)` dropping NULL-bearing rows once, and a NULL refusal now sits in front of
the measure.

#### The acceptance cut both ways in practice, not only in prose

§2.1 said a NAIVE excess of zero would falsify the experiment rather than confirm the guarantee.
**That outcome was produced deliberately and the suite reports it by name** — giving the NAIVE arm the
idempotency key leaves it at 0 duplicates and the negative control fires with its own message
(*"the NAIVE arm did not duplicate. This does not mean the pipeline is exactly-once…"*), rather than
letting a zero pass as a pass. Removing the key from the GUARDED arm lands 39/10 and turns the proof
red. **Both failure directions were shown to be reachable before either number was published.**

#### Two environment facts this proof rests on, measured rather than inherited

**Local Spark on Windows fails to start a session at a rate this phase measured but cannot
explain.** The symptom is a flood of
`java.lang.NullPointerException: Cannot invoke "org.apache.spark.storage.BlockManagerId.executorId()"`
from `BlockManagerMasterEndpoint.register`, ending in
`Py4JError: An error occurred while calling None.org.apache.spark.api.java.JavaSparkContext`. Ivy
resolves all 14 artifacts first, so it is not the download.

> **A HYPOTHESIS WAS RAISED AND FALSIFIED BY THE CONTROLLER'S OWN PROBE, AND THE PROBE IS THE POINT.**
> Only the Kafka session failed while the Delta-only session started fine, and the Kafka session adds
> `hadoop-client-api`/`hadoop-client-runtime` 3.3.4 — which pyspark already bundles. Duplicated
> classes was a plausible mechanism with a real discriminator. **Measured across five starts:** the
> shipped configuration started **twice**, `spark.jars.excludes` for both hadoop artifacts started
> once and **failed** once, and pinning `spark.driver.host`/`bindAddress` to loopback **failed**.
> **The failure does not follow the configuration.** It is a race, and the two "fixes" fix nothing.
>
> **What that avoided is worth more than the diagnosis.** The repair would have touched
> `opl.spark.local_session`, which all 2,683 tests share, to treat a symptom it does not cause — and
> would have shipped a new, false explanation for why it worked. Five samples is a small sample and
> is reported as one: this is not a rate.

**It is a Windows-local fact and is NOT projected onto CI**, which runs `ubuntu-latest` where this
signature is not documented. T6's job measures it there rather than inheriting this.

**And every failed session start leaves an orphan topic.** The fixture waits for the broker to
confirm deletion — added after a reviewer reproduced orphans — but that covers teardown, not a
fixture that dies during **setup**: the topic exists, the session raises, and the finaliser never
runs. Harmless here because topic names carry a uuid, but it is the doubled-corpus hazard the
conftest's own docstring names, arriving through the door the hardening did not cover. **Listed as
unclosed.**

**How the suite was verified, and one trap in the verifying itself.** Three background runs were
reported `killed` by the harness and read as failures; the third had in fact written
`15 passed, 25 deselected in 125.64s` to its output file before the shell died. **The JVM's process
tree outlives pytest**, which is exactly why `CLAUDE.md` says to redirect to a file and read the
file. The controller read a terminal status as a test result twice before checking the file.

### 2.3 LATE ARRIVAL AND DEDUP — published 2026-08-20, while the run was in flight and unknown

**Written before any number came back.** The corpus is the `promotable` profile, whose every figure
is arithmetic on a declaration in `src/opl/generator/profiles.py` and needs no run: **10,000 events,
800 legitimate repeats, 150 redeliveries, 100 late arrivals delayed by exactly `LATENESS_WINDOW_MS`
(3,600,000 ms)**, delivered as **10,150** records.

**Lateness is injected into `emitted_at` and nothing else** — the payment did not move, the delivery
did — so a late record appears in delivery order after records whose `event_time` is newer.

#### The late-arrival prediction, in the form that can be wrong

**Two runs over the SAME corpus at two watermark thresholds**, one narrower than the injected delay
and one wider:

| arm | landed rows |
|---|---|
| watermark **narrower** than 3,600,000 ms | ~~the corpus **minus the 100 late records**~~ |
| watermark **wider** | ~~the whole corpus~~ |
| **difference** | **exactly 100** |

> **BOTH PER-ARM CELLS ARE FALSIFIED AND BOTH ARE THE CONTROLLER'S.** Predicted 10,050 and 10,150;
> measured **9,900 and 10,000**. Each is 150 low, and the 150 is the redelivery count.
>
> **The mechanism is the error, not the arithmetic.** This section wrote the watermark prediction and
> the dedup prediction as two independent tables, as if a row could be counted once for lateness and
> again for duplication. **The implementation composes them into ONE operator chain**, so the dedup
> removes its 150 from *both* arms before either count is taken. Predicting two effects separately
> for a system that applies them together is the same class of error as §2.1's — a term that changes
> meaning between two places in one document.
>
> **The bolded clause survives, and it is the one the experiment was built to test:** the difference
> is **exactly 100**, confirmed at the shipped rate limit and again at a limit nobody tuned. Found by
> the independent reviewer of T4+T5, checking §2.3 clause by clause because they were asked to.

**One arm alone would be a demo.** A single run that drops nothing is indistinguishable from a
watermark that was never consulted, and this phase has already published three checks with that
shape. The pair is the measurement.

#### The dedup prediction

Over the delivered 10,150: **exactly 150 rows collapse** (`COUNT(*) − COUNT(DISTINCT transaction_id)`
= 150, the injected redelivery count), leaving **10,000**, of which **9,200 are distinct
business-attribute tuples** — because **the 800 legitimate repeats SURVIVE**. A repeat is a customer
paying the same supplier the same amount twice: same payer, payee, amount, currency and method, its
own `transaction_id`, and ordinary business.

#### What falsifies each, and every one is a real outcome

- **Both watermark arms landing the same count falsifies the experiment, not the guarantee.** It
  means the watermark never bit — a stateless sink (a watermark outside a stateful operator discards
  nothing), a single micro-batch (a watermark computed from data already seen starts at its floor),
  or a threshold wide enough to admit everything. **This is the outcome the pair exists to make
  visible.**
- ~~**950 collapsing falsifies the dedup key**: it was taken over the business tuple, and the 800
  legitimate repeats were destroyed along with the 150 redeliveries.~~ A test asserting only "150
  fewer rows" would not see it, which is why the surviving count is asserted too.

  > **THIS FALSIFIER WOULD NOT HAVE FIRED, AND IT IS THE CONTROLLER'S SECOND DEFECT IN ONE SECTION.**
  > Measured with the business-tuple key: **526 collapse, not 950** — 150 redeliveries plus 376
  > ordinary payments destroyed, leaving **424** of the 800 repeats standing. So a run that had taken
  > the wrong key would have shown a number this document never named, and **the tripwire published
  > here would have stayed silent.**
  >
  > **The reason is worth more than the correction.** The shipped operator is *windowed*: it collapses
  > only the repeats whose copies fall inside its state window, so the damage is an artefact of the
  > batching and lands on no round number at all. The reviewer verified the mechanism quantitatively
  > — the coarser rate limit collapses **more** (393 against 376), which is the direction the
  > explanation requires, and the corpus's median repeat gap of 8.7 M ms against a ~8 M ms effective
  > window is why the survivors land near half rather than near 0 or 800.
  >
  > **What actually closes the trap is the shipped assertion `surviving_repeats == 800`**, which the
  > implementer wrote — not this document's arithmetic. A falsifier that names the wrong number is
  > worse than none, because it reads as coverage.
- **A surviving-repeat count of zero falsifies nothing and means the test asked nothing** — there
  would have been no legitimate repeat for a dedup to be wrong about.

**And this measures a property of the DATA, not of the processing.** The 150 are the producer
delivering one `transaction_id` twice. §2.2's exactly-once proof is about what a pipeline does when
it dies mid-batch. The two are separate claims over one corpus, and this document keeps them apart
because §2.1 already blurred them once, in the controller's own hand.

### 2.4 THE RESULT — the difference is 100, and Spark's watermark is two batches behind its own report

**Reported** by the implementer, **independently reproduced by the reviewer** through their own probe
and through the shipped suite. Corpus: the `promotable` profile, 10,150 delivered records, one
partition, 133 records per trigger → 77 micro-batches on both arms.

| arm | watermark delay | landed |
|---|---|---|
| DROPPING | 262,500 ms | **9,900** |
| KEEPING | 3,600,000 ms | **10,000** |
| | | **difference = 100** |

**100 is `promotable`'s declared `late_count`**, and `dropped_rows` reads that declaration rather
than a literal — verified by reading `LatenessBoundary.dropped_rows`, which returns
`defects.late_count` and consults neither the rate limit nor the margins.
~~That grep over the module finds four hits, all in docstrings.~~ **It returns TEN, and returned
ten at every revision on this branch** — a re-runnable measurement, offered as evidence, that does
not reproduce in one command. All ten are prose and none is a code literal, so the claim survives;
the evidence given for it did not. Both arms consumed
10,150 rows across the same 77 batches, read from Spark's **source-side** progress, so the difference
cannot be a short read.

**Dedup, on the arm where nothing was dropped:** 10,150 delivered → **10,000 landed, so exactly 150
redeliveries collapsed**; 10,000 distinct `transaction_id` over **9,200** distinct business tuples,
so **the 800 legitimate repeats survived**. The key is `transaction_id` alone and shipped code
refuses any key touching the business attributes.

#### THE QUESTION THE REVIEW WAS SENT TO ANSWER: DERIVED, OR SEARCHED?

The implementer's first run produced **97**, not the predicted 100. They then changed the
configuration and got 100. **That sequence has two readings and they are opposite**, so the reviewer
was asked to decide it by evidence rather than to check the number.

**Verdict: DERIVED.** Four things establish it, and the fourth is the one that settles it:

1. **Nothing in `src/` compares against 100.** The discriminating assertion lives in the test, where
   one side is two Delta `.count()`s and the other is the profile declaration. Shipped code only
   requires `> 0` — **so a 97 would have been accepted by the code and failed in the test**, which is
   the correct place for it.
2. **Sweeping the rate limit 1…260: 191 accepted, 69 refused, and every accepted limit yields a
   positive dropping delay.** ~~And the prediction is 100 on all 191, so no legal configuration
   yields a different number.~~
   > **THAT SECOND CLAUSE IS AN IDENTITY WEARING A MEASUREMENT'S CLOTHES, AND IT IS THE
   > CONTROLLER'S.** `LatenessBoundary.dropped_rows` returns `self.late_count` and reads neither the
   > limit nor the margins, so it *could not have come out otherwise* — sweeping 191 limits to
   > observe it is like measuring that a constant is constant. What the sweep genuinely establishes
   > is the half above: that 191 limits are **accepted** at all, each with a usable delay, so the
   > shipped one was drawn from a wide legal set rather than being the only thing that worked.
   > Found by the reviewer of the correction, reading the field's definition instead of the sweep's
   > output.
3. **The derivation transfers and refuses.** A corpus with 37 late arrivals predicts 37; one with 12
   predicts 12; one it cannot separate is **refused** with a negative margin rather than answered.
4. **The reviewer ran it at a rate limit nobody had used** — 260/trigger, where the narrowest late
   margin is 60,000 ms against 133's 525,000, an **8.75× tighter** boundary. First try:
   **9,900 / 10,000, difference 100.** A boundary tuned to make one configuration come out right does
   not survive an untuned one at a margin that much tighter.

#### THE MECHANISM BEHIND THE 97, AND HOW IT WAS MADE FALSIFIABLE

**Spark 3.5 filters a batch's late events against a watermark TWO batches behind the data, while the
watermark it reports in `StreamingQueryProgress` is ONE batch behind.** The reported value for batch
N equals `max(event_time over batches 0..N−1) − delay`, verified to the millisecond; the three
survivors of the first run sit in batch 50 with event times strictly between the values reported for
batches 49 and 50; and a pure-Python re-simulation reproduces **the same 97 rows and the same three
file positions** at lag 2 and at neither 1 nor 3.

> **THE REVIEWER'S SHARPEST FINDING WAS NOT ABOUT THE NUMBER — IT WAS THAT THE CONSTANT COULD NOT BE
> TESTED.** At the shipped configuration a one-batch and a two-batch lag remove **the same 100
> identities**, so the headline run said nothing about which was right. The only separating
> observation lived in an uncommitted scratchpad. `LATE_EVENT_WATERMARK_LAG_BATCHES = 2` was true,
> well-evidenced, and **unfalsifiable from anything a reader could run** — this project's signature
> defect, wearing a correct answer.
>
> **Closed by a run at 175 records per trigger — a limit the SHIPPED model REFUSES** (`boundary_for`
> at lag 2: *"narrowest late margin is -350000 ms"*), and which only the superseded one-batch lag
> accepts. That is what makes it the separating experiment rather than an ordinary one: the delay it
> ran at is the one-batch model's own. **There the three candidate lags predict three DIFFERENT
> removed-identity sets** — 100 / 97 / 95. **Measured: 97.** Lag 2 confirmed; lag 1
> refuted *at the configuration lag 1 itself derived*, so it was refuted on its own best ground; lag
> 3 refuted. The assertion is set equality element by element, and the predicted sets are built by
> the shipped model with the lag monkeypatched rather than written as literals, so changing the model
> changes what the run is compared against.
>
> `1 passed, 6 deselected in 752.10s`.

#### What this cost, for whoever budgets the CI job

**12 m 32 s** for the falsifier arm alone — corpus derivation, publishing 10,150 records, a Spark
session start and one 58-batch arm. The fixed costs dominate, so it is **not** a third of the file;
the full file runs three arms and costs more than the 16–20 min the two-arm version measured on this
Windows box.

### 2.5 T8 — THE MANAGED BROKER FROM A SERVERLESS JOB, published 2026-08-23 before any workspace run

**Written by the implementer of T8, before the job existed in the workspace.** The job is built, the
bundle validates and the run is the controller's; everything below can be wrong, and each clause says
how.

> **T8 IS NOT THE EXACTLY-ONCE PROOF.** That is §2.1/§2.2, it runs locally, and it stays local because
> it needs a fault injected between the data commit and the offset commit — a serverless task is the
> one place this project cannot kill a process on purpose. T8's sink is `format("delta")`, which is
> exactly-once **by construction** and would therefore report success under every outcome §2.2 exists
> to tell apart. Anyone reading this section as "the real proof, now on real infrastructure" has the
> phase backwards.

**What T8 claims instead, in one sentence:** this lakehouse ingests an event **stream** on the
platform it deploys to, closing the one honest gap in the four-sources claim — until now, event
streams reached the workspace only as **files**.

#### The state of the topic before the run — Reported, measured from this box 2026-08-23

The shipped code (`payment_stream` + `sasl_reader_options`, local Spark, the OSS login-module class)
read the managed cluster over SASL_SSL and drained the topic:

```
OPTIONS | kafka.sasl.jaas.config=<withheld: carries the SASL password>, kafka.sasl.mechanism=SCRAM-SHA-256, kafka.security.protocol=SASL_SSL
READ | OK | input_rows=1 batches=(0,)
LANDED | 1 rows | partitions [2]
  row | Row(kafka_partition=2, kafka_offset=0, transaction_id=None)
```

**So `opl-payments` holds exactly one record today** — §1.2's `opl-cloud-probe-…`, on partition 2 at
offset 0 — **and it is not a payment.** `transaction_id` is NULL because `from_json` yields a struct
of NULLs for a value it cannot parse, and `kafka_value` keeps its bytes. That is the column doing the
job `opl.streaming.ingest` says it is kept for.

**AND THAT SAME READ IS WHAT MEASURES `OSS_SCRAM_LOGIN_MODULE`, WHICH SHIPPED PROSE CALLED
UNMEASURED.** The same session, credential and broker, handed `DATABRICKS_SCRAM_LOGIN_MODULE`
instead, never reaches the broker at all:

```
Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule
```

So the two constants are a **measured pair** rather than one measurement and one label: the shaded
class is on serverless's classpath (§1.2, job run `382097683531247`) and is **not loadable here**;
the unshaded one is loadable here and read the row above. Two files in this change-set had said the
OSS constant was *"a constant no run has exercised"* — **while this section recorded the run that
necessarily used it**, because the shaded name cannot load off Databricks. Both were corrected by
measuring the second arm rather than by softening the first.

#### The predictions, in the form that can be wrong

1. **The launch refuses before a session starts** unless `minimum_rows` is a positive whole number.
   The YAML default is a sentinel; `require_minimum_rows` refuses it, and it refuses `0` as well —
   `0` is the value that would turn the floor into a switch for disabling itself.
2. **`input_rows` equals the number of records on the topic at launch**, which is `1` plus whatever
   the producer publishes beforehand. It is read from Spark's own progress, not from the sink.
3. **The landed table holds exactly `input_rows` rows**, and the probe record among them carries NULL
   contract columns beside a non-NULL `kafka_value`.
4. **A second attempt of the same task consumes 0 and FAILS** rather than reporting a second green
   run — the checkpoint is fixed, so a retry resumes a drained stream and the floor refuses. It also
   cannot double-write: the Delta sink commits the micro-batch id transactionally with the rows.
   `max_retries: 0` does not prevent a retry (24 `(job_run_id, task_key)` pairs have run twice on this
   workspace), so this is the arm that has to be safe, not the happy one.
5. ~~**No `[REDACTED]` appears anywhere in the task output**~~ — **narrowed before the run, because
   as written it could not be read.** **The task's own two `stream_managed_broker:` lines carry no
   `[REDACTED]`**, and the first renders `kafka.sasl.jaas.config` as
   `<withheld: carries the SASL password>`. §1.2 measured that a secret's value is scrubbed from every
   line; the USERNAME is no longer a secret, so those two lines are the control for that repair.
   **The BOOTSTRAP still is a secret, deliberately** — and the Kafka client logs `bootstrap.servers`
   at INFO by design — so a `[REDACTED]` elsewhere in the output is that secret being scrubbed as
   intended and does **not** falsify this. The original clause would have been falsified by the
   platform working correctly, which is a prediction that cannot distinguish its two outcomes.

#### What falsifies each, and none of these is a platform failure

- ~~**A `ClassNotFoundException` naming `kafkashaded.…ScramLoginModule`**~~ →
  **A `javax.security.auth.login.LoginException: No LoginModule found for
  kafkashaded.…ScramLoginModule`** falsifies the one constant no local run can check. That is the
  exact string local OSS Spark produces for that name — measured above — so the failure *mode* is
  known and only the platform it appears on is in question. The struck spelling is not what either
  runtime emits. **Two revisions of this bullet got the house form wrong in opposite directions** —
  the first struck the withdrawal instead of the withdrawn claim, the second named that error and
  then applied no strikethrough at all. §1.2's probe used that spelling on serverless and read a row, so it is measured
  — but by a **batch** read (`spark.read`), and T8 is the first **streaming** read of Kafka on this
  compute. If the shaded name were wrong, nothing local would ever have said so.
- **`input_rows` larger than the topic held** means the checkpoint was not the one this run thought it
  was — the same shape as the month-scoped Auto Loader hazard, reached through a different door.
- **`input_rows` of 1 after a publish** means the producer did not reach this broker, not that the read
  is short: the two brokers share the topic name `opl-payments` and differ only in the bootstrap.
- **A green second run** falsifies prediction 4 and means the floor is not where this document says.
- **A metadata timeout** is one string across four worlds (expired trial, revoked ACL, wrong username,
  no route) — ADR 0018's species, and the reason the trial's expiry date is written down rather than
  left to be inferred from an error.

#### THE FIRST RUN FAILED, AFTER LANDING EVERYTHING — AND THE PLATFORM PROVED PREDICTION 4 BY ITSELF

**Controller-verified 2026-08-23.** Job run **`570309961086740`**, revision
`c19ea0b5a86f91a57fe9a38ae62c1bbb448cdba2` — the deploy verified by artefact first: the wheel
downloaded back out of the workspace hashed **`a3f1cf4e8463cf9cbd40444152cfaeddf53a3282b0a1de5c43a78380358608b9`**
on both sides, and `opl/_revision.py` **inside** it carried that same revision, equal to `git HEAD`
over a clean tree.

`assert_deployed_revision` SUCCESS. `read_managed_broker` **FAILED** — and the table it wrote holds:

| | |
|---|---|
| rows landed | **10,151** (10,150 corpus + §1.2's probe record) |
| distinct `(kafka_partition, kafka_offset)` | **10,151** |
| distinct `transaction_id` | **10,000** |

**So the read worked and the failure is downstream of it.** The 10,000 is the 150 redeliveries
arriving intact — 10,150 deliveries carrying 10,000 distinct ids — with the probe record's NULL id
excluded by `COUNT(DISTINCT)`, which is the one place that operator's NULL-dropping is wanted.

> ~~**AND THE RETRY THIS PROJECT KEEPS MEASURING RAN AGAIN, WHICH TESTED PREDICTION 4 FOR REAL.** If
> the sink were not idempotent the second attempt would have re-consumed offsets the first had
> committed and the table would hold more than 10,151 rows. It holds exactly 10,151, over 10,151
> distinct coordinates. The Delta sink's transactional batch-id commit held under a retry nobody
> staged — which is better evidence than the test that would have staged one.~~
>
> **WITHDRAWN, AND IT IS THIS DOCUMENT'S WORST DEFECT: A SAFETY CLAIM THAT COULD NOT HAVE COME OUT
> OTHERWISE.** The retry is real — `max_retries: 0` did not prevent it, one `task_key` and **two
> `task_run_id`s**, `533379837633364` (84 s) and `740868890853109` (**32 s**). But the second
> attempt's log holds **one line, the launch header**, and it died on the same `CONFIG_NOT_AVAILABLE`
> read. **It consumed zero records.**
>
> **A retry that consumes nothing appends nothing, whatever the sink is.** `10,151 / 10,151` is the
> outcome under both hypotheses, so it cannot distinguish *the guarantee held* from *the guarantee was
> never asked* — ADR 0018's species, published in this phase's own closing document, about a phase
> whose subject is that species.
>
> **And the mechanism inverts the sentence.** Offsets the first attempt *committed* are never
> re-consumed; that is what a committed offset means. Sink idempotency is exercised only when a batch
> is **re-planned**, which requires the checkpoint to be *behind* the data write — the opposite of what
> happened. The shipped task's own header keeps the two apart correctly
> (`databricks/src/stream_managed_broker.py`: *"a second attempt … consumes ZERO records, and
> `write_payment_stream`'s floor REFUSES"*); this document fused them.
>
> **Commit `2fa610c`'s message carries the withdrawn claim too, and is left standing rather than
> reworded** — this repository retracts in place rather than editing the record to look better.
> Found by the closing documentation review.

**What failed, and it is a capability difference nobody had measured:**

```
[CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION] Configuration
spark.sql.streaming.numRecentProgressUpdates is not available.  SQLSTATE: 42K0I
  at com.databricks.sql.connect.SparkConnectConfig$.assertConfigAllowedForRead
  at ...SparkConnectConfigHandler.handleGetWithDefault
```

**Serverless refuses to READ that config**, and the frame names `handleGetWithDefault` — so passing a
default does not help, because the default is applied by the server *after* a read it declines to
perform. That read is `ingest._progress_of`'s ring-buffer cap guard, **built and tested against a
local session where the key resolves to `100`.** The reasoning that rests on it — the runnable-rate-
limit floor of 104 and the trailing-progress arithmetic behind it — lives in
`watermarked_dedup.boundary_for`'s docstring and in `tests/test_streaming_watermarked_dedup.py`,
**not in this document**, and every bit of it was measured on the one compute where the key is
readable. An earlier version of this paragraph cited ~~"§2.3's F9 line of reasoning"~~. §2.3 carries
neither figure, and `F9` is a finding id from the phase plan — which is git-ignored, which this
document's own preamble refuses to point a public reader at, and which therefore cannot be a
citation. Caught by the agent that wrote this phase's ADR and unexercised ledger.

**The repair does not guess the cap.** A fallback of 100 would make a guard that cannot tell
*verified* from *could not look* — this phase's seventh instance of that shape. Instead truncation is
ruled out by a **second measurement**: the ring evicts oldest-first, so a buffer whose oldest retained
update is batch 0 has evicted nothing, whatever the cap is. Where neither the cap nor that evidence is
available, the run prints that the count is a **LOWER BOUND** and says truncation is unruled-out. A
test sweeps all four readings and asserts neither state can borrow the other's vocabulary.

**Three things the failed run established that no local test could:**

1. **`dbutils.secrets.get` works from a `spark_python_task`** — the implementer's highest-risk
   untested line. §1.2's probe was a *notebook*, where `dbutils` is injected; this is not.
2. **The withhold holds in production.** The task's own line printed
   `kafka.sasl.jaas.config=<withheld: carries the SASL password>`.
3. **`spark.readStream.format("kafka")` runs on serverless.** §1.2 had measured only a *batch* read.

#### THE RECORDED RUN — job run `336384048296782`, SUCCESS

**Controller-verified 2026-08-24**, revision `2fa610c2c5501dc8765287022ceebd011f0e509b`, deploy
verified by artefact (wheel sha256 `3b5e2c1601a15408e6b3f215e151027f8a318d73de9a53692d2492d274a893ca`
on both sides; `opl/_revision.py` inside it equal to `git HEAD` over a clean tree). Reset first — the
checkpoint directory removed and the sink dropped, the repair the task's own header documents.

| | |
|---|---|
| rows landed | **10,151** |
| distinct `(kafka_partition, kafka_offset)` | **10,151** |
| distinct `transaction_id` | **10,000** |

**Where the 10,150 came from, because the input to this phase's flagship result had no provenance
line until the closing review asked for one. Controller-verified 2026-08-24**, from this box, before
the run: `publish_records` over `delivered_records(PROFILES["promotable"].stream_spec(pool), …)` with
the synthetic pool, to topic `opl-payments` on the managed cluster, reported
`message_count 10150`, `byte_count 2,969,937` and partition counts `((0, 3290), (1, 3410),
(2, 3450))` — 3,290 + 3,410 + 3,450 = 10,150. The byte count is `promotable`'s, the same figure the
profile sweep measures locally. **Plus §1.2's one probe record already on the topic gives 10,151**,
which is the floor the run was launched with and the row count it landed.

**The task's own output, quoted because the sentence is the deliverable and not the number:**

```
consumed 10151 records from 'opl-payments' across batches (0,) into
workspace.default.streaming_payments_managed_broker. progress ring: 1 updates;
spark.sql.streaming.numRecentProgressUpdates could not be read here (AnalysisException:
[CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION] ... SQLSTATE: 42K0I), so no cap was compared
against -- but the oldest retained update is batch 0, the first this query ran, and the
ring evicts oldest-first, so nothing was evicted and the count above is the run's whole
total. That is a count of records READ from the managed broker on serverless compute --
it is not, and must not be quoted as, evidence about exactly-once processing.
```

**Two properties of that sentence are the point of T8's repair.** It names the refusal it could not
get past, with the SQLSTATE, instead of printing a number that reads as verified — and it then rules
truncation out **by a different measurement** rather than by assuming the cap. A reader can tell
*"I checked the cap"* from *"I could not, and here is why I still know"*. That is the distinction
ADR 0018 says a check must be able to make.

**And it refuses its own most likely misreading in its last clause.** 10,151 records read from a
managed broker on serverless compute is not evidence about exactly-once. That proof is §2.2's, it is
local, and it is local because it needs a fault injected between the data commit and the offset
commit. The job carries the refusal in its header and in its output, not only in this document.

**The predictions of §2.5, marked:** 1 CONFIRMED (the sentinel refuses before a session starts) ·
2 CONFIRMED (`input_rows` 10,151 = 1 probe + 10,150 published, read from Spark's own progress) ·
3 CONFIRMED (10,151 landed; 10,000 distinct ids is the 150 redeliveries intact with the probe's NULL
excluded) · 4 **HALF confirmed, and the half is named** — see below ·
5 CONFIRMED as narrowed (the task's two lines carry no `[REDACTED]` and the JAAS value is withheld).

> **PREDICTION 4 HAS TWO CLAUSES AND ONLY ONE OF THEM WAS TESTED.** It predicted that a second
> attempt *"consumes 0 and FAILS"* at the row floor **and** that it *"cannot double-write"*.
>
> **The second is established**: two attempts, 10,151 rows over 10,151 distinct coordinates.
> **The first is NOT.** Controller-verified 2026-08-24 by reading the retry's own output — task run
> `740868890853109` failed on the **same** `CONFIG_NOT_AVAILABLE` read as the first attempt, which in
> the pre-fix code sits *before* `_refuse_a_run_that_processed_nothing`. The retry never reached the
> floor, so the floor's behaviour on a drained checkpoint remains **unexercised on serverless** and is
> listed as such in §3.
>
> This was marked CONFIRMED in an earlier revision on the strength of the half that held — the
> arithmetic was right and the conclusion drawn from it was wider than it. Caught by the agent that wrote this phase's ADR and unexercised ledger,
> settled by reading the second attempt's log rather than by reasoning about it.

#### What `describe_reader_options` does not cover, so it is not read as more than it is

**Reported** by T8's independent reviewer, measured by them rather than by the controller: the JAAS
option value is carried in the DataFrame's **logical plan**, so it is recoverable from an `explain()`
on the frame before the query starts — `Parsed`, `Analyzed` and `Optimized` all carry it; the
physical plan and a failed query's traceback did not. Nothing this task runs calls `explain()`, and
the withheld-value line is a statement about what the task **prints**, not about what the plan holds.

**What covers the plan is the platform, not this repository.** Databricks replaces every occurrence
of a secret's value in task output — which is exactly why the password, and only the password, is in
the scope. Spark's own `spark.redaction.regex` keys on `secret|password|token` and
`kafka.sasl.jaas.config` matches none of them, so it is not a second defence.

#### The broker is temporary, and the job says so in its own header

The trial's credits expire **~2026-09-03** and the cluster then stops answering.
`databricks/resources/streaming_managed_broker_job.yml` is therefore a **recorded run, not a job a
future reader can run green**, and the table it lands —
`workspace.default.streaming_payments_managed_broker` — is deliberately **not** registered in
`opl.bronze.REGISTRY`: F4's `dataops_reconciliation` and `dataops_freshness` are total over that
registry, so registering it would leave a permanent stale row in a freshness view for a source nobody
can refresh. **When the broker goes away, say so here**, or the next reader re-runs the job, gets a
connection failure, and reads a dead trial account as a regression in this repository.

---

## 3. What is still unexercised

**Protocol §9 condition 6.** A path that ran zero rows through it is not a path that works, and
this list is what stops the phase from being read as more exercised than it is. Each entry says
what would exercise it.

**Two entries below are unusual for this ledger and are marked as such.** One is a whole CI job
that has never executed once, and one is a comparison this phase deliberately refused to resolve.
Neither is a gap that was discovered at the close: both were decided during the phase, and they are
here because the reason they are absent from §1 and §2 is that nothing ran them.

### Written and never run

- **The CI `redpanda` job.** `.github/workflows/ci.yml` gained a `redpanda` job that starts the
  committed compose service and runs `uv run pytest -m redpanda`. **It has never executed.** That
  workflow triggers on pushes to `main` and on pull requests; this branch has had neither, so
  **the first PR run is this job's first execution** — of the job, of the compose service's
  healthcheck, of the Ivy cache key, and of the timeout. *What would exercise it: opening the PR.*
  Nothing in §1 or §2 is a result of this job, and no claim in this document depends on one.
- **The Windows session-start race, on `ubuntu-latest`.** §2.2 measured a local-Spark session
  failing to start across five attempts and reported that as five samples rather than as a rate;
  the two configuration "fixes" were falsified by the same probe. **That is a Windows-local fact
  and this document does not project it onto Linux** — the signature is not documented there.
  *What would exercise it: the same first PR run, and then repeated runs, since one green run is
  not a rate either.*
- **The job's own time budget on Linux.** The ceiling in `ci.yml` is set from the Windows
  measurements §2.4 publishes — **12 m 32 s** for the falsifier arm alone, and a full file of three
  arms costing more than the 16–20 min the two-arm version took on this box. Linux is *expected* to
  be faster and that expectation is not evidence. *What would exercise it: the first green run,
  whose duration replaces the estimate.*

### Branches this phase built that have never taken their other arm

| what | why it has not fired | what would fire it |
|---|---|---|
| **`_progress_of`'s truncation refusal** (`len(progresses) >= cap`) | it has never fired in a shipped run — §2.5's serverless run reports `progress ring: 1 updates`, and `_progress_of`'s own docstring records the local runs at 1 and 3 consuming batches against a cap of 100 | a run with as many progress updates as the cap. Over `promotable` that floor is **104 records a trigger** — the arithmetic is `watermarked_dedup.boundary_for`'s docstring and `tests/test_streaming_watermarked_dedup.py`'s, and **this document has never carried it** |
| **`RingBufferReading`'s fourth state** — unreadable cap over a RESUMED checkpoint, where the count is printed as a **LOWER BOUND** | both serverless runs in §2.5 are fresh checkpoints whose oldest retained update is batch 0, so the second argument always applied | a serverless run resumed against a checkpoint that had already committed a batch and still consumed something — i.e. T8's job re-pointed at a topic carrying new records **without** the reset §2.5 performed |
| **`_progress_of`'s trailing-progress arithmetic on the STATELESS path** | the extra non-consuming batch was measured on the **stateful** chain only (the falsifier arm's checkpoint: `commits` 0..58 with `offsets/57` and `offsets/58` both ending at `{"0":10150}`). `write_payment_stream` is stateless and has no state eviction. ~~No run has measured whether it produces a trailing progress update at all.~~ **§2.5's recorded run measured exactly that** — `progress ring: 1 updates` over `batches (0,)`, so one consuming batch produced one update and no trailing one. What is unmeasured is its behaviour NEAR THE CAP, which is what the third column asks for. (An earlier revision of this cell named `land_stream`, a function that exists nowhere in this repository) | a stateless `availableNow` run through `write_payment_stream` at a rate limit fine enough to approach the cap, with the progress list read back |
| **the drift column through the Kafka transport** | every stream this phase published carries redeliveries and late arrivals and **no drift**: no `DefectSpec` under `tests/integration/` or `src/opl/streaming/` sets `drift_from_index` (checked by `grep`). `ingest`'s docstring argues that `from_json` drops the undeclared key while `kafka_value` keeps the bytes — **an argument, with no run behind it** | publishing a `drifting` profile to a topic and rebuilding the landed `kafka_value` bytes against `b45f1dc7…`, which is the route `tests/integration/test_payment_stream_ingest.py` already takes for an undrifted corpus |
| **a fault across MORE THAN ONE partition** | §2.2's proof ran over **one partition** at `maxOffsetsPerTrigger=10`, chosen so a micro-batch is a countable unit. The managed topic has 3 partitions and §2.5 never faulted | the same two arms over a multi-partition topic, where a replayed batch spans partitions and the offset log carries more than one end |
| **`read_kafka` from the SQL warehouse** | §0.2 established that the function **exists** and nothing else. Every Kafka read in §1 and §2 goes through Spark | a statement reading the managed broker through it — which §0.3 is the reason nobody attempted casually |

### Not exercised by choice, and the choice is recorded

- **The `<=` in the late-data model, and it is UNDECIDED rather than assumed.** `WatermarkMargins`
  drops a row when the delay is `<=` its margin. Measured over `promotable`, **no delivered
  record's margin equals either arm's delay** — at 133 or at 175 records a trigger, under a lag of
  1, 2 or 3 alike — so a model written with `<` would have named the same sets, **including the
  falsifier run's 97**. The two guards are strict in the direction that refuses: the keeping delay
  must strictly *exceed* the widest late margin, and the dropping delay must be strictly *above*
  the widest punctual one, so the one case where the two readings disagree is **refused rather
  than resolved**. The module's docstring no longer attributes the comparison to Spark; that
  attribution was a reading of Spark's source, and the runs beside it were not a test of it.
  *What would exercise it: a corpus, or a rate limit, that puts a delivered record's margin exactly
  on an arm's delay — at which point the guard refuses and the comparison has to be settled from
  Spark's source or from a run built for it.*
- **The ring-buffer cap on serverless.** The platform refuses the read (§2.5), and this repository
  refused to substitute the value it would have returned. Both refusals are deliberate, and
  together they mean **no serverless run has ever compared retained updates against a cap**;
  truncation is ruled out there by the oldest-retained-batch argument instead. *What would exercise
  it: a Databricks compute that permits the read — the cap arm is unchanged and takes precedence
  wherever it can run.*
- **The exactly-once proof on the deploy target.** Refused with a reason, in
  [ADR 0019](adr/0019-the-proof-runs-where-a-process-can-be-killed.md) Decision 2 and in the job's
  own header: it needs a process killed between the data commit and the offset commit. *What would
  exercise it: a way to terminate a task's process at a chosen instant on that compute. Until then,
  moving it there costs it its falsifier.*
- **No latency, no throughput, anywhere in this phase.** §1.2 measured the same serverless read at
  **19,606 ms** and then **2,730 ms** over identical data, so nothing here is quoted as a
  performance figure. *What would produce one: a warmed session and repeated trials under F4 Task
  6's protocol, which nothing in F5 ran.*

### Measured on one compute and not the other

- **The two SCRAM login-module spellings are a measured pair with two untried corners.** §2.5
  measures the shaded name working on serverless and **failing to load locally**, and the OSS name
  working locally. **Nobody has tried the OSS name on serverless**, and nobody has tried the shaded
  name anywhere other than serverless. The constants are selected by environment rather than probed,
  which is correct and is not the same as covered.
- **`foreachBatch` on serverless.** §2.5 established that `spark.readStream.format("kafka")` runs
  there and that a `format("delta")` sink commits transactionally under a retry. The
  `foreachBatch` sink both exactly-once arms are written against has **only ever run locally**.
- **`dbutils.secrets.get` from a `spark_python_task`** now has run (§2.5). Its *failure* mode — a
  missing scope, a revoked key — has not, on either compute.

### Carried out of the phase as follow-ups, not fixed

- **`src/opl/streaming/__init__.py` describes the CI job that was not built.** Its docstring says
  the byte-identity test runs in CI's default invocation *"before T6 gives CI a Redpanda service
  container"*. The shipped job uses `docker compose` against the committed compose file, and
  `ci.yml`'s own comment records at length why a `services:` block was rejected — it would be a
  **second spelling** of the broker's launch arguments. The prose is stale in the one direction
  that matters (it names the rejected design), and it is left rather than patched because the
  correction belongs with whoever next touches that module.
- ~~**Two files sit within single digits of the 800-line cap**, measured with `wc -l`:
  `src/opl/streaming/watermarked_dedup.py` at **795** and
  `tests/test_streaming_watermarked_dedup.py` at **793**.~~ **FALSIFIED BY THIS PHASE'S OWN CLOSING
  COMMIT, and corrected from F6.** `2d077a8` — the commit that carries this very bullet — split
  both files and created `src/opl/streaming/lateness.py`. **Measured 2026-08-28: 459 and 292.**
  The cap is strict (800 is over) and the rule stands; **on `feat/f6-rca-agent` it applies to
  three** — `tests/vault/test_socios_vault.py` (799), `src/opl/gold/facts.py` (799) and
  `databricks/src/triage_llm_control.py` (792), the last created by F6.
  `docs/f6-run-evidence.md` §0.7 carries the trace, and the reason this stood for five days is
  F5's own closing lesson arriving on schedule: **the defect had moved out of the code and into
  the document that judges it.**
- **`describe_reader_options` does not cover the logical plan**, reported by T8's independent
  reviewer and recorded in §2.5. Nothing this task runs calls `explain()`, so **the exposure has
  never occurred in a run**; what covers it is the platform's scrubbing of a secret's value, not
  this repository. *What would exercise it: any code that calls `explain()` on the reader frame
  before the query starts.*
- **The orphan topic left by a fixture that dies during SETUP.** §2.2 lists it as unclosed: the
  teardown hardening waits for the broker to confirm deletion, but a session that raises during
  setup never reaches the finaliser. Harmless today because topic names carry a uuid — which is
  also why no run has ever demonstrated the hazard the conftest's own docstring names.

### Unmeasurable here, and stated as such

- **How much of the trial credit is left.** `/v1/billing`, `/v1/usage`, `/v1/subscriptions` and
  `/v1/organizations` return the control plane's own `NOT_FOUND` for this service account, and the
  cluster's Prometheus endpoint returns **401** under both credentials. **No figure is published.**
  What does not depend on it: the corpus is 40,150 records at ~293 B, ≈ **12 MB per full replay**,
  and the producer counts what it sends.
- **What the broker does after ~2026-09-03.** The cluster stops answering and T8's job then fails
  at the metadata fetch — one string that covers an expired trial, a revoked ACL, a wrong username
  and no route alike. **This document must say the broker is gone once it is.** Until someone
  writes that line, a reader after that date cannot tell a dead trial account from a regression in
  this repository, and the run ids and row counts above are the only thing that dates the evidence.
