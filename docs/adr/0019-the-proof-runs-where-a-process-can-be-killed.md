# ADR 0019 — The exactly-once proof runs where a process can be killed; the broker is a recorded run

## Status

**Accepted**, F5, 2026-08-19/24. The phase opened on a premise nobody had ever tested — one
parenthetical in the master design spec — and **Task 0 falsified it before this phase's plan
existed**. Every decision below was taken after that result, which is why none of them is the
architecture the spec describes.

This ADR records the decisions no existing note owns. The revision guard the T8 job carries stays
in [ADR 0009](0009-deployed-revision-provenance.md); keeping the Kafka connector off the shared
Spark session is an application of [ADR 0004](0004-pyspark-optional-extra.md) rather than a new
ruling; and Decision 1's result touches [ADR 0002](0002-two-layer-topology.md)'s **Context**
without disturbing its Decision.

> **The numbers are in `docs/f5-run-evidence.md`**, with controller-verified separated from
> reported. This document carries the decisions; where a figure appears here it is quoted from
> there.

---

## Context

The master design spec declares F5's topology and then states a limit in parentheses:
*"gerador → Kafka (Redpanda em Docker) local; Auto Loader file-streaming no Databricks
(**limitação honesta: Free não conecta Kafka externo**)"*. That sentence had already decided the
phase's shape — the platform would read **files**, and Kafka would be a thing that happened on a
laptop.

**It had never been measured, and it is false.** What follows is a phase whose plan was written
after its own premise had been withdrawn: the platform reads the stream itself, the proof that
matters does not run there, and the two facts have nothing to do with each other.

Two further facts shaped everything below. **The declared Redpanda container had never once been
started on this box** since F0, and `tests/integration/test_redpanda.py` had existed all along,
deselected by `addopts` and never run in CI — so the phase's starting position was a declared
capability with no execution behind it, which is the species
[ADR 0018](0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md) was written about.
And **F1b's payment corpus already existed**, deterministic, with duplicates, late arrivals and
drift injected on purpose and its digests pinned over the result — so the question was never
"what stream", only "which transport".

---

## Decision 1 — the spec's parenthetical is RETRACTED; the blocker was the NAT, not the platform

`docs/f5-run-evidence.md` §0.2 is a socket matrix run from a Free Edition serverless job. Egress
exists, it is not 443-only, and **`portquiz.net:9092` CONNECTED** — egress on the Kafka port
itself, against a control on 443 proving the host was up. `read_kafka` exists on the SQL
warehouse and `spark.readStream.format("kafka")` builds a plan on jobs compute. §1.2 then closed
it end to end: SASL_SSL, SCRAM-SHA-256, and a row crossing from a laptop to the workspace.

**What actually blocks the Docker container is that nothing forwards 9092 to a machine behind a
residential router** — the dev box's own address timed out, RFC1918 timed out as a negative
control, and `127.0.0.1:9092` returned `ConnectionRefusedError`, which is what makes the two
timeouts a real absence of route rather than an instrument that only knows one word.

### Rejected: leaving the sentence standing with a note

A limitation nobody tested is not an honest limitation; it is an untested claim wearing the word
*honest*. The retraction is published at the top of the evidence document's §0 rather than left to
be inferred from the fact that a later section contradicts it.

### Rejected: the first probe this phase wrote

`read_kafka` from the SQL warehouse returned **the same `TimeoutException` string** for a host
where nothing listens and for `github.com:443`, which certainly accepts TCP. No egress, wrong
protocol and nothing-listening all produced one output. It cost two statements, and it is why §0.2
is a matrix carrying two negative controls instead of a single call.

**What reverses it:** a measurement, on the compute of the day, that a Kafka client cannot reach an
external broker. The claim is falsifiable now in a way it was not when it was written, and that is
the whole of what changed.

> **This is also, for one port, the probe [ADR 0002](0002-two-layer-topology.md) said it had none
> for.** That ADR argues — explicitly *argued, not measured* — that egress out of Databricks
> creates no route into a machine behind a home NAT. §0.2 measures exactly that inbound direction,
> with a control, and returns the answer ADR 0002 argued. **It does not convert ADR 0002's
> paragraph into a measured one**: that paragraph is about Postgres on another port, and one port's
> timeout is not a statement about a host. It is recorded here because a reader holding this result
> must not be able to take it for the Postgres measurement — the same discipline ADR 0002's own
> amendment applies to F-API's egress result.

---

## Decision 2 — the exactly-once proof runs LOCAL, and it stays local

**Not because the platform refuses.** Decision 1 measured that it does not, and §2.5 has a
serverless job reading 10,151 records off a managed broker through `spark.readStream`. The proof
stays local because of the **fault**: it needs a process killed between the batch's data commit and
the streaming checkpoint's offset commit, and **a serverless task is the one place this project
cannot kill a process on purpose**.

The experiment is two arms over one fault and the same offsets — a `foreachBatch` sink appending
with no idempotency key (**NAIVE**) and the same sink appending under Delta's
`txnAppId`/`txnVersion` (**GUARDED**). Measured: NAIVE landed **39** rows over 29 deliveries, an
excess of **10**; GUARDED landed **29**, an excess of **0**. The duplicated rows are offsets
**10–19** — the killed batch — each landed exactly twice.

**The window was located from the filesystem rather than from the code.** At the instant between
the fault and the restart the Delta log carried batch 1's data write while the checkpoint carried
`offsets/1` with **no `commits/1`**: the data was durable and the offsets were not, identically in
both arms. The GUARDED arm's mechanism is visible in the same place — its Delta log carries `txn`
actions, and the replay of batch 1 produced no commit at all.

### Rejected: proving it with `writeStream.format("delta")`

Which is what T8's job uses, and which is **exactly-once by construction**. An experiment on it
reports success under every outcome the proof exists to tell apart — **including the outcome where
nothing ran**. That is ADR 0018's species, and it would have been this phase's flagship result. The
seam between `opl.streaming.ingest` and `opl.streaming.exactly_once` is drawn on exactly this line:
the sink that is correct by construction cannot prove anything about the sink where the guarantee
has to be earned.

### Rejected: a single arm

**A GUARDED zero on its own is worth nothing**, because a zero produced by a fault that never
reached the window is the same zero. The NAIVE arm exists to be falsified, and both failure
directions were shown reachable before either number was published: giving NAIVE the idempotency
key leaves it at 0 duplicates and the suite **fires a negative control by name** rather than
letting a zero pass as a pass; removing the key from GUARDED lands 39/10 and turns the proof red.

**What reverses it:** a way to terminate a task's process at a chosen instant on the deploy target.
Until that exists, moving this proof onto the platform costs it its falsifier, and a proof without
a falsifier is a demo. The evidence document, the job's YAML header and the job's own stdout each
carry that refusal, because it is the misreading this phase is most likely to attract.

---

## Decision 3 — ONE corpus, TWO transports

`src/opl/streaming` publishes a declared profile's records to a Kafka topic **through the
serialiser that already exists**: `b"".join(message_values(records))` equals
`generated_landing.serialised_bytes(records)` byte for byte, with the newline terminator carried
inside each message value. So every count F1b pinned against the file path still describes the
stream. `json` is imported nowhere under the package.

### Rejected: a second corpus, and a second serialiser

A generator producing its own records for Kafka would leave F1b's digests describing one path and
nothing describing the other, and the phase's headline counts would be two measurements that merely
agreed in shape. A `json.dumps` under this package would re-decide `separators`, `ensure_ascii` and
the terminator — byte-identity decisions taken once in `opl.generator.events.to_jsonl` — through
the one door F1b did not lock.

**And the guard against that had to be replaced, because the first one was near-circular.**
Measured against the real serialiser: terminator dropped entirely → **ACCEPTED**; default `json`
separators → **ACCEPTED**; `sort_keys=True` → **ACCEPTED**. Its docstring claimed three defects and
caught one. What actually catches a change to the shared serialiser is the **external** digest
`b45f1dc7…`, and the prose now says so instead of claiming an internal rebuild does it.

**What reverses it:** a transport whose payload cannot be the file path's bytes — Avro or Protobuf
behind a schema registry. Then the second serialiser is the deliverable rather than the defect, and
the digest moves inside it rather than being asserted across it.

---

## Decision 4 — the processing identity is the DELIVERY COORDINATE, not the payment id

Everything the exactly-once proof counts is taken over `(kafka_partition, kafka_offset)`.

**This is measured, not argued.** `transaction_id` reads **24 in both arms** — the identity column
reports the same number whether the pipeline is exactly-once or not, because the corpus carries
deliberate redeliveries. A proof counted over it would have shown the two arms identical and
concluded the opposite of the truth.

The two claims this keeps apart are the ones the phase most needed kept apart, and a published
prediction blurred them anyway: the 150 redeliveries are the **producer** delivering one id twice —
a property of the DATA, already measured in bronze at 40,150 rows over 40,000 distinct.
Exactly-once is a property of **PROCESSING**.

### Rejected: `COUNT(DISTINCT …)` as the operator

`.select(...).distinct().count()` is used instead, with a NULL refusal in front of the measure.
This repository lost 8,761 rows once to `COUNT(DISTINCT …)` dropping NULL-bearing rows. The one
place that drop is *wanted* is T8's landed table, where the probe record's NULL id is excluded on
purpose — and that is stated where it happens rather than left standing as an inconsistency.

**What reverses it:** a source with no per-delivery coordinate. The identity would then have to be
carried in the payload, and it is the instrument that would need rebuilding, not the guarantee.

---

## Decision 5 — the dedup key is the identity ALONE, and one watermark arm is not a measurement

The shipped chain is `withWatermark → dropDuplicatesWithinWatermark` keyed on `transaction_id`, and
shipped code **refuses any key touching the business attributes**. Measured on the arm where
nothing was dropped: 10,150 delivered → **10,000** landed, **exactly 150** redeliveries collapsed,
**9,200** distinct business tuples, so **the 800 legitimate repeats survived**.

### Rejected: the business tuple as the key

A repeat is a customer paying the same supplier the same amount twice — same payer, payee, amount,
currency and method, its own `transaction_id`, and ordinary business. Measured with that key:
**526 collapse** — the 150 redeliveries plus **376** ordinary payments destroyed — leaving **424**
of the 800 standing.

**The published tripwire for this named 950 and would therefore have stayed silent**, because the
shipped operator is *windowed*: the damage is an artefact of the batching and lands on no round
number at all. What closes the trap is the shipped assertion `surviving_repeats == 800`, which the
implementer wrote — not the arithmetic the evidence document published beside it. **A falsifier
that names the wrong number is worse than none, because it reads as coverage.**

### Rejected: one run at one watermark

A single run that drops nothing is indistinguishable from a watermark that was never consulted — a
stateless sink, a single micro-batch, or a threshold wide enough to admit everything all produce
it. **The product is the DIFFERENCE between two arms over one corpus:** 9,900 against 10,000,
difference **100**, which is `promotable`'s declared `late_count` read from the declaration rather
than from a literal. Confirmed again at a rate limit nobody had tuned, where the narrowest late
margin is **8.75×** tighter.

**What reverses it:** a contract in which a repeated business tuple is itself a defect. The key
would move, and the 800 would become something to catch rather than something to protect.

---

## Decision 6 — the managed broker is a RECORDED RUN, not a standing service

The topic, the SASL user and four ACLs were created on a Redpanda Serverless **trial**. A
serverless job read the corpus off it and landed **10,151** rows over **10,151** distinct
coordinates — job run `336384048296782`, SUCCESS, deploy verified by artefact.

**The credits expire ~2026-09-03 and the cluster then stops answering.**
`databricks/resources/streaming_managed_broker_job.yml` is therefore a job a future reader
**cannot run green**, and it says so in its own header. After that date its failure is a dead trial
account, not a regression in this repository.

### Rejected: registering the landed table in `opl.bronze.REGISTRY`

`workspace.default.streaming_payments_managed_broker` is deliberately outside it. **F4's
`dataops_reconciliation` and `dataops_freshness` are TOTAL over that registry** — every registered
table takes a row — so registering this one leaves a permanent stale row in a freshness view for a
source nobody can ever refresh. That is a standing false alert on the one dashboard whose value is
that a red cell means something: the defect ADR 0018's Decision 2 built the `paused_by_decision`
label to avoid, arriving from the other side.

### Rejected: making anything else depend on the broker

Nothing in `databricks/resources/` may be left pointing at it, and nothing else may be made to.
**The obligation this creates is documentary, and it is stated so that it can be discharged:** when
the broker goes away, `docs/f5-run-evidence.md` must say so — otherwise the next reader re-runs the
job, reads a metadata timeout, and cannot tell an expired trial from a revoked ACL, a wrong
username or no route. One string covers four worlds, which is why the expiry date is written down
rather than left to be inferred from an error.

**What reverses it:** a broker with a lifetime longer than the reader's — a paid or self-hosted
cluster this repository can point at. The table then joins the registry, and it joins it **with a
declared cadence**, because ADR 0018's second decision refuses a freshness metric without one.

---

## Decision 7 — only the PASSWORD is a secret; a host and a username are coordinates

**Measured, with a control.** The first run put the Kafka username in the Databricks secret scope
beside the password, and the task's output read `[REDACTED]-cloud-probe-c84ef1c5` for a record
whose value was `opl-cloud-probe-c84ef1c5`. Databricks replaces every occurrence of a **secret's
value** in task output, and the username was `opl` — the prefix of this repository's catalog, its
topic, its job names and its package. Deleting `kafka_user` from the scope, changing nothing else,
returned the unredacted row: job run `838423822976396`.

### Rejected: the username in the scope, and the explanation published without the control

Making a short common string a secret turns `[REDACTED]` into the published answer for table, job
and topic names — **a silent substitution that preserves every other number, in a document whose
whole purpose is quoting run output**. A reader cannot tell a redaction from a value. And the
mechanism was not published until the second arm had run, because an explanation asserted without
testing it is what F4 had to retract.

**What this repository does NOT claim is that it hides the credential.** The JAAS option value is
carried in the DataFrame's **logical plan** and is recoverable from an `explain()` on the frame —
`Parsed`, `Analyzed` and `Optimized` all carry it. Nothing this task runs calls `explain()`, and
the withheld-value line is a statement about what the task **prints**. **What covers the plan is
the platform, not this repository**, which is exactly why the password, and only the password, is
in the scope. Spark's own `spark.redaction.regex` keys on `secret|password|token` and
`kafka.sasl.jaas.config` matches none of them, so it is not a second defence.

**What reverses it:** a deployment where the bootstrap host or the username is itself sensitive — a
shared broker whose tenant is inferable from either. The bootstrap **is** a secret here for that
reason, and the cost of that is visible rather than hidden: a `[REDACTED]` elsewhere in the task
log is that scrubbing working as intended, and the evidence document says so before quoting the
log.

---

## Decision 8 — where the instrument cannot be READ, the run says so rather than assuming its value

`ingest._progress_of` sizes a truncation refusal off `spark.sql.streaming.numRecentProgressUpdates`,
because `recentProgress` is a ring buffer and a total summed over an overflowed one is an
undercount. **Serverless refuses to read that config at all** —
`[CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION]`, SQLSTATE `42K0I`, out of
`SparkConnectConfig$.assertConfigAllowedForRead` — and it refused **after** the run had already
landed 10,151 rows.

### Rejected: a fallback of 100

Which is the value every session this project has measured returns, and which would have made the
guard report *"the ring did not truncate"* on a session that never said how big the ring was —
output identical to a real verification. The evidence document numbers that fallback as this
phase's **seventh** instance of ADR 0018's species.

**And a default does not rescue the read**, which is settled by the frame rather than by argument:
the call that raised *was* `spark.conf.get(key, "100")`, and the refusal is raised inside
`handleGetWithDefault`. **The default is applied by the server, after a read it declines to
perform.**

**So truncation is ruled out by a SECOND measurement instead.** The ring evicts oldest-first, so a
buffer whose oldest retained update is batch 0 has evicted nothing, whatever the cap is. Where
neither the cap nor that argument is available, the run prints that its count is a **LOWER BOUND**
and says truncation is unruled-out. A reading carries **which** argument it used, and a test sweeps
all four readings and asserts neither state can borrow the other's vocabulary.

**What reverses it:** a compute that hands the cap over. The cap arm is unchanged and takes
precedence wherever it can run; nothing here loosens it.

---

## Consequences

**What this phase leaves running.** A producer that is a transport and not a generator; a
checkpointed `availableNow` Delta ingest that keeps the record's bytes beside the parsed columns; a
two-arm exactly-once experiment whose control arm exists to be falsified; a two-arm watermark
experiment whose product is a difference; a serverless job that reads a real broker over SASL_SSL
and prints what it could not check; and a CI job that runs the Kafka tests on Linux — **which has
never executed**, and which the evidence document's §3 says so of rather than leaving to be
assumed.

**What it leaves refused, each with a reversal condition:** proving exactly-once on the deploy
target (Decision 2), a second corpus or serialiser (3), `transaction_id` as the processing identity
(4), a business-tuple dedup key and a single-arm watermark run (5), registering the managed
broker's table (6), a username or host in the secret scope (7), and a substituted ring-buffer cap
(8).

**What it leaves UNDECIDED, and that is a third category rather than a softer second one.** The
`<=` in the late-data model was never reached: over `promotable`, no delivered record's margin
equals either arm's delay, at either rate limit, under a lag of 1, 2 or 3 alike. The guard
**refuses that case** rather than choosing an answer for it, and the module's docstring no longer
attributes the comparison to Spark — that attribution was a reading, and the runs beside it were
not a test of it. It is listed as unexercised, not as settled.

**What it cost to learn, and the reason this ADR is written the way it is.** F4's species was a
check whose output cannot distinguish *passed* from *never ran*. **The evidence document numbers
the ring-buffer fallback this phase REFUSED as its seventh instance of that shape** — so six had
already been found by then. Among them: a probe that returned the same `TimeoutException` string
for a host where nothing listens and for one that certainly accepts TCP; a controller-level size
check blind to untracked files, which reported green having measured none of the work; a rebuild
guard that accepted all three defects its docstring claimed to catch; a live test whose pinned
offsets passed over a **doubled** corpus; a correction that shipped a new ceiling with no test of
its failure arm; and a constant that was true, well-evidenced and **unfalsifiable from anything a
reader could run**.

**The phase's own new defect was somewhere else, and it is worth more than the tally.** Four
published prediction clauses were retracted — **every one the controller's**, and every one found
by an independent reviewer who had been asked to check the prediction clause by clause. The shipped
code never carried any of the four confusions: it had `_DELIVERED = 29` and `_CLEAN_ROWS = 24` as
separate constants, asserted separately, and `surviving_repeats == 800` as an assertion rather than
as arithmetic in prose. **The defect had moved out of the code and into the document that judges
it** — which is the one place a review that only reads code will not look.

**So the standing instruction this phase adds, beside ADR 0018's:** a prediction whose terms change
meaning between two cells of one row is not a prediction, it is two, and one of them is unstated —
and a falsifier must be checked against the mechanism that would fire it, not only against its own
arithmetic. **A tripwire that names the wrong number is worse than no tripwire, because it reads as
coverage.**

---

## References

- `docs/f5-run-evidence.md` — the measurements, controller-verified separated from reported; the
  four retracted prediction clauses, struck inline rather than deleted; and §3's ledger of what is
  still unexercised
- [ADR 0018](0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md) — the species this
  phase kept finding, and the standing instruction this one extends
- [ADR 0002](0002-two-layer-topology.md) — the topology whose Context Decision 1 touches, and whose
  own amendment is the model for touching it that narrowly
- [ADR 0004](0004-pyspark-optional-extra.md) — why the Kafka connector resolves per session and is
  **not** on `opl.spark.local_session`: putting it there would give every test in the suite a Maven
  resolution and a network dependency at session start
- [ADR 0009](0009-deployed-revision-provenance.md) — why the managed-broker job carries a revision
  guard, and why the argument for it here is about **evidence** rather than about rows
- **The phase plan is NOT part of this repository.** It lives in a git-ignored working directory,
  and no link to it is given: F3 shipped a section pointing a public reader at that directory and
  they reached nothing. A citation a reader cannot open is worse than none, because they cannot
  tell a missing document from a withheld argument. Everything this ADR rests on is in
  `docs/f5-run-evidence.md`, in this repository, at a path a reader already has.
