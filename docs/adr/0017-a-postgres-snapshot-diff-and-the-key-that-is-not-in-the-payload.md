# ADR 0017 — a Postgres snapshot-diff, and the key you must diff on is not in the payload

## Status

**Accepted**, F-DB, 2026-08-17/18. The fourth and last of the four sources the job posting
names: Files (CNPJ), Event streams, APIs (PTAX), **Databases (Postgres)**.

This ADR records six decisions and **two patterns that only became visible once four sources
existed**. It amends nothing on its own; the amendment this phase owed to
[ADR 0002](0002-two-layer-topology.md) was made in that file, scoped to its Context, and
[ADR 0003](0003-cnpj-extraction-layer.md) and [ADR 0004](0004-pyspark-optional-extra.md) were
corrected in the same pass because they inherited the same premise.

> **The measured consequences of the run are in `docs/f-db-run-evidence.md` §2.** This
> document carries the decisions and the arguments; that one carries the numbers and which
> predictions survived them. Where a number appears here it is quoted from there.

---

## Context

Master spec §3.2 asks for *"extração incremental snapshot-diff"* against a Postgres source.
The lakehouse already had a snapshot-diff — `opl.vault.observation`'s five-state ledger and the
satellite `hash_diff` machinery, proven over two real RFB months. **So the question this phase
had to answer was not "how do we diff" but "what does a database give us that a monthly file
drop structurally cannot".**

Three things, and the third is the one worth the phase:

1. **A hard DELETE.** F2 measured **zero departures across 68,629,147 keys** — the RFB retains
   baixadas — and re-verified it on 2026-08-15, still zero. So `load_effectivity_satellite`'s
   closing path had **never fired in this lakehouse on any data**.
2. **An update a watermark cannot see.** `updated_at timestamptz DEFAULT now()` **does not fire
   on UPDATE** — a `DEFAULT` is an INSERT-time default. Without an explicit `SET` on every
   write path or a `BEFORE UPDATE` trigger, every update is invisible to a watermark. A
   **default-shaped** trap, not a contrived one.
3. **A row committed out of timestamp order.** `updated_at` orders by transaction **START**;
   visibility orders by transaction **COMMIT**. A slow transaction stamps `t1`, a fast one
   stamps `t2 > t1` and commits first, an extract records `watermark = t2`, and only then does
   the slow transaction commit. `WHERE updated_at > t2` **never returns that row, and never
   will.**

---

## Decision 1 — the extractor runs host-side, and the reason is topology, not egress

The extraction runs off Databricks, as every source in this project does. **The reason
this project gave for that was measured false by F-API and is not the reason here.**
ADR 0002's Context claimed Free Edition serverless blocks outbound internet; a serverless task
has since resolved a public host, taken HTTP 200, pulled 192,973 bytes, and run an API fetch
**in production**.

**What rules out a Databricks-side Postgres read is that the database is a container bound to
`localhost:5433` on a machine behind a home NAT.** Egress *out of* Databricks creates no route
*into* it. These are opposite directions and ADR 0002 conflated them.

**Labelled *argued*, not *measured*.** There is no public address to point a probe at, so no
measurement is offered and none is implied. `psycopg[binary]` on serverless is likewise
**unmeasured and stays that way** — "we did not measure" and "it does not work" are two
sentences this project keeps having to separate.

**Rejected: a cloud Postgres.** It needs an account, it deletes the "Postgres em Docker" the
master spec names, and the diff mechanism is byte-identical either way — so the spend buys
plumbing that proves nothing.

## Decision 2 — land FULL snapshots; derive the diff in the lakehouse

This deviates from §3.2's adjective *incremental*, deliberately. An extractor that diffs
host-side and emits only changed rows was refused with four mechanisms:

- **It breaks bronze's contract** (raw as-is). Every bronze count becomes un-reconcilable
  against the source, and a row that did not change is indistinguishable from one that was
  never there.
- **It makes the landing zone unreplayable.** A stateful extractor that has consumed its own
  previous snapshot cannot rebuild the lakehouse from what it landed.
- **It is a second spelling of the vault.** `changed_rows` is already generic over its watched
  column. Change detection was solved; only the axis was not.
- **And the strongest: the ledger's discriminating power only exists AFTER the DQ gate.**
  `rejected_by_our_gate` versus `absent_after_observation` is [ADR 0010](0010-observation-ledger-over-a-lossy-extract.md)'s
  whole subject, and a host-side differ structurally cannot tell *we dropped it* from *Postgres
  deleted it*, because quarantine does not exist at that point. [ADR 0011](0011-no-hub-socio-and-a-disappearance-driven-effectivity-satellite.md)
  prices the confusion at **1,781 sócio keys per month** whose windows would be falsely closed.

**The cost is stated: extraction is O(full table) per run**, which is nothing at 1,088 rows —
**and saying it is nothing at this scale is not the same as saying it does not matter.** The
mechanism that would make it genuinely incremental is log-based CDC, which master spec §14
already cut. **The deviation is inherited from a cut the spec made, not a new idea.**

## Decision 3 — one `REPEATABLE READ READ ONLY` transaction, stamped by its FIRST statement

A snapshot is more than one statement — the GUC pins, the stamp, the column-list catalog read,
the table read, the watermark query, the reconciliation count — and that, not any property of a
single `SELECT`, is what needs a transaction. Measured: **one `SELECT` under `READ COMMITTED`
is atomic**; a **batched keyset read is not** (nine rows read from a ten-row table, an answer no
instant contains), and **OFFSET paging returns a row twice**.

**The stamp must be the transaction's first statement.** The `REPEATABLE READ` snapshot is
acquired at the first data-reading statement, **not at `BEGIN`**, while `transaction_timestamp()`
is fixed at `BEGIN`. Measured: a **2.503 s** gap in which a row committed after the stamp **is in
the snapshot** — verbatim the smear the ruling exists to prevent. With the stamp first: **0.001 s,
0 rows.** `clock_timestamp()`, never `now()` or `transaction_timestamp()`, which are aliases.

**`READ ONLY` is not decoration** — the `opl` role is a SUPERUSER, so nothing else stops the
extractor writing to the database it exists to observe. **`SERIALIZABLE` is refused with its
mechanism**: for a pure reader, snapshot isolation already *is* a consistent snapshot; SSI exists
to prevent write skew between *writing* transactions, and it would add `40001` retry semantics for
zero correctness gain.

**The visibility set is landed beside the wall clock** — `pg_current_snapshot()::text` and
`pg_current_wal_lsn()::text` — so "which two snapshots did the diff compare" is answerable exactly
rather than by a clock that can step backwards under NTP. **`txid_current()` is refused**: measured,
it assigns an XID **even inside a `READ ONLY` transaction** and advances the cluster counter.

**The transaction commits before the upload, for three measured reasons**, and the sharpest
arrived from a bug rather than from reasoning: a `READ ONLY` transaction that has merely *read* a
table **blocks every `ALTER TABLE` on it** for as long as it lives. An extractor holding its
snapshot across a multi-minute upload is a reader that stops the operational database from being
migrated.

## Decision 4 — Postgres renders its own values, under GUCs that are pinned AND read back

Every column reaches the digest as text, so something must render it. **It must be the database**
— F-API measured `json.loads` → `float` → `str` dropping the trailing zero of `5.07730`.

**But `col::text` is session-dependent**, and libpq reads `PGTZ`, `PGDATESTYLE`, `PGCLIENTENCODING`
and `PGOPTIONS` **from the process environment** and applies them as startup options. No code change
required — one shell variable. Measured end to end: a writer at `DateStyle='SQL, DMY'` renders
`03/08/2026`; a reader at `'ISO, MDY'` parses **2026-03-08**; the stored value was **2026-08-03**.
Nothing raised.

**So the session pins seven GUCs as its first act and READS THEM BACK, refusing on any
disagreement.** A `SET` that is not read back is not a pin. `server_encoding = 'UTF8'` is
**asserted, not assumed** — a `LATIN1` database was created on this very container to check that
the premise needed checking.

**`col::text` is the CAST, not the type's output function**, and the plan said so only after
measuring: `bool` → `'true'` by cast and `'t'` by `typoutput`; `char(5)` → `'ab'` by cast and
`'ab   '` by `typoutput`. **The cast strips the padding, which is a data change**, so `char(n)` is
excluded from the schema outright, along with `float8`, `jsonb`, `json`, arrays, `interval` and
`money`, each for a measured reason.

## Decision 5 — `hub_merchant` keys on its OWN identifier, and the CNPJ is reached by a LINK

Keying `hub_merchant` on the CNPJ would produce, for `12345678`, a digest **byte-identical** to
one `hub_empresa` already holds over 69,062,849 keys — `loading.hash_key_expression` hashes the
padded key components and **the hub's name is not in the digest**. Two tables, one key space, one
digest space, and no guard anywhere refuses it. **ADR 0011 already rejected exactly that shape.**

**It is also the only shape that end-dates.** `vault/registry.py` refuses any
`EffectivitySatellite` whose parent is not a `Link`, and the descriptive satellite *"does not act
on a departure. It reports one."*

**And the loader could not write the link, which this phase got wrong before it got right.**
`link_candidates` reads every hub's business key **from columns named after it**, and
`bronze_merchant` carries `cnpj` (fourteen characters) where `hub_empresa` keys on `cnpj_basico`
(eight). So the derivation is **declared on the link end** — a source column and the prefix width
taken from it, as data — and computed through `loading.hash_key_over`, which already existed for
exactly this and had one caller. `build_registry` refuses a component-count mismatch, a width that
disagrees with the hub's, and a hub declaring no width at all.

**Both ends are identifying**, and that is not a detail: with `merchant_id` alone in the identity,
a merchant re-pointed to another company keeps its link hash key, the old relationship never
becomes `absent_after_observation`, and **no closing row is written**.

**The ledger's grain must be the value the link keys on, not the column it reads it from** — a
correction this phase made to its own ruling after a review reproduced the failure. `cnpj → cnpj[:8]`
is many-to-one, so a grain keyed on the raw column is strictly **finer** than the link's identity,
and a same-root change under one `merchant_id` produced **an active row and a closing row on the
same `applied_date` for the same hash key**, with every count correct. The grain now carries the same
declared derivation; a prefix is idempotent, so **one spelling of the digest survives**.

## Decision 6 — the snapshot axis is declared on `BronzeTable`, and `_snapshot_at` is a CONTRACT column

`observation.MONTH_COLUMN` was a module constant and the before/after-first-observation split is a
**lexicographic** comparison used as chronological. **Two snapshots in one calendar month collapse
onto one observation**, `absent_after_observation` has no producer, and the headline dies. So the
axis became a declaration on the source, defaulting to the month, flowing into the ledger, the
loaders and the job's window parameter.

**The Postgres axis is `to_char(…, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')` at UTC, generated in SQL** —
not `::text`, which yields a space instead of `T`, `+00` instead of `Z`, and **trims trailing
fractional zeros**, which breaks a lexicographic ordering being used as chronological. Text sorting
*is* chronological under ISO+UTC **and stops being so the moment `TimeZone` is not UTC**, which
Decision 4's pin is what guarantees.

**And one layer down, `applied_date` is a DATE.** The satellite dedups `groupBy(hash_key,
applied_date)` with a tie-break the module itself calls the worst fold it performs. **So the two
snapshots are taken on two genuinely different calendar days — taken, not stamped** — which costs
scheduling rather than code. Promoting `applied_date` to a TIMESTAMP was refused: gold's
`company_sk` hashes it *because* a DATE has no zone, and moving it re-opens a timezone defect that
took three measurements to close.

---

## The two patterns, which only four sources could show

### Pattern 1 — the key you must diff on is not in the payload. Three sources, three times.

- **CNPJ**: `applied_date` comes from the RFB mainframe **filename** token, not from any column.
- **PTAX**: the quote date is carried from the **request**, because the API does not return it
  (ADR 0016's central finding).
- **Postgres**: `_snapshot_at` is stamped by the **reading transaction**, because a row cannot
  know when it was looked at.

**Three sources, three times, the coordinate the lakehouse must diff on had to be carried in from
outside the payload.** F-DB is where that stops being three coincidences and becomes a stated
property of snapshot ingestion: **the observation instant is metadata about the READ, and a source
that appears to supply it is usually supplying something else.** `bronze_merchant`'s contract
enforces the distinction at import — `_snapshot_at` and `updated_at` are declared in disjoint
provenance groups, discriminated by a leading underscore, precisely because the tempting edit is
to make one a projection of the other. They are different facts: `updated_at` orders by
transaction START and `_snapshot_at` by transaction COMMIT, and collapsing them makes the miss
this phase measures **unmeasurable while every count stays green**.

### Pattern 2 — four rulings whose decision was right and whose stated mechanism was false

This phase pre-decided eleven tensions so no implementer would stop to ask. **Four of them
reached the correct decision from an argument that measurement then destroyed**, and every one
was caught by someone other than its author:

| ruling | the decision | the mechanism that was false |
|---|---|---|
| **T3** transaction shape | correct | a VOLATILE function was said to escape the *transaction* snapshot. It escapes the *statement* snapshot only — refused by Task 0's implementer, with a control proving the probe reached the mechanism |
| **T4** GUC pins | correct | `extra_float_digits=3` was said to buy round-tripping over the default. Since PG12 both 3 and 1 round-trip; the pin is worth taking **because of the environment**, not the default |
| **T5** the link | correct | `load_link` was said to be able to write it. It cannot — and the task's own docstrings had been saying so for two days |
| **T11** collation | correct | it was said to protect the extractor's byte-identity refusal. That check is single-run and order-invariant; the consumer that needs it is the **seeder's** cross-run content digest, one file away |

**The generalisation is not "be more careful."** It is that **a ruling which publishes its
mechanism can be falsified by the next reader, and a ruling that publishes only its conclusion
cannot.** All four survived as decisions precisely because the argument was written down beside
them and could be attacked. This is the argument for the house style, made by four counter-examples
to the house's own reasoning.

---

## Consequences

### What this buys — measured

Measured on the run of record, 2026-08-17/18, against a deployed wheel verified by artefact.
The two snapshots were **taken** on two different calendar days —
`2026-08-17T21:51:37.226296Z` and `2026-08-18T00:00:56.258041Z` — and `bronze_merchant` carries
**two distinct `_snapshot_ref_date` values**, which is the row this whole design would have died
on. Full detail in `docs/f-db-run-evidence.md` §2.6.

- **END-DATING FIRED, FOR THE FIRST TIME IN THIS LAKEHOUSE: 16 windows closed**, every one
  `closed_by='absent_after_observation'`, controller-verified against the workspace. F2 measured
  **zero** departures across 68,629,147 RFB keys and it was still zero when re-checked eleven
  days later, because **a hard DELETE is the thing a monthly file drop of a public registry
  structurally cannot produce.** That is what the fourth source bought that the first three
  could not.
- **48 rows a watermark extract misses**, computed as `diff_caught − incremental` = 128 − 80 and
  decomposing **16 deletes + 24 silent updates + 8 out-of-order commits** — the complement of a
  real incremental query run **inside snapshot 2's own transaction**, so the miss set is a
  genuine complement rather than a definitional one. **The decomposition is by the measured
  position of `updated_at`, not by class membership**, so it is a property of the data rather
  than a re-reading of the script that made it.
- **The out-of-order miss survives the correct fix.** It was produced with the `BEFORE UPDATE`
  trigger armed — the repair for the other two watermark classes — and the row was unreachable
  anyway. **Six of the seven classes are authored by the mutation script and are declared as
  such before the number; this one is MVCC's.**
- **The integration claim is now a link rather than a collision**, and the check the loader
  structurally cannot make was made separately: all **1,024** merchant CNPJ roots resolve to real
  `hub_empresa` keys. `refuse_unloaded_hubs` is an existence test — it would pass over a link
  pointing at nothing.
- **A refusal earned its place on a second source.** The vault job **failed on purpose** against
  a window holding one instant, rather than reporting **0 closes** — a number indistinguishable
  from a clean load. It is a launch-ordering constraint the job YAML cannot express: the
  effectivity task needs both instants in one window, so **per-snapshot incremental vault runs
  are impossible for this table.**
- **The satellite's dedup fold — the one `satellites.py` calls the worst it performs — was
  exercised and measured at zero.** Snapshot 1 was loaded twice and contributed **0** duplicate
  rows, `0 source rows folded` on both the satellite's and the effectivity's dedup.
- **All nine §1 predictions were confirmed and none was falsified**, and that is a weaker result
  than a falsification would have been. What makes the confirmations mean anything is that
  §1.3's three falsifiers were each reachable and each failed to fire: the miss is 48 and not 40,
  `departed` is 16, and `sat_merchant_dados` landed **+112 = 32 + 80** from one `hash_diff` pass
  with no branch per class anywhere.

### What ships UNEXERCISED

Carried in `docs/f-db-run-evidence.md` §3, accumulated as the phase ran rather than
reconstructed at its end. It is long on purpose.

### What was declined rather than deferred

- **Migrating `opl.vault.partners` onto the declared derivation.** `link_company_partner` also
  carries dependent-child keys, so the generic loader still cannot write it; the migration would
  touch a loader proven over 33.13 GB and buy no capability. Recorded as a seam: the derivation is
  now declarable in one place where it was hard-coded in two.
- **Seeding one of the forty characters the two hash spellings disagree on.** It would produce a
  vault whose Python and Spark digests disagree on real data **with no test going red**, because
  the loaders only ever use the Spark spelling — a latent defect dressed as a demonstration. The
  constraint is a **bronze DQ rule** instead, where every other content constraint in this
  repository lives.

## References

- `docs/f-db-run-evidence.md` — §0 the measurements taken before anything was built, §0.1 and
  §0.5 the two guards nobody had checked, §1 the predictions, §2 what the runs said, §3 what
  ships unexercised.
- [ADR 0002](0002-two-layer-topology.md) — Context amended by this phase; Decision unchanged.
- [ADR 0003](0003-cnpj-extraction-layer.md), [ADR 0004](0004-pyspark-optional-extra.md) —
  amended in the same pass, both having inherited ADR 0002's premise.
- [ADR 0010](0010-observation-ledger-over-a-lossy-extract.md) — `rejected_by_our_gate` versus
  `absent_after_observation`, which Decision 2 rests on.
- [ADR 0011](0011-no-hub-socio-and-a-disappearance-driven-effectivity-satellite.md) — the
  duplicate-hub shape Decision 5 avoids, and the 1,781-key price of confusing our drop with
  their delete.
- [ADR 0016](0016-fx-resolved-by-publication-instant-not-a-holiday-calendar.md) — the second of
  Pattern 1's three sources.
