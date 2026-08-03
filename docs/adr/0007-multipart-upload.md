# ADR 0007 — adopt the SDK's multipart upload path, and pin to the minor

## Status
Accepted

## Context
F1.3 lost 1,371,521,024 bytes of `Estabelecimentos0.zip` to a silent short
upload, twice, byte-identically. The cause was `databricks-sdk-py#878`: in
`databricks-sdk <= 0.41.0`, `_BaseClient._perform` rewound the request body only
inside its `if error is not None` branch, so a raised `ConnectionError`/`Timeout`
skipped the rewind and the retry PUT only the remainder — a complete, well-formed
PUT of the tail, which the server stored and returned 204 for. Fixed in `0.42.0`,
where the rewind became a `before_retry=` callback on the `retried()` decorator
and retries are disabled outright for non-seekable streams. F1.3 recorded the
upgrade as a carry-forward, on the grounds that it "changes the constraint this
phase's architecture was designed around".

Deciding it required knowing what actually changes between versions, so the
published wheels were bisected rather than reasoned about. Installing each
version with `uv pip install --target … --no-deps` and reading
`databricks/sdk/mixins/files.py`:

| version | `FilesExt.upload` | behaviour |
|---|---|---|
| 0.40.0–0.44.x | not overridden | `files.upload()` is the generated single PUT |
| 0.45.0 | overridden | multipart above `multipart_upload_min_stream_size` = **5 MiB** |
| …–0.68.x | overridden | same shape, 5 MiB threshold |
| 0.69.0–0.71.x | rewritten | parallel presigned parts, `use_parallel=True`, **no size gate at all** |
| 0.72.0+ | rewritten | threshold reinstated and raised to **50 MiB**, renamed `files_ext_multipart_upload_min_stream_size` |

Three findings matter. **Multipart-by-default arrived at 0.45.0, not 0.72.0** —
27 minor releases earlier than the project's own release notes led us to believe;
0.72.0 raised a floor rather than introducing the path. **0.71.0 dispatches on
`if ctx.use_parallel:` with no size check**, so it multiparts everything. And
**none of it is reachable by staying current selectively**: our objects are the
Estabelecimentos zips at 320 MB–2.13 GB, above every threshold any version has
used, so on any SDK from 0.45.0 onward every upload this repo performs is
multipart whether or not we intend it.

> **That last sentence is FALSE beyond the one table group it was written from,
> and F1.4b's live run is what showed it.** Estabelecimentos was the only group in
> the Volume when this ADR was accepted, and "our objects are ≥320 MB" was
> generalised from it. Nine of the twenty objects F1.4b PR A uploaded fall
> **below** the 52,428,800 B floor of the installed 0.123.0 and went out as single
> PUTs. Sizes and the path each object took are in
> [`docs/f1.4b-pr-a-run-evidence.md` §2](../f1.4b-pr-a-run-evidence.md#2-the-upload--and-the-nine-objects-that-were-not-multipart),
> stated once and not repeated here.
>
> The **decision** is untouched: multipart is still what the pin adopts and still
> what every object above the floor gets. What is retired is the claim that the
> floor is unreachable in this repository — it is reachable, by the smaller RFB
> tables, and a reader sizing a timeout or a memory budget from "everything here
> is multipart" would be sizing it from a fact that stopped being true the moment
> a second table arrived.

That leaves exactly two coherent positions — pin below 0.45.0 and keep single-PUT
semantics, or adopt multipart deliberately. There is no middle: a "recent but not
too recent" pin picks up multipart silently, which is how this decision nearly
got made by accident.

Measured geometry on the installed `0.123.0`, from
`_get_optimized_performance_parameters_for_upload` (smallest part size giving
≤ 100 parts):

| object | size | part size | parts |
|---|---|---|---|
| `Estabelecimentos0.zip` | 2,128,818,559 B | 50 MiB | 41 |
| `Estabelecimentos3.zip` | 366,824,247 B | 10 MiB | 35 |
| typical part | ~320 MB | 10 MiB | 31 |

> **The 41-part row does not generalise, and F1.4b PR B's Task 4 upload is where
> that stopped being a hypothetical.** It is correct for the 2026-06
> `Estabelecimentos0.zip` measured here (2,128,818,559 B), but the row is keyed
> by object *name*, and the byte count behind that name is what changes every
> month, not the name itself. 2026-07's `Estabelecimentos0.zip` is
> 2,164,567,397 B — 35,748,838 B larger — and takes **42** parts:
> `2,164,567,397 / 52,428,800 = 41.2858`, so the 42nd part is forced; one
> division would have caught this at any point. **Both the F1.4b PR B plan and
> the controller's own Task 4 brief carried "41 parts" forward** as the
> prediction for that run, so a wrong number in this table propagated into two
> downstream documents before a measurement caught it — the reason this is
> written out as a correction rather than the table quietly edited. The part
> *size* (50 MiB — the smallest option in
> `files_ext_multipart_upload_part_size_options` giving ≤ 100 parts)
> generalises across months; the part *count* in this table is a property of
> one month's byte count and does not. The "typical part" row has the same
> looseness at smaller stakes: 2026-07's typical Estabelecimentos objects ran
> 334.8–369.0 MB and took 32–36 parts, not the 31 above. See
> [`docs/f1.4b-pr-b-run-evidence.md` §13.5](../f1.4b-pr-b-run-evidence.md#135-the-estabelecimentos0zip-geometry--42-parts-not-41)
> for the 2026-07 measurement.

with `files_ext_multipart_upload_default_parallelism` = 10 concurrent parts and
`experimental_files_ext_cloud_api_max_retries` = 3.

## Decision
Adopt multipart. Pin `databricks-sdk>=0.123.0,<0.124.0` — a current release, with
the ceiling at the **next minor**.

Multipart is the better fit for this workload on its merits, not merely as the
price of staying current. The unit of retry becomes a part rather than the whole
file, which is what the F1.3 link needed: a transient failure 90% of the way
through a 2.13 GB upload now costs one 50 MiB part instead of restarting
everything. Pinning 18 months back to preserve single-PUT would be preserving a
semantic we do not prefer, in a repository where a stale dependency is itself a
signal.

The ceiling is deliberately narrow, and the bisect is the argument for it: in the
0.40 → 0.123 span, minor releases silently rewrote the upload path three times,
and #878 itself — a silent data-loss bug — shipped under *Internal Changes*. A
permissive ceiling means absorbing an upload-semantics change without noticing,
which is precisely how F1.3 lost part of a file. Raising this bound is therefore
a deliberate act that requires re-reading the upload path.

Two consequences were followed through rather than left implicit:

- **`UPLOAD_RETRY_TIMEOUT_SECONDS` was re-derived, 2 h → 30 min.** The old value
  bounded one whole-file PUT (`Estabelecimentos0.zip`, ~32 min at ~67 MB/min).
  Under multipart no whole-file operation exists; `retry_timeout_seconds` is
  applied per operation, to each control-plane call and — via
  `_retry_cloud_idempotent_operation` — to each presigned part PUT. The largest
  unit is one 50 MiB part, and because ten parts upload concurrently over a
  ~67 MB/min link, one part takes ~7.8 min of wall clock rather than the ~47 s it
  would take alone. 30 min is ~3.8× that. The SDK's own 300 s default is *smaller*
  than one worst-case part here, which would reproduce the F1.3 timeout at part
  granularity, so an explicit value is still required — just a different one.
- **The post-PUT size check stays**, as defence in depth rather than as a
  stand-in for #878. It compares the landed `content_length` against the local
  file, which is independent of how the bytes arrived.

## Consequences
- **Every upload of an object above the floor is now multipart.** Nothing in
  `upload_to_volume`'s interface changes; the difference is entirely inside
  `w.files.upload()`, and which path an object takes is decided there by its size
  and not by us.

  **Two numbers in the first version of this bullet were wrong.** It said "every
  upload this repo performs is now multipart, including the ~15 GB of zips F1.4b
  lands". Neither half survived the run.

  *The path:* nine of PR A's twenty objects went single PUT — see the correction
  in the Context above.

  *The volume:* F1.4b lands **9.376 GB**, not ~15 GB — 2.033 GB in PR A and
  7.343 GB in PR B. The ~15 GB counted the ten 2026-06 Estabelecimentos zips,
  which were **already in the Volume** before F1.4b started: F1.4a's evidence doc
  §4 lists all ten with the mtimes of their F1.3 upload and a total of
  5,259,919,847 B, and F1.4a's reclaim deliberately kept them. F1.4b re-uploads
  nothing.
- **Memory cost is real and previously unbudgeted.** Ten concurrent parts are
  buffered, so `Estabelecimentos0.zip` holds ~500 MiB (10 × 50 MiB) during
  upload; smaller zips hold ~100 MiB. `files_ext_multipart_upload_default_parallelism`
  is the knob if that becomes a problem on a constrained runner. Not tuned here —
  no evidence yet says the default is wrong.
- **`Config.__init__` now reaches the network.** From 0.123.0 it ends with
  `_resolve_host_metadata()`, a best-effort GET of
  `{host}/.well-known/databricks-config`, whose probe client is built from
  `self.retry_timeout_seconds`. Passing the widened upload budget to `Config(...)`
  therefore hands it to that probe, so an unreachable host would block
  `upload_client()` for the whole budget before falling back — 2 h under the old
  value. `upload_client` constructs at the SDK default and raises the budget
  afterwards to keep the probe on the SDK's own 300 s bound; a regression test
  asserts that ordering. The same probe made the hermetic client tests hang
  outright on upgrade (killed at 120 s), and they now neutralise it — a unit test
  must not depend on DNS.
- **`batch_size` is undocumented in this ADR, and it silently overrides
  `files_ext_multipart_upload_batch_url_count = 1`.** That config's own comment
  argues for requesting presigned URLs one at a time, precisely because "the
  more URLs we request at once, the higher chance is that some of the URLs
  will expire before we get to use it" on a non-seekable stream.
  `_get_optimized_performance_parameters_for_upload` computes `batch_size` as
  `ceil(sqrt(part_num))` whenever the content length is known, and nothing in
  this repository overrides that back down to 1 — F1.4b PR B's Task 4 upload
  observed it at 3-8 across every multipart object, **7** on
  `Estabelecimentos0.zip`
  ([`docs/f1.4b-pr-b-run-evidence.md` §13.5](../f1.4b-pr-b-run-evidence.md#135-the-estabelecimentos0zip-geometry--42-parts-not-41)).
  So every upload this repo performs fetches presigned URLs in batches of up to
  8, not one at a time, and the config's own stated mitigation is not in
  effect. Nothing has gone wrong from it — zero URL expiries across 505 parts
  and two PRs — so this is a documentation gap and a latent risk, not an
  incident. It sits directly beside the per-part-retry argument below and
  belongs in this ADR rather than only in run evidence.
- **The upload-as-ZIP design outlives the constraint that created it.** It was
  forced by the 5 GiB single-PUT ceiling of the 0.40 pin; that ceiling is gone
  and a 6.78 GB object would upload fine today. Uploading compressed stays
  because part 0 is 2,128,818,559 B against 6,780,467,695 B unzipped — under a
  third of the bytes — and because the Volume would otherwise hold both copies.
  At the same ~67 MB/min the timeout above is sized with, that is ~101 min for the
  CSV against ~32 min for the ZIP, so **~69 min saved on that part alone**.
  Comments in `opl.bronze.unzip_volume`, `opl.extraction.giants` and
  `scripts/extract_giants.py` were rewritten to give the surviving reason rather
  than the retired one.
  **The minutes are stated once, here, and cited rather than re-derived there.**
  This bullet first said "~53 min" while the timeout bullet above derived "~32 min"
  from the same rate and the same two byte counts — two independent estimates where
  only one could be right, and no throughput reproduces 53 (it would need
  ~88 MB/min, which contradicts the ~32 min sitting six lines up). Same failure mode
  as a duplicated guard: the second copy is not wrong on its own, it is wrong by
  drifting.
- **Multipart HAS now been exercised against this workspace**, and it moved bytes
  **94.7 MB/min** against single PUT's **24.8 MB/min** — a 3.8× difference over the
  same link, in the same run, nine objects each way. The whole run averaged
  **65.5 MB/min** against the **~67 MB/min** this ADR assumed when it re-derived
  `UPLOAD_RETRY_TIMEOUT_SECONDS` above, so the 30-minute per-part budget is
  **consistent with observed aggregate throughput** — the rate the derivation was
  built on survives contact with this link.

  That is weaker than "correctly sized by observation", which this bullet said
  until CodeRabbit read it on PR #6, and the difference is worth keeping. What was
  measured is twenty whole-object transfers and their aggregate rate. What the
  budget is actually about — one 50 MiB part completing inside 30 minutes, and the
  timeout releasing a stalled part so the SDK can retry it — was not exercised:
  no part was timed individually and nothing stalled. The observation removes the
  input assumption from the argument; it does not remove the argument.

  The byte counts, durations and per-object sizes behind those three rates are in
  [`docs/f1.4b-pr-a-run-evidence.md` §2](../f1.4b-pr-a-run-evidence.md#2-the-upload--and-the-nine-objects-that-were-not-multipart)
  and are **cited rather than restated**, for the reason the previous bullet gives
  at length: this ADR has already produced two independent derivations of the same
  quantity that disagreed, and a second copy of a number is not wrong on its own,
  it is wrong by drifting.

  Note what the comparison is worth and what it is not. It is a genuine A/B — same
  client, same link, minutes apart — and it was **unplanned**, which is why it
  exists at all: the ADR expected no single-PUT path to survive. It is also one
  sample of each, over different files. The throughput advantage was never part of
  this decision's argument, which rested on the unit of retry; it is a finding, not
  a vindication.

  **The 3.8× figure above is PR A's sample, not this connection's throughput,
  and F1.4b PR B's Task 4 upload is where that stopped holding.** PR B moved
  its 20 multipart objects at **164.9 MB/min** against single PUT's
  **24.8 MB/min** — **6.6×**, not 3.8× — and the single-PUT half of that ratio
  is the more telling one: PR B's 24.8 MB/min reproduces PR A's 24.8 MB/min to
  three significant figures, on a different day, against different bytes
  (444,440,799 B here versus 444,421,834 B in PR A). A rate that reproduces
  exactly across two runs on different data is a **per-request property, not a
  measurement of this link's bandwidth** — the same client moved multipart
  bytes at up to 199.3 MB/min minutes later in the same run. So neither 94.7,
  nor 24.8, nor 3.8× describes this connection generally; they describe PR A's
  sample. See
  [`docs/f1.4b-pr-b-run-evidence.md` §13.6](../f1.4b-pr-b-run-evidence.md#136-rates--the-multipart-advantage-and-why-single-put-is-not-a-bandwidth-number)
  for the PR B rate table this is drawn from.
- **What this still does not settle.** Per-part retry is still argued from the
  SDK's code, not from an observed recovery. PR A's 31 minutes produced no
  failure, and **PR B's 7.343 GB has now run too** — 505 part uploads across 21
  multipart objects, zero retries, zero URL expiries, zero warnings
  ([`docs/f1.4b-pr-b-run-evidence.md` §13.7](../f1.4b-pr-b-run-evidence.md#137-per-part-retry--the-opportunity-was-taken-and-closed-nothing))
  — and still nothing retried. Across both PRs that is 9.376 GB moved and
  nothing has ever retried. **This did not close the open item.** A link that
  keeps not failing is not the same evidence as an observed recovery, so volume
  alone will not produce one — closing it for real needs a deliberate fault
  injection, not another large upload. `protobuf` returns as a transitive
  dependency of the 0.7x+ line; it is not a core dependency of this project and
  does not affect the serverless wheel install of ADR 0004.
