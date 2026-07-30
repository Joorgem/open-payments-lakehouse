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
- **Every upload this repo performs is now multipart**, including the ~15 GB of
  zips F1.4b lands. Nothing in `upload_to_volume`'s interface changes; the
  difference is entirely inside `w.files.upload()`.
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
- **The upload-as-ZIP design outlives the constraint that created it.** It was
  forced by the 5 GiB single-PUT ceiling of the 0.40 pin; that ceiling is gone
  and a 6.78 GB object would upload fine today. Uploading compressed stays
  because part 0 is 2,128,818,559 B against 6,780,467,695 B unzipped — under a
  third of the bytes, ~53 min saved on that part alone — and because the Volume
  would otherwise hold both copies. Comments in `opl.bronze.unzip_volume`,
  `opl.extraction.giants` and `scripts/extract_giants.py` were rewritten to give
  the surviving reason rather than the retired one.
- **What this does not settle.** Multipart has not yet been exercised against
  this workspace at scale — F1.4b's ~15 GB is the first real run, and it is the
  validation, not a repeat of something already measured. Per-part retry is
  argued from the SDK's code, not from an observed recovery. `protobuf` returns
  as a transitive dependency of the 0.7x+ line; it is not a core dependency of
  this project and does not affect the serverless wheel install of ADR 0004.
