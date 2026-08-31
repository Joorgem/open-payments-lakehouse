# ADR 0012 — the business-key hash has two spellings on purpose, and the JDK's Unicode version is now a vault input

## Status
Accepted. Implemented in `src/opl/vault/hashing.py` (the standard) and
`src/opl/vault/hashing_spark.py` (the second spelling); locked by
`tests/vault/test_hashing.py` (ten literal digests) and
`tests/vault/test_hashing_spark.py` (the equivalence sweep and the divergence
pin). Written in Task 7 of F2 wave 1, after the fact: the decision was taken in
Task 3 and its rationale lived only in a module docstring, which is the wrong
place for a decision whose largest consequence is on the **platform upgrade
path** rather than on the module.

**Why this ADR exists at all.** Every other decision in this package is a
statement about the vault. This one is a statement about the **runtime**: after
it, a Databricks Runtime upgrade that changes the JDK is a change to the vault's
keys. Nobody plans a DBR upgrade by reading a hashing module.

## Context

### The standard, and the volume it has to survive

`opl.vault.hashing.hash_key` is the DV2 business-key standard for this
repository: SHA-256 over components that are trimmed, upper-cased, tagged
(`N`/`E`/`W` for NULL, empty and whitespace) and **length-prefixed**
(`S<len>:<value>`) before being joined with `||`. The length prefix is what makes
the encoding injective: without it `["a||b","c"]` and `["a","b||c"]` both join to
`a||b||c`. A controller probe over 14,424 component lists built from tag
lookalikes, delimiter fragments, colons and accented characters found **0
encoding collisions** over 8,420 distinct encodings; neither the Task 1 review
nor that probe could break the property.

Two facts about that module pull against each other:

1. **It is pure Python and deliberately pyspark-free.** ADR 0004 makes pyspark an
   optional extra because the extraction scripts run off-Databricks. `hash_key`
   is importable and testable with no session, and its correctness is pinned by
   **ten hard-coded digests** typed into `tests/vault/test_hashing.py` and
   computed independently with a bare `hashlib.sha256` over hand-built encoded
   strings. Both sides of those assertions cannot move together.
2. **The vault keys tens of millions of rows per table per month.**
   **69,062,849** companies in 2026-07 (`01f19274-c1e0-1f3a-998a-ee0234483f5c`)
   and 72,318,964 establishments behind them, on a Free Edition workspace.

### The defect class this repository has already paid for

`opl.config` records what two spellings of one rule cost: the month rule had two
implementations and `2026-13` ended up refused at two of four entry points, with
the missed half being the one where an impossible month became a delete boundary.

A second spelling of the **hash** is worse than that, and the difference is worth
stating exactly. A drift in the month rule produces a wrong refusal — loud. A
drift in the hash produces **a different digest for the same business key**: no
error, no row-count anomaly, and every join from satellite to hub simply returns
nothing for the affected rows. The pinned digests cannot see it, because they
exercise the Python spelling and the vault runs the other one.

## Options considered

### 1. A Python UDF wrapping `hash_key`

One source of truth, and the honest first choice: any change to the standard is
caught by the ten pinned digests, because the digests exercise the very code the
vault runs. **Rejected, on two counts and not only the obvious one.**

- **Cost.** A Python UDF serialises every row out of the JVM, through a Python
  worker, and back; it also blocks whole-stage codegen and Photon, so the loss is
  not the marshalling alone — the surrounding plan gets slower too. At ~69M rows
  per table per month this is the difference between a load and a load that does
  not finish. `opl.bronze.snapshot` already made this call for a much cheaper
  derivation ("a UDF would break Catalyst over 144M rows"); this is the same call
  on roughly twenty times the work.
- **Availability, which is the count that actually settled it.** A UDF is the one
  shape that cannot be tested without a session. Choosing it would put the
  standard's ten pinned digests **behind a Spark dependency they do not have**,
  and the pinned digests are the only thing making a change to the standard
  deliberate. The cheap option would have made the expensive guarantee
  conditional on the optional extra.

### 2. A Spark-native expression, unguarded

`sha2` over a `concat_ws` of the same encoding. Fast, and wrong to ship on its
own: it is a second spelling with nothing asserting the two agree, which is
precisely the `opl.config` defect with a silent failure mode instead of a loud
one.

### 3. A Spark-native expression whose price is a mandatory equivalence test

Chosen. The second spelling is admitted **and** the drift it invites is made a
test failure rather than a data corruption.

## Decision

**Spell the standard twice — once in Python for the standard's own pinned
digests, once in Catalyst for the vault — and require the two to be proved equal
over an adversarial corpus and over the whole cased code space.**

The equivalence requirement was made a precondition of the choice rather than a
follow-up, and it **paid for itself before the code shipped**:

- **Trim.** Spark SQL's `trim(str)` removes ASCII SPACE (U+0020) and nothing
  else; Python's `str.strip()` removes **29** characters, NBSP among them. Bronze
  is parsed from **cp1252** RFB CSVs, where an NBSP inside a razão social is an
  ordinary thing, so `"\xa0"` would have encoded as `W` in Python and `S1:\xa0`
  in Spark — two digests for one business key, forever, with nothing failing.
  Closed by deriving `_TRIM_PATTERN` from a `TRIMMED_CHARACTERS` tuple that
  `test_the_trim_class_names_exactly_the_characters_python_strips` asserts
  against `str.isspace()` over all 1,114,112 code points, in both directions.
- **Case, and this one is not closed.** `F.upper` bottoms out in Java's
  `String.toUpperCase`, whose case table is the **JDK's** Unicode version;
  `str.upper()` uses CPython's. JDK 17 ships Unicode 13.0 and CPython 3.12 ships
  Unicode 15.0. **Neither is pinned anywhere in this repository.**

### The Unicode skew, measured rather than reasoned about

Swept over every cased character on `java.version 17.0.19` / CPython 3.12.13 —
**1,525 cased characters, 40 divergent**, all of which gained a case mapping in
Unicode 14.0: U+2C5F, U+A7C1, U+A7D1, U+A7D7, U+A7D9, and the U+10597–U+105BC
span minus U+105A2, U+105B2 and U+105BA. **None of the forty is encodable in
cp1252**, so no CNPJ bronze row can hold one today.

Found by the Task 3 reviewer, which swept all 1,112,064 non-surrogate code points
rather than sampling; confirmed by an independent controller sweep
(`probe_unicode_skew.py`); recomputed a third time by the scoped re-reviewer,
which got the same 1,525 and the same 40. A separate controller parity probe over
24 adversarial Unicode cases — length-changing upper-cases (`ß`→SS, `ﬁ`, `ﬃ`,
`ŉ`, `ǰ`), the Turkish dotless-i hazards, NFC vs NFD, 4-byte emoji, the ohm sign
and six exotic whitespace characters — found **0 mismatches**.

**The forty are pinned as a strict EQUALITY, not as an allow-list**, and that is
the load-bearing detail. An allow-list tolerates the current divergence and goes
red only when new divergences appear. A Java 21 (Unicode 15) runtime would make
these forty **agree** — which changes their digests, which re-keys any vault row
containing one, just as surely as a new divergence would. A change in **either
direction** must therefore be a decision, so both turn the suite red.

## Consequences

- **The JDK's Unicode version is a vault input.** A DBR upgrade that moves the
  JDK is a re-keying event for any row containing one of the forty. The suite
  goes red on such an upgrade *by design*; that red is not a broken test to be
  relaxed, it is the decision point. Anyone tempted to update
  `UNICODE_VERSION_DIVERGENCE` to make CI green is choosing to re-key the vault
  and should say so out loud. `.github/workflows/ci.yml` pins `temurin` 17, which
  is the same case table as the deploy target; that pin is now load-bearing.
- **cp1252, not Latin-1, is the reachability argument** — and the distinction is
  not pedantry. cp1252 **can** produce characters above U+00FF (`Š Œ Ž Ÿ š œ ž`
  and assorted punctuation), so any "bronze cannot hold a character above U+00FF"
  argument is false. The reachability test asserts encodability against the
  imported `CSV_DIALECT["encoding"]` rather than restating a codepoint bound, so
  a dialect change moves the test with it.
- **Wave 2 inherits this, and reaches the forty immediately.** A feed that is not
  cp1252-bound — a payments stream, a UTF-8 API extract — can carry those
  characters on day one. A wave-2 task adding such a feed must read this ADR
  before it keys anything.
- **Re-run the probes whenever the JDK, the encoding or the trim class moves.**
  `probe_unicode_skew.py` and `probe_hash_parity.py` are session-scratchpad
  scripts and are **not in the repository** — a known gap, recorded here rather
  than left to be discovered. What *is* in the repository, and is what actually
  guards the property, is the committed sweep in `test_hashing_spark.py`.
- **No Unicode normalisation is applied, by either spelling.** NFC and NFD
  spellings of one accented name are two different keys. That is a **split, not a
  merge** — it produces two hub rows rather than merging two companies — and
  today's business keys (CNPJ, RFB-masked CPF) are ASCII. It is pinned by test
  and stated in the module rather than left silent, and it is the first thing to
  revisit if a name-derived key is ever introduced.
- **The equivalence test is only as good as its corpus, and that was proved the
  hard way.** The original mitigation claim rested on a 37-value curated list
  which contained **no `i` and no `I`** — so the locale divergence it claimed to
  catch, it could not have caught. The fix was to sweep the cased code points
  instead of curating. **A curated adversarial corpus is a sample presented as a
  proof**; this is the one place in the package where the total sweep is cheap
  enough to be mandatory.

### What would change this decision

- Spark exposing an upper-casing whose Unicode version can be pinned
  independently of the JDK's. That removes the second half of the problem and
  leaves only the ordinary second-spelling risk.
- A feed that must carry one of the forty as part of a business key. That makes
  the divergence reachable rather than latent, and forces a choice between
  pinning the runtime and normalising before hashing.
- A measurement showing the UDF cost is affordable on the target cluster. That
  would collapse the two spellings back into one, which is strictly better if it
  can be paid for.

**AMENDED 2026-08-31 by F7 — the feed in the second condition arrived, and the project
took a third option neither of its branches names.** F-DB's UTF-8 Postgres merchant
registry is that feed. The runtime was not pinned and nothing is normalised before
hashing: a row carrying one of the forty is **rejected at the bronze gate** under
`unhashable_case_divergence` — `opl.bronze.rule_predicates._case_divergence_check`,
registered against the merchant contract in `src/opl/bronze/rules.py`. **The choice is
still unforced**, because merchant's business keys are ASCII: the divergence stays latent,
and that rule has rejected nothing outside a fixture (`docs/f-db-run-evidence.md` §3).
`src/opl/vault/hashing_spark.py`'s module docstring carries the same correction.
