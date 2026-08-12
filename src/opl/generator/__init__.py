# src/opl/generator/__init__.py
"""The synthetic payment stream generator (F1b): seeded, pure, and free of both the
wall clock and I/O.

WHAT "PURE" BUYS, AND WHY IT IS THE FIRST PROPERTY RATHER THAN A STYLE PREFERENCE. A
generator whose output cannot be reproduced byte-for-byte from a seed produces defects
that nobody can assert against: "there are 137 duplicates in this file" is a claim
about ONE file, not about the generator, and re-running it to check the claim produces
a different file. Everything below is therefore a function of its arguments and
nothing else -- no clock, no filesystem, no Spark, no `random` module, no environment.

FOUR THINGS THIS PACKAGE DELIBERATELY DOES NOT DO, each of them the obvious thing:

  1. IT DOES NOT READ THE CLOCK. Both timestamps a record carries are derived from the
     stream's own declared window (`opl.generator.stream.StreamSpec.window_start`).
     Lateness -- Task 2's business -- is the ORDERING between `event_time` and
     `emitted_at`, which is a property of the seed, not of when the process ran.
  2. IT DOES NOT USE `random`. CPython guarantees the reproducibility of
     `random.random()` for a given seed, but NOT of `choice`, `randrange` or `sample`,
     whose internals have changed across releases -- and this stream has to reproduce
     across a Windows dev box on CPython 3.12 and a Databricks serverless runtime.
     Every value is derived instead from a counter and a SHA-256, through
     `opl.generator.derivation`, which is a pure function of (seed, purpose, index).
  3. IT DOES NOT READ SPARK, DATABRICKS, OR ANY TABLE. The counterparty CNPJs must be
     real -- the whole integration claim rests on it -- and they arrive as an ARGUMENT:
     a pool handed to `generate`. The separate, thin concern that extracts that pool
     from the real `hub_empresa` is `opl.generator.cnpj_pool`, which builds the query
     and validates its result without executing anything. The claim "these are real
     CNPJs" then lives at ONE boundary where it can be measured, instead of being
     smeared through a module whose entire value is being testable without a session.
  4. IT DOES NOT WRITE FILES. `generate` returns records; serialising them to JSON
     Lines is `opl.generator.events.to_jsonl`, which returns TEXT. Turning that text
     into bytes on a Volume is F1b Task 3's job, and keeping the boundary here is what
     lets the byte-identity property be asserted in a unit test with no Volume.

WHAT IT DOES NOT CONTAIN YET, so nobody looks for it: DUPLICATES, LATE ARRIVALS and
DRIFT. This is the CLEAN stream, on purpose. Proving that a seed reproduces
byte-identically before any defect is layered on is what keeps a later failure from
having two candidate causes. The one thing that IS here and looks like a defect is the
LEGITIMATE REPEAT -- a different `transaction_id` carrying identical business
attributes -- because a duplicate is only measurable against something it can be
confused with. See `opl.contracts.payments` for the full argument.
"""
