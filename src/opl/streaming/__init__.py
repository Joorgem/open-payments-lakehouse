# src/opl/streaming/__init__.py
"""The Kafka half of F5: ONE CORPUS, TWO TRANSPORTS.

WHAT THIS PACKAGE IS FOR, AND THE ONE SENTENCE IT EXISTS TO KEEP TRUE. F1b already
produces this lakehouse's payment corpus -- five declared profiles, deterministic from
(seed, stream_id, pool), with duplicates, late arrivals and schema drift injected on
purpose and six golden digests pinned over the result. F5 does not generate a second
one. It REPLAYS that corpus through Redpanda, so that every count F1b published against
the file path -- 150 redeliveries, 100 late arrivals, 2,000 drifted rows -- still
describes the stream when the stream arrives as Kafka records instead of as a file.

SO THE PRODUCER IS A TRANSPORT, AND THAT IS A CONSTRAINT RATHER THAN A DESCRIPTION.
Two things it is not:

  1. NOT A SECOND GENERATOR. Nothing here derives a record. `opl.generator` is a pure
     function of its arguments and `opl.generator.defects.delivered_records` is the
     whole delivery; this package takes what that returns, in the order it returns it.
  2. NOT A SECOND SERIALISER. `opl.generator.events.to_jsonl` decided `separators`,
     `ensure_ascii` and the `\\n` terminator as byte-identity decisions rather than as
     defaults, and `opl.bronze.generated_landing.serialised_bytes` decided the UTF-8
     encoding and refuses a carriage return. `opl.streaming.messages` calls that
     function and contains no `json` import at all. A `json.dumps` anywhere under this
     package would be the defect class this repository polices hardest, arriving
     through the one door F1b did not lock.

THE SPLIT BETWEEN THE TWO MODULES IS "WHAT THE BYTES ARE" AGAINST "HOW THEY TRAVEL",
and it is drawn where it is so that the byte-identity claim can be asserted with NO
BROKER RUNNING. `messages` is pure -- records in, frames out -- so the test that
compares a published payload against F1b's golden digest is a unit test that runs in
CI's default invocation today, before T6 gives CI a Redpanda service container.
`producer` is the half that opens a socket, reads credentials from the environment and
counts what the broker acknowledged; its tests need the container and carry the
`integration` marker, which `addopts` deselects. Had the two lived in one module, the
phase's headline property would have been provable only where a broker was running --
which is the same shape as a claim that is checked nowhere.

--- AND THE CONSUMING HALF, WHICH IS THREE MODULES SPLIT ON A DIFFERENT SEAM -------------

`ingest` reads the topic back through Structured Streaming into a local Delta table, and
`exactly_once` is F5's HEADLINE: two `foreachBatch` arms over one fault and the same
offsets, differing only in whether the append carries Delta's `txnAppId` / `txnVersion`.

The seam between those two is "the sink that is correct BY CONSTRUCTION" against "the sink
where the guarantee has to be earned", and it is drawn there because the first cannot prove
anything about the second. `ingest.write_payment_stream` is `format("delta")`, which is
exactly-once whatever happens -- including when nothing ran -- so an experiment on it
reports success under every outcome. `exactly_once` is where the batch is written by user
code and a replay can double-write, which is the only place the property is falsifiable.
Its NAIVE arm exists to be falsified, and if that arm ever stops duplicating, the phase has
proven that the fault missed its window rather than that anything is exactly-once.

`managed_broker` IS NOT A FOURTH CONSUMER. It reads nothing and lands nothing: it holds the
two things that differ when `ingest` is pointed at the MANAGED cluster instead of the local
container -- the JVM client's spelling of a SASL_SSL credential, and the row floor a run
declares at launch. The T8 job composes it with `ingest`; it does not replace it, and if it
ever grows a read of its own then this package has the second spelling of ingest that F5's
plan spends a paragraph refusing.

`watermarked_dedup` IS THE THIRD, and it is cut on a question neither of those two asks:
not where the bytes go, but WHICH ROWS ARRIVE IN TIME TO COUNT. Its chain is
`withWatermark -> dropDuplicatesWithinWatermark` and its product is the DIFFERENCE between
one corpus read at two watermark delays. It sits DOWNSTREAM of the seam above rather than
across it -- it lands through `ingest.write_payment_stream` instead of replacing it -- and
that is the right way round precisely because what it measures is the watermark and not the
write, so a sink that is exactly-once by construction is one fewer thing in the way.

`lateness` IS NOT A FOURTH CONSUMER EITHER, on `managed_broker`'s model. It reads nothing
and lands nothing: it is the ARITHMETIC that places one of `watermarked_dedup`'s arguments
-- the two watermark delays -- over the delivered corpus and a rate limit, with no session,
no socket and no clock in it. That is the seam: a prediction which could only be computed
by starting the run it predicts is not a prediction, and this half imports no pyspark, so
the delays a phase publishes before its run are guarded by a test that needs no JVM. The
chain itself is NOT what was split, and must not be: `watermarked_dedup`'s own opening
paragraph is where that argument lives.
"""
