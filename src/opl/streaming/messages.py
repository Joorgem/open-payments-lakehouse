# src/opl/streaming/messages.py
"""The frames a Kafka producer publishes, stated as an EQUALITY against the bytes the
file path lands rather than as a second serialisation of the same records.

--- THE EQUIVALENCE, SPELLED SO IT CAN BE CHECKED RATHER THAN BELIEVED ------------------

    b"".join(message_values(records)) == generated_landing.serialised_bytes(records)

Byte for byte. Every message value is one `\\n`-TERMINATED JSON line -- the same line, at
the same position, that the `.jsonl` file holds -- so the ordered concatenation of what
this publishes IS the file, and the MULTISET of message values IS the multiset of the
file's terminated lines. `message_values` does not merely promise that: it verifies it on
every call, against a `serialised_bytes` of the whole sequence, and refuses otherwise --
for `generated_landing._write_verified`'s reason exactly, that this repository has a
documented case of a file-rewrite step which passed a comparison and still left the file
byte-changed.

WHAT THAT REFUSAL IS NOT, SAID HERE SO NOBODY HAS TO INFER IT. It is not a check on the
SERIALISER. Both of its sides call `serialised_bytes`, so a serialiser that changed moves
both of them together and is accepted -- that class is caught by EXTERNAL pins, two of which
`tests/test_payment_streaming.py` re-asserts over the bytes the producer hands its client:
`b45f1dc7...`, which `tests/test_payment_generator.py` took over 24 records before Kafka was
in this repository, and `a527b61c...`, which `scripts/probe_byte_identity.py` committed over
`cross-currency`'s 10,000. `_refuse_frames_that_do_not_rebuild_the_file` states its own
smaller domain in its docstring, and it is smaller on purpose.

WHY BOTH FORMS ARE STATED, AND WHICH ONE BELONGS ON WHICH SIDE. A Kafka topic with more
than one partition has NO GLOBAL ORDER: a consumer reading three partitions may interleave
them any way the broker hands them over, so the strongest thing a CONSUMER can assert is
the multiset -- and that is the form
`tests/integration/test_payment_streaming_live.py` asserts after consuming, in
`test_a_three_partition_topic_returns_the_same_multiset`, over a topic that really has
three partitions and with a floor that more than one of them was used. NOTHING IN
`tests/test_payment_streaming.py` CONSUMES ANYTHING: that file is the produce side
throughout, and an earlier version of this paragraph named it here -- which put the
consumer's half of the claim in a file with no consumer in it. On the PRODUCE side the
order is known, because it is the order this function
returns and the order `producer.publish_records` hands to the client, so the stronger
ordered form is available there. It is also the only form that can be compared against a
GOLDEN DIGEST at all: a digest is taken over a byte string, and turning a multiset into a
byte string means choosing an order and a join rule. Choosing them here would be inventing
the file's framing a second time. Taking the produce order invents nothing.

--- THE TERMINATOR TRAVELS INSIDE THE MESSAGE VALUE, AND THAT IS THE DECISION -----------

Two spellings were available and the choice is not cosmetic.

  * VALUE = THE JSON LINE WITHOUT ITS `\\n`. Tidier on the wire -- a Kafka record is framed
    by its own length and needs no separator, and a replay would carry ONE FEWER BYTE PER
    RECORD -- 10,000 across `drifting`, the largest declared profile, and 50,150 across all
    five together. REFUSED: the file's bytes then stop being recoverable from the messages.
    Something, somewhere, has to put a `\\n` back between them, and that something is a
    SECOND SPELLING of `to_jsonl`'s "TERMINATED, NOT SEPARATED" decision -- which would
    end up living in the very test that exists to prove there is no second spelling. A
    test that re-implements the property it is checking passes on the day the property
    breaks.
  * VALUE = THE JSON LINE WITH ITS `\\n`. What this does. `b"".join` is then the entire
    reconstruction rule, no line-separator constant appears anywhere in this package, and
    every frame is produced by the SAME function that produces the file.

WHAT THE CHOICE COSTS, NAMED RATHER THAN WAVED AT: every value ends in one byte of JSON
whitespace -- 10,000 bytes across the largest declared profile's 2,989,447 (`drifting`,
measured by `scripts/probe_byte_identity.py`), which is 0.33%. JSON's grammar makes
whitespace before and after a value insignificant (RFC 8259 section 2), so a parser meets
the object and then end-of-input either way; `tests/test_payment_streaming.py` asserts
that over the real frames rather than citing the RFC and moving on. WHAT WOULD REVERSE THE
DECISION: a consumer that cannot tolerate it. The remedy would then be one strip at THAT
consumer, in the code that has the intolerance -- never moving the file's framing rule
into this package, which is how the two transports would start disagreeing about what a
record is.

--- WHY THE FRAMES ARE BUILT, NOT CUT --------------------------------------------------

The obvious alternative is `serialised_bytes(records).split(b"\\n")`: serialise the whole
file once and cut it into lines. REFUSED, because cutting means this module has to know
WHERE the frames are -- it would spell the terminator to split on, and then spell it again
to put back what `split` removed. Building them one record at a time spells it nowhere:
each frame is `serialised_bytes([record])`, the same call `generated_landing` makes for a
whole file, and the equality at the top is exactly the property `to_jsonl` already
documents -- "terminating is what makes concatenating two generated chunks produce a valid
file rather than one fused line". F1b wrote that sentence about an emitter writing more
than one chunk per stream. This is the same sentence at a chunk size of one record.

THE GUARD DOES CUT, AND THAT IS NOT THE SAME THING. `_refuse_frames_that_do_not_rebuild_the_file`
calls `splitlines` on the landed bytes to COUNT lines, never to produce one. `splitlines`
names no byte, so nothing is spelled a second time and no frame is ever taken from it -- what
it buys is the one disagreement the byte equality cannot express, and that function's
docstring says which.

--- AN EMPTY CORPUS IS REFUSED, AND THAT REFUSAL IS LOAD-BEARING ------------------------

`b"".join(()) == serialised_bytes(())` is `b"" == b""`. The verification above holds over
an empty sequence for reasons that have nothing to do with framing, so a publisher handed
nothing would report a verified stream and publish no records -- and every count this
phase rests on (the 150 redeliveries collapsed, the one late arrival dropped, the
duplicates the NAIVE arm produces) would then be zero, and true. That is F4's species in
its streaming costume: a check whose output cannot distinguish "passed" from "never ran".
So the floor is in the SHIPPED code and not only in the tests -- an empty corpus cannot be
published at all.
"""
from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence

from opl.bronze.generated_landing import serialised_bytes
from opl.contracts.payments import IDENTITY_COLUMN

# The key's encoding. `serialised_bytes` decides the VALUE's (UTF-8, explicitly, so that
# `opl.bronze.reader.jsonl_read_options`' declared `encoding=UTF-8` is not a coincidence),
# and the key is spelled here because Kafka carries it outside the value where no
# serialiser reaches it. It is not part of the byte-identity claim at all -- the file has
# no keys -- and every declared identity is a 64-character sha256 hexdigest, so the choice
# is moot in practice and stated anyway, because a future identity carrying an accent
# would otherwise pick up whatever `str.encode` defaults to.
KEY_ENCODING = "utf-8"


def message_values(records: Sequence[Mapping[str, str]]) -> tuple[bytes, ...]:
    """`records` as one Kafka message value each: the file's lines, terminators included.

    THE FRAMING, and the whole of it. See the module docstring for the equality this
    guarantees, why the terminator is inside the value, and why an empty corpus is a
    refusal rather than an empty result."""
    _refuse_an_empty_corpus(records)
    frames = tuple(serialised_bytes([record]) for record in records)
    _refuse_frames_that_do_not_rebuild_the_file(frames, records)
    return frames


def message_key(record: Mapping[str, str]) -> bytes:
    """`record`'s Kafka key: the contract's identity column, and nothing else.

    TRANSPORT METADATA, NOT CONTENT. The key never enters the byte-identity claim -- the
    landed file has no keys -- and a consumer rebuilding the corpus from keys AND values
    would be reading something the file does not contain.

    WHY A KEY AT ALL, RATHER THAN NULL. librdkafka's default partitioner sends a
    null-keyed record to a partition it picks itself, so which records share a partition
    -- and therefore which interleavings a consumer can observe -- would change between
    two replays of ONE corpus, on a phase whose every other input is deterministic. A
    keyed record goes to `murmur2(key) % partitions`, so the same corpus lays itself out
    the same way every time.

    AND WHY THE IDENTITY COLUMN IS THE RIGHT KEY, which is a stronger reason than
    determinism. A REDELIVERY CARRIES ITS ORIGINAL'S BYTES, `transaction_id` included, so
    keying on it puts every duplicate in the same partition as the row it repeats. T5's
    claim -- that the pipeline collapses exactly the 150 injected redeliveries and zero
    legitimate repeats -- then cannot depend on which partition a duplicate happened to
    land in. A legitimate repeat has its OWN identity, so it is free to land elsewhere,
    which is correct: it is a different payment."""
    try:
        return record[IDENTITY_COLUMN].encode(KEY_ENCODING)
    except KeyError:
        raise KeyError(
            f"a record reached the Kafka producer without its {IDENTITY_COLUMN!r} column. "
            "Every record `opl.generator.events.record_of` renders carries one, and a "
            "redelivery carries its original's -- so this is not a payment from this "
            "lakehouse's generator, and publishing it would put a null-keyed record in a "
            "corpus whose partition layout is supposed to be a function of the seed."
        ) from None


def _refuse_an_empty_corpus(records: Sequence[Mapping[str, str]]) -> None:
    """Refuse to frame nothing. See the module docstring's last section for why this is
    a refusal in shipped code rather than an assertion in a test."""
    if len(records) == 0:
        raise ValueError(
            "refusing to publish an empty record sequence. The byte-identity check below "
            "is `b'' == b''` over an empty corpus, so a publish of nothing would verify, "
            "report success, and leave every count this phase measures at zero and true "
            "-- a result indistinguishable from a run that never happened."
        )


def _refuse_frames_that_do_not_rebuild_the_file(
    frames: tuple[bytes, ...], records: Sequence[Mapping[str, str]]
) -> None:
    """Refuse frames whose concatenation is not the bytes the emitter would land.

    WHAT THIS DOES NOT COVER, STATED FIRST SO IT IS NOT MISTAKEN FOR MORE. Both sides call
    the SAME `serialised_bytes`, so a change to the SHARED serialiser moves them together and
    is ACCEPTED here. Measured on the pinned 24-record corpus: default `json` separators moved
    the emitted bytes from 7,028 to 7,388 and passed this check, and `sort_keys=True` moved the
    digest without moving a byte count and passed it too. The control for that class is EXTERNAL:
    `b45f1dc7...` from `tests/test_payment_generator.py` and `a527b61c...` from
    `scripts/probe_byte_identity.py`, both re-asserted over the published frames by
    `tests/test_payment_streaming.py`. A pin taken outside the code under test is what
    catches a serialiser that moved; this is smaller.

    WHAT IT DOES COVER is the stretch BETWEEN the two calls, which is this module's own code:
    a `frames` expression that stops iterating exactly `records` in exactly that order, an
    edit applied to the frames after building them, or a second serialisation written INSIDE
    `message_values` -- the defect `opl.streaming.__init__` calls the one door F1b did not
    lock. Each of those moves `rebuilt` and leaves `landed` alone.

    AND ONE SERIALISER PROPERTY, the only one the two routes disagree about: `to_jsonl`'s
    "TERMINATED, NOT SEPARATED". A SEPARATING join gives N one-record calls no separator at
    all against the whole-sequence call's N-1, so the byte strings differ. A terminator that
    vanished ENTIRELY does not -- both routes drop the same N bytes and agree on one fused
    line -- so the LINE COUNT is compared as well: `splitlines` cuts `landed` where a
    terminator put a boundary without this module naming the byte, and one piece against N
    frames is what the equality cannot see. (At one record both counts are 1 and it adds
    nothing; the pinned spec is 24 records and every declared profile is 10,000 or more.)"""
    rebuilt = b"".join(frames)
    landed = serialised_bytes(records)
    if rebuilt != landed:
        raise ValueError(
            f"the {len(frames)} Kafka frames rebuild {len(rebuilt)} bytes (sha256 "
            f"{hashlib.sha256(rebuilt).hexdigest()}) and the emitter would land "
            f"{len(landed)} (sha256 {hashlib.sha256(landed).hexdigest()}) for the same "
            f"{len(records)} records. The two transports would then carry different "
            "corpora, and every golden digest F1b pinned would describe only the file."
        )
    lines = len(landed.splitlines(keepends=True))
    if lines != len(frames):
        raise ValueError(
            f"the {len(frames)} Kafka frames rebuild the emitter's {len(landed)} bytes "
            f"exactly, and those bytes cut into {lines} lines. One frame is one line of the "
            "file, so a corpus that rebuilds and does not cut apart has lost its terminator "
            "on BOTH routes at once -- which the equality above structurally cannot see, "
            "because it drops the same bytes from each side. A consumer would find one "
            "fused line where the file has records."
        )
