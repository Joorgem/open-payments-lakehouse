# src/opl/bronze/generated_landing.py
"""The one place a generated stream becomes BYTES ON DISK, and the one place the
byte-identity property F1b rests on can be defeated.

UNDER `opl.bronze` AND NOT UNDER `opl.generator`, AND THAT WAS A FINDING RATHER THAN A
CHOICE. It was written as `opl.generator.emit` and
`tests/test_payment_generator.py::test_the_generator_reads_no_clock_and_no_file` went
red on it immediately: that sweep walks the WHOLE generator package and bans `os`,
`open` and every ambient-value constructor, because the generator's entire value is
that its output is a function of its arguments. A writer is I/O by definition, so it
cannot live there without either weakening the purity guard or exempting one file from
it -- and an exemption is how a property stops being one.

Here it sits beside its real sibling. `opl.bronze.unzip_volume` does the same job for
the file-fed sources: prepare a landing directory for the Auto Loader that is about to
read it, staging through a tmp dir outside every watched path and `os.replace`-ing the
whole file in. Same layer, same mechanism, same package.

THE WHOLE PHASE IS A STATEMENT ABOUT BYTES. `tests/test_payment_generator.py` pins
sha256 `b45f1dc7...` over 7,028 bytes of serialised text, and
`tests/test_payment_defects.py` pins five more. Every one of those digests is taken
over `to_jsonl(...).encode("utf-8")` -- over TEXT that a test encoded itself. Nothing
in that chain says anything about what a WRITER puts in a file, and on Windows the
default is that they differ: Python's text mode translates `\\n` to `\\r\\n`, so a
`open(path, "w").write(text)` ships a file whose every line ends in two bytes where
the pinned digest counted one. Six golden pins stay green and the artefact that
lands in the Volume is not the artefact they describe.

SO THIS MODULE OPENS IN BINARY AND ENCODES UTF-8 EXPLICITLY, and neither half is
optional:

  - BINARY (`"wb"`), so no newline translation exists to be configured. `newline=""`
    on a text handle would also work and is deliberately NOT what this does: it
    leaves the file's encoding to `locale.getpreferredencoding()`, which on this
    dev box is cp1252 -- a razao social arriving in a future column would then be
    written in a different encoding on Windows than on Databricks, which is the same
    class of defect one layer down.
  - UTF-8 EXPLICITLY, because `opl.bronze.reader.jsonl_read_options` declares
    `encoding=UTF-8` on the read side, and a writer whose encoding comes from the
    machine's locale would make that declaration a coincidence.

AND IT VERIFIES, rather than trusting either of those. `_write_verified` reads the
file it just wrote back AS BYTES and refuses if they are not the bytes it serialised.
That is not belt-and-braces: this repository has a documented case of a file-rewrite
step that passed a CONTENT comparison and still left the file byte-changed, because
the comparison read the file in text mode and the difference it was looking for is
exactly what text mode normalises away. A check that can only be written one way is
worth more than a rule someone has to remember.

IDEMPOTENT, BECAUSE `max_retries: 0` DOES NOT PREVENT A RETRY. A repair run, or a
re-run of the generate task alone, calls this again with the same (seed, stream_id,
pool) and therefore the same bytes. Three outcomes, and each is a different fact:
the file is absent (write it), the file is there and identical (say so and touch
nothing -- the generator is deterministic, so this is what a retry looks like), or
the file is there and DIFFERENT (refuse: something changed the derivation, and
overwriting would silently re-stamp a landing directory whose files an Auto Loader
checkpoint already records as read).

NOTHING HALF-WRITTEN EVER EXISTS IN THE LANDING DIR. The payload goes to
`tmp_directory` -- `OplConfig.landing_generated_tmp`, which is outside every
directory a stream reads -- and is `os.replace`d in once it is whole and verified.
`opl.bronze.unzip_volume` has done exactly this on Databricks since F1.3 and its
docstring carries the reason: cloudFiles walks a source dir RECURSIVELY and with no
glob, so a partial file sitting in the landing dir is a file the ingest reads.
`os.replace` works between the two because one UC Volume is one FUSE mount; a system
temp dir is a different device and would raise EXDEV.
"""
from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path

from opl.generator.events import to_jsonl
from opl.generator.stream import StreamSpec

# One record per line, UTF-8, `\n`-terminated. `.jsonl` rather than `.json` because
# the file is NOT one JSON document: `opl.bronze.reader.jsonl_read_options` sets
# `multiLine=false`, and a reader that guessed from the extension would be right.
STREAM_FILE_SUFFIX = ".jsonl"


@dataclass(frozen=True, kw_only=True)
class EmittedFile:
    """What one call to `emit_stream_file` put in the Volume, or found already there.

    THE DIGEST AND THE BYTE COUNT ARE THE EVIDENCE, not decoration: they are the same
    two numbers the golden pins are stated in, so a workspace run's log line and a
    local test assertion are directly comparable without anyone re-deriving either.
    `row_count` is the third because a bare digest mismatch names nothing -- which of
    the three moved is itself the diagnosis (see the defect pins' own docstring)."""

    path: str
    row_count: int
    byte_count: int
    sha256: str
    was_already_there: bool


def filename_for(spec: StreamSpec) -> str:
    """The landing filename for `spec`'s stream: its id, and nothing else.

    THE STREAM ID IS THE FILENAME STEM, which `opl.generator.stream` anticipated
    where it declares the id's alphabet -- A-Z, 0-9, `-`, `_`, no separators, "the
    natural filename stem for whatever writes the stream out". So the name needs no
    sanitising here: `StreamSpec.__post_init__` has already refused anything that is
    not one path component, in the same guard that stops two ids differing only in
    case from deriving one stream.

    NOT THE MONTH, NOT THE BATCH, NOT A TIMESTAMP. The directory already carries the
    month, and a run-scoped component would make the landing filename different on
    every re-run -- which turns the idempotence below into an append: Auto Loader
    tracks files by PATH, so the same rows under a new name are new rows, and the
    promote's `_batch_id` idempotence cannot see it. One stream, one file."""
    return f"{spec.stream_id}{STREAM_FILE_SUFFIX}"


def serialised_bytes(records: Sequence[Mapping[str, str]]) -> bytes:
    """`records` as the exact bytes that belong on disk. THE crossing into bytes.

    `to_jsonl` joins on a literal `"\\n"` and this encodes UTF-8, so these are the
    bytes every golden digest in this phase was taken over -- stated once, here, so
    that a caller cannot serialise one way and write another.

    The `\\r` refusal is a guard on the OUTPUT rather than a restatement of the join.
    `json.dumps` escapes a carriage return inside a VALUE as the two ASCII characters
    `\\` and `r`, so no CR can reach these bytes through a value, and `to_jsonl`
    cannot introduce one through the separator. What it catches is a future edit that
    reintroduces `os.linesep` at either end -- the whole class this phase's
    determinism claim is vulnerable to, refused where the bytes are made rather than
    only in a test."""
    payload = to_jsonl(records).encode("utf-8")
    if b"\r" in payload:
        raise ValueError(
            "the serialised stream contains a carriage return, so its bytes are not "
            "the bytes the golden digests were taken over. `events.to_jsonl` joins on "
            "a literal '\\n'; an `os.linesep` join or a text-mode write would produce "
            "'\\r\\n' on Windows and '\\n' on Databricks, making byte-identity true "
            "only within one operating system."
        )
    return payload


def _write_verified(payload: bytes, path: Path) -> None:
    """Write `payload` to `path` in binary, then READ IT BACK AS BYTES and compare.

    THE READ-BACK IS THE POINT AND IT IS NOT PARANOIA. A text-mode read of the same
    file would normalise `\\r\\n` back to `\\n` and report success over a file that is
    byte-different -- which is precisely how a "restore the original" step in this
    repository passed a content comparison while leaving the file changed. Reading
    bytes is the only comparison that can see the defect the binary write exists to
    prevent, so the write and its proof are one function."""
    with open(path, "wb") as handle:  # binary: no newline translation can exist
        handle.write(payload)
    written = path.read_bytes()
    if written != payload:
        raise OSError(
            f"{path} holds {len(written)} bytes and {len(payload)} were serialised "
            f"(sha256 {hashlib.sha256(written).hexdigest()} vs "
            f"{hashlib.sha256(payload).hexdigest()}). The file on disk is not the "
            "artefact the golden digests describe -- the usual cause is a text-mode "
            "write translating '\\n' into '\\r\\n'."
        )


def _refuse_a_different_stream(existing: bytes, payload: bytes, path: Path) -> None:
    """Refuse a landed file whose bytes are not the ones this run would write.

    A RE-RUN IS EXPECTED AND IS NOT THIS. The generator is a pure function of
    (seed, stream_id, pool), so a repair run produces identical bytes and the caller
    treats that as a no-op. Different bytes under the same name mean the derivation,
    the pool or the profile moved -- and overwriting would silently replace a file an
    Auto Loader checkpoint may already record as read, so the new rows would never be
    ingested while the old rows stay in bronze describing a stream that no longer
    exists anywhere."""
    raise ValueError(
        f"{path} already holds a different stream: {len(existing)} bytes, sha256 "
        f"{hashlib.sha256(existing).hexdigest()}, against the {len(payload)} bytes "
        f"(sha256 {hashlib.sha256(payload).hexdigest()}) this run derived. The "
        "generator is deterministic, so identical inputs give identical bytes -- a "
        "difference means the seed, the pool or the profile changed. Auto Loader "
        "tracks files by path and may already have read this one, so overwriting it "
        "would leave bronze describing a stream that no longer exists. Land the new "
        "stream under a new stream_id, or delete this file deliberately."
    )


def emit_stream_file(
    records: Sequence[Mapping[str, str]],
    spec: StreamSpec,
    *,
    directory: str | Path,
    tmp_directory: str | Path,
) -> EmittedFile:
    """Put `records` in `directory` as `spec`'s one landing file, idempotently.

    `directory` is `OplConfig.landing_generated_table(...)` and `tmp_directory` is
    `landing_generated_tmp(...)`; both are required and neither is defaulted, for
    `unzip_volume.unzip_dir`'s reason -- this module cannot know which directories an
    Auto Loader reads, and getting that wrong puts a half-written file where a stream
    will ingest it."""
    payload = serialised_bytes(records)
    destination = Path(directory) / filename_for(spec)
    digest = hashlib.sha256(payload).hexdigest()
    landed = EmittedFile(
        path=str(destination),
        row_count=len(records),
        byte_count=len(payload),
        sha256=digest,
        was_already_there=False,
    )
    os.makedirs(directory, exist_ok=True)
    if destination.exists():
        existing = destination.read_bytes()
        if existing != payload:
            _refuse_a_different_stream(existing, payload, destination)
        return replace(landed, was_already_there=True)
    os.makedirs(tmp_directory, exist_ok=True)
    staged = Path(tmp_directory) / filename_for(spec)
    _write_verified(payload, staged)
    # Atomic within the Volume's one FUSE mount, so the landing dir goes from holding
    # nothing to holding the whole file. Never a partial one, which cloudFiles would
    # discover and ingest as if it were complete.
    os.replace(staged, destination)
    return landed
