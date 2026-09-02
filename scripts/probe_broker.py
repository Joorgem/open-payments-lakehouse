"""Does the MANAGED Redpanda cluster still answer, TODAY? -- ADR 0019 Decision 6's dated
obligation, and ledger row `f5:980`.

F5 shipped a managed Redpanda cluster on a free trial expected to be deleted around
2026-09-03, and what the broker does after that date is the last thing F5 left unexercised.
The obligation is to write the answer FROM A PROBE AND NEVER FROM THE CALENDAR, because
"the trial expired", "the ACL was revoked", "the username is wrong" and "there is no route
from here" all produce ONE string -- ADR 0018's species, and the reason a date on a page is
not a measurement.

--- THE ONE THING THIS SCRIPT EXISTS TO GET RIGHT, AND IT IS NOT THE CONNECTION -----------

The connection is the easy half. The hard half is that A SUCCESS AND A FAILURE HERE ARE NOT
THE SAME KIND OF FACT, and an output that rendered them symmetrically would be misread by
everyone who did not write it:

  * a SUCCESS from this machine is CONCLUSIVE that the cluster answers. Metadata came back
    over a completed SASL_SSL / SCRAM-SHA-256 exchange, and nothing but a live cluster
    holding this credential produces that;
  * a FAILURE from this machine is NOT CONCLUSIVE that the cluster is gone. A home network
    is not a serverless task's network -- a captive portal, a blocked port, a resolver, an
    expired local credential and a deleted cluster all arrive here as one timeout.

So the verdict sentence is part of the OUTPUT rather than of this docstring, and
`tests/test_probe_broker.py` holds it there. A reader six months from now must be able to
read a pasted line correctly WITHOUT having found this file.

That is also why "could not ask" is a THIRD outcome with its own exit code rather than a
failure. A run with no credential, or without the client library, sent nothing to the
cluster; reporting it beside a real timeout would manufacture the confusion above.

--- THE CREDENTIAL COMES FROM THE ENVIRONMENT, NOT FROM THE DATABRICKS SECRET SCOPE -------

Both work -- the two scope values ARE readable from this machine, measured through the CLI
on 2026-09-02 -- and the environment is chosen deliberately, on this script's own subject.
Reading the scope puts the WORKSPACE on the path of a measurement about the BROKER: an
expired PAT, a revoked ACL on the scope, or a workspace outage would then arrive here as a
failure to probe, which is a fifth world for the one string ADR 0018 is about. The
environment keeps the failure surface confined to the thing being measured. `.env` carries
`KAFKA_BOOTSTRAP` / `KAFKA_USER` / `KAFKA_PASSWORD` and is a copy of the same two scope
values (measured the same day: identical lengths, and both were readable).

`opl.streaming.producer.broker_from_environment` is what reads them, rather than three
`os.environ` lookups here. It already refuses a blank bootstrap, a PADDED bootstrap and
half a credential, each with the diagnosis that failure would otherwise not carry -- and a
second spelling of a credential is the thing `opl.streaming.managed_broker` exists to
prevent one layer up.

--- TWO SECRETS, ONE CHOKE POINT, AND TWO INDEPENDENT DEFENCES ----------------------------

The PASSWORD is a secret and so is the BOOTSTRAP HOST -- that is this repository's own
line, drawn in `databricks/src/stream_managed_broker.py`: the host names a cluster that
must not be committed to a public repository. Every line this script prints goes through
`emit`, which is the ONLY `print` in the file, and `tests/test_probe_broker.py` holds that
by parsing this module rather than by grepping it.

`Redactor` removes the exact values AND anything shaped like a network address. The shape
rule is not decoration: the metadata reply carries the cluster's OTHER broker hosts, which
are not the bootstrap string and would survive an exact-value rule; so would the `ipv4#...`
literal in a connect failure. This file therefore keeps NO DOTTED TOKEN IN ITS OWN
VOCABULARY, because the choke point cannot tell this script's prose from the broker's text
and must not have to.

AND THE SECOND DEFENCE IS AT THE FILE DESCRIPTOR, because the first one is a keyword
argument away from being deleted. MEASURED on 2026-09-02, `confluent-kafka` 2.6.1 against
an unresolvable host: with no `logger=`, the C library writes the bootstrap host to fd 2
ITSELF, from its own thread, three times per line (`%3|...|FAIL|... Failed to resolve
'<host>:9092'`) -- 715 characters of it, which no Python-level redaction can see. Passing
`logger=` reduced that to zero bytes; so did `log_level=0`; an `error_cb` alone did NOT.
So the client gets a logger AND fd 2 is redirected for the client's whole lifetime, with
whatever lands there re-emitted through the choke point.

The logger is not a formality either, though today it is a sink: the same measurement found
that librdkafka's log queue is never served by `list_topics`, so ZERO lines reach it. It
carries a redacting handler and `propagate = False` regardless, because a delivered line
that reached the root logger would go to stderr unredacted, and "zero lines" is a fact
about a version rather than a contract.

--- EXIT CODES, WHICH ARE THE ASYMMETRY IN THE ONE PLACE A SCRIPT CAN BE READ BY A MACHINE

    0  the cluster answered            -- conclusive
    1  the cluster did not answer      -- NOT conclusive that it is gone
    2  no reading was taken at all     -- nothing was sent; not a reading in either
                                          direction

Usage, from the repository root:

    set -a && source .env && set +a
    uv run python scripts/probe_broker.py
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import tempfile
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

from opl.streaming.producer import (
    BOOTSTRAP_ENV_VAR,
    PASSWORD_ENV_VAR,
    BrokerConfig,
    broker_from_environment,
)

EXIT_ANSWERED = 0
EXIT_NO_ANSWER = 1
EXIT_NO_READING = 2

# How long the metadata request may take. The control arm returned in 1.5 seconds, so this
# is a ceiling with a wide margin rather than a guess -- and it is a CEILING: a healthy
# cluster returns as soon as the exchange completes.
DEFAULT_TIMEOUT_SECONDS = 15.0

# Placeholders carry no dot, so the shape rule below cannot rewrite the redaction it just
# made into a second one.
PASSWORD_PLACEHOLDER = "<SASL-password-redacted>"
BROKER_PLACEHOLDER = "<broker-address-redacted>"
ADDRESS_PLACEHOLDER = "<network-address-redacted>"

# Anything SHAPED like a network address, whatever its value. This is what covers the hosts
# this script has never seen: the sibling brokers a metadata reply names, and the `ipv4#...`
# literal librdkafka puts in a connect failure. Over-redaction is the safe direction, and
# it is paid for by keeping this file's own vocabulary free of dotted tokens.
ADDRESS_SHAPE = re.compile(
    r"(?:[A-Za-z0-9_-]+\.)+[A-Za-z][A-Za-z0-9-]+(?::\d{1,5})?"  # a dotted host name
    r"|\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?"  # an IPv4 literal
    r"|\[[0-9A-Fa-f:]+\](?::\d{1,5})?"  # an IPv6 literal in brackets
)

# How much foreign text survives into one line. Applied AFTER redaction, never before.
FOREIGN_TEXT_LIMIT = 400

# The two verdicts, as SENTENCES IN THE OUTPUT. They are constants so that the property
# "the asymmetry is published" is a thing a test can hold, rather than a comment.
ANSWERED_MEANS = (
    "READING -- CONCLUSIVE: the managed cluster ANSWERED this machine. Metadata came back "
    "over a completed SASL_SSL / SCRAM-SHA-256 exchange, which nothing but a live cluster "
    "holding this credential produces. The cluster was alive at the timestamp above."
)
NO_ANSWER_MEANS = (
    "READING -- NOT CONCLUSIVE: the managed cluster DID NOT ANSWER this machine, and that "
    "is NOT evidence that the cluster is gone. A home network is not a serverless task's "
    "network: an expired trial, a revoked ACL, a wrong credential, a blocked port, a name "
    "that no longer resolves and a deleted cluster all arrive here as one timeout. Do not "
    "write 'the broker is gone' from this line. Write that it did not answer from this "
    "machine, on this date, in those words."
)
NO_READING_MEANS = (
    "NO READING -- nothing was sent to the cluster, so this line says nothing about it in "
    "either direction. It is not a success and it is NOT a failure to be quoted as one. "
    "Load the local credential (set -a && source .env && set +a) and run this again."
)


@dataclass(frozen=True)
class Redactor:
    """Removes the two secrets by value, and any network address by shape.

    NOT KEYED TO ONE OF THEM, and that is the defect `probe_postgres_session.redacted_dsn`
    found in itself: a redaction written against the value the author happened to have
    passes every test and leaks every other value. Both secrets are read from the
    environment here and both are registered, so the rules follow whatever is set.

    LONGEST VALUE FIRST, so the full `host:port` is replaced before the bare host and the
    output names which of the two it withheld rather than leaving a `:9092` behind.
    """

    pairs: tuple[tuple[str, str], ...] = ()

    @classmethod
    def of(cls, *, bootstrap: str, password: str) -> Redactor:
        """A redactor for one credential. A blank value is DROPPED, never registered.

        Registering `""` would replace at every position in every string, which destroys
        the output while protecting nothing -- and "no credential is set" is a state this
        script reaches on purpose, on any machine with no local `.env` file."""
        host = bootstrap.rsplit(":", 1)[0]
        candidates = (
            (password, PASSWORD_PLACEHOLDER),
            (bootstrap, BROKER_PLACEHOLDER),
            (host, BROKER_PLACEHOLDER),
        )
        kept = tuple((value, mask) for value, mask in candidates if value.strip())
        return cls(tuple(sorted(kept, key=lambda pair: len(pair[0]), reverse=True)))

    def __call__(self, text: str) -> str:
        cleaned = text
        for value, mask in self.pairs:
            cleaned = cleaned.replace(value, mask)
        return ADDRESS_SHAPE.sub(ADDRESS_PLACEHOLDER, cleaned)


def emit(redactor: Redactor, line: str) -> None:
    """THE ONLY `print` IN THIS FILE, which is the property that makes the redaction a
    guarantee rather than a habit.

    A second print site is how a leak gets added by an edit that looks harmless, so
    `tests/test_probe_broker.py` parses this module and refuses any other one."""
    print(redactor(line))


def foreign(text: str, redactor: Redactor, *, limit: int = FOREIGN_TEXT_LIMIT) -> str:
    """Text that came from outside this file: redacted, flattened onto one line, capped.

    REDACTED BEFORE IT IS CAPPED, NEVER AFTER. Cutting first can leave HALF a host or half
    a password in the output, which the exact-value rules would then no longer match --
    the same defect `probe_postgres_session.redacted_dsn` names, where `b'`, half of a
    quoted password, reached stdout because only the first token started with `password=`.

    FLATTENED because the client library's own lines are multi-line and this output is
    meant to be pasted into an evidence document as it stands."""
    flattened = " ".join(redactor(text).split())
    if len(flattened) <= limit:
        return flattened
    withheld = len(flattened) - limit
    return f"{flattened[:limit]} [... {withheld} more characters withheld]"


class RedactingHandler(logging.Handler):
    """Where the client library's log lines go IF a release ever delivers them.

    MEASURED on 2026-09-02 with `confluent-kafka` 2.6.1: it delivers NONE, because nothing
    serves librdkafka's log queue during a `list_topics` call. This handler exists anyway,
    because that measurement is about a version and not about a contract, and because the
    alternative -- letting the record reach the root logger -- puts it on stderr unredacted.
    """

    def __init__(self, redactor: Redactor) -> None:
        super().__init__()
        self.redactor = redactor

    def emit(self, record: logging.LogRecord) -> None:
        # `emit` here is the module-level choke point, not this method: a method name is a
        # class attribute and does not shadow a global inside the body.
        emit(self.redactor, f"probe_broker:   client log | "
                            f"{foreign(record.getMessage(), self.redactor)}")


def client_logger(redactor: Redactor) -> logging.Logger:
    """A logger the client library writes to INSTEAD of writing to fd 2.

    `propagate = False` is the load-bearing line: the root logger's last-resort handler
    writes to stderr, so a delivered record that propagated would leak the host through the
    one path this script cannot redact. Handlers are REPLACED rather than appended, because
    `getLogger` returns a process-global object and a second call would otherwise print
    every line twice."""
    logger = logging.getLogger("probe_broker.client")
    logger.handlers = [RedactingHandler(redactor)]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    return logger


@contextmanager
def stderr_captured(redactor: Redactor) -> Iterator[list[str]]:
    """fd 2 redirected onto a temporary file for the block; what lands there comes back
    REDACTED, in the yielded list, once the block has closed.

    AT THE FILE DESCRIPTOR, because that is the only layer the C library cannot write
    underneath. Reassigning `sys.stderr` does not move fd 2, and librdkafka writes to fd 2
    from its own thread -- measured: 715 characters carrying the bootstrap host three times,
    from a client built with no logger.

    Decoded with `errors="replace"`: the measured line contained an OS error message in the
    machine's own code page, and a decode failure here would be a crash inside the very
    block that exists to keep a secret out of the output."""
    saved = os.dup(2)
    sink = tempfile.TemporaryFile()
    captured: list[str] = []
    try:
        os.dup2(sink.fileno(), 2)
        yield captured
    finally:
        os.dup2(saved, 2)
        os.close(saved)
        sink.seek(0)
        raw = sink.read().decode("utf-8", errors="replace")
        sink.close()
        captured.extend(foreign(line, redactor) for line in raw.splitlines() if line.strip())


@dataclass(frozen=True, kw_only=True)
class Reading:
    """One metadata request and what came back. `failure` is ALREADY REDACTED.

    A frozen record rather than four prints inline, so `render` is a pure function of a
    measurement and a test can state an output for a reading with no broker anywhere."""

    taken_at: str
    elapsed_seconds: float
    broker_count: int | None = None
    topics: tuple[str, ...] = ()
    failure: str | None = None
    failure_code: str | None = None

    @property
    def answered(self) -> bool:
        return self.failure is None


def error_code(failed: BaseException) -> str:
    """The client library's OWN name for what went wrong, beside the exception type.

    `KafkaException`'s first argument is a `KafkaError` whose `name()` is an enumerated
    value -- `_TIMED_OUT`, `_TRANSPORT`, `_AUTHENTICATION`, `SASL_AUTHENTICATION_FAILED`.
    It is the one part of a failure that separates worlds the message text does not, and
    being enumerated it is safe to print whole."""
    first = failed.args[0] if failed.args else None
    name = getattr(first, "name", None)
    if callable(name):
        return f"{type(failed).__name__} / {name()}"
    return type(failed).__name__


def cluster_metadata(broker: BrokerConfig, redactor: Redactor, *, timeout: float):
    """One metadata request, through a client that CANNOT publish.

    An admin client and not a producer: this probe reads, the topic it would reach holds a
    corpus whose counts are quoted in three documents, and a client with a `produce` method
    on it is a client an edit can be careless with. It takes the same
    `BrokerConfig.client_config()` a producer takes -- measured, it accepts every key --
    so the credential keeps ONE spelling."""
    from confluent_kafka.admin import AdminClient

    client = AdminClient(broker.client_config(), logger=client_logger(redactor))
    return client.list_topics(timeout=timeout)


def probe(broker: BrokerConfig, redactor: Redactor, *, timeout: float) -> tuple[Reading, list[str]]:
    """Ask once, time it, and let NO exception out of this function unredacted.

    THE `except` IS BROAD ON PURPOSE and it changes no outcome: every failure here is
    already "the cluster did not answer", and what the breadth buys is that the text
    reaching the output went through `foreign` rather than through the interpreter's
    traceback printer. Assume the exception text contains the host -- it does."""
    taken_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    started = time.monotonic()
    with stderr_captured(redactor) as captured:
        try:
            metadata = cluster_metadata(broker, redactor, timeout=timeout)
        except Exception as failed:  # noqa: BLE001 -- see the docstring
            reading = Reading(
                taken_at=taken_at,
                elapsed_seconds=time.monotonic() - started,
                failure=foreign(str(failed), redactor),
                failure_code=error_code(failed),
            )
        else:
            reading = Reading(
                taken_at=taken_at,
                elapsed_seconds=time.monotonic() - started,
                broker_count=len(metadata.brokers),
                topics=tuple(sorted(metadata.topics)),
            )
    return reading, captured


def render(reading: Reading, captured: Sequence[str] = ()) -> list[str]:
    """The lines a human pastes into an evidence document, verdict included.

    THE VERDICT IS A LINE, not a comment and not an exit code alone, because the thing that
    gets pasted is the text. Nothing here restates the control arm: a prior measurement
    reprinted by a measuring tool is how a control arm gets read as a fresh reading."""
    head = f"probe_broker: taken at {reading.taken_at} from this developer machine"
    if reading.answered:
        body = [
            f"probe_broker: RESULT -- METADATA RETURNED in {reading.elapsed_seconds:.2f}s",
            f"probe_broker:   brokers | {reading.broker_count}",
            f"probe_broker:   topics  | {len(reading.topics)} {list(reading.topics)}",
            f"probe_broker: {ANSWERED_MEANS}",
        ]
    else:
        body = [
            f"probe_broker: RESULT -- NO METADATA after {reading.elapsed_seconds:.2f}s",
            f"probe_broker:   error   | {reading.failure_code}",
            f"probe_broker:   detail  | {reading.failure}",
            f"probe_broker: {NO_ANSWER_MEANS}",
        ]
    return [head, *body, *[f"probe_broker:   client said | {line}" for line in captured]]


def require_client_library() -> None:
    """Refuse an absent client library EARLY, and refuse it as NO READING.

    Deliberately not inside `probe`'s own `except`: an uninstalled wheel counted as "the
    cluster did not answer" is exactly the confusion this script exists to prevent, since
    nothing was asked."""
    try:
        import confluent_kafka  # noqa: F401
    except ImportError as absent:
        raise ImportError(
            "this probe needs `confluent_kafka`, which this repository declares in the "
            "`dev` dependency group only -- it is not a dependency of the opl wheel. "
            "Install it with `uv sync --all-groups`, then run this again."
        ) from absent


def require_a_sasl_credential(broker: BrokerConfig) -> None:
    """Refuse a probe with no password, rather than sending PLAINTEXT to a SASL listener.

    `BrokerConfig` already refuses HALF a credential; this refuses NEITHER half, which it
    cannot, because a broker with no password is the local PLAINTEXT container and that is
    a legitimate configuration for the module it comes from. It is not legitimate here: the
    managed cluster is this script's whole subject, and a plaintext attempt against its
    SASL_SSL listener times out with the string a deleted cluster produces."""
    if broker.password is None:
        raise ValueError(
            f"{PASSWORD_ENV_VAR} is not set, so this probe would speak PLAINTEXT to the "
            "managed cluster's SASL_SSL listener and time out -- and that timeout reads "
            "exactly like a deleted cluster, which is the one confusion this probe exists "
            f"to prevent. Set {BOOTSTRAP_ENV_VAR} and {PASSWORD_ENV_VAR} together."
        )


def no_reading(redactor: Redactor, unusable: Exception) -> int:
    """Say that nothing was asked, and say what that does and does not mean."""
    emit(redactor, "probe_broker: NO READING TAKEN -- this machine could not even ask.")
    emit(redactor, f"probe_broker:   cause   | {type(unusable).__name__}: "
                   f"{foreign(str(unusable), redactor)}")
    emit(redactor, f"probe_broker: {NO_READING_MEANS}")
    return EXIT_NO_READING


def parse_arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="seconds the metadata request may take before it is called a non-answer",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    """Read the credential, ask once, print the reading, and return the asymmetry.

    THE REDACTOR IS BUILT BEFORE ANYTHING ELSE CAN RAISE, because the first thing that can
    raise is `BrokerConfig`, and one of its own refusals quotes the bootstrap address back
    at the operator -- so the guard against printing the host has to exist before the code
    that would print it."""
    arguments = parse_arguments(argv)
    environment = os.environ if env is None else env
    redactor = Redactor.of(
        bootstrap=environment.get(BOOTSTRAP_ENV_VAR, ""),
        password=environment.get(PASSWORD_ENV_VAR, ""),
    )
    try:
        broker = broker_from_environment(environment)
        require_a_sasl_credential(broker)
        require_client_library()
    except (ValueError, ImportError) as unusable:
        return no_reading(redactor, unusable)
    reading, captured = probe(broker, redactor, timeout=arguments.timeout)
    for line in render(reading, captured):
        emit(redactor, line)
    return EXIT_ANSWERED if reading.answered else EXIT_NO_ANSWER


if __name__ == "__main__":
    sys.exit(main())
