# tests/test_producer_client_logging.py
"""THE CLIENT LIBRARY'S OWN LOG LINES CANNOT PUBLISH THE BOOTSTRAP ADDRESS.

`opl.streaming.producer` treats the bootstrap address as a secret -- ADR 0019 Decision 7's
"what reverses it" clause, and `docs/f5-run-evidence.md`'s "the BOOTSTRAP still is a secret,
deliberately". Everything that module prints, raises or repr's has been arranged around
that. librdkafka is not bound by any of it: it renders the address into its own log lines
and writes them to FILE DESCRIPTOR 2 from an internal thread, underneath every Python-level
defence there is. MEASURED on 2026-09-02, `confluent-kafka` 2.6.1, an invented `.invalid`
host, in the shape `_open_client` really opens a client in -- the host THREE TIMES on fd 2
by the moment `flush` returned, 10 fresh processes out of 10, and SIX times by the time the
client that wrote them had been destroyed.

WHICH IS WHY NO ARM BELOW ASSERTS A BYTE COUNT AS A NUMBER. What fd 2 holds is a function of
how long the client has lived and of how many characters the host's name has, so any figure
read at one instant is a floor -- the producer's own module docstring carries the full
timeline and the arithmetic. What does not move is the count of MENTIONS: never zero without
a defence, and never anything but zero with one.

WHAT THIS FILE LOCKS IS TWO DEFENCES AND THE JOIN BETWEEN THEM, because either alone is one
token away from being deleted and the AST arm alone would be the defect T1a found in
`tests/test_probe_broker.py`: a lock that asserts a KEYWORD IS PRESENT passes with a logger
that publishes the secret. So the two things `_open_client` hands the client are checked for
WHAT THEY DO -- a config whose `log_level` really is librdkafka's silence, and a logger that
really does write nowhere -- and the AST arm only ties those two verified objects to the
call site.

AND THE END-TO-END ARM CARRIES ITS OWN NEGATIVE CONTROL, in a second fresh process that
builds the client the way this module built it BEFORE the fix. Without that control, "zero
bytes on fd 2" is a sentence a broken harness, a missing client library and a machine whose
resolver answers differently all produce -- ADR 0018's species exactly. If the control does
not reproduce the leak the arm SKIPS rather than reporting a green it did not earn; the
other four arms need no broker and no client library and always run.

A FRESH SUBPROCESS PER ARM IS LOAD-BEARING AND NOT TIDINESS. The leak was measured to depend
on whether the compiled extension was already imported when fd 2 was redirected: pre-imported,
the same client wrote ZERO bytes, which is the split `scripts/probe_broker.py`'s docstring
records as a measurement protocol and deliberately states no cause for. Two arms in one
process would therefore give the second one a free pass, and pytest itself imports
`confluent_kafka` at collection time through `tests/integration/test_redpanda.py`.

EVERY VALUE HERE IS INVENTED. The host is in the reserved `.invalid` TLD (RFC 2606) and
cannot resolve anywhere; the password is a literal typed for this file. Nothing reads `.env`
or the environment -- a test that needed the real credential could not run in CI, and this
one has to.
"""
from __future__ import annotations

import ast
import io
import json
import logging
import os
import subprocess
import sys
import tempfile
import textwrap
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from opl.streaming.producer import (
    CLIENT_LOG_LEVEL_OPTION,
    CLIENT_LOGGER_NAME,
    BrokerConfig,
    _client_config_that_cannot_log,
    _discarding_client_logger,
)

_MODULE = Path(__file__).resolve().parents[1] / "src" / "opl" / "streaming" / "producer.py"

_HOST = "seed-t1c-lock.invalid"
_BOOTSTRAP = f"{_HOST}:9092"
_USERNAME = "opl"
_PASSWORD = "invented-not-the-real-one"

_BROKER = BrokerConfig(bootstrap=_BOOTSTRAP, username=_USERNAME, password=_PASSWORD)

# The line librdkafka writes, in its own shape, for the arms that do not open a client.
_CLIENT_LINE = f"%3|FAIL|rdkafka#producer-1| Failed to resolve '{_BOOTSTRAP}'"


@contextmanager
def _file_descriptor_2() -> Iterator[bytes]:
    """fd 2 redirected onto a temporary file for the block; the bytes arrive afterwards.

    AT THE FILE DESCRIPTOR, because that is the only layer a C library writing from its own
    thread cannot get underneath -- reassigning `sys.stderr` does not move fd 2. The yielded
    object is a `bytearray` filled on exit, so the block reads it after the `with`."""
    written = bytearray()
    saved = os.dup(2)
    sink = tempfile.TemporaryFile()
    try:
        os.dup2(sink.fileno(), 2)
        yield written
    finally:
        os.dup2(saved, 2)
        os.close(saved)
        sink.seek(0)
        written.extend(sink.read())
        sink.close()


# --------------------------------------------------------------------------------
# 1. THE FIRST DEFENCE: THE LINE IS NEVER RENDERED
# --------------------------------------------------------------------------------


def test_the_config_the_client_is_built_from_silences_the_library_at_the_source():
    """`log_level` is librdkafka's own syslog threshold, and 0 is the number that matters.

    THE VALUE IS ASSERTED, NOT THE KEY'S PRESENCE. At 0 only LOG_EMERG survives, so the
    `%3|...|FAIL|` line is never rendered at all -- which is what makes every layer below it
    unnecessary rather than merely redundant. Any other number lets the line be built, and a
    built line is one that has to be caught somewhere.

    AND NOTHING IS DROPPED ON THE WAY, which is the failure the second half refuses: a
    `_client_config_that_cannot_log` that silenced the library by rebuilding the mapping
    from scratch would take the SASL block with it and speak PLAINTEXT to a broker expecting
    SASL_SSL -- `_refuse_half_a_credential`'s exact subject, arrived at from the other end."""
    silenced = _client_config_that_cannot_log(_BROKER)
    assert silenced[CLIENT_LOG_LEVEL_OPTION] == 0, (
        f"the client is built with {CLIENT_LOG_LEVEL_OPTION}="
        f"{silenced[CLIENT_LOG_LEVEL_OPTION]!r}, which is above LOG_EMERG, so librdkafka "
        "still renders the line that names the bootstrap address three times"
    )
    plain = _BROKER.client_config()
    assert plain.items() <= silenced.items(), (
        f"silencing the library changed the credential: {sorted(plain.items() - silenced.items())}"
    )
    assert CLIENT_LOG_LEVEL_OPTION not in plain, (
        "`client_config` itself now carries the log level, which silences "
        "`scripts/probe_broker.py`'s admin client too -- that script's whole subject is "
        "publishing what the broker said, redacted, through its own choke point"
    )


# --------------------------------------------------------------------------------
# 2. THE SECOND DEFENCE: A LINE THAT IS RENDERED REACHES NOTHING THAT WRITES
# --------------------------------------------------------------------------------


def test_a_logger_with_no_handler_at_all_publishes_the_address_to_stderr(monkeypatch):
    """THE POSITIVE CONTROL FOR THE ARM BELOW, and the reason its `NullHandler` exists.

    `propagate = False` with an empty handler list looks like the obvious way to silence a
    logger and is not one: `logging.Logger.callHandlers` falls back to `logging.lastResort`
    when a record reaches NO handler anywhere, and `lastResort` is a stderr handler at
    WARNING. So the shape a reader would call "suppressed" publishes librdkafka's ERROR
    lines to stderr -- the exact leak the module is closing, reintroduced by the fix for it.

    Held here rather than asserted in prose, so that deleting the `NullHandler` from
    `_discarding_client_logger` cannot look like a simplification."""
    naive = logging.getLogger("test_producer_client_logging.naive")
    naive.handlers = []
    naive.propagate = False
    buffered = io.StringIO()
    monkeypatch.setattr(sys, "stderr", buffered)
    naive.error(_CLIENT_LINE)
    assert _HOST in buffered.getvalue(), (
        "`logging.lastResort` did not write to `sys.stderr`, so this control cannot show "
        "what the `NullHandler` in `_discarding_client_logger` is for"
    )


def test_the_logger_the_client_is_handed_writes_the_line_nowhere(monkeypatch):
    """WHAT THE LOGGER IS FED IS THE PROPERTY, not that `logger=` was spelled.

    Three ways for the line to escape, all closed and all checked by driving a real record
    through the real logger: `sys.stderr` (where `lastResort` writes, and where a
    `StreamHandler` would), file descriptor 2 (where the C library writes and where a
    handler opened on fd 2 would), and the APPLICATION's own handlers, reached by
    propagation to the root logger. The third is the one that has no obvious symptom: a
    library that hands its host to the root logger has published it in every deployment
    whose logging is configured, and nothing local looks wrong."""
    application = logging.Handler()
    seen: list[str] = []
    application.emit = lambda record: seen.append(record.getMessage())  # type: ignore[method-assign]
    root = logging.getLogger()
    root.addHandler(application)
    buffered = io.StringIO()
    monkeypatch.setattr(sys, "stderr", buffered)
    try:
        with _file_descriptor_2() as descriptor:
            _discarding_client_logger().error(_CLIENT_LINE)
    finally:
        root.removeHandler(application)

    assert buffered.getvalue() == "", (
        f"the logger handed to the client wrote to `sys.stderr`: {buffered.getvalue()!r}"
    )
    assert bytes(descriptor) == b"", (
        f"the logger handed to the client wrote to file descriptor 2: {bytes(descriptor)!r}"
    )
    assert seen == [], (
        f"the record reached a handler on the ROOT logger ({seen!r}), so `propagate` is "
        "still True and every application that configures logging republishes the address"
    )


def test_a_second_publish_replaces_the_handler_rather_than_stacking_another(monkeypatch):
    """`logging.getLogger` returns a PROCESS-GLOBAL object, so two publishes are two calls
    on one logger. Appending would grow the handler list for the life of the process, and --
    worse for a lock -- would leave whatever a previous caller had attached in place.

    AND WHAT SURVIVES IS CHECKED FOR WHAT IT WILL DO, not for what it is an instance of.
    `isinstance(..., logging.NullHandler)` was the whole of that check, and a SUBCLASS
    satisfies it: with one substituted into `_discarding_client_logger` -- overriding
    `handle` to append to a file -- every one of the six arms here reported PASSED while a
    record driven through the shipped function put the host in that file, 76 bytes.
    Assigning `handle` ON AN INSTANCE of the real `NullHandler` leaks the same 76 bytes and
    survives an exact-type check as well; it is the one-line move this file makes on `emit`
    two arms above, so it is not a hypothetical shape.

    NEITHER IS REACHED BY THE ARM ABOVE, whose three escape routes are `sys.stderr`, fd 2
    and the root logger -- a file is a fourth, and enumerating routes never closes the last
    one. So the OBJECT is pinned instead: exactly the stdlib's discard handler, and the
    `handle` that `logging.Logger.callHandlers` will really invoke still the stdlib's own
    stub. That is one constraint covering every route, including routes no assertion here
    could name, and it costs two lines."""
    intruder = logging.Handler()
    first = _discarding_client_logger()
    first.addHandler(intruder)
    second = _discarding_client_logger()
    assert first is second, "the logger is looked up by name; two calls must be one object"
    assert second.propagate is False
    assert len(second.handlers) == 1, (
        f"{len(second.handlers)} handlers after a second publish; the list is replaced, not "
        "appended to, so a handler another caller attached cannot survive into a publish"
    )
    handler = second.handlers[0]
    assert type(handler) is logging.NullHandler, (
        f"the handler left on the logger is a {type(handler).__name__}; `isinstance` cannot "
        "tell a SUBCLASS from the stdlib's discard handler, and one that overrides `handle` "
        "writes the line wherever it likes -- to a file, say -- while the handler count, "
        "`propagate`, `sys.stderr`, fd 2 and the root logger all stay green"
    )
    assert getattr(handler.handle, "__func__", None) is logging.NullHandler.handle, (
        f"`handle` is {handler.handle!r}, reassigned ON THE INSTANCE -- which the exact-type "
        "check above cannot see, because `Logger.callHandlers` looks `handle` up on the "
        "object. It is the same one-line move this file makes on `emit` two arms above, and "
        "it publishes every record the client library hands over"
    )
    assert second.name == CLIENT_LOGGER_NAME


# --------------------------------------------------------------------------------
# 3. THE JOIN: THE CLIENT IS BUILT FROM BOTH OF THE THINGS CHECKED ABOVE
# --------------------------------------------------------------------------------


def test_the_client_is_built_from_both_of_the_defences_and_from_nothing_else():
    """The two arms above check two objects; this is what says the client gets THEM.

    IT IS DELIBERATELY THE WEAKEST ARM AND IT IS WRITTEN LAST. On its own it is exactly the
    lock T1a found holed in `tests/test_probe_broker.py` -- the keyword was there and what
    fed it was a redactor that knew no secrets. What makes it worth having is that the two
    named callees have already been driven, so naming them at the call site completes a
    chain rather than starting one."""
    tree = ast.parse(_MODULE.read_text(encoding="utf-8"), filename=str(_MODULE))
    opener = next(node for node in ast.walk(tree)
                  if isinstance(node, ast.FunctionDef) and node.name == "_open_client")
    built = [node for node in ast.walk(opener)
             if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "Producer"]
    assert len(built) == 1, f"`_open_client` builds {len(built)} clients; exactly one"

    assert len(built[0].args) == 1, "the client takes one positional configuration mapping"
    config = built[0].args[0]
    assert isinstance(config, ast.Call), (
        "the client is built from a bare mapping, so librdkafka's log level is whatever it "
        "defaults to -- 6, which renders the line naming the bootstrap address"
    )
    assert getattr(config.func, "id", None) == "_client_config_that_cannot_log", (
        f"the configuration comes from `{getattr(config.func, 'id', None)}`, not from "
        "`_client_config_that_cannot_log`, so nothing checked above applies to it"
    )
    assert [getattr(arg, "id", None) for arg in config.args] == ["broker"], (
        "the silenced configuration is built from something other than the `broker` this "
        "function was handed, so the client could be opened against a different credential"
    )

    passed = {keyword.arg: keyword.value for keyword in built[0].keywords}
    assert "logger" in passed, (
        "the client is built with NO `logger=`, so a line that survives the log level goes "
        "to file descriptor 2 -- the host three times by the moment `flush` returns, and "
        "six by the time the client that wrote them is destroyed"
    )
    assert isinstance(passed["logger"], ast.Call), (
        "`logger=` is handed a name rather than a call, so what the client writes to is "
        "whatever that name was bound to elsewhere"
    )
    assert getattr(passed["logger"].func, "id", None) == "_discarding_client_logger", (
        f"`logger=` is fed `{getattr(passed['logger'].func, 'id', None)}`, not "
        "`_discarding_client_logger` -- a logger that reaches any handler that writes "
        "publishes the address, and a bare `logging.getLogger(...)` reaches `lastResort`"
    )
    assert passed["logger"].args == [] and passed["logger"].keywords == [], (
        "`_discarding_client_logger` is being handed an argument, so it is no longer the "
        "no-argument sink the arm above drove"
    )


# --------------------------------------------------------------------------------
# 4. END TO END, THROUGH A REAL CLIENT, WITH THE LEAK REPRODUCED BESIDE IT
# --------------------------------------------------------------------------------


_PROBE = textwrap.dedent(
    """
    import json
    import os
    import sys
    import tempfile
    import time

    mode, bootstrap, password = sys.argv[1], sys.argv[2], sys.argv[3]

    # How long fd 2 stays redirected AFTER the publish has returned. See the settle below
    # for why the arm is worth nothing without it.
    SETTLE_SECONDS = 2.0

    # NOT a `confluent_kafka` import: `opl.streaming.producer` imports the compiled
    # extension inside `_open_client`, which is what puts the import INSIDE the window
    # below -- the shape in which the leak was measured.
    from opl.streaming.producer import BrokerConfig, publish_records

    records = [
        {"transaction_id": "t1c-0001", "amount": "1.00"},
        {"transaction_id": "t1c-0002", "amount": "2.00"},
    ]
    broker = BrokerConfig(bootstrap=bootstrap, username="opl", password=password)

    saved = os.dup(2)
    sink = tempfile.TemporaryFile()
    refusal = None
    try:
        os.dup2(sink.fileno(), 2)
        try:
            if mode == "control":
                # WHAT `_open_client` BUILT BEFORE THIS COMMIT, spelled out rather than
                # imported, so the control cannot be silenced by the change it exists to
                # measure.
                from confluent_kafka import Producer

                client = Producer(broker.client_config())
                client.produce("t1c-topic", value=b"x", key=b"k")
                client.flush(0.1)
                del client
            else:
                publish_records(records, topic="t1c-topic", broker=broker, flush_timeout=1.0)
        except BaseException as failed:
            refusal = f"{type(failed).__name__}: {failed}"
        # THE WINDOW OUTLIVES THE CALL, deliberately, and this line is where the arm's
        # zero stops meaning "zero SO FAR". librdkafka writes from a thread of its own and
        # the leak measured beside this one GROWS after the call returns: the host three
        # times by the moment `flush` returned, six by the time the client that wrote them
        # was destroyed. And the client is destroyed HERE, by the `except` above dropping
        # the traceback that kept its frame alive -- already past `publish_records`. With
        # fd 2 restored on return, that burst would go to the REAL stderr, unread and
        # unasserted, and this arm would report a green it had not earned.
        time.sleep(SETTLE_SECONDS)
    finally:
        os.dup2(saved, 2)
        os.close(saved)
        sink.seek(0)
        written = sink.read()
        sink.close()

    host = bootstrap.rsplit(":", 1)[0].encode()
    print(json.dumps({
        "bytes": len(written),
        "host_hits": written.count(host),
        "password_hits": written.count(password.encode()),
        "refusal": refusal,
    }))
    """
)


def _probe(mode: str) -> dict[str, object]:
    """One arm, in its own process. See this module's docstring for why a shared one lies."""
    result = subprocess.run(
        [sys.executable, "-c", _PROBE, mode, _BOOTSTRAP, _PASSWORD],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    return json.loads(result.stdout)


def _the_leak_reproduced_or_skip() -> dict[str, object]:
    """The NEGATIVE CONTROL, and the decision of whether this machine can measure at all.

    LIFTED OUT OF THE ARM BELOW AT A REAL SEAM, not to fit a cap: deciding whether the leak
    reproduces here is a different job from asserting the shipped path is silent, and it is
    the job that has to run first. `pytest.skip` raises, so calling it from a helper skips
    the calling test, and a `return` here is reached only when the control really leaked.

    Without this, "zero bytes on fd 2" is a sentence a broken harness, a missing client
    library and a machine whose resolver answers differently all produce -- ADR 0018's
    species exactly."""
    control = _probe("control")
    if control["bytes"] == 0:
        pytest.skip(
            "the control process wrote 0 bytes to fd 2, so this machine did not reproduce "
            "the leak and an assertion of zero on the shipped path would be vacuous"
        )
    assert control["host_hits"] >= 1, (
        f"the control leaked {control['bytes']} bytes but none of them named the host, so "
        "this arm is not measuring what it claims to"
    )
    assert control["password_hits"] == 0, (
        f"the client library's own log lines carried the SASL PASSWORD "
        f"{control['password_hits']} times inside {control['bytes']} bytes. The address is "
        "a coordinate this repository keeps out of committed text; the password is a "
        "credential whose escape is repaired by ROTATING it and not by closing a file "
        "descriptor, and an arm that watches a leak has to say which of the two it saw"
    )
    return control


def test_a_publish_to_an_unreachable_broker_writes_nothing_to_the_file_descriptor():
    """THE ARM THAT IS ABOUT THE REAL CLIENT, and the only one that would have caught this.

    A real `publish_records` against a host that cannot resolve, fd 2 read at the OS level,
    beside a control process that builds the client the way this module built it before the
    fix. The control is what makes the zero mean something: 440 to 441 bytes naming the host
    THREE TIMES and the password not once, 7 of 7 on 2026-09-02 -- the one-byte spread is
    the width of the millisecond count librdkafka prints into the line. THAT FIGURE IS A
    WHOLE CLIENT'S LIFE and not a reading at an instant: the control opens, flushes and
    destroys its client inside the window, and destruction is where a large part of those
    bytes arrives -- in the timeline the producer's module docstring carries, a three-mention
    reading grows from 254 bytes to 442 at that moment. Every number here is a floor, which
    is what this asserts.

    AND THE WINDOW OUTLIVES THE PUBLISH BY TWO SECONDS, which is what lets the shipped arm's
    zero mean "zero" instead of "zero so far": see the settle in `_PROBE` for why the burst
    this arm most needs to see lands after the call returns. Measured with the settle in
    place at 0 bytes, 0 host mentions and 0 password mentions, 7 of 7 -- the wait buys the
    stronger sentence without buying a flake.

    AND THE REFUSAL IS ASSERTED TOO. Silencing a client library is a change that can eat the
    diagnosis along with the leak; `_refuse_an_incomplete_delivery` still has to name the
    records that never left the process, in its own words, or the fix cost more than the
    defect."""
    pytest.importorskip("confluent_kafka", reason="the client library is a dev-group wheel")

    control = _the_leak_reproduced_or_skip()

    shipped = _probe("shipped")
    # Checked separately from the byte count below, and before it. Zero bytes cannot
    # contain a substring, so today this cannot fail on its own -- what the separate line
    # buys is that the password's severity does not ride on the host's: a floor loosened
    # later to tolerate a timestamp or a thread's parting line must not loosen this too.
    assert shipped["password_hits"] == 0, (
        f"a publish through `publish_records` put the SASL PASSWORD on file descriptor 2 "
        f"{shipped['password_hits']} times, in {shipped['bytes']} bytes"
    )
    assert shipped["bytes"] == 0, (
        f"a publish through `publish_records` wrote {shipped['bytes']} bytes to file "
        f"descriptor 2, naming the bootstrap host {shipped['host_hits']} times -- the "
        f"unsilenced control beside it wrote {control['bytes']}"
    )
    assert "still queued" in str(shipped["refusal"]), (
        f"the publish refused with {shipped['refusal']!r} rather than naming the records "
        "that never left the process -- silencing the client library must not cost the "
        "diagnosis it was never carrying"
    )
