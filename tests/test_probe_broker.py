# tests/test_probe_broker.py
"""Unit tests for `scripts/probe_broker.py`. Hermetic: no broker, no network, no secret.

WHAT IS WORTH LOCKING HERE IS NOT THE KAFKA CALL. One metadata request is the easy half,
it needs a live cluster to exercise, and a test that mocked it would be a test of the mock.
What this file holds is the two properties the script exists for and that an edit could
undo without any symptom:

  1. THAT IT CANNOT LEAK. The password and the bootstrap host are both secrets, and the
     script's own measurements found the leak paths are not where a reader would look --
     the client library writes the host to FILE DESCRIPTOR 2 from its own thread, and one
     of `BrokerConfig`'s refusals quotes the address back at the operator. So the redaction
     is tested on the error path, on the descriptor, and on a host the redactor was never
     told about.
  2. THAT THE ASYMMETRY IS PUBLISHED. A success is conclusive and a failure is not, and the
     script's whole value is that its OUTPUT says which. A comment saying so would be worth
     nothing to the reader of a line pasted into an evidence document, so what is asserted
     is the rendered text.

THE FIXTURES ARE INVENTED, deliberately, and end in `.invalid` -- the RFC-reserved suffix
that cannot resolve. `tests/test_rebuild_pii_reader_sp_script.py` records why: its own
fixtures were once REAL workspace identifiers sitting in three test files with nothing
saying so. A test file for a redaction is the last place a real address should appear.
"""
from __future__ import annotations

import ast
import importlib.util
import os
import sys
from pathlib import Path

import pytest

_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "probe_broker.py"
_spec = importlib.util.spec_from_file_location("probe_broker_cli", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
cli = importlib.util.module_from_spec(_spec)
# REGISTERED BEFORE EXECUTION, which the sibling script tests do not need to do. `dataclass`
# resolves `KW_ONLY` by looking the defining module up in `sys.modules`, so a module executed
# outside it raises `AttributeError: 'NoneType' object has no attribute '__dict__'` at class
# creation -- a failure that names neither the script nor the dataclass.
sys.modules[_spec.name] = cli
_spec.loader.exec_module(cli)

# INVENTED. `.invalid` is reserved by RFC 2606 and cannot resolve, so a fixture that
# escaped into a real client would fail loudly rather than reach somebody's host.
_HOST = "seed-deadbeef.any.us-east-1.mpx.prd.cloud.example.invalid"
_BOOTSTRAP = f"{_HOST}:9092"
_PASSWORD = "not-a-real-password-0123456789ab"
# A SIBLING BROKER, which is what a metadata reply names and what the redactor is never
# told about: it is not the bootstrap string and not its host.
_SIBLING = "d0a1b2c3.any.us-east-1.mpx.prd.cloud.example.invalid:9092"

_REDACTOR = cli.Redactor.of(bootstrap=_BOOTSTRAP, password=_PASSWORD)

_ENV = {"KAFKA_BOOTSTRAP": _BOOTSTRAP, "KAFKA_USER": "opl", "KAFKA_PASSWORD": _PASSWORD}


class _FakeKafkaError:
    """Stands in for `confluent_kafka.KafkaError`: only `name()` is read."""

    def __init__(self, name: str) -> None:
        self._name = name

    def name(self) -> str:
        return self._name


class _LeakyKafkaException(Exception):
    """An exception shaped like the client library's: a `KafkaError` first argument, and
    a message that carries the host and the password -- which is the assumption the
    script's first hard rule tells it to make about foreign exception text."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(_FakeKafkaError(code))
        self._message = message

    def __str__(self) -> str:
        return self._message


def _answered() -> cli.Reading:
    return cli.Reading(
        taken_at="2026-09-03T09:00:00Z",
        elapsed_seconds=1.27,
        broker_count=3,
        topics=("hello-world", "opl-payments"),
    )


def _did_not_answer() -> cli.Reading:
    return cli.Reading(
        taken_at="2026-09-03T09:00:00Z",
        elapsed_seconds=15.01,
        failure="KafkaError{code=_TIMED_OUT}",
        failure_code="KafkaException / _TIMED_OUT",
    )


def _leaks(text: str) -> list[str]:
    """Every secret, and every fragment of one long enough to be a secret, found in `text`.

    A FRAGMENT COUNTS. Half a host is still a host somebody can finish, and half a password
    is what `probe_postgres_session.redacted_dsn` records reaching stdout in exactly this
    project. Twelve characters is short enough to catch a truncation and long enough not to
    fire on ordinary English."""
    found = []
    for secret in (_HOST, _BOOTSTRAP, _PASSWORD, _SIBLING):
        for start in range(0, len(secret) - 12):
            fragment = secret[start:start + 12]
            if fragment in text:
                found.append(fragment)
                break
    return found


# --------------------------------------------------------------------------------
# 1. IT CANNOT LEAK
# --------------------------------------------------------------------------------


def test_both_secrets_are_removed_from_a_line_that_carries_both():
    """The exact values, in the shapes the client library actually writes them in."""
    line = (f"sasl_ssl://{_BOOTSTRAP}/bootstrap: Failed to resolve '{_BOOTSTRAP}' "
            f"for user opl with password {_PASSWORD} and host {_HOST}")
    cleaned = _REDACTOR(line)
    assert _leaks(cleaned) == []
    assert cli.PASSWORD_PLACEHOLDER in cleaned
    assert cli.BROKER_PLACEHOLDER in cleaned


def test_a_broker_the_redactor_was_never_told_about_is_removed_by_shape():
    """The reply names the cluster's OTHER brokers, and a connect failure names an address.

    Neither is the bootstrap string, so an exact-value rule alone would publish both."""
    cleaned = _REDACTOR(f"Connect to ipv4#10.11.12.13:9092 at {_SIBLING} failed")
    assert _leaks(cleaned) == []
    assert "10.11.12.13" not in cleaned
    assert cleaned.count(cli.ADDRESS_PLACEHOLDER) == 2


def test_a_blank_credential_registers_no_rule_at_all():
    """A machine with no `.env` reaches this, and registering `""` would replace at every
    position in every string -- destroying the output while protecting nothing."""
    empty = cli.Redactor.of(bootstrap="", password="")
    assert empty.pairs == ()
    assert empty("nothing here is a secret") == "nothing here is a secret"


def test_the_error_path_cannot_leak(monkeypatch):
    """The path the script's first hard rule is about: exception text from the client.

    `probe` is driven with a failure whose message carries BOTH secrets, and what is
    inspected is the rendered output rather than the exception."""
    def _raise(*_args, **_kwargs):
        raise _LeakyKafkaException("_TIMED_OUT", f"Failed to get metadata for {_BOOTSTRAP} "
                                                 f"using password {_PASSWORD}")

    monkeypatch.setattr(cli, "cluster_metadata", _raise)
    reading, captured = cli.probe(object(), _REDACTOR, timeout=0.1)
    assert not reading.answered
    assert reading.failure_code == "_LeakyKafkaException / _TIMED_OUT"
    rendered = "\n".join(cli.render(reading, captured))
    assert _leaks(rendered) == []
    assert cli.PASSWORD_PLACEHOLDER in rendered and cli.BROKER_PLACEHOLDER in rendered


def test_what_is_written_to_the_file_descriptor_is_captured_and_redacted():
    """MEASURED, and it is why this context manager exists: with no `logger=`, the client
    library writes the bootstrap host to fd 2 ITSELF, from its own thread, three times per
    line -- 715 characters of it against an unresolvable host on 2026-09-02. Reassigning
    `sys.stderr` does not move fd 2, so nothing at the Python level can see that."""
    with cli.stderr_captured(_REDACTOR) as captured:
        os.write(2, f"%3|FAIL|rdkafka: {_BOOTSTRAP} rejected {_PASSWORD}\n".encode())
    assert captured, "nothing was captured from fd 2"
    joined = "\n".join(captured)
    assert _leaks(joined) == []
    assert cli.BROKER_PLACEHOLDER in joined and cli.PASSWORD_PLACEHOLDER in joined


def test_foreign_text_is_redacted_before_it_is_capped_never_after():
    """Cutting first leaves HALF a host in the output, and the exact-value rules no longer
    match it. That is not hypothetical in this repository: `redacted_dsn` records `b'` --
    half of a quoted password -- reaching stdout for the same reason."""
    padding = "x" * 40
    # The cut lands JUST AFTER the host's first dot, which is the cruel case: what is left
    # is `seed-deadbeef.`, a fragment that the exact-value rules no longer match and that
    # the shape rule does not either, because a trailing dot is not a host name.
    cut = len(padding) + _HOST.index(".") + 1
    capped = cli.foreign(f"{padding}{_BOOTSTRAP} trailing", _REDACTOR, limit=cut)
    assert _leaks(capped) == []
    assert capped.startswith(padding)
    assert "more characters withheld" in capped


def test_the_only_print_in_the_script_is_the_choke_point():
    """A SECOND print site is how a leak gets added by an edit that looks harmless, so the
    property is read off the module's syntax rather than trusted."""
    tree = ast.parse(_SCRIPT_PATH.read_text(encoding="utf-8"), filename=str(_SCRIPT_PATH))
    sites = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "print"
    ]
    assert len(sites) == 1, f"{len(sites)} `print` calls; exactly one is allowed"
    enclosing = [
        function.name for function in ast.walk(tree)
        if isinstance(function, ast.FunctionDef)
        and any(node is sites[0] for node in ast.walk(function))
    ]
    assert enclosing == ["emit"], f"the one `print` sits in {enclosing}, not in `emit`"


def test_the_choke_point_redacts_what_it_is_handed(capsys):
    """`emit` is where the guarantee is spent, so it is checked directly rather than
    inferred from the fact that everything routes through it."""
    cli.emit(_REDACTOR, f"probe_broker: {_BOOTSTRAP} and {_PASSWORD}")
    printed = capsys.readouterr().out
    assert _leaks(printed) == []
    assert cli.BROKER_PLACEHOLDER in printed and cli.PASSWORD_PLACEHOLDER in printed


def test_the_client_call_stays_inside_the_descriptor_capture():
    """The two defences are independent, and this is the one that does not depend on the
    client library cooperating. Deleting the `with` would leave the `logger=` argument as
    the only thing standing between the host and a public paste, and the measurement says
    that argument is a default away from being absent."""
    tree = ast.parse(_SCRIPT_PATH.read_text(encoding="utf-8"), filename=str(_SCRIPT_PATH))
    probe = next(f for f in ast.walk(tree)
                 if isinstance(f, ast.FunctionDef) and f.name == "probe")
    guards = [node for node in ast.walk(probe) if isinstance(node, ast.With)]
    assert len(guards) == 1, "`probe` must open exactly one guard"
    assert any(getattr(item.context_expr.func, "id", None) == "stderr_captured"
               for item in guards[0].items)
    calls = [node for node in ast.walk(guards[0])
             if isinstance(node, ast.Call) and getattr(node.func, "id", None)
             == "cluster_metadata"]
    assert calls, "the metadata request must happen INSIDE the capture"


def test_the_client_logger_cannot_reach_a_handler_that_writes_to_stderr():
    """`propagate` is the load-bearing line: the root logger's last-resort handler writes
    to stderr, which is the one path this script cannot redact. Handlers are replaced
    rather than appended, so a second call cannot print every line twice."""
    first = cli.client_logger(_REDACTOR)
    second = cli.client_logger(_REDACTOR)
    assert first is second
    assert first.propagate is False
    assert len(first.handlers) == 1
    assert isinstance(first.handlers[0], cli.RedactingHandler)


# --------------------------------------------------------------------------------
# 2. THE ASYMMETRY IS PUBLISHED
# --------------------------------------------------------------------------------


def test_a_success_is_rendered_as_conclusive():
    rendered = "\n".join(cli.render(_answered()))
    assert cli.ANSWERED_MEANS in rendered
    assert "CONCLUSIVE" in rendered and "NOT CONCLUSIVE" not in rendered
    assert "brokers | 3" in rendered
    assert "['hello-world', 'opl-payments']" in rendered


def test_a_failure_is_rendered_as_NOT_conclusive_and_refuses_the_sentence_it_prevents():
    """The single defect this script exists to remove: a timeout read as a deletion.

    So the failure verdict must SAY it is not conclusive, must say why -- a home network
    is not a serverless task's network -- and must name the wrong sentence in order to
    forbid it, rather than leaving a reader to infer that it is forbidden."""
    rendered = "\n".join(cli.render(_did_not_answer()))
    assert cli.NO_ANSWER_MEANS in rendered
    assert "NOT CONCLUSIVE" in rendered
    assert "is NOT evidence that the cluster is gone" in rendered
    assert "A home network is not a serverless task's network" in rendered
    assert "Do not write 'the broker is gone' from this line" in rendered


def test_the_two_verdicts_are_different_sentences():
    """One sentence covering both outcomes would be the symmetry this file refuses."""
    assert cli.ANSWERED_MEANS != cli.NO_ANSWER_MEANS
    assert cli.NO_READING_MEANS not in (cli.ANSWERED_MEANS, cli.NO_ANSWER_MEANS)


def test_this_script_own_prose_survives_its_own_redaction_unchanged():
    """THE INVARIANT THAT MAKES ONE CHOKE POINT SAFE. The redactor removes anything shaped
    like a network address and cannot tell this script's prose from the broker's text, so
    the script keeps no dotted token in its own vocabulary. A verdict reworded to contain
    one would be published with a hole in it, and nothing else would notice."""
    empty = cli.Redactor.of(bootstrap="", password="")
    lines = [cli.ANSWERED_MEANS, cli.NO_ANSWER_MEANS, cli.NO_READING_MEANS,
             *cli.render(_answered()), *cli.render(_did_not_answer())]
    mangled = [line for line in lines if empty(line) != line]
    assert mangled == [], f"the redactor rewrites this script's own prose: {mangled}"


# --------------------------------------------------------------------------------
# 3. "COULD NOT ASK" IS A THIRD OUTCOME, NOT A FAILURE
# --------------------------------------------------------------------------------


def test_the_three_exit_codes_are_distinct():
    assert len({cli.EXIT_ANSWERED, cli.EXIT_NO_ANSWER, cli.EXIT_NO_READING}) == 3


def test_a_clone_with_no_credential_says_so_and_does_not_crash(capsys):
    """Somebody cloning this repository has no `.env`. That must not be a traceback, and
    it must not be counted as the cluster failing to answer."""
    assert cli.main(argv=[], env={}) == cli.EXIT_NO_READING
    printed = capsys.readouterr().out
    assert "NO READING TAKEN" in printed
    assert cli.NO_READING_MEANS in printed
    assert cli.ANSWERED_MEANS not in printed and cli.NO_ANSWER_MEANS not in printed
    assert "KAFKA_BOOTSTRAP" in printed


def test_a_bootstrap_with_no_password_is_refused_rather_than_timed_out(capsys):
    """PLAINTEXT against a SASL_SSL listener times out with the string a deleted cluster
    produces. Refusing is the only honest outcome, and it is refused as NO READING."""
    env = {"KAFKA_BOOTSTRAP": _BOOTSTRAP}
    assert cli.main(argv=[], env=env) == cli.EXIT_NO_READING
    printed = capsys.readouterr().out
    assert "KAFKA_PASSWORD" in printed
    assert _leaks(printed) == []


def test_an_absent_client_library_is_no_reading_and_not_a_dead_cluster(monkeypatch, capsys):
    """An uninstalled wheel counted as "the cluster did not answer" is the exact confusion
    this script exists to prevent: nothing was asked. So the check runs BEFORE the probe
    and outside its `except`."""
    monkeypatch.setitem(sys.modules, "confluent_kafka", None)
    with pytest.raises(ImportError, match="uv sync --all-groups"):
        cli.require_client_library()
    assert cli.main(argv=[], env=dict(_ENV)) == cli.EXIT_NO_READING
    assert "NO READING TAKEN" in capsys.readouterr().out


def test_the_exit_code_follows_the_reading(monkeypatch, capsys):
    """The asymmetry in the one place a machine can read it."""
    monkeypatch.setattr(cli, "probe", lambda *a, **k: (_answered(), []))
    assert cli.main(argv=[], env=dict(_ENV)) == cli.EXIT_ANSWERED
    monkeypatch.setattr(cli, "probe", lambda *a, **k: (_did_not_answer(), []))
    assert cli.main(argv=[], env=dict(_ENV)) == cli.EXIT_NO_ANSWER
    assert _leaks(capsys.readouterr().out) == []


def test_no_exception_escapes_the_probe(monkeypatch):
    """Not only the client library's own: `probe`'s breadth is what keeps a traceback --
    which nothing redacts -- out of a terminal whose contents get pasted."""
    def _raise(*_args, **_kwargs):
        raise RuntimeError(f"something nobody predicted, at {_BOOTSTRAP}")

    monkeypatch.setattr(cli, "cluster_metadata", _raise)
    reading, _captured = cli.probe(object(), _REDACTOR, timeout=0.1)
    assert reading.failure_code == "RuntimeError"
    assert _leaks(reading.failure) == []
