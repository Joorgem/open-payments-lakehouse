# tests/test_streaming_managed_broker.py
"""`opl.streaming.managed_broker`: the JVM client's spelling of a SASL credential, what a
task may print about it, and the row floor a managed-broker run declares at launch.

NO BROKER AND NO SPARK, deliberately and for `opl.streaming.messages`' reason one layer
along: this module builds strings and refuses inputs, so everything it claims is checkable
in CI's default invocation. The thing it cannot check is the one thing only the workspace
can answer -- whether `DATABRICKS_SCRAM_LOGIN_MODULE` names a class that is actually on the
deploy target's classpath -- and that is measured instead, once, by a run (F5 §1.2, job run
`382097683531247`). A test asserting the literal against itself would be the shape ADR 0018
refuses.

THE PASSWORD USED HERE IS A NONSENSE LITERAL AND IS NOT THE REAL ONE. It exists so the
tests below can assert where it does and does not appear; the real credential is in a
git-ignored `.env` and in a Databricks secret scope, and neither is readable from here."""
from __future__ import annotations

import pytest

from opl.streaming.managed_broker import (
    DATABRICKS_SCRAM_LOGIN_MODULE,
    JAAS_OPTION,
    MECHANISM_OPTION,
    OSS_SCRAM_LOGIN_MODULE,
    PROTOCOL_OPTION,
    SECURITY_PROTOCOL,
    SENTINEL_MINIMUM_ROWS,
    describe_reader_options,
    require_minimum_rows,
    sasl_reader_options,
)
from opl.streaming.producer import SASL_MECHANISM, BrokerConfig

_PASSWORD = "not-the-real-one-0000"
_SASL = BrokerConfig(bootstrap="broker.invalid:9092", username="opl", password=_PASSWORD)
_PLAINTEXT = BrokerConfig(bootstrap="localhost:9092")


def test_a_plaintext_broker_gets_no_security_options_at_all():
    """THE LOCAL CONTAINER, and the empty mapping is a decision rather than a fallback.

    `docker-compose.yml`'s Redpanda advertises `PLAINTEXT://localhost:9092`, so a SASL
    block added "just in case" would not degrade gracefully -- it would fail every local
    read with a handshake error, which is the same error text a wrong password produces."""
    assert sasl_reader_options(_PLAINTEXT) == {}


def test_a_sasl_broker_gets_the_three_options_the_jvm_client_takes():
    options = sasl_reader_options(_SASL)
    assert set(options) == {PROTOCOL_OPTION, MECHANISM_OPTION, JAAS_OPTION}
    assert options[PROTOCOL_OPTION] == SECURITY_PROTOCOL == "SASL_SSL"
    assert options[MECHANISM_OPTION] == SASL_MECHANISM
    assert options[JAAS_OPTION].startswith(DATABRICKS_SCRAM_LOGIN_MODULE + " required ")
    assert options[JAAS_OPTION].endswith(";")


def test_the_two_dialects_carry_the_same_credential_and_the_same_mechanism():
    """ONE CREDENTIAL, TWO SPELLINGS, AND THIS IS WHAT KEEPS THEM ONE.

    `BrokerConfig.client_config` is librdkafka's dialect (`sasl.username` /
    `sasl.password` as separate keys) and `sasl_reader_options` is the JVM client's (one
    JAAS string). The failure this catches is the one `BrokerConfig`'s own `kw_only`
    comment names: the two fields are adjacent and both `str | None`, so a swap sends the
    password as the SASL username -- into the broker's audit log, where nothing in this
    repository could redact it. A swap on ONE side only is invisible to every other test
    here, because each dialect is self-consistent."""
    librdkafka = _SASL.client_config()
    jaas = sasl_reader_options(_SASL)[JAAS_OPTION]

    assert librdkafka["sasl.mechanism"] == SASL_MECHANISM
    assert f'username="{librdkafka["sasl.username"]}"' in jaas
    assert f'password="{librdkafka["sasl.password"]}"' in jaas
    assert 'username="opl"' in jaas and f'password="{_PASSWORD}"' in jaas


def test_the_login_module_default_is_the_one_that_has_actually_been_observed_to_work():
    """The two class names differ, and which one is the DEFAULT is the whole decision.

    DBR shades its bundled Kafka client; open-source Spark does not, and each runtime
    REFUSES the other's name: local OSS Spark handed the shaded one against the managed
    cluster raises `LoginException: No LoginModule found for kafkashaded...` (F5 §2.5). The
    shaded name is the default because this module exists for the job that runs on DBR; the
    unshaded one is the argument a caller on open-source Spark passes.

    WHICH IS THE CLASSPATH QUESTION, AND THIS TEST DOES NOT ASK IT. Nothing here loads
    either class -- that takes a Spark session and a broker -- so what is asserted below is
    the string plumbing, and which name is on which runtime is measured by runs instead
    (F5 §1.2 on serverless, §2.5 from this box)."""
    assert DATABRICKS_SCRAM_LOGIN_MODULE.startswith("kafkashaded.")
    assert OSS_SCRAM_LOGIN_MODULE == DATABRICKS_SCRAM_LOGIN_MODULE.removeprefix("kafkashaded.")
    assert DATABRICKS_SCRAM_LOGIN_MODULE in sasl_reader_options(_SASL)[JAAS_OPTION]
    assert (
        OSS_SCRAM_LOGIN_MODULE
        in sasl_reader_options(_SASL, login_module=OSS_SCRAM_LOGIN_MODULE)[JAAS_OPTION]
    )


@pytest.mark.parametrize("bad", ['pass"word', "pass\\word"])
@pytest.mark.parametrize("field", ["username", "password"])
def test_a_credential_the_jaas_grammar_cannot_carry_is_refused_before_a_socket(field, bad):
    """The refusal, in both fields and both characters.

    WHY IT IS WORTH A GUARD AT ALL: the JAAS value is a double-quoted string assembled by
    concatenation here, so a `"` truncates the credential and a `\\` escapes the next
    character. Neither raises. The broker rejects the SCRAM exchange, and that error text
    is also what an expired trial, a revoked ACL and a wrong username produce -- one string
    across four worlds, which ADR 0018 says is not a check."""
    broker = BrokerConfig(
        bootstrap="broker.invalid:9092",
        username=bad if field == "username" else "opl",
        password=bad if field == "password" else _PASSWORD,
    )
    with pytest.raises(ValueError, match=f"SASL {field} contains"):
        sasl_reader_options(broker)


def test_the_description_names_every_option_and_withholds_the_one_that_is_the_password():
    """WHAT A TASK PRINTS INSTEAD OF THE MAPPING, and the assertion is the absence.

    The positive half matters too: printing the KEYS is what tells a reader of a failed
    handshake that the SASL block was configured at all, which is the question that
    actually arises. Only the JAAS value is withheld, and it is withheld BY NAME rather
    than by guessing at the value's shape."""
    described = describe_reader_options(sasl_reader_options(_SASL))
    assert _PASSWORD not in described
    assert "opl" not in described  # the username travels inside the same JAAS string
    assert f"{PROTOCOL_OPTION}={SECURITY_PROTOCOL}" in described
    assert f"{MECHANISM_OPTION}={SASL_MECHANISM}" in described
    assert f"{JAAS_OPTION}=<withheld" in described


def test_the_description_is_total_so_an_option_added_later_is_not_silently_dropped():
    """A describer that listed the three known keys would print the same line for a mapping
    carrying a fourth -- and the fourth is the one nobody reviewed. `kafka.ssl.truststore.
    location` is not a real option of this project's; it is the shape of the next one."""
    described = describe_reader_options(
        {**sasl_reader_options(_SASL), "kafka.ssl.truststore.location": "/x"}
    )
    assert "kafka.ssl.truststore.location=/x" in described
    assert _PASSWORD not in described


def test_an_empty_mapping_describes_itself_as_a_plaintext_broker_rather_than_as_nothing():
    """An empty string in a task log reads as a task that forgot to print. The local
    container is a legitimate configuration and says so."""
    assert "PLAINTEXT" in describe_reader_options({})


@pytest.mark.parametrize(
    "value",
    [None, "", "   ", SENTINEL_MINIMUM_ROWS, "0", "-1", "1.5", "1e3", "many", "24 rows"],
)
def test_a_run_that_declares_no_usable_floor_is_refused_at_launch(value):
    """Absence, the sentinel, zero, and everything that is not a positive whole number.

    ZERO IS IN THIS LIST ON PURPOSE. It is the one value that would parse and would turn
    the parameter into a switch for disabling the check -- the phase's own species #1
    arriving through the door built to stop it, since over zero rows every dedup claim,
    every multiset comparison and every duplicate count is true and free."""
    with pytest.raises(ValueError, match="minimum_rows="):
        require_minimum_rows(value, action="read the managed broker")


@pytest.mark.parametrize(("value", "expected"), [("1", 1), (" 24 ", 24), ("40150", 40150)])
def test_a_declared_floor_is_taken_as_the_whole_number_it_spells(value, expected):
    """The accepting arm, including the whitespace a `--params` paste carries."""
    assert require_minimum_rows(value, action="read the managed broker") == expected


def test_the_refusal_prints_the_launch_command_rather_than_only_the_bad_value():
    """A refusal that names the parameter and not how to pass it sends an operator to the
    job page to guess. Every other launch-time refusal in this repository prints the
    command -- `assert_deployed_revision`'s does, and this is the same trap one job along."""
    with pytest.raises(ValueError) as excinfo:
        require_minimum_rows(SENTINEL_MINIMUM_ROWS, action="read the managed broker")
    message = str(excinfo.value)
    assert "--params" in message and "minimum_rows=" in message
    assert "read the managed broker" in message
