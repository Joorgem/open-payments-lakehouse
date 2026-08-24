# src/opl/streaming/managed_broker.py
"""What reading the MANAGED broker needs that reading the local container does not.

Two things, and they are the whole module. The local Redpanda in `docker-compose.yml`
speaks PLAINTEXT on a host port, so T2-T5 hand `opl.streaming.ingest.payment_stream` a
bootstrap address and nothing else. The managed cluster speaks SASL_SSL / SCRAM-SHA-256
across the public internet, and the run that reads it is not a test whose assertions the
suite re-checks on every commit -- it is a RECORDED RUN whose row count gets quoted. So:
the credential has to be spelled in the JVM client's dialect, and the run has to declare
what it must consume before it is allowed to report success.

--- ONE CREDENTIAL, TWO DIALECTS, AND THIS IS THE SECOND ---------------------------------

`opl.streaming.producer.BrokerConfig.client_config` already turns a credential into
librdkafka's spelling -- `sasl.username` and `sasl.password` as separate keys -- because
that is what `confluent_kafka` takes. Spark's Kafka source is the JVM client, which has no
such keys: it takes ONE `sasl.jaas.config` string in JAAS's own grammar, with the login
module class named inside it. Same credential, same mechanism constant (`SASL_MECHANISM`,
imported rather than respelled), different serialisation.

That is why this is a module and not two `.option(...)` lines at a call site. A JAAS string
assembled where it is used is a place for the password to be assembled a second time, and
the second copy is the one nobody reviews.

--- THE LOGIN MODULE CLASS IS DIFFERENT ON THE DEPLOY TARGET, AND IT IS MEASURED ----------

Databricks Runtime SHADES its bundled Kafka client, so the class that exists there is
`kafkashaded.org.apache.kafka...ScramLoginModule` and the OSS name is not on the classpath.
Open-source Spark with `spark-sql-kafka-0-10` has the unshaded name and not the shaded one.
Both are named below; the DEFAULT is the Databricks one because this module exists for the
job that runs there, and it is the one that has actually been observed to work -- F5 §1.2,
job run `382097683531247`, a serverless task that completed the SCRAM exchange and returned
a row.

AND THE OTHER CONSTANT IS MEASURED TOO, ON THE RUNTIME THAT HAS IT. Local OSS Spark
(pyspark 3.5.9 + `spark-sql-kafka-0-10`) read the MANAGED cluster over SASL_SSL with
`OSS_SCRAM_LOGIN_MODULE` and returned the one record the topic holds -- F5 §2.5. Handed
`DATABRICKS_SCRAM_LOGIN_MODULE` instead, the same session, credential and broker raised
`javax.security.auth.login.LoginException: No LoginModule found for` that shaded class. So
THE DEFAULT IS NOT PORTABLE, and it does not have to be: a caller on open-source Spark
passes `login_module=OSS_SCRAM_LOGIN_MODULE`, and the failure that sends them looking names
the CLASS rather than the credential -- which is the one diagnosis a failed SCRAM exchange
cannot give.

--- THE PASSWORD REACHES THE JAAS STRING, AND NOTHING HERE PRINTS ONE ---------------------

`sasl_reader_options` returns a mapping in which one VALUE contains the password in clear.
That is unavoidable: it is the only shape the JVM client accepts. Three things follow, and
all three are decisions rather than hygiene.

  1. `describe_reader_options` exists so that a task can log WHAT IT CONFIGURED without
     logging the credential -- every key, every value, except the JAAS one, which is
     withheld by name. A task that printed the options dict directly would put the password
     into a job run's output, and job output is what this phase's evidence quotes from.
  2. Spark's own log redaction is keyed on the OPTION NAME matching `secret|password|token`
     (`spark.redaction.regex`). `kafka.sasl.jaas.config` matches none of those, so a query
     plan or a `DESCRIBE` that echoes source options is not covered by it. Do not rely on
     it here.
  3. Databricks DOES scrub every occurrence of a secret's VALUE from task output, which is
     the last line of defence and the reason the password -- and only the password -- is
     stored in the secret scope. F5 §1.2 measured that scrubbing from the other end, with a
     control: the Kafka USERNAME was in the scope once, and because it is `opl`, the run's
     output rendered this repository's own catalog, topic and job prefix as `[REDACTED]`.

--- AND THE GRAMMAR IS REFUSED BEFORE IT IS SENT ------------------------------------------

JAAS's value grammar is a double-quoted string, and this repository assembles it by
concatenation. A password containing `"` or `\\` would produce a string that is malformed or
that silently truncates the credential -- and the symptom is a failed SCRAM exchange, which
is the same error text an expired trial, a revoked ACL and a wrong username produce.
`_refuse_a_credential_the_jaas_grammar_cannot_carry` names the real cause while the process
is still local, which is exactly the argument `producer._refuse_half_a_credential` makes one
protocol layer down.

--- THE FLOOR IS A LAUNCH PARAMETER HERE, NOT A DEFAULT ----------------------------------

`ingest.write_payment_stream`'s `minimum_rows` DEFAULT is a floor against zero and nothing
else: it accepts a run that consumed 1 record of 40,150. Its own docstring says the exact
count belongs in the caller, and in T2-T5 the caller is a test that states one. For the
managed-broker run there is no such test -- the caller is an operator and the product is a
number in a document -- so the count is declared AT LAUNCH and `require_minimum_rows`
refuses a run that was not given one. `SENTINEL_MINIMUM_ROWS` is what the job YAML defaults
to, on `SENTINEL_REVISION`'s and `SENTINEL_MONTH`'s model: a job-parameter default cannot
validate anything, so its only job is to be a value the code refuses."""
from __future__ import annotations

from collections.abc import Mapping

from opl.streaming.producer import SASL_MECHANISM, BrokerConfig

# The protocol the managed cluster listens with. Named, not inlined, for `SASL_MECHANISM`'s
# reason: the one place it is decided is the one place it would be changed.
SECURITY_PROTOCOL = "SASL_SSL"

# MEASURED on Databricks serverless (F5 §1.2, job run `382097683531247`). DBR shades its
# bundled Kafka client, and the OSS class is not on that classpath.
DATABRICKS_SCRAM_LOGIN_MODULE = (
    "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"
)
# MEASURED from this box against the SAME managed cluster (F5 §2.5): local OSS Spark loads
# this class and refuses the shaded one above with `LoginException: No LoginModule found`.
# Not the default -- it is what a caller on open-source Spark passes.
OSS_SCRAM_LOGIN_MODULE = "org.apache.kafka.common.security.scram.ScramLoginModule"

# The three reader options a SASL_SSL source needs, in Spark's `kafka.`-prefixed spelling.
PROTOCOL_OPTION = "kafka.security.protocol"
MECHANISM_OPTION = "kafka.sasl.mechanism"
JAAS_OPTION = "kafka.sasl.jaas.config"

# The one option whose value carries the password. `describe_reader_options` withholds it,
# and it is a named constant so that "which one is the secret" is a lookup rather than a
# string comparison somebody retypes.
WITHHELD_OPTIONS = frozenset({JAAS_OPTION})

# Characters the concatenated JAAS grammar cannot carry. See the module docstring.
_JAAS_UNSAFE = ('"', "\\")

# What the job YAML's `minimum_rows` parameter defaults to. Not a number, deliberately:
# `require_minimum_rows` refuses it, and a default that PARSED would be a floor nobody
# chose standing in for one somebody had to.
SENTINEL_MINIMUM_ROWS = "REQUIRED-PASS-THE-ROW-COUNT-THIS-RUN-MUST-CONSUME"


def _refuse_a_credential_the_jaas_grammar_cannot_carry(username: str, password: str) -> None:
    """Refuse a username or password this module cannot put inside a JAAS string.

    The failure this prevents is not a crash: a truncated or malformed JAAS string is
    rejected by the broker during the SCRAM exchange, and that error text is also what an
    expired trial, a revoked ACL and a wrong username produce. One string across four
    worlds is not a diagnosis (ADR 0018), so the refusal happens here, before a socket."""
    for what, value in (("username", username), ("password", password)):
        offending = sorted({c for c in _JAAS_UNSAFE if c in value})
        if offending:
            raise ValueError(
                f"the SASL {what} contains {offending}, which this module's JAAS string "
                "is assembled by concatenation and cannot carry. The broker would reject "
                "the SCRAM exchange with the same error an expired trial and a revoked "
                "ACL produce. Rotate the credential to one without those characters."
            )


def jaas_config(username: str, password: str, *, login_module: str) -> str:
    """The `sasl.jaas.config` value for a SCRAM login. CARRIES THE PASSWORD IN CLEAR.

    Return it to a Kafka reader and to nothing that logs -- see the module docstring's
    third section for why Spark's own redaction does not cover this option name."""
    _refuse_a_credential_the_jaas_grammar_cannot_carry(username, password)
    return f'{login_module} required username="{username}" password="{password}";'


def sasl_reader_options(
    broker: BrokerConfig, *, login_module: str = DATABRICKS_SCRAM_LOGIN_MODULE
) -> dict[str, str]:
    """The `kafka.*` reader options for `broker`, or an EMPTY mapping for a plaintext one.

    THE EMPTY CASE IS THE LOCAL CONTAINER AND IS NOT A FALLBACK. `BrokerConfig` keys SASL
    off the password exactly as `client_config` does -- a broker with no password is the
    PLAINTEXT Redpanda in `docker-compose.yml`, and adding a SASL block for it would make
    every local read fail with a handshake error. `producer._refuse_half_a_credential` has
    already refused BOTH asymmetries inside `BrokerConfig.__post_init__`, so by the time a
    config reaches here the two are either both present or both absent. The dangerous half
    is a username with no password; the half that matters HERE is the other one, and it is
    why `broker.username` is handed to `jaas_config` unguarded -- past the `return {}`
    above there is no accepted config whose password is set and whose username is not.

    A bare `dict`, not a frozen mapping, because `DataStreamReader.option` is the consumer
    and the caller merges it into a larger option set. Nothing in this package mutates it."""
    if broker.password is None:
        return {}
    return {
        PROTOCOL_OPTION: SECURITY_PROTOCOL,
        MECHANISM_OPTION: SASL_MECHANISM,
        JAAS_OPTION: jaas_config(broker.username, broker.password, login_module=login_module),
    }


def describe_reader_options(options: Mapping[str, str]) -> str:
    """`options` rendered for a log line, with every withheld value replaced by a reason.

    WHAT A TASK PRINTS INSTEAD OF THE OPTIONS DICT. The point is not tidiness: this run's
    output is what the evidence document quotes, and `kafka.sasl.jaas.config` contains the
    password. Printing the KEYS still says the SASL block was configured at all -- which is
    the thing a reader of a failed handshake actually wants to know -- while the one value
    that must not travel does not.

    TOTAL OVER THE MAPPING, so an option added later is described rather than silently
    dropped, and withheld by NAME rather than by guessing at the value's shape."""
    if not options:
        return "no broker options (a PLAINTEXT broker: no security protocol, no SASL)"
    return ", ".join(
        f"{key}=<withheld: carries the SASL password>"
        if key in WITHHELD_OPTIONS
        else f"{key}={value}"
        for key, value in sorted(options.items())
    )


def require_minimum_rows(value: str | None, *, action: str) -> int:
    """The floor a managed-broker run declares AT LAUNCH, or a refusal naming the launch.

    ABSENCE REFUSES RATHER THAN DEFAULTING, which is `opl.config.require_month`'s decision
    and it is the same one here for a sharper reason: the default this would fall back to
    -- `write_payment_stream`'s `minimum_rows=1` -- accepts a run that consumed one record
    of forty thousand and reports SUCCESS. This run's whole product is the count it prints,
    so a floor nobody chose is a number nobody can read.

    A NON-POSITIVE VALUE IS REFUSED TOO. `0` would turn the parameter into a switch for
    disabling the check, which is the phase's own species #1 arriving through the door
    built to prevent it: over zero rows, every dedup and multiset claim is true and free."""
    text = (value or "").strip()
    if not text.isdigit() or int(text) < 1:
        raise ValueError(
            f"refusing to {action}: minimum_rows={value!r} is not a positive whole number. "
            "Launch with the count you published, e.g. --params "
            f"revision=$(git rev-parse HEAD),minimum_rows=40150. The default is "
            f"{SENTINEL_MINIMUM_ROWS!r}, which exists to be refused: a run that declares no "
            "floor can consume one record of forty thousand and report SUCCESS."
        )
    return int(text)
