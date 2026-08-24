# databricks/src/stream_managed_broker.py
"""Job task: read the payment topic from the MANAGED Redpanda cluster over SASL_SSL and
land it in Delta -- ON THE PLATFORM THIS LAKEHOUSE DEPLOYS TO.

THIS IS NOT THE EXACTLY-ONCE PROOF, AND ANYONE READING IT AS ONE HAS F5 BACKWARDS. The
proof is `opl.streaming.exactly_once`, it runs LOCALLY, and it stays local for a reason no
managed broker weakens: it needs a fault injected BETWEEN the data commit and the offset
commit, and a serverless task is the one place this project cannot kill a process on
purpose. Its NAIVE control arm exists to duplicate, and an arm that stopped duplicating
would mean the fault missed its window rather than that anything was exactly-once. Nothing
here injects a fault, nothing here is a control arm, and this sink -- `format("delta")` --
is exactly-once BY CONSTRUCTION, so an experiment run against it reports success under
every outcome including "nothing ran".

WHAT IT DOES PROVE, WHICH THE LOCAL HALF CANNOT: that this lakehouse ingests an event
STREAM on the platform it deploys to. Until this task, event streams reached the workspace
only as FILES -- `generate_payments.py` writes JSON Lines into a Volume and Auto Loader
reads the directory -- so the "four sources" claim carried one honest gap, a Kafka topic
that only ever existed on the developer's laptop. F5 §0.2 falsified the design spec's
`Free nao conecta Kafka externo` at the socket, and §1.2 closed it end to end: a serverless
job completed the SCRAM exchange against the managed cluster and returned a row (job run
`382097683531247`). This task is that read, done through the SHIPPED code rather than
through a notebook probe.

AND IT IS NOT A SECOND CORPUS, A SECOND SERIALISER OR A SECOND SPELLING OF THE INGEST.
The bytes are the ones T1 publishes -- `opl.generator.events.to_jsonl` serialises them and
`opl.streaming.messages` frames them -- and the read is `opl.streaming.ingest.payment_stream`,
the same function every local run uses, handed two extra arguments. What
`opl.streaming.managed_broker` adds is the JVM client's spelling of a SASL credential and a
launch-declared row floor, neither of which is a read.

--- WHAT IT WRITES, AND WHY THAT TABLE IS NOT IN THE REGISTRY -----------------------------

`workspace.default.streaming_payments_managed_broker`, appended by a checkpointed
`availableNow` stream, carrying each record's partition, offset and RAW VALUE beside the
parsed contract columns (`opl.streaming.ingest` argues each of the three).

IT IS DELIBERATELY NOT REGISTERED IN `opl.bronze.REGISTRY`. F4's `dataops_reconciliation`
and `dataops_freshness` are TOTAL over that registry, so registering it would put a
permanent row in a freshness view for a source whose broker stops answering in days -- a
standing stale-alert for a table nobody can refresh, on the dashboard whose whole value is
that a red cell means something. This task's product is a RECORDED RUN, not a standing
source: a run id, a row count and this task's own output.

EXPECT ROWS WHOSE CONTRACT COLUMNS ARE ALL NULL, and that is not a defect. The topic already
holds the probe records §1.2 published (`opl-cloud-probe-...`), which are not payments;
`from_json` yields a struct of NULLs for a value it cannot parse. `kafka_value` keeps their
bytes, which is exactly why that column is kept.

--- IDEMPOTENCE UNDER A RETRY, WHICH `max_retries: 0` DOES NOT PREVENT --------------------

Measured on this workspace: 24 `(job_run_id, task_key)` pairs ran two attempts with
`max_retries: 0` declared. So this task must be safe to run twice, and it is, in two
independent ways.

  1. IT CANNOT DOUBLE-WRITE. The Delta streaming sink commits the micro-batch id into the
     Delta log transactionally with the rows, and ignores a replay of a batch id it already
     holds. A second attempt over the same checkpoint appends nothing.
  2. AND IT DOES NOT REPORT A SECOND SUCCESS EITHER. The checkpoint is fixed, so a second
     attempt resumes from it, consumes ZERO records, and `write_payment_stream`'s floor
     REFUSES -- the run goes FAILED rather than green over an empty read. That is the
     intended direction: this run's product is a count, and a green run that consumed
     nothing is the exact species F5 §5.1 hunts.

WHICH MAKES A SECOND RECORDED RUN A DELIBERATE ACT, and the repair is named here rather than
left to be discovered: delete the checkpoint directory AND drop the landed table, in that
order or either. A fresh checkpoint over a surviving table would re-read the topic from
`earliest` and append it a second time -- the same shape as the month-scoped Auto Loader
checkpoint hazard `opl.bronze.autoloader.checkpoint_location` documents.

--- CREDENTIALS: ONLY THE PASSWORD IS A SECRET, AND THAT WAS MEASURED THE HARD WAY --------

Databricks replaces every occurrence of a SECRET'S VALUE in task output. The Kafka username
is `opl` -- this repository's catalog, its schema-qualified table prefix, its topic, its job
names and its Python package -- so while it sat in the secret scope the run output rendered
`opl-cloud-probe-c84ef1c5` as `[REDACTED]-cloud-probe-c84ef1c5`, and a reader cannot tell a
redaction from a value. Controller-verified with a control: `kafka_user` was deleted from
the scope, the literal put in its place, nothing else changed, and the value came back
(job runs `382097683531247` then `838423822976396`).

SO THE USERNAME IS A LITERAL HERE. It is a coordinate, not a credential -- an ACL principal
name that opens nothing without the password. The BOOTSTRAP is in the scope for the
opposite reason: it is also a coordinate, but it names a host that must not be committed to
a public repository, and its redaction costs nothing because it collides with no word this
project publishes. The PASSWORD is in the scope because it is a secret, is never defaulted,
and reaches only the Kafka reader -- `opl.streaming.managed_broker.describe_reader_options`
is what this task prints instead of the option mapping that carries it.

argv: [minimum_rows] -- REQUIRED, no default. See
`opl.streaming.managed_broker.require_minimum_rows`: this run's product is the count it
prints, so the count it must reach is declared at launch rather than defaulted to 1."""
import sys

from pyspark.sql import SparkSession

from opl.config import DEFAULT
from opl.streaming.ingest import payment_stream, write_payment_stream
from opl.streaming.managed_broker import (
    describe_reader_options,
    require_minimum_rows,
    sasl_reader_options,
)
from opl.streaming.producer import BrokerConfig

# The topic T1 publishes to on the managed cluster. A literal and not a job parameter: it
# is fixed, and a coordinate that can be wrong for no gain is what the ingestion jobs' paste
# lock exists to catch.
TOPIC = "opl-payments"

# Where the credential lives. The scope name is this workspace's, the two keys are the two
# values that must not be committed. `kafka_user` is ABSENT ON PURPOSE -- see the header.
SECRET_SCOPE = "opl"
BOOTSTRAP_SECRET_KEY = "kafka_bootstrap"
PASSWORD_SECRET_KEY = "kafka_password"

# A coordinate, committed deliberately. See the header's credentials section.
SASL_USERNAME = "opl"

# NOT a registered table, and the header says why. Only the leaf name is spelled here;
# the catalog and schema come from `opl.config` like every other table in this repository.
LANDED_TABLE = "streaming_payments_managed_broker"

# Streaming state lives in the Volume beside Auto Loader's, and `managed-broker` cannot
# collide with the month directories it sits next to: a month is `\d{4}-\d{2}`
# (`opl.config.require_month`) and this is not one. Not month-scoped, because a topic has
# no month -- the offsets are the state, and there is exactly one topic.
CHECKPOINT = f"{DEFAULT.volume_root}/_checkpoints/managed-broker/{TOPIC}"


def _secret(key: str) -> str:
    """One value out of the secret scope, refused if it is blank.

    `dbutils` is imported HERE and not at module scope for `dq_gate_batch._publish`'s
    reason: `databricks.sdk.runtime` builds a workspace client on import and raises without
    workspace credentials, which would make this task unimportable -- and so untestable --
    outside Databricks.

    A BLANK VALUE IS REFUSED rather than passed on. `dbutils.secrets.get` raises for a key
    that is absent, but a key STORED empty comes back as `""` -- and an empty password
    reaches the broker as a failed SCRAM exchange, whose error text is the same one an
    expired trial and a revoked ACL produce."""
    from databricks.sdk.runtime import dbutils
    value = dbutils.secrets.get(SECRET_SCOPE, key)
    if not value.strip():
        raise ValueError(
            f"secret {SECRET_SCOPE}/{key} is empty. Set it before launching this job: an "
            "empty bootstrap or password fails at the broker with the same message an "
            "expired trial, a revoked ACL and a wrong username all produce."
        )
    return value


def _broker() -> BrokerConfig:
    """The managed cluster's coordinates and credential.

    `BrokerConfig` rather than three locals, for the two guards it carries: its
    `__post_init__` refuses half a credential -- a username with no password would speak
    PLAINTEXT to a SASL_SSL listener and time out -- and its `password` field is
    `repr=False`, so an instance reaching a traceback in a job log does not print it."""
    return BrokerConfig(
        bootstrap=_secret(BOOTSTRAP_SECRET_KEY),
        username=SASL_USERNAME,
        password=_secret(PASSWORD_SECRET_KEY),
    )


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    if len(args) > 1:
        raise ValueError(
            f"stream_managed_broker takes exactly one argument, the row floor, and was "
            f"handed {args!r}. Everything else -- the topic, the table, the checkpoint and "
            "the username -- is fixed in this file, because a coordinate that can be wrong "
            "for no gain is what the ingestion jobs' paste lock exists to catch."
        )
    minimum_rows = require_minimum_rows(
        args[0] if args else None, action="read the managed broker"
    )
    broker = _broker()
    options = sasl_reader_options(broker)
    table = DEFAULT.table(LANDED_TABLE)
    # Printed BEFORE the read, so a handshake failure is read against what was configured.
    # `describe_reader_options` and not the mapping: one of its values is the password.
    print(
        f"stream_managed_broker: topic={TOPIC} -> {table} | checkpoint={CHECKPOINT} | "
        f"floor={minimum_rows} rows | {describe_reader_options(options)}"
    )
    spark = SparkSession.builder.getOrCreate()
    frame = payment_stream(
        spark, topic=TOPIC, bootstrap=broker.bootstrap, broker_options=options
    )
    ingested = write_payment_stream(
        frame,
        table=table,
        checkpoint=CHECKPOINT,
        topic=TOPIC,
        minimum_rows=minimum_rows,
    )
    print(
        f"stream_managed_broker: consumed {ingested.input_rows} records from {TOPIC!r} "
        f"across batches {ingested.batch_ids} into {table}. That is a count of records "
        "READ from the managed broker on serverless compute -- it is not, and must not be "
        "quoted as, evidence about exactly-once processing (see this task's header)."
    )


if __name__ == "__main__":
    main()
