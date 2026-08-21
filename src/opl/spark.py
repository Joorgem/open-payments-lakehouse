# src/opl/spark.py
"""Local Delta-enabled SparkSession factory. Mirrors the config Databricks
applies automatically, so code tested here behaves the same on DBR 16.4.

This is the ONE place that describes the session this project runs against
locally, which is why the resource declarations below live here rather than in
the pytest fixture that wraps it: a fixture configuring the session differently
would make this module's claim false for every test that reads it.
"""
import os
import sys

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from opl.config import SESSION_TIMEZONE, SESSION_TIMEZONE_CONFIG

# The Spark option that names the Maven coordinates a session resolves at startup.
# Named because it is READ BACK below, and a second spelling of it would make the
# refusal silently unable to see the package it is looking for.
PACKAGES_CONFIG = "spark.jars.packages"

# THE KAFKA CONNECTOR IS NOT BUNDLED WITH PYSPARK, measured: nothing matching `kafka`
# ships in the wheel's `jars/`. It resolves from Maven -- 11 artifacts, 57,002 kB, ~12 s
# on a cold Ivy cache -- which is why `local_session` takes it as an OPT-IN that defaults
# off (F5 plan T2, pre-decided): this factory builds the session for the whole suite, and
# putting the coordinate here unconditionally would give all 2,683 tests a Maven
# resolution and a network dependency at session start, in a CI job that today needs
# neither.
#
# THE VERSION IS DERIVED FROM THE RUNNING PYSPARK RATHER THAN PINNED, because the
# connector artifact is versioned in lockstep with Spark and a hardcoded `3.5.9` would
# survive a pyspark bump by resolving the WRONG connector -- a mismatch whose symptom is a
# NoSuchMethodError inside the source, naming neither version. Scala 2.12 is not derived
# because nothing exposes it: `delta.pip_utils.configure_spark_with_delta_pip` hardcodes
# the same "2.12" for delta's own coordinate, so the two agree by construction.
KAFKA_CONNECTOR_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"


def _pin_worker_interpreter() -> None:
    """Make Spark's Python workers run the interpreter the driver runs.

    Measured on this machine, not theoretical: with PYSPARK_PYTHON unset, Spark
    launches its workers from the first `python` on PATH -- 3.14 -- against a
    3.12 driver, and every Spark test dies inside py4j with
    ``AssertionError: SRE module mismatch``, a message naming neither Python nor
    PATH and pointing at neither.

    ``sys.executable`` rather than a hardcoded path, so it is right in the venv,
    in CI, and in whatever environment comes next. Set here rather than in the
    test fixture because it has to be set BEFORE a SparkContext exists -- that is
    when the worker command is captured -- and this function is what creates it.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def _delta_builder(app_name: str) -> SparkSession.Builder:
    """The session config itself. Split out of `local_session` for the 50-line cap only:
    everything this project declares about a local session is still declared HERE, which
    is the claim the module docstring makes."""
    return (
        SparkSession.builder.appName(app_name)
        # local[2] and an explicit driver memory DECLARE what this session needs
        # instead of leaving it implicit. The failure they make legible: at
        # ~1.8 GB free, 14 tests fail with a Delta BlockManager error that never
        # mentions memory, while the same commit is green in CI. local[*] gives
        # one executor slot per core, which multiplies the memory each shuffle
        # wants on a laptop that is also running an IDE -- and nothing here is
        # throughput-bound, so the slots buy nothing back.
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        # THE ZONE, DECLARED, because leaving it out is a DECISION to inherit the
        # operating system's -- and the suite then asserts instants no Databricks run
        # would ever write. Serverless defaults to UTC; this box is UTC-3, and
        # `opl.gold.dimensions` turns a DATE into an instant, so an unpinned local
        # session put every version boundary three hours off the payment stream's own
        # UTC clock. `opl.config` argues the value; this is the half of the pin that
        # covers the SESSION FACTORY, and each gold entry point sets the same pair on
        # the session serverless hands it.
        #
        # WHAT IT COSTS, STATED SO IT IS NOT REDISCOVERED AS A BUG: pyspark converts a
        # TIMESTAMP back to Python through `datetime.fromtimestamp`, which reads the
        # OPERATING SYSTEM's zone, not this one -- so on a box that is not UTC,
        # `collect()` hands back a datetime shifted by the offset. That is not a
        # dimension that is wrong, it is two zones meeting in the driver; the gold
        # fixtures compare against `instant_literal`'s own round trip for that reason.
        .config(SESSION_TIMEZONE_CONFIG, SESSION_TIMEZONE)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "./spark-warehouse")
        .config("spark.ui.enabled", "false")
    )


def _refuse_a_session_that_cannot_read_kafka(session: SparkSession) -> None:
    """Refuse a session whose SparkConf does not carry the Kafka connector coordinate.

    WHAT IT READS IS THAT CONF AND NOTHING ELSE, so what it can refuse is a session that was
    never ASKED for the coordinate -- not a request that was made and failed to resolve. A
    session whose SparkConf carries the coordinate passes here on that alone: the string is
    the whole of the evidence, and no jar is looked for.

    THE HAZARD IS `getOrCreate`, NOT THE COORDINATE. `spark.jars.packages` is a STATIC
    configuration: it is applied when the SparkContext is created and ignored on every
    later builder. The JVM behind this factory is process-wide, so a test that took the
    suite's ordinary session first and then asked for `kafka=True` gets the SAME
    Kafka-less session back, silently -- and fails much later, inside `readStream`, with
    ``Failed to find data source: kafka``, a message that names a missing data source and
    not the session that was already open. Refusing here names the actual cause while the
    caller still has a stack that points at it."""
    resolved = session.sparkContext.getConf().get(PACKAGES_CONFIG, "")
    if KAFKA_CONNECTOR_PACKAGE in resolved:
        return
    raise RuntimeError(
        f"this session resolves {PACKAGES_CONFIG}={resolved!r}, which does not carry "
        f"{KAFKA_CONNECTOR_PACKAGE}. `{PACKAGES_CONFIG}` is applied only when the "
        "SparkContext is created, and `getOrCreate` returns the session this JVM already "
        "has -- so a Kafka-less session was opened earlier in this process. Run the "
        "Kafka-reading tests in their own invocation (`-m redpanda`), which is what the "
        "marker is for."
    )


def local_session(app_name: str = "opl", *, kafka: bool = False) -> SparkSession:
    """The local Delta session. `kafka=True` also resolves the Structured Streaming
    Kafka connector from Maven.

    THE ARGUMENT DEFAULTS OFF AND THAT IS THE WHOLE POINT -- see `KAFKA_CONNECTOR_PACKAGE`
    for the cost it keeps off the 2,683 tests that do not read a broker. An argument here
    rather than a second builder beside it because this module's docstring claims to be the
    ONE place a local session is described, and a second builder repeating the master, the
    driver memory, the shuffle partitions and the timezone pin would be a copy that drifts
    -- the exact reason `tests/conftest.py` wraps this factory instead of configuring its
    own."""
    _pin_worker_interpreter()
    extra_packages = [KAFKA_CONNECTOR_PACKAGE] if kafka else []
    session = configure_spark_with_delta_pip(
        _delta_builder(app_name), extra_packages=extra_packages
    ).getOrCreate()
    if kafka:
        _refuse_a_session_that_cannot_read_kafka(session)
    return session
