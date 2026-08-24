# tests/test_stream_managed_broker_task.py
"""Unit test for the `databricks/src/stream_managed_broker.py` job task.

LOADED BY PATH with importlib, the same way every other `databricks/src` task test here
loads its subject -- these scripts are job entry points synced into the workspace, not part
of the opl wheel, so there is no package to import them from.

WHAT THIS FILE CAN CHECK AND WHAT IT DELIBERATELY CANNOT. It can check every refusal that
happens before a broker is reached, and it can check the WIRING -- which secret keys are
read, which one is a literal, where the rows land, where the checkpoint sits. It cannot
check the read: that needs the managed cluster, a serverless session and credentials none
of which exist in CI, and it is the reason T8's product is a recorded run rather than a
green test. The one measurement that settles the platform question was taken once, by a
run: F5 §1.2, job run `382097683531247`.

THE SECRET-SCOPE ASSERTIONS BELOW ARE THE POINT OF THIS FILE. Databricks scrubs every
occurrence of a SECRET'S VALUE from task output, and the Kafka username is `opl` -- this
repository's catalog, topic, package and job prefix. Putting it in the scope once turned
`opl-cloud-probe-c84ef1c5` into `[REDACTED]-cloud-probe-c84ef1c5` in a run's own output,
measured with a control (job runs `382097683531247` then `838423822976396`). That finding
lives in a document; the lock that keeps it true lives here."""
from __future__ import annotations

import ast
import importlib.util
import sys
import types
from pathlib import Path

import pytest

from opl.bronze.registry import REGISTRY
from opl.config import DEFAULT, is_month
from opl.streaming.managed_broker import SENTINEL_MINIMUM_ROWS

_SRC = Path(__file__).resolve().parents[1] / "databricks" / "src"
_TASK = "stream_managed_broker"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tree() -> ast.Module:
    return ast.parse((_SRC / f"{_TASK}.py").read_text(encoding="utf-8"), filename=f"{_TASK}.py")


def _function(name: str) -> ast.FunctionDef:
    found = [n for n in _tree().body if isinstance(n, ast.FunctionDef) and n.name == name]
    assert len(found) == 1, f"{_TASK}.py does not define exactly one module-level {name}()"
    return found[0]


TASK = _load(_TASK)


def test_the_task_imports_without_a_workspace_at_all():
    """`TASK` above is the assertion: loading the module runs its imports.

    `databricks.sdk.runtime` builds a workspace client ON IMPORT and raises without
    credentials, so a module-level import of it would make this task unimportable -- and
    therefore untestable -- anywhere but inside Databricks. `dq_gate_batch` learnt that
    once; this pins it from the AST so the lesson does not have to be relearnt by whoever
    tidies the import block."""
    module_level: set[str] = set()
    for node in _tree().body:
        if isinstance(node, ast.Import):
            module_level |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            module_level.add(node.module.split(".")[0])
    assert "databricks" not in module_level, (
        "the task imports `databricks` at module scope; `databricks.sdk.runtime` raises "
        "without workspace credentials, which makes this file unimportable outside the "
        "platform. Import it inside the function that uses it, as dq_gate_batch does"
    )


@pytest.mark.parametrize("argv", [["1", "2"], ["1", "extra"], ["a", "b", "c"]])
def test_a_second_argument_is_refused_rather_than_ignored(argv):
    """Everything but the floor is fixed in the task file, so a second argument is a launch
    that believes it is configuring something. A task given a parameter it does not read is
    the failure the ingestion jobs' paste lock exists for, reached from the YAML side."""
    with pytest.raises(ValueError, match="exactly one argument"):
        TASK.main(argv)


@pytest.mark.parametrize("argv", [[], [""], [SENTINEL_MINIMUM_ROWS], ["0"]])
def test_a_run_without_a_declared_floor_refuses_before_a_session_is_started(argv):
    """AND BEFORE A SECRET IS READ, which is what makes this test possible at all: the
    refusal happens in `require_minimum_rows`, ahead of `_broker()` and ahead of
    `SparkSession.builder.getOrCreate()`. A floor checked after the read would still be a
    floor, but this test would need a workspace to reach it -- and the run would already
    have paid for a serverless start and a full topic scan before being told."""
    with pytest.raises(ValueError, match="minimum_rows="):
        TASK.main(argv)


def test_only_the_two_coordinates_that_cannot_be_committed_come_from_the_secret_scope():
    """THE REDACTION RULE, LOCKED FROM THE AST.

    `_broker` reads exactly two secrets -- the bootstrap and the password -- and hands the
    USERNAME as a module-level literal. Read structurally rather than by grepping for
    strings: what matters is which expression fills `username=`, and a `_secret("...")`
    there would be the defect no string search distinguishes from a comment about it.

    TOTAL OVER EVERY ARGUMENT SHAPE, through `ast.unparse`, and that is a repair rather
    than a flourish: reading only `ast.Name` arguments made `_secret("kafka_user")` -- a
    LITERAL, the exact form the paragraph above calls the defect -- invisible here.
    Unparsed, a literal reads back as `'kafka_user'` and fails against the constant names
    this list is supposed to hold, and a keyword call reaches the same comparison."""
    broker = _function("_broker")
    calls = [
        node
        for node in ast.walk(broker)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id == "_secret"
    ]
    read = sorted(
        ast.unparse(argument)
        for call in calls
        for argument in (*call.args, *(keyword.value for keyword in call.keywords))
    )
    assert read == ["BOOTSTRAP_SECRET_KEY", "PASSWORD_SECRET_KEY"], (
        f"_broker reads {read} out of the secret scope. Exactly two values must: the "
        "bootstrap, which names a host that must not be committed, and the password. The "
        "USERNAME must not -- it is `opl`, and a secret's value is scrubbed from every "
        "line of task output, so putting it there renders this repository's own catalog, "
        "topic and job prefix as [REDACTED] in the run that is supposed to be the evidence"
    )


def test_the_sasl_username_is_a_module_level_literal_and_not_a_lookup():
    """The other half of the rule above, from the constructor's side."""
    broker = _function("_broker")
    constructions = [
        node
        for node in ast.walk(broker)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "BrokerConfig"
    ]
    assert len(constructions) == 1
    handed = {kw.arg: kw.value for kw in constructions[0].keywords}
    assert isinstance(handed["username"], ast.Name)
    assert handed["username"].id == "SASL_USERNAME"
    assert TASK.SASL_USERNAME == "opl"
    assert "user" not in TASK.BOOTSTRAP_SECRET_KEY and "user" not in TASK.PASSWORD_SECRET_KEY


class _RecordingSecrets:
    """`dbutils.secrets` reduced to the one method `_secret` calls, recording every ask."""

    def __init__(self, values: dict[str, str]) -> None:
        self.values = values
        self.asked: list[tuple[str, str]] = []

    def get(self, scope: str, key: str) -> str:
        self.asked.append((scope, key))
        return self.values[key]


def _stub_the_runtime(monkeypatch, values: dict[str, str]) -> _RecordingSecrets:
    """A fake `databricks.sdk.runtime` in `sys.modules`, so `_secret` can be RUN.

    THIS IS WHAT TURNS TWO AST ASSERTIONS INTO EXECUTED CODE. Everything above this line
    reads the tree because `_secret` reaches for the workspace, and the reason it reaches
    for it from INSIDE the function is the import rule the first test in this file pins.

    SEEDED UNDER THE FULL DOTTED NAME, and that is what keeps the real package out of it:
    measured, `from databricks.sdk.runtime import ...` then resolves to this object with
    neither `databricks` nor `databricks.sdk` entering `sys.modules` at all. `monkeypatch`
    removes the entry again, so no later test in the session sees it."""
    secrets = _RecordingSecrets(values)
    runtime = types.ModuleType("databricks.sdk.runtime")
    runtime.dbutils = types.SimpleNamespace(secrets=secrets)
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", runtime)
    return secrets


def test_the_broker_is_built_from_the_two_secrets_and_the_literal_username(monkeypatch):
    """THE WIRING, EXECUTED -- the same rule the two AST locks above state, reached from
    the other side.

    A structural assertion says which expression fills `username=`; this says which scope
    and which keys the task actually ASKS FOR at run time, in order, and that the object it
    builds does not print the password when a traceback renders it (`repr=False`, which is
    a decision `BrokerConfig` makes and this task depends on)."""
    secrets = _stub_the_runtime(
        monkeypatch,
        {"kafka_bootstrap": "seed.example:9092", "kafka_password": "not-the-real-one-0000"},
    )
    broker = TASK._broker()
    assert secrets.asked == [("opl", "kafka_bootstrap"), ("opl", "kafka_password")]
    assert broker.bootstrap == "seed.example:9092"
    assert broker.username == "opl" and broker.password == "not-the-real-one-0000"
    assert "not-the-real-one-0000" not in repr(broker)


@pytest.mark.parametrize("blank", ["", "   "])
@pytest.mark.parametrize("key", ["kafka_bootstrap", "kafka_password"])
def test_a_secret_stored_blank_is_refused_by_name(monkeypatch, key, blank):
    """THE ARM THAT NEVER FIRED IN ANY TEST until this one.

    `_secret`'s blank refusal is for a key stored EMPTY rather than one that is absent --
    the task's own docstring argues which case is which -- and nothing had ever executed
    it. What this pins is that it fires on EITHER key and names which secret is empty.

    AND FOR THE PASSWORD IT IS THE ONLY THING THAT FIRES AT ALL. Measured:
    `BrokerConfig(bootstrap=..., username="opl", password="")` is ACCEPTED --
    `_refuse_half_a_credential` compares presence, not content -- so without this branch a
    blank password travels to the SCRAM exchange and comes back as the one error string
    ADR 0018 counts across four worlds. A blank BOOTSTRAP would at least be refused by
    `BrokerConfig.__post_init__`, but by the environment variable's name rather than by the
    scope key an operator has to go and fix."""
    values = {"kafka_bootstrap": "seed.example:9092", "kafka_password": "not-the-real-one-0000"}
    _stub_the_runtime(monkeypatch, {**values, key: blank})
    with pytest.raises(ValueError, match=f"secret opl/{key} is empty"):
        TASK._broker()


@pytest.mark.parametrize("padded", ["seed.example:9092\n", " seed.example:9092", "secret\n"])
@pytest.mark.parametrize("key", ["kafka_bootstrap", "kafka_password"])
def test_a_secret_stored_with_surrounding_whitespace_is_refused_by_name(
    monkeypatch, key, padded
):
    """THE ARM THE BLANK CHECK LOOKED LIKE IT COVERED AND DID NOT.

    `_secret` tested `value.strip()` and returned `value`: it VALIDATED one string and
    RETURNED another. A scope value set with a trailing newline -- which is what a paste out
    of a terminal or a file carries -- therefore passed the blank test and reached the
    broker with the newline on it, where it fails the SCRAM exchange or resolves as a host
    nobody is listening on. Both come back as the ONE ERROR STRING this task's neighbours
    exist to stop having a fifth cause (ADR 0018 counts four).

    THE COMPARISON NEXT DOOR IS WHAT MADE IT VISIBLE.
    `opl.streaming.managed_broker.require_minimum_rows` strips what it parses; this did not,
    and the two sit four lines apart in the same task's call graph.

    REFUSED, NOT TRIMMED, and the accepted case below is where that is asserted: a value
    with no padding comes back BYTE FOR BYTE, so a repair that normalised instead would be
    caught here rather than in a broker's error text."""
    values = {"kafka_bootstrap": "seed.example:9092", "kafka_password": "not-the-real-one-0000"}
    _stub_the_runtime(monkeypatch, {**values, key: padded})
    with pytest.raises(ValueError, match=f"secret opl/{key} has leading or trailing"):
        TASK._broker()


def test_an_unpadded_secret_is_returned_exactly_as_the_scope_holds_it(monkeypatch):
    """THE FLOOR UNDER THE REFUSAL ABOVE, and the half that says it is a refusal.

    A guard that trimmed would pass every case above and change what the broker is handed;
    a guard that refused everything would pass them too and stop the task dead. So the
    accepted value is compared for equality with what the scope holds, and it is one whose
    INTERIOR spacing would not survive a `.strip()` mistake spelled as `.replace`."""
    interior = "seed.example:9092,seed2.example:9092"
    _stub_the_runtime(
        monkeypatch, {"kafka_bootstrap": interior, "kafka_password": "not the real one"}
    )
    broker = TASK._broker()
    assert broker.bootstrap == interior
    assert broker.password == "not the real one"


def test_no_print_in_this_task_names_the_option_mapping_or_the_broker():
    """ONE OF THE OPTION VALUES IS THE PASSWORD, and this run's output is what the evidence
    document quotes from.

    `kafka.sasl.jaas.config` carries the credential in clear -- it is the only shape the
    JVM client accepts -- and Spark's own log redaction is keyed on an option NAME matching
    `secret|password|token`, which that one does not. So the task prints
    `describe_reader_options(...)` and never the mapping, and never a `BrokerConfig`
    either. Checked over the AST, so the prose in this file's own docstring explaining the
    hazard cannot trip it.

    WHAT IT CATCHES IS THE IDENTIFIER, WHICH IS NARROWER THAN THE HAZARD -- stated because
    the assertion looks stronger than it is. It walks `print(` calls and looks for the
    NAMES `options` and `broker`, so `print(f"...{options}")` fires it -- and each of these
    was measured GREEN against it: `opts = options` then `print(f"...{opts}")`, `b = broker`
    then `print(f"...{b.password}")`, `sys.stdout.write(f"...{options}")`,
    `logging.info(f"...{broker.password}")`, and an f-string inside a `raise`. No such line
    ships in this task; what this test does is keep the direct re-introduction out.

    THE DEFENCE THAT DOES NOT DEPEND ON THIS FILE is the platform's, and it is why the
    password -- and only the password -- is in the secret scope: Databricks replaces every
    occurrence of a SECRET'S VALUE in task output. F5 §1.2 measured that from the other
    end, with a control: while the USERNAME sat in the scope too, the run's own output
    rendered `opl-cloud-probe-c84ef1c5` as `[REDACTED]-cloud-probe-c84ef1c5`."""
    for call in [
        node
        for node in ast.walk(_tree())
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "print"
    ]:
        described = {
            id(inner)
            for node in ast.walk(call)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "describe_reader_options"
            for inner in ast.walk(node)
        }
        reached = {
            node.id
            for node in ast.walk(call)
            if isinstance(node, ast.Name) and id(node) not in described
        }
        assert not reached & {"options", "broker"}, (
            f"a print() in {_TASK}.py reaches {sorted(reached & {'options', 'broker'})} "
            "directly. One of the reader options carries the SASL password, and a "
            "BrokerConfig's repr only hides it because of one `repr=False`"
        )


def test_the_landed_table_is_deliberately_not_one_the_registry_knows():
    """THE PRE-DECISION, ASSERTED, because an absence is otherwise indistinguishable from
    an omission.

    F4's `dataops_reconciliation` and `dataops_freshness` are TOTAL over `REGISTRY`, so
    registering this table would put a permanent row in a freshness view for a source whose
    broker stops answering within days of this being written -- a standing stale alert for
    a table nobody can refresh. Only the LEAF name is spelled in the task; the catalog and
    schema come from `opl.config` like every other table here."""
    registered = {
        name
        for spec in REGISTRY.values()
        for name in (spec.table_key, spec.staging, spec.bronze, spec.quarantine)
    }
    assert TASK.LANDED_TABLE not in registered
    assert TASK.LANDED_TABLE not in REGISTRY
    assert not TASK.LANDED_TABLE.startswith("bronze_"), (
        "the leaf name reads as a registered bronze table and is not one"
    )


def test_the_checkpoint_sits_in_the_volume_and_cannot_be_read_as_a_month():
    """WHERE THE STREAMING STATE LIVES, and the one collision it could have had.

    Auto Loader's checkpoints are `_checkpoints/<month>/<table_key>`
    (`opl.bronze.autoloader.checkpoint_location`). This one is a SIBLING of those month
    directories, so the property worth pinning is that its first segment cannot be mistaken
    for a month -- an operator clearing `_checkpoints/2026-06` must not be able to reach
    this, and a month-scoped sweep must not find it."""
    prefix = f"{DEFAULT.volume_root}/_checkpoints/"
    assert TASK.CHECKPOINT.startswith(prefix)
    segments = TASK.CHECKPOINT[len(prefix):].split("/")
    assert not any(is_month(segment) for segment in segments), (
        f"{TASK.CHECKPOINT} carries a month-shaped segment, so it sits inside -- or looks "
        "like -- the month-scoped state every ingestion job's checkpoint uses"
    )
    assert segments[-1] == TASK.TOPIC


def test_the_read_starts_from_the_beginning_of_the_topic_and_the_task_says_so_nowhere_else():
    """`payment_stream`'s DEFAULT is `earliest`, and this task takes it.

    The assertion is the ABSENCE of an override: `latest` over an already-published topic
    reads zero records, and this task's whole product is a count. `opl.streaming.ingest`
    refuses that value outright in shipped code -- this pins that the task does not reach
    for the parameter at all, which is one fewer place for it to be wrong."""
    handed = {
        kw.arg
        for node in ast.walk(_function("main"))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "payment_stream"
        for kw in node.keywords
    }
    assert handed == {"topic", "bootstrap", "broker_options"}, (
        f"the task hands payment_stream {sorted(handed)}. `starting_offsets` is not among "
        "them on purpose: the default is `earliest` and the alternative reads nothing"
    )


def test_the_floor_the_launch_declared_is_the_floor_the_sink_is_given():
    """THE PARAMETER'S WHOLE PATH, in one assertion.

    A task that parsed `minimum_rows` and then let `write_payment_stream` fall back on its
    own default would refuse a bad launch and measure against 1 anyway -- green over a run
    that consumed one record of forty thousand, which is precisely what the parameter
    exists to prevent."""
    calls = [
        node
        for node in ast.walk(_function("main"))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "write_payment_stream"
    ]
    assert len(calls) == 1
    handed = {kw.arg: kw.value for kw in calls[0].keywords}
    assert set(handed) == {"table", "checkpoint", "topic", "minimum_rows"}, (
        f"the sink is handed {sorted(handed)}. `path` must not be among them: Unity "
        "Catalog refuses a Delta table inside a Volume, so a serverless run writes to a "
        "NAME"
    )
    assert isinstance(handed["minimum_rows"], ast.Name)
    assert handed["minimum_rows"].id == "minimum_rows"


def test_the_run_says_whether_the_count_it_prints_is_the_whole_count():
    """THE COUNT AND ITS STANDING, IN ONE LINE OF OUTPUT.

    `input_rows` is summed out of a RING BUFFER, and the config sizing that buffer is one
    serverless refuses to read -- job run `570309961086740` died on
    `[CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION]` after landing 10,151 rows. So the count
    this task prints is the run's total under one argument and a LOWER BOUND under
    another, and `RingBufferReading.describe()` is the sentence that says which.

    ASSERTED HERE BECAUSE THE OUTPUT IS THE PRODUCT. This task's own header calls a
    recorded run its product; a run whose output states a number without its standing is
    the number quoted into an evidence document with the caveat dropped."""
    described = [
        node
        for call in ast.walk(_function("main"))
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "print"
        for node in ast.walk(call)
        if isinstance(node, ast.Attribute) and node.attr == "describe"
    ]
    assert described, (
        f"no print() in {_TASK}.py:main reaches `.describe()`. The run would state its "
        "row count without saying whether the progress ring it was summed from could have "
        "dropped batches -- which on this platform is a question the session refuses to "
        "answer, not one that answers itself"
    )
