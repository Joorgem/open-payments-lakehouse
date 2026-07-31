# tests/test_ingest_tasks_batch_id.py
"""Both ingest job tasks must refuse to run without a batch id.

They used to default it to the literal `"manual"`. That is worse than a crash:
the gate and the promote are scoped to `_batch_id == {{job.run_id}}`, so rows
tagged `"manual"` land in staging and are then never evaluated, never promoted
and never reported anywhere -- a silent hole, not a failure. Both job YAMLs do
pass `{{job.run_id}}`, so the default was only ever reachable by a manual or
misconfigured invocation, which is exactly the case that must be loud.

Since the ingest was parameterised by table, the same file also locks the two
refusals that now precede the batch id: an unregistered table name, and the
lookup handed to the generic task that cannot ingest it. F1.4 added the one that
follows it -- a missing month, which used to become `opl.config`'s pinned 2026-06
and was therefore the only one of the four that failed SILENTLY, stamping
`_snapshot_month` and choosing a landing dir with nothing in the log to say so.
All four are one property -- bad arguments are refused before a Spark session is
started.

Loaded by path with the same importlib pattern as the other task tests -- the
`databricks/src` scripts are job entry points, not part of the opl wheel. No JVM
and no workspace: `main` refuses before it touches Spark, which is the point.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from opl.bronze.promote import PromoteRefused

_SRC = Path(__file__).resolve().parents[1] / "databricks" / "src"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Script AND argv together, not two independent parametrizes: the batch id sits at
# a different argv position in each script now (`bronze_ingest` takes the table
# first), so "the argv with no batch id" is no longer one list shared by both.
@pytest.mark.parametrize(
    "script,argv",
    [
        ("bronze_ingest", ["estabelecimentos"]),
        ("bronze_ingest", ["estabelecimentos", ""]),
        ("bronze_ingest", ["estabelecimentos", "   "]),
        ("bronze_lookup_ingest", []),
        ("bronze_lookup_ingest", [""]),
        ("bronze_lookup_ingest", ["   "]),
    ],
)
def test_an_ingest_without_a_batch_id_is_refused(script, argv):
    module = _load(script)
    with pytest.raises(PromoteRefused) as excinfo:
        module.main(argv)
    # The operator has to learn WHICH id is missing and where it comes from.
    assert "batch" in str(excinfo.value).lower()


def test_an_ingest_of_an_unknown_table_is_refused_before_spark():
    """A mistyped table is refused by the registry, and BEFORE the batch id.

    Order matters: this argv also carries a valid batch id, so the only refusal
    that can fire is the table one. The refusal names the registered tables --
    an operator reading a Databricks run log has to learn what to type instead,
    and must not have waited for a serverless session to be told."""
    from opl.bronze.registry import UnknownTable

    module = _load("bronze_ingest")
    with pytest.raises(UnknownTable) as excinfo:
        module.main(["estabelecimento", "12345"])  # a real typo: singular
    assert "estabelecimentos" in str(excinfo.value)


def test_ingesting_the_lookup_through_the_generic_task_is_refused_before_spark():
    """The generic stream cannot ingest the lookup, and says which task can.

    No job YAML points the lookup at this file, so this is the manual or
    misconfigured invocation -- the same case the batch-id refusal exists for. It
    has to refuse HERE rather than let the run proceed: the generic stream adds no
    `lookup_type`, so the rows would be discovered, read and only then rejected by
    the Delta append's schema check -- after a serverless session and a full scan.
    The batch id is valid in this argv, so the table refusal is the only one that
    can fire."""
    module = _load("bronze_ingest")
    with pytest.raises(ValueError) as excinfo:
        module.main(["lookup", "12345"])
    assert "bronze_lookup_ingest" in str(excinfo.value)


def test_the_refusal_points_elsewhere_only_when_somewhere_else_exists():
    """The refusal may say less about an unfamiliar table; it may not say something false.

    The guard tests a LANDING MODE, but the sentence "run bronze_lookup_ingest.py
    instead" is only true of the lookup. They coincide today. Registering any other
    locally-landed table separates them, and a message that pointed that table's
    operator at the lookup's task would be this repo's founding defect in miniature
    -- a confident instruction to the wrong place. Probed with a spec that cannot
    exist in the registry yet, which is exactly why the branch is untested otherwise."""
    from opl.bronze.registry import LANDING_LOCAL, BronzeTable

    module = _load("bronze_ingest")
    not_the_lookup = BronzeTable(
        name="socios",
        contract="lookup",
        table_key="bronze_cnpj_socios",
        staging="bronze_cnpj_socios_staging",
        bronze="bronze_cnpj_socios",
        quarantine="bronze_cnpj_socios_quarantine",
        subdir="socios",
        landing=LANDING_LOCAL,
        prefix="Socios",
        constraints=(),
    )
    message = module._cannot_ingest(not_the_lookup)
    assert "socios" in message
    assert "bronze_lookup_ingest" not in message, (
        "the refusal sends a non-lookup table's operator to the lookup's entry point"
    )
    assert "filename suffix" not in message


# Script AND argv again, for the same reason: the month sits at argv[2] in
# `bronze_ingest` (which takes the table first) and argv[1] in the lookup task. Both
# argvs carry a VALID batch id, so the month refusal is the only one that can fire.
@pytest.mark.parametrize(
    "script,argv",
    [
        ("bronze_ingest", ["estabelecimentos", "12345"]),
        ("bronze_ingest", ["estabelecimentos", "12345", ""]),
        ("bronze_ingest", ["estabelecimentos", "12345", "   "]),
        ("bronze_lookup_ingest", ["12345"]),
        ("bronze_lookup_ingest", ["12345", ""]),
        ("bronze_lookup_ingest", ["12345", "   "]),
    ],
)
def test_an_ingest_without_a_month_is_refused_rather_than_defaulted(script, argv):
    """Neither ingest may answer a missing month with `opl.config`'s pinned one.

    This is not a new rule, it is the one `add_audit_columns` already made: its
    `snapshot_month` was deliberately given no default because the pinned month is
    how F1.2 tied every row to 2026-06 silently. Both these tasks then read that
    exact value into a local and handed it to that parameter -- satisfying the guard
    with the one value it exists to refuse, which made the no-default DECORATIVE.
    That is worse than no guard at all: the next reader sees it and believes the
    hole is closed.

    THIS LOCK IS NOT VACUOUS, which is the whole reason it is worth having. Under
    the old fallback every argv here SUCCEEDED -- it is a valid table and a valid
    batch id, so `main` proceeded to build a stream against the 2026-06 landing dir
    and stamp `_snapshot_month = "2026-06"`. Nothing distinguished that from a
    correct run in the log or in the data, which is why the failure had to be moved
    to before the session rather than reported after it.

    Refused BEFORE Spark, like the table and the batch id: `main` here never
    reaches `SparkSession.builder`, which is what makes this runnable with no JVM."""
    module = _load(script)
    with pytest.raises(ValueError, match="no month was given") as excinfo:
        module.main(argv)
    # The operator has to learn WHICH parameter is missing and where it comes from.
    assert "{{job.parameters.month}}" in str(excinfo.value)


@pytest.mark.parametrize("script", ["bronze_ingest", "bronze_lookup_ingest"])
def test_the_month_goes_through_the_shared_guard(script):
    """Lock the property, not a spelling -- the companion to the batch-id lock below.

    Worth its own check because the defect it prevents is a REGRESSION to something
    that reads as harmless: `else DEFAULT.month` looks like a sensible default in
    review, and the argv-level lock above would go red without saying why. This one
    names the guard, so the next author reaches for it instead of the config."""
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    code = "\n".join(
        line for line in source.splitlines() if not line.strip().startswith("#")
    )
    assert "require_month(" in code, (
        f"{script}.py no longer routes its month through require_month"
    )
    assert "DEFAULT.month" not in code, (
        f"{script}.py substitutes the config's pinned month for a missing one. That "
        "is the value add_audit_columns' snapshot_month has no default in order to "
        "refuse, and it is invisible: it equals the job YAMLs' own default, so the "
        "omission shows nothing until the first run for another month"
    )


@pytest.mark.parametrize("script", ["bronze_ingest", "bronze_lookup_ingest"])
def test_the_batch_id_goes_through_the_shared_guard(script):
    """Lock the property, not a spelling.

    Two weaker locks were tried and rejected: grepping for the old literal
    `"manual"` also matches the comment recording why it was removed, and
    grepping for an `else '...'` fallback also matches this module's own correct
    `args[0] if args else ""`, which passes the empty string INTO the guard on
    purpose. What actually must hold is that the id is validated before use.
    """
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    assert "require_batch_id(" in source, (
        f"{script}.py no longer routes its batch id through require_batch_id -- "
        "an unvalidated id lands rows that are never gated, promoted or reported, "
        "because both the gate and the promote are scoped to the run id"
    )
