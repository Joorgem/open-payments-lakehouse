"""The dashboard, checked as far as anything local can check it -- and no further.

WHAT `bundle validate` DOES NOT DO, MEASURED 2026-08-18 AND THE REASON THIS FILE EXISTS.
A `databricks/dashboards/probe.lvdash.json` containing `{ "this": "is not a dashboard" }`
was declared as a Dashboard resource and `databricks bundle validate -t free` returned
`Validation OK!`, having inlined the string verbatim into `serialized_dashboard`. The CLI
does not parse the dashboard JSON at all. So the bundle's own validation says nothing
about this artefact, and the checks below are the only ones there are before a deploy.

WHAT THEY CANNOT CHECK, stated plainly rather than implied by their absence: whether
Lakeview renders this file. Widget spec versions and encoding shapes are the renderer's
contract, not this repository's, and nothing outside the workspace holds them. That is
settled by the first deploy and is listed as unexercised until then.

WHY THE VIEW NAMES ARE ASSERTED AGAINST `DEFAULT.table(...)`. This repository's rule is
that a catalog and schema are spelled once, in `opl.config`. A `.lvdash.json` cannot call
a function, so its `FROM` clauses are the one place a second spelling was unavoidable --
and an unavoidable second spelling is exactly what gets locked rather than tolerated."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from opl.config import DEFAULT
from opl.dataops.views import DATAOPS_VIEWS

_BUNDLE = Path(__file__).resolve().parents[2] / "databricks"
_DASHBOARD_JSON = _BUNDLE / "dashboards" / "dataops.lvdash.json"
_DASHBOARD_YML = _BUNDLE / "resources" / "dataops_dashboard.yml"

# Keys `bundle generate dashboard` injects on a round-trip and an authored file never has.
# Their absence is what says this file is the source of truth rather than an export.
_ROUND_TRIP_MARKERS = ("pageType",)


@pytest.fixture(scope="module")
def dashboard() -> dict:
    return json.loads(_DASHBOARD_JSON.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def resource() -> dict:
    document = yaml.safe_load(_DASHBOARD_YML.read_text(encoding="utf-8"))
    return document["resources"]["dashboards"]["dataops"]


def test_no_workspace_scoped_warehouse_id_is_committed(resource):
    """`${var.wh}`, not the id `bundle generate dashboard` writes.

    A warehouse id is workspace state. Committed, it resolves to nothing in anybody
    else's workspace -- and the failure is at deploy time, on the one artefact a reviewer
    is most likely to try redeploying, with no hint of why."""
    assert resource["warehouse_id"] == "${var.wh}"
    variables = yaml.safe_load((_BUNDLE / "databricks.yml").read_text(encoding="utf-8"))
    assert variables["variables"]["wh"]["lookup"]["warehouse"], (
        "the `wh` variable no longer looks the warehouse up by name, so either the id is "
        "hardcoded somewhere or the deploy has nothing to resolve"
    )
    assert "13cf10c85b0f189d" not in _DASHBOARD_YML.read_text(encoding="utf-8")
    assert "13cf10c85b0f189d" not in _DASHBOARD_JSON.read_text(encoding="utf-8")


def test_the_dashboard_does_not_embed_the_deployers_credentials(resource):
    """`embed_credentials` is left at its default, false, so viewers use their own reach.

    Correct here because a view over `system.*` confers exactly the slice it selects --
    measured, a service principal with no `system` privileges read such a view and was
    refused the source. Setting it true would hand every viewer the deployer's reach over
    the whole of `system`, which is a grant made in a dashboard file."""
    assert "embed_credentials" not in resource or resource["embed_credentials"] is False


def test_the_json_is_authored_and_not_a_ui_round_trip(dashboard):
    """`bundle generate dashboard --force` sorts keys, pretty-prints and injects
    `pageType`, so a UI-edited copy pulled back over this file produces a diff that is
    mostly formatting and hides whatever actually changed."""
    raw = _DASHBOARD_JSON.read_text(encoding="utf-8")
    for marker in _ROUND_TRIP_MARKERS:
        assert marker not in raw, (
            f"{marker} is a key only the CLI's round-trip adds, so this file came back "
            "from a UI edit and is no longer the source of truth"
        )
    assert list(dashboard) == ["datasets", "pages"]


def test_every_widget_reads_a_dataset_the_file_declares(dashboard):
    """A widget naming a dataset that is not there renders empty, not broken."""
    declared = {dataset["name"] for dataset in dashboard["datasets"]}
    used = set()
    for page in dashboard["pages"]:
        for entry in page["layout"]:
            for query in entry["widget"]["queries"]:
                used.add(query["query"]["datasetName"])
    assert used <= declared, f"widgets reference undeclared datasets: {sorted(used - declared)}"
    assert declared == used, f"datasets nothing displays: {sorted(declared - used)}"


def test_every_widget_field_is_a_column_its_dataset_selects(dashboard):
    """A `fieldName` the query does not return is a blank column, silently."""
    for page in dashboard["pages"]:
        for entry in page["layout"]:
            widget = entry["widget"]
            fields = {f["name"] for f in widget["queries"][0]["query"]["fields"]}
            encoded = {c["fieldName"] for c in widget["spec"]["encodings"]["columns"]}
            assert encoded == fields, (
                f"{widget['name']}: columns {sorted(encoded ^ fields)} are encoded without "
                "being selected, or selected without being shown"
            )


def test_the_only_tables_it_reads_are_views_this_project_creates(dashboard):
    """Qualified through `opl.config.DEFAULT`, and only names in `DATAOPS_VIEWS`.

    A dashboard that read a bronze table directly would be a second definition of a
    metric that has one -- and it would not be covered by the collision lock, the
    reconciliation, or anything else that ranges over the views."""
    known = {DEFAULT.table(view) for view in DATAOPS_VIEWS}
    for dataset in dashboard["datasets"]:
        sql = "".join(dataset["queryLines"])
        qualified = {word for word in sql.split() if word.startswith(f"{DEFAULT.catalog}.")}
        assert qualified, f"{dataset['name']} reads nothing this project owns:\n{sql}"
        assert qualified <= known, (
            f"{dataset['name']} reads {sorted(qualified - known)}, which is no dataops view"
        )


def test_the_freshness_widget_shows_what_makes_a_paused_table_readable(dashboard):
    """THE ACCEPTANCE CASE, at the presentation layer.

    `lookup` is two snapshot months behind its siblings and is not a fault. A dashboard
    that showed the status without the note, or the age without the snapshot month, would
    put an operator back to guessing which of the two it is looking at."""
    freshness = next(d for d in dashboard["datasets"] if d["name"] == "ds_freshness")
    sql = "".join(freshness["queryLines"])
    for column in (
        "source_freshness_status",
        "cadence_note",
        "cadence_kind",
        "last_snapshot_month",
        "source_age_days",
        "pipeline_age_days",
    ):
        assert column in sql, f"{column} is not on the freshness dataset"


def test_the_telemetry_widgets_never_present_an_absent_metric_as_a_number(dashboard):
    """`sql_telemetry` travels with every telemetry dataset.

    130 of 273 task runs have no statement recorded at all, and 15 issued SQL that read
    exactly zero rows. A tile showing `read_rows` without the column that says which of
    those two a row is makes them the same claim."""
    for name in ("ds_sql_coverage", "ds_task_runs"):
        dataset = next(d for d in dashboard["datasets"] if d["name"] == name)
        sql = "".join(dataset["queryLines"])
        assert "sql_telemetry" in sql, f"{name} reports metrics with no telemetry state"
        assert "COALESCE" not in sql.upper(), (
            f"{name} fills a NULL, which is how 'no statement was recorded' becomes 'this "
            "task read no rows'"
        )
