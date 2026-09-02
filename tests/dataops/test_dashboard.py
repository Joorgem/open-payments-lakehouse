"""The dashboard, checked as far as anything local can check it -- and no further.

WHAT `bundle validate` DOES NOT DO, MEASURED 2026-08-18 AND THE REASON THIS FILE EXISTS.
A `databricks/dashboards/probe.lvdash.json` containing `{ "this": "is not a dashboard" }`
was declared as a Dashboard resource and `databricks bundle validate -t free` returned
`Validation OK!`, having inlined the string verbatim into `serialized_dashboard`. The CLI
does not parse the dashboard JSON at all. So the bundle's own validation says nothing
about this artefact, and the checks below are the only ones there are before a deploy.

WHICH IS WHY THE CHECKS BELOW READ `queryLines`. A widget's `fields` list and its
`encodings.columns` list are both written by whoever edits the widget, so comparing them
to each other is a spell-check against a second copy of the same typo -- the first version
of this file did exactly that, and renaming `bronze_rows` to a column no dataset selects,
in BOTH lists, left it green. The dataset's own SQL is the only text here that anything
downstream actually executes, so it is the one a field name has to be checked against.

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
import re
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml
from job_yaml import resource_files

from opl.config import DEFAULT
from opl.dataops.views import DATAOPS_VIEWS

_BUNDLE = Path(__file__).resolve().parents[2] / "databricks"
_DASHBOARD_JSON = _BUNDLE / "dashboards" / "dataops.lvdash.json"
_DASHBOARD_YML = _BUNDLE / "resources" / "dataops_dashboard.yml"

# Keys `bundle generate dashboard` injects on a round-trip and an authored file never has.
# Their absence is what says this file is the source of truth rather than an export.
_ROUND_TRIP_MARKERS = ("pageType",)

# The `opl-free` warehouse's id. It is workspace-scoped platform state, and the claim this
# file locks is that it is committed as no RESOURCE VALUE anywhere in the bundle. It IS
# committed, once, inside the `databricks.yml` comment that argues against committing it --
# which is why the sweep below reads PARSED VALUES rather than raw text: prose that names
# the hazard is not the hazard, and a raw-text check would have to be excused for the one
# file where the string actually appears.
_WORKSPACE_SCOPED_WAREHOUSE_ID = "13cf10c85b0f189d"

# Every committed bundle file that could carry a resource value. `.databricks/` is the
# CLI's build output and is deliberately not swept -- it is generated, git-ignored, and
# holds the resolved id by design.
#
# `resource_files` RATHER THAN A GLOB OF THIS FILE'S OWN, because two sweeps that spelled the
# suffix set for themselves drifted -- one learned `.yaml` and the other did not, and a
# resource file the bundle deploys walked through the gap. This file reads the set from
# `tests/job_yaml.py`, where the CLI's own refusal is quoted beside it and where the grep that
# derives the sweeps still spelling a suffix for themselves is published.
_BUNDLE_FILES = (_BUNDLE / "databricks.yml", *resource_files())

# Everything that turns a NULL into a NUMBER, not just the one spelling the first version
# of this file banned. `sql_telemetry` and a NULL metric are the same statement said twice;
# any of these silently replaces the second half with "0 rows", which is the claim the
# whole telemetry view exists to keep separable. `CASE ... ELSE 0` is the one that got in.
_NULL_EATERS = (
    (r"\bCOALESCE\s*\(", "COALESCE"),
    (r"\bIFNULL\s*\(", "IFNULL"),
    (r"\bNVL2?\s*\(", "NVL"),
    (r"\bELSE\s+0(\.0*)?\b", "CASE ... ELSE 0"),
)

# `FROM`/`JOIN` and the name after it. The first version matched only words already
# starting with this project's catalog, so `system.query.history` -- the catalog this
# project's own telemetry view is built over, and the obvious thing to reach for -- and
# `samples.nyctaxi.trips` were both invisible to it. Measured: adding two such joins left
# it green.
_TABLE_REF = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_][\w.]*)", re.IGNORECASE)


@pytest.fixture(scope="module")
def dashboard() -> dict:
    return json.loads(_DASHBOARD_JSON.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def resource() -> dict:
    document = yaml.safe_load(_DASHBOARD_YML.read_text(encoding="utf-8"))
    return document["resources"]["dashboards"]["dataops"]


def _sql_of(dataset: dict) -> str:
    return "".join(dataset["queryLines"])


def _depths(text: str) -> Iterator[tuple[int, str, int]]:
    """Each character with the paren depth it sits at. Index, character, depth."""
    depth = 0
    for index, char in enumerate(text):
        if char == ")":
            depth -= 1
        yield index, char, depth
        if char == "(":
            depth += 1


def _select_list(sql: str) -> str:
    """The text between the leading SELECT and its own FROM, ignoring nested ones."""
    upper = sql.upper()
    assert upper.lstrip().startswith("SELECT"), f"dataset does not start with SELECT:\n{sql}"
    start = upper.index("SELECT") + len("SELECT")
    for index, _char, depth in _depths(upper):
        if depth == 0 and index >= start and upper.startswith("FROM", index):
            if upper[index - 1].isspace() and not upper[index + 4].isalnum():
                return sql[start:index]
    raise AssertionError(f"dataset has no top-level FROM:\n{sql}")


def _output_name(item: str) -> str:
    """One select-list item's OUTPUT column name: its alias, or its trailing identifier."""
    upper, alias_at = item.upper(), -1
    for index, _char, depth in _depths(upper):
        if depth == 0 and upper.startswith("AS", index) and index > 0:
            if upper[index - 1].isspace() and not upper[index + 2].isalnum():
                alias_at = index + 2
    tail = item[alias_at:] if alias_at >= 0 else item
    return tail.strip().strip("`").split(".")[-1]


def _output_columns(sql: str) -> set[str]:
    """Every column name a dataset's SQL actually returns, read off `queryLines`."""
    items, current = [], []
    for _index, char, depth in _depths(_select_list(sql)):
        if char == "," and depth == 0:
            items.append("".join(current))
            current = []
        else:
            current.append(char)
    items.append("".join(current))
    return {_output_name(item) for item in items if item.strip()}


def _scalars(node) -> Iterator:
    """Every leaf value of a parsed document. Comments are gone by construction."""
    if isinstance(node, dict):
        for value in node.values():
            yield from _scalars(value)
    elif isinstance(node, list):
        for value in node:
            yield from _scalars(value)
    else:
        yield node


def test_no_workspace_scoped_warehouse_id_is_committed_as_a_resource_value(resource):
    """`${var.wh}`, not the id `bundle generate dashboard` writes -- swept over the BUNDLE.

    A warehouse id is workspace state. Committed as a value, it resolves to nothing in
    anybody else's workspace -- and the failure is at deploy time, on the one artefact a
    reviewer is most likely to try redeploying, with no hint of why.

    The first version checked the two dashboard files and NOT `databricks.yml`, which is
    the one bundle file where the literal actually appears. It appears there in a comment
    arguing that it should not be a value, so the check that matters reads parsed values
    across every committed bundle file, and the id in the prose is left alone."""
    assert resource["warehouse_id"] == "${var.wh}"
    variables = yaml.safe_load((_BUNDLE / "databricks.yml").read_text(encoding="utf-8"))
    assert variables["variables"]["wh"]["lookup"]["warehouse"], (
        "the `wh` variable no longer looks the warehouse up by name, so either the id is "
        "hardcoded somewhere or the deploy has nothing to resolve"
    )
    documents = [(path, yaml.safe_load(path.read_text(encoding="utf-8"))) for path in _BUNDLE_FILES]
    documents.append((_DASHBOARD_JSON, json.loads(_DASHBOARD_JSON.read_text(encoding="utf-8"))))
    for path, document in documents:
        carriers = [
            value
            for value in _scalars(document)
            if isinstance(value, str) and _WORKSPACE_SCOPED_WAREHOUSE_ID in value
        ]
        assert not carriers, (
            f"{path.name} carries the workspace-scoped warehouse id as a VALUE ({carriers}), "
            "so this bundle is no longer reproducible into another workspace"
        )


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
    """A `fieldName` the query does not return is a blank column, silently.

    READ OFF `queryLines`, which is the correction: comparing `fields` to
    `encodings.columns` compares two hand-written lists to each other, so the same typo in
    both passes -- measured, renaming `bronze_rows` in both left this file green, which is
    exactly the blank column this docstring claims to prevent. `bundle validate` does not
    parse this JSON at all, so there is no second reader to catch it later."""
    selected = {d["name"]: _output_columns(_sql_of(d)) for d in dashboard["datasets"]}
    for page in dashboard["pages"]:
        for entry in page["layout"]:
            widget = entry["widget"]
            query = widget["queries"][0]["query"]
            fields = {f["name"] for f in query["fields"]}
            encoded = {c["fieldName"] for c in widget["spec"]["encodings"]["columns"]}
            assert encoded == fields, (
                f"{widget['name']}: columns {sorted(encoded ^ fields)} are encoded without "
                "being selected, or selected without being shown"
            )
            columns = selected[query["datasetName"]]
            assert fields <= columns, (
                f"{widget['name']}: {sorted(fields - columns)} are not returned by "
                f"{query['datasetName']}, which selects {sorted(columns)} -- each renders "
                "as a blank column with nothing failing"
            )


def test_the_only_tables_it_reads_are_views_this_project_creates(dashboard):
    """EVERY `FROM`/`JOIN` target, not only the ones already spelled with our catalog.

    A dashboard that read a bronze table directly would be a second definition of a
    metric that has one -- and it would not be covered by the collision lock, the
    reconciliation, or anything else that ranges over the views.

    The first version collected only words starting with `DEFAULT.catalog`, so a join to
    any OTHER catalog was invisible to it: measured, `LEFT JOIN system.query.history` and
    `LEFT JOIN samples.nyctaxi.trips` both left it green -- and `system` is precisely the
    catalog this project's own telemetry view is built over, so it is the one a next
    author reaches for."""
    known = {DEFAULT.table(view) for view in DATAOPS_VIEWS}
    for dataset in dashboard["datasets"]:
        sql = _sql_of(dataset)
        referenced = set(_TABLE_REF.findall(sql))
        assert referenced, f"{dataset['name']} reads nothing at all:\n{sql}"
        assert referenced <= known, (
            f"{dataset['name']} reads {sorted(referenced - known)}, which is no dataops view"
        )


def test_the_freshness_widget_shows_what_makes_a_paused_table_readable(dashboard):
    """THE ACCEPTANCE CASE, at the presentation layer.

    `lookup` is two snapshot months behind its siblings and is not a fault. A dashboard
    that showed the status without the note, or the age without the snapshot month, would
    put an operator back to guessing which of the two it is looking at."""
    freshness = next(d for d in dashboard["datasets"] if d["name"] == "ds_freshness")
    for column in (
        "source_freshness_status",
        "cadence_note",
        "cadence_kind",
        "last_snapshot_month",
        "source_age_days",
        "pipeline_age_days",
    ):
        assert column in _sql_of(freshness), f"{column} is not on the freshness dataset"


def test_the_telemetry_widgets_never_present_an_absent_metric_as_a_number(dashboard):
    """`sql_telemetry` travels with every telemetry dataset, and nothing fills its NULLs.

    Measured 2026-08-18 21:53Z, 129 of 274 task runs have no statement recorded at all and
    16 issued SQL that read exactly zero rows. A tile showing `read_rows` without the
    column that says which of those two a row is makes them the same claim.

    THE BAN IS ON THE MECHANISM, NOT ON ONE SPELLING. The first version banned the string
    `COALESCE` alone, and `SUM(CASE WHEN read_rows = 0 THEN 1 ELSE 0 END)` -- which routes
    a NULL `read_rows` through `ELSE 0` and renders the `no_sql_attributed` group as
    `runs_reading_zero_rows: 0` -- shipped straight past it."""
    for name in ("ds_sql_coverage", "ds_task_runs"):
        dataset = next(d for d in dashboard["datasets"] if d["name"] == name)
        sql = _sql_of(dataset)
        assert "sql_telemetry" in sql, f"{name} reports metrics with no telemetry state"
        for pattern, spelling in _NULL_EATERS:
            assert not re.search(pattern, sql, re.IGNORECASE), (
                f"{name} fills a NULL with {spelling}, which is how 'no statement was "
                "recorded' becomes 'this task read no rows'"
            )
