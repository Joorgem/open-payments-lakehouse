# src/opl/dataops/views.py
"""The ONE list of every view this project creates, and the DDLs that create them.

WHY A COMPOSITION ROOT RATHER THAN A LIST PER MODULE. Free Edition ships one catalog and
one schema, so every object this project makes shares a namespace with every other. The
three collision guards range over the bronze, vault and gold REGISTRIES -- and a view is
in none of them, so it is policed by nothing at all. F4 Task 1 answered that with a
`dataops_` prefix and a test; that test can only be TOTAL if there is one place that knows
every view. Two lists in two modules is the shape where the second one is not checked.

IT IS ALSO WHAT THE JOB TASK LOOPS OVER, so "a view that exists" and "a view the guard
knows about" are the same set by construction rather than by anyone having kept them in
step. Adding a view here is the whole of adding a view: the task issues it, the collision
lock covers it, and no new job and no new guard-list entry is needed
(`databricks/resources/dataops_views_job.yml` already carries the revision guard).

ORDER IS THE ORDER THE TASK ISSUES THEM IN, and it is not load-bearing: none of these
views reads another, so each `CREATE OR REPLACE` stands alone and a failure on any one
leaves the rest as they were."""
from __future__ import annotations

from opl.bronze.reconcile import (
    BATCH_GRAIN_VIEW,
    FILE_GRAIN_VIEW,
    batch_grain_sql,
    create_view_ddl,
    file_grain_sql,
)
from opl.config import DEFAULT, OplConfig
from opl.dataops.freshness import FRESHNESS_VIEW, freshness_sql
from opl.dataops.telemetry import TASK_TELEMETRY_VIEW, task_telemetry_sql

# Every view name, in issue order. `tests/dataops/test_views.py` holds this equal to the
# names `all_view_ddls` actually emits, so the list cannot describe a set the task does
# not create -- an inventory that is not derived from the thing it inventories is exactly
# how a name ends up in no registry.
DATAOPS_VIEWS = (BATCH_GRAIN_VIEW, FILE_GRAIN_VIEW, TASK_TELEMETRY_VIEW, FRESHNESS_VIEW)


def all_view_ddls(config: OplConfig = DEFAULT) -> tuple[str, ...]:
    """Every view, as `CREATE OR REPLACE VIEW` statements, in issue order.

    `task_telemetry_sql` takes no `config` and that is deliberate rather than an
    inconsistency: its sources are `system.lakeflow` and `system.query`, which are fixed
    names in every Unity Catalog metastore. Handing it this project's catalog and schema
    would be a parameter it cannot use and could only be wrong about. Where the view LANDS
    is still `config`'s answer, which is why it is passed to `create_view_ddl`."""
    return (
        create_view_ddl(BATCH_GRAIN_VIEW, batch_grain_sql(config), config),
        create_view_ddl(FILE_GRAIN_VIEW, file_grain_sql(config), config),
        create_view_ddl(TASK_TELEMETRY_VIEW, task_telemetry_sql(), config),
        create_view_ddl(FRESHNESS_VIEW, freshness_sql(config), config),
    )
