"""The composition root: one list, and it is the list the job task issues.

WHY THIS FILE IS WHERE THE COLLISION LOCK LIVES NOW. Free Edition ships one catalog and one
schema, so every object shares a namespace; the three collision guards range over the
bronze, vault and gold REGISTRIES, and a view is in none of them. F4 Task 1 answered that
with a `dataops_` prefix and a test over its own two views. That test could not stay where
it was once there were four: a lock over half a set is a lock that reports green about the
half nobody added to it."""
from __future__ import annotations

from opl.bronze.reconcile import create_view_ddl
from opl.bronze.registry import REGISTRY
from opl.config import OplConfig
from opl.dataops.telemetry import SYSTEM, task_telemetry_sql
from opl.dataops.views import DATAOPS_VIEWS, all_view_ddls

_CONFIG = OplConfig(catalog="spark_catalog", schema="opl_views_probe")


def _created_names(config: OplConfig) -> list[str]:
    """The view names read back OFF the DDLs the task issues, not off any list."""
    prefix = f"CREATE OR REPLACE VIEW {config.catalog}.{config.schema}."
    heads = (ddl.split("\n")[0] for ddl in all_view_ddls(config))
    return [head.removeprefix(prefix).removesuffix(" AS") for head in heads]


def test_the_name_list_is_exactly_what_the_task_creates():
    """`DATAOPS_VIEWS` is DERIVED FROM THE DDLs rather than compared to a second literal.

    An inventory that is not read off the thing it inventories is how a name ends up in no
    registry at all, which is the failure this whole prefix exists to prevent."""
    assert _created_names(_CONFIG) == list(DATAOPS_VIEWS)
    assert len(set(DATAOPS_VIEWS)) == len(DATAOPS_VIEWS)


def test_no_dataops_view_collides_with_a_name_any_registry_owns():
    """TOTAL over `DATAOPS_VIEWS`, which is the property the Task 1 version could not have.

    These four are in no registry by construction -- there is no `tables` and no `View`
    resource in a Databricks Asset Bundle either -- so this is the only place the question
    can be asked at all."""
    from opl.gold.registry import REGISTRY as GOLD
    from opl.vault.domains import REGISTRY as VAULT

    occupied = {name.casefold() for name in GOLD} | {name.casefold() for name in VAULT}
    for spec in REGISTRY.values():
        occupied |= {spec.staging.casefold(), spec.bronze.casefold(), spec.quarantine.casefold()}
    for view in DATAOPS_VIEWS:
        assert view.casefold() not in occupied, f"{view} is a name another layer owns"
        assert view.startswith("dataops_"), f"{view} is unprefixed, so nothing polices it"


def test_every_ddl_replaces_rather_than_skipping():
    """`CREATE OR REPLACE`, not `IF NOT EXISTS`, on all four.

    The opposite of the rule for a TABLE and for the opposite reason: a table must not lose
    rows, a view has none, and `IF NOT EXISTS` leaves an older wheel's definition standing
    while the run that was meant to replace it reports SUCCESS. That matters because
    `max_retries: 0` does not prevent a retry -- 24 measured (job run, task key) pairs in
    this workspace ran two attempts -- so every statement this task issues runs twice."""
    ddls = all_view_ddls(_CONFIG)
    assert len(ddls) == len(DATAOPS_VIEWS) == 4
    for ddl in ddls:
        assert ddl.startswith("CREATE OR REPLACE VIEW ")
        assert "IF NOT EXISTS" not in ddl
    assert create_view_ddl("x", "SELECT 1", _CONFIG).startswith(
        f"CREATE OR REPLACE VIEW {_CONFIG.catalog}.{_CONFIG.schema}.x AS"
    )


def test_the_telemetry_view_that_deploys_reads_the_platforms_own_tables():
    """`SystemTables` is a test seam and must not leak into what ships.

    `tests/dataops/test_telemetry.py` points the shipped SQL at local fixtures, which is
    the only way any of its aggregations can be asserted before a workspace run. This is
    the other half of that bargain: the DDL the job issues names `system.lakeflow` and
    `system.query` and nothing else."""
    ddl = all_view_ddls(_CONFIG)[DATAOPS_VIEWS.index("dataops_task_telemetry")]
    assert "system.lakeflow.job_task_run_timeline" in ddl
    assert "system.query.history" in ddl
    assert "system.lakeflow.jobs" in ddl
    assert task_telemetry_sql() == task_telemetry_sql(SYSTEM)
    assert "opl_views_probe" not in task_telemetry_sql()


def test_the_views_over_this_projects_own_tables_land_where_config_says():
    """The other three are total over registries and read `config.table(...)` throughout,
    which is the rule this repository states for every catalog/schema reference."""
    qualified = f"{_CONFIG.catalog}.{_CONFIG.schema}."
    for name, ddl in zip(DATAOPS_VIEWS, all_view_ddls(_CONFIG), strict=True):
        assert ddl.startswith(f"CREATE OR REPLACE VIEW {qualified}{name} AS")
        if name != "dataops_task_telemetry":
            assert f"FROM {qualified}" in ddl
