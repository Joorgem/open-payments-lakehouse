# src/opl/gold/__init__.py
"""The Kimball star this lakehouse derives from the Data Vault, and the FIRST gold
layer in this repository -- until this package existed,
`grep -ril "kimball|dim_|fact_" src/ databricks/ docs/adr/ tests/` returned nothing.

EMPTY ON PURPOSE, WHICH IS THE OPPOSITE OF `opl.vault.domains.__init__`. That module
runs `discover_domains` and `build_registry` at import because the vault stakes an
extensibility claim on wave 2 adding a domain with a diff of "+1 file, 0 modified", so
its package root has to do the finding. Gold stakes no such claim and has no domains:
its tables are conformed by definition -- one `dim_date`, one `dim_currency`, one
`dim_company` shared by every fact -- so a per-domain registry would be a mechanism for
a decomposition Kimball's own model refuses. `opl.gold.registry` holds the table list
inline, in `opl.bronze.registry`'s shape, and runs its guards in its own foot.

WHAT LIVES HERE, so the next task knows where to put a table rather than deciding again:

  - `opl.gold.columns`  -- the names every dimension carries and the two interval
    sentinels. Pure: imports nothing but `opl.vault.columns`, which itself imports
    nothing, so a spec can be declared and refused without pyspark.
  - `opl.gold.registry` -- the `Scd2Dimension` kind, the registered tables, and the
    guards. A NEW KIND (`dim_date` and `dim_channel` are not SCD2; `fact_payment` is
    not a dimension) lands there beside `Scd2Dimension` until that file approaches this
    project's 800-line cap, at which point the kinds move to `opl.gold.specs` exactly
    as `opl.vault.specs` was split out of `opl.vault.registry` -- that split's own
    docstring argues the shape and there is no reason to re-derive it here.
  - `opl.gold.dimensions` -- the loader.
"""
