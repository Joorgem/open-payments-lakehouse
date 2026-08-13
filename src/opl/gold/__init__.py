# src/opl/gold/__init__.py
"""The Kimball star this lakehouse derives from the Data Vault, and the FIRST gold
layer in this repository.

WHAT "FIRST" IS WORTH, MEASURED, BECAUSE THE CLAIM THAT USED TO STAND HERE WAS FALSE
UNDER EITHER READING. It said `grep -ril "kimball|dim_|fact_" src/ databricks/ docs/adr/
tests/` returned nothing. As written that is a BASIC regular expression, where `|` is a
literal: the pattern matches nothing anywhere, so the sentence asserted nothing. Read as
intended -- `git grep -lEi "kimball|dim_|fact_" d9efae0 -- src databricks docs/adr tests`,
at the commit this branch was cut from -- it matches FIVE files. Four are the word
"arteFACT_" catching `fact_` as a substring (`tests/bronze/test_provenance.py`,
`tests/test_assert_deployed_revision_task.py`, `tests/test_revision_stamp.py`,
`tests/test_vault_job_wiring.py`). The fifth is real and is worth the correction:
`docs/adr/0011-...md:337` says "The dimensional layer must not build a `dim_socio`" -- an
instruction to a layer that did not exist, which is a better statement of where this
repository stood than a grep that returned nothing.

EMPTY ON PURPOSE, WHICH IS THE OPPOSITE OF `opl.vault.domains.__init__`. That module
runs `discover_domains` and `build_registry` at import because the vault stakes an
extensibility claim on wave 2 adding a domain with a diff of "+1 file, 0 modified", so
its package root has to do the finding. Gold stakes no such claim and has no domains:
its tables are conformed by definition -- one `dim_date`, one `dim_currency`, one
`dim_company` shared by every fact -- so a per-domain registry would be a mechanism for
a decomposition Kimball's own model refuses. `opl.gold.registry` holds the table list
inline, in `opl.bronze.registry`'s shape, and runs its guards in its own foot.

WHAT LIVES HERE, so the next task knows where to put a table rather than deciding again.
(THIS LIST NAMED THREE MODULES AND PREDICTED A FOURTH until F3 Tasks 2 and 3; the split it
predicted has happened and FOUR loaders now exist, so it is restated rather than annotated
-- a "where things live" list that is not where things live is worse than none.)

  - `opl.gold.columns`  -- the names every dimension carries and the two interval
    sentinels. Pure: imports nothing but `opl.vault.columns`, which itself imports
    nothing, so a spec can be declared and refused without pyspark.
  - `opl.gold.specs`    -- the KINDS a gold table may be, one dataclass each, with every
    guard answerable about ONE table in isolation. Split out of `opl.gold.registry` at
    the moment that file's own docstring said to, in `opl.vault.specs`' shape. A NEW KIND
    lands here; that module's docstring argues the seam.
  - `opl.gold.registry` -- the registered tables and the WHOLE-SET guards: a name two
    specs claim, a name another layer owns, a source that must resolve against the vault.
  - `opl.gold.members`  -- what each conformed dimension CONTAINS, as plain Python rows.
    Pure, for `columns`' reason and because a calendar's arithmetic asserted through a
    Spark session is arithmetic asserted at seconds per case.
  - `opl.gold.dimensions` -- the SCD2 loader, over one satellite's version chain.
  - `opl.gold.conformed`  -- the loader for a declared value domain and for the calendar.
  - `opl.gold.pit`        -- the point-in-time loader: one row per (hub key, as-of date),
    pointing at the satellite version in force. The one table here that is NOT a
    dimension and that nothing in the star joins to; its docstring says so at length.
  - `opl.gold.facts`      -- the fact loader: one row per payment EVENT, a degenerate
    `transaction_id`, and TWO role-playing foreign keys into ONE `dim_company`, each
    resolved as of that payment's own `event_time`. It is the only module here that reads
    BRONZE, and the only one that reads another gold table.

AND THE ARROWS NOW POINT SOMEWHERE, WHICH IS THE ONE THING THIS PACKAGE COULD NOT SAY
UNTIL TASK 4. Every dimension above existed to be reached by a fact that did not exist, so
"conformed" was a property nothing could check -- `opl.gold.registry` now states it as an
EQUALITY (the registry's conformed dimensions are exactly the set `fact_payment` reaches)
and refuses at import a dimension no fact joins to. Exactly one table in this package is
still outside the star, `pit_estabelecimento`, and it says so itself rather than being
found out.
"""
