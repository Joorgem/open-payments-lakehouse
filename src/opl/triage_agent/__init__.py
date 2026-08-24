# src/opl/triage_agent/__init__.py
"""DQ-incident triage: read what the platform recorded, and REMEDIATE NOTHING.

WHAT THIS PACKAGE IS FOR. When a DQ gate stops a job, the facts needed to triage it are
already in the workspace and are spread across four places: which task run failed
(`system.lakeflow`, folded by `opl.dataops.telemetry`), which rows it rejected and why
(the table's quarantine), whether the batch reconciles at all
(`opl.bronze.reconcile`), and what the same table's recent runs looked like. Nothing
assembles them. A triager assembles them by hand, every time, from a run URL.

WHAT IT IS NOT, AND THE LINE IS ABSOLUTE: it never promotes, never re-runs, never
deletes and never writes to a table this project owns. `repromote_triaged_batch` exists,
takes a batch id, and is launched BY A HUMAN -- `dataops_reconciliation` already prints
that command beside a stranding and deliberately stops there, because "a view that
promoted rows would be a gate bypass wearing a dashboard". The same rule holds here one
layer up: this package reads, ranks and DRAFTS. A person decides.

`incidents` is the feed and it is the only module of this package that decides what an
incident IS. Everything downstream -- severity, sampling, blast radius, the issue
payload -- takes a record from here and adds a column to it, so that the question "which
runs are we triaging" has exactly one answer in this project.
"""
