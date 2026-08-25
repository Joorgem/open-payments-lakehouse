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

`evidence` is the next layer and it GRADES NOTHING: given one of those records it says
what is actually in the workspace for it -- the quarantine census by reject reason, a
bounded sample rendered as column STATES rather than values, and the reconciliation
verdict or the word for its absence. Its header carries the one rule this package cannot
leave to memory: which of its outputs may reach a public artefact and which may not.

`severity` is the first module that GRADES, and it emits TWO columns rather than one: how
bad the incident is, ranked, and what a person should do next. Fusing them is what turns
"2,000 rows" into "promote", and the workspace's largest incident is the one whose recorded
recommendation is do not promote -- a decision no column can derive, so it ships as a
declared hold carrying its citation, on `opl.dataops.cadence`'s pattern. The hold changes
the ACTION and never the severity, and a test requires the recommendation to flip when the
declaration is removed. It still runs nothing: the remedy it prints is
`dataops_reconciliation`'s own column, passed through.

`history` is the fourth of the four facts named above -- what the same table's recent runs
looked like -- and it is the one whose absence is hardest to see. It counts the prior gate
executions an incident can be compared against and how many of those also fired the gate,
and it COMPARES NOTHING: the word it publishes says whether a comparison is possible, never
what one found, because "compared against the last 5, nothing anomalous" is false for ten of
this workspace's eleven incidents and two of them have no prior execution at all. So the
number actually found is on every row, and the two ways of having less than N -- fewer, and
none -- are two words rather than one. It reads the same F4 view `incidents` reads, on a key
(`check_bad_rows`) chosen because it survives a gate task that was renamed mid-project and
whose old runs the telemetry still serves under a name nothing marks as superseded.
"""
