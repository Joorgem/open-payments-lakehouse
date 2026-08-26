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

`blast_radius` answers what ELSE is downstream of the table that was gated -- WHICH tables,
never how much of them, because a proportion classifies socios near 100% in this package's
fixture and near 0% on the deploy with no test able to tell the two apart. It reads nothing
and emits no SQL: the bundle declares which bronze table each vault loader is handed and
`opl.gold.registry` declares which vault table each gold table is built from, so this is a
declaration plus a derivation over registries the wheel already carries, locked by tests
that sweep the job YAMLs and the gold entry points. TWO BRONZE TABLES REACH GOLD WITHOUT A
VAULT TABLE IN BETWEEN and one of them is `payments`, the workspace's largest incident, so
a manifest walked bronze -> vault -> gold answers "nothing downstream" for exactly the
incident that most needs the opposite -- and an import-time guard refuses a registered
bronze table whose downstream set is empty, rather than printing the most reassuring wrong
answer this package can give.

`issue` is the assembly point for all five, and it emits the issue as DATA rather than as a
string: a record with named fields, so that "what would we say about this incident" is a
value a test can diff between two incidents rather than a paragraph built at the moment
somebody presses send. It REFUSES more than it renders -- facts that are not all about one
incident, a census that does not sum to the number the grade was computed from, a job
name still wearing the bundle's development prefix, which carries an operator's username
into what is about to become a public artefact, and a `produced_by` shaped like a filesystem
path, which carries the same username by another route. THE LAST THREE ARE REFUSED AT BOTH
DOORS -- the assembler and the JSON reader -- because the publisher reads a FILE and the
sum check was, for one review pass, made only at the door the publisher does not use. What
is refused at the assembler only is the identity check: a file carries one record and has no
second fact to disagree with. AND THE FILE DOOR REFUSES FOUR THINGS THE ASSEMBLER DOES NOT,
because a CASE ladder over declared literals cannot produce them and a JSON file can: a
severity, an action or a verdict that is not a word this package declares, a rank that is
not the one `SEVERITIES` gives that severity, a hold note this repository never wrote, and a
string where the body prints a bare number. Its field names are the other modules' SQL
aliases and nothing but a test can hold those two spellings equal, so every field is read
through a reader that RAISES on a missing key: a rename upstream refuses to build the issue
instead of publishing a body with a blank where the grade goes.

`report` turns one such record into markdown, and it is written against one failure: a body
that reads like competent analysis for ANY incident is not triage, it is this phase's
species wearing markdown. So every sentence is a constant or a function of a field, and the
tests assert the DIFFERENCES between the workspace's largest incident and its smallest
rather than two golden files. It states the number of prior executions FOUND beside the
number asked for and says in the same breath that no comparison was made, because
"compared against the last 5, nothing anomalous" is false for ten of eleven incidents here;
it renders the blast radius as table NAMES with no magnitude anywhere in the section; it
gives the two evidence removals two sentences so the unexplained pair cannot borrow the
explained trio's account; and it separates what a statement measured from what this
repository declares, so a reader can tell which trust each line is asking for. That last
separation is now three-way and not two: the relations an issue read are DERIVED there
rather than taken from the payload, and the two things that stay the caller's unchecked word
-- which run produced this, and which telemetry view it read -- are labelled in the body as
the caller's word. A backtick in a reject reason is a row value turning `@handle` into a
notification on a public repository, so the values a body prints are fenced by CommonMark's
rule -- and `report.py`'s own header names the exceptions, which is why no sentence here
states that as a universal: it was written as one, and it was false in six places.

NOTHING IN THIS PACKAGE POSTS ANYTHING. The publisher is `scripts/open_triage_issue.py`,
which is outside the wheel -- `pyproject.toml` packages only `src/opl` -- so no task running
in the workspace can import it. That is a credential boundary and not a filing convention:
`gh` on the operator's box already carries `repo` scope, while a Databricks task calling the
GitHub API would need a PAT in a secret scope, which is a new credential, a new human gate,
and a token with repository write sitting beside 55.8M rows of personal data. It prints by
default, posts only under an explicit flag, and takes exactly one incident id.
"""
