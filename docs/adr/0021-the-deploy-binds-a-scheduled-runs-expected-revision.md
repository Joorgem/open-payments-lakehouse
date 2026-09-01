# ADR 0021 — the deploy binds a scheduled run's expected revision, and CI makes the deploy observable

## Status

**Accepted, and deliberately NOT YET IN FORCE.** The decision below is taken; the detector it
depends on — a CI job that deploys on merge and fails when the workspace's deployed revision is
not `main`'s head — does not exist in this repository yet, and cannot until a Databricks token
exists as a GitHub secret. So the schedules this ADR permits are declared under a target whose
mode pauses them, and every guarded job goes on refusing an unparameterised run. **That is not a
gap this ADR leaves open by accident; it is [ADR 0009](0009-deployed-revision-provenance.md)'s own
`the sentinel refusing is the correct default in the meantime`, now with a stated condition for
when it stops being the answer.** Decision 3 says so in the place a reader will look.

## Context

[ADR 0009](0009-deployed-revision-provenance.md) closed with a question rather than an answer:

> **This guard is for operator-launched runs.** None of these jobs is scheduled today. A
> scheduled run has no local repository to bind an expected revision from, so adding a schedule
> means answering this question again rather than passing the sentinel — and the sentinel
> refusing is the correct default in the meantime.

Declaring a schedule is the act that reopens it, and this phase declares schedules. So the
question has to be answered here or the schedules are dishonest.

**What the guard does today, restated so the failure is concrete.** Every job in this bundle
except one carries `assert_deployed_revision` as its first task; derive which:

```bash
grep -L assert_deployed_revision databricks/resources/*.yml   # smoke_job.yml, and a dashboard
grep -L REQUIRED-PASS-A-REVISION databricks/resources/*.yml   # the same two files
```

Each guarded job declares `revision` with the default `REQUIRED-PASS-A-REVISION`, which is
`opl.bronze.provenance.SENTINEL_REVISION` — a value `is_object_name` rejects on purpose. An
operator supplies the real value at launch: `--params revision=$(git rev-parse HEAD)`. **A
schedule has no operator and no `--params`**, so each job parameter takes its declared default,
so `expected` is the sentinel, so the run reaches `_refuse_the_expected_revision` and dies in
seconds, before Spark, on every guarded job, every time.

**That is correct behaviour producing a useless outcome:** a schedule declared with no further
change buys a red run on a cadence and nothing else.

**And the workspace this project deploys to will not launch anything.** Measured by this phase,
against a guarded job, so that a refusal would cost nothing:

```bash
databricks api post /api/2.2/jobs/run-now --json '{"job_id":<a guarded job>}'
# Error: Triggering new runs for organization <redacted> is currently disabled temporarily.
```

Metadata reads and `databricks bundle deploy -t free` over resources that already exist both
work; launching does not. **Nothing below may therefore rest on having watched a run refuse or
pass**, and where a decision would need that, it is not taken.

## Decision 1 — the DEPLOY binds the expected revision, and CI is what makes a missing deploy loud

**Five answers were available. They are set out with their costs, because four of them are
defensible and the choice is between costs rather than between right and wrong.**

| # | answer | what it costs |
|---|---|---|
| 1 | Schedule only `opl_smoke`, the one unguarded job | proves nothing about the pipeline. `opl_smoke` is excluded from the guard precisely because it is the diagnostic you run when you suspect the deployment ([ADR 0009](0009-deployed-revision-provenance.md)); scheduling it makes the schedule a test of the schedule |
| 2 | Bind `expected` at **deploy** time — a bundle variable writing the parameter default | **[ADR 0009](0009-deployed-revision-provenance.md) already rejected this, with a table.** Both values then come from one `bundle deploy`, so *nobody deployed at all* gives `expected == actual == the old sha` and **passes** — which is the socios re-run that terminated SUCCESS having masked only bronze. Under a schedule it is strictly worse: it passes green, unattended, on a cadence |
| 3 | Exempt scheduled runs from the guard | inverts the control. The runs nobody is reading the log of become the only unguarded ones |
| 4 | Bind `expected` from the **remote** repository at run time — the guard fetches `main`'s head from GitHub | restores two genuinely independent sources and still catches *nobody deployed*. Costs a hard network dependency on GitHub inside every scheduled run, makes every branch deploy refuse under a schedule, and **cannot be verified here** (see Decision 4) |
| 5 | Make the **deploy** the binding act, and make **CI** the deployer — with a CI job that reads the deployed wheel's stamp and fails when it is not `main`'s head | keeps option 2's value and closes the hole that made it wrong. *Nobody deployed* stops being silent, because its detector runs on every merge whether or not anyone is watching. Costs a Databricks token in GitHub secrets, and moves a check that lived at run time into the pipeline |

**Option 5 is taken.** The reasoning is that [ADR 0009](0009-deployed-revision-provenance.md)'s
objection to deploy-time binding is an objection to a **silence**, not to the value: the defect
is that nothing goes red when a deploy does not happen. A better value does not repair that; a
**detector for the silence, placed where it cannot be skipped**, does. Two sources survive — the
workspace's deployed artefact and `main`'s head — they are simply compared by CI on merge instead
of by a task at run time.

**What option 5 does NOT claim.** It does not make a scheduled run's own log self-sufficient: a
scheduled run still cannot say, from inside itself, that it is executing `main`. It says that
*somebody would have gone red* if the workspace had drifted. That is weaker than the
operator-launched case and this ADR does not pretend otherwise.

**What reverses it:** a workspace that will launch a run, plus one launch watched refusing on a
revision fetched from the remote — at which point option 4 is verifiable, restores two
independent sources *inside the run*, and should be retaken in place of this decision.

## Decision 2 — the schedules are declared now, and the TARGET'S MODE is what pauses them

The source YAML writes **no `pause_status` anywhere**, and must not. The CLI writes it, and it
writes the opposite value under each deployment mode. Measured on an isolated scratch bundle —
two targets, one job, one `schedule:` block, no `pause_status` in the source:

| what `databricks bundle validate -o json` renders | `mode: development` | `mode: production` |
|---|---|---|
| `schedule.pause_status` | **`PAUSED`** | **`UNPAUSED`** |
| `name` | `[dev <user>] ` prefix | no prefix |
| `presets` | 5 keys set | absent |

So under `free` — this repository's only deployed target, `mode: development` — **a declared
schedule is deployed PAUSED and cannot fire.** A second target is what would make one fire, and
`targets.prod` is now declared for exactly that reason. **It is deliberately never deployed**, on
[ADR 0018](0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md) Decision 6's second
and third grounds — both of which are about a production target, both *"assumed on strong
evidence and not proved"*, and the third of which revokes the platform's own `CREATE TABLE` on the
schema every pipeline writes into. That is not a thing to find out by trying it on the only
workspace there is.

**The safety property is mechanical rather than a promise:** those grounds can fire only if the
bundle declares a securable — `schemas`, `catalogs`, `volumes` or `external_locations`. It
declares none, and this phase adds none.

`mode: production` will not validate without a `workspace.root_path`; the CLI says so itself:

```
Error: target with 'mode: production' must set 'workspace.root_path' to make sure only one copy
is deployed
```

The path is written with `${workspace.current_user.userName}` rather than a literal, so no
operator identity is committed and the bundle stays deployable by whoever holds the profile.

## Decision 3 — until the detector exists, the sentinel goes on refusing, and that is the right default

Stated plainly, because it is the consequence a reader is most likely to be surprised by: **the
schedules this phase declares would, if unpaused today, produce a refusal at the first task of
every guarded job.** No CI deploy exists, so nothing binds a revision for an unattended run, so
the parameter default — a value the guard rejects — is what a scheduled run would carry.

That is [ADR 0009](0009-deployed-revision-provenance.md)'s own *"the sentinel refusing is the
correct default in the meantime"*, and it is preferable to all three of its alternatives: a
scheduled run that passes by comparing a value to itself (option 2), a scheduled run exempt from
the guard (option 3), or no cadence declared anywhere in the repository at all. **A schedule that
is paused and honest states the intended cadence; a schedule that fires green while proving
nothing states something false.**

The order this decision implies, and it is an order rather than a preference: the CI deploy comes
first, the production target is deployed second, and no schedule is unpaused before both.

## Decision 4 — option 4 is DESIGNED and NOT SHIPPED, and the ground is that it cannot be verified here

Option 4 — the guard fetching `main`'s head from the remote at run time — is the more elegant
answer, and this ADR says so rather than dressing the choice up as a preference. It restores two
independent sources *inside the run*, which is the property
[ADR 0009](0009-deployed-revision-provenance.md) argued for in the first place.

**It is rejected for this phase on one ground, and the ground is not a design objection.** It
cannot be verified here. Verifying it means watching a guard refuse, and this workspace refuses to
launch runs (see Context). Changing this project's most load-bearing safety mechanism and shipping
it without ever seeing it refuse would be
[ADR 0018](0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md)'s named species —
a check that reports the expected value because it has never run. Its two real costs stand and are
recorded rather than used as the reason: a hard network dependency on GitHub inside every
scheduled run, and every branch deploy refusing under a schedule.

So it is written down here as the reversal condition of Decision 1, and not shipped.

## Consequences

- **Every schedule this repository declares is inert until two things happen** — a CI deploy, and
  a production target that is actually deployed. Both are named above; neither is done here.
- **No scheduled run has ever fired in this workspace, and none can be made to fire from here.**
  Everything this ADR says about what a scheduled run would carry is derived from the parameter
  defaults and from the CLI's rendering, not from a run. It is recorded as unexercised.
- **`opl_smoke` stays unscheduled and unguarded.** Scheduling the one job whose purpose is to
  answer *"is the deployment what I think it is"* would make the diagnostic answer a question
  nobody asked, on a cadence.
- **The cadences are a claim about the SOURCES, not about the pipeline's readiness.** Each
  `schedule:` block carries its own justification in the YAML it lives in: the RFB CNPJ snapshots
  are monthly, PTAX is published on business days, and the vault and gold jobs follow the bronze
  they read. Jobs whose input is produced by a person or by another machine — the generated
  payments stream, the host-side merchant snapshot — carry no schedule, and the reason is recorded
  beside them rather than left as an absence.
- **A cron offset is not a dependency.** The monthly chain is declared as three clock tiers on one
  day, so a bronze job that fails does not stop the vault job that reads it from starting three
  hours later. Nothing here fixes that, and cross-job orchestration is out of this ADR's scope;
  the mitigation today is that every one of those runs refuses at the guard anyway.

## References

- [ADR 0009](0009-deployed-revision-provenance.md) — the guard, the two-sources argument, and the
  deferred question this ADR answers.
- [ADR 0018](0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md) Decision 6 — why
  a production target is declared and not deployed, and what a securable in the bundle would cost.
- `src/opl/bronze/provenance.py`, `databricks/src/assert_deployed_revision.py` — the comparison
  and the task that runs it first.
- `databricks/databricks.yml`, `databricks/resources/*.yml` — the two targets and the schedules.
