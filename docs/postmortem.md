# Postmortem — the finding nothing was obliged to read

Written 2026-08-31, at the close of F7.

**How to read the labels.** Facts carried here from one of this phase's measurement passes are
**Controller-verified** and say so. Facts whose only source is a git-ignored working document are
labelled ***Reported*** and are restated against something a public reader can check — a file, a
command, an issue number, a run id. Everything else was derived directly from this repository and
from the GitHub API on 2026-08-31, with the command printed beside it.
`docs/f6-run-evidence.md` §2 sets that convention for this repository and this document follows it.

**This is not an inventory of what was built.** The run-evidence documents under `docs/` and the
generated index at [`docs/adr/README.md`](adr/README.md) already do that, at a length nobody
should read twice. What does not exist anywhere else here is the reading you can only take by
putting the phases side by side — and that reading is about a single defect, which this project
found, published, and then merged past.

Where a count is already re-derived by a test, this document names the test rather than repeating
the number. **A number retyped into a second file is the mechanism the rest of this document is
about**, and a postmortem that reproduced it while describing it would be worth nothing.

---

## 1. The subject

### 1.1 The project filed the defect against itself, and then kept merging past it

On **2026-08-18**, two issues were opened on this repository, by this project, about this
project's own public documents:

- **[#25](https://github.com/Joorgem/open-payments-lakehouse/issues/25)** — *"README on main
  states Data Vault, Gold/Kimball and UC governance are 'roadmap, not built' — all three are on
  the same branch."*
- **[#26](https://github.com/Joorgem/open-payments-lakehouse/issues/26)** — *"Route doc counts
  event streams as covered, but F1b is Auto Loader file ingestion — no Kafka, no watermark, no
  exactly-once proof."*

Both were still open on 2026-08-31, when this was written. **The number of pull requests
merged into `main` since the filing is published here as a command and not as an integer,
because an integer here is false within a day — this one was.** The sentence that stood in
this place said *five*, and was overtaken **49 seconds after it was committed** by a merge
of this phase's own work. Re-derive it:

```bash
gh issue list --repo Joorgem/open-payments-lakehouse --state all \
  --json number,createdAt,closedAt,state,title
gh pr list --repo Joorgem/open-payments-lakehouse --state merged --limit 60 \
  --json number,mergedAt,title --jq '.[]|select(.mergedAt>"2026-08-18T21:45:30Z")'
```

At the time of writing that returned **#24** (F4 — DataOps), **#27** (F5 — streaming),
**#28** (F6 — the triage agent), then **#30**, **#31** and **#33** — the last three merged
**during F7 itself**, the phase whose entire subject is these two issues. Each made the README
more wrong than the one before it, and not one of them touched it.

**That the count kept moving while this paragraph was being written is not an embarrassment
to the paragraph; it is the paragraph's subject.** A finding stays open, work keeps landing
on top of it, and every document that states the distance in a number goes stale without
anyone editing it. The command does not.

The sentence the issue names was not a fresh mistake. It entered the file on **2026-07-23**, in
the project's first week, and was still the published statement when this document was written:

> The Data Vault / Kimball / Unity Catalog governance / AI-agent items above are **roadmap, not
> built** — they are planned, not present in this repo's code today.

```bash
git log -S'roadmap, not built' --oneline -- README.md
```

**It was true when it was written.** The Data Vault merged in PR **#9** on 2026-08-11 and the
gold layer in PR **#18** on 2026-08-13, so the sentence was already false a week before the issue
was filed — and the issue is the record of somebody noticing. What happened next is this
document's subject: **nothing consumed the record.**

### 1.2 This is a process defect, and the distinction is the whole point

It is tempting to file this as documentation rot, which is ordinary and boring. It is not that.
The method this project runs on is measure-then-record, and here **the method worked**. The defect
was detected. It was written down. It was written down *publicly*, in a tracker, under a title
that states the fault in one line and names the file. Every step of the discipline was executed
correctly.

What was missing is a **consumer**. No test read `README.md` at all until F7 (§4). No
phase-closing step read the issue list. The protocol's closing conditions ask a phase about its
own work, and #25 was nobody's own work by the time #24 merged. So the finding sat in the one
place everybody could see it and nobody was obliged to look.

> **A finding that is recorded and not consumed is indistinguishable from a finding nobody made.**

There is a second half, and it is why this cost more than a stale line would have. The paragraph
that #25 names is a **disclaimer**. It volunteers the project's own limits, and a reader trusts
a document that does that. **The damaging sentence was the honest-sounding one, and it was damaging
precisely because it was credible** — it denied four shipped subsystems while wearing the costume
of rigour.

---

## 2. Where this species was named, and what F7 adds to it

**F5** named it. Its closing ledger carries a bullet reporting two files within single digits of
this repository's 800-line cap — falsified by **the same commit that carried the bullet**, which
split both files. `docs/f5-run-evidence.md` records the diagnosis in its own words:

> **the defect had moved out of the code and into the document that judges it.**

**F6** tested that by dividing its closing review into a pass over the code and a pass over the
record. The divided pass returned **one blocking defect in the record and none in the code**
(`docs/f6-run-evidence.md`, *"What that adds up to"*). F6 also kept a running count of a narrower
thing: a correction whose defect landed in the *new* claim rather than in the bug it was fixing.
That count reached **five** inside one phase, and the document says so in two different sections:

```bash
grep -n 'in the NEW claim rather than' docs/f6-run-evidence.md
```

**ADR 0018** states the general form as a standing instruction:

> when a check reports the expected value, ask what else would produce that value. If the answer
> is "everything", it is not a check.

**F7's addition is the one that hurts.** F5 said the defect migrates into the judging document.
F6 measured that it does. F7 shows that **the migration can be detected, published under a URL,
and still not stop.** Detection was never the scarce resource. Consumption is.

---

## 3. Nine specimens, every one from the phase that set out to fix documents

F7 exists to repair documents. In doing so, in a single working session, it reproduced its own
subject at nearly every level of the stack: in an ADR amendment, in a triage rule, in a sweep, in
its own first deliverable, in its own prediction list, in its own CI, and in the analysis
commissioned to hunt the species. **That is not an embarrassment appended to the phase — it is the
phase's evidence**, and it is why the conclusion in §4 is as narrow as it is.

They fall into three families.

### 3.1 The correction pass, which is the most dangerous commit in this repository

**(a) A correction drew a boundary, and left the defect standing inside it.**

ADR 0002 is this project's most load-bearing architectural document: extraction runs off
Databricks, landing crosses the control plane, transformation runs on the platform. Its original
Context justified that with *"serverless compute blocks outbound internet to untrusted domains"*.
F-API then measured a serverless task resolving a public host, receiving HTTP 200, and pulling
192,973 bytes from an unrelated second host — and ran an API fetch as a production job task. The
premise was false.

The 2026-08-17 amendment corrected it, and **scoped itself**. The fix was declared to apply to the
Context; everything from *"Validation notes"* onward was ring-fenced as still standing, in the
words *"none of them is retracted here"*. Standing inside that fence was **"The blocked-egress
mitigation is validated"** — a sentence resting on the same F0 row the amendment's own evidence
falsifies, and which `docs/f0-validation-report.md` **strikes**. The claim was struck in one file
and live in another, on the project's central architectural premise, until F7.

Both amendments are in the file, one beneath the other, the second striking the first's
ring-fence: [`docs/adr/0002-two-layer-topology.md`](adr/0002-two-layer-topology.md), *Context* and
*Validation notes*.

> **A boundary drawn around a defect is how a correction pass leaves one behind.** The narrower
> the scope, the more confident the sentence declaring what is *not* being retracted — and that
> sentence is a claim like any other, written by someone who has just stopped checking.

**(b) One claim, wrong through three successive corrections.**

How long CI's `test` job takes has been corrected three times, and every version was written from
whichever runs its author happened to be holding.

F4 recorded that an inherited figure *"should now read ~20 min at ~2,683"*
(`docs/f4-run-evidence.md`). F6 corrected that to *"duration is the tell — healthy `test` on this
suite is ~1 h 32 m"*. F7 corrected F6 — and F7's own first attempt at the correction was itself
overtaken inside the same phase (*Reported*; that working record is git-ignored). What is
published now states no span at all, because there is not one:

> The `test` job's *Unit tests* step ran **31 m 23 s** on `33424009822` … and **4 h 53 m 08 s** on
> `33226013099` attempt 2 … nine times the wall clock for ~3,200 tests either way, so the clock
> says nothing about health. — `docs/f6-run-evidence.md` §3

The invariant that survives is the only thing worth carrying forward: **every failure carrying the
Spark-startup signature lands strictly inside the range of the successes**, so duration
discriminates nothing about health. That is a claim about a relationship rather than about a
value, and the next run either preserves it or falsifies it visibly — which is exactly what none
of the three numbers could do.

**(c) The phase's own first deliverable published the phase's own defect.**

F7's first task shipped [`docs/adr/README.md`](adr/README.md): a **generated**, test-locked index
of every ADR, with a table of reversal conditions and the readings taken against them. It is the
concrete answer to *no more hand-maintained lists*. Among its readings, dated and published, was
the claim that `rejected_by_our_gate` *"has never had a witness in any source"*.

Its source was a line in the phase plan labelled ***Reported***. Nobody checked it.
[ADR 0010's own measured table](adr/0010-observation-ledger-over-a-lossy-extract.md), under *"The
five states, measured against real bronze"*, gives `rejected_by_our_gate` as **4** rows
(estabelecimentos 2026-07), **1,792** and **1,781** (socios, link grain). That measurement has
been in this repository since 2026-08-06:

```bash
git log -S'1,792' --oneline -- docs/adr/0010-observation-ledger-over-a-lossy-extract.md
```

So a *Reported* claim was promoted to a dated reading, rendered into a generated page, locked by a
test, and merged to `main` in PR #31 — **inside the phase whose subject is findings that are not
consumed, in that phase's first deliverable.** The true statement is the narrow one F-DB had
already written: never witnessed *for merchant*. The reading now says that, and its state is still
`NOT MET`, because nobody has yet taken the measurement the condition actually asks for.

### 3.2 Checks that reported the expected value

**(d) The triage rule used for two phases separated nothing.**

CI here suffers a flake in which Spark's driver fails to start on the runner and several dozen
tests fail with `ConnectionRefusedError`. The failure looks catastrophic and the wrong reaction is
expensive, so the project published a rule for telling it from a real defect: *a mass
`ConnectionRefusedError` with no `assert` in the failure lines means no test decided anything.*

The absent `assert` half is worthless. Measured over every failed `test` job in this repository's
history:

```bash
gh run view <id> [--attempt <n>] --log-failed > r.log
grep -c ConnectionRefusedError r.log ; grep -cE 'E +assert' r.log
```

`grep -cE 'E +assert'` returns **zero every time**. The genuine failures are `Py4JJavaError` too —
an executor `OutOfMemoryError` on two runs, a log4j `StackOverflowError` on a third — and none of
them shows a failed assertion either. The half that discriminates is the **count**: 637–654
against 0. The population and the per-run figures are Controller-verified and published in
`docs/f6-run-evidence.md` §3.

**This is ADR 0018's species living inside the rule written to hunt it.** The check reported the
expected value for every case it had ever seen, and the answer to *what else would produce that
value* was "everything".

**(e) A sweep blinded by its own remedy.**

The sweep that finds those flakes starts from `gh run list --json conclusion`, filtered to
failures. `gh run list` reports a run's **latest attempt**. The documented response to this flake
is `gh run rerun --failed`. **So the procedure this project prescribes deletes its own evidence
from the instrument this project uses to count it.**

Run **`32281092103`** is the demonstration. Created 2026-08-19; `test` failed on attempt 1;
attempt 2 passed; the run's conclusion is `success`. The failure list has never returned it. It
was an executor `OutOfMemoryError` — a real failure, not a flake — and it stayed invisible to
every sweep of this repository's failures from 2026-08-19 until F7, on 2026-08-31, enumerated
attempts instead of runs:

```bash
gh api --paginate 'repos/Joorgem/open-payments-lakehouse/actions/runs?per_page=100' \
  --jq '.workflow_runs[]|select(.run_attempt>1)|[.id,.run_attempt]|@tsv'
gh api repos/Joorgem/open-payments-lakehouse/actions/runs/32281092103/attempts/1/jobs \
  --jq '.jobs[]|[.name,.conclusion]|@tsv'
```

**A check that cannot see the cases its own documented procedure creates is not a check of that
population.**

**(f) A prediction whose falsifier could not occur.**

This project publishes predictions before the runs that test them, each with an explicit
falsifier, because a number first written down after the run that produced it is not a prediction.
F7 predicted that Unity Catalog lineage would **not** cover all seven bronze tables' paths to
gold, since lineage records executions rather than structure, and named the falsifier as *"all
seven appearing"*. *(Provenance of the prediction: **Reported** — the phase plan is git-ignored.
Its substance is restated here, and its subject is checkable.)*

Three of the seven bronze tables **declare no gold path at all**, and the module the prediction is
about says so in its own source:

> THREE of the seven registered tables (lookup, merchant, socios) reach no gold table at all
> — `src/opl/triage_agent/blast_radius.py`

So "all seven appearing" could not happen, whatever lineage held. **The prediction's substance
survived** — on the four tables that do declare a gold path, lineage's coverage is genuinely
incomplete (Controller-verified 2026-08-31; the measurements are published with this phase's run
evidence) — but **its stated falsifier was unreachable, which makes that sentence a check that
could only pass.** It is recorded here rather than quietly reworded, because rewording it would be
§3.1(a) all over again.

**(g) A red run on `main` that was structurally invisible.**

On 2026-08-31 the merge of PR #30 to `main` (run `33415074589`, head `d091e37`) failed: its `test`
job went red at **18:51:12Z**. The merge of PR #31 (run `33424009822`, head `24fcefe`) had already
finished **green at 18:47:05Z** — four minutes earlier — from a commit that has `d091e37` as an
ancestor.

```bash
gh api repos/Joorgem/open-payments-lakehouse/actions/runs/33415074589/attempts/1/jobs \
  --jq '.jobs[]|select(.name=="test")|[.conclusion,.completed_at]|@tsv'
gh api repos/Joorgem/open-payments-lakehouse/actions/runs/33424009822 \
  --jq '[.conclusion,.updated_at]|@tsv'
git merge-base --is-ancestor d091e37 24fcefe && echo ancestor
```

**The newest run on `main` was already green while the red one beneath it was still running.**
Anybody glancing at the branch saw green. The red run was surfaced hours later by the review pass,
not by anyone watching CI. Nothing was actually broken — same Spark-startup flake, and the tree
had already passed under another sha — but **a red run on the default branch can be invisible to
the check everyone actually performs**, and that is a property of the display, not of the reader.

### 3.3 Records that nobody was obliged to read

**(h) Twenty-four findings closed by later work, and never struck.**

Protocol condition 6 obliges every phase to publish what it did **not** exercise. Nine documents
did it, honestly and in detail — nine files, ten sections, every entry naming what would exercise
it. **Nobody had ever read them as one list.**

F7 did, and the result is [`docs/unexercised-ledger.md`](unexercised-ledger.md). Its §4.1 is the
finding: entries whose claim a later phase had already made false, still standing unstruck in the
document that made them. **Twenty-four of them when that file was written, ten of those closed by
the same document that still carried them, and two closed *before they were written*.** Those two
numbers are re-derived from the rows by `tests/test_unexercised_ledger.py`; **if this paragraph
and that file ever disagree, the file is the one that is maintained.**

```bash
uv run pytest tests/test_unexercised_ledger.py -q
```

The sharpest single row is `f5:871`. F5's ledger listed the CI `redpanda` job as never having
executed, with *"What would exercise it: opening the PR."* The PR was opened. The job ran. It was
green in run **`32988424065`** — **the very run F6 quotes, in `docs/f6-run-evidence.md`, to
falsify its own claim that CI had not fired.** One phase published that run as proof CI works
while the previous phase's ledger, in another file, went on saying the job had never executed.

Two more entries were closed by ADR 0009's Status **eight and ten days before the entries claiming
them were written**. Nobody involved was careless: `docs/f3-workspace-run-evidence.md`'s version is
emphatic about not over-claiming, and it is wrong anyway.

**And the reason condition 6 could never have caught this is structural, not human:**

> Protocol §9 condition 6 only requires a phase to publish **its own** unexercised paths; that is
> why every row of §4.1 went unstruck, because no phase was ever obliged to look at anyone else's
> list. **The condition needs a second half: publish what you closed of someone else's.**
> — `docs/unexercised-ledger.md` §8

Nine documents discharged that condition correctly and the corpus decayed anyway. **Correct
execution of an incomplete obligation is indistinguishable from correct execution.**

**(i) The analysis commissioned to find unconsumed findings contained one.**

The read-only analysis that produced §4.1 swept every entry in the nine ledgers. Against one of
them — F-DB's *"THE THREE LIVE POSTGRES TESTS RUN ON ONE WINDOWS BOX AND NOWHERE ELSE"* — it wrote
a parenthetical: *"check against `ci.yml`'s `postgres` job, which now runs — see §9"*. Its §9 never
returns to it. **The check was noted and not taken, inside the document whose stated subject is
findings that are noted and not taken.**

The suspicion was correct. `.github/workflows/ci.yml` declares a `postgres` job on
`ubuntu-latest` running `uv run pytest -m postgres -v`, and F-DB's own §3 records that job as
green **later in the same section** as the entry claiming Windows-only.

```bash
grep -n 'runs-on\|pytest -m postgres' .github/workflows/ci.yml
```

**What makes this specimen worth publishing is that the next reader caught it.** It is now a
closed row in §4.1 with both anchors, and the row records where it was found. That is the whole
defence §4 has to offer against this family — and it is a person.

---

## 4. What actually held

Read §3 as a list of what failed to consume a finding, and the pattern is uncomfortable: **every
human-scale consumer in this project failed at least once.**

- The **controller** published a duration rule from the runs it happened to be holding and called
  it health.
- The **implementer** promoted a *Reported* line into a generated, dated, test-locked page without
  opening the ADR that already held the measurement.
- The **plan** carried a falsified figure into its own standing rules.
- The **review passes** caught the ADR 0002 ring-fence, the flake's true population and the
  invisible red run — and the read-only analysis commissioned to hunt unconsumed findings left
  its own parenthetical unfollowed (§3.3(i)).
- The **issue tracker**, the most visible surface any of this has, held the headline defect in the
  open while the work kept landing on top of it. **How many merges is the command in §1.1, not a
  number here** — the number that stood in this sentence went false without anyone editing it,
  which is §1.1's whole subject and was not supposed to survive into §4.
- The **closing protocol** was satisfied nine times, correctly, while the corpus it governs
  decayed.

Every one of those roles is competent, and the record still reads like that. What it shows is that
**no single role was reliable, and what caught each failure was a second reader with a different
job.** That mechanism has the cleanest record here — and it is expensive, slow, and available
roughly once per phase.

So F7's answer is not *read more carefully*. It is to give a finding a consumer that cannot be
tired, distracted, or between phases:

| the deliverable | the consumer that cannot skip it |
|---|---|
| `docs/adr/README.md` — generated, never hand-written | `tests/test_adr_index.py` |
| `README.md` — rebuilt from measurement, not edited | `tests/test_readme_counts.py` |
| `docs/unexercised-ledger.md` — nine ledgers read as one list | `tests/test_unexercised_ledger.py` |

```bash
uv run pytest tests/test_adr_index.py tests/test_readme_counts.py tests/test_unexercised_ledger.py -q
uv run python scripts/generate_adr_index.py --check
```

**Each of the three proves on every run that it is capable of failing.** They do not merely assert
the current state: they mutate the document in memory — rename a title, change a status, move an
anchor past the end of its file, delete a row, add an invented one — and assert that the
comparison names that change and no other. That property is not decoration. It is the difference
between a lock and a green light, and this project has ADR 0018 because it has shipped the second
one before.

---

## 5. And exactly how far a lock reaches, which is the honest half

**A lock defends what it can mechanically check and nothing else.** A postmortem ending *"and now
we have tests"* would be this project's own species one more time — a check reporting the expected
value, with "everything" as the answer to what else would produce it.

The proof is already in §3.1(c), and it is worth stating flatly: **`docs/adr/README.md` was
generated, locked and green — and published a false claim.** The lock was working perfectly. The
false claim lived in the half no parser reads. That page draws the distinction itself:

> Three of these facts are read out of the files and two are declared, and the difference matters
> when you are deciding what to trust.

Titles, statuses and the `## Decision` structure are **parsed**, so they cannot drift; nothing
stores them twice. The phase column and every reversal **reading** are **declared**, in
`scripts/adr_index.py`. `rejected_by_our_gate` was a declared reading. **The green was a true
statement about the parsed half, and said nothing whatever about the declared one.**

The consolidated ledger publishes the same split about itself, in its §0.3:

- **mechanical** — the id, the quoted claim (the test resolves every anchor against the source
  line it names), the published totals, and the two counts its opening paragraph states;
- **hand-assigned** — the bucket. Deciding that one guard fires on an edit while another fires on
  data is a judgement about what a guard *protects*. Nothing derives it and nothing checks it;
- **prose, asserted only by being non-empty** — the *what would exercise it* column;
- **not checkable offline at all** — run ids and statement ids. The test asserts their shape and
  says so rather than pretending.

And the residue no test can reach: **"is this still true today?"** No test can run a Databricks
job, so **closing an entry will always be a human act.** What the lock buys there is smaller and
still worth having: closing an entry is now a visible edit to one maintained file, rather than a
strike buried deep inside a phase document nobody will open again.

**A lock can carry the species too, and one did.** The first draft of
`tests/test_readme_counts.py` caught deletion, rewording, reordering and misformatting, and
**missed addition** — a tenth row carrying an invented count passed, in a table the README
describes as failing when it drifts. The repair is recorded in `_stated_table`'s own docstring:
every row must now be claimed by a needle, and the row count is asserted rather than the rows
iterated. **A lock is a check, so ADR 0018's instruction applies to it in full.**

**One thing F7 published as broken and did not fix**, which is the fairest test of everything
above. The GitHub repository description and topics are stale in exactly the way #25 describes:
the description still carries the row count F7 replaced in the README, and the topics stop at
medallion architecture, naming neither the Data Vault, nor the star, nor governance, nor DataOps
(Controller-verified 2026-08-31). They are account settings rather than tracked files, so no
commit reaches them and no test can.

```bash
gh repo view Joorgem/open-payments-lakehouse --json description,repositoryTopics
```

**If they are still stale the next time anyone opens this repository's front page, this document
has recorded a finding that nothing consumed, and a reader should hold that against it.** Stated
against a reader rather than against a phase, for §6's reason.

---

## 6. What would falsify this document

Stated in this project's own idiom, because a postmortem with no falsifier is a brochure with
footnotes.

**And three of the four below had to be restated, for the reason §3.2(f) publishes as a specimen
of this project's own species.** They were written against *the next phase*.
`docs/f7-run-evidence.md` opens by saying ~~**F7 is this project's last phase**~~ — a fact this
document did not mention, so the two disagreed about whether another one is coming — and against
that, *"the next phase merging past…"*, *"a phase closing without…"* and *"a hand-maintained list
surviving a phase…"* were ~~falsifiers that **cannot occur**~~ **taken to be falsifiers that could
never fire**. That is specimen (f) exactly, reproduced in the section whose whole subject is
falsifiability. **There may be no next phase. There will be a next merge to `main`, and a next
reader.** Each condition below is stated against one of those instead.

> **The struck fact went false on 2026-09-01, and not one of the conditions below moves.** The
> owner decided, after both documents were committed, to keep expanding this repository, and PR
> [#34](https://github.com/Joorgem/open-payments-lakehouse/pull/34) merged that day outside F7's
> plan — [`docs/f7-run-evidence.md`](f7-run-evidence.md)'s header carries the correction and the
> command that checks it. *"There will be a next merge to `main`, and a next reader"* was true
> when it was written and is true now: **a falsifier that fires on the next merge fires whether or
> not a phase is ever declared**, which is why restating them against a merge and a reader was the
> right repair even though the reason given for it has stopped being true. **This section had no
> example of that shape before** — not a claim going false, but a *justification* going false over
> a conclusion that held. Same species as §1.1's count and as F7's dated line, differing only in
> cause: neither a clock nor a merge moved under this one — **a decision was taken after it was
> written.**

1. **A locked document going false in its declared half again, with its test green.** That would
   mean the parsed/declared line is not where the defence belongs, and the readings need a
   consumer of their own — most plausibly a rule that a declared reading may cite a tracked file
   and never a plan line. This one needed no restating: it is about a document and a test, and
   both outlive the phase.
2. **The next merge to `main` landing while a self-filed issue about this repository's own public
   record is open, or on top of a red run on `main`.** The mechanism in §1 would then be intact
   and only the instance repaired. `gh issue list --state open` and the default branch's run list
   are the two commands that settle it — and **#25 and #26 were both open when this was written**,
   so this one is decided by the next merge rather than by some future phase.
3. **Work closing on this repository — a phase, a pull request, a session — without answering
   *which of someone else's ledger rows did I close?*** §3.3(h) is the argument that condition 6
   is incomplete. Work that closes cleanly without the second half and is later found to have
   closed somebody's row silently confirms it; work that adopts the second half and still leaves a
   silent closure refutes the proposed repair.
4. **A hand-maintained list in this repository still agreeing with the code and the runs the next
   time anyone re-derives it.** The claim underneath §4's table is that such a list rots. One that
   does not — measured, not asserted — weakens the case for what generating them costs.

---

## 7. What this document does not cover, and where it is instead

- **What was built.** The run-evidence documents under `docs/`, the ADRs and their generated
  index, and the README's *What is built* table, which is re-derived from the registries, the
  bundle YAMLs and a real test collection rather than typed.
- **What ships unexercised.** [`docs/unexercised-ledger.md`](unexercised-ledger.md), the
  carry-forward, and the only list of its kind here that is maintained rather than historical.
- **Why each decision was taken, and what would reverse it.**
  [`docs/adr/README.md`](adr/README.md) — including the reversal conditions nobody has read yet,
  marked `NOT READ` rather than `NOT MET`, because the two are different and the page refuses to
  collapse them.
- **This phase's own runs, predictions and outcomes.** `docs/f7-run-evidence.md`.

**One thing is deliberately left standing.** The ledger sections inside the phase documents are
*not* edited to agree with the consolidated one. They are point-in-time records of what a
phase believed on the day it closed, and this project's most valuable property is that it keeps
the wrong belief beside the correction, struck rather than deleted. **A repository that rewrites
its own history to look consistent has destroyed the only evidence that it was ever measuring
anything.**
