# Specs

Design documents written before implementation, one per phase. Each states the
problem, the options considered, the decision and its consequences, and is the
document the phase's plan and its ADRs are derived from.

These are committed deliberately: the ADRs in `docs/adr/` record decisions once
taken, and these record the design work that produced them. Together with the
run-evidence documents in `docs/`, they are the trail from intent to executed
result.

Execution plans, session ledgers and handoffs are NOT here — they live in the
git-ignored `.plans/` directory, because they carry per-session detail (task
ordering, review rounds, agent bookkeeping) that is noise to anyone outside the
session that produced it.
