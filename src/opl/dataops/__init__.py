"""DataOps: what the platform records about this project's runs, and what it does not.

Three modules, and the seam between them is WHO KNOWS THE FACT.

  * `telemetry` -- facts only the PLATFORM has: which task ran, for how long, and what
    its statements read and wrote. Nothing here is instrumented; it is a view over
    `system.lakeflow` and `system.query`.
  * `cadence` -- facts only a HUMAN has: how often each bronze table is expected to be
    re-ingested, and which one is deliberately not. Data in the repository, because no
    schedule in this bundle can fire for a metric to derive a rhythm from: F8 declared
    cadences on 2026-09-01, and every one of them deploys `PAUSED` under the only target
    this repository deploys (ADR 0021). This line said "there is no schedule anywhere in
    this bundle" until the day after that stopped being true.
  * `freshness` -- the two measurements the cadence is compared against, per bronze
    table, kept apart because they answer different questions and one of them is
    undefined for two of the seven tables.

`views` is the composition root: it is the ONE list of every view this project
creates, which is what makes "no dataops view collides with a name another layer owns"
a question that can be asked at all (Free Edition ships one catalog and one schema, and
the three registry collision guards range over registries no view can be in)."""
