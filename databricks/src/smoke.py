# databricks/src/smoke.py
"""Deployment spike: proves a serverless Asset Bundle job can import the opl
wheel and read central config. Intentionally trivial (near-zero quota).

THE ONE JOB THAT REPORTS THE DEPLOYED REVISION WITHOUT REFUSING ON IT, and that is
the reason it is also the one job with no `assert_deployed_revision` task (ADR 0009).
Its purpose is to answer "does the deployed wheel import and can it read config",
i.e. it is what you run WHEN YOU SUSPECT THE DEPLOYMENT -- so a guard here would take
the diagnostic away in the case it exists for. It writes nothing, so a wrong revision
costs a re-run and nothing else.

An empty revision is printed as-is rather than defaulted or hidden. That is what an
editable install and an unstamped wheel look like, and this job's job is to say what
is there. Every other job refuses on the same value."""
from opl.bronze.provenance import built_revision
from opl.config import DEFAULT


def main() -> None:
    print(
        "opl_smoke OK | "
        f"catalog={DEFAULT.catalog} schema={DEFAULT.schema} "
        f"volume_root={DEFAULT.volume_root} month={DEFAULT.month} "
        f"built_revision={built_revision()!r}"
    )


if __name__ == "__main__":
    main()
