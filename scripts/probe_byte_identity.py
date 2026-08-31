# scripts/probe_byte_identity.py
"""Emit every declared stream and compare it against the published baseline.

    uv run python scripts/probe_byte_identity.py

WHY THIS EXISTS AS A COMMITTED ARTEFACT. F3 published three byte counts and "matching
sha256, `cmp` clean" at its close, measured in a throwaway worktree, and the numbers had
no artefact in this repository -- so the next person told to "re-run that probe" had to
reconstruct it from a sentence. That is the provenance defect this phase struck in its own
plan (`docs/f-api-run-evidence.md` §0.1) and repaired for the PTAX rates with
`scripts/probe_ptax.py`. This is the same repair for the byte-identity claim.

IT NEEDS NO SPARK, NO CNPJ POOL AND NO DATABRICKS, which is the whole point: every landed
byte is a function of the declaration plus the pool, and the pool below is SYNTHETIC. That
is sound because a `cnpj_basico` is exactly eight characters, so which companies are in the
pool decides the digest but not the byte count -- and the digest is still a complete
fingerprint of the derivation as long as the pool is the same one every run uses, which is
why it is declared here rather than passed in.

WHAT IT REPLACES. The baseline was established by emitting from two git worktrees and
running `cmp` between them; with the numbers recorded below, one run answers the same
question. If you do compare two trees, CREATE THE SECOND WORKTREE OUTSIDE THE REPOSITORY
ROOT: `tests/test_revision_stamp.py::test_the_watched_paths_cover_everything...` rglobs for
`databricks.yml` excluding only `.venv`, so a worktree under the root turns it red locally
and is invisible to CI.

A FAILURE HERE IS NOT AUTOMATICALLY A BUG. It means a landed file's bytes moved, which is
sometimes exactly what a change intends -- a new profile, a re-declared window. What it may
never be is a SURPRISE: the numbers below are what F1b and F3 published their row counts,
duplicate counts and resolution rates against, and `opl.bronze.generated_landing` refuses to
overwrite a landed file whose bytes differ, so a moved digest means the correct stream can
never be written into a Volume that already holds the old one.
"""
from __future__ import annotations

import hashlib
import sys

from opl.bronze.generated_landing import serialised_bytes
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import delivered_records
from opl.generator.profiles import POOL_SIZE, PROFILES

# The synthetic pool: `POOL_SIZE` eight-digit keys in canonical order. NOT the real
# `hub_empresa` draw -- that needs a 69M-row table and a session -- so the digests below
# are a derivation fingerprint and NOT the digests of the files in the Volume.
_POOL = validated_pool(tuple(f"{n:08d}" for n in range(1, POOL_SIZE + 1)))

# (rows, bytes, sha256) per profile, against `_POOL`. The first three byte counts are the
# ones F3 published at its close; `between-snapshots`' and `cross-currency`' were
# published by F-API Task 3 in `docs/f-api-run-evidence.md` §1.
_BASELINE: dict[str, tuple[int, int, str]] = {
    "clean": (
        10_000, 2_925_069,
        "fccd6c48088909cd2f7f13fe1500a948ec670c5c8bfa29b3ae9e268a57b3dbea",
    ),
    "promotable": (
        10_150, 2_969_937,
        "5603cdd48c612f229afc5ff01d77134b5d42cdb82c15b45fb77a82c5df4aa77d",
    ),
    "drifting": (
        10_000, 2_989_447,
        "54db876f678396631edf7c2287cbf83c3b59d52360730612f611f760cc921425",
    ),
    "between-snapshots": (
        10_000, 2_926_409,
        "3381ba267d857f3fbb7cc7b25ff0df1bb87b25f0a340719dfb44f1d8d8be9dac",
    ),
    "cross-currency": (
        10_000, 2_926_588,
        "a527b61c19ec7933dd15bc7896ca4e34efeaa1b90d2bab768c1f0f80336b37dc",
    ),
}


def measured(name: str) -> tuple[int, int, str]:
    """(rows, bytes, sha256) for the profile called `name`, from its declaration."""
    profile = PROFILES[name]
    records = delivered_records(profile.stream_spec(_POOL), profile.defects)
    payload = serialised_bytes(records)
    return len(records), len(payload), hashlib.sha256(payload).hexdigest()


def main() -> int:
    """Print one line per declared profile and return non-zero if any of them moved."""
    undeclared = sorted(set(_BASELINE) - set(PROFILES))
    unbaselined = sorted(set(PROFILES) - set(_BASELINE))
    moved = list(undeclared) + list(unbaselined)
    for name in undeclared:
        print(f"GONE      {name:18s} has a baseline here and is no longer declared")
    for name in unbaselined:
        print(f"NEW       {name:18s} is declared and has no baseline here")
    for name in sorted(set(PROFILES) & set(_BASELINE)):
        rows, size, digest = measured(name)
        expected = _BASELINE[name]
        verdict = "IDENTICAL" if (rows, size, digest) == expected else "MOVED"
        if verdict == "MOVED":
            moved.append(name)
        print(
            f"{verdict:9s} {name:18s} {PROFILES[name].stream_id:22s} "
            f"rows={rows:6d} bytes={size:9d} sha256={digest}"
        )
        if verdict == "MOVED":
            print(f"          {'':18s} baseline rows={expected[0]} bytes={expected[1]} "
                  f"sha256={expected[2]}")
    print(f"\n{len(moved)} of {len(PROFILES)} profile(s) differ from the baseline")
    return 1 if moved else 0


if __name__ == "__main__":
    sys.exit(main())
