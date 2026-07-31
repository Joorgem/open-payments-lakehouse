# scripts/extract_cnpj.py
"""Extract a CNPJ month recorte from the RFB WebDAV share, unzip, and land into
the UC Volume. Extraction runs OFF Databricks (two-layer topology, ADR 0002)."""
from __future__ import annotations

import argparse
from pathlib import Path

from opl.bronze.registry import LANDING_LOCAL, spec_for_contract
from opl.config import DEFAULT
from opl.contracts.cnpj_schemas import FILE_GROUPS
from opl.extraction.cnpj_source import (
    RECORTE_GROUPS,
    SHARE_TOKEN,
    WEBDAV_BASE,
    expected_files,
)
from opl.extraction.landing import unzip_single, upload_client, upload_to_volume
from opl.extraction.webdav import WebDavClient


def _parse_groups(raw: str | None) -> list[str] | None:
    """Parse the raw --groups CLI value into a validated list of group names.

    Returns None when raw is None (caller should fall back to the default
    recorte). Raises ValueError with a friendly, actionable message if any
    parsed name is not a known FILE_GROUPS key.
    """
    if raw is None:
        return None
    names = [g.strip() for g in raw.split(",")]
    names = [g for g in names if g]
    invalid = [g for g in names if g not in FILE_GROUPS]
    if invalid:
        valid = ", ".join(sorted(FILE_GROUPS))
        raise ValueError(
            f"unknown group(s): {', '.join(invalid)}. Valid groups are: {valid}"
        )
    return names


def _landing_dir(group: str, month: str) -> str:
    """The Volume directory `group`'s inner file must be PUT into.

    THROUGH THE REGISTRY, because this function is the PRODUCER of the directory
    the lookup's Auto Loader reads. It did not exist until the F1.4a review: the
    target was `landing_cnpj_root/<month>`, the month ROOT, which is where the six
    lookup CSVs used to sit loose. Task 8 moved them into `lookups/` and deleted
    the `pathGlobFilter` that had kept the stream out of its neighbours' files --
    with a one-off migration script, leaving this producer pointed at the old
    place. The next `extract_cnpj.py --month 2026-07` would therefore have landed
    the six files where `bronze_lookup_stream` no longer looks, and
    `bronze_lookup_ingest` would have reported SUCCESS having ingested zero rows:
    an empty source dir is indistinguishable from nothing-new-to-read, which is
    the hazard this branch recorded and believed it had closed.

    `FILE_GROUPS[group]["table"]` is a CONTRACT key, so the resolution goes through
    `spec_for_contract` and not `table_spec` -- see that function for the F1.4b
    paste it refuses.

    NO FALLBACK for a spec whose `subdir` is not a usable directory name, and none
    is possible: `registry._assert_subdirs_are_single_path_components` refuses such
    a spec at IMPORT, `""` and `"."` by name, precisely because both resolve
    `landing_table(...)` back onto the month root. A branch here would be a second
    route to the state that guard exists to make unreachable.

    A group whose table does not land LOCAL is refused rather than landed: the
    landing mode is the registry's answer to HOW a table's bytes reach the Volume,
    and this script is the LOCAL producer (unzip on the extraction host, PUT the
    inner file). Symmetric to `extract_giants.py` refusing a single-part group, and
    to `unzip_table.py` / `bronze_ingest.py` refusing anything that is not zips."""
    spec = spec_for_contract(FILE_GROUPS[group]["table"])
    if spec.landing != LANDING_LOCAL:
        raise ValueError(
            f"{group} feeds {spec.name}, which lands as {spec.landing!r}, not "
            f"{LANDING_LOCAL!r} -- this script unzips locally and PUTs the inner "
            "file, which for a zips-landed table would send the multi-gigabyte "
            "extract over the wire instead of the third of the bytes its zip is, "
            "and would skip the in-Volume unzip the job flow expects. Use "
            "scripts/extract_giants.py for it, or --no-upload to download only."
        )
    return DEFAULT.landing_table(spec.subdir, month)


def _landing_dirs_by_file(groups: list[str], month: str) -> dict[str, str]:
    """Every expected filename mapped to the Volume dir its inner file lands in.

    Resolved for EVERY group up front, not per file inside the download loop: an
    unregistered group is a configuration answer that will be the same for all of
    its files, and inside the loop the `except Exception` would turn it into a
    per-file ERROR line only after the bytes were already on the wire -- over a
    link ADR 0003 measured at ~50% transient 500s, for payloads up to Simples'
    several-GB inner CSV. Bad input is refused before the work starts, the same
    ruling `require_month` holds for the job tasks."""
    out: dict[str, str] = {}
    for group in groups:
        volume_dir = _landing_dir(group, month)
        for fname in expected_files([group]):
            out[fname] = volume_dir
    return out


def run(
    client,
    month: str,
    groups: list[str],
    dest: str,
    upload: bool = True,
    require_complete: bool = False,
) -> int:
    """Download, unzip, and (optionally) land the given file groups for `month`.

    Fetches the WebDAV directory listing exactly ONCE and derives both the
    completeness check and the per-file download loop from that same
    listing (no duplicate network round-trip).

    Returns:
        0 if every expected-present file was landed without error.
        1 if any expected file was missing from the share (SKIP) or any
          download/unzip/upload step raised.
        2 if require_complete is set and the month is incomplete (no
          downloads are attempted in that case).

    Raises before the first network call if any group has no landing dir.
    """
    # BEFORE the listing, so a group with nowhere to land costs no request at all.
    # Only when uploading: the registry answers "where in the VOLUME does this
    # land", a question a --no-upload capture never asks -- which is what keeps the
    # full dev recorte of ADR 0003 (Simples included) downloadable while nothing may
    # be LANDED without a registered home.
    targets = _landing_dirs_by_file(groups, month) if upload else {}
    entries = {e.name: e for e in client.list_dir(month)}
    expected = expected_files(groups)
    missing = [f for f in expected if f not in entries]
    ok = len(missing) == 0
    print(f"completeness {month} for {groups}: ok={ok} missing={missing}")

    if require_complete and not ok:
        print(f"error: {month} is incomplete for {groups}; missing={missing}")
        return 2

    w = upload_client() if upload else None
    landed = 0
    had_error = False
    for fname in expected:
        entry = entries.get(fname)
        if entry is None:
            print(f"  SKIP {fname} (not present)")
            had_error = True
            continue
        local_dir = Path(dest) / month
        try:
            zp = client.download(entry.rel_path, local_dir / fname, expected_size=entry.size)
            inner = unzip_single(zp, local_dir / "unz")
            if w is not None:
                target = upload_to_volume(w, inner, targets[fname])
                print(f"  landed {fname} -> {target} ({inner.stat().st_size} B)")
            else:
                print(f"  downloaded+unzipped {fname} -> {inner} ({inner.stat().st_size} B)")
            landed += 1
        except Exception as exc:  # any land failure must fail the run
            print(f"  ERROR {fname}: {exc}")
            had_error = True
    print(f"done: {landed} files")
    return 1 if had_error else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True, help="YYYY-MM, e.g. 2026-06")
    ap.add_argument("--groups", default=None,
                   help="comma-separated group names; default = dev recorte")
    ap.add_argument("--dest", default="data/cnpj", help="local landing dir")
    ap.add_argument("--no-upload", action="store_true", help="skip UC Volume upload")
    ap.add_argument("--require-complete", action="store_true",
                   help="exit 2 without downloading if the month is incomplete")
    args = ap.parse_args()

    try:
        groups = _parse_groups(args.groups)
    except ValueError as exc:
        print(f"error: {exc}")
        return 2
    if groups is None:
        groups = RECORTE_GROUPS

    client = WebDavClient(WEBDAV_BASE, SHARE_TOKEN)

    try:
        return run(
            client,
            args.month,
            groups,
            args.dest,
            upload=not args.no_upload,
            require_complete=args.require_complete,
        )
    except ValueError as exc:
        # The only ValueError `run` lets out is the landing-target refusal, raised
        # before any byte moves (every per-file failure is caught in its loop and
        # counted). Reported as rc=2 like the unknown-group refusal above, because
        # it is the same kind of event -- a usage error -- and a traceback is not
        # what this repo hands an operator. `UnknownTable` is a ValueError by
        # design, for exactly this: the message is prose meant to be read.
        print(f"error: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
