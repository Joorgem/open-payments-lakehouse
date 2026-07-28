# scripts/extract_cnpj.py
"""Extract a CNPJ month recorte from the RFB WebDAV share, unzip, and land into
the UC Volume. Extraction runs OFF Databricks (two-layer topology, ADR 0002)."""
from __future__ import annotations

import argparse
from pathlib import Path

from opl.contracts.cnpj_schemas import FILE_GROUPS
from opl.extraction.cnpj_source import (
    RECORTE_GROUPS,
    SHARE_TOKEN,
    WEBDAV_BASE,
    expected_files,
)
from opl.extraction.landing import (
    LANDING_VOLUME_DIR,
    unzip_single,
    upload_client,
    upload_to_volume,
)
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
    """
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
                target = upload_to_volume(w, inner, f"{LANDING_VOLUME_DIR}/{month}")
                print(f"  landed {fname} -> {target} ({inner.stat().st_size} B)")
            else:
                print(f"  downloaded+unzipped {fname} -> {inner} ({inner.stat().st_size} B)")
            landed += 1
        except Exception as exc:  # noqa: BLE001 - any land failure must fail the run
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

    return run(
        client,
        args.month,
        groups,
        args.dest,
        upload=not args.no_upload,
        require_complete=args.require_complete,
    )


if __name__ == "__main__":
    raise SystemExit(main())
