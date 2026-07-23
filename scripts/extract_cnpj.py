# scripts/extract_cnpj.py
"""Extract a CNPJ month recorte from the RFB WebDAV share, unzip, and land into
the UC Volume. Extraction runs OFF Databricks (two-layer topology, ADR 0002)."""
from __future__ import annotations

import argparse
from pathlib import Path

from databricks.sdk import WorkspaceClient
from opl.extraction.cnpj_source import (
    RECORTE_GROUPS,
    SHARE_TOKEN,
    WEBDAV_BASE,
    check_month_complete,
    expected_files,
)
from opl.extraction.landing import LANDING_VOLUME_DIR, unzip_single, upload_to_volume
from opl.extraction.webdav import WebDavClient


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True, help="YYYY-MM, e.g. 2026-06")
    ap.add_argument("--groups", default=None,
                   help="comma-separated group names; default = dev recorte")
    ap.add_argument("--dest", default="data/cnpj", help="local landing dir")
    ap.add_argument("--no-upload", action="store_true", help="skip UC Volume upload")
    args = ap.parse_args()

    groups = args.groups.split(",") if args.groups else RECORTE_GROUPS
    wd = WebDavClient(WEBDAV_BASE, SHARE_TOKEN)

    ok, missing = check_month_complete(wd, args.month, groups)
    print(f"completeness {args.month} for {groups}: ok={ok} missing={missing}")

    entries = {e.name: e for e in wd.list_dir(args.month)}
    w = None if args.no_upload else WorkspaceClient(profile="opl-free")
    landed = 0
    for fname in expected_files(groups):
        entry = entries.get(fname)
        if entry is None:
            print(f"  SKIP {fname} (not present)")
            continue
        local_dir = Path(args.dest) / args.month
        zp = wd.download(entry.rel_path, local_dir / fname, expected_size=entry.size)
        inner = unzip_single(zp, local_dir / "unz")
        if w is not None:
            target = upload_to_volume(w, inner, f"{LANDING_VOLUME_DIR}/{args.month}")
            print(f"  landed {fname} -> {target} ({inner.stat().st_size} B)")
        else:
            print(f"  downloaded+unzipped {fname} -> {inner} ({inner.stat().st_size} B)")
        landed += 1
    print(f"done: {landed} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
