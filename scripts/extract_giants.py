# scripts/extract_giants.py
"""Extract multi-part CNPJ 'giant' tables (Empresas / Estabelecimentos / Socios)
from the RFB WebDAV share: download the selected .zip parts (resumable, size-
verified) and — with --upload — land the ZIPs (not CSVs; Files API caps single
PUTs at 5 GiB) into the UC Volume zips subdir. Extraction runs OFF Databricks
(two-layer topology, ADR 0002). Unzip happens later, on Databricks."""
from __future__ import annotations

import argparse
from pathlib import Path

from databricks.sdk import WorkspaceClient
from opl.config import DEFAULT
from opl.contracts.cnpj_schemas import FILE_GROUPS
from opl.extraction.cnpj_source import SHARE_TOKEN, WEBDAV_BASE
from opl.extraction.giants import download_parts, upload_zips
from opl.extraction.webdav import WebDavClient

# The SDK defaults both windows to 300 s. A ~340 MB part (Estabelecimentos3.zip
# is 366,824,247 B) cannot complete a single PUT in that time on this uplink,
# which moves roughly 10 MB/min -- ~35 min for one part. The default killed an
# upload with `Timed out after 0:05:00`. 2 h leaves ample room for the largest
# part; these are ceilings, not waits, so a fast link is unaffected.
UPLOAD_RETRY_TIMEOUT_SECONDS = 2 * 60 * 60
UPLOAD_HTTP_TIMEOUT_SECONDS = 2 * 60 * 60


def _parse_parts(raw: str | None, group: str) -> list[int]:
    """Parse the raw --parts CLI value into a validated list of part indices.

    Returns every part of the group when raw is None. Raises ValueError with a
    friendly message on non-integer or out-of-range values.
    """
    total = FILE_GROUPS[group]["parts"]
    if raw is None:
        return list(range(total))
    out: list[int] = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            i = int(tok)
        except ValueError:
            raise ValueError(f"--parts must be comma-separated integers; got {tok!r}") from None
        if not 0 <= i < total:
            raise ValueError(f"part {i} out of range for {group} (0..{total - 1})")
        out.append(i)
    return out


def run(
    client,
    month: str,
    group: str,
    parts: list[int],
    dest: str,
    upload: bool = False,
) -> int:
    """Download the requested parts of `group` for `month` and optionally land
    the ZIPs in the UC Volume.

    Returns:
        0 if every requested part downloaded (and uploaded, if `upload`) with
          size integrity.
        1 if any part was missing on the server, failed its size check, or any
          download/upload step raised.
    """
    table = FILE_GROUPS[group]["table"]
    local_dir = Path(dest) / month / "giants"
    print(f"giants {month} {group} parts={parts} upload={upload}")

    w = (
        WorkspaceClient(
            profile="opl-free",
            retry_timeout_seconds=UPLOAD_RETRY_TIMEOUT_SECONDS,
            http_timeout_seconds=UPLOAD_HTTP_TIMEOUT_SECONDS,
        )
        if upload
        else None
    )
    downloaded = 0
    uploaded = 0
    had_error = False
    for i in parts:
        try:
            local_paths = download_parts(client, month, group, local_dir, parts=[i])
            zp = local_paths[0]
            downloaded += 1
            if w is not None:
                target = upload_zips(w, [zp], DEFAULT, table, month)[0]
                uploaded += 1
                print(f"  landed {zp.name} -> {target} ({zp.stat().st_size} B)")
            else:
                print(f"  downloaded {zp.name} -> {zp} ({zp.stat().st_size} B)")
        except Exception as exc:  # noqa: BLE001 - any part failure must fail the run
            print(f"  ERROR {group}{i}.zip: {exc}")
            had_error = True
    suffix = f", {uploaded} uploaded" if upload else ""
    print(f"done: {downloaded}/{len(parts)} downloaded{suffix}")
    return 1 if had_error else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True, help="YYYY-MM, e.g. 2026-06")
    ap.add_argument("--group", required=True,
                   help="multi-part group name, e.g. Estabelecimentos")
    ap.add_argument("--parts", default=None,
                   help="comma-separated part indices (e.g. 1,2); default = all parts")
    ap.add_argument("--dest", default="data/cnpj", help="local download dir")
    ap.add_argument("--upload", action="store_true",
                   help="also upload the ZIPs to the UC Volume zips subdir")
    args = ap.parse_args()

    if args.group not in FILE_GROUPS:
        valid = ", ".join(sorted(FILE_GROUPS))
        print(f"error: unknown group {args.group!r}. Valid groups are: {valid}")
        return 2
    if FILE_GROUPS[args.group]["parts"] == 1:
        print(f"error: {args.group} is a single-part group; use extract_cnpj.py")
        return 2

    try:
        parts = _parse_parts(args.parts, args.group)
    except ValueError as exc:
        print(f"error: {exc}")
        return 2

    client = WebDavClient(WEBDAV_BASE, SHARE_TOKEN)

    return run(client, args.month, args.group, parts, args.dest, upload=args.upload)


if __name__ == "__main__":
    raise SystemExit(main())
