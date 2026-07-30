# src/opl/extraction/giants.py
"""Giants extraction: download multi-part RFB zips (resumable, mid-stream
retry via WebDavClient) and land the ZIPs in a UC Volume subdir. ZIPs — not
CSVs — because part 0's ZIP is 2,128,818,559 B against 6,780,467,695 B unzipped:
under a third of the bytes over the wire, and the unzip then runs on the cluster
where the bytes already are (unzip_volume module). This used to be forced by a
5 GiB single-PUT ceiling — a property of the old databricks-sdk 0.40 pin, not of
the Files API — which no longer exists now that ADR 0007 has adopted the
multipart upload path. The choice survives the reason that created it."""
from __future__ import annotations

from pathlib import Path

from databricks.sdk import WorkspaceClient
from opl.config import OplConfig
from opl.extraction.cnpj_source import expected_files
from opl.extraction.landing import upload_to_volume
from opl.extraction.webdav import WebDavClient


def part_files(group: str) -> list[str]:
    return expected_files([group])


def download_parts(client: WebDavClient, month: str, group: str, dest_dir: Path,
                   parts: list[int] | None = None) -> list[Path]:
    wanted = part_files(group)
    if parts is not None:
        wanted = [f"{group}{i}.zip" for i in parts]
    manifest = {e.name: e.size for e in client.list_dir(month) if not e.is_dir}
    missing = [f for f in wanted if f not in manifest]
    if missing:
        raise FileNotFoundError(f"{month}: not on server: {missing}")
    out: list[Path] = []
    for name in wanted:
        out.append(client.download(f"{month}/{name}", Path(dest_dir) / name,
                                   expected_size=manifest[name]))
    return out


def upload_zips(w: WorkspaceClient, local_paths: list[Path], cfg: OplConfig,
                table: str, month: str) -> list[str]:
    volume_dir = cfg.landing_zips(table, month)
    return [upload_to_volume(w, p, volume_dir) for p in local_paths]
