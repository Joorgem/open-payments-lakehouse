# src/opl/extraction/giants.py
"""Giants extraction: download multi-part RFB zips (resumable, mid-stream
retry via WebDavClient) and land the ZIPs in a UC Volume subdir. ZIPs — not
CSVs — because the Files API caps single-PUT uploads at 5 GiB and part 0's
unzipped CSV exceeds it; unzip happens on Databricks (unzip_volume module)."""
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
