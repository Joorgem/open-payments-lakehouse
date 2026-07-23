# src/opl/extraction/landing.py
"""Unzip a CNPJ part (single inner K-file) and land raw files into a UC Volume
via the Databricks control plane (two-layer topology, ADR 0002)."""
from __future__ import annotations

import zipfile
from pathlib import Path

from databricks.sdk import WorkspaceClient

LANDING_VOLUME_DIR = "/Volumes/workspace/default/landing/cnpj"


def unzip_single(zip_path: Path, dest_dir: Path) -> Path:
    zip_path = Path(zip_path)
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as z:
        members = [m for m in z.namelist() if not m.endswith("/")]
        if len(members) != 1:
            raise ValueError(f"{zip_path.name}: expected 1 inner file, got {len(members)}")
        inner = members[0]
        z.extract(inner, dest_dir)
    return dest_dir / inner


def upload_to_volume(w: WorkspaceClient, local_path: Path, volume_dir: str) -> str:
    local_path = Path(local_path)
    target = f"{volume_dir.rstrip('/')}/{local_path.name}"
    with open(local_path, "rb") as f:
        w.files.upload(target, f, overwrite=True)
    return target
