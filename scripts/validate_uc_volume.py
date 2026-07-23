"""Proves the two-layer topology: the extraction layer (here, local) can land
files into a Unity Catalog Volume via the control plane (allowed), even though
serverless compute cannot reach the public internet.

Catalog/schema note: the design brief assumed `main.default`, which is the
layout on some Databricks Free Edition workspaces. This workspace instead
ships only `workspace`, `system`, and `samples` catalogs (confirmed via
`databricks catalogs list --profile opl-free`); `workspace.default` is the
auto-created writable schema, so the probe targets
`/Volumes/workspace/default/landing` instead of `/Volumes/main/default/landing`.
"""
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from opl.config import DEFAULT

CATALOG = DEFAULT.catalog
SCHEMA = DEFAULT.schema
VOLUME_PATH = DEFAULT.volume_root  # created below if missing


def main() -> int:
    w = WorkspaceClient(profile="opl-free")
    # Ensure a volume exists (schema 'default' ships with the 'workspace' catalog on Free).
    try:
        w.volumes.create(catalog_name=CATALOG, schema_name=SCHEMA,
                         name="landing", volume_type=VolumeType.MANAGED)
    except Exception as e:  # already exists / permission — report, don't crash
        print(f"volume create note: {e}")
    target = f"{VOLUME_PATH}/f0_probe.txt"
    w.files.upload(target, b"f0-egress-probe", overwrite=True)
    resp = w.files.download(target)
    ok = resp.contents.read() == b"f0-egress-probe"
    print(f"UC Volume upload+download roundtrip: {'OK' if ok else 'FAILED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
