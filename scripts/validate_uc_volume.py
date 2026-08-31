"""Proves the two-layer topology: the extraction layer (here, local) can land
files into a Unity Catalog Volume via the control plane, byte-identical on the
way back.

WHAT THIS PROBE DOES AND DOES NOT ESTABLISH. It establishes the control-plane
landing path, which is what the topology rests on and what this file has always
verified. It says NOTHING about serverless egress -- and the sentence that used
to close this paragraph, "even though serverless compute cannot reach the public
internet", was measured FALSE on 2026-08-14: a serverless task resolved a public
host and received HTTP 200 from it, pulled 192,973 bytes from a SECOND, UNRELATED
host, and an API fetch has since run as a production job task. Two hosts and not
one: one host is consistent with a single allowlisted domain, and it is the second
that rules that out. Corrected here in 2026-08-17's pass rather than only in
ADR 0002, because a retraction that reaches the ADR and not the code is this
repository's own documented failure, committed twice in F-API.

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
