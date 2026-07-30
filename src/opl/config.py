"""Central lakehouse coordinates. Single source of truth for catalog/schema and
UC Volume paths (Databricks Free Edition ships only workspace.default — no main).
Frozen: config is data, never mutated in place."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OplConfig:
    catalog: str = "workspace"
    schema: str = "default"
    landing_volume: str = "landing"
    month: str = "2026-06"

    @property
    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.landing_volume}"

    @property
    def landing_cnpj_root(self) -> str:
        return f"{self.volume_root}/cnpj"

    def landing_cnpj_month(self, month: str | None = None) -> str:
        return f"{self.landing_cnpj_root}/{month or self.month}"

    def landing_zips(self, table: str, month: str | None = None) -> str:
        return f"{self.landing_cnpj_month(month)}/zips/{table}"

    def landing_table(self, table: str, month: str | None = None) -> str:
        return f"{self.landing_cnpj_month(month)}/{table}"

    def landing_tmp(self, table: str, month: str | None = None) -> str:
        """Where a writer may stage a half-written file before it is a landed file.

        DELIBERATELY OUTSIDE `landing_cnpj_root`, and therefore outside every dir an
        Auto Loader reads: every stream reads its own `landing_table(...)` subdir with
        NO glob, and cloudFiles walks a source dir RECURSIVELY (empirically -- an F1.3
        probe planted in the `zips/` subdir was ingested by a stream reading the month
        root). A temporary anywhere under a watched dir would depend on a glob to stay
        invisible; one under `volume_root` cannot be reached by any source path at all.
        It sits beside `_schemas/` and `_checkpoints/` (see `opl.bronze.autoloader`),
        the same convention for state that lives in the Volume but is not data.

        STILL THE SAME FILESYSTEM as `landing_table`, which is what makes
        `os.replace` from here into there work: one UC Volume is one FUSE mount, so
        every path under `volume_root` is renameable onto every other. A system temp
        dir (`/tmp`, `%TEMP%`) is a different device and `os.replace` across devices
        raises `EXDEV` -- so "somewhere else" is not enough, it has to be here.

        Mirrors the landing layout (`<month>/<table>`) so an operator listing this
        tree can map each staging dir 1:1 onto the landing dir it feeds."""
        return f"{self.volume_root}/_tmp/cnpj/{month or self.month}/{table}"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"


DEFAULT = OplConfig()
