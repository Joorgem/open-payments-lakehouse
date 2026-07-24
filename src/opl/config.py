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

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"


DEFAULT = OplConfig()
