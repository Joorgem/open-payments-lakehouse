# src/opl/extraction/cnpj_source.py
"""CNPJ-specific source knowledge: which files a month should contain, a
completeness check, and the bounded dev 'recorte'. Access is the RFB public
Nextcloud share (public share id — not a secret)."""
from __future__ import annotations

from opl.contracts.cnpj_schemas import FILE_GROUPS

SHARE_TOKEN = "YggdBLfdninEJX9"  # gitleaks:allow — public Nextcloud share id for RFB open data
WEBDAV_BASE = "https://arquivos.receitafederal.gov.br/public.php/webdav"

# Dev recorte: the six lookup tables + Simples — small, complete, no 10-part giants.
RECORTE_GROUPS: list[str] = [
    "Cnaes", "Motivos", "Municipios", "Naturezas", "Paises", "Qualificacoes", "Simples",
]


def expected_files(groups: list[str] | None = None) -> list[str]:
    names = list(FILE_GROUPS) if groups is None else groups
    out: list[str] = []
    for g in names:
        spec = FILE_GROUPS[g]
        if spec["parts"] == 1:
            out.append(f"{spec['prefix']}.zip")
        else:
            out.extend(f"{spec['prefix']}{i}.zip" for i in range(spec["parts"]))
    return out


def check_month_complete(client, month: str, groups: list[str] | None = None):
    """Return (is_complete, missing_filenames) for a month folder."""
    present = {e.name for e in client.list_dir(month) if not e.is_dir}
    expected = expected_files(groups)
    missing = [f for f in expected if f not in present]
    return (len(missing) == 0, missing)
