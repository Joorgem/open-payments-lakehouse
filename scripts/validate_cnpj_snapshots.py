"""F0 risk probe: confirms (a) the Receita Federal CNPJ open-data host lists at
least 2-3 consecutive monthly snapshots (needed for real SCD2 historization)
and (b) the BCB PTAX OData API answers. Downloads NOTHING large -- HEAD/range
requests only.

CNPJ host note: the design brief assumed a plain Apache directory tree at
`arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/<YYYY-MM>/`.
That path 404s. As of 2026, Receita Federal serves dados_abertos_cnpj through
a Nextcloud instance ("SERPRO+", same host) reached via a public share rather
than a filesystem path. Confirmed live 2026-07-23 via WebDAV PROPFIND: monthly
folders back to 2023-05 exist, including every month through the current one
(2026-07), each holding the usual Empresas*/Estabelecimentos*/Socios*.zip
parts. The share's WebDAV root requires HTTP Basic auth with the share token
as username and an empty password -- this is Nextcloud's standard "public
link" auth convention, not a real credential.
"""
import sys
from datetime import date

import requests

from opl.extraction.cnpj_source import SHARE_TOKEN

CNPJ_WEBDAV_BASE = "https://arquivos.receitafederal.gov.br/public.php/webdav"
CNPJ_AUTH = (SHARE_TOKEN, "")
CNPJ_PROBE_FILE = "Empresas0.zip"  # first part of a large table; HEAD only, never downloaded

PTAX = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@di,dataFinalCotacao=@df)"
    "?@di='01-02-2026'&@df='01-31-2026'&$top=1&$format=json"
)

HEADERS = {"User-Agent": "open-payments-lakehouse-f0-probe/1.0"}


def check_ptax() -> bool:
    r = requests.get(PTAX, headers=HEADERS, timeout=30)
    ok = r.status_code == 200 and "value" in r.json()
    print(f"BCB PTAX reachable: {'OK' if ok else 'FAILED'} ({r.status_code})")
    return ok


def _recent_months(n: int = 4) -> list[str]:
    """Return the n most recent YYYY-MM strings, ending at the current month."""
    today = date.today()
    months = []
    y, m = today.year, today.month
    for _ in range(n):
        months.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return list(reversed(months))


def check_snapshots() -> bool:
    # RF (via its Nextcloud "SERPRO+" public share) publishes monthly folders
    # like 2026-06/, 2026-07/ ... probe the last few months relative to today.
    months = _recent_months(4)
    found = []
    for m in months:
        url = f"{CNPJ_WEBDAV_BASE}/{m}/{CNPJ_PROBE_FILE}"
        try:
            r = requests.head(
                url, auth=CNPJ_AUTH, headers=HEADERS, timeout=30, allow_redirects=True
            )
            if r.status_code < 400:
                found.append(m)
        except requests.RequestException as e:
            print(f"  {m}: error {e}")
    print(f"CNPJ monthly snapshots reachable: {found} ({len(found)}/{len(months)})")
    return len(found) >= 2


def main() -> int:
    ptax = check_ptax()
    snaps = check_snapshots()
    return 0 if (ptax and snaps) else 1


if __name__ == "__main__":
    sys.exit(main())
