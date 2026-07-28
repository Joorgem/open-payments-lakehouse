# databricks/src/unzip_estabelecimentos.py
"""Job task: unzip landed Estabelecimentos part zips inside the Volume
(idempotent -- re-runs skip already-extracted parts)."""
import sys

from opl.bronze.unzip_volume import unzip_dir
from opl.config import DEFAULT


def main() -> None:
    month = sys.argv[1] if len(sys.argv) > 1 else DEFAULT.month
    out = unzip_dir(DEFAULT.landing_zips("estabelecimentos", month),
                    DEFAULT.landing_table("estabelecimentos", month))
    print(f"unzip: {len(out)} inner files present in landing/estabelecimentos")


if __name__ == "__main__":
    main()
