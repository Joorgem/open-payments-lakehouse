# databricks/src/unzip_estabelecimentos.py
"""Job task: unzip landed Estabelecimentos part zips inside the Volume
(idempotent -- re-runs skip already-extracted parts)."""
import sys

from opl.bronze.unzip_volume import unzip_dir
from opl.config import DEFAULT


def main() -> None:
    month = sys.argv[1] if len(sys.argv) > 1 else DEFAULT.month
    # The three dirs are the whole safety argument of this task, so they are all
    # named here, by the config that owns the Volume layout: read the zips, land the
    # inner files where the ingest task's Auto Loader looks, and stage each
    # half-written file OUTSIDE that dir (and outside the month root the lookup
    # stream walks) -- see OplConfig.landing_tmp for why that location is both
    # invisible to every stream and on the same filesystem, which os.replace needs.
    out = unzip_dir(DEFAULT.landing_zips("estabelecimentos", month),
                    DEFAULT.landing_table("estabelecimentos", month),
                    tmp_dir=DEFAULT.landing_tmp("estabelecimentos", month))
    print(f"unzip: {len(out)} inner files present in landing/estabelecimentos")


if __name__ == "__main__":
    main()
