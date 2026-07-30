# databricks/src/unzip_table.py
"""Job task: unzip a table's landed part zips inside the Volume (idempotent --
re-runs skip already-extracted parts).

argv: [table, month]"""
import sys

from opl.bronze.registry import LANDING_ZIPS, table_spec
from opl.bronze.unzip_volume import unzip_dir
from opl.config import DEFAULT


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    spec = table_spec(args[0] if args else "")
    month = args[1] if len(args) > 1 else DEFAULT.month
    if spec.landing != LANDING_ZIPS:
        raise ValueError(
            f"{spec.name} does not land as zips (landing={spec.landing!r}), so there "
            "is nothing in its zips subdir to unzip. Running this task for it would "
            "silently do nothing and let the ingest that follows read an empty dir."
        )
    # The three dirs are the whole safety argument of this task, so they are all
    # named here, by the config that owns the Volume layout: read the zips, land the
    # inner files where the ingest task's Auto Loader looks, and stage each
    # half-written file OUTSIDE that dir (and outside the month root the lookup
    # stream walks) -- see OplConfig.landing_tmp for why that location is both
    # invisible to every stream and on the same filesystem, which os.replace needs.
    out = unzip_dir(
        DEFAULT.landing_zips(spec.subdir, month),
        DEFAULT.landing_table(spec.subdir, month),
        tmp_dir=DEFAULT.landing_tmp(spec.subdir, month),
    )
    print(f"unzip: {len(out)} inner files present in landing/{spec.subdir}")


if __name__ == "__main__":
    main()
