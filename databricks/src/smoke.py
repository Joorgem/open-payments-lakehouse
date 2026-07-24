# databricks/src/smoke.py
"""Deployment spike: proves a serverless Asset Bundle job can import the opl
wheel and read central config. Intentionally trivial (near-zero quota)."""
from opl.config import DEFAULT


def main() -> None:
    print(
        "opl_smoke OK | "
        f"catalog={DEFAULT.catalog} schema={DEFAULT.schema} "
        f"volume_root={DEFAULT.volume_root} month={DEFAULT.month}"
    )


if __name__ == "__main__":
    main()
