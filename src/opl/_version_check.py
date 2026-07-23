"""Dual-target version guard. Local OSS versions must match the pinned pair
so that behavior tested locally reproduces on Databricks DBR 16.4 LTS."""
import platform
from importlib.metadata import version

_EXPECTED = {"pyspark": "3.5.9", "delta": "3.3.1"}


def assert_versions() -> dict[str, str]:
    actual = {
        "python": platform.python_version(),
        "pyspark": version("pyspark"),
        "delta": version("delta-spark"),
    }
    for pkg, expected in _EXPECTED.items():
        if actual[pkg] != expected:
            raise RuntimeError(
                f"{pkg} version drift: expected {expected}, got {actual[pkg]}. "
                "Local OSS versions must match the Databricks target pair."
            )
    if not actual["python"].startswith("3.12"):
        raise RuntimeError(f"Python must be 3.12.x (PySpark 3.5 limit); got {actual['python']}")
    return actual
