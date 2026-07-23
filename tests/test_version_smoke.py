from opl._version_check import assert_versions


def test_versions_match_dual_target():
    versions = assert_versions()
    assert versions["python"].startswith("3.12")
    assert versions["pyspark"] == "3.5.3"
    assert versions["delta"] == "3.3.1"
