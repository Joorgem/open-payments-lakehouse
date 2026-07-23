# tests/test_delta_roundtrip.py
from opl.spark import local_session


def test_delta_write_read_roundtrip(tmp_path):
    spark = local_session("test-roundtrip")
    path = str(tmp_path / "t")
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    df.write.format("delta").mode("overwrite").save(path)
    out = spark.read.format("delta").load(path)
    assert sorted(r.id for r in out.collect()) == [1, 2]
    spark.stop()
