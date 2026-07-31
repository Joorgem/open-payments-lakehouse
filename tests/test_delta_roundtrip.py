# tests/test_delta_roundtrip.py
def test_delta_write_read_roundtrip(spark, tmp_path):
    path = str(tmp_path / "t")
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    df.write.format("delta").mode("overwrite").save(path)
    out = spark.read.format("delta").load(path)
    assert sorted(r.id for r in out.collect()) == [1, 2]
