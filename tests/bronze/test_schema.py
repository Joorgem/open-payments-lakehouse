from pyspark.sql.types import StringType, StructType

from opl.bronze.schema import struct_for


def test_lookup_schema_is_two_string_columns_in_order():
    st = struct_for("lookup")
    assert isinstance(st, StructType)
    assert [f.name for f in st.fields] == ["codigo", "descricao"]
    assert all(isinstance(f.dataType, StringType) for f in st.fields)


def test_estabelecimentos_schema_all_string_and_full_order():
    st = struct_for("estabelecimentos")
    assert len(st.fields) == 30
    assert st.fields[0].name == "cnpj_basico"
    assert all(isinstance(f.dataType, StringType) for f in st.fields)


def test_unknown_table_raises():
    import pytest
    with pytest.raises(KeyError):
        struct_for("nope")
