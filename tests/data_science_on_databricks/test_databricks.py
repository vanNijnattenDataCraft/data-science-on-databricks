import pytest
from pyspark.sql import DataFrame
from pyspark.sql import types as T


@pytest.mark.databricks
def test_create_table() -> None:
    from databricks.sdk.runtime import spark

    data = [
        (1,),
        (2,),
        (3,),
    ]
    schema = T.StructType(
        [
            T.StructField("index", T.StringType(), True),
        ]
    )
    df: DataFrame = spark.createDataFrame(data, schema)  # pyright: ignore[reportUnknownMemberType]
    assert df.count() == 3
