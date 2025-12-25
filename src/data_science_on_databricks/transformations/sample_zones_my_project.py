import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dp.table
def sample_zones_my_project() -> DataFrame:
    # Read from the "sample_trips" table, then sum all the fares
    return (
        spark.read.table("sample_trips_my_project")
        .groupBy(F.col("pickup_zip"))
        .agg(F.sum("fare_amount").alias("total_fare"))
    )
