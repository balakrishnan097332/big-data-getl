"""ConfTest fixture for SparkSession and logger."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark_session():
    """Return a sparksession fixture."""
    spark = (
        SparkSession
        .builder
        .master("local[*]")
        .appName("pysparktest")
        .getOrCreate()
    )

    yield spark
