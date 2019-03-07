"""ConfTest fixture for SparkSession and logger."""
import boto3
import pytest
from moto import mock_s3

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


@mock_s3
@pytest.fixture(scope="function")
def s3_mock():
    """Mock boto3 using moto library."""
    mock_s3().start()
    s3 = boto3.resource('s3')
    return s3
