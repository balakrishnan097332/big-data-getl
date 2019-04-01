from pyspark.sql import SparkSession
"""ConfTest fixture for SparkSession and logger."""
from shutil import rmtree
from typing import List

import boto3

import pytest
from moto import mock_s3


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
    yield boto3.client('s3')


@pytest.fixture(scope="function")
def tmp_dir(pytestconfig):
    """Return a tmp dir folder to write to that is then cleaned up."""
    tmp_path = '{}/tests/data/tmp'.format(pytestconfig.rootdir)

    yield tmp_path

    rmtree(tmp_path)
