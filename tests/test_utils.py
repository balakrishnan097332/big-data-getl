"""Unit test for GETL utils function."""
import json
from unittest import mock
from unittest.mock import patch

import boto3
import botocore
import pytest
from botocore.exceptions import ClientError
from moto import mock_s3

from big_data_getl.utils import (copy_and_cleanup, copy_files, delete_files,
                                 json_to_spark_schema)
from pyspark.sql.types import StructType
from tests.data.utils.example_json_schema import create_json_schema

# HELPER FUNCTION


def create_s3_files(s3,
                    paths: list,
                    bucket_name: str = 'pytest',
                    stud: str = ''
                    ) -> None:
    """Create files in S3 bucket."""
    create_s3_bucket(s3, bucket_name)
    test_binary_data = b'Here we have some test data'
    for obj in paths:
        object = s3.Object(bucket_name, stud + obj)
        object.put(Body=test_binary_data)

# HELPER FUNCTION


def create_s3_bucket(s3, bucket_name: str = 'pytest') -> None:
    """Create files in S3 bucket."""
    s3.create_bucket(Bucket=bucket_name)


@mock.patch('big_data_getl.utils.StructType')
def test_json_to_spark_schema_passes_correct_parameters(m_struct):
    """json_to_spark_schema is called with right parameters and right order."""
    # Arrange
    json_to_spark_schema(create_json_schema())

    # Act & Assert
    m_struct.fromJson.assert_called_with(create_json_schema())


def test_json_to_spark_schema():
    """json_to_spark_schema should load the json schema as StructType."""
    # Act
    result_schema = json_to_spark_schema(create_json_schema())

    # Assert
    assert isinstance(result_schema, StructType)


@pytest.mark.parametrize('invalid_schema, missed_key', [
    ('missing_name', 'name'),
    ('missing_type', 'type'),
    ('missing_nullable', 'nullable'),
    ('missing_type_and_name', 'name'),
    ('missing_metadata', 'metadata')
])
def test_json_to_spark_schema_invalid(invalid_schema: str, missed_key: str):
    """json_to_spark_schema should raise KeyError for missing key."""
    # Arrange
    msg = 'All schema columns must have a name, type and nullable key'

    # Act & Assert
    with pytest.raises(KeyError) as excinfo:
        json_to_spark_schema(create_json_schema(invalid_schema))
    assert 'Missing key: \'{0}\'. Valid format: {1}'.format(
        missed_key, msg
    ) in str(excinfo)


@pytest.mark.parametrize('obj_list', [
    ['my/key/including/not_present.txt'],
    ['my/key/including/n_present.txt', 'my/key/including/not_present.txt']
])
def test_delete_files_filenotfound(s3_mock, obj_list: str):
    """delete_files returns FileNotFoundError for file is not present in S3."""
    # Arrange
    bucket_name = 'pytest'
    create_s3_bucket(s3_mock)

    # Act & Assert
    with pytest.raises(FileNotFoundError) as excinfo:
        delete_files(bucket_name, obj_list)
    for obj in obj_list:
        assert obj in str(excinfo)


@pytest.mark.parametrize('obj_list', [
    ['my/raw/including/not_present.txt'],
    ['my/key/including/n_present.txt', 'my/raw/including/not_present.txt']
])
def test_delete_files_rawlayer(s3_mock, obj_list: str):
    """delete_files returns PermissionError when deleting files from raw."""
    # Act & Assert
    with pytest.raises(PermissionError) as excinfo:
        delete_files('bucket_name', obj_list)
    assert 'Access Denied to remove files from raw layer' in str(excinfo)


@pytest.mark.parametrize('obj_list', [
    [],
    ['valid/path.json'],
    ['valids/'],
    ['valid/path.json', 'valid/path2.json'],
    ['valids3/', 'valids4/']
])
def test_delete_files_success(s3_mock, obj_list):
    """delete_files returns None after successful deletion.

    The deleted files should no longer exist in the bucket
    """
    # Arrange
    NoneType = type(None)
    create_s3_bucket(s3_mock)
    bucket = s3_mock.Bucket('pytest')
    create_s3_files(s3_mock, obj_list)

    # Act & Assert
    assert isinstance(delete_files(
        'pytest',
        obj_list),
        NoneType
    )

    for obj in obj_list:
        with pytest.raises(ClientError) as excinfo:
            bucket.Object(obj).get()
        assert 'NoSuchKey' in str(excinfo)


@mock.patch('big_data_getl.utils.boto3')
def test_delete_files_passes_correct_parameters(m_boto3):
    """delete_files is called with right parameters and in right order."""
    # Arrange
    m_s3 = m_boto3.resource
    m_bucket = m_s3.return_value.Bucket

    # Act
    delete_files('pytest', ['test'])

    # Assert
    m_s3.assert_called_with('s3')
    m_bucket.assert_called_with('pytest')
    m_bucket.return_value.delete_objects.assert_called_with(
        Delete={'Objects': [{'Key': 'test'}]}
    )


@mock.patch('big_data_getl.utils.boto3')
def test_copy_files_passes_correct_parameters(m_boto3):
    """copy_files is called with right parameters and in right order."""
    # Arrange
    m_s3 = m_boto3.resource

    # Act
    copy_files(
        'origin_bucket',
        'destination_bucket',
        'origin_stud',
        'destination_stud',
        ['obj_list']
    )
    copy_source = {
        'Bucket': 'origin_bucket',
        'Key': 's3a://' + 'origin_stud' + '/' + 'obj_list'
    }

    # Assert
    m_s3.assert_called_with('s3')
    m_s3.return_value.meta.client.copy.assert_called_with(
        copy_source,
        'destination_bucket',
        's3a://' + 'destination_stud' + '/' + 'obj_list'
    )


@pytest.mark.parametrize('obj_list', [
    [],
    ['valid/path1.json'],
    ['valids/'],
    ['valid/path2.json', 'valid/path3.json'],
    ['valids3/', 'valids4/']
])
def test_copy_files_successful(s3_mock, obj_list):
    """copy_files returns None after successful deletion.

    The copied files should exist in the target location
    """
    # Arrange
    NoneType = type(None)
    origin_bucket = 'hq-dl-landingzone-dev-amc'
    destination_bucket = 'hq-datalake'
    origin_stud = 'hq-dl-landingzone-dev-amc/landing-zone/amc-connections'
    destination_stud = 'hq-datalake/raw/amc/amc-connections'
    create_s3_files(s3_mock,
                    obj_list,
                    origin_bucket,
                    's3a://' + origin_stud + '/')
    create_s3_bucket(s3_mock, destination_bucket)

    # Act & Assert
    assert isinstance(
        copy_files(
            origin_bucket,
            destination_bucket,
            origin_stud,
            destination_stud,
            obj_list
        ),
        NoneType
    )

    for obj in obj_list:
        dest_file_paths = 's3a://' + destination_stud + '/' + obj
        return_value = (
            s3_mock
            .Bucket(destination_bucket)
            .Object(dest_file_paths)
            .get()
        )
        assert return_value['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.parametrize('obj_list', [
    ['my/key/including/not_present.txt'],
    ['my/key/including/n_present.txt', 'my/key/including/not_present.txt']
])
def test_copy_files_failure(s3_mock, obj_list):
    """copy_files returns None after successful deletion.

    The copied files should exist in the target location
    """
    # Arrange
    origin_bucket = 'hq-dl-landingzone-dev-amc'
    destination_bucket = 'hq-datalake'
    origin_stud = 'hq-dl-landingzone-dev-amc/landing-zone/amc-connections'
    destination_stud = 'hq-datalake/raw/amc/amc-connections'
    create_s3_bucket(s3_mock, origin_bucket)
    create_s3_bucket(s3_mock, destination_bucket)
    error_msg = (
        "An error occurred (404) when calling the "
        "HeadObject operation: Not Found"
    )

    # Act & Assert
    with pytest.raises(FileNotFoundError) as excinfo:
        copy_files(
            origin_bucket,
            destination_bucket,
            origin_stud,
            destination_stud,
            obj_list
        )
    assert error_msg in str(excinfo)


@mock.patch('big_data_getl.utils.delete_files')
@mock.patch('big_data_getl.utils.copy_files')
@mock.patch('big_data_getl.utils.boto3')
def test_copy_and_cleanup_passes_correct_parameters(m_boto3, m_copy, m_delete):
    """copy_files is called with right parameters and in right order."""
    # Arrange
    create_s3_bucket(m_boto3.resource('s3'), 'destination_bucket')
    create_s3_bucket(m_boto3.resource('s3'), 'origin_bucket')
    origin_paths = ['obj_list']
    obj_list = []
    for obj in origin_paths:
        obj_list.append('s3a://' + 'origin_stud' + '/' + obj)

    # Act
    copy_and_cleanup(
        'origin_bucket',
        'destination_bucket',
        'origin_stud',
        'destination_stud',
        ['obj_list']
    )

    # Assert
    m_copy.assert_called_once_with(
        'origin_bucket',
        'destination_bucket',
        'origin_stud',
        'destination_stud',
        ['obj_list']
    )

    m_delete.assert_called_with(
        'origin_bucket',
        obj_list
    )
