"""Unit test for GETL utils function."""
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from pyspark.sql.types import StructType

from big_data_getl.utils import (copy_and_cleanup, copy_files, delete_files,
                                 json_to_spark_schema)
from tests.data.utils.example_schema import create_json_schema


# HELPER FUNCTION
def create_s3_files(s3_mock, keys, bucket='tmp-bucket') -> None:
    """Create files in S3 bucket."""
    s3_mock.create_bucket(Bucket=bucket)
    for key in keys:
        s3_mock.put_object(Bucket=bucket, Key=key, Body=b'Here we have some test data')


@mock.patch('big_data_getl.utils.StructType')
def test_json_to_spark_schema_correct_params(m_struct):
    """json_to_spark_schema is called with right parameters and in the right order."""
    # Arrange & Act
    json_to_spark_schema(create_json_schema())

    # Assert
    m_struct.fromJson.assert_called_with(create_json_schema())


def test_json_to_spark_schema():
    """json_to_spark_schema should load the json schema as StructType."""
    # Arrange & Act
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
def test_json_to_spark_schema_invalid(invalid_schema, missed_key):
    """json_to_spark_schema should raise KeyError for missing key."""
    # Arrange & Act
    with pytest.raises(KeyError) as excinfo:
        json_to_spark_schema(create_json_schema(invalid_schema))

    # Assert
    assert 'Missing key: \'{0}\'. Valid format: {1}'.format(
        missed_key, 'All schema columns must have a name, type and nullable key'
    ) in str(excinfo)


@mock.patch('big_data_getl.utils._get_collections_of_files')
def test_delete_files_throws_expections(m_get_files, s3_mock):
    """delete_files returns FileNotFoundError files we cannot remove from S3."""
    # Arrange
    s3_mock.create_bucket(Bucket='bucket')
    m_get_files.return_value = [[{'Key': 'path/to/file/in/s3.txt'}]]
    error_msg = "Could not delete the following file(s) [{'Key': 'path/to/file/in/s3.txt'}]"

    # Act & Assert
    with pytest.raises(FileNotFoundError) as excinfo:
        delete_files(['bucket/to/file/in/s3.txt'])

    assert str(excinfo.value) == error_msg


@pytest.mark.parametrize('paths', [
    ['my/raw/including/not_present.txt'],
    ['my/key/including/n_present.txt', 'my/raw/including/not_present.txt']
])
def test_delete_files_not_possible_from_raw(paths):
    """delete_files returns PermissionError when deleting files from raw."""
    # Act & Assert
    with pytest.raises(PermissionError) as excinfo:
        delete_files(paths)

    assert 'Access Denied: Not possible to remove files from raw layer' in str(excinfo)


@pytest.mark.parametrize('paths,bucket,files', [
    ([], 'landingzone', []),
    (
        ['landingzone/amc-connect/file.json', 'landingzone/amc-connect/test/file.json'],
        'landingzone',
        ['amc-connect/file.json', 'amc-connect/test/file.json']
    ),
    (
        ['landingzone/amc-connect/'],
        'landingzone',
        ['amc-connect/file.json', 'amc-connect/file2.json', 'amc-connect/subfolder/file2.json']
    )
])
def test_delete_files_success(s3_mock, paths, bucket, files):
    """delete_files should remove files and files with keys and prefixes."""
    # Arrange
    create_s3_files(s3_mock, files, bucket=bucket)

    # Act & Assert
    assert delete_files(paths) is None

    for _file in files:
        with pytest.raises(ClientError) as excinfo:
            s3_mock.get_object(Bucket=bucket, Key=_file)

        assert 'NoSuchKey' in str(excinfo)


@mock.patch('big_data_getl.utils.boto3')
def test_copy_files_passes_correct_parameters(m_boto3):
    """copy_files is called with right parameters and in right order."""
    # Arrange
    m_s3 = m_boto3.client
    m_s3.return_value.get_paginator.return_value.paginate.return_value = [
        {'Contents': [{'Key': 'fake/key'}]}
    ]
    copy_source = {
        'Bucket': 'landingzone',
        'Key': 'fake/key'
    }

    # Act
    copy_files([('landingzone/amc-connect', 'datalake/amc/raw')])

    # Assert
    m_s3.assert_called_with('s3')
    m_s3.return_value.copy.assert_called_with(
        copy_source,
        'datalake',
        'amc/raw'
    )


@pytest.mark.parametrize('transactions,source_bucket,target_bucket,files', [
    (
        [],
        'tmp-bucket',
        'tmp-bucket',
        {'create_files': [], 'check_files': []}
    ),
    (
        [('landingzone/amc-connect/file.json', 'datalake/amc/raw/file.json')],
        'landingzone',
        'datalake',
        {'create_files': ['amc-connect/file.json'], 'check_files': ['amc/raw/file.json']}
    ),
    (
        [('landingzone/amc-connect/', 'datalake/amc/raw/')],
        'landingzone',
        'datalake',
        {'create_files': ['amc-connect/file.json'], 'check_files': ['amc/raw/file.json']}
    ),
    (
        [('landingzone/amc-connect/', 'datalake/amc/raw/')],
        'landingzone',
        'datalake',
        {'create_files': ['amc-connect/file.json'], 'check_files': ['amc/raw/file.json']}
    ),
    (
        [('landingzone/amc-connect/', 'datalake/amc/raw/')],
        'landingzone',
        'datalake',
        {
            'create_files': [
                'amc-connect/file.json',
                'amc-connect/file2.json',
                'amc-connect/test/file.json'
            ],
            'check_files': [
                'amc/raw/file.json',
                'amc/raw/file2.json',
                'amc/raw/test/file.json'
            ]
        }
    )
])
def test_copy_files_successful(s3_mock, transactions, source_bucket, target_bucket, files):
    """copy_files should copie files to target location."""
    # Arrange
    create_s3_files(s3_mock, files['create_files'], bucket=source_bucket)
    s3_mock.create_bucket(Bucket=target_bucket)

    # Act & Assert
    assert copy_files(transactions) is None

    for target_file in files['check_files']:
        res = s3_mock.get_object(Bucket=target_bucket, Key=target_file)
        assert res['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.parametrize('transactions,source_bucket,target_bucket,error_msg', [
    (
        [('landingzone/amc-connect/fol', 'datalake/amc/raw/')],
        'landingzone',
        'datalake',
        'Prefix must end with "/", passed was amc-connect/fol'
    ),
    (
        [('landingzone/amc-connect/', 'datalake/amc/raw')],
        'landingzone',
        'datalake',
        'Prefix must end with "/", passed was amc/raw'
    )
])
def test_copy_files_throws_exceptions(
        s3_mock,
        transactions,
        source_bucket,
        target_bucket,
        error_msg):
    """copy_files failes when there is no files to copie."""
    # Arrange
    s3_mock.create_bucket(Bucket=source_bucket)
    s3_mock.create_bucket(Bucket=target_bucket)

    # Act & Assert
    with pytest.raises(ValueError) as excinfo:
        copy_files(transactions)

    assert error_msg in str(excinfo.value)


@mock.patch('big_data_getl.utils.delete_files')
@mock.patch('big_data_getl.utils.copy_files')
def test_copy_and_cleanup_pass_parameters(m_copy, m_delete):
    """copy_files is called with right parameters and in right order."""
    # Arrange & Act
    copy_and_cleanup([('bucket/key', 'bucket/key2')])

    # Assert
    m_copy.assert_called_once_with([('bucket/key', 'bucket/key2')])
    m_delete.assert_called_once_with(['bucket/key'])
