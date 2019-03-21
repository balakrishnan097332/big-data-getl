"""Unit test for GETL utils function."""
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from pyspark.sql.types import StructType

from big_data_getl.utils import (copy_and_cleanup, copy_keys, delete_files,
                                 json_to_spark_schema)
from tests.data.utils.example_schema import create_json_schema


# HELPER FUNCTION
def create_s3_files(s3_mock, keys, bucket='tmp-bucket') -> None:
    """Create files in S3 bucket."""
    s3_mock.create_bucket(Bucket=bucket)
    for key in keys:
        s3_mock.put_object(Bucket=bucket, Key=key,
                           Body=b'Here we have some test data')


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
    with pytest.raises(KeyError) as key_error:
        json_to_spark_schema(create_json_schema(invalid_schema))

    # Assert
    assert 'Missing key: \'{0}\'. Valid format: {1}'.format(
        missed_key, 'All schema columns must have a name, type and nullable key'
    ) in str(key_error)


@pytest.mark.parametrize('invalid_json', [
    'invalid', {'invalid', }
])
def test_json_to_spark_invalid_json(invalid_json):
    """json_to_spark_schema should raise TypeError for invalid json."""
    # Arrange & Act
    with pytest.raises(TypeError) as type_error:
        json_to_spark_schema(invalid_json)

    # Assert
    assert 'Invalid json was provided' in str(type_error)


@pytest.mark.parametrize('paths', [
    ['husqvarna-datalake/raw/including/not_present.txt'],
    ['my/key/including/n_present.txt',
        'husqvarna-datalake/raw/including/not_present.txt']
])
def test_delete_files_not_possible_from_raw(paths):
    """delete_files returns PermissionError when deleting files from raw."""
    # Act & Assert
    with pytest.raises(PermissionError) as excinfo:
        delete_files(paths)

    assert 'Access Denied: Not possible to remove files from raw layer' in str(
        excinfo)


@pytest.mark.parametrize('paths,bucket,files', [
    ([], 'landingzone', []),
    (
        ['landingzone/amc-connect/file.json',
            'landingzone/amc-connect/test/file.json'],
        'landingzone',
        ['amc-connect/test/file.json', 'amc-connect/test/file.json']
    )
])
def test_delete_files_success(s3_mock, paths, bucket, files):
    """delete_files should remove files successfully."""
    # Arrange
    create_s3_files(s3_mock, files, bucket=bucket)

    # Act & Assert
    assert delete_files(paths) is None

    for _file in files:
        with pytest.raises(ClientError) as excinfo:
            s3_mock.get_object(Bucket=bucket, Key=_file)

        assert 'NoSuchKey' in str(excinfo)


@pytest.mark.parametrize('paths,bucket', [
    ([], 'landingzone'),
    (
        ['landingzone/amc-connect/file.json',
         'landingzone/amc-connect/test/file.json'],
        'landingzone'
    )
])
def test_delete_files_success_nofile(s3_mock, paths, bucket):
    """delete_files should run successfully even when files not found."""
    # Arrange
    s3_mock.create_bucket(Bucket=bucket)

    # Act & Assert
    assert delete_files(paths) is None


@mock.patch('big_data_getl.utils.boto3')
def test_copy_keys_passes_correct_parameters(m_boto3):
    """copy_keys is called with right parameters and in right order."""
    # Arrange
    m_s3 = m_boto3.client
    m_s3.return_value.get_paginator.return_value.paginate.return_value = [
        {'Contents': [{'Key': 'fake/key'}]}
    ]
    copy_source = {
        'Bucket': 'landingzone',
        'Key': 'amc-connect/fake/key.json'
    }

    # Act
    copy_keys([('landingzone/amc-connect/fake/key.json',
                'datalake/amc/raw/fake/key.json')])

    # Assert
    m_s3.assert_called_with('s3')
    m_s3.return_value.copy.assert_called_with(
        copy_source,
        'datalake',
        'amc/raw/fake/key.json'
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
        {'create_files': ['amc-connect/file.json'],
            'check_files': ['amc/raw/file.json']}
    ),
    (
        [('landingzone/amc-connect/file.json', 'datalake/amc/raw/file.json'),
         ('landingzone/amc-connect/file2.json', 'datalake/amc/raw/file2.json'),
         ('landingzone/amc-connect/test/file.json', 'datalake/amc/raw/test/file.json')],
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
def test_copy_keys_successful(s3_mock, transactions, source_bucket, target_bucket, files):
    """copy_keys should copy files to target location."""
    # Arrange
    create_s3_files(s3_mock, files['create_files'], bucket=source_bucket)
    s3_mock.create_bucket(Bucket=target_bucket)

    # Act & Assert
    assert copy_keys(transactions) is None

    for target_file in files['check_files']:
        res = s3_mock.get_object(Bucket=target_bucket, Key=target_file)
        assert res['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.parametrize('transactions,source_bucket,target_bucket,error_msg', [
    (
        [('landingzone/amc-connect/file.json', 'datalake/amc/raw/file.json')],
        'landingzone',
        'datalake',
        'File not found with bucket: landingzone key: amc-connect/file.json'
    ),
    (
        [('landingzone/amc-connect/file.json', 'datalake/amc/raw/file.json')],
        'wrong_src_bkt',
        'wrong_tgt_bkt',
        'The specified bucket landingzone does not exist'
    )
])
def test_copy_keys_throws_exceptions(s3_mock,
                                     transactions,
                                     source_bucket,
                                     target_bucket,
                                     error_msg):
    """copy_keys throws exception when files or bucket not found."""
    # Arrange
    s3_mock.create_bucket(Bucket=target_bucket)
    s3_mock.create_bucket(Bucket=source_bucket)

    # Act & Assert
    with pytest.raises(FileNotFoundError) as file_not_found:
        copy_keys(transactions)

    assert error_msg in str(file_not_found)


@mock.patch('big_data_getl.utils.delete_files')
@mock.patch('big_data_getl.utils.copy_keys')
def test_copy_and_cleanup_pass_parameters(m_copy, m_delete):
    """copy_keys is called with right parameters and in right order."""
    # Arrange & Act
    copy_and_cleanup([('bucket/key', 'bucket/key2')])

    # Assert
    m_copy.assert_called_once_with([('bucket/key', 'bucket/key2')])
    m_delete.assert_called_once_with(['bucket/key'])


@mock.patch('big_data_getl.utils.delete_files')
@mock.patch('big_data_getl.utils.copy_keys')
def test_copy_and_cleanup_call_order(m_copy, m_delete):
    """copy_keys is called with copy and delete functions in right order."""
    # Arrange & Act
    manager = mock.Mock()
    manager.attach_mock(m_copy, 'c')
    manager.attach_mock(m_delete, 'd')

    copy_and_cleanup([('from', 'to')])

    # Assert
    expected_call = [mock.call.c([('from', 'to')]), mock.call.d(['from'])]
    assert manager.mock_calls == expected_call
