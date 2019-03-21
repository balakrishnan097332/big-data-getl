"""Unit test for GETL utils function."""
import json
from unittest import mock
from unittest.mock import patch

import boto3
import botocore
import pytest
from moto import mock_s3

from big_data_getl.write import write
from pyspark.sql import DataFrame
from tests.data.utils.example_json_schema import create_json_schema


@pytest.mark.parametrize('file_type, write_mode', [
    ('json', 'append'),
    ('delta', 'append'),
    ('json', 'overwrite'),
    ('delta', 'overwrite')
])
@mock.patch('big_data_getl.write.DataFrame')
def test_write_passes_correct_parameters(m_df, file_type, write_mode):
    """Write is called with right parameters and right order.

    write returns none after successful write
    """
    # Act & Assert
    assert isinstance(
        write(m_df, 'folder_path', file_type, write_mode), None)
    m_df.write.mode.assert_called_with(write_mode)
    m_df.write.mode.return_value.format.assert_called_with(file_type)
    m_df.write.mode.return_value.format.return_value.save.assert_called_with(
        'folder_path')


@pytest.mark.parametrize('file_type', [
    'csv', 'parquet', 'orc', 'avro'
])
@mock.patch('big_data_getl.write.DataFrame')
def test_write_implementation_error(m_df, file_type):
    """Write should raise NotImplementedError when called to write
    file types other than json or delta."""
    # Arrange
    msg = ('Write as {} is not supported. Accepted values are JSON & delta'
           ).format(
        file_type
    )
    # Act & Assert
    with pytest.raises(NotImplementedError) as excinfo:
        write(m_df, 'folder_path', file_type)
    assert msg in str(excinfo)
