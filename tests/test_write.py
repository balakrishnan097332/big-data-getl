"""Unit test for GETL write function."""
import os
from unittest import mock

from tests.data.schema_sample import create_valid_schema

import pytest
from big_data_getl.write import write_delta, write_json


@pytest.mark.parametrize('mode', [
    None, 'append', 'overwrite', 'error'
])
@mock.patch('big_data_getl.write.DataFrame')
def test_write_json_passes_correct_parameters(m_df, mode):
    """write_json is called with right parameters and right order."""
    # Arrange
    if mode is None:
        args = (m_df, 'folder_path')
    else:
        args = (m_df, 'folder_path', mode)

    # Act & Assert
    assert write_json(*args) is None

    m_df.repartition.assert_called_with(1)
    m_df.repartition.return_value.write.save.assert_called_with(
        format='json',
        mode='overwrite' if mode is None else mode,
        path='folder_path'
    )


@pytest.mark.parametrize('mode', [
    None, 'append', 'overwrite', 'error'
])
@mock.patch('big_data_getl.write.DataFrame')
def test_write_delta_passes_correct_parameters(m_df, mode):
    """write_delta is called with right parameters and right order."""
    # Arrange
    if mode is None:
        args = (m_df, 'folder_path')
    else:
        args = (m_df, 'folder_path', mode)

    # Act & Assert
    assert write_delta(*args) is None

    m_df.write.save.assert_called_with(
        format='delta',
        mode='append' if mode is None else mode,
        path='folder_path'
    )


def test_write_json_writes_one_file(spark_session, pytestconfig, tmp_dir):
    """Write json should only write one file."""
    # Arrange
    data = [{'name': 'Mark Steelspitter'} for d in range(10000)]
    df = spark_session.createDataFrame(data, create_valid_schema())

    # Act
    write_json(df, tmp_dir)

    # Assert
    json_files = [_file for _file in os.listdir(tmp_dir) if _file.endswith('.json')]
    assert len(json_files) == 1
