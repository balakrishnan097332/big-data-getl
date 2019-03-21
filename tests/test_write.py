"""Unit test for GETL write function."""
from unittest import mock

import pytest

from big_data_getl.write import write_json, write_delta


@pytest.mark.parametrize('mode', [
    None, 'append', 'overwrite', 'error'
])
@mock.patch('big_data_getl.write.DataFrame')
def test_write_json_passes_correct_parameters(m_df, mode):
    """write_json is called with right parameters and right order."""
    # Arrange
    m_df_repartition = m_df.repartition(1)
    if mode is None:
        args = (m_df, 'folder_path')
    else:
        args = (m_df, 'folder_path', mode)

    # Act & Assert
    assert write_json(*args) is None

    m_df.repartition.assert_called_with(1)
    m_df_repartition.write.save.assert_called_with(
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


@pytest.mark.parametrize('write_function', [
    write_delta, write_json
])
@mock.patch('big_data_getl.write.DataFrame')
def test_write_raises_error_invalid_writemode(m_df, write_function):
    """write is called with right parameters and right order."""
    # Assign
    args = (m_df, 'folder_path', 'invalid_mode')
    error = 'Allowable write modes are overwrite, append, error, errorifexisits, error. But invalid_mode was provided'
    # Act & Assert
    with pytest.raises(ValueError) as value_error:
        write_function(*args)
    assert error in str(value_error)
