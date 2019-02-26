"""Unit test for GETL load method."""
from os import environ
from unittest import mock

import pytest
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SparkSession

from big_data_getl.load import load_json, load_rdd, load_xml
from tests.data.load.example_schema import (create_json_schema,
                                            create_valid_schema)

environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages com.databricks:spark-xml_2.10:0.4.1 pyspark-shell')


# FUNCTIONS
@pytest.mark.spark
def test_load_json_valid_path(spark_session):
    """load_json should be able to load json files to a dataframe."""
    # Arrange
    file_paths = ['./tests/data/load/sample.json']

    # Act
    result_df = load_json(spark_session, file_paths, create_json_schema())

    # Assert
    assert str(result_df) == str(spark_session.createDataFrame(
        [{'name': 'Mark Steelspitter', 'empid': 9, 'happy': False}],
        create_valid_schema()
    ))


def test_load_json_passes_correct_parameters():
    """load_json should pass the correct parameters to the spark session."""
    # Arrange
    file_paths = ['./big_data_getl/load/test/data/sample.json']
    m_spark = mock.Mock()
    m_spark.read.schema.return_value.json.return_value = 'df_result'

    # Act
    result = load_json(m_spark, file_paths, create_json_schema())

    # Assert
    assert result == 'df_result'
    m_spark.read.schema.assert_called_with(create_valid_schema())
    m_spark.read.schema.return_value.json.assert_called_with(
        file_paths,
        multiLine=True
    )


@pytest.mark.spark
def test_load_json_invalid_path(spark_session, pytestconfig):
    """load_json should produces an FileNotFound exeption."""
    # Arrange
    invalid_file_paths = ['./file/that/do/not/exist.json']
    execution_dir = pytestconfig.rootdir

    # Act
    with pytest.raises(FileNotFoundError) as excinfo:
        load_json(
            spark_session,
            invalid_file_paths,
            create_json_schema('missing')
        )

    # Assert
    assert 'Path does not exist: file:{}/file/that/do/not/exist.json'.format(
        execution_dir
    ) in str(excinfo)


@pytest.mark.spark
@pytest.mark.parametrize('schema', [
    'valid',
    'castable',
    'uncastable',
    'missing',
    'missing_non_nullable'
])
def test_json_load_with_invalid_schema(spark_session, schema):
    """
    load_json should return success for an invalid schema.

    1. Valid schema that matches data
    2. Schema with Integer field defined as a string
    3. Schema with String field defined as an Integer
    4. A missing nullable field
    5. A missing non-nullable field
    """
    result_df = load_json(
        spark_session,
        paths=['./tests/data/load/sample.json'],
        json_schema=create_json_schema(schema)
    )

    assert isinstance(result_df, DataFrame)


def test_load_xml_passes_correct_parameters():
    """load_xml should pass correct parameters to the spark session."""
    # Arrange
    m_spark = mock.Mock()
    m_format = m_spark.read.schema.return_value.format
    m_format.return_value.options.return_value.load.return_value = 'df_result'

    # Act
    result = load_xml(
        m_spark,
        ['./big_data_getl/load/test/data/employee.xml'],
        create_json_schema(),
        'employee'
    )

    # Assert
    assert result == 'df_result'
    m_spark.read.schema.assert_called_with(create_valid_schema())
    m_format.assert_called_with('xml')
    m_format.return_value.options.assert_called_with(rowTag='employee')
    m_format.return_value.options.return_value.load.assert_called_with(
        './big_data_getl/load/test/data/employee.xml'
    )


@pytest.mark.spark
@pytest.mark.parametrize('schema,rowtag', [
    ('valid', 'employee'),
    ('valid', 'unknown_tag'),
    ('missing', 'employee'),
    ('missing', 'unknown_tag'),
    ('castable', 'employee'),
    ('castable', 'unknown_tag'),
    ('uncastable', 'unknown_tag'),
    ('uncastable', 'employee')
])
def test_load_xml_with_invalid_params(spark_session, schema, rowtag):
    """load_xml should return success for an invalid schema or rowtag."""
    result_df = load_xml(
        spark_session,
        paths=['./tests/load/data/employee.xml'],
        json_schema=create_json_schema(schema),
        row_tag=rowtag
    )

    assert isinstance(result_df, DataFrame)


@pytest.mark.parametrize('paths,val_paths', [
    ([], ''),
    (['a'], 'a'),
    (['a', 'b'], 'a,b')
])
def test_load_rdd_passes_correct_args(paths, val_paths):
    """load_rdd passes arguments to correct to spark context."""
    # Arrange
    m_spark = mock.Mock()
    text_file = m_spark.sparkContext.textFile
    text_file.return_value = 'test_result'

    # Act
    result_df = load_rdd(m_spark, paths)

    # Assert
    assert result_df == 'test_result'
    text_file.assert_called_with(val_paths)


@pytest.mark.spark
@pytest.mark.parametrize('file_paths', [
    ([]),
    (['./tests/load/data/employee.xml']),
    (['./file/not/exits.json']),
    (['./file/not/exits.json', './tests/load/data/employee.xml'])
])
def test_load_rdd_successful(spark_session, file_paths):
    """load_rdd should return a MapPartitionsRDD type."""
    assert isinstance(load_rdd(spark_session, file_paths), RDD)


@pytest.mark.parametrize('load_function,function_params,error_msg', [
    (load_json, {'json_schema': {}}, 'read'),
    (load_xml, {'json_schema': {}, 'row_tag': 'employee'}, 'read'),
    (load_rdd, {}, 'sparkContext')
])
def test_load_exception(load_function, function_params, error_msg):
    """load_json, load_xml, load_rdd should raise AttributeError.

    When passed invalid spark session
    """
    params = {
        'spark': SparkSession.builder,
        'paths': [],
        **function_params
    }

    with pytest.raises(AttributeError) as excinfo:
        load_function(**params)

    assert "'Builder' object has no attribute '{}'".format(
        error_msg
    ) in str(excinfo)
