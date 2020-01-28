"""Unit test for GETL transform function."""

from unittest import mock
# from ast import literal_eval
import pytest

from big_data_getl.transform.transform import transform
from big_data_getl.transform.private_functions import _predicate_to_sql
from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql.column import Column


# HELPER


def create_simple_df(spark_session) -> DataFrame:
    """Return a valid DF."""
    return (spark_session.createDataFrame(
        [("Cinderella", 16, False),
         ("Snow white", 17, True),
         ("Belle", 18, False),
         ("Jasmine", 19, True)],
        ["name", "age", "happy"]))


@pytest.mark.parametrize('args', [
    [('add_fields', ('new_col', 'fiter_val'))],
    [('add_fields', ('new_col', 'fiter_val'))],
    []
])
@mock.patch('big_data_getl.transform.transform.DataFrame')
def test_transform_returns_df_successfully(m_df, args):
    """Transform returns DF successfully."""
    assert transform(m_df, args) is m_df


@pytest.mark.spark
@pytest.mark.parametrize('args,princess_name', [
    ([('where', [('age', '<', 18)])], ["Cinderella", "Snow white"]),
    ([('where', [('name', 'like', 'Cin%')])], ["Cinderella"]),
    ([('where', [('name', 'like', '%in%')])], ["Cinderella", "Jasmine"]),
    ([('where', [('happy', '==', True)])], ["Snow white", "Jasmine"]),
    ([('where', [(('happy', '==', True), 'and', ('age', '<', 18))])], ["Snow white"]),
    ([('where', [(('happy', '==', False), 'and', ('age', '<', 17))])], ["Cinderella"]),
])
def test_where_returns_df_successfully(args, princess_name, spark_session):
    """_where returns DF successfully."""
    # Act
    result = transform(create_simple_df(spark_session), args)

    # Assert
    assert result.count() == len(princess_name)


@pytest.mark.parametrize('args,q_string', [
    (('age', '<', 18), "age < 18"),
    (('name', 'like', 'Cin%'), "name like 'Cin%'"),
    (('happy', '==', True), "happy == True"),
    ((('happy', '==', True), 'and', ('age', '<', 18)), "(happy == True and age < 18)"),
    ((('happy', '==', False), 'and', ('age', '<', 17)), "(happy == False and age < 17)")
])
def test_predicate_to_sql_returns_successfully(args, q_string):
    """predicate_to_sql returns query string successfully after parsing input parms."""
    # Act
    result = _predicate_to_sql(args)

    # Assert
    assert result == q_string


@pytest.mark.parametrize('args,error', [
    (('age', '<'), None),
    (('name', 'Cin%'), None),
    ((True, '==', 'happy'), None),
    ((('happy', '==', True, 'last'),
      'and', ('age', '<', 18)), "('happy', '==', True, 'last')"),
    ((('happy', '==', False), 'and', (17, ' <', 'age')), "(17, ' <', 'age')"),
    ([('where', [(('happy', '==', True),
       'and', ('age', '<', 18),
       'and', ('colors', 'in', ['green']))])], None)
])
def test_predicate_to_sql_raises_value_error(args, error):
    """predicate_to_sql raises value error for illegal operand datatypes."""
    # Arrange
    allowable_types = [int, float, str, list, bool]
    error_message = 'Expected format: (tuple, str, tuple) or any of, {}. But, got {}'.format(
        [(str, str, dt) for dt in allowable_types], args if error is None else error)

    # Act # Assert
    with pytest.raises(ValueError) as value_error:
        _predicate_to_sql(args)

    assert error_message in str(value_error)


@pytest.mark.spark
@pytest.mark.parametrize('args,col_name', [
    ([('where', [('age1', '==', 1)])], 'age1'),
    ([('where', [('name1', '==', 'b')])], 'name1'),
    ([('where', [('name1', '>', 'b')])], 'name1'),
    ([('where', [('age1', '>', 2)])], 'age1'),
    ([('where', [('age1', '>', 'c')])], 'age1'),
    ([('where', [(('name1', 'in', ['c', 'z']), 'or', ('yes', '!=', False))])], 'name1'),
    ([('where', [(('age1', '>', 2), 'and', ('age', '>', 'c'))])], 'age1')
])
def test_where_throws_col_not_found(args, col_name, spark_session):
    """Spark throws column not found exception."""
    # Arrange
    error_message = "cannot resolve '`{}`' given input columns: [name, age, happy]".format(
        col_name
    )
    # Act
    with pytest.raises(ValueError) as col_not_found:
        transform(create_simple_df(spark_session), args)
    # Assert
    assert error_message in str(col_not_found)


@pytest.mark.parametrize('args,operator', [
    ([('where', [(('name1', 'in', ['c', 'z']), '&', ('yes', '!=', False))])], '&'),
    ([('where', [(('age1', '|', 2), '|', ('age', '>', 'c'))])], '|')
])
@mock.patch('big_data_getl.transform.transform.DataFrame')
def test_where_throws_invalid_operator(m_df, args, operator):
    """Where throws invalid operator when passing an invalid LogicalPredicate."""
    # Arrange
    error_message = (
        'Only \'AND/OR\' allowed in LogicalPredicate. But \'{}\' was provided'.format(
            operator
        )
    )

    # Act
    with pytest.raises(ValueError) as invalid_operator:
        transform(m_df, args)

    # Assert
    assert error_message in str(invalid_operator)


@pytest.mark.parametrize('args, call_parm', [
    ([('filter', [('age', '==', 1)])], "age == 1"),
    ([('filter', [(('age', '>', 2), 'and', ('age', '>', 'c'))])], "(age > 2 and age > 'c')")
])
@mock.patch('big_data_getl.transform.transform.DataFrame')
def test_filter_passes_right_parameters(m_df, args, call_parm):
    """transform_filter passes right parameters and in right order."""
    # Assign
    m_df.where.return_value = 'm_where_df'
    transform(m_df, args)
    # Act & Assert
    m_df.where.assert_called_with(call_parm)
    m_df.subtract.assert_called_with('m_where_df')


@pytest.mark.spark
@pytest.mark.parametrize('args,princess_name', [
    ([('filter', [('age', '<', 18)])], ["Belle", "Jasmine"]),
    ([('filter', [('name', 'like', 'Cin%')])], ["Belle", "Jasmine", "Snow white"]),
    ([('filter', [('name', 'like', '%in%')])], ["Belle", "Snow white"]),
    ([('filter', [('happy', '==', True)])], ["Belle", "Cinderella"]),
    ([('filter', [(('happy', '==', True), 'and', ('age', '<', 18))])],
     ["Belle", "Jasmine", "Cinderella"]),
    ([('filter', [(('happy', '==', False), 'and', ('age', '<', 17))])],
     ["Belle", "Jasmine", "Snow white"])
])
def test_filter_returns_df_successfully(args, princess_name, spark_session):
    """_filter returns DF successfully."""
    # Arrange
    q_string = ''
    if len(princess_name) > 1:
        q_string = "\"name in {}\"".format(tuple(princess_name))
    else:
        q_string = "\"name in (\'{}\')\"".format(princess_name[0])

    # Act
    result = transform(create_simple_df(spark_session), args)

    # Assert
    assert result.where(eval(q_string)).count() == len(princess_name)


@pytest.mark.spark
@pytest.mark.parametrize('args,col_count', [
    ([('select', [['name', 'age']])], 2),
    ([('select', [['name', 'age', 'happy']])], 3),
    ([('select', [['*']])], 3)
])
def test_select_returns_df_successfully(args, col_count, spark_session):
    """Select returns DF successfully."""
    # Act
    result = transform(create_simple_df(spark_session), args)

    # Assert
    assert len(result.columns) == col_count


@pytest.mark.spark
@pytest.mark.parametrize('args', [
    ([('cast_column', ["age", T.StringType()])]),
    ([('cast_column', ["name", T.DecimalType()])]),
    ([('cast_column', ["happy", T.DateType()])]),
])
def test_cast_returns_df_successfully(args, spark_session):
    """cast_column returns DF successfully after type cast even when uncastable."""
    # Act
    result = transform(create_simple_df(spark_session), args)

    # Assert
    assert isinstance(result, Column)


@pytest.mark.spark
@pytest.mark.parametrize('args,col', [
    ([('cast_column', ["no_age", T.DateType()])], "no_age"),
    ([('cast_column', ["no_name", T.StringType()])], "no_name"),
])
def test_cast_attribute_error(args, col, spark_session):
    """cast_column returns attribute error when column not found."""
    # Assign
    error_message = 'Column \'{}\' not found in df'.format(col)
    # Act & Assert
    with pytest.raises(AttributeError) as column_not_found:
        transform(create_simple_df(spark_session), args)
    assert error_message in str(column_not_found)


# Union TCs


@pytest.mark.spark
def test_union_returns_df_successfully(spark_session):
    """Union returns DF successfully."""
    # Arrange
    df = create_simple_df(spark_session)

    # Act
    result = transform(df, [('union', [df])])

    # Assert
    assert isinstance(result, DataFrame)


@pytest.mark.spark
@pytest.mark.parametrize('eval_df, error_message', [
    ('df.select(["name", "age", df.happy.cast(T.StringType())])',
     'Union can only be performed on tables with the compatible column types'),
    ('df.drop("name")',
     'Union can only be performed on tables with the same number of columns'),
])
def test_union_raises_value_error_datatype(eval_df, error_message, spark_session):
    """Union returns Value error for union of 2 unsymetrical DFs."""
    # Arrange
    df = create_simple_df(spark_session)

    # Act & Assert
    with pytest.raises(ValueError) as union_error:
        transform(eval(eval_df), [('union', [df])])
    assert error_message in str(union_error)


# Join TCs

@pytest.mark.spark
@pytest.mark.parametrize('col_list, join_type', [
    (['name'], 'left'),
    (['name', 'age'], 'right'),
    (['name', 'age'], 'inner'),
    (['name', 'age'], 'outer'),
])
def test_join_returns_df_successfully(col_list, join_type, spark_session):
    """Join returns DF successfully."""
    # Arrange
    df = create_simple_df(spark_session)
    args = [('join', [df, col_list, join_type])]

    # Act
    result = transform(df, args)

    # Assert
    assert isinstance(result, DataFrame)


@pytest.mark.spark
@pytest.mark.parametrize('col_list, join_type', [
    (['name'], 'left'),
    (['name', 'age'], 'right'),
    (['name', 'age'], 'inner'),
    (['name', 'age'], 'outer'),
])
@mock.patch('big_data_getl.transform.transform.DataFrame')
def test_join_called_in_right_order(m_df, col_list, join_type):
    """Join is called with right parameters and in right order."""
    # Assign
    args = [('join', [m_df, col_list, join_type])]

    # Act
    transform(m_df, args)

    # Act
    m_df.join.assert_called_with(m_df, col_list, join_type)

# Unroll TC


@pytest.mark.spark
@pytest.mark.parametrize('args, col_count', [
    ([('unroll', ['payload', ['api-key-name', 'attributes', 'locale', 'type']])], 5),
    ([('unroll', ['payload', ['attributes', 'locale', 'type']])], 4),
    ([('unroll', ['payload', ['attributes', 'locale', 'type'], True])], 3),
])
def test_unroll_returns_df_successfully(args, col_count, spark_session):
    """Unroll returns DF successfully."""
    # Arrange
    file_name = './tests/data/complex_json_sample.json'
    df = spark_session.read.option("multiline", "true").json(file_name)

    # Act
    result = transform(df, args)

    # Assert
    assert isinstance(result, DataFrame)
    assert len(result.columns) == col_count


@pytest.mark.parametrize('args, call_parm, drop', [
    ([('unroll', ['payload', ['api-key-name', 'attributes', 'locale', 'type']])],
     ['payload', 'payload.api-key-name', 'payload.attributes', 'payload.locale', 'payload.type'],
     False),
    ([('unroll', ['payload', ['attributes', 'locale', 'type']])],
     ['payload', 'payload.attributes', 'payload.locale', 'payload.type'], False),
    ([('unroll', ['payload', ['attributes', 'locale', 'type'], True])],
     ['payload', 'payload.attributes', 'payload.locale', 'payload.type'], True),
])
@mock.patch('big_data_getl.transform.transform.DataFrame')
def test_unroll_passes_right_parameters(m_df, args, call_parm, drop):
    """Unroll passes right parameters and in right order."""
    # Assign
    m_df.select.return_value.drop.return_value = 'm_drop_df'

    # Act
    transform(m_df, args)

    # Act
    m_df.select.assert_called_with(call_parm)
    if drop:
        m_df.select.return_value.drop.assert_called_with('payload')
