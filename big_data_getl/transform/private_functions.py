"""Private functions for spark transformation operations."""
import logging

from typing import Callable, List, Tuple, TypeVar
from pyspark.sql import DataFrame, types
from pyspark.sql.column import Column
from pyspark.sql.utils import AnalysisException

LOGGING = logging.getLogger(__name__)


PREDICATE = Tuple[str, str, str]
LOGICALPREDICATE = Tuple[PREDICATE, str, PREDICATE]
PredicateType = TypeVar('T', PREDICATE, LOGICALPREDICATE)


def _select_fields(df: DataFrame, cols: List[str], add_cols=False) -> DataFrame:
    """Select columns mentioned in cols argument.

    If, add_cols is true, add missing columns with None values.
    Raises: ReadableValue error when add_cols is set to False

    """
    try:
        return df.select(cols)
    except AnalysisException as column_not_found:
        LOGGING.error(str(column_not_found))
        raise ValueError(str(column_not_found))


def _add_fields(df: DataFrame, new_col: str, func: Callable) -> DataFrame:
    """Return dataframe with new column applying specified function.

    Cases Covered:
    --------------
    1. new col with null fields
    2. new col concat from two cols
    3. new col with literal value
    4. new col with calculated value

    """
    return df


def _cast_column(df: DataFrame, col: str, new_data_type: types) -> Column:
    """Return DF with the column cast to new type and with the columns in the same order."""

    # TODO 1. Should we raise an exception when trying to cast into uncastable datatype?
    # TODO 2. How should this function be used. Where should it be placed in the YAML?
    eval_string = ""
    try:
        if "(" in str(new_data_type):
            eval_string = "df.{}.cast(types.{})".format(col, new_data_type)
        else:
            eval_string = "df.{}.cast(types.{}())".format(col, new_data_type)
        return eval(eval_string)
    except AttributeError:
        raise AttributeError('Column \'{}\' not found in df'.format(col))


def _unroll(df: DataFrame, col: str, new_col_list: list, drop=False) -> DataFrame:
    """Explode the DF with values in the new col. Drop the original column if drop is set to true."""
    cols = [col + '.' + s for s in new_col_list]
    cols[:0] = [col]
    df = _select_fields(df, cols)
    if drop:
        df = df.drop(col)
    return df


def _join(left_df: DataFrame, right_df: DataFrame, cols: List[str], join_type='left') -> DataFrame:
    """Return a joined DF."""
    return left_df.join(right_df, cols, join_type)


def _union(left_df: DataFrame, right_df: DataFrame) -> DataFrame:
    """Return union of DFs."""
    try:
        return left_df.union(right_df)
    except AnalysisException as exception:
        LOGGING.error(str(exception))
        raise ValueError(str(exception))


def _where(df: DataFrame, predicate: PredicateType) -> DataFrame:
    """Apply where to DF and returns rows satifying the specified condition."""
    try:
        return df.where(_predicate_to_sql(predicate))
    except AnalysisException as analysis_exception:
        LOGGING.error(str(analysis_exception))
        raise ValueError(str(analysis_exception))


def _filter(df: DataFrame, param: PredicateType) -> DataFrame:
    """Apply filter to DF and filters out(removes) rows satifying the specified condition."""
    return df.subtract(_where(df, param))


def _predicate_to_sql(predicate: PredicateType, sql: str = '') -> str:
    """Convert user predicate input to a valid SQL query string."""
    _validate_param(predicate)

    if _is_predicate(predicate):
        return """{} {} {} {}""".format(
            sql,
            predicate[0],
            predicate[1],
            _format_operand(predicate[2])
        ).strip()
    return '({} {} {})'.format(
        _predicate_to_sql(predicate[0], sql),
        predicate[1],
        _predicate_to_sql(predicate[2], sql)
    )


def _is_predicate(predicate: PredicateType) -> bool:
    """Check the format of predicate and return boolean."""
    return not _is_logical_predicate(predicate)


def _is_logical_predicate(predicate: PredicateType) -> bool:
    """Check the format of logical predicate and return boolean."""
    return tuple(map(type, predicate)) == (tuple, str, tuple)


def _validate_param(predicate: PredicateType) -> None:
    """Validate predicate and logical predicate and raise value error if not."""
    if _is_predicate(predicate):
        _validate_predicate(predicate)

    if _is_logical_predicate(predicate):
        _validate_logical_predicate(predicate)


def _validate_logical_predicate(predicate) -> None:
    """Raise value error if the Logical Predicate operand is not AND/OR."""
    if predicate[1].lower() not in ('and', 'or'):
        raise ValueError(
            'Only \'AND/OR\' allowed in LogicalPredicate. But \'{}\' was provided'.format(
                predicate[1])
        )


def _validate_predicate(predicate: PredicateType) -> None:
    """Raise value error if the parameters does not confirm to allowed data types."""
    allowable_types = [int, float, str, list, bool]
    if not tuple(map(type, predicate)) in [(str, str, dt) for dt in allowable_types]:
        raise ValueError(
            'Expected format: (tuple, str, tuple) or any of, {}. But, got {}'.format(
                [(str, str, dt) for dt in allowable_types], predicate))


def _format_operand(variable: str) -> str:
    """Convert constant to SQL format according to its datatype."""
    if isinstance(variable, str):
        return "\'{}\'".format(variable)

    if isinstance(variable, list):
        if len(variable) > 1:
            return tuple(variable)

        return "(\'{}\')".format(variable[0])

    return variable
