"""Performs spark transformation operations."""
from typing import List, Tuple

from big_data_getl.transform import private_functions as pf
from pyspark.sql import DataFrame

FUNCTION_MAPPER = {
    'where': pf._where,
    'filter': pf._filter,
    'select': pf._select_fields,
    'add_fields': pf._add_fields,
    'cast_column': pf._cast_column,
    'unroll': pf._unroll,
    'join': pf._join,
    'union': pf._union
}


def transform(df: DataFrame, executor: List[Tuple[str]]) -> DataFrame:
    """Perform all transformation operations on the dataframe.

    Args:
        df (dataframe): Spark dataframe on which transformations needs to be applied on.
        executor (List[Tuple[str]]): List that represents [('function_name', 'function_args')...]

    Returns:
        DataFrame

    Calls:
        # TODO

    Sample Use:
        # TODO - Change it to final call footprint
        executor = [
            ('where', ('col', '=', 'fiter_val')),
            ('cast_col', ('col1', IntType)),
            ('select', (['col1', 'col2'])),
            ...
            ]
        df = transform(df, executor)

    """
    for params in executor:
        function_name = params[0]
        args = params[1]
        df = FUNCTION_MAPPER[function_name](df, *args)

    return df
