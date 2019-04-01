"""
The Write module containing spark write functions for lift jobs.

1. write_json: Writes dataframe as JSON file.
2. write_delta: Writes dataframe as a delta file.

"""
import logging

from enum import Enum
from pyspark.sql import DataFrame

LOGGING = logging.getLogger(__name__)


class WriteMode(Enum):
    """Enum of the different write modes."""

    OVERWRITE = 'overwrite'
    APPEND = 'append'
    IGNORE = 'ignore'
    ERROR = 'error'
    ERRORIFEXISTS = 'errorifexists'


def write_json(dataframe: DataFrame,
               folder_path: str,
               write_mode: WriteMode = WriteMode.OVERWRITE.value) -> None:
    """Write the dataframe as json to a location.

    Args:
        dataframe   (DataFrame): Dataframe that is to be written.
        folder_path (str): Complete S3 folder path where file is to be written.
        write_mode  (str): Specifies the behavior of save operation when data already exists.
                           write_mode set to 'overwrite' by default for JSON file which replaces the
                           existing data (if any) by new incoming data

    Returns:
        None

    Sample Use:
        write_json(dataframe, 's3://husqvarna-datalake/trusted/write_here/', 'overwrite')

    """
    # coalescing small files into 1 larger file.
    df_repartitioned = dataframe.repartition(1)
    _write(df_repartitioned, folder_path, 'json', write_mode)


def write_delta(dataframe: DataFrame,
                folder_path: str,
                write_mode: WriteMode = WriteMode.APPEND.value) -> None:
    """Write the dataframe as delta to a location.

    Args:
        dataframe   (DataFrame): Dataframe that is to be written.
        folder_path (str): Complete S3 folder path where file is to be written.
        write_mode  (str): Specifies the behavior of save operation when data already exists.
                           write_mode set to 'append' by default for JSON file which appends the new
                           incoming data with the already existing (if any).

    Returns:
        None

    Sample Use:
        write_delta(df, 's3://husqvarna-datalake/trusted/write_here/', 'append')

    """
    _write(dataframe, folder_path, 'delta', write_mode)


def _write(dataframe: DataFrame, folder_path: str, write_format: str, write_mode: str) -> None:
    dataframe.write.save(path=folder_path, format=write_format, mode=write_mode)
