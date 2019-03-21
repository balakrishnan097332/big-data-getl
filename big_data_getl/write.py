"""
The Write module containing spark write functions for lift jobs.

1. write_json: Writes dataframe as JSON file.
2. write_delta: Writes dataframe as a delta file.

TODO:
    Add validation for write_mode. Ensuring correct enums are passed.
    Add typing for all the private functions.

"""
import logging

from pyspark.sql import DataFrame

LOGGING = logging.getLogger(__name__)


def enum(**named_values):
    return type('Enum', (), named_values)


WRITE_MODE_ENUM = enum(OVERWRITE='overwrite', APPEND='append',
                       IGNORE='ignore', ERROR='error', ERRORIFEXISTS='errorifexists')


def write_json(dataframe: DataFrame,
               folder_path: str,
               write_mode=WRITE_MODE_ENUM.OVERWRITE) -> None:
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


def write_delta(dataframe: DataFrame, folder_path: str, write_mode=WRITE_MODE_ENUM.APPEND) -> None:
    """Write the dataframe as delta to a location.

    Args:
        spark       (SparkSession): SparkSession instance.
        dataframe   (DataFrame): Dataframe that is to be written.
        folder_path (str): Complete S3 folder path where file is to be written.
        write_mode  (str): Specifies the behavior of save operation when data already exists.
        write_mode set to 'append' by default for JSON file which appends the new incoming data
        with the already existing data (if any).
    Returns:
        None

    Sample Use:
        write_delta(df, 's3://husqvarna-datalake/trusted/write_here/', 'append')

    """
    _write(dataframe, folder_path, 'delta', write_mode)


def _write(dataframe: DataFrame, folder_path: str, write_format: str, write_mode: str) -> None:
    if write_mode in (WRITE_MODE_ENUM.OVERWRITE,
                      WRITE_MODE_ENUM.APPEND,
                      WRITE_MODE_ENUM.ERROR,
                      WRITE_MODE_ENUM.ERRORIFEXISTS,
                      WRITE_MODE_ENUM.ERROR):
        dataframe.write.save(
            path=folder_path,
            format=write_format,
            mode=write_mode)
    else:
        raise ValueError(
            'Allowable write modes are overwrite, append, error, errorifexisits, error. But {} was provided'.format(
                write_mode))
