"""
The Write module does the following.

1. write: Writes a dataframe as JSON file.
2. write_df : Writes a dataframe as a delta file.

"""
import logging

from pyspark.sql import DataFrame
from botocore.exceptions import ClientError

LOGGING = logging.getLogger(__name__)


def write(
    df: DataFrame,
    folder_path: str,
    file_type: str,
    write_mode='append'
) -> None:
    """Write the dataframe to a file location.

    Args:
        df (DataFrame): Dataframe that is to be written.
        folder_path(str): Complete S3 folder path where file is to be written
            Sample usage - s3://husqvarna-datalake/trusted/system/write_here
        file_type(str): Spark output file type. Currently we support only JSON
        and Delta files.
        write_mode: Indicates if you want to overwrite or append to existing
        data of any.

    Returns:
        None

    Calls: write_delta to write files in delta file format.

    Raises:
        PermissionError: Access denied to output folder path.
        NotImplementedError: Reuqtest to write file in formats other
        than JSON or Delta

    """
    if file_type == 'json':
        try:
            df.write.mode(write_mode).format(file_type).save(folder_path)
        except ClientError as excinfo:
            LOGGING.error(str(excinfo))
            raise PermissionError(str(excinfo))
    elif file_type == 'delta':
        _write_delta(df, folder_path, write_mode)
    else:
        raise NotImplementedError(
            ('Write as {} is not supported. Accepted values are JSON & delta'
             ).format(
                file_type
            )
        )


def _write_delta(df: DataFrame, folder_path: str, write_mode: str) -> None:
    """Write the dataframe to a file location.

    Args:
        df (DataFrame): Dataframe that is to be written.
        folder_path(str): Complete S3 folder path where file is to be written
            Sample usage - s3://husqvarna-datalake/trusted/system/write_here
        file_type(str): Spark output file type. Currently we support only JSON
        and Delta files.
        write_mode: Indicates if you want to overwrite or append to existing
        data of any.

    Returns:
        None

    Calls: write_delta to write files in delta file format.

    Raises:
        ClientError: Access denied to output folder path.

    """
    try:
        df.write.mode(write_mode).format('delta').save(folder_path)
    except ClientError as excinfo:
        LOGGING.error(str(excinfo))
        raise PermissionError(str(excinfo))
