"""
The Utils module containing utility functions for lift jobs.

Utilities supported:
1. json_to_spark_schema: Converts a json schema to spark schema.
2. delete_files: Deletes a list of s3 files provided.
3. copy_keys: Copies files between S3 buckets.
4. copy_and_cleanup: Copies files between S3 and removes them from source.

"""
import logging
from typing import Dict, List, Tuple, TypeVar

import boto3
from botocore.exceptions import ClientError

from pyspark.sql.types import StructType

LOGGING = logging.getLogger(__name__)
JsonSchemaType = TypeVar('T', int, float, str)


def json_to_spark_schema(json_schema: Dict[str, JsonSchemaType]) -> StructType:
    """
    Return Spark Schema for a JSON schema.

    Args:
        json_schema (Dict[str, JSON_SCHEMA_TYPE]): schema in json format.

    Returns:
        StructType: Spark Schema for the corresponding JSON schema.

    Raises:
        KeyError: Missing Schema key fields Name/Field/Nullable
        TypeError: Invalid json was provided

    """
    try:
        return StructType.fromJson(json_schema)
    except KeyError as key_error:
        LOGGING.error(str(key_error))
        raise KeyError('Missing key: {0}. Valid format: {1}'.format(
            str(key_error), 'All schema columns must have a name, type and nullable key'
        ))
    except TypeError as key_error:
        LOGGING.error(str(key_error))
        raise TypeError('Invalid json was provided')


def delete_files(paths: List[str]) -> None:
    """Delete list of files from S3 bucket.

     Args:
        paths  (List[str]): A list of paths pointing out to a key

    Returns:
        None

    Raises:
        PermissionError: When requested to deleted files from raw layer

    Sample Use:
        delete_files(['landingzone/amc-connect/file.txt', 'datalake/amc/raw/file.txt'])

    """
    if any('husqvarna-datalake/raw/' in path for path in paths):
        raise PermissionError('Access Denied: Not possible to remove files from raw layer')

    client = boto3.client('s3')

    for path in paths:
        bucket, key = _extract_bucket_and_key(path)
        client.delete_object(Bucket=bucket, Key=key)


def copy_and_cleanup(paths: List[Tuple[str]]) -> None:
    """Move files from source S3 bucket to the destination bucket.

     Args:
        paths (List[Tuple[str]]): a list that represents [('source', 'target')...]

    Returns:
        None

    Calls:
        copy_keys to copy files between buckets
        delete_files for source cleanup

    Sample Use:
        copy_keys([('landingzone/amc-connect/file.txt', 'datalake/amc/raw/file.txt')])

    """
    copy_keys(paths)
    delete_files([t[0] for t in paths])


def copy_keys(paths: List[Tuple[str]]) -> None:
    """Copy files from source S3 bucket to the destination bucket.

     Args:
        paths (List[Tuple[str]]): a list that represents [('source', 'target')...]

    Returns:
        None

    Raises:
        FileNotFoundError: When any of requested files are not found in S3

    Sample Use:
        copy_keys([('landingzone/amc-connect/file.txt', 'datalake/amc/raw/file.txt')])

    """
    client = boto3.client('s3')

    for path in paths:
        source_bucket, source_key = _extract_bucket_and_key(path[0])
        target_bucket, target_key = _extract_bucket_and_key(path[1])

        copy_source = {
            'Bucket': source_bucket,
            'Key': source_key
        }

        # Copy the file from the source key to the target key
        try:
            client.copy(copy_source, target_bucket, target_key)
        except ClientError as client_error:
            LOGGING.error(str(client_error))

            if client_error.response['Error']['Code'] == 'NoSuchBucket':
                raise FileNotFoundError(
                    'The specified bucket {} does not exist'.format(source_bucket))

            if client_error.response['Error']['Code'] == '404':
                raise FileNotFoundError('File not found with bucket: {} key: {}'.format(
                    source_bucket,
                    source_key
                ))

            raise client_error

#
# PRIVAT FUNCTIONS
#


def _extract_bucket_and_key(s3_path: str) -> List[str]:
    """Extract the bucket and the key from a string.

    The key can be either a key or a prefix
    """
    paths = s3_path.split('/')
    return paths[0], '/'.join(paths[1:])
