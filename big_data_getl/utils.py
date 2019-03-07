"""
The Utils module does the following.

1. json_to_spark_schema: Converts a json schema to spark schema.
2. delete_files: Deletes a list of s3 files provided.
3. copy_files: Copies files between S3 buckets.
4. copy_and_cleanup: Copies files between S3 and removes them from source.
"""
import logging
from pathlib import Path
from typing import Dict, List, Tuple, TypeVar

import boto3
from botocore.exceptions import ClientError
from pyspark.sql.types import StructType

LOGGING = logging.getLogger(__name__)
JsonSchemaType = TypeVar('T', int, float, str, complex)

# CONSTANTS
PAGE_SIZE = 1000


def json_to_spark_schema(json_schema: Dict[str, JsonSchemaType]) -> StructType:
    """
    Return Spark Schema for a JSON schema.

    Args:
        json_schema (Dict[str, JSON_SCHEMA_TYPE]): schema in json format.

    Returns:
        StructType: Spark Schema for the corresponding JSON schema.

    Raises:
        ValueError: Unable to load json schema(Invalid JSON).
        KeyError: Missing Schema key fields Name/Field/Nullable

    """
    try:
        return StructType.fromJson(json_schema)
    except KeyError as excinfo:
        LOGGING.error(str(excinfo))

        raise KeyError('Missing key: {0}. Valid format: {1}'.format(
            str(excinfo), 'All schema columns must have a name, type and nullable key'
        ))


def delete_files(paths: List[str]) -> None:
    """Delete list of files from S3 bucket.

     Args:
        paths  (List[str]): A list of paths pointing out a prefix or a key

    Returns:
        None

    Raises:
        FileNotFoundError: When any of requested files are not found in S3
        PermissionError: When requested to deleted files from raw layer

    Sample Use:
        delete_files(['landingzone/amc-connect/', 'datalake/amc/raw/file.txt'])

    """
    if any("/raw/" in path for path in paths):
        raise PermissionError('Access Denied: Not possible to remove files from raw layer')

    client = boto3.client('s3')

    for path in paths:
        bucket, location = _extract_bucket_and_location(path)

        # Validate if the iterator is empty
        for files in _get_collections_of_files(client, bucket, location):
            print(files)
            files_to_delete = {
                'Objects': list(map(lambda f: {'Key': f['Key']}, files))
            }
            result = client.delete_objects(Bucket=bucket, Delete=files_to_delete)

            if 'Errors' in result:
                raise FileNotFoundError('Could not delete the following file(s) {}'.format(
                    result['Errors']
                ))


def copy_and_cleanup(transactions: List[Tuple[str]]) -> None:
    """Move files from source S3 bucket to the destination bucket.

     Args:
        transactions (List[Tuple[str]]): a list that represents [('source', 'target')...]

    Returns:
        None

    Calls:
        copy_files to copy files between buckets
        delete_files for source cleanup

    Sample Use:
        copy_and_cleanup([('landingzone/amc-connect', 'datalake/amc/raw')])

    """
    copy_files(transactions)
    delete_files([t[0] for t in transactions])


def copy_files(transactions: List[Tuple[str]]) -> None:
    """Copy files from source S3 bucket to the destination bucket.

     Args:
        transactions (List[Tuple[str]]): a list that represents [('source', 'target')...]

    Returns:
        None

    Raises:
        FileNotFoundError: When any of requested files are not found in S3

    Sample Use:
        # Coping files with key
        copy_files([('landingzone/amc-connect/file.txt', 'datalake/amc/raw/file.txt')])

        # Coping files with prefix
        copy_files([('landingzone/amc-connect', 'datalake/amc/raw')])

    """
    try:
        client = boto3.client('s3')

        for transaction in transactions:
            source_bucket, source_location = _extract_bucket_and_location(transaction[0])
            target_bucket, target_location = _extract_bucket_and_location(transaction[1])

            # Check if the source location is a prefix
            is_prefix = _is_prefix(client, source_bucket, source_location)

            # If the source loc is a prefix then the target loc should also be a prefix
            if is_prefix:
                _validate_prefix(target_location)

            for source_file in _get_files(client, source_bucket, source_location):
                copy_source = {
                    'Bucket': source_bucket,
                    'Key': source_file['Key']
                }

                if is_prefix:
                    # Copy the file from the source prefix to the target prefix
                    target_key = _get_target_key(
                        target_location,
                        source_location,
                        source_file['Key']
                    )
                    client.copy(copy_source, target_bucket, target_key)
                else:
                    # Copy the file from the soruce key to the target key
                    client.copy(copy_source, target_bucket, target_location)

    except ClientError as error:
        raise FileNotFoundError(str(error))

#
# PRIVAT FUNCTIONS
#


def _get_target_key(target_prefix, source_prefix, source_key):
    """Create a new key for a object relative to the target prefix."""
    new_source_key = source_key.replace(source_prefix, '')
    return str(Path(target_prefix) / new_source_key)


def _validate_prefix(prefix):
    if not prefix.endswith('/'):
        raise ValueError('Prefix must end with "/", passed was {}'.format(prefix))


def _is_prefix(client, bucket, location):
    try:
        # Check if the location is a key pointing to one object
        client.head_object(Bucket=bucket, Key=location)
        return False
    except ClientError:
        # If the location should be a prefix that ends with a "/"
        _validate_prefix(location)
        return True


def _get_files(client, bucket, prefix):
    """Get a generator that will return file by file from a given prefix."""
    for page in _get_collections_of_files(client, bucket, prefix):
        for _file in page:
            yield _file


def _get_collections_of_files(client, bucket, prefix):
    """Get a generator that will return collection of files from a given prefix."""
    paginator = _get_paginator(client, bucket, prefix)

    for page in paginator:
        yield page['Contents'] if 'Contents' in page else []


def _get_paginator(client, bucket, key):
    return client.get_paginator('list_objects_v2').paginate(
        Bucket=bucket,
        Prefix=key,
        PaginationConfig={
            'PageSize': PAGE_SIZE
        }
    )


def _extract_bucket_and_location(s3_path):
    """Extract the bucket and the location from a string.

    The location can be either a key or a prefix
    """
    paths = s3_path.split('/')
    return paths[0], '/'.join(paths[1:])
