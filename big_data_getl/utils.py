"""
The Utils module does the following.

1. json_to_spark_schema: Converts a json schema to spark schema.
2. delete_files: Deletes a list of s3 files provided.
3. copy_files: Copies files between S3 buckets.
4. copy_and_cleanup: Copies files between S3 and removes them from source.
"""
import json
import logging
from typing import Dict, List, TypeVar

import boto3
import botocore
from boto.s3.connection import Bucket
from botocore.exceptions import ClientError

from pyspark.sql.types import StructType

LOGGING = logging.getLogger(__name__)
JSON_SCHEMA_TYPE = TypeVar('T', int, float, str, complex)


def json_to_spark_schema(
    json_schema: Dict[str, JSON_SCHEMA_TYPE]
) -> StructType:
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
        msg = 'All schema columns must have a name, type and nullable key'
        raise KeyError('Missing key: {0}. Valid format: {1}'.format(
            str(excinfo), msg
        )
        )


def delete_files(bucket_name: str, obj_list: List[str]) -> None:
    """Delete list of files from S3 bucket.

     Args:
        bucket_name (str): Name of S3 bucket.

    Returns:
        None

    Raises:
        FileNotFoundError: When any of requested files are not found in S3
        PermissionError: When requested to deleted files from raw layer

    """
    if any("/raw/" in s for s in obj_list):
        raise PermissionError('Access Denied to remove files from raw layer')

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    objects_to_delete = []

    for obj in obj_list:
        objects_to_delete.append({'Key': obj})

    delete_marker = bucket.delete_objects(
        Delete={
            'Objects': objects_to_delete
        }
    )
    if 'Errors' in delete_marker:
        raise FileNotFoundError(str(delete_marker['Errors']))


def copy_files(origin_bucket: str,
               destination_bucket: str,
               origin_stud: str,
               destination_stud: str,
               origin_paths: List[str]
               ) -> None:
    """Copy files from source S3 bucket to the destination bucket.

     Args:
        origin_bucket (str): Source S3 bucket.
        destination_bucket (str): Destination S3 bucket.
        origin_stud (str): Source S3 path
        destination_stud (str): Destination S3 path
        origin_paths (List[str]): Trailing source S3 paths under which
                                  destination files also will be written.

    Returns:
        None

    Raises:
        FileNotFoundError: When any of requested files are not found in S3

    Sample Use:
        copy_files('husqvarna-dl-landingzone-dev-amc',
                   'husqvarna-datalake',
                   'husqvarna-dl-landingzone-dev-amc/landing-zone/amc-connections',
                   'husqvarna-datalake/raw/amc/amc-connections',
                   [
                   '2018/10/17/12/amc-dev-connections-2-2018-10-17-12-30-10-bf0682b8-57cf-49e6-9c99.gz',
                   '2018/10/17/11/amc-dev-connections-1-2018-10-17-11-30-11-af0582b8-57cf-49e6-9c88.gz'
                   ]
                   )

    """
    s3 = boto3.resource('s3')

    for trailing_path in origin_paths:
        copy_source = {
            'Bucket': origin_bucket,
            'Key': 's3a://' + origin_stud + '/' + trailing_path
        }
        try:
            s3.meta.client.copy(copy_source,
                                destination_bucket,
                                's3a://' +
                                destination_stud + '/' + trailing_path
                                )
        except ClientError as copy_marker:
            raise FileNotFoundError(str(copy_marker))


def copy_and_cleanup(origin_bucket: str,
                     destination_bucket: str,
                     origin_stud: str,
                     destination_stud: str,
                     origin_paths: List[str]
                     ) -> None:
    """Move files from source S3 bucket to the destination bucket.

     Args:
        origin_bucket (str): Source S3 bucket.
        destination_bucket (str): Destination S3 bucket.
        origin_stud (str): Source S3 path
        destination_stud (str): Destination S3 path
        origin_paths (List[str]): Trailing source S3 paths under which
                                  destination files also will be written.

    Returns:
        None

    Calls:
        copy_files to copy files between buckets
        delete_files for source cleanup

    Sample Use:
        copy_and_cleanup('husqvarna-dl-landingzone-dev-amc',
                         'husqvarna-datalake',
                         'husqvarna-dl-landingzone-dev-amc/landing-zone/amc-connections',
                         'husqvarna-datalake/raw/amc/amc-connections',
                         [
                         '2018/10/17/12/amc-dev-connections-2-2018-10-17-12-30-10-bf0682b8-57cf-49e6-9c99.gz',
                         '2018/10/17/11/amc-dev-connections-1-2018-10-17-11-30-11-af0582b8-57cf-49e6-9c88.gz'
                         ]
                         )

    """
    copy_files(origin_bucket,
               destination_bucket,
               origin_stud,
               destination_stud,
               origin_paths,
               )

    obj_list = []
    for obj in origin_paths:
        obj_list.append('s3a://' + origin_stud + '/' + obj)

    delete_files(origin_bucket, obj_list)
