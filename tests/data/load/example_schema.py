"""A Helper that can supply schema of various types to test load GETL."""

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    BooleanType,
    IntegerType
)
import json


def create_json_schema(schema_type: str = 'valid') -> dict:
    """Return a json schema."""
    mapper = {
        'valid': create_valid_schema,
        'missing': schema_missing_field,
        'extra': schema_extra_field,
        'castable': schema_different_castable_data_field,
        'uncastable': schema_different_uncastable_data_field,
        'missing_non_nullable': schema_extra_missing_non_nullable_field
    }

    return json.loads(mapper[schema_type]().json())


def create_valid_schema() -> StructType:
    """Return a spark schema."""
    return (
        StructType([
            StructField("name", StringType(), True),
            StructField("empid", IntegerType(), True),
            StructField("happy", BooleanType(), True)])
    )


def schema_missing_field() -> StructType:
    """Return an sample spark schema with a missing field."""
    return (
        StructType([
            StructField("name", IntegerType(), True),
            StructField("empid", StringType(), True)])
    )


def schema_extra_field() -> StructType:
    """Return an sample spark schema with an extra field defined."""
    return (
        StructType([
            StructField("name", StringType(), True),
            StructField("empid", IntegerType(), True),
            StructField("happy", BooleanType(), True),
            StructField("extra", BooleanType(), True)])
    )


def schema_extra_missing_non_nullable_field() -> StructType:
    """Return an sample spark schema with an extra field defined."""
    return (
        StructType([
            StructField("name", StringType(), True),
            StructField("empid", IntegerType(), True),
            StructField("happy", BooleanType(), True),
            StructField("extra", BooleanType(), False)])
    )


def schema_different_castable_data_field() -> StructType:
    """Return an sample spark schema with castable change in datatype."""
    return (
        StructType([
            StructField("name", StringType(), True),
            StructField("empid", StringType(), True),
            StructField("happy", BooleanType(), True)])
    )


def schema_different_uncastable_data_field() -> StructType:
    """Return an sample spark schema with uncastable change in datatype."""
    return (
        StructType([
            StructField("name", IntegerType(), True),
            StructField("empid", StringType(), True),
            StructField("happy", BooleanType(), True)])
    )
