"""
The Generic module that does the following.

1. load_json: Get list of JSON files with schema and load into a dataframe
2. load_xml: Get list of XML files with schema and load into a dataframe
3. load_rdd: Get list of JSON/XML files and load into an RDD

"""

import logging
from typing import Dict, List, TypeVar

from big_data_getl.utils import json_to_spark_schema
from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

LOGGING = logging.getLogger(__name__)
JsonSchemaType = TypeVar('T', int, float, str, complex)


def load_json(spark: SparkSession,
              paths: List[str],
              json_schema: Dict[str, JsonSchemaType]) -> DataFrame:
    """Load json files and returns a DataFrame.

     Args:
        spark       (SparkSession): SparkSession from calling module.
        paths       (List[str]): List of paths to be loaded into DF.
        json_schema (Dict[str, T]): Schema defined in json format.

    Returns:
        DataFrame: Returns dataframe on successful load.

    Raises:
        FileNotFoundError: JSON file not found.

    """
    try:
        return (
            spark
            .read
            .schema(json_to_spark_schema(json_schema))
            .json(paths, multiLine=True)
        )
    except AnalysisException as spark_exception:
        LOGGING.error(str(spark_exception))
        raise FileNotFoundError(str(spark_exception))


def load_xml(spark: SparkSession,
             paths: List[str],
             json_schema: Dict[str, JsonSchemaType],
             row_tag: str
             ) -> DataFrame:
    """Load xml files and returns a DataFrame.

    Args:
        spark      (SparkSession): SparkSession from calling module.
        paths      (List[str]): List of paths which needs to be loaded into DF.
        schema     (Dict[str, T]): Schema defined in Spark schema type.
        row_rag    (str): Specifying the root tag for the xml document.

    Returns:
        DataFrame: Dataframe on successful load.

    """
    return (
        spark
        .read
        .schema(json_to_spark_schema(json_schema))
        .format('xml')
        .options(rowTag=row_tag)
        .load(','.join(paths))
    )


def load_rdd(spark: SparkSession, paths: List[str]) -> RDD:
    """Return an RDD for a textFile.

    The Generic module which load file into a RDD.
    Currently we handle XML and JSON.

    Args:
        spark (SparkSession): SparkSession from calling module.
        paths (List[str]): List of paths which needs to be loaded into DF.

    Returns:
        RDD: RDD containing Row of string.

    """
    return (
        spark
        .sparkContext
        .textFile(','.join(paths))
    )
