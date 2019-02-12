from pyspark.sql import SparkSession
from pyspark.sql.types import *

def load_json(spark_session,json_file_path,json_schema):
    json_df = spark_session.read.schema(json_schema).json(json_file_path)
    print("read successful")

if __name__ == "__main__":
    device_sessions_paths = 's3://husqvarna-dl-landingzone-live-smart-system/dev/location/2019/01/24/20/sg-dev-bd-location-ingest-Firehose-FIDOSXOLGGA-1-2019-01-24-20-22-51-dfd4a79d-7dce-4415-b1fa-3d7405f936cb.gz'

    nested_desiredconfiguration_field = StructType([
    StructField("config_type",StringType(),True)
    ,StructField("created_at",StringType(),True)
    ,StructField("relationships",ArrayType(StringType(),True),True)
    ,StructField("scheduled_events",ArrayType(StringType(),True),True)
    ,StructField("version_token",StringType(),True)])

    nested_latappconf_field = StructType([
    StructField("config_type",StringType(),True)
    ,StructField("created_at",StringType(),True)
    ,StructField("relationships",ArrayType(StringType(),True),True)
    ,StructField("scheduled_events",ArrayType(StringType(),True),True)
    ,StructField("version_token",StringType(),True)])

    nested_settings_field = StructType([
    StructField("id",StringType(),True)
    ,StructField("name",StringType(),True)
    ,StructField("value",StringType(),True)])

    nested_devices_field = StructType([
    StructField("adapter_device_ref",StringType(),True)
    ,StructField("adapter_location_ref",StringType(),True)
    ,StructField("adapter_protocol",StringType(),True)
    ,StructField("category",StringType(),True)
    ,StructField("description",StringType(),True)
    ,StructField("desired_configuration",nested_desiredconfiguration_field,True)
    ,StructField("externalConfigurationPossible",LongType(),True)
    ,StructField("id",StringType(),True)
    ,StructField("latest_applied_configuration",nested_latappconf_field,True)
    ,StructField("name",StringType(),True)
    ,StructField("scheduled_events",ArrayType(StringType(),True),True)
    ,StructField("settings",ArrayType(nested_settings_field),True)])

    nested_garden_image = StructType([
    StructField("device_id",StringType(),True)
    ,StructField("image_id",StringType(),True)
    ,StructField("mode",StringType(),True)])

    nested_geoposition_field = StructType([
    StructField("address",StringType(),True)
    ,StructField("city",StringType(),True)
    ,StructField("id",StringType(),True)
    ,StructField("latitude",DoubleType(),True)
    ,StructField("longitude",DoubleType(),True)
    ,StructField("time_zone",StringType(),True)])

    nested_data_field = StructType([
    StructField("__id__",StringType(),True)
    ,StructField("__version__",LongType(),True)
    ,StructField("adapter_location_ref",StringType(),True)
    ,StructField("adapter_protocol",StringType(),True)
    ,StructField("authorized_user_ids",ArrayType(StringType(),True),True)
    ,StructField("devices",ArrayType(nested_devices_field),True)
    ,StructField("garden_image",nested_garden_image,True)
    ,StructField("geo_position",nested_geoposition_field,True)
    ,StructField("name",StringType(),True)
    ,StructField("zones",ArrayType(StringType(),True),True)])

    location_schema = StructType([
    StructField("id",StringType(),True)
    ,StructField("operation",StringType(),True)
    ,StructField("timestamp",LongType(),True)
    ,StructField("sequenceNumber",StringType(),True)
    ,StructField("data",nested_data_field,True)])
    print("hi")

  #  load_json(SparkSession,device_sessions_paths,device_sessions_paths)
    try:
        pass
    except expression as identifier:
        pass

    session = SparkSession.builder.appName("myApp").getOrCreate()
    json_df = session.read.schema(location_schema).json(device_sessions_paths)
    print("read successful")
    session.stop