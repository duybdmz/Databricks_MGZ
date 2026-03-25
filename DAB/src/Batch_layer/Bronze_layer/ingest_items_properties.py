import dlt
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType
)
from pyspark.sql.functions import current_timestamp, input_file_name


item_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("itemid", IntegerType(), True),
    StructField("property", StringType(), True),
    StructField("value", StringType(), True)
])

category_schema = StructType([
    StructField("categoryid", IntegerType(), True),
    StructField("parentid", IntegerType(), True)
])

bronze_path_item = "s3://data-batch-dbr/raw/item_properties/"
bronze_path_category = "s3://data-batch-dbr/raw/category_tree/"

@dlt.table(
    name="retail_rocket.bronze.bronze_item_properties", 
)
def ingest_data_properties():
    # Đọc file CSV từ S3 bằng Auto Loader 
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(item_schema)
        .load(bronze_path_item)
        .selectExpr("*", "_metadata.file_name as source_file", "current_timestamp() as ingestion_time")
    )
    
@dlt.table(
    name="retail_rocket.bronze.bronze_category_tree"
)
def bronze_category_tree():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(category_schema) 
        .load(bronze_path_category)
        .selectExpr("*", "_metadata.file_name as source_file", "current_timestamp() as ingestion_time")
    )