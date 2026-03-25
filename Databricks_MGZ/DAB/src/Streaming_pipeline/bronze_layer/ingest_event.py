import dlt

# Kinesis configuration
KINESIS_STREAM_NAME = "databrick-lambda-architecture-events"
KINESIS_REGION = "us-east-1"
STARTING_POSITION = "trim_horizon"
awsAccessKey = "AKIATOZJ4IAR2X4TCWV4"
awsSecretKey = "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"
# AWS credentials (nên dùng Databricks Secrets trong production)
# awsAccessKey = dbutils.secrets.get(scope="aws", key="access_key")
# awsSecretKey = dbutils.secrets.get(scope="aws", key="secret_key")


# Đọc từ Kinesis stream
@dlt.table(
    name = "retail_rocket.bronze.ingest_events" , 
    comment = "đây là phần ingest từ kinesis"
)
def ingest_data_events():
    return (
    spark.readStream
    .format("kinesis")
    .option("streamName", KINESIS_STREAM_NAME)
    .option("region", KINESIS_REGION)
    .option("initialPosition", STARTING_POSITION)
    .option("awsAccessKey", awsAccessKey)
    .option("awsSecretKey", awsSecretKey)
    .load()
    .selectExpr(
        "cast(data as string) as raw_payload", 
        "partitionKey",                      
        "sequenceNumber",                      
        "approximateArrivalTimestamp",         
        "current_timestamp() as ingestion_time"
    )
)
