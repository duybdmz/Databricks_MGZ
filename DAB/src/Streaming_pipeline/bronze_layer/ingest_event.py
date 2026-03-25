import dlt
import os

env_path = "/Workspace/Users/duybd@mz.co.kr/Databricks_MGZ/DAB/src/.env"
with open(env_path) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ[key.strip()] = value.strip()

# Kinesis configuration
KINESIS_STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME")
KINESIS_REGION = os.environ.get("KINESIS_REGION")
STARTING_POSITION = os.environ.get("KINESIS_STARTING_POSITION")


awsAccessKey = os.environ.get("AWS_ACCESS_KEY")
awsSecretKey = os.environ.get("AWS_SECRET_KEY")

@dlt.table(
    name="retail_rocket.bronze.ingest_events",
    comment="đây là phần ingest từ kinesis",
)
def ingest_data_events():
    return (
        spark.readStream.format("kinesis")
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
            "current_timestamp() as ingestion_time",
        )
    )
