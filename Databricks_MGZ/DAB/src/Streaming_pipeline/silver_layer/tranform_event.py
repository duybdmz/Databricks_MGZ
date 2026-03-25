import dlt
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType

events_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("visitorid", IntegerType(), True),
    StructField("event", StringType(), True),
    StructField("itemid", IntegerType(), True),
    StructField("transactionid", IntegerType(), True)
])


@dlt.table(
    name="retail_rocket.silver.transform_events",
    comment="Lớp Silver: Dữ liệu sự kiện thời gian thực đã bóc tách JSON, làm sạch và xóa trùng lặp."
)
# Cổng chất lượng dữ liệu (Expectations): Tự động loại bỏ dữ liệu rác
@dlt.expect_or_drop("valid_visitor_and_item", "visitorid IS NOT NULL AND itemid IS NOT NULL")
@dlt.expect_or_drop("valid_event_type", "event IN ('view', 'addtocart', 'transaction')")
def silver_events():
    df_bronze = dlt.read_stream("retail_rocket.bronze.ingest_events")
    df_parsed = df_bronze.withColumn(
        "parsed_data", 
        from_json(col("raw_payload"), events_schema)
    ).select(
        "parsed_data.*", 
        "approximateArrivalTimestamp", 
        "ingestion_time"               
    )
    
    df_watermarked = df_parsed.withColumn(
        "event_time", 
        (col("timestamp") / 1000).cast("timestamp")
    ).withWatermark("event_time", "10 minutes") 
    

    df_deduplicated = df_watermarked.dropDuplicatesWithinWatermark(
        ["visitorid", "itemid", "event", "event_time"]
    )
    
    return df_deduplicated