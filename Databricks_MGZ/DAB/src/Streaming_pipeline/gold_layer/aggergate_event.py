import dlt
from pyspark.sql.functions import col, window, count, sum, when


@dlt.table(
    name="retail_rocket.gold.speed_view_item_performance"
)
def speed_view_item_performance():
    
    events_stream = spark.readStream.table("retail_rocket.silver.transform_events")
    return (
        events_stream
        .withWatermark("event_time", "15 minutes") 
        .groupBy(
            window(col("event_time"), "5 minutes"), 
            col("itemid")
        )
        .agg(
            count("*").alias("realtime_interactions"),
            sum(when(col("event") == "transaction", 1).otherwise(0)).alias("realtime_purchases")
        )
    )