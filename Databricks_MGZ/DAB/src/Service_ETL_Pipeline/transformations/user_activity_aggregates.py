from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Real-time user activity aggregates per 5-minute tumbling window for traffic monitoring",
    cluster_by=["window_start"],
)
def user_activity_aggregates():
    return (
        spark.readStream.table("retail_rocket.silver.transform_events")
        .withWatermark("event_time", "10 minutes")
        .groupBy(F.window("event_time", "5 minutes"), "event")
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("visitorid").alias("unique_visitors"),
            F.approx_count_distinct("itemid").alias("unique_items"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event",
            "event_count",
            "unique_visitors",
            "unique_items",
        )
    )
