from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Real-time e-commerce conversion funnel: view → addtocart → transaction per 1-minute window",
    cluster_by=["window_start"],  # Tối ưu storage: sắp xếp dữ liệu theo thời gian window
)
def sales_funnel_summary_rt():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")  
        .table("retail_rocket.silver.transform_events")  
        .withWatermark("event_time", "1 minute")  
        .groupBy(
            F.window("event_time", "1 minute"), 
            "event"  
        )

        .agg(
            F.count("*").alias("event_count"),  
            F.approx_count_distinct("visitorid").alias("unique_visitors"),  
            F.approx_count_distinct("itemid").alias("unique_items"),  
            # Số item khác nhau
        )

        .select(
            F.col("window.start").alias("window_start"),  
            F.col("window.end").alias("window_end"),  

            "event",
            "event_count",
            "unique_visitors",
            "unique_items",

            F.when(F.col("event") == "view", 1)
            .when(F.col("event") == "addtocart", 2)
            .when(F.col("event") == "transaction", 3)
            .alias("funnel_stage"),  
            F.when(F.col("event") == "view", "1. View")
            .when(F.col("event") == "addtocart", "2. Add to Cart")
            .when(F.col("event") == "transaction", "3. Transaction")
            .alias("stage_label"),  
        )
    )
