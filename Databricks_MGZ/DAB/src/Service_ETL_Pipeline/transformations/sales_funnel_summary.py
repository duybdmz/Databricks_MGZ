from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="E-commerce conversion funnel summary: view → addtocart → transaction with drop-off rates",
    cluster_by=["funnel_stage"],
)
def sales_funnel_summary():
    events = spark.read.table("retail_rocket.silver.transform_events")
    funnel = (
        events.groupBy("event")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("visitorid").alias("unique_visitors"),
            F.countDistinct("itemid").alias("unique_items"),
        )
        .withColumn(
            "funnel_stage",
            F.when(F.col("event") == "view", 1)
            .when(F.col("event") == "addtocart", 2)
            .when(F.col("event") == "transaction", 3),
        )
        .withColumn(
            "stage_label",
            F.when(F.col("event") == "view", "1. View")
            .when(F.col("event") == "addtocart", "2. Add to Cart")
            .when(F.col("event") == "transaction", "3. Transaction"),
        )
    )

    total_views = events.filter(F.col("event") == "view").count()

    funnel_with_rates = funnel.withColumn(
        "conversion_rate_from_view",
        F.when(
            F.lit(total_views) > 0,
            F.round(F.col("event_count") / F.lit(total_views) * 100, 2),
        ).otherwise(0),
    )

    return funnel_with_rates.orderBy("funnel_stage")
