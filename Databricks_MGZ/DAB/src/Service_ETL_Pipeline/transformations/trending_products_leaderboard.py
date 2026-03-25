from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Trending products and categories leaderboard - pre-joined flat table for dashboard",
    cluster_by=["categoryid"],
)
def trending_products_leaderboard():
    events = spark.read.table("retail_rocket.silver.transform_events")
    products = spark.read.table("retail_rocket.gold.gold_dim_product")
    categories = spark.read.table("retail_rocket.silver.silver_category_tree")

    item_interactions = events.groupBy("itemid", "event").agg(
        F.count("*").alias("interaction_count"),
        F.countDistinct("visitorid").alias("unique_visitors"),
        F.min("event_time").alias("first_interaction"),
        F.max("event_time").alias("last_interaction"),
    )

    enriched = (
        item_interactions.join(products, "itemid", "left")
        .join(	
            categories.select(
                F.col("categoryid"),
                F.col("parentid").alias("parent_categoryid"),
            ),
            "categoryid",
            "left",
        )
    )

    from pyspark.sql.window import Window

    rank_window = Window.partitionBy("event").orderBy(
        F.desc("interaction_count")
    )

    return enriched.withColumn("rank", F.row_number().over(rank_window))
