from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Real-time user activity aggregates per 1-minute tumbling window for traffic monitoring",
    cluster_by=["window_start"],  # Sắp xếp dữ liệu vật lý theo window_start để query nhanh hơn
)
def user_activity_aggregates():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")  # Bỏ qua update/delete, chỉ đọc insert mới
        .table("retail_rocket.silver.transform_events")  # Đọc Delta table dạng streaming (incremental)

        .withWatermark("event_time", "1 minute")  
        # Định nghĩa độ trễ tối đa của data (late data)
        # Giúp Spark biết khi nào đóng window và giải phóng state

        .groupBy(
            F.window("event_time", "1 minute"),  # Gom dữ liệu theo cửa sổ 1 phút
            "event"  # Gom thêm theo loại event
        )

        .agg(
            F.count("*").alias("event_count"),  # Tổng số event
            F.approx_count_distinct("visitorid").alias("unique_visitors"),  # Unique visitor (approx)
            F.approx_count_distinct("itemid").alias("unique_items"),  # Unique item (approx)
        )

        .select(
            F.col("window.start").alias("window_start"),  # Start của window
            F.col("window.end").alias("window_end"),      # End của window
            "event",
            "event_count",
            "unique_visitors",
            "unique_items",
        )
    )
