import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="retail_rocket.silver.silver_category_tree",
    comment="Dữ liệu danh mục đã làm sạch và xóa trùng lặp"
)
@dlt.expect_or_drop("valid_category", "categoryid IS NOT NULL")
def silver_category_tree():
    
    return (
        dlt.read("retail_rocket.bronze.bronze_category_tree")
        .dropDuplicates(["categoryid", "parentid"])
    )


dlt.create_streaming_table(
    name="retail_rocket.silver.silver_item_properties",
    comment="Trạng thái thuộc tính mới nhất của từng sản phẩm (SCD Type 1)",
    expect_all_or_drop={
        "valid_itemid": "itemid IS NOT NULL",
        "valid_property": "property IS NOT NULL",
        "valid_timestamp": "timestamp IS NOT NULL"
    }
)


dlt.apply_changes(
    target="retail_rocket.silver.silver_item_properties",
    source="retail_rocket.bronze.bronze_item_properties",
    keys=["itemid", "property"],
    sequence_by=col("timestamp"),
    except_column_list=["source_file", "ingestion_time"],
    stored_as_scd_type=1
)
