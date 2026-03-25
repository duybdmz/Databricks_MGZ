import dlt
from pyspark.sql.functions import col


@dlt.table(
    name="retail_rocket.gold.gold_dim_product",
    comment="Bảng Dimension Sản phẩm chứa thông tin danh mục, dùng để join với luồng sự kiện (events)"
)
def gold_dim_product():
    # Đọc dữ liệu sạch từ lớp Silver
    df_props = dlt.read("retail_rocket.silver.silver_item_properties")
    df_cats = dlt.read("retail_rocket.silver.silver_category_tree")
    
    # Giả sử chúng ta muốn lọc riêng thuộc tính 'categoryid' của từng sản phẩm
    df_item_category = df_props.filter(col("property") == "categoryid").select(
        col("itemid"),
        col("value").cast("integer").alias("categoryid")
    )
    
    # Kết hợp (Join) với cây danh mục để lấy thông tin parentid
    df_dim_product = df_item_category.join(df_cats, on="categoryid", how="left")
    
    return df_dim_product
