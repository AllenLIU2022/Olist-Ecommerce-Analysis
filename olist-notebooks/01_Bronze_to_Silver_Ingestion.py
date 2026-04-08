#!/usr/bin/env python
# coding: utf-8

# ## 01_Bronze_to_Silver_Ingestion
# 
# 
# 

# In[18]:


# 定义数据路径 
storage_account = "liufei2026storage"
container = "data"
file_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/01-bronze/olist_ecommerce/olist_orders_dataset.csv"

# 使用 PySpark 读取 CSV
df_orders = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# 展示前 5 行看看
display(df_orders.limit(5))


# In[19]:


from pyspark.sql.functions import col, to_timestamp

# 列表展示需要转换的所有时间列
timestamp_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

# 循环转换格式：从 String 转为 Timestamp
df_silver_orders = df_orders
for c in timestamp_cols:
    df_silver_orders = df_silver_orders.withColumn(c, to_timestamp(col(c), "yyyy-MM-dd HH:mm:ss"))

# 验证结果：看 Schema 是否变成了 timestamp
df_silver_orders.printSchema()
display(df_silver_orders.limit(5))


# In[20]:


# 定义 Silver 层存储路径
silver_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/02-silver/olist_ecommerce/orders"

# 写入数据（使用 overwrite 模式，方便你反复运行实验）
df_silver_orders.write.mode("overwrite").parquet(silver_path)

print("✅ 成功将数据持久化到 Silver 层！")


# In[22]:


from pyspark.sql.functions import col, to_timestamp, cast

def process_silver_v2(file_name, target_folder, date_cols=[], custom_transform=None):
    # 1. 基础读取
    path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/01-bronze/olist_ecommerce/{file_name}.csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    
    # 2. 通用日期转换
    for col_name in date_cols:
        df = df.withColumn(col_name, to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss"))
    
    # 3. 执行特殊逻辑 (如果传了自定义转换函数)
    if custom_transform:
        df = custom_transform(df)
    
    # 4. 写入 Silver
    output_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/02-silver/olist_ecommerce/{target_folder}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ 表 {target_folder} 清洗并存入 Silver")


# --- 针对不同表的特殊“配方” ---

# 1. 客户表：强制邮编为字符串
def transform_customers(df):
    return df.withColumn("customer_zip_code_prefix", col("customer_zip_code_prefix").cast("string"))

# 2. 产品表：填充缺失值
def transform_products(df):
    return df.fillna("Unknown", subset=["product_category_name"])

#3. 地理表：
def tansform_geograph(df):
    return df.withColumn("geolocation_zip_code_prefix",col("geolocation_zip_code_prefix").cast("string")).dropDuplicates(["geolocation_zip_code_prefix"])

#4.销售员表：
def tansform_seller(df):
    return df.withColumn("seller_zip_code_prefix",col("seller_zip_code_prefix").cast("string"))

# --- 执行 ---
process_silver_v2("olist_customers_dataset", "customers", custom_transform=transform_customers)
process_silver_v2("olist_products_dataset", "products", custom_transform=transform_products)
process_silver_v2("olist_geolocation_dataset", "geolocation", custom_transform=tansform_geograph)
process_silver_v2("olist_order_items_dataset", "items", date_cols=["shipping_limit_date"])
process_silver_v2("olist_order_payments_dataset", "payments")
process_silver_v2("olist_order_reviews_dataset", "reviews", date_cols=["review_creation_date", "review_answer_timestamp"])
process_silver_v2("olist_sellers_dataset", "sellers", custom_transform=tansform_seller)
process_silver_v2("product_category_name_translation", "category")



# 
