from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, max as spark_max
from pyspark.sql.types import DoubleType, IntegerType
import os

# Simple checkpoint
CHECKPOINT_FILE = "/opt/spark-data/all_sources_checkpoint.txt"

def get_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        return open(CHECKPOINT_FILE).read().strip()
    return "1900-01-01"

def save_checkpoint(timestamp):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    open(CHECKPOINT_FILE, 'w').write(timestamp)

def process_source(spark, source_name, table_name):
    """Generic function to process any source"""
    print(f"Processing {source_name}...")
    
    checkpoint = get_checkpoint()
    
    # Read all data for this source
    df = spark.read.json(f"s3a://fashion-bronze-raw/{source_name}/*/*/*/*.json") \
        .select("raw_api_data.*", "kafka_metadata.bronze_timestamp") \
        .filter(col("bronze_timestamp") > checkpoint)
    
    if df.count() == 0:
        print(f"No new {source_name} data")
        return None
    
    # Basic cleaning (adjust per source)
    if source_name == "joor_orders":
        clean_df = df.select(
            coalesce(col("order_id"), lit("")).alias("order_id"),
            coalesce(col("buyer"), lit("")).alias("buyer"),
            coalesce(col("price").cast(DoubleType()), lit(0.0)).alias("price"),
            coalesce(col("quantity").cast(IntegerType()), lit(0)).alias("quantity"),
            coalesce(col("sku"), lit("")).alias("sku"),
            col("bronze_timestamp")
        )
    elif source_name == "shopify_orders":
        clean_df = df.select(
            coalesce(col("id").cast("string"), lit("")).alias("order_id"),
            coalesce(col("customer"), lit("")).alias("customer_name"),
            coalesce(col("price").cast(DoubleType()), lit(0.0)).alias("price"),
            coalesce(col("quantity").cast(IntegerType()), lit(0)).alias("quantity"),
            coalesce(col("sku"), lit("")).alias("sku"),
            col("bronze_timestamp")
        )
    elif source_name == "tiktok_orders":
        clean_df = df.select(
            coalesce(col("order_id"), lit("")).alias("order_id"),
            coalesce(col("buyer"), lit("")).alias("buyer_name"),
            coalesce(col("price_cents").cast(DoubleType()) / 100, lit(0.0)).alias("price"),
            coalesce(col("quantity").cast(IntegerType()), lit(0)).alias("quantity"),
            coalesce(col("sku"), lit("")).alias("sku"),
            col("bronze_timestamp")
        )
    elif source_name == "freight_data":
        clean_df = df.select(
            coalesce(col("tracking"), lit("")).alias("tracking_number"),
            coalesce(col("provider"), lit("")).alias("provider"),
            coalesce(col("cost").cast(DoubleType()), lit(0.0)).alias("cost"),
            coalesce(col("order_ref"), lit("")).alias("order_reference"),
            col("bronze_timestamp")
        )
    else:  # gsheets_data
        clean_df = df.select(
            coalesce(col("sheet_name"), lit("")).alias("sheet_name"),
            col("sheet_data").alias("raw_data"),
            col("bronze_timestamp")
        )
    
    # Save to PostgreSQL
    clean_df.drop("bronze_timestamp").write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://data-postgres:5432/fashion_silver") \
        .option("dbtable", table_name) \
        .option("user", "silver_user") \
        .option("password", "silver_pass_2024") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    print(f"Processed {clean_df.count()} {source_name} records")
    return clean_df

# Start Spark
spark = SparkSession.builder \
    .appName("Fashion-All-Sources-Batch") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "bronze_access_key") \
    .config("spark.hadoop.fs.s3a.secret.key", "bronze_secret_key_2024") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print("Processing all fashion data sources...")

sources = [
    ("joor_orders", "silver_joor_orders"),
    ("shopify_orders", "silver_shopify_orders"), 
    ("tiktok_orders", "silver_tiktok_orders"),
    ("freight_data", "silver_freight_data"),
    ("gsheets_data", "silver_gsheets_data")
]

all_timestamps = []

for source_name, table_name in sources:
    try:
        result_df = process_source(spark, source_name, table_name)
        if result_df:
            latest = result_df.select(spark_max("bronze_timestamp")).collect()[0][0]
            all_timestamps.append(latest)
    except Exception as e:
        print(f"Error processing {source_name}: {e}")

# Update checkpoint with latest timestamp across all sources
if all_timestamps:
    newest_timestamp = max(all_timestamps)
    save_checkpoint(newest_timestamp)
    print(f"Updated checkpoint to: {newest_timestamp}")

spark.stop()
print("All sources processed")