from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql.types import DoubleType, IntegerType

# Start Spark
spark = SparkSession.builder \
    .appName("Joor-Simple") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "bronze_access_key") \
    .config("spark.hadoop.fs.s3a.secret.key", "bronze_secret_key_2024") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Read and process data
df = spark.read.json("s3a://fashion-bronze-raw/joor_orders/2025/06/15/*.json").select("raw_api_data.*").select(
    coalesce(col("order_id"), lit("UNKNOWN")).alias("order_id"),
    coalesce(col("buyer"), lit("UNKNOWN")).alias("buyer"), 
    coalesce(col("price").cast(DoubleType()), lit(0.0)).alias("price"),
    coalesce(col("quantity").cast(IntegerType()), lit(0)).alias("quantity"),
    coalesce(col("sku"), lit("UNKNOWN")).alias("sku")
)

# Show results
df.show()
print(f"Processed {df.count()} records")

spark.stop()