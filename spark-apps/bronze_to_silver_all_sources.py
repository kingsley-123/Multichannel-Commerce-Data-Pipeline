from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, when
from pyspark.sql.types import DoubleType, IntegerType

def clean_price(price_col):
    """Handle messy price data: $25.50, "30", 45.99"""
    return when(col(price_col).rlike("^\\$.*"), 
                col(price_col).substr(2, 50).cast(DoubleType())) \
           .otherwise(col(price_col).cast(DoubleType()))

def process_joor(spark):
    """Process Joor B2B orders"""
    stream_df = spark.readStream \
        .format("json") \
        .option("path", "s3a://fashion-bronze-raw/joor_orders/*/*/*.json") \
        .load()
    
    clean_df = stream_df \
        .select("raw_api_data.*") \
        .select(
            coalesce(col("order_id"), lit("")).alias("order_id"),
            coalesce(col("buyer"), lit("")).alias("buyer"),
            clean_price("price").alias("price"),
            coalesce(col("quantity").cast(IntegerType()), lit(0)).alias("quantity"),
            coalesce(col("sku"), lit("")).alias("sku"),
            coalesce(col("currency"), lit("USD")).alias("currency"),
            coalesce(col("country"), lit("")).alias("country"),
            coalesce(col("payment"), lit("")).alias("payment_method")
        )
    
    return clean_df.writeStream \
        .outputMode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://data-postgres:5432/fashion_silver") \
        .option("dbtable", "silver_joor_orders") \
        .option("user", "silver_user") \
        .option("password", "silver_pass_2024") \
        .option("driver", "org.postgresql.Driver") \
        .option("checkpointLocation", "/opt/spark-data/checkpoints/joor") \
        .trigger(processingTime='30 seconds')

def process_shopify(spark):
    """Process Shopify DTC orders"""
    stream_df = spark.readStream \
        .format("json") \
        .option("path", "s3a://fashion-bronze-raw/shopify_orders/*/*/*.json") \
        .load()
    
    clean_df = stream_df \
        .select("raw_api_data.*") \
        .select(
            coalesce(col("id").cast("string"), lit("")).alias("order_id"),
            coalesce(col("customer"), lit("")).alias("customer_name"),
            coalesce(col("email"), lit("")).alias("customer_email"),
            clean_price("price").alias("price"),
            coalesce(col("quantity").cast(IntegerType()), lit(0)).alias("quantity"),
            coalesce(col("sku"), lit("")).alias("sku"),
            coalesce(col("country"), lit("")).alias("country"),
            coalesce(col("status"), lit("")).alias("order_status")
        )
    
    return clean_df.writeStream \
        .outputMode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://data-postgres:5432/fashion_silver") \
        .option("dbtable", "silver_shopify_orders") \
        .option("user", "silver_user") \
        .option("password", "silver_pass_2024") \
        .option("driver", "org.postgresql.Driver") \
        .option("checkpointLocation", "/opt/spark-data/checkpoints/shopify") \
        .trigger(processingTime='30 seconds')

def process_tiktok(spark):
    """Process TikTok livestream orders"""
    stream_df = spark.readStream \
        .format("json") \
        .option("path", "s3a://fashion-bronze-raw/tiktok_orders/*/*/*.json") \
        .load()
    
    clean_df = stream_df \
        .select("raw_api_data.*") \
        .select(
            coalesce(col("order_id"), lit("")).alias("order_id"),
            coalesce(col("buyer"), lit("")).alias("buyer_name"),
            # TikTok uses price_cents
            coalesce(col("price_cents").cast(DoubleType()) / 100, lit(0.0)).alias("price"),
            coalesce(col("quantity").cast(IntegerType()), lit(0)).alias("quantity"),
            coalesce(col("sku"), lit("")).alias("sku"),
            coalesce(col("currency"), lit("USD")).alias("currency"),
            coalesce(col("country"), lit("")).alias("country"),
            coalesce(col("creator"), lit("")).alias("creator_handle"),
            coalesce(col("live_stream"), lit(False)).alias("is_livestream")
        )
    
    return clean_df.writeStream \
        .outputMode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://data-postgres:5432/fashion_silver") \
        .option("dbtable", "silver_tiktok_orders") \
        .option("user", "silver_user") \
        .option("password", "silver_pass_2024") \
        .option("driver", "org.postgresql.Driver") \
        .option("checkpointLocation", "/opt/spark-data/checkpoints/tiktok") \
        .trigger(processingTime='30 seconds')

def process_freight(spark):
    """Process freight data from DHL, UPS, EasyShip"""
    stream_df = spark.readStream \
        .format("json") \
        .option("path", "s3a://fashion-bronze-raw/freight_data/*/*/*.json") \
        .load()
    
    clean_df = stream_df \
        .select("raw_api_data.*") \
        .select(
            coalesce(col("tracking"), lit("")).alias("tracking_number"),
            coalesce(col("provider"), lit("")).alias("provider"),
            coalesce(col("cost").cast(DoubleType()), lit(0.0)).alias("cost"),
            coalesce(col("currency"), lit("USD")).alias("currency"),
            coalesce(col("status"), lit("")).alias("status"),
            coalesce(col("weight"), lit("")).alias("weight"),
            coalesce(col("order_ref"), lit("")).alias("order_reference")
        )
    
    return clean_df.writeStream \
        .outputMode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://data-postgres:5432/fashion_silver") \
        .option("dbtable", "silver_freight_data") \
        .option("user", "silver_user") \
        .option("password", "silver_pass_2024") \
        .option("driver", "org.postgresql.Driver") \
        .option("checkpointLocation", "/opt/spark-data/checkpoints/freight") \
        .trigger(processingTime='30 seconds')

def process_gsheets(spark):
    """Process Google Sheets data (Production Tracker, Sales Tracker)"""
    stream_df = spark.readStream \
        .format("json") \
        .option("path", "s3a://fashion-bronze-raw/gsheets_data/*/*/*.json") \
        .load()
    
    # Google Sheets data is more complex - just store raw for now
    clean_df = stream_df \
        .select("raw_api_data.*") \
        .select(
            coalesce(col("sheet_name"), lit("")).alias("sheet_name"),
            col("sheet_data").alias("raw_data")
        )
    
    return clean_df.writeStream \
        .outputMode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://data-postgres:5432/fashion_silver") \
        .option("dbtable", "silver_gsheets_data") \
        .option("user", "silver_user") \
        .option("password", "silver_pass_2024") \
        .option("driver", "org.postgresql.Driver") \
        .option("checkpointLocation", "/opt/spark-data/checkpoints/gsheets") \
        .trigger(processingTime='30 seconds')

def main():
    # Start Spark
    spark = SparkSession.builder \
        .appName("Fashion-All-Sources-Streaming") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "bronze_access_key") \
        .config("spark.hadoop.fs.s3a.secret.key", "bronze_secret_key_2024") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    
    print("Starting all fashion data sources streaming...")
    
    # Start all streaming queries
    queries = []
    
    try:
        print("Starting Joor B2B processing...")
        queries.append(process_joor(spark).start())
        
        print("Starting Shopify DTC processing...")
        queries.append(process_shopify(spark).start())

        print("Starting TikTok livestream processing...")
        queries.append(process_tiktok(spark).start())

        print("Starting freight data processing...")
        queries.append(process_freight(spark).start())

        print("Starting Google Sheets processing...")
        queries.append(process_gsheets(spark).start())
        
        print("All sources streaming! Processing new data every 30 seconds...")
        
        # Wait for all queries to finish (runs forever)
        for query in queries:
            query.awaitTermination()
            
    except Exception as e:
        print(f"❌ Error: {e}")
        for query in queries:
            query.stop()
        raise
    finally:
        spark.stop()

main()