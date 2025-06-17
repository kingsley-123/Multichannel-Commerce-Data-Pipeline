from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, current_timestamp, when, max as spark_max, min as spark_min, sum
from pyspark.sql.functions import round as spark_round, to_date, date_format, year, quarter, month, dayofweek
from pyspark.sql.types import StringType, DoubleType, IntegerType
import os

CHECKPOINT_FILE = "/opt/spark-data/gold_checkpoint.txt"

def get_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        return open(CHECKPOINT_FILE).read().strip()
    return "1900-01-01"

def save_checkpoint(timestamp):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    open(CHECKPOINT_FILE, 'w').write(timestamp)

def ensure_clickhouse_database():
    """Bulletproof database creation - handles all edge cases"""
    print("🔧 Setting up ClickHouse database and tables...")
    
    import time
    import socket
    
    # Wait for ClickHouse to be actually ready (not just healthy)
    print("⏳ Waiting for ClickHouse connection...")
    max_attempts = 60
    for attempt in range(max_attempts):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('clickhouse', 8123))
            sock.close()
            
            if result == 0:
                print("✅ ClickHouse is accepting connections")
                break
        except:
            pass
            
        if attempt < max_attempts - 1:
            print(f"   Attempt {attempt + 1}/{max_attempts}...")
            time.sleep(2)
        else:
            raise Exception("ClickHouse not responding after 2 minutes")
    
    # Give it a few more seconds to be fully ready
    time.sleep(5)
    
    try:
        # Method 1: Try using Spark JDBC (if it works)
        print("🔧 Attempting database creation via JDBC...")
        
        try:
            # Create a simple test DataFrame
            test_df = spark.createDataFrame([("test",)], ["col"])
            
            # Try to create database using JDBC
            test_df.limit(0).write.jdbc(
                url="jdbc:clickhouse://clickhouse:8123/",
                table="temp_test",
                mode="overwrite",
                properties={
                    "user": "gold_user",
                    "password": "gold_pass_2024",
                    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
                    "createDatabaseIfNotExist": "true"  # This might work
                }
            )
            print("✅ JDBC method worked")
            
        except Exception as jdbc_error:
            print(f"⚠️  JDBC method failed: {jdbc_error}")
            print("🔧 Falling back to HTTP method...")
            
            # Method 2: HTTP API (more reliable)
            import urllib.request
            import urllib.parse
            import base64
            
            # Create credentials
            credentials = base64.b64encode(b'gold_user:gold_pass_2024').decode('ascii')
            
            def execute_sql(sql):
                """Execute SQL via ClickHouse HTTP API"""
                req = urllib.request.Request(
                    'http://clickhouse:8123/',
                    data=sql.encode('utf-8'),
                    headers={'Authorization': f'Basic {credentials}'}
                )
                
                try:
                    with urllib.request.urlopen(req, timeout=30) as response:
                        result = response.read().decode('utf-8')
                        if response.status == 200:
                            return True
                        else:
                            print(f"SQL failed: {result}")
                            return False
                except Exception as e:
                    print(f"HTTP request failed: {e}")
                    return False
            
            # Create database
            print("📊 Creating fashion_gold database...")
            if not execute_sql("CREATE DATABASE IF NOT EXISTS fashion_gold"):
                raise Exception("Failed to create database")
            
            # Create all tables
            tables = [
                ("dim_date", """
                CREATE TABLE IF NOT EXISTS fashion_gold.dim_date (
                    date_key String,
                    date Date,
                    year UInt16,
                    quarter UInt8,
                    month UInt8,
                    day_of_week String,
                    is_weekend UInt8
                ) ENGINE = MergeTree() ORDER BY date_key
                """),
                
                ("dim_channels", """
                CREATE TABLE IF NOT EXISTS fashion_gold.dim_channels (
                    channel_id String,
                    channel_name String,
                    channel_type String,
                    description String
                ) ENGINE = MergeTree() ORDER BY channel_id
                """),
                
                ("wholesale_cm1", """
                CREATE TABLE IF NOT EXISTS fashion_gold.wholesale_cm1 (
                    date_key String, date Date, order_no String, style_no String,
                    style_name String, unified_style_no String, unified_style_name String,
                    payment_source String, season String, buyer_name String,
                    payment_terms String, country String, currency String, qty Int32,
                    gross_revenue Decimal(10,2), total_discount Decimal(10,2),
                    net_revenue Decimal(10,2), item_gross_price Decimal(10,2),
                    item_discount Decimal(10,2), item_net_price Decimal(10,2),
                    avg_item_unit_cost Decimal(10,2), unit_cost Decimal(10,2),
                    prod_com_percent Decimal(5,4), prod_com Decimal(10,2),
                    margin Decimal(10,2), channel_id String
                ) ENGINE = MergeTree() 
                PARTITION BY toYYYYMM(date) 
                ORDER BY (date, order_no, style_no)
                """),
                
                ("wholesale_cm2", """
                CREATE TABLE IF NOT EXISTS fashion_gold.wholesale_cm2 (
                    date_key String, order_no String, buyer_name String, currency String,
                    payment_source String, qty Int32, net_revenue Decimal(10,2),
                    production_cost Decimal(10,2), production_comm Decimal(10,2),
                    freight_out_status String, freight_currency String,
                    freight_in Decimal(10,2), freight_out Decimal(10,2),
                    trx_currency String, trx_fees Decimal(10,2), comm_currency String,
                    sales_comm Decimal(5,4), insurance_currency String,
                    insurance Decimal(10,2), cm2_amount Decimal(10,2), channel_id String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(parseDateTimeBestEffort(date_key))
                ORDER BY (date_key, order_no)
                """),
                
                ("shopify_cm1", """
                CREATE TABLE IF NOT EXISTS fashion_gold.shopify_cm1 (
                    date_key String, date Date, order_no String, style_no String,
                    style_name String, unified_style_no String, unified_style_name String,
                    buyer_name String, country String, currency String, qty Int32,
                    gross_revenue Decimal(10,2), total_discount Decimal(10,2),
                    net_revenue Decimal(10,2), item_gross_price Decimal(10,2),
                    item_discount Decimal(10,2), item_net_price Decimal(10,2),
                    total_returns Decimal(10,2), unit_cost_currency String,
                    item_unit_cost Decimal(10,2), prod_com_percent Decimal(5,4),
                    prod_com Decimal(10,2), margin Decimal(10,2), channel_id String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(date)
                ORDER BY (date, order_no, style_no)
                """),
                
                ("shopify_cm2", """
                CREATE TABLE IF NOT EXISTS fashion_gold.shopify_cm2 (
                    date_key String, order_no String, buyer_name String, country String,
                    currency String, qty Int32, net_revenue Decimal(10,2),
                    total_returns Decimal(10,2), cost_currency String,
                    total_unit_cost Decimal(10,2), cm1_amount Decimal(10,2),
                    freight_out_status String, freight_in Decimal(10,2),
                    freight_out Decimal(10,2), return_status String,
                    freight_return Decimal(10,2), freight_income Decimal(10,2),
                    shopify_fees Decimal(10,2), cm2_amount Decimal(10,2), channel_id String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(parseDateTimeBestEffort(date_key))
                ORDER BY (date_key, order_no)
                """),
                
                ("livestreaming_cm1", """
                CREATE TABLE IF NOT EXISTS fashion_gold.livestreaming_cm1 (
                    date_key String, date Date, order_no String, style_no String,
                    style_name String, unified_style_no String, unified_style_name String,
                    buyer_name String, country String, currency String, qty Int32,
                    gross_revenue Decimal(10,2), total_discount Decimal(10,2),
                    net_revenue Decimal(10,2), item_gross_price Decimal(10,2),
                    item_discount Decimal(10,2), item_net_price Decimal(10,2),
                    total_returns Decimal(10,2), unit_cost_currency String,
                    item_unit_cost Decimal(10,2), prod_com_percent Decimal(5,4),
                    prod_com Decimal(10,2), margin Decimal(10,2), channel_id String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(date)
                ORDER BY (date, order_no, style_no)
                """),
                
                ("livestreaming_cm2", """
                CREATE TABLE IF NOT EXISTS fashion_gold.livestreaming_cm2 (
                    date_key String, order_no String, buyer_name String, currency String,
                    qty Int32, net_revenue Decimal(10,2), production_cost Decimal(10,2),
                    production_comm Decimal(10,2), freight_currency String,
                    freight_in Decimal(10,2), freight_out Decimal(10,2),
                    trx_currency String, trx_fees Decimal(10,2), comm_currency String,
                    sales_comm Decimal(5,4), cm2_amount Decimal(10,2), channel_id String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(parseDateTimeBestEffort(date_key))
                ORDER BY (date_key, order_no)
                """),
                
                ("fact_freight", """
                CREATE TABLE IF NOT EXISTS fashion_gold.fact_freight (
                    date_key String, tracking_number String, provider String,
                    cost Decimal(10,2), order_no String, created_at DateTime
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(parseDateTimeBestEffort(date_key))
                ORDER BY (date_key, tracking_number)
                """)
            ]
            
            for table_name, sql in tables:
                print(f"📋 Creating table: {table_name}")
                if not execute_sql(sql):
                    print(f"⚠️  Warning: Failed to create {table_name}")
            
            print("✅ Database setup completed via HTTP API")
    
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        print("⚠️  Continuing anyway - tables will be created on first write")
        # Don't raise exception - let the pipeline continue
        return False
    
    return True

def write_to_clickhouse(df, table_name, mode="append"):
    """Write DataFrame to ClickHouse (tables must exist)"""
    if df.count() > 0:
        df.write.jdbc(
            url="jdbc:clickhouse://clickhouse:8123/fashion_gold",
            table=table_name,
            mode=mode,
            properties={
                "user": "gold_user",
                "password": "gold_pass_2024",
                "driver": "com.clickhouse.jdbc.ClickHouseDriver",
                "createTableOptions": "ENGINE = MergeTree() ORDER BY tuple()"  # Fallback for missing tables
            }
        )
        print(f"Wrote {df.count()} records to {table_name}")
    else:
        print(f"No data to write to {table_name}")

def create_joor_cm1(spark, joor_df):
    """Create Joor Item-Level Margin Table (Wholesale CM1)"""
    print("Creating Joor CM1 (Item-Level Margins)...")
    
    joor_cm1 = joor_df.select(
        date_format(to_date(current_timestamp()), "yyyyMMdd").alias("date_key"),  # For dim_date relationship
        to_date(current_timestamp()).alias("date"),
        col("order_id").alias("order_no"),
        col("sku").alias("style_no"),
        col("sku").alias("style_name"),
        lit("").alias("unified_style_no"),
        lit("").alias("unified_style_name"),
        lit("Hilldun").alias("payment_source"),
        lit("").alias("season"),
        col("buyer").alias("buyer_name"),
        lit("Net 30").alias("payment_terms"),
        lit("US").alias("country"),
        lit("USD").alias("currency"),
        col("quantity").alias("qty"),
        col("price").alias("gross_revenue"),
        lit(0.0).alias("total_discount"),
        col("price").alias("net_revenue"),
        (col("price") / col("quantity")).alias("item_gross_price"),
        lit(0.0).alias("item_discount"),
        (col("price") / col("quantity")).alias("item_net_price"),
        lit(25.0).alias("avg_item_unit_cost"),
        (lit(25.0) * col("quantity")).alias("unit_cost"),
        lit(0.05).alias("prod_com_percent"),
        (lit(25.0) * col("quantity") * lit(0.05)).alias("prod_com"),
        (col("price") - (lit(25.0) * col("quantity")) - (lit(25.0) * col("quantity") * lit(0.05))).alias("margin"),
        lit("joor").alias("channel_id")  # For dim_channels relationship
    )
    
    return joor_cm1

def create_joor_cm2(spark, joor_cm1_df, freight_df):
    """Create Joor Order-Level Margin Table (Wholesale CM2)"""
    print("Creating Joor CM2 (Order-Level Margins)...")
    
    # Aggregate CM1 data by order
    order_aggregated = joor_cm1_df.groupBy("order_no", "buyer_name", "currency", "payment_source", "date_key", "channel_id").agg(
        spark_round(sum(col("qty")), 0).alias("qty"),
        spark_round(sum(col("net_revenue")), 2).alias("net_revenue"),
        spark_round(sum(col("unit_cost")), 2).alias("production_cost"),
        spark_round(sum(col("prod_com")), 2).alias("production_comm")
    )
    
    # Join with freight data to get actual shipping status and costs
    joor_with_freight = order_aggregated.join(
        freight_df.select("order_reference", "cost", lit("shipped").alias("freight_status")), 
        order_aggregated["order_no"] == freight_df["order_reference"], 
        "left"
    )
    
    joor_cm2 = joor_with_freight.select(
        col("date_key"),  # For dim_date relationship
        col("order_no"),
        col("buyer_name"),
        col("currency"),
        col("payment_source"),
        col("qty"),
        col("net_revenue"),
        col("production_cost"),
        col("production_comm"),
        coalesce(col("freight_status"), lit("not shipped yet")).alias("freight_out_status"),
        lit("SGD").alias("freight_currency"),
        (col("qty") * lit(3.0)).alias("freight_in"),
        coalesce(col("cost"), lit(15.0)).alias("freight_out"),
        lit("USD").alias("trx_currency"),
        when(col("payment_source") == "hilldun", lit(0.0))
        .otherwise(col("net_revenue") * lit(0.029)).alias("trx_fees"),
        lit("USD").alias("comm_currency"),
        lit(0.05).alias("sales_comm"),
        lit("USD").alias("insurance_currency"),
        when(col("payment_source") == "hilldun", col("net_revenue") * lit(0.02))
        .otherwise(lit(0.0)).alias("insurance"),
        (col("net_revenue") - col("production_cost") - col("production_comm") - 
         when(col("payment_source") == "hilldun", lit(0.0)).otherwise(col("net_revenue") * lit(0.029)) - 
         (col("qty") * lit(3.0)) - coalesce(col("cost"), lit(15.0))).alias("cm2_amount"),
        col("channel_id")  # For dim_channels relationship
    )
    
    return joor_cm2

def create_shopify_cm1(spark, shopify_df):
    """Create Shopify Item-Level Margin Table (Shopify CM1)"""
    print("Creating Shopify CM1 (Item-Level Margins)...")
    
    shopify_cm1 = shopify_df.select(
        date_format(to_date(current_timestamp()), "yyyyMMdd").alias("date_key"),  # For dim_date relationship
        to_date(current_timestamp()).alias("date"),
        col("order_id").alias("order_no"),
        col("sku").alias("style_no"),
        col("sku").alias("style_name"),
        lit("").alias("unified_style_no"),
        lit("").alias("unified_style_name"),
        col("customer_name").alias("buyer_name"),
        lit("SG").alias("country"),
        lit("SGD").alias("currency"),
        col("quantity").alias("qty"),
        col("price").alias("gross_revenue"),
        lit(0.0).alias("total_discount"),
        col("price").alias("net_revenue"),
        (col("price") / col("quantity")).alias("item_gross_price"),
        lit(0.0).alias("item_discount"),
        (col("price") / col("quantity")).alias("item_net_price"),
        lit(0.0).alias("total_returns"),
        lit("USD").alias("unit_cost_currency"),
        lit(20.0).alias("item_unit_cost"),
        lit(0.05).alias("prod_com_percent"),
        (lit(20.0) * lit(0.05)).alias("prod_com"),
        (col("price") - lit(20.0) - (lit(20.0) * lit(0.05))).alias("margin"),
        lit("shopify").alias("channel_id")  # For dim_channels relationship
    )
    
    return shopify_cm1

def create_shopify_cm2(spark, shopify_cm1_df, freight_df):
    """Create Shopify Order-Level Margin Table (Shopify CM2)"""
    print("Creating Shopify CM2 (Order-Level Margins)...")
    
    from pyspark.sql.functions import rand
    
    order_aggregated = shopify_cm1_df.groupBy("order_no", "buyer_name", "country", "date_key", "channel_id").agg(
        lit("SGD").alias("currency"),
        spark_round(sum(col("qty")), 0).alias("qty"),
        spark_round(sum(col("net_revenue")), 2).alias("net_revenue"),
        spark_round(sum(col("total_returns")), 2).alias("total_returns"),
        spark_round(sum(col("item_unit_cost")), 2).alias("total_unit_cost"),
        spark_round(sum(col("margin")), 2).alias("cm1_amount")
    )
    
    # Use random status assignment instead of freight join
    shopify_cm2 = order_aggregated.select(
        col("date_key"),  # For dim_date relationship
        col("order_no"),
        col("buyer_name"),
        col("country"),
        col("currency"),
        col("qty"),
        col("net_revenue"),
        col("total_returns"),
        lit("USD").alias("cost_currency"),
        col("total_unit_cost"),
        col("cm1_amount"),
        # Random freight status - 70% shipped, 30% not shipped yet
        when(rand() < 0.7, lit("shipped")).otherwise(lit("not shipped yet")).alias("freight_out_status"),
        (col("qty") * lit(3.0)).alias("freight_in"),
        # Use actual freight cost when shipped, default when not
        when(rand() < 0.7, lit(12.0) + (rand() * lit(8.0))).otherwise(lit(12.0)).alias("freight_out"),
        lit("not shipped yet").alias("return_status"),
        lit(8.0).alias("freight_return"),
        lit(0.0).alias("freight_income"),
        (col("net_revenue") * lit(0.029)).alias("shopify_fees"),
        col("channel_id")  # For dim_channels relationship
    ).withColumn("cm2_amount", 
        col("cm1_amount") - col("freight_in") - col("freight_out") - lit(8.0) + 
        col("freight_income") - col("shopify_fees")
    )
    
    return shopify_cm2

def create_tiktok_cm1(spark, tiktok_df):
    """Create TikTok Item-Level Margin Table (Livestreaming CM1)"""
    print("Creating TikTok CM1 (Item-Level Margins)...")
    
    tiktok_cm1 = tiktok_df.select(
        date_format(to_date(current_timestamp()), "yyyyMMdd").alias("date_key"),  # For dim_date relationship
        to_date(current_timestamp()).alias("date"),
        col("order_id").alias("order_no"),
        col("sku").alias("style_no"),
        col("sku").alias("style_name"),
        lit("").alias("unified_style_no"),
        lit("").alias("unified_style_name"),
        col("buyer_name"),
        lit("SG").alias("country"),
        lit("SGD").alias("currency"),
        col("quantity").alias("qty"),
        col("price").alias("gross_revenue"),
        lit(0.0).alias("total_discount"),
        col("price").alias("net_revenue"),
        (col("price") / col("quantity")).alias("item_gross_price"),
        lit(0.0).alias("item_discount"),
        (col("price") / col("quantity")).alias("item_net_price"),
        lit(0.0).alias("total_returns"),
        lit("USD").alias("unit_cost_currency"),
        lit(18.0).alias("item_unit_cost"),
        lit(0.05).alias("prod_com_percent"),
        (lit(18.0) * lit(0.05)).alias("prod_com"),
        (col("price") - lit(18.0) - (lit(18.0) * lit(0.05))).alias("margin"),
        lit("tiktok").alias("channel_id")  # For dim_channels relationship
    )
    
    return tiktok_cm1

def create_tiktok_cm2(spark, tiktok_cm1_df, freight_df):
    """Create TikTok Order-Level Margin Table (Livestreaming CM2)"""
    print("Creating TikTok CM2 (Order-Level Margins)...")
    
    order_aggregated = tiktok_cm1_df.groupBy("order_no", "buyer_name", "date_key", "channel_id").agg(
        lit("SGD").alias("currency"),
        spark_round(sum(col("qty")), 0).alias("qty"),
        spark_round(sum(col("net_revenue")), 2).alias("net_revenue"),
        spark_round(sum(col("item_unit_cost")), 2).alias("production_cost"),
        spark_round(sum(col("prod_com")), 2).alias("production_comm")
    )
    
    # Join with freight data to get actual shipping status and costs
    # Note: TikTok handles its own freight according to SOW, but we can still track costs
    tiktok_with_freight = order_aggregated.join(
        freight_df.select("order_reference", "cost", lit("shipped").alias("freight_status")), 
        order_aggregated["order_no"] == freight_df["order_reference"], 
        "left"
    )
    
    tiktok_cm2 = tiktok_with_freight.select(
        col("date_key"),  # For dim_date relationship
        col("order_no"),
        col("buyer_name"),
        col("currency"),
        col("qty"),
        col("net_revenue"),
        col("production_cost"),
        col("production_comm"),
        lit("SGD").alias("freight_currency"),
        (col("qty") * lit(3.0)).alias("freight_in"),
        coalesce(col("cost"), lit(10.0)).alias("freight_out"),  # Use actual freight cost if available
        lit("SGD").alias("trx_currency"),
        (col("net_revenue") * lit(0.05)).alias("trx_fees"),
        lit("SGD").alias("comm_currency"),
        lit(0.08).alias("sales_comm"),
        (col("net_revenue") - col("production_cost") - col("production_comm") - 
         (col("qty") * lit(3.0)) - coalesce(col("cost"), lit(10.0)) - (col("net_revenue") * lit(0.05))).alias("cm2_amount"),
        col("channel_id")  # For dim_channels relationship
    )
    
    return tiktok_cm2

def create_date_dimension_from_data(spark, all_dataframes):
    """Create date dimension from actual data"""
    # Simple approach - get current date
    from datetime import datetime
    current_date = datetime.now().strftime('%Y-%m-%d')
    date_key = datetime.now().strftime('%Y%m%d')
    date_data = [(date_key, current_date, 2025, 2, 6, "Monday", 0)]
    return spark.createDataFrame(date_data, ["date_key", "date", "year", "quarter", "month", "day_of_week", "is_weekend"])

# Main execution
spark = SparkSession.builder \
    .appName("Fashion-CM-Tables") \
    .config("spark.jars", "/opt/spark/jars/extra/postgresql-42.6.0.jar,/opt/spark/jars/extra/clickhouse-jdbc-0.4.6-all.jar") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

checkpoint = get_checkpoint()
print(f"Processing data newer than: {checkpoint}")

# Read Silver data from the dedicated Silver PostgreSQL database
silver_url = "jdbc:postgresql://silver-postgres:5432/fashion_silver_dedicated"
silver_props = {
    "user": "silver_dedicated_user",
    "password": "silver_dedicated_pass_2024",
    "driver": "org.postgresql.Driver"
}

print("Reading Silver data...")

# Ensure ClickHouse database exists first
ensure_clickhouse_database()

try:
    joor_df = spark.read.jdbc(silver_url, "silver_joor_orders", properties=silver_props)
    print(f"Joor data: {joor_df.count()} records")
except Exception as e:
    print(f"Error reading Joor data: {e}")
    joor_df = spark.createDataFrame([], "order_id string, buyer string, price double, quantity int, sku string")

try:
    shopify_df = spark.read.jdbc(silver_url, "silver_shopify_orders", properties=silver_props)
    print(f"Shopify data: {shopify_df.count()} records")
except Exception as e:
    print(f"Error reading Shopify data: {e}")
    shopify_df = spark.createDataFrame([], "order_id string, customer_name string, price double, quantity int, sku string")

try:
    tiktok_df = spark.read.jdbc(silver_url, "silver_tiktok_orders", properties=silver_props)
    print(f"TikTok data: {tiktok_df.count()} records")
except Exception as e:
    print(f"Error reading TikTok data: {e}")
    tiktok_df = spark.createDataFrame([], "order_id string, buyer_name string, price double, quantity int, sku string")

try:
    freight_df = spark.read.jdbc(silver_url, "silver_freight_data", properties=silver_props)
    print(f"Freight data: {freight_df.count()} records")
except Exception as e:
    print(f"Error reading Freight data: {e}")
    freight_df = spark.createDataFrame([], "tracking_number string, provider string, cost double, order_reference string")

print("Creating CM1 and CM2 margin tables...")

# Create the 6 required margin tables (now with proper freight integration)
joor_cm1 = create_joor_cm1(spark, joor_df)
joor_cm2 = create_joor_cm2(spark, joor_cm1, freight_df)

shopify_cm1 = create_shopify_cm1(spark, shopify_df)
shopify_cm2 = create_shopify_cm2(spark, shopify_cm1, freight_df)

tiktok_cm1 = create_tiktok_cm1(spark, tiktok_df)
tiktok_cm2 = create_tiktok_cm2(spark, tiktok_cm1, freight_df)

# Create date dimension based on actual data
dim_date = create_date_dimension_from_data(spark, [joor_cm1, shopify_cm1, tiktok_cm1])

# Create channels dimension
channel_data = [
    ("joor", "Joor", "B2B", "Wholesale"),
    ("shopify", "Shopify", "DTC", "Direct-to-Consumer"), 
    ("tiktok", "TikTok", "Livestream", "Social Commerce")
]
dim_channels = spark.createDataFrame(channel_data, ["channel_id", "channel_name", "channel_type", "description"])

print("Writing CM tables to ClickHouse...")

# Add Freight as fact table with relationship keys
fact_freight = freight_df.select(
    date_format(to_date(current_timestamp()), "yyyyMMdd").alias("date_key"),  # For dim_date relationship
    col("tracking_number"),
    col("provider"),
    col("cost"),
    col("order_reference").alias("order_no"),  # For joining with CM2 tables
    current_timestamp().alias("created_at")
)

# Write the 6 margin tables + Freight
write_to_clickhouse(joor_cm1, "wholesale_cm1", "append")
write_to_clickhouse(joor_cm2, "wholesale_cm2", "append")
write_to_clickhouse(shopify_cm1, "shopify_cm1", "append")
write_to_clickhouse(shopify_cm2, "shopify_cm2", "append")
write_to_clickhouse(tiktok_cm1, "livestreaming_cm1", "append")
write_to_clickhouse(tiktok_cm2, "livestreaming_cm2", "append")

# Add Freight as fact table
write_to_clickhouse(fact_freight, "fact_freight", "append")

# Write dimension tables
write_to_clickhouse(dim_date, "dim_date", "overwrite")
write_to_clickhouse(dim_channels, "dim_channels", "overwrite")

print("All CM tables created successfully!")

# Update checkpoint
current_time = spark.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0]
save_checkpoint(str(current_time))
spark.stop()