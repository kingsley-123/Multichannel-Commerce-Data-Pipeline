# fashion_data_generator.py
"""
Fashion Data Generator - UPDATED FOR NEW ENVIRONMENT VARIABLES
Generates realistic daily volumes with time-based patterns
Designed for millions of records over time
"""

import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
import numpy as np
from faker import Faker
import random
import time
import os
import uuid
from datetime import datetime, timedelta

# Database Connection - UPDATED to use new Silver layer environment variables
DB_HOST = os.getenv('SILVER_DB_HOST', 'data-postgres')
DB_PORT = os.getenv('SILVER_DB_PORT', '5432')
DB_NAME = os.getenv('SILVER_DB_NAME', 'fashion_silver')
DB_USER = os.getenv('SILVER_DB_USER', 'silver_user')
DB_PASSWORD = os.getenv('SILVER_DB_PASSWORD', 'silver_pass_2024')

# Initialize with better randomization
current_time = datetime.now()
# Use process ID + current time for better uniqueness
seed_value = int(time.time() * 1000) + os.getpid()
random.seed(seed_value)
np.random.seed(seed_value % (2**31))
fake = Faker()
fake.seed_instance(seed_value)

# PRODUCTION VOLUME CONFIGURATION
BRAND_SIZE = "large"  # "small", "medium", "large"

DAILY_VOLUMES = {
    "small": {
        "joor": (50, 150),      # 50-150 B2B orders/day
        "shopify": (300, 800),   # 300-800 DTC orders/day
        "tiktok": (100, 400)     # 100-400 livestream orders/day
    },
    "medium": {
        "joor": (100, 300),
        "shopify": (800, 2500),
        "tiktok": (400, 1200)
    },
    "large": {
        "joor": (200, 600),     # 200-600 B2B orders/day
        "shopify": (3000, 8000), # 3K-8K DTC orders/day
        "tiktok": (1500, 4000)   # 1.5K-4K livestream orders/day
    }
}

def wait_for_db():
    """Wait for database to be ready with retry logic"""
    max_retries = 30
    retry_count = 0
    
    print(f"🔌 Connecting to Silver database: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, 
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
            )
            conn.close()
            print("✅ Silver database connection successful!")
            return True
        except Exception as e:
            retry_count += 1
            print(f"⏳ Waiting for Silver database... attempt {retry_count}/{max_retries}")
            if retry_count < 5:  # Only show error details for first few attempts
                print(f"   Error: {e}")
            time.sleep(2)
    
    print("❌ Could not connect to Silver database after 30 attempts")
    return False

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, 
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

def generate_unique_order_number(platform, batch_index, order_index):
    """Generate truly unique order numbers using UUID and multiple factors"""
    timestamp = int(time.time() * 1000000)  # microseconds
    process_id = os.getpid()
    uuid_part = str(uuid.uuid4())[:8]  # First 8 chars of UUID
    
    platform_prefix = {
        'joor': 'JOR',
        'shopify': 'SH', 
        'tiktok': 'TT'
    }[platform]
    
    return f"{platform_prefix}{timestamp}{process_id}{batch_index:03d}{order_index:04d}{uuid_part}"

def init_database():
    """Create production-ready tables with indexes"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("🔧 Creating Silver layer database tables...")
    
    # Enhanced Joor orders table with partitioning considerations
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS joor_orders (
            id BIGSERIAL PRIMARY KEY,
            order_no VARCHAR(100) UNIQUE NOT NULL,
            order_date DATE NOT NULL,
            style_no VARCHAR(50) NOT NULL,
            style_name VARCHAR(100) NOT NULL,
            unified_style_no VARCHAR(20) NOT NULL,
            buyer_name VARCHAR(100) NOT NULL,
            country VARCHAR(50) NOT NULL,
            currency VARCHAR(5) NOT NULL,
            qty INTEGER NOT NULL,
            gross_revenue DECIMAL(12,2) NOT NULL,
            total_discount DECIMAL(12,2) NOT NULL,
            net_revenue DECIMAL(12,2) NOT NULL,
            item_gross_price DECIMAL(10,2) NOT NULL,
            item_net_price DECIMAL(10,2) NOT NULL,
            unit_cost DECIMAL(12,2) NOT NULL,
            prod_commission DECIMAL(12,2) NOT NULL,
            cm1_margin DECIMAL(12,2) NOT NULL,
            transaction_fees DECIMAL(10,2) DEFAULT 0,
            insurance DECIMAL(10,2) DEFAULT 0,
            freight_in_sgd DECIMAL(10,2) NOT NULL,
            payment_source VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Enhanced Shopify orders table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shopify_orders (
            id BIGSERIAL PRIMARY KEY,
            order_no VARCHAR(100) UNIQUE NOT NULL,
            order_date DATE NOT NULL,
            style_no VARCHAR(50) NOT NULL,
            unified_style_no VARCHAR(20) NOT NULL,
            buyer_name VARCHAR(100) NOT NULL,
            country VARCHAR(50) NOT NULL,
            qty INTEGER NOT NULL,
            net_revenue DECIMAL(12,2) NOT NULL,
            unit_cost DECIMAL(12,2) NOT NULL,
            prod_commission DECIMAL(12,2) NOT NULL,
            cm1_margin DECIMAL(12,2) NOT NULL,
            shopify_fees DECIMAL(10,2) NOT NULL,
            freight_in_sgd DECIMAL(10,2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Enhanced TikTok orders table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tiktok_orders (
            id BIGSERIAL PRIMARY KEY,
            order_no VARCHAR(100) UNIQUE NOT NULL,
            order_date DATE NOT NULL,
            style_no VARCHAR(50) NOT NULL,
            unified_style_no VARCHAR(20) NOT NULL,
            buyer_name VARCHAR(100) NOT NULL,
            country VARCHAR(50) NOT NULL,
            qty INTEGER NOT NULL,
            net_revenue DECIMAL(12,2) NOT NULL,
            unit_cost DECIMAL(12,2) NOT NULL,
            prod_commission DECIMAL(12,2) NOT NULL,
            cm1_margin DECIMAL(12,2) NOT NULL,
            transaction_fees DECIMAL(10,2) NOT NULL,
            seller_commission DECIMAL(10,2) NOT NULL,
            freight_out DECIMAL(10,2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Enhanced freight table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS freight_data (
            id BIGSERIAL PRIMARY KEY,
            order_no VARCHAR(100) NOT NULL,
            carrier VARCHAR(20) NOT NULL,
            shipping_cost DECIMAL(10,2) NOT NULL,
            currency VARCHAR(5) NOT NULL,
            status VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Production indexes for performance
    production_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_joor_order_date ON joor_orders(order_date);",
        "CREATE INDEX IF NOT EXISTS idx_joor_created_at ON joor_orders(created_at);",
        "CREATE INDEX IF NOT EXISTS idx_joor_style_no ON joor_orders(style_no);",
        "CREATE INDEX IF NOT EXISTS idx_joor_buyer ON joor_orders(buyer_name);",
        
        "CREATE INDEX IF NOT EXISTS idx_shopify_order_date ON shopify_orders(order_date);",
        "CREATE INDEX IF NOT EXISTS idx_shopify_created_at ON shopify_orders(created_at);",
        "CREATE INDEX IF NOT EXISTS idx_shopify_style_no ON shopify_orders(style_no);",
        
        "CREATE INDEX IF NOT EXISTS idx_tiktok_order_date ON tiktok_orders(order_date);",
        "CREATE INDEX IF NOT EXISTS idx_tiktok_created_at ON tiktok_orders(created_at);",
        "CREATE INDEX IF NOT EXISTS idx_tiktok_style_no ON tiktok_orders(style_no);",
        
        "CREATE INDEX IF NOT EXISTS idx_freight_order_no ON freight_data(order_no);",
        "CREATE INDEX IF NOT EXISTS idx_freight_created_at ON freight_data(created_at);"
    ]
    
    for index_sql in production_indexes:
        cursor.execute(index_sql)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✓ Silver layer database tables and indexes created")

def get_hourly_multiplier(hour, platform):
    """Get realistic hourly traffic patterns"""
    patterns = {
        "joor": {  # B2B business hours
            **{h: 0.1 for h in range(0, 8)},   # Night: 10% 
            **{h: 1.5 for h in range(8, 12)},  # Morning: 150%
            **{h: 0.8 for h in range(12, 14)}, # Lunch: 80%
            **{h: 1.8 for h in range(14, 18)}, # Afternoon: 180%
            **{h: 0.3 for h in range(18, 24)}  # Evening: 30%
        },
        "shopify": {  # DTC consumer patterns
            **{h: 0.2 for h in range(0, 8)},   # Night: 20%
            **{h: 0.8 for h in range(8, 11)},  # Morning: 80%
            **{h: 1.5 for h in range(11, 14)}, # Lunch: 150%
            **{h: 1.0 for h in range(14, 18)}, # Afternoon: 100%
            **{h: 2.2 for h in range(18, 22)}, # Evening: 220%
            **{h: 1.0 for h in range(22, 24)}  # Late: 100%
        },
        "tiktok": {  # Livestream patterns
            **{h: 0.3 for h in range(0, 10)},  # Morning: 30%
            **{h: 1.8 for h in range(10, 14)}, # Lunch streams: 180%
            **{h: 1.0 for h in range(14, 19)}, # Afternoon: 100%
            **{h: 3.5 for h in range(19, 23)}, # Prime time: 350%
            **{h: 1.2 for h in range(23, 24)}  # Late: 120%
        }
    }
    return patterns[platform].get(hour, 1.0)

def get_daily_volume(platform, brand_size=BRAND_SIZE):
    """Get realistic daily volume for platform"""
    min_vol, max_vol = DAILY_VOLUMES[brand_size][platform]
    base_volume = random.randint(min_vol, max_vol)
    
    # Apply day-of-week variations
    weekday = current_time.weekday()
    if platform == "joor":  # B2B lower on weekends
        weekend_factor = 0.2 if weekday >= 5 else 1.0
    else:  # DTC/TikTok higher on weekends
        weekend_factor = 1.3 if weekday >= 5 else 1.0
    
    return int(base_volume * weekend_factor)

def get_hourly_volume(platform, hour_of_day):
    """Calculate realistic hourly volume"""
    daily_volume = get_daily_volume(platform)
    hourly_base = daily_volume / 24
    multiplier = get_hourly_multiplier(hour_of_day, platform)
    
    # Add randomness
    hourly_volume = int(hourly_base * multiplier * random.uniform(0.7, 1.3))
    return max(1, hourly_volume)  # Minimum 1 order

def create_master_data():
    """Create enhanced master data for production scale"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Products table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            style_no VARCHAR(50) PRIMARY KEY,
            unified_style_no VARCHAR(20),
            unified_style_name VARCHAR(100),
            category VARCHAR(20),
            avg_unit_cost DECIMAL(10,2)
        );
    """)
    
    # Buyers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS buyers (
            buyer_name VARCHAR(100) PRIMARY KEY,
            payment_source VARCHAR(20),
            season VARCHAR(20),
            payment_terms VARCHAR(50),
            sales_commission_pct DECIMAL(5,2),
            prod_commission_pct DECIMAL(5,2),
            country VARCHAR(50)
        );
    """)
    
    # Check existing data
    cursor.execute("SELECT COUNT(*) FROM products")
    product_count = cursor.fetchone()[0]
    
    if product_count > 0:
        print(f"✓ Using existing {product_count} products")
        cursor.close()
        conn.close()
        return
    
    print("📦 Creating Silver layer master data...")
    
    # Create unique products using UUID and timestamp
    products = []
    categories = ['TOP', 'BTM', 'DRS', 'OUT', 'ACC']
    seasons = ['SS25', 'FW25', 'SS26', 'Resort25']
    cost_ranges = {'TOP': (8, 35), 'BTM': (15, 55), 'DRS': (25, 85), 'OUT': (25, 85), 'ACC': (5, 25)}
    
    # Use set to ensure unique style numbers
    used_style_numbers = set()
    
    for i in range(500):  # 500 products for production
        category = random.choice(categories)
        season = random.choice(seasons)
        min_cost, max_cost = cost_ranges[category]
        
        # Generate truly unique style number
        while True:
            style_num = random.randint(100, 99999)
            timestamp_part = int(time.time()) % 10000
            style_no = f"{category}-{season}-{style_num:05d}-{timestamp_part}"
            if style_no not in used_style_numbers:
                used_style_numbers.add(style_no)
                break
        
        products.append((
            style_no,
            f"UNI-{i+1:04d}",
            fake.catch_phrase(),
            category,
            round(random.uniform(min_cost, max_cost), 2)
        ))
    
    # Create unique buyers using UUID
    buyers = []
    used_buyer_names = set()
    
    for i in range(200):  # 200 buyers for production
        while True:
            company_name = fake.company()
            # Add UUID suffix to ensure uniqueness
            buyer_name = f"{company_name} Store {str(uuid.uuid4())[:8]}"
            if buyer_name not in used_buyer_names:
                used_buyer_names.add(buyer_name)
                break
        
        buyers.append((
            buyer_name,
            random.choice(['Hilldun', 'Stripe']),
            random.choice(['SS25', 'FW25', 'SS26', 'Resort25']),
            random.choice(['Net 30', 'Net 60', 'COD', '2/10 Net 30']),
            round(random.uniform(2.5, 8.0), 2),
            round(random.uniform(4.0, 6.0), 2),
            random.choice(['United States', 'Canada', 'United Kingdom', 'Australia', 'Germany', 'France', 'Japan'])
        ))
    
    # Use INSERT ... ON CONFLICT to handle any remaining duplicates
    try:
        execute_batch(cursor, 
            "INSERT INTO products VALUES (%s, %s, %s, %s, %s) ON CONFLICT (style_no) DO NOTHING", 
            products, page_size=1000)
        
        execute_batch(cursor, 
            "INSERT INTO buyers VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (buyer_name) DO NOTHING", 
            buyers, page_size=1000)
        
        conn.commit()
        print(f"✓ Created {len(products)} products and {len(buyers)} buyers (Silver layer)")
    except Exception as e:
        print(f"⚠️  Warning during master data creation: {e}")
        conn.rollback()
    
    cursor.close()
    conn.close()

def generate_freight_data(order_numbers, coverage_percent=0.7):
    """Generate freight data for orders"""
    if not order_numbers:
        print("⚠️  No order numbers provided for freight generation")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Select random orders for freight (70% coverage by default)
        num_freight_orders = int(len(order_numbers) * coverage_percent)
        selected_orders = random.sample(order_numbers, min(num_freight_orders, len(order_numbers)))
        
        freight_records = []
        carriers = ['DHL', 'UPS', 'EasyShip', 'FedEx', 'USPS']
        statuses = ['pending', 'shipped', 'in_transit', 'delivered', 'exception']
        currencies = ['USD', 'SGD', 'EUR']
        
        for order_no in selected_orders:
            carrier = random.choice(carriers)
            status = random.choice(statuses)
            currency = random.choice(currencies)
            
            # Generate realistic shipping costs based on carrier
            if carrier == 'DHL':
                cost = round(random.uniform(15.0, 45.0), 2)
            elif carrier == 'UPS':
                cost = round(random.uniform(12.0, 38.0), 2)
            elif carrier == 'EasyShip':
                cost = round(random.uniform(8.0, 25.0), 2)
            elif carrier == 'FedEx':
                cost = round(random.uniform(18.0, 50.0), 2)
            else:  # USPS
                cost = round(random.uniform(5.0, 20.0), 2)
            
            freight_records.append((
                order_no,
                carrier,
                cost,
                currency,
                status
            ))
        
        # Insert freight data
        query = """
            INSERT INTO freight_data (
                order_no, carrier, shipping_cost, currency, status
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        
        execute_batch(cursor, query, freight_records, page_size=1000)
        conn.commit()
        print(f"  📦 Generated {len(freight_records)} freight records ({coverage_percent*100}% coverage)")
        
    except Exception as e:
        print(f"⚠️  Warning during freight data generation: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def generate_orders_batch(platform, count, batch_size=1000):
    """Generate orders in batches for better performance"""
    if count == 0:
        return []
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get master data
    cursor.execute("SELECT * FROM products")
    products = cursor.fetchall()
    cursor.execute("SELECT * FROM buyers WHERE country = ANY(%s)", 
                  (['United States', 'Canada', 'United Kingdom', 'Australia'],))
    buyers = cursor.fetchall()
    
    all_orders = []
    order_numbers = []
    
    # Process in batches
    for batch_start in range(0, count, batch_size):
        batch_end = min(batch_start + batch_size, count)
        batch_count = batch_end - batch_start
        batch_index = batch_start // batch_size
        
        orders = []
        
        for order_index in range(batch_count):
            product = random.choice(products)
            
            # Generate truly unique order numbers
            order_no = generate_unique_order_number(platform, batch_index, order_index)
            
            if platform == "joor":
                buyer = random.choice(buyers)
                qty = random.randint(10, 200)  # Larger B2B orders
                
                # Convert Decimal to float
                avg_unit_cost = float(product[4])
                prod_commission_pct = float(buyer[5])
                
                # Calculations
                markup = random.uniform(2.0, 3.2)
                gross_price = avg_unit_cost * markup
                gross_revenue = gross_price * qty
                discount = gross_revenue * random.uniform(0, 0.15)
                net_revenue = gross_revenue - discount
                
                unit_cost = avg_unit_cost * qty
                prod_commission = unit_cost * (prod_commission_pct / 100)
                cm1_margin = net_revenue - unit_cost - prod_commission
                
                transaction_fees = net_revenue * 0.029 if buyer[1] == 'Stripe' else 0
                insurance = net_revenue * 0.02 if buyer[1] == 'Hilldun' else 0
                
                orders.append((
                    order_no, current_time.date(), product[0], product[2], product[1],
                    buyer[0], buyer[6], 'USD', qty, gross_revenue, discount, net_revenue,
                    gross_revenue/qty, net_revenue/qty, unit_cost, prod_commission, cm1_margin,
                    transaction_fees, insurance, 3.0 * qty, buyer[1]
                ))
                
            elif platform == "shopify":
                qty = random.randint(1, 5)  # DTC orders
                
                avg_unit_cost = float(product[4])
                markup = random.uniform(3.5, 5.5)
                net_revenue = avg_unit_cost * qty * markup * random.uniform(0.7, 0.9)
                
                unit_cost = avg_unit_cost * qty
                prod_commission = unit_cost * 0.05
                cm1_margin = net_revenue - unit_cost - prod_commission
                
                orders.append((
                    order_no, current_time.date(), product[0], product[1],
                    fake.name(), random.choice(['United States', 'Canada', 'United Kingdom', 'Australia']), qty,
                    net_revenue, unit_cost, prod_commission, cm1_margin,
                    net_revenue * 0.026, 3.0 * qty
                ))
                
            elif platform == "tiktok":
                qty = random.randint(1, 3)  # Small livestream orders
                
                avg_unit_cost = float(product[4])
                markup = random.uniform(2.8, 4.2)
                net_revenue = avg_unit_cost * qty * markup * random.uniform(0.4, 0.8)
                
                unit_cost = avg_unit_cost * qty
                prod_commission = unit_cost * 0.05
                cm1_margin = net_revenue - unit_cost - prod_commission
                
                orders.append((
                    order_no, current_time.date(), product[0], product[1],
                    f"TikTok_User_{random.randint(100000, 999999)}", 
                    random.choice(['United States', 'Singapore', 'Malaysia', 'Thailand']), qty,
                    net_revenue, unit_cost, prod_commission, cm1_margin,
                    net_revenue * 0.035, net_revenue * 0.12, random.uniform(3, 12)
                ))
            
            order_numbers.append(order_no)
        
        # Insert batch with ON CONFLICT handling
        if platform == "joor":
            query = """
                INSERT INTO joor_orders (
                    order_no, order_date, style_no, style_name, unified_style_no,
                    buyer_name, country, currency, qty, gross_revenue, total_discount, net_revenue,
                    item_gross_price, item_net_price, unit_cost, prod_commission, cm1_margin,
                    transaction_fees, insurance, freight_in_sgd, payment_source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_no) DO NOTHING
            """
        elif platform == "shopify":
            query = """
                INSERT INTO shopify_orders (
                    order_no, order_date, style_no, unified_style_no,
                    buyer_name, country, qty, net_revenue, unit_cost, prod_commission, cm1_margin,
                    shopify_fees, freight_in_sgd
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_no) DO NOTHING
            """
        elif platform == "tiktok":
            query = """
                INSERT INTO tiktok_orders (
                    order_no, order_date, style_no, unified_style_no,
                    buyer_name, country, qty, net_revenue, unit_cost, prod_commission, cm1_margin,
                    transaction_fees, seller_commission, freight_out
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_no) DO NOTHING
            """
        
        try:
            execute_batch(cursor, query, orders, page_size=1000)
            all_orders.extend(orders)
            print(f"  📦 Inserted batch {batch_index + 1}: {len(orders)} {platform} orders")
        except Exception as e:
            print(f"⚠️  Warning during batch insert: {e}")
            conn.rollback()
            # Continue with next batch
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return order_numbers

def main():
    """Production data generation main function - UPDATED FOR SILVER LAYER"""
    print("🏭 Production Fashion Data Generator - SILVER LAYER VERSION")
    print(f"📊 Brand Size: {BRAND_SIZE.upper()}")
    print(f"⏰ Current Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔌 Silver Database: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"🎲 Seed Value: {seed_value}")
    
    # Wait for database to be ready
    if not wait_for_db():
        print("❌ Exiting: Could not connect to Silver database")
        exit(1)
    
    # Get expected volumes
    hour = current_time.hour
    joor_volume = get_hourly_volume('joor', hour)
    shopify_volume = get_hourly_volume('shopify', hour)
    tiktok_volume = get_hourly_volume('tiktok', hour)
    total_volume = joor_volume + shopify_volume + tiktok_volume
    
    print(f"📈 Expected Hourly Volumes (Hour {hour}):")
    print(f"   Joor (B2B): {joor_volume:,} orders")
    print(f"   Shopify (DTC): {shopify_volume:,} orders")
    print(f"   TikTok (Livestream): {tiktok_volume:,} orders")
    print(f"   🎯 Total: {total_volume:,} orders")
    print(f"📅 Daily Projection: {total_volume * 24:,} orders")
    
    try:
        # Initialize
        init_database()
        create_master_data()
        
        print(f"\n🚀 Starting Silver layer data generation...")
        start_time = time.time()
        
        # Generate orders in batches for performance
        print(f"📦 Generating order data...")
        joor_orders = generate_orders_batch('joor', joor_volume)
        shopify_orders = generate_orders_batch('shopify', shopify_volume)
        tiktok_orders = generate_orders_batch('tiktok', tiktok_volume)
        
        # Generate freight data
        print(f"🚚 Generating freight data...")
        all_orders = joor_orders + shopify_orders + tiktok_orders
        if all_orders:
            generate_freight_data(all_orders, coverage_percent=0.7)
        else:
            print("⚠️  No orders generated, skipping freight data")
        
        generation_time = time.time() - start_time
        
        # Final stats
        print(f"\n⚡ Performance:")
        print(f"   Generation Time: {generation_time:.2f} seconds")
        if total_volume > 0:
            print(f"   Records/Second: {total_volume/generation_time:,.0f}")
        print(f"   Memory Efficiency: Batch processing used")
        print(f"   Unique Order Numbers: ✅ UUID-based generation")
        print(f"   Freight Data: ✅ Generated with 70% coverage")
        
        # Show final counts
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM joor_orders")
        joor_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM shopify_orders")
        shopify_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM tiktok_orders")
        tiktok_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM freight_data")
        freight_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM products")
        product_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM buyers")
        buyer_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"\n📊 Silver Layer Database Summary:")
        print(f"   📦 Products: {product_count:,}")
        print(f"   👥 Buyers: {buyer_count:,}")
        print(f"   🏢 Joor Orders: {joor_count:,}")
        print(f"   🛒 Shopify Orders: {shopify_count:,}")
        print(f"   📱 TikTok Orders: {tiktok_count:,}")
        print(f"   🚚 Freight Records: {freight_count:,}")
        print(f"   🎯 Total Orders: {joor_count + shopify_count + tiktok_count:,}")
        
        print(f"\n✅ Silver layer data generation complete!")
        print(f"🔄 Run every hour via Airflow for realistic data flow")
        print(f"🌐 Access data via API: http://localhost:5000/api/freight")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        raise e

# Execute main function directly when script is run
main()