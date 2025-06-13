# raw_api_data_generator.py
"""
Raw API Data Generator Service
Generates messy, realistic API responses continuously for Bronze layer
"""

import psycopg2
from psycopg2.extras import execute_batch, Json
import random
import time
import os
import uuid
from datetime import datetime, timedelta
from faker import Faker

# Database Connection
DB_HOST = os.getenv('SILVER_DB_HOST', 'data-postgres')
DB_PORT = os.getenv('SILVER_DB_PORT', '5432')
DB_NAME = os.getenv('SILVER_DB_NAME', 'fashion_silver')
DB_USER = os.getenv('SILVER_DB_USER', 'silver_user')
DB_PASSWORD = os.getenv('SILVER_DB_PASSWORD', 'silver_pass_2024')

# Service Configuration
RUN_CONTINUOUSLY = os.getenv('RUN_CONTINUOUSLY', 'true').lower() == 'true'
INTERVAL_SECONDS = int(os.getenv('INTERVAL_SECONDS', '30'))

# Random seed for different data each run
seed = int(time.time() * 1000) + os.getpid()
random.seed(seed)
fake = Faker()
fake.seed_instance(seed)

def get_db_connection():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)

def init_tables():
    """Create raw data tables"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    tables = [
        "CREATE TABLE IF NOT EXISTS raw_joor_api (id BIGSERIAL PRIMARY KEY, api_response JSONB, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);",
        "CREATE TABLE IF NOT EXISTS raw_shopify_api (id BIGSERIAL PRIMARY KEY, api_response JSONB, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);",
        "CREATE TABLE IF NOT EXISTS raw_tiktok_api (id BIGSERIAL PRIMARY KEY, api_response JSONB, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);",
        "CREATE TABLE IF NOT EXISTS raw_freight_api (id BIGSERIAL PRIMARY KEY, api_response JSONB, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);",
        "CREATE TABLE IF NOT EXISTS raw_gsheets_api (id BIGSERIAL PRIMARY KEY, sheet_name VARCHAR(100), api_response JSONB, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
    ]
    
    for sql in tables:
        cursor.execute(sql)
    
    conn.commit()
    cursor.close()
    conn.close()

def generate_joor_data(count=10):
    """Generate messy Joor B2B data"""
    orders = []
    
    for _ in range(count):
        order = {
            "order_id": str(uuid.uuid4()),
            "buyer": fake.company(),
            "quantity": random.randint(10, 100),
            "price": str(random.uniform(25, 200)),  # String price (messy)
            "currency": random.choice(["USD", "usd", "$"]),
            "payment": random.choice(["stripe", "hilldun", "net_30"]),
            "country": random.choice(["US", "USA", "United States"]),
            "sku": f"JOR-{random.randint(1000, 9999)}",
            "product": fake.catch_phrase(),
            "date": datetime.now().isoformat() + random.choice(['Z', '+00:00', ''])
        }
        orders.append((Json(order),))  # Wrap with Json()
    
    return orders

def generate_shopify_data(count=50):
    """Generate messy Shopify DTC data"""
    orders = []
    
    for _ in range(count):
        order = {
            "id": random.randint(100000, 999999),
            "customer": fake.name(),
            "email": fake.email(),
            "quantity": random.randint(1, 5),
            "price": f"{random.uniform(20, 300):.2f}",  # String format
            "country": random.choice(["US", "CA", "GB"]),
            "sku": f"SH-{random.randint(1000, 9999)}",
            "product": fake.catch_phrase(),
            "payment": random.choice(["shopify_payments", "paypal", "stripe"]),
            "status": random.choice(["paid", "pending", "refunded"]),
            "created_at": datetime.now().isoformat()
        }
        orders.append((Json(order),))  # Wrap with Json()
    
    return orders

def generate_tiktok_data(count=30):
    """Generate messy TikTok data"""
    orders = []
    
    for _ in range(count):
        order = {
            "order_id": f"TT{random.randint(10000000, 99999999)}",
            "buyer": fake.name(),
            "quantity": random.randint(1, 3),
            "price_cents": random.randint(1500, 10000),  # TikTok uses cents
            "currency": random.choice(["USD", "SGD"]),
            "country": random.choice(["US", "SG", "MY"]),
            "sku": f"TT-{random.randint(1000, 9999)}",
            "product": fake.catch_phrase(),
            "live_stream": random.choice([True, False]),
            "creator": f"@{fake.user_name()}",
            "timestamp": int(time.time())
        }
        orders.append((Json(order),))  # Wrap with Json()
    
    return orders

def generate_freight_data(count=40):
    """Generate messy freight data"""
    freight = []
    
    for _ in range(count):
        provider = random.choice(['dhl', 'ups', 'easyship'])
        
        if provider == 'dhl':
            data = {
                "tracking": fake.bothify('DHL#######'),
                "cost": random.uniform(15, 45),
                "currency": "USD",
                "status": random.choice(["picked_up", "in_transit", "delivered"]),
                "weight": f"{random.uniform(1, 5):.1f} kg"
            }
        elif provider == 'ups':
            data = {
                "tracking": fake.bothify('1Z###########'),
                "cost": random.uniform(12, 38),
                "currency": "USD", 
                "status": random.choice(["origin", "transit", "delivered"]),
                "weight": f"{random.uniform(2, 10):.1f} lbs"
            }
        else:  # easyship
            data = {
                "tracking": fake.bothify('ES######'),
                "cost": random.uniform(8, 25),
                "currency": "SGD",
                "status": random.choice(["created", "shipped", "delivered"]),
                "weight": f"{random.uniform(0.5, 3):.1f} kg"
            }
        
        data["provider"] = provider
        data["order_ref"] = f"ORDER-{random.randint(10000, 99999)}"
        freight.append((Json(data),))  # Wrap with Json()
    
    return freight

def generate_sheets_data():
    """Generate messy Google Sheets data"""
    sheets = []
    
    # Production sheet
    production = {
        "sheet_name": "Production Tracker",
        "data": [
            ["Style No", "Cost USD", "Factory", "Date"],
            *[[f"STYLE-{random.randint(1000, 9999)}", 
               f"${random.uniform(10, 50):.2f}",  # Mix of string/number formats
               random.choice(["Factory A", "Factory B", ""]),
               fake.date_this_year().strftime('%m/%d/%Y') if random.random() > 0.1 else ""] 
              for _ in range(20)]
        ]
    }
    sheets.append(("Production Tracker", Json(production)))  # Wrap with Json()
    
    # Sales sheet
    sales = {
        "sheet_name": "Sales Tracker", 
        "data": [
            ["Buyer", "Commission %", "Payment", "Country"],
            *[[fake.company(),
               f"{random.uniform(3, 8):.1f}%" if random.random() > 0.3 else str(random.uniform(3, 8)),
               random.choice(["Hilldun", "stripe", "NET30"]),
               random.choice(["US", "USA", "United States"])]
              for _ in range(15)]
        ]
    }
    sheets.append(("Sales Tracker", Json(sales)))  # Wrap with Json()
    
    return sheets

def generate_data():
    """Generate all raw data - single run"""
    # Update seed for each run
    global seed
    seed = int(time.time() * 1000) + os.getpid()
    random.seed(seed)
    fake.seed_instance(seed)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Generate data
        joor_data = generate_joor_data(random.randint(5, 15))
        shopify_data = generate_shopify_data(random.randint(20, 60))
        tiktok_data = generate_tiktok_data(random.randint(15, 40))
        freight_data = generate_freight_data(random.randint(20, 50))
        sheets_data = generate_sheets_data()
        
        # Insert data (Json wrapper already applied in generate functions)
        execute_batch(cursor, "INSERT INTO raw_joor_api (api_response) VALUES (%s)", joor_data)
        execute_batch(cursor, "INSERT INTO raw_shopify_api (api_response) VALUES (%s)", shopify_data)
        execute_batch(cursor, "INSERT INTO raw_tiktok_api (api_response) VALUES (%s)", tiktok_data)
        execute_batch(cursor, "INSERT INTO raw_freight_api (api_response) VALUES (%s)", freight_data)
        
        for sheet_name, sheet_data in sheets_data:
            cursor.execute("INSERT INTO raw_gsheets_api (sheet_name, api_response) VALUES (%s, %s)", (sheet_name, sheet_data))
        
        conn.commit()
        
        total = len(joor_data) + len(shopify_data) + len(tiktok_data) + len(freight_data) + len(sheets_data)
        print(f"✅ Generated {total} raw API responses")
        print(f"📊 Joor: {len(joor_data)}, Shopify: {len(shopify_data)}, TikTok: {len(tiktok_data)}, Freight: {len(freight_data)}, Sheets: {len(sheets_data)}")
        
        return total
        
    except Exception as e:
        print(f"❌ Error during data generation: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function - runs once or continuously"""
    if RUN_CONTINUOUSLY:
        print("🏭 Raw API Data Generator Service Started")
        print(f"⏰ Generating new API data every {INTERVAL_SECONDS} seconds")
        print("🔄 This simulates real-time API feeds")
        
        # Initialize tables once
        init_tables()
        
        while True:
            try:
                print(f"\n📡 {datetime.now().strftime('%H:%M:%S')} - Generating new API data...")
                generate_data()
                print(f"😴 Sleeping for {INTERVAL_SECONDS} seconds...")
                time.sleep(INTERVAL_SECONDS)
                
            except KeyboardInterrupt:
                print("\n⏹️  Service stopped")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
                print(f"🔄 Retrying in {INTERVAL_SECONDS} seconds...")
                time.sleep(INTERVAL_SECONDS)
    else:
        # Run once (for testing or manual runs)
        print(f"🚀 Raw API Generator - Single Run")
        init_tables()
        generate_data()

main()