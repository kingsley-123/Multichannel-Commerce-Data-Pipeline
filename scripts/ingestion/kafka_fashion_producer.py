# Streams recent orders from PostgreSQL to Kafka topics
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer # type: ignore
import os
from datetime import datetime

# Database connection
DB_HOST = os.getenv('SILVER_DB_HOST', 'data-postgres')
DB_PORT = os.getenv('SILVER_DB_PORT', '5432')
DB_NAME = os.getenv('SILVER_DB_NAME', 'fashion_silver')
DB_USER = os.getenv('SILVER_DB_USER', 'silver_user')
DB_PASSWORD = os.getenv('SILVER_DB_PASSWORD', 'silver_pass_2024')

# Kafka connection
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')

def get_recent_freight():
    """Get freight data from last minute"""
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute("""
        SELECT * FROM freight_data 
        WHERE created_at > NOW() - INTERVAL '1 minute'
        ORDER BY created_at
    """)
    
    freight_data = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return freight_data

def get_recent_orders(platform):
    """Get orders from last minute"""
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute(f"""
        SELECT * FROM {platform}_orders 
        WHERE created_at > NOW() - INTERVAL '1 minute'
        ORDER BY created_at
    """)
    
    orders = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return orders

def convert_to_json(record):
    """Convert database record to JSON"""
    data = dict(record)
    for key, value in data.items():
        if hasattr(value, 'isoformat'):  # datetime objects
            data[key] = value.isoformat()
        elif hasattr(value, '__float__'):  # decimal objects
            data[key] = float(value)
    return data

def main():
    """Stream fashion data to Kafka"""
    print(f"Streaming fashion data at {datetime.now()}")
    
    # Setup Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    total_sent = 0
    
    # Stream each platform
    for platform in ['joor', 'shopify', 'tiktok']:
        orders = get_recent_orders(platform)
        
        if not orders:
            print(f"  No recent {platform} orders")
            continue
            
        topic = f'raw-{platform}-orders'
        
        for order in orders:
            data = convert_to_json(order)
            producer.send(topic, data)
            total_sent += 1
        
        print(f"  Sent {len(orders)} {platform} orders to {topic}")
    
    # Stream freight data
    freight_data = get_recent_freight()
    
    if freight_data:
        for record in freight_data:
            data = convert_to_json(record)
            producer.send('raw-freight-data', data)
            total_sent += 1
        
        print(f"  Sent {len(freight_data)} freight records to raw-freight-data")
    else:
        print(f"  No recent freight data")
    
    producer.flush()
    producer.close()
    
    print(f"✅ Total: {total_sent} messages sent to Kafka")

main()