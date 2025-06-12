# Streams recent orders from PostgreSQL to Kafka every minute

import json
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer # type: ignore
import os
import time
from datetime import datetime

# Configuration
DB_HOST = os.getenv('SILVER_DB_HOST', 'data-postgres')
DB_PORT = os.getenv('SILVER_DB_PORT', '5432')
DB_NAME = os.getenv('SILVER_DB_NAME', 'fashion_silver')
DB_USER = os.getenv('SILVER_DB_USER', 'silver_user')
DB_PASSWORD = os.getenv('SILVER_DB_PASSWORD', 'silver_pass_2024')
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')

def get_recent_data(table_name):
    """Get data from last minute"""
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute(f"""
        SELECT * FROM {table_name} 
        WHERE created_at > NOW() - INTERVAL '1 minute'
        ORDER BY created_at
    """)
    
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data

def convert_to_json(record):
    """Convert database record to JSON"""
    data = dict(record)
    for key, value in data.items():
        if hasattr(value, 'isoformat'):
            data[key] = value.isoformat()
        elif hasattr(value, '__float__'):
            data[key] = float(value)
    return data

def stream_data():
    """Stream data to Kafka"""
    print(f" Streaming at {datetime.now()}")
    
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    total = 0
    
    # Stream orders
    for platform in ['joor', 'shopify', 'tiktok']:
        orders = get_recent_data(f'{platform}_orders')
        if orders:
            for order in orders:
                producer.send(f'raw-{platform}-orders', convert_to_json(order))
                total += 1
            print(f"  Sent {len(orders)} {platform} orders")
    
    # Stream freight
    freight = get_recent_data('freight_data')
    if freight:
        for record in freight:
            producer.send('raw-freight-data', convert_to_json(record))
            total += 1
        print(f"  Sent {len(freight)} freight records")
    
    producer.flush()
    producer.close()
    print(f" Total: {total} messages sent")

def main():
    """Run service continuously"""
    print("🏭 Kafka Producer Service Started")
    
    while True:
        try:
            stream_data()
            time.sleep(60)  # Wait 1 minute
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(30)  # Retry in 30 seconds

main()