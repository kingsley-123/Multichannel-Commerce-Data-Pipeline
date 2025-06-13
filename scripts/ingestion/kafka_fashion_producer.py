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
    """Get raw API data from last minute"""
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    if table_name == 'raw_gsheets_api':
        # Google Sheets data has sheet_name column
        cursor.execute(f"""
            SELECT sheet_name, api_response, created_at FROM {table_name} 
            WHERE created_at > NOW() - INTERVAL '1 minute'
            ORDER BY created_at
        """)
    else:
        # Other raw API tables
        cursor.execute(f"""
            SELECT api_response, created_at FROM {table_name} 
            WHERE created_at > NOW() - INTERVAL '1 minute'
            ORDER BY created_at
        """)
    
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data

def stream_data():
    """Stream raw API data to Kafka"""
    print(f"🚀 Streaming at {datetime.now()}")
    
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    total = 0
    
    # Map raw API tables to Kafka topics
    table_topic_map = {
        'raw_joor_api': 'raw-joor-orders',
        'raw_shopify_api': 'raw-shopify-orders', 
        'raw_tiktok_api': 'raw-tiktok-orders',
        'raw_freight_api': 'raw-freight-data',
        'raw_gsheets_api': 'raw-gsheets-data'
    }
    
    for table_name, topic in table_topic_map.items():
        records = get_recent_data(table_name)
        
        if not records:
            print(f"  📭 No recent {table_name} data")
            continue
        
        for record in records:
            try:
                # Extract the raw API response JSON
                if table_name == 'raw_gsheets_api':
                    # Google Sheets includes sheet_name
                    message_data = {
                        'sheet_name': record['sheet_name'],
                        'data': record['api_response'],
                        'stream_timestamp': datetime.now().isoformat()
                    }
                else:
                    # Other APIs just send the response
                    message_data = {
                        'data': record['api_response'],
                        'stream_timestamp': datetime.now().isoformat()
                    }
                
                producer.send(topic, message_data)
                total += 1
                
            except Exception as e:
                print(f"⚠️ Error streaming {table_name} record: {e}")
                continue
        
        print(f"  📦 Sent {len(records)} {table_name} records to {topic}")
    
    producer.flush()
    producer.close()
    print(f"✅ Total: {total} messages sent to Kafka")

def main():
    """Run producer service continuously"""
    print("🏭 Kafka Producer Service - Raw API Data")
    print("📡 Streaming raw API responses to Kafka topics")
    print("⏰ Running every 60 seconds")
    
    while True:
        try:
            stream_data()
            print(f"😴 Sleeping for 60 seconds...")
            time.sleep(60)
        except Exception as e:
            print(f"❌ Error: {e}")
            print("🔄 Retrying in 30 seconds...")
            time.sleep(30)

main()