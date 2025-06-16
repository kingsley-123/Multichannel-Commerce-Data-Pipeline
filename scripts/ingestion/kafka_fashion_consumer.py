import json
import os
from datetime import datetime
from kafka import KafkaConsumer # type: ignore
from minio import Minio # type: ignore
import io

# Configuration
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')
MINIO_HOST = os.getenv('MINIO_HOST', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'bronze_access_key')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'bronze_secret_key_2024')

def main():
    """Consume and write to MinIO"""
    print(f"🚀 Consumer started at {datetime.now()}")
    
    # Setup MinIO
    minio_client = Minio(MINIO_HOST, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    
    # Setup Kafka consumer - ALL 5 topics
    consumer = KafkaConsumer(
        'raw-joor-orders', 'raw-shopify-orders', 'raw-tiktok-orders', 'raw-freight-data', 'raw-gsheets-data',
        bootstrap_servers=[KAFKA_SERVERS],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='fashion-consumer'
    )
    
    count = 0
    
    for message in consumer:
        try:
            # Extract the actual API data from producer wrapper
            message_data = message.value
            
            if 'data' in message_data:
                # New format from updated producer
                api_data = message_data['data']
                stream_timestamp = message_data.get('stream_timestamp', datetime.now().isoformat())
                
                # Handle Google Sheets special case
                if message.topic == 'raw-gsheets-data' and 'sheet_name' in message_data:
                    api_data = {
                        'sheet_name': message_data['sheet_name'],
                        'sheet_data': api_data
                    }
            else:
                # Fallback for old format
                api_data = message_data
                stream_timestamp = datetime.now().isoformat()
            
            # Create filename with date partition
            now = datetime.now()
            date_path = now.strftime('%Y/%m/%d')
            timestamp = int(now.timestamp())
            topic_clean = message.topic.replace('raw-', '').replace('-', '_')
            filename = f"{topic_clean}/{date_path}/{timestamp}_{count}.json"
            
            # Prepare final Bronze layer data
            bronze_data = {
                'raw_api_data': api_data,
                'kafka_metadata': {
                    'topic': message.topic,
                    'partition': message.partition,
                    'offset': message.offset,
                    'stream_timestamp': stream_timestamp,
                    'bronze_timestamp': datetime.now().isoformat()
                }
            }
            
            # FIXED: Convert to JSON bytes (single line for Spark compatibility)
            json_data = json.dumps(bronze_data)  # NO INDENT - this is the key fix
            data_stream = io.BytesIO(json_data.encode('utf-8'))
            
            # Write to MinIO Bronze layer
            minio_client.put_object('fashion-bronze-raw', filename, data_stream, length=len(json_data))
            
            count += 1
            print(f"✅ {message.topic} → {filename}")
            
        except Exception as e:
            print(f"❌ Error: {e}")

main()