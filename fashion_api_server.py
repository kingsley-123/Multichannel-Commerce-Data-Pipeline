# fashion_api_server.py
"""
Fashion Data API Server - UPDATED FOR SILVER LAYER
Complete corrected version with new environment variables
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
import time
from datetime import datetime
from flask import Flask, jsonify, request

# Database Connection - UPDATED to use Silver layer environment variables
DB_HOST = os.getenv('SILVER_DB_HOST', 'data-postgres')
DB_PORT = os.getenv('SILVER_DB_PORT', '5432')
DB_NAME = os.getenv('SILVER_DB_NAME', 'fashion_silver')
DB_USER = os.getenv('SILVER_DB_USER', 'silver_user')
DB_PASSWORD = os.getenv('SILVER_DB_PASSWORD', 'silver_pass_2024')

app = Flask(__name__)

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

def get_order_data(platform, limit=None):
    """Get order data from database"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        if limit:
            query = f"SELECT * FROM {platform}_orders ORDER BY created_at DESC LIMIT %s"
            cursor.execute(query, (limit,))
        else:
            query = f"SELECT * FROM {platform}_orders ORDER BY created_at DESC"
            cursor.execute(query)
        
        data = cursor.fetchall()
        
        # Convert to JSON serializable format
        results = []
        for row in data:
            row_dict = dict(row)
            for key, value in row_dict.items():
                if isinstance(value, datetime):
                    row_dict[key] = value.isoformat()
                elif hasattr(value, 'isoformat'):
                    row_dict[key] = value.isoformat()
                elif value is None:
                    row_dict[key] = None
            results.append(row_dict)
        
        return results
        
    except Exception as e:
        print(f"Error getting {platform} data: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def get_master_data(table_name):
    """Get master data (products, buyers)"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        data = cursor.fetchall()
        
        # Convert to JSON serializable
        results = []
        for row in data:
            row_dict = dict(row)
            for key, value in row_dict.items():
                if isinstance(value, datetime):
                    row_dict[key] = value.isoformat()
                elif hasattr(value, 'isoformat'):
                    row_dict[key] = value.isoformat()
                elif value is None:
                    row_dict[key] = None
            results.append(row_dict)
        
        return results
        
    except Exception as e:
        print(f"Error getting {table_name}: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def get_freight_data(limit=None):
    """Get freight data"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        if limit:
            query = "SELECT * FROM freight_data ORDER BY created_at DESC LIMIT %s"
            cursor.execute(query, (limit,))
        else:
            query = "SELECT * FROM freight_data ORDER BY created_at DESC"
            cursor.execute(query)
        
        data = cursor.fetchall()
        
        # Convert to JSON serializable
        results = []
        for row in data:
            row_dict = dict(row)
            for key, value in row_dict.items():
                if isinstance(value, datetime):
                    row_dict[key] = value.isoformat()
                elif hasattr(value, 'isoformat'):
                    row_dict[key] = value.isoformat()
                elif value is None:
                    row_dict[key] = None
            results.append(row_dict)
        
        return results
        
    except Exception as e:
        print(f"Error getting freight data: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def get_stats():
    """Get database statistics"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        stats = {}
        tables = ['products', 'buyers', 'joor_orders', 'shopify_orders', 'tiktok_orders', 'freight_data']
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                stats[table] = cursor.fetchone()[0]
            except:
                stats[table] = 0
        
        return stats
        
    except Exception as e:
        print(f"Error getting stats: {e}")
        return {}
    finally:
        cursor.close()
        conn.close()

# API Routes
@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'connected',
            'layer': 'silver',
            'connection_details': f"{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat(),
            'layer': 'silver',
            'connection_details': f"{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        }), 500

@app.route('/api/joor/orders')
def api_joor_orders():
    """Joor B2B wholesale orders API"""
    limit = request.args.get('limit', type=int)
    data = get_order_data('joor', limit)
    return jsonify({
        'orders': data, 
        'count': len(data),
        'platform': 'joor',
        'layer': 'silver',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/shopify/orders')
def api_shopify_orders():
    """Shopify DTC e-commerce orders API"""
    limit = request.args.get('limit', type=int)
    data = get_order_data('shopify', limit)
    return jsonify({
        'orders': data, 
        'count': len(data),
        'platform': 'shopify',
        'layer': 'silver',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/tiktok/orders')
def api_tiktok_orders():
    """TikTok Shop livestreaming orders API"""
    limit = request.args.get('limit', type=int)
    data = get_order_data('tiktok', limit)
    return jsonify({
        'orders': data, 
        'count': len(data),
        'platform': 'tiktok',
        'layer': 'silver',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/products')
def api_products():
    """Products/Production Tracker API"""
    data = get_master_data('products')
    return jsonify({
        'products': data,
        'count': len(data),
        'layer': 'silver',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/buyers')
def api_buyers():
    """Buyers/Sales Tracker API"""
    data = get_master_data('buyers')
    return jsonify({
        'buyers': data,
        'count': len(data),
        'layer': 'silver',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/freight')
def api_freight():
    """Freight data API (DHL, UPS, EasyShip)"""
    limit = request.args.get('limit', type=int)
    data = get_freight_data(limit)
    return jsonify({
        'freight_data': data,
        'count': len(data),
        'layer': 'silver',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/stats')
def api_stats():
    """Database statistics API"""
    stats = get_stats()
    return jsonify({
        'statistics': stats,
        'layer': 'silver',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/endpoints')
def api_endpoints():
    """List all available endpoints"""
    endpoints = {
        'health': '/api/health',
        'statistics': '/api/stats',
        'joor_orders': '/api/joor/orders',
        'shopify_orders': '/api/shopify/orders',
        'tiktok_orders': '/api/tiktok/orders',
        'products': '/api/products',
        'buyers': '/api/buyers',
        'freight': '/api/freight',
        'endpoints': '/api/endpoints'
    }
    
    return jsonify({
        'available_endpoints': endpoints,
        'query_parameters': {
            'limit': 'Limit number of results (e.g., ?limit=10)',
            'example': '/api/joor/orders?limit=5'
        },
        'server_info': {
            'database_host': DB_HOST,
            'database_port': DB_PORT,
            'database_name': DB_NAME,
            'database_user': DB_USER,
            'layer': 'silver',
            'architecture': 'medallion'
        },
        'timestamp': datetime.now().isoformat()
    })

@app.route('/')
def index():
    """Root endpoint - redirect to endpoints list"""
    return jsonify({
        'message': 'Fashion Data API Server - Silver Layer',
        'version': '2.0',
        'architecture': 'medallion',
        'layer': 'silver',
        'endpoints': '/api/endpoints',
        'health': '/api/health',
        'timestamp': datetime.now().isoformat()
    })

def main():
    """Start the API server"""
    print("🚀 Fashion Data API Server - Silver Layer")
    print(f"🔧 Silver Layer Environment Variables:")
    print(f"   SILVER_DB_HOST: {DB_HOST}")
    print(f"   SILVER_DB_PORT: {DB_PORT}")
    print(f"   SILVER_DB_NAME: {DB_NAME}")
    print(f"   SILVER_DB_USER: {DB_USER}")
    
    # Wait for database to be ready
    if not wait_for_db():
        print("❌ Exiting: Could not connect to Silver database")
        exit(1)
    
    # Show available endpoints
    print("\n📡 Available Silver Layer API endpoints:")
    print("  GET /                        - API info")
    print("  GET /api/health              - Health check")
    print("  GET /api/stats               - Database statistics")
    print("  GET /api/joor/orders         - Joor wholesale orders")
    print("  GET /api/shopify/orders      - Shopify DTC orders")
    print("  GET /api/tiktok/orders       - TikTok livestreaming orders")
    print("  GET /api/products            - Product catalog")
    print("  GET /api/buyers              - Buyer/sales tracker data")
    print("  GET /api/freight             - Freight data")
    print("  GET /api/endpoints           - List all endpoints")
    
    print("\n🔧 Query parameters:")
    print("  ?limit=N                     - Limit results (e.g., ?limit=10)")
    
    print(f"\n🌐 Starting Silver Layer API server on http://0.0.0.0:5000")
    print("📡 Serving cleaned data from Silver layer (PostgreSQL)!")
    print("🔄 Ready to feed data to Gold layer (ClickHouse) via Spark!")
    print("🛑 Container will restart if this fails")
    
    # Start Flask server
    app.run(host='0.0.0.0', port=5000, debug=False)

# Execute main function directly
main()