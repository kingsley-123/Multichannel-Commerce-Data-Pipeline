import psycopg2
from psycopg2.extras import RealDictCursor
import os
import time
from datetime import datetime
from flask import Flask, jsonify, request

# Database Connection
DB_HOST = os.getenv('SILVER_DB_HOST', 'data-postgres')
DB_PORT = os.getenv('SILVER_DB_PORT', '5432')
DB_NAME = os.getenv('SILVER_DB_NAME', 'fashion_silver')
DB_USER = os.getenv('SILVER_DB_USER', 'silver_user')
DB_PASSWORD = os.getenv('SILVER_DB_PASSWORD', 'silver_pass_2024')

app = Flask(__name__)

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)

def get_raw_api_data(platform, limit=None):
    """Get raw API data from database"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        table_name = f"raw_{platform}_api"
        
        if limit:
            query = f"SELECT id, api_response, created_at FROM {table_name} ORDER BY created_at DESC LIMIT %s"
            cursor.execute(query, (limit,))
        else:
            query = f"SELECT id, api_response, created_at FROM {table_name} ORDER BY created_at DESC"
            cursor.execute(query)
        
        data = cursor.fetchall()
        
        # Convert to JSON serializable format
        results = []
        for row in data:
            result = {
                'id': row['id'],
                'api_response': row['api_response'],  # Raw JSON data
                'created_at': row['created_at'].isoformat()
            }
            results.append(result)
        
        return results
        
    except Exception as e:
        print(f"Error getting {platform} data: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def get_gsheets_data(limit=None):
    """Get Google Sheets data"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        if limit:
            query = "SELECT id, sheet_name, api_response, created_at FROM raw_gsheets_api ORDER BY created_at DESC LIMIT %s"
            cursor.execute(query, (limit,))
        else:
            query = "SELECT id, sheet_name, api_response, created_at FROM raw_gsheets_api ORDER BY created_at DESC"
            cursor.execute(query)
        
        data = cursor.fetchall()
        
        # Convert to JSON serializable
        results = []
        for row in data:
            result = {
                'id': row['id'],
                'sheet_name': row['sheet_name'],
                'api_response': row['api_response'],
                'created_at': row['created_at'].isoformat()
            }
            results.append(result)
        
        return results
        
    except Exception as e:
        print(f"Error getting Google Sheets data: {e}")
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
        tables = ['raw_joor_api', 'raw_shopify_api', 'raw_tiktok_api', 'raw_freight_api', 'raw_gsheets_api']
        
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
            'layer': 'bronze/raw',
            'connection_details': f"{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat(),
            'layer': 'bronze/raw'
        }), 500

@app.route('/api/joor/raw')
def api_joor_raw():
    """Raw Joor B2B API responses"""
    limit = request.args.get('limit', type=int)
    data = get_raw_api_data('joor', limit)
    return jsonify({
        'raw_api_responses': data, 
        'count': len(data),
        'platform': 'joor',
        'layer': 'bronze/raw',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/shopify/raw')
def api_shopify_raw():
    """Raw Shopify DTC API responses"""
    limit = request.args.get('limit', type=int)
    data = get_raw_api_data('shopify', limit)
    return jsonify({
        'raw_api_responses': data, 
        'count': len(data),
        'platform': 'shopify',
        'layer': 'bronze/raw',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/tiktok/raw')
def api_tiktok_raw():
    """Raw TikTok Shop API responses"""
    limit = request.args.get('limit', type=int)
    data = get_raw_api_data('tiktok', limit)
    return jsonify({
        'raw_api_responses': data, 
        'count': len(data),
        'platform': 'tiktok',
        'layer': 'bronze/raw',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/freight/raw')
def api_freight_raw():
    """Raw freight API responses (DHL, UPS, EasyShip)"""
    limit = request.args.get('limit', type=int)
    data = get_raw_api_data('freight', limit)
    return jsonify({
        'raw_api_responses': data,
        'count': len(data),
        'platform': 'freight',
        'layer': 'bronze/raw',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/gsheets/raw')
def api_gsheets_raw():
    """Raw Google Sheets API responses"""
    limit = request.args.get('limit', type=int)
    data = get_gsheets_data(limit)
    return jsonify({
        'raw_api_responses': data,
        'count': len(data),
        'platform': 'google_sheets',
        'layer': 'bronze/raw',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/stats')
def api_stats():
    """Database statistics API"""
    stats = get_stats()
    return jsonify({
        'statistics': stats,
        'layer': 'bronze/raw',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/endpoints')
def api_endpoints():
    """List all available endpoints"""
    endpoints = {
        'health': '/api/health',
        'statistics': '/api/stats',
        'joor_raw': '/api/joor/raw',
        'shopify_raw': '/api/shopify/raw',
        'tiktok_raw': '/api/tiktok/raw',
        'freight_raw': '/api/freight/raw',
        'gsheets_raw': '/api/gsheets/raw',
        'endpoints': '/api/endpoints'
    }
    
    return jsonify({
        'available_endpoints': endpoints,
        'query_parameters': {
            'limit': 'Limit number of results (e.g., ?limit=10)',
            'example': '/api/joor/raw?limit=5'
        },
        'server_info': {
            'database_host': DB_HOST,
            'database_name': DB_NAME,
            'layer': 'bronze/raw',
            'architecture': 'medallion',
            'note': 'Serving raw API data - use Spark for Silver layer processing'
        },
        'timestamp': datetime.now().isoformat()
    })

@app.route('/')
def index():
    """Root endpoint"""
    return jsonify({
        'message': 'Fashion Data API Server - Bronze/Raw Layer',
        'version': '3.0',
        'architecture': 'medallion',
        'layer': 'bronze/raw',
        'endpoints': '/api/endpoints',
        'health': '/api/health',
        'note': 'Currently serving raw API data. Clean data available after Spark processing.',
        'timestamp': datetime.now().isoformat()
    })

def main():
    """Start the API server"""
    print("🚀 Fashion Data API Server - Bronze/Raw Layer")
    print(f"🔧 Database: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    print("\n📡 Available Raw Data API endpoints:")
    print("  GET /api/joor/raw            - Raw Joor API responses")
    print("  GET /api/shopify/raw         - Raw Shopify API responses")
    print("  GET /api/tiktok/raw          - Raw TikTok API responses")
    print("  GET /api/freight/raw         - Raw freight API responses")
    print("  GET /api/gsheets/raw         - Raw Google Sheets responses")
    print("  GET /api/stats               - Database statistics")
    
    print(f"\n🌐 Starting Raw Data API server on http://0.0.0.0:5000")
    print("📊 Serving raw API data from Bronze layer!")
    print("⚡ Build Spark jobs to process Bronze → Silver → Gold")
    
    app.run(host='0.0.0.0', port=5000, debug=False)

main()