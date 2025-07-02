#!/usr/bin/env python3
"""
Simple database connection test for the sales database.
"""

import os
import sys
from dotenv import load_dotenv

try:
    import psycopg2
except ImportError:
    print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

def test_connection():
    """Test basic database connectivity."""
    load_dotenv()
    
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'database': os.getenv('DB_NAME', 'sales'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('PGPASSWORD', '')
    }
    
    try:
        print(f"Connecting to {config['database']} at {config['host']}:{config['port']}")
        
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        
        # Test basic queries
        cursor.execute("SELECT current_database(), current_user, now();")
        db_name, user, timestamp = cursor.fetchone()
        
        print(f"✅ Connected successfully!")
        print(f"   Database: {db_name}")
        print(f"   User: {user}")
        print(f"   Timestamp: {timestamp}")
        
        # Count tables
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        table_count = cursor.fetchone()[0]
        print(f"   Tables: {table_count}")
        
        if table_count > 0:
            # Show some table names
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name 
                LIMIT 5;
            """)
            tables = [row[0] for row in cursor.fetchall()]
            print(f"   Sample tables: {', '.join(tables)}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == '__main__':
    success = test_connection()
    sys.exit(0 if success else 1)