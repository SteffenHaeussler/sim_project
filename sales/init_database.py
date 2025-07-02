#!/usr/bin/env python3
"""
International Sales Database Initialization Script

This script initializes the sales database by:
1. Creating the database if it doesn't exist
2. Running the schema creation script
3. Optionally populating with initial reference data
4. Providing database connection testing

Usage:
    python init_database.py [options]

Options:
    --drop-existing    Drop existing database before creating
    --test-connection  Test database connection only
    --populate-ref     Populate with basic reference data
    --verbose          Enable verbose logging
    --help            Show this help message
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

try:
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    from psycopg2 import sql
except ImportError:
    print("Error: psycopg2 not installed. Install with: pip install psycopg2-binary")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print("Warning: python-dotenv not installed. Using environment variables directly.")
    load_dotenv = None


class DatabaseInitializer:
    """Handles database initialization for the sales system."""
    
    def __init__(self, config: Dict[str, Any], verbose: bool = False):
        self.config = config
        self.verbose = verbose
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        level = logging.DEBUG if self.verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        return logging.getLogger(__name__)
    
    def _get_connection(self, database: Optional[str] = None) -> psycopg2.extensions.connection:
        """Get database connection."""
        conn_params = {
            'host': self.config['host'],
            'port': self.config['port'],
            'user': self.config['user'],
            'password': self.config['password']
        }
        
        if database:
            conn_params['database'] = database
            
        try:
            connection = psycopg2.connect(**conn_params)
            self.logger.debug(f"Connected to database: {database or 'default'}")
            return connection
        except psycopg2.Error as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            self.logger.info("Testing database connection...")
            
            # Test connection to PostgreSQL server
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            self.logger.info(f"PostgreSQL version: {version}")
            
            # Test connection to target database
            try:
                db_conn = self._get_connection(self.config['database'])
                db_cursor = db_conn.cursor()
                db_cursor.execute("SELECT current_database();")
                db_name = db_cursor.fetchone()[0]
                self.logger.info(f"Connected to database: {db_name}")
                
                # Test if tables exist
                db_cursor.execute("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('countries', 'products', 'orders');
                """)
                table_count = db_cursor.fetchone()[0]
                
                if table_count > 0:
                    self.logger.info(f"Found {table_count} main tables in database")
                else:
                    self.logger.info("No main tables found - database may need initialization")
                    
                db_cursor.close()
                db_conn.close()
                
            except psycopg2.Error as e:
                if "does not exist" in str(e):
                    self.logger.warning(f"Database '{self.config['database']}' does not exist")
                else:
                    raise
            
            cursor.close()
            conn.close()
            
            self.logger.info("Connection test successful!")
            return True
            
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def database_exists(self) -> bool:
        """Check if the target database exists."""
        try:
            conn = self._get_connection()
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s;",
                (self.config['database'],)
            )
            exists = cursor.fetchone() is not None
            
            cursor.close()
            conn.close()
            return exists
            
        except psycopg2.Error as e:
            self.logger.error(f"Error checking database existence: {e}")
            return False
    
    def create_database(self, drop_existing: bool = False) -> bool:
        """Create the target database."""
        try:
            conn = self._get_connection()
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            db_name = self.config['database']
            
            if drop_existing and self.database_exists():
                self.logger.warning(f"Dropping existing database: {db_name}")
                
                # Terminate active connections
                cursor.execute("""
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = %s
                    AND pid <> pg_backend_pid();
                """, (db_name,))
                
                # Drop database
                cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    sql.Identifier(db_name)
                ))
                self.logger.info(f"Database {db_name} dropped successfully")
            
            if not self.database_exists():
                self.logger.info(f"Creating database: {db_name}")
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(db_name)
                ))
                self.logger.info(f"Database {db_name} created successfully")
            else:
                self.logger.info(f"Database {db_name} already exists")
            
            cursor.close()
            conn.close()
            return True
            
        except psycopg2.Error as e:
            self.logger.error(f"Error creating database: {e}")
            return False
    
    def run_schema_script(self, schema_file: str = "schema.sql") -> bool:
        """Execute the schema creation script."""
        try:
            schema_path = Path(schema_file)
            if not schema_path.exists():
                self.logger.error(f"Schema file not found: {schema_path}")
                return False
            
            self.logger.info(f"Reading schema from: {schema_path}")
            
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Connect to the target database
            conn = self._get_connection(self.config['database'])
            cursor = conn.cursor()
            
            self.logger.info("Executing schema creation script...")
            
            # Execute the schema script
            cursor.execute(schema_sql)
            conn.commit()
            
            # Verify table creation
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name;
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            self.logger.info(f"Created {len(tables)} tables:")
            for table in tables:
                self.logger.info(f"  - {table}")
            
            cursor.close()
            conn.close()
            
            self.logger.info("Schema creation completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"Error running schema script: {e}")
            return False
    
    def populate_reference_data(self) -> bool:
        """Populate database with basic reference data."""
        try:
            conn = self._get_connection(self.config['database'])
            cursor = conn.cursor()
            
            self.logger.info("Populating reference data...")
            
            # Insert basic currencies
            currencies_data = [
                ('USD', 'US Dollar', '$'),
                ('EUR', 'Euro', '€'),
                ('GBP', 'British Pound', '£'),
                ('JPY', 'Japanese Yen', '¥'),
                ('CAD', 'Canadian Dollar', 'C$'),
                ('AUD', 'Australian Dollar', 'A$'),
                ('CHF', 'Swiss Franc', 'CHF'),
                ('CNY', 'Chinese Yuan', '¥'),
                ('INR', 'Indian Rupee', '₹'),
                ('BRL', 'Brazilian Real', 'R$')
            ]
            
            cursor.executemany("""
                INSERT INTO currencies (currency_code, name, symbol) 
                VALUES (%s, %s, %s)
                ON CONFLICT (currency_code) DO NOTHING;
            """, currencies_data)
            
            # Insert basic countries
            countries_data = [
                ('United States', 'USA', 'North America', 'USD'),
                ('United Kingdom', 'GBR', 'Europe', 'GBP'),
                ('Germany', 'DEU', 'Europe', 'EUR'),
                ('France', 'FRA', 'Europe', 'EUR'),
                ('Japan', 'JPN', 'Asia', 'JPY'),
                ('Canada', 'CAN', 'North America', 'CAD'),
                ('Australia', 'AUS', 'Oceania', 'AUD'),
                ('Switzerland', 'CHE', 'Europe', 'CHF'),
                ('China', 'CHN', 'Asia', 'CNY'),
                ('India', 'IND', 'Asia', 'INR'),
                ('Brazil', 'BRA', 'South America', 'BRL')
            ]
            
            cursor.executemany("""
                INSERT INTO countries (name, code, region, currency_code) 
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name) DO NOTHING;
            """, countries_data)
            
            # Insert basic tax types
            tax_types_data = [
                ('VAT', 'Value Added Tax'),
                ('SALES_TAX', 'Sales Tax'),
                ('GST', 'Goods and Services Tax'),
                ('EXCISE', 'Excise Tax'),
                ('CUSTOM_DUTY', 'Custom Duty')
            ]
            
            cursor.executemany("""
                INSERT INTO tax_types (name, description) 
                VALUES (%s, %s)
                ON CONFLICT (name) DO NOTHING;
            """, tax_types_data)
            
            # Insert basic cost types
            cost_types_data = [
                ('PURCHASE', 'Product Purchase Cost'),
                ('TRANSPORT', 'Transportation Cost'),
                ('DUTIES', 'Import Duties'),
                ('STORAGE', 'Storage and Warehousing'),
                ('HANDLING', 'Handling and Processing'),
                ('INSURANCE', 'Insurance Cost'),
                ('OVERHEAD', 'General Overhead')
            ]
            
            cursor.executemany("""
                INSERT INTO cost_types (name, description) 
                VALUES (%s, %s)
                ON CONFLICT (name) DO NOTHING;
            """, cost_types_data)
            
            # Insert basic roles
            roles_data = [
                ('CEO', 'Chief Executive Officer'),
                ('Sales Manager', 'Sales Team Manager'),
                ('Sales Representative', 'Sales Representative'),
                ('Procurement Manager', 'Procurement Team Manager'),
                ('Inventory Manager', 'Inventory Management'),
                ('Customer Service', 'Customer Service Representative'),
                ('Finance Manager', 'Finance Team Manager'),
                ('Operations Manager', 'Operations Team Manager')
            ]
            
            cursor.executemany("""
                INSERT INTO roles (name, description) 
                VALUES (%s, %s)
                ON CONFLICT (name) DO NOTHING;
            """, roles_data)
            
            conn.commit()
            
            # Verify data insertion
            cursor.execute("SELECT COUNT(*) FROM currencies;")
            currencies_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM countries;")
            countries_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM tax_types;")
            tax_types_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM cost_types;")
            cost_types_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM roles;")
            roles_count = cursor.fetchone()[0]
            
            self.logger.info(f"Reference data populated:")
            self.logger.info(f"  - {currencies_count} currencies")
            self.logger.info(f"  - {countries_count} countries")
            self.logger.info(f"  - {tax_types_count} tax types")
            self.logger.info(f"  - {cost_types_count} cost types")
            self.logger.info(f"  - {roles_count} roles")
            
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error populating reference data: {e}")
            return False
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get database information and statistics."""
        try:
            conn = self._get_connection(self.config['database'])
            cursor = conn.cursor()
            
            # Get table information
            cursor.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    n_tup_ins as inserts,
                    n_tup_upd as updates,
                    n_tup_del as deletes
                FROM pg_stat_user_tables
                ORDER BY tablename;
            """)
            
            tables_info = cursor.fetchall()
            
            # Get database size
            cursor.execute("""
                SELECT pg_size_pretty(pg_database_size(%s));
            """, (self.config['database'],))
            
            db_size = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            return {
                'database_name': self.config['database'],
                'database_size': db_size,
                'tables': tables_info,
                'table_count': len(tables_info)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting database info: {e}")
            return {}


def load_config() -> Dict[str, Any]:
    """Load database configuration from environment."""
    if load_dotenv:
        load_dotenv()
    
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'database': os.getenv('DB_NAME', 'sales'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('PGPASSWORD', '')
    }
    
    # Validate configuration
    if not config['password']:
        print("Error: Database password not set. Check PGPASSWORD environment variable.")
        sys.exit(1)
    
    return config


def main():
    """Main function to handle command line arguments and run initialization."""
    parser = argparse.ArgumentParser(
        description='Initialize the international sales database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--drop-existing',
        action='store_true',
        help='Drop existing database before creating'
    )
    
    parser.add_argument(
        '--test-connection',
        action='store_true',
        help='Test database connection only'
    )
    
    parser.add_argument(
        '--populate-ref',
        action='store_true',
        help='Populate with basic reference data'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--schema-file',
        default='schema.sql',
        help='Path to schema SQL file (default: schema.sql)'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config()
    
    # Initialize database handler
    db_init = DatabaseInitializer(config, verbose=args.verbose)
    
    try:
        if args.test_connection:
            # Test connection only
            success = db_init.test_connection()
            if success:
                info = db_init.get_database_info()
                if info:
                    print(f"\nDatabase Info:")
                    print(f"  Name: {info['database_name']}")
                    print(f"  Size: {info['database_size']}")
                    print(f"  Tables: {info['table_count']}")
            sys.exit(0 if success else 1)
        
        # Full initialization process
        print(f"Initializing database: {config['database']}")
        print(f"Host: {config['host']}:{config['port']}")
        print(f"User: {config['user']}")
        print("=" * 50)
        
        # Step 1: Create database
        if not db_init.create_database(drop_existing=args.drop_existing):
            print("Failed to create database")
            sys.exit(1)
        
        # Step 2: Run schema script
        if not db_init.run_schema_script(args.schema_file):
            print("Failed to create schema")
            sys.exit(1)
        
        # Step 3: Populate reference data (if requested)
        if args.populate_ref:
            if not db_init.populate_reference_data():
                print("Failed to populate reference data")
                sys.exit(1)
        
        # Step 4: Final verification
        if not db_init.test_connection():
            print("Final connection test failed")
            sys.exit(1)
        
        print("=" * 50)
        print("Database initialization completed successfully!")
        
        # Show database info
        info = db_init.get_database_info()
        if info:
            print(f"\nDatabase Summary:")
            print(f"  Name: {info['database_name']}")
            print(f"  Size: {info['database_size']}")
            print(f"  Tables: {info['table_count']}")
        
        print(f"\nNext steps:")
        print(f"  1. Run data generation: python generate_data.py")
        print(f"  2. Test with sample queries: python -c \"import psycopg2; print('Ready!')\"")
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()