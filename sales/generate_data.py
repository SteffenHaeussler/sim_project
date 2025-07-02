#!/usr/bin/env python3
"""
International Sales Database Data Generator

This script generates comprehensive synthetic data for the international sales database
with realistic business patterns, multi-currency operations, and temporal consistency.

Usage:
    python generate_data.py [options]

Options:
    --preset {small,medium,large,enterprise}  Use predefined data volumes
    --customers N                             Number of customers
    --orders N                                Number of sales orders
    --products N                              Number of products
    --suppliers N                             Number of suppliers
    --employees N                             Number of employees
    --date-range START-END                    Date range (YYYY-YYYY)
    --only TABLE1,TABLE2                      Generate only specific tables
    --validate-only                           Validate existing data only
    --batch-size N                            Batch size for inserts (default: 1000)
    --verbose                                 Enable verbose logging
    --help                                    Show this help message
"""

import os
import sys
import argparse
import logging
from datetime import datetime, date
from typing import Dict, List, Any, Optional

try:
    import psycopg2
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Error: Missing required dependency: {e}")
    print("Install with: uv sync")
    sys.exit(1)

# Import our modular generators
from generators.config import GenerationConfig
from generators.geographic import GeographicGenerator
from generators.currency import CurrencyGenerator
from generators.tax import TaxGenerator
from generators.hr import HRGenerator
from generators.product import ProductGenerator
from generators.supplier import SupplierGenerator
from generators.customer import CustomerGenerator
from generators.sales import SalesGenerator
from generators.inventory import InventoryGenerator
from generators.validation import ValidationGenerator


class SalesDataGenerator:
    """Main orchestrator for generating international sales data."""
    
    def __init__(self, config: GenerationConfig, verbose: bool = False):
        self.config = config
        self.verbose = verbose
        self.logger = self._setup_logging()
        
        # Database connection
        self.conn = None
        self.cursor = None
        
        # Shared cache for generated data
        self.cache = {
            'countries': {},
            'territories': {},
            'currencies': {},
            'employees': {},
            'products': {},
            'suppliers': {},
            'customers': {}
        }
        
        # Initialize generator modules
        self._init_generators()
        
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        level = logging.DEBUG if self.verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        return logging.getLogger(__name__)
    
    def _init_generators(self):
        """Initialize all generator modules."""
        # Note: We'll initialize these when we have database connection
        pass
    
    def connect_database(self) -> bool:
        """Connect to the database."""
        try:
            load_dotenv()
            
            self.conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', '5432')),
                database=os.getenv('DB_NAME', 'sales'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('PGPASSWORD', '')
            )
            self.cursor = self.conn.cursor()
            self.logger.info("Database connection established")
            
            # Initialize generators with database connection
            self.geographic_gen = GeographicGenerator(self.config, self.conn, self.cursor, self.cache, self.logger)
            self.currency_gen = CurrencyGenerator(self.config, self.conn, self.cursor, self.cache, self.logger)
            self.tax_gen = TaxGenerator(self.config, self.conn, self.cursor, self.cache, self.logger)
            self.hr_gen = HRGenerator(self.config, self.conn, self.cursor, self.cache, self.logger)
            self.product_gen = ProductGenerator(self.config, self.conn, self.cursor, self.cache, self.logger)
            self.supplier_gen = SupplierGenerator(self.config, self.conn, self.cursor, self.cache, self.logger)
            self.customer_gen = CustomerGenerator(self.config, self.conn, self.cursor, self.cache, self.logger)
            self.sales_gen = SalesGenerator(self.config, self.conn, self.cursor, self.cache, self.logger)
            self.inventory_gen = InventoryGenerator(self.config, self.conn, self.cursor, self.cache, self.logger)
            self.validation_gen = ValidationGenerator(self.config, self.conn, self.cursor, self.cache, self.logger)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            return False
    
    def disconnect_database(self):
        """Disconnect from the database."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("Database connection closed")
    
    def clear_existing_data(self, tables: List[str] = None):
        """Clear existing data from specified tables."""
        if tables is None:
            # Clear all tables in dependency order
            tables = [
                'purchase_order_line_costs', 'purchase_order_details', 'purchase_orders',
                'product_costs', 'inventory', 'product_suppliers', 'product_prices',
                'order_details', 'orders', 'customer_addresses', 'sales_targets',
                'employees', 'customers', 'suppliers', 'products', 'product_categories',
                'shipping_methods', 'addresses', 'tax_rates', 'territories',
                'exchange_rates', 'countries', 'currencies', 'tax_types', 'roles', 'cost_types'
            ]
        
        self.logger.info(f"Clearing data from {len(tables)} tables...")
        
        for table in tables:
            try:
                self.cursor.execute(f"DELETE FROM {table} CASCADE;")
                self.conn.commit()
                self.logger.debug(f"Cleared table: {table}")
            except Exception as e:
                self.logger.warning(f"Could not clear table {table}: {e}")
                self.conn.rollback()
    
    def generate_all_data(self, clear_existing: bool = False, tables_to_generate: List[str] = None):
        """Generate complete dataset."""
        self.logger.info("Starting comprehensive data generation...")
        
        start_time = datetime.now()
        
        try:
            if not self.connect_database():
                return False
            
            if clear_existing:
                self.clear_existing_data()
            
            # Default generation order - IMPORTANT: currencies must come before geographic
            # because countries have foreign key references to currencies
            all_generators = [
                ('currency', self.currency_gen.generate_currency_data),
                ('geographic', self.geographic_gen.generate_geographic_data),
                ('tax', self.tax_gen.generate_tax_data),
                ('hr', self.hr_gen.generate_hr_data),
                ('products', self.product_gen.generate_product_data),
                ('suppliers', self.supplier_gen.generate_supplier_data),
                ('customers', self.customer_gen.generate_customer_data),
                ('sales', self.sales_gen.generate_sales_data),
                ('inventory', self.inventory_gen.generate_inventory_data),
            ]
            
            # Filter generators if specific tables requested
            if tables_to_generate:
                generators = [(name, func) for name, func in all_generators if name in tables_to_generate]
            else:
                generators = all_generators
            
            # Execute generators
            for name, generator_func in generators:
                self.logger.info(f"Generating {name} data...")
                try:
                    generator_func()
                    self.logger.info(f"✓ {name} data generation completed")
                except Exception as e:
                    self.logger.error(f"✗ {name} data generation failed: {e}")
                    raise
            
            # Final validation
            self.logger.info("Running data integrity validation...")
            if self.validation_gen.validate_data_integrity():
                self.logger.info("✓ Data integrity validation passed")
            else:
                self.logger.warning("⚠ Data integrity validation found issues")
            
            elapsed = datetime.now() - start_time
            self.logger.info(f"Data generation completed in {elapsed}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Data generation failed: {e}")
            return False
        finally:
            self.disconnect_database()


def main():
    """Main function to handle command line arguments and run data generation."""
    parser = argparse.ArgumentParser(
        description='Generate synthetic data for the international sales database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Preset configurations
    parser.add_argument(
        '--preset',
        choices=['small', 'medium', 'large', 'enterprise'],
        help='Use predefined data volume preset'
    )
    
    # Volume controls
    parser.add_argument('--countries', type=int, help='Number of countries')
    parser.add_argument('--territories', type=int, help='Number of territories')
    parser.add_argument('--customers', type=int, help='Number of customers')
    parser.add_argument('--orders', type=int, help='Number of sales orders')
    parser.add_argument('--products', type=int, help='Number of products')
    parser.add_argument('--suppliers', type=int, help='Number of suppliers')
    parser.add_argument('--employees', type=int, help='Number of employees')
    parser.add_argument('--purchase-orders', type=int, help='Number of purchase orders')
    
    # Date range
    parser.add_argument(
        '--date-range',
        help='Date range in format YYYY-YYYY (e.g., 2022-2024)'
    )
    
    # Selective generation
    parser.add_argument(
        '--only',
        help='Generate only specific data types (comma-separated): geographic,currency,tax,hr,products,suppliers,customers,sales,inventory'
    )
    
    # Database options
    parser.add_argument(
        '--clear-existing',
        action='store_true',
        help='Clear existing data before generating new data'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for database inserts (default: 1000)'
    )
    
    # Validation
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate existing data, do not generate new data'
    )
    
    # Logging
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    try:
        # Create configuration
        if args.preset:
            config = GenerationConfig.from_preset(args.preset)
            print(f"Using preset: {args.preset}")
        else:
            config = GenerationConfig()
        
        # Override with command line arguments
        if args.countries:
            config.countries = args.countries
        if args.territories:
            config.territories = args.territories
        if args.customers:
            config.customers = args.customers
        if args.orders:
            config.sales_orders = args.orders
        if args.products:
            config.products = args.products
        if args.suppliers:
            config.suppliers = args.suppliers
        if args.employees:
            config.employees = args.employees
        if args.purchase_orders:
            config.purchase_orders = args.purchase_orders
        if args.batch_size:
            config.batch_size = args.batch_size
        
        # Parse date range
        if args.date_range:
            try:
                start_year, end_year = args.date_range.split('-')
                config.start_date = date(int(start_year), 1, 1)
                config.end_date = date(int(end_year), 12, 31)
            except ValueError:
                print("Error: Date range must be in format YYYY-YYYY")
                sys.exit(1)
        
        # Parse selective generation
        tables_to_generate = None
        if args.only:
            tables_to_generate = [table.strip() for table in args.only.split(',')]
            valid_tables = ['geographic', 'currency', 'tax', 'hr', 'products', 'suppliers', 'customers', 'sales', 'inventory']
            invalid_tables = [t for t in tables_to_generate if t not in valid_tables]
            if invalid_tables:
                print(f"Error: Invalid table names: {', '.join(invalid_tables)}")
                print(f"Valid options: {', '.join(valid_tables)}")
                sys.exit(1)
        
        # Display configuration
        print("=" * 60)
        print("International Sales Database Data Generator")
        print("=" * 60)
        print(f"Configuration:")
        print(f"  Countries: {config.countries}")
        print(f"  Territories: {config.territories}")
        print(f"  Employees: {config.employees}")
        print(f"  Products: {config.products}")
        print(f"  Suppliers: {config.suppliers}")
        print(f"  Customers: {config.customers}")
        print(f"  Purchase Orders: {config.purchase_orders}")
        print(f"  Sales Orders: {config.sales_orders}")
        print(f"  Date Range: {config.start_date} to {config.end_date}")
        print(f"  Batch Size: {config.batch_size}")
        
        if tables_to_generate:
            print(f"  Tables to Generate: {', '.join(tables_to_generate)}")
        if args.clear_existing:
            print(f"  ⚠ Will clear existing data")
        
        print("=" * 60)
        
        # Initialize generator
        generator = SalesDataGenerator(config, verbose=args.verbose)
        
        if args.validate_only:
            # Validation only mode
            print("Running data validation...")
            if generator.connect_database():
                try:
                    if generator.validation_gen.validate_data_integrity():
                        print("✓ Data integrity validation passed")
                        sys.exit(0)
                    else:
                        print("✗ Data integrity validation failed")
                        sys.exit(1)
                finally:
                    generator.disconnect_database()
            else:
                print("✗ Could not connect to database")
                sys.exit(1)
        
        # Ask for confirmation for large datasets
        total_records_estimate = (
            config.countries + config.territories + config.employees + 
            config.products + config.suppliers + config.customers + 
            config.purchase_orders + config.sales_orders
        )
        
        if total_records_estimate > 10000 and not args.preset:
            response = input(f"\nThis will generate approximately {total_records_estimate:,} records. Continue? (y/N): ")
            if response.lower() not in ['y', 'yes']:
                print("Operation cancelled")
                sys.exit(0)
        
        # Generate data
        print("\nStarting data generation...")
        success = generator.generate_all_data(
            clear_existing=args.clear_existing,
            tables_to_generate=tables_to_generate
        )
        
        if success:
            print("\n" + "=" * 60)
            print("✓ Data generation completed successfully!")
            print("=" * 60)
            
            # Display summary statistics
            if generator.connect_database():
                try:
                    print("\nDatabase Summary:")
                    
                    summary_queries = [
                        ("Countries", "SELECT COUNT(*) FROM countries"),
                        ("Territories", "SELECT COUNT(*) FROM territories"),
                        ("Employees", "SELECT COUNT(*) FROM employees"),
                        ("Products", "SELECT COUNT(*) FROM products"),
                        ("Suppliers", "SELECT COUNT(*) FROM suppliers"),
                        ("Customers", "SELECT COUNT(*) FROM customers"),
                        ("Purchase Orders", "SELECT COUNT(*) FROM purchase_orders"),
                        ("Sales Orders", "SELECT COUNT(*) FROM orders"),
                        ("Inventory Records", "SELECT COUNT(*) FROM inventory"),
                        ("Exchange Rate Records", "SELECT COUNT(*) FROM exchange_rates"),
                    ]
                    
                    for label, query in summary_queries:
                        try:
                            generator.cursor.execute(query)
                            count = generator.cursor.fetchone()[0]
                            print(f"  {label:<20}: {count:>8,}")
                        except:
                            print(f"  {label:<20}: {'-':>8}")
                    
                    # Database size
                    try:
                        generator.cursor.execute("SELECT pg_size_pretty(pg_database_size(current_database()));")
                        db_size = generator.cursor.fetchone()[0]
                        print(f"  {'Database Size':<20}: {db_size:>8}")
                    except:
                        pass
                        
                finally:
                    generator.disconnect_database()
            
            print(f"\nNext steps:")
            print(f"  1. Run sample queries: psql -f sample_queries.sql")
            print(f"  2. Test connection: uv run test_connection.py")
            print(f"  3. Validate data: uv run generate_data.py --validate-only")
            
        else:
            print("\n" + "=" * 60)
            print("✗ Data generation failed!")
            print("=" * 60)
            print("Check the logs above for error details.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()